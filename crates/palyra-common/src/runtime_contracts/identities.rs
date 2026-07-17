//! Typed identities and causal links shared by runtime subsystems.
//!
//! The wrappers keep control-flow, side-effect, approval, and delivery identities
//! distinct while preserving legacy opaque wire values during migration.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::RuntimeGeneration;

/// Schema version for [`RuntimeIdentitySetV1`].
pub const RUNTIME_IDENTITY_SET_SCHEMA_VERSION: u32 = 1;
/// Maximum UTF-8 byte length accepted for one runtime identity.
pub const MAX_RUNTIME_ID_BYTES: usize = 128;
/// Schema version for [`LegacyRuntimeIdentityAdapter`].
pub const LEGACY_RUNTIME_IDENTITY_ADAPTER_SCHEMA_VERSION: u32 = 1;
/// Maximum missing typed fields retained by a legacy identity adapter.
pub const MAX_LEGACY_RUNTIME_IDENTITY_MISSING_FIELDS: usize = 16;
/// Maximum causal links carried by one runtime identity set.
pub const MAX_RUNTIME_CAUSAL_LINKS: usize = 32;

/// Validation error for a typed runtime identity.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeIdentityError {
    /// The identity was empty after trimming.
    #[error("runtime identity must not be empty")]
    Empty,
    /// The identity exceeded [`MAX_RUNTIME_ID_BYTES`].
    #[error("runtime identity exceeds {MAX_RUNTIME_ID_BYTES} bytes")]
    TooLong,
    /// The identity contained whitespace, control bytes, or unsupported punctuation.
    #[error("runtime identity contains unsupported characters")]
    InvalidCharacters,
    /// The identity set or legacy adapter used an unsupported schema version.
    #[error("runtime identity schema version {observed} is unsupported")]
    UnsupportedSchemaVersion { observed: u32 },
    /// The identity set carried too many causal relationships.
    #[error("runtime identity set exceeds {MAX_RUNTIME_CAUSAL_LINKS} causal links")]
    TooManyCausalLinks,
    /// A causal relation did not bind the typed identities required by its semantics.
    #[error("runtime identity causal relationship is invalid")]
    InvalidCausalLink,
    /// Legacy adaptation evidence used an invalid reason code or missing-field list.
    #[error("legacy runtime identity adapter metadata is invalid")]
    InvalidLegacyAdapterMetadata,
}

fn validate_identity(raw: &str) -> Result<String, RuntimeIdentityError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(RuntimeIdentityError::Empty);
    }
    if value.len() > MAX_RUNTIME_ID_BYTES {
        return Err(RuntimeIdentityError::TooLong);
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
    }) {
        return Err(RuntimeIdentityError::InvalidCharacters);
    }
    Ok(value.to_owned())
}

macro_rules! runtime_identity {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(value.as_str()).map_err(serde::de::Error::custom)
            }
        }

        impl $name {
            /// Parses and validates an opaque runtime identity.
            ///
            /// # Errors
            /// Returns [`RuntimeIdentityError`] when the value is empty, oversized,
            /// or contains characters that are unsafe for durable correlation fields.
            pub fn parse(raw: &str) -> Result<Self, RuntimeIdentityError> {
                validate_identity(raw).map(Self)
            }

            /// Returns the validated wire value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }

            /// Consumes the wrapper and returns its wire value.
            #[must_use]
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl TryFrom<&str> for $name {
            type Error = RuntimeIdentityError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = RuntimeIdentityError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value.as_str())
            }
        }
    };
}

runtime_identity!(
    /// Identifies one end-to-end trace across run continuations.
    RuntimeTraceId
);
runtime_identity!(
    /// Identifies the user-visible session owning runtime work.
    RuntimeSessionId
);
runtime_identity!(
    /// Identifies one run; continuations always receive a new value.
    RuntimeRunId
);
runtime_identity!(
    /// Identifies one provider or harness attempt within a run.
    RuntimeAttemptId
);
runtime_identity!(
    /// Identifies one model-proposed tool call.
    RuntimeToolProposalId
);
runtime_identity!(
    /// Identifies the stable side-effect execution behind a proposal.
    RuntimeToolExecutionId
);
runtime_identity!(
    /// Identifies the exact action covered by an approval decision.
    RuntimeApprovalSubjectId
);
runtime_identity!(
    /// Identifies one durable delivery intent bound to output identity.
    RuntimeDeliveryIntentId
);
runtime_identity!(
    /// Identifies one plugin invocation.
    RuntimePluginCallId
);
runtime_identity!(
    /// Identifies one provider-facing context projection.
    RuntimeContextProjectionId
);
runtime_identity!(
    /// Identifies one recovery action.
    RuntimeRecoveryActionId
);
runtime_identity!(
    /// Identifies a stable idempotent or side-effecting operation.
    RuntimeOperationId
);
runtime_identity!(
    /// Identifies one host-owned long-lived runtime instance.
    RuntimeInstanceId
);
runtime_identity!(
    /// Identifies one runtime event.
    RuntimeEventId
);
runtime_identity!(
    /// Identifies one runtime or process lease.
    RuntimeLeaseId
);

runtime_contract_enum! {
    /// Stable relation names used to describe runtime causality without overloading parent ids.
    pub enum RuntimeCausalLinkKind {
        RecoveredFrom => "recovered_from",
        ChildOf => "child_of",
        Supersedes => "supersedes",
        DeliversOutput => "delivers_output"
    }
}

runtime_contract_enum! {
    /// Identity domain attached to one side of a [`RuntimeCausalLink`].
    pub enum RuntimeIdentityKind {
        Run => "run",
        Attempt => "attempt",
        ToolProposal => "tool_proposal",
        ToolExecution => "tool_execution",
        ApprovalSubject => "approval_subject",
        DeliveryIntent => "delivery_intent",
        PluginCall => "plugin_call",
        ContextProjection => "context_projection",
        RecoveryAction => "recovery_action",
        Operation => "operation",
        RuntimeInstance => "runtime_instance",
        Event => "event"
    }
}

/// One validated identity used by a causal link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeIdentityRef {
    /// Identity domain.
    pub kind: RuntimeIdentityKind,
    /// Opaque validated identity value.
    pub value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeIdentityRefWire {
    kind: RuntimeIdentityKind,
    value: String,
}

impl<'de> Deserialize<'de> for RuntimeIdentityRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = RuntimeIdentityRefWire::deserialize(deserializer)?;
        Self::new(wire.kind, wire.value.as_str()).map_err(serde::de::Error::custom)
    }
}

impl RuntimeIdentityRef {
    /// Creates a validated causal identity reference.
    ///
    /// # Errors
    /// Returns [`RuntimeIdentityError`] when `value` is not a valid runtime identity.
    pub fn new(kind: RuntimeIdentityKind, value: &str) -> Result<Self, RuntimeIdentityError> {
        Ok(Self { kind, value: validate_identity(value)? })
    }
}

/// Stable causal relationship between two runtime identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCausalLink {
    /// Relationship semantics.
    pub relation: RuntimeCausalLinkKind,
    /// Identity that caused or superseded the target.
    pub source: RuntimeIdentityRef,
    /// Identity affected by the relationship.
    pub target: RuntimeIdentityRef,
}

/// Correlation identities carried by generation-aware runtime operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeIdentitySetV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// End-to-end trace identity.
    pub trace_id: RuntimeTraceId,
    /// Owning session identity.
    pub session_id: RuntimeSessionId,
    /// Current run identity.
    pub run_id: RuntimeRunId,
    /// Current write generation.
    pub generation: RuntimeGeneration,
    /// Provider or harness attempt identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<RuntimeAttemptId>,
    /// Tool proposal identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_proposal_id: Option<RuntimeToolProposalId>,
    /// Stable tool execution identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_execution_id: Option<RuntimeToolExecutionId>,
    /// Approval subject identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_subject_id: Option<RuntimeApprovalSubjectId>,
    /// Delivery intent identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_intent_id: Option<RuntimeDeliveryIntentId>,
    /// Plugin invocation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_call_id: Option<RuntimePluginCallId>,
    /// Provider context projection identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_projection_id: Option<RuntimeContextProjectionId>,
    /// Recovery action identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_action_id: Option<RuntimeRecoveryActionId>,
    /// Stable side-effect operation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<RuntimeOperationId>,
    /// Runtime instance identity for long-lived callbacks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_instance_id: Option<RuntimeInstanceId>,
    /// Causal relations known at this boundary. The field is always serialized so the Rust DTO
    /// remains byte-shape compatible with the required JSON Schema property.
    #[serde(default)]
    pub causal_links: Vec<RuntimeCausalLink>,
}

impl RuntimeIdentitySetV1 {
    /// Builds the minimum identity set for a run generation.
    pub fn for_run(
        trace_id: RuntimeTraceId,
        session_id: RuntimeSessionId,
        run_id: RuntimeRunId,
        generation: RuntimeGeneration,
    ) -> Self {
        Self {
            schema_version: RUNTIME_IDENTITY_SET_SCHEMA_VERSION,
            trace_id,
            session_id,
            run_id,
            generation,
            attempt_id: None,
            tool_proposal_id: None,
            tool_execution_id: None,
            approval_subject_id: None,
            delivery_intent_id: None,
            plugin_call_id: None,
            context_projection_id: None,
            recovery_action_id: None,
            operation_id: None,
            runtime_instance_id: None,
            causal_links: Vec::new(),
        }
    }

    /// Adapts legacy session/run strings and records why typed identity metadata was unavailable.
    ///
    /// # Errors
    /// Returns [`RuntimeIdentityError`] when either legacy identity is invalid.
    pub fn from_legacy_run(
        session_id: &str,
        run_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<(Self, LegacyRuntimeIdentityAdapter), RuntimeIdentityError> {
        let trace_id = RuntimeTraceId::parse(run_id)?;
        Ok((
            Self::for_run(
                trace_id,
                RuntimeSessionId::parse(session_id)?,
                RuntimeRunId::parse(run_id)?,
                generation,
            ),
            LegacyRuntimeIdentityAdapter {
                schema_version: LEGACY_RUNTIME_IDENTITY_ADAPTER_SCHEMA_VERSION,
                reason_code: "runtime.identity.legacy_adapter_used".to_owned(),
                missing_fields: vec!["attempt_id".to_owned(), "operation_id".to_owned()],
            },
        ))
    }

    /// Binds a delivery intent to the exact durable event containing its output.
    ///
    /// The delivery identity remains stable when the run generation changes;
    /// the output event, rather than the generation, is the dedupe authority.
    ///
    /// # Errors
    /// Returns [`RuntimeIdentityError::TooManyCausalLinks`] when the bounded
    /// identity set has no room for the required relation.
    pub fn bind_delivery_intent(
        &mut self,
        delivery_intent_id: RuntimeDeliveryIntentId,
        output_event_id: &RuntimeEventId,
    ) -> Result<(), RuntimeIdentityError> {
        self.causal_links.retain(|link| {
            link.relation != RuntimeCausalLinkKind::DeliversOutput
                || link.source.kind != RuntimeIdentityKind::DeliveryIntent
        });
        if self.causal_links.len() >= MAX_RUNTIME_CAUSAL_LINKS {
            return Err(RuntimeIdentityError::TooManyCausalLinks);
        }
        self.causal_links.push(RuntimeCausalLink {
            relation: RuntimeCausalLinkKind::DeliversOutput,
            source: RuntimeIdentityRef::new(
                RuntimeIdentityKind::DeliveryIntent,
                delivery_intent_id.as_str(),
            )?,
            target: RuntimeIdentityRef::new(RuntimeIdentityKind::Event, output_event_id.as_str())?,
        });
        self.delivery_intent_id = Some(delivery_intent_id);
        Ok(())
    }

    /// Validates the identity-set schema version.
    ///
    /// # Errors
    /// Returns [`RuntimeIdentityError::UnsupportedSchemaVersion`] for unknown versions.
    pub fn validate(&self) -> Result<(), RuntimeIdentityError> {
        if self.schema_version != RUNTIME_IDENTITY_SET_SCHEMA_VERSION {
            return Err(RuntimeIdentityError::UnsupportedSchemaVersion {
                observed: self.schema_version,
            });
        }
        if self.causal_links.len() > MAX_RUNTIME_CAUSAL_LINKS {
            return Err(RuntimeIdentityError::TooManyCausalLinks);
        }
        let matching_delivery_links = self
            .delivery_intent_id
            .as_ref()
            .map(|delivery_intent_id| {
                self.causal_links
                    .iter()
                    .filter(|link| {
                        link.relation == RuntimeCausalLinkKind::DeliversOutput
                            && link.source.kind == RuntimeIdentityKind::DeliveryIntent
                            && link.source.value == delivery_intent_id.as_str()
                            && link.target.kind == RuntimeIdentityKind::Event
                    })
                    .count()
            })
            .unwrap_or_default();
        let has_unbound_delivery_link = self.causal_links.iter().any(|link| {
            link.relation == RuntimeCausalLinkKind::DeliversOutput
                && (self.delivery_intent_id.as_ref().is_none_or(|delivery_intent_id| {
                    link.source.kind != RuntimeIdentityKind::DeliveryIntent
                        || link.source.value != delivery_intent_id.as_str()
                }) || link.target.kind != RuntimeIdentityKind::Event)
        });
        if matching_delivery_links != usize::from(self.delivery_intent_id.is_some())
            || has_unbound_delivery_link
        {
            return Err(RuntimeIdentityError::InvalidCausalLink);
        }
        Ok(())
    }

    /// Produces domain-separated SHA-256 diagnostics without exposing raw identities.
    #[must_use]
    pub fn redacted_diagnostics(&self) -> RuntimeIdentityDiagnosticsV1 {
        let mut hashes = BTreeMap::new();
        insert_hash(&mut hashes, "trace_id", self.trace_id.as_str());
        insert_hash(&mut hashes, "session_id", self.session_id.as_str());
        insert_hash(&mut hashes, "run_id", self.run_id.as_str());
        if let Some(value) = &self.attempt_id {
            insert_hash(&mut hashes, "attempt_id", value.as_str());
        }
        if let Some(value) = &self.tool_proposal_id {
            insert_hash(&mut hashes, "tool_proposal_id", value.as_str());
        }
        if let Some(value) = &self.tool_execution_id {
            insert_hash(&mut hashes, "tool_execution_id", value.as_str());
        }
        if let Some(value) = &self.approval_subject_id {
            insert_hash(&mut hashes, "approval_subject_id", value.as_str());
        }
        if let Some(value) = &self.delivery_intent_id {
            insert_hash(&mut hashes, "delivery_intent_id", value.as_str());
        }
        if let Some(value) = &self.plugin_call_id {
            insert_hash(&mut hashes, "plugin_call_id", value.as_str());
        }
        if let Some(value) = &self.context_projection_id {
            insert_hash(&mut hashes, "context_projection_id", value.as_str());
        }
        if let Some(value) = &self.recovery_action_id {
            insert_hash(&mut hashes, "recovery_action_id", value.as_str());
        }
        if let Some(value) = &self.operation_id {
            insert_hash(&mut hashes, "operation_id", value.as_str());
        }
        if let Some(value) = &self.runtime_instance_id {
            insert_hash(&mut hashes, "runtime_instance_id", value.as_str());
        }
        let causal_links =
            self.causal_links.iter().map(RuntimeCausalLinkDiagnosticsV1::from_link).collect();
        RuntimeIdentityDiagnosticsV1 {
            schema_version: 1,
            generation: self.generation,
            identity_hashes: hashes,
            causal_link_count: self.causal_links.len(),
            causal_links,
            redaction_level: "hash_only".to_owned(),
        }
    }
}

fn insert_hash(hashes: &mut BTreeMap<String, String>, domain: &str, value: &str) {
    hashes.insert(domain.to_owned(), identity_hash(domain, value));
}

fn identity_hash(domain: &str, value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"palyra.runtime.identity.v1\0");
    digest.update(domain.as_bytes());
    digest.update(b"\0");
    digest.update(value.as_bytes());
    hex::encode(digest.finalize())
}

/// Diagnostic projection emitted when legacy records lack typed identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyRuntimeIdentityAdapter {
    /// Projection schema version.
    pub schema_version: u32,
    /// Stable reason code explaining the fallback.
    pub reason_code: String,
    /// Typed fields unavailable in the source record.
    pub missing_fields: Vec<String>,
}

impl LegacyRuntimeIdentityAdapter {
    /// Removes fields that a host projection populated from validated legacy metadata.
    ///
    /// Returns `None` when no identity remains unavailable, so callers omit the adapter rather
    /// than persisting empty or contradictory migration evidence.
    #[must_use]
    pub fn reconcile_with_identities(mut self, identities: &RuntimeIdentitySetV1) -> Option<Self> {
        self.missing_fields.retain(|field| match field.as_str() {
            "attempt_id" => identities.attempt_id.is_none(),
            "tool_proposal_id" => identities.tool_proposal_id.is_none(),
            "tool_execution_id" => identities.tool_execution_id.is_none(),
            "approval_subject_id" => identities.approval_subject_id.is_none(),
            "delivery_intent_id" => identities.delivery_intent_id.is_none(),
            "plugin_call_id" => identities.plugin_call_id.is_none(),
            "context_projection_id" => identities.context_projection_id.is_none(),
            "recovery_action_id" => identities.recovery_action_id.is_none(),
            "operation_id" => identities.operation_id.is_none(),
            "runtime_instance_id" => identities.runtime_instance_id.is_none(),
            _ => true,
        });
        (!self.missing_fields.is_empty()).then_some(self)
    }

    /// Validates bounded legacy identity adaptation evidence before persistence.
    ///
    /// # Errors
    /// Returns [`RuntimeIdentityError`] when the schema version, reason code, or missing-field
    /// vocabulary is unsupported, duplicated, or outside the public contract bounds.
    pub fn validate(&self) -> Result<(), RuntimeIdentityError> {
        if self.schema_version != LEGACY_RUNTIME_IDENTITY_ADAPTER_SCHEMA_VERSION {
            return Err(RuntimeIdentityError::UnsupportedSchemaVersion {
                observed: self.schema_version,
            });
        }
        if self.reason_code != "runtime.identity.legacy_adapter_used"
            || self.missing_fields.is_empty()
            || self.missing_fields.len() > MAX_LEGACY_RUNTIME_IDENTITY_MISSING_FIELDS
        {
            return Err(RuntimeIdentityError::InvalidLegacyAdapterMetadata);
        }
        let mut observed_fields = std::collections::BTreeSet::new();
        for field in &self.missing_fields {
            if !is_legacy_runtime_identity_field(field.as_str())
                || !observed_fields.insert(field.as_str())
            {
                return Err(RuntimeIdentityError::InvalidLegacyAdapterMetadata);
            }
        }
        Ok(())
    }
}

fn is_legacy_runtime_identity_field(field: &str) -> bool {
    matches!(
        field,
        "attempt_id"
            | "tool_proposal_id"
            | "tool_execution_id"
            | "approval_subject_id"
            | "delivery_intent_id"
            | "plugin_call_id"
            | "context_projection_id"
            | "recovery_action_id"
            | "operation_id"
            | "runtime_instance_id"
    )
}

/// Hash-only identity projection safe for diagnostics and traces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeIdentityDiagnosticsV1 {
    /// Projection schema version.
    pub schema_version: u32,
    /// Active generation.
    pub generation: RuntimeGeneration,
    /// Domain-separated identity hashes.
    pub identity_hashes: BTreeMap<String, String>,
    /// Number of causal links without their raw identities.
    pub causal_link_count: usize,
    /// Bounded causal relationships with hash-only endpoints.
    #[serde(default)]
    pub causal_links: Vec<RuntimeCausalLinkDiagnosticsV1>,
    /// Redaction posture; always `hash_only` in this version.
    pub redaction_level: String,
}

/// Hash-only diagnostic projection of one causal relationship.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCausalLinkDiagnosticsV1 {
    /// Stable causal relationship kind.
    pub relation: RuntimeCausalLinkKind,
    /// Source identity domain.
    pub source_kind: RuntimeIdentityKind,
    /// Domain-separated source identity hash.
    pub source_sha256: String,
    /// Target identity domain.
    pub target_kind: RuntimeIdentityKind,
    /// Domain-separated target identity hash.
    pub target_sha256: String,
}

impl RuntimeCausalLinkDiagnosticsV1 {
    fn from_link(link: &RuntimeCausalLink) -> Self {
        let source_domain = format!("causal.source.{}", link.source.kind.as_str());
        let target_domain = format!("causal.target.{}", link.target.kind.as_str());
        Self {
            relation: link.relation,
            source_kind: link.source.kind,
            source_sha256: identity_hash(source_domain.as_str(), link.source.value.as_str()),
            target_kind: link.target.kind,
            target_sha256: identity_hash(target_domain.as_str(), link.target.value.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{de::DeserializeOwned, Serialize};
    use std::fmt::Debug;

    fn assert_identity_round_trip<T>(identity: T)
    where
        T: Debug + PartialEq + Serialize + DeserializeOwned,
    {
        let encoded = serde_json::to_string(&identity).expect("identity should serialize");
        let decoded: T =
            serde_json::from_str(encoded.as_str()).expect("identity should deserialize");
        assert_eq!(decoded, identity);
    }

    #[test]
    fn every_typed_identity_round_trips() {
        assert_identity_round_trip(RuntimeTraceId::parse("trace_01").expect("trace id"));
        assert_identity_round_trip(RuntimeSessionId::parse("session_01").expect("session id"));
        assert_identity_round_trip(RuntimeRunId::parse("run_01").expect("run id"));
        assert_identity_round_trip(RuntimeAttemptId::parse("attempt_01").expect("attempt id"));
        assert_identity_round_trip(
            RuntimeToolProposalId::parse("proposal_01").expect("tool proposal id"),
        );
        assert_identity_round_trip(
            RuntimeToolExecutionId::parse("execution_01").expect("tool execution id"),
        );
        assert_identity_round_trip(
            RuntimeApprovalSubjectId::parse("approval_01").expect("approval subject id"),
        );
        assert_identity_round_trip(
            RuntimeDeliveryIntentId::parse("delivery_01").expect("delivery intent id"),
        );
        assert_identity_round_trip(
            RuntimePluginCallId::parse("plugin_01").expect("plugin call id"),
        );
        assert_identity_round_trip(
            RuntimeContextProjectionId::parse("context_01").expect("context projection id"),
        );
        assert_identity_round_trip(
            RuntimeRecoveryActionId::parse("recovery_01").expect("recovery action id"),
        );
        assert_identity_round_trip(
            RuntimeOperationId::parse("operation_01").expect("operation id"),
        );
        assert_identity_round_trip(
            RuntimeInstanceId::parse("instance_01").expect("runtime instance id"),
        );
        assert_identity_round_trip(RuntimeEventId::parse("event_01").expect("event id"));
        assert_identity_round_trip(RuntimeLeaseId::parse("lease_01").expect("lease id"));
    }

    #[test]
    fn identity_rejects_whitespace_and_separates_domains() {
        assert_eq!(RuntimeRunId::parse("run id"), Err(RuntimeIdentityError::InvalidCharacters));
        assert!(serde_json::from_str::<RuntimeRunId>(r#""run id""#).is_err());
        assert!(serde_json::from_str::<RuntimeIdentityRef>(r#"{"kind":"run","value":"run id"}"#)
            .is_err());
        let generation = RuntimeGeneration::new(1).expect("generation should validate");
        let (identities, adapter) =
            RuntimeIdentitySetV1::from_legacy_run("session_01", "run_01", generation)
                .expect("legacy ids should adapt");
        let diagnostics = identities.redacted_diagnostics();
        assert_ne!(
            diagnostics.identity_hashes.get("trace_id"),
            diagnostics.identity_hashes.get("run_id")
        );
        assert_eq!(adapter.reason_code, "runtime.identity.legacy_adapter_used");
        adapter.validate().expect("legacy adapter should validate");
    }

    #[test]
    fn legacy_identity_adapter_reconciles_host_enriched_fields() {
        let generation = RuntimeGeneration::new(1).expect("generation should validate");
        let (mut identities, adapter) =
            RuntimeIdentitySetV1::from_legacy_run("session_01", "run_01", generation)
                .expect("legacy ids should adapt");
        identities.operation_id =
            Some(RuntimeOperationId::parse("operation_01").expect("operation id"));
        let reconciled = adapter
            .reconcile_with_identities(&identities)
            .expect("attempt identity should remain unavailable");
        assert_eq!(reconciled.missing_fields, vec!["attempt_id"]);
        identities.attempt_id = Some(RuntimeAttemptId::parse("attempt_01").expect("attempt id"));
        assert_eq!(reconciled.reconcile_with_identities(&identities), None);
    }

    #[test]
    fn legacy_identity_adapter_rejects_unbounded_or_sensitive_field_names() {
        let mut adapter = LegacyRuntimeIdentityAdapter {
            schema_version: LEGACY_RUNTIME_IDENTITY_ADAPTER_SCHEMA_VERSION,
            reason_code: "runtime.identity.legacy_adapter_used".to_owned(),
            missing_fields: vec!["raw prompt".to_owned()],
        };
        assert_eq!(adapter.validate(), Err(RuntimeIdentityError::InvalidLegacyAdapterMetadata));

        adapter.missing_fields = vec!["future_identity_id".to_owned()];
        assert_eq!(adapter.validate(), Err(RuntimeIdentityError::InvalidLegacyAdapterMetadata));

        adapter.missing_fields = vec!["attempt_id".to_owned(); 2];
        assert_eq!(adapter.validate(), Err(RuntimeIdentityError::InvalidLegacyAdapterMetadata));
    }

    #[test]
    fn legacy_identity_adapter_requires_the_canonical_reason_code() {
        let adapter = LegacyRuntimeIdentityAdapter {
            schema_version: LEGACY_RUNTIME_IDENTITY_ADAPTER_SCHEMA_VERSION,
            reason_code: "runtime.identity.other_adapter".to_owned(),
            missing_fields: vec!["attempt_id".to_owned()],
        };
        assert_eq!(adapter.validate(), Err(RuntimeIdentityError::InvalidLegacyAdapterMetadata));
    }

    #[test]
    fn identity_set_rejects_unbounded_causal_links() {
        let mut identities = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01").expect("trace id"),
            RuntimeSessionId::parse("session_01").expect("session id"),
            RuntimeRunId::parse("run_01").expect("run id"),
            RuntimeGeneration::new(1).expect("generation"),
        );
        let identity =
            RuntimeIdentityRef::new(RuntimeIdentityKind::Run, "run_01").expect("identity ref");
        identities.causal_links = (0..=MAX_RUNTIME_CAUSAL_LINKS)
            .map(|_| RuntimeCausalLink {
                relation: RuntimeCausalLinkKind::ChildOf,
                source: identity.clone(),
                target: identity.clone(),
            })
            .collect();

        assert_eq!(identities.validate(), Err(RuntimeIdentityError::TooManyCausalLinks));
    }

    #[test]
    fn identity_set_serializes_required_empty_causal_links() {
        let identities = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01").expect("trace id"),
            RuntimeSessionId::parse("session_01").expect("session id"),
            RuntimeRunId::parse("run_01").expect("run id"),
            RuntimeGeneration::new(1).expect("generation"),
        );

        let encoded = serde_json::to_value(&identities).expect("identity set should serialize");
        assert_eq!(encoded.get("causal_links"), Some(&serde_json::json!([])));
    }

    #[test]
    fn provider_retry_preserves_tool_execution_identity() {
        let generation = RuntimeGeneration::new(2).expect("generation should validate");
        let mut identities = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01").expect("trace id"),
            RuntimeSessionId::parse("session_01").expect("session id"),
            RuntimeRunId::parse("run_01").expect("run id"),
            generation,
        );
        identities.attempt_id = Some(RuntimeAttemptId::parse("attempt_01").expect("attempt id"));
        identities.tool_execution_id =
            Some(RuntimeToolExecutionId::parse("execution_01").expect("execution id"));
        let execution_id = identities.tool_execution_id.clone();
        identities.attempt_id = Some(RuntimeAttemptId::parse("attempt_02").expect("attempt id"));
        assert_eq!(identities.tool_execution_id, execution_id);
    }

    #[test]
    fn delivery_intent_dedupes_by_output_across_generations() {
        let delivery_id =
            RuntimeDeliveryIntentId::parse("delivery_01").expect("delivery intent id");
        let output_event_id = RuntimeEventId::parse("output_event_01").expect("output event id");
        let mut first_generation = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01").expect("trace id"),
            RuntimeSessionId::parse("session_01").expect("session id"),
            RuntimeRunId::parse("run_01").expect("run id"),
            RuntimeGeneration::new(1).expect("generation"),
        );
        first_generation
            .bind_delivery_intent(delivery_id.clone(), &output_event_id)
            .expect("delivery binding");
        first_generation
            .bind_delivery_intent(delivery_id.clone(), &output_event_id)
            .expect("duplicate delivery binding");
        first_generation.validate().expect("first generation identity set");
        assert_eq!(
            first_generation
                .causal_links
                .iter()
                .filter(|link| link.relation == RuntimeCausalLinkKind::DeliversOutput)
                .count(),
            1
        );

        let mut continuation = first_generation.clone();
        continuation.generation = RuntimeGeneration::new(2).expect("generation");
        continuation.validate().expect("continuation identity set");

        assert_eq!(continuation.delivery_intent_id, Some(delivery_id));
        assert_eq!(continuation.causal_links, first_generation.causal_links);
    }

    #[test]
    fn delivery_intent_without_output_binding_is_rejected() {
        let mut identities = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01").expect("trace id"),
            RuntimeSessionId::parse("session_01").expect("session id"),
            RuntimeRunId::parse("run_01").expect("run id"),
            RuntimeGeneration::new(1).expect("generation"),
        );
        identities.delivery_intent_id =
            Some(RuntimeDeliveryIntentId::parse("delivery_01").expect("delivery intent id"));

        assert_eq!(identities.validate(), Err(RuntimeIdentityError::InvalidCausalLink));
    }

    #[test]
    fn continuation_keeps_trace_and_reports_recovery_relation_without_raw_ids() {
        let trace_id = RuntimeTraceId::parse("trace_01").expect("trace id");
        let previous_run_id = RuntimeRunId::parse("run_01").expect("previous run id");
        let current_run_id = RuntimeRunId::parse("run_02").expect("current run id");
        let mut identities = RuntimeIdentitySetV1::for_run(
            trace_id.clone(),
            RuntimeSessionId::parse("session_01").expect("session id"),
            current_run_id.clone(),
            RuntimeGeneration::new(2).expect("generation"),
        );
        identities.causal_links.push(RuntimeCausalLink {
            relation: RuntimeCausalLinkKind::RecoveredFrom,
            source: RuntimeIdentityRef::new(RuntimeIdentityKind::Run, current_run_id.as_str())
                .expect("current run ref"),
            target: RuntimeIdentityRef::new(RuntimeIdentityKind::Run, previous_run_id.as_str())
                .expect("previous run ref"),
        });

        let diagnostics = identities.redacted_diagnostics();

        assert_eq!(identities.trace_id, trace_id);
        assert_ne!(identities.run_id, previous_run_id);
        assert_eq!(diagnostics.causal_link_count, 1);
        assert_eq!(diagnostics.causal_links.len(), 1);
        let recovery = &diagnostics.causal_links[0];
        assert_eq!(recovery.relation, RuntimeCausalLinkKind::RecoveredFrom);
        assert_eq!(recovery.source_kind, RuntimeIdentityKind::Run);
        assert_eq!(recovery.target_kind, RuntimeIdentityKind::Run);
        assert_ne!(recovery.source_sha256, recovery.target_sha256);
        let encoded = serde_json::to_string(&diagnostics).expect("diagnostics should serialize");
        assert!(!encoded.contains(current_run_id.as_str()));
        assert!(!encoded.contains(previous_run_id.as_str()));
    }
}
