//! Host-validated trusted tool registration and conformance evidence.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    admit_external_tool_descriptor, McpAdmittedToolDescriptor, McpCatalogAuthority,
    McpCatalogEpochPin, McpConformanceReportV1, McpDescriptorAdmissionError,
    McpDescriptorAdmissionPolicy, McpDescriptorTrustVerifier, McpExternalToolDescriptor,
    TrustedExternalToolRegistrationRequest,
};

/// Activation state for one host-validated external descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTrustedToolActivationState {
    /// Descriptor is valid but requires an explicit approval decision.
    PendingApproval,
    /// Exact descriptor digest is approved for catalog publication.
    Active,
    /// Operator or policy explicitly disabled the descriptor.
    Disabled,
}

impl McpTrustedToolActivationState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PendingApproval => "pending_approval",
            Self::Active => "active",
            Self::Disabled => "disabled",
        }
    }
}

/// Durable trusted external tool head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpTrustedToolRecordV1 {
    /// Stable record schema.
    pub schema_version: u32,
    /// Durable MCP server identity.
    pub server_id: String,
    /// Namespaced external tool name.
    pub tool_name: String,
    /// Runtime generation that supplied the descriptor.
    pub runtime_generation: u64,
    /// Catalog epoch that supplied the descriptor.
    pub catalog_epoch: u64,
    /// Validated bounded descriptor.
    pub descriptor: McpExternalToolDescriptor,
    /// Canonical descriptor digest.
    pub descriptor_sha256: String,
    /// Host-verified issuer.
    pub verified_issuer_id: String,
    /// Approval/activation state.
    pub activation: McpTrustedToolActivationState,
    /// Digest approved by the host; present only for active state.
    pub approved_descriptor_sha256: Option<String>,
    /// Monotonic compare-and-swap revision.
    pub revision: u64,
    /// Stable transition reason.
    pub reason_code: String,
    /// Creation time.
    pub created_at_unix_ms: i64,
    /// Last transition time.
    pub updated_at_unix_ms: i64,
}

impl McpTrustedToolRecordV1 {
    /// Validates durable registration and approval invariants.
    ///
    /// # Errors
    /// Returns [`McpTrustedToolRegistryError::InvalidRecord`] for malformed state.
    pub fn validate(&self) -> Result<(), McpTrustedToolRegistryError> {
        let approval_valid = match self.activation {
            McpTrustedToolActivationState::Active => {
                self.approved_descriptor_sha256.as_deref() == Some(self.descriptor_sha256.as_str())
            }
            McpTrustedToolActivationState::PendingApproval
            | McpTrustedToolActivationState::Disabled => self.approved_descriptor_sha256.is_none(),
        };
        if self.schema_version != 1
            || !valid_identifier(&self.server_id)
            || !valid_tool_name(&self.tool_name)
            || self.tool_name != self.descriptor.name
            || self.runtime_generation == 0
            || self.catalog_epoch == 0
            || !valid_sha256(&self.descriptor_sha256)
            || !valid_identifier(&self.verified_issuer_id)
            || !valid_reason_code(&self.reason_code)
            || self.created_at_unix_ms <= 0
            || self.updated_at_unix_ms < self.created_at_unix_ms
            || !approval_valid
        {
            return Err(McpTrustedToolRegistryError::InvalidRecord);
        }
        Ok(())
    }

    fn admitted(&self) -> McpAdmittedToolDescriptor {
        McpAdmittedToolDescriptor {
            server_id: self.server_id.clone(),
            runtime_generation: self.runtime_generation,
            catalog_epoch: self.catalog_epoch,
            descriptor: self.descriptor.clone(),
            descriptor_sha256: self.descriptor_sha256.clone(),
            verified_issuer_id: self.verified_issuer_id.clone(),
            requires_reapproval: false,
        }
    }
}

/// Durable trusted-tool and conformance evidence boundary.
#[async_trait]
pub trait McpSecurityEvidenceStore: Send + Sync {
    /// Loads one trusted descriptor head.
    async fn load_trusted_tool(
        &self,
        server_id: &str,
        tool_name: &str,
    ) -> Result<Option<McpTrustedToolRecordV1>, McpSecurityEvidenceStoreError>;

    /// Atomically commits a trusted descriptor head and immutable event.
    async fn persist_trusted_tool(
        &self,
        expected_revision: Option<u64>,
        record: &McpTrustedToolRecordV1,
    ) -> Result<(), McpSecurityEvidenceStoreError>;

    /// Persists one idempotent conformance report.
    async fn persist_conformance_report(
        &self,
        report: &McpConformanceReportV1,
    ) -> Result<(), McpSecurityEvidenceStoreError>;

    /// Loads the latest report for operator qualification.
    async fn latest_conformance_report(
        &self,
        server_id: &str,
    ) -> Result<Option<McpConformanceReportV1>, McpSecurityEvidenceStoreError>;
}

/// Durable MCP security evidence failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpSecurityEvidenceStoreError {
    /// Head compare-and-swap failed.
    #[error("mcp trusted tool revision conflict")]
    RevisionConflict {
        /// Caller-supplied expected revision.
        expected: Option<u64>,
        /// Current durable revision.
        actual: Option<u64>,
    },
    /// Stored security evidence is corrupt.
    #[error("corrupt mcp security evidence: {reason_code}")]
    Corrupt {
        /// Stable corruption reason.
        reason_code: String,
    },
    /// Security evidence storage is unavailable.
    #[error("mcp security evidence unavailable: {reason_code}")]
    Unavailable {
        /// Stable storage reason.
        reason_code: String,
    },
}

/// Host approval command for an exact descriptor revision and digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpTrustedToolApproval {
    /// Durable server identity.
    pub server_id: String,
    /// Namespaced tool name.
    pub tool_name: String,
    /// Current head revision.
    pub expected_revision: u64,
    /// Exact digest being approved or denied.
    pub descriptor_sha256: String,
    /// True activates the descriptor; false disables it.
    pub approved: bool,
    /// Stable host reason.
    pub reason_code: String,
    /// Decision time.
    pub decided_at_unix_ms: i64,
}

/// Host service for trusted external descriptor registration.
pub struct McpTrustedToolRegistry {
    authority: Arc<McpCatalogAuthority>,
    policy: McpDescriptorAdmissionPolicy,
    verifier: Arc<dyn McpDescriptorTrustVerifier>,
    store: Arc<dyn McpSecurityEvidenceStore>,
}

impl std::fmt::Debug for McpTrustedToolRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("McpTrustedToolRegistry").finish_non_exhaustive()
    }
}

impl McpTrustedToolRegistry {
    /// Creates a host-authoritative trusted descriptor registry.
    #[must_use]
    pub fn new(
        authority: Arc<McpCatalogAuthority>,
        policy: McpDescriptorAdmissionPolicy,
        verifier: Arc<dyn McpDescriptorTrustVerifier>,
        store: Arc<dyn McpSecurityEvidenceStore>,
    ) -> Self {
        Self { authority, policy, verifier, store }
    }

    /// Validates, generation-fences, and durably records one descriptor.
    ///
    /// A new or changed digest always enters `pending_approval`; identical
    /// descriptors preserve the prior activation state.
    ///
    /// # Errors
    /// Returns a stale pin, admission, durable state, or storage error.
    pub async fn register(
        &self,
        pin: &McpCatalogEpochPin,
        request: TrustedExternalToolRegistrationRequest,
        now_unix_ms: i64,
    ) -> Result<McpTrustedToolRecordV1, McpTrustedToolRegistryError> {
        if now_unix_ms <= 0 {
            return Err(McpTrustedToolRegistryError::InvalidRecord);
        }
        let current_pin = self.authority.pin().ok_or(McpTrustedToolRegistryError::StaleCatalog)?;
        if current_pin != *pin {
            return Err(McpTrustedToolRegistryError::StaleCatalog);
        }
        pin.validate(&request.server_id, request.runtime_generation, request.catalog_epoch)
            .map_err(|_| McpTrustedToolRegistryError::StaleCatalog)?;
        let previous =
            self.store.load_trusted_tool(&request.server_id, &request.descriptor.name).await?;
        let previous_admitted = previous.as_ref().map(McpTrustedToolRecordV1::admitted);
        let admitted = admit_external_tool_descriptor(
            request,
            previous_admitted.as_ref(),
            &self.policy,
            self.verifier.as_ref(),
        )?;
        let (activation, approved_descriptor_sha256) = if admitted.requires_reapproval {
            (McpTrustedToolActivationState::PendingApproval, None)
        } else {
            let previous = previous.as_ref().ok_or(McpTrustedToolRegistryError::InvalidRecord)?;
            (previous.activation, previous.approved_descriptor_sha256.clone())
        };
        let record = McpTrustedToolRecordV1 {
            schema_version: 1,
            server_id: admitted.server_id,
            tool_name: admitted.descriptor.name.clone(),
            runtime_generation: admitted.runtime_generation,
            catalog_epoch: admitted.catalog_epoch,
            descriptor: admitted.descriptor,
            descriptor_sha256: admitted.descriptor_sha256,
            verified_issuer_id: admitted.verified_issuer_id,
            activation,
            approved_descriptor_sha256,
            revision: previous.as_ref().map_or(0, |record| record.revision.saturating_add(1)),
            reason_code: if admitted.requires_reapproval {
                "mcp.runtime.trusted_tool.pending_approval".to_owned()
            } else {
                "mcp.runtime.trusted_tool.refreshed".to_owned()
            },
            created_at_unix_ms: previous
                .as_ref()
                .map_or(now_unix_ms, |record| record.created_at_unix_ms),
            updated_at_unix_ms: now_unix_ms,
        };
        record.validate()?;
        self.store
            .persist_trusted_tool(previous.as_ref().map(|record| record.revision), &record)
            .await?;
        Ok(record)
    }

    /// Applies an explicit approval or disable decision to the current descriptor.
    ///
    /// # Errors
    /// Returns stale catalog, digest, revision, validation, or storage errors.
    pub async fn decide(
        &self,
        decision: &McpTrustedToolApproval,
    ) -> Result<McpTrustedToolRecordV1, McpTrustedToolRegistryError> {
        if !valid_identifier(&decision.server_id)
            || !valid_tool_name(&decision.tool_name)
            || !valid_sha256(&decision.descriptor_sha256)
            || !valid_reason_code(&decision.reason_code)
            || decision.decided_at_unix_ms <= 0
        {
            return Err(McpTrustedToolRegistryError::InvalidRecord);
        }
        let pin = self.authority.pin().ok_or(McpTrustedToolRegistryError::StaleCatalog)?;
        let current = self
            .store
            .load_trusted_tool(&decision.server_id, &decision.tool_name)
            .await?
            .ok_or(McpTrustedToolRegistryError::NotFound)?;
        if current.revision != decision.expected_revision
            || current.descriptor_sha256 != decision.descriptor_sha256
            || current.runtime_generation != pin.runtime_generation
            || current.catalog_epoch != pin.catalog_epoch
        {
            return Err(McpTrustedToolRegistryError::StaleApproval);
        }
        if decision.approved {
            let report = self
                .store
                .latest_conformance_report(&decision.server_id)
                .await?
                .ok_or(McpTrustedToolRegistryError::ConformanceRequired)?;
            if report.runtime_generation != pin.runtime_generation
                || report.catalog_epoch != pin.catalog_epoch
                || !report.qualifies_for_production()
            {
                return Err(McpTrustedToolRegistryError::ConformanceRequired);
            }
        }
        let next_revision =
            current.revision.checked_add(1).ok_or(McpTrustedToolRegistryError::InvalidRecord)?;
        let mut next = current.clone();
        next.activation = if decision.approved {
            McpTrustedToolActivationState::Active
        } else {
            McpTrustedToolActivationState::Disabled
        };
        next.approved_descriptor_sha256 =
            decision.approved.then(|| current.descriptor_sha256.clone());
        next.revision = next_revision;
        next.reason_code = decision.reason_code.clone();
        next.updated_at_unix_ms = decision.decided_at_unix_ms;
        next.validate()?;
        self.store.persist_trusted_tool(Some(current.revision), &next).await?;
        Ok(next)
    }

    /// Rebinds a descriptor head to the host-created catalog epoch that
    /// applied its current activation state.
    pub async fn rebind_catalog_epoch(
        &self,
        previous: &McpTrustedToolRecordV1,
        pin: &McpCatalogEpochPin,
        now_unix_ms: i64,
    ) -> Result<McpTrustedToolRecordV1, McpTrustedToolRegistryError> {
        let current_pin = self.authority.pin().ok_or(McpTrustedToolRegistryError::StaleCatalog)?;
        if current_pin != *pin
            || current_pin.runtime_generation != previous.runtime_generation
            || current_pin.catalog_epoch <= previous.catalog_epoch
            || now_unix_ms < previous.updated_at_unix_ms
        {
            return Err(McpTrustedToolRegistryError::StaleCatalog);
        }
        let current = self
            .store
            .load_trusted_tool(&previous.server_id, &previous.tool_name)
            .await?
            .ok_or(McpTrustedToolRegistryError::NotFound)?;
        if current != *previous {
            return Err(McpTrustedToolRegistryError::StaleApproval);
        }
        let mut next = current.clone();
        next.catalog_epoch = current_pin.catalog_epoch;
        next.revision =
            current.revision.checked_add(1).ok_or(McpTrustedToolRegistryError::InvalidRecord)?;
        next.reason_code = match current.activation {
            McpTrustedToolActivationState::PendingApproval => {
                "mcp.runtime.trusted_tool.catalog_withdrawn"
            }
            McpTrustedToolActivationState::Active => "mcp.runtime.trusted_tool.catalog_activated",
            McpTrustedToolActivationState::Disabled => "mcp.runtime.trusted_tool.catalog_disabled",
        }
        .to_owned();
        next.updated_at_unix_ms = now_unix_ms;
        next.validate()?;
        self.store.persist_trusted_tool(Some(current.revision), &next).await?;
        Ok(next)
    }

    /// Persists a report only when it matches the current generation and epoch.
    ///
    /// # Errors
    /// Returns invalid report, stale catalog, or storage errors.
    pub async fn record_conformance(
        &self,
        report: &McpConformanceReportV1,
    ) -> Result<(), McpTrustedToolRegistryError> {
        report.validate().map_err(|_| McpTrustedToolRegistryError::InvalidConformance)?;
        let pin = self.authority.pin().ok_or(McpTrustedToolRegistryError::StaleCatalog)?;
        pin.validate(&report.server_id, report.runtime_generation, report.catalog_epoch)
            .map_err(|_| McpTrustedToolRegistryError::StaleCatalog)?;
        self.store.persist_conformance_report(report).await?;
        Ok(())
    }
}

/// Trusted external descriptor registry failure.
#[derive(Debug, Error)]
pub enum McpTrustedToolRegistryError {
    /// Catalog generation or epoch is not current.
    #[error("stale mcp trusted tool catalog")]
    StaleCatalog,
    /// Trusted descriptor head does not exist.
    #[error("mcp trusted tool not found")]
    NotFound,
    /// Approval revision, digest, generation, or epoch is stale.
    #[error("stale mcp trusted tool approval")]
    StaleApproval,
    /// Durable trusted descriptor state is invalid.
    #[error("invalid mcp trusted tool record")]
    InvalidRecord,
    /// Conformance report is invalid.
    #[error("invalid mcp conformance evidence")]
    InvalidConformance,
    /// The active generation and catalog epoch lack a complete passing report.
    #[error("mcp trusted tool activation requires current passing conformance")]
    ConformanceRequired,
    /// Descriptor admission failed.
    #[error(transparent)]
    Admission(#[from] McpDescriptorAdmissionError),
    /// Durable security evidence failed.
    #[error(transparent)]
    Store(#[from] McpSecurityEvidenceStoreError),
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 256
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
}

fn valid_tool_name(value: &str) -> bool {
    valid_identifier(value) && value.contains('.')
}

fn valid_reason_code(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 192
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':')
        })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sha2::{Digest as _, Sha256};

    use super::*;
    use crate::application::mcp_runtime::{
        McpConformanceCheck, McpConformanceCheckKind, McpConformanceCheckStatus,
        McpDescriptorAttestation, McpRuntimeLifecycleState, McpServerRecordV2,
        McpSessionTransportKind, McpToolEffectClassification, McpVerifiedDescriptorIdentity,
    };

    #[derive(Default)]
    struct MemoryEvidenceStore {
        trusted_tool: Mutex<Option<McpTrustedToolRecordV1>>,
        conformance: Mutex<Option<McpConformanceReportV1>>,
    }

    #[async_trait]
    impl McpSecurityEvidenceStore for MemoryEvidenceStore {
        async fn load_trusted_tool(
            &self,
            server_id: &str,
            tool_name: &str,
        ) -> Result<Option<McpTrustedToolRecordV1>, McpSecurityEvidenceStoreError> {
            Ok(self
                .trusted_tool
                .lock()
                .expect("evidence lock should be healthy")
                .clone()
                .filter(|record| record.server_id == server_id && record.tool_name == tool_name))
        }

        async fn persist_trusted_tool(
            &self,
            expected_revision: Option<u64>,
            record: &McpTrustedToolRecordV1,
        ) -> Result<(), McpSecurityEvidenceStoreError> {
            let mut current = self.trusted_tool.lock().expect("evidence lock should be healthy");
            let actual = current.as_ref().map(|record| record.revision);
            if actual != expected_revision {
                return Err(McpSecurityEvidenceStoreError::RevisionConflict {
                    expected: expected_revision,
                    actual,
                });
            }
            *current = Some(record.clone());
            Ok(())
        }

        async fn persist_conformance_report(
            &self,
            report: &McpConformanceReportV1,
        ) -> Result<(), McpSecurityEvidenceStoreError> {
            *self.conformance.lock().expect("evidence lock should be healthy") =
                Some(report.clone());
            Ok(())
        }

        async fn latest_conformance_report(
            &self,
            server_id: &str,
        ) -> Result<Option<McpConformanceReportV1>, McpSecurityEvidenceStoreError> {
            Ok(self
                .conformance
                .lock()
                .expect("evidence lock should be healthy")
                .clone()
                .filter(|report| report.server_id == server_id))
        }
    }

    struct AcceptingVerifier;

    impl McpDescriptorTrustVerifier for AcceptingVerifier {
        fn verify(
            &self,
            request: &TrustedExternalToolRegistrationRequest,
            _canonical_descriptor_sha256: &str,
        ) -> Result<McpVerifiedDescriptorIdentity, McpDescriptorAdmissionError> {
            Ok(McpVerifiedDescriptorIdentity {
                issuer_id: request.attestation.issuer_id.clone(),
                key_id: request.attestation.key_id.clone(),
            })
        }
    }

    fn ready_record() -> McpServerRecordV2 {
        McpServerRecordV2::configured(
            "server-a".to_owned(),
            McpSessionTransportKind::Stdio,
            None,
            "trusted-local".to_owned(),
            1_000,
        )
        .expect("configured record should validate")
        .begin_handshake(1_001)
        .expect("handshake should validate")
        .mark_ready("a".repeat(64), 1_002)
        .expect("ready record should validate")
    }

    fn registration_request(pin: &McpCatalogEpochPin) -> TrustedExternalToolRegistrationRequest {
        let descriptor = McpExternalToolDescriptor {
            name: "trusted.lookup".to_owned(),
            description: "Reads a bounded trusted fixture.".to_owned(),
            input_schema_json: serde_json::json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
            }),
            output_schema_json: None,
            effect: McpToolEffectClassification::ReadOnly,
            approval_class: "read_only".to_owned(),
        };
        let descriptor_sha256 = hex::encode(Sha256::digest(
            serde_json::to_vec(&descriptor).expect("descriptor should serialize"),
        ));
        TrustedExternalToolRegistrationRequest {
            server_id: pin.server_id.clone(),
            runtime_generation: pin.runtime_generation,
            catalog_epoch: pin.catalog_epoch,
            descriptor,
            attestation: McpDescriptorAttestation {
                issuer_id: "issuer-a".to_owned(),
                key_id: "key-a".to_owned(),
                descriptor_sha256,
                signature: "test-signature".to_owned(),
            },
        }
    }

    fn passing_conformance(pin: &McpCatalogEpochPin) -> McpConformanceReportV1 {
        McpConformanceReportV1 {
            schema_version: 1,
            server_id: pin.server_id.clone(),
            transport: McpSessionTransportKind::Stdio,
            runtime_generation: pin.runtime_generation,
            catalog_epoch: pin.catalog_epoch,
            started_at_unix_ms: 1_010,
            completed_at_unix_ms: 1_020,
            checks: [
                McpConformanceCheckKind::SingleOwnerGeneration,
                McpConformanceCheckKind::GenerationFencing,
                McpConformanceCheckKind::DurableRestore,
                McpConformanceCheckKind::CatalogEpochs,
                McpConformanceCheckKind::HostPolicyEnforcement,
                McpConformanceCheckKind::BoundedResources,
                McpConformanceCheckKind::CleanDrain,
            ]
            .into_iter()
            .map(|kind| McpConformanceCheck {
                kind,
                status: McpConformanceCheckStatus::Passed,
                evidence_ref: format!("artifact:{kind:?}"),
            })
            .collect(),
        }
    }

    fn registry_fixture() -> (
        McpTrustedToolRegistry,
        Arc<McpCatalogAuthority>,
        Arc<MemoryEvidenceStore>,
        McpCatalogEpochPin,
    ) {
        let authority =
            Arc::new(McpCatalogAuthority::new("server-a".to_owned()).expect("authority validates"));
        let ready = ready_record();
        assert_eq!(ready.lifecycle, McpRuntimeLifecycleState::Ready);
        authority.apply_committed(&ready).expect("ready record should apply");
        let pin = authority.pin().expect("ready record should expose a pin");
        let store = Arc::new(MemoryEvidenceStore::default());
        let policy = McpDescriptorAdmissionPolicy {
            trusted_issuer_ids: std::collections::BTreeSet::from(["issuer-a".to_owned()]),
            ..McpDescriptorAdmissionPolicy::default()
        };
        let registry = McpTrustedToolRegistry::new(
            authority.clone(),
            policy,
            Arc::new(AcceptingVerifier),
            store.clone(),
        );
        (registry, authority, store, pin)
    }

    #[tokio::test]
    async fn approval_requires_passing_conformance_for_the_current_catalog() {
        let (registry, _authority, _store, pin) = registry_fixture();
        let pending = registry
            .register(&pin, registration_request(&pin), 1_030)
            .await
            .expect("trusted descriptor should register");
        let decision = McpTrustedToolApproval {
            server_id: pin.server_id.clone(),
            tool_name: pending.tool_name.clone(),
            expected_revision: pending.revision,
            descriptor_sha256: pending.descriptor_sha256.clone(),
            approved: true,
            reason_code: "mcp.runtime.trusted_tool.operator_approved".to_owned(),
            decided_at_unix_ms: 1_040,
        };

        assert!(matches!(
            registry.decide(&decision).await,
            Err(McpTrustedToolRegistryError::ConformanceRequired)
        ));

        registry
            .record_conformance(&passing_conformance(&pin))
            .await
            .expect("current passing conformance should persist");
        let active = registry
            .decide(&decision)
            .await
            .expect("current passing conformance should permit activation");
        assert_eq!(active.activation, McpTrustedToolActivationState::Active);
        assert_eq!(
            active.approved_descriptor_sha256.as_deref(),
            Some(active.descriptor_sha256.as_str())
        );
    }

    #[tokio::test]
    async fn conformance_from_an_old_catalog_epoch_cannot_activate_a_descriptor() {
        let (registry, authority, _store, pin) = registry_fixture();
        registry
            .record_conformance(&passing_conformance(&pin))
            .await
            .expect("initial conformance should persist");
        let pending = registry
            .register(&pin, registration_request(&pin), 1_030)
            .await
            .expect("trusted descriptor should register");
        let advanced = ready_record()
            .advance_catalog(Some("b".repeat(64)), 1_031)
            .expect("host catalog should advance");
        authority.apply_committed(&advanced).expect("advanced record should apply");
        let advanced_pin = authority.pin().expect("advanced pin should exist");
        let rebound = registry
            .rebind_catalog_epoch(&pending, &advanced_pin, 1_032)
            .await
            .expect("pending record should rebind to the host epoch");
        let decision = McpTrustedToolApproval {
            server_id: rebound.server_id.clone(),
            tool_name: rebound.tool_name.clone(),
            expected_revision: rebound.revision,
            descriptor_sha256: rebound.descriptor_sha256.clone(),
            approved: true,
            reason_code: "mcp.runtime.trusted_tool.operator_approved".to_owned(),
            decided_at_unix_ms: 1_040,
        };

        assert!(matches!(
            registry.decide(&decision).await,
            Err(McpTrustedToolRegistryError::ConformanceRequired)
        ));
    }
}
