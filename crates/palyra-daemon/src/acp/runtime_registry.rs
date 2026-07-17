//! ACP runtime adapter registry and handle cache contracts.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use palyra_common::runtime_contracts::{
    AcpCapability, AcpScope, AcpSessionBindingRecord, RuntimeGeneration,
    ACP_DEFAULT_DISCONNECT_GRACE_MS,
};
use serde::{Deserialize, Serialize};

pub(crate) const ACP_RUNTIME_REGISTRY_SCHEMA_VERSION: u32 = 1;
pub(crate) const ACP_RUNTIME_PROMOTION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpRuntimeFeature {
    PermissionRelay,
    Streaming,
    Compaction,
    NativeThreadIdentity,
}

impl AcpRuntimeFeature {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PermissionRelay => "permission_relay",
            Self::Streaming => "streaming",
            Self::Compaction => "compaction",
            Self::NativeThreadIdentity => "native_thread_identity",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpCompactionOwner {
    Palyra,
    Harness,
    ExternalRuntime,
}

impl AcpCompactionOwner {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Palyra => "palyra",
            Self::Harness => "harness",
            Self::ExternalRuntime => "external_runtime",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpRuntimeAdapterDescriptor {
    pub runtime_id: String,
    pub display_name: String,
    pub features: BTreeSet<AcpRuntimeFeature>,
    pub supported_scopes: BTreeSet<AcpScope>,
    pub supported_capabilities: BTreeSet<AcpCapability>,
    pub compaction_owner: AcpCompactionOwner,
    pub default_timeout_ms: u64,
    pub handle_ttl_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_thread_identity: Option<String>,
    pub rollout_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpRuntimeMaturityStatus {
    Preview,
    GatedProduction,
    RollbackPreview,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcpRuntimePromotionInput {
    pub session_lifecycle_ready: bool,
    pub harness_conformance_ready: bool,
    pub permission_relay_host_owned: bool,
    pub resources_redacted: bool,
    pub method_registry_updated: bool,
    pub rollback_preview_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpRuntimeMethodMaturity {
    pub method: String,
    pub status: AcpRuntimeMaturityStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpRuntimePromotionProjection {
    pub schema_version: u32,
    pub status: AcpRuntimeMaturityStatus,
    pub production_enabled: bool,
    pub rollback_status: AcpRuntimeMaturityStatus,
    pub failing_gates: Vec<String>,
    pub method_registry: Vec<AcpRuntimeMethodMaturity>,
    pub permission_relay_owner: String,
    pub resource_redaction: String,
}

impl AcpRuntimeAdapterDescriptor {
    #[must_use]
    pub(crate) fn supports_feature(&self, feature: AcpRuntimeFeature) -> bool {
        self.features.contains(&feature)
    }
}

pub(crate) fn build_acp_runtime_promotion_projection(
    input: AcpRuntimePromotionInput,
) -> AcpRuntimePromotionProjection {
    let mut failing_gates = Vec::new();
    if !input.session_lifecycle_ready {
        failing_gates.push("session_lifecycle".to_owned());
    }
    if !input.harness_conformance_ready {
        failing_gates.push("harness_conformance".to_owned());
    }
    if !input.permission_relay_host_owned {
        failing_gates.push("permission_relay_host_owned".to_owned());
    }
    if !input.resources_redacted {
        failing_gates.push("resources_redacted".to_owned());
    }
    if !input.method_registry_updated {
        failing_gates.push("method_registry_updated".to_owned());
    }
    if !input.rollback_preview_available {
        failing_gates.push("rollback_preview_available".to_owned());
    }
    let production_enabled = failing_gates.is_empty();
    let status = if production_enabled {
        AcpRuntimeMaturityStatus::GatedProduction
    } else {
        AcpRuntimeMaturityStatus::Preview
    };
    let method_status = if input.method_registry_updated && production_enabled {
        AcpRuntimeMaturityStatus::GatedProduction
    } else {
        AcpRuntimeMaturityStatus::Preview
    };
    AcpRuntimePromotionProjection {
        schema_version: ACP_RUNTIME_PROMOTION_SCHEMA_VERSION,
        status,
        production_enabled,
        rollback_status: AcpRuntimeMaturityStatus::RollbackPreview,
        failing_gates,
        method_registry: [
            "session.create",
            "session.list",
            "session.fork",
            "run.wait",
            "run.cancel",
            "session.delete",
        ]
        .into_iter()
        .map(|method| AcpRuntimeMethodMaturity { method: method.to_owned(), status: method_status })
        .collect(),
        permission_relay_owner: if input.permission_relay_host_owned {
            "host_owned".to_owned()
        } else {
            "blocked".to_owned()
        },
        resource_redaction: if input.resources_redacted {
            "redacted_contracts".to_owned()
        } else {
            "blocked".to_owned()
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpRuntimeHandleState {
    Active,
    Stale,
    Crashed,
    Disposed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpRuntimeHandleReasonCode {
    AdapterRegistered,
    HandleCreated,
    HandleValidated,
    HandleRefreshed,
    TtlExpired,
    PermissionWideningDenied,
    RuntimeCrashed,
    RolloutDisabled,
    AdapterMissing,
    BindingMismatch,
    #[serde(rename = "runtime.generation.stale_suppressed")]
    StaleGeneration,
    #[serde(rename = "runtime.generation.exhausted")]
    GenerationExhausted,
    Disposed,
}

impl AcpRuntimeHandleReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AdapterRegistered => "acp_runtime.adapter_registered",
            Self::HandleCreated => "acp_runtime.handle_created",
            Self::HandleValidated => "acp_runtime.handle_validated",
            Self::HandleRefreshed => "acp_runtime.handle_refreshed",
            Self::TtlExpired => "acp_runtime.ttl_expired",
            Self::PermissionWideningDenied => "acp_runtime.permission_widening_denied",
            Self::RuntimeCrashed => "acp_runtime.runtime_crashed",
            Self::RolloutDisabled => "acp_runtime.rollout_disabled",
            Self::AdapterMissing => "acp_runtime.adapter_missing",
            Self::BindingMismatch => "acp_runtime.binding_mismatch",
            Self::StaleGeneration => "runtime.generation.stale_suppressed",
            Self::GenerationExhausted => "runtime.generation.exhausted",
            Self::Disposed => "acp_runtime.disposed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpHandleAuditRecord {
    pub schema_version: u32,
    pub event_type: String,
    pub runtime_id: String,
    pub handle_id: String,
    pub reason_code: AcpRuntimeHandleReasonCode,
    pub redacted_diagnostics: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpRuntimeHandle {
    pub handle_id: String,
    pub runtime_id: String,
    pub binding_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    pub generation: RuntimeGeneration,
    pub granted_scopes: BTreeSet<AcpScope>,
    pub granted_capabilities: BTreeSet<AcpCapability>,
    pub permission_fingerprint: String,
    pub state: AcpRuntimeHandleState,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
    pub last_validated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<AcpRuntimeHandleReasonCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crash_classification: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_thread_identity: Option<String>,
    pub audit: Vec<AcpHandleAuditRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcpHandleValidation {
    pub ok: bool,
    pub handle: AcpRuntimeHandle,
    pub reason_code: AcpRuntimeHandleReasonCode,
}

#[derive(Debug, Default)]
pub(crate) struct AcpRuntimeRegistry {
    adapters: BTreeMap<String, AcpRuntimeAdapterDescriptor>,
    handles: BTreeMap<String, AcpRuntimeHandle>,
}

impl AcpRuntimeRegistry {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register_adapter(
        &mut self,
        descriptor: AcpRuntimeAdapterDescriptor,
    ) -> AcpHandleAuditRecord {
        let runtime_id = descriptor.runtime_id.clone();
        self.adapters.insert(runtime_id.clone(), descriptor);
        audit_record(
            runtime_id.as_str(),
            "",
            AcpRuntimeHandleReasonCode::AdapterRegistered,
            [("registry", "registered")],
        )
    }

    pub(crate) fn create_handle(
        &mut self,
        runtime_id: &str,
        binding: &AcpSessionBindingRecord,
        now_unix_ms: i64,
    ) -> Result<AcpRuntimeHandle, AcpRuntimeHandleReasonCode> {
        let descriptor =
            self.adapters.get(runtime_id).ok_or(AcpRuntimeHandleReasonCode::AdapterMissing)?;
        if !descriptor.rollout_enabled {
            return Err(AcpRuntimeHandleReasonCode::RolloutDisabled);
        }

        let generation = RuntimeGeneration::new(1)
            .expect("the initial ACP runtime generation is a valid non-zero constant");
        let handle_id = format!("acphandle_{runtime_id}_{}_{}", binding.binding_id, generation);
        let permission_fingerprint = permission_fingerprint(binding);
        let audit = vec![audit_record(
            runtime_id,
            handle_id.as_str(),
            AcpRuntimeHandleReasonCode::HandleCreated,
            [
                ("binding_hash", crate::sha256_hex(binding.binding_id.as_bytes())),
                ("compaction_owner", descriptor.compaction_owner.as_str().to_owned()),
            ],
        )];
        let ttl_ms = descriptor.handle_ttl_ms.clamp(1_000, ACP_DEFAULT_DISCONNECT_GRACE_MS);
        let handle = AcpRuntimeHandle {
            handle_id: handle_id.clone(),
            runtime_id: runtime_id.to_owned(),
            binding_id: binding.binding_id.clone(),
            acp_session_id: binding.acp_session_id.clone(),
            palyra_session_id: binding.palyra_session_id.clone(),
            generation,
            granted_scopes: binding.scopes.iter().copied().collect(),
            granted_capabilities: binding.capabilities.iter().copied().collect(),
            permission_fingerprint,
            state: AcpRuntimeHandleState::Active,
            created_at_unix_ms: now_unix_ms,
            expires_at_unix_ms: now_unix_ms.saturating_add(ttl_ms),
            last_validated_at_unix_ms: now_unix_ms,
            stale_reason: None,
            crash_classification: None,
            native_thread_identity: descriptor.native_thread_identity.clone(),
            audit,
        };
        self.handles.insert(handle_id, handle.clone());
        Ok(handle)
    }

    pub(crate) fn validate_handle(
        &mut self,
        handle_id: &str,
        expected_generation: RuntimeGeneration,
        now_unix_ms: i64,
    ) -> Result<AcpHandleValidation, AcpRuntimeHandleReasonCode> {
        let handle =
            self.handles.get_mut(handle_id).ok_or(AcpRuntimeHandleReasonCode::AdapterMissing)?;
        if handle.generation != expected_generation {
            append_stale_generation_audit(handle, expected_generation);
            return Ok(AcpHandleValidation {
                ok: false,
                handle: handle.clone(),
                reason_code: AcpRuntimeHandleReasonCode::StaleGeneration,
            });
        }
        let reason = if handle.state == AcpRuntimeHandleState::Disposed {
            AcpRuntimeHandleReasonCode::Disposed
        } else if handle.crash_classification.is_some() {
            handle.state = AcpRuntimeHandleState::Crashed;
            AcpRuntimeHandleReasonCode::RuntimeCrashed
        } else if now_unix_ms > handle.expires_at_unix_ms {
            handle.state = AcpRuntimeHandleState::Stale;
            handle.stale_reason = Some(AcpRuntimeHandleReasonCode::TtlExpired);
            AcpRuntimeHandleReasonCode::TtlExpired
        } else {
            handle.last_validated_at_unix_ms = now_unix_ms;
            AcpRuntimeHandleReasonCode::HandleValidated
        };
        let ok = reason == AcpRuntimeHandleReasonCode::HandleValidated;
        handle.audit.push(audit_record(
            handle.runtime_id.as_str(),
            handle.handle_id.as_str(),
            reason,
            [("state", handle.state_label())],
        ));
        Ok(AcpHandleValidation { ok, handle: handle.clone(), reason_code: reason })
    }

    pub(crate) fn refresh_handle(
        &mut self,
        handle_id: &str,
        expected_generation: RuntimeGeneration,
        binding: &AcpSessionBindingRecord,
        now_unix_ms: i64,
    ) -> Result<AcpRuntimeHandle, AcpRuntimeHandleReasonCode> {
        let handle =
            self.handles.get_mut(handle_id).ok_or(AcpRuntimeHandleReasonCode::AdapterMissing)?;
        if handle.generation != expected_generation {
            append_stale_generation_audit(handle, expected_generation);
            return Err(AcpRuntimeHandleReasonCode::StaleGeneration);
        }
        if handle.binding_id != binding.binding_id {
            return Err(AcpRuntimeHandleReasonCode::BindingMismatch);
        }
        let new_scopes = binding.scopes.iter().copied().collect::<BTreeSet<_>>();
        let new_capabilities = binding.capabilities.iter().copied().collect::<BTreeSet<_>>();
        if !new_scopes.is_subset(&handle.granted_scopes)
            || !new_capabilities.is_subset(&handle.granted_capabilities)
        {
            handle.audit.push(audit_record(
                handle.runtime_id.as_str(),
                handle.handle_id.as_str(),
                AcpRuntimeHandleReasonCode::PermissionWideningDenied,
                [("state", handle.state_label())],
            ));
            return Err(AcpRuntimeHandleReasonCode::PermissionWideningDenied);
        }
        let descriptor = self
            .adapters
            .get(handle.runtime_id.as_str())
            .ok_or(AcpRuntimeHandleReasonCode::AdapterMissing)?;
        let next_generation = match handle.generation.next() {
            Ok(generation) => generation,
            Err(_) => {
                handle.audit.push(audit_record(
                    handle.runtime_id.as_str(),
                    handle.handle_id.as_str(),
                    AcpRuntimeHandleReasonCode::GenerationExhausted,
                    [("state", handle.state_label())],
                ));
                return Err(AcpRuntimeHandleReasonCode::GenerationExhausted);
            }
        };
        handle.generation = next_generation;
        handle.granted_scopes = new_scopes;
        handle.granted_capabilities = new_capabilities;
        handle.permission_fingerprint = permission_fingerprint(binding);
        handle.state = AcpRuntimeHandleState::Active;
        handle.stale_reason = None;
        handle.expires_at_unix_ms = now_unix_ms
            .saturating_add(descriptor.handle_ttl_ms.clamp(1_000, ACP_DEFAULT_DISCONNECT_GRACE_MS));
        handle.last_validated_at_unix_ms = now_unix_ms;
        handle.audit.push(audit_record(
            handle.runtime_id.as_str(),
            handle.handle_id.as_str(),
            AcpRuntimeHandleReasonCode::HandleRefreshed,
            [("state", handle.state_label())],
        ));
        Ok(handle.clone())
    }

    pub(crate) fn classify_crash(
        &mut self,
        handle_id: &str,
        crash_classification: &str,
    ) -> Result<AcpRuntimeHandle, AcpRuntimeHandleReasonCode> {
        let handle =
            self.handles.get_mut(handle_id).ok_or(AcpRuntimeHandleReasonCode::AdapterMissing)?;
        handle.state = AcpRuntimeHandleState::Crashed;
        handle.crash_classification = Some(redact_tokenish(crash_classification));
        handle.audit.push(audit_record(
            handle.runtime_id.as_str(),
            handle.handle_id.as_str(),
            AcpRuntimeHandleReasonCode::RuntimeCrashed,
            [("state", handle.state_label())],
        ));
        Ok(handle.clone())
    }

    pub(crate) fn dispose_handle(
        &mut self,
        handle_id: &str,
    ) -> Result<AcpRuntimeHandle, AcpRuntimeHandleReasonCode> {
        let handle =
            self.handles.get_mut(handle_id).ok_or(AcpRuntimeHandleReasonCode::AdapterMissing)?;
        handle.state = AcpRuntimeHandleState::Disposed;
        handle.audit.push(audit_record(
            handle.runtime_id.as_str(),
            handle.handle_id.as_str(),
            AcpRuntimeHandleReasonCode::Disposed,
            [("cleanup", "handle_disposed")],
        ));
        Ok(handle.clone())
    }
}

impl AcpRuntimeHandle {
    fn state_label(&self) -> &'static str {
        match self.state {
            AcpRuntimeHandleState::Active => "active",
            AcpRuntimeHandleState::Stale => "stale",
            AcpRuntimeHandleState::Crashed => "crashed",
            AcpRuntimeHandleState::Disposed => "disposed",
        }
    }
}

fn append_stale_generation_audit(
    handle: &mut AcpRuntimeHandle,
    observed_generation: RuntimeGeneration,
) {
    // A superseded handle may add redacted forensic evidence, but it cannot refresh validation
    // time, permissions, lifecycle state, or any other authority owned by the current generation.
    handle.audit.push(audit_record(
        handle.runtime_id.as_str(),
        handle.handle_id.as_str(),
        AcpRuntimeHandleReasonCode::StaleGeneration,
        [
            ("state", handle.state_label().to_owned()),
            ("expected_generation", handle.generation.to_string()),
            ("observed_generation", observed_generation.to_string()),
        ],
    ));
}

fn permission_fingerprint(binding: &AcpSessionBindingRecord) -> String {
    let mut parts = binding.scopes.iter().map(|scope| scope.as_str()).collect::<Vec<_>>();
    parts.extend(binding.capabilities.iter().map(|capability| capability.as_str()));
    parts.sort_unstable();
    crate::sha256_hex(parts.join("|").as_bytes())
}

fn audit_record<K, V>(
    runtime_id: &str,
    handle_id: &str,
    reason_code: AcpRuntimeHandleReasonCode,
    diagnostics: impl IntoIterator<Item = (K, V)>,
) -> AcpHandleAuditRecord
where
    K: Into<String>,
    V: AsRef<str>,
{
    AcpHandleAuditRecord {
        schema_version: ACP_RUNTIME_REGISTRY_SCHEMA_VERSION,
        event_type: "acp.runtime_handle".to_owned(),
        runtime_id: runtime_id.to_owned(),
        handle_id: handle_id.to_owned(),
        reason_code,
        redacted_diagnostics: diagnostics
            .into_iter()
            .map(|(key, value)| (key.into(), redact_tokenish(value.as_ref())))
            .collect(),
    }
}

fn redact_tokenish(value: &str) -> String {
    if value.len() > 80 || value.to_ascii_lowercase().contains("token") {
        palyra_common::redaction::REDACTED.to_owned()
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_common::runtime_contracts::{AcpCursor, AcpSessionMode};
    use serde_json::json;

    fn descriptor() -> AcpRuntimeAdapterDescriptor {
        AcpRuntimeAdapterDescriptor {
            runtime_id: "native-acp".to_owned(),
            display_name: "Native ACP".to_owned(),
            features: BTreeSet::from([
                AcpRuntimeFeature::PermissionRelay,
                AcpRuntimeFeature::Streaming,
                AcpRuntimeFeature::NativeThreadIdentity,
            ]),
            supported_scopes: BTreeSet::from([AcpScope::SessionsRead, AcpScope::RunsWrite]),
            supported_capabilities: BTreeSet::from([
                AcpCapability::SessionLoad,
                AcpCapability::ApprovalBridge,
            ]),
            compaction_owner: AcpCompactionOwner::ExternalRuntime,
            default_timeout_ms: 30_000,
            handle_ttl_ms: 10_000,
            native_thread_identity: Some("thread/native-1".to_owned()),
            rollout_enabled: true,
        }
    }

    fn binding(scopes: Vec<AcpScope>, capabilities: Vec<AcpCapability>) -> AcpSessionBindingRecord {
        AcpSessionBindingRecord {
            schema_version: 1,
            binding_id: "acpbind_test".to_owned(),
            acp_client_id: "zed-extension".to_owned(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_key: "repo:palyra".to_owned(),
            session_label: None,
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes,
            capabilities,
            mode: AcpSessionMode::Normal,
            config: json!({}),
            cursor: AcpCursor::default(),
            last_seen_at_unix_ms: 1_000,
            protocol_version: 1,
            stale_permissions: false,
        }
    }

    #[test]
    fn runtime_registry_creates_and_validates_handle() {
        let mut registry = AcpRuntimeRegistry::new();
        let audit = registry.register_adapter(descriptor());
        assert_eq!(audit.reason_code, AcpRuntimeHandleReasonCode::AdapterRegistered);

        let handle = registry
            .create_handle(
                "native-acp",
                &binding(
                    vec![AcpScope::SessionsRead],
                    vec![AcpCapability::SessionLoad, AcpCapability::ApprovalBridge],
                ),
                1_000,
            )
            .expect("handle should be created");
        assert_eq!(handle.state, AcpRuntimeHandleState::Active);
        assert_eq!(handle.native_thread_identity.as_deref(), Some("thread/native-1"));

        let validation = registry
            .validate_handle(handle.handle_id.as_str(), handle.generation, 2_000)
            .expect("handle should validate");
        assert!(validation.ok);
        assert_eq!(validation.reason_code, AcpRuntimeHandleReasonCode::HandleValidated);
    }

    #[test]
    fn stale_handle_refreshes_without_permission_widening() {
        let mut registry = AcpRuntimeRegistry::new();
        registry.register_adapter(descriptor());
        let original = binding(
            vec![AcpScope::SessionsRead, AcpScope::RunsWrite],
            vec![AcpCapability::SessionLoad, AcpCapability::ApprovalBridge],
        );
        let handle = registry
            .create_handle("native-acp", &original, 1_000)
            .expect("handle should be created");
        let validation = registry
            .validate_handle(handle.handle_id.as_str(), handle.generation, 12_000)
            .expect("expired handle should classify");
        assert_eq!(validation.reason_code, AcpRuntimeHandleReasonCode::TtlExpired);

        let refreshed = registry
            .refresh_handle(
                handle.handle_id.as_str(),
                handle.generation,
                &binding(vec![AcpScope::SessionsRead], vec![AcpCapability::SessionLoad]),
                12_100,
            )
            .expect("narrower permissions should refresh");
        assert_eq!(refreshed.state, AcpRuntimeHandleState::Active);
        assert_eq!(refreshed.generation.get(), 2);
    }

    #[test]
    fn refresh_never_widens_permissions() {
        let mut registry = AcpRuntimeRegistry::new();
        registry.register_adapter(descriptor());
        let handle = registry
            .create_handle(
                "native-acp",
                &binding(vec![AcpScope::SessionsRead], vec![AcpCapability::SessionLoad]),
                1_000,
            )
            .expect("handle should be created");

        let error = registry
            .refresh_handle(
                handle.handle_id.as_str(),
                handle.generation,
                &binding(
                    vec![AcpScope::SessionsRead, AcpScope::ApprovalsWrite],
                    vec![AcpCapability::SessionLoad, AcpCapability::ApprovalBridge],
                ),
                2_000,
            )
            .expect_err("widening must fail closed");
        assert_eq!(error, AcpRuntimeHandleReasonCode::PermissionWideningDenied);
    }

    #[test]
    fn refreshed_handle_rejects_stale_generation_without_mutating_authority() {
        let mut registry = AcpRuntimeRegistry::new();
        registry.register_adapter(descriptor());
        let narrowed_binding =
            binding(vec![AcpScope::SessionsRead], vec![AcpCapability::SessionLoad]);
        let original = registry
            .create_handle("native-acp", &narrowed_binding, 1_000)
            .expect("handle should be created");

        let refreshed = registry
            .refresh_handle(
                original.handle_id.as_str(),
                original.generation,
                &narrowed_binding,
                2_000,
            )
            .expect("current handle authority should refresh");
        assert_eq!(original.generation.get(), 1);
        assert_eq!(refreshed.generation.get(), 2);

        let stale_validation = registry
            .validate_handle(original.handle_id.as_str(), original.generation, 3_000)
            .expect("stale validation should produce a diagnostic result");
        assert!(!stale_validation.ok);
        assert_eq!(stale_validation.reason_code, AcpRuntimeHandleReasonCode::StaleGeneration);
        assert_eq!(stale_validation.handle.generation, refreshed.generation);
        assert_eq!(stale_validation.handle.state, AcpRuntimeHandleState::Active);
        assert_eq!(stale_validation.handle.last_validated_at_unix_ms, 2_000);
        let stale_audit = stale_validation
            .handle
            .audit
            .last()
            .expect("stale validation should append audit evidence");
        assert_eq!(stale_audit.reason_code, AcpRuntimeHandleReasonCode::StaleGeneration);
        assert_eq!(
            stale_audit.redacted_diagnostics.get("expected_generation").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            stale_audit.redacted_diagnostics.get("observed_generation").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            serde_json::to_value(stale_audit)
                .expect("audit should serialize")
                .pointer("/reason_code")
                .and_then(serde_json::Value::as_str),
            Some("runtime.generation.stale_suppressed")
        );

        let refresh_error = registry
            .refresh_handle(
                original.handle_id.as_str(),
                original.generation,
                &narrowed_binding,
                3_100,
            )
            .expect_err("superseded handle authority must not refresh again");
        assert_eq!(refresh_error, AcpRuntimeHandleReasonCode::StaleGeneration);

        let current_validation = registry
            .validate_handle(original.handle_id.as_str(), refreshed.generation, 3_200)
            .expect("refreshed authority should remain valid");
        assert!(current_validation.ok);
        assert_eq!(current_validation.reason_code, AcpRuntimeHandleReasonCode::HandleValidated);
        assert_eq!(current_validation.handle.last_validated_at_unix_ms, 3_200);
    }

    #[test]
    fn dispose_cleanup_is_audited() {
        let mut registry = AcpRuntimeRegistry::new();
        registry.register_adapter(descriptor());
        let handle = registry
            .create_handle(
                "native-acp",
                &binding(vec![AcpScope::SessionsRead], vec![AcpCapability::SessionLoad]),
                1_000,
            )
            .expect("handle should be created");

        let disposed =
            registry.dispose_handle(handle.handle_id.as_str()).expect("dispose should succeed");
        assert_eq!(disposed.state, AcpRuntimeHandleState::Disposed);
        assert!(disposed
            .audit
            .iter()
            .any(|entry| entry.reason_code == AcpRuntimeHandleReasonCode::Disposed));
    }

    #[test]
    fn promotion_projection_requires_all_gates_for_production() {
        let projection = build_acp_runtime_promotion_projection(AcpRuntimePromotionInput {
            session_lifecycle_ready: true,
            harness_conformance_ready: true,
            permission_relay_host_owned: true,
            resources_redacted: true,
            method_registry_updated: true,
            rollback_preview_available: true,
        });

        assert_eq!(projection.schema_version, ACP_RUNTIME_PROMOTION_SCHEMA_VERSION);
        assert_eq!(projection.status, AcpRuntimeMaturityStatus::GatedProduction);
        assert!(projection.production_enabled);
        assert!(projection.failing_gates.is_empty());
        assert!(projection
            .method_registry
            .iter()
            .all(|entry| entry.status == AcpRuntimeMaturityStatus::GatedProduction));
        assert_eq!(projection.permission_relay_owner, "host_owned");
        assert_eq!(projection.resource_redaction, "redacted_contracts");
    }

    #[test]
    fn promotion_projection_keeps_preview_when_security_gates_fail() {
        let projection = build_acp_runtime_promotion_projection(AcpRuntimePromotionInput {
            session_lifecycle_ready: true,
            harness_conformance_ready: true,
            permission_relay_host_owned: false,
            resources_redacted: false,
            method_registry_updated: true,
            rollback_preview_available: true,
        });

        assert_eq!(projection.status, AcpRuntimeMaturityStatus::Preview);
        assert!(!projection.production_enabled);
        assert_eq!(projection.rollback_status, AcpRuntimeMaturityStatus::RollbackPreview);
        assert!(projection.failing_gates.contains(&"permission_relay_host_owned".to_owned()));
        assert!(projection.failing_gates.contains(&"resources_redacted".to_owned()));
        assert!(projection
            .method_registry
            .iter()
            .all(|entry| entry.status == AcpRuntimeMaturityStatus::Preview));
    }
}
