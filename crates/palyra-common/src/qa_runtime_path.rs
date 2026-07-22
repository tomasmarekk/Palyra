//! Metadata-only QA evidence for the runtime path selected by a real run.
//!
//! The contract keeps path identity and fallback reason codes explicit while
//! rejecting free-form payloads that could hide secrets or unbounded data.

use std::{collections::BTreeSet, error::Error, fmt};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Current schema version for [`RuntimePathEvidence`].
pub const QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION: u32 = 1;
/// Canonical tape event emitted for an MCP tools/call transport invocation.
pub const MCP_TRANSPORT_INVOCATION_EVENT: &str = "mcp.transport.invocation";
/// Current schema version for [`McpTransportInvocationEvent`].
pub const MCP_TRANSPORT_INVOCATION_EVENT_SCHEMA_VERSION: u32 = 1;
/// Canonical tape event emitted after an authoritative context-engine binding is verified.
pub const CONTEXT_ENGINE_BINDING_EVENT: &str = "runtime.context_engine.bound";
/// Current schema version for [`ContextEngineBindingEvent`].
pub const CONTEXT_ENGINE_BINDING_EVENT_SCHEMA_VERSION: u32 = 1;
/// Canonical tape event emitted after a provider adapter serves a QA run turn.
pub const PROVIDER_LANE_ATTESTATION_EVENT: &str = "provider.lane.attested";
/// Current schema version for [`ProviderLaneAttestationEvent`].
pub const PROVIDER_LANE_ATTESTATION_EVENT_SCHEMA_VERSION: u32 = 1;
/// Canonical tape event emitted when provider routing moves to another provider/model pair.
pub const PROVIDER_ROUTE_CHANGE_EVENT: &str = "provider.route.changed";
/// Current schema version for [`ProviderRouteChangeEvent`].
pub const PROVIDER_ROUTE_CHANGE_EVENT_SCHEMA_VERSION: u32 = 1;
/// Marker emitted when bounded provider route-change evidence omits later transitions.
pub const PROVIDER_ROUTE_CHANGE_EVIDENCE_TRUNCATED_EVENT: &str =
    "provider.route.evidence_truncated";
/// Materialization used by a schema-v1 deterministic QA provider fixture.
pub const QA_PROVIDER_FIXTURE_MATERIALIZATION: &str = "qa_mock_fixture";
/// Materialization used by a redacted schema-v2 record/replay provider fixture.
pub const QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION: &str = "redacted_record_replay";
/// Materialization used by a live provider profile projected into the isolated daemon.
pub const QA_PROVIDER_LIVE_MATERIALIZATION: &str = "live_provider_profile";

const PROVIDER_LANES: &[&str] = &["fixture", "record_replay", "live"];
const MAX_METADATA_TOKEN_BYTES: usize = 192;
const MAX_RUNTIME_VERSION_BYTES: usize = 256;
const MAX_PROVIDER_BASE_URL_BYTES: usize = 2_048;
const MAX_SOURCE_EVENTS: usize = 32;
const MAX_REASON_CODES: usize = 64;
const MAX_FALLBACKS: usize = 32;

/// Metadata-only proof of the context-engine implementation bound to a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextEngineBindingEvent {
    /// Version of this event payload.
    pub schema_version: u32,
    /// Canonical event name duplicated inside the payload for fail-closed decoding.
    pub event_name: String,
    /// Exact registered engine implementation selected for execution.
    pub engine_id: String,
    /// Exact registered implementation version.
    pub engine_version: String,
    /// Immutable context projection epoch selected for the run.
    pub projection_epoch: u64,
}

impl ContextEngineBindingEvent {
    /// Validates event identity and bounded context-engine metadata.
    ///
    /// # Errors
    /// Returns [`QaRuntimePathValidationError`] when the event is malformed or unbounded.
    pub fn validate_shape(&self) -> Result<(), QaRuntimePathValidationError> {
        if self.schema_version != CONTEXT_ENGINE_BINDING_EVENT_SCHEMA_VERSION {
            return Err(validation_error(
                "unsupported_context_engine_binding_schema_version",
                "$.schema_version",
                format!(
                    "expected schema version {CONTEXT_ENGINE_BINDING_EVENT_SCHEMA_VERSION}, got {}",
                    self.schema_version
                ),
            ));
        }
        if self.event_name != CONTEXT_ENGINE_BINDING_EVENT {
            return Err(validation_error(
                "context_engine_binding_event_name_mismatch",
                "$.event_name",
                format!("expected '{CONTEXT_ENGINE_BINDING_EVENT}'"),
            ));
        }
        validate_slug_token(self.engine_id.as_str(), "$.engine_id", MAX_METADATA_TOKEN_BYTES)?;
        validate_version_token(
            self.engine_version.as_str(),
            "$.engine_version",
            MAX_RUNTIME_VERSION_BYTES,
        )?;
        if self.projection_epoch == 0 {
            return Err(validation_error(
                "context_engine_binding_projection_epoch_invalid",
                "$.projection_epoch",
                "projection epoch must be non-zero",
            ));
        }
        Ok(())
    }
}

/// Metadata-only proof produced by the provider adapter that served a QA turn.
///
/// Fixture and replay lanes retain only a digest of the exact materialized
/// bytes. The raw fixture, endpoint, profile, and credential never enter this
/// event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLaneAttestationEvent {
    /// Version of this event payload.
    pub schema_version: u32,
    /// Canonical event name duplicated inside the payload for fail-closed decoding.
    pub event_name: String,
    /// Digest of the parent-issued scenario execution key.
    pub execution_key_digest: String,
    /// Binding digest correlated against the execution key.
    pub provider_binding_sha256: String,
    /// Lane derived by the provider adapter from the binding it actually used.
    pub provider_lane: String,
    /// Bounded description of how the binding was materialized.
    pub materialization_kind: String,
    /// Digest of exact fixture bytes loaded by deterministic adapters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized_input_sha256: Option<String>,
    /// Redacted metadata independently derived from the materialized live binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_binding: Option<ProviderLiveBindingMetadata>,
    /// Provider selected by the runtime registry after routing.
    pub provider_id: String,
    /// Model selected by the runtime registry after routing.
    pub model_id: String,
}

/// Secret-free fields that identify the live provider binding used by the daemon.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLiveBindingMetadata {
    /// Provider transport implementation selected by runtime configuration.
    pub provider_kind: String,
    /// Projected profile alias visible inside the isolated daemon.
    pub auth_profile_id: String,
    /// Credential semantics selected for the projected profile.
    pub auth_provider_kind: String,
    /// Digest of the normalized provider base URL; the URL itself is never persisted.
    pub base_url_sha256: String,
    /// Raw live-provider payload storage must remain disabled.
    pub raw_payload_storage: bool,
}

impl ProviderLaneAttestationEvent {
    /// Validates event identity, bounded metadata, and fixture binding correlation.
    ///
    /// # Errors
    /// Returns [`QaRuntimePathValidationError`] when the event is malformed,
    /// exposes unsupported metadata, or its binding hash does not correspond
    /// to the attested materialized fixture bytes.
    pub fn validate_shape(&self) -> Result<(), QaRuntimePathValidationError> {
        if self.schema_version != PROVIDER_LANE_ATTESTATION_EVENT_SCHEMA_VERSION {
            return Err(validation_error(
                "unsupported_provider_lane_attestation_schema_version",
                "$.schema_version",
                format!(
                    "expected schema version {PROVIDER_LANE_ATTESTATION_EVENT_SCHEMA_VERSION}, got {}",
                    self.schema_version
                ),
            ));
        }
        if self.event_name != PROVIDER_LANE_ATTESTATION_EVENT {
            return Err(validation_error(
                "provider_lane_attestation_event_name_mismatch",
                "$.event_name",
                format!("expected '{PROVIDER_LANE_ATTESTATION_EVENT}'"),
            ));
        }
        validate_sha256_hex(self.execution_key_digest.as_str(), "$.execution_key_digest")?;
        validate_sha256_hex(self.provider_binding_sha256.as_str(), "$.provider_binding_sha256")?;
        validate_provider_lane(self.provider_lane.as_str(), "$.provider_lane")?;
        validate_slug_token(self.provider_id.as_str(), "$.provider_id", MAX_METADATA_TOKEN_BYTES)?;
        validate_version_token(self.model_id.as_str(), "$.model_id", MAX_RUNTIME_VERSION_BYTES)?;

        let expected_materialization = match self.provider_lane.as_str() {
            "fixture" => QA_PROVIDER_FIXTURE_MATERIALIZATION,
            "record_replay" => QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION,
            "live" => QA_PROVIDER_LIVE_MATERIALIZATION,
            _ => unreachable!("provider lane was validated above"),
        };
        if self.materialization_kind != expected_materialization {
            return Err(validation_error(
                "provider_lane_attestation_materialization_mismatch",
                "$.materialization_kind",
                format!(
                    "lane '{}' requires materialization '{expected_materialization}'",
                    self.provider_lane
                ),
            ));
        }

        match (
            self.provider_lane.as_str(),
            self.materialized_input_sha256.as_deref(),
            self.live_binding.as_ref(),
        ) {
            ("fixture" | "record_replay", Some(materialized_input_sha256), None) => {
                validate_sha256_hex(materialized_input_sha256, "$.materialized_input_sha256")?;
                let actual_binding = qa_provider_binding_sha256(
                    self.provider_lane.as_str(),
                    self.materialization_kind.as_str(),
                    materialized_input_sha256,
                )?;
                if self.provider_binding_sha256 != actual_binding {
                    return Err(validation_error(
                        "provider_lane_attestation_binding_mismatch",
                        "$.provider_binding_sha256",
                        "binding digest does not match the attested materialized fixture bytes",
                    ));
                }
            }
            ("fixture" | "record_replay", None, None) => {
                return Err(validation_error(
                    "provider_lane_attestation_materialized_digest_missing",
                    "$.materialized_input_sha256",
                    "fixture and record_replay lanes require a materialized input digest",
                ));
            }
            ("fixture" | "record_replay", _, Some(_)) => {
                return Err(validation_error(
                    "provider_lane_attestation_fixture_live_binding_forbidden",
                    "$.live_binding",
                    "fixture and record_replay lanes must not persist live binding metadata",
                ));
            }
            ("live", None, Some(live_binding)) => {
                let actual_binding = qa_live_provider_binding_sha256(
                    self.provider_id.as_str(),
                    self.model_id.as_str(),
                    live_binding,
                )?;
                if self.provider_binding_sha256 != actual_binding {
                    return Err(validation_error(
                        "provider_lane_attestation_binding_mismatch",
                        "$.provider_binding_sha256",
                        "binding digest does not match the attested live provider metadata",
                    ));
                }
            }
            ("live", Some(_), _) => {
                return Err(validation_error(
                    "provider_lane_attestation_live_materialized_digest_forbidden",
                    "$.materialized_input_sha256",
                    "live lane attestation must not persist materialized provider input metadata",
                ));
            }
            ("live", None, None) => {
                return Err(validation_error(
                    "provider_lane_attestation_live_binding_missing",
                    "$.live_binding",
                    "live lane attestation requires independently derived binding metadata",
                ));
            }
            _ => unreachable!("provider lane was validated above"),
        }
        Ok(())
    }
}

/// Derives the execution-key binding digest from an actually materialized QA fixture.
///
/// # Errors
/// Returns [`QaRuntimePathValidationError`] when the lane, materialization, or
/// input digest is not canonical.
pub fn qa_provider_binding_sha256(
    provider_lane: &str,
    materialization_kind: &str,
    materialized_input_sha256: &str,
) -> Result<String, QaRuntimePathValidationError> {
    validate_provider_lane(provider_lane, "$.provider_lane")?;
    validate_slug_token(materialization_kind, "$.materialization_kind", MAX_METADATA_TOKEN_BYTES)?;
    validate_sha256_hex(materialized_input_sha256, "$.materialized_input_sha256")?;
    let expected_materialization = match provider_lane {
        "fixture" => QA_PROVIDER_FIXTURE_MATERIALIZATION,
        "record_replay" => QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION,
        "live" => QA_PROVIDER_LIVE_MATERIALIZATION,
        _ => unreachable!("provider lane was validated above"),
    };
    if provider_lane == "live" || materialization_kind != expected_materialization {
        return Err(validation_error(
            "provider_lane_binding_materialization_invalid",
            "$.materialization_kind",
            "fixture binding digests require the lane's canonical materialization kind",
        ));
    }

    let mut hasher = Sha256::new();
    hasher.update(b"palyra.qa.provider-binding.v1\0");
    for (label, value) in [
        ("provider_lane", provider_lane),
        ("materialization_kind", materialization_kind),
        ("materialized_input_sha256", materialized_input_sha256),
    ] {
        hasher.update((label.len() as u64).to_be_bytes());
        hasher.update(label.as_bytes());
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Hashes the canonical provider endpoint without persisting its URL.
///
/// The daemon config parser already canonicalizes absolute provider URLs. This
/// helper additionally removes surrounding whitespace and trailing slashes so
/// the parent runner and isolated daemon derive the same metadata digest.
///
/// # Errors
/// Returns [`QaRuntimePathValidationError`] when the URL is empty, unbounded,
/// or contains control characters.
pub fn qa_live_provider_base_url_sha256(
    base_url: &str,
) -> Result<String, QaRuntimePathValidationError> {
    let normalized = base_url.trim().trim_end_matches('/');
    if normalized.is_empty()
        || normalized.len() > MAX_PROVIDER_BASE_URL_BYTES
        || normalized.chars().any(char::is_control)
    {
        return Err(validation_error(
            "provider_live_base_url_invalid",
            "$.live_binding.base_url_sha256",
            format!(
                "provider base URL must normalize to 1..={MAX_PROVIDER_BASE_URL_BYTES} non-control UTF-8 bytes"
            ),
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.qa.provider-base-url.v1\0");
    hasher.update((normalized.len() as u64).to_be_bytes());
    hasher.update(normalized.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

/// Derives a live execution-key binding from metadata the parent and daemon
/// can observe independently.
///
/// # Errors
/// Returns [`QaRuntimePathValidationError`] when any metadata field is
/// malformed or live raw-payload storage is enabled.
pub fn qa_live_provider_binding_sha256(
    provider_id: &str,
    model_id: &str,
    metadata: &ProviderLiveBindingMetadata,
) -> Result<String, QaRuntimePathValidationError> {
    validate_slug_token(provider_id, "$.provider_id", MAX_METADATA_TOKEN_BYTES)?;
    validate_version_token(model_id, "$.model_id", MAX_RUNTIME_VERSION_BYTES)?;
    validate_slug_token(
        metadata.provider_kind.as_str(),
        "$.live_binding.provider_kind",
        MAX_METADATA_TOKEN_BYTES,
    )?;
    validate_slug_token(
        metadata.auth_profile_id.as_str(),
        "$.live_binding.auth_profile_id",
        MAX_METADATA_TOKEN_BYTES,
    )?;
    validate_slug_token(
        metadata.auth_provider_kind.as_str(),
        "$.live_binding.auth_provider_kind",
        MAX_METADATA_TOKEN_BYTES,
    )?;
    validate_sha256_hex(metadata.base_url_sha256.as_str(), "$.live_binding.base_url_sha256")?;
    if metadata.raw_payload_storage {
        return Err(validation_error(
            "provider_live_raw_payload_storage_forbidden",
            "$.live_binding.raw_payload_storage",
            "live QA provider binding requires raw payload storage to remain disabled",
        ));
    }

    let mut hasher = Sha256::new();
    hasher.update(b"palyra.qa.live-provider-binding.v1\0");
    for (label, value) in [
        ("provider_lane", "live"),
        ("materialization_kind", QA_PROVIDER_LIVE_MATERIALIZATION),
        ("provider_id", provider_id),
        ("model_id", model_id),
        ("provider_kind", metadata.provider_kind.as_str()),
        ("auth_profile_id", metadata.auth_profile_id.as_str()),
        ("auth_provider_kind", metadata.auth_provider_kind.as_str()),
        ("base_url_sha256", metadata.base_url_sha256.as_str()),
        ("raw_payload_storage", "false"),
    ] {
        hasher.update((label.len() as u64).to_be_bytes());
        hasher.update(label.as_bytes());
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Metadata-only evidence for one executed provider/model route transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRouteChangeEvent {
    /// Version of this event payload.
    pub schema_version: u32,
    /// Canonical event name duplicated inside the payload for fail-closed decoding.
    pub event_name: String,
    /// Zero-based transition index within the provider response attempt chain.
    pub transition_index: u32,
    /// Provider used by the preceding executed attempt.
    pub from_provider_id: String,
    /// Model used by the preceding executed attempt.
    pub from_model_id: String,
    /// Provider used by the next executed attempt.
    pub to_provider_id: String,
    /// Model used by the next executed attempt.
    pub to_model_id: String,
    /// Stable reason code projected into runtime fallback evidence.
    pub reason_code: String,
}

impl ProviderRouteChangeEvent {
    /// Validates bounded identities and requires an actual provider/model change.
    ///
    /// # Errors
    /// Returns [`QaRuntimePathValidationError`] when the payload is malformed
    /// or describes the same route on both sides.
    pub fn validate_shape(&self) -> Result<(), QaRuntimePathValidationError> {
        if self.schema_version != PROVIDER_ROUTE_CHANGE_EVENT_SCHEMA_VERSION {
            return Err(validation_error(
                "unsupported_provider_route_change_schema_version",
                "$.schema_version",
                format!(
                    "expected schema version {PROVIDER_ROUTE_CHANGE_EVENT_SCHEMA_VERSION}, got {}",
                    self.schema_version
                ),
            ));
        }
        if self.event_name != PROVIDER_ROUTE_CHANGE_EVENT {
            return Err(validation_error(
                "provider_route_change_event_name_mismatch",
                "$.event_name",
                format!("expected '{PROVIDER_ROUTE_CHANGE_EVENT}'"),
            ));
        }
        validate_slug_token(
            self.from_provider_id.as_str(),
            "$.from_provider_id",
            MAX_METADATA_TOKEN_BYTES,
        )?;
        validate_version_token(
            self.from_model_id.as_str(),
            "$.from_model_id",
            MAX_RUNTIME_VERSION_BYTES,
        )?;
        validate_slug_token(
            self.to_provider_id.as_str(),
            "$.to_provider_id",
            MAX_METADATA_TOKEN_BYTES,
        )?;
        validate_version_token(
            self.to_model_id.as_str(),
            "$.to_model_id",
            MAX_RUNTIME_VERSION_BYTES,
        )?;
        if self.reason_code != "runtime_path.provider.route_changed" {
            return Err(validation_error(
                "provider_route_change_reason_invalid",
                "$.reason_code",
                "provider route changes require reason code 'runtime_path.provider.route_changed'",
            ));
        }
        if self.from_provider_id == self.to_provider_id && self.from_model_id == self.to_model_id {
            return Err(validation_error(
                "provider_route_change_identity_unchanged",
                "$.to_provider_id",
                "provider route evidence requires a changed provider or model identity",
            ));
        }
        Ok(())
    }
}

/// Connection lifecycle actually used for one MCP transport invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransportInvocationMode {
    /// The transport establishes and tears down its connection for each operation.
    PerCall,
    /// The transport reuses a supervised connection across operations.
    Persistent,
}

impl McpTransportInvocationMode {
    /// Returns the canonical runtime-path identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PerCall => "per_call",
            Self::Persistent => "persistent",
        }
    }
}

/// Metadata-only tape payload proving the MCP transport used for one tools/call operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpTransportInvocationEvent {
    /// Version of this event payload.
    pub schema_version: u32,
    /// Canonical event name duplicated inside the payload for fail-closed decoding.
    pub event_name: String,
    /// Hash-bound MCP invocation attestation identifier.
    pub attestation_id: String,
    /// Redacted transport identity derived from the host-reviewed manifest.
    pub transport_id: String,
    /// Namespaced MCP tool identifier that crossed the transport boundary.
    pub namespaced_tool_id: String,
    /// Connection lifecycle reported by the transport implementation that executed the call.
    pub transport_mode: McpTransportInvocationMode,
}

impl McpTransportInvocationEvent {
    /// Validates the canonical event identity and bounded attestation references.
    ///
    /// # Errors
    /// Returns [`QaRuntimePathValidationError`] when the payload is unsupported,
    /// malformed, or does not identify an MCP transport/tool invocation.
    pub fn validate_shape(&self) -> Result<(), QaRuntimePathValidationError> {
        if self.schema_version != MCP_TRANSPORT_INVOCATION_EVENT_SCHEMA_VERSION {
            return Err(validation_error(
                "unsupported_mcp_transport_invocation_schema_version",
                "$.schema_version",
                format!(
                    "expected schema version {MCP_TRANSPORT_INVOCATION_EVENT_SCHEMA_VERSION}, got {}",
                    self.schema_version
                ),
            ));
        }
        if self.event_name != MCP_TRANSPORT_INVOCATION_EVENT {
            return Err(validation_error(
                "mcp_transport_invocation_event_name_mismatch",
                "$.event_name",
                format!("expected '{MCP_TRANSPORT_INVOCATION_EVENT}'"),
            ));
        }
        validate_slug_token(
            self.attestation_id.as_str(),
            "$.attestation_id",
            MAX_METADATA_TOKEN_BYTES,
        )?;
        if !self.attestation_id.starts_with("mcpatt_") {
            return Err(validation_error(
                "mcp_transport_invocation_attestation_id_invalid",
                "$.attestation_id",
                "attestation_id must use the mcpatt_ namespace",
            ));
        }
        validate_slug_token(
            self.transport_id.as_str(),
            "$.transport_id",
            MAX_METADATA_TOKEN_BYTES,
        )?;
        if !self.transport_id.starts_with("mcp.transport.") {
            return Err(validation_error(
                "mcp_transport_invocation_transport_id_invalid",
                "$.transport_id",
                "transport_id must use the mcp.transport. namespace",
            ));
        }
        validate_mcp_identifier_token(
            self.namespaced_tool_id.as_str(),
            "$.namespaced_tool_id",
            MAX_METADATA_TOKEN_BYTES,
        )?;
        if !self.namespaced_tool_id.starts_with("mcp.") {
            return Err(validation_error(
                "mcp_transport_invocation_tool_id_invalid",
                "$.namespaced_tool_id",
                "namespaced_tool_id must use the mcp. namespace",
            ));
        }
        Ok(())
    }
}

/// One runtime component proven by a specific metadata event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePathComponentEvidence {
    /// Stable component identifier, such as `embedded_run_stream`.
    pub id: String,
    /// Event that supplied the component identity.
    pub source_event: String,
    /// Stable reason code explaining the selection.
    pub reason_code: String,
}

/// One explicit fallback observed while selecting or executing the runtime path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFallbackEvidence {
    /// Runtime component that used the fallback.
    pub component: String,
    /// Direct path that was attempted, when it is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Path that was used after the fallback decision.
    pub to: String,
    /// Stable reason code emitted by the runtime.
    pub reason_code: String,
    /// Event that supplied the fallback evidence.
    pub source_event: String,
}

/// Complete metadata-only evidence for the runtime path used by one QA run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePathEvidence {
    /// Version of this evidence contract.
    pub schema_version: u32,
    /// Exact daemon runtime identity, such as a content-addressed binary version.
    pub runtime_version: String,
    /// Public runtime contract version reported by the daemon.
    pub runtime_contract_version: String,
    /// QA runner contract and build identity that collected the evidence.
    pub runner_version: String,
    /// Provider execution lane attested by the adapter, or `unobserved` on partial evidence.
    pub provider_lane: String,
    /// Runtime owner that executed the agent attempt.
    pub attempt_owner: String,
    /// Harness selection evidence, including an explicit embedded path.
    pub harness: RuntimePathComponentEvidence,
    /// Context assembly implementation selected for the run.
    pub context_engine: RuntimePathComponentEvidence,
    /// MCP transport/session evidence when the run used MCP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_transport_mode: Option<RuntimePathComponentEvidence>,
    /// Whether every required evidence source was present and mutually consistent.
    pub complete: bool,
    /// Ordered event names used to construct this projection.
    pub source_events: Vec<String>,
    /// Stable reason codes retained from validation and selection decisions.
    pub reason_codes: Vec<String>,
    /// Ordered fallback decisions; allowances never remove these records.
    pub fallbacks: Vec<RuntimeFallbackEvidence>,
    /// Exact count of retained fallback records.
    pub fallback_count: u32,
}

impl RuntimePathEvidence {
    /// Validates bounded metadata and internal fallback counts.
    ///
    /// # Errors
    /// Returns [`QaRuntimePathValidationError`] when the evidence is unsupported,
    /// incomplete in shape, internally inconsistent, or contains unsafe metadata.
    pub fn validate_shape(&self) -> Result<(), QaRuntimePathValidationError> {
        if self.schema_version != QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION {
            return Err(validation_error(
                "unsupported_runtime_path_schema_version",
                "$.schema_version",
                format!(
                    "expected schema version {QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION}, got {}",
                    self.schema_version
                ),
            ));
        }
        validate_version_token(
            self.runtime_version.as_str(),
            "$.runtime_version",
            MAX_RUNTIME_VERSION_BYTES,
        )?;
        validate_version_token(
            self.runtime_contract_version.as_str(),
            "$.runtime_contract_version",
            MAX_RUNTIME_VERSION_BYTES,
        )?;
        validate_version_token(
            self.runner_version.as_str(),
            "$.runner_version",
            MAX_RUNTIME_VERSION_BYTES,
        )?;
        if self.provider_lane == "unobserved" {
            if self.complete {
                return Err(validation_error(
                    "runtime_path_provider_lane_unobserved_complete",
                    "$.provider_lane",
                    "complete runtime-path evidence requires an attested provider lane",
                ));
            }
        } else {
            validate_provider_lane(self.provider_lane.as_str(), "$.provider_lane")?;
        }
        validate_slug_token(
            self.attempt_owner.as_str(),
            "$.attempt_owner",
            MAX_METADATA_TOKEN_BYTES,
        )?;
        validate_component(&self.harness, "$.harness")?;
        validate_component(&self.context_engine, "$.context_engine")?;
        if let Some(mcp) = self.mcp_transport_mode.as_ref() {
            validate_component(mcp, "$.mcp_transport_mode")?;
        }
        validate_token_list(
            self.source_events.as_slice(),
            "$.source_events",
            MAX_SOURCE_EVENTS,
            "runtime_path_source_events_limit_exceeded",
        )?;
        validate_token_list(
            self.reason_codes.as_slice(),
            "$.reason_codes",
            MAX_REASON_CODES,
            "runtime_path_reason_codes_limit_exceeded",
        )?;
        if self.fallbacks.len() > MAX_FALLBACKS {
            return Err(validation_error(
                "runtime_path_fallback_limit_exceeded",
                "$.fallbacks",
                format!("fallback count must not exceed {MAX_FALLBACKS}"),
            ));
        }
        let fallback_count = u32::try_from(self.fallbacks.len()).map_err(|_| {
            validation_error(
                "runtime_path_fallback_count_overflow",
                "$.fallback_count",
                "fallback count cannot be represented as u32",
            )
        })?;
        if self.fallback_count != fallback_count {
            return Err(validation_error(
                "runtime_path_fallback_count_mismatch",
                "$.fallback_count",
                format!(
                    "declared fallback_count {} differs from {} retained records",
                    self.fallback_count, fallback_count
                ),
            ));
        }
        for (index, fallback) in self.fallbacks.iter().enumerate() {
            validate_fallback(fallback, index)?;
        }
        validate_source_bindings(self)?;
        Ok(())
    }
}

/// Exact runtime-path requirement declared by a schema-v5 QA scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoHiddenFallbackExpectation {
    /// Exact public runtime contract version required by the scenario.
    pub runtime_contract_version: String,
    /// Exact provider lane required by the scenario.
    pub provider_lane: String,
    /// Exact owner that must execute the attempt.
    pub attempt_owner: String,
    /// Exact harness identifier, including explicit embedded identifiers.
    pub harness_id: String,
    /// Exact context-engine identifier, including an explicit legacy identifier.
    pub context_engine_id: String,
    /// Required MCP transport/session mode; omission requires no MCP evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_transport_mode: Option<McpTransportInvocationMode>,
    /// Maximum number of explicit fallbacks that may be observed.
    pub max_fallback_count: u32,
    /// Exact fallback reason codes allowed within the maximum count.
    #[serde(default)]
    pub allowed_fallback_reason_codes: Vec<String>,
}

impl NoHiddenFallbackExpectation {
    /// Validates exact selectors and bounded fallback allowances.
    ///
    /// # Errors
    /// Returns [`QaRuntimePathValidationError`] when a selector is unsafe, a
    /// provider lane is unsupported, or fallback allowances exceed their bounds.
    pub fn validate_shape(&self) -> Result<(), QaRuntimePathValidationError> {
        validate_version_token(
            self.runtime_contract_version.as_str(),
            "$.runtime_contract_version",
            MAX_RUNTIME_VERSION_BYTES,
        )?;
        validate_provider_lane(self.provider_lane.as_str(), "$.provider_lane")?;
        validate_slug_token(
            self.attempt_owner.as_str(),
            "$.attempt_owner",
            MAX_METADATA_TOKEN_BYTES,
        )?;
        validate_slug_token(self.harness_id.as_str(), "$.harness_id", MAX_METADATA_TOKEN_BYTES)?;
        validate_slug_token(
            self.context_engine_id.as_str(),
            "$.context_engine_id",
            MAX_METADATA_TOKEN_BYTES,
        )?;
        if usize::try_from(self.max_fallback_count).map_or(true, |count| count > MAX_FALLBACKS) {
            return Err(validation_error(
                "runtime_path_expected_fallback_limit_exceeded",
                "$.max_fallback_count",
                format!("max_fallback_count must not exceed {MAX_FALLBACKS}"),
            ));
        }
        validate_token_list(
            self.allowed_fallback_reason_codes.as_slice(),
            "$.allowed_fallback_reason_codes",
            MAX_FALLBACKS,
            "runtime_path_allowed_fallback_limit_exceeded",
        )?;
        Ok(())
    }
}

/// One exact mismatch between scenario expectations and observed runtime evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePathMismatch {
    /// Stable mismatch reason code.
    pub code: String,
    /// JSONPath-like location of the mismatched evidence.
    pub path: String,
    /// Scenario-declared value.
    pub expected: String,
    /// Runtime-observed value.
    pub actual: String,
}

/// Evaluates exact runtime selectors and explicit fallback allowances.
///
/// The returned mismatch list is deterministic and never removes fallback
/// evidence, including fallbacks whose reason codes were explicitly allowed.
///
/// # Errors
/// Returns [`QaRuntimePathValidationError`] when either contract is malformed.
pub fn evaluate_no_hidden_fallback(
    expectation: &NoHiddenFallbackExpectation,
    evidence: &RuntimePathEvidence,
) -> Result<Vec<RuntimePathMismatch>, QaRuntimePathValidationError> {
    expectation.validate_shape()?;
    evidence.validate_shape()?;

    let mut mismatches = Vec::new();
    if !evidence.complete {
        mismatches.push(runtime_path_mismatch(
            "runtime_path_evidence_incomplete",
            "$.complete",
            "true",
            "false",
        ));
    }
    compare_exact(
        &mut mismatches,
        "runtime_path_contract_version_mismatch",
        "$.runtime_contract_version",
        expectation.runtime_contract_version.as_str(),
        evidence.runtime_contract_version.as_str(),
    );
    compare_exact(
        &mut mismatches,
        "runtime_path_provider_lane_mismatch",
        "$.provider_lane",
        expectation.provider_lane.as_str(),
        evidence.provider_lane.as_str(),
    );
    compare_exact(
        &mut mismatches,
        "runtime_path_attempt_owner_mismatch",
        "$.attempt_owner",
        expectation.attempt_owner.as_str(),
        evidence.attempt_owner.as_str(),
    );
    compare_exact(
        &mut mismatches,
        "runtime_path_harness_mismatch",
        "$.harness.id",
        expectation.harness_id.as_str(),
        evidence.harness.id.as_str(),
    );
    compare_exact(
        &mut mismatches,
        "runtime_path_context_engine_mismatch",
        "$.context_engine.id",
        expectation.context_engine_id.as_str(),
        evidence.context_engine.id.as_str(),
    );
    match (expectation.mcp_transport_mode, evidence.mcp_transport_mode.as_ref()) {
        (None, None) => {}
        (None, Some(actual)) => mismatches.push(runtime_path_mismatch(
            "runtime_path_unexpected_mcp_transport",
            "$.mcp_transport_mode.id",
            "absent",
            actual.id.as_str(),
        )),
        (Some(expected), None) => mismatches.push(runtime_path_mismatch(
            "runtime_path_mcp_transport_missing",
            "$.mcp_transport_mode.id",
            expected.as_str(),
            "absent",
        )),
        (Some(expected), Some(actual)) => compare_exact(
            &mut mismatches,
            "runtime_path_mcp_transport_mismatch",
            "$.mcp_transport_mode.id",
            expected.as_str(),
            actual.id.as_str(),
        ),
    }
    if evidence.fallback_count > expectation.max_fallback_count {
        mismatches.push(runtime_path_mismatch(
            "runtime_path_fallback_count_exceeded",
            "$.fallback_count",
            expectation.max_fallback_count.to_string(),
            evidence.fallback_count.to_string(),
        ));
    }
    let allowed = expectation
        .allowed_fallback_reason_codes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for (index, fallback) in evidence.fallbacks.iter().enumerate() {
        if !allowed.contains(fallback.reason_code.as_str()) {
            mismatches.push(runtime_path_mismatch(
                "runtime_path_fallback_reason_not_allowed",
                format!("$.fallbacks[{index}].reason_code"),
                "one of the explicit allowed_fallback_reason_codes",
                fallback.reason_code.as_str(),
            ));
        }
    }
    Ok(mismatches)
}

/// Validation failure for runtime-path evidence or expectations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaRuntimePathValidationError {
    code: &'static str,
    path: String,
    message: String,
}

impl QaRuntimePathValidationError {
    /// Returns the stable validation reason code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Returns the JSONPath-like location of the invalid field.
    #[must_use]
    pub fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Returns the bounded validation message.
    #[must_use]
    pub fn message(&self) -> &str {
        self.message.as_str()
    }
}

impl fmt::Display for QaRuntimePathValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} at {}: {}", self.code, self.path, self.message)
    }
}

impl Error for QaRuntimePathValidationError {}

fn validate_component(
    component: &RuntimePathComponentEvidence,
    path: &str,
) -> Result<(), QaRuntimePathValidationError> {
    validate_slug_token(component.id.as_str(), format!("{path}.id"), MAX_METADATA_TOKEN_BYTES)?;
    validate_slug_token(
        component.source_event.as_str(),
        format!("{path}.source_event"),
        MAX_METADATA_TOKEN_BYTES,
    )?;
    validate_slug_token(
        component.reason_code.as_str(),
        format!("{path}.reason_code"),
        MAX_METADATA_TOKEN_BYTES,
    )
}

fn validate_fallback(
    fallback: &RuntimeFallbackEvidence,
    index: usize,
) -> Result<(), QaRuntimePathValidationError> {
    let path = format!("$.fallbacks[{index}]");
    validate_slug_token(
        fallback.component.as_str(),
        format!("{path}.component"),
        MAX_METADATA_TOKEN_BYTES,
    )?;
    if let Some(from) = fallback.from.as_deref() {
        validate_slug_token(from, format!("{path}.from"), MAX_METADATA_TOKEN_BYTES)?;
    }
    validate_slug_token(fallback.to.as_str(), format!("{path}.to"), MAX_METADATA_TOKEN_BYTES)?;
    validate_slug_token(
        fallback.reason_code.as_str(),
        format!("{path}.reason_code"),
        MAX_METADATA_TOKEN_BYTES,
    )?;
    validate_slug_token(
        fallback.source_event.as_str(),
        format!("{path}.source_event"),
        MAX_METADATA_TOKEN_BYTES,
    )
}

fn validate_source_bindings(
    evidence: &RuntimePathEvidence,
) -> Result<(), QaRuntimePathValidationError> {
    if evidence.complete && evidence.source_events.is_empty() {
        return Err(validation_error(
            "runtime_path_source_events_required",
            "$.source_events",
            "complete runtime-path evidence requires at least one source event",
        ));
    }
    if evidence.complete && evidence.reason_codes.is_empty() {
        return Err(validation_error(
            "runtime_path_reason_codes_required",
            "$.reason_codes",
            "complete runtime-path evidence requires at least one reason code",
        ));
    }

    let sources = evidence.source_events.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut bindings = vec![
        ("$.harness.source_event".to_owned(), evidence.harness.source_event.as_str()),
        ("$.context_engine.source_event".to_owned(), evidence.context_engine.source_event.as_str()),
    ];
    if let Some(mcp) = evidence.mcp_transport_mode.as_ref() {
        bindings.push(("$.mcp_transport_mode.source_event".to_owned(), mcp.source_event.as_str()));
    }
    bindings.extend(evidence.fallbacks.iter().enumerate().map(|(index, fallback)| {
        (format!("$.fallbacks[{index}].source_event"), fallback.source_event.as_str())
    }));
    for (path, source) in bindings {
        if !sources.contains(source) {
            return Err(validation_error(
                "runtime_path_source_event_unbound",
                path,
                format!("source event '{source}' is absent from $.source_events"),
            ));
        }
    }
    Ok(())
}

fn validate_provider_lane(value: &str, path: &str) -> Result<(), QaRuntimePathValidationError> {
    if PROVIDER_LANES.contains(&value) {
        return Ok(());
    }
    Err(validation_error(
        "runtime_path_provider_lane_invalid",
        path,
        format!("expected one of {}, got '{value}'", PROVIDER_LANES.join(", ")),
    ))
}

fn validate_sha256_hex(value: &str, path: &str) -> Result<(), QaRuntimePathValidationError> {
    if value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Ok(());
    }
    Err(validation_error(
        "runtime_path_sha256_invalid",
        path,
        "expected exactly 64 lowercase hexadecimal characters",
    ))
}

fn validate_token_list(
    values: &[String],
    path: &str,
    max_items: usize,
    limit_code: &'static str,
) -> Result<(), QaRuntimePathValidationError> {
    if values.len() > max_items {
        return Err(validation_error(
            limit_code,
            path,
            format!("list must not contain more than {max_items} items"),
        ));
    }
    let mut unique = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        validate_slug_token(value.as_str(), format!("{path}[{index}]"), MAX_METADATA_TOKEN_BYTES)?;
        if !unique.insert(value.as_str()) {
            return Err(validation_error(
                "runtime_path_metadata_duplicate",
                format!("{path}[{index}]"),
                format!("duplicate metadata token '{value}'"),
            ));
        }
    }
    Ok(())
}

fn validate_slug_token(
    value: &str,
    path: impl Into<String>,
    max_bytes: usize,
) -> Result<(), QaRuntimePathValidationError> {
    let path = path.into();
    let valid = !value.is_empty()
        && value.len() <= max_bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        return Ok(());
    }
    Err(validation_error(
        "runtime_path_metadata_invalid",
        path,
        format!(
            "metadata must be 1..={max_bytes} ASCII bytes using letters, digits, '.', '_', or '-'"
        ),
    ))
}

fn validate_mcp_identifier_token(
    value: &str,
    path: impl Into<String>,
    max_bytes: usize,
) -> Result<(), QaRuntimePathValidationError> {
    let path = path.into();
    let valid = !value.is_empty()
        && value.len() <= max_bytes
        && !matches!(value.as_bytes().first(), Some(b'.' | b':'))
        && !matches!(value.as_bytes().last(), Some(b'.' | b':'))
        && !value.contains("..")
        && !value.contains("::")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'));
    if valid {
        return Ok(());
    }
    Err(validation_error(
        "runtime_path_metadata_invalid",
        path,
        format!(
            "MCP identifier must be 1..={max_bytes} safe ASCII bytes using valid dot or colon segments"
        ),
    ))
}

fn validate_version_token(
    value: &str,
    path: impl Into<String>,
    max_bytes: usize,
) -> Result<(), QaRuntimePathValidationError> {
    let path = path.into();
    let has_drive_prefix = value.as_bytes().get(1) == Some(&b':')
        && value.as_bytes().first().is_some_and(u8::is_ascii_alphabetic);
    let valid = !value.is_empty()
        && value.len() <= max_bytes
        && !value.starts_with('/')
        && !value.starts_with('\\')
        && !has_drive_prefix
        && !value.contains("\\")
        && !value.contains("://")
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        });
    if valid {
        return Ok(());
    }
    Err(validation_error(
        "runtime_path_metadata_invalid",
        path,
        format!("version metadata must be a non-path token of 1..={max_bytes} safe ASCII bytes"),
    ))
}

fn compare_exact(
    mismatches: &mut Vec<RuntimePathMismatch>,
    code: &'static str,
    path: &'static str,
    expected: &str,
    actual: &str,
) {
    if expected != actual {
        mismatches.push(runtime_path_mismatch(code, path, expected, actual));
    }
}

fn runtime_path_mismatch(
    code: impl Into<String>,
    path: impl Into<String>,
    expected: impl Into<String>,
    actual: impl Into<String>,
) -> RuntimePathMismatch {
    RuntimePathMismatch {
        code: code.into(),
        path: path.into(),
        expected: expected.into(),
        actual: actual.into(),
    }
}

fn validation_error(
    code: &'static str,
    path: impl Into<String>,
    message: impl Into<String>,
) -> QaRuntimePathValidationError {
    QaRuntimePathValidationError { code, path: path.into(), message: message.into() }
}

#[cfg(test)]
mod tests;
