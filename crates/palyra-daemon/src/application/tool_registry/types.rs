//! Shared types for the model-visible tool registry: schema-version constants,
//! dialect/policy enums with stable wire labels, and the catalog-snapshot,
//! normalization-audit, and rejection records exchanged between the catalog
//! builder and tool-call intake.
//!
//! The `as_str` labels and serde shapes here feed catalog hashing and tape
//! payloads; changing any of them changes hashes and replay output.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use palyra_common::tool_catalog::{
    expand_toolset_profiles, ToolCatalogExposureMode, ToolsetProfileExpansionReport,
};

use crate::tool_protocol::{ToolCallConfig, ToolRequestContext};

/// Schema version stamped into every catalog snapshot; bump on breaking payload changes.
pub(super) const TOOL_CATALOG_SCHEMA_VERSION: u32 = 1;
/// Version stamped into every builtin registry entry.
pub(super) const TOOL_REGISTRY_ENTRY_VERSION: u32 = 1;
/// Schema version stamped into every [`ToolCallRejection`] record.
pub(super) const TOOL_REJECTION_SCHEMA_VERSION: u32 = 1;
/// Schema version stamped into provider-schema transform audit records.
pub(super) const TOOL_SCHEMA_TRANSFORM_AUDIT_VERSION: u32 = 1;
/// Maximum schema nesting depth accepted by provider sanitization and validation.
pub(super) const MAX_SCHEMA_DEPTH: usize = 8;
/// Maximum number of properties accepted on a single object schema node.
pub(super) const MAX_SCHEMA_PROPERTIES: usize = 128;
/// Catalog bridge tool that searches compact/hybrid tool indexes.
pub(crate) const TOOL_CATALOG_SEARCH_TOOL_NAME: &str = "palyra.tools.search";
/// Catalog bridge tool that describes one indexed tool.
pub(crate) const TOOL_CATALOG_DESCRIBE_TOOL_NAME: &str = "palyra.tools.describe";
/// Catalog bridge tool that invokes one indexed tool after schema validation.
pub(crate) const TOOL_CATALOG_INVOKE_TOOL_NAME: &str = "palyra.tools.invoke";

/// Provider schema dialect a catalog snapshot is sanitized and serialized for.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolSchemaDialect {
    OpenAiCompatible,
    Anthropic,
    Deterministic,
}

impl ToolSchemaDialect {
    /// Maps a provider kind string to its dialect.
    ///
    /// Unknown kinds fall back to [`Self::OpenAiCompatible`], the most widely
    /// implemented tool wire shape.
    pub(crate) fn from_provider_kind(provider_kind: &str) -> Self {
        match provider_kind.trim().to_ascii_lowercase().as_str() {
            "anthropic" => Self::Anthropic,
            "deterministic" => Self::Deterministic,
            _ => Self::OpenAiCompatible,
        }
    }

    /// Stable snake_case label used in catalog hashes and tape payloads.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiCompatible => "openai_compatible",
            Self::Anthropic => "anthropic",
            Self::Deterministic => "deterministic",
        }
    }
}

/// Runtime surface a tool catalog is built for and a tool may be exposed on.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolExposureSurface {
    RunStream,
    RouteMessage,
}

impl ToolExposureSurface {
    /// Stable snake_case label used in catalog hashes and tape payloads.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RunStream => "run_stream",
            Self::RouteMessage => "route_message",
        }
    }
}

/// Whether a tool runs without approval or requires an operator approval gate.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolApprovalPosture {
    Safe,
    ApprovalRequired,
}

impl ToolApprovalPosture {
    /// Stable snake_case label used in catalog hashes and tape payloads.
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::ApprovalRequired => "approval_required",
        }
    }
}

/// How a tool may be scheduled relative to other tool calls in the same turn.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolParallelismPolicy {
    ReadOnly,
    Idempotent,
    Exclusive,
}

impl ToolParallelismPolicy {
    /// Stable snake_case label used in catalog hashes and tape payloads.
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Idempotent => "idempotent",
            Self::Exclusive => "exclusive",
        }
    }
}

/// Replay safety class used by compaction, trajectory export, and replay
/// runners to decide whether a tool call may be repeated automatically.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolReplaySafetyClass {
    ReadOnly,
    IdempotentWrite,
    NonIdempotentWrite,
    ExternalSideEffect,
    RequiresHumanConfirmation,
}

impl ToolReplaySafetyClass {
    /// Stable snake_case label used in catalog hashes and tape payloads.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::IdempotentWrite => "idempotent_write",
            Self::NonIdempotentWrite => "non_idempotent_write",
            Self::ExternalSideEffect => "external_side_effect",
            Self::RequiresHumanConfirmation => "requires_human_confirmation",
        }
    }

    /// Returns true when replay needs prior execution evidence or explicit
    /// human confirmation before repeating the call.
    pub(crate) const fn requires_replay_evidence(self) -> bool {
        matches!(
            self,
            Self::NonIdempotentWrite | Self::ExternalSideEffect | Self::RequiresHumanConfirmation
        )
    }
}

/// How a tool result is projected back into model context (inline, summarized, or redacted).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolResultProjectionPolicy {
    InlineUnlessLarge,
    SummarizeAndArtifact,
    RedactedPreviewAndArtifact,
}

impl ToolResultProjectionPolicy {
    /// Stable snake_case label used in catalog hashes and tape payloads.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InlineUnlessLarge => "inline_unless_large",
            Self::SummarizeAndArtifact => "summarize_and_artifact",
            Self::RedactedPreviewAndArtifact => "redacted_preview_and_artifact",
        }
    }
}

/// Why a registry entry was excluded from a model-visible catalog snapshot.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolCatalogFilterReasonCode {
    NotAllowlisted,
    UnknownTool,
    RuntimeUnavailable,
    ProviderSchemaIncompatible,
    SurfaceUnsupported,
    BudgetExhausted,
    PolicyInvisible,
}

impl ToolCatalogFilterReasonCode {
    /// Stable snake_case label used in catalog hashes and tape payloads.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NotAllowlisted => "not_allowlisted",
            Self::UnknownTool => "unknown_tool",
            Self::RuntimeUnavailable => "runtime_unavailable",
            Self::ProviderSchemaIncompatible => "provider_schema_incompatible",
            Self::SurfaceUnsupported => "surface_unsupported",
            Self::BudgetExhausted => "budget_exhausted",
            Self::PolicyInvisible => "policy_invisible",
        }
    }
}

/// A builtin tool's registry metadata before per-provider catalog filtering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolRegistryEntry {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) version: u32,
    pub(crate) provenance: String,
    pub(crate) input_schema: Value,
    pub(crate) schema_hash: String,
    pub(crate) capabilities: Vec<String>,
    pub(crate) approval_posture: ToolApprovalPosture,
    pub(crate) projection_policy: ToolResultProjectionPolicy,
    pub(crate) parallelism_policy: ToolParallelismPolicy,
    pub(crate) replay_safety_class: ToolReplaySafetyClass,
    pub(crate) target_surfaces: Vec<ToolExposureSurface>,
}

/// A tool as exposed to the model: internal and provider-sanitized schemas
/// plus stable content hashes for audit and replay comparison.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ModelVisibleTool {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) version: u32,
    pub(crate) provenance: String,
    pub(crate) schema: Value,
    pub(crate) provider_schema: Value,
    pub(crate) internal_schema_hash: String,
    pub(crate) provider_schema_hash: String,
    pub(crate) provider_schema_transform: ToolSchemaTransformAudit,
    pub(crate) description_hash: String,
    pub(crate) capabilities: Vec<String>,
    pub(crate) approval_posture: ToolApprovalPosture,
    pub(crate) projection_policy: ToolResultProjectionPolicy,
    pub(crate) parallelism_policy: ToolParallelismPolicy,
    pub(crate) replay_safety_class: ToolReplaySafetyClass,
    pub(crate) exposure_reason: String,
}

/// Hash-only audit of provider schema normalization for one exposed tool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolSchemaTransformAudit {
    pub(crate) schema_version: u32,
    pub(crate) dialect: ToolSchemaDialect,
    pub(crate) input_schema_hash: String,
    pub(crate) output_schema_hash: String,
    pub(crate) steps: Vec<ToolSchemaTransformStep>,
}

/// One deterministic provider-schema transform step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolSchemaTransformStep {
    pub(crate) json_pointer: String,
    pub(crate) reason_code: String,
    pub(crate) from: String,
    pub(crate) to: String,
}

/// One compact index entry describing a policy-visible target tool without
/// shipping its full schema to the provider prompt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolCatalogIndexEntry {
    pub(crate) name: String,
    pub(crate) short_description: String,
    pub(crate) keywords: Vec<String>,
    pub(crate) capability_class: String,
    pub(crate) risk_tier: String,
    pub(crate) approval_summary: String,
    pub(crate) exposure_reason: String,
    pub(crate) repair_hint: String,
    pub(crate) projection_policy: ToolResultProjectionPolicy,
    pub(crate) replay_safety_class: ToolReplaySafetyClass,
    pub(crate) provider_schema_hash: String,
    pub(crate) internal_schema_hash: String,
}

/// Compact searchable index for all authorized target tools.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolCatalogIndex {
    pub(crate) schema_version: u32,
    pub(crate) index_digest: String,
    pub(crate) entries: Vec<ToolCatalogIndexEntry>,
}

/// Error returned by compact-catalog bridge helpers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolCatalogBridgeError {
    pub(crate) reason_code: String,
    pub(crate) message: String,
}

/// Resolved target call for `palyra.tools.invoke`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCatalogInvokeTarget {
    pub(crate) tool_name: String,
    pub(crate) schema_digest: String,
    pub(crate) input_json: Vec<u8>,
}

/// Cached runtime-availability probe result captured while building a catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AvailabilityProbeResult {
    pub(crate) runtime: String,
    pub(crate) status: String,
    pub(crate) cache_status: String,
    pub(crate) ttl_ms: u64,
    pub(crate) checked_at_unix_ms: i64,
    pub(crate) ttl_expires_unix_ms: i64,
    pub(crate) last_good_unix_ms: Option<i64>,
    pub(crate) last_good_grace_until_unix_ms: Option<i64>,
    pub(crate) reason_code: String,
    pub(crate) repair_hint: String,
    pub(crate) cache_key_hash: String,
    pub(crate) config_hash: String,
    pub(crate) grace_allowed: bool,
}

/// A tool excluded from a catalog snapshot, with the filter reason and an
/// operator-facing repair hint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FilteredToolCatalogEntry {
    pub(crate) name: String,
    pub(crate) reason_code: ToolCatalogFilterReasonCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) external_reason_code: Option<String>,
    pub(crate) repair_hint: String,
}

/// Immutable per-provider-turn tool catalog.
///
/// `catalog_hash` covers every model-visible field (see
/// `hashing::catalog_hash_payload`) and `snapshot_id` is derived from it, so
/// identical build inputs always produce identical ids.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ModelVisibleToolCatalogSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) snapshot_id: String,
    pub(crate) catalog_hash: String,
    pub(crate) provider_dialect: ToolSchemaDialect,
    pub(crate) provider_kind: String,
    pub(crate) provider_model_id: Option<String>,
    pub(crate) surface: ToolExposureSurface,
    pub(crate) principal_hash: String,
    pub(crate) channel_hash: Option<String>,
    pub(crate) remaining_tool_budget: Option<u32>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) profile_expansion: ToolsetProfileExpansionReport,
    pub(crate) exposure_mode: ToolCatalogExposureMode,
    pub(crate) compact_tool_threshold: usize,
    pub(crate) direct_tool_count: usize,
    pub(crate) exposed_tool_count: usize,
    pub(crate) estimated_direct_tool_bytes: usize,
    pub(crate) estimated_exposed_tool_bytes: usize,
    pub(crate) estimated_saved_bytes: usize,
    pub(crate) availability_probes: Vec<AvailabilityProbeResult>,
    pub(crate) index: ToolCatalogIndex,
    pub(crate) indexed_tools: Vec<ModelVisibleTool>,
    pub(crate) tools: Vec<ModelVisibleTool>,
    pub(crate) filtered_tools: Vec<FilteredToolCatalogEntry>,
}

/// Per-runtime catalog policy after daemon config precedence has been applied.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolCatalogPolicySnapshot {
    pub(crate) profile_expansion: ToolsetProfileExpansionReport,
    pub(crate) exposure_mode: ToolCatalogExposureMode,
    pub(crate) compact_tool_threshold: usize,
}

impl ToolCatalogPolicySnapshot {
    /// Builds a policy snapshot from the loaded daemon tool-call config.
    #[must_use]
    pub(crate) fn from_loaded_tool_call_config(config: &crate::config::ToolCallConfig) -> Self {
        let profile_expansion = expand_toolset_profiles(
            config.toolset_profiles.as_slice(),
            config.explicit_allowed_tools.as_slice(),
            config.extra_tools.as_slice(),
            config.disabled_tools.as_slice(),
        )
        .unwrap_or_else(|_| ToolsetProfileExpansionReport {
            profiles: config.toolset_profiles.clone(),
            profile_expansions: Vec::new(),
            explicit_allowed_tools: config.explicit_allowed_tools.clone(),
            extra_tools: config.extra_tools.clone(),
            disabled_tools: config.disabled_tools.clone(),
            effective_allowed_tools: config.allowed_tools.clone(),
        });
        Self {
            profile_expansion,
            exposure_mode: config.catalog_exposure_mode,
            compact_tool_threshold: config.compact_tool_threshold,
        }
    }

    /// Builds the legacy direct policy used by tests and older in-process call sites.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn direct_from_allowed_tools(allowed_tools: &[String]) -> Self {
        let profile_expansion = expand_toolset_profiles(&[], allowed_tools, &[], &[])
            .unwrap_or_else(|_| ToolsetProfileExpansionReport {
                profiles: Vec::new(),
                profile_expansions: Vec::new(),
                explicit_allowed_tools: allowed_tools.to_vec(),
                extra_tools: Vec::new(),
                disabled_tools: Vec::new(),
                effective_allowed_tools: allowed_tools.to_vec(),
            });
        Self {
            profile_expansion,
            exposure_mode: ToolCatalogExposureMode::Direct,
            compact_tool_threshold: 16,
        }
    }
}

/// Inputs for building one [`ModelVisibleToolCatalogSnapshot`].
pub(crate) struct ToolCatalogBuildRequest<'a> {
    pub(crate) config: &'a ToolCallConfig,
    pub(crate) catalog_policy: &'a ToolCatalogPolicySnapshot,
    pub(crate) browser_service_enabled: bool,
    pub(crate) browser_service_configured: bool,
    pub(crate) request_context: &'a ToolRequestContext,
    pub(crate) provider_kind: &'a str,
    pub(crate) provider_model_id: Option<&'a str>,
    pub(crate) surface: ToolExposureSurface,
    pub(crate) remaining_tool_budget: Option<u32>,
    pub(crate) created_at_unix_ms: i64,
}

/// One recorded type coercion applied during argument normalization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolArgumentNormalizationStep {
    pub(crate) json_pointer: String,
    pub(crate) from_type: String,
    pub(crate) to_type: String,
    pub(crate) reason_code: String,
}

/// Hash-anchored audit trail of every normalization step applied to one call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolArgumentNormalizationAudit {
    pub(crate) raw_json_hash: String,
    pub(crate) normalized_json_hash: String,
    pub(crate) steps: Vec<ToolArgumentNormalizationStep>,
}

/// Canonicalized tool-call arguments accepted by intake, plus their audit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NormalizedToolCall {
    pub(crate) input_json: Vec<u8>,
    pub(crate) audit: ToolArgumentNormalizationAudit,
}

/// Category of a tool-call intake rejection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolCallRejectionKind {
    UnknownTool,
    UnavailableTool,
    MalformedArguments,
    SchemaInvalid,
    PolicyInvisible,
    UnsupportedParallelism,
}

impl ToolCallRejectionKind {
    /// Stable snake_case label used in rejection payloads and tape output.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownTool => "unknown_tool",
            Self::UnavailableTool => "unavailable_tool",
            Self::MalformedArguments => "malformed_arguments",
            Self::SchemaInvalid => "schema_invalid",
            Self::PolicyInvisible => "policy_invisible",
            Self::UnsupportedParallelism => "unsupported_parallelism",
        }
    }
}

/// Structured rejection emitted when a tool call fails catalog validation,
/// carrying the snapshot identity so replays can correlate it with the catalog
/// the model actually saw.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolCallRejection {
    pub(crate) schema_version: u32,
    pub(crate) kind: ToolCallRejectionKind,
    pub(crate) tool_name: String,
    pub(crate) reason_code: String,
    pub(crate) message: String,
    pub(crate) raw_json_hash: String,
    pub(crate) snapshot_id: Option<String>,
    pub(crate) catalog_hash: Option<String>,
}
