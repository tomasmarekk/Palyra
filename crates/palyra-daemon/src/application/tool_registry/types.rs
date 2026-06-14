//! Shared types for the model-visible tool registry: schema-version constants,
//! dialect/policy enums with stable wire labels, and the catalog-snapshot,
//! normalization-audit, and rejection records exchanged between the catalog
//! builder and tool-call intake.
//!
//! The `as_str` labels and serde shapes here feed catalog hashing and tape
//! payloads; changing any of them changes hashes and replay output.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::tool_protocol::{ToolCallConfig, ToolRequestContext};

/// Schema version stamped into every catalog snapshot; bump on breaking payload changes.
pub(super) const TOOL_CATALOG_SCHEMA_VERSION: u32 = 1;
/// Version stamped into every builtin registry entry.
pub(super) const TOOL_REGISTRY_ENTRY_VERSION: u32 = 1;
/// Schema version stamped into every [`ToolCallRejection`] record.
pub(super) const TOOL_REJECTION_SCHEMA_VERSION: u32 = 1;
/// Maximum schema nesting depth accepted by provider sanitization and validation.
pub(super) const MAX_SCHEMA_DEPTH: usize = 8;
/// Maximum number of properties accepted on a single object schema node.
pub(super) const MAX_SCHEMA_PROPERTIES: usize = 128;

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
    pub(crate) description_hash: String,
    pub(crate) capabilities: Vec<String>,
    pub(crate) approval_posture: ToolApprovalPosture,
    pub(crate) projection_policy: ToolResultProjectionPolicy,
    pub(crate) parallelism_policy: ToolParallelismPolicy,
    pub(crate) exposure_reason: String,
}

/// A tool excluded from a catalog snapshot, with the filter reason and an
/// operator-facing repair hint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FilteredToolCatalogEntry {
    pub(crate) name: String,
    pub(crate) reason_code: ToolCatalogFilterReasonCode,
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
    pub(crate) tools: Vec<ModelVisibleTool>,
    pub(crate) filtered_tools: Vec<FilteredToolCatalogEntry>,
}

/// Inputs for building one [`ModelVisibleToolCatalogSnapshot`].
pub(crate) struct ToolCatalogBuildRequest<'a> {
    pub(crate) config: &'a ToolCallConfig,
    pub(crate) browser_service_enabled: bool,
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
