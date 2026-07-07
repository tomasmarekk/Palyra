//! Model-visible tool registry: builtin tool metadata, per-provider-turn
//! catalog snapshots, and tool-call argument normalization against those
//! snapshots.
//!
//! Flow: `builtin` declares the static registry entries, `catalog` filters
//! them into a content-hashed `ModelVisibleToolCatalogSnapshot` for one
//! provider turn, and `normalization` validates incoming tool calls against
//! that snapshot. `hashing` provides the canonical-JSON hashing that keeps
//! snapshot ids and catalog hashes stable across runs.

mod builtin;
mod catalog;
mod hashing;
mod normalization;
mod schema;
mod types;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use catalog::build_model_visible_tool_catalog_snapshot_with_external_tools;
#[cfg(test)]
pub(crate) use catalog::effective_tool_surface_report;
pub(crate) use catalog::{
    build_model_visible_tool_catalog_snapshot,
    build_model_visible_tool_catalog_snapshot_with_external_records, describe_catalog_tool,
    projection_policy_for_tool, provider_tools_from_catalog_snapshot,
    resolve_catalog_invoke_target, search_tool_catalog_index, snapshot_to_provider_request_value,
    tool_catalog_tape_payload,
};
pub(crate) use hashing::{canonical_json_bytes, stable_hash_bytes, stable_hash_value};
pub(crate) use normalization::{
    normalization_audit_tape_payload, rejection_tape_payload, tool_call_rejection_outcome,
    validate_tool_call_against_catalog_snapshot, validate_tool_call_against_model_visible_tool,
};
pub(crate) use schema::sanitize_schema_for_provider;
pub(crate) use types::{
    FilteredToolCatalogEntry, ModelVisibleToolCatalogSnapshot, NormalizedToolCall,
    ToolApprovalPosture, ToolArgumentNormalizationAudit, ToolCallRejection, ToolCatalogBridgeError,
    ToolCatalogBuildRequest, ToolCatalogFilterReasonCode, ToolCatalogPolicySnapshot,
    ToolExposureSurface, ToolParallelismPolicy, ToolRegistryEntry, ToolReplaySafetyClass,
    ToolResultProjectionPolicy, ToolSchemaDialect, TOOL_CATALOG_DESCRIBE_TOOL_NAME,
    TOOL_CATALOG_INVOKE_TOOL_NAME, TOOL_CATALOG_SEARCH_TOOL_NAME,
};
