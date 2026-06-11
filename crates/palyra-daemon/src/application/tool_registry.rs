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

pub(crate) use catalog::{
    build_model_visible_tool_catalog_snapshot, projection_policy_for_tool,
    provider_tools_from_catalog_snapshot, snapshot_to_provider_request_value,
    tool_catalog_tape_payload,
};
pub(crate) use normalization::{
    normalization_audit_tape_payload, rejection_tape_payload, tool_call_rejection_outcome,
    validate_tool_call_against_catalog_snapshot,
};
pub(crate) use types::{
    ModelVisibleToolCatalogSnapshot, NormalizedToolCall, ToolArgumentNormalizationAudit,
    ToolCallRejection, ToolCatalogBuildRequest, ToolExposureSurface, ToolResultProjectionPolicy,
    ToolSchemaDialect,
};
