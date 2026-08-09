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
mod reconciliation;
mod schema;
mod types;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use catalog::effective_tool_surface_report;
#[cfg(test)]
pub(crate) use catalog::{
    build_model_visible_tool_catalog_snapshot,
    build_model_visible_tool_catalog_snapshot_with_external_tools,
};
pub(crate) use catalog::{
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
#[cfg(test)]
pub(crate) use reconciliation::tool_execution_semantics;
pub(crate) use reconciliation::{resolve_tool_execution_semantics, safe_resume_matrix};
pub(crate) use schema::sanitize_schema_for_provider;
pub(crate) use types::{
    FilteredToolCatalogEntry, ModelVisibleToolCatalogSnapshot, NormalizedToolCall,
    ToolApprovalPosture, ToolArgumentNormalizationAudit, ToolCallRejection, ToolCatalogBridgeError,
    ToolCatalogBuildRequest, ToolCatalogFilterReasonCode, ToolCatalogPolicySnapshot,
    ToolExposureSurface, ToolParallelismPolicy, ToolRegistryEntry, ToolReplaySafetyClass,
    ToolResultProjectionPolicy, ToolSchemaDialect, TOOL_CATALOG_DESCRIBE_TOOL_NAME,
    TOOL_CATALOG_INVOKE_TOOL_NAME, TOOL_CATALOG_SEARCH_TOOL_NAME,
};

/// Converts one verified durable dynamic-tool version into the standard
/// registry model used by every provider surface.
pub(crate) fn dynamic_tool_registry_entry(
    record: &crate::journal::dynamic_tools::DynamicToolActiveRecord,
) -> ToolRegistryEntry {
    let proposal = &record.artifact.proposal;
    let parallelism_policy = if !proposal.semantics.mutating {
        ToolParallelismPolicy::ReadOnly
    } else if proposal.semantics.idempotent {
        ToolParallelismPolicy::Idempotent
    } else {
        ToolParallelismPolicy::Exclusive
    };
    ToolRegistryEntry {
        name: proposal.tool_name.clone(),
        description: proposal.description.clone(),
        version: 1,
        provenance: dynamic_tool_record_provenance(record),
        input_schema: proposal.input_schema.clone(),
        schema_hash: stable_hash_value(&proposal.input_schema),
        capabilities: proposal.capability_needs.clone(),
        // Dynamic names are fail-closed in the standard policy metadata, so the
        // catalog must not advertise a weaker posture than dispatch enforces.
        approval_posture: ToolApprovalPosture::ApprovalRequired,
        projection_policy: ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        parallelism_policy,
        replay_safety_class: ToolReplaySafetyClass::RequiresHumanConfirmation,
        target_surfaces: vec![ToolExposureSurface::RunStream, ToolExposureSurface::RouteMessage],
    }
}

/// Keeps dynamic-tool rollout authority inside the registry seam instead of
/// adding feature branches to frozen orchestration call sites.
pub(crate) fn active_dynamic_tool_registry_entries(
    runtime_state: &crate::gateway::GatewayRuntimeState,
) -> Result<Vec<ToolRegistryEntry>, crate::journal::JournalError> {
    if !runtime_state.config.feature_rollouts.dynamic_tool_builder.enabled {
        return Ok(Vec::new());
    }
    runtime_state
        .journal_store
        .active_dynamic_tools()
        .map(|records| records.iter().map(dynamic_tool_registry_entry).collect())
}

/// Returns the exact digest/generation fence embedded in a catalog snapshot.
pub(crate) fn dynamic_tool_record_provenance(
    record: &crate::journal::dynamic_tools::DynamicToolActiveRecord,
) -> String {
    format!(
        "dynamic:{}:{}:{}:{}:{}",
        record.artifact.artifact_sha256,
        record.runtime_eval.evidence_sha256,
        record.decision.approval_generation,
        record.decision.catalog_epoch,
        record.registry_catalog_epoch,
    )
}

/// Extracts a dynamic entry's exact provenance from the snapshot seen by the model.
pub(crate) fn dynamic_tool_snapshot_provenance(
    snapshot: &ModelVisibleToolCatalogSnapshot,
    tool_name: &str,
) -> Result<Option<String>, &'static str> {
    if !tool_name.starts_with("dynamic.") {
        return Ok(None);
    }
    let exposed = unique_dynamic_tool_provenance(snapshot.tools.as_slice(), tool_name)?;
    let indexed = unique_dynamic_tool_provenance(snapshot.indexed_tools.as_slice(), tool_name)?;
    let Some(provenance) = exposed.or(indexed) else {
        return Err("dynamic_tool.catalog_binding_missing");
    };
    if exposed.zip(indexed).is_some_and(|(left, right)| left != right) {
        return Err("dynamic_tool.catalog_binding_invalid");
    }
    Ok(Some(provenance.to_owned()))
}

fn unique_dynamic_tool_provenance<'a>(
    tools: &'a [types::ModelVisibleTool],
    tool_name: &str,
) -> Result<Option<&'a str>, &'static str> {
    let mut matches = tools.iter().filter(|tool| tool.name == tool_name);
    let Some(tool) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() || !tool.provenance.starts_with("dynamic:") {
        return Err("dynamic_tool.catalog_binding_invalid");
    }
    Ok(Some(tool.provenance.as_str()))
}
