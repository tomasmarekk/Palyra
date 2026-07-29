//! Bounded operator recovery for managed coding worktrees and snapshots.
//!
//! Mutations require an administrator console session, retain generation
//! fences, preserve active process/LSP ownership checks, and emit audit events.

use crate::application::coding_runtime::CodingTaskCleanupOutcomeV2;
use crate::application::managed_coding_recovery::{
    ManagedCodingRecoveryInventoryV1, ManagedCodingSnapshotGcOutcomeV1,
    ManagedCodingWorktreeMutationV1,
};
use crate::application::managed_worktree_snapshots::{
    WorktreeRestoreReportV1, WorktreeSnapshotDescriptorV1,
};
use crate::*;

const RECOVERY_REQUEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleManagedWorktreeRetainRequest {
    schema_version: u32,
    generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleManagedWorktreeCleanupRequest {
    schema_version: u32,
    run_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleManagedSnapshotGcRequest {
    schema_version: u32,
    #[serde(default)]
    force: bool,
}

/// Lists redacted worktree records and bounded snapshot summaries.
///
/// # Errors
/// Returns an error response when administrator authorization or durable
/// registry access fails.
pub(crate) async fn console_managed_coding_recovery_inventory_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ManagedCodingRecoveryInventoryV1>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let inventory =
        state.runtime.managed_coding_recovery_inventory().await.map_err(runtime_status_response)?;
    Ok(Json(inventory))
}

/// Loads one snapshot descriptor without reading its artifact payloads.
///
/// # Errors
/// Returns an error response for invalid identity, authorization, or storage
/// failure.
pub(crate) async fn console_managed_coding_snapshot_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(snapshot_id): Path<String>,
) -> Result<Json<WorktreeSnapshotDescriptorV1>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let snapshot_id = normalize_non_empty_field(snapshot_id, "snapshot_id")?;
    let descriptor = state
        .runtime
        .managed_coding_snapshot_descriptor(snapshot_id)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(descriptor))
}

/// Restores byte-exact dirty state after base, conflict, and artifact checks.
///
/// # Errors
/// Returns an error response when authorization or any restore invariant
/// fails.
pub(crate) async fn console_managed_coding_snapshot_restore_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(snapshot_id): Path<String>,
) -> Result<Json<WorktreeRestoreReportV1>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let snapshot_id = normalize_non_empty_field(snapshot_id, "snapshot_id")?;
    let report = state
        .runtime
        .restore_managed_coding_snapshot(snapshot_id.clone())
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_console_event(
            &session.context,
            "coding.snapshot.restored",
            json!({
                "snapshot_id": snapshot_id,
                "reason_code": "coding.snapshot_restored",
            }),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(report))
}

/// Retains one unlocked worktree using the caller's exact generation fence.
///
/// # Errors
/// Returns an error response for invalid schema, stale generation, active
/// ownership, authorization, or storage failure.
pub(crate) async fn console_managed_coding_worktree_retain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(worktree_id): Path<String>,
    Json(payload): Json<ConsoleManagedWorktreeRetainRequest>,
) -> Result<Json<ManagedCodingWorktreeMutationV1>, Response> {
    validate_recovery_schema(payload.schema_version).map_err(runtime_status_response)?;
    let session = authorize_console_session(&state, &headers, true)?;
    let worktree_id = normalize_non_empty_field(worktree_id, "worktree_id")?;
    let record = state
        .runtime
        .retain_managed_coding_worktree(worktree_id.clone(), payload.generation)
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_console_event(
            &session.context,
            "coding.worktree.retained",
            json!({
                "worktree_id": worktree_id,
                "requested_generation": payload.generation,
                "reason_code": "coding.operator_retained",
            }),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(record))
}

/// Reconciles one interrupted worktree after managed process and LSP leases
/// prove that no live authority remains.
///
/// # Errors
/// Returns an error response for invalid schema, mismatched run ownership,
/// active leases, authorization, or cleanup failure.
pub(crate) async fn console_managed_coding_worktree_cleanup_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(worktree_id): Path<String>,
    Json(payload): Json<ConsoleManagedWorktreeCleanupRequest>,
) -> Result<Json<CodingTaskCleanupOutcomeV2>, Response> {
    validate_recovery_schema(payload.schema_version).map_err(runtime_status_response)?;
    let session = authorize_console_session(&state, &headers, true)?;
    let worktree_id = normalize_non_empty_field(worktree_id, "worktree_id")?;
    let run_id = normalize_non_empty_field(payload.run_id, "run_id")?;
    let report = state
        .runtime
        .reconcile_managed_coding_worktree(worktree_id.clone(), run_id)
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_console_event(
            &session.context,
            "coding.worktree.reconciled",
            json!({
                "worktree_id": worktree_id,
                "run_identity_supplied": true,
                "reason_code": "coding.worktree_reconciled",
            }),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(report))
}

/// Removes one retained snapshot while active leases remain non-bypassable.
///
/// `force=true` bypasses only passive worktree retention. It never bypasses
/// process, PTY, LSP, MCP, or external-runtime ownership.
///
/// # Errors
/// Returns an error response for invalid schema, authorization, active
/// ownership, or storage failure.
pub(crate) async fn console_managed_coding_snapshot_gc_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(snapshot_id): Path<String>,
    Json(payload): Json<ConsoleManagedSnapshotGcRequest>,
) -> Result<Json<ManagedCodingSnapshotGcOutcomeV1>, Response> {
    validate_recovery_schema(payload.schema_version).map_err(runtime_status_response)?;
    let session = authorize_console_session(&state, &headers, true)?;
    let snapshot_id = normalize_non_empty_field(snapshot_id, "snapshot_id")?;
    let report = state
        .runtime
        .gc_managed_coding_snapshot(snapshot_id.clone(), payload.force)
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_console_event(
            &session.context,
            "coding.snapshot.gc",
            json!({
                "snapshot_id": snapshot_id,
                "force_requested": payload.force,
                "decision": report.decision,
            }),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(report))
}

fn validate_recovery_schema(schema_version: u32) -> Result<(), tonic::Status> {
    if schema_version == RECOVERY_REQUEST_SCHEMA_VERSION {
        return Ok(());
    }
    Err(tonic::Status::invalid_argument("managed coding recovery request schema_version must be 1"))
}
