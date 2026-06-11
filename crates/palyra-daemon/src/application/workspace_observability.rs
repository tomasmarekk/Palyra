//! Workspace checkpoint observability: artifact listing, diffing, and restore.
//!
//! Workspace-mutating tools record journal-backed checkpoints (preflight and
//! post-change snapshots of every touched file). This module captures those
//! checkpoints ([`capture_workspace_patch_checkpoint`]), aggregates them into
//! per-path artifact histories for console/run views, diffs two anchors (runs
//! or checkpoints), and restores checkpoint state back onto disk.
//!
//! Restore and capture both write through path-containment guards: relative
//! paths are validated component-by-component, targets are canonicalized (or
//! their nearest existing ancestor is), and symlinks anywhere on the resolved
//! path are rejected before any filesystem mutation. Treat changes to those
//! guards as security changes. All inline payloads served to clients are
//! bounded by the `MAX_*` constants below.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_common::workspace_patch::WorkspacePatchFileAttestation;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Digest;
use tonic::Status;
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    gateway::GatewayRuntimeState,
    journal::{
        OrchestratorBackgroundTaskListFilter, OrchestratorBackgroundTaskRecord,
        OrchestratorCheckpointRecord, OrchestratorCompactionArtifactRecord,
        OrchestratorRunStatusSnapshot, WorkspaceCheckpointCreateRequest,
        WorkspaceCheckpointFileCreateRequest, WorkspaceCheckpointFilePayload,
        WorkspaceCheckpointFileRecord, WorkspaceCheckpointListFilter, WorkspaceCheckpointRecord,
        WorkspaceCheckpointRestoreMarkRequest, WorkspaceRestoreActivityFilter,
        WorkspaceRestoreActivitySummary, WorkspaceRestoreReportCreateRequest,
        WorkspaceRestoreReportListFilter, WorkspaceRestoreReportRecord,
    },
};

const TEXT_PREVIEW_CHAR_LIMIT: usize = 480;
const TEXT_SEARCH_CHAR_LIMIT: usize = 64 * 1024;
const MAX_ARTIFACT_LIST_LIMIT: usize = 256;
const MAX_COMPARE_FILE_LIMIT: usize = 256;
const MAX_INLINE_ARTIFACT_BYTES: usize = 256 * 1024;
const MAX_DIFF_TEXT_BYTES: usize = 64 * 1024;
const MAX_DIFF_LINES: usize = 160;
const MAX_ACTIVITY_LIST_LIMIT: usize = 32;

/// Identity of one tracked workspace file: which root it lives under plus its
/// root-relative path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WorkspaceArtifactKey {
    workspace_root_index: u32,
    path: String,
}

/// One file snapshot paired with the checkpoint that captured it.
#[derive(Debug, Clone)]
struct WorkspaceArtifactEntry {
    checkpoint: WorkspaceCheckpointRecord,
    file: WorkspaceCheckpointFileRecord,
}

/// Client-facing projection of a workspace checkpoint record.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceCheckpointSummary {
    pub checkpoint_id: String,
    pub session_id: String,
    pub run_id: String,
    pub source_kind: String,
    pub source_label: String,
    pub checkpoint_stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paired_checkpoint_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposal_id: Option<String>,
    pub actor_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub summary_text: String,
    pub diff_summary: Value,
    pub compare_summary: Value,
    pub risk_level: String,
    pub review_posture: String,
    pub created_at_unix_ms: i64,
    pub restore_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_restored_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_restore_report_id: Option<String>,
}

/// One historical snapshot of a workspace artifact at a specific checkpoint.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceArtifactVersion {
    pub artifact_id: String,
    pub checkpoint_id: String,
    pub checkpoint_created_at_unix_ms: i64,
    pub change_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moved_from_path: Option<String>,
    pub content_type: String,
    pub is_text: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    pub deleted: bool,
}

/// Per-path artifact history: latest state plus all captured versions,
/// newest first.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceArtifactRecord {
    pub artifact_id: String,
    pub path: String,
    pub display_path: String,
    pub workspace_root_index: u32,
    pub latest_checkpoint_id: String,
    pub latest_checkpoint_created_at_unix_ms: i64,
    pub latest_checkpoint_label: String,
    pub source_kind: String,
    pub source_label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposal_id: Option<String>,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub change_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moved_from_path: Option<String>,
    pub content_type: String,
    pub preview_kind: String,
    pub is_text: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    pub deleted: bool,
    pub version_count: usize,
    pub versions: Vec<WorkspaceArtifactVersion>,
}

/// Single-artifact detail view with optional inline content (bounded to
/// [`MAX_INLINE_ARTIFACT_BYTES`]).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceArtifactDetail {
    pub artifact: WorkspaceArtifactRecord,
    pub checkpoint: WorkspaceCheckpointSummary,
    pub content_available: bool,
    pub content_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_base64: Option<String>,
}

/// File state on one side (left or right anchor) of a workspace diff.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceDiffSide {
    pub artifact_id: String,
    pub checkpoint_id: String,
    pub change_kind: String,
    pub content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    pub deleted: bool,
}

/// One changed file in a workspace diff, with an optional bounded text diff.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceDiffFileRecord {
    pub path: String,
    pub display_path: String,
    pub workspace_root_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<WorkspaceDiffSide>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<WorkspaceDiffSide>,
    pub diff_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff_text: Option<String>,
}

/// One path that could not be restored, with the failure reason.
///
/// Also deserialized from the `failed_paths_json` column of stored restore
/// reports, so the field set is part of the persisted report format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkspaceRestoreFailure {
    pub path: String,
    pub display_path: String,
    pub workspace_root_index: u32,
    pub error: String,
}

/// Inputs for [`capture_workspace_patch_checkpoint`]: the acting identity,
/// the run/tool provenance, and the attested set of files being touched.
pub(crate) struct WorkspacePatchCheckpointCapture<'a> {
    pub principal: &'a str,
    pub device_id: &'a str,
    pub channel: Option<&'a str>,
    pub session_id: &'a str,
    pub run_id: &'a str,
    pub tool_name: &'a str,
    pub proposal_id: &'a str,
    pub checkpoint_stage: WorkspacePatchCheckpointStage,
    pub mutation_id: Option<&'a str>,
    pub paired_checkpoint_id: Option<&'a str>,
    pub compare_summary_json: &'a str,
    pub risk_level: &'a str,
    pub review_posture: &'a str,
    pub workspace_roots: &'a [PathBuf],
    pub files_touched: &'a [WorkspacePatchFileAttestation],
}

/// Whether a checkpoint snapshots the workspace before or after a mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspacePatchCheckpointStage {
    /// Pre-mutation snapshot capturing the state a restore would roll back to.
    Preflight,
    /// Post-mutation snapshot capturing the applied result.
    PostChange,
}

impl WorkspacePatchCheckpointStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::PostChange => "post_change",
        }
    }

    fn source_kind(self) -> &'static str {
        match self {
            Self::Preflight => "tool_preflight",
            Self::PostChange => "tool_result",
        }
    }

    fn source_label(self) -> &'static str {
        match self {
            Self::Preflight => "Workspace patch preflight",
            Self::PostChange => "Workspace patch",
        }
    }
}

/// Optional substring filter and result cap for artifact listings.
pub(crate) struct WorkspaceArtifactListQuery<'a> {
    pub query: Option<&'a str>,
    pub limit: usize,
}

/// Reference point for a workspace diff: a whole run or a single checkpoint.
pub(crate) enum WorkspaceCompareAnchor {
    Run(String),
    Checkpoint(String),
}

/// Inputs for [`restore_workspace_checkpoint`]: the acting identity, the
/// checkpoint to roll back to, and the restore scope (whole workspace or one
/// file).
pub(crate) struct WorkspaceRestoreRequest<'a> {
    pub principal: &'a str,
    pub device_id: &'a str,
    pub channel: Option<&'a str>,
    pub target_session_id: &'a str,
    pub checkpoint: WorkspaceCheckpointRecord,
    pub scope_kind: &'a str,
    pub target_path: Option<&'a str>,
    pub target_workspace_root_index: Option<u32>,
    pub branched_session_id: Option<&'a str>,
}

/// Combined run-scoped workspace view: artifacts, checkpoints, background
/// tasks, compactions, and session checkpoints for one run.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunWorkspaceArtifactsResponse {
    pub artifacts: Vec<WorkspaceArtifactRecord>,
    pub workspace_checkpoints: Vec<WorkspaceCheckpointSummary>,
    pub background_tasks: Vec<OrchestratorBackgroundTaskRecord>,
    pub compactions: Vec<OrchestratorCompactionArtifactRecord>,
    pub session_checkpoints: Vec<OrchestratorCheckpointRecord>,
}

/// Identity and label of one diff anchor as shown to clients.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceAnchorSummary {
    pub kind: String,
    pub id: String,
    pub label: String,
    pub session_id: String,
    pub run_id: String,
    pub created_at_unix_ms: i64,
}

/// Result of [`compare_workspace_anchors`]: the changed files between two
/// anchors of the same session.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceDiffResponse {
    pub left_anchor: WorkspaceAnchorSummary,
    pub right_anchor: WorkspaceAnchorSummary,
    pub files_changed: usize,
    pub files: Vec<WorkspaceDiffFileRecord>,
}

/// Result of [`restore_workspace_checkpoint`], including per-path outcomes
/// and the persisted restore report.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceRestoreOutcome {
    pub scope_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_workspace_root_index: Option<u32>,
    pub restored_paths: Vec<String>,
    pub failed_paths: Vec<WorkspaceRestoreFailure>,
    pub affects_context_stack: bool,
    pub report: WorkspaceRestoreReportRecord,
}

/// Scope filters and result cap for workspace activity snapshots.
pub(crate) struct WorkspaceActivityQuery<'a> {
    pub session_id: Option<&'a str>,
    pub run_id: Option<&'a str>,
    pub device_id: Option<&'a str>,
    pub limit: usize,
}

/// Client-facing projection of a workspace restore report record.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceRestoreReportSummary {
    pub report_id: String,
    pub checkpoint_id: String,
    pub session_id: String,
    pub run_id: String,
    pub actor_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub scope_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_path: Option<String>,
    pub reconciliation_summary: String,
    pub reconciliation_prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branched_session_id: Option<String>,
    pub result_state: String,
    pub created_at_unix_ms: i64,
}

/// Restore report detail: the report, its source checkpoint, and the decoded
/// per-path results.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceRestoreReportDetail {
    pub report: WorkspaceRestoreReportSummary,
    pub checkpoint: WorkspaceCheckpointSummary,
    pub restored_paths: Vec<String>,
    pub failed_paths: Vec<WorkspaceRestoreFailure>,
}

/// Recent workspace checkpoint/restore activity for a session, run, or
/// device scope.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceActivitySnapshot {
    pub summary: WorkspaceRestoreActivitySummary,
    pub recent_checkpoints: Vec<WorkspaceCheckpointSummary>,
    pub recent_restore_reports: Vec<WorkspaceRestoreReportSummary>,
}

/// Loads the run-scoped workspace view: aggregated artifacts (optionally
/// filtered by `query`), checkpoints, related background tasks, compactions,
/// and the run's session checkpoints.
///
/// # Errors
/// Returns the journal's [`Status`] when any of the underlying listings fail.
pub(crate) async fn load_run_workspace_artifacts(
    runtime_state: &Arc<GatewayRuntimeState>,
    run: &OrchestratorRunStatusSnapshot,
    query: WorkspaceArtifactListQuery<'_>,
) -> Result<RunWorkspaceArtifactsResponse, Status> {
    let checkpoints = runtime_state
        .list_workspace_checkpoints(WorkspaceCheckpointListFilter {
            session_id: Some(run.session_id.clone()),
            run_id: Some(run.run_id.clone()),
            device_id: None,
            limit: Some(MAX_ARTIFACT_LIST_LIMIT),
        })
        .await?;
    let artifacts =
        aggregate_run_workspace_artifacts(runtime_state, checkpoints.as_slice()).await?;
    let normalized_query = query.query.map(normalize_query).filter(|value| !value.is_empty());
    let artifacts = artifacts
        .into_iter()
        .filter(|artifact| {
            normalized_query
                .as_deref()
                .is_none_or(|needle| artifact_matches_query(artifact, needle))
        })
        .take(query.limit.clamp(1, MAX_ARTIFACT_LIST_LIMIT))
        .collect::<Vec<_>>();

    let background_tasks = runtime_state
        .list_orchestrator_background_tasks(OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(run.principal.clone()),
            device_id: Some(run.device_id.clone()),
            channel: run.channel.clone(),
            session_id: Some(run.session_id.clone()),
            include_completed: true,
            limit: 64,
        })
        .await?
        .into_iter()
        .filter(|task| {
            task.parent_run_id.as_deref() == Some(run.run_id.as_str())
                || task.target_run_id.as_deref() == Some(run.run_id.as_str())
        })
        .collect::<Vec<_>>();

    Ok(RunWorkspaceArtifactsResponse {
        artifacts,
        workspace_checkpoints: checkpoints.into_iter().map(workspace_checkpoint_summary).collect(),
        background_tasks,
        compactions: runtime_state
            .list_orchestrator_compaction_artifacts(run.session_id.clone())
            .await?,
        session_checkpoints: runtime_state
            .list_orchestrator_checkpoints(run.session_id.clone())
            .await?
            .into_iter()
            .filter(|checkpoint| checkpoint.run_id.as_deref() == Some(run.run_id.as_str()))
            .collect(),
    })
}

/// Loads one workspace artifact, optionally with bounded inline content.
///
/// # Errors
/// Returns `Status::not_found` when the artifact or its checkpoint does not
/// exist, and `Status::permission_denied` when the artifact belongs to a
/// different run or session than the authenticated `run` context.
pub(crate) async fn load_workspace_artifact_detail(
    runtime_state: &Arc<GatewayRuntimeState>,
    run: &OrchestratorRunStatusSnapshot,
    artifact_id: &str,
    include_content: bool,
) -> Result<WorkspaceArtifactDetail, Status> {
    let payload = runtime_state
        .get_workspace_checkpoint_file_payload(artifact_id.to_owned())
        .await?
        .ok_or_else(|| Status::not_found(format!("workspace artifact not found: {artifact_id}")))?;
    let checkpoint = runtime_state
        .get_workspace_checkpoint(payload.file.checkpoint_id.clone())
        .await?
        .ok_or_else(|| {
            Status::not_found(format!("workspace checkpoint not found for artifact: {artifact_id}"))
        })?;
    if checkpoint.run_id != run.run_id || checkpoint.session_id != run.session_id {
        return Err(Status::permission_denied(
            "workspace artifact does not belong to the authenticated run context",
        ));
    }

    let artifact = workspace_artifact_from_payload(
        &payload,
        &checkpoint,
        vec![WorkspaceArtifactVersion {
            artifact_id: payload.file.artifact_id.clone(),
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            checkpoint_created_at_unix_ms: checkpoint.created_at_unix_ms,
            change_kind: payload.file.change_kind.clone(),
            moved_from_path: payload.file.moved_from_path.clone(),
            content_type: payload.file.content_type.clone(),
            is_text: payload.file.is_text,
            size_bytes: payload.file.after_size_bytes,
            content_sha256: payload.file.after_content_sha256.clone(),
            deleted: payload.file.deleted(),
        }],
    );
    let (content_available, content_truncated, text_content, content_base64) =
        build_inline_artifact_content(&payload, include_content);

    Ok(WorkspaceArtifactDetail {
        artifact,
        checkpoint: workspace_checkpoint_summary(checkpoint),
        content_available,
        content_truncated,
        text_content,
        content_base64,
    })
}

/// Summarizes recent checkpoint and restore activity for the queried scope,
/// capped at [`MAX_ACTIVITY_LIST_LIMIT`] entries per list.
///
/// # Errors
/// Returns the journal's [`Status`] when any of the underlying queries fail.
pub(crate) async fn load_workspace_activity_snapshot(
    runtime_state: &Arc<GatewayRuntimeState>,
    query: WorkspaceActivityQuery<'_>,
) -> Result<WorkspaceActivitySnapshot, Status> {
    let limit = query.limit.clamp(1, MAX_ACTIVITY_LIST_LIMIT);
    let checkpoint_filter = WorkspaceCheckpointListFilter {
        session_id: query.session_id.map(str::to_owned),
        run_id: query.run_id.map(str::to_owned),
        device_id: query.device_id.map(str::to_owned),
        limit: Some(limit),
    };
    let restore_filter = WorkspaceRestoreReportListFilter {
        checkpoint_id: None,
        session_id: query.session_id.map(str::to_owned),
        run_id: query.run_id.map(str::to_owned),
        device_id: query.device_id.map(str::to_owned),
        limit: Some(limit),
    };
    let activity_filter = WorkspaceRestoreActivityFilter {
        session_id: query.session_id.map(str::to_owned),
        run_id: query.run_id.map(str::to_owned),
        device_id: query.device_id.map(str::to_owned),
    };
    let (summary, checkpoints, restore_reports) = tokio::try_join!(
        runtime_state.summarize_workspace_restore_activity(activity_filter),
        runtime_state.list_workspace_checkpoints(checkpoint_filter),
        runtime_state.list_workspace_restore_reports(restore_filter),
    )?;

    Ok(WorkspaceActivitySnapshot {
        summary,
        recent_checkpoints: checkpoints.into_iter().map(workspace_checkpoint_summary).collect(),
        recent_restore_reports: restore_reports
            .into_iter()
            .map(workspace_restore_report_summary)
            .collect(),
    })
}

/// Loads one restore report together with its source checkpoint and decoded
/// per-path results.
///
/// # Errors
/// Returns `Status::not_found` when the report or its checkpoint is missing,
/// and `Status::internal` when the stored path JSON fails to decode.
pub(crate) async fn load_workspace_restore_report_detail(
    runtime_state: &Arc<GatewayRuntimeState>,
    report_id: &str,
) -> Result<WorkspaceRestoreReportDetail, Status> {
    let report =
        runtime_state.get_workspace_restore_report(report_id.to_owned()).await?.ok_or_else(
            || Status::not_found(format!("workspace restore report not found: {report_id}")),
        )?;
    let checkpoint = runtime_state
        .get_workspace_checkpoint(report.checkpoint_id.clone())
        .await?
        .ok_or_else(|| {
            Status::not_found(format!(
                "workspace checkpoint not found for restore report: {report_id}"
            ))
        })?;

    Ok(WorkspaceRestoreReportDetail {
        report: workspace_restore_report_summary(report.clone()),
        checkpoint: workspace_checkpoint_summary(checkpoint),
        restored_paths: parse_workspace_restore_paths(report.restored_paths_json.as_str())
            .map_err(|error| {
                Status::internal(format!(
                    "failed to decode workspace restored paths for report {report_id}: {error}"
                ))
            })?,
        failed_paths: parse_workspace_restore_failures(report.failed_paths_json.as_str()).map_err(
            |error| {
                Status::internal(format!(
                    "failed to decode workspace restore failures for report {report_id}: {error}"
                ))
            },
        )?,
    })
}

/// Diffs the workspace state of two anchors, returning at most
/// `limit.clamp(1, MAX_COMPARE_FILE_LIMIT)` changed files with bounded text
/// diffs where both sides are text.
///
/// # Errors
/// Returns `Status::not_found` when an anchor does not exist,
/// `Status::failed_precondition` when the anchors belong to different
/// sessions, and the journal's [`Status`] when payload loading fails.
pub(crate) async fn compare_workspace_anchors(
    runtime_state: &Arc<GatewayRuntimeState>,
    left: WorkspaceCompareAnchor,
    right: WorkspaceCompareAnchor,
    limit: usize,
) -> Result<WorkspaceDiffResponse, Status> {
    let left_anchor = load_compare_anchor(runtime_state, left).await?;
    let right_anchor = load_compare_anchor(runtime_state, right).await?;
    if left_anchor.summary.session_id != right_anchor.summary.session_id {
        return Err(Status::failed_precondition(
            "workspace compare requires anchors from the same session",
        ));
    }

    let mut keys = BTreeSet::new();
    keys.extend(left_anchor.artifacts.keys().cloned());
    keys.extend(right_anchor.artifacts.keys().cloned());

    let mut files = Vec::new();
    for key in keys {
        let left_entry = left_anchor.artifacts.get(&key);
        let right_entry = right_anchor.artifacts.get(&key);
        let changed = match (left_entry, right_entry) {
            (Some(left_entry), Some(right_entry)) => {
                left_entry.file.after_content_sha256 != right_entry.file.after_content_sha256
                    || left_entry.file.change_kind != right_entry.file.change_kind
            }
            (Some(_), None) | (None, Some(_)) => true,
            (None, None) => false,
        };
        if !changed {
            continue;
        }
        if files.len() >= limit.clamp(1, MAX_COMPARE_FILE_LIMIT) {
            break;
        }

        let diff_text = build_diff_text(runtime_state, left_entry, right_entry).await?;
        let diff_kind = if diff_text.is_some() {
            "text".to_owned()
        } else if left_entry.is_some_and(|entry| entry.file.deleted())
            || right_entry.is_some_and(|entry| entry.file.deleted())
        {
            "metadata_only".to_owned()
        } else {
            "binary".to_owned()
        };
        files.push(WorkspaceDiffFileRecord {
            path: key.path.clone(),
            display_path: workspace_display_path(key.workspace_root_index, key.path.as_str()),
            workspace_root_index: key.workspace_root_index,
            left: left_entry.map(diff_side_from_entry),
            right: right_entry.map(diff_side_from_entry),
            diff_kind,
            diff_text,
        });
    }

    Ok(WorkspaceDiffResponse {
        left_anchor: left_anchor.summary,
        right_anchor: right_anchor.summary,
        files_changed: files.len(),
        files,
    })
}

/// Restores the workspace (or one file) to the state captured at the given
/// checkpoint, then persists a restore report and marks the checkpoint as
/// restored.
///
/// Per-path write failures do not abort the restore; they are collected into
/// `failed_paths` and reflected in the report's `result_state`.
///
/// # Errors
/// Returns `Status::invalid_argument` for an unknown `scope_kind` or a
/// file-scope request without `target_path`, `Status::not_found` /
/// `Status::failed_precondition` when the target file state is missing or
/// ambiguous across roots, and the journal's [`Status`] when agent
/// resolution, state collection, or report persistence fails.
pub(crate) async fn restore_workspace_checkpoint(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: WorkspaceRestoreRequest<'_>,
) -> Result<WorkspaceRestoreOutcome, Status> {
    let scope_kind = request.scope_kind.trim();
    if scope_kind != "workspace" && scope_kind != "file" {
        return Err(Status::invalid_argument(
            "workspace restore scope_kind must be 'workspace' or 'file'",
        ));
    }

    let agent_outcome = runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: request.principal.to_owned(),
            channel: request.channel.map(str::to_owned),
            session_id: Some(request.target_session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await?;
    let workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let target_entries =
        collect_workspace_state_for_checkpoint(runtime_state, &request.checkpoint).await?;

    let selected = if scope_kind == "file" {
        let target_path =
            request.target_path.map(str::trim).filter(|value| !value.is_empty()).ok_or_else(
                || Status::invalid_argument("workspace file restore requires target_path"),
            )?;
        let matching = target_entries
            .into_iter()
            .filter(|(key, _)| {
                key.path == target_path
                    && request
                        .target_workspace_root_index
                        .is_none_or(|value| value == key.workspace_root_index)
            })
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Err(Status::not_found(format!(
                "workspace file state not found at checkpoint for path: {target_path}"
            )));
        }
        if matching.len() > 1 && request.target_workspace_root_index.is_none() {
            return Err(Status::failed_precondition(format!(
                "workspace file path is ambiguous across roots: {target_path}"
            )));
        }
        matching
    } else {
        target_entries.into_iter().collect::<Vec<_>>()
    };

    let mut restored_paths = Vec::new();
    let mut failed_paths = Vec::new();
    let mut affects_context_stack = false;
    for (key, entry) in selected {
        affects_context_stack |= path_affects_context_stack(key.path.as_str());
        match restore_workspace_entry(runtime_state, workspace_roots.as_slice(), &key, &entry).await
        {
            Ok(()) => restored_paths.push(key.path.clone()),
            Err(error) => failed_paths.push(WorkspaceRestoreFailure {
                path: key.path.clone(),
                display_path: workspace_display_path(key.workspace_root_index, key.path.as_str()),
                workspace_root_index: key.workspace_root_index,
                error: error.message().to_owned(),
            }),
        }
    }

    let result_state = if failed_paths.is_empty() {
        "succeeded"
    } else if restored_paths.is_empty() {
        "failed"
    } else {
        "partial_failure"
    };
    let report = runtime_state
        .create_workspace_restore_report(WorkspaceRestoreReportCreateRequest {
            report_id: Ulid::new().to_string(),
            checkpoint_id: request.checkpoint.checkpoint_id.clone(),
            session_id: request.target_session_id.to_owned(),
            run_id: request.checkpoint.run_id.clone(),
            actor_principal: request.principal.to_owned(),
            device_id: request.device_id.to_owned(),
            channel: request.channel.map(str::to_owned),
            scope_kind: scope_kind.to_owned(),
            target_path: request.target_path.map(str::to_owned),
            restored_paths_json: serde_json::to_string(&restored_paths).map_err(|error| {
                Status::internal(format!(
                    "failed to encode restored workspace paths for report: {error}"
                ))
            })?,
            failed_paths_json: serde_json::to_string(&failed_paths).map_err(|error| {
                Status::internal(format!(
                    "failed to encode failed workspace paths for report: {error}"
                ))
            })?,
            reconciliation_summary: build_reconciliation_summary(
                scope_kind,
                request.checkpoint.checkpoint_id.as_str(),
                restored_paths.as_slice(),
                failed_paths.as_slice(),
            ),
            reconciliation_prompt: build_reconciliation_prompt(
                request.checkpoint.checkpoint_id.as_str(),
                restored_paths.as_slice(),
                failed_paths.as_slice(),
            ),
            branched_session_id: request.branched_session_id.map(str::to_owned),
            result_state: result_state.to_owned(),
        })
        .await?;
    runtime_state
        .mark_workspace_checkpoint_restored(WorkspaceCheckpointRestoreMarkRequest {
            checkpoint_id: request.checkpoint.checkpoint_id.clone(),
            latest_restore_report_id: Some(report.report_id.clone()),
        })
        .await?;

    Ok(WorkspaceRestoreOutcome {
        scope_kind: scope_kind.to_owned(),
        target_path: request.target_path.map(str::to_owned),
        target_workspace_root_index: request.target_workspace_root_index,
        restored_paths,
        failed_paths,
        affects_context_stack,
        report,
    })
}

async fn aggregate_run_workspace_artifacts(
    runtime_state: &Arc<GatewayRuntimeState>,
    checkpoints: &[WorkspaceCheckpointRecord],
) -> Result<Vec<WorkspaceArtifactRecord>, Status> {
    let mut versions_by_path = BTreeMap::<WorkspaceArtifactKey, Vec<WorkspaceArtifactEntry>>::new();
    for checkpoint in checkpoints {
        let files =
            runtime_state.list_workspace_checkpoint_files(checkpoint.checkpoint_id.clone()).await?;
        for file in files {
            versions_by_path
                .entry(WorkspaceArtifactKey {
                    workspace_root_index: file.workspace_root_index,
                    path: file.path.clone(),
                })
                .or_default()
                .push(WorkspaceArtifactEntry { checkpoint: checkpoint.clone(), file });
        }
    }

    let mut artifacts = Vec::with_capacity(versions_by_path.len());
    for (_key, mut versions) in versions_by_path {
        // Newest first: timestamp, then stage (post_change outranks preflight
        // at the same instant), then checkpoint id as a deterministic
        // tiebreaker so `versions[0]` is always the authoritative latest.
        versions.sort_by(|left, right| {
            right
                .checkpoint
                .created_at_unix_ms
                .cmp(&left.checkpoint.created_at_unix_ms)
                .then_with(|| {
                    checkpoint_stage_order(right.checkpoint.checkpoint_stage.as_str())
                        .cmp(&checkpoint_stage_order(left.checkpoint.checkpoint_stage.as_str()))
                })
                .then_with(|| right.checkpoint.checkpoint_id.cmp(&left.checkpoint.checkpoint_id))
        });
        let latest = versions
            .first()
            .cloned()
            .ok_or_else(|| Status::internal("workspace artifact version list was empty"))?;
        let version_rows = versions
            .iter()
            .map(|entry| WorkspaceArtifactVersion {
                artifact_id: entry.file.artifact_id.clone(),
                checkpoint_id: entry.checkpoint.checkpoint_id.clone(),
                checkpoint_created_at_unix_ms: entry.checkpoint.created_at_unix_ms,
                change_kind: entry.file.change_kind.clone(),
                moved_from_path: entry.file.moved_from_path.clone(),
                content_type: entry.file.content_type.clone(),
                is_text: entry.file.is_text,
                size_bytes: entry.file.after_size_bytes,
                content_sha256: entry.file.after_content_sha256.clone(),
                deleted: entry.file.deleted(),
            })
            .collect::<Vec<_>>();
        artifacts.push(workspace_artifact_from_entry(&latest, version_rows));
    }
    artifacts.sort_by(|left, right| {
        left.workspace_root_index
            .cmp(&right.workspace_root_index)
            .then_with(|| left.path.cmp(&right.path))
    });
    Ok(artifacts)
}

fn workspace_artifact_from_entry(
    entry: &WorkspaceArtifactEntry,
    versions: Vec<WorkspaceArtifactVersion>,
) -> WorkspaceArtifactRecord {
    WorkspaceArtifactRecord {
        artifact_id: entry.file.artifact_id.clone(),
        path: entry.file.path.clone(),
        display_path: workspace_display_path(
            entry.file.workspace_root_index,
            entry.file.path.as_str(),
        ),
        workspace_root_index: entry.file.workspace_root_index,
        latest_checkpoint_id: entry.checkpoint.checkpoint_id.clone(),
        latest_checkpoint_created_at_unix_ms: entry.checkpoint.created_at_unix_ms,
        latest_checkpoint_label: entry.checkpoint.summary_text.clone(),
        source_kind: entry.checkpoint.source_kind.clone(),
        source_label: entry.checkpoint.source_label.clone(),
        tool_name: entry.checkpoint.tool_name.clone(),
        proposal_id: entry.checkpoint.proposal_id.clone(),
        device_id: entry.checkpoint.device_id.clone(),
        channel: entry.checkpoint.channel.clone(),
        change_kind: entry.file.change_kind.clone(),
        moved_from_path: entry.file.moved_from_path.clone(),
        content_type: entry.file.content_type.clone(),
        preview_kind: preview_kind(entry.file.content_type.as_str(), entry.file.is_text),
        is_text: entry.file.is_text,
        preview_text: entry.file.preview_text.clone(),
        size_bytes: entry.file.after_size_bytes,
        content_sha256: entry.file.after_content_sha256.clone(),
        deleted: entry.file.deleted(),
        version_count: versions.len(),
        versions,
    }
}

fn workspace_artifact_from_payload(
    payload: &WorkspaceCheckpointFilePayload,
    checkpoint: &WorkspaceCheckpointRecord,
    versions: Vec<WorkspaceArtifactVersion>,
) -> WorkspaceArtifactRecord {
    workspace_artifact_from_entry(
        &WorkspaceArtifactEntry { checkpoint: checkpoint.clone(), file: payload.file.clone() },
        versions,
    )
}

fn build_inline_artifact_content(
    payload: &WorkspaceCheckpointFilePayload,
    include_content: bool,
) -> (bool, bool, Option<String>, Option<String>) {
    let Some(content_bytes) = payload.content_bytes.as_deref() else {
        return (false, false, None, None);
    };
    if !include_content {
        return (true, false, None, None);
    }
    let truncated = content_bytes.len() > MAX_INLINE_ARTIFACT_BYTES;
    let selected_bytes = &content_bytes[..content_bytes.len().min(MAX_INLINE_ARTIFACT_BYTES)];
    let text_content = if payload.file.is_text {
        std::str::from_utf8(selected_bytes).ok().map(ToOwned::to_owned)
    } else {
        None
    };
    let content_base64 = Some(BASE64_STANDARD.encode(selected_bytes));
    (true, truncated, text_content, content_base64)
}

async fn load_compare_anchor(
    runtime_state: &Arc<GatewayRuntimeState>,
    anchor: WorkspaceCompareAnchor,
) -> Result<LoadedCompareAnchor, Status> {
    match anchor {
        WorkspaceCompareAnchor::Run(run_id) => {
            let run =
                runtime_state.orchestrator_run_status_snapshot(run_id.clone()).await?.ok_or_else(
                    || Status::not_found(format!("orchestrator run not found: {run_id}")),
                )?;
            let checkpoints = runtime_state
                .list_workspace_checkpoints(WorkspaceCheckpointListFilter {
                    session_id: Some(run.session_id.clone()),
                    run_id: Some(run.run_id.clone()),
                    device_id: None,
                    limit: Some(MAX_ARTIFACT_LIST_LIMIT),
                })
                .await?;
            Ok(LoadedCompareAnchor {
                summary: WorkspaceAnchorSummary {
                    kind: "run".to_owned(),
                    id: run.run_id.clone(),
                    label: format!("Run {}", run.run_id),
                    session_id: run.session_id.clone(),
                    run_id: run.run_id,
                    created_at_unix_ms: run.created_at_unix_ms,
                },
                artifacts: load_anchor_artifacts(runtime_state, checkpoints.as_slice()).await?,
            })
        }
        WorkspaceCompareAnchor::Checkpoint(checkpoint_id) => {
            let checkpoint = runtime_state
                .get_workspace_checkpoint(checkpoint_id.clone())
                .await?
                .ok_or_else(|| {
                Status::not_found(format!("workspace checkpoint not found: {checkpoint_id}"))
            })?;
            let files = runtime_state
                .list_workspace_checkpoint_files(checkpoint.checkpoint_id.clone())
                .await?;
            Ok(LoadedCompareAnchor {
                summary: WorkspaceAnchorSummary {
                    kind: "checkpoint".to_owned(),
                    id: checkpoint.checkpoint_id.clone(),
                    label: checkpoint.summary_text.clone(),
                    session_id: checkpoint.session_id.clone(),
                    run_id: checkpoint.run_id.clone(),
                    created_at_unix_ms: checkpoint.created_at_unix_ms,
                },
                artifacts: files
                    .into_iter()
                    .map(|file| {
                        (
                            WorkspaceArtifactKey {
                                workspace_root_index: file.workspace_root_index,
                                path: file.path.clone(),
                            },
                            WorkspaceArtifactEntry { checkpoint: checkpoint.clone(), file },
                        )
                    })
                    .collect(),
            })
        }
    }
}

async fn load_anchor_artifacts(
    runtime_state: &Arc<GatewayRuntimeState>,
    checkpoints: &[WorkspaceCheckpointRecord],
) -> Result<BTreeMap<WorkspaceArtifactKey, WorkspaceArtifactEntry>, Status> {
    let mut artifacts = BTreeMap::<WorkspaceArtifactKey, WorkspaceArtifactEntry>::new();
    for checkpoint in checkpoints {
        let files =
            runtime_state.list_workspace_checkpoint_files(checkpoint.checkpoint_id.clone()).await?;
        for file in files {
            let key = WorkspaceArtifactKey {
                workspace_root_index: file.workspace_root_index,
                path: file.path.clone(),
            };
            let candidate = WorkspaceArtifactEntry { checkpoint: checkpoint.clone(), file };
            // Keep only the newest snapshot per path; on equal timestamps the
            // first one seen wins, matching the input listing order.
            match artifacts.get(&key) {
                Some(existing)
                    if existing.checkpoint.created_at_unix_ms
                        >= candidate.checkpoint.created_at_unix_ms => {}
                _ => {
                    artifacts.insert(key, candidate);
                }
            }
        }
    }
    Ok(artifacts)
}

async fn build_diff_text(
    runtime_state: &Arc<GatewayRuntimeState>,
    left: Option<&WorkspaceArtifactEntry>,
    right: Option<&WorkspaceArtifactEntry>,
) -> Result<Option<String>, Status> {
    let left_payload = match left {
        Some(entry) => {
            runtime_state
                .get_workspace_checkpoint_file_payload(entry.file.artifact_id.clone())
                .await?
        }
        None => None,
    };
    let right_payload = match right {
        Some(entry) => {
            runtime_state
                .get_workspace_checkpoint_file_payload(entry.file.artifact_id.clone())
                .await?
        }
        None => None,
    };
    let left_text = payload_text_for_diff(left_payload.as_ref());
    let right_text = payload_text_for_diff(right_payload.as_ref());
    match (left_text, right_text) {
        (Some(left_text), Some(right_text)) => Ok(Some(build_line_diff_preview(
            left_text.as_str(),
            right_text.as_str(),
            MAX_DIFF_LINES,
        ))),
        (Some(left_text), None) => {
            Ok(Some(build_line_diff_preview(left_text.as_str(), "", MAX_DIFF_LINES)))
        }
        (None, Some(right_text)) => {
            Ok(Some(build_line_diff_preview("", right_text.as_str(), MAX_DIFF_LINES)))
        }
        (None, None) => Ok(None),
    }
}

async fn collect_workspace_state_for_checkpoint(
    runtime_state: &Arc<GatewayRuntimeState>,
    checkpoint: &WorkspaceCheckpointRecord,
) -> Result<BTreeMap<WorkspaceArtifactKey, WorkspaceArtifactEntry>, Status> {
    let mut checkpoints = runtime_state
        .list_workspace_checkpoints(WorkspaceCheckpointListFilter {
            session_id: Some(checkpoint.session_id.clone()),
            run_id: None,
            device_id: None,
            limit: Some(MAX_ARTIFACT_LIST_LIMIT),
        })
        .await?;
    // Reconstruct workspace state *at* the target checkpoint by replaying the
    // session's checkpoints in chronological order up to and including it.
    // The (timestamp, stage, checkpoint id) ordering here must mirror the
    // sort in `aggregate_run_workspace_artifacts` or restores would pick a
    // different "latest" version than the artifact views display.
    checkpoints.retain(|candidate| {
        candidate.created_at_unix_ms < checkpoint.created_at_unix_ms
            || (candidate.created_at_unix_ms == checkpoint.created_at_unix_ms && {
                let candidate_stage_order =
                    checkpoint_stage_order(candidate.checkpoint_stage.as_str());
                let target_stage_order =
                    checkpoint_stage_order(checkpoint.checkpoint_stage.as_str());
                candidate_stage_order < target_stage_order
                    || (candidate_stage_order == target_stage_order
                        && candidate.checkpoint_id <= checkpoint.checkpoint_id)
            })
    });
    checkpoints.sort_by(|left, right| {
        left.created_at_unix_ms
            .cmp(&right.created_at_unix_ms)
            .then_with(|| {
                checkpoint_stage_order(left.checkpoint_stage.as_str())
                    .cmp(&checkpoint_stage_order(right.checkpoint_stage.as_str()))
            })
            .then_with(|| left.checkpoint_id.cmp(&right.checkpoint_id))
    });

    let mut state = BTreeMap::<WorkspaceArtifactKey, WorkspaceArtifactEntry>::new();
    for candidate in checkpoints {
        let files =
            runtime_state.list_workspace_checkpoint_files(candidate.checkpoint_id.clone()).await?;
        for file in files {
            state.insert(
                WorkspaceArtifactKey {
                    workspace_root_index: file.workspace_root_index,
                    path: file.path.clone(),
                },
                WorkspaceArtifactEntry { checkpoint: candidate.clone(), file },
            );
        }
    }
    Ok(state)
}

async fn restore_workspace_entry(
    runtime_state: &Arc<GatewayRuntimeState>,
    workspace_roots: &[PathBuf],
    key: &WorkspaceArtifactKey,
    entry: &WorkspaceArtifactEntry,
) -> Result<(), Status> {
    let workspace_root = workspace_roots
        .get(key.workspace_root_index as usize)
        .ok_or_else(|| Status::internal("workspace restore root index is out of range"))?;
    let canonical_workspace_root = canonicalize_workspace_restore_root(workspace_root)?;
    let absolute_path = resolve_workspace_restore_target(
        canonical_workspace_root.as_path(),
        Path::new(key.path.as_str()),
    )?;
    if entry.file.deleted() {
        match fs::remove_file(absolute_path.as_path()) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(Status::internal(format!(
                    "failed to remove restored workspace file {}: {error}",
                    absolute_path.display()
                )));
            }
        }
    }

    let payload = runtime_state
        .get_workspace_checkpoint_file_payload(entry.file.artifact_id.clone())
        .await?
        .ok_or_else(|| {
            Status::not_found(format!(
                "workspace restore artifact payload not found: {}",
                entry.file.artifact_id
            ))
        })?;
    let content_bytes = payload.content_bytes.ok_or_else(|| {
        Status::internal(format!(
            "workspace restore payload is missing bytes for artifact {}",
            entry.file.artifact_id
        ))
    })?;
    if let Some(parent) = absolute_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            Status::internal(format!(
                "failed to create workspace restore parent directory {}: {error}",
                parent.display()
            ))
        })?;
    }
    // Re-check confinement after creating parents: the directory tree may
    // have changed (e.g. a symlink swapped in) since the resolve-time check
    // in `resolve_workspace_restore_target`.
    ensure_workspace_restore_target_confined(
        canonical_workspace_root.as_path(),
        absolute_path.as_path(),
    )?;
    fs::write(absolute_path.as_path(), content_bytes).map_err(|error| {
        Status::internal(format!(
            "failed to write restored workspace file {}: {error}",
            absolute_path.display()
        ))
    })
}

fn resolve_workspace_restore_target(
    workspace_root: &Path,
    relative_path: &Path,
) -> Result<PathBuf, Status> {
    validate_workspace_restore_relative_path(relative_path)?;
    let absolute_path = workspace_root.join(relative_path);
    ensure_workspace_restore_target_confined(workspace_root, absolute_path.as_path())?;
    Ok(absolute_path)
}

fn canonicalize_workspace_restore_root(workspace_root: &Path) -> Result<PathBuf, Status> {
    let canonical = fs::canonicalize(workspace_root).map_err(|error| {
        Status::internal(format!(
            "failed to canonicalize workspace restore root {}: {error}",
            workspace_root.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(Status::invalid_argument(format!(
            "workspace restore root is not a directory: {}",
            workspace_root.display()
        )));
    }
    Ok(canonical)
}

fn validate_workspace_restore_relative_path(relative_path: &Path) -> Result<(), Status> {
    if relative_path.as_os_str().is_empty() || relative_path.is_absolute() {
        return Err(Status::invalid_argument(format!(
            "workspace restore path must be relative: {}",
            relative_path.display()
        )));
    }
    let mut has_normal_component = false;
    for component in relative_path.components() {
        match component {
            Component::Normal(_) => has_normal_component = true,
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Status::invalid_argument(format!(
                    "workspace restore path escapes workspace root: {}",
                    relative_path.display()
                )));
            }
        }
    }
    if !has_normal_component {
        return Err(Status::invalid_argument(format!(
            "workspace restore path must name a file: {}",
            relative_path.display()
        )));
    }
    Ok(())
}

/// Verifies that `absolute_path` cannot write outside `workspace_root`.
///
/// Symlinks are rejected outright -- both at the leaf and at the nearest
/// existing ancestor of a not-yet-created target -- because a symlink inside
/// the workspace can redirect a confined-looking path anywhere on the
/// filesystem. The surviving path (or ancestor) is then canonicalized and
/// prefix-checked against the already-canonical workspace root.
fn ensure_workspace_restore_target_confined(
    workspace_root: &Path,
    absolute_path: &Path,
) -> Result<(), Status> {
    match fs::symlink_metadata(absolute_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(Status::invalid_argument(format!(
                    "workspace restore path cannot target a symlink: {}",
                    absolute_path.display()
                )));
            }
            let canonical = fs::canonicalize(absolute_path).map_err(|error| {
                Status::internal(format!(
                    "failed to canonicalize workspace restore path {}: {error}",
                    absolute_path.display()
                ))
            })?;
            ensure_canonical_restore_path_within_root(
                workspace_root,
                canonical.as_path(),
                absolute_path,
            )
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            let ancestor = nearest_existing_restore_ancestor(absolute_path)?;
            let metadata = fs::symlink_metadata(ancestor.as_path()).map_err(|error| {
                Status::internal(format!(
                    "failed to inspect workspace restore ancestor {}: {error}",
                    ancestor.display()
                ))
            })?;
            if metadata.file_type().is_symlink() {
                return Err(Status::invalid_argument(format!(
                    "workspace restore path resolves through a symlinked ancestor: {}",
                    ancestor.display()
                )));
            }
            let canonical_ancestor = fs::canonicalize(ancestor.as_path()).map_err(|error| {
                Status::internal(format!(
                    "failed to canonicalize workspace restore ancestor {}: {error}",
                    ancestor.display()
                ))
            })?;
            ensure_canonical_restore_path_within_root(
                workspace_root,
                canonical_ancestor.as_path(),
                absolute_path,
            )
        }
        Err(error) => Err(Status::internal(format!(
            "failed to inspect workspace restore path {}: {error}",
            absolute_path.display()
        ))),
    }
}

fn nearest_existing_restore_ancestor(path: &Path) -> Result<PathBuf, Status> {
    let mut cursor = path.to_path_buf();
    loop {
        match fs::symlink_metadata(cursor.as_path()) {
            Ok(_) => return Ok(cursor),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                let Some(parent) = cursor.parent() else {
                    return Err(Status::invalid_argument(format!(
                        "workspace restore path escapes workspace root: {}",
                        path.display()
                    )));
                };
                cursor = parent.to_path_buf();
            }
            Err(error) => {
                return Err(Status::internal(format!(
                    "failed to inspect workspace restore ancestor {}: {error}",
                    cursor.display()
                )));
            }
        }
    }
}

fn ensure_canonical_restore_path_within_root(
    workspace_root: &Path,
    canonical_path: &Path,
    display_path: &Path,
) -> Result<(), Status> {
    if canonical_path.starts_with(workspace_root) {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "workspace restore path escapes workspace root: {}",
            display_path.display()
        )))
    }
}

fn workspace_checkpoint_summary(
    checkpoint: WorkspaceCheckpointRecord,
) -> WorkspaceCheckpointSummary {
    WorkspaceCheckpointSummary {
        checkpoint_id: checkpoint.checkpoint_id,
        session_id: checkpoint.session_id,
        run_id: checkpoint.run_id,
        source_kind: checkpoint.source_kind,
        source_label: checkpoint.source_label,
        checkpoint_stage: checkpoint.checkpoint_stage,
        mutation_id: checkpoint.mutation_id,
        paired_checkpoint_id: checkpoint.paired_checkpoint_id,
        tool_name: checkpoint.tool_name,
        proposal_id: checkpoint.proposal_id,
        actor_principal: checkpoint.actor_principal,
        device_id: checkpoint.device_id,
        channel: checkpoint.channel,
        summary_text: checkpoint.summary_text,
        diff_summary: parse_diff_summary_value(checkpoint.diff_summary_json.as_str()),
        compare_summary: parse_diff_summary_value(checkpoint.compare_summary_json.as_str()),
        risk_level: checkpoint.risk_level,
        review_posture: checkpoint.review_posture,
        created_at_unix_ms: checkpoint.created_at_unix_ms,
        restore_count: checkpoint.restore_count,
        last_restored_at_unix_ms: checkpoint.last_restored_at_unix_ms,
        latest_restore_report_id: checkpoint.latest_restore_report_id,
    }
}

fn workspace_restore_report_summary(
    report: WorkspaceRestoreReportRecord,
) -> WorkspaceRestoreReportSummary {
    WorkspaceRestoreReportSummary {
        report_id: report.report_id,
        checkpoint_id: report.checkpoint_id,
        session_id: report.session_id,
        run_id: report.run_id,
        actor_principal: report.actor_principal,
        device_id: report.device_id,
        channel: report.channel,
        scope_kind: report.scope_kind,
        target_path: report.target_path,
        reconciliation_summary: report.reconciliation_summary,
        reconciliation_prompt: report.reconciliation_prompt,
        branched_session_id: report.branched_session_id,
        result_state: report.result_state,
        created_at_unix_ms: report.created_at_unix_ms,
    }
}

fn parse_diff_summary_value(raw: &str) -> Value {
    serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}

fn parse_workspace_restore_paths(raw: &str) -> Result<Vec<String>, serde_json::Error> {
    serde_json::from_str(raw)
}

fn parse_workspace_restore_failures(
    raw: &str,
) -> Result<Vec<WorkspaceRestoreFailure>, serde_json::Error> {
    serde_json::from_str(raw)
}

fn normalize_query(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn artifact_matches_query(artifact: &WorkspaceArtifactRecord, query: &str) -> bool {
    artifact.path.to_ascii_lowercase().contains(query)
        || artifact.display_path.to_ascii_lowercase().contains(query)
        || artifact
            .preview_text
            .as_deref()
            .is_some_and(|value| value.to_ascii_lowercase().contains(query))
        || artifact.versions.iter().any(|version| {
            version
                .moved_from_path
                .as_deref()
                .is_some_and(|value| value.to_ascii_lowercase().contains(query))
        })
}

fn diff_side_from_entry(entry: &WorkspaceArtifactEntry) -> WorkspaceDiffSide {
    WorkspaceDiffSide {
        artifact_id: entry.file.artifact_id.clone(),
        checkpoint_id: entry.checkpoint.checkpoint_id.clone(),
        change_kind: entry.file.change_kind.clone(),
        content_type: entry.file.content_type.clone(),
        size_bytes: entry.file.after_size_bytes,
        content_sha256: entry.file.after_content_sha256.clone(),
        deleted: entry.file.deleted(),
    }
}

fn payload_text_for_diff(payload: Option<&WorkspaceCheckpointFilePayload>) -> Option<String> {
    let payload = payload?;
    if !payload.file.is_text {
        return None;
    }
    let bytes = payload.content_bytes.as_deref()?;
    let selected = &bytes[..bytes.len().min(MAX_DIFF_TEXT_BYTES)];
    std::str::from_utf8(selected).ok().map(ToOwned::to_owned)
}

fn build_reconciliation_summary(
    scope_kind: &str,
    checkpoint_id: &str,
    restored_paths: &[String],
    failed_paths: &[WorkspaceRestoreFailure],
) -> String {
    let restored = restored_paths.len();
    let failed = failed_paths.len();
    if scope_kind == "file" {
        format!(
            "Workspace file restore from checkpoint {checkpoint_id} applied {restored} path(s) with {failed} failure(s)."
        )
    } else {
        format!(
            "Workspace restore from checkpoint {checkpoint_id} applied {restored} tracked path(s) with {failed} failure(s)."
        )
    }
}

fn build_reconciliation_prompt(
    checkpoint_id: &str,
    restored_paths: &[String],
    failed_paths: &[WorkspaceRestoreFailure],
) -> String {
    let mut prompt = format!(
        "Workspace was restored from checkpoint {checkpoint_id}. Confirm the restored state before making further edits."
    );
    if !restored_paths.is_empty() {
        let preview = restored_paths.iter().take(6).cloned().collect::<Vec<_>>().join(", ");
        prompt.push_str(format!(" Restored paths: {preview}.").as_str());
    }
    if !failed_paths.is_empty() {
        let preview = failed_paths
            .iter()
            .take(4)
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>()
            .join(", ");
        prompt.push_str(format!(" Review failed paths: {preview}.").as_str());
    }
    prompt
}

/// Returns whether restoring `path` touches files that feed the agent's
/// context stack (instructions, memory, project context), so callers can
/// prompt the model to re-read them after a restore.
fn path_affects_context_stack(path: &str) -> bool {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    normalized == "palyra.md"
        || normalized == "memory.md"
        || normalized == "heartbeat.md"
        || normalized.starts_with("context/")
        || normalized.starts_with("projects/")
}

fn workspace_display_path(workspace_root_index: u32, path: &str) -> String {
    if workspace_root_index == 0 {
        path.to_owned()
    } else {
        format!("root:{}:{path}", workspace_root_index)
    }
}

fn preview_kind(content_type: &str, is_text: bool) -> String {
    if is_text {
        "text".to_owned()
    } else if content_type.starts_with("image/") {
        "image".to_owned()
    } else {
        "binary".to_owned()
    }
}

/// Builds a unified-style line diff (` `/`-`/`+` prefixes) capped at
/// `max_output_lines`, appending `...` when truncated.
///
/// Classic longest-common-subsequence DP; quadratic in line count, which is
/// acceptable because both inputs are already capped at
/// [`MAX_DIFF_TEXT_BYTES`] by `payload_text_for_diff`.
fn build_line_diff_preview(left: &str, right: &str, max_output_lines: usize) -> String {
    let left_lines = left.lines().collect::<Vec<_>>();
    let right_lines = right.lines().collect::<Vec<_>>();
    let mut dp = vec![vec![0usize; right_lines.len() + 1]; left_lines.len() + 1];
    for left_index in (0..left_lines.len()).rev() {
        for right_index in (0..right_lines.len()).rev() {
            dp[left_index][right_index] = if left_lines[left_index] == right_lines[right_index] {
                dp[left_index + 1][right_index + 1] + 1
            } else {
                dp[left_index + 1][right_index].max(dp[left_index][right_index + 1])
            };
        }
    }

    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut rows = Vec::new();
    while left_index < left_lines.len() && right_index < right_lines.len() {
        if left_lines[left_index] == right_lines[right_index] {
            rows.push(format!(" {}", left_lines[left_index]));
            left_index += 1;
            right_index += 1;
        } else if dp[left_index + 1][right_index] >= dp[left_index][right_index + 1] {
            rows.push(format!("-{}", left_lines[left_index]));
            left_index += 1;
        } else {
            rows.push(format!("+{}", right_lines[right_index]));
            right_index += 1;
        }
        if rows.len() >= max_output_lines {
            rows.push("...".to_owned());
            return rows.join("\n");
        }
    }
    while left_index < left_lines.len() && rows.len() < max_output_lines {
        rows.push(format!("-{}", left_lines[left_index]));
        left_index += 1;
    }
    while right_index < right_lines.len() && rows.len() < max_output_lines {
        rows.push(format!("+{}", right_lines[right_index]));
        right_index += 1;
    }
    if left_index < left_lines.len() || right_index < right_lines.len() {
        rows.push("...".to_owned());
    }
    rows.join("\n")
}

/// Captures a workspace checkpoint for a patch mutation, snapshotting every
/// attested file at the requested stage.
///
/// Returns `Ok(None)` when the attestation lists no touched files (nothing to
/// checkpoint).
///
/// # Errors
/// Returns `Status::failed_precondition` when on-disk content no longer
/// matches the attested hash/size (the workspace changed between planning and
/// capture), `Status::invalid_argument` when a path escapes its workspace
/// root, and the journal's [`Status`] when checkpoint persistence fails.
pub(crate) async fn capture_workspace_patch_checkpoint(
    runtime_state: &Arc<GatewayRuntimeState>,
    input: WorkspacePatchCheckpointCapture<'_>,
) -> Result<Option<WorkspaceCheckpointRecord>, Status> {
    if input.files_touched.is_empty() {
        return Ok(None);
    }

    let mut files = Vec::new();
    let mut created = 0usize;
    let mut updated = 0usize;
    let mut deleted = 0usize;
    let mut moved = 0usize;

    for attestation in input.files_touched {
        match attestation.operation.as_str() {
            "create" => created += 1,
            "delete" => deleted += 1,
            "move" => moved += 1,
            _ => updated += 1,
        }
        files.extend(build_workspace_checkpoint_files(
            input.checkpoint_stage,
            input.workspace_roots,
            attestation,
        )?);
    }

    let checkpoint = runtime_state
        .create_workspace_checkpoint(WorkspaceCheckpointCreateRequest {
            checkpoint_id: Ulid::new().to_string(),
            session_id: input.session_id.to_owned(),
            run_id: input.run_id.to_owned(),
            source_kind: input.checkpoint_stage.source_kind().to_owned(),
            source_label: input.checkpoint_stage.source_label().to_owned(),
            checkpoint_stage: input.checkpoint_stage.as_str().to_owned(),
            mutation_id: input.mutation_id.map(str::to_owned),
            paired_checkpoint_id: input.paired_checkpoint_id.map(str::to_owned),
            tool_name: Some(input.tool_name.to_owned()),
            proposal_id: Some(input.proposal_id.to_owned()),
            actor_principal: input.principal.to_owned(),
            device_id: input.device_id.to_owned(),
            channel: input.channel.map(str::to_owned),
            summary_text: format!(
                "{} workspace file{} changed via {}",
                input.files_touched.len(),
                if input.files_touched.len() == 1 { "" } else { "s" },
                input.tool_name
            ),
            diff_summary_json: json!({
                "files": input.files_touched.len(),
                "created": created,
                "updated": updated,
                "deleted": deleted,
                "moved": moved,
                "paths": input.files_touched.iter().map(|file| file.path.clone()).collect::<Vec<_>>(),
            })
            .to_string(),
            compare_summary_json: input.compare_summary_json.to_owned(),
            risk_level: input.risk_level.to_owned(),
            review_posture: input.review_posture.to_owned(),
            files,
        })
        .await?;
    Ok(Some(checkpoint))
}

fn build_workspace_checkpoint_files(
    stage: WorkspacePatchCheckpointStage,
    workspace_roots: &[PathBuf],
    attestation: &WorkspacePatchFileAttestation,
) -> Result<Vec<WorkspaceCheckpointFileCreateRequest>, Status> {
    match stage {
        WorkspacePatchCheckpointStage::PostChange => {
            Ok(vec![build_post_change_checkpoint_file(workspace_roots, attestation)?])
        }
        WorkspacePatchCheckpointStage::Preflight => {
            build_preflight_checkpoint_files(workspace_roots, attestation)
        }
    }
}

fn build_post_change_checkpoint_file(
    workspace_roots: &[PathBuf],
    attestation: &WorkspacePatchFileAttestation,
) -> Result<WorkspaceCheckpointFileCreateRequest, Status> {
    let captured_content = if attestation.after_sha256.is_some() {
        Some(read_existing_workspace_checkpoint_content(
            workspace_roots,
            attestation.path.as_str(),
            attestation.workspace_root_index,
            "post_change",
            attestation.after_sha256.as_deref(),
            attestation.after_size_bytes,
        )?)
    } else {
        None
    };
    let content_bytes = captured_content.as_ref().map(|content| content.bytes.clone());
    let content_type = infer_content_type(attestation.path.as_str(), content_bytes.as_deref());
    let (is_text, preview_text, search_text) =
        summarize_workspace_content(content_type.as_str(), content_bytes.as_deref());

    Ok(WorkspaceCheckpointFileCreateRequest {
        artifact_id: Ulid::new().to_string(),
        path: attestation.path.clone(),
        workspace_root_index: attestation.workspace_root_index as u32,
        moved_from_path: attestation.moved_from.clone(),
        change_kind: attestation.operation.clone(),
        before_content_sha256: attestation.before_sha256.clone(),
        before_size_bytes: attestation.before_size_bytes,
        after_content_sha256: captured_content.as_ref().map(|content| content.sha256.clone()),
        after_size_bytes: captured_content.as_ref().map(|content| content.size_bytes),
        content_type,
        is_text,
        preview_text,
        search_text,
        content_bytes,
    })
}

fn build_preflight_checkpoint_files(
    workspace_roots: &[PathBuf],
    attestation: &WorkspacePatchFileAttestation,
) -> Result<Vec<WorkspaceCheckpointFileCreateRequest>, Status> {
    match attestation.operation.as_str() {
        "create" => Ok(vec![build_absent_checkpoint_file(
            attestation.path.clone(),
            attestation.workspace_root_index,
            None,
            "preflight_create",
        )]),
        // A move needs two preflight entries so a restore can undo it fully:
        // the source's content (to recreate it) and the destination's absence
        // (to delete the moved file).
        "move" => {
            let source_path = attestation.moved_from.clone().ok_or_else(|| {
                Status::internal("workspace preflight move checkpoint missing source path")
            })?;
            Ok(vec![
                build_existing_checkpoint_file(
                    workspace_roots,
                    source_path.clone(),
                    attestation.workspace_root_index,
                    None,
                    "preflight_move_source",
                    attestation.before_sha256.clone(),
                    attestation.before_size_bytes,
                )?,
                build_absent_checkpoint_file(
                    attestation.path.clone(),
                    attestation.workspace_root_index,
                    Some(source_path),
                    "preflight_move_destination",
                ),
            ])
        }
        "delete" => Ok(vec![build_existing_checkpoint_file(
            workspace_roots,
            attestation.path.clone(),
            attestation.workspace_root_index,
            None,
            "preflight_delete",
            attestation.before_sha256.clone(),
            attestation.before_size_bytes,
        )?]),
        _ => Ok(vec![build_existing_checkpoint_file(
            workspace_roots,
            attestation.path.clone(),
            attestation.workspace_root_index,
            attestation.moved_from.clone(),
            "preflight_update",
            attestation.before_sha256.clone(),
            attestation.before_size_bytes,
        )?]),
    }
}

struct WorkspaceCheckpointContentRead {
    bytes: Vec<u8>,
    sha256: String,
    size_bytes: u64,
}

/// Reads a workspace file for checkpoint capture, verifying it still matches
/// the size and SHA-256 the mutation plan attested.
///
/// The hash check makes a checkpoint trustworthy as a restore source: if the
/// file changed between planning and capture, the capture fails closed with
/// `failed_precondition` instead of snapshotting unattested content.
fn read_existing_workspace_checkpoint_content(
    workspace_roots: &[PathBuf],
    path: &str,
    workspace_root_index: usize,
    change_kind: &str,
    expected_sha256: Option<&str>,
    expected_size_bytes: Option<u64>,
) -> Result<WorkspaceCheckpointContentRead, Status> {
    let expected_sha256 =
        expected_sha256.filter(|value| !value.trim().is_empty()).ok_or_else(|| {
            Status::failed_precondition(format!(
                "workspace checkpoint {change_kind} is missing planned content hash for {path}"
            ))
        })?;
    let expected_size_bytes = expected_size_bytes.ok_or_else(|| {
        Status::failed_precondition(format!(
            "workspace checkpoint {change_kind} is missing planned size for {path}"
        ))
    })?;
    let workspace_root = workspace_roots
        .get(workspace_root_index)
        .ok_or_else(|| Status::internal("workspace checkpoint root index is out of range"))?;
    let canonical_workspace_root = canonicalize_workspace_checkpoint_root(workspace_root)?;
    let absolute_path = resolve_existing_workspace_checkpoint_file(
        canonical_workspace_root.as_path(),
        Path::new(path),
    )?;
    let content_bytes = read_bounded_workspace_checkpoint_file(
        absolute_path.as_path(),
        expected_size_bytes,
        path,
        change_kind,
    )?;
    let actual_size_bytes = u64::try_from(content_bytes.len()).unwrap_or(u64::MAX);
    if actual_size_bytes != expected_size_bytes {
        return Err(Status::failed_precondition(format!(
            "workspace checkpoint {change_kind} content changed before capture for {path}: expected_size={expected_size_bytes} actual_size={actual_size_bytes}"
        )));
    }
    let actual_sha256 = hex::encode(sha2::Sha256::digest(content_bytes.as_slice()));
    if !actual_sha256.eq_ignore_ascii_case(expected_sha256) {
        return Err(Status::failed_precondition(format!(
            "workspace checkpoint {change_kind} content changed before capture for {path}: planned hash mismatch"
        )));
    }
    Ok(WorkspaceCheckpointContentRead {
        bytes: content_bytes,
        sha256: actual_sha256,
        size_bytes: actual_size_bytes,
    })
}

fn canonicalize_workspace_checkpoint_root(workspace_root: &Path) -> Result<PathBuf, Status> {
    let canonical = fs::canonicalize(workspace_root).map_err(|error| {
        Status::internal(format!(
            "failed to canonicalize workspace checkpoint root {}: {error}",
            workspace_root.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(Status::invalid_argument(format!(
            "workspace checkpoint root is not a directory: {}",
            workspace_root.display()
        )));
    }
    Ok(canonical)
}

fn resolve_existing_workspace_checkpoint_file(
    workspace_root: &Path,
    relative_path: &Path,
) -> Result<PathBuf, Status> {
    validate_workspace_restore_relative_path(relative_path)?;
    let absolute_path = workspace_root.join(relative_path);
    ensure_workspace_restore_target_confined(workspace_root, absolute_path.as_path())?;
    Ok(absolute_path)
}

fn read_bounded_workspace_checkpoint_file(
    absolute_path: &Path,
    expected_size_bytes: u64,
    display_path: &str,
    change_kind: &str,
) -> Result<Vec<u8>, Status> {
    let mut file = fs::File::open(absolute_path).map_err(|error| {
        Status::internal(format!(
            "failed to read workspace checkpoint {change_kind} artifact {}: {error}",
            absolute_path.display()
        ))
    })?;
    // Read one byte past the attested size: a file that grew since planning
    // then fails the caller's size comparison instead of being silently
    // truncated to the expected length, and oversized files cannot balloon
    // memory.
    let read_limit = expected_size_bytes.checked_add(1).ok_or_else(|| {
        Status::failed_precondition(format!(
            "workspace checkpoint {change_kind} expected size is too large for {display_path}"
        ))
    })?;
    let mut content_bytes = Vec::new();
    file.by_ref().take(read_limit).read_to_end(&mut content_bytes).map_err(|error| {
        Status::internal(format!(
            "failed to read workspace checkpoint {change_kind} artifact {}: {error}",
            absolute_path.display()
        ))
    })?;
    Ok(content_bytes)
}

fn build_existing_checkpoint_file(
    workspace_roots: &[PathBuf],
    path: String,
    workspace_root_index: usize,
    moved_from_path: Option<String>,
    change_kind: &str,
    content_sha256: Option<String>,
    size_bytes: Option<u64>,
) -> Result<WorkspaceCheckpointFileCreateRequest, Status> {
    let captured_content = read_existing_workspace_checkpoint_content(
        workspace_roots,
        path.as_str(),
        workspace_root_index,
        change_kind,
        content_sha256.as_deref(),
        size_bytes,
    )?;
    let content_type = infer_content_type(path.as_str(), Some(captured_content.bytes.as_slice()));
    let (is_text, preview_text, search_text) =
        summarize_workspace_content(content_type.as_str(), Some(captured_content.bytes.as_slice()));

    Ok(WorkspaceCheckpointFileCreateRequest {
        artifact_id: Ulid::new().to_string(),
        path,
        workspace_root_index: workspace_root_index as u32,
        moved_from_path,
        change_kind: change_kind.to_owned(),
        before_content_sha256: Some(captured_content.sha256.clone()),
        before_size_bytes: Some(captured_content.size_bytes),
        after_content_sha256: Some(captured_content.sha256),
        after_size_bytes: Some(captured_content.size_bytes),
        content_type,
        is_text,
        preview_text,
        search_text,
        content_bytes: Some(captured_content.bytes),
    })
}

fn build_absent_checkpoint_file(
    path: String,
    workspace_root_index: usize,
    moved_from_path: Option<String>,
    change_kind: &str,
) -> WorkspaceCheckpointFileCreateRequest {
    let content_type = infer_content_type(path.as_str(), None);
    WorkspaceCheckpointFileCreateRequest {
        artifact_id: Ulid::new().to_string(),
        path,
        workspace_root_index: workspace_root_index as u32,
        moved_from_path,
        change_kind: change_kind.to_owned(),
        before_content_sha256: None,
        before_size_bytes: None,
        after_content_sha256: None,
        after_size_bytes: None,
        content_type,
        is_text: false,
        preview_text: None,
        search_text: None,
        content_bytes: None,
    }
}

fn infer_content_type(path: &str, content_bytes: Option<&[u8]>) -> String {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".md") || lower.ends_with(".txt") || lower.ends_with(".log") {
        return "text/plain; charset=utf-8".to_owned();
    }
    if lower.ends_with(".json") {
        return "application/json".to_owned();
    }
    if lower.ends_with(".yaml") || lower.ends_with(".yml") {
        return "application/yaml".to_owned();
    }
    if lower.ends_with(".html") || lower.ends_with(".htm") {
        return "text/html; charset=utf-8".to_owned();
    }
    if lower.ends_with(".png") {
        return "image/png".to_owned();
    }
    if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        return "image/jpeg".to_owned();
    }
    if lower.ends_with(".gif") {
        return "image/gif".to_owned();
    }
    if lower.ends_with(".svg") {
        return "image/svg+xml".to_owned();
    }
    if content_bytes.is_some_and(|bytes| std::str::from_utf8(bytes).is_ok()) {
        return "text/plain; charset=utf-8".to_owned();
    }
    "application/octet-stream".to_owned()
}

fn summarize_workspace_content(
    content_type: &str,
    content_bytes: Option<&[u8]>,
) -> (bool, Option<String>, Option<String>) {
    let Some(content_bytes) = content_bytes else {
        return (false, None, None);
    };
    let is_probably_text = content_type.starts_with("text/")
        || matches!(content_type, "application/json" | "application/yaml");
    if !is_probably_text {
        return (false, None, None);
    }
    let Ok(text) = std::str::from_utf8(content_bytes) else {
        return (false, None, None);
    };
    let preview_text = truncate_chars(text, TEXT_PREVIEW_CHAR_LIMIT);
    let search_text = truncate_chars(text, TEXT_SEARCH_CHAR_LIMIT);
    (true, Some(preview_text), Some(search_text))
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let mut truncated = value.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

trait WorkspaceCheckpointFileRecordExt {
    fn deleted(&self) -> bool;
}

impl WorkspaceCheckpointFileRecordExt for WorkspaceCheckpointFileRecord {
    fn deleted(&self) -> bool {
        self.after_content_sha256.is_none()
    }
}

fn checkpoint_stage_order(stage: &str) -> u8 {
    match stage {
        "preflight" => 0,
        "post_change" => 1,
        _ => 2,
    }
}

struct LoadedCompareAnchor {
    summary: WorkspaceAnchorSummary,
    artifacts: BTreeMap<WorkspaceArtifactKey, WorkspaceArtifactEntry>,
}

#[cfg(test)]
mod tests {
    use super::{
        artifact_matches_query, build_line_diff_preview, build_preflight_checkpoint_files,
        infer_content_type, resolve_workspace_restore_target, summarize_workspace_content,
        WorkspaceArtifactRecord, WorkspaceArtifactVersion,
    };
    use palyra_common::workspace_patch::WorkspacePatchFileAttestation;
    use sha2::{Digest, Sha256};
    use std::path::Path;

    fn sample_artifact() -> WorkspaceArtifactRecord {
        WorkspaceArtifactRecord {
            artifact_id: "artifact-1".to_owned(),
            path: "notes.md".to_owned(),
            display_path: "notes.md".to_owned(),
            workspace_root_index: 0,
            latest_checkpoint_id: "checkpoint-1".to_owned(),
            latest_checkpoint_created_at_unix_ms: 1,
            latest_checkpoint_label: "Workspace patch".to_owned(),
            source_kind: "tool_result".to_owned(),
            source_label: "Workspace patch".to_owned(),
            tool_name: Some("palyra.fs.apply_patch".to_owned()),
            proposal_id: Some("proposal-1".to_owned()),
            device_id: "device-1".to_owned(),
            channel: Some("cli".to_owned()),
            change_kind: "update".to_owned(),
            moved_from_path: None,
            content_type: "text/plain; charset=utf-8".to_owned(),
            preview_kind: "text".to_owned(),
            is_text: true,
            preview_text: Some("hello workspace".to_owned()),
            size_bytes: Some(14),
            content_sha256: Some("hash".to_owned()),
            deleted: false,
            version_count: 1,
            versions: vec![WorkspaceArtifactVersion {
                artifact_id: "artifact-1".to_owned(),
                checkpoint_id: "checkpoint-1".to_owned(),
                checkpoint_created_at_unix_ms: 1,
                change_kind: "update".to_owned(),
                moved_from_path: None,
                content_type: "text/plain; charset=utf-8".to_owned(),
                is_text: true,
                size_bytes: Some(14),
                content_sha256: Some("hash".to_owned()),
                deleted: false,
            }],
        }
    }

    #[test]
    fn infer_content_type_uses_extension_first() {
        assert_eq!(infer_content_type("notes.md", None), "text/plain; charset=utf-8");
        assert_eq!(infer_content_type("report.json", None), "application/json");
    }

    #[test]
    fn summarize_workspace_content_skips_binary_bytes() {
        let (is_text, preview, search) =
            summarize_workspace_content("application/octet-stream", Some(&[0, 159, 146, 150]));
        assert!(!is_text);
        assert!(preview.is_none());
        assert!(search.is_none());
    }

    #[test]
    fn preflight_move_checkpoint_restores_source_and_removes_destination() {
        let tempdir = tempfile::tempdir().expect("workspace tempdir should be created");
        let source_parent = tempdir.path().join("src");
        std::fs::create_dir_all(source_parent.as_path()).expect("source parent should be created");
        let source_path = source_parent.join("old.rs");
        std::fs::write(source_path.as_path(), b"old").expect("source file should be written");
        let before_sha256 = hex::encode(Sha256::digest(b"old"));

        let files = build_preflight_checkpoint_files(
            &[tempdir.path().to_path_buf()],
            &WorkspacePatchFileAttestation {
                path: "src/new.rs".to_owned(),
                workspace_root_index: 0,
                operation: "move".to_owned(),
                moved_from: Some("src/old.rs".to_owned()),
                before_sha256: Some(before_sha256.clone()),
                before_size_bytes: Some(3),
                after_sha256: Some(hex::encode(Sha256::digest(b"new"))),
                after_size_bytes: Some(3),
            },
        )
        .expect("preflight move checkpoint should be captured");

        assert_eq!(files.len(), 2);
        let source = files
            .iter()
            .find(|file| file.path == "src/old.rs")
            .expect("source restore entry should exist");
        assert_eq!(source.after_content_sha256.as_deref(), Some(before_sha256.as_str()));
        assert_eq!(source.content_bytes.as_deref(), Some(&b"old"[..]));

        let destination = files
            .iter()
            .find(|file| file.path == "src/new.rs")
            .expect("destination absence entry should exist");
        assert!(destination.after_content_sha256.is_none());
        assert!(destination.content_bytes.is_none());
    }

    #[test]
    fn preflight_checkpoint_rejects_changed_existing_content() {
        let tempdir = tempfile::tempdir().expect("workspace tempdir should be created");
        let target_path = tempdir.path().join("notes.txt");
        std::fs::write(target_path.as_path(), b"old").expect("source file should be written");
        let before_sha256 = hex::encode(Sha256::digest(b"old"));
        std::fs::write(target_path.as_path(), b"new").expect("source file should change");

        let error = build_preflight_checkpoint_files(
            &[tempdir.path().to_path_buf()],
            &WorkspacePatchFileAttestation {
                path: "notes.txt".to_owned(),
                workspace_root_index: 0,
                operation: "update".to_owned(),
                moved_from: None,
                before_sha256: Some(before_sha256),
                before_size_bytes: Some(3),
                after_sha256: Some(hex::encode(Sha256::digest(b"patched"))),
                after_size_bytes: Some(7),
            },
        )
        .expect_err("changed preflight content should be rejected");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[cfg(unix)]
    #[test]
    fn preflight_checkpoint_rejects_symlink_swapped_parent_escape() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("workspace tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let nested = workspace.join("nested");
        std::fs::create_dir_all(nested.as_path()).expect("nested workspace dir should be created");
        let target_path = nested.join("notes.txt");
        std::fs::write(target_path.as_path(), b"old").expect("source file should be written");
        let before_sha256 = hex::encode(Sha256::digest(b"old"));

        let outside = tempdir.path().join("outside");
        std::fs::create_dir_all(outside.as_path()).expect("outside dir should be created");
        std::fs::write(outside.join("notes.txt").as_path(), b"secret")
            .expect("outside file should be written");
        std::fs::remove_file(target_path.as_path()).expect("source file should be removed");
        std::fs::remove_dir(nested.as_path()).expect("nested dir should be removed");
        symlink(outside.as_path(), nested.as_path()).expect("symlink parent should be created");

        let error = build_preflight_checkpoint_files(
            &[workspace],
            &WorkspacePatchFileAttestation {
                path: "nested/notes.txt".to_owned(),
                workspace_root_index: 0,
                operation: "update".to_owned(),
                moved_from: None,
                before_sha256: Some(before_sha256),
                before_size_bytes: Some(3),
                after_sha256: Some(hex::encode(Sha256::digest(b"patched"))),
                after_size_bytes: Some(7),
            },
        )
        .expect_err("symlink-swapped parent should be rejected before capture");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn workspace_restore_target_allows_new_relative_path_under_root() {
        let tempdir = tempfile::tempdir().expect("workspace tempdir should be created");
        let workspace_root =
            std::fs::canonicalize(tempdir.path()).expect("workspace root should canonicalize");

        let target = resolve_workspace_restore_target(
            workspace_root.as_path(),
            Path::new("nested/notes.txt"),
        )
        .expect("relative restore target should be accepted");

        assert_eq!(target, workspace_root.join("nested").join("notes.txt"));
    }

    #[test]
    fn workspace_restore_target_rejects_parent_component_escape() {
        let tempdir = tempfile::tempdir().expect("workspace tempdir should be created");
        let workspace_root =
            std::fs::canonicalize(tempdir.path()).expect("workspace root should canonicalize");

        let error =
            resolve_workspace_restore_target(workspace_root.as_path(), Path::new("../outside.txt"))
                .expect_err("parent traversal should be rejected");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[cfg(unix)]
    #[test]
    fn workspace_restore_target_rejects_symlink_leaf_escape() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("workspace tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        let outside_file = tempdir.path().join("outside.txt");
        std::fs::write(outside_file.as_path(), b"outside").expect("outside file should be written");
        symlink(outside_file.as_path(), workspace.join("notes.txt").as_path())
            .expect("symlink leaf should be created");
        let workspace_root =
            std::fs::canonicalize(workspace.as_path()).expect("workspace root should canonicalize");

        let error =
            resolve_workspace_restore_target(workspace_root.as_path(), Path::new("notes.txt"))
                .expect_err("symlink leaf should be rejected");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[cfg(unix)]
    #[test]
    fn workspace_restore_target_rejects_symlink_parent_escape() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("workspace tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        std::fs::create_dir_all(outside.as_path()).expect("outside dir should be created");
        symlink(outside.as_path(), workspace.join("link").as_path())
            .expect("symlink parent should be created");
        let workspace_root =
            std::fs::canonicalize(workspace.as_path()).expect("workspace root should canonicalize");

        let error =
            resolve_workspace_restore_target(workspace_root.as_path(), Path::new("link/notes.txt"))
                .expect_err("symlink parent should be rejected");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn artifact_query_matches_preview_and_path() {
        let artifact = sample_artifact();
        assert!(artifact_matches_query(&artifact, "workspace"));
        assert!(artifact_matches_query(&artifact, "notes"));
        assert!(!artifact_matches_query(&artifact, "missing"));
    }

    #[test]
    fn line_diff_preview_marks_removed_and_added_lines() {
        let diff = build_line_diff_preview("alpha\nbeta\n", "alpha\ngamma\n", 20);
        assert!(diff.contains("-beta"));
        assert!(diff.contains("+gamma"));
    }
}
