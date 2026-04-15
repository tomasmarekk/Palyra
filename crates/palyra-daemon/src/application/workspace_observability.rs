use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_common::workspace_patch::WorkspacePatchFileAttestation;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WorkspaceArtifactKey {
    workspace_root_index: u32,
    path: String,
}

#[derive(Debug, Clone)]
struct WorkspaceArtifactEntry {
    checkpoint: WorkspaceCheckpointRecord,
    file: WorkspaceCheckpointFileRecord,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceCheckpointSummary {
    pub checkpoint_id: String,
    pub session_id: String,
    pub run_id: String,
    pub source_kind: String,
    pub source_label: String,
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
    pub created_at_unix_ms: i64,
    pub restore_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_restored_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_restore_report_id: Option<String>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkspaceRestoreFailure {
    pub path: String,
    pub display_path: String,
    pub workspace_root_index: u32,
    pub error: String,
}

pub(crate) struct WorkspacePatchCheckpointCapture<'a> {
    pub principal: &'a str,
    pub device_id: &'a str,
    pub channel: Option<&'a str>,
    pub session_id: &'a str,
    pub run_id: &'a str,
    pub tool_name: &'a str,
    pub proposal_id: &'a str,
    pub workspace_roots: &'a [PathBuf],
    pub files_touched: &'a [WorkspacePatchFileAttestation],
}

pub(crate) struct WorkspaceArtifactListQuery<'a> {
    pub query: Option<&'a str>,
    pub limit: usize,
}

pub(crate) enum WorkspaceCompareAnchor {
    Run(String),
    Checkpoint(String),
}

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

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunWorkspaceArtifactsResponse {
    pub artifacts: Vec<WorkspaceArtifactRecord>,
    pub workspace_checkpoints: Vec<WorkspaceCheckpointSummary>,
    pub background_tasks: Vec<OrchestratorBackgroundTaskRecord>,
    pub compactions: Vec<OrchestratorCompactionArtifactRecord>,
    pub session_checkpoints: Vec<OrchestratorCheckpointRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceAnchorSummary {
    pub kind: String,
    pub id: String,
    pub label: String,
    pub session_id: String,
    pub run_id: String,
    pub created_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceDiffResponse {
    pub left_anchor: WorkspaceAnchorSummary,
    pub right_anchor: WorkspaceAnchorSummary,
    pub files_changed: usize,
    pub files: Vec<WorkspaceDiffFileRecord>,
}

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

pub(crate) struct WorkspaceActivityQuery<'a> {
    pub session_id: Option<&'a str>,
    pub run_id: Option<&'a str>,
    pub device_id: Option<&'a str>,
    pub limit: usize,
}

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

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceRestoreReportDetail {
    pub report: WorkspaceRestoreReportSummary,
    pub checkpoint: WorkspaceCheckpointSummary,
    pub restored_paths: Vec<String>,
    pub failed_paths: Vec<WorkspaceRestoreFailure>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceActivitySnapshot {
    pub summary: WorkspaceRestoreActivitySummary,
    pub recent_checkpoints: Vec<WorkspaceCheckpointSummary>,
    pub recent_restore_reports: Vec<WorkspaceRestoreReportSummary>,
}

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
                .map(|needle| artifact_matches_query(artifact, needle))
                .unwrap_or(true)
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
        } else if left_entry.map(|entry| entry.file.deleted()).unwrap_or(false)
            || right_entry.map(|entry| entry.file.deleted()).unwrap_or(false)
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
                        .map(|value| value == key.workspace_root_index)
                        .unwrap_or(true)
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
        versions.sort_by(|left, right| {
            right
                .checkpoint
                .created_at_unix_ms
                .cmp(&left.checkpoint.created_at_unix_ms)
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
    checkpoints.retain(|candidate| {
        candidate.created_at_unix_ms < checkpoint.created_at_unix_ms
            || (candidate.created_at_unix_ms == checkpoint.created_at_unix_ms
                && candidate.checkpoint_id <= checkpoint.checkpoint_id)
    });
    checkpoints.sort_by(|left, right| {
        left.created_at_unix_ms
            .cmp(&right.created_at_unix_ms)
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
    let absolute_path = workspace_root.join(Path::new(key.path.as_str()));
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
    fs::write(absolute_path.as_path(), content_bytes).map_err(|error| {
        Status::internal(format!(
            "failed to write restored workspace file {}: {error}",
            absolute_path.display()
        ))
    })
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
        tool_name: checkpoint.tool_name,
        proposal_id: checkpoint.proposal_id,
        actor_principal: checkpoint.actor_principal,
        device_id: checkpoint.device_id,
        channel: checkpoint.channel,
        summary_text: checkpoint.summary_text,
        diff_summary: parse_diff_summary_value(checkpoint.diff_summary_json.as_str()),
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
            .map(|value| value.to_ascii_lowercase().contains(query))
            .unwrap_or(false)
        || artifact.versions.iter().any(|version| {
            version
                .moved_from_path
                .as_deref()
                .map(|value| value.to_ascii_lowercase().contains(query))
                .unwrap_or(false)
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

pub(crate) async fn capture_workspace_patch_checkpoint(
    runtime_state: &Arc<GatewayRuntimeState>,
    input: WorkspacePatchCheckpointCapture<'_>,
) -> Result<Option<WorkspaceCheckpointRecord>, Status> {
    if input.files_touched.is_empty() {
        return Ok(None);
    }

    let mut files = Vec::with_capacity(input.files_touched.len());
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
        files.push(build_workspace_checkpoint_file(input.workspace_roots, attestation)?);
    }

    let checkpoint = runtime_state
        .create_workspace_checkpoint(WorkspaceCheckpointCreateRequest {
            checkpoint_id: Ulid::new().to_string(),
            session_id: input.session_id.to_owned(),
            run_id: input.run_id.to_owned(),
            source_kind: "tool_result".to_owned(),
            source_label: "Workspace patch".to_owned(),
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
            files,
        })
        .await?;
    Ok(Some(checkpoint))
}

fn build_workspace_checkpoint_file(
    workspace_roots: &[PathBuf],
    attestation: &WorkspacePatchFileAttestation,
) -> Result<WorkspaceCheckpointFileCreateRequest, Status> {
    let workspace_root = workspace_roots
        .get(attestation.workspace_root_index)
        .ok_or_else(|| Status::internal("workspace checkpoint root index is out of range"))?;
    let absolute_path = workspace_root.join(Path::new(attestation.path.as_str()));
    let content_bytes = if attestation.after_sha256.is_some() {
        Some(fs::read(absolute_path.as_path()).map_err(|error| {
            Status::internal(format!(
                "failed to read workspace checkpoint artifact {}: {error}",
                absolute_path.display()
            ))
        })?)
    } else {
        None
    };
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
        after_content_sha256: attestation.after_sha256.clone(),
        after_size_bytes: attestation.after_size_bytes,
        content_type,
        is_text,
        preview_text,
        search_text,
        content_bytes,
    })
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

struct LoadedCompareAnchor {
    summary: WorkspaceAnchorSummary,
    artifacts: BTreeMap<WorkspaceArtifactKey, WorkspaceArtifactEntry>,
}

#[cfg(test)]
mod tests {
    use super::{
        artifact_matches_query, build_line_diff_preview, infer_content_type,
        summarize_workspace_content, WorkspaceArtifactRecord, WorkspaceArtifactVersion,
    };

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
