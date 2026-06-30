//! Checkpointed mutation flow for `palyra.fs.apply_patch` writes.
//!
//! Real (non-dry-run) patches are bracketed by workspace checkpoints:
//! preflight capture -> apply (via the palyra-common workspace_patch engine)
//! -> post-change capture -> pair link with a compare summary. High-risk
//! mutations (deletes/moves, lockfiles, CI/security paths) fail closed when
//! the preflight checkpoint cannot be captured; lower-risk ones degrade to a
//! flagged best effort instead of blocking the patch.
//!
//! Journal event names and output JSON keys are pinned by tests and
//! fixtures; keep them byte-identical.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use palyra_common::workspace_patch::{
    apply_workspace_patch, apply_workspace_patch_with_canonical_root_constraints,
    validate_workspace_patch_roots_with_canonical_constraints, WorkspacePatchError,
    WorkspacePatchFileAttestation, WorkspacePatchLimits, WorkspacePatchOutcome,
    WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
};
use serde_json::{json, Value};
use tracing::{error, warn};
use ulid::Ulid;

use crate::{
    application::project_facts::{
        append_project_facts_output, project_facts_journal_projection, workspace_root_ref,
        ProjectFactsCaptureRequest, ProjectFactsDecision, ProjectFactsJournalProjection,
        ProjectFactsService, ProjectFactsSnapshot, PROJECT_FACTS_COMPLETED_EVENT,
        PROJECT_FACTS_FAILED_EVENT, PROJECT_FACTS_STARTED_EVENT,
    },
    application::tool_runtime::code_intel,
    application::workspace_observability::{
        capture_workspace_patch_checkpoint, compare_workspace_anchors, WorkspaceCompareAnchor,
        WorkspacePatchCheckpointCapture, WorkspacePatchCheckpointStage,
    },
    gateway::{current_unix_ms, record_agent_journal_event, GatewayRuntimeState},
    journal::{
        JournalAppendRequest, WorkspaceCheckpointPairLinkRequest, WorkspaceCheckpointRecord,
    },
    tool_protocol::ToolExecutionOutcome,
    transport::grpc::auth::RequestContext,
    transport::grpc::proto::palyra::common::v1 as common_v1,
};

use super::{workspace_patch_error_outcome, workspace_patch_tool_execution_outcome};

/// Borrowed inputs for one checkpointed patch mutation; the planned outcome
/// is included so risk is assessed before any write happens.
pub(super) struct WorkspacePatchMutationRequest<'a> {
    pub(super) principal: &'a str,
    pub(super) device_id: &'a str,
    pub(super) channel: Option<&'a str>,
    pub(super) session_id: &'a str,
    pub(super) run_id: &'a str,
    pub(super) proposal_id: &'a str,
    pub(super) input_json: &'a [u8],
    pub(super) patch: &'a str,
    pub(super) redaction_policy: &'a WorkspacePatchRedactionPolicy,
    pub(super) limits: &'a WorkspacePatchLimits,
    pub(super) workspace_roots: &'a [PathBuf],
    pub(super) canonical_constraint_roots: &'a [PathBuf],
    pub(super) risk_path_prefixes: &'a [String],
    pub(super) planned_outcome: WorkspacePatchOutcome,
}

/// Applies a planned patch with checkpoint bracketing.
///
/// The ordering is deliberate: constraint validation and the preflight
/// checkpoint happen before any write so high-risk mutations can fail closed
/// with the workspace untouched, while checkpoint or pair-link failures
/// after a successful apply only degrade the output (`degraded: true`)
/// instead of reverting the patch.
pub(super) async fn execute_workspace_patch_mutation(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: WorkspacePatchMutationRequest<'_>,
) -> ToolExecutionOutcome {
    let WorkspacePatchMutationRequest {
        principal,
        device_id,
        channel,
        session_id,
        run_id,
        proposal_id,
        input_json,
        patch,
        redaction_policy,
        limits,
        workspace_roots,
        canonical_constraint_roots,
        risk_path_prefixes,
        planned_outcome,
    } = request;
    let mutation_id = Ulid::new().to_string();
    let risk = assess_workspace_mutation_risk(
        planned_outcome.files_touched.as_slice(),
        risk_path_prefixes,
    );
    let mut preflight_checkpoint = None;
    let mut preflight_error = None;

    if let Err(error) =
        validate_patch_roots_against_constraints(workspace_roots, canonical_constraint_roots)
    {
        return workspace_patch_error_outcome(
            proposal_id,
            input_json,
            false,
            patch,
            redaction_policy,
            limits,
            &error,
        );
    }

    match capture_workspace_patch_checkpoint(
        runtime_state,
        WorkspacePatchCheckpointCapture {
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            tool_name: "palyra.fs.apply_patch",
            proposal_id,
            checkpoint_stage: WorkspacePatchCheckpointStage::Preflight,
            mutation_id: Some(mutation_id.as_str()),
            paired_checkpoint_id: None,
            compare_summary_json: "{}",
            risk_level: risk.level.as_str(),
            review_posture: risk.review_posture,
            workspace_roots,
            files_touched: planned_outcome.files_touched.as_slice(),
        },
    )
    .await
    {
        Ok(checkpoint) => {
            preflight_checkpoint = checkpoint;
            if let Some(checkpoint) = preflight_checkpoint.as_ref() {
                record_workspace_checkpoint_created_event(runtime_state, checkpoint).await;
            }
        }
        Err(status) => {
            error!(
                proposal_id = %proposal_id,
                session_id = %session_id,
                run_id = %run_id,
                risk_level = %risk.level.as_str(),
                error = %status,
                "workspace preflight checkpoint capture failed before patch apply"
            );
            if risk.fail_closed_without_preflight {
                return workspace_patch_preflight_failure_outcome(
                    proposal_id,
                    input_json,
                    &planned_outcome,
                    workspace_roots,
                    mutation_id.as_str(),
                    &risk,
                    status.message(),
                );
            }
            preflight_error = Some(status.message().to_owned());
        }
    }

    let diagnostic_baseline = code_intel::capture_diagnostic_snapshot(
        &runtime_state.config.code_intel,
        workspace_roots,
        planned_outcome.files_touched.as_slice(),
    );

    let request = WorkspacePatchRequest {
        patch: patch.to_owned(),
        dry_run: false,
        redaction_policy: redaction_policy.clone(),
    };
    let outcome = match apply_patch_with_constraints(
        workspace_roots,
        canonical_constraint_roots,
        &request,
        limits,
    ) {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_patch_error_outcome(
                proposal_id,
                input_json,
                false,
                patch,
                redaction_policy,
                limits,
                &error,
            );
        }
    };

    let mut output_value = match serde_json::to_value(&outcome) {
        Ok(value) => value,
        Err(error) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.fs.apply_patch failed to serialize output: {error}"),
            );
        }
    };
    super::augment_workspace_patch_output_paths(&mut output_value, workspace_roots);

    let mut post_change_checkpoint = None;
    let mut post_change_error = None;
    match capture_workspace_patch_checkpoint(
        runtime_state,
        WorkspacePatchCheckpointCapture {
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            tool_name: "palyra.fs.apply_patch",
            proposal_id,
            checkpoint_stage: WorkspacePatchCheckpointStage::PostChange,
            mutation_id: Some(mutation_id.as_str()),
            paired_checkpoint_id: preflight_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.checkpoint_id.as_str()),
            compare_summary_json: "{}",
            risk_level: risk.level.as_str(),
            review_posture: risk.review_posture,
            workspace_roots,
            files_touched: outcome.files_touched.as_slice(),
        },
    )
    .await
    {
        Ok(checkpoint) => {
            post_change_checkpoint = checkpoint;
            if let Some(checkpoint) = post_change_checkpoint.as_ref() {
                record_workspace_checkpoint_created_event(runtime_state, checkpoint).await;
            }
        }
        Err(status) => {
            error!(
                proposal_id = %proposal_id,
                session_id = %session_id,
                run_id = %run_id,
                error = %status,
                "workspace post-change checkpoint capture failed after patch apply"
            );
            post_change_error = Some(status.message().to_owned());
        }
    }

    let mut compare_summary = json!({});
    let mut pair_error = None;
    if let (Some(preflight), Some(post_change)) =
        (preflight_checkpoint.as_ref(), post_change_checkpoint.as_ref())
    {
        compare_summary =
            workspace_patch_pair_compare_summary(runtime_state, preflight, post_change).await;
        let compare_summary_json = compare_summary.to_string();
        match runtime_state
            .link_workspace_checkpoint_pair(WorkspaceCheckpointPairLinkRequest {
                mutation_id: mutation_id.clone(),
                preflight_checkpoint_id: preflight.checkpoint_id.clone(),
                post_change_checkpoint_id: post_change.checkpoint_id.clone(),
                compare_summary_json,
                review_posture: risk.review_posture.to_owned(),
            })
            .await
        {
            Ok(()) => {
                record_workspace_checkpoint_pair_event(
                    runtime_state,
                    preflight,
                    post_change,
                    mutation_id.as_str(),
                    &compare_summary,
                    &risk,
                )
                .await;
            }
            Err(status) => {
                error!(
                    proposal_id = %proposal_id,
                    session_id = %session_id,
                    run_id = %run_id,
                    error = %status,
                    "workspace checkpoint pair link failed"
                );
                pair_error = Some(status.message().to_owned());
            }
        }
    }

    let checkpoint_output_context = WorkspaceCheckpointOutputContext {
        mutation_id: mutation_id.as_str(),
        risk: &risk,
        preflight_checkpoint: preflight_checkpoint.as_ref(),
        post_change_checkpoint: post_change_checkpoint.as_ref(),
        compare_summary: &compare_summary,
        preflight_error: preflight_error.as_deref(),
        post_change_error: post_change_error.as_deref(),
        pair_error: pair_error.as_deref(),
    };
    append_workspace_checkpoint_output(&mut output_value, checkpoint_output_context);
    let diagnostic_after = code_intel::capture_diagnostic_snapshot(
        &runtime_state.config.code_intel,
        workspace_roots,
        outcome.files_touched.as_slice(),
    );
    let diagnostic_delta = code_intel::diagnostic_delta(
        &runtime_state.config.code_intel,
        &diagnostic_baseline,
        &diagnostic_after,
    );
    code_intel::append_diagnostics_output(&mut output_value, diagnostic_delta);
    if let Some(project_facts) = capture_project_facts_for_coding_posture(
        runtime_state,
        ProjectFactsPatchCapture {
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            workspace_roots,
            files_touched: outcome.files_touched.as_slice(),
        },
    )
    .await
    {
        append_project_facts_output(&mut output_value, project_facts);
    }
    serialize_workspace_patch_success_value(proposal_id, input_json, output_value)
}

struct ProjectFactsPatchCapture<'a> {
    principal: &'a str,
    device_id: &'a str,
    channel: Option<&'a str>,
    session_id: &'a str,
    run_id: &'a str,
    workspace_roots: &'a [PathBuf],
    files_touched: &'a [WorkspacePatchFileAttestation],
}

async fn capture_project_facts_for_coding_posture(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: ProjectFactsPatchCapture<'_>,
) -> Option<ProjectFactsSnapshot> {
    if !runtime_state.config.feature_rollouts.verification_runtime.enabled {
        return None;
    }
    let (workspace_root_index, workspace_root) =
        select_project_facts_workspace_root(request.workspace_roots, request.files_touched)?;
    let started_at_unix_ms = current_unix_ms();
    let root_ref =
        workspace_root_ref(workspace_root_index, workspace_root, workspace_root.exists());
    let started_projection = project_facts_journal_projection(
        PROJECT_FACTS_STARTED_EVENT,
        request.session_id,
        request.run_id,
        None,
        root_ref.clone(),
        started_at_unix_ms,
        None,
    );
    record_project_facts_journal_projection(
        runtime_state,
        request.principal,
        request.device_id,
        request.channel,
        started_projection,
    )
    .await;

    let snapshot = ProjectFactsService::capture(ProjectFactsCaptureRequest {
        workspace_root_index,
        workspace_root,
        files_touched: request.files_touched,
        generated_at_unix_ms: current_unix_ms(),
        rollout_enabled: true,
    });
    let event_type = if snapshot.decision == ProjectFactsDecision::Failed {
        PROJECT_FACTS_FAILED_EVENT
    } else {
        PROJECT_FACTS_COMPLETED_EVENT
    };
    let projection = project_facts_journal_projection(
        event_type,
        request.session_id,
        request.run_id,
        Some(snapshot.clone()),
        snapshot.workspace_root.clone(),
        current_unix_ms(),
        None,
    );
    record_project_facts_journal_projection(
        runtime_state,
        request.principal,
        request.device_id,
        request.channel,
        projection,
    )
    .await;
    Some(snapshot)
}

async fn record_project_facts_journal_projection(
    runtime_state: &Arc<GatewayRuntimeState>,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
    projection: ProjectFactsJournalProjection,
) {
    let payload_json = match serde_json::to_string(&projection) {
        Ok(payload) => payload,
        Err(error) => {
            warn!(
                event_type = %projection.event_type,
                error = %error,
                "failed to serialize project facts journal projection"
            );
            return;
        }
    };
    if let Err(error) = runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: projection.session_id,
            run_id: projection.run_id,
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: projection.created_at_unix_ms,
            payload_json: payload_json.into_bytes(),
            principal: principal.to_owned(),
            device_id: device_id.to_owned(),
            channel: channel.map(str::to_owned),
        })
        .await
    {
        warn!(
            event_type = %projection.event_type,
            status_code = ?error.code(),
            status_message = %error.message(),
            "project facts journal write failed"
        );
    }
}

fn select_project_facts_workspace_root<'a>(
    workspace_roots: &'a [PathBuf],
    files_touched: &[WorkspacePatchFileAttestation],
) -> Option<(usize, &'a Path)> {
    if workspace_roots.is_empty() {
        return None;
    }
    let root_index = files_touched
        .iter()
        .find_map(|file| {
            (file.workspace_root_index < workspace_roots.len()).then_some(file.workspace_root_index)
        })
        .unwrap_or(0);
    workspace_roots.get(root_index).map(|root| (root_index, root.as_path()))
}

/// Dispatches to the constrained engine entry point when canonical
/// constraint roots are present (narrowed override/focus scope), otherwise
/// to the plain one.
fn apply_patch_with_constraints(
    workspace_roots: &[PathBuf],
    canonical_constraint_roots: &[PathBuf],
    request: &WorkspacePatchRequest,
    limits: &WorkspacePatchLimits,
) -> Result<WorkspacePatchOutcome, WorkspacePatchError> {
    if canonical_constraint_roots.is_empty() {
        apply_workspace_patch(workspace_roots, request, limits)
    } else {
        apply_workspace_patch_with_canonical_root_constraints(
            workspace_roots,
            canonical_constraint_roots,
            request,
            limits,
        )
    }
}

/// Re-validates the narrowed roots against the canonical agent roots right
/// before the mutation; a no-op when no constraints apply.
fn validate_patch_roots_against_constraints(
    workspace_roots: &[PathBuf],
    canonical_constraint_roots: &[PathBuf],
) -> Result<(), WorkspacePatchError> {
    if canonical_constraint_roots.is_empty() {
        Ok(())
    } else {
        validate_workspace_patch_roots_with_canonical_constraints(
            workspace_roots,
            canonical_constraint_roots,
        )
    }
}

/// Risk class of one planned mutation; rendered into checkpoint records and
/// journal events via [`Self::as_str`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkspaceMutationRiskLevel {
    Low,
    Medium,
    High,
}

impl WorkspaceMutationRiskLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Risk verdict for one mutation: level, the review posture it implies, and
/// whether the mutation must be refused when no preflight checkpoint exists.
#[derive(Debug, Clone, Copy)]
struct WorkspaceMutationRisk {
    level: WorkspaceMutationRiskLevel,
    review_posture: &'static str,
    fail_closed_without_preflight: bool,
}

/// Classifies a planned mutation: file count escalates Low -> Medium (>4)
/// -> High (>8); any delete/move or high-risk path (including `moved_from`
/// sources) forces High, which requires review and fails closed without a
/// preflight checkpoint.
fn assess_workspace_mutation_risk(
    files_touched: &[WorkspacePatchFileAttestation],
    risk_path_prefixes: &[String],
) -> WorkspaceMutationRisk {
    let mut level = if files_touched.len() > 4 {
        WorkspaceMutationRiskLevel::Medium
    } else {
        WorkspaceMutationRiskLevel::Low
    };
    if files_touched.len() > 8 {
        level = WorkspaceMutationRiskLevel::High;
    }
    for file in files_touched {
        if matches!(file.operation.as_str(), "delete" | "move")
            || has_high_risk_workspace_path(file.path.as_str(), risk_path_prefixes)
            || file
                .moved_from
                .as_deref()
                .is_some_and(|path| has_high_risk_workspace_path(path, risk_path_prefixes))
        {
            level = WorkspaceMutationRiskLevel::High;
            break;
        }
        if has_medium_risk_workspace_path(file.path.as_str(), risk_path_prefixes) {
            level = WorkspaceMutationRiskLevel::Medium;
        }
    }
    WorkspaceMutationRisk {
        level,
        review_posture: if level == WorkspaceMutationRiskLevel::High {
            "review_required"
        } else {
            "standard"
        },
        fail_closed_without_preflight: level == WorkspaceMutationRiskLevel::High,
    }
}

/// Checks the path both directly and re-rooted under each risk prefix, so
/// rules keyed to repository-root paths still match when the patch ran from
/// a narrowed workspace root.
fn has_high_risk_workspace_path(path: &str, risk_path_prefixes: &[String]) -> bool {
    is_high_risk_workspace_path(path)
        || risk_path_prefixes.iter().any(|prefix| {
            is_high_risk_workspace_path(prefixed_workspace_risk_path(prefix, path).as_str())
        })
}

fn has_medium_risk_workspace_path(path: &str, risk_path_prefixes: &[String]) -> bool {
    is_medium_risk_workspace_path(path)
        || risk_path_prefixes.iter().any(|prefix| {
            is_medium_risk_workspace_path(prefixed_workspace_risk_path(prefix, path).as_str())
        })
}

fn prefixed_workspace_risk_path(prefix: &str, path: &str) -> String {
    let normalized_prefix = normalize_workspace_risk_path(prefix);
    let normalized_path = normalize_workspace_risk_path(path);
    if normalized_prefix.is_empty() {
        normalized_path
    } else if normalized_path.is_empty() {
        normalized_prefix
    } else {
        format!("{normalized_prefix}/{normalized_path}")
    }
}

fn normalize_workspace_risk_path(path: &str) -> String {
    path.replace('\\', "/").trim().trim_start_matches("./").trim_matches('/').to_owned()
}

/// Repository paths whose mutation always escalates to High risk:
/// supply-chain manifests/lockfiles, CI workflows, and security-critical
/// crate subtrees.
fn is_high_risk_workspace_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    normalized == "cargo.toml"
        || normalized == "cargo.lock"
        || normalized == "deny.toml"
        || normalized == "osv-scanner.toml"
        || normalized == "npm-audit-dev-allowlist.json"
        || normalized == "package-lock.json"
        || normalized == "pnpm-lock.yaml"
        || normalized.starts_with(".github/workflows/")
        || normalized.starts_with("crates/palyra-auth/")
        || normalized.starts_with("crates/palyra-vault/")
        || normalized.starts_with("crates/palyra-policy/")
        || normalized.starts_with("crates/palyra-sandbox/")
        || normalized.starts_with("crates/palyra-daemon/src/application/approvals/")
        || normalized.starts_with("crates/palyra-daemon/src/application/tool_runtime/")
        || normalized.starts_with("crates/palyra-daemon/src/transport/")
}

fn is_medium_risk_workspace_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    normalized.starts_with("scripts/")
        || normalized.ends_with(".toml")
        || normalized.ends_with(".yaml")
        || normalized.ends_with(".yml")
        || normalized.ends_with(".json")
}

/// Records the `workspace.checkpoint.created` journal event for a captured
/// checkpoint.
async fn record_workspace_checkpoint_created_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    checkpoint: &WorkspaceCheckpointRecord,
) {
    // Journaling is best-effort here: a journal write failure must not fail
    // a patch whose checkpoint capture already succeeded.
    let _ = record_agent_journal_event(
        runtime_state,
        &RequestContext {
            principal: checkpoint.actor_principal.clone(),
            device_id: checkpoint.device_id.clone(),
            channel: checkpoint.channel.clone(),
        },
        json!({
            "event": "workspace.checkpoint.created",
            "checkpoint_id": checkpoint.checkpoint_id.as_str(),
            "session_id": checkpoint.session_id.as_str(),
            "run_id": checkpoint.run_id.as_str(),
            "source_kind": checkpoint.source_kind.as_str(),
            "source_label": checkpoint.source_label.as_str(),
            "checkpoint_stage": checkpoint.checkpoint_stage.as_str(),
            "mutation_id": checkpoint.mutation_id.as_deref(),
            "paired_checkpoint_id": checkpoint.paired_checkpoint_id.as_deref(),
            "tool_name": checkpoint.tool_name.as_deref(),
            "proposal_id": checkpoint.proposal_id.as_deref(),
            "actor_principal": checkpoint.actor_principal.as_str(),
            "device_id": checkpoint.device_id.as_str(),
            "channel": checkpoint.channel.as_deref(),
            "summary_text": checkpoint.summary_text.as_str(),
            "risk_level": checkpoint.risk_level.as_str(),
            "review_posture": checkpoint.review_posture.as_str(),
            "diff_summary": parse_checkpoint_json_field(checkpoint.diff_summary_json.as_str()),
            "compare_summary": parse_checkpoint_json_field(checkpoint.compare_summary_json.as_str()),
        }),
    )
    .await;
}

/// Records the `workspace.checkpoint.pair_created` journal event once both
/// checkpoints exist and are linked; best-effort like the created event.
async fn record_workspace_checkpoint_pair_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    preflight: &WorkspaceCheckpointRecord,
    post_change: &WorkspaceCheckpointRecord,
    mutation_id: &str,
    compare_summary: &Value,
    risk: &WorkspaceMutationRisk,
) {
    let _ = record_agent_journal_event(
        runtime_state,
        &RequestContext {
            principal: post_change.actor_principal.clone(),
            device_id: post_change.device_id.clone(),
            channel: post_change.channel.clone(),
        },
        json!({
            "event": "workspace.checkpoint.pair_created",
            "mutation_id": mutation_id,
            "preflight_checkpoint_id": preflight.checkpoint_id.as_str(),
            "post_change_checkpoint_id": post_change.checkpoint_id.as_str(),
            "session_id": post_change.session_id.as_str(),
            "run_id": post_change.run_id.as_str(),
            "proposal_id": post_change.proposal_id.as_deref(),
            "risk_level": risk.level.as_str(),
            "review_posture": risk.review_posture,
            "compare_summary": compare_summary,
        }),
    )
    .await;
}

/// Compares the preflight and post-change checkpoints into a bounded summary
/// (up to 64 files); a compare failure is reported inside the summary rather
/// than failing the mutation.
async fn workspace_patch_pair_compare_summary(
    runtime_state: &Arc<GatewayRuntimeState>,
    preflight: &WorkspaceCheckpointRecord,
    post_change: &WorkspaceCheckpointRecord,
) -> Value {
    let started = Instant::now();
    // The `as u64` latency casts below are safe: as_millis() exceeds u64
    // only after ~584 million years of elapsed time.
    match compare_workspace_anchors(
        runtime_state,
        WorkspaceCompareAnchor::Checkpoint(preflight.checkpoint_id.clone()),
        WorkspaceCompareAnchor::Checkpoint(post_change.checkpoint_id.clone()),
        64,
    )
    .await
    {
        Ok(diff) => json!({
            "files_changed": diff.files_changed,
            "compare_latency_ms": started.elapsed().as_millis() as u64,
            "paths": diff.files.iter().map(|file| file.path.clone()).collect::<Vec<_>>(),
        }),
        Err(status) => json!({
            "compare_latency_ms": started.elapsed().as_millis() as u64,
            "compare_error": status.message(),
        }),
    }
}

/// Everything needed to render the checkpoint section of the tool output.
struct WorkspaceCheckpointOutputContext<'a> {
    mutation_id: &'a str,
    risk: &'a WorkspaceMutationRisk,
    preflight_checkpoint: Option<&'a WorkspaceCheckpointRecord>,
    post_change_checkpoint: Option<&'a WorkspaceCheckpointRecord>,
    compare_summary: &'a Value,
    preflight_error: Option<&'a str>,
    post_change_error: Option<&'a str>,
    pair_error: Option<&'a str>,
}

/// Appends checkpoint metadata to the successful patch output, including a
/// `degraded` flag when any capture or pair-link step failed.
fn append_workspace_checkpoint_output(
    output_value: &mut Value,
    context: WorkspaceCheckpointOutputContext<'_>,
) {
    let Some(payload) = output_value.as_object_mut() else {
        return;
    };
    // Post-change values are published under both the `workspace_checkpoint*`
    // and `post_change_checkpoint*` keys; consumers rely on both spellings.
    if let Some(checkpoint) = context.post_change_checkpoint {
        payload.insert("workspace_checkpoint".to_owned(), checkpoint_output_value(checkpoint));
        payload.insert("post_change_checkpoint".to_owned(), checkpoint_output_value(checkpoint));
    }
    if let Some(checkpoint) = context.preflight_checkpoint {
        payload.insert("preflight_checkpoint".to_owned(), checkpoint_output_value(checkpoint));
    }
    if let Some(error) = context.preflight_error {
        payload.insert("preflight_checkpoint_error".to_owned(), Value::String(error.to_owned()));
    }
    if let Some(error) = context.post_change_error {
        payload.insert("workspace_checkpoint_error".to_owned(), Value::String(error.to_owned()));
        payload.insert("post_change_checkpoint_error".to_owned(), Value::String(error.to_owned()));
    }
    if let Some(error) = context.pair_error {
        payload
            .insert("workspace_checkpoint_pair_error".to_owned(), Value::String(error.to_owned()));
    }
    let degraded = context.preflight_error.is_some()
        || context.post_change_error.is_some()
        || context.pair_error.is_some();
    payload.insert(
        "workspace_checkpoint_pair".to_owned(),
        json!({
            "mutation_id": context.mutation_id,
            "preflight_checkpoint_id": context.preflight_checkpoint
                .map(|checkpoint| checkpoint.checkpoint_id.as_str()),
            "post_change_checkpoint_id": context.post_change_checkpoint
                .map(|checkpoint| checkpoint.checkpoint_id.as_str()),
            "risk_level": context.risk.level.as_str(),
            "review_posture": context.risk.review_posture,
            "degraded": degraded,
            "compare_summary": context.compare_summary,
        }),
    );
}

fn checkpoint_output_value(checkpoint: &WorkspaceCheckpointRecord) -> Value {
    json!({
        "checkpoint_id": checkpoint.checkpoint_id.as_str(),
        "session_id": checkpoint.session_id.as_str(),
        "run_id": checkpoint.run_id.as_str(),
        "summary_text": checkpoint.summary_text.as_str(),
        "source_kind": checkpoint.source_kind.as_str(),
        "source_label": checkpoint.source_label.as_str(),
        "checkpoint_stage": checkpoint.checkpoint_stage.as_str(),
        "mutation_id": checkpoint.mutation_id.as_deref(),
        "paired_checkpoint_id": checkpoint.paired_checkpoint_id.as_deref(),
        "tool_name": checkpoint.tool_name.as_deref(),
        "device_id": checkpoint.device_id.as_str(),
        "channel": checkpoint.channel.as_deref(),
        "created_at_unix_ms": checkpoint.created_at_unix_ms,
        "risk_level": checkpoint.risk_level.as_str(),
        "review_posture": checkpoint.review_posture.as_str(),
        "diff_summary": parse_checkpoint_json_field(checkpoint.diff_summary_json.as_str()),
        "compare_summary": parse_checkpoint_json_field(checkpoint.compare_summary_json.as_str()),
    })
}

/// Parses a stored JSON field, falling back to the raw string so malformed
/// stored JSON still surfaces in events instead of being dropped.
fn parse_checkpoint_json_field(raw: &str) -> Value {
    serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}

/// Serializes the checkpoint-augmented output value into the success
/// outcome.
fn serialize_workspace_patch_success_value(
    proposal_id: &str,
    input_json: &[u8],
    output_value: Value,
) -> ToolExecutionOutcome {
    match serde_json::to_vec(&output_value) {
        Ok(output_json) => workspace_patch_tool_execution_outcome(
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => workspace_patch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.fs.apply_patch failed to serialize output: {error}"),
        ),
    }
}

/// Failure outcome for a high-risk mutation refused because its preflight
/// checkpoint could not be captured; includes the planned (unapplied)
/// outcome and a degraded pair stub so callers can see what was blocked.
fn workspace_patch_preflight_failure_outcome(
    proposal_id: &str,
    input_json: &[u8],
    planned_outcome: &WorkspacePatchOutcome,
    workspace_roots: &[PathBuf],
    mutation_id: &str,
    risk: &WorkspaceMutationRisk,
    checkpoint_error: &str,
) -> ToolExecutionOutcome {
    let mut output_value = serde_json::to_value(planned_outcome).unwrap_or_else(|_| json!({}));
    super::augment_workspace_patch_output_paths(&mut output_value, workspace_roots);
    if let Some(payload) = output_value.as_object_mut() {
        payload.insert("dry_run".to_owned(), Value::Bool(false));
        payload.insert(
            "preflight_checkpoint_error".to_owned(),
            Value::String(checkpoint_error.to_owned()),
        );
        payload.insert(
            "workspace_checkpoint_pair".to_owned(),
            json!({
                "mutation_id": mutation_id,
                "preflight_checkpoint_id": null,
                "post_change_checkpoint_id": null,
                "risk_level": risk.level.as_str(),
                "review_posture": risk.review_posture,
                "degraded": true,
                "compare_summary": {},
            }),
        );
    }
    let output_json = serde_json::to_vec(&output_value).unwrap_or_else(|_| b"{}".to_vec());
    workspace_patch_tool_execution_outcome(
        proposal_id,
        input_json,
        false,
        output_json,
        format!(
            "palyra.fs.apply_patch refused high-risk mutation because preflight checkpoint failed: {checkpoint_error}"
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        assess_workspace_mutation_risk, select_project_facts_workspace_root,
        WorkspaceMutationRiskLevel,
    };
    use palyra_common::workspace_patch::WorkspacePatchFileAttestation;

    fn attestation(path: &str, operation: &str) -> WorkspacePatchFileAttestation {
        attestation_for_root(path, operation, 0)
    }

    fn attestation_for_root(
        path: &str,
        operation: &str,
        workspace_root_index: usize,
    ) -> WorkspacePatchFileAttestation {
        WorkspacePatchFileAttestation {
            path: path.to_owned(),
            workspace_root_index,
            operation: operation.to_owned(),
            moved_from: None,
            before_sha256: None,
            before_size_bytes: None,
            after_sha256: Some("sha256".to_owned()),
            after_size_bytes: Some(1),
        }
    }

    #[test]
    fn risk_assessment_applies_workspace_root_prefixes() {
        let files = vec![attestation("ci.yml", "add")];

        let without_prefix = assess_workspace_mutation_risk(files.as_slice(), &[]);
        assert_eq!(without_prefix.level, WorkspaceMutationRiskLevel::Medium);
        assert!(!without_prefix.fail_closed_without_preflight);

        let prefixes = vec![".github/workflows".to_owned()];
        let with_prefix = assess_workspace_mutation_risk(files.as_slice(), prefixes.as_slice());
        assert_eq!(with_prefix.level, WorkspaceMutationRiskLevel::High);
        assert!(with_prefix.fail_closed_without_preflight);
        assert_eq!(with_prefix.review_posture, "review_required");
    }

    #[test]
    fn risk_assessment_applies_workspace_root_prefixes_to_moved_from_paths() {
        let mut file = attestation("safe.rs", "replace");
        file.moved_from = Some("old.rs".to_owned());
        let prefixes = vec!["crates/palyra-auth".to_owned()];

        let risk = assess_workspace_mutation_risk(&[file], prefixes.as_slice());

        assert_eq!(risk.level, WorkspaceMutationRiskLevel::High);
        assert!(risk.fail_closed_without_preflight);
    }

    #[test]
    fn project_facts_capture_uses_touched_workspace_root_index() {
        let roots = vec![PathBuf::from("first"), PathBuf::from("second")];
        let files = vec![attestation_for_root("src/lib.rs", "update", 1)];

        let selected = select_project_facts_workspace_root(roots.as_slice(), files.as_slice())
            .expect("workspace root should be selected");

        assert_eq!(selected.0, 1);
        assert_eq!(selected.1, roots[1].as_path());
    }
}
