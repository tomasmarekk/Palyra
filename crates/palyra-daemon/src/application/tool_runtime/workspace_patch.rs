use std::{path::PathBuf, sync::Arc, time::Instant};

use palyra_common::workspace_patch::{
    apply_workspace_patch, compute_patch_sha256, redact_patch_preview, WorkspacePatchError,
    WorkspacePatchFileAttestation, WorkspacePatchLimits, WorkspacePatchOutcome,
    WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::{error, warn};
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    application::workspace_observability::{
        capture_workspace_patch_checkpoint, compare_workspace_anchors, WorkspaceCompareAnchor,
        WorkspacePatchCheckpointCapture, WorkspacePatchCheckpointStage,
    },
    gateway::{
        current_unix_ms, record_agent_journal_event, GatewayRuntimeState,
        MAX_PATCH_TOOL_MARKER_BYTES, MAX_PATCH_TOOL_PATTERN_BYTES,
        MAX_PATCH_TOOL_REDACTION_PATTERNS, MAX_PATCH_TOOL_SECRET_FILE_MARKERS,
        MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES,
    },
    journal::{WorkspaceCheckpointPairLinkRequest, WorkspaceCheckpointRecord},
    tool_protocol::{ToolAttestation, ToolExecutionOutcome},
    transport::grpc::auth::RequestContext,
};

pub(crate) struct WorkspacePatchToolRequest<'a> {
    pub(crate) principal: &'a str,
    pub(crate) device_id: &'a str,
    pub(crate) channel: Option<&'a str>,
    pub(crate) session_id: &'a str,
    pub(crate) run_id: &'a str,
    pub(crate) proposal_id: &'a str,
    pub(crate) input_json: &'a [u8],
}

impl<'a> WorkspacePatchToolRequest<'a> {
    pub(crate) fn from_runtime_context(
        context: crate::gateway::ToolRuntimeExecutionContext<'a>,
        proposal_id: &'a str,
        input_json: &'a [u8],
    ) -> Self {
        Self {
            principal: context.principal,
            device_id: context.device_id,
            channel: context.channel,
            session_id: context.session_id,
            run_id: context.run_id,
            proposal_id,
            input_json,
        }
    }
}

pub(crate) async fn execute_workspace_patch_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: WorkspacePatchToolRequest<'_>,
) -> ToolExecutionOutcome {
    let WorkspacePatchToolRequest {
        principal,
        device_id,
        channel,
        session_id,
        run_id,
        proposal_id,
        input_json,
    } = request;
    if input_json.len() > MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES {
        return workspace_patch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!(
                "palyra.fs.apply_patch input exceeds {MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES} bytes"
            ),
        );
    }

    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.fs.apply_patch invalid JSON input: {error}"),
            );
        }
    };

    let patch = match parsed.get("patch").and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => value.to_owned(),
        _ => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch requires non-empty string field 'patch'".to_owned(),
            );
        }
    };

    let dry_run = match parsed.get("dry_run") {
        Some(Value::Bool(value)) => *value,
        Some(_) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch dry_run must be a boolean".to_owned(),
            );
        }
        None => false,
    };

    let mut redaction_policy = WorkspacePatchRedactionPolicy::default();
    match parse_patch_string_array_field(
        &parsed,
        "redaction_patterns",
        MAX_PATCH_TOOL_REDACTION_PATTERNS,
        MAX_PATCH_TOOL_PATTERN_BYTES,
    ) {
        Ok(Some(patterns)) => {
            extend_patch_string_defaults(&mut redaction_policy.redaction_patterns, patterns);
        }
        Ok(None) => {}
        Err(message) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                message,
            );
        }
    }
    match parse_patch_string_array_field(
        &parsed,
        "secret_file_markers",
        MAX_PATCH_TOOL_SECRET_FILE_MARKERS,
        MAX_PATCH_TOOL_MARKER_BYTES,
    ) {
        Ok(Some(markers)) => {
            extend_patch_string_defaults(&mut redaction_policy.secret_file_markers, markers);
        }
        Ok(None) => {}
        Err(message) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                message,
            );
        }
    }

    let agent_outcome = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: principal.to_owned(),
            channel: channel.map(str::to_owned),
            session_id: Some(session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "palyra.fs.apply_patch failed to resolve agent workspace: {}",
                    error.message()
                ),
            );
        }
    };
    let workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let limits = WorkspacePatchLimits::default();
    let planning_request = WorkspacePatchRequest {
        patch: patch.clone(),
        dry_run: true,
        redaction_policy: redaction_policy.clone(),
    };

    let planned_outcome =
        match apply_workspace_patch(workspace_roots.as_slice(), &planning_request, &limits) {
            Ok(outcome) => outcome,
            Err(error) => {
                return workspace_patch_error_outcome(
                    proposal_id,
                    input_json,
                    dry_run,
                    patch.as_str(),
                    &redaction_policy,
                    &limits,
                    &error,
                );
            }
        };

    if dry_run {
        return serialize_workspace_patch_success(proposal_id, input_json, &planned_outcome);
    }

    let mutation_id = Ulid::new().to_string();
    let risk = assess_workspace_mutation_risk(planned_outcome.files_touched.as_slice());
    let mut preflight_checkpoint = None;
    let mut preflight_error = None;

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
            workspace_roots: workspace_roots.as_slice(),
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
                    mutation_id.as_str(),
                    &risk,
                    status.message(),
                );
            }
            preflight_error = Some(status.message().to_owned());
        }
    }

    let request = WorkspacePatchRequest {
        patch: patch.clone(),
        dry_run: false,
        redaction_policy: redaction_policy.clone(),
    };
    let outcome = match apply_workspace_patch(workspace_roots.as_slice(), &request, &limits) {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_patch_error_outcome(
                proposal_id,
                input_json,
                false,
                patch.as_str(),
                &redaction_policy,
                &limits,
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
            workspace_roots: workspace_roots.as_slice(),
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

    serialize_workspace_patch_success_value(proposal_id, input_json, output_value)
}

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

#[derive(Debug, Clone, Copy)]
struct WorkspaceMutationRisk {
    level: WorkspaceMutationRiskLevel,
    review_posture: &'static str,
    fail_closed_without_preflight: bool,
}

fn assess_workspace_mutation_risk(
    files_touched: &[WorkspacePatchFileAttestation],
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
            || is_high_risk_workspace_path(file.path.as_str())
            || file.moved_from.as_deref().is_some_and(is_high_risk_workspace_path)
        {
            level = WorkspaceMutationRiskLevel::High;
            break;
        }
        if is_medium_risk_workspace_path(file.path.as_str()) {
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

async fn record_workspace_checkpoint_created_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    checkpoint: &WorkspaceCheckpointRecord,
) {
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
            "compare_summary": parse_checkpoint_json_field(
                checkpoint.compare_summary_json.as_str()
            ),
        }),
    )
    .await;
}

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

async fn workspace_patch_pair_compare_summary(
    runtime_state: &Arc<GatewayRuntimeState>,
    preflight: &WorkspaceCheckpointRecord,
    post_change: &WorkspaceCheckpointRecord,
) -> Value {
    let started = Instant::now();
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

fn append_workspace_checkpoint_output(
    output_value: &mut Value,
    context: WorkspaceCheckpointOutputContext<'_>,
) {
    let Some(payload) = output_value.as_object_mut() else {
        return;
    };
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

fn parse_checkpoint_json_field(raw: &str) -> Value {
    serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}

fn serialize_workspace_patch_success(
    proposal_id: &str,
    input_json: &[u8],
    outcome: &WorkspacePatchOutcome,
) -> ToolExecutionOutcome {
    match serde_json::to_vec(outcome) {
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

fn workspace_patch_preflight_failure_outcome(
    proposal_id: &str,
    input_json: &[u8],
    planned_outcome: &WorkspacePatchOutcome,
    mutation_id: &str,
    risk: &WorkspaceMutationRisk,
    checkpoint_error: &str,
) -> ToolExecutionOutcome {
    let mut output_value = serde_json::to_value(planned_outcome).unwrap_or_else(|_| json!({}));
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

fn workspace_patch_error_outcome(
    proposal_id: &str,
    input_json: &[u8],
    dry_run: bool,
    patch: &str,
    redaction_policy: &WorkspacePatchRedactionPolicy,
    limits: &WorkspacePatchLimits,
    error: &WorkspacePatchError,
) -> ToolExecutionOutcome {
    if let Some((line, column)) = error.parse_location() {
        warn!(
            proposal_id = %proposal_id,
            line,
            column,
            error = %error,
            "workspace patch parse failed"
        );
    } else {
        warn!(
            proposal_id = %proposal_id,
            error = %error,
            "workspace patch execution failed"
        );
    }
    let failure_payload = json!({
        "patch_sha256": compute_patch_sha256(patch),
        "dry_run": dry_run,
        "files_touched": [],
        "rollback_performed": error.rollback_performed(),
        "redacted_preview": redact_patch_preview(
            patch,
            redaction_policy,
            limits.max_preview_bytes
        ),
        "parse_error": error
            .parse_location()
            .map(|(line, column)| json!({ "line": line, "column": column })),
        "error": error.to_string(),
    });
    let output_json = serde_json::to_vec(&failure_payload).unwrap_or_else(|_| b"{}".to_vec());
    workspace_patch_tool_execution_outcome(
        proposal_id,
        input_json,
        false,
        output_json,
        format!("palyra.fs.apply_patch failed: {error}"),
    )
}

pub(crate) fn extend_patch_string_defaults(defaults: &mut Vec<String>, additions: Vec<String>) {
    for addition in additions {
        if !defaults.iter().any(|existing| existing == &addition) {
            defaults.push(addition);
        }
    }
}

pub(crate) fn parse_patch_string_array_field(
    payload: &serde_json::Map<String, Value>,
    field_name: &str,
    max_items: usize,
    max_item_bytes: usize,
) -> Result<Option<Vec<String>>, String> {
    let Some(value) = payload.get(field_name) else {
        return Ok(None);
    };
    let Value::Array(values) = value else {
        return Err(format!("palyra.fs.apply_patch {field_name} must be an array of strings"));
    };
    if values.len() > max_items {
        return Err(format!("palyra.fs.apply_patch {field_name} exceeds limit ({max_items})"));
    }
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let Some(raw) = value.as_str() else {
            return Err(format!("palyra.fs.apply_patch {field_name} must be an array of strings"));
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.len() > max_item_bytes {
            return Err(format!(
                "palyra.fs.apply_patch {field_name} entries must be <= {max_item_bytes} bytes"
            ));
        }
        parsed.push(trimmed.to_owned());
    }
    Ok(Some(parsed))
}

fn workspace_patch_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.fs.apply_patch.attestation.v1");
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = hex::encode(hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "workspace_patch".to_owned(),
            sandbox_enforcement: "workspace_roots".to_owned(),
        },
    }
}
