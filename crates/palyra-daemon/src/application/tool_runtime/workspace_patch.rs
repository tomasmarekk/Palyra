use std::{path::PathBuf, sync::Arc};

use palyra_common::workspace_patch::{
    apply_workspace_patch, compute_patch_sha256, redact_patch_preview, WorkspacePatchLimits,
    WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::{error, warn};
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    application::workspace_observability::{
        capture_workspace_patch_checkpoint, WorkspacePatchCheckpointCapture,
    },
    gateway::{
        current_unix_ms, record_agent_journal_event, GatewayRuntimeState,
        MAX_PATCH_TOOL_MARKER_BYTES, MAX_PATCH_TOOL_PATTERN_BYTES,
        MAX_PATCH_TOOL_REDACTION_PATTERNS, MAX_PATCH_TOOL_SECRET_FILE_MARKERS,
        MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES,
    },
    tool_protocol::{ToolAttestation, ToolExecutionOutcome},
    transport::grpc::auth::RequestContext,
};

pub(crate) async fn execute_workspace_patch_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
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
    let request = WorkspacePatchRequest {
        patch: patch.clone(),
        dry_run,
        redaction_policy: redaction_policy.clone(),
    };

    match apply_workspace_patch(workspace_roots.as_slice(), &request, &limits) {
        Ok(outcome) => {
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

            if !dry_run {
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
                        workspace_roots: workspace_roots.as_slice(),
                        files_touched: outcome.files_touched.as_slice(),
                    },
                )
                .await
                {
                    Ok(Some(checkpoint)) => {
                        if let Value::Object(payload) = &mut output_value {
                            payload.insert(
                                "workspace_checkpoint".to_owned(),
                                json!({
                                    "checkpoint_id": checkpoint.checkpoint_id,
                                    "session_id": checkpoint.session_id,
                                    "run_id": checkpoint.run_id,
                                    "summary_text": checkpoint.summary_text,
                                    "source_kind": checkpoint.source_kind,
                                    "source_label": checkpoint.source_label,
                                    "tool_name": checkpoint.tool_name,
                                    "device_id": checkpoint.device_id,
                                    "channel": checkpoint.channel,
                                    "created_at_unix_ms": checkpoint.created_at_unix_ms,
                                    "diff_summary": serde_json::from_str::<Value>(
                                        checkpoint.diff_summary_json.as_str()
                                    )
                                    .unwrap_or_else(|_| {
                                        Value::String(checkpoint.diff_summary_json.clone())
                                    }),
                                }),
                            );
                        }
                        let _ = record_agent_journal_event(
                            runtime_state,
                            &RequestContext {
                                principal: principal.to_owned(),
                                device_id: device_id.to_owned(),
                                channel: channel.map(str::to_owned),
                            },
                            json!({
                                "event": "workspace.checkpoint.created",
                                "checkpoint_id": checkpoint.checkpoint_id,
                                "session_id": checkpoint.session_id,
                                "run_id": checkpoint.run_id,
                                "source_kind": checkpoint.source_kind,
                                "source_label": checkpoint.source_label,
                                "tool_name": checkpoint.tool_name,
                                "proposal_id": checkpoint.proposal_id,
                                "actor_principal": checkpoint.actor_principal,
                                "device_id": checkpoint.device_id,
                                "channel": checkpoint.channel,
                                "summary_text": checkpoint.summary_text,
                                "diff_summary": serde_json::from_str::<Value>(
                                    checkpoint.diff_summary_json.as_str(),
                                )
                                .unwrap_or_else(|_| {
                                    Value::String(checkpoint.diff_summary_json.clone())
                                }),
                            }),
                        )
                        .await;
                    }
                    Ok(None) => {}
                    Err(status) => {
                        error!(
                            proposal_id = %proposal_id,
                            session_id = %session_id,
                            run_id = %run_id,
                            error = %status,
                            "workspace checkpoint capture failed after patch apply"
                        );
                        if let Value::Object(payload) = &mut output_value {
                            payload.insert(
                                "workspace_checkpoint_error".to_owned(),
                                Value::String(status.message().to_owned()),
                            );
                        }
                    }
                }
            }

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
        Err(error) => {
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
                "patch_sha256": compute_patch_sha256(patch.as_str()),
                "dry_run": dry_run,
                "files_touched": [],
                "rollback_performed": error.rollback_performed(),
                "redacted_preview": redact_patch_preview(
                    patch.as_str(),
                    &redaction_policy,
                    limits.max_preview_bytes
                ),
                "parse_error": error
                    .parse_location()
                    .map(|(line, column)| json!({ "line": line, "column": column })),
                "error": error.to_string(),
            });
            let output_json =
                serde_json::to_vec(&failure_payload).unwrap_or_else(|_| b"{}".to_vec());
            workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                output_json,
                format!("palyra.fs.apply_patch failed: {error}"),
            )
        }
    }
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
