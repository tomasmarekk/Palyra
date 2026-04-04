#![allow(clippy::result_large_err)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use serde_json::{json, Value};
use tokio_stream::StreamExt;
use tonic::{Code, Request, Status};
use tracing::warn;
use ulid::Ulid;

use crate::{
    delegation::{
        DelegationExecutionMode, DelegationMergeProvenanceRecord, DelegationMergeResult,
        DelegationMergeStrategy, DelegationSnapshot,
    },
    gateway::{
        proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
        GatewayAuthConfig, GatewayRuntimeState, HEADER_CHANNEL, HEADER_DEVICE_ID, HEADER_PRINCIPAL,
    },
    journal::{
        OrchestratorBackgroundTaskRecord, OrchestratorBackgroundTaskUpdateRequest,
        OrchestratorRunMetadataUpdateRequest, OrchestratorTapeAppendRequest,
    },
};

const BACKGROUND_QUEUE_IDLE_SLEEP: Duration = Duration::from_secs(3);
const DEFAULT_BACKGROUND_CHANNEL: &str = "console:background";

pub(crate) fn spawn_background_queue_loop(
    runtime: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(error) = poll_background_queue(&runtime, &auth, grpc_url.as_str()).await {
                warn!(status_code = ?error.code(), status_message = %error.message(), "background queue poll failed");
            }
            tokio::time::sleep(BACKGROUND_QUEUE_IDLE_SLEEP).await;
        }
    })
}

async fn poll_background_queue(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
) -> Result<(), Status> {
    let tasks = runtime
        .list_orchestrator_background_tasks(crate::journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: None,
            device_id: None,
            channel: None,
            session_id: None,
            include_completed: false,
            limit: 32,
        })
        .await?;
    for task in tasks.iter() {
        if let Err(error) =
            process_background_task(runtime, auth, grpc_url, task, tasks.as_slice()).await
        {
            warn!(
                task_id = %task.task_id,
                status_code = ?error.code(),
                status_message = %error.message(),
                "background task processing failed"
            );
            let _ = runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.task_id.clone(),
                    state: Some("failed".to_owned()),
                    target_run_id: None,
                    increment_attempt_count: false,
                    last_error: Some(Some(error.message().to_owned())),
                    result_json: Some(Some(
                        json!({
                            "status": "failed",
                            "task_id": task.task_id,
                            "error": error.message(),
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                })
                .await;
        }
    }
    Ok(())
}

async fn process_background_task(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
    task: &OrchestratorBackgroundTaskRecord,
    all_tasks: &[OrchestratorBackgroundTaskRecord],
) -> Result<(), Status> {
    if is_terminal_task_state(task.state.as_str()) || task.state == "paused" {
        return Ok(());
    }

    let now = crate::gateway::current_unix_ms();
    if let Some(expires_at_unix_ms) = task.expires_at_unix_ms {
        if expires_at_unix_ms <= now {
            runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.task_id.clone(),
                    state: Some("expired".to_owned()),
                    target_run_id: None,
                    increment_attempt_count: false,
                    last_error: Some(Some("background task expired before dispatch".to_owned())),
                    result_json: Some(Some(
                        json!({
                            "status": "expired",
                            "task_id": task.task_id,
                            "expired_at_unix_ms": expires_at_unix_ms,
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(now)),
                })
                .await?;
            return Ok(());
        }
    }
    if task.not_before_unix_ms.is_some_and(|not_before| not_before > now) {
        return Ok(());
    }
    if sync_parent_run_cancellation(runtime, task).await? {
        return Ok(());
    }
    if task.state == "cancel_requested" {
        if let Some(target_run_id) = task.target_run_id.as_deref() {
            let snapshot =
                runtime.orchestrator_run_status_snapshot(target_run_id.to_owned()).await?;
            if snapshot
                .as_ref()
                .map(|run| is_terminal_run_state(run.state.as_str()))
                .unwrap_or(true)
            {
                finalize_task_from_run(runtime, task, snapshot.as_ref(), "cancelled").await?;
            }
        } else {
            runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.task_id.clone(),
                    state: Some("cancelled".to_owned()),
                    target_run_id: None,
                    increment_attempt_count: false,
                    last_error: Some(Some("cancelled before dispatch".to_owned())),
                    result_json: Some(Some(
                        json!({
                            "status": "cancelled",
                            "task_id": task.task_id,
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(now)),
                })
                .await?;
        }
        return Ok(());
    }
    if task.state == "running" {
        if let Some(target_run_id) = task.target_run_id.as_deref() {
            let snapshot =
                runtime.orchestrator_run_status_snapshot(target_run_id.to_owned()).await?;
            if let Some(run) = snapshot.as_ref() {
                if is_terminal_run_state(run.state.as_str()) {
                    finalize_task_from_run(runtime, task, Some(run), run.state.as_str()).await?;
                }
                return Ok(());
            }
        }
    }
    if task.max_attempts > 0 && task.attempt_count >= task.max_attempts {
        runtime
            .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                task_id: task.task_id.clone(),
                state: Some("failed".to_owned()),
                target_run_id: None,
                increment_attempt_count: false,
                last_error: Some(Some("background task exhausted retry budget".to_owned())),
                result_json: Some(Some(
                    json!({
                        "status": "failed",
                        "task_id": task.task_id,
                        "attempt_count": task.attempt_count,
                        "max_attempts": task.max_attempts,
                    })
                    .to_string(),
                )),
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(Some(now)),
            })
            .await?;
        return Ok(());
    }

    if task_is_blocked_by_serial_sibling(all_tasks, task) {
        return Ok(());
    }

    dispatch_background_task(runtime, auth, grpc_url, task).await
}

async fn dispatch_background_task(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<(), Status> {
    let run_id = Ulid::new().to_string();
    let started_at_unix_ms = crate::gateway::current_unix_ms();
    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: Some("running".to_owned()),
            target_run_id: Some(Some(run_id.clone())),
            increment_attempt_count: true,
            last_error: Some(None),
            result_json: Some(None),
            started_at_unix_ms: Some(Some(started_at_unix_ms)),
            completed_at_unix_ms: Some(None),
        })
        .await?;
    let runtime = Arc::clone(runtime);
    let auth = auth.clone();
    let grpc_url = grpc_url.to_owned();
    let task = task.clone();
    tokio::spawn(async move {
        if let Err(error) =
            run_background_task_stream(&runtime, &auth, grpc_url.as_str(), &task, run_id.as_str())
                .await
        {
            warn!(
                task_id = %task.task_id,
                run_id = %run_id,
                status_code = ?error.code(),
                status_message = %error.message(),
                "background task stream failed"
            );
            let _ = runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.task_id.clone(),
                    state: Some("failed".to_owned()),
                    target_run_id: Some(None),
                    increment_attempt_count: false,
                    last_error: Some(Some(error.message().to_owned())),
                    result_json: Some(Some(
                        json!({
                            "status": "failed",
                            "task_id": task.task_id,
                            "run_id": run_id,
                            "error": error.message(),
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                })
                .await;
        }
    });
    Ok(())
}

async fn run_background_task_stream(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
    task: &OrchestratorBackgroundTaskRecord,
    run_id: &str,
) -> Result<(), Status> {
    let mut client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(grpc_url.to_owned())
            .await
            .map_err(|error| {
                Status::unavailable(format!("failed to connect background queue gateway: {error}"))
            })?;
    let prompt_text = task
        .input_text
        .clone()
        .unwrap_or_else(|| format!("Background task {} ({})", task.task_id, task.task_kind));
    let origin_kind = if task.delegation.is_some() { "delegation" } else { "background" };
    let mut run_request = Request::new(tokio_stream::iter(vec![common_v1::RunStreamRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: task.session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: run_id.to_owned() }),
        input: Some(common_v1::MessageEnvelope {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            envelope_id: Some(common_v1::CanonicalId { ulid: Ulid::new().to_string() }),
            timestamp_unix_ms: crate::gateway::current_unix_ms(),
            origin: Some(common_v1::EnvelopeOrigin {
                r#type: common_v1::envelope_origin::OriginType::System as i32,
                channel: task
                    .channel
                    .clone()
                    .unwrap_or_else(|| DEFAULT_BACKGROUND_CHANNEL.to_owned()),
                conversation_id: task.session_id.clone(),
                sender_display: "palyra-background".to_owned(),
                sender_handle: "background".to_owned(),
                sender_verified: true,
            }),
            content: Some(common_v1::MessageContent { text: prompt_text, attachments: Vec::new() }),
            security: None,
            max_payload_bytes: 0,
        }),
        allow_sensitive_tools: false,
        session_key: String::new(),
        session_label: String::new(),
        reset_session: false,
        require_existing: true,
        tool_approval_response: None,
        origin_kind: origin_kind.to_owned(),
        origin_run_id: task
            .parent_run_id
            .as_ref()
            .map(|ulid| common_v1::CanonicalId { ulid: ulid.clone() }),
        parameter_delta_json: build_parameter_delta_bytes(task)?,
        queued_input_id: task
            .queued_input_id
            .as_ref()
            .map(|ulid| common_v1::CanonicalId { ulid: ulid.clone() }),
    }]));
    inject_background_metadata(
        run_request.metadata_mut(),
        auth,
        task.owner_principal.as_str(),
        task.device_id.as_str(),
        task.channel.as_deref(),
    )?;

    let mut stream = client
        .run_stream(run_request)
        .await
        .map_err(|error| Status::internal(format!("background RunStream failed: {error}")))?
        .into_inner();

    append_parent_spawned_event(runtime, task, run_id).await?;

    let mut stream_error = None::<String>;
    while let Some(event) = stream.next().await {
        if let Err(error) = event {
            stream_error = Some(format!("background run stream read failed: {error}"));
            break;
        }
    }

    let run_snapshot = runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?;
    if let Some(run) = run_snapshot.as_ref() {
        let run_with_merge = if let Some(delegation) = task.delegation.as_ref() {
            let merge_result = build_merge_result(runtime, run, delegation).await?;
            runtime
                .update_orchestrator_run_metadata(OrchestratorRunMetadataUpdateRequest {
                    run_id: run_id.to_owned(),
                    parent_run_id: Some(task.parent_run_id.clone()),
                    delegation: Some(Some(delegation.clone())),
                    merge_result: Some(Some(merge_result.clone())),
                })
                .await?;
            let refreshed = runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?;
            append_parent_merge_event(runtime, task, run, &merge_result).await?;
            refreshed.unwrap_or_else(|| run.clone())
        } else {
            run.clone()
        };
        finalize_task_from_run(runtime, task, Some(&run_with_merge), run_with_merge.state.as_str())
            .await?;
        if let Some(error_message) = stream_error {
            warn!(
                task_id = %task.task_id,
                run_id = %run_id,
                status = %run_with_merge.state,
                error = %error_message,
                "background run stream ended with a transport error after persistence"
            );
        }
        return Ok(());
    }

    if let Some(error_message) = stream_error {
        return Err(Status::internal(error_message));
    }

    Err(Status::internal(format!("background run {run_id} finished without a persisted snapshot")))
}

async fn sync_parent_run_cancellation(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<bool, Status> {
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(false);
    };
    let Some(parent_run) =
        runtime.orchestrator_run_status_snapshot(parent_run_id.to_owned()).await?
    else {
        return Ok(false);
    };
    if !parent_run.cancel_requested && parent_run.state != "cancelled" {
        return Ok(false);
    }

    let cancellation_reason = "cancelled because the parent run was cancelled".to_owned();
    if let Some(target_run_id) = task.target_run_id.as_ref() {
        let child_run = runtime.orchestrator_run_status_snapshot(target_run_id.clone()).await?;
        if child_run.as_ref().is_some_and(|snapshot| is_terminal_run_state(snapshot.state.as_str()))
        {
            finalize_task_from_run(
                runtime,
                task,
                child_run.as_ref(),
                child_run.as_ref().map(|snapshot| snapshot.state.as_str()).unwrap_or("cancelled"),
            )
            .await?;
            return Ok(true);
        }
        runtime
            .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                task_id: task.task_id.clone(),
                state: Some("cancel_requested".to_owned()),
                target_run_id: None,
                increment_attempt_count: false,
                last_error: Some(Some(cancellation_reason.clone())),
                result_json: None,
                started_at_unix_ms: None,
                completed_at_unix_ms: None,
            })
            .await?;
        runtime
            .request_orchestrator_cancel(crate::journal::OrchestratorCancelRequest {
                run_id: target_run_id.clone(),
                reason: "delegated_parent_cancelled".to_owned(),
            })
            .await?;
        return Ok(true);
    }

    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: Some("cancelled".to_owned()),
            target_run_id: Some(None),
            increment_attempt_count: false,
            last_error: Some(Some(cancellation_reason.clone())),
            result_json: Some(Some(
                json!({
                    "status": "cancelled",
                    "task_id": task.task_id,
                    "reason": cancellation_reason,
                    "parent_run_id": parent_run_id,
                })
                .to_string(),
            )),
            started_at_unix_ms: None,
            completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
        })
        .await?;
    Ok(true)
}

fn task_is_blocked_by_serial_sibling(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    task: &OrchestratorBackgroundTaskRecord,
) -> bool {
    let Some(group_id) = delegation_serial_group(task) else {
        return false;
    };
    all_tasks.iter().any(|candidate| {
        candidate.task_id != task.task_id
            && delegation_serial_group(candidate).is_some_and(|candidate_group| {
                candidate_group == group_id && serial_sibling_blocks(candidate, task)
            })
    })
}

fn delegation_serial_group(task: &OrchestratorBackgroundTaskRecord) -> Option<&str> {
    let delegation = task.delegation.as_ref()?;
    (delegation.execution_mode == DelegationExecutionMode::Serial)
        .then_some(delegation.group_id.as_str())
}

fn serial_sibling_blocks(
    sibling: &OrchestratorBackgroundTaskRecord,
    current: &OrchestratorBackgroundTaskRecord,
) -> bool {
    if is_terminal_task_state(sibling.state.as_str()) || sibling.state == "failed" {
        return false;
    }
    match sibling.state.as_str() {
        "running" => true,
        "cancel_requested" => sibling.target_run_id.is_some(),
        "queued" | "paused" => task_precedes_in_serial_group(sibling, current),
        _ => false,
    }
}

fn task_precedes_in_serial_group(
    sibling: &OrchestratorBackgroundTaskRecord,
    current: &OrchestratorBackgroundTaskRecord,
) -> bool {
    sibling.created_at_unix_ms < current.created_at_unix_ms
        || (sibling.created_at_unix_ms == current.created_at_unix_ms
            && sibling.task_id < current.task_id)
}

async fn finalize_task_from_run(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    run: Option<&crate::journal::OrchestratorRunStatusSnapshot>,
    fallback_state: &str,
) -> Result<(), Status> {
    let normalized_state = match run.map(|value| value.state.as_str()).unwrap_or(fallback_state) {
        "done" => "succeeded",
        "cancelled" => "cancelled",
        "failed" => "failed",
        "running" | "accepted" | "in_progress" => "running",
        other => other,
    };
    if normalized_state == "running" {
        return Ok(());
    }
    let completed_at_unix_ms = run
        .and_then(|value| value.completed_at_unix_ms)
        .unwrap_or_else(crate::gateway::current_unix_ms);
    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: Some(normalized_state.to_owned()),
            target_run_id: None,
            increment_attempt_count: false,
            last_error: Some(run.and_then(|value| value.last_error.clone())),
            result_json: Some(Some(
                json!({
                    "status": normalized_state,
                    "task_id": task.task_id,
                    "run": run.map(run_status_to_json).unwrap_or_else(|| json!({
                        "state": fallback_state,
                    })),
                })
                .to_string(),
            )),
            started_at_unix_ms: None,
            completed_at_unix_ms: Some(Some(completed_at_unix_ms)),
        })
        .await
}

fn extract_parameter_delta_bytes(payload_json: Option<&str>) -> Result<Vec<u8>, Status> {
    let Some(payload_json) = payload_json else {
        return Ok(Vec::new());
    };
    if payload_json.trim().is_empty() {
        return Ok(Vec::new());
    }
    let payload = serde_json::from_str::<Value>(payload_json).map_err(|error| {
        Status::invalid_argument(format!("invalid background payload_json: {error}"))
    })?;
    let Some(parameter_delta) = payload.get("parameter_delta") else {
        return Ok(Vec::new());
    };
    serde_json::to_vec(parameter_delta).map_err(|error| {
        Status::internal(format!("failed to encode background parameter_delta: {error}"))
    })
}

fn build_parameter_delta_bytes(task: &OrchestratorBackgroundTaskRecord) -> Result<Vec<u8>, Status> {
    let mut merged = match extract_parameter_delta_value(task.payload_json.as_deref())? {
        Some(Value::Object(object)) => Value::Object(object),
        Some(other) => json!({ "prior_parameter_delta": other }),
        None => json!({}),
    };
    if let Some(root) = merged.as_object_mut() {
        root.insert(
            "background_task".to_owned(),
            json!({
                "task_id": task.task_id,
                "task_kind": task.task_kind,
                "parent_run_id": task.parent_run_id,
            }),
        );
        if let Some(delegation) = task.delegation.as_ref() {
            root.insert(
                "delegation".to_owned(),
                serde_json::to_value(delegation).map_err(|error| {
                    Status::internal(format!(
                        "failed to encode background delegation parameter_delta: {error}"
                    ))
                })?,
            );
        }
    }
    serde_json::to_vec(&merged).map_err(|error| {
        Status::internal(format!("failed to encode background parameter_delta bytes: {error}"))
    })
}

fn extract_parameter_delta_value(payload_json: Option<&str>) -> Result<Option<Value>, Status> {
    let bytes = extract_parameter_delta_bytes(payload_json)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(bytes.as_slice()).map(Some).map_err(|error| {
        Status::internal(format!("failed to parse background parameter_delta value: {error}"))
    })
}

async fn build_merge_result(
    runtime: &Arc<GatewayRuntimeState>,
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    delegation: &DelegationSnapshot,
) -> Result<DelegationMergeResult, Status> {
    let tape_events = load_run_tape(runtime, run.run_id.as_str()).await?;
    let mut proposals = HashMap::<String, (String, bool)>::new();
    let mut model_output = String::new();
    let mut warnings = Vec::new();
    let mut provenance = Vec::new();

    for event in tape_events {
        let payload =
            serde_json::from_str::<Value>(event.payload_json.as_str()).unwrap_or(Value::Null);
        match event.event_type.as_str() {
            "tool_proposal" => {
                let Some(proposal_id) = payload.get("proposal_id").and_then(Value::as_str) else {
                    continue;
                };
                let tool_name =
                    payload.get("tool_name").and_then(Value::as_str).unwrap_or("unknown_tool");
                let approval_required =
                    payload.get("approval_required").and_then(Value::as_bool).unwrap_or(false);
                proposals.insert(proposal_id.to_owned(), (tool_name.to_owned(), approval_required));
            }
            "model_token" => {
                if let Some(token) = payload.get("token").and_then(Value::as_str) {
                    model_output.push_str(token);
                }
            }
            "tool_result" => {
                let proposal_id = payload
                    .get("proposal_id")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown-proposal");
                let (tool_name, approval_required) = proposals
                    .get(proposal_id)
                    .cloned()
                    .unwrap_or_else(|| ("unknown_tool".to_owned(), false));
                let excerpt = payload
                    .get("output_json")
                    .map(value_excerpt)
                    .filter(|value| !value.is_empty())
                    .or_else(|| {
                        payload.get("error").and_then(Value::as_str).map(ToString::to_string)
                    })
                    .unwrap_or_else(|| "tool completed without a structured payload".to_owned());
                provenance.push(DelegationMergeProvenanceRecord {
                    child_run_id: run.run_id.clone(),
                    kind: "tool_result".to_owned(),
                    label: tool_name.clone(),
                    excerpt: truncate_excerpt(excerpt.as_str(), 240),
                    tool_name: Some(tool_name),
                    requires_approval: approval_required,
                });
            }
            _ => {}
        }
    }

    if model_output.trim().is_empty() {
        warnings.push("child run finished without model output tokens".to_owned());
    } else {
        provenance.insert(
            0,
            DelegationMergeProvenanceRecord {
                child_run_id: run.run_id.clone(),
                kind: "model_summary".to_owned(),
                label: "Model output".to_owned(),
                excerpt: truncate_excerpt(model_output.trim(), 320),
                tool_name: None,
                requires_approval: delegation.merge_contract.approval_required,
            },
        );
    }
    if run.state == "failed" {
        warnings.push(
            run.last_error.clone().unwrap_or_else(|| "child run failed before merge".to_owned()),
        );
    } else if run.state == "cancelled" {
        warnings.push("child run was cancelled before merge".to_owned());
    }

    let summary_text = build_merge_summary(
        delegation.merge_contract.strategy,
        run,
        model_output.trim(),
        provenance.as_slice(),
        warnings.as_slice(),
    );
    Ok(DelegationMergeResult {
        status: run.state.clone(),
        strategy: delegation.merge_contract.strategy,
        summary_text,
        warnings,
        approval_required: delegation.merge_contract.approval_required,
        provenance,
        merged_at_unix_ms: Some(crate::gateway::current_unix_ms()),
    })
}

async fn load_run_tape(
    runtime: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> Result<Vec<crate::journal::OrchestratorTapeRecord>, Status> {
    let mut after_seq = None;
    let mut events = Vec::new();
    for _ in 0..16 {
        let page =
            runtime.orchestrator_tape_snapshot(run_id.to_owned(), after_seq, Some(128)).await?;
        after_seq = page.next_after_seq;
        events.extend(page.events);
        if after_seq.is_none() {
            break;
        }
    }
    Ok(events)
}

fn build_merge_summary(
    strategy: DelegationMergeStrategy,
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    model_output: &str,
    provenance: &[DelegationMergeProvenanceRecord],
    warnings: &[String],
) -> String {
    let base_summary = if model_output.is_empty() {
        format!("Child run {} completed with state '{}'.", run.run_id, run.state)
    } else {
        truncate_excerpt(model_output, 600)
    };
    match strategy {
        DelegationMergeStrategy::Summarize => base_summary,
        DelegationMergeStrategy::Compare => {
            format!("{} Sources captured: {}.", base_summary, provenance.len())
        }
        DelegationMergeStrategy::PatchReview => format!(
            "{} Patch-oriented evidence entries: {}.",
            base_summary,
            provenance
                .iter()
                .filter(|record| record.tool_name.as_deref() == Some("palyra.fs.apply_patch"))
                .count()
        ),
        DelegationMergeStrategy::Triage => {
            if warnings.is_empty() {
                format!("{} No merge warnings were raised.", base_summary)
            } else {
                format!("{} Warnings: {}.", base_summary, warnings.join(" | "))
            }
        }
    }
}

async fn append_parent_spawned_event(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    child_run_id: &str,
) -> Result<(), Status> {
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(());
    };
    append_parent_tape_event(
        runtime,
        parent_run_id,
        "child_run_spawned",
        json!({
            "task_id": task.task_id,
            "child_run_id": child_run_id,
            "session_id": task.session_id,
            "delegation": task.delegation,
        }),
    )
    .await
}

async fn append_parent_merge_event(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    merge_result: &DelegationMergeResult,
) -> Result<(), Status> {
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(());
    };
    let event_type = match run.state.as_str() {
        "done" => "child_run_merged",
        "failed" => "child_run_failed",
        "cancelled" => "child_run_cancelled",
        _ => "child_run_merged",
    };
    append_parent_tape_event(
        runtime,
        parent_run_id,
        event_type,
        json!({
            "task_id": task.task_id,
            "child_run_id": run.run_id,
            "child_state": run.state,
            "merge_result": merge_result,
        }),
    )
    .await
}

async fn append_parent_tape_event(
    runtime: &Arc<GatewayRuntimeState>,
    parent_run_id: &str,
    event_type: &str,
    payload: Value,
) -> Result<(), Status> {
    for _ in 0..3 {
        let Some(run) = runtime.orchestrator_run_status_snapshot(parent_run_id.to_owned()).await?
        else {
            return Ok(());
        };
        let seq = i64::try_from(run.tape_events).unwrap_or(i64::MAX);
        match runtime
            .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
                run_id: parent_run_id.to_owned(),
                seq,
                event_type: event_type.to_owned(),
                payload_json: payload.to_string(),
            })
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) if error.code() == Code::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(Status::aborted(format!("failed to append parent tape event '{event_type}' after retries")))
}

fn value_excerpt(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

fn truncate_excerpt(value: &str, max_chars: usize) -> String {
    let mut excerpt = value.trim().chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        excerpt.push_str("...");
    }
    excerpt
}

fn inject_background_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    auth: &GatewayAuthConfig,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
) -> Result<(), Status> {
    if auth.require_auth {
        let token = auth.admin_token.as_ref().ok_or_else(|| {
            Status::permission_denied("admin token is required for background queue auth")
        })?;
        metadata.insert(
            "authorization",
            format!("Bearer {token}").parse().map_err(|_| {
                Status::internal("failed to encode background queue authorization metadata")
            })?,
        );
    }
    metadata.insert(
        HEADER_PRINCIPAL,
        principal
            .parse()
            .map_err(|_| Status::invalid_argument("background principal metadata is invalid"))?,
    );
    metadata.insert(
        HEADER_DEVICE_ID,
        device_id
            .parse()
            .map_err(|_| Status::invalid_argument("background device_id metadata is invalid"))?,
    );
    let header_channel =
        channel.filter(|value| !value.trim().is_empty()).unwrap_or(DEFAULT_BACKGROUND_CHANNEL);
    metadata.insert(
        HEADER_CHANNEL,
        header_channel
            .parse()
            .map_err(|_| Status::invalid_argument("background channel metadata is invalid"))?,
    );
    Ok(())
}

fn run_status_to_json(run: &crate::journal::OrchestratorRunStatusSnapshot) -> Value {
    json!({
        "run_id": run.run_id,
        "session_id": run.session_id,
        "state": run.state,
        "cancel_requested": run.cancel_requested,
        "cancel_reason": run.cancel_reason,
        "prompt_tokens": run.prompt_tokens,
        "completion_tokens": run.completion_tokens,
        "total_tokens": run.total_tokens,
        "origin_kind": run.origin_kind,
        "origin_run_id": run.origin_run_id,
        "parent_run_id": run.parent_run_id,
        "delegation": run.delegation,
        "merge_result": run.merge_result,
        "updated_at_unix_ms": run.updated_at_unix_ms,
        "completed_at_unix_ms": run.completed_at_unix_ms,
        "last_error": run.last_error,
    })
}

fn is_terminal_task_state(state: &str) -> bool {
    matches!(state, "succeeded" | "failed" | "cancelled" | "expired")
}

fn is_terminal_run_state(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled")
}
