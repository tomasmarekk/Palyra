use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use palyra_common::{
    redaction::{is_sensitive_key, redact_auth_error, redact_url_segments_in_text, REDACTED},
    runtime_contracts::{
        ArtifactRetentionPolicy, ToolResultSensitivity, ToolResultVisibility, ToolTurnBudget,
    },
};
use serde_json::{json, Map, Value};
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{interval, timeout, MissedTickBehavior},
};
use tonic::{Status, Streaming};
use tracing::{info, Instrument};
use ulid::Ulid;

use crate::{
    application::approvals::{
        approval_subject_type_for_tool, build_pending_tool_approval,
        record_approval_requested_journal_event, record_approval_resolved_journal_event,
        resolve_cached_tool_approval_for_proposal,
    },
    application::execution_gate::ToolProposalApprovalState,
    application::tool_registry::{
        normalization_audit_tape_payload, projection_policy_for_tool, rejection_tape_payload,
        tool_call_rejection_outcome, validate_tool_call_against_catalog_snapshot,
        ModelVisibleToolCatalogSnapshot, NormalizedToolCall, ToolArgumentNormalizationAudit,
        ToolCallRejection, ToolResultProjectionPolicy,
    },
    application::tool_runtime::artifacts::bounded_tool_result_artifact_content,
    application::tool_security::{
        approval_execution_context_for_backend_selection, evaluate_tool_proposal_security,
        record_tool_proposal_decision_audit_trail, resolve_tool_proposal_decision_for_context,
        ResolvedToolProposalDecision, ToolProposalBackendSelection, ToolProposalSecurityEvaluation,
    },
    gateway::{
        await_tool_approval_response, best_effort_mark_approval_error,
        build_and_ingest_tool_result_memory_summary,
        execute_tool_with_runtime_dispatch_with_cancellation,
        record_tool_execution_outcome_metrics, shared_tool_budget, shared_tool_budget_remaining,
        tool_cancellation_requires_execution_drain, GatewayRuntimeState,
        RunStreamToolExecutionOutcome, SharedToolBudget, ToolApprovalOutcome,
        ToolRuntimeExecutionContext, TOOL_APPROVAL_RESPONSE_TIMEOUT,
    },
    journal::{
        ApprovalCreateRequest, ApprovalResolveRequest, OrchestratorTapeAppendRequest,
        ToolResultArtifactCreateRequest,
    },
    orchestrator::RunStateMachine,
    tool_protocol::{denied_execution_outcome, ToolExecutionOutcome},
    transport::grpc::auth::RequestContext,
};

use super::{
    cancellation::transition_run_stream_to_cancelled,
    tape::{
        send_tool_approval_request_with_tape, send_tool_approval_response_with_tape,
        send_tool_attestation_with_tape, send_tool_decision_with_tape,
        send_tool_proposal_with_tape, send_tool_result_with_tape,
    },
};

const MAX_PARALLEL_TOOL_CALLS_PER_GROUP: usize = 4;
const TOOL_PARALLELISM_ENABLED_ENV: &str = "PALYRA_TOOL_PARALLELISM_ENABLED";

#[derive(Debug, Clone)]
pub(crate) struct RunStreamToolProposalPreparation {
    decision: crate::tool_protocol::ToolDecision,
    resolved_session_id: String,
    backend_selection: ToolProposalBackendSelection,
}

#[derive(Debug, Clone)]
pub(crate) struct RunStreamPreparedToolExecution {
    proposal_id: String,
    tool_name: String,
    input_json: Vec<u8>,
    decision: crate::tool_protocol::ToolDecision,
    resolved_session_id: String,
    backend_selection: ToolProposalBackendSelection,
}

#[derive(Debug, Clone)]
pub(crate) enum RunStreamToolProposalPreparationOutcome {
    Prepared(RunStreamPreparedToolExecution),
    Completed(RunStreamToolExecutionOutcome),
}

#[derive(Debug, Clone)]
pub(crate) enum RunStreamPreparedToolExecutionBatchOutcome {
    Completed(Vec<RunStreamToolExecutionOutcome>),
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolParallelism {
    Never,
    ReadOnlySafe,
    PathScoped,
    IdempotentNetwork,
}

impl ToolParallelism {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::ReadOnlySafe => "read_only_safe",
            Self::PathScoped => "path_scoped",
            Self::IdempotentNetwork => "idempotent_network",
        }
    }

    const fn is_parallel_safe(self) -> bool {
        !matches!(self, Self::Never)
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum ParallelToolExecutionTaskOutcome {
    Completed {
        order: usize,
        prepared: RunStreamPreparedToolExecution,
        outcome: ToolExecutionOutcome,
    },
    Cancelled,
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_run_stream_tool_proposal_event(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    stream: &mut Streaming<crate::transport::grpc::proto::palyra::common::v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    run_state: &mut RunStateMachine,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    remaining_tool_budget: &mut u32,
    allow_sensitive_tools: bool,
    approval_cache_generation: Option<u64>,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    match prepare_run_stream_tool_proposal_event(
        sender,
        stream,
        runtime_state,
        request_context,
        active_session_id,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
        tool_catalog_snapshot,
        remaining_tool_budget,
        allow_sensitive_tools,
        approval_cache_generation,
        tape_seq,
    )
    .await?
    {
        RunStreamToolProposalPreparationOutcome::Prepared(prepared) => {
            execute_prepared_run_stream_tool_proposal(
                sender,
                runtime_state,
                request_context,
                run_state,
                run_id,
                prepared,
                remaining_tool_budget,
                tape_seq,
            )
            .await
        }
        RunStreamToolProposalPreparationOutcome::Completed(outcome) => Ok(outcome),
    }
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_run_stream_tool_proposal_event(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    stream: &mut Streaming<crate::transport::grpc::proto::palyra::common::v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    remaining_tool_budget: &mut u32,
    allow_sensitive_tools: bool,
    approval_cache_generation: Option<u64>,
    tape_seq: &mut i64,
) -> Result<RunStreamToolProposalPreparationOutcome, Status> {
    let NormalizedToolCall { input_json: normalized_input_json, audit } =
        match validate_tool_call_against_catalog_snapshot(
            tool_catalog_snapshot,
            tool_name,
            input_json,
        ) {
            Ok(normalized) => normalized,
            Err(rejection) => {
                *remaining_tool_budget = (*remaining_tool_budget).saturating_sub(1);
                let outcome = reject_run_stream_tool_call(
                    sender,
                    runtime_state,
                    run_id,
                    proposal_id,
                    tool_name,
                    input_json,
                    rejection,
                    tape_seq,
                )
                .await?;
                return Ok(RunStreamToolProposalPreparationOutcome::Completed(outcome));
            }
        };
    if !audit.steps.is_empty() {
        append_tool_argument_normalization_tape_event(
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            tool_name,
            &audit,
        )
        .await?;
    }

    let RunStreamToolProposalPreparation { decision, resolved_session_id, backend_selection } =
        prepare_run_stream_tool_proposal_execution(
            sender,
            stream,
            runtime_state,
            request_context,
            active_session_id,
            session_id,
            run_id,
            proposal_id,
            tool_name,
            normalized_input_json.as_slice(),
            remaining_tool_budget,
            allow_sensitive_tools,
            approval_cache_generation,
            tape_seq,
        )
        .await?;

    Ok(RunStreamToolProposalPreparationOutcome::Prepared(RunStreamPreparedToolExecution {
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        input_json: normalized_input_json,
        decision,
        resolved_session_id,
        backend_selection,
    }))
}

#[allow(clippy::result_large_err)]
async fn append_tool_argument_normalization_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    audit: &ToolArgumentNormalizationAudit,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool.arguments.normalized".to_owned(),
            payload_json: normalization_audit_tape_payload(proposal_id, tool_name, audit),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn reject_run_stream_tool_call(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    rejection: ToolCallRejection,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    runtime_state.record_tool_proposal();
    send_tool_proposal_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        input_json,
        false,
    )
    .await?;
    let reason = format!("{}: {}", rejection.kind.as_str(), rejection.message);
    send_tool_decision_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        false,
        reason.as_str(),
        false,
        true,
    )
    .await?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool.intake_rejected".to_owned(),
            payload_json: rejection_tape_payload(proposal_id, &rejection),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);

    let execution_outcome = tool_call_rejection_outcome(proposal_id, input_json, &rejection);
    send_tool_result_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        false,
        execution_outcome.output_json.as_slice(),
        execution_outcome.error.as_str(),
    )
    .await?;
    send_tool_attestation_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        execution_outcome.attestation.attestation_id.as_str(),
        execution_outcome.attestation.execution_sha256.as_str(),
        execution_outcome.attestation.executed_at_unix_ms,
        execution_outcome.attestation.timed_out,
        execution_outcome.attestation.executor.as_str(),
        execution_outcome.attestation.sandbox_enforcement.as_str(),
    )
    .await?;
    runtime_state.record_tool_attestation_emitted();
    Ok(RunStreamToolExecutionOutcome::Completed {
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        outcome: execution_outcome,
    })
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_prepared_run_stream_tool_proposals_ordered(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_state: &mut RunStateMachine,
    run_id: &str,
    prepared_tools: Vec<RunStreamPreparedToolExecution>,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<RunStreamPreparedToolExecutionBatchOutcome, Status> {
    let mut completed = Vec::new();
    for group in split_parallel_tool_groups(prepared_tools) {
        if group.can_run_parallel && group.tools.len() > 1 && run_stream_tool_parallelism_enabled()
        {
            match execute_parallel_prepared_tool_group(
                sender,
                runtime_state,
                request_context,
                run_state,
                run_id,
                group.tools,
                remaining_tool_budget,
                tape_seq,
            )
            .await?
            {
                RunStreamPreparedToolExecutionBatchOutcome::Completed(mut outcomes) => {
                    completed.append(&mut outcomes);
                }
                RunStreamPreparedToolExecutionBatchOutcome::Cancelled => {
                    return Ok(RunStreamPreparedToolExecutionBatchOutcome::Cancelled);
                }
            }
        } else {
            for prepared in group.tools {
                match execute_prepared_run_stream_tool_proposal(
                    sender,
                    runtime_state,
                    request_context,
                    run_state,
                    run_id,
                    prepared,
                    remaining_tool_budget,
                    tape_seq,
                )
                .await?
                {
                    RunStreamToolExecutionOutcome::Completed {
                        proposal_id,
                        tool_name,
                        outcome,
                    } => {
                        completed.push(RunStreamToolExecutionOutcome::Completed {
                            proposal_id,
                            tool_name,
                            outcome,
                        });
                    }
                    RunStreamToolExecutionOutcome::Cancelled => {
                        return Ok(RunStreamPreparedToolExecutionBatchOutcome::Cancelled);
                    }
                }
            }
        }
    }

    Ok(RunStreamPreparedToolExecutionBatchOutcome::Completed(completed))
}

#[derive(Debug)]
struct PreparedToolExecutionGroup {
    can_run_parallel: bool,
    tools: Vec<RunStreamPreparedToolExecution>,
}

fn split_parallel_tool_groups(
    prepared_tools: Vec<RunStreamPreparedToolExecution>,
) -> Vec<PreparedToolExecutionGroup> {
    let mut groups = Vec::new();
    let mut parallel_tools = Vec::new();
    let mut parallel_path_scopes = Vec::<String>::new();

    for prepared in prepared_tools {
        let parallelism =
            classify_tool_parallelism(prepared.tool_name.as_str(), prepared.input_json.as_slice());
        let path_scope =
            path_scope_key(prepared.tool_name.as_str(), prepared.input_json.as_slice());
        let has_path_conflict = matches!(parallelism, ToolParallelism::PathScoped)
            && path_scope
                .as_ref()
                .map(|scope| parallel_path_scopes.iter().any(|existing| existing == scope))
                .unwrap_or(true);
        let can_run_parallel =
            prepared.decision.allowed && parallelism.is_parallel_safe() && !has_path_conflict;
        if can_run_parallel {
            if let Some(scope) = path_scope {
                parallel_path_scopes.push(scope);
            }
            parallel_tools.push(prepared);
            if parallel_tools.len() == MAX_PARALLEL_TOOL_CALLS_PER_GROUP {
                groups.push(PreparedToolExecutionGroup {
                    can_run_parallel: true,
                    tools: std::mem::take(&mut parallel_tools),
                });
                parallel_path_scopes.clear();
            }
            continue;
        }

        if !parallel_tools.is_empty() {
            groups.push(PreparedToolExecutionGroup {
                can_run_parallel: true,
                tools: std::mem::take(&mut parallel_tools),
            });
            parallel_path_scopes.clear();
        }
        groups.push(PreparedToolExecutionGroup { can_run_parallel: false, tools: vec![prepared] });
    }

    if !parallel_tools.is_empty() {
        groups.push(PreparedToolExecutionGroup { can_run_parallel: true, tools: parallel_tools });
    }

    groups
}

pub(crate) fn classify_tool_parallelism(tool_name: &str, input_json: &[u8]) -> ToolParallelism {
    match tool_name {
        "palyra.echo"
        | "palyra.sleep"
        | "palyra.memory.status"
        | "palyra.memory.search"
        | "palyra.memory.recall"
        | "palyra.memory.session_search"
        | "palyra.session_search"
        | "palyra.routines.query"
        | "palyra.artifact.read"
        | "palyra.fs.read_file"
        | "palyra.fs.list_dir"
        | "palyra.fs.search"
        | "palyra.browser.title"
        | "palyra.browser.screenshot"
        | "palyra.browser.pdf"
        | "palyra.browser.observe"
        | "palyra.browser.network_log"
        | "palyra.browser.console_log"
        | "palyra.browser.tabs.list"
        | "palyra.browser.permissions.get" => ToolParallelism::ReadOnlySafe,
        "palyra.http.fetch" if is_idempotent_http_fetch_input(input_json) => {
            ToolParallelism::IdempotentNetwork
        }
        "palyra.process.run" => classify_process_runner_parallelism(input_json),
        "palyra.fs.apply_patch" => ToolParallelism::Never,
        _ => ToolParallelism::Never,
    }
}

fn path_scope_key(tool_name: &str, input_json: &[u8]) -> Option<String> {
    if tool_name != "palyra.process.run" {
        return None;
    }
    let payload = serde_json::from_slice::<Value>(input_json).ok()?;
    let command = payload.get("command")?.as_str()?;
    let mut args = payload
        .get("args")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|arg| arg.contains('/') || arg.contains('\\') || arg.starts_with('.'))
        .map(|arg| arg.replace('\\', "/"))
        .collect::<Vec<_>>();
    args.sort();
    if args.is_empty() {
        None
    } else {
        Some(format!("{command}:{}", args.join("|")))
    }
}

fn is_idempotent_http_fetch_input(input_json: &[u8]) -> bool {
    let Ok(payload) = serde_json::from_slice::<Value>(input_json) else {
        return false;
    };
    let method =
        payload.get("method").and_then(Value::as_str).unwrap_or("GET").trim().to_ascii_uppercase();
    matches!(method.as_str(), "GET" | "HEAD")
}

fn classify_process_runner_parallelism(input_json: &[u8]) -> ToolParallelism {
    let Ok(payload) = serde_json::from_slice::<Value>(input_json) else {
        return ToolParallelism::Never;
    };
    let Some(command) = payload.get("command").and_then(Value::as_str) else {
        return ToolParallelism::Never;
    };
    let read_only_commands = ["cat", "head", "tail", "ls", "pwd", "rg", "grep", "find", "wc"];
    if read_only_commands.contains(&command) {
        ToolParallelism::PathScoped
    } else {
        ToolParallelism::Never
    }
}

fn run_stream_tool_parallelism_enabled() -> bool {
    std::env::var(TOOL_PARALLELISM_ENABLED_ENV)
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off" | "disabled"
            )
        })
        .unwrap_or(true)
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn execute_parallel_prepared_tool_group(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_state: &mut RunStateMachine,
    run_id: &str,
    prepared_tools: Vec<RunStreamPreparedToolExecution>,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<RunStreamPreparedToolExecutionBatchOutcome, Status> {
    let group_id = Ulid::new().to_string();
    append_tool_parallel_group_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "tool.parallel_group.started",
        group_id.as_str(),
        "started",
        prepared_tools.as_slice(),
        None,
    )
    .await?;

    let mut join_set = JoinSet::new();
    let nested_tool_budget = shared_tool_budget(*remaining_tool_budget);
    for (order, prepared) in prepared_tools.into_iter().enumerate() {
        let runtime_state = Arc::clone(runtime_state);
        let request_context = request_context.clone();
        let run_id = run_id.to_owned();
        let nested_tool_budget = nested_tool_budget.clone();
        join_set.spawn(async move {
            match execute_prepared_tool_runtime(
                &runtime_state,
                &request_context,
                run_id.as_str(),
                &prepared,
                Some(nested_tool_budget),
            )
            .await?
            {
                Some(outcome) => {
                    Ok(ParallelToolExecutionTaskOutcome::Completed { order, prepared, outcome })
                }
                None => Ok(ParallelToolExecutionTaskOutcome::Cancelled),
            }
        });
    }

    let mut completed =
        BTreeMap::<usize, (RunStreamPreparedToolExecution, ToolExecutionOutcome)>::new();
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(ParallelToolExecutionTaskOutcome::Completed { order, prepared, outcome })) => {
                completed.insert(order, (prepared, outcome));
            }
            Ok(Ok(ParallelToolExecutionTaskOutcome::Cancelled)) => {
                drain_parallel_tool_group_after_cancel(&mut join_set).await?;
                append_tool_parallel_group_tape_event(
                    runtime_state,
                    run_id,
                    tape_seq,
                    "tool.parallel_group.cancelled",
                    group_id.as_str(),
                    "cancelled",
                    &[],
                    Some("cancel_requested_after_parallel_drain"),
                )
                .await?;
                transition_run_stream_to_cancelled(
                    sender,
                    runtime_state,
                    run_state,
                    run_id,
                    tape_seq,
                )
                .await?;
                return Ok(RunStreamPreparedToolExecutionBatchOutcome::Cancelled);
            }
            Ok(Err(error)) => {
                join_set.abort_all();
                return Err(error);
            }
            Err(error) => {
                join_set.abort_all();
                return Err(Status::internal(format!(
                    "parallel tool execution task failed to join: {error}"
                )));
            }
        }
    }
    *remaining_tool_budget = shared_tool_budget_remaining(&nested_tool_budget);

    let mut finalized = Vec::with_capacity(completed.len());
    for (_, (prepared, execution_outcome)) in completed {
        finalized.push(
            finalize_prepared_tool_execution_outcome(
                sender,
                runtime_state,
                request_context,
                run_id,
                &prepared,
                execution_outcome,
                tape_seq,
            )
            .await?,
        );
    }

    append_tool_parallel_group_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "tool.parallel_group.completed",
        group_id.as_str(),
        "completed",
        &[],
        None,
    )
    .await?;
    Ok(RunStreamPreparedToolExecutionBatchOutcome::Completed(finalized))
}

#[allow(clippy::result_large_err)]
async fn drain_parallel_tool_group_after_cancel(
    join_set: &mut JoinSet<Result<ParallelToolExecutionTaskOutcome, Status>>,
) -> Result<(), Status> {
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(ParallelToolExecutionTaskOutcome::Completed { .. }))
            | Ok(Ok(ParallelToolExecutionTaskOutcome::Cancelled)) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => {
                return Err(Status::internal(format!(
                    "parallel tool execution task failed to join while draining cancellation: {error}"
                )));
            }
        }
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn append_tool_parallel_group_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    event_type: &str,
    group_id: &str,
    status: &str,
    tools: &[RunStreamPreparedToolExecution],
    reason: Option<&str>,
) -> Result<(), Status> {
    let tool_entries = tools
        .iter()
        .enumerate()
        .map(|(order, prepared)| {
            json!({
                "order": order,
                "proposal_id": prepared.proposal_id.as_str(),
                "tool_name": prepared.tool_name.as_str(),
                "parallelism": classify_tool_parallelism(
                    prepared.tool_name.as_str(),
                    prepared.input_json.as_slice()
                ).as_str(),
            })
        })
        .collect::<Vec<_>>();
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: event_type.to_owned(),
            payload_json: json!({
                "schema_version": 1,
                "group_id": group_id,
                "status": status,
                "max_parallelism": MAX_PARALLEL_TOOL_CALLS_PER_GROUP,
                "tools": tool_entries,
                "reason": reason,
            })
            .to_string(),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn prepare_run_stream_tool_proposal_execution(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    stream: &mut Streaming<crate::transport::grpc::proto::palyra::common::v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    remaining_tool_budget: &mut u32,
    allow_sensitive_tools: bool,
    approval_cache_generation: Option<u64>,
    tape_seq: &mut i64,
) -> Result<RunStreamToolProposalPreparation, Status> {
    let resolved_session_id = active_session_id.ok_or_else(|| {
        Status::internal(
            "run stream internal invariant violated: missing session_id while preparing tool proposal",
        )
    })?;
    let ToolProposalSecurityEvaluation {
        skill_context,
        skill_gate_decision,
        approval_subject_id,
        proposal_approval_required,
        effective_posture,
        backend_selection,
    } = evaluate_tool_proposal_security(
        runtime_state,
        request_context,
        resolved_session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
    )
    .await;
    runtime_state.record_tool_proposal();
    send_tool_proposal_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        input_json,
        proposal_approval_required,
    )
    .await?;
    let approval_outcome = resolve_run_stream_tool_approval_outcome(
        sender,
        stream,
        runtime_state,
        request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
        skill_context.as_ref(),
        approval_subject_id.as_str(),
        proposal_approval_required,
        &backend_selection,
        allow_sensitive_tools,
        approval_cache_generation,
        tape_seq,
    )
    .await?;
    let ResolvedToolProposalDecision { decision, gate_report } =
        resolve_tool_proposal_decision_for_context(
            runtime_state,
            request_context,
            request_context.channel.as_deref(),
            session_id,
            run_id,
            tool_name,
            skill_context.as_ref(),
            remaining_tool_budget,
            skill_gate_decision,
            proposal_approval_required,
            &effective_posture,
            &backend_selection,
            ToolProposalApprovalState {
                outcome: approval_outcome.as_ref(),
                pending_approval_id: None,
            },
        );
    send_tool_decision_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        decision.allowed,
        decision.reason.as_str(),
        decision.approval_required,
        decision.policy_enforced,
    )
    .await?;
    record_tool_proposal_decision_audit_trail(
        runtime_state,
        request_context,
        resolved_session_id,
        run_id,
        proposal_id,
        tool_name,
        skill_context.as_ref(),
        &decision,
        gate_report.as_ref(),
    )
    .await?;
    Ok(RunStreamToolProposalPreparation {
        decision,
        resolved_session_id: resolved_session_id.to_owned(),
        backend_selection,
    })
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn resolve_run_stream_tool_approval_outcome(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    stream: &mut Streaming<crate::transport::grpc::proto::palyra::common::v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    skill_context: Option<&crate::gateway::ToolSkillContext>,
    approval_subject_id: &str,
    proposal_approval_required: bool,
    backend_selection: &ToolProposalBackendSelection,
    allow_sensitive_tools: bool,
    approval_cache_generation: Option<u64>,
    tape_seq: &mut i64,
) -> Result<Option<ToolApprovalOutcome>, Status> {
    if proposal_approval_required && allow_sensitive_tools {
        let outcome = allow_sensitive_tools_approval_outcome();
        send_tool_approval_response_with_tape(
            sender,
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            outcome.approval_id.as_str(),
            outcome.approved,
            outcome.reason.as_str(),
            outcome.decision_scope,
            outcome.decision_scope_ttl_ms,
        )
        .await?;
        return Ok(Some(outcome));
    }

    let cached_approval_outcome = resolve_cached_tool_approval_for_proposal(
        runtime_state,
        request_context,
        session_id,
        approval_subject_id,
        proposal_approval_required,
        run_id,
        proposal_id,
        "run stream",
    );
    if let Some(cached_outcome) = cached_approval_outcome {
        send_tool_approval_response_with_tape(
            sender,
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            cached_outcome.approval_id.as_str(),
            cached_outcome.approved,
            cached_outcome.reason.as_str(),
            cached_outcome.decision_scope,
            cached_outcome.decision_scope_ttl_ms,
        )
        .await?;
        return Ok(Some(cached_outcome));
    }
    if !proposal_approval_required {
        return Ok(None);
    }

    let pending_approval = build_pending_tool_approval(
        tool_name,
        skill_context,
        input_json,
        &runtime_state.config.tool_call,
        approval_execution_context_for_backend_selection(backend_selection).as_ref(),
    );
    runtime_state
        .create_approval_record(ApprovalCreateRequest {
            approval_id: pending_approval.approval_id.clone(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            principal: request_context.principal.clone(),
            device_id: request_context.device_id.clone(),
            channel: request_context.channel.clone(),
            subject_type: approval_subject_type_for_tool(tool_name),
            subject_id: pending_approval.prompt.subject_id.clone(),
            request_summary: pending_approval.request_summary.clone(),
            policy_snapshot: pending_approval.policy_snapshot.clone(),
            prompt: pending_approval.prompt.clone(),
        })
        .await?;
    info!(
        run_id = run_id,
        proposal_id = proposal_id,
        approval_id = %pending_approval.approval_id,
        subject_id = %pending_approval.prompt.subject_id,
        "approval requested"
    );

    if let Err(error) = send_tool_approval_request_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        pending_approval.approval_id.as_str(),
        tool_name,
        input_json,
        true,
        pending_approval.request_summary.as_str(),
        &pending_approval.prompt,
    )
    .await
    {
        best_effort_mark_approval_error(
            runtime_state,
            pending_approval.approval_id.as_str(),
            format!("approval_request_dispatch_error: {}", error.message()),
        )
        .await;
        return Err(error);
    }
    if let Err(error) = record_approval_requested_journal_event(
        runtime_state,
        request_context,
        session_id,
        run_id,
        proposal_id,
        pending_approval.approval_id.as_str(),
        tool_name,
        pending_approval.prompt.subject_id.as_str(),
        pending_approval.request_summary.as_str(),
        &pending_approval.policy_snapshot,
        &pending_approval.prompt,
    )
    .await
    {
        best_effort_mark_approval_error(
            runtime_state,
            pending_approval.approval_id.as_str(),
            format!("approval_request_journal_error: {}", error.message()),
        )
        .await;
        return Err(error);
    }

    let response = match timeout(
        TOOL_APPROVAL_RESPONSE_TIMEOUT,
        await_tool_approval_response(
            runtime_state,
            stream,
            session_id,
            run_id,
            proposal_id,
            pending_approval.approval_id.as_str(),
        ),
    )
    .await
    {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => ToolApprovalOutcome {
            approval_id: pending_approval.approval_id.clone(),
            approved: false,
            reason: format!("approval_response_error: {}", error.message()),
            decision: crate::journal::ApprovalDecision::Error,
            decision_scope: crate::journal::ApprovalDecisionScope::Once,
            decision_scope_ttl_ms: None,
        },
        Err(_) => ToolApprovalOutcome {
            approval_id: pending_approval.approval_id.clone(),
            approved: false,
            reason: "approval_response_timeout".to_owned(),
            decision: crate::journal::ApprovalDecision::Timeout,
            decision_scope: crate::journal::ApprovalDecisionScope::Once,
            decision_scope_ttl_ms: None,
        },
    };

    let resolved = runtime_state
        .resolve_approval_record(ApprovalResolveRequest {
            approval_id: pending_approval.approval_id.clone(),
            decision: response.decision,
            decision_scope: response.decision_scope,
            decision_reason: response.reason.clone(),
            decision_scope_ttl_ms: response.decision_scope_ttl_ms,
        })
        .await?;
    info!(
        run_id = run_id,
        proposal_id = proposal_id,
        approval_id = %resolved.approval_id,
        decision = %response.decision.as_str(),
        decision_scope = %response.decision_scope.as_str(),
        "approval resolved"
    );

    record_approval_resolved_journal_event(
        runtime_state,
        request_context,
        session_id,
        run_id,
        Some(proposal_id),
        response.approval_id.as_str(),
        response.decision,
        response.decision_scope,
        response.decision_scope_ttl_ms,
        response.reason.as_str(),
    )
    .await?;

    send_tool_approval_response_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        response.approval_id.as_str(),
        response.approved,
        response.reason.as_str(),
        response.decision_scope,
        response.decision_scope_ttl_ms,
    )
    .await?;

    runtime_state.remember_tool_approval_if_generation(
        request_context,
        session_id,
        approval_subject_id,
        &response,
        approval_cache_generation,
    );
    Ok(Some(response))
}

fn allow_sensitive_tools_approval_outcome() -> ToolApprovalOutcome {
    ToolApprovalOutcome {
        approval_id: Ulid::new().to_string(),
        approved: true,
        reason: "approved_by_run_stream_allow_sensitive_tools".to_owned(),
        decision: crate::journal::ApprovalDecision::Allow,
        decision_scope: crate::journal::ApprovalDecisionScope::Once,
        decision_scope_ttl_ms: None,
    }
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn execute_prepared_run_stream_tool_proposal(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_state: &mut RunStateMachine,
    run_id: &str,
    prepared: RunStreamPreparedToolExecution,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    let nested_tool_budget = shared_tool_budget(*remaining_tool_budget);
    let execution_outcome = match execute_prepared_tool_runtime(
        runtime_state,
        request_context,
        run_id,
        &prepared,
        Some(nested_tool_budget.clone()),
    )
    .await?
    {
        Some(outcome) => outcome,
        None => {
            transition_run_stream_to_cancelled(sender, runtime_state, run_state, run_id, tape_seq)
                .await?;
            return Ok(RunStreamToolExecutionOutcome::Cancelled);
        }
    };
    *remaining_tool_budget = shared_tool_budget_remaining(&nested_tool_budget);
    finalize_prepared_tool_execution_outcome(
        sender,
        runtime_state,
        request_context,
        run_id,
        &prepared,
        execution_outcome,
        tape_seq,
    )
    .await
}

#[allow(clippy::result_large_err)]
async fn execute_prepared_tool_runtime(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    remaining_tool_budget: Option<SharedToolBudget>,
) -> Result<Option<ToolExecutionOutcome>, Status> {
    if !prepared.decision.allowed {
        return Ok(Some(denied_execution_outcome(
            prepared.proposal_id.as_str(),
            prepared.tool_name.as_str(),
            prepared.input_json.as_slice(),
            prepared.decision.reason.as_str(),
        )));
    }

    runtime_state.record_tool_execution_attempt();
    let started_at = Instant::now();
    let mut cancel_poll = interval(Duration::from_millis(100));
    cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let must_drain_execution_after_cancel =
        tool_cancellation_requires_execution_drain(prepared.tool_name.as_str());
    let cancellation_requested = Arc::new(AtomicBool::new(false));
    let tool_span = tracing::info_span!(
        "tool.call",
        run_id = %run_id,
        tool_call_id = %prepared.proposal_id,
        tool_name = %prepared.tool_name,
        execution_surface = "run_stream",
        status = tracing::field::Empty,
    );
    let mut execution_future = Box::pin(
        execute_tool_with_runtime_dispatch_with_cancellation(
            runtime_state,
            ToolRuntimeExecutionContext {
                principal: request_context.principal.as_str(),
                device_id: request_context.device_id.as_str(),
                channel: request_context.channel.as_deref(),
                session_id: prepared.resolved_session_id.as_str(),
                run_id,
                execution_backend: prepared.backend_selection.resolution.resolved,
                backend_reason_code: prepared.backend_selection.resolution.reason_code.as_str(),
            },
            prepared.proposal_id.as_str(),
            prepared.tool_name.as_str(),
            prepared.input_json.as_slice(),
            remaining_tool_budget,
            Some(Arc::clone(&cancellation_requested)),
        )
        .instrument(tool_span),
    );
    let mut cancel_requested_during_execution = false;
    let outcome = loop {
        tokio::select! {
            result = &mut execution_future => {
                break result;
            }
            _ = cancel_poll.tick() => {
                match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
                    Ok(true) => {
                        if must_drain_execution_after_cancel {
                            cancellation_requested.store(true, Ordering::Relaxed);
                            cancel_requested_during_execution = true;
                            break execution_future.await;
                        }
                        return Ok(None);
                    }
                    Ok(false) => {}
                    Err(error) => return Err(error),
                }
            }
        }
    };
    record_tool_execution_outcome_metrics(
        runtime_state,
        crate::gateway::ToolExecutionTraceContext {
            run_id,
            proposal_id: prepared.proposal_id.as_str(),
            tool_name: prepared.tool_name.as_str(),
            execution_surface: "run_stream",
        },
        prepared.decision.allowed,
        started_at,
        &outcome,
    );
    if cancel_requested_during_execution {
        return Ok(None);
    }
    Ok(Some(outcome))
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn finalize_prepared_tool_execution_outcome(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    execution_outcome: ToolExecutionOutcome,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    let execution_outcome = project_tool_result_for_model(
        runtime_state,
        ToolRuntimeExecutionContext {
            principal: request_context.principal.as_str(),
            device_id: request_context.device_id.as_str(),
            channel: request_context.channel.as_deref(),
            session_id: prepared.resolved_session_id.as_str(),
            run_id,
            execution_backend: prepared.backend_selection.resolution.resolved,
            backend_reason_code: prepared.backend_selection.resolution.reason_code.as_str(),
        },
        prepared.proposal_id.as_str(),
        prepared.tool_name.as_str(),
        execution_outcome,
    )
    .await?;

    send_tool_result_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        prepared.proposal_id.as_str(),
        execution_outcome.success,
        execution_outcome.output_json.as_slice(),
        execution_outcome.error.as_str(),
    )
    .await?;

    send_tool_attestation_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        prepared.proposal_id.as_str(),
        execution_outcome.attestation.attestation_id.as_str(),
        execution_outcome.attestation.execution_sha256.as_str(),
        execution_outcome.attestation.executed_at_unix_ms,
        execution_outcome.attestation.timed_out,
        execution_outcome.attestation.executor.as_str(),
        execution_outcome.attestation.sandbox_enforcement.as_str(),
    )
    .await?;
    runtime_state.record_tool_attestation_emitted();

    let _ = build_and_ingest_tool_result_memory_summary(
        runtime_state,
        ToolRuntimeExecutionContext {
            principal: request_context.principal.as_str(),
            device_id: request_context.device_id.as_str(),
            channel: request_context.channel.as_deref(),
            session_id: prepared.resolved_session_id.as_str(),
            run_id,
            execution_backend: prepared.backend_selection.resolution.resolved,
            backend_reason_code: prepared.backend_selection.resolution.reason_code.as_str(),
        },
        prepared.tool_name.as_str(),
        prepared.decision.allowed,
        &execution_outcome,
        "run_stream_tool_result",
    )
    .await;
    Ok(RunStreamToolExecutionOutcome::Completed {
        proposal_id: prepared.proposal_id.clone(),
        tool_name: prepared.tool_name.clone(),
        outcome: execution_outcome,
    })
}

async fn project_tool_result_for_model(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    outcome: ToolExecutionOutcome,
) -> Result<ToolExecutionOutcome, Status> {
    let budget = ToolTurnBudget::default();
    let should_spill = should_project_tool_result_for_model(tool_name, &outcome, &budget);
    if !should_spill {
        return Ok(outcome);
    }

    let projection_policy = projection_policy_for_tool(tool_name);
    let default_sensitive =
        matches!(projection_policy, ToolResultProjectionPolicy::RedactedPreviewAndArtifact);

    let sensitivity = tool_result_sensitivity(tool_name, default_sensitive);
    let preview = redacted_tool_result_preview(
        outcome.output_json.as_slice(),
        budget.max_artifact_preview_bytes,
    );
    let artifact_max_payload_bytes = runtime_state.tool_result_artifact_max_payload_bytes();
    let artifact_content = bounded_tool_result_artifact_content(
        outcome.output_json.as_slice(),
        artifact_max_payload_bytes,
    )
    .map_err(Status::resource_exhausted)?;
    let artifact = runtime_state
        .create_tool_result_artifact(ToolResultArtifactCreateRequest {
            artifact_id: Ulid::new().to_string(),
            session_id: context.session_id.to_owned(),
            run_id: context.run_id.to_owned(),
            proposal_id: proposal_id.to_owned(),
            tool_name: tool_name.to_owned(),
            mime_type: "application/json".to_owned(),
            sensitivity,
            retention: ArtifactRetentionPolicy::keep(),
            redacted_preview: preview.clone(),
            content: artifact_content.content.clone(),
        })
        .await?;

    let summary = summarize_tool_result_for_model(
        outcome.output_json.as_slice(),
        budget.max_model_summary_bytes,
    );
    let visibility = if default_sensitive {
        ToolResultVisibility::RedactedPreview
    } else {
        ToolResultVisibility::ModelSummary
    };
    let saved_model_visible_bytes =
        outcome.output_json.len().saturating_sub(summary.len()).try_into().unwrap_or(u64::MAX);
    let projected = json!({
        "schema_version": 1,
        "visibility": visibility.as_str(),
        "projection_policy": projection_policy.as_str(),
        "summary": summary,
        "redacted_preview": preview,
        "artifact": artifact,
        "budget": {
            "max_model_inline_bytes": budget.max_model_inline_bytes,
            "max_model_summary_bytes": budget.max_model_summary_bytes,
            "max_artifact_preview_bytes": budget.max_artifact_preview_bytes,
            "max_artifact_payload_bytes": artifact_max_payload_bytes,
        },
        "metrics": {
            "spilled_artifacts": 1,
            "artifact_content_truncated": artifact_content.truncated,
            "original_artifact_output_bytes": artifact_content.original_output_bytes,
            "stored_artifact_output_bytes": artifact_content.stored_output_bytes,
            "saved_model_visible_bytes": saved_model_visible_bytes,
        }
    });
    let mut projected_outcome = outcome;
    projected_outcome.output_json = serde_json::to_vec(&projected).map_err(|error| {
        Status::internal(format!("failed to serialize projected tool result: {error}"))
    })?;
    Ok(projected_outcome)
}

fn should_project_tool_result_for_model(
    tool_name: &str,
    outcome: &ToolExecutionOutcome,
    budget: &ToolTurnBudget,
) -> bool {
    if os_file_list_dir_result_can_stay_inline(tool_name, outcome.output_json.as_slice(), budget) {
        return false;
    }
    match projection_policy_for_tool(tool_name) {
        ToolResultProjectionPolicy::InlineUnlessLarge => {
            outcome.output_json.len() > budget.max_model_inline_bytes
        }
        ToolResultProjectionPolicy::SummarizeAndArtifact
        | ToolResultProjectionPolicy::RedactedPreviewAndArtifact => true,
    }
}

fn os_file_list_dir_result_can_stay_inline(
    tool_name: &str,
    output_json: &[u8],
    budget: &ToolTurnBudget,
) -> bool {
    if tool_name != crate::gateway::OS_FILE_TOOL_NAME
        || output_json.len() > budget.max_model_inline_bytes
    {
        return false;
    }
    serde_json::from_slice::<Value>(output_json)
        .ok()
        .and_then(|value| value.get("operation").and_then(Value::as_str).map(str::to_owned))
        .is_some_and(|operation| operation == "list_dir")
}

fn tool_result_sensitivity(tool_name: &str, default_sensitive: bool) -> ToolResultSensitivity {
    if tool_name == crate::gateway::PROCESS_RUNNER_TOOL_NAME {
        ToolResultSensitivity::StdoutStderr
    } else if tool_name == crate::gateway::HTTP_FETCH_TOOL_NAME
        || tool_name.starts_with("palyra.browser.")
        || tool_name == "palyra.plugin.run"
    {
        ToolResultSensitivity::ProviderRawPayload
    } else if tool_name == crate::gateway::WORKSPACE_PATCH_TOOL_NAME
        || tool_name == crate::gateway::WORKSPACE_READ_FILE_TOOL_NAME
        || tool_name == crate::gateway::WORKSPACE_LIST_DIR_TOOL_NAME
        || tool_name == crate::gateway::WORKSPACE_SEARCH_TOOL_NAME
    {
        ToolResultSensitivity::InternalPath
    } else if default_sensitive {
        ToolResultSensitivity::ApprovalRiskData
    } else {
        ToolResultSensitivity::Public
    }
}

fn summarize_tool_result_for_model(output_json: &[u8], max_bytes: usize) -> String {
    let preview = redacted_tool_result_preview(output_json, max_bytes);
    if preview.len() <= max_bytes {
        preview
    } else {
        truncate_utf8(preview.as_str(), max_bytes)
    }
}

fn redacted_tool_result_preview(output_json: &[u8], max_bytes: usize) -> String {
    let redacted = match serde_json::from_slice::<Value>(output_json) {
        Ok(mut value) => {
            redact_sensitive_json_value(&mut value);
            serde_json::to_string(&value).unwrap_or_else(|_| REDACTED.to_owned())
        }
        Err(_) => {
            let raw = String::from_utf8_lossy(output_json);
            redact_auth_error(redact_url_segments_in_text(raw.as_ref()).as_str())
        }
    };
    truncate_utf8(redacted.as_str(), max_bytes)
}

fn redact_sensitive_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => redact_sensitive_json_object(map),
        Value::Array(values) => {
            for value in values {
                redact_sensitive_json_value(value);
            }
        }
        Value::String(text) => {
            *text = redact_auth_error(redact_url_segments_in_text(text).as_str());
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn redact_sensitive_json_object(map: &mut Map<String, Value>) {
    for (key, value) in map.iter_mut() {
        if is_sensitive_key(key.as_str()) {
            *value = Value::String(REDACTED.to_owned());
        } else if is_stream_binary_payload_key(key.as_str()) {
            *value = stream_binary_payload_placeholder(value);
        } else {
            redact_sensitive_json_value(value);
        }
    }
}

fn is_stream_binary_payload_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '.'], "_");
    matches!(
        normalized.as_str(),
        "bytes_base64"
            | "image_base64"
            | "pdf_base64"
            | "inline_base64"
            | "screenshot_base64"
            | "failure_screenshot_base64"
            | "failure_image_base64"
    ) || normalized.ends_with("_image_base64")
        || normalized.ends_with("_pdf_base64")
        || normalized.ends_with("_screenshot_base64")
}

fn stream_binary_payload_placeholder(value: &Value) -> Value {
    let char_len = value.as_str().map(str::len).unwrap_or_default();
    Value::String(format!("<redacted:base64 chars={char_len}>"))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    value
        .char_indices()
        .take_while(|(index, ch)| index.saturating_add(ch.len_utf8()) <= max_bytes)
        .map(|(_, ch)| ch)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        allow_sensitive_tools_approval_outcome, classify_tool_parallelism,
        drain_parallel_tool_group_after_cancel, ParallelToolExecutionTaskOutcome, ToolParallelism,
    };
    use crate::journal::{ApprovalDecision, ApprovalDecisionScope};
    use crate::tool_protocol::{ToolAttestation, ToolExecutionOutcome};
    use palyra_common::runtime_contracts::ToolTurnBudget;
    use palyra_common::validate_canonical_id;
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::{
        task::JoinSet,
        time::{sleep, Duration},
    };

    #[test]
    fn tool_parallelism_classifies_safe_and_unsafe_tools() {
        assert_eq!(
            classify_tool_parallelism("palyra.echo", br#"{"text":"hello"}"#),
            ToolParallelism::ReadOnlySafe
        );
        assert_eq!(
            classify_tool_parallelism(
                "palyra.http.fetch",
                br#"{"url":"https://example.test/status","method":"GET"}"#
            ),
            ToolParallelism::IdempotentNetwork
        );
        assert_eq!(
            classify_tool_parallelism(
                "palyra.http.fetch",
                br#"{"url":"https://example.test/update","method":"POST"}"#
            ),
            ToolParallelism::Never
        );
        assert_eq!(
            classify_tool_parallelism(
                "palyra.fs.read_file",
                br#"{"path":"agent-e2e-tool-test.js"}"#
            ),
            ToolParallelism::ReadOnlySafe
        );
        assert_eq!(
            classify_tool_parallelism("palyra.fs.list_dir", br#"{"path":"scenarios"}"#),
            ToolParallelism::ReadOnlySafe
        );
        assert_eq!(
            classify_tool_parallelism("palyra.fs.search", br#"{"query":"customerId"}"#),
            ToolParallelism::ReadOnlySafe
        );
        assert_eq!(
            classify_tool_parallelism("palyra.fs.apply_patch", br#"{"patch":"..."}"#),
            ToolParallelism::Never
        );
    }

    #[test]
    fn allow_sensitive_tools_outcome_is_explicit_once_approval() {
        let outcome = allow_sensitive_tools_approval_outcome();

        assert!(outcome.approved);
        assert_eq!(outcome.decision, ApprovalDecision::Allow);
        assert_eq!(outcome.decision_scope, ApprovalDecisionScope::Once);
        assert!(
            outcome.reason.contains("allow_sensitive_tools"),
            "approval reason should identify the run-stream bypass"
        );
        validate_canonical_id(outcome.approval_id.as_str())
            .expect("auto approval id should be canonical");
    }

    #[tokio::test]
    async fn parallel_cancel_drain_waits_for_sibling_tasks() {
        let sibling_completed = Arc::new(AtomicBool::new(false));
        let sibling_completed_for_task = Arc::clone(&sibling_completed);
        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            sleep(Duration::from_millis(10)).await;
            sibling_completed_for_task.store(true, Ordering::SeqCst);
            Ok(ParallelToolExecutionTaskOutcome::Cancelled)
        });

        drain_parallel_tool_group_after_cancel(&mut join_set)
            .await
            .expect("drain should wait for sibling tasks");

        assert!(
            sibling_completed.load(Ordering::SeqCst),
            "parallel cancellation must wait instead of aborting sibling tasks"
        );
    }

    #[test]
    fn tool_result_projection_preview_redacts_binary_base64_payloads() {
        let raw = "A".repeat(4096);
        let output_json = serde_json::to_vec(&json!({
            "success": true,
            "mime_type": "image/png",
            "size_bytes": 3072,
            "layout_metrics": {
                "viewport_width": 390,
                "viewport_height": 844,
                "document_scroll_width": 980,
                "document_client_width": 390,
                "horizontal_overflow": true
            },
            "image_base64": raw,
        }))
        .expect("test payload should serialize");

        let preview = super::redacted_tool_result_preview(output_json.as_slice(), 1024);

        assert!(preview.contains("\"horizontal_overflow\":true"), "{preview}");
        assert!(preview.contains("\"document_scroll_width\":980"), "{preview}");
        assert!(preview.contains("\"mime_type\":\"image/png\""), "{preview}");
        assert!(preview.contains("\"size_bytes\":3072"), "{preview}");
        assert!(preview.contains("\"image_base64\":\"<redacted:base64 chars=4096>\""), "{preview}");
        assert!(!preview.contains("AAAA"), "{preview}");
    }

    #[test]
    fn tool_result_projection_preview_preserves_benign_source_structure() {
        let source = "const match = document.cookie.match(/(?:^|; )theme=([^;]*)/);\n\
                      const fixture = 'token=a%3Db%3Dc';\n\
                      const selector = '#password';\n";
        let output_json = serde_json::to_vec(&json!({
            "path": "app.js",
            "text": source,
            "redacted": false,
        }))
        .expect("test payload should serialize");

        let preview = super::redacted_tool_result_preview(output_json.as_slice(), 2048);

        assert!(preview.contains("document.cookie.match(/(?:^|; )theme=([^;]*)/)"), "{preview}");
        assert!(preview.contains("token=a%3Db%3Dc"), "{preview}");
        assert!(preview.contains("#password"), "{preview}");
        assert!(!preview.contains("<redacted>"), "{preview}");
    }

    #[test]
    fn failed_browser_tool_results_require_redacted_projection() {
        let raw = "B".repeat(4096);
        let output_json = serde_json::to_vec(&json!({
            "success": false,
            "error": "selector not found",
            "failure_screenshot_mime_type": "image/png",
            "failure_screenshot_base64": raw,
        }))
        .expect("test payload should serialize");
        let outcome = ToolExecutionOutcome {
            success: false,
            output_json,
            error: "selector not found".to_owned(),
            attestation: ToolAttestation {
                attestation_id: "01ARZ3NDEKTSV4RRFFQ69G5FAA".to_owned(),
                execution_sha256: "0".repeat(64),
                executed_at_unix_ms: 0,
                timed_out: false,
                executor: "test".to_owned(),
                sandbox_enforcement: "n/a".to_owned(),
            },
        };

        assert!(super::should_project_tool_result_for_model(
            "palyra.browser.click",
            &outcome,
            &ToolTurnBudget::default()
        ));
        let preview = super::redacted_tool_result_preview(outcome.output_json.as_slice(), 1024);
        assert!(
            preview.contains("\"failure_screenshot_base64\":\"<redacted:base64 chars=4096>\""),
            "{preview}"
        );
        assert!(!preview.contains("BBBB"), "{preview}");
    }

    #[test]
    fn os_file_list_dir_results_stay_inline_when_under_model_budget() {
        let output_json = serde_json::to_vec(&json!({
            "operation": "list_dir",
            "path": "/home/example/.cache/palyra/os-cache",
            "entries": [
                {
                    "name": "zero-a.tmp",
                    "path": "/home/example/.cache/palyra/os-cache/zero-a.tmp",
                    "kind": "file",
                    "size_bytes": 0
                },
                {
                    "name": "f-newest.cache",
                    "path": "/home/example/.cache/palyra/os-cache/f-newest.cache",
                    "kind": "file",
                    "size_bytes": 32
                }
            ],
            "entry_count": 2,
            "skipped_entries": 0,
            "truncated": false
        }))
        .expect("test payload should serialize");
        let outcome = ToolExecutionOutcome {
            success: true,
            output_json,
            error: String::new(),
            attestation: ToolAttestation {
                attestation_id: "01ARZ3NDEKTSV4RRFFQ69G5FAB".to_owned(),
                execution_sha256: "0".repeat(64),
                executed_at_unix_ms: 0,
                timed_out: false,
                executor: "test".to_owned(),
                sandbox_enforcement: "n/a".to_owned(),
            },
        };

        assert!(
            !super::should_project_tool_result_for_model(
                crate::gateway::OS_FILE_TOOL_NAME,
                &outcome,
                &ToolTurnBudget::default()
            ),
            "small OS list_dir metadata must remain fully model-visible for cleanup workflows"
        );
    }
}
