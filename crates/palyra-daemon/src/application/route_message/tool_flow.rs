//! Inline tool execution for the route-message surface.
//!
//! Mirrors the run-stream tool flow (catalog validation, security
//! evaluation, approval gate, policy decision, runtime dispatch) but runs
//! synchronously inside the single routed exchange and returns a one-line
//! summary string that is folded back into the model conversation instead
//! of streamed wire events. Every stage still lands on the orchestrator
//! tape so routed runs replay like streamed ones.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use palyra_common::runtime_contracts::{
    ExecutionWrapperCapability, RuntimeIdempotencyClass, SideEffectFenceState, SideEffectFenceV1,
    SideEffectRetryDecision, ToolResultVisibility,
};
use serde_json::json;
use tonic::Status;
use tracing::warn;

use crate::{
    application::{
        execution_gate::ToolProposalApprovalState,
        route_message::approval::resolve_route_tool_approval_outcome,
        run_stream::{
            flow_control::RunStreamFlowControl,
            tape::{
                append_mcp_transport_invocation_tape_event, append_tool_attestation_tape_event,
                append_tool_decision_tape_event, append_tool_proposal_tape_event,
                append_tool_result_tape_event, mcp_transport_invocation_tape_append_request,
                redact_run_stream_text, redacted_run_stream_output_json,
                tool_attestation_tape_payload, tool_result_tape_payload,
                ToolAttestationTapePayload,
            },
        },
        side_effect_reconciliation::{
            reconcile_unknown_tool_side_effect, record_side_effect_reconciliation_receipt,
            SideEffectReconciliationBinding, SideEffectReconciliationOutcome,
        },
        tool_governance::{apply_host_tool_result_middleware, ToolResultMiddlewareReport},
        tool_registry::{
            describe_catalog_tool, normalization_audit_tape_payload, rejection_tape_payload,
            resolve_catalog_invoke_target, resolve_tool_execution_semantics,
            search_tool_catalog_index, tool_call_rejection_outcome,
            validate_tool_call_against_catalog_snapshot,
            validate_tool_call_against_model_visible_tool, ModelVisibleToolCatalogSnapshot,
            NormalizedToolCall, ToolArgumentNormalizationAudit, ToolCallRejection,
            ToolCatalogBridgeError, ToolReplaySafetyClass, TOOL_CATALOG_DESCRIBE_TOOL_NAME,
            TOOL_CATALOG_INVOKE_TOOL_NAME, TOOL_CATALOG_SEARCH_TOOL_NAME,
        },
        tool_security::{
            evaluate_tool_proposal_security, record_tool_proposal_decision_audit_trail,
            resolve_tool_proposal_decision_for_context, ResolvedToolProposalDecision,
            ToolProposalSecurityEvaluation,
        },
    },
    gateway::{
        build_and_ingest_tool_result_memory_summary,
        execute_tool_with_runtime_dispatch_with_cancellation_and_progress,
        record_tool_execution_outcome_metrics, shared_tool_budget, shared_tool_budget_remaining,
        tool_cancellation_requires_execution_drain, GatewayRuntimeState, ToolExecutionTraceContext,
        ToolRuntimeDispatchControls, ToolRuntimeExecutionContext, PROCESS_RUNNER_TOOL_NAME,
    },
    journal::{
        OrchestratorTapeAppendRequest, SideEffectFenceCleanupOutcomeRequest,
        ToolEffectObservationCommitRequest,
    },
    tool_protocol::{build_tool_execution_outcome, denied_execution_outcome, ToolExecutionOutcome},
    transport::grpc::auth::RequestContext,
};

#[derive(Debug, Clone)]
struct ActiveRouteToolSideEffectFence {
    operation_id: palyra_common::runtime_contracts::RuntimeOperationId,
    generation: palyra_common::runtime_contracts::RuntimeGeneration,
    intent_sha256: String,
    strategy: palyra_common::runtime_contracts::ReconciliationStrategy,
    external_idempotency_key_sha256: Option<String>,
}

/// Validates, gates, and (when allowed) executes one tool proposal from the
/// routed provider turn, returning the textual tool-result summary fed back
/// to the model.
///
/// Approval-required proposals are recorded as pending and denied for this
/// run (there is no interactive client to answer the prompt); denied
/// proposals still produce a denial outcome on the tape so replays stay
/// complete.
///
/// # Errors
/// Returns a status when a tape append, approval recording, or the decision
/// audit trail fails; tool execution failures are reported inside the
/// returned summary, not as errors.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_route_tool_proposal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    route_request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    flow_control: &RunStreamFlowControl,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<String, Status> {
    let NormalizedToolCall { input_json: normalized_input_json, audit } =
        match validate_tool_call_against_catalog_snapshot(
            tool_catalog_snapshot,
            tool_name,
            input_json,
        ) {
            Ok(normalized) => normalized,
            Err(rejection) => {
                return reject_route_tool_call(
                    runtime_state,
                    run_id,
                    proposal_id,
                    tool_name,
                    input_json,
                    rejection,
                    tape_seq,
                )
                .await;
            }
        };
    if !audit.steps.is_empty() {
        append_route_tool_argument_normalization_tape_event(
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            tool_name,
            &audit,
        )
        .await?;
    }
    if tool_name == TOOL_CATALOG_SEARCH_TOOL_NAME || tool_name == TOOL_CATALOG_DESCRIBE_TOOL_NAME {
        let bridge_result = if tool_name == TOOL_CATALOG_SEARCH_TOOL_NAME {
            search_tool_catalog_index(tool_catalog_snapshot, normalized_input_json.as_slice())
        } else {
            describe_catalog_tool(tool_catalog_snapshot, normalized_input_json.as_slice())
        };
        return complete_route_catalog_bridge_tool_call(
            runtime_state,
            route_request_context,
            session_id,
            run_id,
            proposal_id,
            tool_name,
            normalized_input_json.as_slice(),
            bridge_result,
            tool_catalog_snapshot.index.index_digest.as_str(),
            tape_seq,
        )
        .await;
    }
    let (execution_tool_name, mut execution_input_json) = if tool_name
        == TOOL_CATALOG_INVOKE_TOOL_NAME
    {
        let target = match resolve_catalog_invoke_target(
            tool_catalog_snapshot,
            normalized_input_json.as_slice(),
        ) {
            Ok(target) => target,
            Err(error) => {
                return complete_route_catalog_bridge_tool_call(
                    runtime_state,
                    route_request_context,
                    session_id,
                    run_id,
                    proposal_id,
                    tool_name,
                    normalized_input_json.as_slice(),
                    Err(error),
                    tool_catalog_snapshot.index.index_digest.as_str(),
                    tape_seq,
                )
                .await;
            }
        };
        let Some(target_tool) =
            tool_catalog_snapshot.indexed_tools.iter().find(|tool| tool.name == target.tool_name)
        else {
            return complete_route_catalog_bridge_tool_call(
                runtime_state,
                route_request_context,
                session_id,
                run_id,
                proposal_id,
                tool_name,
                normalized_input_json.as_slice(),
                Err(ToolCatalogBridgeError {
                    reason_code: "tool_catalog.tool_not_indexed".to_owned(),
                    message: "tool_id is unknown or hidden in the current catalog snapshot"
                        .to_owned(),
                }),
                tool_catalog_snapshot.index.index_digest.as_str(),
                tape_seq,
            )
            .await;
        };
        let target_call = match validate_tool_call_against_model_visible_tool(
            tool_catalog_snapshot,
            target_tool,
            target.tool_name.as_str(),
            target.input_json.as_slice(),
        ) {
            Ok(normalized) => normalized,
            Err(error) => {
                return complete_route_catalog_bridge_tool_call(
                    runtime_state,
                    route_request_context,
                    session_id,
                    run_id,
                    proposal_id,
                    tool_name,
                    normalized_input_json.as_slice(),
                    Err(ToolCatalogBridgeError {
                        reason_code: error.reason_code,
                        message: error.message,
                    }),
                    tool_catalog_snapshot.index.index_digest.as_str(),
                    tape_seq,
                )
                .await;
            }
        };
        if !target_call.audit.steps.is_empty() {
            append_route_tool_argument_normalization_tape_event(
                runtime_state,
                run_id,
                tape_seq,
                proposal_id,
                target.tool_name.as_str(),
                &target_call.audit,
            )
            .await?;
        }
        append_route_catalog_invoke_lineage_tape_event(
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            target.tool_name.as_str(),
            target.schema_digest.as_str(),
            tool_catalog_snapshot.index.index_digest.as_str(),
            target_call.audit.normalized_json_hash.as_str(),
        )
        .await?;
        (target.tool_name, target_call.input_json)
    } else {
        (tool_name.to_owned(), normalized_input_json)
    };
    execution_input_json =
        crate::application::run_stream::tool_flow::dispatch_tool_argument_patch_if_enabled(
            runtime_state,
            run_id,
            proposal_id,
            execution_tool_name.as_str(),
            execution_input_json.as_slice(),
            tool_catalog_snapshot,
            tape_seq,
        )
        .await?;
    let tool_name = execution_tool_name.as_str();
    let input_json = execution_input_json.as_slice();
    let replay_safety_class = tool_catalog_snapshot
        .tools
        .iter()
        .chain(tool_catalog_snapshot.indexed_tools.iter())
        .find(|tool| tool.name == tool_name)
        .map_or(ToolReplaySafetyClass::RequiresHumanConfirmation, |tool| tool.replay_safety_class);

    let ToolProposalSecurityEvaluation {
        skill_context,
        skill_gate_decision,
        approval_subject_id: _,
        proposal_approval_required,
        effective_posture,
        backend_selection,
    } = evaluate_tool_proposal_security(
        runtime_state,
        route_request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
    )
    .await;
    runtime_state.record_tool_proposal();
    append_tool_proposal_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        input_json,
        proposal_approval_required,
    )
    .await?;
    let pending_approval_id = resolve_route_tool_approval_outcome(
        runtime_state,
        route_request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
        skill_context.as_ref(),
        proposal_approval_required,
        &backend_selection,
        tape_seq,
    )
    .await?;

    let ResolvedToolProposalDecision { decision, gate_report } =
        resolve_tool_proposal_decision_for_context(
            runtime_state,
            route_request_context,
            route_request_context.channel.as_deref(),
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
                outcome: None,
                pending_approval_id: pending_approval_id.as_deref(),
            },
        );
    append_tool_decision_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "tool.decision",
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
        route_request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        skill_context.as_ref(),
        &decision,
        gate_report.as_ref(),
    )
    .await?;

    let mut post_execution_error = None;
    let (mut execution_outcome, active_side_effect_fence) = if decision.allowed {
        if runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await? {
            return Err(Status::cancelled(crate::gateway::CANCELLED_REASON));
        }
        let mut execution_wrapper = ExecutionWrapperCapability::new(crate::sha256_hex(
            format!("route-tool-wrapper-v1\0{run_id}\0{proposal_id}").as_bytes(),
        ));
        execution_wrapper.next_call().map_err(|error| {
            Status::failed_precondition(format!(
                "route tool execution wrapper rejected continuation {}: {}",
                error.code, error.message
            ))
        })?;
        runtime_state.record_tool_execution_attempt();
        let started_at = Instant::now();
        let execution_timeout = route_tool_execution_timeout(runtime_state, tool_name);
        let tool_cancellation = flow_control.child(
            palyra_common::runtime_contracts::CancellationScopeKind::ToolExecution,
            execution_timeout,
        )?;
        let process_cancellation = if tool_name == PROCESS_RUNNER_TOOL_NAME {
            Some(flow_control.child_from(
                &tool_cancellation,
                palyra_common::runtime_contracts::CancellationScopeKind::Process,
                execution_timeout,
            )?)
        } else {
            None
        };
        let effective_cancellation = process_cancellation.unwrap_or(tool_cancellation);
        let execution_deadline =
            RunStreamFlowControl::remaining_for_new_work(&effective_cancellation)?;
        let active_side_effect_fence = prepare_route_tool_side_effect_fence(
            runtime_state,
            session_id,
            run_id,
            proposal_id,
            tool_name,
            input_json,
            replay_safety_class,
        )
        .await?;
        if let Some(fence) = active_side_effect_fence.as_ref() {
            runtime_state
                .transition_tool_side_effect_fence(
                    fence.operation_id.clone(),
                    SideEffectFenceState::EffectStarted,
                    fence.generation,
                    "tool.effect.started".to_owned(),
                    None,
                )
                .await?;
        }
        // Dispatch may spawn nested tool calls (e.g. delegation), so the
        // budget travels as a shared counter and is read back afterwards to
        // charge everything the dispatch consumed.
        let nested_tool_budget = shared_tool_budget(*remaining_tool_budget);
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let execution_runtime_state = Arc::clone(runtime_state);
        let execution_principal = route_request_context.principal.clone();
        let execution_device_id = route_request_context.device_id.clone();
        let execution_channel = route_request_context.channel.clone();
        let execution_session_id = session_id.to_owned();
        let execution_run_id = run_id.to_owned();
        let execution_proposal_id = proposal_id.to_owned();
        let execution_tool_name = tool_name.to_owned();
        let dispatched_input_json = execution_input_json.clone();
        let execution_backend = backend_selection.resolution.resolved;
        let execution_backend_reason = backend_selection.resolution.reason_code.clone();
        let execution_cancellation = effective_cancellation.clone();
        let execution_cancellation_requested = Arc::clone(&cancellation_requested);
        let execution_tool_budget = nested_tool_budget.clone();
        let child_task_parent_context = flow_control.root_context().clone();
        let mut execution_future = Box::pin(async move {
            execute_tool_with_runtime_dispatch_with_cancellation_and_progress(
                &execution_runtime_state,
                ToolRuntimeExecutionContext {
                    principal: execution_principal.as_str(),
                    device_id: execution_device_id.as_str(),
                    channel: execution_channel.as_deref(),
                    session_id: execution_session_id.as_str(),
                    run_id: execution_run_id.as_str(),
                    execution_backend,
                    backend_reason_code: execution_backend_reason.as_str(),
                },
                execution_proposal_id.as_str(),
                execution_tool_name.as_str(),
                dispatched_input_json.as_slice(),
                ToolRuntimeDispatchControls {
                    remaining_tool_budget: Some(execution_tool_budget),
                    cancellation_requested: Some(execution_cancellation_requested),
                    process_progress_sink: None,
                    cancellation_context: Some(execution_cancellation),
                    child_task_parent_context: Some(child_task_parent_context),
                },
            )
            .await
        });
        let execution_deadline_sleep = tokio::time::sleep(execution_deadline);
        tokio::pin!(execution_deadline_sleep);
        let mut cancel_poll = tokio::time::interval(Duration::from_millis(100));
        cancel_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let must_drain = tool_cancellation_requires_execution_drain(tool_name)
            || active_side_effect_fence.is_some();
        let outcome = loop {
            tokio::select! {
                outcome = &mut execution_future => break outcome,
                _ = &mut execution_deadline_sleep => {
                    cancellation_requested.store(true, Ordering::Relaxed);
                    if must_drain {
                        match tokio::time::timeout(
                            Duration::from_millis(effective_cancellation.hard_abort_after_ms.max(1)),
                            &mut execution_future,
                        )
                        .await
                        {
                            Ok(outcome) => {
                                post_execution_error = Some(Status::deadline_exceeded(
                                    "route-message tool execution deadline exceeded",
                                ));
                                break outcome;
                            }
                            Err(_) => {
                                detach_route_tool_cleanup_after_hard_boundary(
                                    execution_future,
                                    RouteToolCleanupSupervisor {
                                        runtime_state: Arc::clone(runtime_state),
                                        fence: active_side_effect_fence.clone(),
                                        run_id: run_id.to_owned(),
                                        proposal_id: proposal_id.to_owned(),
                                        tool_name: tool_name.to_owned(),
                                        decision_allowed: decision.allowed,
                                        started_at,
                                    },
                                );
                                return Err(Status::deadline_exceeded(
                                    "route-message tool cleanup remains unknown after hard boundary",
                                ));
                            }
                        }
                    }
                    return Err(Status::deadline_exceeded(
                        "route-message tool execution deadline exceeded",
                    ));
                }
                _ = cancel_poll.tick() => {
                    match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
                        Ok(true) => {
                            cancellation_requested.store(true, Ordering::Relaxed);
                            if must_drain {
                                match tokio::time::timeout(
                                    Duration::from_millis(
                                        effective_cancellation.hard_abort_after_ms.max(1),
                                    ),
                                    &mut execution_future,
                                )
                                .await
                                {
                                    Ok(outcome) => {
                                        post_execution_error = Some(Status::cancelled(
                                            crate::gateway::CANCELLED_REASON,
                                        ));
                                        break outcome;
                                    }
                                    Err(_) => {
                                        detach_route_tool_cleanup_after_hard_boundary(
                                            execution_future,
                                            RouteToolCleanupSupervisor {
                                                runtime_state: Arc::clone(runtime_state),
                                                fence: active_side_effect_fence.clone(),
                                                run_id: run_id.to_owned(),
                                                proposal_id: proposal_id.to_owned(),
                                                tool_name: tool_name.to_owned(),
                                                decision_allowed: decision.allowed,
                                                started_at,
                                            },
                                        );
                                        return Err(Status::cancelled(
                                            "route-message tool cleanup remains unknown after cancellation",
                                        ));
                                    }
                                }
                            }
                            return Err(Status::cancelled(crate::gateway::CANCELLED_REASON));
                        }
                        Ok(false) => {}
                        Err(error) => {
                            cancellation_requested.store(true, Ordering::Relaxed);
                            if must_drain {
                                match tokio::time::timeout(
                                    Duration::from_millis(
                                        effective_cancellation.hard_abort_after_ms.max(1),
                                    ),
                                    &mut execution_future,
                                )
                                .await
                                {
                                    Ok(outcome) => {
                                        post_execution_error = Some(error);
                                        break outcome;
                                    }
                                    Err(_) => {
                                        detach_route_tool_cleanup_after_hard_boundary(
                                            execution_future,
                                            RouteToolCleanupSupervisor {
                                                runtime_state: Arc::clone(runtime_state),
                                                fence: active_side_effect_fence.clone(),
                                                run_id: run_id.to_owned(),
                                                proposal_id: proposal_id.to_owned(),
                                                tool_name: tool_name.to_owned(),
                                                decision_allowed: decision.allowed,
                                                started_at,
                                            },
                                        );
                                        return Err(error);
                                    }
                                }
                            }
                            return Err(error);
                        }
                    }
                }
            }
        };
        *remaining_tool_budget = shared_tool_budget_remaining(&nested_tool_budget);
        if let Some(fence) = active_side_effect_fence.as_ref() {
            if let Err(error) = record_side_effect_reconciliation_receipt(
                runtime_state,
                run_id,
                tape_seq,
                proposal_id,
                tool_name,
                SideEffectReconciliationBinding {
                    operation_id: &fence.operation_id,
                    generation: fence.generation,
                    intent_sha256: fence.intent_sha256.as_str(),
                    strategy: fence.strategy,
                    external_idempotency_key_sha256: fence
                        .external_idempotency_key_sha256
                        .as_deref(),
                },
                &outcome,
            )
            .await
            {
                if let Err(settlement_error) =
                    mark_route_tool_side_effect_unknown(runtime_state, Some(fence)).await
                {
                    warn!(
                        run_id,
                        proposal_id,
                        tool_name,
                        error = %settlement_error,
                        "failed to mark route tool effect unknown after reconciliation receipt persistence failure"
                    );
                }
                return Err(error);
            }
        }
        record_tool_execution_outcome_metrics(
            runtime_state,
            ToolExecutionTraceContext {
                run_id,
                proposal_id,
                tool_name,
                execution_surface: "route_message",
            },
            decision.allowed,
            started_at,
            &outcome,
        );
        (outcome, active_side_effect_fence)
    } else {
        (
            denied_execution_outcome(proposal_id, tool_name, input_json, decision.reason.as_str()),
            None,
        )
    };

    if runtime_state.config.feature_rollouts.tool_result_middleware.enabled
        || tool_name.starts_with("mcp.")
    {
        let report = apply_host_tool_result_middleware(
            tool_name,
            execution_outcome.output_json.as_slice(),
            ToolResultVisibility::ModelInline,
        )
        .map_err(|error| {
            Status::failed_precondition(format!("tool_result_middleware.invalid_output: {error}"))
        })?;
        execution_outcome.output_json.clone_from(&report.model_visible_output_json);
        append_route_tool_result_middleware_tape_event(runtime_state, run_id, tape_seq, &report)
            .await?;
    }

    if active_side_effect_fence.is_some() && !execution_outcome.attestation.timed_out {
        commit_route_tool_execution_outcome(
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            tool_name,
            active_side_effect_fence.as_ref(),
            &execution_outcome,
        )
        .await?;
    } else {
        append_route_tool_execution_tape_events(
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            tool_name,
            &execution_outcome,
        )
        .await?;
        settle_route_tool_side_effect_fence(
            runtime_state,
            active_side_effect_fence.as_ref(),
            &execution_outcome,
        )
        .await?;
    }
    if let Some(error) = post_execution_error {
        return Err(error);
    }

    Ok(build_and_ingest_tool_result_memory_summary(
        runtime_state,
        ToolRuntimeExecutionContext {
            principal: route_request_context.principal.as_str(),
            device_id: route_request_context.device_id.as_str(),
            channel: route_request_context.channel.as_deref(),
            session_id,
            run_id,
            execution_backend: backend_selection.resolution.resolved,
            backend_reason_code: backend_selection.resolution.reason_code.as_str(),
        },
        tool_name,
        decision.allowed,
        &execution_outcome,
        "route_message_tool_result",
    )
    .await)
}

#[allow(clippy::result_large_err)]
async fn append_route_tool_result_middleware_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    report: &ToolResultMiddlewareReport,
) -> Result<(), Status> {
    let payload_json = serde_json::to_string(report).map_err(|error| {
        Status::internal(format!("failed to serialize tool result middleware report: {error}"))
    })?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool.result_middleware".to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::result_large_err)]
async fn prepare_route_tool_side_effect_fence(
    runtime_state: &Arc<GatewayRuntimeState>,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    replay_safety_class: ToolReplaySafetyClass,
) -> Result<Option<ActiveRouteToolSideEffectFence>, Status> {
    let resolved_semantics =
        resolve_tool_execution_semantics(tool_name, replay_safety_class, input_json);
    let semantics = resolved_semantics.semantics;
    if matches!(
        semantics.idempotency_class,
        RuntimeIdempotencyClass::ReadOnly | RuntimeIdempotencyClass::DeterministicIdempotent
    ) {
        return Ok(None);
    }
    semantics.validate().map_err(|error| Status::failed_precondition(error.to_string()))?;
    let Some((generation_session_id, generation)) =
        runtime_state.runtime_generation_for_tool(run_id.to_owned()).await?
    else {
        return Err(Status::failed_precondition(
            "tool side effect requires an active runtime generation",
        ));
    };
    if generation_session_id != session_id {
        return Err(Status::failed_precondition(
            "tool side effect session does not own the active runtime generation",
        ));
    }
    let (operation_id, tool_execution_id) =
        GatewayRuntimeState::tool_side_effect_identities(proposal_id)?;
    let intent_sha256 = crate::sha256_hex(input_json);
    let strategy = semantics.reconciliation_strategy;
    let external_idempotency_key_sha256 = resolved_semantics.external_idempotency_key_sha256;
    let fence = SideEffectFenceV1 {
        schema_version: 1,
        operation_id: operation_id.clone(),
        tool_execution_id,
        intent_generation: generation,
        observed_generation: generation,
        intent_sha256: intent_sha256.clone(),
        state: SideEffectFenceState::IntentRecorded,
        semantics,
        external_idempotency_key_sha256: external_idempotency_key_sha256.clone(),
        evidence_sha256: None,
        reason_code: "tool.effect.intent_recorded".to_owned(),
        updated_at_unix_ms: crate::gateway::current_unix_ms(),
    };
    match runtime_state
        .prepare_tool_side_effect_fence(session_id.to_owned(), run_id.to_owned(), fence)
        .await?
    {
        SideEffectRetryDecision::Safe => Ok(Some(ActiveRouteToolSideEffectFence {
            operation_id,
            generation,
            intent_sha256,
            strategy,
            external_idempotency_key_sha256,
        })),
        SideEffectRetryDecision::Completed => {
            Err(Status::already_exists("tool side effect already completed for this proposal"))
        }
        SideEffectRetryDecision::ReconciliationRequired => {
            match reconcile_unknown_tool_side_effect(
                runtime_state,
                run_id,
                proposal_id,
                tool_name,
                SideEffectReconciliationBinding {
                    operation_id: &operation_id,
                    generation,
                    intent_sha256: intent_sha256.as_str(),
                    strategy,
                    external_idempotency_key_sha256: external_idempotency_key_sha256.as_deref(),
                },
            )
            .await?
            {
                SideEffectReconciliationOutcome::Reconciled => Err(Status::already_exists(
                    "tool side effect was reconciled from an exact durable receipt",
                )),
                SideEffectReconciliationOutcome::Blocked { reason_code } => {
                    Err(Status::failed_precondition(format!(
                        "{reason_code}: tool side effect requires exact reconciliation evidence before retry"
                    )))
                }
            }
        }
        SideEffectRetryDecision::ConfirmationRequired => {
            Err(Status::failed_precondition("tool side effect requires confirmation before retry"))
        }
        SideEffectRetryDecision::Blocked => Err(Status::failed_precondition(
            "tool side effect retry is blocked by durable evidence",
        )),
    }
}

fn route_tool_execution_timeout(runtime_state: &GatewayRuntimeState, tool_name: &str) -> Duration {
    let configured = Duration::from_millis(runtime_state.config.tool_call.execution_timeout_ms);
    if tool_name == PROCESS_RUNNER_TOOL_NAME {
        configured.max(Duration::from_secs(1))
    } else {
        configured.max(Duration::from_millis(1))
    }
}

struct RouteToolCleanupSupervisor {
    runtime_state: Arc<GatewayRuntimeState>,
    fence: Option<ActiveRouteToolSideEffectFence>,
    run_id: String,
    proposal_id: String,
    tool_name: String,
    decision_allowed: bool,
    started_at: Instant,
}

fn detach_route_tool_cleanup_after_hard_boundary<F>(
    execution_future: std::pin::Pin<Box<F>>,
    supervisor: RouteToolCleanupSupervisor,
) where
    F: std::future::Future<Output = ToolExecutionOutcome> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = mark_route_tool_side_effect_unknown(
            &supervisor.runtime_state,
            supervisor.fence.as_ref(),
        )
        .await
        {
            warn!(
                run_id = %supervisor.run_id,
                proposal_id = %supervisor.proposal_id,
                tool_name = %supervisor.tool_name,
                error = %error,
                "route-message tool cleanup supervisor could not persist the hard-boundary uncertainty"
            );
            return;
        }
        let outcome = execution_future.await;
        if let Err(error) = record_route_tool_side_effect_cleanup_outcome(
            &supervisor.runtime_state,
            supervisor.fence.as_ref(),
            &outcome,
        )
        .await
        {
            warn!(
                run_id = %supervisor.run_id,
                proposal_id = %supervisor.proposal_id,
                tool_name = %supervisor.tool_name,
                error = %error,
                "route-message tool cleanup supervisor failed to record the late effect outcome"
            );
        }
        record_tool_execution_outcome_metrics(
            &supervisor.runtime_state,
            ToolExecutionTraceContext {
                run_id: supervisor.run_id.as_str(),
                proposal_id: supervisor.proposal_id.as_str(),
                tool_name: supervisor.tool_name.as_str(),
                execution_surface: "route_message_cleanup_supervisor",
            },
            supervisor.decision_allowed,
            supervisor.started_at,
            &outcome,
        );
    });
}

fn route_tool_side_effect_cleanup_outcome_request(
    fence: &ActiveRouteToolSideEffectFence,
    outcome: &ToolExecutionOutcome,
) -> SideEffectFenceCleanupOutcomeRequest {
    SideEffectFenceCleanupOutcomeRequest {
        operation_id: fence.operation_id.as_str().to_owned(),
        observed_generation: fence.generation,
        outcome_observed: !outcome.attestation.timed_out,
        reason_code: if outcome.attestation.timed_out {
            "tool.effect.cleanup_unknown"
        } else {
            "tool.effect.cleanup_reconciled"
        }
        .to_owned(),
        evidence_sha256: (!outcome.attestation.timed_out)
            .then(|| outcome.attestation.execution_sha256.clone()),
    }
}

#[allow(clippy::result_large_err)]
async fn record_route_tool_side_effect_cleanup_outcome(
    runtime_state: &Arc<GatewayRuntimeState>,
    fence: Option<&ActiveRouteToolSideEffectFence>,
    outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    let Some(fence) = fence else {
        return Ok(());
    };
    runtime_state
        .record_tool_side_effect_cleanup_outcome(route_tool_side_effect_cleanup_outcome_request(
            fence, outcome,
        ))
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn mark_route_tool_side_effect_unknown(
    runtime_state: &Arc<GatewayRuntimeState>,
    fence: Option<&ActiveRouteToolSideEffectFence>,
) -> Result<(), Status> {
    let Some(fence) = fence else {
        return Ok(());
    };
    runtime_state
        .transition_tool_side_effect_fence(
            fence.operation_id.clone(),
            SideEffectFenceState::EffectUnknown,
            fence.generation,
            "tool.effect.ack_unknown".to_owned(),
            None,
        )
        .await
        .map(|_| ())
}

#[allow(clippy::too_many_arguments, clippy::result_large_err)]
async fn commit_route_tool_execution_outcome(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    fence: Option<&ActiveRouteToolSideEffectFence>,
    outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    let Some(fence) = fence else {
        return Err(Status::internal(
            "mutating route tool result requires an active side-effect fence",
        ));
    };
    let safe_output_json = redacted_run_stream_output_json(outcome.output_json.as_slice());
    let safe_error = redact_run_stream_text(outcome.error.as_str());
    let mut result_seq = *tape_seq;
    let mut tape_events = Vec::with_capacity(4);
    if let Some(invocation) = outcome.attestation.mcp_transport_invocation.as_deref() {
        tape_events
            .push(mcp_transport_invocation_tape_append_request(run_id, result_seq, invocation)?);
        result_seq = result_seq.saturating_add(1);
    }
    let attestation_seq = result_seq.saturating_add(1);
    let legacy_seq = attestation_seq.saturating_add(1);
    tape_events.extend([
        OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: result_seq,
            event_type: "tool_result".to_owned(),
            payload_json: tool_result_tape_payload(
                proposal_id,
                outcome.success,
                safe_output_json.as_slice(),
                safe_error.as_str(),
            ),
        },
        OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: attestation_seq,
            event_type: "tool_attestation".to_owned(),
            payload_json: tool_attestation_tape_payload(ToolAttestationTapePayload {
                proposal_id,
                attestation_id: outcome.attestation.attestation_id.as_str(),
                execution_sha256: outcome.attestation.execution_sha256.as_str(),
                executed_at_unix_ms: outcome.attestation.executed_at_unix_ms,
                timed_out: outcome.attestation.timed_out,
                executor: outcome.attestation.executor.as_str(),
                sandbox_enforcement: outcome.attestation.sandbox_enforcement.as_str(),
                execution_manifest: outcome.attestation.execution_manifest.as_deref(),
            }),
        },
        OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: legacy_seq,
            event_type: "tool.executed".to_owned(),
            payload_json: json!({
                "proposal_id": proposal_id,
                "tool_name": tool_name,
                "success": outcome.success,
                "error": outcome.error,
                "attestation": {
                    "attestation_id": outcome.attestation.attestation_id,
                    "execution_sha256": outcome.attestation.execution_sha256,
                    "executed_at_unix_ms": outcome.attestation.executed_at_unix_ms,
                    "timed_out": outcome.attestation.timed_out,
                    "executor": outcome.attestation.executor,
                    "sandbox_enforcement": outcome.attestation.sandbox_enforcement,
                }
            })
            .to_string(),
        },
    ]);
    runtime_state
        .commit_tool_effect_observation(ToolEffectObservationCommitRequest {
            operation_id: fence.operation_id.clone(),
            generation: fence.generation,
            evidence_sha256: outcome.attestation.execution_sha256.clone(),
            tape_events,
        })
        .await?;
    runtime_state.record_tool_attestation_emitted();
    *tape_seq = legacy_seq.saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn settle_route_tool_side_effect_fence(
    runtime_state: &Arc<GatewayRuntimeState>,
    fence: Option<&ActiveRouteToolSideEffectFence>,
    outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    let Some(fence) = fence else {
        return Ok(());
    };
    if outcome.attestation.timed_out {
        return mark_route_tool_side_effect_unknown(runtime_state, Some(fence)).await;
    }
    let next = SideEffectFenceState::EffectObserved;
    let reason_code = "tool.effect.observed";
    let evidence_sha256 = Some(outcome.attestation.execution_sha256.clone());
    runtime_state
        .transition_tool_side_effect_fence(
            fence.operation_id.clone(),
            next,
            fence.generation,
            reason_code.to_owned(),
            evidence_sha256,
        )
        .await
        .map(|_| ())
}

/// Records the argument-normalization audit on the tape so replays can see
/// how the model's raw arguments were rewritten before validation.
#[allow(clippy::result_large_err)]
async fn append_route_tool_argument_normalization_tape_event(
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

#[allow(clippy::result_large_err, clippy::too_many_arguments)]
async fn complete_route_catalog_bridge_tool_call(
    runtime_state: &Arc<GatewayRuntimeState>,
    route_request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    bridge_result: Result<serde_json::Value, ToolCatalogBridgeError>,
    index_digest: &str,
    tape_seq: &mut i64,
) -> Result<String, Status> {
    runtime_state.record_tool_proposal();
    append_tool_proposal_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        input_json,
        false,
    )
    .await?;
    let success = bridge_result.is_ok();
    let reason = bridge_result
        .as_ref()
        .map(|_| "catalog bridge query resolved".to_owned())
        .unwrap_or_else(|error| format!("{}: {}", error.reason_code, error.message));
    append_tool_decision_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "tool.decision",
        proposal_id,
        tool_name,
        success,
        reason.as_str(),
        false,
        true,
    )
    .await?;
    let output_value = bridge_result.unwrap_or_else(|error| {
        json!({
            "schema_version": 1,
            "error": {
                "reason_code": error.reason_code,
                "message": error.message,
                "index_digest": index_digest,
            }
        })
    });
    append_route_catalog_bridge_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        index_digest,
        &output_value,
        success,
    )
    .await?;
    let output_json = serde_json::to_vec(&output_value).unwrap_or_else(|_| b"{}".to_vec());
    let execution_outcome = build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        success,
        output_json,
        if success { String::new() } else { reason },
        false,
        "tool_catalog_bridge".to_owned(),
        format!("catalog_snapshot:index_digest={index_digest}"),
    );
    append_route_tool_execution_tape_events(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        &execution_outcome,
    )
    .await?;
    Ok(build_and_ingest_tool_result_memory_summary(
        runtime_state,
        ToolRuntimeExecutionContext {
            principal: route_request_context.principal.as_str(),
            device_id: route_request_context.device_id.as_str(),
            channel: route_request_context.channel.as_deref(),
            session_id,
            run_id,
            execution_backend: crate::execution_backends::ExecutionBackendPreference::LocalSandbox,
            backend_reason_code: "tool_catalog_bridge",
        },
        tool_name,
        success,
        &execution_outcome,
        "route_message_tool_catalog_bridge_result",
    )
    .await)
}

#[allow(clippy::result_large_err, clippy::too_many_arguments)]
async fn append_route_catalog_bridge_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    index_digest: &str,
    output_value: &serde_json::Value,
    success: bool,
) -> Result<(), Status> {
    let event_type = match tool_name {
        TOOL_CATALOG_SEARCH_TOOL_NAME => "tool.catalog_search",
        TOOL_CATALOG_DESCRIBE_TOOL_NAME => "tool.catalog_describe",
        TOOL_CATALOG_INVOKE_TOOL_NAME => "tool.catalog_invoke",
        _ => "tool.catalog_bridge",
    };
    let result_ids = output_value
        .get("results")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|result| result.get("id").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: event_type.to_owned(),
            payload_json: json!({
                "schema_version": 1,
                "proposal_id": proposal_id,
                "tool_name": tool_name,
                "success": success,
                "index_digest": index_digest,
                "result_ids": result_ids,
                "filtered_count": output_value
                    .get("filtered_count")
                    .and_then(serde_json::Value::as_u64),
                "schema_digest": output_value
                    .get("schema_digest")
                    .and_then(serde_json::Value::as_str),
                "error": output_value.get("error"),
            })
            .to_string(),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err, clippy::too_many_arguments)]
async fn append_route_catalog_invoke_lineage_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    target_tool_name: &str,
    schema_digest: &str,
    index_digest: &str,
    normalized_arguments_hash: &str,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool.catalog_invoke.lineage".to_owned(),
            payload_json: json!({
                "schema_version": 1,
                "proposal_id": proposal_id,
                "bridge_tool_name": TOOL_CATALOG_INVOKE_TOOL_NAME,
                "target_tool_name": target_tool_name,
                "schema_digest": schema_digest,
                "index_digest": index_digest,
                "normalized_arguments_hash": normalized_arguments_hash,
            })
            .to_string(),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

/// Records the full proposal/decision/rejection/executed tape sequence for a
/// call that failed catalog validation, so a rejected call replays with the
/// same event shape as an executed one, and returns its summary line.
#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn reject_route_tool_call(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    rejection: ToolCallRejection,
    tape_seq: &mut i64,
) -> Result<String, Status> {
    runtime_state.record_tool_proposal();
    append_tool_proposal_tape_event(
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
    append_tool_decision_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "tool.decision",
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
    append_route_tool_execution_tape_events(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        &execution_outcome,
    )
    .await?;
    Ok(format!("tool={tool_name} success=false error={reason}"))
}

/// Appends canonical result and attestation events plus the legacy routed
/// execution projection consumed by existing transcript readers.
#[allow(clippy::result_large_err)]
async fn append_route_tool_execution_tape_events(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    execution_outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    append_mcp_transport_invocation_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        execution_outcome.attestation.mcp_transport_invocation.as_deref(),
    )
    .await?;
    append_tool_result_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        execution_outcome.success,
        execution_outcome.output_json.as_slice(),
        execution_outcome.error.as_str(),
    )
    .await?;
    append_tool_attestation_tape_event(
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
        execution_outcome.attestation.execution_manifest.as_deref(),
    )
    .await?;
    runtime_state.record_tool_attestation_emitted();

    // INTENTIONAL: usage and ACP transcript consumers still read this legacy
    // projection. Canonical V2 evidence lives in the two events above.
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool.executed".to_owned(),
            payload_json: json!({
                "proposal_id": proposal_id,
                "tool_name": tool_name,
                "success": execution_outcome.success,
                "error": execution_outcome.error,
                "attestation": {
                    "attestation_id": execution_outcome.attestation.attestation_id,
                    "execution_sha256": execution_outcome.attestation.execution_sha256,
                    "executed_at_unix_ms": execution_outcome.attestation.executed_at_unix_ms,
                    "timed_out": execution_outcome.attestation.timed_out,
                    "executor": execution_outcome.attestation.executor,
                    "sandbox_enforcement": execution_outcome.attestation.sandbox_enforcement,
                }
            })
            .to_string(),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}
