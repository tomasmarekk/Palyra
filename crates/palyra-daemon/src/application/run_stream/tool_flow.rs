//! Tool-proposal flow for run streams: intake, approval gates, execution.
//!
//! A proposal moves through catalog validation/normalization, security
//! evaluation, the approval gate (sensitive-tool bypass, cached decision, or
//! interactive prompt with timeout), the policy decision, and finally runtime
//! dispatch. Every stage emits its wire event plus tape row through the
//! `tape` helpers so the run stays replayable. Allowed proposals may execute
//! in bounded parallel groups when classified side-effect safe; cancellation
//! is polled during execution, with drain semantics for tools that must not
//! be dropped mid-flight.

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use palyra_common::{
    qa_fault_injection::{QaFaultAction, QaFaultDirective, QaFaultRecoveryClass},
    redaction::{is_sensitive_key, redact_auth_error, redact_url_segments_in_text, REDACTED},
    runtime_contracts::{
        ArtifactRetentionPolicy, BackpressureOverflowAction, CancellationScopeKind,
        CancellationSettlementOutcome, RunLifecycleHookDecisionKind, RunLifecycleHookPhase,
        RuntimeIdempotencyClass, SideEffectFenceState, SideEffectFenceV1, SideEffectRetryDecision,
        ToolResultArtifactRef, ToolResultProjectionAuditRecord, ToolResultProjectionDecisionKind,
        ToolResultProjectionPolicyKind, ToolResultSensitivity, ToolResultVisibility,
        ToolTurnBudget,
    },
};
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{interval, timeout, MissedTickBehavior},
};
use tonic::{Status, Streaming};
use tracing::{info, warn, Instrument};
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    application::approvals::{
        approval_subject_type_for_tool, build_pending_tool_approval,
        record_approval_requested_journal_event, record_approval_resolved_journal_event,
        resolve_cached_tool_approval_for_proposal,
    },
    application::execution_gate::ToolProposalApprovalState,
    application::side_effect_reconciliation::{
        reconcile_unknown_tool_side_effect, record_side_effect_reconciliation_receipt,
        SideEffectReconciliationBinding, SideEffectReconciliationOutcome,
    },
    application::tool_governance::{
        apply_host_tool_result_middleware, build_tool_call_signature,
        evaluate_before_tool_decision_pipeline, synthetic_tool_result_outcome,
        BeforeToolDecisionInput, BeforeToolDecisionKind, BeforeToolDecisionReport,
        ToolCallSignature, ToolResultMiddlewareReport,
    },
    application::tool_registry::{
        describe_catalog_tool, normalization_audit_tape_payload, projection_policy_for_tool,
        rejection_tape_payload, resolve_catalog_invoke_target, resolve_tool_execution_semantics,
        search_tool_catalog_index, tool_call_rejection_outcome,
        validate_tool_call_against_catalog_snapshot, validate_tool_call_against_model_visible_tool,
        ModelVisibleToolCatalogSnapshot, NormalizedToolCall, ToolArgumentNormalizationAudit,
        ToolCallRejection, ToolCatalogBridgeError, ToolReplaySafetyClass,
        ToolResultProjectionPolicy, TOOL_CATALOG_DESCRIBE_TOOL_NAME, TOOL_CATALOG_INVOKE_TOOL_NAME,
        TOOL_CATALOG_SEARCH_TOOL_NAME,
    },
    application::tool_runtime::{
        artifacts::bounded_tool_result_artifact_content,
        workspace_patch::normalized_workspace_patch_approval_input_json,
        workspace_scope::{
            session_active_workspace_root, workspace_roots_with_run_launch_context_for_agent_source,
        },
    },
    application::tool_security::{
        approval_execution_context_for_backend_selection, evaluate_tool_proposal_security,
        record_tool_proposal_decision_audit_trail, resolve_tool_proposal_decision_for_context,
        ResolvedToolProposalDecision, ToolProposalBackendSelection, ToolProposalSecurityEvaluation,
    },
    gateway::{
        await_tool_approval_response, best_effort_mark_approval_error,
        build_and_ingest_tool_result_memory_summary,
        execute_tool_with_runtime_dispatch_with_cancellation_and_progress,
        record_tool_execution_outcome_metrics, shared_tool_budget, shared_tool_budget_remaining,
        tool_cancellation_requires_execution_drain, GatewayRuntimeState,
        RunStreamToolExecutionOutcome, SharedToolBudget, ToolApprovalOutcome,
        ToolRuntimeDispatchControls, ToolRuntimeExecutionContext, PROCESS_RUNNER_TOOL_NAME,
        SESSIONS_SPAWN_TOOL_NAME, TOOL_APPROVAL_RESPONSE_TIMEOUT,
    },
    journal::{
        ApprovalCreateRequest, ApprovalResolveRequest, OrchestratorTapeAppendRequest,
        SideEffectFenceCleanupOutcomeRequest, ToolEffectObservationCommitRequest,
        ToolResultArtifactCreateRequest,
    },
    orchestrator::{RunLifecycleState, RunStateMachine},
    sandbox_runner::{ProcessProgressEvent, ProcessProgressSink},
    tool_protocol::{build_tool_execution_outcome, denied_execution_outcome, ToolExecutionOutcome},
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};

use super::{
    cancellation::{
        record_run_interrupt_observation, request_persisted_run_interrupt,
        transition_run_stream_to_cancelled,
    },
    flow_control::{
        process_progress_backpressure_policy, LiveCancellationScope, RunInterruptPhase,
        RunStreamFlowControl, PROCESS_PROGRESS_BACKPRESSURE_REASON_CODE,
        PROCESS_PROGRESS_BACKPRESSURE_TAPE_EVENT,
    },
    orchestration::RunStreamHarnessLifecycle,
    tape::{
        redact_run_stream_text, redacted_run_stream_output_json, send_status_with_tape,
        send_tool_approval_request_with_tape, send_tool_approval_response_with_tape,
        send_tool_attestation_with_tape, send_tool_decision_with_tape,
        send_tool_proposal_with_tape, send_tool_result_with_tape, tool_attestation_event,
        tool_attestation_tape_payload, tool_result_event, tool_result_tape_payload,
        ToolAttestationTapePayload, RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE,
    },
};

const MAX_PARALLEL_TOOL_CALLS_PER_GROUP: usize = 4;
const TOOL_PARALLELISM_ENABLED_ENV: &str = "PALYRA_TOOL_PARALLELISM_ENABLED";
const TOOL_RESULT_PROJECTION_POLICY_EVENT: &str = "tool.result.projection_policy";
const TOOL_RESULT_REPLAY_SAFETY_EVENT: &str = "tool.result.replay_safety";
const TOOL_BEFORE_DECISION_EVENT: &str = "tool.before_decision";
const TOOL_RESULT_MIDDLEWARE_EVENT: &str = "tool.result.middleware";
const TOOL_REPAIR_CANDIDATE_DETECTED_EVENT: &str = "tool.repair.candidate_detected";
const TOOL_REPAIR_ACCEPTED_EVENT: &str = "tool.repair.accepted";
const TOOL_REPAIR_REJECTED_EVENT: &str = "tool.repair.rejected";
const TOOL_EFFECT_STARTED_EVENT: &str = "tool_effect_started";

include!("tool_flow/stages.rs");
include!("tool_flow/owner.rs");

type RunStreamProgressSender = mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>;

struct ProcessProgressSlot {
    latest: Option<ProcessProgressEvent>,
    coalesced: u64,
}

struct ProcessProgressReceiver {
    notifications: mpsc::Receiver<()>,
    slot: Arc<std::sync::Mutex<ProcessProgressSlot>>,
}

impl ProcessProgressReceiver {
    async fn recv(&mut self) -> Option<(ProcessProgressEvent, u64)> {
        loop {
            match self.notifications.recv().await {
                Some(()) => {
                    let mut slot =
                        self.slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    if let Some(progress) = slot.latest.take() {
                        let coalesced = std::mem::take(&mut slot.coalesced);
                        return Some((progress, coalesced));
                    }
                }
                None => {
                    let mut slot =
                        self.slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    return slot
                        .latest
                        .take()
                        .map(|progress| (progress, std::mem::take(&mut slot.coalesced)));
                }
            }
        }
    }
}

/// Decision context produced by the proposal preparation pipeline.
#[derive(Debug, Clone)]
pub(crate) struct RunStreamToolProposalPreparation {
    decision: crate::tool_protocol::ToolDecision,
    resolved_session_id: String,
    backend_selection: ToolProposalBackendSelection,
    tool_signature: ToolCallSignature,
    synthetic_outcome: Option<ToolExecutionOutcome>,
    approval_timed_out: bool,
}

/// Host-owned gate state produced before any approval wait begins.
///
/// Keeping this state separate makes the approval suspension point explicit
/// while preserving the established policy, audit, and dispatch paths.
struct RunStreamToolGatePreparation {
    resolved_session_id: String,
    skill_context: Option<crate::gateway::ToolSkillContext>,
    skill_gate_decision: Option<crate::tool_protocol::ToolDecision>,
    approval_subject_id: String,
    proposal_approval_required: bool,
    effective_posture: crate::tool_posture::EffectiveToolPosture,
    backend_selection: ToolProposalBackendSelection,
    tool_signature: ToolCallSignature,
    synthetic_outcome: Option<ToolExecutionOutcome>,
}

/// A fully gated tool proposal that is ready for runtime dispatch.
///
/// Carries the normalized input plus the approval/policy decision; denied
/// proposals are still "executed" to produce a structured denial outcome.
#[derive(Debug, Clone)]
pub(crate) struct RunStreamPreparedToolExecution {
    proposal_id: String,
    tool_name: String,
    input_json: Vec<u8>,
    replay_safety_class: ToolReplaySafetyClass,
    tool_signature: ToolCallSignature,
    decision: crate::tool_protocol::ToolDecision,
    resolved_session_id: String,
    backend_selection: ToolProposalBackendSelection,
}

struct PreparedToolRuntimeExecution<'a> {
    progress_sender: Option<&'a RunStreamProgressSender>,
    runtime_state: &'a Arc<GatewayRuntimeState>,
    request_context: &'a RequestContext,
    run_id: &'a str,
    progress_tape_seq: Option<&'a mut i64>,
    effect_started_tape_seq: Option<i64>,
    prepared: &'a RunStreamPreparedToolExecution,
    remaining_tool_budget: Option<SharedToolBudget>,
    flow_control: RunStreamFlowControl,
    cancellation: LiveCancellationScope,
}

/// Result of preparing one tool proposal.
#[derive(Debug, Clone)]
pub(crate) enum RunStreamToolProposalPreparationOutcome {
    /// The proposal passed intake and is ready for execution.
    Prepared(RunStreamPreparedToolExecution),
    /// Intake rejected the call; the synthetic failure outcome was already
    /// streamed and taped.
    Completed(RunStreamToolExecutionOutcome),
}

/// Result of executing a batch of prepared tool proposals.
#[derive(Debug, Clone)]
pub(crate) enum RunStreamPreparedToolExecutionBatchOutcome {
    /// Outcomes for every proposal, in the original proposal order.
    Completed(Vec<RunStreamToolExecutionOutcome>),
    /// A terminal settlement won mid-batch; the state machine already follows it.
    Terminal(RunLifecycleState),
}

/// Side-effect classification deciding whether a tool may run in parallel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolParallelism {
    /// Mutating or unknown tools; always executed sequentially.
    Never,
    /// Read-only tools safe to run alongside anything.
    ReadOnlySafe,
    /// Read-only process commands; parallel only across disjoint path scopes.
    PathScoped,
    /// Idempotent network fetches (GET/HEAD).
    IdempotentNetwork,
}

/// Provider retry posture for a completed tool result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolReplaySafety {
    ReadOnly,
    PathScopedRead,
    IdempotentNetwork,
    HostSyntheticResult,
    DeniedNoReplay,
    MutatingRequiresGuard,
}

impl ToolReplaySafety {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::PathScopedRead => "path_scoped_read",
            Self::IdempotentNetwork => "idempotent_network",
            Self::HostSyntheticResult => "host_synthetic_result",
            Self::DeniedNoReplay => "denied_no_replay",
            Self::MutatingRequiresGuard => "mutating_requires_guard",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ToolResultReplaySafetyReport {
    schema_version: u8,
    event_type: String,
    proposal_id: String,
    tool_name: String,
    replay_safety: String,
    provider_retry_allowed: bool,
    explicit_guard_required: bool,
    synthetic_result_allowed: bool,
    reason_code: String,
    parallelism: String,
    success: bool,
    timed_out: bool,
    output_sha256: String,
    error_sha256: String,
    attestation_id: String,
}

#[derive(Debug, Clone)]
struct ProjectedToolExecutionOutcome {
    outcome: ToolExecutionOutcome,
    audit: Option<ToolResultProjectionAuditRecord>,
    middleware_report: Option<ToolResultMiddlewareReport>,
}

#[derive(Debug, Clone)]
struct ActiveToolSideEffectFence {
    operation_id: palyra_common::runtime_contracts::RuntimeOperationId,
    generation: palyra_common::runtime_contracts::RuntimeGeneration,
    intent_sha256: String,
    strategy: palyra_common::runtime_contracts::ReconciliationStrategy,
    external_idempotency_key_sha256: Option<String>,
}

#[derive(Debug)]
struct PreparedToolRuntimeOutcome {
    outcome: ToolExecutionOutcome,
    side_effect_fence: Option<ActiveToolSideEffectFence>,
    post_execution_error: Option<Status>,
}

#[derive(Debug)]
enum ToolOutcomeFinalizationError {
    BeforeSettlement(Status),
    SettlementFailed(Status),
    AfterSettlement(Status),
}

impl ToolOutcomeFinalizationError {
    fn into_status(self) -> Status {
        match self {
            Self::BeforeSettlement(error)
            | Self::SettlementFailed(error)
            | Self::AfterSettlement(error) => error,
        }
    }
}

#[derive(Debug)]
struct FinalizedToolExecutionOutcome<T> {
    completed: RunStreamToolExecutionOutcome,
    retained_projection: T,
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
        outcome: PreparedToolRuntimeOutcome,
    },
    Cancelled,
}

/// Prepares and executes a single tool proposal end to end.
///
/// Convenience wrapper over [`prepare_run_stream_tool_proposal_event`]
/// followed by sequential execution; batched callers prepare first and then
/// use [`execute_prepared_run_stream_tool_proposals_ordered`].
///
/// # Errors
///
/// Returns `Status::cancelled` when the client stream drops, journal errors
/// from tape/approval persistence, or internal errors from the runtime
/// dispatch path.
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
    flow_control: &RunStreamFlowControl,
    tape_seq: &mut i64,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
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
        flow_control,
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
                flow_control,
                tape_seq,
                harness_lifecycle,
            )
            .await
        }
        RunStreamToolProposalPreparationOutcome::Completed(outcome) => Ok(outcome),
    }
}

/// Validates, normalizes, and gates a tool proposal without executing it.
///
/// Catalog rejections complete the proposal immediately with a synthetic
/// failure result so the model still receives structured feedback. Otherwise
/// the proposal runs through the security evaluation and approval gate and
/// comes back ready to execute.
///
/// # Errors
///
/// Returns `Status::cancelled` when the client stream drops, an internal
/// invariant error when the active session is missing, or journal errors
/// from tape and approval persistence.
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
    flow_control: &RunStreamFlowControl,
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
    if tool_name == TOOL_CATALOG_SEARCH_TOOL_NAME || tool_name == TOOL_CATALOG_DESCRIBE_TOOL_NAME {
        let bridge_result = if tool_name == TOOL_CATALOG_SEARCH_TOOL_NAME {
            search_tool_catalog_index(tool_catalog_snapshot, normalized_input_json.as_slice())
        } else {
            describe_catalog_tool(tool_catalog_snapshot, normalized_input_json.as_slice())
        };
        let outcome = complete_catalog_bridge_tool_call(
            sender,
            runtime_state,
            run_id,
            proposal_id,
            tool_name,
            normalized_input_json.as_slice(),
            bridge_result,
            tool_catalog_snapshot.index.index_digest.as_str(),
            tape_seq,
        )
        .await?;
        return Ok(RunStreamToolProposalPreparationOutcome::Completed(outcome));
    }
    let (execution_tool_name, execution_input_json) = if tool_name == TOOL_CATALOG_INVOKE_TOOL_NAME
    {
        let target = match resolve_catalog_invoke_target(
            tool_catalog_snapshot,
            normalized_input_json.as_slice(),
        ) {
            Ok(target) => target,
            Err(error) => {
                let outcome = complete_catalog_bridge_tool_call(
                    sender,
                    runtime_state,
                    run_id,
                    proposal_id,
                    tool_name,
                    normalized_input_json.as_slice(),
                    Err(error),
                    tool_catalog_snapshot.index.index_digest.as_str(),
                    tape_seq,
                )
                .await?;
                return Ok(RunStreamToolProposalPreparationOutcome::Completed(outcome));
            }
        };
        let Some(target_tool) =
            tool_catalog_snapshot.indexed_tools.iter().find(|tool| tool.name == target.tool_name)
        else {
            let outcome = complete_catalog_bridge_tool_call(
                sender,
                runtime_state,
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
            .await?;
            return Ok(RunStreamToolProposalPreparationOutcome::Completed(outcome));
        };
        let target_call = match validate_tool_call_against_model_visible_tool(
            tool_catalog_snapshot,
            target_tool,
            target.tool_name.as_str(),
            target.input_json.as_slice(),
        ) {
            Ok(normalized) => normalized,
            Err(error) => {
                let bridge_error = ToolCatalogBridgeError {
                    reason_code: error.reason_code,
                    message: error.message,
                };
                let outcome = complete_catalog_bridge_tool_call(
                    sender,
                    runtime_state,
                    run_id,
                    proposal_id,
                    tool_name,
                    normalized_input_json.as_slice(),
                    Err(bridge_error),
                    tool_catalog_snapshot.index.index_digest.as_str(),
                    tape_seq,
                )
                .await?;
                return Ok(RunStreamToolProposalPreparationOutcome::Completed(outcome));
            }
        };
        if !target_call.audit.steps.is_empty() {
            append_tool_argument_normalization_tape_event(
                runtime_state,
                run_id,
                tape_seq,
                proposal_id,
                target.tool_name.as_str(),
                &target_call.audit,
            )
            .await?;
        }
        append_catalog_invoke_lineage_tape_event(
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

    let RunStreamToolProposalPreparation {
        decision,
        resolved_session_id,
        backend_selection,
        tool_signature,
        synthetic_outcome,
        approval_timed_out: _,
    } = prepare_run_stream_tool_proposal_execution(
        sender,
        stream,
        runtime_state,
        request_context,
        active_session_id,
        session_id,
        run_id,
        proposal_id,
        execution_tool_name.as_str(),
        execution_input_json.as_slice(),
        remaining_tool_budget,
        allow_sensitive_tools,
        approval_cache_generation,
        flow_control,
        tape_seq,
    )
    .await?;

    let replay_safety_class = tool_catalog_snapshot
        .tools
        .iter()
        .chain(tool_catalog_snapshot.indexed_tools.iter())
        .find(|tool| tool.name == execution_tool_name)
        .map_or(ToolReplaySafetyClass::RequiresHumanConfirmation, |tool| tool.replay_safety_class);
    let prepared = RunStreamPreparedToolExecution {
        proposal_id: proposal_id.to_owned(),
        tool_name: execution_tool_name,
        input_json: execution_input_json,
        replay_safety_class,
        tool_signature,
        decision,
        resolved_session_id,
        backend_selection,
    };
    if let Some(outcome) = synthetic_outcome {
        let completed = finalize_prepared_tool_execution_outcome(
            sender,
            runtime_state,
            request_context,
            run_id,
            &prepared,
            outcome,
            None,
            tape_seq,
        )
        .await?;
        return Ok(RunStreamToolProposalPreparationOutcome::Completed(completed));
    }

    Ok(RunStreamToolProposalPreparationOutcome::Prepared(prepared))
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
    if audit_has_repair_steps(audit) {
        append_tool_repair_tape_event(
            runtime_state,
            run_id,
            tape_seq,
            TOOL_REPAIR_CANDIDATE_DETECTED_EVENT,
            json!({
                "schema_version": 1,
                "proposal_id": proposal_id,
                "tool_name": tool_name,
                "raw_json_hash": audit.raw_json_hash.as_str(),
                "repair_step_count": audit.steps.len(),
                "repair_reason_codes": repair_reason_codes(audit),
            }),
        )
        .await?;
        append_tool_repair_tape_event(
            runtime_state,
            run_id,
            tape_seq,
            TOOL_REPAIR_ACCEPTED_EVENT,
            json!({
                "schema_version": 1,
                "proposal_id": proposal_id,
                "tool_name": tool_name,
                "raw_json_hash": audit.raw_json_hash.as_str(),
                "normalized_json_hash": audit.normalized_json_hash.as_str(),
                "repair_step_count": audit.steps.len(),
                "repair_reason_codes": repair_reason_codes(audit),
            }),
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn append_tool_repair_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    event_type: &str,
    payload: Value,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: event_type.to_owned(),
            payload_json: payload.to_string(),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

fn audit_has_repair_steps(audit: &ToolArgumentNormalizationAudit) -> bool {
    audit.steps.iter().any(|step| step.reason_code.contains("_repair"))
}

fn repair_reason_codes(audit: &ToolArgumentNormalizationAudit) -> Vec<&str> {
    audit.steps.iter().map(|step| step.reason_code.as_str()).collect()
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn complete_catalog_bridge_tool_call(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    bridge_result: Result<Value, ToolCatalogBridgeError>,
    index_digest: &str,
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
    let success = bridge_result.is_ok();
    let reason = bridge_result
        .as_ref()
        .map(|_| "catalog bridge query resolved".to_owned())
        .unwrap_or_else(|error| format!("{}: {}", error.reason_code, error.message));
    send_tool_decision_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
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
    append_catalog_bridge_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        CatalogBridgeTapeEvent {
            proposal_id,
            tool_name,
            index_digest,
            output_value: &output_value,
            success,
        },
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
    send_tool_result_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
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
    Ok(RunStreamToolExecutionOutcome::Completed {
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        input_json: input_json.to_vec(),
        outcome: execution_outcome,
    })
}

struct CatalogBridgeTapeEvent<'a> {
    proposal_id: &'a str,
    tool_name: &'a str,
    index_digest: &'a str,
    output_value: &'a Value,
    success: bool,
}

#[allow(clippy::result_large_err)]
async fn append_catalog_bridge_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    event: CatalogBridgeTapeEvent<'_>,
) -> Result<(), Status> {
    let event_type = match event.tool_name {
        TOOL_CATALOG_SEARCH_TOOL_NAME => "tool.catalog_search",
        TOOL_CATALOG_DESCRIBE_TOOL_NAME => "tool.catalog_describe",
        TOOL_CATALOG_INVOKE_TOOL_NAME => "tool.catalog_invoke",
        _ => "tool.catalog_bridge",
    };
    let result_ids = event
        .output_value
        .get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|result| result.get("id").and_then(Value::as_str))
        .collect::<Vec<_>>();
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: event_type.to_owned(),
            payload_json: json!({
                "schema_version": 1,
                "proposal_id": event.proposal_id,
                "tool_name": event.tool_name,
                "success": event.success,
                "index_digest": event.index_digest,
                "result_ids": result_ids,
                "filtered_count": event.output_value.get("filtered_count").and_then(Value::as_u64),
                "schema_digest": event.output_value.get("schema_digest").and_then(Value::as_str),
                "error": event.output_value.get("error"),
            })
            .to_string(),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn append_catalog_invoke_lineage_tape_event(
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
    if rejection.kind.as_str() == "malformed_arguments" {
        append_tool_repair_tape_event(
            runtime_state,
            run_id,
            tape_seq,
            TOOL_REPAIR_REJECTED_EVENT,
            json!({
                "schema_version": 1,
                "proposal_id": proposal_id,
                "tool_name": rejection.tool_name.as_str(),
                "raw_json_hash": rejection.raw_json_hash.as_str(),
                "reason_code": rejection.reason_code.as_str(),
                "kind": rejection.kind.as_str(),
                "snapshot_id": rejection.snapshot_id.as_deref(),
                "catalog_hash": rejection.catalog_hash.as_deref(),
            }),
        )
        .await?;
    }

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
        execution_outcome.attestation.execution_manifest.as_deref(),
    )
    .await?;
    runtime_state.record_tool_attestation_emitted();
    Ok(RunStreamToolExecutionOutcome::Completed {
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        input_json: input_json.to_vec(),
        outcome: execution_outcome,
    })
}

/// Executes prepared proposals, preserving the model's proposal order.
///
/// Adjacent parallel-safe proposals are grouped (up to
/// `MAX_PARALLEL_TOOL_CALLS_PER_GROUP`) and run concurrently when the
/// `PALYRA_TOOL_PARALLELISM_ENABLED` switch is not disabled; everything else
/// runs sequentially. Result events are always finalized in proposal order so
/// the wire stream and tape stay deterministic regardless of task scheduling.
///
/// # Errors
///
/// Returns `Status::cancelled` when the client stream drops, internal errors
/// when a parallel task panics or fails to join, or journal errors from the
/// tape path.
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
    flow_control: &RunStreamFlowControl,
    tape_seq: &mut i64,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
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
                flow_control,
                tape_seq,
                harness_lifecycle,
            )
            .await?
            {
                RunStreamPreparedToolExecutionBatchOutcome::Completed(mut outcomes) => {
                    completed.append(&mut outcomes);
                }
                RunStreamPreparedToolExecutionBatchOutcome::Terminal(state) => {
                    return Ok(RunStreamPreparedToolExecutionBatchOutcome::Terminal(state));
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
                    flow_control,
                    tape_seq,
                    harness_lifecycle,
                )
                .await?
                {
                    RunStreamToolExecutionOutcome::Completed {
                        proposal_id,
                        tool_name,
                        input_json,
                        outcome,
                    } => {
                        completed.push(RunStreamToolExecutionOutcome::Completed {
                            proposal_id,
                            tool_name,
                            input_json,
                            outcome,
                        });
                    }
                    RunStreamToolExecutionOutcome::Terminal(state) => {
                        return Ok(RunStreamPreparedToolExecutionBatchOutcome::Terminal(state));
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
        // Path-scoped tools without a derivable scope key are treated as
        // conflicting (unwrap_or(true)): when we cannot prove disjoint paths,
        // the call must run sequentially.
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

/// Classifies a tool call's parallel safety from its name and input.
///
/// Deny-by-default: anything not explicitly listed is [`ToolParallelism::Never`].
/// HTTP fetches qualify only for idempotent methods, and process runs only for
/// an allowlist of read-only commands.
pub(crate) fn classify_tool_parallelism(tool_name: &str, input_json: &[u8]) -> ToolParallelism {
    match tool_name {
        "palyra.echo"
        | "palyra.sleep"
        | "palyra.memory.status"
        | "palyra.context.inspect"
        | "palyra.memory.search"
        | "palyra.memory.recall"
        | "palyra.memory.session_search"
        | "palyra.session_search"
        | "palyra.routines.query"
        | "palyra.artifact.read"
        | "palyra.image.observe"
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

fn classify_tool_result_replay_safety(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    outcome: &ToolExecutionOutcome,
) -> ToolResultReplaySafetyReport {
    let parallelism = classify_tool_parallelism(tool_name, input_json);
    let synthetic_result_allowed = is_host_synthetic_tool_result(outcome);
    let replay_safety = if synthetic_result_allowed {
        ToolReplaySafety::HostSyntheticResult
    } else if is_policy_denial_outcome(outcome) {
        ToolReplaySafety::DeniedNoReplay
    } else {
        match parallelism {
            ToolParallelism::ReadOnlySafe => ToolReplaySafety::ReadOnly,
            ToolParallelism::PathScoped => ToolReplaySafety::PathScopedRead,
            ToolParallelism::IdempotentNetwork => ToolReplaySafety::IdempotentNetwork,
            ToolParallelism::Never => ToolReplaySafety::MutatingRequiresGuard,
        }
    };
    let provider_retry_allowed = matches!(
        replay_safety,
        ToolReplaySafety::ReadOnly
            | ToolReplaySafety::PathScopedRead
            | ToolReplaySafety::IdempotentNetwork
            | ToolReplaySafety::HostSyntheticResult
    );
    let explicit_guard_required = matches!(replay_safety, ToolReplaySafety::MutatingRequiresGuard);
    let reason_code = match replay_safety {
        ToolReplaySafety::ReadOnly => "tool_replay.read_only_result_retry_allowed",
        ToolReplaySafety::PathScopedRead => "tool_replay.path_scoped_result_retry_allowed",
        ToolReplaySafety::IdempotentNetwork => "tool_replay.idempotent_network_retry_allowed",
        ToolReplaySafety::HostSyntheticResult => "tool_replay.host_synthetic_result_allowed",
        ToolReplaySafety::DeniedNoReplay => "tool_replay.policy_denial_no_replay",
        ToolReplaySafety::MutatingRequiresGuard if outcome.attestation.timed_out => {
            "tool_replay.mutating_timeout_requires_guard"
        }
        ToolReplaySafety::MutatingRequiresGuard => "tool_replay.mutating_result_requires_guard",
    };

    ToolResultReplaySafetyReport {
        schema_version: 1,
        event_type: TOOL_RESULT_REPLAY_SAFETY_EVENT.to_owned(),
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        replay_safety: replay_safety.as_str().to_owned(),
        provider_retry_allowed,
        explicit_guard_required,
        synthetic_result_allowed,
        reason_code: reason_code.to_owned(),
        parallelism: parallelism.as_str().to_owned(),
        success: outcome.success,
        timed_out: outcome.attestation.timed_out,
        output_sha256: crate::sha256_hex(outcome.output_json.as_slice()),
        error_sha256: crate::sha256_hex(outcome.error.as_bytes()),
        attestation_id: outcome.attestation.attestation_id.clone(),
    }
}

fn is_host_synthetic_tool_result(outcome: &ToolExecutionOutcome) -> bool {
    if outcome.success || outcome.attestation.timed_out {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<Value>(outcome.output_json.as_slice()) else {
        return false;
    };
    value.get("host_generated").and_then(Value::as_bool) == Some(true)
        && value.get("kind").and_then(Value::as_str) == Some("synthetic_tool_result")
}

fn is_policy_denial_outcome(outcome: &ToolExecutionOutcome) -> bool {
    if outcome.success {
        return false;
    }
    let normalized_error = outcome.error.to_ascii_lowercase();
    if normalized_error.contains("denied")
        || normalized_error.contains("not allowed")
        || normalized_error.contains("approval")
        || normalized_error.contains("policy")
    {
        return true;
    }
    let Ok(value) = serde_json::from_slice::<Value>(outcome.output_json.as_slice()) else {
        return false;
    };
    let reason_code = value
        .get("reason_code")
        .or_else(|| value.pointer("/error/reason_code"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    reason_code.contains("denied")
        || reason_code.contains("not_allowed")
        || reason_code.contains("approval")
        || reason_code.contains("policy")
}

// Builds a normalized "command:path|path" key for read-only process runs so
// two commands touching the same paths never share a parallel group.
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
    flow_control: &RunStreamFlowControl,
    tape_seq: &mut i64,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
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
    // One shared budget across the group: concurrent executions draw from the
    // same pool, and the caller's counter is re-synced after the group joins.
    let nested_tool_budget = shared_tool_budget(*remaining_tool_budget);
    for (order, prepared) in prepared_tools.into_iter().enumerate() {
        let effect_started_tape_seq = *tape_seq;
        *tape_seq = (*tape_seq).saturating_add(1);
        let runtime_state = Arc::clone(runtime_state);
        let request_context = request_context.clone();
        let run_id = run_id.to_owned();
        let nested_tool_budget = nested_tool_budget.clone();
        let cancellation = flow_control.live_child(
            CancellationScopeKind::ToolExecution,
            tool_execution_timeout(&runtime_state, prepared.tool_name.as_str()),
        )?;
        let flow_control = flow_control.clone();
        join_set.spawn(async move {
            match execute_prepared_tool_runtime(PreparedToolRuntimeExecution {
                progress_sender: None,
                runtime_state: &runtime_state,
                request_context: &request_context,
                run_id: run_id.as_str(),
                progress_tape_seq: None,
                effect_started_tape_seq: Some(effect_started_tape_seq),
                prepared: &prepared,
                remaining_tool_budget: Some(nested_tool_budget),
                flow_control,
                cancellation,
            })
            .await?
            {
                Some(outcome) => {
                    Ok(ParallelToolExecutionTaskOutcome::Completed { order, prepared, outcome })
                }
                None => Ok(ParallelToolExecutionTaskOutcome::Cancelled),
            }
        });
    }

    // Keyed by proposal order: tasks join in completion order, but result
    // events and tape rows must be finalized in the model's proposal order.
    let mut completed =
        BTreeMap::<usize, (RunStreamPreparedToolExecution, PreparedToolRuntimeOutcome)>::new();
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(ParallelToolExecutionTaskOutcome::Completed { order, prepared, outcome })) => {
                completed.insert(order, (prepared, outcome));
            }
            Ok(Ok(ParallelToolExecutionTaskOutcome::Cancelled)) => {
                let settlement = drain_parallel_tool_group_after_cancel(
                    join_set,
                    Duration::from_millis(flow_control.root_context().hard_abort_after_ms),
                )
                .await?;
                let (status, reason) = match settlement {
                    CancellationSettlementOutcome::Graceful => {
                        ("cancelled", "cancel_requested_after_parallel_drain")
                    }
                    CancellationSettlementOutcome::CleanupUnknown => {
                        ("cleanup_unknown", "parallel_tool_cleanup_unknown")
                    }
                    CancellationSettlementOutcome::HardAborted => {
                        return Err(Status::internal(
                            "parallel tool drain reported hard abort without cleanup evidence",
                        ));
                    }
                };
                append_tool_parallel_group_tape_event(
                    runtime_state,
                    run_id,
                    tape_seq,
                    "tool.parallel_group.cancelled",
                    group_id.as_str(),
                    status,
                    &[],
                    Some(reason),
                )
                .await?;
                let effective_state = transition_run_stream_to_cancelled(
                    sender,
                    runtime_state,
                    run_state,
                    run_id,
                    flow_control,
                    tape_seq,
                    harness_lifecycle,
                )
                .await?;
                return Ok(RunStreamPreparedToolExecutionBatchOutcome::Terminal(effective_state));
            }
            Ok(Err(error)) => {
                return Err(drain_parallel_tool_group_after_error(
                    join_set,
                    error,
                    Duration::from_millis(flow_control.root_context().hard_abort_after_ms),
                )
                .await);
            }
            Err(error) => {
                let status = Status::internal(format!(
                    "parallel tool execution task failed to join: {error}"
                ));
                return Err(drain_parallel_tool_group_after_error(
                    join_set,
                    status,
                    Duration::from_millis(flow_control.root_context().hard_abort_after_ms),
                )
                .await);
            }
        }
    }
    *remaining_tool_budget = shared_tool_budget_remaining(&nested_tool_budget);

    let mut finalized = Vec::with_capacity(completed.len());
    for (_, (prepared, execution_outcome)) in completed {
        let completed = finalize_prepared_tool_execution_outcome(
            sender,
            runtime_state,
            request_context,
            run_id,
            &prepared,
            execution_outcome.outcome,
            execution_outcome.side_effect_fence.as_ref(),
            tape_seq,
        )
        .await?;
        if let Some(error) = execution_outcome.post_execution_error {
            return Err(error);
        }
        finalized.push(completed);
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

// INTENTIONAL: waits for sibling tasks instead of aborting them. Aborting
// would drop tool executions mid-flight (half-applied side effects, leaked
// browser/process state). The caller supplies one bounded group settlement
// budget so a hung sibling becomes explicit cleanup_unknown instead of
// blocking cancellation forever.
#[allow(clippy::result_large_err)]
async fn drain_parallel_tool_group_after_cancel(
    mut join_set: JoinSet<Result<ParallelToolExecutionTaskOutcome, Status>>,
    settle_budget: Duration,
) -> Result<CancellationSettlementOutcome, Status> {
    let mut settle_deadline =
        Box::pin(tokio::time::sleep(settle_budget.max(Duration::from_millis(1))));
    loop {
        tokio::select! {
            joined = join_set.join_next() => {
                let Some(joined) = joined else {
                    return Ok(CancellationSettlementOutcome::Graceful);
                };
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
            _ = &mut settle_deadline => {
                supervise_parallel_tool_group_cleanup(join_set);
                return Ok(CancellationSettlementOutcome::CleanupUnknown);
            }
        }
    }
}

fn supervise_parallel_tool_group_cleanup(
    mut join_set: JoinSet<Result<ParallelToolExecutionTaskOutcome, Status>>,
) {
    tokio::spawn(async move {
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(Ok(ParallelToolExecutionTaskOutcome::Completed { .. }))
                | Ok(Ok(ParallelToolExecutionTaskOutcome::Cancelled)) => {}
                Ok(Err(error)) => warn!(
                    status_code = ?error.code(),
                    status_message = %error.message(),
                    "parallel tool cleanup supervisor observed a task error"
                ),
                Err(error) => warn!(
                    error = %error,
                    "parallel tool cleanup supervisor failed to join a task"
                ),
            }
        }
    });
}

async fn drain_parallel_tool_group_after_error(
    join_set: JoinSet<Result<ParallelToolExecutionTaskOutcome, Status>>,
    original_error: Status,
    settle_budget: Duration,
) -> Status {
    let original_code = original_error.code();
    let original_message = original_error.message().to_owned();
    match drain_parallel_tool_group_after_cancel(join_set, settle_budget).await {
        Ok(CancellationSettlementOutcome::Graceful) => original_error,
        Ok(CancellationSettlementOutcome::CleanupUnknown) => Status::new(
            original_code,
            format!("{original_message}; parallel tool cleanup remains unknown"),
        ),
        Ok(CancellationSettlementOutcome::HardAborted) => Status::new(
            original_code,
            format!("{original_message}; invalid hard-abort settlement without cleanup evidence"),
        ),
        Err(drain_error) => Status::new(
            original_code,
            format!(
                "{original_message}; additional parallel tool drain error: {}",
                drain_error.message()
            ),
        ),
    }
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
    flow_control: &RunStreamFlowControl,
    tape_seq: &mut i64,
) -> Result<RunStreamToolProposalPreparation, Status> {
    let prepared_gate = prepare_run_stream_tool_gate_without_approval(
        sender,
        runtime_state,
        request_context,
        active_session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
        tape_seq,
    )
    .await?;
    resolve_run_stream_tool_gate_approval(
        sender,
        stream,
        runtime_state,
        request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
        remaining_tool_budget,
        allow_sensitive_tools,
        approval_cache_generation,
        flow_control,
        tape_seq,
        prepared_gate,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn prepare_run_stream_tool_gate_without_approval(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    tape_seq: &mut i64,
) -> Result<RunStreamToolGatePreparation, Status> {
    let resolved_session_id = active_session_id.ok_or_else(|| {
        Status::internal(
            "run stream internal invariant violated: missing session_id while preparing tool proposal",
        )
    })?;
    let ToolProposalSecurityEvaluation {
        skill_context,
        mut skill_gate_decision,
        approval_subject_id,
        mut proposal_approval_required,
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
    let (hook_decision, hook_reason) = dispatch_before_tool_hook_if_enabled(
        runtime_state,
        run_id,
        proposal_id,
        tool_name,
        input_json,
    )
    .await?;
    let tool_signature = build_tool_call_signature(tool_name, input_json);
    let guardrail_decision = runtime_state.before_tool_guardrail_decision(run_id, &tool_signature);
    let before_tool_report = evaluate_before_tool_decision_pipeline(BeforeToolDecisionInput {
        tool_name,
        normalized_input_json: input_json,
        hook_decision,
        hook_reason: hook_reason.as_deref(),
        guardrail_decision: guardrail_decision.clone(),
    });
    append_before_tool_decision_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        &before_tool_report,
    )
    .await?;
    let mut synthetic_outcome = None;
    match before_tool_report.final_decision {
        BeforeToolDecisionKind::RequireApproval => {
            proposal_approval_required = true;
        }
        BeforeToolDecisionKind::Block | BeforeToolDecisionKind::FailRun => {
            proposal_approval_required = false;
            skill_gate_decision = Some(crate::tool_protocol::ToolDecision {
                allowed: false,
                reason: format!(
                    "inline runtime hook blocked tool={tool_name}; reason_code={}; {}",
                    before_tool_report.final_reason_code,
                    hook_reason
                        .as_deref()
                        .unwrap_or("hook returned a terminal before-tool decision")
                ),
                approval_required: false,
                policy_enforced: true,
            });
        }
        BeforeToolDecisionKind::SynthesizeResult => {
            proposal_approval_required = false;
            let synthetic_result =
                guardrail_decision.as_ref().and_then(|decision| decision.synthetic_result.as_ref());
            if let Some(result) = synthetic_result {
                synthetic_outcome =
                    Some(synthetic_tool_result_outcome(proposal_id, tool_name, input_json, result));
            }
            skill_gate_decision = Some(crate::tool_protocol::ToolDecision {
                allowed: false,
                reason: format!(
                    "tool guardrail synthesized host result for tool={tool_name}; reason_code={}",
                    before_tool_report.final_reason_code
                ),
                approval_required: false,
                policy_enforced: true,
            });
        }
        BeforeToolDecisionKind::Allow
        | BeforeToolDecisionKind::RequireReread
        | BeforeToolDecisionKind::RequireSmallerPatch => {}
    }
    Ok(RunStreamToolGatePreparation {
        resolved_session_id: resolved_session_id.to_owned(),
        skill_context,
        skill_gate_decision,
        approval_subject_id,
        proposal_approval_required,
        effective_posture,
        backend_selection,
        tool_signature,
        synthetic_outcome,
    })
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn resolve_run_stream_tool_gate_approval(
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
    remaining_tool_budget: &mut u32,
    allow_sensitive_tools: bool,
    approval_cache_generation: Option<u64>,
    flow_control: &RunStreamFlowControl,
    tape_seq: &mut i64,
    prepared_gate: RunStreamToolGatePreparation,
) -> Result<RunStreamToolProposalPreparation, Status> {
    let RunStreamToolGatePreparation {
        resolved_session_id,
        skill_context,
        skill_gate_decision,
        approval_subject_id,
        proposal_approval_required,
        effective_posture,
        backend_selection,
        tool_signature,
        synthetic_outcome,
    } = prepared_gate;
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
        flow_control,
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
    let approval_timed_out = approval_outcome
        .as_ref()
        .is_some_and(|outcome| outcome.decision == crate::journal::ApprovalDecision::Timeout);
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
        resolved_session_id.as_str(),
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
        resolved_session_id,
        backend_selection,
        tool_signature,
        synthetic_outcome,
        approval_timed_out,
    })
}

#[allow(clippy::result_large_err)]
async fn dispatch_before_tool_hook_if_enabled(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> Result<(Option<BeforeToolDecisionKind>, Option<String>), Status> {
    if !runtime_state.config.feature_rollouts.inline_runtime_hooks.enabled {
        return Ok((None, None));
    }
    let report = crate::hooks::dispatch_named_event_with_report(
        Arc::clone(runtime_state),
        &runtime_state.config.tool_call.wasm_runtime,
        Duration::from_millis(runtime_state.config.tool_call.execution_timeout_ms),
        RunLifecycleHookPhase::BeforeTool.event_name(),
        json!({
            "schema_version": 1,
            "run_id": run_id,
            "proposal_id": proposal_id,
            "tool_name": tool_name,
            "input_bytes": input_json.len(),
            "input_sha256": crate::sha256_hex(input_json),
            "redaction_level": "hash_only_tool_arguments",
        }),
    )
    .await
    .map_err(|error| {
        Status::failed_precondition(format!("inline before-tool hook dispatch failed: {error}"))
    })?;
    let Some(resolution) = report.lifecycle_resolution else {
        return Ok((None, None));
    };
    let decision = match resolution.selected.kind {
        RunLifecycleHookDecisionKind::RequestApproval => BeforeToolDecisionKind::RequireApproval,
        RunLifecycleHookDecisionKind::Block => BeforeToolDecisionKind::Block,
        RunLifecycleHookDecisionKind::FailRun => BeforeToolDecisionKind::FailRun,
        RunLifecycleHookDecisionKind::Continue
        | RunLifecycleHookDecisionKind::Annotate
        | RunLifecycleHookDecisionKind::TransformPreview => return Ok((None, None)),
    };
    let reason = resolution.selected.reason.clone().unwrap_or_else(|| {
        format!("hook {} selected {}", resolution.selected.hook_id, decision.as_str())
    });
    Ok((Some(decision), Some(reason)))
}

#[allow(clippy::result_large_err)]
async fn append_before_tool_decision_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    report: &BeforeToolDecisionReport,
) -> Result<(), Status> {
    let payload_json = serde_json::to_string(&json!({
        "proposal_id": proposal_id,
        "report": report,
    }))
    .map_err(|error| {
        Status::internal(format!("failed to serialize before-tool decision report: {error}"))
    })?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: TOOL_BEFORE_DECISION_EVENT.to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
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
    flow_control: &RunStreamFlowControl,
    tape_seq: &mut i64,
) -> Result<Option<ToolApprovalOutcome>, Status> {
    // Approval gate precedence: (1) explicit allow-sensitive-tools bypass,
    // (2) cached session-scoped decision, (3) no approval needed, (4)
    // interactive prompt with a hard response timeout. Every resolved outcome
    // is echoed to the stream and tape so replay shows who allowed what.
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

    let approval_input_json_override = if tool_name == "palyra.fs.apply_patch" {
        normalized_workspace_patch_approval_input_json(
            runtime_state,
            request_context.principal.as_str(),
            request_context.channel.as_deref(),
            session_id,
            run_id,
            input_json,
        )
        .await
    } else {
        None
    };
    let approval_input_json = approval_input_json_override.as_deref().unwrap_or(input_json);
    let pending_approval = build_pending_tool_approval(
        tool_name,
        skill_context,
        approval_input_json,
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
        approval_input_json,
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

    let _interrupt_phase = flow_control.enter_interrupt_phase(RunInterruptPhase::Approval);
    let mut approval_cancellation = flow_control
        .live_child(CancellationScopeKind::ApprovalWait, TOOL_APPROVAL_RESPONSE_TIMEOUT)?;
    let approval_timeout =
        RunStreamFlowControl::remaining_for_new_work(approval_cancellation.context())?;
    let approval_response = timeout(
        approval_timeout,
        await_tool_approval_response(
            runtime_state,
            stream,
            session_id,
            run_id,
            proposal_id,
            pending_approval.approval_id.as_str(),
        ),
    );
    tokio::pin!(approval_response);
    let approval_result = tokio::select! {
        biased;
        reason = approval_cancellation.cancelled() => {
            flow_control.request_cancel(reason);
            Ok(Err(Status::cancelled("approval wait cancelled by run authority")))
        }
        result = &mut approval_response => result,
    };
    let response = match approval_result {
        Ok(Ok(value)) => value,
        Ok(Err(error)) if error.code() == tonic::Code::Cancelled => {
            request_persisted_run_interrupt(runtime_state, run_id, flow_control).await?;
            record_run_interrupt_observation(runtime_state, flow_control);
            runtime_state.record_run_stream_approval_cancelled();
            ToolApprovalOutcome {
                approval_id: pending_approval.approval_id.clone(),
                approved: false,
                reason: "approval_cancelled_by_run".to_owned(),
                decision: crate::journal::ApprovalDecision::Error,
                decision_scope: crate::journal::ApprovalDecisionScope::Once,
                decision_scope_ttl_ms: None,
            }
        }
        Ok(Err(error)) => ToolApprovalOutcome {
            approval_id: pending_approval.approval_id.clone(),
            approved: false,
            reason: format!("approval_response_error: {}", error.message()),
            decision: crate::journal::ApprovalDecision::Error,
            decision_scope: crate::journal::ApprovalDecisionScope::Once,
            decision_scope_ttl_ms: None,
        },
        // Fail closed: an expired approval window denies the tool call rather
        // than leaving the proposal pending forever.
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

    // Generation-guarded cache write: if the session approval cache was reset
    // while this prompt was pending, the stale decision must not be cached.
    if response.reason != "approval_cancelled_by_run" {
        runtime_state.remember_tool_approval_if_generation(
            request_context,
            session_id,
            approval_subject_id,
            &response,
            approval_cache_generation,
        );
    }
    Ok(Some(response))
}

fn tool_execution_timeout(runtime_state: &GatewayRuntimeState, tool_name: &str) -> Duration {
    let configured = Duration::from_millis(runtime_state.config.tool_call.execution_timeout_ms);
    if tool_name == PROCESS_RUNNER_TOOL_NAME {
        configured.max(Duration::from_secs(1))
    } else {
        configured
    }
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

fn process_progress_channel_for_tool(
    tool_name: &str,
    enabled: bool,
) -> (Option<ProcessProgressSink>, Option<ProcessProgressReceiver>) {
    if !enabled || tool_name != PROCESS_RUNNER_TOOL_NAME {
        return (None, None);
    }
    let policy = process_progress_backpressure_policy()
        .expect("process progress backpressure policy is a validated compile-time contract");
    debug_assert_eq!(policy.overflow_action, BackpressureOverflowAction::CoalesceProgress);
    // One queued notification plus a mutex-held latest snapshot implements
    // latest-value coalescing without an unbounded producer task backlog.
    let (sender, receiver) = mpsc::channel(policy.capacity);
    let slot = Arc::new(std::sync::Mutex::new(ProcessProgressSlot { latest: None, coalesced: 0 }));
    let slot_for_sink = Arc::clone(&slot);
    let sink: ProcessProgressSink = Arc::new(move |progress| {
        let mut slot = slot_for_sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.latest.replace(progress).is_some() {
            slot.coalesced = slot.coalesced.saturating_add(1);
        }
        drop(slot);
        let _ = sender.try_send(());
    });
    (Some(sink), Some(ProcessProgressReceiver { notifications: receiver, slot }))
}

#[allow(clippy::result_large_err)]
async fn send_process_progress_status_with_tape(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    progress: &ProcessProgressEvent,
) -> Result<(), Status> {
    let message = process_progress_status_message(proposal_id, progress);
    send_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::InProgress,
        message.as_str(),
    )
    .await
}

#[allow(clippy::result_large_err)]
async fn append_process_progress_backpressure_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    coalesced: u64,
) -> Result<(), Status> {
    if coalesced == 0 {
        return Ok(());
    }
    let policy = process_progress_backpressure_policy()
        .expect("process progress backpressure policy is a validated compile-time contract");
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: PROCESS_PROGRESS_BACKPRESSURE_TAPE_EVENT.to_owned(),
            payload_json: json!({
                "schema_version": 1,
                "boundary": "process_progress",
                "overflow_action": policy.overflow_action.as_str(),
                "coalesced_count": coalesced,
                "capacity": policy.capacity,
                "reason_code": PROCESS_PROGRESS_BACKPRESSURE_REASON_CODE,
            })
            .to_string(),
        })
        .await?;
    *tape_seq = tape_seq.saturating_add(1);
    Ok(())
}

fn process_progress_status_message(proposal_id: &str, progress: &ProcessProgressEvent) -> String {
    serde_json::to_string(&json!({
        "event": "tool.process.progress",
        "proposal_id": proposal_id,
        "pid": progress.pid,
        "elapsed_ms": progress.elapsed_ms,
        "stdout_bytes": progress.stdout_bytes,
        "stderr_bytes": progress.stderr_bytes,
        "stdout_tail": progress.stdout_tail,
        "stderr_tail": progress.stderr_tail,
        "last_output_at_ms": progress.last_output_at_ms,
    }))
    .unwrap_or_else(|_| {
        format!(
            "tool.process.progress proposal_id={proposal_id} pid={} elapsed_ms={}",
            progress.pid, progress.elapsed_ms
        )
    })
}

#[allow(clippy::result_large_err)]
pub(crate) async fn append_tool_effect_started_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: i64,
    proposal_id: &str,
    tool_name: &str,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: tape_seq,
            event_type: TOOL_EFFECT_STARTED_EVENT.to_owned(),
            payload_json: json!({
                "proposal_id": proposal_id,
                "tool_name": tool_name,
            })
            .to_string(),
        })
        .await
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
    flow_control: &RunStreamFlowControl,
    tape_seq: &mut i64,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    let nested_tool_budget = shared_tool_budget(*remaining_tool_budget);
    let cancellation = flow_control.live_child(
        CancellationScopeKind::ToolExecution,
        tool_execution_timeout(runtime_state, prepared.tool_name.as_str()),
    )?;
    let execution_outcome = match execute_prepared_tool_runtime(PreparedToolRuntimeExecution {
        progress_sender: Some(sender),
        runtime_state,
        request_context,
        run_id,
        progress_tape_seq: Some(tape_seq),
        effect_started_tape_seq: None,
        prepared: &prepared,
        remaining_tool_budget: Some(nested_tool_budget.clone()),
        flow_control: flow_control.clone(),
        cancellation,
    })
    .await?
    {
        Some(outcome) => outcome,
        None => {
            let effective_state = transition_run_stream_to_cancelled(
                sender,
                runtime_state,
                run_state,
                run_id,
                flow_control,
                tape_seq,
                harness_lifecycle,
            )
            .await?;
            return Ok(RunStreamToolExecutionOutcome::Terminal(effective_state));
        }
    };
    *remaining_tool_budget = shared_tool_budget_remaining(&nested_tool_budget);
    let finalized = finalize_prepared_tool_execution_outcome(
        sender,
        runtime_state,
        request_context,
        run_id,
        &prepared,
        execution_outcome.outcome,
        execution_outcome.side_effect_fence.as_ref(),
        tape_seq,
    )
    .await?;
    if let Some(error) = execution_outcome.post_execution_error {
        return Err(error);
    }
    Ok(finalized)
}

#[allow(clippy::result_large_err)]
async fn execute_prepared_tool_runtime(
    execution: PreparedToolRuntimeExecution<'_>,
) -> Result<Option<PreparedToolRuntimeOutcome>, Status> {
    let PreparedToolRuntimeExecution {
        progress_sender,
        runtime_state,
        request_context,
        run_id,
        mut progress_tape_seq,
        effect_started_tape_seq,
        prepared,
        remaining_tool_budget,
        flow_control,
        mut cancellation,
    } = execution;
    let _interrupt_phase = flow_control.enter_interrupt_phase(RunInterruptPhase::Tool);
    if !cancellation.permits_new_work(crate::gateway::current_unix_ms()) {
        record_run_interrupt_observation(runtime_state, &flow_control);
        return Ok(None);
    }
    if !prepared.decision.allowed {
        return Ok(Some(PreparedToolRuntimeOutcome {
            outcome: denied_execution_outcome(
                prepared.proposal_id.as_str(),
                prepared.tool_name.as_str(),
                prepared.input_json.as_slice(),
                prepared.decision.reason.as_str(),
            ),
            side_effect_fence: None,
            post_execution_error: None,
        }));
    }

    if runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await? {
        request_persisted_run_interrupt(runtime_state, run_id, &flow_control).await?;
        record_run_interrupt_observation(runtime_state, &flow_control);
        return Ok(None);
    }

    runtime_state.record_tool_execution_attempt();
    let started_at = Instant::now();
    let mut cancel_poll = interval(Duration::from_millis(100));
    cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut must_drain_execution_after_cancel =
        tool_cancellation_requires_execution_drain(prepared.tool_name.as_str());
    let active_side_effect_fence =
        prepare_tool_side_effect_fence(runtime_state, run_id, prepared).await?;
    must_drain_execution_after_cancel |= active_side_effect_fence.is_some();
    let cancellation_requested = Arc::new(AtomicBool::new(false));
    let tool_span = tracing::info_span!(
        "tool.call",
        run_id = %run_id,
        tool_call_id = %prepared.proposal_id,
        tool_name = %prepared.tool_name,
        execution_surface = "run_stream",
        status = tracing::field::Empty,
    );
    let process_cancellation = if prepared.tool_name == PROCESS_RUNNER_TOOL_NAME {
        Some(flow_control.child_from(
            cancellation.context(),
            CancellationScopeKind::Process,
            tool_execution_timeout(runtime_state, prepared.tool_name.as_str()),
        )?)
    } else {
        None
    };
    let (process_progress_sink, mut process_progress_rx) =
        process_progress_channel_for_tool(prepared.tool_name.as_str(), progress_sender.is_some());
    apply_tool_fault(runtime_state, "tool.before_effect", prepared.proposal_id.as_str())?;
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
    let effect_started_tape_result = if let Some(seq) = effect_started_tape_seq {
        append_tool_effect_started_tape_event(
            runtime_state,
            run_id,
            seq,
            prepared.proposal_id.as_str(),
            prepared.tool_name.as_str(),
        )
        .await
    } else if let Some(tape_seq) = progress_tape_seq.as_deref_mut() {
        let result = append_tool_effect_started_tape_event(
            runtime_state,
            run_id,
            *tape_seq,
            prepared.proposal_id.as_str(),
            prepared.tool_name.as_str(),
        )
        .await;
        if result.is_ok() {
            *tape_seq = (*tape_seq).saturating_add(1);
        }
        result
    } else {
        Err(Status::internal("tool effect start requires an assigned durable tape sequence"))
    };
    if let Err(error) = effect_started_tape_result {
        if active_side_effect_fence.is_some() {
            if let Err(settlement_error) =
                mark_tool_side_effect_unknown(runtime_state, active_side_effect_fence.as_ref())
                    .await
            {
                warn!(
                    run_id = %run_id,
                    proposal_id = %prepared.proposal_id,
                    tool_name = %prepared.tool_name,
                    error = %settlement_error,
                    "failed to mark tool effect unknown after start-evidence append failure"
                );
            }
        }
        return Err(error);
    }
    let execution_deadline = RunStreamFlowControl::remaining_for_new_work(
        process_cancellation.as_ref().unwrap_or(cancellation.context()),
    )?;
    let mut execution_deadline_sleep = Box::pin(tokio::time::sleep(execution_deadline));
    let execution_runtime_state = Arc::clone(runtime_state);
    let execution_principal = request_context.principal.clone();
    let execution_device_id = request_context.device_id.clone();
    let execution_channel = request_context.channel.clone();
    let execution_session_id = prepared.resolved_session_id.clone();
    let execution_run_id = run_id.to_owned();
    let execution_proposal_id = prepared.proposal_id.clone();
    let execution_tool_name = prepared.tool_name.clone();
    let execution_input_json = prepared.input_json.clone();
    let execution_backend = prepared.backend_selection.resolution.resolved;
    let execution_backend_reason = prepared.backend_selection.resolution.reason_code.clone();
    let execution_cancellation =
        process_cancellation.clone().unwrap_or_else(|| cancellation.context().clone());
    let execution_cancellation_requested = Arc::clone(&cancellation_requested);
    let child_task_parent_context = flow_control.root_context().clone();
    let mut execution_future = Box::pin(
        async move {
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
                execution_input_json.as_slice(),
                ToolRuntimeDispatchControls {
                    remaining_tool_budget,
                    cancellation_requested: Some(execution_cancellation_requested),
                    process_progress_sink,
                    cancellation_context: Some(execution_cancellation),
                    child_task_parent_context: Some(child_task_parent_context),
                },
            )
            .await
        }
        .instrument(tool_span),
    );
    let mut post_start_error = None;
    // The execution future is created once and pinned outside the select
    // loop, so losing a select race to the cancel poll never drops execution
    // progress (cancel-safe polling of `&mut future`).
    let outcome = loop {
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                flow_control.request_cancel(reason);
                record_run_interrupt_observation(runtime_state, &flow_control);
                if must_drain_execution_after_cancel {
                    cancellation_requested.store(true, Ordering::Relaxed);
                    post_start_error = Some(Status::cancelled(
                        "tool execution completed after cancellation was requested",
                    ));
                    match timeout(
                        Duration::from_millis(cancellation.context().hard_abort_after_ms.max(1)),
                        &mut execution_future,
                    )
                    .await
                    {
                        Ok(outcome) => break outcome,
                        Err(_) => {
                            supervise_serial_tool_cleanup(
                                execution_future,
                                SerialToolCleanupSupervisor {
                                    runtime_state: Arc::clone(runtime_state),
                                    fence: active_side_effect_fence.clone(),
                                    run_id: run_id.to_owned(),
                                    proposal_id: prepared.proposal_id.clone(),
                                    tool_name: prepared.tool_name.clone(),
                                    decision_allowed: prepared.decision.allowed,
                                    started_at,
                                },
                            );
                            return Ok(None);
                        }
                    }
                }
                // Read-only and deterministic-idempotent tools may be
                // abandoned safely; mutating tools are always drained.
                return Ok(None);
            }
            result = &mut execution_future => {
                break result;
            }
            _ = &mut execution_deadline_sleep => {
                runtime_state.record_run_stream_tool_deadline_exceeded();
                cancellation_requested.store(true, Ordering::Relaxed);
                if !must_drain_execution_after_cancel {
                    return Err(Status::deadline_exceeded(format!(
                        "tool execution deadline exceeded for {}",
                        prepared.tool_name
                    )));
                }
                post_start_error = Some(Status::deadline_exceeded(format!(
                    "tool execution deadline exceeded for {}",
                    prepared.tool_name
                )));
                match timeout(
                    Duration::from_millis(cancellation.context().hard_abort_after_ms.max(1)),
                    &mut execution_future,
                )
                .await
                {
                    Ok(outcome) => break outcome,
                    Err(_) => {
                        supervise_serial_tool_cleanup(
                            execution_future,
                            SerialToolCleanupSupervisor {
                                runtime_state: Arc::clone(runtime_state),
                                fence: active_side_effect_fence.clone(),
                                run_id: run_id.to_owned(),
                                proposal_id: prepared.proposal_id.clone(),
                                tool_name: prepared.tool_name.clone(),
                                decision_allowed: prepared.decision.allowed,
                                started_at,
                            },
                        );
                        return Err(Status::deadline_exceeded(format!(
                            "tool execution exceeded the hard boundary for {}; cleanup ownership transferred and durable uncertainty is pending",
                            prepared.tool_name
                        )));
                    }
                }
            }
            progress = async {
                match process_progress_rx.as_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => None,
                }
            }, if process_progress_rx.is_some() => {
                match progress {
                    Some((progress, coalesced)) => {
                        runtime_state.record_run_stream_progress_coalesced(coalesced);
                        let tape_seq = progress_tape_seq
                            .as_deref_mut()
                            .expect("progress receiver requires tape sequence");
                        if let Err(error) = append_process_progress_backpressure_tape_event(
                            runtime_state,
                            run_id,
                            tape_seq,
                            coalesced,
                        )
                        .await
                        {
                            if must_drain_execution_after_cancel {
                                cancellation_requested.store(true, Ordering::Relaxed);
                                post_start_error = Some(error);
                                match timeout(
                                    Duration::from_millis(
                                        cancellation.context().hard_abort_after_ms.max(1),
                                    ),
                                    &mut execution_future,
                                )
                                .await
                                {
                                    Ok(outcome) => break outcome,
                                    Err(_) => {
                                        supervise_serial_tool_cleanup(
                                            execution_future,
                                            SerialToolCleanupSupervisor {
                                                runtime_state: Arc::clone(runtime_state),
                                                fence: active_side_effect_fence.clone(),
                                                run_id: run_id.to_owned(),
                                                proposal_id: prepared.proposal_id.clone(),
                                                tool_name: prepared.tool_name.clone(),
                                                decision_allowed: prepared.decision.allowed,
                                                started_at,
                                            },
                                        );
                                        return Err(post_start_error
                                            .take()
                                            .expect("backpressure persistence error should remain available"));
                                    }
                                }
                            }
                            return Err(error);
                        }
                        if let Err(error) = send_process_progress_status_with_tape(
                            progress_sender.expect("progress receiver requires sender"),
                            runtime_state,
                            run_id,
                            tape_seq,
                            prepared.proposal_id.as_str(),
                            &progress,
                        )
                        .await
                        {
                            if must_drain_execution_after_cancel {
                                cancellation_requested.store(true, Ordering::Relaxed);
                                post_start_error = Some(error);
                                match timeout(
                                    Duration::from_millis(
                                        cancellation.context().hard_abort_after_ms.max(1),
                                    ),
                                    &mut execution_future,
                                )
                                .await
                                {
                                    Ok(outcome) => break outcome,
                                    Err(_) => {
                                        supervise_serial_tool_cleanup(
                                            execution_future,
                                            SerialToolCleanupSupervisor {
                                                runtime_state: Arc::clone(runtime_state),
                                                fence: active_side_effect_fence.clone(),
                                                run_id: run_id.to_owned(),
                                                proposal_id: prepared.proposal_id.clone(),
                                                tool_name: prepared.tool_name.clone(),
                                                decision_allowed: prepared.decision.allowed,
                                                started_at,
                                            },
                                        );
                                        return Err(post_start_error
                                            .take()
                                            .expect("progress delivery error should remain available"));
                                    }
                                }
                            }
                            return Err(error);
                        }
                    }
                    None => {
                        process_progress_rx = None;
                    }
                }
            }
            _ = cancel_poll.tick() => {
                match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
                    Ok(true) => {
                        request_persisted_run_interrupt(runtime_state, run_id, &flow_control)
                            .await?;
                        record_run_interrupt_observation(runtime_state, &flow_control);
                        if must_drain_execution_after_cancel {
                            cancellation_requested.store(true, Ordering::Relaxed);
                            post_start_error = Some(Status::cancelled(
                                "tool execution completed after cancellation was requested",
                            ));
                            match timeout(
                                Duration::from_millis(
                                    cancellation.context().hard_abort_after_ms.max(1),
                                ),
                                &mut execution_future,
                            )
                            .await
                            {
                                Ok(outcome) => break outcome,
                                Err(_) => {
                                    supervise_serial_tool_cleanup(
                                        execution_future,
                                        SerialToolCleanupSupervisor {
                                            runtime_state: Arc::clone(runtime_state),
                                            fence: active_side_effect_fence.clone(),
                                            run_id: run_id.to_owned(),
                                            proposal_id: prepared.proposal_id.clone(),
                                            tool_name: prepared.tool_name.clone(),
                                            decision_allowed: prepared.decision.allowed,
                                            started_at,
                                        },
                                    );
                                    return Ok(None);
                                }
                            }
                        }
                        // Read-only and deterministic-idempotent tools may be
                        // abandoned safely; mutating tools are always drained.
                        return Ok(None);
                    }
                    Ok(false) => {}
                    Err(error) => {
                        if must_drain_execution_after_cancel {
                            cancellation_requested.store(true, Ordering::Relaxed);
                            post_start_error = Some(error);
                            match timeout(
                                Duration::from_millis(
                                    cancellation.context().hard_abort_after_ms.max(1),
                                ),
                                &mut execution_future,
                            )
                            .await
                            {
                                Ok(outcome) => break outcome,
                                Err(_) => {
                                    supervise_serial_tool_cleanup(
                                        execution_future,
                                        SerialToolCleanupSupervisor {
                                            runtime_state: Arc::clone(runtime_state),
                                            fence: active_side_effect_fence.clone(),
                                            run_id: run_id.to_owned(),
                                            proposal_id: prepared.proposal_id.clone(),
                                            tool_name: prepared.tool_name.clone(),
                                            decision_allowed: prepared.decision.allowed,
                                            started_at,
                                        },
                                    );
                                    return Err(post_start_error
                                        .take()
                                        .expect("cancellation polling error should remain available"));
                                }
                            }
                        }
                        return Err(error);
                    }
                }
            }
        }
    };
    if let (Some(fence), Some(tape_seq)) = (active_side_effect_fence.as_ref(), progress_tape_seq) {
        if let Err(error) = record_side_effect_reconciliation_receipt(
            runtime_state,
            run_id,
            tape_seq,
            prepared.proposal_id.as_str(),
            prepared.tool_name.as_str(),
            SideEffectReconciliationBinding {
                operation_id: &fence.operation_id,
                generation: fence.generation,
                intent_sha256: fence.intent_sha256.as_str(),
                strategy: fence.strategy,
                external_idempotency_key_sha256: fence.external_idempotency_key_sha256.as_deref(),
            },
            &outcome,
        )
        .await
        {
            if let Err(settlement_error) =
                mark_tool_side_effect_unknown(runtime_state, active_side_effect_fence.as_ref())
                    .await
            {
                warn!(
                    run_id = %run_id,
                    proposal_id = %prepared.proposal_id,
                    tool_name = %prepared.tool_name,
                    error = %settlement_error,
                    "failed to mark tool effect unknown after reconciliation receipt persistence failure"
                );
            }
            return Err(error);
        }
    }
    if let Err(error) = apply_tool_fault(
        runtime_state,
        "tool.after_effect_before_ack",
        prepared.proposal_id.as_str(),
    ) {
        if let Err(settlement_error) =
            mark_tool_side_effect_unknown(runtime_state, active_side_effect_fence.as_ref()).await
        {
            warn!(
                run_id = %run_id,
                proposal_id = %prepared.proposal_id,
                tool_name = %prepared.tool_name,
                error = %settlement_error,
                "failed to mark tool effect unknown after acknowledgement fault"
            );
        }
        return Err(error);
    }
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
    Ok(Some(PreparedToolRuntimeOutcome {
        outcome,
        side_effect_fence: active_side_effect_fence,
        post_execution_error: post_start_error,
    }))
}

struct SerialToolCleanupSupervisor {
    runtime_state: Arc<GatewayRuntimeState>,
    fence: Option<ActiveToolSideEffectFence>,
    run_id: String,
    proposal_id: String,
    tool_name: String,
    decision_allowed: bool,
    started_at: Instant,
}

fn supervise_serial_tool_cleanup<F>(
    execution_future: Pin<Box<F>>,
    supervisor: SerialToolCleanupSupervisor,
) where
    F: Future<Output = ToolExecutionOutcome> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) =
            mark_tool_side_effect_unknown(&supervisor.runtime_state, supervisor.fence.as_ref())
                .await
        {
            warn!(
                run_id = %supervisor.run_id,
                proposal_id = %supervisor.proposal_id,
                tool_name = %supervisor.tool_name,
                error = %error,
                "serial tool cleanup supervisor failed to persist effect uncertainty"
            );
            return;
        }
        let outcome = execution_future.await;
        if let Err(error) = record_tool_side_effect_cleanup_outcome(
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
                "serial tool cleanup supervisor failed to record the late effect outcome"
            );
        }
        record_tool_execution_outcome_metrics(
            &supervisor.runtime_state,
            crate::gateway::ToolExecutionTraceContext {
                run_id: supervisor.run_id.as_str(),
                proposal_id: supervisor.proposal_id.as_str(),
                tool_name: supervisor.tool_name.as_str(),
                execution_surface: "run_stream_cleanup_supervisor",
            },
            supervisor.decision_allowed,
            supervisor.started_at,
            &outcome,
        );
    });
}

#[allow(clippy::result_large_err)]
async fn prepare_tool_side_effect_fence(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
) -> Result<Option<ActiveToolSideEffectFence>, Status> {
    let resolved_semantics = resolve_tool_execution_semantics(
        prepared.tool_name.as_str(),
        prepared.replay_safety_class,
        prepared.input_json.as_slice(),
    );
    let semantics = resolved_semantics.semantics;
    if matches!(
        semantics.idempotency_class,
        RuntimeIdempotencyClass::ReadOnly | RuntimeIdempotencyClass::DeterministicIdempotent
    ) {
        return Ok(None);
    }
    semantics.validate().map_err(|error| Status::failed_precondition(error.to_string()))?;
    let Some((session_id, generation)) =
        runtime_state.runtime_generation_for_tool(run_id.to_owned()).await?
    else {
        return Err(Status::failed_precondition(
            "tool side effect requires an active runtime generation",
        ));
    };
    if session_id != prepared.resolved_session_id {
        return Err(Status::failed_precondition(
            "tool side effect session does not own the active runtime generation",
        ));
    }
    let (operation_id, tool_execution_id) =
        GatewayRuntimeState::tool_side_effect_identities(prepared.proposal_id.as_str())?;
    let intent_sha256 = crate::sha256_hex(prepared.input_json.as_slice());
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
    match runtime_state.prepare_tool_side_effect_fence(session_id, run_id.to_owned(), fence).await?
    {
        SideEffectRetryDecision::Safe => Ok(Some(ActiveToolSideEffectFence {
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
                prepared.proposal_id.as_str(),
                prepared.tool_name.as_str(),
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

fn tool_side_effect_cleanup_outcome_request(
    fence: &ActiveToolSideEffectFence,
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
async fn record_tool_side_effect_cleanup_outcome(
    runtime_state: &Arc<GatewayRuntimeState>,
    fence: Option<&ActiveToolSideEffectFence>,
    outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    let Some(fence) = fence else {
        return Ok(());
    };
    runtime_state
        .record_tool_side_effect_cleanup_outcome(tool_side_effect_cleanup_outcome_request(
            fence, outcome,
        ))
        .await
        .map(|_| ())
}

#[allow(clippy::too_many_arguments, clippy::result_large_err)]
async fn commit_run_stream_tool_execution_outcome(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    fence: Option<&ActiveToolSideEffectFence>,
    outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    let Some(fence) = fence else {
        return Err(Status::internal(
            "mutating run-stream tool result requires an active side-effect fence",
        ));
    };
    let safe_output_json = redacted_run_stream_output_json(outcome.output_json.as_slice());
    let safe_error = redact_run_stream_text(outcome.error.as_str());
    let result_seq = *tape_seq;
    let attestation_seq = result_seq.saturating_add(1);
    let legacy_seq = attestation_seq.saturating_add(1);
    let tape_events = vec![
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
    ];
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
async fn send_committed_run_stream_tool_outcome(
    sender: &RunStreamProgressSender,
    run_id: &str,
    proposal_id: &str,
    outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    let result_event = tool_result_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        outcome.success,
        redacted_run_stream_output_json(outcome.output_json.as_slice()),
        redact_run_stream_text(outcome.error.as_str()),
    );
    sender
        .send(Ok(result_event))
        .await
        .map_err(|_| Status::cancelled(RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE))?;
    let attestation_event = tool_attestation_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        outcome.attestation.attestation_id.clone(),
        outcome.attestation.execution_sha256.clone(),
        outcome.attestation.executed_at_unix_ms,
        outcome.attestation.timed_out,
        outcome.attestation.executor.clone(),
    );
    sender
        .send(Ok(attestation_event))
        .await
        .map_err(|_| Status::cancelled(RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE))
}

#[allow(clippy::result_large_err)]
async fn settle_tool_side_effect_fence(
    runtime_state: &Arc<GatewayRuntimeState>,
    fence: Option<&ActiveToolSideEffectFence>,
    outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    let Some(fence) = fence else {
        return Ok(());
    };
    if outcome.attestation.timed_out {
        return mark_tool_side_effect_unknown(runtime_state, Some(fence)).await;
    }
    runtime_state
        .transition_tool_side_effect_fence(
            fence.operation_id.clone(),
            SideEffectFenceState::EffectObserved,
            fence.generation,
            "tool.effect.observed".to_owned(),
            Some(outcome.attestation.execution_sha256.clone()),
        )
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn mark_tool_side_effect_unknown(
    runtime_state: &Arc<GatewayRuntimeState>,
    fence: Option<&ActiveToolSideEffectFence>,
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
    side_effect_fence: Option<&ActiveToolSideEffectFence>,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    project_retain_commit_tool_execution_outcome(
        sender,
        runtime_state,
        request_context,
        run_id,
        prepared,
        execution_outcome,
        side_effect_fence,
        tape_seq,
        |_| Ok(()),
    )
    .await
    .map(|finalized| finalized.completed)
    .map_err(ToolOutcomeFinalizationError::into_status)
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn project_prepared_tool_execution_outcome(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    execution_outcome: ToolExecutionOutcome,
    tape_seq: &mut i64,
) -> Result<ToolExecutionOutcome, Status> {
    dispatch_after_tool_hook_if_enabled(runtime_state, run_id, prepared, &execution_outcome)
        .await?;
    let replay_safety = classify_tool_result_replay_safety(
        prepared.proposal_id.as_str(),
        prepared.tool_name.as_str(),
        prepared.input_json.as_slice(),
        &execution_outcome,
    );
    let projected = project_tool_result_for_model(
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
    append_tool_result_replay_safety_tape_event(runtime_state, run_id, tape_seq, &replay_safety)
        .await?;
    if let Some(report) = projected.middleware_report.as_ref() {
        append_tool_result_middleware_tape_event(runtime_state, run_id, tape_seq, report).await?;
    }
    if let Some(audit) = projected.audit.as_ref() {
        append_tool_result_projection_audit_tape_event(runtime_state, run_id, tape_seq, audit)
            .await?;
    }
    let execution_outcome = projected.outcome;
    if prepared.decision.allowed {
        runtime_state.record_tool_guardrail_result(
            run_id,
            &prepared.tool_signature,
            execution_outcome.success,
            (!execution_outcome.success).then_some(execution_outcome.error.as_str()),
        );
    }

    append_sessions_spawn_tape_event_if_needed(
        runtime_state,
        run_id,
        tape_seq,
        prepared,
        &execution_outcome,
    )
    .await?;
    Ok(execution_outcome)
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn commit_and_publish_projected_tool_execution_outcome(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    execution_outcome: ToolExecutionOutcome,
    side_effect_fence: Option<&ActiveToolSideEffectFence>,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, ToolOutcomeFinalizationError> {
    let transition_is_atomic =
        side_effect_fence.is_some() && !execution_outcome.attestation.timed_out;
    if transition_is_atomic {
        commit_run_stream_tool_execution_outcome(
            runtime_state,
            run_id,
            tape_seq,
            prepared.proposal_id.as_str(),
            prepared.tool_name.as_str(),
            side_effect_fence,
            &execution_outcome,
        )
        .await
        .map_err(ToolOutcomeFinalizationError::BeforeSettlement)?;
        send_committed_run_stream_tool_outcome(
            sender,
            run_id,
            prepared.proposal_id.as_str(),
            &execution_outcome,
        )
        .await
        .map_err(ToolOutcomeFinalizationError::AfterSettlement)?;
    } else {
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
        .await
        .map_err(ToolOutcomeFinalizationError::BeforeSettlement)?;

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
            execution_outcome.attestation.execution_manifest.as_deref(),
        )
        .await
        .map_err(ToolOutcomeFinalizationError::BeforeSettlement)?;
        runtime_state.record_tool_attestation_emitted();
        settle_tool_side_effect_fence(runtime_state, side_effect_fence, &execution_outcome)
            .await
            .map_err(ToolOutcomeFinalizationError::BeforeSettlement)?;
    }
    apply_tool_fault_after_tool_commit(
        runtime_state,
        prepared.proposal_id.as_str(),
        transition_is_atomic,
    )
    .map_err(ToolOutcomeFinalizationError::AfterSettlement)?;

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
        input_json: prepared.input_json.clone(),
        outcome: execution_outcome,
    })
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn project_retain_commit_tool_execution_outcome<T>(
    sender: &RunStreamProgressSender,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    execution_outcome: ToolExecutionOutcome,
    side_effect_fence: Option<&ActiveToolSideEffectFence>,
    tape_seq: &mut i64,
    retain_projection: impl FnOnce(&ToolExecutionOutcome) -> Result<T, Status>,
) -> Result<FinalizedToolExecutionOutcome<T>, ToolOutcomeFinalizationError> {
    let projected = project_prepared_tool_execution_outcome(
        runtime_state,
        request_context,
        run_id,
        prepared,
        execution_outcome,
        tape_seq,
    )
    .await;
    retain_commit_projected_tool_execution_outcome(
        sender,
        runtime_state,
        request_context,
        run_id,
        prepared,
        projected,
        side_effect_fence,
        tape_seq,
        retain_projection,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn retain_commit_projected_tool_execution_outcome<T>(
    sender: &RunStreamProgressSender,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    projected: Result<ToolExecutionOutcome, Status>,
    side_effect_fence: Option<&ActiveToolSideEffectFence>,
    tape_seq: &mut i64,
    retain_projection: impl FnOnce(&ToolExecutionOutcome) -> Result<T, Status>,
) -> Result<FinalizedToolExecutionOutcome<T>, ToolOutcomeFinalizationError> {
    let projected = match projected {
        Ok(projected) => projected,
        Err(error) => {
            return Err(settle_failed_tool_finalization(
                runtime_state,
                run_id,
                prepared,
                side_effect_fence,
                error,
            )
            .await);
        }
    };
    let retained_projection = match retain_projection(&projected) {
        Ok(retained_projection) => retained_projection,
        Err(error) => {
            return Err(settle_failed_tool_finalization(
                runtime_state,
                run_id,
                prepared,
                side_effect_fence,
                error,
            )
            .await);
        }
    };
    let completed = match commit_and_publish_projected_tool_execution_outcome(
        sender,
        runtime_state,
        request_context,
        run_id,
        prepared,
        projected,
        side_effect_fence,
        tape_seq,
    )
    .await
    {
        Ok(completed) => completed,
        Err(ToolOutcomeFinalizationError::BeforeSettlement(error)) => {
            return Err(settle_failed_tool_finalization(
                runtime_state,
                run_id,
                prepared,
                side_effect_fence,
                error,
            )
            .await);
        }
        Err(error @ ToolOutcomeFinalizationError::AfterSettlement(_))
        | Err(error @ ToolOutcomeFinalizationError::SettlementFailed(_)) => return Err(error),
    };
    Ok(FinalizedToolExecutionOutcome { completed, retained_projection })
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn finalize_drained_tool_execution_before_error<T>(
    sender: &RunStreamProgressSender,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    execution_outcome: ToolExecutionOutcome,
    side_effect_fence: Option<&ActiveToolSideEffectFence>,
    tape_seq: &mut i64,
    retain_projection: impl FnOnce(&ToolExecutionOutcome) -> Result<T, Status>,
    post_execution_error: Status,
) -> Result<Status, Status> {
    match project_retain_commit_tool_execution_outcome(
        sender,
        runtime_state,
        request_context,
        run_id,
        prepared,
        execution_outcome,
        side_effect_fence,
        tape_seq,
        retain_projection,
    )
    .await
    {
        Ok(_) => {}
        Err(ToolOutcomeFinalizationError::BeforeSettlement(_)) if side_effect_fence.is_some() => {}
        Err(ToolOutcomeFinalizationError::AfterSettlement(error)) => {
            // The atomic tape/fence commit already owns the durable truth.
            // A later stream failure must not downgrade an observed effect.
            warn!(
                run_id = %run_id,
                proposal_id = %prepared.proposal_id,
                tool_name = %prepared.tool_name,
                error = %error,
                "drained tool outcome was durably settled before publication failed"
            );
        }
        Err(ToolOutcomeFinalizationError::BeforeSettlement(finalization_error))
        | Err(ToolOutcomeFinalizationError::SettlementFailed(finalization_error)) => {
            return Err(finalization_error);
        }
    }
    Ok(post_execution_error)
}

#[allow(clippy::result_large_err)]
async fn settle_failed_tool_finalization(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    side_effect_fence: Option<&ActiveToolSideEffectFence>,
    finalization_error: Status,
) -> ToolOutcomeFinalizationError {
    let Some(side_effect_fence) = side_effect_fence else {
        return ToolOutcomeFinalizationError::BeforeSettlement(finalization_error);
    };
    if let Err(settlement_error) =
        mark_tool_side_effect_unknown(runtime_state, Some(side_effect_fence)).await
    {
        return ToolOutcomeFinalizationError::SettlementFailed(Status::internal(format!(
            "tool outcome finalization failed before settlement: {}; effect-unknown persistence also failed: {}",
            redact_run_stream_text(finalization_error.message()),
            redact_run_stream_text(settlement_error.message())
        )));
    }
    warn!(
        run_id = %run_id,
        proposal_id = %prepared.proposal_id,
        tool_name = %prepared.tool_name,
        error = %finalization_error,
        "tool outcome could not be committed and was marked effect unknown"
    );
    ToolOutcomeFinalizationError::BeforeSettlement(finalization_error)
}

fn apply_tool_fault_after_tool_commit(
    runtime_state: &GatewayRuntimeState,
    actor: &str,
    transition_is_atomic: bool,
) -> Result<(), Status> {
    let point_id = "tool.after_ack_before_transition";
    match runtime_state
        .fault_injection
        .checkpoint(point_id, actor)
        .map_err(|error| Status::internal(format!("qa_fault.tool_checkpoint_failed: {error}")))?
    {
        QaFaultDirective::Continue => Ok(()),
        QaFaultDirective::Activate(directive) => match directive.activation.action.clone() {
            QaFaultAction::TerminateProcess => {
                let recovery_class = if transition_is_atomic {
                    QaFaultRecoveryClass::EffectConfirmed
                } else {
                    QaFaultRecoveryClass::TransitionPending
                };
                runtime_state
                    .fault_injection
                    .record_verified_recovery(
                        &directive,
                        recovery_class,
                        if transition_is_atomic {
                            "qa_fault.tool_atomic_observation_confirmed_before_termination"
                        } else {
                            "qa_fault.tool_boundary_classified_before_termination"
                        },
                    )
                    .map_err(|error| {
                        Status::internal(format!("qa_fault.tool_recovery_failed: {error}"))
                    })?;
                #[cfg(feature = "qa-fault-injection")]
                runtime_state.fault_injection.terminate_process();
                #[cfg(not(feature = "qa-fault-injection"))]
                Err(Status::internal(
                    "qa_fault.feature_disabled: terminate directive reached a feature-off build",
                ))
            }
            action => Err(Status::internal(format!(
                "qa_fault.tool_action_unsupported: {}",
                action.kind().as_str()
            ))),
        },
    }
}

fn apply_tool_fault(
    runtime_state: &GatewayRuntimeState,
    point_id: &'static str,
    actor: &str,
) -> Result<(), Status> {
    match runtime_state
        .fault_injection
        .checkpoint(point_id, actor)
        .map_err(|error| Status::internal(format!("qa_fault.tool_checkpoint_failed: {error}")))?
    {
        QaFaultDirective::Continue => Ok(()),
        QaFaultDirective::Activate(directive) => match directive.activation.action.clone() {
            QaFaultAction::Timeout => {
                runtime_state.fault_injection.record_immediate_recovery(&directive).map_err(
                    |error| Status::internal(format!("qa_fault.tool_recovery_failed: {error}")),
                )?;
                Err(Status::deadline_exceeded(format!(
                    "qa_fault.tool_timeout: activation={}",
                    directive.activation.id
                )))
            }
            QaFaultAction::TerminateProcess => {
                let recovery_class = match point_id {
                    "tool.before_effect" => QaFaultRecoveryClass::FailedClosed,
                    "tool.after_effect_before_ack" => QaFaultRecoveryClass::OutcomeUnknown,
                    "tool.after_ack_before_transition" => QaFaultRecoveryClass::TransitionPending,
                    _ => {
                        return Err(Status::internal(format!(
                            "qa_fault.tool_recovery_unclassified: {point_id}"
                        )));
                    }
                };
                runtime_state
                    .fault_injection
                    .record_verified_recovery(
                        &directive,
                        recovery_class,
                        "qa_fault.tool_boundary_classified_before_termination",
                    )
                    .map_err(|error| {
                        Status::internal(format!("qa_fault.tool_recovery_failed: {error}"))
                    })?;
                #[cfg(feature = "qa-fault-injection")]
                runtime_state.fault_injection.terminate_process();
                #[cfg(not(feature = "qa-fault-injection"))]
                Err(Status::internal(
                    "qa_fault.feature_disabled: terminate directive reached a feature-off build",
                ))
            }
            action => Err(Status::internal(format!(
                "qa_fault.tool_action_unsupported: {}",
                action.kind().as_str()
            ))),
        },
    }
}

#[allow(clippy::result_large_err)]
async fn dispatch_after_tool_hook_if_enabled(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    prepared: &RunStreamPreparedToolExecution,
    outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    if !runtime_state.config.feature_rollouts.inline_runtime_hooks.enabled {
        return Ok(());
    }
    let report = crate::hooks::dispatch_named_event_with_report(
        Arc::clone(runtime_state),
        &runtime_state.config.tool_call.wasm_runtime,
        Duration::from_millis(runtime_state.config.tool_call.execution_timeout_ms),
        RunLifecycleHookPhase::AfterTool.event_name(),
        json!({
            "schema_version": 1,
            "run_id": run_id,
            "proposal_id": prepared.proposal_id.as_str(),
            "tool_name": prepared.tool_name.as_str(),
            "success": outcome.success,
            "output_bytes": outcome.output_json.len(),
            "output_sha256": crate::sha256_hex(outcome.output_json.as_slice()),
            "error_sha256": crate::sha256_hex(outcome.error.as_bytes()),
            "attestation_id": outcome.attestation.attestation_id.as_str(),
            "redaction_level": "hash_only_tool_result",
        }),
    )
    .await
    .map_err(|error| {
        Status::failed_precondition(format!("inline after-tool hook dispatch failed: {error}"))
    })?;
    if report
        .lifecycle_resolution
        .as_ref()
        .is_some_and(|resolution| resolution.selected.kind == RunLifecycleHookDecisionKind::FailRun)
    {
        return Err(Status::failed_precondition(
            "inline after-tool hook requested fail_run before result projection",
        ));
    }
    Ok(())
}

async fn project_tool_result_for_model(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    outcome: ToolExecutionOutcome,
) -> Result<ProjectedToolExecutionOutcome, Status> {
    let budget = ToolTurnBudget::default();
    let middleware_report = if runtime_state.config.feature_rollouts.tool_result_middleware.enabled
    {
        Some(
            apply_host_tool_result_middleware(
                tool_name,
                outcome.output_json.as_slice(),
                ToolResultVisibility::ModelInline,
            )
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "tool_result_middleware.invalid_output: {error}"
                ))
            })?,
        )
    } else {
        None
    };
    let should_spill = should_project_tool_result_for_model(tool_name, &outcome, &budget);
    if !should_spill {
        return Ok(ProjectedToolExecutionOutcome { outcome, audit: None, middleware_report });
    }

    let projection_policy = projection_policy_for_tool(tool_name);
    let default_sensitive =
        matches!(projection_policy, ToolResultProjectionPolicy::RedactedPreviewAndArtifact);

    let sensitivity =
        tool_result_sensitivity(tool_name, outcome.output_json.as_slice(), default_sensitive);
    let preview = redacted_tool_result_preview(
        tool_name,
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
        tool_name,
        outcome.output_json.as_slice(),
        budget.max_model_summary_bytes,
    );
    let visibility = if default_sensitive {
        ToolResultVisibility::RedactedPreview
    } else {
        ToolResultVisibility::ModelSummary
    };
    let workspace_spill = workspace_spill_projection_for_artifact(
        runtime_state,
        context,
        &artifact,
        artifact_content.content.as_slice(),
    )
    .await;
    let saved_model_visible_bytes =
        outcome.output_json.len().saturating_sub(summary.len()).try_into().unwrap_or(u64::MAX);
    let projected = json!({
        "schema_version": 1,
        "visibility": visibility.as_str(),
        "projection_policy": projection_policy.as_str(),
        "summary": summary,
        "redacted_preview": preview,
        "artifact": &artifact,
        "workspace_spill": workspace_spill,
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
    let audit = ToolResultProjectionAuditRecord {
        schema_version: 1,
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        policy: projection_policy_contract(projection_policy),
        decision: ToolResultProjectionDecisionKind::SpilledToArtifact,
        visibility,
        sensitivity,
        reason_code: "tool_result_projection.high_volume_artifact".to_owned(),
        redaction_level: if default_sensitive {
            "redacted_preview_only".to_owned()
        } else {
            "model_summary_only".to_owned()
        },
        artifact_id: Some(artifact.artifact_id.clone()),
        artifact_digest_sha256: Some(artifact.digest_sha256.clone()),
        original_output_bytes: artifact_content
            .original_output_bytes
            .try_into()
            .unwrap_or(u64::MAX),
        model_visible_output_bytes: projected_outcome
            .output_json
            .len()
            .try_into()
            .unwrap_or(u64::MAX),
        saved_model_visible_bytes,
        budget,
    };
    Ok(ProjectedToolExecutionOutcome {
        outcome: projected_outcome,
        audit: Some(audit),
        middleware_report,
    })
}

async fn workspace_spill_projection_for_artifact(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    artifact: &ToolResultArtifactRef,
    content: &[u8],
) -> Value {
    let full_read_requires_gate = artifact.sensitivity.requires_full_read_gate();
    if full_read_requires_gate && !workspace_spill_policy_grants_sensitivity(artifact.sensitivity) {
        return workspace_spill_unavailable_projection(
            artifact,
            "workspace_spill.sensitive_output_requires_policy_grant",
        );
    }

    match create_workspace_spill_file(runtime_state, context, artifact, content).await {
        Ok(spill) => json!({
            "schema_version": 1,
            "created": true,
            "workspace_visible_path": spill.relative_path,
            "workspace_root_index": spill.workspace_root_index,
            "reason_code": "workspace_spill.created",
            "canonical_source": "journal_tool_result_artifact",
            "artifact_id": artifact.artifact_id.as_str(),
            "digest_sha256": artifact.digest_sha256.as_str(),
            "spill_digest_sha256": spill.digest_sha256,
            "read_tools": ["palyra.fs.read_file", "palyra.artifact.read"],
            "policy": {
                "requires_run_owned_workspace_scope": true,
                "requires_sensitive_output_grant": full_read_requires_gate,
                "retention": &artifact.retention,
            },
        }),
        Err(reason_code) => workspace_spill_unavailable_projection(artifact, reason_code.as_str()),
    }
}

fn workspace_spill_policy_grants_sensitivity(sensitivity: ToolResultSensitivity) -> bool {
    matches!(sensitivity, ToolResultSensitivity::Public | ToolResultSensitivity::StdoutStderr)
}

fn workspace_spill_unavailable_projection(
    artifact: &ToolResultArtifactRef,
    reason_code: &str,
) -> Value {
    json!({
        "schema_version": 1,
        "created": false,
        "workspace_visible_path": null,
        "reason_code": reason_code,
        "canonical_source": "journal_tool_result_artifact",
        "artifact_id": artifact.artifact_id.as_str(),
        "digest_sha256": artifact.digest_sha256.as_str(),
        "read_tools": ["palyra.artifact.read"],
        "policy": {
            "requires_run_owned_workspace_scope": true,
            "requires_sensitive_output_grant": artifact.sensitivity.requires_full_read_gate(),
            "retention": &artifact.retention,
        },
    })
}

struct WorkspaceSpillFile {
    relative_path: String,
    workspace_root_index: usize,
    digest_sha256: String,
}

async fn create_workspace_spill_file(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    artifact: &ToolResultArtifactRef,
    content: &[u8],
) -> Result<WorkspaceSpillFile, String> {
    let (workspace_root_index, workspace_root) =
        resolve_tool_result_spill_workspace_root(runtime_state, context).await?;
    let spill_dir = workspace_root
        .join(".palyra")
        .join("tool-spills")
        .join(safe_spill_path_component(context.run_id)?);
    fs::create_dir_all(spill_dir.as_path())
        .map_err(|_| "workspace_spill.create_dir_failed".to_owned())?;
    let canonical_root = fs::canonicalize(workspace_root.as_path())
        .map_err(|_| "workspace_spill.workspace_root_unavailable".to_owned())?;
    let canonical_dir = fs::canonicalize(spill_dir.as_path())
        .map_err(|_| "workspace_spill.create_dir_failed".to_owned())?;
    if !canonical_dir.starts_with(canonical_root.as_path()) {
        return Err("workspace_spill.scope_escape_detected".to_owned());
    }

    let file_name = format!("{}.json", safe_spill_path_component(artifact.artifact_id.as_str())?);
    let spill_path = canonical_dir.join(file_name);
    write_new_spill_file(spill_path.as_path(), content)?;
    let stored = fs::read(spill_path.as_path())
        .map_err(|_| "workspace_spill.digest_read_failed".to_owned())?;
    let digest_sha256 = crate::sha256_hex(stored.as_slice());
    if digest_sha256 != artifact.digest_sha256 {
        let _ = fs::remove_file(spill_path.as_path());
        return Err("workspace_spill.digest_mismatch".to_owned());
    }
    let relative_path =
        format!(".palyra/tool-spills/{}/{}.json", context.run_id, artifact.artifact_id);
    Ok(WorkspaceSpillFile { relative_path, workspace_root_index, digest_sha256 })
}

async fn resolve_tool_result_spill_workspace_root(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
) -> Result<(usize, PathBuf), String> {
    let agent_outcome = runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
        .map_err(|_| "workspace_spill.agent_workspace_unavailable".to_owned())?;
    let agent_workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let workspace_roots = workspace_roots_with_run_launch_context_for_agent_source(
        runtime_state,
        context.run_id,
        agent_workspace_roots.as_slice(),
        agent_outcome.source,
    )
    .await;
    if workspace_roots.is_empty() {
        return Err("workspace_spill.workspace_scope_unavailable".to_owned());
    }
    if let Ok(Some(active_root)) =
        session_active_workspace_root(runtime_state, context.session_id, workspace_roots.as_slice())
            .await
    {
        if let Some((index, root)) = workspace_roots.iter().enumerate().find(|(_, root)| {
            same_canonical_workspace_root(root.as_path(), active_root.root.as_path())
        }) {
            return Ok((index, root.clone()));
        }
        return Ok((0, active_root.root));
    }
    Ok((0, workspace_roots[0].clone()))
}

fn write_new_spill_file(path: &Path, content: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            "workspace_spill.file_already_exists"
        } else {
            "workspace_spill.write_failed"
        }
        .to_owned()
    })?;
    file.write_all(content).map_err(|_| "workspace_spill.write_failed".to_owned())
}

fn same_canonical_workspace_root(left: &Path, right: &Path) -> bool {
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn safe_spill_path_component(value: &str) -> Result<&str, String> {
    if !value.is_empty()
        && value.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        Ok(value)
    } else {
        Err("workspace_spill.invalid_path_component".to_owned())
    }
}

#[allow(clippy::result_large_err)]
async fn append_tool_result_middleware_tape_event(
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
            event_type: TOOL_RESULT_MIDDLEWARE_EVENT.to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn append_tool_result_projection_audit_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    audit: &ToolResultProjectionAuditRecord,
) -> Result<(), Status> {
    let payload_json = serde_json::to_string(audit).map_err(|error| {
        Status::internal(format!("failed to serialize tool result projection audit: {error}"))
    })?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: TOOL_RESULT_PROJECTION_POLICY_EVENT.to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn append_tool_result_replay_safety_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    report: &ToolResultReplaySafetyReport,
) -> Result<(), Status> {
    let payload_json = serde_json::to_string(report).map_err(|error| {
        Status::internal(format!("failed to serialize tool result replay safety: {error}"))
    })?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: TOOL_RESULT_REPLAY_SAFETY_EVENT.to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn append_sessions_spawn_tape_event_if_needed(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    prepared: &RunStreamPreparedToolExecution,
    execution_outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
    if prepared.tool_name != SESSIONS_SPAWN_TOOL_NAME || !execution_outcome.success {
        return Ok(());
    }
    let Ok(output) = serde_json::from_slice::<Value>(execution_outcome.output_json.as_slice())
    else {
        return Ok(());
    };
    let Some(payload) = sessions_spawn_tape_payload(
        prepared.proposal_id.as_str(),
        prepared.tool_name.as_str(),
        run_id,
        &output,
    ) else {
        return Ok(());
    };
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "subagent.spawned".to_owned(),
            payload_json: payload.to_string(),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

fn sessions_spawn_tape_payload(
    proposal_id: &str,
    tool_name: &str,
    parent_run_id: &str,
    output: &Value,
) -> Option<Value> {
    let child_run_id = output.get("child_run_id").and_then(Value::as_str)?;
    Some(json!({
        "schema_version": 1,
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "parent_run_id": parent_run_id,
        "task_id": output.get("task_id").and_then(Value::as_str),
        "child_run_id": child_run_id,
        "child_session_id": output.get("child_session_id").and_then(Value::as_str),
        "state": output.get("state").and_then(Value::as_str).unwrap_or("queued"),
        "transcript_ref": output.get("transcript_ref").cloned().unwrap_or(Value::Null),
    }))
}

fn projection_policy_contract(
    projection_policy: ToolResultProjectionPolicy,
) -> ToolResultProjectionPolicyKind {
    match projection_policy {
        ToolResultProjectionPolicy::InlineUnlessLarge => {
            ToolResultProjectionPolicyKind::InlineUnlessLarge
        }
        ToolResultProjectionPolicy::SummarizeAndArtifact => {
            ToolResultProjectionPolicyKind::SummarizeAndArtifact
        }
        ToolResultProjectionPolicy::RedactedPreviewAndArtifact => {
            ToolResultProjectionPolicyKind::RedactedPreviewAndArtifact
        }
    }
}

fn should_project_tool_result_for_model(
    tool_name: &str,
    outcome: &ToolExecutionOutcome,
    budget: &ToolTurnBudget,
) -> bool {
    if os_file_redacted_read_result_can_stay_inline(
        tool_name,
        outcome.output_json.as_slice(),
        budget,
    ) {
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

// Small OS-file redacted text-only read results bypass artifact projection:
// config-audit workflows need the full payload model-visible, but only after
// the runtime marks it non-authoritative redacted placeholder text.
fn os_file_redacted_read_result_can_stay_inline(
    tool_name: &str,
    output_json: &[u8],
    budget: &ToolTurnBudget,
) -> bool {
    if tool_name != crate::gateway::OS_FILE_TOOL_NAME
        || output_json.len() > budget.max_model_inline_bytes
    {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<Value>(output_json) else {
        return false;
    };
    value.get("operation").and_then(Value::as_str) == Some("read")
        && os_file_read_result_has_model_visible_text(&value)
}

fn os_file_read_result_has_model_visible_text(value: &Value) -> bool {
    value.get("text").is_some_and(Value::is_string)
        && value.get("bytes_base64").is_none_or(Value::is_null)
        && value.get("redacted").is_some_and(|value| value.as_bool() == Some(true))
        && value.get("text_authoritative").is_some_and(|value| value.as_bool() == Some(false))
}

fn tool_result_sensitivity(
    tool_name: &str,
    output_json: &[u8],
    default_sensitive: bool,
) -> ToolResultSensitivity {
    if tool_name == crate::gateway::PROCESS_RUNNER_TOOL_NAME {
        ToolResultSensitivity::StdoutStderr
    } else if tool_name == crate::gateway::HTTP_FETCH_TOOL_NAME
        || tool_name.starts_with("palyra.browser.")
        || tool_name == "palyra.plugin.run"
    {
        ToolResultSensitivity::ProviderRawPayload
    } else if tool_name == crate::gateway::WORKSPACE_READ_FILE_TOOL_NAME
        && workspace_read_result_can_be_public_artifact(output_json)
    {
        ToolResultSensitivity::Public
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

fn workspace_read_result_can_be_public_artifact(output_json: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(output_json) else {
        return false;
    };
    if !workspace_read_text_already_sanitized(crate::gateway::WORKSPACE_READ_FILE_TOOL_NAME, &value)
        || value.get("bytes_base64").is_some_and(|payload| !payload.is_null())
    {
        return false;
    }
    value.get("path").and_then(Value::as_str).is_some_and(workspace_read_display_path_is_model_safe)
}

fn workspace_read_display_path_is_model_safe(path: &str) -> bool {
    let path = path.trim();
    if path.is_empty()
        || path.starts_with('/')
        || path.starts_with('\\')
        || looks_like_windows_absolute_path(path)
    {
        return false;
    }
    path.split(['/', '\\']).all(|component| !matches!(component, "" | "." | ".."))
}

fn looks_like_windows_absolute_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

fn summarize_tool_result_for_model(
    tool_name: &str,
    output_json: &[u8],
    max_bytes: usize,
) -> String {
    let preview = redacted_tool_result_preview(tool_name, output_json, max_bytes);
    if preview.len() <= max_bytes {
        preview
    } else {
        truncate_utf8(preview.as_str(), max_bytes)
    }
}

#[derive(Clone, Copy)]
struct ToolResultPreviewRedaction {
    preserve_workspace_read_text: bool,
}

fn redacted_tool_result_preview(tool_name: &str, output_json: &[u8], max_bytes: usize) -> String {
    if tool_name == crate::gateway::BROWSER_OBSERVE_TOOL_NAME {
        if let Some(preview) = browser_observe_selector_capture_preview(output_json, max_bytes) {
            return preview;
        }
    }
    let redacted = match serde_json::from_slice::<Value>(output_json) {
        Ok(mut value) => {
            let redaction = ToolResultPreviewRedaction {
                preserve_workspace_read_text: workspace_read_text_already_sanitized(
                    tool_name, &value,
                ),
            };
            redact_sensitive_json_value(&mut value, redaction);
            serde_json::to_string(&value).unwrap_or_else(|_| REDACTED.to_owned())
        }
        Err(_) => {
            let raw = String::from_utf8_lossy(output_json);
            redact_auth_error(redact_url_segments_in_text(raw.as_ref()).as_str())
        }
    };
    truncate_utf8(redacted.as_str(), max_bytes)
}

fn browser_observe_selector_capture_preview(
    output_json: &[u8],
    max_bytes: usize,
) -> Option<String> {
    let value = serde_json::from_slice::<Value>(output_json).ok()?;
    let captures = value.get("element_captures").and_then(Value::as_array)?;
    if captures.is_empty() {
        return None;
    }

    let mut preview = Map::new();
    for key in [
        "success",
        "page_url",
        "element_captures",
        "dom_truncated",
        "accessibility_tree_truncated",
        "visible_text_truncated",
        "safety",
        "error",
    ] {
        if let Some(entry) = value.get(key) {
            preview.insert(key.to_owned(), entry.clone());
        }
    }

    let mut omitted = Map::new();
    for key in ["dom_snapshot", "accessibility_tree", "visible_text"] {
        if let Some(text) = value.get(key).and_then(Value::as_str).filter(|text| !text.is_empty()) {
            omitted.insert(format!("{key}_bytes"), json!(text.len()));
        }
    }
    if !omitted.is_empty() {
        preview.insert("omitted_observation_text".to_owned(), Value::Object(omitted));
    }

    let mut preview = Value::Object(preview);
    redact_sensitive_json_value(
        &mut preview,
        ToolResultPreviewRedaction { preserve_workspace_read_text: false },
    );
    serde_json::to_string(&preview).ok().map(|rendered| truncate_utf8(rendered.as_str(), max_bytes))
}

// Workspace read results that the read tool already scanned and marked clean
// (redacted=false, non-binary) keep their text mostly intact in previews;
// re-running the aggressive secret heuristics would mangle benign source code.
fn workspace_read_text_already_sanitized(tool_name: &str, value: &Value) -> bool {
    tool_name == crate::gateway::WORKSPACE_READ_FILE_TOOL_NAME
        && value.get("text").is_some_and(Value::is_string)
        && value.get("redacted").and_then(Value::as_bool) == Some(false)
        && !value.get("binary").and_then(Value::as_bool).unwrap_or(false)
}

fn redact_sensitive_json_value(value: &mut Value, redaction: ToolResultPreviewRedaction) {
    match value {
        Value::Object(map) => redact_sensitive_json_object(map, redaction),
        Value::Array(values) => {
            for value in values {
                redact_sensitive_json_value(value, redaction);
            }
        }
        Value::String(text) => {
            *text = redact_auth_error(redact_url_segments_in_text(text).as_str());
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn redact_sensitive_json_object(
    map: &mut Map<String, Value>,
    redaction: ToolResultPreviewRedaction,
) {
    for key in map.keys().cloned().collect::<Vec<_>>() {
        if is_sensitive_key(key.as_str()) {
            if let Some(value) = map.get_mut(&key) {
                *value = Value::String(REDACTED.to_owned());
            }
        } else if is_stream_binary_payload_key(key.as_str()) {
            if let Some(value) = map.remove(&key) {
                let replacement_key =
                    unique_omitted_binary_payload_key(map, stream_binary_payload_kind(&key));
                map.insert(replacement_key, stream_binary_payload_placeholder(&key, &value));
            }
        } else if redaction.preserve_workspace_read_text && key == "text" {
            if let Some(Value::String(text)) = map.get_mut(&key) {
                *text = redact_url_segments_in_text(text.as_str());
            }
        } else if let Some(value) = map.get_mut(&key) {
            redact_sensitive_json_value(value, redaction);
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

fn stream_binary_payload_kind(key: &str) -> &'static str {
    let normalized = key.to_ascii_lowercase().replace(['-', '.'], "_");
    if normalized.contains("screenshot") {
        "screenshot"
    } else if normalized.contains("image") {
        "image"
    } else if normalized.contains("pdf") {
        "pdf"
    } else if normalized.contains("bytes") {
        "bytes"
    } else {
        "binary"
    }
}

fn unique_omitted_binary_payload_key(map: &Map<String, Value>, kind: &str) -> String {
    let base = format!("omitted_{kind}_payload");
    if !map.contains_key(&base) {
        return base;
    }
    for index in 2.. {
        let candidate = format!("{base}_{index}");
        if !map.contains_key(&candidate) {
            return candidate;
        }
    }
    unreachable!("unbounded suffix search must find a free omitted payload key")
}

fn stream_binary_payload_placeholder(key: &str, value: &Value) -> Value {
    let char_len = value.as_str().map(str::len).unwrap_or_default();
    json!({
        "omitted": true,
        "kind": stream_binary_payload_kind(key),
        "encoding": "base64",
        "base64_chars": char_len,
    })
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
        allow_sensitive_tools_approval_outcome, append_process_progress_backpressure_tape_event,
        classify_tool_parallelism, classify_tool_result_replay_safety,
        commit_run_stream_tool_execution_outcome, drain_parallel_tool_group_after_cancel,
        drain_parallel_tool_group_after_error, finalize_drained_tool_execution_before_error,
        process_progress_channel_for_tool, process_progress_status_message,
        projection_policy_contract, retain_commit_projected_tool_execution_outcome,
        sessions_spawn_tape_payload, settle_failed_tool_finalization,
        tool_side_effect_cleanup_outcome_request, workspace_spill_policy_grants_sensitivity,
        workspace_spill_unavailable_projection, OrchestratorTapeAppendRequest,
        ParallelToolExecutionTaskOutcome, RunStreamPreparedToolExecution, ToolParallelism,
        TOOL_RESULT_PROJECTION_POLICY_EVENT, TOOL_RESULT_REPLAY_SAFETY_EVENT,
    };
    use crate::application::tool_governance::build_tool_call_signature;
    use crate::application::tool_registry::{
        tool_execution_semantics, ToolReplaySafetyClass, ToolResultProjectionPolicy,
    };
    use crate::application::tool_security::ToolProposalBackendSelection;
    use crate::execution_backends::{ExecutionBackendPreference, ExecutionBackendResolution};
    use crate::gateway::runtime::tests::{start_test_orchestrator_run, test_runtime_state};
    use crate::gateway::GatewayRuntimeState;
    use crate::journal::{ApprovalDecision, ApprovalDecisionScope};
    use crate::sandbox_runner::ProcessProgressEvent;
    use crate::tool_protocol::{ToolAttestation, ToolDecision, ToolExecutionOutcome};
    use crate::transport::grpc::auth::RequestContext;
    use palyra_common::runtime_contracts::{
        project_legacy_runtime_error, ArtifactRetentionPolicy, CancellationSettlementOutcome,
        ReconciliationStrategy, RuntimeErrorClass, RuntimeErrorObservation, RuntimeEventEnvelopeV2,
        RuntimeEventName, RuntimeEventPayloadRef, RuntimeGeneration, RuntimeIdempotencyClass,
        RuntimeOperationId, RuntimeRetryability, SideEffectFenceState, SideEffectFenceV1,
        SideEffectRestartPolicy, ToolResultArtifactRef, ToolResultProjectionPolicyKind,
        ToolResultSensitivity, ToolTurnBudget,
    };
    use palyra_common::validate_canonical_id;
    use serde_json::{json, Value};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::{
        task::JoinSet,
        time::{sleep, Duration},
    };
    use tonic::{Code, Status};

    fn start_backpressure_test_run(state: &GatewayRuntimeState, session_id: &str, run_id: &str) {
        start_test_orchestrator_run(state, session_id, run_id);
    }

    fn tool_outcome_for_replay_test(
        success: bool,
        output: Value,
        error: &str,
        timed_out: bool,
    ) -> ToolExecutionOutcome {
        ToolExecutionOutcome {
            success,
            output_json: serde_json::to_vec(&output).expect("test output should serialize"),
            error: error.to_owned(),
            attestation: ToolAttestation {
                attestation_id: "01ARZ3NDEKTSV4RRFFQ69G5RA1".to_owned(),
                execution_sha256: "0".repeat(64),
                executed_at_unix_ms: 0,
                timed_out,
                executor: "test".to_owned(),
                sandbox_enforcement: "n/a".to_owned(),
                execution_manifest: None,
            },
        }
    }

    fn mutating_prepared_tool(
        session_id: &str,
        proposal_id: &str,
    ) -> RunStreamPreparedToolExecution {
        let tool_name = "palyra.fs.apply_patch";
        let input_json = br#"{"patch":"*** Begin Patch\n*** End Patch"}"#.to_vec();
        RunStreamPreparedToolExecution {
            proposal_id: proposal_id.to_owned(),
            tool_name: tool_name.to_owned(),
            replay_safety_class: ToolReplaySafetyClass::ExternalSideEffect,
            tool_signature: build_tool_call_signature(tool_name, input_json.as_slice()),
            input_json,
            decision: ToolDecision {
                allowed: true,
                reason: "test policy grant".to_owned(),
                approval_required: false,
                policy_enforced: true,
            },
            resolved_session_id: session_id.to_owned(),
            backend_selection: ToolProposalBackendSelection {
                agent_id: None,
                requested_preference: ExecutionBackendPreference::LocalSandbox,
                resolution: ExecutionBackendResolution {
                    requested: ExecutionBackendPreference::LocalSandbox,
                    resolved: ExecutionBackendPreference::LocalSandbox,
                    fallback_used: false,
                    reason_code: "test_local_sandbox".to_owned(),
                    approval_required: false,
                    reason: "test local sandbox".to_owned(),
                },
            },
        }
    }

    async fn start_mutating_tool_fence(
        state: &Arc<GatewayRuntimeState>,
        session_id: &str,
        run_id: &str,
        proposal_id: &str,
    ) -> super::ActiveToolSideEffectFence {
        start_test_orchestrator_run(state, session_id, run_id);
        let (_, generation) = state
            .runtime_generation_for_tool_blocking(run_id)
            .expect("tool generation query should succeed")
            .expect("tool generation should be active");
        let (operation_id, tool_execution_id) =
            GatewayRuntimeState::tool_side_effect_identities(proposal_id)
                .expect("tool side-effect identities should validate");
        let intent_sha256 = "a".repeat(64);
        let fence = SideEffectFenceV1 {
            schema_version: 1,
            operation_id: operation_id.clone(),
            tool_execution_id,
            intent_generation: generation,
            observed_generation: generation,
            intent_sha256: intent_sha256.clone(),
            state: SideEffectFenceState::IntentRecorded,
            semantics: tool_execution_semantics(
                "palyra.fs.apply_patch",
                ToolReplaySafetyClass::ExternalSideEffect,
            ),
            external_idempotency_key_sha256: None,
            evidence_sha256: None,
            reason_code: "tool.effect.intent_recorded".to_owned(),
            updated_at_unix_ms: crate::gateway::current_unix_ms(),
        };
        state
            .prepare_tool_side_effect_fence(session_id.to_owned(), run_id.to_owned(), fence)
            .await
            .expect("tool effect intent should persist");
        state
            .transition_tool_side_effect_fence(
                operation_id.clone(),
                SideEffectFenceState::EffectStarted,
                generation,
                "tool.effect.started".to_owned(),
                None,
            )
            .await
            .expect("tool effect should start");
        super::ActiveToolSideEffectFence {
            operation_id,
            generation,
            intent_sha256,
            strategy: ReconciliationStrategy::WorkspaceDigest,
            external_idempotency_key_sha256: None,
        }
    }

    fn assert_tool_fence_is_observed(
        state: &GatewayRuntimeState,
        fence: &super::ActiveToolSideEffectFence,
    ) {
        assert_tool_fence_state(state, fence, SideEffectFenceState::EffectObserved);
    }

    fn assert_tool_fence_state(
        state: &GatewayRuntimeState,
        fence: &super::ActiveToolSideEffectFence,
        expected: SideEffectFenceState,
    ) {
        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("journal database should reopen");
        let state_name: String = connection
            .query_row(
                "SELECT state FROM runtime_side_effect_fences WHERE operation_ulid = ?1",
                rusqlite::params![fence.operation_id.as_str()],
                |row| row.get(0),
            )
            .expect("side-effect fence should load");
        assert_eq!(state_name, expected.as_str());
        let unresolved: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM runtime_side_effect_fences
                 WHERE operation_ulid = ?1 AND state IN (?2, ?3)",
                rusqlite::params![
                    fence.operation_id.as_str(),
                    SideEffectFenceState::IntentRecorded.as_str(),
                    SideEffectFenceState::EffectStarted.as_str(),
                ],
                |row| row.get(0),
            )
            .expect("unresolved fence count should load");
        assert_eq!(unresolved, 0, "mutating tool fence must not remain unresolved");
    }

    #[test]
    fn late_serial_cleanup_maps_definitive_and_ambiguous_outcomes() {
        let fence = super::ActiveToolSideEffectFence {
            operation_id: RuntimeOperationId::parse("operation_cleanup_supervisor_test")
                .expect("operation id should validate"),
            generation: RuntimeGeneration::new(7).expect("generation should validate"),
            intent_sha256: "a".repeat(64),
            strategy: ReconciliationStrategy::WorkspaceDigest,
            external_idempotency_key_sha256: None,
        };
        let definitive = tool_side_effect_cleanup_outcome_request(
            &fence,
            &tool_outcome_for_replay_test(true, json!({"ok": true}), "", false),
        );
        assert!(definitive.outcome_observed);
        assert_eq!(definitive.reason_code, "tool.effect.cleanup_reconciled");
        assert_eq!(definitive.evidence_sha256.as_deref(), Some("0".repeat(64).as_str()));

        let ambiguous = tool_side_effect_cleanup_outcome_request(
            &fence,
            &tool_outcome_for_replay_test(false, json!({}), "timed out", true),
        );
        assert!(!ambiguous.outcome_observed);
        assert_eq!(ambiguous.reason_code, "tool.effect.cleanup_unknown");
        assert_eq!(ambiguous.evidence_sha256, None);
    }

    #[tokio::test]
    async fn definitive_run_stream_tool_result_commits_fence_and_canonical_tape_atomically() {
        let state = test_runtime_state();
        let session_id = "session_run_stream_atomic_tool_effect";
        let run_id = "run_run_stream_atomic_tool_effect";
        let proposal_id = "proposal_run_stream_atomic_tool_effect";
        start_test_orchestrator_run(&state, session_id, run_id);
        let (_, generation) = state
            .runtime_generation_for_tool_blocking(run_id)
            .expect("tool generation query should succeed")
            .expect("tool generation should be active");
        let (operation_id, tool_execution_id) =
            GatewayRuntimeState::tool_side_effect_identities(proposal_id)
                .expect("tool side-effect identities should validate");
        let semantics = tool_execution_semantics(
            "palyra.fs.apply_patch",
            ToolReplaySafetyClass::ExternalSideEffect,
        );
        let fence = SideEffectFenceV1 {
            schema_version: 1,
            operation_id: operation_id.clone(),
            tool_execution_id,
            intent_generation: generation,
            observed_generation: generation,
            intent_sha256: "a".repeat(64),
            state: SideEffectFenceState::IntentRecorded,
            semantics,
            external_idempotency_key_sha256: None,
            evidence_sha256: None,
            reason_code: "tool.effect.intent_recorded".to_owned(),
            updated_at_unix_ms: crate::gateway::current_unix_ms(),
        };
        state
            .prepare_tool_side_effect_fence(session_id.to_owned(), run_id.to_owned(), fence)
            .await
            .expect("tool effect intent should persist");
        state
            .transition_tool_side_effect_fence(
                operation_id.clone(),
                SideEffectFenceState::EffectStarted,
                generation,
                "tool.effect.started".to_owned(),
                None,
            )
            .await
            .expect("tool effect should start");
        let active_fence = super::ActiveToolSideEffectFence {
            operation_id,
            generation,
            intent_sha256: "a".repeat(64),
            strategy: ReconciliationStrategy::WorkspaceDigest,
            external_idempotency_key_sha256: None,
        };
        let outcome = tool_outcome_for_replay_test(true, json!({"ok": true}), "", false);
        let mut tape_seq = 0;

        commit_run_stream_tool_execution_outcome(
            &state,
            run_id,
            &mut tape_seq,
            proposal_id,
            "palyra.fs.apply_patch",
            Some(&active_fence),
            &outcome,
        )
        .await
        .expect("definitive result should commit atomically");

        assert_eq!(tape_seq, 3);
        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("journal database should reopen");
        let tape_types = connection
            .prepare(
                "SELECT event_type FROM orchestrator_tape WHERE run_ulid = ?1 ORDER BY seq ASC",
            )
            .expect("tape query should prepare")
            .query_map(rusqlite::params![run_id], |row| row.get::<_, String>(0))
            .expect("tape query should execute")
            .collect::<Result<Vec<_>, _>>()
            .expect("tape rows should decode");
        assert_eq!(tape_types, ["tool_result", "tool_attestation", "tool.executed"]);
        let (state_name, fence_json): (String, String) = connection
            .query_row(
                "SELECT state, fence_json FROM runtime_side_effect_fences WHERE operation_ulid = ?1",
                rusqlite::params![active_fence.operation_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("side-effect fence should load");
        assert_eq!(state_name, SideEffectFenceState::EffectObserved.as_str());
        let persisted: SideEffectFenceV1 =
            serde_json::from_str(fence_json.as_str()).expect("fence JSON should decode");
        assert_eq!(persisted.state, SideEffectFenceState::EffectObserved);
        assert_eq!(
            persisted.evidence_sha256.as_deref(),
            Some(outcome.attestation.execution_sha256.as_str())
        );
        let observed_events: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM runtime_events_v2 WHERE run_ulid = ?1 AND event_name = ?2",
                rusqlite::params![run_id, RuntimeEventName::ToolEffectObserved.as_str()],
                |row| row.get(0),
            )
            .expect("observed runtime event count should load");
        assert_eq!(observed_events, 1);
    }

    #[tokio::test]
    async fn cancelled_mutating_tool_completion_settles_fence_before_error() {
        let state = test_runtime_state();
        let session_id = "session_cancelled_drained_mutation";
        let run_id = "run_cancelled_drained_mutation";
        let proposal_id = "proposal_cancelled_drained_mutation";
        let fence = start_mutating_tool_fence(&state, session_id, run_id, proposal_id).await;
        let prepared = mutating_prepared_tool(session_id, proposal_id);
        let request_context = RequestContext {
            principal: "test-principal".to_owned(),
            device_id: "test-device".to_owned(),
            channel: None,
        };
        let (sender, _receiver) = tokio::sync::mpsc::channel(4);
        let mut tape_seq = 0;

        let propagated = finalize_drained_tool_execution_before_error(
            &sender,
            &state,
            &request_context,
            run_id,
            &prepared,
            tool_outcome_for_replay_test(true, json!({"ok": true}), "", false),
            Some(&fence),
            &mut tape_seq,
            |_| Ok(()),
            Status::cancelled("tool execution completed after cancellation was requested"),
        )
        .await
        .expect("drained mutation should settle before cancellation propagates");

        assert_eq!(propagated.code(), Code::Cancelled);
        assert_tool_fence_is_observed(&state, &fence);
    }

    #[tokio::test]
    async fn progress_channel_failure_after_mutation_does_not_leave_fence_unresolved() {
        let state = test_runtime_state();
        let session_id = "session_progress_failure_drained_mutation";
        let run_id = "run_progress_failure_drained_mutation";
        let proposal_id = "proposal_progress_failure_drained_mutation";
        let fence = start_mutating_tool_fence(&state, session_id, run_id, proposal_id).await;
        let prepared = mutating_prepared_tool(session_id, proposal_id);
        let request_context = RequestContext {
            principal: "test-principal".to_owned(),
            device_id: "test-device".to_owned(),
            channel: None,
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        let mut tape_seq = 0;

        let propagated = finalize_drained_tool_execution_before_error(
            &sender,
            &state,
            &request_context,
            run_id,
            &prepared,
            tool_outcome_for_replay_test(true, json!({"ok": true}), "", false),
            Some(&fence),
            &mut tape_seq,
            |_| Ok(()),
            Status::cancelled("process progress channel closed after mutation"),
        )
        .await
        .expect("durable settlement should survive progress publication failure");

        assert_eq!(propagated.code(), Code::Cancelled);
        assert_tool_fence_is_observed(&state, &fence);
    }

    #[tokio::test]
    async fn failed_drained_mutation_commit_marks_fence_effect_unknown() {
        let state = test_runtime_state();
        let session_id = "session_failed_drained_mutation_commit";
        let run_id = "run_failed_drained_mutation_commit";
        let proposal_id = "proposal_failed_drained_mutation_commit";
        let fence = start_mutating_tool_fence(&state, session_id, run_id, proposal_id).await;
        let prepared = mutating_prepared_tool(session_id, proposal_id);

        let failure = settle_failed_tool_finalization(
            &state,
            run_id,
            &prepared,
            Some(&fence),
            Status::internal("test durable commit failure"),
        )
        .await;
        assert!(matches!(failure, super::ToolOutcomeFinalizationError::BeforeSettlement(_)));

        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("journal database should reopen");
        let state_name: String = connection
            .query_row(
                "SELECT state FROM runtime_side_effect_fences WHERE operation_ulid = ?1",
                rusqlite::params![fence.operation_id.as_str()],
                |row| row.get(0),
            )
            .expect("side-effect fence should load");
        assert_eq!(state_name, SideEffectFenceState::EffectUnknown.as_str());
    }

    #[tokio::test]
    async fn ordinary_v2_projection_failure_marks_successful_mutation_effect_unknown() {
        let state = test_runtime_state();
        let session_id = "session_v2_projection_failure";
        let run_id = "run_v2_projection_failure";
        let proposal_id = "proposal_v2_projection_failure";
        let fence = start_mutating_tool_fence(&state, session_id, run_id, proposal_id).await;
        let prepared = mutating_prepared_tool(session_id, proposal_id);
        let request_context = RequestContext {
            principal: "test-principal".to_owned(),
            device_id: "test-device".to_owned(),
            channel: None,
        };
        let (sender, _receiver) = tokio::sync::mpsc::channel(4);
        let mut tape_seq = 0;
        let mut retained = false;
        let successful_outcome = tool_outcome_for_replay_test(true, json!({"ok": true}), "", false);
        assert!(successful_outcome.success, "the mutation must complete before projection fails");
        let projection_result: Result<ToolExecutionOutcome, Status> =
            Err(Status::internal("test projection failure"));

        let result = retain_commit_projected_tool_execution_outcome(
            &sender,
            &state,
            &request_context,
            run_id,
            &prepared,
            projection_result,
            Some(&fence),
            &mut tape_seq,
            |_| {
                retained = true;
                Ok(())
            },
        )
        .await;

        assert!(matches!(result, Err(super::ToolOutcomeFinalizationError::BeforeSettlement(_))));
        assert!(!retained, "a failed projection must never reach projection retention");
        assert_tool_fence_state(&state, &fence, SideEffectFenceState::EffectUnknown);
    }

    #[tokio::test]
    async fn ordinary_v2_retention_failure_marks_successful_mutation_effect_unknown() {
        let state = test_runtime_state();
        let session_id = "session_v2_retention_failure";
        let run_id = "run_v2_retention_failure";
        let proposal_id = "proposal_v2_retention_failure";
        let fence = start_mutating_tool_fence(&state, session_id, run_id, proposal_id).await;
        let prepared = mutating_prepared_tool(session_id, proposal_id);
        let request_context = RequestContext {
            principal: "test-principal".to_owned(),
            device_id: "test-device".to_owned(),
            channel: None,
        };
        let (sender, _receiver) = tokio::sync::mpsc::channel(4);
        let mut tape_seq = 0;

        let result = retain_commit_projected_tool_execution_outcome(
            &sender,
            &state,
            &request_context,
            run_id,
            &prepared,
            Ok(tool_outcome_for_replay_test(true, json!({"ok": true}), "", false)),
            Some(&fence),
            &mut tape_seq,
            |_| Err::<(), _>(Status::internal("test projection retention failure")),
        )
        .await;

        assert!(matches!(result, Err(super::ToolOutcomeFinalizationError::BeforeSettlement(_))));
        assert_tool_fence_state(&state, &fence, SideEffectFenceState::EffectUnknown);
    }

    #[tokio::test]
    async fn ordinary_v2_commit_failure_marks_successful_mutation_effect_unknown() {
        let state = test_runtime_state();
        let session_id = "session_v2_commit_failure";
        let run_id = "run_v2_commit_failure";
        let proposal_id = "proposal_v2_commit_failure";
        let fence = start_mutating_tool_fence(&state, session_id, run_id, proposal_id).await;
        let prepared = mutating_prepared_tool(session_id, proposal_id);
        let request_context = RequestContext {
            principal: "test-principal".to_owned(),
            device_id: "test-device".to_owned(),
            channel: None,
        };
        state
            .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
                run_id: run_id.to_owned(),
                seq: 0,
                event_type: "test.tool_finalization_collision".to_owned(),
                payload_json: "{}".to_owned(),
            })
            .await
            .expect("test collision row should persist");
        let (sender, _receiver) = tokio::sync::mpsc::channel(4);
        let mut tape_seq = 0;
        let mut retention_count = 0_u8;

        let result = retain_commit_projected_tool_execution_outcome(
            &sender,
            &state,
            &request_context,
            run_id,
            &prepared,
            Ok(tool_outcome_for_replay_test(true, json!({"ok": true}), "", false)),
            Some(&fence),
            &mut tape_seq,
            |_| {
                retention_count += 1;
                Ok(())
            },
        )
        .await;

        assert!(matches!(result, Err(super::ToolOutcomeFinalizationError::BeforeSettlement(_))));
        assert_eq!(retention_count, 1, "the projection must be retained exactly once");
        assert_tool_fence_state(&state, &fence, SideEffectFenceState::EffectUnknown);
    }

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
    fn registry_replay_safety_maps_to_restart_semantics() {
        let read_only = tool_execution_semantics("palyra.echo", ToolReplaySafetyClass::ReadOnly);
        assert_eq!(read_only.idempotency_class, RuntimeIdempotencyClass::ReadOnly);
        assert_eq!(read_only.restart_policy, SideEffectRestartPolicy::SafeRetry);
        assert_eq!(read_only.reconciliation_strategy, ReconciliationStrategy::None);
        read_only.validate().expect("read-only semantics should validate");

        let workspace = tool_execution_semantics(
            "palyra.fs.apply_patch",
            ToolReplaySafetyClass::ExternalSideEffect,
        );
        assert_eq!(workspace.idempotency_class, RuntimeIdempotencyClass::ReconciliableMutation);
        assert_eq!(workspace.restart_policy, SideEffectRestartPolicy::ReconcileBeforeRetry);
        assert_eq!(workspace.reconciliation_strategy, ReconciliationStrategy::WorkspaceDigest);
        workspace.validate().expect("workspace semantics should validate");

        let process = tool_execution_semantics(
            "palyra.process.run",
            ToolReplaySafetyClass::ExternalSideEffect,
        );
        assert_eq!(process.reconciliation_strategy, ReconciliationStrategy::ProcessProvenance);
        process.validate().expect("process semantics should validate");

        let confirmed = tool_execution_semantics(
            "palyra.browser.type",
            ToolReplaySafetyClass::RequiresHumanConfirmation,
        );
        assert_eq!(confirmed.idempotency_class, RuntimeIdempotencyClass::NonIdempotent);
        assert_eq!(confirmed.restart_policy, SideEffectRestartPolicy::RequireConfirmation);
        confirmed.validate().expect("confirmation semantics should validate");
    }

    #[test]
    fn replay_safety_allows_read_only_tool_results() {
        let outcome = tool_outcome_for_replay_test(true, json!({"text": "hello"}), "", false);

        let report = classify_tool_result_replay_safety(
            "proposal-1",
            "palyra.fs.read_file",
            br#"{"path":"README.md"}"#,
            &outcome,
        );

        assert_eq!(report.event_type, TOOL_RESULT_REPLAY_SAFETY_EVENT);
        assert_eq!(report.replay_safety, "read_only");
        assert!(report.provider_retry_allowed);
        assert!(!report.explicit_guard_required);
    }

    #[test]
    fn replay_safety_requires_guard_for_mutating_timeouts() {
        let outcome = tool_outcome_for_replay_test(
            false,
            json!({"error": "timed out while applying patch"}),
            "timed out",
            true,
        );

        let report = classify_tool_result_replay_safety(
            "proposal-1",
            "palyra.fs.apply_patch",
            br#"{"patch":"diff --git a/a b/a"}"#,
            &outcome,
        );

        assert_eq!(report.replay_safety, "mutating_requires_guard");
        assert!(!report.provider_retry_allowed);
        assert!(report.explicit_guard_required);
        assert_eq!(report.reason_code, "tool_replay.mutating_timeout_requires_guard");
        let runtime_error = project_legacy_runtime_error(
            report.reason_code.as_str(),
            RuntimeErrorObservation {
                output_emitted: false,
                side_effect_may_have_occurred: report.explicit_guard_required,
            },
            "mutating tool outcome is unknown after timeout",
            "reconcile the tool effect before an idempotency-guarded retry",
        )
        .expect("typed replay-safety report should project");
        assert_eq!(runtime_error.class(), RuntimeErrorClass::ToolExecutionUnknown);
        assert_eq!(runtime_error.retryability(), RuntimeRetryability::RequiresIdempotencyGuard);
        assert!(runtime_error.side_effect_may_have_occurred());
    }

    #[test]
    fn replay_safety_allows_only_host_synthetic_results() {
        let outcome = tool_outcome_for_replay_test(
            false,
            json!({
                "schema_version": 1,
                "host_generated": true,
                "kind": "synthetic_tool_result",
                "reason_code": "tool_guardrail.invalid_arguments"
            }),
            "invalid arguments",
            false,
        );

        let report = classify_tool_result_replay_safety(
            "proposal-1",
            "palyra.fs.apply_patch",
            br#"{"patch":"bad"}"#,
            &outcome,
        );

        assert_eq!(report.replay_safety, "host_synthetic_result");
        assert!(report.provider_retry_allowed);
        assert!(report.synthetic_result_allowed);
        assert!(!report.explicit_guard_required);
    }

    #[test]
    fn sessions_spawn_tape_payload_projects_child_refs_only() {
        let output = json!({
            "task_id": "task-1",
            "child_run_id": "child-run",
            "child_session_id": "session-1",
            "state": "queued",
            "task": "read https://example.com/callback?access_token=secret",
            "transcript_ref": {
                "kind": "orchestrator_run_tape",
                "status": "pending",
                "run_id": "child-run",
                "session_id": "session-1"
            }
        });

        let payload =
            sessions_spawn_tape_payload("proposal-1", "sessions_spawn", "parent-run", &output)
                .expect("spawn output with child_run_id should produce tape payload");
        let payload_text = payload.to_string();

        assert_eq!(payload["schema_version"], 1);
        assert_eq!(payload["proposal_id"], "proposal-1");
        assert_eq!(payload["tool_name"], "sessions_spawn");
        assert_eq!(payload["parent_run_id"], "parent-run");
        assert_eq!(payload["child_run_id"], "child-run");
        assert_eq!(payload["child_session_id"], "session-1");
        assert_eq!(payload["transcript_ref"]["run_id"], "child-run");
        assert!(!payload_text.contains("access_token"));
        assert!(!payload_text.contains("secret"));
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

    #[test]
    fn projection_policy_audit_event_uses_shared_contract_labels() {
        assert_eq!(TOOL_RESULT_PROJECTION_POLICY_EVENT, "tool.result.projection_policy");
        assert_eq!(
            projection_policy_contract(ToolResultProjectionPolicy::InlineUnlessLarge),
            ToolResultProjectionPolicyKind::InlineUnlessLarge
        );
        assert_eq!(
            projection_policy_contract(ToolResultProjectionPolicy::SummarizeAndArtifact),
            ToolResultProjectionPolicyKind::SummarizeAndArtifact
        );
        assert_eq!(
            projection_policy_contract(ToolResultProjectionPolicy::RedactedPreviewAndArtifact),
            ToolResultProjectionPolicyKind::RedactedPreviewAndArtifact
        );
    }

    #[tokio::test]
    async fn process_progress_channel_is_bounded_and_coalesces_overflow() {
        let (sink, receiver) = process_progress_channel_for_tool("palyra.process.run", true);
        let sink = sink.expect("process progress sink should be enabled");
        let mut receiver = receiver.expect("process progress receiver should be enabled");
        for elapsed_ms in 0..100 {
            sink(ProcessProgressEvent {
                pid: 42,
                elapsed_ms,
                stdout_bytes: 0,
                stderr_bytes: 0,
                stdout_tail: String::new(),
                stderr_tail: String::new(),
                last_output_at_ms: None,
            });
        }
        let (latest, coalesced) = receiver.recv().await.expect("latest progress snapshot");
        assert_eq!(latest.elapsed_ms, 99);
        assert_eq!(coalesced, 99);
        assert_eq!(receiver.notifications.len(), 0);
    }

    #[tokio::test]
    async fn zero_process_progress_coalescing_emits_no_backpressure_event() {
        let state = test_runtime_state();
        let session_id = "session_process_progress_no_backpressure";
        let run_id = "run_process_progress_no_backpressure";
        start_backpressure_test_run(&state, session_id, run_id);
        let mut tape_seq = 0;

        append_process_progress_backpressure_tape_event(&state, run_id, &mut tape_seq, 0)
            .await
            .expect("zero coalescing should be a no-op");

        assert_eq!(tape_seq, 0);
        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("journal database should reopen");
        let tape_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM orchestrator_tape WHERE run_ulid = ?1",
                rusqlite::params![run_id],
                |row| row.get(0),
            )
            .expect("tape count should load");
        let runtime_event_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM runtime_events_v2 WHERE run_ulid = ?1",
                rusqlite::params![run_id],
                |row| row.get(0),
            )
            .expect("runtime event count should load");
        assert_eq!(tape_count, 0);
        assert_eq!(runtime_event_count, 0);
    }

    #[tokio::test]
    async fn coalesced_process_progress_emits_metadata_only_backpressure_event() {
        let state = test_runtime_state();
        let session_id = "session_process_progress_backpressure";
        let run_id = "run_process_progress_backpressure";
        start_backpressure_test_run(&state, session_id, run_id);
        let mut tape_seq = 0;

        append_process_progress_backpressure_tape_event(&state, run_id, &mut tape_seq, 7)
            .await
            .expect("coalescing should persist backpressure evidence");

        assert_eq!(tape_seq, 1);
        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("journal database should reopen");
        let (event_type, payload_json): (String, String) = connection
            .query_row(
                "SELECT event_type, payload_json FROM orchestrator_tape WHERE run_ulid = ?1 AND seq = 0",
                rusqlite::params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("backpressure tape row should exist");
        assert_eq!(event_type, super::PROCESS_PROGRESS_BACKPRESSURE_TAPE_EVENT);
        let tape_payload: Value =
            serde_json::from_str(payload_json.as_str()).expect("tape payload should decode");
        assert_eq!(tape_payload["boundary"], "process_progress");
        assert_eq!(tape_payload["overflow_action"], "coalesce_progress");
        assert_eq!(tape_payload["coalesced_count"], 7);
        assert_eq!(tape_payload["capacity"], 1);
        assert_eq!(tape_payload["reason_code"], super::PROCESS_PROGRESS_BACKPRESSURE_REASON_CODE);
        assert!(!payload_json.contains("prompt"));
        assert!(!payload_json.contains("stdout_tail"));
        assert!(!payload_json.contains("stderr_tail"));

        let raw_envelope = connection
            .query_row(
                "SELECT envelope_json FROM runtime_events_v2 WHERE run_ulid = ?1 AND event_name = ?2",
                rusqlite::params![run_id, RuntimeEventName::BackpressureApplied.as_str()],
                |row| row.get::<_, String>(0),
            )
            .expect("canonical backpressure event should exist");
        let envelope: RuntimeEventEnvelopeV2 =
            serde_json::from_str(raw_envelope.as_str()).expect("runtime event should decode");
        envelope.validate().expect("runtime event should validate");
        assert_eq!(envelope.event_name, RuntimeEventName::BackpressureApplied);
        assert_eq!(envelope.reason_code, super::PROCESS_PROGRESS_BACKPRESSURE_REASON_CODE);
        assert!(matches!(
            envelope.payload,
            RuntimeEventPayloadRef::Inline { ref metadata }
                if metadata["boundary"] == "process_progress"
                    && metadata["coalesced_count"] == 7
        ));
        assert!(!raw_envelope.contains("prompt"));
        assert!(!raw_envelope.contains("stdout_tail"));
        assert!(!raw_envelope.contains("stderr_tail"));
    }

    #[tokio::test]
    async fn process_progress_backpressure_rejects_inactive_generation() {
        let state = test_runtime_state();
        let mut tape_seq = 0;

        let error = append_process_progress_backpressure_tape_event(
            &state,
            "run_process_progress_inactive",
            &mut tape_seq,
            1,
        )
        .await
        .expect_err("inactive generation must fail closed");

        assert_eq!(error.code(), Code::Aborted);
        assert_eq!(tape_seq, 0);
    }

    #[test]
    fn process_progress_status_message_is_structured_json() {
        let message = process_progress_status_message(
            "proposal-1",
            &ProcessProgressEvent {
                pid: 42,
                elapsed_ms: 6_000,
                stdout_bytes: 128,
                stderr_bytes: 9,
                stdout_tail: "progress line".to_owned(),
                stderr_tail: "warning".to_owned(),
                last_output_at_ms: Some(5_800),
            },
        );
        let parsed: Value =
            serde_json::from_str(message.as_str()).expect("progress status should be JSON");

        assert_eq!(parsed["event"], "tool.process.progress");
        assert_eq!(parsed["proposal_id"], "proposal-1");
        assert_eq!(parsed["pid"], 42);
        assert_eq!(parsed["elapsed_ms"], 6_000);
        assert_eq!(parsed["stdout_tail"], "progress line");
        assert_eq!(parsed["stderr_tail"], "warning");
        assert_eq!(parsed["last_output_at_ms"], 5_800);
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

        let settlement =
            drain_parallel_tool_group_after_cancel(join_set, Duration::from_millis(100))
                .await
                .expect("drain should wait for sibling tasks");

        assert_eq!(settlement, CancellationSettlementOutcome::Graceful);
        assert!(
            sibling_completed.load(Ordering::SeqCst),
            "parallel cancellation must wait instead of aborting sibling tasks"
        );
    }

    #[tokio::test]
    async fn parallel_cancel_drain_reports_cleanup_unknown_after_budget() {
        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            std::future::pending::<()>().await;
            Ok(ParallelToolExecutionTaskOutcome::Cancelled)
        });

        let settlement =
            drain_parallel_tool_group_after_cancel(join_set, Duration::from_millis(10))
                .await
                .expect("hung sibling should produce a bounded settlement");

        assert_eq!(settlement, CancellationSettlementOutcome::CleanupUnknown);
    }

    #[tokio::test]
    async fn parallel_error_drain_waits_for_sibling_tasks() {
        let sibling_completed = Arc::new(AtomicBool::new(false));
        let sibling_completed_for_task = Arc::clone(&sibling_completed);
        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            sleep(Duration::from_millis(10)).await;
            sibling_completed_for_task.store(true, Ordering::SeqCst);
            Ok(ParallelToolExecutionTaskOutcome::Cancelled)
        });

        let error = drain_parallel_tool_group_after_error(
            join_set,
            Status::internal("primary error"),
            Duration::from_millis(100),
        )
        .await;

        assert_eq!(error.code(), Code::Internal);
        assert_eq!(error.message(), "primary error");
        assert!(
            sibling_completed.load(Ordering::SeqCst),
            "parallel errors must drain sibling tasks instead of aborting them"
        );
    }

    #[test]
    fn cancelled_workspace_write_tools_do_not_drain_execution() {
        assert!(
            !crate::gateway::tool_cancellation_requires_execution_drain(
                crate::gateway::WORKSPACE_PATCH_TOOL_NAME
            ),
            "cancelled run-stream apply_patch calls must be skipped before workspace mutation"
        );
        assert!(
            !crate::gateway::tool_cancellation_requires_execution_drain(
                crate::gateway::OS_FILE_TOOL_NAME
            ),
            "cancelled run-stream os_file writes must be skipped before OS mutation"
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

        let preview = super::redacted_tool_result_preview(
            "palyra.browser.screenshot",
            output_json.as_slice(),
            1024,
        );

        assert!(preview.contains("\"horizontal_overflow\":true"), "{preview}");
        assert!(preview.contains("\"document_scroll_width\":980"), "{preview}");
        assert!(preview.contains("\"mime_type\":\"image/png\""), "{preview}");
        assert!(preview.contains("\"size_bytes\":3072"), "{preview}");
        assert!(preview.contains("\"omitted_image_payload\""), "{preview}");
        assert!(preview.contains("\"base64_chars\":4096"), "{preview}");
        assert!(!preview.contains("image_base64"), "{preview}");
        assert!(!preview.contains("AAAA"), "{preview}");
    }

    #[test]
    fn browser_observe_preview_prioritizes_selector_captures() {
        let output_json = serde_json::to_vec(&json!({
            "success": true,
            "dom_snapshot": "D".repeat(4096),
            "accessibility_tree": "",
            "visible_text": "",
            "element_captures": [
                {
                    "selector": "#state-badge",
                    "matched": true,
                    "text": "done",
                    "computed_style": {
                        "opacity": "1",
                    }
                }
            ],
            "dom_truncated": true,
            "accessibility_tree_truncated": false,
            "visible_text_truncated": false,
            "page_url": "http://127.0.0.1:8857/",
            "safety": {
                "redacted": false,
            },
            "error": "",
        }))
        .expect("test payload should serialize");

        let preview = super::redacted_tool_result_preview(
            crate::gateway::BROWSER_OBSERVE_TOOL_NAME,
            output_json.as_slice(),
            1024,
        );

        assert!(preview.contains("\"element_captures\""), "{preview}");
        assert!(preview.contains("#state-badge"), "{preview}");
        assert!(preview.contains("\"text\":\"done\""), "{preview}");
        assert!(preview.contains("\"opacity\":\"1\""), "{preview}");
        assert!(preview.contains("\"dom_snapshot_bytes\":4096"), "{preview}");
        assert!(!preview.contains("DDDDDDDD"), "{preview}");
    }

    #[test]
    fn tool_result_projection_preview_preserves_benign_source_structure() {
        let source = "const match = document.cookie.match(/(?:^|; )theme=([^;]*)/);\n\
                      const fixture = 'token=a%3Db%3Dc';\n\
                      const selector = '#password';\n\
                      const password=document.querySelector('#password').value;\n\
                      const saved=localStorage.getItem('mock-session');\n\
                      if (username !== 'demo' || password !== 'demo') throw new Error('bad login');\n\
                      localStorage.setItem('mock-session', JSON.stringify({ username: 'demo', password: 'demo/demo' }));\n";
        let output_json = serde_json::to_vec(&json!({
            "path": "app.js",
            "text": source,
            "binary": false,
            "redacted": false,
        }))
        .expect("test payload should serialize");

        let preview = super::redacted_tool_result_preview(
            crate::gateway::WORKSPACE_READ_FILE_TOOL_NAME,
            output_json.as_slice(),
            4096,
        );

        assert!(preview.contains("document.cookie.match(/(?:^|; )theme=([^;]*)/)"), "{preview}");
        assert!(preview.contains("token=a%3Db%3Dc"), "{preview}");
        assert!(preview.contains("#password"), "{preview}");
        assert!(
            preview.contains("password=document.querySelector('#password').value"),
            "{preview}"
        );
        assert!(preview.contains("localStorage.getItem('mock-session')"), "{preview}");
        assert!(preview.contains("password !== 'demo'"), "{preview}");
        assert!(preview.contains("demo/demo"), "{preview}");
        assert!(!preview.contains("<redacted>"), "{preview}");
    }

    #[test]
    fn tool_result_projection_preview_still_redacts_workspace_text_when_read_file_did() {
        let output_json = serde_json::to_vec(&json!({
            "path": ".env",
            "text": "APP_SECRET=[REDACTED_SECRET]\n",
            "binary": false,
            "redacted": true,
        }))
        .expect("test payload should serialize");

        let preview = super::redacted_tool_result_preview(
            crate::gateway::WORKSPACE_READ_FILE_TOOL_NAME,
            output_json.as_slice(),
            1024,
        );

        assert!(preview.contains("APP_SECRET=<redacted>"), "{preview}");
        assert!(!preview.contains("[REDACTED_SECRET]"), "{preview}");
    }

    #[test]
    fn clean_workspace_read_file_text_artifacts_are_public() {
        let output_json = serde_json::to_vec(&json!({
            "path": "src/app.js",
            "workspace_root_index": 0,
            "offset_bytes": 0,
            "returned_bytes": 29,
            "size_bytes": 29,
            "eof": true,
            "chunk_sha256": "0".repeat(64),
            "text": "export const ok = true;\n",
            "binary": false,
            "redacted": false,
        }))
        .expect("test payload should serialize");

        assert_eq!(
            super::tool_result_sensitivity(
                crate::gateway::WORKSPACE_READ_FILE_TOOL_NAME,
                output_json.as_slice(),
                false,
            ),
            ToolResultSensitivity::Public
        );
    }

    #[test]
    fn unsafe_workspace_read_file_artifacts_stay_internal_path() {
        let redacted_output = serde_json::to_vec(&json!({
            "path": ".env",
            "text": "APP_SECRET=[REDACTED_SECRET]\n",
            "binary": false,
            "redacted": true,
        }))
        .expect("test payload should serialize");
        let binary_output = serde_json::to_vec(&json!({
            "path": "assets/logo.bin",
            "bytes_base64": "AAAA",
            "binary": true,
            "redacted": false,
        }))
        .expect("test payload should serialize");
        let host_path_output = serde_json::to_vec(&json!({
            "path": "C:\\Users\\alice\\repo\\src\\main.rs",
            "text": "fn main() {}\n",
            "binary": false,
            "redacted": false,
        }))
        .expect("test payload should serialize");

        for output_json in [redacted_output, binary_output, host_path_output] {
            assert_eq!(
                super::tool_result_sensitivity(
                    crate::gateway::WORKSPACE_READ_FILE_TOOL_NAME,
                    output_json.as_slice(),
                    false,
                ),
                ToolResultSensitivity::InternalPath
            );
        }
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
                execution_manifest: None,
            },
        };

        assert!(super::should_project_tool_result_for_model(
            "palyra.browser.click",
            &outcome,
            &ToolTurnBudget::default()
        ));
        let preview = super::redacted_tool_result_preview(
            "palyra.browser.click",
            outcome.output_json.as_slice(),
            1024,
        );
        assert!(preview.contains("\"omitted_screenshot_payload\""), "{preview}");
        assert!(preview.contains("\"base64_chars\":4096"), "{preview}");
        assert!(preview.contains("\"encoding\":\"base64\""), "{preview}");
        assert!(preview.contains("\"kind\":\"screenshot\""), "{preview}");
        assert!(preview.contains("\"omitted\":true"), "{preview}");
        assert!(!preview.contains("failure_screenshot_base64"), "{preview}");
        assert!(!preview.contains("BBBB"), "{preview}");
    }

    #[test]
    fn os_file_list_dir_results_use_redacted_projection_when_under_model_budget() {
        let output_json = serde_json::to_vec(&json!({
            "operation": "list_dir",
            "path": "/home/example/.cache/palyra/os-cache",
            "resolved_path": "/home/example/.cache/palyra/os-cache",
            "entries": [
                {
                    "name": "zero-a.tmp",
                    "path": "/home/example/.cache/palyra/os-cache/zero-a.tmp",
                    "resolved_path": "/home/example/.cache/palyra/os-cache/zero-a.tmp",
                    "kind": "file",
                    "size_bytes": 0
                },
                {
                    "name": "f-newest.cache",
                    "path": "/home/example/.cache/palyra/os-cache/f-newest.cache",
                    "resolved_path": "/home/example/.cache/palyra/os-cache/f-newest.cache",
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
                execution_manifest: None,
            },
        };

        assert!(
            super::should_project_tool_result_for_model(
                crate::gateway::OS_FILE_TOOL_NAME,
                &outcome,
                &ToolTurnBudget::default()
            ),
            "small OS list_dir metadata must use RedactedPreviewAndArtifact projection"
        );
    }

    #[test]
    fn workspace_spill_projection_keeps_sensitive_artifacts_journal_scoped() {
        let artifact = ToolResultArtifactRef {
            artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FAE".to_owned(),
            digest_sha256: "a".repeat(64),
            mime_type: "application/json".to_owned(),
            size_bytes: 1024,
            sensitivity: ToolResultSensitivity::Secret,
            retention: ArtifactRetentionPolicy::keep(),
            origin_tool_call_id: "call-secret".to_owned(),
            tool_name: "palyra.browser.click".to_owned(),
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAF".to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAG".to_owned(),
            storage_backend: "journal".to_owned(),
            redacted_preview: "{}".to_owned(),
            created_at_unix_ms: 1,
        };

        let spill = workspace_spill_unavailable_projection(
            &artifact,
            "workspace_spill.sensitive_output_requires_policy_grant",
        );

        assert!(!workspace_spill_policy_grants_sensitivity(ToolResultSensitivity::Secret));
        assert!(workspace_spill_policy_grants_sensitivity(ToolResultSensitivity::StdoutStderr));
        assert_eq!(spill.pointer("/created").and_then(Value::as_bool), Some(false));
        assert_eq!(
            spill.pointer("/reason_code").and_then(Value::as_str),
            Some("workspace_spill.sensitive_output_requires_policy_grant")
        );
        assert_eq!(
            spill.pointer("/canonical_source").and_then(Value::as_str),
            Some("journal_tool_result_artifact")
        );
        assert_eq!(
            spill.pointer("/read_tools/0").and_then(Value::as_str),
            Some("palyra.artifact.read")
        );
        assert_eq!(
            spill.pointer("/policy/requires_sensitive_output_grant").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn os_file_redacted_read_text_results_stay_inline_when_under_model_budget() {
        let output_json = serde_json::to_vec(&json!({
            "operation": "read",
            "path": "/home/example/.config/palyra-e2e/settings.toml",
            "resolved_path": "/home/example/.config/palyra-e2e/settings.toml",
            "text": "[auth]\nrefresh_token = \"[REDACTED_SECRET]\"\nmode = \"local\"\n",
            "text_authoritative": false,
            "redaction_notice": "text contains redacted secret placeholders; use it for structure only and do not write the redacted text back verbatim",
            "bytes_base64": null,
            "redacted": true,
            "size_bytes": 96,
        }))
        .expect("test payload should serialize");
        let outcome = ToolExecutionOutcome {
            success: true,
            output_json,
            error: String::new(),
            attestation: ToolAttestation {
                attestation_id: "01ARZ3NDEKTSV4RRFFQ69G5FAC".to_owned(),
                execution_sha256: "0".repeat(64),
                executed_at_unix_ms: 0,
                timed_out: false,
                executor: "test".to_owned(),
                sandbox_enforcement: "n/a".to_owned(),
                execution_manifest: None,
            },
        };

        assert!(
            !super::should_project_tool_result_for_model(
                crate::gateway::OS_FILE_TOOL_NAME,
                &outcome,
                &ToolTurnBudget::default()
            ),
            "small redacted OS read text must stay model-visible for config audits"
        );
    }

    #[test]
    fn os_file_read_text_results_require_redaction_metadata_to_stay_inline() {
        let base_payload = json!({
            "operation": "read",
            "path": "/home/example/.config/palyra-e2e/session.txt",
            "resolved_path": "/home/example/.config/palyra-e2e/session.txt",
            "text": "oauth_callback=https://idp.example/callback?ticket=CORPLOGIN-9f2c7a84&nonce=N-44129\n",
            "bytes_base64": null,
            "size_bytes": 91,
        });
        let mut unredacted_authoritative = base_payload.clone();
        unredacted_authoritative["redacted"] = json!(false);
        unredacted_authoritative["text_authoritative"] = json!(true);
        let mut missing_redacted = base_payload.clone();
        missing_redacted["text_authoritative"] = json!(false);
        let mut missing_text_authority = base_payload;
        missing_text_authority["redacted"] = json!(true);
        let cases = [
            ("unredacted authoritative text", unredacted_authoritative),
            ("missing redacted marker", missing_redacted),
            ("missing text authority marker", missing_text_authority),
        ];

        for (case, payload) in cases {
            let outcome = ToolExecutionOutcome {
                success: true,
                output_json: serde_json::to_vec(&payload).expect("test payload should serialize"),
                error: String::new(),
                attestation: ToolAttestation {
                    attestation_id: "01ARZ3NDEKTSV4RRFFQ69G5FAD".to_owned(),
                    execution_sha256: "0".repeat(64),
                    executed_at_unix_ms: 0,
                    timed_out: false,
                    executor: "test".to_owned(),
                    sandbox_enforcement: "n/a".to_owned(),
                    execution_manifest: None,
                },
            };

            assert!(
                super::should_project_tool_result_for_model(
                    crate::gateway::OS_FILE_TOOL_NAME,
                    &outcome,
                    &ToolTurnBudget::default()
                ),
                "{case} must use RedactedPreviewAndArtifact projection"
            );
        }
    }
}
