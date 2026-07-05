//! Turn control plane decisions for run and queue operations.
//!
//! This module is intentionally side-effect free. It validates the requested
//! operator action, selects the existing runtime operation to call, and emits a
//! journal-ready audit projection. Runtime code owns the actual cancel, pause,
//! resume, and queued-input priority writes.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const TURN_CONTROL_SCHEMA_VERSION: i64 = 1;
pub(crate) const TURN_CONTROL_EVENT_STARTED: &str = "turncontrolplane_zakladni_operace.started";
pub(crate) const TURN_CONTROL_EVENT_COMPLETED: &str = "turncontrolplane_zakladni_operace.completed";
pub(crate) const TURN_CONTROL_EVENT_FAILED: &str = "turncontrolplane_zakladni_operace.failed";
pub(crate) const TURN_CONTROL_REDACTION_NONE: &str = "none";
pub(crate) const TURN_CONTROL_REDACTION_REDACTED: &str = "redacted";

/// User-facing control command vocabulary shared by API, CLI, and audit events.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ControlCommand {
    Status,
    Cancel,
    Pause,
    Redirect,
    Resume,
    Steer,
    Yield,
}

impl ControlCommand {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Cancel => "cancel",
            Self::Pause => "pause",
            Self::Redirect => "redirect",
            Self::Resume => "resume",
            Self::Steer => "steer",
            Self::Yield => "yield",
        }
    }

    #[must_use]
    pub(crate) const fn operation(self) -> TurnControlOperation {
        match self {
            Self::Status => TurnControlOperation::Status,
            Self::Cancel => TurnControlOperation::CancelRun,
            Self::Pause => TurnControlOperation::PauseQueue,
            Self::Redirect => TurnControlOperation::RedirectRun,
            Self::Resume => TurnControlOperation::ResumeQueue,
            Self::Steer => TurnControlOperation::PrioritizeQueuedInput,
            Self::Yield => TurnControlOperation::YieldRun,
        }
    }

    #[must_use]
    pub(crate) fn audit_event_type(self, outcome: &str) -> String {
        format!("turn_control.{}.{outcome}", self.as_str())
    }
}

/// Active phase used to explain why a control command applied immediately or
/// entered the queue for a later safe boundary.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ControlActivePhase {
    ProviderStream,
    ToolExecution,
    ApprovalPending,
    Queue,
    BackgroundTask,
    Idle,
}

impl ControlActivePhase {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderStream => "provider_stream",
            Self::ToolExecution => "tool_execution",
            Self::ApprovalPending => "approval_pending",
            Self::Queue => "queue",
            Self::BackgroundTask => "background_task",
            Self::Idle => "idle",
        }
    }
}

/// Decision-time safety boundary attached to every control command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ControlSafetyBoundary {
    pub command: ControlCommand,
    pub active_phase: ControlActivePhase,
    pub target_kind: String,
    pub deferred: bool,
    pub reason_code: String,
}

/// Basic control-plane operation selected by an operator surface.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnControlOperation {
    Status,
    CancelRun,
    RedirectRun,
    PauseQueue,
    ResumeQueue,
    PrioritizeQueuedInput,
    YieldRun,
}

impl TurnControlOperation {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::CancelRun => "cancel_run",
            Self::RedirectRun => "redirect_run",
            Self::PauseQueue => "pause_queue",
            Self::ResumeQueue => "resume_queue",
            Self::PrioritizeQueuedInput => "prioritize_queued_input",
            Self::YieldRun => "yield_run",
        }
    }

    #[must_use]
    pub(crate) const fn target_kind(self) -> &'static str {
        match self {
            Self::Status | Self::CancelRun | Self::RedirectRun | Self::YieldRun => "run",
            Self::PauseQueue | Self::ResumeQueue => "session_queue",
            Self::PrioritizeQueuedInput => "queued_input",
        }
    }

    #[must_use]
    pub(crate) const fn control_command(self) -> ControlCommand {
        match self {
            Self::Status => ControlCommand::Status,
            Self::CancelRun => ControlCommand::Cancel,
            Self::RedirectRun => ControlCommand::Redirect,
            Self::PauseQueue => ControlCommand::Pause,
            Self::ResumeQueue => ControlCommand::Resume,
            Self::PrioritizeQueuedInput => ControlCommand::Steer,
            Self::YieldRun => ControlCommand::Yield,
        }
    }

    #[must_use]
    pub(crate) const fn default_active_phase(self) -> ControlActivePhase {
        match self {
            Self::Status | Self::CancelRun | Self::RedirectRun | Self::YieldRun => {
                ControlActivePhase::ProviderStream
            }
            Self::PauseQueue | Self::ResumeQueue | Self::PrioritizeQueuedInput => {
                ControlActivePhase::Queue
            }
        }
    }
}

/// Runtime action chosen by the decision layer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnControlAction {
    Observe,
    RequestRunCancel,
    EnqueueRedirect,
    SetQueuePaused,
    SetQueuedInputPriority,
    Yield,
    Reject,
}

impl TurnControlAction {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::RequestRunCancel => "request_run_cancel",
            Self::EnqueueRedirect => "enqueue_redirect",
            Self::SetQueuePaused => "set_queue_paused",
            Self::SetQueuedInputPriority => "set_queued_input_priority",
            Self::Yield => "yield",
            Self::Reject => "reject",
        }
    }
}

/// Stable reason codes for turn-control decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnControlReasonCode {
    StatusObserved,
    RunCancelSelected,
    RunRedirectSelected,
    QueuePauseSelected,
    QueueResumeSelected,
    QueuedInputPrioritySelected,
    RunYieldSelected,
    DryRun,
    MissingActor,
    MissingRunId,
    MissingSessionId,
    MissingQueuedInputId,
    MissingPriorityLane,
    MissingRedirectInstruction,
}

impl TurnControlReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StatusObserved => "turn_control.status_observed",
            Self::RunCancelSelected => "turn_control.run_cancel_selected",
            Self::RunRedirectSelected => "turn_control.run_redirect_selected",
            Self::QueuePauseSelected => "turn_control.queue_pause_selected",
            Self::QueueResumeSelected => "turn_control.queue_resume_selected",
            Self::QueuedInputPrioritySelected => "turn_control.queued_input_priority_selected",
            Self::RunYieldSelected => "turn_control.run_yield_selected",
            Self::DryRun => "turn_control.dry_run",
            Self::MissingActor => "turn_control.missing_actor",
            Self::MissingRunId => "turn_control.missing_run_id",
            Self::MissingSessionId => "turn_control.missing_session_id",
            Self::MissingQueuedInputId => "turn_control.missing_queued_input_id",
            Self::MissingPriorityLane => "turn_control.missing_priority_lane",
            Self::MissingRedirectInstruction => "turn_control.missing_redirect_instruction",
        }
    }
}

/// Operator request accepted by the turn control plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TurnControlRequest {
    pub operation: TurnControlOperation,
    pub actor_principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_phase: Option<ControlActivePhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued_input_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority_lane: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instruction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub dry_run: bool,
}

/// Journal-ready projection attached to each decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TurnControlJournalProjection {
    pub started_event_type: String,
    pub terminal_event_type: String,
    pub reason_code: String,
    pub target_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    pub payload_json: String,
    pub evidence_refs_json: String,
    pub redaction_level: String,
}

/// Pure decision produced before any runtime side effect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TurnControlDecision {
    pub schema_version: i64,
    pub operation: TurnControlOperation,
    pub command: ControlCommand,
    pub action: TurnControlAction,
    pub accepted: bool,
    pub reason_code: String,
    pub actor_principal: String,
    pub active_phase: ControlActivePhase,
    pub safety_boundary: ControlSafetyBoundary,
    pub target_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator_reason: Option<String>,
    pub dry_run: bool,
    pub journal_projection: TurnControlJournalProjection,
}

/// Result returned after the runtime applies or previews a turn-control request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TurnControlApplyOutcome {
    pub decision: TurnControlDecision,
    pub effect: Value,
}

/// Surface that delivered an input while a run was already active.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActiveRunInputSurface {
    Cli,
    WebConsole,
    Channel,
    Automation,
    Advisor,
}

#[allow(dead_code)]
impl ActiveRunInputSurface {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::WebConsole => "web_console",
            Self::Channel => "channel",
            Self::Automation => "automation",
            Self::Advisor => "advisor",
        }
    }
}

/// Operator intent inferred before active-run input routing.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActiveRunInputIntent {
    Followup,
    Steer,
    Interrupt,
    Cancel,
    ApprovalResponse,
    RoutineTick,
    Unknown,
}

#[allow(dead_code)]
impl ActiveRunInputIntent {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Followup => "followup",
            Self::Steer => "steer",
            Self::Interrupt => "interrupt",
            Self::Cancel => "cancel",
            Self::ApprovalResponse => "approval_response",
            Self::RoutineTick => "routine_tick",
            Self::Unknown => "unknown",
        }
    }
}

/// Deterministic route selected for input arriving during an active run.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActiveRunInputAction {
    Queue,
    Steer,
    Interrupt,
    Cancel,
    Ignore,
    Merge,
}

#[allow(dead_code)]
impl ActiveRunInputAction {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Steer => "steer",
            Self::Interrupt => "interrupt",
            Self::Cancel => "cancel",
            Self::Ignore => "ignore",
            Self::Merge => "merge",
        }
    }
}

/// Stable reason code for active-run input policy decisions.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActiveRunInputReasonCode {
    UnauthorizedActor,
    AdvisorNotAuthoritative,
    UnknownIntent,
    CancelRequested,
    ApprovalResponseInterrupts,
    ExplicitInterrupt,
    SteeringAccepted,
    SteeringQueuedAtBoundary,
    MergeWindowOpen,
    FollowupQueued,
    RoutineQueued,
    QueuePaused,
}

#[allow(dead_code)]
impl ActiveRunInputReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::UnauthorizedActor => "active_input.unauthorized_actor",
            Self::AdvisorNotAuthoritative => "active_input.advisor_not_authoritative",
            Self::UnknownIntent => "active_input.unknown_intent",
            Self::CancelRequested => "active_input.cancel_requested",
            Self::ApprovalResponseInterrupts => "active_input.approval_response_interrupts",
            Self::ExplicitInterrupt => "active_input.explicit_interrupt",
            Self::SteeringAccepted => "active_input.steering_accepted",
            Self::SteeringQueuedAtBoundary => "active_input.steering_queued_at_boundary",
            Self::MergeWindowOpen => "active_input.merge_window_open",
            Self::FollowupQueued => "active_input.followup_queued",
            Self::RoutineQueued => "active_input.routine_queued",
            Self::QueuePaused => "active_input.queue_paused",
        }
    }
}

/// Inputs needed to route a follow-up without touching runtime state.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ActiveRunInputPolicyRequest {
    pub surface: ActiveRunInputSurface,
    pub intent: ActiveRunInputIntent,
    pub active_phase: ControlActivePhase,
    pub actor_authorized: bool,
    pub queue_paused: bool,
    pub merge_window_open: bool,
    pub tool_drain_active: bool,
    pub approval_pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

/// Journal-ready projection for the active input decision.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ActiveRunInputPolicyDecision {
    pub schema_version: i64,
    pub action: ActiveRunInputAction,
    pub reason_code: String,
    pub accepted: bool,
    pub active_phase: ControlActivePhase,
    pub surface: ActiveRunInputSurface,
    pub intent: ActiveRunInputIntent,
    pub target_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    pub journal_payload_json: String,
}

#[must_use]
#[allow(dead_code)]
pub(crate) fn decide_active_run_input_policy(
    request: &ActiveRunInputPolicyRequest,
) -> ActiveRunInputPolicyDecision {
    if !request.actor_authorized {
        return active_input_decision(
            request,
            ActiveRunInputAction::Ignore,
            ActiveRunInputReasonCode::UnauthorizedActor,
            false,
        );
    }
    if request.surface == ActiveRunInputSurface::Advisor {
        return active_input_decision(
            request,
            ActiveRunInputAction::Ignore,
            ActiveRunInputReasonCode::AdvisorNotAuthoritative,
            false,
        );
    }
    if request.queue_paused
        && matches!(
            request.intent,
            ActiveRunInputIntent::Followup | ActiveRunInputIntent::RoutineTick
        )
    {
        return active_input_decision(
            request,
            ActiveRunInputAction::Ignore,
            ActiveRunInputReasonCode::QueuePaused,
            false,
        );
    }

    match request.intent {
        ActiveRunInputIntent::Cancel => active_input_decision(
            request,
            ActiveRunInputAction::Cancel,
            ActiveRunInputReasonCode::CancelRequested,
            true,
        ),
        ActiveRunInputIntent::ApprovalResponse if request.approval_pending => {
            active_input_decision(
                request,
                ActiveRunInputAction::Interrupt,
                ActiveRunInputReasonCode::ApprovalResponseInterrupts,
                true,
            )
        }
        ActiveRunInputIntent::Interrupt => active_input_decision(
            request,
            ActiveRunInputAction::Interrupt,
            ActiveRunInputReasonCode::ExplicitInterrupt,
            true,
        ),
        ActiveRunInputIntent::Steer => {
            if request.tool_drain_active
                || matches!(
                    request.active_phase,
                    ControlActivePhase::ToolExecution | ControlActivePhase::ApprovalPending
                )
            {
                active_input_decision(
                    request,
                    ActiveRunInputAction::Queue,
                    ActiveRunInputReasonCode::SteeringQueuedAtBoundary,
                    true,
                )
            } else {
                active_input_decision(
                    request,
                    ActiveRunInputAction::Steer,
                    ActiveRunInputReasonCode::SteeringAccepted,
                    true,
                )
            }
        }
        ActiveRunInputIntent::Followup if request.merge_window_open => active_input_decision(
            request,
            ActiveRunInputAction::Merge,
            ActiveRunInputReasonCode::MergeWindowOpen,
            true,
        ),
        ActiveRunInputIntent::Followup => active_input_decision(
            request,
            ActiveRunInputAction::Queue,
            ActiveRunInputReasonCode::FollowupQueued,
            true,
        ),
        ActiveRunInputIntent::RoutineTick => active_input_decision(
            request,
            ActiveRunInputAction::Queue,
            ActiveRunInputReasonCode::RoutineQueued,
            true,
        ),
        ActiveRunInputIntent::ApprovalResponse | ActiveRunInputIntent::Unknown => {
            active_input_decision(
                request,
                ActiveRunInputAction::Ignore,
                ActiveRunInputReasonCode::UnknownIntent,
                false,
            )
        }
    }
}

#[allow(dead_code)]
fn active_input_decision(
    request: &ActiveRunInputPolicyRequest,
    action: ActiveRunInputAction,
    reason_code: ActiveRunInputReasonCode,
    accepted: bool,
) -> ActiveRunInputPolicyDecision {
    let (target_kind, target_id) = active_input_target(request);
    let reason_code = reason_code.as_str().to_owned();
    let payload = json!({
        "schema_version": TURN_CONTROL_SCHEMA_VERSION,
        "event_type": "active_run_input.policy_decision",
        "action": action.as_str(),
        "reason_code": reason_code.as_str(),
        "accepted": accepted,
        "active_phase": request.active_phase.as_str(),
        "surface": request.surface.as_str(),
        "intent": request.intent.as_str(),
        "target_kind": target_kind,
        "target_id": target_id.as_deref(),
        "queue_paused": request.queue_paused,
        "merge_window_open": request.merge_window_open,
        "tool_drain_active": request.tool_drain_active,
        "approval_pending": request.approval_pending,
    });
    ActiveRunInputPolicyDecision {
        schema_version: TURN_CONTROL_SCHEMA_VERSION,
        action,
        reason_code,
        accepted,
        active_phase: request.active_phase,
        surface: request.surface,
        intent: request.intent,
        target_kind,
        target_id,
        journal_payload_json: payload.to_string(),
    }
}

#[allow(dead_code)]
fn active_input_target(request: &ActiveRunInputPolicyRequest) -> (String, Option<String>) {
    request
        .run_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|run_id| ("run".to_owned(), Some(run_id.to_owned())))
        .or_else(|| {
            request
                .session_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|session_id| ("session".to_owned(), Some(session_id.to_owned())))
        })
        .unwrap_or_else(|| ("none".to_owned(), None))
}

#[must_use]
pub(crate) fn decide_turn_control_request(request: &TurnControlRequest) -> TurnControlDecision {
    if request.actor_principal.trim().is_empty() {
        return rejected(request, TurnControlReasonCode::MissingActor, None);
    }

    match request.operation {
        TurnControlOperation::Status => accepted(
            request,
            TurnControlAction::Observe,
            TurnControlReasonCode::StatusObserved,
            request.run_id.as_deref().or(request.session_id.as_deref()).map(str::to_owned),
        ),
        TurnControlOperation::CancelRun => {
            let Some(run_id) = nonempty_optional(request.run_id.as_deref()) else {
                return rejected(request, TurnControlReasonCode::MissingRunId, None);
            };
            accepted(
                request,
                TurnControlAction::RequestRunCancel,
                TurnControlReasonCode::RunCancelSelected,
                Some(run_id.to_owned()),
            )
        }
        TurnControlOperation::RedirectRun => {
            let Some(run_id) = nonempty_optional(request.run_id.as_deref()) else {
                return rejected(request, TurnControlReasonCode::MissingRunId, None);
            };
            if nonempty_optional(request.instruction.as_deref()).is_none() {
                return rejected(
                    request,
                    TurnControlReasonCode::MissingRedirectInstruction,
                    Some(run_id.to_owned()),
                );
            }
            accepted(
                request,
                TurnControlAction::EnqueueRedirect,
                TurnControlReasonCode::RunRedirectSelected,
                Some(run_id.to_owned()),
            )
        }
        TurnControlOperation::PauseQueue => {
            let Some(session_id) = nonempty_optional(request.session_id.as_deref()) else {
                return rejected(request, TurnControlReasonCode::MissingSessionId, None);
            };
            accepted(
                request,
                TurnControlAction::SetQueuePaused,
                TurnControlReasonCode::QueuePauseSelected,
                Some(session_id.to_owned()),
            )
        }
        TurnControlOperation::ResumeQueue => {
            let Some(session_id) = nonempty_optional(request.session_id.as_deref()) else {
                return rejected(request, TurnControlReasonCode::MissingSessionId, None);
            };
            accepted(
                request,
                TurnControlAction::SetQueuePaused,
                TurnControlReasonCode::QueueResumeSelected,
                Some(session_id.to_owned()),
            )
        }
        TurnControlOperation::PrioritizeQueuedInput => {
            let Some(queued_input_id) = nonempty_optional(request.queued_input_id.as_deref())
            else {
                return rejected(request, TurnControlReasonCode::MissingQueuedInputId, None);
            };
            if nonempty_optional(request.priority_lane.as_deref()).is_none() {
                return rejected(
                    request,
                    TurnControlReasonCode::MissingPriorityLane,
                    Some(queued_input_id.to_owned()),
                );
            }
            accepted(
                request,
                TurnControlAction::SetQueuedInputPriority,
                TurnControlReasonCode::QueuedInputPrioritySelected,
                Some(queued_input_id.to_owned()),
            )
        }
        TurnControlOperation::YieldRun => accepted(
            request,
            TurnControlAction::Yield,
            TurnControlReasonCode::RunYieldSelected,
            request.run_id.as_deref().or(request.session_id.as_deref()).map(str::to_owned),
        ),
    }
}

fn accepted(
    request: &TurnControlRequest,
    action: TurnControlAction,
    reason_code: TurnControlReasonCode,
    target_id: Option<String>,
) -> TurnControlDecision {
    let reason_code = if request.dry_run { TurnControlReasonCode::DryRun } else { reason_code };
    decision(request, action, true, reason_code, target_id)
}

fn rejected(
    request: &TurnControlRequest,
    reason_code: TurnControlReasonCode,
    target_id: Option<String>,
) -> TurnControlDecision {
    decision(request, TurnControlAction::Reject, false, reason_code, target_id)
}

fn decision(
    request: &TurnControlRequest,
    action: TurnControlAction,
    accepted: bool,
    reason_code: TurnControlReasonCode,
    target_id: Option<String>,
) -> TurnControlDecision {
    let command = request.operation.control_command();
    let active_phase =
        request.active_phase.unwrap_or_else(|| request.operation.default_active_phase());
    let terminal_outcome = if accepted { "completed" } else { "failed" };
    let terminal_event_type = command.audit_event_type(terminal_outcome);
    let reason_code = reason_code.as_str().to_owned();
    let target_kind = request.operation.target_kind().to_owned();
    let safety_boundary = build_safety_boundary(command, active_phase, &target_kind);
    let operator_reason = request
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let payload_json = json!({
        "schema_version": TURN_CONTROL_SCHEMA_VERSION,
        "operation": request.operation.as_str(),
        "command": command.as_str(),
        "action": action.as_str(),
        "accepted": accepted,
        "reason_code": reason_code,
        "actor_principal": request.actor_principal.trim(),
        "active_phase": active_phase.as_str(),
        "safety_boundary": &safety_boundary,
        "target_kind": target_kind,
        "target_id": target_id.as_deref(),
        "session_id": request.session_id.as_deref(),
        "run_id": request.run_id.as_deref(),
        "queued_input_id": request.queued_input_id.as_deref(),
        "priority_lane": request.priority_lane.as_deref(),
        "instruction_present": nonempty_optional(request.instruction.as_deref()).is_some(),
        "operator_reason": operator_reason.as_deref(),
        "dry_run": request.dry_run,
    })
    .to_string();
    let evidence_refs_json = json!([{
        "kind": "turn_control_request",
        "operation": request.operation.as_str(),
        "command": command.as_str(),
        "active_phase": active_phase.as_str(),
        "target_kind": target_kind,
        "target_id": target_id.as_deref(),
    }])
    .to_string();
    TurnControlDecision {
        schema_version: TURN_CONTROL_SCHEMA_VERSION,
        operation: request.operation,
        command,
        action,
        accepted,
        reason_code: reason_code.clone(),
        actor_principal: request.actor_principal.trim().to_owned(),
        active_phase,
        safety_boundary,
        target_kind: target_kind.clone(),
        target_id: target_id.clone(),
        operator_reason,
        dry_run: request.dry_run,
        journal_projection: TurnControlJournalProjection {
            started_event_type: command.audit_event_type("started"),
            terminal_event_type: terminal_event_type.to_owned(),
            reason_code,
            target_kind,
            target_id,
            payload_json,
            evidence_refs_json,
            redaction_level: TURN_CONTROL_REDACTION_NONE.to_owned(),
        },
    }
}

fn build_safety_boundary(
    command: ControlCommand,
    active_phase: ControlActivePhase,
    target_kind: &str,
) -> ControlSafetyBoundary {
    let deferred = matches!(
        (command, active_phase),
        (
            ControlCommand::Redirect | ControlCommand::Steer,
            ControlActivePhase::ToolExecution | ControlActivePhase::ApprovalPending
        )
    );
    ControlSafetyBoundary {
        command,
        active_phase,
        target_kind: target_kind.to_owned(),
        deferred,
        reason_code: format!(
            "turn_control.boundary.{}.{}",
            active_phase.as_str(),
            command.as_str()
        ),
    }
}

fn nonempty_optional(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_request(operation: TurnControlOperation) -> TurnControlRequest {
        TurnControlRequest {
            operation,
            actor_principal: "user:ops".to_owned(),
            active_phase: None,
            session_id: None,
            run_id: None,
            queued_input_id: None,
            priority_lane: None,
            instruction: None,
            reason: Some("operator requested control".to_owned()),
            dry_run: false,
        }
    }

    #[test]
    fn cancel_run_decision_is_serializable_and_auditable() {
        let mut request = base_request(TurnControlOperation::CancelRun);
        request.run_id = Some("01ARZ3NDEKTSV4RRFFQ69G5RUN".to_owned());

        let decision = decide_turn_control_request(&request);

        assert!(decision.accepted);
        assert_eq!(decision.command, ControlCommand::Cancel);
        assert_eq!(decision.action, TurnControlAction::RequestRunCancel);
        assert_eq!(decision.reason_code, TurnControlReasonCode::RunCancelSelected.as_str());
        assert_eq!(decision.active_phase, ControlActivePhase::ProviderStream);
        assert_eq!(
            decision.safety_boundary.reason_code,
            "turn_control.boundary.provider_stream.cancel"
        );
        assert_eq!(decision.target_kind, "run");
        assert_eq!(decision.target_id.as_deref(), Some("01ARZ3NDEKTSV4RRFFQ69G5RUN"));
        assert_eq!(decision.journal_projection.started_event_type, "turn_control.cancel.started");
        assert_eq!(
            decision.journal_projection.terminal_event_type,
            "turn_control.cancel.completed"
        );

        let encoded = serde_json::to_string(&decision).expect("decision should serialize");
        let decoded: TurnControlDecision =
            serde_json::from_str(encoded.as_str()).expect("decision should deserialize");
        assert_eq!(decoded, decision);
    }

    #[test]
    fn prioritize_requires_priority_lane() {
        let mut request = base_request(TurnControlOperation::PrioritizeQueuedInput);
        request.queued_input_id = Some("queued-1".to_owned());

        let decision = decide_turn_control_request(&request);

        assert!(!decision.accepted);
        assert_eq!(decision.action, TurnControlAction::Reject);
        assert_eq!(decision.reason_code, TurnControlReasonCode::MissingPriorityLane.as_str());
        assert_eq!(decision.target_id.as_deref(), Some("queued-1"));
        assert_eq!(decision.journal_projection.terminal_event_type, "turn_control.steer.failed");
    }

    #[test]
    fn dry_run_keeps_action_auditable_without_selecting_mutation_reason() {
        let mut request = base_request(TurnControlOperation::PauseQueue);
        request.session_id = Some("session-1".to_owned());
        request.dry_run = true;

        let decision = decide_turn_control_request(&request);

        assert!(decision.accepted);
        assert_eq!(decision.action, TurnControlAction::SetQueuePaused);
        assert_eq!(decision.reason_code, TurnControlReasonCode::DryRun.as_str());
        assert!(decision.dry_run);
    }

    #[test]
    fn redirect_requires_instruction_and_records_deferred_boundary() {
        let mut missing = base_request(TurnControlOperation::RedirectRun);
        missing.run_id = Some("01ARZ3NDEKTSV4RRFFQ69G5RED".to_owned());

        let rejected = decide_turn_control_request(&missing);

        assert!(!rejected.accepted);
        assert_eq!(rejected.action, TurnControlAction::Reject);
        assert_eq!(
            rejected.reason_code,
            TurnControlReasonCode::MissingRedirectInstruction.as_str()
        );
        assert_eq!(rejected.journal_projection.terminal_event_type, "turn_control.redirect.failed");

        let mut request = base_request(TurnControlOperation::RedirectRun);
        request.run_id = Some("01ARZ3NDEKTSV4RRFFQ69G5RED".to_owned());
        request.instruction = Some("Use the new operator constraint".to_owned());
        request.active_phase = Some(ControlActivePhase::ApprovalPending);

        let decision = decide_turn_control_request(&request);

        assert!(decision.accepted);
        assert_eq!(decision.command, ControlCommand::Redirect);
        assert_eq!(decision.action, TurnControlAction::EnqueueRedirect);
        assert_eq!(decision.reason_code, TurnControlReasonCode::RunRedirectSelected.as_str());
        assert!(decision.safety_boundary.deferred);
        assert_eq!(
            decision.safety_boundary.reason_code,
            "turn_control.boundary.approval_pending.redirect"
        );
    }

    #[test]
    fn yield_is_observable_without_a_mutating_target() {
        let request = base_request(TurnControlOperation::YieldRun);

        let decision = decide_turn_control_request(&request);

        assert!(decision.accepted);
        assert_eq!(decision.command, ControlCommand::Yield);
        assert_eq!(decision.action, TurnControlAction::Yield);
        assert_eq!(decision.reason_code, TurnControlReasonCode::RunYieldSelected.as_str());
        assert_eq!(decision.target_id, None);
        assert_eq!(decision.journal_projection.terminal_event_type, "turn_control.yield.completed");
    }

    fn active_input_request(intent: ActiveRunInputIntent) -> ActiveRunInputPolicyRequest {
        ActiveRunInputPolicyRequest {
            surface: ActiveRunInputSurface::WebConsole,
            intent,
            active_phase: ControlActivePhase::ProviderStream,
            actor_authorized: true,
            queue_paused: false,
            merge_window_open: false,
            tool_drain_active: false,
            approval_pending: false,
            run_id: Some("run-1".to_owned()),
            session_id: Some("session-1".to_owned()),
        }
    }

    #[test]
    fn active_followup_merges_inside_open_merge_window() {
        let mut request = active_input_request(ActiveRunInputIntent::Followup);
        request.merge_window_open = true;

        let decision = decide_active_run_input_policy(&request);

        assert_eq!(decision.action, ActiveRunInputAction::Merge);
        assert!(decision.accepted);
        assert_eq!(decision.reason_code, ActiveRunInputReasonCode::MergeWindowOpen.as_str());
        assert_eq!(decision.target_kind, "run");
        assert!(decision.journal_payload_json.contains("active_run_input.policy_decision"));
    }

    #[test]
    fn active_steer_queues_while_tool_drain_is_active() {
        let mut request = active_input_request(ActiveRunInputIntent::Steer);
        request.active_phase = ControlActivePhase::ToolExecution;
        request.tool_drain_active = true;

        let decision = decide_active_run_input_policy(&request);

        assert_eq!(decision.action, ActiveRunInputAction::Queue);
        assert_eq!(
            decision.reason_code,
            ActiveRunInputReasonCode::SteeringQueuedAtBoundary.as_str()
        );
        assert!(decision.accepted);
    }

    #[test]
    fn active_policy_ignores_unauthorized_or_advisor_input() {
        let mut unauthorized = active_input_request(ActiveRunInputIntent::Cancel);
        unauthorized.actor_authorized = false;
        let unauthorized_decision = decide_active_run_input_policy(&unauthorized);
        assert_eq!(unauthorized_decision.action, ActiveRunInputAction::Ignore);
        assert!(!unauthorized_decision.accepted);

        let mut advisor = active_input_request(ActiveRunInputIntent::Steer);
        advisor.surface = ActiveRunInputSurface::Advisor;
        let advisor_decision = decide_active_run_input_policy(&advisor);
        assert_eq!(advisor_decision.action, ActiveRunInputAction::Ignore);
        assert_eq!(
            advisor_decision.reason_code,
            ActiveRunInputReasonCode::AdvisorNotAuthoritative.as_str()
        );
    }
}
