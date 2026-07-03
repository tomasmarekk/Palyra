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
}
