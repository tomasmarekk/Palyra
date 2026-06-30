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

/// Basic control-plane operation selected by an operator surface.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnControlOperation {
    Status,
    CancelRun,
    PauseQueue,
    ResumeQueue,
    PrioritizeQueuedInput,
}

impl TurnControlOperation {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::CancelRun => "cancel_run",
            Self::PauseQueue => "pause_queue",
            Self::ResumeQueue => "resume_queue",
            Self::PrioritizeQueuedInput => "prioritize_queued_input",
        }
    }

    #[must_use]
    pub(crate) const fn target_kind(self) -> &'static str {
        match self {
            Self::Status | Self::CancelRun => "run",
            Self::PauseQueue | Self::ResumeQueue => "session_queue",
            Self::PrioritizeQueuedInput => "queued_input",
        }
    }
}

/// Runtime action chosen by the decision layer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnControlAction {
    Observe,
    RequestRunCancel,
    SetQueuePaused,
    SetQueuedInputPriority,
    Reject,
}

impl TurnControlAction {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::RequestRunCancel => "request_run_cancel",
            Self::SetQueuePaused => "set_queue_paused",
            Self::SetQueuedInputPriority => "set_queued_input_priority",
            Self::Reject => "reject",
        }
    }
}

/// Stable reason codes for turn-control decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnControlReasonCode {
    StatusObserved,
    RunCancelSelected,
    QueuePauseSelected,
    QueueResumeSelected,
    QueuedInputPrioritySelected,
    DryRun,
    MissingActor,
    MissingRunId,
    MissingSessionId,
    MissingQueuedInputId,
    MissingPriorityLane,
}

impl TurnControlReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StatusObserved => "turn_control.status_observed",
            Self::RunCancelSelected => "turn_control.run_cancel_selected",
            Self::QueuePauseSelected => "turn_control.queue_pause_selected",
            Self::QueueResumeSelected => "turn_control.queue_resume_selected",
            Self::QueuedInputPrioritySelected => "turn_control.queued_input_priority_selected",
            Self::DryRun => "turn_control.dry_run",
            Self::MissingActor => "turn_control.missing_actor",
            Self::MissingRunId => "turn_control.missing_run_id",
            Self::MissingSessionId => "turn_control.missing_session_id",
            Self::MissingQueuedInputId => "turn_control.missing_queued_input_id",
            Self::MissingPriorityLane => "turn_control.missing_priority_lane",
        }
    }
}

/// Operator request accepted by the turn control plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TurnControlRequest {
    pub operation: TurnControlOperation,
    pub actor_principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued_input_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority_lane: Option<String>,
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
    pub action: TurnControlAction,
    pub accepted: bool,
    pub reason_code: String,
    pub actor_principal: String,
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
    let terminal_event_type =
        if accepted { TURN_CONTROL_EVENT_COMPLETED } else { TURN_CONTROL_EVENT_FAILED };
    let reason_code = reason_code.as_str().to_owned();
    let target_kind = request.operation.target_kind().to_owned();
    let operator_reason = request
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let payload_json = json!({
        "schema_version": TURN_CONTROL_SCHEMA_VERSION,
        "operation": request.operation.as_str(),
        "action": action.as_str(),
        "accepted": accepted,
        "reason_code": reason_code,
        "actor_principal": request.actor_principal.trim(),
        "target_kind": target_kind,
        "target_id": target_id.as_deref(),
        "session_id": request.session_id.as_deref(),
        "run_id": request.run_id.as_deref(),
        "queued_input_id": request.queued_input_id.as_deref(),
        "priority_lane": request.priority_lane.as_deref(),
        "operator_reason": operator_reason.as_deref(),
        "dry_run": request.dry_run,
    })
    .to_string();
    let evidence_refs_json = json!([{
        "kind": "turn_control_request",
        "operation": request.operation.as_str(),
        "target_kind": target_kind,
        "target_id": target_id.as_deref(),
    }])
    .to_string();
    TurnControlDecision {
        schema_version: TURN_CONTROL_SCHEMA_VERSION,
        operation: request.operation,
        action,
        accepted,
        reason_code: reason_code.clone(),
        actor_principal: request.actor_principal.trim().to_owned(),
        target_kind: target_kind.clone(),
        target_id: target_id.clone(),
        operator_reason,
        dry_run: request.dry_run,
        journal_projection: TurnControlJournalProjection {
            started_event_type: TURN_CONTROL_EVENT_STARTED.to_owned(),
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
            session_id: None,
            run_id: None,
            queued_input_id: None,
            priority_lane: None,
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
        assert_eq!(decision.action, TurnControlAction::RequestRunCancel);
        assert_eq!(decision.reason_code, TurnControlReasonCode::RunCancelSelected.as_str());
        assert_eq!(decision.target_kind, "run");
        assert_eq!(decision.target_id.as_deref(), Some("01ARZ3NDEKTSV4RRFFQ69G5RUN"));
        assert_eq!(decision.journal_projection.started_event_type, TURN_CONTROL_EVENT_STARTED);
        assert_eq!(decision.journal_projection.terminal_event_type, TURN_CONTROL_EVENT_COMPLETED);

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
        assert_eq!(decision.journal_projection.terminal_event_type, TURN_CONTROL_EVENT_FAILED);
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
}
