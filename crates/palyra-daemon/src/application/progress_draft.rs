//! Progress draft projection for long-running agent work.
//!
//! A progress draft is a backend synchronization record: it condenses many
//! run-tape events into one durable, restart-friendly state row that later
//! renderers can use without sending a new visible channel message for every
//! model token or tool event.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::journal::{
    OrchestratorTapeAppendRequest, ProgressDraftRecord, ProgressDraftTapeEventRequest,
};

pub(crate) const PROGRESS_DRAFT_SCHEMA_VERSION: i64 = 1;
pub(crate) const PROGRESS_DRAFT_EVENT_CREATED: &str = "progress_draft.created";
pub(crate) const PROGRESS_DRAFT_EVENT_UPDATED: &str = "progress_draft.updated";
pub(crate) const PROGRESS_DRAFT_EVENT_COMPLETED: &str = "progress_draft.completed";
pub(crate) const PROGRESS_DRAFT_REASON_CREATED_FROM_TAPE: &str = "progress_draft.created_from_tape";
pub(crate) const PROGRESS_DRAFT_REASON_TAPE_UPDATED: &str = "progress_draft.tape_updated";
pub(crate) const PROGRESS_DRAFT_REASON_RUN_COMPLETED: &str = "progress_draft.run_completed";
pub(crate) const PROGRESS_DRAFT_REASON_RUN_FAILED: &str = "progress_draft.run_failed";
pub(crate) const PROGRESS_DRAFT_REASON_RUN_CANCELLED: &str = "progress_draft.run_cancelled";
pub(crate) const PROGRESS_DRAFT_REDACTION_NONE: &str = "none";
pub(crate) const PROGRESS_DRAFT_REDACTION_REDACTED: &str = "redacted";

const RENDER_POLICY_INTERNAL_ONLY: &str = "internal_only";

/// Durable progress state persisted in `progress_drafts.state`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProgressDraftState {
    Pending,
    Running,
    WaitingApproval,
    RetryScheduled,
    Compacting,
    Completed,
    Failed,
    Cancelled,
}

impl ProgressDraftState {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::WaitingApproval => "waiting_approval",
            Self::RetryScheduled => "retry_scheduled",
            Self::Compacting => "compacting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "waiting_approval" => Some(Self::WaitingApproval),
            "retry_scheduled" => Some(Self::RetryScheduled),
            "compacting" => Some(Self::Compacting),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Policy hint for future channel renderers.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProgressDraftRenderPolicy {
    InternalOnly,
    ChannelStatus,
    Suppressed,
}

impl ProgressDraftRenderPolicy {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InternalOnly => RENDER_POLICY_INTERNAL_ONLY,
            Self::ChannelStatus => "channel_status",
            Self::Suppressed => "suppressed",
        }
    }

    #[must_use]
    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            RENDER_POLICY_INTERNAL_ONLY => Some(Self::InternalOnly),
            "channel_status" => Some(Self::ChannelStatus),
            "suppressed" => Some(Self::Suppressed),
            _ => None,
        }
    }
}

/// Backend-facing draft shape used by pure projection tests and diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProgressDraft {
    pub schema_version: i64,
    pub run_id: String,
    pub state: ProgressDraftState,
    pub summary: String,
    pub last_visible_step: String,
    pub hidden_internal_state_hash: String,
    pub render_policy: ProgressDraftRenderPolicy,
}

impl ProgressDraft {
    #[must_use]
    pub(crate) fn from_record(record: &ProgressDraftRecord) -> Option<Self> {
        Some(Self {
            schema_version: record.version,
            run_id: record.run_id.clone(),
            state: ProgressDraftState::from_str(record.state.as_str())?,
            summary: record.summary.clone(),
            last_visible_step: record.last_visible_step.clone(),
            hidden_internal_state_hash: record.hidden_internal_state_hash.clone(),
            render_policy: ProgressDraftRenderPolicy::from_str(record.render_policy.as_str())?,
        })
    }
}

/// Minimal renderer contract for operator and channel surfaces.
pub(crate) struct ProgressDraftRenderer;

impl ProgressDraftRenderer {
    #[must_use]
    pub(crate) fn operator_summary(record: &ProgressDraftRecord) -> String {
        ProgressDraft::from_record(record).map_or_else(
            || format!("{}: {}", record.state, record.summary),
            |draft| format!("{}: {}", draft.state.as_str(), draft.summary),
        )
    }
}

/// Builds a journal upsert request from a tape event. Non-progress events and
/// high-volume non-final model tokens return `None`.
pub(crate) fn project_progress_draft_tape_event(
    request: &OrchestratorTapeAppendRequest,
) -> Option<ProgressDraftTapeEventRequest> {
    let payload = serde_json::from_str::<Value>(request.payload_json.as_str())
        .unwrap_or_else(|_| json!({ "raw_payload": request.payload_json }));
    let projection = projection_for_event(request.event_type.as_str(), &payload)?;
    let hidden_internal_state_hash = hidden_state_hash(
        request.run_id.as_str(),
        request.seq,
        request.event_type.as_str(),
        request.payload_json.as_str(),
    );
    let evidence_refs_json = json!([{
        "kind": "orchestrator_tape",
        "run_id": request.run_id,
        "seq": request.seq,
        "event_type": request.event_type,
    }])
    .to_string();
    let payload_json = json!({
        "schema_version": PROGRESS_DRAFT_SCHEMA_VERSION,
        "source_event_type": request.event_type,
        "source_tape_seq": request.seq,
        "summary": projection.summary,
        "last_visible_step": projection.last_visible_step,
        "state": projection.state.as_str(),
    })
    .to_string();

    Some(ProgressDraftTapeEventRequest {
        run_id: request.run_id.clone(),
        source_tape_seq: request.seq,
        source_event_type: request.event_type.clone(),
        state: projection.state.as_str().to_owned(),
        summary: projection.summary,
        last_visible_step: projection.last_visible_step,
        hidden_internal_state_hash,
        version: PROGRESS_DRAFT_SCHEMA_VERSION,
        render_policy: projection.render_policy.as_str().to_owned(),
        channel_instance_id: optional_string_field(&payload, "channel_instance_id"),
        external_message_id: optional_string_field(&payload, "external_message_id"),
        reason_code: projection.reason_code.to_owned(),
        evidence_refs_json,
        payload_json,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TapeProjection {
    state: ProgressDraftState,
    summary: String,
    last_visible_step: String,
    reason_code: &'static str,
    render_policy: ProgressDraftRenderPolicy,
}

fn projection_for_event(event_type: &str, payload: &Value) -> Option<TapeProjection> {
    match event_type {
        "message.received" => Some(projection(
            ProgressDraftState::Pending,
            "Run input received",
            "queued",
            PROGRESS_DRAFT_REASON_CREATED_FROM_TAPE,
        )),
        "provider.call" => Some(projection(
            ProgressDraftState::Running,
            "Waiting for model provider response",
            "model_started",
            PROGRESS_DRAFT_REASON_TAPE_UPDATED,
        )),
        "status" => status_projection(payload),
        "tool_proposal" | "tool.call" => Some(tool_projection(
            payload,
            if bool_field(payload, "approval_required") {
                ProgressDraftState::WaitingApproval
            } else {
                ProgressDraftState::Running
            },
            "Tool proposed",
            "tool_proposed",
            PROGRESS_DRAFT_REASON_TAPE_UPDATED,
        )),
        "tool_approval_request" => Some(tool_approval_request_projection(payload)),
        "tool_approval_response" => Some(projection(
            ProgressDraftState::Running,
            if bool_field(payload, "approved") {
                "Tool approval granted"
            } else {
                "Tool approval denied"
            },
            "approval_resolved",
            PROGRESS_DRAFT_REASON_TAPE_UPDATED,
        )),
        "tool_result" => Some(tool_result_projection(payload)),
        "tool.executed" => Some(tool_executed_projection(payload)),
        "tool_attestation" => Some(projection(
            ProgressDraftState::Running,
            "Tool execution attestation recorded",
            "tool_attested",
            PROGRESS_DRAFT_REASON_TAPE_UPDATED,
        )),
        "session.compaction" => Some(session_compaction_projection(payload)),
        "provider_turn_output" | "message.replied" => Some(projection(
            ProgressDraftState::Completed,
            "Run reply recorded",
            "run_completed",
            PROGRESS_DRAFT_REASON_RUN_COMPLETED,
        )),
        "model_token" if bool_field(payload, "is_final") => Some(projection(
            ProgressDraftState::Running,
            "Model output updated",
            "model_output",
            PROGRESS_DRAFT_REASON_TAPE_UPDATED,
        )),
        value if value.contains("retry") => Some(projection(
            ProgressDraftState::RetryScheduled,
            "Retry scheduled",
            "retry_scheduled",
            PROGRESS_DRAFT_REASON_TAPE_UPDATED,
        )),
        _ => None,
    }
}

fn status_projection(payload: &Value) -> Option<TapeProjection> {
    let kind = string_field(payload, "kind").unwrap_or("in_progress");
    let message = string_field(payload, "message").unwrap_or(kind);
    let lower_message = message.to_ascii_lowercase();
    let state = match kind {
        "done" => ProgressDraftState::Completed,
        "cancelled" => ProgressDraftState::Cancelled,
        "failed" | "needs_continuation" => ProgressDraftState::Failed,
        _ if lower_message.contains("session.compaction") => ProgressDraftState::Compacting,
        _ if lower_message.contains("retry") => ProgressDraftState::RetryScheduled,
        _ => ProgressDraftState::Running,
    };
    let reason_code = match state {
        ProgressDraftState::Completed => PROGRESS_DRAFT_REASON_RUN_COMPLETED,
        ProgressDraftState::Cancelled => PROGRESS_DRAFT_REASON_RUN_CANCELLED,
        ProgressDraftState::Failed => PROGRESS_DRAFT_REASON_RUN_FAILED,
        _ => PROGRESS_DRAFT_REASON_TAPE_UPDATED,
    };
    Some(projection(state, message, status_step(kind, message), reason_code))
}

fn tool_projection(
    payload: &Value,
    state: ProgressDraftState,
    prefix: &str,
    last_visible_step: &str,
    reason_code: &'static str,
) -> TapeProjection {
    let tool_name = string_field(payload, "tool_name").unwrap_or("unknown tool");
    projection(state, format!("{prefix}: {tool_name}"), last_visible_step, reason_code)
}

fn tool_approval_request_projection(payload: &Value) -> TapeProjection {
    let summary =
        string_field(payload, "request_summary").map(ToOwned::to_owned).unwrap_or_else(|| {
            let tool_name = string_field(payload, "tool_name").unwrap_or("unknown tool");
            format!("Waiting for tool approval: {tool_name}")
        });
    projection(
        ProgressDraftState::WaitingApproval,
        summary,
        "approval_required",
        PROGRESS_DRAFT_REASON_TAPE_UPDATED,
    )
}

fn tool_result_projection(payload: &Value) -> TapeProjection {
    let success = bool_field(payload, "success");
    let proposal_id = string_field(payload, "proposal_id").unwrap_or("unknown proposal");
    projection(
        ProgressDraftState::Running,
        format!("Tool {}: {proposal_id}", if success { "completed" } else { "failed" }),
        "tool_completed",
        PROGRESS_DRAFT_REASON_TAPE_UPDATED,
    )
}

fn tool_executed_projection(payload: &Value) -> TapeProjection {
    let success = bool_field(payload, "success");
    let tool_name = string_field(payload, "tool_name").unwrap_or("unknown tool");
    projection(
        ProgressDraftState::Running,
        format!("Tool {}: {tool_name}", if success { "completed" } else { "failed" }),
        "tool_completed",
        PROGRESS_DRAFT_REASON_TAPE_UPDATED,
    )
}

fn session_compaction_projection(payload: &Value) -> TapeProjection {
    let event = string_field(payload, "event").unwrap_or("session.compaction");
    projection(
        ProgressDraftState::Compacting,
        format!("Session compaction: {event}"),
        "compaction",
        PROGRESS_DRAFT_REASON_TAPE_UPDATED,
    )
}

fn projection(
    state: ProgressDraftState,
    summary: impl Into<String>,
    last_visible_step: impl Into<String>,
    reason_code: &'static str,
) -> TapeProjection {
    TapeProjection {
        state,
        summary: summary.into(),
        last_visible_step: last_visible_step.into(),
        reason_code,
        render_policy: ProgressDraftRenderPolicy::InternalOnly,
    }
}

fn status_step(kind: &str, message: &str) -> &'static str {
    if kind == "done" {
        "run_completed"
    } else if kind == "cancelled" {
        "run_cancelled"
    } else if kind == "failed" || kind == "needs_continuation" {
        "run_failed"
    } else if message.contains("session.compaction") {
        "compaction"
    } else {
        "running"
    }
}

fn string_field<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload.get(key).and_then(Value::as_str).filter(|value| !value.trim().is_empty())
}

fn optional_string_field(payload: &Value, key: &str) -> Option<String> {
    string_field(payload, key).map(ToOwned::to_owned)
}

fn bool_field(payload: &Value, key: &str) -> bool {
    payload.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn hidden_state_hash(run_id: &str, seq: i64, event_type: &str, payload_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(run_id.as_bytes());
    hasher.update(seq.to_be_bytes());
    hasher.update(event_type.as_bytes());
    hasher.update(payload_json.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tape_event(event_type: &str, payload: Value) -> OrchestratorTapeAppendRequest {
        OrchestratorTapeAppendRequest {
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5PD1".to_owned(),
            seq: 7,
            event_type: event_type.to_owned(),
            payload_json: payload.to_string(),
        }
    }

    #[test]
    fn progress_draft_state_serializes_as_snake_case() {
        let encoded =
            serde_json::to_string(&ProgressDraftState::WaitingApproval).expect("state serializes");

        assert_eq!(encoded, r#""waiting_approval""#);
    }

    #[test]
    fn tool_approval_request_projects_waiting_state() {
        let request = tape_event(
            "tool_approval_request",
            json!({
                "tool_name": "palyra.process.run",
                "request_summary": "approval requested",
            }),
        );

        let projection =
            project_progress_draft_tape_event(&request).expect("approval request should project");

        assert_eq!(projection.state, ProgressDraftState::WaitingApproval.as_str());
        assert_eq!(projection.last_visible_step, "approval_required");
        assert_eq!(projection.summary, "approval requested");
        assert_eq!(projection.reason_code, PROGRESS_DRAFT_REASON_TAPE_UPDATED);
    }

    #[test]
    fn non_final_model_tokens_do_not_update_draft() {
        let request = tape_event("model_token", json!({"is_final": false, "token": "partial"}));

        assert!(project_progress_draft_tape_event(&request).is_none());
    }

    #[test]
    fn terminal_status_projects_completed_state() {
        let request = tape_event("status", json!({"kind": "done", "message": "Done."}));

        let projection =
            project_progress_draft_tape_event(&request).expect("done status should project");

        assert_eq!(projection.state, ProgressDraftState::Completed.as_str());
        assert_eq!(projection.reason_code, PROGRESS_DRAFT_REASON_RUN_COMPLETED);
        assert_eq!(projection.last_visible_step, "run_completed");
    }

    #[test]
    fn hidden_state_hash_is_stable_for_same_tape_event() {
        let request = tape_event("tool_result", json!({"proposal_id": "p1", "success": true}));

        let left = project_progress_draft_tape_event(&request).expect("result should project");
        let right = project_progress_draft_tape_event(&request).expect("result should project");

        assert_eq!(left.hidden_internal_state_hash, right.hidden_internal_state_hash);
    }
}
