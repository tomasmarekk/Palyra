//! Safe-resume classification for interrupted or orphaned runs.
//!
//! The classifier is intentionally side-effect free: it folds run metadata and
//! redacted tape signals into an auditable decision. Startup recovery records
//! the decision before preserving the current fail-closed terminalization
//! behavior; later replay work can consume the same contract.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    application::tool_registry::tool_wait_is_safe_to_resume, orchestrator::RunLifecycleState,
};

pub(crate) const RESUME_CLASSIFIER_SCHEMA_VERSION: i64 = 1;
pub(crate) const RUN_RESUME_DECISION_RECORDED_EVENT: &str = "run.resume.decision_recorded";
pub(crate) const RESUME_REDACTION_NONE: &str = "none";
pub(crate) const DEFAULT_RESUME_FRESHNESS_TTL_MS: u64 = 10 * 60 * 1000;

/// Resume classifier outcome for a run that might be continued.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResumeDecisionKind {
    SafeToResume,
    NeedsUserConfirmation,
    StaleDoNotResume,
    TerminalDoNotResume,
    PolicyBlocked,
}

impl ResumeDecisionKind {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::SafeToResume => "safe_to_resume",
            Self::NeedsUserConfirmation => "needs_user_confirmation",
            Self::StaleDoNotResume => "stale_do_not_resume",
            Self::TerminalDoNotResume => "terminal_do_not_resume",
            Self::PolicyBlocked => "policy_blocked",
        }
    }
}

/// Replay posture paired with a resume decision.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplayContinuityPolicy {
    ContinueFromTape,
    RequireUserConfirmation,
    DoNotReplay,
    BlockedByPolicy,
}

impl ReplayContinuityPolicy {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ContinueFromTape => "continue_from_tape",
            Self::RequireUserConfirmation => "require_user_confirmation",
            Self::DoNotReplay => "do_not_replay",
            Self::BlockedByPolicy => "blocked_by_policy",
        }
    }
}

/// Stable reason codes emitted by the resume classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResumeReasonCode {
    FreshReadOnlyToolWait,
    NeedsOperatorConfirmation,
    ResumePrincipalUnavailable,
    PrincipalMismatch,
    ChannelMismatch,
    ChannelUnavailable,
    PendingApproval,
    MutatingToolInFlight,
    RoutineLeaseActive,
    WorkspaceMutationCheckpointMissing,
    MissingFreshnessEvidence,
    LastModelTurnMissing,
    FreshnessWindowExpired,
    TerminalRunState,
}

impl ResumeReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::FreshReadOnlyToolWait => "resume.fresh_read_only_tool_wait",
            Self::NeedsOperatorConfirmation => "resume.needs_operator_confirmation",
            Self::ResumePrincipalUnavailable => "resume.principal_unavailable",
            Self::PrincipalMismatch => "resume.principal_mismatch",
            Self::ChannelMismatch => "resume.channel_mismatch",
            Self::ChannelUnavailable => "resume.channel_unavailable",
            Self::PendingApproval => "resume.pending_approval",
            Self::MutatingToolInFlight => "resume.mutating_tool_in_flight",
            Self::RoutineLeaseActive => "resume.routine_lease_active",
            Self::WorkspaceMutationCheckpointMissing => {
                "resume.workspace_mutation_checkpoint_missing"
            }
            Self::MissingFreshnessEvidence => "resume.missing_freshness_evidence",
            Self::LastModelTurnMissing => "resume.last_model_turn_missing",
            Self::FreshnessWindowExpired => "resume.freshness_window_expired",
            Self::TerminalRunState => "resume.terminal_run_state",
        }
    }
}

/// Input snapshot used to classify a potential resume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeClassifierInput {
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    pub(crate) run_state: String,
    pub(crate) run_principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reconnect_principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reconnect_channel: Option<String>,
    pub(crate) channel_exists: bool,
    pub(crate) observed_at_unix_ms: i64,
    pub(crate) max_freshness_age_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_transcript_event_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_model_turn_unix_ms: Option<i64>,
    pub(crate) mutating_tool_in_flight: bool,
    pub(crate) read_only_tool_wait: bool,
    pub(crate) pending_approval: bool,
    pub(crate) routine_lease_active: bool,
    pub(crate) workspace_mutation_checkpoint_clean: bool,
}

/// Journal-ready classifier decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeDecision {
    pub(crate) schema_version: i64,
    pub(crate) decision: ResumeDecisionKind,
    pub(crate) replay_continuity_policy: ReplayContinuityPolicy,
    pub(crate) reason_code: String,
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) freshness_age_ms: Option<u64>,
    pub(crate) payload_json: String,
    pub(crate) evidence_refs_json: String,
    pub(crate) redaction_level: String,
}

/// Tape event view used by the classifier's observation summarizer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeTapeObservation {
    pub(crate) event_type: String,
    pub(crate) payload_json: String,
    pub(crate) created_at_unix_ms: i64,
}

/// Safety signals derived from a run tape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeTapeSafetySignals {
    pub(crate) last_transcript_event_unix_ms: Option<i64>,
    pub(crate) last_model_turn_unix_ms: Option<i64>,
    pub(crate) mutating_tool_in_flight: bool,
    pub(crate) read_only_tool_wait: bool,
    pub(crate) pending_approval: bool,
    pub(crate) workspace_mutation_checkpoint_clean: bool,
}

impl Default for ResumeTapeSafetySignals {
    fn default() -> Self {
        Self {
            last_transcript_event_unix_ms: None,
            last_model_turn_unix_ms: None,
            mutating_tool_in_flight: false,
            read_only_tool_wait: false,
            pending_approval: false,
            workspace_mutation_checkpoint_clean: true,
        }
    }
}

/// Classifies whether a run can resume, requires an operator, or must stay blocked.
#[must_use]
pub(crate) fn classify_resume(input: &ResumeClassifierInput) -> ResumeDecision {
    let freshness_age_ms = latest_freshness_source(input)
        .map(|created_at_unix_ms| age_ms(input.observed_at_unix_ms, created_at_unix_ms));

    if RunLifecycleState::from_str(input.run_state.as_str())
        .is_some_and(RunLifecycleState::is_terminal)
    {
        return resume_decision(
            input,
            freshness_age_ms,
            ResumeDecisionKind::TerminalDoNotResume,
            ReplayContinuityPolicy::DoNotReplay,
            ResumeReasonCode::TerminalRunState,
        );
    }
    if input
        .reconnect_principal
        .as_deref()
        .is_some_and(|principal| principal != input.run_principal)
    {
        return resume_decision(
            input,
            freshness_age_ms,
            ResumeDecisionKind::PolicyBlocked,
            ReplayContinuityPolicy::BlockedByPolicy,
            ResumeReasonCode::PrincipalMismatch,
        );
    }
    if !input.channel_exists {
        return resume_decision(
            input,
            freshness_age_ms,
            ResumeDecisionKind::PolicyBlocked,
            ReplayContinuityPolicy::BlockedByPolicy,
            ResumeReasonCode::ChannelUnavailable,
        );
    }
    if input.reconnect_channel.as_deref().is_some()
        && input.run_channel.as_deref() != input.reconnect_channel.as_deref()
    {
        return resume_decision(
            input,
            freshness_age_ms,
            ResumeDecisionKind::PolicyBlocked,
            ReplayContinuityPolicy::BlockedByPolicy,
            ResumeReasonCode::ChannelMismatch,
        );
    }
    if input.pending_approval {
        return resume_decision(
            input,
            freshness_age_ms,
            ResumeDecisionKind::NeedsUserConfirmation,
            ReplayContinuityPolicy::RequireUserConfirmation,
            ResumeReasonCode::PendingApproval,
        );
    }
    if input.mutating_tool_in_flight {
        return resume_decision(
            input,
            freshness_age_ms,
            ResumeDecisionKind::NeedsUserConfirmation,
            ReplayContinuityPolicy::RequireUserConfirmation,
            ResumeReasonCode::MutatingToolInFlight,
        );
    }
    if input.routine_lease_active {
        return resume_decision(
            input,
            freshness_age_ms,
            ResumeDecisionKind::NeedsUserConfirmation,
            ReplayContinuityPolicy::RequireUserConfirmation,
            ResumeReasonCode::RoutineLeaseActive,
        );
    }
    if !input.workspace_mutation_checkpoint_clean {
        return resume_decision(
            input,
            freshness_age_ms,
            ResumeDecisionKind::NeedsUserConfirmation,
            ReplayContinuityPolicy::RequireUserConfirmation,
            ResumeReasonCode::WorkspaceMutationCheckpointMissing,
        );
    }
    let Some(freshness_age_ms) = freshness_age_ms else {
        return resume_decision(
            input,
            None,
            ResumeDecisionKind::StaleDoNotResume,
            ReplayContinuityPolicy::DoNotReplay,
            ResumeReasonCode::MissingFreshnessEvidence,
        );
    };
    if freshness_age_ms > input.max_freshness_age_ms {
        return resume_decision(
            input,
            Some(freshness_age_ms),
            ResumeDecisionKind::StaleDoNotResume,
            ReplayContinuityPolicy::DoNotReplay,
            ResumeReasonCode::FreshnessWindowExpired,
        );
    }
    if input.last_model_turn_unix_ms.is_none() {
        return resume_decision(
            input,
            Some(freshness_age_ms),
            ResumeDecisionKind::NeedsUserConfirmation,
            ReplayContinuityPolicy::RequireUserConfirmation,
            ResumeReasonCode::LastModelTurnMissing,
        );
    }
    if input.reconnect_principal.as_deref().is_none() {
        return resume_decision(
            input,
            Some(freshness_age_ms),
            ResumeDecisionKind::NeedsUserConfirmation,
            ReplayContinuityPolicy::RequireUserConfirmation,
            ResumeReasonCode::ResumePrincipalUnavailable,
        );
    }
    if input.read_only_tool_wait {
        return resume_decision(
            input,
            Some(freshness_age_ms),
            ResumeDecisionKind::SafeToResume,
            ReplayContinuityPolicy::ContinueFromTape,
            ResumeReasonCode::FreshReadOnlyToolWait,
        );
    }
    resume_decision(
        input,
        Some(freshness_age_ms),
        ResumeDecisionKind::NeedsUserConfirmation,
        ReplayContinuityPolicy::RequireUserConfirmation,
        ResumeReasonCode::NeedsOperatorConfirmation,
    )
}

/// Derives safety signals from a run tape without applying any resume policy.
#[must_use]
pub(crate) fn summarize_resume_tape_observations(
    events: &[ResumeTapeObservation],
) -> ResumeTapeSafetySignals {
    let mut signals = ResumeTapeSafetySignals::default();
    let mut proposals = BTreeMap::<String, (String, Vec<u8>)>::new();
    let mut unresolved_tool_waits = BTreeMap::<String, bool>::new();

    for (event_index, event) in events.iter().enumerate() {
        let payload = serde_json::from_str::<Value>(event.payload_json.as_str()).ok();
        if is_transcript_event(event.event_type.as_str()) {
            signals.last_transcript_event_unix_ms = Some(event.created_at_unix_ms);
        }
        if is_model_turn_event(event.event_type.as_str()) {
            signals.last_model_turn_unix_ms = Some(event.created_at_unix_ms);
        }
        match event.event_type.as_str() {
            "tool_proposal" => {
                if let Some(payload) = payload.as_ref() {
                    if let (Some(proposal_id), Some(tool_name)) =
                        (proposal_id(payload), tool_name(payload))
                    {
                        let input_json = tool_input_json(payload)
                            .map(Value::to_string)
                            .unwrap_or_default()
                            .into_bytes();
                        proposals
                            .insert(proposal_id.to_owned(), (tool_name.to_owned(), input_json));
                    }
                }
            }
            "tool_decision" => {
                let Some(payload) = payload.as_ref() else {
                    continue;
                };
                if tool_decision_kind(payload) != Some("allow") {
                    continue;
                }
                if tool_approval_required(payload) {
                    signals.pending_approval = true;
                }
                let proposal_id = proposal_id(payload).map(str::to_owned);
                let proposal = proposal_id.as_deref().and_then(|id| proposals.get(id));
                let tool_name = proposal
                    .map(|(tool_name, _)| tool_name.as_str())
                    .or_else(|| tool_name(payload));
                let decision_input = tool_input_json(payload).map(Value::to_string);
                let input_json = proposal
                    .map(|(_, input_json)| input_json.as_slice())
                    .or_else(|| decision_input.as_deref().map(str::as_bytes))
                    .unwrap_or_default();
                let safe_to_resume = tool_name
                    .is_some_and(|tool_name| tool_wait_is_safe_to_resume(tool_name, input_json));
                let correlation_key =
                    proposal_id.unwrap_or_else(|| format!("missing-proposal-id-{event_index}"));
                unresolved_tool_waits
                    .entry(correlation_key)
                    .and_modify(|existing| *existing &= safe_to_resume)
                    .or_insert(safe_to_resume);
            }
            "tool_approval_request" => {
                signals.pending_approval = true;
            }
            "tool_approval_response" => {
                signals.pending_approval = false;
            }
            "tool_result" => {
                if let Some(result_proposal) = payload.as_ref().and_then(proposal_id) {
                    unresolved_tool_waits.remove(result_proposal);
                }
            }
            event_type if event_type.contains("workspace_checkpoint") => {
                if checkpoint_stage(payload.as_ref()) == Some("preflight") {
                    signals.workspace_mutation_checkpoint_clean = false;
                } else if checkpoint_stage(payload.as_ref()) == Some("post_change") {
                    signals.workspace_mutation_checkpoint_clean = true;
                }
            }
            _ => {}
        }
    }

    signals.mutating_tool_in_flight =
        unresolved_tool_waits.values().any(|safe_to_resume| !safe_to_resume);
    signals.read_only_tool_wait =
        !unresolved_tool_waits.is_empty() && !signals.mutating_tool_in_flight;
    signals
}

fn resume_decision(
    input: &ResumeClassifierInput,
    freshness_age_ms: Option<u64>,
    decision: ResumeDecisionKind,
    replay_continuity_policy: ReplayContinuityPolicy,
    reason_code: ResumeReasonCode,
) -> ResumeDecision {
    let reason_code = reason_code.as_str().to_owned();
    let payload_json = json!({
        "event": RUN_RESUME_DECISION_RECORDED_EVENT,
        "schema_version": RESUME_CLASSIFIER_SCHEMA_VERSION,
        "decision": decision.as_str(),
        "replay_continuity_policy": replay_continuity_policy.as_str(),
        "reason_code": reason_code,
        "run_id": input.run_id.as_str(),
        "session_id": input.session_id.as_str(),
        "freshness_age_ms": freshness_age_ms,
        "max_freshness_age_ms": input.max_freshness_age_ms,
        "criteria": {
            "run_state": input.run_state.as_str(),
            "principal_match": input
                .reconnect_principal
                .as_deref()
                .map(|principal| principal == input.run_principal),
            "channel_match": input
                .reconnect_channel
                .as_deref()
                .map(|channel| input.run_channel.as_deref() == Some(channel)),
            "channel_exists": input.channel_exists,
            "last_transcript_event_unix_ms": input.last_transcript_event_unix_ms,
            "last_model_turn_unix_ms": input.last_model_turn_unix_ms,
            "mutating_tool_in_flight": input.mutating_tool_in_flight,
            "read_only_tool_wait": input.read_only_tool_wait,
            "pending_approval": input.pending_approval,
            "routine_lease_active": input.routine_lease_active,
            "workspace_mutation_checkpoint_clean": input.workspace_mutation_checkpoint_clean,
        },
    })
    .to_string();
    let evidence_refs_json = json!([{
        "kind": "run",
        "run_id": input.run_id.as_str(),
        "session_id": input.session_id.as_str(),
        "last_transcript_event_unix_ms": input.last_transcript_event_unix_ms,
        "last_model_turn_unix_ms": input.last_model_turn_unix_ms,
    }])
    .to_string();
    ResumeDecision {
        schema_version: RESUME_CLASSIFIER_SCHEMA_VERSION,
        decision,
        replay_continuity_policy,
        reason_code,
        run_id: input.run_id.clone(),
        session_id: input.session_id.clone(),
        freshness_age_ms,
        payload_json,
        evidence_refs_json,
        redaction_level: RESUME_REDACTION_NONE.to_owned(),
    }
}

fn latest_freshness_source(input: &ResumeClassifierInput) -> Option<i64> {
    input.last_transcript_event_unix_ms.max(input.last_model_turn_unix_ms)
}

fn age_ms(observed_at_unix_ms: i64, created_at_unix_ms: i64) -> u64 {
    u64::try_from(observed_at_unix_ms.saturating_sub(created_at_unix_ms).max(0)).unwrap_or_default()
}

fn is_transcript_event(event_type: &str) -> bool {
    matches!(event_type, "message.received" | "message.replied" | "queued.input")
}

fn is_model_turn_event(event_type: &str) -> bool {
    matches!(event_type, "provider_turn_output" | "model_token" | "message.replied")
}

fn proposal_id(payload: &Value) -> Option<&str> {
    payload
        .pointer("/tool_decision/proposal_id")
        .or_else(|| payload.pointer("/tool_proposal/proposal_id"))
        .or_else(|| payload.pointer("/tool_result/proposal_id"))
        .or_else(|| payload.pointer("/proposal_id"))
        .and_then(Value::as_str)
}

fn tool_name(payload: &Value) -> Option<&str> {
    payload
        .pointer("/tool_proposal/tool_name")
        .or_else(|| payload.pointer("/tool_approval_request/tool_name"))
        .or_else(|| payload.pointer("/tool_decision/tool_name"))
        .or_else(|| payload.pointer("/tool_name"))
        .and_then(Value::as_str)
}

fn tool_input_json(payload: &Value) -> Option<&Value> {
    payload
        .pointer("/tool_proposal/input_json")
        .or_else(|| payload.pointer("/tool_decision/input_json"))
        .or_else(|| payload.pointer("/input_json"))
}

fn tool_decision_kind(payload: &Value) -> Option<&str> {
    payload
        .pointer("/tool_decision/kind")
        .or_else(|| payload.pointer("/decision"))
        .or_else(|| payload.pointer("/kind"))
        .and_then(Value::as_str)
}

fn tool_approval_required(payload: &Value) -> bool {
    payload
        .pointer("/tool_decision/approval_required")
        .or_else(|| payload.pointer("/approval_required"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn checkpoint_stage(payload: Option<&Value>) -> Option<&str> {
    payload?
        .pointer("/checkpoint_stage")
        .or_else(|| payload?.pointer("/workspace_checkpoint/checkpoint_stage"))
        .and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use super::{
        classify_resume, summarize_resume_tape_observations, ReplayContinuityPolicy,
        ResumeClassifierInput, ResumeDecision, ResumeDecisionKind, ResumeReasonCode,
        ResumeTapeObservation, DEFAULT_RESUME_FRESHNESS_TTL_MS,
    };
    use serde_json::json;

    #[test]
    fn fresh_read_only_wait_with_matching_principal_and_channel_is_safe_to_resume() {
        let input = base_input();
        let decision = classify_resume(&input);

        assert_eq!(decision.decision, ResumeDecisionKind::SafeToResume);
        assert_eq!(decision.replay_continuity_policy, ReplayContinuityPolicy::ContinueFromTape);
        assert_eq!(decision.reason_code, ResumeReasonCode::FreshReadOnlyToolWait.as_str());
        assert_eq!(decision.freshness_age_ms, Some(1_000));

        let roundtrip: ResumeDecision =
            serde_json::from_str(&serde_json::to_string(&decision).expect("serializes"))
                .expect("deserializes");
        assert_eq!(roundtrip, decision);
    }

    #[test]
    fn mutating_tool_wait_requires_user_confirmation() {
        let mut input = base_input();
        input.read_only_tool_wait = false;
        input.mutating_tool_in_flight = true;

        let decision = classify_resume(&input);

        assert_eq!(decision.decision, ResumeDecisionKind::NeedsUserConfirmation);
        assert_eq!(
            decision.replay_continuity_policy,
            ReplayContinuityPolicy::RequireUserConfirmation
        );
        assert_eq!(decision.reason_code, ResumeReasonCode::MutatingToolInFlight.as_str());
    }

    #[test]
    fn stale_freshness_blocks_resume() {
        let mut input = base_input();
        input.last_transcript_event_unix_ms = Some(1_000);
        input.last_model_turn_unix_ms = Some(1_000);
        input.observed_at_unix_ms = 1_000
            + i64::try_from(DEFAULT_RESUME_FRESHNESS_TTL_MS).expect("default ttl fits i64")
            + 1;

        let decision = classify_resume(&input);

        assert_eq!(decision.decision, ResumeDecisionKind::StaleDoNotResume);
        assert_eq!(decision.replay_continuity_policy, ReplayContinuityPolicy::DoNotReplay);
        assert_eq!(decision.reason_code, ResumeReasonCode::FreshnessWindowExpired.as_str());
    }

    #[test]
    fn terminal_run_state_never_resumes() {
        let mut input = base_input();
        input.run_state = "done".to_owned();

        let decision = classify_resume(&input);

        assert_eq!(decision.decision, ResumeDecisionKind::TerminalDoNotResume);
        assert_eq!(decision.reason_code, ResumeReasonCode::TerminalRunState.as_str());
    }

    #[test]
    fn principal_mismatch_is_policy_blocked() {
        let mut input = base_input();
        input.reconnect_principal = Some("user:other".to_owned());

        let decision = classify_resume(&input);

        assert_eq!(decision.decision, ResumeDecisionKind::PolicyBlocked);
        assert_eq!(decision.replay_continuity_policy, ReplayContinuityPolicy::BlockedByPolicy);
        assert_eq!(decision.reason_code, ResumeReasonCode::PrincipalMismatch.as_str());
    }

    #[test]
    fn tape_summary_distinguishes_read_only_wait_from_mutating_wait() {
        let read_only = summarize_resume_tape_observations(&[
            tape_event("message.received", 1_000, json!({"text": "check status"})),
            tape_event("provider_turn_output", 1_100, json!({"text": "checking"})),
            tape_event(
                "tool_proposal",
                1_200,
                json!({"tool_proposal": {"proposal_id": "p1", "tool_name": "palyra.process.status"}}),
            ),
            tape_event(
                "tool_decision",
                1_300,
                json!({"tool_decision": {"proposal_id": "p1", "kind": "allow"}}),
            ),
        ]);
        assert!(read_only.read_only_tool_wait);
        assert!(!read_only.mutating_tool_in_flight);

        let mutating = summarize_resume_tape_observations(&[
            tape_event(
                "tool_proposal",
                1_200,
                json!({"tool_proposal": {"proposal_id": "p2", "tool_name": "palyra.fs.apply_patch"}}),
            ),
            tape_event(
                "tool_decision",
                1_300,
                json!({"tool_decision": {"proposal_id": "p2", "kind": "allow"}}),
            ),
        ]);
        assert!(mutating.mutating_tool_in_flight);
        assert!(!mutating.read_only_tool_wait);
    }

    #[test]
    fn tape_summary_uses_download_output_path_effect() {
        let mutating = summarize_resume_tape_observations(&[
            tape_event(
                "tool_proposal",
                1_200,
                json!({
                    "tool_proposal": {
                        "proposal_id": "download",
                        "tool_name": "palyra.browser.downloads.get",
                        "input_json": {
                            "download_id": "download-1",
                            "output_path": "artifacts/report.pdf"
                        }
                    }
                }),
            ),
            tape_event(
                "tool_decision",
                1_300,
                json!({"tool_decision": {"proposal_id": "download", "kind": "allow"}}),
            ),
        ]);
        assert!(mutating.mutating_tool_in_flight);
        assert!(!mutating.read_only_tool_wait);

        let read_only = summarize_resume_tape_observations(&[
            tape_event(
                "tool_proposal",
                1_200,
                json!({
                    "tool_proposal": {
                        "proposal_id": "download",
                        "tool_name": "palyra.browser.downloads.get",
                        "input_json": {"download_id": "download-1"}
                    }
                }),
            ),
            tape_event(
                "tool_decision",
                1_300,
                json!({"tool_decision": {"proposal_id": "download", "kind": "allow"}}),
            ),
        ]);
        assert!(read_only.read_only_tool_wait);
        assert!(!read_only.mutating_tool_in_flight);
    }

    #[test]
    fn tape_summary_retains_each_unresolved_tool_until_its_result() {
        let mut events = vec![
            tape_event(
                "tool_proposal",
                1_100,
                json!({
                    "tool_proposal": {
                        "proposal_id": "mutating",
                        "tool_name": "palyra.fs.apply_patch"
                    }
                }),
            ),
            tape_event(
                "tool_proposal",
                1_200,
                json!({
                    "tool_proposal": {
                        "proposal_id": "read-only",
                        "tool_name": "palyra.process.status"
                    }
                }),
            ),
            tape_event(
                "tool_decision",
                1_300,
                json!({"tool_decision": {"proposal_id": "mutating", "kind": "allow"}}),
            ),
            tape_event(
                "tool_decision",
                1_400,
                json!({"tool_decision": {"proposal_id": "read-only", "kind": "allow"}}),
            ),
        ];

        let mixed = summarize_resume_tape_observations(events.as_slice());
        assert!(mixed.mutating_tool_in_flight);
        assert!(!mixed.read_only_tool_wait);

        events.push(tape_event(
            "tool_result",
            1_500,
            json!({"tool_result": {"proposal_id": "read-only"}}),
        ));
        let mutating_only = summarize_resume_tape_observations(events.as_slice());
        assert!(mutating_only.mutating_tool_in_flight);
        assert!(!mutating_only.read_only_tool_wait);

        events.push(tape_event(
            "tool_result",
            1_600,
            json!({"tool_result": {"proposal_id": "mutating"}}),
        ));
        let resolved = summarize_resume_tape_observations(events.as_slice());
        assert!(!resolved.mutating_tool_in_flight);
        assert!(!resolved.read_only_tool_wait);
    }

    fn base_input() -> ResumeClassifierInput {
        ResumeClassifierInput {
            run_id: "run-1".to_owned(),
            session_id: "session-1".to_owned(),
            run_state: "in_progress".to_owned(),
            run_principal: "user:ops".to_owned(),
            reconnect_principal: Some("user:ops".to_owned()),
            run_channel: Some("console".to_owned()),
            reconnect_channel: Some("console".to_owned()),
            channel_exists: true,
            observed_at_unix_ms: 2_000,
            max_freshness_age_ms: DEFAULT_RESUME_FRESHNESS_TTL_MS,
            last_transcript_event_unix_ms: Some(1_000),
            last_model_turn_unix_ms: Some(1_000),
            mutating_tool_in_flight: false,
            read_only_tool_wait: true,
            pending_approval: false,
            routine_lease_active: false,
            workspace_mutation_checkpoint_clean: true,
        }
    }

    fn tape_event(
        event_type: &str,
        created_at_unix_ms: i64,
        payload: serde_json::Value,
    ) -> ResumeTapeObservation {
        ResumeTapeObservation {
            event_type: event_type.to_owned(),
            payload_json: payload.to_string(),
            created_at_unix_ms,
        }
    }
}
