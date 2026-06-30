//! Replay-continuity policy for provider and user transcript reconstruction.
//!
//! The policy is observe-only in this milestone: it records whether provider
//! history and user transcript evidence would be safe to replay, without
//! changing the existing provider dispatch or startup recovery behavior.

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::application::resume_classifier::{
    ReplayContinuityPolicy, ResumeDecision, ResumeDecisionKind, ResumeTapeObservation,
};

pub(crate) const REPLAY_CONTINUITY_SCHEMA_VERSION: i64 = 1;
pub(crate) const REPLAY_CONTINUITY_EVENT_STARTED: &str =
    "replay_continuity_policy_pro_provider_a_user_transcript.started";
pub(crate) const REPLAY_CONTINUITY_EVENT_COMPLETED: &str =
    "replay_continuity_policy_pro_provider_a_user_transcript.completed";
pub(crate) const REPLAY_CONTINUITY_EVENT_FAILED: &str =
    "replay_continuity_policy_pro_provider_a_user_transcript.failed";
pub(crate) const REPLAY_CONTINUITY_REDACTION_NONE: &str = "none";
pub(crate) const REPLAY_CONTINUITY_ROLLOUT_OBSERVE_ONLY: &str = "observe_only";

/// Transcript surface considered by replay-continuity policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplayTranscriptSurface {
    ProviderTranscript,
    UserTranscript,
}

impl ReplayTranscriptSurface {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderTranscript => "provider_transcript",
            Self::UserTranscript => "user_transcript",
        }
    }
}

/// Replay decision for one transcript surface.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplayTranscriptDecisionKind {
    AllowReplay,
    RequireUserConfirmation,
    SkipReplay,
    BlockReplay,
}

impl ReplayTranscriptDecisionKind {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AllowReplay => "allow_replay",
            Self::RequireUserConfirmation => "require_user_confirmation",
            Self::SkipReplay => "skip_replay",
            Self::BlockReplay => "block_replay",
        }
    }
}

/// Stable reason codes emitted by transcript replay policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayTranscriptReasonCode {
    ProviderTranscriptAllowed,
    UserTranscriptAllowed,
    ResumeRequiresConfirmation,
    ResumeForbidsReplay,
    ResumePolicyBlocked,
    TranscriptEvidenceMissing,
    FreshnessExpired,
}

impl ReplayTranscriptReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderTranscriptAllowed => "replay.provider_transcript_allowed",
            Self::UserTranscriptAllowed => "replay.user_transcript_allowed",
            Self::ResumeRequiresConfirmation => "replay.resume_requires_confirmation",
            Self::ResumeForbidsReplay => "replay.resume_forbids_replay",
            Self::ResumePolicyBlocked => "replay.resume_policy_blocked",
            Self::TranscriptEvidenceMissing => "replay.transcript_evidence_missing",
            Self::FreshnessExpired => "replay.freshness_expired",
        }
    }
}

/// Redacted tape evidence counts used by replay-continuity policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct ReplayTranscriptSignals {
    pub(crate) provider_transcript_event_count: u64,
    pub(crate) user_transcript_event_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_provider_transcript_event_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_user_transcript_event_unix_ms: Option<i64>,
}

impl ReplayTranscriptSignals {
    #[must_use]
    fn surface_event_count(&self, surface: ReplayTranscriptSurface) -> u64 {
        match surface {
            ReplayTranscriptSurface::ProviderTranscript => self.provider_transcript_event_count,
            ReplayTranscriptSurface::UserTranscript => self.user_transcript_event_count,
        }
    }

    #[must_use]
    fn surface_last_event_unix_ms(&self, surface: ReplayTranscriptSurface) -> Option<i64> {
        match surface {
            ReplayTranscriptSurface::ProviderTranscript => {
                self.last_provider_transcript_event_unix_ms
            }
            ReplayTranscriptSurface::UserTranscript => self.last_user_transcript_event_unix_ms,
        }
    }
}

/// Input snapshot for projecting replay policy from a resume decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReplayContinuityPolicyInput {
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    pub(crate) resume_decision: ResumeDecisionKind,
    pub(crate) resume_replay_policy: ReplayContinuityPolicy,
    pub(crate) resume_reason_code: String,
    pub(crate) observed_at_unix_ms: i64,
    pub(crate) max_freshness_age_ms: u64,
    pub(crate) transcript_signals: ReplayTranscriptSignals,
}

impl ReplayContinuityPolicyInput {
    #[must_use]
    pub(crate) fn from_resume_decision(
        decision: &ResumeDecision,
        observed_at_unix_ms: i64,
        max_freshness_age_ms: u64,
        transcript_signals: ReplayTranscriptSignals,
    ) -> Self {
        Self {
            run_id: decision.run_id.clone(),
            session_id: decision.session_id.clone(),
            resume_decision: decision.decision,
            resume_replay_policy: decision.replay_continuity_policy,
            resume_reason_code: decision.reason_code.clone(),
            observed_at_unix_ms,
            max_freshness_age_ms,
            transcript_signals,
        }
    }
}

/// Replay decision for one transcript surface, safe to store in journal payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReplayTranscriptDecision {
    pub(crate) surface: ReplayTranscriptSurface,
    pub(crate) decision: ReplayTranscriptDecisionKind,
    pub(crate) reason_code: String,
    pub(crate) source_event_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_event_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) freshness_age_ms: Option<u64>,
    pub(crate) redaction_level: String,
}

/// Journal-ready replay-continuity projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReplayContinuityPolicyProjection {
    pub(crate) schema_version: i64,
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    pub(crate) rollout_mode: String,
    pub(crate) resume_decision: ResumeDecisionKind,
    pub(crate) resume_replay_policy: ReplayContinuityPolicy,
    pub(crate) resume_reason_code: String,
    pub(crate) provider_transcript: ReplayTranscriptDecision,
    pub(crate) user_transcript: ReplayTranscriptDecision,
    pub(crate) payload_json: String,
    pub(crate) evidence_refs_json: String,
    pub(crate) redaction_level: String,
}

/// Summarizes transcript replay evidence without preserving raw transcript text.
#[must_use]
pub(crate) fn summarize_replay_transcript_observations(
    events: &[ResumeTapeObservation],
) -> ReplayTranscriptSignals {
    let mut signals = ReplayTranscriptSignals::default();
    for event in events {
        if is_provider_transcript_event(event.event_type.as_str()) {
            signals.provider_transcript_event_count =
                signals.provider_transcript_event_count.saturating_add(1);
            signals.last_provider_transcript_event_unix_ms = Some(event.created_at_unix_ms);
        }
        if is_user_transcript_event(event.event_type.as_str()) {
            signals.user_transcript_event_count =
                signals.user_transcript_event_count.saturating_add(1);
            signals.last_user_transcript_event_unix_ms = Some(event.created_at_unix_ms);
        }
    }
    signals
}

/// Projects provider/user transcript replay decisions from the resume policy.
#[must_use]
pub(crate) fn project_replay_continuity_policy(
    input: &ReplayContinuityPolicyInput,
) -> ReplayContinuityPolicyProjection {
    let provider_transcript =
        project_surface_replay_decision(input, ReplayTranscriptSurface::ProviderTranscript);
    let user_transcript =
        project_surface_replay_decision(input, ReplayTranscriptSurface::UserTranscript);
    let evidence_refs = json!([
        evidence_ref(input, ReplayTranscriptSurface::ProviderTranscript, &provider_transcript,),
        evidence_ref(input, ReplayTranscriptSurface::UserTranscript, &user_transcript),
    ]);
    let evidence_refs_json = evidence_refs.to_string();
    let payload_json = json!({
        "event": REPLAY_CONTINUITY_EVENT_COMPLETED,
        "schema_version": REPLAY_CONTINUITY_SCHEMA_VERSION,
        "rollout_mode": REPLAY_CONTINUITY_ROLLOUT_OBSERVE_ONLY,
        "run_id": input.run_id.as_str(),
        "session_id": input.session_id.as_str(),
        "resume_decision": input.resume_decision.as_str(),
        "resume_replay_policy": input.resume_replay_policy.as_str(),
        "resume_reason_code": input.resume_reason_code.as_str(),
        "provider_transcript": transcript_decision_payload(&provider_transcript),
        "user_transcript": transcript_decision_payload(&user_transcript),
        "evidence_refs": evidence_refs,
        "redaction_level": REPLAY_CONTINUITY_REDACTION_NONE,
    })
    .to_string();
    ReplayContinuityPolicyProjection {
        schema_version: REPLAY_CONTINUITY_SCHEMA_VERSION,
        run_id: input.run_id.clone(),
        session_id: input.session_id.clone(),
        rollout_mode: REPLAY_CONTINUITY_ROLLOUT_OBSERVE_ONLY.to_owned(),
        resume_decision: input.resume_decision,
        resume_replay_policy: input.resume_replay_policy,
        resume_reason_code: input.resume_reason_code.clone(),
        provider_transcript,
        user_transcript,
        payload_json,
        evidence_refs_json,
        redaction_level: REPLAY_CONTINUITY_REDACTION_NONE.to_owned(),
    }
}

fn project_surface_replay_decision(
    input: &ReplayContinuityPolicyInput,
    surface: ReplayTranscriptSurface,
) -> ReplayTranscriptDecision {
    let source_event_count = input.transcript_signals.surface_event_count(surface);
    let last_event_unix_ms = input.transcript_signals.surface_last_event_unix_ms(surface);
    let freshness_age_ms =
        last_event_unix_ms.map(|created_at| age_ms(input.observed_at_unix_ms, created_at));
    let (decision, reason_code) = match input.resume_replay_policy {
        ReplayContinuityPolicy::BlockedByPolicy => (
            ReplayTranscriptDecisionKind::BlockReplay,
            ReplayTranscriptReasonCode::ResumePolicyBlocked,
        ),
        ReplayContinuityPolicy::DoNotReplay => (
            ReplayTranscriptDecisionKind::BlockReplay,
            ReplayTranscriptReasonCode::ResumeForbidsReplay,
        ),
        ReplayContinuityPolicy::RequireUserConfirmation => (
            ReplayTranscriptDecisionKind::RequireUserConfirmation,
            ReplayTranscriptReasonCode::ResumeRequiresConfirmation,
        ),
        ReplayContinuityPolicy::ContinueFromTape => allow_or_skip_surface(
            surface,
            source_event_count,
            freshness_age_ms,
            input.max_freshness_age_ms,
        ),
    };
    ReplayTranscriptDecision {
        surface,
        decision,
        reason_code: reason_code.as_str().to_owned(),
        source_event_count,
        last_event_unix_ms,
        freshness_age_ms,
        redaction_level: REPLAY_CONTINUITY_REDACTION_NONE.to_owned(),
    }
}

fn allow_or_skip_surface(
    surface: ReplayTranscriptSurface,
    source_event_count: u64,
    freshness_age_ms: Option<u64>,
    max_freshness_age_ms: u64,
) -> (ReplayTranscriptDecisionKind, ReplayTranscriptReasonCode) {
    if source_event_count == 0 || freshness_age_ms.is_none() {
        return (
            ReplayTranscriptDecisionKind::SkipReplay,
            ReplayTranscriptReasonCode::TranscriptEvidenceMissing,
        );
    }
    if freshness_age_ms.is_some_and(|age| age > max_freshness_age_ms) {
        return (
            ReplayTranscriptDecisionKind::BlockReplay,
            ReplayTranscriptReasonCode::FreshnessExpired,
        );
    }
    let reason = match surface {
        ReplayTranscriptSurface::ProviderTranscript => {
            ReplayTranscriptReasonCode::ProviderTranscriptAllowed
        }
        ReplayTranscriptSurface::UserTranscript => {
            ReplayTranscriptReasonCode::UserTranscriptAllowed
        }
    };
    (ReplayTranscriptDecisionKind::AllowReplay, reason)
}

fn evidence_ref(
    input: &ReplayContinuityPolicyInput,
    surface: ReplayTranscriptSurface,
    decision: &ReplayTranscriptDecision,
) -> serde_json::Value {
    json!({
        "kind": "orchestrator_tape",
        "run_id": input.run_id.as_str(),
        "session_id": input.session_id.as_str(),
        "surface": surface.as_str(),
        "source_event_count": decision.source_event_count,
        "last_event_unix_ms": decision.last_event_unix_ms,
        "redaction_level": REPLAY_CONTINUITY_REDACTION_NONE,
    })
}

fn transcript_decision_payload(decision: &ReplayTranscriptDecision) -> serde_json::Value {
    json!({
        "surface": decision.surface.as_str(),
        "decision": decision.decision.as_str(),
        "reason_code": decision.reason_code.as_str(),
        "source_event_count": decision.source_event_count,
        "last_event_unix_ms": decision.last_event_unix_ms,
        "freshness_age_ms": decision.freshness_age_ms,
        "redaction_level": decision.redaction_level.as_str(),
    })
}

fn age_ms(observed_at_unix_ms: i64, created_at_unix_ms: i64) -> u64 {
    u64::try_from(observed_at_unix_ms.saturating_sub(created_at_unix_ms).max(0)).unwrap_or_default()
}

fn is_provider_transcript_event(event_type: &str) -> bool {
    matches!(event_type, "provider_turn_output" | "message.replied")
}

fn is_user_transcript_event(event_type: &str) -> bool {
    matches!(event_type, "message.received" | "queued.input")
}

#[cfg(test)]
mod tests {
    use super::{
        project_replay_continuity_policy, summarize_replay_transcript_observations,
        ReplayContinuityPolicyInput, ReplayTranscriptDecisionKind, ReplayTranscriptReasonCode,
        ReplayTranscriptSignals, ReplayTranscriptSurface, REPLAY_CONTINUITY_EVENT_COMPLETED,
    };
    use crate::application::resume_classifier::{
        ReplayContinuityPolicy, ResumeDecisionKind, ResumeTapeObservation,
    };

    #[test]
    fn continue_from_tape_allows_fresh_provider_and_user_transcript_replay() {
        let projection = project_replay_continuity_policy(&base_input(
            ReplayContinuityPolicy::ContinueFromTape,
            ReplayTranscriptSignals {
                provider_transcript_event_count: 2,
                user_transcript_event_count: 1,
                last_provider_transcript_event_unix_ms: Some(1_800),
                last_user_transcript_event_unix_ms: Some(1_700),
            },
        ));

        assert_eq!(
            projection.provider_transcript.decision,
            ReplayTranscriptDecisionKind::AllowReplay
        );
        assert_eq!(
            projection.provider_transcript.reason_code,
            ReplayTranscriptReasonCode::ProviderTranscriptAllowed.as_str()
        );
        assert_eq!(projection.user_transcript.decision, ReplayTranscriptDecisionKind::AllowReplay);
        assert_eq!(
            projection.user_transcript.reason_code,
            ReplayTranscriptReasonCode::UserTranscriptAllowed.as_str()
        );
        let payload = serde_json::from_str::<serde_json::Value>(projection.payload_json.as_str())
            .expect("projection payload should be json");
        assert_eq!(payload["event"], REPLAY_CONTINUITY_EVENT_COMPLETED);
        assert_eq!(payload["rollout_mode"], "observe_only");

        let roundtrip = serde_json::from_str::<super::ReplayContinuityPolicyProjection>(
            serde_json::to_string(&projection).expect("projection should serialize").as_str(),
        )
        .expect("projection should deserialize");
        assert_eq!(roundtrip, projection);
    }

    #[test]
    fn require_confirmation_keeps_transcript_replay_gated() {
        let projection = project_replay_continuity_policy(&base_input(
            ReplayContinuityPolicy::RequireUserConfirmation,
            fresh_signals(),
        ));

        assert_eq!(
            projection.provider_transcript.decision,
            ReplayTranscriptDecisionKind::RequireUserConfirmation
        );
        assert_eq!(
            projection.user_transcript.reason_code,
            ReplayTranscriptReasonCode::ResumeRequiresConfirmation.as_str()
        );
    }

    #[test]
    fn blocked_resume_policy_blocks_both_transcript_surfaces() {
        let projection = project_replay_continuity_policy(&base_input(
            ReplayContinuityPolicy::BlockedByPolicy,
            fresh_signals(),
        ));

        assert_eq!(
            projection.provider_transcript.decision,
            ReplayTranscriptDecisionKind::BlockReplay
        );
        assert_eq!(
            projection.user_transcript.reason_code,
            ReplayTranscriptReasonCode::ResumePolicyBlocked.as_str()
        );
    }

    #[test]
    fn missing_or_stale_evidence_does_not_replay_transcripts() {
        let missing = project_replay_continuity_policy(&base_input(
            ReplayContinuityPolicy::ContinueFromTape,
            ReplayTranscriptSignals {
                provider_transcript_event_count: 0,
                user_transcript_event_count: 1,
                last_provider_transcript_event_unix_ms: None,
                last_user_transcript_event_unix_ms: Some(1_700),
            },
        ));
        assert_eq!(missing.provider_transcript.decision, ReplayTranscriptDecisionKind::SkipReplay);
        assert_eq!(
            missing.provider_transcript.reason_code,
            ReplayTranscriptReasonCode::TranscriptEvidenceMissing.as_str()
        );

        let stale = project_replay_continuity_policy(&base_input(
            ReplayContinuityPolicy::ContinueFromTape,
            ReplayTranscriptSignals {
                provider_transcript_event_count: 1,
                user_transcript_event_count: 1,
                last_provider_transcript_event_unix_ms: Some(100),
                last_user_transcript_event_unix_ms: Some(1_700),
            },
        ));
        assert_eq!(stale.provider_transcript.decision, ReplayTranscriptDecisionKind::BlockReplay);
        assert_eq!(
            stale.provider_transcript.reason_code,
            ReplayTranscriptReasonCode::FreshnessExpired.as_str()
        );
    }

    #[test]
    fn transcript_summary_counts_surfaces_without_raw_content() {
        let signals = summarize_replay_transcript_observations(&[
            tape_event("message.received", 1_000, r#"{"text":"secret-ish"}"#),
            tape_event("provider_turn_output", 1_100, r#"{"full_text":"answer"}"#),
            tape_event("message.replied", 1_200, r#"{"reply_text":"visible"}"#),
            tape_event("queued.input", 1_300, r#"{"text":"later"}"#),
            tape_event("tool_result", 1_400, r#"{"output":"not transcript"}"#),
        ]);

        assert_eq!(signals.user_transcript_event_count, 2);
        assert_eq!(signals.provider_transcript_event_count, 2);
        assert_eq!(signals.last_user_transcript_event_unix_ms, Some(1_300));
        assert_eq!(signals.last_provider_transcript_event_unix_ms, Some(1_200));
    }

    fn base_input(
        resume_replay_policy: ReplayContinuityPolicy,
        transcript_signals: ReplayTranscriptSignals,
    ) -> ReplayContinuityPolicyInput {
        ReplayContinuityPolicyInput {
            run_id: "run-1".to_owned(),
            session_id: "session-1".to_owned(),
            resume_decision: ResumeDecisionKind::SafeToResume,
            resume_replay_policy,
            resume_reason_code: "resume.fresh_read_only_tool_wait".to_owned(),
            observed_at_unix_ms: 2_000,
            max_freshness_age_ms: 1_000,
            transcript_signals,
        }
    }

    fn fresh_signals() -> ReplayTranscriptSignals {
        ReplayTranscriptSignals {
            provider_transcript_event_count: 1,
            user_transcript_event_count: 1,
            last_provider_transcript_event_unix_ms: Some(1_800),
            last_user_transcript_event_unix_ms: Some(1_700),
        }
    }

    fn tape_event(
        event_type: &str,
        created_at_unix_ms: i64,
        payload_json: &str,
    ) -> ResumeTapeObservation {
        ResumeTapeObservation {
            event_type: event_type.to_owned(),
            payload_json: payload_json.to_owned(),
            created_at_unix_ms,
        }
    }

    #[test]
    fn surface_names_are_stable() {
        assert_eq!(ReplayTranscriptSurface::ProviderTranscript.as_str(), "provider_transcript");
        assert_eq!(ReplayTranscriptSurface::UserTranscript.as_str(), "user_transcript");
    }
}
