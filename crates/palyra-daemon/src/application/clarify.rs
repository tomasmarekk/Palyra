//! Structured clarification requests shared by model tools and UI surfaces.
//!
//! The module is pure policy: it validates the question contract, selects the
//! queue/default/reject outcome, and emits a metadata-only audit projection.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::json;

pub(crate) const CLARIFY_SCHEMA_VERSION: u64 = 1;
pub(crate) const CLARIFY_TOOL_NAME: &str = "palyra.clarify.ask";
pub(crate) const MAX_CLARIFY_CHOICES: usize = 8;
pub(crate) const MIN_CLARIFY_TIMEOUT_MS: u64 = 1_000;
pub(crate) const MAX_CLARIFY_TIMEOUT_MS: u64 = 15 * 60 * 1_000;
const CLARIFY_REDACTION_LEVEL: &str = "metadata_only";

/// One bounded option presented with a clarification question.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ClarifyChoice {
    pub choice_id: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Clarification request normalized before it reaches a user-facing queue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StructuredClarifyRequest {
    pub principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub question: String,
    pub choices: Vec<ClarifyChoice>,
    pub allow_free_text: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_choice_id: Option<String>,
    pub timeout_ms: u64,
}

/// Current queue posture for the same principal/session/run scope.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ClarifyQueueState {
    pub interactive_channel_available: bool,
    pub pending_for_scope: bool,
    pub pending_count_for_principal: usize,
    pub max_pending_per_principal: usize,
}

/// Terminal decision for the structured clarify request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClarifyDecisionKind {
    Pending,
    Defaulted,
    Rejected,
}

impl ClarifyDecisionKind {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Defaulted => "defaulted",
            Self::Rejected => "rejected",
        }
    }
}

/// Stable clarify reason codes consumed by audit fixtures and UI messages.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ClarifyReasonCode {
    #[serde(rename = "clarify.pending")]
    Pending,
    #[serde(rename = "clarify.defaulted.no_interactive_channel")]
    DefaultedNoInteractiveChannel,
    #[serde(rename = "clarify.rejected.missing_principal")]
    MissingPrincipal,
    #[serde(rename = "clarify.rejected.missing_scope")]
    MissingScope,
    #[serde(rename = "clarify.rejected.empty_question")]
    EmptyQuestion,
    #[serde(rename = "clarify.rejected.no_answer_mode")]
    NoAnswerMode,
    #[serde(rename = "clarify.rejected.too_many_choices")]
    TooManyChoices,
    #[serde(rename = "clarify.rejected.empty_choice_id")]
    EmptyChoiceId,
    #[serde(rename = "clarify.rejected.duplicate_choice_id")]
    DuplicateChoiceId,
    #[serde(rename = "clarify.rejected.default_choice_missing")]
    DefaultChoiceMissing,
    #[serde(rename = "clarify.rejected.timeout_out_of_range")]
    TimeoutOutOfRange,
    #[serde(rename = "clarify.rejected.pending_scope_exists")]
    PendingScopeExists,
    #[serde(rename = "clarify.rejected.pending_principal_limit")]
    PendingPrincipalLimit,
    #[serde(rename = "clarify.rejected.no_interactive_channel")]
    NoInteractiveChannel,
}

impl ClarifyReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "clarify.pending",
            Self::DefaultedNoInteractiveChannel => "clarify.defaulted.no_interactive_channel",
            Self::MissingPrincipal => "clarify.rejected.missing_principal",
            Self::MissingScope => "clarify.rejected.missing_scope",
            Self::EmptyQuestion => "clarify.rejected.empty_question",
            Self::NoAnswerMode => "clarify.rejected.no_answer_mode",
            Self::TooManyChoices => "clarify.rejected.too_many_choices",
            Self::EmptyChoiceId => "clarify.rejected.empty_choice_id",
            Self::DuplicateChoiceId => "clarify.rejected.duplicate_choice_id",
            Self::DefaultChoiceMissing => "clarify.rejected.default_choice_missing",
            Self::TimeoutOutOfRange => "clarify.rejected.timeout_out_of_range",
            Self::PendingScopeExists => "clarify.rejected.pending_scope_exists",
            Self::PendingPrincipalLimit => "clarify.rejected.pending_principal_limit",
            Self::NoInteractiveChannel => "clarify.rejected.no_interactive_channel",
        }
    }
}

/// Metadata-only event projection for one clarify decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ClarifyAuditProjection {
    pub schema_version: u64,
    pub event_type: String,
    pub reason_code: String,
    pub scope_kind: String,
    pub scope_id_hash: String,
    pub principal_hash: String,
    pub question_hash: String,
    pub choice_count: usize,
    pub allow_free_text: bool,
    pub timeout_ms: u64,
    pub default_choice_present: bool,
    pub redaction_level: String,
    pub payload_json: String,
}

/// Pure queue/default/reject decision for `palyra.clarify.ask`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ClarifyDecision {
    pub kind: ClarifyDecisionKind,
    pub reason_code: String,
    pub accepted: bool,
    pub selected_choice_id: Option<String>,
    pub queue_key: Option<String>,
    pub audit: ClarifyAuditProjection,
}

#[must_use]
pub(crate) fn decide_structured_clarify_request(
    request: &StructuredClarifyRequest,
    queue_state: ClarifyQueueState,
) -> ClarifyDecision {
    if request.principal.trim().is_empty() {
        return rejected(request, ClarifyReasonCode::MissingPrincipal);
    }
    if clarify_scope(request).is_none() {
        return rejected(request, ClarifyReasonCode::MissingScope);
    }
    if request.question.trim().is_empty() {
        return rejected(request, ClarifyReasonCode::EmptyQuestion);
    }
    if request.choices.is_empty() && !request.allow_free_text {
        return rejected(request, ClarifyReasonCode::NoAnswerMode);
    }
    if request.choices.len() > MAX_CLARIFY_CHOICES {
        return rejected(request, ClarifyReasonCode::TooManyChoices);
    }
    if !(MIN_CLARIFY_TIMEOUT_MS..=MAX_CLARIFY_TIMEOUT_MS).contains(&request.timeout_ms) {
        return rejected(request, ClarifyReasonCode::TimeoutOutOfRange);
    }

    let mut choice_ids = BTreeSet::new();
    for choice in &request.choices {
        let choice_id = choice.choice_id.trim();
        if choice_id.is_empty() {
            return rejected(request, ClarifyReasonCode::EmptyChoiceId);
        }
        if !choice_ids.insert(choice_id.to_owned()) {
            return rejected(request, ClarifyReasonCode::DuplicateChoiceId);
        }
    }
    if let Some(default_choice_id) = request.default_choice_id.as_deref() {
        if !choice_ids.contains(default_choice_id.trim()) {
            return rejected(request, ClarifyReasonCode::DefaultChoiceMissing);
        }
    }
    if queue_state.pending_for_scope {
        return rejected(request, ClarifyReasonCode::PendingScopeExists);
    }
    if queue_state.pending_count_for_principal >= queue_state.max_pending_per_principal {
        return rejected(request, ClarifyReasonCode::PendingPrincipalLimit);
    }
    if !queue_state.interactive_channel_available {
        return match request.default_choice_id.as_deref().map(str::trim).filter(|id| !id.is_empty())
        {
            Some(default_choice_id) => ClarifyDecision {
                kind: ClarifyDecisionKind::Defaulted,
                reason_code: ClarifyReasonCode::DefaultedNoInteractiveChannel.as_str().to_owned(),
                accepted: true,
                selected_choice_id: Some(default_choice_id.to_owned()),
                queue_key: None,
                audit: audit_projection(
                    request,
                    ClarifyDecisionKind::Defaulted,
                    ClarifyReasonCode::DefaultedNoInteractiveChannel,
                ),
            },
            None => rejected(request, ClarifyReasonCode::NoInteractiveChannel),
        };
    }

    ClarifyDecision {
        kind: ClarifyDecisionKind::Pending,
        reason_code: ClarifyReasonCode::Pending.as_str().to_owned(),
        accepted: true,
        selected_choice_id: None,
        queue_key: clarify_scope(request).map(|(kind, id)| format!("{kind}:{id}")),
        audit: audit_projection(request, ClarifyDecisionKind::Pending, ClarifyReasonCode::Pending),
    }
}

fn rejected(request: &StructuredClarifyRequest, reason_code: ClarifyReasonCode) -> ClarifyDecision {
    ClarifyDecision {
        kind: ClarifyDecisionKind::Rejected,
        reason_code: reason_code.as_str().to_owned(),
        accepted: false,
        selected_choice_id: None,
        queue_key: None,
        audit: audit_projection(request, ClarifyDecisionKind::Rejected, reason_code),
    }
}

fn audit_projection(
    request: &StructuredClarifyRequest,
    kind: ClarifyDecisionKind,
    reason_code: ClarifyReasonCode,
) -> ClarifyAuditProjection {
    let (scope_kind, scope_id) =
        clarify_scope(request).unwrap_or(("unknown", request.principal.as_str()));
    let payload = json!({
        "schema_version": CLARIFY_SCHEMA_VERSION,
        "tool": CLARIFY_TOOL_NAME,
        "decision": kind.as_str(),
        "reason_code": reason_code.as_str(),
        "scope_kind": scope_kind,
        "scope_id_hash": crate::sha256_hex(scope_id.as_bytes()),
        "principal_hash": crate::sha256_hex(request.principal.trim().as_bytes()),
        "question_hash": crate::sha256_hex(request.question.trim().as_bytes()),
        "choice_count": request.choices.len(),
        "allow_free_text": request.allow_free_text,
        "timeout_ms": request.timeout_ms,
        "default_choice_present": request.default_choice_id.is_some(),
        "not_approval": true,
    });
    ClarifyAuditProjection {
        schema_version: CLARIFY_SCHEMA_VERSION,
        event_type: format!("clarify.ask.{}", kind.as_str()),
        reason_code: reason_code.as_str().to_owned(),
        scope_kind: scope_kind.to_owned(),
        scope_id_hash: crate::sha256_hex(scope_id.as_bytes()),
        principal_hash: crate::sha256_hex(request.principal.trim().as_bytes()),
        question_hash: crate::sha256_hex(request.question.trim().as_bytes()),
        choice_count: request.choices.len(),
        allow_free_text: request.allow_free_text,
        timeout_ms: request.timeout_ms,
        default_choice_present: request.default_choice_id.is_some(),
        redaction_level: CLARIFY_REDACTION_LEVEL.to_owned(),
        payload_json: payload.to_string(),
    }
}

fn clarify_scope(request: &StructuredClarifyRequest) -> Option<(&'static str, &str)> {
    request
        .run_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(|run_id| ("run", run_id))
        .or_else(|| {
            request
                .session_id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(|session_id| ("session", session_id))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn choice(choice_id: &str) -> ClarifyChoice {
        ClarifyChoice {
            choice_id: choice_id.to_owned(),
            label: choice_id.to_owned(),
            description: None,
        }
    }

    fn request() -> StructuredClarifyRequest {
        StructuredClarifyRequest {
            principal: "user:ops".to_owned(),
            session_id: Some("session-1".to_owned()),
            run_id: Some("run-1".to_owned()),
            question: "Which branch should continue?".to_owned(),
            choices: vec![choice("main"), choice("hold")],
            allow_free_text: false,
            default_choice_id: Some("hold".to_owned()),
            timeout_ms: 60_000,
        }
    }

    fn queue_state() -> ClarifyQueueState {
        ClarifyQueueState {
            interactive_channel_available: true,
            pending_for_scope: false,
            pending_count_for_principal: 0,
            max_pending_per_principal: 4,
        }
    }

    #[test]
    fn clarify_request_queues_with_metadata_only_audit() {
        let decision = decide_structured_clarify_request(&request(), queue_state());

        assert_eq!(decision.kind, ClarifyDecisionKind::Pending);
        assert!(decision.accepted);
        assert_eq!(decision.queue_key.as_deref(), Some("run:run-1"));
        assert_eq!(decision.audit.event_type, "clarify.ask.pending");
        assert_eq!(decision.audit.scope_kind, "run");
        assert!(!decision.audit.payload_json.contains("Which branch"));
    }

    #[test]
    fn clarify_request_defaults_when_channel_is_missing_and_default_exists() {
        let mut state = queue_state();
        state.interactive_channel_available = false;

        let decision = decide_structured_clarify_request(&request(), state);

        assert_eq!(decision.kind, ClarifyDecisionKind::Defaulted);
        assert!(decision.accepted);
        assert_eq!(decision.selected_choice_id.as_deref(), Some("hold"));
        assert_eq!(decision.reason_code, ClarifyReasonCode::DefaultedNoInteractiveChannel.as_str());
    }

    #[test]
    fn clarify_request_rejects_duplicate_choices_and_missing_default() {
        let mut duplicate = request();
        duplicate.choices = vec![choice("same"), choice("same")];
        let duplicate_decision = decide_structured_clarify_request(&duplicate, queue_state());
        assert_eq!(duplicate_decision.kind, ClarifyDecisionKind::Rejected);
        assert_eq!(duplicate_decision.reason_code, ClarifyReasonCode::DuplicateChoiceId.as_str());

        let mut missing_default = request();
        missing_default.default_choice_id = Some("missing".to_owned());
        let missing_decision = decide_structured_clarify_request(&missing_default, queue_state());
        assert_eq!(missing_decision.reason_code, ClarifyReasonCode::DefaultChoiceMissing.as_str());
    }
}
