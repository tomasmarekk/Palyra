//! Provider-turn recovery state and diagnostics.
//!
//! The state machine is deliberately side-effect free. It classifies provider
//! anomalies, spends bounded per-reason retry budgets, and returns redacted
//! tape-ready decisions. Run-stream orchestration owns the actual retry,
//! prompt mutation, compaction, credential refresh, and failover actions.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    model_provider::{
        ProviderFailureClass, ProviderFinishReason, TerminalOutcomeClass,
        TerminalOutcomeClassification,
    },
    sha256_hex,
};

pub(crate) const PROVIDER_TURN_RECOVERY_SCHEMA_VERSION: u16 = 1;
pub(crate) const PROVIDER_TURN_RECOVERY_EVENT: &str = "provider.turn_recovery.decision";
pub(crate) const PROVIDER_CONTEXT_PRESSURE_EVENT: &str = "provider.context_pressure";
pub(crate) const PROVIDER_CANCELLATION_CLOSURE_EVENT: &str = "provider.cancellation_closure";

const DEFAULT_SINGLE_RETRY_BUDGET: u8 = 1;
const DEFAULT_LENGTH_RETRY_BUDGET: u8 = 3;
const DEFAULT_TIMEOUT_RETRY_BUDGET: u8 = 1;
const MIN_COMPACTION_TOKEN_SAVINGS: u64 = 512;

/// Provider-neutral anomaly observed for one turn.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderTurnAnomaly {
    ProviderTimeout,
    BrowserFollowupTimeout,
    ToolFollowupTimeout,
    LengthFinalText,
    LengthToolArguments,
    MaxOutputTokensTooLarge,
    ReasoningOnly,
    EmptyFinalAnswer,
    EmptyPostToolResponse,
    PartialContentStream,
    PartialToolCall,
    DroppedToolCall,
    ToolCallsFinishWithoutPayload,
    MalformedToolSequence,
    InvalidToolName,
    MalformedJsonArguments,
    TruncatedToolArguments,
    ContentPolicyBlocked,
    ContextOverflow,
    MultimodalUnsupported,
    InvalidEncryptedContent,
    MalformedStream,
    AuthInvalid,
    AuthExpired,
    PermissionDenied,
    RateLimit,
    UnicodeSurrogate,
}

impl ProviderTurnAnomaly {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderTimeout => "provider_timeout",
            Self::BrowserFollowupTimeout => "browser_followup_timeout",
            Self::ToolFollowupTimeout => "tool_followup_timeout",
            Self::LengthFinalText => "length_final_text",
            Self::LengthToolArguments => "length_tool_arguments",
            Self::MaxOutputTokensTooLarge => "max_output_tokens_too_large",
            Self::ReasoningOnly => "reasoning_only",
            Self::EmptyFinalAnswer => "empty_final_answer",
            Self::EmptyPostToolResponse => "empty_post_tool_response",
            Self::PartialContentStream => "partial_content_stream",
            Self::PartialToolCall => "partial_tool_call",
            Self::DroppedToolCall => "dropped_tool_call",
            Self::ToolCallsFinishWithoutPayload => "tool_calls_finish_without_payload",
            Self::MalformedToolSequence => "malformed_tool_sequence",
            Self::InvalidToolName => "invalid_tool_name",
            Self::MalformedJsonArguments => "malformed_json_arguments",
            Self::TruncatedToolArguments => "truncated_tool_arguments",
            Self::ContentPolicyBlocked => "content_policy_blocked",
            Self::ContextOverflow => "context_overflow",
            Self::MultimodalUnsupported => "multimodal_unsupported",
            Self::InvalidEncryptedContent => "invalid_encrypted_content",
            Self::MalformedStream => "malformed_stream",
            Self::AuthInvalid => "auth_invalid",
            Self::AuthExpired => "auth_expired",
            Self::PermissionDenied => "permission_denied",
            Self::RateLimit => "rate_limit",
            Self::UnicodeSurrogate => "unicode_surrogate",
        }
    }
}

/// Runtime action selected by the recovery state machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderTurnRecoveryAction {
    RetrySameProvider,
    RetryWithPrompt,
    CompactAndRetry,
    LowerReasoningEffort,
    ShrinkMultimodal,
    StripUnsupportedContent,
    RefreshCredential,
    FailoverProvider,
    BackoffRetry,
    SyntheticToolResult,
    FailDeterministic,
}

impl ProviderTurnRecoveryAction {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RetrySameProvider => "retry_same_provider",
            Self::RetryWithPrompt => "retry_with_prompt",
            Self::CompactAndRetry => "compact_and_retry",
            Self::LowerReasoningEffort => "lower_reasoning_effort",
            Self::ShrinkMultimodal => "shrink_multimodal",
            Self::StripUnsupportedContent => "strip_unsupported_content",
            Self::RefreshCredential => "refresh_credential",
            Self::FailoverProvider => "failover_provider",
            Self::BackoffRetry => "backoff_retry",
            Self::SyntheticToolResult => "synthetic_tool_result",
            Self::FailDeterministic => "fail_deterministic",
        }
    }
}

/// How visible the recovery decision should be to the operator.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UserVisibleStatusPolicy {
    Silent,
    StatusOnly,
    SafeMessage,
}

impl UserVisibleStatusPolicy {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Silent => "silent",
            Self::StatusOnly => "status_only",
            Self::SafeMessage => "safe_message",
        }
    }
}

/// Optional context carried into one recovery decision.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProviderTurnRecoveryInput {
    pub(crate) credential_id: Option<String>,
    pub(crate) retry_after_ms: Option<u64>,
    pub(crate) context_pressure: Option<ContextPressureReport>,
}

/// Tape-ready decision selected for one anomaly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProviderTurnRecoveryDecision {
    pub(crate) schema_version: u16,
    pub(crate) event_type: String,
    pub(crate) anomaly: ProviderTurnAnomaly,
    pub(crate) action: ProviderTurnRecoveryAction,
    pub(crate) reason_code: String,
    pub(crate) attempt: u8,
    pub(crate) exhausted: bool,
    pub(crate) prompt_mutation: Option<String>,
    pub(crate) context_mutation_plan: Option<String>,
    pub(crate) user_visible_status_policy: UserVisibleStatusPolicy,
    pub(crate) retry_after_ms: Option<u64>,
    pub(crate) credential_ref_hash: Option<String>,
}

impl ProviderTurnRecoveryDecision {
    #[must_use]
    pub(crate) fn tape_payload(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "event": self.event_type,
            "redaction_level": "redacted_recovery_decision",
            "anomaly": self.anomaly.as_str(),
            "action": self.action.as_str(),
            "reason_code": self.reason_code,
            "attempt": self.attempt,
            "exhausted": self.exhausted,
            "prompt_mutation": self.prompt_mutation,
            "context_mutation_plan": self.context_mutation_plan,
            "user_visible_status_policy": self.user_visible_status_policy.as_str(),
            "retry_after_ms": self.retry_after_ms,
            "credential_ref_hash": self.credential_ref_hash,
        })
    }
}

/// Per-run state that prevents retry and compaction loops.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProviderTurnRecoveryState {
    attempts_by_anomaly: BTreeMap<ProviderTurnAnomaly, u8>,
    refreshed_credential_hashes: BTreeSet<String>,
    unicode_retry_used: bool,
    last_compaction_attempt: Option<CompactionAttemptSummary>,
}

impl ProviderTurnRecoveryState {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub(crate) fn record_compaction_attempt(&mut self, before_tokens: u64, after_tokens: u64) {
        let token_savings = before_tokens.saturating_sub(after_tokens);
        self.last_compaction_attempt =
            Some(CompactionAttemptSummary { before_tokens, after_tokens, token_savings });
    }

    #[must_use]
    pub(crate) fn compaction_cooldown_active(&self) -> bool {
        self.last_compaction_attempt
            .as_ref()
            .is_some_and(|attempt| attempt.token_savings < MIN_COMPACTION_TOKEN_SAVINGS)
    }

    #[must_use]
    pub(crate) fn decide(
        &mut self,
        anomaly: ProviderTurnAnomaly,
        input: ProviderTurnRecoveryInput,
    ) -> ProviderTurnRecoveryDecision {
        let attempt = self.record_attempt(anomaly);
        let credential_ref_hash = input
            .credential_id
            .as_deref()
            .map(|credential_id| short_hash(credential_id.as_bytes()));
        let budget = retry_budget(anomaly);
        let exhausted = attempt > budget;
        let mut action = recovery_action_for_anomaly(anomaly, exhausted);
        let mut reason_code = format!("provider.turn_recovery.{}", anomaly.as_str());
        let mut prompt_mutation = prompt_mutation_for_anomaly(anomaly, action);
        let mut context_mutation_plan = context_mutation_for_anomaly(anomaly, action);

        match anomaly {
            ProviderTurnAnomaly::AuthInvalid | ProviderTurnAnomaly::AuthExpired => {
                if let Some(hash) = credential_ref_hash.as_ref() {
                    if !self.refreshed_credential_hashes.insert(hash.clone()) {
                        action = ProviderTurnRecoveryAction::FailoverProvider;
                        reason_code.push_str(".credential_refresh_exhausted");
                        prompt_mutation = None;
                        context_mutation_plan = Some("provider_failover_policy_only".to_owned());
                    }
                }
            }
            ProviderTurnAnomaly::UnicodeSurrogate => {
                if self.unicode_retry_used {
                    action = ProviderTurnRecoveryAction::FailDeterministic;
                    reason_code.push_str(".unicode_retry_exhausted");
                    prompt_mutation = None;
                    context_mutation_plan = None;
                } else {
                    self.unicode_retry_used = true;
                }
            }
            ProviderTurnAnomaly::ContextOverflow => {
                if self.compaction_cooldown_active() {
                    action = ProviderTurnRecoveryAction::FailDeterministic;
                    reason_code.push_str(".compaction_cooldown");
                    prompt_mutation = None;
                    context_mutation_plan =
                        Some("compression_exhausted_operator_status".to_owned());
                } else if matches!(
                    input.context_pressure.as_ref().map(|report| report.dominant_source),
                    Some(ContextPressureSource::OutputCap)
                ) {
                    action = ProviderTurnRecoveryAction::LowerReasoningEffort;
                    reason_code.push_str(".output_cap");
                    context_mutation_plan =
                        Some("lower_max_output_tokens_or_reasoning_effort".to_owned());
                }
            }
            ProviderTurnAnomaly::MaxOutputTokensTooLarge => {
                action = ProviderTurnRecoveryAction::LowerReasoningEffort;
                context_mutation_plan = Some("lower_max_output_tokens".to_owned());
            }
            _ => {}
        }

        ProviderTurnRecoveryDecision {
            schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            event_type: PROVIDER_TURN_RECOVERY_EVENT.to_owned(),
            anomaly,
            action,
            reason_code,
            attempt,
            exhausted: matches!(action, ProviderTurnRecoveryAction::FailDeterministic)
                || exhausted
                    && !matches!(
                        anomaly,
                        ProviderTurnAnomaly::AuthInvalid | ProviderTurnAnomaly::AuthExpired
                    ),
            prompt_mutation,
            context_mutation_plan,
            user_visible_status_policy: user_visible_status_policy(action),
            retry_after_ms: input.retry_after_ms,
            credential_ref_hash,
        }
    }

    fn record_attempt(&mut self, anomaly: ProviderTurnAnomaly) -> u8 {
        let attempt = self.attempts_by_anomaly.entry(anomaly).or_default();
        *attempt = attempt.saturating_add(1);
        *attempt
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactionAttemptSummary {
    before_tokens: u64,
    after_tokens: u64,
    token_savings: u64,
}

/// Dominant cause of context pressure before a provider call.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextPressureSource {
    Transcript,
    ToolSchemas,
    Attachments,
    Memory,
    OutputCap,
    Balanced,
}

impl ContextPressureSource {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Transcript => "transcript",
            Self::ToolSchemas => "tool_schemas",
            Self::Attachments => "attachments",
            Self::Memory => "memory",
            Self::OutputCap => "output_cap",
            Self::Balanced => "balanced",
        }
    }
}

/// Redacted pressure report emitted before a provider call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextPressureReport {
    pub(crate) schema_version: u16,
    pub(crate) event_type: String,
    pub(crate) prompt_tokens_estimate: u64,
    pub(crate) tool_schema_bytes: usize,
    pub(crate) compact_catalog_savings_bytes: usize,
    pub(crate) memory_segment_tokens: u64,
    pub(crate) attachment_count: usize,
    pub(crate) session_tail_tokens: u64,
    pub(crate) max_output_tokens: Option<u64>,
    pub(crate) dominant_source: ContextPressureSource,
    pub(crate) compaction_cooldown_active: bool,
}

/// Inputs for building an aggregate context-pressure report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContextPressureInput {
    pub(crate) prompt_tokens_estimate: u64,
    pub(crate) tool_schema_bytes: usize,
    pub(crate) compact_catalog_savings_bytes: usize,
    pub(crate) memory_segment_tokens: u64,
    pub(crate) attachment_count: usize,
    pub(crate) session_tail_tokens: u64,
    pub(crate) max_output_tokens: Option<u64>,
    pub(crate) compaction_cooldown_active: bool,
}

impl ContextPressureReport {
    #[must_use]
    pub(crate) fn new(input: ContextPressureInput) -> Self {
        let dominant_source = dominant_context_pressure_source(
            input.session_tail_tokens,
            input.tool_schema_bytes,
            input.memory_segment_tokens,
            input.attachment_count,
            input.max_output_tokens,
        );
        Self {
            schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            event_type: PROVIDER_CONTEXT_PRESSURE_EVENT.to_owned(),
            prompt_tokens_estimate: input.prompt_tokens_estimate,
            tool_schema_bytes: input.tool_schema_bytes,
            compact_catalog_savings_bytes: input.compact_catalog_savings_bytes,
            memory_segment_tokens: input.memory_segment_tokens,
            attachment_count: input.attachment_count,
            session_tail_tokens: input.session_tail_tokens,
            max_output_tokens: input.max_output_tokens,
            dominant_source,
            compaction_cooldown_active: input.compaction_cooldown_active,
        }
    }

    #[must_use]
    pub(crate) fn tape_payload(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "event": self.event_type,
            "redaction_level": "bounded_aggregate_counts",
            "prompt_tokens_estimate": self.prompt_tokens_estimate,
            "tool_schema_bytes": self.tool_schema_bytes,
            "compact_catalog_savings_bytes": self.compact_catalog_savings_bytes,
            "memory_segment_tokens": self.memory_segment_tokens,
            "attachment_count": self.attachment_count,
            "session_tail_tokens": self.session_tail_tokens,
            "max_output_tokens": self.max_output_tokens,
            "dominant_source": self.dominant_source.as_str(),
            "compaction_cooldown_active": self.compaction_cooldown_active,
        })
    }
}

/// Phase used to close and audit cancellations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderCancellationPhase {
    BeforeExecution,
    DuringProvider,
    ToolQueued,
    ToolRunning,
    Draining,
    Completed,
    ResultSuppressed,
    SideEffectUncertain,
}

impl ProviderCancellationPhase {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeExecution => "before_execution",
            Self::DuringProvider => "during_provider",
            Self::ToolQueued => "tool_queued",
            Self::ToolRunning => "tool_running",
            Self::Draining => "draining",
            Self::Completed => "completed",
            Self::ResultSuppressed => "result_suppressed",
            Self::SideEffectUncertain => "side_effect_uncertain",
        }
    }
}

/// Tape-ready cancellation closure classification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProviderCancellationClosure {
    pub(crate) schema_version: u16,
    pub(crate) event_type: String,
    pub(crate) phase: ProviderCancellationPhase,
    pub(crate) side_effects_possible: bool,
    pub(crate) provider_tail_closed: bool,
    pub(crate) reason_code: String,
}

impl ProviderCancellationClosure {
    #[must_use]
    pub(crate) fn tape_payload(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "event": self.event_type,
            "redaction_level": "no_payload",
            "phase": self.phase.as_str(),
            "side_effects_possible": self.side_effects_possible,
            "provider_tail_closed": self.provider_tail_closed,
            "reason_code": self.reason_code,
        })
    }
}

#[must_use]
pub(crate) fn cancellation_closure(
    phase: ProviderCancellationPhase,
) -> ProviderCancellationClosure {
    let side_effects_possible = matches!(
        phase,
        ProviderCancellationPhase::ToolRunning
            | ProviderCancellationPhase::Draining
            | ProviderCancellationPhase::SideEffectUncertain
    );
    let provider_tail_closed = !matches!(
        phase,
        ProviderCancellationPhase::DuringProvider | ProviderCancellationPhase::ToolQueued
    );
    ProviderCancellationClosure {
        schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
        event_type: PROVIDER_CANCELLATION_CLOSURE_EVENT.to_owned(),
        phase,
        side_effects_possible,
        provider_tail_closed,
        reason_code: format!("provider.cancellation.{}", phase.as_str()),
    }
}

#[must_use]
pub(crate) fn anomaly_from_terminal_outcome(
    outcome: &TerminalOutcomeClassification,
) -> Option<ProviderTurnAnomaly> {
    match (outcome.class, outcome.finish_reason) {
        (_, Some(ProviderFinishReason::ContentFilter)) => {
            Some(ProviderTurnAnomaly::ContentPolicyBlocked)
        }
        (_, Some(ProviderFinishReason::Length)) if outcome.continues_tool_execution => {
            Some(ProviderTurnAnomaly::LengthToolArguments)
        }
        (_, Some(ProviderFinishReason::Length)) => Some(ProviderTurnAnomaly::LengthFinalText),
        (TerminalOutcomeClass::ReasoningOnly, _) | (TerminalOutcomeClass::PlanningOnly, _) => {
            Some(ProviderTurnAnomaly::ReasoningOnly)
        }
        (TerminalOutcomeClass::Empty, _) => Some(ProviderTurnAnomaly::EmptyFinalAnswer),
        (TerminalOutcomeClass::ProtocolError, _) => {
            Some(ProviderTurnAnomaly::MalformedToolSequence)
        }
        (
            TerminalOutcomeClass::VisibleText
            | TerminalOutcomeClass::ToolOnly
            | TerminalOutcomeClass::IntentionalSilent
            | TerminalOutcomeClass::ProviderError,
            _,
        ) => None,
    }
}

#[must_use]
#[allow(dead_code)]
pub(crate) fn anomaly_from_provider_failure_class(
    failure_class: ProviderFailureClass,
) -> ProviderTurnAnomaly {
    match failure_class {
        ProviderFailureClass::AuthInvalid => ProviderTurnAnomaly::AuthInvalid,
        ProviderFailureClass::AuthExpired => ProviderTurnAnomaly::AuthExpired,
        ProviderFailureClass::PermissionDenied => ProviderTurnAnomaly::PermissionDenied,
        ProviderFailureClass::RateLimit | ProviderFailureClass::RateLimited => {
            ProviderTurnAnomaly::RateLimit
        }
        ProviderFailureClass::BadToolArguments => ProviderTurnAnomaly::MalformedJsonArguments,
        ProviderFailureClass::TruncatedToolArguments => ProviderTurnAnomaly::TruncatedToolArguments,
        ProviderFailureClass::ContextOverflow | ProviderFailureClass::ContextWindowExceeded => {
            ProviderTurnAnomaly::ContextOverflow
        }
        ProviderFailureClass::ContentPolicyBlocked => ProviderTurnAnomaly::ContentPolicyBlocked,
        ProviderFailureClass::MalformedStream => ProviderTurnAnomaly::MalformedStream,
        ProviderFailureClass::EmptyOutput => ProviderTurnAnomaly::EmptyFinalAnswer,
        ProviderFailureClass::PrematureFinal => ProviderTurnAnomaly::PartialContentStream,
        ProviderFailureClass::PayloadTooLarge => ProviderTurnAnomaly::MaxOutputTokensTooLarge,
        ProviderFailureClass::ProviderTimeout => ProviderTurnAnomaly::ProviderTimeout,
        ProviderFailureClass::SchemaRejected
        | ProviderFailureClass::MalformedResponse
        | ProviderFailureClass::TransientUpstream
        | ProviderFailureClass::PermanentUpstream
        | ProviderFailureClass::ProviderUnavailable
        | ProviderFailureClass::NetworkUnavailable
        | ProviderFailureClass::Quota
        | ProviderFailureClass::QuotaExceeded => ProviderTurnAnomaly::MalformedStream,
    }
}

fn retry_budget(anomaly: ProviderTurnAnomaly) -> u8 {
    match anomaly {
        ProviderTurnAnomaly::LengthFinalText => DEFAULT_LENGTH_RETRY_BUDGET,
        ProviderTurnAnomaly::ProviderTimeout
        | ProviderTurnAnomaly::BrowserFollowupTimeout
        | ProviderTurnAnomaly::ToolFollowupTimeout => DEFAULT_TIMEOUT_RETRY_BUDGET,
        ProviderTurnAnomaly::ContextOverflow
        | ProviderTurnAnomaly::ReasoningOnly
        | ProviderTurnAnomaly::EmptyFinalAnswer
        | ProviderTurnAnomaly::EmptyPostToolResponse
        | ProviderTurnAnomaly::MalformedJsonArguments
        | ProviderTurnAnomaly::DroppedToolCall
        | ProviderTurnAnomaly::PartialContentStream
        | ProviderTurnAnomaly::MalformedStream
        | ProviderTurnAnomaly::MultimodalUnsupported
        | ProviderTurnAnomaly::InvalidEncryptedContent
        | ProviderTurnAnomaly::RateLimit
        | ProviderTurnAnomaly::UnicodeSurrogate => DEFAULT_SINGLE_RETRY_BUDGET,
        _ => 0,
    }
}

fn recovery_action_for_anomaly(
    anomaly: ProviderTurnAnomaly,
    exhausted: bool,
) -> ProviderTurnRecoveryAction {
    if exhausted
        && !matches!(anomaly, ProviderTurnAnomaly::AuthInvalid | ProviderTurnAnomaly::AuthExpired)
    {
        return ProviderTurnRecoveryAction::FailDeterministic;
    }
    match anomaly {
        ProviderTurnAnomaly::ProviderTimeout
        | ProviderTurnAnomaly::BrowserFollowupTimeout
        | ProviderTurnAnomaly::ToolFollowupTimeout
        | ProviderTurnAnomaly::MalformedStream
        | ProviderTurnAnomaly::UnicodeSurrogate => ProviderTurnRecoveryAction::RetrySameProvider,
        ProviderTurnAnomaly::LengthFinalText
        | ProviderTurnAnomaly::ReasoningOnly
        | ProviderTurnAnomaly::EmptyFinalAnswer
        | ProviderTurnAnomaly::EmptyPostToolResponse
        | ProviderTurnAnomaly::DroppedToolCall
        | ProviderTurnAnomaly::PartialContentStream
        | ProviderTurnAnomaly::InvalidToolName
        | ProviderTurnAnomaly::MalformedJsonArguments => {
            ProviderTurnRecoveryAction::RetryWithPrompt
        }
        ProviderTurnAnomaly::ContextOverflow => ProviderTurnRecoveryAction::CompactAndRetry,
        ProviderTurnAnomaly::MultimodalUnsupported => ProviderTurnRecoveryAction::ShrinkMultimodal,
        ProviderTurnAnomaly::InvalidEncryptedContent => {
            ProviderTurnRecoveryAction::StripUnsupportedContent
        }
        ProviderTurnAnomaly::AuthInvalid | ProviderTurnAnomaly::AuthExpired => {
            ProviderTurnRecoveryAction::RefreshCredential
        }
        ProviderTurnAnomaly::RateLimit => ProviderTurnRecoveryAction::BackoffRetry,
        ProviderTurnAnomaly::PermissionDenied
        | ProviderTurnAnomaly::LengthToolArguments
        | ProviderTurnAnomaly::MaxOutputTokensTooLarge
        | ProviderTurnAnomaly::PartialToolCall
        | ProviderTurnAnomaly::ToolCallsFinishWithoutPayload
        | ProviderTurnAnomaly::MalformedToolSequence
        | ProviderTurnAnomaly::TruncatedToolArguments
        | ProviderTurnAnomaly::ContentPolicyBlocked => {
            ProviderTurnRecoveryAction::FailDeterministic
        }
    }
}

fn prompt_mutation_for_anomaly(
    anomaly: ProviderTurnAnomaly,
    action: ProviderTurnRecoveryAction,
) -> Option<String> {
    if !matches!(action, ProviderTurnRecoveryAction::RetryWithPrompt) {
        return None;
    }
    match anomaly {
        ProviderTurnAnomaly::LengthFinalText => Some("continue_visible_answer_only".to_owned()),
        ProviderTurnAnomaly::ReasoningOnly => {
            Some("return_visible_answer_without_reasoning".to_owned())
        }
        ProviderTurnAnomaly::EmptyFinalAnswer | ProviderTurnAnomaly::EmptyPostToolResponse => {
            Some("provide_concise_visible_final_answer".to_owned())
        }
        ProviderTurnAnomaly::DroppedToolCall | ProviderTurnAnomaly::PartialContentStream => {
            Some("repeat_structured_tool_proposal_or_final_answer".to_owned())
        }
        ProviderTurnAnomaly::InvalidToolName => {
            Some("select_tool_name_from_visible_catalog".to_owned())
        }
        ProviderTurnAnomaly::MalformedJsonArguments => {
            Some("retry_tool_arguments_as_valid_json_object".to_owned())
        }
        _ => None,
    }
}

fn context_mutation_for_anomaly(
    anomaly: ProviderTurnAnomaly,
    action: ProviderTurnRecoveryAction,
) -> Option<String> {
    match action {
        ProviderTurnRecoveryAction::CompactAndRetry => Some("compact_session_context".to_owned()),
        ProviderTurnRecoveryAction::LowerReasoningEffort => Some("lower_output_budget".to_owned()),
        ProviderTurnRecoveryAction::ShrinkMultimodal => {
            Some("shrink_or_drop_oversized_images".to_owned())
        }
        ProviderTurnRecoveryAction::StripUnsupportedContent => {
            Some("strip_unsupported_provider_content".to_owned())
        }
        ProviderTurnRecoveryAction::SyntheticToolResult
            if matches!(anomaly, ProviderTurnAnomaly::MalformedJsonArguments) =>
        {
            Some("insert_synthetic_schema_guidance_tool_result".to_owned())
        }
        _ => None,
    }
}

fn user_visible_status_policy(action: ProviderTurnRecoveryAction) -> UserVisibleStatusPolicy {
    match action {
        ProviderTurnRecoveryAction::FailDeterministic => UserVisibleStatusPolicy::SafeMessage,
        ProviderTurnRecoveryAction::BackoffRetry
        | ProviderTurnRecoveryAction::CompactAndRetry
        | ProviderTurnRecoveryAction::FailoverProvider => UserVisibleStatusPolicy::StatusOnly,
        _ => UserVisibleStatusPolicy::Silent,
    }
}

fn dominant_context_pressure_source(
    session_tail_tokens: u64,
    tool_schema_bytes: usize,
    memory_segment_tokens: u64,
    attachment_count: usize,
    max_output_tokens: Option<u64>,
) -> ContextPressureSource {
    let tool_tokens = u64::try_from(tool_schema_bytes / 4).unwrap_or(u64::MAX);
    let output_tokens = max_output_tokens.unwrap_or_default();
    let attachment_tokens =
        u64::try_from(attachment_count).unwrap_or(u64::MAX).saturating_mul(1_024);
    let mut sources = [
        (ContextPressureSource::Transcript, session_tail_tokens),
        (ContextPressureSource::ToolSchemas, tool_tokens),
        (ContextPressureSource::Memory, memory_segment_tokens),
        (ContextPressureSource::Attachments, attachment_tokens),
        (ContextPressureSource::OutputCap, output_tokens),
    ];
    sources.sort_by(|left, right| {
        right.1.cmp(&left.1).then_with(|| left.0.as_str().cmp(right.0.as_str()))
    });
    let (source, highest) = sources[0];
    let second = sources.get(1).map_or(0, |(_, value)| *value);
    if highest == 0 || highest <= second.saturating_add(second / 10) {
        ContextPressureSource::Balanced
    } else {
        source
    }
}

fn short_hash(bytes: &[u8]) -> String {
    let hash = sha256_hex(bytes);
    hash.chars().take(16).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decide_once(anomaly: ProviderTurnAnomaly) -> ProviderTurnRecoveryDecision {
        ProviderTurnRecoveryState::new().decide(anomaly, ProviderTurnRecoveryInput::default())
    }

    #[test]
    fn recovery_state_matrix_covers_core_anomalies() {
        let cases = [
            (ProviderTurnAnomaly::ProviderTimeout, ProviderTurnRecoveryAction::RetrySameProvider),
            (ProviderTurnAnomaly::ReasoningOnly, ProviderTurnRecoveryAction::RetryWithPrompt),
            (
                ProviderTurnAnomaly::EmptyPostToolResponse,
                ProviderTurnRecoveryAction::RetryWithPrompt,
            ),
            (ProviderTurnAnomaly::LengthFinalText, ProviderTurnRecoveryAction::RetryWithPrompt),
            (
                ProviderTurnAnomaly::ContentPolicyBlocked,
                ProviderTurnRecoveryAction::FailDeterministic,
            ),
            (
                ProviderTurnAnomaly::TruncatedToolArguments,
                ProviderTurnRecoveryAction::FailDeterministic,
            ),
            (ProviderTurnAnomaly::ContextOverflow, ProviderTurnRecoveryAction::CompactAndRetry),
            (
                ProviderTurnAnomaly::MultimodalUnsupported,
                ProviderTurnRecoveryAction::ShrinkMultimodal,
            ),
            (ProviderTurnAnomaly::RateLimit, ProviderTurnRecoveryAction::BackoffRetry),
        ];

        for (anomaly, expected_action) in cases {
            let decision = decide_once(anomaly);
            assert_eq!(decision.action, expected_action, "{anomaly:?}");
            assert!(decision.reason_code.contains(anomaly.as_str()));
        }
    }

    #[test]
    fn retry_budget_blocks_reasoning_only_loops() {
        let mut state = ProviderTurnRecoveryState::new();
        let first =
            state.decide(ProviderTurnAnomaly::ReasoningOnly, ProviderTurnRecoveryInput::default());
        let second =
            state.decide(ProviderTurnAnomaly::ReasoningOnly, ProviderTurnRecoveryInput::default());

        assert_eq!(first.action, ProviderTurnRecoveryAction::RetryWithPrompt);
        assert_eq!(second.action, ProviderTurnRecoveryAction::FailDeterministic);
        assert!(second.exhausted);
    }

    #[test]
    fn length_recovery_has_larger_budget() {
        let mut state = ProviderTurnRecoveryState::new();

        for _ in 0..DEFAULT_LENGTH_RETRY_BUDGET {
            let decision = state
                .decide(ProviderTurnAnomaly::LengthFinalText, ProviderTurnRecoveryInput::default());
            assert_eq!(decision.action, ProviderTurnRecoveryAction::RetryWithPrompt);
        }
        let exhausted = state
            .decide(ProviderTurnAnomaly::LengthFinalText, ProviderTurnRecoveryInput::default());

        assert_eq!(exhausted.action, ProviderTurnRecoveryAction::FailDeterministic);
    }

    #[test]
    fn content_filter_is_terminal_without_retry() {
        let decision = decide_once(ProviderTurnAnomaly::ContentPolicyBlocked);

        assert_eq!(decision.action, ProviderTurnRecoveryAction::FailDeterministic);
        assert!(decision.exhausted);
        assert_eq!(decision.user_visible_status_policy, UserVisibleStatusPolicy::SafeMessage);
    }

    #[test]
    fn credential_refresh_is_capped_per_credential() {
        let mut state = ProviderTurnRecoveryState::new();
        let input = ProviderTurnRecoveryInput {
            credential_id: Some("credential-a".to_owned()),
            ..ProviderTurnRecoveryInput::default()
        };

        let first = state.decide(ProviderTurnAnomaly::AuthExpired, input.clone());
        let second = state.decide(ProviderTurnAnomaly::AuthExpired, input);

        assert_eq!(first.action, ProviderTurnRecoveryAction::RefreshCredential);
        assert_eq!(second.action, ProviderTurnRecoveryAction::FailoverProvider);
        assert!(second.reason_code.ends_with("credential_refresh_exhausted"));
        assert_eq!(first.credential_ref_hash, second.credential_ref_hash);
    }

    #[test]
    fn unicode_retry_is_single_use() {
        let mut state = ProviderTurnRecoveryState::new();

        let first = state
            .decide(ProviderTurnAnomaly::UnicodeSurrogate, ProviderTurnRecoveryInput::default());
        let second = state
            .decide(ProviderTurnAnomaly::UnicodeSurrogate, ProviderTurnRecoveryInput::default());

        assert_eq!(first.action, ProviderTurnRecoveryAction::RetrySameProvider);
        assert_eq!(second.action, ProviderTurnRecoveryAction::FailDeterministic);
    }

    #[test]
    fn output_cap_is_not_compacted_as_context_overflow() {
        let pressure = ContextPressureReport::new(ContextPressureInput {
            prompt_tokens_estimate: 100_000,
            tool_schema_bytes: 1024,
            compact_catalog_savings_bytes: 0,
            memory_segment_tokens: 100,
            attachment_count: 0,
            session_tail_tokens: 1_000,
            max_output_tokens: Some(64_000),
            compaction_cooldown_active: false,
        });
        let mut state = ProviderTurnRecoveryState::new();

        let decision = state.decide(
            ProviderTurnAnomaly::ContextOverflow,
            ProviderTurnRecoveryInput {
                context_pressure: Some(pressure),
                ..ProviderTurnRecoveryInput::default()
            },
        );

        assert_eq!(decision.action, ProviderTurnRecoveryAction::LowerReasoningEffort);
        assert!(decision.reason_code.ends_with(".output_cap"));
    }

    #[test]
    fn compaction_cooldown_blocks_repeated_unhelpful_compression() {
        let mut state = ProviderTurnRecoveryState::new();
        state.record_compaction_attempt(10_000, 9_900);

        let decision = state
            .decide(ProviderTurnAnomaly::ContextOverflow, ProviderTurnRecoveryInput::default());

        assert_eq!(decision.action, ProviderTurnRecoveryAction::FailDeterministic);
        assert!(decision.reason_code.ends_with(".compaction_cooldown"));
    }

    #[test]
    fn pressure_report_serializes_aggregate_counts_only() {
        let report = ContextPressureReport::new(ContextPressureInput {
            prompt_tokens_estimate: 12_000,
            tool_schema_bytes: 20_000,
            compact_catalog_savings_bytes: 10_000,
            memory_segment_tokens: 200,
            attachment_count: 2,
            session_tail_tokens: 500,
            max_output_tokens: Some(1_000),
            compaction_cooldown_active: false,
        });
        let payload = report.tape_payload();

        assert_eq!(payload["event"], PROVIDER_CONTEXT_PRESSURE_EVENT);
        assert_eq!(payload["redaction_level"], "bounded_aggregate_counts");
        assert_eq!(payload["dominant_source"], "tool_schemas");
    }

    #[test]
    fn cancellation_closure_classifies_uncertain_side_effects() {
        let closure = cancellation_closure(ProviderCancellationPhase::SideEffectUncertain);

        assert!(closure.side_effects_possible);
        assert!(closure.provider_tail_closed);
        assert_eq!(closure.reason_code, "provider.cancellation.side_effect_uncertain");
    }
}
