//! Provider-attempt planning, bounded recovery, and redacted diagnostics.
//!
//! The state machine owns attempt identity, aggregate usage, retry budgets, and
//! action selection. Side effects still pass through narrow host commands so a
//! recovery decision cannot silently acquire broader network or tool authority.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    model_provider::{
        ProviderFailureClass, ProviderFinishReason, ProviderRequest, ProviderResponse,
        ProviderTerminalValidationOutcome, TerminalOutcomeClass, TerminalOutcomeClassification,
    },
    sha256_hex,
};

pub(crate) const PROVIDER_TURN_RECOVERY_SCHEMA_VERSION: u16 = 1;
pub(crate) const PROVIDER_TURN_RECOVERY_EVENT: &str = "provider.turn_recovery.decision";
pub(crate) const PROVIDER_ATTEMPT_PLAN_EVENT: &str = "provider.attempt.plan";
pub(crate) const PROVIDER_ATTEMPT_OUTCOME_EVENT: &str = "provider.attempt.outcome";
pub(crate) const RECOVERY_ACTION_STARTED_EVENT: &str = "recovery.action.started";
pub(crate) const RECOVERY_ACTION_COMPLETED_EVENT: &str = "recovery.action.completed";
pub(crate) const RECOVERY_ACTION_FAILED_EVENT: &str = "recovery.action.failed";
pub(crate) const RECOVERY_ACTION_BLOCKED_EVENT: &str = "recovery.action.blocked";
pub(crate) const PROVIDER_CONTEXT_PRESSURE_EVENT: &str = "provider.context_pressure";
pub(crate) const PROVIDER_CANCELLATION_CLOSURE_EVENT: &str = "provider.cancellation_closure";

const DEFAULT_GLOBAL_RECOVERY_BUDGET: u8 = 6;
const DEFAULT_SINGLE_RETRY_BUDGET: u8 = 1;
const DEFAULT_LENGTH_RETRY_BUDGET: u8 = 3;
const DEFAULT_TIMEOUT_RETRY_BUDGET: u8 = 1;
const DEFAULT_MULTIMODAL_RETRY_BUDGET: u8 = 3;
const DEFAULT_BACKOFF_MS: u64 = 250;
pub(crate) const MAX_RECOVERY_BACKOFF_MS: u64 = 2_000;
const MIN_COMPACTION_TOKEN_SAVINGS: u64 = 512;
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "the startup recovery owner consumes this durable boundary")
)]
const PROVIDER_ATTEMPT_CHECKPOINT_SCHEMA_VERSION: u16 = 1;

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

/// Side-effect posture at the point where a retry is considered.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderRecoverySideEffectState {
    None,
    ConfirmedWithReconciliation,
    Uncertain,
}

impl ProviderRecoverySideEffectState {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ConfirmedWithReconciliation => "confirmed_with_reconciliation",
            Self::Uncertain => "uncertain",
        }
    }
}

/// Redacted structural difference from the immutable original request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RedactedProviderRequestDiff {
    changed_fields: Vec<String>,
    original_message_count: usize,
    current_message_count: usize,
    original_vision_input_count: usize,
    current_vision_input_count: usize,
    original_input_bytes: usize,
    current_input_bytes: usize,
    original_model_ref_sha256: Option<String>,
    current_model_ref_sha256: Option<String>,
}

/// Immutable plan recorded before one provider side effect starts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProviderAttemptPlan {
    pub(crate) schema_version: u16,
    pub(crate) event_type: String,
    pub(crate) attempt_index: u16,
    pub(crate) plan_id: String,
    pub(crate) original_request_digest_sha256: String,
    pub(crate) request_digest_sha256: String,
    pub(crate) request_diff: RedactedProviderRequestDiff,
    pub(crate) provider_ref_sha256: String,
    pub(crate) credential_ref_sha256: String,
    pub(crate) model_ref_sha256: String,
    pub(crate) route_class: String,
    pub(crate) network_authority_sha256: String,
    pub(crate) tool_authority_sha256: String,
}

impl ProviderAttemptPlan {
    #[must_use]
    pub(crate) fn tape_payload(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "event": self.event_type,
            "redaction_level": "hashes_and_structural_diff",
            "attempt_index": self.attempt_index,
            "plan_id": self.plan_id,
            "original_request_digest_sha256": self.original_request_digest_sha256,
            "request_digest_sha256": self.request_digest_sha256,
            "request_diff": self.request_diff,
            "provider_ref_sha256": self.provider_ref_sha256,
            "credential_ref_sha256": self.credential_ref_sha256,
            "model_ref_sha256": self.model_ref_sha256,
            "route_class": self.route_class,
            "network_authority_sha256": self.network_authority_sha256,
            "tool_authority_sha256": self.tool_authority_sha256,
        })
    }
}

/// Result of one transport attempt, including all provider-internal candidates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProviderAttemptOutcome {
    pub(crate) schema_version: u16,
    pub(crate) event_type: String,
    pub(crate) attempt_index: u16,
    pub(crate) plan_id: String,
    pub(crate) provider_ref_sha256: String,
    pub(crate) credential_ref_sha256: String,
    pub(crate) model_ref_sha256: String,
    pub(crate) route_class: String,
    pub(crate) disposition: String,
    pub(crate) reason_code: String,
    pub(crate) candidate_attempt_count: u16,
    pub(crate) prompt_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) cache_tokens: u64,
    pub(crate) estimated_cost_microusd: u64,
    pub(crate) aggregate_prompt_tokens: u64,
    pub(crate) aggregate_output_tokens: u64,
    pub(crate) aggregate_cache_tokens: u64,
    pub(crate) aggregate_estimated_cost_microusd: u64,
}

impl ProviderAttemptOutcome {
    #[must_use]
    pub(crate) fn tape_payload(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "event": self.event_type,
            "redaction_level": "aggregate_usage_and_reason_code",
            "attempt_index": self.attempt_index,
            "plan_id": self.plan_id,
            "provider_ref_sha256": self.provider_ref_sha256,
            "credential_ref_sha256": self.credential_ref_sha256,
            "model_ref_sha256": self.model_ref_sha256,
            "route_class": self.route_class,
            "disposition": self.disposition,
            "reason_code": self.reason_code,
            "candidate_attempt_count": self.candidate_attempt_count,
            "usage": {
                "prompt_tokens": self.prompt_tokens,
                "output_tokens": self.output_tokens,
                "cache_tokens": self.cache_tokens,
                "estimated_cost_microusd": self.estimated_cost_microusd,
            },
            "aggregate_usage": {
                "prompt_tokens": self.aggregate_prompt_tokens,
                "output_tokens": self.aggregate_output_tokens,
                "cache_tokens": self.aggregate_cache_tokens,
                "estimated_cost_microusd": self.aggregate_estimated_cost_microusd,
            },
        })
    }
}

/// Narrow host command produced by the recovery executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProviderRecoveryCommand {
    RetryCurrentRequest,
    AppendGuidance { guidance: String },
    RecoverContext,
    LowerOutputBudget,
    DropVisionInputs,
    StripUnsupportedContent,
    RefreshCredential,
    SelectFallbackRoute,
    Backoff { delay_ms: u64 },
    FailDeterministic,
}

impl ProviderRecoveryCommand {
    #[must_use]
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::RetryCurrentRequest => "retry_current_request",
            Self::AppendGuidance { .. } => "append_guidance",
            Self::RecoverContext => "recover_context",
            Self::LowerOutputBudget => "lower_output_budget",
            Self::DropVisionInputs => "drop_vision_inputs",
            Self::StripUnsupportedContent => "strip_unsupported_content",
            Self::RefreshCredential => "refresh_credential",
            Self::SelectFallbackRoute => "select_fallback_route",
            Self::Backoff { .. } => "backoff",
            Self::FailDeterministic => "fail_deterministic",
        }
    }
}

/// Result vocabulary for every recovery action.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryActionDisposition {
    Completed,
    Failed,
    Blocked,
    Unsupported,
}

impl RecoveryActionDisposition {
    #[must_use]
    const fn event_type(self) -> &'static str {
        match self {
            Self::Completed => RECOVERY_ACTION_COMPLETED_EVENT,
            Self::Failed => RECOVERY_ACTION_FAILED_EVENT,
            Self::Blocked | Self::Unsupported => RECOVERY_ACTION_BLOCKED_EVENT,
        }
    }

    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Durable terminal record for one recovery action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RecoveryActionOutcome {
    pub(crate) schema_version: u16,
    pub(crate) event_type: String,
    pub(crate) plan_id: String,
    pub(crate) attempt_index: u16,
    pub(crate) anomaly: ProviderTurnAnomaly,
    pub(crate) action: ProviderTurnRecoveryAction,
    pub(crate) command: String,
    pub(crate) disposition: RecoveryActionDisposition,
    pub(crate) reason_code: String,
    pub(crate) original_request_digest_sha256: String,
    pub(crate) request_digest_sha256: String,
    pub(crate) request_diff: RedactedProviderRequestDiff,
    pub(crate) side_effect_state: ProviderRecoverySideEffectState,
    pub(crate) partial_user_visible_output: bool,
    pub(crate) network_authority_sha256: String,
    pub(crate) tool_authority_sha256: String,
}

impl RecoveryActionOutcome {
    #[must_use]
    pub(crate) fn tape_payload(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "event": self.event_type,
            "redaction_level": "hashes_structural_diff_and_reason_code",
            "plan_id": self.plan_id,
            "attempt_index": self.attempt_index,
            "anomaly": self.anomaly.as_str(),
            "action": self.action.as_str(),
            "command": self.command,
            "disposition": self.disposition.as_str(),
            "reason_code": self.reason_code,
            "original_request_digest_sha256": self.original_request_digest_sha256,
            "request_digest_sha256": self.request_digest_sha256,
            "request_diff": self.request_diff,
            "side_effect_state": self.side_effect_state.as_str(),
            "partial_user_visible_output": self.partial_user_visible_output,
            "network_authority_sha256": self.network_authority_sha256,
            "tool_authority_sha256": self.tool_authority_sha256,
        })
    }
}

/// Runtime facts required to enforce retry safety and build typed guidance.
#[derive(Debug, Clone)]
pub(crate) struct RecoveryExecutorInput {
    pub(crate) issue_summary: String,
    pub(crate) completed_tool_calls: u32,
    pub(crate) side_effect_state: ProviderRecoverySideEffectState,
    pub(crate) partial_user_visible_output: bool,
    pub(crate) summary_only_closeout: bool,
}

impl Default for RecoveryExecutorInput {
    fn default() -> Self {
        Self {
            issue_summary: String::new(),
            completed_tool_calls: 0,
            side_effect_state: ProviderRecoverySideEffectState::None,
            partial_user_visible_output: false,
            summary_only_closeout: false,
        }
    }
}

/// Started record plus the command a host must apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedRecoveryAction {
    pub(crate) plan: ProviderAttemptPlan,
    pub(crate) decision: ProviderTurnRecoveryDecision,
    pub(crate) command: Option<ProviderRecoveryCommand>,
    pub(crate) immediate_outcome: Option<RecoveryActionOutcome>,
    side_effect_state: ProviderRecoverySideEffectState,
    partial_user_visible_output: bool,
}

impl PreparedRecoveryAction {
    #[must_use]
    pub(crate) fn started_payload(&self) -> Value {
        json!({
            "schema_version": PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            "event": RECOVERY_ACTION_STARTED_EVENT,
            "redaction_level": "hashes_and_reason_code",
            "plan_id": self.plan.plan_id,
            "attempt_index": self.plan.attempt_index,
            "anomaly": self.decision.anomaly.as_str(),
            "action": self.decision.action.as_str(),
            "command": self.command.as_ref().map(ProviderRecoveryCommand::as_str),
            "reason_code": self.decision.reason_code,
            "original_request_digest_sha256": self.plan.original_request_digest_sha256,
            "request_digest_sha256": self.plan.request_digest_sha256,
            "network_authority_sha256": self.plan.network_authority_sha256,
            "tool_authority_sha256": self.plan.tool_authority_sha256,
        })
    }

    #[must_use]
    pub(crate) fn completed(self, reason_code: impl Into<String>) -> RecoveryActionOutcome {
        self.outcome(RecoveryActionDisposition::Completed, reason_code.into())
    }

    #[must_use]
    pub(crate) fn failed(self, reason_code: impl Into<String>) -> RecoveryActionOutcome {
        self.outcome(RecoveryActionDisposition::Failed, reason_code.into())
    }

    #[must_use]
    pub(crate) fn blocked(self, reason_code: impl Into<String>) -> RecoveryActionOutcome {
        self.outcome(RecoveryActionDisposition::Blocked, reason_code.into())
    }

    #[must_use]
    pub(crate) fn unsupported(self, reason_code: impl Into<String>) -> RecoveryActionOutcome {
        self.outcome(RecoveryActionDisposition::Unsupported, reason_code.into())
    }

    #[must_use]
    fn outcome(
        self,
        disposition: RecoveryActionDisposition,
        reason_code: String,
    ) -> RecoveryActionOutcome {
        let command =
            self.command.as_ref().map_or("none", ProviderRecoveryCommand::as_str).to_owned();
        RecoveryActionOutcome {
            schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            event_type: disposition.event_type().to_owned(),
            plan_id: self.plan.plan_id,
            attempt_index: self.plan.attempt_index,
            anomaly: self.decision.anomaly,
            action: self.decision.action,
            command,
            disposition,
            reason_code,
            original_request_digest_sha256: self.plan.original_request_digest_sha256,
            request_digest_sha256: self.plan.request_digest_sha256,
            request_diff: self.plan.request_diff,
            side_effect_state: self.side_effect_state,
            partial_user_visible_output: self.partial_user_visible_output,
            network_authority_sha256: self.plan.network_authority_sha256,
            tool_authority_sha256: self.plan.tool_authority_sha256,
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

/// Per-run provider attempt and recovery state machine.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProviderAttemptStateMachine {
    original_request: Option<ProviderRequest>,
    original_request_digest_sha256: String,
    primary_provider_ref_sha256: String,
    network_authority_sha256: String,
    tool_authority_sha256: String,
    next_attempt_index: u16,
    global_recovery_actions: u8,
    attempts_by_anomaly: BTreeMap<ProviderTurnAnomaly, u8>,
    refreshed_credential_hashes: BTreeSet<String>,
    unicode_retry_used: bool,
    last_compaction_attempt: Option<CompactionAttemptSummary>,
    aggregate_prompt_tokens: u64,
    aggregate_output_tokens: u64,
    aggregate_cache_tokens: u64,
    aggregate_estimated_cost_microusd: u64,
}

/// Secret-free recovery state needed to resume bounded attempts after restart.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "the startup recovery owner consumes this durable boundary")
)]
pub(crate) struct ProviderAttemptCheckpoint {
    schema_version: u16,
    original_request_digest_sha256: String,
    primary_provider_ref_sha256: String,
    network_authority_sha256: String,
    tool_authority_sha256: String,
    next_attempt_index: u16,
    global_recovery_actions: u8,
    attempts_by_anomaly: BTreeMap<ProviderTurnAnomaly, u8>,
    refreshed_credential_hashes: BTreeSet<String>,
    unicode_retry_used: bool,
    aggregate_prompt_tokens: u64,
    aggregate_output_tokens: u64,
    aggregate_cache_tokens: u64,
    aggregate_estimated_cost_microusd: u64,
}

impl ProviderAttemptStateMachine {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub(crate) fn for_request(
        request: &ProviderRequest,
        network_authority: &str,
        tool_authority: &str,
    ) -> Self {
        Self {
            original_request: Some(request.clone()),
            original_request_digest_sha256: provider_request_digest(request),
            network_authority_sha256: sha256_hex(network_authority.as_bytes()),
            tool_authority_sha256: sha256_hex(tool_authority.as_bytes()),
            ..Self::default()
        }
    }

    /// Captures only bounded counters and hashes; the original request remains
    /// owned by the run context instead of being copied into recovery state.
    #[must_use]
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "the startup recovery owner consumes this durable boundary")
    )]
    pub(crate) fn checkpoint(&self) -> ProviderAttemptCheckpoint {
        ProviderAttemptCheckpoint {
            schema_version: PROVIDER_ATTEMPT_CHECKPOINT_SCHEMA_VERSION,
            original_request_digest_sha256: self.original_request_digest_sha256.clone(),
            primary_provider_ref_sha256: self.primary_provider_ref_sha256.clone(),
            network_authority_sha256: self.network_authority_sha256.clone(),
            tool_authority_sha256: self.tool_authority_sha256.clone(),
            next_attempt_index: self.next_attempt_index,
            global_recovery_actions: self.global_recovery_actions,
            attempts_by_anomaly: self.attempts_by_anomaly.clone(),
            refreshed_credential_hashes: self.refreshed_credential_hashes.clone(),
            unicode_retry_used: self.unicode_retry_used,
            aggregate_prompt_tokens: self.aggregate_prompt_tokens,
            aggregate_output_tokens: self.aggregate_output_tokens,
            aggregate_cache_tokens: self.aggregate_cache_tokens,
            aggregate_estimated_cost_microusd: self.aggregate_estimated_cost_microusd,
        }
    }

    /// Restores a checkpoint only when the run request and both authority
    /// envelopes still match their sealed hashes.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "the startup recovery owner consumes this durable boundary")
    )]
    pub(crate) fn restore(
        request: &ProviderRequest,
        network_authority: &str,
        tool_authority: &str,
        checkpoint: ProviderAttemptCheckpoint,
    ) -> Result<Self, &'static str> {
        if checkpoint.schema_version != PROVIDER_ATTEMPT_CHECKPOINT_SCHEMA_VERSION {
            return Err("provider.recovery.checkpoint_schema_unsupported");
        }
        let request_digest = provider_request_digest(request);
        if request_digest != checkpoint.original_request_digest_sha256 {
            return Err("provider.recovery.checkpoint_request_mismatch");
        }
        let network_authority_sha256 = sha256_hex(network_authority.as_bytes());
        let tool_authority_sha256 = sha256_hex(tool_authority.as_bytes());
        if network_authority_sha256 != checkpoint.network_authority_sha256
            || tool_authority_sha256 != checkpoint.tool_authority_sha256
        {
            return Err("provider.recovery.checkpoint_authority_mismatch");
        }
        Ok(Self {
            original_request: Some(request.clone()),
            original_request_digest_sha256: request_digest,
            primary_provider_ref_sha256: checkpoint.primary_provider_ref_sha256,
            network_authority_sha256,
            tool_authority_sha256,
            next_attempt_index: checkpoint.next_attempt_index,
            global_recovery_actions: checkpoint.global_recovery_actions,
            attempts_by_anomaly: checkpoint.attempts_by_anomaly,
            refreshed_credential_hashes: checkpoint.refreshed_credential_hashes,
            unicode_retry_used: checkpoint.unicode_retry_used,
            last_compaction_attempt: None,
            aggregate_prompt_tokens: checkpoint.aggregate_prompt_tokens,
            aggregate_output_tokens: checkpoint.aggregate_output_tokens,
            aggregate_cache_tokens: checkpoint.aggregate_cache_tokens,
            aggregate_estimated_cost_microusd: checkpoint.aggregate_estimated_cost_microusd,
        })
    }

    /// Seals one transport attempt before the provider side effect starts.
    #[must_use]
    pub(crate) fn plan_attempt(
        &mut self,
        request: &ProviderRequest,
        provider_id: &str,
        credential_id: &str,
        model_id: &str,
    ) -> ProviderAttemptPlan {
        self.ensure_original_request(request);
        self.next_attempt_index = self.next_attempt_index.saturating_add(1).max(1);
        let request_digest_sha256 = provider_request_digest(request);
        let provider_ref_sha256 = sha256_hex(provider_id.as_bytes());
        if self.primary_provider_ref_sha256.is_empty() {
            self.primary_provider_ref_sha256 = provider_ref_sha256.clone();
        }
        let route_class = if provider_ref_sha256 == self.primary_provider_ref_sha256 {
            "primary"
        } else {
            "fallback"
        };
        let original_request = self.original_request.as_ref().unwrap_or(request);
        let plan_seed = format!(
            "{}:{}:{}",
            self.original_request_digest_sha256, self.next_attempt_index, request_digest_sha256
        );
        ProviderAttemptPlan {
            schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            event_type: PROVIDER_ATTEMPT_PLAN_EVENT.to_owned(),
            attempt_index: self.next_attempt_index,
            plan_id: short_hash(plan_seed.as_bytes()),
            original_request_digest_sha256: self.original_request_digest_sha256.clone(),
            request_digest_sha256,
            request_diff: redacted_request_diff(original_request, request),
            provider_ref_sha256,
            credential_ref_sha256: sha256_hex(credential_id.as_bytes()),
            model_ref_sha256: sha256_hex(model_id.as_bytes()),
            route_class: route_class.to_owned(),
            network_authority_sha256: self.network_authority_sha256.clone(),
            tool_authority_sha256: self.tool_authority_sha256.clone(),
        }
    }

    /// Records one completed transport attempt and aggregates every nested
    /// candidate's usage instead of retaining only the final successful route.
    #[must_use]
    pub(crate) fn record_completed_attempt(
        &mut self,
        plan: &ProviderAttemptPlan,
        response: &ProviderResponse,
    ) -> ProviderAttemptOutcome {
        let mut prompt_tokens = 0_u64;
        let mut output_tokens = 0_u64;
        let mut cache_tokens = 0_u64;
        let mut estimated_cost_microusd = 0_u64;
        let mut state_count = 0_u16;
        for state in response.attempts.iter().filter_map(|attempt| attempt.state.as_ref()) {
            state_count = state_count.saturating_add(1);
            prompt_tokens = prompt_tokens.saturating_add(state.prompt_tokens);
            output_tokens = output_tokens.saturating_add(state.output_tokens);
            cache_tokens = cache_tokens.saturating_add(state.cache_tokens);
            estimated_cost_microusd = estimated_cost_microusd
                .saturating_add(state.estimated_cost_microusd.unwrap_or_default());
        }
        if state_count == 0 {
            prompt_tokens = response.prompt_tokens;
            output_tokens = response.completion_tokens;
        }
        self.record_usage(prompt_tokens, output_tokens, cache_tokens, estimated_cost_microusd);
        ProviderAttemptOutcome {
            schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            event_type: PROVIDER_ATTEMPT_OUTCOME_EVENT.to_owned(),
            attempt_index: plan.attempt_index,
            plan_id: plan.plan_id.clone(),
            provider_ref_sha256: plan.provider_ref_sha256.clone(),
            credential_ref_sha256: plan.credential_ref_sha256.clone(),
            model_ref_sha256: plan.model_ref_sha256.clone(),
            route_class: plan.route_class.clone(),
            disposition: "completed".to_owned(),
            reason_code: "provider.attempt.transport_completed".to_owned(),
            candidate_attempt_count: u16::try_from(response.attempts.len().max(1))
                .unwrap_or(u16::MAX),
            prompt_tokens,
            output_tokens,
            cache_tokens,
            estimated_cost_microusd,
            aggregate_prompt_tokens: self.aggregate_prompt_tokens,
            aggregate_output_tokens: self.aggregate_output_tokens,
            aggregate_cache_tokens: self.aggregate_cache_tokens,
            aggregate_estimated_cost_microusd: self.aggregate_estimated_cost_microusd,
        }
    }

    #[must_use]
    pub(crate) fn record_failed_attempt(
        &self,
        plan: &ProviderAttemptPlan,
        disposition: &str,
        reason_code: &str,
    ) -> ProviderAttemptOutcome {
        ProviderAttemptOutcome {
            schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            event_type: PROVIDER_ATTEMPT_OUTCOME_EVENT.to_owned(),
            attempt_index: plan.attempt_index,
            plan_id: plan.plan_id.clone(),
            provider_ref_sha256: plan.provider_ref_sha256.clone(),
            credential_ref_sha256: plan.credential_ref_sha256.clone(),
            model_ref_sha256: plan.model_ref_sha256.clone(),
            route_class: plan.route_class.clone(),
            disposition: disposition.to_owned(),
            reason_code: reason_code.to_owned(),
            candidate_attempt_count: 1,
            prompt_tokens: 0,
            output_tokens: 0,
            cache_tokens: 0,
            estimated_cost_microusd: 0,
            aggregate_prompt_tokens: self.aggregate_prompt_tokens,
            aggregate_output_tokens: self.aggregate_output_tokens,
            aggregate_cache_tokens: self.aggregate_cache_tokens,
            aggregate_estimated_cost_microusd: self.aggregate_estimated_cost_microusd,
        }
    }

    /// Converts a selected action into one bounded host command.
    #[must_use]
    pub(crate) fn prepare_recovery(
        &self,
        decision: ProviderTurnRecoveryDecision,
        plan: &ProviderAttemptPlan,
        input: RecoveryExecutorInput,
    ) -> PreparedRecoveryAction {
        let unsafe_repeat = input.side_effect_state == ProviderRecoverySideEffectState::Uncertain;
        let silent_failover_after_partial = decision.action
            == ProviderTurnRecoveryAction::FailoverProvider
            && input.partial_user_visible_output;
        let blocked_reason = if unsafe_repeat && action_repeats_provider_effect(decision.action) {
            Some("provider.recovery.blocked.side_effect_uncertain")
        } else if silent_failover_after_partial {
            Some("provider.recovery.blocked.partial_output_requires_merge_policy")
        } else {
            None
        };
        if let Some(reason_code) = blocked_reason {
            let prepared = PreparedRecoveryAction {
                plan: plan.clone(),
                decision,
                command: None,
                immediate_outcome: None,
                side_effect_state: input.side_effect_state,
                partial_user_visible_output: input.partial_user_visible_output,
            };
            let outcome = prepared
                .clone()
                .outcome(RecoveryActionDisposition::Blocked, reason_code.to_owned());
            return PreparedRecoveryAction { immediate_outcome: Some(outcome), ..prepared };
        }

        let command = match decision.action {
            ProviderTurnRecoveryAction::RetrySameProvider => {
                Some(ProviderRecoveryCommand::RetryCurrentRequest)
            }
            ProviderTurnRecoveryAction::RetryWithPrompt => recovery_guidance(
                decision.anomaly,
                decision.attempt,
                input.issue_summary.as_str(),
                input.completed_tool_calls,
                input.summary_only_closeout,
            )
            .map(|guidance| ProviderRecoveryCommand::AppendGuidance { guidance }),
            ProviderTurnRecoveryAction::CompactAndRetry => {
                Some(ProviderRecoveryCommand::RecoverContext)
            }
            ProviderTurnRecoveryAction::LowerReasoningEffort => {
                Some(ProviderRecoveryCommand::LowerOutputBudget)
            }
            ProviderTurnRecoveryAction::ShrinkMultimodal => {
                Some(ProviderRecoveryCommand::DropVisionInputs)
            }
            ProviderTurnRecoveryAction::StripUnsupportedContent => {
                Some(ProviderRecoveryCommand::StripUnsupportedContent)
            }
            ProviderTurnRecoveryAction::RefreshCredential => {
                Some(ProviderRecoveryCommand::RefreshCredential)
            }
            ProviderTurnRecoveryAction::FailoverProvider => {
                Some(ProviderRecoveryCommand::SelectFallbackRoute)
            }
            ProviderTurnRecoveryAction::BackoffRetry => Some(ProviderRecoveryCommand::Backoff {
                delay_ms: decision
                    .retry_after_ms
                    .unwrap_or(DEFAULT_BACKOFF_MS)
                    .min(MAX_RECOVERY_BACKOFF_MS),
            }),
            ProviderTurnRecoveryAction::SyntheticToolResult => None,
            ProviderTurnRecoveryAction::FailDeterministic => {
                Some(ProviderRecoveryCommand::FailDeterministic)
            }
        };
        let mut prepared = PreparedRecoveryAction {
            plan: plan.clone(),
            decision,
            command,
            immediate_outcome: None,
            side_effect_state: input.side_effect_state,
            partial_user_visible_output: input.partial_user_visible_output,
        };
        if prepared.command.is_none() {
            let reason_code =
                if prepared.decision.action == ProviderTurnRecoveryAction::SyntheticToolResult {
                    "provider.recovery.unsupported.synthetic_tool_result"
                } else {
                    "provider.recovery.unsupported.action_context"
                };
            prepared.immediate_outcome = Some(
                prepared
                    .clone()
                    .outcome(RecoveryActionDisposition::Unsupported, reason_code.to_owned()),
            );
        }
        prepared
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
        self.global_recovery_actions = self.global_recovery_actions.saturating_add(1);
        let credential_ref_hash = input
            .credential_id
            .as_deref()
            .map(|credential_id| short_hash(credential_id.as_bytes()));
        let budget = retry_budget(anomaly);
        let exhausted = attempt > budget;
        let global_exhausted = self.global_recovery_actions > DEFAULT_GLOBAL_RECOVERY_BUDGET;
        let mut action = recovery_action_for_anomaly(anomaly, exhausted);
        let mut reason_code = format!("provider.turn_recovery.{}", anomaly.as_str());
        let mut prompt_mutation = prompt_mutation_for_anomaly(anomaly, action);
        let mut context_mutation_plan = context_mutation_for_anomaly(anomaly, action);

        if global_exhausted {
            action = ProviderTurnRecoveryAction::FailDeterministic;
            reason_code.push_str(".global_budget_exhausted");
            prompt_mutation = None;
            context_mutation_plan = None;
        } else {
            match anomaly {
                ProviderTurnAnomaly::AuthInvalid | ProviderTurnAnomaly::AuthExpired => {
                    if let Some(hash) = credential_ref_hash.as_ref() {
                        if !self.refreshed_credential_hashes.insert(hash.clone()) {
                            action = ProviderTurnRecoveryAction::FailoverProvider;
                            reason_code.push_str(".credential_refresh_exhausted");
                            prompt_mutation = None;
                            context_mutation_plan =
                                Some("provider_failover_policy_only".to_owned());
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
                ProviderTurnAnomaly::MultimodalUnsupported if !exhausted => match attempt {
                    1 => {
                        action = ProviderTurnRecoveryAction::ShrinkMultimodal;
                        context_mutation_plan =
                            Some("replace_non_current_images_with_metadata".to_owned());
                    }
                    2 => {
                        action = ProviderTurnRecoveryAction::StripUnsupportedContent;
                        context_mutation_plan =
                            Some("strip_provider_unsupported_multimodal_parts".to_owned());
                        prompt_mutation = None;
                    }
                    _ => {
                        action = ProviderTurnRecoveryAction::RetryWithPrompt;
                        prompt_mutation = Some("use_textual_image_metadata_fallback".to_owned());
                        context_mutation_plan =
                            Some("route_image_facts_through_metadata_only_tools".to_owned());
                    }
                },
                ProviderTurnAnomaly::MaxOutputTokensTooLarge => {
                    action = ProviderTurnRecoveryAction::LowerReasoningEffort;
                    context_mutation_plan = Some("lower_max_output_tokens".to_owned());
                }
                _ => {}
            }
        }

        ProviderTurnRecoveryDecision {
            schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            event_type: PROVIDER_TURN_RECOVERY_EVENT.to_owned(),
            anomaly,
            action,
            reason_code,
            attempt,
            exhausted: global_exhausted
                || matches!(action, ProviderTurnRecoveryAction::FailDeterministic)
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

    fn ensure_original_request(&mut self, request: &ProviderRequest) {
        if self.original_request.is_some() {
            return;
        }
        self.original_request = Some(request.clone());
        self.original_request_digest_sha256 = provider_request_digest(request);
        if self.network_authority_sha256.is_empty() {
            self.network_authority_sha256 = sha256_hex(b"provider_network_authority_unbound");
        }
        if self.tool_authority_sha256.is_empty() {
            self.tool_authority_sha256 = sha256_hex(b"provider_tool_authority_unbound");
        }
    }

    fn record_usage(
        &mut self,
        prompt_tokens: u64,
        output_tokens: u64,
        cache_tokens: u64,
        estimated_cost_microusd: u64,
    ) {
        self.aggregate_prompt_tokens = self.aggregate_prompt_tokens.saturating_add(prompt_tokens);
        self.aggregate_output_tokens = self.aggregate_output_tokens.saturating_add(output_tokens);
        self.aggregate_cache_tokens = self.aggregate_cache_tokens.saturating_add(cache_tokens);
        self.aggregate_estimated_cost_microusd =
            self.aggregate_estimated_cost_microusd.saturating_add(estimated_cost_microusd);
    }
}

fn provider_request_digest(request: &ProviderRequest) -> String {
    let serialized = serde_json::to_vec(request).unwrap_or_default();
    sha256_hex(serialized.as_slice())
}

fn optional_ref_digest(value: Option<&str>) -> Option<String> {
    value.map(|value| sha256_hex(value.as_bytes()))
}

fn redacted_request_diff(
    original: &ProviderRequest,
    current: &ProviderRequest,
) -> RedactedProviderRequestDiff {
    let mut changed_fields = Vec::new();
    if original.input_text != current.input_text {
        changed_fields.push("input_text".to_owned());
    }
    if original.messages != current.messages {
        changed_fields.push("messages".to_owned());
    }
    if original.vision_inputs != current.vision_inputs {
        changed_fields.push("vision_inputs".to_owned());
    }
    if original.model_override != current.model_override {
        changed_fields.push("model_override".to_owned());
    }
    if original.tool_catalog_snapshot != current.tool_catalog_snapshot {
        changed_fields.push("tool_catalog_snapshot".to_owned());
    }
    if original.max_output_tokens != current.max_output_tokens {
        changed_fields.push("max_output_tokens".to_owned());
    }
    if original.reasoning_effort != current.reasoning_effort {
        changed_fields.push("reasoning_effort".to_owned());
    }
    RedactedProviderRequestDiff {
        changed_fields,
        original_message_count: original.messages.len(),
        current_message_count: current.messages.len(),
        original_vision_input_count: original.vision_inputs.len(),
        current_vision_input_count: current.vision_inputs.len(),
        original_input_bytes: original.input_text.len(),
        current_input_bytes: current.input_text.len(),
        original_model_ref_sha256: optional_ref_digest(original.model_override.as_deref()),
        current_model_ref_sha256: optional_ref_digest(current.model_override.as_deref()),
    }
}

const fn action_repeats_provider_effect(action: ProviderTurnRecoveryAction) -> bool {
    matches!(
        action,
        ProviderTurnRecoveryAction::RetrySameProvider
            | ProviderTurnRecoveryAction::RetryWithPrompt
            | ProviderTurnRecoveryAction::CompactAndRetry
            | ProviderTurnRecoveryAction::LowerReasoningEffort
            | ProviderTurnRecoveryAction::ShrinkMultimodal
            | ProviderTurnRecoveryAction::StripUnsupportedContent
            | ProviderTurnRecoveryAction::RefreshCredential
            | ProviderTurnRecoveryAction::FailoverProvider
            | ProviderTurnRecoveryAction::BackoffRetry
    )
}

fn recovery_guidance(
    anomaly: ProviderTurnAnomaly,
    attempt: u8,
    issue_summary: &str,
    completed_tool_calls: u32,
    summary_only_closeout: bool,
) -> Option<String> {
    let safe_issue = bounded_recovery_issue(issue_summary);
    let guidance = match anomaly {
        ProviderTurnAnomaly::LengthFinalText => match attempt {
            1 => {
                "The previous assistant output hit the provider output limit before a complete final answer or structured tool call. Continue now with no explanatory preamble. If a tool is required, issue one concise structured tool call using the visible schema. Otherwise answer in at most 120 words and do not claim unverified work."
            }
            2 => {
                "The previous length recovery also hit the output limit. Do not restate prior work. Issue exactly one small structured tool call, or provide a final answer under 60 words and mark unfinished work as partial."
            }
            _ => {
                "This is the last length-recovery attempt. Produce one minimal structured tool call or one concise final answer. Do not include previews or explanatory prose before the result."
            }
        }
        .to_owned(),
        ProviderTurnAnomaly::BrowserFollowupTimeout if completed_tool_calls > 0 => format!(
            "The browser follow-up timed out after tool evidence was recorded. Continue from the existing evidence and do not recapture it unless it is missing or stale. Issue one minimal next tool call or finish concisely. Last issue: {safe_issue}"
        ),
        ProviderTurnAnomaly::ToolFollowupTimeout if completed_tool_calls > 0 => format!(
            "The tool follow-up timed out after tool results were recorded. Continue from those results and do not rerun completed tools unless their evidence is missing or stale. Issue one minimal next tool call or finish concisely. Last issue: {safe_issue}"
        ),
        ProviderTurnAnomaly::ReasoningOnly | ProviderTurnAnomaly::EmptyFinalAnswer
            if summary_only_closeout =>
        {
            "The user requested a summary-only closeout. Do not call tools or propose future work. Answer with the current conversation status and explicitly mark any filesystem, command, browser, or validation state that is unknown.".to_owned()
        }
        ProviderTurnAnomaly::ReasoningOnly | ProviderTurnAnomaly::EmptyFinalAnswer
            if completed_tool_calls == 0 =>
        {
            "The provider did not produce a user-visible final answer. Retry once with a concise visible answer, or issue the minimal structured tool call required to make progress. Do not return analysis-only text or an empty response.".to_owned()
        }
        ProviderTurnAnomaly::EmptyPostToolResponse
        | ProviderTurnAnomaly::ReasoningOnly
        | ProviderTurnAnomaly::EmptyFinalAnswer => {
            "The previous turn did not provide a usable final answer after tool execution. Continue from the existing tool evidence. If the work is complete, summarize changed artifacts and validation; otherwise issue the next minimal tool call. Do not repeat completed tools or claim success without evidence.".to_owned()
        }
        ProviderTurnAnomaly::DroppedToolCall | ProviderTurnAnomaly::PartialContentStream => {
            "The provider stream ended before a complete structured action or visible answer. Produce either one complete tool call using the visible schema or one concise final answer. Do not repeat already completed tool operations.".to_owned()
        }
        ProviderTurnAnomaly::InvalidToolName => {
            "The proposed tool name was not in the visible catalog. Select exactly one tool name from the current catalog and provide only arguments accepted by that schema.".to_owned()
        }
        ProviderTurnAnomaly::MalformedJsonArguments => {
            "The previous tool arguments were not a valid JSON object. Retry once with exactly one complete tool call and schema-valid JSON arguments; do not include prose around the call.".to_owned()
        }
        ProviderTurnAnomaly::MultimodalUnsupported => {
            "The selected provider cannot consume the original media parts. Continue using only the retained textual image metadata and explicitly state when visual evidence is unavailable.".to_owned()
        }
        _ => return None,
    };
    Some(guidance)
}

fn bounded_recovery_issue(issue: &str) -> String {
    let normalized = issue.trim().replace(['\r', '\n'], " ");
    const MAX_BYTES: usize = 512;
    if normalized.len() <= MAX_BYTES {
        return normalized;
    }
    let mut boundary = MAX_BYTES;
    while !normalized.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    format!("{}...", &normalized[..boundary])
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
pub(crate) fn anomaly_from_terminal_validation(
    outcome: &ProviderTerminalValidationOutcome,
) -> ProviderTurnAnomaly {
    let reason = outcome.reason_code.as_str();
    if reason.contains("reasoning_only") {
        ProviderTurnAnomaly::ReasoningOnly
    } else if reason.contains("tool")
        && (reason.contains("partial") || reason.contains("ambiguous"))
    {
        ProviderTurnAnomaly::PartialToolCall
    } else if reason.contains("idle_timeout") {
        ProviderTurnAnomaly::ProviderTimeout
    } else if reason.contains("partial") {
        ProviderTurnAnomaly::PartialContentStream
    } else {
        ProviderTurnAnomaly::MalformedStream
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
        ProviderFailureClass::UnsupportedMultimodal => ProviderTurnAnomaly::MultimodalUnsupported,
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
        | ProviderTurnAnomaly::InvalidEncryptedContent
        | ProviderTurnAnomaly::RateLimit
        | ProviderTurnAnomaly::UnicodeSurrogate => DEFAULT_SINGLE_RETRY_BUDGET,
        ProviderTurnAnomaly::MultimodalUnsupported => DEFAULT_MULTIMODAL_RETRY_BUDGET,
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
        | ProviderTurnAnomaly::MalformedStream
        | ProviderTurnAnomaly::UnicodeSurrogate => ProviderTurnRecoveryAction::RetrySameProvider,
        ProviderTurnAnomaly::BrowserFollowupTimeout
        | ProviderTurnAnomaly::ToolFollowupTimeout
        | ProviderTurnAnomaly::LengthFinalText
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
        ProviderTurnAnomaly::BrowserFollowupTimeout => {
            Some("continue_from_existing_browser_evidence".to_owned())
        }
        ProviderTurnAnomaly::ToolFollowupTimeout => {
            Some("continue_from_existing_tool_evidence".to_owned())
        }
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
    use crate::model_provider::{
        ProviderAttemptState, ProviderAttemptSummary, ProviderFinishReason,
        ProviderRawProviderRefs, ProviderTurnOutput, ProviderUsage,
    };

    const PROVIDER_RECOVERY_LIFECYCLE_GOLDEN: &str =
        include_str!("../../../../fixtures/golden/provider_recovery_lifecycle_cases.json");

    #[derive(serde::Deserialize)]
    struct ProviderRecoveryLifecycleGolden {
        cases: Vec<ProviderRecoveryLifecycleCase>,
    }

    #[derive(serde::Deserialize)]
    struct ProviderRecoveryLifecycleCase {
        id: String,
        anomaly: String,
        retry_after_ms: Option<u64>,
        credential_id: Option<String>,
        expected: ProviderRecoveryLifecycleExpected,
    }

    #[derive(serde::Deserialize)]
    struct ProviderRecoveryLifecycleExpected {
        event_type: String,
        action: String,
        reason_prefix: String,
        attempt: u8,
        exhausted: bool,
    }

    fn decide_once(anomaly: ProviderTurnAnomaly) -> ProviderTurnRecoveryDecision {
        ProviderAttemptStateMachine::new().decide(anomaly, ProviderTurnRecoveryInput::default())
    }

    fn provider_request(input: &str) -> ProviderRequest {
        ProviderRequest::from_input_text(input.to_owned(), false, Vec::new(), None)
    }

    fn attempt_plan(
        state: &mut ProviderAttemptStateMachine,
        request: &ProviderRequest,
    ) -> ProviderAttemptPlan {
        state.plan_attempt(request, "provider-a", "credential-a", "model-a")
    }

    fn decision_for(
        anomaly: ProviderTurnAnomaly,
        action: ProviderTurnRecoveryAction,
    ) -> ProviderTurnRecoveryDecision {
        ProviderTurnRecoveryDecision {
            schema_version: PROVIDER_TURN_RECOVERY_SCHEMA_VERSION,
            event_type: PROVIDER_TURN_RECOVERY_EVENT.to_owned(),
            anomaly,
            action,
            reason_code: format!("provider.turn_recovery.{}", anomaly.as_str()),
            attempt: 1,
            exhausted: false,
            prompt_mutation: None,
            context_mutation_plan: None,
            user_visible_status_policy: UserVisibleStatusPolicy::Silent,
            retry_after_ms: Some(10_000),
            credential_ref_hash: None,
        }
    }

    fn golden_anomaly(value: &str) -> ProviderTurnAnomaly {
        match value {
            "malformed_stream" => ProviderTurnAnomaly::MalformedStream,
            "partial_tool_call" => ProviderTurnAnomaly::PartialToolCall,
            "empty_final_answer" => ProviderTurnAnomaly::EmptyFinalAnswer,
            "rate_limit" => ProviderTurnAnomaly::RateLimit,
            "auth_expired" => ProviderTurnAnomaly::AuthExpired,
            "unicode_surrogate" => ProviderTurnAnomaly::UnicodeSurrogate,
            "multimodal_unsupported" => ProviderTurnAnomaly::MultimodalUnsupported,
            other => panic!("unsupported provider recovery golden anomaly: {other}"),
        }
    }

    #[test]
    fn recovery_lifecycle_golden_matches_state_machine() {
        let golden = serde_json::from_str::<ProviderRecoveryLifecycleGolden>(
            PROVIDER_RECOVERY_LIFECYCLE_GOLDEN,
        )
        .expect("provider recovery lifecycle golden should parse");

        for case in golden.cases {
            let mut state = ProviderAttemptStateMachine::new();
            let decision = state.decide(
                golden_anomaly(case.anomaly.as_str()),
                ProviderTurnRecoveryInput {
                    credential_id: case.credential_id,
                    retry_after_ms: case.retry_after_ms,
                    context_pressure: None,
                },
            );

            assert_eq!(decision.event_type, case.expected.event_type, "{}", case.id);
            assert_eq!(decision.action.as_str(), case.expected.action, "{}", case.id);
            assert_eq!(decision.attempt, case.expected.attempt, "{}", case.id);
            assert_eq!(decision.exhausted, case.expected.exhausted, "{}", case.id);
            assert!(
                decision.reason_code.starts_with(case.expected.reason_prefix.as_str()),
                "{} expected reason prefix {}, got {}",
                case.id,
                case.expected.reason_prefix,
                decision.reason_code
            );
        }
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
        let mut state = ProviderAttemptStateMachine::new();
        let first =
            state.decide(ProviderTurnAnomaly::ReasoningOnly, ProviderTurnRecoveryInput::default());
        let second =
            state.decide(ProviderTurnAnomaly::ReasoningOnly, ProviderTurnRecoveryInput::default());

        assert_eq!(first.action, ProviderTurnRecoveryAction::RetryWithPrompt);
        assert_eq!(second.action, ProviderTurnRecoveryAction::FailDeterministic);
        assert!(second.exhausted);
    }

    #[test]
    fn multimodal_unsupported_uses_deterministic_recovery_ladder() {
        let mut state = ProviderAttemptStateMachine::new();

        let first = state.decide(
            ProviderTurnAnomaly::MultimodalUnsupported,
            ProviderTurnRecoveryInput::default(),
        );
        let second = state.decide(
            ProviderTurnAnomaly::MultimodalUnsupported,
            ProviderTurnRecoveryInput::default(),
        );
        let third = state.decide(
            ProviderTurnAnomaly::MultimodalUnsupported,
            ProviderTurnRecoveryInput::default(),
        );
        let fourth = state.decide(
            ProviderTurnAnomaly::MultimodalUnsupported,
            ProviderTurnRecoveryInput::default(),
        );

        assert_eq!(first.action, ProviderTurnRecoveryAction::ShrinkMultimodal);
        assert_eq!(
            first.context_mutation_plan.as_deref(),
            Some("replace_non_current_images_with_metadata")
        );
        assert_eq!(second.action, ProviderTurnRecoveryAction::StripUnsupportedContent);
        assert_eq!(
            second.context_mutation_plan.as_deref(),
            Some("strip_provider_unsupported_multimodal_parts")
        );
        assert_eq!(third.action, ProviderTurnRecoveryAction::RetryWithPrompt);
        assert_eq!(third.prompt_mutation.as_deref(), Some("use_textual_image_metadata_fallback"));
        assert_eq!(
            third.context_mutation_plan.as_deref(),
            Some("route_image_facts_through_metadata_only_tools")
        );
        assert_eq!(fourth.action, ProviderTurnRecoveryAction::FailDeterministic);
        assert!(fourth.exhausted);
    }

    #[test]
    fn length_recovery_has_larger_budget() {
        let mut state = ProviderAttemptStateMachine::new();

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
        let mut state = ProviderAttemptStateMachine::new();
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
        let mut state = ProviderAttemptStateMachine::new();

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
        let mut state = ProviderAttemptStateMachine::new();

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
        let mut state = ProviderAttemptStateMachine::new();
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

    #[test]
    fn attempt_plan_redacts_requests_and_seals_authority() {
        let original = provider_request("private-user-input-secret");
        let mut state =
            ProviderAttemptStateMachine::for_request(&original, "network:provider-only", "tools:a");
        let original_plan = attempt_plan(&mut state, &original);
        let mut recovered = original.clone();
        recovered.input_text.push_str("\nrecovery guidance");
        recovered
            .messages
            .push(crate::model_provider::ProviderMessage::user_text("bounded recovery guidance"));
        let recovered_plan = attempt_plan(&mut state, &recovered);
        let payload = recovered_plan.tape_payload().to_string();

        assert_eq!(
            original_plan.original_request_digest_sha256,
            recovered_plan.original_request_digest_sha256
        );
        assert_ne!(original_plan.request_digest_sha256, recovered_plan.request_digest_sha256);
        assert_eq!(original_plan.network_authority_sha256, recovered_plan.network_authority_sha256);
        assert_eq!(original_plan.tool_authority_sha256, recovered_plan.tool_authority_sha256);
        assert!(!payload.contains("private-user-input-secret"));
        assert!(!payload.contains("bounded recovery guidance"));
        assert!(recovered_plan.request_diff.changed_fields.contains(&"input_text".to_owned()));
        assert!(recovered_plan.request_diff.changed_fields.contains(&"messages".to_owned()));
    }

    #[test]
    fn global_budget_stops_strategy_rotation() {
        let mut state = ProviderAttemptStateMachine::new();
        for anomaly in [
            ProviderTurnAnomaly::ProviderTimeout,
            ProviderTurnAnomaly::ReasoningOnly,
            ProviderTurnAnomaly::EmptyFinalAnswer,
            ProviderTurnAnomaly::ContextOverflow,
            ProviderTurnAnomaly::MultimodalUnsupported,
            ProviderTurnAnomaly::RateLimit,
        ] {
            assert_ne!(
                state.decide(anomaly, ProviderTurnRecoveryInput::default()).action,
                ProviderTurnRecoveryAction::FailDeterministic
            );
        }

        let exhausted = state
            .decide(ProviderTurnAnomaly::UnicodeSurrogate, ProviderTurnRecoveryInput::default());

        assert_eq!(exhausted.action, ProviderTurnRecoveryAction::FailDeterministic);
        assert!(exhausted.exhausted);
        assert!(exhausted.reason_code.ends_with(".global_budget_exhausted"));
    }

    #[test]
    fn uncertain_side_effect_blocks_provider_repetition() {
        let request = provider_request("run a side effect");
        let mut state = ProviderAttemptStateMachine::for_request(&request, "network:a", "tools:a");
        let plan = attempt_plan(&mut state, &request);
        let decision = decision_for(
            ProviderTurnAnomaly::ProviderTimeout,
            ProviderTurnRecoveryAction::RetrySameProvider,
        );

        let prepared = state.prepare_recovery(
            decision,
            &plan,
            RecoveryExecutorInput {
                side_effect_state: ProviderRecoverySideEffectState::Uncertain,
                ..RecoveryExecutorInput::default()
            },
        );

        assert!(prepared.command.is_none());
        let outcome =
            prepared.immediate_outcome.expect("unsafe retry must have a terminal outcome");
        assert_eq!(outcome.disposition, RecoveryActionDisposition::Blocked);
        assert_eq!(outcome.reason_code, "provider.recovery.blocked.side_effect_uncertain");
    }

    #[test]
    fn partial_output_blocks_silent_route_failover() {
        let request = provider_request("produce a visible answer");
        let mut state = ProviderAttemptStateMachine::for_request(&request, "network:a", "tools:a");
        let plan = attempt_plan(&mut state, &request);
        let decision = decision_for(
            ProviderTurnAnomaly::AuthExpired,
            ProviderTurnRecoveryAction::FailoverProvider,
        );

        let prepared = state.prepare_recovery(
            decision,
            &plan,
            RecoveryExecutorInput {
                partial_user_visible_output: true,
                ..RecoveryExecutorInput::default()
            },
        );

        let outcome = prepared
            .immediate_outcome
            .expect("partial-output failover must require an explicit merge policy");
        assert_eq!(outcome.disposition, RecoveryActionDisposition::Blocked);
        assert_eq!(
            outcome.reason_code,
            "provider.recovery.blocked.partial_output_requires_merge_policy"
        );
    }

    #[test]
    fn every_recovery_action_has_a_command_or_explicit_unsupported_outcome() {
        let request = provider_request("recover");
        let mut state = ProviderAttemptStateMachine::for_request(&request, "network:a", "tools:a");
        let plan = attempt_plan(&mut state, &request);
        let cases = [
            (ProviderTurnAnomaly::ProviderTimeout, ProviderTurnRecoveryAction::RetrySameProvider),
            (ProviderTurnAnomaly::ReasoningOnly, ProviderTurnRecoveryAction::RetryWithPrompt),
            (ProviderTurnAnomaly::ContextOverflow, ProviderTurnRecoveryAction::CompactAndRetry),
            (
                ProviderTurnAnomaly::MaxOutputTokensTooLarge,
                ProviderTurnRecoveryAction::LowerReasoningEffort,
            ),
            (
                ProviderTurnAnomaly::MultimodalUnsupported,
                ProviderTurnRecoveryAction::ShrinkMultimodal,
            ),
            (
                ProviderTurnAnomaly::MultimodalUnsupported,
                ProviderTurnRecoveryAction::StripUnsupportedContent,
            ),
            (ProviderTurnAnomaly::AuthExpired, ProviderTurnRecoveryAction::RefreshCredential),
            (ProviderTurnAnomaly::AuthExpired, ProviderTurnRecoveryAction::FailoverProvider),
            (ProviderTurnAnomaly::RateLimit, ProviderTurnRecoveryAction::BackoffRetry),
            (
                ProviderTurnAnomaly::MalformedToolSequence,
                ProviderTurnRecoveryAction::SyntheticToolResult,
            ),
            (
                ProviderTurnAnomaly::ContentPolicyBlocked,
                ProviderTurnRecoveryAction::FailDeterministic,
            ),
        ];

        for (anomaly, action) in cases {
            let prepared = state.prepare_recovery(
                decision_for(anomaly, action),
                &plan,
                RecoveryExecutorInput::default(),
            );
            if action == ProviderTurnRecoveryAction::SyntheticToolResult {
                let outcome =
                    prepared.immediate_outcome.expect("unsupported action must be explicit");
                assert_eq!(outcome.disposition, RecoveryActionDisposition::Unsupported);
            } else {
                assert!(prepared.command.is_some(), "missing command for {action:?}");
                assert!(prepared.immediate_outcome.is_none());
            }
        }
    }

    #[test]
    fn post_tool_guidance_continues_without_rerun_command() {
        let request = provider_request("continue after tools");
        let mut state = ProviderAttemptStateMachine::for_request(&request, "network:a", "tools:a");
        let plan = attempt_plan(&mut state, &request);
        let prepared = state.prepare_recovery(
            decision_for(
                ProviderTurnAnomaly::EmptyPostToolResponse,
                ProviderTurnRecoveryAction::RetryWithPrompt,
            ),
            &plan,
            RecoveryExecutorInput {
                issue_summary: "provider returned no final answer".to_owned(),
                completed_tool_calls: 2,
                side_effect_state: ProviderRecoverySideEffectState::ConfirmedWithReconciliation,
                ..RecoveryExecutorInput::default()
            },
        );

        let ProviderRecoveryCommand::AppendGuidance { guidance } =
            prepared.command.expect("post-tool retry should append bounded guidance")
        else {
            panic!("post-tool recovery must not produce a tool-rerun command");
        };
        assert!(guidance.contains("Do not repeat completed tools"));
    }

    #[test]
    fn checkpoint_round_trip_preserves_budgets_without_request_payload() {
        let request = provider_request("checkpoint-secret-input");
        let mut state =
            ProviderAttemptStateMachine::for_request(&request, "network:provider-only", "tools:a");
        let first = state
            .decide(ProviderTurnAnomaly::ProviderTimeout, ProviderTurnRecoveryInput::default());
        assert_eq!(first.action, ProviderTurnRecoveryAction::RetrySameProvider);
        let first_plan = attempt_plan(&mut state, &request);
        let encoded = serde_json::to_string(&state.checkpoint()).expect("checkpoint should encode");
        assert!(!encoded.contains("checkpoint-secret-input"));
        let checkpoint =
            serde_json::from_str(&encoded).expect("checkpoint should decode after restart");
        let mut restored = ProviderAttemptStateMachine::restore(
            &request,
            "network:provider-only",
            "tools:a",
            checkpoint,
        )
        .expect("matching request and authority should restore");

        let exhausted = restored
            .decide(ProviderTurnAnomaly::ProviderTimeout, ProviderTurnRecoveryInput::default());
        let second_plan = attempt_plan(&mut restored, &request);

        assert_eq!(exhausted.action, ProviderTurnRecoveryAction::FailDeterministic);
        assert_eq!(second_plan.attempt_index, first_plan.attempt_index + 1);
        let mismatch = ProviderAttemptStateMachine::restore(
            &request,
            "network:expanded",
            "tools:a",
            restored.checkpoint(),
        )
        .expect_err("expanded authority must invalidate the checkpoint");
        assert_eq!(mismatch, "provider.recovery.checkpoint_authority_mismatch");
    }

    #[test]
    fn completed_attempt_aggregates_all_candidate_usage_and_cost() {
        let request = provider_request("aggregate usage");
        let mut state = ProviderAttemptStateMachine::for_request(&request, "network:a", "tools:a");
        let plan = attempt_plan(&mut state, &request);
        let output = ProviderTurnOutput::text(
            "done".to_owned(),
            ProviderFinishReason::Stop,
            ProviderUsage::new(99, 99, "test"),
            ProviderRawProviderRefs::default(),
        );
        let attempts = [(3, 5, 7, 11), (13, 17, 19, 23)]
            .into_iter()
            .enumerate()
            .map(|(attempt_index, (prompt_tokens, output_tokens, cache_tokens, cost))| {
                ProviderAttemptSummary {
                    provider_id: "provider-a".to_owned(),
                    model_id: "model-a".to_owned(),
                    outcome: "success".to_owned(),
                    retryable: false,
                    served_from_cache: false,
                    reason_code: None,
                    state: Some(ProviderAttemptState {
                        attempt_index: u32::try_from(attempt_index).unwrap_or(u32::MAX),
                        provider_profile_id: "provider-a".to_owned(),
                        credential_id: "credential-a".to_owned(),
                        model_id: "model-a".to_owned(),
                        error_class: None,
                        retry_after_ms: None,
                        cooldown_until_unix_ms: None,
                        prompt_tokens,
                        output_tokens,
                        cache_tokens,
                        estimated_cost_microusd: Some(cost),
                        final_disposition: "success".to_owned(),
                        repair_hint: None,
                    }),
                }
            })
            .collect();
        let response = ProviderResponse {
            events: crate::model_provider::provider_events_from_output(&output),
            prompt_tokens: output.usage.prompt_tokens,
            completion_tokens: output.usage.completion_tokens,
            output,
            retry_count: 0,
            provider_id: "provider-a".to_owned(),
            model_id: "model-a".to_owned(),
            served_from_cache: false,
            failover_count: 1,
            attempts,
            qa_lane_attestation: None,
        };

        let outcome = state.record_completed_attempt(&plan, &response);

        assert_eq!(outcome.candidate_attempt_count, 2);
        assert_eq!(outcome.prompt_tokens, 16);
        assert_eq!(outcome.output_tokens, 22);
        assert_eq!(outcome.cache_tokens, 26);
        assert_eq!(outcome.estimated_cost_microusd, 34);
        assert_eq!(outcome.aggregate_prompt_tokens, 16);
        assert_eq!(outcome.aggregate_estimated_cost_microusd, 34);
    }
}
