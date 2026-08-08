//! Provider error taxonomy and recovery classification.
//!
//! These types are serialized into status snapshots, journals, and console
//! envelopes. Display text and serde names are compatibility-sensitive.

use serde::{Deserialize, Serialize};

use crate::{ProviderErrorEnvelope, ProviderRecoveryDecisionKind};

/// Terminal failure of a provider operation after the per-provider retry
/// budget is spent.
///
/// Display strings are part of the operator-facing contract (tests and
/// fixtures assert on them); change them only deliberately. Each error maps
/// to a [`ProviderFailureClassification`] for recovery routing.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ProviderError {
    #[error("model provider circuit breaker is open; retry after {retry_after_ms}ms")]
    CircuitOpen { retry_after_ms: u64 },
    #[error(
        "openai-compatible provider requires PALYRA_MODEL_PROVIDER_OPENAI_API_KEY, PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID, model_provider.openai_api_key_secret_ref, or model_provider.openai_api_key_vault_ref"
    )]
    MissingApiKey,
    #[error(
        "anthropic provider requires PALYRA_MODEL_PROVIDER_ANTHROPIC_API_KEY, PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID, model_provider.anthropic_api_key_secret_ref, or model_provider.anthropic_api_key_vault_ref"
    )]
    MissingAnthropicApiKey,
    #[error(
        "openai-compatible embeddings provider requires model_provider.openai_embeddings_model or PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL"
    )]
    MissingEmbeddingsModel,
    #[error("provider '{provider}' does not support vision inputs")]
    VisionUnsupported { provider: String },
    #[error("provider '{provider}' does not support audio synthesis")]
    AudioSynthesisUnsupported { provider: String },
    #[error("embeddings request is invalid: {message}")]
    InvalidEmbeddingsRequest { message: String },
    #[error(
        "provider request failed after {retry_count} retries (retryable={retryable}): {message}"
    )]
    RequestFailed {
        message: String,
        retryable: bool,
        retry_count: u32,
        classification: ProviderFailureClassification,
    },
    #[error("provider response was invalid after {retry_count} retries: {message}")]
    InvalidResponse {
        message: String,
        retry_count: u32,
        classification: ProviderFailureClassification,
    },
    #[error("provider state lock was poisoned")]
    StatePoisoned,
}

impl ProviderError {
    /// Returns how many retries were attempted before this error became
    /// terminal (zero for errors raised before any request was sent).
    #[must_use]
    pub const fn retry_count(&self) -> u32 {
        match self {
            Self::RequestFailed { retry_count, .. } => *retry_count,
            Self::InvalidResponse { retry_count, .. } => *retry_count,
            _ => 0,
        }
    }

    /// Returns true when the request was rejected by an open circuit breaker.
    #[must_use]
    pub const fn is_circuit_open(&self) -> bool {
        matches!(self, Self::CircuitOpen { .. })
    }

    /// Returns the failure classification driving recovery routing
    /// (retry, failover, credential rotation, fail closed).
    #[must_use]
    pub fn classification(&self) -> ProviderFailureClassification {
        match self {
            Self::CircuitOpen { .. } => ProviderFailureClassification::new(
                ProviderFailureClass::TransientUpstream,
                ProviderFailureAction::Retry,
                None,
                Some("circuit_open".to_owned()),
            ),
            Self::MissingApiKey | Self::MissingAnthropicApiKey => {
                ProviderFailureClassification::new(
                    ProviderFailureClass::AuthInvalid,
                    ProviderFailureAction::RotateCredential,
                    None,
                    Some("missing_api_key".to_owned()),
                )
            }
            Self::MissingEmbeddingsModel => ProviderFailureClassification::new(
                ProviderFailureClass::PermanentUpstream,
                ProviderFailureAction::FailClosedNoRetry,
                None,
                Some("missing_embeddings_model".to_owned()),
            ),
            Self::VisionUnsupported { .. } => ProviderFailureClassification::new(
                ProviderFailureClass::UnsupportedMultimodal,
                ProviderFailureAction::Retry,
                None,
                Some("vision_unsupported".to_owned()),
            ),
            Self::AudioSynthesisUnsupported { .. } => ProviderFailureClassification::new(
                ProviderFailureClass::UnsupportedMultimodal,
                ProviderFailureAction::FailClosedNoRetry,
                None,
                Some("audio_synthesis_unsupported".to_owned()),
            ),
            Self::InvalidEmbeddingsRequest { .. } => ProviderFailureClassification::new(
                ProviderFailureClass::MalformedResponse,
                ProviderFailureAction::FailClosedNoRetry,
                None,
                Some("invalid_embeddings_request".to_owned()),
            ),
            Self::RequestFailed { classification, .. }
            | Self::InvalidResponse { classification, .. } => classification.clone(),
            Self::StatePoisoned => ProviderFailureClassification::new(
                ProviderFailureClass::PermanentUpstream,
                ProviderFailureAction::FailClosedNoRetry,
                None,
                Some("state_poisoned".to_owned()),
            ),
        }
    }

    /// Builds the serializable failure snapshot (classification, recovery
    /// plan, and a message safe for journaling) for this error.
    #[must_use]
    pub fn failure_snapshot(&self) -> ProviderFailureSnapshot {
        let message = match self {
            Self::CircuitOpen { retry_after_ms } => {
                format!("model provider circuit breaker is open; retry after {retry_after_ms}ms")
            }
            Self::MissingApiKey => "model provider API key is missing".to_owned(),
            Self::MissingAnthropicApiKey => {
                "anthropic model provider API key is missing".to_owned()
            }
            Self::MissingEmbeddingsModel => "model provider embeddings model is missing".to_owned(),
            Self::VisionUnsupported { provider } => {
                format!("provider '{provider}' does not support vision inputs")
            }
            Self::AudioSynthesisUnsupported { provider } => {
                format!("provider '{provider}' does not support audio synthesis")
            }
            Self::InvalidEmbeddingsRequest { message }
            | Self::RequestFailed { message, .. }
            | Self::InvalidResponse { message, .. } => message.clone(),
            Self::StatePoisoned => "model provider state lock poisoned".to_owned(),
        };
        match self {
            Self::CircuitOpen { retry_after_ms } => {
                self.classification().snapshot(message).with_retry_after_ms(Some(*retry_after_ms))
            }
            _ => self.classification().snapshot(message),
        }
    }

    /// Builds the stable, redacted error envelope surfaced to console/journal
    /// consumers.
    #[must_use]
    pub fn envelope(&self) -> ProviderErrorEnvelope {
        ProviderErrorEnvelope::from_error(self)
    }
}

/// Fine-grained failure class derived from HTTP status, response body
/// keywords, and transport errors; the primary key for recovery decisions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureClass {
    AuthInvalid,
    AuthExpired,
    PermissionDenied,
    RateLimit,
    RateLimited,
    Quota,
    QuotaExceeded,
    SchemaRejected,
    BadToolArguments,
    TruncatedToolArguments,
    ContextOverflow,
    TransientUpstream,
    PermanentUpstream,
    ContextWindowExceeded,
    ContentPolicyBlocked,
    MalformedResponse,
    MalformedStream,
    EmptyOutput,
    PrematureFinal,
    PayloadTooLarge,
    ProviderUnavailable,
    NetworkUnavailable,
    ProviderTimeout,
    UnsupportedMultimodal,
}

impl ProviderFailureClass {
    /// Returns the canonical snake_case identifier used in failure snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthInvalid => "auth_invalid",
            Self::AuthExpired => "auth_expired",
            Self::PermissionDenied => "permission_denied",
            Self::RateLimit => "rate_limit",
            Self::RateLimited => "rate_limited",
            Self::Quota => "quota",
            Self::QuotaExceeded => "quota_exceeded",
            Self::SchemaRejected => "schema_rejected",
            Self::BadToolArguments => "bad_tool_arguments",
            Self::TruncatedToolArguments => "truncated_tool_arguments",
            Self::ContextOverflow => "context_overflow",
            Self::TransientUpstream => "transient_upstream",
            Self::PermanentUpstream => "permanent_upstream",
            Self::ContextWindowExceeded => "context_window_exceeded",
            Self::ContentPolicyBlocked => "content_policy_blocked",
            Self::MalformedResponse => "malformed_response",
            Self::MalformedStream => "malformed_stream",
            Self::EmptyOutput => "empty_output",
            Self::PrematureFinal => "premature_final",
            Self::PayloadTooLarge => "payload_too_large",
            Self::ProviderUnavailable => "provider_unavailable",
            Self::NetworkUnavailable => "network_unavailable",
            Self::ProviderTimeout => "provider_timeout",
            Self::UnsupportedMultimodal => "unsupported_multimodal",
        }
    }
}

/// Action the runtime recommends in response to a classified failure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureAction {
    Retry,
    RotateCredential,
    ProviderFailover,
    ReevaluateBudget,
    UserActionRequired,
    FailClosedNoRetry,
}

impl ProviderFailureAction {
    /// Returns the canonical snake_case identifier used in failure snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::RotateCredential => "rotate_credential",
            Self::ProviderFailover => "provider_failover",
            Self::ReevaluateBudget => "budget_re_evaluate",
            Self::UserActionRequired => "user_action_required",
            Self::FailClosedNoRetry => "fail_closed_no_retry",
        }
    }
}

/// Coarse failure category in the recovery-plan taxonomy shown to operators.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureCategory {
    Transient,
    RateLimit,
    Quota,
    Auth,
    Policy,
    ContextOverflow,
    MalformedResponse,
    SafetyStop,
    ProviderBug,
}

impl ProviderFailureCategory {
    /// Returns the canonical snake_case identifier used in recovery plans.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::RateLimit => "rate_limit",
            Self::Quota => "quota",
            Self::Auth => "auth",
            Self::Policy => "policy",
            Self::ContextOverflow => "context_overflow",
            Self::MalformedResponse => "malformed_response",
            Self::SafetyStop => "safety_stop",
            Self::ProviderBug => "provider_bug",
        }
    }
}

/// Recovery step suggested to the orchestrator/operator for a failure
/// category.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRecoveryAction {
    RetrySame,
    RetryAfter,
    RetryTransformed,
    FallbackModel,
    CompactAndRetry,
    AskUser,
    Abort,
    FailClosed,
}

impl ProviderRecoveryAction {
    /// Returns the canonical snake_case identifier used in recovery plans.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetrySame => "retry_same",
            Self::RetryAfter => "retry_after",
            Self::RetryTransformed => "retry_transformed",
            Self::FallbackModel => "fallback_model",
            Self::CompactAndRetry => "compact_and_retry",
            Self::AskUser => "ask_user",
            Self::Abort => "abort",
            Self::FailClosed => "fail_closed",
        }
    }
}

/// Serialized recovery plan: failure category, suggested action, and an
/// optional retry delay.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderRecoveryPlanSnapshot {
    pub category: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// Typed failure classification: class, recommended action, originating HTTP
/// status, and an internal trace detail keyed by call site.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderFailureClassification {
    pub class: ProviderFailureClass,
    pub recommended_action: ProviderFailureAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ProviderFailureClassification {
    /// Creates a classification from its parts.
    #[must_use]
    pub const fn new(
        class: ProviderFailureClass,
        recommended_action: ProviderFailureAction,
        status_code: Option<u16>,
        provider_detail: Option<String>,
    ) -> Self {
        Self { class, recommended_action, status_code, provider_detail, retry_after_ms: None }
    }

    /// Returns this classification with a provider-supplied retry delay.
    #[must_use]
    pub const fn with_retry_after_ms(mut self, retry_after_ms: Option<u64>) -> Self {
        self.retry_after_ms = retry_after_ms;
        self
    }

    /// Renders this classification into a serializable snapshot carrying
    /// `message`; the recovery plan is derived from the failure class.
    #[must_use]
    pub fn snapshot(&self, message: String) -> ProviderFailureSnapshot {
        let mut recovery = provider_recovery_plan(self);
        if self.retry_after_ms.is_some() {
            recovery.retry_after_ms = self.retry_after_ms;
        }
        ProviderFailureSnapshot {
            class: self.class.as_str().to_owned(),
            recommended_action: self.recommended_action.as_str().to_owned(),
            recovery,
            status_code: self.status_code,
            provider_detail: self.provider_detail.clone(),
            message,
        }
    }
}

/// Serializable failure record stored in runtime metrics and journals; the
/// message must already be safe to persist.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderFailureSnapshot {
    pub class: String,
    pub recommended_action: String,
    pub recovery: ProviderRecoveryPlanSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_detail: Option<String>,
    pub message: String,
}

impl ProviderFailureSnapshot {
    /// Sets the recovery plan retry delay (used for circuit-open failures).
    #[must_use]
    pub fn with_retry_after_ms(mut self, retry_after_ms: Option<u64>) -> Self {
        self.recovery.retry_after_ms = retry_after_ms;
        self
    }
}

/// Provider-neutral retry policy derived from a recovery decision.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderRetryPolicy {
    /// Maximum additional provider attempts allowed by this policy.
    pub max_attempts: u32,
    /// Whether the retry must first transform the provider request payload.
    pub requires_transformed_request: bool,
    /// Whether the retry must compact context before sending a new request.
    pub requires_context_compaction: bool,
    /// Whether the retry must switch to an authorized fallback provider.
    pub requires_provider_failover: bool,
    /// Whether the retry must refresh credentials through the auth/vault boundary.
    pub requires_credential_refresh: bool,
}

impl ProviderRetryPolicy {
    /// Builds a retry policy for the recovery decision selected by the classifier.
    #[must_use]
    pub const fn for_decision(decision: ProviderRecoveryDecisionKind) -> Self {
        match decision {
            ProviderRecoveryDecisionKind::RetrySameProvider
            | ProviderRecoveryDecisionKind::RetryAfter => Self {
                max_attempts: 1,
                requires_transformed_request: false,
                requires_context_compaction: false,
                requires_provider_failover: false,
                requires_credential_refresh: false,
            },
            ProviderRecoveryDecisionKind::RetryTransformed => Self {
                max_attempts: 1,
                requires_transformed_request: true,
                requires_context_compaction: false,
                requires_provider_failover: false,
                requires_credential_refresh: false,
            },
            ProviderRecoveryDecisionKind::CompactAndRetry => Self {
                max_attempts: 1,
                requires_transformed_request: false,
                requires_context_compaction: true,
                requires_provider_failover: false,
                requires_credential_refresh: false,
            },
            ProviderRecoveryDecisionKind::FailoverProvider => Self {
                max_attempts: 1,
                requires_transformed_request: false,
                requires_context_compaction: false,
                requires_provider_failover: true,
                requires_credential_refresh: false,
            },
            ProviderRecoveryDecisionKind::RefreshCredential => Self {
                max_attempts: 1,
                requires_transformed_request: false,
                requires_context_compaction: false,
                requires_provider_failover: false,
                requires_credential_refresh: true,
            },
            ProviderRecoveryDecisionKind::AskUser
            | ProviderRecoveryDecisionKind::Abort
            | ProviderRecoveryDecisionKind::FailClosed => Self {
                max_attempts: 0,
                requires_transformed_request: false,
                requires_context_compaction: false,
                requires_provider_failover: false,
                requires_credential_refresh: false,
            },
        }
    }
}

/// Central provider failure classifier used by runtime code and fixture validation.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProviderFailureClassifier;

impl ProviderFailureClassifier {
    /// Creates a provider failure classifier with the built-in taxonomy.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Classifies an HTTP provider failure by status and redacted response body.
    #[must_use]
    pub fn classify_http_failure(
        self,
        status_code: u16,
        retryable: bool,
        provider_detail: &str,
        response_body: &str,
    ) -> ProviderFailureClassification {
        classify_http_provider_failure(status_code, retryable, provider_detail, response_body)
    }

    /// Classifies a provider compatibility fixture category.
    #[must_use]
    pub fn classify_fixture_category(
        self,
        category: &str,
        status_code: Option<u16>,
        provider_detail: &str,
    ) -> ProviderFailureClassification {
        let (class, action) = match category {
            "truncated_tool_args" => (
                ProviderFailureClass::TruncatedToolArguments,
                ProviderFailureAction::FailClosedNoRetry,
            ),
            "invalid_json_arguments" | "invalid_tool_name" => {
                (ProviderFailureClass::BadToolArguments, ProviderFailureAction::FailClosedNoRetry)
            }
            "empty_final_answer" => {
                (ProviderFailureClass::EmptyOutput, ProviderFailureAction::Retry)
            }
            "context_overflow" => {
                (ProviderFailureClass::ContextOverflow, ProviderFailureAction::ReevaluateBudget)
            }
            "rate_limit" => (ProviderFailureClass::RateLimit, ProviderFailureAction::Retry),
            "quota" => (ProviderFailureClass::Quota, ProviderFailureAction::UserActionRequired),
            "auth_expired" => {
                (ProviderFailureClass::AuthExpired, ProviderFailureAction::RotateCredential)
            }
            "unsupported_schema" => {
                (ProviderFailureClass::SchemaRejected, ProviderFailureAction::FailClosedNoRetry)
            }
            "malformed_sse_chunk" => {
                (ProviderFailureClass::MalformedStream, ProviderFailureAction::Retry)
            }
            "partial_tool_call" => (
                ProviderFailureClass::TruncatedToolArguments,
                ProviderFailureAction::FailClosedNoRetry,
            ),
            "unicode_surrogate" => {
                (ProviderFailureClass::MalformedStream, ProviderFailureAction::Retry)
            }
            "unsupported_multimodal" => {
                (ProviderFailureClass::UnsupportedMultimodal, ProviderFailureAction::Retry)
            }
            "tool_result_too_large" => {
                (ProviderFailureClass::PayloadTooLarge, ProviderFailureAction::FailClosedNoRetry)
            }
            "premature_final_after_patch" => {
                (ProviderFailureClass::PrematureFinal, ProviderFailureAction::FailClosedNoRetry)
            }
            _ => {
                (ProviderFailureClass::PermanentUpstream, ProviderFailureAction::FailClosedNoRetry)
            }
        };
        provider_failure_classification(class, action, status_code, provider_detail)
    }
}

fn provider_recovery_plan(
    classification: &ProviderFailureClassification,
) -> ProviderRecoveryPlanSnapshot {
    let (category, action) = match classification.class {
        ProviderFailureClass::AuthInvalid | ProviderFailureClass::AuthExpired => {
            (ProviderFailureCategory::Auth, ProviderRecoveryAction::AskUser)
        }
        ProviderFailureClass::PermissionDenied => {
            (ProviderFailureCategory::Policy, ProviderRecoveryAction::AskUser)
        }
        ProviderFailureClass::RateLimit | ProviderFailureClass::RateLimited => {
            (ProviderFailureCategory::RateLimit, ProviderRecoveryAction::RetryAfter)
        }
        ProviderFailureClass::Quota | ProviderFailureClass::QuotaExceeded => {
            (ProviderFailureCategory::Quota, ProviderRecoveryAction::AskUser)
        }
        ProviderFailureClass::ContextOverflow | ProviderFailureClass::ContextWindowExceeded => {
            (ProviderFailureCategory::ContextOverflow, ProviderRecoveryAction::CompactAndRetry)
        }
        ProviderFailureClass::SchemaRejected
        | ProviderFailureClass::BadToolArguments
        | ProviderFailureClass::TruncatedToolArguments
        | ProviderFailureClass::PayloadTooLarge
        | ProviderFailureClass::PrematureFinal => {
            (ProviderFailureCategory::MalformedResponse, ProviderRecoveryAction::FailClosed)
        }
        ProviderFailureClass::MalformedStream | ProviderFailureClass::EmptyOutput => {
            (ProviderFailureCategory::MalformedResponse, ProviderRecoveryAction::RetrySame)
        }
        ProviderFailureClass::UnsupportedMultimodal
            if classification.recommended_action == ProviderFailureAction::FailClosedNoRetry =>
        {
            (ProviderFailureCategory::MalformedResponse, ProviderRecoveryAction::FailClosed)
        }
        ProviderFailureClass::UnsupportedMultimodal => {
            (ProviderFailureCategory::MalformedResponse, ProviderRecoveryAction::RetryTransformed)
        }
        ProviderFailureClass::ContentPolicyBlocked => {
            (ProviderFailureCategory::SafetyStop, ProviderRecoveryAction::Abort)
        }
        ProviderFailureClass::MalformedResponse => {
            (ProviderFailureCategory::MalformedResponse, ProviderRecoveryAction::FallbackModel)
        }
        ProviderFailureClass::ProviderUnavailable
        | ProviderFailureClass::NetworkUnavailable
        | ProviderFailureClass::ProviderTimeout => {
            (ProviderFailureCategory::Transient, ProviderRecoveryAction::RetrySame)
        }
        ProviderFailureClass::TransientUpstream => {
            let action = if classification
                .provider_detail
                .as_deref()
                .is_some_and(|detail| detail.contains("circuit_open"))
            {
                ProviderRecoveryAction::RetryAfter
            } else {
                ProviderRecoveryAction::RetrySame
            };
            (ProviderFailureCategory::Transient, action)
        }
        ProviderFailureClass::PermanentUpstream => {
            (ProviderFailureCategory::ProviderBug, ProviderRecoveryAction::FallbackModel)
        }
    };
    ProviderRecoveryPlanSnapshot {
        category: category.as_str().to_owned(),
        action: action.as_str().to_owned(),
        retry_after_ms: None,
    }
}

/// Builds a failure classification from explicit class/action values.
#[must_use]
pub fn provider_failure_classification(
    class: ProviderFailureClass,
    recommended_action: ProviderFailureAction,
    status_code: Option<u16>,
    provider_detail: impl Into<String>,
) -> ProviderFailureClassification {
    ProviderFailureClassification::new(
        class,
        recommended_action,
        status_code,
        Some(provider_detail.into()),
    )
}

/// Classifies a failure that must fail closed without retrying.
#[must_use]
pub fn fail_closed_provider_classification(
    provider_detail: impl Into<String>,
) -> ProviderFailureClassification {
    provider_failure_classification(
        ProviderFailureClass::PermanentUpstream,
        ProviderFailureAction::FailClosedNoRetry,
        None,
        provider_detail,
    )
}

/// Classifies a failure that should move to a fallback provider when possible.
#[must_use]
pub fn failover_provider_classification(
    provider_detail: impl Into<String>,
) -> ProviderFailureClassification {
    provider_failure_classification(
        ProviderFailureClass::PermanentUpstream,
        ProviderFailureAction::ProviderFailover,
        None,
        provider_detail,
    )
}

/// Classifies a transient provider failure that can retry the same provider.
#[must_use]
pub fn retry_provider_classification(
    provider_detail: impl Into<String>,
) -> ProviderFailureClassification {
    provider_failure_classification(
        ProviderFailureClass::TransientUpstream,
        ProviderFailureAction::Retry,
        None,
        provider_detail,
    )
}

/// Classifies a provider failure that requires operator or user action.
#[must_use]
pub fn user_action_provider_classification(
    provider_detail: impl Into<String>,
) -> ProviderFailureClassification {
    provider_failure_classification(
        ProviderFailureClass::PermanentUpstream,
        ProviderFailureAction::UserActionRequired,
        None,
        provider_detail,
    )
}

/// Detects provider-probe vault availability failures from sanitized diagnostic text.
///
/// The CLI and daemon can usually classify typed vault errors before text is
/// generated, but multi-candidate auth-profile probes aggregate per-root errors
/// into a redacted string. This matcher intentionally covers only availability
/// failures; ordinary `secret not found` remains a missing credential.
#[must_use]
pub fn provider_probe_message_indicates_vault_unavailable(message: &str) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return false;
    }
    normalized.contains("vault i/o failure")
        || normalized.contains("vault backend unavailable")
        || normalized.contains("failed to acquire metadata lock")
        || normalized.contains("timed out waiting for metadata lock")
        || normalized.contains("metadata.lock: access is denied")
        || (normalized.contains("metadata.lock") && normalized.contains("os error 5"))
}

/// Classifies an HTTP provider failure by status and response body.
#[must_use]
pub fn classify_http_provider_failure(
    status_code: u16,
    retryable: bool,
    provider_detail: &str,
    response_body: &str,
) -> ProviderFailureClassification {
    let normalized_body = response_body.to_ascii_lowercase();
    let (class, recommended_action) = if status_code == 401 {
        if normalized_body.contains("expired") {
            (ProviderFailureClass::AuthExpired, ProviderFailureAction::RotateCredential)
        } else {
            (ProviderFailureClass::AuthInvalid, ProviderFailureAction::RotateCredential)
        }
    } else if status_code == 402
        || normalized_body.contains("insufficient_quota")
        || normalized_body.contains("quota")
        || normalized_body.contains("billing")
        || normalized_body.contains("credits")
        || normalized_body.contains("plan usage limit")
        || normalized_body.contains("token plan usage limit")
        || normalized_body.contains("usage limit reached")
    {
        (ProviderFailureClass::Quota, ProviderFailureAction::UserActionRequired)
    } else if status_code == 429 {
        (ProviderFailureClass::RateLimit, ProviderFailureAction::Retry)
    } else if matches!(status_code, 400 | 422)
        && provider_body_indicates_unsupported_multimodal(normalized_body.as_str())
    {
        (ProviderFailureClass::UnsupportedMultimodal, ProviderFailureAction::Retry)
    } else if matches!(status_code, 400 | 422)
        && (normalized_body.contains("json_schema")
            || normalized_body.contains("response_format")
            || normalized_body.contains("schema")
            || normalized_body.contains("unsupported"))
    {
        (ProviderFailureClass::SchemaRejected, ProviderFailureAction::FailClosedNoRetry)
    } else if matches!(status_code, 400 | 413)
        && (normalized_body.contains("context")
            || normalized_body.contains("maximum context")
            || normalized_body.contains("context_length")
            || normalized_body.contains("token limit")
            || normalized_body.contains("too many tokens"))
    {
        (ProviderFailureClass::ContextOverflow, ProviderFailureAction::ReevaluateBudget)
    } else if status_code == 413
        || normalized_body.contains("payload too large")
        || normalized_body.contains("request body too large")
        || normalized_body.contains("tool result payload too large")
    {
        (ProviderFailureClass::PayloadTooLarge, ProviderFailureAction::FailClosedNoRetry)
    } else if normalized_body.contains("policy")
        || normalized_body.contains("safety")
        || normalized_body.contains("moderation")
        || normalized_body.contains("blocked")
    {
        (ProviderFailureClass::ContentPolicyBlocked, ProviderFailureAction::FailClosedNoRetry)
    } else if status_code == 403 {
        (ProviderFailureClass::PermissionDenied, ProviderFailureAction::UserActionRequired)
    } else if matches!(status_code, 502..=504)
        || normalized_body.contains("provider unavailable")
        || normalized_body.contains("service unavailable")
        || normalized_body.contains("upstream unavailable")
    {
        (ProviderFailureClass::ProviderUnavailable, ProviderFailureAction::Retry)
    } else if retryable {
        (ProviderFailureClass::TransientUpstream, ProviderFailureAction::Retry)
    } else {
        (ProviderFailureClass::PermanentUpstream, ProviderFailureAction::ProviderFailover)
    };
    provider_failure_classification(class, recommended_action, Some(status_code), provider_detail)
}

fn provider_body_indicates_unsupported_multimodal(normalized_body: &str) -> bool {
    normalized_body.contains("vision_unsupported")
        || normalized_body.contains("unsupported multimodal")
        || normalized_body.contains("multimodal input is not supported")
        || normalized_body.contains("image input is not supported")
        || normalized_body.contains("images are not supported")
        || (normalized_body.contains("unsupported")
            && (normalized_body.contains("image")
                || normalized_body.contains("vision")
                || normalized_body.contains("multimodal")))
}

/// Classifies a transport failure as network-unavailable.
#[must_use]
pub fn classify_network_provider_failure(provider_detail: &str) -> ProviderFailureClassification {
    provider_failure_classification(
        ProviderFailureClass::NetworkUnavailable,
        ProviderFailureAction::Retry,
        None,
        provider_detail,
    )
}

/// Classifies a transport failure, preserving timeout-specific recovery.
#[must_use]
pub fn classify_transport_provider_failure(
    provider_detail: &str,
    is_timeout: bool,
) -> ProviderFailureClassification {
    if is_timeout {
        return provider_failure_classification(
            ProviderFailureClass::ProviderTimeout,
            ProviderFailureAction::Retry,
            None,
            format!("{provider_detail}:timeout"),
        );
    }
    classify_network_provider_failure(provider_detail)
}

/// Classifies a `reqwest` transport failure.
#[must_use]
pub fn classify_reqwest_provider_failure(
    provider_detail: &str,
    error: &reqwest::Error,
) -> ProviderFailureClassification {
    classify_transport_provider_failure(provider_detail, error.is_timeout())
}

/// Classifies a malformed provider response that should not be retried.
#[must_use]
pub fn invalid_response_classification(provider_detail: &str) -> ProviderFailureClassification {
    provider_failure_classification(
        ProviderFailureClass::MalformedResponse,
        ProviderFailureAction::FailClosedNoRetry,
        None,
        provider_detail,
    )
}

/// Classifies a malformed provider response that can be retried.
#[must_use]
pub fn retryable_invalid_response_classification(
    provider_detail: &str,
) -> ProviderFailureClassification {
    provider_failure_classification(
        ProviderFailureClass::MalformedResponse,
        ProviderFailureAction::Retry,
        None,
        provider_detail,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_probe_vault_unavailable_matches_lock_errors() {
        assert!(provider_probe_message_indicates_vault_unavailable(
            "failed to load auth profile secret from candidate vault roots: C:\\state\\vault \
             (vault I/O failure: timed out waiting for metadata lock \
             C:\\state\\vault\\metadata.lock); C:\\state\\runtime\\vault (secret not found)"
        ));
        assert!(provider_probe_message_indicates_vault_unavailable(
            "failed to acquire metadata lock C:\\state\\vault\\metadata.lock: access is denied"
        ));
        assert!(provider_probe_message_indicates_vault_unavailable(
            "failed to acquire metadata lock C:\\state\\vault\\metadata.lock (os error 5)"
        ));
    }

    #[test]
    fn provider_probe_vault_unavailable_rejects_missing_secret() {
        assert!(!provider_probe_message_indicates_vault_unavailable("secret not found"));
        assert!(!provider_probe_message_indicates_vault_unavailable(
            "failed to load vault secret 'openai_access': secret not found"
        ));
    }

    #[test]
    fn unsupported_multimodal_uses_transformed_recovery() {
        let classification = classify_http_provider_failure(
            400,
            false,
            "openai_chat_http",
            r#"{"error":{"message":"image input is not supported by this model"}}"#,
        );
        let snapshot = classification.snapshot("image input is not supported".to_owned());

        assert_eq!(classification.class, ProviderFailureClass::UnsupportedMultimodal);
        assert_eq!(snapshot.recovery.action, "retry_transformed");
    }
}
