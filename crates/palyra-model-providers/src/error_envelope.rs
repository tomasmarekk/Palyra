//! Stable, redacted provider error envelope.
//!
//! Translates an internal [`ProviderError`] plus its failure classification
//! into the serialized shape consumed by console, journal, and channel
//! surfaces: a coarse kind/severity/retryability triple, a failover
//! eligibility flag, and a message scrubbed of credential material. The
//! serde shape is a published contract; extend it additively only.
use serde::{Deserialize, Serialize};

use crate::{sanitize_remote_error, ProviderError, ProviderFailureClass, ProviderFailureSnapshot};

/// Tape/journal event type for audited provider recovery decisions.
pub const PROVIDER_RECOVERY_DECISION_EVENT: &str = "provider.recovery.decision";

/// Coarse error category exposed to envelope consumers.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    Auth,
    Quota,
    RateLimit,
    TransientNetwork,
    MalformedResponse,
    ProviderPolicy,
    Timeout,
    UnsupportedFeature,
    CircuitOpen,
    MissingConfiguration,
    Internal,
}

/// How damaging the failure is for the current run, from recoverable
/// (retry/failover possible) to fatal (operator action required).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorSeverity {
    Recoverable,
    Degraded,
    Fatal,
}

/// Whether and how the failed operation may be retried.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRetryability {
    NotRetryable,
    RetrySameProvider,
    RetryAfter,
    RefreshCredential,
}

/// Provider-neutral recovery action selected after classifying a provider failure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRecoveryDecisionKind {
    RetrySameProvider,
    RetryAfter,
    RetryTransformed,
    RefreshCredential,
    FailoverProvider,
    CompactAndRetry,
    AskUser,
    Abort,
    FailClosed,
}

impl ProviderRecoveryDecisionKind {
    /// Returns the stable reason-code suffix for audit payloads.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetrySameProvider => "retry_same_provider",
            Self::RetryAfter => "retry_after",
            Self::RetryTransformed => "retry_transformed",
            Self::RefreshCredential => "refresh_credential",
            Self::FailoverProvider => "failover_provider",
            Self::CompactAndRetry => "compact_and_retry",
            Self::AskUser => "ask_user",
            Self::Abort => "abort",
            Self::FailClosed => "fail_closed",
        }
    }
}

/// Redacted, serializable recovery decision derived from a provider error.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderRecoveryDecision {
    pub schema_version: u32,
    pub event_type: String,
    pub decision: ProviderRecoveryDecisionKind,
    pub reason_code: String,
    pub retry_after_ms: Option<u64>,
    pub failover_eligible: bool,
    pub redaction_level: String,
    pub redacted_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_trace_ref: Option<String>,
    pub classification: ProviderFailureSnapshot,
}

/// Serialized provider failure surfaced outside the daemon core.
///
/// `redacted_message` has passed credential scrubbing and is safe to log,
/// persist, and show to operators; the raw upstream body is never carried.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderErrorEnvelope {
    pub kind: ProviderErrorKind,
    pub severity: ProviderErrorSeverity,
    pub retryability: ProviderRetryability,
    pub failover_eligible: bool,
    pub redacted_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_trace_ref: Option<String>,
    pub classification: ProviderFailureSnapshot,
    pub recovery_decision: ProviderRecoveryDecision,
}

impl ProviderErrorEnvelope {
    /// Builds the envelope for `error`, deriving kind, retryability,
    /// failover eligibility, and severity from its failure classification
    /// and redacting the message for safe exposure.
    #[must_use]
    pub fn from_error(error: &ProviderError) -> Self {
        let classification = error.failure_snapshot();
        let kind = provider_error_kind(error, &classification);
        let retryability = provider_retryability(error, &classification);
        let malformed_failover_eligible =
            matches!(classification.class.as_str(), "malformed_response");
        let failover_eligible = matches!(
            kind,
            ProviderErrorKind::RateLimit
                | ProviderErrorKind::TransientNetwork
                | ProviderErrorKind::Timeout
        ) || (matches!(kind, ProviderErrorKind::MalformedResponse)
            && malformed_failover_eligible)
            || matches!(classification.recommended_action.as_str(), "provider_failover");
        // Severity is derived, not stored: anything retryable or failover
        // eligible is recoverable by definition; malformed/internal failures
        // degrade the run; everything else needs operator action.
        let severity = if failover_eligible || retryability != ProviderRetryability::NotRetryable {
            ProviderErrorSeverity::Recoverable
        } else if matches!(kind, ProviderErrorKind::MalformedResponse | ProviderErrorKind::Internal)
        {
            ProviderErrorSeverity::Degraded
        } else {
            ProviderErrorSeverity::Fatal
        };
        let redacted_message = sanitize_remote_error(classification.message.as_str());
        let mut redacted_classification = classification.clone();
        redacted_classification.message = redacted_message.clone();
        let recovery_decision = provider_recovery_decision(
            kind,
            retryability,
            failover_eligible,
            &classification,
            redacted_message.as_str(),
        );
        Self {
            kind,
            severity,
            retryability,
            failover_eligible,
            redacted_message,
            provider_trace_ref: classification.provider_detail.clone(),
            classification: redacted_classification,
            recovery_decision,
        }
    }
}

fn provider_recovery_decision(
    kind: ProviderErrorKind,
    retryability: ProviderRetryability,
    failover_eligible: bool,
    classification: &ProviderFailureSnapshot,
    redacted_message: &str,
) -> ProviderRecoveryDecision {
    let decision = provider_recovery_decision_kind(
        kind,
        retryability,
        failover_eligible,
        classification.recovery.action.as_str(),
    );
    let mut redacted_classification = classification.clone();
    redacted_classification.message = redacted_message.to_owned();
    ProviderRecoveryDecision {
        schema_version: 1,
        event_type: PROVIDER_RECOVERY_DECISION_EVENT.to_owned(),
        decision,
        reason_code: format!("provider.recovery.{}", decision.as_str()),
        retry_after_ms: classification.recovery.retry_after_ms,
        failover_eligible,
        redaction_level: "redacted_provider_error".to_owned(),
        redacted_message: redacted_message.to_owned(),
        provider_trace_ref: classification.provider_detail.clone(),
        classification: redacted_classification,
    }
}

fn provider_recovery_decision_kind(
    kind: ProviderErrorKind,
    retryability: ProviderRetryability,
    failover_eligible: bool,
    recovery_action: &str,
) -> ProviderRecoveryDecisionKind {
    match retryability {
        ProviderRetryability::RetryAfter => ProviderRecoveryDecisionKind::RetryAfter,
        ProviderRetryability::RetrySameProvider => ProviderRecoveryDecisionKind::RetrySameProvider,
        ProviderRetryability::RefreshCredential => ProviderRecoveryDecisionKind::RefreshCredential,
        ProviderRetryability::NotRetryable => match recovery_action {
            "retry_transformed" => ProviderRecoveryDecisionKind::RetryTransformed,
            "compact_and_retry" => ProviderRecoveryDecisionKind::CompactAndRetry,
            "ask_user" => ProviderRecoveryDecisionKind::AskUser,
            "abort" => ProviderRecoveryDecisionKind::Abort,
            _ if failover_eligible => ProviderRecoveryDecisionKind::FailoverProvider,
            _ if matches!(
                kind,
                ProviderErrorKind::Quota
                    | ProviderErrorKind::Auth
                    | ProviderErrorKind::ProviderPolicy
                    | ProviderErrorKind::MissingConfiguration
                    | ProviderErrorKind::UnsupportedFeature
            ) =>
            {
                ProviderRecoveryDecisionKind::AskUser
            }
            _ => ProviderRecoveryDecisionKind::FailClosed,
        },
    }
}

fn provider_error_kind(
    error: &ProviderError,
    classification: &ProviderFailureSnapshot,
) -> ProviderErrorKind {
    match error {
        ProviderError::CircuitOpen { .. } => ProviderErrorKind::CircuitOpen,
        ProviderError::MissingApiKey
        | ProviderError::MissingAnthropicApiKey
        | ProviderError::MissingEmbeddingsModel => ProviderErrorKind::MissingConfiguration,
        ProviderError::VisionUnsupported { .. } => ProviderErrorKind::UnsupportedFeature,
        ProviderError::InvalidEmbeddingsRequest { .. } => ProviderErrorKind::MalformedResponse,
        ProviderError::StatePoisoned => ProviderErrorKind::Internal,
        ProviderError::RequestFailed { .. } | ProviderError::InvalidResponse { .. } => {
            match classification.class.as_str() {
                "auth_invalid" | "auth_expired" | "permission_denied" => ProviderErrorKind::Auth,
                "quota" | "quota_exceeded" => ProviderErrorKind::Quota,
                "rate_limit" | "rate_limited" => ProviderErrorKind::RateLimit,
                "network_unavailable" | "provider_unavailable" => {
                    ProviderErrorKind::TransientNetwork
                }
                "provider_timeout" => ProviderErrorKind::Timeout,
                "schema_rejected"
                | "bad_tool_arguments"
                | "truncated_tool_arguments"
                | "context_overflow"
                | "malformed_response"
                | "malformed_stream"
                | "empty_output"
                | "premature_final"
                | "payload_too_large" => ProviderErrorKind::MalformedResponse,
                "context_window_exceeded" => ProviderErrorKind::MalformedResponse,
                "content_policy_blocked" => ProviderErrorKind::ProviderPolicy,
                "transient_upstream" => ProviderErrorKind::TransientNetwork,
                _ => ProviderErrorKind::Internal,
            }
        }
    }
}

fn provider_retryability(
    error: &ProviderError,
    classification: &ProviderFailureSnapshot,
) -> ProviderRetryability {
    match error {
        ProviderError::CircuitOpen { .. } => ProviderRetryability::RetryAfter,
        ProviderError::MissingApiKey | ProviderError::MissingAnthropicApiKey => {
            ProviderRetryability::RefreshCredential
        }
        ProviderError::RequestFailed { classification, .. }
            if matches!(classification.class, ProviderFailureClass::AuthExpired) =>
        {
            ProviderRetryability::RefreshCredential
        }
        ProviderError::RequestFailed { retryable: true, .. } => {
            if classification.recovery.retry_after_ms.is_some()
                || matches!(classification.class.as_str(), "rate_limit" | "rate_limited")
            {
                ProviderRetryability::RetryAfter
            } else {
                ProviderRetryability::RetrySameProvider
            }
        }
        _ => ProviderRetryability::NotRetryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        classify_http_provider_failure, ProviderError, ProviderFailureClass,
        ProviderFailureSnapshot,
    };

    #[test]
    fn recovery_decision_redacts_raw_provider_message() {
        let error = ProviderError::RequestFailed {
            message: "upstream failed with sk-secret-token".to_owned(),
            retryable: true,
            retry_count: 1,
            classification: classify_http_provider_failure(
                429,
                true,
                "openai_chat_http",
                "rate limit",
            ),
        };

        let envelope = ProviderErrorEnvelope::from_error(&error);

        assert_eq!(envelope.recovery_decision.event_type, PROVIDER_RECOVERY_DECISION_EVENT);
        assert_eq!(envelope.recovery_decision.decision, ProviderRecoveryDecisionKind::RetryAfter);
        assert_eq!(envelope.recovery_decision.reason_code, "provider.recovery.retry_after");
        assert!(!envelope.recovery_decision.redacted_message.contains("sk-secret-token"));
        assert!(!envelope.recovery_decision.classification.message.contains("sk-secret-token"));
    }

    #[test]
    fn provider_auth_error_envelope_redacts_credential_material() {
        let error = ProviderError::RequestFailed {
            message: "401 unauthorized bearer live-token api_key=sk-secret-token".to_owned(),
            retryable: false,
            retry_count: 0,
            classification: classify_http_provider_failure(
                401,
                false,
                "openai_chat_http",
                "invalid api key",
            ),
        };

        let envelope = ProviderErrorEnvelope::from_error(&error);

        assert_eq!(envelope.kind, ProviderErrorKind::Auth);
        assert_eq!(envelope.retryability, ProviderRetryability::NotRetryable);
        assert_eq!(envelope.recovery_decision.decision, ProviderRecoveryDecisionKind::AskUser);
        assert!(!envelope.redacted_message.contains("live-token"));
        assert!(!envelope.redacted_message.contains("sk-secret-token"));
        assert!(!envelope.classification.message.contains("live-token"));
        assert!(!envelope.recovery_decision.redacted_message.contains("sk-secret-token"));
    }

    #[test]
    fn recovery_decision_maps_context_overflow_to_compaction() {
        let classification = classify_http_provider_failure(
            400,
            false,
            "openai_chat_http",
            "maximum context length exceeded",
        );
        let snapshot: ProviderFailureSnapshot =
            classification.snapshot("context exceeded".to_owned());
        let decision = provider_recovery_decision(
            ProviderErrorKind::MalformedResponse,
            ProviderRetryability::NotRetryable,
            false,
            &snapshot,
            "context exceeded",
        );

        assert_eq!(snapshot.class, ProviderFailureClass::ContextOverflow.as_str());
        assert_eq!(decision.decision, ProviderRecoveryDecisionKind::CompactAndRetry);
        assert_eq!(decision.reason_code, "provider.recovery.compact_and_retry");
    }
}
