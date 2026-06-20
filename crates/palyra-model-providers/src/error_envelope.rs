//! Stable, redacted provider error envelope.
//!
//! Translates an internal [`ProviderError`] plus its failure classification
//! into the serialized shape consumed by console, journal, and channel
//! surfaces: a coarse kind/severity/retryability triple, a failover
//! eligibility flag, and a message scrubbed of credential material. The
//! serde shape is a published contract; extend it additively only.
use serde::{Deserialize, Serialize};

use crate::{sanitize_remote_error, ProviderError, ProviderFailureClass, ProviderFailureSnapshot};

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
        let failover_eligible =
            matches!(
                kind,
                ProviderErrorKind::RateLimit
                    | ProviderErrorKind::TransientNetwork
                    | ProviderErrorKind::Timeout
                    | ProviderErrorKind::MalformedResponse
            ) || matches!(classification.recommended_action.as_str(), "provider_failover");
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
        Self {
            kind,
            severity,
            retryability,
            failover_eligible,
            redacted_message: sanitize_remote_error(classification.message.as_str()),
            provider_trace_ref: classification.provider_detail.clone(),
            classification,
        }
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
                "quota_exceeded" => ProviderErrorKind::Quota,
                "rate_limited" => ProviderErrorKind::RateLimit,
                "network_unavailable" => ProviderErrorKind::TransientNetwork,
                "provider_timeout" => ProviderErrorKind::Timeout,
                "malformed_response" => ProviderErrorKind::MalformedResponse,
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
        ProviderError::RequestFailed { retryable: true, .. } => {
            if classification.recovery.retry_after_ms.is_some()
                || classification.class == ProviderFailureClass::RateLimited.as_str()
            {
                ProviderRetryability::RetryAfter
            } else {
                ProviderRetryability::RetrySameProvider
            }
        }
        _ => ProviderRetryability::NotRetryable,
    }
}
