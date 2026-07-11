//! Stable, redacted provider error envelope.
//!
//! Translates an internal [`ProviderError`] plus its failure classification
//! into the serialized shape consumed by console, journal, and channel
//! surfaces: a coarse kind/severity/retryability triple, a failover
//! eligibility flag, and a message scrubbed of credential material. The
//! serde shape is a published contract; extend it additively only.
use palyra_common::runtime_contracts::{
    RuntimeErrorClass, RuntimeErrorEnvelopeV1, RuntimeErrorEnvelopeV1Input, RuntimeErrorPhase,
    RuntimeErrorSecurityClass, RuntimeErrorUserVisibility, RuntimeErrorValidationError,
    RuntimeRetryability, RuntimeSubsystem,
};
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
        let transformed_retry_eligible =
            matches!(classification.recovery.action.as_str(), "retry_transformed");
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
        let severity = if failover_eligible
            || transformed_retry_eligible
            || retryability != ProviderRetryability::NotRetryable
        {
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

    /// Projects this typed provider failure into the shared runtime-error contract.
    ///
    /// `output_emitted` must describe externally visible model output observed before
    /// the provider failure. Provider retryability and recovery posture come from the
    /// typed fields on this envelope and never from `redacted_message` wording.
    ///
    /// # Errors
    /// Returns [`RuntimeErrorValidationError`] if an existing provider reason code or
    /// sanitized presentation field violates the shared runtime contract.
    pub fn runtime_error_envelope(
        &self,
        output_emitted: bool,
    ) -> Result<RuntimeErrorEnvelopeV1, RuntimeErrorValidationError> {
        let decision = self.recovery_decision.decision;
        let retryability = provider_runtime_retryability(self.retryability, decision);
        let retryability = if output_emitted && retryability.allows_automatic_retry() {
            RuntimeRetryability::RequiresOperatorReview
        } else {
            retryability
        };
        RuntimeErrorEnvelopeV1::try_new(RuntimeErrorEnvelopeV1Input {
            class: provider_runtime_error_class(self.kind, self.retryability, decision),
            reason_code: self.recovery_decision.reason_code.clone(),
            subsystem: provider_runtime_subsystem(self.kind, decision),
            phase: provider_runtime_phase(decision),
            retryability,
            security_class: RuntimeErrorSecurityClass::Sensitive,
            user_visibility: provider_runtime_user_visibility(decision),
            output_emitted,
            side_effect_may_have_occurred: false,
            safe_message: self.redacted_message.clone(),
            recovery_hint: provider_runtime_recovery_hint(decision).to_owned(),
        })
    }
}

const fn provider_runtime_subsystem(
    kind: ProviderErrorKind,
    decision: ProviderRecoveryDecisionKind,
) -> RuntimeSubsystem {
    if matches!(kind, ProviderErrorKind::Auth | ProviderErrorKind::MissingConfiguration)
        || matches!(decision, ProviderRecoveryDecisionKind::RefreshCredential)
    {
        RuntimeSubsystem::Auth
    } else {
        RuntimeSubsystem::Provider
    }
}

const fn provider_runtime_phase(decision: ProviderRecoveryDecisionKind) -> RuntimeErrorPhase {
    if matches!(
        decision,
        ProviderRecoveryDecisionKind::Abort | ProviderRecoveryDecisionKind::FailClosed
    ) {
        RuntimeErrorPhase::ProviderFinalization
    } else {
        RuntimeErrorPhase::ProviderRecovery
    }
}

const fn provider_runtime_error_class(
    kind: ProviderErrorKind,
    retryability: ProviderRetryability,
    decision: ProviderRecoveryDecisionKind,
) -> RuntimeErrorClass {
    if matches!(kind, ProviderErrorKind::Auth | ProviderErrorKind::MissingConfiguration) {
        return RuntimeErrorClass::AuthUnavailable;
    }
    if !matches!(retryability, ProviderRetryability::NotRetryable)
        || matches!(
            decision,
            ProviderRecoveryDecisionKind::RetrySameProvider
                | ProviderRecoveryDecisionKind::RetryAfter
                | ProviderRecoveryDecisionKind::RetryTransformed
                | ProviderRecoveryDecisionKind::FailoverProvider
                | ProviderRecoveryDecisionKind::CompactAndRetry
        )
    {
        RuntimeErrorClass::ProviderRetryable
    } else {
        RuntimeErrorClass::ProviderTerminal
    }
}

const fn provider_runtime_retryability(
    retryability: ProviderRetryability,
    decision: ProviderRecoveryDecisionKind,
) -> RuntimeRetryability {
    match retryability {
        ProviderRetryability::RetrySameProvider => RuntimeRetryability::SafeSameRequest,
        ProviderRetryability::RetryAfter => RuntimeRetryability::SafeAfterBackoff,
        ProviderRetryability::RefreshCredential => RuntimeRetryability::RequiresCredentialRefresh,
        ProviderRetryability::NotRetryable => match decision {
            ProviderRecoveryDecisionKind::RetryTransformed => {
                RuntimeRetryability::RequiresRequestTransform
            }
            ProviderRecoveryDecisionKind::FailoverProvider => {
                RuntimeRetryability::RequiresProviderFailover
            }
            ProviderRecoveryDecisionKind::CompactAndRetry => {
                RuntimeRetryability::RequiresContextCompaction
            }
            ProviderRecoveryDecisionKind::AskUser => RuntimeRetryability::RequiresOperatorReview,
            ProviderRecoveryDecisionKind::RetrySameProvider
            | ProviderRecoveryDecisionKind::RetryAfter
            | ProviderRecoveryDecisionKind::RefreshCredential
            | ProviderRecoveryDecisionKind::Abort
            | ProviderRecoveryDecisionKind::FailClosed => RuntimeRetryability::NotRetryable,
        },
    }
}

const fn provider_runtime_user_visibility(
    decision: ProviderRecoveryDecisionKind,
) -> RuntimeErrorUserVisibility {
    match decision {
        ProviderRecoveryDecisionKind::AskUser | ProviderRecoveryDecisionKind::RefreshCredential => {
            RuntimeErrorUserVisibility::ActionRequired
        }
        ProviderRecoveryDecisionKind::Abort | ProviderRecoveryDecisionKind::FailClosed => {
            RuntimeErrorUserVisibility::SafeMessage
        }
        ProviderRecoveryDecisionKind::RetrySameProvider
        | ProviderRecoveryDecisionKind::RetryAfter
        | ProviderRecoveryDecisionKind::RetryTransformed
        | ProviderRecoveryDecisionKind::FailoverProvider
        | ProviderRecoveryDecisionKind::CompactAndRetry => RuntimeErrorUserVisibility::StatusOnly,
    }
}

const fn provider_runtime_recovery_hint(decision: ProviderRecoveryDecisionKind) -> &'static str {
    match decision {
        ProviderRecoveryDecisionKind::RetrySameProvider => {
            "retry the provider request within the configured retry budget"
        }
        ProviderRecoveryDecisionKind::RetryAfter => {
            "wait for the structured backoff interval before retrying"
        }
        ProviderRecoveryDecisionKind::RetryTransformed => {
            "transform the rejected provider request before retrying"
        }
        ProviderRecoveryDecisionKind::RefreshCredential => {
            "refresh the provider credential through the auth boundary"
        }
        ProviderRecoveryDecisionKind::FailoverProvider => {
            "select an eligible provider through the host-owned failover policy"
        }
        ProviderRecoveryDecisionKind::CompactAndRetry => {
            "compact provider context through the host-owned context boundary before retrying"
        }
        ProviderRecoveryDecisionKind::AskUser => {
            "request safe user or operator action before continuing"
        }
        ProviderRecoveryDecisionKind::Abort => {
            "stop the provider attempt and preserve its redacted evidence"
        }
        ProviderRecoveryDecisionKind::FailClosed => {
            "stop automatic recovery and inspect redacted provider diagnostics"
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
                "unsupported_multimodal" => ProviderErrorKind::UnsupportedFeature,
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
        ProviderError::RequestFailed { .. }
            if classification.recovery.action.as_str() == "retry_transformed" =>
        {
            ProviderRetryability::NotRetryable
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
    fn provider_retry_after_projects_from_typed_retryability() {
        let error = ProviderError::RequestFailed {
            message: "rate limited by provider".to_owned(),
            retryable: true,
            retry_count: 0,
            classification: classify_http_provider_failure(
                429,
                true,
                "openai_chat_http",
                "rate limit",
            ),
        };
        let provider = ProviderErrorEnvelope::from_error(&error);
        let runtime =
            provider.runtime_error_envelope(false).expect("typed provider failure should project");

        assert_eq!(runtime.class(), RuntimeErrorClass::ProviderRetryable);
        assert_eq!(runtime.retryability(), RuntimeRetryability::SafeAfterBackoff);
        assert_eq!(runtime.reason_code(), "provider.recovery.retry_after");
        assert!(!runtime.output_emitted());
        assert!(!runtime.side_effect_may_have_occurred());

        let partial = provider
            .runtime_error_envelope(true)
            .expect("partial output should project conservatively");
        assert_eq!(partial.retryability(), RuntimeRetryability::RequiresOperatorReview);
        assert!(!partial.to_palyra_error_envelope().retryable);
    }

    #[test]
    fn provider_runtime_projection_never_copies_raw_credentials() {
        let error = ProviderError::RequestFailed {
            message: "401 bearer live-token api_key=sk-secret-token".to_owned(),
            retryable: false,
            retry_count: 0,
            classification: classify_http_provider_failure(
                401,
                false,
                "openai_chat_http",
                "invalid api key",
            ),
        };
        let runtime = ProviderErrorEnvelope::from_error(&error)
            .runtime_error_envelope(false)
            .expect("redacted provider failure should project");
        let encoded = serde_json::to_string(&runtime).expect("runtime error should serialize");

        assert_eq!(runtime.class(), RuntimeErrorClass::AuthUnavailable);
        assert!(!encoded.contains("live-token"));
        assert!(!encoded.contains("sk-secret-token"));
    }

    #[test]
    fn provider_runtime_classification_does_not_depend_on_message_wording() {
        let classification =
            classify_http_provider_failure(503, true, "openai_chat_http", "provider unavailable");
        let first = ProviderError::RequestFailed {
            message: "first safe wording".to_owned(),
            retryable: true,
            retry_count: 0,
            classification: classification.clone(),
        };
        let second = ProviderError::RequestFailed {
            message: "different safe wording".to_owned(),
            retryable: true,
            retry_count: 0,
            classification,
        };
        let first = ProviderErrorEnvelope::from_error(&first)
            .runtime_error_envelope(false)
            .expect("first provider failure should project");
        let second = ProviderErrorEnvelope::from_error(&second)
            .runtime_error_envelope(false)
            .expect("second provider failure should project");

        assert_eq!(first.class(), second.class());
        assert_eq!(first.retryability(), second.retryability());
        assert_eq!(first.reason_code(), second.reason_code());
    }

    #[test]
    fn provider_runtime_projection_matches_exact_compatibility_metadata() {
        for error in [ProviderError::MissingApiKey, ProviderError::StatePoisoned] {
            let runtime = ProviderErrorEnvelope::from_error(&error)
                .runtime_error_envelope(false)
                .expect("typed provider failure should project");
            let mapping = palyra_common::runtime_contracts::legacy_runtime_error_mapping(
                runtime.reason_code(),
            )
            .expect("tested provider reason should have an exact compatibility mapping");

            assert_eq!(runtime.class(), mapping.class);
            assert_eq!(runtime.subsystem(), mapping.subsystem);
            assert_eq!(runtime.phase(), mapping.phase);
            assert_eq!(runtime.retryability(), mapping.retryability);
        }
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

    #[test]
    fn recovery_decision_maps_multimodal_rejection_to_transform() {
        let error = ProviderError::RequestFailed {
            message: "image input is not supported by this model".to_owned(),
            retryable: false,
            retry_count: 0,
            classification: classify_http_provider_failure(
                400,
                false,
                "openai_chat_http",
                r#"{"error":{"code":"vision_unsupported","message":"image input is not supported"}}"#,
            ),
        };

        let envelope = ProviderErrorEnvelope::from_error(&error);

        assert_eq!(envelope.kind, ProviderErrorKind::UnsupportedFeature);
        assert_eq!(envelope.severity, ProviderErrorSeverity::Recoverable);
        assert_eq!(
            envelope.recovery_decision.decision,
            ProviderRecoveryDecisionKind::RetryTransformed
        );
    }
}
