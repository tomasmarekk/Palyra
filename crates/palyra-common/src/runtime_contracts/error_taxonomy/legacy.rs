//! Compatibility projections between strict and frozen public runtime errors.

use super::*;

/// Evidence known at the call site while projecting a legacy error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeErrorObservation {
    /// Whether user-visible or externally visible output was already emitted.
    pub output_emitted: bool,
    /// Whether a mutating or external effect may already have occurred.
    pub side_effect_may_have_occurred: bool,
}

/// Exact compatibility rule for one existing reason code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LegacyRuntimeErrorMapping {
    /// Existing reason code retained without renaming.
    pub legacy_reason_code: &'static str,
    /// New top-level class.
    pub class: RuntimeErrorClass,
    /// Owning subsystem.
    pub subsystem: RuntimeSubsystem,
    /// Classification phase.
    pub phase: RuntimeErrorPhase,
    /// Structured retry posture.
    pub retryability: RuntimeRetryability,
    /// Sensitivity assigned to the compatibility projection.
    pub security_class: RuntimeErrorSecurityClass,
    /// Maximum user visibility assigned to the projection.
    pub user_visibility: RuntimeErrorUserVisibility,
}

/// Canonical exact-code rules for adopted legacy codes. Unknown codes fail closed.
pub const LEGACY_RUNTIME_ERROR_MAPPINGS: &[LegacyRuntimeErrorMapping] = &[
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "provider.recovery.retry_after",
        class: RuntimeErrorClass::ProviderRetryable,
        subsystem: RuntimeSubsystem::Provider,
        phase: RuntimeErrorPhase::ProviderRecovery,
        retryability: RuntimeRetryability::SafeAfterBackoff,
        security_class: RuntimeErrorSecurityClass::Sensitive,
        user_visibility: RuntimeErrorUserVisibility::StatusOnly,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "provider.recovery.fail_closed",
        class: RuntimeErrorClass::ProviderTerminal,
        subsystem: RuntimeSubsystem::Provider,
        phase: RuntimeErrorPhase::ProviderFinalization,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Sensitive,
        user_visibility: RuntimeErrorUserVisibility::SafeMessage,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "provider.recovery.refresh_credential",
        class: RuntimeErrorClass::AuthUnavailable,
        subsystem: RuntimeSubsystem::Auth,
        phase: RuntimeErrorPhase::ProviderRecovery,
        retryability: RuntimeRetryability::RequiresCredentialRefresh,
        security_class: RuntimeErrorSecurityClass::Sensitive,
        user_visibility: RuntimeErrorUserVisibility::ActionRequired,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "tool_catalog.invalid_json",
        class: RuntimeErrorClass::InvalidRequest,
        subsystem: RuntimeSubsystem::Tool,
        phase: RuntimeErrorPhase::ToolValidation,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::SafeMessage,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "tool_replay.mutating_timeout_requires_guard",
        class: RuntimeErrorClass::ToolExecutionUnknown,
        subsystem: RuntimeSubsystem::Tool,
        phase: RuntimeErrorPhase::ToolExecution,
        retryability: RuntimeRetryability::RequiresIdempotencyGuard,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::ActionRequired,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "approval.pending",
        class: RuntimeErrorClass::ApprovalRequired,
        subsystem: RuntimeSubsystem::Approval,
        phase: RuntimeErrorPhase::Approval,
        retryability: RuntimeRetryability::RequiresApproval,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::ActionRequired,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "approval.required",
        class: RuntimeErrorClass::ApprovalRequired,
        subsystem: RuntimeSubsystem::Approval,
        phase: RuntimeErrorPhase::Approval,
        retryability: RuntimeRetryability::RequiresApproval,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::ActionRequired,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "approval.denied",
        class: RuntimeErrorClass::PolicyDenied,
        subsystem: RuntimeSubsystem::Approval,
        phase: RuntimeErrorPhase::Approval,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::SafeMessage,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "platform_outcome_unknown",
        class: RuntimeErrorClass::DeliveryUnknown,
        subsystem: RuntimeSubsystem::Delivery,
        phase: RuntimeErrorPhase::DeliveryAcknowledgement,
        retryability: RuntimeRetryability::RequiresIdempotencyGuard,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::ActionRequired,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "channel.delivery.deferred_retry.concurrency_limit",
        class: RuntimeErrorClass::RecoveryBlocked,
        subsystem: RuntimeSubsystem::Delivery,
        phase: RuntimeErrorPhase::DeliveryQueue,
        retryability: RuntimeRetryability::SafeAfterBackoff,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::StatusOnly,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "agent_harness_plugin.contract_missing",
        class: RuntimeErrorClass::PluginContractViolation,
        subsystem: RuntimeSubsystem::Plugin,
        phase: RuntimeErrorPhase::PluginNegotiation,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::ActionRequired,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "agent_harness_plugin.contract_rejected",
        class: RuntimeErrorClass::PluginContractViolation,
        subsystem: RuntimeSubsystem::Plugin,
        phase: RuntimeErrorPhase::PluginNegotiation,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::ActionRequired,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "plugin.host_call.denied.service_grant_missing",
        class: RuntimeErrorClass::PolicyDenied,
        subsystem: RuntimeSubsystem::Plugin,
        phase: RuntimeErrorPhase::PluginHostCallAuthorization,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Sensitive,
        user_visibility: RuntimeErrorUserVisibility::SafeMessage,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "workspace_patch.execution_failed",
        class: RuntimeErrorClass::RecoveryBlocked,
        subsystem: RuntimeSubsystem::Tool,
        phase: RuntimeErrorPhase::ToolExecution,
        retryability: RuntimeRetryability::RequiresOperatorReview,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::ActionRequired,
    },
    LegacyRuntimeErrorMapping {
        legacy_reason_code: "runtime.invariant.phase_mismatch",
        class: RuntimeErrorClass::InternalInvariantViolation,
        subsystem: RuntimeSubsystem::RuntimeKernel,
        phase: RuntimeErrorPhase::Internal,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::StatusOnly,
    },
];

/// Returns the exact compatibility rule for a stable legacy reason code.
#[must_use]
pub fn legacy_runtime_error_mapping(reason_code: &str) -> Option<LegacyRuntimeErrorMapping> {
    LEGACY_RUNTIME_ERROR_MAPPINGS
        .iter()
        .copied()
        .find(|mapping| mapping.legacy_reason_code == reason_code)
}

/// Projects an exactly mapped legacy reason code into the strict runtime envelope.
///
/// # Errors
/// Returns [`RuntimeErrorValidationError::UnmappedLegacyReasonCode`] for unknown codes,
/// or another validation error when call-site side-effect evidence conflicts with the
/// mapped retry posture. No prefix or message-text inference is performed.
pub fn project_legacy_runtime_error(
    reason_code: &str,
    observation: RuntimeErrorObservation,
    safe_message: impl Into<String>,
    recovery_hint: impl Into<String>,
) -> Result<RuntimeErrorEnvelopeV1, RuntimeErrorValidationError> {
    let mapping = legacy_runtime_error_mapping(reason_code)
        .ok_or(RuntimeErrorValidationError::UnmappedLegacyReasonCode)?;
    let retryability = if observation.side_effect_may_have_occurred
        && mapping.retryability.allows_automatic_retry()
    {
        RuntimeRetryability::RequiresIdempotencyGuard
    } else {
        mapping.retryability
    };
    RuntimeErrorEnvelopeV1::try_new(RuntimeErrorEnvelopeV1Input {
        class: mapping.class,
        reason_code: mapping.legacy_reason_code.to_owned(),
        subsystem: mapping.subsystem,
        phase: mapping.phase,
        retryability,
        security_class: mapping.security_class,
        user_visibility: mapping.user_visibility,
        output_emitted: observation.output_emitted,
        side_effect_may_have_occurred: observation.side_effect_may_have_occurred,
        safe_message: safe_message.into(),
        recovery_hint: recovery_hint.into(),
    })
}

/// Explicit call-site context used for a fallible legacy public-error projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyPublicErrorProjectionContext {
    /// Source subsystem when no exact compatibility mapping exists.
    pub subsystem: RuntimeSubsystem,
    /// Source phase when no exact compatibility mapping exists.
    pub phase: RuntimeErrorPhase,
    /// Sensitivity applied to the strict projection.
    pub security_class: RuntimeErrorSecurityClass,
    /// Maximum user visibility applied to the strict projection.
    pub user_visibility: RuntimeErrorUserVisibility,
    /// Structured output and side-effect evidence supplied by the caller.
    pub observation: RuntimeErrorObservation,
}

/// Projects a frozen [`PalyraErrorEnvelope`] into the strict runtime contract.
///
/// Exact registry entries take precedence. Otherwise the typed legacy category and
/// Boolean retry field are used with explicit call-site context; message text is never
/// used for classification. Source text is sanitized before it enters the new envelope.
///
/// # Errors
/// Returns a validation error for unsafe reason codes or incompatible retry/side-effect
/// evidence.
pub fn project_palyra_error_envelope(
    legacy: &PalyraErrorEnvelope,
    context: LegacyPublicErrorProjectionContext,
) -> Result<RuntimeErrorEnvelopeV1, RuntimeErrorValidationError> {
    if legacy_runtime_error_mapping(legacy.code.as_str()).is_some() {
        return project_legacy_runtime_error(
            legacy.code.as_str(),
            context.observation,
            legacy.message.as_str(),
            legacy.recovery_hint.as_str(),
        );
    }

    let class = fallback_runtime_class(
        legacy.category,
        legacy.retryable,
        context.observation.side_effect_may_have_occurred,
    );
    let retryability = fallback_runtime_retryability(
        class,
        legacy.retryable,
        context.observation.side_effect_may_have_occurred,
    );
    RuntimeErrorEnvelopeV1::try_new(RuntimeErrorEnvelopeV1Input {
        class,
        reason_code: legacy.code.clone(),
        subsystem: context.subsystem,
        phase: context.phase,
        retryability,
        security_class: context.security_class,
        user_visibility: context.user_visibility,
        output_emitted: context.observation.output_emitted,
        side_effect_may_have_occurred: context.observation.side_effect_may_have_occurred,
        safe_message: legacy.message.clone(),
        recovery_hint: legacy.recovery_hint.clone(),
    })
}

fn fallback_runtime_class(
    category: PalyraErrorCategory,
    retryable: bool,
    side_effect_may_have_occurred: bool,
) -> RuntimeErrorClass {
    match category {
        PalyraErrorCategory::Auth => RuntimeErrorClass::AuthUnavailable,
        PalyraErrorCategory::Validation | PalyraErrorCategory::NotFound => {
            RuntimeErrorClass::InvalidRequest
        }
        PalyraErrorCategory::Policy => RuntimeErrorClass::PolicyDenied,
        PalyraErrorCategory::Approval => RuntimeErrorClass::ApprovalRequired,
        PalyraErrorCategory::Provider => {
            if retryable {
                RuntimeErrorClass::ProviderRetryable
            } else {
                RuntimeErrorClass::ProviderTerminal
            }
        }
        PalyraErrorCategory::Sandbox
        | PalyraErrorCategory::Mcp
        | PalyraErrorCategory::ExecutionBackend
        | PalyraErrorCategory::Tool => {
            if side_effect_may_have_occurred {
                RuntimeErrorClass::ToolExecutionUnknown
            } else {
                RuntimeErrorClass::RecoveryBlocked
            }
        }
        PalyraErrorCategory::Conflict
        | PalyraErrorCategory::RateLimit
        | PalyraErrorCategory::Dependency
        | PalyraErrorCategory::Availability => RuntimeErrorClass::RecoveryBlocked,
        PalyraErrorCategory::Internal => RuntimeErrorClass::InternalInvariantViolation,
    }
}

fn fallback_runtime_retryability(
    class: RuntimeErrorClass,
    retryable: bool,
    side_effect_may_have_occurred: bool,
) -> RuntimeRetryability {
    if class == RuntimeErrorClass::ApprovalRequired {
        RuntimeRetryability::RequiresApproval
    } else if side_effect_may_have_occurred {
        RuntimeRetryability::RequiresIdempotencyGuard
    } else if retryable {
        // The frozen Boolean proves retry eligibility, not that an immediate replay is safe.
        RuntimeRetryability::SafeAfterBackoff
    } else {
        RuntimeRetryability::NotRetryable
    }
}
