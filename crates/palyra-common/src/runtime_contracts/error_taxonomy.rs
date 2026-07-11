//! Strict runtime-error metadata shared by the runtime kernel and managed services.
//!
//! The existing [`PalyraErrorEnvelope`] remains the tolerant public compatibility
//! surface. This module defines the stricter metadata contract used for control-flow,
//! retry, replay-safety, diagnostics, and trace decisions. Free-form source errors are
//! never retained verbatim: presentation text is sanitized and byte-bounded before the
//! envelope can be constructed or deserialized.

use std::fmt;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};

use crate::security_posture::{
    sanitize_outbound_message_with_policy, OutboundMessage, SurfaceKind, SurfaceSanitizationPolicy,
};

use super::{PalyraErrorCategory, PalyraErrorEnvelope};

/// Schema version accepted by [`RuntimeErrorEnvelopeV1`].
pub const RUNTIME_ERROR_ENVELOPE_SCHEMA_VERSION: u32 = 1;
/// Stable contract identifier embedded in public runtime snapshots and diagnostics.
pub const RUNTIME_ERROR_CONTRACT_VERSION: &str = "runtime-error-envelope.v1";
/// Repository-relative JSON Schema path for the strict envelope.
pub const RUNTIME_ERROR_SCHEMA_PATH: &str = "schemas/json/common/runtime-error-envelope.v1.json";
/// Maximum UTF-8 byte length of a runtime reason code.
pub const MAX_RUNTIME_ERROR_REASON_CODE_BYTES: usize = 128;
/// Maximum UTF-8 byte length retained for a sanitized safe message.
pub const MAX_RUNTIME_ERROR_SAFE_MESSAGE_BYTES: usize = 2_048;
/// Maximum UTF-8 byte length retained for a sanitized recovery hint.
pub const MAX_RUNTIME_ERROR_RECOVERY_HINT_BYTES: usize = 512;
// Reserve the shared sanitizer's ASCII ellipsis so the envelope's advertised cap
// applies to the final serialized text rather than only to its pre-suffix prefix.
const SANITIZER_TRUNCATION_SUFFIX_BYTES: usize = 3;

macro_rules! runtime_error_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $variant:ident => $wire:literal
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        #[non_exhaustive]
        pub enum $name {
            $(
                #[serde(rename = $wire)]
                $variant,
            )+
        }

        impl $name {
            /// Every value defined by this contract version, in stable snapshot order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// Returns the stable serialized name for this value.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

runtime_error_enum! {
    /// Top-level runtime failure class used for policy and recovery decisions.
    pub enum RuntimeErrorClass {
        InvalidRequest => "invalid_request",
        PolicyDenied => "policy_denied",
        ApprovalRequired => "approval_required",
        AuthUnavailable => "auth_unavailable",
        ProviderRetryable => "provider_retryable",
        ProviderTerminal => "provider_terminal",
        ToolExecutionUnknown => "tool_execution_unknown",
        PluginContractViolation => "plugin_contract_violation",
        RecoveryBlocked => "recovery_blocked",
        DeliveryUnknown => "delivery_unknown",
        Cancelled => "cancelled",
        InternalInvariantViolation => "internal_invariant_violation"
    }
}

runtime_error_enum! {
    /// Runtime owner responsible for the failed operation.
    pub enum RuntimeSubsystem {
        RuntimeKernel => "runtime_kernel",
        RunStream => "run_stream",
        ControlPlane => "control_plane",
        Auth => "auth",
        Provider => "provider",
        Policy => "policy",
        Approval => "approval",
        Tool => "tool",
        Plugin => "plugin",
        Recovery => "recovery",
        Delivery => "delivery",
        BackgroundQueue => "background_queue",
        Session => "session",
        Worker => "worker",
        Mcp => "mcp",
        Browser => "browser"
    }
}

runtime_error_enum! {
    /// Operation phase where a runtime error was classified.
    pub enum RuntimeErrorPhase {
        RequestValidation => "request_validation",
        Admission => "admission",
        Queueing => "queueing",
        RuntimeSelection => "runtime_selection",
        ContextAssembly => "context_assembly",
        ProviderCall => "provider_call",
        ProviderRecovery => "provider_recovery",
        ProviderFinalization => "provider_finalization",
        PolicyEvaluation => "policy_evaluation",
        ToolValidation => "tool_validation",
        ToolGate => "tool_gate",
        Approval => "approval",
        ToolExecution => "tool_execution",
        ResultProjection => "result_projection",
        Compaction => "compaction",
        Verification => "verification",
        Recovery => "recovery",
        DeliveryIntent => "delivery_intent",
        DeliveryQueue => "delivery_queue",
        DeliverySend => "delivery_send",
        DeliveryAcknowledgement => "delivery_acknowledgement",
        Finalization => "finalization",
        Cancellation => "cancellation",
        PluginNegotiation => "plugin_negotiation",
        PluginHostCallAuthorization => "plugin_host_call_authorization",
        PluginExecution => "plugin_execution",
        Internal => "internal"
    }
}

runtime_error_enum! {
    /// Structured retry posture; this is never inferred from human-readable text.
    pub enum RuntimeRetryability {
        NotRetryable => "not_retryable",
        SafeSameRequest => "safe_same_request",
        SafeAfterBackoff => "safe_after_backoff",
        RequiresCredentialRefresh => "requires_credential_refresh",
        RequiresRequestTransform => "requires_request_transform",
        RequiresContextCompaction => "requires_context_compaction",
        RequiresProviderFailover => "requires_provider_failover",
        RequiresApproval => "requires_approval",
        RequiresIdempotencyGuard => "requires_idempotency_guard",
        RequiresOperatorReview => "requires_operator_review"
    }
}

impl RuntimeRetryability {
    /// Returns whether the same request may be retried automatically without new evidence.
    #[must_use]
    pub const fn allows_automatic_retry(self) -> bool {
        matches!(self, Self::SafeSameRequest | Self::SafeAfterBackoff)
    }
}

runtime_error_enum! {
    /// Highest sensitivity class assigned before an error leaves its owning subsystem.
    pub enum RuntimeErrorSecurityClass {
        Public => "public",
        Operator => "operator",
        Internal => "internal",
        Sensitive => "sensitive"
    }
}

runtime_error_enum! {
    /// Maximum user-facing visibility allowed for a sanitized runtime error.
    pub enum RuntimeErrorUserVisibility {
        Silent => "silent",
        StatusOnly => "status_only",
        SafeMessage => "safe_message",
        ActionRequired => "action_required"
    }
}

runtime_error_enum! {
    /// Closed-run outcomes with stable phase and reason-code projections.
    pub enum RuntimeTerminalOutcome {
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
        TimedOut => "timed_out"
    }
}

impl RuntimeTerminalOutcome {
    /// Returns the phase responsible for recording this closed-run outcome.
    #[must_use]
    pub const fn phase(self) -> RuntimeErrorPhase {
        match self {
            Self::Completed | Self::Failed | Self::TimedOut => RuntimeErrorPhase::Finalization,
            Self::Cancelled => RuntimeErrorPhase::Cancellation,
        }
    }

    /// Returns the stable reason code emitted by terminal metadata projections.
    #[must_use]
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::Completed => "runtime.terminal.completed",
            Self::Failed => "runtime.terminal.failed",
            Self::Cancelled => "runtime.terminal.cancelled",
            Self::TimedOut => "runtime.terminal.timed_out",
        }
    }
}

runtime_error_enum! {
    /// Binding invariant owned by the host runtime rather than an adapter.
    pub enum RuntimeInvariant {
        ExactlyOneTerminalEvent => "exactly_one_terminal_event",
        OneActiveGeneration => "one_active_generation",
        NoAutomaticDuplicateSideEffect => "no_automatic_duplicate_side_effect",
        DurableIntentBeforeEffect => "durable_intent_before_effect",
        DurableDeliveryIntentBeforeSend => "durable_delivery_intent_before_send",
        UnknownOutcomeDistinct => "unknown_outcome_distinct"
    }
}

/// Stable diagnostics descriptor for one binding runtime invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RuntimeInvariantDescriptor {
    /// Canonical invariant identifier.
    pub invariant: RuntimeInvariant,
    /// Stable code emitted when evidence violates the invariant.
    pub violation_reason_code: &'static str,
    /// Host-owned subsystem that enforces the invariant.
    pub subsystem: RuntimeSubsystem,
    /// Phase in which the invariant is checked.
    pub phase: RuntimeErrorPhase,
    /// Repository-owned test that proves the contract behavior.
    pub evidence_test: &'static str,
}

/// Canonical runtime invariant registry used by diagnostics and tests.
pub const RUNTIME_INVARIANT_DESCRIPTORS: &[RuntimeInvariantDescriptor] = &[
    RuntimeInvariantDescriptor {
        invariant: RuntimeInvariant::ExactlyOneTerminalEvent,
        violation_reason_code: "runtime.invariant.exactly_one_terminal_event",
        subsystem: RuntimeSubsystem::RuntimeKernel,
        phase: RuntimeErrorPhase::Finalization,
        evidence_test: "runtime_contracts::error_taxonomy::tests::closed_generation_requires_exactly_one_terminal_event",
    },
    RuntimeInvariantDescriptor {
        invariant: RuntimeInvariant::OneActiveGeneration,
        violation_reason_code: "runtime.invariant.one_active_generation",
        subsystem: RuntimeSubsystem::RuntimeKernel,
        phase: RuntimeErrorPhase::Admission,
        evidence_test: "runtime_contracts::error_taxonomy::tests::active_generation_is_single_owner",
    },
    RuntimeInvariantDescriptor {
        invariant: RuntimeInvariant::NoAutomaticDuplicateSideEffect,
        violation_reason_code: "runtime.invariant.no_automatic_duplicate_side_effect",
        subsystem: RuntimeSubsystem::Tool,
        phase: RuntimeErrorPhase::ToolExecution,
        evidence_test: "runtime_contracts::error_taxonomy::tests::unresolved_side_effect_is_not_automatically_replayed",
    },
    RuntimeInvariantDescriptor {
        invariant: RuntimeInvariant::DurableIntentBeforeEffect,
        violation_reason_code: "runtime.invariant.durable_intent_before_effect",
        subsystem: RuntimeSubsystem::Tool,
        phase: RuntimeErrorPhase::ToolExecution,
        evidence_test: "runtime_contracts::error_taxonomy::tests::effect_requires_prior_durable_intent",
    },
    RuntimeInvariantDescriptor {
        invariant: RuntimeInvariant::DurableDeliveryIntentBeforeSend,
        violation_reason_code: "runtime.invariant.durable_delivery_intent_before_send",
        subsystem: RuntimeSubsystem::Delivery,
        phase: RuntimeErrorPhase::DeliverySend,
        evidence_test: "runtime_contracts::error_taxonomy::tests::delivery_send_requires_prior_durable_intent",
    },
    RuntimeInvariantDescriptor {
        invariant: RuntimeInvariant::UnknownOutcomeDistinct,
        violation_reason_code: "runtime.invariant.unknown_outcome_distinct",
        subsystem: RuntimeSubsystem::Recovery,
        phase: RuntimeErrorPhase::Recovery,
        evidence_test: "runtime_contracts::error_taxonomy::tests::unknown_outcome_is_not_success_or_failure",
    },
];

/// Finite evidence used to validate the binding runtime invariants.
///
/// Counts describe one run generation. An open generation may have zero terminal
/// events, while a closed generation must have exactly one. An unresolved automatic
/// side-effect attempt count above one represents an unsafe replay, not an operator-
/// confirmed or idempotency-protected retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeInvariantEvidence {
    /// Whether the generation has been durably closed.
    pub closed_generation: bool,
    /// Durable terminal events observed for the generation.
    pub terminal_event_count: u32,
    /// Generations concurrently holding active authority for the run.
    pub active_generation_count: u32,
    /// Automatic attempts of one unresolved non-idempotent or external effect.
    pub automatic_unresolved_side_effect_attempts: u32,
    /// Whether a mutating or external effect was started.
    pub mutating_or_external_effect_started: bool,
    /// Whether the effect intent was durable before execution started.
    pub durable_effect_intent: bool,
    /// Whether a connector adapter send was started.
    pub delivery_send_started: bool,
    /// Whether the delivery intent was durable before the adapter call.
    pub durable_delivery_intent: bool,
    /// Whether the observed effect or delivery outcome is unresolved.
    pub outcome_unknown: bool,
    /// Whether an unresolved outcome was incorrectly collapsed into success or failure.
    pub unknown_outcome_marked_terminal: bool,
}

/// One failed invariant returned by [`validate_runtime_invariant_evidence`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RuntimeInvariantViolation {
    /// Invariant that failed.
    pub invariant: RuntimeInvariant,
    /// Stable reason code from [`RUNTIME_INVARIANT_DESCRIPTORS`].
    pub reason_code: &'static str,
}

/// Collection of invariant violations found in one evidence snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInvariantValidationError {
    violations: Vec<RuntimeInvariantViolation>,
}

impl RuntimeInvariantValidationError {
    /// Returns all invariant violations in stable registry order.
    #[must_use]
    pub fn violations(&self) -> &[RuntimeInvariantViolation] {
        self.violations.as_slice()
    }
}

impl fmt::Display for RuntimeInvariantValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason_codes = self
            .violations
            .iter()
            .map(|violation| violation.reason_code)
            .collect::<Vec<_>>()
            .join(", ");
        write!(formatter, "runtime invariant validation failed: {reason_codes}")
    }
}

impl std::error::Error for RuntimeInvariantValidationError {}

/// Validates closed-terminal, generation, side-effect, and delivery ordering evidence.
///
/// # Errors
/// Returns every failed invariant in stable registry order. Unknown outcomes are kept
/// distinct from succeeded or failed outcomes, and unresolved effects may not be replayed
/// automatically without external evidence represented by a fresh evidence snapshot.
pub fn validate_runtime_invariant_evidence(
    evidence: RuntimeInvariantEvidence,
) -> Result<(), RuntimeInvariantValidationError> {
    let mut violations = Vec::new();
    let invalid_terminal_count = if evidence.closed_generation {
        evidence.terminal_event_count != 1
    } else {
        evidence.terminal_event_count > 1
    };
    if invalid_terminal_count {
        push_invariant_violation(&mut violations, RuntimeInvariant::ExactlyOneTerminalEvent);
    }
    if evidence.active_generation_count > 1 {
        push_invariant_violation(&mut violations, RuntimeInvariant::OneActiveGeneration);
    }
    if evidence.automatic_unresolved_side_effect_attempts > 1 {
        push_invariant_violation(&mut violations, RuntimeInvariant::NoAutomaticDuplicateSideEffect);
    }
    if evidence.mutating_or_external_effect_started && !evidence.durable_effect_intent {
        push_invariant_violation(&mut violations, RuntimeInvariant::DurableIntentBeforeEffect);
    }
    if evidence.delivery_send_started && !evidence.durable_delivery_intent {
        push_invariant_violation(
            &mut violations,
            RuntimeInvariant::DurableDeliveryIntentBeforeSend,
        );
    }
    if evidence.outcome_unknown && evidence.unknown_outcome_marked_terminal {
        push_invariant_violation(&mut violations, RuntimeInvariant::UnknownOutcomeDistinct);
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(RuntimeInvariantValidationError { violations })
    }
}

fn push_invariant_violation(
    violations: &mut Vec<RuntimeInvariantViolation>,
    invariant: RuntimeInvariant,
) {
    let descriptor = runtime_invariant_descriptor(invariant);
    violations.push(RuntimeInvariantViolation {
        invariant,
        reason_code: descriptor.violation_reason_code,
    });
}

/// Returns the registry descriptor for `invariant`.
#[must_use]
pub fn runtime_invariant_descriptor(invariant: RuntimeInvariant) -> RuntimeInvariantDescriptor {
    *RUNTIME_INVARIANT_DESCRIPTORS
        .iter()
        .find(|descriptor| descriptor.invariant == invariant)
        .expect("every runtime invariant must have a descriptor")
}

/// Construction input for a validated [`RuntimeErrorEnvelopeV1`].
pub struct RuntimeErrorEnvelopeV1Input {
    /// Top-level error class.
    pub class: RuntimeErrorClass,
    /// Existing or newly registered stable reason code.
    pub reason_code: String,
    /// Subsystem that owns the classification.
    pub subsystem: RuntimeSubsystem,
    /// Phase in which the error was classified.
    pub phase: RuntimeErrorPhase,
    /// Typed retry posture derived from structured evidence.
    pub retryability: RuntimeRetryability,
    /// Sensitivity assigned before projection.
    pub security_class: RuntimeErrorSecurityClass,
    /// Maximum user-facing visibility allowed for the error.
    pub user_visibility: RuntimeErrorUserVisibility,
    /// Whether user-visible or externally visible output was already emitted.
    pub output_emitted: bool,
    /// Whether a mutating or external effect may already have occurred.
    pub side_effect_may_have_occurred: bool,
    /// Human-readable source text; construction sanitizes and bounds it.
    pub safe_message: String,
    /// Recovery guidance; construction sanitizes and bounds it independently.
    pub recovery_hint: String,
}

/// Strict schema-version-1 runtime error metadata envelope.
///
/// Fields are private so callers cannot bypass schema, reason-code, redaction, or
/// cross-field validation. Use [`Self::try_new`] and the accessor methods.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeErrorEnvelopeV1 {
    schema_version: u32,
    class: RuntimeErrorClass,
    reason_code: String,
    subsystem: RuntimeSubsystem,
    phase: RuntimeErrorPhase,
    retryability: RuntimeRetryability,
    security_class: RuntimeErrorSecurityClass,
    user_visibility: RuntimeErrorUserVisibility,
    output_emitted: bool,
    side_effect_may_have_occurred: bool,
    safe_message: String,
    recovery_hint: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeErrorEnvelopeV1Wire {
    schema_version: u32,
    class: RuntimeErrorClass,
    reason_code: String,
    subsystem: RuntimeSubsystem,
    phase: RuntimeErrorPhase,
    retryability: RuntimeRetryability,
    security_class: RuntimeErrorSecurityClass,
    user_visibility: RuntimeErrorUserVisibility,
    output_emitted: bool,
    side_effect_may_have_occurred: bool,
    safe_message: String,
    recovery_hint: String,
}

impl<'de> Deserialize<'de> for RuntimeErrorEnvelopeV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RuntimeErrorEnvelopeV1Wire::deserialize(deserializer)?;
        if wire.schema_version != RUNTIME_ERROR_ENVELOPE_SCHEMA_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported runtime error schema version {}; expected {}",
                wire.schema_version, RUNTIME_ERROR_ENVELOPE_SCHEMA_VERSION
            )));
        }
        Self::try_new(RuntimeErrorEnvelopeV1Input {
            class: wire.class,
            reason_code: wire.reason_code,
            subsystem: wire.subsystem,
            phase: wire.phase,
            retryability: wire.retryability,
            security_class: wire.security_class,
            user_visibility: wire.user_visibility,
            output_emitted: wire.output_emitted,
            side_effect_may_have_occurred: wire.side_effect_may_have_occurred,
            safe_message: wire.safe_message,
            recovery_hint: wire.recovery_hint,
        })
        .map_err(D::Error::custom)
    }
}

/// Validation error returned while constructing a strict runtime envelope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeErrorValidationError {
    /// Reason code is not a short lowercase machine identifier.
    #[error("invalid runtime reason code: {reason}")]
    InvalidReasonCode { reason: String },
    /// A user-visible error has no sanitized message after redaction.
    #[error("runtime error with user visibility {visibility} requires a safe message")]
    MissingSafeMessage { visibility: RuntimeErrorUserVisibility },
    /// Every runtime envelope must retain bounded recovery guidance.
    #[error("runtime error recovery hint is empty after redaction")]
    MissingRecoveryHint,
    /// An uncertainty class cannot claim that no side effect may have occurred.
    #[error("runtime error class {class} requires side_effect_may_have_occurred=true")]
    UncertainClassWithoutSideEffect { class: RuntimeErrorClass },
    /// Unsafe automatic retry was requested after an uncertain side effect.
    #[error("runtime retryability {retryability} is unsafe when a side effect may have occurred")]
    UnsafeRetryAfterUncertainSideEffect { retryability: RuntimeRetryability },
    /// Automatic replay would duplicate output already visible outside the runtime.
    #[error("runtime retryability {retryability} is unsafe after output was emitted")]
    UnsafeRetryAfterOutput { retryability: RuntimeRetryability },
    /// Error class and retry posture express contradictory recovery semantics.
    #[error("runtime error class {class} does not allow retryability {retryability}")]
    ClassRetryabilityMismatch { class: RuntimeErrorClass, retryability: RuntimeRetryability },
    /// Approval-required errors must wait for approval rather than retry automatically.
    #[error("approval_required errors must use requires_approval retryability")]
    ApprovalRetryabilityMismatch,
    /// Exact compatibility registry has no entry for a legacy reason code.
    #[error("legacy runtime reason code is not mapped")]
    UnmappedLegacyReasonCode,
}

impl RuntimeErrorEnvelopeV1 {
    /// Constructs a schema-version-1 envelope after sanitizing text and validating semantics.
    ///
    /// # Errors
    /// Returns [`RuntimeErrorValidationError`] when the reason code is unsafe, required
    /// presentation text is empty after redaction, or retry/side-effect fields conflict.
    pub fn try_new(
        input: RuntimeErrorEnvelopeV1Input,
    ) -> Result<Self, RuntimeErrorValidationError> {
        validate_runtime_reason_code(input.reason_code.as_str())?;
        validate_runtime_error_semantics(
            input.class,
            input.retryability,
            input.output_emitted,
            input.side_effect_may_have_occurred,
        )?;
        let safe_message = sanitize_runtime_error_text(
            input.safe_message.as_str(),
            MAX_RUNTIME_ERROR_SAFE_MESSAGE_BYTES,
        );
        if matches!(
            input.user_visibility,
            RuntimeErrorUserVisibility::SafeMessage | RuntimeErrorUserVisibility::ActionRequired
        ) && safe_message.trim().is_empty()
        {
            return Err(RuntimeErrorValidationError::MissingSafeMessage {
                visibility: input.user_visibility,
            });
        }
        let recovery_hint = sanitize_runtime_error_text(
            input.recovery_hint.as_str(),
            MAX_RUNTIME_ERROR_RECOVERY_HINT_BYTES,
        );
        if recovery_hint.trim().is_empty() {
            return Err(RuntimeErrorValidationError::MissingRecoveryHint);
        }

        Ok(Self {
            schema_version: RUNTIME_ERROR_ENVELOPE_SCHEMA_VERSION,
            class: input.class,
            reason_code: input.reason_code,
            subsystem: input.subsystem,
            phase: input.phase,
            retryability: input.retryability,
            security_class: input.security_class,
            user_visibility: input.user_visibility,
            output_emitted: input.output_emitted,
            side_effect_may_have_occurred: input.side_effect_may_have_occurred,
            safe_message,
            recovery_hint,
        })
    }

    /// Returns the immutable schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Returns the top-level error class.
    #[must_use]
    pub const fn class(&self) -> RuntimeErrorClass {
        self.class
    }

    /// Returns the stable reason code.
    #[must_use]
    pub fn reason_code(&self) -> &str {
        self.reason_code.as_str()
    }

    /// Returns the subsystem that owns this classification.
    #[must_use]
    pub const fn subsystem(&self) -> RuntimeSubsystem {
        self.subsystem
    }

    /// Returns the operation phase where the failure was classified.
    #[must_use]
    pub const fn phase(&self) -> RuntimeErrorPhase {
        self.phase
    }

    /// Returns the typed retry posture.
    #[must_use]
    pub const fn retryability(&self) -> RuntimeRetryability {
        self.retryability
    }

    /// Returns the highest sensitivity class assigned to the envelope.
    #[must_use]
    pub const fn security_class(&self) -> RuntimeErrorSecurityClass {
        self.security_class
    }

    /// Returns the maximum allowed user visibility.
    #[must_use]
    pub const fn user_visibility(&self) -> RuntimeErrorUserVisibility {
        self.user_visibility
    }

    /// Returns whether output was already emitted before the error.
    #[must_use]
    pub const fn output_emitted(&self) -> bool {
        self.output_emitted
    }

    /// Returns whether a mutating or external side effect may already have occurred.
    #[must_use]
    pub const fn side_effect_may_have_occurred(&self) -> bool {
        self.side_effect_may_have_occurred
    }

    /// Returns the bounded, sanitized safe message.
    #[must_use]
    pub fn safe_message(&self) -> &str {
        self.safe_message.as_str()
    }

    /// Returns the bounded, sanitized recovery hint.
    #[must_use]
    pub fn recovery_hint(&self) -> &str {
        self.recovery_hint.as_str()
    }

    /// Projects this strict envelope onto the frozen public compatibility envelope.
    ///
    /// The projection is total and deterministic. Legacy `retryable=true` is emitted only
    /// for retry postures that permit an automatic retry of the same request.
    #[must_use]
    pub fn to_palyra_error_envelope(&self) -> PalyraErrorEnvelope {
        PalyraErrorEnvelope::new(
            legacy_category(self.class),
            self.reason_code.clone(),
            self.safe_message.clone(),
            self.recovery_hint.clone(),
            self.retryability.allows_automatic_retry()
                && !self.output_emitted
                && !self.side_effect_may_have_occurred,
            self.security_class != RuntimeErrorSecurityClass::Public,
        )
    }
}

fn validate_runtime_error_semantics(
    class: RuntimeErrorClass,
    retryability: RuntimeRetryability,
    output_emitted: bool,
    side_effect_may_have_occurred: bool,
) -> Result<(), RuntimeErrorValidationError> {
    if matches!(class, RuntimeErrorClass::ToolExecutionUnknown | RuntimeErrorClass::DeliveryUnknown)
        && !side_effect_may_have_occurred
    {
        return Err(RuntimeErrorValidationError::UncertainClassWithoutSideEffect { class });
    }
    if side_effect_may_have_occurred && retryability.allows_automatic_retry() {
        return Err(RuntimeErrorValidationError::UnsafeRetryAfterUncertainSideEffect {
            retryability,
        });
    }
    if output_emitted && retryability.allows_automatic_retry() {
        return Err(RuntimeErrorValidationError::UnsafeRetryAfterOutput { retryability });
    }
    if class == RuntimeErrorClass::Cancelled && retryability != RuntimeRetryability::NotRetryable {
        return Err(RuntimeErrorValidationError::ClassRetryabilityMismatch { class, retryability });
    }
    if matches!(
        class,
        RuntimeErrorClass::InvalidRequest
            | RuntimeErrorClass::PolicyDenied
            | RuntimeErrorClass::AuthUnavailable
            | RuntimeErrorClass::ProviderTerminal
            | RuntimeErrorClass::PluginContractViolation
            | RuntimeErrorClass::InternalInvariantViolation
    ) && retryability.allows_automatic_retry()
    {
        return Err(RuntimeErrorValidationError::ClassRetryabilityMismatch { class, retryability });
    }
    if class == RuntimeErrorClass::ApprovalRequired
        && retryability != RuntimeRetryability::RequiresApproval
    {
        return Err(RuntimeErrorValidationError::ApprovalRetryabilityMismatch);
    }
    Ok(())
}

fn legacy_category(class: RuntimeErrorClass) -> PalyraErrorCategory {
    match class {
        RuntimeErrorClass::InvalidRequest => PalyraErrorCategory::Validation,
        RuntimeErrorClass::PolicyDenied => PalyraErrorCategory::Policy,
        RuntimeErrorClass::ApprovalRequired => PalyraErrorCategory::Approval,
        RuntimeErrorClass::AuthUnavailable => PalyraErrorCategory::Auth,
        RuntimeErrorClass::ProviderRetryable | RuntimeErrorClass::ProviderTerminal => {
            PalyraErrorCategory::Provider
        }
        RuntimeErrorClass::ToolExecutionUnknown => PalyraErrorCategory::Tool,
        RuntimeErrorClass::PluginContractViolation => PalyraErrorCategory::Dependency,
        RuntimeErrorClass::RecoveryBlocked | RuntimeErrorClass::DeliveryUnknown => {
            PalyraErrorCategory::Availability
        }
        RuntimeErrorClass::Cancelled => PalyraErrorCategory::Conflict,
        RuntimeErrorClass::InternalInvariantViolation => PalyraErrorCategory::Internal,
    }
}

fn validate_runtime_reason_code(reason_code: &str) -> Result<(), RuntimeErrorValidationError> {
    let bytes = reason_code.as_bytes();
    let valid_length = (3..=MAX_RUNTIME_ERROR_REASON_CODE_BYTES).contains(&bytes.len());
    let valid_edges = bytes.first().zip(bytes.last()).is_some_and(|(first, last)| {
        is_reason_alphanumeric(*first) && is_reason_alphanumeric(*last)
    });
    let valid_characters = bytes
        .iter()
        .all(|byte| is_reason_alphanumeric(*byte) || matches!(*byte, b'.' | b'_' | b'/' | b'-'));
    let separators_are_bounded = !bytes.windows(2).any(|window| {
        is_reason_namespace_separator(window[0]) && is_reason_namespace_separator(window[1])
    });

    if valid_length && valid_edges && valid_characters && separators_are_bounded {
        Ok(())
    } else {
        Err(RuntimeErrorValidationError::InvalidReasonCode {
            reason: "expected 3-128 lowercase ASCII bytes using letters, digits, '.', '_', '/', or '-' without adjacent namespace separators"
                .to_owned(),
        })
    }
}

const fn is_reason_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

const fn is_reason_namespace_separator(byte: u8) -> bool {
    matches!(byte, b'.' | b'/')
}

fn sanitize_runtime_error_text(raw: &str, max_text_bytes: usize) -> String {
    let policy = SurfaceSanitizationPolicy {
        surface: SurfaceKind::InternalDiagnostics,
        allow_raw_provider_error: false,
        allow_tool_stderr: false,
        allow_file_paths: false,
        allow_redacted_placeholders: true,
        allow_policy_reason_codes: true,
        allow_internal_run_ids: false,
        allow_stack_traces: false,
        allow_model_routing_decision: false,
        max_text_bytes: max_text_bytes.saturating_sub(SANITIZER_TRUNCATION_SUFFIX_BYTES),
    };
    sanitize_outbound_message_with_policy(
        &OutboundMessage { surface: SurfaceKind::InternalDiagnostics, text: raw.trim().to_owned() },
        policy,
    )
    .sanitized_text
    .trim()
    .to_owned()
}

mod legacy;
pub use legacy::*;

/// Builds the public, metadata-only snapshot used by contract gates and diagnostics.
#[must_use]
pub fn runtime_error_contract_snapshot() -> Value {
    json!({
        "snapshot_version": RUNTIME_ERROR_CONTRACT_VERSION,
        "changelog_note": "Adds a strict metadata-only runtime error taxonomy, binding invariant registry, and exact legacy compatibility map without changing frozen public error envelopes.",
        "schema_version": RUNTIME_ERROR_ENVELOPE_SCHEMA_VERSION,
        "schema_path": RUNTIME_ERROR_SCHEMA_PATH,
        "deny_unknown_fields": true,
        "raw_provider_payload_allowed": false,
        "raw_stderr_allowed": false,
        "reason_code_max_bytes": MAX_RUNTIME_ERROR_REASON_CODE_BYTES,
        "safe_message_max_bytes": MAX_RUNTIME_ERROR_SAFE_MESSAGE_BYTES,
        "recovery_hint_max_bytes": MAX_RUNTIME_ERROR_RECOVERY_HINT_BYTES,
        "required_fields": [
            "schema_version",
            "class",
            "reason_code",
            "subsystem",
            "phase",
            "retryability",
            "security_class",
            "user_visibility",
            "output_emitted",
            "side_effect_may_have_occurred",
            "safe_message",
            "recovery_hint"
        ],
        "classes": enum_wire_values(RuntimeErrorClass::ALL.iter().map(|value| value.as_str())),
        "subsystems": enum_wire_values(RuntimeSubsystem::ALL.iter().map(|value| value.as_str())),
        "phases": enum_wire_values(RuntimeErrorPhase::ALL.iter().map(|value| value.as_str())),
        "retryability": enum_wire_values(RuntimeRetryability::ALL.iter().map(|value| value.as_str())),
        "security_classes": enum_wire_values(RuntimeErrorSecurityClass::ALL.iter().map(|value| value.as_str())),
        "user_visibility": enum_wire_values(RuntimeErrorUserVisibility::ALL.iter().map(|value| value.as_str())),
        "terminal_outcomes": RuntimeTerminalOutcome::ALL.iter().map(|outcome| json!({
            "outcome": outcome.as_str(),
            "phase": outcome.phase().as_str(),
            "reason_code": outcome.reason_code(),
        })).collect::<Vec<_>>(),
        "invariants": RUNTIME_INVARIANT_DESCRIPTORS,
        "legacy_compatibility": LEGACY_RUNTIME_ERROR_MAPPINGS,
        "legacy_projection": {
            "new_to_palyra_error": "total",
            "palyra_error_to_new": "fallible_typed_category_or_exact_reason_code",
            "message_text_classification_allowed": false,
            "mass_reason_code_rename_required": false,
        },
    })
}

fn enum_wire_values<'a>(values: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
    values.collect()
}

#[cfg(test)]
mod tests;
