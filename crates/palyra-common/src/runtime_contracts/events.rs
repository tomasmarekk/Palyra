//! Unified generation-aware runtime event envelope.
//!
//! The host owns sequence, terminal classification, and redaction. External
//! runtimes can propose metadata but cannot grant their own ordering authority.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::{
    LegacyRuntimeIdentityAdapter, RuntimeErrorPhase, RuntimeGenerationLane, RuntimeIdentityError,
    RuntimeIdentitySetV1, RuntimeRetryability, RuntimeSubsystem,
};

/// Schema version accepted by [`RuntimeEventEnvelopeV2`].
pub const RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION: u32 = 2;
/// Maximum serialized bytes retained for inline runtime event metadata.
pub const RUNTIME_EVENT_MAX_INLINE_METADATA_BYTES: usize = 8 * 1024;
/// Maximum extension fields retained from a less-trusted producer.
pub const RUNTIME_EVENT_MAX_EXTENSION_FIELDS: usize = 16;
/// Reserved extension key carrying bounded legacy identity adaptation evidence.
pub const RUNTIME_EVENT_LEGACY_IDENTITY_ADAPTER_EXTENSION: &str = "legacy_identity_adapter";

runtime_contract_enum! {
    /// Actor classes that may produce runtime events after host validation.
    pub enum RuntimeEventActorKind {
        Host => "host",
        Harness => "harness",
        Provider => "provider",
        Tool => "tool",
        Plugin => "plugin",
        Worker => "worker",
        Operator => "operator"
    }
}

runtime_contract_enum! {
    /// Redaction posture selected by the host before persistence.
    pub enum RuntimeEventRedactionClass {
        MetadataOnly => "metadata_only",
        HashOnly => "hash_only",
        RedactedText => "redacted_text",
        RedactedJson => "redacted_json",
        ArtifactReference => "artifact_reference"
    }
}

runtime_contract_enum! {
    /// Canonical events introduced by the shared runtime layer.
    pub enum RuntimeEventName {
        GenerationActivated => "runtime.generation.activated",
        GenerationSuperseded => "runtime.generation.superseded",
        StaleEventSuppressed => "runtime.stale_event.suppressed",
        RunQueued => "run.queued",
        RunStarted => "run.started",
        HarnessAttemptStarted => "harness.attempt.started",
        HarnessAttemptCompleted => "harness.attempt.completed",
        HarnessAttemptFailed => "harness.attempt.failed",
        HarnessAttemptCancelled => "harness.attempt.cancelled",
        HarnessAttemptCleanedUp => "harness.attempt.cleaned_up",
        ProviderAttemptStarted => "provider.attempt.started",
        ProviderAttemptCompleted => "provider.attempt.completed",
        ModelDelta => "model.delta",
        ToolProposed => "tool.proposed",
        ToolDecisionRecorded => "tool.decision.recorded",
        ToolIntentRecorded => "tool.intent.recorded",
        ToolEffectStarted => "tool.effect.started",
        ToolEffectObserved => "tool.effect.observed",
        ToolEffectUnknown => "tool.effect.unknown",
        ToolEffectCleanupReconciled => "tool.effect.cleanup_reconciled",
        ToolEffectCleanupUnknown => "tool.effect.cleanup_unknown",
        ToolEffectReceiptReconciled => "tool.effect.receipt_reconciled",
        ToolEffectReconciled => "tool.effect.reconciled",
        ToolEffectAbandoned => "tool.effect.abandoned",
        ToolResultObserved => "tool.result.observed",
        ToolAttestationObserved => "tool.attestation.observed",
        ApprovalRequired => "approval.required",
        ApprovalResolved => "approval.resolved",
        DeliveryIntentRecorded => "delivery.intent.recorded",
        CleanupCompleted => "runtime.cleanup.completed",
        CleanupPartial => "runtime.cleanup.partial",
        CleanupUnknown => "runtime.cleanup.unknown",
        RunCompleted => "run.completed",
        RunFailed => "run.failed",
        RunCancelled => "run.cancelled",
        BackpressureApplied => "runtime.backpressure.applied",
        CompatibilityBlocked => "runtime.compatibility.blocked"
    }
}

/// Closed semantic contract for one canonical runtime event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RuntimeEventDescriptor {
    /// Canonical event name.
    pub name: RuntimeEventName,
    /// Generation lane that authorizes persistence.
    pub generation_lane: RuntimeGenerationLane,
    /// Owning subsystem.
    pub subsystem: RuntimeSubsystem,
    /// Required runtime phase.
    pub phase: RuntimeErrorPhase,
    /// Actor allowed to emit the event after host validation.
    pub actor_kind: RuntimeEventActorKind,
    /// Required retry posture.
    pub retryability: RuntimeRetryability,
    /// Required redaction posture.
    pub redaction_class: RuntimeEventRedactionClass,
    /// Whether the event closes its generation.
    pub terminal: bool,
    /// Required typed identity fields.
    pub required_identity_fields: &'static [&'static str],
}

const NO_REQUIRED_IDENTITIES: &[&str] = &[];
const ATTEMPT_IDENTITIES: &[&str] = &["attempt_id"];
const PROPOSAL_IDENTITIES: &[&str] = &["tool_proposal_id"];
const APPROVAL_IDENTITIES: &[&str] = &["tool_proposal_id", "approval_subject_id"];
const EFFECT_IDENTITIES: &[&str] = &["tool_execution_id", "operation_id"];
const RESULT_IDENTITIES: &[&str] = &["tool_proposal_id", "tool_execution_id", "operation_id"];
const DELIVERY_IDENTITIES: &[&str] = &["delivery_intent_id", "operation_id"];

#[allow(clippy::too_many_arguments)]
const fn runtime_event_descriptor(
    name: RuntimeEventName,
    generation_lane: RuntimeGenerationLane,
    subsystem: RuntimeSubsystem,
    phase: RuntimeErrorPhase,
    actor_kind: RuntimeEventActorKind,
    retryability: RuntimeRetryability,
    redaction_class: RuntimeEventRedactionClass,
    terminal: bool,
    required_identity_fields: &'static [&'static str],
) -> RuntimeEventDescriptor {
    RuntimeEventDescriptor {
        name,
        generation_lane,
        subsystem,
        phase,
        actor_kind,
        retryability,
        redaction_class,
        terminal,
        required_identity_fields,
    }
}

/// Complete descriptor registry for generation-aware runtime events.
pub const RUNTIME_EVENT_DESCRIPTORS: &[RuntimeEventDescriptor] = &[
    runtime_event_descriptor(
        RuntimeEventName::GenerationActivated,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Admission,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::GenerationSuperseded,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::StaleEventSuppressed,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::HashOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::RunQueued,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RunStream,
        RuntimeErrorPhase::Admission,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::RunStarted,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RunStream,
        RuntimeErrorPhase::Admission,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::HarnessAttemptStarted,
        RuntimeGenerationLane::Harness,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::RuntimeSelection,
        RuntimeEventActorKind::Harness,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        ATTEMPT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::HarnessAttemptCompleted,
        RuntimeGenerationLane::Harness,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Finalization,
        RuntimeEventActorKind::Harness,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        true,
        ATTEMPT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::HarnessAttemptFailed,
        RuntimeGenerationLane::Harness,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Finalization,
        RuntimeEventActorKind::Harness,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        true,
        ATTEMPT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::HarnessAttemptCancelled,
        RuntimeGenerationLane::Harness,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Cancellation,
        RuntimeEventActorKind::Harness,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        true,
        ATTEMPT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::HarnessAttemptCleanedUp,
        RuntimeGenerationLane::Harness,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Harness,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        ATTEMPT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ProviderAttemptStarted,
        RuntimeGenerationLane::Provider,
        RuntimeSubsystem::Provider,
        RuntimeErrorPhase::ProviderCall,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        ATTEMPT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ProviderAttemptCompleted,
        RuntimeGenerationLane::Provider,
        RuntimeSubsystem::Provider,
        RuntimeErrorPhase::ProviderFinalization,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        ATTEMPT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ModelDelta,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Provider,
        RuntimeErrorPhase::ProviderCall,
        RuntimeEventActorKind::Provider,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolProposed,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::ToolValidation,
        RuntimeEventActorKind::Provider,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        PROPOSAL_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolDecisionRecorded,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Policy,
        RuntimeErrorPhase::ToolGate,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        PROPOSAL_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolIntentRecorded,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::ToolExecution,
        RuntimeEventActorKind::Tool,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolEffectStarted,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::ToolExecution,
        RuntimeEventActorKind::Tool,
        RuntimeRetryability::RequiresIdempotencyGuard,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolEffectObserved,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::ToolExecution,
        RuntimeEventActorKind::Tool,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolEffectUnknown,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::ToolExecution,
        RuntimeEventActorKind::Tool,
        RuntimeRetryability::RequiresOperatorReview,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolEffectCleanupReconciled,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolEffectCleanupUnknown,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::RequiresOperatorReview,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolEffectReceiptReconciled,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolEffectReconciled,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Operator,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolEffectAbandoned,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Operator,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::HashOnly,
        false,
        EFFECT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolResultObserved,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::ResultProjection,
        RuntimeEventActorKind::Tool,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        RESULT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ToolAttestationObserved,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Tool,
        RuntimeErrorPhase::Verification,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        RESULT_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ApprovalRequired,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Approval,
        RuntimeErrorPhase::Approval,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::RequiresApproval,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        APPROVAL_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::ApprovalResolved,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::Approval,
        RuntimeErrorPhase::Approval,
        RuntimeEventActorKind::Operator,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        APPROVAL_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::DeliveryIntentRecorded,
        RuntimeGenerationLane::Delivery,
        RuntimeSubsystem::Delivery,
        RuntimeErrorPhase::DeliveryIntent,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::RequiresIdempotencyGuard,
        RuntimeEventRedactionClass::HashOnly,
        false,
        DELIVERY_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::CleanupCompleted,
        RuntimeGenerationLane::Process,
        RuntimeSubsystem::Recovery,
        RuntimeErrorPhase::Finalization,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::HashOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::CleanupPartial,
        RuntimeGenerationLane::Process,
        RuntimeSubsystem::Recovery,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::RequiresOperatorReview,
        RuntimeEventRedactionClass::HashOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::CleanupUnknown,
        RuntimeGenerationLane::Process,
        RuntimeSubsystem::Recovery,
        RuntimeErrorPhase::Recovery,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::RequiresOperatorReview,
        RuntimeEventRedactionClass::HashOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::RunCompleted,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RunStream,
        RuntimeErrorPhase::Finalization,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        true,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::RunFailed,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RunStream,
        RuntimeErrorPhase::Finalization,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        true,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::RunCancelled,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RunStream,
        RuntimeErrorPhase::Cancellation,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::NotRetryable,
        RuntimeEventRedactionClass::MetadataOnly,
        true,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::BackpressureApplied,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Queueing,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::SafeAfterBackoff,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
    runtime_event_descriptor(
        RuntimeEventName::CompatibilityBlocked,
        RuntimeGenerationLane::Run,
        RuntimeSubsystem::RuntimeKernel,
        RuntimeErrorPhase::Admission,
        RuntimeEventActorKind::Host,
        RuntimeRetryability::RequiresOperatorReview,
        RuntimeEventRedactionClass::MetadataOnly,
        false,
        NO_REQUIRED_IDENTITIES,
    ),
];

impl RuntimeEventName {
    /// Returns the complete semantic descriptor for this event.
    #[must_use]
    pub const fn descriptor(self) -> &'static RuntimeEventDescriptor {
        match self {
            Self::GenerationActivated => &RUNTIME_EVENT_DESCRIPTORS[0],
            Self::GenerationSuperseded => &RUNTIME_EVENT_DESCRIPTORS[1],
            Self::StaleEventSuppressed => &RUNTIME_EVENT_DESCRIPTORS[2],
            Self::RunQueued => &RUNTIME_EVENT_DESCRIPTORS[3],
            Self::RunStarted => &RUNTIME_EVENT_DESCRIPTORS[4],
            Self::HarnessAttemptStarted => &RUNTIME_EVENT_DESCRIPTORS[5],
            Self::HarnessAttemptCompleted => &RUNTIME_EVENT_DESCRIPTORS[6],
            Self::HarnessAttemptFailed => &RUNTIME_EVENT_DESCRIPTORS[7],
            Self::HarnessAttemptCancelled => &RUNTIME_EVENT_DESCRIPTORS[8],
            Self::HarnessAttemptCleanedUp => &RUNTIME_EVENT_DESCRIPTORS[9],
            Self::ProviderAttemptStarted => &RUNTIME_EVENT_DESCRIPTORS[10],
            Self::ProviderAttemptCompleted => &RUNTIME_EVENT_DESCRIPTORS[11],
            Self::ModelDelta => &RUNTIME_EVENT_DESCRIPTORS[12],
            Self::ToolProposed => &RUNTIME_EVENT_DESCRIPTORS[13],
            Self::ToolDecisionRecorded => &RUNTIME_EVENT_DESCRIPTORS[14],
            Self::ToolIntentRecorded => &RUNTIME_EVENT_DESCRIPTORS[15],
            Self::ToolEffectStarted => &RUNTIME_EVENT_DESCRIPTORS[16],
            Self::ToolEffectObserved => &RUNTIME_EVENT_DESCRIPTORS[17],
            Self::ToolEffectUnknown => &RUNTIME_EVENT_DESCRIPTORS[18],
            Self::ToolEffectCleanupReconciled => &RUNTIME_EVENT_DESCRIPTORS[19],
            Self::ToolEffectCleanupUnknown => &RUNTIME_EVENT_DESCRIPTORS[20],
            Self::ToolEffectReceiptReconciled => &RUNTIME_EVENT_DESCRIPTORS[21],
            Self::ToolEffectReconciled => &RUNTIME_EVENT_DESCRIPTORS[22],
            Self::ToolEffectAbandoned => &RUNTIME_EVENT_DESCRIPTORS[23],
            Self::ToolResultObserved => &RUNTIME_EVENT_DESCRIPTORS[24],
            Self::ToolAttestationObserved => &RUNTIME_EVENT_DESCRIPTORS[25],
            Self::ApprovalRequired => &RUNTIME_EVENT_DESCRIPTORS[26],
            Self::ApprovalResolved => &RUNTIME_EVENT_DESCRIPTORS[27],
            Self::DeliveryIntentRecorded => &RUNTIME_EVENT_DESCRIPTORS[28],
            Self::CleanupCompleted => &RUNTIME_EVENT_DESCRIPTORS[29],
            Self::CleanupPartial => &RUNTIME_EVENT_DESCRIPTORS[30],
            Self::CleanupUnknown => &RUNTIME_EVENT_DESCRIPTORS[31],
            Self::RunCompleted => &RUNTIME_EVENT_DESCRIPTORS[32],
            Self::RunFailed => &RUNTIME_EVENT_DESCRIPTORS[33],
            Self::RunCancelled => &RUNTIME_EVENT_DESCRIPTORS[34],
            Self::BackpressureApplied => &RUNTIME_EVENT_DESCRIPTORS[35],
            Self::CompatibilityBlocked => &RUNTIME_EVENT_DESCRIPTORS[36],
        }
    }

    /// Returns whether this event closes a run generation.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        self.descriptor().terminal
    }

    /// Returns required identity fields for this event family.
    #[must_use]
    pub const fn required_identity_fields(self) -> &'static [&'static str] {
        self.descriptor().required_identity_fields
    }
}

/// Bounded payload carried by a runtime event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeEventPayloadRef {
    /// Small sanitized metadata retained inline.
    Inline {
        /// Host-sanitized JSON metadata.
        metadata: Value,
    },
    /// Larger content stored in an approval-gated artifact.
    Artifact {
        /// Opaque artifact identity.
        artifact_id: String,
        /// SHA-256 digest of the stored payload.
        digest_sha256: String,
        /// Original payload byte count.
        size_bytes: u64,
    },
    /// Payload omitted because the boundary permits metadata only.
    Omitted {
        /// Stable omission reason.
        reason_code: String,
        /// Hash of the omitted payload when safe to compute.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        digest_sha256: Option<String>,
        /// Observed byte count.
        size_bytes: u64,
    },
}

/// Host-authoritative runtime event shared by journal, replay, QA, and diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeEventEnvelopeV2 {
    /// Contract schema version; must equal 2.
    pub schema_version: u32,
    /// Unique event identity.
    pub event_id: super::RuntimeEventId,
    /// Typed runtime identities.
    pub identities: RuntimeIdentitySetV1,
    /// Monotonic sequence within one generation.
    pub sequence: u64,
    /// Causal parent event identity when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_parent_event_id: Option<super::RuntimeEventId>,
    /// Owning subsystem.
    pub subsystem: RuntimeSubsystem,
    /// Runtime phase.
    pub phase: RuntimeErrorPhase,
    /// Canonical event name.
    pub event_name: RuntimeEventName,
    /// Stable reason code.
    pub reason_code: String,
    /// Actor that produced the event.
    pub actor_kind: RuntimeEventActorKind,
    /// Structured retry posture.
    pub retryability: RuntimeRetryability,
    /// Host-selected redaction posture.
    pub redaction_class: RuntimeEventRedactionClass,
    /// Whether this event closes the generation.
    pub terminal: bool,
    /// Bounded metadata or artifact reference.
    pub payload: RuntimeEventPayloadRef,
    /// Unix timestamp in milliseconds.
    pub occurred_at_unix_ms: i64,
    /// Bounded forward-compatible metadata.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
}

/// Event validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeEventValidationError {
    /// Envelope schema version is unsupported.
    #[error("runtime event schema version {observed} is unsupported")]
    UnsupportedSchemaVersion { observed: u32 },
    /// Stable reason code is missing or oversized.
    #[error("runtime event reason code is invalid")]
    InvalidReasonCode,
    /// Envelope metadata contradicts the descriptor registry.
    #[error("runtime event {event_name} field {field} does not match event registry")]
    DescriptorMismatch { event_name: String, field: &'static str },
    /// Terminal events cannot be retried automatically.
    #[error("terminal runtime event cannot allow automatic retry")]
    TerminalRetryable,
    /// Required typed identity is missing.
    #[error("runtime event {event_name} requires identity field {field}")]
    MissingIdentity { event_name: String, field: &'static str },
    /// Inline metadata exceeds the hard cap.
    #[error(
        "runtime event inline metadata exceeds {RUNTIME_EVENT_MAX_INLINE_METADATA_BYTES} bytes"
    )]
    InlineMetadataTooLarge,
    /// Artifact or digest metadata is malformed.
    #[error("runtime event payload reference is invalid")]
    InvalidPayloadReference,
    /// Timestamp or extension bounds are invalid.
    #[error("runtime event metadata is outside contract bounds")]
    InvalidMetadata,
    /// Legacy identity adaptation evidence is malformed or conflicts with typed identities.
    #[error("runtime event legacy identity adapter is invalid")]
    InvalidLegacyIdentityAdapter,
    /// Sequence was not strictly monotonic.
    #[error("runtime event sequence must increase monotonically")]
    NonMonotonicSequence,
    /// A generation emitted more than one terminal event.
    #[error("runtime generation emitted more than one terminal event")]
    DuplicateTerminal,
}

impl RuntimeEventEnvelopeV2 {
    /// Records validated legacy identity adaptation evidence in the reserved extension field.
    ///
    /// # Errors
    /// Returns [`RuntimeIdentityError`] when the adapter is outside its bounded contract.
    pub fn record_legacy_identity_adapter(
        &mut self,
        adapter: LegacyRuntimeIdentityAdapter,
    ) -> Result<(), RuntimeIdentityError> {
        adapter.validate()?;
        let value = serde_json::to_value(adapter)
            .map_err(|_| RuntimeIdentityError::InvalidLegacyAdapterMetadata)?;
        self.extensions.insert(RUNTIME_EVENT_LEGACY_IDENTITY_ADAPTER_EXTENSION.to_owned(), value);
        Ok(())
    }

    /// Validates the envelope against the closed event registry and hard bounds.
    ///
    /// # Errors
    /// Returns [`RuntimeEventValidationError`] when correlation, ordering metadata,
    /// terminal posture, retryability, or payload bounds violate the contract.
    pub fn validate(&self) -> Result<(), RuntimeEventValidationError> {
        if self.schema_version != RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION {
            return Err(RuntimeEventValidationError::UnsupportedSchemaVersion {
                observed: self.schema_version,
            });
        }
        self.identities.validate().map_err(|_| RuntimeEventValidationError::InvalidMetadata)?;
        if !is_runtime_metadata_name(self.reason_code.as_str(), 128) {
            return Err(RuntimeEventValidationError::InvalidReasonCode);
        }
        let descriptor = self.event_name.descriptor();
        validate_descriptor_field(
            self.subsystem == descriptor.subsystem,
            self.event_name,
            "subsystem",
        )?;
        validate_descriptor_field(self.phase == descriptor.phase, self.event_name, "phase")?;
        validate_descriptor_field(
            self.actor_kind == descriptor.actor_kind,
            self.event_name,
            "actor_kind",
        )?;
        validate_descriptor_field(
            self.retryability == descriptor.retryability,
            self.event_name,
            "retryability",
        )?;
        validate_descriptor_field(
            self.redaction_class == descriptor.redaction_class,
            self.event_name,
            "redaction_class",
        )?;
        validate_descriptor_field(
            self.terminal == descriptor.terminal,
            self.event_name,
            "terminal",
        )?;
        if self.terminal && self.retryability.allows_automatic_retry() {
            return Err(RuntimeEventValidationError::TerminalRetryable);
        }
        for field in descriptor.required_identity_fields {
            if !identity_field_present(&self.identities, field) {
                return Err(RuntimeEventValidationError::MissingIdentity {
                    event_name: self.event_name.as_str().to_owned(),
                    field,
                });
            }
        }
        if self.occurred_at_unix_ms < 0
            || self.extensions.len() > RUNTIME_EVENT_MAX_EXTENSION_FIELDS
            || self.extensions.keys().any(|key| key.trim().is_empty() || key.len() > 64)
        {
            return Err(RuntimeEventValidationError::InvalidMetadata);
        }
        self.validate_legacy_identity_adapter()?;
        validate_payload(&self.payload)
    }

    fn validate_legacy_identity_adapter(&self) -> Result<(), RuntimeEventValidationError> {
        let Some(value) = self.extensions.get(RUNTIME_EVENT_LEGACY_IDENTITY_ADAPTER_EXTENSION)
        else {
            return Ok(());
        };
        let adapter: LegacyRuntimeIdentityAdapter = serde_json::from_value(value.clone())
            .map_err(|_| RuntimeEventValidationError::InvalidLegacyIdentityAdapter)?;
        adapter
            .validate()
            .map_err(|_| RuntimeEventValidationError::InvalidLegacyIdentityAdapter)?;
        for field in &adapter.missing_fields {
            if identity_field_present(&self.identities, field) {
                return Err(RuntimeEventValidationError::InvalidLegacyIdentityAdapter);
            }
        }
        Ok(())
    }
}

fn validate_descriptor_field(
    matches: bool,
    event_name: RuntimeEventName,
    field: &'static str,
) -> Result<(), RuntimeEventValidationError> {
    if matches {
        return Ok(());
    }
    Err(RuntimeEventValidationError::DescriptorMismatch {
        event_name: event_name.as_str().to_owned(),
        field,
    })
}

fn identity_field_present(identities: &RuntimeIdentitySetV1, field: &str) -> bool {
    match field {
        "attempt_id" => identities.attempt_id.is_some(),
        "tool_proposal_id" => identities.tool_proposal_id.is_some(),
        "tool_execution_id" => identities.tool_execution_id.is_some(),
        "approval_subject_id" => identities.approval_subject_id.is_some(),
        "delivery_intent_id" => identities.delivery_intent_id.is_some(),
        "plugin_call_id" => identities.plugin_call_id.is_some(),
        "context_projection_id" => identities.context_projection_id.is_some(),
        "recovery_action_id" => identities.recovery_action_id.is_some(),
        "operation_id" => identities.operation_id.is_some(),
        "runtime_instance_id" => identities.runtime_instance_id.is_some(),
        _ => false,
    }
}

fn validate_payload(payload: &RuntimeEventPayloadRef) -> Result<(), RuntimeEventValidationError> {
    match payload {
        RuntimeEventPayloadRef::Inline { metadata } => {
            let bytes = serde_json::to_vec(metadata)
                .map_err(|_| RuntimeEventValidationError::InvalidPayloadReference)?;
            if bytes.len() > RUNTIME_EVENT_MAX_INLINE_METADATA_BYTES {
                return Err(RuntimeEventValidationError::InlineMetadataTooLarge);
            }
        }
        RuntimeEventPayloadRef::Artifact { artifact_id, digest_sha256, size_bytes } => {
            if !is_runtime_identity_value(artifact_id)
                || !is_sha256(digest_sha256)
                || *size_bytes == 0
            {
                return Err(RuntimeEventValidationError::InvalidPayloadReference);
            }
        }
        RuntimeEventPayloadRef::Omitted { reason_code, digest_sha256, .. } => {
            if !is_runtime_metadata_name(reason_code, 128)
                || digest_sha256.as_deref().is_some_and(|digest| !is_sha256(digest))
            {
                return Err(RuntimeEventValidationError::InvalidPayloadReference);
            }
        }
    }
    Ok(())
}

fn is_runtime_metadata_name(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

fn is_runtime_identity_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= super::MAX_RUNTIME_ID_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// In-memory validator used by tests and adapters before journal allocation.
#[derive(Debug, Default)]
pub struct RuntimeEventSequenceValidator {
    last_sequence: BTreeMap<(String, String, u64), u64>,
    terminal_generations: BTreeSet<(String, String, u64)>,
}

impl RuntimeEventSequenceValidator {
    /// Validates and records an event in generation order.
    ///
    /// # Errors
    /// Returns [`RuntimeEventValidationError::NonMonotonicSequence`] or
    /// [`RuntimeEventValidationError::DuplicateTerminal`] for invalid ordering.
    pub fn observe(
        &mut self,
        event: &RuntimeEventEnvelopeV2,
    ) -> Result<(), RuntimeEventValidationError> {
        event.validate()?;
        let key = (
            event.identities.session_id.as_str().to_owned(),
            event.event_name.descriptor().generation_lane.as_str().to_owned(),
            event.identities.generation.get(),
        );
        if self.last_sequence.get(&key).is_some_and(|last| event.sequence <= *last) {
            return Err(RuntimeEventValidationError::NonMonotonicSequence);
        }
        if event.terminal && !self.terminal_generations.insert(key.clone()) {
            return Err(RuntimeEventValidationError::DuplicateTerminal);
        }
        self.last_sequence.insert(key, event.sequence);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_contracts::{
        RuntimeGeneration, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
    };
    use proptest::prelude::*;

    fn event(sequence: u64, name: RuntimeEventName) -> RuntimeEventEnvelopeV2 {
        event_for_session(sequence, name, "session_01")
    }

    fn event_for_session(
        sequence: u64,
        name: RuntimeEventName,
        session_id: &str,
    ) -> RuntimeEventEnvelopeV2 {
        let descriptor = name.descriptor();
        RuntimeEventEnvelopeV2 {
            schema_version: 2,
            event_id: super::super::RuntimeEventId::parse(format!("event_{sequence}").as_str())
                .expect("event id"),
            identities: RuntimeIdentitySetV1::for_run(
                RuntimeTraceId::parse("trace_01").expect("trace id"),
                RuntimeSessionId::parse(session_id).expect("session id"),
                RuntimeRunId::parse("run_01").expect("run id"),
                RuntimeGeneration::new(1).expect("generation"),
            ),
            sequence,
            causal_parent_event_id: None,
            subsystem: descriptor.subsystem,
            phase: descriptor.phase,
            event_name: name,
            reason_code: "runtime.test".to_owned(),
            actor_kind: descriptor.actor_kind,
            retryability: descriptor.retryability,
            redaction_class: descriptor.redaction_class,
            terminal: descriptor.terminal,
            payload: RuntimeEventPayloadRef::Inline { metadata: serde_json::json!({}) },
            occurred_at_unix_ms: 42,
            extensions: BTreeMap::new(),
        }
    }

    proptest! {
        #[test]
        fn increasing_sequences_are_accepted(start in 0_u64..1_000, len in 1_usize..32) {
            let mut validator = RuntimeEventSequenceValidator::default();
            for offset in 0..len {
                let sequence = start + u64::try_from(offset).expect("small offset");
                validator.observe(&event(sequence, RuntimeEventName::RunStarted))?;
            }
        }
    }

    #[test]
    fn sequence_validator_uses_session_lane_generation_authority() {
        let mut validator = RuntimeEventSequenceValidator::default();
        validator
            .observe(&event_for_session(1, RuntimeEventName::RunStarted, "session_01"))
            .expect("first session event should pass");
        assert_eq!(
            validator.observe(&event_for_session(1, RuntimeEventName::RunStarted, "session_01")),
            Err(RuntimeEventValidationError::NonMonotonicSequence)
        );
        validator
            .observe(&event_for_session(1, RuntimeEventName::RunStarted, "session_02"))
            .expect("independent session lane should start at one");
    }

    #[test]
    fn descriptor_registry_covers_every_event_name_once() {
        assert_eq!(RUNTIME_EVENT_DESCRIPTORS.len(), RuntimeEventName::wire_contract_values().len());
        let unique_names = RUNTIME_EVENT_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(unique_names.len(), RUNTIME_EVENT_DESCRIPTORS.len());
        for descriptor in RUNTIME_EVENT_DESCRIPTORS {
            assert_eq!(descriptor.name.descriptor(), descriptor);
        }
    }

    #[test]
    fn descriptor_mismatch_is_rejected() {
        let mut wrong_phase = event(1, RuntimeEventName::RunStarted);
        wrong_phase.phase = RuntimeErrorPhase::Finalization;
        assert_eq!(
            wrong_phase.validate(),
            Err(RuntimeEventValidationError::DescriptorMismatch {
                event_name: RuntimeEventName::RunStarted.as_str().to_owned(),
                field: "phase",
            })
        );

        let mut wrong_actor = event(2, RuntimeEventName::ApprovalRequired);
        wrong_actor.actor_kind = RuntimeEventActorKind::Operator;
        assert_eq!(
            wrong_actor.validate(),
            Err(RuntimeEventValidationError::DescriptorMismatch {
                event_name: RuntimeEventName::ApprovalRequired.as_str().to_owned(),
                field: "actor_kind",
            })
        );
    }

    #[test]
    fn descriptor_required_identities_are_enforced() {
        let provider_attempt = event(1, RuntimeEventName::ProviderAttemptCompleted);
        assert_eq!(
            provider_attempt.validate(),
            Err(RuntimeEventValidationError::MissingIdentity {
                event_name: RuntimeEventName::ProviderAttemptCompleted.as_str().to_owned(),
                field: "attempt_id",
            })
        );

        let tool_result = event(2, RuntimeEventName::ToolResultObserved);
        assert_eq!(
            tool_result.validate(),
            Err(RuntimeEventValidationError::MissingIdentity {
                event_name: RuntimeEventName::ToolResultObserved.as_str().to_owned(),
                field: "tool_proposal_id",
            })
        );
    }

    #[test]
    fn duplicate_terminal_is_rejected() {
        let mut validator = RuntimeEventSequenceValidator::default();
        validator.observe(&event(1, RuntimeEventName::RunCompleted)).expect("first terminal");
        assert_eq!(
            validator.observe(&event(2, RuntimeEventName::RunFailed)),
            Err(RuntimeEventValidationError::DuplicateTerminal)
        );
    }

    #[test]
    fn oversize_inline_payload_is_rejected() {
        let mut event = event(1, RuntimeEventName::RunStarted);
        event.payload = RuntimeEventPayloadRef::Inline {
            metadata: serde_json::json!({"value": "x".repeat(RUNTIME_EVENT_MAX_INLINE_METADATA_BYTES)}),
        };
        assert_eq!(event.validate(), Err(RuntimeEventValidationError::InlineMetadataTooLarge));
    }

    #[test]
    fn legacy_identity_adapter_round_trips_as_bounded_extension() {
        let generation = RuntimeGeneration::new(1).expect("generation");
        let (identities, adapter) =
            RuntimeIdentitySetV1::from_legacy_run("session_01", "run_01", generation)
                .expect("legacy identities should adapt");
        let mut event = event(1, RuntimeEventName::RunStarted);
        event.identities = identities;
        event.record_legacy_identity_adapter(adapter.clone()).expect("adapter should record");

        event.validate().expect("adapter evidence should validate");
        let encoded = serde_json::to_value(&event).expect("event should serialize");
        assert_eq!(
            encoded.pointer("/extensions/legacy_identity_adapter"),
            Some(&serde_json::to_value(adapter).expect("adapter should serialize"))
        );
        let encoded_text = encoded.to_string();
        assert!(!encoded_text.contains("raw_prompt"));
        assert!(!encoded_text.contains("environment"));
        assert!(!encoded_text.contains("secret"));
    }

    #[test]
    fn legacy_identity_adapter_rejects_present_missing_identity() {
        let generation = RuntimeGeneration::new(1).expect("generation");
        let (identities, adapter) =
            RuntimeIdentitySetV1::from_legacy_run("session_01", "run_01", generation)
                .expect("legacy identities should adapt");
        let mut event = event(1, RuntimeEventName::RunStarted);
        event.identities = identities;
        event.identities.attempt_id =
            Some(super::super::RuntimeAttemptId::parse("attempt_01").expect("attempt id"));
        event.record_legacy_identity_adapter(adapter).expect("adapter should record");

        assert_eq!(
            event.validate(),
            Err(RuntimeEventValidationError::InvalidLegacyIdentityAdapter)
        );
    }
}
