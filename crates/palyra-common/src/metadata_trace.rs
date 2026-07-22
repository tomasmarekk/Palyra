//! Strict metadata-only trace contract for production runtime paths.
//!
//! This module exposes only bounded machine metadata and domain-separated hashes;
//! rich prompts, tool arguments, provider payloads, and diagnostic text have no wire fields.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

/// Current schema version for [`MetadataTraceV1`].
pub const METADATA_TRACE_SCHEMA_VERSION: u32 = 1;
/// Maximum append-only segments retained for one run trace.
pub const METADATA_TRACE_MAX_SEGMENTS: usize = 16;
/// Maximum events retained across all segments of one run trace.
pub const METADATA_TRACE_MAX_EVENTS: usize = 512;
/// Maximum canonical JSON bytes occupied by one trace event.
pub const METADATA_TRACE_MAX_EVENT_BYTES: usize = 2_048;
/// Maximum schema digests carried by one runtime-selection event.
pub const METADATA_TRACE_MAX_SCHEMA_HASHES: usize = 8;
/// Maximum duration represented by one stage timing.
pub const METADATA_TRACE_MAX_STAGE_DURATION_MS: u64 = 86_400_000;
/// Maximum representable recording timestamp (end of year 9999 UTC).
pub const METADATA_TRACE_MAX_UNIX_MS: u64 = 253_402_300_799_999;
/// Maximum provider, tool, or recovery attempt number.
pub const METADATA_TRACE_MAX_ATTEMPTS: u16 = 64;
/// Maximum context-item count represented by the metadata trace.
pub const METADATA_TRACE_MAX_CONTEXT_ITEMS: u32 = 100_000;
/// Maximum source identifier bytes accepted by [`metadata_trace_id_sha256`].
pub const METADATA_TRACE_MAX_ID_SOURCE_BYTES: usize = 4_096;

const MAX_MACHINE_IDENTIFIER_BYTES: usize = 96;
const MAX_REASON_CODE_BYTES: usize = 128;
const MAX_CAPACITY_OBSERVED: u32 = 1_000_000;

/// Metadata-only projection of one run's append-only trace segments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataTraceV1 {
    /// Contract version for this trace.
    pub schema_version: u32,
    /// Domain-separated SHA-256 digest of the run identifier.
    pub run_id_sha256: String,
    /// Domain-separated SHA-256 digest of the owning session identifier.
    pub session_id_sha256: String,
    /// Ordered append-only trace segments.
    pub segments: Vec<MetadataTraceSegmentV1>,
}

/// One crash-safe append-only segment of a metadata trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataTraceSegmentV1 {
    /// Domain-separated SHA-256 digest of the segment identifier.
    pub segment_id_sha256: String,
    /// Zero-based append order within the run trace.
    pub segment_index: u16,
    /// One-based runtime generation represented by this segment.
    pub generation: u32,
    /// Durable interpretation of the segment's valid prefix.
    pub status: MetadataTraceSegmentStatusV1,
    /// Events in global trace-sequence order.
    pub events: Vec<MetadataTraceEventV1>,
}

/// Status assigned to the valid prefix of an append-only trace segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceSegmentStatusV1 {
    /// The segment ended with a durable terminalization event.
    Complete,
    /// The writer stopped before terminalization or the segment remains active.
    Interrupted,
    /// A corrupt suffix was excluded while preserving the verified prefix.
    CorruptSuffixIsolated,
}

impl MetadataTraceSegmentStatusV1 {
    /// Returns the stable wire value for this status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Interrupted => "interrupted",
            Self::CorruptSuffixIsolated => "corrupt_suffix_isolated",
        }
    }
}

/// One bounded event in a metadata trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataTraceEventV1 {
    /// Zero-based global event order across every segment.
    pub sequence: u32,
    /// One-based runtime generation that emitted the event.
    pub generation: u32,
    /// Wall-clock recording time in Unix milliseconds.
    pub recorded_at_unix_ms: u64,
    /// Domain-separated SHA-256 digest of the event identifier.
    pub event_id_sha256: String,
    /// Digest of an earlier causal event, absent only for the root event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_parent_event_id_sha256: Option<String>,
    /// Bounded elapsed time for the represented stage, when measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_duration_ms: Option<u64>,
    /// Closed, typed event payload.
    pub event: MetadataTraceEventDataV1,
}

/// Closed event vocabulary for the metadata trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "metadata", rename_all = "snake_case", deny_unknown_fields)]
pub enum MetadataTraceEventDataV1 {
    /// A newly admitted run received its root trace event.
    RunStarted(RunStartedMetadataV1),
    /// The runtime and harness path were selected.
    RuntimeSelected(RuntimeSelectedMetadataV1),
    /// A side-effect-free V2 shadow plan was compared with the authoritative legacy plan.
    RuntimeShadowDifferential(RuntimeShadowDifferentialMetadataV1),
    /// Bounded context assembly completed.
    ContextAssembled(ContextAssembledMetadataV1),
    /// A provider route attempt changed phase.
    ProviderAttempt(ProviderAttemptMetadataV1),
    /// A tool call crossed the policy gate.
    ToolGate(ToolGateMetadataV1),
    /// An approval changed state.
    Approval(ApprovalMetadataV1),
    /// A tool call reached an observable outcome.
    ToolOutcome(ToolOutcomeMetadataV1),
    /// Runtime recovery selected a bounded strategy.
    Recovery(RecoveryMetadataV1),
    /// A delivery intent changed durable state.
    DeliveryIntent(DeliveryIntentMetadataV1),
    /// The run reached one terminal outcome.
    Terminalization(TerminalizationMetadataV1),
    /// A new segment continued an interrupted generation.
    RecoveryContinuation(RecoveryContinuationMetadataV1),
    /// A deterministic contract cap prevented further metadata capture.
    CapacityReached(CapacityReachedMetadataV1),
}

impl MetadataTraceEventDataV1 {
    /// Returns the stable wire event kind.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RunStarted(_) => "run_started",
            Self::RuntimeSelected(_) => "runtime_selected",
            Self::RuntimeShadowDifferential(_) => "runtime_shadow_differential",
            Self::ContextAssembled(_) => "context_assembled",
            Self::ProviderAttempt(_) => "provider_attempt",
            Self::ToolGate(_) => "tool_gate",
            Self::Approval(_) => "approval",
            Self::ToolOutcome(_) => "tool_outcome",
            Self::Recovery(_) => "recovery",
            Self::DeliveryIntent(_) => "delivery_intent",
            Self::Terminalization(_) => "terminalization",
            Self::RecoveryContinuation(_) => "recovery_continuation",
            Self::CapacityReached(_) => "capacity_reached",
        }
    }
}

/// Metadata emitted when a root trace segment is created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunStartedMetadataV1 {
    /// Admission path that created the root trace.
    pub entrypoint: MetadataTraceEntrypointV1,
}

/// Metadata emitted after runtime-path selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSelectedMetadataV1 {
    /// Selected harness identifier.
    pub harness_id: String,
    /// Selected harness implementation version.
    pub harness_version: String,
    /// Selected runtime identifier.
    pub runtime_id: String,
    /// Selected runtime implementation version.
    pub runtime_version: String,
    /// Route class used for the first provider attempt.
    pub route_class: MetadataTraceRouteClassV1,
    /// Domain-separated digest of the selected auth profile, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_profile_id_sha256: Option<String>,
    /// Ordered schema identifiers and their exact digests.
    pub schema_hashes: Vec<MetadataTraceSchemaHashV1>,
}

/// Metadata emitted for one observe-only RuntimeKernelV2 shadow comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeShadowDifferentialMetadataV1 {
    /// Bounded reason that the run entered shadow evaluation.
    pub enrollment: MetadataTraceShadowEnrollmentV1,
    /// Aggregate result derived from the fixed differential dimensions.
    pub classification: MetadataTraceShadowClassificationV1,
    /// Stable machine reason code for the classification.
    pub reason_code: String,
    /// Runtime-selection projection comparison.
    pub runtime_selection: MetadataTraceDifferentialOutcomeV1,
    /// Ordered context-segment projection comparison.
    pub context_segments: MetadataTraceDifferentialOutcomeV1,
    /// Context trust and instruction-safety comparison.
    pub context_safety: MetadataTraceDifferentialOutcomeV1,
    /// Input token-budget comparison.
    pub token_budget: MetadataTraceDifferentialOutcomeV1,
    /// Model-visible tool-catalog comparison.
    pub tool_catalog: MetadataTraceDifferentialOutcomeV1,
    /// Policy-input comparison.
    pub policy_input: MetadataTraceDifferentialOutcomeV1,
    /// Canonical expected-phase comparison.
    pub phase_plan: MetadataTraceDifferentialOutcomeV1,
    /// Whether this comparison prevents unattended promotion.
    pub promotion_blocked: bool,
    /// Proof that the compared candidate had no side-effect authority.
    pub shadow_side_effect_free: bool,
}

/// One schema identifier and exact SHA-256 digest used by the runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataTraceSchemaHashV1 {
    /// Stable machine identifier for the schema.
    pub schema_id: String,
    /// SHA-256 digest of the exact schema bytes.
    pub sha256: String,
}

/// Metadata emitted after context assembly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextAssembledMetadataV1 {
    /// Selected context-engine identifier.
    pub context_engine_id: String,
    /// Selected context-engine implementation version.
    pub context_engine_version: String,
    /// Digest of the context contract used for this assembly.
    pub context_schema_sha256: String,
    /// Bounded count of candidate context items.
    pub input_item_count: u32,
    /// Bounded count of retained context items.
    pub retained_item_count: u32,
}

/// Metadata emitted for a provider attempt phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAttemptMetadataV1 {
    /// Domain-separated digest of the provider identifier.
    pub provider_id_sha256: String,
    /// Domain-separated digest of the model identifier.
    pub model_id_sha256: String,
    /// Route class used by this attempt.
    pub route_class: MetadataTraceRouteClassV1,
    /// Domain-separated digest of the auth profile, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_profile_id_sha256: Option<String>,
    /// One-based bounded attempt number.
    pub attempt: u16,
    /// Observable phase outcome for the attempt.
    pub outcome: MetadataTraceProviderAttemptOutcomeV1,
    /// Stable machine reason code for this phase.
    pub reason_code: String,
}

/// Metadata emitted for one tool-gate decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolGateMetadataV1 {
    /// Domain-separated digest of the tool identifier.
    pub tool_id_sha256: String,
    /// Closed gate decision.
    pub decision: MetadataTraceToolGateDecisionV1,
    /// Stable machine reason code for the decision.
    pub reason_code: String,
}

/// Metadata emitted for one approval transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalMetadataV1 {
    /// Domain-separated digest of the approval identifier.
    pub approval_id_sha256: String,
    /// Closed approval state.
    pub decision: MetadataTraceApprovalDecisionV1,
    /// Stable machine reason code for the transition.
    pub reason_code: String,
}

/// Metadata emitted for one tool outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolOutcomeMetadataV1 {
    /// Domain-separated digest of the tool identifier.
    pub tool_id_sha256: String,
    /// One-based bounded attempt number.
    pub attempt: u16,
    /// Closed tool outcome.
    pub outcome: MetadataTraceToolOutcomeV1,
    /// Stable machine reason code for the outcome.
    pub reason_code: String,
}

/// Metadata emitted for one recovery decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryMetadataV1 {
    /// Closed recovery strategy.
    pub strategy: MetadataTraceRecoveryStrategyV1,
    /// One-based bounded recovery attempt number.
    pub attempt: u16,
    /// Stable machine reason code for recovery.
    pub reason_code: String,
}

/// Metadata emitted for one delivery-intent transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryIntentMetadataV1 {
    /// Domain-separated digest of the delivery identifier.
    pub delivery_id_sha256: String,
    /// Closed delivery route.
    pub route: MetadataTraceDeliveryRouteV1,
    /// Closed durable delivery state.
    pub state: MetadataTraceDeliveryStateV1,
    /// Stable machine reason code for the transition.
    pub reason_code: String,
}

/// Metadata emitted exactly once when a run terminalizes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalizationMetadataV1 {
    /// Closed terminal outcome.
    pub outcome: MetadataTraceTerminalOutcomeV1,
    /// Stable machine reason code for terminalization.
    pub reason_code: String,
    /// Whether user-visible output was emitted before terminalization.
    pub output_emitted: bool,
    /// Whether an external side effect may have occurred.
    pub side_effect_may_have_occurred: bool,
}

/// Metadata emitted as the first event of a continuation segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryContinuationMetadataV1 {
    /// Digest of the immediately preceding segment.
    pub previous_segment_id_sha256: String,
    /// Stable machine reason code for continuation.
    pub reason_code: String,
}

/// Metadata emitted when a deterministic trace limit is reached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityReachedMetadataV1 {
    /// Contract limit that stopped further capture.
    pub limit_kind: MetadataTraceCapacityLimitV1,
    /// Bounded observed value at the limit boundary.
    pub observed: u32,
    /// Configured hard limit.
    pub limit: u32,
    /// Stable machine reason code for the cap.
    pub reason_code: String,
}

/// Admission path for a root metadata trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceEntrypointV1 {
    /// A newly admitted run.
    NewRun,
    /// A recovered legacy run receiving its first metadata trace.
    Recovery,
}

/// Closed provider-route classes recorded by the metadata trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceRouteClassV1 {
    /// Primary configured route.
    Primary,
    /// Explicit bounded fallback route.
    Fallback,
    /// Deterministic fixture route.
    Fixture,
    /// Redacted record/replay route.
    RecordReplay,
    /// Explicitly authorized live route.
    Live,
}

/// Closed enrollment reasons for RuntimeKernelV2 shadow comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceShadowEnrollmentV1 {
    /// Deployment-key sampling selected the session.
    DeterministicSample,
    /// A host-owned explicit QA enrollment selected the session.
    ExplicitSession,
}

/// Closed aggregate classifications for RuntimeKernelV2 shadow comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceShadowClassificationV1 {
    /// Every fixed differential dimension matched.
    Expected,
    /// Only bounded operational drift was observed.
    Benign,
    /// Behavioral drift requires operator review.
    Risky,
    /// A safety or authority invariant diverged.
    InvariantViolation,
}

/// Closed outcomes for one RuntimeKernelV2 shadow differential dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceDifferentialOutcomeV1 {
    /// Both projections matched.
    Match,
    /// Drift stayed within the fixed operational tolerance.
    BenignDifference,
    /// Behavioral drift requires review.
    RiskyDifference,
    /// Safety or authority semantics diverged.
    InvariantViolation,
}

/// Closed provider-attempt outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceProviderAttemptOutcomeV1 {
    /// Adapter invocation started.
    Started,
    /// Adapter invocation succeeded.
    Succeeded,
    /// Adapter returned a retryable failure.
    RetryableFailure,
    /// Adapter returned a terminal failure.
    TerminalFailure,
    /// Adapter invocation was cancelled.
    Cancelled,
}

/// Closed tool-gate decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceToolGateDecisionV1 {
    /// Policy allowed execution.
    Allowed,
    /// Policy denied execution.
    Denied,
    /// Execution requires explicit approval.
    ApprovalRequired,
}

/// Closed approval transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceApprovalDecisionV1 {
    /// Approval was requested.
    Requested,
    /// Approval was granted.
    Approved,
    /// Approval was denied.
    Denied,
    /// Approval expired.
    Expired,
    /// Approval was cancelled.
    Cancelled,
}

/// Closed tool outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceToolOutcomeV1 {
    /// Tool execution succeeded.
    Succeeded,
    /// Tool execution failed without uncertain side effects.
    Failed,
    /// Tool outcome or side effects are unknown.
    Unknown,
    /// Tool execution was cancelled.
    Cancelled,
}

/// Closed recovery strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceRecoveryStrategyV1 {
    /// Retry the same route under its bounded policy.
    RetrySameRoute,
    /// Select an explicit provider fallback.
    ProviderFailover,
    /// Compact context before another attempt.
    ContextCompaction,
    /// Stop behind an idempotency guard.
    IdempotencyGuard,
    /// Require operator review before progress.
    OperatorReview,
}

/// Closed delivery routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceDeliveryRouteV1 {
    /// Direct response delivery.
    Direct,
    /// First-party channel delivery.
    Channel,
    /// Connector-backed delivery.
    Connector,
    /// Durable background-queue delivery.
    BackgroundQueue,
}

/// Closed durable delivery states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceDeliveryStateV1 {
    /// Intent was planned but not yet queued.
    Planned,
    /// Intent was durably queued.
    Queued,
    /// Adapter send completed.
    Sent,
    /// External acknowledgement was observed.
    Acknowledged,
    /// Delivery failed without uncertain completion.
    Failed,
    /// Delivery completion remains unknown.
    Unknown,
}

/// Closed terminal outcomes for production runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceTerminalOutcomeV1 {
    /// Run completed normally.
    Done,
    /// Run failed terminally.
    Failed,
    /// Run was cancelled through the normal cancellation path.
    Cancelled,
    /// Startup recovery or invariant handling forced termination.
    ForcedAbort,
}

/// Closed capacity limits that can stop metadata capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceCapacityLimitV1 {
    /// Total event count reached the run cap.
    EventCount,
    /// Segment count reached the run cap.
    SegmentCount,
    /// One event exceeded its canonical byte cap.
    EventBytes,
    /// Schema-hash cardinality reached its cap.
    SchemaHashCount,
    /// A measured stage exceeded the duration cap.
    StageDuration,
}

/// Identity domains used by [`metadata_trace_id_sha256`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTraceIdDomainV1 {
    /// Run identifier.
    Run,
    /// Session identifier.
    Session,
    /// Segment identifier.
    Segment,
    /// Event identifier.
    Event,
    /// Provider identifier.
    Provider,
    /// Model identifier.
    Model,
    /// Tool identifier.
    Tool,
    /// Approval identifier.
    Approval,
    /// Delivery identifier.
    Delivery,
    /// Auth-profile identifier.
    AuthProfile,
    /// MCP server identifier.
    McpServer,
    /// MCP tool identifier.
    McpTool,
    /// Other bounded internal identifier.
    Custom,
}

impl MetadataTraceIdDomainV1 {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Session => "session",
            Self::Segment => "segment",
            Self::Event => "event",
            Self::Provider => "provider",
            Self::Model => "model",
            Self::Tool => "tool",
            Self::Approval => "approval",
            Self::Delivery => "delivery",
            Self::AuthProfile => "auth_profile",
            Self::McpServer => "mcp_server",
            Self::McpTool => "mcp_tool",
            Self::Custom => "custom",
        }
    }
}

/// Validation failure for strict metadata traces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataTraceValidationError {
    code: &'static str,
    path: String,
    message: String,
}

impl MetadataTraceValidationError {
    /// Returns the stable validation reason code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Returns the JSONPath-like location of the invalid field.
    #[must_use]
    pub fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Returns the bounded validation message.
    #[must_use]
    pub fn message(&self) -> &str {
        self.message.as_str()
    }
}

impl fmt::Display for MetadataTraceValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} at {}: {}", self.code, self.path, self.message)
    }
}

impl Error for MetadataTraceValidationError {}

mod validation;
pub use validation::metadata_trace_id_sha256;

#[cfg(test)]
mod tests;
