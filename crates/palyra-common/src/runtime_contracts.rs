//! Shared runtime vocabulary for queueing, flow orchestration, delivery policy,
//! auxiliary tasks, and worker lifecycle reporting.
//!
//! Design note:
//! - These enums define the canonical wire names that runtime preview stabilizes before
//!   queue, retrieval, flow, and worker business logic is expanded.
//! - Backward-compatible aliases keep persisted records and existing UI payloads
//!   readable while new surfaces emit only the canonical forms.
//! - Intentionally deferred variants stay out of this module until the
//!   corresponding behavior is implemented and covered by rollout/config
//!   guardrails, diagnostics, and regression harnesses.
//! - Wire names, aliases, and serialized shapes are pinned by the runtime-contract
//!   snapshot gate (`scripts/test/check-runtime-contract-snapshots.sh`): add aliases
//!   instead of renaming canonical strings.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

macro_rules! runtime_contract_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $variant:ident => $canonical:literal $(| $alias:literal )*
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(
                #[serde(rename = $canonical $(, alias = $alias)*)]
                $variant,
            )+
        }

        impl $name {
            /// Returns the canonical wire name for this variant.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $canonical,
                    )+
                }
            }

            /// Parses a canonical wire name or backward-compatible alias.
            ///
            /// Matching is case-insensitive and ignores surrounding whitespace.
            #[must_use]
            pub fn parse(value: &str) -> Option<Self> {
                let normalized = value.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    $(
                        $canonical $(| $alias )* => Some(Self::$variant),
                    )+
                    _ => None,
                }
            }

            /// Option-returning alias for [`Self::parse`], kept alongside the
            /// `FromStr` impl for call-site ergonomics.
            #[allow(clippy::should_implement_trait)]
            #[must_use]
            pub fn from_str(value: &str) -> Option<Self> {
                Self::parse(value)
            }
        }

        impl std::str::FromStr for $name {
            type Err = ();

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value).ok_or(())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

runtime_contract_enum! {
    /// Canonical run lifecycle states shared by daemon, CLI/API, replay, and future realtime/ACP
    /// adapters. These are the public states; individual transports may still keep legacy labels
    /// internally and map them through this type at the boundary.
    pub enum RunLifecyclePhase {
        Queued => "queued" | "pending" | "accepted",
        Running => "running" | "in_progress" | "streaming",
        WaitingForApproval => "waiting_for_approval" | "approval_wait" | "awaiting_approval" | "waiting",
        Paused => "paused",
        Completed => "completed" | "done" | "succeeded",
        Failed => "failed",
        Aborted => "aborted" | "cancelled" | "canceled",
        Expired => "expired" | "timed_out" | "timeout"
    }
}

impl RunLifecyclePhase {
    /// Returns `true` for states a run can never transition out of.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Aborted | Self::Expired)
    }

    /// Returns `true` while the run is parked waiting on an approval or operator action.
    #[must_use]
    pub const fn is_waiting(self) -> bool {
        matches!(self, Self::WaitingForApproval | Self::Paused)
    }
}

runtime_contract_enum! {
    /// Sandboxed hook phases where a plugin can observe a run and return a typed decision.
    pub enum RunLifecycleHookPhase {
        BeforeRun => "before_run" | "run:before_run",
        BeforeTool => "before_tool" | "run:before_tool",
        AfterTool => "after_tool" | "run:after_tool",
        BeforeDelivery => "before_delivery" | "run:before_delivery",
        AfterRun => "after_run" | "run:after_run"
    }
}

impl RunLifecycleHookPhase {
    /// Returns the namespaced `run:*` event name used on event buses.
    #[must_use]
    pub fn event_name(self) -> &'static str {
        match self {
            Self::BeforeRun => "run:before_run",
            Self::BeforeTool => "run:before_tool",
            Self::AfterTool => "run:after_tool",
            Self::BeforeDelivery => "run:before_delivery",
            Self::AfterRun => "run:after_run",
        }
    }

    /// Parses either the short phase name or the namespaced `run:*` event name.
    #[must_use]
    pub fn parse_hook_event(raw: &str) -> Option<Self> {
        Self::parse(raw.trim().to_ascii_lowercase().as_str())
    }
}

runtime_contract_enum! {
    /// Decision returned by a run lifecycle hook after sandboxed execution.
    pub enum RunLifecycleHookDecisionKind {
        Continue => "continue",
        Annotate => "annotate",
        RequestApproval => "request_approval",
        Block => "block",
        TransformPreview => "transform_preview",
        FailRun => "fail_run"
    }
}

impl RunLifecycleHookDecisionKind {
    /// Returns the arbitration weight for this decision kind; higher values win.
    ///
    /// Gaps between values are deliberate so new kinds can be inserted without
    /// renumbering the pinned ordering.
    #[must_use]
    pub const fn priority(self) -> u8 {
        match self {
            Self::Continue => 10,
            Self::Annotate => 20,
            Self::TransformPreview => 30,
            Self::RequestApproval => 40,
            Self::Block => 50,
            Self::FailRun => 60,
        }
    }

    /// Returns `true` for decisions that stop further processing of the phase.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::RequestApproval | Self::Block | Self::FailRun)
    }

    /// Reports whether this decision kind may be returned from the given hook phase.
    #[must_use]
    pub const fn is_allowed_in_phase(self, phase: RunLifecycleHookPhase) -> bool {
        match self {
            Self::Continue | Self::Annotate => true,
            Self::RequestApproval | Self::Block => matches!(
                phase,
                RunLifecycleHookPhase::BeforeRun
                    | RunLifecycleHookPhase::BeforeTool
                    | RunLifecycleHookPhase::BeforeDelivery
            ),
            Self::TransformPreview => matches!(phase, RunLifecycleHookPhase::BeforeDelivery),
            Self::FailRun => matches!(
                phase,
                RunLifecycleHookPhase::BeforeRun
                    | RunLifecycleHookPhase::BeforeTool
                    | RunLifecycleHookPhase::AfterTool
                    | RunLifecycleHookPhase::BeforeDelivery
            ),
        }
    }
}

runtime_contract_enum! {
    /// Stable actor kind names for runtime audit records.
    pub enum RuntimeActorKind {
        System => "system",
        Principal => "principal" | "user",
        Agent => "agent",
        Connector => "connector",
        Scheduler => "scheduler",
        Worker => "worker",
        Policy => "policy",
        Replay => "replay"
    }
}

runtime_contract_enum! {
    /// Stable operation state for global idempotency records.
    pub enum IdempotencyOperationState {
        Started => "started" | "in_progress",
        Completed => "completed" | "succeeded",
        Failed => "failed",
        Expired => "expired"
    }
}

impl IdempotencyOperationState {
    /// Returns `true` for states an idempotency record can never transition out of.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Expired)
    }
}

runtime_contract_enum! {
    /// Result of checking a global idempotency key before a side effect is executed.
    pub enum IdempotencyReplayDecision {
        Reserved => "reserved",
        SamePayloadRetry => "same_payload_retry",
        CompletedReplayResult => "completed_replay_result",
        ConflictingPayload => "conflicting_payload",
        ExpiredRetry => "expired_retry"
    }
}

runtime_contract_enum! {
    /// How a tool result may be exposed after policy, sensitivity, and budget checks.
    pub enum ToolResultVisibility {
        ModelInline => "model_inline",
        ModelSummary => "model_summary",
        AuditArtifact => "audit_artifact",
        RedactedPreview => "redacted_preview"
    }
}

runtime_contract_enum! {
    /// Sensitivity taxonomy for durable tool result artifacts.
    pub enum ToolResultSensitivity {
        Public => "public",
        InternalPath => "internal_path",
        StdoutStderr => "stdout_stderr",
        PersonalData => "personal_data",
        Secret => "secret",
        ProviderRawPayload => "provider_raw_payload",
        ApprovalRiskData => "approval_risk_data"
    }
}

impl ToolResultSensitivity {
    /// Returns `true` when reading the full artifact payload requires the audit read gate.
    #[must_use]
    pub const fn requires_full_read_gate(self) -> bool {
        matches!(
            self,
            Self::InternalPath
                | Self::StdoutStderr
                | Self::PersonalData
                | Self::Secret
                | Self::ProviderRawPayload
                | Self::ApprovalRiskData
        )
    }
}

runtime_contract_enum! {
    /// Retention class for durable tool result artifacts.
    pub enum ArtifactRetentionDisposition {
        Keep => "keep",
        ExpireAfter => "expire_after",
        PurgeOnRequest => "purge_on_request",
        AuditLegalHold => "audit_legal_hold"
    }
}

/// Stable error envelope used by runtime contracts instead of leaking internal debug strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StableErrorEnvelope {
    pub code: String,
    pub message: String,
    pub recovery_hint: String,
}

impl StableErrorEnvelope {
    /// Creates an envelope from a stable code, operator-facing message, and recovery hint.
    #[must_use]
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        recovery_hint: impl Into<String>,
    ) -> Self {
        Self { code: code.into(), message: message.into(), recovery_hint: recovery_hint.into() }
    }
}

/// Audit-visible identity of the actor that caused a runtime transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeActorRef {
    pub kind: RuntimeActorKind,
    pub id: String,
}

/// Canonical audit record for run lifecycle transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLifecycleTransitionRecord {
    pub schema_version: u32,
    pub event_id: String,
    pub run_id: String,
    pub session_id: String,
    pub from_state: Option<RunLifecyclePhase>,
    pub to_state: RunLifecyclePhase,
    pub actor: RuntimeActorRef,
    pub correlation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub reason: String,
    pub occurred_at_unix_ms: i64,
}

/// Typed decision emitted by one sandboxed run lifecycle hook invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunLifecycleHookDecision {
    pub schema_version: u32,
    pub phase: RunLifecycleHookPhase,
    pub kind: RunLifecycleHookDecisionKind,
    pub hook_id: String,
    pub plugin_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub annotations: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transformed_preview: Option<Value>,
}

impl RunLifecycleHookDecision {
    /// Creates a schema-version-1 decision with empty annotations and no preview transform.
    #[must_use]
    pub fn new(
        phase: RunLifecycleHookPhase,
        kind: RunLifecycleHookDecisionKind,
        hook_id: impl Into<String>,
        plugin_id: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: 1,
            phase,
            kind,
            hook_id: hook_id.into(),
            plugin_id: plugin_id.into(),
            reason: None,
            annotations: Value::Object(Default::default()),
            transformed_preview: None,
        }
    }
}

/// Deterministic aggregate of all decisions returned for a lifecycle phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunLifecycleHookResolution {
    pub schema_version: u32,
    pub phase: RunLifecycleHookPhase,
    pub selected: RunLifecycleHookDecision,
    pub candidates: Vec<RunLifecycleHookDecision>,
    #[serde(default)]
    pub conflicts: Vec<String>,
    pub terminal: bool,
}

/// Validation failure for lifecycle hook decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLifecycleHookDecisionError {
    pub code: String,
    pub message: String,
}

impl RunLifecycleHookDecisionError {
    /// Creates a validation error from a stable code and human-readable message.
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into() }
    }
}

/// Resolves sandboxed lifecycle hook decisions with deterministic terminal precedence.
///
/// # Errors
///
/// Returns a [`RunLifecycleHookDecisionError`] with code `phase_mismatch` when a decision
/// was produced for a different phase, or `decision_not_allowed_in_phase` when its kind is
/// not permitted in `phase`.
pub fn resolve_run_lifecycle_hook_decisions(
    phase: RunLifecycleHookPhase,
    mut decisions: Vec<RunLifecycleHookDecision>,
) -> Result<RunLifecycleHookResolution, RunLifecycleHookDecisionError> {
    for decision in &decisions {
        if decision.phase != phase {
            return Err(RunLifecycleHookDecisionError::new(
                "phase_mismatch",
                format!(
                    "hook decision for {} cannot be resolved in {}",
                    decision.phase.as_str(),
                    phase.as_str()
                ),
            ));
        }
        if !decision.kind.is_allowed_in_phase(phase) {
            return Err(RunLifecycleHookDecisionError::new(
                "decision_not_allowed_in_phase",
                format!("{} is not allowed during {}", decision.kind.as_str(), phase.as_str()),
            ));
        }
    }

    // No hook responded: synthesize a Continue so resolution always selects something.
    if decisions.is_empty() {
        decisions.push(RunLifecycleHookDecision::new(
            phase,
            RunLifecycleHookDecisionKind::Continue,
            "runtime",
            "runtime",
        ));
    }

    // Highest priority wins; hook/plugin ids break ties so the outcome is deterministic
    // regardless of hook invocation order.
    decisions.sort_by(|left, right| {
        right
            .kind
            .priority()
            .cmp(&left.kind.priority())
            .then_with(|| left.hook_id.cmp(&right.hook_id))
            .then_with(|| left.plugin_id.cmp(&right.plugin_id))
    });
    let selected = decisions[0].clone();
    // Record losers as conflicts only when either side carried semantic weight (terminal,
    // transform, or annotate); Continue losing to Continue is not a conflict.
    let conflicts = decisions
        .iter()
        .skip(1)
        .filter(|decision| {
            selected.kind.is_terminal()
                || decision.kind.is_terminal()
                || matches!(
                    selected.kind,
                    RunLifecycleHookDecisionKind::TransformPreview
                        | RunLifecycleHookDecisionKind::Annotate
                )
                || matches!(
                    decision.kind,
                    RunLifecycleHookDecisionKind::TransformPreview
                        | RunLifecycleHookDecisionKind::Annotate
                )
        })
        .map(|decision| {
            format!("{}:{}:{}", decision.hook_id, decision.plugin_id, decision.kind.as_str())
        })
        .collect::<Vec<_>>();

    Ok(RunLifecycleHookResolution {
        schema_version: 1,
        phase,
        terminal: selected.kind.is_terminal(),
        selected,
        candidates: decisions,
        conflicts,
    })
}

/// Public snapshot of a global idempotency record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRecordSnapshot {
    pub key: String,
    pub scope: String,
    pub operation_kind: String,
    pub payload_sha256: String,
    pub state: IdempotencyOperationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StableErrorEnvelope>,
    pub first_seen_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
}

/// Result returned by the runtime before a side-effecting operation is executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyCheckOutcome {
    pub decision: IdempotencyReplayDecision,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<IdempotencyRecordSnapshot>,
}

/// Retention policy attached to a tool result artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRetentionPolicy {
    pub disposition: ArtifactRetentionDisposition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
    pub legal_hold: bool,
}

impl ArtifactRetentionPolicy {
    /// Creates a policy that retains the artifact indefinitely without legal hold.
    #[must_use]
    pub const fn keep() -> Self {
        Self {
            disposition: ArtifactRetentionDisposition::Keep,
            expires_at_unix_ms: None,
            legal_hold: false,
        }
    }

    /// Creates a policy that places the artifact under audit legal hold.
    #[must_use]
    pub const fn audit_legal_hold() -> Self {
        Self {
            disposition: ArtifactRetentionDisposition::AuditLegalHold,
            expires_at_unix_ms: None,
            legal_hold: true,
        }
    }
}

/// Durable reference to a full audit-visible tool result payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResultArtifactRef {
    pub artifact_id: String,
    pub digest_sha256: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub sensitivity: ToolResultSensitivity,
    pub retention: ArtifactRetentionPolicy,
    pub origin_tool_call_id: String,
    pub tool_name: String,
    pub run_id: String,
    pub session_id: String,
    pub storage_backend: String,
    pub redacted_preview: String,
    pub created_at_unix_ms: i64,
}

/// Request contract for `palyra.artifact.read`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactReadRequest {
    pub artifact_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_digest_sha256: Option<String>,
    #[serde(default)]
    pub offset_bytes: u64,
    #[serde(default)]
    pub max_bytes: u64,
    #[serde(default = "default_artifact_text_preview")]
    pub text_preview: bool,
}

fn default_artifact_text_preview() -> bool {
    true
}

/// Response contract for `palyra.artifact.read`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactReadResponse {
    pub artifact: ToolResultArtifactRef,
    pub offset_bytes: u64,
    pub returned_bytes: u64,
    pub eof: bool,
    pub visibility: ToolResultVisibility,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// Per-turn budget settings used before putting tool output back into the model context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolTurnBudget {
    pub max_model_inline_bytes: usize,
    pub max_model_summary_bytes: usize,
    pub max_artifact_preview_bytes: usize,
    pub max_artifact_read_bytes: usize,
}

impl Default for ToolTurnBudget {
    fn default() -> Self {
        Self {
            max_model_inline_bytes: 8 * 1024,
            max_model_summary_bytes: 2 * 1024,
            max_artifact_preview_bytes: 1_024,
            max_artifact_read_bytes: 16 * 1024,
        }
    }
}

/// Observability counters for model-visible budget projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ToolResultBudgetMetrics {
    pub spilled_artifacts: u64,
    pub rejected_payloads: u64,
    pub saved_model_visible_bytes: u64,
}

/// Lowest realtime protocol version the daemon accepts.
pub const REALTIME_PROTOCOL_MIN_VERSION: u32 = 1;
/// Highest realtime protocol version the daemon accepts.
pub const REALTIME_PROTOCOL_MAX_VERSION: u32 = 1;
/// Default heartbeat interval offered to realtime clients, in milliseconds.
pub const REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 15_000;

runtime_contract_enum! {
    /// Role declared by a realtime client during the initial handshake.
    pub enum RealtimeRole {
        Operator => "operator",
        ReadOnly => "read_only",
        Agent => "agent",
        Connector => "connector",
        Node => "node"
    }
}

runtime_contract_enum! {
    /// Scope names used by realtime method and event authorization.
    pub enum RealtimeScope {
        RunsRead => "runs:read",
        RunsWrite => "runs:write",
        ApprovalsRead => "approvals:read",
        ApprovalsWrite => "approvals:write",
        NodesRead => "nodes:read",
        NodesWrite => "nodes:write",
        ConfigRead => "config:read",
        ConfigWrite => "config:write",
        EventsRead => "events:read",
        EventsSensitive => "events:sensitive"
    }
}

runtime_contract_enum! {
    /// Capability names returned by realtime negotiation and checked by handlers.
    pub enum RealtimeCapability {
        EventStream => "event_stream",
        SnapshotRefresh => "snapshot_refresh",
        RunControl => "run_control",
        ApprovalControl => "approval_control",
        NodePresence => "node_presence",
        CapabilityGrant => "capability_grant",
        ConfigSchemaLookup => "config_schema_lookup",
        ConfigReload => "config_reload",
        SensitiveEvents => "sensitive_events"
    }
}

runtime_contract_enum! {
    /// Stable command names for command-router backed realtime methods.
    pub enum RealtimeCommand {
        RunCreate => "run.create",
        RunWait => "run.wait",
        RunEvents => "run.events",
        RunAbort => "run.abort",
        RunGet => "run.get",
        ApprovalList => "approval.list",
        ApprovalGet => "approval.get",
        ApprovalDecide => "approval.decide",
        NodePresence => "node.presence",
        NodeCapabilityGrant => "node.capability.grant",
        NodeCapabilityRevoke => "node.capability.revoke",
        ConfigSchemaLookup => "config.schema.lookup",
        ConfigReloadPlan => "config.reload.plan",
        ConfigReloadApply => "config.reload.apply"
    }
}

runtime_contract_enum! {
    /// Event topics routed through the realtime event router.
    pub enum RealtimeEventTopic {
        Run => "run",
        Approval => "approval",
        Node => "node",
        Config => "config",
        System => "system"
    }
}

runtime_contract_enum! {
    /// Event sensitivity is evaluated before serialization to each client.
    pub enum RealtimeEventSensitivity {
        Public => "public",
        Internal => "internal",
        Sensitive => "sensitive",
        Secret => "secret"
    }
}

/// Supported realtime protocol range advertised in compatibility errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeProtocolVersionRange {
    pub min: u32,
    pub max: u32,
}

impl Default for RealtimeProtocolVersionRange {
    fn default() -> Self {
        Self { min: REALTIME_PROTOCOL_MIN_VERSION, max: REALTIME_PROTOCOL_MAX_VERSION }
    }
}

impl RealtimeProtocolVersionRange {
    /// Returns `true` when `protocol_version` falls inside the inclusive range.
    #[must_use]
    pub const fn contains(self, protocol_version: u32) -> bool {
        protocol_version >= self.min && protocol_version <= self.max
    }
}

/// Cursor supplied by realtime clients when reconnecting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RealtimeCursor {
    pub sequence: u64,
}

/// Handshake request sent as the first realtime WebSocket frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeHandshakeRequest {
    pub protocol_version: u32,
    pub client_id: String,
    pub role: RealtimeRole,
    #[serde(default)]
    pub requested_scopes: Vec<RealtimeScope>,
    #[serde(default)]
    pub requested_capabilities: Vec<RealtimeCapability>,
    #[serde(default)]
    pub requested_commands: Vec<RealtimeCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_cursor: Option<RealtimeCursor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<RealtimeSubscription>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_interval_ms: Option<u64>,
}

/// Handshake response after role/scope/capability/command negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeHandshakeAccepted {
    pub protocol_version: u32,
    pub client_id: String,
    pub auth_subject: String,
    pub role: RealtimeRole,
    pub scopes: Vec<RealtimeScope>,
    pub capabilities: Vec<RealtimeCapability>,
    pub commands: Vec<RealtimeCommand>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<RealtimeSubscription>,
    pub cursor: RealtimeCursor,
    pub heartbeat_interval_ms: u64,
    pub server_time_unix_ms: i64,
    pub sdk_abi_version: String,
}

/// Stable realtime error envelope with optional compatibility metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeErrorEnvelope {
    pub error: StableErrorEnvelope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_protocol_versions: Option<RealtimeProtocolVersionRange>,
}

/// Event envelope stored and filtered before serialization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealtimeEventEnvelope {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_id: String,
    pub topic: RealtimeEventTopic,
    pub sensitivity: RealtimeEventSensitivity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_session_id: Option<String>,
    pub occurred_at_unix_ms: i64,
    pub payload: Value,
}

/// Subscription filter carried in connection state and restored on reconnect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeSubscription {
    /// Topics to receive; empty means all topics.
    #[serde(default)]
    pub topics: Vec<RealtimeEventTopic>,
    /// Session ids to receive; empty means all sessions.
    #[serde(default)]
    pub session_ids: Vec<String>,
}

impl RealtimeSubscription {
    /// Creates an unfiltered subscription (empty filters match everything).
    #[must_use]
    pub fn all_topics() -> Self {
        Self { topics: Vec::new(), session_ids: Vec::new() }
    }
}

/// Method/command descriptor exported by the runtime registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeMethodDescriptor {
    pub command: RealtimeCommand,
    pub version: u32,
    pub required_scopes: Vec<RealtimeScope>,
    pub required_capabilities: Vec<RealtimeCapability>,
    pub idempotency_required: bool,
    pub side_effecting: bool,
    pub rate_limit_bucket: String,
}

/// Command frame accepted by the shared backend command router.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealtimeCommandEnvelope {
    pub request_id: String,
    pub command: RealtimeCommand,
    #[serde(default)]
    pub params: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,
}

/// Stable command-router result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealtimeCommandResultEnvelope {
    pub request_id: String,
    pub command: RealtimeCommand,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StableErrorEnvelope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// `true` when the result was served from an idempotency record instead of re-executing.
    pub replayed: bool,
}

/// Lowest ACP protocol version the daemon bridge accepts.
pub const ACP_PROTOCOL_MIN_VERSION: u32 = 1;
/// Highest ACP protocol version the daemon bridge accepts.
pub const ACP_PROTOCOL_MAX_VERSION: u32 = 1;
/// Default cap on transcript events returned by a single ACP replay.
pub const ACP_DEFAULT_REPLAY_MAX_EVENTS: usize = 200;
/// Grace period in milliseconds before a disconnected ACP client's pending state expires.
pub const ACP_DEFAULT_DISCONNECT_GRACE_MS: i64 = 10 * 60 * 1_000;

runtime_contract_enum! {
    /// Transport that carried an ACP request into the daemon-backed bridge.
    pub enum AcpTransportKind {
        Stdio => "stdio",
        Http => "http",
        Websocket => "websocket"
    }
}

runtime_contract_enum! {
    /// Authorization scopes negotiated by daemon-level ACP clients.
    pub enum AcpScope {
        SessionsRead => "sessions:read",
        SessionsWrite => "sessions:write",
        RunsRead => "runs:read",
        RunsWrite => "runs:write",
        ApprovalsRead => "approvals:read",
        ApprovalsWrite => "approvals:write",
        BindingsRead => "bindings:read",
        BindingsWrite => "bindings:write",
        EventsRead => "events:read",
        EventsSensitive => "events:sensitive"
    }
}

runtime_contract_enum! {
    /// Feature capabilities advertised and enforced by daemon-level ACP.
    pub enum AcpCapability {
        RuntimeStatus => "runtime_status",
        SessionList => "session_list",
        SessionLoad => "session_load",
        SessionNew => "session_new",
        SessionReplay => "session_replay",
        RunControl => "run_control",
        ApprovalBridge => "approval_bridge",
        PendingPrompts => "pending_prompts",
        SessionConfig => "session_config",
        SessionFork => "session_fork",
        SessionCompact => "session_compact",
        SessionExplain => "session_explain",
        ConversationBindings => "conversation_bindings",
        BindingRepair => "binding_repair",
        SensitiveReplay => "sensitive_replay"
    }
}

runtime_contract_enum! {
    /// Stable command names for the ACP control plane.
    pub enum AcpCommand {
        Status => "status" | "acp.status",
        SessionList => "session.list",
        SessionLoad => "session.load",
        SessionNew => "session.new",
        SessionReplay => "session.replay",
        SessionResume => "session.resume",
        SessionFork => "session.fork",
        SessionCompactPreview => "session.compact.preview",
        SessionCompactApply => "session.compact.apply",
        SessionExplain => "session.explain",
        SessionModeSet => "session.mode.set",
        SessionConfigSet => "session.config.set",
        RunCreate => "run.create",
        RunAbort => "run.abort",
        ApprovalList => "approval.list",
        ApprovalRequest => "approval.request",
        ApprovalDecide => "approval.decide",
        BindingList => "binding.list",
        BindingUpsert => "binding.upsert",
        BindingGet => "binding.get",
        BindingDetach => "binding.detach",
        BindingRepairPlan => "binding.repair.plan",
        BindingRepairApply => "binding.repair.apply",
        BindingExplain => "binding.explain",
        Reconnect => "reconnect"
    }
}

runtime_contract_enum! {
    /// ACP-facing session execution mode. Mode changes are policy-visible and auditable.
    pub enum AcpSessionMode {
        Normal => "normal",
        Planning => "planning",
        Review => "review",
        ReadOnly => "read_only"
    }
}

runtime_contract_enum! {
    /// ACP permission bridge outcome after mapping through Palyra approvals.
    pub enum AcpPermissionDecision {
        Allow => "allow",
        Deny => "deny",
        Timeout => "timeout",
        Error => "error"
    }
}

runtime_contract_enum! {
    /// Sensitivity label for connector-independent conversation bindings.
    pub enum ConversationBindingSensitivity {
        Public => "public",
        Internal => "internal",
        Sensitive => "sensitive"
    }
}

runtime_contract_enum! {
    /// Conflict state for reverse indexes between external conversations and Palyra sessions.
    pub enum ConversationBindingConflictState {
        None => "none",
        DuplicateActiveBinding => "duplicate_active_binding",
        DuplicateExternalIdentity => "duplicate_external_identity",
        DuplicateSession => "duplicate_session",
        StaleThread => "stale_thread",
        PrincipalMismatch => "principal_mismatch",
        WorkspaceMismatch => "workspace_mismatch",
        ExpiredReference => "expired_reference",
        ParentMissing => "parent_missing",
        Detached => "detached"
    }
}

runtime_contract_enum! {
    /// Diagnostic conflict kind emitted by ACP binding explain and repair previews.
    pub enum AcpBindingConflictKind {
        DuplicateActiveBinding => "duplicate_active_binding",
        StaleThread => "stale_thread",
        PrincipalMismatch => "principal_mismatch",
        WorkspaceMismatch => "workspace_mismatch",
        ExpiredReferenced => "expired_referenced" | "expired_reference",
        ParentMissing => "parent_missing"
    }
}

runtime_contract_enum! {
    /// Safe repair action vocabulary for ACP binding previews and audited apply flows.
    pub enum AcpBindingRepairActionKind {
        Detach => "detach",
        Rebind => "rebind",
        Expire => "expire",
        Split => "split",
        MarkStale => "mark_stale"
    }
}

/// Supported ACP protocol range advertised in compatibility responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpProtocolVersionRange {
    pub min: u32,
    pub max: u32,
}

impl Default for AcpProtocolVersionRange {
    fn default() -> Self {
        Self { min: ACP_PROTOCOL_MIN_VERSION, max: ACP_PROTOCOL_MAX_VERSION }
    }
}

impl AcpProtocolVersionRange {
    /// Returns `true` when `protocol_version` falls inside the inclusive range.
    #[must_use]
    pub const fn contains(self, protocol_version: u32) -> bool {
        protocol_version >= self.min && protocol_version <= self.max
    }
}

/// Cursor supplied by ACP clients when reconnecting or replaying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AcpCursor {
    pub sequence: u64,
}

/// Authenticated daemon-side context for an ACP client request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpClientContext {
    pub protocol_version: u32,
    pub client_id: String,
    pub transport: AcpTransportKind,
    pub owner_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default)]
    pub scopes: Vec<AcpScope>,
    #[serde(default)]
    pub capabilities: Vec<AcpCapability>,
}

/// Durable mapping between an ACP client session and a Palyra orchestrator session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpSessionBindingRecord {
    pub schema_version: u32,
    pub binding_id: String,
    pub acp_client_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    pub session_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_label: Option<String>,
    pub owner_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default)]
    pub scopes: Vec<AcpScope>,
    #[serde(default)]
    pub capabilities: Vec<AcpCapability>,
    pub mode: AcpSessionMode,
    #[serde(default)]
    pub config: Value,
    pub cursor: AcpCursor,
    pub last_seen_at_unix_ms: i64,
    pub protocol_version: u32,
    pub stale_permissions: bool,
}

/// Pending prompt or approval retained during ACP disconnect grace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpPendingPromptRecord {
    pub prompt_id: String,
    pub acp_client_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub prompt_kind: String,
    pub redacted_summary: String,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
}

/// Canonical connector-independent binding from an external conversation to a Palyra session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationBindingRecord {
    pub schema_version: u32,
    pub binding_id: String,
    pub connector_kind: String,
    pub external_identity: String,
    pub external_conversation_id: String,
    pub palyra_session_id: String,
    pub owner_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    pub sensitivity: ConversationBindingSensitivity,
    pub delivery_cursor: AcpCursor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<String>,
    pub conflict_state: ConversationBindingConflictState,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// ACP command frame accepted by the daemon bridge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpCommandEnvelope {
    pub request_id: String,
    pub command: AcpCommand,
    #[serde(default)]
    pub params: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,
}

/// Stable ACP result envelope that mirrors command-router error semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpCommandResultEnvelope {
    pub request_id: String,
    pub command: AcpCommand,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StableErrorEnvelope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// `true` when the result was served from an idempotency record instead of re-executing.
    pub replayed: bool,
}

/// Replay budget applied before ACP transcript events leave the daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpReplayCap {
    pub max_events: usize,
    pub max_payload_bytes: usize,
    pub include_sensitive: bool,
}

impl Default for AcpReplayCap {
    fn default() -> Self {
        Self {
            max_events: ACP_DEFAULT_REPLAY_MAX_EVENTS,
            max_payload_bytes: 64 * 1024,
            include_sensitive: false,
        }
    }
}

/// ACP compatibility error envelope with protocol-range metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpErrorEnvelope {
    pub error: StableErrorEnvelope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_protocol_versions: Option<AcpProtocolVersionRange>,
}

/// Schema lookup record for runtime config control-plane clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfigSchemaField {
    pub path: String,
    pub value_type: String,
    pub default_value: String,
    pub validator: String,
    pub sensitivity: ToolResultSensitivity,
    pub reloadable: bool,
    pub reload_impact: String,
}

/// Node/device presence surfaced over realtime without exposing secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeNodePresence {
    pub device_id: String,
    pub state: String,
    pub ttl_ms: u64,
    pub last_seen_at_unix_ms: i64,
    pub heartbeat_interval_ms: u64,
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub attestation: Vec<String>,
}

runtime_contract_enum! {
    /// Canonical queue runtime modes used by queue orchestration surfaces.
    pub enum QueueMode {
        Followup => "followup" | "follow_up",
        Collect => "collect",
        Steer => "steer",
        SteerBacklog => "steer_backlog" | "steer-backlog",
        Interrupt => "interrupt"
    }
}

runtime_contract_enum! {
    /// Canonical queue decisions used by queue explainability and event payloads.
    pub enum QueueDecision {
        Enqueue => "enqueue",
        Merge => "merge" | "coalesce",
        Steer => "steer",
        SteerBacklog => "steer_backlog" | "steer-backlog",
        Interrupt => "interrupt",
        Overflow => "overflow",
        Defer => "defer" | "deferred"
    }
}

runtime_contract_enum! {
    /// High-level pruning policy classes that keep future rollout knobs stable.
    pub enum PruningPolicyClass {
        Disabled => "disabled" | "off",
        Conservative => "conservative" | "safe",
        Balanced => "balanced" | "default",
        Aggressive => "aggressive" | "high_reduction"
    }
}

runtime_contract_enum! {
    /// Background and auxiliary task kinds shared across daemon, CLI, and web console.
    pub enum AuxiliaryTaskKind {
        BackgroundPrompt => "background_prompt",
        DelegationPrompt => "delegation_prompt",
        Summary => "summary" | "auxiliary_summary",
        RecallSearch => "recall_search" | "auxiliary_recall",
        Classification => "classification" | "auxiliary_classification",
        Extraction => "extraction" | "auxiliary_extraction",
        Vision => "vision" | "auxiliary_vision",
        AttachmentDerivation => "attachment_derivation",
        AttachmentRecompute => "attachment_recompute",
        PostRunReflection => "post_run_reflection" | "reflection"
    }
}

runtime_contract_enum! {
    /// Canonical auxiliary task lifecycle states.
    pub enum AuxiliaryTaskState {
        Queued => "queued" | "pending",
        Running => "running" | "in_progress",
        Paused => "paused",
        Succeeded => "succeeded" | "complete" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled",
        Expired => "expired"
    }
}

impl AuxiliaryTaskState {
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::Paused | Self::CancelRequested)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled | Self::Expired)
    }
}

runtime_contract_enum! {
    /// Queue lifecycle states currently persisted for queued inputs.
    pub enum QueuedInputState {
        Pending => "pending" | "queued",
        Forwarded => "forwarded" | "delivered",
        DeliveryFailed => "delivery_failed" | "failed_delivery",
        Merged => "merged",
        Steered => "steered",
        Interrupted => "interrupted",
        Overflowed => "overflowed" | "overflow",
        Rejected => "rejected" | "reject",
        Cancelled => "cancelled" | "canceled"
    }
}

impl QueuedInputState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

runtime_contract_enum! {
    /// Canonical flow states for future durable orchestration surfaces.
    pub enum FlowState {
        Pending => "pending",
        Ready => "ready",
        Running => "running" | "in_progress",
        WaitingForApproval => "waiting_for_approval" | "approval_wait" | "waiting",
        Paused => "paused",
        Blocked => "blocked",
        Retrying => "retrying",
        Compensating => "compensating",
        TimedOut => "timed_out" | "timeout",
        Succeeded => "succeeded" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled"
    }
}

impl FlowState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled)
    }
}

runtime_contract_enum! {
    /// Canonical flow step states for step adapter and retry surfaces.
    pub enum FlowStepState {
        Pending => "pending",
        Ready => "ready",
        Running => "running" | "in_progress",
        WaitingForApproval => "waiting_for_approval" | "approval_wait" | "waiting",
        Paused => "paused",
        Blocked => "blocked",
        Retrying => "retrying",
        Skipped => "skipped",
        Compensating => "compensating",
        Compensated => "compensated",
        TimedOut => "timed_out" | "timeout",
        Succeeded => "succeeded" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled"
    }
}

impl FlowStepState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Skipped
                | Self::Compensated
                | Self::TimedOut
                | Self::Succeeded
                | Self::Failed
                | Self::Cancelled
        )
    }
}

runtime_contract_enum! {
    /// Delivery arbitration policies reserved for descendant-aware completion.
    pub enum DeliveryPolicy {
        PreferTerminalDescendant => "prefer_terminal_descendant" | "prefer_child_terminal",
        SuppressStaleParent => "suppress_stale_parent",
        MergeProgressUpdates => "merge_progress_updates" | "coalesce_progress",
        DeliverInterimParent => "deliver_interim_parent",
        RequireFinalReview => "require_final_review"
    }
}

runtime_contract_enum! {
    /// Shared worker lifecycle states surfaced by preview diagnostics and audit events.
    pub enum WorkerLifecycleState {
        Registered => "registered",
        Available => "available" | "ready",
        Assigned => "assigned" | "leased",
        Busy => "busy" | "running",
        Degraded => "degraded",
        Draining => "draining",
        Offline => "offline",
        Completed => "completed" | "succeeded",
        Failed => "failed",
        Orphaned => "orphaned"
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcpBindingConflictKind, AcpBindingRepairActionKind, AcpCapability, AcpClientContext,
        AcpCommand, AcpCommandEnvelope, AcpProtocolVersionRange, AcpScope, AcpSessionBindingRecord,
        AcpSessionMode, AcpTransportKind, ArtifactReadRequest, ArtifactRetentionDisposition,
        ArtifactRetentionPolicy, AuxiliaryTaskKind, AuxiliaryTaskState, DeliveryPolicy, FlowState,
        FlowStepState, IdempotencyReplayDecision, PruningPolicyClass, QueueDecision, QueueMode,
        QueuedInputState, RealtimeCapability, RealtimeCommand, RealtimeCommandEnvelope,
        RealtimeEventSensitivity, RealtimeEventTopic, RealtimeHandshakeRequest,
        RealtimeProtocolVersionRange, RealtimeRole, RealtimeScope, RealtimeSubscription,
        RunLifecycleHookDecision, RunLifecycleHookDecisionKind, RunLifecycleHookPhase,
        RunLifecyclePhase, ToolResultSensitivity, ToolResultVisibility, ToolTurnBudget,
        WorkerLifecycleState, ACP_PROTOCOL_MAX_VERSION, ACP_PROTOCOL_MIN_VERSION,
        REALTIME_PROTOCOL_MAX_VERSION, REALTIME_PROTOCOL_MIN_VERSION,
    };
    use serde_json::json;

    #[test]
    fn queue_modes_round_trip_with_canonical_serialization() {
        let serialized =
            serde_json::to_string(&QueueMode::SteerBacklog).expect("queue mode should serialize");
        assert_eq!(serialized, "\"steer_backlog\"");
        let parsed: QueueMode =
            serde_json::from_str("\"steer_backlog\"").expect("queue mode should deserialize");
        assert_eq!(parsed, QueueMode::SteerBacklog);
        assert_eq!(parsed.as_str(), "steer_backlog");
    }

    #[test]
    fn runtime_contract_aliases_stay_backward_compatible() {
        assert_eq!(QueueMode::parse("follow_up"), Some(QueueMode::Followup));
        assert_eq!(QueueDecision::parse("coalesce"), Some(QueueDecision::Merge));
        assert_eq!(QueuedInputState::parse("delivered"), Some(QueuedInputState::Forwarded));
        assert_eq!(AuxiliaryTaskState::parse("canceled"), Some(AuxiliaryTaskState::Cancelled));
        assert_eq!(WorkerLifecycleState::parse("leased"), Some(WorkerLifecycleState::Assigned));
    }

    #[test]
    fn task_and_flow_state_helpers_identify_terminal_states() {
        assert!(AuxiliaryTaskState::Succeeded.is_terminal());
        assert!(AuxiliaryTaskState::Queued.is_active());
        assert!(QueuedInputState::DeliveryFailed.is_terminal());
        assert!(FlowState::TimedOut.is_terminal());
        assert!(FlowStepState::Compensated.is_terminal());
    }

    #[test]
    fn extended_runtime_contracts_expose_expected_canonical_names() {
        assert_eq!(PruningPolicyClass::Balanced.as_str(), "balanced");
        assert_eq!(AuxiliaryTaskKind::Summary.as_str(), "summary");
        assert_eq!(AuxiliaryTaskKind::RecallSearch.as_str(), "recall_search");
        assert_eq!(AuxiliaryTaskKind::Classification.as_str(), "classification");
        assert_eq!(AuxiliaryTaskKind::Extraction.as_str(), "extraction");
        assert_eq!(AuxiliaryTaskKind::Vision.as_str(), "vision");
        assert_eq!(AuxiliaryTaskKind::PostRunReflection.as_str(), "post_run_reflection");
        assert_eq!(DeliveryPolicy::PreferTerminalDescendant.as_str(), "prefer_terminal_descendant");
    }

    #[test]
    fn phase_one_runtime_contracts_parse_legacy_aliases_to_canonical_names() {
        assert_eq!(RunLifecyclePhase::parse("accepted"), Some(RunLifecyclePhase::Queued));
        assert_eq!(RunLifecyclePhase::parse("in_progress"), Some(RunLifecyclePhase::Running));
        assert_eq!(RunLifecyclePhase::parse("done"), Some(RunLifecyclePhase::Completed));
        assert_eq!(RunLifecyclePhase::parse("cancelled"), Some(RunLifecyclePhase::Aborted));
        assert!(RunLifecyclePhase::Completed.is_terminal());
        assert!(RunLifecyclePhase::WaitingForApproval.is_waiting());
        assert_eq!(ToolResultVisibility::AuditArtifact.as_str(), "audit_artifact");
        assert_eq!(IdempotencyReplayDecision::ConflictingPayload.as_str(), "conflicting_payload");
    }

    #[test]
    fn artifact_read_defaults_to_text_preview() {
        let request: ArtifactReadRequest = serde_json::from_value(json!({
            "artifact_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        }))
        .expect("artifact read request should deserialize");

        assert!(request.text_preview);
    }

    #[test]
    fn lifecycle_hook_events_and_decisions_use_stable_rules() {
        assert_eq!(
            RunLifecycleHookPhase::parse_hook_event("run:before_tool"),
            Some(RunLifecycleHookPhase::BeforeTool)
        );
        assert_eq!(RunLifecycleHookPhase::BeforeDelivery.event_name(), "run:before_delivery");
        assert!(RunLifecycleHookDecisionKind::TransformPreview
            .is_allowed_in_phase(RunLifecycleHookPhase::BeforeDelivery));
        assert!(!RunLifecycleHookDecisionKind::TransformPreview
            .is_allowed_in_phase(RunLifecycleHookPhase::AfterRun));
        assert!(!RunLifecycleHookDecisionKind::FailRun
            .is_allowed_in_phase(RunLifecycleHookPhase::AfterRun));
    }

    #[test]
    fn lifecycle_hook_resolution_prefers_terminal_decisions() {
        let phase = RunLifecycleHookPhase::BeforeTool;
        let decisions = vec![
            RunLifecycleHookDecision::new(
                phase,
                RunLifecycleHookDecisionKind::Annotate,
                "audit-note",
                "annotator",
            ),
            RunLifecycleHookDecision::new(
                phase,
                RunLifecycleHookDecisionKind::RequestApproval,
                "approval-gate",
                "approver",
            ),
            RunLifecycleHookDecision::new(
                phase,
                RunLifecycleHookDecisionKind::Block,
                "policy-gate",
                "policy",
            ),
        ];
        let resolution = super::resolve_run_lifecycle_hook_decisions(phase, decisions)
            .expect("valid decisions should resolve");
        assert_eq!(resolution.selected.kind, RunLifecycleHookDecisionKind::Block);
        assert!(resolution.terminal);
        assert_eq!(resolution.candidates.len(), 3);
        assert_eq!(resolution.conflicts.len(), 2);
    }

    #[test]
    fn lifecycle_hook_resolution_rejects_invalid_terminal_phase() {
        let phase = RunLifecycleHookPhase::AfterRun;
        let decisions = vec![RunLifecycleHookDecision::new(
            phase,
            RunLifecycleHookDecisionKind::FailRun,
            "late-fail",
            "plugin",
        )];
        let error = super::resolve_run_lifecycle_hook_decisions(phase, decisions)
            .expect_err("after_run must not accept terminal decisions");
        assert_eq!(error.code, "decision_not_allowed_in_phase");
    }

    #[test]
    fn artifact_contracts_capture_sensitivity_retention_and_budget_defaults() {
        assert!(ToolResultSensitivity::Secret.requires_full_read_gate());
        assert!(ToolResultSensitivity::StdoutStderr.requires_full_read_gate());
        assert!(ToolResultSensitivity::InternalPath.requires_full_read_gate());

        let keep = ArtifactRetentionPolicy::keep();
        assert_eq!(keep.disposition, ArtifactRetentionDisposition::Keep);
        assert!(!keep.legal_hold);

        let hold = ArtifactRetentionPolicy::audit_legal_hold();
        assert_eq!(hold.disposition, ArtifactRetentionDisposition::AuditLegalHold);
        assert!(hold.legal_hold);

        let budget = ToolTurnBudget::default();
        assert!(budget.max_model_inline_bytes > budget.max_model_summary_bytes);
        assert!(budget.max_artifact_read_bytes >= budget.max_model_inline_bytes);
    }

    #[test]
    fn realtime_contracts_use_stable_wire_names() {
        assert_eq!(RealtimeRole::Operator.as_str(), "operator");
        assert_eq!(RealtimeScope::RunsRead.as_str(), "runs:read");
        assert_eq!(RealtimeCapability::SnapshotRefresh.as_str(), "snapshot_refresh");
        assert_eq!(RealtimeCommand::ConfigReloadApply.as_str(), "config.reload.apply");
        assert_eq!(RealtimeEventTopic::Approval.as_str(), "approval");
        assert_eq!(RealtimeEventSensitivity::Sensitive.as_str(), "sensitive");
        assert!(RealtimeProtocolVersionRange::default().contains(REALTIME_PROTOCOL_MIN_VERSION));
        assert!(RealtimeProtocolVersionRange::default().contains(REALTIME_PROTOCOL_MAX_VERSION));
    }

    #[test]
    fn realtime_handshake_and_command_frames_are_json_safe() {
        let handshake = RealtimeHandshakeRequest {
            protocol_version: 1,
            client_id: "console-a".to_owned(),
            role: RealtimeRole::Operator,
            requested_scopes: vec![RealtimeScope::RunsRead, RealtimeScope::ApprovalsWrite],
            requested_capabilities: vec![RealtimeCapability::RunControl],
            requested_commands: vec![RealtimeCommand::RunGet, RealtimeCommand::ApprovalDecide],
            event_cursor: None,
            subscriptions: vec![RealtimeSubscription {
                topics: vec![RealtimeEventTopic::Run],
                session_ids: vec!["session-a".to_owned()],
            }],
            heartbeat_interval_ms: Some(5_000),
        };
        let serialized = serde_json::to_value(&handshake).expect("handshake should serialize");
        assert_eq!(serialized["requested_scopes"], json!(["runs:read", "approvals:write"]));
        assert_eq!(serialized["subscriptions"][0]["topics"], json!(["run"]));

        let command = RealtimeCommandEnvelope {
            request_id: "req-1".to_owned(),
            command: RealtimeCommand::RunWait,
            params: json!({ "run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV" }),
            idempotency_key: None,
            expected_version: None,
        };
        let decoded: RealtimeCommandEnvelope =
            serde_json::from_value(serde_json::to_value(command).unwrap()).unwrap();
        assert_eq!(decoded.command, RealtimeCommand::RunWait);
    }

    #[test]
    fn realtime_handshake_rejects_unknown_scope_and_capability_names() {
        let unknown_scope = json!({
            "protocol_version": 1,
            "client_id": "console-a",
            "role": "operator",
            "requested_scopes": ["runs:read", "unknown:scope"],
            "requested_capabilities": [],
            "requested_commands": []
        });
        assert!(serde_json::from_value::<RealtimeHandshakeRequest>(unknown_scope).is_err());

        let unknown_capability = json!({
            "protocol_version": 1,
            "client_id": "console-a",
            "role": "operator",
            "requested_scopes": [],
            "requested_capabilities": ["unknown_capability"],
            "requested_commands": []
        });
        assert!(serde_json::from_value::<RealtimeHandshakeRequest>(unknown_capability).is_err());
    }

    #[test]
    fn acp_contracts_use_stable_wire_names() {
        assert_eq!(AcpTransportKind::Stdio.as_str(), "stdio");
        assert_eq!(AcpScope::SessionsRead.as_str(), "sessions:read");
        assert_eq!(AcpCapability::ApprovalBridge.as_str(), "approval_bridge");
        assert_eq!(AcpCapability::RuntimeStatus.as_str(), "runtime_status");
        assert_eq!(AcpCommand::Status.as_str(), "status");
        assert_eq!(AcpCommand::BindingRepairApply.as_str(), "binding.repair.apply");
        assert_eq!(AcpSessionMode::ReadOnly.as_str(), "read_only");
        assert_eq!(
            AcpBindingConflictKind::DuplicateActiveBinding.as_str(),
            "duplicate_active_binding"
        );
        assert_eq!(AcpBindingRepairActionKind::MarkStale.as_str(), "mark_stale");
        assert!(AcpProtocolVersionRange::default().contains(ACP_PROTOCOL_MIN_VERSION));
        assert!(AcpProtocolVersionRange::default().contains(ACP_PROTOCOL_MAX_VERSION));
    }

    #[test]
    fn acp_context_and_command_frames_reject_unknown_contract_values() {
        let context = AcpClientContext {
            protocol_version: 1,
            client_id: "zed-extension".to_owned(),
            transport: AcpTransportKind::Stdio,
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes: vec![AcpScope::SessionsRead, AcpScope::ApprovalsWrite],
            capabilities: vec![AcpCapability::SessionReplay],
        };
        let serialized = serde_json::to_value(&context).expect("context should serialize");
        assert_eq!(serialized["scopes"], json!(["sessions:read", "approvals:write"]));
        assert_eq!(serialized["capabilities"], json!(["session_replay"]));

        let command = AcpCommandEnvelope {
            request_id: "req-1".to_owned(),
            command: AcpCommand::SessionReplay,
            params: json!({ "session_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV" }),
            idempotency_key: None,
            expected_version: None,
        };
        let decoded: AcpCommandEnvelope =
            serde_json::from_value(serde_json::to_value(command).unwrap()).unwrap();
        assert_eq!(decoded.command, AcpCommand::SessionReplay);

        let unknown_scope = json!({
            "protocol_version": 1,
            "client_id": "zed-extension",
            "transport": "stdio",
            "owner_principal": "operator",
            "device_id": "desktop",
            "scopes": ["sessions:read", "unknown:scope"],
            "capabilities": []
        });
        assert!(serde_json::from_value::<AcpClientContext>(unknown_scope).is_err());
    }

    #[test]
    fn acp_binding_record_persists_cursor_mode_and_stale_permissions() {
        let binding = AcpSessionBindingRecord {
            schema_version: 1,
            binding_id: "acpbind_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            acp_client_id: "zed-extension".to_owned(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_key: "cwd:C:/repo".to_owned(),
            session_label: Some("Repo".to_owned()),
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes: vec![AcpScope::SessionsRead],
            capabilities: vec![AcpCapability::SessionLoad],
            mode: AcpSessionMode::Review,
            config: json!({ "model_profile": "default" }),
            cursor: Default::default(),
            last_seen_at_unix_ms: 1,
            protocol_version: 1,
            stale_permissions: true,
        };
        let serialized = serde_json::to_value(&binding).expect("binding should serialize");
        assert_eq!(serialized["mode"], "review");
        assert_eq!(serialized["stale_permissions"], true);
        assert_eq!(serialized["cursor"]["sequence"], 0);
    }
}
