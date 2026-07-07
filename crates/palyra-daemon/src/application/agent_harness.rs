//! Host-owned agent harness selection and prepared-attempt contracts.
//!
//! A harness may execute a prepared agent attempt, but it does not own provider
//! resolution, credentials, transcript storage, sandbox policy, tool execution,
//! approval resolution, or direct journal writes. Those authorities stay with
//! the daemon and are exposed to harnesses only through callbacks.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use palyra_common::runtime_contracts::{
    AgentHarnessAttemptClassification, AgentHarnessAttemptFinalizerSummary,
    AgentHarnessAttemptReplaySafety, AgentHarnessAttemptResult, AgentHarnessAttemptTerminalStatus,
    AgentHarnessCallbackKind, AgentHarnessSelectionMode, AgentHarnessSupportOutcome,
    PREPARED_AGENT_ATTEMPT_SCHEMA,
};
use serde::Serialize;
use serde_json::Value;

/// Stable id for the embedded Palyra harness.
pub const EMBEDDED_PALYRA_HARNESS_ID: &str = "embedded_palyra";

/// Compatibility name for the built-in Palyra harness implementation.
pub type BuiltinPalyraHarness = EmbeddedPalyraHarness;

/// Stable descriptor for a native agent harness implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHarnessDescriptor {
    /// Stable harness identifier used by explicit selection policy.
    pub id: String,
    /// Operator-visible label.
    pub label: String,
    /// Whether this harness is the embedded runtime path.
    pub embedded_default: bool,
    /// Stable hash over descriptor fields used by diagnostics and replay fixtures.
    pub descriptor_hash: String,
}

impl AgentHarnessDescriptor {
    /// Builds a descriptor and computes its deterministic hash.
    #[must_use]
    pub fn new(id: impl Into<String>, label: impl Into<String>, embedded_default: bool) -> Self {
        let id = id.into();
        let label = label.into();
        let descriptor_hash = descriptor_hash(id.as_str(), label.as_str(), embedded_default);
        Self { id, label, embedded_default, descriptor_hash }
    }

    /// Builds the canonical embedded Palyra descriptor.
    #[must_use]
    pub fn embedded_palyra() -> Self {
        Self::new(EMBEDDED_PALYRA_HARNESS_ID, "Palyra embedded runtime", true)
    }
}

fn descriptor_hash(id: &str, label: &str, embedded_default: bool) -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for byte in
        id.bytes().chain([0]).chain(label.bytes()).chain([0]).chain([u8::from(embedded_default)])
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("fnv1a64:{hash:016x}")
}

/// Host-owned routing inputs used when asking a harness whether it can serve an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentHarnessSupportRequest<'a> {
    /// Selection mode requested by host policy or operator configuration.
    pub selection_mode: AgentHarnessSelectionMode,
    /// Explicit harness id requested by policy, if any.
    pub explicit_harness_id: Option<&'a str>,
    /// Resolved provider id; credentials are not included.
    pub provider_id: &'a str,
    /// Resolved model id.
    pub model_id: &'a str,
    /// Runtime policy profile selected by host configuration.
    pub runtime_policy: &'a str,
    /// Channel family for the attempt.
    pub channel_kind: &'a str,
    /// Sandbox posture required by host policy.
    pub sandbox_mode: &'a str,
    /// Safe tool policy summary. Raw tool arguments are not included.
    pub tool_policy_summary: &'a str,
    /// Model capability labels relevant to harness compatibility.
    pub model_capabilities: &'a [&'a str],
    /// Whether the attempt may perform side effects.
    pub mutating: bool,
    /// Whether the prepared attempt can be safely retried by the same harness.
    pub replay_safe: bool,
    /// Whether policy permits fallback from a preferred plugin/native route.
    pub fallback_allowed: bool,
    /// Whether replayability is mandatory for the selected harness.
    pub replay_required: bool,
}

/// Auditable answer returned by a harness support probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHarnessSupportDecision {
    /// Support outcome used by selection arbitration.
    pub outcome: AgentHarnessSupportOutcome,
    /// Stable machine-readable reason code.
    pub reason_code: String,
}

impl AgentHarnessSupportDecision {
    /// Constructs a supported decision.
    #[must_use]
    pub fn supported(reason_code: impl Into<String>) -> Self {
        Self { outcome: AgentHarnessSupportOutcome::Supported, reason_code: reason_code.into() }
    }

    /// Constructs a preferred decision.
    #[must_use]
    pub fn preferred(reason_code: impl Into<String>) -> Self {
        Self { outcome: AgentHarnessSupportOutcome::Preferred, reason_code: reason_code.into() }
    }

    /// Constructs a declined decision.
    #[must_use]
    pub fn declined(reason_code: impl Into<String>) -> Self {
        Self { outcome: AgentHarnessSupportOutcome::Declined, reason_code: reason_code.into() }
    }

    fn selection_rank(&self) -> u8 {
        match self.outcome {
            AgentHarnessSupportOutcome::Declined => 0,
            AgentHarnessSupportOutcome::Supported => 1,
            AgentHarnessSupportOutcome::Preferred => 2,
        }
    }
}

/// Registry lifecycle failure with a stable reason code.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AgentHarnessRegistryError {
    #[error("agent harness registry has been disposed")]
    Disposed,
    #[error("agent harness already registered: {harness_id}")]
    AlreadyRegistered { harness_id: String },
}

/// Host-owned registry for native and plugin-backed harness descriptors.
#[derive(Default)]
pub struct AgentHarnessRegistry {
    harnesses: BTreeMap<String, Arc<dyn AgentHarness>>,
    disposed: bool,
}

impl AgentHarnessRegistry {
    /// Builds a registry with the embedded Palyra harness installed as default.
    ///
    /// # Errors
    /// Returns [`AgentHarnessRegistryError`] if the embedded harness cannot be registered.
    pub fn with_embedded_default() -> Result<Self, AgentHarnessRegistryError> {
        let mut registry = Self::default();
        registry.register(EmbeddedPalyraHarness::default())?;
        Ok(registry)
    }

    /// Registers a harness implementation.
    ///
    /// # Errors
    /// Returns [`AgentHarnessRegistryError::Disposed`] after disposal or
    /// [`AgentHarnessRegistryError::AlreadyRegistered`] for duplicate ids.
    pub fn register<H>(&mut self, harness: H) -> Result<(), AgentHarnessRegistryError>
    where
        H: AgentHarness + 'static,
    {
        self.register_arc(Arc::new(harness))
    }

    /// Registers an already shared harness implementation.
    ///
    /// # Errors
    /// Returns [`AgentHarnessRegistryError::Disposed`] after disposal or
    /// [`AgentHarnessRegistryError::AlreadyRegistered`] for duplicate ids.
    pub fn register_arc(
        &mut self,
        harness: Arc<dyn AgentHarness>,
    ) -> Result<(), AgentHarnessRegistryError> {
        if self.disposed {
            return Err(AgentHarnessRegistryError::Disposed);
        }
        let harness_id = harness.descriptor().id.clone();
        if self.harnesses.contains_key(harness_id.as_str()) {
            return Err(AgentHarnessRegistryError::AlreadyRegistered { harness_id });
        }
        self.harnesses.insert(harness_id, harness);
        Ok(())
    }

    /// Removes a harness by id and returns its descriptor when present.
    pub fn unregister(&mut self, harness_id: &str) -> Option<AgentHarnessDescriptor> {
        self.harnesses.remove(harness_id).map(|harness| harness.descriptor().clone())
    }

    /// Returns descriptors in deterministic id order.
    #[must_use]
    pub fn list(&self) -> Vec<AgentHarnessDescriptor> {
        self.harnesses.values().map(|harness| harness.descriptor().clone()).collect()
    }

    /// Looks up a registered harness.
    #[must_use]
    pub fn lookup(&self, harness_id: &str) -> Option<&dyn AgentHarness> {
        self.harnesses.get(harness_id).map(Arc::as_ref)
    }

    /// Resets the registry to the embedded default harness.
    ///
    /// # Errors
    /// Returns [`AgentHarnessRegistryError::Disposed`] after disposal.
    pub fn reset(&mut self) -> Result<(), AgentHarnessRegistryError> {
        if self.disposed {
            return Err(AgentHarnessRegistryError::Disposed);
        }
        self.harnesses.clear();
        self.register(EmbeddedPalyraHarness::default())
    }

    /// Disposes the registry and drops all registered harness handles.
    pub fn dispose(&mut self) {
        self.harnesses.clear();
        self.disposed = true;
    }

    /// Selects a harness from the registered set.
    ///
    /// # Errors
    /// Returns [`AgentHarnessSelectionError`] when policy cannot select a safe harness.
    pub fn select<'a>(
        &'a self,
        request: &AgentHarnessSupportRequest<'_>,
    ) -> Result<SelectedAgentHarness<'a>, AgentHarnessSelectionError> {
        let harnesses: Vec<&'a dyn AgentHarness> =
            self.harnesses.values().map(Arc::as_ref).collect();
        select_agent_harness(harnesses.as_slice(), request)
    }
}

/// Callback capabilities handed to a prepared attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedAgentAttemptCallbacks {
    /// Callback kinds accepted for this attempt.
    pub allowed: Vec<AgentHarnessCallbackKind>,
    /// Whether direct journal writes are allowed. This is always false for public attempts.
    pub direct_journal_write_allowed: bool,
}

impl PreparedAgentAttemptCallbacks {
    /// Returns the host-controlled callback set from the public prepared-attempt schema.
    #[must_use]
    pub fn host_controlled() -> Self {
        Self {
            allowed: PREPARED_AGENT_ATTEMPT_SCHEMA.callback_kinds.to_vec(),
            direct_journal_write_allowed: PREPARED_AGENT_ATTEMPT_SCHEMA
                .direct_journal_write_allowed,
        }
    }

    /// Returns `true` when the callback is available to the harness.
    #[must_use]
    pub fn allows(&self, callback: AgentHarnessCallbackKind) -> bool {
        self.allowed.contains(&callback)
    }
}

/// Shared cancellation token for a prepared harness attempt.
#[derive(Debug, Clone, Default)]
pub struct AgentHarnessCancellation {
    cancelled: Arc<AtomicBool>,
}

impl AgentHarnessCancellation {
    /// Requests cancellation of the running attempt.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Returns `true` once cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

/// Sanitized, host-controlled attempt passed to a selected harness.
#[derive(Debug)]
pub struct PreparedAgentAttempt<'a> {
    /// Run id for audit correlation.
    pub run_id: &'a str,
    /// Session id for audit correlation.
    pub session_id: &'a str,
    /// Resolved provider id.
    pub provider_id: &'a str,
    /// Resolved model id.
    pub model_id: &'a str,
    /// Credential state metadata without raw secret material.
    pub auth_state_metadata: &'a Value,
    /// Context token budget the harness must not exceed.
    pub context_token_budget: u64,
    /// Reasoning policy selected by host routing.
    pub reasoning_policy: Option<&'a str>,
    /// Transcript view after host redaction and trust labeling.
    pub sanitized_transcript_view: &'a [Value],
    /// Tool schemas and identifiers visible to the model.
    pub tool_surface: &'a Value,
    /// Host-owned tool policy summary.
    pub tool_policy: &'a Value,
    /// Workspace root visible to the attempt, if any.
    pub workspace_root: Option<&'a Path>,
    /// Sandbox posture selected by the host.
    pub sandbox: &'a str,
    /// Trace context propagated through host callbacks.
    pub trace_context: &'a str,
    /// Callback surface for emitting host-owned events.
    pub callbacks: PreparedAgentAttemptCallbacks,
    /// Cancellation token observed by the harness.
    pub cancellation: AgentHarnessCancellation,
}

/// Final outcome returned by a harness attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHarnessRunOutcome {
    /// Stable status label such as `completed`, `blocked`, or `failed`.
    pub status: String,
    /// Callback kinds used by the harness.
    pub emitted_callbacks: Vec<AgentHarnessCallbackKind>,
    /// Optional safe final message.
    pub final_message: Option<String>,
}

impl AgentHarnessRunOutcome {
    /// Converts the legacy minimal outcome into a structured attempt result.
    #[must_use]
    pub fn to_attempt_result(
        &self,
        replay_safe: bool,
        diagnostic_trace_id: impl Into<String>,
    ) -> AgentHarnessAttemptResult {
        let (terminal_status, classification) =
            terminal_status_and_classification(self.status.as_str());
        let replay_safety = if replay_safe {
            AgentHarnessAttemptReplaySafety::ReplaySafe
        } else if terminal_status == AgentHarnessAttemptTerminalStatus::Completed {
            AgentHarnessAttemptReplaySafety::Unknown
        } else {
            AgentHarnessAttemptReplaySafety::NotReplaySafe
        };
        let mut result = AgentHarnessAttemptResult::minimal(
            terminal_status,
            classification,
            replay_safety,
            diagnostic_trace_id,
        );
        result.finalizer_summary = Some(AgentHarnessAttemptFinalizerSummary {
            final_message_present: self.final_message.is_some(),
            finish_reason: Some(self.status.clone()),
        });
        result
    }
}

fn terminal_status_and_classification(
    status: &str,
) -> (AgentHarnessAttemptTerminalStatus, AgentHarnessAttemptClassification) {
    let normalized = status.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "empty" | "empty_response" => (
            AgentHarnessAttemptTerminalStatus::Completed,
            AgentHarnessAttemptClassification::EmptyResponse,
        ),
        "policy_blocked" => (
            AgentHarnessAttemptTerminalStatus::Blocked,
            AgentHarnessAttemptClassification::PolicyBlocked,
        ),
        "approval_denied" => (
            AgentHarnessAttemptTerminalStatus::Blocked,
            AgentHarnessAttemptClassification::ApprovalDenied,
        ),
        "provider_error" => (
            AgentHarnessAttemptTerminalStatus::Failed,
            AgentHarnessAttemptClassification::ProviderError,
        ),
        "tool_error" => (
            AgentHarnessAttemptTerminalStatus::Failed,
            AgentHarnessAttemptClassification::ToolError,
        ),
        "internal_error" => (
            AgentHarnessAttemptTerminalStatus::Failed,
            AgentHarnessAttemptClassification::InternalError,
        ),
        "deterministic_failure" => (
            AgentHarnessAttemptTerminalStatus::Failed,
            AgentHarnessAttemptClassification::DeterministicFailure,
        ),
        _ => {
            let terminal_status = AgentHarnessAttemptTerminalStatus::parse(normalized.as_str())
                .unwrap_or(AgentHarnessAttemptTerminalStatus::Failed);
            let classification = match terminal_status {
                AgentHarnessAttemptTerminalStatus::Completed
                | AgentHarnessAttemptTerminalStatus::Yielded => {
                    AgentHarnessAttemptClassification::Ok
                }
                AgentHarnessAttemptTerminalStatus::Blocked => {
                    AgentHarnessAttemptClassification::PolicyBlocked
                }
                AgentHarnessAttemptTerminalStatus::Cancelled => {
                    AgentHarnessAttemptClassification::NativeRuntimeError
                }
                AgentHarnessAttemptTerminalStatus::TimedOut => {
                    AgentHarnessAttemptClassification::ProviderError
                }
                AgentHarnessAttemptTerminalStatus::Failed => {
                    AgentHarnessAttemptClassification::InternalError
                }
            };
            (terminal_status, classification)
        }
    }
}

/// Native harness abstraction. Implementations are selected only after host policy prepares the
/// attempt and never receive direct authority over tools, approvals, or the journal.
pub trait AgentHarness: Send + Sync {
    /// Returns the stable harness descriptor.
    fn descriptor(&self) -> &AgentHarnessDescriptor;

    /// Reports whether this harness can execute the prepared route.
    fn supports(&self, request: &AgentHarnessSupportRequest<'_>) -> AgentHarnessSupportDecision;

    /// Executes a sanitized prepared attempt through host-owned callbacks.
    fn run_attempt(&self, attempt: PreparedAgentAttempt<'_>) -> AgentHarnessRunOutcome;
}

/// Successful harness selection result.
pub struct SelectedAgentHarness<'a> {
    /// Selected harness.
    pub harness: &'a dyn AgentHarness,
    /// Support decision that justified the route.
    pub decision: AgentHarnessSupportDecision,
    /// Whether selection used a policy-approved fallback from a preferred route.
    pub fallback_used: bool,
    /// Fallback policy summary applied to this selection.
    pub fallback_policy: String,
    /// Selection mode used for the decision.
    pub selection_mode: AgentHarnessSelectionMode,
}

impl SelectedAgentHarness<'_> {
    /// Builds a redacted diagnostics report for this selection decision.
    #[must_use]
    pub fn diagnostics(&self) -> AgentHarnessSelectionDiagnostics {
        let descriptor = self.harness.descriptor();
        AgentHarnessSelectionDiagnostics {
            harness_id: descriptor.id.clone(),
            descriptor_hash: descriptor.descriptor_hash.clone(),
            selection_mode: self.selection_mode,
            support_outcome: self.decision.outcome,
            reason_code: self.decision.reason_code.clone(),
            fallback_used: self.fallback_used,
            fallback_policy: self.fallback_policy.clone(),
            embedded_default: descriptor.embedded_default,
        }
    }
}

/// Redacted selection decision visible through diagnostics and replay fixtures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentHarnessSelectionDiagnostics {
    pub harness_id: String,
    pub descriptor_hash: String,
    pub selection_mode: AgentHarnessSelectionMode,
    pub support_outcome: AgentHarnessSupportOutcome,
    pub reason_code: String,
    pub fallback_used: bool,
    pub fallback_policy: String,
    pub embedded_default: bool,
}

/// Harness selection failure with a stable reason code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHarnessSelectionError {
    /// Stable machine-readable reason.
    pub code: String,
    /// Safe operator-facing message.
    pub message: String,
}

impl AgentHarnessSelectionError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into() }
    }
}

/// Selects a harness without silently falling back from explicit or mutating routes.
///
/// # Errors
///
/// Returns `explicit_harness_not_found`, `explicit_harness_declined`, or
/// `no_supported_harness` when the selection cannot be satisfied.
pub fn select_agent_harness<'a>(
    harnesses: &[&'a dyn AgentHarness],
    request: &AgentHarnessSupportRequest<'_>,
) -> Result<SelectedAgentHarness<'a>, AgentHarnessSelectionError> {
    if matches!(request.selection_mode, AgentHarnessSelectionMode::Embedded) {
        return select_embedded_harness(harnesses, request);
    }

    let explicit_mode = matches!(
        request.selection_mode,
        AgentHarnessSelectionMode::Explicit | AgentHarnessSelectionMode::ExplicitPlugin
    );
    let preferred_mode = matches!(
        request.selection_mode,
        AgentHarnessSelectionMode::PreferredPlugin
            | AgentHarnessSelectionMode::ModelScoped
            | AgentHarnessSelectionMode::ProviderScoped
    );

    if let Some(requested_id) = request.explicit_harness_id {
        if explicit_mode {
            return select_requested_harness(harnesses, request, requested_id);
        }
        if preferred_mode {
            if let Some(selected) = try_requested_harness(harnesses, request, requested_id)? {
                return Ok(selected);
            }
            if request.mutating || !request.fallback_allowed {
                return Err(AgentHarnessSelectionError::new(
                    "preferred_harness_unavailable_for_mutation",
                    format!("preferred harness '{requested_id}' cannot fall back for this route"),
                ));
            }
            return select_best_harness(harnesses, request, true, "policy_allowed");
        }
    }

    select_best_harness(harnesses, request, false, "not_applicable")
}

fn select_embedded_harness<'a>(
    harnesses: &[&'a dyn AgentHarness],
    request: &AgentHarnessSupportRequest<'_>,
) -> Result<SelectedAgentHarness<'a>, AgentHarnessSelectionError> {
    let Some(harness) =
        harnesses.iter().copied().find(|harness| harness.descriptor().embedded_default)
    else {
        return Err(AgentHarnessSelectionError::new(
            "embedded_harness_not_registered",
            "embedded Palyra harness is not registered",
        ));
    };
    let decision = harness.supports(request);
    if decision.outcome == AgentHarnessSupportOutcome::Declined {
        return Err(AgentHarnessSelectionError::new(
            "embedded_harness_declined",
            "embedded Palyra harness declined the prepared attempt",
        ));
    }
    Ok(SelectedAgentHarness {
        harness,
        decision,
        fallback_used: false,
        fallback_policy: "not_applicable".to_owned(),
        selection_mode: request.selection_mode,
    })
}

fn select_requested_harness<'a>(
    harnesses: &[&'a dyn AgentHarness],
    request: &AgentHarnessSupportRequest<'_>,
    requested_id: &str,
) -> Result<SelectedAgentHarness<'a>, AgentHarnessSelectionError> {
    let Some(selected) = try_requested_harness(harnesses, request, requested_id)? else {
        return Err(AgentHarnessSelectionError::new(
            "explicit_harness_not_found",
            format!("explicit harness '{requested_id}' is not registered"),
        ));
    };
    Ok(selected)
}

fn try_requested_harness<'a>(
    harnesses: &[&'a dyn AgentHarness],
    request: &AgentHarnessSupportRequest<'_>,
    requested_id: &str,
) -> Result<Option<SelectedAgentHarness<'a>>, AgentHarnessSelectionError> {
    let Some(harness) =
        harnesses.iter().copied().find(|harness| harness.descriptor().id == requested_id)
    else {
        return Ok(None);
    };
    let decision = harness.supports(request);
    if decision.outcome == AgentHarnessSupportOutcome::Declined {
        if matches!(
            request.selection_mode,
            AgentHarnessSelectionMode::Explicit | AgentHarnessSelectionMode::ExplicitPlugin
        ) {
            return Err(AgentHarnessSelectionError::new(
                "explicit_harness_declined",
                format!("explicit harness '{requested_id}' declined the prepared attempt"),
            ));
        }
        return Ok(None);
    }
    Ok(Some(SelectedAgentHarness {
        harness,
        decision,
        fallback_used: false,
        fallback_policy: "not_applicable".to_owned(),
        selection_mode: request.selection_mode,
    }))
}

fn select_best_harness<'a>(
    harnesses: &[&'a dyn AgentHarness],
    request: &AgentHarnessSupportRequest<'_>,
    fallback_used: bool,
    fallback_policy: &str,
) -> Result<SelectedAgentHarness<'a>, AgentHarnessSelectionError> {
    let mut candidates: Vec<(&'a dyn AgentHarness, AgentHarnessSupportDecision)> = harnesses
        .iter()
        .copied()
        .filter_map(|harness| {
            let decision = harness.supports(request);
            (decision.outcome != AgentHarnessSupportOutcome::Declined)
                .then_some((harness, decision))
        })
        .collect();

    candidates.sort_by(|(left_harness, left_decision), (right_harness, right_decision)| {
        right_decision
            .selection_rank()
            .cmp(&left_decision.selection_rank())
            .then_with(|| left_harness.descriptor().id.cmp(&right_harness.descriptor().id))
    });

    let Some((harness, decision)) = candidates.into_iter().next() else {
        return Err(AgentHarnessSelectionError::new(
            "no_supported_harness",
            "no registered harness supports the prepared attempt",
        ));
    };

    Ok(SelectedAgentHarness {
        harness,
        decision,
        fallback_used,
        fallback_policy: fallback_policy.to_owned(),
        selection_mode: request.selection_mode,
    })
}

/// Embedded Palyra harness used as the default execution route.
#[derive(Debug, Clone)]
pub struct EmbeddedPalyraHarness {
    descriptor: AgentHarnessDescriptor,
}

impl Default for EmbeddedPalyraHarness {
    fn default() -> Self {
        Self { descriptor: AgentHarnessDescriptor::embedded_palyra() }
    }
}

impl AgentHarness for EmbeddedPalyraHarness {
    fn descriptor(&self) -> &AgentHarnessDescriptor {
        &self.descriptor
    }

    fn supports(&self, request: &AgentHarnessSupportRequest<'_>) -> AgentHarnessSupportDecision {
        if matches!(
            request.selection_mode,
            AgentHarnessSelectionMode::Explicit | AgentHarnessSelectionMode::ExplicitPlugin
        ) && request.explicit_harness_id != Some(self.descriptor.id.as_str())
        {
            return AgentHarnessSupportDecision::declined("harness.explicit_id_mismatch");
        }
        AgentHarnessSupportDecision::supported("harness.embedded_default")
    }

    fn run_attempt(&self, attempt: PreparedAgentAttempt<'_>) -> AgentHarnessRunOutcome {
        AgentHarnessRunOutcome {
            status: if attempt.cancellation.is_cancelled() {
                "cancelled".to_owned()
            } else {
                "completed".to_owned()
            },
            emitted_callbacks: vec![AgentHarnessCallbackKind::FinalOutcome],
            final_message: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Debug)]
    struct DummyHarness {
        descriptor: AgentHarnessDescriptor,
        preferred: bool,
        supports_mutating: bool,
    }

    impl DummyHarness {
        fn new(id: &str, preferred: bool, supports_mutating: bool) -> Self {
            Self {
                descriptor: AgentHarnessDescriptor::new(id, id, false),
                preferred,
                supports_mutating,
            }
        }
    }

    impl AgentHarness for DummyHarness {
        fn descriptor(&self) -> &AgentHarnessDescriptor {
            &self.descriptor
        }

        fn supports(
            &self,
            request: &AgentHarnessSupportRequest<'_>,
        ) -> AgentHarnessSupportDecision {
            if request.mutating && !self.supports_mutating {
                return AgentHarnessSupportDecision::declined("harness.mutating_not_supported");
            }
            if self.preferred {
                AgentHarnessSupportDecision::preferred("harness.preferred")
            } else {
                AgentHarnessSupportDecision::supported("harness.supported")
            }
        }

        fn run_attempt(&self, attempt: PreparedAgentAttempt<'_>) -> AgentHarnessRunOutcome {
            let emitted_callbacks =
                if attempt.callbacks.allows(AgentHarnessCallbackKind::FinalOutcome) {
                    vec![AgentHarnessCallbackKind::FinalOutcome]
                } else {
                    Vec::new()
                };
            AgentHarnessRunOutcome {
                status: "completed".to_owned(),
                emitted_callbacks,
                final_message: Some(format!("{}:{}", attempt.provider_id, attempt.model_id)),
            }
        }
    }

    fn support_request<'a>(
        selection_mode: AgentHarnessSelectionMode,
        explicit_harness_id: Option<&'a str>,
        mutating: bool,
    ) -> AgentHarnessSupportRequest<'a> {
        AgentHarnessSupportRequest {
            selection_mode,
            explicit_harness_id,
            provider_id: "openai",
            model_id: "gpt",
            runtime_policy: "default",
            channel_kind: "operator_cli",
            sandbox_mode: "host_owned",
            tool_policy_summary: "approval_required",
            model_capabilities: &["text"],
            mutating,
            replay_safe: false,
            fallback_allowed: true,
            replay_required: false,
        }
    }

    #[test]
    fn default_registry_exposes_embedded_harness_descriptor() {
        let registry =
            AgentHarnessRegistry::with_embedded_default().expect("embedded registry should build");

        let descriptors = registry.list();

        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].id, EMBEDDED_PALYRA_HARNESS_ID);
        assert!(descriptors[0].embedded_default);
        assert!(descriptors[0].descriptor_hash.starts_with("fnv1a64:"));
        assert!(registry.lookup(EMBEDDED_PALYRA_HARNESS_ID).is_some());
    }

    #[test]
    fn registry_reset_restores_embedded_harness_and_dispose_closes_registration() {
        let mut registry =
            AgentHarnessRegistry::with_embedded_default().expect("embedded registry should build");
        registry.register(DummyHarness::new("zeta.harness", false, true)).unwrap();

        registry.reset().expect("reset should restore embedded default");
        assert_eq!(registry.list().len(), 1);
        assert!(registry.lookup("zeta.harness").is_none());

        registry.dispose();
        let error = registry
            .register(DummyHarness::new("new.harness", false, true))
            .expect_err("disposed registry must not accept new harnesses");
        assert_eq!(error, AgentHarnessRegistryError::Disposed);
    }

    #[test]
    fn auto_selection_prefers_claimed_harness() {
        let embedded = EmbeddedPalyraHarness::default();
        let dummy = DummyHarness::new("acme.harness", true, true);
        let harnesses: [&dyn AgentHarness; 2] = [&embedded, &dummy];

        let selected = select_agent_harness(
            &harnesses,
            &support_request(AgentHarnessSelectionMode::Auto, None, false),
        )
        .expect("auto selection should find a harness");

        assert_eq!(selected.harness.descriptor().id, "acme.harness");
        assert_eq!(selected.decision.outcome, AgentHarnessSupportOutcome::Preferred);
        assert!(!selected.fallback_used);
    }

    #[test]
    fn explicit_decline_does_not_fallback_to_embedded_harness() {
        let embedded = EmbeddedPalyraHarness::default();
        let dummy = DummyHarness::new("acme.harness", true, false);
        let harnesses: [&dyn AgentHarness; 2] = [&embedded, &dummy];

        let error = match select_agent_harness(
            &harnesses,
            &support_request(AgentHarnessSelectionMode::Explicit, Some("acme.harness"), true),
        ) {
            Ok(selected) => panic!(
                "explicit mutating route must not fallback to {}",
                selected.harness.descriptor().id
            ),
            Err(error) => error,
        };

        assert_eq!(error.code, "explicit_harness_declined");
    }

    #[test]
    fn explicit_missing_harness_fails_closed() {
        let registry =
            AgentHarnessRegistry::with_embedded_default().expect("embedded registry should build");

        let error = match registry.select(&support_request(
            AgentHarnessSelectionMode::ExplicitPlugin,
            Some("missing.plugin"),
            false,
        )) {
            Ok(selected) => panic!(
                "explicit missing harness must not select {}",
                selected.harness.descriptor().id
            ),
            Err(error) => error,
        };

        assert_eq!(error.code, "explicit_harness_not_found");
    }

    #[test]
    fn auto_selection_has_deterministic_tie_breaker() {
        let embedded = EmbeddedPalyraHarness::default();
        let alpha = DummyHarness::new("alpha.harness", false, true);
        let beta = DummyHarness::new("beta.harness", false, true);
        let harnesses: [&dyn AgentHarness; 3] = [&beta, &embedded, &alpha];

        let selected = select_agent_harness(
            &harnesses,
            &support_request(AgentHarnessSelectionMode::Auto, None, false),
        )
        .expect("auto selection should find a harness");

        assert_eq!(selected.harness.descriptor().id, "alpha.harness");
        assert_eq!(selected.decision.reason_code, "harness.supported");
    }

    #[test]
    fn preferred_plugin_missing_cannot_fallback_for_mutating_route() {
        let registry =
            AgentHarnessRegistry::with_embedded_default().expect("embedded registry should build");

        let error = match registry.select(&support_request(
            AgentHarnessSelectionMode::PreferredPlugin,
            Some("missing.plugin"),
            true,
        )) {
            Ok(selected) => panic!(
                "mutating preferred route must not fallback to {}",
                selected.harness.descriptor().id
            ),
            Err(error) => error,
        };

        assert_eq!(error.code, "preferred_harness_unavailable_for_mutation");
    }

    #[test]
    fn preferred_plugin_missing_can_fallback_when_policy_allows_read_only_route() {
        let registry =
            AgentHarnessRegistry::with_embedded_default().expect("embedded registry should build");

        let selected = registry
            .select(&support_request(
                AgentHarnessSelectionMode::PreferredPlugin,
                Some("missing.plugin"),
                false,
            ))
            .expect("read-only preferred route may fallback by policy");

        let diagnostics = selected.diagnostics();
        assert_eq!(diagnostics.harness_id, EMBEDDED_PALYRA_HARNESS_ID);
        assert!(diagnostics.fallback_used);
        assert_eq!(diagnostics.fallback_policy, "policy_allowed");
        assert_eq!(diagnostics.reason_code, "harness.embedded_default");
    }

    #[test]
    fn prepared_attempt_exposes_callbacks_without_journal_authority() {
        let harness = DummyHarness::new("acme.harness", false, true);
        let auth = json!({ "credential_state": "present" });
        let transcript = vec![json!({ "role": "user", "content": "<redacted>" })];
        let tools = json!({ "tools": [] });
        let policy = json!({ "approval_required": true });
        let attempt = PreparedAgentAttempt {
            run_id: "run-1",
            session_id: "session-1",
            provider_id: "openai",
            model_id: "gpt",
            auth_state_metadata: &auth,
            context_token_budget: 8_192,
            reasoning_policy: Some("standard"),
            sanitized_transcript_view: transcript.as_slice(),
            tool_surface: &tools,
            tool_policy: &policy,
            workspace_root: None,
            sandbox: "host_owned",
            trace_context: "trace-1",
            callbacks: PreparedAgentAttemptCallbacks::host_controlled(),
            cancellation: AgentHarnessCancellation::default(),
        };

        assert!(!attempt.callbacks.direct_journal_write_allowed);
        let outcome = harness.run_attempt(attempt);

        assert_eq!(outcome.status, "completed");
        assert!(outcome.emitted_callbacks.contains(&AgentHarnessCallbackKind::FinalOutcome));
        assert_eq!(outcome.final_message.as_deref(), Some("openai:gpt"));

        let structured = outcome.to_attempt_result(false, "trace?access_token=secret");
        let serialized = serde_json::to_string(&structured).expect("result should serialize");
        assert_eq!(structured.terminal_status, AgentHarnessAttemptTerminalStatus::Completed);
        assert_eq!(structured.classification, AgentHarnessAttemptClassification::Ok);
        assert!(structured
            .finalizer_summary
            .as_ref()
            .is_some_and(|summary| summary.final_message_present));
        assert!(!serialized.contains("secret"));
    }

    #[test]
    fn cancellation_token_is_observable_by_embedded_harness() {
        let harness = EmbeddedPalyraHarness::default();
        let cancellation = AgentHarnessCancellation::default();
        cancellation.cancel();
        let auth = json!({});
        let transcript = Vec::<Value>::new();
        let tools = json!({});
        let policy = json!({});

        let outcome = harness.run_attempt(PreparedAgentAttempt {
            run_id: "run-1",
            session_id: "session-1",
            provider_id: "openai",
            model_id: "gpt",
            auth_state_metadata: &auth,
            context_token_budget: 1,
            reasoning_policy: None,
            sanitized_transcript_view: transcript.as_slice(),
            tool_surface: &tools,
            tool_policy: &policy,
            workspace_root: None,
            sandbox: "host_owned",
            trace_context: "trace-1",
            callbacks: PreparedAgentAttemptCallbacks::host_controlled(),
            cancellation,
        });

        assert_eq!(outcome.status, "cancelled");
    }
}
