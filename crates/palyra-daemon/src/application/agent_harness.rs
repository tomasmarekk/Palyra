//! Host-owned agent harness selection and prepared-attempt contracts.
//!
//! A harness may execute a prepared agent attempt, but it does not own provider
//! resolution, credentials, transcript storage, sandbox policy, tool execution,
//! approval resolution, or direct journal writes. Those authorities stay with
//! the daemon and are exposed to harnesses only through callbacks.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use palyra_common::runtime_contracts::{
    AgentHarnessCallbackKind, AgentHarnessSelectionMode, AgentHarnessSupportOutcome,
    PREPARED_AGENT_ATTEMPT_SCHEMA,
};
use serde_json::Value;

/// Stable descriptor for a native agent harness implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHarnessDescriptor {
    /// Stable harness identifier used by explicit selection policy.
    pub id: String,
    /// Operator-visible label.
    pub label: String,
    /// Whether this harness is the embedded runtime path.
    pub embedded_default: bool,
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
    /// Whether the attempt may perform side effects.
    pub mutating: bool,
    /// Whether the prepared attempt can be safely retried by the same harness.
    pub replay_safe: bool,
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
    harnesses: &'a [&'a dyn AgentHarness],
    request: &AgentHarnessSupportRequest<'_>,
) -> Result<SelectedAgentHarness<'a>, AgentHarnessSelectionError> {
    if let Some(explicit_id) = request.explicit_harness_id {
        let Some(harness) =
            harnesses.iter().copied().find(|harness| harness.descriptor().id == explicit_id)
        else {
            return Err(AgentHarnessSelectionError::new(
                "explicit_harness_not_found",
                format!("explicit harness '{explicit_id}' is not registered"),
            ));
        };
        let decision = harness.supports(request);
        if decision.outcome == AgentHarnessSupportOutcome::Declined {
            return Err(AgentHarnessSelectionError::new(
                "explicit_harness_declined",
                format!("explicit harness '{explicit_id}' declined the prepared attempt"),
            ));
        }
        return Ok(SelectedAgentHarness { harness, decision });
    }

    harnesses
        .iter()
        .copied()
        .filter_map(|harness| {
            let decision = harness.supports(request);
            (decision.outcome != AgentHarnessSupportOutcome::Declined)
                .then_some((harness, decision))
        })
        .max_by(|(left_harness, left_decision), (right_harness, right_decision)| {
            left_decision
                .selection_rank()
                .cmp(&right_decision.selection_rank())
                .then_with(|| right_harness.descriptor().id.cmp(&left_harness.descriptor().id))
        })
        .map(|(harness, decision)| SelectedAgentHarness { harness, decision })
        .ok_or_else(|| {
            AgentHarnessSelectionError::new(
                "no_supported_harness",
                "no registered harness supports the prepared attempt",
            )
        })
}

/// Embedded Palyra harness used as the default execution route.
#[derive(Debug, Clone)]
pub struct EmbeddedPalyraHarness {
    descriptor: AgentHarnessDescriptor,
}

impl Default for EmbeddedPalyraHarness {
    fn default() -> Self {
        Self {
            descriptor: AgentHarnessDescriptor {
                id: "palyra.embedded".to_owned(),
                label: "Palyra embedded runtime".to_owned(),
                embedded_default: true,
            },
        }
    }
}

impl AgentHarness for EmbeddedPalyraHarness {
    fn descriptor(&self) -> &AgentHarnessDescriptor {
        &self.descriptor
    }

    fn supports(&self, request: &AgentHarnessSupportRequest<'_>) -> AgentHarnessSupportDecision {
        if matches!(request.selection_mode, AgentHarnessSelectionMode::Explicit)
            && request.explicit_harness_id != Some(self.descriptor.id.as_str())
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
                descriptor: AgentHarnessDescriptor {
                    id: id.to_owned(),
                    label: id.to_owned(),
                    embedded_default: false,
                },
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
            mutating,
            replay_safe: false,
        }
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
