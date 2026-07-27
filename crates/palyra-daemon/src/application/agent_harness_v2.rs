//! Authoritative asynchronous agent-harness execution contract.
//!
//! Embedded, native, process-backed, and WASM adapters share one accepted,
//! ordered-event, exactly-one-terminal protocol while the host keeps authority.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    agent_harness::{
        AgentHarness, AgentHarnessCancellation, AgentHarnessCapabilities, AgentHarnessDescriptor,
        EmbeddedPalyraHarness, PreparedAgentAttempt, PreparedAgentAttemptCallbacks,
        AGENT_HARNESS_CONTRACT_VERSION_V2,
    },
    agent_harness_host::{
        HarnessCancellationContext, HarnessCapabilityHandle, HarnessHost, HarnessHostError,
    },
};

/// Maximum non-terminal events accepted for one attempt.
pub const MAX_AGENT_HARNESS_EVENTS: usize = 4_096;
/// Maximum text bytes accepted in one event.
pub const MAX_AGENT_HARNESS_TEXT_BYTES: usize = 1024 * 1024;
/// Maximum encoded bytes accepted for structured event payloads.
pub const MAX_AGENT_HARNESS_EVENT_BYTES: usize = 256 * 1024;

/// Sanitized, owned request safe to send to an external harness.
#[derive(Debug, Clone)]
pub struct AgentHarnessAttemptRequestV2 {
    pub run_id: String,
    pub session_id: String,
    pub generation: u64,
    pub deadline_unix_ms: i64,
    pub provider_id: String,
    pub model_id: String,
    pub context_token_budget: u64,
    pub reasoning_policy: Option<String>,
    pub sanitized_transcript: Vec<Value>,
    pub tool_surface: Value,
    pub tool_catalog_epoch: u64,
    pub workspace_root: Option<String>,
    pub sandbox: String,
    pub trace_context: String,
    pub host_capability: HarnessCapabilityHandle,
    pub cancellation: HarnessCancellationContext,
}

impl AgentHarnessAttemptRequestV2 {
    /// Validates bounded identifiers and generation-pinned request metadata.
    ///
    /// # Errors
    /// Returns [`AgentHarnessV2Error::InvalidAttempt`] for malformed or oversized input.
    pub fn validate(&self) -> Result<(), AgentHarnessV2Error> {
        let transcript_bytes = serde_json::to_vec(&self.sanitized_transcript)
            .map_err(|_| AgentHarnessV2Error::InvalidAttempt)?
            .len();
        let tool_surface_bytes = serde_json::to_vec(&self.tool_surface)
            .map_err(|_| AgentHarnessV2Error::InvalidAttempt)?
            .len();
        if self.run_id.trim().is_empty()
            || self.run_id.len() > 128
            || self.session_id.trim().is_empty()
            || self.session_id.len() > 128
            || self.generation == 0
            || self.deadline_unix_ms <= current_unix_ms()
            || self.provider_id.trim().is_empty()
            || self.provider_id.len() > 128
            || self.model_id.trim().is_empty()
            || self.model_id.len() > 256
            || self.context_token_budget == 0
            || self.tool_catalog_epoch == 0
            || self.sandbox.trim().is_empty()
            || self.sandbox.len() > 128
            || self.trace_context.len() > 512
            || transcript_bytes > 4 * 1024 * 1024
            || tool_surface_bytes > 1024 * 1024
        {
            return Err(AgentHarnessV2Error::InvalidAttempt);
        }
        Ok(())
    }
}

fn current_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// First event emitted by a harness attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessAcceptedV2 {
    pub generation: u64,
    pub sequence: u64,
}

/// Non-terminal harness observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentHarnessEventKindV2 {
    TextDelta { text: String },
    Progress { completed_units: u64, total_units: u64, label: String },
    ReasoningMetadata { summary: String },
    Usage { prompt_tokens: u64, completion_tokens: u64 },
    ToolProposed { call_id: String, tool_name: String, input_json: Value },
    ToolOutcome { call_id: String, outcome: String },
    ApprovalRequired { call_id: String, approval_id: String },
    ApprovalResolved { approval_id: String, outcome: String },
    CompactionRequested,
    CompactionCompleted,
    SideQuestionRequested { question_id: String },
    Heartbeat { ordinal: u64 },
    Checkpoint { checkpoint_id: String },
    ArtifactCreated { artifact_id: String },
    CancellationObserved,
}

/// One monotonically ordered generation-pinned harness event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessEventV2 {
    pub generation: u64,
    pub sequence: u64,
    pub event: AgentHarnessEventKindV2,
}

impl AgentHarnessEventV2 {
    fn validate(&self) -> Result<(), AgentHarnessV2Error> {
        let valid = match &self.event {
            AgentHarnessEventKindV2::TextDelta { text } => {
                !text.is_empty() && text.len() <= MAX_AGENT_HARNESS_TEXT_BYTES
            }
            AgentHarnessEventKindV2::Progress { completed_units, total_units, label } => {
                *total_units > 0
                    && completed_units <= total_units
                    && !label.trim().is_empty()
                    && label.len() <= 256
            }
            AgentHarnessEventKindV2::ReasoningMetadata { summary } => summary.len() <= 8 * 1024,
            AgentHarnessEventKindV2::Usage { prompt_tokens, completion_tokens } => {
                prompt_tokens.checked_add(*completion_tokens).is_some()
            }
            AgentHarnessEventKindV2::ToolProposed { call_id, tool_name, input_json } => {
                valid_id(call_id)
                    && valid_id(tool_name)
                    && serde_json::to_vec(input_json)
                        .is_ok_and(|encoded| encoded.len() <= MAX_AGENT_HARNESS_EVENT_BYTES)
            }
            AgentHarnessEventKindV2::ToolOutcome { call_id, outcome } => {
                valid_id(call_id) && valid_id(outcome)
            }
            AgentHarnessEventKindV2::ApprovalRequired { call_id, approval_id } => {
                valid_id(call_id) && valid_id(approval_id)
            }
            AgentHarnessEventKindV2::ApprovalResolved { approval_id, outcome } => {
                valid_id(approval_id) && valid_id(outcome)
            }
            AgentHarnessEventKindV2::SideQuestionRequested { question_id } => valid_id(question_id),
            AgentHarnessEventKindV2::Heartbeat { ordinal } => *ordinal > 0,
            AgentHarnessEventKindV2::Checkpoint { checkpoint_id } => valid_id(checkpoint_id),
            AgentHarnessEventKindV2::ArtifactCreated { artifact_id } => valid_id(artifact_id),
            AgentHarnessEventKindV2::CompactionRequested
            | AgentHarnessEventKindV2::CompactionCompleted
            | AgentHarnessEventKindV2::CancellationObserved => true,
        };
        if self.sequence == 0 || !valid {
            return Err(AgentHarnessV2Error::InvalidEvent);
        }
        Ok(())
    }
}

/// Terminal outcome emitted exactly once by a harness attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AgentHarnessTerminalOutcomeV2 {
    Completed { final_message: Option<String> },
    Blocked { reason_code: String },
    Failed { reason_code: String, safe_message: String },
    Cancelled { reason_code: String },
    TimedOut { reason_code: String },
}

/// Generation-aware terminal callback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessTerminalV2 {
    pub generation: u64,
    pub sequence: u64,
    pub outcome: AgentHarnessTerminalOutcomeV2,
}

impl AgentHarnessTerminalV2 {
    fn validate(&self) -> Result<(), AgentHarnessV2Error> {
        let valid = match &self.outcome {
            AgentHarnessTerminalOutcomeV2::Completed { final_message } => {
                final_message.as_ref().is_none_or(|message| message.len() <= 1024 * 1024)
            }
            AgentHarnessTerminalOutcomeV2::Blocked { reason_code }
            | AgentHarnessTerminalOutcomeV2::Cancelled { reason_code }
            | AgentHarnessTerminalOutcomeV2::TimedOut { reason_code } => valid_reason(reason_code),
            AgentHarnessTerminalOutcomeV2::Failed { reason_code, safe_message } => {
                valid_reason(reason_code) && safe_message.len() <= 8 * 1024
            }
        };
        if self.sequence == 0 || !valid {
            return Err(AgentHarnessV2Error::InvalidTerminal);
        }
        Ok(())
    }
}

/// Receipt proving that the terminal callback was accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHarnessTerminalReceiptV2 {
    pub generation: u64,
    pub terminal_sequence: u64,
    pub event_count: usize,
}

/// Generation-pinned response to same-turn steering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHarnessSteerOutcomeV2 {
    Accepted { generation: u64 },
    Rejected { reason_code: String },
}

/// Async sink shared by embedded and external harness adapters.
#[async_trait]
pub trait AgentHarnessEventSinkV2: Send {
    async fn accepted(
        &mut self,
        accepted: AgentHarnessAcceptedV2,
    ) -> Result<(), AgentHarnessV2Error>;
    async fn event(&mut self, event: AgentHarnessEventV2) -> Result<(), AgentHarnessV2Error>;
    async fn terminal(
        &mut self,
        terminal: AgentHarnessTerminalV2,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error>;
}

/// Host-facing async harness contract implemented by every production adapter.
#[async_trait]
pub trait AgentHarnessV2: Send + Sync {
    fn descriptor(&self) -> &AgentHarnessDescriptor;

    async fn run_attempt(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        host: &dyn HarnessHost,
        sink: &mut dyn AgentHarnessEventSinkV2,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error>;

    async fn compact(
        &self,
        _request: &AgentHarnessAttemptRequestV2,
        _host: &dyn HarnessHost,
    ) -> Result<(), AgentHarnessV2Error> {
        Err(AgentHarnessV2Error::UnsupportedCapability { capability: "compaction" })
    }

    async fn side_question(
        &self,
        _request: &AgentHarnessAttemptRequestV2,
        _host: &dyn HarnessHost,
        _question: &str,
    ) -> Result<Value, AgentHarnessV2Error> {
        Err(AgentHarnessV2Error::UnsupportedCapability { capability: "side_question" })
    }

    async fn steer(
        &self,
        _generation: u64,
        _input: &str,
    ) -> Result<AgentHarnessSteerOutcomeV2, AgentHarnessV2Error> {
        Err(AgentHarnessV2Error::UnsupportedCapability { capability: "steering" })
    }

    async fn reset(&self, _generation: u64) -> Result<(), AgentHarnessV2Error> {
        Ok(())
    }

    async fn dispose(&self) -> Result<(), AgentHarnessV2Error> {
        Ok(())
    }

    async fn health_probe(&self) -> Result<AgentHarnessHealthV2, AgentHarnessV2Error> {
        Ok(AgentHarnessHealthV2 { ready: true, reason_code: "harness.health.ready".to_owned() })
    }
}

/// Redacted harness health result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentHarnessHealthV2 {
    pub ready: bool,
    pub reason_code: String,
}

/// State-enforcing sink used at every host boundary.
pub struct ValidatingAgentHarnessEventSink<Sink> {
    generation: u64,
    last_sequence: u64,
    accepted: bool,
    terminal: bool,
    event_count: usize,
    inner: Sink,
}

impl<Sink> ValidatingAgentHarnessEventSink<Sink> {
    /// Wraps a sink with ordering and terminalization enforcement.
    #[must_use]
    pub fn new(generation: u64, inner: Sink) -> Self {
        Self {
            generation,
            last_sequence: 0,
            accepted: false,
            terminal: false,
            event_count: 0,
            inner,
        }
    }

    /// Returns whether exactly one terminal has been accepted.
    #[must_use]
    pub const fn terminalized(&self) -> bool {
        self.terminal
    }

    /// Returns the wrapped sink.
    #[must_use]
    pub fn into_inner(self) -> Sink {
        self.inner
    }

    fn validate_sequence(&self, generation: u64, sequence: u64) -> Result<(), AgentHarnessV2Error> {
        if generation != self.generation {
            return Err(AgentHarnessV2Error::StaleGeneration {
                active: self.generation,
                observed: generation,
            });
        }
        let expected =
            self.last_sequence.checked_add(1).ok_or(AgentHarnessV2Error::InvalidEvent)?;
        if sequence != expected {
            return Err(AgentHarnessV2Error::NonMonotonicSequence { expected, observed: sequence });
        }
        Ok(())
    }
}

#[async_trait]
impl<Sink> AgentHarnessEventSinkV2 for ValidatingAgentHarnessEventSink<Sink>
where
    Sink: AgentHarnessEventSinkV2,
{
    async fn accepted(
        &mut self,
        accepted: AgentHarnessAcceptedV2,
    ) -> Result<(), AgentHarnessV2Error> {
        if self.accepted {
            return Err(AgentHarnessV2Error::DuplicateAccepted);
        }
        if self.terminal {
            return Err(AgentHarnessV2Error::EventAfterTerminal);
        }
        self.validate_sequence(accepted.generation, accepted.sequence)?;
        if accepted.sequence != 1 {
            return Err(AgentHarnessV2Error::EventBeforeAccepted);
        }
        self.inner.accepted(accepted).await?;
        self.accepted = true;
        self.last_sequence = accepted.sequence;
        Ok(())
    }

    async fn event(&mut self, event: AgentHarnessEventV2) -> Result<(), AgentHarnessV2Error> {
        if !self.accepted {
            return Err(AgentHarnessV2Error::EventBeforeAccepted);
        }
        if self.terminal {
            return Err(AgentHarnessV2Error::EventAfterTerminal);
        }
        if self.event_count >= MAX_AGENT_HARNESS_EVENTS {
            return Err(AgentHarnessV2Error::EventLimitExceeded);
        }
        event.validate()?;
        self.validate_sequence(event.generation, event.sequence)?;
        self.inner.event(event.clone()).await?;
        self.last_sequence = event.sequence;
        self.event_count = self.event_count.saturating_add(1);
        Ok(())
    }

    async fn terminal(
        &mut self,
        terminal: AgentHarnessTerminalV2,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
        if !self.accepted {
            return Err(AgentHarnessV2Error::EventBeforeAccepted);
        }
        if self.terminal {
            return Err(AgentHarnessV2Error::DuplicateTerminal);
        }
        terminal.validate()?;
        self.validate_sequence(terminal.generation, terminal.sequence)?;
        let receipt = self.inner.terminal(terminal.clone()).await?;
        if receipt.generation != terminal.generation
            || receipt.terminal_sequence != terminal.sequence
            || receipt.event_count != self.event_count
        {
            return Err(AgentHarnessV2Error::InvalidTerminalReceipt);
        }
        self.last_sequence = terminal.sequence;
        self.terminal = true;
        Ok(receipt)
    }
}

/// Executes a harness through the strict sink and verifies terminalization on return.
///
/// # Errors
/// Returns [`AgentHarnessV2Error`] for invalid input, protocol violations, or missing terminal.
pub async fn execute_agent_harness_v2<Sink>(
    harness: &dyn AgentHarnessV2,
    request: &AgentHarnessAttemptRequestV2,
    host: &dyn HarnessHost,
    sink: Sink,
) -> Result<(AgentHarnessTerminalReceiptV2, Sink), AgentHarnessV2Error>
where
    Sink: AgentHarnessEventSinkV2,
{
    request.validate()?;
    if harness.descriptor().contract_version != AGENT_HARNESS_CONTRACT_VERSION_V2 {
        return Err(AgentHarnessV2Error::ContractVersionMismatch);
    }
    let mut validating = ValidatingAgentHarnessEventSink::new(request.generation, sink);
    let receipt = harness.run_attempt(request, host, &mut validating).await?;
    if !validating.terminalized() {
        return Err(AgentHarnessV2Error::MissingTerminal);
    }
    Ok((receipt, validating.into_inner()))
}

#[async_trait]
impl AgentHarnessV2 for EmbeddedPalyraHarness {
    fn descriptor(&self) -> &AgentHarnessDescriptor {
        #[allow(deprecated)]
        <Self as super::agent_harness::AgentHarness>::descriptor(self)
    }

    async fn run_attempt(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        _host: &dyn HarnessHost,
        sink: &mut dyn AgentHarnessEventSinkV2,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
        sink.accepted(AgentHarnessAcceptedV2 { generation: request.generation, sequence: 1 })
            .await?;
        sink.terminal(AgentHarnessTerminalV2 {
            generation: request.generation,
            sequence: 2,
            outcome: AgentHarnessTerminalOutcomeV2::Completed { final_message: None },
        })
        .await
    }
}

/// Async-v2 bridge for executable legacy adapters during the migration window.
pub struct LegacyAgentHarnessV2Adapter {
    descriptor: AgentHarnessDescriptor,
    legacy: Arc<dyn AgentHarness>,
}

impl LegacyAgentHarnessV2Adapter {
    /// Wraps a registered legacy adapter without widening its capabilities.
    #[must_use]
    pub fn new(legacy: Arc<dyn AgentHarness>) -> Self {
        Self { descriptor: legacy.descriptor().clone(), legacy }
    }
}

#[async_trait]
#[allow(deprecated)]
impl AgentHarnessV2 for LegacyAgentHarnessV2Adapter {
    fn descriptor(&self) -> &AgentHarnessDescriptor {
        &self.descriptor
    }

    async fn run_attempt(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        _host: &dyn HarnessHost,
        sink: &mut dyn AgentHarnessEventSinkV2,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
        sink.accepted(AgentHarnessAcceptedV2 { generation: request.generation, sequence: 1 })
            .await?;
        let legacy = Arc::clone(&self.legacy);
        let run_id = request.run_id.clone();
        let session_id = request.session_id.clone();
        let provider_id = request.provider_id.clone();
        let model_id = request.model_id.clone();
        let context_token_budget = request.context_token_budget;
        let reasoning_policy = request.reasoning_policy.clone();
        let transcript = request.sanitized_transcript.clone();
        let tool_surface = request.tool_surface.clone();
        let workspace_root = request.workspace_root.as_ref().map(PathBuf::from);
        let sandbox = request.sandbox.clone();
        let trace_context = request.trace_context.clone();
        let legacy_cancellation = AgentHarnessCancellation::default();
        let cancellation_for_attempt = legacy_cancellation.clone();
        let mut async_cancellation = request.cancellation.clone();
        let cancellation_for_watch = legacy_cancellation.clone();
        let cancellation_watch = tokio::spawn(async move {
            async_cancellation.cancelled().await;
            cancellation_for_watch.cancel();
        });
        let outcome = tokio::task::spawn_blocking(move || {
            let auth_state_metadata = Value::Null;
            let tool_policy = serde_json::json!({"host_owned": true});
            legacy.run_attempt(PreparedAgentAttempt {
                run_id: run_id.as_str(),
                session_id: session_id.as_str(),
                provider_id: provider_id.as_str(),
                model_id: model_id.as_str(),
                auth_state_metadata: &auth_state_metadata,
                context_token_budget,
                reasoning_policy: reasoning_policy.as_deref(),
                sanitized_transcript_view: transcript.as_slice(),
                tool_surface: &tool_surface,
                tool_policy: &tool_policy,
                workspace_root: workspace_root.as_deref(),
                sandbox: sandbox.as_str(),
                trace_context: trace_context.as_str(),
                callbacks: PreparedAgentAttemptCallbacks::host_controlled(),
                cancellation: cancellation_for_attempt,
            })
        })
        .await
        .map_err(|_| AgentHarnessV2Error::Transport {
            reason_code: "harness.legacy.worker_failed".to_owned(),
        })?;
        cancellation_watch.abort();
        let terminal = match outcome.status.as_str() {
            "completed" => {
                AgentHarnessTerminalOutcomeV2::Completed { final_message: outcome.final_message }
            }
            "cancelled" => AgentHarnessTerminalOutcomeV2::Cancelled {
                reason_code: "harness.legacy.cancelled".to_owned(),
            },
            "blocked" | "declined" => AgentHarnessTerminalOutcomeV2::Blocked {
                reason_code: "harness.legacy.blocked".to_owned(),
            },
            _ => AgentHarnessTerminalOutcomeV2::Failed {
                reason_code: "harness.legacy.failed".to_owned(),
                safe_message: "The legacy harness adapter failed its migrated async turn."
                    .to_owned(),
            },
        };
        sink.terminal(AgentHarnessTerminalV2 {
            generation: request.generation,
            sequence: 2,
            outcome: terminal,
        })
        .await
    }
}

/// Fail-closed asynchronous harness error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AgentHarnessV2Error {
    #[error("agent harness attempt request is invalid")]
    InvalidAttempt,
    #[error("agent harness descriptor contract version is incompatible")]
    ContractVersionMismatch,
    #[error("agent harness event arrived before acceptance")]
    EventBeforeAccepted,
    #[error("agent harness acceptance was emitted more than once")]
    DuplicateAccepted,
    #[error("agent harness terminal was emitted more than once")]
    DuplicateTerminal,
    #[error("agent harness returned without a terminal")]
    MissingTerminal,
    #[error("agent harness event arrived after terminalization")]
    EventAfterTerminal,
    #[error("agent harness generation is stale")]
    StaleGeneration { active: u64, observed: u64 },
    #[error("agent harness event sequence is not monotonic")]
    NonMonotonicSequence { expected: u64, observed: u64 },
    #[error("agent harness event is invalid")]
    InvalidEvent,
    #[error("agent harness event limit was exceeded")]
    EventLimitExceeded,
    #[error("agent harness terminal is invalid")]
    InvalidTerminal,
    #[error("agent harness terminal receipt is invalid")]
    InvalidTerminalReceipt,
    #[error("agent harness capability is not supported: {capability}")]
    UnsupportedCapability { capability: &'static str },
    #[error("agent harness host call failed: {0}")]
    Host(#[from] HarnessHostError),
    #[error("agent harness transport failed: {reason_code}")]
    Transport { reason_code: String },
}

fn valid_id(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256
}

fn valid_reason(value: &str) -> bool {
    valid_id(value)
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

/// Shared harness reference stored by registries and managers.
pub type SharedAgentHarnessV2 = Arc<dyn AgentHarnessV2>;

/// Capability set required by the full external runtime contract.
#[must_use]
pub const fn full_external_harness_capabilities() -> AgentHarnessCapabilities {
    AgentHarnessCapabilities {
        steering: true,
        resume: true,
        compaction: true,
        dynamic_tools: true,
        approvals: true,
        computer_use: false,
        transcript_mirror: true,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use super::*;
    use crate::application::agent_harness_host::{
        GuardedHarnessHost, HarnessCancellationContext, HarnessCapabilityStore, HarnessHostBackend,
        HarnessHostOperation,
    };

    #[derive(Debug, Default)]
    struct RecordingSink {
        accepted: usize,
        events: Vec<AgentHarnessEventV2>,
        terminal: Option<AgentHarnessTerminalV2>,
    }

    #[async_trait]
    impl AgentHarnessEventSinkV2 for RecordingSink {
        async fn accepted(
            &mut self,
            _accepted: AgentHarnessAcceptedV2,
        ) -> Result<(), AgentHarnessV2Error> {
            self.accepted = self.accepted.saturating_add(1);
            Ok(())
        }

        async fn event(&mut self, event: AgentHarnessEventV2) -> Result<(), AgentHarnessV2Error> {
            self.events.push(event);
            Ok(())
        }

        async fn terminal(
            &mut self,
            terminal: AgentHarnessTerminalV2,
        ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
            self.terminal = Some(terminal.clone());
            Ok(AgentHarnessTerminalReceiptV2 {
                generation: terminal.generation,
                terminal_sequence: terminal.sequence,
                event_count: self.events.len(),
            })
        }
    }

    #[derive(Debug)]
    struct NoopBackend;

    #[async_trait]
    impl HarnessHostBackend for NoopBackend {
        async fn invoke(
            &self,
            _operation: HarnessHostOperation,
            _payload: Value,
            _cancellation: HarnessCancellationContext,
        ) -> Result<Value, HarnessHostError> {
            Ok(Value::Null)
        }
    }

    #[derive(Debug)]
    struct FakeExternalHarness {
        descriptor: AgentHarnessDescriptor,
        duplicate_terminal: bool,
    }

    #[async_trait]
    impl AgentHarnessV2 for FakeExternalHarness {
        fn descriptor(&self) -> &AgentHarnessDescriptor {
            &self.descriptor
        }

        async fn run_attempt(
            &self,
            request: &AgentHarnessAttemptRequestV2,
            _host: &dyn HarnessHost,
            sink: &mut dyn AgentHarnessEventSinkV2,
        ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
            sink.accepted(AgentHarnessAcceptedV2 { generation: request.generation, sequence: 1 })
                .await?;
            sink.event(AgentHarnessEventV2 {
                generation: request.generation,
                sequence: 2,
                event: AgentHarnessEventKindV2::TextDelta { text: "ok".to_owned() },
            })
            .await?;
            let terminal = AgentHarnessTerminalV2 {
                generation: request.generation,
                sequence: 3,
                outcome: AgentHarnessTerminalOutcomeV2::Completed {
                    final_message: Some("ok".to_owned()),
                },
            };
            let receipt = sink.terminal(terminal.clone()).await?;
            if self.duplicate_terminal {
                sink.terminal(terminal).await?;
            }
            Ok(receipt)
        }
    }

    fn request(host_capability: HarnessCapabilityHandle) -> AgentHarnessAttemptRequestV2 {
        let (_sender, cancellation) = HarnessCancellationContext::channel();
        AgentHarnessAttemptRequestV2 {
            run_id: "run-1".to_owned(),
            session_id: "session-1".to_owned(),
            generation: 4,
            deadline_unix_ms: now_unix_ms_for_tests() + 5_000,
            provider_id: "provider".to_owned(),
            model_id: "model".to_owned(),
            context_token_budget: 8_192,
            reasoning_policy: None,
            sanitized_transcript: vec![json!({"role":"user","content":"hello"})],
            tool_surface: json!({"tools":[]}),
            tool_catalog_epoch: 1,
            workspace_root: None,
            sandbox: "host_owned".to_owned(),
            trace_context: "trace".to_owned(),
            host_capability,
            cancellation,
        }
    }

    fn host() -> (GuardedHarnessHost<NoopBackend>, HarnessCapabilityHandle) {
        let capabilities = Arc::new(HarnessCapabilityStore::default());
        let handle = capabilities
            .issue(
                "fake_external",
                4,
                vec![HarnessHostOperation::Heartbeat],
                now_unix_ms_for_tests() + 5_000,
            )
            .expect("capability");
        let (_sender, cancellation) = HarnessCancellationContext::channel();
        (
            GuardedHarnessHost::new(
                Arc::new(NoopBackend),
                capabilities,
                cancellation,
                Duration::from_secs(1),
            ),
            handle,
        )
    }

    fn fake(duplicate_terminal: bool) -> FakeExternalHarness {
        FakeExternalHarness {
            descriptor: AgentHarnessDescriptor::with_capabilities(
                "fake_external",
                "Fake external",
                false,
                full_external_harness_capabilities(),
            ),
            duplicate_terminal,
        }
    }

    fn now_unix_ms_for_tests() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn embedded_and_external_share_the_same_async_contract() {
        let (host, handle) = host();
        let request = request(handle);
        let embedded = EmbeddedPalyraHarness::default();

        let (embedded_receipt, embedded_sink) =
            execute_agent_harness_v2(&embedded, &request, &host, RecordingSink::default())
                .await
                .expect("embedded conformance");
        let (external_receipt, external_sink) =
            execute_agent_harness_v2(&fake(false), &request, &host, RecordingSink::default())
                .await
                .expect("external conformance");

        assert_eq!(embedded_sink.accepted, 1);
        assert_eq!(external_sink.accepted, 1);
        assert_eq!(embedded_receipt.generation, external_receipt.generation);
        assert!(embedded_sink.terminal.is_some());
        assert!(external_sink.terminal.is_some());
    }

    #[tokio::test]
    async fn duplicate_terminal_and_stale_generation_fail_closed() {
        let (host, handle) = host();
        let request = request(handle);
        let duplicate =
            execute_agent_harness_v2(&fake(true), &request, &host, RecordingSink::default())
                .await
                .expect_err("duplicate terminal");
        assert_eq!(duplicate, AgentHarnessV2Error::DuplicateTerminal);

        let mut stale_request = request;
        stale_request.generation = 5;
        let mut sink = ValidatingAgentHarnessEventSink::new(4, RecordingSink::default());
        let stale = sink
            .accepted(AgentHarnessAcceptedV2 { generation: stale_request.generation, sequence: 1 })
            .await
            .expect_err("stale generation");
        assert_eq!(stale, AgentHarnessV2Error::StaleGeneration { active: 4, observed: 5 });
    }
}
