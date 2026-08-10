//! Pure budget and message state for the run-stream agent loop.
//!
//! [`AgentRunLoopState`] tracks wall-clock budget plus the growing provider
//! message history; `orchestration` drives the actual loop. Step-count fields
//! are intentionally serialized as unlimited so long-horizon runs cannot be
//! terminated by a model-turn or tool-call ceiling. This module performs no
//! I/O, which keeps every budget rule unit-testable.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    application::verification::{VerificationCommandClassifier, VerificationKind},
    gateway::current_unix_ms,
    model_provider::{
        ProviderMessage, ProviderMessageRole, ProviderResponse, ProviderTurnOutput,
        TerminalOutcomeClass, TerminalOutcomeClassification,
    },
};
use palyra_common::process_runner_input::parse_process_runner_tool_input;

/// Default wall-clock budget per run (15 minutes) covering long browser workflows.
pub(crate) const DEFAULT_AGENT_LOOP_WALL_CLOCK_BUDGET_MS: u64 = 900_000;

const BROWSER_SESSION_CREATE_TOOL_NAME: &str = "palyra.browser.session.create";
const BROWSER_SESSION_CLOSE_TOOL_NAME: &str = "palyra.browser.session.close";
const OS_FILE_TOOL_NAME: &str = "palyra.fs.os_file";
const PROCESS_RUN_TOOL_NAME: &str = "palyra.process.run";
const PROCESS_STATUS_TOOL_NAME: &str = "palyra.process.status";
const PROCESS_STOP_TOOL_NAME: &str = "palyra.process.stop";
const ROUTINES_QUERY_TOOL_NAME: &str = "palyra.routines.query";
const ROUTINES_CONTROL_TOOL_NAME: &str = "palyra.routines.control";
const WORKSPACE_LIST_DIR_TOOL_NAME: &str = "palyra.fs.list_dir";
const WORKSPACE_PATCH_TOOL_NAME: &str = "palyra.fs.apply_patch";
const WORKSPACE_READ_FILE_TOOL_NAME: &str = "palyra.fs.read_file";
const WORKSPACE_SEARCH_TOOL_NAME: &str = "palyra.fs.search";
const RUN_PROGRESS_CHECKPOINT_SCHEMA_VERSION: u32 = 1;
const RUN_PROGRESS_CHECKPOINT_MAX_BYTES: usize = 8 * 1024;
const RUN_PROGRESS_MAX_PRODUCED_FILES: usize = 16;
const RUN_PROGRESS_MAX_MISSING_ARTIFACTS: usize = 12;
const RUN_PROGRESS_MAX_ACTIVE_PROCESSES: usize = 8;
const RUN_PROGRESS_MAX_FAILED_ATTEMPTS: usize = 8;
const FINAL_ANSWER_CONTRACT_SCHEMA_VERSION: u32 = 1;
const FINAL_ANSWER_CONTRACT_REDACTION_LEVEL: &str = "metadata_only";

/// Tape event emitted when a repeated tool signature first crosses the soft guardrail.
pub(crate) const TOOL_LOOP_WARNING_EVENT: &str = "tool.loop.warning";
/// Tape event emitted when guardrail guidance is injected into the next model turn.
pub(crate) const TOOL_LOOP_GUIDANCE_INJECTED_EVENT: &str = "tool.loop.guidance_injected";
/// Tape event emitted when a repeated denied tool loop is stopped.
pub(crate) const TOOL_LOOP_BLOCKED_EVENT: &str = "tool.loop.blocked";
/// Audit event projected when a final answer contract begins evaluation.
pub(crate) const FINAL_ANSWER_CONTRACT_STARTED_EVENT: &str =
    "final_answer_contract_a_evidence_summary.started";
/// Audit event projected when the terminal answer satisfies the final answer contract.
pub(crate) const FINAL_ANSWER_CONTRACT_COMPLETED_EVENT: &str =
    "final_answer_contract_a_evidence_summary.completed";
/// Audit event projected when the terminal answer is failed or requires continuation.
pub(crate) const FINAL_ANSWER_CONTRACT_FAILED_EVENT: &str =
    "final_answer_contract_a_evidence_summary.failed";
/// Soft guard event emitted when a code-changing run tries to finish without fresh verification.
pub(crate) const VERIFICATION_FINALIZER_NUDGE_EVENT: &str = "verification.finalizer.nudge";
/// Soft guard event emitted when the final answer explicitly carries an unverified reason.
pub(crate) const VERIFICATION_FINALIZER_UNVERIFIED_ALLOWED_EVENT: &str =
    "verification.finalizer.unverified_allowed";

/// Observable class of one tool attempt for repeated-no-progress detection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolResultClass {
    Success,
    Failure,
    ValidationFailure,
    PolicyDenied,
    ApprovalDenied,
    NotFound,
    Timeout,
    ReadNoProgress,
}

impl ToolResultClass {
    /// Stable snake_case label used in guardrail reason codes.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::ValidationFailure => "validation_failure",
            Self::PolicyDenied => "policy_denied",
            Self::ApprovalDenied => "approval_denied",
            Self::NotFound => "not_found",
            Self::Timeout => "timeout",
            Self::ReadNoProgress => "read_no_progress",
        }
    }

    const fn is_denial(&self) -> bool {
        matches!(self, Self::PolicyDenied | Self::ApprovalDenied)
    }
}

pub(crate) type RunProgressOutcomeClass = ToolResultClass;

/// Process-local normalized attempt data fed into [`RunProgressController`].
///
/// The output digest supports equality checks only and must never become
/// replay-visible diagnostic evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunProgressAttempt {
    pub(crate) tool_name: String,
    pub(crate) normalized_input_json: Vec<u8>,
    pub(crate) normalized_output_hash: Option<String>,
    pub(crate) volatile_output_fields: Vec<String>,
    pub(crate) workspace_key: Option<String>,
    pub(crate) query_hash: Option<String>,
    pub(crate) progress_percent: Option<u8>,
    pub(crate) sensitivity: String,
    pub(crate) outcome_class: RunProgressOutcomeClass,
}

/// Stable per-run signature for repeated tool-call guardrails.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ToolCallSignature {
    pub(crate) tool_name: String,
    pub(crate) canonical_arguments_hash: String,
    pub(crate) normalized_path_scope: Option<String>,
    pub(crate) query_hash: Option<String>,
    pub(crate) last_result_class: ToolResultClass,
}

/// Stable attempt fingerprint used for loop detection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RunProgressAttemptFingerprint {
    pub(crate) tool_name: String,
    pub(crate) input_hash: String,
    pub(crate) workspace_key: Option<String>,
    pub(crate) query_hash: Option<String>,
    pub(crate) sensitivity: String,
    pub(crate) outcome_class: RunProgressOutcomeClass,
}

/// Controller decision emitted when repeated attempts stop making progress.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RunProgressIntervention {
    pub(crate) event_type: String,
    pub(crate) reason_code: String,
    pub(crate) signature: ToolCallSignature,
    pub(crate) fingerprint: RunProgressAttemptFingerprint,
    pub(crate) attempts: u32,
    pub(crate) guidance: String,
    pub(crate) terminate_run: bool,
    pub(crate) learning_observation: String,
    pub(crate) detection: LoopDetectionEvidence,
}

/// Redacted, replay-visible evidence explaining why a run loop intervened.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LoopDetectionEvidence {
    pub(crate) schema_version: u32,
    pub(crate) detector_type: LoopDetectorType,
    pub(crate) cycle_length: Option<u8>,
    pub(crate) monotonic_progress: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) volatile_fields_stripped: Vec<String>,
}

/// Host-defined detector classification used by diagnostics and no-progress metrics.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LoopDetectorType {
    RepeatedSignature,
    AlternatingCycle,
    VolatileFieldPoll,
}

/// Pure no-progress detector for repeated tool failure, denial, or read loops.
#[derive(Debug, Clone)]
pub(crate) struct RunProgressController {
    guardrail_state: ToolLoopGuardrailState,
    recent_observations: VecDeque<RunProgressObservation>,
    poll_counts: BTreeMap<String, u32>,
    last_progress_percent: BTreeMap<String, u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunProgressObservation {
    series_key: String,
    fingerprint_key: String,
    outcome_hash: String,
}

/// Per-run repeated tool-call counters, keyed by sanitized call signature.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolLoopGuardrailState {
    guidance_threshold: u32,
    attempt_counts: BTreeMap<String, ToolLoopGuardrailCounter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolLoopGuardrailCounter {
    pub(crate) signature: ToolCallSignature,
    pub(crate) attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ToolLoopGuardrailDecision {
    signature: ToolCallSignature,
    attempts: u32,
    event_type: &'static str,
    reason_code: String,
    block_run: bool,
}

impl RunProgressController {
    /// Creates a controller that intervenes after `max_repeated_attempts`.
    #[must_use]
    pub(crate) fn new(max_repeated_attempts: u32) -> Self {
        Self {
            guardrail_state: ToolLoopGuardrailState::new(max_repeated_attempts),
            recent_observations: VecDeque::with_capacity(6),
            poll_counts: BTreeMap::new(),
            last_progress_percent: BTreeMap::new(),
        }
    }

    /// Records an attempt and returns guidance once the same failure repeats.
    pub(crate) fn observe(
        &mut self,
        attempt: RunProgressAttempt,
    ) -> Option<RunProgressIntervention> {
        if attempt.outcome_class == RunProgressOutcomeClass::Success {
            self.clear();
            return None;
        }
        if attempt.outcome_class == RunProgressOutcomeClass::ReadNoProgress
            && attempt.normalized_output_hash.is_some()
        {
            return self.observe_read_progress(attempt);
        }
        let decision = self.guardrail_state.observe(&attempt)?;
        let fingerprint = attempt.fingerprint();
        Some(RunProgressIntervention {
            event_type: decision.event_type.to_owned(),
            reason_code: decision.reason_code,
            signature: decision.signature,
            guidance: guidance_for_repeated_attempt(&fingerprint, decision.attempts),
            terminate_run: decision.block_run,
            learning_observation: format!(
                "repeated_no_progress tool={} outcome={:?} attempts={}",
                fingerprint.tool_name, fingerprint.outcome_class, decision.attempts
            ),
            detection: LoopDetectionEvidence {
                schema_version: 1,
                detector_type: LoopDetectorType::RepeatedSignature,
                cycle_length: None,
                monotonic_progress: false,
                volatile_fields_stripped: attempt.volatile_output_fields,
            },
            fingerprint,
            attempts: decision.attempts,
        })
    }

    fn observe_read_progress(
        &mut self,
        attempt: RunProgressAttempt,
    ) -> Option<RunProgressIntervention> {
        let series_key = attempt.series_key();
        if let Some(progress_percent) = attempt.progress_percent {
            let previous = self.last_progress_percent.get(series_key.as_str()).copied();
            self.last_progress_percent.insert(series_key.clone(), progress_percent);
            if previous.is_none_or(|value| progress_percent > value) {
                self.poll_counts.retain(|key, _| !key.starts_with(series_key.as_str()));
                self.recent_observations.retain(|observation| observation.series_key != series_key);
                return None;
            }
        }

        let fingerprint = attempt.fingerprint();
        let outcome_hash = attempt.normalized_output_hash.clone().unwrap_or_default();
        let observation = RunProgressObservation {
            series_key: series_key.clone(),
            fingerprint_key: format!("{}:{outcome_hash}", fingerprint.input_hash),
            outcome_hash: outcome_hash.clone(),
        };
        self.recent_observations.push_back(observation);
        while self.recent_observations.len() > 6 {
            self.recent_observations.pop_front();
        }

        if let Some(cycle_attempts) = self.alternating_cycle_attempts(series_key.as_str()) {
            return Some(self.loop_intervention(
                attempt,
                fingerprint,
                cycle_attempts,
                LoopDetectorType::AlternatingCycle,
                Some(2),
            ));
        }

        let poll_key = format!("{series_key}:{}:{outcome_hash}", fingerprint.input_hash);
        let attempts = {
            let attempts = self.poll_counts.entry(poll_key).or_default();
            *attempts = attempts.saturating_add(1);
            *attempts
        };
        if attempts < self.guardrail_state.guidance_threshold {
            return None;
        }
        Some(self.loop_intervention(
            attempt,
            fingerprint,
            attempts,
            LoopDetectorType::VolatileFieldPoll,
            None,
        ))
    }

    fn alternating_cycle_attempts(&self, series_key: &str) -> Option<u32> {
        let observations = self
            .recent_observations
            .iter()
            .filter(|observation| observation.series_key == series_key)
            .collect::<Vec<_>>();
        if observations.len() < 4 {
            return None;
        }
        let tail = &observations[observations.len().saturating_sub(6)..];
        let alternating = tail.len() >= 4
            && tail.iter().enumerate().all(|(index, observation)| {
                observation.fingerprint_key == tail[index % 2].fingerprint_key
            })
            && tail[0].fingerprint_key != tail[1].fingerprint_key
            && tail[0].outcome_hash == tail[2].outcome_hash
            && tail[1].outcome_hash == tail[3].outcome_hash;
        alternating.then(|| u32::try_from(tail.len()).unwrap_or(u32::MAX))
    }

    fn loop_intervention(
        &self,
        attempt: RunProgressAttempt,
        fingerprint: RunProgressAttemptFingerprint,
        attempts: u32,
        detector_type: LoopDetectorType,
        cycle_length: Option<u8>,
    ) -> RunProgressIntervention {
        let terminate_run = match detector_type {
            LoopDetectorType::AlternatingCycle => attempts >= 6,
            LoopDetectorType::VolatileFieldPoll => {
                attempts > self.guardrail_state.guidance_threshold
            }
            LoopDetectorType::RepeatedSignature => false,
        };
        let event_type =
            if terminate_run { TOOL_LOOP_BLOCKED_EVENT } else { TOOL_LOOP_WARNING_EVENT };
        let detector_label = match detector_type {
            LoopDetectorType::AlternatingCycle => "alternating_cycle",
            LoopDetectorType::VolatileFieldPoll => "volatile_field_poll",
            LoopDetectorType::RepeatedSignature => "repeated_signature",
        };
        RunProgressIntervention {
            event_type: event_type.to_owned(),
            reason_code: format!("tool.loop.{detector_label}"),
            signature: attempt.signature(),
            fingerprint: fingerprint.clone(),
            attempts,
            guidance: guidance_for_repeated_attempt(&fingerprint, attempts),
            terminate_run,
            learning_observation: format!(
                "repeated_no_progress detector={detector_label} tool={} attempts={attempts}",
                fingerprint.tool_name
            ),
            detection: LoopDetectionEvidence {
                schema_version: 1,
                detector_type,
                cycle_length,
                monotonic_progress: false,
                volatile_fields_stripped: attempt.volatile_output_fields,
            },
        }
    }

    fn clear(&mut self) {
        self.guardrail_state.clear();
        self.recent_observations.clear();
        self.poll_counts.clear();
        self.last_progress_percent.clear();
    }
}

impl ToolLoopGuardrailState {
    /// Creates per-run guardrail state with a conservative soft guidance threshold.
    #[must_use]
    pub(crate) fn new(guidance_threshold: u32) -> Self {
        Self { guidance_threshold: guidance_threshold.max(1), attempt_counts: BTreeMap::new() }
    }

    /// Records a sanitized signature and returns the guardrail decision, if any.
    fn observe(&mut self, attempt: &RunProgressAttempt) -> Option<ToolLoopGuardrailDecision> {
        let signature = attempt.signature();
        let counter = self.attempt_counts.entry(signature.stable_key()).or_insert_with(|| {
            ToolLoopGuardrailCounter { signature: signature.clone(), attempts: 0 }
        });
        counter.attempts = counter.attempts.saturating_add(1);
        if counter.attempts < self.guidance_threshold {
            return None;
        }
        let denial = counter.signature.last_result_class.is_denial();
        let mutating_failure = !denial
            && is_mutating_tool_for_loop_guardrail(counter.signature.tool_name.as_str())
            && !matches!(
                counter.signature.last_result_class,
                ToolResultClass::Success | ToolResultClass::ReadNoProgress
            );
        let block_run = denial || mutating_failure || counter.attempts > self.guidance_threshold;
        let event_type = if block_run { TOOL_LOOP_BLOCKED_EVENT } else { TOOL_LOOP_WARNING_EVENT };
        let reason_code = if mutating_failure && block_run {
            format!("tool.loop.mutating_{}", counter.signature.last_result_class.as_str())
        } else {
            format!("tool.loop.{}", counter.signature.last_result_class.as_str())
        };
        Some(ToolLoopGuardrailDecision {
            reason_code,
            signature: counter.signature.clone(),
            attempts: counter.attempts,
            event_type,
            block_run,
        })
    }

    fn clear(&mut self) {
        self.attempt_counts.clear();
    }
}

fn is_mutating_tool_for_loop_guardrail(tool_name: &str) -> bool {
    matches!(
        tool_name,
        WORKSPACE_PATCH_TOOL_NAME
            | PROCESS_RUN_TOOL_NAME
            | PROCESS_STOP_TOOL_NAME
            | BROWSER_SESSION_CREATE_TOOL_NAME
            | BROWSER_SESSION_CLOSE_TOOL_NAME
            | ROUTINES_CONTROL_TOOL_NAME
    )
}

impl RunProgressAttempt {
    fn signature(&self) -> ToolCallSignature {
        ToolCallSignature {
            tool_name: self.tool_name.clone(),
            canonical_arguments_hash: crate::sha256_hex(self.normalized_input_json.as_slice()),
            normalized_path_scope: self.workspace_key.clone(),
            query_hash: self.query_hash.clone(),
            last_result_class: self.outcome_class.clone(),
        }
    }

    fn fingerprint(&self) -> RunProgressAttemptFingerprint {
        RunProgressAttemptFingerprint {
            tool_name: self.tool_name.clone(),
            input_hash: crate::sha256_hex(self.normalized_input_json.as_slice()),
            workspace_key: self.workspace_key.clone(),
            query_hash: self.query_hash.clone(),
            sensitivity: self.sensitivity.clone(),
            outcome_class: self.outcome_class.clone(),
        }
    }

    fn series_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.tool_name,
            self.workspace_key.as_deref().unwrap_or(""),
            self.query_hash.as_deref().unwrap_or("")
        )
    }
}

impl ToolCallSignature {
    fn stable_key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.tool_name,
            self.canonical_arguments_hash,
            self.normalized_path_scope.as_deref().unwrap_or(""),
            self.query_hash.as_deref().unwrap_or(""),
            self.last_result_class.as_str()
        )
    }
}

fn guidance_for_repeated_attempt(
    fingerprint: &RunProgressAttemptFingerprint,
    attempts: u32,
) -> String {
    match fingerprint.outcome_class {
        RunProgressOutcomeClass::PolicyDenied | RunProgressOutcomeClass::ApprovalDenied => format!(
            "Tool '{}' was denied {attempts} times with the same arguments; stop retrying and ask for a changed policy, approval, or alternate plan.",
            fingerprint.tool_name
        ),
        RunProgressOutcomeClass::ValidationFailure => format!(
            "Tool '{}' failed validation {attempts} times with the same arguments; inspect the schema, change the arguments, or use catalog describe before retrying.",
            fingerprint.tool_name
        ),
        RunProgressOutcomeClass::NotFound => format!(
            "Tool '{}' returned not-found {attempts} times for the same scope; change the path/query or verify the target exists before retrying.",
            fingerprint.tool_name
        ),
        RunProgressOutcomeClass::Timeout => format!(
            "Tool '{}' timed out {attempts} times with the same arguments; narrow the request or choose a cheaper verification path before retrying.",
            fingerprint.tool_name
        ),
        RunProgressOutcomeClass::ReadNoProgress => format!(
            "Tool '{}' repeated the same read without new evidence {attempts} times; change query/path or summarize current evidence before continuing.",
            fingerprint.tool_name
        ),
        RunProgressOutcomeClass::Failure => format!(
            "Tool '{}' failed {attempts} times with the same normalized input; change the input or choose a different tool before retrying.",
            fingerprint.tool_name
        ),
        RunProgressOutcomeClass::Success => "progress observed".to_owned(),
    }
}

/// Why the agent loop stopped; serialized into tape payloads as `snake_case`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AgentLoopTerminationReason {
    FinalAnswer,
    /// Legacy replay reason retained for older tapes; runtime no longer
    /// terminates agentic workflows by model-turn count.
    MaxTurns,
    /// Legacy replay reason retained for older tapes; runtime no longer
    /// terminates agentic workflows by tool-call count.
    MaxToolCalls,
    WallClock,
    Cancellation,
    ApprovalDenied,
    ProviderError,
    ContextBudgetExhausted,
    IncompleteFinalAnswer,
    RepeatedToolFailure,
    BrowserFollowupTimeout,
    ToolFollowupTimeout,
    RunLoopPhaseTimeout,
}

impl AgentLoopTerminationReason {
    /// Returns the stable `snake_case` reason code used in tape payloads and
    /// `reason_code=` status markers.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::FinalAnswer => "final_answer",
            Self::MaxTurns => "max_turns",
            Self::MaxToolCalls => "max_tool_calls",
            Self::WallClock => "wall_clock",
            Self::Cancellation => "cancellation",
            Self::ApprovalDenied => "approval_denied",
            Self::ProviderError => "provider_error",
            Self::ContextBudgetExhausted => "context_budget_exhausted",
            Self::IncompleteFinalAnswer => "incomplete_final_answer",
            Self::RepeatedToolFailure => "repeated_tool_failure",
            Self::BrowserFollowupTimeout => "browser_followup_timeout",
            Self::ToolFollowupTimeout => "tool_followup_timeout",
            Self::RunLoopPhaseTimeout => "run_loop_phase_timeout",
        }
    }

    /// Returns `true` only for a clean final answer.
    pub(crate) const fn is_success(self) -> bool {
        matches!(self, Self::FinalAnswer)
    }

    /// Returns `true` when the run should be surfaced as a resumable partial.
    ///
    /// Requires at least one completed tool call: without tool evidence there
    /// is nothing to continue from, so the same reasons stay plain failures.
    pub(crate) const fn needs_continuation(self, completed_tool_calls: u32) -> bool {
        completed_tool_calls > 0
            && matches!(
                self,
                Self::WallClock
                    | Self::ProviderError
                    | Self::IncompleteFinalAnswer
                    | Self::BrowserFollowupTimeout
                    | Self::ToolFollowupTimeout
                    | Self::RunLoopPhaseTimeout
            )
    }
}

/// Accumulated provider token usage across all turns of one run.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentLoopUsageSnapshot {
    pub(crate) prompt_tokens: u64,
    pub(crate) completion_tokens: u64,
    pub(crate) total_tokens: u64,
}

impl AgentLoopUsageSnapshot {
    fn add(&mut self, prompt_tokens: u64, completion_tokens: u64) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(completion_tokens);
        self.total_tokens = self.prompt_tokens.saturating_add(self.completion_tokens);
    }
}

/// Point-in-time view of loop budgets, serialized into agent-loop tape events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentLoopSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) current_turn: u32,
    pub(crate) model_turn_limit: Option<u32>,
    pub(crate) remaining_model_turns: Option<u32>,
    pub(crate) tool_call_limit: Option<u32>,
    pub(crate) remaining_tool_calls: Option<u32>,
    pub(crate) completed_tool_calls: u32,
    pub(crate) message_count: usize,
    pub(crate) wall_clock_budget_ms: u64,
    pub(crate) wall_clock_remaining_ms: u64,
    pub(crate) active_limits: Vec<String>,
    pub(crate) elapsed_ms: u64,
    pub(crate) usage: AgentLoopUsageSnapshot,
    pub(crate) termination_reason: Option<AgentLoopTerminationReason>,
}

/// Terminal envelope written to the `agent_loop.terminated` tape event,
/// carrying the status/lifecycle/continuation contract consumers replay.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentLoopFinalizationEnvelope {
    pub(crate) schema_version: u32,
    pub(crate) termination_reason: AgentLoopTerminationReason,
    pub(crate) status: String,
    pub(crate) lifecycle_state: String,
    pub(crate) reason_code: String,
    pub(crate) terminal_outcome: TerminalOutcomeClassification,
    pub(crate) partial: bool,
    pub(crate) continuation_required: bool,
    pub(crate) user_visible_message: String,
    pub(crate) usage: AgentLoopUsageSnapshot,
    pub(crate) tool_count: u32,
    pub(crate) artifact_refs: Vec<String>,
    pub(crate) final_answer_contract: FinalAnswerContract,
    pub(crate) evidence_summary: FinalAnswerEvidenceSummary,
    pub(crate) verification_finalizer: FinalizationVerificationReport,
    pub(crate) progress_checkpoint: Option<RunProgressCheckpoint>,
    pub(crate) provider_trace_ref: Option<String>,
}

/// Terminal decision for the final answer contract.
///
/// The contract is observe-only: it gives objective
/// finalization and replay consumers a stable judgment without changing the
/// run state machine or bypassing existing policy/tool gates.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinalAnswerDecision {
    Accepted,
    NeedsContinuation,
    Rejected,
}

/// Evidence coverage derived from recorded tool/message state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinalAnswerEvidenceCoverage {
    NotRequired,
    Satisfied,
    GapsDetected,
    NoToolEvidence,
}

/// Soft verification status applied before accepting a final answer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinalizationVerificationStatus {
    NotRequired,
    Verified,
    NudgeRequired,
    UnverifiedAllowed,
}

/// Bounded summary of one stale requirement still relevant at finalization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalizationVerificationRequirementSummary {
    pub(crate) requirement_id: String,
    pub(crate) required_kind: String,
    pub(crate) changed_paths: Vec<String>,
}

/// Verify-before-finish report embedded in the terminal finalization envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalizationVerificationReport {
    pub(crate) schema_version: u32,
    pub(crate) status: FinalizationVerificationStatus,
    pub(crate) reason_code: String,
    pub(crate) enforcement_mode: String,
    pub(crate) surface_policy: String,
    pub(crate) code_mutation_seen: bool,
    pub(crate) pending_requirement_count: usize,
    pub(crate) satisfied_requirement_count: usize,
    pub(crate) pending_requirements: Vec<FinalizationVerificationRequirementSummary>,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) event_type: Option<String>,
    pub(crate) nudge: Option<String>,
    pub(crate) unverified_reason: Option<String>,
    pub(crate) redaction_level: String,
}

/// Pure soft guard for final answers after code mutation.
pub(crate) struct VerifyBeforeFinishGuard;

struct VerifyBeforeFinishRequest<'a> {
    run_id: &'a str,
    reason: AgentLoopTerminationReason,
    final_answer: &'a str,
    messages: &'a [ProviderMessage],
}

/// Tape projection for the terminal final answer contract event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalAnswerJournalProjection {
    pub(crate) schema_version: u32,
    pub(crate) event_type: String,
    pub(crate) source_event_refs: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Final answer contract embedded in `agent_loop.terminated`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalAnswerContract {
    pub(crate) schema_version: u32,
    pub(crate) decision: FinalAnswerDecision,
    pub(crate) reason_code: String,
    pub(crate) final_answer_required: bool,
    pub(crate) evidence_summary_required: bool,
    pub(crate) tool_evidence_required_for_tool_claims: bool,
    pub(crate) enforcement_mode: String,
    pub(crate) journal_projection: FinalAnswerJournalProjection,
    pub(crate) event_types: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Bounded evidence summary attached to the final answer contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalAnswerEvidenceSummary {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) decision: FinalAnswerDecision,
    pub(crate) coverage: FinalAnswerEvidenceCoverage,
    pub(crate) reason_code: String,
    pub(crate) tool_count: u32,
    pub(crate) produced_files_count: usize,
    pub(crate) missing_artifacts_count: usize,
    pub(crate) active_process_count: usize,
    pub(crate) known_failed_attempt_count: usize,
    pub(crate) last_successful_tool: Option<String>,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Structured continuation state inferred from already-recorded tool evidence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RunProgressCheckpoint {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) task_goal_summary: String,
    pub(crate) last_successful_tool: Option<RunProgressToolSummary>,
    pub(crate) produced_files: Vec<RunProgressFileSummary>,
    pub(crate) missing_artifacts: Vec<RunProgressMissingArtifact>,
    pub(crate) active_processes: Vec<RunProgressProcessSummary>,
    pub(crate) known_failed_attempts: Vec<String>,
    pub(crate) recommended_next_action: String,
}

/// Last completed tool step worth handing to a continuation run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RunProgressToolSummary {
    pub(crate) tool_name: String,
    pub(crate) summary: String,
    pub(crate) artifact_refs: Vec<String>,
}

/// File artifact produced or updated by a successful file-writing tool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RunProgressFileSummary {
    pub(crate) path: String,
    pub(crate) root_label: String,
    pub(crate) status: String,
    pub(crate) operation: String,
    pub(crate) sha256: Option<String>,
    pub(crate) size_bytes: Option<u64>,
}

/// Artifact name inferred from the task prompt but not yet seen in tool output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RunProgressMissingArtifact {
    pub(crate) path: String,
    pub(crate) root_label: String,
    pub(crate) required_by: String,
}

/// Run-owned process state that may matter to a continuation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RunProgressProcessSummary {
    pub(crate) pid: u32,
    pub(crate) kind: String,
    pub(crate) status: String,
    pub(crate) cleanup: String,
    pub(crate) log_artifact: Option<String>,
}

/// Mutable budget and message state for one agent-loop run.
///
/// Owns the provider message history (user input, assistant turns, tool
/// results, recovery guidance) and enforces only the wall-clock budget. All
/// methods are synchronous and side-effect free beyond `self`.
#[derive(Debug, Clone)]
pub(crate) struct AgentRunLoopState {
    messages: Vec<ProviderMessage>,
    max_model_turns: Option<u32>,
    remaining_model_turns: Option<u32>,
    max_tool_calls: Option<u32>,
    remaining_tool_calls: Option<u32>,
    wall_clock_budget_ms: u64,
    started_at_unix_ms: i64,
    started_at: Instant,
    current_turn: u32,
    completed_tool_calls: u32,
    usage: AgentLoopUsageSnapshot,
}

impl AgentRunLoopState {
    /// Creates loop state seeded with the initial provider messages.
    ///
    /// Legacy model/tool count arguments are accepted for call-site
    /// compatibility but intentionally ignored. `wall_clock_budget_ms` is
    /// raised to at least 1 ms, so the loop cannot start with an already-spent
    /// clock budget.
    pub(crate) fn new(
        messages: Vec<ProviderMessage>,
        _legacy_max_model_turns: u32,
        _legacy_max_tool_calls: u32,
        wall_clock_budget_ms: u64,
    ) -> Self {
        Self {
            messages,
            max_model_turns: None,
            remaining_model_turns: None,
            max_tool_calls: None,
            remaining_tool_calls: None,
            wall_clock_budget_ms: wall_clock_budget_ms.max(1),
            started_at_unix_ms: current_unix_ms(),
            started_at: Instant::now(),
            current_turn: 0,
            completed_tool_calls: 0,
            usage: AgentLoopUsageSnapshot::default(),
        }
    }

    /// Consumes one model turn and returns its 1-based turn id.
    ///
    /// # Errors
    ///
    /// Returns [`AgentLoopTerminationReason::WallClock`] when the wall-clock
    /// budget is spent.
    pub(crate) fn start_model_turn(&mut self) -> Result<u32, AgentLoopTerminationReason> {
        if self.elapsed() > Duration::from_millis(self.wall_clock_budget_ms) {
            return Err(AgentLoopTerminationReason::WallClock);
        }
        self.current_turn = self.current_turn.saturating_add(1);
        Ok(self.current_turn)
    }

    /// Accumulates token usage reported by a provider turn.
    pub(crate) fn record_provider_response(&mut self, response: &ProviderResponse) {
        self.usage.add(response.prompt_tokens, response.completion_tokens);
    }

    /// Appends the assistant turn (text and tool calls) to the history.
    pub(crate) fn append_assistant_turn(&mut self, output: &ProviderTurnOutput) {
        self.messages.push(ProviderMessage::assistant_from_output(output));
    }

    /// Appends a synthetic user message used for recovery guidance prompts.
    pub(crate) fn append_user_guidance(&mut self, text: impl Into<String>) {
        self.messages.push(ProviderMessage::user_text(text.into()));
    }

    /// Appends tool-result messages and counts them as completed tool calls.
    pub(crate) fn append_tool_result_messages(&mut self, messages: Vec<ProviderMessage>) {
        let added = messages.len().try_into().unwrap_or(u32::MAX);
        self.completed_tool_calls = self.completed_tool_calls.saturating_add(added);
        self.messages.extend(messages);
    }

    /// Returns an owned copy of the message history for the next provider request.
    pub(crate) fn messages(&self) -> Vec<ProviderMessage> {
        self.messages.clone()
    }

    /// Replaces only the provider projection after a preflight context
    /// recovery; durable session evidence remains in the journal.
    pub(crate) fn replace_messages(&mut self, messages: Vec<ProviderMessage>) {
        self.messages = messages;
    }

    /// Number of tool results appended so far in this run.
    pub(crate) fn completed_tool_calls(&self) -> u32 {
        self.completed_tool_calls
    }

    /// Accepts legacy tool-budget sync calls without making them terminal.
    pub(crate) fn sync_remaining_tool_calls(&mut self, _remaining_tool_calls: u32) {
        self.remaining_tool_calls = None;
    }

    /// Builds the serializable budget snapshot embedded in tape payloads.
    pub(crate) fn snapshot(
        &self,
        run_id: &str,
        termination_reason: Option<AgentLoopTerminationReason>,
    ) -> AgentLoopSnapshot {
        let elapsed_ms = self.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        AgentLoopSnapshot {
            schema_version: 1,
            run_id: run_id.to_owned(),
            current_turn: self.current_turn,
            model_turn_limit: self.max_model_turns,
            remaining_model_turns: self.remaining_model_turns,
            tool_call_limit: self.max_tool_calls,
            remaining_tool_calls: self.remaining_tool_calls,
            completed_tool_calls: self.completed_tool_calls,
            message_count: self.messages.len(),
            wall_clock_budget_ms: self.wall_clock_budget_ms,
            wall_clock_remaining_ms: self.wall_clock_budget_ms.saturating_sub(elapsed_ms),
            active_limits: self.active_limits(),
            elapsed_ms,
            usage: self.usage.clone(),
            termination_reason,
        }
    }

    fn active_limits(&self) -> Vec<String> {
        let mut limits = vec!["wall_clock".to_owned()];
        if self.max_model_turns.is_some() {
            limits.push("model_turns".to_owned());
        }
        if self.max_tool_calls.is_some() {
            limits.push("tool_calls".to_owned());
        }
        limits
    }

    fn finalization_outcome(
        &self,
        reason: AgentLoopTerminationReason,
    ) -> AgentLoopFinalizationOutcome {
        if reason.is_success() {
            return AgentLoopFinalizationOutcome::completed(reason);
        }
        if reason.needs_continuation(self.completed_tool_calls) {
            return AgentLoopFinalizationOutcome::needs_continuation(reason);
        }
        AgentLoopFinalizationOutcome::failed(reason)
    }

    /// Builds the terminal envelope for `reason`, classifying the run as
    /// completed, resumable partial (`needs_continuation`), or failed.
    pub(crate) fn finalization_envelope(
        &self,
        run_id: &str,
        reason: AgentLoopTerminationReason,
        user_visible_message: impl Into<String>,
        provider_trace_ref: Option<String>,
    ) -> AgentLoopFinalizationEnvelope {
        let user_visible_message = user_visible_message.into();
        let outcome = self.finalization_outcome(reason);
        let terminal_outcome =
            terminal_outcome_from_finalization(reason, user_visible_message.as_str());
        let evidence_checkpoint = self.progress_checkpoint(run_id, reason);
        let final_answer_contract = final_answer_contract(run_id, outcome, reason);
        let evidence_summary = final_answer_evidence_summary(
            run_id,
            outcome,
            reason,
            self.completed_tool_calls,
            &evidence_checkpoint,
        );
        let verification_finalizer =
            self.verify_before_finish_guard(run_id, reason, user_visible_message.as_str());
        let progress_checkpoint = outcome.continuation_required.then_some(evidence_checkpoint);
        AgentLoopFinalizationEnvelope {
            schema_version: 1,
            termination_reason: reason,
            status: outcome.status.to_owned(),
            lifecycle_state: outcome.lifecycle_state.to_owned(),
            reason_code: outcome.reason_code.to_owned(),
            terminal_outcome,
            partial: outcome.partial,
            continuation_required: outcome.continuation_required,
            user_visible_message,
            usage: self.usage.clone(),
            tool_count: self.completed_tool_calls,
            artifact_refs: Vec::new(),
            final_answer_contract,
            evidence_summary,
            verification_finalizer,
            progress_checkpoint,
            provider_trace_ref,
        }
    }

    /// Evaluates whether finalization should nudge the model toward verification.
    pub(crate) fn verify_before_finish_guard(
        &self,
        run_id: &str,
        reason: AgentLoopTerminationReason,
        final_answer: &str,
    ) -> FinalizationVerificationReport {
        VerifyBeforeFinishGuard::evaluate(VerifyBeforeFinishRequest {
            run_id,
            reason,
            final_answer,
            messages: self.messages.as_slice(),
        })
    }

    /// Infers bounded continuation state from the provider message history.
    pub(crate) fn progress_checkpoint(
        &self,
        run_id: &str,
        reason: AgentLoopTerminationReason,
    ) -> RunProgressCheckpoint {
        build_run_progress_checkpoint(run_id, reason, self.messages.as_slice())
    }

    /// Serializes the model-visible checkpoint and keeps it under the prompt cap.
    pub(crate) fn progress_checkpoint_json(
        &self,
        run_id: &str,
        reason: AgentLoopTerminationReason,
    ) -> String {
        let mut checkpoint = self.progress_checkpoint(run_id, reason);
        checkpoint.truncate_for_model();
        serde_json::to_string(&checkpoint).unwrap_or_else(|_| {
            json!({
                "schema_version": RUN_PROGRESS_CHECKPOINT_SCHEMA_VERSION,
                "run_id": run_id,
                "recommended_next_action": "checkpoint serialization failed; inspect latest run status and continue from the last successful tool evidence",
            })
            .to_string()
        })
    }

    /// Serializes the `agent_loop.started` tape payload.
    ///
    /// Serialization failures degrade to `"{}"` instead of erroring: tape
    /// telemetry must never abort a live run.
    pub(crate) fn start_payload(&self, run_id: &str) -> String {
        serde_json::to_string(&json!({
            "event": "agent_loop.started",
            "started_at_unix_ms": self.started_at_unix_ms,
            "max_model_turns": self.max_model_turns,
            "max_tool_calls": self.max_tool_calls,
            "step_count_limit_active": false,
            "state": self.snapshot(run_id, None),
        }))
        .unwrap_or_else(|_| "{}".to_owned())
    }

    /// Serializes a per-turn tape payload for the given agent-loop event name.
    pub(crate) fn turn_payload(&self, run_id: &str, event: &str) -> String {
        serde_json::to_string(&json!({
            "event": event,
            "state": self.snapshot(run_id, None),
        }))
        .unwrap_or_else(|_| "{}".to_owned())
    }

    /// Serializes the `agent_loop.terminated` tape payload, embedding the
    /// finalization envelope replay consumers rely on.
    pub(crate) fn termination_payload(
        &self,
        run_id: &str,
        reason: AgentLoopTerminationReason,
        user_visible_message: &str,
        provider_trace_ref: Option<String>,
    ) -> String {
        serde_json::to_string(&json!({
            "event": "agent_loop.terminated",
            "termination_reason": reason.as_str(),
            "state": self.snapshot(run_id, Some(reason)),
            "finalization": self.finalization_envelope(
                run_id,
                reason,
                user_visible_message.to_owned(),
                provider_trace_ref,
            ),
        }))
        .unwrap_or_else(|_| "{}".to_owned())
    }

    /// Appends operator cleanup instructions for resources the run may have
    /// leaked (open browser sessions, routines it created), derived from the
    /// tool-call/tool-result pairs in the message history.
    pub(crate) fn message_with_cleanup_guidance(&self, message: &str) -> String {
        let cleanup = self.cleanup_instructions();
        if cleanup.is_empty() {
            return message.to_owned();
        }
        format!(
            "{message} Terminal cleanup will be attempted for run-owned resources. If any resource remains or is detached: {}",
            cleanup.join(" ")
        )
    }

    fn cleanup_instructions(&self) -> Vec<String> {
        let mut cleanup = pending_browser_session_ids(self.messages.as_slice())
            .into_iter()
            .map(|session_id| {
                format!(
                    "browser session {session_id}; close it with `palyra browser session close {session_id} --json` or stop browserd with `palyra browser stop --json` if it remains open."
                )
            })
            .collect::<Vec<_>>();
        cleanup.extend(pending_background_process_cleanup_instructions(self.messages.as_slice()));
        cleanup.extend(pending_routine_cleanup_instructions(self.messages.as_slice()));
        cleanup
    }

    fn elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(self.started_at)
    }
}

fn final_answer_contract(
    run_id: &str,
    outcome: AgentLoopFinalizationOutcome,
    reason: AgentLoopTerminationReason,
) -> FinalAnswerContract {
    let decision = final_answer_decision(outcome);
    FinalAnswerContract {
        schema_version: FINAL_ANSWER_CONTRACT_SCHEMA_VERSION,
        decision,
        reason_code: reason.as_str().to_owned(),
        final_answer_required: true,
        evidence_summary_required: true,
        tool_evidence_required_for_tool_claims: true,
        enforcement_mode: "observe_only".to_owned(),
        journal_projection: FinalAnswerJournalProjection {
            schema_version: FINAL_ANSWER_CONTRACT_SCHEMA_VERSION,
            event_type: final_answer_event_type(decision).to_owned(),
            source_event_refs: vec![format!("tape:{run_id}:agent_loop.terminated")],
            redaction_level: FINAL_ANSWER_CONTRACT_REDACTION_LEVEL.to_owned(),
        },
        event_types: final_answer_event_types(),
        redaction_level: FINAL_ANSWER_CONTRACT_REDACTION_LEVEL.to_owned(),
    }
}

fn terminal_outcome_from_finalization(
    reason: AgentLoopTerminationReason,
    user_visible_message: &str,
) -> TerminalOutcomeClassification {
    match reason {
        AgentLoopTerminationReason::FinalAnswer => TerminalOutcomeClassification::runtime_observed(
            TerminalOutcomeClass::VisibleText,
            "terminal_outcome.visible_text",
            user_visible_message.trim().len(),
            0,
        ),
        AgentLoopTerminationReason::ProviderError => TerminalOutcomeClassification::runtime(
            TerminalOutcomeClass::ProviderError,
            "terminal_outcome.provider_error",
        ),
        AgentLoopTerminationReason::IncompleteFinalAnswer => {
            let normalized = user_visible_message.to_ascii_lowercase();
            let class = if normalized.contains("reasoning-only") {
                TerminalOutcomeClass::ReasoningOnly
            } else if normalized.contains("empty final answer") {
                TerminalOutcomeClass::Empty
            } else if normalized.contains("planning or intent statement") {
                TerminalOutcomeClass::PlanningOnly
            } else {
                TerminalOutcomeClass::ProtocolError
            };
            TerminalOutcomeClassification::runtime(
                class,
                format!("terminal_outcome.{}", class.as_str()),
            )
        }
        AgentLoopTerminationReason::ApprovalDenied
        | AgentLoopTerminationReason::RepeatedToolFailure
        | AgentLoopTerminationReason::ContextBudgetExhausted
        | AgentLoopTerminationReason::BrowserFollowupTimeout
        | AgentLoopTerminationReason::ToolFollowupTimeout
        | AgentLoopTerminationReason::RunLoopPhaseTimeout
        | AgentLoopTerminationReason::WallClock
        | AgentLoopTerminationReason::Cancellation
        | AgentLoopTerminationReason::MaxTurns
        | AgentLoopTerminationReason::MaxToolCalls => TerminalOutcomeClassification::runtime(
            TerminalOutcomeClass::ProtocolError,
            format!("terminal_outcome.{}", reason.as_str()),
        ),
    }
}

fn final_answer_evidence_summary(
    run_id: &str,
    outcome: AgentLoopFinalizationOutcome,
    reason: AgentLoopTerminationReason,
    tool_count: u32,
    checkpoint: &RunProgressCheckpoint,
) -> FinalAnswerEvidenceSummary {
    let decision = final_answer_decision(outcome);
    FinalAnswerEvidenceSummary {
        schema_version: FINAL_ANSWER_CONTRACT_SCHEMA_VERSION,
        run_id: run_id.to_owned(),
        decision,
        coverage: final_answer_evidence_coverage(decision, tool_count, checkpoint),
        reason_code: reason.as_str().to_owned(),
        tool_count,
        produced_files_count: checkpoint.produced_files.len(),
        missing_artifacts_count: checkpoint.missing_artifacts.len(),
        active_process_count: checkpoint.active_processes.len(),
        known_failed_attempt_count: checkpoint.known_failed_attempts.len(),
        last_successful_tool: checkpoint
            .last_successful_tool
            .as_ref()
            .map(|tool| checkpoint_text_id(tool.tool_name.as_str())),
        evidence_refs: final_answer_evidence_refs(run_id, tool_count, checkpoint),
        redaction_level: FINAL_ANSWER_CONTRACT_REDACTION_LEVEL.to_owned(),
    }
}

fn final_answer_decision(outcome: AgentLoopFinalizationOutcome) -> FinalAnswerDecision {
    if outcome.status == "completed" {
        FinalAnswerDecision::Accepted
    } else if outcome.continuation_required {
        FinalAnswerDecision::NeedsContinuation
    } else {
        FinalAnswerDecision::Rejected
    }
}

fn final_answer_evidence_coverage(
    decision: FinalAnswerDecision,
    tool_count: u32,
    checkpoint: &RunProgressCheckpoint,
) -> FinalAnswerEvidenceCoverage {
    if tool_count == 0 {
        return if decision == FinalAnswerDecision::Accepted {
            FinalAnswerEvidenceCoverage::NotRequired
        } else {
            FinalAnswerEvidenceCoverage::NoToolEvidence
        };
    }
    if !checkpoint.missing_artifacts.is_empty() || !checkpoint.active_processes.is_empty() {
        FinalAnswerEvidenceCoverage::GapsDetected
    } else {
        FinalAnswerEvidenceCoverage::Satisfied
    }
}

fn final_answer_event_type(decision: FinalAnswerDecision) -> &'static str {
    match decision {
        FinalAnswerDecision::Accepted => FINAL_ANSWER_CONTRACT_COMPLETED_EVENT,
        FinalAnswerDecision::NeedsContinuation | FinalAnswerDecision::Rejected => {
            FINAL_ANSWER_CONTRACT_FAILED_EVENT
        }
    }
}

fn final_answer_event_types() -> Vec<String> {
    [
        FINAL_ANSWER_CONTRACT_STARTED_EVENT,
        FINAL_ANSWER_CONTRACT_COMPLETED_EVENT,
        FINAL_ANSWER_CONTRACT_FAILED_EVENT,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn final_answer_evidence_refs(
    run_id: &str,
    tool_count: u32,
    checkpoint: &RunProgressCheckpoint,
) -> Vec<String> {
    let mut refs = BTreeSet::<String>::new();
    refs.insert(format!("tape:{run_id}:agent_loop.terminated"));
    if tool_count > 0 {
        refs.insert(format!("tape:{run_id}:tool_results"));
    }
    if let Some(tool) = checkpoint.last_successful_tool.as_ref() {
        refs.insert(format!("tool:{}", checkpoint_text_id(tool.tool_name.as_str())));
        for artifact_ref in &tool.artifact_refs {
            refs.insert(format!("artifact:{}", checkpoint_text_id(artifact_ref.as_str())));
        }
    }
    for file in &checkpoint.produced_files {
        refs.insert(format!("file:{}", checkpoint_path(file.path.as_str())));
    }
    for artifact in &checkpoint.missing_artifacts {
        refs.insert(format!("missing_artifact:{}", checkpoint_path(artifact.path.as_str())));
    }
    for process in &checkpoint.active_processes {
        refs.insert(format!("process:{}", process.pid));
    }
    for index in 0..checkpoint.known_failed_attempts.len() {
        refs.insert(format!("tool_failure:{}", index + 1));
    }
    refs.into_iter().take(32).collect()
}

impl VerifyBeforeFinishGuard {
    fn evaluate(request: VerifyBeforeFinishRequest<'_>) -> FinalizationVerificationReport {
        let activity = verification_activity_from_messages(request.run_id, request.messages);
        if request.reason != AgentLoopTerminationReason::FinalAnswer {
            return verification_finalizer_report(
                FinalizationVerificationStatus::NotRequired,
                "verification.finalizer.not_final_answer",
                activity,
                None,
            );
        }
        if !activity.code_mutation_seen {
            return verification_finalizer_report(
                FinalizationVerificationStatus::NotRequired,
                "verification.finalizer.no_code_mutation",
                activity,
                None,
            );
        }
        if activity.pending_requirements.is_empty() {
            return verification_finalizer_report(
                FinalizationVerificationStatus::Verified,
                "verification.finalizer.fresh_verification_found",
                activity,
                None,
            );
        }
        if let Some(reason) = final_answer_unverified_reason(request.final_answer) {
            return verification_finalizer_report(
                FinalizationVerificationStatus::UnverifiedAllowed,
                "verification.finalizer.unverified_allowed",
                activity,
                Some(reason),
            );
        }
        verification_finalizer_report(
            FinalizationVerificationStatus::NudgeRequired,
            "verification.finalizer.stale_after_code_mutation",
            activity,
            None,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingFinalizationVerificationRequirement {
    requirement_id: String,
    required_kind: VerificationKind,
    changed_paths: Vec<String>,
}

#[derive(Debug, Default)]
struct FinalizationVerificationActivity {
    code_mutation_seen: bool,
    pending_requirements: BTreeMap<String, PendingFinalizationVerificationRequirement>,
    satisfied_requirements: BTreeMap<String, PendingFinalizationVerificationRequirement>,
    evidence_refs: BTreeSet<String>,
}

fn verification_finalizer_report(
    status: FinalizationVerificationStatus,
    reason_code: &str,
    activity: FinalizationVerificationActivity,
    unverified_reason: Option<String>,
) -> FinalizationVerificationReport {
    let pending_requirements = requirement_summaries(activity.pending_requirements.values());
    let satisfied_requirement_count = activity.satisfied_requirements.len();
    let event_type = match status {
        FinalizationVerificationStatus::NudgeRequired => {
            Some(VERIFICATION_FINALIZER_NUDGE_EVENT.to_owned())
        }
        FinalizationVerificationStatus::UnverifiedAllowed => {
            Some(VERIFICATION_FINALIZER_UNVERIFIED_ALLOWED_EVENT.to_owned())
        }
        FinalizationVerificationStatus::NotRequired | FinalizationVerificationStatus::Verified => {
            None
        }
    };
    let nudge = (status == FinalizationVerificationStatus::NudgeRequired)
        .then(|| verification_finalizer_nudge_text(pending_requirements.as_slice()));
    FinalizationVerificationReport {
        schema_version: FINAL_ANSWER_CONTRACT_SCHEMA_VERSION,
        status,
        reason_code: reason_code.to_owned(),
        enforcement_mode: "soft_nudge".to_owned(),
        surface_policy: "default_coding_session_soft_guard".to_owned(),
        code_mutation_seen: activity.code_mutation_seen,
        pending_requirement_count: pending_requirements.len(),
        satisfied_requirement_count,
        pending_requirements,
        evidence_refs: activity.evidence_refs.into_iter().take(32).collect(),
        event_type,
        nudge,
        unverified_reason,
        redaction_level: FINAL_ANSWER_CONTRACT_REDACTION_LEVEL.to_owned(),
    }
}

fn verification_activity_from_messages(
    run_id: &str,
    messages: &[ProviderMessage],
) -> FinalizationVerificationActivity {
    let mut activity = FinalizationVerificationActivity::default();
    let mut tool_calls_by_id = BTreeMap::<String, ProviderMessageToolCallRef>::new();

    for message in messages {
        for tool_call in &message.tool_calls {
            tool_calls_by_id.insert(
                tool_call.proposal_id.clone(),
                ProviderMessageToolCallRef {
                    tool_name: tool_call.tool_name.clone(),
                    input_json: tool_call.input_json.clone(),
                },
            );
        }

        if message.role != ProviderMessageRole::Tool {
            continue;
        }
        let Some(tool_call_id) = message.tool_call_id.as_deref() else {
            continue;
        };
        let Some(tool_call) = tool_calls_by_id.get(tool_call_id) else {
            continue;
        };
        let Ok(raw_output) = serde_json::from_str::<Value>(message.text_content().as_str()) else {
            continue;
        };
        let output = model_visible_tool_result_payload(&raw_output);
        if tool_call.tool_name == WORKSPACE_PATCH_TOOL_NAME
            && model_visible_tool_result_succeeded(&raw_output)
        {
            observe_workspace_patch_verification_requirements(
                run_id,
                tool_call_id,
                output,
                &mut activity,
            );
            continue;
        }
        if tool_call.tool_name == PROCESS_RUN_TOOL_NAME
            && process_run_verification_result_passed(&raw_output, output)
        {
            observe_process_run_verification(tool_call, tool_call_id, &mut activity);
        }
    }

    activity
}

fn observe_workspace_patch_verification_requirements(
    run_id: &str,
    tool_call_id: &str,
    output: &Value,
    activity: &mut FinalizationVerificationActivity,
) {
    if output.get("dry_run").and_then(Value::as_bool) == Some(true) {
        return;
    }
    if output.get("files_touched").and_then(Value::as_array).is_some_and(|files| {
        files.iter().any(|file| {
            !matches!(file.get("operation").and_then(Value::as_str).unwrap_or_default(), "no_op")
        })
    }) {
        activity.code_mutation_seen = true;
    }
    for requirement in stale_requirements_from_patch_output(output) {
        activity.code_mutation_seen = true;
        activity.evidence_refs.insert(format!("tape:{run_id}:tool_result:{tool_call_id}"));
        activity.pending_requirements.insert(requirement_key(&requirement), requirement);
    }
}

fn observe_process_run_verification(
    tool_call: &ProviderMessageToolCallRef,
    tool_call_id: &str,
    activity: &mut FinalizationVerificationActivity,
) {
    let Ok(input_json) = serde_json::to_vec(&tool_call.input_json) else {
        return;
    };
    let Ok(input) = parse_process_runner_tool_input(input_json.as_slice()) else {
        return;
    };
    let classification = VerificationCommandClassifier::classify_process_run(&input);
    if !classification.is_verification {
        return;
    }
    let matching = activity
        .pending_requirements
        .iter()
        .filter(|(_, requirement)| requirement.required_kind == classification.kind)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in matching {
        if let Some(requirement) = activity.pending_requirements.remove(key.as_str()) {
            activity.evidence_refs.insert(format!("tool_call:{tool_call_id}"));
            activity.satisfied_requirements.insert(key, requirement);
        }
    }
}

fn stale_requirements_from_patch_output(
    output: &Value,
) -> Vec<PendingFinalizationVerificationRequirement> {
    let Some(requirements) =
        output.pointer("/coding_posture/verification/requirements").and_then(Value::as_array)
    else {
        return Vec::new();
    };
    requirements
        .iter()
        .filter_map(|state| {
            let requirement = state.get("requirement")?;
            let required_kind = requirement
                .get("required_kind")
                .and_then(Value::as_str)
                .and_then(verification_kind_from_str)?;
            Some(PendingFinalizationVerificationRequirement {
                requirement_id: requirement
                    .get("requirement_id")
                    .and_then(Value::as_str)
                    .map(checkpoint_text_id)
                    .unwrap_or_else(|| {
                        format!("verification.required_after_patch.{}", required_kind.as_str())
                    }),
                required_kind,
                changed_paths: requirement
                    .get("changed_paths")
                    .and_then(Value::as_array)
                    .map(|paths| {
                        paths
                            .iter()
                            .filter_map(Value::as_str)
                            .map(checkpoint_path)
                            .take(12)
                            .collect()
                    })
                    .unwrap_or_default(),
            })
        })
        .collect()
}

fn process_run_verification_result_passed(raw_output: &Value, output: &Value) -> bool {
    model_visible_tool_result_succeeded(raw_output)
        && output.get("exit_code").and_then(Value::as_i64).is_none_or(|code| code == 0)
}

fn verification_kind_from_str(value: &str) -> Option<VerificationKind> {
    match value {
        "build" => Some(VerificationKind::Build),
        "check" => Some(VerificationKind::Check),
        "format" => Some(VerificationKind::Format),
        "lint" => Some(VerificationKind::Lint),
        "test" => Some(VerificationKind::Test),
        "typecheck" => Some(VerificationKind::Typecheck),
        _ => None,
    }
}

fn requirement_key(requirement: &PendingFinalizationVerificationRequirement) -> String {
    format!("{}:{}", requirement.required_kind.as_str(), requirement.requirement_id)
}

fn requirement_summaries<'a>(
    requirements: impl Iterator<Item = &'a PendingFinalizationVerificationRequirement>,
) -> Vec<FinalizationVerificationRequirementSummary> {
    requirements
        .take(16)
        .map(|requirement| FinalizationVerificationRequirementSummary {
            requirement_id: requirement.requirement_id.clone(),
            required_kind: requirement.required_kind.as_str().to_owned(),
            changed_paths: requirement.changed_paths.iter().take(12).cloned().collect(),
        })
        .collect()
}

fn verification_finalizer_nudge_text(
    pending_requirements: &[FinalizationVerificationRequirementSummary],
) -> String {
    let kinds = pending_requirements
        .iter()
        .map(|requirement| requirement.required_kind.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "Verification is stale after code changes. Run a matching verification command for: {kinds}. If verification cannot be run, final answer must explicitly include verification_status=unverified and the reason."
    )
}

fn final_answer_unverified_reason(text: &str) -> Option<String> {
    let normalized = normalize_finalizer_text(text);
    let explicit = normalized.contains("verification_status=unverified")
        || normalized.contains("verification status: unverified")
        || normalized.contains("verification: unverified")
        || normalized.contains("unverified because")
        || normalized.contains("could not verify")
        || normalized.contains("unable to verify")
        || normalized.contains("tests not run");
    explicit.then(|| checkpoint_text(text, 320))
}

fn normalize_finalizer_text(text: &str) -> String {
    text.to_ascii_lowercase().split_whitespace().collect::<Vec<_>>().join(" ")
}

impl RunProgressCheckpoint {
    fn truncate_for_model(&mut self) {
        truncate_vec(&mut self.produced_files, RUN_PROGRESS_MAX_PRODUCED_FILES);
        truncate_vec(&mut self.missing_artifacts, RUN_PROGRESS_MAX_MISSING_ARTIFACTS);
        truncate_vec(&mut self.active_processes, RUN_PROGRESS_MAX_ACTIVE_PROCESSES);
        truncate_vec(&mut self.known_failed_attempts, RUN_PROGRESS_MAX_FAILED_ATTEMPTS);
        if let Some(last_successful_tool) = self.last_successful_tool.as_mut() {
            last_successful_tool.summary =
                checkpoint_text(last_successful_tool.summary.as_str(), 320);
            truncate_vec(&mut last_successful_tool.artifact_refs, 8);
        }
        self.task_goal_summary = checkpoint_text(self.task_goal_summary.as_str(), 320);
        self.recommended_next_action = checkpoint_text(self.recommended_next_action.as_str(), 480);

        while serde_json::to_vec(self).map_or(usize::MAX, |bytes| bytes.len())
            > RUN_PROGRESS_CHECKPOINT_MAX_BYTES
        {
            if self.produced_files.pop().is_some()
                || self.known_failed_attempts.pop().is_some()
                || self.active_processes.pop().is_some()
                || self.missing_artifacts.pop().is_some()
            {
                continue;
            }
            self.task_goal_summary =
                "continue the existing user request from session context".to_owned();
            self.recommended_next_action =
                "inspect the last successful tool evidence and continue the same session"
                    .to_owned();
            break;
        }
    }
}

fn truncate_vec<T>(items: &mut Vec<T>, max_len: usize) {
    if items.len() > max_len {
        items.truncate(max_len);
    }
}

fn build_run_progress_checkpoint(
    run_id: &str,
    reason: AgentLoopTerminationReason,
    messages: &[ProviderMessage],
) -> RunProgressCheckpoint {
    let mut tool_calls_by_id = BTreeMap::<String, ProviderMessageToolCallRef>::new();
    let mut produced_files_by_path = BTreeMap::<String, RunProgressFileSummary>::new();
    let mut satisfied_file_paths = BTreeSet::<String>::new();
    let mut process_by_pid = BTreeMap::<u32, RunProgressProcessSummary>::new();
    let mut known_failed_attempts = Vec::<String>::new();
    let mut last_successful_tool = None::<RunProgressToolSummary>;

    for message in messages {
        for tool_call in &message.tool_calls {
            tool_calls_by_id.insert(
                tool_call.proposal_id.clone(),
                ProviderMessageToolCallRef {
                    tool_name: tool_call.tool_name.clone(),
                    input_json: tool_call.input_json.clone(),
                },
            );
        }

        if message.role != ProviderMessageRole::Tool {
            continue;
        }
        let Some(tool_call_id) = message.tool_call_id.as_deref() else {
            continue;
        };
        let Some(tool_call) = tool_calls_by_id.get(tool_call_id) else {
            continue;
        };
        let Ok(raw_output) = serde_json::from_str::<Value>(message.text_content().as_str()) else {
            continue;
        };
        let output = model_visible_tool_result_payload(&raw_output);

        if model_visible_tool_result_succeeded(&raw_output) {
            collect_produced_files(
                tool_call.tool_name.as_str(),
                output,
                &mut produced_files_by_path,
            );
            collect_satisfied_file_evidence(
                tool_call.tool_name.as_str(),
                output,
                &mut satisfied_file_paths,
            );
            collect_process_progress(tool_call.tool_name.as_str(), output, &mut process_by_pid);
            last_successful_tool = Some(tool_success_summary(tool_call.tool_name.as_str(), output));
        } else if known_failed_attempts.len() < RUN_PROGRESS_MAX_FAILED_ATTEMPTS {
            known_failed_attempts
                .push(tool_failure_summary(tool_call.tool_name.as_str(), &raw_output));
        }
    }

    let produced_files = produced_files_by_path.into_values().collect::<Vec<_>>();
    for file in &produced_files {
        insert_satisfied_path(file.path.as_str(), &mut satisfied_file_paths);
    }
    let missing_artifacts = missing_artifacts_from_messages(messages, &satisfied_file_paths);
    RunProgressCheckpoint {
        schema_version: RUN_PROGRESS_CHECKPOINT_SCHEMA_VERSION,
        run_id: run_id.to_owned(),
        task_goal_summary: checkpoint_task_goal_summary(messages),
        last_successful_tool,
        recommended_next_action: checkpoint_next_action(
            reason,
            missing_artifacts.as_slice(),
            process_by_pid.values(),
        ),
        produced_files,
        missing_artifacts,
        active_processes: process_by_pid.into_values().collect(),
        known_failed_attempts,
    }
}

#[derive(Debug, Clone)]
struct ProviderMessageToolCallRef {
    tool_name: String,
    input_json: Value,
}

fn model_visible_tool_result_payload(output: &Value) -> &Value {
    output
        .get("output")
        .filter(|_| output.get("tool_name").and_then(Value::as_str).is_some())
        .unwrap_or(output)
}

fn model_visible_tool_result_succeeded(output: &Value) -> bool {
    output.get("success").and_then(Value::as_bool) != Some(false)
}

fn collect_produced_files(
    tool_name: &str,
    output: &Value,
    produced_files_by_path: &mut BTreeMap<String, RunProgressFileSummary>,
) {
    match tool_name {
        WORKSPACE_PATCH_TOOL_NAME => collect_workspace_patch_files(output, produced_files_by_path),
        OS_FILE_TOOL_NAME => collect_os_file_artifacts(output, produced_files_by_path),
        _ => {}
    }
}

fn collect_workspace_patch_files(
    output: &Value,
    produced_files_by_path: &mut BTreeMap<String, RunProgressFileSummary>,
) {
    if output.get("dry_run").and_then(Value::as_bool) == Some(true) {
        return;
    }
    let Some(files) = output.get("files_touched").and_then(Value::as_array) else {
        return;
    };
    for file in files {
        let Some(path) = file.get("path").and_then(Value::as_str).map(checkpoint_path) else {
            continue;
        };
        if path.is_empty() {
            continue;
        }
        let operation =
            file.get("operation").and_then(Value::as_str).unwrap_or("update").to_owned();
        if matches!(operation.as_str(), "delete" | "no_op") {
            continue;
        }
        produced_files_by_path.insert(
            path.clone(),
            RunProgressFileSummary {
                path,
                root_label: "app".to_owned(),
                status: "exists".to_owned(),
                operation,
                sha256: file.get("after_sha256").and_then(Value::as_str).map(checkpoint_sha),
                size_bytes: file.get("after_size_bytes").and_then(Value::as_u64),
            },
        );
    }
}

fn collect_os_file_artifacts(
    output: &Value,
    produced_files_by_path: &mut BTreeMap<String, RunProgressFileSummary>,
) {
    if output.get("dry_run").and_then(Value::as_bool) == Some(true) {
        return;
    }
    let operation = output.get("operation").and_then(Value::as_str).unwrap_or_default();
    let path_key = match operation {
        "write" | "mkdir" => "path",
        "copy" | "move" => "target_path",
        _ => return,
    };
    let Some(path) = output.get(path_key).and_then(Value::as_str).map(checkpoint_path) else {
        return;
    };
    if path.is_empty() {
        return;
    }
    produced_files_by_path.insert(
        path.clone(),
        RunProgressFileSummary {
            path,
            root_label: "app".to_owned(),
            status: "exists".to_owned(),
            operation: operation.to_owned(),
            sha256: output.get("content_sha256").and_then(Value::as_str).map(checkpoint_sha),
            size_bytes: output
                .get("bytes_written")
                .and_then(Value::as_u64)
                .or_else(|| output.get("size_bytes").and_then(Value::as_u64)),
        },
    );
}

fn collect_satisfied_file_evidence(
    tool_name: &str,
    output: &Value,
    satisfied_file_paths: &mut BTreeSet<String>,
) {
    match tool_name {
        WORKSPACE_READ_FILE_TOOL_NAME => {
            collect_output_path(output, "path", satisfied_file_paths);
        }
        WORKSPACE_LIST_DIR_TOOL_NAME => {
            collect_output_path(output, "path", satisfied_file_paths);
            collect_array_item_paths(output, "entries", satisfied_file_paths);
        }
        WORKSPACE_SEARCH_TOOL_NAME => {
            collect_output_path(output, "path", satisfied_file_paths);
            collect_array_item_paths(output, "matches", satisfied_file_paths);
        }
        OS_FILE_TOOL_NAME => collect_os_file_satisfied_paths(output, satisfied_file_paths),
        _ => {}
    }
}

fn collect_os_file_satisfied_paths(output: &Value, satisfied_file_paths: &mut BTreeSet<String>) {
    match output.get("operation").and_then(Value::as_str).unwrap_or_default() {
        "read" | "stat" => {
            collect_output_path(output, "path", satisfied_file_paths);
            collect_output_path(output, "resolved_path", satisfied_file_paths);
        }
        "list_dir" => {
            collect_output_path(output, "path", satisfied_file_paths);
            collect_output_path(output, "resolved_path", satisfied_file_paths);
            collect_array_item_paths(output, "entries", satisfied_file_paths);
        }
        "search" => {
            collect_output_path(output, "path", satisfied_file_paths);
            collect_output_path(output, "resolved_path", satisfied_file_paths);
            collect_array_item_paths(output, "matches", satisfied_file_paths);
        }
        _ => {}
    }
}

fn collect_array_item_paths(
    output: &Value,
    array_key: &str,
    satisfied_file_paths: &mut BTreeSet<String>,
) {
    let Some(items) = output.get(array_key).and_then(Value::as_array) else {
        return;
    };
    for item in items {
        collect_output_path(item, "path", satisfied_file_paths);
        collect_output_path(item, "resolved_path", satisfied_file_paths);
    }
}

fn collect_output_path(output: &Value, key: &str, satisfied_file_paths: &mut BTreeSet<String>) {
    if let Some(path) = output.get(key).and_then(Value::as_str) {
        insert_satisfied_path(path, satisfied_file_paths);
    }
}

fn insert_satisfied_path(path: &str, satisfied_file_paths: &mut BTreeSet<String>) {
    if let Some(path) = normalized_checkpoint_path_key(path) {
        satisfied_file_paths.insert(path);
    }
}

fn collect_process_progress(
    tool_name: &str,
    output: &Value,
    process_by_pid: &mut BTreeMap<u32, RunProgressProcessSummary>,
) {
    match tool_name {
        PROCESS_RUN_TOOL_NAME => {
            if output.get("background").and_then(Value::as_bool) != Some(true) {
                return;
            }
            let Some(pid) = process_pid(output) else {
                return;
            };
            let (status, cleanup) = if process_is_detached_handoff(output) {
                (
                    "detached_background_started",
                    "detached_background_process_not_stopped_by_terminal_cleanup_use_cleanup_portable_stop_command",
                )
            } else {
                (
                    "run_owned_background_started",
                    "terminal_run_cleanup_will_stop_process_if_still_running",
                )
            };
            process_by_pid.insert(
                pid,
                RunProgressProcessSummary {
                    pid,
                    kind: "background".to_owned(),
                    status: status.to_owned(),
                    cleanup: cleanup.to_owned(),
                    log_artifact: output
                        .get("log_artifact")
                        .and_then(Value::as_str)
                        .map(checkpoint_text_id),
                },
            );
        }
        PROCESS_STATUS_TOOL_NAME => {
            let Some(pid) = process_pid(output) else {
                return;
            };
            let status = match output.get("alive").and_then(Value::as_bool) {
                Some(true) => "running",
                Some(false) => "stopped",
                None => "status_unknown",
            };
            process_by_pid
                .entry(pid)
                .and_modify(|process| process.status = status.to_owned())
                .or_insert_with(|| RunProgressProcessSummary {
                    pid,
                    kind: "background".to_owned(),
                    status: status.to_owned(),
                    cleanup: "inspect_process_status_before_claiming_service_lifetime".to_owned(),
                    log_artifact: None,
                });
        }
        PROCESS_STOP_TOOL_NAME => {
            let Some(pid) = process_pid(output) else {
                return;
            };
            let status = if output.get("stopped").and_then(Value::as_bool) == Some(true) {
                "stopped"
            } else {
                "stop_attempted_status_unknown"
            };
            process_by_pid
                .entry(pid)
                .and_modify(|process| process.status = status.to_owned())
                .or_insert_with(|| RunProgressProcessSummary {
                    pid,
                    kind: "background".to_owned(),
                    status: status.to_owned(),
                    cleanup: "stop_command_already_attempted".to_owned(),
                    log_artifact: None,
                });
        }
        _ => {}
    }
}

fn process_pid(output: &Value) -> Option<u32> {
    let pid = output
        .pointer("/process_handle/direct_process_pid")
        .and_then(Value::as_u64)
        .or_else(|| output.get("pid").and_then(Value::as_u64))?;
    u32::try_from(pid).ok().filter(|pid| *pid > 0)
}

fn process_is_detached_handoff(output: &Value) -> bool {
    output.get("durable_handoff").and_then(Value::as_bool) == Some(true)
        || output.get("run_owned_lifetime").and_then(Value::as_bool) == Some(false)
        || output.get("lifetime_mode").and_then(Value::as_str) == Some("detached")
}

fn tool_success_summary(tool_name: &str, output: &Value) -> RunProgressToolSummary {
    RunProgressToolSummary {
        tool_name: tool_name.to_owned(),
        summary: checkpoint_text(tool_success_summary_text(tool_name, output).as_str(), 384),
        artifact_refs: artifact_refs_from_tool_output(output),
    }
}

fn tool_success_summary_text(tool_name: &str, output: &Value) -> String {
    match tool_name {
        WORKSPACE_PATCH_TOOL_NAME => {
            let files = output
                .get("files_touched")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.get("path").and_then(Value::as_str))
                        .take(6)
                        .map(checkpoint_path)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if files.is_empty() {
                "workspace patch completed".to_owned()
            } else {
                format!("workspace patch updated {}", files.join(", "))
            }
        }
        OS_FILE_TOOL_NAME => {
            let operation = output.get("operation").and_then(Value::as_str).unwrap_or("operation");
            let path = output
                .get("target_path")
                .or_else(|| output.get("path"))
                .and_then(Value::as_str)
                .map(checkpoint_path)
                .unwrap_or_else(|| "unknown path".to_owned());
            format!("os_file {operation} completed for {path}")
        }
        PROCESS_RUN_TOOL_NAME => {
            if output.get("background").and_then(Value::as_bool) == Some(true) {
                process_pid(output).map_or_else(
                    || "background process started with unknown pid".to_owned(),
                    |pid| format!("run-owned background process pid={pid} started"),
                )
            } else {
                let exit_code = output.get("exit_code").and_then(Value::as_i64).unwrap_or(0);
                format!("process completed with exit_code={exit_code}")
            }
        }
        PROCESS_STATUS_TOOL_NAME | PROCESS_STOP_TOOL_NAME => {
            let pid =
                process_pid(output).map_or_else(|| "unknown".to_owned(), |pid| pid.to_string());
            format!("{tool_name} completed for pid={pid}")
        }
        _ => format!("{tool_name} completed successfully"),
    }
}

fn artifact_refs_from_tool_output(output: &Value) -> Vec<String> {
    let mut refs = BTreeSet::<String>::new();
    collect_artifact_refs(output, &mut refs);
    refs.into_iter().take(8).collect()
}

fn collect_artifact_refs(value: &Value, refs: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if matches!(
                    key.as_str(),
                    "artifact_id"
                        | "checkpoint_id"
                        | "preflight_checkpoint_id"
                        | "post_change_checkpoint_id"
                        | "mutation_id"
                ) {
                    if let Some(raw) = value.as_str() {
                        refs.insert(checkpoint_text_id(raw));
                    }
                }
                collect_artifact_refs(value, refs);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_artifact_refs(item, refs);
            }
        }
        _ => {}
    }
}

fn tool_failure_summary(tool_name: &str, output: &Value) -> String {
    let error = output
        .get("error")
        .and_then(Value::as_str)
        .or_else(|| output.get("message").and_then(Value::as_str))
        .unwrap_or("tool failed without a structured error");
    checkpoint_text(format!("{tool_name} failed: {error}").as_str(), 512)
}

fn missing_artifacts_from_messages(
    messages: &[ProviderMessage],
    satisfied_file_paths: &BTreeSet<String>,
) -> Vec<RunProgressMissingArtifact> {
    let mut candidates = BTreeSet::<String>::new();
    for message in messages.iter().filter(|message| message.role == ProviderMessageRole::User) {
        for path in file_artifact_tokens(message.text_content().as_str()) {
            if !artifact_path_satisfied(path.as_str(), satisfied_file_paths) {
                candidates.insert(path);
            }
        }
    }
    candidates
        .into_iter()
        .take(RUN_PROGRESS_MAX_MISSING_ARTIFACTS)
        .map(|path| RunProgressMissingArtifact {
            path,
            root_label: "app".to_owned(),
            required_by: "task_prompt".to_owned(),
        })
        .collect()
}

fn artifact_path_satisfied(path: &str, satisfied_file_paths: &BTreeSet<String>) -> bool {
    let Some(candidate) = normalized_checkpoint_path_key(path) else {
        return false;
    };
    let candidate_suffix = format!("/{candidate}");
    satisfied_file_paths.iter().any(|satisfied| {
        satisfied == &candidate
            || satisfied.ends_with(candidate_suffix.as_str())
            || candidate.ends_with(format!("/{satisfied}").as_str())
    })
}

fn file_artifact_tokens(text: &str) -> Vec<String> {
    let mut tokens = Vec::<String>::new();
    let mut token = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '/' | '\\') {
            token.push(ch);
        } else {
            push_file_artifact_token(token.as_str(), &mut tokens);
            token.clear();
        }
    }
    push_file_artifact_token(token.as_str(), &mut tokens);
    tokens
}

fn push_file_artifact_token(raw: &str, tokens: &mut Vec<String>) {
    let token =
        raw.trim_matches(['.', ',', ';', ':', ')', ']', '}', '"', '\'', '`']).replace('\\', "/");
    if token.is_empty()
        || token.contains("://")
        || token.to_ascii_lowercase().contains(".palyra")
        || !file_artifact_extension_allowed(token.as_str())
    {
        return;
    }
    let normalized = token.trim_start_matches("./").to_owned();
    if !tokens.contains(&normalized) {
        tokens.push(normalized);
    }
}

fn file_artifact_extension_allowed(path: &str) -> bool {
    let Some(extension) = path.rsplit('.').next() else {
        return false;
    };
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "css"
            | "csv"
            | "html"
            | "js"
            | "json"
            | "jsx"
            | "md"
            | "py"
            | "rs"
            | "toml"
            | "ts"
            | "tsx"
            | "txt"
            | "yaml"
            | "yml"
    )
}

fn checkpoint_task_goal_summary(messages: &[ProviderMessage]) -> String {
    let produced_files = messages
        .iter()
        .rev()
        .find(|message| message.role == ProviderMessageRole::User)
        .map(|message| file_artifact_tokens(message.text_content().as_str()))
        .unwrap_or_default();
    if produced_files.is_empty() {
        return "continue the existing user request from session context".to_owned();
    }
    checkpoint_text(format!("continue task involving {}", produced_files.join(", ")).as_str(), 320)
}

fn checkpoint_next_action<'a>(
    reason: AgentLoopTerminationReason,
    missing_artifacts: &[RunProgressMissingArtifact],
    processes: impl Iterator<Item = &'a RunProgressProcessSummary>,
) -> String {
    if let Some(first_missing) = missing_artifacts.first() {
        return checkpoint_text(
            format!("create or verify missing artifact {} before final answer", first_missing.path)
                .as_str(),
            480,
        );
    }
    let process_count = processes.count();
    if process_count > 0 && reason == AgentLoopTerminationReason::WallClock {
        return "verify run-owned background process state and continue validation before claiming the service is still running".to_owned();
    }
    "verify the latest successful tool evidence and produce a final answer if requested artifacts are complete".to_owned()
}

fn checkpoint_path(path: &str) -> String {
    checkpoint_text(path.replace('\\', "/").as_str(), 260)
}

fn normalized_checkpoint_path_key(path: &str) -> Option<String> {
    let normalized =
        checkpoint_path(path).trim_start_matches("./").trim_matches('/').to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}

fn checkpoint_sha(value: &str) -> String {
    checkpoint_text(value, 128)
}

fn checkpoint_text_id(value: &str) -> String {
    checkpoint_text(value, 160)
}

fn checkpoint_text(value: &str, max_chars: usize) -> String {
    let normalized = value.trim().replace(['\r', '\n'], " ");
    let redacted = palyra_common::redaction::redact_internal_runtime_paths(normalized.as_str());
    if redacted.chars().count() > max_chars {
        let limit = max_chars.saturating_sub(3);
        let mut output = redacted.chars().take(limit).collect::<String>();
        output.push_str("...");
        output
    } else {
        redacted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AgentLoopFinalizationOutcome {
    status: &'static str,
    lifecycle_state: &'static str,
    reason_code: &'static str,
    partial: bool,
    continuation_required: bool,
}

impl AgentLoopFinalizationOutcome {
    const fn completed(reason: AgentLoopTerminationReason) -> Self {
        Self {
            status: "completed",
            lifecycle_state: "done",
            reason_code: reason.as_str(),
            partial: false,
            continuation_required: false,
        }
    }

    const fn failed(reason: AgentLoopTerminationReason) -> Self {
        Self {
            status: "failed",
            lifecycle_state: "failed",
            reason_code: reason.as_str(),
            partial: false,
            continuation_required: false,
        }
    }

    const fn needs_continuation(reason: AgentLoopTerminationReason) -> Self {
        Self {
            status: "needs_continuation",
            lifecycle_state: "needs_continuation",
            reason_code: reason.as_str(),
            partial: true,
            continuation_required: true,
        }
    }
}

#[derive(Debug, Clone)]
struct BrowserToolCallRef {
    tool_name: String,
    input_json: Value,
}

#[derive(Debug, Clone)]
struct RoutineToolCallRef {
    tool_name: String,
    input_json: Value,
}

#[derive(Debug, Clone)]
struct BackgroundProcessCleanupCandidate {
    pid: u32,
    stop_command: Option<String>,
    status_command: Option<String>,
    start_context: Option<String>,
}

// Replays browser session create/close tool pairs from the message history to
// find sessions the run opened but never confirmably closed. BTree containers
// keep the cleanup guidance deterministically ordered.
fn pending_browser_session_ids(messages: &[ProviderMessage]) -> BTreeSet<String> {
    let mut tool_calls_by_id = BTreeMap::<String, BrowserToolCallRef>::new();
    let mut open_session_ids = BTreeSet::<String>::new();

    for message in messages {
        for tool_call in &message.tool_calls {
            if matches!(
                tool_call.tool_name.as_str(),
                BROWSER_SESSION_CREATE_TOOL_NAME | BROWSER_SESSION_CLOSE_TOOL_NAME
            ) {
                tool_calls_by_id.insert(
                    tool_call.proposal_id.clone(),
                    BrowserToolCallRef {
                        tool_name: tool_call.tool_name.clone(),
                        input_json: tool_call.input_json.clone(),
                    },
                );
            }
        }

        if message.role != crate::model_provider::ProviderMessageRole::Tool {
            continue;
        }
        let Some(tool_call_id) = message.tool_call_id.as_deref() else {
            continue;
        };
        let Some(tool_call) = tool_calls_by_id.get(tool_call_id) else {
            continue;
        };
        let Ok(output) = serde_json::from_str::<Value>(message.text_content().as_str()) else {
            continue;
        };

        match tool_call.tool_name.as_str() {
            BROWSER_SESSION_CREATE_TOOL_NAME => {
                if let Some(session_id) = output.get("session_id").and_then(Value::as_str) {
                    open_session_ids.insert(session_id.to_owned());
                }
            }
            BROWSER_SESSION_CLOSE_TOOL_NAME if browser_session_close_confirmed(&output) => {
                if let Some(session_id) =
                    tool_call.input_json.get("session_id").and_then(Value::as_str)
                {
                    open_session_ids.remove(session_id);
                }
            }
            _ => {}
        }
    }

    open_session_ids
}

fn pending_background_process_cleanup_instructions(messages: &[ProviderMessage]) -> Vec<String> {
    let mut process_tool_calls_by_id = BTreeMap::<String, String>::new();
    let mut candidates_by_pid = BTreeMap::<u32, BackgroundProcessCleanupCandidate>::new();

    for message in messages {
        for tool_call in &message.tool_calls {
            if matches!(
                tool_call.tool_name.as_str(),
                PROCESS_RUN_TOOL_NAME | PROCESS_STATUS_TOOL_NAME | PROCESS_STOP_TOOL_NAME
            ) {
                process_tool_calls_by_id
                    .insert(tool_call.proposal_id.clone(), tool_call.tool_name.clone());
            }
        }

        if message.role != crate::model_provider::ProviderMessageRole::Tool {
            continue;
        }
        let Some(tool_call_id) = message.tool_call_id.as_deref() else {
            continue;
        };
        let Some(tool_name) = process_tool_calls_by_id.get(tool_call_id) else {
            continue;
        };
        let Ok(raw_output) = serde_json::from_str::<Value>(message.text_content().as_str()) else {
            continue;
        };
        let output = model_visible_tool_result_payload(&raw_output);

        match tool_name.as_str() {
            PROCESS_RUN_TOOL_NAME => {
                if output.get("background").and_then(Value::as_bool) != Some(true)
                    || !process_is_detached_handoff(output)
                {
                    continue;
                }
                let Some(pid) = process_pid(output) else {
                    continue;
                };
                candidates_by_pid.insert(pid, background_process_cleanup_candidate(pid, output));
            }
            PROCESS_STATUS_TOOL_NAME => {
                if let Some(pid) = process_pid(output) {
                    if output.get("alive").and_then(Value::as_bool) == Some(false) {
                        candidates_by_pid.remove(&pid);
                    }
                }
            }
            PROCESS_STOP_TOOL_NAME => {
                if let Some(pid) = process_pid(output) {
                    if process_stop_confirmed(output) {
                        candidates_by_pid.remove(&pid);
                    }
                }
            }
            _ => {}
        }
    }

    candidates_by_pid.into_values().map(background_process_cleanup_instruction).collect()
}

fn background_process_cleanup_candidate(
    pid: u32,
    output: &Value,
) -> BackgroundProcessCleanupCandidate {
    BackgroundProcessCleanupCandidate {
        pid,
        stop_command: process_command_label(
            output
                .pointer("/cleanup/portable_stop_command")
                .or_else(|| output.pointer("/handoff/stop_command")),
        ),
        status_command: process_command_label(
            output
                .pointer("/cleanup/portable_status_command")
                .or_else(|| output.pointer("/handoff/status_command")),
        ),
        start_context: process_start_context_label(output.pointer("/handoff/start_command")),
    }
}

fn background_process_cleanup_instruction(candidate: BackgroundProcessCleanupCandidate) -> String {
    let stop_command =
        candidate.stop_command.unwrap_or_else(|| format!("palyra.process.stop {}", candidate.pid));
    let status_command = candidate
        .status_command
        .unwrap_or_else(|| format!("palyra.process.status {}", candidate.pid));
    let start_context = candidate
        .start_context
        .map(|context| format!(" Start context: {context}."))
        .unwrap_or_default();
    format!(
        "detached background process pid {}; terminal cleanup will not stop it. Inspect it with `{status_command}` and stop it with `{stop_command}` if it remains alive.{start_context}",
        candidate.pid
    )
}

fn process_command_label(value: Option<&Value>) -> Option<String> {
    let value = value?;
    let command = value.get("command").and_then(Value::as_str)?;
    let mut parts = vec![command.to_owned()];
    if let Some(args) = value.get("args").and_then(Value::as_array) {
        parts.extend(args.iter().filter_map(Value::as_str).map(ToOwned::to_owned));
    }
    Some(checkpoint_text(parts.join(" ").as_str(), 260))
}

fn process_start_context_label(value: Option<&Value>) -> Option<String> {
    let value = value?;
    let mut parts = Vec::new();
    if let Some(cwd) = value.get("cwd").and_then(Value::as_str) {
        parts.push(format!("cwd={}", checkpoint_text(cwd, 180)));
    }
    if let Some(command) = process_command_label(Some(value)) {
        parts.push(format!("command={command}"));
    }
    (!parts.is_empty()).then(|| checkpoint_text(parts.join("; ").as_str(), 420))
}

fn process_stop_confirmed(output: &Value) -> bool {
    output.get("stopped").and_then(Value::as_bool) == Some(true)
        || output.get("alive").and_then(Value::as_bool) == Some(false)
        || output.get("process_tree_alive").and_then(Value::as_bool) == Some(false)
}

// Flags only routines this run itself created (an upsert without a
// routine_id) and still left behind; pre-existing routines the run touched
// are deliberately not reported as leaks.
fn pending_routine_cleanup_instructions(messages: &[ProviderMessage]) -> Vec<String> {
    let mut tool_calls_by_id = BTreeMap::<String, RoutineToolCallRef>::new();
    let mut created_routine_ids = BTreeSet::<String>::new();
    let mut latest_routine_views = BTreeMap::<String, Value>::new();

    for message in messages {
        for tool_call in &message.tool_calls {
            if matches!(
                tool_call.tool_name.as_str(),
                ROUTINES_QUERY_TOOL_NAME | ROUTINES_CONTROL_TOOL_NAME
            ) {
                tool_calls_by_id.insert(
                    tool_call.proposal_id.clone(),
                    RoutineToolCallRef {
                        tool_name: tool_call.tool_name.clone(),
                        input_json: tool_call.input_json.clone(),
                    },
                );
            }
        }

        if message.role != crate::model_provider::ProviderMessageRole::Tool {
            continue;
        }
        let Some(tool_call_id) = message.tool_call_id.as_deref() else {
            continue;
        };
        let Some(tool_call) = tool_calls_by_id.get(tool_call_id) else {
            continue;
        };
        let Ok(raw_output) = serde_json::from_str::<Value>(message.text_content().as_str()) else {
            continue;
        };
        let output = routine_tool_output_payload(&raw_output);
        let operation = routine_tool_operation(&tool_call.input_json, output);

        if tool_call.tool_name == ROUTINES_CONTROL_TOOL_NAME
            && operation == Some("delete")
            && output.get("deleted").and_then(Value::as_bool).unwrap_or(false)
        {
            if let Some(routine_id) = routine_id_from_routine_payload(output) {
                created_routine_ids.remove(routine_id);
                latest_routine_views.remove(routine_id);
            }
            continue;
        }

        if tool_call.tool_name == ROUTINES_CONTROL_TOOL_NAME && operation == Some("upsert") {
            let upsert_created_routine =
                tool_call.input_json.get("routine_id").and_then(Value::as_str).is_none();
            if upsert_created_routine {
                if let Some(routine) = output.get("routine").and_then(Value::as_object) {
                    if let Some(routine_id) = routine.get("routine_id").and_then(Value::as_str) {
                        created_routine_ids.insert(routine_id.to_owned());
                        latest_routine_views
                            .insert(routine_id.to_owned(), Value::Object(routine.clone()));
                    }
                }
            }
            continue;
        }

        if let Some(routine) = output.get("routine").and_then(Value::as_object) {
            if let Some(routine_id) = routine.get("routine_id").and_then(Value::as_str) {
                if created_routine_ids.contains(routine_id) {
                    latest_routine_views
                        .insert(routine_id.to_owned(), Value::Object(routine.clone()));
                }
            }
        }
    }

    created_routine_ids
        .into_iter()
        .map(|routine_id| {
            routine_cleanup_instruction(
                routine_id.as_str(),
                latest_routine_views.get(routine_id.as_str()),
            )
        })
        .collect()
}

fn routine_tool_output_payload(output: &Value) -> &Value {
    output
        .get("output")
        .filter(|_| output.get("tool_name").and_then(Value::as_str).is_some())
        .unwrap_or(output)
}

fn routine_tool_operation<'a>(input: &'a Value, output: &'a Value) -> Option<&'a str> {
    output
        .get("operation")
        .and_then(Value::as_str)
        .or_else(|| input.get("operation").and_then(Value::as_str))
}

fn routine_id_from_routine_payload(output: &Value) -> Option<&str> {
    output
        .get("routine_id")
        .and_then(Value::as_str)
        .or_else(|| output.get("routine")?.get("routine_id")?.as_str())
}

fn routine_cleanup_instruction(routine_id: &str, latest_view: Option<&Value>) -> String {
    let enabled = latest_view
        .and_then(|view| view.get("enabled"))
        .map(json_value_label)
        .unwrap_or_else(|| "unknown".to_owned());
    let next_run_at_unix_ms = latest_view
        .and_then(|view| view.get("next_run_at_unix_ms"))
        .map(json_value_label)
        .unwrap_or_else(|| "unknown".to_owned());
    let last_outcome_kind = latest_view
        .and_then(|view| view.get("last_outcome_kind"))
        .map(json_value_label)
        .unwrap_or_else(|| "unknown".to_owned());
    format!(
        "routine {routine_id} (enabled={enabled}, next_run_at_unix_ms={next_run_at_unix_ms}, last_outcome_kind={last_outcome_kind}); inspect it with `palyra routines show --id {routine_id} --json` or delete it with `palyra routines delete --id {routine_id} --json` if it remains present."
    )
}

fn json_value_label(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        _ => "structured".to_owned(),
    }
}

// A close counts as confirmed when it succeeded or when browserd reports the
// session as already absent; in both cases there is nothing left to clean up.
fn browser_session_close_confirmed(output: &Value) -> bool {
    output.get("closed").and_then(Value::as_bool).unwrap_or(false)
        || output.get("reason").and_then(Value::as_str).is_some_and(browser_session_absent_reason)
        || output.get("error").and_then(Value::as_str).is_some_and(browser_session_absent_reason)
        || output.get("output").is_some_and(browser_session_close_confirmed)
}

fn browser_session_absent_reason(raw: &str) -> bool {
    raw.contains("session_not_found") || raw.contains("chromium_session_not_found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_provider::{
        ProviderFinishReason, ProviderMessageContentPart, ProviderMessageRole,
        ProviderMessageToolCall, ProviderRawProviderRefs, ProviderUsage,
    };

    fn failed_attempt(tool_name: &str, input: &[u8]) -> RunProgressAttempt {
        RunProgressAttempt {
            tool_name: tool_name.to_owned(),
            normalized_input_json: input.to_vec(),
            normalized_output_hash: None,
            volatile_output_fields: Vec::new(),
            workspace_key: Some("workspace-a".to_owned()),
            query_hash: None,
            progress_percent: None,
            sensitivity: "normal".to_owned(),
            outcome_class: RunProgressOutcomeClass::Failure,
        }
    }

    fn append_successful_workspace_patch(state: &mut AgentRunLoopState, proposal_id: &str) {
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: proposal_id.to_owned(),
                tool_name: WORKSPACE_PATCH_TOOL_NAME.to_owned(),
                input_json: serde_json::json!({
                    "patch": "*** Begin Patch\n*** Add File: extract.js\n+console.log(1)\n*** End Patch"
                }),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            proposal_id,
            serde_json::json!({
                "patch_sha256": "abc",
                "dry_run": false,
                "files_touched": [{
                    "path": "extract.js",
                    "workspace_root_index": 0,
                    "operation": "create",
                    "after_sha256": "sha",
                    "after_size_bytes": 42
                }],
                "rollback_performed": false,
                "redacted_preview": ""
            })
            .to_string(),
        )]);
    }

    fn append_workspace_patch_with_stale_verification(
        state: &mut AgentRunLoopState,
        proposal_id: &str,
    ) {
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: proposal_id.to_owned(),
                tool_name: WORKSPACE_PATCH_TOOL_NAME.to_owned(),
                input_json: serde_json::json!({
                    "patch": "*** Begin Patch\n*** Add File: src/lib.rs\n+pub fn value() -> u8 { 1 }\n*** End Patch"
                }),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            proposal_id,
            serde_json::json!({
                "patch_sha256": "abc",
                "dry_run": false,
                "files_touched": [{
                    "path": "src/lib.rs",
                    "workspace_root_index": 0,
                    "operation": "create",
                    "after_sha256": "sha",
                    "after_size_bytes": 42
                }],
                "coding_posture": {
                    "schema_version": 1,
                    "instruction_authority": "none",
                    "verification": {
                        "schema_version": 1,
                        "instruction_authority": "none",
                        "freshness_status": "stale",
                        "requirements": [{
                            "schema_version": 1,
                            "workspace_root": {
                                "index": 0,
                                "root_id_sha256": "root-a",
                                "display_name": "workspace",
                                "exists": true
                            },
                            "requirement": {
                                "requirement_id": "verification.required_after_patch.test",
                                "workspace_root": {
                                    "index": 0,
                                    "root_id_sha256": "root-a",
                                    "display_name": "workspace",
                                    "exists": true
                                },
                                "required_kind": "test",
                                "changed_paths": ["src/lib.rs"],
                                "min_created_at_unix_ms": 500,
                                "reason_code": "verification.required_after_patch"
                            },
                            "latest_event_id": null,
                            "latest_passing_event_id": null,
                            "freshness": {
                                "schema_version": 1,
                                "status": "stale",
                                "requirement_id": "verification.required_after_patch.test",
                                "matched_event_id": null,
                                "checked_at_unix_ms": 500,
                                "reason_codes": [
                                    "verification.freshness_checked",
                                    "verification.no_passing_evidence",
                                    "verification.state_stale"
                                ],
                                "redaction_level": "metadata_and_redacted_summary"
                            },
                            "redaction_level": "metadata_and_redacted_summary"
                        }],
                        "redaction_level": "metadata_and_redacted_summary"
                    }
                },
                "rollback_performed": false,
                "redacted_preview": ""
            })
            .to_string(),
        )]);
    }

    fn append_successful_process_verification(state: &mut AgentRunLoopState, proposal_id: &str) {
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: proposal_id.to_owned(),
                tool_name: PROCESS_RUN_TOOL_NAME.to_owned(),
                input_json: serde_json::json!({
                    "command": "cargo",
                    "args": ["test"]
                }),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            proposal_id,
            serde_json::json!({
                "exit_code": 0,
                "stdout": "test result: ok",
                "stderr": "",
                "background": false
            })
            .to_string(),
        )]);
    }

    #[test]
    fn run_progress_controller_intervenes_after_repeated_failure() {
        let mut controller = RunProgressController::new(3);
        let attempt = failed_attempt("palyra.fs.read_file", br#"{"path":"missing"}"#);

        assert!(controller.observe(attempt.clone()).is_none());
        assert!(controller.observe(attempt.clone()).is_none());
        let intervention =
            controller.observe(attempt.clone()).expect("third identical failure intervenes");

        assert_eq!(intervention.attempts, 3);
        assert!(!intervention.terminate_run);
        assert!(intervention.guidance.contains("palyra.fs.read_file"));
        assert_eq!(intervention.fingerprint.input_hash.len(), 64);
        assert_eq!(intervention.event_type, TOOL_LOOP_WARNING_EVENT);
        assert_eq!(intervention.reason_code, "tool.loop.failure");

        let blocked = controller.observe(attempt).expect("fourth identical failure blocks");
        assert!(blocked.terminate_run);
        assert_eq!(blocked.event_type, TOOL_LOOP_BLOCKED_EVENT);
        assert_eq!(blocked.reason_code, "tool.loop.failure");
    }

    #[test]
    fn run_progress_controller_blocks_mutating_failure_at_threshold() {
        let mut controller = RunProgressController::new(3);
        let attempt = failed_attempt("palyra.fs.apply_patch", br#"{"patch":"bad"}"#);

        assert!(controller.observe(attempt.clone()).is_none());
        assert!(controller.observe(attempt.clone()).is_none());
        let intervention =
            controller.observe(attempt).expect("third identical mutating failure should block");

        assert!(intervention.terminate_run);
        assert_eq!(intervention.event_type, TOOL_LOOP_BLOCKED_EVENT);
        assert_eq!(intervention.reason_code, "tool.loop.mutating_failure");
    }

    #[test]
    fn run_progress_controller_terminates_repeated_denial() {
        let mut controller = RunProgressController::new(2);
        let mut attempt = failed_attempt("palyra.process.run", br#"{"command":"curl"}"#);
        attempt.outcome_class = RunProgressOutcomeClass::PolicyDenied;

        assert!(controller.observe(attempt.clone()).is_none());
        let intervention = controller.observe(attempt).expect("second denial intervenes");

        assert!(intervention.terminate_run);
        assert!(intervention.learning_observation.contains("repeated_no_progress"));
        assert_eq!(intervention.event_type, TOOL_LOOP_BLOCKED_EVENT);
        assert_eq!(intervention.reason_code, "tool.loop.policy_denied");
    }

    #[test]
    fn tool_loop_guardrail_guides_after_three_repeated_read_only_no_progress_calls() {
        let mut controller = RunProgressController::new(3);
        let mut attempt =
            failed_attempt("palyra.fs.read_file", br#"{"path":"src/main.rs","max_bytes":4096}"#);
        attempt.workspace_key = Some("src/main.rs".to_owned());
        attempt.outcome_class = RunProgressOutcomeClass::ReadNoProgress;

        assert!(controller.observe(attempt.clone()).is_none());
        assert!(controller.observe(attempt.clone()).is_none());
        let intervention = controller.observe(attempt.clone()).expect("third read loop warns");

        assert!(!intervention.terminate_run);
        assert_eq!(intervention.event_type, TOOL_LOOP_WARNING_EVENT);
        assert_eq!(intervention.reason_code, "tool.loop.read_no_progress");
        assert_eq!(intervention.signature.normalized_path_scope.as_deref(), Some("src/main.rs"));
        assert_eq!(intervention.signature.canonical_arguments_hash.len(), 64);
        assert!(intervention.guidance.contains("repeated the same read"));

        let blocked = controller.observe(attempt).expect("fourth identical read loop should block");
        assert!(blocked.terminate_run);
        assert_eq!(blocked.event_type, TOOL_LOOP_BLOCKED_EVENT);
        assert_eq!(blocked.reason_code, "tool.loop.read_no_progress");
    }

    #[test]
    fn tool_loop_guardrail_blocks_repeated_policy_denied_mutating_call() {
        let mut controller = RunProgressController::new(3);
        let mut attempt = failed_attempt(
            "palyra.fs.apply_patch",
            br#"{"patch":"*** Begin Patch\n*** Add File: secret.txt\n+token\n*** End Patch"}"#,
        );
        attempt.outcome_class = RunProgressOutcomeClass::PolicyDenied;

        assert!(controller.observe(attempt.clone()).is_none());
        assert!(controller.observe(attempt.clone()).is_none());
        let intervention = controller.observe(attempt).expect("third denial blocks");

        assert!(intervention.terminate_run);
        assert_eq!(intervention.event_type, TOOL_LOOP_BLOCKED_EVENT);
        assert_eq!(intervention.reason_code, "tool.loop.policy_denied");
        assert!(intervention.guidance.contains("stop retrying"));
    }

    #[test]
    fn tool_loop_guardrail_state_serializes_without_raw_arguments() {
        let mut state = ToolLoopGuardrailState::new(1);
        let attempt = failed_attempt("palyra.fs.search", br#"{"query":"needle"}"#);
        let decision = state.observe(&attempt).expect("first attempt crosses threshold");
        let serialized = serde_json::to_string(&state).expect("state should serialize");

        assert_eq!(decision.signature.canonical_arguments_hash.len(), 64);
        assert!(serialized.contains("canonical_arguments_hash"));
        assert!(!serialized.contains("needle"));
    }

    #[test]
    fn run_progress_controller_success_resets_failure_counts() {
        let mut controller = RunProgressController::new(2);
        let failure = failed_attempt("palyra.fs.read_file", br#"{"path":"missing"}"#);
        let mut success = failure.clone();
        success.outcome_class = RunProgressOutcomeClass::Success;

        assert!(controller.observe(failure.clone()).is_none());
        assert!(controller.observe(success).is_none());
        assert!(controller.observe(failure).is_none());
    }

    #[test]
    fn run_progress_controller_detects_alternating_read_cycle() {
        let mut controller = RunProgressController::new(3);
        let mut first = failed_attempt("palyra.process.status", br#"{"process_id":"a"}"#);
        first.outcome_class = RunProgressOutcomeClass::ReadNoProgress;
        first.normalized_output_hash = Some("a".repeat(64));
        let mut second = first.clone();
        second.normalized_input_json = br#"{"process_id":"b"}"#.to_vec();
        second.normalized_output_hash = Some("b".repeat(64));

        assert!(controller.observe(first.clone()).is_none());
        assert!(controller.observe(second.clone()).is_none());
        assert!(controller.observe(first.clone()).is_none());
        let warning = controller.observe(second.clone()).expect("A/B/A/B should warn");
        assert_eq!(warning.reason_code, "tool.loop.alternating_cycle");
        assert_eq!(warning.detection.detector_type, LoopDetectorType::AlternatingCycle);
        assert!(!warning.terminate_run);

        assert!(controller.observe(first).is_some());
        let blocked = controller.observe(second).expect("A/B/A/B/A/B should block");
        assert!(blocked.terminate_run);
        assert_eq!(blocked.detection.cycle_length, Some(2));
    }

    #[test]
    fn run_progress_controller_detects_volatile_field_poll() {
        let mut controller = RunProgressController::new(3);
        let mut attempt = failed_attempt("palyra.process.status", br#"{"process_id":"a"}"#);
        attempt.outcome_class = RunProgressOutcomeClass::ReadNoProgress;
        attempt.normalized_output_hash = Some("c".repeat(64));
        attempt.volatile_output_fields = vec!["timestamp".to_owned(), "request_id".to_owned()];

        assert!(controller.observe(attempt.clone()).is_none());
        assert!(controller.observe(attempt.clone()).is_none());
        let warning = controller.observe(attempt).expect("third identical poll should warn");

        assert_eq!(warning.reason_code, "tool.loop.volatile_field_poll");
        assert_eq!(warning.detection.detector_type, LoopDetectorType::VolatileFieldPoll);
        assert_eq!(
            warning.detection.volatile_fields_stripped,
            vec!["timestamp".to_owned(), "request_id".to_owned()]
        );
        let serialized =
            serde_json::to_value(&warning).expect("loop intervention should serialize");
        assert!(
            serialized.pointer("/detection/normalized_outcome_hash").is_none(),
            "replay-visible evidence must not expose process-local output digests: {serialized}"
        );
    }

    #[test]
    fn run_progress_controller_allows_monotonic_progress() {
        let mut controller = RunProgressController::new(2);
        let mut attempt = failed_attempt("palyra.process.status", br#"{"process_id":"a"}"#);
        attempt.outcome_class = RunProgressOutcomeClass::ReadNoProgress;
        attempt.normalized_output_hash = Some("d".repeat(64));

        for percent in [10, 20, 40, 100] {
            attempt.progress_percent = Some(percent);
            assert!(controller.observe(attempt.clone()).is_none());
        }
    }

    #[test]
    fn run_progress_controller_allows_legitimate_two_step_iteration() {
        let mut controller = RunProgressController::new(3);
        let mut first = failed_attempt("palyra.process.status", br#"{"process_id":"a"}"#);
        first.outcome_class = RunProgressOutcomeClass::ReadNoProgress;
        first.normalized_output_hash = Some("e".repeat(64));
        let mut second = first.clone();
        second.normalized_input_json = br#"{"process_id":"b"}"#.to_vec();
        second.normalized_output_hash = Some("f".repeat(64));

        assert!(controller.observe(first).is_none());
        assert!(controller.observe(second).is_none());
    }

    #[test]
    fn loop_state_ignores_legacy_turn_budget_and_serializes_unlimited_snapshot() {
        let mut state =
            AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 1, 2, 10_000);
        for expected_turn in 1..=200 {
            assert_eq!(state.start_model_turn(), Ok(expected_turn));
        }

        let payload = state.start_payload("run-01");
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("start payload should be JSON");
        assert_eq!(parsed["event"], "agent_loop.started");
        assert_eq!(parsed["max_model_turns"], serde_json::Value::Null);
        assert_eq!(parsed["max_tool_calls"], serde_json::Value::Null);
        assert_eq!(parsed["step_count_limit_active"], false);
        assert_eq!(parsed["state"]["model_turn_limit"], serde_json::Value::Null);
        assert_eq!(parsed["state"]["tool_call_limit"], serde_json::Value::Null);
        assert_eq!(parsed["state"]["remaining_model_turns"], serde_json::Value::Null);
        assert_eq!(parsed["state"]["remaining_tool_calls"], serde_json::Value::Null);
        assert_eq!(parsed["state"]["active_limits"], serde_json::json!(["wall_clock"]));
        assert_eq!(parsed["state"]["wall_clock_budget_ms"], 10_000);
        assert!(
            parsed["state"]["wall_clock_remaining_ms"]
                .as_u64()
                .is_some_and(|value| value <= 10_000),
            "remaining wall-clock budget should be bounded by the configured budget: {parsed}"
        );
    }

    #[test]
    fn loop_state_keeps_legacy_step_reasons_as_plain_failures() {
        let mut state =
            AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 2, 1, 10_000);
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-01",
            r#"{"ok":true}"#,
        )]);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::MaxToolCalls,
            "legacy step-count termination reason observed",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["termination_reason"], "max_tool_calls");
        assert_eq!(parsed["finalization"]["status"], "failed");
        assert_eq!(parsed["finalization"]["lifecycle_state"], "failed");
        assert_eq!(parsed["finalization"]["reason_code"], "max_tool_calls");
        assert_eq!(parsed["finalization"]["partial"], false);
        assert_eq!(parsed["finalization"]["continuation_required"], false);
        assert_eq!(parsed["finalization"]["tool_count"], 1);
    }

    #[test]
    fn loop_state_marks_incomplete_final_answer_after_tools_as_needs_continuation() {
        let mut state =
            AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 2, 1, 10_000);
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-01",
            r#"{"ok":true}"#,
        )]);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::IncompleteFinalAnswer,
            "Partial result: model hit finish_reason=length after tool work.",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["termination_reason"], "incomplete_final_answer");
        assert_eq!(parsed["finalization"]["status"], "needs_continuation");
        assert_eq!(parsed["finalization"]["lifecycle_state"], "needs_continuation");
        assert_eq!(parsed["finalization"]["partial"], true);
        assert_eq!(parsed["finalization"]["continuation_required"], true);
        assert_eq!(parsed["finalization"]["tool_count"], 1);
    }

    #[test]
    fn final_answer_contract_serializes_completed_evidence_summary() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("Create extract.js.".to_owned())],
            2,
            4,
            10_000,
        );
        append_successful_workspace_patch(&mut state, "call-patch");

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::FinalAnswer,
            "Created extract.js.",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");
        let finalization = parsed["finalization"].clone();
        let envelope: AgentLoopFinalizationEnvelope =
            serde_json::from_value(finalization.clone()).expect("finalization should round-trip");

        assert_eq!(finalization["final_answer_contract"]["decision"], "accepted");
        assert_eq!(finalization["terminal_outcome"]["class"], "visible_text");
        assert_eq!(
            finalization["terminal_outcome"]["reason_code"],
            "terminal_outcome.visible_text"
        );
        assert_eq!(
            finalization["terminal_outcome"]["visible_text_bytes"],
            serde_json::json!("Created extract.js.".len())
        );
        assert_eq!(
            finalization["final_answer_contract"]["journal_projection"]["event_type"],
            FINAL_ANSWER_CONTRACT_COMPLETED_EVENT
        );
        assert_eq!(finalization["final_answer_contract"]["enforcement_mode"], "observe_only");
        assert_eq!(finalization["evidence_summary"]["coverage"], "satisfied");
        assert_eq!(finalization["evidence_summary"]["produced_files_count"], 1);
        assert!(finalization["evidence_summary"]["evidence_refs"]
            .as_array()
            .expect("evidence refs should be an array")
            .iter()
            .any(|value| value == "file:extract.js"));
        assert_eq!(envelope.final_answer_contract.decision, FinalAnswerDecision::Accepted);
        assert_eq!(envelope.evidence_summary.coverage, FinalAnswerEvidenceCoverage::Satisfied);
    }

    #[test]
    fn verify_before_finish_guard_nudges_after_unverified_code_mutation() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("Create src/lib.rs.".to_owned())],
            2,
            4,
            10_000,
        );
        append_workspace_patch_with_stale_verification(&mut state, "call-patch");

        let report = state.verify_before_finish_guard(
            "run-01",
            AgentLoopTerminationReason::FinalAnswer,
            "Created src/lib.rs.",
        );

        assert_eq!(report.status, FinalizationVerificationStatus::NudgeRequired);
        assert_eq!(report.event_type.as_deref(), Some(VERIFICATION_FINALIZER_NUDGE_EVENT));
        assert_eq!(report.pending_requirement_count, 1);
        assert!(report
            .nudge
            .as_deref()
            .is_some_and(|text| text.contains("verification_status=unverified")));

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::FinalAnswer,
            "Created src/lib.rs.",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should parse");
        assert_eq!(parsed["finalization"]["verification_finalizer"]["status"], "nudge_required");
    }

    #[test]
    fn verify_before_finish_guard_accepts_matching_process_verification() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("Create src/lib.rs.".to_owned())],
            2,
            4,
            10_000,
        );
        append_workspace_patch_with_stale_verification(&mut state, "call-patch");
        append_successful_process_verification(&mut state, "call-test");

        let report = state.verify_before_finish_guard(
            "run-01",
            AgentLoopTerminationReason::FinalAnswer,
            "Created src/lib.rs and cargo test passed.",
        );

        assert_eq!(report.status, FinalizationVerificationStatus::Verified);
        assert_eq!(report.pending_requirement_count, 0);
        assert_eq!(report.satisfied_requirement_count, 1);
        assert!(report.evidence_refs.iter().any(|reference| reference == "tool_call:call-test"));
    }

    #[test]
    fn verify_before_finish_guard_allows_explicit_unverified_reason() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("Create src/lib.rs.".to_owned())],
            2,
            4,
            10_000,
        );
        append_workspace_patch_with_stale_verification(&mut state, "call-patch");

        let report = state.verify_before_finish_guard(
            "run-01",
            AgentLoopTerminationReason::FinalAnswer,
            "Created src/lib.rs. verification_status=unverified because cargo is unavailable.",
        );

        assert_eq!(report.status, FinalizationVerificationStatus::UnverifiedAllowed);
        assert_eq!(
            report.event_type.as_deref(),
            Some(VERIFICATION_FINALIZER_UNVERIFIED_ALLOWED_EVENT)
        );
        assert!(report
            .unverified_reason
            .as_deref()
            .is_some_and(|reason| { reason.contains("verification_status=unverified") }));
    }

    #[test]
    fn verify_before_finish_guard_skips_runs_without_code_mutation() {
        let state = AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 2, 4, 10_000);

        let report = state.verify_before_finish_guard(
            "run-01",
            AgentLoopTerminationReason::FinalAnswer,
            "Plain answer.",
        );

        assert_eq!(report.status, FinalizationVerificationStatus::NotRequired);
        assert_eq!(report.reason_code, "verification.finalizer.no_code_mutation");
    }

    #[test]
    fn final_answer_contract_rejects_incomplete_answer_without_tool_evidence() {
        let state = AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 2, 1, 10_000);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::IncompleteFinalAnswer,
            "model returned no usable answer before any tool evidence",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["finalization"]["status"], "failed");
        assert_eq!(parsed["finalization"]["terminal_outcome"]["class"], "protocol_error");
        assert_eq!(parsed["finalization"]["final_answer_contract"]["decision"], "rejected");
        assert_eq!(
            parsed["finalization"]["final_answer_contract"]["journal_projection"]["event_type"],
            FINAL_ANSWER_CONTRACT_FAILED_EVENT
        );
        assert_eq!(parsed["finalization"]["evidence_summary"]["coverage"], "no_tool_evidence");
        assert_eq!(parsed["finalization"]["evidence_summary"]["tool_count"], 0);
    }

    #[test]
    fn terminal_outcome_serializes_reasoning_only_recovery() {
        let state = AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 2, 1, 10_000);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::IncompleteFinalAnswer,
            "model returned reasoning-only output without a user-visible final answer",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["finalization"]["terminal_outcome"]["class"], "reasoning_only");
        assert_eq!(
            parsed["finalization"]["terminal_outcome"]["reason_code"],
            "terminal_outcome.reasoning_only"
        );
        assert_eq!(parsed["finalization"]["terminal_outcome"]["requires_recovery"], true);
        assert_eq!(parsed["finalization"]["final_answer_contract"]["decision"], "rejected");
    }

    #[test]
    fn progress_checkpoint_reports_produced_and_missing_artifacts() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text(
                "Create extract.js and final solution.txt.".to_owned(),
            )],
            2,
            4,
            10_000,
        );
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: "call-patch".to_owned(),
                tool_name: WORKSPACE_PATCH_TOOL_NAME.to_owned(),
                input_json: serde_json::json!({"patch":"*** Begin Patch\n*** Add File: extract.js\n+console.log(1)\n*** End Patch"}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-patch",
            serde_json::json!({
                "patch_sha256": "abc",
                "dry_run": false,
                "files_touched": [{
                    "path": "extract.js",
                    "workspace_root_index": 0,
                    "operation": "create",
                    "after_sha256": "sha",
                    "after_size_bytes": 42
                }],
                "rollback_performed": false,
                "redacted_preview": ""
            })
            .to_string(),
        )]);

        let checkpoint =
            state.progress_checkpoint("run-01", AgentLoopTerminationReason::IncompleteFinalAnswer);

        assert_eq!(checkpoint.produced_files[0].path, "extract.js");
        assert_eq!(checkpoint.produced_files[0].status, "exists");
        assert!(checkpoint
            .missing_artifacts
            .iter()
            .any(|artifact| artifact.path == "solution.txt"));
        assert_eq!(
            checkpoint.last_successful_tool.as_ref().map(|tool| tool.tool_name.as_str()),
            Some(WORKSPACE_PATCH_TOOL_NAME)
        );
    }

    #[test]
    fn progress_checkpoint_does_not_mark_successfully_read_prompt_files_missing() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text(
                "Read SCENARIO_CONTEXT.txt and instruction.md, then continue.".to_owned(),
            )],
            2,
            4,
            10_000,
        );
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![
                crate::model_provider::ProviderOutputContentPart::ToolCall {
                    proposal_id: "call-context".to_owned(),
                    tool_name: OS_FILE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "operation": "read",
                        "path": "C:/runs/S006/SCENARIO_CONTEXT.txt"
                    }),
                },
                crate::model_provider::ProviderOutputContentPart::ToolCall {
                    proposal_id: "call-instruction".to_owned(),
                    tool_name: OS_FILE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "operation": "read",
                        "path": "C:/runs/S006/instruction.md"
                    }),
                },
            ],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![
            ProviderMessage::tool_result(
                "call-context",
                serde_json::json!({
                    "operation": "read",
                    "path": "C:/runs/S006/SCENARIO_CONTEXT.txt",
                    "resolved_path": "C:/runs/S006/SCENARIO_CONTEXT.txt",
                    "text": "scenario context"
                })
                .to_string(),
            ),
            ProviderMessage::tool_result(
                "call-instruction",
                serde_json::json!({
                    "operation": "read",
                    "path": "C:/runs/S006/instruction.md",
                    "resolved_path": "C:/runs/S006/instruction.md",
                    "text": "scenario instructions"
                })
                .to_string(),
            ),
        ]);

        let checkpoint = state.progress_checkpoint("run-01", AgentLoopTerminationReason::WallClock);

        assert!(
            checkpoint.missing_artifacts.is_empty(),
            "read files should satisfy prompt artifact references: {:?}",
            checkpoint.missing_artifacts
        );
        assert!(
            !checkpoint.recommended_next_action.contains("missing artifact"),
            "checkpoint should not ask for already-read files: {}",
            checkpoint.recommended_next_action
        );
    }

    #[test]
    fn progress_checkpoint_redacts_internal_runtime_paths_from_failed_attempts() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("Write solution.txt".to_owned())],
            2,
            4,
            10_000,
        );
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: "call-os-file".to_owned(),
                tool_name: OS_FILE_TOOL_NAME.to_owned(),
                input_json: serde_json::json!({"operation":"read","path":"solution.txt"}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-os-file",
            serde_json::json!({
                "success": false,
                "tool_name": OS_FILE_TOOL_NAME,
                "error": r#"failed at C:\Users\Aftab Jafar Ansari\.palyra\sessions\01ABC\tape.ndjson"#,
                "output": {}
            })
            .to_string(),
        )]);

        let checkpoint_json =
            state.progress_checkpoint_json("run-01", AgentLoopTerminationReason::ProviderError);

        assert!(!checkpoint_json.contains("Aftab Jafar Ansari"), "{checkpoint_json}");
        assert!(!checkpoint_json.contains(".palyra"), "{checkpoint_json}");
        assert!(checkpoint_json.contains("<palyra_internal_artifact_ref>"));
    }

    #[test]
    fn progress_checkpoint_preserves_last_successful_tool_after_provider_error() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("Create extract.js")],
            2,
            4,
            10_000,
        );
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: "call-patch".to_owned(),
                tool_name: WORKSPACE_PATCH_TOOL_NAME.to_owned(),
                input_json: serde_json::json!({}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-patch",
            serde_json::json!({
                "patch_sha256": "abc",
                "dry_run": false,
                "files_touched": [{
                    "path": "extract.js",
                    "workspace_root_index": 0,
                    "operation": "create",
                    "after_sha256": "sha",
                    "after_size_bytes": 42
                }],
                "rollback_performed": false,
                "redacted_preview": ""
            })
            .to_string(),
        )]);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::ProviderError,
            "model provider reported finish_reason=tool_calls without a structured tool call payload",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(
            parsed["finalization"]["progress_checkpoint"]["last_successful_tool"]["tool_name"],
            WORKSPACE_PATCH_TOOL_NAME
        );
        assert_eq!(
            parsed["finalization"]["progress_checkpoint"]["produced_files"][0]["path"],
            "extract.js"
        );
    }

    #[test]
    fn progress_checkpoint_reports_background_process_state_after_wall_clock() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("Start dev server")],
            2,
            4,
            10_000,
        );
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: "call-process".to_owned(),
                tool_name: PROCESS_RUN_TOOL_NAME.to_owned(),
                input_json: serde_json::json!({"command":"npm","args":["run","dev"],"background":true}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-process",
            serde_json::json!({
                "background": true,
                "pid": 12345,
                "process_handle": {"direct_process_pid": 12345},
                "run_lifecycle_note": "run-owned process"
            })
            .to_string(),
        )]);

        let checkpoint = state.progress_checkpoint("run-01", AgentLoopTerminationReason::WallClock);

        assert_eq!(checkpoint.active_processes[0].pid, 12345);
        assert_eq!(checkpoint.active_processes[0].status, "run_owned_background_started");
        assert!(checkpoint.recommended_next_action.contains("background process state"));
    }

    #[test]
    fn loop_state_marks_browser_followup_timeout_after_tools_as_needs_continuation() {
        let mut state =
            AgentRunLoopState::new(vec![ProviderMessage::user_text("open the page")], 2, 1, 10_000);
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-01",
            r#"{"ok":true}"#,
        )]);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::BrowserFollowupTimeout,
            "Partial result: browser follow-up model turn timed out.",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["termination_reason"], "browser_followup_timeout");
        assert_eq!(parsed["finalization"]["status"], "needs_continuation");
        assert_eq!(parsed["finalization"]["reason_code"], "browser_followup_timeout");
        assert_eq!(parsed["finalization"]["partial"], true);
        assert_eq!(parsed["finalization"]["continuation_required"], true);
        assert_eq!(parsed["finalization"]["tool_count"], 1);
    }

    #[test]
    fn loop_state_marks_tool_followup_timeout_after_tools_as_needs_continuation() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("write files and validate them")],
            2,
            1,
            10_000,
        );
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-01",
            r#"{"ok":true}"#,
        )]);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::ToolFollowupTimeout,
            "Partial result: tool follow-up model turn timed out.",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["termination_reason"], "tool_followup_timeout");
        assert_eq!(parsed["finalization"]["status"], "needs_continuation");
        assert_eq!(parsed["finalization"]["reason_code"], "tool_followup_timeout");
        assert_eq!(parsed["finalization"]["partial"], true);
        assert_eq!(parsed["finalization"]["continuation_required"], true);
        assert_eq!(parsed["finalization"]["tool_count"], 1);
    }

    #[test]
    fn loop_state_marks_run_loop_phase_timeout_after_tools_as_needs_continuation() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("create files and run tests")],
            2,
            1,
            10_000,
        );
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-01",
            r#"{"ok":true}"#,
        )]);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::RunLoopPhaseTimeout,
            "Partial result: tool catalog snapshot timed out.",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["termination_reason"], "run_loop_phase_timeout");
        assert_eq!(parsed["finalization"]["status"], "needs_continuation");
        assert_eq!(parsed["finalization"]["reason_code"], "run_loop_phase_timeout");
        assert_eq!(parsed["finalization"]["partial"], true);
        assert_eq!(parsed["finalization"]["continuation_required"], true);
        assert_eq!(parsed["finalization"]["tool_count"], 1);
        assert!(parsed["finalization"]["progress_checkpoint"].is_object());
    }

    #[test]
    fn loop_state_marks_provider_error_after_tools_as_needs_continuation() {
        let mut state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("create app files")],
            2,
            1,
            10_000,
        );
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "call-01",
            r#"{"ok":true}"#,
        )]);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::ProviderError,
            "Partial result: provider failed after tool work.",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["termination_reason"], "provider_error");
        assert_eq!(parsed["finalization"]["status"], "needs_continuation");
        assert_eq!(parsed["finalization"]["lifecycle_state"], "needs_continuation");
        assert_eq!(parsed["finalization"]["reason_code"], "provider_error");
        assert_eq!(parsed["finalization"]["partial"], true);
        assert_eq!(parsed["finalization"]["continuation_required"], true);
        assert_eq!(parsed["finalization"]["tool_count"], 1);
    }

    #[test]
    fn loop_state_keeps_provider_error_without_tools_as_failure() {
        let state = AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 2, 1, 10_000);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::ProviderError,
            "provider failed before any tool evidence",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["termination_reason"], "provider_error");
        assert_eq!(parsed["finalization"]["status"], "failed");
        assert_eq!(parsed["finalization"]["lifecycle_state"], "failed");
        assert_eq!(parsed["finalization"]["partial"], false);
        assert_eq!(parsed["finalization"]["continuation_required"], false);
    }

    #[test]
    fn loop_state_keeps_incomplete_final_answer_without_tools_as_failure() {
        let state = AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 2, 1, 10_000);

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::IncompleteFinalAnswer,
            "model returned no usable answer before any tool evidence",
            None,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");

        assert_eq!(parsed["termination_reason"], "incomplete_final_answer");
        assert_eq!(parsed["finalization"]["status"], "failed");
        assert_eq!(parsed["finalization"]["lifecycle_state"], "failed");
        assert_eq!(parsed["finalization"]["partial"], false);
        assert_eq!(parsed["finalization"]["continuation_required"], false);
        assert_eq!(parsed["finalization"]["tool_count"], 0);
    }

    #[test]
    fn wall_clock_budget_allows_longer_browser_verification_workflows() {
        assert_eq!(DEFAULT_AGENT_LOOP_WALL_CLOCK_BUDGET_MS, 15 * 60 * 1_000);
    }

    #[test]
    fn assistant_and_tool_messages_preserve_native_tool_ids() {
        let output = ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: "call-01".to_owned(),
                tool_name: "palyra.echo".to_owned(),
                input_json: serde_json::json!({"text":"hello"}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(3, 1, "provider"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        };
        let assistant = ProviderMessage::assistant_from_output(&output);
        assert_eq!(assistant.tool_calls[0].proposal_id, "call-01");

        let tool = ProviderMessage::tool_result("call-01", r#"{"echo":"hello"}"#);
        assert_eq!(tool.tool_call_id.as_deref(), Some("call-01"));
        assert!(tool.tool_calls.is_empty());
    }

    #[test]
    fn loop_state_reports_browser_session_cleanup_guidance_on_failure() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-create".to_owned(),
                    tool_name: BROWSER_SESSION_CREATE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({"allow_private_targets": true}),
                }],
            },
            ProviderMessage::tool_result(
                "call-create",
                serde_json::json!({"session_id": session_id}).to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert!(message.contains("agent loop wall-clock budget exhausted"));
        assert!(message.contains("browser session 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(message.contains("palyra browser session close 01ARZ3NDEKTSV4RRFFQ69G5FAV --json"));
        assert!(message.contains("palyra browser stop --json"));
    }

    #[test]
    fn loop_state_reports_detached_background_process_cleanup_guidance_on_failure() {
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-process".to_owned(),
                    tool_name: PROCESS_RUN_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "command": "node",
                        "args": ["C:/fixtures/S068/bin/slow-preview.js", "C:/fixtures/S068"],
                        "background": true,
                        "lifetime_mode": "detached",
                    }),
                }],
            },
            ProviderMessage::tool_result(
                "call-process",
                serde_json::json!({
                    "background": true,
                    "durable_handoff": true,
                    "run_owned_lifetime": false,
                    "lifetime_mode": "detached",
                    "pid": 40660,
                    "cleanup": {
                        "portable_stop_command": {
                            "command": "palyra.process.stop",
                            "args": ["40660"]
                        },
                        "portable_status_command": {
                            "command": "palyra.process.status",
                            "args": ["40660"]
                        }
                    },
                    "handoff": {
                        "start_command": {
                            "command": "node",
                            "args": [
                                "C:/fixtures/S068/bin/slow-preview.js",
                                "C:/fixtures/S068"
                            ],
                            "cwd": "C:/fixtures/S068"
                        }
                    }
                })
                .to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("cancelled by request");
        let checkpoint = state.progress_checkpoint(
            "01KWFMEPWN8K9WQ6QH5VX4GS9B",
            AgentLoopTerminationReason::Cancellation,
        );

        assert!(message.contains("cancelled by request"));
        assert!(message.contains("detached background process pid 40660"));
        assert!(message.contains("palyra.process.status 40660"));
        assert!(message.contains("palyra.process.stop 40660"));
        assert!(message.contains("C:/fixtures/S068"));
        assert_eq!(checkpoint.active_processes.len(), 1);
        assert_eq!(checkpoint.active_processes[0].status, "detached_background_started");
        assert!(
            checkpoint.active_processes[0].cleanup.contains("detached_background_process"),
            "{checkpoint:?}"
        );
    }

    #[test]
    fn loop_state_omits_cleanup_guidance_for_closed_browser_sessions() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: vec![ProviderMessageContentPart::text("creating browser")],
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-create".to_owned(),
                    tool_name: BROWSER_SESSION_CREATE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({}),
                }],
            },
            ProviderMessage::tool_result(
                "call-create",
                serde_json::json!({"session_id": session_id}).to_string(),
            ),
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-close".to_owned(),
                    tool_name: BROWSER_SESSION_CLOSE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({"session_id": session_id}),
                }],
            },
            ProviderMessage::tool_result(
                "call-close",
                serde_json::json!({"closed": true}).to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert_eq!(message, "agent loop wall-clock budget exhausted");
    }

    #[test]
    fn loop_state_omits_cleanup_guidance_when_browser_session_is_already_absent() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: vec![ProviderMessageContentPart::text("creating browser")],
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-create".to_owned(),
                    tool_name: BROWSER_SESSION_CREATE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({}),
                }],
            },
            ProviderMessage::tool_result(
                "call-create",
                serde_json::json!({"session_id": session_id}).to_string(),
            ),
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-close".to_owned(),
                    tool_name: BROWSER_SESSION_CLOSE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({"session_id": session_id}),
                }],
            },
            ProviderMessage::tool_result(
                "call-close",
                serde_json::json!({
                    "success": false,
                    "tool_name": BROWSER_SESSION_CLOSE_TOOL_NAME,
                    "error": "palyra.browser.session.close failed: session_not_found",
                    "output": {
                        "closed": false,
                        "reason": "session_not_found"
                    }
                })
                .to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert_eq!(message, "agent loop wall-clock budget exhausted");
    }

    #[test]
    fn loop_state_reports_created_routine_cleanup_guidance_on_failure() {
        let routine_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-upsert".to_owned(),
                    tool_name: ROUTINES_CONTROL_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "operation": "upsert",
                        "trigger_kind": "schedule",
                        "name": "heartbeat",
                        "prompt": "append heartbeat",
                        "schedule_type": "every",
                        "every_interval_ms": 60_000,
                        "max_runs": 3,
                    }),
                }],
            },
            ProviderMessage::tool_result(
                "call-upsert",
                serde_json::json!({
                    "operation": "upsert",
                    "routine": {
                        "routine_id": routine_id,
                        "enabled": false,
                        "next_run_at_unix_ms": null,
                        "last_outcome_kind": "success_with_output"
                    }
                })
                .to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert!(message.contains("agent loop wall-clock budget exhausted"));
        assert!(message.contains("routine 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(message.contains("enabled=false"));
        assert!(message.contains("next_run_at_unix_ms=null"));
        assert!(message.contains("last_outcome_kind=success_with_output"));
        assert!(message.contains("palyra routines delete --id 01ARZ3NDEKTSV4RRFFQ69G5FAV --json"));
    }

    #[test]
    fn loop_state_omits_created_routine_cleanup_after_delete() {
        let routine_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-upsert".to_owned(),
                    tool_name: ROUTINES_CONTROL_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "operation": "upsert",
                        "trigger_kind": "schedule",
                        "name": "heartbeat",
                        "prompt": "append heartbeat",
                        "schedule_type": "every",
                        "every_interval_ms": 60_000,
                    }),
                }],
            },
            ProviderMessage::tool_result(
                "call-upsert",
                serde_json::json!({
                    "operation": "upsert",
                    "routine": {
                        "routine_id": routine_id,
                        "enabled": true,
                        "next_run_at_unix_ms": 42,
                    }
                })
                .to_string(),
            ),
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-delete".to_owned(),
                    tool_name: ROUTINES_CONTROL_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "operation": "delete",
                        "routine_id": routine_id,
                    }),
                }],
            },
            ProviderMessage::tool_result(
                "call-delete",
                serde_json::json!({
                    "operation": "delete",
                    "deleted": true,
                    "routine_id": routine_id,
                })
                .to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert_eq!(message, "agent loop wall-clock budget exhausted");
    }
}
