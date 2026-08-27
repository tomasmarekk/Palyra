//! Verification evidence ledger contracts.
//!
//! This module is deliberately storage-neutral: it defines the journal payload
//! shapes and pure read-model logic for verification freshness, while later
//! runtime integrations decide when to append the events. The model is
//! conservative about path coverage; unknown coverage never becomes a fresh
//! passing verification.

#![allow(dead_code)]

use std::{collections::BTreeSet, fmt, path::Component};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::application::project_facts::ProjectWorkspaceRootRef;
use palyra_common::{
    process_runner_input::ProcessRunnerToolInput,
    redaction::{is_sensitive_key, redact_diagnostic_text, REDACTED},
};

pub(crate) const VERIFICATION_SCHEMA_VERSION: u32 = 1;
pub(crate) const VERIFICATION_COMMAND_CLASSIFIED: &str = "verification.command.classified";
pub(crate) const VERIFICATION_EVENT_RECORDED: &str = "verification.event.recorded";
pub(crate) const VERIFICATION_STATE_STALE: &str = "verification.state.stale";
pub(crate) const VERIFICATION_FRESHNESS_CHECKED: &str = "verification.freshness.checked";
pub(crate) const VERIFICATION_REDACTION_LEVEL: &str = "metadata_and_redacted_summary";
const MAX_OUTPUT_SUMMARY_CHARS: usize = 640;

pub(crate) const VERIFICATION_STATUS_ROLLOUT_DISABLED: &str =
    "verification.status.rollout_disabled";
pub(crate) const VERIFICATION_STATUS_NO_RECENT_EVENTS: &str =
    "verification.status.no_recent_events";
pub(crate) const VERIFICATION_STATUS_RECENT_STALE_REQUIREMENT: &str =
    "verification.status.recent_stale_requirement";
pub(crate) const VERIFICATION_STATUS_RECENT_FAILED_EVENT: &str =
    "verification.status.recent_failed_event";
pub(crate) const VERIFICATION_STATUS_RECENT_UNKNOWN_REQUIREMENT: &str =
    "verification.status.recent_unknown_requirement";
pub(crate) const VERIFICATION_STATUS_RECENT_FRESH_EVIDENCE: &str =
    "verification.status.recent_fresh_evidence";
pub(crate) const VERIFICATION_STATUS_JOURNAL_UNAVAILABLE: &str =
    "verification.status.journal_unavailable";
pub(crate) const VERIFICATION_OBSERVED_TOOL_ACTIVITY: &str = "verification.observed_tool_activity";
pub(crate) const VERIFICATION_OBSERVED_TOOL_ACTIVITY_INCOMPLETE: &str =
    "verification.observed_tool_activity_incomplete";
pub(crate) const VERIFICATION_OBSERVED_PATCH_MUTATION: &str =
    "verification.observed_patch_mutation";
pub(crate) const VERIFICATION_OBSERVED_PROCESS_ATTEMPT: &str =
    "verification.observed_process_attempt";
const VERIFICATION_FINALIZER_NO_CODE_MUTATION: &str = "verification.finalizer.no_code_mutation";
const VERIFICATION_FINALIZER_OBSERVED_MUTATION_REQUIRES_VERIFICATION: &str =
    "verification.finalizer.observed_mutation_requires_verification";
const VERIFICATION_FINALIZER_ROLLOUT_DISABLED_WITH_OBSERVED_MUTATION: &str =
    "verification.finalizer.rollout_disabled_with_observed_mutation";

/// Verification work family. Classifiers may map several commands into one kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VerificationKind {
    Build,
    Check,
    Format,
    Inspect,
    Lint,
    Test,
    Typecheck,
    Unknown,
}

impl VerificationKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Check => "check",
            Self::Format => "format",
            Self::Inspect => "inspect",
            Self::Lint => "lint",
            Self::Test => "test",
            Self::Typecheck => "typecheck",
            Self::Unknown => "unknown",
        }
    }
}

/// How much of the changed workspace a verification event claims to cover.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VerificationScope {
    Workspace,
    ChangedPaths,
    PathSet,
    Unknown,
}

impl VerificationScope {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::ChangedPaths => "changed_paths",
            Self::PathSet => "path_set",
            Self::Unknown => "unknown",
        }
    }
}

/// Result status of a verification attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VerificationStatus {
    Passed,
    Failed,
    TimedOut,
    Cancelled,
    Skipped,
    Unknown,
}

impl VerificationStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
            Self::Unknown => "unknown",
        }
    }

    const fn is_passing(self) -> bool {
        matches!(self, Self::Passed)
    }
}

/// Freshness read-model status for a requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VerificationFreshnessStatus {
    Fresh,
    Stale,
    Unknown,
}

impl VerificationFreshnessStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Unknown => "unknown",
        }
    }
}

/// Operator-facing verification status derived from recent journal projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VerificationDiagnosticsDecision {
    Disabled,
    Fresh,
    Stale,
    Failed,
    Unknown,
    NoEvidence,
}

impl VerificationDiagnosticsDecision {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
            Self::NoEvidence => "no_evidence",
        }
    }
}

/// Stable status summary consumed by CLI status and console diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationStatusForCliAndConsoleDiagnostics {
    pub(crate) schema_version: u32,
    pub(crate) decision: VerificationDiagnosticsDecision,
    pub(crate) rollout_enabled: bool,
    pub(crate) journal_total_events: u64,
    pub(crate) journal_window_events: u64,
    pub(crate) verification_projection_events: u64,
    pub(crate) classified_commands: u64,
    pub(crate) recorded_events: u64,
    pub(crate) passing_events: u64,
    pub(crate) failed_events: u64,
    pub(crate) stale_requirements: u64,
    pub(crate) fresh_requirements: u64,
    pub(crate) unknown_requirements: u64,
    pub(crate) latest_event_type: Option<String>,
    pub(crate) latest_status: Option<String>,
    pub(crate) latest_created_at_unix_ms: Option<i64>,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) journal_events: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Public, run-scoped artifact that explains which verification evidence exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationSummary {
    pub(crate) schema_version: u32,
    pub(crate) state: String,
    pub(crate) rollout_enabled: bool,
    pub(crate) changed_files: Vec<String>,
    pub(crate) commands_executed: Vec<VerificationSummaryCommand>,
    pub(crate) command_classification: Vec<VerificationSummaryCommandClassification>,
    pub(crate) latest_verification_status: VerificationStatusForCliAndConsoleDiagnostics,
    pub(crate) unverified_mutations: Vec<VerificationSummaryUnverifiedMutation>,
    pub(crate) stale_evidence_reasons: Vec<String>,
    pub(crate) diagnostics: Vec<VerificationSummaryDiagnostic>,
    pub(crate) final_answer: VerificationSummaryFinalAnswer,
    pub(crate) final_answer_allowed: bool,
    pub(crate) final_answer_allowed_because: String,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Public command row derived from verification command/event projections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationSummaryCommand {
    pub(crate) command: String,
    pub(crate) is_verification: bool,
    pub(crate) kind: String,
    pub(crate) scope: String,
    pub(crate) status: Option<String>,
    pub(crate) exit_code: Option<i32>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) reason_codes: Vec<String>,
}

/// Public classification row for process commands, including unrelated commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationSummaryCommandClassification {
    pub(crate) command: String,
    pub(crate) is_verification: bool,
    pub(crate) kind: String,
    pub(crate) scope: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) reason_codes: Vec<String>,
}

/// Public stale/unknown requirement row for mutations that still need evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationSummaryUnverifiedMutation {
    pub(crate) requirement_id: String,
    pub(crate) required_kind: String,
    pub(crate) changed_files: Vec<String>,
    pub(crate) freshness_status: String,
    pub(crate) min_created_at_unix_ms: i64,
    pub(crate) reason_codes: Vec<String>,
}

/// Public diagnostic evidence row from code-intelligence/LSP journal events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationSummaryDiagnostic {
    pub(crate) event_type: String,
    pub(crate) new_errors: u64,
    pub(crate) new_warnings: u64,
    pub(crate) degraded: bool,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) evidence_refs: Vec<String>,
}

/// Public final-answer verification decision derived from the finalizer envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationSummaryFinalAnswer {
    pub(crate) observed: bool,
    pub(crate) status: Option<String>,
    pub(crate) reason_code: Option<String>,
    pub(crate) allowed: bool,
    pub(crate) allowed_because: String,
    pub(crate) pending_requirement_count: Option<u64>,
    pub(crate) satisfied_requirement_count: Option<u64>,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) nudge: Option<String>,
    pub(crate) unverified_reason: Option<String>,
}

/// Redacted tool activity recovered from the durable run tape when the
/// experimental verification journal has no projection for ordinary tool use.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct VerificationObservedToolActivity {
    pub(crate) changed_files: Vec<String>,
    pub(crate) commands_executed: Vec<VerificationSummaryCommand>,
    pub(crate) command_classification: Vec<VerificationSummaryCommandClassification>,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) complete: bool,
}

/// Inputs for building a public verification summary without coupling to transport types.
pub(crate) struct VerificationSummaryRequest<'a> {
    pub(crate) rollout_enabled: bool,
    pub(crate) journal_total_events: u64,
    pub(crate) journal_window_events: u64,
    pub(crate) projections: &'a [VerificationJournalProjection],
    pub(crate) diagnostics: &'a [VerificationSummaryDiagnostic],
    pub(crate) finalizer: Option<&'a Value>,
    pub(crate) observed_tool_activity: Option<&'a VerificationObservedToolActivity>,
}

/// Stable reason codes used by verification journal payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum VerificationReasonCode {
    CommandClassified,
    CommandNotVerification,
    CommandSupported,
    EventRecorded,
    EventStatusFailed,
    EventStatusPassed,
    FreshnessChecked,
    FreshPassingEvidenceFound,
    InvalidCommand,
    NoChangedPaths,
    NoPassingEvidence,
    RequiredAfterPatch,
    ScopeUnknown,
    StateStale,
    WorkspaceMismatch,
}

impl VerificationReasonCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CommandClassified => "verification.command_classified",
            Self::CommandNotVerification => "verification.command_not_verification",
            Self::CommandSupported => "verification.command_supported",
            Self::EventRecorded => "verification.event_recorded",
            Self::EventStatusFailed => "verification.event_status_failed",
            Self::EventStatusPassed => "verification.event_status_passed",
            Self::FreshnessChecked => "verification.freshness_checked",
            Self::FreshPassingEvidenceFound => "verification.fresh_passing_evidence_found",
            Self::InvalidCommand => "verification.invalid_command",
            Self::NoChangedPaths => "verification.no_changed_paths",
            Self::NoPassingEvidence => "verification.no_passing_evidence",
            Self::RequiredAfterPatch => "verification.required_after_patch",
            Self::ScopeUnknown => "verification.scope_unknown",
            Self::StateStale => "verification.state_stale",
            Self::WorkspaceMismatch => "verification.workspace_mismatch",
        }
    }
}

/// Canonical command tokens derived from `palyra.process.run` input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CanonicalCommand {
    pub(crate) executable: String,
    pub(crate) args: Vec<String>,
    pub(crate) display: String,
}

/// Passive classification for one process-run command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationCommandClassification {
    pub(crate) schema_version: u32,
    pub(crate) canonical_command: CanonicalCommand,
    pub(crate) is_verification: bool,
    pub(crate) kind: VerificationKind,
    pub(crate) scope: VerificationScope,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Classifies `palyra.process.run` commands without executing anything.
pub(crate) struct VerificationCommandClassifier;

impl VerificationCommandClassifier {
    #[must_use]
    pub(crate) fn classify_process_run(
        input: &ProcessRunnerToolInput,
    ) -> VerificationCommandClassification {
        let canonical_command = canonical_command_from_process_input(input);
        let classification_args = normalized_command_args(input.args.as_slice());
        let (kind, scope) = classify_command_parts(
            canonical_command.executable.as_str(),
            classification_args.as_slice(),
        );
        let is_verification =
            !matches!(kind, VerificationKind::Inspect | VerificationKind::Unknown);
        let mut reason_codes = BTreeSet::new();
        reason_codes.insert(VerificationReasonCode::CommandClassified);
        if is_verification {
            reason_codes.insert(VerificationReasonCode::CommandSupported);
        } else {
            reason_codes.insert(VerificationReasonCode::CommandNotVerification);
        }
        VerificationCommandClassification {
            schema_version: VERIFICATION_SCHEMA_VERSION,
            canonical_command,
            is_verification,
            kind,
            scope,
            reason_codes: render_reason_codes(reason_codes),
            redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
        }
    }
}

/// Redacted command output summary suitable for durable journal storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationOutputSummary {
    pub(crate) text: String,
    pub(crate) truncated: bool,
    pub(crate) redacted: bool,
    pub(crate) artifact_refs: Vec<String>,
}

impl VerificationOutputSummary {
    #[must_use]
    pub(crate) fn from_redacted_text(
        text: &str,
        redacted: bool,
        artifact_refs: Vec<String>,
    ) -> Self {
        let text = text.trim();
        let truncated = text.chars().count() > MAX_OUTPUT_SUMMARY_CHARS;
        let text = if truncated {
            text.chars().take(MAX_OUTPUT_SUMMARY_CHARS).collect()
        } else {
            text.to_owned()
        };
        Self { text, truncated, redacted, artifact_refs: normalize_string_set(artifact_refs) }
    }
}

/// Immutable evidence event recorded for one observed verification attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationEvent {
    pub(crate) schema_version: u32,
    pub(crate) event_id: String,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) workspace_root: ProjectWorkspaceRootRef,
    pub(crate) command: String,
    pub(crate) canonical_command: String,
    pub(crate) kind: VerificationKind,
    pub(crate) scope: VerificationScope,
    pub(crate) status: VerificationStatus,
    pub(crate) exit_code: Option<i32>,
    pub(crate) changed_paths: Vec<String>,
    pub(crate) output_summary: VerificationOutputSummary,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Request object for building a verified event contract.
pub(crate) struct VerificationEventCreateRequest {
    pub(crate) event_id: String,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) workspace_root: ProjectWorkspaceRootRef,
    pub(crate) command: String,
    pub(crate) kind: VerificationKind,
    pub(crate) scope: VerificationScope,
    pub(crate) status: VerificationStatus,
    pub(crate) exit_code: Option<i32>,
    pub(crate) changed_paths: Vec<String>,
    pub(crate) output_summary: VerificationOutputSummary,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) evidence_refs: Vec<String>,
}

/// Validation error for malformed verification event inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VerificationEventError {
    MissingField(&'static str),
}

impl fmt::Display for VerificationEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField(field) => write!(formatter, "verification event missing {field}"),
        }
    }
}

impl std::error::Error for VerificationEventError {}

impl VerificationEvent {
    /// Builds a stable event and validates the fields required for replay.
    ///
    /// # Errors
    /// Returns [`VerificationEventError::MissingField`] when identity,
    /// command, or timing fields are blank or invalid.
    pub(crate) fn create(
        request: VerificationEventCreateRequest,
    ) -> Result<Self, VerificationEventError> {
        require_non_empty(request.event_id.as_str(), "event_id")?;
        require_non_empty(request.session_id.as_str(), "session_id")?;
        require_non_empty(request.run_id.as_str(), "run_id")?;
        require_non_empty(request.command.as_str(), "command")?;
        if request.created_at_unix_ms <= 0 {
            return Err(VerificationEventError::MissingField("created_at_unix_ms"));
        }
        let canonical_command = canonicalize_verification_command(request.command.as_str())
            .filter(|command| !command.is_empty())
            .ok_or(VerificationEventError::MissingField("canonical_command"))?;
        let mut reason_codes = BTreeSet::new();
        reason_codes.insert(VerificationReasonCode::EventRecorded);
        if request.status.is_passing() {
            reason_codes.insert(VerificationReasonCode::EventStatusPassed);
        } else if matches!(request.status, VerificationStatus::Failed) {
            reason_codes.insert(VerificationReasonCode::EventStatusFailed);
        }
        let changed_paths = normalize_string_set(
            request
                .changed_paths
                .into_iter()
                .map(|path| normalize_relative_path(path.as_str()))
                .filter(|path| path != ".")
                .collect(),
        );
        if changed_paths.is_empty() {
            reason_codes.insert(VerificationReasonCode::NoChangedPaths);
        }
        if request.scope == VerificationScope::Unknown {
            reason_codes.insert(VerificationReasonCode::ScopeUnknown);
        }
        Ok(Self {
            schema_version: VERIFICATION_SCHEMA_VERSION,
            event_id: request.event_id,
            session_id: request.session_id,
            run_id: request.run_id,
            workspace_root: request.workspace_root,
            command: request.command.trim().to_owned(),
            canonical_command,
            kind: request.kind,
            scope: request.scope,
            status: request.status,
            exit_code: request.exit_code,
            changed_paths,
            output_summary: request.output_summary,
            created_at_unix_ms: request.created_at_unix_ms,
            evidence_refs: normalize_string_set(request.evidence_refs),
            reason_codes: render_reason_codes(reason_codes),
            redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
        })
    }
}

/// Requirement that a verifier must satisfy before a final answer can rely
/// on fresh verification evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationRequirement {
    pub(crate) requirement_id: String,
    pub(crate) workspace_root: ProjectWorkspaceRootRef,
    pub(crate) required_kind: VerificationKind,
    pub(crate) changed_paths: Vec<String>,
    pub(crate) min_created_at_unix_ms: i64,
    pub(crate) reason_code: String,
}

/// Inputs used to mark patch-touched paths as requiring fresh verification.
pub(crate) struct VerificationPatchStaleRequest {
    pub(crate) workspace_root: ProjectWorkspaceRootRef,
    pub(crate) required_kinds: Vec<VerificationKind>,
    pub(crate) changed_paths: Vec<String>,
    pub(crate) changed_at_unix_ms: i64,
}

impl VerificationRequirement {
    #[must_use]
    pub(crate) fn new(
        requirement_id: &str,
        workspace_root: ProjectWorkspaceRootRef,
        required_kind: VerificationKind,
        changed_paths: Vec<String>,
        min_created_at_unix_ms: i64,
        reason_code: &str,
    ) -> Self {
        Self {
            requirement_id: requirement_id.trim().to_owned(),
            workspace_root,
            required_kind,
            changed_paths: normalize_string_set(
                changed_paths
                    .into_iter()
                    .map(|path| normalize_relative_path(path.as_str()))
                    .filter(|path| path != ".")
                    .collect(),
            ),
            min_created_at_unix_ms,
            reason_code: reason_code.trim().to_owned(),
        }
    }
}

/// Freshness decision for a requirement against the current event ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationFreshnessDecision {
    pub(crate) schema_version: u32,
    pub(crate) status: VerificationFreshnessStatus,
    pub(crate) requirement_id: String,
    pub(crate) matched_event_id: Option<String>,
    pub(crate) checked_at_unix_ms: i64,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Journal-backed state projection for one verification requirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationState {
    pub(crate) schema_version: u32,
    pub(crate) workspace_root: ProjectWorkspaceRootRef,
    pub(crate) requirement: VerificationRequirement,
    pub(crate) latest_event_id: Option<String>,
    pub(crate) latest_passing_event_id: Option<String>,
    pub(crate) freshness: VerificationFreshnessDecision,
    pub(crate) redaction_level: String,
}

/// Journal projection wrapper used for recorded/stale/freshness events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationJournalProjection {
    pub(crate) schema_version: u32,
    pub(crate) event_type: String,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) redaction_level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) classification: Option<VerificationCommandClassification>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) event: Option<VerificationEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) state: Option<VerificationState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) freshness: Option<VerificationFreshnessDecision>,
}

/// Builds the CLI/console verification status from recent journal projections.
#[must_use]
pub(crate) fn verification_status_for_cli_and_console(
    rollout_enabled: bool,
    journal_total_events: u64,
    journal_window_events: u64,
    projections: &[VerificationJournalProjection],
) -> VerificationStatusForCliAndConsoleDiagnostics {
    let mut classified_commands = 0_u64;
    let mut recorded_events = 0_u64;
    let mut passing_events = 0_u64;
    let mut failed_events = 0_u64;
    let mut stale_requirements = 0_u64;
    let mut fresh_requirements = 0_u64;
    let mut unknown_requirements = 0_u64;
    let mut latest_event_type = None;
    let mut latest_status = None;
    let mut latest_created_at_unix_ms = None;

    for projection in projections {
        if latest_created_at_unix_ms.is_none_or(|latest| projection.created_at_unix_ms >= latest) {
            latest_created_at_unix_ms = Some(projection.created_at_unix_ms);
            latest_event_type = Some(projection.event_type.clone());
            latest_status = projection_status(projection);
        }
        match projection.event_type.as_str() {
            VERIFICATION_COMMAND_CLASSIFIED => {
                classified_commands = classified_commands.saturating_add(1);
            }
            VERIFICATION_EVENT_RECORDED => {
                recorded_events = recorded_events.saturating_add(1);
                if projection.event.as_ref().is_some_and(|event| event.status.is_passing()) {
                    passing_events = passing_events.saturating_add(1);
                } else if projection.event.as_ref().is_some_and(|event| {
                    matches!(
                        event.status,
                        VerificationStatus::Failed
                            | VerificationStatus::TimedOut
                            | VerificationStatus::Cancelled
                    )
                }) {
                    failed_events = failed_events.saturating_add(1);
                }
            }
            VERIFICATION_STATE_STALE | VERIFICATION_FRESHNESS_CHECKED => {
                if let Some(status) = projection_freshness_status(projection) {
                    match status {
                        VerificationFreshnessStatus::Fresh => {
                            fresh_requirements = fresh_requirements.saturating_add(1);
                        }
                        VerificationFreshnessStatus::Stale => {
                            stale_requirements = stale_requirements.saturating_add(1);
                        }
                        VerificationFreshnessStatus::Unknown => {
                            unknown_requirements = unknown_requirements.saturating_add(1);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let decision = verification_diagnostics_decision(
        rollout_enabled,
        projections.is_empty(),
        stale_requirements,
        failed_events,
        unknown_requirements,
        fresh_requirements,
        passing_events,
    );
    let reason_codes = verification_diagnostics_reason_codes(decision);

    VerificationStatusForCliAndConsoleDiagnostics {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        decision,
        rollout_enabled,
        journal_total_events,
        journal_window_events,
        verification_projection_events: u64::try_from(projections.len()).unwrap_or(u64::MAX),
        classified_commands,
        recorded_events,
        passing_events,
        failed_events,
        stale_requirements,
        fresh_requirements,
        unknown_requirements,
        latest_event_type,
        latest_status,
        latest_created_at_unix_ms,
        reason_codes,
        journal_events: vec![
            VERIFICATION_COMMAND_CLASSIFIED.to_owned(),
            VERIFICATION_EVENT_RECORDED.to_owned(),
            VERIFICATION_STATE_STALE.to_owned(),
            VERIFICATION_FRESHNESS_CHECKED.to_owned(),
        ],
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
    }
}

/// Builds a public artifact summary from run-scoped verification journal projections.
#[must_use]
pub(crate) fn verification_summary_for_public_artifact(
    request: VerificationSummaryRequest<'_>,
) -> VerificationSummary {
    let latest_verification_status = verification_status_for_cli_and_console(
        request.rollout_enabled,
        request.journal_total_events,
        request.journal_window_events,
        request.projections,
    );
    let mut changed_files = Vec::new();
    let mut commands_executed = Vec::new();
    let mut command_classification = Vec::new();
    let mut unverified_mutations = Vec::new();
    let mut stale_evidence_reasons = Vec::new();
    let mut evidence_refs = Vec::new();
    let mut reason_codes = latest_verification_status.reason_codes.clone();

    for projection in request.projections {
        evidence_refs.extend(projection.evidence_refs.clone());
        reason_codes.extend(projection.reason_codes.clone());

        if let Some(classification) = projection.classification.as_ref() {
            command_classification.push(VerificationSummaryCommandClassification {
                command: classification.canonical_command.display.clone(),
                is_verification: classification.is_verification,
                kind: classification.kind.as_str().to_owned(),
                scope: classification.scope.as_str().to_owned(),
                created_at_unix_ms: projection.created_at_unix_ms,
                reason_codes: classification.reason_codes.clone(),
            });
            if !classification.is_verification {
                commands_executed.push(VerificationSummaryCommand {
                    command: classification.canonical_command.display.clone(),
                    is_verification: false,
                    kind: classification.kind.as_str().to_owned(),
                    scope: classification.scope.as_str().to_owned(),
                    status: None,
                    exit_code: None,
                    created_at_unix_ms: projection.created_at_unix_ms,
                    evidence_refs: projection.evidence_refs.clone(),
                    reason_codes: classification.reason_codes.clone(),
                });
            }
        }

        if let Some(event) = projection.event.as_ref() {
            changed_files.extend(event.changed_paths.iter().map(|path| public_changed_path(path)));
            commands_executed.push(VerificationSummaryCommand {
                command: event.canonical_command.clone(),
                is_verification: true,
                kind: event.kind.as_str().to_owned(),
                scope: event.scope.as_str().to_owned(),
                status: Some(event.status.as_str().to_owned()),
                exit_code: event.exit_code,
                created_at_unix_ms: event.created_at_unix_ms,
                evidence_refs: normalize_string_set(event.evidence_refs.clone()),
                reason_codes: event.reason_codes.clone(),
            });
        }

        if let Some(state) = projection.state.as_ref() {
            changed_files.extend(
                state.requirement.changed_paths.iter().map(|path| public_changed_path(path)),
            );
            if !matches!(state.freshness.status, VerificationFreshnessStatus::Fresh) {
                stale_evidence_reasons.extend(state.freshness.reason_codes.clone());
                stale_evidence_reasons.push(state.requirement.reason_code.clone());
                unverified_mutations.push(VerificationSummaryUnverifiedMutation {
                    requirement_id: state.requirement.requirement_id.clone(),
                    required_kind: state.requirement.required_kind.as_str().to_owned(),
                    changed_files: normalize_string_set(
                        state
                            .requirement
                            .changed_paths
                            .iter()
                            .map(|path| public_changed_path(path))
                            .collect(),
                    ),
                    freshness_status: state.freshness.status.as_str().to_owned(),
                    min_created_at_unix_ms: state.requirement.min_created_at_unix_ms,
                    reason_codes: normalize_string_set(
                        state
                            .freshness
                            .reason_codes
                            .iter()
                            .cloned()
                            .chain(std::iter::once(state.requirement.reason_code.clone()))
                            .collect(),
                    ),
                });
            }
        }

        if let Some(freshness) = projection.freshness.as_ref() {
            if !matches!(freshness.status, VerificationFreshnessStatus::Fresh) {
                stale_evidence_reasons.extend(freshness.reason_codes.clone());
            }
        }
    }

    for diagnostic in request.diagnostics {
        evidence_refs.extend(diagnostic.evidence_refs.clone());
        reason_codes.extend(diagnostic.reason_codes.clone());
        if diagnostic.degraded || diagnostic.new_errors > 0 {
            stale_evidence_reasons.extend(diagnostic.reason_codes.clone());
        }
    }

    if let Some(activity) = request.observed_tool_activity {
        changed_files.extend(activity.changed_files.iter().map(|path| public_changed_path(path)));
        for observed in &activity.commands_executed {
            let already_projected = commands_executed.iter().any(|existing| {
                existing.command == observed.command
                    && existing.status == observed.status
                    && existing
                        .evidence_refs
                        .iter()
                        .any(|reference| observed.evidence_refs.contains(reference))
            });
            if !already_projected {
                commands_executed.push(observed.clone());
            }
        }
        for observed in &activity.command_classification {
            if !command_classification.contains(observed) {
                command_classification.push(observed.clone());
            }
        }
        evidence_refs.extend(activity.evidence_refs.clone());
        reason_codes.extend(activity.reason_codes.clone());
        if !activity.complete {
            stale_evidence_reasons.push(VERIFICATION_OBSERVED_TOOL_ACTIVITY_INCOMPLETE.to_owned());
        }
    }

    let final_answer = reconcile_final_answer_with_observed_tool_activity(
        final_answer_summary_from_finalizer(request.finalizer),
        request.rollout_enabled,
        request.observed_tool_activity,
    );
    evidence_refs.extend(final_answer.evidence_refs.clone());
    if let Some(reason_code) = final_answer.reason_code.as_ref() {
        reason_codes.push(reason_code.clone());
        if !final_answer.allowed {
            stale_evidence_reasons.push(reason_code.clone());
        }
    }

    let state = verification_summary_state(
        request.rollout_enabled,
        request.projections,
        request.diagnostics,
        &final_answer,
    );
    let final_answer_allowed = final_answer.allowed;
    let final_answer_allowed_because = final_answer.allowed_because.clone();

    VerificationSummary {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        state,
        rollout_enabled: request.rollout_enabled,
        changed_files: normalize_string_set(changed_files),
        commands_executed,
        command_classification,
        latest_verification_status,
        unverified_mutations,
        stale_evidence_reasons: normalize_string_set(stale_evidence_reasons),
        diagnostics: request.diagnostics.to_vec(),
        final_answer,
        final_answer_allowed,
        final_answer_allowed_because,
        evidence_refs: normalize_string_set(evidence_refs),
        reason_codes: normalize_string_set(reason_codes),
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
    }
}

/// Parses a verification projection from a redacted journal payload.
#[must_use]
pub(crate) fn verification_projection_from_payload(
    payload: &Value,
) -> Option<VerificationJournalProjection> {
    let event_type = payload.get("event_type").and_then(Value::as_str)?;
    if !matches!(
        event_type,
        VERIFICATION_COMMAND_CLASSIFIED
            | VERIFICATION_EVENT_RECORDED
            | VERIFICATION_STATE_STALE
            | VERIFICATION_FRESHNESS_CHECKED
    ) {
        return None;
    }
    serde_json::from_value::<VerificationJournalProjection>(payload.clone()).ok()
}

/// Parses a public code-intelligence diagnostics row from a redacted journal payload.
#[must_use]
pub(crate) fn verification_diagnostic_from_payload(
    payload: &Value,
) -> Option<VerificationSummaryDiagnostic> {
    let event_type = payload.get("event").and_then(Value::as_str)?;
    if !event_type.contains("diagnostics.delta") {
        return None;
    }
    Some(VerificationSummaryDiagnostic {
        event_type: event_type.to_owned(),
        new_errors: payload.get("new_errors").and_then(Value::as_u64).unwrap_or(0),
        new_warnings: payload.get("new_warnings").and_then(Value::as_u64).unwrap_or(0),
        degraded: payload.get("degraded").and_then(Value::as_bool).unwrap_or(false),
        reason_codes: json_string_array(payload.get("reason_codes")),
        evidence_refs: json_string_array(payload.get("evidence_refs")),
    })
}

fn verification_summary_state(
    rollout_enabled: bool,
    projections: &[VerificationJournalProjection],
    diagnostics: &[VerificationSummaryDiagnostic],
    final_answer: &VerificationSummaryFinalAnswer,
) -> String {
    if !rollout_enabled {
        return "disabled".to_owned();
    }
    if projections.is_empty() && diagnostics.is_empty() && !final_answer.observed {
        return "not_available".to_owned();
    }
    "available".to_owned()
}

fn json_string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).map(str::to_owned).collect())
        .unwrap_or_default()
}

fn final_answer_summary_from_finalizer(
    finalizer: Option<&Value>,
) -> VerificationSummaryFinalAnswer {
    let Some(finalizer) = finalizer else {
        return VerificationSummaryFinalAnswer {
            observed: false,
            status: None,
            reason_code: None,
            allowed: false,
            allowed_because: "verification.finalizer.not_observed".to_owned(),
            pending_requirement_count: None,
            satisfied_requirement_count: None,
            evidence_refs: Vec::new(),
            nudge: None,
            unverified_reason: None,
        };
    };

    let status = finalizer.get("status").and_then(Value::as_str).map(str::to_owned);
    let reason_code = finalizer.get("reason_code").and_then(Value::as_str).map(str::to_owned);
    let unverified_reason =
        finalizer.get("unverified_reason").and_then(Value::as_str).map(str::to_owned);
    let pending_requirement_count =
        finalizer.get("pending_requirement_count").and_then(Value::as_u64);
    let satisfied_requirement_count =
        finalizer.get("satisfied_requirement_count").and_then(Value::as_u64);
    let evidence_refs = finalizer
        .get("evidence_refs")
        .and_then(Value::as_array)
        .map(|refs| refs.iter().filter_map(Value::as_str).map(str::to_owned).collect::<Vec<_>>())
        .unwrap_or_default();
    let nudge = finalizer.get("nudge").and_then(Value::as_str).map(str::to_owned);

    let allowed =
        matches!(status.as_deref(), Some("not_required" | "verified" | "unverified_allowed"));
    let allowed_because = match status.as_deref() {
        Some("verified") => "verification.finalizer.fresh_verification_found".to_owned(),
        Some("unverified_allowed") => unverified_reason
            .as_ref()
            .map(|reason| format!("verification.finalizer.unverified_allowed:{reason}"))
            .unwrap_or_else(|| "verification.finalizer.unverified_allowed".to_owned()),
        Some("not_required") => {
            reason_code.clone().unwrap_or_else(|| "verification.finalizer.not_required".to_owned())
        }
        Some("nudge_required") => reason_code
            .clone()
            .unwrap_or_else(|| "verification.finalizer.nudge_required".to_owned()),
        _ => reason_code.clone().unwrap_or_else(|| "verification.finalizer.unknown".to_owned()),
    };

    VerificationSummaryFinalAnswer {
        observed: true,
        status,
        reason_code,
        allowed,
        allowed_because,
        pending_requirement_count,
        satisfied_requirement_count,
        evidence_refs: normalize_string_set(evidence_refs),
        nudge,
        unverified_reason,
    }
}

fn reconcile_final_answer_with_observed_tool_activity(
    mut final_answer: VerificationSummaryFinalAnswer,
    rollout_enabled: bool,
    observed_tool_activity: Option<&VerificationObservedToolActivity>,
) -> VerificationSummaryFinalAnswer {
    let Some(activity) = observed_tool_activity else {
        return final_answer;
    };
    if activity.changed_files.is_empty()
        || final_answer.reason_code.as_deref() != Some(VERIFICATION_FINALIZER_NO_CODE_MUTATION)
    {
        return final_answer;
    }

    let reason_code = if rollout_enabled {
        VERIFICATION_FINALIZER_OBSERVED_MUTATION_REQUIRES_VERIFICATION
    } else {
        VERIFICATION_FINALIZER_ROLLOUT_DISABLED_WITH_OBSERVED_MUTATION
    };
    final_answer.observed = true;
    final_answer.status =
        Some(if rollout_enabled { "nudge_required" } else { "unverified_allowed" }.to_owned());
    final_answer.reason_code = Some(reason_code.to_owned());
    final_answer.allowed = !rollout_enabled;
    final_answer.allowed_because = reason_code.to_owned();
    final_answer.evidence_refs = normalize_string_set(
        final_answer
            .evidence_refs
            .into_iter()
            .chain(activity.evidence_refs.iter().cloned())
            .collect(),
    );
    if rollout_enabled {
        final_answer.nudge = Some(
            "Run verification after the observed code mutation before relying on the final answer."
                .to_owned(),
        );
        final_answer.unverified_reason = None;
    } else {
        final_answer.nudge = None;
        final_answer.unverified_reason = Some(VERIFICATION_STATUS_ROLLOUT_DISABLED.to_owned());
    }
    final_answer
}

fn projection_status(projection: &VerificationJournalProjection) -> Option<String> {
    projection
        .event
        .as_ref()
        .map(|event| event.status.as_str().to_owned())
        .or_else(|| {
            projection.state.as_ref().map(|state| state.freshness.status.as_str().to_owned())
        })
        .or_else(|| {
            projection.freshness.as_ref().map(|freshness| freshness.status.as_str().to_owned())
        })
}

fn projection_freshness_status(
    projection: &VerificationJournalProjection,
) -> Option<VerificationFreshnessStatus> {
    projection
        .state
        .as_ref()
        .map(|state| state.freshness.status)
        .or_else(|| projection.freshness.as_ref().map(|freshness| freshness.status))
}

fn verification_diagnostics_decision(
    rollout_enabled: bool,
    no_recent_events: bool,
    stale_requirements: u64,
    failed_events: u64,
    unknown_requirements: u64,
    fresh_requirements: u64,
    passing_events: u64,
) -> VerificationDiagnosticsDecision {
    if !rollout_enabled {
        return VerificationDiagnosticsDecision::Disabled;
    }
    if no_recent_events {
        return VerificationDiagnosticsDecision::NoEvidence;
    }
    if stale_requirements > 0 {
        return VerificationDiagnosticsDecision::Stale;
    }
    if failed_events > 0 {
        return VerificationDiagnosticsDecision::Failed;
    }
    if unknown_requirements > 0 {
        return VerificationDiagnosticsDecision::Unknown;
    }
    if fresh_requirements > 0 || passing_events > 0 {
        return VerificationDiagnosticsDecision::Fresh;
    }
    VerificationDiagnosticsDecision::Unknown
}

fn verification_diagnostics_reason_codes(decision: VerificationDiagnosticsDecision) -> Vec<String> {
    match decision {
        VerificationDiagnosticsDecision::Disabled => {
            vec![VERIFICATION_STATUS_ROLLOUT_DISABLED.to_owned()]
        }
        VerificationDiagnosticsDecision::NoEvidence => {
            vec![VERIFICATION_STATUS_NO_RECENT_EVENTS.to_owned()]
        }
        VerificationDiagnosticsDecision::Stale => {
            vec![VERIFICATION_STATUS_RECENT_STALE_REQUIREMENT.to_owned()]
        }
        VerificationDiagnosticsDecision::Failed => {
            vec![VERIFICATION_STATUS_RECENT_FAILED_EVENT.to_owned()]
        }
        VerificationDiagnosticsDecision::Unknown => {
            vec![VERIFICATION_STATUS_RECENT_UNKNOWN_REQUIREMENT.to_owned()]
        }
        VerificationDiagnosticsDecision::Fresh => {
            vec![VERIFICATION_STATUS_RECENT_FRESH_EVIDENCE.to_owned()]
        }
    }
}

#[must_use]
pub(crate) fn verification_command_classified_projection(
    session_id: &str,
    run_id: &str,
    created_at_unix_ms: i64,
    classification: VerificationCommandClassification,
) -> VerificationJournalProjection {
    VerificationJournalProjection {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        event_type: VERIFICATION_COMMAND_CLASSIFIED.to_owned(),
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        created_at_unix_ms,
        reason_codes: classification.reason_codes.clone(),
        evidence_refs: Vec::new(),
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
        classification: Some(classification),
        event: None,
        state: None,
        freshness: None,
    }
}

#[must_use]
pub(crate) fn verification_event_recorded_projection(
    event: VerificationEvent,
) -> VerificationJournalProjection {
    VerificationJournalProjection {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        event_type: VERIFICATION_EVENT_RECORDED.to_owned(),
        session_id: event.session_id.clone(),
        run_id: event.run_id.clone(),
        created_at_unix_ms: event.created_at_unix_ms,
        reason_codes: event.reason_codes.clone(),
        evidence_refs: event.evidence_refs.clone(),
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
        classification: None,
        event: Some(event),
        state: None,
        freshness: None,
    }
}

#[must_use]
pub(crate) fn verification_freshness_checked_projection(
    session_id: &str,
    run_id: &str,
    decision: VerificationFreshnessDecision,
) -> VerificationJournalProjection {
    VerificationJournalProjection {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        event_type: VERIFICATION_FRESHNESS_CHECKED.to_owned(),
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        created_at_unix_ms: decision.checked_at_unix_ms,
        reason_codes: decision.reason_codes.clone(),
        evidence_refs: decision
            .matched_event_id
            .as_ref()
            .map(|event_id| vec![format!("verification_event:{event_id}")])
            .unwrap_or_default(),
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
        classification: None,
        event: None,
        state: None,
        freshness: Some(decision),
    }
}

#[must_use]
pub(crate) fn verification_state_stale_projection(
    session_id: &str,
    run_id: &str,
    state: VerificationState,
) -> VerificationJournalProjection {
    let mut reason_codes = state.freshness.reason_codes.clone();
    reason_codes.push(state.requirement.reason_code.clone());
    VerificationJournalProjection {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        event_type: VERIFICATION_STATE_STALE.to_owned(),
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        created_at_unix_ms: state.freshness.checked_at_unix_ms,
        reason_codes: normalize_string_set(reason_codes),
        evidence_refs: state
            .latest_event_id
            .as_ref()
            .map(|event_id| vec![format!("verification_event:{event_id}")])
            .unwrap_or_default(),
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
        classification: None,
        event: None,
        state: Some(state),
        freshness: None,
    }
}

/// Builds stale verification states after a successful workspace mutation.
#[must_use]
pub(crate) fn build_patch_stale_verification_states(
    request: VerificationPatchStaleRequest,
) -> Vec<VerificationState> {
    let changed_paths = normalize_string_set(request.changed_paths);
    if changed_paths.is_empty() {
        return Vec::new();
    }
    normalize_verification_kinds(request.required_kinds)
        .into_iter()
        .map(|required_kind| {
            let requirement = VerificationRequirement::new(
                format!("verification.required_after_patch.{}", required_kind.as_str()).as_str(),
                request.workspace_root.clone(),
                required_kind,
                changed_paths.clone(),
                request.changed_at_unix_ms,
                VerificationReasonCode::RequiredAfterPatch.as_str(),
            );
            build_verification_state(requirement, &[], request.changed_at_unix_ms)
        })
        .collect()
}

/// Adds model-visible, non-instructional verification freshness metadata to a patch output.
pub(crate) fn append_verification_stale_output(
    output_value: &mut Value,
    states: Vec<VerificationState>,
) {
    if states.is_empty() {
        return;
    }
    let Some(payload) = output_value.as_object_mut() else {
        return;
    };
    let coding_posture = payload.entry("coding_posture").or_insert_with(|| {
        json!({
            "schema_version": VERIFICATION_SCHEMA_VERSION,
            "instruction_authority": "none",
            "redaction_level": VERIFICATION_REDACTION_LEVEL,
        })
    });
    let Some(coding_posture) = coding_posture.as_object_mut() else {
        return;
    };
    coding_posture.insert(
        "verification".to_owned(),
        json!({
            "schema_version": VERIFICATION_SCHEMA_VERSION,
            "instruction_authority": "none",
            "freshness_status": VerificationFreshnessStatus::Stale,
            "requirements": states,
            "redaction_level": VERIFICATION_REDACTION_LEVEL,
        }),
    );
}

#[must_use]
pub(crate) fn build_verification_state(
    requirement: VerificationRequirement,
    events: &[VerificationEvent],
    checked_at_unix_ms: i64,
) -> VerificationState {
    let latest_event = events.iter().max_by_key(|event| event.created_at_unix_ms);
    let latest_passing_event = events
        .iter()
        .filter(|event| event.status.is_passing())
        .max_by_key(|event| event.created_at_unix_ms);
    let freshness = verification_freshness_decision(&requirement, events, checked_at_unix_ms);
    VerificationState {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        workspace_root: requirement.workspace_root.clone(),
        requirement,
        latest_event_id: latest_event.map(|event| event.event_id.clone()),
        latest_passing_event_id: latest_passing_event.map(|event| event.event_id.clone()),
        freshness,
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
    }
}

#[must_use]
pub(crate) fn verification_freshness_decision(
    requirement: &VerificationRequirement,
    events: &[VerificationEvent],
    checked_at_unix_ms: i64,
) -> VerificationFreshnessDecision {
    let mut reason_codes = BTreeSet::new();
    reason_codes.insert(VerificationReasonCode::FreshnessChecked);
    if requirement.changed_paths.is_empty() {
        reason_codes.insert(VerificationReasonCode::NoChangedPaths);
    }
    let mut saw_unknown_coverage = false;
    let mut saw_workspace_mismatch = false;
    for event in events.iter().filter(|event| {
        event.kind == requirement.required_kind
            && event.status.is_passing()
            && event.created_at_unix_ms >= requirement.min_created_at_unix_ms
    }) {
        if event.workspace_root.root_id_sha256 != requirement.workspace_root.root_id_sha256 {
            saw_workspace_mismatch = true;
            continue;
        }
        match verification_event_covers_requirement(event, requirement) {
            CoverageDecision::Covered => {
                reason_codes.insert(VerificationReasonCode::FreshPassingEvidenceFound);
                return VerificationFreshnessDecision {
                    schema_version: VERIFICATION_SCHEMA_VERSION,
                    status: VerificationFreshnessStatus::Fresh,
                    requirement_id: requirement.requirement_id.clone(),
                    matched_event_id: Some(event.event_id.clone()),
                    checked_at_unix_ms,
                    reason_codes: render_reason_codes(reason_codes),
                    redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
                };
            }
            CoverageDecision::Unknown => {
                saw_unknown_coverage = true;
            }
            CoverageDecision::NotCovered => {}
        }
    }
    if saw_workspace_mismatch {
        reason_codes.insert(VerificationReasonCode::WorkspaceMismatch);
    }
    if saw_unknown_coverage {
        reason_codes.insert(VerificationReasonCode::ScopeUnknown);
        return VerificationFreshnessDecision {
            schema_version: VERIFICATION_SCHEMA_VERSION,
            status: VerificationFreshnessStatus::Unknown,
            requirement_id: requirement.requirement_id.clone(),
            matched_event_id: None,
            checked_at_unix_ms,
            reason_codes: render_reason_codes(reason_codes),
            redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
        };
    }
    reason_codes.insert(VerificationReasonCode::NoPassingEvidence);
    reason_codes.insert(VerificationReasonCode::StateStale);
    VerificationFreshnessDecision {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        status: VerificationFreshnessStatus::Stale,
        requirement_id: requirement.requirement_id.clone(),
        matched_event_id: None,
        checked_at_unix_ms,
        reason_codes: render_reason_codes(reason_codes),
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoverageDecision {
    Covered,
    NotCovered,
    Unknown,
}

fn verification_event_covers_requirement(
    event: &VerificationEvent,
    requirement: &VerificationRequirement,
) -> CoverageDecision {
    match event.scope {
        VerificationScope::Workspace => CoverageDecision::Covered,
        VerificationScope::Unknown => CoverageDecision::Unknown,
        VerificationScope::ChangedPaths | VerificationScope::PathSet => {
            if requirement.changed_paths.is_empty() {
                return CoverageDecision::Covered;
            }
            if event.changed_paths.is_empty() {
                return CoverageDecision::Unknown;
            }
            let event_paths = event.changed_paths.iter().collect::<BTreeSet<_>>();
            if requirement.changed_paths.iter().all(|path| event_paths.contains(path)) {
                CoverageDecision::Covered
            } else {
                CoverageDecision::NotCovered
            }
        }
    }
}

fn canonical_command_from_process_input(input: &ProcessRunnerToolInput) -> CanonicalCommand {
    let executable = normalize_executable_token(input.command.as_str());
    let args = redacted_command_args(input.args.as_slice());
    let display = std::iter::once(executable.as_str())
        .chain(args.iter().map(String::as_str))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    CanonicalCommand { executable, args, display }
}

fn classify_command_parts(
    executable: &str,
    args: &[String],
) -> (VerificationKind, VerificationScope) {
    match executable {
        "cargo" => classify_cargo(args),
        "npm" | "pnpm" | "yarn" => classify_node_package_script(args),
        "pytest" => (VerificationKind::Test, VerificationScope::Workspace),
        "ruff" => classify_ruff(args),
        "mypy" | "tsc" => (VerificationKind::Typecheck, VerificationScope::Workspace),
        "go" => classify_go(args),
        "gradle" | "gradlew" => classify_gradle(args),
        "make" => classify_make(args),
        "python" | "python3" | "py" => classify_python_module(args),
        "git" => classify_git(args),
        "ls" | "dir" | "cat" | "type" | "rg" | "grep" => {
            (VerificationKind::Inspect, VerificationScope::Workspace)
        }
        _ => (VerificationKind::Unknown, VerificationScope::Unknown),
    }
}

fn classify_cargo(args: &[String]) -> (VerificationKind, VerificationScope) {
    match first_non_option_arg(args).as_deref() {
        Some("test") => (VerificationKind::Test, VerificationScope::Workspace),
        Some("check") => (VerificationKind::Check, VerificationScope::Workspace),
        Some("clippy") => (VerificationKind::Lint, VerificationScope::Workspace),
        Some("fmt") => (VerificationKind::Format, VerificationScope::Workspace),
        Some("build") => (VerificationKind::Build, VerificationScope::Workspace),
        _ => (VerificationKind::Unknown, VerificationScope::Unknown),
    }
}

fn classify_node_package_script(args: &[String]) -> (VerificationKind, VerificationScope) {
    let Some(script) = node_script_name(args) else {
        return (VerificationKind::Unknown, VerificationScope::Unknown);
    };
    classify_script_name(script.as_str())
}

fn classify_ruff(args: &[String]) -> (VerificationKind, VerificationScope) {
    match first_non_option_arg(args).as_deref() {
        Some("format") => (VerificationKind::Format, VerificationScope::Workspace),
        Some("check") | None => (VerificationKind::Lint, VerificationScope::Workspace),
        _ => (VerificationKind::Lint, VerificationScope::Workspace),
    }
}

fn classify_go(args: &[String]) -> (VerificationKind, VerificationScope) {
    match first_non_option_arg(args).as_deref() {
        Some("test") => (VerificationKind::Test, VerificationScope::Workspace),
        Some("build") => (VerificationKind::Build, VerificationScope::Workspace),
        _ => (VerificationKind::Unknown, VerificationScope::Unknown),
    }
}

fn classify_gradle(args: &[String]) -> (VerificationKind, VerificationScope) {
    let tasks = args.iter().filter(|arg| !arg.starts_with('-')).collect::<Vec<_>>();
    if tasks.iter().any(|task| task.as_str() == "test" || task.ends_with(":test")) {
        return (VerificationKind::Test, VerificationScope::Workspace);
    }
    if tasks.iter().any(|task| task.as_str() == "check" || task.ends_with(":check")) {
        return (VerificationKind::Check, VerificationScope::Workspace);
    }
    if tasks.iter().any(|task| task.as_str() == "build" || task.ends_with(":build")) {
        return (VerificationKind::Build, VerificationScope::Workspace);
    }
    (VerificationKind::Unknown, VerificationScope::Unknown)
}

fn classify_make(args: &[String]) -> (VerificationKind, VerificationScope) {
    let target = first_non_option_arg(args).unwrap_or_else(|| "all".to_owned());
    classify_script_name(target.as_str())
}

fn classify_python_module(args: &[String]) -> (VerificationKind, VerificationScope) {
    let module = args.windows(2).find(|window| window[0] == "-m").map(|window| window[1].as_str());
    match module {
        Some("pytest") => (VerificationKind::Test, VerificationScope::Workspace),
        Some("mypy") => (VerificationKind::Typecheck, VerificationScope::Workspace),
        Some("ruff") => (VerificationKind::Lint, VerificationScope::Workspace),
        _ => (VerificationKind::Unknown, VerificationScope::Unknown),
    }
}

fn classify_git(args: &[String]) -> (VerificationKind, VerificationScope) {
    match first_non_option_arg(args).as_deref() {
        Some("diff" | "status" | "log" | "show" | "grep" | "ls-files") => {
            (VerificationKind::Inspect, VerificationScope::Workspace)
        }
        _ => (VerificationKind::Unknown, VerificationScope::Unknown),
    }
}

fn classify_script_name(script: &str) -> (VerificationKind, VerificationScope) {
    let normalized = script.to_ascii_lowercase();
    if normalized == "test" || normalized.starts_with("test:") || normalized.ends_with(":test") {
        (VerificationKind::Test, VerificationScope::Workspace)
    } else if normalized == "lint"
        || normalized.starts_with("lint:")
        || normalized.ends_with(":lint")
    {
        (VerificationKind::Lint, VerificationScope::Workspace)
    } else if normalized == "typecheck"
        || normalized == "type-check"
        || normalized == "tsc"
        || normalized.contains("typecheck")
    {
        (VerificationKind::Typecheck, VerificationScope::Workspace)
    } else if normalized == "format"
        || normalized == "fmt"
        || normalized.starts_with("format:")
        || normalized.ends_with(":format")
    {
        (VerificationKind::Format, VerificationScope::Workspace)
    } else if normalized == "build"
        || normalized.starts_with("build:")
        || normalized.ends_with(":build")
    {
        (VerificationKind::Build, VerificationScope::Workspace)
    } else if normalized == "check"
        || normalized.starts_with("check:")
        || normalized.ends_with(":check")
        || normalized.ends_with(":ci")
    {
        (VerificationKind::Check, VerificationScope::Workspace)
    } else {
        (VerificationKind::Unknown, VerificationScope::Unknown)
    }
}

fn node_script_name(args: &[String]) -> Option<String> {
    let first = first_non_option_arg(args)?;
    if first == "run" || first == "run-script" {
        args.iter().skip_while(|arg| arg.as_str() != first).nth(1).cloned()
    } else {
        Some(first)
    }
}

fn first_non_option_arg(args: &[String]) -> Option<String> {
    let mut skip_next = false;
    for arg in args {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg == "--" {
            continue;
        }
        if arg.starts_with('-') {
            skip_next = command_option_takes_value(arg.as_str());
            continue;
        }
        return Some(arg.clone());
    }
    None
}

fn normalize_executable_token(value: &str) -> String {
    let trimmed = value.trim().trim_matches('"').trim_matches('\'');
    let file_name = trimmed.rsplit(['/', '\\']).next().unwrap_or(trimmed);
    let file_name = file_name.strip_suffix(".exe").unwrap_or(file_name);
    file_name
        .strip_prefix("./")
        .unwrap_or(file_name)
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase()
}

fn normalize_command_arg(value: &str) -> String {
    value.trim().trim_matches('"').trim_matches('\'').to_owned()
}

fn normalized_command_args(args: &[String]) -> Vec<String> {
    args.iter().map(|arg| normalize_command_arg(arg)).collect()
}

fn redacted_command_args(args: &[String]) -> Vec<String> {
    let mut redacted_args = Vec::with_capacity(args.len());
    let mut redact_next = false;
    for arg in normalized_command_args(args) {
        if redact_next {
            redacted_args.push(REDACTED.to_owned());
            redact_next = false;
            continue;
        }
        redact_next =
            command_option_takes_value(arg.as_str()) && !command_arg_has_inline_value(arg.as_str());
        redacted_args.push(redact_diagnostic_text(arg.as_str()));
    }
    redacted_args
}

fn command_option_takes_value(arg: &str) -> bool {
    if command_arg_has_inline_value(arg) {
        return false;
    }
    let key = arg.trim_start_matches('-').trim_start_matches('/');
    if key.is_empty() {
        return false;
    }
    if is_sensitive_key(key) {
        return true;
    }
    matches!(
        key,
        "C" | "c"
            | "config"
            | "cwd"
            | "dir"
            | "directory"
            | "f"
            | "file"
            | "filter"
            | "manifest-path"
            | "o"
            | "output"
            | "p"
            | "package"
            | "prefix"
            | "project"
            | "target"
            | "workspace"
    )
}

fn command_arg_has_inline_value(arg: &str) -> bool {
    arg.contains('=') || arg.contains(':')
}

fn normalize_verification_kinds(kinds: Vec<VerificationKind>) -> Vec<VerificationKind> {
    kinds
        .into_iter()
        .filter(|kind| *kind != VerificationKind::Unknown)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[must_use]
pub(crate) fn canonicalize_verification_command(command: &str) -> Option<String> {
    let tokens = command.split_whitespace().filter(|token| !token.is_empty()).collect::<Vec<_>>();
    (!tokens.is_empty()).then(|| tokens.join(" "))
}

fn require_non_empty(value: &str, field: &'static str) -> Result<(), VerificationEventError> {
    if value.trim().is_empty() {
        Err(VerificationEventError::MissingField(field))
    } else {
        Ok(())
    }
}

fn normalize_relative_path(path: &str) -> String {
    let mut parts = Vec::new();
    for component in std::path::Path::new(path).components() {
        match component {
            Component::Normal(value) => {
                if let Some(part) = value.to_str().filter(|value| !value.is_empty()) {
                    parts.push(part.to_owned());
                }
            }
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                parts.push("_outside_workspace".to_owned());
            }
        }
    }
    if parts.is_empty() {
        ".".to_owned()
    } else {
        parts.join("/")
    }
}

fn public_changed_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return ".".to_owned();
    }
    let path = trimmed.replace('\\', "/");
    let lower = path.to_ascii_lowercase();
    let has_drive_prefix = path.as_bytes().get(1).is_some_and(|byte| *byte == b':');
    let looks_absolute = path.starts_with('/') || has_drive_prefix || path.starts_with("~/");
    let contains_private_home =
        lower.contains("/users/") || lower.contains("/home/") || lower.contains("/desktop/");
    if looks_absolute || contains_private_home {
        return "<redacted:path>".to_owned();
    }
    let normalized = normalize_relative_path(path.as_str());
    if normalized == "." || normalized.starts_with("_outside_workspace") {
        return "<redacted:path>".to_owned();
    }
    normalized
}

fn normalize_string_set(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn render_reason_codes(reason_codes: BTreeSet<VerificationReasonCode>) -> Vec<String> {
    reason_codes.into_iter().map(VerificationReasonCode::as_str).map(str::to_owned).collect()
}

#[cfg(test)]
mod tests {
    use crate::application::project_facts::ProjectWorkspaceRootRef;

    use super::{
        append_verification_stale_output, build_patch_stale_verification_states,
        build_verification_state, canonicalize_verification_command,
        verification_command_classified_projection, verification_event_recorded_projection,
        verification_state_stale_projection, verification_status_for_cli_and_console,
        verification_summary_for_public_artifact, VerificationCommandClassifier,
        VerificationDiagnosticsDecision, VerificationEvent, VerificationEventCreateRequest,
        VerificationFreshnessStatus, VerificationKind, VerificationOutputSummary,
        VerificationPatchStaleRequest, VerificationRequirement, VerificationScope,
        VerificationStatus, VerificationSummaryDiagnostic, VerificationSummaryRequest,
        VERIFICATION_COMMAND_CLASSIFIED, VERIFICATION_EVENT_RECORDED, VERIFICATION_REDACTION_LEVEL,
        VERIFICATION_SCHEMA_VERSION, VERIFICATION_STATE_STALE,
    };
    use palyra_common::process_runner_input::ProcessRunnerToolInput;
    use serde_json::json;

    fn root_ref(id: &str) -> ProjectWorkspaceRootRef {
        ProjectWorkspaceRootRef {
            index: 0,
            root_id_sha256: id.to_owned(),
            display_name: "workspace".to_owned(),
            exists: true,
        }
    }

    fn event(
        event_id: &str,
        status: VerificationStatus,
        scope: VerificationScope,
        changed_paths: Vec<&str>,
        created_at_unix_ms: i64,
    ) -> VerificationEvent {
        VerificationEvent::create(VerificationEventCreateRequest {
            event_id: event_id.to_owned(),
            session_id: "session-1".to_owned(),
            run_id: "run-1".to_owned(),
            workspace_root: root_ref("root-a"),
            command: "cargo   test --workspace".to_owned(),
            kind: VerificationKind::Test,
            scope,
            status,
            exit_code: Some(if status == VerificationStatus::Passed { 0 } else { 1 }),
            changed_paths: changed_paths.into_iter().map(str::to_owned).collect(),
            output_summary: VerificationOutputSummary::from_redacted_text(
                "test result: ok",
                false,
                vec!["artifact:stdout".to_owned()],
            ),
            created_at_unix_ms,
            evidence_refs: vec!["tool_call:process-1".to_owned()],
        })
        .expect("event should be valid")
    }

    fn process_input(command: &str, args: &[&str]) -> ProcessRunnerToolInput {
        ProcessRunnerToolInput {
            command: command.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            cwd: None,
            env: Default::default(),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: Default::default(),
            keep_running_after_run: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        }
    }

    #[test]
    fn process_run_classifier_recognizes_required_verification_commands() {
        for (command, args, expected_kind) in [
            ("cargo", vec!["test"], VerificationKind::Test),
            ("cargo", vec!["check"], VerificationKind::Check),
            ("cargo", vec!["build"], VerificationKind::Build),
            ("npm", vec!["test"], VerificationKind::Test),
            ("npm", vec!["run", "lint"], VerificationKind::Lint),
            ("pnpm", vec!["test"], VerificationKind::Test),
            ("yarn", vec!["test"], VerificationKind::Test),
            ("pytest", vec![], VerificationKind::Test),
            ("ruff", vec!["check"], VerificationKind::Lint),
            ("mypy", vec!["src"], VerificationKind::Typecheck),
            ("tsc", vec!["--noEmit"], VerificationKind::Typecheck),
            ("go", vec!["test", "./..."], VerificationKind::Test),
            ("./gradlew", vec!["test"], VerificationKind::Test),
            ("make", vec!["check"], VerificationKind::Check),
        ] {
            let classification = VerificationCommandClassifier::classify_process_run(
                &process_input(command, args.as_slice()),
            );
            assert!(
                classification.is_verification,
                "{command} {args:?} should be classified as verification"
            );
            assert_eq!(classification.kind, expected_kind, "{command} {args:?}");
            assert_eq!(classification.scope, VerificationScope::Workspace);
        }
    }

    #[test]
    fn process_run_classifier_distinguishes_non_verification_commands() {
        let classification = VerificationCommandClassifier::classify_process_run(&process_input(
            "npm",
            &["install"],
        ));

        assert!(!classification.is_verification);
        assert_eq!(classification.kind, VerificationKind::Unknown);
        assert_eq!(classification.scope, VerificationScope::Unknown);
        assert!(classification
            .reason_codes
            .iter()
            .any(|code| code == "verification.command_not_verification"));
    }

    #[test]
    fn process_run_classifier_recognizes_inspect_without_counting_as_verification() {
        let classification =
            VerificationCommandClassifier::classify_process_run(&process_input("git", &["status"]));

        assert_eq!(classification.kind, VerificationKind::Inspect);
        assert_eq!(classification.scope, VerificationScope::Workspace);
        assert!(!classification.is_verification);
        assert!(classification
            .reason_codes
            .iter()
            .any(|code| code == "verification.command_not_verification"));
    }

    #[test]
    fn process_run_classifier_redacts_secret_args_without_losing_classification() {
        let classification = VerificationCommandClassifier::classify_process_run(&process_input(
            "npm",
            &["--token", "sk-secret-token", "test"],
        ));

        assert!(classification.is_verification);
        assert_eq!(classification.kind, VerificationKind::Test);
        assert_eq!(classification.canonical_command.args, vec!["--token", "<redacted>", "test"]);
        assert!(!classification.canonical_command.display.contains("sk-secret-token"));
    }

    #[test]
    fn command_classified_projection_keeps_unknown_as_classification_only() {
        let classification = VerificationCommandClassifier::classify_process_run(&process_input(
            "npm",
            &["install"],
        ));
        let projection =
            verification_command_classified_projection("session-1", "run-1", 100, classification);
        let value = serde_json::to_value(&projection).expect("projection should serialize");

        assert_eq!(value["event_type"], VERIFICATION_COMMAND_CLASSIFIED);
        assert_eq!(value["classification"]["is_verification"], false);
        assert!(value.get("event").is_none());
    }

    #[test]
    fn verification_event_serializes_stable_contract() {
        let event = event(
            "event-1",
            VerificationStatus::Passed,
            VerificationScope::ChangedPaths,
            vec!["src/lib.rs"],
            100,
        );

        assert_eq!(event.schema_version, VERIFICATION_SCHEMA_VERSION);
        assert_eq!(event.canonical_command, "cargo test --workspace");
        assert!(event.reason_codes.iter().any(|code| code == "verification.event_status_passed"));
        let serialized = serde_json::to_string(&event).expect("event should serialize");
        let roundtrip = serde_json::from_str::<VerificationEvent>(serialized.as_str())
            .expect("event roundtrip");
        assert_eq!(roundtrip, event);
    }

    #[test]
    fn passed_event_after_change_is_fresh_for_covered_paths() {
        let requirement = VerificationRequirement::new(
            "req-1",
            root_ref("root-a"),
            VerificationKind::Test,
            vec!["src/lib.rs".to_owned()],
            90,
            "verification.required_after_patch",
        );
        let state = build_verification_state(
            requirement,
            &[event(
                "event-1",
                VerificationStatus::Passed,
                VerificationScope::ChangedPaths,
                vec!["src/lib.rs"],
                100,
            )],
            110,
        );

        assert_eq!(state.freshness.status, VerificationFreshnessStatus::Fresh);
        assert_eq!(state.latest_passing_event_id.as_deref(), Some("event-1"));
    }

    #[test]
    fn failed_event_is_recorded_but_not_fresh() {
        let failed = event(
            "event-2",
            VerificationStatus::Failed,
            VerificationScope::ChangedPaths,
            vec!["src/lib.rs"],
            120,
        );
        let requirement = VerificationRequirement::new(
            "req-2",
            root_ref("root-a"),
            VerificationKind::Test,
            vec!["src/lib.rs".to_owned()],
            100,
            "verification.required_after_patch",
        );
        let state = build_verification_state(requirement, std::slice::from_ref(&failed), 130);
        let projection = verification_event_recorded_projection(failed);

        assert_eq!(projection.event_type, VERIFICATION_EVENT_RECORDED);
        assert!(projection
            .reason_codes
            .iter()
            .any(|code| code == "verification.event_status_failed"));
        assert_eq!(state.freshness.status, VerificationFreshnessStatus::Stale);
        assert!(state
            .freshness
            .reason_codes
            .iter()
            .any(|code| code == "verification.no_passing_evidence"));
    }

    #[test]
    fn unknown_coverage_never_becomes_fresh() {
        let requirement = VerificationRequirement::new(
            "req-3",
            root_ref("root-a"),
            VerificationKind::Test,
            vec!["src/lib.rs".to_owned()],
            100,
            "verification.required_after_patch",
        );
        let state = build_verification_state(
            requirement,
            &[event(
                "event-3",
                VerificationStatus::Passed,
                VerificationScope::Unknown,
                vec![],
                120,
            )],
            130,
        );

        assert_eq!(state.freshness.status, VerificationFreshnessStatus::Unknown);
        assert!(state
            .freshness
            .reason_codes
            .iter()
            .any(|code| code == "verification.scope_unknown"));
    }

    #[test]
    fn stale_projection_preserves_redaction_boundary() {
        let requirement = VerificationRequirement::new(
            "req-4",
            root_ref("root-a"),
            VerificationKind::Test,
            vec!["src/lib.rs".to_owned()],
            200,
            "verification.required_after_patch",
        );
        let state = build_verification_state(
            requirement,
            &[event(
                "event-old",
                VerificationStatus::Passed,
                VerificationScope::ChangedPaths,
                vec!["src/lib.rs"],
                100,
            )],
            210,
        );
        let projection = verification_state_stale_projection("session-1", "run-1", state);
        let value = serde_json::to_value(&projection).expect("projection should serialize");

        assert_eq!(value["event_type"], VERIFICATION_STATE_STALE);
        assert_eq!(value["redaction_level"], VERIFICATION_REDACTION_LEVEL);
        assert_eq!(value["state"]["freshness"]["status"], "stale");
    }

    #[test]
    fn verification_status_for_cli_reports_recent_fresh_evidence() {
        let projection = verification_event_recorded_projection(event(
            "event-1",
            VerificationStatus::Passed,
            VerificationScope::Workspace,
            vec![],
            1_000,
        ));

        let status = verification_status_for_cli_and_console(true, 12, 5, &[projection]);

        assert_eq!(status.decision, VerificationDiagnosticsDecision::Fresh);
        assert_eq!(status.recorded_events, 1);
        assert_eq!(status.passing_events, 1);
        assert_eq!(status.latest_status.as_deref(), Some("passed"));
        assert!(status
            .reason_codes
            .iter()
            .any(|code| code == "verification.status.recent_fresh_evidence"));
    }

    #[test]
    fn verification_status_for_cli_prioritizes_stale_requirements() {
        let requirement = VerificationRequirement::new(
            "req-stale",
            root_ref("root-a"),
            VerificationKind::Test,
            vec!["src/lib.rs".to_owned()],
            2_000,
            "verification.required_after_patch",
        );
        let state = build_verification_state(
            requirement,
            &[event(
                "event-old",
                VerificationStatus::Passed,
                VerificationScope::Workspace,
                vec![],
                1_000,
            )],
            2_100,
        );
        let projection = verification_state_stale_projection("session-1", "run-1", state);

        let status = verification_status_for_cli_and_console(true, 12, 5, &[projection]);

        assert_eq!(status.decision, VerificationDiagnosticsDecision::Stale);
        assert_eq!(status.stale_requirements, 1);
        assert_eq!(status.latest_status.as_deref(), Some("stale"));
        assert!(status
            .reason_codes
            .iter()
            .any(|code| code == "verification.status.recent_stale_requirement"));
    }

    #[test]
    fn verification_status_for_cli_marks_failed_latest_verification_event() {
        let projection = verification_event_recorded_projection(event(
            "event-2",
            VerificationStatus::Failed,
            VerificationScope::Workspace,
            vec![],
            1_000,
        ));

        let status = verification_status_for_cli_and_console(true, 12, 5, &[projection]);

        assert_eq!(status.decision, VerificationDiagnosticsDecision::Failed);
        assert_eq!(status.failed_events, 1);
        assert_eq!(status.latest_status.as_deref(), Some("failed"));
    }

    #[test]
    fn verification_status_for_cli_distinguishes_disabled_and_no_evidence() {
        let disabled = verification_status_for_cli_and_console(false, 12, 5, &[]);
        let no_evidence = verification_status_for_cli_and_console(true, 12, 5, &[]);

        assert_eq!(disabled.decision, VerificationDiagnosticsDecision::Disabled);
        assert_eq!(no_evidence.decision, VerificationDiagnosticsDecision::NoEvidence);
        assert_eq!(no_evidence.verification_projection_events, 0);
        assert_eq!(no_evidence.redaction_level, VERIFICATION_REDACTION_LEVEL);
    }

    #[test]
    fn verification_summary_keeps_unrelated_command_out_of_valid_evidence() {
        let classification = VerificationCommandClassifier::classify_process_run(&process_input(
            "curl",
            &["https://example.invalid"],
        ));
        let projection =
            verification_command_classified_projection("session-1", "run-1", 100, classification);

        let summary = verification_summary_for_public_artifact(VerificationSummaryRequest {
            rollout_enabled: true,
            journal_total_events: 1,
            journal_window_events: 1,
            projections: &[projection],
            diagnostics: &[],
            finalizer: None,
            observed_tool_activity: None,
        });

        assert_eq!(summary.latest_verification_status.classified_commands, 1);
        assert_eq!(summary.commands_executed.len(), 1);
        assert!(!summary.commands_executed[0].is_verification);
        assert_eq!(summary.commands_executed[0].kind, "unknown");
        assert_eq!(
            summary.latest_verification_status.decision,
            VerificationDiagnosticsDecision::Unknown
        );
    }

    #[test]
    fn verification_summary_redacts_absolute_changed_paths() {
        let projection = verification_event_recorded_projection(event(
            "event-abs",
            VerificationStatus::Passed,
            VerificationScope::ChangedPaths,
            vec![r"C:\Users\Palo\Desktop\palyra-repo\palyra\src\lib.rs"],
            1_000,
        ));

        let summary = verification_summary_for_public_artifact(VerificationSummaryRequest {
            rollout_enabled: true,
            journal_total_events: 1,
            journal_window_events: 1,
            projections: &[projection],
            diagnostics: &[],
            finalizer: None,
            observed_tool_activity: None,
        });

        assert_eq!(summary.changed_files, vec!["<redacted:path>"]);
        let encoded = serde_json::to_string(&summary).expect("summary should serialize");
        assert!(!encoded.contains("Palo"), "{encoded}");
        assert!(!encoded.contains("Users"), "{encoded}");
    }

    #[test]
    fn verification_summary_exports_unverified_mutations_and_finalizer_reason() {
        let requirement = VerificationRequirement::new(
            "req-summary",
            root_ref("root-a"),
            VerificationKind::Test,
            vec!["src/lib.rs".to_owned()],
            2_000,
            "verification.required_after_patch",
        );
        let state = build_verification_state(requirement, &[], 2_100);
        let projection = verification_state_stale_projection("session-1", "run-1", state);
        let diagnostic = VerificationSummaryDiagnostic {
            event_type: "code_intel.diagnostics.delta".to_owned(),
            new_errors: 1,
            new_warnings: 0,
            degraded: false,
            reason_codes: vec!["code_intel.diagnostics.new_errors".to_owned()],
            evidence_refs: vec!["diagnostics:after_patch".to_owned()],
        };
        let finalizer = json!({
            "status": "nudge_required",
            "reason_code": "verification.finalizer.stale_after_code_mutation",
            "pending_requirement_count": 1,
            "satisfied_requirement_count": 0,
            "evidence_refs": ["verification_state:req-summary"],
            "nudge": "Run tests before final answer."
        });

        let summary = verification_summary_for_public_artifact(VerificationSummaryRequest {
            rollout_enabled: true,
            journal_total_events: 2,
            journal_window_events: 2,
            projections: &[projection],
            diagnostics: &[diagnostic],
            finalizer: Some(&finalizer),
            observed_tool_activity: None,
        });

        assert_eq!(summary.state, "available");
        assert_eq!(summary.changed_files, vec!["src/lib.rs"]);
        assert_eq!(summary.unverified_mutations.len(), 1);
        assert_eq!(summary.unverified_mutations[0].freshness_status, "stale");
        assert_eq!(summary.diagnostics[0].new_errors, 1);
        assert!(!summary.final_answer_allowed);
        assert_eq!(
            summary.final_answer_allowed_because,
            "verification.finalizer.stale_after_code_mutation"
        );
        assert!(summary
            .stale_evidence_reasons
            .iter()
            .any(|reason| reason == "code_intel.diagnostics.new_errors"));
    }

    #[test]
    fn patch_stale_builder_marks_changed_paths_for_required_kinds() {
        let states = build_patch_stale_verification_states(VerificationPatchStaleRequest {
            workspace_root: root_ref("root-a"),
            required_kinds: vec![
                VerificationKind::Unknown,
                VerificationKind::Test,
                VerificationKind::Test,
                VerificationKind::Lint,
            ],
            changed_paths: vec![
                " src/lib.rs ".to_owned(),
                "./src/lib.rs".to_owned(),
                "../outside.rs".to_owned(),
            ],
            changed_at_unix_ms: 500,
        });

        assert_eq!(states.len(), 2);
        assert_eq!(states[0].requirement.required_kind, VerificationKind::Lint);
        assert_eq!(states[1].requirement.required_kind, VerificationKind::Test);
        assert_eq!(states[0].freshness.status, VerificationFreshnessStatus::Stale);
        assert_eq!(
            states[0].requirement.changed_paths,
            vec!["_outside_workspace/outside.rs", "src/lib.rs"]
        );
        assert!(states[0]
            .freshness
            .reason_codes
            .iter()
            .any(|code| code == "verification.state_stale"));
    }

    #[test]
    fn stale_output_projection_is_non_instructional() {
        let states = build_patch_stale_verification_states(VerificationPatchStaleRequest {
            workspace_root: root_ref("root-a"),
            required_kinds: vec![VerificationKind::Test],
            changed_paths: vec!["src/lib.rs".to_owned()],
            changed_at_unix_ms: 500,
        });
        let mut output = serde_json::json!({"ok": true});

        append_verification_stale_output(&mut output, states);

        assert_eq!(output["coding_posture"]["instruction_authority"], "none");
        assert_eq!(output["coding_posture"]["verification"]["freshness_status"], "stale");
        assert_eq!(
            output["coding_posture"]["verification"]["requirements"][0]["requirement"]
                ["required_kind"],
            "test"
        );
    }

    #[test]
    fn canonical_command_collapses_whitespace_and_rejects_blank() {
        assert_eq!(
            canonicalize_verification_command(" cargo   test \t --workspace ").as_deref(),
            Some("cargo test --workspace")
        );
        assert_eq!(canonicalize_verification_command(" \n\t "), None);
    }

    #[test]
    fn output_summary_is_bounded_and_marks_artifact_refs() {
        let long = "a".repeat(800);
        let summary = VerificationOutputSummary::from_redacted_text(
            long.as_str(),
            true,
            vec!["artifact:full-output".to_owned()],
        );

        assert!(summary.truncated);
        assert!(summary.redacted);
        assert_eq!(summary.text.chars().count(), 640);
        assert_eq!(summary.artifact_refs, vec!["artifact:full-output"]);
    }
}
