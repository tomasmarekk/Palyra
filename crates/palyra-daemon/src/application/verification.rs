//! Verification evidence ledger contracts.
//!
//! This module is deliberately storage-neutral: it defines the journal payload
//! shapes and pure read-model logic for verification freshness, while later
//! runtime integrations decide when to append the events. The model is
//! conservative about path coverage; unknown coverage never becomes a fresh
//! passing verification.

// M35 lands the ledger schema before M36 wires process-run evidence into it.
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

/// Verification work family. Classifiers may map several commands into one kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VerificationKind {
    Build,
    Check,
    Format,
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
        let is_verification = kind != VerificationKind::Unknown;
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
        verification_state_stale_projection, VerificationCommandClassifier, VerificationEvent,
        VerificationEventCreateRequest, VerificationFreshnessStatus, VerificationKind,
        VerificationOutputSummary, VerificationPatchStaleRequest, VerificationRequirement,
        VerificationScope, VerificationStatus, VERIFICATION_COMMAND_CLASSIFIED,
        VERIFICATION_EVENT_RECORDED, VERIFICATION_REDACTION_LEVEL, VERIFICATION_SCHEMA_VERSION,
        VERIFICATION_STATE_STALE,
    };
    use palyra_common::process_runner_input::ProcessRunnerToolInput;

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
            lifetime_mode: Default::default(),
            keep_running_after_run: false,
        }
    }

    #[test]
    fn process_run_classifier_recognizes_required_verification_commands() {
        for (command, args, expected_kind) in [
            ("cargo", vec!["test"], VerificationKind::Test),
            ("cargo", vec!["check"], VerificationKind::Check),
            ("npm", vec!["test"], VerificationKind::Test),
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
