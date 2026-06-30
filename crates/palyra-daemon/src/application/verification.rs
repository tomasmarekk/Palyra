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

use crate::application::project_facts::ProjectWorkspaceRootRef;

pub(crate) const VERIFICATION_SCHEMA_VERSION: u32 = 1;
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
    EventRecorded,
    EventStatusFailed,
    EventStatusPassed,
    FreshnessChecked,
    FreshPassingEvidenceFound,
    InvalidCommand,
    NoChangedPaths,
    NoPassingEvidence,
    ScopeUnknown,
    StateStale,
    WorkspaceMismatch,
}

impl VerificationReasonCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::EventRecorded => "verification.event_recorded",
            Self::EventStatusFailed => "verification.event_status_failed",
            Self::EventStatusPassed => "verification.event_status_passed",
            Self::FreshnessChecked => "verification.freshness_checked",
            Self::FreshPassingEvidenceFound => "verification.fresh_passing_evidence_found",
            Self::InvalidCommand => "verification.invalid_command",
            Self::NoChangedPaths => "verification.no_changed_paths",
            Self::NoPassingEvidence => "verification.no_passing_evidence",
            Self::ScopeUnknown => "verification.scope_unknown",
            Self::StateStale => "verification.state_stale",
            Self::WorkspaceMismatch => "verification.workspace_mismatch",
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
    pub(crate) event: Option<VerificationEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) state: Option<VerificationState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) freshness: Option<VerificationFreshnessDecision>,
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
    VerificationJournalProjection {
        schema_version: VERIFICATION_SCHEMA_VERSION,
        event_type: VERIFICATION_STATE_STALE.to_owned(),
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        created_at_unix_ms: state.freshness.checked_at_unix_ms,
        reason_codes: state.freshness.reason_codes.clone(),
        evidence_refs: state
            .latest_event_id
            .as_ref()
            .map(|event_id| vec![format!("verification_event:{event_id}")])
            .unwrap_or_default(),
        redaction_level: VERIFICATION_REDACTION_LEVEL.to_owned(),
        event: None,
        state: Some(state),
        freshness: None,
    }
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
        build_verification_state, canonicalize_verification_command,
        verification_event_recorded_projection, verification_state_stale_projection,
        VerificationEvent, VerificationEventCreateRequest, VerificationFreshnessStatus,
        VerificationKind, VerificationOutputSummary, VerificationRequirement, VerificationScope,
        VerificationStatus, VERIFICATION_EVENT_RECORDED, VERIFICATION_REDACTION_LEVEL,
        VERIFICATION_SCHEMA_VERSION, VERIFICATION_STATE_STALE,
    };

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
