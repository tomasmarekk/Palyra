//! Automation suggestion and blueprint contracts layered over routines and objectives.

use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use palyra_common::config_system::write_content_with_backups;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

const AUTOMATION_SUGGESTIONS_SCHEMA_VERSION: u32 = 1;
const AUTOMATION_DIR: &str = "automation";
const AUTOMATION_SUGGESTIONS_FILE: &str = "suggestions.json";
const MAX_AUTOMATION_SUGGESTIONS: usize = 2_048;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationSuggestionSource {
    Agent,
    Operator,
    Blueprint,
    Doctor,
    RoutineInsight,
    LearningGraph,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationCandidateType {
    Routine,
    Cron,
    Objective,
}

impl AutomationCandidateType {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "routine" => Some(Self::Routine),
            "cron" => Some(Self::Cron),
            "objective" => Some(Self::Objective),
            _ => None,
        }
    }

    const fn action(self) -> &'static str {
        match self {
            Self::Routine | Self::Cron => "create_routine",
            Self::Objective => "create_objective",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl AutomationRiskLevel {
    const fn requires_review(self) -> bool {
        matches!(self, Self::Medium | Self::High | Self::Critical)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationSuggestionStatus {
    Proposed,
    Accepted,
    Dismissed,
    Snoozed,
    Expired,
    Superseded,
}

impl AutomationSuggestionStatus {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "proposed" => Some(Self::Proposed),
            "accepted" => Some(Self::Accepted),
            "dismissed" => Some(Self::Dismissed),
            "snoozed" => Some(Self::Snoozed),
            "expired" => Some(Self::Expired),
            "superseded" => Some(Self::Superseded),
            _ => None,
        }
    }

    const fn terminal(self) -> bool {
        matches!(self, Self::Accepted | Self::Dismissed | Self::Expired | Self::Superseded)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationSuggestionProvenance {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source_artifact_ref: Option<String>,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationSuggestionRecord {
    pub(crate) suggestion_id: String,
    pub(crate) source: AutomationSuggestionSource,
    pub(crate) candidate_type: AutomationCandidateType,
    pub(crate) proposed_spec: Value,
    pub(crate) reason: String,
    pub(crate) risk_level: AutomationRiskLevel,
    #[serde(default)]
    pub(crate) required_approvals: Vec<String>,
    pub(crate) status: AutomationSuggestionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) created_from_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) created_from_run_id: Option<String>,
    pub(crate) dedupe_key: String,
    pub(crate) provenance: AutomationSuggestionProvenance,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) snoozed_until_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) supersedes_suggestion_id: Option<String>,
    #[serde(default)]
    pub(crate) lifecycle_events: Vec<AutomationSuggestionLifecycleEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationSuggestionLifecycleEvent {
    pub(crate) at_unix_ms: i64,
    pub(crate) status: AutomationSuggestionStatus,
    pub(crate) actor: String,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationSuggestionInput {
    pub(crate) source: AutomationSuggestionSource,
    pub(crate) candidate_type: AutomationCandidateType,
    pub(crate) proposed_spec: Value,
    pub(crate) reason: String,
    pub(crate) risk_level: AutomationRiskLevel,
    #[serde(default)]
    pub(crate) required_approvals: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) created_from_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) created_from_run_id: Option<String>,
    #[serde(default)]
    pub(crate) provenance: Option<AutomationSuggestionProvenance>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationBlueprint {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) candidate_type: AutomationCandidateType,
    #[serde(default)]
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) parameters_schema: Value,
    pub(crate) risk_level: AutomationRiskLevel,
    pub(crate) default_schedule: Value,
    #[serde(default)]
    pub(crate) expected_artifacts: Vec<String>,
    pub(crate) verification_strategy: String,
    pub(crate) proposed_spec_template: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationBlueprintSuggestionRequest {
    pub(crate) blueprint_id: String,
    #[serde(default)]
    pub(crate) parameters: Value,
    pub(crate) reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationSuggestionAcceptPlan {
    pub(crate) suggestion_id: String,
    pub(crate) action: String,
    pub(crate) review_required: bool,
    pub(crate) approval_policy_preserved: bool,
    pub(crate) idempotency_key: String,
    pub(crate) payload: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationSuggestionTransitionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) actor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) snoozed_until_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AutomationSuggestionsDocument {
    schema_version: u32,
    #[serde(default)]
    suggestions: Vec<AutomationSuggestionRecord>,
}

impl Default for AutomationSuggestionsDocument {
    fn default() -> Self {
        Self { schema_version: AUTOMATION_SUGGESTIONS_SCHEMA_VERSION, suggestions: Vec::new() }
    }
}

pub(crate) fn load_automation_suggestions() -> Result<Vec<AutomationSuggestionRecord>> {
    Ok(load_suggestions_document()?.suggestions)
}

pub(crate) fn create_automation_suggestion(
    input: AutomationSuggestionInput,
) -> Result<AutomationSuggestionRecord> {
    let mut document = load_suggestions_document()?;
    if document.suggestions.len() >= MAX_AUTOMATION_SUGGESTIONS {
        anyhow::bail!("automation suggestion registry limit exceeded");
    }
    let now = crate::unix_ms_now().context("failed to read system clock")?;
    let proposed_spec = normalize_spec(input.proposed_spec)?;
    let dedupe_key = automation_suggestion_dedupe_key(input.candidate_type, &proposed_spec);
    if let Some(existing) = document
        .suggestions
        .iter()
        .find(|entry| entry.dedupe_key == dedupe_key && !entry.status.terminal())
    {
        return Ok(existing.clone());
    }
    let actor = actor_from_source(input.source);
    let record = AutomationSuggestionRecord {
        suggestion_id: Ulid::new().to_string(),
        source: input.source,
        candidate_type: input.candidate_type,
        proposed_spec,
        reason: normalize_non_empty(input.reason, "reason")?,
        risk_level: input.risk_level,
        required_approvals: normalize_string_list(input.required_approvals),
        status: AutomationSuggestionStatus::Proposed,
        created_from_session_id: normalize_optional(input.created_from_session_id),
        created_from_run_id: normalize_optional(input.created_from_run_id),
        dedupe_key,
        provenance: input.provenance.unwrap_or(AutomationSuggestionProvenance {
            session_id: None,
            run_id: None,
            source_artifact_ref: None,
            evidence_refs: Vec::new(),
        }),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
        snoozed_until_unix_ms: None,
        supersedes_suggestion_id: None,
        lifecycle_events: vec![AutomationSuggestionLifecycleEvent {
            at_unix_ms: now,
            status: AutomationSuggestionStatus::Proposed,
            actor: actor.to_owned(),
            reason: "suggestion created".to_owned(),
        }],
    };
    document.suggestions.push(record.clone());
    save_suggestions_document(&document)?;
    Ok(record)
}

pub(crate) fn transition_automation_suggestion(
    suggestion_id: &str,
    next_status: AutomationSuggestionStatus,
    request: AutomationSuggestionTransitionRequest,
) -> Result<AutomationSuggestionRecord> {
    let mut document = load_suggestions_document()?;
    let normalized_id = normalize_non_empty(suggestion_id, "suggestion_id")?;
    let entry = document
        .suggestions
        .iter_mut()
        .find(|entry| entry.suggestion_id == normalized_id)
        .with_context(|| format!("automation suggestion '{normalized_id}' not found"))?;
    if entry.status.terminal() && entry.status != next_status {
        anyhow::bail!(
            "automation suggestion '{}' is already terminal with status {:?}",
            entry.suggestion_id,
            entry.status
        );
    }
    if next_status == AutomationSuggestionStatus::Snoozed
        && request.snoozed_until_unix_ms.unwrap_or_default() <= 0
    {
        anyhow::bail!("snoozed_until_unix_ms is required for snoozed suggestions");
    }
    let now = crate::unix_ms_now().context("failed to read system clock")?;
    entry.status = next_status;
    entry.updated_at_unix_ms = now;
    entry.snoozed_until_unix_ms = if next_status == AutomationSuggestionStatus::Snoozed {
        request.snoozed_until_unix_ms
    } else {
        None
    };
    entry.lifecycle_events.push(AutomationSuggestionLifecycleEvent {
        at_unix_ms: now,
        status: next_status,
        actor: request
            .actor
            .and_then(normalize_optional_string)
            .unwrap_or_else(|| "operator".to_owned()),
        reason: request
            .reason
            .and_then(normalize_optional_string)
            .unwrap_or_else(|| format!("transitioned to {:?}", next_status)),
    });
    let updated = entry.clone();
    save_suggestions_document(&document)?;
    Ok(updated)
}

pub(crate) fn accept_automation_suggestion(
    suggestion_id: &str,
    request: AutomationSuggestionTransitionRequest,
) -> Result<(AutomationSuggestionRecord, AutomationSuggestionAcceptPlan)> {
    let mut document = load_suggestions_document()?;
    let normalized_id = normalize_non_empty(suggestion_id, "suggestion_id")?;
    let entry = document
        .suggestions
        .iter_mut()
        .find(|entry| entry.suggestion_id == normalized_id)
        .with_context(|| format!("automation suggestion '{normalized_id}' not found"))?;
    if entry.status.terminal() && entry.status != AutomationSuggestionStatus::Accepted {
        anyhow::bail!(
            "automation suggestion '{}' is already terminal with status {:?}",
            entry.suggestion_id,
            entry.status
        );
    }
    let now = crate::unix_ms_now().context("failed to read system clock")?;
    entry.status = AutomationSuggestionStatus::Accepted;
    entry.updated_at_unix_ms = now;
    entry.snoozed_until_unix_ms = None;
    entry.lifecycle_events.push(AutomationSuggestionLifecycleEvent {
        at_unix_ms: now,
        status: AutomationSuggestionStatus::Accepted,
        actor: request
            .actor
            .and_then(normalize_optional_string)
            .unwrap_or_else(|| "operator".to_owned()),
        reason: request
            .reason
            .and_then(normalize_optional_string)
            .unwrap_or_else(|| "operator accepted automation suggestion".to_owned()),
    });
    let updated = entry.clone();
    let plan = AutomationSuggestionAcceptPlan {
        suggestion_id: updated.suggestion_id.clone(),
        action: updated.candidate_type.action().to_owned(),
        review_required: updated.risk_level.requires_review()
            || !updated.required_approvals.is_empty()
            || spec_requires_approval(&updated.proposed_spec),
        approval_policy_preserved: true,
        idempotency_key: updated.dedupe_key.clone(),
        payload: updated.proposed_spec.clone(),
    };
    save_suggestions_document(&document)?;
    Ok((updated, plan))
}

pub(crate) fn create_automation_suggestion_from_blueprint(
    request: AutomationBlueprintSuggestionRequest,
) -> Result<AutomationSuggestionRecord> {
    let blueprint = automation_blueprint(request.blueprint_id.as_str())
        .with_context(|| format!("automation blueprint '{}' not found", request.blueprint_id))?;
    validate_blueprint_parameters(&blueprint, &request.parameters)?;
    let spec = render_blueprint_spec(&blueprint, &request.parameters);
    create_automation_suggestion(AutomationSuggestionInput {
        source: AutomationSuggestionSource::Blueprint,
        candidate_type: blueprint.candidate_type,
        proposed_spec: spec,
        reason: request.reason,
        risk_level: blueprint.risk_level,
        required_approvals: if blueprint.risk_level.requires_review() {
            vec!["operator_review".to_owned()]
        } else {
            Vec::new()
        },
        created_from_session_id: request.session_id.clone(),
        created_from_run_id: request.run_id.clone(),
        provenance: Some(AutomationSuggestionProvenance {
            session_id: request.session_id,
            run_id: request.run_id,
            source_artifact_ref: Some(format!("automation-blueprint:{}", blueprint.id)),
            evidence_refs: vec![blueprint.verification_strategy.clone()],
        }),
    })
}

pub(crate) fn automation_blueprints() -> Vec<AutomationBlueprint> {
    vec![
        AutomationBlueprint {
            id: "local_repo_hygiene".to_owned(),
            title: "Local repo hygiene".to_owned(),
            description: "Run scoped format, lint, and artifact hygiene checks for a workspace.".to_owned(),
            candidate_type: AutomationCandidateType::Routine,
            required_capabilities: vec![
                "tool:workspace.read".to_owned(),
                "tool:process.run".to_owned(),
            ],
            parameters_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "workspace": { "type": "string", "minLength": 1 },
                    "cadence": { "type": "string", "enum": ["daily", "weekly"] }
                },
                "required": ["workspace"]
            }),
            risk_level: AutomationRiskLevel::Medium,
            default_schedule: json!({"type": "every", "interval_ms": 86_400_000}),
            expected_artifacts: vec!["hygiene_report".to_owned()],
            verification_strategy: "doctor registry and targeted command exit status".to_owned(),
            proposed_spec_template: json!({
                "name": "Local repo hygiene",
                "prompt": "Inspect repository hygiene for the configured workspace and report actionable failures.",
                "trigger_kind": "schedule",
                "enabled": false,
                "schedule_type": "every",
                "schedule_payload": "86400000",
                "approval_policy": { "mode": "before_first_run" },
                "execution": { "execution_posture": "sensitive_tools" }
            }),
        },
        AutomationBlueprint {
            id: "summary_report".to_owned(),
            title: "Summary report".to_owned(),
            description: "Produce a periodic operator summary from recent sessions, routines, and objectives.".to_owned(),
            candidate_type: AutomationCandidateType::Routine,
            required_capabilities: vec!["tool:journal.read".to_owned(), "tool:memory.search".to_owned()],
            parameters_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "channel": { "type": "string", "minLength": 1 },
                    "window": { "type": "string", "enum": ["daily", "weekly"] }
                }
            }),
            risk_level: AutomationRiskLevel::Low,
            default_schedule: json!({"type": "every", "interval_ms": 86_400_000}),
            expected_artifacts: vec!["summary_markdown".to_owned()],
            verification_strategy: "support bundle redaction and report generation smoke".to_owned(),
            proposed_spec_template: json!({
                "name": "Summary report",
                "prompt": "Summarize recent operator activity, blockers, and follow-up actions.",
                "trigger_kind": "schedule",
                "enabled": false,
                "schedule_type": "every",
                "schedule_payload": "86400000",
                "approval_policy": { "mode": "none" }
            }),
        },
        AutomationBlueprint {
            id: "dependency_check".to_owned(),
            title: "Dependency check".to_owned(),
            description: "Run existing advisory and dependency gates without broadening dependency policy.".to_owned(),
            candidate_type: AutomationCandidateType::Routine,
            required_capabilities: vec![
                "tool:process.run".to_owned(),
                "tool:workspace.read".to_owned(),
            ],
            parameters_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "workspace": { "type": "string", "minLength": 1 },
                    "include_node": { "type": "boolean" }
                },
                "required": ["workspace"]
            }),
            risk_level: AutomationRiskLevel::Medium,
            default_schedule: json!({"type": "every", "interval_ms": 604_800_000}),
            expected_artifacts: vec!["advisory_report".to_owned(), "license_report".to_owned()],
            verification_strategy: "cargo deny, OSV, and npm audit summaries".to_owned(),
            proposed_spec_template: json!({
                "name": "Dependency check",
                "prompt": "Run dependency advisory, license, and duplicate dependency checks using configured repository gates.",
                "trigger_kind": "schedule",
                "enabled": false,
                "schedule_type": "every",
                "schedule_payload": "604800000",
                "approval_policy": { "mode": "before_first_run" }
            }),
        },
        AutomationBlueprint {
            id: "memory_review".to_owned(),
            title: "Memory review".to_owned(),
            description: "Review durable memory, learning candidates, and conflicts without mutating recall state automatically.".to_owned(),
            candidate_type: AutomationCandidateType::Routine,
            required_capabilities: vec!["tool:memory.search".to_owned(), "tool:journal.read".to_owned()],
            parameters_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "scope": { "type": "string", "enum": ["principal", "workspace"] },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200 }
                }
            }),
            risk_level: AutomationRiskLevel::Low,
            default_schedule: json!({"type": "every", "interval_ms": 604_800_000}),
            expected_artifacts: vec!["memory_conflict_report".to_owned()],
            verification_strategy: "learning graph conflict projection and archived-node exclusion".to_owned(),
            proposed_spec_template: json!({
                "name": "Memory review",
                "prompt": "Review durable memory candidates, conflicts, archived state, and stale procedures. Report proposed changes only.",
                "trigger_kind": "schedule",
                "enabled": false,
                "schedule_type": "every",
                "schedule_payload": "604800000",
                "approval_policy": { "mode": "none" }
            }),
        },
    ]
}

pub(crate) fn automation_blueprint(blueprint_id: &str) -> Option<AutomationBlueprint> {
    let normalized = blueprint_id.trim().to_ascii_lowercase();
    automation_blueprints().into_iter().find(|blueprint| blueprint.id == normalized)
}

pub(crate) fn validate_blueprint_parameters(
    blueprint: &AutomationBlueprint,
    parameters: &Value,
) -> Result<()> {
    let Value::Object(values) = parameters else {
        if parameters.is_null() {
            return Ok(());
        }
        anyhow::bail!("blueprint parameters must be a JSON object");
    };
    let allowed = blueprint
        .parameters_schema
        .pointer("/properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for key in values.keys() {
        if !allowed.contains_key(key) {
            anyhow::bail!("unsupported blueprint parameter '{key}'");
        }
    }
    if let Some(required) =
        blueprint.parameters_schema.pointer("/required").and_then(Value::as_array)
    {
        for key in required.iter().filter_map(Value::as_str) {
            if !values.contains_key(key) {
                anyhow::bail!("missing required blueprint parameter '{key}'");
            }
        }
    }
    Ok(())
}

pub(crate) fn automation_suggestion_dedupe_key(
    candidate_type: AutomationCandidateType,
    proposed_spec: &Value,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(candidate_type.action().as_bytes());
    hasher.update(b":");
    hasher.update(stable_json(proposed_spec).as_bytes());
    hex::encode(hasher.finalize())
}

fn load_suggestions_document() -> Result<AutomationSuggestionsDocument> {
    let path = suggestions_path()?;
    if !path.exists() {
        return Ok(AutomationSuggestionsDocument::default());
    }
    let payload = fs::read_to_string(path.as_path())
        .with_context(|| format!("failed to read {}", path.display()))?;
    let document: AutomationSuggestionsDocument = serde_json::from_str(payload.as_str())
        .with_context(|| format!("failed to parse {}", path.display()))?;
    if document.schema_version != AUTOMATION_SUGGESTIONS_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported automation suggestions schema version {}",
            document.schema_version
        );
    }
    Ok(document)
}

fn save_suggestions_document(document: &AutomationSuggestionsDocument) -> Result<()> {
    let path = suggestions_path()?;
    let payload = serde_json::to_string_pretty(document)
        .context("failed to serialize automation suggestions")?;
    write_content_with_backups(path.as_path(), payload.as_str(), 0)
        .with_context(|| format!("failed to write {}", path.display()))
}

fn suggestions_path() -> Result<PathBuf> {
    let root = crate::routines::resolve_routines_root(None)
        .context("failed to resolve routines root for automation suggestions")?
        .join(AUTOMATION_DIR);
    fs::create_dir_all(root.as_path())
        .with_context(|| format!("failed to create {}", root.display()))?;
    Ok(root.join(AUTOMATION_SUGGESTIONS_FILE))
}

fn render_blueprint_spec(blueprint: &AutomationBlueprint, parameters: &Value) -> Value {
    let mut spec = blueprint.proposed_spec_template.clone();
    if let Value::Object(map) = &mut spec {
        map.insert("blueprint_id".to_owned(), Value::String(blueprint.id.clone()));
        map.insert("parameters".to_owned(), parameters.clone());
        map.insert("required_capabilities".to_owned(), json!(blueprint.required_capabilities));
        map.insert("expected_artifacts".to_owned(), json!(blueprint.expected_artifacts));
        map.insert("verification_strategy".to_owned(), json!(blueprint.verification_strategy));
    }
    spec
}

fn normalize_spec(value: Value) -> Result<Value> {
    match value {
        Value::Object(map) if !map.is_empty() => Ok(Value::Object(map)),
        _ => anyhow::bail!("automation suggestion proposed_spec must be a non-empty JSON object"),
    }
}

fn normalize_non_empty(value: impl AsRef<str>, field: &'static str) -> Result<String> {
    let trimmed = value.as_ref().trim();
    if trimmed.is_empty() {
        anyhow::bail!("{field} cannot be empty");
    }
    if trimmed.chars().any(char::is_control) {
        anyhow::bail!("{field} cannot contain control characters");
    }
    Ok(trimmed.to_owned())
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(normalize_optional_string)
}

fn normalize_optional_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn normalize_string_list(values: Vec<String>) -> Vec<String> {
    let mut normalized =
        values.into_iter().filter_map(|value| normalize_optional(Some(value))).collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn spec_requires_approval(spec: &Value) -> bool {
    spec.pointer("/approval_policy/mode")
        .and_then(Value::as_str)
        .is_some_and(|mode| !matches!(mode, "none" | "disabled"))
}

const fn actor_from_source(source: AutomationSuggestionSource) -> &'static str {
    match source {
        AutomationSuggestionSource::Agent => "agent",
        AutomationSuggestionSource::Operator => "operator",
        AutomationSuggestionSource::Blueprint => "blueprint",
        AutomationSuggestionSource::Doctor => "doctor",
        AutomationSuggestionSource::RoutineInsight => "routine_insight",
        AutomationSuggestionSource::LearningGraph => "learning_graph",
    }
}

fn stable_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            let body = entries
                .into_iter()
                .map(|(key, value)| format!("{key}:{}", stable_json(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
        Value::Array(values) => {
            let body = values.iter().map(stable_json).collect::<Vec<_>>().join(",");
            format!("[{body}]")
        }
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        accept_automation_suggestion, automation_blueprint, automation_blueprints,
        automation_suggestion_dedupe_key, create_automation_suggestion,
        validate_blueprint_parameters, AutomationCandidateType, AutomationRiskLevel,
        AutomationSuggestionInput, AutomationSuggestionSource, AutomationSuggestionStatus,
        AutomationSuggestionTransitionRequest,
    };
    use serde_json::json;

    #[test]
    fn blueprint_catalog_contains_safe_reviewable_defaults() {
        let blueprints = automation_blueprints();
        assert!(blueprints.iter().any(|blueprint| blueprint.id == "local_repo_hygiene"));
        assert!(blueprints.iter().all(|blueprint| !blueprint.required_capabilities.is_empty()));
        assert!(blueprints.iter().all(|blueprint| !blueprint.proposed_spec_template["enabled"]
            .as_bool()
            .unwrap_or(true)));
    }

    #[test]
    fn blueprint_parameters_reject_unknown_keys() {
        let blueprint = automation_blueprint("dependency_check").expect("blueprint exists");
        let error = validate_blueprint_parameters(
            &blueprint,
            &json!({"workspace": ".", "unexpected": true}),
        )
        .expect_err("unknown parameter should be rejected");
        assert!(error.to_string().contains("unsupported blueprint parameter"));
    }

    #[test]
    fn dedupe_key_uses_stable_object_order() {
        let left = json!({"name": "a", "enabled": false});
        let right = json!({"enabled": false, "name": "a"});
        assert_eq!(
            automation_suggestion_dedupe_key(AutomationCandidateType::Routine, &left),
            automation_suggestion_dedupe_key(AutomationCandidateType::Routine, &right)
        );
    }

    #[test]
    fn lifecycle_accept_builds_review_preserving_plan() {
        let _guard = crate::test_env::lock();
        let tempdir = tempfile::tempdir().expect("tempdir");
        std::env::set_var("PALYRA_STATE_ROOT", tempdir.path());
        let record = create_automation_suggestion(AutomationSuggestionInput {
            source: AutomationSuggestionSource::Agent,
            candidate_type: AutomationCandidateType::Routine,
            proposed_spec: json!({
                "name": "Sensitive routine",
                "enabled": false,
                "approval_policy": { "mode": "before_first_run" }
            }),
            reason: "operator repeats this task".to_owned(),
            risk_level: AutomationRiskLevel::Medium,
            required_approvals: Vec::new(),
            created_from_session_id: Some("session-1".to_owned()),
            created_from_run_id: Some("run-1".to_owned()),
            provenance: None,
        })
        .expect("suggestion should be created");
        let (accepted, plan) = accept_automation_suggestion(
            record.suggestion_id.as_str(),
            AutomationSuggestionTransitionRequest {
                actor: Some("operator".to_owned()),
                reason: Some("approved".to_owned()),
                snoozed_until_unix_ms: None,
            },
        )
        .expect("suggestion should accept");
        assert_eq!(accepted.status, AutomationSuggestionStatus::Accepted);
        assert_eq!(plan.action, "create_routine");
        assert!(plan.review_required);
        assert!(plan.approval_policy_preserved);
        std::env::remove_var("PALYRA_STATE_ROOT");
    }
}
