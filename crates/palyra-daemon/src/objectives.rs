use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::routines::{
    RoutineApprovalPolicy, RoutineDeliveryConfig, RoutineQuietHours, RoutineTriggerKind,
};

const OBJECTIVES_DIRECTORY: &str = "objectives";
const OBJECTIVES_REGISTRY_FILE: &str = "registry.json";
const OBJECTIVES_SCHEMA_VERSION: u32 = 1;
const MAX_OBJECTIVE_COUNT: usize = 512;
const MAX_HISTORY_ENTRIES: usize = 256;
const MAX_LINKED_IDS: usize = 256;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveKind {
    #[default]
    Objective,
    Heartbeat,
    StandingOrder,
    Program,
}

impl ObjectiveKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Objective => "objective",
            Self::Heartbeat => "heartbeat",
            Self::StandingOrder => "standing_order",
            Self::Program => "program",
        }
    }

    #[must_use]
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "objective" => Some(Self::Objective),
            "heartbeat" => Some(Self::Heartbeat),
            "standing_order" => Some(Self::StandingOrder),
            "program" => Some(Self::Program),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveState {
    #[default]
    Draft,
    Active,
    Paused,
    Cancelled,
    Archived,
}

impl ObjectiveState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Cancelled => "cancelled",
            Self::Archived => "archived",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObjectivePriority {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

impl ObjectivePriority {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }

    #[must_use]
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "low" => Some(Self::Low),
            "normal" => Some(Self::Normal),
            "high" => Some(Self::High),
            "critical" => Some(Self::Critical),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_runs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveWorkspaceBinding {
    pub workspace_document_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_label: Option<String>,
    #[serde(default)]
    pub related_document_paths: Vec<String>,
    #[serde(default)]
    pub related_memory_ids: Vec<String>,
    #[serde(default)]
    pub related_session_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveAutomationBinding {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routine_id: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    pub trigger_kind: RoutineTriggerKind,
    pub schedule_type: String,
    pub schedule_payload_json: String,
    #[serde(default)]
    pub delivery: RoutineDeliveryConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiet_hours: Option<RoutineQuietHours>,
    #[serde(default)]
    pub cooldown_ms: u64,
    #[serde(default)]
    pub approval_policy: RoutineApprovalPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveAttemptRecord {
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_kind: Option<String>,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_next_step: Option<String>,
    pub created_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveApproachKind {
    Attempted,
    Learned,
    FailedApproach,
    RecommendedNextStep,
    StandingOrder,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveApproachRecord {
    pub entry_id: String,
    pub kind: ObjectiveApproachKind,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub created_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveLifecycleRecord {
    pub event_id: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_state: Option<ObjectiveState>,
    pub to_state: ObjectiveState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub occurred_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveRecord {
    pub objective_id: String,
    pub kind: ObjectiveKind,
    pub state: ObjectiveState,
    pub name: String,
    pub prompt: String,
    pub owner_principal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default)]
    pub priority: ObjectivePriority,
    #[serde(default)]
    pub budget: ObjectiveBudget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_focus: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success_criteria: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_condition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_recommended_step: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standing_order: Option<String>,
    pub workspace: ObjectiveWorkspaceBinding,
    pub automation: ObjectiveAutomationBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt: Option<ObjectiveAttemptRecord>,
    #[serde(default)]
    pub attempt_history: Vec<ObjectiveAttemptRecord>,
    #[serde(default)]
    pub approach_history: Vec<ObjectiveApproachRecord>,
    #[serde(default)]
    pub lifecycle_history: Vec<ObjectiveLifecycleRecord>,
    #[serde(default)]
    pub linked_run_ids: Vec<String>,
    #[serde(default)]
    pub linked_artifact_paths: Vec<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ObjectiveUpsert {
    pub record: ObjectiveRecord,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectiveRegistryDocument {
    schema_version: u32,
    objectives: Vec<ObjectiveRecord>,
}

#[derive(Debug, Error)]
pub enum ObjectiveRegistryError {
    #[error("failed to create objectives storage directory `{path}`: {source}")]
    CreateDirectory { path: PathBuf, source: std::io::Error },
    #[error("failed to open objectives registry `{path}`: {source}")]
    OpenFile { path: PathBuf, source: std::io::Error },
    #[error("failed to read objectives registry `{path}`: {source}")]
    ReadFile { path: PathBuf, source: std::io::Error },
    #[error("failed to parse objectives registry `{path}`: {source}")]
    ParseFile { path: PathBuf, source: serde_json::Error },
    #[error("failed to write objectives registry `{path}`: {source}")]
    WriteFile { path: PathBuf, source: std::io::Error },
    #[error("failed to serialize objectives registry `{path}`: {source}")]
    SerializeFile { path: PathBuf, source: serde_json::Error },
    #[error("objectives registry lock poisoned")]
    LockPoisoned,
    #[error("objective limit exceeded")]
    RegistryLimitExceeded,
    #[error("invalid field `{field}`: {message}")]
    InvalidField { field: &'static str, message: String },
}

#[derive(Debug)]
pub struct ObjectiveRegistry {
    document_path: PathBuf,
    file: Mutex<fs::File>,
    document: Mutex<ObjectiveRegistryDocument>,
}

impl ObjectiveRegistry {
    pub fn open(state_root: &Path) -> Result<Self, ObjectiveRegistryError> {
        let objectives_root = state_root.join(OBJECTIVES_DIRECTORY);
        fs::create_dir_all(&objectives_root).map_err(|source| {
            ObjectiveRegistryError::CreateDirectory { path: objectives_root.clone(), source }
        })?;
        let document_path = objectives_root.join(OBJECTIVES_REGISTRY_FILE);
        let mut file = fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&document_path)
            .map_err(|source| ObjectiveRegistryError::OpenFile {
                path: document_path.clone(),
                source,
            })?;
        let document = load_registry_document(&document_path, &mut file)?;
        Ok(Self { document_path, file: Mutex::new(file), document: Mutex::new(document) })
    }

    pub fn list_objectives(&self) -> Result<Vec<ObjectiveRecord>, ObjectiveRegistryError> {
        let document = self.document.lock().map_err(|_| ObjectiveRegistryError::LockPoisoned)?;
        Ok(document.objectives.clone())
    }

    pub fn get_objective(
        &self,
        objective_id: &str,
    ) -> Result<Option<ObjectiveRecord>, ObjectiveRegistryError> {
        let normalized = normalize_id(objective_id, "objective_id")?;
        let document = self.document.lock().map_err(|_| ObjectiveRegistryError::LockPoisoned)?;
        Ok(document.objectives.iter().find(|entry| entry.objective_id == normalized).cloned())
    }

    pub fn upsert_objective(
        &self,
        request: ObjectiveUpsert,
    ) -> Result<ObjectiveRecord, ObjectiveRegistryError> {
        let now = crate::unix_ms_now().map_err(|error| ObjectiveRegistryError::InvalidField {
            field: "updated_at_unix_ms",
            message: error.to_string(),
        })?;
        let normalized = normalize_objective_record(request.record, now)?;
        let mut document =
            self.document.lock().map_err(|_| ObjectiveRegistryError::LockPoisoned)?;
        if let Some(existing) = document
            .objectives
            .iter_mut()
            .find(|entry| entry.objective_id == normalized.objective_id)
        {
            *existing = normalized.clone();
        } else {
            if document.objectives.len() >= MAX_OBJECTIVE_COUNT {
                return Err(ObjectiveRegistryError::RegistryLimitExceeded);
            }
            document.objectives.push(normalized.clone());
        }
        document.objectives.sort_by(|left, right| left.objective_id.cmp(&right.objective_id));
        persist_registry_document(&self.document_path, &self.file, &document)?;
        Ok(normalized)
    }
}

fn load_registry_document(
    path: &Path,
    file: &mut fs::File,
) -> Result<ObjectiveRegistryDocument, ObjectiveRegistryError> {
    let mut buffer = String::new();
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ObjectiveRegistryError::ReadFile { path: path.to_path_buf(), source })?;
    file.read_to_string(&mut buffer)
        .map_err(|source| ObjectiveRegistryError::ReadFile { path: path.to_path_buf(), source })?;
    if buffer.trim().is_empty() {
        let document = ObjectiveRegistryDocument {
            schema_version: OBJECTIVES_SCHEMA_VERSION,
            objectives: vec![],
        };
        write_registry_document(path, file, &document)?;
        return Ok(document);
    }
    let document = serde_json::from_str::<ObjectiveRegistryDocument>(&buffer)
        .map_err(|source| ObjectiveRegistryError::ParseFile { path: path.to_path_buf(), source })?;
    Ok(document)
}

fn persist_registry_document(
    path: &Path,
    file_mutex: &Mutex<fs::File>,
    document: &ObjectiveRegistryDocument,
) -> Result<(), ObjectiveRegistryError> {
    let mut file = file_mutex.lock().map_err(|_| ObjectiveRegistryError::LockPoisoned)?;
    write_registry_document(path, &mut file, document)
}

fn write_registry_document(
    path: &Path,
    file: &mut fs::File,
    document: &ObjectiveRegistryDocument,
) -> Result<(), ObjectiveRegistryError> {
    let serialized = serde_json::to_vec_pretty(document).map_err(|source| {
        ObjectiveRegistryError::SerializeFile { path: path.to_path_buf(), source }
    })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ObjectiveRegistryError::WriteFile { path: path.to_path_buf(), source })?;
    file.set_len(0)
        .map_err(|source| ObjectiveRegistryError::WriteFile { path: path.to_path_buf(), source })?;
    file.write_all(&serialized)
        .and_then(|_| file.write_all(b"\n"))
        .and_then(|_| file.sync_all())
        .map_err(|source| ObjectiveRegistryError::WriteFile { path: path.to_path_buf(), source })
}

fn normalize_objective_record(
    mut record: ObjectiveRecord,
    now_unix_ms: i64,
) -> Result<ObjectiveRecord, ObjectiveRegistryError> {
    record.objective_id = normalize_id(record.objective_id.as_str(), "objective_id")?;
    record.name = normalize_text(record.name, "name", false, 200)?;
    record.prompt = normalize_text(record.prompt, "prompt", false, 8_000)?;
    record.owner_principal = normalize_text(record.owner_principal, "owner_principal", false, 200)?;
    record.channel = normalize_optional_text(record.channel, "channel", 200)?;
    record.current_focus = normalize_optional_text(record.current_focus, "current_focus", 2_000)?;
    record.success_criteria =
        normalize_optional_text(record.success_criteria, "success_criteria", 4_000)?;
    record.exit_condition =
        normalize_optional_text(record.exit_condition, "exit_condition", 2_000)?;
    record.next_recommended_step =
        normalize_optional_text(record.next_recommended_step, "next_recommended_step", 2_000)?;
    record.standing_order =
        normalize_optional_text(record.standing_order, "standing_order", 4_000)?;
    record.budget.notes = normalize_optional_text(record.budget.notes, "budget.notes", 500)?;
    record.workspace.workspace_document_path = normalize_text(
        record.workspace.workspace_document_path,
        "workspace.workspace_document_path",
        false,
        500,
    )?;
    record.workspace.session_key =
        normalize_optional_text(record.workspace.session_key, "workspace.session_key", 200)?;
    record.workspace.session_label =
        normalize_optional_text(record.workspace.session_label, "workspace.session_label", 200)?;
    record.workspace.related_document_paths = normalize_string_list(
        record.workspace.related_document_paths,
        "workspace.related_document_paths",
        500,
    )?;
    record.workspace.related_memory_ids = normalize_string_list(
        record.workspace.related_memory_ids,
        "workspace.related_memory_ids",
        200,
    )?;
    record.workspace.related_session_ids = normalize_string_list(
        record.workspace.related_session_ids,
        "workspace.related_session_ids",
        200,
    )?;
    record.automation.routine_id =
        normalize_optional_text(record.automation.routine_id, "automation.routine_id", 64)?;
    record.automation.schedule_type =
        normalize_text(record.automation.schedule_type, "automation.schedule_type", false, 32)?;
    record.automation.schedule_payload_json = normalize_text(
        record.automation.schedule_payload_json,
        "automation.schedule_payload_json",
        false,
        8_000,
    )?;
    record.automation.template_id =
        normalize_optional_text(record.automation.template_id, "automation.template_id", 128)?;
    record.attempt_history = normalize_attempts(record.attempt_history)?;
    record.approach_history = normalize_approaches(record.approach_history)?;
    record.lifecycle_history = normalize_lifecycle(record.lifecycle_history)?;
    record.last_attempt = match record.last_attempt {
        Some(attempt) => {
            let mut attempts = normalize_attempts(vec![attempt])?;
            attempts.pop()
        }
        None => None,
    };
    record.linked_run_ids = normalize_string_list(record.linked_run_ids, "linked_run_ids", 64)?;
    record.linked_artifact_paths =
        normalize_string_list(record.linked_artifact_paths, "linked_artifact_paths", 500)?;
    if record.created_at_unix_ms <= 0 {
        record.created_at_unix_ms = now_unix_ms;
    }
    record.updated_at_unix_ms = now_unix_ms;
    if record.state == ObjectiveState::Archived && record.archived_at_unix_ms.is_none() {
        record.archived_at_unix_ms = Some(now_unix_ms);
    }
    Ok(record)
}

fn normalize_attempts(
    attempts: Vec<ObjectiveAttemptRecord>,
) -> Result<Vec<ObjectiveAttemptRecord>, ObjectiveRegistryError> {
    let mut normalized = attempts
        .into_iter()
        .map(|mut attempt| {
            attempt.attempt_id = normalize_id(attempt.attempt_id.as_str(), "attempt_id")?;
            attempt.run_id = normalize_optional_text(attempt.run_id, "run_id", 64)?;
            attempt.session_id = normalize_optional_text(attempt.session_id, "session_id", 64)?;
            attempt.status = normalize_text(attempt.status, "attempt.status", false, 64)?;
            attempt.outcome_kind =
                normalize_optional_text(attempt.outcome_kind, "attempt.outcome_kind", 64)?;
            attempt.summary = normalize_text(attempt.summary, "attempt.summary", false, 2_000)?;
            attempt.learned = normalize_optional_text(attempt.learned, "attempt.learned", 2_000)?;
            attempt.recommended_next_step = normalize_optional_text(
                attempt.recommended_next_step,
                "attempt.recommended_next_step",
                2_000,
            )?;
            Ok(attempt)
        })
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort_by(|left, right| {
        left.created_at_unix_ms
            .cmp(&right.created_at_unix_ms)
            .then_with(|| left.attempt_id.cmp(&right.attempt_id))
    });
    if normalized.len() > MAX_HISTORY_ENTRIES {
        normalized = normalized.split_off(normalized.len() - MAX_HISTORY_ENTRIES);
    }
    Ok(normalized)
}

fn normalize_approaches(
    approaches: Vec<ObjectiveApproachRecord>,
) -> Result<Vec<ObjectiveApproachRecord>, ObjectiveRegistryError> {
    let mut normalized = approaches
        .into_iter()
        .map(|mut entry| {
            entry.entry_id = normalize_id(entry.entry_id.as_str(), "approach.entry_id")?;
            entry.summary = normalize_text(entry.summary, "approach.summary", false, 2_000)?;
            entry.run_id = normalize_optional_text(entry.run_id, "approach.run_id", 64)?;
            Ok(entry)
        })
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort_by(|left, right| {
        left.created_at_unix_ms
            .cmp(&right.created_at_unix_ms)
            .then_with(|| left.entry_id.cmp(&right.entry_id))
    });
    if normalized.len() > MAX_HISTORY_ENTRIES {
        normalized = normalized.split_off(normalized.len() - MAX_HISTORY_ENTRIES);
    }
    Ok(normalized)
}

fn normalize_lifecycle(
    entries: Vec<ObjectiveLifecycleRecord>,
) -> Result<Vec<ObjectiveLifecycleRecord>, ObjectiveRegistryError> {
    let mut normalized = entries
        .into_iter()
        .map(|mut entry| {
            entry.event_id = normalize_id(entry.event_id.as_str(), "lifecycle.event_id")?;
            entry.action = normalize_text(entry.action, "lifecycle.action", false, 64)?;
            entry.reason = normalize_optional_text(entry.reason, "lifecycle.reason", 500)?;
            entry.run_id = normalize_optional_text(entry.run_id, "lifecycle.run_id", 64)?;
            Ok(entry)
        })
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort_by(|left, right| {
        left.occurred_at_unix_ms
            .cmp(&right.occurred_at_unix_ms)
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    if normalized.len() > MAX_HISTORY_ENTRIES {
        normalized = normalized.split_off(normalized.len() - MAX_HISTORY_ENTRIES);
    }
    Ok(normalized)
}

fn normalize_string_list(
    values: Vec<String>,
    field: &'static str,
    max_len: usize,
) -> Result<Vec<String>, ObjectiveRegistryError> {
    let mut normalized = values
        .into_iter()
        .map(|value| normalize_text(value, field, false, max_len))
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort();
    normalized.dedup();
    if normalized.len() > MAX_LINKED_IDS {
        normalized.truncate(MAX_LINKED_IDS);
    }
    Ok(normalized)
}

fn normalize_optional_text(
    value: Option<String>,
    field: &'static str,
    max_len: usize,
) -> Result<Option<String>, ObjectiveRegistryError> {
    value
        .map(|value| normalize_text(value, field, false, max_len))
        .transpose()
        .map(|value| value.filter(|entry| !entry.is_empty()))
}

fn normalize_text(
    value: String,
    field: &'static str,
    allow_empty: bool,
    max_len: usize,
) -> Result<String, ObjectiveRegistryError> {
    let trimmed = value.trim().to_owned();
    if trimmed.is_empty() && !allow_empty {
        return Err(ObjectiveRegistryError::InvalidField {
            field,
            message: "value must not be empty".to_owned(),
        });
    }
    if trimmed.len() > max_len {
        return Err(ObjectiveRegistryError::InvalidField {
            field,
            message: format!("value must be at most {max_len} bytes"),
        });
    }
    Ok(trimmed)
}

fn normalize_id(value: &str, field: &'static str) -> Result<String, ObjectiveRegistryError> {
    let trimmed = value.trim();
    Ulid::from_string(trimmed).map_err(|_| ObjectiveRegistryError::InvalidField {
        field,
        message: "value must be a canonical ULID".to_owned(),
    })?;
    Ok(trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        ObjectiveApproachKind, ObjectiveApproachRecord, ObjectiveAttemptRecord,
        ObjectiveAutomationBinding, ObjectiveBudget, ObjectiveKind, ObjectiveLifecycleRecord,
        ObjectivePriority, ObjectiveRecord, ObjectiveRegistry, ObjectiveState, ObjectiveUpsert,
        ObjectiveWorkspaceBinding,
    };
    use crate::routines::{
        shadow_manual_schedule_payload_json, RoutineApprovalPolicy, RoutineDeliveryConfig,
        RoutineTriggerKind,
    };
    use std::{env, fs, path::PathBuf};
    use ulid::Ulid;

    fn temp_state_root() -> PathBuf {
        let path = env::temp_dir().join(format!("palyra-objective-tests-{}", Ulid::new()));
        fs::create_dir_all(&path).expect("temp state root should be created");
        path
    }

    fn sample_record() -> ObjectiveRecord {
        ObjectiveRecord {
            objective_id: Ulid::new().to_string(),
            kind: ObjectiveKind::Objective,
            state: ObjectiveState::Draft,
            name: "Ship objective board".to_owned(),
            prompt: "Track and complete the objective board target.".to_owned(),
            owner_principal: "user:ops".to_owned(),
            channel: Some("cli".to_owned()),
            priority: ObjectivePriority::High,
            budget: ObjectiveBudget {
                max_runs: Some(5),
                max_tokens: Some(20_000),
                notes: Some("Keep batches reviewable.".to_owned()),
            },
            current_focus: Some("Finalize backend contract.".to_owned()),
            success_criteria: Some("Objective board renders current focus and health.".to_owned()),
            exit_condition: Some("Board is visible in web, CLI, and TUI.".to_owned()),
            next_recommended_step: Some("Wire the overview card.".to_owned()),
            standing_order: None,
            workspace: ObjectiveWorkspaceBinding {
                workspace_document_path: "projects/objectives/demo.md".to_owned(),
                session_key: Some("session:planning".to_owned()),
                session_label: Some("Planning".to_owned()),
                related_document_paths: vec!["context/current-focus.md".to_owned()],
                related_memory_ids: vec!["01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()],
                related_session_ids: vec!["01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()],
            },
            automation: ObjectiveAutomationBinding {
                routine_id: Some(Ulid::new().to_string()),
                enabled: false,
                trigger_kind: RoutineTriggerKind::Manual,
                schedule_type: "at".to_owned(),
                schedule_payload_json: shadow_manual_schedule_payload_json(),
                delivery: RoutineDeliveryConfig::default(),
                quiet_hours: None,
                cooldown_ms: 0,
                approval_policy: RoutineApprovalPolicy::default(),
                template_id: None,
            },
            last_attempt: Some(ObjectiveAttemptRecord {
                attempt_id: Ulid::new().to_string(),
                run_id: Some(Ulid::new().to_string()),
                session_id: None,
                status: "scheduled".to_owned(),
                outcome_kind: Some("success_with_output".to_owned()),
                summary: "Seeded the first attempt.".to_owned(),
                learned: None,
                recommended_next_step: Some("Inspect the run output.".to_owned()),
                created_at_unix_ms: 10,
                completed_at_unix_ms: Some(11),
            }),
            attempt_history: vec![],
            approach_history: vec![ObjectiveApproachRecord {
                entry_id: Ulid::new().to_string(),
                kind: ObjectiveApproachKind::Attempted,
                summary: "Started from the routines surface.".to_owned(),
                run_id: None,
                created_at_unix_ms: 5,
            }],
            lifecycle_history: vec![ObjectiveLifecycleRecord {
                event_id: Ulid::new().to_string(),
                action: "created".to_owned(),
                from_state: None,
                to_state: ObjectiveState::Draft,
                reason: Some("seed".to_owned()),
                run_id: None,
                occurred_at_unix_ms: 1,
            }],
            linked_run_ids: vec![Ulid::new().to_string()],
            linked_artifact_paths: vec!["projects/objectives/demo.md".to_owned()],
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            archived_at_unix_ms: None,
        }
    }

    #[test]
    fn registry_round_trips_objectives() {
        let state_root = temp_state_root();
        let registry = ObjectiveRegistry::open(state_root.as_path()).expect("registry should open");
        let created = registry
            .upsert_objective(ObjectiveUpsert { record: sample_record() })
            .expect("objective should save");
        let fetched = registry
            .get_objective(created.objective_id.as_str())
            .expect("objective lookup should succeed")
            .expect("objective should exist");
        assert_eq!(fetched.objective_id, created.objective_id);
        assert_eq!(fetched.name, "Ship objective board");
        assert_eq!(fetched.priority, ObjectivePriority::High);
        assert_eq!(fetched.workspace.workspace_document_path, "projects/objectives/demo.md");
    }

    #[test]
    fn registry_sets_archive_timestamp_when_state_is_archived() {
        let state_root = temp_state_root();
        let registry = ObjectiveRegistry::open(state_root.as_path()).expect("registry should open");
        let mut record = sample_record();
        record.state = ObjectiveState::Archived;
        let archived =
            registry.upsert_objective(ObjectiveUpsert { record }).expect("objective should save");
        assert!(archived.archived_at_unix_ms.is_some());
    }

    #[test]
    fn registry_trims_and_deduplicates_linked_collections() {
        let state_root = temp_state_root();
        let registry = ObjectiveRegistry::open(state_root.as_path()).expect("registry should open");
        let mut record = sample_record();
        record.workspace.related_document_paths =
            vec![" projects/inbox.md ".to_owned(), "projects/inbox.md".to_owned()];
        let saved =
            registry.upsert_objective(ObjectiveUpsert { record }).expect("objective should save");
        assert_eq!(saved.workspace.related_document_paths, vec!["projects/inbox.md".to_owned()]);
    }
}
