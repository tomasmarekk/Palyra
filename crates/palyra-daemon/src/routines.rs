use std::{
    collections::BTreeSet,
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    cron::{self, CronTimezoneMode},
    gateway::proto::palyra::cron::v1 as cron_v1,
    journal::{CronJobRecord, CronRunRecord, CronRunStatus},
};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use palyra_common::{default_state_root, IdentityStorePathError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

const ROUTINE_REGISTRY_VERSION: u32 = 1;
const ROUTINES_DIR: &str = "routines";
const ROUTINES_REGISTRY_FILE: &str = "definitions.json";
const ROUTINE_RUNS_FILE: &str = "run_metadata.json";
const MAX_ROUTINE_COUNT: usize = 2_048;
const MAX_ROUTINE_RUN_METADATA_COUNT: usize = 8_192;
const MIN_EVERY_INTERVAL_MS: u64 = 5 * 60 * 1_000;
pub const SHADOW_AT_TIMESTAMP_RFC3339: &str = "2100-01-01T00:00:00Z";
pub const ROUTINE_EXPORT_SCHEMA_ID: &str = "palyra.routine.export.v1";
pub const ROUTINE_EXPORT_SCHEMA_VERSION: u32 = 1;
pub const ROUTINE_TEMPLATE_PACK_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineTriggerKind {
    Schedule,
    Hook,
    Webhook,
    SystemEvent,
    Manual,
}

impl RoutineTriggerKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Hook => "hook",
            Self::Webhook => "webhook",
            Self::SystemEvent => "system_event",
            Self::Manual => "manual",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "schedule" => Some(Self::Schedule),
            "hook" => Some(Self::Hook),
            "webhook" => Some(Self::Webhook),
            "system_event" => Some(Self::SystemEvent),
            "manual" => Some(Self::Manual),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineRunMode {
    #[default]
    SameSession,
    FreshSession,
}

impl RoutineRunMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SameSession => "same_session",
            Self::FreshSession => "fresh_session",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "same_session" => Some(Self::SameSession),
            "fresh_session" => Some(Self::FreshSession),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineExecutionPosture {
    #[default]
    Standard,
    SensitiveTools,
}

impl RoutineExecutionPosture {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::SensitiveTools => "sensitive_tools",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "standard" => Some(Self::Standard),
            "sensitive_tools" => Some(Self::SensitiveTools),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineDeliveryMode {
    SameChannel,
    SpecificChannel,
    LocalOnly,
    LogsOnly,
}

impl RoutineDeliveryMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SameChannel => "same_channel",
            Self::SpecificChannel => "specific_channel",
            Self::LocalOnly => "local_only",
            Self::LogsOnly => "logs_only",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "same_channel" => Some(Self::SameChannel),
            "specific_channel" => Some(Self::SpecificChannel),
            "local_only" => Some(Self::LocalOnly),
            "logs_only" => Some(Self::LogsOnly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineSilentPolicy {
    #[default]
    Noisy,
    FailureOnly,
    AuditOnly,
}

impl RoutineSilentPolicy {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Noisy => "noisy",
            Self::FailureOnly => "failure_only",
            Self::AuditOnly => "audit_only",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "noisy" => Some(Self::Noisy),
            "failure_only" => Some(Self::FailureOnly),
            "audit_only" => Some(Self::AuditOnly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineRunOutcomeKind {
    SuccessWithOutput,
    SuccessNoOp,
    Skipped,
    Throttled,
    Failed,
    Denied,
}

impl RoutineRunOutcomeKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SuccessWithOutput => "success_with_output",
            Self::SuccessNoOp => "success_no_op",
            Self::Skipped => "skipped",
            Self::Throttled => "throttled",
            Self::Failed => "failed",
            Self::Denied => "denied",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineDispatchMode {
    #[default]
    Normal,
    TestRun,
    Replay,
}

impl RoutineDispatchMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::TestRun => "test_run",
            Self::Replay => "replay",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineExecutionConfig {
    #[serde(default)]
    pub run_mode: RoutineRunMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub procedure_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_profile_id: Option<String>,
    #[serde(default)]
    pub execution_posture: RoutineExecutionPosture,
}

impl Default for RoutineExecutionConfig {
    fn default() -> Self {
        Self {
            run_mode: RoutineRunMode::SameSession,
            procedure_profile_id: None,
            skill_profile_id: None,
            provider_profile_id: None,
            execution_posture: RoutineExecutionPosture::Standard,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineApprovalMode {
    None,
    BeforeEnable,
    BeforeFirstRun,
}

impl RoutineApprovalMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::BeforeEnable => "before_enable",
            Self::BeforeFirstRun => "before_first_run",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "before_enable" => Some(Self::BeforeEnable),
            "before_first_run" => Some(Self::BeforeFirstRun),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineDeliveryConfig {
    pub mode: RoutineDeliveryMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_mode: Option<RoutineDeliveryMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_channel: Option<String>,
    #[serde(default)]
    pub silent_policy: RoutineSilentPolicy,
}

impl Default for RoutineDeliveryConfig {
    fn default() -> Self {
        Self {
            mode: RoutineDeliveryMode::SameChannel,
            channel: None,
            failure_mode: None,
            failure_channel: None,
            silent_policy: RoutineSilentPolicy::Noisy,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineQuietHours {
    pub start_minute_of_day: u16,
    pub end_minute_of_day: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineApprovalPolicy {
    pub mode: RoutineApprovalMode,
}

impl Default for RoutineApprovalPolicy {
    fn default() -> Self {
        Self { mode: RoutineApprovalMode::None }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineMetadataRecord {
    pub routine_id: String,
    pub trigger_kind: RoutineTriggerKind,
    pub trigger_payload_json: String,
    #[serde(default)]
    pub execution: RoutineExecutionConfig,
    pub delivery: RoutineDeliveryConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiet_hours: Option<RoutineQuietHours>,
    #[serde(default)]
    pub cooldown_ms: u64,
    #[serde(default)]
    pub approval_policy: RoutineApprovalPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub struct RoutineMetadataUpsert {
    pub routine_id: String,
    pub trigger_kind: RoutineTriggerKind,
    pub trigger_payload_json: String,
    pub execution: RoutineExecutionConfig,
    pub delivery: RoutineDeliveryConfig,
    pub quiet_hours: Option<RoutineQuietHours>,
    pub cooldown_ms: u64,
    pub approval_policy: RoutineApprovalPolicy,
    pub template_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineRunMetadataRecord {
    pub run_id: String,
    pub routine_id: String,
    pub trigger_kind: RoutineTriggerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_reason: Option<String>,
    pub trigger_payload_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_dedupe_key: Option<String>,
    #[serde(default)]
    pub execution: RoutineExecutionConfig,
    pub delivery: RoutineDeliveryConfig,
    #[serde(default)]
    pub dispatch_mode: RoutineDispatchMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_override: Option<RoutineRunOutcomeKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_delivered: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safety_note: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub struct RoutineRunMetadataUpsert {
    pub run_id: String,
    pub routine_id: String,
    pub trigger_kind: RoutineTriggerKind,
    pub trigger_reason: Option<String>,
    pub trigger_payload_json: String,
    pub trigger_dedupe_key: Option<String>,
    pub execution: RoutineExecutionConfig,
    pub delivery: RoutineDeliveryConfig,
    pub dispatch_mode: RoutineDispatchMode,
    pub source_run_id: Option<String>,
    pub outcome_override: Option<RoutineRunOutcomeKind>,
    pub outcome_message: Option<String>,
    pub output_delivered: Option<bool>,
    pub skip_reason: Option<String>,
    pub delivery_reason: Option<String>,
    pub approval_note: Option<String>,
    pub safety_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RoutineSchedulePreview {
    pub phrase: String,
    pub normalized_text: String,
    pub explanation: String,
    pub schedule_type: String,
    pub schedule_payload_json: String,
    pub schedule_payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at_unix_ms: Option<i64>,
    pub timezone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineTemplateDefinition {
    pub template_id: String,
    pub title: String,
    pub description: String,
    pub trigger_kind: RoutineTriggerKind,
    pub default_name: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub natural_language_schedule: Option<String>,
    pub delivery_mode: RoutineDeliveryMode,
    pub approval_mode: RoutineApprovalMode,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineExportBundle {
    pub schema_id: String,
    pub schema_version: u32,
    pub exported_at_unix_ms: i64,
    pub routine: RoutineMetadataRecord,
    pub job: CronJobRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RoutineRegistryDocument {
    schema_version: u32,
    #[serde(default)]
    routines: Vec<RoutineMetadataRecord>,
}

impl Default for RoutineRegistryDocument {
    fn default() -> Self {
        Self { schema_version: ROUTINE_REGISTRY_VERSION, routines: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RoutineRunMetadataDocument {
    schema_version: u32,
    #[serde(default)]
    runs: Vec<RoutineRunMetadataRecord>,
}

impl Default for RoutineRunMetadataDocument {
    fn default() -> Self {
        Self { schema_version: ROUTINE_REGISTRY_VERSION, runs: Vec::new() }
    }
}

#[derive(Debug, Clone)]
struct RegistryPath {
    path: PathBuf,
}

impl RegistryPath {
    fn as_path(&self) -> &Path {
        self.path.as_path()
    }

    fn to_path_buf(&self) -> PathBuf {
        self.path.clone()
    }
}

#[derive(Debug)]
pub struct RoutineRegistry {
    definitions_path: RegistryPath,
    definitions_file: Mutex<fs::File>,
    definitions: Mutex<RoutineRegistryDocument>,
    run_metadata_path: RegistryPath,
    run_metadata_file: Mutex<fs::File>,
    run_metadata: Mutex<RoutineRunMetadataDocument>,
}

#[derive(Debug, Error)]
pub enum RoutineRegistryError {
    #[error("routine registry lock poisoned")]
    LockPoisoned,
    #[error("failed to read routine registry {path}: {source}")]
    ReadRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse routine registry {path}: {source}")]
    ParseRegistry {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to write routine registry {path}: {source}")]
    WriteRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize routine registry: {0}")]
    SerializeRegistry(#[from] serde_json::Error),
    #[error("unsupported routine registry version {0}")]
    UnsupportedVersion(u32),
    #[error("routine registry limit exceeded")]
    RegistryLimitExceeded,
    #[error("invalid {field}: {message}")]
    InvalidField { field: &'static str, message: String },
    #[error("system time before unix epoch: {0}")]
    InvalidSystemTime(#[from] std::time::SystemTimeError),
    #[error("failed to resolve default state root: {0}")]
    DefaultStateRoot(#[from] IdentityStorePathError),
}

impl RoutineRegistry {
    pub fn open(state_root: &Path) -> Result<Self, RoutineRegistryError> {
        let routines_root = resolve_routines_root(Some(state_root))?;
        let definitions_path = RegistryPath { path: routines_root.join(ROUTINES_REGISTRY_FILE) };
        let run_metadata_path = RegistryPath { path: routines_root.join(ROUTINE_RUNS_FILE) };
        let mut definitions_file = open_registry_file(&definitions_path)?;
        let definitions = load_registry_document(&definitions_path, &mut definitions_file)?;
        let mut run_metadata_file = open_registry_file(&run_metadata_path)?;
        let run_metadata = load_run_metadata_document(&run_metadata_path, &mut run_metadata_file)?;
        Ok(Self {
            definitions_path,
            definitions_file: Mutex::new(definitions_file),
            definitions: Mutex::new(definitions),
            run_metadata_path,
            run_metadata_file: Mutex::new(run_metadata_file),
            run_metadata: Mutex::new(run_metadata),
        })
    }

    pub fn list_routines(&self) -> Result<Vec<RoutineMetadataRecord>, RoutineRegistryError> {
        let definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        Ok(definitions.routines.clone())
    }

    pub fn get_routine(
        &self,
        routine_id: &str,
    ) -> Result<Option<RoutineMetadataRecord>, RoutineRegistryError> {
        let normalized = normalize_identifier(routine_id, "routine_id")?;
        let definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        Ok(definitions.routines.iter().find(|entry| entry.routine_id == normalized).cloned())
    }

    pub fn upsert_routine(
        &self,
        request: RoutineMetadataUpsert,
    ) -> Result<RoutineMetadataRecord, RoutineRegistryError> {
        let now = unix_ms_now()?;
        let normalized = normalize_routine_metadata_upsert(request, now)?;
        let mut definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        if let Some(existing) =
            definitions.routines.iter_mut().find(|entry| entry.routine_id == normalized.routine_id)
        {
            existing.trigger_kind = normalized.trigger_kind;
            existing.trigger_payload_json = normalized.trigger_payload_json;
            existing.execution = normalized.execution;
            existing.delivery = normalized.delivery;
            existing.quiet_hours = normalized.quiet_hours;
            existing.cooldown_ms = normalized.cooldown_ms;
            existing.approval_policy = normalized.approval_policy;
            existing.template_id = normalized.template_id;
            existing.updated_at_unix_ms = now;
            let updated = existing.clone();
            let document = RoutineRegistryDocument {
                schema_version: ROUTINE_REGISTRY_VERSION,
                routines: definitions.routines.clone(),
            };
            drop(definitions);
            write_registry_document(&self.definitions_path, &self.definitions_file, &document)?;
            return Ok(updated);
        }
        if definitions.routines.len() >= MAX_ROUTINE_COUNT {
            return Err(RoutineRegistryError::RegistryLimitExceeded);
        }
        definitions.routines.push(normalized.clone());
        definitions.routines.sort_by(|left, right| left.routine_id.cmp(&right.routine_id));
        let document = RoutineRegistryDocument {
            schema_version: ROUTINE_REGISTRY_VERSION,
            routines: definitions.routines.clone(),
        };
        drop(definitions);
        write_registry_document(&self.definitions_path, &self.definitions_file, &document)?;
        Ok(normalized)
    }

    pub fn delete_routine(&self, routine_id: &str) -> Result<bool, RoutineRegistryError> {
        let normalized = normalize_identifier(routine_id, "routine_id")?;
        let mut definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        let before = definitions.routines.len();
        definitions.routines.retain(|entry| entry.routine_id != normalized);
        let deleted = definitions.routines.len() != before;
        if deleted {
            let document = RoutineRegistryDocument {
                schema_version: ROUTINE_REGISTRY_VERSION,
                routines: definitions.routines.clone(),
            };
            drop(definitions);
            write_registry_document(&self.definitions_path, &self.definitions_file, &document)?;
        }
        Ok(deleted)
    }

    pub fn sync_schedule_routines(
        &self,
        cron_jobs: &[CronJobRecord],
    ) -> Result<(), RoutineRegistryError> {
        let now = unix_ms_now()?;
        let mut definitions =
            self.definitions.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        let schedule_ids =
            cron_jobs.iter().map(|job| job.job_id.clone()).collect::<BTreeSet<String>>();
        for job in cron_jobs {
            if let Some(existing) =
                definitions.routines.iter_mut().find(|entry| entry.routine_id == job.job_id)
            {
                if existing.trigger_kind != RoutineTriggerKind::Schedule {
                    continue;
                }
                existing.trigger_payload_json = build_schedule_trigger_payload(job)?;
                existing.updated_at_unix_ms = now;
                continue;
            }
            definitions.routines.push(RoutineMetadataRecord {
                routine_id: job.job_id.clone(),
                trigger_kind: RoutineTriggerKind::Schedule,
                trigger_payload_json: build_schedule_trigger_payload(job)?,
                execution: RoutineExecutionConfig::default(),
                delivery: RoutineDeliveryConfig::default(),
                quiet_hours: None,
                cooldown_ms: 0,
                approval_policy: RoutineApprovalPolicy::default(),
                template_id: None,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            });
        }
        definitions.routines.retain(|entry| {
            entry.trigger_kind != RoutineTriggerKind::Schedule
                || schedule_ids.contains(&entry.routine_id)
        });
        definitions.routines.sort_by(|left, right| left.routine_id.cmp(&right.routine_id));
        let document = RoutineRegistryDocument {
            schema_version: ROUTINE_REGISTRY_VERSION,
            routines: definitions.routines.clone(),
        };
        drop(definitions);
        write_registry_document(&self.definitions_path, &self.definitions_file, &document)
    }

    pub fn list_run_metadata(
        &self,
        routine_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RoutineRunMetadataRecord>, RoutineRegistryError> {
        let normalized_routine_id = match routine_id {
            Some(value) => Some(normalize_identifier(value, "routine_id")?),
            None => None,
        };
        let run_metadata =
            self.run_metadata.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        let mut entries = run_metadata
            .runs
            .iter()
            .rev()
            .filter(|entry| {
                normalized_routine_id
                    .as_ref()
                    .is_none_or(|routine_id| entry.routine_id == *routine_id)
            })
            .take(limit.clamp(1, MAX_ROUTINE_RUN_METADATA_COUNT))
            .cloned()
            .collect::<Vec<_>>();
        entries.reverse();
        Ok(entries)
    }

    pub fn find_run_metadata(
        &self,
        run_id: &str,
    ) -> Result<Option<RoutineRunMetadataRecord>, RoutineRegistryError> {
        let normalized = normalize_identifier(run_id, "run_id")?;
        let run_metadata =
            self.run_metadata.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        Ok(run_metadata.runs.iter().find(|entry| entry.run_id == normalized).cloned())
    }

    pub fn seen_dedupe_key(
        &self,
        routine_id: &str,
        dedupe_key: &str,
    ) -> Result<bool, RoutineRegistryError> {
        let normalized_routine_id = normalize_identifier(routine_id, "routine_id")?;
        let normalized_dedupe_key =
            normalize_freeform_identifier(dedupe_key, "trigger_dedupe_key")?;
        let run_metadata =
            self.run_metadata.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        Ok(run_metadata.runs.iter().any(|entry| {
            entry.routine_id == normalized_routine_id
                && entry.trigger_dedupe_key.as_deref() == Some(normalized_dedupe_key.as_str())
        }))
    }

    pub fn upsert_run_metadata(
        &self,
        request: RoutineRunMetadataUpsert,
    ) -> Result<RoutineRunMetadataRecord, RoutineRegistryError> {
        let now = unix_ms_now()?;
        let normalized = normalize_routine_run_metadata_upsert(request, now)?;
        let mut run_metadata =
            self.run_metadata.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
        if let Some(existing) =
            run_metadata.runs.iter_mut().find(|entry| entry.run_id == normalized.run_id)
        {
            existing.trigger_kind = normalized.trigger_kind;
            existing.trigger_reason = normalized.trigger_reason;
            existing.trigger_payload_json = normalized.trigger_payload_json;
            existing.trigger_dedupe_key = normalized.trigger_dedupe_key;
            existing.execution = normalized.execution;
            existing.delivery = normalized.delivery;
            existing.dispatch_mode = normalized.dispatch_mode;
            existing.source_run_id = normalized.source_run_id;
            existing.outcome_override = normalized.outcome_override;
            existing.outcome_message = normalized.outcome_message;
            existing.output_delivered = normalized.output_delivered;
            existing.skip_reason = normalized.skip_reason;
            existing.delivery_reason = normalized.delivery_reason;
            existing.approval_note = normalized.approval_note;
            existing.safety_note = normalized.safety_note;
            existing.updated_at_unix_ms = now;
            let updated = existing.clone();
            let document = RoutineRunMetadataDocument {
                schema_version: ROUTINE_REGISTRY_VERSION,
                runs: run_metadata.runs.clone(),
            };
            drop(run_metadata);
            write_registry_document(&self.run_metadata_path, &self.run_metadata_file, &document)?;
            return Ok(updated);
        }
        run_metadata.runs.push(normalized.clone());
        if run_metadata.runs.len() > MAX_ROUTINE_RUN_METADATA_COUNT {
            let overflow = run_metadata.runs.len() - MAX_ROUTINE_RUN_METADATA_COUNT;
            run_metadata.runs.drain(0..overflow);
        }
        let document = RoutineRunMetadataDocument {
            schema_version: ROUTINE_REGISTRY_VERSION,
            runs: run_metadata.runs.clone(),
        };
        drop(run_metadata);
        write_registry_document(&self.run_metadata_path, &self.run_metadata_file, &document)?;
        Ok(normalized)
    }
}

pub fn resolve_routines_root(state_root: Option<&Path>) -> Result<PathBuf, RoutineRegistryError> {
    let root = match state_root {
        Some(path) => path.to_path_buf(),
        None => default_state_root()?,
    };
    let routines_root = root.join(ROUTINES_DIR);
    fs::create_dir_all(routines_root.as_path()).map_err(|source| {
        RoutineRegistryError::WriteRegistry { path: routines_root.clone(), source }
    })?;
    Ok(routines_root)
}

#[must_use]
pub fn shadow_manual_schedule_payload_json() -> String {
    json!({ "timestamp_rfc3339": SHADOW_AT_TIMESTAMP_RFC3339 }).to_string()
}

#[must_use]
pub fn default_outcome_from_cron_status(status: CronRunStatus) -> RoutineRunOutcomeKind {
    match status {
        CronRunStatus::Succeeded => RoutineRunOutcomeKind::SuccessWithOutput,
        CronRunStatus::Skipped => RoutineRunOutcomeKind::Skipped,
        CronRunStatus::Denied => RoutineRunOutcomeKind::Denied,
        CronRunStatus::Failed => RoutineRunOutcomeKind::Failed,
        CronRunStatus::Accepted | CronRunStatus::Running => {
            RoutineRunOutcomeKind::SuccessWithOutput
        }
    }
}

pub fn join_run_metadata(
    routine_id: &str,
    run: &CronRunRecord,
    metadata: Option<&RoutineRunMetadataRecord>,
) -> Value {
    let outcome_kind = metadata
        .and_then(|entry| entry.outcome_override)
        .unwrap_or_else(|| default_outcome_from_cron_status(run.status));
    let execution = metadata.map(|entry| entry.execution.clone()).unwrap_or_default();
    let delivery = metadata.map(|entry| entry.delivery.clone()).unwrap_or_default();
    let output_delivered = metadata
        .and_then(|entry| entry.output_delivered)
        .unwrap_or_else(|| delivery_announced_for_outcome(&delivery, outcome_kind));
    let delivery_reason = metadata
        .and_then(|entry| entry.delivery_reason.clone())
        .unwrap_or_else(|| delivery_reason_for_outcome(&delivery, outcome_kind));
    let effective_delivery = effective_delivery_target(
        &delivery,
        matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied),
    );
    json!({
        "routine_id": routine_id,
        "run_id": run.run_id,
        "status": run.status.as_str(),
        "outcome_kind": outcome_kind.as_str(),
        "outcome_message": metadata.and_then(|entry| entry.outcome_message.clone()).or_else(|| run.error_message_redacted.clone()),
        "error_kind": run.error_kind,
        "trigger_kind": metadata.map(|entry| entry.trigger_kind.as_str()).unwrap_or(RoutineTriggerKind::Schedule.as_str()),
        "trigger_reason": metadata.and_then(|entry| entry.trigger_reason.clone()),
        "trigger_payload": metadata.and_then(|entry| serde_json::from_str::<Value>(&entry.trigger_payload_json).ok()).unwrap_or_else(|| json!({})),
        "run_mode": execution.run_mode.as_str(),
        "execution_posture": execution.execution_posture.as_str(),
        "procedure_profile_id": execution.procedure_profile_id,
        "skill_profile_id": execution.skill_profile_id,
        "provider_profile_id": execution.provider_profile_id,
        "provider_routing": provider_routing_preview(&execution),
        "delivery_mode": delivery.mode.as_str(),
        "delivery_channel": delivery.channel,
        "delivery_failure_mode": delivery.failure_mode.map(RoutineDeliveryMode::as_str),
        "delivery_failure_channel": delivery.failure_channel,
        "silent_policy": delivery.silent_policy.as_str(),
        "delivery_preview": routine_delivery_preview(&delivery),
        "effective_delivery_mode": effective_delivery.mode.as_str(),
        "effective_delivery_channel": effective_delivery.channel,
        "delivery_reason": delivery_reason,
        "dispatch_mode": metadata.map(|entry| entry.dispatch_mode.as_str()).unwrap_or(RoutineDispatchMode::Normal.as_str()),
        "source_run_id": metadata.and_then(|entry| entry.source_run_id.clone()),
        "skip_reason": metadata.and_then(|entry| entry.skip_reason.clone()).or_else(|| run.error_kind.clone()),
        "approval_note": metadata.and_then(|entry| entry.approval_note.clone()),
        "safety_note": metadata.and_then(|entry| entry.safety_note.clone()),
        "output_delivered": output_delivered,
        "attempt": run.attempt,
        "session_id": run.session_id,
        "orchestrator_run_id": run.orchestrator_run_id,
        "started_at_unix_ms": run.started_at_unix_ms,
        "finished_at_unix_ms": run.finished_at_unix_ms,
        "model_tokens_in": run.model_tokens_in,
        "model_tokens_out": run.model_tokens_out,
        "tool_calls": run.tool_calls,
        "tool_denies": run.tool_denies,
    })
}

pub fn build_routine_export_bundle(
    job: &CronJobRecord,
    routine: &RoutineMetadataRecord,
) -> Result<RoutineExportBundle, RoutineRegistryError> {
    Ok(RoutineExportBundle {
        schema_id: ROUTINE_EXPORT_SCHEMA_ID.to_owned(),
        schema_version: ROUTINE_EXPORT_SCHEMA_VERSION,
        exported_at_unix_ms: unix_ms_now()?,
        routine: routine.clone(),
        job: job.clone(),
    })
}

pub fn validate_routine_export_bundle(
    bundle: &RoutineExportBundle,
) -> Result<(), RoutineRegistryError> {
    if bundle.schema_id.trim() != ROUTINE_EXPORT_SCHEMA_ID {
        return Err(RoutineRegistryError::InvalidField {
            field: "schema_id",
            message: format!(
                "unsupported routine export schema '{}'; expected {}",
                bundle.schema_id, ROUTINE_EXPORT_SCHEMA_ID
            ),
        });
    }
    if bundle.schema_version != ROUTINE_EXPORT_SCHEMA_VERSION {
        return Err(RoutineRegistryError::InvalidField {
            field: "schema_version",
            message: format!(
                "unsupported routine export schema version {}; expected {}",
                bundle.schema_version, ROUTINE_EXPORT_SCHEMA_VERSION
            ),
        });
    }
    Ok(())
}

#[must_use]
pub fn routine_templates() -> Vec<RoutineTemplateDefinition> {
    vec![
        RoutineTemplateDefinition {
            template_id: "heartbeat".to_owned(),
            title: "Heartbeat".to_owned(),
            description: "Lightweight liveness check that posts a brief status heartbeat on a fixed cadence."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Schedule,
            default_name: "Heartbeat".to_owned(),
            prompt: "Check system heartbeat, summarize current status in one short paragraph, and include only actionable anomalies."
                .to_owned(),
            natural_language_schedule: Some("every weekday at 9".to_owned()),
            delivery_mode: RoutineDeliveryMode::SameChannel,
            approval_mode: RoutineApprovalMode::None,
            tags: vec!["status".to_owned(), "ops".to_owned()],
        },
        RoutineTemplateDefinition {
            template_id: "daily-report".to_owned(),
            title: "Daily report".to_owned(),
            description: "Collect a compact daily operational report for a chosen channel or team inbox."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Schedule,
            default_name: "Daily report".to_owned(),
            prompt: "Prepare a daily report covering incidents, pending approvals, and notable usage changes. Keep the output concise and operator-focused."
                .to_owned(),
            natural_language_schedule: Some("every weekday at 17".to_owned()),
            delivery_mode: RoutineDeliveryMode::SpecificChannel,
            approval_mode: RoutineApprovalMode::None,
            tags: vec!["report".to_owned(), "ops".to_owned()],
        },
        RoutineTemplateDefinition {
            template_id: "follow-up".to_owned(),
            title: "Follow-up".to_owned(),
            description: "Reusable follow-up template for manual or event-driven reminders."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Manual,
            default_name: "Follow-up".to_owned(),
            prompt: "Review the latest context for the routine target and draft a short follow-up with next steps only."
                .to_owned(),
            natural_language_schedule: None,
            delivery_mode: RoutineDeliveryMode::SameChannel,
            approval_mode: RoutineApprovalMode::None,
            tags: vec!["workflow".to_owned(), "reminder".to_owned()],
        },
        RoutineTemplateDefinition {
            template_id: "change-check".to_owned(),
            title: "Change check".to_owned(),
            description: "Periodic check for recent repository or environment changes that deserve operator attention."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Schedule,
            default_name: "Change check".to_owned(),
            prompt: "Inspect recent changes, call out risky diffs or regressions, and highlight only material updates that need action."
                .to_owned(),
            natural_language_schedule: Some("every 2h".to_owned()),
            delivery_mode: RoutineDeliveryMode::LogsOnly,
            approval_mode: RoutineApprovalMode::BeforeFirstRun,
            tags: vec!["changes".to_owned(), "review".to_owned()],
        },
        RoutineTemplateDefinition {
            template_id: "document-ingest".to_owned(),
            title: "Document ingest".to_owned(),
            description: "Process new document payloads coming from a webhook or manual fire and turn them into structured summaries."
                .to_owned(),
            trigger_kind: RoutineTriggerKind::Webhook,
            default_name: "Document ingest".to_owned(),
            prompt: "Inspect the incoming document payload, produce a summary, and extract durable facts that are safe to index."
                .to_owned(),
            natural_language_schedule: None,
            delivery_mode: RoutineDeliveryMode::LocalOnly,
            approval_mode: RoutineApprovalMode::BeforeFirstRun,
            tags: vec!["documents".to_owned(), "ingest".to_owned()],
        },
    ]
}

pub fn natural_language_schedule_preview(
    phrase: &str,
    timezone_mode: CronTimezoneMode,
    now_unix_ms: i64,
) -> Result<RoutineSchedulePreview, RoutineRegistryError> {
    let normalized_phrase = phrase.trim();
    if normalized_phrase.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field: "phrase",
            message: "phrase cannot be empty".to_owned(),
        });
    }

    if let Some(parsed) = parse_relative_phrase(normalized_phrase, now_unix_ms)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Some(parsed) = parse_interval_phrase(normalized_phrase)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Some(parsed) = parse_weekday_phrase(normalized_phrase)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Some(parsed) = parse_daily_phrase(normalized_phrase)? {
        return preview_from_schedule(normalized_phrase, timezone_mode, now_unix_ms, parsed);
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(normalized_phrase) {
        let timestamp = timestamp.with_timezone(&Utc);
        return preview_from_schedule(
            normalized_phrase,
            timezone_mode,
            now_unix_ms,
            ParsedNaturalLanguageSchedule {
                normalized_text: timestamp.to_rfc3339(),
                explanation: format!(
                    "Interpreted as one explicit timestamp in {} mode.",
                    timezone_mode.as_str()
                ),
                schedule: cron_v1::Schedule {
                    r#type: cron_v1::ScheduleType::At as i32,
                    spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                        timestamp_rfc3339: timestamp.to_rfc3339(),
                    })),
                },
            },
        );
    }

    Err(RoutineRegistryError::InvalidField {
        field: "phrase",
        message: "supported phrases include 'in 30 minutes', 'za 30 minut', 'every 2h', 'every weekday at 9', 'každý pracovní den v 9', or an RFC3339 timestamp".to_owned(),
    })
}

fn build_schedule_trigger_payload(job: &CronJobRecord) -> Result<String, RoutineRegistryError> {
    serde_json::to_string(&json!({
        "schedule_type": job.schedule_type.as_str(),
        "schedule_payload": serde_json::from_str::<Value>(job.schedule_payload_json.as_str()).unwrap_or_else(|_| json!({ "raw": job.schedule_payload_json })),
    }))
    .map_err(Into::into)
}

fn normalize_routine_metadata_upsert(
    request: RoutineMetadataUpsert,
    now: i64,
) -> Result<RoutineMetadataRecord, RoutineRegistryError> {
    Ok(RoutineMetadataRecord {
        routine_id: normalize_identifier(request.routine_id.as_str(), "routine_id")?,
        trigger_kind: request.trigger_kind,
        trigger_payload_json: normalize_payload_json(
            request.trigger_payload_json,
            "trigger_payload_json",
        )?,
        execution: normalize_execution(request.execution)?,
        delivery: normalize_delivery(request.delivery)?,
        quiet_hours: normalize_quiet_hours(request.quiet_hours)?,
        cooldown_ms: request.cooldown_ms,
        approval_policy: request.approval_policy,
        template_id: request.template_id.and_then(trim_to_option),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    })
}

fn normalize_routine_run_metadata_upsert(
    request: RoutineRunMetadataUpsert,
    now: i64,
) -> Result<RoutineRunMetadataRecord, RoutineRegistryError> {
    Ok(RoutineRunMetadataRecord {
        run_id: normalize_identifier(request.run_id.as_str(), "run_id")?,
        routine_id: normalize_identifier(request.routine_id.as_str(), "routine_id")?,
        trigger_kind: request.trigger_kind,
        trigger_reason: request.trigger_reason.and_then(trim_to_option),
        trigger_payload_json: normalize_payload_json(
            request.trigger_payload_json,
            "trigger_payload_json",
        )?,
        trigger_dedupe_key: request
            .trigger_dedupe_key
            .map(|value| normalize_freeform_identifier(value.as_str(), "trigger_dedupe_key"))
            .transpose()?,
        execution: normalize_execution(request.execution)?,
        delivery: normalize_delivery(request.delivery)?,
        dispatch_mode: request.dispatch_mode,
        source_run_id: request
            .source_run_id
            .map(|value| normalize_identifier(value.as_str(), "source_run_id"))
            .transpose()?,
        outcome_override: request.outcome_override,
        outcome_message: request.outcome_message.and_then(trim_to_option),
        output_delivered: request.output_delivered,
        skip_reason: request.skip_reason.and_then(trim_to_option),
        delivery_reason: request.delivery_reason.and_then(trim_to_option),
        approval_note: request.approval_note.and_then(trim_to_option),
        safety_note: request.safety_note.and_then(trim_to_option),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    })
}

fn normalize_execution(
    execution: RoutineExecutionConfig,
) -> Result<RoutineExecutionConfig, RoutineRegistryError> {
    Ok(RoutineExecutionConfig {
        run_mode: execution.run_mode,
        procedure_profile_id: execution
            .procedure_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.procedure_profile_id"))
            .transpose()?,
        skill_profile_id: execution
            .skill_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.skill_profile_id"))
            .transpose()?,
        provider_profile_id: execution
            .provider_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.provider_profile_id"))
            .transpose()?,
        execution_posture: execution.execution_posture,
    })
}

fn normalize_delivery(
    delivery: RoutineDeliveryConfig,
) -> Result<RoutineDeliveryConfig, RoutineRegistryError> {
    let channel = delivery.channel.and_then(trim_to_option);
    let failure_channel = delivery.failure_channel.and_then(trim_to_option);
    if matches!(delivery.mode, RoutineDeliveryMode::SpecificChannel) && channel.is_none() {
        return Err(RoutineRegistryError::InvalidField {
            field: "delivery.channel",
            message: "delivery.channel is required for delivery.mode=specific_channel".to_owned(),
        });
    }
    if matches!(delivery.failure_mode, Some(RoutineDeliveryMode::SpecificChannel))
        && failure_channel.as_ref().or(channel.as_ref()).is_none()
    {
        return Err(RoutineRegistryError::InvalidField {
            field: "delivery.failure_channel",
            message: "delivery.failure_channel or delivery.channel is required for failure_mode=specific_channel".to_owned(),
        });
    }
    Ok(RoutineDeliveryConfig {
        mode: delivery.mode,
        channel,
        failure_mode: delivery.failure_mode,
        failure_channel,
        silent_policy: delivery.silent_policy,
    })
}

fn normalize_quiet_hours(
    quiet_hours: Option<RoutineQuietHours>,
) -> Result<Option<RoutineQuietHours>, RoutineRegistryError> {
    let Some(quiet_hours) = quiet_hours else {
        return Ok(None);
    };
    if quiet_hours.start_minute_of_day >= 1_440 || quiet_hours.end_minute_of_day >= 1_440 {
        return Err(RoutineRegistryError::InvalidField {
            field: "quiet_hours",
            message: "quiet hours minute-of-day values must be between 0 and 1439".to_owned(),
        });
    }
    Ok(Some(RoutineQuietHours {
        start_minute_of_day: quiet_hours.start_minute_of_day,
        end_minute_of_day: quiet_hours.end_minute_of_day,
        timezone: quiet_hours.timezone.and_then(trim_to_option),
    }))
}

pub fn validate_routine_prompt_self_contained(
    prompt: &str,
    execution: &RoutineExecutionConfig,
) -> Result<(), RoutineRegistryError> {
    let trimmed = prompt.trim();
    if trimmed.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field: "prompt",
            message: "prompt cannot be empty".to_owned(),
        });
    }
    if execution.run_mode != RoutineRunMode::FreshSession {
        return Ok(());
    }

    const FRAGILE_PROMPT_MARKERS: &[&str] = &[
        "as above",
        "same as before",
        "previous context",
        "prior context",
        "resume where you left off",
        "pick up where you left off",
        "continue from earlier",
        "the earlier thread",
        "the conversation above",
    ];
    let normalized = trimmed.to_ascii_lowercase();
    if let Some(marker) =
        FRAGILE_PROMPT_MARKERS.iter().copied().find(|marker| normalized.contains(marker))
    {
        return Err(RoutineRegistryError::InvalidField {
            field: "prompt",
            message: format!(
                "fresh-session routines must stay self-contained; remove fragile context reference '{marker}'"
            ),
        });
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct EffectiveDeliveryTarget {
    mode: RoutineDeliveryMode,
    channel: Option<String>,
}

fn effective_delivery_target(
    delivery: &RoutineDeliveryConfig,
    failure_path: bool,
) -> EffectiveDeliveryTarget {
    if failure_path {
        EffectiveDeliveryTarget {
            mode: delivery.failure_mode.unwrap_or(delivery.mode),
            channel: delivery.failure_channel.clone().or_else(|| delivery.channel.clone()),
        }
    } else {
        EffectiveDeliveryTarget { mode: delivery.mode, channel: delivery.channel.clone() }
    }
}

fn delivery_announced_for_outcome(
    delivery: &RoutineDeliveryConfig,
    outcome_kind: RoutineRunOutcomeKind,
) -> bool {
    if delivery.silent_policy == RoutineSilentPolicy::AuditOnly {
        return false;
    }
    let failure_path =
        matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied);
    if !failure_path && delivery.silent_policy == RoutineSilentPolicy::FailureOnly {
        return false;
    }
    let effective = effective_delivery_target(delivery, failure_path);
    !matches!(effective.mode, RoutineDeliveryMode::LocalOnly | RoutineDeliveryMode::LogsOnly)
}

fn delivery_reason_for_outcome(
    delivery: &RoutineDeliveryConfig,
    outcome_kind: RoutineRunOutcomeKind,
) -> String {
    if delivery.silent_policy == RoutineSilentPolicy::AuditOnly {
        return "delivery suppressed by silent_policy=audit_only; audit trail remains available"
            .to_owned();
    }
    let failure_path =
        matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied);
    if !failure_path && delivery.silent_policy == RoutineSilentPolicy::FailureOnly {
        return "successful runs stay silent; failures still use the configured failure target"
            .to_owned();
    }
    let effective = effective_delivery_target(delivery, failure_path);
    match effective.mode {
        RoutineDeliveryMode::LocalOnly => {
            "delivery stays local to the automation session and is not announced externally"
                .to_owned()
        }
        RoutineDeliveryMode::LogsOnly => {
            "delivery is restricted to logs and diagnostics surfaces".to_owned()
        }
        RoutineDeliveryMode::SameChannel => {
            "delivery is eligible for the routine origin channel".to_owned()
        }
        RoutineDeliveryMode::SpecificChannel => format!(
            "delivery is eligible for explicit channel {}",
            effective.channel.unwrap_or_else(|| "unknown".to_owned())
        ),
    }
}

fn provider_routing_preview(execution: &RoutineExecutionConfig) -> Value {
    if let Some(profile_id) = execution.provider_profile_id.as_ref() {
        json!({
            "mode": "pinned",
            "profile_id": profile_id,
        })
    } else {
        json!({
            "mode": "auto",
        })
    }
}

pub fn routine_delivery_preview(delivery: &RoutineDeliveryConfig) -> Value {
    let success_target = effective_delivery_target(delivery, false);
    let failure_target = effective_delivery_target(delivery, true);
    json!({
        "silent_policy": delivery.silent_policy.as_str(),
        "success": {
            "mode": success_target.mode.as_str(),
            "channel": success_target.channel,
            "announced": delivery_announced_for_outcome(delivery, RoutineRunOutcomeKind::SuccessWithOutput),
            "reason": delivery_reason_for_outcome(delivery, RoutineRunOutcomeKind::SuccessWithOutput),
        },
        "failure": {
            "mode": failure_target.mode.as_str(),
            "channel": failure_target.channel,
            "announced": delivery_announced_for_outcome(delivery, RoutineRunOutcomeKind::Failed),
            "reason": delivery_reason_for_outcome(delivery, RoutineRunOutcomeKind::Failed),
        },
    })
}

fn normalize_payload_json(
    payload_json: String,
    field: &'static str,
) -> Result<String, RoutineRegistryError> {
    let parsed = serde_json::from_str::<Value>(payload_json.as_str()).map_err(|error| {
        RoutineRegistryError::InvalidField {
            field,
            message: format!("payload must be valid JSON: {error}"),
        }
    })?;
    serde_json::to_string(&parsed).map_err(Into::into)
}

fn normalize_identifier(raw: &str, field: &'static str) -> Result<String, RoutineRegistryError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    if trimmed.len() > 128 {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "value must be 128 bytes or fewer".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn normalize_freeform_identifier(
    raw: &str,
    field: &'static str,
) -> Result<String, RoutineRegistryError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    if trimmed.len() > 256 {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "value must be 256 bytes or fewer".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn trim_to_option(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn unix_ms_now() -> Result<i64, RoutineRegistryError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(now.as_millis() as i64)
}

fn open_registry_file(path: &RegistryPath) -> Result<fs::File, RoutineRegistryError> {
    let parent = path.as_path().parent().ok_or_else(|| RoutineRegistryError::WriteRegistry {
        path: path.to_path_buf(),
        source: std::io::Error::other("routine registry path has no parent"),
    })?;
    fs::create_dir_all(parent).map_err(|source| RoutineRegistryError::WriteRegistry {
        path: parent.to_path_buf(),
        source,
    })?;
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.as_path())
        .map_err(|source| RoutineRegistryError::WriteRegistry { path: path.to_path_buf(), source })
}

fn load_registry_document(
    path: &RegistryPath,
    file: &mut fs::File,
) -> Result<RoutineRegistryDocument, RoutineRegistryError> {
    load_json_document(path, file)
}

fn load_run_metadata_document(
    path: &RegistryPath,
    file: &mut fs::File,
) -> Result<RoutineRunMetadataDocument, RoutineRegistryError> {
    load_json_document(path, file)
}

fn load_json_document<T>(
    path: &RegistryPath,
    file: &mut fs::File,
) -> Result<T, RoutineRegistryError>
where
    T: for<'de> Deserialize<'de> + Default + HasSchemaVersion,
{
    file.seek(SeekFrom::Start(0)).map_err(|source| RoutineRegistryError::ReadRegistry {
        path: path.to_path_buf(),
        source,
    })?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).map_err(|source| RoutineRegistryError::ReadRegistry {
        path: path.to_path_buf(),
        source,
    })?;
    if buffer.is_empty() {
        return Ok(T::default());
    }
    let parsed = serde_json::from_slice::<T>(&buffer).map_err(|source| {
        RoutineRegistryError::ParseRegistry { path: path.to_path_buf(), source }
    })?;
    if parsed.schema_version() != ROUTINE_REGISTRY_VERSION {
        return Err(RoutineRegistryError::UnsupportedVersion(parsed.schema_version()));
    }
    Ok(parsed)
}

fn write_registry_document<T>(
    path: &RegistryPath,
    file: &Mutex<fs::File>,
    document: &T,
) -> Result<(), RoutineRegistryError>
where
    T: Serialize,
{
    let payload = serde_json::to_vec_pretty(document)?;
    let mut file = file.lock().map_err(|_| RoutineRegistryError::LockPoisoned)?;
    file.set_len(0).map_err(|source| RoutineRegistryError::WriteRegistry {
        path: path.to_path_buf(),
        source,
    })?;
    file.seek(SeekFrom::Start(0)).map_err(|source| RoutineRegistryError::WriteRegistry {
        path: path.to_path_buf(),
        source,
    })?;
    file.write_all(payload.as_slice()).map_err(|source| RoutineRegistryError::WriteRegistry {
        path: path.to_path_buf(),
        source,
    })?;
    file.flush().map_err(|source| RoutineRegistryError::WriteRegistry {
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all()
        .map_err(|source| RoutineRegistryError::WriteRegistry { path: path.to_path_buf(), source })
}

trait HasSchemaVersion {
    fn schema_version(&self) -> u32;
}

impl HasSchemaVersion for RoutineRegistryDocument {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

impl HasSchemaVersion for RoutineRunMetadataDocument {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

#[derive(Debug, Clone)]
struct ParsedNaturalLanguageSchedule {
    normalized_text: String,
    explanation: String,
    schedule: cron_v1::Schedule,
}

fn preview_from_schedule(
    phrase: &str,
    timezone_mode: CronTimezoneMode,
    now_unix_ms: i64,
    parsed: ParsedNaturalLanguageSchedule,
) -> Result<RoutineSchedulePreview, RoutineRegistryError> {
    let normalized = cron::normalize_schedule(Some(parsed.schedule), now_unix_ms, timezone_mode)
        .map_err(|error| RoutineRegistryError::InvalidField {
            field: "phrase",
            message: error.message().to_owned(),
        })?;
    let schedule_payload = serde_json::from_str::<Value>(normalized.schedule_payload_json.as_str())
        .map_err(RoutineRegistryError::SerializeRegistry)?;
    Ok(RoutineSchedulePreview {
        phrase: phrase.trim().to_owned(),
        normalized_text: parsed.normalized_text,
        explanation: parsed.explanation,
        schedule_type: normalized.schedule_type.as_str().to_owned(),
        schedule_payload_json: normalized.schedule_payload_json,
        schedule_payload,
        next_run_at_unix_ms: normalized.next_run_at_unix_ms,
        timezone: timezone_mode.as_str().to_owned(),
    })
}

fn parse_relative_phrase(
    phrase: &str,
    now_unix_ms: i64,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    let (quantity, unit) = match tokens.as_slice() {
        ["in", quantity, unit] => (*quantity, *unit),
        ["za", quantity, unit] => (*quantity, *unit),
        _ => return Ok(None),
    };
    let duration_ms = parse_duration_to_ms(quantity, unit, "phrase")?;
    let now = Utc.timestamp_millis_opt(now_unix_ms).single().ok_or_else(|| {
        RoutineRegistryError::InvalidField {
            field: "phrase",
            message: "current timestamp could not be resolved".to_owned(),
        }
    })?;
    let target = now
        .checked_add_signed(ChronoDuration::milliseconds(duration_ms as i64))
        .ok_or_else(|| RoutineRegistryError::InvalidField {
            field: "phrase",
            message: "relative schedule overflows supported timestamp range".to_owned(),
        })?;
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: target.to_rfc3339(),
        explanation: format!(
            "Interpreted as a one-time run {} from now.",
            humanize_duration(duration_ms)
        ),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::At as i32,
            spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                timestamp_rfc3339: target.to_rfc3339(),
            })),
        },
    }))
}

fn parse_interval_phrase(
    phrase: &str,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    let (quantity, unit) = match tokens.as_slice() {
        ["every", compact] => split_compact_duration(compact).unwrap_or(("", "")),
        ["every", quantity, unit] => (*quantity, *unit),
        ["každé", compact] | ["kazde", compact] => {
            split_compact_duration(compact).unwrap_or(("", ""))
        }
        ["každé", quantity, unit] | ["kazde", quantity, unit] => (*quantity, *unit),
        _ => return Ok(None),
    };
    if quantity.is_empty() || unit.is_empty() {
        return Ok(None);
    }
    let interval_ms = parse_duration_to_ms(quantity, unit, "phrase")?;
    if interval_ms < MIN_EVERY_INTERVAL_MS {
        return Err(RoutineRegistryError::InvalidField {
            field: "phrase",
            message: format!(
                "repeating schedules must be at least {} minutes apart",
                MIN_EVERY_INTERVAL_MS / 60_000
            ),
        });
    }
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: format!("every {}", humanize_duration(interval_ms)),
        explanation: format!(
            "Interpreted as a repeating interval of {}.",
            humanize_duration(interval_ms)
        ),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule { interval_ms })),
        },
    }))
}

fn parse_weekday_phrase(
    phrase: &str,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let prefix = if normalized.starts_with("every weekday at ") {
        "every weekday at "
    } else if normalized.starts_with("každý pracovní den v ") {
        "každý pracovní den v "
    } else if normalized.starts_with("kazdy pracovni den v ") {
        "kazdy pracovni den v "
    } else {
        return Ok(None);
    };
    let time = parse_time_components(normalized.trim_start_matches(prefix), "phrase")?;
    let expression = format!("{} {} * * 1-5", time.minute, time.hour);
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: format!("weekdays at {:02}:{:02}", time.hour, time.minute),
        explanation: "Interpreted as every weekday at a fixed local/UTC wall clock time."
            .to_owned(),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule { expression })),
        },
    }))
}

fn parse_daily_phrase(
    phrase: &str,
) -> Result<Option<ParsedNaturalLanguageSchedule>, RoutineRegistryError> {
    let normalized = normalize_phrase(phrase);
    let prefix = if normalized.starts_with("daily at ") {
        "daily at "
    } else if normalized.starts_with("denně v ") {
        "denně v "
    } else if normalized.starts_with("denne v ") {
        "denne v "
    } else {
        return Ok(None);
    };
    let time = parse_time_components(normalized.trim_start_matches(prefix), "phrase")?;
    let expression = format!("{} {} * * *", time.minute, time.hour);
    Ok(Some(ParsedNaturalLanguageSchedule {
        normalized_text: format!("daily at {:02}:{:02}", time.hour, time.minute),
        explanation: "Interpreted as a daily wall clock schedule.".to_owned(),
        schedule: cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule { expression })),
        },
    }))
}

fn normalize_phrase(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
}

fn split_compact_duration(value: &str) -> Option<(&str, &str)> {
    let digits = value
        .char_indices()
        .take_while(|(_, character)| character.is_ascii_digit())
        .last()
        .map(|(index, character)| index + character.len_utf8())
        .unwrap_or(0);
    if digits == 0 || digits >= value.len() {
        return None;
    }
    Some(value.split_at(digits))
}

fn parse_duration_to_ms(
    quantity: &str,
    unit: &str,
    field: &'static str,
) -> Result<u64, RoutineRegistryError> {
    let quantity = quantity.parse::<u64>().map_err(|_| RoutineRegistryError::InvalidField {
        field,
        message: format!("duration quantity '{quantity}' must be numeric"),
    })?;
    if quantity == 0 {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: "duration quantity must be greater than zero".to_owned(),
        });
    }
    let normalized = unit.trim().to_lowercase();
    let multiplier = match normalized.as_str() {
        "m" | "min" | "mins" | "minute" | "minutes" | "minuta" | "minuty" | "minut" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" | "hod" | "hodina" | "hodiny" | "hodin" => {
            60 * 60_000
        }
        "d" | "day" | "days" | "den" | "dny" | "dni" => 24 * 60 * 60_000,
        _ => {
            return Err(RoutineRegistryError::InvalidField {
                field,
                message: format!("unsupported duration unit '{unit}'"),
            })
        }
    };
    quantity.checked_mul(multiplier).ok_or_else(|| RoutineRegistryError::InvalidField {
        field,
        message: "duration is too large".to_owned(),
    })
}

#[derive(Debug, Clone, Copy)]
struct ParsedTimeOfDay {
    hour: u8,
    minute: u8,
}

fn parse_time_components(
    raw: &str,
    field: &'static str,
) -> Result<ParsedTimeOfDay, RoutineRegistryError> {
    let trimmed = raw.trim();
    let (hour, minute) = if let Some((hour, minute)) = trimmed.split_once(':') {
        (hour, minute)
    } else {
        (trimmed, "0")
    };
    let hour = hour.parse::<u8>().map_err(|_| RoutineRegistryError::InvalidField {
        field,
        message: format!("time '{trimmed}' must use hour or hour:minute format"),
    })?;
    let minute = minute.parse::<u8>().map_err(|_| RoutineRegistryError::InvalidField {
        field,
        message: format!("time '{trimmed}' must use hour or hour:minute format"),
    })?;
    if hour > 23 || minute > 59 {
        return Err(RoutineRegistryError::InvalidField {
            field,
            message: format!("time '{trimmed}' must stay within 00:00-23:59"),
        });
    }
    Ok(ParsedTimeOfDay { hour, minute })
}

fn humanize_duration(duration_ms: u64) -> String {
    if duration_ms.is_multiple_of(60 * 60 * 1_000) {
        format!("{} hour(s)", duration_ms / (60 * 60 * 1_000))
    } else if duration_ms.is_multiple_of(60 * 1_000) {
        format!("{} minute(s)", duration_ms / (60 * 1_000))
    } else {
        format!("{} ms", duration_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_routine_export_bundle, default_outcome_from_cron_status,
        natural_language_schedule_preview, resolve_routines_root, routine_delivery_preview,
        shadow_manual_schedule_payload_json, validate_routine_export_bundle,
        validate_routine_prompt_self_contained, RoutineApprovalMode, RoutineApprovalPolicy,
        RoutineDeliveryConfig, RoutineDeliveryMode, RoutineExecutionConfig, RoutineRegistry,
        RoutineRunMetadataUpsert, RoutineRunMode, RoutineSilentPolicy, RoutineTriggerKind,
        ROUTINE_EXPORT_SCHEMA_ID,
    };
    use crate::{
        cron::CronTimezoneMode,
        journal::{CronRunStatus, CronScheduleType},
    };
    use chrono::DateTime;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_state_root() -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("palyra-routines-test-{stamp}"))
    }

    #[test]
    fn resolve_routines_root_creates_directory() {
        let root = temp_state_root();
        let routines_root =
            resolve_routines_root(Some(root.as_path())).expect("root should resolve");
        assert!(routines_root.exists(), "routines directory should exist");
    }

    #[test]
    fn registry_round_trips_metadata_and_run_metadata() {
        let root = temp_state_root();
        let registry = RoutineRegistry::open(root.as_path()).expect("registry should open");
        let created = registry
            .upsert_routine(super::RoutineMetadataUpsert {
                routine_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                trigger_kind: RoutineTriggerKind::Manual,
                trigger_payload_json: json!({ "kind": "manual" }).to_string(),
                execution: RoutineExecutionConfig::default(),
                delivery: RoutineDeliveryConfig {
                    mode: RoutineDeliveryMode::SpecificChannel,
                    channel: Some("system:routines".to_owned()),
                    failure_mode: None,
                    failure_channel: None,
                    silent_policy: RoutineSilentPolicy::Noisy,
                },
                quiet_hours: None,
                cooldown_ms: 60_000,
                approval_policy: RoutineApprovalPolicy {
                    mode: RoutineApprovalMode::BeforeFirstRun,
                },
                template_id: Some("heartbeat".to_owned()),
            })
            .expect("metadata upsert should succeed");
        assert_eq!(created.trigger_kind, RoutineTriggerKind::Manual);

        let run = registry
            .upsert_run_metadata(RoutineRunMetadataUpsert {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                routine_id: created.routine_id.clone(),
                trigger_kind: RoutineTriggerKind::Manual,
                trigger_reason: Some("manual fire".to_owned()),
                trigger_payload_json: json!({ "source": "operator" }).to_string(),
                trigger_dedupe_key: Some("manual:1".to_owned()),
                execution: RoutineExecutionConfig::default(),
                delivery: created.delivery.clone(),
                dispatch_mode: super::RoutineDispatchMode::Normal,
                source_run_id: None,
                outcome_override: None,
                outcome_message: None,
                output_delivered: Some(true),
                skip_reason: None,
                delivery_reason: None,
                approval_note: None,
                safety_note: None,
            })
            .expect("run metadata upsert should succeed");
        assert_eq!(run.trigger_kind, RoutineTriggerKind::Manual);
        assert!(
            registry
                .seen_dedupe_key(created.routine_id.as_str(), "manual:1")
                .expect("dedupe lookup should succeed"),
            "dedupe key should be discoverable"
        );
    }

    #[test]
    fn shadow_manual_schedule_payload_uses_future_timestamp() {
        let payload =
            serde_json::from_str::<serde_json::Value>(&shadow_manual_schedule_payload_json())
                .expect("payload should parse");
        assert_eq!(payload, json!({ "timestamp_rfc3339": "2100-01-01T00:00:00Z" }));
        let _ = CronScheduleType::At;
    }

    #[test]
    fn default_outcome_mapping_tracks_cron_status() {
        assert_eq!(
            default_outcome_from_cron_status(CronRunStatus::Succeeded).as_str(),
            "success_with_output"
        );
        assert_eq!(default_outcome_from_cron_status(CronRunStatus::Skipped).as_str(), "skipped");
    }

    #[test]
    fn natural_language_preview_supports_english_and_czech_inputs() {
        let now = 1_700_000_000_000_i64;
        let english = natural_language_schedule_preview("every 2h", CronTimezoneMode::Utc, now)
            .expect("english schedule preview should parse");
        assert_eq!(english.schedule_type, "every");
        assert_eq!(english.schedule_payload["interval_ms"], json!(7_200_000_u64));

        let czech =
            natural_language_schedule_preview("každý pracovní den v 9", CronTimezoneMode::Utc, now)
                .expect("czech weekday schedule preview should parse");
        assert_eq!(czech.schedule_type, "cron");
        assert_eq!(czech.schedule_payload["expression"], json!("0 9 * * 1-5"));
    }

    #[test]
    fn natural_language_preview_rejects_dangerously_frequent_repeat() {
        let error = natural_language_schedule_preview("every 1m", CronTimezoneMode::Utc, 0)
            .expect_err("too-frequent repeat should be rejected");
        assert!(
            error.to_string().contains("at least"),
            "error should explain minimum repeat interval"
        );
    }

    #[test]
    fn natural_language_preview_preserves_dst_boundary_timestamps() {
        let preview = natural_language_schedule_preview(
            "2026-03-29T03:30:00+02:00",
            CronTimezoneMode::Utc,
            0,
        )
        .expect("dst boundary timestamp should parse");
        assert_eq!(preview.schedule_type, "at");
        let expected = DateTime::parse_from_rfc3339("2026-03-29T03:30:00+02:00")
            .expect("timestamp should parse")
            .timestamp_millis();
        assert_eq!(preview.next_run_at_unix_ms, Some(expected));
    }

    #[test]
    fn routine_export_bundle_round_trips_metadata() {
        let bundle = build_routine_export_bundle(
            &crate::journal::CronJobRecord {
                job_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                name: "Heartbeat".to_owned(),
                prompt: "Ping".to_owned(),
                owner_principal: "user:test".to_owned(),
                channel: "system:routines".to_owned(),
                session_key: None,
                session_label: None,
                schedule_type: CronScheduleType::Every,
                schedule_payload_json: json!({ "interval_ms": 3_600_000_u64 }).to_string(),
                enabled: true,
                concurrency_policy: crate::journal::CronConcurrencyPolicy::Forbid,
                retry_policy: crate::journal::CronRetryPolicy {
                    max_attempts: 1,
                    backoff_ms: 1_000,
                },
                misfire_policy: crate::journal::CronMisfirePolicy::Skip,
                jitter_ms: 0,
                next_run_at_unix_ms: Some(1_700_000_000_000),
                last_run_at_unix_ms: None,
                queued_run: false,
                created_at_unix_ms: 1_700_000_000_000,
                updated_at_unix_ms: 1_700_000_000_000,
            },
            &super::RoutineMetadataRecord {
                routine_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                trigger_kind: RoutineTriggerKind::Schedule,
                trigger_payload_json: json!({ "schedule_type": "every" }).to_string(),
                execution: RoutineExecutionConfig::default(),
                delivery: RoutineDeliveryConfig::default(),
                quiet_hours: None,
                cooldown_ms: 0,
                approval_policy: RoutineApprovalPolicy::default(),
                template_id: Some("heartbeat".to_owned()),
                created_at_unix_ms: 1_700_000_000_000,
                updated_at_unix_ms: 1_700_000_000_000,
            },
        )
        .expect("export bundle should build");
        assert_eq!(bundle.schema_id, ROUTINE_EXPORT_SCHEMA_ID);
        validate_routine_export_bundle(&bundle).expect("bundle should validate");
    }

    #[test]
    fn fresh_session_prompt_validation_rejects_brittle_references() {
        let error = validate_routine_prompt_self_contained(
            "Resume where you left off and keep the same output style.",
            &RoutineExecutionConfig {
                run_mode: RoutineRunMode::FreshSession,
                ..RoutineExecutionConfig::default()
            },
        )
        .expect_err("fresh-session prompt should reject implicit context");
        assert!(
            error.to_string().contains("self-contained"),
            "error should explain the self-contained requirement"
        );
    }

    #[test]
    fn delivery_preview_reflects_failure_only_policy() {
        let preview = routine_delivery_preview(&RoutineDeliveryConfig {
            mode: RoutineDeliveryMode::SameChannel,
            channel: None,
            failure_mode: Some(RoutineDeliveryMode::SpecificChannel),
            failure_channel: Some("ops:alerts".to_owned()),
            silent_policy: RoutineSilentPolicy::FailureOnly,
        });
        assert_eq!(preview["silent_policy"], json!("failure_only"));
        assert_eq!(preview["success"]["announced"], json!(false));
        assert_eq!(preview["failure"]["mode"], json!("specific_channel"));
        assert_eq!(preview["failure"]["channel"], json!("ops:alerts"));
        assert_eq!(preview["failure"]["announced"], json!(true));
    }
}
