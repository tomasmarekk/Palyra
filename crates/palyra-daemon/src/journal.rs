use std::{
    fmt, fs,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use palyra_a2ui::{apply_patch_document, parse_patch_document};
use rusqlite::{params, params_from_iter, Connection, ErrorCode, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::orchestrator::RunLifecycleState;

const REDACTED_MARKER: &str = "<redacted>";
const MAX_RECENT_EVENTS_LIMIT: usize = 500;
const SENSITIVE_KEY_FRAGMENTS: &[&str] = &[
    "secret",
    "token",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "private_key",
    "proof",
    "signature",
];
const SENSITIVE_KEY_TOKENS: &[&str] = &["pin"];
const MAX_CRON_JOBS_LIST_LIMIT: usize = 500;
const MAX_CRON_RUNS_LIST_LIMIT: usize = 500;
const MAX_APPROVALS_LIST_LIMIT: usize = 500;
const MAX_APPROVALS_QUERY_LIMIT: usize = MAX_APPROVALS_LIST_LIMIT + 1;
const MAX_MEMORY_ITEMS_LIST_LIMIT: usize = 500;
const MAX_MEMORY_SEARCH_CANDIDATES: usize = 256;
const MAX_CANVAS_PATCHES_QUERY_LIMIT: usize = 1_000;
const DEFAULT_MEMORY_VECTOR_DIMS: usize = 64;
const DEFAULT_MEMORY_EMBEDDING_MODEL: &str = "hash-embedding-v1";
const MEMORY_RETENTION_DAY_MS: i64 = 24 * 60 * 60 * 1_000;
const MEMORY_MAINTENANCE_STATE_SINGLETON_KEY: i64 = 1;

pub trait MemoryEmbeddingProvider: Send + Sync {
    fn model_name(&self) -> &str;
    fn dimensions(&self) -> usize;
    fn embed_text(&self, text: &str) -> Vec<f32>;
}

#[derive(Debug, Clone)]
pub struct HashMemoryEmbeddingProvider {
    dimensions: usize,
}

impl Default for HashMemoryEmbeddingProvider {
    fn default() -> Self {
        Self { dimensions: DEFAULT_MEMORY_VECTOR_DIMS }
    }
}

impl HashMemoryEmbeddingProvider {
    #[must_use]
    pub fn with_dimensions(dimensions: usize) -> Self {
        Self { dimensions: dimensions.max(1) }
    }
}

impl MemoryEmbeddingProvider for HashMemoryEmbeddingProvider {
    fn model_name(&self) -> &str {
        DEFAULT_MEMORY_EMBEDDING_MODEL
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn embed_text(&self, text: &str) -> Vec<f32> {
        hash_embed_text(text, self.dimensions)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CronScheduleType {
    Cron,
    Every,
    At,
}

impl CronScheduleType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cron => "cron",
            Self::Every => "every",
            Self::At => "at",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "cron" => Some(Self::Cron),
            "every" => Some(Self::Every),
            "at" => Some(Self::At),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CronConcurrencyPolicy {
    Forbid,
    Replace,
    QueueOne,
}

impl CronConcurrencyPolicy {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Forbid => "forbid",
            Self::Replace => "replace",
            Self::QueueOne => "queue(1)",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "forbid" => Some(Self::Forbid),
            "replace" => Some(Self::Replace),
            "queue(1)" => Some(Self::QueueOne),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CronMisfirePolicy {
    Skip,
    CatchUp,
}

impl CronMisfirePolicy {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::CatchUp => "catch_up",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "skip" => Some(Self::Skip),
            "catch_up" => Some(Self::CatchUp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CronRunStatus {
    Accepted,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Denied,
}

impl CronRunStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Denied => "denied",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "accepted" => Some(Self::Accepted),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            "denied" => Some(Self::Denied),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_active(self) -> bool {
        matches!(self, Self::Accepted | Self::Running)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CronRetryPolicy {
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronJobCreateRequest {
    pub job_id: String,
    pub name: String,
    pub prompt: String,
    pub owner_principal: String,
    pub channel: String,
    pub session_key: Option<String>,
    pub session_label: Option<String>,
    pub schedule_type: CronScheduleType,
    pub schedule_payload_json: String,
    pub enabled: bool,
    pub concurrency_policy: CronConcurrencyPolicy,
    pub retry_policy: CronRetryPolicy,
    pub misfire_policy: CronMisfirePolicy,
    pub jitter_ms: u64,
    pub next_run_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CronJobUpdatePatch {
    pub name: Option<String>,
    pub prompt: Option<String>,
    pub owner_principal: Option<String>,
    pub channel: Option<String>,
    pub session_key: Option<Option<String>>,
    pub session_label: Option<Option<String>>,
    pub schedule_type: Option<CronScheduleType>,
    pub schedule_payload_json: Option<String>,
    pub enabled: Option<bool>,
    pub concurrency_policy: Option<CronConcurrencyPolicy>,
    pub retry_policy: Option<CronRetryPolicy>,
    pub misfire_policy: Option<CronMisfirePolicy>,
    pub jitter_ms: Option<u64>,
    pub next_run_at_unix_ms: Option<Option<i64>>,
    pub queued_run: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CronJobRecord {
    pub job_id: String,
    pub name: String,
    pub prompt: String,
    pub owner_principal: String,
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_label: Option<String>,
    pub schedule_type: CronScheduleType,
    pub schedule_payload_json: String,
    pub enabled: bool,
    pub concurrency_policy: CronConcurrencyPolicy,
    pub retry_policy: CronRetryPolicy,
    pub misfire_policy: CronMisfirePolicy,
    pub jitter_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_run_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_at_unix_ms: Option<i64>,
    pub queued_run: bool,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronRunsListFilter<'a> {
    pub job_id: Option<&'a str>,
    pub after_run_id: Option<&'a str>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronJobsListFilter<'a> {
    pub after_job_id: Option<&'a str>,
    pub limit: usize,
    pub enabled: Option<bool>,
    pub owner_principal: Option<&'a str>,
    pub channel: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronRunStartRequest {
    pub run_id: String,
    pub job_id: String,
    pub attempt: u32,
    pub session_id: Option<String>,
    pub orchestrator_run_id: Option<String>,
    pub status: CronRunStatus,
    pub error_kind: Option<String>,
    pub error_message_redacted: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronRunFinalizeRequest {
    pub run_id: String,
    pub status: CronRunStatus,
    pub error_kind: Option<String>,
    pub error_message_redacted: Option<String>,
    pub model_tokens_in: u64,
    pub model_tokens_out: u64,
    pub tool_calls: u64,
    pub tool_denies: u64,
    pub orchestrator_run_id: Option<String>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CronRunRecord {
    pub run_id: String,
    pub job_id: String,
    pub attempt: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orchestrator_run_id: Option<String>,
    pub started_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<i64>,
    pub status: CronRunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message_redacted: Option<String>,
    pub model_tokens_in: u64,
    pub model_tokens_out: u64,
    pub tool_calls: u64,
    pub tool_denies: u64,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemorySource {
    TapeUserMessage,
    TapeToolResult,
    Summary,
    Manual,
    Import,
}

impl MemorySource {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TapeUserMessage => "tape:user_message",
            Self::TapeToolResult => "tape:tool_result",
            Self::Summary => "summary",
            Self::Manual => "manual",
            Self::Import => "import",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "tape:user_message" => Some(Self::TapeUserMessage),
            "tape:tool_result" => Some(Self::TapeToolResult),
            "summary" => Some(Self::Summary),
            "manual" => Some(Self::Manual),
            "import" => Some(Self::Import),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryItemCreateRequest {
    pub memory_id: String,
    pub principal: String,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub source: MemorySource,
    pub content_text: String,
    pub tags: Vec<String>,
    pub confidence: Option<f64>,
    pub ttl_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemorySearchRequest {
    pub principal: String,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub query: String,
    pub top_k: usize,
    pub min_score: f64,
    pub tags: Vec<String>,
    pub sources: Vec<MemorySource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryItemsListFilter {
    pub after_memory_id: Option<String>,
    pub principal: String,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub limit: usize,
    pub tags: Vec<String>,
    pub sources: Vec<MemorySource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPurgeRequest {
    pub principal: String,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub purge_all_principal: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MemoryItemRecord {
    pub memory_id: String,
    pub principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub source: MemorySource,
    pub content_text: String,
    pub content_hash: String,
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_unix_ms: Option<i64>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MemoryScoreBreakdown {
    pub lexical_score: f64,
    pub vector_score: f64,
    pub recency_score: f64,
    pub final_score: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MemorySearchHit {
    pub item: MemoryItemRecord,
    pub snippet: String,
    pub score: f64,
    pub breakdown: MemoryScoreBreakdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryRetentionPolicy {
    pub max_entries: Option<usize>,
    pub max_bytes: Option<u64>,
    pub ttl_days: Option<u32>,
}

impl MemoryRetentionPolicy {
    #[must_use]
    pub const fn is_enforced(self) -> bool {
        self.max_entries.is_some() || self.max_bytes.is_some() || self.ttl_days.is_some()
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MemoryUsageSnapshot {
    pub entries: u64,
    pub approx_bytes: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MemoryMaintenanceRunRecord {
    pub ran_at_unix_ms: i64,
    pub deleted_expired_count: u64,
    pub deleted_capacity_count: u64,
    pub deleted_total_count: u64,
    pub entries_before: u64,
    pub entries_after: u64,
    pub approx_bytes_before: u64,
    pub approx_bytes_after: u64,
    pub vacuum_performed: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MemoryMaintenanceStatus {
    pub usage: MemoryUsageSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run: Option<MemoryMaintenanceRunRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_vacuum_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_vacuum_due_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_maintenance_run_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryMaintenanceRequest {
    pub now_unix_ms: i64,
    pub retention: MemoryRetentionPolicy,
    pub next_vacuum_due_at_unix_ms: Option<i64>,
    pub next_maintenance_run_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MemoryMaintenanceOutcome {
    pub ran_at_unix_ms: i64,
    pub deleted_expired_count: u64,
    pub deleted_capacity_count: u64,
    pub deleted_total_count: u64,
    pub entries_before: u64,
    pub entries_after: u64,
    pub approx_bytes_before: u64,
    pub approx_bytes_after: u64,
    pub vacuum_performed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_vacuum_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_vacuum_due_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_maintenance_run_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct MemoryMaintenanceStateRow {
    last_run_at_unix_ms: Option<i64>,
    last_vacuum_at_unix_ms: Option<i64>,
    next_vacuum_due_at_unix_ms: Option<i64>,
    next_maintenance_run_at_unix_ms: Option<i64>,
    last_deleted_expired_count: u64,
    last_deleted_capacity_count: u64,
    last_deleted_total_count: u64,
    last_entries_before: u64,
    last_entries_after: u64,
    last_bytes_before: u64,
    last_bytes_after: u64,
    last_vacuum_performed: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalSubjectType {
    Tool,
    ChannelSend,
    SecretAccess,
    BrowserAction,
    NodeCapability,
}

impl ApprovalSubjectType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tool => "tool",
            Self::ChannelSend => "channel_send",
            Self::SecretAccess => "secret_access",
            Self::BrowserAction => "browser_action",
            Self::NodeCapability => "node_capability",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "tool" => Some(Self::Tool),
            "channel_send" => Some(Self::ChannelSend),
            "secret_access" => Some(Self::SecretAccess),
            "browser_action" => Some(Self::BrowserAction),
            "node_capability" => Some(Self::NodeCapability),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Allow,
    Deny,
    Timeout,
    Error,
}

impl ApprovalDecision {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "allow" => Some(Self::Allow),
            "deny" => Some(Self::Deny),
            "timeout" => Some(Self::Timeout),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionScope {
    Once,
    Session,
    Timeboxed,
}

impl ApprovalDecisionScope {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Once => "once",
            Self::Session => "session",
            Self::Timeboxed => "timeboxed",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "once" => Some(Self::Once),
            "session" => Some(Self::Session),
            "timeboxed" => Some(Self::Timeboxed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl ApprovalRiskLevel {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovalPolicySnapshot {
    pub policy_id: String,
    pub policy_hash: String,
    pub evaluation_summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovalPromptOption {
    pub option_id: String,
    pub label: String,
    pub description: String,
    pub default_selected: bool,
    pub decision_scope: ApprovalDecisionScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timebox_ttl_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovalPromptRecord {
    pub title: String,
    pub risk_level: ApprovalRiskLevel,
    pub subject_id: String,
    pub summary: String,
    pub options: Vec<ApprovalPromptOption>,
    pub timeout_seconds: u32,
    pub details_json: String,
    pub policy_explanation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalCreateRequest {
    pub approval_id: String,
    pub session_id: String,
    pub run_id: String,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub subject_type: ApprovalSubjectType,
    pub subject_id: String,
    pub request_summary: String,
    pub policy_snapshot: ApprovalPolicySnapshot,
    pub prompt: ApprovalPromptRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalResolveRequest {
    pub approval_id: String,
    pub decision: ApprovalDecision,
    pub decision_scope: ApprovalDecisionScope,
    pub decision_reason: String,
    pub decision_scope_ttl_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ApprovalRecord {
    pub approval_id: String,
    pub session_id: String,
    pub run_id: String,
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub requested_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_at_unix_ms: Option<i64>,
    pub subject_type: ApprovalSubjectType,
    pub subject_id: String,
    pub request_summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision: Option<ApprovalDecision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_scope: Option<ApprovalDecisionScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_scope_ttl_ms: Option<i64>,
    pub policy_snapshot: ApprovalPolicySnapshot,
    pub prompt: ApprovalPromptRecord,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalsListFilter<'a> {
    pub after_approval_id: Option<&'a str>,
    pub limit: usize,
    pub since_unix_ms: Option<i64>,
    pub until_unix_ms: Option<i64>,
    pub subject_id: Option<&'a str>,
    pub principal: Option<&'a str>,
    pub decision: Option<ApprovalDecision>,
    pub subject_type: Option<ApprovalSubjectType>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillExecutionStatus {
    Active,
    Quarantined,
    Disabled,
}

impl SkillExecutionStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Quarantined => "quarantined",
            Self::Disabled => "disabled",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "quarantined" => Some(Self::Quarantined),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillStatusUpsertRequest {
    pub skill_id: String,
    pub version: String,
    pub status: SkillExecutionStatus,
    pub reason: Option<String>,
    pub detected_at_ms: i64,
    pub operator_principal: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SkillStatusRecord {
    pub skill_id: String,
    pub version: String,
    pub status: SkillExecutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub detected_at_ms: i64,
    pub operator_principal: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanvasStateTransitionRequest {
    pub canvas_id: String,
    pub session_id: String,
    pub principal: String,
    pub state_version: u64,
    pub base_state_version: u64,
    pub state_schema_version: u64,
    pub state_json: String,
    pub patch_json: String,
    pub bundle_json: String,
    pub allowed_parent_origins_json: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
    pub closed: bool,
    pub close_reason: Option<String>,
    pub actor_principal: String,
    pub actor_device_id: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CanvasStateSnapshotRecord {
    pub canvas_id: String,
    pub session_id: String,
    pub principal: String,
    pub state_version: u64,
    pub state_schema_version: u64,
    pub state_json: String,
    pub bundle_json: String,
    pub allowed_parent_origins_json: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
    pub closed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CanvasStatePatchRecord {
    pub seq: i64,
    pub canvas_id: String,
    pub state_version: u64,
    pub base_state_version: u64,
    pub state_schema_version: u64,
    pub patch_json: String,
    pub resulting_state_json: String,
    pub closed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    pub actor_principal: String,
    pub actor_device_id: String,
    pub applied_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CanvasStateReplayRecord {
    pub canvas_id: String,
    pub state_version: u64,
    pub state_schema_version: u64,
    pub state_json: String,
    pub closed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    pub patches_applied: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalConfig {
    pub db_path: PathBuf,
    pub hash_chain_enabled: bool,
    pub max_payload_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalAppendRequest {
    pub event_id: String,
    pub session_id: String,
    pub run_id: String,
    pub kind: i32,
    pub actor: i32,
    pub timestamp_unix_ms: i64,
    pub payload_json: Vec<u8>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalAppendOutcome {
    pub redacted: bool,
    pub hash: Option<String>,
    pub prev_hash: Option<String>,
    pub write_duration: Duration,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JournalEventRecord {
    pub seq: i64,
    pub event_id: String,
    pub session_id: String,
    pub run_id: String,
    pub kind: i32,
    pub actor: i32,
    pub timestamp_unix_ms: i64,
    pub payload_json: String,
    pub redacted: bool,
    pub hash: Option<String>,
    pub prev_hash: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub created_at_unix_ms: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorSessionUpsertRequest {
    pub session_id: String,
    pub session_key: String,
    pub session_label: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorSessionResolveRequest {
    pub session_id: Option<String>,
    pub session_key: Option<String>,
    pub session_label: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub require_existing: bool,
    pub reset_session: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrchestratorSessionRecord {
    pub session_id: String,
    pub session_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_label: Option<String>,
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrchestratorSessionResolveOutcome {
    pub session: OrchestratorSessionRecord,
    pub created: bool,
    pub reset_applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorRunStartRequest {
    pub run_id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorUsageDelta {
    pub run_id: String,
    pub prompt_tokens_delta: u64,
    pub completion_tokens_delta: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorTapeAppendRequest {
    pub run_id: String,
    pub seq: i64,
    pub event_type: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorCancelRequest {
    pub run_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrchestratorTapeRecord {
    pub seq: i64,
    pub event_type: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrchestratorRunStatusSnapshot {
    pub run_id: String,
    pub session_id: String,
    pub state: String,
    pub cancel_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub created_at_unix_ms: i64,
    pub started_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub tape_events: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum JournalError {
    #[error("journal db path cannot be empty")]
    EmptyPath,
    #[error("journal db path cannot contain parent traversal ('..'): {path}")]
    ParentTraversalPath { path: String },
    #[error("failed to create journal directory at {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open journal sqlite database at {path}: {source}")]
    OpenConnection {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("journal lock poisoned")]
    LockPoisoned,
    #[error("journal event already exists: {event_id}")]
    DuplicateEventId { event_id: String },
    #[error("orchestrator run already exists: {run_id}")]
    DuplicateRunId { run_id: String },
    #[error("orchestrator tape sequence already exists for run {run_id} at seq {seq}")]
    DuplicateTapeSequence { run_id: String, seq: i64 },
    #[error("approval record already exists: {approval_id}")]
    DuplicateApprovalId { approval_id: String },
    #[error("cron job already exists: {job_id}")]
    DuplicateCronJobId { job_id: String },
    #[error("cron run already exists: {run_id}")]
    DuplicateCronRunId { run_id: String },
    #[error("memory item already exists: {memory_id}")]
    DuplicateMemoryId { memory_id: String },
    #[error("canvas state already exists for canvas {canvas_id} at version {state_version}")]
    DuplicateCanvasStateVersion { canvas_id: String, state_version: u64 },
    #[error("cron job not found: {job_id}")]
    CronJobNotFound { job_id: String },
    #[error("cron run not found: {run_id}")]
    CronRunNotFound { run_id: String },
    #[error("memory item not found: {memory_id}")]
    MemoryNotFound { memory_id: String },
    #[error("approval record not found: {approval_id}")]
    ApprovalNotFound { approval_id: String },
    #[error("canvas state not found: {canvas_id}")]
    CanvasStateNotFound { canvas_id: String },
    #[error("orchestrator run not found: {run_id}")]
    RunNotFound { run_id: String },
    #[error("orchestrator session identity mismatch for session: {session_id}")]
    SessionIdentityMismatch { session_id: String },
    #[error("orchestrator session not found for selector: {selector}")]
    SessionNotFound { selector: String },
    #[error("invalid orchestrator session selector: {reason}")]
    InvalidSessionSelector { reason: String },
    #[error("invalid canvas replay state for {canvas_id}: {reason}")]
    InvalidCanvasReplay { canvas_id: String, reason: String },
    #[error("{payload_kind} payload exceeds max bytes ({actual_bytes} > {max_bytes})")]
    PayloadTooLarge { payload_kind: &'static str, actual_bytes: usize, max_bytes: usize },
    #[error("journal max payload bytes must be greater than 0")]
    InvalidPayloadLimit,
    #[error("memory embedding vector dimensions must be greater than 0")]
    InvalidMemoryVectorDimensions,
    #[error("journal sqlite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("failed to serialize journal payload: {0}")]
    SerializePayload(#[from] serde_json::Error),
    #[error("system time before unix epoch: {0}")]
    InvalidSystemTime(#[from] std::time::SystemTimeError),
}

struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "create_event_journal",
        sql: r#"
            CREATE TABLE IF NOT EXISTS journal_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_ulid TEXT NOT NULL UNIQUE,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                kind INTEGER NOT NULL,
                actor INTEGER NOT NULL,
                timestamp_unix_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                redacted INTEGER NOT NULL,
                hash TEXT,
                prev_hash TEXT,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_journal_events_run_ts
                ON journal_events(run_ulid, timestamp_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_journal_events_created_at
                ON journal_events(created_at_unix_ms);
            CREATE TRIGGER IF NOT EXISTS trg_journal_events_prevent_update
            BEFORE UPDATE ON journal_events
            BEGIN
                SELECT RAISE(ABORT, 'journal_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_journal_events_prevent_delete
            BEFORE DELETE ON journal_events
            BEGIN
                SELECT RAISE(ABORT, 'journal_events is append-only');
            END;
        "#,
    },
    Migration {
        version: 2,
        name: "create_orchestrator_tables",
        sql: r#"
            CREATE TABLE IF NOT EXISTS orchestrator_sessions (
                session_ulid TEXT PRIMARY KEY,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orchestrator_runs (
                run_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                state TEXT NOT NULL,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                cancel_reason TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                started_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER,
                updated_at_unix_ms INTEGER NOT NULL,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_session
                ON orchestrator_runs(session_ulid);

            CREATE TABLE IF NOT EXISTS orchestrator_tape (
                run_ulid TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY(run_ulid, seq),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_tape_run_seq
                ON orchestrator_tape(run_ulid, seq);
            CREATE TRIGGER IF NOT EXISTS trg_orchestrator_tape_prevent_update
            BEFORE UPDATE ON orchestrator_tape
            BEGIN
                SELECT RAISE(ABORT, 'orchestrator_tape is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_orchestrator_tape_prevent_delete
            BEFORE DELETE ON orchestrator_tape
            BEGIN
                SELECT RAISE(ABORT, 'orchestrator_tape is append-only');
            END;
        "#,
    },
    Migration {
        version: 3,
        name: "orchestrator_session_keys_and_labels",
        sql: r#"
            ALTER TABLE orchestrator_sessions
                ADD COLUMN session_key TEXT;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN session_label TEXT;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN last_run_ulid TEXT;

            UPDATE orchestrator_sessions
            SET session_key = session_ulid
            WHERE session_key IS NULL OR TRIM(session_key) = '';

            CREATE UNIQUE INDEX IF NOT EXISTS idx_orchestrator_sessions_session_key
                ON orchestrator_sessions(session_key);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_sessions_session_label
                ON orchestrator_sessions(session_label);
        "#,
    },
    Migration {
        version: 4,
        name: "create_cron_jobs_and_runs",
        sql: r#"
            CREATE TABLE IF NOT EXISTS cron_jobs (
                job_ulid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                prompt TEXT NOT NULL,
                owner_principal TEXT NOT NULL,
                channel TEXT NOT NULL,
                session_key TEXT,
                session_label TEXT,
                schedule_type TEXT NOT NULL,
                schedule_payload_json TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                concurrency_policy TEXT NOT NULL,
                retry_policy_json TEXT NOT NULL,
                misfire_policy TEXT NOT NULL,
                jitter_ms INTEGER NOT NULL DEFAULT 0,
                next_run_at_unix_ms INTEGER,
                last_run_at_unix_ms INTEGER,
                queued_run INTEGER NOT NULL DEFAULT 0,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_cron_jobs_enabled_next_run
                ON cron_jobs(enabled, next_run_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_cron_jobs_owner
                ON cron_jobs(owner_principal);
            CREATE INDEX IF NOT EXISTS idx_cron_jobs_channel
                ON cron_jobs(channel);

            CREATE TABLE IF NOT EXISTS cron_runs (
                run_ulid TEXT PRIMARY KEY,
                job_ulid TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                session_ulid TEXT,
                orchestrator_run_ulid TEXT,
                started_at_unix_ms INTEGER NOT NULL,
                finished_at_unix_ms INTEGER,
                status TEXT NOT NULL,
                error_kind TEXT,
                error_message_redacted TEXT,
                model_tokens_in INTEGER NOT NULL DEFAULT 0,
                model_tokens_out INTEGER NOT NULL DEFAULT 0,
                tool_calls INTEGER NOT NULL DEFAULT 0,
                tool_denies INTEGER NOT NULL DEFAULT 0,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(job_ulid) REFERENCES cron_jobs(job_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_cron_runs_job_started
                ON cron_runs(job_ulid, started_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_cron_runs_job_status
                ON cron_runs(job_ulid, status);
            CREATE INDEX IF NOT EXISTS idx_cron_runs_started
                ON cron_runs(started_at_unix_ms DESC);
        "#,
    },
    Migration {
        version: 5,
        name: "create_approvals_table",
        sql: r#"
            CREATE TABLE IF NOT EXISTS approvals (
                approval_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                requested_at_unix_ms INTEGER NOT NULL,
                resolved_at_unix_ms INTEGER,
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                request_summary TEXT NOT NULL,
                decision TEXT,
                decision_scope TEXT,
                decision_reason TEXT,
                decision_scope_ttl_ms INTEGER,
                policy_snapshot_json TEXT NOT NULL,
                prompt_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_approvals_run
                ON approvals(run_ulid, requested_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_approvals_session
                ON approvals(session_ulid, requested_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_approvals_principal
                ON approvals(principal);
            CREATE INDEX IF NOT EXISTS idx_approvals_subject_id
                ON approvals(subject_id);
            CREATE INDEX IF NOT EXISTS idx_approvals_resolved
                ON approvals(resolved_at_unix_ms DESC, approval_ulid ASC);
        "#,
    },
    Migration {
        version: 6,
        name: "create_memory_tables",
        sql: r#"
            CREATE TABLE IF NOT EXISTS memory_items (
                memory_ulid TEXT PRIMARY KEY,
                principal TEXT NOT NULL,
                channel TEXT,
                session_ulid TEXT,
                source TEXT NOT NULL,
                content_text TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                confidence REAL,
                ttl_unix_ms INTEGER,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_memory_items_scope
                ON memory_items(principal, channel, session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_memory_items_ttl
                ON memory_items(ttl_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_memory_items_source
                ON memory_items(source);

            CREATE VIRTUAL TABLE IF NOT EXISTS memory_items_fts
                USING fts5(memory_ulid UNINDEXED, content_text, tokenize='unicode61');
            CREATE TRIGGER IF NOT EXISTS trg_memory_items_ai
            AFTER INSERT ON memory_items
            BEGIN
                INSERT INTO memory_items_fts(memory_ulid, content_text)
                VALUES (new.memory_ulid, new.content_text);
            END;
            CREATE TRIGGER IF NOT EXISTS trg_memory_items_ad
            AFTER DELETE ON memory_items
            BEGIN
                DELETE FROM memory_items_fts WHERE memory_ulid = old.memory_ulid;
            END;

            CREATE TABLE IF NOT EXISTS memory_vectors (
                memory_ulid TEXT PRIMARY KEY,
                embedding_model TEXT NOT NULL,
                dims INTEGER NOT NULL,
                vector_blob BLOB NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(memory_ulid) REFERENCES memory_items(memory_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_memory_vectors_model
                ON memory_vectors(embedding_model);
        "#,
    },
    Migration {
        version: 7,
        name: "create_skill_status_table",
        sql: r#"
            CREATE TABLE IF NOT EXISTS skill_status (
                skill_id TEXT NOT NULL,
                version TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT,
                detected_at_ms INTEGER NOT NULL,
                operator_principal TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY(skill_id, version)
            );
            CREATE INDEX IF NOT EXISTS idx_skill_status_skill_detected
                ON skill_status(skill_id, detected_at_ms DESC, version DESC);
            CREATE INDEX IF NOT EXISTS idx_skill_status_state
                ON skill_status(status, detected_at_ms DESC);
        "#,
    },
    Migration {
        version: 8,
        name: "create_canvas_state_tables",
        sql: r#"
            CREATE TABLE IF NOT EXISTS canvas_state_snapshots (
                canvas_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                principal TEXT NOT NULL,
                state_version INTEGER NOT NULL,
                state_schema_version INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                bundle_json TEXT NOT NULL,
                allowed_parent_origins_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                expires_at_unix_ms INTEGER NOT NULL,
                closed INTEGER NOT NULL,
                close_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_canvas_state_snapshots_scope
                ON canvas_state_snapshots(principal, session_ulid, updated_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS canvas_state_patches (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                canvas_ulid TEXT NOT NULL,
                state_version INTEGER NOT NULL,
                base_state_version INTEGER NOT NULL,
                state_schema_version INTEGER NOT NULL,
                patch_json TEXT NOT NULL,
                resulting_state_json TEXT NOT NULL,
                closed INTEGER NOT NULL,
                close_reason TEXT,
                actor_principal TEXT NOT NULL,
                actor_device_id TEXT NOT NULL,
                applied_at_unix_ms INTEGER NOT NULL,
                UNIQUE(canvas_ulid, state_version)
            );
            CREATE INDEX IF NOT EXISTS idx_canvas_state_patches_canvas_version
                ON canvas_state_patches(canvas_ulid, state_version ASC);
            CREATE TRIGGER IF NOT EXISTS trg_canvas_state_patches_prevent_update
            BEFORE UPDATE ON canvas_state_patches
            BEGIN
                SELECT RAISE(ABORT, 'canvas_state_patches is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_canvas_state_patches_prevent_delete
            BEFORE DELETE ON canvas_state_patches
            BEGIN
                SELECT RAISE(ABORT, 'canvas_state_patches is append-only');
            END;
        "#,
    },
    Migration {
        version: 9,
        name: "create_memory_maintenance_state",
        sql: r#"
            CREATE TABLE IF NOT EXISTS memory_maintenance_state (
                singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
                last_run_at_unix_ms INTEGER,
                last_vacuum_at_unix_ms INTEGER,
                next_vacuum_due_at_unix_ms INTEGER,
                next_maintenance_run_at_unix_ms INTEGER,
                last_deleted_expired_count INTEGER NOT NULL DEFAULT 0,
                last_deleted_capacity_count INTEGER NOT NULL DEFAULT 0,
                last_deleted_total_count INTEGER NOT NULL DEFAULT 0,
                last_entries_before INTEGER NOT NULL DEFAULT 0,
                last_entries_after INTEGER NOT NULL DEFAULT 0,
                last_bytes_before INTEGER NOT NULL DEFAULT 0,
                last_bytes_after INTEGER NOT NULL DEFAULT 0,
                last_vacuum_performed INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO memory_maintenance_state(singleton_key)
            VALUES (1);
        "#,
    },
];

pub struct JournalStore {
    config: JournalConfig,
    connection: Mutex<Connection>,
    memory_embedding_provider: Arc<dyn MemoryEmbeddingProvider>,
}

impl fmt::Debug for JournalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JournalStore")
            .field("db_path", &self.config.db_path)
            .field("hash_chain_enabled", &self.config.hash_chain_enabled)
            .field("max_payload_bytes", &self.config.max_payload_bytes)
            .finish()
    }
}

impl JournalStore {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open(config: JournalConfig) -> Result<Self, JournalError> {
        Self::open_with_memory_embedding_provider(
            config,
            Arc::new(HashMemoryEmbeddingProvider::default()),
        )
    }

    pub fn open_with_memory_embedding_provider(
        config: JournalConfig,
        memory_embedding_provider: Arc<dyn MemoryEmbeddingProvider>,
    ) -> Result<Self, JournalError> {
        if config.max_payload_bytes == 0 {
            return Err(JournalError::InvalidPayloadLimit);
        }
        if memory_embedding_provider.dimensions() == 0 {
            return Err(JournalError::InvalidMemoryVectorDimensions);
        }
        validate_db_path(&config.db_path)?;
        if let Some(parent) = config.db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|source| JournalError::CreateDirectory {
                    path: parent.to_path_buf(),
                    source,
                })?;
            }
        }

        let mut connection = Connection::open(&config.db_path).map_err(|source| {
            JournalError::OpenConnection { path: config.db_path.clone(), source }
        })?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )?;

        apply_migrations(&mut connection)?;
        Ok(Self { config, connection: Mutex::new(connection), memory_embedding_provider })
    }

    pub fn append(
        &self,
        request: &JournalAppendRequest,
    ) -> Result<JournalAppendOutcome, JournalError> {
        if request.payload_json.len() > self.config.max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "journal",
                actual_bytes: request.payload_json.len(),
                max_bytes: self.config.max_payload_bytes,
            });
        }
        let started_at = Instant::now();
        let (payload_json, redacted) = sanitize_payload(&request.payload_json)?;
        let created_at_unix_ms = current_unix_ms()?;

        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;

        let prev_hash = if self.config.hash_chain_enabled {
            transaction
                .query_row("SELECT hash FROM journal_events ORDER BY seq DESC LIMIT 1", [], |row| {
                    row.get::<_, Option<String>>(0)
                })
                .optional()?
                .flatten()
        } else {
            None
        };

        let hash = if self.config.hash_chain_enabled {
            Some(compute_hash(prev_hash.as_deref(), request, &payload_json))
        } else {
            None
        };

        match transaction.execute(
            r#"
                INSERT INTO journal_events (
                    event_ulid,
                    session_ulid,
                    run_ulid,
                    kind,
                    actor,
                    timestamp_unix_ms,
                    payload_json,
                    redacted,
                    hash,
                    prev_hash,
                    principal,
                    device_id,
                    channel,
                    created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
            params![
                request.event_id,
                request.session_id,
                request.run_id,
                request.kind,
                request.actor,
                request.timestamp_unix_ms,
                payload_json,
                redacted as i64,
                hash,
                prev_hash,
                request.principal,
                request.device_id,
                request.channel,
                created_at_unix_ms,
            ],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("journal_events.event_ulid"))
                            .unwrap_or(false)) =>
            {
                return Err(JournalError::DuplicateEventId { event_id: request.event_id.clone() });
            }
            Err(error) => return Err(error.into()),
        }
        transaction.commit()?;

        Ok(JournalAppendOutcome { redacted, hash, prev_hash, write_duration: started_at.elapsed() })
    }

    pub fn total_events(&self) -> Result<usize, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let total_events: i64 =
            guard.query_row("SELECT COUNT(*) FROM journal_events", [], |row| row.get(0))?;
        Ok(total_events as usize)
    }

    pub fn latest_hash(&self) -> Result<Option<String>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard
            .query_row("SELECT hash FROM journal_events ORDER BY seq DESC LIMIT 1", [], |row| {
                row.get::<_, Option<String>>(0)
            })
            .optional()
            .map(|row| row.flatten())
            .map_err(JournalError::from)
    }

    pub fn recent(&self, requested_limit: usize) -> Result<Vec<JournalEventRecord>, JournalError> {
        let limit = requested_limit.clamp(1, MAX_RECENT_EVENTS_LIMIT) as i64;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    seq,
                    event_ulid,
                    session_ulid,
                    run_ulid,
                    kind,
                    actor,
                    timestamp_unix_ms,
                    payload_json,
                    redacted,
                    hash,
                    prev_hash,
                    principal,
                    device_id,
                    channel,
                    created_at_unix_ms
                FROM journal_events
                ORDER BY seq DESC
                LIMIT ?1
            "#,
        )?;
        let mut rows = statement.query(params![limit])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            events.push(JournalEventRecord {
                seq: row.get(0)?,
                event_id: row.get(1)?,
                session_id: row.get(2)?,
                run_id: row.get(3)?,
                kind: row.get(4)?,
                actor: row.get(5)?,
                timestamp_unix_ms: row.get(6)?,
                payload_json: row.get(7)?,
                redacted: row.get::<_, i64>(8)? == 1,
                hash: row.get(9)?,
                prev_hash: row.get(10)?,
                principal: row.get(11)?,
                device_id: row.get(12)?,
                channel: row.get(13)?,
                created_at_unix_ms: row.get(14)?,
            });
        }
        Ok(events)
    }

    #[cfg(test)]
    pub fn upsert_orchestrator_session(
        &self,
        request: &OrchestratorSessionUpsertRequest,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated =
            guard.execute(
                r#"
                INSERT INTO orchestrator_sessions (
                    session_ulid,
                    session_key,
                    session_label,
                    principal,
                    device_id,
                    channel,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
                ON CONFLICT(session_ulid) DO UPDATE SET
                    updated_at_unix_ms = excluded.updated_at_unix_ms,
                    session_label = COALESCE(excluded.session_label, orchestrator_sessions.session_label)
                WHERE orchestrator_sessions.principal = excluded.principal
                  AND orchestrator_sessions.device_id = excluded.device_id
                  AND COALESCE(orchestrator_sessions.channel, '') = COALESCE(excluded.channel, '')
                  AND orchestrator_sessions.session_key = excluded.session_key
            "#,
                params![
                    request.session_id,
                    request.session_key,
                    request.session_label,
                    request.principal,
                    request.device_id,
                    request.channel,
                    now,
                ],
            )?;
        if updated == 0 {
            return Err(JournalError::SessionIdentityMismatch {
                session_id: request.session_id.clone(),
            });
        }
        Ok(())
    }

    pub fn resolve_orchestrator_session(
        &self,
        request: &OrchestratorSessionResolveRequest,
    ) -> Result<OrchestratorSessionResolveOutcome, JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;

        let requested_session_id =
            request.session_id.clone().and_then(normalize_optional_session_field);
        let requested_session_key =
            request.session_key.clone().and_then(normalize_optional_session_field);
        let requested_session_label =
            request.session_label.clone().and_then(normalize_optional_session_field);

        let existing_by_id = if let Some(session_id) = requested_session_id.as_deref() {
            load_orchestrator_session_by_id(&guard, session_id)?
        } else {
            None
        };
        let existing_by_key = if let Some(session_key) = requested_session_key.as_deref() {
            load_orchestrator_session_by_key(&guard, session_key)?
        } else {
            None
        };

        let mut existing = match (existing_by_id, existing_by_key) {
            (Some(by_id), Some(by_key)) => {
                if by_id.session_id != by_key.session_id {
                    return Err(JournalError::InvalidSessionSelector {
                        reason:
                            "session_id and session_key selectors resolve to different sessions"
                                .to_owned(),
                    });
                }
                Some(by_id)
            }
            (Some(by_id), None) => {
                if let Some(session_key) = requested_session_key.as_deref() {
                    if by_id.session_key != session_key {
                        return Err(JournalError::InvalidSessionSelector {
                            reason: "provided session_id does not match existing session_key"
                                .to_owned(),
                        });
                    }
                }
                Some(by_id)
            }
            (None, Some(by_key)) => {
                if let Some(session_id) = requested_session_id.as_deref() {
                    if by_key.session_id != session_id {
                        return Err(JournalError::InvalidSessionSelector {
                            reason: "provided session_key does not match existing session_id"
                                .to_owned(),
                        });
                    }
                }
                Some(by_key)
            }
            (None, None) => {
                if requested_session_id.is_none() && requested_session_key.is_none() {
                    if let Some(session_label) = requested_session_label.as_deref() {
                        load_orchestrator_session_by_label(&guard, session_label)?
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };

        if let Some(mut session) = existing.take() {
            if session.principal != request.principal
                || session.device_id != request.device_id
                || session.channel != request.channel
            {
                return Err(JournalError::SessionIdentityMismatch {
                    session_id: session.session_id,
                });
            }

            guard.execute(
                r#"
                    UPDATE orchestrator_sessions
                    SET
                        updated_at_unix_ms = ?2,
                        session_label = COALESCE(?3, session_label),
                        last_run_ulid = CASE WHEN ?4 = 1 THEN NULL ELSE last_run_ulid END
                    WHERE session_ulid = ?1
                "#,
                params![
                    session.session_id,
                    now,
                    requested_session_label,
                    if request.reset_session { 1_i64 } else { 0_i64 },
                ],
            )?;

            session.updated_at_unix_ms = now;
            if requested_session_label.is_some() {
                session.session_label = requested_session_label.clone();
            }
            if request.reset_session {
                session.last_run_id = None;
            }
            return Ok(OrchestratorSessionResolveOutcome {
                session,
                created: false,
                reset_applied: request.reset_session,
            });
        }

        if request.require_existing {
            let selector = requested_session_id
                .clone()
                .or(requested_session_key.clone())
                .or(requested_session_label.clone())
                .unwrap_or_else(|| "<unspecified>".to_owned());
            return Err(JournalError::SessionNotFound { selector });
        }

        let session_id = requested_session_id.unwrap_or_else(|| ulid::Ulid::new().to_string());
        let session_key = requested_session_key.unwrap_or_else(|| session_id.clone());
        let session_label = requested_session_label;

        guard.execute(
            r#"
                INSERT INTO orchestrator_sessions (
                    session_ulid,
                    session_key,
                    session_label,
                    principal,
                    device_id,
                    channel,
                    created_at_unix_ms,
                    updated_at_unix_ms,
                    last_run_ulid
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, NULL)
            "#,
            params![
                session_id,
                session_key,
                session_label,
                request.principal,
                request.device_id,
                request.channel,
                now,
            ],
        )?;

        Ok(OrchestratorSessionResolveOutcome {
            session: OrchestratorSessionRecord {
                session_id: session_id.clone(),
                session_key,
                session_label,
                principal: request.principal.clone(),
                device_id: request.device_id.clone(),
                channel: request.channel.clone(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
                last_run_id: None,
            },
            created: true,
            reset_applied: false,
        })
    }

    pub fn list_orchestrator_sessions(
        &self,
        after_session_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OrchestratorSessionRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = limit.max(1);
        load_orchestrator_sessions_page(&guard, after_session_key, limit)
    }

    pub fn start_orchestrator_run(
        &self,
        request: &OrchestratorRunStartRequest,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        match guard.execute(
            r#"
                INSERT INTO orchestrator_runs (
                    run_ulid,
                    session_ulid,
                    state,
                    cancel_requested,
                    cancel_reason,
                    created_at_unix_ms,
                    started_at_unix_ms,
                    completed_at_unix_ms,
                    updated_at_unix_ms,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    last_error
                ) VALUES (?1, ?2, ?3, 0, NULL, ?4, ?4, NULL, ?4, 0, 0, 0, NULL)
            "#,
            params![request.run_id, request.session_id, RunLifecycleState::Accepted.as_str(), now,],
        ) {
            Ok(_) => {
                guard.execute(
                    r#"
                        UPDATE orchestrator_sessions
                        SET
                            updated_at_unix_ms = ?2,
                            last_run_ulid = ?3
                        WHERE session_ulid = ?1
                    "#,
                    params![request.session_id, now, request.run_id],
                )?;
                Ok(())
            }
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("orchestrator_runs.run_ulid"))
                            .unwrap_or(false)) =>
            {
                Err(JournalError::DuplicateRunId { run_id: request.run_id.clone() })
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn update_orchestrator_run_state(
        &self,
        run_id: &str,
        state: RunLifecycleState,
        error_message: Option<&str>,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let completed_at = if state.is_terminal() { Some(now) } else { None };
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE orchestrator_runs
                SET
                    state = ?2,
                    completed_at_unix_ms = COALESCE(?3, completed_at_unix_ms),
                    updated_at_unix_ms = ?4,
                    last_error = COALESCE(?5, last_error)
                WHERE run_ulid = ?1
            "#,
            params![run_id, state.as_str(), completed_at, now, error_message],
        )?;
        if updated == 0 {
            return Err(JournalError::RunNotFound { run_id: run_id.to_owned() });
        }
        Ok(())
    }

    pub fn add_orchestrator_usage(
        &self,
        delta: &OrchestratorUsageDelta,
    ) -> Result<(), JournalError> {
        if delta.prompt_tokens_delta == 0 && delta.completion_tokens_delta == 0 {
            return Ok(());
        }
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE orchestrator_runs
                SET
                    prompt_tokens = prompt_tokens + ?2,
                    completion_tokens = completion_tokens + ?3,
                    total_tokens = total_tokens + (?2 + ?3),
                    updated_at_unix_ms = ?4
                WHERE run_ulid = ?1
            "#,
            params![
                delta.run_id,
                delta.prompt_tokens_delta as i64,
                delta.completion_tokens_delta as i64,
                now,
            ],
        )?;
        if updated == 0 {
            return Err(JournalError::RunNotFound { run_id: delta.run_id.clone() });
        }
        Ok(())
    }

    pub fn append_orchestrator_tape_event(
        &self,
        request: &OrchestratorTapeAppendRequest,
    ) -> Result<(), JournalError> {
        if request.payload_json.len() > self.config.max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "orchestrator_tape",
                actual_bytes: request.payload_json.len(),
                max_bytes: self.config.max_payload_bytes,
            });
        }
        let (payload_json, _) = sanitize_payload(request.payload_json.as_bytes())?;
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        match guard.execute(
            r#"
                INSERT INTO orchestrator_tape (
                    run_ulid,
                    seq,
                    event_type,
                    payload_json,
                    created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![request.run_id, request.seq, request.event_type, payload_json, now,],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("orchestrator_tape"))
                            .unwrap_or(false)) =>
            {
                Err(JournalError::DuplicateTapeSequence {
                    run_id: request.run_id.clone(),
                    seq: request.seq,
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn request_orchestrator_cancel(
        &self,
        request: &OrchestratorCancelRequest,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE orchestrator_runs
                SET
                    cancel_requested = 1,
                    cancel_reason = ?2,
                    updated_at_unix_ms = ?3
                WHERE run_ulid = ?1
            "#,
            params![request.run_id, request.reason, now],
        )?;
        if updated == 0 {
            return Err(JournalError::RunNotFound { run_id: request.run_id.clone() });
        }
        Ok(())
    }

    pub fn is_orchestrator_cancel_requested(&self, run_id: &str) -> Result<bool, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let value = guard
            .query_row(
                "SELECT cancel_requested FROM orchestrator_runs WHERE run_ulid = ?1",
                params![run_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let Some(value) = value else {
            return Err(JournalError::RunNotFound { run_id: run_id.to_owned() });
        };
        Ok(value == 1)
    }

    #[cfg(test)]
    pub fn orchestrator_tape(
        &self,
        run_id: &str,
    ) -> Result<Vec<OrchestratorTapeRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_orchestrator_tape(&guard, run_id)
    }

    pub fn orchestrator_tape_page(
        &self,
        run_id: &str,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<OrchestratorTapeRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = limit.max(1);
        load_orchestrator_tape_page(&guard, run_id, after_seq, limit)
    }

    pub fn orchestrator_run_status_snapshot(
        &self,
        run_id: &str,
    ) -> Result<Option<OrchestratorRunStatusSnapshot>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    runs.run_ulid,
                    runs.session_ulid,
                    runs.state,
                    runs.cancel_requested,
                    runs.cancel_reason,
                    sessions.principal,
                    sessions.device_id,
                    sessions.channel,
                    runs.prompt_tokens,
                    runs.completion_tokens,
                    runs.total_tokens,
                    runs.created_at_unix_ms,
                    runs.started_at_unix_ms,
                    runs.completed_at_unix_ms,
                    runs.updated_at_unix_ms,
                    runs.last_error
                FROM orchestrator_runs AS runs
                INNER JOIN orchestrator_sessions AS sessions
                    ON sessions.session_ulid = runs.session_ulid
                WHERE runs.run_ulid = ?1
            "#,
        )?;
        let row = statement
            .query_row(params![run_id], |row| {
                let raw_state: String = row.get(2)?;
                let normalized_state = RunLifecycleState::from_str(raw_state.as_str())
                    .map(|state| state.as_str().to_owned())
                    .unwrap_or(raw_state);
                Ok(OrchestratorRunStatusSnapshot {
                    run_id: row.get(0)?,
                    session_id: row.get(1)?,
                    state: normalized_state,
                    cancel_requested: row.get::<_, i64>(3)? == 1,
                    cancel_reason: row.get(4)?,
                    principal: row.get(5)?,
                    device_id: row.get(6)?,
                    channel: row.get(7)?,
                    prompt_tokens: row.get::<_, i64>(8)? as u64,
                    completion_tokens: row.get::<_, i64>(9)? as u64,
                    total_tokens: row.get::<_, i64>(10)? as u64,
                    created_at_unix_ms: row.get(11)?,
                    started_at_unix_ms: row.get(12)?,
                    completed_at_unix_ms: row.get(13)?,
                    updated_at_unix_ms: row.get(14)?,
                    last_error: row.get(15)?,
                    tape_events: 0,
                })
            })
            .optional()?;
        let Some(mut snapshot) = row else {
            return Ok(None);
        };
        snapshot.tape_events = guard.query_row(
            "SELECT COUNT(*) FROM orchestrator_tape WHERE run_ulid = ?1",
            params![run_id],
            |row| row.get::<_, i64>(0),
        )? as u64;
        Ok(Some(snapshot))
    }

    pub fn create_cron_job(
        &self,
        request: &CronJobCreateRequest,
    ) -> Result<CronJobRecord, JournalError> {
        let now = current_unix_ms()?;
        let retry_policy_json = serde_json::to_string(&request.retry_policy)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        match guard.execute(
            r#"
                INSERT INTO cron_jobs (
                    job_ulid,
                    name,
                    prompt,
                    owner_principal,
                    channel,
                    session_key,
                    session_label,
                    schedule_type,
                    schedule_payload_json,
                    enabled,
                    concurrency_policy,
                    retry_policy_json,
                    misfire_policy,
                    jitter_ms,
                    next_run_at_unix_ms,
                    last_run_at_unix_ms,
                    queued_run,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, NULL, 0, ?16, ?16
                )
            "#,
            params![
                request.job_id,
                request.name,
                request.prompt,
                request.owner_principal,
                request.channel,
                request.session_key,
                request.session_label,
                request.schedule_type.as_str(),
                request.schedule_payload_json,
                request.enabled as i64,
                request.concurrency_policy.as_str(),
                retry_policy_json,
                request.misfire_policy.as_str(),
                request.jitter_ms as i64,
                request.next_run_at_unix_ms,
                now,
            ],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("cron_jobs.job_ulid"))
                            .unwrap_or(false)) =>
            {
                return Err(JournalError::DuplicateCronJobId {
                    job_id: request.job_id.clone(),
                });
            }
            Err(error) => return Err(error.into()),
        }
        load_cron_job_by_id(&guard, request.job_id.as_str())?
            .ok_or_else(|| JournalError::CronJobNotFound { job_id: request.job_id.clone() })
    }

    pub fn update_cron_job(
        &self,
        job_id: &str,
        patch: &CronJobUpdatePatch,
    ) -> Result<CronJobRecord, JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let Some(existing) = load_cron_job_by_id(&guard, job_id)? else {
            return Err(JournalError::CronJobNotFound { job_id: job_id.to_owned() });
        };

        let name = patch.name.clone().unwrap_or(existing.name);
        let prompt = patch.prompt.clone().unwrap_or(existing.prompt);
        let owner_principal = patch.owner_principal.clone().unwrap_or(existing.owner_principal);
        let channel = patch.channel.clone().unwrap_or(existing.channel);
        let session_key = patch.session_key.clone().unwrap_or(existing.session_key);
        let session_label = patch.session_label.clone().unwrap_or(existing.session_label);
        let schedule_type = patch.schedule_type.unwrap_or(existing.schedule_type);
        let schedule_payload_json =
            patch.schedule_payload_json.clone().unwrap_or(existing.schedule_payload_json);
        let enabled = patch.enabled.unwrap_or(existing.enabled);
        let concurrency_policy = patch.concurrency_policy.unwrap_or(existing.concurrency_policy);
        let retry_policy = patch.retry_policy.clone().unwrap_or(existing.retry_policy);
        let misfire_policy = patch.misfire_policy.unwrap_or(existing.misfire_policy);
        let jitter_ms = patch.jitter_ms.unwrap_or(existing.jitter_ms);
        let next_run_at_unix_ms = patch.next_run_at_unix_ms.unwrap_or(existing.next_run_at_unix_ms);
        let queued_run = patch.queued_run.unwrap_or(existing.queued_run);

        guard.execute(
            r#"
                UPDATE cron_jobs
                SET
                    name = ?2,
                    prompt = ?3,
                    owner_principal = ?4,
                    channel = ?5,
                    session_key = ?6,
                    session_label = ?7,
                    schedule_type = ?8,
                    schedule_payload_json = ?9,
                    enabled = ?10,
                    concurrency_policy = ?11,
                    retry_policy_json = ?12,
                    misfire_policy = ?13,
                    jitter_ms = ?14,
                    next_run_at_unix_ms = ?15,
                    queued_run = ?16,
                    updated_at_unix_ms = ?17
                WHERE job_ulid = ?1
            "#,
            params![
                job_id,
                name,
                prompt,
                owner_principal,
                channel,
                session_key,
                session_label,
                schedule_type.as_str(),
                schedule_payload_json,
                enabled as i64,
                concurrency_policy.as_str(),
                serde_json::to_string(&retry_policy)?,
                misfire_policy.as_str(),
                jitter_ms as i64,
                next_run_at_unix_ms,
                queued_run as i64,
                now,
            ],
        )?;
        load_cron_job_by_id(&guard, job_id)?
            .ok_or_else(|| JournalError::CronJobNotFound { job_id: job_id.to_owned() })
    }

    pub fn delete_cron_job(&self, job_id: &str) -> Result<bool, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let deleted =
            guard.execute("DELETE FROM cron_jobs WHERE job_ulid = ?1", params![job_id])?;
        Ok(deleted > 0)
    }

    pub fn cron_job(&self, job_id: &str) -> Result<Option<CronJobRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_cron_job_by_id(&guard, job_id)
    }

    pub fn list_cron_jobs(
        &self,
        filter: CronJobsListFilter<'_>,
    ) -> Result<Vec<CronJobRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = filter.limit.clamp(1, MAX_CRON_JOBS_LIST_LIMIT);
        let mut statement = guard.prepare(
            r#"
                SELECT
                    job_ulid,
                    name,
                    prompt,
                    owner_principal,
                    channel,
                    session_key,
                    session_label,
                    schedule_type,
                    schedule_payload_json,
                    enabled,
                    concurrency_policy,
                    retry_policy_json,
                    misfire_policy,
                    jitter_ms,
                    next_run_at_unix_ms,
                    last_run_at_unix_ms,
                    queued_run,
                    created_at_unix_ms,
                    updated_at_unix_ms
                FROM cron_jobs
                WHERE
                    (?1 IS NULL OR job_ulid > ?1) AND
                    (?2 IS NULL OR enabled = ?2) AND
                    (?3 IS NULL OR owner_principal = ?3) AND
                    (?4 IS NULL OR channel = ?4)
                ORDER BY job_ulid ASC
                LIMIT ?5
            "#,
        )?;
        let enabled = filter.enabled.map(|value| if value { 1_i64 } else { 0_i64 });
        let mut rows = statement.query(params![
            filter.after_job_id,
            enabled,
            filter.owner_principal,
            filter.channel,
            limit as i64
        ])?;
        let mut jobs = Vec::new();
        while let Some(row) = rows.next()? {
            jobs.push(map_cron_job_row(row)?);
        }
        Ok(jobs)
    }

    pub fn list_due_cron_jobs(
        &self,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<Vec<CronJobRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = limit.clamp(1, MAX_CRON_JOBS_LIST_LIMIT);
        let mut statement = guard.prepare(
            r#"
                SELECT
                    job_ulid,
                    name,
                    prompt,
                    owner_principal,
                    channel,
                    session_key,
                    session_label,
                    schedule_type,
                    schedule_payload_json,
                    enabled,
                    concurrency_policy,
                    retry_policy_json,
                    misfire_policy,
                    jitter_ms,
                    next_run_at_unix_ms,
                    last_run_at_unix_ms,
                    queued_run,
                    created_at_unix_ms,
                    updated_at_unix_ms
                FROM cron_jobs
                WHERE
                    enabled = 1
                    AND next_run_at_unix_ms IS NOT NULL
                    AND next_run_at_unix_ms <= ?1
                ORDER BY next_run_at_unix_ms ASC, job_ulid ASC
                LIMIT ?2
            "#,
        )?;
        let mut rows = statement.query(params![now_unix_ms, limit as i64])?;
        let mut jobs = Vec::new();
        while let Some(row) = rows.next()? {
            jobs.push(map_cron_job_row(row)?);
        }
        Ok(jobs)
    }

    pub fn first_due_cron_job_time(&self) -> Result<Option<i64>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let due = guard
            .query_row(
                r#"
                    SELECT next_run_at_unix_ms
                    FROM cron_jobs
                    WHERE enabled = 1 AND next_run_at_unix_ms IS NOT NULL
                    ORDER BY next_run_at_unix_ms ASC
                    LIMIT 1
                "#,
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()?
            .flatten();
        Ok(due)
    }

    pub fn set_cron_job_next_run(
        &self,
        job_id: &str,
        next_run_at_unix_ms: Option<i64>,
        last_run_at_unix_ms: Option<i64>,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE cron_jobs
                SET
                    next_run_at_unix_ms = ?2,
                    last_run_at_unix_ms = COALESCE(?3, last_run_at_unix_ms),
                    updated_at_unix_ms = ?4
                WHERE job_ulid = ?1
            "#,
            params![job_id, next_run_at_unix_ms, last_run_at_unix_ms, now],
        )?;
        if updated == 0 {
            return Err(JournalError::CronJobNotFound { job_id: job_id.to_owned() });
        }
        Ok(())
    }

    pub fn set_cron_job_queue_state(
        &self,
        job_id: &str,
        queued_run: bool,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE cron_jobs
                SET
                    queued_run = ?2,
                    updated_at_unix_ms = ?3
                WHERE job_ulid = ?1
            "#,
            params![job_id, queued_run as i64, now],
        )?;
        if updated == 0 {
            return Err(JournalError::CronJobNotFound { job_id: job_id.to_owned() });
        }
        Ok(())
    }

    pub fn start_cron_run(&self, request: &CronRunStartRequest) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        match guard.execute(
            r#"
                INSERT INTO cron_runs (
                    run_ulid,
                    job_ulid,
                    attempt,
                    session_ulid,
                    orchestrator_run_ulid,
                    started_at_unix_ms,
                    finished_at_unix_ms,
                    status,
                    error_kind,
                    error_message_redacted,
                    model_tokens_in,
                    model_tokens_out,
                    tool_calls,
                    tool_denies,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9, 0, 0, 0, 0, ?6, ?6
                )
            "#,
            params![
                request.run_id,
                request.job_id,
                request.attempt as i64,
                request.session_id,
                request.orchestrator_run_id,
                now,
                request.status.as_str(),
                request.error_kind,
                request.error_message_redacted.as_ref().map(|value| redact_error_text(value)),
            ],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("cron_runs.run_ulid"))
                            .unwrap_or(false)) =>
            {
                return Err(JournalError::DuplicateCronRunId { run_id: request.run_id.clone() });
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub fn finalize_cron_run(&self, request: &CronRunFinalizeRequest) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let finished_at = if request.status.is_active() { None } else { Some(now) };
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE cron_runs
                SET
                    status = ?2,
                    finished_at_unix_ms = COALESCE(?3, finished_at_unix_ms),
                    error_kind = ?4,
                    error_message_redacted = ?5,
                    model_tokens_in = ?6,
                    model_tokens_out = ?7,
                    tool_calls = ?8,
                    tool_denies = ?9,
                    orchestrator_run_ulid = COALESCE(?10, orchestrator_run_ulid),
                    session_ulid = COALESCE(?11, session_ulid),
                    updated_at_unix_ms = ?12
                WHERE run_ulid = ?1
            "#,
            params![
                request.run_id,
                request.status.as_str(),
                finished_at,
                request.error_kind,
                request.error_message_redacted.as_ref().map(|value| redact_error_text(value)),
                request.model_tokens_in as i64,
                request.model_tokens_out as i64,
                request.tool_calls as i64,
                request.tool_denies as i64,
                request.orchestrator_run_id,
                request.session_id,
                now
            ],
        )?;
        if updated == 0 {
            return Err(JournalError::CronRunNotFound { run_id: request.run_id.clone() });
        }
        Ok(())
    }

    pub fn cron_run(&self, run_id: &str) -> Result<Option<CronRunRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_cron_run_by_id(&guard, run_id)
    }

    pub fn active_cron_run_for_job(
        &self,
        job_id: &str,
    ) -> Result<Option<CronRunRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    run_ulid,
                    job_ulid,
                    attempt,
                    session_ulid,
                    orchestrator_run_ulid,
                    started_at_unix_ms,
                    finished_at_unix_ms,
                    status,
                    error_kind,
                    error_message_redacted,
                    model_tokens_in,
                    model_tokens_out,
                    tool_calls,
                    tool_denies,
                    created_at_unix_ms,
                    updated_at_unix_ms
                FROM cron_runs
                WHERE job_ulid = ?1
                  AND status IN ('accepted', 'running')
                ORDER BY started_at_unix_ms DESC
                LIMIT 1
            "#,
        )?;
        statement.query_row(params![job_id], map_cron_run_row).optional().map_err(Into::into)
    }

    pub fn list_cron_runs(
        &self,
        filter: CronRunsListFilter<'_>,
    ) -> Result<Vec<CronRunRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = filter.limit.clamp(1, MAX_CRON_RUNS_LIST_LIMIT);
        let mut statement = guard.prepare(
            r#"
                SELECT
                    run_ulid,
                    job_ulid,
                    attempt,
                    session_ulid,
                    orchestrator_run_ulid,
                    started_at_unix_ms,
                    finished_at_unix_ms,
                    status,
                    error_kind,
                    error_message_redacted,
                    model_tokens_in,
                    model_tokens_out,
                    tool_calls,
                    tool_denies,
                    created_at_unix_ms,
                    updated_at_unix_ms
                FROM cron_runs
                WHERE
                    (?1 IS NULL OR job_ulid = ?1) AND
                    (?2 IS NULL OR run_ulid > ?2)
                ORDER BY run_ulid ASC
                LIMIT ?3
            "#,
        )?;
        let mut rows =
            statement.query(params![filter.job_id, filter.after_run_id, limit as i64])?;
        let mut runs = Vec::new();
        while let Some(row) = rows.next()? {
            runs.push(map_cron_run_row(row)?);
        }
        Ok(runs)
    }

    pub fn create_approval(
        &self,
        request: &ApprovalCreateRequest,
    ) -> Result<ApprovalRecord, JournalError> {
        let now = current_unix_ms()?;
        let policy_snapshot_json =
            sanitize_payload(serde_json::to_string(&request.policy_snapshot)?.as_bytes())?.0;
        let prompt_json = sanitize_payload(serde_json::to_string(&request.prompt)?.as_bytes())?.0;
        let request_summary =
            sanitize_object_text_field("summary", request.request_summary.as_str())?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        match guard.execute(
            r#"
                INSERT INTO approvals (
                    approval_ulid,
                    session_ulid,
                    run_ulid,
                    principal,
                    device_id,
                    channel,
                    requested_at_unix_ms,
                    resolved_at_unix_ms,
                    subject_type,
                    subject_id,
                    request_summary,
                    decision,
                    decision_scope,
                    decision_reason,
                    decision_scope_ttl_ms,
                    policy_snapshot_json,
                    prompt_json,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9, ?10, NULL, NULL, NULL, NULL, ?11, ?12, ?13, ?13
                )
            "#,
            params![
                request.approval_id,
                request.session_id,
                request.run_id,
                request.principal,
                request.device_id,
                request.channel,
                now,
                request.subject_type.as_str(),
                request.subject_id,
                request_summary,
                policy_snapshot_json,
                prompt_json,
                now,
            ],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("approvals.approval_ulid"))
                            .unwrap_or(false)) =>
            {
                return Err(JournalError::DuplicateApprovalId {
                    approval_id: request.approval_id.clone(),
                });
            }
            Err(error) => return Err(error.into()),
        }
        load_approval_by_id(&guard, request.approval_id.as_str())?.ok_or_else(|| {
            JournalError::ApprovalNotFound { approval_id: request.approval_id.clone() }
        })
    }

    pub fn resolve_approval(
        &self,
        request: &ApprovalResolveRequest,
    ) -> Result<ApprovalRecord, JournalError> {
        let now = current_unix_ms()?;
        let decision_reason =
            sanitize_object_text_field("reason", request.decision_reason.as_str())?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE approvals
                SET
                    decision = ?2,
                    decision_scope = ?3,
                    decision_reason = ?4,
                    decision_scope_ttl_ms = ?5,
                    resolved_at_unix_ms = COALESCE(resolved_at_unix_ms, ?6),
                    updated_at_unix_ms = ?6
                WHERE approval_ulid = ?1
            "#,
            params![
                request.approval_id,
                request.decision.as_str(),
                request.decision_scope.as_str(),
                decision_reason,
                request.decision_scope_ttl_ms,
                now
            ],
        )?;
        if updated == 0 {
            return Err(JournalError::ApprovalNotFound {
                approval_id: request.approval_id.clone(),
            });
        }
        load_approval_by_id(&guard, request.approval_id.as_str())?.ok_or_else(|| {
            JournalError::ApprovalNotFound { approval_id: request.approval_id.clone() }
        })
    }

    pub fn approval(&self, approval_id: &str) -> Result<Option<ApprovalRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_approval_by_id(&guard, approval_id)
    }

    pub fn list_approvals(
        &self,
        filter: ApprovalsListFilter<'_>,
    ) -> Result<Vec<ApprovalRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = filter.limit.clamp(1, MAX_APPROVALS_QUERY_LIMIT);
        let mut statement = guard.prepare(
            r#"
                SELECT
                    approval_ulid,
                    session_ulid,
                    run_ulid,
                    principal,
                    device_id,
                    channel,
                    requested_at_unix_ms,
                    resolved_at_unix_ms,
                    subject_type,
                    subject_id,
                    request_summary,
                    decision,
                    decision_scope,
                    decision_reason,
                    decision_scope_ttl_ms,
                    policy_snapshot_json,
                    prompt_json,
                    created_at_unix_ms,
                    updated_at_unix_ms
                FROM approvals
                WHERE
                    (?1 IS NULL OR approval_ulid > ?1) AND
                    (?2 IS NULL OR requested_at_unix_ms >= ?2) AND
                    (?3 IS NULL OR requested_at_unix_ms <= ?3) AND
                    (?4 IS NULL OR subject_id = ?4) AND
                    (?5 IS NULL OR principal = ?5) AND
                    (?6 IS NULL OR decision = ?6) AND
                    (?7 IS NULL OR subject_type = ?7)
                ORDER BY approval_ulid ASC
                LIMIT ?8
            "#,
        )?;
        let mut rows = statement.query(params![
            filter.after_approval_id,
            filter.since_unix_ms,
            filter.until_unix_ms,
            filter.subject_id,
            filter.principal,
            filter.decision.map(|value| value.as_str()),
            filter.subject_type.map(|value| value.as_str()),
            limit as i64,
        ])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(map_approval_row(row)?);
        }
        Ok(records)
    }

    pub fn upsert_skill_status(
        &self,
        request: &SkillStatusUpsertRequest,
    ) -> Result<SkillStatusRecord, JournalError> {
        let now = current_unix_ms()?;
        let reason = request
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard.execute(
            r#"
                INSERT INTO skill_status (
                    skill_id,
                    version,
                    status,
                    reason,
                    detected_at_ms,
                    operator_principal,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
                ON CONFLICT(skill_id, version) DO UPDATE SET
                    status = excluded.status,
                    reason = excluded.reason,
                    detected_at_ms = excluded.detected_at_ms,
                    operator_principal = excluded.operator_principal,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                request.skill_id.as_str(),
                request.version.as_str(),
                request.status.as_str(),
                reason,
                request.detected_at_ms,
                request.operator_principal.as_str(),
                now,
            ],
        )?;
        load_skill_status_by_key(&guard, request.skill_id.as_str(), request.version.as_str())?
            .ok_or(JournalError::Sqlite(rusqlite::Error::QueryReturnedNoRows))
    }

    pub fn skill_status(
        &self,
        skill_id: &str,
        version: &str,
    ) -> Result<Option<SkillStatusRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_skill_status_by_key(&guard, skill_id, version)
    }

    pub fn latest_skill_status(
        &self,
        skill_id: &str,
    ) -> Result<Option<SkillStatusRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_latest_skill_status_by_id(&guard, skill_id)
    }

    pub fn record_canvas_state_transition(
        &self,
        request: &CanvasStateTransitionRequest,
    ) -> Result<CanvasStateSnapshotRecord, JournalError> {
        if request.state_version == 0 {
            return Err(JournalError::InvalidCanvasReplay {
                canvas_id: request.canvas_id.clone(),
                reason: "state_version must be greater than 0".to_owned(),
            });
        }
        if request.state_schema_version == 0 {
            return Err(JournalError::InvalidCanvasReplay {
                canvas_id: request.canvas_id.clone(),
                reason: "state_schema_version must be greater than 0".to_owned(),
            });
        }
        if request.state_version <= request.base_state_version {
            return Err(JournalError::InvalidCanvasReplay {
                canvas_id: request.canvas_id.clone(),
                reason: format!(
                    "state_version {} must be greater than base_state_version {}",
                    request.state_version, request.base_state_version
                ),
            });
        }
        if request.state_json.len() > self.config.max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "canvas_state",
                actual_bytes: request.state_json.len(),
                max_bytes: self.config.max_payload_bytes,
            });
        }
        if request.patch_json.len() > self.config.max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "canvas_patch",
                actual_bytes: request.patch_json.len(),
                max_bytes: self.config.max_payload_bytes,
            });
        }
        if request.bundle_json.len() > self.config.max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "canvas_bundle",
                actual_bytes: request.bundle_json.len(),
                max_bytes: self.config.max_payload_bytes,
            });
        }
        if request.allowed_parent_origins_json.len() > self.config.max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "canvas_allowed_parent_origins",
                actual_bytes: request.allowed_parent_origins_json.len(),
                max_bytes: self.config.max_payload_bytes,
            });
        }

        let close_reason = request
            .close_reason
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        match transaction.execute(
            r#"
                INSERT INTO canvas_state_patches (
                    canvas_ulid,
                    state_version,
                    base_state_version,
                    state_schema_version,
                    patch_json,
                    resulting_state_json,
                    closed,
                    close_reason,
                    actor_principal,
                    actor_device_id,
                    applied_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                request.canvas_id.as_str(),
                request.state_version as i64,
                request.base_state_version as i64,
                request.state_schema_version as i64,
                request.patch_json.as_str(),
                request.state_json.as_str(),
                i64::from(request.closed),
                close_reason.as_deref(),
                request.actor_principal.as_str(),
                request.actor_device_id.as_str(),
                request.updated_at_unix_ms,
            ],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("canvas_state_patches.canvas_ulid"))
                            .unwrap_or(false)) =>
            {
                return Err(JournalError::DuplicateCanvasStateVersion {
                    canvas_id: request.canvas_id.clone(),
                    state_version: request.state_version,
                });
            }
            Err(error) => return Err(error.into()),
        }

        transaction.execute(
            r#"
                INSERT INTO canvas_state_snapshots (
                    canvas_ulid,
                    session_ulid,
                    principal,
                    state_version,
                    state_schema_version,
                    state_json,
                    bundle_json,
                    allowed_parent_origins_json,
                    created_at_unix_ms,
                    updated_at_unix_ms,
                    expires_at_unix_ms,
                    closed,
                    close_reason
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ON CONFLICT(canvas_ulid) DO UPDATE SET
                    session_ulid = excluded.session_ulid,
                    principal = excluded.principal,
                    state_version = excluded.state_version,
                    state_schema_version = excluded.state_schema_version,
                    state_json = excluded.state_json,
                    bundle_json = excluded.bundle_json,
                    allowed_parent_origins_json = excluded.allowed_parent_origins_json,
                    created_at_unix_ms = excluded.created_at_unix_ms,
                    updated_at_unix_ms = excluded.updated_at_unix_ms,
                    expires_at_unix_ms = excluded.expires_at_unix_ms,
                    closed = excluded.closed,
                    close_reason = excluded.close_reason
            "#,
            params![
                request.canvas_id.as_str(),
                request.session_id.as_str(),
                request.principal.as_str(),
                request.state_version as i64,
                request.state_schema_version as i64,
                request.state_json.as_str(),
                request.bundle_json.as_str(),
                request.allowed_parent_origins_json.as_str(),
                request.created_at_unix_ms,
                request.updated_at_unix_ms,
                request.expires_at_unix_ms,
                i64::from(request.closed),
                close_reason.as_deref(),
            ],
        )?;
        transaction.commit()?;
        drop(guard);
        self.canvas_state_snapshot(request.canvas_id.as_str())?.ok_or_else(|| {
            JournalError::CanvasStateNotFound { canvas_id: request.canvas_id.clone() }
        })
    }

    pub fn canvas_state_snapshot(
        &self,
        canvas_id: &str,
    ) -> Result<Option<CanvasStateSnapshotRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_canvas_state_snapshot_by_id(&guard, canvas_id)
    }

    pub fn list_canvas_state_snapshots(
        &self,
        limit: usize,
    ) -> Result<Vec<CanvasStateSnapshotRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = limit.clamp(1, MAX_CANVAS_PATCHES_QUERY_LIMIT) as i64;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    canvas_ulid,
                    session_ulid,
                    principal,
                    state_version,
                    state_schema_version,
                    state_json,
                    bundle_json,
                    allowed_parent_origins_json,
                    created_at_unix_ms,
                    updated_at_unix_ms,
                    expires_at_unix_ms,
                    closed,
                    close_reason
                FROM canvas_state_snapshots
                ORDER BY updated_at_unix_ms DESC, canvas_ulid ASC
                LIMIT ?1
            "#,
        )?;
        let mut rows = statement.query(params![limit])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(map_canvas_state_snapshot_row(row)?);
        }
        Ok(records)
    }

    pub fn list_canvas_state_patches(
        &self,
        canvas_id: &str,
        after_state_version: u64,
        limit: usize,
    ) -> Result<Vec<CanvasStatePatchRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = limit.clamp(1, MAX_CANVAS_PATCHES_QUERY_LIMIT) as i64;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    seq,
                    canvas_ulid,
                    state_version,
                    base_state_version,
                    state_schema_version,
                    patch_json,
                    resulting_state_json,
                    closed,
                    close_reason,
                    actor_principal,
                    actor_device_id,
                    applied_at_unix_ms
                FROM canvas_state_patches
                WHERE canvas_ulid = ?1
                  AND state_version > ?2
                ORDER BY state_version ASC
                LIMIT ?3
            "#,
        )?;
        let mut rows = statement.query(params![canvas_id, after_state_version as i64, limit])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(map_canvas_state_patch_row(row)?);
        }
        Ok(records)
    }

    pub fn replay_canvas_state(
        &self,
        canvas_id: &str,
    ) -> Result<Option<CanvasStateReplayRecord>, JournalError> {
        let mut next_after = 0_u64;
        let mut patches = Vec::new();
        loop {
            let batch = self.list_canvas_state_patches(
                canvas_id,
                next_after,
                MAX_CANVAS_PATCHES_QUERY_LIMIT,
            )?;
            if batch.is_empty() {
                break;
            }
            next_after = batch.last().map(|record| record.state_version).unwrap_or(next_after);
            patches.extend(batch);
            if patches.len() % MAX_CANVAS_PATCHES_QUERY_LIMIT != 0 {
                break;
            }
        }
        if patches.is_empty() {
            return Ok(None);
        }

        let mut current_state = Value::Null;
        let mut current_state_version = 0_u64;
        let mut current_state_schema_version = 1_u64;
        let mut closed = false;
        let mut close_reason = None;

        for patch_record in &patches {
            if patch_record.base_state_version != current_state_version {
                return Err(JournalError::InvalidCanvasReplay {
                    canvas_id: canvas_id.to_owned(),
                    reason: format!(
                        "patch version {} expected base {}, got {}",
                        patch_record.state_version,
                        current_state_version,
                        patch_record.base_state_version
                    ),
                });
            }
            let patch_document =
                parse_patch_document(patch_record.patch_json.as_bytes()).map_err(|error| {
                    JournalError::InvalidCanvasReplay {
                        canvas_id: canvas_id.to_owned(),
                        reason: format!(
                            "failed to parse patch for state_version {}: {error}",
                            patch_record.state_version
                        ),
                    }
                })?;
            current_state =
                apply_patch_document(&current_state, &patch_document).map_err(|error| {
                    JournalError::InvalidCanvasReplay {
                        canvas_id: canvas_id.to_owned(),
                        reason: format!(
                            "failed to apply patch for state_version {}: {error}",
                            patch_record.state_version
                        ),
                    }
                })?;
            let expected_state: Value = serde_json::from_str(
                patch_record.resulting_state_json.as_str(),
            )
            .map_err(|error| JournalError::InvalidCanvasReplay {
                canvas_id: canvas_id.to_owned(),
                reason: format!(
                    "invalid resulting state JSON at version {}: {error}",
                    patch_record.state_version
                ),
            })?;
            if current_state != expected_state {
                return Err(JournalError::InvalidCanvasReplay {
                    canvas_id: canvas_id.to_owned(),
                    reason: format!(
                        "patched state mismatch at version {}",
                        patch_record.state_version
                    ),
                });
            }
            current_state_version = patch_record.state_version;
            current_state_schema_version = patch_record.state_schema_version;
            closed = patch_record.closed;
            close_reason = patch_record.close_reason.clone();
        }

        Ok(Some(CanvasStateReplayRecord {
            canvas_id: canvas_id.to_owned(),
            state_version: current_state_version,
            state_schema_version: current_state_schema_version,
            state_json: serde_json::to_string(&current_state)?,
            closed,
            close_reason,
            patches_applied: patches.len(),
        }))
    }

    pub fn create_memory_item(
        &self,
        request: &MemoryItemCreateRequest,
    ) -> Result<MemoryItemRecord, JournalError> {
        let now = current_unix_ms()?;
        let embedding_dims = self.memory_embedding_provider.dimensions();
        let normalized_content = normalize_memory_text(request.content_text.as_str());
        let content_text = sanitize_object_text_field("content_text", normalized_content.as_str())?;
        let tags = normalize_memory_tags(request.tags.as_slice());
        let tags_json = serde_json::to_string(&tags)?;
        let content_hash = sha256_hex(content_text.as_bytes());
        let vector = normalize_embedding_dimensions(
            self.memory_embedding_provider.embed_text(content_text.as_str()),
            embedding_dims,
        );
        let vector_blob = encode_vector_blob(vector.as_slice());

        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        match transaction.execute(
            r#"
                INSERT INTO memory_items (
                    memory_ulid,
                    principal,
                    channel,
                    session_ulid,
                    source,
                    content_text,
                    content_hash,
                    tags_json,
                    confidence,
                    ttl_unix_ms,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)
            "#,
            params![
                request.memory_id.as_str(),
                request.principal.as_str(),
                request.channel.as_deref(),
                request.session_id.as_deref(),
                request.source.as_str(),
                content_text,
                content_hash,
                tags_json,
                request.confidence,
                request.ttl_unix_ms,
                now,
            ],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("memory_items.memory_ulid"))
                            .unwrap_or(false)) =>
            {
                return Err(JournalError::DuplicateMemoryId {
                    memory_id: request.memory_id.clone(),
                });
            }
            Err(error) => return Err(error.into()),
        }

        transaction.execute(
            r#"
                INSERT INTO memory_vectors (
                    memory_ulid,
                    embedding_model,
                    dims,
                    vector_blob,
                    created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                request.memory_id.as_str(),
                self.memory_embedding_provider.model_name(),
                embedding_dims as i64,
                vector_blob,
                now,
            ],
        )?;

        transaction.commit()?;
        drop(guard);
        self.purge_expired_memory_items(now)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_memory_item_by_id(&guard, request.memory_id.as_str(), now)?
            .ok_or_else(|| JournalError::MemoryNotFound { memory_id: request.memory_id.clone() })
    }

    pub fn memory_item(&self, memory_id: &str) -> Result<Option<MemoryItemRecord>, JournalError> {
        let now = current_unix_ms()?;
        self.purge_expired_memory_items(now)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_memory_item_by_id(&guard, memory_id, now)
    }

    pub fn delete_memory_item(
        &self,
        memory_id: &str,
        principal: &str,
        channel: Option<&str>,
    ) -> Result<bool, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let deleted = guard.execute(
            r#"
                DELETE FROM memory_items
                WHERE memory_ulid = ?1
                  AND principal = ?2
                  AND (?3 IS NULL OR channel = ?3)
            "#,
            params![memory_id, principal, channel],
        )?;
        Ok(deleted > 0)
    }

    pub fn list_memory_items(
        &self,
        filter: &MemoryItemsListFilter,
    ) -> Result<Vec<MemoryItemRecord>, JournalError> {
        let now = current_unix_ms()?;
        let requested_tags = normalize_memory_tags(filter.tags.as_slice());
        self.purge_expired_memory_items(now)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let limit = filter.limit.clamp(1, MAX_MEMORY_ITEMS_LIST_LIMIT);
        let fetch_limit = limit.saturating_mul(4).clamp(limit, MAX_MEMORY_SEARCH_CANDIDATES);
        let mut statement = guard.prepare(
            r#"
                SELECT
                    memory_ulid,
                    principal,
                    channel,
                    session_ulid,
                    source,
                    content_text,
                    content_hash,
                    tags_json,
                    confidence,
                    ttl_unix_ms,
                    created_at_unix_ms,
                    updated_at_unix_ms
                FROM memory_items
                WHERE
                    (?1 IS NULL OR memory_ulid > ?1) AND
                    principal = ?2 AND
                    (?3 IS NULL OR channel = ?3) AND
                    (?4 IS NULL OR session_ulid = ?4) AND
                    (ttl_unix_ms IS NULL OR ttl_unix_ms > ?5)
                ORDER BY memory_ulid ASC
                LIMIT ?6
            "#,
        )?;
        let mut rows = statement.query(params![
            filter.after_memory_id.as_deref(),
            filter.principal.as_str(),
            filter.channel.as_deref(),
            filter.session_id.as_deref(),
            now,
            fetch_limit as i64
        ])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            let item = map_memory_item_row(row)?;
            if !memory_source_matches(item.source, filter.sources.as_slice()) {
                continue;
            }
            if !memory_tags_match(item.tags.as_slice(), requested_tags.as_slice()) {
                continue;
            }
            items.push(item);
            if items.len() >= limit {
                break;
            }
        }
        Ok(items)
    }

    pub fn purge_memory(&self, request: &MemoryPurgeRequest) -> Result<u64, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let deleted = if request.purge_all_principal {
            guard.execute(
                r#"
                    DELETE FROM memory_items
                    WHERE principal = ?1
                      AND (?2 IS NULL OR channel = ?2)
                      AND (?3 IS NULL OR session_ulid = ?3)
                "#,
                params![
                    request.principal.as_str(),
                    request.channel.as_deref(),
                    request.session_id.as_deref()
                ],
            )?
        } else if let Some(session_id) = request.session_id.as_deref() {
            guard.execute(
                r#"
                    DELETE FROM memory_items
                    WHERE principal = ?1
                      AND session_ulid = ?2
                      AND (?3 IS NULL OR channel = ?3)
                "#,
                params![request.principal.as_str(), session_id, request.channel.as_deref()],
            )?
        } else if let Some(channel) = request.channel.as_deref() {
            guard.execute(
                "DELETE FROM memory_items WHERE principal = ?1 AND channel = ?2",
                params![request.principal.as_str(), channel],
            )?
        } else {
            0
        };
        Ok(deleted as u64)
    }

    pub fn purge_expired_memory_items(&self, now_unix_ms: i64) -> Result<u64, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let deleted = guard.execute(
            "DELETE FROM memory_items WHERE ttl_unix_ms IS NOT NULL AND ttl_unix_ms <= ?1",
            params![now_unix_ms],
        )?;
        Ok(deleted as u64)
    }

    pub fn memory_maintenance_status(&self) -> Result<MemoryMaintenanceStatus, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let usage = query_memory_usage_snapshot(&guard)?;
        let state = load_memory_maintenance_state(&guard)?.unwrap_or_default();
        let last_run = state.last_run_at_unix_ms.map(|ran_at_unix_ms| MemoryMaintenanceRunRecord {
            ran_at_unix_ms,
            deleted_expired_count: state.last_deleted_expired_count,
            deleted_capacity_count: state.last_deleted_capacity_count,
            deleted_total_count: state.last_deleted_total_count,
            entries_before: state.last_entries_before,
            entries_after: state.last_entries_after,
            approx_bytes_before: state.last_bytes_before,
            approx_bytes_after: state.last_bytes_after,
            vacuum_performed: state.last_vacuum_performed,
        });
        Ok(MemoryMaintenanceStatus {
            usage,
            last_run,
            last_vacuum_at_unix_ms: state.last_vacuum_at_unix_ms,
            next_vacuum_due_at_unix_ms: state.next_vacuum_due_at_unix_ms,
            next_maintenance_run_at_unix_ms: state.next_maintenance_run_at_unix_ms,
        })
    }

    pub fn run_memory_maintenance(
        &self,
        request: &MemoryMaintenanceRequest,
    ) -> Result<MemoryMaintenanceOutcome, JournalError> {
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let usage_before = query_memory_usage_snapshot(&guard)?;
        let previous_state = load_memory_maintenance_state(&guard)?.unwrap_or_default();

        let mut deleted_expired_count = 0_u64;
        let mut deleted_capacity_count = 0_u64;
        if request.retention.is_enforced() {
            let transaction = guard.transaction()?;
            deleted_expired_count = deleted_expired_count.saturating_add(transaction.execute(
                "DELETE FROM memory_items WHERE ttl_unix_ms IS NOT NULL AND ttl_unix_ms <= ?1",
                params![request.now_unix_ms],
            )? as u64);
            if let Some(ttl_days) = request.retention.ttl_days {
                let cutoff = request
                    .now_unix_ms
                    .saturating_sub(i64::from(ttl_days).saturating_mul(MEMORY_RETENTION_DAY_MS));
                deleted_expired_count = deleted_expired_count.saturating_add(transaction.execute(
                    "DELETE FROM memory_items WHERE created_at_unix_ms <= ?1",
                    params![cutoff],
                )?
                    as u64);
            }
            if let Some(max_entries) = request.retention.max_entries {
                deleted_capacity_count = deleted_capacity_count.saturating_add(
                    evict_oldest_memory_items_by_entry_cap(&transaction, max_entries)?,
                );
            }
            if let Some(max_bytes) = request.retention.max_bytes {
                deleted_capacity_count = deleted_capacity_count.saturating_add(
                    evict_oldest_memory_items_by_byte_cap(&transaction, max_bytes)?,
                );
            }
            transaction.commit()?;
        }

        let mut last_vacuum_at_unix_ms = previous_state.last_vacuum_at_unix_ms;
        let mut vacuum_performed = false;
        if request.next_vacuum_due_at_unix_ms.is_some_and(|due_at| due_at <= request.now_unix_ms) {
            guard.execute_batch("VACUUM;")?;
            vacuum_performed = true;
            last_vacuum_at_unix_ms = Some(request.now_unix_ms);
        }

        let usage_after = query_memory_usage_snapshot(&guard)?;
        let deleted_total_count = deleted_expired_count.saturating_add(deleted_capacity_count);
        upsert_memory_maintenance_state(
            &guard,
            request.now_unix_ms,
            request.next_vacuum_due_at_unix_ms,
            request.next_maintenance_run_at_unix_ms,
            last_vacuum_at_unix_ms,
            deleted_expired_count,
            deleted_capacity_count,
            deleted_total_count,
            usage_before.entries,
            usage_after.entries,
            usage_before.approx_bytes,
            usage_after.approx_bytes,
            vacuum_performed,
        )?;
        Ok(MemoryMaintenanceOutcome {
            ran_at_unix_ms: request.now_unix_ms,
            deleted_expired_count,
            deleted_capacity_count,
            deleted_total_count,
            entries_before: usage_before.entries,
            entries_after: usage_after.entries,
            approx_bytes_before: usage_before.approx_bytes,
            approx_bytes_after: usage_after.approx_bytes,
            vacuum_performed,
            last_vacuum_at_unix_ms,
            next_vacuum_due_at_unix_ms: request.next_vacuum_due_at_unix_ms,
            next_maintenance_run_at_unix_ms: request.next_maintenance_run_at_unix_ms,
        })
    }

    pub fn search_memory(
        &self,
        request: &MemorySearchRequest,
    ) -> Result<Vec<MemorySearchHit>, JournalError> {
        let query_text = normalize_memory_text(request.query.as_str());
        let embedding_dims = self.memory_embedding_provider.dimensions();
        let requested_tags = normalize_memory_tags(request.tags.as_slice());
        if query_text.is_empty() {
            return Ok(Vec::new());
        }
        let now = current_unix_ms()?;
        let top_k = request.top_k.clamp(1, MAX_MEMORY_ITEMS_LIST_LIMIT);
        let candidate_limit = top_k.saturating_mul(8).clamp(top_k, MAX_MEMORY_SEARCH_CANDIDATES);
        let fts_query = build_fts_query(query_text.as_str());
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }

        let query_vector = normalize_embedding_dimensions(
            self.memory_embedding_provider.embed_text(query_text.as_str()),
            embedding_dims,
        );
        self.purge_expired_memory_items(now)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    memory.memory_ulid,
                    memory.principal,
                    memory.channel,
                    memory.session_ulid,
                    memory.source,
                    memory.content_text,
                    memory.content_hash,
                    memory.tags_json,
                    memory.confidence,
                    memory.ttl_unix_ms,
                    memory.created_at_unix_ms,
                    memory.updated_at_unix_ms,
                    bm25(memory_items_fts) AS lexical_rank,
                    vectors.dims,
                    vectors.vector_blob
                FROM memory_items_fts
                INNER JOIN memory_items AS memory
                    ON memory.memory_ulid = memory_items_fts.memory_ulid
                LEFT JOIN memory_vectors AS vectors
                    ON vectors.memory_ulid = memory.memory_ulid
                WHERE
                    memory_items_fts MATCH ?1 AND
                    memory.principal = ?2 AND
                    (?3 IS NULL OR memory.channel = ?3) AND
                    (?4 IS NULL OR memory.session_ulid = ?4) AND
                    (memory.ttl_unix_ms IS NULL OR memory.ttl_unix_ms > ?5)
                ORDER BY lexical_rank ASC, memory.created_at_unix_ms DESC
                LIMIT ?6
            "#,
        )?;
        let mut rows = statement.query(params![
            fts_query,
            request.principal.as_str(),
            request.channel.as_deref(),
            request.session_id.as_deref(),
            now,
            candidate_limit as i64,
        ])?;

        let mut candidates = Vec::new();
        while let Some(row) = rows.next()? {
            let item = map_memory_item_row(row)?;
            if !memory_source_matches(item.source, request.sources.as_slice()) {
                continue;
            }
            if !memory_tags_match(item.tags.as_slice(), requested_tags.as_slice()) {
                continue;
            }
            let lexical_rank: f64 = row.get(12)?;
            let lexical_raw = (-lexical_rank).max(0.0);
            let dims = row.get::<_, Option<i64>>(13)?.unwrap_or_default() as usize;
            let vector_raw = if dims == embedding_dims {
                let vector_blob: Option<Vec<u8>> = row.get(14)?;
                vector_blob
                    .as_ref()
                    .map(|blob| decode_vector_blob(blob.as_slice(), dims))
                    .map(|embedding| {
                        cosine_similarity(query_vector.as_slice(), embedding.as_slice())
                    })
                    .unwrap_or(0.0)
                    .max(0.0)
            } else {
                0.0
            };
            let recency_raw = recency_score(now, item.created_at_unix_ms);
            candidates.push(RankedMemoryCandidate {
                item,
                lexical_raw,
                vector_raw,
                recency_raw,
                lexical_score: 0.0,
                vector_score: 0.0,
                final_score: 0.0,
            });
        }

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let lexical_max =
            candidates.iter().map(|candidate| candidate.lexical_raw).fold(0.0, f64::max);
        let vector_max =
            candidates.iter().map(|candidate| candidate.vector_raw).fold(0.0, f64::max);

        for candidate in &mut candidates {
            candidate.lexical_score =
                if lexical_max > 0.0 { candidate.lexical_raw / lexical_max } else { 0.0 };
            candidate.vector_score =
                if vector_max > 0.0 { candidate.vector_raw / vector_max } else { 0.0 };
            candidate.final_score = (0.55 * candidate.lexical_score)
                + (0.35 * candidate.vector_score)
                + (0.10 * candidate.recency_raw);
        }

        candidates.retain(|candidate| candidate.final_score >= request.min_score);
        candidates.sort_by(|left, right| {
            right
                .final_score
                .total_cmp(&left.final_score)
                .then_with(|| right.item.created_at_unix_ms.cmp(&left.item.created_at_unix_ms))
                .then_with(|| left.item.memory_id.cmp(&right.item.memory_id))
        });
        candidates.truncate(top_k);

        let hits = candidates
            .into_iter()
            .map(|candidate| MemorySearchHit {
                snippet: memory_snippet(candidate.item.content_text.as_str(), query_text.as_str()),
                score: candidate.final_score,
                breakdown: MemoryScoreBreakdown {
                    lexical_score: candidate.lexical_score,
                    vector_score: candidate.vector_score,
                    recency_score: candidate.recency_raw,
                    final_score: candidate.final_score,
                },
                item: candidate.item,
            })
            .collect();
        Ok(hits)
    }
}

#[cfg(test)]
fn load_orchestrator_tape(
    connection: &Connection,
    run_id: &str,
) -> Result<Vec<OrchestratorTapeRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT seq, event_type, payload_json
            FROM orchestrator_tape
            WHERE run_ulid = ?1
            ORDER BY seq ASC
        "#,
    )?;
    let mut rows = statement.query(params![run_id])?;
    let mut tape = Vec::new();
    while let Some(row) = rows.next()? {
        tape.push(OrchestratorTapeRecord {
            seq: row.get(0)?,
            event_type: row.get(1)?,
            payload_json: row.get(2)?,
        });
    }
    Ok(tape)
}

fn load_orchestrator_tape_page(
    connection: &Connection,
    run_id: &str,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Vec<OrchestratorTapeRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT seq, event_type, payload_json
            FROM orchestrator_tape
            WHERE run_ulid = ?1
              AND (?2 IS NULL OR seq > ?2)
            ORDER BY seq ASC
            LIMIT ?3
        "#,
    )?;
    let mut rows = statement.query(params![run_id, after_seq, limit as i64])?;
    let mut tape = Vec::new();
    while let Some(row) = rows.next()? {
        tape.push(OrchestratorTapeRecord {
            seq: row.get(0)?,
            event_type: row.get(1)?,
            payload_json: row.get(2)?,
        });
    }
    Ok(tape)
}

fn map_orchestrator_session_row(
    row: &rusqlite::Row<'_>,
) -> Result<OrchestratorSessionRecord, rusqlite::Error> {
    Ok(OrchestratorSessionRecord {
        session_id: row.get(0)?,
        session_key: row.get(1)?,
        session_label: row.get(2)?,
        principal: row.get(3)?,
        device_id: row.get(4)?,
        channel: row.get(5)?,
        created_at_unix_ms: row.get(6)?,
        updated_at_unix_ms: row.get(7)?,
        last_run_id: row.get(8)?,
    })
}

fn load_orchestrator_session_by_id(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<OrchestratorSessionRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                session_ulid,
                session_key,
                session_label,
                principal,
                device_id,
                channel,
                created_at_unix_ms,
                updated_at_unix_ms,
                last_run_ulid
            FROM orchestrator_sessions
            WHERE session_ulid = ?1
            LIMIT 1
        "#,
    )?;
    statement
        .query_row(params![session_id], map_orchestrator_session_row)
        .optional()
        .map_err(Into::into)
}

fn load_orchestrator_session_by_key(
    connection: &Connection,
    session_key: &str,
) -> Result<Option<OrchestratorSessionRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                session_ulid,
                session_key,
                session_label,
                principal,
                device_id,
                channel,
                created_at_unix_ms,
                updated_at_unix_ms,
                last_run_ulid
            FROM orchestrator_sessions
            WHERE session_key = ?1
            LIMIT 1
        "#,
    )?;
    statement
        .query_row(params![session_key], map_orchestrator_session_row)
        .optional()
        .map_err(Into::into)
}

fn load_orchestrator_session_by_label(
    connection: &Connection,
    session_label: &str,
) -> Result<Option<OrchestratorSessionRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                session_ulid,
                session_key,
                session_label,
                principal,
                device_id,
                channel,
                created_at_unix_ms,
                updated_at_unix_ms,
                last_run_ulid
            FROM orchestrator_sessions
            WHERE session_label = ?1
            ORDER BY updated_at_unix_ms DESC
            LIMIT 1
        "#,
    )?;
    statement
        .query_row(params![session_label], map_orchestrator_session_row)
        .optional()
        .map_err(Into::into)
}

fn load_orchestrator_sessions_page(
    connection: &Connection,
    after_session_key: Option<&str>,
    limit: usize,
) -> Result<Vec<OrchestratorSessionRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                session_ulid,
                session_key,
                session_label,
                principal,
                device_id,
                channel,
                created_at_unix_ms,
                updated_at_unix_ms,
                last_run_ulid
            FROM orchestrator_sessions
            WHERE (?1 IS NULL OR session_key > ?1)
            ORDER BY session_key ASC
            LIMIT ?2
        "#,
    )?;
    let mut rows = statement.query(params![after_session_key, limit as i64])?;
    let mut sessions = Vec::new();
    while let Some(row) = rows.next()? {
        sessions.push(map_orchestrator_session_row(row)?);
    }
    Ok(sessions)
}

fn map_cron_job_row(row: &rusqlite::Row<'_>) -> Result<CronJobRecord, rusqlite::Error> {
    let schedule_type_raw: String = row.get(7)?;
    let schedule_type =
        CronScheduleType::from_str(schedule_type_raw.as_str()).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid cron schedule_type value: {schedule_type_raw}"),
                )),
            )
        })?;
    let concurrency_policy_raw: String = row.get(10)?;
    let concurrency_policy = CronConcurrencyPolicy::from_str(concurrency_policy_raw.as_str())
        .ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                10,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid cron concurrency_policy value: {concurrency_policy_raw}"),
                )),
            )
        })?;
    let retry_policy_json: String = row.get(11)?;
    let retry_policy: CronRetryPolicy =
        serde_json::from_str(retry_policy_json.as_str()).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid retry_policy_json: {error}"),
                )),
            )
        })?;
    let misfire_policy_raw: String = row.get(12)?;
    let misfire_policy =
        CronMisfirePolicy::from_str(misfire_policy_raw.as_str()).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                12,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid cron misfire_policy value: {misfire_policy_raw}"),
                )),
            )
        })?;

    Ok(CronJobRecord {
        job_id: row.get(0)?,
        name: row.get(1)?,
        prompt: row.get(2)?,
        owner_principal: row.get(3)?,
        channel: row.get(4)?,
        session_key: row.get(5)?,
        session_label: row.get(6)?,
        schedule_type,
        schedule_payload_json: row.get(8)?,
        enabled: row.get::<_, i64>(9)? == 1,
        concurrency_policy,
        retry_policy,
        misfire_policy,
        jitter_ms: row.get::<_, i64>(13)? as u64,
        next_run_at_unix_ms: row.get(14)?,
        last_run_at_unix_ms: row.get(15)?,
        queued_run: row.get::<_, i64>(16)? == 1,
        created_at_unix_ms: row.get(17)?,
        updated_at_unix_ms: row.get(18)?,
    })
}

fn load_cron_job_by_id(
    connection: &Connection,
    job_id: &str,
) -> Result<Option<CronJobRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                job_ulid,
                name,
                prompt,
                owner_principal,
                channel,
                session_key,
                session_label,
                schedule_type,
                schedule_payload_json,
                enabled,
                concurrency_policy,
                retry_policy_json,
                misfire_policy,
                jitter_ms,
                next_run_at_unix_ms,
                last_run_at_unix_ms,
                queued_run,
                created_at_unix_ms,
                updated_at_unix_ms
            FROM cron_jobs
            WHERE job_ulid = ?1
            LIMIT 1
        "#,
    )?;
    statement.query_row(params![job_id], map_cron_job_row).optional().map_err(Into::into)
}

fn map_cron_run_row(row: &rusqlite::Row<'_>) -> Result<CronRunRecord, rusqlite::Error> {
    let status_raw: String = row.get(7)?;
    let status = CronRunStatus::from_str(status_raw.as_str()).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            7,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid cron run status value: {status_raw}"),
            )),
        )
    })?;
    Ok(CronRunRecord {
        run_id: row.get(0)?,
        job_id: row.get(1)?,
        attempt: row.get::<_, i64>(2)? as u32,
        session_id: row.get(3)?,
        orchestrator_run_id: row.get(4)?,
        started_at_unix_ms: row.get(5)?,
        finished_at_unix_ms: row.get(6)?,
        status,
        error_kind: row.get(8)?,
        error_message_redacted: row.get(9)?,
        model_tokens_in: row.get::<_, i64>(10)? as u64,
        model_tokens_out: row.get::<_, i64>(11)? as u64,
        tool_calls: row.get::<_, i64>(12)? as u64,
        tool_denies: row.get::<_, i64>(13)? as u64,
        created_at_unix_ms: row.get(14)?,
        updated_at_unix_ms: row.get(15)?,
    })
}

fn load_cron_run_by_id(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<CronRunRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                run_ulid,
                job_ulid,
                attempt,
                session_ulid,
                orchestrator_run_ulid,
                started_at_unix_ms,
                finished_at_unix_ms,
                status,
                error_kind,
                error_message_redacted,
                model_tokens_in,
                model_tokens_out,
                tool_calls,
                tool_denies,
                created_at_unix_ms,
                updated_at_unix_ms
            FROM cron_runs
            WHERE run_ulid = ?1
            LIMIT 1
        "#,
    )?;
    statement.query_row(params![run_id], map_cron_run_row).optional().map_err(Into::into)
}

fn map_approval_row(row: &rusqlite::Row<'_>) -> Result<ApprovalRecord, rusqlite::Error> {
    let subject_type_raw: String = row.get(8)?;
    let subject_type =
        ApprovalSubjectType::from_str(subject_type_raw.as_str()).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid approval subject_type value: {subject_type_raw}"),
                )),
            )
        })?;
    let decision = row
        .get::<_, Option<String>>(11)?
        .map(|value| {
            ApprovalDecision::from_str(value.as_str()).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    11,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid approval decision value: {value}"),
                    )),
                )
            })
        })
        .transpose()?;
    let decision_scope = row
        .get::<_, Option<String>>(12)?
        .map(|value| {
            ApprovalDecisionScope::from_str(value.as_str()).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    12,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid approval decision_scope value: {value}"),
                    )),
                )
            })
        })
        .transpose()?;
    let policy_snapshot_json: String = row.get(15)?;
    let policy_snapshot: ApprovalPolicySnapshot =
        serde_json::from_str(policy_snapshot_json.as_str()).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                15,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid approval policy_snapshot_json: {error}"),
                )),
            )
        })?;
    let prompt_json: String = row.get(16)?;
    let prompt: ApprovalPromptRecord =
        serde_json::from_str(prompt_json.as_str()).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                16,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid approval prompt_json: {error}"),
                )),
            )
        })?;

    Ok(ApprovalRecord {
        approval_id: row.get(0)?,
        session_id: row.get(1)?,
        run_id: row.get(2)?,
        principal: row.get(3)?,
        device_id: row.get(4)?,
        channel: row.get(5)?,
        requested_at_unix_ms: row.get(6)?,
        resolved_at_unix_ms: row.get(7)?,
        subject_type,
        subject_id: row.get(9)?,
        request_summary: row.get(10)?,
        decision,
        decision_scope,
        decision_reason: row.get(13)?,
        decision_scope_ttl_ms: row.get(14)?,
        policy_snapshot,
        prompt,
        created_at_unix_ms: row.get(17)?,
        updated_at_unix_ms: row.get(18)?,
    })
}

fn load_approval_by_id(
    connection: &Connection,
    approval_id: &str,
) -> Result<Option<ApprovalRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                approval_ulid,
                session_ulid,
                run_ulid,
                principal,
                device_id,
                channel,
                requested_at_unix_ms,
                resolved_at_unix_ms,
                subject_type,
                subject_id,
                request_summary,
                decision,
                decision_scope,
                decision_reason,
                decision_scope_ttl_ms,
                policy_snapshot_json,
                prompt_json,
                created_at_unix_ms,
                updated_at_unix_ms
            FROM approvals
            WHERE approval_ulid = ?1
            LIMIT 1
        "#,
    )?;
    statement.query_row(params![approval_id], map_approval_row).optional().map_err(Into::into)
}

fn map_canvas_state_snapshot_row(
    row: &rusqlite::Row<'_>,
) -> Result<CanvasStateSnapshotRecord, rusqlite::Error> {
    Ok(CanvasStateSnapshotRecord {
        canvas_id: row.get(0)?,
        session_id: row.get(1)?,
        principal: row.get(2)?,
        state_version: integer_to_u64(row, 3, "canvas_state_snapshots.state_version")?,
        state_schema_version: integer_to_u64(
            row,
            4,
            "canvas_state_snapshots.state_schema_version",
        )?,
        state_json: row.get(5)?,
        bundle_json: row.get(6)?,
        allowed_parent_origins_json: row.get(7)?,
        created_at_unix_ms: row.get(8)?,
        updated_at_unix_ms: row.get(9)?,
        expires_at_unix_ms: row.get(10)?,
        closed: row.get::<_, i64>(11)? == 1,
        close_reason: row.get(12)?,
    })
}

fn load_canvas_state_snapshot_by_id(
    connection: &Connection,
    canvas_id: &str,
) -> Result<Option<CanvasStateSnapshotRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                canvas_ulid,
                session_ulid,
                principal,
                state_version,
                state_schema_version,
                state_json,
                bundle_json,
                allowed_parent_origins_json,
                created_at_unix_ms,
                updated_at_unix_ms,
                expires_at_unix_ms,
                closed,
                close_reason
            FROM canvas_state_snapshots
            WHERE canvas_ulid = ?1
            LIMIT 1
        "#,
    )?;
    statement
        .query_row(params![canvas_id], map_canvas_state_snapshot_row)
        .optional()
        .map_err(Into::into)
}

fn map_canvas_state_patch_row(
    row: &rusqlite::Row<'_>,
) -> Result<CanvasStatePatchRecord, rusqlite::Error> {
    Ok(CanvasStatePatchRecord {
        seq: row.get(0)?,
        canvas_id: row.get(1)?,
        state_version: integer_to_u64(row, 2, "canvas_state_patches.state_version")?,
        base_state_version: integer_to_u64(row, 3, "canvas_state_patches.base_state_version")?,
        state_schema_version: integer_to_u64(row, 4, "canvas_state_patches.state_schema_version")?,
        patch_json: row.get(5)?,
        resulting_state_json: row.get(6)?,
        closed: row.get::<_, i64>(7)? == 1,
        close_reason: row.get(8)?,
        actor_principal: row.get(9)?,
        actor_device_id: row.get(10)?,
        applied_at_unix_ms: row.get(11)?,
    })
}

fn map_skill_status_row(row: &rusqlite::Row<'_>) -> Result<SkillStatusRecord, rusqlite::Error> {
    let status_raw: String = row.get(2)?;
    let status = SkillExecutionStatus::from_str(status_raw.as_str()).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid skill status value: {status_raw}"),
            )),
        )
    })?;
    Ok(SkillStatusRecord {
        skill_id: row.get(0)?,
        version: row.get(1)?,
        status,
        reason: row.get(3)?,
        detected_at_ms: row.get(4)?,
        operator_principal: row.get(5)?,
        created_at_unix_ms: row.get(6)?,
        updated_at_unix_ms: row.get(7)?,
    })
}

fn load_skill_status_by_key(
    connection: &Connection,
    skill_id: &str,
    version: &str,
) -> Result<Option<SkillStatusRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                skill_id,
                version,
                status,
                reason,
                detected_at_ms,
                operator_principal,
                created_at_unix_ms,
                updated_at_unix_ms
            FROM skill_status
            WHERE lower(skill_id) = lower(?1)
              AND version = ?2
            LIMIT 1
        "#,
    )?;
    statement
        .query_row(params![skill_id, version], map_skill_status_row)
        .optional()
        .map_err(Into::into)
}

fn load_latest_skill_status_by_id(
    connection: &Connection,
    skill_id: &str,
) -> Result<Option<SkillStatusRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                skill_id,
                version,
                status,
                reason,
                detected_at_ms,
                operator_principal,
                created_at_unix_ms,
                updated_at_unix_ms
            FROM skill_status
            WHERE lower(skill_id) = lower(?1)
            ORDER BY detected_at_ms DESC, updated_at_unix_ms DESC, version DESC
            LIMIT 1
        "#,
    )?;
    statement.query_row(params![skill_id], map_skill_status_row).optional().map_err(Into::into)
}

fn integer_to_u64(
    row: &rusqlite::Row<'_>,
    column: usize,
    field_name: &'static str,
) -> Result<u64, rusqlite::Error> {
    let raw: i64 = row.get(column)?;
    u64::try_from(raw).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{field_name} is negative"),
            )),
        )
    })
}

#[derive(Debug, Clone)]
struct RankedMemoryCandidate {
    item: MemoryItemRecord,
    lexical_raw: f64,
    vector_raw: f64,
    recency_raw: f64,
    lexical_score: f64,
    vector_score: f64,
    final_score: f64,
}

fn map_memory_item_row(row: &rusqlite::Row<'_>) -> Result<MemoryItemRecord, rusqlite::Error> {
    let source_raw: String = row.get(4)?;
    let source = MemorySource::from_str(source_raw.as_str()).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid memory source value: {source_raw}"),
            )),
        )
    })?;
    let tags_json: String = row.get(7)?;
    let tags: Vec<String> = serde_json::from_str(tags_json.as_str()).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            7,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid memory tags_json: {error}"),
            )),
        )
    })?;
    Ok(MemoryItemRecord {
        memory_id: row.get(0)?,
        principal: row.get(1)?,
        channel: row.get(2)?,
        session_id: row.get(3)?,
        source,
        content_text: row.get(5)?,
        content_hash: row.get(6)?,
        tags,
        confidence: row.get(8)?,
        ttl_unix_ms: row.get(9)?,
        created_at_unix_ms: row.get(10)?,
        updated_at_unix_ms: row.get(11)?,
    })
}

fn load_memory_item_by_id(
    connection: &Connection,
    memory_id: &str,
    now_unix_ms: i64,
) -> Result<Option<MemoryItemRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                memory_ulid,
                principal,
                channel,
                session_ulid,
                source,
                content_text,
                content_hash,
                tags_json,
                confidence,
                ttl_unix_ms,
                created_at_unix_ms,
                updated_at_unix_ms
            FROM memory_items
            WHERE memory_ulid = ?1
              AND (ttl_unix_ms IS NULL OR ttl_unix_ms > ?2)
            LIMIT 1
        "#,
    )?;
    statement
        .query_row(params![memory_id, now_unix_ms], map_memory_item_row)
        .optional()
        .map_err(Into::into)
}

fn query_memory_usage_snapshot(
    connection: &Connection,
) -> Result<MemoryUsageSnapshot, JournalError> {
    let (entries_raw, bytes_raw): (i64, i64) = connection.query_row(
        r#"
            SELECT
                COUNT(*),
                COALESCE(
                    SUM(
                        COALESCE(length(memory.content_text), 0) +
                        COALESCE(length(memory.content_hash), 0) +
                        COALESCE(length(memory.tags_json), 0) +
                        COALESCE(length(vectors.vector_blob), 0)
                    ),
                    0
                )
            FROM memory_items AS memory
            LEFT JOIN memory_vectors AS vectors
                ON vectors.memory_ulid = memory.memory_ulid
        "#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(MemoryUsageSnapshot {
        entries: entries_raw.max(0) as u64,
        approx_bytes: bytes_raw.max(0) as u64,
    })
}

fn load_memory_maintenance_state(
    connection: &Connection,
) -> Result<Option<MemoryMaintenanceStateRow>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT
                    last_run_at_unix_ms,
                    last_vacuum_at_unix_ms,
                    next_vacuum_due_at_unix_ms,
                    next_maintenance_run_at_unix_ms,
                    last_deleted_expired_count,
                    last_deleted_capacity_count,
                    last_deleted_total_count,
                    last_entries_before,
                    last_entries_after,
                    last_bytes_before,
                    last_bytes_after,
                    last_vacuum_performed
                FROM memory_maintenance_state
                WHERE singleton_key = ?1
                LIMIT 1
            "#,
            params![MEMORY_MAINTENANCE_STATE_SINGLETON_KEY],
            |row| {
                Ok(MemoryMaintenanceStateRow {
                    last_run_at_unix_ms: row.get(0)?,
                    last_vacuum_at_unix_ms: row.get(1)?,
                    next_vacuum_due_at_unix_ms: row.get(2)?,
                    next_maintenance_run_at_unix_ms: row.get(3)?,
                    last_deleted_expired_count: row.get::<_, i64>(4)?.max(0) as u64,
                    last_deleted_capacity_count: row.get::<_, i64>(5)?.max(0) as u64,
                    last_deleted_total_count: row.get::<_, i64>(6)?.max(0) as u64,
                    last_entries_before: row.get::<_, i64>(7)?.max(0) as u64,
                    last_entries_after: row.get::<_, i64>(8)?.max(0) as u64,
                    last_bytes_before: row.get::<_, i64>(9)?.max(0) as u64,
                    last_bytes_after: row.get::<_, i64>(10)?.max(0) as u64,
                    last_vacuum_performed: row.get::<_, i64>(11)? != 0,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
fn upsert_memory_maintenance_state(
    connection: &Connection,
    ran_at_unix_ms: i64,
    next_vacuum_due_at_unix_ms: Option<i64>,
    next_maintenance_run_at_unix_ms: Option<i64>,
    last_vacuum_at_unix_ms: Option<i64>,
    deleted_expired_count: u64,
    deleted_capacity_count: u64,
    deleted_total_count: u64,
    entries_before: u64,
    entries_after: u64,
    approx_bytes_before: u64,
    approx_bytes_after: u64,
    vacuum_performed: bool,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO memory_maintenance_state (
                singleton_key,
                last_run_at_unix_ms,
                last_vacuum_at_unix_ms,
                next_vacuum_due_at_unix_ms,
                next_maintenance_run_at_unix_ms,
                last_deleted_expired_count,
                last_deleted_capacity_count,
                last_deleted_total_count,
                last_entries_before,
                last_entries_after,
                last_bytes_before,
                last_bytes_after,
                last_vacuum_performed
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(singleton_key) DO UPDATE SET
                last_run_at_unix_ms = excluded.last_run_at_unix_ms,
                last_vacuum_at_unix_ms = excluded.last_vacuum_at_unix_ms,
                next_vacuum_due_at_unix_ms = excluded.next_vacuum_due_at_unix_ms,
                next_maintenance_run_at_unix_ms = excluded.next_maintenance_run_at_unix_ms,
                last_deleted_expired_count = excluded.last_deleted_expired_count,
                last_deleted_capacity_count = excluded.last_deleted_capacity_count,
                last_deleted_total_count = excluded.last_deleted_total_count,
                last_entries_before = excluded.last_entries_before,
                last_entries_after = excluded.last_entries_after,
                last_bytes_before = excluded.last_bytes_before,
                last_bytes_after = excluded.last_bytes_after,
                last_vacuum_performed = excluded.last_vacuum_performed
        "#,
        params![
            MEMORY_MAINTENANCE_STATE_SINGLETON_KEY,
            ran_at_unix_ms,
            last_vacuum_at_unix_ms,
            next_vacuum_due_at_unix_ms,
            next_maintenance_run_at_unix_ms,
            deleted_expired_count as i64,
            deleted_capacity_count as i64,
            deleted_total_count as i64,
            entries_before as i64,
            entries_after as i64,
            approx_bytes_before as i64,
            approx_bytes_after as i64,
            if vacuum_performed { 1_i64 } else { 0_i64 }
        ],
    )?;
    Ok(())
}

fn evict_oldest_memory_items_by_entry_cap(
    transaction: &Transaction<'_>,
    max_entries: usize,
) -> Result<u64, JournalError> {
    let max_entries_i64 = i64::try_from(max_entries).unwrap_or(i64::MAX);
    let current_entries: i64 =
        transaction.query_row("SELECT COUNT(*) FROM memory_items", [], |row| row.get(0))?;
    if current_entries <= max_entries_i64 {
        return Ok(0);
    }
    let overflow = current_entries.saturating_sub(max_entries_i64);
    let mut statement = transaction.prepare(
        r#"
            SELECT memory_ulid
            FROM memory_items
            ORDER BY created_at_unix_ms ASC, memory_ulid ASC
            LIMIT ?1
        "#,
    )?;
    let ids = statement
        .query_map(params![overflow], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    delete_memory_items_by_ids(transaction, ids.as_slice())
}

fn evict_oldest_memory_items_by_byte_cap(
    transaction: &Transaction<'_>,
    max_bytes: u64,
) -> Result<u64, JournalError> {
    let mut statement = transaction.prepare(
        r#"
            SELECT
                memory.memory_ulid,
                (
                    COALESCE(length(memory.content_text), 0) +
                    COALESCE(length(memory.content_hash), 0) +
                    COALESCE(length(memory.tags_json), 0) +
                    COALESCE(length(vectors.vector_blob), 0)
                ) AS approx_bytes
            FROM memory_items AS memory
            LEFT JOIN memory_vectors AS vectors
                ON vectors.memory_ulid = memory.memory_ulid
            ORDER BY memory.created_at_unix_ms ASC, memory.memory_ulid ASC
        "#,
    )?;
    let mut rows = statement.query([])?;
    let mut ordered = Vec::<(String, u64)>::new();
    let mut total_bytes = 0_u64;
    while let Some(row) = rows.next()? {
        let memory_id: String = row.get(0)?;
        let row_bytes = row.get::<_, i64>(1)?.max(0) as u64;
        total_bytes = total_bytes.saturating_add(row_bytes);
        ordered.push((memory_id, row_bytes));
    }
    if total_bytes <= max_bytes {
        return Ok(0);
    }
    let mut bytes_to_remove = total_bytes.saturating_sub(max_bytes);
    let mut ids_to_delete = Vec::<String>::new();
    for (memory_id, row_bytes) in ordered {
        ids_to_delete.push(memory_id);
        bytes_to_remove = bytes_to_remove.saturating_sub(row_bytes.max(1));
        if bytes_to_remove == 0 {
            break;
        }
    }
    delete_memory_items_by_ids(transaction, ids_to_delete.as_slice())
}

fn delete_memory_items_by_ids(
    transaction: &Transaction<'_>,
    ids: &[String],
) -> Result<u64, JournalError> {
    if ids.is_empty() {
        return Ok(0);
    }
    let mut deleted = 0_u64;
    for chunk in ids.chunks(250) {
        let placeholders = std::iter::repeat_n("?", chunk.len()).collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM memory_items WHERE memory_ulid IN ({placeholders})");
        deleted = deleted.saturating_add(
            transaction.execute(sql.as_str(), params_from_iter(chunk.iter().map(String::as_str)))?
                as u64,
        );
    }
    Ok(deleted)
}

fn normalize_memory_text(raw: &str) -> String {
    let mut normalized = String::with_capacity(raw.len());
    let mut previous_was_whitespace = false;
    for character in raw.chars() {
        if character.is_control() && !character.is_whitespace() {
            continue;
        }
        if character.is_whitespace() {
            if !previous_was_whitespace {
                normalized.push(' ');
                previous_was_whitespace = true;
            }
        } else {
            normalized.push(character);
            previous_was_whitespace = false;
        }
    }
    normalized.trim().to_owned()
}

fn normalize_memory_tags(raw: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for tag in raw {
        let trimmed = tag.trim().to_ascii_lowercase();
        if trimmed.is_empty() {
            continue;
        }
        if !trimmed.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, ':' | '_' | '-' | '.')
        }) {
            continue;
        }
        if !normalized.iter().any(|existing| existing == &trimmed) {
            normalized.push(trimmed);
        }
    }
    normalized
}

fn build_fts_query(query: &str) -> String {
    let mut terms = Vec::new();
    for token in query.split_whitespace() {
        let normalized = token
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || ch == '_' {
                    ch.to_ascii_lowercase()
                } else {
                    ' '
                }
            })
            .collect::<String>();
        for term in normalized.split_whitespace() {
            if !term.is_empty() {
                terms.push(term.to_owned());
            }
        }
    }
    terms.join(" ")
}

fn memory_source_matches(source: MemorySource, filter_sources: &[MemorySource]) -> bool {
    filter_sources.is_empty() || filter_sources.contains(&source)
}

fn memory_tags_match(item_tags: &[String], requested_tags: &[String]) -> bool {
    requested_tags.is_empty()
        || requested_tags
            .iter()
            .all(|required| item_tags.iter().any(|candidate| candidate == required))
}

fn recency_score(now_unix_ms: i64, created_at_unix_ms: i64) -> f64 {
    let age_ms = (now_unix_ms - created_at_unix_ms).max(0) as f64;
    1.0 / (1.0 + (age_ms / (24.0 * 60.0 * 60.0 * 1_000.0)))
}

fn memory_snippet(content: &str, query: &str) -> String {
    const MAX_SNIPPET_CHARS: usize = 200;
    if content.chars().count() <= MAX_SNIPPET_CHARS {
        return content.to_owned();
    }
    let lowered_content = content.to_ascii_lowercase();
    let first_term = query.split_whitespace().next().unwrap_or_default().to_ascii_lowercase();
    if first_term.is_empty() {
        return content.chars().take(MAX_SNIPPET_CHARS).collect();
    }
    let byte_index = lowered_content.find(first_term.as_str()).unwrap_or_default();
    let start_chars = content[..byte_index].chars().count().saturating_sub(48);
    content.chars().skip(start_chars).take(MAX_SNIPPET_CHARS).collect()
}

fn sha256_hex(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    format!("{:x}", hasher.finalize())
}

fn hash_embed_text(text: &str, dims: usize) -> Vec<f32> {
    let mut vector = vec![0_f32; dims];
    if dims == 0 {
        return vector;
    }
    for token in text.split_whitespace() {
        let normalized = token.to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        let digest = Sha256::digest(normalized.as_bytes());
        let index = usize::from(digest[0]) % dims;
        let sign = if digest[1] % 2 == 0 { 1.0_f32 } else { -1.0_f32 };
        let magnitude = 1.0 + (f32::from(digest[2]) / 255.0);
        vector[index] += sign * magnitude;
    }
    normalize_vector(vector.as_mut_slice());
    vector
}

fn normalize_embedding_dimensions(mut vector: Vec<f32>, expected_dims: usize) -> Vec<f32> {
    if expected_dims == 0 {
        return Vec::new();
    }
    if vector.len() < expected_dims {
        vector.resize(expected_dims, 0.0);
    } else if vector.len() > expected_dims {
        vector.truncate(expected_dims);
    }
    normalize_vector(vector.as_mut_slice());
    vector
}

fn normalize_vector(vector: &mut [f32]) {
    let norm = vector.iter().map(|value| f64::from(*value).powi(2)).sum::<f64>().sqrt();
    if norm <= f64::EPSILON {
        return;
    }
    for value in vector {
        *value = (f64::from(*value) / norm) as f32;
    }
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }
    left.iter()
        .zip(right.iter())
        .map(|(a, b)| f64::from(*a) * f64::from(*b))
        .sum::<f64>()
        .clamp(-1.0, 1.0)
}

fn encode_vector_blob(vector: &[f32]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(std::mem::size_of_val(vector));
    for value in vector {
        blob.extend_from_slice(value.to_le_bytes().as_slice());
    }
    blob
}

fn decode_vector_blob(blob: &[u8], dims: usize) -> Vec<f32> {
    let expected_bytes = dims.saturating_mul(std::mem::size_of::<f32>());
    if blob.len() != expected_bytes {
        return vec![0_f32; dims];
    }
    let mut vector = Vec::with_capacity(dims);
    for chunk in blob.chunks_exact(std::mem::size_of::<f32>()) {
        vector.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    normalize_vector(vector.as_mut_slice());
    vector
}

fn apply_migrations(connection: &mut Connection) -> Result<(), JournalError> {
    connection.execute_batch(
        r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at_unix_ms INTEGER NOT NULL
            )
        "#,
    )?;

    for migration in MIGRATIONS {
        let already_applied = connection
            .query_row(
                "SELECT 1 FROM schema_migrations WHERE version = ?1 LIMIT 1",
                params![migration.version],
                |_row| Ok(()),
            )
            .optional()?
            .is_some();
        if already_applied {
            continue;
        }

        let transaction = connection.transaction()?;
        transaction.execute_batch(migration.sql)?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, name, applied_at_unix_ms) VALUES (?1, ?2, ?3)",
            params![migration.version, migration.name, current_unix_ms()?],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

fn sanitize_payload(raw_payload: &[u8]) -> Result<(String, bool), JournalError> {
    if raw_payload.is_empty() {
        return Ok(("{}".to_owned(), false));
    }

    let raw_text = match std::str::from_utf8(raw_payload) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                json!({
                    "redacted": true,
                    "reason": "binary_or_non_utf8_payload",
                    "bytes": raw_payload.len(),
                })
                .to_string(),
                true,
            ));
        }
    };

    let mut value: Value = match serde_json::from_str(raw_text) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                json!({
                    "redacted": true,
                    "reason": "non_json_payload",
                    "bytes": raw_payload.len(),
                })
                .to_string(),
                true,
            ));
        }
    };

    let redacted = redact_value(&mut value, None);
    Ok((serde_json::to_string(&value)?, redacted))
}

pub fn redact_payload_json(raw_payload: &[u8]) -> Result<String, JournalError> {
    let (payload, _) = sanitize_payload(raw_payload)?;
    Ok(payload)
}

fn sanitize_object_text_field(key: &str, value: &str) -> Result<String, JournalError> {
    let payload = sanitize_payload(json!({ key: value }).to_string().as_bytes())?.0;
    let parsed = serde_json::from_str::<Value>(payload.as_str()).ok();
    let sanitized = parsed
        .as_ref()
        .and_then(|json| json.get(key))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| REDACTED_MARKER.to_owned());
    Ok(sanitized)
}

fn redact_value(value: &mut Value, key_context: Option<&str>) -> bool {
    match value {
        Value::Object(object) => {
            let mut redacted = false;
            for (key, child) in object.iter_mut() {
                if is_sensitive_key(key) {
                    *child = Value::String(REDACTED_MARKER.to_owned());
                    redacted = true;
                    continue;
                }
                redacted |= redact_value(child, Some(key.as_str()));
            }
            redacted
        }
        Value::Array(items) => {
            let mut redacted = false;
            for item in items {
                redacted |= redact_value(item, key_context);
            }
            redacted
        }
        Value::String(text) => {
            if key_context.map(is_sensitive_key).unwrap_or(false) {
                *value = Value::String(REDACTED_MARKER.to_owned());
                return true;
            }

            if let Ok(mut embedded_json) = serde_json::from_str::<Value>(text) {
                if redact_value(&mut embedded_json, None) {
                    *value = Value::String(
                        serde_json::to_string(&embedded_json)
                            .unwrap_or_else(|_| REDACTED_MARKER.to_owned()),
                    );
                    return true;
                }
            }

            if looks_like_secret(text) {
                *value = Value::String(REDACTED_MARKER.to_owned());
                return true;
            }

            false
        }
        _ => key_context.map(is_sensitive_key).unwrap_or(false),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    if SENSITIVE_KEY_FRAGMENTS.iter().any(|fragment| normalized.contains(fragment)) {
        return true;
    }
    normalized
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|token| SENSITIVE_KEY_TOKENS.contains(&token))
}

fn looks_like_secret(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    normalized.contains("bearer ")
        || normalized.starts_with("sk-")
        || normalized.contains("api_key=")
        || normalized.contains("secret=")
        || normalized.contains("token=")
        || normalized.contains("refresh_token")
        || normalized.contains("oauth_refresh_token")
        || normalized.contains("set-cookie:")
        || normalized.contains("cookie:")
}

fn redact_error_text(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    crate::model_provider::sanitize_remote_error(trimmed)
}

fn compute_hash(
    prev_hash: Option<&str>,
    request: &JournalAppendRequest,
    payload_json: &str,
) -> String {
    let mut hasher = Sha256::new();
    if let Some(prev_hash) = prev_hash {
        hasher.update(prev_hash.as_bytes());
    }
    hasher.update(b"|");
    hasher.update(request.event_id.as_bytes());
    hasher.update(b"|");
    hasher.update(request.session_id.as_bytes());
    hasher.update(b"|");
    hasher.update(request.run_id.as_bytes());
    hasher.update(b"|");
    hasher.update(request.kind.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(request.actor.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(request.timestamp_unix_ms.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(request.principal.as_bytes());
    hasher.update(b"|");
    hasher.update(request.device_id.as_bytes());
    hasher.update(b"|");
    if let Some(channel) = &request.channel {
        hasher.update(channel.as_bytes());
    }
    hasher.update(b"|");
    hasher.update(payload_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn current_unix_ms() -> Result<i64, JournalError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(now.as_millis() as i64)
}

fn normalize_optional_session_field(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn validate_db_path(path: &Path) -> Result<(), JournalError> {
    let path_text = path.to_string_lossy();
    if path_text.trim().is_empty() {
        return Err(JournalError::EmptyPath);
    }
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(JournalError::ParentTraversalPath { path: path_text.into_owned() });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use rusqlite::{params, Connection};
    use serde_json::json;

    use crate::orchestrator::RunLifecycleState;

    use super::{
        build_fts_query, current_unix_ms, ApprovalCreateRequest, ApprovalDecision,
        ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption, ApprovalPromptRecord,
        ApprovalResolveRequest, ApprovalRiskLevel, ApprovalSubjectType, ApprovalsListFilter,
        CanvasStateTransitionRequest, CronConcurrencyPolicy, CronJobCreateRequest,
        CronJobsListFilter, CronMisfirePolicy, CronRetryPolicy, CronRunFinalizeRequest,
        CronRunStartRequest, CronRunStatus, CronRunsListFilter, CronScheduleType,
        JournalAppendRequest, JournalConfig, JournalError, JournalStore, MemoryEmbeddingProvider,
        MemoryItemCreateRequest, MemoryItemsListFilter, MemoryMaintenanceRequest,
        MemoryPurgeRequest, MemoryRetentionPolicy, MemorySearchRequest, MemorySource,
        OrchestratorCancelRequest, OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest,
        OrchestratorTapeAppendRequest, OrchestratorUsageDelta, SkillExecutionStatus,
        SkillStatusUpsertRequest, MEMORY_RETENTION_DAY_MS,
    };

    static TEMP_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug)]
    struct FixedMemoryEmbeddingProvider {
        model_name: &'static str,
        dimensions: usize,
        vector: Vec<f32>,
    }

    impl MemoryEmbeddingProvider for FixedMemoryEmbeddingProvider {
        fn model_name(&self) -> &'static str {
            self.model_name
        }

        fn dimensions(&self) -> usize {
            self.dimensions
        }

        fn embed_text(&self, _text: &str) -> Vec<f32> {
            self.vector.clone()
        }
    }

    fn build_request(event_id: &str, payload_json: &[u8]) -> JournalAppendRequest {
        JournalAppendRequest {
            event_id: event_id.to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            kind: 1,
            actor: 1,
            timestamp_unix_ms: 1_730_000_000_000,
            payload_json: payload_json.to_vec(),
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        }
    }

    fn upsert_orchestrator_session(store: &JournalStore, session_id: &str) {
        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.to_owned(),
                session_key: session_id.to_owned(),
                session_label: None,
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("orchestrator session should be upserted");
    }

    fn sample_cron_job_request(job_id: &str) -> CronJobCreateRequest {
        CronJobCreateRequest {
            job_id: job_id.to_owned(),
            name: "Hourly report".to_owned(),
            prompt: "summarize health status".to_owned(),
            owner_principal: "user:ops".to_owned(),
            channel: "system:cron".to_owned(),
            session_key: Some("cron:hourly-report".to_owned()),
            session_label: Some("Hourly report".to_owned()),
            schedule_type: CronScheduleType::Every,
            schedule_payload_json: r#"{"interval_ms":60000}"#.to_owned(),
            enabled: true,
            concurrency_policy: CronConcurrencyPolicy::Forbid,
            retry_policy: CronRetryPolicy { max_attempts: 3, backoff_ms: 500 },
            misfire_policy: CronMisfirePolicy::Skip,
            jitter_ms: 250,
            next_run_at_unix_ms: Some(1_730_000_060_000),
        }
    }

    fn sample_approval_request(approval_id: &str) -> ApprovalCreateRequest {
        ApprovalCreateRequest {
            approval_id: approval_id.to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: "tool:palyra.process.run".to_owned(),
            request_summary: "run process with oauth_refresh_token=super-secret".to_owned(),
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: "tool_call_policy.v1".to_owned(),
                policy_hash: "sha256:test".to_owned(),
                evaluation_summary: "approval_required=true policy_enforced=true".to_owned(),
            },
            prompt: ApprovalPromptRecord {
                title: "Allow tool execution".to_owned(),
                risk_level: ApprovalRiskLevel::High,
                subject_id: "tool:palyra.process.run".to_owned(),
                summary: "Run a local process in workspace".to_owned(),
                options: vec![
                    ApprovalPromptOption {
                        option_id: "allow_once".to_owned(),
                        label: "Allow once".to_owned(),
                        description: "Allow this single action".to_owned(),
                        default_selected: true,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                    ApprovalPromptOption {
                        option_id: "deny_once".to_owned(),
                        label: "Deny".to_owned(),
                        description: "Deny this action".to_owned(),
                        default_selected: false,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                ],
                timeout_seconds: 60,
                details_json: json!({
                    "tool_name": "palyra.process.run",
                    "args": ["echo", "hello"],
                    "cookie": "sessionid=abc123"
                })
                .to_string(),
                policy_explanation: "Sensitive process execution requires explicit approval"
                    .to_owned(),
            },
        }
    }

    fn sample_memory_request(
        memory_id: &str,
        principal: &str,
        channel: Option<&str>,
        session_id: Option<&str>,
        source: MemorySource,
        content_text: &str,
    ) -> MemoryItemCreateRequest {
        MemoryItemCreateRequest {
            memory_id: memory_id.to_owned(),
            principal: principal.to_owned(),
            channel: channel.map(str::to_owned),
            session_id: session_id.map(str::to_owned),
            source,
            content_text: content_text.to_owned(),
            tags: Vec::new(),
            confidence: Some(0.9),
            ttl_unix_ms: None,
        }
    }

    fn sample_canvas_transition_request(
        canvas_id: &str,
        state_version: u64,
        base_state_version: u64,
        state_json: &str,
        patch_json: &str,
        closed: bool,
    ) -> CanvasStateTransitionRequest {
        CanvasStateTransitionRequest {
            canvas_id: canvas_id.to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAA".to_owned(),
            principal: "user:ops".to_owned(),
            state_version,
            base_state_version,
            state_schema_version: 1,
            state_json: state_json.to_owned(),
            patch_json: patch_json.to_owned(),
            bundle_json: r#"{"bundle_id":"demo","entrypoint_path":"app.js","assets":{"app.js":{"content_type":"application/javascript","body":[99,111,110,115,111,108,101,46,108,111,103,40,34,111,107,34,41,59]}},"sha256":"demo","signature":"sig"}"#.to_owned(),
            allowed_parent_origins_json: r#"["https://console.example.com"]"#.to_owned(),
            created_at_unix_ms: 1_730_000_000_000,
            updated_at_unix_ms: 1_730_000_000_000 + (state_version as i64 * 100),
            expires_at_unix_ms: 1_730_000_360_000,
            closed,
            close_reason: if closed { Some("operator_close".to_owned()) } else { None },
            actor_principal: "user:ops".to_owned(),
            actor_device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        }
    }

    fn temp_db_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("palyra-journal-test-{nonce}-{}-{counter}.sqlite3", std::process::id()))
    }

    fn test_journal_config(db_path: PathBuf, hash_chain_enabled: bool) -> JournalConfig {
        JournalConfig { db_path, hash_chain_enabled, max_payload_bytes: 256 * 1024 }
    }

    #[test]
    fn open_applies_initial_migration() {
        let db_path = temp_db_path();
        let _store = JournalStore::open(test_journal_config(db_path.clone(), false))
            .expect("journal store should open");

        let connection = Connection::open(db_path).expect("journal db should open");
        let migration_v1: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![1],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        let migration_v2: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![2],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        let migration_v3: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![3],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        let migration_v4: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![4],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        let migration_v5: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![5],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        let migration_v6: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![6],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        let migration_v7: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![7],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        let migration_v8: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![8],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        assert_eq!(migration_v1, 1, "migration v1 should be recorded exactly once");
        assert_eq!(migration_v2, 1, "migration v2 should be recorded exactly once");
        assert_eq!(migration_v3, 1, "migration v3 should be recorded exactly once");
        assert_eq!(migration_v4, 1, "migration v4 should be recorded exactly once");
        assert_eq!(migration_v5, 1, "migration v5 should be recorded exactly once");
        assert_eq!(migration_v6, 1, "migration v6 should be recorded exactly once");
        assert_eq!(migration_v7, 1, "migration v7 should be recorded exactly once");
        assert_eq!(migration_v8, 1, "migration v8 should be recorded exactly once");
    }

    #[test]
    fn append_redacts_sensitive_payload_fields() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .append(&build_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FB0",
                br#"{"token":"SECRET_TOKEN_VALUE","nested":{"password":"123456"},"safe":"ok"}"#,
            ))
            .expect("append should succeed");
        let records = store.recent(1).expect("recent journal query should succeed");
        assert_eq!(records.len(), 1);
        assert!(
            !records[0].payload_json.contains("SECRET_TOKEN_VALUE"),
            "raw secret token must not leak into journal payload"
        );
        assert!(!records[0].payload_json.contains("123456"), "password must be redacted");
        assert!(records[0].payload_json.contains("<redacted>"), "payload should contain marker");
        assert!(records[0].redacted, "record should flag that redaction occurred");
    }

    #[test]
    fn append_redacts_pin_token_without_overmatching_pin_substrings() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .append(&build_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FB9",
                br#"{"pin":"1234","pinpoint_id":"region-1"}"#,
            ))
            .expect("append should succeed");
        let records = store.recent(1).expect("recent journal query should succeed");
        assert_eq!(records.len(), 1);
        assert!(
            records[0].payload_json.contains("\"pin\":\"<redacted>\""),
            "explicit pin keys should remain redacted"
        );
        assert!(
            records[0].payload_json.contains("\"pinpoint_id\":\"region-1\""),
            "pin substring in benign key names must not trigger over-redaction"
        );
    }

    #[test]
    fn append_non_json_payload_is_stored_as_redacted_metadata() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB1", b"api_token=SECRET"))
            .expect("append should succeed");

        let records = store.recent(1).expect("recent journal query should succeed");
        assert!(records[0].redacted, "non-JSON payloads must be marked as redacted");
        assert!(
            records[0].payload_json.contains("non_json_payload"),
            "redacted metadata should explain why payload was transformed"
        );
    }

    #[test]
    fn append_rejects_payloads_over_configured_limit() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig {
            db_path,
            hash_chain_enabled: false,
            max_payload_bytes: 32,
        })
        .expect("journal store should open");

        let oversized_payload = vec![b'a'; 64];
        let error = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB7", oversized_payload.as_slice()))
            .expect_err("oversized journal payload must fail");
        assert!(matches!(
            error,
            JournalError::PayloadTooLarge {
                payload_kind,
                actual_bytes,
                max_bytes
            } if payload_kind == "journal" && actual_bytes == 64 && max_bytes == 32
        ));
    }

    #[test]
    fn append_duplicate_event_id_returns_deterministic_duplicate_error() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB6", br#"{"kind":"first"}"#))
            .expect("first append should succeed");
        let error = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB6", br#"{"kind":"duplicate"}"#))
            .expect_err("duplicate event ids must be rejected deterministically");
        assert!(matches!(
            error,
            JournalError::DuplicateEventId { ref event_id }
                if event_id == "01ARZ3NDEKTSV4RRFFQ69G5FB6"
        ));
    }

    #[test]
    fn hash_chain_links_events_when_enabled() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path.clone(), true))
            .expect("journal store should open");

        let first = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB2", br#"{"kind":"first"}"#))
            .expect("first append should succeed");
        let second = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB3", br#"{"kind":"second"}"#))
            .expect("second append should succeed");

        assert!(
            first.hash.is_some() && second.hash.is_some(),
            "hash chain mode should generate hashes"
        );
        assert_eq!(
            second.prev_hash, first.hash,
            "second event must link to first event hash in hash-chain mode"
        );
    }

    #[test]
    fn append_only_triggers_reject_update_and_delete() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path.clone(), false))
            .expect("journal store should open");

        store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB4", br#"{"kind":"immutable"}"#))
            .expect("append should succeed");
        let connection = Connection::open(db_path).expect("journal db should open");

        let update_error = connection
            .execute(
                "UPDATE journal_events SET actor = 99 WHERE event_ulid = ?1",
                params!["01ARZ3NDEKTSV4RRFFQ69G5FB4"],
            )
            .expect_err("updates must be rejected");
        assert!(
            update_error.to_string().contains("append-only"),
            "update errors should mention append-only invariant"
        );

        let delete_error = connection
            .execute(
                "DELETE FROM journal_events WHERE event_ulid = ?1",
                params!["01ARZ3NDEKTSV4RRFFQ69G5FB4"],
            )
            .expect_err("deletes must be rejected");
        assert!(
            delete_error.to_string().contains("append-only"),
            "delete errors should mention append-only invariant"
        );
    }

    #[test]
    fn recent_query_limit_is_clamped_to_prevent_unbounded_reads() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        for index in 0..3 {
            let event_id = format!("01ARZ3NDEKTSV4RRFFQ69G5FC{index}");
            let payload = format!(r#"{{"index":{index}}}"#);
            store
                .append(&build_request(event_id.as_str(), payload.as_bytes()))
                .expect("append should succeed");
        }

        let records = store.recent(0).expect("recent query should clamp low limit");
        assert_eq!(records.len(), 1, "limit=0 should be clamped to a single record");
    }

    #[test]
    fn orchestrator_run_status_snapshot_persists_usage_and_tape_count() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("orchestrator run should start");
        store
            .update_orchestrator_run_state(
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                RunLifecycleState::InProgress,
                None,
            )
            .expect("run should transition to in_progress");
        store
            .add_orchestrator_usage(&OrchestratorUsageDelta {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                prompt_tokens_delta: 3,
                completion_tokens_delta: 2,
            })
            .expect("usage counters should persist");
        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 0,
                event_type: "status".to_owned(),
                payload_json: r#"{"kind":"accepted"}"#.to_owned(),
            })
            .expect("first tape event should persist");
        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 1,
                event_type: "model_token".to_owned(),
                payload_json: r#"{"token":"alpha","is_final":false}"#.to_owned(),
            })
            .expect("second tape event should persist");
        store
            .update_orchestrator_run_state(
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                RunLifecycleState::Done,
                None,
            )
            .expect("run should transition to done");

        let snapshot = store
            .orchestrator_run_status_snapshot("01ARZ3NDEKTSV4RRFFQ69G5FAX")
            .expect("run snapshot query should succeed")
            .expect("snapshot should exist");
        assert_eq!(snapshot.state, "done");
        assert_eq!(snapshot.prompt_tokens, 3);
        assert_eq!(snapshot.completion_tokens, 2);
        assert_eq!(snapshot.total_tokens, 5);
        assert_eq!(snapshot.tape_events, 2);

        let tape = store
            .orchestrator_tape("01ARZ3NDEKTSV4RRFFQ69G5FAX")
            .expect("run tape query should succeed");
        assert_eq!(tape.len(), 2);
        assert_eq!(tape[0].seq, 0);
        assert_eq!(tape[1].event_type, "model_token");
        assert!(
            !tape[1].payload_json.contains("alpha"),
            "sensitive token values must not leak into persisted tape payloads"
        );
        assert!(
            tape[1].payload_json.contains("<redacted>"),
            "persisted tape payloads should preserve explicit redaction marker"
        );
    }

    #[test]
    fn orchestrator_tape_page_applies_after_seq_and_limit() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("run start should succeed");
        for seq in 0..5 {
            store
                .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                    run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                    seq,
                    event_type: "status".to_owned(),
                    payload_json: format!(r#"{{"seq":{seq}}}"#),
                })
                .expect("tape append should succeed");
        }

        let page = store
            .orchestrator_tape_page("01ARZ3NDEKTSV4RRFFQ69G5FAX", Some(1), 2)
            .expect("page query should succeed");
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].seq, 2);
        assert_eq!(page[1].seq, 3);
    }

    #[test]
    fn orchestrator_tape_append_rejects_payload_over_configured_limit() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig {
            db_path,
            hash_chain_enabled: false,
            max_payload_bytes: 24,
        })
        .expect("journal store should open");
        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("run start should succeed");

        let error = store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 0,
                event_type: "status".to_owned(),
                payload_json: r#"{"token":"secret-value-that-is-too-long"}"#.to_owned(),
            })
            .expect_err("oversized tape payload should fail");
        assert!(matches!(
            error,
            JournalError::PayloadTooLarge {
                payload_kind,
                actual_bytes,
                max_bytes
            } if payload_kind == "orchestrator_tape" && actual_bytes > max_bytes && max_bytes == 24
        ));
    }

    #[test]
    fn orchestrator_session_identity_is_immutable() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                session_key: "session:immutable".to_owned(),
                session_label: Some("Immutable session".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("initial session upsert should succeed");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("run start should succeed");

        let mismatch = store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                session_key: "session:immutable".to_owned(),
                session_label: Some("Immutable session".to_owned()),
                principal: "user:other".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
                channel: Some("web".to_owned()),
            })
            .expect_err("session identity mismatch should be rejected");
        assert!(matches!(
            mismatch,
            JournalError::SessionIdentityMismatch { ref session_id }
                if session_id == "01ARZ3NDEKTSV4RRFFQ69G5FAW"
        ));

        let snapshot = store
            .orchestrator_run_status_snapshot("01ARZ3NDEKTSV4RRFFQ69G5FAX")
            .expect("run snapshot query should succeed")
            .expect("run snapshot should exist");
        assert_eq!(snapshot.principal, "user:ops");
        assert_eq!(snapshot.device_id, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(snapshot.channel.as_deref(), Some("cli"));
    }

    #[test]
    fn orchestrator_rejects_duplicate_run_id_and_tape_sequence() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("first run start should succeed");
        let duplicate_run = store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect_err("duplicate run IDs must be rejected");
        assert!(matches!(
            duplicate_run,
            JournalError::DuplicateRunId { ref run_id }
                if run_id == "01ARZ3NDEKTSV4RRFFQ69G5FAX"
        ));

        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 7,
                event_type: "status".to_owned(),
                payload_json: r#"{"kind":"accepted"}"#.to_owned(),
            })
            .expect("first tape sequence should succeed");
        let duplicate_tape = store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 7,
                event_type: "status".to_owned(),
                payload_json: r#"{"kind":"accepted"}"#.to_owned(),
            })
            .expect_err("duplicate tape sequence should be rejected");
        assert!(matches!(
            duplicate_tape,
            JournalError::DuplicateTapeSequence { ref run_id, seq }
                if run_id == "01ARZ3NDEKTSV4RRFFQ69G5FAX" && seq == 7
        ));
    }

    #[test]
    fn orchestrator_cancel_flag_is_persisted() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("run start should succeed");
        assert!(
            !store
                .is_orchestrator_cancel_requested("01ARZ3NDEKTSV4RRFFQ69G5FAX")
                .expect("cancel status should query"),
            "cancel request should be false before cancellation"
        );
        store
            .request_orchestrator_cancel(&OrchestratorCancelRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                reason: "operator_requested".to_owned(),
            })
            .expect("cancel request should persist");
        assert!(
            store
                .is_orchestrator_cancel_requested("01ARZ3NDEKTSV4RRFFQ69G5FAX")
                .expect("cancel status should query"),
            "cancel request should persist"
        );
    }

    #[test]
    fn cron_job_crud_and_filters_roundtrip() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        let first = store
            .create_cron_job(&sample_cron_job_request("01ARZ3NDEKTSV4RRFFQ69G5FB0"))
            .expect("first cron job should be inserted");
        assert_eq!(first.name, "Hourly report");
        let mut second_request = sample_cron_job_request("01ARZ3NDEKTSV4RRFFQ69G5FB1");
        second_request.owner_principal = "user:finance".to_owned();
        second_request.channel = "slack:ops".to_owned();
        second_request.enabled = false;
        second_request.next_run_at_unix_ms = Some(1_730_000_090_000);
        store.create_cron_job(&second_request).expect("second cron job should be inserted");

        let loaded = store
            .cron_job("01ARZ3NDEKTSV4RRFFQ69G5FB0")
            .expect("cron job lookup should succeed")
            .expect("cron job must exist");
        assert_eq!(loaded.retry_policy.max_attempts, 3);
        assert_eq!(loaded.jitter_ms, 250);

        let due =
            store.list_due_cron_jobs(1_730_000_070_000, 10).expect("due jobs query should succeed");
        assert_eq!(due.len(), 1, "only enabled due job should be returned");
        assert_eq!(due[0].job_id, "01ARZ3NDEKTSV4RRFFQ69G5FB0");

        let filtered = store
            .list_cron_jobs(CronJobsListFilter {
                after_job_id: None,
                limit: 10,
                enabled: Some(false),
                owner_principal: Some("user:finance"),
                channel: Some("slack:ops"),
            })
            .expect("list cron jobs with filters should succeed");
        assert_eq!(filtered.len(), 1, "owner/channel/enabled filters should match one job");
        assert_eq!(filtered[0].job_id, "01ARZ3NDEKTSV4RRFFQ69G5FB1");
    }

    #[test]
    fn cron_run_start_finalize_and_active_lookup() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let job = store
            .create_cron_job(&sample_cron_job_request("01ARZ3NDEKTSV4RRFFQ69G5FBC"))
            .expect("cron job should be inserted");
        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");

        store
            .start_cron_run(&CronRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FBD".to_owned(),
                job_id: job.job_id.clone(),
                attempt: 1,
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()),
                orchestrator_run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned()),
                status: CronRunStatus::Running,
                error_kind: None,
                error_message_redacted: None,
            })
            .expect("cron run start should persist");

        let active = store
            .active_cron_run_for_job(job.job_id.as_str())
            .expect("active cron run query should succeed")
            .expect("active run should exist");
        assert_eq!(active.status, CronRunStatus::Running);

        store
            .finalize_cron_run(&CronRunFinalizeRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FBD".to_owned(),
                status: CronRunStatus::Succeeded,
                error_kind: None,
                error_message_redacted: None,
                model_tokens_in: 11,
                model_tokens_out: 7,
                tool_calls: 2,
                tool_denies: 1,
                orchestrator_run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned()),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()),
            })
            .expect("cron run finalize should persist");
        store
            .set_cron_job_next_run(
                job.job_id.as_str(),
                Some(1_730_000_120_000),
                Some(1_730_000_090_000),
            )
            .expect("next run metadata should update");

        let finalized = store
            .cron_run("01ARZ3NDEKTSV4RRFFQ69G5FBD")
            .expect("cron run lookup should succeed")
            .expect("cron run should exist");
        assert_eq!(finalized.status, CronRunStatus::Succeeded);
        assert_eq!(finalized.model_tokens_in, 11);
        assert_eq!(finalized.tool_denies, 1);

        let listed = store
            .list_cron_runs(CronRunsListFilter {
                job_id: Some(job.job_id.as_str()),
                after_run_id: None,
                limit: 5,
            })
            .expect("cron runs listing should succeed");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].run_id, "01ARZ3NDEKTSV4RRFFQ69G5FBD");
    }

    #[test]
    fn cron_run_duplicate_id_returns_deterministic_error() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let job = store
            .create_cron_job(&sample_cron_job_request("01ARZ3NDEKTSV4RRFFQ69G5FBE"))
            .expect("cron job should be inserted");

        store
            .start_cron_run(&CronRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FBF".to_owned(),
                job_id: job.job_id.clone(),
                attempt: 1,
                session_id: None,
                orchestrator_run_id: None,
                status: CronRunStatus::Accepted,
                error_kind: None,
                error_message_redacted: None,
            })
            .expect("initial cron run should persist");
        let duplicate = store
            .start_cron_run(&CronRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FBF".to_owned(),
                job_id: job.job_id.clone(),
                attempt: 2,
                session_id: None,
                orchestrator_run_id: None,
                status: CronRunStatus::Running,
                error_kind: None,
                error_message_redacted: None,
            })
            .expect_err("duplicate cron run ids must fail");
        assert!(matches!(
            duplicate,
            JournalError::DuplicateCronRunId { ref run_id }
                if run_id == "01ARZ3NDEKTSV4RRFFQ69G5FBF"
        ));
    }

    #[test]
    fn cron_run_error_message_is_sanitized_before_persist() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let job = store
            .create_cron_job(&sample_cron_job_request("01ARZ3NDEKTSV4RRFFQ69G5FBG"))
            .expect("cron job should be inserted");

        store
            .start_cron_run(&CronRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FBH".to_owned(),
                job_id: job.job_id.clone(),
                attempt: 1,
                session_id: None,
                orchestrator_run_id: None,
                status: CronRunStatus::Failed,
                error_kind: Some("scheduler_internal".to_owned()),
                error_message_redacted: Some(
                    "Bearer topsecret123 api_key=abc token=qwe secret=xyz".to_owned(),
                ),
            })
            .expect("cron run start should persist");

        let stored = store
            .cron_run("01ARZ3NDEKTSV4RRFFQ69G5FBH")
            .expect("cron run lookup should succeed")
            .expect("cron run should exist");
        let message = stored
            .error_message_redacted
            .expect("stored cron run should include redacted error message");
        assert!(!message.contains("topsecret123"), "bearer token value must be redacted");
        assert!(!message.contains("api_key=abc"), "api_key value must be redacted");
        assert!(!message.contains("token=qwe"), "token value must be redacted");
        assert!(!message.contains("secret=xyz"), "secret value must be redacted");
        assert!(message.contains("<redacted>"), "redaction marker should be present");
    }

    #[test]
    fn approval_records_can_be_created_resolved_and_queried() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let approval_id = "01ARZ3NDEKTSV4RRFFQ69G5FBJ";
        let created = store
            .create_approval(&sample_approval_request(approval_id))
            .expect("approval create should persist");
        assert_eq!(created.approval_id, approval_id);
        assert!(created.resolved_at_unix_ms.is_none(), "new approvals should be unresolved");
        assert!(created.decision.is_none(), "new approvals should have no decision");

        let resolved = store
            .resolve_approval(&ApprovalResolveRequest {
                approval_id: approval_id.to_owned(),
                decision: ApprovalDecision::Deny,
                decision_scope: ApprovalDecisionScope::Once,
                decision_reason: "deny token=abc cookie:sessionid=abc123".to_owned(),
                decision_scope_ttl_ms: None,
            })
            .expect("approval resolve should persist");
        assert_eq!(resolved.decision, Some(ApprovalDecision::Deny));
        assert_eq!(resolved.decision_scope, Some(ApprovalDecisionScope::Once));
        assert!(
            !resolved.decision_reason.as_deref().unwrap_or_default().contains("token=abc"),
            "resolved decision reason should redact token values"
        );
        assert!(
            !resolved.decision_reason.as_deref().unwrap_or_default().contains("sessionid=abc123"),
            "resolved decision reason should redact cookie values"
        );
        assert!(
            resolved.decision_reason.as_deref().unwrap_or_default().contains("<redacted>"),
            "resolved decision reason should include redaction marker"
        );
        assert!(
            resolved.resolved_at_unix_ms.is_some(),
            "resolved approvals should include resolved timestamp"
        );

        let listed = store
            .list_approvals(ApprovalsListFilter {
                after_approval_id: None,
                limit: 10,
                since_unix_ms: None,
                until_unix_ms: None,
                subject_id: Some("tool:palyra.process.run"),
                principal: Some("user:ops"),
                decision: Some(ApprovalDecision::Deny),
                subject_type: Some(ApprovalSubjectType::Tool),
            })
            .expect("approvals list should succeed");
        assert_eq!(listed.len(), 1, "filters should return the matching approval record");
        assert_eq!(listed[0].approval_id, approval_id);

        let fetched = store
            .approval(approval_id)
            .expect("approval lookup should succeed")
            .expect("approval should exist");
        assert_eq!(fetched.decision, Some(ApprovalDecision::Deny));
    }

    #[test]
    fn approval_request_summaries_and_prompt_details_are_redacted() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let approval_id = "01ARZ3NDEKTSV4RRFFQ69G5FBK";
        let stored = store
            .create_approval(&sample_approval_request(approval_id))
            .expect("approval create should persist");
        assert!(
            !stored.request_summary.contains("super-secret"),
            "request summary should redact refresh token values"
        );
        assert!(
            stored.request_summary.contains("<redacted>"),
            "request summary should include redaction marker"
        );
        assert!(
            !stored.prompt.details_json.contains("sessionid=abc123"),
            "prompt details should redact cookie values"
        );
        assert!(
            stored.prompt.details_json.contains("<redacted>"),
            "prompt details should include redaction marker"
        );
    }

    #[test]
    fn skill_status_upsert_and_latest_lookup_track_quarantine_state() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        let active = store
            .upsert_skill_status(&SkillStatusUpsertRequest {
                skill_id: "acme.echo_http".to_owned(),
                version: "1.0.0".to_owned(),
                status: SkillExecutionStatus::Active,
                reason: Some("initial audit pass".to_owned()),
                detected_at_ms: 1_730_000_000_100,
                operator_principal: "user:ops".to_owned(),
            })
            .expect("active status should persist");
        assert_eq!(active.status, SkillExecutionStatus::Active);

        let quarantined = store
            .upsert_skill_status(&SkillStatusUpsertRequest {
                skill_id: "acme.echo_http".to_owned(),
                version: "1.0.0".to_owned(),
                status: SkillExecutionStatus::Quarantined,
                reason: Some("operator quarantine after audit".to_owned()),
                detected_at_ms: 1_730_000_000_200,
                operator_principal: "user:security".to_owned(),
            })
            .expect("quarantine status should update existing row");
        assert_eq!(quarantined.status, SkillExecutionStatus::Quarantined);
        assert_eq!(
            quarantined.operator_principal, "user:security",
            "operator principal should reflect latest update"
        );

        let loaded = store
            .skill_status("acme.echo_http", "1.0.0")
            .expect("skill status lookup should succeed")
            .expect("skill status should exist");
        assert_eq!(loaded.status, SkillExecutionStatus::Quarantined);
        let loaded_case_variant = store
            .skill_status("Acme.Echo_Http", "1.0.0")
            .expect("case-variant skill status lookup should succeed")
            .expect("case-variant skill status should resolve");
        assert_eq!(
            loaded_case_variant.status,
            SkillExecutionStatus::Quarantined,
            "skill status lookup should be case-insensitive for skill_id"
        );

        store
            .upsert_skill_status(&SkillStatusUpsertRequest {
                skill_id: "acme.echo_http".to_owned(),
                version: "1.1.0".to_owned(),
                status: SkillExecutionStatus::Active,
                reason: Some("newer version enabled".to_owned()),
                detected_at_ms: 1_730_000_000_300,
                operator_principal: "user:ops".to_owned(),
            })
            .expect("newer version status should persist");
        let latest = store
            .latest_skill_status("acme.echo_http")
            .expect("latest skill status lookup should succeed")
            .expect("latest skill status should exist");
        assert_eq!(latest.version, "1.1.0");
        assert_eq!(latest.status, SkillExecutionStatus::Active);
        let latest_case_variant = store
            .latest_skill_status("Acme.Echo_Http")
            .expect("case-variant latest lookup should succeed")
            .expect("case-variant latest skill status should exist");
        assert_eq!(
            latest_case_variant.version, "1.1.0",
            "latest lookup should be case-insensitive for skill_id"
        );
    }

    #[test]
    fn canvas_state_transitions_persist_and_replay_deterministically() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let canvas_id = "01ARZ3NDEKTSV4RRFFQ69G5FC9";

        let first = sample_canvas_transition_request(
            canvas_id,
            1,
            0,
            r#"{"counter":1,"items":[]}"#,
            r#"{"v":1,"ops":[{"op":"replace","path":"","value":{"counter":1,"items":[]}}]}"#,
            false,
        );
        store
            .record_canvas_state_transition(&first)
            .expect("initial canvas transition should persist");

        let second = sample_canvas_transition_request(
            canvas_id,
            2,
            1,
            r#"{"counter":2,"items":["a"]}"#,
            r#"{"v":1,"ops":[{"op":"replace","path":"/counter","value":2},{"op":"add","path":"/items/0","value":"a"}]}"#,
            false,
        );
        store
            .record_canvas_state_transition(&second)
            .expect("second canvas transition should persist");

        let third = sample_canvas_transition_request(
            canvas_id,
            3,
            2,
            r#"{"counter":2,"items":["a"]}"#,
            r#"{"v":1,"ops":[{"op":"replace","path":"","value":{"counter":2,"items":["a"]}}]}"#,
            true,
        );
        let latest =
            store.record_canvas_state_transition(&third).expect("close transition should persist");
        assert_eq!(latest.state_version, 3);
        assert!(latest.closed, "latest snapshot should reflect closed flag");

        let replayed = store
            .replay_canvas_state(canvas_id)
            .expect("replay should succeed")
            .expect("replay must return a final state");
        assert_eq!(replayed.state_version, 3);
        assert_eq!(replayed.state_schema_version, 1);
        assert_eq!(replayed.state_json, r#"{"counter":2,"items":["a"]}"#);
        assert!(replayed.closed, "replayed state should preserve closed flag");
        assert_eq!(replayed.patches_applied, 3);

        let patches = store
            .list_canvas_state_patches(canvas_id, 1, 10)
            .expect("patch listing should succeed");
        assert_eq!(patches.len(), 2, "after state version filter should apply");
        assert_eq!(patches[0].state_version, 2);
        assert_eq!(patches[1].state_version, 3);
    }

    #[test]
    fn canvas_state_transition_rejects_duplicate_state_version() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let canvas_id = "01ARZ3NDEKTSV4RRFFQ69G5FD0";
        let request = sample_canvas_transition_request(
            canvas_id,
            1,
            0,
            r#"{"status":"ok"}"#,
            r#"{"v":1,"ops":[{"op":"replace","path":"","value":{"status":"ok"}}]}"#,
            false,
        );
        store.record_canvas_state_transition(&request).expect("first transition should succeed");
        let duplicate = store
            .record_canvas_state_transition(&request)
            .expect_err("duplicate state version should fail");
        assert!(matches!(
            duplicate,
            JournalError::DuplicateCanvasStateVersion {
                ref canvas_id,
                state_version
            } if canvas_id == "01ARZ3NDEKTSV4RRFFQ69G5FD0" && state_version == 1
        ));
    }

    #[test]
    fn canvas_state_transition_rejects_oversized_payload() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig {
            db_path,
            hash_chain_enabled: false,
            max_payload_bytes: 96,
        })
        .expect("journal store should open");
        let oversized = format!("\"{}\"", "a".repeat(256));
        let request = sample_canvas_transition_request(
            "01ARZ3NDEKTSV4RRFFQ69G5FD1",
            1,
            0,
            oversized.as_str(),
            r#"{"v":1,"ops":[{"op":"replace","path":"","value":"ok"}]}"#,
            false,
        );
        let error = store
            .record_canvas_state_transition(&request)
            .expect_err("oversized state payload should fail");
        assert!(matches!(
            error,
            JournalError::PayloadTooLarge {
                payload_kind,
                actual_bytes: _,
                max_bytes: 96
            } if payload_kind == "canvas_state"
        ));
    }

    #[test]
    fn memory_ingest_and_search_returns_expected_hits() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAR";
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC1",
                "user:ops",
                Some("cli"),
                Some(session_id),
                MemorySource::TapeUserMessage,
                "deploy checklist for release candidate validation",
            ))
            .expect("memory item should be created");
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC2",
                "user:ops",
                Some("cli"),
                Some(session_id),
                MemorySource::Summary,
                "release summary and rollback note",
            ))
            .expect("memory item should be created");

        let hits = store
            .search_memory(&MemorySearchRequest {
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some(session_id.to_owned()),
                query: "deploy checklist".to_owned(),
                top_k: 5,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("memory search should succeed");
        assert!(
            hits.iter().any(|hit| hit.item.memory_id == "01ARZ3NDEKTSV4RRFFQ69G5FC1"),
            "search should return the matching memory item"
        );
    }

    #[test]
    fn memory_store_supports_custom_embedding_provider_plug() {
        let db_path = temp_db_path();
        let provider = Arc::new(FixedMemoryEmbeddingProvider {
            model_name: "test-embedding-v1",
            dimensions: 4,
            vector: vec![0.25, 0.5, 0.75, 1.0],
        });
        let store = JournalStore::open_with_memory_embedding_provider(
            test_journal_config(db_path, false),
            provider,
        )
        .expect("journal store should open with custom embedding provider");

        let memory_id = "01ARZ3NDEKTSV4RRFFQ69G5FC0";
        store
            .create_memory_item(&sample_memory_request(
                memory_id,
                "user:ops",
                Some("cli"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FAT"),
                MemorySource::Manual,
                "custom embedding provider roundtrip",
            ))
            .expect("memory item should be created");

        let guard = store.connection.lock().expect("connection lock should not be poisoned");
        let (model_name, dims): (String, i64) = guard
            .query_row(
                "SELECT embedding_model, dims FROM memory_vectors WHERE memory_ulid = ?1",
                params![memory_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("memory vector metadata should be persisted");
        drop(guard);

        assert_eq!(model_name, "test-embedding-v1");
        assert_eq!(dims, 4);
    }

    #[test]
    fn build_fts_query_sanitizes_reserved_characters_and_operators() {
        let query = r#""Deploy" OR release* NEAR(checklist,rollback) NOT path:/tmp"#;
        let fts_query = build_fts_query(query);
        assert_eq!(fts_query, "deploy or release near checklist rollback not path tmp");
        assert!(
            fts_query
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == ' '),
            "FTS query should only include normalized term characters"
        );
    }

    #[test]
    fn memory_search_handles_operator_like_query_tokens_without_sql_errors() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FBQ";
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FCF",
                "user:ops",
                Some("cli"),
                Some(session_id),
                MemorySource::Manual,
                "deploy rollback checklist for release candidate",
            ))
            .expect("memory item should be created");

        let operator_like_hits = store
            .search_memory(&MemorySearchRequest {
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some(session_id.to_owned()),
                query: r#""deploy" OR release* NEAR(checklist,rollback) NOT"#.to_owned(),
                top_k: 5,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("operator-like query should not fail search execution");
        assert!(operator_like_hits.len() <= 5, "search results should remain bounded by top_k");

        let symbol_only_hits = store
            .search_memory(&MemorySearchRequest {
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some(session_id.to_owned()),
                query: r#""***""#.to_owned(),
                top_k: 5,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("symbol-only query should degrade to empty result instead of failing");
        assert!(
            symbol_only_hits.is_empty(),
            "symbol-only query should not produce invalid FTS queries or false-positive hits"
        );
    }

    #[test]
    fn memory_scope_isolation_enforces_principal_and_channel_boundaries() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC3",
                "user:a",
                Some("cli"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FAS"),
                MemorySource::Manual,
                "alpha private memory",
            ))
            .expect("memory item should be created");
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC4",
                "user:b",
                Some("cli"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FAS"),
                MemorySource::Manual,
                "beta private memory",
            ))
            .expect("memory item should be created");
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC5",
                "user:a",
                Some("slack"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FAS"),
                MemorySource::Manual,
                "alpha slack-only memory",
            ))
            .expect("memory item should be created");

        let principal_hits = store
            .search_memory(&MemorySearchRequest {
                principal: "user:a".to_owned(),
                channel: None,
                session_id: None,
                query: "private memory".to_owned(),
                top_k: 10,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("principal search should succeed");
        assert!(
            principal_hits.iter().all(|hit| hit.item.principal == "user:a"),
            "search must not cross principal boundaries"
        );

        let channel_hits = store
            .search_memory(&MemorySearchRequest {
                principal: "user:a".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: None,
                query: "memory".to_owned(),
                top_k: 10,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("channel search should succeed");
        assert!(
            channel_hits.iter().all(|hit| hit.item.channel.as_deref() == Some("cli")),
            "channel-scoped search must not return memories from a different channel"
        );
    }

    #[test]
    fn memory_ranking_prefers_exact_matches_over_weak_matches() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAT";

        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC6",
                "user:ops",
                Some("cli"),
                Some(session_id),
                MemorySource::Summary,
                "database migration rollback strategy for tenant upgrade",
            ))
            .expect("exact-match memory should be created");
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC7",
                "user:ops",
                Some("cli"),
                Some(session_id),
                MemorySource::Summary,
                "database notes and maintenance checklist",
            ))
            .expect("weak-match memory should be created");

        let hits = store
            .search_memory(&MemorySearchRequest {
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some(session_id.to_owned()),
                query: "database migration rollback strategy".to_owned(),
                top_k: 5,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("memory search should succeed");
        assert!(!hits.is_empty(), "search should return ranked memory hits");
        assert_eq!(
            hits[0].item.memory_id, "01ARZ3NDEKTSV4RRFFQ69G5FC6",
            "exact phrase match should rank ahead of weaker matches"
        );
    }

    #[test]
    fn memory_redaction_removes_secrets_from_store_and_results() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAU";

        let stored = store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC8",
                "user:ops",
                Some("cli"),
                Some(session_id),
                MemorySource::Manual,
                "api_key=secret123 cookie:sessionid=abc123 release note",
            ))
            .expect("memory item should be created");
        assert!(
            !stored.content_text.contains("secret123"),
            "ingested memory should redact secret values before persistence"
        );
        assert!(
            !stored.content_text.contains("sessionid=abc123"),
            "ingested memory should redact cookie values before persistence"
        );
        assert!(
            stored.content_text.contains("<redacted>"),
            "ingested memory should include redaction marker"
        );

        let fetched = store
            .memory_item("01ARZ3NDEKTSV4RRFFQ69G5FC8")
            .expect("memory lookup should succeed")
            .expect("memory item should exist");
        assert!(
            !fetched.content_text.contains("secret123"),
            "memory item lookup must not leak raw secret values"
        );

        let listed = store
            .list_memory_items(&MemoryItemsListFilter {
                after_memory_id: None,
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some(session_id.to_owned()),
                limit: 10,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("memory listing should succeed");
        assert_eq!(listed.len(), 1, "list should return the stored redacted memory item");
        assert!(
            !listed[0].content_text.contains("secret123"),
            "list results must not leak raw secret values"
        );
        assert!(
            !listed[0].content_text.contains("sessionid=abc123"),
            "list results must not leak raw cookie values"
        );
    }

    #[test]
    fn memory_purge_forgets_session_items() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FC9",
                "user:ops",
                Some("cli"),
                Some(session_id),
                MemorySource::Manual,
                "session-specific memory to purge",
            ))
            .expect("memory item should be created");
        let deleted = store
            .purge_memory(&MemoryPurgeRequest {
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some(session_id.to_owned()),
                purge_all_principal: false,
            })
            .expect("purge should succeed");
        assert_eq!(deleted, 1, "session purge should delete matching session memories");
    }

    #[test]
    fn memory_purge_all_principal_respects_channel_scope() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FDA",
                "user:ops",
                Some("cli"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FAA"),
                MemorySource::Manual,
                "cli-only memory item",
            ))
            .expect("cli memory should be created");
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FDB",
                "user:ops",
                Some("slack"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FAB"),
                MemorySource::Manual,
                "slack-only memory item",
            ))
            .expect("slack memory should be created");

        let deleted = store
            .purge_memory(&MemoryPurgeRequest {
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: None,
                purge_all_principal: true,
            })
            .expect("channel-scoped purge-all should succeed");
        assert_eq!(deleted, 1, "purge-all should not widen past channel scope");

        let remaining_slack = store
            .list_memory_items(&MemoryItemsListFilter {
                after_memory_id: None,
                principal: "user:ops".to_owned(),
                channel: Some("slack".to_owned()),
                session_id: None,
                limit: 10,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .expect("slack listing should succeed");
        assert_eq!(
            remaining_slack.len(),
            1,
            "channel-scoped purge-all must preserve unrelated channel memories"
        );
    }

    #[test]
    fn memory_lookup_hides_expired_items() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        let ttl_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_millis() as i64;
        let mut request = sample_memory_request(
            "01ARZ3NDEKTSV4RRFFQ69G5FDC",
            "user:ops",
            Some("cli"),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAC"),
            MemorySource::Manual,
            "short-lived memory",
        );
        request.ttl_unix_ms = Some(ttl_now.saturating_add(120));
        store.create_memory_item(&request).expect("memory item should be created");

        std::thread::sleep(Duration::from_millis(220));
        let fetched =
            store.memory_item("01ARZ3NDEKTSV4RRFFQ69G5FDC").expect("memory lookup should succeed");
        assert!(fetched.is_none(), "memory lookup should not return items after ttl expiration");
    }

    #[test]
    fn memory_delete_respects_channel_scope_filter() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FDD",
                "user:ops",
                Some("slack"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FAD"),
                MemorySource::Manual,
                "channel-filtered delete target",
            ))
            .expect("memory item should be created");

        let denied_delete = store
            .delete_memory_item("01ARZ3NDEKTSV4RRFFQ69G5FDD", "user:ops", Some("cli"))
            .expect("cross-channel delete should complete without storage error");
        assert!(
            !denied_delete,
            "channel-constrained delete should not remove memory from another channel"
        );

        let allowed_delete = store
            .delete_memory_item("01ARZ3NDEKTSV4RRFFQ69G5FDD", "user:ops", Some("slack"))
            .expect("same-channel delete should complete without storage error");
        assert!(allowed_delete, "same-channel delete should remove matching memory");
    }

    #[test]
    fn memory_delete_with_channel_filter_does_not_widen_to_channel_agnostic_items() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FDE",
                "user:ops",
                None,
                Some("01ARZ3NDEKTSV4RRFFQ69G5FAE"),
                MemorySource::Manual,
                "channel-agnostic memory",
            ))
            .expect("memory item should be created");

        let deleted = store
            .delete_memory_item("01ARZ3NDEKTSV4RRFFQ69G5FDE", "user:ops", Some("cli"))
            .expect("channel-filtered delete should complete without storage error");
        assert!(!deleted, "channel-filtered delete must not remove channel-agnostic memory");

        let remaining =
            store.memory_item("01ARZ3NDEKTSV4RRFFQ69G5FDE").expect("memory lookup should succeed");
        assert!(
            remaining.is_some(),
            "channel-agnostic memory should remain after mismatched channel-filtered delete"
        );
    }

    #[test]
    fn memory_maintenance_reduces_synthetic_store_size_deterministically() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FB0";
        for index in 0..6 {
            let memory_id = format!("01ARZ3NDEKTSV4RRFFQ69G5F{:02}", index + 50);
            store
                .create_memory_item(&sample_memory_request(
                    memory_id.as_str(),
                    "user:ops",
                    Some("cli"),
                    Some(session_id),
                    MemorySource::Manual,
                    format!("synthetic memory row {index} for deterministic retention trimming")
                        .as_str(),
                ))
                .expect("memory item should be created");
        }
        let now = current_unix_ms().expect("system clock should be available");
        let outcome = store
            .run_memory_maintenance(&MemoryMaintenanceRequest {
                now_unix_ms: now,
                retention: MemoryRetentionPolicy {
                    max_entries: Some(3),
                    max_bytes: None,
                    ttl_days: None,
                },
                next_vacuum_due_at_unix_ms: None,
                next_maintenance_run_at_unix_ms: Some(now.saturating_add(300_000)),
            })
            .expect("maintenance should succeed");
        assert_eq!(outcome.entries_before, 6, "synthetic test should start with six memory rows");
        assert_eq!(outcome.entries_after, 3, "entry cap should deterministically trim rows");
        assert!(
            outcome.approx_bytes_after < outcome.approx_bytes_before,
            "maintenance should reduce approximate memory footprint"
        );
        assert_eq!(
            outcome.deleted_capacity_count, 3,
            "capacity pass should evict oldest three rows"
        );
        assert_eq!(
            outcome.deleted_expired_count, 0,
            "ttl eviction should not run without ttl policy"
        );
    }

    #[test]
    fn memory_maintenance_ttl_days_eviction_updates_status_snapshot() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        let memory_id = "01ARZ3NDEKTSV4RRFFQ69G5FE1";
        store
            .create_memory_item(&sample_memory_request(
                memory_id,
                "user:ops",
                Some("cli"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FB1"),
                MemorySource::Manual,
                "stale memory item for ttl-days retention",
            ))
            .expect("memory item should be created");
        let now = current_unix_ms().expect("system clock should be available");
        let stale_created_at = now.saturating_sub(14_i64.saturating_mul(MEMORY_RETENTION_DAY_MS));
        {
            let guard = store.connection.lock().expect("connection lock should not be poisoned");
            guard
                .execute(
                    "UPDATE memory_items SET created_at_unix_ms = ?1, updated_at_unix_ms = ?1, ttl_unix_ms = NULL WHERE memory_ulid = ?2",
                    params![stale_created_at, memory_id],
                )
                .expect("test fixture update should succeed");
        }

        let outcome = store
            .run_memory_maintenance(&MemoryMaintenanceRequest {
                now_unix_ms: now,
                retention: MemoryRetentionPolicy {
                    max_entries: None,
                    max_bytes: None,
                    ttl_days: Some(7),
                },
                next_vacuum_due_at_unix_ms: None,
                next_maintenance_run_at_unix_ms: Some(now.saturating_add(300_000)),
            })
            .expect("maintenance should succeed");
        assert_eq!(outcome.deleted_expired_count, 1, "ttl-days policy should remove stale row");
        assert_eq!(outcome.entries_after, 0, "stale row should be evicted");

        let status = store.memory_maintenance_status().expect("status snapshot should load");
        let last_run = status.last_run.expect("maintenance status should include last run");
        assert_eq!(last_run.deleted_expired_count, 1);
        assert_eq!(status.usage.entries, 0, "usage snapshot should reflect ttl eviction");
        assert_eq!(
            status.next_maintenance_run_at_unix_ms,
            Some(now.saturating_add(300_000)),
            "status should expose next scheduled maintenance check"
        );
    }

    #[test]
    fn memory_maintenance_vacuum_runs_only_when_due() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");
        store
            .create_memory_item(&sample_memory_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FE2",
                "user:ops",
                Some("cli"),
                Some("01ARZ3NDEKTSV4RRFFQ69G5FB2"),
                MemorySource::Manual,
                "vacuum cadence probe row",
            ))
            .expect("memory item should be created");
        let now = current_unix_ms().expect("system clock should be available");

        let first = store
            .run_memory_maintenance(&MemoryMaintenanceRequest {
                now_unix_ms: now,
                retention: MemoryRetentionPolicy {
                    max_entries: None,
                    max_bytes: None,
                    ttl_days: None,
                },
                next_vacuum_due_at_unix_ms: Some(now.saturating_add(60_000)),
                next_maintenance_run_at_unix_ms: None,
            })
            .expect("maintenance should succeed");
        assert!(!first.vacuum_performed, "vacuum should stay disabled before due timestamp");

        let second = store
            .run_memory_maintenance(&MemoryMaintenanceRequest {
                now_unix_ms: now.saturating_add(1_000),
                retention: MemoryRetentionPolicy {
                    max_entries: None,
                    max_bytes: None,
                    ttl_days: None,
                },
                next_vacuum_due_at_unix_ms: Some(now.saturating_add(1_000)),
                next_maintenance_run_at_unix_ms: None,
            })
            .expect("maintenance should succeed");
        assert!(second.vacuum_performed, "vacuum should run exactly when due");
        assert_eq!(
            second.last_vacuum_at_unix_ms,
            Some(now.saturating_add(1_000)),
            "maintenance outcome should record the last vacuum timestamp"
        );
    }

    #[test]
    fn write_duration_is_reported_for_observability() {
        let db_path = temp_db_path();
        let store = JournalStore::open(test_journal_config(db_path, false))
            .expect("journal store should open");

        let outcome = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB5", br#"{"status":"ok"}"#))
            .expect("append should succeed");
        assert!(
            outcome.write_duration < Duration::from_secs(1),
            "local sqlite append should complete in bounded time"
        );
    }
}
