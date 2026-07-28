//! Typed cross-session read, message, and generation-control contracts.
//! Opaque ownership tokens authorize descendants; no contract exposes owner
//! principals, raw transcripts, credentials, or unrestricted session lookup.

use serde::{Deserialize, Serialize};

/// Schema version for model-visible session operation contracts.
pub const SESSION_OPERATIONS_SCHEMA_VERSION: u32 = 1;

/// Bounded delegation budget projected without exposing policy internals.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionBudgetV2 {
    pub tokens: u64,
    pub attempts: u64,
    pub max_attempts: u64,
}

/// Last durable child progress attached to a session summary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionProgressV2 {
    pub task_id: String,
    pub state: String,
    pub revision: u64,
    pub updated_at_unix_ms: i64,
}

/// Policy-scoped model projection of one current or descendant session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSummaryV2 {
    pub schema_version: u32,
    pub session_id: String,
    pub relation: String,
    pub parent_session_id: Option<String>,
    pub origin_run_id: Option<String>,
    pub state: String,
    pub generation: Option<u64>,
    pub budget: Option<SessionBudgetV2>,
    pub last_progress: Option<SessionProgressV2>,
    pub ownership_token: Option<String>,
    pub title: String,
    pub preview: Option<String>,
    pub last_run_id: Option<String>,
    pub updated_at_unix_ms: i64,
}

/// Idempotent, generation-aware message queued for an authorized child.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionMessageRequest {
    pub request_id: String,
    pub target_session_id: String,
    pub ownership_token: String,
    pub message: String,
    #[serde(default)]
    pub expected_generation: Option<u64>,
}

/// Stable model-visible outcomes for session message delivery admission.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionMessageOutcomeKind {
    Queued,
    Delivered,
    Rejected,
    TargetBusy,
}

/// Auditable result of a model-visible session message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionMessageOutcome {
    pub schema_version: u32,
    pub command_id: String,
    pub request_id: String,
    pub outcome: SessionMessageOutcomeKind,
    pub reason_code: String,
    pub target_session_id: String,
    pub target_run_id: String,
    pub target_generation: u64,
    pub queued_input_id: Option<String>,
}

/// Generation-aware guidance for an authorized descendant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSteerRequest {
    pub request_id: String,
    pub target_session_id: String,
    pub ownership_token: String,
    pub instruction: String,
    #[serde(default)]
    pub expected_generation: Option<u64>,
}

/// Terminal cancellation request for an authorized descendant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInterruptRequest {
    pub request_id: String,
    pub target_session_id: String,
    pub ownership_token: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub expected_generation: Option<u64>,
}

/// Session-scoped model-route replacement for an authorized descendant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LiveModelSwitchRequest {
    pub request_id: String,
    pub target_session_id: String,
    pub ownership_token: String,
    pub model_profile: String,
    #[serde(default)]
    pub instruction: Option<String>,
    #[serde(default)]
    pub expected_generation: Option<u64>,
}

/// Durable coalescing result for steer and live-model-switch commands.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupersedeOutcome {
    pub schema_version: u32,
    pub command_id: String,
    pub superseded_command_id: Option<String>,
    pub target_run_id: String,
    pub observed_generation: u64,
    pub replacement_generation: Option<u64>,
    pub state: String,
    pub reason_code: String,
}
