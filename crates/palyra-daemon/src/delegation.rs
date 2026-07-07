//! Pure delegation contract logic: profiles, templates, scopes, and records.
//!
//! Resolves operator delegation requests against the built-in catalog and
//! the parent run's context, enforcing scope containment (child tool and
//! skill allowlists must be subsets of the parent's), budget and limit
//! ceilings, and serial-mode concurrency clamps. Also builds the redacted
//! [`DelegatedRunRecord`] snapshots that journal and console surfaces
//! persist. No I/O happens here; callers in the tool runtime and gateway
//! own persistence and execution.

use palyra_common::redaction::{redact_auth_error, redact_url_segments_in_text};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;

pub(crate) mod child_session;

const DELEGATED_RUN_SCHEMA_VERSION: u32 = 1;
const DEFAULT_MAX_ATTEMPTS: u64 = 3;
const DEFAULT_MAX_CONCURRENT_CHILDREN: u64 = 2;
const DEFAULT_MAX_CHILDREN_PER_PARENT: u64 = 8;
const DEFAULT_MAX_TOTAL_CHILDREN: u64 = 16;
const DEFAULT_MAX_PARALLEL_GROUPS: u64 = 2;
const DEFAULT_MAX_DEPTH: u64 = 3;
const DEFAULT_MAX_BUDGET_SHARE_BPS: u64 = 10_000;
const DEFAULT_CHILD_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
const MAX_DELEGATION_BUDGET_TOKENS: u64 = 32_768;
const MAX_DELEGATION_ATTEMPTS: u64 = 16;
const MAX_DELEGATION_CONCURRENT_CHILDREN: u64 = 16;
const MAX_DELEGATION_CHILDREN_PER_PARENT: u64 = 64;
const MAX_DELEGATION_TOTAL_CHILDREN: u64 = 256;
const MAX_DELEGATION_PARALLEL_GROUPS: u64 = 16;
const MAX_DELEGATION_DEPTH: u64 = 8;
const MAX_DELEGATION_BUDGET_SHARE_BPS: u64 = 10_000;
const MAX_DELEGATION_CHILD_TIMEOUT_MS: u64 = 6 * 60 * 60 * 1_000;

const fn default_max_total_children() -> u64 {
    DEFAULT_MAX_TOTAL_CHILDREN
}

const fn default_max_depth() -> u64 {
    DEFAULT_MAX_DEPTH
}

const fn default_max_budget_share_bps() -> u64 {
    DEFAULT_MAX_BUDGET_SHARE_BPS
}

/// Functional role a delegated child run plays for its parent.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationRole {
    Research,
    Synthesis,
    Review,
    Patching,
    Triage,
}

/// Whether sibling children run one at a time or concurrently; serial mode
/// clamps the runtime concurrency limits to one.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationExecutionMode {
    Serial,
    Parallel,
}

/// Which memory a delegated child may read, from nothing up to the parent
/// session plus the workspace.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationMemoryScopeKind {
    None,
    ParentSession,
    ParentSessionAndWorkspace,
    WorkspaceOnly,
}

/// How child outputs are folded back into the parent run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationMergeStrategy {
    Summarize,
    Compare,
    PatchReview,
    Triage,
}

/// Merge strategy plus whether the merge needs operator approval.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeContract {
    pub strategy: DelegationMergeStrategy,
    pub approval_required: bool,
}

/// Concurrency, fan-out, depth, budget-share, and timeout ceilings for one
/// delegation; validated against the hard `MAX_DELEGATION_*` caps before a
/// snapshot is produced.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationRuntimeLimits {
    pub max_concurrent_children: u64,
    pub max_children_per_parent: u64,
    #[serde(default = "default_max_total_children")]
    pub max_total_children: u64,
    pub max_parallel_groups: u64,
    #[serde(default = "default_max_depth")]
    pub max_depth: u64,
    #[serde(default = "default_max_budget_share_bps")]
    pub max_budget_share_bps: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_budget_override: Option<u64>,
    pub child_timeout_ms: u64,
}

impl Default for DelegationRuntimeLimits {
    fn default() -> Self {
        Self {
            max_concurrent_children: DEFAULT_MAX_CONCURRENT_CHILDREN,
            max_children_per_parent: DEFAULT_MAX_CHILDREN_PER_PARENT,
            max_total_children: DEFAULT_MAX_TOTAL_CHILDREN,
            max_parallel_groups: DEFAULT_MAX_PARALLEL_GROUPS,
            max_depth: DEFAULT_MAX_DEPTH,
            max_budget_share_bps: DEFAULT_MAX_BUDGET_SHARE_BPS,
            child_budget_override: None,
            child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
        }
    }
}

/// Fully resolved, validated delegation contract for one child run; the
/// output of [`resolve_delegation_request`] after catalog, template,
/// manifest, and parent-context constraints are applied.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationSnapshot {
    pub profile_id: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    pub role: DelegationRole,
    pub execution_mode: DelegationExecutionMode,
    pub group_id: String,
    pub model_profile: String,
    pub tool_allowlist: Vec<String>,
    pub skill_allowlist: Vec<String>,
    pub memory_scope: DelegationMemoryScopeKind,
    pub budget_tokens: u64,
    pub max_attempts: u64,
    pub merge_contract: DelegationMergeContract,
    #[serde(default)]
    pub runtime_limits: DelegationRuntimeLimits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

/// Lifecycle state of a delegated child run as observed by the parent.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedRunState {
    Queued,
    Accepted,
    Running,
    WaitingForSlot,
    WaitingForApproval,
    MergePreview,
    Merged,
    Failed,
    Cancelled,
    TimedOut,
    Rejected,
}

impl DelegatedRunState {
    /// Returns the canonical snake_case state name used in journal records.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::WaitingForSlot => "waiting_for_slot",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::MergePreview => "merge_preview",
            Self::Merged => "merged",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Rejected => "rejected",
        }
    }

    /// Maps a raw child runtime state string onto the delegated-run state
    /// machine.
    ///
    /// Unknown strings map to `Failed` (fail-closed) so a child reporting a
    /// state this build does not understand never looks healthy.
    #[must_use]
    pub fn from_child_state(value: &str) -> Self {
        match value {
            "queued" => Self::Queued,
            "accepted" => Self::Accepted,
            "running" | "in_progress" | "model_streaming" | "tool_proposed" | "tool_decided"
            | "tool_completed" | "tool_attested" => Self::Running,
            "waiting" | "waiting_for_slot" => Self::WaitingForSlot,
            "waiting_for_approval" | "approval_resolved" => Self::WaitingForApproval,
            "merge_preview" => Self::MergePreview,
            "done" | "completed" | "merged" => Self::Merged,
            "cancelled" | "canceled" => Self::Cancelled,
            "timed_out" | "timeout" => Self::TimedOut,
            "rejected" => Self::Rejected,
            _ => Self::Failed,
        }
    }
}

/// Progress of folding a child's output back into the parent run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationMergeStatus {
    NotReady,
    PreviewReady,
    ApprovalRequired,
    Approved,
    Merged,
    Rejected,
    Failed,
}

impl DelegationMergeStatus {
    /// Returns the canonical snake_case status name used in journal records.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotReady => "not_ready",
            Self::PreviewReady => "preview_ready",
            Self::ApprovalRequired => "approval_required",
            Self::Approved => "approved",
            Self::Merged => "merged",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
        }
    }
}

/// Reference handed to a child run (context, memory, or artifact) with the
/// reason it was shared and its sensitivity label.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegatedRunReference {
    pub ref_id: String,
    pub reason: String,
    pub sensitivity: String,
}

impl DelegatedRunReference {
    /// Returns a JSON projection with every field passed through the shared
    /// redaction transforms, safe to journal or display.
    #[must_use]
    pub fn safe_snapshot_json(&self) -> Value {
        json!({
            "ref_id": safe_text(self.ref_id.as_str()),
            "reason": safe_text(self.reason.as_str()),
            "sensitivity": safe_text(self.sensitivity.as_str()),
        })
    }
}

/// Everything a child run is allowed to touch: allowlists, memory scope,
/// shared references, and the generated child prompt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegatedRunScope {
    pub tool_allowlist: Vec<String>,
    pub skill_allowlist: Vec<String>,
    pub memory_scope: DelegationMemoryScopeKind,
    pub context_refs: Vec<DelegatedRunReference>,
    pub memory_refs: Vec<DelegatedRunReference>,
    pub artifact_refs: Vec<DelegatedRunReference>,
    pub child_prompt: String,
}

impl DelegatedRunScope {
    /// Returns a JSON projection with free-text fields redacted, safe to
    /// journal or display.
    #[must_use]
    pub fn safe_snapshot_json(&self) -> Value {
        json!({
            "tool_allowlist": self.tool_allowlist,
            "skill_allowlist": self.skill_allowlist,
            "memory_scope": self.memory_scope,
            "context_refs": self.context_refs.iter().map(DelegatedRunReference::safe_snapshot_json).collect::<Vec<_>>(),
            "memory_refs": self.memory_refs.iter().map(DelegatedRunReference::safe_snapshot_json).collect::<Vec<_>>(),
            "artifact_refs": self.artifact_refs.iter().map(DelegatedRunReference::safe_snapshot_json).collect::<Vec<_>>(),
            "child_prompt": safe_text(self.child_prompt.as_str()),
        })
    }
}

/// Flattened budget and limit ceilings recorded on a delegated run; derived
/// from the snapshot's budget plus its runtime limits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegatedRunBudgets {
    pub budget_tokens: u64,
    pub max_attempts: u64,
    pub max_concurrent_children: u64,
    pub max_children_per_parent: u64,
    pub max_total_children: u64,
    pub max_parallel_groups: u64,
    pub max_depth: u64,
    pub max_budget_share_bps: u64,
    pub max_wall_clock_ms: u64,
}

impl From<&DelegationSnapshot> for DelegatedRunBudgets {
    fn from(value: &DelegationSnapshot) -> Self {
        Self {
            budget_tokens: value.budget_tokens,
            max_attempts: value.max_attempts,
            max_concurrent_children: value.runtime_limits.max_concurrent_children,
            max_children_per_parent: value.runtime_limits.max_children_per_parent,
            max_total_children: value.runtime_limits.max_total_children,
            max_parallel_groups: value.runtime_limits.max_parallel_groups,
            max_depth: value.runtime_limits.max_depth,
            max_budget_share_bps: value.runtime_limits.max_budget_share_bps,
            max_wall_clock_ms: value.runtime_limits.child_timeout_ms,
        }
    }
}

/// One observed state transition on a delegated run's lifecycle tape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegatedRunLifecycleEvent {
    pub event_type: String,
    pub state: String,
    pub reason: String,
    pub observed_at_unix_ms: i64,
}

/// Persisted record of one delegated child run: identity, redacted scope,
/// budgets, state, merge status, and lifecycle events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegatedRunRecord {
    pub schema_version: u32,
    pub delegated_run_id: String,
    pub parent_run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub objective: String,
    pub scope: DelegatedRunScope,
    pub budgets: DelegatedRunBudgets,
    pub state: DelegatedRunState,
    pub merge_status: DelegationMergeStatus,
    pub delegation: DelegationSnapshot,
    pub lifecycle_events: Vec<DelegatedRunLifecycleEvent>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

impl DelegatedRunRecord {
    /// Returns the full record as JSON with free-text fields redacted, safe
    /// to journal or display.
    #[must_use]
    pub fn safe_snapshot_json(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "delegated_run_id": self.delegated_run_id,
            "parent_run_id": self.parent_run_id,
            "child_run_id": self.child_run_id,
            "task_id": self.task_id,
            "objective": safe_text(self.objective.as_str()),
            "scope": self.scope.safe_snapshot_json(),
            "budgets": self.budgets,
            "state": self.state.as_str(),
            "merge_status": self.merge_status.as_str(),
            "delegation": self.delegation,
            "lifecycle_events": self.lifecycle_events,
            "created_at_unix_ms": self.created_at_unix_ms,
            "updated_at_unix_ms": self.updated_at_unix_ms,
        })
    }

    /// Returns the compact "why does this child exist" view (reason, scope,
    /// limits, state) used by explain surfaces.
    #[must_use]
    pub fn explain_json(&self) -> Value {
        json!({
            "delegated_run_id": self.delegated_run_id,
            "reason": safe_text(self.objective.as_str()),
            "scope": {
                "memory_scope": self.scope.memory_scope,
                "tool_allowlist": self.scope.tool_allowlist,
                "skill_allowlist": self.scope.skill_allowlist,
                "context_refs": self.scope.context_refs.iter().map(DelegatedRunReference::safe_snapshot_json).collect::<Vec<_>>(),
                "memory_refs": self.scope.memory_refs.iter().map(DelegatedRunReference::safe_snapshot_json).collect::<Vec<_>>(),
                "artifact_refs": self.scope.artifact_refs.iter().map(DelegatedRunReference::safe_snapshot_json).collect::<Vec<_>>(),
            },
            "limits": self.budgets,
            "state": self.state.as_str(),
            "merge_status": self.merge_status.as_str(),
        })
    }
}

/// All delegated children of one parent run with active/terminal counts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegatedRunGraphSnapshot {
    pub parent_run_id: String,
    pub children: Vec<DelegatedRunRecord>,
    pub active_child_count: u64,
    pub terminal_child_count: u64,
}

/// Availability of the durable child transcript reference attached to a
/// subagent record.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubagentTranscriptStatus {
    Pending,
    Available,
    Stale,
}

/// Health of the task-to-child-run link and its repairability.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubagentLinkStatus {
    Healthy,
    Pending,
    Stale,
}

/// Redacted reference to a scope, memory, or artifact item shared with a
/// subagent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubagentReferenceRecord {
    pub ref_id: String,
    pub reason: String,
    pub sensitivity: String,
}

impl From<&DelegatedRunReference> for SubagentReferenceRecord {
    fn from(value: &DelegatedRunReference) -> Self {
        Self {
            ref_id: safe_text(value.ref_id.as_str()),
            reason: safe_text(value.reason.as_str()),
            sensitivity: safe_text(value.sensitivity.as_str()),
        }
    }
}

/// Redacted scope projection for a persistent subagent record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubagentScopeRecord {
    pub memory_scope: DelegationMemoryScopeKind,
    pub tool_allowlist: Vec<String>,
    pub skill_allowlist: Vec<String>,
    pub context_refs: Vec<SubagentReferenceRecord>,
    pub memory_refs: Vec<SubagentReferenceRecord>,
    pub artifact_refs: Vec<SubagentReferenceRecord>,
}

impl From<&DelegatedRunScope> for SubagentScopeRecord {
    fn from(value: &DelegatedRunScope) -> Self {
        Self {
            memory_scope: value.memory_scope,
            tool_allowlist: value.tool_allowlist.clone(),
            skill_allowlist: value.skill_allowlist.clone(),
            context_refs: value.context_refs.iter().map(SubagentReferenceRecord::from).collect(),
            memory_refs: value.memory_refs.iter().map(SubagentReferenceRecord::from).collect(),
            artifact_refs: value.artifact_refs.iter().map(SubagentReferenceRecord::from).collect(),
        }
    }
}

/// Stable pointer to the child run tape that contains the subagent transcript.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubagentTranscriptRef {
    pub kind: String,
    pub status: SubagentTranscriptStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub session_id: String,
}

/// One operator-reviewable action for repairing a stale task-to-run link.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubagentLinkRepairAction {
    pub action: String,
    pub target: String,
    pub reason: String,
    pub automatic_apply: bool,
    pub policy_gate: String,
}

/// Non-mutating repair plan for the link between a subagent task and child run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubagentLinkRepairPlan {
    pub status: SubagentLinkStatus,
    pub reason: String,
    pub automatic_apply: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<SubagentLinkRepairAction>,
}

/// Persistent, redacted operator view of one delegated child run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubagentSessionRecord {
    pub schema_version: u32,
    pub record_id: String,
    pub task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_run_id: Option<String>,
    pub child_session_id: String,
    pub role: DelegationRole,
    pub scope: SubagentScopeRecord,
    pub budget: DelegatedRunBudgets,
    pub status: String,
    pub transcript_ref: SubagentTranscriptRef,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<Value>,
    pub verification_state: String,
    pub stale_link_repair: SubagentLinkRepairPlan,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Parameters for [`build_subagent_session_record`].
#[derive(Debug, Clone, PartialEq)]
pub struct SubagentSessionRecordBuildRequest {
    pub task_id: String,
    pub parent_run_id: Option<String>,
    pub child_run_id: Option<String>,
    pub child_session_id: String,
    pub scope: DelegatedRunScope,
    pub delegation: DelegationSnapshot,
    pub status: String,
    pub child_run_exists: bool,
    pub task_terminal: bool,
    pub artifacts: Vec<Value>,
    pub evidence_refs: Vec<Value>,
    pub verification_state: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Builds the redacted persistent subagent record exposed by session snapshot,
/// CLI, and trajectory export surfaces.
#[must_use]
pub fn build_subagent_session_record(
    request: SubagentSessionRecordBuildRequest,
) -> SubagentSessionRecord {
    let transcript_status = subagent_transcript_status(
        request.child_run_id.as_deref(),
        request.child_run_exists,
        request.task_terminal,
    );
    let stale_link_repair = build_subagent_link_repair_plan(
        request.task_id.as_str(),
        request.child_run_id.as_deref(),
        transcript_status,
    );
    let record_id = request
        .parent_run_id
        .as_deref()
        .map(|parent_run_id| {
            stable_delegated_run_id(
                parent_run_id,
                request.child_run_id.as_deref(),
                Some(request.task_id.as_str()),
            )
        })
        .unwrap_or_else(|| format!("subagent_{}", request.task_id));

    SubagentSessionRecord {
        schema_version: 1,
        record_id,
        task_id: request.task_id,
        parent_run_id: request.parent_run_id,
        child_run_id: request.child_run_id.clone(),
        child_session_id: request.child_session_id.clone(),
        role: request.delegation.role,
        scope: SubagentScopeRecord::from(&request.scope),
        budget: DelegatedRunBudgets::from(&request.delegation),
        status: safe_text(request.status.as_str()),
        transcript_ref: SubagentTranscriptRef {
            kind: "orchestrator_run_tape".to_owned(),
            status: transcript_status,
            run_id: request.child_run_id,
            session_id: request.child_session_id,
        },
        artifacts: request.artifacts,
        evidence_refs: request.evidence_refs,
        verification_state: safe_text(request.verification_state.as_str()),
        stale_link_repair,
        created_at_unix_ms: request.created_at_unix_ms,
        updated_at_unix_ms: request.updated_at_unix_ms,
    }
}

const fn subagent_transcript_status(
    child_run_id: Option<&str>,
    child_run_exists: bool,
    task_terminal: bool,
) -> SubagentTranscriptStatus {
    match (child_run_id.is_some(), child_run_exists, task_terminal) {
        (true, true, _) => SubagentTranscriptStatus::Available,
        (true, false, true) => SubagentTranscriptStatus::Stale,
        _ => SubagentTranscriptStatus::Pending,
    }
}

fn build_subagent_link_repair_plan(
    task_id: &str,
    child_run_id: Option<&str>,
    transcript_status: SubagentTranscriptStatus,
) -> SubagentLinkRepairPlan {
    match (child_run_id, transcript_status) {
        (Some(_), SubagentTranscriptStatus::Available) => SubagentLinkRepairPlan {
            status: SubagentLinkStatus::Healthy,
            reason: "child run tape is available".to_owned(),
            automatic_apply: false,
            actions: Vec::new(),
        },
        (Some(child_run_id), SubagentTranscriptStatus::Stale) => SubagentLinkRepairPlan {
            status: SubagentLinkStatus::Stale,
            reason: "subagent task points at a child run tape that is no longer present".to_owned(),
            automatic_apply: false,
            actions: vec![
                SubagentLinkRepairAction {
                    action: "inspect_child_run".to_owned(),
                    target: format!("run:{child_run_id}"),
                    reason: "confirm whether the run tape was pruned or never committed".to_owned(),
                    automatic_apply: false,
                    policy_gate: "subagent.link.repair".to_owned(),
                },
                SubagentLinkRepairAction {
                    action: "retry_or_clear_task_target".to_owned(),
                    target: format!("task:{task_id}"),
                    reason:
                        "retry the delegated task or clear the stale target after operator review"
                            .to_owned(),
                    automatic_apply: false,
                    policy_gate: "subagent.link.repair".to_owned(),
                },
            ],
        },
        _ => SubagentLinkRepairPlan {
            status: SubagentLinkStatus::Pending,
            reason: "child run tape has not been attached yet".to_owned(),
            automatic_apply: false,
            actions: Vec::new(),
        },
    }
}

impl DelegatedRunGraphSnapshot {
    /// Returns the graph with each child rendered through
    /// [`DelegatedRunRecord::explain_json`].
    #[must_use]
    pub fn explain_json(&self) -> Value {
        json!({
            "parent_run_id": self.parent_run_id,
            "active_child_count": self.active_child_count,
            "terminal_child_count": self.terminal_child_count,
            "children": self.children.iter().map(DelegatedRunRecord::explain_json).collect::<Vec<_>>(),
        })
    }
}

/// Assembles the child-run graph for `parent_run_id`, counting children in
/// terminal states (merged/failed/cancelled/timed-out/rejected) as done and
/// everything else as active.
#[must_use]
pub fn build_delegated_run_graph(
    parent_run_id: String,
    children: Vec<DelegatedRunRecord>,
) -> DelegatedRunGraphSnapshot {
    let terminal_child_count =
        children.iter().filter(|child| delegated_run_state_terminal(child.state)).count();
    DelegatedRunGraphSnapshot {
        parent_run_id,
        active_child_count: children.len().saturating_sub(terminal_child_count) as u64,
        terminal_child_count: terminal_child_count as u64,
        children,
    }
}

const fn delegated_run_state_terminal(state: DelegatedRunState) -> bool {
    matches!(
        state,
        DelegatedRunState::Merged
            | DelegatedRunState::Failed
            | DelegatedRunState::Cancelled
            | DelegatedRunState::TimedOut
            | DelegatedRunState::Rejected
    )
}

/// Parameters for [`build_delegated_run_record`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegatedRunRecordBuildRequest {
    pub parent_run_id: String,
    pub child_run_id: Option<String>,
    pub task_id: Option<String>,
    pub objective: String,
    pub scope: DelegatedRunScope,
    pub delegation: DelegationSnapshot,
    pub state: DelegatedRunState,
    pub merge_status: DelegationMergeStatus,
    pub event_type: String,
    pub event_reason: String,
    pub observed_at_unix_ms: i64,
}

/// Builds the initial persisted record for a delegated run, redacting
/// free-text fields and seeding the lifecycle tape with the first event.
///
/// The run id is derived deterministically from the parent run and the
/// child run (or task) id, so re-building for the same pair yields the
/// same record identity.
#[must_use]
pub fn build_delegated_run_record(request: DelegatedRunRecordBuildRequest) -> DelegatedRunRecord {
    let delegated_run_id = stable_delegated_run_id(
        request.parent_run_id.as_str(),
        request.child_run_id.as_deref(),
        request.task_id.as_deref(),
    );
    DelegatedRunRecord {
        schema_version: DELEGATED_RUN_SCHEMA_VERSION,
        delegated_run_id,
        parent_run_id: request.parent_run_id,
        child_run_id: request.child_run_id,
        task_id: request.task_id,
        objective: safe_text(request.objective.as_str()),
        scope: request.scope,
        budgets: DelegatedRunBudgets::from(&request.delegation),
        state: request.state,
        merge_status: request.merge_status,
        delegation: request.delegation,
        lifecycle_events: vec![DelegatedRunLifecycleEvent {
            event_type: request.event_type,
            state: request.state.as_str().to_owned(),
            reason: safe_text(request.event_reason.as_str()),
            observed_at_unix_ms: request.observed_at_unix_ms,
        }],
        created_at_unix_ms: request.observed_at_unix_ms,
        updated_at_unix_ms: request.observed_at_unix_ms,
    }
}

/// Unvalidated reference input destined for a child scope; normalized and
/// redacted by [`build_delegated_scope`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegatedReferenceInput {
    pub ref_id: String,
    pub reason: String,
    pub sensitivity: String,
}

/// Parameters for [`build_delegated_scope`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegatedScopeBuildRequest {
    pub objective: String,
    pub delegation: DelegationSnapshot,
    pub parent_tool_allowlist: Vec<String>,
    pub parent_skill_allowlist: Vec<String>,
    pub context_refs: Vec<DelegatedReferenceInput>,
    pub memory_refs: Vec<DelegatedReferenceInput>,
    pub artifact_refs: Vec<DelegatedReferenceInput>,
}

/// Builds the validated, redacted scope handed to a child run.
///
/// Re-checks scope containment against the parent allowlists even though
/// [`resolve_delegation_request`] already did, so a snapshot mutated
/// between resolution and spawn cannot escalate.
///
/// # Errors
/// Returns `Status::invalid_argument` for an empty objective, empty
/// reference fields, or allowlist entries outside a non-empty parent
/// allowlist.
pub fn build_delegated_scope(
    request: DelegatedScopeBuildRequest,
) -> Result<DelegatedRunScope, Status> {
    let tool_allowlist = normalize_allowlist(request.delegation.tool_allowlist.clone());
    validate_allowlist_subset(
        "delegation.scope.tool_allowlist",
        tool_allowlist.as_slice(),
        request.parent_tool_allowlist.as_slice(),
    )?;
    let skill_allowlist = normalize_allowlist(request.delegation.skill_allowlist.clone());
    validate_allowlist_subset(
        "delegation.scope.skill_allowlist",
        skill_allowlist.as_slice(),
        request.parent_skill_allowlist.as_slice(),
    )?;
    let objective = normalize_optional_text(Some(request.objective.as_str()))
        .ok_or_else(|| Status::invalid_argument("delegation objective cannot be empty"))?;

    Ok(DelegatedRunScope {
        tool_allowlist,
        skill_allowlist,
        memory_scope: request.delegation.memory_scope,
        context_refs: normalize_refs("delegation.scope.context_refs", request.context_refs)?,
        memory_refs: normalize_refs("delegation.scope.memory_refs", request.memory_refs)?,
        artifact_refs: normalize_refs("delegation.scope.artifact_refs", request.artifact_refs)?,
        child_prompt: build_child_prompt(
            objective.as_str(),
            request.delegation.profile_id.as_str(),
            request.delegation.memory_scope,
        ),
    })
}

/// Validates, redacts, sorts, and dedups reference inputs; the stable order
/// keeps record snapshots deterministic for replay comparison.
fn normalize_refs(
    field: &str,
    values: Vec<DelegatedReferenceInput>,
) -> Result<Vec<DelegatedRunReference>, Status> {
    let mut output = Vec::new();
    for value in values {
        let ref_id = normalize_optional_text(Some(value.ref_id.as_str()))
            .ok_or_else(|| Status::invalid_argument(format!("{field}.ref_id cannot be empty")))?;
        let reason = normalize_optional_text(Some(value.reason.as_str()))
            .ok_or_else(|| Status::invalid_argument(format!("{field}.reason cannot be empty")))?;
        let sensitivity =
            normalize_optional_text(Some(value.sensitivity.as_str())).ok_or_else(|| {
                Status::invalid_argument(format!("{field}.sensitivity cannot be empty"))
            })?;
        output.push(DelegatedRunReference {
            ref_id: safe_text(ref_id.as_str()),
            reason: safe_text(reason.as_str()),
            sensitivity: safe_text(sensitivity.as_str()),
        });
    }
    output.sort_by(|left, right| {
        left.ref_id
            .cmp(&right.ref_id)
            .then_with(|| left.reason.cmp(&right.reason))
            .then_with(|| left.sensitivity.cmp(&right.sensitivity))
    });
    output.dedup_by(|left, right| {
        left.ref_id == right.ref_id
            && left.reason == right.reason
            && left.sensitivity == right.sensitivity
    });
    Ok(output)
}

/// Renders the fixed containment preamble handed to every child run; the
/// wording is asserted by scope tests, so change both together.
fn build_child_prompt(
    objective: &str,
    profile_id: &str,
    memory_scope: DelegationMemoryScopeKind,
) -> String {
    format!(
        "Delegated objective: {objective}\nProfile: {profile_id}\nMemory scope: {:?}\nDo not request broader tools, memory, artifacts, credentials, or context than the delegated scope permits. Return a concise summary with blockers, evidence refs, and next action.",
        memory_scope
    )
}

fn stable_delegated_run_id(
    parent_run_id: &str,
    child_run_id: Option<&str>,
    task_id: Option<&str>,
) -> String {
    // Deterministic over (parent, child-or-task) so the same delegation
    // always resolves to one record id; "pending" covers runs recorded
    // before a child id exists.
    format!("dr_{}_{}", parent_run_id, child_run_id.or(task_id).unwrap_or("pending"))
}

/// Applies the shared auth and URL redaction transforms to free text before
/// it is persisted or displayed.
fn safe_text(value: &str) -> String {
    redact_url_segments_in_text(&redact_auth_error(value))
}

/// One piece of evidence backing a merge result (message, tool call, or
/// artifact excerpt) attributed to the child run that produced it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeProvenanceRecord {
    pub child_run_id: String,
    pub kind: String,
    pub label: String,
    pub excerpt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub requires_approval: bool,
}

/// Coarse classification of why a delegated merge failed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationMergeFailureCategory {
    Model,
    Tool,
    Approval,
    Budget,
    Cancellation,
    Transport,
    Unknown,
}

/// Aggregated token usage and timing of the child runs behind one merge.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeUsageSummary {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
}

/// Approval posture of one merge: whether approval is required and how the
/// observed approval events resolved.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeApprovalSummary {
    pub approval_required: bool,
    pub approval_events: u64,
    pub approval_pending: bool,
    pub approval_denied: bool,
}

/// Artifact produced by a child run and surfaced with the merge result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeArtifactReference {
    pub artifact_id: String,
    pub artifact_kind: String,
    pub label: String,
}

/// Condensed view of one tool invocation a child run performed, kept for
/// merge review.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationToolTraceSummary {
    pub child_run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposal_id: Option<String>,
    pub tool_name: String,
    pub status: String,
    pub excerpt: String,
    pub requires_approval: bool,
}

/// Outcome of merging child outputs: summary text, warnings, approval and
/// usage posture, and the provenance trail backing the result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeResult {
    pub status: String,
    pub strategy: DelegationMergeStrategy,
    pub summary_text: String,
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_category: Option<DelegationMergeFailureCategory>,
    pub approval_required: bool,
    #[serde(default)]
    pub approval_summary: DelegationMergeApprovalSummary,
    #[serde(default)]
    pub usage_summary: DelegationMergeUsageSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_references: Vec<DelegationMergeArtifactReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_trace_summary: Vec<DelegationToolTraceSummary>,
    pub provenance: Vec<DelegationMergeProvenanceRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merged_at_unix_ms: Option<i64>,
}

/// Catalog entry describing a reusable delegation profile (role, scope,
/// budgets, merge contract) that requests resolve against.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationProfileDefinition {
    pub profile_id: String,
    pub display_name: String,
    pub description: String,
    pub role: DelegationRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile: Option<String>,
    pub tool_allowlist: Vec<String>,
    pub skill_allowlist: Vec<String>,
    pub memory_scope: DelegationMemoryScopeKind,
    pub budget_tokens: u64,
    pub max_attempts: u64,
    pub execution_mode: DelegationExecutionMode,
    pub merge_contract: DelegationMergeContract,
    #[serde(default)]
    pub runtime_limits: DelegationRuntimeLimits,
}

/// Catalog entry bundling a primary profile with execution mode, merge
/// strategy, and optional limit overrides for a named workflow pattern.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationTemplateDefinition {
    pub template_id: String,
    pub display_name: String,
    pub description: String,
    pub primary_profile_id: String,
    pub recommended_profiles: Vec<String>,
    pub execution_mode: DelegationExecutionMode,
    pub merge_strategy: DelegationMergeStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_limits: Option<DelegationRuntimeLimits>,
    pub examples: Vec<String>,
}

/// The set of delegation profiles and templates available to operators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationCatalog {
    pub profiles: Vec<DelegationProfileDefinition>,
    pub templates: Vec<DelegationTemplateDefinition>,
}

/// Raw operator delegation request: at most one of `profile_id` or
/// `template_id`, plus optional execution-mode, group, and manifest
/// overrides. Validated by [`resolve_delegation_request`].
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct DelegationRequestInput {
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub template_id: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub execution_mode: Option<DelegationExecutionMode>,
    #[serde(default)]
    pub manifest: Option<DelegationManifestInput>,
}

/// Per-request manifest overriding individual fields of the resolved
/// profile; every override is re-validated against the hard caps.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct DelegationManifestInput {
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub role: Option<DelegationRole>,
    #[serde(default)]
    pub model_profile: Option<String>,
    #[serde(default)]
    pub tool_allowlist: Vec<String>,
    #[serde(default)]
    pub skill_allowlist: Vec<String>,
    #[serde(default)]
    pub memory_scope: Option<DelegationMemoryScopeKind>,
    #[serde(default)]
    pub budget_tokens: Option<u64>,
    #[serde(default)]
    pub max_attempts: Option<u64>,
    #[serde(default)]
    pub merge_strategy: Option<DelegationMergeStrategy>,
    #[serde(default)]
    pub approval_required: Option<bool>,
    #[serde(default)]
    pub max_concurrent_children: Option<u64>,
    #[serde(default)]
    pub max_children_per_parent: Option<u64>,
    #[serde(default)]
    pub max_total_children: Option<u64>,
    #[serde(default)]
    pub max_parallel_groups: Option<u64>,
    #[serde(default)]
    pub max_depth: Option<u64>,
    #[serde(default)]
    pub max_budget_share_bps: Option<u64>,
    #[serde(default)]
    pub child_budget_override: Option<u64>,
    #[serde(default)]
    pub child_timeout_ms: Option<u64>,
}

/// The parent run's identity, allowlists, and budget ceiling; every child
/// scope and budget is validated to stay within this envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationParentContext {
    pub parent_run_id: Option<String>,
    pub agent_id: Option<String>,
    pub parent_model_profile: Option<String>,
    pub parent_tool_allowlist: Vec<String>,
    pub parent_skill_allowlist: Vec<String>,
    pub parent_budget_tokens: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
fn profile_definition(
    profile_id: &str,
    display_name: &str,
    description: &str,
    role: DelegationRole,
    tool_allowlist: &[&str],
    memory_scope: DelegationMemoryScopeKind,
    budget_tokens: u64,
    execution_mode: DelegationExecutionMode,
    merge_strategy: DelegationMergeStrategy,
    approval_required: bool,
) -> DelegationProfileDefinition {
    DelegationProfileDefinition {
        profile_id: profile_id.to_owned(),
        display_name: display_name.to_owned(),
        description: description.to_owned(),
        role,
        model_profile: None,
        tool_allowlist: normalize_allowlist(
            tool_allowlist.iter().map(ToString::to_string).collect(),
        ),
        skill_allowlist: Vec::new(),
        memory_scope,
        budget_tokens,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        execution_mode,
        merge_contract: DelegationMergeContract { strategy: merge_strategy, approval_required },
        runtime_limits: default_runtime_limits_for_execution_mode(execution_mode),
    }
}

fn default_runtime_limits_for_execution_mode(
    execution_mode: DelegationExecutionMode,
) -> DelegationRuntimeLimits {
    let mut limits = DelegationRuntimeLimits::default();
    if execution_mode == DelegationExecutionMode::Serial {
        limits.max_concurrent_children = 1;
        limits.max_parallel_groups = 1;
    }
    limits
}

/// Returns the built-in profile and template catalog.
///
/// This is the only catalog source today; ids referenced by templates must
/// resolve against the profiles defined here.
pub fn built_in_delegation_catalog() -> DelegationCatalog {
    let profiles = vec![
        profile_definition(
            "research",
            "Research",
            "Collect evidence and return a concise summary with source-aware recommendations.",
            DelegationRole::Research,
            &["palyra.http.fetch"],
            DelegationMemoryScopeKind::ParentSessionAndWorkspace,
            1_800,
            DelegationExecutionMode::Parallel,
            DelegationMergeStrategy::Summarize,
            false,
        ),
        profile_definition(
            "synthesis",
            "Synthesis",
            "Condense existing findings into a decision-ready answer for the parent run.",
            DelegationRole::Synthesis,
            &[],
            DelegationMemoryScopeKind::ParentSession,
            1_600,
            DelegationExecutionMode::Serial,
            DelegationMergeStrategy::Summarize,
            false,
        ),
        profile_definition(
            "review",
            "Review",
            "Inspect evidence or code paths and highlight risks, regressions, and tradeoffs.",
            DelegationRole::Review,
            &["palyra.http.fetch"],
            DelegationMemoryScopeKind::ParentSessionAndWorkspace,
            2_200,
            DelegationExecutionMode::Parallel,
            DelegationMergeStrategy::Triage,
            false,
        ),
        profile_definition(
            "patching",
            "Patching",
            "Prepare patch-oriented output with strong provenance and approval-aware merge rules.",
            DelegationRole::Patching,
            &["palyra.fs.apply_patch", "palyra.http.fetch"],
            DelegationMemoryScopeKind::ParentSessionAndWorkspace,
            2_600,
            DelegationExecutionMode::Serial,
            DelegationMergeStrategy::PatchReview,
            true,
        ),
        profile_definition(
            "triage",
            "Triage",
            "Summarize multiple signals into a prioritized issue list for parent review.",
            DelegationRole::Triage,
            &["palyra.http.fetch"],
            DelegationMemoryScopeKind::ParentSession,
            1_400,
            DelegationExecutionMode::Parallel,
            DelegationMergeStrategy::Triage,
            false,
        ),
    ];
    let templates = vec![
        DelegationTemplateDefinition {
            template_id: "compare_variants".to_owned(),
            display_name: "Compare Variants".to_owned(),
            description: "Run parallel variant checks and merge the output into a side-by-side comparison."
                .to_owned(),
            primary_profile_id: "research".to_owned(),
            recommended_profiles: vec!["research".to_owned(), "synthesis".to_owned()],
            execution_mode: DelegationExecutionMode::Parallel,
            merge_strategy: DelegationMergeStrategy::Compare,
            runtime_limits: Some(DelegationRuntimeLimits {
                max_concurrent_children: 2,
                max_children_per_parent: 8,
                max_total_children: DEFAULT_MAX_TOTAL_CHILDREN,
                max_parallel_groups: 2,
                max_depth: DEFAULT_MAX_DEPTH,
                max_budget_share_bps: DEFAULT_MAX_BUDGET_SHARE_BPS,
                child_budget_override: None,
                child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
            }),
            examples: vec![
                "/delegate compare_variants Compare the current branch with the release branch for migration risk."
                    .to_owned(),
            ],
        },
        DelegationTemplateDefinition {
            template_id: "research_then_synthesize".to_owned(),
            display_name: "Research Then Synthesize".to_owned(),
            description: "Collect evidence first, then hand it off for a condensed answer.".to_owned(),
            primary_profile_id: "research".to_owned(),
            recommended_profiles: vec!["research".to_owned(), "synthesis".to_owned()],
            execution_mode: DelegationExecutionMode::Serial,
            merge_strategy: DelegationMergeStrategy::Summarize,
            runtime_limits: Some(DelegationRuntimeLimits {
                max_concurrent_children: 1,
                max_children_per_parent: 6,
                max_total_children: DEFAULT_MAX_TOTAL_CHILDREN,
                max_parallel_groups: 1,
                max_depth: DEFAULT_MAX_DEPTH,
                max_budget_share_bps: DEFAULT_MAX_BUDGET_SHARE_BPS,
                child_budget_override: None,
                child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
            }),
            examples: vec![
                "/delegate research_then_synthesize Read the recent daemon changes and summarize deployment impact."
                    .to_owned(),
            ],
        },
        DelegationTemplateDefinition {
            template_id: "review_and_patch".to_owned(),
            display_name: "Review And Patch".to_owned(),
            description: "Audit a change first and keep patch-oriented output approval-aware.".to_owned(),
            primary_profile_id: "patching".to_owned(),
            recommended_profiles: vec!["review".to_owned(), "patching".to_owned()],
            execution_mode: DelegationExecutionMode::Serial,
            merge_strategy: DelegationMergeStrategy::PatchReview,
            runtime_limits: Some(DelegationRuntimeLimits {
                max_concurrent_children: 1,
                max_children_per_parent: 4,
                max_total_children: DEFAULT_MAX_TOTAL_CHILDREN,
                max_parallel_groups: 1,
                max_depth: DEFAULT_MAX_DEPTH,
                max_budget_share_bps: DEFAULT_MAX_BUDGET_SHARE_BPS,
                child_budget_override: None,
                child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
            }),
            examples: vec![
                "/delegate review_and_patch Investigate the failing web lint output and propose the minimal fix."
                    .to_owned(),
            ],
        },
        DelegationTemplateDefinition {
            template_id: "multi_source_triage".to_owned(),
            display_name: "Multi-Source Triage".to_owned(),
            description: "Blend transcript, references, and external evidence into a prioritized triage summary."
                .to_owned(),
            primary_profile_id: "triage".to_owned(),
            recommended_profiles: vec!["triage".to_owned(), "research".to_owned()],
            execution_mode: DelegationExecutionMode::Parallel,
            merge_strategy: DelegationMergeStrategy::Triage,
            runtime_limits: Some(DelegationRuntimeLimits {
                max_concurrent_children: 3,
                max_children_per_parent: 10,
                max_total_children: DEFAULT_MAX_TOTAL_CHILDREN,
                max_parallel_groups: 3,
                max_depth: DEFAULT_MAX_DEPTH,
                max_budget_share_bps: DEFAULT_MAX_BUDGET_SHARE_BPS,
                child_budget_override: None,
                child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
            }),
            examples: vec![
                "/delegate multi_source_triage Triage the failing workflow signals and summarize the probable root causes."
                    .to_owned(),
            ],
        },
    ];

    DelegationCatalog { profiles, templates }
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_owned())
}

fn normalize_allowlist(values: Vec<String>) -> Vec<String> {
    let mut normalized = values
        .into_iter()
        .filter_map(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_ascii_lowercase())
            }
        })
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn find_profile<'a>(
    catalog: &'a DelegationCatalog,
    profile_id: &str,
) -> Option<&'a DelegationProfileDefinition> {
    catalog.profiles.iter().find(|profile| profile.profile_id == profile_id)
}

fn find_template<'a>(
    catalog: &'a DelegationCatalog,
    template_id: &str,
) -> Option<&'a DelegationTemplateDefinition> {
    catalog.templates.iter().find(|template| template.template_id == template_id)
}

fn ensure_identifier(raw: &str, field: &str) -> Result<String, Status> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(format!("{field} cannot be empty")));
    }
    if trimmed.len() > 64 {
        return Err(Status::invalid_argument(format!("{field} cannot exceed 64 characters")));
    }
    if trimmed.chars().any(|character| {
        !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    }) {
        return Err(Status::invalid_argument(format!("{field} contains unsupported characters")));
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn ensure_budget_tokens(value: u64, field: &str) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > MAX_DELEGATION_BUDGET_TOKENS {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the maximum supported delegation budget"
        )));
    }
    Ok(value)
}

fn ensure_attempts(value: u64, field: &str) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > MAX_DELEGATION_ATTEMPTS {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the maximum supported attempt count"
        )));
    }
    Ok(value)
}

fn ensure_count_limit(value: u64, field: &str, maximum: u64) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > maximum {
        return Err(Status::invalid_argument(format!("{field} exceeds the supported maximum")));
    }
    Ok(value)
}

fn ensure_child_timeout_ms(value: u64, field: &str) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > MAX_DELEGATION_CHILD_TIMEOUT_MS {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the maximum supported child timeout"
        )));
    }
    Ok(value)
}

fn ensure_budget_share_bps(value: u64, field: &str) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > MAX_DELEGATION_BUDGET_SHARE_BPS {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the supported budget share ceiling"
        )));
    }
    Ok(value)
}

fn ensure_runtime_limits(
    limits: DelegationRuntimeLimits,
    field: &str,
) -> Result<DelegationRuntimeLimits, Status> {
    let child_budget_override = limits
        .child_budget_override
        .map(|value| ensure_budget_tokens(value, &format!("{field}.child_budget_override")))
        .transpose()?;
    Ok(DelegationRuntimeLimits {
        max_concurrent_children: ensure_count_limit(
            limits.max_concurrent_children,
            &format!("{field}.max_concurrent_children"),
            MAX_DELEGATION_CONCURRENT_CHILDREN,
        )?,
        max_children_per_parent: ensure_count_limit(
            limits.max_children_per_parent,
            &format!("{field}.max_children_per_parent"),
            MAX_DELEGATION_CHILDREN_PER_PARENT,
        )?,
        max_total_children: ensure_count_limit(
            limits.max_total_children,
            &format!("{field}.max_total_children"),
            MAX_DELEGATION_TOTAL_CHILDREN,
        )?,
        max_parallel_groups: ensure_count_limit(
            limits.max_parallel_groups,
            &format!("{field}.max_parallel_groups"),
            MAX_DELEGATION_PARALLEL_GROUPS,
        )?,
        max_depth: ensure_count_limit(
            limits.max_depth,
            &format!("{field}.max_depth"),
            MAX_DELEGATION_DEPTH,
        )?,
        max_budget_share_bps: ensure_budget_share_bps(
            limits.max_budget_share_bps,
            &format!("{field}.max_budget_share_bps"),
        )?,
        child_budget_override,
        child_timeout_ms: ensure_child_timeout_ms(
            limits.child_timeout_ms,
            &format!("{field}.child_timeout_ms"),
        )?,
    })
}

fn apply_manifest_runtime_overrides(
    limits: &mut DelegationRuntimeLimits,
    manifest: &DelegationManifestInput,
) -> Result<(), Status> {
    if let Some(value) = manifest.max_concurrent_children {
        limits.max_concurrent_children = ensure_count_limit(
            value,
            "delegation.manifest.max_concurrent_children",
            MAX_DELEGATION_CONCURRENT_CHILDREN,
        )?;
    }
    if let Some(value) = manifest.max_children_per_parent {
        limits.max_children_per_parent = ensure_count_limit(
            value,
            "delegation.manifest.max_children_per_parent",
            MAX_DELEGATION_CHILDREN_PER_PARENT,
        )?;
    }
    if let Some(value) = manifest.max_total_children {
        limits.max_total_children = ensure_count_limit(
            value,
            "delegation.manifest.max_total_children",
            MAX_DELEGATION_TOTAL_CHILDREN,
        )?;
    }
    if let Some(value) = manifest.max_parallel_groups {
        limits.max_parallel_groups = ensure_count_limit(
            value,
            "delegation.manifest.max_parallel_groups",
            MAX_DELEGATION_PARALLEL_GROUPS,
        )?;
    }
    if let Some(value) = manifest.max_depth {
        limits.max_depth =
            ensure_count_limit(value, "delegation.manifest.max_depth", MAX_DELEGATION_DEPTH)?;
    }
    if let Some(value) = manifest.max_budget_share_bps {
        limits.max_budget_share_bps =
            ensure_budget_share_bps(value, "delegation.manifest.max_budget_share_bps")?;
    }
    if let Some(value) = manifest.child_budget_override {
        limits.child_budget_override =
            Some(ensure_budget_tokens(value, "delegation.manifest.child_budget_override")?);
    }
    if let Some(value) = manifest.child_timeout_ms {
        limits.child_timeout_ms =
            ensure_child_timeout_ms(value, "delegation.manifest.child_timeout_ms")?;
    }
    Ok(())
}

fn validate_allowlist_subset(
    field: &str,
    requested: &[String],
    parent_allowlist: &[String],
) -> Result<(), Status> {
    // Empty parent allowlists are the default unrestricted harness mode; a
    // non-empty list opts into subset containment for delegated children.
    if parent_allowlist.is_empty() {
        return Ok(());
    }
    if let Some(disallowed) = requested
        .iter()
        .find(|candidate| !parent_allowlist.iter().any(|parent| parent == *candidate))
    {
        return Err(Status::invalid_argument(format!(
            "{field} entry '{disallowed}' exceeds the parent allowlist"
        )));
    }
    Ok(())
}

/// Resolves a raw delegation request into a validated
/// [`DelegationSnapshot`].
///
/// Layering order: catalog profile (or the template's primary profile with
/// template overrides), then manifest field overrides, then the request's
/// execution mode. Serial mode clamps concurrency to one before the limits
/// are validated; the child budget must fit both the parent's absolute
/// budget and the configured budget share.
///
/// # Errors
/// Returns `Status::invalid_argument` for conflicting profile/template
/// ids, out-of-cap overrides, budget escalation, or allowlist entries
/// outside the parent's; `Status::not_found` for unknown profile or
/// template ids; `Status::failed_precondition` when the parent allowlist
/// is empty but the child requests grants; and `Status::internal` for a
/// template referencing a missing profile.
pub fn resolve_delegation_request(
    request: &DelegationRequestInput,
    parent: &DelegationParentContext,
) -> Result<DelegationSnapshot, Status> {
    let catalog = built_in_delegation_catalog();
    if request.profile_id.is_some() && request.template_id.is_some() {
        return Err(Status::invalid_argument(
            "delegation cannot specify both profile_id and template_id",
        ));
    }

    let requested_profile_id = normalize_optional_text(request.profile_id.as_deref())
        .map(|value| value.to_ascii_lowercase());
    let requested_template_id = normalize_optional_text(request.template_id.as_deref())
        .map(|value| value.to_ascii_lowercase());

    let mut base_profile = if let Some(profile_id) = requested_profile_id.as_deref() {
        find_profile(&catalog, profile_id).cloned().ok_or_else(|| {
            Status::not_found(format!("delegation profile not found: {profile_id}"))
        })?
    } else {
        find_profile(&catalog, "research").cloned().expect("default research profile must exist")
    };
    let mut template_id = None;
    if let Some(template_key) = requested_template_id.as_deref() {
        let template = find_template(&catalog, template_key).ok_or_else(|| {
            Status::not_found(format!("delegation template not found: {template_key}"))
        })?;
        base_profile = find_profile(&catalog, template.primary_profile_id.as_str())
            .cloned()
            .ok_or_else(|| {
                Status::internal(format!(
                    "delegation template '{}' references an unknown primary profile",
                    template.template_id
                ))
            })?;
        base_profile.execution_mode = template.execution_mode;
        base_profile.merge_contract.strategy = template.merge_strategy;
        if let Some(runtime_limits) = template.runtime_limits.clone() {
            base_profile.runtime_limits = runtime_limits;
        }
        template_id = Some(template.template_id.clone());
    }

    if let Some(manifest) = request.manifest.as_ref() {
        if let Some(profile_id) = manifest.profile_id.as_deref() {
            let profile_key = ensure_identifier(profile_id, "delegation.manifest.profile_id")?;
            base_profile =
                find_profile(&catalog, profile_key.as_str()).cloned().ok_or_else(|| {
                    Status::not_found(format!(
                        "delegation manifest profile not found: {profile_key}"
                    ))
                })?;
        }
        if let Some(display_name) = normalize_optional_text(manifest.display_name.as_deref()) {
            base_profile.display_name = display_name;
        }
        if let Some(description) = normalize_optional_text(manifest.description.as_deref()) {
            base_profile.description = description;
        }
        if let Some(role) = manifest.role {
            base_profile.role = role;
        }
        if let Some(model_profile) = normalize_optional_text(manifest.model_profile.as_deref()) {
            base_profile.model_profile = Some(model_profile);
        }
        let manifest_tool_allowlist = normalize_allowlist(manifest.tool_allowlist.clone());
        if !manifest_tool_allowlist.is_empty() {
            base_profile.tool_allowlist = manifest_tool_allowlist;
        }
        let manifest_skill_allowlist = normalize_allowlist(manifest.skill_allowlist.clone());
        if !manifest_skill_allowlist.is_empty() {
            base_profile.skill_allowlist = manifest_skill_allowlist;
        }
        if let Some(memory_scope) = manifest.memory_scope {
            base_profile.memory_scope = memory_scope;
        }
        if let Some(budget_tokens) = manifest.budget_tokens {
            base_profile.budget_tokens =
                ensure_budget_tokens(budget_tokens, "delegation.manifest.budget_tokens")?;
        }
        if let Some(max_attempts) = manifest.max_attempts {
            base_profile.max_attempts =
                ensure_attempts(max_attempts, "delegation.manifest.max_attempts")?;
        }
        if let Some(merge_strategy) = manifest.merge_strategy {
            base_profile.merge_contract.strategy = merge_strategy;
        }
        if let Some(approval_required) = manifest.approval_required {
            base_profile.merge_contract.approval_required = approval_required;
        }
        apply_manifest_runtime_overrides(&mut base_profile.runtime_limits, manifest)?;
    }

    let execution_mode = request.execution_mode.unwrap_or(base_profile.execution_mode);
    if execution_mode == DelegationExecutionMode::Serial {
        base_profile.runtime_limits.max_concurrent_children = 1;
        base_profile.runtime_limits.max_parallel_groups = 1;
    }
    let runtime_limits =
        ensure_runtime_limits(base_profile.runtime_limits, "delegation.runtime_limits")?;
    let budget_tokens = ensure_budget_tokens(
        runtime_limits.child_budget_override.unwrap_or(base_profile.budget_tokens),
        "delegation.budget_tokens",
    )?;
    let max_attempts = ensure_attempts(base_profile.max_attempts, "delegation.max_attempts")?;
    if let Some(parent_budget_tokens) = parent.parent_budget_tokens {
        if budget_tokens > parent_budget_tokens {
            return Err(Status::invalid_argument(
                "delegation budget_tokens exceeds the parent budget ceiling",
            ));
        }
        // max(1) keeps tiny parent budgets from rounding the share down to
        // zero, which would reject every child regardless of its budget.
        let allowed_share = parent_budget_tokens
            .saturating_mul(runtime_limits.max_budget_share_bps)
            .saturating_div(10_000)
            .max(1);
        if budget_tokens > allowed_share {
            return Err(Status::invalid_argument(
                "delegation budget_tokens exceeds the configured parent budget share",
            ));
        }
    }

    let tool_allowlist = normalize_allowlist(base_profile.tool_allowlist.clone());
    validate_allowlist_subset(
        "delegation.tool_allowlist",
        tool_allowlist.as_slice(),
        parent.parent_tool_allowlist.as_slice(),
    )?;
    let skill_allowlist = normalize_allowlist(base_profile.skill_allowlist.clone());
    validate_allowlist_subset(
        "delegation.skill_allowlist",
        skill_allowlist.as_slice(),
        parent.parent_skill_allowlist.as_slice(),
    )?;

    let group_id = if let Some(group_id) = request.group_id.as_deref() {
        ensure_identifier(group_id, "delegation.group_id")?
    } else if execution_mode == DelegationExecutionMode::Serial {
        format!(
            "serial-{}-{}",
            parent
                .parent_run_id
                .as_deref()
                .and_then(|value| normalize_optional_text(Some(value)))
                .unwrap_or_else(|| "root".to_owned()),
            template_id.as_deref().unwrap_or(base_profile.profile_id.as_str()).replace('.', "-")
        )
    } else {
        format!(
            "parallel-{}",
            parent
                .parent_run_id
                .as_deref()
                .and_then(|value| normalize_optional_text(Some(value)))
                .unwrap_or_else(|| "root".to_owned())
        )
    };

    let model_profile = base_profile
        .model_profile
        .clone()
        .or_else(|| normalize_optional_text(parent.parent_model_profile.as_deref()))
        .ok_or_else(|| {
            Status::failed_precondition(
                "delegation requires a model profile from the manifest or parent agent",
            )
        })?;

    Ok(DelegationSnapshot {
        profile_id: base_profile.profile_id,
        display_name: base_profile.display_name,
        description: Some(base_profile.description),
        template_id,
        role: base_profile.role,
        execution_mode,
        group_id,
        model_profile,
        tool_allowlist,
        skill_allowlist,
        memory_scope: base_profile.memory_scope,
        budget_tokens,
        max_attempts,
        merge_contract: base_profile.merge_contract,
        runtime_limits,
        agent_id: parent.agent_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        build_delegated_run_record, build_delegated_scope, build_subagent_session_record,
        built_in_delegation_catalog, resolve_delegation_request, DelegatedReferenceInput,
        DelegatedRunRecordBuildRequest, DelegatedRunState, DelegatedScopeBuildRequest,
        DelegationExecutionMode, DelegationManifestInput, DelegationMemoryScopeKind,
        DelegationMergeStatus, DelegationParentContext, DelegationRequestInput, DelegationRole,
        SubagentLinkStatus, SubagentSessionRecordBuildRequest, SubagentTranscriptStatus,
    };

    fn parent_context() -> DelegationParentContext {
        DelegationParentContext {
            parent_run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            agent_id: Some("main".to_owned()),
            parent_model_profile: Some("provider-runtime-model".to_owned()),
            parent_tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            parent_skill_allowlist: vec!["repo.read".to_owned()],
            parent_budget_tokens: Some(2_400),
        }
    }

    #[test]
    fn built_in_catalog_contains_expected_templates() {
        let catalog = built_in_delegation_catalog();
        assert!(
            catalog.templates.iter().any(|template| template.template_id == "review_and_patch"),
            "template pack should expose the review_and_patch pattern"
        );
        assert!(
            catalog.profiles.iter().any(|profile| profile.profile_id == "research"),
            "catalog should expose a research profile"
        );
    }

    #[test]
    fn resolve_delegation_request_inherits_parent_model_profile_without_catalog_default() {
        let snapshot = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("research".to_owned()),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("built-in profiles should inherit the parent provider model");

        assert_eq!(snapshot.model_profile, "provider-runtime-model");
    }

    #[test]
    fn resolve_delegation_request_manifest_model_profile_overrides_parent() {
        let snapshot = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("synthesis".to_owned()),
                manifest: Some(DelegationManifestInput {
                    model_profile: Some("operator-selected-model".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("manifest model profile should remain an explicit operator override");

        assert_eq!(snapshot.model_profile, "operator-selected-model");
    }

    #[test]
    fn resolve_delegation_request_requires_parent_or_manifest_model_profile() {
        let mut parent = parent_context();
        parent.parent_model_profile = None;

        let error = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("synthesis".to_owned()),
                ..Default::default()
            },
            &parent,
        )
        .expect_err("delegation must not fall back to a hard-coded provider model");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("requires a model profile"));
    }

    #[test]
    fn resolve_delegation_request_caps_against_parent_context() {
        let error = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    display_name: Some("Wide patcher".to_owned()),
                    role: Some(DelegationRole::Patching),
                    tool_allowlist: vec!["palyra.fs.apply_patch".to_owned()],
                    budget_tokens: Some(2_000),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect_err("wider manifest than parent should fail");
        assert!(
            error
                .message()
                .contains("delegation tool_allowlist entry 'palyra.fs.apply_patch' exceeds")
                || error.message().contains("delegation.tool_allowlist entry"),
            "validation should explain the allowlist conflict"
        );
    }

    #[test]
    fn resolve_delegation_request_treats_empty_parent_allowlist_as_unrestricted() {
        let mut parent = parent_context();
        parent.parent_tool_allowlist.clear();
        let delegated = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("research".to_owned()),
                ..Default::default()
            },
            &parent,
        )
        .expect("empty parent allowlist should not block default harness tools");

        assert!(delegated.tool_allowlist.contains(&"palyra.http.fetch".to_owned()));
        let no_tools = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("synthesis".to_owned()),
                ..Default::default()
            },
            &parent,
        )
        .expect("no-tool child profile should remain valid under an empty parent allowlist");
        assert!(no_tools.tool_allowlist.is_empty());
    }

    #[test]
    fn resolve_delegation_request_accepts_template_override() {
        let snapshot = resolve_delegation_request(
            &DelegationRequestInput {
                template_id: Some("research_then_synthesize".to_owned()),
                execution_mode: Some(DelegationExecutionMode::Serial),
                manifest: Some(DelegationManifestInput {
                    display_name: Some("Focused research".to_owned()),
                    memory_scope: Some(DelegationMemoryScopeKind::ParentSession),
                    budget_tokens: Some(1_200),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("template override should resolve");

        assert_eq!(snapshot.display_name, "Focused research");
        assert_eq!(snapshot.execution_mode, DelegationExecutionMode::Serial);
        assert_eq!(snapshot.memory_scope, DelegationMemoryScopeKind::ParentSession);
        assert_eq!(snapshot.budget_tokens, 1_200);
        assert_eq!(snapshot.template_id.as_deref(), Some("research_then_synthesize"));
    }

    #[test]
    fn resolve_delegation_request_applies_runtime_overrides_and_budget_ceiling() {
        let snapshot = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("research".to_owned()),
                manifest: Some(DelegationManifestInput {
                    max_concurrent_children: Some(3),
                    max_children_per_parent: Some(9),
                    max_total_children: Some(12),
                    max_parallel_groups: Some(2),
                    max_depth: Some(4),
                    max_budget_share_bps: Some(5_000),
                    child_budget_override: Some(1_100),
                    child_timeout_ms: Some(45_000),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("safe runtime overrides should resolve");

        assert_eq!(snapshot.budget_tokens, 1_100);
        assert_eq!(snapshot.runtime_limits.max_concurrent_children, 3);
        assert_eq!(snapshot.runtime_limits.max_children_per_parent, 9);
        assert_eq!(snapshot.runtime_limits.max_total_children, 12);
        assert_eq!(snapshot.runtime_limits.max_parallel_groups, 2);
        assert_eq!(snapshot.runtime_limits.max_depth, 4);
        assert_eq!(snapshot.runtime_limits.max_budget_share_bps, 5_000);
        assert_eq!(snapshot.runtime_limits.child_budget_override, Some(1_100));
        assert_eq!(snapshot.runtime_limits.child_timeout_ms, 45_000);

        let error = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    child_budget_override: Some(9_999),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect_err("child budget override above parent ceiling should fail");
        assert!(error.message().contains("delegation budget_tokens exceeds"));
    }

    #[test]
    fn resolve_delegation_request_forces_serial_runtime_limits() {
        let snapshot = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("research".to_owned()),
                execution_mode: Some(DelegationExecutionMode::Serial),
                manifest: Some(DelegationManifestInput {
                    max_concurrent_children: Some(4),
                    max_parallel_groups: Some(3),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("serial override should resolve");

        assert_eq!(snapshot.execution_mode, DelegationExecutionMode::Serial);
        assert_eq!(snapshot.runtime_limits.max_concurrent_children, 1);
        assert_eq!(snapshot.runtime_limits.max_parallel_groups, 1);
    }

    #[test]
    fn resolve_delegation_request_rejects_budget_share_escalation() {
        let error = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    budget_tokens: Some(1_300),
                    max_budget_share_bps: Some(5_000),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect_err("budget above the configured parent share should fail");

        assert!(error.message().contains("configured parent budget share"));
    }

    #[test]
    fn delegated_scope_builder_rejects_scope_escalation_and_redacts_refs() {
        let delegation = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    budget_tokens: Some(1_000),
                    tool_allowlist: vec!["palyra.http.fetch".to_owned()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("delegation should resolve");

        let scope = build_delegated_scope(DelegatedScopeBuildRequest {
            objective: "Read https://example.com/callback?access_token=secret and summarize"
                .to_owned(),
            delegation: delegation.clone(),
            parent_tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            parent_skill_allowlist: vec![],
            context_refs: vec![DelegatedReferenceInput {
                ref_id: "ctx-1".to_owned(),
                reason: "needed for the child objective".to_owned(),
                sensitivity: "internal".to_owned(),
            }],
            memory_refs: Vec::new(),
            artifact_refs: Vec::new(),
        })
        .expect("narrow scope should build");

        assert_eq!(scope.tool_allowlist, vec!["palyra.http.fetch"]);
        assert!(scope.child_prompt.contains("Do not request broader tools"));
        assert!(!scope.safe_snapshot_json()["child_prompt"]
            .as_str()
            .expect("child prompt should be text")
            .contains("secret"));

        let mut wider = delegation;
        wider.tool_allowlist = vec!["palyra.fs.apply_patch".to_owned()];
        let error = build_delegated_scope(DelegatedScopeBuildRequest {
            objective: "Patch files".to_owned(),
            delegation: wider,
            parent_tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            parent_skill_allowlist: vec![],
            context_refs: Vec::new(),
            memory_refs: Vec::new(),
            artifact_refs: Vec::new(),
        })
        .expect_err("wider tool scope should fail");
        assert!(error.message().contains("scope.tool_allowlist"));
    }

    #[test]
    fn delegated_scope_builder_accepts_empty_parent_allowlist_default() {
        let delegation = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    budget_tokens: Some(1_000),
                    tool_allowlist: vec!["palyra.http.fetch".to_owned()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("delegation should resolve");

        let scope = build_delegated_scope(DelegatedScopeBuildRequest {
            objective: "Fetch docs and summarize".to_owned(),
            delegation,
            parent_tool_allowlist: Vec::new(),
            parent_skill_allowlist: Vec::new(),
            context_refs: Vec::new(),
            memory_refs: Vec::new(),
            artifact_refs: Vec::new(),
        })
        .expect("empty parent allowlist should not block default harness tools");

        assert_eq!(scope.tool_allowlist, vec!["palyra.http.fetch"]);
    }

    #[test]
    fn delegated_run_record_has_deterministic_safe_snapshot_and_explain() {
        let delegation = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    budget_tokens: Some(1_000),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("delegation should resolve");
        let scope = build_delegated_scope(DelegatedScopeBuildRequest {
            objective: "Summarize child output".to_owned(),
            delegation: delegation.clone(),
            parent_tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            parent_skill_allowlist: vec!["repo.read".to_owned()],
            context_refs: Vec::new(),
            memory_refs: Vec::new(),
            artifact_refs: vec![DelegatedReferenceInput {
                ref_id: "artifact-1".to_owned(),
                reason: "merge evidence".to_owned(),
                sensitivity: "internal".to_owned(),
            }],
        })
        .expect("scope should build");

        let record = build_delegated_run_record(DelegatedRunRecordBuildRequest {
            parent_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            child_run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()),
            task_id: Some("task-1".to_owned()),
            objective: "Summarize child output".to_owned(),
            scope,
            delegation,
            state: DelegatedRunState::Running,
            merge_status: DelegationMergeStatus::NotReady,
            event_type: "child_started".to_owned(),
            event_reason: "spawned by delegate command".to_owned(),
            observed_at_unix_ms: 1_000,
        });

        let first =
            serde_json::to_string(&record.safe_snapshot_json()).expect("snapshot should serialize");
        let second = serde_json::to_string(&record.safe_snapshot_json())
            .expect("snapshot should serialize deterministically");
        assert_eq!(first, second);
        assert_eq!(
            record.delegated_run_id,
            "dr_01ARZ3NDEKTSV4RRFFQ69G5FAV_01ARZ3NDEKTSV4RRFFQ69G5FAW"
        );
        assert_eq!(record.explain_json()["state"], "running");
    }

    #[test]
    fn subagent_session_record_serializes_transcript_ref_and_stale_repair_plan() {
        let delegation = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    budget_tokens: Some(1_000),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("delegation should resolve");
        let scope = build_delegated_scope(DelegatedScopeBuildRequest {
            objective: "Summarize child output with token=secret".to_owned(),
            delegation: delegation.clone(),
            parent_tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            parent_skill_allowlist: vec!["repo.read".to_owned()],
            context_refs: Vec::new(),
            memory_refs: Vec::new(),
            artifact_refs: vec![DelegatedReferenceInput {
                ref_id: "artifact-1".to_owned(),
                reason: "merge evidence".to_owned(),
                sensitivity: "internal".to_owned(),
            }],
        })
        .expect("scope should build");

        let record = build_subagent_session_record(SubagentSessionRecordBuildRequest {
            task_id: "task-1".to_owned(),
            parent_run_id: Some("parent-run".to_owned()),
            child_run_id: Some("child-run".to_owned()),
            child_session_id: "session-1".to_owned(),
            scope,
            delegation,
            status: "succeeded".to_owned(),
            child_run_exists: false,
            task_terminal: true,
            artifacts: vec![serde_json::json!({
                "artifact_id": "artifact-1",
                "artifact_kind": "patch",
            })],
            evidence_refs: Vec::new(),
            verification_state: "verified".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
        });

        assert_eq!(record.record_id, "dr_parent-run_child-run");
        assert_eq!(record.transcript_ref.status, SubagentTranscriptStatus::Stale);
        assert_eq!(record.stale_link_repair.status, SubagentLinkStatus::Stale);
        assert_eq!(record.stale_link_repair.actions.len(), 2);

        let encoded = serde_json::to_string(&record).expect("subagent record should serialize");
        assert!(encoded.contains("\"transcript_ref\""), "{encoded}");
        assert!(encoded.contains("\"retry_or_clear_task_target\""), "{encoded}");
        assert!(!encoded.contains("token=secret"), "{encoded}");
    }
}
