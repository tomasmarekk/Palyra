//! Versioned work graph records shared by storage, scheduling, and host tool surfaces.

use serde::{Deserialize, Serialize};

/// Schema version for durable work graph records and event payloads.
pub(crate) const WORK_GRAPH_SCHEMA_VERSION: u32 = 1;

/// Maximum number of items accepted in one graph.
pub(crate) const MAX_WORK_GRAPH_ITEMS: usize = 1_024;

/// Maximum number of direct dependencies accepted for one item.
pub(crate) const MAX_WORK_ITEM_DEPENDENCIES: usize = 128;

/// Maximum UTF-8 byte length of a model-visible item title.
pub(crate) const MAX_WORK_ITEM_TITLE_BYTES: usize = 512;

/// Maximum UTF-8 byte length of a bounded item description.
pub(crate) const MAX_WORK_ITEM_DESCRIPTION_BYTES: usize = 16 * 1024;

/// Durable lifecycle of a work graph.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkGraphState {
    Active,
    Succeeded,
    Failed,
    Cancelled,
    Archived,
    Invalid,
}

impl WorkGraphState {
    /// Stable storage representation.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Archived => "archived",
            Self::Invalid => "invalid",
        }
    }

    /// Parses a storage value without accepting unknown future variants.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "archived" => Some(Self::Archived),
            "invalid" => Some(Self::Invalid),
            _ => None,
        }
    }

    /// Whether the graph no longer admits execution.
    pub(crate) const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Archived | Self::Invalid
        )
    }
}

/// Host-owned lifecycle of one work item.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkItemState {
    Draft,
    BlockedByDependencies,
    Ready,
    Claimed,
    Running,
    Waiting,
    Review,
    Succeeded,
    Failed,
    Cancelled,
    Stale,
    Archived,
}

impl WorkItemState {
    /// Stable storage representation.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::BlockedByDependencies => "blocked_by_dependencies",
            Self::Ready => "ready",
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Review => "review",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Stale => "stale",
            Self::Archived => "archived",
        }
    }

    /// Parses a storage value without widening the accepted state machine.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "draft" => Some(Self::Draft),
            "blocked_by_dependencies" => Some(Self::BlockedByDependencies),
            "ready" => Some(Self::Ready),
            "claimed" => Some(Self::Claimed),
            "running" => Some(Self::Running),
            "waiting" => Some(Self::Waiting),
            "review" => Some(Self::Review),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "stale" => Some(Self::Stale),
            "archived" => Some(Self::Archived),
            _ => None,
        }
    }

    /// Whether dependency evaluation may treat the item as complete.
    pub(crate) const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled | Self::Archived)
    }

    /// Whether the item currently owns or may own execution authority.
    pub(crate) const fn is_claimed(self) -> bool {
        matches!(self, Self::Claimed | Self::Running | Self::Waiting)
    }
}

/// Resource posture used when admitting a work item.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkResourceClass {
    Interactive,
    CpuHeavy,
    IoHeavy,
    ProviderBound,
    WorkspaceRead,
    WorkspaceMutation,
}

impl WorkResourceClass {
    /// Stable storage representation.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::CpuHeavy => "cpu_heavy",
            Self::IoHeavy => "io_heavy",
            Self::ProviderBound => "provider_bound",
            Self::WorkspaceRead => "workspace_read",
            Self::WorkspaceMutation => "workspace_mutation",
        }
    }

    /// Parses a persisted resource class.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "interactive" => Some(Self::Interactive),
            "cpu_heavy" => Some(Self::CpuHeavy),
            "io_heavy" => Some(Self::IoHeavy),
            "provider_bound" => Some(Self::ProviderBound),
            "workspace_read" => Some(Self::WorkspaceRead),
            "workspace_mutation" => Some(Self::WorkspaceMutation),
            _ => None,
        }
    }
}

/// Host verification posture for a work item result.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkVerificationState {
    Unverified,
    Pending,
    Verified,
    Rejected,
    Waived,
}

impl WorkVerificationState {
    /// Stable storage representation.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Unverified => "unverified",
            Self::Pending => "pending",
            Self::Verified => "verified",
            Self::Rejected => "rejected",
            Self::Waived => "waived",
        }
    }

    /// Parses a persisted verification state.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "unverified" => Some(Self::Unverified),
            "pending" => Some(Self::Pending),
            "verified" => Some(Self::Verified),
            "rejected" => Some(Self::Rejected),
            "waived" => Some(Self::Waived),
            _ => None,
        }
    }

    /// Whether host policy permits a successful terminal transition.
    pub(crate) const fn permits_success(self) -> bool {
        matches!(self, Self::Verified | Self::Waived)
    }
}

/// Delegated execution budget shared by a graph and its children.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct WorkBudgetV1 {
    pub(crate) max_turns: Option<u64>,
    pub(crate) max_provider_calls: Option<u64>,
    pub(crate) max_tokens: Option<u64>,
    pub(crate) max_cost_micros: Option<u64>,
    pub(crate) max_wall_time_ms: Option<u64>,
}

impl WorkBudgetV1 {
    /// Returns true when every bounded child dimension fits inside the parent.
    pub(crate) fn fits_within(self, parent: Self) -> bool {
        budget_dimension_fits(self.max_turns, parent.max_turns)
            && budget_dimension_fits(self.max_provider_calls, parent.max_provider_calls)
            && budget_dimension_fits(self.max_tokens, parent.max_tokens)
            && budget_dimension_fits(self.max_cost_micros, parent.max_cost_micros)
            && budget_dimension_fits(self.max_wall_time_ms, parent.max_wall_time_ms)
    }
}

fn budget_dimension_fits(child: Option<u64>, parent: Option<u64>) -> bool {
    match (child, parent) {
        (None, Some(_)) => false,
        (Some(child), Some(parent)) => child <= parent,
        (_, None) => true,
    }
}

/// Principal, device, and conversation boundary for a graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkGraphOwnerScopeV1 {
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) origin_run_id: Option<String>,
}

/// Durable header for one host-authoritative graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkGraphRecordV1 {
    pub(crate) schema_version: u32,
    pub(crate) graph_id: String,
    pub(crate) owner: WorkGraphOwnerScopeV1,
    pub(crate) objective_id: Option<String>,
    pub(crate) routine_id: Option<String>,
    pub(crate) flow_id: Option<String>,
    pub(crate) flow_step_id: Option<String>,
    pub(crate) state: WorkGraphState,
    pub(crate) budget: WorkBudgetV1,
    pub(crate) revision: u64,
    pub(crate) reason_code: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) completed_at_unix_ms: Option<i64>,
}

/// Immutable specification accepted when a graph is created.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkItemSpecV1 {
    pub(crate) work_item_id: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) priority: i32,
    pub(crate) capability_profile: String,
    pub(crate) dependency_ids: Vec<String>,
    pub(crate) compensates_work_item_id: Option<String>,
    pub(crate) serialization_key: Option<String>,
    pub(crate) resource_class: WorkResourceClass,
    pub(crate) provider_profile: Option<String>,
    pub(crate) workspace_scope: Option<String>,
    pub(crate) budget: WorkBudgetV1,
    pub(crate) max_runtime_ms: u64,
    pub(crate) requires_review: bool,
}

/// Durable item projection. Claim and execution fields are added by later host transitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkItemRecordV1 {
    pub(crate) schema_version: u32,
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) state: WorkItemState,
    pub(crate) priority: i32,
    pub(crate) capability_profile: String,
    pub(crate) dependency_ids: Vec<String>,
    pub(crate) compensates_work_item_id: Option<String>,
    pub(crate) serialization_key: Option<String>,
    pub(crate) resource_class: WorkResourceClass,
    pub(crate) provider_profile: Option<String>,
    pub(crate) workspace_scope: Option<String>,
    pub(crate) budget: WorkBudgetV1,
    pub(crate) max_runtime_ms: u64,
    pub(crate) requires_review: bool,
    pub(crate) verification_state: WorkVerificationState,
    pub(crate) claim: Option<super::WorkItemClaimV1>,
    pub(crate) attempt_count: u64,
    pub(crate) revision: u64,
    pub(crate) reason_code: String,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) artifact_refs: Vec<String>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) completed_at_unix_ms: Option<i64>,
}

/// Atomic creation request for a complete graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkGraphCreateRequest {
    pub(crate) graph_id: String,
    pub(crate) owner: WorkGraphOwnerScopeV1,
    pub(crate) objective_id: Option<String>,
    pub(crate) routine_id: Option<String>,
    pub(crate) flow_id: Option<String>,
    pub(crate) flow_step_id: Option<String>,
    pub(crate) budget: WorkBudgetV1,
    pub(crate) items: Vec<WorkItemSpecV1>,
    pub(crate) actor_principal: String,
}

/// Host-authoritative transition request guarded by a record revision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkItemTransitionRequest {
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) expected_revision: u64,
    pub(crate) target_state: WorkItemState,
    pub(crate) verification_state: Option<WorkVerificationState>,
    pub(crate) reason_code: String,
    pub(crate) actor_principal: String,
}

/// Result of a successful host transition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkItemTransitionOutcome {
    pub(crate) item: WorkItemRecordV1,
    pub(crate) graph_revision: u64,
    pub(crate) dependency_states_changed: Vec<String>,
}

/// Stable machine-readable reason codes emitted by validation and storage.
pub(crate) mod reason {
    pub(crate) const CREATED: &str = "work_graph.created";
    pub(crate) const READY: &str = "work_graph.dependencies_satisfied";
    pub(crate) const DEPENDENCY_BLOCKED: &str = "work_graph.dependencies_pending";
    pub(crate) const DEPENDENCY_FAILED: &str = "work_graph.dependency_failed";
    pub(crate) const INVALID_GRAPH: &str = "work_graph.invalid";
    pub(crate) const INVALID_TRANSITION: &str = "work_graph.invalid_transition";
    pub(crate) const STALE_REVISION: &str = "work_graph.stale_revision";
    pub(crate) const CANCELLED: &str = "work_graph.cancelled";
    pub(crate) const COMPENSATION_REQUIRED: &str = "work_graph.compensation_required";
}

/// Fail-closed validation error with a stable reason code.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[error("{reason_code}: {message}")]
pub(crate) struct WorkGraphValidationError {
    pub(crate) reason_code: &'static str,
    pub(crate) message: String,
}

impl WorkGraphValidationError {
    pub(super) fn new(reason_code: &'static str, message: impl Into<String>) -> Self {
        Self { reason_code, message: message.into() }
    }
}
