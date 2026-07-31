//! Bounded worker handoff, review, comment, and model-request contracts.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    WorkBudgetV1, WorkClaimAuthority, WorkGraphConcurrencyPolicy, WorkGraphState, WorkItemSpecV1,
    WorkItemState, WorkVerificationState, WORK_GRAPH_SCHEMA_VERSION,
};

/// Maximum UTF-8 byte length of a worker summary automatically returned to a parent.
pub(crate) const MAX_WORK_HANDOFF_SUMMARY_BYTES: usize = 8 * 1024;
/// Maximum serialized byte length of one on-demand structured result.
pub(crate) const MAX_WORK_HANDOFF_RESULT_BYTES: usize = 64 * 1024;
/// Maximum number of evidence or artifact references in one handoff.
pub(crate) const MAX_WORK_HANDOFF_REFS: usize = 64;
/// Maximum UTF-8 byte length of one opaque evidence or artifact reference.
pub(crate) const MAX_WORK_HANDOFF_REF_BYTES: usize = 2 * 1024;
/// Maximum UTF-8 byte length of one append-only comment.
pub(crate) const MAX_WORK_GRAPH_COMMENT_BYTES: usize = 8 * 1024;
/// Maximum number of records returned by a model-visible query.
pub(crate) const MAX_WORK_GRAPH_QUERY_RECORDS: usize = 64;

/// Durable, bounded result produced by one exact claim generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkItemHandoffEnvelopeV1 {
    pub(crate) schema_version: u32,
    pub(crate) handoff_id: String,
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) claim_generation: u64,
    pub(crate) summary: String,
    pub(crate) structured_result: Value,
    pub(crate) context_cost_tokens: u32,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) artifact_refs: Vec<String>,
    pub(crate) verification_state: WorkVerificationState,
    pub(crate) provenance_sha256: String,
    pub(crate) created_at_unix_ms: i64,
}

/// Host-only request that binds a handoff to current generation authority.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WorkItemHandoffCreateRequest {
    pub(crate) authority: WorkClaimAuthority,
    pub(crate) expected_item_revision: u64,
    pub(crate) actor_principal: String,
    pub(crate) summary: String,
    pub(crate) structured_result: Value,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) artifact_refs: Vec<String>,
    pub(crate) verification_state: WorkVerificationState,
}

/// Handoff plus the durable revisions produced by its append.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkItemHandoffCommitOutcome {
    pub(crate) handoff: WorkItemHandoffEnvelopeV1,
    pub(crate) item_revision: u64,
    pub(crate) graph_revision: u64,
}

/// Append-only scoped comment projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphCommentRecordV1 {
    pub(crate) schema_version: u32,
    pub(crate) sequence: u64,
    pub(crate) comment_id: String,
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) author_principal: String,
    pub(crate) body: String,
    pub(crate) provenance_sha256: String,
    pub(crate) created_at_unix_ms: i64,
}

/// Host-scoped append-only comment request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkGraphCommentCreateRequest {
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) actor_principal: String,
    pub(crate) body: String,
}

/// Reviewer decision over an immutable handoff.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkGraphReviewDecision {
    Approve,
    Reject,
}

impl WorkGraphReviewDecision {
    /// Stable storage representation.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Reject => "reject",
        }
    }
}

/// Append-only review evidence projected from one immutable handoff.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphReviewRecordV1 {
    pub(crate) schema_version: u32,
    pub(crate) review_id: String,
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) handoff_id: String,
    pub(crate) reviewer_principal: String,
    pub(crate) decision: WorkGraphReviewDecision,
    pub(crate) reason_code: String,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) provenance_sha256: String,
    pub(crate) created_at_unix_ms: i64,
}

/// Host-scoped review request; evidence is copied from the referenced handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkGraphReviewRequest {
    pub(crate) graph_id: String,
    pub(crate) work_item_id: String,
    pub(crate) handoff_id: String,
    pub(crate) reviewer_principal: String,
    pub(crate) decision: WorkGraphReviewDecision,
    pub(crate) reason_code: String,
}

/// Result of a durable review and its host-applied item transition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphReviewOutcomeV1 {
    pub(crate) review: WorkGraphReviewRecordV1,
    pub(crate) item_state: WorkItemState,
    pub(crate) item_revision: u64,
    pub(crate) graph_revision: u64,
}

/// Parent-safe result reference included in graph terminal summaries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkItemHandoffSummaryV1 {
    pub(crate) handoff_id: String,
    pub(crate) work_item_id: String,
    pub(crate) summary: String,
    pub(crate) context_cost_tokens: u32,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) artifact_refs: Vec<String>,
    pub(crate) verification_state: WorkVerificationState,
    pub(crate) provenance_sha256: String,
}

/// Bounded graph result consumed by flows, objectives, and delivery arbitration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphTerminalSummaryV1 {
    pub(crate) schema_version: u32,
    pub(crate) graph_id: String,
    pub(crate) graph_revision: u64,
    pub(crate) state: WorkGraphState,
    pub(crate) reason_code: String,
    pub(crate) objective_id: Option<String>,
    pub(crate) flow_id: Option<String>,
    pub(crate) flow_step_id: Option<String>,
    pub(crate) item_count: u32,
    pub(crate) succeeded_item_count: u32,
    pub(crate) total_context_cost_tokens: u64,
    pub(crate) handoffs: Vec<WorkItemHandoffSummaryV1>,
}

/// Bounded owner-scoped graph projection returned by list operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphListEntryV1 {
    pub(crate) schema_version: u32,
    pub(crate) graph_id: String,
    pub(crate) objective_id: Option<String>,
    pub(crate) state: WorkGraphState,
    pub(crate) revision: u64,
    pub(crate) reason_code: String,
    pub(crate) item_count: u32,
    pub(crate) ready_item_count: u32,
    pub(crate) active_item_count: u32,
    pub(crate) terminal_item_count: u32,
    pub(crate) updated_at_unix_ms: i64,
}

/// Stable model-visible operation names for the WorkGraph request surface.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkGraphToolOperation {
    List,
    Create,
    Claim,
    Complete,
    Block,
    Unblock,
    Heartbeat,
    SideEffect,
    Reclaim,
    Cancel,
    Comment,
    Review,
    Retrieve,
    Diagnostics,
}

/// Model-authored request. Identity and transition authority are supplied by the host context.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphToolRequest {
    pub(crate) operation: WorkGraphToolOperation,
    #[serde(default)]
    pub(crate) graph_id: Option<String>,
    #[serde(default)]
    pub(crate) work_item_id: Option<String>,
    #[serde(default)]
    pub(crate) handoff_id: Option<String>,
    #[serde(default)]
    pub(crate) claim_token: Option<String>,
    #[serde(default)]
    pub(crate) claim_generation: Option<u64>,
    #[serde(default)]
    pub(crate) expected_revision: Option<u64>,
    #[serde(default)]
    pub(crate) summary: Option<String>,
    #[serde(default)]
    pub(crate) structured_result: Option<Value>,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
    #[serde(default)]
    pub(crate) artifact_refs: Vec<String>,
    #[serde(default)]
    pub(crate) body: Option<String>,
    #[serde(default)]
    pub(crate) reason_code: Option<String>,
    #[serde(default)]
    pub(crate) review_decision: Option<WorkGraphReviewDecision>,
    #[serde(default)]
    pub(crate) objective_id: Option<String>,
    #[serde(default)]
    pub(crate) routine_id: Option<String>,
    #[serde(default)]
    pub(crate) flow_id: Option<String>,
    #[serde(default)]
    pub(crate) flow_step_id: Option<String>,
    #[serde(default)]
    pub(crate) budget: Option<WorkBudgetV1>,
    #[serde(default)]
    pub(crate) concurrency_policy: Option<WorkGraphConcurrencyPolicy>,
    #[serde(default)]
    pub(crate) items: Vec<WorkItemSpecV1>,
    #[serde(default)]
    pub(crate) capability_profiles: Vec<String>,
    #[serde(default)]
    pub(crate) lease_ttl_ms: Option<u64>,
    #[serde(default)]
    pub(crate) extend_by_ms: Option<u64>,
    #[serde(default)]
    pub(crate) side_effect_state: Option<super::WorkSideEffectFenceState>,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

/// Redaction-safe host decision returned for every accepted model request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphHostDecisionV1 {
    pub(crate) schema_version: u32,
    pub(crate) accepted: bool,
    pub(crate) reason_code: String,
    pub(crate) graph_id: Option<String>,
    pub(crate) work_item_id: Option<String>,
    pub(crate) handoff_id: Option<String>,
    pub(crate) state: Option<String>,
    pub(crate) revision: Option<u64>,
}

impl WorkGraphHostDecisionV1 {
    /// Creates a compact accepted decision at the current WorkGraph schema.
    pub(crate) fn accepted(reason_code: impl Into<String>) -> Self {
        Self {
            schema_version: WORK_GRAPH_SCHEMA_VERSION,
            accepted: true,
            reason_code: reason_code.into(),
            graph_id: None,
            work_item_id: None,
            handoff_id: None,
            state: None,
            revision: None,
        }
    }
}
