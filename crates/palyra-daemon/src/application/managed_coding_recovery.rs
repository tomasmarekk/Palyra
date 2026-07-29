//! Closed operator contracts for managed coding recovery.
//!
//! Recovery surfaces expose the opaque identities required to select an
//! object, but never serialize host paths, attached run identities, snapshot
//! content, Git index bytes, or process authority.

use serde::{Deserialize, Serialize};

use super::managed_worktree_executor::ManagedWorktreeLifecycleV2;
use super::managed_worktree_snapshots::SnapshotGcDecisionV1;

/// Redacted durable worktree summary for operator recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingWorktreeSummaryV1 {
    /// Summary schema version.
    pub schema_version: u32,
    /// Opaque worktree identity used by recovery mutations.
    pub worktree_id: String,
    /// Current mutation generation.
    pub generation: u64,
    /// SHA-256 of the canonical source repository path.
    pub source_repo_sha256: String,
    /// SHA-256 of the canonical managed worktree path.
    pub worktree_path_sha256: String,
    /// Host-generated branch name.
    pub branch: String,
    /// Requested base ref.
    pub base_ref: String,
    /// Current lifecycle.
    pub lifecycle: ManagedWorktreeLifecycleV2,
    /// Latest observed dirty state.
    pub dirty: bool,
    /// Whether an exclusive run lock is present.
    pub locked: bool,
    /// Number of attached runs without exposing their identities.
    pub attached_run_count: usize,
    /// Creation timestamp.
    pub created_at_unix_ms: i64,
    /// Most recent durable mutation timestamp.
    pub updated_at_unix_ms: i64,
    /// Stable lifecycle reason.
    pub reason_code: String,
}

/// Content-free snapshot summary for recovery inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingSnapshotSummaryV1 {
    /// Summary schema version.
    pub schema_version: u32,
    /// Opaque snapshot identity used by recovery mutations.
    pub snapshot_id: String,
    /// Managed worktree identity.
    pub worktree_id: String,
    /// Worktree generation captured by the snapshot.
    pub worktree_generation: u64,
    /// Exact Git base commit.
    pub base_commit: String,
    /// Number of retained changed paths.
    pub entry_count: usize,
    /// Total retained file and Git index bytes.
    pub total_bytes: u64,
    /// Capture timestamp.
    pub created_at_unix_ms: i64,
}

/// Bounded recovery inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingRecoveryInventoryV1 {
    /// Inventory schema version.
    pub schema_version: u32,
    /// Redacted worktree summaries.
    pub worktrees: Vec<ManagedCodingWorktreeSummaryV1>,
    /// Content-free snapshot summaries.
    pub snapshots: Vec<ManagedCodingSnapshotSummaryV1>,
    /// Stable result reason.
    pub reason_code: String,
}

/// Result of a generation-fenced worktree retention request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingWorktreeMutationV1 {
    /// Result schema version.
    pub schema_version: u32,
    /// Mutated worktree identity.
    pub worktree_id: String,
    /// New mutation generation.
    pub generation: u64,
    /// Resulting lifecycle.
    pub lifecycle: ManagedWorktreeLifecycleV2,
    /// Latest observed dirty state.
    pub dirty: bool,
    /// Stable mutation reason.
    pub reason_code: String,
}

/// Result of one snapshot garbage-collection request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingSnapshotGcOutcomeV1 {
    /// Result schema version.
    pub schema_version: u32,
    /// Snapshot identity.
    pub snapshot_id: String,
    /// Applied retention decision.
    pub decision: SnapshotGcDecisionV1,
    /// Whether passive retention bypass was requested.
    pub force_requested: bool,
    /// Stable decision reason.
    pub reason_code: String,
}
