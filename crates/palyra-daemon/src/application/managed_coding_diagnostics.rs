//! Closed operator diagnostics for the managed coding runtime.

use serde::{Deserialize, Serialize};

use super::coding_runtime::CodingRuntimeCapabilityReportV2;
use super::local_resource_governor::{
    ResourcePressureActionStateV1, ResourcePriority, ResourceServiceKind, ResourceUnitsV1,
};
use super::lsp_workspace_supervisor::LspDiagnosticsSnapshotV2;

/// Redacted resource usage without per-owner identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingResourceDiagnosticsV1 {
    /// Currently charged resource units.
    pub used: ResourceUnitsV1,
    /// Configured global limit.
    pub limit: ResourceUnitsV1,
    /// Number of active leases.
    pub active_leases: usize,
    /// Number of distinct owners without their identities.
    pub owner_count: usize,
}

/// Redacted pressure decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingPressureDecisionV1 {
    /// SHA-256 of the lease identity.
    pub lease_id_sha256: String,
    /// SHA-256 of the service owner identity.
    pub owner_id_sha256: String,
    /// Service class.
    pub service: ResourceServiceKind,
    /// Retention priority.
    pub priority: ResourcePriority,
    /// Capacity released when applied.
    pub released: ResourceUnitsV1,
    /// Stable decision reason.
    pub reason_code: String,
}

/// Most recent capacity-exhaustion evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingPressureDiagnosticsV1 {
    /// Projection schema version.
    pub schema_version: u32,
    /// Additional requested capacity.
    pub required: ResourceUnitsV1,
    /// Whether capacity was available without revocation.
    pub capacity_available: bool,
    /// Deterministic redacted relief plan.
    pub eviction_plan: Vec<ManagedCodingPressureDecisionV1>,
    /// Stable pressure reason.
    pub reason_code: String,
    /// Observation timestamp.
    pub observed_at_unix_ms: i64,
}

/// Redacted pressure-relief action evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingPressureActionV1 {
    /// Projection schema version.
    pub schema_version: u32,
    /// SHA-256 of the selected lease identity.
    pub lease_id_sha256: String,
    /// SHA-256 of the selected owner identity.
    pub owner_id_sha256: String,
    /// Selected service class.
    pub service: ResourceServiceKind,
    /// Selected retention priority.
    pub priority: ResourcePriority,
    /// Capacity released by the action.
    pub released: ResourceUnitsV1,
    /// Applied, skipped, or failed outcome.
    pub state: ResourcePressureActionStateV1,
    /// Stable outcome reason.
    pub reason_code: String,
    /// Observation timestamp.
    pub observed_at_unix_ms: i64,
}

/// Aggregate worktree and snapshot counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingWorktreeDiagnosticsV1 {
    /// Active or retained worktrees.
    pub active: usize,
    /// Worktrees with observed dirty state.
    pub dirty: usize,
    /// Worktrees holding an exclusive run lock.
    pub locked: usize,
    /// Retained snapshot count.
    pub retained_snapshots: usize,
}

/// Closed daemon diagnostics for managed coding services.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCodingDiagnosticsV1 {
    /// Projection schema version.
    pub schema_version: u32,
    /// Availability label.
    pub status: String,
    /// Active managed coding task count.
    pub active_tasks: usize,
    /// Closed per-task capability reports.
    pub capabilities: Vec<CodingRuntimeCapabilityReportV2>,
    /// Redacted resource usage.
    pub resources: ManagedCodingResourceDiagnosticsV1,
    /// Most recent pressure evaluation, when capacity exhaustion occurred.
    pub pressure: Option<ManagedCodingPressureDiagnosticsV1>,
    /// Bounded recent pressure actions.
    pub pressure_actions: Vec<ManagedCodingPressureActionV1>,
    /// Worktree and snapshot counts.
    pub worktrees: ManagedCodingWorktreeDiagnosticsV1,
    /// Closed language-service health projection.
    pub language_services: Option<LspDiagnosticsSnapshotV2>,
    /// Stable availability reason.
    pub reason_code: String,
}
