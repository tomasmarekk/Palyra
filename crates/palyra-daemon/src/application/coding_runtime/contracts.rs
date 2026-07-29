use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::super::local_resource_governor::ResourceUnitsV1;
use super::super::lsp_document_sync::{DiagnosticsDeltaV2, DiagnosticsFallbackPlanV2};
use super::super::lsp_workspace_supervisor::{LspLanguageV2, LspServerHandleV2};
use super::super::process_supervisor::ProcessSessionState;
use super::super::pty_backend::{PtyBackendKind, TerminalSanitizationReportV1};

/// Schema for integrated coding runtime reports and cleanup evidence.
pub const CODING_RUNTIME_SCHEMA_VERSION: u32 = 2;

/// Host-selected coding execution posture.
#[derive(Debug, Clone)]
pub struct CodingExecutionProfileV2 {
    /// Create and exclusively attach a dedicated Git worktree.
    pub managed_worktree_enabled: bool,
    /// Permit an explicit in-place workspace only when managed worktrees are disabled.
    pub in_place_workspace_fallback_allowed: bool,
    /// Start a persistent language server.
    pub persistent_lsp_enabled: bool,
    /// Permit compiler CLI verification when LSP is unavailable.
    pub cli_diagnostics_fallback_allowed: bool,
    /// Prefer a native terminal for policies that require terminal semantics.
    pub native_pty_enabled: bool,
    /// Permit a clearly degraded pipe-backed run when native PTY creation fails.
    pub process_fallback_without_pty_allowed: bool,
    /// Snapshot and retain dirty worktrees instead of deleting them.
    pub retain_dirty_worktrees: bool,
}

/// Host-owned command policy. Callers select only `command_id`.
#[derive(Debug, Clone)]
pub struct CodingCommandPolicyV2 {
    /// Stable caller-visible command identity.
    pub command_id: String,
    /// Trusted absolute executable.
    pub executable: PathBuf,
    /// Host-owned argument vector.
    pub args: Vec<String>,
    /// Explicit environment after inherited values are cleared.
    pub env: BTreeMap<String, String>,
    /// Whether correct semantics require a native PTY.
    pub requires_terminal: bool,
    /// Overall bounded runtime.
    pub timeout: Duration,
    /// Optional silent-output deadline for pipe-backed execution.
    pub no_output_timeout: Option<Duration>,
    /// Resource grant charged to the selected backend.
    pub resource_units: ResourceUnitsV1,
}

/// Integrated runtime bounds and policy.
#[derive(Debug, Clone)]
pub struct CodingRuntimeConfig {
    /// Execution posture.
    pub profile: CodingExecutionProfileV2,
    /// Maximum concurrently registered coding tasks.
    pub max_tasks: usize,
    /// Maximum files in one full-content patch.
    pub max_patch_files: usize,
    /// Maximum bytes in one source document.
    pub max_source_file_bytes: usize,
    /// Maximum raw/output chunks returned by a command completion.
    pub max_command_output_chunks: usize,
    /// Additional wait allowance after the process execution deadline.
    pub process_drain_allowance: Duration,
    /// Host command policies.
    pub command_policies: Vec<CodingCommandPolicyV2>,
}

/// Availability state reported for one runtime capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodingCapabilityStatusV2 {
    /// Capability is configured and active for this task.
    Active,
    /// Capability is configured but no live service is currently claimed.
    Configured,
    /// Explicit fallback is active.
    Degraded,
    /// Capability is intentionally disabled by host policy.
    Disabled,
    /// Required capability is unavailable and no safe fallback exists.
    Blocked,
}

/// Effective workspace isolation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodingWorkspaceIsolationV2 {
    /// Dedicated exclusive Git worktree.
    ManagedWorktree,
    /// Explicit host-authorized in-place workspace degradation.
    InPlaceExplicit,
}

/// Host authority that selected the coding workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodingWorkspaceAdmissionV2 {
    /// The run request explicitly selected managed coding.
    Explicit,
    /// Operator code-intelligence policy selected a configured repository.
    Policy,
}

/// One closed fallback-matrix row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingFallbackEntryV2 {
    /// Primary capability.
    pub capability: String,
    /// Selected fallback, when any.
    pub fallback: Option<String>,
    /// Whether the fallback is active.
    pub active: bool,
    /// Stable reason.
    pub reason_code: String,
}

/// Truthful task-scoped capability report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingRuntimeCapabilityReportV2 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Authority that selected the coding workspace.
    pub workspace_admission: CodingWorkspaceAdmissionV2,
    /// Workspace isolation state.
    pub workspace_isolation: CodingWorkspaceIsolationV2,
    /// Shared process authority.
    pub process_supervisor: CodingCapabilityStatusV2,
    /// Native terminal posture. `Configured` never claims a live PTY.
    pub native_pty: CodingCapabilityStatusV2,
    /// Persistent process-backed LSP state.
    pub persistent_lsp: CodingCapabilityStatusV2,
    /// Durable process-completion wake bridge.
    pub objective_wait_bridge: CodingCapabilityStatusV2,
    /// Closed fallback matrix.
    pub fallback_matrix: Vec<CodingFallbackEntryV2>,
    /// Stable redacted reasons.
    pub reason_codes: Vec<String>,
}

/// Request to begin one managed coding task.
#[derive(Debug, Clone)]
pub struct CodingTaskBeginRequestV2 {
    /// Host-issued task identity.
    pub task_id: String,
    /// Owning chat session.
    pub session_id: String,
    /// Owning run.
    pub run_id: String,
    /// Authority that selected the coding workspace.
    pub workspace_admission: CodingWorkspaceAdmissionV2,
    /// Source Git repository or explicit in-place workspace.
    pub source_repo: PathBuf,
    /// Git base ref for managed worktree creation.
    pub base_ref: String,
    /// Human-readable managed branch slug.
    pub branch_slug: String,
    /// Language server policy.
    pub language: LspLanguageV2,
}

/// Live task handle. The workspace path is trusted local state, not durable telemetry.
#[derive(Debug, Clone)]
pub struct CodingTaskHandleV2 {
    /// Task identity.
    pub task_id: String,
    /// Owning session.
    pub session_id: String,
    /// Owning run.
    pub run_id: String,
    /// Effective workspace root.
    pub workspace_root: PathBuf,
    /// Managed worktree identity, when enabled.
    pub worktree_id: Option<String>,
    /// Current managed worktree generation.
    pub worktree_generation: Option<u64>,
    /// Language policy.
    pub language: LspLanguageV2,
    /// Active persistent LSP handle, when available.
    pub lsp_handle: Option<LspServerHandleV2>,
    /// Truthful capability report.
    pub capabilities: CodingRuntimeCapabilityReportV2,
}

/// Full-content patch input.
#[derive(Debug, Clone)]
pub struct CodingSourceEditV2 {
    /// Safe workspace-relative path.
    pub relative_path: PathBuf,
    /// Full replacement text.
    pub text: String,
}

/// Opaque admission ticket that binds diagnostics to one existing patch mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodingPatchVerificationTicketV2 {
    /// Host-issued single-use ticket identity.
    pub ticket_id: String,
    /// Managed coding task receiving the patch.
    pub task_id: String,
    /// Bounded workspace-relative paths covered by the baseline.
    pub relative_paths: Vec<PathBuf>,
}

/// Result of applying and synchronizing one source patch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingPatchOutcomeV2 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Task identity.
    pub task_id: String,
    /// Applied workspace-relative paths.
    pub applied_files: Vec<String>,
    /// Generation-aware LSP evidence when available.
    pub diagnostics: Option<DiagnosticsDeltaV2>,
    /// Explicit CLI fallback when LSP evidence is unavailable.
    pub fallback: Option<DiagnosticsFallbackPlanV2>,
    /// Whether files were changed successfully.
    pub applied: bool,
    /// Whether diagnostics constitute positive verification.
    pub diagnostics_verified: bool,
    /// Source and artifact references.
    pub evidence_refs: Vec<String>,
    /// Stable redacted reasons.
    pub reason_codes: Vec<String>,
}

/// Backend that actually executed a coding command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodingCommandBackendV2 {
    /// Shared pipe-backed ProcessSupervisor for a non-terminal command.
    Process,
    /// Real native Unix PTY or Windows ConPTY.
    NativePty,
    /// Explicit degraded pipe fallback for a terminal-required command.
    ProcessWithoutPty,
}

/// Request to execute one host-owned command policy.
#[derive(Debug, Clone)]
pub struct CodingCommandRequestV2 {
    /// Task identity.
    pub task_id: String,
    /// Host policy identity.
    pub command_id: String,
    /// Optional objective continuation linkage.
    pub objective_wait: Option<CodingObjectiveWaitContextV2>,
}

/// Non-blocking command lifecycle projected by the coding runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodingCommandLifecycleV2 {
    /// Execution is owned by its process or terminal actor.
    Running,
    /// Execution reached a durable terminal outcome.
    Completed,
    /// The actor could not produce a trustworthy terminal outcome.
    Failed,
}

/// Handle returned immediately after a command is durably admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingCommandHandleV2 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Task identity.
    pub task_id: String,
    /// Host command policy.
    pub command_id: String,
    /// Process or terminal session identity.
    pub execution_id: String,
    /// Selected backend.
    pub backend: CodingCommandBackendV2,
    /// Native PTY kind when a terminal is active.
    pub pty_backend: Option<PtyBackendKind>,
    /// Current lifecycle.
    pub lifecycle: CodingCommandLifecycleV2,
    /// Durable wait-barrier receipt when objective continuation was requested.
    pub wait_barrier: Option<CodingWaitBarrierReceiptV2>,
    /// Stable visible reasons, including explicit degradation.
    pub reason_codes: Vec<String>,
}

/// Poll result for one non-blocking coding command.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingCommandStatusV2 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Stable launch handle.
    pub handle: CodingCommandHandleV2,
    /// Terminal outcome when `lifecycle` is `completed`.
    pub outcome: Option<CodingCommandOutcomeV2>,
    /// Stable actor failure reason when `lifecycle` is `failed`.
    pub failure_reason_code: Option<String>,
}

/// Model-safe terminal output projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingTerminalOutputV2 {
    /// Redacted display-safe text; raw terminal bytes are never serialized.
    pub safe_text: String,
    /// Latest terminal cursor.
    pub next_cursor: u64,
    /// Whether the requested cursor preceded retained history.
    pub cursor_reset: bool,
    /// Terminal escape/control sanitization evidence.
    pub sanitization: TerminalSanitizationReportV1,
    /// Whether resident raw history was evicted.
    pub truncated: bool,
    /// Whether secret redaction changed the display-safe text.
    pub redacted: bool,
    /// Stable redaction reasons.
    pub redaction_reason_codes: Vec<String>,
}

/// Durable objective linkage for process completion.
#[derive(Debug, Clone)]
pub struct CodingObjectiveWaitContextV2 {
    /// Objective attempt owning the wait barrier.
    pub objective_attempt_id: String,
    /// Session to wake.
    pub session_id: String,
    /// Root run.
    pub root_run_id: String,
    /// Attempt generation.
    pub attempt_generation: u64,
    /// Bounded continuation prompt.
    pub continuation_prompt: String,
    /// Continuation token budget.
    pub budget_tokens: u64,
    /// Barrier expiry.
    pub expires_at_unix_ms: i64,
}

/// Durable barrier registration receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingWaitBarrierReceiptV2 {
    /// Barrier identity.
    pub barrier_id: String,
    /// Process-session source.
    pub process_session_id: String,
    /// Stable registration reason.
    pub reason_code: String,
}

/// Process completion wake receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingWakeReceiptV2 {
    /// Barrier identity.
    pub barrier_id: String,
    /// Number of coalesced wake intents.
    pub wake_intent_count: usize,
    /// Stable completion reason.
    pub reason_code: String,
}

/// Completed build, test, or fallback command.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingCommandOutcomeV2 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Task identity.
    pub task_id: String,
    /// Host policy identity.
    pub command_id: String,
    /// Backend actually used.
    pub backend: CodingCommandBackendV2,
    /// Native PTY kind when a PTY was active.
    pub pty_backend: Option<PtyBackendKind>,
    /// Process or terminal session evidence identity.
    pub execution_id: String,
    /// Process state for pipe-backed execution.
    pub process_state: Option<ProcessSessionState>,
    /// Portable exit code.
    pub exit_code: Option<i64>,
    /// Whether exact process-tree cleanup was verified.
    pub cleanup_verified: bool,
    /// Whether resident command output was truncated.
    pub output_truncated: bool,
    /// Completion wake evidence.
    pub wake: Option<CodingWakeReceiptV2>,
    /// Source references.
    pub evidence_refs: Vec<String>,
    /// Stable redacted reasons, including any PTY degradation.
    pub reason_codes: Vec<String>,
}

/// Final worktree disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodingWorktreeDispositionV2 {
    /// Clean worktree was removed.
    Removed,
    /// Dirty worktree and a lossless snapshot were retained.
    DirtyRetained,
    /// Explicit in-place workspace was left untouched.
    InPlacePreserved,
}

/// Cleanup result emitted only after processes and LSP have settled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingTaskCleanupOutcomeV2 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Task identity.
    pub task_id: String,
    /// LSP stopped or was never active.
    pub lsp_settled: bool,
    /// Number of still-active process sessions owned by this task.
    pub active_process_count: usize,
    /// Whether the exclusive worktree run lock was released.
    pub worktree_lock_released: bool,
    /// Final workspace disposition.
    pub worktree_disposition: CodingWorktreeDispositionV2,
    /// Lossless snapshot identity for retained dirty state.
    pub snapshot_id: Option<String>,
    /// Source references.
    pub evidence_refs: Vec<String>,
    /// Stable redacted reasons.
    pub reason_codes: Vec<String>,
}
