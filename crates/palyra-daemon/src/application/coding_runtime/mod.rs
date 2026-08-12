//! Integrated coding execution recipe over managed worktrees, processes, PTY,
//! persistent LSP diagnostics, durable wakes, and lossless cleanup.

pub mod contracts;
mod wake_bridge;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub use contracts::{
    CodingCapabilityStatusV2, CodingCommandBackendV2, CodingCommandHandleV2,
    CodingCommandLifecycleV2, CodingCommandOutcomeV2, CodingCommandPolicyV2,
    CodingCommandRequestV2, CodingCommandStatusV2, CodingExecutionProfileV2, CodingFallbackEntryV2,
    CodingObjectiveWaitContextV2, CodingPatchOutcomeV2, CodingPatchVerificationTicketV2,
    CodingRuntimeCapabilityReportV2, CodingRuntimeConfig, CodingSourceEditV2,
    CodingTaskBeginRequestV2, CodingTaskCleanupOutcomeV2, CodingTaskHandleV2,
    CodingTerminalOutputV2, CodingWaitBarrierReceiptV2, CodingWakeReceiptV2,
    CodingWorkspaceAdmissionV2, CodingWorkspaceIsolationV2, CodingWorktreeDispositionV2,
    CODING_RUNTIME_SCHEMA_VERSION,
};
use thiserror::Error;
pub use wake_bridge::CodingWakeBridge;

use super::local_resource_governor::{
    LocalResourceGovernor, ResourceLeaseRequestV1, ResourcePriority, ResourceServiceKind,
};
use super::lsp_document_sync::{
    fallback_tool_for_language, DiagnosticsBaselineDescriptorV2, DiagnosticsDeltaStatusV2,
    DiagnosticsFallbackPlanV2, LspDocumentChangeV2, LspDocumentCoordinator,
    LspDocumentOpenRequestV2,
};
use super::lsp_workspace_supervisor::{
    LspServerHandleV2, LspWorkspaceOpenRequestV2, LspWorkspaceSupervisor,
};
use super::managed_worktree_executor::{
    ManagedWorktreeCreateRequestV2, ManagedWorktreeExecutor, ManagedWorktreeExecutorError,
    ManagedWorktreeRemoveRequestV2,
};
use super::managed_worktree_snapshots::{WorktreeSnapshotError, WorktreeSnapshotStore};
use super::process_supervisor::{
    ProcessCompletion, ProcessLaunchSpec, ProcessOwnerV2, ProcessSessionState, ProcessSupervisor,
    ProcessSupervisorError,
};
use super::pty_backend::{
    NativePtySession, PtyBackend as _, PtyBackendError, PtyExitOutcomeV1, PtyLaunchSpec,
    PtyOutputPageV1, TerminalInputRequestV1, TerminalResizeRequestV1, TerminalSanitizationReportV1,
    TerminalSizeV1,
};
use crate::sandbox_runner::redact_process_output_projection;

const MAX_IDENTITY_BYTES: usize = 128;
const MAX_ARGUMENTS: usize = 256;
const MAX_ARGUMENT_BYTES: usize = 16 * 1024;
const MAX_ENVIRONMENT_KEYS: usize = 256;

/// Integrated coding runtime failure.
#[derive(Debug, Error)]
pub enum CodingRuntimeError {
    /// Host policy is invalid or unbounded.
    #[error("coding runtime configuration is invalid")]
    InvalidConfiguration,
    /// Task, command, path, or patch request is invalid.
    #[error("coding runtime request is invalid: {0}")]
    InvalidRequest(String),
    /// Task capacity is exhausted.
    #[error("coding runtime task capacity is exhausted")]
    TaskCapacityExhausted,
    /// Task identity is unknown.
    #[error("coding runtime task was not found")]
    TaskNotFound,
    /// Task identity already exists.
    #[error("coding runtime task already exists")]
    TaskAlreadyExists,
    /// Required workspace isolation is unavailable.
    #[error("coding workspace isolation is unavailable")]
    WorkspaceIsolationUnavailable,
    /// Persistent LSP is unavailable and CLI fallback is forbidden.
    #[error("coding LSP is unavailable: {0}")]
    LspUnavailable(String),
    /// Native PTY is unavailable and pipe fallback is forbidden.
    #[error("native PTY is unavailable and no fallback is allowed")]
    PtyUnavailable,
    /// Objective wait was requested without a durable bridge.
    #[error("coding objective wait bridge is unavailable")]
    WaitBridgeUnavailable,
    /// An objective wait barrier or completion wake failed.
    #[error("coding objective wait bridge failed: {0}")]
    WaitBridge(String),
    /// Managed worktree operation failed.
    #[error("coding worktree operation failed: {0}")]
    Worktree(String),
    /// Lossless worktree snapshot failed.
    #[error("coding worktree snapshot failed: {0}")]
    Snapshot(String),
    /// Process supervision failed.
    #[error("coding process operation failed: {0}")]
    Process(String),
    /// PTY operation failed.
    #[error("coding PTY operation failed: {0}")]
    Pty(String),
    /// LSP document synchronization failed.
    #[error("coding LSP document operation failed: {0}")]
    DocumentSync(String),
    /// Filesystem mutation failed.
    #[error("coding source operation failed: {0}")]
    Io(String),
    /// In-memory task state is unavailable.
    #[error("coding runtime task state is unavailable")]
    StateUnavailable,
}

#[derive(Clone)]
struct CodingTaskState {
    handle: CodingTaskHandleV2,
    open_documents: BTreeSet<String>,
    active_processes: BTreeSet<String>,
    active_terminals: BTreeSet<String>,
}

#[derive(Clone)]
struct PendingPatchVerification {
    ticket: CodingPatchVerificationTicketV2,
    baseline: Option<DiagnosticsBaselineDescriptorV2>,
}

/// Task-scoped coding recipe. This is orchestration over existing authorities,
/// not a second agent framework or a second OS-process owner.
pub struct CodingRuntime {
    config: CodingRuntimeConfig,
    process: Arc<ProcessSupervisor>,
    governor: LocalResourceGovernor,
    worktrees: Arc<ManagedWorktreeExecutor>,
    snapshots: Option<Arc<WorktreeSnapshotStore>>,
    lsp: Option<Arc<LspWorkspaceSupervisor>>,
    documents: Option<Arc<LspDocumentCoordinator>>,
    wake_bridge: Arc<RwLock<Option<Arc<dyn CodingWakeBridge>>>>,
    command_policies: BTreeMap<String, CodingCommandPolicyV2>,
    tasks: Arc<Mutex<BTreeMap<String, CodingTaskState>>>,
    commands: Arc<Mutex<BTreeMap<String, CodingCommandStatusV2>>>,
    terminals: Arc<Mutex<BTreeMap<String, Arc<Mutex<NativePtySession>>>>>,
    patch_verifications: Mutex<BTreeMap<String, PendingPatchVerification>>,
    monitors: Mutex<Vec<JoinHandle<()>>>,
}

impl CodingRuntime {
    /// Opens the integrated recipe after validating every host-owned backend.
    ///
    /// # Errors
    /// Returns an error for an unsafe command policy or impossible fallback posture.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        config: CodingRuntimeConfig,
        process: Arc<ProcessSupervisor>,
        governor: LocalResourceGovernor,
        worktrees: Arc<ManagedWorktreeExecutor>,
        snapshots: Option<Arc<WorktreeSnapshotStore>>,
        lsp: Option<Arc<LspWorkspaceSupervisor>>,
        documents: Option<Arc<LspDocumentCoordinator>>,
        wake_bridge: Option<Arc<dyn CodingWakeBridge>>,
    ) -> Result<Self, CodingRuntimeError> {
        validate_config(&config)?;
        if config.profile.managed_worktree_enabled
            && config.profile.retain_dirty_worktrees
            && snapshots.is_none()
        {
            return Err(CodingRuntimeError::InvalidConfiguration);
        }
        if config.profile.persistent_lsp_enabled
            && (lsp.is_none() || documents.is_none())
            && !config.profile.cli_diagnostics_fallback_allowed
        {
            return Err(CodingRuntimeError::InvalidConfiguration);
        }
        let command_policies = config
            .command_policies
            .iter()
            .cloned()
            .map(|policy| (policy.command_id.clone(), policy))
            .collect();
        Ok(Self {
            config,
            process,
            governor,
            worktrees,
            snapshots,
            lsp,
            documents,
            wake_bridge: Arc::new(RwLock::new(wake_bridge)),
            command_policies,
            tasks: Arc::new(Mutex::new(BTreeMap::new())),
            commands: Arc::new(Mutex::new(BTreeMap::new())),
            terminals: Arc::new(Mutex::new(BTreeMap::new())),
            patch_verifications: Mutex::new(BTreeMap::new()),
            monitors: Mutex::new(Vec::new()),
        })
    }

    /// Installs the durable wake bridge after the daemon-wide runtime has been
    /// placed in its final `Arc`.
    ///
    /// This avoids a self-referential constructor while preserving a single
    /// production-owned coding runtime instance.
    ///
    /// # Errors
    /// Returns an error when runtime state is unavailable.
    pub fn install_wake_bridge(
        &self,
        wake_bridge: Arc<dyn CodingWakeBridge>,
    ) -> Result<(), CodingRuntimeError> {
        let mut slot =
            self.wake_bridge.write().map_err(|_| CodingRuntimeError::StateUnavailable)?;
        if slot.is_some() {
            return Err(CodingRuntimeError::InvalidRequest(
                "coding wake bridge is already installed".to_owned(),
            ));
        }
        *slot = Some(wake_bridge);
        Ok(())
    }

    /// Creates or explicitly degrades the workspace scope, attaches the run,
    /// and starts a process-backed LSP when host policy permits.
    ///
    /// # Errors
    /// Returns an error for unsafe identity, isolation, Git, or required LSP failure.
    pub fn begin_task(
        &self,
        request: CodingTaskBeginRequestV2,
    ) -> Result<CodingTaskHandleV2, CodingRuntimeError> {
        validate_identity("task_id", request.task_id.as_str())?;
        validate_identity("session_id", request.session_id.as_str())?;
        validate_identity("run_id", request.run_id.as_str())?;
        {
            let tasks = self.lock_tasks()?;
            if tasks.contains_key(request.task_id.as_str()) {
                return Err(CodingRuntimeError::TaskAlreadyExists);
            }
            if tasks.len() >= self.config.max_tasks {
                return Err(CodingRuntimeError::TaskCapacityExhausted);
            }
        }
        let source_repo = canonical_workspace(request.source_repo.as_path())?;
        let (workspace_root, worktree_id, worktree_generation, workspace_isolation) =
            if self.config.profile.managed_worktree_enabled {
                let record = self
                    .worktrees
                    .create(ManagedWorktreeCreateRequestV2 {
                        worktree_id: request.task_id.clone(),
                        source_repo: source_repo.clone(),
                        branch_slug: request.branch_slug.clone(),
                        base_ref: request.base_ref.clone(),
                    })
                    .map_err(map_worktree_error)?;
                if let Err(error) = self.worktrees.attach_run(
                    record.worktree_id.as_str(),
                    record.generation,
                    request.run_id.as_str(),
                ) {
                    let _ = self.worktrees.remove(ManagedWorktreeRemoveRequestV2 {
                        worktree_id: record.worktree_id.clone(),
                        generation: record.generation,
                        snapshot_available: false,
                    });
                    return Err(map_worktree_error(error));
                }
                let attached = self
                    .worktrees
                    .status(record.worktree_id.as_str())
                    .map_err(map_worktree_error)?;
                (
                    attached.record.worktree_path,
                    Some(attached.record.worktree_id),
                    Some(attached.record.generation),
                    CodingWorkspaceIsolationV2::ManagedWorktree,
                )
            } else if self.config.profile.in_place_workspace_fallback_allowed {
                (source_repo, None, None, CodingWorkspaceIsolationV2::InPlaceExplicit)
            } else {
                return Err(CodingRuntimeError::WorkspaceIsolationUnavailable);
            };

        let lsp_handle =
            self.start_lsp_if_enabled(&request, workspace_root.as_path(), worktree_id.as_deref());
        let lsp_handle = match lsp_handle {
            Ok(handle) => handle,
            Err(error) if self.config.profile.cli_diagnostics_fallback_allowed => {
                let _ = error;
                None
            }
            Err(error) => {
                self.rollback_begun_worktree(
                    worktree_id.as_deref(),
                    worktree_generation,
                    request.run_id.as_str(),
                );
                return Err(error);
            }
        };
        let capabilities = self.capability_report(
            request.workspace_admission,
            workspace_isolation,
            lsp_handle.as_ref(),
        );
        let handle = CodingTaskHandleV2 {
            task_id: request.task_id.clone(),
            session_id: request.session_id,
            run_id: request.run_id,
            workspace_root,
            worktree_id,
            worktree_generation,
            language: request.language,
            lsp_handle,
            capabilities,
        };
        self.lock_tasks()?.insert(
            request.task_id,
            CodingTaskState {
                handle: handle.clone(),
                open_documents: BTreeSet::new(),
                active_processes: BTreeSet::new(),
                active_terminals: BTreeSet::new(),
            },
        );
        Ok(handle)
    }

    /// Resolves the one active managed coding task attached to a run.
    ///
    /// # Errors
    /// Returns an error when runtime state is unavailable or duplicate bindings
    /// violate the one-workspace-per-run invariant.
    pub fn task_handle_for_run(
        &self,
        run_id: &str,
    ) -> Result<Option<CodingTaskHandleV2>, CodingRuntimeError> {
        validate_identity("run_id", run_id)?;
        let tasks = self.lock_tasks()?;
        let mut matches = tasks
            .values()
            .filter(|task| task.handle.run_id == run_id)
            .map(|task| task.handle.clone());
        let task = matches.next();
        if matches.next().is_some() {
            return Err(CodingRuntimeError::StateUnavailable);
        }
        Ok(task)
    }

    /// Returns redacted capability reports for every active coding task.
    ///
    /// # Errors
    /// Returns an error when runtime state is unavailable.
    pub fn active_capability_reports(
        &self,
    ) -> Result<Vec<CodingRuntimeCapabilityReportV2>, CodingRuntimeError> {
        Ok(self.lock_tasks()?.values().map(|task| task.handle.capabilities.clone()).collect())
    }

    /// Resolves an exact host-owned command policy for a process-tool invocation.
    ///
    /// Caller-provided arguments must match the frozen policy byte-for-byte.
    /// Basename lookup is accepted only for a single path component; absolute
    /// requests must canonicalize to the configured executable.
    #[must_use]
    pub fn matching_command_policy_id(&self, command: &str, args: &[String]) -> Option<String> {
        self.matching_command_policy(command, args).map(|policy| policy.command_id)
    }

    /// Resolves an exact policy together with its resource admission contract.
    #[must_use]
    pub fn matching_command_policy(
        &self,
        command: &str,
        args: &[String],
    ) -> Option<CodingCommandPolicyV2> {
        let requested = Path::new(command);
        if command.trim().is_empty() || command.chars().any(char::is_control) {
            return None;
        }
        self.command_policies.values().find_map(|policy| {
            if policy.args != args {
                return None;
            }
            let executable_matches = if requested.is_absolute() {
                requested.canonicalize().ok().as_deref() == Some(policy.executable.as_path())
            } else if requested.components().count() == 1 {
                requested
                    .file_name()
                    .and_then(|value| value.to_str())
                    .zip(policy.executable.file_name().and_then(|value| value.to_str()))
                    .is_some_and(|(left, right)| executable_labels_match(left, right))
            } else {
                false
            };
            executable_matches.then(|| policy.clone())
        })
    }

    /// Captures exact-version diagnostics before the existing workspace patch
    /// authority mutates any file.
    ///
    /// The ticket is single-use. This runtime never writes source bytes; the
    /// caller must execute the repository's canonical patch path and then call
    /// [`Self::complete_patch_verification`].
    ///
    /// # Errors
    /// Returns an error for unsafe paths, unreadable source, or unavailable
    /// LSP evidence.
    pub fn prepare_patch_verification(
        &self,
        task_id: &str,
        relative_paths: &[PathBuf],
    ) -> Result<CodingPatchVerificationTicketV2, CodingRuntimeError> {
        let task = self.task(task_id)?;
        let relative_paths = normalize_patch_paths(
            task.handle.workspace_root.as_path(),
            relative_paths,
            self.config.max_patch_files,
        )?;
        let baseline = if let (Some(handle), Some(documents)) =
            (task.handle.lsp_handle.as_ref(), self.documents.as_ref())
        {
            for relative_path in &relative_paths {
                let relative = path_to_slash(relative_path)?;
                if task.open_documents.contains(relative.as_str()) {
                    continue;
                }
                let text = read_source_document(
                    task.handle.workspace_root.as_path(),
                    relative_path,
                    self.config.max_source_file_bytes,
                )?;
                documents
                    .open_document(LspDocumentOpenRequestV2 {
                        handle: handle.clone(),
                        workspace_root: task.handle.workspace_root.clone(),
                        relative_path: relative_path.clone(),
                        language_id: language_id(task.handle.language).to_owned(),
                        text,
                    })
                    .map_err(|error| CodingRuntimeError::DocumentSync(error.to_string()))?;
                self.lock_tasks()?
                    .get_mut(task_id)
                    .ok_or(CodingRuntimeError::TaskNotFound)?
                    .open_documents
                    .insert(relative);
            }
            Some(
                documents
                    .capture_baseline(handle, relative_paths.as_slice())
                    .map_err(|error| CodingRuntimeError::DocumentSync(error.to_string()))?,
            )
        } else {
            None
        };
        let ticket = CodingPatchVerificationTicketV2 {
            ticket_id: format!("coding_patch_{}", ulid::Ulid::generate()),
            task_id: task_id.to_owned(),
            relative_paths,
        };
        let mut pending =
            self.patch_verifications.lock().map_err(|_| CodingRuntimeError::StateUnavailable)?;
        if pending.len() >= self.config.max_tasks.saturating_mul(2).max(1) {
            return Err(CodingRuntimeError::TaskCapacityExhausted);
        }
        pending.insert(
            ticket.ticket_id.clone(),
            PendingPatchVerification { ticket: ticket.clone(), baseline },
        );
        Ok(ticket)
    }

    /// Synchronizes the post-mutation bytes and returns bounded diagnostics
    /// introduced, resolved, and unchanged by the canonical patch operation.
    ///
    /// # Errors
    /// Returns an error for an unknown ticket, task generation change,
    /// unreadable source, or corrupt LSP evidence.
    pub fn complete_patch_verification(
        &self,
        ticket_id: &str,
    ) -> Result<CodingPatchOutcomeV2, CodingRuntimeError> {
        let pending = self
            .patch_verifications
            .lock()
            .map_err(|_| CodingRuntimeError::StateUnavailable)?
            .remove(ticket_id)
            .ok_or_else(|| {
                CodingRuntimeError::InvalidRequest(
                    "coding patch verification ticket was not found".to_owned(),
                )
            })?;
        let task = self.task(pending.ticket.task_id.as_str())?;
        let applied_files = pending
            .ticket
            .relative_paths
            .iter()
            .map(|path| path_to_slash(path))
            .collect::<Result<Vec<_>, _>>()?;
        let (Some(handle), Some(documents), Some(baseline)) =
            (task.handle.lsp_handle.as_ref(), self.documents.as_ref(), pending.baseline.as_ref())
        else {
            let fallback_tool = fallback_tool_for_language(task.handle.language);
            return Ok(CodingPatchOutcomeV2 {
                schema_version: CODING_RUNTIME_SCHEMA_VERSION,
                task_id: pending.ticket.task_id,
                applied_files,
                diagnostics: None,
                fallback: Some(DiagnosticsFallbackPlanV2 {
                    tool: fallback_tool,
                    command_label: fallback_tool.command_label().to_owned(),
                    reason_code: "coding.lsp_unavailable".to_owned(),
                }),
                applied: true,
                diagnostics_verified: false,
                evidence_refs: Vec::new(),
                reason_codes: vec!["coding.patch_cli_fallback_required".to_owned()],
            });
        };
        if baseline.handle_id != handle.handle_id || baseline.server_generation != handle.generation
        {
            return Err(CodingRuntimeError::DocumentSync(
                "LSP generation changed before patch completion".to_owned(),
            ));
        }
        let changes = pending
            .ticket
            .relative_paths
            .iter()
            .map(|relative_path| {
                Ok(LspDocumentChangeV2 {
                    relative_path: relative_path.clone(),
                    text: read_source_document(
                        task.handle.workspace_root.as_path(),
                        relative_path,
                        self.config.max_source_file_bytes,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, CodingRuntimeError>>()?;
        let diagnostics = documents
            .verify_changes(handle, baseline, changes.as_slice())
            .map_err(|error| CodingRuntimeError::DocumentSync(error.to_string()))?;
        let mut evidence_refs =
            vec![format!("diagnostics-baseline:{}", baseline.artifact.artifact_id)];
        if let Some(artifact) = diagnostics.full_diagnostics_artifact.as_ref() {
            evidence_refs.push(format!("diagnostics-delta:{}", artifact.artifact_id));
        }
        let reason_code = match diagnostics.status {
            DiagnosticsDeltaStatusV2::Verified => "coding.patch_lsp_verified",
            DiagnosticsDeltaStatusV2::BlockingDiagnostics => "coding.patch_blocking_diagnostics",
            DiagnosticsDeltaStatusV2::DiagnosticsTimedOut => "coding.patch_diagnostics_timeout",
            DiagnosticsDeltaStatusV2::ServerGenerationChanged => {
                "coding.patch_lsp_generation_changed"
            }
            DiagnosticsDeltaStatusV2::FallbackRequired => "coding.patch_cli_fallback_required",
        };
        Ok(CodingPatchOutcomeV2 {
            schema_version: CODING_RUNTIME_SCHEMA_VERSION,
            task_id: pending.ticket.task_id,
            applied_files,
            fallback: diagnostics.fallback.clone(),
            applied: true,
            diagnostics_verified: diagnostics.verified(),
            diagnostics: Some(diagnostics),
            evidence_refs,
            reason_codes: vec![reason_code.to_owned()],
        })
    }

    /// Discards a pre-mutation diagnostics ticket after patch denial or failure.
    pub fn cancel_patch_verification(&self, ticket_id: &str) {
        if let Ok(mut pending) = self.patch_verifications.lock() {
            pending.remove(ticket_id);
        }
    }

    /// Executes one host-owned verification command through native PTY or the
    /// shared ProcessSupervisor, with explicit fallback and completion wake.
    ///
    /// # Errors
    /// Returns an error for unknown policy, unsafe backend, or failed durable wake.
    pub fn run_command(
        &self,
        request: CodingCommandRequestV2,
    ) -> Result<CodingCommandOutcomeV2, CodingRuntimeError> {
        let timeout = self
            .command_policies
            .get(request.command_id.as_str())
            .map(|policy| policy.timeout + self.config.process_drain_allowance)
            .ok_or_else(|| {
                CodingRuntimeError::InvalidRequest("unknown coding command policy".to_owned())
            })?;
        let handle = self.start_command(request)?;
        self.wait_command(handle.execution_id.as_str(), timeout)
    }

    /// Durably admits a command and returns before the child exits.
    ///
    /// Process completion is monitored independently, settles the optional
    /// wait barrier, and stores a terminal outcome for [`Self::command_status`].
    /// This is the production path for long builds and tests.
    ///
    /// # Errors
    /// Returns an error for unknown task or policy, unsafe fallback posture,
    /// launch failure, or wait-barrier registration failure.
    pub fn start_command(
        &self,
        request: CodingCommandRequestV2,
    ) -> Result<CodingCommandHandleV2, CodingRuntimeError> {
        let task = self.task(request.task_id.as_str())?;
        let policy =
            self.command_policies.get(request.command_id.as_str()).cloned().ok_or_else(|| {
                CodingRuntimeError::InvalidRequest("unknown coding command policy".to_owned())
            })?;
        if policy.requires_terminal && self.config.profile.native_pty_enabled {
            match self.start_pty_command(&task, &policy, request.objective_wait.as_ref()) {
                Ok(handle) => return Ok(handle),
                Err(error) if self.config.profile.process_fallback_without_pty_allowed => {
                    let _ = error;
                    return self.start_process_command(
                        &task,
                        &policy,
                        request.objective_wait.as_ref(),
                        CodingCommandBackendV2::ProcessWithoutPty,
                        Some("coding.pty_unavailable_process_fallback"),
                    );
                }
                Err(error) => return Err(error),
            }
        }
        if policy.requires_terminal && !self.config.profile.process_fallback_without_pty_allowed {
            return Err(CodingRuntimeError::PtyUnavailable);
        }
        self.start_process_command(
            &task,
            &policy,
            request.objective_wait.as_ref(),
            if policy.requires_terminal {
                CodingCommandBackendV2::ProcessWithoutPty
            } else {
                CodingCommandBackendV2::Process
            },
            policy.requires_terminal.then_some("coding.pty_disabled_process_fallback"),
        )
    }

    /// Returns the latest non-blocking command state.
    ///
    /// # Errors
    /// Returns an error for an unknown execution or unavailable state.
    pub fn command_status(
        &self,
        execution_id: &str,
    ) -> Result<CodingCommandStatusV2, CodingRuntimeError> {
        self.commands
            .lock()
            .map_err(|_| CodingRuntimeError::StateUnavailable)?
            .get(execution_id)
            .cloned()
            .ok_or_else(|| {
                CodingRuntimeError::InvalidRequest("coding execution was not found".to_owned())
            })
    }

    /// Bounded compatibility wait over the non-blocking command state.
    ///
    /// # Errors
    /// Returns an error for actor failure or when the caller's local wait
    /// deadline expires. The managed execution itself remains supervised.
    pub fn wait_command(
        &self,
        execution_id: &str,
        timeout: Duration,
    ) -> Result<CodingCommandOutcomeV2, CodingRuntimeError> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self.command_status(execution_id)?;
            match status.handle.lifecycle {
                CodingCommandLifecycleV2::Completed => {
                    return status.outcome.ok_or(CodingRuntimeError::StateUnavailable);
                }
                CodingCommandLifecycleV2::Failed => {
                    return Err(CodingRuntimeError::Process(
                        status
                            .failure_reason_code
                            .unwrap_or_else(|| "coding.command_actor_failed".to_owned()),
                    ));
                }
                CodingCommandLifecycleV2::Running => {}
            }
            if Instant::now() >= deadline {
                return Err(CodingRuntimeError::Process(
                    "coding.command_local_wait_timed_out".to_owned(),
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Settles commands and LSP before releasing the worktree lock, then
    /// removes clean state or snapshots and retains dirty state.
    ///
    /// # Errors
    /// Returns an error when cleanup evidence cannot be completed.
    pub fn cleanup_task(
        &self,
        task_id: &str,
    ) -> Result<CodingTaskCleanupOutcomeV2, CodingRuntimeError> {
        let task = self.task(task_id)?;
        self.patch_verifications
            .lock()
            .map_err(|_| CodingRuntimeError::StateUnavailable)?
            .retain(|_, pending| pending.ticket.task_id != task_id);
        let mut reason_codes = Vec::new();
        for process_session_id in &task.active_processes {
            let _ = self.process.terminate(process_session_id.as_str());
            let _ = self.process.wait(
                process_session_id.as_str(),
                None,
                self.config.max_command_output_chunks,
                self.config.process_drain_allowance,
            );
        }
        let wake_bridge = self.wake_bridge()?;
        for terminal_id in &task.active_terminals {
            let terminal = self
                .terminals
                .lock()
                .map_err(|_| CodingRuntimeError::StateUnavailable)?
                .get(terminal_id)
                .cloned();
            let Some(terminal) = terminal else {
                continue;
            };
            let (descriptor, exit, output) = {
                let mut terminal =
                    terminal.lock().map_err(|_| CodingRuntimeError::StateUnavailable)?;
                let exit = terminal.terminate_with_outcome().map_err(map_pty_error)?;
                let descriptor = terminal.session_descriptor();
                let output = terminal.poll_output(None, self.config.max_command_output_chunks);
                (descriptor, exit, output)
            };
            finalize_terminal_command(
                TerminalFinalizationContext {
                    commands: self.commands.as_ref(),
                    terminals: self.terminals.as_ref(),
                    tasks: self.tasks.as_ref(),
                    wake_bridge: wake_bridge.as_ref(),
                    execution_id: terminal_id.as_str(),
                },
                &descriptor,
                &exit,
                output,
            );
        }
        let active_process_count = task
            .active_processes
            .iter()
            .filter(|process_session_id| {
                self.process
                    .status(process_session_id.as_str())
                    .map(|record| !record.state.is_terminal())
                    .unwrap_or(false)
            })
            .count()
            .saturating_add(
                self.lock_tasks()?.get(task_id).map_or(0, |current| current.active_terminals.len()),
            );
        if active_process_count > 0 {
            return Err(CodingRuntimeError::Process(
                "active processes remain after cleanup".to_owned(),
            ));
        }

        let mut lsp_settled = task.handle.lsp_handle.is_none();
        if let Some(handle) = task.handle.lsp_handle.as_ref() {
            if let Some(documents) = self.documents.as_ref() {
                for relative_path in &task.open_documents {
                    let _ = documents.close_document(
                        handle.handle_id.as_str(),
                        Path::new(relative_path.as_str()),
                    );
                }
            }
            if let Some(lsp) = self.lsp.as_ref() {
                match lsp.evict(handle.handle_id.as_str()) {
                    Ok(_) => lsp_settled = true,
                    Err(_) => {
                        lsp_settled = lsp
                            .health()
                            .map(|health| {
                                !health.handles.iter().any(|current| {
                                    current.handle_id == handle.handle_id
                                        && current.lifecycle
                                            == super::lsp_workspace_supervisor::LspServerLifecycleV2::Ready
                                })
                            })
                            .unwrap_or(false);
                    }
                }
            }
        }
        if !lsp_settled {
            return Err(CodingRuntimeError::LspUnavailable(
                "cleanup could not settle the active server".to_owned(),
            ));
        }

        let mut evidence_refs = Vec::new();
        let (worktree_lock_released, worktree_disposition, snapshot_id) =
            if let (Some(worktree_id), Some(generation)) =
                (task.handle.worktree_id.as_deref(), task.handle.worktree_generation)
            {
                let detached = self
                    .worktrees
                    .detach_run(worktree_id, generation, task.handle.run_id.as_str())
                    .map_err(map_worktree_error)?;
                let status = self.worktrees.status(worktree_id).map_err(map_worktree_error)?;
                if status.record.dirty {
                    let snapshot = self
                        .snapshots
                        .as_ref()
                        .ok_or(CodingRuntimeError::InvalidConfiguration)?
                        .capture(worktree_id)
                        .map_err(map_snapshot_error)?;
                    evidence_refs.push(format!("worktree-snapshot:{}", snapshot.snapshot_id));
                    if self.config.profile.retain_dirty_worktrees {
                        self.worktrees
                            .retain(
                                worktree_id,
                                status.record.generation,
                                "coding.dirty_worktree_retained",
                            )
                            .map_err(map_worktree_error)?;
                        reason_codes.push("coding.cleanup_dirty_retained".to_owned());
                        (
                            detached.locked_by_run.is_none(),
                            CodingWorktreeDispositionV2::DirtyRetained,
                            Some(snapshot.snapshot_id),
                        )
                    } else {
                        self.worktrees
                            .remove(ManagedWorktreeRemoveRequestV2 {
                                worktree_id: worktree_id.to_owned(),
                                generation: status.record.generation,
                                snapshot_available: true,
                            })
                            .map_err(map_worktree_error)?;
                        reason_codes.push("coding.cleanup_dirty_snapshotted_removed".to_owned());
                        (
                            detached.locked_by_run.is_none(),
                            CodingWorktreeDispositionV2::Removed,
                            Some(snapshot.snapshot_id),
                        )
                    }
                } else {
                    self.worktrees
                        .remove(ManagedWorktreeRemoveRequestV2 {
                            worktree_id: worktree_id.to_owned(),
                            generation: status.record.generation,
                            snapshot_available: false,
                        })
                        .map_err(map_worktree_error)?;
                    reason_codes.push("coding.cleanup_clean_removed".to_owned());
                    (detached.locked_by_run.is_none(), CodingWorktreeDispositionV2::Removed, None)
                }
            } else {
                reason_codes.push("coding.cleanup_in_place_preserved".to_owned());
                (true, CodingWorktreeDispositionV2::InPlacePreserved, None)
            };
        self.lock_tasks()?.remove(task_id);
        Ok(CodingTaskCleanupOutcomeV2 {
            schema_version: CODING_RUNTIME_SCHEMA_VERSION,
            task_id: task_id.to_owned(),
            lsp_settled,
            active_process_count,
            worktree_lock_released,
            worktree_disposition,
            snapshot_id,
            evidence_refs,
            reason_codes,
        })
    }

    /// Settles every task and joins all command-completion monitors.
    ///
    /// The process supervisor remains owned by the daemon-wide managed coding
    /// composition root and is shut down after this method returns.
    ///
    /// # Errors
    /// Returns the first cleanup or monitor failure after attempting every
    /// remaining task.
    pub fn shutdown(&self) -> Result<(), CodingRuntimeError> {
        let task_ids = self.lock_tasks()?.keys().cloned().collect::<Vec<_>>();
        let mut first_error = None;
        for task_id in task_ids {
            if let Err(error) = self.cleanup_task(task_id.as_str()) {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        let monitors = {
            let mut monitors =
                self.monitors.lock().map_err(|_| CodingRuntimeError::StateUnavailable)?;
            std::mem::take(&mut *monitors)
        };
        for monitor in monitors {
            if monitor.join().is_err() && first_error.is_none() {
                first_error = Some(CodingRuntimeError::Process(
                    "coding completion monitor panicked during shutdown".to_owned(),
                ));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Reconciles a worktree lock left by a prior daemon generation.
    ///
    /// Recovery is allowed only after the shared governor proves that neither
    /// the run nor the worktree has an active managed-service lease. Dirty
    /// state is snapshotted and retained; no PID-only cleanup is attempted.
    ///
    /// # Errors
    /// Returns an error for an unknown lock, active service ownership, or
    /// incomplete snapshot/worktree cleanup.
    pub fn reconcile_interrupted_task(
        &self,
        worktree_id: &str,
        run_id: &str,
    ) -> Result<CodingTaskCleanupOutcomeV2, CodingRuntimeError> {
        validate_identity("worktree_id", worktree_id)?;
        validate_identity("run_id", run_id)?;
        let record = self
            .worktrees
            .list()
            .map_err(map_worktree_error)?
            .into_iter()
            .find(|record| record.worktree_id == worktree_id)
            .ok_or(CodingRuntimeError::TaskNotFound)?;
        if record.locked_by_run.as_deref() != Some(run_id) {
            return Err(CodingRuntimeError::InvalidRequest(
                "interrupted worktree lock does not belong to the supplied run".to_owned(),
            ));
        }
        let worktree_owner = format!("worktree-{worktree_id}");
        let active_lease = self
            .governor
            .active_leases()
            .map_err(|error| CodingRuntimeError::Process(error.to_string()))?
            .into_iter()
            .any(|lease| lease.owner_id == run_id || lease.owner_id == worktree_owner);
        if active_lease {
            return Err(CodingRuntimeError::Process(
                "active managed-service lease blocks interrupted task reconciliation".to_owned(),
            ));
        }
        let detached = self
            .worktrees
            .detach_run(worktree_id, record.generation, run_id)
            .map_err(map_worktree_error)?;
        let status = self.worktrees.status(worktree_id).map_err(map_worktree_error)?;
        let mut evidence_refs = vec![format!("worktree:{worktree_id}")];
        let (worktree_disposition, snapshot_id) = if status.record.dirty {
            let snapshot = self
                .snapshots
                .as_ref()
                .ok_or(CodingRuntimeError::InvalidConfiguration)?
                .capture(worktree_id)
                .map_err(map_snapshot_error)?;
            self.worktrees
                .retain(
                    worktree_id,
                    status.record.generation,
                    "coding.restart_dirty_worktree_retained",
                )
                .map_err(map_worktree_error)?;
            evidence_refs.push(format!("worktree-snapshot:{}", snapshot.snapshot_id));
            (CodingWorktreeDispositionV2::DirtyRetained, Some(snapshot.snapshot_id))
        } else {
            self.worktrees
                .remove(ManagedWorktreeRemoveRequestV2 {
                    worktree_id: worktree_id.to_owned(),
                    generation: status.record.generation,
                    snapshot_available: false,
                })
                .map_err(map_worktree_error)?;
            (CodingWorktreeDispositionV2::Removed, None)
        };
        Ok(CodingTaskCleanupOutcomeV2 {
            schema_version: CODING_RUNTIME_SCHEMA_VERSION,
            task_id: worktree_id.to_owned(),
            lsp_settled: true,
            active_process_count: 0,
            worktree_lock_released: detached.locked_by_run.is_none(),
            worktree_disposition,
            snapshot_id,
            evidence_refs,
            reason_codes: vec!["coding.restart_reconciled_without_pid_adoption".to_owned()],
        })
    }

    /// Returns a current task handle without mutating service liveness.
    ///
    /// # Errors
    /// Returns an error when task state is absent or unavailable.
    pub fn task_handle(&self, task_id: &str) -> Result<CodingTaskHandleV2, CodingRuntimeError> {
        Ok(self.task(task_id)?.handle)
    }

    fn start_lsp_if_enabled(
        &self,
        request: &CodingTaskBeginRequestV2,
        workspace_root: &Path,
        worktree_id: Option<&str>,
    ) -> Result<Option<LspServerHandleV2>, CodingRuntimeError> {
        if !self.config.profile.persistent_lsp_enabled {
            return Ok(None);
        }
        let lsp = self.lsp.as_ref().ok_or_else(|| {
            CodingRuntimeError::LspUnavailable("backend is not configured".to_owned())
        })?;
        lsp.ensure(LspWorkspaceOpenRequestV2 {
            workspace_root: workspace_root.to_path_buf(),
            worktree_id: worktree_id.unwrap_or(request.task_id.as_str()).to_owned(),
            run_id: request.run_id.clone(),
            language: request.language,
        })
        .map(Some)
        .map_err(|error| CodingRuntimeError::LspUnavailable(error.to_string()))
    }

    fn start_process_command(
        &self,
        task: &CodingTaskState,
        policy: &CodingCommandPolicyV2,
        objective_wait: Option<&CodingObjectiveWaitContextV2>,
        backend: CodingCommandBackendV2,
        degradation_reason: Option<&str>,
    ) -> Result<CodingCommandHandleV2, CodingRuntimeError> {
        let wake_bridge = self.wake_bridge()?;
        if objective_wait.is_some() && wake_bridge.is_none() {
            return Err(CodingRuntimeError::WaitBridgeUnavailable);
        }
        let record = self
            .process
            .launch(ProcessLaunchSpec {
                executable: policy.executable.clone(),
                args: policy.args.clone(),
                cwd: task.handle.workspace_root.clone(),
                env: policy.env.clone(),
                owner: ProcessOwnerV2 {
                    session_id: task.handle.session_id.clone(),
                    run_id: task.handle.run_id.clone(),
                    turn_id: "coding-command".to_owned(),
                    agent_id: "coding-runtime".to_owned(),
                    correlation_id: format!("{}-{}", task.handle.task_id, policy.command_id),
                },
                timeout: policy.timeout,
                no_output_timeout: policy.no_output_timeout,
                lease_duration: policy.timeout + self.config.process_drain_allowance,
                resource_priority: ResourcePriority::Foreground,
                resource_service: ResourceServiceKind::Process,
                resource_units: policy.resource_units,
            })
            .map_err(map_process_error)?;
        self.lock_tasks()?
            .get_mut(task.handle.task_id.as_str())
            .ok_or(CodingRuntimeError::TaskNotFound)?
            .active_processes
            .insert(record.process_session_id.clone());
        let barrier = match (objective_wait, wake_bridge.as_ref()) {
            (Some(context), Some(bridge)) => match bridge.register_process_wait(context, &record) {
                Ok(barrier) => Some(barrier),
                Err(error) => {
                    let _ = self.process.terminate(record.process_session_id.as_str());
                    self.remove_active_process(
                        task.handle.task_id.as_str(),
                        record.process_session_id.as_str(),
                    )?;
                    return Err(CodingRuntimeError::WaitBridge(error));
                }
            },
            _ => None,
        };
        let mut reason_codes = vec!["coding.command_running".to_owned()];
        if let Some(reason) = degradation_reason {
            reason_codes.push(reason.to_owned());
        }
        let handle = CodingCommandHandleV2 {
            schema_version: CODING_RUNTIME_SCHEMA_VERSION,
            task_id: task.handle.task_id.clone(),
            command_id: policy.command_id.clone(),
            execution_id: record.process_session_id.clone(),
            backend,
            pty_backend: None,
            lifecycle: CodingCommandLifecycleV2::Running,
            wait_barrier: barrier.clone(),
            reason_codes,
        };
        self.commands.lock().map_err(|_| CodingRuntimeError::StateUnavailable)?.insert(
            record.process_session_id.clone(),
            CodingCommandStatusV2 {
                schema_version: CODING_RUNTIME_SCHEMA_VERSION,
                handle: handle.clone(),
                outcome: None,
                failure_reason_code: None,
            },
        );

        let process = Arc::clone(&self.process);
        let tasks = Arc::clone(&self.tasks);
        let commands = Arc::clone(&self.commands);
        let task = task.clone();
        let task_id = task.handle.task_id.clone();
        let policy = policy.clone();
        let execution_id = record.process_session_id.clone();
        let degradation_reason = degradation_reason.map(str::to_owned);
        let max_chunks = self.config.max_command_output_chunks;
        let drain_allowance = self.config.process_drain_allowance;
        let monitor_barrier = barrier.clone();
        let monitor_wake_bridge = wake_bridge.clone();
        self.spawn_completion_monitor("palyra-coding-process-monitor", move || {
            let completion = wait_process_completion(
                process.as_ref(),
                execution_id.as_str(),
                policy.timeout,
                drain_allowance,
                max_chunks,
            );
            remove_active_process_from_tasks(
                tasks.as_ref(),
                task.handle.task_id.as_str(),
                execution_id.as_str(),
            );
            match completion {
                Ok(completion) => {
                    let wake_result = match (monitor_barrier.as_ref(), monitor_wake_bridge.as_ref())
                    {
                        (Some(barrier), Some(bridge)) => {
                            bridge.emit_process_completion(barrier, &completion.record).map(Some)
                        }
                        _ => Ok(None),
                    };
                    let (wake, lifecycle, failure_reason) = match wake_result {
                        Ok(wake) => (wake, CodingCommandLifecycleV2::Completed, None),
                        Err(_) => (
                            None,
                            CodingCommandLifecycleV2::Failed,
                            Some("coding.process_completion_wake_failed".to_owned()),
                        ),
                    };
                    let outcome = project_process_outcome(
                        &task,
                        &policy,
                        backend,
                        degradation_reason.as_deref(),
                        completion,
                        wake,
                    );
                    settle_command_status(
                        commands.as_ref(),
                        execution_id.as_str(),
                        lifecycle,
                        Some(outcome),
                        failure_reason,
                    );
                }
                Err(error) => {
                    settle_command_status(
                        commands.as_ref(),
                        execution_id.as_str(),
                        CodingCommandLifecycleV2::Failed,
                        None,
                        Some(coding_error_reason(&error)),
                    );
                }
            }
        })
        .map_err(|error| {
            let _ = self.process.terminate(record.process_session_id.as_str());
            if let Ok(completion) = self.process.wait(
                record.process_session_id.as_str(),
                None,
                self.config.max_command_output_chunks,
                self.config.process_drain_allowance,
            ) {
                if let (Some(barrier), Some(bridge)) = (barrier.as_ref(), wake_bridge.as_ref()) {
                    let _ = bridge.emit_process_completion(barrier, &completion.record);
                }
            }
            let _ =
                self.remove_active_process(task_id.as_str(), record.process_session_id.as_str());
            settle_command_status(
                self.commands.as_ref(),
                record.process_session_id.as_str(),
                CodingCommandLifecycleV2::Failed,
                None,
                Some("coding.process_monitor_spawn_failed".to_owned()),
            );
            CodingRuntimeError::Process(error.to_string())
        })?;
        Ok(handle)
    }

    fn start_pty_command(
        &self,
        task: &CodingTaskState,
        policy: &CodingCommandPolicyV2,
        objective_wait: Option<&CodingObjectiveWaitContextV2>,
    ) -> Result<CodingCommandHandleV2, CodingRuntimeError> {
        let wake_bridge = self.wake_bridge()?;
        if objective_wait.is_some() && wake_bridge.is_none() {
            return Err(CodingRuntimeError::WaitBridgeUnavailable);
        }
        let mut terminal = NativePtySession::spawn(
            PtyLaunchSpec {
                executable: policy.executable.clone(),
                args: policy.args.clone(),
                cwd: task.handle.workspace_root.clone(),
                env: policy.env.clone(),
                size: TerminalSizeV1::default(),
                max_raw_bytes: usize::try_from(policy.resource_units.spool_bytes)
                    .unwrap_or(usize::MAX)
                    .max(1),
                max_raw_chunks: self.config.max_command_output_chunks,
            },
            self.governor.clone(),
            ResourceLeaseRequestV1 {
                owner_id: task.handle.run_id.clone(),
                generation: task.handle.worktree_generation.unwrap_or(1),
                service: ResourceServiceKind::Pty,
                priority: ResourcePriority::Interactive,
                requested: policy.resource_units,
                duration: policy.timeout
                    + self.config.process_drain_allowance
                    + Duration::from_secs(30),
            },
        )
        .map_err(map_pty_error)?;
        let descriptor = terminal.session_descriptor();
        let barrier = match (objective_wait, wake_bridge.as_ref()) {
            (Some(context), Some(bridge)) => {
                match bridge.register_terminal_wait(context, &descriptor) {
                    Ok(barrier) => Some(barrier),
                    Err(error) => {
                        let _ = terminal.terminate_tree();
                        return Err(CodingRuntimeError::WaitBridge(error));
                    }
                }
            }
            _ => None,
        };
        let handle = CodingCommandHandleV2 {
            schema_version: CODING_RUNTIME_SCHEMA_VERSION,
            task_id: task.handle.task_id.clone(),
            command_id: policy.command_id.clone(),
            backend: CodingCommandBackendV2::NativePty,
            pty_backend: Some(descriptor.backend),
            execution_id: descriptor.pty_session_id.clone(),
            lifecycle: CodingCommandLifecycleV2::Running,
            wait_barrier: barrier.clone(),
            reason_codes: vec!["coding.terminal_running".to_owned()],
        };
        let terminal = Arc::new(Mutex::new(terminal));
        self.terminals
            .lock()
            .map_err(|_| CodingRuntimeError::StateUnavailable)?
            .insert(descriptor.pty_session_id.clone(), Arc::clone(&terminal));
        self.lock_tasks()?
            .get_mut(task.handle.task_id.as_str())
            .ok_or(CodingRuntimeError::TaskNotFound)?
            .active_terminals
            .insert(descriptor.pty_session_id.clone());
        self.commands.lock().map_err(|_| CodingRuntimeError::StateUnavailable)?.insert(
            descriptor.pty_session_id.clone(),
            CodingCommandStatusV2 {
                schema_version: CODING_RUNTIME_SCHEMA_VERSION,
                handle: handle.clone(),
                outcome: None,
                failure_reason_code: None,
            },
        );

        let commands = Arc::clone(&self.commands);
        let terminals = Arc::clone(&self.terminals);
        let tasks = Arc::clone(&self.tasks);
        let execution_id = descriptor.pty_session_id.clone();
        let timeout = policy.timeout;
        let max_chunks = self.config.max_command_output_chunks;
        let monitor_wake_bridge = wake_bridge.clone();
        self.spawn_completion_monitor("palyra-coding-terminal-monitor", move || {
            let deadline = Instant::now() + timeout;
            loop {
                if command_is_terminal(commands.as_ref(), execution_id.as_str()) {
                    return;
                }
                let settled = terminal
                    .lock()
                    .map_err(|_| CodingRuntimeError::StateUnavailable)
                    .and_then(|mut terminal| {
                        let exit = if Instant::now() >= deadline {
                            Some(terminal.terminate_with_outcome().map_err(map_pty_error)?)
                        } else {
                            terminal.try_settle().map_err(map_pty_error)?
                        };
                        Ok(exit.map(|exit| {
                            let descriptor = terminal.session_descriptor();
                            let output = terminal.poll_output(None, max_chunks);
                            (descriptor, exit, output)
                        }))
                    });
                match settled {
                    Ok(Some((descriptor, exit, output))) => {
                        finalize_terminal_command(
                            TerminalFinalizationContext {
                                commands: commands.as_ref(),
                                terminals: terminals.as_ref(),
                                tasks: tasks.as_ref(),
                                wake_bridge: monitor_wake_bridge.as_ref(),
                                execution_id: execution_id.as_str(),
                            },
                            &descriptor,
                            &exit,
                            output,
                        );
                        return;
                    }
                    Ok(None) => thread::sleep(Duration::from_millis(10)),
                    Err(error) => {
                        settle_command_status(
                            commands.as_ref(),
                            execution_id.as_str(),
                            CodingCommandLifecycleV2::Failed,
                            None,
                            Some(coding_error_reason(&error)),
                        );
                        remove_active_terminal_from_tasks(tasks.as_ref(), execution_id.as_str());
                        if let Ok(mut terminals) = terminals.lock() {
                            terminals.remove(execution_id.as_str());
                        }
                        return;
                    }
                }
            }
        })
        .map_err(|error| {
            let settled = self
                .terminals
                .lock()
                .ok()
                .and_then(|terminals| terminals.get(descriptor.pty_session_id.as_str()).cloned())
                .and_then(|terminal| {
                    terminal.lock().ok().and_then(|mut terminal| {
                        terminal.terminate_with_outcome().ok().map(|exit| {
                            let settled_descriptor = terminal.session_descriptor();
                            (settled_descriptor, exit)
                        })
                    })
                });
            if let Some((settled_descriptor, exit)) = settled {
                if let (Some(barrier), Some(bridge)) = (barrier.as_ref(), wake_bridge.as_ref()) {
                    let _ = bridge.emit_terminal_completion(barrier, &settled_descriptor, &exit);
                }
            }
            remove_active_terminal_from_tasks(
                self.tasks.as_ref(),
                descriptor.pty_session_id.as_str(),
            );
            if let Ok(mut terminals) = self.terminals.lock() {
                terminals.remove(descriptor.pty_session_id.as_str());
            }
            if let Ok(mut commands) = self.commands.lock() {
                if let Some(status) = commands.get_mut(descriptor.pty_session_id.as_str()) {
                    status.handle.lifecycle = CodingCommandLifecycleV2::Failed;
                    status.failure_reason_code =
                        Some("coding.terminal_monitor_spawn_failed".to_owned());
                }
            }
            CodingRuntimeError::Pty(error.to_string())
        })?;
        Ok(handle)
    }

    /// Applies one owner-fenced input action to a live native terminal.
    ///
    /// # Errors
    /// Returns an error for an unknown terminal, stale owner generation, or
    /// native I/O failure.
    pub fn terminal_input(
        &self,
        request: TerminalInputRequestV1,
    ) -> Result<(), CodingRuntimeError> {
        let terminal = self.terminal(request.pty_session_id.as_str())?;
        let result = terminal
            .lock()
            .map_err(|_| CodingRuntimeError::StateUnavailable)?
            .apply_input(request)
            .map_err(map_pty_error);
        result
    }

    /// Applies one owner-fenced resize to a live native terminal.
    ///
    /// # Errors
    /// Returns an error for an unknown terminal, stale owner generation, or
    /// native resize failure.
    pub fn terminal_resize(
        &self,
        request: TerminalResizeRequestV1,
    ) -> Result<(), CodingRuntimeError> {
        let terminal = self.terminal(request.pty_session_id.as_str())?;
        let result = terminal
            .lock()
            .map_err(|_| CodingRuntimeError::StateUnavailable)?
            .apply_resize(request)
            .map_err(map_pty_error);
        result
    }

    /// Polls bounded, escape-sanitized, secret-redacted terminal text.
    ///
    /// Raw chunks remain inside the terminal actor and never enter this
    /// serializable contract.
    ///
    /// # Errors
    /// Returns an error for an unknown terminal or unavailable actor state.
    pub fn terminal_output(
        &self,
        execution_id: &str,
        after_cursor: Option<u64>,
        max_chunks: usize,
    ) -> Result<CodingTerminalOutputV2, CodingRuntimeError> {
        let terminal = self.terminal(execution_id)?;
        let output = terminal
            .lock()
            .map_err(|_| CodingRuntimeError::StateUnavailable)?
            .poll_output(after_cursor, max_chunks);
        let (safe_text, redacted, redaction_reason_codes) =
            redact_process_output_projection(output.safe_text.as_str());
        Ok(CodingTerminalOutputV2 {
            safe_text,
            next_cursor: output.next_cursor,
            cursor_reset: output.cursor_reset,
            sanitization: output.sanitization,
            truncated: output.truncated,
            redacted,
            redaction_reason_codes,
        })
    }

    /// Polls redacted output for either a pipe-backed command or native terminal.
    ///
    /// # Errors
    /// Returns an error for an unknown execution or unavailable actor state.
    pub fn command_output(
        &self,
        execution_id: &str,
        after_cursor: Option<u64>,
        max_chunks: usize,
    ) -> Result<CodingTerminalOutputV2, CodingRuntimeError> {
        let status = self.command_status(execution_id)?;
        if status.handle.backend == CodingCommandBackendV2::NativePty {
            return self.terminal_output(execution_id, after_cursor, max_chunks);
        }
        let page =
            self.process.tail(execution_id, after_cursor, max_chunks).map_err(map_process_error)?;
        let redacted = page.chunks.iter().any(|chunk| chunk.redacted);
        let mut redaction_reason_codes = page
            .chunks
            .iter()
            .flat_map(|chunk| chunk.redaction_reason_codes.iter().cloned())
            .collect::<Vec<_>>();
        redaction_reason_codes.sort();
        redaction_reason_codes.dedup();
        Ok(CodingTerminalOutputV2 {
            safe_text: page.chunks.iter().map(|chunk| chunk.text_projection.as_str()).collect(),
            next_cursor: page.next_cursor,
            cursor_reset: page.cursor_reset,
            sanitization: TerminalSanitizationReportV1::default(),
            truncated: page.truncated,
            redacted,
            redaction_reason_codes,
        })
    }

    fn terminal(
        &self,
        execution_id: &str,
    ) -> Result<Arc<Mutex<NativePtySession>>, CodingRuntimeError> {
        self.terminals
            .lock()
            .map_err(|_| CodingRuntimeError::StateUnavailable)?
            .get(execution_id)
            .cloned()
            .ok_or_else(|| CodingRuntimeError::InvalidRequest("terminal was not found".to_owned()))
    }

    fn capability_report(
        &self,
        workspace_admission: CodingWorkspaceAdmissionV2,
        workspace_isolation: CodingWorkspaceIsolationV2,
        lsp_handle: Option<&LspServerHandleV2>,
    ) -> CodingRuntimeCapabilityReportV2 {
        let mut reason_codes = Vec::new();
        reason_codes.push(
            match workspace_admission {
                CodingWorkspaceAdmissionV2::Explicit => "coding.workspace_explicitly_selected",
                CodingWorkspaceAdmissionV2::Policy => "coding.workspace_policy_selected",
            }
            .to_owned(),
        );
        let workspace_fallback = workspace_isolation == CodingWorkspaceIsolationV2::InPlaceExplicit;
        if workspace_fallback {
            reason_codes.push("coding.workspace_in_place_explicit".to_owned());
        }
        let lsp_active = lsp_handle.is_some();
        if self.config.profile.persistent_lsp_enabled && !lsp_active {
            reason_codes.push("coding.lsp_cli_fallback_active".to_owned());
        }
        CodingRuntimeCapabilityReportV2 {
            schema_version: CODING_RUNTIME_SCHEMA_VERSION,
            workspace_admission,
            workspace_isolation,
            process_supervisor: CodingCapabilityStatusV2::Configured,
            native_pty: if self.config.profile.native_pty_enabled {
                CodingCapabilityStatusV2::Configured
            } else if self.config.profile.process_fallback_without_pty_allowed {
                CodingCapabilityStatusV2::Degraded
            } else {
                CodingCapabilityStatusV2::Disabled
            },
            persistent_lsp: if lsp_active {
                CodingCapabilityStatusV2::Active
            } else if self.config.profile.persistent_lsp_enabled
                && self.config.profile.cli_diagnostics_fallback_allowed
            {
                CodingCapabilityStatusV2::Degraded
            } else {
                CodingCapabilityStatusV2::Disabled
            },
            objective_wait_bridge: if self.wake_bridge().ok().flatten().is_some() {
                CodingCapabilityStatusV2::Configured
            } else {
                CodingCapabilityStatusV2::Disabled
            },
            fallback_matrix: vec![
                CodingFallbackEntryV2 {
                    capability: "managed_worktree".to_owned(),
                    fallback: self
                        .config
                        .profile
                        .in_place_workspace_fallback_allowed
                        .then(|| "in_place_workspace".to_owned()),
                    active: workspace_fallback,
                    reason_code: if workspace_fallback {
                        "coding.workspace_in_place_explicit"
                    } else {
                        "coding.managed_worktree_active"
                    }
                    .to_owned(),
                },
                CodingFallbackEntryV2 {
                    capability: "native_pty".to_owned(),
                    fallback: self
                        .config
                        .profile
                        .process_fallback_without_pty_allowed
                        .then(|| "process_without_pty".to_owned()),
                    active: !self.config.profile.native_pty_enabled,
                    reason_code: if self.config.profile.native_pty_enabled {
                        "coding.pty_configured"
                    } else {
                        "coding.pty_disabled_process_fallback"
                    }
                    .to_owned(),
                },
                CodingFallbackEntryV2 {
                    capability: "persistent_lsp".to_owned(),
                    fallback: self
                        .config
                        .profile
                        .cli_diagnostics_fallback_allowed
                        .then(|| "compiler_cli".to_owned()),
                    active: self.config.profile.persistent_lsp_enabled && !lsp_active,
                    reason_code: if lsp_active {
                        "coding.lsp_active"
                    } else {
                        "coding.lsp_cli_fallback_active"
                    }
                    .to_owned(),
                },
            ],
            reason_codes,
        }
    }

    fn rollback_begun_worktree(
        &self,
        worktree_id: Option<&str>,
        generation: Option<u64>,
        run_id: &str,
    ) {
        let (Some(worktree_id), Some(generation)) = (worktree_id, generation) else {
            return;
        };
        if let Ok(detached) = self.worktrees.detach_run(worktree_id, generation, run_id) {
            let _ = self.worktrees.remove(ManagedWorktreeRemoveRequestV2 {
                worktree_id: worktree_id.to_owned(),
                generation: detached.generation,
                snapshot_available: false,
            });
        }
    }

    fn task(&self, task_id: &str) -> Result<CodingTaskState, CodingRuntimeError> {
        self.lock_tasks()?.get(task_id).cloned().ok_or(CodingRuntimeError::TaskNotFound)
    }

    fn remove_active_process(
        &self,
        task_id: &str,
        process_session_id: &str,
    ) -> Result<(), CodingRuntimeError> {
        self.lock_tasks()?
            .get_mut(task_id)
            .ok_or(CodingRuntimeError::TaskNotFound)?
            .active_processes
            .remove(process_session_id);
        Ok(())
    }

    fn lock_tasks(
        &self,
    ) -> Result<MutexGuard<'_, BTreeMap<String, CodingTaskState>>, CodingRuntimeError> {
        self.tasks.lock().map_err(|_| CodingRuntimeError::StateUnavailable)
    }

    fn wake_bridge(&self) -> Result<Option<Arc<dyn CodingWakeBridge>>, CodingRuntimeError> {
        self.wake_bridge
            .read()
            .map(|bridge| bridge.clone())
            .map_err(|_| CodingRuntimeError::StateUnavailable)
    }

    fn spawn_completion_monitor(
        &self,
        name: &str,
        operation: impl FnOnce() + Send + 'static,
    ) -> Result<(), CodingRuntimeError> {
        let mut monitors =
            self.monitors.lock().map_err(|_| CodingRuntimeError::StateUnavailable)?;
        let mut retained = Vec::with_capacity(monitors.len());
        for monitor in monitors.drain(..) {
            if monitor.is_finished() {
                monitor.join().map_err(|_| {
                    CodingRuntimeError::Process("coding completion monitor panicked".to_owned())
                })?;
            } else {
                retained.push(monitor);
            }
        }
        *monitors = retained;
        let monitor_limit = self.config.max_tasks.saturating_mul(4).max(1);
        if monitors.len() >= monitor_limit {
            return Err(CodingRuntimeError::Process(
                "coding completion monitor capacity is exhausted".to_owned(),
            ));
        }
        let monitor = thread::Builder::new()
            .name(name.to_owned())
            .spawn(operation)
            .map_err(|error| CodingRuntimeError::Process(error.to_string()))?;
        monitors.push(monitor);
        Ok(())
    }
}

fn wait_process_completion(
    process: &ProcessSupervisor,
    process_session_id: &str,
    timeout: Duration,
    drain_allowance: Duration,
    max_chunks: usize,
) -> Result<ProcessCompletion, CodingRuntimeError> {
    match process.wait(process_session_id, None, max_chunks, timeout + drain_allowance) {
        Ok(completion) => Ok(completion),
        Err(ProcessSupervisorError::CommandTimeout) => {
            process.terminate(process_session_id).map_err(map_process_error)?;
            process
                .wait(process_session_id, None, max_chunks, drain_allowance)
                .map_err(map_process_error)
        }
        Err(error) => Err(map_process_error(error)),
    }
}

fn remove_active_process_from_tasks(
    tasks: &Mutex<BTreeMap<String, CodingTaskState>>,
    task_id: &str,
    execution_id: &str,
) {
    if let Ok(mut tasks) = tasks.lock() {
        if let Some(task) = tasks.get_mut(task_id) {
            task.active_processes.remove(execution_id);
        }
    }
}

fn remove_active_terminal_from_tasks(
    tasks: &Mutex<BTreeMap<String, CodingTaskState>>,
    execution_id: &str,
) {
    if let Ok(mut tasks) = tasks.lock() {
        for task in tasks.values_mut() {
            task.active_terminals.remove(execution_id);
        }
    }
}

fn command_is_terminal(
    commands: &Mutex<BTreeMap<String, CodingCommandStatusV2>>,
    execution_id: &str,
) -> bool {
    commands
        .lock()
        .ok()
        .and_then(|commands| commands.get(execution_id).cloned())
        .is_none_or(|status| status.handle.lifecycle != CodingCommandLifecycleV2::Running)
}

struct TerminalFinalizationContext<'a> {
    commands: &'a Mutex<BTreeMap<String, CodingCommandStatusV2>>,
    terminals: &'a Mutex<BTreeMap<String, Arc<Mutex<NativePtySession>>>>,
    tasks: &'a Mutex<BTreeMap<String, CodingTaskState>>,
    wake_bridge: Option<&'a Arc<dyn CodingWakeBridge>>,
    execution_id: &'a str,
}

fn finalize_terminal_command(
    context: TerminalFinalizationContext<'_>,
    descriptor: &super::pty_backend::PtySessionDescriptorV1,
    exit: &PtyExitOutcomeV1,
    output: PtyOutputPageV1,
) {
    let Some(handle) = context.commands.lock().ok().and_then(|commands| {
        commands.get(context.execution_id).map(|status| status.handle.clone())
    }) else {
        return;
    };
    let wake_result = match (handle.wait_barrier.as_ref(), context.wake_bridge) {
        (Some(barrier), Some(bridge)) => {
            bridge.emit_terminal_completion(barrier, descriptor, exit).map(Some)
        }
        _ => Ok(None),
    };
    let (wake, lifecycle, failure_reason) = match wake_result {
        Ok(wake) => (wake, CodingCommandLifecycleV2::Completed, None),
        Err(_) => (
            None,
            CodingCommandLifecycleV2::Failed,
            Some("coding.terminal_completion_wake_failed".to_owned()),
        ),
    };
    let outcome = project_pty_outcome(&handle, descriptor, exit, output, wake);
    settle_command_status(
        context.commands,
        context.execution_id,
        lifecycle,
        Some(outcome),
        failure_reason,
    );
    remove_active_terminal_from_tasks(context.tasks, context.execution_id);
    if let Ok(mut terminals) = context.terminals.lock() {
        terminals.remove(context.execution_id);
    }
}

fn settle_command_status(
    commands: &Mutex<BTreeMap<String, CodingCommandStatusV2>>,
    execution_id: &str,
    lifecycle: CodingCommandLifecycleV2,
    outcome: Option<CodingCommandOutcomeV2>,
    failure_reason_code: Option<String>,
) {
    if let Ok(mut commands) = commands.lock() {
        if let Some(status) = commands.get_mut(execution_id) {
            status.handle.lifecycle = lifecycle;
            status.outcome = outcome;
            status.failure_reason_code = failure_reason_code;
        }
    }
}

fn project_pty_outcome(
    handle: &CodingCommandHandleV2,
    descriptor: &super::pty_backend::PtySessionDescriptorV1,
    exit: &PtyExitOutcomeV1,
    output: PtyOutputPageV1,
    wake: Option<CodingWakeReceiptV2>,
) -> CodingCommandOutcomeV2 {
    CodingCommandOutcomeV2 {
        schema_version: CODING_RUNTIME_SCHEMA_VERSION,
        task_id: handle.task_id.clone(),
        command_id: handle.command_id.clone(),
        backend: handle.backend,
        pty_backend: Some(descriptor.backend),
        execution_id: descriptor.pty_session_id.clone(),
        process_state: None,
        exit_code: Some(i64::from(exit.exit_code)),
        cleanup_verified: exit.cleanup_verified,
        output_truncated: output.truncated,
        wake,
        evidence_refs: vec![
            format!("pty-session:{}", descriptor.pty_session_id),
            format!("pty-output-cursor:{}", output.next_cursor),
        ],
        reason_codes: vec![if exit.exit_code == 0 {
            "coding.pty_command_succeeded"
        } else {
            "coding.pty_command_failed"
        }
        .to_owned()],
    }
}

fn coding_error_reason(error: &CodingRuntimeError) -> String {
    match error {
        CodingRuntimeError::WaitBridge(_) => "coding.wait_bridge_failed",
        CodingRuntimeError::Pty(_) | CodingRuntimeError::PtyUnavailable => {
            "coding.pty_actor_failed"
        }
        CodingRuntimeError::Process(_) => "coding.process_actor_failed",
        _ => "coding.command_actor_failed",
    }
    .to_owned()
}

fn project_process_outcome(
    task: &CodingTaskState,
    policy: &CodingCommandPolicyV2,
    backend: CodingCommandBackendV2,
    degradation_reason: Option<&str>,
    completion: ProcessCompletion,
    wake: Option<CodingWakeReceiptV2>,
) -> CodingCommandOutcomeV2 {
    let outcome = completion.record.outcome.as_ref();
    let mut reason_codes = vec![if completion.record.state == ProcessSessionState::Succeeded {
        "coding.process_command_succeeded"
    } else {
        "coding.process_command_failed"
    }
    .to_owned()];
    if let Some(reason) = degradation_reason {
        reason_codes.push(reason.to_owned());
    }
    CodingCommandOutcomeV2 {
        schema_version: CODING_RUNTIME_SCHEMA_VERSION,
        task_id: task.handle.task_id.clone(),
        command_id: policy.command_id.clone(),
        backend,
        pty_backend: None,
        execution_id: completion.record.process_session_id.clone(),
        process_state: Some(completion.record.state),
        exit_code: outcome.and_then(|outcome| outcome.exit_code).map(i64::from),
        cleanup_verified: outcome.is_some_and(|outcome| outcome.cleanup_verified),
        output_truncated: completion.output.truncated,
        wake,
        evidence_refs: vec![
            format!("process-session:{}", completion.record.process_session_id),
            format!("process-output-cursor:{}", completion.output.next_cursor),
        ],
        reason_codes,
    }
}

fn validate_config(config: &CodingRuntimeConfig) -> Result<(), CodingRuntimeError> {
    if config.max_tasks == 0
        || config.max_patch_files == 0
        || config.max_source_file_bytes == 0
        || config.max_command_output_chunks == 0
        || config.process_drain_allowance.is_zero()
        || (!config.profile.managed_worktree_enabled
            && !config.profile.in_place_workspace_fallback_allowed)
    {
        return Err(CodingRuntimeError::InvalidConfiguration);
    }
    let mut command_ids = BTreeSet::new();
    for policy in &config.command_policies {
        validate_identity("command_id", policy.command_id.as_str())?;
        if !command_ids.insert(policy.command_id.as_str())
            || !policy.executable.is_absolute()
            || !policy.executable.is_file()
            || policy.args.len() > MAX_ARGUMENTS
            || policy.args.iter().any(|argument| argument.len() > MAX_ARGUMENT_BYTES)
            || policy.env.len() > MAX_ENVIRONMENT_KEYS
            || policy.timeout.is_zero()
            || policy.no_output_timeout.is_some_and(|timeout| timeout.is_zero())
            || policy.no_output_timeout.is_some_and(|timeout| timeout > policy.timeout)
            || policy.resource_units.processes == 0
            || policy.resource_units.is_zero()
        {
            return Err(CodingRuntimeError::InvalidConfiguration);
        }
    }
    Ok(())
}

fn executable_labels_match(requested: &str, configured: &str) -> bool {
    #[cfg(windows)]
    {
        fn normalized(label: &str) -> &str {
            [".exe", ".cmd", ".bat", ".com"]
                .into_iter()
                .find_map(|extension| {
                    label
                        .to_ascii_lowercase()
                        .ends_with(extension)
                        .then(|| &label[..label.len().saturating_sub(extension.len())])
                })
                .unwrap_or(label)
        }
        normalized(requested).eq_ignore_ascii_case(normalized(configured))
    }
    #[cfg(not(windows))]
    {
        requested == configured
    }
}

fn validate_identity(field: &str, value: &str) -> Result<(), CodingRuntimeError> {
    if value.trim().is_empty()
        || value.len() > MAX_IDENTITY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(CodingRuntimeError::InvalidRequest(format!(
            "{field} must be non-empty, bounded, and free of control characters"
        )));
    }
    Ok(())
}

fn normalize_patch_paths(
    workspace_root: &Path,
    relative_paths: &[PathBuf],
    max_paths: usize,
) -> Result<Vec<PathBuf>, CodingRuntimeError> {
    if relative_paths.is_empty() || relative_paths.len() > max_paths {
        return Err(CodingRuntimeError::InvalidRequest(
            "coding patch path count violates policy".to_owned(),
        ));
    }
    let mut normalized = BTreeSet::new();
    for path in relative_paths {
        if path.is_absolute()
            || path.components().any(|component| {
                !matches!(component, std::path::Component::Normal(_))
                    || component.as_os_str() == ".git"
            })
        {
            return Err(CodingRuntimeError::InvalidRequest(
                "coding patch paths must be safe workspace-relative paths".to_owned(),
            ));
        }
        let relative = path_to_slash(path)?;
        let absolute = workspace_root.join(path);
        reject_link_ancestors(workspace_root, absolute.as_path())?;
        if absolute.exists() {
            let canonical = absolute
                .canonicalize()
                .map_err(|error| CodingRuntimeError::Io(error.to_string()))?;
            if !canonical.starts_with(workspace_root) {
                return Err(CodingRuntimeError::InvalidRequest(
                    "coding patch path escapes the managed workspace".to_owned(),
                ));
            }
        }
        normalized.insert(relative);
    }
    Ok(normalized.into_iter().map(PathBuf::from).collect())
}

fn read_source_document(
    workspace_root: &Path,
    relative_path: &Path,
    max_bytes: usize,
) -> Result<String, CodingRuntimeError> {
    let absolute = workspace_root.join(relative_path);
    reject_link_ancestors(workspace_root, absolute.as_path())?;
    let metadata = match fs::symlink_metadata(absolute.as_path()) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
        Err(error) => return Err(CodingRuntimeError::Io(error.to_string())),
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > u64::try_from(max_bytes).unwrap_or(u64::MAX)
    {
        return Err(CodingRuntimeError::InvalidRequest(
            "LSP-managed source path violates file policy".to_owned(),
        ));
    }
    let canonical =
        absolute.canonicalize().map_err(|error| CodingRuntimeError::Io(error.to_string()))?;
    if !canonical.starts_with(workspace_root) {
        return Err(CodingRuntimeError::InvalidRequest(
            "LSP-managed source path escapes the managed workspace".to_owned(),
        ));
    }
    let bytes = fs::read(canonical).map_err(|error| CodingRuntimeError::Io(error.to_string()))?;
    String::from_utf8(bytes).map_err(|_| {
        CodingRuntimeError::InvalidRequest(
            "LSP-managed source files must contain valid UTF-8".to_owned(),
        )
    })
}

fn reject_link_ancestors(workspace_root: &Path, target: &Path) -> Result<(), CodingRuntimeError> {
    let relative = target.strip_prefix(workspace_root).map_err(|_| {
        CodingRuntimeError::InvalidRequest(
            "coding patch path escapes the managed workspace".to_owned(),
        )
    })?;
    let mut current = workspace_root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        let metadata = match fs::symlink_metadata(current.as_path()) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(CodingRuntimeError::Io(error.to_string())),
        };
        if metadata.file_type().is_symlink() {
            return Err(CodingRuntimeError::InvalidRequest(
                "coding patch path crosses a symbolic link".to_owned(),
            ));
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt as _;
            use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

            if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                return Err(CodingRuntimeError::InvalidRequest(
                    "coding patch path crosses a reparse point".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn path_to_slash(path: &Path) -> Result<String, CodingRuntimeError> {
    let mut segments = Vec::new();
    for component in path.components() {
        let std::path::Component::Normal(segment) = component else {
            return Err(CodingRuntimeError::InvalidRequest(
                "coding patch path is not normalized".to_owned(),
            ));
        };
        let segment = segment.to_str().ok_or_else(|| {
            CodingRuntimeError::InvalidRequest(
                "coding patch path must contain valid UTF-8".to_owned(),
            )
        })?;
        if segment.is_empty() || segment.chars().any(char::is_control) {
            return Err(CodingRuntimeError::InvalidRequest(
                "coding patch path contains an invalid segment".to_owned(),
            ));
        }
        segments.push(segment);
    }
    if segments.is_empty() {
        return Err(CodingRuntimeError::InvalidRequest(
            "coding patch path cannot be empty".to_owned(),
        ));
    }
    Ok(segments.join("/"))
}

fn canonical_workspace(path: &Path) -> Result<PathBuf, CodingRuntimeError> {
    if !path.is_absolute() || !path.is_dir() {
        return Err(CodingRuntimeError::InvalidRequest(
            "source workspace must be an existing absolute directory".to_owned(),
        ));
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|error| CodingRuntimeError::Io(error.to_string()))?;
    if metadata.file_type().is_symlink() {
        return Err(CodingRuntimeError::InvalidRequest(
            "source workspace cannot be a symbolic link".to_owned(),
        ));
    }
    path.canonicalize().map_err(|error| CodingRuntimeError::Io(error.to_string()))
}

const fn language_id(language: super::lsp_workspace_supervisor::LspLanguageV2) -> &'static str {
    match language {
        super::lsp_workspace_supervisor::LspLanguageV2::Rust => "rust",
        super::lsp_workspace_supervisor::LspLanguageV2::TypeScript => "typescript",
        super::lsp_workspace_supervisor::LspLanguageV2::Python => "python",
    }
}

fn map_worktree_error(error: ManagedWorktreeExecutorError) -> CodingRuntimeError {
    CodingRuntimeError::Worktree(error.to_string())
}

fn map_snapshot_error(error: WorktreeSnapshotError) -> CodingRuntimeError {
    CodingRuntimeError::Snapshot(error.to_string())
}

fn map_process_error(error: ProcessSupervisorError) -> CodingRuntimeError {
    CodingRuntimeError::Process(error.to_string())
}

fn map_pty_error(error: PtyBackendError) -> CodingRuntimeError {
    CodingRuntimeError::Pty(error.to_string())
}
