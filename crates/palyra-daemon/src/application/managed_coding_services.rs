//! Daemon-owned composition root for managed coding process, worktree, and
//! language-service authorities.

use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::coding_runtime::{
    CodingCommandPolicyV2, CodingExecutionProfileV2, CodingRuntime, CodingRuntimeConfig,
    CodingRuntimeError,
};
use super::local_resource_governor::{
    LocalResourceGovernor, LocalResourceGovernorConfig, ResourcePressureActionStateV1,
    ResourcePressureActionV1, ResourceServiceKind, ResourceUnitsV1,
};
use super::lsp_document_sync::{
    LspDocumentCoordinator, LspDocumentSyncConfig, LspDocumentSyncError,
};
use super::lsp_workspace_supervisor::{
    LspServerCommandPolicyV2, LspWorkspaceSupervisor, LspWorkspaceSupervisorConfig,
    LspWorkspaceSupervisorError,
};
use super::managed_coding_diagnostics::{
    ManagedCodingDiagnosticsV1, ManagedCodingPressureActionV1, ManagedCodingPressureDecisionV1,
    ManagedCodingPressureDiagnosticsV1, ManagedCodingResourceDiagnosticsV1,
    ManagedCodingWorktreeDiagnosticsV1,
};
use super::managed_worktree_executor::{
    ManagedWorktreeExecutor, ManagedWorktreeExecutorConfig, ManagedWorktreeExecutorError,
    ManagedWorktreeLifecycleV2, ManagedWorktreeRecordV2,
};
use super::managed_worktree_snapshots::{
    SnapshotGcDecisionV1, WorktreeRestoreReportV1, WorktreeSnapshotDescriptorV1,
    WorktreeSnapshotError, WorktreeSnapshotStore, WorktreeSnapshotStoreConfig,
};
use super::process_supervisor::{
    ProcessSupervisor, ProcessSupervisorConfig, ProcessSupervisorError,
};

/// Host paths and policies used to build one daemon-wide managed coding stack.
#[derive(Clone)]
pub struct ManagedCodingServicesConfig {
    /// Absolute owner-only state root.
    pub state_root: PathBuf,
    /// Absolute managed worktree root.
    pub managed_worktree_root: PathBuf,
    /// Absolute trusted Git executable.
    pub git_executable: PathBuf,
    /// Effective execution and fallback posture.
    pub profile: CodingExecutionProfileV2,
    /// Fixed command policies selectable by coding orchestration.
    pub command_policies: Vec<CodingCommandPolicyV2>,
    /// Fixed language-server policies selected by language only.
    pub lsp_policies: Vec<LspServerCommandPolicyV2>,
    /// Host evidence that denied-network language servers are isolated.
    pub lsp_network_isolation_verified: bool,
    /// Idle language-server eviction threshold.
    pub lsp_idle_ttl: Duration,
}

/// Construction or orderly shutdown failure for managed coding services.
#[derive(Debug, Error)]
pub enum ManagedCodingServicesError {
    /// Host state or executable policy is invalid.
    #[error("managed coding services configuration is invalid")]
    InvalidConfiguration,
    /// Resource governor initialization failed.
    #[error("managed coding resource governor failed: {0}")]
    ResourceGovernor(String),
    /// Process authority initialization or shutdown failed.
    #[error("managed coding process supervisor failed: {0}")]
    Process(String),
    /// Worktree executor initialization failed.
    #[error("managed coding worktree executor failed: {0}")]
    Worktree(String),
    /// Snapshot service initialization failed.
    #[error("managed coding snapshot service failed: {0}")]
    Snapshot(String),
    /// Language server initialization or shutdown failed.
    #[error("managed coding language service failed: {0}")]
    LanguageService(String),
    /// Integrated runtime initialization or shutdown failed.
    #[error("managed coding runtime failed: {0}")]
    Runtime(String),
}

/// Shared daemon lifecycle owner for the complete managed coding stack.
pub struct ManagedCodingRuntimeServices {
    runtime: Arc<CodingRuntime>,
    process: Arc<ProcessSupervisor>,
    governor: LocalResourceGovernor,
    worktrees: Arc<ManagedWorktreeExecutor>,
    snapshots: Arc<WorktreeSnapshotStore>,
    lsp: Option<Arc<LspWorkspaceSupervisor>>,
}

/// Resolves an executable only from absolute `PATH` entries and returns its
/// canonical host path. Relative search directories are ignored because daemon
/// startup must not trust its current directory as executable authority.
#[must_use]
pub(crate) fn resolve_trusted_executable(name: &str) -> Option<PathBuf> {
    if name.trim().is_empty()
        || name.contains(std::path::MAIN_SEPARATOR)
        || name.contains('/')
        || name.contains('\\')
    {
        return None;
    }
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path).filter(|directory| directory.is_absolute()) {
        #[cfg(windows)]
        let candidates = {
            let mut candidates = vec![directory.join(name)];
            let extensions =
                std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_owned());
            candidates.extend(
                extensions.split(';').map(str::trim).filter(|extension| !extension.is_empty()).map(
                    |extension| directory.join(format!("{name}{}", extension.to_ascii_lowercase())),
                ),
            );
            candidates
        };
        #[cfg(not(windows))]
        let candidates = vec![directory.join(name)];
        for candidate in candidates {
            if candidate.is_file() {
                if let Ok(canonical) = candidate.canonicalize() {
                    return Some(canonical);
                }
            }
        }
    }
    None
}

/// Resolves either an absolute configured executable or a basename through
/// the trusted absolute `PATH` search.
#[must_use]
pub(crate) fn resolve_trusted_configured_executable(raw: &str) -> Option<PathBuf> {
    let raw = raw.trim();
    if raw.is_empty() || raw.chars().any(char::is_control) {
        return None;
    }
    let configured = Path::new(raw);
    if configured.is_absolute() {
        return configured.is_file().then(|| configured.canonicalize().ok()).flatten();
    }
    if configured.components().count() != 1 {
        return None;
    }
    resolve_trusted_executable(raw)
}

/// Hashes a trusted executable for language-service cache identity.
///
/// # Errors
/// Returns an error when the executable cannot be read completely.
pub(crate) fn executable_fingerprint(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex::encode(hasher.finalize()))
}

impl ManagedCodingRuntimeServices {
    /// Shares the single daemon resource authority with bounded WorkGraph admission.
    pub(crate) fn resource_governor(&self) -> LocalResourceGovernor {
        self.governor.clone()
    }

    /// Opens every service under one bounded resource and lifecycle authority.
    ///
    /// # Errors
    /// Returns an error when any host path, policy, durable registry, or
    /// process authority cannot be initialized safely.
    pub fn open(config: ManagedCodingServicesConfig) -> Result<Self, ManagedCodingServicesError> {
        if !config.state_root.is_absolute()
            || !config.managed_worktree_root.is_absolute()
            || !config.git_executable.is_absolute()
            || !config.git_executable.is_file()
            || config.lsp_idle_ttl.is_zero()
        {
            return Err(ManagedCodingServicesError::InvalidConfiguration);
        }
        let limits = service_resource_limit();
        let governor = LocalResourceGovernor::open(LocalResourceGovernorConfig {
            registry_path: config.state_root.join("resource-leases.json"),
            global_limit: limits,
            per_owner_limit: limits,
            max_records: 4_096,
        })
        .map_err(|error| ManagedCodingServicesError::ResourceGovernor(error.to_string()))?;
        let process = Arc::new(
            ProcessSupervisor::start(ProcessSupervisorConfig {
                state_root: config.state_root.clone(),
                max_sessions: 64,
                max_retained_chunks_per_session: 512,
                max_retained_bytes_per_session: 4 * 1024 * 1024,
                max_artifact_bytes_per_session: 16 * 1024 * 1024,
                drain_timeout: Duration::from_secs(5),
                resource_governor: governor.clone(),
            })
            .map_err(map_process_error)?,
        );
        let worktrees = Arc::new(
            ManagedWorktreeExecutor::open(
                ManagedWorktreeExecutorConfig {
                    registry_path: config.state_root.join("worktrees.json"),
                    managed_root: config.managed_worktree_root,
                    git_executable: config.git_executable,
                    max_records: 512,
                },
                Arc::clone(&process),
            )
            .map_err(map_worktree_error)?,
        );
        let snapshots = Arc::new(
            WorktreeSnapshotStore::open(
                WorktreeSnapshotStoreConfig {
                    artifact_root: config.state_root.join("worktree-snapshots"),
                    max_files: 512,
                    max_file_bytes: 8 * 1024 * 1024,
                    max_total_bytes: 64 * 1024 * 1024,
                },
                Arc::clone(&worktrees),
                governor.clone(),
            )
            .map_err(map_snapshot_error)?,
        );
        let (lsp, documents) = if config.lsp_policies.is_empty() {
            (None, None)
        } else {
            let lsp = Arc::new(
                LspWorkspaceSupervisor::open(
                    LspWorkspaceSupervisorConfig {
                        registry_path: config.state_root.join("lsp-registry.json"),
                        max_servers: 16,
                        max_registry_entries: 512,
                        max_header_bytes: 8 * 1024,
                        max_message_bytes: 1024 * 1024,
                        max_notifications: 512,
                        initialize_timeout: Duration::from_secs(10),
                        request_timeout: Duration::from_secs(10),
                        server_lifetime: Duration::from_secs(8 * 60 * 60),
                        idle_ttl: config.lsp_idle_ttl,
                        broken_ttl: Duration::from_secs(60),
                        circuit_breaker_failures: 3,
                        network_isolation_verified: config.lsp_network_isolation_verified,
                        resource_units: language_service_resources(),
                        policies: config.lsp_policies,
                    },
                    Arc::clone(&process),
                )
                .map_err(map_lsp_error)?,
            );
            let documents = Arc::new(
                LspDocumentCoordinator::open(
                    LspDocumentSyncConfig {
                        artifact_root: config.state_root.join("diagnostics"),
                        artifact_owner_id: "managed-coding-runtime".to_owned(),
                        max_documents: 512,
                        max_document_bytes: 8 * 1024 * 1024,
                        max_diagnostics_per_document: 2_048,
                        max_visible_delta_items: 128,
                        max_artifact_bytes: 16 * 1024 * 1024,
                        max_artifacts: 4_096,
                        diagnostics_timeout: Duration::from_secs(10),
                    },
                    Arc::clone(&lsp),
                )
                .map_err(map_document_error)?,
            );
            (Some(lsp), Some(documents))
        };
        let runtime = Arc::new(
            CodingRuntime::open(
                CodingRuntimeConfig {
                    profile: config.profile,
                    max_tasks: 64,
                    max_patch_files: 128,
                    max_source_file_bytes: 8 * 1024 * 1024,
                    max_command_output_chunks: 512,
                    process_drain_allowance: Duration::from_secs(5),
                    command_policies: config.command_policies,
                },
                Arc::clone(&process),
                governor.clone(),
                Arc::clone(&worktrees),
                Some(Arc::clone(&snapshots)),
                lsp.clone(),
                documents,
                None,
            )
            .map_err(map_runtime_error)?,
        );
        Ok(Self { runtime, process, governor, worktrees, snapshots, lsp })
    }

    /// Returns the single integrated runtime owned by this service root.
    #[must_use]
    pub fn runtime(&self) -> &Arc<CodingRuntime> {
        &self.runtime
    }

    /// Lists bounded durable worktree records for operator recovery.
    ///
    /// # Errors
    /// Returns an error when the durable registry is unavailable.
    pub fn worktree_records(
        &self,
    ) -> Result<Vec<ManagedWorktreeRecordV2>, ManagedCodingServicesError> {
        self.worktrees.list().map_err(map_worktree_error)
    }

    /// Lists durable snapshot descriptors without reading artifact payloads.
    ///
    /// # Errors
    /// Returns an error when the owner-only snapshot registry is unavailable.
    pub fn snapshot_descriptors(
        &self,
    ) -> Result<Vec<WorktreeSnapshotDescriptorV1>, ManagedCodingServicesError> {
        self.snapshots.list_descriptors().map_err(map_snapshot_error)
    }

    /// Loads one bounded snapshot descriptor for operator inspection.
    ///
    /// # Errors
    /// Returns an error when the identity is invalid, absent, or corrupt.
    pub fn snapshot_descriptor(
        &self,
        snapshot_id: &str,
    ) -> Result<WorktreeSnapshotDescriptorV1, ManagedCodingServicesError> {
        self.snapshots.load(snapshot_id).map_err(map_snapshot_error)
    }

    /// Restores one exact dirty-worktree snapshot after base and conflict validation.
    ///
    /// # Errors
    /// Returns an error when integrity, base, ownership, or path checks fail.
    pub fn restore_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<WorktreeRestoreReportV1, ManagedCodingServicesError> {
        self.snapshots.restore(snapshot_id).map_err(map_snapshot_error)
    }

    /// Applies generation-fenced operator retention to an unlocked active worktree.
    ///
    /// # Errors
    /// Returns an error for stale generations, live locks, or unsafe lifecycle state.
    pub fn retain_worktree(
        &self,
        worktree_id: &str,
        generation: u64,
    ) -> Result<ManagedWorktreeRecordV2, ManagedCodingServicesError> {
        self.worktrees
            .retain(worktree_id, generation, "coding.operator_retained")
            .map_err(map_worktree_error)
    }

    /// Reconciles an interrupted run only after all managed-service leases are absent.
    ///
    /// # Errors
    /// Returns an error for an active owner, mismatched run, or failed snapshot/cleanup.
    pub fn reconcile_interrupted_worktree(
        &self,
        worktree_id: &str,
        run_id: &str,
    ) -> Result<super::coding_runtime::CodingTaskCleanupOutcomeV2, ManagedCodingServicesError> {
        self.runtime.reconcile_interrupted_task(worktree_id, run_id).map_err(map_runtime_error)
    }

    /// Garbage-collects one snapshot while preserving active owner and process fences.
    ///
    /// # Errors
    /// Returns an error when the snapshot is absent, active, or cannot be removed safely.
    pub fn gc_snapshot(
        &self,
        snapshot_id: &str,
        force: bool,
    ) -> Result<SnapshotGcDecisionV1, ManagedCodingServicesError> {
        self.snapshots.gc(snapshot_id, force).map_err(map_snapshot_error)
    }

    /// Applies the safe subset of a deterministic pressure plan owned by this stack.
    ///
    /// Idle language servers are restartable and have an exact supervisor-owned
    /// process handle. Other service classes remain untouched because a resource
    /// lease alone never grants cleanup authority.
    ///
    /// # Errors
    /// Returns an error when pressure evaluation or evidence persistence fails.
    pub fn relieve_pressure(
        &self,
        required: ResourceUnitsV1,
    ) -> Result<Vec<ResourcePressureActionV1>, ManagedCodingServicesError> {
        let pressure = self
            .governor
            .pressure_snapshot(required)
            .map_err(|error| ManagedCodingServicesError::ResourceGovernor(error.to_string()))?;
        if pressure.capacity_available {
            return Ok(Vec::new());
        }
        let mut actions = Vec::new();
        for decision in pressure.eviction_plan {
            let outcome = if decision.service == ResourceServiceKind::Lsp {
                self.evict_pressure_selected_lsp(decision.owner_id.as_str())
            } else {
                None
            };
            let (state, reason_code) = match outcome {
                Some(Ok(())) => {
                    (ResourcePressureActionStateV1::Applied, "resource.lsp_eviction_applied")
                }
                Some(Err(())) => {
                    (ResourcePressureActionStateV1::Failed, "resource.lsp_eviction_failed")
                }
                None => {
                    (ResourcePressureActionStateV1::Skipped, "resource.pressure_action_unowned")
                }
            };
            let action = self
                .governor
                .record_pressure_action(decision, state, reason_code)
                .map_err(|error| ManagedCodingServicesError::ResourceGovernor(error.to_string()))?;
            actions.push(action);
            if self
                .governor
                .pressure_snapshot(required)
                .map_err(|error| ManagedCodingServicesError::ResourceGovernor(error.to_string()))?
                .capacity_available
            {
                break;
            }
        }
        Ok(actions)
    }

    fn evict_pressure_selected_lsp(&self, owner_id: &str) -> Option<Result<(), ()>> {
        let supervisor = self.lsp.as_ref()?;
        let health = supervisor.health().ok()?;
        let handle = health.handles.into_iter().find(|handle| {
            format!("worktree-{}", handle.worktree_id) == owner_id
                && matches!(
                    handle.lifecycle,
                    super::lsp_workspace_supervisor::LspServerLifecycleV2::Starting
                        | super::lsp_workspace_supervisor::LspServerLifecycleV2::Ready
                )
        })?;
        Some(supervisor.evict(handle.handle_id.as_str()).map(|_| ()).map_err(|_| ()))
    }

    /// Returns a redacted, bounded health projection for operator diagnostics.
    #[must_use]
    pub fn diagnostics_snapshot(&self) -> ManagedCodingDiagnosticsV1 {
        let capabilities = self.runtime.active_capability_reports().unwrap_or_default();
        let resources = self.governor.snapshot();
        let (last_pressure, pressure_actions) =
            self.governor.pressure_evidence().unwrap_or_default();
        let pressure = last_pressure.map(|pressure| {
            let eviction_plan = pressure
                .eviction_plan
                .into_iter()
                .map(|decision| ManagedCodingPressureDecisionV1 {
                    lease_id_sha256: sha256_identity(decision.lease_id.as_str()),
                    owner_id_sha256: sha256_identity(decision.owner_id.as_str()),
                    service: decision.service,
                    priority: decision.priority,
                    released: decision.released,
                    reason_code: decision.reason_code,
                })
                .collect();
            ManagedCodingPressureDiagnosticsV1 {
                schema_version: 1,
                required: pressure.required,
                capacity_available: pressure.capacity_available,
                eviction_plan,
                reason_code: pressure.reason_code,
                observed_at_unix_ms: pressure.observed_at_unix_ms,
            }
        });
        let pressure_actions = pressure_actions
            .into_iter()
            .map(|action| ManagedCodingPressureActionV1 {
                schema_version: 1,
                lease_id_sha256: sha256_identity(action.decision.lease_id.as_str()),
                owner_id_sha256: sha256_identity(action.decision.owner_id.as_str()),
                service: action.decision.service,
                priority: action.decision.priority,
                released: action.decision.released,
                state: action.state,
                reason_code: action.reason_code,
                observed_at_unix_ms: action.observed_at_unix_ms,
            })
            .collect();
        let worktree_records = self.worktrees.list().unwrap_or_default();
        let snapshots = self.snapshots.list().unwrap_or_default();
        let active_worktrees = worktree_records
            .iter()
            .filter(|record| {
                matches!(
                    record.lifecycle,
                    ManagedWorktreeLifecycleV2::Creating
                        | ManagedWorktreeLifecycleV2::Active
                        | ManagedWorktreeLifecycleV2::Retained
                        | ManagedWorktreeLifecycleV2::Failed
                )
            })
            .count();
        let dirty_worktrees = worktree_records.iter().filter(|record| record.dirty).count();
        let locked_worktrees =
            worktree_records.iter().filter(|record| record.locked_by_run.is_some()).count();
        let lsp = self.lsp.as_ref().and_then(|supervisor| supervisor.diagnostics_health().ok());
        ManagedCodingDiagnosticsV1 {
            schema_version: 1,
            status: "available".to_owned(),
            active_tasks: capabilities.len(),
            capabilities,
            resources: ManagedCodingResourceDiagnosticsV1 {
                used: resources.used,
                limit: resources.limit,
                active_leases: resources.active_leases,
                owner_count: resources.owner_usage.len(),
            },
            pressure,
            pressure_actions,
            worktrees: ManagedCodingWorktreeDiagnosticsV1 {
                active: active_worktrees,
                dirty: dirty_worktrees,
                locked: locked_worktrees,
                retained_snapshots: snapshots.len(),
            },
            language_services: lsp,
            reason_code: "coding.runtime_available".to_owned(),
        }
    }

    /// Settles tasks, language servers, and process trees in dependency order.
    ///
    /// # Errors
    /// Returns the first failure after attempting every shutdown layer.
    pub fn shutdown(&self) -> Result<(), ManagedCodingServicesError> {
        let mut first_error = self.runtime.shutdown().err().map(map_runtime_error);
        if let Some(lsp) = self.lsp.as_ref() {
            if let Err(error) = lsp.shutdown() {
                if first_error.is_none() {
                    first_error = Some(map_lsp_error(error));
                }
            }
        }
        if let Err(error) = self.process.shutdown() {
            if first_error.is_none() {
                first_error = Some(map_process_error(error));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn sha256_identity(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

impl Drop for ManagedCodingRuntimeServices {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

const fn service_resource_limit() -> ResourceUnitsV1 {
    ResourceUnitsV1 {
        processes: 64,
        memory_bytes: 16 * 1024 * 1024 * 1024,
        file_descriptors: 8_192,
        sockets: 256,
        spool_bytes: 512 * 1024 * 1024,
        concurrency: 256,
    }
}

const fn language_service_resources() -> ResourceUnitsV1 {
    ResourceUnitsV1 {
        processes: 1,
        memory_bytes: 1024 * 1024 * 1024,
        file_descriptors: 256,
        sockets: 0,
        spool_bytes: 16 * 1024 * 1024,
        concurrency: 1,
    }
}

fn map_process_error(error: ProcessSupervisorError) -> ManagedCodingServicesError {
    ManagedCodingServicesError::Process(error.to_string())
}

fn map_worktree_error(error: ManagedWorktreeExecutorError) -> ManagedCodingServicesError {
    ManagedCodingServicesError::Worktree(error.to_string())
}

fn map_snapshot_error(error: WorktreeSnapshotError) -> ManagedCodingServicesError {
    ManagedCodingServicesError::Snapshot(error.to_string())
}

fn map_lsp_error(error: LspWorkspaceSupervisorError) -> ManagedCodingServicesError {
    ManagedCodingServicesError::LanguageService(error.to_string())
}

fn map_document_error(error: LspDocumentSyncError) -> ManagedCodingServicesError {
    ManagedCodingServicesError::LanguageService(error.to_string())
}

fn map_runtime_error(error: CodingRuntimeError) -> ManagedCodingServicesError {
    ManagedCodingServicesError::Runtime(error.to_string())
}
