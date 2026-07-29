//! Durable host Git worktree executor for isolated coding runs.
//! Mutations are intent-recorded, path-confined, and executed through the
//! shared process supervisor with hooks and implicit file transport disabled.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::local_resource_governor::{ResourcePriority, ResourceUnitsV1};
use super::managed_worktrees::ManagedWorktreeRunAttachment;
use super::process_supervisor::{
    ProcessLaunchSpec, ProcessOutputStream, ProcessOwnerV2, ProcessSessionState, ProcessSupervisor,
    ProcessSupervisorError,
};

const MANAGED_WORKTREE_RECORD_SCHEMA_VERSION: u32 = 2;
const MANAGED_WORKTREE_REGISTRY_SCHEMA_VERSION: u32 = 2;
const MAX_WORKTREE_ID_BYTES: usize = 128;
const MAX_BRANCH_SLUG_BYTES: usize = 128;
const MAX_BASE_REF_BYTES: usize = 256;
const GIT_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Durable lifecycle of a host-managed Git worktree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedWorktreeLifecycleV2 {
    /// Intent is durable but Git creation has not settled.
    Creating,
    /// Worktree exists and may be attached.
    Active,
    /// Git removal is in progress.
    Removing,
    /// Dirty or leased worktree is retained for operator recovery.
    Retained,
    /// A host Git operation failed and requires reconciliation.
    Failed,
    /// Git and registry cleanup completed.
    Removed,
}

/// Versioned durable record for one managed worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorktreeRecordV2 {
    /// Record schema version.
    pub schema_version: u32,
    /// Host-issued worktree identity.
    pub worktree_id: String,
    /// Monotonic mutation generation.
    pub generation: u64,
    /// Canonical source repository path.
    pub source_repo: PathBuf,
    /// Canonical or intended managed worktree path.
    pub worktree_path: PathBuf,
    /// Dedicated managed branch.
    pub branch: String,
    /// Requested base ref.
    pub base_ref: String,
    /// Durable lifecycle.
    pub lifecycle: ManagedWorktreeLifecycleV2,
    /// Latest observed Git dirty state.
    pub dirty: bool,
    /// Run holding the exclusive mutation lock.
    pub locked_by_run: Option<String>,
    /// Attached run identities.
    pub attached_run_ids: Vec<String>,
    /// Creation timestamp.
    pub created_at_unix_ms: i64,
    /// Most recent durable mutation timestamp.
    pub updated_at_unix_ms: i64,
    /// Stable transition reason.
    pub reason_code: String,
}

/// Real worktree creation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedWorktreeCreateRequestV2 {
    /// Caller-selected bounded worktree identity.
    pub worktree_id: String,
    /// Existing source Git repository.
    pub source_repo: PathBuf,
    /// Human-readable branch slug normalized into the managed namespace.
    pub branch_slug: String,
    /// Git base ref resolved by the source repository.
    pub base_ref: String,
}

/// Real worktree removal policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedWorktreeRemoveRequestV2 {
    /// Worktree identity.
    pub worktree_id: String,
    /// Expected mutation generation.
    pub generation: u64,
    /// Whether a verified snapshot exists for dirty retention.
    pub snapshot_available: bool,
}

/// Bounded Git worktree status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedWorktreeStatusV2 {
    /// Current durable record.
    pub record: ManagedWorktreeRecordV2,
    /// Porcelain-v2 output, bounded by the process supervisor.
    pub porcelain_v2: String,
}

/// Durable executor policy and host paths.
#[derive(Debug, Clone)]
pub struct ManagedWorktreeExecutorConfig {
    /// Absolute durable registry path.
    pub registry_path: PathBuf,
    /// Absolute dedicated root outside source repositories.
    pub managed_root: PathBuf,
    /// Absolute trusted Git executable path.
    pub git_executable: PathBuf,
    /// Maximum retained worktree records.
    pub max_records: usize,
}

impl ManagedWorktreeExecutorConfig {
    fn validate(&self) -> Result<(), ManagedWorktreeExecutorError> {
        if !self.registry_path.is_absolute()
            || !self.managed_root.is_absolute()
            || !self.git_executable.is_absolute()
            || !self.git_executable.is_file()
            || self.max_records == 0
        {
            return Err(ManagedWorktreeExecutorError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Host Git, path safety, generation, or durable registry failure.
#[derive(Debug, Error)]
pub enum ManagedWorktreeExecutorError {
    /// Executor configuration is missing a trusted absolute path or bound.
    #[error("managed worktree executor configuration is invalid")]
    InvalidConfiguration,
    /// Request identity, branch, or base ref is malformed.
    #[error("managed worktree request is invalid: {0}")]
    InvalidRequest(String),
    /// Source path is not an existing canonical Git repository.
    #[error("managed worktree source repository is invalid")]
    InvalidSourceRepository,
    /// Managed root or target crosses a symlink, junction, or source boundary.
    #[error("managed worktree path violates containment policy")]
    UnsafePath,
    /// Worktree identity already exists.
    #[error("managed worktree already exists")]
    Duplicate,
    /// The dedicated managed branch already exists.
    #[error("managed worktree branch already exists")]
    BranchCollision,
    /// Worktree identity is unknown.
    #[error("managed worktree was not found")]
    NotFound,
    /// Mutation generation is stale.
    #[error("managed worktree generation does not match")]
    GenerationMismatch,
    /// Another run retains the worktree lock.
    #[error("managed worktree is locked by run {0}")]
    Locked(String),
    /// Dirty removal requires a verified snapshot.
    #[error("dirty managed worktree requires a verified snapshot before removal")]
    DirtyRequiresSnapshot,
    /// Durable registry reached its configured bound.
    #[error("managed worktree registry is full")]
    RegistryFull,
    /// Shared process execution failed.
    #[error("managed worktree Git process failed: {0}")]
    Process(String),
    /// Git returned a non-success exit.
    #[error("managed worktree Git command failed: {0}")]
    Git(String),
    /// Durable registry access failed.
    #[error("managed worktree persistence failed: {0}")]
    Persistence(String),
    /// In-memory executor state was poisoned.
    #[error("managed worktree executor state is unavailable")]
    StateUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedWorktreeRegistryV2 {
    schema_version: u32,
    updated_at_unix_ms: i64,
    records: BTreeMap<String, ManagedWorktreeRecordV2>,
}

/// Shared durable Git worktree host executor.
pub struct ManagedWorktreeExecutor {
    config: ManagedWorktreeExecutorConfig,
    process_supervisor: Arc<ProcessSupervisor>,
    registry: Mutex<ManagedWorktreeRegistryV2>,
}

impl ManagedWorktreeExecutor {
    /// Opens the durable registry and validates the managed root.
    ///
    /// # Errors
    /// Returns an error when configuration, path hardening, or registry decoding fails.
    pub fn open(
        config: ManagedWorktreeExecutorConfig,
        process_supervisor: Arc<ProcessSupervisor>,
    ) -> Result<Self, ManagedWorktreeExecutorError> {
        config.validate()?;
        create_private_dir(config.managed_root.as_path())?;
        reject_reparse_point(config.managed_root.as_path())?;
        let managed_root = config
            .managed_root
            .canonicalize()
            .map_err(|_| ManagedWorktreeExecutorError::UnsafePath)?;
        if let Some(parent) = config.registry_path.parent() {
            create_private_dir(parent)?;
        }
        let registry = if config.registry_path.exists() {
            read_registry(config.registry_path.as_path())?
        } else {
            let registry = ManagedWorktreeRegistryV2 {
                schema_version: MANAGED_WORKTREE_REGISTRY_SCHEMA_VERSION,
                updated_at_unix_ms: unix_time_ms(),
                records: BTreeMap::new(),
            };
            write_registry(config.registry_path.as_path(), &registry)?;
            registry
        };
        Ok(Self {
            config: ManagedWorktreeExecutorConfig { managed_root, ..config },
            process_supervisor,
            registry: Mutex::new(registry),
        })
    }

    /// Creates a dedicated Git worktree with a durable pre-mutation intent.
    ///
    /// # Errors
    /// Returns an error for invalid paths, duplicates, capacity, Git failure,
    /// or a registry transition that cannot be persisted.
    pub fn create(
        &self,
        request: ManagedWorktreeCreateRequestV2,
    ) -> Result<ManagedWorktreeRecordV2, ManagedWorktreeExecutorError> {
        validate_create_request(&request)?;
        let source_repo = canonical_source_repo(request.source_repo.as_path())?;
        if paths_overlap(source_repo.as_path(), self.config.managed_root.as_path()) {
            return Err(ManagedWorktreeExecutorError::UnsafePath);
        }
        let worktree_id = request.worktree_id.trim().to_owned();
        let worktree_path = self.config.managed_root.join(worktree_id.as_str());
        validate_new_target(self.config.managed_root.as_path(), worktree_path.as_path())?;
        let branch = managed_branch_name(request.branch_slug.as_str(), worktree_id.as_str())?;
        let existing_branch = self.run_git(
            vec![
                "-C".to_owned(),
                git_path_argument(source_repo.as_path()),
                "branch".to_owned(),
                "--list".to_owned(),
                "--format=%(refname)".to_owned(),
                "--".to_owned(),
                branch.clone(),
            ],
            source_repo.as_path(),
            worktree_id.as_str(),
            0,
        )?;
        if !existing_branch.trim().is_empty() {
            return Err(ManagedWorktreeExecutorError::BranchCollision);
        }
        let now = unix_time_ms();
        let mut record = ManagedWorktreeRecordV2 {
            schema_version: MANAGED_WORKTREE_RECORD_SCHEMA_VERSION,
            worktree_id: worktree_id.clone(),
            generation: 1,
            source_repo: source_repo.clone(),
            worktree_path: worktree_path.clone(),
            branch: branch.clone(),
            base_ref: request.base_ref.trim().to_owned(),
            lifecycle: ManagedWorktreeLifecycleV2::Creating,
            dirty: false,
            locked_by_run: None,
            attached_run_ids: Vec::new(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            reason_code: "managed_worktree.create_intent".to_owned(),
        };
        {
            let mut registry = self.lock_registry()?;
            if registry.records.contains_key(worktree_id.as_str()) {
                return Err(ManagedWorktreeExecutorError::Duplicate);
            }
            if registry.records.len() >= self.config.max_records {
                return Err(ManagedWorktreeExecutorError::RegistryFull);
            }
            registry.records.insert(worktree_id.clone(), record.clone());
            persist_registry(self.config.registry_path.as_path(), &registry)?;
        }
        let args = vec![
            "-C".to_owned(),
            git_path_argument(source_repo.as_path()),
            "-c".to_owned(),
            disabled_hooks_config(),
            "-c".to_owned(),
            "protocol.file.allow=never".to_owned(),
            "worktree".to_owned(),
            "add".to_owned(),
            "--no-track".to_owned(),
            "-b".to_owned(),
            branch.clone(),
            git_path_argument(worktree_path.as_path()),
            record.base_ref.clone(),
        ];
        match self.run_git(args, source_repo.as_path(), worktree_id.as_str(), record.generation) {
            Ok(_) => match canonical_managed_target(
                self.config.managed_root.as_path(),
                worktree_path.as_path(),
            ) {
                Ok(canonical_target) => {
                    record.worktree_path = canonical_target;
                    record.lifecycle = ManagedWorktreeLifecycleV2::Active;
                    record.updated_at_unix_ms = unix_time_ms();
                    record.reason_code = "managed_worktree.created".to_owned();
                    self.replace_record(record.clone())?;
                    Ok(record)
                }
                Err(error) => {
                    self.finish_failed_create(&mut record, branch.as_str());
                    Err(error)
                }
            },
            Err(error) => {
                self.finish_failed_create(&mut record, branch.as_str());
                Err(error)
            }
        }
    }

    /// Refreshes dirty state from `git status --porcelain=v2 -z`.
    ///
    /// # Errors
    /// Returns an error when the record, path, Git command, or durable update fails.
    pub fn status(
        &self,
        worktree_id: &str,
    ) -> Result<ManagedWorktreeStatusV2, ManagedWorktreeExecutorError> {
        let mut record = self.record(worktree_id)?;
        ensure_active_path(self.config.managed_root.as_path(), &record)?;
        let outcome = self.run_git(
            vec![
                "-C".to_owned(),
                git_path_argument(record.worktree_path.as_path()),
                "-c".to_owned(),
                disabled_hooks_config(),
                "status".to_owned(),
                "--porcelain=v2".to_owned(),
                "-z".to_owned(),
                "--untracked-files=normal".to_owned(),
            ],
            record.worktree_path.as_path(),
            worktree_id,
            record.generation,
        )?;
        record.dirty = !outcome.trim_matches('\0').trim().is_empty();
        record.updated_at_unix_ms = unix_time_ms();
        record.reason_code = "managed_worktree.status_refreshed".to_owned();
        self.replace_record(record.clone())?;
        Ok(ManagedWorktreeStatusV2 { record, porcelain_v2: outcome })
    }

    /// Attaches an existing run using generation-fenced exclusive locking.
    ///
    /// # Errors
    /// Returns an error when identity, generation, lock ownership, or persistence fails.
    pub fn attach_run(
        &self,
        worktree_id: &str,
        generation: u64,
        run_id: &str,
    ) -> Result<ManagedWorktreeRunAttachment, ManagedWorktreeExecutorError> {
        validate_identity("run_id", run_id, MAX_WORKTREE_ID_BYTES)?;
        let mut registry = self.lock_registry()?;
        let record =
            registry.records.get_mut(worktree_id).ok_or(ManagedWorktreeExecutorError::NotFound)?;
        if record.generation != generation {
            return Err(ManagedWorktreeExecutorError::GenerationMismatch);
        }
        if let Some(owner) = record.locked_by_run.as_deref() {
            if owner != run_id {
                return Err(ManagedWorktreeExecutorError::Locked(owner.to_owned()));
            }
        }
        if record.lifecycle != ManagedWorktreeLifecycleV2::Active {
            return Err(ManagedWorktreeExecutorError::UnsafePath);
        }
        if !record.attached_run_ids.iter().any(|attached| attached == run_id) {
            record.attached_run_ids.push(run_id.to_owned());
            record.attached_run_ids.sort();
        }
        record.locked_by_run = Some(run_id.to_owned());
        record.generation = record.generation.saturating_add(1);
        record.updated_at_unix_ms = unix_time_ms();
        record.reason_code = "managed_worktree.run_attached".to_owned();
        let attachment = ManagedWorktreeRunAttachment {
            worktree_id: record.worktree_id.clone(),
            run_id: run_id.to_owned(),
            workspace_root: record.worktree_path.clone(),
            workspace_policy: "worktree_scoped_rw".to_owned(),
            sandbox_mount: "explicit_rw_worktree_mount".to_owned(),
            reason_code: "managed_worktree.run_attached".to_owned(),
        };
        persist_registry(self.config.registry_path.as_path(), &registry)?;
        Ok(attachment)
    }

    /// Releases an exact run lock without removing attachment history.
    ///
    /// # Errors
    /// Returns an error when the record, generation, lock owner, or persistence fails.
    pub fn detach_run(
        &self,
        worktree_id: &str,
        generation: u64,
        run_id: &str,
    ) -> Result<ManagedWorktreeRecordV2, ManagedWorktreeExecutorError> {
        let mut registry = self.lock_registry()?;
        let record =
            registry.records.get_mut(worktree_id).ok_or(ManagedWorktreeExecutorError::NotFound)?;
        if record.generation != generation {
            return Err(ManagedWorktreeExecutorError::GenerationMismatch);
        }
        match record.locked_by_run.as_deref() {
            Some(owner) if owner == run_id => {}
            Some(owner) => return Err(ManagedWorktreeExecutorError::Locked(owner.to_owned())),
            None => {
                return Err(ManagedWorktreeExecutorError::InvalidRequest(
                    "managed worktree has no active run lock".to_owned(),
                ));
            }
        }
        record.locked_by_run = None;
        record.generation = record.generation.saturating_add(1);
        record.updated_at_unix_ms = unix_time_ms();
        record.reason_code = "managed_worktree.run_detached".to_owned();
        let detached = record.clone();
        persist_registry(self.config.registry_path.as_path(), &registry)?;
        Ok(detached)
    }

    /// Marks an unlocked worktree retained for operator recovery.
    ///
    /// # Errors
    /// Returns an error for stale generation, active lock, unsafe lifecycle,
    /// or durable registry failure.
    pub fn retain(
        &self,
        worktree_id: &str,
        generation: u64,
        reason_code: &str,
    ) -> Result<ManagedWorktreeRecordV2, ManagedWorktreeExecutorError> {
        validate_identity("reason_code", reason_code, MAX_BASE_REF_BYTES)?;
        let mut registry = self.lock_registry()?;
        let record =
            registry.records.get_mut(worktree_id).ok_or(ManagedWorktreeExecutorError::NotFound)?;
        if record.generation != generation {
            return Err(ManagedWorktreeExecutorError::GenerationMismatch);
        }
        if let Some(owner) = record.locked_by_run.as_ref() {
            return Err(ManagedWorktreeExecutorError::Locked(owner.clone()));
        }
        if record.lifecycle != ManagedWorktreeLifecycleV2::Active {
            return Err(ManagedWorktreeExecutorError::UnsafePath);
        }
        record.lifecycle = ManagedWorktreeLifecycleV2::Retained;
        record.generation = record.generation.saturating_add(1);
        record.updated_at_unix_ms = unix_time_ms();
        record.reason_code = reason_code.to_owned();
        let retained = record.clone();
        persist_registry(self.config.registry_path.as_path(), &registry)?;
        Ok(retained)
    }

    /// Removes a clean or snapshotted worktree through trusted Git.
    ///
    /// # Errors
    /// Returns an error for stale generations, locks, dirty unsnapshotted state,
    /// Git failure, unsafe paths, or durable transition failure.
    pub fn remove(
        &self,
        request: ManagedWorktreeRemoveRequestV2,
    ) -> Result<ManagedWorktreeRecordV2, ManagedWorktreeExecutorError> {
        let status = self.status(request.worktree_id.as_str())?;
        let mut record = status.record;
        if record.generation != request.generation {
            return Err(ManagedWorktreeExecutorError::GenerationMismatch);
        }
        if let Some(run_id) = record.locked_by_run.as_ref() {
            return Err(ManagedWorktreeExecutorError::Locked(run_id.clone()));
        }
        if record.dirty && !request.snapshot_available {
            record.lifecycle = ManagedWorktreeLifecycleV2::Retained;
            record.reason_code = "managed_worktree.dirty_retained".to_owned();
            record.updated_at_unix_ms = unix_time_ms();
            self.replace_record(record)?;
            return Err(ManagedWorktreeExecutorError::DirtyRequiresSnapshot);
        }
        record.lifecycle = ManagedWorktreeLifecycleV2::Removing;
        record.generation = record.generation.saturating_add(1);
        record.updated_at_unix_ms = unix_time_ms();
        record.reason_code = "managed_worktree.remove_intent".to_owned();
        self.replace_record(record.clone())?;
        self.run_git(
            vec![
                "-C".to_owned(),
                git_path_argument(record.source_repo.as_path()),
                "-c".to_owned(),
                disabled_hooks_config(),
                "worktree".to_owned(),
                "remove".to_owned(),
                if record.dirty { "--force".to_owned() } else { "--".to_owned() },
                git_path_argument(record.worktree_path.as_path()),
            ],
            record.source_repo.as_path(),
            record.worktree_id.as_str(),
            record.generation,
        )?;
        if record.worktree_path.exists() {
            return Err(ManagedWorktreeExecutorError::Git(
                "Git reported success but the managed worktree path remains".to_owned(),
            ));
        }
        record.lifecycle = ManagedWorktreeLifecycleV2::Removed;
        record.dirty = false;
        record.updated_at_unix_ms = unix_time_ms();
        record.reason_code = "managed_worktree.removed".to_owned();
        self.replace_record(record.clone())?;
        Ok(record)
    }

    /// Lists durable worktree records in stable identity order.
    ///
    /// # Errors
    /// Returns an error when in-memory registry state is unavailable.
    pub fn list(&self) -> Result<Vec<ManagedWorktreeRecordV2>, ManagedWorktreeExecutorError> {
        Ok(self.lock_registry()?.records.values().cloned().collect())
    }

    pub(crate) fn record(
        &self,
        worktree_id: &str,
    ) -> Result<ManagedWorktreeRecordV2, ManagedWorktreeExecutorError> {
        self.lock_registry()?
            .records
            .get(worktree_id)
            .cloned()
            .ok_or(ManagedWorktreeExecutorError::NotFound)
    }

    fn replace_record(
        &self,
        record: ManagedWorktreeRecordV2,
    ) -> Result<(), ManagedWorktreeExecutorError> {
        let mut registry = self.lock_registry()?;
        registry.records.insert(record.worktree_id.clone(), record);
        persist_registry(self.config.registry_path.as_path(), &registry)
    }

    fn finish_failed_create(&self, record: &mut ManagedWorktreeRecordV2, branch: &str) {
        let cleanup_verified = self.cleanup_failed_create(record, branch);
        record.lifecycle = ManagedWorktreeLifecycleV2::Failed;
        record.updated_at_unix_ms = unix_time_ms();
        record.reason_code = if cleanup_verified {
            "managed_worktree.create_failed_cleanup_verified".to_owned()
        } else {
            "managed_worktree.create_failed_cleanup_required".to_owned()
        };
        let _ = self.replace_record(record.clone());
    }

    fn cleanup_failed_create(&self, record: &ManagedWorktreeRecordV2, branch: &str) -> bool {
        if path_metadata(record.worktree_path.as_path()).ok().flatten().is_some() {
            if canonical_managed_target(
                self.config.managed_root.as_path(),
                record.worktree_path.as_path(),
            )
            .is_err()
            {
                return false;
            }
            let _ = self.run_git(
                vec![
                    "-C".to_owned(),
                    git_path_argument(record.source_repo.as_path()),
                    "-c".to_owned(),
                    disabled_hooks_config(),
                    "worktree".to_owned(),
                    "remove".to_owned(),
                    "--force".to_owned(),
                    "--".to_owned(),
                    git_path_argument(record.worktree_path.as_path()),
                ],
                record.source_repo.as_path(),
                record.worktree_id.as_str(),
                record.generation,
            );
        }
        if path_metadata(record.worktree_path.as_path()).ok().flatten().is_some() {
            return false;
        }
        let _ = self.run_git(
            vec![
                "-C".to_owned(),
                git_path_argument(record.source_repo.as_path()),
                "-c".to_owned(),
                disabled_hooks_config(),
                "branch".to_owned(),
                "-D".to_owned(),
                "--".to_owned(),
                branch.to_owned(),
            ],
            record.source_repo.as_path(),
            record.worktree_id.as_str(),
            record.generation,
        );
        true
    }

    pub(crate) fn run_git(
        &self,
        args: Vec<String>,
        cwd: &Path,
        worktree_id: &str,
        generation: u64,
    ) -> Result<String, ManagedWorktreeExecutorError> {
        let timeout = GIT_OPERATION_TIMEOUT;
        let record = self
            .process_supervisor
            .launch(ProcessLaunchSpec {
                executable: self.config.git_executable.clone(),
                args,
                cwd: cwd.to_path_buf(),
                env: git_environment(),
                owner: ProcessOwnerV2 {
                    session_id: "managed-worktree".to_owned(),
                    run_id: format!("worktree-{worktree_id}"),
                    turn_id: "host-git".to_owned(),
                    agent_id: "managed-worktree-executor".to_owned(),
                    correlation_id: format!("worktree-{worktree_id}-{generation}"),
                },
                timeout,
                no_output_timeout: None,
                lease_duration: timeout + Duration::from_secs(30),
                resource_priority: ResourcePriority::Foreground,
                resource_service: super::local_resource_governor::ResourceServiceKind::Worktree,
                resource_units: ResourceUnitsV1 {
                    processes: 1,
                    memory_bytes: 256 * 1024 * 1024,
                    file_descriptors: 16,
                    sockets: 0,
                    spool_bytes: 2 * 1024 * 1024,
                    concurrency: 1,
                },
            })
            .map_err(map_process_error)?;
        let completion = self
            .process_supervisor
            .wait(record.process_session_id.as_str(), None, 256, timeout + Duration::from_secs(10))
            .map_err(map_process_error)?;
        if completion.output.truncated {
            return Err(ManagedWorktreeExecutorError::Git(
                "Git output exceeded the authoritative process spool".to_owned(),
            ));
        }
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut cursor = None;
        loop {
            let page = self
                .process_supervisor
                .tail_raw(record.process_session_id.as_str(), cursor, 256)
                .map_err(map_process_error)?;
            if page.cursor_reset || page.truncated {
                return Err(ManagedWorktreeExecutorError::Git(
                    "Git output exceeded the authoritative process spool".to_owned(),
                ));
            }
            for chunk in page.chunks {
                cursor = Some(chunk.sequence);
                match chunk.stream {
                    ProcessOutputStream::Stdout => stdout.extend_from_slice(chunk.bytes.as_slice()),
                    ProcessOutputStream::Stderr => stderr.extend_from_slice(chunk.bytes.as_slice()),
                }
            }
            if !page.has_more {
                break;
            }
            if cursor != Some(page.last_returned_cursor) {
                return Err(ManagedWorktreeExecutorError::Git(
                    "Git output cursor did not advance".to_owned(),
                ));
            }
        }
        let stdout = String::from_utf8(stdout).map_err(|_| {
            ManagedWorktreeExecutorError::Git(
                "Git machine protocol returned a non-UTF-8 path".to_owned(),
            )
        })?;
        let stderr = String::from_utf8(stderr).map_err(|_| {
            ManagedWorktreeExecutorError::Git(
                "Git diagnostic output was not valid UTF-8".to_owned(),
            )
        })?;
        if completion.record.state != ProcessSessionState::Succeeded {
            let detail = bounded_git_detail(if stderr.trim().is_empty() {
                stdout.as_str()
            } else {
                stderr.as_str()
            });
            return Err(ManagedWorktreeExecutorError::Git(detail));
        }
        Ok(stdout)
    }

    fn lock_registry(
        &self,
    ) -> Result<MutexGuard<'_, ManagedWorktreeRegistryV2>, ManagedWorktreeExecutorError> {
        self.registry.lock().map_err(|_| ManagedWorktreeExecutorError::StateUnavailable)
    }
}

fn validate_create_request(
    request: &ManagedWorktreeCreateRequestV2,
) -> Result<(), ManagedWorktreeExecutorError> {
    validate_identity("worktree_id", request.worktree_id.as_str(), MAX_WORKTREE_ID_BYTES)?;
    validate_identity("branch_slug", request.branch_slug.as_str(), MAX_BRANCH_SLUG_BYTES)?;
    validate_identity("base_ref", request.base_ref.as_str(), MAX_BASE_REF_BYTES)?;
    if request
        .worktree_id
        .chars()
        .any(|character| !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_'))
    {
        return Err(ManagedWorktreeExecutorError::InvalidRequest(
            "worktree_id may contain only ASCII alphanumerics, dash, and underscore".to_owned(),
        ));
    }
    Ok(())
}

fn validate_identity(
    field: &str,
    value: &str,
    max_bytes: usize,
) -> Result<(), ManagedWorktreeExecutorError> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(ManagedWorktreeExecutorError::InvalidRequest(format!(
            "{field} must be non-empty, bounded, and free of control characters"
        )));
    }
    Ok(())
}

fn canonical_source_repo(path: &Path) -> Result<PathBuf, ManagedWorktreeExecutorError> {
    reject_reparse_point(path)?;
    let canonical =
        path.canonicalize().map_err(|_| ManagedWorktreeExecutorError::InvalidSourceRepository)?;
    if !canonical.join(".git").exists() {
        return Err(ManagedWorktreeExecutorError::InvalidSourceRepository);
    }
    Ok(canonical)
}

fn validate_new_target(
    managed_root: &Path,
    target: &Path,
) -> Result<(), ManagedWorktreeExecutorError> {
    if path_metadata(target)?.is_some()
        || target.parent() != Some(managed_root)
        || target.components().any(|component| matches!(component, Component::ParentDir))
    {
        return Err(ManagedWorktreeExecutorError::UnsafePath);
    }
    reject_reparse_point(managed_root)
}

fn path_metadata(path: &Path) -> Result<Option<fs::Metadata>, ManagedWorktreeExecutorError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ManagedWorktreeExecutorError::UnsafePath),
    }
}

fn canonical_managed_target(
    managed_root: &Path,
    target: &Path,
) -> Result<PathBuf, ManagedWorktreeExecutorError> {
    reject_reparse_point(target)?;
    let canonical = target.canonicalize().map_err(|_| ManagedWorktreeExecutorError::UnsafePath)?;
    if canonical.parent() != Some(managed_root) {
        return Err(ManagedWorktreeExecutorError::UnsafePath);
    }
    Ok(canonical)
}

fn ensure_active_path(
    managed_root: &Path,
    record: &ManagedWorktreeRecordV2,
) -> Result<(), ManagedWorktreeExecutorError> {
    if record.lifecycle != ManagedWorktreeLifecycleV2::Active
        && record.lifecycle != ManagedWorktreeLifecycleV2::Retained
    {
        return Err(ManagedWorktreeExecutorError::UnsafePath);
    }
    let canonical = canonical_managed_target(managed_root, record.worktree_path.as_path())?;
    if canonical != record.worktree_path {
        return Err(ManagedWorktreeExecutorError::UnsafePath);
    }
    Ok(())
}

fn reject_reparse_point(path: &Path) -> Result<(), ManagedWorktreeExecutorError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| ManagedWorktreeExecutorError::UnsafePath)?;
    if metadata.file_type().is_symlink() {
        return Err(ManagedWorktreeExecutorError::UnsafePath);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(ManagedWorktreeExecutorError::UnsafePath);
        }
    }
    Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn managed_branch_name(
    slug: &str,
    worktree_id: &str,
) -> Result<String, ManagedWorktreeExecutorError> {
    let normalized = slug
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches(['-', '_'])
        .to_owned();
    if normalized.is_empty() {
        return Err(ManagedWorktreeExecutorError::InvalidRequest(
            "branch slug normalizes to an empty value".to_owned(),
        ));
    }
    Ok(format!("palyra/{normalized}-{worktree_id}"))
}

#[cfg(windows)]
fn git_path_argument(path: &Path) -> String {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let lowercase = normalized.to_ascii_lowercase();
    if lowercase.starts_with("//?/unc/") {
        format!("//{}", &normalized[8..])
    } else if lowercase.starts_with("//?/") || lowercase.starts_with("//./") {
        normalized[4..].to_owned()
    } else {
        normalized
    }
}

#[cfg(not(windows))]
fn git_path_argument(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn disabled_hooks_config() -> String {
    #[cfg(windows)]
    {
        "core.hooksPath=NUL".to_owned()
    }
    #[cfg(not(windows))]
    {
        "core.hooksPath=/dev/null".to_owned()
    }
}

fn git_environment() -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for key in ["HOME", "SYSTEMROOT", "TEMP", "TMP", "USERPROFILE"] {
        if let Ok(value) = std::env::var(key) {
            env.insert(key.to_owned(), value);
        }
    }
    env.insert("GIT_CONFIG_NOSYSTEM".to_owned(), "1".to_owned());
    env.insert("GIT_TERMINAL_PROMPT".to_owned(), "0".to_owned());
    env
}

fn bounded_git_detail(value: &str) -> String {
    let detail = value.trim().chars().take(1_024).collect::<String>();
    if detail.is_empty() {
        "Git command exited unsuccessfully without diagnostic output".to_owned()
    } else {
        detail
    }
}

fn map_process_error(error: ProcessSupervisorError) -> ManagedWorktreeExecutorError {
    ManagedWorktreeExecutorError::Process(error.to_string())
}

fn read_registry(path: &Path) -> Result<ManagedWorktreeRegistryV2, ManagedWorktreeExecutorError> {
    let bytes = fs::read(path)
        .map_err(|error| ManagedWorktreeExecutorError::Persistence(error.to_string()))?;
    let registry = serde_json::from_slice::<ManagedWorktreeRegistryV2>(bytes.as_slice())
        .map_err(|error| ManagedWorktreeExecutorError::Persistence(error.to_string()))?;
    if registry.schema_version != MANAGED_WORKTREE_REGISTRY_SCHEMA_VERSION {
        return Err(ManagedWorktreeExecutorError::Persistence(
            "unsupported managed worktree registry schema".to_owned(),
        ));
    }
    Ok(registry)
}

fn persist_registry(
    path: &Path,
    registry: &ManagedWorktreeRegistryV2,
) -> Result<(), ManagedWorktreeExecutorError> {
    let mut snapshot = registry.clone();
    snapshot.updated_at_unix_ms = unix_time_ms();
    write_registry(path, &snapshot)
}

fn write_registry(
    path: &Path,
    registry: &ManagedWorktreeRegistryV2,
) -> Result<(), ManagedWorktreeExecutorError> {
    let payload = serde_json::to_vec_pretty(registry)
        .map_err(|error| ManagedWorktreeExecutorError::Persistence(error.to_string()))?;
    atomic_replace(path, payload.as_slice())
}

fn atomic_replace(path: &Path, payload: &[u8]) -> Result<(), ManagedWorktreeExecutorError> {
    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".tmp.{}.{}", std::process::id(), timestamp_ns));
    let temporary_path = PathBuf::from(temporary_name);
    fs::write(temporary_path.as_path(), payload)
        .map_err(|error| ManagedWorktreeExecutorError::Persistence(error.to_string()))?;
    harden_file(temporary_path.as_path())?;
    if let Err(rename_error) = fs::rename(temporary_path.as_path(), path) {
        if !path.is_file() {
            let _ = fs::remove_file(temporary_path.as_path());
            return Err(ManagedWorktreeExecutorError::Persistence(rename_error.to_string()));
        }
        // Keep a rollback copy until the replacement is installed because
        // Windows rename does not replace an existing open destination.
        let mut rollback_name = path.as_os_str().to_os_string();
        rollback_name.push(format!(".swap.{}.{}", std::process::id(), timestamp_ns));
        let rollback_path = PathBuf::from(rollback_name);
        fs::rename(path, rollback_path.as_path())
            .map_err(|error| ManagedWorktreeExecutorError::Persistence(error.to_string()))?;
        if let Err(install_error) = fs::rename(temporary_path.as_path(), path) {
            let _ = fs::rename(rollback_path.as_path(), path);
            let _ = fs::remove_file(temporary_path.as_path());
            return Err(ManagedWorktreeExecutorError::Persistence(install_error.to_string()));
        }
        let _ = fs::remove_file(rollback_path);
    }
    harden_file(path)
}

fn create_private_dir(path: &Path) -> Result<(), ManagedWorktreeExecutorError> {
    fs::create_dir_all(path)
        .map_err(|error| ManagedWorktreeExecutorError::Persistence(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| ManagedWorktreeExecutorError::Persistence(error.to_string()))?;
    }
    Ok(())
}

fn harden_file(path: &Path) -> Result<(), ManagedWorktreeExecutorError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| ManagedWorktreeExecutorError::Persistence(error.to_string()))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn unix_time_ms() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::managed_worktree_test_support::ManagedWorktreeTestFixture;
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn creates_real_isolated_worktree_and_reports_dirty_status() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("primary");
        assert_eq!(record.lifecycle, ManagedWorktreeLifecycleV2::Active);
        assert_eq!(record.worktree_path.parent(), Some(fixture.managed_root.as_path()));
        assert!(record.worktree_path.join(".git").is_file());
        fs::write(record.worktree_path.join("tracked.txt"), b"changed\n").expect("edit worktree");
        let status = fixture.executor.status("primary").expect("worktree status");
        assert!(status.record.dirty);
        assert!(!status.porcelain_v2.is_empty());
    }

    #[test]
    fn concurrent_create_has_one_winner_and_one_duplicate() {
        let fixture = ManagedWorktreeTestFixture::new();
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let executor = Arc::clone(&fixture.executor);
            let source_repo = fixture.source_repo.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                executor.create(ManagedWorktreeCreateRequestV2 {
                    worktree_id: "concurrent".to_owned(),
                    source_repo,
                    branch_slug: "concurrent".to_owned(),
                    base_ref: "HEAD".to_owned(),
                })
            }));
        }
        barrier.wait();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().expect("join create worker"))
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    matches!(
                        result,
                        Err(ManagedWorktreeExecutorError::Duplicate
                            | ManagedWorktreeExecutorError::BranchCollision)
                    )
                })
                .count(),
            1
        );
    }

    #[test]
    fn branch_collision_fails_before_durable_create_intent() {
        let fixture = ManagedWorktreeTestFixture::new();
        fixture.git(fixture.source_repo.as_path(), &["branch", "palyra/collision-collision"]);
        let error = fixture
            .executor
            .create(ManagedWorktreeCreateRequestV2 {
                worktree_id: "collision".to_owned(),
                source_repo: fixture.source_repo.clone(),
                branch_slug: "collision".to_owned(),
                base_ref: "HEAD".to_owned(),
            })
            .expect_err("reject branch collision");
        assert!(matches!(error, ManagedWorktreeExecutorError::BranchCollision));
        assert!(fixture.executor.list().expect("list records").is_empty());
    }

    #[test]
    fn live_lock_is_generation_fenced_and_scopes_the_workspace_mount() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("locked");
        let attachment = fixture
            .executor
            .attach_run("locked", record.generation, "run-a")
            .expect("attach owner");
        assert_eq!(attachment.workspace_root, record.worktree_path);
        assert_eq!(attachment.workspace_policy, "worktree_scoped_rw");
        let attached = fixture.executor.record("locked").expect("attached record");
        let stale_error = fixture
            .executor
            .detach_run("locked", record.generation, "run-a")
            .expect_err("reject stale generation");
        assert!(matches!(stale_error, ManagedWorktreeExecutorError::GenerationMismatch));
        let lock_error = fixture
            .executor
            .attach_run("locked", attached.generation, "run-b")
            .expect_err("reject foreign lock");
        assert!(matches!(lock_error, ManagedWorktreeExecutorError::Locked(_)));
        let detached = fixture
            .executor
            .detach_run("locked", attached.generation, "run-a")
            .expect("detach owner");
        assert!(detached.locked_by_run.is_none());
    }

    #[test]
    fn dirty_remove_retains_until_a_verified_snapshot_is_available() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("retained");
        fs::write(record.worktree_path.join("tracked.txt"), b"dirty\n").expect("dirty worktree");
        let error = fixture
            .executor
            .remove(ManagedWorktreeRemoveRequestV2 {
                worktree_id: "retained".to_owned(),
                generation: record.generation,
                snapshot_available: false,
            })
            .expect_err("retain dirty worktree");
        assert!(matches!(error, ManagedWorktreeExecutorError::DirtyRequiresSnapshot));
        let retained = fixture.executor.record("retained").expect("retained record");
        assert_eq!(retained.lifecycle, ManagedWorktreeLifecycleV2::Retained);
        let removed = fixture
            .executor
            .remove(ManagedWorktreeRemoveRequestV2 {
                worktree_id: "retained".to_owned(),
                generation: retained.generation,
                snapshot_available: true,
            })
            .expect("remove snapshotted worktree");
        assert_eq!(removed.lifecycle, ManagedWorktreeLifecycleV2::Removed);
        assert!(!record.worktree_path.exists());
    }

    #[test]
    fn failed_create_is_durable_and_leaves_no_managed_path() {
        let fixture = ManagedWorktreeTestFixture::new();
        let error = fixture
            .executor
            .create(ManagedWorktreeCreateRequestV2 {
                worktree_id: "failed".to_owned(),
                source_repo: fixture.source_repo.clone(),
                branch_slug: "failed".to_owned(),
                base_ref: "refs/heads/does-not-exist".to_owned(),
            })
            .expect_err("fail invalid base");
        assert!(matches!(error, ManagedWorktreeExecutorError::Git(_)));
        let record = fixture.executor.record("failed").expect("failed record");
        assert_eq!(record.lifecycle, ManagedWorktreeLifecycleV2::Failed);
        assert_eq!(record.reason_code, "managed_worktree.create_failed_cleanup_verified");
        assert!(!record.worktree_path.exists());
    }

    #[test]
    fn registry_survives_executor_restart() {
        let fixture = ManagedWorktreeTestFixture::new();
        let created = fixture.create_worktree("restart");
        let reopened = ManagedWorktreeExecutor::open(
            ManagedWorktreeExecutorConfig {
                registry_path: fixture.worktree_registry_path.clone(),
                managed_root: fixture.managed_root.clone(),
                git_executable: fixture.git_executable.clone(),
                max_records: 64,
            },
            Arc::clone(&fixture.supervisor),
        )
        .expect("reopen worktree registry");
        assert_eq!(reopened.record("restart").expect("recovered record"), created);
    }

    #[test]
    fn registry_rejects_unknown_versions_and_fields_without_rewrite() {
        let mutators: [fn(&mut serde_json::Value); 2] = [
            |value| value["schema_version"] = serde_json::json!(999),
            |value| value["unknown_registry_field"] = serde_json::json!(true),
        ];
        for mutate in mutators {
            let fixture = ManagedWorktreeTestFixture::new();
            fixture.create_worktree("contract");
            let path = fixture.worktree_registry_path.clone();
            let mut value: serde_json::Value =
                serde_json::from_slice(fs::read(path.as_path()).expect("read registry").as_slice())
                    .expect("decode registry");
            mutate(&mut value);
            let bytes = serde_json::to_vec_pretty(&value).expect("encode invalid registry");
            fs::write(path.as_path(), bytes.as_slice()).expect("write invalid registry");

            assert!(matches!(
                read_registry(path.as_path()),
                Err(ManagedWorktreeExecutorError::Persistence(_))
            ));
            assert_eq!(fs::read(path.as_path()).expect("read unchanged registry"), bytes);
        }
    }

    #[test]
    fn dangling_link_target_is_rejected_before_git_mutation() {
        let fixture = ManagedWorktreeTestFixture::new();
        let target = fixture.managed_root.join("linked");
        if !create_dangling_link(target.as_path()) {
            return;
        }
        let error = fixture
            .executor
            .create(ManagedWorktreeCreateRequestV2 {
                worktree_id: "linked".to_owned(),
                source_repo: fixture.source_repo.clone(),
                branch_slug: "linked".to_owned(),
                base_ref: "HEAD".to_owned(),
            })
            .expect_err("reject linked target");
        assert!(matches!(error, ManagedWorktreeExecutorError::UnsafePath));
    }

    #[cfg(unix)]
    fn create_dangling_link(path: &Path) -> bool {
        std::os::unix::fs::symlink(path.with_extension("missing"), path).is_ok()
    }

    #[cfg(windows)]
    fn create_dangling_link(path: &Path) -> bool {
        std::os::windows::fs::symlink_file(path.with_extension("missing"), path).is_ok()
    }
}
