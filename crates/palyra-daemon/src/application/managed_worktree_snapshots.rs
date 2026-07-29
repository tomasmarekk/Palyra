//! Byte-exact dirty worktree snapshots and generation-safe restore.
//! Git index bytes and allowed changed files are stored in owner-only local
//! artifacts; ignored files, symlinks, and nested repositories fail closed.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use palyra_safety::{
    inspect_text, SafetyContentKind, SafetyFindingCategory, SafetyPhase, SafetySourceKind,
    TrustLabel,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::local_resource_governor::{LocalResourceGovernor, ResourceServiceKind};
use super::managed_worktree_executor::{
    ManagedWorktreeExecutor, ManagedWorktreeExecutorError, ManagedWorktreeLifecycleV2,
    ManagedWorktreeRecordV2,
};

const WORKTREE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const WORKTREE_RESTORE_REPORT_SCHEMA_VERSION: u32 = 1;
const MAX_SNAPSHOT_ID_BYTES: usize = 128;

/// Origin of a path retained by a worktree snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPathKindV1 {
    /// Path is present in the Git index or base tree.
    Tracked,
    /// Path is untracked but not ignored by repository policy.
    AllowedUntracked,
}

/// Byte-exact snapshot entry for one changed worktree path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorktreeSnapshotEntryV1 {
    /// Normalized repository-relative UTF-8 path.
    pub path: String,
    /// Tracked or policy-allowed untracked origin.
    pub kind: SnapshotPathKindV1,
    /// Whether the path existed in the working tree at capture.
    pub exists: bool,
    /// Byte count when the path existed.
    pub size_bytes: u64,
    /// SHA-256 digest when the path existed.
    pub content_sha256: Option<String>,
    /// Snapshot-local content filename when the path existed.
    pub artifact_file: Option<String>,
    /// Unix file mode when available.
    pub unix_mode: Option<u32>,
    /// Cross-platform readonly bit.
    pub readonly: bool,
}

/// Durable descriptor for a lossless dirty worktree snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorktreeSnapshotDescriptorV1 {
    /// Descriptor schema version.
    pub schema_version: u32,
    /// Host-issued snapshot identity.
    pub snapshot_id: String,
    /// Managed worktree identity.
    pub worktree_id: String,
    /// Worktree mutation generation at capture.
    pub worktree_generation: u64,
    /// Exact base commit.
    pub base_commit: String,
    /// SHA-256 of the complete Git index bytes.
    pub index_sha256: String,
    /// Snapshot-local Git index filename.
    pub index_artifact_file: String,
    /// Changed and allowed-untracked entries.
    pub entries: Vec<WorktreeSnapshotEntryV1>,
    /// Total captured file and index bytes.
    pub total_bytes: u64,
    /// Capture timestamp.
    pub created_at_unix_ms: i64,
}

/// Verified restore result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorktreeRestoreReportV1 {
    /// Report schema version.
    pub schema_version: u32,
    /// Restored snapshot identity.
    pub snapshot_id: String,
    /// Managed worktree identity.
    pub worktree_id: String,
    /// Exact base commit validated before mutation.
    pub base_commit: String,
    /// Restored repository-relative paths.
    pub restored_paths: Vec<String>,
    /// Whether the exact Git index bytes were restored.
    pub index_restored: bool,
    /// Completion timestamp.
    pub completed_at_unix_ms: i64,
}

/// Snapshot retention decision made by orphan cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotGcDecisionV1 {
    /// Snapshot was deleted from owner-only local storage.
    Removed,
    /// Active process, PTY, LSP, MCP, or external runtime lease blocks cleanup.
    BlockedByActiveLease,
    /// Worktree lock or non-forced policy retained the snapshot.
    Retained,
}

/// Snapshot artifact policy.
#[derive(Debug, Clone)]
pub struct WorktreeSnapshotStoreConfig {
    /// Absolute owner-only snapshot root.
    pub artifact_root: PathBuf,
    /// Maximum changed paths per snapshot.
    pub max_files: usize,
    /// Maximum bytes for one file or Git index.
    pub max_file_bytes: u64,
    /// Maximum total bytes per snapshot.
    pub max_total_bytes: u64,
}

impl WorktreeSnapshotStoreConfig {
    fn validate(&self) -> Result<(), WorktreeSnapshotError> {
        if !self.artifact_root.is_absolute()
            || self.max_files == 0
            || self.max_file_bytes == 0
            || self.max_total_bytes == 0
            || self.max_file_bytes > self.max_total_bytes
        {
            return Err(WorktreeSnapshotError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Snapshot capture, validation, restore, or cleanup failure.
#[derive(Debug, Error)]
pub enum WorktreeSnapshotError {
    /// Artifact bounds or path configuration is invalid.
    #[error("worktree snapshot configuration is invalid")]
    InvalidConfiguration,
    /// Worktree is not active or retained.
    #[error("managed worktree cannot be snapshotted in its current lifecycle")]
    InvalidWorktreeState,
    /// Git command failed.
    #[error("worktree snapshot Git command failed: {0}")]
    Git(String),
    /// Changed path is non-UTF-8, unsafe, ignored, nested, or symlinked.
    #[error("worktree snapshot path is unsafe: {0}")]
    UnsafePath(String),
    /// File count, per-file bytes, or total bytes exceed policy.
    #[error("worktree snapshot exceeds configured bounds")]
    BoundsExceeded,
    /// A changed path, file, or Git index contains sensitive material.
    #[error("worktree snapshot contains sensitive material")]
    SensitiveMaterial,
    /// Snapshot identity is malformed or unknown.
    #[error("worktree snapshot was not found")]
    NotFound,
    /// Current base commit differs from the snapshot base.
    #[error("worktree snapshot base commit does not match current HEAD")]
    BaseMismatch,
    /// Current dirty paths include changes not represented by the snapshot.
    #[error("worktree restore would overwrite unrelated dirty paths")]
    RestoreConflict,
    /// Artifact digest does not match the descriptor.
    #[error("worktree snapshot artifact integrity check failed")]
    Integrity,
    /// Active resource or run ownership blocks garbage collection.
    #[error("worktree snapshot cleanup is blocked by active ownership")]
    ActiveOwnership,
    /// Artifact storage failed.
    #[error("worktree snapshot storage failed: {0}")]
    Storage(String),
}

/// Owner-only snapshot, restore, inspection, and GC service.
pub struct WorktreeSnapshotStore {
    config: WorktreeSnapshotStoreConfig,
    executor: Arc<ManagedWorktreeExecutor>,
    resource_governor: LocalResourceGovernor,
}

impl WorktreeSnapshotStore {
    /// Opens hardened snapshot storage.
    ///
    /// # Errors
    /// Returns an error when policy or artifact-root hardening fails.
    pub fn open(
        config: WorktreeSnapshotStoreConfig,
        executor: Arc<ManagedWorktreeExecutor>,
        resource_governor: LocalResourceGovernor,
    ) -> Result<Self, WorktreeSnapshotError> {
        config.validate()?;
        create_private_dir(config.artifact_root.as_path())?;
        reject_link(config.artifact_root.as_path())?;
        let artifact_root = config
            .artifact_root
            .canonicalize()
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
        Ok(Self {
            config: WorktreeSnapshotStoreConfig { artifact_root, ..config },
            executor,
            resource_governor,
        })
    }

    /// Captures exact index bytes and all tracked/allowed-untracked dirty paths.
    ///
    /// # Errors
    /// Returns an error for Git failure, unsafe paths, sensitive material,
    /// nested repositories, policy bounds, or non-durable artifacts.
    pub fn capture(
        &self,
        worktree_id: &str,
    ) -> Result<WorktreeSnapshotDescriptorV1, WorktreeSnapshotError> {
        let record = self.executor.record(worktree_id).map_err(map_executor_error)?;
        validate_worktree_state(&record)?;
        let base_commit = self
            .git(&record, vec!["rev-parse".to_owned(), "--verify".to_owned(), "HEAD".to_owned()])?
            .trim()
            .to_owned();
        if base_commit.len() != 40 && base_commit.len() != 64 {
            return Err(WorktreeSnapshotError::Git(
                "Git returned an invalid base commit".to_owned(),
            ));
        }
        let tracked = self.changed_tracked_paths(&record)?;
        let untracked = self.changed_untracked_paths(&record)?;
        let all_paths = tracked.union(&untracked).cloned().collect::<BTreeSet<_>>();
        if all_paths.len() > self.config.max_files {
            return Err(WorktreeSnapshotError::BoundsExceeded);
        }
        for relative in &all_paths {
            enforce_snapshot_path_policy(relative)?;
        }
        let index_path_output = self.git(
            &record,
            vec![
                "rev-parse".to_owned(),
                "--path-format=absolute".to_owned(),
                "--git-path".to_owned(),
                "index".to_owned(),
            ],
        )?;
        let index_path = PathBuf::from(index_path_output.trim());
        let index_bytes = read_bounded_file(index_path.as_path(), self.config.max_file_bytes)?;
        enforce_snapshot_content_policy(index_bytes.as_slice())?;
        let snapshot_id = format!("snapshot_{}", ulid::Ulid::new());
        let snapshot_root = self.config.artifact_root.join(snapshot_id.as_str());
        create_private_dir(snapshot_root.as_path())?;
        let mut creation = SnapshotCreationGuard::new(snapshot_root.clone());
        let files_root = snapshot_root.join("files");
        create_private_dir(files_root.as_path())?;
        let index_artifact_file = "index.bin".to_owned();
        write_private_file(
            snapshot_root.join(index_artifact_file.as_str()).as_path(),
            &index_bytes,
        )?;
        let mut total_bytes = u64::try_from(index_bytes.len()).unwrap_or(u64::MAX);
        let mut entries = Vec::with_capacity(all_paths.len());
        for relative in all_paths {
            let absolute = safe_worktree_path(record.worktree_path.as_path(), relative.as_str())?;
            reject_nested_repository(record.worktree_path.as_path(), relative.as_str())?;
            let kind = if untracked.contains(relative.as_str()) {
                SnapshotPathKindV1::AllowedUntracked
            } else {
                SnapshotPathKindV1::Tracked
            };
            let entry = if path_metadata(absolute.as_path())?.is_some() {
                reject_link(absolute.as_path())?;
                let metadata = fs::metadata(absolute.as_path())
                    .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
                if !metadata.is_file() {
                    return Err(WorktreeSnapshotError::UnsafePath(relative));
                }
                let bytes = read_bounded_file(absolute.as_path(), self.config.max_file_bytes)?;
                enforce_snapshot_content_policy(bytes.as_slice())?;
                total_bytes =
                    total_bytes.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                if total_bytes > self.config.max_total_bytes {
                    return Err(WorktreeSnapshotError::BoundsExceeded);
                }
                let artifact_file = format!("{}.bin", sha256_bytes(relative.as_bytes()));
                write_private_file(files_root.join(artifact_file.as_str()).as_path(), &bytes)?;
                WorktreeSnapshotEntryV1 {
                    path: relative,
                    kind,
                    exists: true,
                    size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                    content_sha256: Some(sha256_bytes(bytes.as_slice())),
                    artifact_file: Some(artifact_file),
                    unix_mode: unix_mode(&metadata),
                    readonly: metadata.permissions().readonly(),
                }
            } else {
                WorktreeSnapshotEntryV1 {
                    path: relative,
                    kind,
                    exists: false,
                    size_bytes: 0,
                    content_sha256: None,
                    artifact_file: None,
                    unix_mode: None,
                    readonly: false,
                }
            };
            entries.push(entry);
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let descriptor = WorktreeSnapshotDescriptorV1 {
            schema_version: WORKTREE_SNAPSHOT_SCHEMA_VERSION,
            snapshot_id,
            worktree_id: record.worktree_id,
            worktree_generation: record.generation,
            base_commit,
            index_sha256: sha256_bytes(index_bytes.as_slice()),
            index_artifact_file,
            entries,
            total_bytes,
            created_at_unix_ms: unix_time_ms(),
        };
        let descriptor_bytes = serde_json::to_vec_pretty(&descriptor)
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
        write_private_file(snapshot_root.join("descriptor.json").as_path(), &descriptor_bytes)?;
        creation.commit();
        Ok(descriptor)
    }

    /// Restores exact dirty bytes and the captured Git index after full validation.
    ///
    /// # Errors
    /// Returns an error for base mismatch, unrelated dirty paths, unsafe paths,
    /// artifact integrity failure, or storage mutation failure.
    pub fn restore(
        &self,
        snapshot_id: &str,
    ) -> Result<WorktreeRestoreReportV1, WorktreeSnapshotError> {
        let descriptor = self.load(snapshot_id)?;
        let record =
            self.executor.record(descriptor.worktree_id.as_str()).map_err(map_executor_error)?;
        validate_worktree_state(&record)?;
        let current_base = self
            .git(&record, vec!["rev-parse".to_owned(), "--verify".to_owned(), "HEAD".to_owned()])?
            .trim()
            .to_owned();
        if current_base != descriptor.base_commit {
            return Err(WorktreeSnapshotError::BaseMismatch);
        }
        let represented =
            descriptor.entries.iter().map(|entry| entry.path.clone()).collect::<BTreeSet<_>>();
        let current = self
            .changed_tracked_paths(&record)?
            .union(&self.changed_untracked_paths(&record)?)
            .cloned()
            .collect::<BTreeSet<_>>();
        if current.iter().any(|path| !represented.contains(path)) {
            return Err(WorktreeSnapshotError::RestoreConflict);
        }
        let snapshot_root = self.snapshot_root(snapshot_id)?;
        let index_bytes = read_bounded_file(
            snapshot_root.join(descriptor.index_artifact_file.as_str()).as_path(),
            self.config.max_file_bytes,
        )?;
        if sha256_bytes(index_bytes.as_slice()) != descriptor.index_sha256 {
            return Err(WorktreeSnapshotError::Integrity);
        }
        let mut validated = Vec::new();
        for entry in &descriptor.entries {
            let absolute = safe_worktree_path(record.worktree_path.as_path(), entry.path.as_str())?;
            reject_nested_repository(record.worktree_path.as_path(), entry.path.as_str())?;
            if path_metadata(absolute.as_path())?.is_some() {
                reject_link(absolute.as_path())?;
            }
            let bytes = match entry.artifact_file.as_deref() {
                Some(artifact) => {
                    let bytes = read_bounded_file(
                        snapshot_root.join("files").join(artifact).as_path(),
                        self.config.max_file_bytes,
                    )?;
                    let actual_sha256 = sha256_bytes(&bytes);
                    if entry.content_sha256.as_deref() != Some(actual_sha256.as_str())
                        || entry.size_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                    {
                        return Err(WorktreeSnapshotError::Integrity);
                    }
                    Some(bytes)
                }
                None if !entry.exists => None,
                None => return Err(WorktreeSnapshotError::Integrity),
            };
            validated.push((entry.clone(), absolute, bytes));
        }
        for (entry, absolute, bytes) in &validated {
            match bytes {
                Some(bytes) => {
                    if let Some(parent) = absolute.parent() {
                        create_confined_parent(record.worktree_path.as_path(), parent)?;
                    }
                    atomic_replace(absolute.as_path(), bytes.as_slice())?;
                    restore_permissions(absolute.as_path(), entry)?;
                }
                None if path_metadata(absolute.as_path())?.is_some() => {
                    reject_link(absolute.as_path())?;
                    if !absolute.is_file() {
                        return Err(WorktreeSnapshotError::UnsafePath(entry.path.clone()));
                    }
                    fs::remove_file(absolute.as_path())
                        .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
                }
                None => {}
            }
        }
        let index_path_output = self.git(
            &record,
            vec![
                "rev-parse".to_owned(),
                "--path-format=absolute".to_owned(),
                "--git-path".to_owned(),
                "index".to_owned(),
            ],
        )?;
        let index_path = PathBuf::from(index_path_output.trim());
        atomic_replace(index_path.as_path(), index_bytes.as_slice())?;
        Ok(WorktreeRestoreReportV1 {
            schema_version: WORKTREE_RESTORE_REPORT_SCHEMA_VERSION,
            snapshot_id: descriptor.snapshot_id,
            worktree_id: descriptor.worktree_id,
            base_commit: descriptor.base_commit,
            restored_paths: descriptor.entries.into_iter().map(|entry| entry.path).collect(),
            index_restored: true,
            completed_at_unix_ms: unix_time_ms(),
        })
    }

    /// Loads and integrity-checks a snapshot descriptor.
    ///
    /// # Errors
    /// Returns an error for malformed identity, missing descriptor, or schema drift.
    pub fn load(
        &self,
        snapshot_id: &str,
    ) -> Result<WorktreeSnapshotDescriptorV1, WorktreeSnapshotError> {
        let root = self.snapshot_root(snapshot_id)?;
        let bytes =
            fs::read(root.join("descriptor.json")).map_err(|_| WorktreeSnapshotError::NotFound)?;
        let descriptor = serde_json::from_slice::<WorktreeSnapshotDescriptorV1>(&bytes)
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
        if descriptor.schema_version != WORKTREE_SNAPSHOT_SCHEMA_VERSION
            || descriptor.snapshot_id != snapshot_id
        {
            return Err(WorktreeSnapshotError::Integrity);
        }
        Ok(descriptor)
    }

    /// Lists snapshot identities in stable order.
    ///
    /// # Errors
    /// Returns an error when artifact storage cannot be enumerated.
    pub fn list(&self) -> Result<Vec<String>, WorktreeSnapshotError> {
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(self.config.artifact_root.as_path())
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?
        {
            let entry = entry.map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
            if entry
                .file_type()
                .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?
                .is_dir()
            {
                if let Some(name) = entry.file_name().to_str() {
                    if validate_snapshot_id(name).is_ok()
                        && entry.path().join("descriptor.json").is_file()
                    {
                        snapshots.push(name.to_owned());
                    }
                }
            }
        }
        snapshots.sort();
        Ok(snapshots)
    }

    /// Lists integrity-checked descriptors in stable snapshot identity order.
    ///
    /// # Errors
    /// Returns an error when storage enumeration or any retained descriptor fails.
    pub fn list_descriptors(
        &self,
    ) -> Result<Vec<WorktreeSnapshotDescriptorV1>, WorktreeSnapshotError> {
        self.list()?.into_iter().map(|snapshot_id| self.load(snapshot_id.as_str())).collect()
    }

    /// Removes a snapshot only when no run lock or matching runtime lease is active.
    ///
    /// # Errors
    /// Returns an error for missing snapshots, active ownership, or unsafe storage.
    pub fn gc(
        &self,
        snapshot_id: &str,
        force: bool,
    ) -> Result<SnapshotGcDecisionV1, WorktreeSnapshotError> {
        let descriptor = self.load(snapshot_id)?;
        let record =
            self.executor.record(descriptor.worktree_id.as_str()).map_err(map_executor_error)?;
        if record.locked_by_run.is_some() && !force {
            return Ok(SnapshotGcDecisionV1::Retained);
        }
        let owners = record
            .attached_run_ids
            .iter()
            .cloned()
            .chain(std::iter::once(format!("worktree-{}", record.worktree_id)))
            .collect::<BTreeSet<_>>();
        let blocked = self
            .resource_governor
            .active_leases()
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?
            .iter()
            .any(|lease| {
                owners.contains(lease.owner_id.as_str())
                    && matches!(
                        lease.service,
                        ResourceServiceKind::Process
                            | ResourceServiceKind::Pty
                            | ResourceServiceKind::Lsp
                            | ResourceServiceKind::Mcp
                            | ResourceServiceKind::ExternalRuntime
                    )
            });
        if blocked {
            return Ok(SnapshotGcDecisionV1::BlockedByActiveLease);
        }
        if !force && record.lifecycle != ManagedWorktreeLifecycleV2::Removed {
            return Ok(SnapshotGcDecisionV1::Retained);
        }
        let root = self.snapshot_root(snapshot_id)?;
        fs::remove_dir_all(root.as_path())
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
        Ok(SnapshotGcDecisionV1::Removed)
    }

    fn changed_tracked_paths(
        &self,
        record: &ManagedWorktreeRecordV2,
    ) -> Result<BTreeSet<String>, WorktreeSnapshotError> {
        let unstaged = self.git(
            record,
            vec![
                "diff".to_owned(),
                "--name-only".to_owned(),
                "-z".to_owned(),
                "--no-ext-diff".to_owned(),
            ],
        )?;
        let staged = self.git(
            record,
            vec![
                "diff".to_owned(),
                "--cached".to_owned(),
                "--name-only".to_owned(),
                "-z".to_owned(),
                "--no-ext-diff".to_owned(),
            ],
        )?;
        Ok(parse_git_paths(unstaged.as_str())?
            .union(&parse_git_paths(staged.as_str())?)
            .cloned()
            .collect())
    }

    fn changed_untracked_paths(
        &self,
        record: &ManagedWorktreeRecordV2,
    ) -> Result<BTreeSet<String>, WorktreeSnapshotError> {
        let output = self.git(
            record,
            vec![
                "ls-files".to_owned(),
                "--others".to_owned(),
                "--exclude-standard".to_owned(),
                "-z".to_owned(),
            ],
        )?;
        parse_git_paths(output.as_str())
    }

    fn git(
        &self,
        record: &ManagedWorktreeRecordV2,
        mut args: Vec<String>,
    ) -> Result<String, WorktreeSnapshotError> {
        let mut full = vec![
            "-C".to_owned(),
            git_path_argument(record.worktree_path.as_path()),
            "-c".to_owned(),
            disabled_hooks_config(),
        ];
        full.append(&mut args);
        self.executor
            .run_git(
                full,
                record.worktree_path.as_path(),
                record.worktree_id.as_str(),
                record.generation,
            )
            .map_err(map_executor_error)
    }

    fn snapshot_root(&self, snapshot_id: &str) -> Result<PathBuf, WorktreeSnapshotError> {
        validate_snapshot_id(snapshot_id)?;
        let path = self.config.artifact_root.join(snapshot_id);
        if !path.is_dir() {
            return Err(WorktreeSnapshotError::NotFound);
        }
        reject_link(path.as_path())?;
        let canonical = path.canonicalize().map_err(|_| WorktreeSnapshotError::NotFound)?;
        if canonical.parent() != Some(self.config.artifact_root.as_path()) {
            return Err(WorktreeSnapshotError::UnsafePath(snapshot_id.to_owned()));
        }
        Ok(canonical)
    }
}

fn validate_worktree_state(record: &ManagedWorktreeRecordV2) -> Result<(), WorktreeSnapshotError> {
    if matches!(
        record.lifecycle,
        ManagedWorktreeLifecycleV2::Active | ManagedWorktreeLifecycleV2::Retained
    ) {
        Ok(())
    } else {
        Err(WorktreeSnapshotError::InvalidWorktreeState)
    }
}

fn enforce_snapshot_path_policy(relative: &str) -> Result<(), WorktreeSnapshotError> {
    let normalized = normalize_relative_path(relative)?;
    let lowered = normalized.to_ascii_lowercase();
    let mut components = lowered.split('/').collect::<Vec<_>>();
    let file_name = components.pop().ok_or_else(|| {
        WorktreeSnapshotError::UnsafePath("snapshot path has no filename".to_owned())
    })?;

    const SENSITIVE_FILE_NAMES: &[&str] = &[
        ".dockercfg",
        ".git-credentials",
        ".htpasswd",
        ".netrc",
        ".npmrc",
        ".pypirc",
        "application_default_credentials.json",
        "credentials",
        "credentials.json",
        "id_dsa",
        "id_ecdsa",
        "id_ed25519",
        "id_rsa",
        "secrets.json",
        "secrets.yaml",
        "secrets.yml",
        "service-account-key.json",
        "service_account_key.json",
    ];
    const SENSITIVE_EXTENSIONS: &[&str] = &["jks", "kdbx", "key", "keystore", "p12", "pem", "pfx"];
    const SENSITIVE_DIRECTORIES: &[&str] = &[".aws", ".gnupg", ".kube", ".ssh"];
    const ENV_TEMPLATE_SUFFIXES: &[&str] = &[".dist", ".example", ".sample", ".template"];

    let env_file_is_sensitive = file_name == ".env"
        || (file_name.starts_with(".env.")
            && !ENV_TEMPLATE_SUFFIXES.iter().any(|suffix| file_name.ends_with(suffix)));
    let extension_is_sensitive = file_name
        .rsplit_once('.')
        .is_some_and(|(_, extension)| SENSITIVE_EXTENSIONS.contains(&extension));

    // Credential-store formats and directories are denied by name because
    // encrypted or binary payloads cannot be proven safe by a text scan.
    if env_file_is_sensitive
        || SENSITIVE_FILE_NAMES.contains(&file_name)
        || extension_is_sensitive
        || components.iter().any(|component| SENSITIVE_DIRECTORIES.contains(component))
    {
        return Err(WorktreeSnapshotError::SensitiveMaterial);
    }
    Ok(())
}

fn enforce_snapshot_content_policy(bytes: &[u8]) -> Result<(), WorktreeSnapshotError> {
    if let Ok(text) = std::str::from_utf8(bytes) {
        return enforce_snapshot_text_policy(text);
    }

    let mut projection = String::with_capacity(bytes.len());
    let mut separator_emitted = true;
    for byte in bytes {
        if byte.is_ascii_graphic() || byte.is_ascii_whitespace() {
            projection.push(char::from(*byte));
            separator_emitted = byte.is_ascii_whitespace();
        } else if !separator_emitted {
            projection.push('\n');
            separator_emitted = true;
        }
    }
    enforce_snapshot_text_policy(projection.as_str())
}

fn enforce_snapshot_text_policy(text: &str) -> Result<(), WorktreeSnapshotError> {
    let scan = inspect_text(
        text,
        SafetyPhase::Export,
        SafetySourceKind::Workspace,
        SafetyContentKind::WorkspaceDocument,
        TrustLabel::TrustedLocal,
    );
    // Snapshot restore is byte-exact. Redacting at capture would silently
    // corrupt the worktree, so secret-bearing input is rejected instead.
    if scan.has_category(SafetyFindingCategory::SecretLeak) {
        return Err(WorktreeSnapshotError::SensitiveMaterial);
    }
    Ok(())
}

fn parse_git_paths(output: &str) -> Result<BTreeSet<String>, WorktreeSnapshotError> {
    if output.contains('\u{fffd}') {
        return Err(WorktreeSnapshotError::UnsafePath(
            "non-UTF-8 Git paths are not supported by the bounded process projection".to_owned(),
        ));
    }
    output.split('\0').filter(|value| !value.is_empty()).map(normalize_relative_path).collect()
}

fn normalize_relative_path(path: &str) -> Result<String, WorktreeSnapshotError> {
    let normalized = path.replace('\\', "/");
    if normalized.is_empty()
        || normalized.starts_with('/')
        || normalized.split('/').any(|part| part.is_empty() || part == "." || part == "..")
        || Path::new(normalized.as_str()).is_absolute()
        || Path::new(normalized.as_str())
            .components()
            .any(|component| matches!(component, Component::Prefix(_) | Component::RootDir))
    {
        return Err(WorktreeSnapshotError::UnsafePath(path.to_owned()));
    }
    Ok(normalized)
}

fn safe_worktree_path(root: &Path, relative: &str) -> Result<PathBuf, WorktreeSnapshotError> {
    let relative = normalize_relative_path(relative)?;
    let path = root.join(relative.as_str());
    let nearest = nearest_existing_ancestor(path.as_path())?;
    reject_link(nearest.as_path())?;
    let canonical = nearest
        .canonicalize()
        .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    if !canonical.starts_with(root) {
        return Err(WorktreeSnapshotError::UnsafePath(relative));
    }
    Ok(path)
}

fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf, WorktreeSnapshotError> {
    let mut current = path;
    loop {
        if path_metadata(current)?.is_some() {
            return Ok(current.to_path_buf());
        }
        current = current
            .parent()
            .ok_or_else(|| WorktreeSnapshotError::UnsafePath(path.display().to_string()))?;
    }
}

fn create_confined_parent(root: &Path, parent: &Path) -> Result<(), WorktreeSnapshotError> {
    let existing = nearest_existing_ancestor(parent)?;
    reject_link(existing.as_path())?;
    let canonical = existing
        .canonicalize()
        .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    if !canonical.starts_with(root) {
        return Err(WorktreeSnapshotError::UnsafePath(parent.display().to_string()));
    }
    fs::create_dir_all(parent).map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))
}

fn reject_nested_repository(root: &Path, relative: &str) -> Result<(), WorktreeSnapshotError> {
    let relative = normalize_relative_path(relative)?;
    let mut parent = root.to_path_buf();
    let component_count = Path::new(relative.as_str()).components().count();
    for component in
        Path::new(relative.as_str()).components().take(component_count.saturating_sub(1))
    {
        parent.push(component.as_os_str());
        let marker = parent.join(".git");
        if path_metadata(marker.as_path())?.is_some() {
            return Err(WorktreeSnapshotError::UnsafePath(relative));
        }
    }
    Ok(())
}

fn path_metadata(path: &Path) -> Result<Option<fs::Metadata>, WorktreeSnapshotError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(WorktreeSnapshotError::Storage(error.to_string())),
    }
}

fn read_bounded_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>, WorktreeSnapshotError> {
    reject_link(path)?;
    let metadata =
        fs::metadata(path).map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(WorktreeSnapshotError::BoundsExceeded);
    }
    fs::read(path).map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))
}

fn validate_snapshot_id(snapshot_id: &str) -> Result<(), WorktreeSnapshotError> {
    if snapshot_id.is_empty()
        || snapshot_id.len() > MAX_SNAPSHOT_ID_BYTES
        || !snapshot_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(WorktreeSnapshotError::NotFound);
    }
    Ok(())
}

fn reject_link(path: &Path) -> Result<(), WorktreeSnapshotError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    if metadata.file_type().is_symlink() {
        return Err(WorktreeSnapshotError::UnsafePath(path.display().to_string()));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(WorktreeSnapshotError::UnsafePath(path.display().to_string()));
        }
    }
    Ok(())
}

fn atomic_replace(path: &Path, bytes: &[u8]) -> Result<(), WorktreeSnapshotError> {
    if path_metadata(path)?.is_some() {
        reject_link(path)?;
    }
    let parent = path
        .parent()
        .ok_or_else(|| WorktreeSnapshotError::UnsafePath(path.display().to_string()))?;
    fs::create_dir_all(parent)
        .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".restore.{}", ulid::Ulid::new()));
    let temporary_path = PathBuf::from(temporary_name);
    fs::write(temporary_path.as_path(), bytes)
        .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    if let Err(rename_error) = fs::rename(temporary_path.as_path(), path) {
        if !path.is_file() {
            let _ = fs::remove_file(temporary_path.as_path());
            return Err(WorktreeSnapshotError::Storage(rename_error.to_string()));
        }
        let mut rollback_name = path.as_os_str().to_os_string();
        rollback_name.push(format!(".rollback.{}", ulid::Ulid::new()));
        let rollback_path = PathBuf::from(rollback_name);
        fs::rename(path, rollback_path.as_path())
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
        if let Err(install_error) = fs::rename(temporary_path.as_path(), path) {
            let _ = fs::rename(rollback_path.as_path(), path);
            let _ = fs::remove_file(temporary_path.as_path());
            return Err(WorktreeSnapshotError::Storage(install_error.to_string()));
        }
        let _ = fs::remove_file(rollback_path);
    }
    Ok(())
}

struct SnapshotCreationGuard {
    root: PathBuf,
    committed: bool,
}

impl SnapshotCreationGuard {
    fn new(root: PathBuf) -> Self {
        Self { root, committed: false }
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for SnapshotCreationGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_dir_all(self.root.as_path());
        }
    }
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), WorktreeSnapshotError> {
    fs::write(path, bytes).map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    }
    Ok(())
}

fn create_private_dir(path: &Path) -> Result<(), WorktreeSnapshotError> {
    fs::create_dir_all(path).map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?;
    }
    Ok(())
}

#[cfg(unix)]
fn unix_mode(metadata: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt as _;
    Some(metadata.mode())
}

#[cfg(not(unix))]
fn unix_mode(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

fn restore_permissions(
    path: &Path,
    entry: &WorktreeSnapshotEntryV1,
) -> Result<(), WorktreeSnapshotError> {
    let mut permissions = fs::metadata(path)
        .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))?
        .permissions();
    permissions.set_readonly(entry.readonly);
    #[cfg(unix)]
    if let Some(mode) = entry.unix_mode {
        use std::os::unix::fs::PermissionsExt as _;
        permissions.set_mode(mode);
    }
    fs::set_permissions(path, permissions)
        .map_err(|error| WorktreeSnapshotError::Storage(error.to_string()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
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

fn map_executor_error(error: ManagedWorktreeExecutorError) -> WorktreeSnapshotError {
    WorktreeSnapshotError::Git(error.to_string())
}

fn unix_time_ms() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::local_resource_governor::{
        ResourceLeaseRequestV1, ResourcePriority, ResourceUnitsV1,
    };
    use crate::application::managed_worktree_test_support::ManagedWorktreeTestFixture;
    use std::time::Duration;

    fn store(fixture: &ManagedWorktreeTestFixture, max_file_bytes: u64) -> WorktreeSnapshotStore {
        WorktreeSnapshotStore::open(
            WorktreeSnapshotStoreConfig {
                artifact_root: fixture
                    .managed_root
                    .parent()
                    .expect("managed root parent")
                    .join("snapshots"),
                max_files: 128,
                max_file_bytes,
                max_total_bytes: max_file_bytes.saturating_mul(128),
            },
            Arc::clone(&fixture.executor),
            fixture.governor.clone(),
        )
        .expect("open snapshot store")
    }

    #[test]
    fn restores_staged_unstaged_untracked_bytes_and_exact_index() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("snapshot");
        fs::write(record.worktree_path.join("tracked.txt"), b"staged\r\n")
            .expect("write staged bytes");
        fixture.git(record.worktree_path.as_path(), &["add", "tracked.txt"]);
        let expected_index =
            fixture.git_output(record.worktree_path.as_path(), &["show", ":tracked.txt"]);
        fs::write(record.worktree_path.join("tracked.txt"), b"working\r\n")
            .expect("write unstaged bytes");
        fs::write(record.worktree_path.join("untracked.txt"), b"untracked\r\n")
            .expect("write untracked bytes");
        let store = store(&fixture, 1024 * 1024);
        let snapshot = store.capture("snapshot").expect("capture worktree");

        fs::write(record.worktree_path.join("tracked.txt"), b"later\n").expect("mutate tracked");
        fs::remove_file(record.worktree_path.join("untracked.txt")).expect("remove untracked");
        let report = store.restore(snapshot.snapshot_id.as_str()).expect("restore worktree");
        assert!(report.index_restored);
        assert_eq!(
            fs::read(record.worktree_path.join("tracked.txt")).expect("read tracked"),
            b"working\r\n"
        );
        assert_eq!(
            fs::read(record.worktree_path.join("untracked.txt")).expect("read untracked"),
            b"untracked\r\n"
        );
        assert_eq!(
            fixture.git_output(record.worktree_path.as_path(), &["show", ":tracked.txt"]),
            expected_index
        );
    }

    #[cfg(unix)]
    #[test]
    fn restore_preserves_executable_mode() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("executable");
        let script = record.worktree_path.join("tool.sh");
        fs::write(script.as_path(), b"#!/bin/sh\r\nexit 0\r\n").expect("write script");
        fs::set_permissions(script.as_path(), fs::Permissions::from_mode(0o755))
            .expect("set executable");
        let store = store(&fixture, 1024 * 1024);
        let snapshot = store.capture("executable").expect("capture executable");
        fs::set_permissions(script.as_path(), fs::Permissions::from_mode(0o600))
            .expect("clear executable");
        store.restore(snapshot.snapshot_id.as_str()).expect("restore executable");
        assert_eq!(fs::metadata(script).expect("script metadata").mode() & 0o777, 0o755);
    }

    #[test]
    fn base_revision_mismatch_fails_before_restore_mutation() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("base-mismatch");
        fs::write(record.worktree_path.join("tracked.txt"), b"snapshot\n").expect("edit tracked");
        let store = store(&fixture, 1024 * 1024);
        let snapshot = store.capture("base-mismatch").expect("capture snapshot");
        fixture.git(record.worktree_path.as_path(), &["add", "tracked.txt"]);
        fixture.git(record.worktree_path.as_path(), &["commit", "-m", "advance base"]);
        let error = store.restore(snapshot.snapshot_id.as_str()).expect_err("reject base mismatch");
        assert!(matches!(error, WorktreeSnapshotError::BaseMismatch));
        assert_eq!(
            fs::read(record.worktree_path.join("tracked.txt")).expect("read current bytes"),
            b"snapshot\n"
        );
    }

    #[test]
    fn restore_rejects_unknown_descriptor_versions_and_fields_before_mutation() {
        let mutators: [fn(&mut serde_json::Value); 2] = [
            |value| value["schema_version"] = serde_json::json!(999),
            |value| value["unknown_descriptor_field"] = serde_json::json!(true),
        ];
        for mutate in mutators {
            let fixture = ManagedWorktreeTestFixture::new();
            let record = fixture.create_worktree("descriptor-contract");
            let tracked_path = record.worktree_path.join("tracked.txt");
            fs::write(tracked_path.as_path(), b"snapshot bytes\n").expect("edit tracked");
            let store = store(&fixture, 1024 * 1024);
            let snapshot = store.capture("descriptor-contract").expect("capture snapshot");
            fs::write(tracked_path.as_path(), b"current bytes\n").expect("replace tracked");
            let descriptor_path = store
                .snapshot_root(snapshot.snapshot_id.as_str())
                .expect("snapshot root")
                .join("descriptor.json");
            let mut value: serde_json::Value = serde_json::from_slice(
                fs::read(descriptor_path.as_path()).expect("read descriptor").as_slice(),
            )
            .expect("decode descriptor");
            mutate(&mut value);
            let bytes = serde_json::to_vec_pretty(&value).expect("encode invalid descriptor");
            fs::write(descriptor_path.as_path(), bytes.as_slice())
                .expect("write invalid descriptor");

            assert!(store.restore(snapshot.snapshot_id.as_str()).is_err());
            assert_eq!(
                fs::read(tracked_path.as_path()).expect("read unmodified worktree"),
                b"current bytes\n"
            );
            assert_eq!(
                fs::read(descriptor_path.as_path()).expect("read unchanged descriptor"),
                bytes
            );
        }
    }

    #[test]
    fn nested_repository_is_rejected() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("nested");
        let nested = record.worktree_path.join("nested");
        fs::create_dir_all(nested.as_path()).expect("create nested root");
        fixture.git(nested.as_path(), &["init", "--initial-branch=main"]);
        fs::write(nested.join("file.txt"), b"nested\n").expect("write nested file");
        let error =
            store(&fixture, 1024 * 1024).capture("nested").expect_err("reject nested repository");
        assert!(matches!(error, WorktreeSnapshotError::UnsafePath(_)));
    }

    #[test]
    fn active_runtime_lease_blocks_snapshot_gc() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("leased");
        fs::write(record.worktree_path.join("tracked.txt"), b"dirty\n").expect("dirty worktree");
        let store = store(&fixture, 1024 * 1024);
        let snapshot = store.capture("leased").expect("capture snapshot");
        let lease = fixture
            .governor
            .acquire(ResourceLeaseRequestV1 {
                owner_id: "worktree-leased".to_owned(),
                generation: 7,
                service: ResourceServiceKind::Lsp,
                priority: ResourcePriority::IdleService,
                requested: ResourceUnitsV1 {
                    processes: 1,
                    memory_bytes: 64 * 1024 * 1024,
                    file_descriptors: 8,
                    sockets: 0,
                    spool_bytes: 64 * 1024,
                    concurrency: 1,
                },
                duration: Duration::from_secs(60),
            })
            .expect("acquire LSP lease");
        assert_eq!(
            store.gc(snapshot.snapshot_id.as_str(), true).expect("blocked GC"),
            SnapshotGcDecisionV1::BlockedByActiveLease
        );
        fixture
            .governor
            .release(lease.lease_id.as_str(), lease.generation)
            .expect("release LSP lease");
        assert_eq!(
            store.gc(snapshot.snapshot_id.as_str(), true).expect("forced GC"),
            SnapshotGcDecisionV1::Removed
        );
    }

    #[test]
    fn failed_capture_removes_partial_artifact_directory() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("bounded");
        fs::write(record.worktree_path.join("tracked.txt"), vec![b'x'; 2_048])
            .expect("write oversized change");
        let store = store(&fixture, 1_024);
        let error = store.capture("bounded").expect_err("reject oversized snapshot");
        assert!(matches!(error, WorktreeSnapshotError::BoundsExceeded));
        assert!(store.list().expect("list snapshots").is_empty());
    }

    #[test]
    fn sensitive_path_is_rejected_before_snapshot_persistence() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("sensitive-path");
        fs::write(record.worktree_path.join(".env.local"), b"PUBLIC_LABEL=local\n")
            .expect("write sensitive path");
        let store = store(&fixture, 1024 * 1024);

        let error = store.capture("sensitive-path").expect_err("reject sensitive path");

        assert!(matches!(error, WorktreeSnapshotError::SensitiveMaterial));
        assert!(store.list().expect("list snapshots").is_empty());
    }

    #[test]
    fn secret_like_content_is_rejected_without_retained_artifacts() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("sensitive-content");
        fs::write(
            record.worktree_path.join("notes.txt"),
            b"APP_SECRET=palyra_test_secret_snapshot_canary\n",
        )
        .expect("write secret-like content");
        let store = store(&fixture, 1024 * 1024);

        let error = store.capture("sensitive-content").expect_err("reject secret-like content");

        assert!(matches!(error, WorktreeSnapshotError::SensitiveMaterial));
        assert!(store.list().expect("list snapshots").is_empty());
    }

    #[test]
    fn secret_like_git_index_metadata_is_rejected() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("sensitive-index");
        let file_name = "palyra_test_secret_snapshot_index_canary.txt";
        fs::write(record.worktree_path.join(file_name), b"ordinary content\n")
            .expect("write indexed file");
        fixture.git(record.worktree_path.as_path(), &["add", file_name]);
        let store = store(&fixture, 1024 * 1024);

        let error = store.capture("sensitive-index").expect_err("reject secret-like index");

        assert!(matches!(error, WorktreeSnapshotError::SensitiveMaterial));
        assert!(store.list().expect("list snapshots").is_empty());
    }

    #[test]
    fn ordinary_text_and_binary_content_remain_lossless() {
        let fixture = ManagedWorktreeTestFixture::new();
        let record = fixture.create_worktree("ordinary-content");
        let binary = [0_u8, 1, 2, 127, 128, 159, 255];
        fs::write(record.worktree_path.join("notes.txt"), b"ordinary workspace notes\n")
            .expect("write ordinary text");
        fs::write(record.worktree_path.join("asset.bin"), binary).expect("write ordinary binary");
        let store = store(&fixture, 1024 * 1024);

        let snapshot = store.capture("ordinary-content").expect("capture ordinary content");

        assert_eq!(
            snapshot.entries.iter().map(|entry| entry.path.as_str()).collect::<Vec<_>>(),
            vec!["asset.bin", "notes.txt"]
        );
        fs::write(record.worktree_path.join("asset.bin"), b"later")
            .expect("mutate ordinary binary");
        store.restore(snapshot.snapshot_id.as_str()).expect("restore ordinary content");
        assert_eq!(
            fs::read(record.worktree_path.join("asset.bin")).expect("read restored binary"),
            binary
        );
    }
}
