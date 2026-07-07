//! Managed git worktree registry contracts for coding workflows.
//!
//! The registry is storage-neutral and side-effect free: it validates paths,
//! branch namespace, dirty removal policy, restore plans, run attachment, and
//! garbage-collection eligibility before a host git executor performs IO.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MANAGED_WORKTREE_SCHEMA_VERSION: u32 = 1;
pub const MANAGED_WORKTREE_BRANCH_PREFIX: &str = "palyra";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedWorktreeState {
    Active,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorktreeRecord {
    pub schema_version: u32,
    pub worktree_id: String,
    pub source_repo: PathBuf,
    pub worktree_path: PathBuf,
    pub branch: String,
    pub base_ref: String,
    pub state: ManagedWorktreeState,
    pub dirty: bool,
    pub snapshot_ref: Option<String>,
    pub locked_by_run: Option<String>,
    pub attached_run_ids: Vec<String>,
    pub created_at_unix_ms: i64,
    pub last_used_at_unix_ms: i64,
    pub sandbox_mount: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedWorktreeCreateRequest {
    pub worktree_id: String,
    pub source_repo: PathBuf,
    pub worktree_path: PathBuf,
    pub branch_slug: String,
    pub base_ref: String,
    pub now_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedWorktreeRemoveRequest {
    pub worktree_id: String,
    pub force: bool,
    pub snapshot_ref: Option<String>,
    pub now_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedWorktreeRestoreRequest {
    pub worktree_id: String,
    pub snapshot_ref: String,
    pub paths: Vec<String>,
    pub now_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorktreeRestorePlan {
    pub worktree_id: String,
    pub snapshot_ref: String,
    pub restored_paths: Vec<String>,
    pub status: String,
    pub working_tree_state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorktreeRunAttachment {
    pub worktree_id: String,
    pub run_id: String,
    pub workspace_root: PathBuf,
    pub workspace_policy: String,
    pub sandbox_mount: String,
    pub reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorktreeGcReport {
    pub removed_worktree_ids: Vec<String>,
    pub skipped_locked_worktree_ids: Vec<String>,
    pub skipped_dirty_worktree_ids: Vec<String>,
    pub idle_cutoff_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ManagedWorktreeError {
    #[error("managed worktree already exists: {worktree_id}")]
    Duplicate { worktree_id: String },
    #[error("managed worktree not found: {worktree_id}")]
    NotFound { worktree_id: String },
    #[error("managed worktree id is empty")]
    EmptyWorktreeId,
    #[error("managed worktree path is inside source repository")]
    WorktreeInsideSourceRepo,
    #[error("managed worktree branch slug is empty")]
    EmptyBranchSlug,
    #[error("dirty managed worktree removal requires a snapshot or force")]
    DirtyRemoveRequiresSnapshot,
    #[error("managed worktree is locked by run {run_id}")]
    Locked { run_id: String },
    #[error("managed worktree restore requires at least one path")]
    EmptyRestorePaths,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorktreeRegistry {
    records: BTreeMap<String, ManagedWorktreeRecord>,
}

impl ManagedWorktreeRegistry {
    #[must_use]
    pub fn list(&self) -> Vec<ManagedWorktreeRecord> {
        self.records.values().cloned().collect()
    }

    pub fn create(
        &mut self,
        request: ManagedWorktreeCreateRequest,
    ) -> Result<ManagedWorktreeRecord, ManagedWorktreeError> {
        let worktree_id = non_empty(request.worktree_id, ManagedWorktreeError::EmptyWorktreeId)?;
        if self.records.contains_key(worktree_id.as_str()) {
            return Err(ManagedWorktreeError::Duplicate { worktree_id });
        }
        if path_is_within(request.worktree_path.as_path(), request.source_repo.as_path()) {
            return Err(ManagedWorktreeError::WorktreeInsideSourceRepo);
        }
        let branch = managed_branch_name(request.branch_slug.as_str())?;
        let record = ManagedWorktreeRecord {
            schema_version: MANAGED_WORKTREE_SCHEMA_VERSION,
            worktree_id: worktree_id.clone(),
            source_repo: request.source_repo,
            worktree_path: request.worktree_path,
            branch,
            base_ref: request.base_ref,
            state: ManagedWorktreeState::Active,
            dirty: false,
            snapshot_ref: None,
            locked_by_run: None,
            attached_run_ids: Vec::new(),
            created_at_unix_ms: request.now_unix_ms,
            last_used_at_unix_ms: request.now_unix_ms,
            sandbox_mount: "explicit_rw_worktree_mount".to_owned(),
        };
        self.records.insert(worktree_id, record.clone());
        Ok(record)
    }

    pub fn remove(
        &mut self,
        request: ManagedWorktreeRemoveRequest,
    ) -> Result<ManagedWorktreeRecord, ManagedWorktreeError> {
        let record = self.record_mut(request.worktree_id.as_str())?;
        if let Some(run_id) = record.locked_by_run.as_ref() {
            return Err(ManagedWorktreeError::Locked { run_id: run_id.clone() });
        }
        if record.dirty && !request.force && request.snapshot_ref.is_none() {
            return Err(ManagedWorktreeError::DirtyRemoveRequiresSnapshot);
        }
        if let Some(snapshot_ref) = request.snapshot_ref {
            record.snapshot_ref = Some(snapshot_ref);
        }
        record.state = ManagedWorktreeState::Removed;
        record.last_used_at_unix_ms = request.now_unix_ms;
        Ok(record.clone())
    }

    pub fn restore(
        &mut self,
        request: ManagedWorktreeRestoreRequest,
    ) -> Result<ManagedWorktreeRestorePlan, ManagedWorktreeError> {
        if request.paths.is_empty() {
            return Err(ManagedWorktreeError::EmptyRestorePaths);
        }
        let record = self.record_mut(request.worktree_id.as_str())?;
        let mut restored_paths = request
            .paths
            .into_iter()
            .filter_map(|path| normalize_workspace_path(path.as_str()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        restored_paths.sort();
        record.dirty = true;
        record.snapshot_ref = Some(request.snapshot_ref.clone());
        record.last_used_at_unix_ms = request.now_unix_ms;
        Ok(ManagedWorktreeRestorePlan {
            worktree_id: record.worktree_id.clone(),
            snapshot_ref: request.snapshot_ref,
            restored_paths,
            status: "restore_planned".to_owned(),
            working_tree_state: "unstaged_modifications_planned".to_owned(),
        })
    }

    pub fn attach_run(
        &mut self,
        worktree_id: &str,
        run_id: &str,
        now_unix_ms: i64,
    ) -> Result<ManagedWorktreeRunAttachment, ManagedWorktreeError> {
        let record = self.record_mut(worktree_id)?;
        record.last_used_at_unix_ms = now_unix_ms;
        if !record.attached_run_ids.iter().any(|existing| existing == run_id) {
            record.attached_run_ids.push(run_id.to_owned());
            record.attached_run_ids.sort();
        }
        record.locked_by_run = Some(run_id.to_owned());
        Ok(ManagedWorktreeRunAttachment {
            worktree_id: record.worktree_id.clone(),
            run_id: run_id.to_owned(),
            workspace_root: record.worktree_path.clone(),
            workspace_policy: "worktree_scoped_rw".to_owned(),
            sandbox_mount: record.sandbox_mount.clone(),
            reason_code: "managed_worktree.run_attached".to_owned(),
        })
    }

    pub fn gc(&mut self, now_unix_ms: i64, idle_age_ms: i64) -> ManagedWorktreeGcReport {
        let idle_cutoff_unix_ms = now_unix_ms.saturating_sub(idle_age_ms.max(0));
        let mut removed_worktree_ids = Vec::new();
        let mut skipped_locked_worktree_ids = Vec::new();
        let mut skipped_dirty_worktree_ids = Vec::new();
        for record in self.records.values_mut() {
            if record.state == ManagedWorktreeState::Removed
                || record.last_used_at_unix_ms > idle_cutoff_unix_ms
            {
                continue;
            }
            if record.locked_by_run.is_some() {
                skipped_locked_worktree_ids.push(record.worktree_id.clone());
                continue;
            }
            if record.dirty {
                skipped_dirty_worktree_ids.push(record.worktree_id.clone());
                continue;
            }
            record.state = ManagedWorktreeState::Removed;
            removed_worktree_ids.push(record.worktree_id.clone());
        }
        ManagedWorktreeGcReport {
            removed_worktree_ids,
            skipped_locked_worktree_ids,
            skipped_dirty_worktree_ids,
            idle_cutoff_unix_ms,
        }
    }

    pub fn mark_dirty(
        &mut self,
        worktree_id: &str,
        dirty: bool,
    ) -> Result<(), ManagedWorktreeError> {
        self.record_mut(worktree_id)?.dirty = dirty;
        Ok(())
    }

    fn record_mut(
        &mut self,
        worktree_id: &str,
    ) -> Result<&mut ManagedWorktreeRecord, ManagedWorktreeError> {
        self.records
            .get_mut(worktree_id)
            .ok_or_else(|| ManagedWorktreeError::NotFound { worktree_id: worktree_id.to_owned() })
    }
}

fn managed_branch_name(slug: &str) -> Result<String, ManagedWorktreeError> {
    let normalized = slug
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '/') {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches(['-', '/', '_'])
        .to_owned();
    if normalized.is_empty() {
        return Err(ManagedWorktreeError::EmptyBranchSlug);
    }
    Ok(format!("{MANAGED_WORKTREE_BRANCH_PREFIX}/{normalized}"))
}

fn non_empty(value: String, error: ManagedWorktreeError) -> Result<String, ManagedWorktreeError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(error)
    } else {
        Ok(trimmed.to_owned())
    }
}

fn path_is_within(child: &Path, parent: &Path) -> bool {
    normalized_components(child).starts_with(normalized_components(parent).as_slice())
}

fn normalized_components(path: &Path) -> Vec<String> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().to_ascii_lowercase()),
            Component::Prefix(prefix) => {
                Some(prefix.as_os_str().to_string_lossy().to_ascii_lowercase())
            }
            Component::RootDir => Some("/".to_owned()),
            Component::CurDir => None,
            Component::ParentDir => Some("..".to_owned()),
        })
        .collect()
}

fn normalize_workspace_path(path: &str) -> Option<String> {
    let normalized = path.trim().replace('\\', "/");
    if normalized.is_empty() || normalized.split('/').any(|component| component == "..") {
        return None;
    }
    Some(
        normalized
            .split('/')
            .filter(|component| !component.is_empty() && *component != ".")
            .collect::<Vec<_>>()
            .join("/"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_request(worktree_id: &str, path: &str) -> ManagedWorktreeCreateRequest {
        ManagedWorktreeCreateRequest {
            worktree_id: worktree_id.to_owned(),
            source_repo: PathBuf::from("C:/repo/palyra"),
            worktree_path: PathBuf::from(path),
            branch_slug: "Feature Branch".to_owned(),
            base_ref: "main".to_owned(),
            now_unix_ms: 1_000,
        }
    }

    #[test]
    fn create_rejects_worktree_inside_source_repo() {
        let mut registry = ManagedWorktreeRegistry::default();
        let error = registry
            .create(create_request("wt-1", "C:/repo/palyra/.tmp/worktree"))
            .expect_err("source-contained worktree should be rejected");

        assert_eq!(error, ManagedWorktreeError::WorktreeInsideSourceRepo);
    }

    #[test]
    fn create_uses_palyra_branch_namespace() {
        let mut registry = ManagedWorktreeRegistry::default();

        let record = registry
            .create(create_request("wt-1", "C:/repo/worktrees/wt-1"))
            .expect("worktree should be created");

        assert_eq!(record.branch, "palyra/feature-branch");
        assert_eq!(record.sandbox_mount, "explicit_rw_worktree_mount");
    }

    #[test]
    fn dirty_remove_requires_snapshot_or_force() {
        let mut registry = ManagedWorktreeRegistry::default();
        registry
            .create(create_request("wt-1", "C:/repo/worktrees/wt-1"))
            .expect("worktree should be created");
        registry.mark_dirty("wt-1", true).expect("worktree should be marked dirty");

        let error = registry
            .remove(ManagedWorktreeRemoveRequest {
                worktree_id: "wt-1".to_owned(),
                force: false,
                snapshot_ref: None,
                now_unix_ms: 2_000,
            })
            .expect_err("dirty remove without snapshot should fail");

        assert_eq!(error, ManagedWorktreeError::DirtyRemoveRequiresSnapshot);
    }

    #[test]
    fn attach_run_returns_workspace_policy() {
        let mut registry = ManagedWorktreeRegistry::default();
        registry
            .create(create_request("wt-1", "C:/repo/worktrees/wt-1"))
            .expect("worktree should be created");

        let attachment = registry.attach_run("wt-1", "run-1", 2_000).expect("run should attach");

        assert_eq!(attachment.workspace_policy, "worktree_scoped_rw");
        assert_eq!(attachment.sandbox_mount, "explicit_rw_worktree_mount");
        assert_eq!(attachment.workspace_root, PathBuf::from("C:/repo/worktrees/wt-1"));
    }

    #[test]
    fn gc_respects_idle_age_locks_and_dirty_state() {
        let mut registry = ManagedWorktreeRegistry::default();
        registry
            .create(create_request("clean", "C:/repo/worktrees/clean"))
            .expect("clean worktree should be created");
        registry
            .create(create_request("locked", "C:/repo/worktrees/locked"))
            .expect("locked worktree should be created");
        registry
            .create(create_request("dirty", "C:/repo/worktrees/dirty"))
            .expect("dirty worktree should be created");
        registry.attach_run("locked", "run-1", 1_500).expect("run should attach");
        registry.mark_dirty("dirty", true).expect("dirty flag should be set");

        let report = registry.gc(10_000, 5_000);

        assert_eq!(report.removed_worktree_ids, vec!["clean"]);
        assert_eq!(report.skipped_locked_worktree_ids, vec!["locked"]);
        assert_eq!(report.skipped_dirty_worktree_ids, vec!["dirty"]);
    }

    #[test]
    fn restore_plans_unstaged_modifications_from_snapshot() {
        let mut registry = ManagedWorktreeRegistry::default();
        registry
            .create(create_request("wt-1", "C:/repo/worktrees/wt-1"))
            .expect("worktree should be created");

        let plan = registry
            .restore(ManagedWorktreeRestoreRequest {
                worktree_id: "wt-1".to_owned(),
                snapshot_ref: "snapshot:abc".to_owned(),
                paths: vec!["src/lib.rs".to_owned(), "./src/lib.rs".to_owned()],
                now_unix_ms: 2_000,
            })
            .expect("restore should be planned");

        assert_eq!(plan.restored_paths, vec!["src/lib.rs"]);
        assert_eq!(plan.working_tree_state, "unstaged_modifications_planned");
    }
}
