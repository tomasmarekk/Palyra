use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use super::local_resource_governor::{
    LocalResourceGovernor, LocalResourceGovernorConfig, ResourceUnitsV1,
};
use super::managed_worktree_executor::{
    ManagedWorktreeCreateRequestV2, ManagedWorktreeExecutor, ManagedWorktreeExecutorConfig,
    ManagedWorktreeRecordV2,
};
use super::process_supervisor::{ProcessSupervisor, ProcessSupervisorConfig};

pub(crate) struct ManagedWorktreeTestFixture {
    pub(crate) executor: Arc<ManagedWorktreeExecutor>,
    pub(crate) supervisor: Arc<ProcessSupervisor>,
    pub(crate) governor: LocalResourceGovernor,
    pub(crate) source_repo: PathBuf,
    pub(crate) managed_root: PathBuf,
    pub(crate) worktree_registry_path: PathBuf,
    pub(crate) git_executable: PathBuf,
    _temp: TempDir,
}

impl ManagedWorktreeTestFixture {
    pub(crate) fn new() -> Self {
        let temp = tempfile::tempdir().expect("managed worktree temp root");
        let source_repo = temp.path().join("source");
        let mut managed_root = temp.path().join("managed");
        fs::create_dir_all(source_repo.as_path()).expect("create source repository");
        fs::create_dir_all(managed_root.as_path()).expect("create managed root");
        managed_root = managed_root.canonicalize().expect("canonical managed root");
        let git_executable = find_git_executable();
        run_git(
            git_executable.as_path(),
            source_repo.as_path(),
            &["init", "--initial-branch=main"],
        );
        run_git(
            git_executable.as_path(),
            source_repo.as_path(),
            &["config", "user.email", "managed-worktree@example.invalid"],
        );
        run_git(
            git_executable.as_path(),
            source_repo.as_path(),
            &["config", "user.name", "Managed Worktree Tests"],
        );
        fs::write(source_repo.join("tracked.txt"), b"base\n").expect("write tracked fixture");
        run_git(git_executable.as_path(), source_repo.as_path(), &["add", "tracked.txt"]);
        run_git(git_executable.as_path(), source_repo.as_path(), &["commit", "-m", "fixture"]);

        let limits = ResourceUnitsV1 {
            processes: 32,
            memory_bytes: 8 * 1024 * 1024 * 1024,
            file_descriptors: 2_048,
            sockets: 512,
            spool_bytes: 128 * 1024 * 1024,
            concurrency: 128,
        };
        let governor = LocalResourceGovernor::open(LocalResourceGovernorConfig {
            registry_path: temp.path().join("state").join("resource-leases.json"),
            global_limit: limits,
            per_owner_limit: limits,
            max_records: 512,
        })
        .expect("open resource governor");
        let supervisor = Arc::new(
            ProcessSupervisor::start(ProcessSupervisorConfig {
                state_root: temp.path().join("state"),
                max_sessions: 16,
                max_retained_chunks_per_session: 64,
                max_retained_bytes_per_session: 512 * 1024,
                max_artifact_bytes_per_session: 4 * 1024 * 1024,
                drain_timeout: Duration::from_secs(3),
                resource_governor: governor.clone(),
            })
            .expect("start process supervisor"),
        );
        let worktree_registry_path = temp.path().join("state").join("worktrees.json");
        let executor = Arc::new(
            ManagedWorktreeExecutor::open(
                ManagedWorktreeExecutorConfig {
                    registry_path: worktree_registry_path.clone(),
                    managed_root: managed_root.clone(),
                    git_executable: git_executable.clone(),
                    max_records: 64,
                },
                Arc::clone(&supervisor),
            )
            .expect("open managed worktree executor"),
        );
        Self {
            executor,
            supervisor,
            governor,
            source_repo,
            managed_root,
            worktree_registry_path,
            git_executable,
            _temp: temp,
        }
    }

    pub(crate) fn create_worktree(&self, worktree_id: &str) -> ManagedWorktreeRecordV2 {
        self.executor
            .create(ManagedWorktreeCreateRequestV2 {
                worktree_id: worktree_id.to_owned(),
                source_repo: self.source_repo.clone(),
                branch_slug: worktree_id.to_owned(),
                base_ref: "HEAD".to_owned(),
            })
            .expect("create managed worktree")
    }

    pub(crate) fn git(&self, cwd: &Path, args: &[&str]) {
        let _ = run_git(self.git_executable.as_path(), cwd, args);
    }

    pub(crate) fn git_output(&self, cwd: &Path, args: &[&str]) -> Vec<u8> {
        run_git(self.git_executable.as_path(), cwd, args)
    }
}

fn find_git_executable() -> PathBuf {
    let path = env::var_os("PATH").expect("PATH contains Git");
    let executable_names: &[&str] = if cfg!(windows) { &["git.exe", "git.cmd"] } else { &["git"] };
    for directory in env::split_paths(&path) {
        for executable_name in executable_names {
            let candidate = directory.join(executable_name);
            if candidate.is_file() {
                return candidate.canonicalize().expect("canonical Git executable");
            }
        }
    }
    panic!("Git executable was not found on PATH");
}

fn run_git(git_executable: &Path, cwd: &Path, args: &[&str]) -> Vec<u8> {
    let output = Command::new(git_executable)
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("run Git fixture command");
    assert!(
        output.status.success(),
        "Git fixture command failed: {:?}; stdout={}; stderr={}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
