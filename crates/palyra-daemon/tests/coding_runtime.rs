use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use palyra_daemon::application::coding_runtime::{
    CodingCapabilityStatusV2, CodingCommandBackendV2, CodingCommandPolicyV2,
    CodingCommandRequestV2, CodingExecutionProfileV2, CodingObjectiveWaitContextV2,
    CodingPatchOutcomeV2, CodingRuntime, CodingRuntimeConfig, CodingRuntimeError,
    CodingSourceEditV2, CodingTaskBeginRequestV2, CodingWaitBarrierReceiptV2, CodingWakeBridge,
    CodingWakeReceiptV2, CodingWorkspaceAdmissionV2, CodingWorkspaceIsolationV2,
    CodingWorktreeDispositionV2,
};
use palyra_daemon::application::local_resource_governor::{
    LocalResourceGovernor, LocalResourceGovernorConfig, ResourcePressureActionStateV1,
    ResourcePriority, ResourceServiceKind, ResourceUnitsV1,
};
use palyra_daemon::application::lsp_document_sync::{
    DiagnosticsDeltaStatusV2, LspDocumentCoordinator, LspDocumentSyncConfig,
};
use palyra_daemon::application::lsp_workspace_supervisor::{
    LspLanguageV2, LspServerCommandPolicyV2, LspWorkspaceSupervisor, LspWorkspaceSupervisorConfig,
};
use palyra_daemon::application::managed_coding_services::{
    ManagedCodingRuntimeServices, ManagedCodingServicesConfig,
};
use palyra_daemon::application::managed_worktree_executor::{
    ManagedWorktreeExecutor, ManagedWorktreeExecutorConfig, ManagedWorktreeLifecycleV2,
};
use palyra_daemon::application::managed_worktree_snapshots::{
    WorktreeSnapshotStore, WorktreeSnapshotStoreConfig,
};
use palyra_daemon::application::process_supervisor::{
    ProcessLaunchSpec, ProcessOwnerV2, ProcessSessionRecordV2, ProcessSupervisor,
    ProcessSupervisorConfig,
};
use serde::Serialize;

const VERIFY_COMMAND: &str = "verify-marker";
const TERMINAL_COMMAND: &str = "terminal-probe";
const DEFAULT_SOAK_ITERATIONS: usize = 8;
const MAX_SOAK_ITERATIONS: usize = 64;

#[derive(Serialize)]
struct CodingRuntimeSoakReportV1 {
    schema_version: u32,
    reason_code: &'static str,
    iterations: usize,
    patch_observations: usize,
    warm_lsp_generation: u64,
    introduced_total: usize,
    resolved_total: usize,
    patch_latency_p50_ms: u64,
    patch_latency_p95_ms: u64,
    patch_latency_max_ms: u64,
    cleanup_active_process_count: usize,
    cleanup_lsp_settled: bool,
    remaining_resource_leases: usize,
}

#[derive(Default)]
struct RecordingWakeBridge {
    registered: Mutex<Vec<String>>,
    completed: Mutex<Vec<String>>,
}

impl CodingWakeBridge for RecordingWakeBridge {
    fn register_process_wait(
        &self,
        _context: &CodingObjectiveWaitContextV2,
        process: &ProcessSessionRecordV2,
    ) -> Result<CodingWaitBarrierReceiptV2, String> {
        self.registered
            .lock()
            .map_err(|_| "recording lock poisoned".to_owned())?
            .push(process.process_session_id.clone());
        Ok(CodingWaitBarrierReceiptV2 {
            barrier_id: format!("barrier-{}", process.process_session_id),
            process_session_id: process.process_session_id.clone(),
            reason_code: "test.wait_registered".to_owned(),
        })
    }

    fn emit_process_completion(
        &self,
        barrier: &CodingWaitBarrierReceiptV2,
        process: &ProcessSessionRecordV2,
    ) -> Result<CodingWakeReceiptV2, String> {
        if barrier.process_session_id != process.process_session_id
            || !process.state.is_terminal()
            || process.outcome.is_none()
        {
            return Err("completion is not settled".to_owned());
        }
        self.completed
            .lock()
            .map_err(|_| "recording lock poisoned".to_owned())?
            .push(process.process_session_id.clone());
        Ok(CodingWakeReceiptV2 {
            barrier_id: barrier.barrier_id.clone(),
            wake_intent_count: 1,
            reason_code: "test.completion_wake".to_owned(),
        })
    }
}

struct Fixture {
    runtime: CodingRuntime,
    documents: Option<Arc<LspDocumentCoordinator>>,
    lsp: Option<Arc<LspWorkspaceSupervisor>>,
    snapshots: Arc<WorktreeSnapshotStore>,
    executor: Arc<ManagedWorktreeExecutor>,
    process: Arc<ProcessSupervisor>,
    governor: LocalResourceGovernor,
    wake: Arc<RecordingWakeBridge>,
    root: tempfile::TempDir,
    source_repo: PathBuf,
    git: PathBuf,
    governor_config: LocalResourceGovernorConfig,
    executor_config: ManagedWorktreeExecutorConfig,
    snapshot_config: WorktreeSnapshotStoreConfig,
    process_state_root: PathBuf,
}

impl Fixture {
    fn new(profile: CodingExecutionProfileV2, provide_lsp: bool) -> Self {
        let root = tempfile::tempdir().expect("temp root");
        let git = find_executable("git").expect("Git executable");
        let source_repo = initialize_source_repo(root.path(), git.as_path());
        let governor_config = LocalResourceGovernorConfig {
            registry_path: root.path().join("state").join("resource-leases.json"),
            global_limit: resource_limit(),
            per_owner_limit: resource_limit(),
            max_records: 1024,
        };
        let governor = LocalResourceGovernor::open(governor_config.clone()).expect("governor");
        let process_state_root = root.path().join("state");
        let process = Arc::new(
            ProcessSupervisor::start(process_config(
                process_state_root.as_path(),
                governor.clone(),
            ))
            .expect("process supervisor"),
        );
        let executor_config = ManagedWorktreeExecutorConfig {
            registry_path: root.path().join("state").join("worktrees.json"),
            managed_root: root.path().join("managed-worktrees"),
            git_executable: git.clone(),
            max_records: 128,
        };
        let executor = Arc::new(
            ManagedWorktreeExecutor::open(executor_config.clone(), Arc::clone(&process))
                .expect("worktree executor"),
        );
        let snapshot_config = WorktreeSnapshotStoreConfig {
            artifact_root: root.path().join("snapshots"),
            max_files: 128,
            max_file_bytes: 2 * 1024 * 1024,
            max_total_bytes: 16 * 1024 * 1024,
        };
        let snapshots = Arc::new(
            WorktreeSnapshotStore::open(
                snapshot_config.clone(),
                Arc::clone(&executor),
                governor.clone(),
            )
            .expect("snapshot store"),
        );
        let (lsp, documents) = if provide_lsp {
            let lsp = Arc::new(
                LspWorkspaceSupervisor::open(lsp_config(root.path()), Arc::clone(&process))
                    .expect("LSP supervisor"),
            );
            let documents = Arc::new(
                LspDocumentCoordinator::open(
                    LspDocumentSyncConfig {
                        artifact_root: root.path().join("diagnostics"),
                        artifact_owner_id: "coding-runtime-tests".to_owned(),
                        max_documents: 64,
                        max_document_bytes: 2 * 1024 * 1024,
                        max_diagnostics_per_document: 256,
                        max_visible_delta_items: 64,
                        max_artifact_bytes: 4 * 1024 * 1024,
                        max_artifacts: 512,
                        diagnostics_timeout: Duration::from_millis(250),
                    },
                    Arc::clone(&lsp),
                )
                .expect("document coordinator"),
            );
            (Some(lsp), Some(documents))
        } else {
            (None, None)
        };
        let wake = Arc::new(RecordingWakeBridge::default());
        let wake_bridge: Arc<dyn CodingWakeBridge> = wake.clone();
        let runtime = CodingRuntime::open(
            runtime_config(profile),
            Arc::clone(&process),
            governor.clone(),
            Arc::clone(&executor),
            Some(Arc::clone(&snapshots)),
            lsp.clone(),
            documents.clone(),
            Some(wake_bridge),
        )
        .expect("coding runtime");
        Self {
            runtime,
            documents,
            lsp,
            snapshots,
            executor,
            process,
            governor,
            wake,
            root,
            source_repo,
            git,
            governor_config,
            executor_config,
            snapshot_config,
            process_state_root,
        }
    }

    fn begin(&self, task_id: &str, language: LspLanguageV2) {
        self.runtime
            .begin_task(CodingTaskBeginRequestV2 {
                task_id: task_id.to_owned(),
                session_id: "session-test".to_owned(),
                run_id: format!("run-{task_id}"),
                workspace_admission: CodingWorkspaceAdmissionV2::Explicit,
                source_repo: self.source_repo.clone(),
                base_ref: "HEAD".to_owned(),
                branch_slug: format!("{task_id}-branch"),
                language,
            })
            .expect("begin coding task");
    }
}

#[test]
fn rust_typescript_and_python_tasks_use_managed_worktrees_and_live_lsp() {
    let fixture = Fixture::new(full_profile(), true);
    for (task_id, language, path, clean_text) in [
        ("rust-task", LspLanguageV2::Rust, "src/lib.rs", "pub fn value() -> u32 { 2 }\n"),
        (
            "typescript-task",
            LspLanguageV2::TypeScript,
            "src/app.ts",
            "export const value: number = 2;\n",
        ),
        (
            "python-task",
            LspLanguageV2::Python,
            "src/main.py",
            "def value() -> int:\n    return 2\n",
        ),
    ] {
        fixture.begin(task_id, language);
        let handle = fixture.runtime.task_handle(task_id).expect("task handle");
        assert_eq!(
            handle.capabilities.workspace_isolation,
            CodingWorkspaceIsolationV2::ManagedWorktree
        );
        assert_eq!(handle.capabilities.persistent_lsp, CodingCapabilityStatusV2::Active);
        let patch = apply_edits(&fixture.runtime, task_id, &[edit(path, clean_text)])
            .expect("apply clean patch");
        assert!(patch.applied);
        assert!(patch.diagnostics_verified);
        assert_eq!(
            patch.diagnostics.as_ref().map(|delta| delta.status),
            Some(DiagnosticsDeltaStatusV2::Verified)
        );
        let cleanup = fixture.runtime.cleanup_task(task_id).expect("cleanup dirty task");
        assert_eq!(cleanup.worktree_disposition, CodingWorktreeDispositionV2::DirtyRetained);
        assert!(cleanup.snapshot_id.is_some());
        assert!(cleanup.lsp_settled);
        assert_eq!(cleanup.active_process_count, 0);
    }
}

#[test]
fn build_failure_repair_and_completion_wake_share_one_process_authority() {
    let fixture = Fixture::new(full_profile(), true);
    fixture.begin("repair-task", LspLanguageV2::Rust);
    let context = objective_wait();
    let initial = fixture
        .runtime
        .run_command(CodingCommandRequestV2 {
            task_id: "repair-task".to_owned(),
            command_id: VERIFY_COMMAND.to_owned(),
            objective_wait: Some(context.clone()),
        })
        .expect("initial build");
    assert_eq!(initial.exit_code, Some(0));
    assert!(initial.cleanup_verified);
    assert!(initial.wake.is_some());

    apply_edits(&fixture.runtime, "repair-task", &[edit("build.flag", "broken\n")])
        .expect("break build");
    let failed = fixture
        .runtime
        .run_command(CodingCommandRequestV2 {
            task_id: "repair-task".to_owned(),
            command_id: VERIFY_COMMAND.to_owned(),
            objective_wait: None,
        })
        .expect("failed build outcome");
    assert_ne!(failed.exit_code, Some(0));
    assert!(failed.cleanup_verified);

    apply_edits(&fixture.runtime, "repair-task", &[edit("build.flag", "ready\n")])
        .expect("repair build");
    let repaired = fixture
        .runtime
        .run_command(CodingCommandRequestV2 {
            task_id: "repair-task".to_owned(),
            command_id: VERIFY_COMMAND.to_owned(),
            objective_wait: Some(context),
        })
        .expect("repaired build");
    assert_eq!(repaired.exit_code, Some(0));
    assert!(repaired.cleanup_verified);
    assert_eq!(fixture.wake.registered.lock().expect("registered").len(), 2);
    assert_eq!(fixture.wake.completed.lock().expect("completed").len(), 2);

    let cleanup = fixture.runtime.cleanup_task("repair-task").expect("cleanup");
    assert_eq!(cleanup.worktree_disposition, CodingWorktreeDispositionV2::Removed);
    assert_eq!(cleanup.active_process_count, 0);
}

#[test]
fn native_pty_is_real_and_disabled_pty_fallback_is_explicit() {
    let fixture = Fixture::new(full_profile(), true);
    fixture.begin("pty-task", LspLanguageV2::Rust);
    let native = fixture
        .runtime
        .run_command(CodingCommandRequestV2 {
            task_id: "pty-task".to_owned(),
            command_id: TERMINAL_COMMAND.to_owned(),
            objective_wait: None,
        })
        .expect("native PTY command");
    #[cfg(not(windows))]
    {
        assert_eq!(native.backend, CodingCommandBackendV2::NativePty);
        assert!(native.pty_backend.is_some());
        assert_eq!(native.exit_code, Some(0));
    }
    #[cfg(windows)]
    if windows_native_pty_required() {
        assert_eq!(native.backend, CodingCommandBackendV2::NativePty);
        assert!(native.pty_backend.is_some());
        assert_eq!(native.exit_code, Some(0));
    }
    #[cfg(windows)]
    if !windows_native_pty_required() {
        assert_eq!(native.backend, CodingCommandBackendV2::ProcessWithoutPty);
        assert!(native.pty_backend.is_none());
        assert!(native
            .reason_codes
            .iter()
            .any(|reason| reason == "coding.pty_disabled_process_fallback"));
    }
    assert!(native.cleanup_verified);
    fixture.runtime.cleanup_task("pty-task").expect("native cleanup");

    let mut profile = full_profile();
    profile.native_pty_enabled = false;
    let degraded = Fixture::new(profile, true);
    degraded.begin("pipe-task", LspLanguageV2::Rust);
    let outcome = degraded
        .runtime
        .run_command(CodingCommandRequestV2 {
            task_id: "pipe-task".to_owned(),
            command_id: TERMINAL_COMMAND.to_owned(),
            objective_wait: None,
        })
        .expect("explicit pipe fallback");
    assert_eq!(outcome.backend, CodingCommandBackendV2::ProcessWithoutPty);
    assert!(outcome
        .reason_codes
        .iter()
        .any(|reason| reason == "coding.pty_disabled_process_fallback"));
    degraded.runtime.cleanup_task("pipe-task").expect("degraded cleanup");
}

#[test]
fn unavailable_lsp_uses_explicit_cli_fallback_and_retains_dirty_snapshot() {
    let fixture = Fixture::new(full_profile(), false);
    fixture.begin("fallback-task", LspLanguageV2::Python);
    let handle = fixture.runtime.task_handle("fallback-task").expect("task handle");
    assert_eq!(handle.capabilities.persistent_lsp, CodingCapabilityStatusV2::Degraded);
    let patch = apply_edits(
        &fixture.runtime,
        "fallback-task",
        &[edit("src/main.py", "def value() -> int:\n    return 9\n")],
    )
    .expect("fallback patch");
    assert!(patch.diagnostics.is_none());
    assert_eq!(
        patch.fallback.as_ref().map(|fallback| fallback.command_label.as_str()),
        Some("pyright")
    );
    assert!(!patch.diagnostics_verified);

    let cleanup = fixture.runtime.cleanup_task("fallback-task").expect("cleanup");
    assert_eq!(cleanup.worktree_disposition, CodingWorktreeDispositionV2::DirtyRetained);
    let snapshot_id = cleanup.snapshot_id.expect("snapshot");
    assert!(fixture.snapshots.list().expect("snapshots").contains(&snapshot_id));
    let record = fixture
        .executor
        .list()
        .expect("worktrees")
        .into_iter()
        .find(|record| record.worktree_id == "fallback-task")
        .expect("retained record");
    assert_eq!(record.lifecycle, ManagedWorktreeLifecycleV2::Retained);
    assert!(record.locked_by_run.is_none());
    assert!(record.worktree_path.exists());
}

#[test]
fn restart_during_build_releases_services_then_reconciles_lock_without_pid_adoption() {
    let fixture = Fixture::new(full_profile(), true);
    fixture.begin("restart-task", LspLanguageV2::Rust);
    apply_edits(
        &fixture.runtime,
        "restart-task",
        &[edit("src/lib.rs", "pub fn value() -> u32 { 7 }\n")],
    )
    .expect("dirty task");
    let handle = fixture.runtime.task_handle("restart-task").expect("task");
    let long_process = fixture
        .process
        .launch(long_process_spec(&handle.workspace_root, handle.run_id.as_str()))
        .expect("long build");
    assert!(!fixture
        .process
        .status(long_process.process_session_id.as_str())
        .expect("running")
        .state
        .is_terminal());

    let Fixture {
        runtime,
        documents,
        lsp,
        snapshots,
        executor,
        process,
        governor,
        wake: _wake,
        root,
        source_repo: _source_repo,
        git: _git,
        governor_config,
        executor_config,
        snapshot_config,
        process_state_root,
    } = fixture;
    drop(runtime);
    drop(documents);
    drop(lsp);
    drop(snapshots);
    drop(executor);
    drop(process);
    drop(governor);

    let governor = LocalResourceGovernor::open(governor_config).expect("restart governor");
    let process = Arc::new(
        ProcessSupervisor::start(process_config(process_state_root.as_path(), governor.clone()))
            .expect("restart process supervisor"),
    );
    let executor = Arc::new(
        ManagedWorktreeExecutor::open(executor_config, Arc::clone(&process))
            .expect("restart executor"),
    );
    let snapshots = Arc::new(
        WorktreeSnapshotStore::open(snapshot_config, Arc::clone(&executor), governor.clone())
            .expect("restart snapshot store"),
    );
    let mut recovery_profile = full_profile();
    recovery_profile.persistent_lsp_enabled = false;
    recovery_profile.native_pty_enabled = false;
    let recovery = CodingRuntime::open(
        runtime_config(recovery_profile),
        process,
        governor,
        executor,
        Some(snapshots),
        None,
        None,
        None,
    )
    .expect("recovery runtime");
    let outcome = recovery
        .reconcile_interrupted_task("restart-task", "run-restart-task")
        .expect("reconcile interrupted task");
    assert_eq!(outcome.worktree_disposition, CodingWorktreeDispositionV2::DirtyRetained);
    assert!(outcome.snapshot_id.is_some());
    assert!(outcome.worktree_lock_released);
    assert_eq!(outcome.active_process_count, 0);
    assert_eq!(outcome.reason_codes, ["coding.restart_reconciled_without_pid_adoption"]);
    drop(root);
}

#[test]
fn warm_lsp_repeated_diagnostics_soak_is_bounded_and_leaves_no_services() {
    let fixture = Fixture::new(full_profile(), true);
    fixture.begin("soak-task", LspLanguageV2::Rust);
    let handle = fixture.runtime.task_handle("soak-task").expect("task handle");
    let generation = handle.lsp_handle.as_ref().expect("live LSP").generation;
    let iterations = soak_iterations();
    let mut latencies_ms = Vec::with_capacity(iterations.saturating_mul(2));
    let mut introduced_total = 0;
    let mut resolved_total = 0;

    for iteration in 0..iterations {
        let introduced_started = Instant::now();
        let introduced = apply_edits(
            &fixture.runtime,
            "soak-task",
            &[edit(
                "src/lib.rs",
                format!(
                    "pub fn value() -> u32 {{ {iteration} }}\n// ERROR iteration {iteration}\n"
                )
                .as_str(),
            )],
        )
        .expect("introduce diagnostic");
        latencies_ms.push(elapsed_ms(introduced_started));
        let introduced = introduced.diagnostics.expect("introduced diagnostics");
        assert_eq!(introduced.status, DiagnosticsDeltaStatusV2::BlockingDiagnostics);
        assert_eq!(introduced.introduced_count, 1);
        assert_eq!(introduced.result_server_generation, Some(generation));
        introduced_total += introduced.introduced_count;

        let resolved_started = Instant::now();
        let resolved = apply_edits(
            &fixture.runtime,
            "soak-task",
            &[edit(
                "src/lib.rs",
                format!("pub fn value() -> u32 {{ {} }}\n", iteration.saturating_add(1)).as_str(),
            )],
        )
        .expect("resolve diagnostic");
        latencies_ms.push(elapsed_ms(resolved_started));
        let resolved = resolved.diagnostics.expect("resolved diagnostics");
        assert_eq!(resolved.status, DiagnosticsDeltaStatusV2::Verified);
        assert_eq!(resolved.resolved_count, 1);
        assert_eq!(resolved.result_server_generation, Some(generation));
        resolved_total += resolved.resolved_count;

        let build = fixture
            .runtime
            .run_command(CodingCommandRequestV2 {
                task_id: "soak-task".to_owned(),
                command_id: VERIFY_COMMAND.to_owned(),
                objective_wait: None,
            })
            .expect("repeat build");
        assert_eq!(build.exit_code, Some(0));
        assert!(build.cleanup_verified);
        assert_eq!(
            fixture
                .runtime
                .task_handle("soak-task")
                .expect("warm task")
                .lsp_handle
                .expect("warm LSP")
                .generation,
            generation
        );
    }

    latencies_ms.sort_unstable();
    let cleanup = fixture.runtime.cleanup_task("soak-task").expect("soak cleanup");
    assert_eq!(cleanup.active_process_count, 0);
    assert!(cleanup.lsp_settled);
    let remaining_resource_leases =
        fixture.governor.active_leases().expect("active resource leases").len();
    assert_eq!(remaining_resource_leases, 0);
    let report = CodingRuntimeSoakReportV1 {
        schema_version: 1,
        reason_code: "coding_runtime.soak_completed",
        iterations,
        patch_observations: latencies_ms.len(),
        warm_lsp_generation: generation,
        introduced_total,
        resolved_total,
        patch_latency_p50_ms: percentile(&latencies_ms, 50),
        patch_latency_p95_ms: percentile(&latencies_ms, 95),
        patch_latency_max_ms: latencies_ms.last().copied().unwrap_or_default(),
        cleanup_active_process_count: cleanup.active_process_count,
        cleanup_lsp_settled: cleanup.lsp_settled,
        remaining_resource_leases,
    };
    assert!(report.patch_latency_p95_ms <= 2_000, "warm LSP p95 exceeded 2 seconds");
    write_soak_report(&report);
}

#[test]
fn resource_pressure_evicts_idle_lsp_and_publishes_redacted_evidence() {
    let root = tempfile::tempdir().expect("temp root");
    let git = find_executable("git").expect("Git executable");
    let source_repo = initialize_source_repo(root.path(), git.as_path());
    let services = ManagedCodingRuntimeServices::open(ManagedCodingServicesConfig {
        state_root: root.path().join("managed-state"),
        managed_worktree_root: root.path().join("managed-worktrees"),
        git_executable: git,
        profile: full_profile(),
        command_policies: Vec::new(),
        lsp_policies: vec![lsp_policy(LspLanguageV2::Rust)],
        lsp_network_isolation_verified: true,
        lsp_idle_ttl: Duration::from_secs(30),
    })
    .expect("managed coding services");
    let task = services
        .runtime()
        .begin_task(CodingTaskBeginRequestV2 {
            task_id: "pressure-runtime".to_owned(),
            session_id: "pressure-session".to_owned(),
            run_id: "pressure-run".to_owned(),
            workspace_admission: CodingWorkspaceAdmissionV2::Policy,
            source_repo,
            base_ref: "HEAD".to_owned(),
            branch_slug: "pressure-runtime".to_owned(),
            language: LspLanguageV2::Rust,
        })
        .expect("begin pressure task");
    assert_eq!(task.capabilities.persistent_lsp, CodingCapabilityStatusV2::Active);

    let actions = services
        .relieve_pressure(ResourceUnitsV1 {
            processes: 64,
            memory_bytes: 16 * 1024 * 1024 * 1024,
            file_descriptors: 8_192,
            sockets: 256,
            spool_bytes: 512 * 1024 * 1024,
            concurrency: 256,
        })
        .expect("apply pressure relief");
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].state, ResourcePressureActionStateV1::Applied);
    assert_eq!(actions[0].reason_code, "resource.lsp_eviction_applied");

    let diagnostics = services.diagnostics_snapshot();
    assert_eq!(
        diagnostics.language_services.as_ref().expect("language diagnostics").active_servers,
        0
    );
    assert_eq!(diagnostics.pressure_actions[0].reason_code, "resource.lsp_eviction_applied");
    let serialized = serde_json::to_string(&diagnostics).expect("serialize diagnostics");
    assert!(!serialized.contains("worktree-pressure-runtime"));
    assert!(!serialized.contains("pressure-run"));
}

fn full_profile() -> CodingExecutionProfileV2 {
    CodingExecutionProfileV2 {
        managed_worktree_enabled: true,
        in_place_workspace_fallback_allowed: false,
        persistent_lsp_enabled: true,
        cli_diagnostics_fallback_allowed: true,
        native_pty_enabled: !cfg!(windows) || windows_native_pty_required(),
        process_fallback_without_pty_allowed: true,
        retain_dirty_worktrees: true,
    }
}

fn windows_native_pty_required() -> bool {
    std::env::var_os("CI").is_some()
}

fn runtime_config(profile: CodingExecutionProfileV2) -> CodingRuntimeConfig {
    CodingRuntimeConfig {
        profile,
        max_tasks: 16,
        max_patch_files: 16,
        max_source_file_bytes: 2 * 1024 * 1024,
        max_command_output_chunks: 128,
        process_drain_allowance: Duration::from_secs(3),
        command_policies: vec![verification_policy(), terminal_policy()],
    }
}

fn verification_policy() -> CodingCommandPolicyV2 {
    #[cfg(windows)]
    let (executable, args, env) = (
        system_command(),
        vec![
            "/D".to_owned(),
            "/V:ON".to_owned(),
            "/S".to_owned(),
            "/C".to_owned(),
            "set /p marker=<build.flag & if \"!marker!\"==\"ready\" (exit /b 0) else (exit /b 1)"
                .to_owned(),
        ],
        windows_environment(),
    );
    #[cfg(not(windows))]
    let (executable, args, env) = (
        PathBuf::from("/bin/sh"),
        vec!["-c".to_owned(), "grep -qx ready build.flag".to_owned()],
        BTreeMap::from([("PATH".to_owned(), "/usr/bin:/bin".to_owned())]),
    );
    CodingCommandPolicyV2 {
        command_id: VERIFY_COMMAND.to_owned(),
        executable,
        args,
        env,
        requires_terminal: false,
        timeout: Duration::from_secs(10),
        no_output_timeout: None,
        resource_units: command_resources(),
    }
}

fn terminal_policy() -> CodingCommandPolicyV2 {
    CodingCommandPolicyV2 {
        command_id: TERMINAL_COMMAND.to_owned(),
        executable: PathBuf::from(env!("CARGO_BIN_EXE_palyra-pty-fixture")),
        args: vec!["probe".to_owned()],
        env: {
            #[cfg(windows)]
            {
                windows_environment()
            }
            #[cfg(not(windows))]
            {
                BTreeMap::new()
            }
        },
        requires_terminal: true,
        timeout: Duration::from_secs(10),
        no_output_timeout: None,
        resource_units: command_resources(),
    }
}

fn lsp_config(root: &Path) -> LspWorkspaceSupervisorConfig {
    LspWorkspaceSupervisorConfig {
        registry_path: root.join("state").join("lsp-registry.json"),
        max_servers: 8,
        max_registry_entries: 128,
        max_header_bytes: 8 * 1024,
        max_message_bytes: 256 * 1024,
        max_notifications: 256,
        initialize_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_millis(500),
        server_lifetime: Duration::from_secs(60),
        idle_ttl: Duration::from_secs(30),
        broken_ttl: Duration::from_millis(100),
        circuit_breaker_failures: 2,
        network_isolation_verified: true,
        resource_units: ResourceUnitsV1 {
            processes: 1,
            memory_bytes: 128 * 1024 * 1024,
            file_descriptors: 32,
            sockets: 0,
            spool_bytes: 2 * 1024 * 1024,
            concurrency: 1,
        },
        policies: [LspLanguageV2::Rust, LspLanguageV2::TypeScript, LspLanguageV2::Python]
            .into_iter()
            .map(lsp_policy)
            .collect(),
    }
}

fn lsp_policy(language: LspLanguageV2) -> LspServerCommandPolicyV2 {
    let env = BTreeMap::from([("PALYRA_LSP_FIXTURE_MODE".to_owned(), "normal".to_owned())]);
    #[cfg(windows)]
    let env = {
        let mut env = env;
        env.extend(windows_environment());
        env
    };
    LspServerCommandPolicyV2 {
        language,
        executable: PathBuf::from(env!("CARGO_BIN_EXE_palyra-lsp-fixture")),
        args: Vec::new(),
        env,
        toolchain_fingerprint: format!("{language:?}-coding-fixture-v1"),
        network_allowed: false,
    }
}

fn process_config(state_root: &Path, governor: LocalResourceGovernor) -> ProcessSupervisorConfig {
    ProcessSupervisorConfig {
        state_root: state_root.to_path_buf(),
        max_sessions: 32,
        max_retained_chunks_per_session: 512,
        max_retained_bytes_per_session: 4 * 1024 * 1024,
        max_artifact_bytes_per_session: 16 * 1024 * 1024,
        drain_timeout: Duration::from_secs(3),
        resource_governor: governor,
    }
}

fn initialize_source_repo(root: &Path, git: &Path) -> PathBuf {
    let source = root.join("source");
    fs::create_dir_all(source.join("src")).expect("source tree");
    fs::write(source.join("src/lib.rs"), "pub fn value() -> u32 { 1 }\n").expect("Rust");
    fs::write(source.join("src/app.ts"), "export const value: number = 1;\n").expect("TypeScript");
    fs::write(source.join("src/main.py"), "def value() -> int:\n    return 1\n").expect("Python");
    fs::write(source.join("build.flag"), "ready\n").expect("build marker");
    git_ok(git, source.as_path(), &["init", "-q"]);
    git_ok(git, source.as_path(), &["config", "user.name", "Palyra Test"]);
    git_ok(git, source.as_path(), &["config", "user.email", "palyra-test@example.invalid"]);
    git_ok(git, source.as_path(), &["add", "."]);
    git_ok(git, source.as_path(), &["commit", "-qm", "initial"]);
    source.canonicalize().expect("canonical source")
}

fn git_ok(git: &Path, cwd: &Path, args: &[&str]) {
    let output = Command::new(git)
        .args(args)
        .current_dir(cwd)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("run Git");
    assert!(output.status.success(), "Git failed: {}", String::from_utf8_lossy(&output.stderr));
}

fn find_executable(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    let extensions = if cfg!(windows) {
        std::env::var_os("PATHEXT")
            .map(|value| {
                value
                    .to_string_lossy()
                    .split(';')
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![".EXE".to_owned()])
    } else {
        vec![String::new()]
    };
    std::env::split_paths(&path).find_map(|directory| {
        extensions.iter().find_map(|extension| {
            let candidate = directory.join(format!("{name}{extension}"));
            candidate.is_file().then(|| candidate.canonicalize().unwrap_or(candidate))
        })
    })
}

fn edit(path: &str, text: &str) -> CodingSourceEditV2 {
    CodingSourceEditV2 { relative_path: PathBuf::from(path), text: text.to_owned() }
}

fn apply_edits(
    runtime: &CodingRuntime,
    task_id: &str,
    edits: &[CodingSourceEditV2],
) -> Result<CodingPatchOutcomeV2, CodingRuntimeError> {
    let paths = edits.iter().map(|edit| edit.relative_path.clone()).collect::<Vec<_>>();
    let ticket = runtime.prepare_patch_verification(task_id, paths.as_slice())?;
    let workspace_root = runtime.task_handle(task_id)?.workspace_root;
    for edit in edits {
        let target = workspace_root.join(edit.relative_path.as_path());
        let result = target
            .parent()
            .map_or(Ok(()), fs::create_dir_all)
            .and_then(|()| fs::write(target, edit.text.as_bytes()));
        if let Err(error) = result {
            runtime.cancel_patch_verification(ticket.ticket_id.as_str());
            return Err(CodingRuntimeError::Io(error.to_string()));
        }
    }
    runtime.complete_patch_verification(ticket.ticket_id.as_str())
}

fn objective_wait() -> CodingObjectiveWaitContextV2 {
    CodingObjectiveWaitContextV2 {
        objective_attempt_id: "attempt-test".to_owned(),
        session_id: "session-test".to_owned(),
        root_run_id: "root-run-test".to_owned(),
        attempt_generation: 1,
        continuation_prompt: "Continue after verification completes.".to_owned(),
        budget_tokens: 512,
        expires_at_unix_ms: now_ms().saturating_add(60_000),
    }
}

fn long_process_spec(workspace: &Path, run_id: &str) -> ProcessLaunchSpec {
    #[cfg(windows)]
    let (executable, args, env) = (
        system_command(),
        vec![
            "/D".to_owned(),
            "/S".to_owned(),
            "/C".to_owned(),
            "ping -n 30 127.0.0.1 >nul".to_owned(),
        ],
        windows_environment(),
    );
    #[cfg(not(windows))]
    let (executable, args, env) = (
        PathBuf::from("/bin/sh"),
        vec!["-c".to_owned(), "sleep 30".to_owned()],
        BTreeMap::from([("PATH".to_owned(), "/usr/bin:/bin".to_owned())]),
    );
    ProcessLaunchSpec {
        executable,
        args,
        cwd: workspace.to_path_buf(),
        env,
        owner: ProcessOwnerV2 {
            session_id: "session-test".to_owned(),
            run_id: run_id.to_owned(),
            turn_id: "restart-build".to_owned(),
            agent_id: "coding-runtime-test".to_owned(),
            correlation_id: "restart-build-process".to_owned(),
        },
        timeout: Duration::from_secs(60),
        no_output_timeout: None,
        lease_duration: Duration::from_secs(90),
        resource_priority: ResourcePriority::Foreground,
        resource_service: ResourceServiceKind::Process,
        resource_units: command_resources(),
    }
}

fn command_resources() -> ResourceUnitsV1 {
    ResourceUnitsV1 {
        processes: 1,
        memory_bytes: 128 * 1024 * 1024,
        file_descriptors: 32,
        sockets: 0,
        spool_bytes: 2 * 1024 * 1024,
        concurrency: 1,
    }
}

fn resource_limit() -> ResourceUnitsV1 {
    ResourceUnitsV1 {
        processes: 128,
        memory_bytes: 16 * 1024 * 1024 * 1024,
        file_descriptors: 8192,
        sockets: 512,
        spool_bytes: 1024 * 1024 * 1024,
        concurrency: 512,
    }
}

#[cfg(windows)]
fn system_command() -> PathBuf {
    PathBuf::from(
        std::env::var_os("COMSPEC").unwrap_or_else(|| "C:\\Windows\\System32\\cmd.exe".into()),
    )
}

#[cfg(windows)]
fn windows_environment() -> BTreeMap<String, String> {
    ["SYSTEMROOT", "WINDIR", "TEMP", "TMP", "PATH", "PATHEXT"]
        .into_iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
        .collect()
}

fn now_ms() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
        .unwrap_or(i64::MAX)
}

fn soak_iterations() -> usize {
    std::env::var("PALYRA_CODING_RUNTIME_SOAK_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=MAX_SOAK_ITERATIONS).contains(value))
        .unwrap_or(DEFAULT_SOAK_ITERATIONS)
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted
        .len()
        .saturating_mul(percentile)
        .saturating_add(99)
        .checked_div(100)
        .unwrap_or(1)
        .max(1);
    sorted[rank.saturating_sub(1).min(sorted.len().saturating_sub(1))]
}

fn write_soak_report(report: &CodingRuntimeSoakReportV1) {
    let Some(path) = std::env::var_os("PALYRA_CODING_RUNTIME_SOAK_REPORT") else {
        return;
    };
    let path = PathBuf::from(path);
    let parent = path.parent().expect("soak report parent");
    fs::create_dir_all(parent).expect("create soak report directory");
    let bytes = serde_json::to_vec_pretty(report).expect("serialize soak report");
    fs::write(path, bytes).expect("write soak report");
}
