use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use palyra_daemon::application::local_resource_governor::{
    LocalResourceGovernor, LocalResourceGovernorConfig, ResourceUnitsV1,
};
use palyra_daemon::application::lsp_document_sync::{
    fallback_tool_for_language, DiagnosticsDeltaStatusV2, DiagnosticsFallbackToolV2,
    LspDocumentChangeV2, LspDocumentCoordinator, LspDocumentOpenRequestV2, LspDocumentSyncConfig,
    LspDocumentSyncError,
};
use palyra_daemon::application::lsp_workspace_supervisor::{
    LspLanguageV2, LspServerCommandPolicyV2, LspServerHandleV2, LspWorkspaceOpenRequestV2,
    LspWorkspaceSupervisor, LspWorkspaceSupervisorConfig,
};
use palyra_daemon::application::process_supervisor::{ProcessSupervisor, ProcessSupervisorConfig};

struct Fixture {
    documents: LspDocumentCoordinator,
    lsp: Arc<LspWorkspaceSupervisor>,
    _process: Arc<ProcessSupervisor>,
    _governor: LocalResourceGovernor,
    _root: tempfile::TempDir,
    workspace: PathBuf,
}

impl Fixture {
    fn new(mode: &str) -> Self {
        let root = tempfile::tempdir().expect("temp root");
        let workspace = root.path().join("workspace");
        fs::create_dir_all(workspace.join("src")).expect("workspace");
        let governor = LocalResourceGovernor::open(LocalResourceGovernorConfig {
            registry_path: root.path().join("state").join("resource-leases.json"),
            global_limit: resource_limit(),
            per_owner_limit: resource_limit(),
            max_records: 256,
        })
        .expect("governor");
        let process = Arc::new(
            ProcessSupervisor::start(ProcessSupervisorConfig {
                state_root: root.path().join("state"),
                max_sessions: 16,
                max_retained_chunks_per_session: 256,
                max_retained_bytes_per_session: 2 * 1024 * 1024,
                max_artifact_bytes_per_session: 4 * 1024 * 1024,
                drain_timeout: Duration::from_secs(2),
                resource_governor: governor.clone(),
            })
            .expect("process supervisor"),
        );
        let lsp = Arc::new(
            LspWorkspaceSupervisor::open(
                LspWorkspaceSupervisorConfig {
                    registry_path: root.path().join("state").join("lsp-registry.json"),
                    max_servers: 4,
                    max_registry_entries: 32,
                    max_header_bytes: 8 * 1024,
                    max_message_bytes: 256 * 1024,
                    max_notifications: 128,
                    initialize_timeout: Duration::from_millis(500),
                    request_timeout: Duration::from_millis(500),
                    server_lifetime: Duration::from_secs(30),
                    idle_ttl: Duration::from_secs(10),
                    broken_ttl: Duration::from_millis(50),
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
                    policies: vec![policy(mode)],
                },
                Arc::clone(&process),
            )
            .expect("LSP supervisor"),
        );
        let documents = LspDocumentCoordinator::open(
            LspDocumentSyncConfig {
                artifact_root: root.path().join("artifacts"),
                artifact_owner_id: "run-lsp-document-sync-test".to_owned(),
                max_documents: 16,
                max_document_bytes: 1024 * 1024,
                max_diagnostics_per_document: 128,
                max_visible_delta_items: 32,
                max_artifact_bytes: 2 * 1024 * 1024,
                max_artifacts: 64,
                diagnostics_timeout: Duration::from_millis(150),
            },
            Arc::clone(&lsp),
        )
        .expect("document coordinator");
        Self { documents, lsp, _process: process, _governor: governor, _root: root, workspace }
    }

    fn ensure(&self) -> LspServerHandleV2 {
        self.lsp
            .ensure(LspWorkspaceOpenRequestV2 {
                workspace_root: self.workspace.clone(),
                worktree_id: "worktree-test".to_owned(),
                run_id: "run-test".to_owned(),
                language: LspLanguageV2::Rust,
            })
            .expect("ensure LSP")
    }

    fn open(&self, handle: &LspServerHandleV2, path: &str, text: &str) {
        self.documents
            .open_document(LspDocumentOpenRequestV2 {
                handle: handle.clone(),
                workspace_root: self.workspace.clone(),
                relative_path: PathBuf::from(path),
                language_id: "rust".to_owned(),
                text: text.to_owned(),
            })
            .expect("open document");
    }

    fn baseline(
        &self,
        handle: &LspServerHandleV2,
        paths: &[&str],
    ) -> palyra_daemon::application::lsp_document_sync::DiagnosticsBaselineDescriptorV2 {
        self.documents
            .capture_baseline(
                handle,
                paths.iter().map(PathBuf::from).collect::<Vec<_>>().as_slice(),
            )
            .expect("capture baseline")
    }
}

#[test]
fn insert_and_delete_line_shifts_remain_preexisting() {
    let fixture = Fixture::new("normal");
    let handle = fixture.ensure();
    fixture.open(&handle, "src/space file.rs", "first\nsecond\nERROR\n");
    let baseline = fixture.baseline(&handle, &["src/space file.rs"]);
    let inserted = fixture
        .documents
        .verify_changes(
            &handle,
            &baseline,
            &[change("src/space file.rs", "prefix\nfirst\nsecond\nERROR\n")],
        )
        .expect("verify insertion");
    assert_eq!(inserted.status, DiagnosticsDeltaStatusV2::Verified);
    assert_eq!(inserted.unchanged_count, 1);
    assert_eq!(inserted.unchanged[0].line_shift, 1);
    assert_eq!(inserted.introduced_count, 0);

    let shifted_baseline = fixture.baseline(&handle, &["src/space file.rs"]);
    let deleted = fixture
        .documents
        .verify_changes(
            &handle,
            &shifted_baseline,
            &[change("src/space file.rs", "second\nERROR\n")],
        )
        .expect("verify deletion");
    assert_eq!(deleted.status, DiagnosticsDeltaStatusV2::Verified);
    assert_eq!(deleted.unchanged_count, 1);
    assert_eq!(deleted.unchanged[0].line_shift, -2);
}

#[test]
fn multiple_files_report_introduced_resolved_and_unchanged_with_full_artifact() {
    let fixture = Fixture::new("normal");
    let handle = fixture.ensure();
    fixture.open(&handle, "src/a.rs", "ERROR\n");
    fixture.open(&handle, "src/b.rs", "clean\n");
    let baseline = fixture.baseline(&handle, &["src/a.rs", "src/b.rs"]);
    let delta = fixture
        .documents
        .verify_changes(
            &handle,
            &baseline,
            &[change("src/a.rs", "prefix\nERROR\n"), change("src/b.rs", "ERROR\n")],
        )
        .expect("verify files");
    assert_eq!(delta.status, DiagnosticsDeltaStatusV2::BlockingDiagnostics);
    assert_eq!(delta.introduced_count, 1);
    assert_eq!(delta.unchanged_count, 1);
    assert_eq!(delta.resolved_count, 0);
    assert_eq!(delta.blocking_introduced_count, 1);
    assert!(!delta.verified());
    assert!(delta.full_diagnostics_artifact.is_some());
}

#[test]
fn server_restart_refuses_cross_generation_delta_and_selects_cli_fallback() {
    let fixture = Fixture::new("normal");
    let first = fixture.ensure();
    fixture.open(&first, "src/lib.rs", "clean\n");
    let baseline = fixture.baseline(&first, &["src/lib.rs"]);
    fixture.lsp.evict(first.handle_id.as_str()).expect("evict first server");
    let restarted = fixture.ensure();
    assert!(restarted.generation > first.generation);

    let delta = fixture
        .documents
        .verify_changes(&restarted, &baseline, &[change("src/lib.rs", "ERROR\n")])
        .expect("generation outcome");
    assert_eq!(delta.status, DiagnosticsDeltaStatusV2::ServerGenerationChanged);
    assert_eq!(
        delta.fallback.as_ref().map(|fallback| fallback.tool),
        Some(DiagnosticsFallbackToolV2::CargoCheck)
    );
    assert!(delta.full_diagnostics_artifact.is_none());
}

#[test]
fn missing_post_change_diagnostics_is_an_explicit_timeout_with_fallback() {
    let fixture = Fixture::new("diagnostics_once");
    let handle = fixture.ensure();
    fixture.open(&handle, "src/lib.rs", "clean\n");
    let baseline = fixture.baseline(&handle, &["src/lib.rs"]);
    let delta = fixture
        .documents
        .verify_changes(&handle, &baseline, &[change("src/lib.rs", "ERROR\n")])
        .expect("timeout outcome");
    assert_eq!(delta.status, DiagnosticsDeltaStatusV2::DiagnosticsTimedOut);
    assert_eq!(delta.reason_codes, ["lsp.diagnostics_timeout"]);
    assert!(!delta.verified());
    assert_eq!(
        fixture
            .documents
            .document_state(handle.handle_id.as_str(), Path::new("src/lib.rs"))
            .expect("state")
            .diagnostics_version,
        None
    );
}

#[test]
fn no_diagnostics_on_open_returns_typed_timeout_instead_of_empty_success() {
    let fixture = Fixture::new("no_diagnostics");
    let handle = fixture.ensure();
    let error = fixture
        .documents
        .open_document(LspDocumentOpenRequestV2 {
            handle,
            workspace_root: fixture.workspace.clone(),
            relative_path: PathBuf::from("src/lib.rs"),
            language_id: "rust".to_owned(),
            text: "clean\n".to_owned(),
        })
        .expect_err("diagnostics timeout");
    assert!(matches!(error, LspDocumentSyncError::DiagnosticsTimedOut));
}

#[test]
fn rollback_advances_version_and_restores_exact_diagnostics_state() {
    let fixture = Fixture::new("normal");
    let handle = fixture.ensure();
    fixture.open(&handle, "src/lib.rs", "clean\n");
    let baseline = fixture.baseline(&handle, &["src/lib.rs"]);
    let blocking = fixture
        .documents
        .verify_changes(&handle, &baseline, &[change("src/lib.rs", "ERROR\n")])
        .expect("blocking delta");
    assert_eq!(blocking.status, DiagnosticsDeltaStatusV2::BlockingDiagnostics);

    let rollback = fixture
        .documents
        .synchronize_rollback(&handle, &[change("src/lib.rs", "clean\n")])
        .expect("rollback");
    assert!(rollback.synchronized);
    assert_eq!(rollback.documents[0].document_version, 3);
    assert_eq!(rollback.documents[0].diagnostics_version, Some(3));
    assert_eq!(rollback.documents[0].diagnostic_count, 0);
    let restored = fixture.baseline(&handle, &["src/lib.rs"]);
    assert_eq!(restored.documents[0].diagnostic_count, 0);
}

#[test]
fn language_fallback_matrix_is_closed_and_explicit() {
    assert_eq!(
        fallback_tool_for_language(LspLanguageV2::Rust),
        DiagnosticsFallbackToolV2::CargoCheck
    );
    assert_eq!(
        fallback_tool_for_language(LspLanguageV2::TypeScript),
        DiagnosticsFallbackToolV2::TscNoEmit
    );
    assert_eq!(
        fallback_tool_for_language(LspLanguageV2::Python),
        DiagnosticsFallbackToolV2::Pyright
    );
}

fn policy(mode: &str) -> LspServerCommandPolicyV2 {
    let mut env = BTreeMap::from([("PALYRA_LSP_FIXTURE_MODE".to_owned(), mode.to_owned())]);
    for key in ["SYSTEMROOT", "TEMP", "TMP"] {
        if let Ok(value) = std::env::var(key) {
            env.insert(key.to_owned(), value);
        }
    }
    LspServerCommandPolicyV2 {
        language: LspLanguageV2::Rust,
        executable: PathBuf::from(env!("CARGO_BIN_EXE_palyra-lsp-fixture")),
        args: Vec::new(),
        env,
        toolchain_fingerprint: format!("rust-fixture-{mode}"),
        network_allowed: false,
    }
}

fn resource_limit() -> ResourceUnitsV1 {
    ResourceUnitsV1 {
        processes: 32,
        memory_bytes: 8 * 1024 * 1024 * 1024,
        file_descriptors: 4096,
        sockets: 256,
        spool_bytes: 256 * 1024 * 1024,
        concurrency: 128,
    }
}

fn change(path: &str, text: &str) -> LspDocumentChangeV2 {
    LspDocumentChangeV2 { relative_path: PathBuf::from(path), text: text.to_owned() }
}
