use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use palyra_daemon::application::local_resource_governor::{
    LocalResourceGovernor, LocalResourceGovernorConfig, ResourceServiceKind, ResourceUnitsV1,
};
use palyra_daemon::application::lsp_workspace_supervisor::{
    LspLanguageV2, LspServerCommandPolicyV2, LspServerLifecycleV2, LspWorkspaceOpenRequestV2,
    LspWorkspaceSupervisor, LspWorkspaceSupervisorConfig, LspWorkspaceSupervisorError,
};
use palyra_daemon::application::process_supervisor::{ProcessSupervisor, ProcessSupervisorConfig};
use serde_json::{json, Value};

struct Fixture {
    lsp: LspWorkspaceSupervisor,
    process: Arc<ProcessSupervisor>,
    governor: LocalResourceGovernor,
    root: tempfile::TempDir,
}

impl Fixture {
    fn new(modes: &[(LspLanguageV2, &str)]) -> Self {
        let root = tempfile::tempdir().expect("temp root");
        let governor = LocalResourceGovernor::open(LocalResourceGovernorConfig {
            registry_path: root.path().join("state").join("resource-leases.json"),
            global_limit: resource_limit(),
            per_owner_limit: resource_limit(),
            max_records: 512,
        })
        .expect("open governor");
        let process = Arc::new(
            ProcessSupervisor::start(ProcessSupervisorConfig {
                state_root: root.path().join("state"),
                max_sessions: 16,
                max_retained_chunks_per_session: 512,
                max_retained_bytes_per_session: 4 * 1024 * 1024,
                max_artifact_bytes_per_session: 8 * 1024 * 1024,
                drain_timeout: Duration::from_secs(2),
                resource_governor: governor.clone(),
            })
            .expect("start process supervisor"),
        );
        let policies = modes.iter().map(|(language, mode)| policy(*language, mode)).collect();
        let lsp =
            LspWorkspaceSupervisor::open(lsp_config(root.path(), policies), Arc::clone(&process))
                .expect("open LSP supervisor");
        Self { lsp, process, governor, root }
    }

    fn workspace(&self, name: &str) -> PathBuf {
        let path = self.root.path().join(name);
        fs::create_dir_all(path.as_path()).expect("create workspace");
        path
    }
}

fn policy(language: LspLanguageV2, mode: &str) -> LspServerCommandPolicyV2 {
    let mut env = BTreeMap::from([("PALYRA_LSP_FIXTURE_MODE".to_owned(), mode.to_owned())]);
    for key in ["SYSTEMROOT", "TEMP", "TMP"] {
        if let Ok(value) = std::env::var(key) {
            env.insert(key.to_owned(), value);
        }
    }
    LspServerCommandPolicyV2 {
        language,
        executable: PathBuf::from(env!("CARGO_BIN_EXE_palyra-lsp-fixture")),
        args: Vec::new(),
        env,
        toolchain_fingerprint: format!("{language:?}-fixture-v1"),
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

fn lsp_config(
    root: &Path,
    policies: Vec<LspServerCommandPolicyV2>,
) -> LspWorkspaceSupervisorConfig {
    LspWorkspaceSupervisorConfig {
        registry_path: root.join("state").join("lsp-registry.json"),
        max_servers: 8,
        max_registry_entries: 64,
        max_header_bytes: 8 * 1024,
        max_message_bytes: 256 * 1024,
        max_notifications: 128,
        initialize_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_millis(500),
        server_lifetime: Duration::from_secs(30),
        idle_ttl: Duration::from_millis(50),
        broken_ttl: Duration::from_millis(50),
        circuit_breaker_failures: 2,
        network_isolation_verified: true,
        resource_units: ResourceUnitsV1 {
            processes: 1,
            memory_bytes: 256 * 1024 * 1024,
            file_descriptors: 32,
            sockets: 0,
            spool_bytes: 8 * 1024 * 1024,
            concurrency: 1,
        },
        policies,
    }
}

fn open_request(
    workspace_root: PathBuf,
    identity: &str,
    language: LspLanguageV2,
) -> LspWorkspaceOpenRequestV2 {
    LspWorkspaceOpenRequestV2 {
        workspace_root,
        worktree_id: identity.to_owned(),
        run_id: format!("run-{identity}"),
        language,
    }
}

#[test]
fn rust_typescript_and_python_servers_are_real_isolated_services() {
    let fixture = Fixture::new(&[
        (LspLanguageV2::Rust, "normal"),
        (LspLanguageV2::TypeScript, "normal"),
        (LspLanguageV2::Python, "normal"),
    ]);
    let cases = [
        (LspLanguageV2::Rust, "rust"),
        (LspLanguageV2::TypeScript, "typescript"),
        (LspLanguageV2::Python, "python"),
    ];
    let mut handles = Vec::new();
    for (language, identity) in cases {
        let workspace = fixture.workspace(identity);
        let handle = fixture
            .lsp
            .ensure(open_request(workspace.clone(), identity, language))
            .expect("initialize language server");
        assert_eq!(handle.lifecycle, LspServerLifecycleV2::Ready);
        let reused = fixture
            .lsp
            .ensure(open_request(workspace, identity, language))
            .expect("reuse language server");
        assert_eq!(reused.handle_id, handle.handle_id);
        let outcome = fixture
            .lsp
            .request(handle.handle_id.as_str(), "fixture/echo", json!({"language": identity}))
            .expect("echo request");
        assert_eq!(outcome.result, json!({"language": identity}));
        handles.push(handle);
    }
    assert_eq!(fixture.lsp.health().expect("health").active_servers, 3);
    assert_eq!(
        fixture
            .governor
            .active_leases()
            .expect("active leases")
            .iter()
            .filter(|lease| lease.service == ResourceServiceKind::Lsp)
            .count(),
        3
    );
    assert_ne!(handles[0].workspace_root_sha256, handles[1].workspace_root_sha256);
    assert_ne!(handles[1].workspace_root_sha256, handles[2].workspace_root_sha256);
}

#[test]
fn operator_health_omits_server_payloads_and_raw_identities() {
    let fixture = Fixture::new(&[(LspLanguageV2::Rust, "sensitive_capabilities")]);
    let worktree_id = "sensitive-health-worktree";
    let handle = fixture
        .lsp
        .ensure(open_request(
            fixture.workspace("sensitive-health"),
            worktree_id,
            LspLanguageV2::Rust,
        ))
        .expect("initialize fixture server");
    let health = fixture.lsp.diagnostics_health().expect("operator health");
    let serialized = serde_json::to_string(&health).expect("serialize operator health");
    assert_eq!(health.schema_version, 2);
    assert_eq!(health.handles.len(), 1);
    assert!(health.handles[0].capabilities_present);
    assert!(!serialized.contains("fixture-secret-capability"));
    assert!(!serialized.contains("C:\\\\private\\\\workspace"));
    assert!(!serialized.contains(handle.handle_id.as_str()));
    assert!(!serialized.contains(handle.process_session_id.as_str()));
    assert!(!serialized.contains(worktree_id));
    assert!(!serialized.contains("capabilities\":{"));
}

#[test]
fn initialize_timeout_malformed_frame_and_oversize_open_broken_cache() {
    for (mode, expected) in
        [("initialize_timeout", "timeout"), ("malformed", "malformed"), ("oversize", "oversize")]
    {
        let fixture = Fixture::new(&[(LspLanguageV2::Rust, mode)]);
        let error = fixture
            .lsp
            .ensure(open_request(fixture.workspace(mode), mode, LspLanguageV2::Rust))
            .expect_err("reject fixture failure");
        match expected {
            "timeout" => assert!(matches!(error, LspWorkspaceSupervisorError::RequestTimeout)),
            "malformed" => assert!(matches!(error, LspWorkspaceSupervisorError::MalformedFrame)),
            "oversize" => assert!(matches!(error, LspWorkspaceSupervisorError::OversizedFrame)),
            _ => unreachable!(),
        }
        let health = fixture.lsp.health().expect("health");
        assert_eq!(health.active_servers, 0);
        assert_eq!(health.broken_servers.len(), 1);
    }
}

#[test]
fn crashed_server_restarts_only_after_explicit_circuit_reset() {
    let fixture = Fixture::new(&[(LspLanguageV2::Rust, "normal")]);
    let workspace = fixture.workspace("restart");
    let first = fixture
        .lsp
        .ensure(open_request(workspace.clone(), "restart", LspLanguageV2::Rust))
        .expect("initialize first generation");
    let error = fixture
        .lsp
        .request(first.handle_id.as_str(), "fixture/crash", Value::Null)
        .expect_err("observe crash");
    assert!(matches!(error, LspWorkspaceSupervisorError::ServerCrashed));
    let circuit = fixture
        .lsp
        .ensure(open_request(workspace.clone(), "restart", LspLanguageV2::Rust))
        .expect_err("broken cache blocks restart");
    assert!(matches!(circuit, LspWorkspaceSupervisorError::CircuitOpen(_)));
    fixture.lsp.reset_broken(workspace.as_path(), LspLanguageV2::Rust).expect("reset circuit");
    let second = fixture
        .lsp
        .ensure(open_request(workspace, "restart", LspLanguageV2::Rust))
        .expect("restart server");
    assert_eq!(second.generation, first.generation + 1);
    assert_eq!(second.restart_count, 1);
}

#[test]
fn idle_reap_terminates_process_and_releases_lsp_lease() {
    let fixture = Fixture::new(&[(LspLanguageV2::Rust, "normal")]);
    let handle = fixture
        .lsp
        .ensure(open_request(fixture.workspace("idle"), "idle", LspLanguageV2::Rust))
        .expect("initialize server");
    std::thread::sleep(Duration::from_millis(80));
    let reaped = fixture.lsp.reap_idle().expect("reap idle");
    assert_eq!(reaped.len(), 1);
    assert_eq!(reaped[0].lifecycle, LspServerLifecycleV2::Stopped);
    let status =
        fixture.process.status(handle.process_session_id.as_str()).expect("process status");
    assert!(status.state.is_terminal());
    assert_eq!(fixture.lsp.health().expect("health").active_servers, 0);
    assert!(fixture
        .governor
        .active_leases()
        .expect("active leases")
        .iter()
        .all(|lease| lease.service != ResourceServiceKind::Lsp));
}

#[test]
fn denied_network_policy_requires_verified_host_isolation() {
    let root = tempfile::tempdir().expect("temp root");
    let governor = LocalResourceGovernor::open(LocalResourceGovernorConfig {
        registry_path: root.path().join("leases.json"),
        global_limit: resource_limit(),
        per_owner_limit: resource_limit(),
        max_records: 32,
    })
    .expect("governor");
    let process = Arc::new(
        ProcessSupervisor::start(ProcessSupervisorConfig {
            state_root: root.path().join("process"),
            max_sessions: 2,
            max_retained_chunks_per_session: 64,
            max_retained_bytes_per_session: 64 * 1024,
            max_artifact_bytes_per_session: 256 * 1024,
            drain_timeout: Duration::from_secs(1),
            resource_governor: governor,
        })
        .expect("process supervisor"),
    );
    let mut config = lsp_config(root.path(), vec![policy(LspLanguageV2::Rust, "normal")]);
    config.network_isolation_verified = false;
    let lsp = LspWorkspaceSupervisor::open(config, process).expect("open LSP supervisor");
    let workspace = root.path().join("workspace");
    fs::create_dir_all(workspace.as_path()).expect("workspace");
    assert!(matches!(
        lsp.ensure(open_request(workspace, "network", LspLanguageV2::Rust)),
        Err(LspWorkspaceSupervisorError::NetworkIsolationUnavailable)
    ));
}
