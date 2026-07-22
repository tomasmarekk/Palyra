use super::*;
use std::{ffi::OsStr, process::ExitStatus};

const NO_TOOLS_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/real_runtime/text_exact.yaml");
const READ_ONLY_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/real_runtime/read_only_tool.yaml");
const APPROVAL_DENIED_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/real_runtime/mutation_approval_denied.yaml");
const FAULT_MUTATION_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/fault_injection/tool_effect_before_ack.yaml");
const PROCESS_FAULT_MUTATION_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/fault_injection/process_effect_before_ack.yaml");
const FAULT_DELIVERY_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/fault_injection/delivery_effect_before_ack.yaml");
const PROVIDER_RECOVERY_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/real_runtime/malformed_stream_recovery.yaml");
const RUNTIME_KERNEL_SHADOW_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/runtime_kernel_v2/shadow_differential.yaml");
const RUNTIME_KERNEL_V2_TEXT_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/runtime_kernel_v2/authoritative_text.yaml");
const RUNTIME_KERNEL_V2_TOOL_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/runtime_kernel_v2/authoritative_tool.yaml");
const RUNTIME_KERNEL_V2_APPROVAL_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/runtime_kernel_v2/authoritative_approval.yaml");
const RUNTIME_KERNEL_V2_CANCEL_SCENARIO: &str = include_str!(
    "../../../../../../qa/scenarios/runtime_kernel_v2/authoritative_cancellation.yaml"
);
const RUNTIME_KERNEL_V2_COMPACTION_SCENARIO: &str =
    include_str!("../../../../../../qa/scenarios/runtime_kernel_v2/authoritative_compaction.yaml");

fn parse_scenario(source: &str) -> QaScenarioManifest {
    palyra_common::qa_scenarios::parse_qa_scenario_manifest_yaml(source)
        .expect("real runtime scenario should remain valid")
}

fn parse_scenario_with_policy_profile(source: &str, profile: &str) -> QaScenarioManifest {
    let mut replaced = false;
    let source = source
        .lines()
        .map(|line| {
            if line.trim_start().starts_with("policy_profile:") {
                replaced = true;
                let indentation = &line[..line.len() - line.trim_start().len()];
                format!("{indentation}policy_profile: {profile}")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(replaced, "scenario should declare a policy profile");
    parse_scenario(source.as_str())
}

#[test]
fn isolated_daemon_config_enables_only_declared_process_runtime() {
    let base_config = isolated_daemon_config(&parse_scenario(NO_TOOLS_SCENARIO));
    assert_eq!(base_config, QA_BASE_DAEMON_CONFIG);
    assert!(!base_config.contains("process_runner"));

    let process_config = isolated_daemon_config(&parse_scenario(PROCESS_FAULT_MUTATION_SCENARIO));
    assert!(process_config.contains("[tool_call.process_runner]"));
    assert!(process_config.contains("enabled = true"));
    assert!(process_config.contains(r#"tier = "b""#));
    assert!(process_config.contains(r#"path_access_mode = "workspace_only""#));
    assert!(process_config.contains(r#"allowed_executables = ["echo"]"#));
    assert!(process_config.contains("allow_interpreters = false"));
    assert!(process_config.contains(r#"egress_enforcement_mode = "none""#));
}

fn command_env<'a>(command: &'a Command, key: &str) -> Option<&'a OsStr> {
    command
        .get_envs()
        .find(|(candidate, _)| *candidate == OsStr::new(key))
        .and_then(|(_, value)| value)
}

fn long_running_test_process() -> OwnedDaemonProcess {
    #[cfg(windows)]
    let mut command = {
        let mut command = Command::new("powershell.exe");
        command.args(["-NoProfile", "-NonInteractive", "-Command", "Start-Sleep -Seconds 30"]);
        command
    };
    #[cfg(not(windows))]
    let mut command = {
        let mut command = Command::new("sleep");
        command.arg("30");
        command
    };
    let preparation = configure_daemon_process_tree(&mut command)
        .expect("cleanup child process tree should configure");
    let child = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("long-running cleanup child should start");
    match attach_daemon_process_tree(child, preparation) {
        Ok(process) => process,
        Err(failure) => panic!("cleanup child should have tree ownership: {:#}", failure.error),
    }
}

const GRANDCHILD_HELPER_TEST: &str =
    "commands::qa_runner::process::tests::process_tree_grandchild_helper";
const GRANDCHILD_HELPER_MODE_ENV: &str = "PALYRA_QA_PROCESS_TREE_GRANDCHILD_HELPER_MODE";

fn test_process_with_grandchild(pid_path: &Path) -> OwnedDaemonProcess {
    let mut command = Command::new(std::env::current_exe().expect("test executable path"));
    command
        .args(["--exact", GRANDCHILD_HELPER_TEST, "--nocapture"])
        .env(GRANDCHILD_HELPER_MODE_ENV, "launcher")
        .env("PALYRA_QA_TEST_PID_PATH", pid_path);
    let preparation = configure_daemon_process_tree(&mut command)
        .expect("grandchild process tree should configure");
    let child = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("grandchild fixture parent should start");
    match attach_daemon_process_tree(child, preparation) {
        Ok(process) => process,
        Err(failure) => {
            panic!("grandchild fixture should have tree ownership: {:#}", failure.error)
        }
    }
}

#[test]
fn process_tree_grandchild_helper() {
    let Ok(mode) = std::env::var(GRANDCHILD_HELPER_MODE_ENV) else {
        return;
    };
    if mode == "sleep" {
        thread::sleep(Duration::from_secs(30));
        return;
    }
    assert_eq!(mode, "launcher");
    let pid_path = PathBuf::from(
        std::env::var_os("PALYRA_QA_TEST_PID_PATH")
            .expect("grandchild helper pid path should be configured"),
    );
    let child = Command::new(std::env::current_exe().expect("test executable path"))
        .args(["--exact", GRANDCHILD_HELPER_TEST, "--nocapture"])
        .env(GRANDCHILD_HELPER_MODE_ENV, "sleep")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("grandchild helper should start");
    fs::write(pid_path, child.id().to_string()).expect("grandchild pid should be recorded");
    let _child = child;
    thread::sleep(Duration::from_secs(30));
}

fn wait_for_recorded_process_id(path: &Path, timeout: Duration) -> u32 {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(text) = fs::read_to_string(path) {
            if let Ok(process_id) = text.trim().parse() {
                return process_id;
            }
        }
        assert!(Instant::now() < deadline, "grandchild pid should be recorded");
        thread::sleep(SHUTDOWN_POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn test_process_with_escaped_grandchild(pid_path: &Path, detached: bool) -> OwnedDaemonProcess {
    test_process_with_escape_mode(pid_path, if detached { "detached" } else { "launcher" })
}

#[cfg(all(unix, not(target_os = "macos")))]
const PROCESS_TREE_LIVENESS_FD_ENV: &str = "PALYRA_QA_TEST_PROCESS_TREE_LIVENESS_FD";

#[cfg(unix)]
fn test_process_with_escape_mode(pid_path: &Path, mode: &str) -> OwnedDaemonProcess {
    #[cfg(not(target_os = "macos"))]
    use std::os::fd::AsRawFd;

    const HELPER_TEST: &str = "commands::qa_runner::process::tests::unix_process_tree_helper";

    let mut command = Command::new(std::env::current_exe().expect("test executable path"));
    command.args(["--exact", HELPER_TEST, "--nocapture"]);
    command
        .env("PALYRA_QA_TEST_PID_PATH", pid_path)
        .env("PALYRA_QA_PROCESS_TREE_HELPER_MODE", mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let preparation = configure_daemon_process_tree(&mut command)
        .expect("escaped-grandchild process tree should configure");
    #[cfg(not(target_os = "macos"))]
    command.env(
        PROCESS_TREE_LIVENESS_FD_ENV,
        preparation.descendant_liveness_write.as_raw_fd().to_string(),
    );
    let child = command.spawn().expect("escaped-grandchild fixture parent should start");
    match attach_daemon_process_tree(child, preparation) {
        Ok(process) => process,
        Err(failure) => {
            panic!("escaped-grandchild fixture should have tree ownership: {:#}", failure.error)
        }
    }
}

#[cfg(unix)]
#[test]
fn unix_process_tree_helper() {
    use std::os::unix::process::CommandExt;

    const HELPER_TEST: &str = "commands::qa_runner::process::tests::unix_process_tree_helper";
    let Ok(mode) = std::env::var("PALYRA_QA_PROCESS_TREE_HELPER_MODE") else {
        return;
    };
    let pid_path = PathBuf::from(
        std::env::var_os("PALYRA_QA_TEST_PID_PATH")
            .expect("process-tree helper pid path should be configured"),
    );
    if mode == "sleep" {
        fs::write(pid_path.as_path(), std::process::id().to_string())
            .expect("escaped helper pid should be recorded");
        thread::sleep(Duration::from_secs(30));
        return;
    }
    #[cfg(target_os = "macos")]
    if mode == "sleep_close_fds" {
        close_test_process_non_stdio_descriptors();
        fs::write(pid_path.as_path(), std::process::id().to_string())
            .expect("escaped helper pid should be recorded");
        thread::sleep(Duration::from_secs(30));
        return;
    }
    assert!(matches!(
        mode.as_str(),
        "launcher" | "detached" | "detached_close_fds" | "intermediate_close_fds"
    ));
    #[cfg(target_os = "macos")]
    let child_mode = if mode == "intermediate_close_fds" { "sleep_close_fds" } else { "sleep" };
    #[cfg(not(target_os = "macos"))]
    let child_mode = "sleep";
    let mut child = Command::new(std::env::current_exe().expect("test executable path"));
    child
        .args(["--exact", HELPER_TEST, "--nocapture"])
        .env(
            "PALYRA_QA_PROCESS_TREE_HELPER_MODE",
            if mode == "detached_close_fds" { "intermediate_close_fds" } else { child_mode },
        )
        .env("PALYRA_QA_TEST_PID_PATH", pid_path.as_os_str())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(not(target_os = "macos"))]
    if mode == "intermediate_close_fds" {
        let liveness_file_descriptor = std::env::var(PROCESS_TREE_LIVENESS_FD_ENV)
            .expect("process-tree liveness descriptor should be configured")
            .parse::<i32>()
            .expect("process-tree liveness descriptor should be an integer");
        // SAFETY: the descriptor came from the live inherited pipe before spawn; close(2) is
        // async-signal-safe, and the closure performs no allocation or locking.
        unsafe {
            child.pre_exec(move || {
                if unix_close(liveness_file_descriptor) == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }
    if mode != "intermediate_close_fds" {
        // SAFETY: setsid(2) is async-signal-safe and the closure performs no allocation or locks.
        unsafe {
            child.pre_exec(|| {
                if unix_setsid() == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }
    #[expect(
        clippy::zombie_processes,
        reason = "the fixture intentionally orphans this child for process-tree cleanup discovery"
    )]
    let _child = child.spawn().expect("escaped helper child should start");
    let _ = wait_for_recorded_process_id(pid_path.as_path(), Duration::from_secs(5));
    if mode == "launcher" {
        thread::sleep(Duration::from_secs(30));
    }
}

#[cfg(target_os = "macos")]
fn close_test_process_non_stdio_descriptors() {
    let descriptors = fs::read_dir("/dev/fd")
        .expect("test process descriptor directory should be readable")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().to_string_lossy().parse::<i32>().ok())
        .filter(|descriptor| *descriptor > 2)
        .collect::<Vec<_>>();
    for descriptor in descriptors {
        // SAFETY: close(2) accepts every integer descriptor; this isolated helper deliberately
        // releases all inherited non-stdio resources before it records readiness.
        unsafe {
            libc::close(descriptor);
        }
    }
}

fn shared_test_state_root() -> (SharedStateRoot, PathBuf) {
    let root = tempfile::tempdir().expect("state root should exist");
    let path = root.path().to_path_buf();
    let pin = pin_state_root(path.as_path()).expect("state root should be pinned");
    (
        Arc::new(Mutex::new(StateRootOwnership {
            root: Some(root),
            pin: Some(pin),
            path_substituted: false,
            startup_cleanup_delegated: false,
        })),
        path,
    )
}

fn test_sandbox() -> (QaDaemonSandbox, PathBuf) {
    let state_root = tempfile::tempdir().expect("state root should exist");
    let root_path = state_root.path().to_path_buf();
    let workspace = root_path.join("workspace");
    fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
    let state_root_pin = pin_state_root(root_path.as_path()).expect("state root should be pinned");
    let state_root = Arc::new(Mutex::new(StateRootOwnership {
        root: Some(state_root),
        pin: Some(state_root_pin),
        path_substituted: false,
        startup_cleanup_delegated: false,
    }));
    (
        QaDaemonSandbox {
            launch: QaDaemonLaunchContext {
                binary: PathBuf::from("palyrad"),
                workspace,
                state_root: root_path.clone(),
                identity_root: root_path.join("identity"),
                config_path: root_path.join("palyra.toml"),
                vault_dir: root_path.join("vault"),
                provider: QaDaemonProviderEnvironment::Deterministic {
                    provider_fixture: root_path.join("provider.yaml"),
                },
                execution_key_digest: "a".repeat(64),
                provider_binding_sha256: "b".repeat(64),
                admin_token: "test-only-token".to_owned(),
                principal: "user:test".to_owned(),
                allowed_tools: String::new(),
                policy_profile: "qa_no_tools".to_owned(),
                expected_runtime_contract_version: PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION
                    .to_owned(),
                expected_git_hash: "test".to_owned(),
                fault: None,
            },
            cleanup_admission: None,
            child: Some(long_running_test_process()),
            state_root,
            admin_port: 1,
            grpc_port: 2,
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            active_session_id: None,
            active_run_id: None,
            log_threads: Vec::new(),
            log_drain_join_failed: false,
            log_tail: Arc::new(Mutex::new(VecDeque::new())),
            runtime_health: QaDaemonRuntimeHealth {
                service: "palyrad".to_owned(),
                status: "ok".to_owned(),
                version: "0.1.0".to_owned(),
                git_hash: "test".to_owned(),
                build_profile: "debug".to_owned(),
                _uptime_seconds: 0,
                public_runtime_contract_version: PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION
                    .to_owned(),
                qa_scenario_schema_version: QA_SCENARIO_SCHEMA_VERSION,
                qa_mock_provider_fixture_schema_version: QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION,
            },
            secret_sentinels: Vec::new(),
            fault_launch_documents: Vec::new(),
            daemon_restarts: 0,
        },
        root_path,
    )
}

fn create_test_failure_journal(state_root: &Path) -> Connection {
    let data_dir = state_root.join("data");
    fs::create_dir_all(data_dir.as_path()).expect("journal directory should exist");
    let connection =
        Connection::open(data_dir.join("journal.sqlite3")).expect("diagnostic journal should open");
    connection
        .execute_batch(
            r#"
            CREATE TABLE orchestrator_runs (
                run_ulid TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                cancel_requested INTEGER NOT NULL,
                last_error TEXT
            );
            CREATE TABLE orchestrator_tape (
                run_ulid TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE journal_events (
                seq INTEGER NOT NULL,
                run_ulid TEXT NOT NULL,
                kind INTEGER NOT NULL,
                actor INTEGER NOT NULL,
                redacted INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            );
            "#,
        )
        .expect("diagnostic journal schema should be created");
    connection
}

fn wait_for_process_exit(process_id: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !process_is_alive(process_id, Duration::from_millis(500).min(remaining)) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(SHUTDOWN_POLL_INTERVAL);
    }
}

#[cfg(windows)]
fn process_is_alive(process_id: u32, timeout: Duration) -> bool {
    let filter = format!("PID eq {process_id}");
    let mut probe = match Command::new("tasklist.exe")
        .args(["/FI", filter.as_str(), "/FO", "CSV", "/NH"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(probe) => probe,
        Err(_) => return true,
    };
    let Some(status) = wait_for_probe(&mut probe, timeout) else {
        return true;
    };
    if !status.success() {
        return true;
    }
    let mut output = String::new();
    let Some(mut stdout) = probe.stdout.take() else {
        return true;
    };
    if stdout.read_to_string(&mut output).is_err() {
        return true;
    }
    let expected = process_id.to_string();
    output.lines().any(|line| {
        line.split(',').nth(1).is_some_and(|field| field.trim().trim_matches('"') == expected)
    })
}

#[cfg(not(windows))]
fn process_is_alive(process_id: u32, timeout: Duration) -> bool {
    let process_id = process_id.to_string();
    let mut probe = match Command::new("kill")
        .args(["-0", process_id.as_str()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(probe) => probe,
        Err(_) => return true,
    };
    wait_for_probe(&mut probe, timeout).is_none_or(|status| status.success())
}

fn wait_for_probe(probe: &mut Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        match probe.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {}
            Err(_) => return None,
        }
        let now = Instant::now();
        if now >= deadline {
            let _ = terminate_child_with_timeout(probe, Duration::from_secs(1));
            return None;
        }
        thread::sleep(SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
    }
}

#[path = "tests_runtime.rs"]
mod runtime;

#[path = "tests_diagnostics.rs"]
mod diagnostics;

#[path = "tests_fixture.rs"]
mod fixture;

#[path = "tests_cleanup.rs"]
mod cleanup;
