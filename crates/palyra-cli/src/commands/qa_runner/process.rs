//! Isolated daemon process and workspace lifecycle for the fixture runner.

#[cfg(unix)]
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    collections::{BTreeMap, VecDeque},
    env,
    fmt::Write as _,
    fs,
    future::Future,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, ChildStderr, ChildStdout, Command, Stdio},
    sync::{mpsc, Arc, Mutex, MutexGuard, OnceLock},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use palyra_auth::{AuthCredential, AuthProfileRecord, AuthProfileScope};
#[cfg(test)]
use palyra_common::qa_fault_injection::QaFaultEvidenceSidecarRecord;
#[cfg(test)]
use palyra_common::qa_scenarios::QaScenarioStep;
use palyra_common::{
    qa_fault_injection::{
        parse_qa_fault_evidence_sidecar_ndjson, QaFaultEvidenceSidecar, QaFaultInjectionPlan,
        QaFaultLaunchDocument, QA_FAULT_CAPABILITY_PATH_ENV, QA_FAULT_CAPABILITY_PREFIX,
        QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES, QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_LAUNCH_SCHEMA_VERSION,
    },
    qa_scenarios::{
        QaScenarioApprovalDecision, QaScenarioLiveProviderKind, QaScenarioManifest,
        QaScenarioStepAction, QA_SCENARIO_SCHEMA_VERSION,
    },
    redaction::{is_sensitive_key, redact_diagnostic_text},
    runtime_contracts::PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
};
use palyra_model_providers::QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION;
use palyra_vault::{
    ensure_owner_only_dir, ensure_owner_only_file, BackendPreference, Vault, VaultConfig, VaultRef,
    VaultScope,
};
use ring::rand::{SecureRandom, SystemRandom};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use ulid::Ulid;

use crate::{
    client::operator::OperatorRuntime,
    proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    AgentConnection, SessionCleanupInput,
};

use super::{
    digest_materialized_fixture_set, digest_to_hex, resolve_runner_path, sha256_file,
    validate_redacted_replay_fixture, QaPreparedLiveBinding, QaPreparedRunnerBinding,
    QaPreparedScenarioExecution,
};

mod diagnostics_state;
mod fixture_fault_provider;
mod journal_projection;
mod journal_snapshot;
mod process_tree;
#[cfg(windows)]
mod process_tree_windows;
mod startup;
mod workspace_diagnostics;

use diagnostics_state::{
    metadata_is_indirection, open_directory_no_follow, pin_state_root, pinned_directory_removed,
    project_failure_fault_record, unavailable_failure_journal, unavailable_failure_workspace,
};
use fixture_fault_provider::{
    ensure_fault_context_within_state_root, load_fault_evidence_sidecar,
    materialize_fixture_snapshot, prepare_fault_context, prepare_fault_launch,
    prepare_provider_environment, verify_bound_fault_launch_handshake_with_hook,
    QaDaemonProviderEnvironment, QaLiveTransportEnvironment, QaPreparedProviderEnvironment,
    SecretBytes,
};
use journal_projection::load_failure_run_projection_from_snapshot;
use journal_snapshot::{
    digest_validated_journal_file, load_failure_run_projection, sqlite_read_only_uri,
    validate_existing_path_components,
};
use process_tree::{
    attach_daemon_process_tree, configure_daemon_process_tree, QaDaemonEnvironment,
};
#[cfg(windows)]
use process_tree_windows::{
    resume_suspended_windows_process, windows_assign_process_to_job_object, WindowsJobHandle,
    WINDOWS_CREATE_SUSPENDED,
};
use startup::{
    acquire_startup_cleanup_admission, bounded_log_summary, cleanup_session_with_timeout,
    join_owned_log_threads_bounded, register_startup_cleanup, start_daemon,
    validate_policy_profile, StartupCleanupAdmission,
};
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
use workspace_diagnostics::QA_OS_NO_FOLLOW;
use workspace_diagnostics::{
    contains_absolute_path_marker, load_failure_workspace_projection,
    open_failure_workspace_file_no_follow, open_file_identity, open_file_link_count,
    same_open_file_identity, OpenFileIdentity,
};

#[cfg(test)]
use fixture_fault_provider::{
    copy_live_secret, open_isolated_live_vault, write_owner_only_new_file,
};
#[cfg(test)]
use journal_projection::{collect_failure_payload_fields, project_failure_payload};
#[cfg(test)]
use journal_snapshot::{
    load_failure_run_projection_with_hook, materialize_failure_journal_snapshot,
    materialize_failure_journal_snapshot_with_hook, validate_failure_journal_files,
};
#[cfg(all(test, windows))]
use process_tree::attach_windows_daemon_process_tree_with;
#[cfg(all(test, target_os = "macos"))]
use process_tree::mac_process_baseline_with;
#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
use process_tree::parse_linux_process_stat;
#[cfg(all(test, unix))]
use process_tree::wait_for_child_exit;
#[cfg(all(test, unix))]
use process_tree::{
    acquire_unix_process_tree_marker_scan, unix_close, unix_descendant_liveness_closed,
    unix_identity_matching_roots, unix_other_tree_processes_with_registry,
    unix_preexisting_process_groups, unix_process_disappeared, unix_process_identity_is_active,
    unix_process_requires_marker_scan, unix_process_table, unix_recursive_descendants, unix_setsid,
    unix_signal_process_identity_with, UNIX_ESRCH, UNIX_SIGKILL,
};
#[cfg(test)]
use startup::{
    acquire_startup_cleanup_admission_with, configure_isolated_environment,
    configure_live_transport_environment, drive_startup_cleanup_reaper_inline,
    parse_health_response, parse_port_from_log, push_log_tail, read_health_response,
    register_startup_cleanup_with, terminate_child_with_timeout, validate_daemon_contract,
    wait_for_listen_ports, StartupCleanupReaperState,
};
#[cfg(test)]
use workspace_diagnostics::load_failure_workspace_projection_with_hook;

const DAEMON_START_TIMEOUT: Duration = Duration::from_secs(15);
const DAEMON_HEALTH_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
// Unix cleanup can require several full process-table and marker scans before proving inactivity.
// Keep one bounded budget with enough headroom for concurrent cleanup under system load.
const DAEMON_TERMINATION_TIMEOUT: Duration = Duration::from_secs(15);
const LOG_DRAIN_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DROP_CLEANUP_ATTEMPTS: usize = 3;
const STARTUP_REAPER_MAX_ATTEMPTS: usize = 3;
const STARTUP_REAPER_BACKOFF: Duration = Duration::from_millis(50);
const MAX_WORKSPACE_ENTRIES: usize = 1_024;
const MAX_WORKSPACE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_WORKSPACE_DEPTH: usize = 32;
const MAX_LOG_TAIL_LINES: usize = 32;
const MAX_LOG_LINE_BYTES: usize = 8 * 1024;
const MAX_LOG_LINE_CHARS: usize = 2_048;
const MAX_HEALTH_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_FAILURE_TAPE_EVENTS: usize = 64;
const MAX_FAILURE_JOURNAL_EVENTS: usize = 64;
const MAX_FAILURE_PAYLOAD_BYTES: usize = 16 * 1024;
const MAX_FAILURE_PAYLOAD_FIELDS: usize = 32;
const MAX_FAILURE_PAYLOAD_DEPTH: usize = 6;
const MAX_FAILURE_TEXT_CHARS: usize = 2_048;
const MAX_FAILURE_FAULT_RECORDS: usize = 64;
const MAX_FAILURE_WORKSPACE_ARTIFACTS: usize = 256;
const MAX_FAILURE_WORKSPACE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_FAILURE_JOURNAL_DB_BYTES: u64 = 256 * 1024 * 1024;
const MAX_FAILURE_JOURNAL_WAL_BYTES: u64 = 256 * 1024 * 1024;
const MAX_FAILURE_JOURNAL_SHM_BYTES: u64 = 16 * 1024 * 1024;
const MAX_FAILURE_JOURNAL_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FAILURE_SQL_TEXT_BYTES: usize = MAX_FAILURE_TEXT_CHARS;
const MAX_FAILURE_PAYLOAD_ARRAY_ITEMS: usize = 16;
const FAILURE_DIAGNOSTICS_SCHEMA_VERSION: u32 = 1;
const FAILURE_DIAGNOSTICS_FORMAT: &str = "palyra-qa-failure-diagnostics";
const REDACTED_ABSOLUTE_PATH: &str = "<redacted:absolute_path>";
const REDACTED_SECRET_SENTINEL: &str = "<redacted:secret_sentinel>";
const FAILURE_JOURNAL_SNAPSHOT_PREFIX: &str = ".palyra-qa-journal-snapshot-";
const QA_RUNNER_PRINCIPAL: &str = "admin:qa-runner";
const QA_LIVE_PROFILE_ALIAS: &str = "qa-live-selected";
const QA_AUTH_REGISTRY_SCHEMA_VERSION: u32 = 1;
const MIN_SECRET_SENTINEL_BYTES: usize = 8;
const QA_FAULT_DIRECTORY: &str = "qa-fault";
const QA_FAULT_PLAN_FILE: &str = "plan.json";
const QA_FAULT_EVIDENCE_FILE: &str = "evidence.ndjson";
const QA_FAULT_CAPABILITY_BYTES: usize = 32;
const QA_FAULT_LAUNCH_LIFETIME_MS: i64 = 60_000;
#[cfg(unix)]
const QA_PROCESS_TREE_MARKER_ENV: &str = "PALYRA_QA_PROCESS_TREE_MARKER";
const QA_READ_ONLY_TOOLS: &[&str] =
    &["palyra.fs.read_file", "palyra.fs.list_dir", "palyra.fs.search"];
const QA_APPROVAL_MUTATION_TOOLS: &[&str] = &["palyra.fs.apply_patch"];
const QA_FAULT_MUTATION_TOOLS: &[&str] =
    &["palyra.fs.apply_patch", "palyra.process.run", "palyra.http.fetch", "sessions_spawn"];
const QA_FAULT_DELIVERY_TOOLS: &[&str] = &["palyra.clarify.ask"];
const QA_BASE_DAEMON_CONFIG: &str = "version = 1\n";
const QA_PROCESS_DAEMON_CONFIG: &str = r#"version = 1

[tool_call.process_runner]
enabled = true
tier = "b"
workspace_root = "."
path_access_mode = "workspace_only"
allowed_executables = ["echo"]
allow_interpreters = false
egress_enforcement_mode = "none"
"#;

#[derive(Debug, Clone, Deserialize)]
pub(super) struct QaDaemonRuntimeHealth {
    pub(super) service: String,
    pub(super) status: String,
    pub(super) version: String,
    pub(super) git_hash: String,
    pub(super) build_profile: String,
    #[serde(rename = "uptime_seconds")]
    _uptime_seconds: u64,
    pub(super) public_runtime_contract_version: String,
    pub(super) qa_scenario_schema_version: u32,
    pub(super) qa_mock_provider_fixture_schema_version: u32,
}

/// Verified process and filesystem teardown state.
pub(super) struct QaDaemonShutdown {
    pub(super) daemon_terminated: bool,
    pub(super) workspace_removed: bool,
}

/// Owns every resource that must disappear after one scenario execution.
// INTENTIONAL: no `Debug`; the child environment contains its admin token.
pub(super) struct QaDaemonSandbox {
    launch: QaDaemonLaunchContext,
    cleanup_admission: Option<StartupCleanupAdmission>,
    child: Option<OwnedDaemonProcess>,
    state_root: SharedStateRoot,
    admin_port: u16,
    grpc_port: u16,
    device_id: String,
    active_session_id: Option<String>,
    active_run_id: Option<String>,
    log_threads: Vec<JoinHandle<()>>,
    log_drain_join_failed: bool,
    log_tail: Arc<Mutex<VecDeque<String>>>,
    runtime_health: QaDaemonRuntimeHealth,
    secret_sentinels: Vec<SecretBytes>,
    fault_launch_documents: Vec<QaFaultLaunchDocument>,
    daemon_restarts: u32,
}

// INTENTIONAL: no `Debug`; the launch environment contains its admin token.
struct QaDaemonLaunchContext {
    binary: PathBuf,
    workspace: PathBuf,
    state_root: PathBuf,
    identity_root: PathBuf,
    config_path: PathBuf,
    vault_dir: PathBuf,
    provider: QaDaemonProviderEnvironment,
    execution_key_digest: String,
    provider_binding_sha256: String,
    admin_token: String,
    principal: String,
    allowed_tools: String,
    expected_runtime_contract_version: String,
    expected_git_hash: String,
    fault: Option<QaRunnerFaultContext>,
}

struct QaRunnerFaultContext {
    directory: PathBuf,
    plan: QaFaultInjectionPlan,
    plan_path: PathBuf,
    plan_sha256: String,
    evidence_path: PathBuf,
}

struct QaPreparedFaultLaunch {
    document: QaFaultLaunchDocument,
    launch_relative_path: PathBuf,
    capability_relative_path: PathBuf,
    capability_path: PathBuf,
    capability_sentinel: SecretBytes,
}

#[derive(Debug)]
struct PinnedStateRoot {
    directory: fs::File,
}

struct StateRootOwnership {
    root: Option<TempDir>,
    pin: Option<PinnedStateRoot>,
    path_substituted: bool,
    startup_cleanup_delegated: bool,
}

type SharedStateRoot = Arc<Mutex<StateRootOwnership>>;

struct QaStartedDaemon {
    process: OwnedDaemonProcess,
    admin_port: u16,
    grpc_port: u16,
    log_threads: Vec<JoinHandle<()>>,
    log_tail: Arc<Mutex<VecDeque<String>>>,
    runtime_health: QaDaemonRuntimeHealth,
    fault_launch_document: Option<QaFaultLaunchDocument>,
    fault_secret_sentinel: Option<SecretBytes>,
}

struct OwnedDaemonProcess {
    child: Child,
    tree: Option<DaemonProcessTree>,
    descendants_possible_without_tree: bool,
    cleanup_verified: bool,
}

struct AttachDaemonProcessFailure {
    error: anyhow::Error,
    process: OwnedDaemonProcess,
}

struct StartupCleanupOwnership {
    process: Option<OwnedDaemonProcess>,
    log_threads: Vec<JoinHandle<()>>,
    log_join_failed: bool,
    state_root: Option<SharedStateRoot>,
}

struct DaemonProcessTree {
    #[cfg(unix)]
    root_identity: UnixProcessIdentity,
    #[cfg(unix)]
    process_group_id: i32,
    #[cfg(unix)]
    tracked_descendants: Mutex<BTreeMap<i32, UnixProcessIdentity>>,
    #[cfg(unix)]
    descendant_discovery_complete: Mutex<bool>,
    #[cfg(unix)]
    descendant_liveness_read: Mutex<fs::File>,
    #[cfg(unix)]
    containment_marker: String,
    #[cfg(unix)]
    preexisting_processes: BTreeMap<i32, UnixProcessIdentity>,
    #[cfg(windows)]
    job: WindowsJobHandle,
}

struct DaemonProcessTreePreparation {
    // The write end deliberately survives exec and is inherited by descendants. EOF is only one
    // necessary cleanup signal; the identity-bound marker scan independently covers closefrom.
    #[cfg(unix)]
    descendant_liveness_read: fs::File,
    #[cfg(unix)]
    descendant_liveness_write: fs::File,
    #[cfg(unix)]
    containment_marker: String,
    // Exact identities captured before marker injection cannot belong to this tree. Keeping the
    // start token prevents a recycled PID from inheriting the exemption.
    #[cfg(unix)]
    preexisting_processes: BTreeMap<i32, UnixProcessIdentity>,
    #[cfg(unix)]
    launch_guard: RwLockWriteGuard<'static, ()>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UnixProcessIdentity {
    process_id: i32,
    start_token_high: u64,
    start_token_low: u64,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UnixProcessSnapshot {
    identity: UnixProcessIdentity,
    parent_id: i32,
    process_group_id: i32,
    owner_id: u32,
}

#[cfg(unix)]
type UnixProcessTreeRegistry = BTreeMap<String, BTreeMap<i32, UnixProcessIdentity>>;

#[cfg(unix)]
// Marker scans share the read side, while a launch owns the write side until its root identity is
// registered. This prevents a scan from inspecting an unregistered sibling without serializing
// independent cleanup work.
static UNIX_PROCESS_TREE_COORDINATION: RwLock<()> = RwLock::new(());
#[cfg(unix)]
static UNIX_PROCESS_TREE_REGISTRY: OnceLock<Mutex<UnixProcessTreeRegistry>> = OnceLock::new();

/// Bounded, redaction-safe diagnostic snapshot captured after a failed QA observation.
#[derive(Debug, Serialize)]
pub(super) struct QaFailureDiagnostics {
    schema_version: u32,
    format: &'static str,
    failure_reason_code: String,
    runtime: QaFailureRuntimeVersions,
    daemon_terminated: bool,
    daemon_log_tail: Vec<String>,
    fault_sidecar: QaFailureFaultSidecar,
    journal: QaFailureJournalProjection,
    workspace: QaFailureWorkspaceProjection,
}

#[derive(Debug, Serialize)]
struct QaFailureRuntimeVersions {
    runner_version: String,
    runtime_version: String,
    runtime_contract_version: String,
    palyrad_version: String,
    palyrad_git_hash: String,
    palyrad_build_profile: String,
    qa_scenario_schema_version: u32,
    qa_mock_provider_fixture_schema_version: u32,
    daemon_restarts: u32,
}

#[derive(Debug, Serialize)]
struct QaFailureFaultSidecar {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_code: Option<&'static str>,
    record_count: usize,
    records_complete: bool,
    records: Vec<QaFailureFaultRecord>,
}

#[derive(Debug, Serialize)]
struct QaFailureFaultRecord {
    record_type: &'static str,
    sequence: u32,
    launch_id: String,
    plan_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    activation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    point_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    occurrence: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    action: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    activation_sequence: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    release_position: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery_class: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_code: Option<String>,
}

#[derive(Debug, Serialize)]
struct QaFailureJournalProjection {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_code: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    run: Option<QaFailureRunProjection>,
}

#[derive(Debug, Serialize)]
struct QaFailureRunProjection {
    state: String,
    cancel_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    last_error_complete: bool,
    tape_events_complete: bool,
    journal_events_complete: bool,
    tape_events: Vec<QaFailureTapeEvent>,
    journal_events: Vec<QaFailureJournalEvent>,
}

#[derive(Debug, Serialize)]
struct QaFailureTapeEvent {
    seq: i64,
    event_type: String,
    payload_complete: bool,
    fields: Map<String, Value>,
}

#[derive(Debug, Serialize)]
struct QaFailureJournalEvent {
    seq: i64,
    kind: i32,
    actor: i32,
    redacted: bool,
    payload_complete: bool,
    fields: Map<String, Value>,
}

#[derive(Debug, Serialize)]
struct QaFailureWorkspaceProjection {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_code: Option<&'static str>,
    artifacts_complete: bool,
    hashed_bytes: u64,
    artifacts: Vec<QaFailureWorkspaceArtifact>,
}

#[derive(Debug, Serialize)]
struct QaFailureWorkspaceArtifact {
    path: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Default)]
struct FailureWorkspaceBudget {
    entries_seen: usize,
    hashed_bytes: u64,
    complete: bool,
}

impl QaDaemonSandbox {
    /// Starts one isolated daemon rooted in a copied workspace and scoped provider binding.
    pub(super) fn spawn(
        manifest: &QaScenarioManifest,
        prepared: &QaPreparedScenarioExecution,
    ) -> Result<Self> {
        validate_policy_profile(manifest)?;
        if sha256_file(prepared.palyrad_binary.as_path())? != prepared.palyrad_binary_sha256 {
            anyhow::bail!("qa.runner.daemon_binary_changed");
        }
        let startup_admission = acquire_startup_cleanup_admission()?;
        let state_root = tempfile::Builder::new()
            .prefix("palyra-qa-")
            .tempdir()
            .context("qa.runner.temp_root_create_failed: failed to create isolated state root")?;
        let state_root_path = state_root.path().to_path_buf();
        let state_root_pin = match pin_state_root(state_root_path.as_path()) {
            Ok(pin) => pin,
            Err(error) => {
                state_root.close().context("qa.runner.temp_root_cleanup_failed_after_pin_error")?;
                return Err(error);
            }
        };
        let state_root = Arc::new(Mutex::new(StateRootOwnership {
            root: Some(state_root),
            pin: Some(state_root_pin),
            path_substituted: false,
            startup_cleanup_delegated: false,
        }));
        let spawn_result = (|| {
            let fixture_snapshot = materialize_fixture_snapshot(
                prepared.repository_root.as_path(),
                prepared.fixture_paths.as_slice(),
                prepared.execution_key.fixture_set_sha256.as_str(),
                state_root_path.join("input-snapshot").as_path(),
            )?;
            if let QaPreparedRunnerBinding::RecordReplay { replay_fixture } = &prepared.binding {
                validate_redacted_replay_fixture(fixture_snapshot.path(replay_fixture)?.as_path())?;
            }
            let workspace = state_root_path.join("workspace");
            fs::create_dir_all(workspace.as_path()).with_context(|| {
                format!("failed to create QA workspace {}", workspace.display())
            })?;
            match prepared.workspace_fixture.as_deref() {
                Some(source) => copy_workspace_fixture(
                    fixture_snapshot.path(source)?.as_path(),
                    workspace.as_path(),
                )?,
                None => {
                    fs::write(workspace.join("README.md"), "# Isolated Palyra QA workspace\n")
                        .context("failed to initialize empty QA workspace")?;
                }
            }

            let config_path = state_root_path.join("palyra.toml");
            fs::write(config_path.as_path(), isolated_daemon_config(manifest))
                .context("failed to write isolated QA daemon config")?;
            let vault_dir = state_root_path.join("vault");
            fs::create_dir_all(vault_dir.as_path())
                .context("failed to create isolated QA vault directory")?;
            let identity_root = state_root_path.join("identity");

            let principal = QA_RUNNER_PRINCIPAL.to_owned();
            let device_id = Ulid::new().to_string();
            let admin_token = format!("qa-{}-{}", Ulid::new(), Ulid::new());
            let provider_environment = prepare_provider_environment(
                prepared,
                &fixture_snapshot,
                state_root_path.as_path(),
                identity_root.as_path(),
                vault_dir.as_path(),
            )?;
            let QaPreparedProviderEnvironment { provider, mut live_secret_sentinels } =
                provider_environment;
            live_secret_sentinels.push(SecretBytes::new(admin_token.as_bytes().to_vec())?);
            let fault = prepare_fault_context(
                state_root_path.as_path(),
                manifest.fault_injection.as_ref(),
            )?;
            let mut launch = QaDaemonLaunchContext {
                binary: prepared.palyrad_binary.clone(),
                workspace,
                state_root: state_root_path.clone(),
                identity_root,
                config_path,
                vault_dir,
                provider,
                execution_key_digest: prepared.execution_key.digest.clone(),
                provider_binding_sha256: prepared.execution_key.provider_binding_sha256.clone(),
                admin_token,
                principal,
                allowed_tools: manifest.requires.tools.join(","),
                expected_runtime_contract_version: prepared.runtime_contract_version.clone(),
                expected_git_hash: prepared.expected_palyrad_git_hash.clone(),
                fault,
            };
            let started = start_daemon(
                &mut launch,
                DAEMON_START_TIMEOUT.saturating_add(DAEMON_HEALTH_TIMEOUT),
                Arc::clone(&state_root),
                &startup_admission,
            )?;
            let fault_launch_documents = started.fault_launch_document.iter().cloned().collect();
            if let Some(sentinel) = started.fault_secret_sentinel {
                live_secret_sentinels.push(sentinel);
            }

            Ok(Self {
                launch,
                cleanup_admission: None,
                child: Some(started.process),
                state_root: Arc::clone(&state_root),
                admin_port: started.admin_port,
                grpc_port: started.grpc_port,
                device_id,
                active_session_id: None,
                active_run_id: None,
                log_threads: started.log_threads,
                log_drain_join_failed: false,
                log_tail: started.log_tail,
                runtime_health: started.runtime_health,
                secret_sentinels: live_secret_sentinels,
                fault_launch_documents,
                daemon_restarts: 0,
            })
        })();
        match spawn_result {
            Ok(mut sandbox) => {
                sandbox.cleanup_admission = Some(startup_admission);
                Ok(sandbox)
            }
            Err(error) => {
                {
                    let mut ownership = lock_unpoisoned(&state_root);
                    if ownership.startup_cleanup_delegated {
                        return Err(error);
                    }
                    if ownership.remove_verified() {
                        return Err(error);
                    }
                    ownership.startup_cleanup_delegated = true;
                }
                let reaper_started = register_startup_cleanup(
                    &startup_admission,
                    StartupCleanupOwnership {
                        process: None,
                        log_threads: Vec::new(),
                        log_join_failed: false,
                        state_root: Some(Arc::clone(&state_root)),
                    },
                );
                Err(anyhow::anyhow!(
                    "{error:#}; qa.runner.daemon_start_state_root_cleanup_failed: cleanup_deferred=true, reaper_started={reaper_started}"
                ))
            }
        }
    }

    pub(super) fn admin_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.admin_port)
    }

    pub(super) fn grpc_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.grpc_port)
    }

    pub(super) fn admin_token(&self) -> &str {
        self.launch.admin_token.as_str()
    }

    pub(super) fn principal(&self) -> &str {
        self.launch.principal.as_str()
    }

    pub(super) fn device_id(&self) -> &str {
        self.device_id.as_str()
    }

    pub(super) fn workspace(&self) -> &Path {
        self.launch.workspace.as_path()
    }

    pub(super) fn runtime_health(&self) -> &QaDaemonRuntimeHealth {
        &self.runtime_health
    }

    fn verify_state_root_identity(&self) -> Result<()> {
        lock_unpoisoned(&self.state_root).verify_identity()
    }

    fn with_pinned_state_root_read<Output, ReadOperation>(
        &self,
        identity_error: &'static str,
        read_operation: ReadOperation,
    ) -> Result<Output>
    where
        ReadOperation: FnOnce(&Path) -> Result<Output>,
    {
        let mut ownership = lock_unpoisoned(&self.state_root);
        ownership.verify_identity().with_context(|| identity_error)?;
        let root_path = ownership
            .root
            .as_ref()
            .map(TempDir::path)
            .context("qa.runner.state_root_removed")?
            .to_path_buf();
        let output = read_operation(root_path.as_path());
        ownership.verify_path_identity().with_context(|| identity_error)?;
        output
    }

    pub(super) fn fault_evidence_sidecar(&self) -> Result<Option<QaFaultEvidenceSidecar>> {
        let Some(context) = self.launch.fault.as_ref() else {
            return Ok(None);
        };
        let launch = self
            .fault_launch_documents
            .last()
            .context("qa.runner.fault_launch_handshake_missing")?;
        self.with_pinned_state_root_read(
            "qa.runner.fault_evidence_state_root_identity_invalid",
            |state_root| {
                ensure_fault_context_within_state_root(state_root, context)?;
                load_fault_evidence_sidecar(context, launch)
            },
        )
        .map(Some)
    }

    pub(super) const fn daemon_restarts(&self) -> u32 {
        self.daemon_restarts
    }

    /// Captures a bounded failure bundle after the daemon process has been quiesced.
    pub(super) fn failure_diagnostics(
        &self,
        runner_version: &str,
        runtime_version: &str,
        failure_reason_code: &str,
        daemon_terminated: bool,
    ) -> QaFailureDiagnostics {
        let filesystem_available = daemon_terminated;
        QaFailureDiagnostics {
            schema_version: FAILURE_DIAGNOSTICS_SCHEMA_VERSION,
            format: FAILURE_DIAGNOSTICS_FORMAT,
            failure_reason_code: self.sanitize_diagnostic_text(failure_reason_code),
            runtime: QaFailureRuntimeVersions {
                runner_version: self.sanitize_diagnostic_text(runner_version),
                runtime_version: self.sanitize_diagnostic_text(runtime_version),
                runtime_contract_version: self.sanitize_diagnostic_text(
                    self.runtime_health.public_runtime_contract_version.as_str(),
                ),
                palyrad_version: self
                    .sanitize_diagnostic_text(self.runtime_health.version.as_str()),
                palyrad_git_hash: self
                    .sanitize_diagnostic_text(self.runtime_health.git_hash.as_str()),
                palyrad_build_profile: self
                    .sanitize_diagnostic_text(self.runtime_health.build_profile.as_str()),
                qa_scenario_schema_version: self.runtime_health.qa_scenario_schema_version,
                qa_mock_provider_fixture_schema_version: self
                    .runtime_health
                    .qa_mock_provider_fixture_schema_version,
                daemon_restarts: self.daemon_restarts,
            },
            daemon_terminated,
            daemon_log_tail: self.redacted_log_tail(),
            fault_sidecar: self.failure_fault_sidecar(filesystem_available),
            journal: self.failure_journal_projection(filesystem_available),
            workspace: self.failure_workspace_projection(filesystem_available),
        }
    }

    /// Waits for a declared fault to terminate the daemon with the pinned code.
    pub(super) fn wait_for_expected_exit(
        &mut self,
        expected_code: i32,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| anyhow::anyhow!("qa.runner.expected_exit_deadline_overflow"))?;
        loop {
            let process = self.child.as_mut().context("qa.runner.expected_exit_without_process")?;
            let status =
                process.child.try_wait().context("qa.runner.expected_exit_status_failed")?;
            if let Some(status) = status {
                if !process.cleanup_descendants_after_observed_exit(
                    deadline.saturating_duration_since(Instant::now()),
                ) {
                    anyhow::bail!("qa.runner.expected_exit_descendant_cleanup_failed");
                }
                let log_drains = join_owned_log_threads_bounded(
                    &mut self.log_threads,
                    LOG_DRAIN_JOIN_TIMEOUT.min(deadline.saturating_duration_since(Instant::now())),
                );
                self.log_drain_join_failed |= log_drains.join_failed;
                if !log_drains.all_joined || self.log_drain_join_failed {
                    anyhow::bail!("qa.runner.expected_exit_log_drain_failed");
                }
                self.child.take();
                if status.code() != Some(expected_code) {
                    anyhow::bail!(
                        "qa.runner.unexpected_daemon_exit: expected_code={expected_code}; diagnostics={}",
                        bounded_log_summary(&self.log_tail)
                    );
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "qa.runner.expected_daemon_exit_timeout: diagnostics={}",
                    bounded_log_summary(&self.log_tail)
                );
            }
            thread::sleep(
                SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }

    /// Restarts the daemon while preserving the scenario journal and workspace.
    pub(super) fn restart_preserving_state(&mut self, timeout: Duration) -> Result<()> {
        if self.child.is_some() || !self.log_threads.is_empty() || self.log_drain_join_failed {
            anyhow::bail!("qa.runner.restart_requires_terminated_daemon");
        }
        self.verify_state_root_identity()
            .context("qa.runner.restart_state_root_identity_invalid")?;
        let cleanup_admission = self
            .cleanup_admission
            .as_ref()
            .context("qa.runner.restart_cleanup_admission_missing")?;
        let started = start_daemon(
            &mut self.launch,
            timeout,
            Arc::clone(&self.state_root),
            cleanup_admission,
        )?;
        self.child = Some(started.process);
        self.admin_port = started.admin_port;
        self.grpc_port = started.grpc_port;
        self.log_threads = started.log_threads;
        self.log_tail = started.log_tail;
        self.runtime_health = started.runtime_health;
        if let Some(document) = started.fault_launch_document {
            self.fault_launch_documents.push(document);
        }
        if let Some(sentinel) = started.fault_secret_sentinel {
            self.secret_sentinels.push(sentinel);
        }
        self.daemon_restarts = self
            .daemon_restarts
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("qa.runner.daemon_restart_count_overflow"))?;
        Ok(())
    }

    pub(super) fn contains_secret(&self, payload: &[u8]) -> bool {
        self.secret_sentinels.iter().any(|secret| {
            payload.windows(secret.as_slice().len()).any(|window| window == secret.as_slice())
        })
    }

    fn sanitize_diagnostic_text(&self, text: &str) -> String {
        self.project_diagnostic_text(text).0
    }

    fn project_diagnostic_text(&self, text: &str) -> (String, bool) {
        if self.contains_secret(text.as_bytes()) {
            return (REDACTED_SECRET_SENTINEL.to_owned(), false);
        }
        let redacted = redact_diagnostic_text(text);
        let redaction_complete = redacted == text;
        let mut characters = redacted.chars();
        let bounded = characters.by_ref().take(MAX_FAILURE_TEXT_CHARS).collect::<String>();
        let text_complete = characters.next().is_none();
        if contains_absolute_path_marker(bounded.as_str()) {
            (REDACTED_ABSOLUTE_PATH.to_owned(), false)
        } else {
            (bounded, redaction_complete && text_complete)
        }
    }

    fn redacted_log_tail(&self) -> Vec<String> {
        lock_unpoisoned(&self.log_tail)
            .iter()
            .map(|line| self.sanitize_diagnostic_text(line))
            .collect()
    }

    fn failure_fault_sidecar(&self, filesystem_available: bool) -> QaFailureFaultSidecar {
        if self.launch.fault.is_none() {
            return QaFailureFaultSidecar {
                status: "not_configured",
                reason_code: None,
                record_count: 0,
                records_complete: true,
                records: Vec::new(),
            };
        }
        if !filesystem_available {
            return QaFailureFaultSidecar {
                status: "unavailable",
                reason_code: Some("qa.runner.failure_diagnostics_runtime_not_quiesced"),
                record_count: 0,
                records_complete: false,
                records: Vec::new(),
            };
        }
        if self.verify_state_root_identity().is_err() {
            return QaFailureFaultSidecar {
                status: "unavailable",
                reason_code: Some("qa.runner.failure_diagnostics_state_root_identity_invalid"),
                record_count: 0,
                records_complete: false,
                records: Vec::new(),
            };
        }
        let sidecar = match self.fault_evidence_sidecar() {
            Ok(Some(sidecar)) => sidecar,
            Ok(None) | Err(_) => {
                return QaFailureFaultSidecar {
                    status: "unavailable",
                    reason_code: Some("qa.runner.failure_diagnostics_fault_sidecar_unavailable"),
                    record_count: 0,
                    records_complete: false,
                    records: Vec::new(),
                };
            }
        };
        let record_count = sidecar.records().len();
        let records = sidecar
            .records()
            .iter()
            .take(MAX_FAILURE_FAULT_RECORDS)
            .map(|record| project_failure_fault_record(self, record))
            .collect();
        QaFailureFaultSidecar {
            status: "available",
            reason_code: None,
            record_count,
            records_complete: record_count <= MAX_FAILURE_FAULT_RECORDS,
            records,
        }
    }

    fn failure_journal_projection(&self, filesystem_available: bool) -> QaFailureJournalProjection {
        if !filesystem_available {
            return unavailable_failure_journal(
                "qa.runner.failure_diagnostics_runtime_not_quiesced",
            );
        }
        if self.verify_state_root_identity().is_err() {
            return unavailable_failure_journal(
                "qa.runner.failure_diagnostics_state_root_identity_invalid",
            );
        }
        let root = self.launch.state_root.as_path();
        let Some(run_id) = self.active_run_id() else {
            return unavailable_failure_journal("qa.runner.failure_diagnostics_run_id_unavailable");
        };
        match load_failure_run_projection(self, root, run_id) {
            Ok(Some(run)) => QaFailureJournalProjection {
                status: "available",
                reason_code: None,
                run: Some(run),
            },
            Ok(None) => {
                unavailable_failure_journal("qa.runner.failure_diagnostics_journal_run_unavailable")
            }
            Err(_) => {
                unavailable_failure_journal("qa.runner.failure_diagnostics_journal_unavailable")
            }
        }
    }

    fn failure_workspace_projection(
        &self,
        filesystem_available: bool,
    ) -> QaFailureWorkspaceProjection {
        if !filesystem_available {
            return unavailable_failure_workspace(
                "qa.runner.failure_diagnostics_runtime_not_quiesced",
            );
        }
        if self.verify_state_root_identity().is_err() {
            return unavailable_failure_workspace(
                "qa.runner.failure_diagnostics_state_root_identity_invalid",
            );
        }
        match load_failure_workspace_projection(self) {
            Ok(projection) => projection,
            Err(_) => {
                unavailable_failure_workspace("qa.runner.failure_diagnostics_workspace_unavailable")
            }
        }
    }

    /// Records the latest session created by the scenario runtime.
    pub(super) fn record_session_id(&mut self, session_id: &str) {
        self.active_session_id = Some(session_id.to_owned());
    }

    /// Records the latest run created by the scenario runtime.
    pub(super) fn record_run_id(&mut self, run_id: &str) {
        self.active_run_id = Some(run_id.to_owned());
    }

    /// Returns the recorded session without consuming cleanup evidence.
    pub(super) fn active_session_id(&self) -> Option<&str> {
        self.active_session_id.as_deref()
    }

    /// Returns the recorded run without consuming cleanup evidence.
    pub(super) fn active_run_id(&self) -> Option<&str> {
        self.active_run_id.as_deref()
    }

    /// Archives the recorded session, or succeeds when execution never created one.
    pub(super) async fn cleanup_active_session(&self) -> bool {
        match self.active_session_id() {
            Some(session_id) => self.cleanup_session(session_id).await,
            None => true,
        }
    }

    /// Archives the scenario session through the production gateway API.
    pub(super) async fn cleanup_session(&self, session_id: &str) -> bool {
        let runtime = OperatorRuntime::new(AgentConnection {
            grpc_url: self.grpc_url(),
            token: Some(self.launch.admin_token.clone()),
            principal: self.launch.principal.clone(),
            device_id: self.device_id.clone(),
            channel: "qa".to_owned(),
            trace_id: format!("qa:{}", Ulid::new()),
        });
        cleanup_session_with_timeout(
            runtime.cleanup_session(SessionCleanupInput {
                session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
                session_key: String::new(),
            }),
            SESSION_CLEANUP_TIMEOUT,
        )
        .await
    }

    /// Terminates the daemon and joins its bounded log drains without removing state.
    pub(super) fn terminate_for_failure_diagnostics(&mut self) -> bool {
        let child_terminated = match self.child.as_mut() {
            Some(process) => process.terminate_tree(DAEMON_TERMINATION_TIMEOUT),
            None => true,
        };
        if !child_terminated {
            return false;
        }
        let log_drains =
            join_owned_log_threads_bounded(&mut self.log_threads, LOG_DRAIN_JOIN_TIMEOUT);
        self.log_drain_join_failed |= log_drains.join_failed;
        let cleanup_complete = log_drains.all_joined && !self.log_drain_join_failed;
        if cleanup_complete {
            self.child.take();
        }
        cleanup_complete
    }

    /// Removes the isolated state root after failure diagnostics have been persisted.
    pub(super) fn remove_state_root(&mut self) -> bool {
        if self.child.is_some() || !self.log_threads.is_empty() {
            return false;
        }
        lock_unpoisoned(&self.state_root).remove_verified()
    }

    /// Terminates the daemon, joins log drains, and removes the temporary root.
    pub(super) fn shutdown(&mut self) -> QaDaemonShutdown {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> QaDaemonShutdown {
        let daemon_terminated = self.terminate_for_failure_diagnostics();
        let workspace_removed = self.remove_state_root();
        if daemon_terminated && workspace_removed {
            self.cleanup_admission.take();
        }
        QaDaemonShutdown { daemon_terminated, workspace_removed }
    }
}

/// Enables only the local runtimes explicitly required by the isolated scenario.
fn isolated_daemon_config(manifest: &QaScenarioManifest) -> &'static str {
    if manifest.requires.tools.iter().any(|tool| tool.starts_with("palyra.process.")) {
        QA_PROCESS_DAEMON_CONFIG
    } else {
        QA_BASE_DAEMON_CONFIG
    }
}

fn copy_workspace_fixture(source: &Path, destination: &Path) -> Result<()> {
    if !source.is_dir() {
        anyhow::bail!("qa.runner.workspace_fixture_invalid: workspace fixture must be a directory");
    }
    let mut budget = WorkspaceCopyBudget::default();
    copy_workspace_directory(source, destination, &mut budget, 0)
}

#[derive(Default)]
struct WorkspaceCopyBudget {
    entries: usize,
    bytes: u64,
}

fn copy_workspace_directory(
    source: &Path,
    destination: &Path,
    budget: &mut WorkspaceCopyBudget,
    depth: usize,
) -> Result<()> {
    if depth > MAX_WORKSPACE_DEPTH {
        anyhow::bail!("qa.runner.workspace_fixture_too_deep");
    }
    fs::create_dir_all(destination).with_context(|| {
        format!("failed to create copied QA workspace directory {}", destination.display())
    })?;
    for entry in fs::read_dir(source)
        .with_context(|| format!("failed to read QA workspace fixture {}", source.display()))?
    {
        let entry = entry.context("failed to inspect QA workspace fixture entry")?;
        budget.entries = budget.entries.saturating_add(1);
        if budget.entries > MAX_WORKSPACE_ENTRIES {
            anyhow::bail!("qa.runner.workspace_fixture_too_many_entries");
        }
        let metadata = fs::symlink_metadata(entry.path())
            .context("failed to inspect QA workspace fixture metadata")?;
        if metadata.file_type().is_symlink() {
            anyhow::bail!("qa.runner.workspace_fixture_symlink_denied");
        }
        let target = destination.join(entry.file_name());
        if metadata.is_dir() {
            copy_workspace_directory(
                entry.path().as_path(),
                target.as_path(),
                budget,
                depth.saturating_add(1),
            )?;
        } else if metadata.is_file() {
            budget.bytes = budget.bytes.saturating_add(metadata.len());
            if budget.bytes > MAX_WORKSPACE_BYTES {
                anyhow::bail!("qa.runner.workspace_fixture_too_large");
            }
            fs::copy(entry.path(), target.as_path())
                .context("failed to copy QA workspace fixture file")?;
        } else {
            anyhow::bail!("qa.runner.workspace_fixture_special_file_denied");
        }
    }
    Ok(())
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
