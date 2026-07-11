//! Isolated daemon process and workspace lifecycle for the fixture runner.

use std::{
    collections::{BTreeMap, VecDeque},
    env, fs,
    future::Future,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, ChildStderr, ChildStdout, Command, Stdio},
    sync::{mpsc, Arc, Mutex, MutexGuard},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use palyra_auth::{AuthCredential, AuthProfileRecord, AuthProfileScope};
use palyra_common::{
    qa_scenarios::{
        QaScenarioApprovalDecision, QaScenarioLiveProviderKind, QaScenarioManifest,
        QaScenarioStepAction, QA_SCENARIO_SCHEMA_VERSION,
    },
    runtime_contracts::PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
};
use palyra_model_providers::QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION;
use palyra_vault::{BackendPreference, Vault, VaultConfig, VaultRef, VaultScope};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use ulid::Ulid;

use crate::{
    client::operator::OperatorRuntime,
    proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    AgentConnection, SessionCleanupInput,
};

use super::{
    digest_materialized_fixture_set, resolve_runner_path, sha256_file,
    validate_redacted_replay_fixture, QaPreparedLiveBinding, QaPreparedRunnerBinding,
    QaPreparedScenarioExecution,
};

const DAEMON_START_TIMEOUT: Duration = Duration::from_secs(15);
const DAEMON_HEALTH_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const DAEMON_TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_DRAIN_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAX_WORKSPACE_ENTRIES: usize = 1_024;
const MAX_WORKSPACE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_WORKSPACE_DEPTH: usize = 32;
const MAX_LOG_TAIL_LINES: usize = 32;
const MAX_LOG_LINE_CHARS: usize = 2_048;
const QA_RUNNER_PRINCIPAL: &str = "admin:qa-runner";
const QA_LIVE_PROFILE_ALIAS: &str = "qa-live-selected";
const QA_AUTH_REGISTRY_SCHEMA_VERSION: u32 = 1;
const MIN_LIVE_SECRET_BYTES: usize = 8;
const QA_READ_ONLY_TOOLS: &[&str] =
    &["palyra.fs.read_file", "palyra.fs.list_dir", "palyra.fs.search"];
const QA_APPROVAL_DENIED_TOOLS: &[&str] = &["palyra.fs.apply_patch"];

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
    child: Option<Child>,
    state_root: Option<TempDir>,
    workspace: PathBuf,
    admin_port: u16,
    grpc_port: u16,
    admin_token: String,
    principal: String,
    device_id: String,
    active_session_id: Option<String>,
    active_run_id: Option<String>,
    log_threads: Vec<JoinHandle<()>>,
    runtime_health: QaDaemonRuntimeHealth,
    live_secret_sentinels: Vec<SecretBytes>,
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
        let state_root = tempfile::Builder::new()
            .prefix("palyra-qa-")
            .tempdir()
            .context("qa.runner.temp_root_create_failed: failed to create isolated state root")?;
        let fixture_snapshot = materialize_fixture_snapshot(
            prepared.repository_root.as_path(),
            prepared.fixture_paths.as_slice(),
            prepared.execution_key.fixture_set_sha256.as_str(),
            state_root.path().join("input-snapshot").as_path(),
        )?;
        if let QaPreparedRunnerBinding::RecordReplay { replay_fixture } = &prepared.binding {
            validate_redacted_replay_fixture(fixture_snapshot.path(replay_fixture)?.as_path())?;
        }
        let workspace = state_root.path().join("workspace");
        fs::create_dir_all(workspace.as_path())
            .with_context(|| format!("failed to create QA workspace {}", workspace.display()))?;
        match prepared.workspace_fixture.as_deref() {
            Some(source) => copy_workspace_fixture(
                fixture_snapshot.path(source)?.as_path(),
                workspace.as_path(),
            )?,
            None => fs::write(workspace.join("README.md"), "# Isolated Palyra QA workspace\n")
                .context("failed to initialize empty QA workspace")?,
        }

        let config_path = state_root.path().join("palyra.toml");
        fs::write(config_path.as_path(), "version = 1\n")
            .context("failed to write isolated QA daemon config")?;
        let vault_dir = state_root.path().join("vault");
        fs::create_dir_all(vault_dir.as_path())
            .context("failed to create isolated QA vault directory")?;
        let identity_root = state_root.path().join("identity");

        let principal = QA_RUNNER_PRINCIPAL.to_owned();
        let device_id = Ulid::new().to_string();
        let admin_token = format!("qa-{}-{}", Ulid::new(), Ulid::new());
        let provider_environment = prepare_provider_environment(
            prepared,
            &fixture_snapshot,
            state_root.path(),
            identity_root.as_path(),
            vault_dir.as_path(),
        )?;
        let mut command = Command::new(prepared.palyrad_binary.as_path());
        command
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                "0",
                "--grpc-bind",
                "127.0.0.1",
                "--grpc-port",
                "0",
            ])
            .current_dir(workspace.as_path())
            .env_clear();
        preserve_platform_environment(&mut command);
        configure_isolated_environment(
            &mut command,
            QaDaemonEnvironment {
                manifest,
                state_root: state_root.path(),
                identity_root: identity_root.as_path(),
                config_path: config_path.as_path(),
                vault_dir: vault_dir.as_path(),
                provider: &provider_environment.provider,
                admin_token: admin_token.as_str(),
                principal: principal.as_str(),
            },
        );
        command.stdout(Stdio::piped()).stderr(Stdio::piped());

        let mut child = command.spawn().with_context(|| {
            format!(
                "qa.runner.daemon_start_failed: failed to start {}",
                prepared.palyrad_binary.display()
            )
        })?;
        let stdout = child
            .stdout
            .take()
            .context("qa.runner.daemon_stdout_unavailable: failed to capture daemon stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("qa.runner.daemon_stderr_unavailable: failed to capture daemon stderr")?;
        let log_tail = Arc::new(Mutex::new(VecDeque::new()));
        let (ports_tx, ports_rx) = mpsc::sync_channel(1);
        let stdout_thread = spawn_stdout_reader(stdout, ports_tx, Arc::clone(&log_tail));
        let stderr_thread = spawn_stderr_reader(stderr, Arc::clone(&log_tail));
        let (admin_port, grpc_port) = match wait_for_listen_ports(&ports_rx, &mut child, &log_tail)
        {
            Ok(ports) => ports,
            Err(error) => {
                terminate_and_join(&mut child, [stdout_thread, stderr_thread]);
                return Err(error);
            }
        };
        let runtime_health = match wait_for_health(
            admin_port,
            &mut child,
            &log_tail,
            prepared.runtime_contract_version.as_str(),
            prepared.expected_palyrad_git_hash.as_str(),
        ) {
            Ok(health) => health,
            Err(error) => {
                terminate_and_join(&mut child, [stdout_thread, stderr_thread]);
                return Err(error);
            }
        };

        Ok(Self {
            child: Some(child),
            state_root: Some(state_root),
            workspace,
            admin_port,
            grpc_port,
            admin_token,
            principal,
            device_id,
            active_session_id: None,
            active_run_id: None,
            log_threads: vec![stdout_thread, stderr_thread],
            runtime_health,
            live_secret_sentinels: provider_environment.live_secret_sentinels,
        })
    }

    pub(super) fn admin_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.admin_port)
    }

    pub(super) fn grpc_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.grpc_port)
    }

    pub(super) fn admin_token(&self) -> &str {
        self.admin_token.as_str()
    }

    pub(super) fn principal(&self) -> &str {
        self.principal.as_str()
    }

    pub(super) fn device_id(&self) -> &str {
        self.device_id.as_str()
    }

    pub(super) fn workspace(&self) -> &Path {
        self.workspace.as_path()
    }

    pub(super) fn runtime_health(&self) -> &QaDaemonRuntimeHealth {
        &self.runtime_health
    }

    pub(super) fn contains_live_secret(&self, payload: &[u8]) -> bool {
        self.live_secret_sentinels.iter().any(|secret| {
            payload.windows(secret.as_slice().len()).any(|window| window == secret.as_slice())
        })
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
            token: Some(self.admin_token.clone()),
            principal: self.principal.clone(),
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

    /// Terminates the daemon, joins log drains, and removes the temporary root.
    pub(super) fn shutdown(&mut self) -> QaDaemonShutdown {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> QaDaemonShutdown {
        let child_terminated = self.child.as_mut().is_none_or(terminate_child);
        self.child.take();
        let log_drains_joined =
            join_log_threads_bounded(self.log_threads.drain(..), LOG_DRAIN_JOIN_TIMEOUT);
        let daemon_terminated = child_terminated && log_drains_joined;
        let workspace_root = self.state_root.as_ref().map(|root| root.path().to_path_buf());
        let workspace_removed = match self.state_root.take() {
            Some(root) => {
                root.close().is_ok() && workspace_root.as_ref().is_none_or(|path| !path.exists())
            }
            None => workspace_root.as_ref().is_none_or(|path| !path.exists()),
        };
        QaDaemonShutdown { daemon_terminated, workspace_removed }
    }
}

impl Drop for QaDaemonSandbox {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

struct SecretBytes(Vec<u8>);

impl SecretBytes {
    fn new(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() < MIN_LIVE_SECRET_BYTES {
            anyhow::bail!("qa.runner.live_secret_too_short");
        }
        Ok(Self(bytes))
    }

    fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

struct QaPreparedProviderEnvironment {
    provider: QaDaemonProviderEnvironment,
    live_secret_sentinels: Vec<SecretBytes>,
}

enum QaDaemonProviderEnvironment {
    Deterministic {
        provider_fixture: PathBuf,
    },
    Live {
        registry_path: PathBuf,
        auth_provider_kind: String,
        transport: QaLiveTransportEnvironment,
    },
}

enum QaLiveTransportEnvironment {
    OpenAiCompatible { model: String, base_url: Option<String> },
    Anthropic { model: String, base_url: Option<String> },
}

#[derive(Serialize)]
struct QaAuthRegistryDocument<'a> {
    version: u32,
    profiles: &'a [AuthProfileRecord],
}

#[derive(Debug)]
struct QaMaterializedFixtureSnapshot {
    paths: BTreeMap<String, PathBuf>,
}

impl QaMaterializedFixtureSnapshot {
    fn path(&self, relative: &str) -> Result<&PathBuf> {
        self.paths
            .get(relative)
            .ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_snapshot_missing"))
    }
}

fn materialize_fixture_snapshot(
    repository_root: &Path,
    fixture_paths: &[String],
    expected_digest: &str,
    snapshot_root: &Path,
) -> Result<QaMaterializedFixtureSnapshot> {
    fs::create_dir_all(snapshot_root).context("qa.runner.fixture_snapshot_create_failed")?;
    let mut budget = WorkspaceCopyBudget::default();
    let mut paths = BTreeMap::new();
    let mut digest_entries = Vec::with_capacity(fixture_paths.len());
    for (index, relative) in fixture_paths.iter().enumerate() {
        if paths.contains_key(relative) {
            anyhow::bail!("qa.runner.fixture_snapshot_duplicate");
        }
        let source = resolve_runner_path(repository_root, relative, "declared fixture")?;
        let destination = snapshot_root.join(format!("{index:04}"));
        copy_fixture_input(source.as_path(), destination.as_path(), &mut budget, 0)?;
        digest_entries.push((relative.clone(), destination.clone()));
        paths.insert(relative.clone(), destination);
    }
    let actual_digest = digest_materialized_fixture_set(digest_entries.as_slice())?;
    if actual_digest != expected_digest {
        anyhow::bail!("qa.runner.fixture_changed");
    }
    Ok(QaMaterializedFixtureSnapshot { paths })
}

fn copy_fixture_input(
    source: &Path,
    destination: &Path,
    budget: &mut WorkspaceCopyBudget,
    depth: usize,
) -> Result<()> {
    if depth > MAX_WORKSPACE_DEPTH {
        anyhow::bail!("qa.runner.fixture_snapshot_too_deep");
    }
    let metadata =
        fs::symlink_metadata(source).context("qa.runner.fixture_snapshot_metadata_failed")?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("qa.runner.fixture_symlink_denied");
    }
    budget.entries = budget
        .entries
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_snapshot_too_many_entries"))?;
    if budget.entries > MAX_WORKSPACE_ENTRIES {
        anyhow::bail!("qa.runner.fixture_snapshot_too_many_entries");
    }
    if metadata.is_file() {
        budget.bytes = budget
            .bytes
            .checked_add(metadata.len())
            .ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_snapshot_too_large"))?;
        if budget.bytes > MAX_WORKSPACE_BYTES {
            anyhow::bail!("qa.runner.fixture_snapshot_too_large");
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).context("qa.runner.fixture_snapshot_create_failed")?;
        }
        fs::copy(source, destination).context("qa.runner.fixture_snapshot_copy_failed")?;
        return Ok(());
    }
    if !metadata.is_dir() {
        anyhow::bail!("qa.runner.fixture_special_file_denied");
    }
    fs::create_dir_all(destination).context("qa.runner.fixture_snapshot_create_failed")?;
    let mut children = fs::read_dir(source)
        .context("qa.runner.fixture_snapshot_read_failed")?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .context("qa.runner.fixture_snapshot_read_failed")?;
    children.sort_by_key(|path| path.file_name().map(std::ffi::OsStr::to_os_string));
    for child in children {
        let name = child
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_snapshot_path_invalid"))?;
        copy_fixture_input(child.as_path(), destination.join(name).as_path(), budget, depth + 1)?;
    }
    Ok(())
}

fn prepare_provider_environment(
    prepared: &QaPreparedScenarioExecution,
    fixture_snapshot: &QaMaterializedFixtureSnapshot,
    state_root: &Path,
    identity_root: &Path,
    vault_dir: &Path,
) -> Result<QaPreparedProviderEnvironment> {
    match &prepared.binding {
        QaPreparedRunnerBinding::Fixture { provider_fixture } => {
            Ok(QaPreparedProviderEnvironment {
                provider: QaDaemonProviderEnvironment::Deterministic {
                    provider_fixture: fixture_snapshot.path(provider_fixture)?.to_path_buf(),
                },
                live_secret_sentinels: Vec::new(),
            })
        }
        QaPreparedRunnerBinding::RecordReplay { replay_fixture } => {
            Ok(QaPreparedProviderEnvironment {
                provider: QaDaemonProviderEnvironment::Deterministic {
                    provider_fixture: fixture_snapshot.path(replay_fixture)?.to_path_buf(),
                },
                live_secret_sentinels: Vec::new(),
            })
        }
        QaPreparedRunnerBinding::Live(binding) => {
            prepare_live_provider_environment(binding, state_root, identity_root, vault_dir)
        }
    }
}

fn prepare_live_provider_environment(
    binding: &QaPreparedLiveBinding,
    state_root: &Path,
    identity_root: &Path,
    vault_dir: &Path,
) -> Result<QaPreparedProviderEnvironment> {
    let source_vault = Vault::open_default().context("qa.runner.live_source_vault_unavailable")?;
    let scoped_vault = open_isolated_live_vault(vault_dir, identity_root)?;
    let scope = VaultScope::Global;
    let mut sentinels = Vec::new();
    let credential = match &binding.profile.credential {
        AuthCredential::ApiKey { api_key_vault_ref } => AuthCredential::ApiKey {
            api_key_vault_ref: copy_live_secret(
                &source_vault,
                &scoped_vault,
                &scope,
                api_key_vault_ref,
                "api_key",
                &mut sentinels,
            )?,
        },
        AuthCredential::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint,
            client_id,
            client_secret_vault_ref,
            scopes,
            expires_at_unix_ms,
            refresh_state,
        } => AuthCredential::Oauth {
            access_token_vault_ref: copy_live_secret(
                &source_vault,
                &scoped_vault,
                &scope,
                access_token_vault_ref,
                "access_token",
                &mut sentinels,
            )?,
            refresh_token_vault_ref: copy_live_secret(
                &source_vault,
                &scoped_vault,
                &scope,
                refresh_token_vault_ref,
                "refresh_token",
                &mut sentinels,
            )?,
            token_endpoint: token_endpoint.clone(),
            client_id: client_id.clone(),
            client_secret_vault_ref: client_secret_vault_ref
                .as_deref()
                .map(|secret_ref| {
                    copy_live_secret(
                        &source_vault,
                        &scoped_vault,
                        &scope,
                        secret_ref,
                        "client_secret",
                        &mut sentinels,
                    )
                })
                .transpose()?,
            scopes: scopes.clone(),
            expires_at_unix_ms: *expires_at_unix_ms,
            refresh_state: refresh_state.clone(),
        },
    };
    let projected_profile = AuthProfileRecord {
        profile_id: QA_LIVE_PROFILE_ALIAS.to_owned(),
        provider: binding.profile.provider.clone(),
        profile_name: "QA live selected profile".to_owned(),
        scope: AuthProfileScope::Global,
        credential,
        created_at_unix_ms: binding.profile.created_at_unix_ms,
        updated_at_unix_ms: binding.profile.updated_at_unix_ms,
    };
    let registry_path = state_root.join("auth_profiles.toml");
    let registry = QaAuthRegistryDocument {
        version: QA_AUTH_REGISTRY_SCHEMA_VERSION,
        profiles: std::slice::from_ref(&projected_profile),
    };
    let registry_text =
        toml::to_string_pretty(&registry).context("qa.runner.live_registry_encode_failed")?;
    fs::write(registry_path.as_path(), registry_text.as_bytes())
        .context("qa.runner.live_registry_write_failed")?;
    palyra_vault::ensure_owner_only_file(registry_path.as_path())
        .context("qa.runner.live_registry_permissions_failed")?;

    Ok(QaPreparedProviderEnvironment {
        provider: QaDaemonProviderEnvironment::Live {
            registry_path,
            auth_provider_kind: binding.auth_provider_kind.clone(),
            transport: match binding.provider_kind {
                QaScenarioLiveProviderKind::OpenAiCompatible => {
                    QaLiveTransportEnvironment::OpenAiCompatible {
                        model: binding.model.clone(),
                        base_url: binding.base_url.clone(),
                    }
                }
                QaScenarioLiveProviderKind::Anthropic => QaLiveTransportEnvironment::Anthropic {
                    model: binding.model.clone(),
                    base_url: binding.base_url.clone(),
                },
            },
        },
        live_secret_sentinels: sentinels,
    })
}

fn open_isolated_live_vault(vault_dir: &Path, identity_root: &Path) -> Result<Vault> {
    Vault::open_with_config(VaultConfig {
        root: Some(vault_dir.to_path_buf()),
        identity_store_root: Some(identity_root.to_path_buf()),
        backend_preference: BackendPreference::EncryptedFile,
        ..VaultConfig::default()
    })
    .context("qa.runner.live_scoped_vault_unavailable")
}

fn copy_live_secret(
    source_vault: &Vault,
    scoped_vault: &Vault,
    scope: &VaultScope,
    source_reference: &str,
    label: &str,
    sentinels: &mut Vec<SecretBytes>,
) -> Result<String> {
    let source =
        VaultRef::parse(source_reference).context("qa.runner.live_secret_reference_invalid")?;
    let secret = SecretBytes::new(
        source_vault
            .get_secret(&source.scope, source.key.as_str())
            .context("qa.runner.live_secret_unavailable")?,
    )?;
    let key = format!("qa_live_{label}_{}", Ulid::new().to_string().to_ascii_lowercase());
    scoped_vault
        .put_secret(scope, key.as_str(), secret.as_slice())
        .context("qa.runner.live_secret_projection_failed")?;
    let reference = format!("{scope}/{key}");
    sentinels.push(secret);
    Ok(reference)
}

struct QaDaemonEnvironment<'a> {
    manifest: &'a QaScenarioManifest,
    state_root: &'a Path,
    identity_root: &'a Path,
    config_path: &'a Path,
    vault_dir: &'a Path,
    provider: &'a QaDaemonProviderEnvironment,
    admin_token: &'a str,
    principal: &'a str,
}

fn configure_isolated_environment(command: &mut Command, environment: QaDaemonEnvironment<'_>) {
    let allowed_tools = environment.manifest.requires.tools.join(",");
    command
        .env("PALYRA_CONFIG", environment.config_path)
        .env("PALYRA_STATE_ROOT", environment.state_root)
        .env("PALYRA_JOURNAL_DB_PATH", environment.state_root.join("data/journal.sqlite3"))
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", environment.identity_root)
        .env("PALYRA_VAULT_DIR", environment.vault_dir)
        .env("PALYRA_QA_LAB_MODE", "preview_only")
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("PALYRA_GATEWAY_QUIC_ENABLED", "false")
        .env("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS", "true")
        .env("PALYRA_ADMIN_REQUIRE_AUTH", "true")
        .env("PALYRA_ADMIN_TOKEN", environment.admin_token)
        .env("PALYRA_ADMIN_BOUND_PRINCIPAL", environment.principal)
        .env("PALYRA_TOOL_CALL_ALLOWED_TOOLS", allowed_tools)
        .env("RUST_LOG", "info");
    match environment.provider {
        QaDaemonProviderEnvironment::Deterministic { provider_fixture } => {
            command
                .env("PALYRA_MODEL_PROVIDER_KIND", "deterministic")
                .env("PALYRA_QA_MOCK_PROVIDER_FIXTURE_PATH", provider_fixture)
                .env("PALYRA_OFFLINE", "true");
        }
        QaDaemonProviderEnvironment::Live { registry_path, auth_provider_kind, transport } => {
            command
                .env("PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID", QA_LIVE_PROFILE_ALIAS)
                .env("PALYRA_MODEL_PROVIDER_AUTH_PROVIDER_KIND", auth_provider_kind)
                .env("PALYRA_AUTH_PROFILES_PATH", registry_path)
                .env("PALYRA_OFFLINE", "false");
            configure_live_transport_environment(command, transport);
        }
    }
}

fn configure_live_transport_environment(
    command: &mut Command,
    transport: &QaLiveTransportEnvironment,
) {
    match transport {
        QaLiveTransportEnvironment::OpenAiCompatible { model, base_url } => {
            command
                .env("PALYRA_MODEL_PROVIDER_KIND", "openai_compatible")
                .env("PALYRA_MODEL_PROVIDER_OPENAI_MODEL", model);
            if let Some(base_url) = base_url {
                command.env("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", base_url);
            }
        }
        QaLiveTransportEnvironment::Anthropic { model, base_url } => {
            command
                .env("PALYRA_MODEL_PROVIDER_KIND", "anthropic")
                .env("PALYRA_MODEL_PROVIDER_ANTHROPIC_MODEL", model);
            if let Some(base_url) = base_url {
                command.env("PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL", base_url);
            }
        }
    }
}

fn preserve_platform_environment(command: &mut Command) {
    const SAFE_KEYS: &[&str] = &[
        "PATH",
        "HOME",
        "USERPROFILE",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TMP",
        "LOCALAPPDATA",
        "APPDATA",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
    ];
    for key in SAFE_KEYS {
        if let Some(value) = env::var_os(key) {
            command.env(key, value);
        }
    }
}

fn validate_policy_profile(manifest: &QaScenarioManifest) -> Result<()> {
    let profile = manifest
        .runner
        .as_ref()
        .and_then(|runner| runner.policy_profile())
        .unwrap_or("qa_restricted");
    match profile {
        "qa_restricted" if manifest.requires.tools.is_empty() => Ok(()),
        "qa_restricted" => {
            anyhow::bail!("qa.runner.policy_profile_mismatch: qa_restricted cannot expose tools")
        }
        "qa_provider_recovery" if manifest.requires.tools.is_empty() => Ok(()),
        "qa_provider_recovery" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: qa_provider_recovery cannot expose tools"
        ),
        "qa_no_tools" if manifest.requires.tools.is_empty() => Ok(()),
        "qa_no_tools" => {
            anyhow::bail!("qa.runner.policy_profile_mismatch: qa_no_tools cannot expose tools")
        }
        "qa_read_only" if has_exact_tool_subset(&manifest.requires.tools, QA_READ_ONLY_TOOLS) => {
            Ok(())
        }
        "qa_read_only" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: qa_read_only requires explicit workspace read tools"
        ),
        "qa_approval_denied"
            if has_exact_tools(&manifest.requires.tools, QA_APPROVAL_DENIED_TOOLS)
                && approval_steps_deny_only(manifest) =>
        {
            Ok(())
        }
        "qa_approval_denied" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: qa_approval_denied requires only the approved mutation tool and explicit deny decisions"
        ),
        _ => anyhow::bail!(
            "qa.runner.unsupported_policy_profile: unsupported fixture policy profile"
        ),
    }
}

fn has_exact_tool_subset(tools: &[String], allowed: &[&str]) -> bool {
    !tools.is_empty()
        && tools.iter().all(|tool| allowed.contains(&tool.as_str()))
        && tools.iter().enumerate().all(|(index, tool)| !tools[..index].contains(tool))
}

fn has_exact_tools(tools: &[String], expected: &[&str]) -> bool {
    tools.len() == expected.len()
        && tools.iter().zip(expected).all(|(actual, expected)| actual == expected)
}

fn approval_steps_deny_only(manifest: &QaScenarioManifest) -> bool {
    let mut saw_deny = false;
    for step in
        manifest.steps.iter().filter(|step| step.action == QaScenarioStepAction::ApprovalDecision)
    {
        if !matches!(step.decision.as_ref(), Some(QaScenarioApprovalDecision::Deny)) {
            return false;
        }
        saw_deny = true;
    }
    saw_deny
}

async fn cleanup_session_with_timeout<F>(cleanup: F, timeout: Duration) -> bool
where
    F: Future<Output = Result<gateway_v1::CleanupSessionResponse>>,
{
    matches!(tokio::time::timeout(timeout, cleanup).await, Ok(Ok(response)) if response.cleaned)
}

fn spawn_stdout_reader(
    stdout: ChildStdout,
    ports_tx: mpsc::SyncSender<Result<(u16, u16), String>>,
    log_tail: Arc<Mutex<VecDeque<String>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut admin_port = None;
        let mut grpc_port = None;
        let mut ports_tx = Some(ports_tx);
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                if let Some(sender) = ports_tx.take() {
                    let _ = sender.send(Err("qa.runner.daemon_log_read_failed".to_owned()));
                }
                break;
            };
            push_log_tail(&log_tail, line.as_str());
            admin_port = admin_port.or_else(|| parse_port_from_log(&line, "\"listen_addr\":\""));
            grpc_port = grpc_port.or_else(|| parse_port_from_log(&line, "\"grpc_listen_addr\":\""));
            if let (Some(admin), Some(grpc)) = (admin_port, grpc_port) {
                if let Some(sender) = ports_tx.take() {
                    let _ = sender.send(Ok((admin, grpc)));
                }
            }
        }
        if let Some(sender) = ports_tx.take() {
            let _ = sender.send(Err("qa.runner.daemon_ports_not_published".to_owned()));
        }
    })
}

fn spawn_stderr_reader(
    stderr: ChildStderr,
    log_tail: Arc<Mutex<VecDeque<String>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            push_log_tail(&log_tail, line.as_str());
        }
    })
}

fn push_log_tail(log_tail: &Mutex<VecDeque<String>>, line: &str) {
    let mut tail = lock_unpoisoned(log_tail);
    let bounded = line.chars().take(MAX_LOG_LINE_CHARS).collect::<String>();
    tail.push_back(bounded);
    while tail.len() > MAX_LOG_TAIL_LINES {
        tail.pop_front();
    }
}

fn parse_port_from_log(line: &str, prefix: &str) -> Option<u16> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|address| address.port())
}

fn wait_for_listen_ports(
    receiver: &mpsc::Receiver<Result<(u16, u16), String>>,
    child: &mut Child,
    log_tail: &Mutex<VecDeque<String>>,
) -> Result<(u16, u16)> {
    let deadline = Instant::now() + DAEMON_START_TIMEOUT;
    // Stdout EOF can race process reaping; retain the reader failure until
    // the loop has a chance to report the more actionable child exit status.
    let mut reader_failure = None;
    loop {
        if reader_failure.is_none() {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(ports)) => return Ok(ports),
                Ok(Err(code)) => reader_failure = Some(code),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    reader_failure = Some("qa.runner.daemon_log_reader_disconnected".to_owned());
                }
            }
        } else {
            thread::sleep(SHUTDOWN_POLL_INTERVAL);
        }
        if let Some(status) = child.try_wait().context("failed to inspect QA daemon status")? {
            anyhow::bail!(
                "qa.runner.daemon_exited_early: status={status}; diagnostics={}",
                bounded_log_summary(log_tail)
            );
        }
        if Instant::now() >= deadline {
            let reason = reader_failure.as_deref().unwrap_or("qa.runner.daemon_start_timeout");
            anyhow::bail!("{reason}: diagnostics={}", bounded_log_summary(log_tail));
        }
    }
}

fn wait_for_health(
    port: u16,
    child: &mut Child,
    log_tail: &Mutex<VecDeque<String>>,
    expected_runtime_contract_version: &str,
    expected_git_hash: &str,
) -> Result<QaDaemonRuntimeHealth> {
    let deadline = Instant::now() + DAEMON_HEALTH_TIMEOUT;
    let request = b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().context("failed to inspect QA daemon status")? {
            anyhow::bail!(
                "qa.runner.daemon_exited_before_health: status={status}; diagnostics={}",
                bounded_log_summary(log_tail)
            );
        }
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
            let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
            if stream.write_all(request).is_ok() {
                let mut response = String::new();
                if stream.read_to_string(&mut response).is_ok()
                    && response.starts_with("HTTP/1.1 200")
                {
                    let health = parse_health_response(response.as_str())?;
                    validate_daemon_contract(
                        &health,
                        expected_runtime_contract_version,
                        expected_git_hash,
                    )?;
                    return Ok(health);
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    anyhow::bail!("qa.runner.daemon_health_timeout: diagnostics={}", bounded_log_summary(log_tail))
}

fn parse_health_response(response: &str) -> Result<QaDaemonRuntimeHealth> {
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("qa.runner.daemon_health_invalid"))?;
    let health = serde_json::from_str::<QaDaemonRuntimeHealth>(body.trim())
        .context("qa.runner.daemon_contract_mismatch")?;
    if health.service != "palyrad"
        || health.status != "ok"
        || health.version.trim().is_empty()
        || health.git_hash.trim().is_empty()
        || health.build_profile.trim().is_empty()
    {
        anyhow::bail!("qa.runner.daemon_health_invalid");
    }
    Ok(health)
}

fn validate_daemon_contract(
    health: &QaDaemonRuntimeHealth,
    expected_runtime_contract_version: &str,
    expected_git_hash: &str,
) -> Result<()> {
    let git_hash_mismatch = is_concrete_git_hash(expected_git_hash)
        && is_concrete_git_hash(health.git_hash.as_str())
        && health.git_hash != expected_git_hash;
    if health.public_runtime_contract_version != expected_runtime_contract_version
        || health.public_runtime_contract_version != PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION
        || health.qa_scenario_schema_version != QA_SCENARIO_SCHEMA_VERSION
        || health.qa_mock_provider_fixture_schema_version != QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION
        || git_hash_mismatch
    {
        anyhow::bail!("qa.runner.daemon_contract_mismatch");
    }
    Ok(())
}

fn is_concrete_git_hash(value: &str) -> bool {
    value.len() >= 7 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn bounded_log_summary(log_tail: &Mutex<VecDeque<String>>) -> String {
    let tail = lock_unpoisoned(log_tail);
    if tail.is_empty() {
        return "unavailable".to_owned();
    }
    // The durable runner descriptor must never inherit raw daemon output.
    format!("captured_lines={}", tail.len())
}

fn terminate_and_join<const N: usize>(child: &mut Child, threads: [JoinHandle<()>; N]) {
    let _ = terminate_child(child);
    let _ = join_log_threads_bounded(threads, LOG_DRAIN_JOIN_TIMEOUT);
}

fn terminate_child(child: &mut Child) -> bool {
    terminate_child_with_timeout(child, DAEMON_TERMINATION_TIMEOUT)
}

fn terminate_child_with_timeout(child: &mut Child, timeout: Duration) -> bool {
    match child.try_wait() {
        Ok(Some(_)) => return true,
        Ok(None) => {}
        Err(_) => return false,
    }
    if child.kill().is_err() {
        return false;
    }

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        thread::sleep(SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
    }
}

fn join_log_threads_bounded(
    threads: impl IntoIterator<Item = JoinHandle<()>>,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut all_joined = true;
    for thread in threads {
        while !thread.is_finished() && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            thread::sleep(SHUTDOWN_POLL_INTERVAL.min(remaining));
        }
        if thread.is_finished() {
            all_joined &= thread.join().is_ok();
        } else {
            // Detaching is the only bounded cross-platform fallback when a failed
            // child termination leaves a pipe reader blocked in the OS.
            all_joined = false;
        }
    }
    all_joined
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
mod tests {
    use super::*;
    use std::{ffi::OsStr, process::ExitStatus};

    const NO_TOOLS_SCENARIO: &str =
        include_str!("../../../../../qa/scenarios/real_runtime/text_exact.yaml");
    const READ_ONLY_SCENARIO: &str =
        include_str!("../../../../../qa/scenarios/real_runtime/read_only_tool.yaml");
    const APPROVAL_DENIED_SCENARIO: &str =
        include_str!("../../../../../qa/scenarios/real_runtime/mutation_approval_denied.yaml");
    const PROVIDER_RECOVERY_SCENARIO: &str =
        include_str!("../../../../../qa/scenarios/real_runtime/malformed_stream_recovery.yaml");

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

    fn command_env<'a>(command: &'a Command, key: &str) -> Option<&'a OsStr> {
        command
            .get_envs()
            .find(|(candidate, _)| *candidate == OsStr::new(key))
            .and_then(|(_, value)| value)
    }

    fn long_running_test_child() -> Child {
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
        command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("long-running cleanup child should start")
    }

    fn test_sandbox() -> (QaDaemonSandbox, PathBuf) {
        let state_root = tempfile::tempdir().expect("state root should exist");
        let root_path = state_root.path().to_path_buf();
        let workspace = root_path.join("workspace");
        fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        (
            QaDaemonSandbox {
                child: Some(long_running_test_child()),
                state_root: Some(state_root),
                workspace,
                admin_port: 1,
                grpc_port: 2,
                admin_token: "test-only-token".to_owned(),
                principal: "user:test".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                active_session_id: None,
                active_run_id: None,
                log_threads: Vec::new(),
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
                    qa_mock_provider_fixture_schema_version:
                        QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION,
                },
                live_secret_sentinels: Vec::new(),
            },
            root_path,
        )
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

    #[test]
    fn parse_port_requires_a_complete_socket_address() {
        assert_eq!(
            parse_port_from_log(r#"{"listen_addr":"127.0.0.1:43210"}"#, "\"listen_addr\":\""),
            Some(43_210)
        );
        assert_eq!(parse_port_from_log("listen_addr=43210", "\"listen_addr\":\""), None);
    }

    #[test]
    fn port_reader_eof_does_not_mask_an_early_daemon_exit() {
        #[cfg(windows)]
        let mut child = Command::new("cmd.exe")
            .args(["/D", "/C", "exit", "7"])
            .spawn()
            .expect("short-lived Windows child should start");
        #[cfg(not(windows))]
        let mut child = Command::new("sh")
            .args(["-c", "exit 7"])
            .spawn()
            .expect("short-lived Unix child should start");
        let status = child.wait().expect("short-lived child should exit");
        assert!(!status.success());
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .send(Err("qa.runner.daemon_ports_not_published".to_owned()))
            .expect("reader failure should be queued");
        drop(sender);
        let log_tail = Mutex::new(VecDeque::from(["secret-shaped diagnostic".to_owned()]));

        let error = wait_for_listen_ports(&receiver, &mut child, &log_tail)
            .expect_err("early exit should fail startup");
        let message = error.to_string();

        assert!(message.starts_with("qa.runner.daemon_exited_early:"));
        assert!(message.contains("diagnostics=captured_lines=1"));
        assert!(!message.contains("secret-shaped diagnostic"));
    }

    #[test]
    fn daemon_health_requires_current_contract_handshake_and_git_hash() {
        let body = serde_json::json!({
            "service": "palyrad",
            "status": "ok",
            "version": "0.1.0",
            "git_hash": "abcdef123456",
            "build_profile": "debug",
            "uptime_seconds": 1,
            "public_runtime_contract_version": PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
            "qa_scenario_schema_version": QA_SCENARIO_SCHEMA_VERSION,
            "qa_mock_provider_fixture_schema_version":
                QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION,
        });
        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{body}");
        let mut health =
            parse_health_response(response.as_str()).expect("current health should parse");
        validate_daemon_contract(&health, PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION, "abcdef123456")
            .expect("matching daemon contract should be accepted");

        health.public_runtime_contract_version = "runtime-contracts.stale".to_owned();
        assert!(validate_daemon_contract(
            &health,
            PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
            "abcdef123456",
        )
        .is_err());
        health.public_runtime_contract_version =
            PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION.to_owned();
        assert!(validate_daemon_contract(
            &health,
            PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
            "fedcba654321",
        )
        .is_err());

        let legacy = "HTTP/1.1 200 OK\r\n\r\n{\"service\":\"palyrad\",\"status\":\"ok\",\"version\":\"0.1.0\",\"git_hash\":\"abcdef123456\",\"build_profile\":\"debug\",\"uptime_seconds\":1}";
        let error = parse_health_response(legacy)
            .expect_err("legacy health without contract fields must be rejected");
        assert!(error.to_string().contains("qa.runner.daemon_contract_mismatch"));
    }

    #[test]
    fn runner_principal_uses_the_console_admin_namespace() {
        assert!(QA_RUNNER_PRINCIPAL.starts_with("admin:"));
    }

    #[test]
    fn active_runtime_ids_remain_available_for_cleanup_evidence() {
        let (mut sandbox, _) = test_sandbox();
        sandbox.record_session_id("01ARZ3NDEKTSV4RRFFQ69G5FAA");
        sandbox.record_run_id("01ARZ3NDEKTSV4RRFFQ69G5FAB");

        assert_eq!(sandbox.active_session_id(), Some("01ARZ3NDEKTSV4RRFFQ69G5FAA"));
        assert_eq!(sandbox.active_run_id(), Some("01ARZ3NDEKTSV4RRFFQ69G5FAB"));
    }

    #[tokio::test]
    async fn cleanup_without_an_active_session_is_a_successful_no_op() {
        let (sandbox, _) = test_sandbox();

        assert!(sandbox.cleanup_active_session().await);
    }

    #[test]
    fn no_tool_profiles_reject_every_tool_allowlist() {
        let mut no_tools = parse_scenario(NO_TOOLS_SCENARIO);
        assert!(validate_policy_profile(&no_tools).is_ok());
        no_tools.requires.tools.push("palyra.fs.read_file".to_owned());
        assert!(validate_policy_profile(&no_tools).is_err());

        let mut restricted = parse_scenario_with_policy_profile(NO_TOOLS_SCENARIO, "qa_restricted");
        assert!(validate_policy_profile(&restricted).is_ok());
        restricted.requires.tools.push("palyra.fs.read_file".to_owned());
        assert!(validate_policy_profile(&restricted).is_err());

        let mut recovery = parse_scenario(PROVIDER_RECOVERY_SCENARIO);
        assert!(validate_policy_profile(&recovery).is_ok());
        recovery.requires.tools.push("palyra.fs.read_file".to_owned());
        assert!(validate_policy_profile(&recovery).is_err());
    }

    #[test]
    fn read_only_profile_requires_a_unique_explicit_read_tool_subset() {
        let mut manifest = parse_scenario(READ_ONLY_SCENARIO);
        assert!(validate_policy_profile(&manifest).is_ok());

        manifest.requires.tools.clear();
        assert!(validate_policy_profile(&manifest).is_err());

        manifest.requires.tools = vec!["palyra.fs.search".to_owned()];
        assert!(validate_policy_profile(&manifest).is_ok());

        manifest.requires.tools.push("palyra.fs.apply_patch".to_owned());
        assert!(validate_policy_profile(&manifest).is_err());

        manifest.requires.tools =
            vec!["palyra.fs.read_file".to_owned(), "palyra.fs.read_file".to_owned()];
        assert!(validate_policy_profile(&manifest).is_err());
    }

    #[test]
    fn approval_denied_profile_requires_exact_mutation_tool_and_deny_only_steps() {
        let mut manifest = parse_scenario(APPROVAL_DENIED_SCENARIO);
        assert!(validate_policy_profile(&manifest).is_ok());

        manifest.requires.tools.push("palyra.process.run".to_owned());
        assert!(validate_policy_profile(&manifest).is_err());

        manifest.requires.tools = vec!["palyra.fs.apply_patch".to_owned()];
        let decision = manifest
            .steps
            .iter_mut()
            .find(|step| step.action == QaScenarioStepAction::ApprovalDecision)
            .expect("approval scenario should contain a decision step");
        decision.decision = Some(QaScenarioApprovalDecision::Allow);
        assert!(validate_policy_profile(&manifest).is_err());
    }

    #[test]
    fn no_tool_environment_uses_an_empty_allowlist() {
        let manifest = parse_scenario(NO_TOOLS_SCENARIO);
        let root = tempfile::tempdir().expect("environment root should exist");
        let mut command = Command::new("palyrad");
        let provider = QaDaemonProviderEnvironment::Deterministic {
            provider_fixture: root.path().join("provider.yaml"),
        };
        configure_isolated_environment(
            &mut command,
            QaDaemonEnvironment {
                manifest: &manifest,
                state_root: root.path(),
                identity_root: &root.path().join("identity"),
                config_path: &root.path().join("palyra.toml"),
                vault_dir: &root.path().join("vault"),
                provider: &provider,
                admin_token: "test-token",
                principal: "admin:test",
            },
        );

        let allowed_tools = command
            .get_envs()
            .find(|(key, _)| *key == std::ffi::OsStr::new("PALYRA_TOOL_CALL_ALLOWED_TOOLS"))
            .and_then(|(_, value)| value)
            .expect("tool allowlist environment should be configured");
        assert!(allowed_tools.is_empty());
    }

    #[test]
    fn projected_live_secret_reopens_with_the_child_identity_root() {
        const SECRET: &[u8] = b"qa-live-projection-secret";

        let source_root = tempfile::tempdir().expect("source vault root should exist");
        let source_vault = Vault::open_with_config(VaultConfig {
            root: Some(source_root.path().join("vault")),
            identity_store_root: Some(source_root.path().join("identity")),
            backend_preference: BackendPreference::EncryptedFile,
            ..VaultConfig::default()
        })
        .expect("source vault should open without host state");
        source_vault
            .put_secret(&VaultScope::Global, "source_api_key", SECRET)
            .expect("source secret should be stored");

        let child_root = tempfile::tempdir().expect("child state root should exist");
        let child_identity_root = child_root.path().join("identity");
        let child_vault_dir = child_root.path().join("vault");
        let projected_vault =
            open_isolated_live_vault(child_vault_dir.as_path(), child_identity_root.as_path())
                .expect("projected vault should bootstrap the child identity");
        let mut sentinels = Vec::new();
        let projected_reference = copy_live_secret(
            &source_vault,
            &projected_vault,
            &VaultScope::Global,
            "global/source_api_key",
            "api_key",
            &mut sentinels,
        )
        .expect("live secret should project into the child vault");
        drop(projected_vault);

        let child_vault = Vault::open_with_config(VaultConfig {
            root: Some(child_vault_dir),
            identity_store_root: Some(child_identity_root),
            ..VaultConfig::default()
        })
        .expect("child runtime should reopen its projected vault");
        let projected_reference = VaultRef::parse(projected_reference.as_str())
            .expect("projected reference should parse");
        let resolved = child_vault
            .get_secret(&projected_reference.scope, projected_reference.key.as_str())
            .expect("child runtime should decrypt the projected secret");

        assert_eq!(resolved, SECRET);
        assert_eq!(sentinels.len(), 1);
        assert_eq!(sentinels[0].as_slice(), SECRET);
    }

    #[test]
    fn openai_live_transport_sets_only_openai_model_and_endpoint_variables() {
        let mut command = Command::new("palyrad");
        configure_live_transport_environment(
            &mut command,
            &QaLiveTransportEnvironment::OpenAiCompatible {
                model: "gpt-test".to_owned(),
                base_url: Some("https://api.openai.example/v1".to_owned()),
            },
        );

        assert_eq!(
            command_env(&command, "PALYRA_MODEL_PROVIDER_KIND"),
            Some(OsStr::new("openai_compatible"))
        );
        assert_eq!(
            command_env(&command, "PALYRA_MODEL_PROVIDER_OPENAI_MODEL"),
            Some(OsStr::new("gpt-test"))
        );
        assert_eq!(
            command_env(&command, "PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL"),
            Some(OsStr::new("https://api.openai.example/v1"))
        );
        assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_ANTHROPIC_MODEL"), None);
        assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL"), None);
    }

    #[test]
    fn anthropic_live_transport_sets_only_anthropic_model_and_endpoint_variables() {
        let mut command = Command::new("palyrad");
        configure_live_transport_environment(
            &mut command,
            &QaLiveTransportEnvironment::Anthropic {
                model: "claude-test".to_owned(),
                base_url: Some("https://api.anthropic.example".to_owned()),
            },
        );

        assert_eq!(
            command_env(&command, "PALYRA_MODEL_PROVIDER_KIND"),
            Some(OsStr::new("anthropic"))
        );
        assert_eq!(
            command_env(&command, "PALYRA_MODEL_PROVIDER_ANTHROPIC_MODEL"),
            Some(OsStr::new("claude-test"))
        );
        assert_eq!(
            command_env(&command, "PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL"),
            Some(OsStr::new("https://api.anthropic.example"))
        );
        assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_OPENAI_MODEL"), None);
        assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL"), None);
    }

    #[tokio::test]
    async fn session_cleanup_requires_a_positive_cleaned_response() {
        let rejected = cleanup_session_with_timeout(
            async {
                Ok(gateway_v1::CleanupSessionResponse { cleaned: false, ..Default::default() })
            },
            Duration::from_millis(100),
        )
        .await;
        assert!(!rejected);

        let accepted = cleanup_session_with_timeout(
            async {
                Ok(gateway_v1::CleanupSessionResponse { cleaned: true, ..Default::default() })
            },
            Duration::from_millis(100),
        )
        .await;
        assert!(accepted);
    }

    #[tokio::test]
    async fn session_cleanup_timeout_covers_the_whole_operation() {
        let started = Instant::now();
        let cleaned = cleanup_session_with_timeout(
            std::future::pending::<Result<gateway_v1::CleanupSessionResponse>>(),
            Duration::from_millis(20),
        )
        .await;

        assert!(!cleaned);
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn workspace_copy_is_bounded_and_preserves_regular_files() {
        let source = tempfile::tempdir().expect("source tempdir should exist");
        let destination = tempfile::tempdir().expect("destination tempdir should exist");
        fs::create_dir_all(source.path().join("src")).expect("fixture directory should exist");
        fs::write(source.path().join("src/app.txt"), "fixture").expect("fixture file should exist");

        copy_workspace_fixture(source.path(), destination.path())
            .expect("regular fixture should copy");

        assert_eq!(
            fs::read_to_string(destination.path().join("src/app.txt"))
                .expect("copied file should be readable"),
            "fixture"
        );
    }

    #[test]
    fn fixture_snapshot_is_immutable_and_rejects_pre_copy_changes() {
        let repository = tempfile::tempdir().expect("repository root should exist");
        let repository_root =
            fs::canonicalize(repository.path()).expect("repository root should canonicalize");
        let fixture_path = repository_root.join("fixtures/provider.yaml");
        fs::create_dir_all(fixture_path.parent().expect("fixture should have a parent"))
            .expect("fixture directory should exist");
        fs::write(fixture_path.as_path(), b"original").expect("original fixture should be written");
        let paths = vec!["fixtures/provider.yaml".to_owned()];
        let expected = super::super::digest_repository_fixture_set(
            repository_root.as_path(),
            paths.iter().map(String::as_str),
        )
        .expect("fixture set should hash");
        let snapshot_root = repository_root.join("snapshot-a");
        let snapshot = materialize_fixture_snapshot(
            repository_root.as_path(),
            paths.as_slice(),
            expected.as_str(),
            snapshot_root.as_path(),
        )
        .expect("fixture snapshot should materialize");

        fs::write(fixture_path.as_path(), b"changed-after-snapshot")
            .expect("origin fixture should change");
        assert_eq!(
            fs::read(snapshot.path("fixtures/provider.yaml").expect("snapshot path should exist"))
                .expect("snapshot should remain readable"),
            b"original"
        );

        let stale_digest = super::super::digest_repository_fixture_set(
            repository_root.as_path(),
            paths.iter().map(String::as_str),
        )
        .expect("changed fixture set should hash");
        fs::write(fixture_path.as_path(), b"changed-before-copy")
            .expect("origin fixture should change again");
        let error = materialize_fixture_snapshot(
            repository_root.as_path(),
            paths.as_slice(),
            stale_digest.as_str(),
            repository_root.join("snapshot-b").as_path(),
        )
        .expect_err("changed input must not run under a stale execution key");
        assert!(error.to_string().contains("qa.runner.fixture_changed"));
    }

    #[test]
    fn shutdown_kills_worker_and_removes_workspace() {
        let (mut sandbox, root_path) = test_sandbox();

        let shutdown = sandbox.shutdown();

        assert!(shutdown.daemon_terminated);
        assert!(shutdown.workspace_removed);
        assert!(!root_path.exists());
    }

    #[test]
    fn log_drain_timeout_is_bounded_and_fail_closed() {
        let (release_tx, release_rx) = mpsc::channel();
        let blocked_reader = thread::spawn(move || {
            let _ = release_rx.recv();
        });
        let started = Instant::now();

        let joined = join_log_threads_bounded([blocked_reader], Duration::from_millis(20));

        assert!(!joined);
        assert!(started.elapsed() < Duration::from_millis(500));
        let _ = release_tx.send(());
    }

    #[test]
    fn panic_unwind_still_removes_isolated_workspace() {
        let (sandbox, root_path) = test_sandbox();
        let process_id =
            sandbox.child.as_ref().expect("sandbox should own its child before unwind").id();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _sandbox = sandbox;
            panic!("exercise QA sandbox unwind cleanup");
        }));

        assert!(outcome.is_err());
        assert!(
            wait_for_process_exit(process_id, Duration::from_secs(2)),
            "sandbox child {process_id} should be dead after unwind"
        );
        assert!(!root_path.exists());
    }
}
