//! Persistent, workspace-isolated language-server supervision.
//!
//! Server commands come only from host policy. The supervisor owns JSON-RPC
//! framing and metadata while the shared process actor retains every OS handle,
//! stdio stream, cleanup authority, and resource lease.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::local_resource_governor::{ResourcePriority, ResourceServiceKind, ResourceUnitsV1};
use super::process_supervisor::{
    ProcessLaunchSpec, ProcessOutputStream, ProcessOwnerV2, ProcessSessionState, ProcessSupervisor,
    ProcessSupervisorError,
};
use crate::sandbox_runner::redact_process_output_projection;

const LSP_HANDLE_SCHEMA_VERSION: u32 = 2;
const LSP_REGISTRY_SCHEMA_VERSION: u32 = 2;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_METHOD_BYTES: usize = 256;
const MAX_URI_BYTES: usize = 8 * 1024;
const PROCESS_WRITE_BYTES: usize = 8 * 1024;
const PROCESS_TAIL_CHUNKS: usize = 256;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Supported language-server policy class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LspLanguageV2 {
    /// Rust language server.
    Rust,
    /// TypeScript or JavaScript language server.
    TypeScript,
    /// Python language server.
    Python,
}

/// Host-owned executable policy. Callers select only a language, never a command.
#[derive(Debug, Clone)]
pub struct LspServerCommandPolicyV2 {
    /// Language served by this command.
    pub language: LspLanguageV2,
    /// Absolute trusted executable.
    pub executable: PathBuf,
    /// Host-configured argument vector.
    pub args: Vec<String>,
    /// Explicit environment after inherited values are cleared.
    pub env: BTreeMap<String, String>,
    /// Stable toolchain and configuration fingerprint.
    pub toolchain_fingerprint: String,
    /// Whether this server policy explicitly permits network access.
    pub network_allowed: bool,
}

/// Bounded LSP lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LspServerLifecycleV2 {
    /// Process exists but initialize has not completed.
    Starting,
    /// Initialize completed and the supervised process is active.
    Ready,
    /// Protocol, timeout, or process failure opened the broken cache.
    Broken,
    /// Idle reap or orderly shutdown stopped the server.
    Stopped,
    /// Resource pressure evicted an idle server.
    Evicted,
}

/// Closed structural summary of server-controlled initialize capabilities.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspCapabilitySummaryV2 {
    /// Whether the initialize response contained capability data.
    pub present: bool,
    /// Serialized byte count before the raw payload was discarded.
    pub serialized_bytes: u64,
    /// Whether the server declared a text-document synchronization mode.
    pub text_document_sync: bool,
    /// Whether the server declared a diagnostics provider.
    pub diagnostic_provider: bool,
    /// Whether workspace-folder support was explicitly enabled.
    pub workspace_folders: bool,
}

/// Durable metadata handle without raw command, workspace path, or server payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspServerHandleV2 {
    /// Handle schema version.
    pub schema_version: u32,
    /// Host-issued handle identity.
    pub handle_id: String,
    /// Hash of the canonical workspace root.
    pub workspace_root_sha256: String,
    /// Managed worktree identity.
    pub worktree_id: String,
    /// Language policy.
    pub language: LspLanguageV2,
    /// Toolchain fingerprint.
    pub toolchain_fingerprint: String,
    /// Shared process-session identity.
    pub process_session_id: String,
    /// Monotonic LSP generation.
    pub generation: u64,
    /// Current lifecycle.
    pub lifecycle: LspServerLifecycleV2,
    /// Closed summary of initialize capabilities; the raw server payload is discarded.
    pub capabilities: LspCapabilitySummaryV2,
    /// Number of relaunches after the initial generation.
    pub restart_count: u32,
    /// Last diagnostics notification timestamp.
    pub last_diagnostics_at_unix_ms: Option<i64>,
    /// Last request or notification timestamp.
    pub last_used_at_unix_ms: i64,
    /// Stable lifecycle reason.
    pub reason_code: String,
}

/// Initialize or runtime failure cache entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspBrokenServerEntryV2 {
    /// Workspace hash.
    pub workspace_root_sha256: String,
    /// Language policy.
    pub language: LspLanguageV2,
    /// Toolchain fingerprint.
    pub toolchain_fingerprint: String,
    /// Consecutive failure count.
    pub failure_count: u32,
    /// Retry deadline when below the circuit threshold.
    pub retry_after_unix_ms: i64,
    /// Whether only manual reset may close the circuit.
    pub manual_reset_required: bool,
    /// Stable redacted reason.
    pub reason_code: String,
}

/// Host request to open or reuse one workspace server.
#[derive(Debug, Clone)]
pub struct LspWorkspaceOpenRequestV2 {
    /// Existing workspace root.
    pub workspace_root: PathBuf,
    /// Managed worktree identity used by cleanup and resource leases.
    pub worktree_id: String,
    /// Owning run identity.
    pub run_id: String,
    /// Language policy to select.
    pub language: LspLanguageV2,
}

/// Bounded JSON-RPC request result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspRequestOutcomeV2 {
    /// Server handle.
    pub handle_id: String,
    /// Server generation.
    pub server_generation: u64,
    /// Request id.
    pub request_id: u64,
    /// JSON-RPC result.
    pub result: Value,
    /// Stable success reason.
    pub reason_code: String,
    /// Wall-clock latency.
    pub elapsed_ms: u64,
}

/// Published diagnostics notification attributed to an exact document version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspPublishedDiagnosticsV2 {
    /// Server handle.
    pub handle_id: String,
    /// Server generation.
    pub server_generation: u64,
    /// Document URI.
    pub uri: String,
    /// Language-server document version.
    pub document_version: i64,
    /// Raw bounded diagnostic objects for typed normalization.
    pub diagnostics: Vec<Value>,
    /// Observation timestamp.
    pub observed_at_unix_ms: i64,
}

/// Redacted health projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspHealthSnapshotV2 {
    /// Active and terminal handles.
    pub handles: Vec<LspServerHandleV2>,
    /// Broken-server cache.
    pub broken_servers: Vec<LspBrokenServerEntryV2>,
    /// Active process-backed server count.
    pub active_servers: usize,
    /// Stable health reason.
    pub reason_code: String,
}

/// Closed operator projection of one language-server handle.
///
/// Server-controlled initialize capabilities are reduced to bounded structural
/// metadata. Host identities are hashed so diagnostics cannot become an
/// alternate path, process, or worktree discovery surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspDiagnosticsHandleV2 {
    /// Projection schema version.
    pub schema_version: u32,
    /// Hash of the host-issued handle identity.
    pub handle_id_sha256: String,
    /// Hash of the canonical workspace root.
    pub workspace_root_sha256: String,
    /// Hash of the managed worktree identity.
    pub worktree_id_sha256: String,
    /// Language policy.
    pub language: LspLanguageV2,
    /// Hash of the host-owned toolchain fingerprint.
    pub toolchain_fingerprint_sha256: String,
    /// Hash of the supervised process-session identity.
    pub process_session_id_sha256: String,
    /// Monotonic server generation.
    pub generation: u64,
    /// Current lifecycle.
    pub lifecycle: LspServerLifecycleV2,
    /// Whether the server returned any initialize capability data.
    pub capabilities_present: bool,
    /// Serialized capability byte count before omission.
    pub capabilities_bytes: u64,
    /// Number of relaunches after the initial generation.
    pub restart_count: u32,
    /// Last diagnostics notification timestamp.
    pub last_diagnostics_at_unix_ms: Option<i64>,
    /// Last request or notification timestamp.
    pub last_used_at_unix_ms: i64,
    /// Stable host-owned lifecycle reason.
    pub reason_code: String,
}

/// Closed operator projection of one broken-server cache entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspDiagnosticsBrokenServerV2 {
    /// Hash of the canonical workspace root.
    pub workspace_root_sha256: String,
    /// Language policy.
    pub language: LspLanguageV2,
    /// Hash of the host-owned toolchain fingerprint.
    pub toolchain_fingerprint_sha256: String,
    /// Consecutive failure count.
    pub failure_count: u32,
    /// Retry deadline when below the circuit threshold.
    pub retry_after_unix_ms: i64,
    /// Whether only manual reset may close the circuit.
    pub manual_reset_required: bool,
    /// Stable host-owned failure reason.
    pub reason_code: String,
}

/// Closed, bounded language-service diagnostics for operator surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LspDiagnosticsSnapshotV2 {
    /// Projection schema version.
    pub schema_version: u32,
    /// Active and retained terminal handles.
    pub handles: Vec<LspDiagnosticsHandleV2>,
    /// Broken-server cache.
    pub broken_servers: Vec<LspDiagnosticsBrokenServerV2>,
    /// Active process-backed server count.
    pub active_servers: usize,
    /// Stable health reason.
    pub reason_code: String,
}

/// Persistent supervisor bounds and host policy.
#[derive(Clone)]
pub struct LspWorkspaceSupervisorConfig {
    /// Absolute owner-only metadata file.
    pub registry_path: PathBuf,
    /// Maximum live workspace servers.
    pub max_servers: usize,
    /// Maximum retained active and terminal metadata handles.
    pub max_registry_entries: usize,
    /// Maximum JSON-RPC header bytes.
    pub max_header_bytes: usize,
    /// Maximum JSON-RPC body bytes.
    pub max_message_bytes: usize,
    /// Maximum retained notifications per server.
    pub max_notifications: usize,
    /// Initialize response deadline.
    pub initialize_timeout: Duration,
    /// Normal request deadline.
    pub request_timeout: Duration,
    /// Maximum process lifetime before deterministic cleanup.
    pub server_lifetime: Duration,
    /// Idle server reap threshold.
    pub idle_ttl: Duration,
    /// Retry delay after a server failure.
    pub broken_ttl: Duration,
    /// Consecutive failures before manual reset is required.
    pub circuit_breaker_failures: u32,
    /// Host evidence that denied-network policies run inside a network-isolated scope.
    pub network_isolation_verified: bool,
    /// Resource grant charged to the LSP service.
    pub resource_units: ResourceUnitsV1,
    /// Host command policies.
    pub policies: Vec<LspServerCommandPolicyV2>,
}

/// LSP policy, framing, process, lifecycle, or persistence failure.
#[derive(Debug, Error)]
pub enum LspWorkspaceSupervisorError {
    /// Configuration is unbounded or contains an untrusted executable.
    #[error("LSP workspace supervisor configuration is invalid")]
    InvalidConfiguration,
    /// Workspace, worktree, run, method, or payload identity is invalid.
    #[error("LSP workspace request is invalid: {0}")]
    InvalidRequest(String),
    /// No host policy exists for the requested language.
    #[error("LSP server is unavailable: no host command policy")]
    ServerUnavailable,
    /// A denied-network server cannot be launched without isolation evidence.
    #[error("LSP server network isolation is unavailable")]
    NetworkIsolationUnavailable,
    /// Live server capacity is exhausted.
    #[error("LSP server capacity is exhausted")]
    CapacityExhausted,
    /// Broken-server retry or manual-reset circuit is open.
    #[error("LSP broken-server circuit is open: {0}")]
    CircuitOpen(String),
    /// Handle is unknown.
    #[error("LSP server handle was not found")]
    HandleNotFound,
    /// Initialize or request deadline elapsed.
    #[error("LSP request timed out")]
    RequestTimeout,
    /// JSON-RPC header or body is malformed.
    #[error("LSP JSON-RPC framing is malformed")]
    MalformedFrame,
    /// A frame exceeds configured memory bounds.
    #[error("LSP JSON-RPC frame exceeds configured bounds")]
    OversizedFrame,
    /// JSON-RPC returned an error object.
    #[error("LSP JSON-RPC request failed")]
    JsonRpc,
    /// Supervised process exited before the operation completed.
    #[error("LSP server process exited")]
    ServerCrashed,
    /// Shared process operation failed.
    #[error("LSP process supervision failed: {0}")]
    Process(String),
    /// Durable metadata access failed.
    #[error("LSP metadata persistence failed: {0}")]
    Persistence(String),
    /// In-memory state was poisoned.
    #[error("LSP supervisor state is unavailable")]
    StateUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LspServerKey {
    workspace_root_sha256: String,
    language: LspLanguageV2,
    toolchain_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LspRegistryV2 {
    schema_version: u32,
    handles: Vec<LspServerHandleV2>,
    broken_servers: Vec<LspBrokenServerEntryV2>,
    updated_at_unix_ms: i64,
}

struct LiveLspServer {
    workspace_root: PathBuf,
    handle: LspServerHandleV2,
    output_cursor: u64,
    stdout_buffer: Vec<u8>,
    notifications: VecDeque<Value>,
    next_request_id: u64,
    last_used: Instant,
    stderr_preview: String,
}

struct SupervisorState {
    registry: LspRegistryV2,
    servers: BTreeMap<LspServerKey, Arc<Mutex<LiveLspServer>>>,
}

/// Process-backed persistent LSP workspace authority.
pub struct LspWorkspaceSupervisor {
    config: LspWorkspaceSupervisorConfig,
    process_supervisor: Arc<ProcessSupervisor>,
    policies: BTreeMap<LspLanguageV2, LspServerCommandPolicyV2>,
    state: Mutex<SupervisorState>,
}

impl LspWorkspaceSupervisor {
    /// Opens durable metadata and marks handles from a prior daemon generation stopped.
    ///
    /// # Errors
    /// Returns an error for invalid host policy or corrupt durable metadata.
    pub fn open(
        config: LspWorkspaceSupervisorConfig,
        process_supervisor: Arc<ProcessSupervisor>,
    ) -> Result<Self, LspWorkspaceSupervisorError> {
        validate_config(&config)?;
        if let Some(parent) = config.registry_path.parent() {
            create_private_dir(parent)?;
        }
        let mut registry = if config.registry_path.exists() {
            read_registry(config.registry_path.as_path())?
        } else {
            LspRegistryV2 {
                schema_version: LSP_REGISTRY_SCHEMA_VERSION,
                handles: Vec::new(),
                broken_servers: Vec::new(),
                updated_at_unix_ms: unix_time_ms(),
            }
        };
        let mut changed = false;
        for handle in &mut registry.handles {
            if matches!(
                handle.lifecycle,
                LspServerLifecycleV2::Starting | LspServerLifecycleV2::Ready
            ) {
                handle.lifecycle = LspServerLifecycleV2::Stopped;
                handle.reason_code = "lsp.restart_requires_relaunch".to_owned();
                changed = true;
            }
        }
        registry.updated_at_unix_ms = unix_time_ms();
        if changed || !config.registry_path.exists() {
            write_registry(config.registry_path.as_path(), &registry)?;
        }
        let policies =
            config.policies.iter().cloned().map(|policy| (policy.language, policy)).collect();
        Ok(Self {
            config,
            process_supervisor,
            policies,
            state: Mutex::new(SupervisorState { registry, servers: BTreeMap::new() }),
        })
    }

    /// Starts or reuses the exact workspace, language, and toolchain service.
    ///
    /// # Errors
    /// Returns an error for unsafe workspace state, missing policy, circuit
    /// breaker, initialize failure, or process admission failure.
    pub fn ensure(
        &self,
        request: LspWorkspaceOpenRequestV2,
    ) -> Result<LspServerHandleV2, LspWorkspaceSupervisorError> {
        validate_identity("worktree_id", request.worktree_id.as_str())?;
        validate_identity("run_id", request.run_id.as_str())?;
        let workspace_root = canonical_workspace(request.workspace_root.as_path())?;
        let policy = self
            .policies
            .get(&request.language)
            .cloned()
            .ok_or(LspWorkspaceSupervisorError::ServerUnavailable)?;
        if !policy.network_allowed && !self.config.network_isolation_verified {
            return Err(LspWorkspaceSupervisorError::NetworkIsolationUnavailable);
        }
        let key = LspServerKey {
            workspace_root_sha256: sha256_path(workspace_root.as_path()),
            language: request.language,
            toolchain_fingerprint: policy.toolchain_fingerprint.clone(),
        };
        self.reap_expired_broken_cache()?;
        if let Some(server) = self.server_for_key(&key)? {
            let mut live = lock_server(&server)?;
            let status = self
                .process_supervisor
                .status(live.handle.process_session_id.as_str())
                .map_err(map_process_error)?;
            if status.state == ProcessSessionState::Running {
                live.last_used = Instant::now();
                live.handle.last_used_at_unix_ms = unix_time_ms();
                let handle = live.handle.clone();
                drop(live);
                self.persist_live_handle(handle.clone())?;
                return Ok(handle);
            }
            let handle = live.handle.clone();
            drop(live);
            self.record_failure(&key, &handle, "lsp.server_crashed")?;
            return Err(LspWorkspaceSupervisorError::ServerCrashed);
        }
        {
            let state = self.lock_state()?;
            if let Some(broken) = broken_for_key(&state.registry, &key) {
                if broken.manual_reset_required || broken.retry_after_unix_ms > unix_time_ms() {
                    return Err(LspWorkspaceSupervisorError::CircuitOpen(
                        broken.reason_code.clone(),
                    ));
                }
            }
            if state.servers.len() >= self.config.max_servers {
                return Err(LspWorkspaceSupervisorError::CapacityExhausted);
            }
        }

        let previous = self.latest_handle_for_key(&key)?;
        let generation = previous.as_ref().map_or(1, |handle| handle.generation.saturating_add(1));
        let restart_count =
            previous.as_ref().map_or(0, |handle| handle.restart_count.saturating_add(1));
        let process_record = self
            .process_supervisor
            .launch(ProcessLaunchSpec {
                executable: policy.executable,
                args: policy.args,
                cwd: workspace_root.clone(),
                env: policy.env,
                owner: ProcessOwnerV2 {
                    session_id: "coding-lsp".to_owned(),
                    run_id: format!("worktree-{}", request.worktree_id),
                    turn_id: "lsp-service".to_owned(),
                    agent_id: "lsp-workspace-supervisor".to_owned(),
                    correlation_id: format!(
                        "lsp-{}-{generation}",
                        key.workspace_root_sha256.get(..16).unwrap_or("workspace")
                    ),
                },
                timeout: self.config.server_lifetime,
                no_output_timeout: None,
                lease_duration: self.config.server_lifetime + Duration::from_secs(30),
                resource_priority: ResourcePriority::IdleService,
                resource_service: ResourceServiceKind::Lsp,
                resource_units: self.config.resource_units,
            })
            .map_err(map_process_error)?;
        let now = unix_time_ms();
        let handle = LspServerHandleV2 {
            schema_version: LSP_HANDLE_SCHEMA_VERSION,
            handle_id: format!("lsp_{}", ulid::Ulid::new()),
            workspace_root_sha256: key.workspace_root_sha256.clone(),
            worktree_id: request.worktree_id,
            language: request.language,
            toolchain_fingerprint: key.toolchain_fingerprint.clone(),
            process_session_id: process_record.process_session_id,
            generation,
            lifecycle: LspServerLifecycleV2::Starting,
            capabilities: LspCapabilitySummaryV2::default(),
            restart_count,
            last_diagnostics_at_unix_ms: None,
            last_used_at_unix_ms: now,
            reason_code: "lsp.initialize_pending".to_owned(),
        };
        let mut live = LiveLspServer {
            workspace_root,
            handle,
            output_cursor: 0,
            stdout_buffer: Vec::new(),
            notifications: VecDeque::new(),
            next_request_id: 1,
            last_used: Instant::now(),
            stderr_preview: String::new(),
        };
        let workspace_uri = path_to_file_uri(live.workspace_root.as_path());
        let initialize = self.send_request_to_live(
            &mut live,
            "initialize",
            json!({
                "processId": null,
                "rootUri": workspace_uri.clone(),
                "capabilities": {},
                "workspaceFolders": [{
                    "uri": workspace_uri,
                    "name": "workspace"
                }]
            }),
            self.config.initialize_timeout,
        );
        match initialize {
            Ok(outcome) => {
                let capabilities =
                    outcome.result.get("capabilities").cloned().unwrap_or(Value::Null);
                if serde_json::to_vec(&capabilities)
                    .map(|bytes| bytes.len() > self.config.max_message_bytes)
                    .unwrap_or(true)
                {
                    let _ =
                        self.process_supervisor.terminate(live.handle.process_session_id.as_str());
                    self.record_failure(&key, &live.handle, "lsp.capabilities_oversized")?;
                    return Err(LspWorkspaceSupervisorError::OversizedFrame);
                }
                live.handle.capabilities = summarize_capabilities(&capabilities);
                live.handle.lifecycle = LspServerLifecycleV2::Ready;
                live.handle.reason_code = "lsp.initialized".to_owned();
                self.send_notification_to_live(&mut live, "initialized", json!({}))?;
                let handle = live.handle.clone();
                let server = Arc::new(Mutex::new(live));
                let mut state = self.lock_state()?;
                remove_broken_for_key(&mut state.registry, &key);
                upsert_handle(&mut state.registry, handle.clone());
                state.servers.insert(key, server);
                persist_state(&self.config, &mut state)?;
                Ok(handle)
            }
            Err(error) => {
                let _ = self.process_supervisor.terminate(live.handle.process_session_id.as_str());
                self.record_failure(&key, &live.handle, error_reason_code(&error))?;
                Err(error)
            }
        }
    }

    /// Sends one bounded JSON-RPC request to a ready server.
    ///
    /// # Errors
    /// Returns an error for unknown handles, protocol failure, timeout, or crash.
    pub fn request(
        &self,
        handle_id: &str,
        method: &str,
        params: Value,
    ) -> Result<LspRequestOutcomeV2, LspWorkspaceSupervisorError> {
        validate_method(method)?;
        let (key, server) = self.server_for_handle(handle_id)?;
        let result = {
            let mut live = lock_server(&server)?;
            if live.handle.lifecycle != LspServerLifecycleV2::Ready {
                return Err(LspWorkspaceSupervisorError::ServerCrashed);
            }
            self.send_request_to_live(&mut live, method, params, self.config.request_timeout)
        };
        if let Err(error) = &result {
            if matches!(
                error,
                LspWorkspaceSupervisorError::MalformedFrame
                    | LspWorkspaceSupervisorError::OversizedFrame
                    | LspWorkspaceSupervisorError::ServerCrashed
            ) {
                let handle = lock_server(&server)?.handle.clone();
                let _ = self.process_supervisor.terminate(handle.process_session_id.as_str());
                self.record_failure(&key, &handle, error_reason_code(error))?;
            }
        }
        result
    }

    /// Sends a bounded JSON-RPC notification.
    ///
    /// # Errors
    /// Returns an error for unknown handles, invalid method, or process backpressure.
    pub fn notify(
        &self,
        handle_id: &str,
        method: &str,
        params: Value,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        validate_method(method)?;
        let (_, server) = self.server_for_handle(handle_id)?;
        let mut live = lock_server(&server)?;
        self.send_notification_to_live(&mut live, method, params)
    }

    /// Sends the standard cancellation notification for one request id.
    ///
    /// # Errors
    /// Returns an error for unknown handles or process backpressure.
    pub fn cancel_request(
        &self,
        handle_id: &str,
        request_id: u64,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        self.notify(handle_id, "$/cancelRequest", json!({"id": request_id}))
    }

    /// Waits for diagnostics carrying at least the requested document version.
    ///
    /// # Errors
    /// Returns an explicit timeout instead of projecting missing diagnostics as empty.
    pub fn wait_for_diagnostics(
        &self,
        handle_id: &str,
        uri: &str,
        minimum_version: i64,
        timeout: Duration,
    ) -> Result<LspPublishedDiagnosticsV2, LspWorkspaceSupervisorError> {
        validate_document_uri(uri)?;
        if timeout.is_zero() {
            return Err(LspWorkspaceSupervisorError::InvalidRequest(
                "diagnostics timeout must be non-zero".to_owned(),
            ));
        }
        let (_, server) = self.server_for_handle(handle_id)?;
        let deadline = Instant::now() + timeout;
        loop {
            let mut live = lock_server(&server)?;
            self.pump_output(&mut live)?;
            if let Some(index) = live.notifications.iter().position(|notification| {
                notification.get("method").and_then(Value::as_str)
                    == Some("textDocument/publishDiagnostics")
                    && notification.pointer("/params/uri").and_then(Value::as_str) == Some(uri)
                    && notification
                        .pointer("/params/version")
                        .and_then(Value::as_i64)
                        .is_some_and(|version| version >= minimum_version)
            }) {
                let notification = live
                    .notifications
                    .remove(index)
                    .ok_or(LspWorkspaceSupervisorError::MalformedFrame)?;
                let diagnostics = notification
                    .pointer("/params/diagnostics")
                    .and_then(Value::as_array)
                    .cloned()
                    .ok_or(LspWorkspaceSupervisorError::MalformedFrame)?;
                let document_version = notification
                    .pointer("/params/version")
                    .and_then(Value::as_i64)
                    .ok_or(LspWorkspaceSupervisorError::MalformedFrame)?;
                let observed_at_unix_ms = unix_time_ms();
                live.handle.last_diagnostics_at_unix_ms = Some(observed_at_unix_ms);
                let handle = live.handle.clone();
                drop(live);
                self.persist_live_handle(handle.clone())?;
                return Ok(LspPublishedDiagnosticsV2 {
                    handle_id: handle.handle_id,
                    server_generation: handle.generation,
                    uri: uri.to_owned(),
                    document_version,
                    diagnostics,
                    observed_at_unix_ms,
                });
            }
            drop(live);
            if Instant::now() >= deadline {
                return Err(LspWorkspaceSupervisorError::RequestTimeout);
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    /// Stops servers idle past the configured threshold.
    ///
    /// # Errors
    /// Returns an error when state or process cleanup is unavailable.
    pub fn reap_idle(&self) -> Result<Vec<LspServerHandleV2>, LspWorkspaceSupervisorError> {
        let candidates = {
            let state = self.lock_state()?;
            state
                .servers
                .iter()
                .filter_map(|(key, server)| {
                    let live = server.lock().ok()?;
                    (live.last_used.elapsed() >= self.config.idle_ttl)
                        .then_some((key.clone(), Arc::clone(server)))
                })
                .collect::<Vec<_>>()
        };
        let mut reaped = Vec::new();
        for (key, server) in candidates {
            let mut live = lock_server(&server)?;
            let _ = self.process_supervisor.terminate(live.handle.process_session_id.as_str());
            live.handle.lifecycle = LspServerLifecycleV2::Stopped;
            live.handle.reason_code = "lsp.idle_reaped".to_owned();
            let handle = live.handle.clone();
            drop(live);
            self.remove_live_server(&key, handle.clone())?;
            reaped.push(handle);
        }
        Ok(reaped)
    }

    /// Evicts an exact server selected by resource pressure.
    ///
    /// # Errors
    /// Returns an error for unknown handles or failed cleanup persistence.
    pub fn evict(&self, handle_id: &str) -> Result<LspServerHandleV2, LspWorkspaceSupervisorError> {
        let (key, server) = self.server_for_handle(handle_id)?;
        let mut live = lock_server(&server)?;
        self.process_supervisor
            .terminate(live.handle.process_session_id.as_str())
            .map_err(map_process_error)?;
        live.handle.lifecycle = LspServerLifecycleV2::Evicted;
        live.handle.reason_code = "lsp.resource_pressure_evicted".to_owned();
        let handle = live.handle.clone();
        drop(live);
        self.remove_live_server(&key, handle.clone())?;
        Ok(handle)
    }

    /// Stops every process-backed server and persists terminal metadata.
    ///
    /// # Errors
    /// Returns the first cleanup or persistence failure after attempting each
    /// server that was active when shutdown began.
    pub fn shutdown(&self) -> Result<Vec<LspServerHandleV2>, LspWorkspaceSupervisorError> {
        let candidates = {
            let state = self.lock_state()?;
            state
                .servers
                .iter()
                .map(|(key, server)| (key.clone(), Arc::clone(server)))
                .collect::<Vec<_>>()
        };
        let mut stopped = Vec::with_capacity(candidates.len());
        let mut first_error = None;
        for (key, server) in candidates {
            let result = (|| {
                let mut live = lock_server(&server)?;
                self.process_supervisor
                    .terminate(live.handle.process_session_id.as_str())
                    .map_err(map_process_error)?;
                live.handle.lifecycle = LspServerLifecycleV2::Stopped;
                live.handle.reason_code = "lsp.daemon_shutdown".to_owned();
                let handle = live.handle.clone();
                drop(live);
                self.remove_live_server(&key, handle.clone())?;
                Ok(handle)
            })();
            match result {
                Ok(handle) => stopped.push(handle),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(stopped),
        }
    }

    /// Clears a broken-server circuit for an exact workspace and language.
    ///
    /// # Errors
    /// Returns an error for unsafe workspace state or metadata persistence failure.
    pub fn reset_broken(
        &self,
        workspace_root: &Path,
        language: LspLanguageV2,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        let workspace_root = canonical_workspace(workspace_root)?;
        let policy =
            self.policies.get(&language).ok_or(LspWorkspaceSupervisorError::ServerUnavailable)?;
        let key = LspServerKey {
            workspace_root_sha256: sha256_path(workspace_root.as_path()),
            language,
            toolchain_fingerprint: policy.toolchain_fingerprint.clone(),
        };
        let mut state = self.lock_state()?;
        remove_broken_for_key(&mut state.registry, &key);
        persist_state(&self.config, &mut state)
    }

    /// Returns bounded lifecycle and circuit-breaker diagnostics.
    ///
    /// # Errors
    /// Returns an error when in-memory state is unavailable.
    pub fn health(&self) -> Result<LspHealthSnapshotV2, LspWorkspaceSupervisorError> {
        let state = self.lock_state()?;
        let mut handles = state.registry.handles.clone();
        handles.sort_by(|left, right| left.handle_id.cmp(&right.handle_id));
        let mut broken_servers = state.registry.broken_servers.clone();
        broken_servers.sort_by(|left, right| {
            left.workspace_root_sha256
                .cmp(&right.workspace_root_sha256)
                .then(left.language.cmp(&right.language))
        });
        let active_servers = state.servers.len();
        Ok(LspHealthSnapshotV2 {
            handles,
            broken_servers,
            active_servers,
            reason_code: if active_servers == 0 {
                "lsp.no_active_servers".to_owned()
            } else {
                "lsp.active".to_owned()
            },
        })
    }

    /// Returns a closed operator projection without arbitrary server payloads or raw identities.
    ///
    /// # Errors
    /// Returns an error when in-memory state is unavailable.
    pub fn diagnostics_health(
        &self,
    ) -> Result<LspDiagnosticsSnapshotV2, LspWorkspaceSupervisorError> {
        let health = self.health()?;
        let handles = health
            .handles
            .into_iter()
            .map(|handle| LspDiagnosticsHandleV2 {
                schema_version: 2,
                handle_id_sha256: sha256_text(handle.handle_id.as_str()),
                workspace_root_sha256: handle.workspace_root_sha256,
                worktree_id_sha256: sha256_text(handle.worktree_id.as_str()),
                language: handle.language,
                toolchain_fingerprint_sha256: sha256_text(handle.toolchain_fingerprint.as_str()),
                process_session_id_sha256: sha256_text(handle.process_session_id.as_str()),
                generation: handle.generation,
                lifecycle: handle.lifecycle,
                capabilities_present: handle.capabilities.present,
                capabilities_bytes: handle.capabilities.serialized_bytes,
                restart_count: handle.restart_count,
                last_diagnostics_at_unix_ms: handle.last_diagnostics_at_unix_ms,
                last_used_at_unix_ms: handle.last_used_at_unix_ms,
                reason_code: handle.reason_code,
            })
            .collect();
        let broken_servers = health
            .broken_servers
            .into_iter()
            .map(|entry| LspDiagnosticsBrokenServerV2 {
                workspace_root_sha256: entry.workspace_root_sha256,
                language: entry.language,
                toolchain_fingerprint_sha256: sha256_text(entry.toolchain_fingerprint.as_str()),
                failure_count: entry.failure_count,
                retry_after_unix_ms: entry.retry_after_unix_ms,
                manual_reset_required: entry.manual_reset_required,
                reason_code: entry.reason_code,
            })
            .collect();
        Ok(LspDiagnosticsSnapshotV2 {
            schema_version: 2,
            handles,
            broken_servers,
            active_servers: health.active_servers,
            reason_code: health.reason_code,
        })
    }

    fn send_request_to_live(
        &self,
        live: &mut LiveLspServer,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<LspRequestOutcomeV2, LspWorkspaceSupervisorError> {
        let request_id = live.next_request_id;
        live.next_request_id = live.next_request_id.saturating_add(1);
        let frame = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        });
        self.write_frame(live, &frame, timeout)?;
        let started = Instant::now();
        let deadline = started + timeout;
        loop {
            for message in self.pump_output(live)? {
                if message.get("id").and_then(Value::as_u64) == Some(request_id) {
                    if message.get("error").is_some() {
                        return Err(LspWorkspaceSupervisorError::JsonRpc);
                    }
                    live.last_used = Instant::now();
                    live.handle.last_used_at_unix_ms = unix_time_ms();
                    return Ok(LspRequestOutcomeV2 {
                        handle_id: live.handle.handle_id.clone(),
                        server_generation: live.handle.generation,
                        request_id,
                        result: message.get("result").cloned().unwrap_or(Value::Null),
                        reason_code: "lsp.request_completed".to_owned(),
                        elapsed_ms: u64::try_from(started.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                    });
                }
                self.retain_notification(live, message);
            }
            let status = self
                .process_supervisor
                .status(live.handle.process_session_id.as_str())
                .map_err(map_process_error)?;
            if status.state.is_terminal() {
                return Err(LspWorkspaceSupervisorError::ServerCrashed);
            }
            if Instant::now() >= deadline {
                let _ = self.send_notification_to_live(
                    live,
                    "$/cancelRequest",
                    json!({"id": request_id}),
                );
                return Err(LspWorkspaceSupervisorError::RequestTimeout);
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn send_notification_to_live(
        &self,
        live: &mut LiveLspServer,
        method: &str,
        params: Value,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        let frame = json!({"jsonrpc": "2.0", "method": method, "params": params});
        self.write_frame(live, &frame, self.config.request_timeout)?;
        live.last_used = Instant::now();
        live.handle.last_used_at_unix_ms = unix_time_ms();
        Ok(())
    }

    fn write_frame(
        &self,
        live: &LiveLspServer,
        message: &Value,
        timeout: Duration,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        let body =
            serde_json::to_vec(message).map_err(|_| LspWorkspaceSupervisorError::MalformedFrame)?;
        if body.is_empty() || body.len() > self.config.max_message_bytes {
            return Err(LspWorkspaceSupervisorError::OversizedFrame);
        }
        let mut frame = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
        frame.extend_from_slice(body.as_slice());
        let deadline = Instant::now() + timeout;
        for chunk in frame.chunks(PROCESS_WRITE_BYTES) {
            loop {
                match self
                    .process_supervisor
                    .write(live.handle.process_session_id.as_str(), chunk.to_vec())
                {
                    Ok(()) => break,
                    Err(ProcessSupervisorError::StdinBusy) if Instant::now() < deadline => {
                        thread::sleep(POLL_INTERVAL);
                    }
                    Err(error) => return Err(map_process_error(error)),
                }
            }
        }
        Ok(())
    }

    fn pump_output(
        &self,
        live: &mut LiveLspServer,
    ) -> Result<Vec<Value>, LspWorkspaceSupervisorError> {
        loop {
            let page = self
                .process_supervisor
                .tail_raw(
                    live.handle.process_session_id.as_str(),
                    Some(live.output_cursor),
                    PROCESS_TAIL_CHUNKS,
                )
                .map_err(map_process_error)?;
            if page.cursor_reset || page.truncated {
                return Err(LspWorkspaceSupervisorError::OversizedFrame);
            }
            for chunk in page.chunks {
                match chunk.stream {
                    ProcessOutputStream::Stdout => {
                        live.stdout_buffer.extend_from_slice(chunk.bytes.as_slice());
                        if live.stdout_buffer.len()
                            > self
                                .config
                                .max_message_bytes
                                .saturating_add(self.config.max_header_bytes)
                        {
                            return Err(LspWorkspaceSupervisorError::OversizedFrame);
                        }
                    }
                    ProcessOutputStream::Stderr => {
                        let remaining = 4096usize.saturating_sub(live.stderr_preview.len());
                        let decoded = String::from_utf8_lossy(chunk.bytes.as_slice());
                        let redacted = redact_process_output_projection(decoded.as_ref()).0;
                        live.stderr_preview.extend(redacted.chars().take(remaining));
                    }
                }
            }
            live.output_cursor = page.last_returned_cursor;
            if !page.has_more {
                break;
            }
        }
        let mut messages = Vec::new();
        while let Some(message) = decode_frame(
            &mut live.stdout_buffer,
            self.config.max_header_bytes,
            self.config.max_message_bytes,
        )? {
            if message.get("method").is_some() && message.get("id").is_some() {
                self.handle_server_request(live, &message)?;
            } else if message.get("method").is_some() {
                self.retain_notification(live, message);
            } else {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    fn handle_server_request(
        &self,
        live: &LiveLspServer,
        message: &Value,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        let id = message.get("id").cloned().ok_or(LspWorkspaceSupervisorError::MalformedFrame)?;
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .ok_or(LspWorkspaceSupervisorError::MalformedFrame)?;
        let response = match method {
            "workspace/configuration" => {
                let item_count = message
                    .pointer("/params/items")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len)
                    .min(self.config.max_notifications);
                json!({"jsonrpc": "2.0", "id": id, "result": vec![Value::Null; item_count]})
            }
            "client/registerCapability"
            | "client/unregisterCapability"
            | "window/workDoneProgress/create"
            | "window/showMessageRequest" => {
                json!({"jsonrpc": "2.0", "id": id, "result": Value::Null})
            }
            "workspace/applyEdit" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "applied": false,
                    "failureReason": "workspace edits require the patch authority"
                }
            }),
            _ => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": "server request is not supported by host policy"
                }
            }),
        };
        self.write_frame(live, &response, self.config.request_timeout)
    }

    fn retain_notification(&self, live: &mut LiveLspServer, message: Value) {
        live.notifications.push_back(message);
        while live.notifications.len() > self.config.max_notifications {
            live.notifications.pop_front();
        }
    }

    fn server_for_key(
        &self,
        key: &LspServerKey,
    ) -> Result<Option<Arc<Mutex<LiveLspServer>>>, LspWorkspaceSupervisorError> {
        Ok(self.lock_state()?.servers.get(key).cloned())
    }

    fn server_for_handle(
        &self,
        handle_id: &str,
    ) -> Result<(LspServerKey, Arc<Mutex<LiveLspServer>>), LspWorkspaceSupervisorError> {
        validate_identity("handle_id", handle_id)?;
        let state = self.lock_state()?;
        for (key, server) in &state.servers {
            let live = lock_server(server)?;
            if live.handle.handle_id == handle_id {
                return Ok((key.clone(), Arc::clone(server)));
            }
        }
        Err(LspWorkspaceSupervisorError::HandleNotFound)
    }

    fn latest_handle_for_key(
        &self,
        key: &LspServerKey,
    ) -> Result<Option<LspServerHandleV2>, LspWorkspaceSupervisorError> {
        Ok(self
            .lock_state()?
            .registry
            .handles
            .iter()
            .filter(|handle| handle_matches_key(handle, key))
            .max_by_key(|handle| handle.generation)
            .cloned())
    }

    fn record_failure(
        &self,
        key: &LspServerKey,
        handle: &LspServerHandleV2,
        reason_code: &str,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        let mut state = self.lock_state()?;
        state.servers.remove(key);
        let previous_failures =
            broken_for_key(&state.registry, key).map_or(0, |entry| entry.failure_count);
        let failure_count = previous_failures.saturating_add(1);
        remove_broken_for_key(&mut state.registry, key);
        state.registry.broken_servers.push(LspBrokenServerEntryV2 {
            workspace_root_sha256: key.workspace_root_sha256.clone(),
            language: key.language,
            toolchain_fingerprint: key.toolchain_fingerprint.clone(),
            failure_count,
            retry_after_unix_ms: unix_time_ms()
                .saturating_add(duration_ms_i64(self.config.broken_ttl)),
            manual_reset_required: failure_count >= self.config.circuit_breaker_failures,
            reason_code: bounded_reason(reason_code),
        });
        let mut broken_handle = handle.clone();
        broken_handle.lifecycle = LspServerLifecycleV2::Broken;
        broken_handle.reason_code = bounded_reason(reason_code);
        upsert_handle(&mut state.registry, broken_handle);
        persist_state(&self.config, &mut state)
    }

    fn remove_live_server(
        &self,
        key: &LspServerKey,
        handle: LspServerHandleV2,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        let mut state = self.lock_state()?;
        state.servers.remove(key);
        upsert_handle(&mut state.registry, handle);
        persist_state(&self.config, &mut state)
    }

    fn persist_live_handle(
        &self,
        handle: LspServerHandleV2,
    ) -> Result<(), LspWorkspaceSupervisorError> {
        let mut state = self.lock_state()?;
        upsert_handle(&mut state.registry, handle);
        persist_state(&self.config, &mut state)
    }

    fn reap_expired_broken_cache(&self) -> Result<(), LspWorkspaceSupervisorError> {
        let now = unix_time_ms();
        let mut state = self.lock_state()?;
        let before = state.registry.broken_servers.len();
        state
            .registry
            .broken_servers
            .retain(|entry| entry.manual_reset_required || entry.retry_after_unix_ms > now);
        if state.registry.broken_servers.len() != before {
            persist_state(&self.config, &mut state)?;
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, SupervisorState>, LspWorkspaceSupervisorError> {
        self.state.lock().map_err(|_| LspWorkspaceSupervisorError::StateUnavailable)
    }
}

impl Drop for LspWorkspaceSupervisor {
    fn drop(&mut self) {
        let servers = self
            .state
            .lock()
            .map(|state| state.servers.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for server in servers {
            if let Ok(live) = server.lock() {
                let _ = self.process_supervisor.terminate(live.handle.process_session_id.as_str());
            }
        }
    }
}

fn decode_frame(
    buffer: &mut Vec<u8>,
    max_header_bytes: usize,
    max_message_bytes: usize,
) -> Result<Option<Value>, LspWorkspaceSupervisorError> {
    let Some(header_end) = find_subslice(buffer.as_slice(), b"\r\n\r\n") else {
        if buffer.len() > max_header_bytes {
            return Err(LspWorkspaceSupervisorError::OversizedFrame);
        }
        return Ok(None);
    };
    if header_end > max_header_bytes {
        return Err(LspWorkspaceSupervisorError::OversizedFrame);
    }
    let header = std::str::from_utf8(&buffer[..header_end])
        .map_err(|_| LspWorkspaceSupervisorError::MalformedFrame)?;
    let mut content_length = None;
    for line in header.split("\r\n") {
        let Some((name, value)) = line.split_once(':') else {
            return Err(LspWorkspaceSupervisorError::MalformedFrame);
        };
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(LspWorkspaceSupervisorError::MalformedFrame);
            }
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| LspWorkspaceSupervisorError::MalformedFrame)?,
            );
        }
    }
    let content_length = content_length.ok_or(LspWorkspaceSupervisorError::MalformedFrame)?;
    if content_length == 0 || content_length > max_message_bytes {
        return Err(LspWorkspaceSupervisorError::OversizedFrame);
    }
    let body_start = header_end.saturating_add(4);
    let frame_end = body_start
        .checked_add(content_length)
        .ok_or(LspWorkspaceSupervisorError::OversizedFrame)?;
    if buffer.len() < frame_end {
        return Ok(None);
    }
    let message = serde_json::from_slice::<Value>(&buffer[body_start..frame_end])
        .map_err(|_| LspWorkspaceSupervisorError::MalformedFrame)?;
    buffer.drain(..frame_end);
    Ok(Some(message))
}

fn validate_config(
    config: &LspWorkspaceSupervisorConfig,
) -> Result<(), LspWorkspaceSupervisorError> {
    if !config.registry_path.is_absolute()
        || config.max_servers == 0
        || config.max_registry_entries < config.max_servers
        || config.max_header_bytes < 32
        || config.max_message_bytes == 0
        || config.max_notifications == 0
        || config.initialize_timeout.is_zero()
        || config.request_timeout.is_zero()
        || config.server_lifetime <= config.initialize_timeout
        || config.idle_ttl.is_zero()
        || config.broken_ttl.is_zero()
        || config.circuit_breaker_failures == 0
        || config.resource_units.processes == 0
        || config.resource_units.is_zero()
        || config.policies.is_empty()
    {
        return Err(LspWorkspaceSupervisorError::InvalidConfiguration);
    }
    let mut languages = BTreeMap::new();
    for policy in &config.policies {
        if !policy.executable.is_absolute()
            || !policy.executable.is_file()
            || policy.toolchain_fingerprint.trim().is_empty()
            || policy.toolchain_fingerprint.len() > MAX_IDENTITY_BYTES
            || languages.insert(policy.language, ()).is_some()
        {
            return Err(LspWorkspaceSupervisorError::InvalidConfiguration);
        }
    }
    Ok(())
}

fn canonical_workspace(path: &Path) -> Result<PathBuf, LspWorkspaceSupervisorError> {
    if !path.is_absolute() || !path.is_dir() {
        return Err(LspWorkspaceSupervisorError::InvalidRequest(
            "workspace root must be an existing absolute directory".to_owned(),
        ));
    }
    reject_link(path)?;
    path.canonicalize().map_err(|error| {
        LspWorkspaceSupervisorError::InvalidRequest(format!(
            "workspace canonicalization failed: {error}"
        ))
    })
}

fn reject_link(path: &Path) -> Result<(), LspWorkspaceSupervisorError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        LspWorkspaceSupervisorError::InvalidRequest(format!("workspace metadata failed: {error}"))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(LspWorkspaceSupervisorError::InvalidRequest(
            "workspace root cannot be a symbolic link".to_owned(),
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(LspWorkspaceSupervisorError::InvalidRequest(
                "workspace root cannot be a reparse point".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_identity(field: &str, value: &str) -> Result<(), LspWorkspaceSupervisorError> {
    if value.trim().is_empty()
        || value.len() > MAX_IDENTITY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(LspWorkspaceSupervisorError::InvalidRequest(format!(
            "{field} must be non-empty, bounded, and free of control characters"
        )));
    }
    Ok(())
}

fn validate_method(method: &str) -> Result<(), LspWorkspaceSupervisorError> {
    if method.trim().is_empty()
        || method.len() > MAX_METHOD_BYTES
        || method.chars().any(char::is_control)
    {
        return Err(LspWorkspaceSupervisorError::InvalidRequest(
            "JSON-RPC method is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_document_uri(uri: &str) -> Result<(), LspWorkspaceSupervisorError> {
    if !uri.starts_with("file://") || uri.len() > MAX_URI_BYTES || uri.chars().any(char::is_control)
    {
        return Err(LspWorkspaceSupervisorError::InvalidRequest(
            "document URI must be a bounded file URI without control characters".to_owned(),
        ));
    }
    Ok(())
}

fn lock_server(
    server: &Arc<Mutex<LiveLspServer>>,
) -> Result<MutexGuard<'_, LiveLspServer>, LspWorkspaceSupervisorError> {
    server.lock().map_err(|_| LspWorkspaceSupervisorError::StateUnavailable)
}

fn broken_for_key<'a>(
    registry: &'a LspRegistryV2,
    key: &LspServerKey,
) -> Option<&'a LspBrokenServerEntryV2> {
    registry.broken_servers.iter().find(|entry| {
        entry.workspace_root_sha256 == key.workspace_root_sha256
            && entry.language == key.language
            && entry.toolchain_fingerprint == key.toolchain_fingerprint
    })
}

fn remove_broken_for_key(registry: &mut LspRegistryV2, key: &LspServerKey) {
    registry.broken_servers.retain(|entry| {
        entry.workspace_root_sha256 != key.workspace_root_sha256
            || entry.language != key.language
            || entry.toolchain_fingerprint != key.toolchain_fingerprint
    });
}

fn handle_matches_key(handle: &LspServerHandleV2, key: &LspServerKey) -> bool {
    handle.workspace_root_sha256 == key.workspace_root_sha256
        && handle.language == key.language
        && handle.toolchain_fingerprint == key.toolchain_fingerprint
}

fn upsert_handle(registry: &mut LspRegistryV2, handle: LspServerHandleV2) {
    if let Some(existing) =
        registry.handles.iter_mut().find(|existing| existing.handle_id == handle.handle_id)
    {
        *existing = handle;
    } else {
        registry.handles.push(handle);
    }
}

fn persist_state(
    config: &LspWorkspaceSupervisorConfig,
    state: &mut SupervisorState,
) -> Result<(), LspWorkspaceSupervisorError> {
    let active_handle_ids = state
        .servers
        .values()
        .filter_map(|server| server.lock().ok().map(|live| live.handle.handle_id.clone()))
        .collect::<BTreeSet<_>>();
    state.registry.handles.sort_by(|left, right| {
        active_handle_ids
            .contains(&right.handle_id)
            .cmp(&active_handle_ids.contains(&left.handle_id))
            .then(right.last_used_at_unix_ms.cmp(&left.last_used_at_unix_ms))
    });
    state.registry.handles.truncate(config.max_registry_entries);
    state.registry.broken_servers.sort_by(|left, right| {
        right
            .manual_reset_required
            .cmp(&left.manual_reset_required)
            .then(right.retry_after_unix_ms.cmp(&left.retry_after_unix_ms))
    });
    state.registry.broken_servers.truncate(config.max_registry_entries);
    state.registry.updated_at_unix_ms = unix_time_ms();
    write_registry(config.registry_path.as_path(), &state.registry)
}

fn read_registry(path: &Path) -> Result<LspRegistryV2, LspWorkspaceSupervisorError> {
    let bytes = fs::read(path)
        .map_err(|error| LspWorkspaceSupervisorError::Persistence(error.to_string()))?;
    let registry = serde_json::from_slice::<LspRegistryV2>(&bytes)
        .map_err(|error| LspWorkspaceSupervisorError::Persistence(error.to_string()))?;
    if registry.schema_version != LSP_REGISTRY_SCHEMA_VERSION {
        return Err(LspWorkspaceSupervisorError::Persistence(
            "unsupported LSP registry schema".to_owned(),
        ));
    }
    Ok(registry)
}

fn write_registry(
    path: &Path,
    registry: &LspRegistryV2,
) -> Result<(), LspWorkspaceSupervisorError> {
    let payload = serde_json::to_vec_pretty(registry)
        .map_err(|error| LspWorkspaceSupervisorError::Persistence(error.to_string()))?;
    let mut temporary = path.as_os_str().to_os_string();
    temporary.push(format!(".tmp.{}", ulid::Ulid::new()));
    let temporary = PathBuf::from(temporary);
    fs::write(temporary.as_path(), payload)
        .map_err(|error| LspWorkspaceSupervisorError::Persistence(error.to_string()))?;
    harden_file(temporary.as_path())?;
    if let Err(rename_error) = fs::rename(temporary.as_path(), path) {
        if path.is_file() {
            let mut swap = path.as_os_str().to_os_string();
            swap.push(format!(".swap.{}", ulid::Ulid::new()));
            let swap = PathBuf::from(swap);
            fs::rename(path, swap.as_path())
                .map_err(|error| LspWorkspaceSupervisorError::Persistence(error.to_string()))?;
            if let Err(error) = fs::rename(temporary.as_path(), path) {
                let _ = fs::rename(swap.as_path(), path);
                let _ = fs::remove_file(temporary);
                return Err(LspWorkspaceSupervisorError::Persistence(error.to_string()));
            }
            let _ = fs::remove_file(swap);
        } else {
            let _ = fs::remove_file(temporary);
            return Err(LspWorkspaceSupervisorError::Persistence(rename_error.to_string()));
        }
    }
    harden_file(path)
}

fn create_private_dir(path: &Path) -> Result<(), LspWorkspaceSupervisorError> {
    fs::create_dir_all(path)
        .map_err(|error| LspWorkspaceSupervisorError::Persistence(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| LspWorkspaceSupervisorError::Persistence(error.to_string()))?;
    }
    Ok(())
}

fn harden_file(_path: &Path) -> Result<(), LspWorkspaceSupervisorError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(_path, fs::Permissions::from_mode(0o600))
            .map_err(|error| LspWorkspaceSupervisorError::Persistence(error.to_string()))?;
    }
    Ok(())
}

fn sha256_path(path: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_os_str().to_string_lossy().as_bytes());
    hex::encode(hasher.finalize())
}

fn sha256_text(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn summarize_capabilities(capabilities: &Value) -> LspCapabilitySummaryV2 {
    let serialized_bytes = serde_json::to_vec(capabilities)
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or_default();
    LspCapabilitySummaryV2 {
        present: !capabilities.is_null(),
        serialized_bytes,
        text_document_sync: capabilities
            .get("textDocumentSync")
            .is_some_and(|value| !value.is_null()),
        diagnostic_provider: capabilities
            .get("diagnosticProvider")
            .is_some_and(|value| !matches!(value, Value::Null | Value::Bool(false))),
        workspace_folders: capabilities
            .pointer("/workspace/workspaceFolders/supported")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

pub(crate) fn path_to_file_uri(path: &Path) -> String {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let normalized = normalized.strip_prefix("//?/").unwrap_or(normalized.as_str());
    let encoded = percent_encode_uri_path(normalized);
    if encoded.starts_with('/') {
        format!("file://{encoded}")
    } else {
        format!("file:///{encoded}")
    }
}

fn percent_encode_uri_path(path: &str) -> String {
    let mut encoded = String::with_capacity(path.len());
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/' | b':') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

fn error_reason_code(error: &LspWorkspaceSupervisorError) -> &'static str {
    match error {
        LspWorkspaceSupervisorError::RequestTimeout => "lsp.request_timeout",
        LspWorkspaceSupervisorError::MalformedFrame => "lsp.malformed_frame",
        LspWorkspaceSupervisorError::OversizedFrame => "lsp.oversized_frame",
        LspWorkspaceSupervisorError::ServerCrashed => "lsp.server_crashed",
        LspWorkspaceSupervisorError::JsonRpc => "lsp.json_rpc_error",
        _ => "lsp.supervisor_failure",
    }
}

fn map_process_error(error: ProcessSupervisorError) -> LspWorkspaceSupervisorError {
    match error {
        ProcessSupervisorError::SessionNotFound | ProcessSupervisorError::SessionNotWritable => {
            LspWorkspaceSupervisorError::ServerCrashed
        }
        other => LspWorkspaceSupervisorError::Process(other.to_string()),
    }
}

fn bounded_reason(reason: &str) -> String {
    reason.chars().take(MAX_IDENTITY_BYTES).collect()
}

fn duration_ms_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn unix_time_ms() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoder_handles_split_frames_and_multiple_messages() {
        let first = br#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        let second = br#"{"jsonrpc":"2.0","method":"notice"}"#;
        let mut buffer = format!("Content-Length: {}\r\n\r\n", first.len()).into_bytes();
        buffer.extend_from_slice(first);
        buffer.extend_from_slice(format!("Content-Length: {}\r\n\r\n", second.len()).as_bytes());
        buffer.extend_from_slice(second);
        assert_eq!(
            decode_frame(&mut buffer, 1024, 1024)
                .expect("first frame")
                .and_then(|value| value.get("id").and_then(Value::as_u64)),
            Some(1)
        );
        assert_eq!(
            decode_frame(&mut buffer, 1024, 1024)
                .expect("second frame")
                .and_then(|value| value.get("method").and_then(Value::as_str).map(str::to_owned)),
            Some("notice".to_owned())
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn decoder_rejects_duplicate_length_and_oversized_body_before_allocation() {
        let mut duplicate = b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}".to_vec();
        assert!(matches!(
            decode_frame(&mut duplicate, 1024, 1024),
            Err(LspWorkspaceSupervisorError::MalformedFrame)
        ));
        let mut oversized = b"Content-Length: 999999999\r\n\r\n".to_vec();
        assert!(matches!(
            decode_frame(&mut oversized, 1024, 1024),
            Err(LspWorkspaceSupervisorError::OversizedFrame)
        ));
    }

    #[test]
    fn registry_rejects_unknown_versions_and_fields_without_rewrite() {
        let mutators: [fn(&mut Value); 2] = [
            |value| value["schema_version"] = json!(999),
            |value| value["unknown_registry_field"] = json!(true),
        ];
        for mutate in mutators {
            let temp = tempfile::tempdir().expect("temp dir");
            let path = temp.path().join("lsp-registry.json");
            let mut value = serde_json::to_value(LspRegistryV2 {
                schema_version: LSP_REGISTRY_SCHEMA_VERSION,
                handles: Vec::new(),
                broken_servers: Vec::new(),
                updated_at_unix_ms: unix_time_ms(),
            })
            .expect("encode registry value");
            mutate(&mut value);
            let bytes = serde_json::to_vec_pretty(&value).expect("encode invalid registry");
            fs::write(path.as_path(), bytes.as_slice()).expect("write invalid registry");

            assert!(matches!(
                read_registry(path.as_path()),
                Err(LspWorkspaceSupervisorError::Persistence(_))
            ));
            assert_eq!(fs::read(path.as_path()).expect("read unchanged registry"), bytes);
        }
    }
}
