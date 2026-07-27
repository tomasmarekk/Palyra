//! Live ACP process ownership, session queues, and restart-safe bindings.
//!
//! Only operator-validated descriptors can launch a child. Durable state keeps
//! routing metadata and hashes, never command payloads or raw process leases.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use palyra_common::{
    runtime_contracts::{AcpSessionBindingRecord, StableErrorEnvelope},
    versioned_json::{parse_versioned_json, VersionedJsonFormat},
};
use palyra_vault::ensure_owner_only_file;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex as AsyncMutex, Notify};

use super::{
    session_actor_queue::{
        AcpPreparedTurn, AcpSessionActorQueue, AcpSessionActorQueuePolicy, AcpTurnQueueDecisionKind,
    },
    AcpRuntimeError, AcpRuntimeResult,
};
use crate::{
    application::managed_runtime::{
        ManagedRuntimeDescriptor, ManagedRuntimeHealthState, ManagedRuntimeStartRequest,
        RuntimeTransport, RuntimeTransportCommand, RuntimeTransportError, RuntimeTransportEvent,
        StdioRuntimeTransport,
    },
    config::{AcpRuntimeBackendConfig, AcpRuntimeConfig},
    sha256_hex, unix_ms_now,
};

const ACP_LIVE_BINDINGS_SCHEMA_VERSION: u32 = 2;
const ACP_LIVE_BINDINGS_FILE_NAME: &str = "live-runtime-bindings.v2.json";
const ACP_LIVE_BINDINGS_FORMAT: VersionedJsonFormat =
    VersionedJsonFormat::new("ACP live runtime bindings", ACP_LIVE_BINDINGS_SCHEMA_VERSION);
const MAX_LIFECYCLES: usize = 1_024;

/// Restart-safe ACP session-to-runtime binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpRuntimeBindingV2 {
    pub schema_version: u32,
    pub session_binding_id: String,
    pub palyra_session_id_sha256: String,
    pub backend_id: String,
    pub generation: u64,
    pub resume_metadata: AcpResumeMetadataV2,
}

/// Payload-free information an ACP runtime may use to resume safely.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpResumeMetadataV2 {
    pub last_acknowledged_sequence: u64,
    pub visible_output_committed: bool,
}

/// Manager-owned lifecycle state for one ACP runtime command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpCommandState {
    Queued,
    Dispatching,
    Streaming,
    Completed,
    Cancelled,
    TimedOut,
    Failed,
    StaleSuppressed,
}

/// Redaction-safe command diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpCommandLifecycle {
    pub command_id_sha256: String,
    pub session_binding_id: String,
    pub backend_id: String,
    pub generation: u64,
    pub state: AcpCommandState,
    pub reason_code: String,
    pub queued_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub visible_output_committed: bool,
}

/// Payload-free health for one configured ACP process backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpHandleHealth {
    pub backend_id: String,
    pub configured: bool,
    pub enabled: bool,
    pub state: String,
    pub generation: u64,
    pub attached_sessions: usize,
    pub active_commands: usize,
    pub pending_commands: usize,
    pub process_lease_sha256: Option<String>,
    pub reason_code: String,
}

/// Successful terminal response from a live ACP backend.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpLiveCommandResult {
    pub backend_id: String,
    pub generation: u64,
    pub outcome: String,
    pub terminal: Value,
}

/// Fail-closed errors from the live process manager.
#[derive(Debug, Error)]
pub(crate) enum AcpLiveRuntimeError {
    #[error("live ACP runtime rollout is disabled")]
    RolloutDisabled,
    #[error("ACP session did not explicitly select a live runtime backend")]
    BackendNotSelected,
    #[error("ACP runtime backend '{0}' is not configured and enabled")]
    BackendUnavailable(String),
    #[error("ACP runtime command queue is full")]
    Backpressure,
    #[error("ACP runtime command '{0}' was not found")]
    CommandNotFound(String),
    #[error("ACP runtime failed before producing visible output")]
    RuntimeUnavailable,
    #[error("ACP runtime failed after visible output; failover is unsafe")]
    FailoverAfterOutput,
    #[error("ACP runtime command timed out")]
    TimedOut,
    #[error("ACP runtime state failed: {0}")]
    State(String),
}

impl AcpLiveRuntimeError {
    /// Converts the manager failure into a stable console error envelope.
    pub(crate) fn to_stable_error(&self) -> StableErrorEnvelope {
        let (code, recovery) = match self {
            Self::RolloutDisabled => (
                "acp/runtime_rollout_disabled",
                "Enable the ACP runtime rollout before selecting a live backend.",
            ),
            Self::BackendNotSelected => (
                "acp/runtime_backend_not_selected",
                "Set config.runtime_backend to an operator-configured backend id.",
            ),
            Self::BackendUnavailable(_) => (
                "acp/runtime_backend_unavailable",
                "Select an enabled operator-configured ACP runtime backend.",
            ),
            Self::Backpressure => {
                ("acp/runtime_backpressure", "Wait for the active ACP command to finish and retry.")
            }
            Self::CommandNotFound(_) => (
                "acp/runtime_command_not_found",
                "Refresh the ACP run state and retry with a current command id.",
            ),
            Self::RuntimeUnavailable => (
                "acp/runtime_unavailable",
                "Inspect backend health and retry; only trusted configured fallbacks are eligible.",
            ),
            Self::FailoverAfterOutput => (
                "acp/runtime_failover_unsafe",
                "Start a new turn; automatic failover is blocked after visible output.",
            ),
            Self::TimedOut => (
                "acp/runtime_timeout",
                "Retry after checking backend health and command timeout configuration.",
            ),
            Self::State(_) => (
                "acp/runtime_state_error",
                "Inspect ACP runtime state permissions and restart the daemon if needed.",
            ),
        };
        StableErrorEnvelope::new(code, self.to_string(), recovery)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableBindingsV2 {
    schema_version: u32,
    updated_at_unix_ms: i64,
    bindings: BTreeMap<String, AcpRuntimeBindingV2>,
}

impl Default for DurableBindingsV2 {
    fn default() -> Self {
        Self {
            schema_version: ACP_LIVE_BINDINGS_SCHEMA_VERSION,
            updated_at_unix_ms: 0,
            bindings: BTreeMap::new(),
        }
    }
}

struct LiveHandle {
    transport: Arc<dyn RuntimeTransport>,
    generation: u64,
    attached_sessions: BTreeSet<String>,
    active_commands: BTreeSet<String>,
    last_used_at_unix_ms: i64,
    process_lease_sha256: String,
}

#[derive(Default)]
struct ManagerState {
    durable: DurableBindingsV2,
    handles: BTreeMap<String, LiveHandle>,
    lifecycles: BTreeMap<String, AcpCommandLifecycle>,
}

struct LifecycleUpdate<'a> {
    command_id: &'a str,
    session_binding_id: &'a str,
    backend_id: &'a str,
    generation: u64,
    state: AcpCommandState,
    reason_code: &'a str,
    visible_output: bool,
}

#[derive(Default)]
struct SessionQueueSlot {
    queue: AcpSessionActorQueue,
    notify: Arc<Notify>,
}

type TransportFactory = Arc<
    dyn Fn(ManagedRuntimeDescriptor) -> Result<Arc<dyn RuntimeTransport>, RuntimeTransportError>
        + Send
        + Sync,
>;

/// Owns live ACP processes and serializes mutating commands per ACP session.
pub(crate) struct AcpLiveRuntimeManager {
    rollout_enabled: bool,
    config: AcpRuntimeConfig,
    root: PathBuf,
    bindings_path: PathBuf,
    state: AsyncMutex<ManagerState>,
    queues: Mutex<BTreeMap<String, SessionQueueSlot>>,
    transport_factory: TransportFactory,
}

impl std::fmt::Debug for AcpLiveRuntimeManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcpLiveRuntimeManager")
            .field("rollout_enabled", &self.rollout_enabled)
            .field("configured_backends", &self.config.backends.len())
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl AcpLiveRuntimeManager {
    /// Opens restart-safe ACP manager state without restoring any raw process handle.
    ///
    /// # Errors
    /// Fails when the durable metadata file cannot be parsed or permission-hardened.
    pub(crate) fn open(
        root: &Path,
        rollout_enabled: bool,
        config: AcpRuntimeConfig,
    ) -> AcpRuntimeResult<Self> {
        let transport_factory: TransportFactory = Arc::new(|descriptor| {
            StdioRuntimeTransport::new(descriptor)
                .map(|transport| Arc::new(transport) as Arc<dyn RuntimeTransport>)
        });
        Self::open_with_factory(root, rollout_enabled, config, transport_factory)
    }

    fn open_with_factory(
        root: &Path,
        rollout_enabled: bool,
        config: AcpRuntimeConfig,
        transport_factory: TransportFactory,
    ) -> AcpRuntimeResult<Self> {
        let bindings_path = root.join(ACP_LIVE_BINDINGS_FILE_NAME);
        let durable = load_durable_bindings(bindings_path.as_path())?;
        Ok(Self {
            rollout_enabled,
            config,
            root: root.to_path_buf(),
            bindings_path,
            state: AsyncMutex::new(ManagerState {
                durable,
                handles: BTreeMap::new(),
                lifecycles: BTreeMap::new(),
            }),
            queues: Mutex::new(BTreeMap::new()),
            transport_factory,
        })
    }

    /// Returns the trusted backend id selected by session configuration.
    ///
    /// An absent selection preserves the built-in Palyra command path. An
    /// invalid selection is not silently replaced with a default backend.
    pub(crate) fn selected_backend(
        &self,
        binding: &AcpSessionBindingRecord,
    ) -> Result<Option<String>, AcpLiveRuntimeError> {
        let Some(raw) = binding.config.get("runtime_backend") else {
            return Ok(None);
        };
        let Some(backend_id) = raw.as_str() else {
            return Err(AcpLiveRuntimeError::BackendUnavailable("<invalid>".to_owned()));
        };
        self.require_backend(backend_id)?;
        Ok(Some(backend_id.to_owned()))
    }

    /// Executes one generation-pinned command, using trusted failover only
    /// before any visible output has been emitted.
    pub(crate) async fn execute(
        &self,
        binding: &AcpSessionBindingRecord,
        command_id: &str,
        method: &str,
        payload: Value,
    ) -> Result<AcpLiveCommandResult, AcpLiveRuntimeError> {
        self.evict_idle().await?;
        let primary =
            self.selected_backend(binding)?.ok_or(AcpLiveRuntimeError::BackendNotSelected)?;
        if self.interrupted_after_visible_output(&binding.binding_id).await {
            return Err(AcpLiveRuntimeError::FailoverAfterOutput);
        }
        self.wait_for_turn(binding, command_id, primary.as_str()).await?;
        let mut last_error = AcpLiveRuntimeError::RuntimeUnavailable;
        for backend_id in self.candidate_chain(primary.as_str())? {
            match self
                .execute_attempt(binding, command_id, method, payload.clone(), &backend_id)
                .await
            {
                Ok(result) => {
                    self.finish_turn(&binding.binding_id, command_id, false)?;
                    return Ok(result);
                }
                Err(AcpLiveRuntimeError::FailoverAfterOutput) => {
                    self.finish_turn(&binding.binding_id, command_id, false)?;
                    return Err(AcpLiveRuntimeError::FailoverAfterOutput);
                }
                Err(error) => {
                    last_error = error;
                }
            }
        }
        self.finish_turn(&binding.binding_id, command_id, false)?;
        Err(last_error)
    }

    /// Cancels an active or queued session command.
    ///
    /// The durable generation is invalidated and saved before the session
    /// queue promotes its successor.
    pub(crate) async fn cancel(
        &self,
        binding: &AcpSessionBindingRecord,
        command_id: &str,
    ) -> Result<Value, AcpLiveRuntimeError> {
        let backend_id =
            self.selected_backend(binding)?.ok_or(AcpLiveRuntimeError::BackendNotSelected)?;
        let active = self.queue_contains_active(&binding.binding_id, command_id)?;
        let queued = self.queue_contains(&binding.binding_id, command_id)?;
        let interrupted = self.interrupted_after_visible_output(&binding.binding_id).await;
        if active || interrupted {
            self.invalidate_backend(
                backend_id.as_str(),
                command_id,
                "acp_runtime.cancelled",
                AcpCommandState::Cancelled,
            )
            .await?;
        }
        if queued {
            self.finish_turn(&binding.binding_id, command_id, true)?;
        }
        if !queued && !interrupted {
            let found = {
                let state = self.state.lock().await;
                state.lifecycles.contains_key(command_id)
            };
            if !found {
                return Err(AcpLiveRuntimeError::CommandNotFound(command_id.to_owned()));
            }
        }
        Ok(json!({
            "command_id": command_id,
            "cancelled": true,
            "generation_invalidated": active || interrupted,
        }))
    }

    /// Returns one payload-free lifecycle snapshot.
    pub(crate) async fn lifecycle(&self, command_id: &str) -> Option<AcpCommandLifecycle> {
        self.state.lock().await.lifecycles.get(command_id).cloned()
    }

    /// Returns payload-free health for configured handles and session queues.
    pub(crate) async fn health(&self) -> Vec<AcpHandleHealth> {
        let (active_commands, pending_commands) = self.queue_counts();
        let state = self.state.lock().await;
        self.config
            .backends
            .iter()
            .map(|backend| {
                let handle = state.handles.get(backend.id.as_str());
                let runtime_health = handle.map(|value| value.transport.health());
                AcpHandleHealth {
                    backend_id: backend.id.clone(),
                    configured: true,
                    enabled: backend.enabled && self.rollout_enabled,
                    state: runtime_health
                        .as_ref()
                        .map(|health| health_state_name(health.state))
                        .unwrap_or("closed")
                        .to_owned(),
                    generation: handle.map_or(0, |value| value.generation),
                    attached_sessions: handle.map_or(0, |value| value.attached_sessions.len()),
                    active_commands,
                    pending_commands,
                    process_lease_sha256: handle.map(|value| value.process_lease_sha256.clone()),
                    reason_code: runtime_health
                        .map(|health| health.last_reason_code)
                        .unwrap_or_else(|| "acp_runtime.not_started".to_owned()),
                }
            })
            .collect()
    }

    /// Closes processes whose trusted idle TTL has expired.
    pub(crate) async fn evict_idle(&self) -> Result<usize, AcpLiveRuntimeError> {
        let now = unix_ms_now().map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?;
        let idle_ttl = i64::try_from(self.config.idle_ttl_ms).unwrap_or(i64::MAX);
        let stale = {
            let state = self.state.lock().await;
            state
                .handles
                .iter()
                .filter(|(_, handle)| {
                    handle.active_commands.is_empty()
                        && now.saturating_sub(handle.last_used_at_unix_ms) >= idle_ttl
                })
                .map(|(backend_id, _)| backend_id.clone())
                .collect::<Vec<_>>()
        };
        for backend_id in &stale {
            self.invalidate_backend(
                backend_id,
                "",
                "acp_runtime.idle_evicted",
                AcpCommandState::StaleSuppressed,
            )
            .await?;
        }
        Ok(stale.len())
    }

    async fn execute_attempt(
        &self,
        binding: &AcpSessionBindingRecord,
        command_id: &str,
        method: &str,
        payload: Value,
        backend_id: &str,
    ) -> Result<AcpLiveCommandResult, AcpLiveRuntimeError> {
        let (transport, generation, timeout) = self.ensure_handle(binding, backend_id).await?;
        let mut events =
            transport.event_stream().map_err(|_| AcpLiveRuntimeError::RuntimeUnavailable)?;
        let now = unix_ms_now().map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?;
        self.update_lifecycle(LifecycleUpdate {
            command_id,
            session_binding_id: binding.binding_id.as_str(),
            backend_id,
            generation,
            state: AcpCommandState::Dispatching,
            reason_code: "acp_runtime.dispatching",
            visible_output: false,
        })
        .await;
        if transport
            .send_command(RuntimeTransportCommand {
                command_id: command_id.to_owned(),
                generation,
                method: method.to_owned(),
                payload,
                deadline_unix_ms: now
                    .saturating_add(i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX)),
            })
            .await
            .is_err()
        {
            self.invalidate_backend(
                backend_id,
                command_id,
                "acp_runtime.process_failed",
                AcpCommandState::Failed,
            )
            .await?;
            return Err(AcpLiveRuntimeError::RuntimeUnavailable);
        }
        {
            let mut state = self.state.lock().await;
            if let Some(handle) = state.handles.get_mut(backend_id) {
                handle.active_commands.insert(command_id.to_owned());
                handle.last_used_at_unix_ms = now;
            }
        }
        let observed = tokio::time::timeout(
            timeout,
            self.wait_for_terminal(&mut events, binding, command_id, backend_id, generation),
        )
        .await;
        match observed {
            Ok(result) => result,
            Err(_) => {
                let visible_output = self
                    .state
                    .lock()
                    .await
                    .lifecycles
                    .get(command_id)
                    .is_some_and(|lifecycle| lifecycle.visible_output_committed);
                self.invalidate_backend(
                    backend_id,
                    command_id,
                    "acp_runtime.timed_out",
                    AcpCommandState::TimedOut,
                )
                .await?;
                Err(if visible_output {
                    AcpLiveRuntimeError::FailoverAfterOutput
                } else {
                    AcpLiveRuntimeError::TimedOut
                })
            }
        }
    }

    async fn wait_for_terminal(
        &self,
        events: &mut broadcast::Receiver<RuntimeTransportEvent>,
        binding: &AcpSessionBindingRecord,
        command_id: &str,
        backend_id: &str,
        generation: u64,
    ) -> Result<AcpLiveCommandResult, AcpLiveRuntimeError> {
        let mut visible_output = false;
        loop {
            let event = events.recv().await.map_err(|_| AcpLiveRuntimeError::RuntimeUnavailable)?;
            match event {
                RuntimeTransportEvent::Accepted {
                    command_id: observed,
                    generation: observed_generation,
                    ..
                } if observed == command_id && observed_generation == generation => {}
                RuntimeTransportEvent::Event {
                    command_id: observed,
                    generation: observed_generation,
                    sequence,
                    method,
                    ..
                } if observed == command_id && observed_generation == generation => {
                    visible_output |= is_visible_event(method.as_str());
                    self.record_progress(
                        binding,
                        command_id,
                        backend_id,
                        generation,
                        sequence,
                        visible_output,
                    )
                    .await?;
                }
                RuntimeTransportEvent::Terminal {
                    command_id: observed,
                    generation: observed_generation,
                    sequence,
                    outcome,
                    payload,
                } if observed == command_id && observed_generation == generation => {
                    self.record_terminal(
                        binding,
                        command_id,
                        backend_id,
                        generation,
                        sequence,
                        visible_output,
                    )
                    .await?;
                    return Ok(AcpLiveCommandResult {
                        backend_id: backend_id.to_owned(),
                        generation,
                        outcome,
                        terminal: payload,
                    });
                }
                RuntimeTransportEvent::ChildExited { generation: observed, .. }
                | RuntimeTransportEvent::ProtocolError { generation: observed, .. }
                    if observed == generation =>
                {
                    self.invalidate_backend(
                        backend_id,
                        command_id,
                        "acp_runtime.process_failed",
                        AcpCommandState::Failed,
                    )
                    .await?;
                    return Err(if visible_output {
                        AcpLiveRuntimeError::FailoverAfterOutput
                    } else {
                        AcpLiveRuntimeError::RuntimeUnavailable
                    });
                }
                _ => {}
            }
        }
    }

    async fn ensure_handle(
        &self,
        binding: &AcpSessionBindingRecord,
        backend_id: &str,
    ) -> Result<(Arc<dyn RuntimeTransport>, u64, Duration), AcpLiveRuntimeError> {
        let backend = self.require_backend(backend_id)?.clone();
        let now = unix_ms_now().map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?;
        let mut state = self.state.lock().await;
        let ready = state.handles.get(backend_id).and_then(|handle| {
            (handle.transport.health().state == ManagedRuntimeHealthState::Ready)
                .then(|| (Arc::clone(&handle.transport), handle.generation))
        });
        if let Some((transport, generation)) = ready {
            if let Some(handle) = state.handles.get_mut(backend_id) {
                handle.attached_sessions.insert(binding.binding_id.clone());
                handle.last_used_at_unix_ms = now;
            }
            let resume_metadata = state
                .durable
                .bindings
                .get(&binding.binding_id)
                .map(|saved| saved.resume_metadata.clone())
                .unwrap_or_default();
            state.durable.bindings.insert(
                binding.binding_id.clone(),
                durable_binding(binding, backend_id, generation, resume_metadata),
            );
            self.save_locked(&mut state).await?;
            return Ok((transport, generation, Duration::from_millis(backend.command_timeout_ms)));
        }
        if let Some(stale) = state.handles.remove(backend_id) {
            let _ = stale.transport.close().await;
        }
        let generation = next_generation(&state, backend_id)?;
        let resume_state = state
            .durable
            .bindings
            .get(&binding.binding_id)
            .map(|saved| saved.resume_metadata.clone())
            .unwrap_or_default();
        let resume_metadata = serde_json::to_string(&resume_state)
            .map(Some)
            .map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?;
        let transport = (self.transport_factory)(descriptor_from_config(&backend))
            .map_err(|_| AcpLiveRuntimeError::RuntimeUnavailable)?;
        let runtime_binding = transport
            .start(ManagedRuntimeStartRequest {
                session_id: binding.binding_id.clone(),
                generation,
                resume_metadata_json: resume_metadata,
            })
            .await
            .map_err(|_| AcpLiveRuntimeError::RuntimeUnavailable)?;
        let process_lease_sha256 = sha256_hex(
            serde_json::to_vec(&runtime_binding.lease)
                .map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?
                .as_slice(),
        );
        state.durable.bindings.insert(
            binding.binding_id.clone(),
            durable_binding(binding, backend.id.as_str(), generation, resume_state),
        );
        state.handles.insert(
            backend.id.clone(),
            LiveHandle {
                transport: Arc::clone(&transport),
                generation,
                attached_sessions: BTreeSet::from([binding.binding_id.clone()]),
                active_commands: BTreeSet::new(),
                last_used_at_unix_ms: now,
                process_lease_sha256,
            },
        );
        self.save_locked(&mut state).await?;
        Ok((transport, generation, Duration::from_millis(backend.command_timeout_ms)))
    }

    async fn record_progress(
        &self,
        binding: &AcpSessionBindingRecord,
        command_id: &str,
        backend_id: &str,
        generation: u64,
        sequence: u64,
        visible_output: bool,
    ) -> Result<(), AcpLiveRuntimeError> {
        let mut state = self.state.lock().await;
        let Some(saved) = state.durable.bindings.get_mut(&binding.binding_id) else {
            return Err(AcpLiveRuntimeError::State("live binding disappeared".to_owned()));
        };
        if saved.generation != generation {
            return Err(AcpLiveRuntimeError::RuntimeUnavailable);
        }
        saved.resume_metadata.last_acknowledged_sequence = sequence;
        saved.resume_metadata.visible_output_committed |= visible_output;
        update_lifecycle_locked(
            &mut state,
            LifecycleUpdate {
                command_id,
                session_binding_id: binding.binding_id.as_str(),
                backend_id,
                generation,
                state: AcpCommandState::Streaming,
                reason_code: "acp_runtime.streaming",
                visible_output,
            },
        );
        self.save_locked(&mut state).await
    }

    async fn record_terminal(
        &self,
        binding: &AcpSessionBindingRecord,
        command_id: &str,
        backend_id: &str,
        generation: u64,
        sequence: u64,
        visible_output: bool,
    ) -> Result<(), AcpLiveRuntimeError> {
        let mut state = self.state.lock().await;
        if let Some(saved) = state.durable.bindings.get_mut(&binding.binding_id) {
            saved.resume_metadata.last_acknowledged_sequence = sequence;
            saved.resume_metadata.visible_output_committed = false;
        }
        if let Some(handle) = state.handles.get_mut(backend_id) {
            handle.active_commands.remove(command_id);
            handle.last_used_at_unix_ms =
                unix_ms_now().map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?;
        }
        update_lifecycle_locked(
            &mut state,
            LifecycleUpdate {
                command_id,
                session_binding_id: binding.binding_id.as_str(),
                backend_id,
                generation,
                state: AcpCommandState::Completed,
                reason_code: "acp_runtime.completed",
                visible_output,
            },
        );
        self.save_locked(&mut state).await
    }

    async fn invalidate_backend(
        &self,
        backend_id: &str,
        command_id: &str,
        reason_code: &str,
        command_state: AcpCommandState,
    ) -> Result<(), AcpLiveRuntimeError> {
        let (handle, generation) = {
            let mut state = self.state.lock().await;
            let handle = state.handles.remove(backend_id);
            let generation = handle.as_ref().map_or(0, |value| value.generation);
            for binding in state
                .durable
                .bindings
                .values_mut()
                .filter(|binding| binding.backend_id == backend_id)
            {
                binding.generation = binding.generation.saturating_add(1);
                if reason_code != "acp_runtime.process_failed" {
                    binding.resume_metadata.visible_output_committed = false;
                }
            }
            if !command_id.is_empty() {
                if let Some(lifecycle) = state.lifecycles.get_mut(command_id) {
                    lifecycle.state = command_state;
                    lifecycle.reason_code = reason_code.to_owned();
                    lifecycle.updated_at_unix_ms = unix_ms_now().unwrap_or(0);
                }
            }
            // This durable write is intentionally completed before callers
            // promote another command from the affected session queue.
            self.save_locked(&mut state).await?;
            (handle, generation)
        };
        if let Some(handle) = handle {
            if !command_id.is_empty() {
                let _ = handle.transport.cancel(command_id, generation).await;
            }
            let _ = handle.transport.close().await;
        }
        Ok(())
    }

    async fn update_lifecycle(&self, update: LifecycleUpdate<'_>) {
        let mut state = self.state.lock().await;
        update_lifecycle_locked(&mut state, update);
    }

    async fn save_locked(&self, state: &mut ManagerState) -> Result<(), AcpLiveRuntimeError> {
        state.durable.updated_at_unix_ms =
            unix_ms_now().map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?;
        let payload = serde_json::to_vec_pretty(&state.durable)
            .map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?;
        let root = self.root.clone();
        let path = self.bindings_path.clone();
        tokio::task::spawn_blocking(move || super::write_atomically(&root, &path, &payload))
            .await
            .map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?
            .map_err(|error| AcpLiveRuntimeError::State(error.to_string()))?;
        ensure_owner_only_file(self.bindings_path.as_path())
            .map_err(|error| AcpLiveRuntimeError::State(error.to_string()))
    }

    async fn wait_for_turn(
        &self,
        binding: &AcpSessionBindingRecord,
        command_id: &str,
        backend_id: &str,
    ) -> Result<(), AcpLiveRuntimeError> {
        let notify = {
            let mut queues = self
                .queues
                .lock()
                .map_err(|_| AcpLiveRuntimeError::State("ACP queue lock failed".to_owned()))?;
            let slot = queues.entry(binding.binding_id.clone()).or_default();
            let decision = slot.queue.enqueue(
                AcpPreparedTurn {
                    turn_id: command_id.to_owned(),
                    acp_session_id: binding.acp_session_id.clone(),
                    palyra_session_id: binding.palyra_session_id.clone(),
                    runtime_id: backend_id.to_owned(),
                    handle_id: binding.binding_id.clone(),
                    mutating: true,
                },
                self.queue_policy(),
            );
            if decision.decision == AcpTurnQueueDecisionKind::BackpressureRejected {
                return Err(AcpLiveRuntimeError::Backpressure);
            }
            Arc::clone(&slot.notify)
        };
        self.update_lifecycle(LifecycleUpdate {
            command_id,
            session_binding_id: binding.binding_id.as_str(),
            backend_id,
            generation: 0,
            state: AcpCommandState::Queued,
            reason_code: "acp_runtime.queued",
            visible_output: false,
        })
        .await;
        loop {
            let notified = notify.notified();
            let active = {
                let queues = self
                    .queues
                    .lock()
                    .map_err(|_| AcpLiveRuntimeError::State("ACP queue lock failed".to_owned()))?;
                queues.get(&binding.binding_id).is_some_and(|slot| slot.queue.is_active(command_id))
            };
            if active {
                return Ok(());
            }
            notified.await;
        }
    }

    fn finish_turn(
        &self,
        session_binding_id: &str,
        command_id: &str,
        cancelled: bool,
    ) -> Result<super::session_actor_queue::AcpTurnQueueDecision, AcpLiveRuntimeError> {
        let mut queues = self
            .queues
            .lock()
            .map_err(|_| AcpLiveRuntimeError::State("ACP queue lock failed".to_owned()))?;
        let slot = queues
            .get_mut(session_binding_id)
            .ok_or_else(|| AcpLiveRuntimeError::CommandNotFound(command_id.to_owned()))?;
        let decision = if cancelled {
            slot.queue.cancel_and_promote(command_id, self.queue_policy())
        } else {
            slot.queue.complete_turn(command_id, self.queue_policy())
        };
        slot.notify.notify_waiters();
        Ok(decision)
    }

    fn queue_contains_active(
        &self,
        session_binding_id: &str,
        command_id: &str,
    ) -> Result<bool, AcpLiveRuntimeError> {
        let queues = self
            .queues
            .lock()
            .map_err(|_| AcpLiveRuntimeError::State("ACP queue lock failed".to_owned()))?;
        Ok(queues.get(session_binding_id).is_some_and(|slot| slot.queue.is_active(command_id)))
    }

    fn queue_contains(
        &self,
        session_binding_id: &str,
        command_id: &str,
    ) -> Result<bool, AcpLiveRuntimeError> {
        let queues = self
            .queues
            .lock()
            .map_err(|_| AcpLiveRuntimeError::State("ACP queue lock failed".to_owned()))?;
        Ok(queues.get(session_binding_id).is_some_and(|slot| slot.queue.contains(command_id)))
    }

    async fn interrupted_after_visible_output(&self, session_binding_id: &str) -> bool {
        self.state
            .lock()
            .await
            .durable
            .bindings
            .get(session_binding_id)
            .is_some_and(|binding| binding.resume_metadata.visible_output_committed)
    }

    fn queue_counts(&self) -> (usize, usize) {
        self.queues.lock().map_or((0, 0), |queues| {
            queues.values().fold((0, 0), |(active, pending), slot| {
                (active + slot.queue.active_turn_count(), pending + slot.queue.pending_turn_count())
            })
        })
    }

    fn queue_policy(&self) -> AcpSessionActorQueuePolicy {
        AcpSessionActorQueuePolicy {
            supports_concurrent_turns: false,
            max_active_turns: 1,
            max_pending_turns: self.config.max_pending_commands,
        }
    }

    fn require_backend(
        &self,
        backend_id: &str,
    ) -> Result<&AcpRuntimeBackendConfig, AcpLiveRuntimeError> {
        if !self.rollout_enabled {
            return Err(AcpLiveRuntimeError::RolloutDisabled);
        }
        self.config
            .backends
            .iter()
            .find(|backend| backend.id == backend_id && backend.enabled)
            .ok_or_else(|| AcpLiveRuntimeError::BackendUnavailable(backend_id.to_owned()))
    }

    fn candidate_chain(&self, primary: &str) -> Result<Vec<String>, AcpLiveRuntimeError> {
        let backend = self.require_backend(primary)?;
        let mut candidates = vec![backend.id.clone()];
        candidates.extend(
            backend
                .fallback_backend_ids
                .iter()
                .filter(|candidate| {
                    self.config
                        .backends
                        .iter()
                        .any(|backend| backend.id == candidate.as_str() && backend.enabled)
                })
                .cloned(),
        );
        Ok(candidates)
    }
}

fn descriptor_from_config(config: &AcpRuntimeBackendConfig) -> ManagedRuntimeDescriptor {
    ManagedRuntimeDescriptor {
        runtime_id: config.id.clone(),
        protocol_version: config.protocol_version.clone(),
        capability_digest: config.capability_digest_sha256.clone(),
        executable: config.executable.clone(),
        args: config.args.clone(),
        cwd: config.cwd.clone(),
        env: BTreeMap::new(),
        handshake_timeout: Duration::from_millis(config.handshake_timeout_ms),
        command_timeout: Duration::from_millis(config.command_timeout_ms),
        lease_duration: Duration::from_millis(config.lease_duration_ms),
    }
}

fn durable_binding(
    binding: &AcpSessionBindingRecord,
    backend_id: &str,
    generation: u64,
    resume_metadata: AcpResumeMetadataV2,
) -> AcpRuntimeBindingV2 {
    AcpRuntimeBindingV2 {
        schema_version: ACP_LIVE_BINDINGS_SCHEMA_VERSION,
        session_binding_id: binding.binding_id.clone(),
        palyra_session_id_sha256: sha256_hex(binding.palyra_session_id.as_bytes()),
        backend_id: backend_id.to_owned(),
        generation,
        resume_metadata,
    }
}

fn next_generation(state: &ManagerState, backend_id: &str) -> Result<u64, AcpLiveRuntimeError> {
    let maximum = state
        .durable
        .bindings
        .values()
        .filter(|binding| binding.backend_id == backend_id)
        .map(|binding| binding.generation)
        .chain(state.handles.get(backend_id).map(|handle| handle.generation))
        .max()
        .unwrap_or(0);
    maximum
        .checked_add(1)
        .ok_or_else(|| AcpLiveRuntimeError::State("ACP runtime generation exhausted".to_owned()))
}

fn update_lifecycle_locked(state: &mut ManagerState, update: LifecycleUpdate<'_>) {
    let now = unix_ms_now().unwrap_or(0);
    let queued_at = state
        .lifecycles
        .get(update.command_id)
        .map_or(now, |lifecycle| lifecycle.queued_at_unix_ms);
    state.lifecycles.insert(
        update.command_id.to_owned(),
        AcpCommandLifecycle {
            command_id_sha256: sha256_hex(update.command_id.as_bytes()),
            session_binding_id: update.session_binding_id.to_owned(),
            backend_id: update.backend_id.to_owned(),
            generation: update.generation,
            state: update.state,
            reason_code: update.reason_code.to_owned(),
            queued_at_unix_ms: queued_at,
            updated_at_unix_ms: now,
            visible_output_committed: update.visible_output,
        },
    );
    while state.lifecycles.len() > MAX_LIFECYCLES {
        let Some(oldest) = state
            .lifecycles
            .iter()
            .min_by_key(|(_, lifecycle)| lifecycle.updated_at_unix_ms)
            .map(|(command_id, _)| command_id.clone())
        else {
            break;
        };
        state.lifecycles.remove(oldest.as_str());
    }
}

fn load_durable_bindings(path: &Path) -> AcpRuntimeResult<DurableBindingsV2> {
    if !path.exists() {
        return Ok(DurableBindingsV2::default());
    }
    ensure_owner_only_file(path).map_err(|source| AcpRuntimeError::PermissionHarden {
        path: path.to_path_buf(),
        message: source.to_string(),
    })?;
    let payload = fs::read(path).map_err(|source| AcpRuntimeError::Io {
        operation: "read",
        path: path.to_path_buf(),
        source,
    })?;
    let durable = parse_versioned_json::<DurableBindingsV2>(
        payload.as_slice(),
        ACP_LIVE_BINDINGS_FORMAT,
        &[],
    )
    .map_err(|source| AcpRuntimeError::VersionedJson { path: path.to_path_buf(), source })?;
    for (binding_id, binding) in &durable.bindings {
        if binding.schema_version != ACP_LIVE_BINDINGS_SCHEMA_VERSION
            || binding.session_binding_id != *binding_id
            || binding.generation == 0
            || binding.palyra_session_id_sha256.len() != 64
        {
            return Err(AcpRuntimeError::StateInvariant {
                message: "ACP live runtime binding metadata is invalid".to_owned(),
            });
        }
    }
    Ok(durable)
}

fn is_visible_event(method: &str) -> bool {
    matches!(method, "text_delta" | "message" | "tool_call" | "tool_result" | "approval_request")
}

fn health_state_name(state: ManagedRuntimeHealthState) -> &'static str {
    match state {
        ManagedRuntimeHealthState::Starting => "starting",
        ManagedRuntimeHealthState::Ready => "ready",
        ManagedRuntimeHealthState::Draining => "draining",
        ManagedRuntimeHealthState::Closed => "closed",
        ManagedRuntimeHealthState::Crashed => "crashed",
        ManagedRuntimeHealthState::Quarantined => "quarantined",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use palyra_common::runtime_contracts::{
        AcpCapability, AcpCursor, AcpScope, AcpSessionMode, CleanupReportV1, ProcessLeaseV1,
        ProcessOwnershipKind, ProcessProvenance, RuntimeGeneration, RuntimeInstanceId,
        RuntimeLeaseId, RUNTIME_HANDLE_SCHEMA_VERSION,
    };

    use super::*;
    use crate::application::managed_runtime::{ManagedRuntimeHealth, RuntimeBindingRecord};

    struct FakeTransport {
        backend_id: String,
        events: broadcast::Sender<RuntimeTransportEvent>,
        binding: Mutex<Option<RuntimeBindingRecord>>,
        health: Mutex<ManagedRuntimeHealth>,
    }

    impl FakeTransport {
        fn new(backend_id: String) -> Self {
            let (events, _) = broadcast::channel(64);
            Self {
                backend_id,
                events,
                binding: Mutex::new(None),
                health: Mutex::new(ManagedRuntimeHealth {
                    state: ManagedRuntimeHealthState::Closed,
                    generation: 0,
                    protocol_strikes: 0,
                    last_reason_code: "fake.closed".to_owned(),
                    stderr_tail_redacted: String::new(),
                }),
            }
        }
    }

    #[async_trait]
    impl RuntimeTransport for FakeTransport {
        async fn start(
            &self,
            request: ManagedRuntimeStartRequest,
        ) -> Result<RuntimeBindingRecord, RuntimeTransportError> {
            let binding = RuntimeBindingRecord {
                runtime_id: self.backend_id.clone(),
                session_id: request.session_id,
                generation: request.generation,
                protocol_version: "acp.fixture.v1".to_owned(),
                capability_digest: "a".repeat(64),
                nonce_sha256: "b".repeat(64),
                lease: fake_lease(self.backend_id.as_str(), request.generation),
                resume_metadata_json: request.resume_metadata_json,
                last_acknowledged_sequence: 0,
            };
            *self.binding.lock().map_err(|_| RuntimeTransportError::Unavailable)? =
                Some(binding.clone());
            *self.health.lock().map_err(|_| RuntimeTransportError::Unavailable)? =
                ManagedRuntimeHealth {
                    state: ManagedRuntimeHealthState::Ready,
                    generation: request.generation,
                    protocol_strikes: 0,
                    last_reason_code: "fake.ready".to_owned(),
                    stderr_tail_redacted: String::new(),
                };
            Ok(binding)
        }

        async fn send_command(
            &self,
            command: RuntimeTransportCommand,
        ) -> Result<(), RuntimeTransportError> {
            let generation = command.generation;
            let command_id = command.command_id;
            let _ = self.events.send(RuntimeTransportEvent::Accepted {
                command_id: command_id.clone(),
                generation,
                sequence: 1,
            });
            if command.method == "hang" {
                return Ok(());
            }
            if command.method == "crash" && self.backend_id == "primary" {
                let _ = self
                    .events
                    .send(RuntimeTransportEvent::ChildExited { generation, exit_code: Some(17) });
                return Ok(());
            }
            if command.method == "crash_after_output" && self.backend_id == "primary" {
                let _ = self.events.send(RuntimeTransportEvent::Event {
                    command_id,
                    generation,
                    sequence: 2,
                    method: "text_delta".to_owned(),
                    payload: json!({ "text": "visible but never persisted" }),
                });
                let _ = self
                    .events
                    .send(RuntimeTransportEvent::ChildExited { generation, exit_code: Some(17) });
                return Ok(());
            }
            let _ = self.events.send(RuntimeTransportEvent::Event {
                command_id: command_id.clone(),
                generation,
                sequence: 2,
                method: "text_delta".to_owned(),
                payload: json!({ "text": "fixture" }),
            });
            let _ = self.events.send(RuntimeTransportEvent::Terminal {
                command_id,
                generation,
                sequence: 3,
                outcome: "completed".to_owned(),
                payload: json!({ "final_message": "fixture complete" }),
            });
            Ok(())
        }

        fn event_stream(
            &self,
        ) -> Result<broadcast::Receiver<RuntimeTransportEvent>, RuntimeTransportError> {
            Ok(self.events.subscribe())
        }

        async fn cancel(
            &self,
            _command_id: &str,
            _generation: u64,
        ) -> Result<(), RuntimeTransportError> {
            Ok(())
        }

        async fn close(&self) -> Result<CleanupReportV1, RuntimeTransportError> {
            if let Ok(mut health) = self.health.lock() {
                health.state = ManagedRuntimeHealthState::Closed;
                health.last_reason_code = "fake.closed".to_owned();
            }
            Err(RuntimeTransportError::Unavailable)
        }

        fn binding(&self) -> Result<Option<RuntimeBindingRecord>, RuntimeTransportError> {
            self.binding
                .lock()
                .map(|binding| binding.clone())
                .map_err(|_| RuntimeTransportError::Unavailable)
        }

        fn health(&self) -> ManagedRuntimeHealth {
            self.health.lock().map_or_else(
                |_| ManagedRuntimeHealth {
                    state: ManagedRuntimeHealthState::Quarantined,
                    generation: 0,
                    protocol_strikes: 1,
                    last_reason_code: "fake.lock_failed".to_owned(),
                    stderr_tail_redacted: String::new(),
                },
                |health| health.clone(),
            )
        }
    }

    #[tokio::test]
    async fn two_sessions_share_one_process_lease() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let creations = Arc::new(AtomicUsize::new(0));
        let manager = manager(tempdir.path(), config(60_000, true), Arc::clone(&creations));
        let first = binding("binding-a", "session-a", "primary");
        let second = binding("binding-b", "session-b", "primary");

        let first_result = manager
            .execute(&first, "command-a", "prompt", json!({ "prompt": "first secret" }))
            .await
            .expect("first command");
        let second_result = manager
            .execute(&second, "command-b", "prompt", json!({ "prompt": "second secret" }))
            .await
            .expect("second command");

        assert_eq!(first_result.generation, second_result.generation);
        assert_eq!(creations.load(Ordering::SeqCst), 1);
        let health = manager.health().await;
        assert_eq!(health[0].attached_sessions, 2);
        assert!(health[0].process_lease_sha256.is_some());
    }

    #[tokio::test]
    async fn cancellation_invalidates_generation_before_queue_promotion() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let creations = Arc::new(AtomicUsize::new(0));
        let manager =
            Arc::new(manager(tempdir.path(), config(60_000, true), Arc::clone(&creations)));
        let session = binding("binding-a", "session-a", "primary");
        let first_manager = Arc::clone(&manager);
        let first_session = session.clone();
        let first = tokio::spawn(async move {
            first_manager
                .execute(&first_session, "command-a", "hang", json!({ "prompt": "never persist" }))
                .await
        });
        wait_for_state(&manager, "command-a", AcpCommandState::Dispatching).await;
        let second_manager = Arc::clone(&manager);
        let second_session = session.clone();
        let second = tokio::spawn(async move {
            second_manager
                .execute(&second_session, "command-b", "prompt", json!({ "prompt": "queued" }))
                .await
        });
        wait_for_state(&manager, "command-b", AcpCommandState::Queued).await;

        manager.cancel(&session, "command-a").await.expect("cancel active command");
        let result = second.await.expect("second task").expect("promoted command");
        first.abort();

        assert!(result.generation >= 3);
        assert_eq!(creations.load(Ordering::SeqCst), 2);
        let lifecycle = manager.lifecycle("command-a").await.expect("cancel lifecycle");
        assert_eq!(lifecycle.state, AcpCommandState::Cancelled);
    }

    #[tokio::test]
    async fn crash_fails_over_only_before_visible_output() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let creations = Arc::new(AtomicUsize::new(0));
        let manager = manager(tempdir.path(), config(60_000, true), creations);
        let session = binding("binding-a", "session-a", "primary");

        let recovered = manager
            .execute(&session, "command-a", "crash", json!({}))
            .await
            .expect("pre-output crash should use trusted fallback");
        assert_eq!(recovered.backend_id, "fallback");

        let error = manager
            .execute(&session, "command-b", "crash_after_output", json!({}))
            .await
            .expect_err("visible output must block fallback");
        assert!(matches!(error, AcpLiveRuntimeError::FailoverAfterOutput));
    }

    #[tokio::test]
    async fn idle_eviction_recreates_and_restart_state_stays_payload_free() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let creations = Arc::new(AtomicUsize::new(0));
        let runtime_config = config(1, false);
        let live_manager = manager(tempdir.path(), runtime_config.clone(), Arc::clone(&creations));
        let session = binding("binding-a", "session-a", "primary");
        let first = live_manager
            .execute(&session, "command-a", "prompt", json!({ "prompt": "TOP SECRET PROMPT" }))
            .await
            .expect("first command");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(live_manager.evict_idle().await.expect("idle eviction"), 1);
        let second = live_manager
            .execute(&session, "command-b", "prompt", json!({ "prompt": "second" }))
            .await
            .expect("recreated command");
        assert!(second.generation > first.generation);

        let persisted =
            fs::read_to_string(tempdir.path().join(ACP_LIVE_BINDINGS_FILE_NAME)).expect("state");
        assert!(!persisted.contains("TOP SECRET PROMPT"));
        assert!(!persisted.contains("lease_primary"));
        drop(live_manager);

        let reopened = manager(tempdir.path(), runtime_config, Arc::clone(&creations));
        let third = reopened
            .execute(&session, "command-c", "prompt", json!({ "prompt": "after restart" }))
            .await
            .expect("restart reconstruction");
        assert!(third.generation > second.generation);
    }

    fn manager(
        root: &Path,
        config: AcpRuntimeConfig,
        creations: Arc<AtomicUsize>,
    ) -> AcpLiveRuntimeManager {
        let factory: TransportFactory = Arc::new(move |descriptor| {
            creations.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(FakeTransport::new(descriptor.runtime_id)) as Arc<dyn RuntimeTransport>)
        });
        AcpLiveRuntimeManager::open_with_factory(root, true, config, factory)
            .expect("manager should open")
    }

    fn config(idle_ttl_ms: u64, with_fallback: bool) -> AcpRuntimeConfig {
        let executable = std::env::current_exe().expect("test executable");
        let cwd = executable.parent().expect("test executable parent").to_path_buf();
        let backend = |id: &str, fallbacks: Vec<String>| AcpRuntimeBackendConfig {
            id: id.to_owned(),
            enabled: true,
            executable: executable.clone(),
            args: Vec::new(),
            cwd: cwd.clone(),
            protocol_version: "acp.fixture.v1".to_owned(),
            capability_digest_sha256: "a".repeat(64),
            handshake_timeout_ms: 1_000,
            command_timeout_ms: 5_000,
            lease_duration_ms: 60_000,
            fallback_backend_ids: fallbacks,
        };
        let mut backends = vec![backend(
            "primary",
            if with_fallback { vec!["fallback".to_owned()] } else { Vec::new() },
        )];
        if with_fallback {
            backends.push(backend("fallback", Vec::new()));
        }
        AcpRuntimeConfig { max_pending_commands: 4, idle_ttl_ms, backends }
    }

    fn binding(
        binding_id: &str,
        acp_session_id: &str,
        backend_id: &str,
    ) -> AcpSessionBindingRecord {
        AcpSessionBindingRecord {
            schema_version: 1,
            binding_id: binding_id.to_owned(),
            acp_client_id: "test-client".to_owned(),
            acp_session_id: acp_session_id.to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_key: format!("test:{acp_session_id}"),
            session_label: None,
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes: vec![AcpScope::RunsRead, AcpScope::RunsWrite],
            capabilities: vec![AcpCapability::RunControl],
            mode: AcpSessionMode::Normal,
            config: json!({ "runtime_backend": backend_id }),
            cursor: AcpCursor::default(),
            last_seen_at_unix_ms: 0,
            protocol_version: 1,
            stale_permissions: false,
        }
    }

    fn fake_lease(backend_id: &str, generation: u64) -> ProcessLeaseV1 {
        ProcessLeaseV1 {
            schema_version: RUNTIME_HANDLE_SCHEMA_VERSION,
            lease_id: RuntimeLeaseId::parse(format!("lease_{backend_id}").as_str())
                .expect("lease id"),
            instance_id: RuntimeInstanceId::parse(format!("instance_{backend_id}").as_str())
                .expect("instance id"),
            generation: RuntimeGeneration::new(generation).expect("generation"),
            pid: 42,
            provenance: ProcessProvenance {
                ownership_kind: ProcessOwnershipKind::WindowsJobObject,
                start_token: "fake-start-token".to_owned(),
                executable_sha256: "c".repeat(64),
                owner_nonce: "fake-owner-nonce".to_owned(),
                ownership_identity_sha256: "d".repeat(64),
            },
            issued_at_unix_ms: 1,
            expires_at_unix_ms: 60_001,
            verified_at_unix_ms: 1,
        }
    }

    async fn wait_for_state(
        manager: &AcpLiveRuntimeManager,
        command_id: &str,
        expected: AcpCommandState,
    ) {
        for _ in 0..100 {
            if manager
                .lifecycle(command_id)
                .await
                .is_some_and(|lifecycle| lifecycle.state == expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("command did not reach expected lifecycle state");
    }
}
