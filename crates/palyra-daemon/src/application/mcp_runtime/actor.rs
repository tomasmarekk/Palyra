//! Single-owner actor for generation-fenced persistent MCP sessions.
//!
//! The actor exclusively owns transport I/O, routes concurrent requests by
//! identifier, publishes bounded notifications, and drains before releasing
//! the session. Persistence transitions are compare-and-swap committed.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

use super::{
    supervisor::{
        McpReconnectPolicy, McpRuntimeEventV2, McpRuntimeLifecycleState, McpRuntimeRecordStore,
        McpRuntimeStoreError, McpRuntimeSupervisorError, McpServerRecordV2, ReconnectOutcome,
    },
    transport::{
        McpCallbackResponsePayload, McpConnectRequest, McpResponsePayload,
        McpServerCallbackRequest, McpServerCallbackResponse, McpServerNotification,
        McpSessionConnector, McpSessionReader, McpSessionRequest, McpSessionWriter,
        McpTransportError, McpTransportEvent, McpTransportHealth, McpTransportHealthState,
    },
};

const MAX_METHOD_BYTES: usize = 256;
const MAX_REQUEST_PAYLOAD_BYTES: usize = 1024 * 1024;
const TIMER_GRANULARITY: Duration = Duration::from_millis(25);

/// Runtime limits and initialization contract for one MCP session actor.
#[derive(Debug, Clone)]
pub struct McpSessionActorConfig {
    /// Durable server record restored or newly configured by the host.
    pub record: McpServerRecordV2,
    /// MCP client initialization payload.
    pub initialize: super::transport::McpInitializeRequest,
    /// Host-owned binding applied to every external callback.
    pub callback_binding: McpCallbackBinding,
    /// Bounded host-to-actor command capacity.
    pub command_queue_capacity: usize,
    /// Bounded actor-to-host notification capacity.
    pub notification_queue_capacity: usize,
    /// Maximum accepted requests awaiting a transport response.
    pub max_in_flight_requests: usize,
    /// Per-request response deadline.
    pub request_timeout: Duration,
    /// Transport setup and initialization deadline.
    pub handshake_timeout: Duration,
    /// Host callback decision deadline.
    pub callback_timeout: Duration,
    /// Deadline for one transport write or close operation.
    pub transport_operation_timeout: Duration,
    /// Maximum idle interval before the actor issues a protocol ping.
    pub keepalive_interval: Duration,
    /// Maximum time allowed for one keepalive response.
    pub keepalive_timeout: Duration,
    /// Default drain deadline after the final handle is dropped.
    pub default_drain_timeout: Duration,
    /// Bounded reconnect and quarantine policy.
    pub reconnect_policy: McpReconnectPolicy,
}

impl McpSessionActorConfig {
    /// Validates actor capacities, deadlines, and durable startup state.
    ///
    /// # Errors
    /// Returns [`McpActorError::InvalidConfiguration`] for unsafe limits or state.
    pub fn validate(&self) -> Result<(), McpActorError> {
        self.record.validate()?;
        self.initialize.validate()?;
        self.reconnect_policy.validate()?;
        if self.command_queue_capacity == 0
            || self.notification_queue_capacity == 0
            || self.max_in_flight_requests == 0
            || self.request_timeout.is_zero()
            || self.handshake_timeout.is_zero()
            || self.callback_timeout.is_zero()
            || self.transport_operation_timeout.is_zero()
            || self.keepalive_interval.is_zero()
            || self.keepalive_timeout.is_zero()
            || self.default_drain_timeout.is_zero()
            || !self.callback_binding.is_valid()
        {
            return Err(McpActorError::InvalidConfiguration {
                reason_code: "mcp.runtime.actor.invalid_config",
            });
        }
        Ok(())
    }
}

/// Host-owned principal, session, and origin binding for server callbacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpCallbackBinding {
    /// Principal authorized to receive the callback.
    pub principal_id: String,
    /// Session authorized to receive the callback.
    pub session_id: String,
    /// Origin authorized to receive the callback.
    pub origin: String,
}

impl McpCallbackBinding {
    pub(crate) fn is_valid(&self) -> bool {
        valid_binding(&self.principal_id)
            && valid_binding(&self.session_id)
            && valid_binding(&self.origin)
    }
}

/// Host policy boundary for sampling, elicitation, and roots callbacks.
#[async_trait]
pub trait McpHostCallbackPort: Send + Sync {
    /// Resolves a generation-, principal-, session-, and origin-bound callback.
    ///
    /// The implementation must not grant tools that were absent from an
    /// explicit sampling request and must return only redaction-safe output.
    async fn handle_callback(
        &self,
        request: &McpServerCallbackRequest,
    ) -> Result<McpCallbackResponsePayload, McpHostCallbackError>;

    /// Observes an already durable runtime record transition.
    ///
    /// Production policy services use this synchronous hook to replace their
    /// generation and catalog authority before another callback is handled.
    /// Implementations must not perform I/O or fail independently.
    fn runtime_record_committed(&self, _record: &McpServerRecordV2) {}
}

/// Host callback policy or availability failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpHostCallbackError {
    /// Host policy denied the callback.
    #[error("mcp host callback denied: {reason_code}")]
    Denied {
        /// Stable policy reason.
        reason_code: String,
        /// Sanitized explanation safe for the server.
        safe_message: String,
    },
    /// A required host service was unavailable.
    #[error("mcp host callback unavailable: {reason_code}")]
    Unavailable {
        /// Stable availability reason.
        reason_code: String,
    },
}

/// Cloneable host handle for a running session actor.
#[derive(Clone)]
pub struct McpActorHandle {
    commands: mpsc::Sender<McpActorCommand>,
}

impl McpActorHandle {
    /// Sends a generation- and catalog-pinned request through the persistent session.
    ///
    /// # Errors
    /// Returns [`McpActorError`] for stale fencing data, bounded backpressure,
    /// transport failure, remote errors, or response timeout.
    pub async fn request(
        &self,
        expected_runtime_generation: u64,
        expected_catalog_epoch: u64,
        method: impl Into<String>,
        params_json: Value,
    ) -> Result<Value, McpActorError> {
        self.request_with_callback_binding(
            expected_runtime_generation,
            expected_catalog_epoch,
            method,
            params_json,
            None,
        )
        .await
    }

    /// Sends a pinned request whose nested callbacks inherit the supplied
    /// host-owned principal, session, and origin.
    pub async fn request_with_callback_binding(
        &self,
        expected_runtime_generation: u64,
        expected_catalog_epoch: u64,
        method: impl Into<String>,
        params_json: Value,
        callback_binding: Option<McpCallbackBinding>,
    ) -> Result<Value, McpActorError> {
        let method = method.into();
        validate_request_input(&method, &params_json)?;
        if callback_binding.as_ref().is_some_and(|binding| !binding.is_valid()) {
            return Err(McpActorError::InvalidConfiguration {
                reason_code: "mcp.runtime.callback.binding_invalid",
            });
        }
        let (reply, receiver) = oneshot::channel();
        self.commands
            .try_send(McpActorCommand::Request {
                expected_runtime_generation,
                expected_catalog_epoch,
                method,
                params_json,
                callback_binding,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => McpActorError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => McpActorError::ActorStopped,
            })?;
        receiver.await.map_err(|_| McpActorError::ActorStopped)?
    }

    /// Commits a host-authored catalog digest only if the caller's complete
    /// generation and epoch pin is still current.
    pub async fn advance_host_catalog(
        &self,
        expected_runtime_generation: u64,
        expected_catalog_epoch: u64,
        catalog_digest: String,
    ) -> Result<McpServerRecordV2, McpActorError> {
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(McpActorCommand::AdvanceHostCatalog {
                expected_runtime_generation,
                expected_catalog_epoch,
                catalog_digest,
                reply,
            })
            .await
            .map_err(|_| McpActorError::ActorStopped)?;
        receiver.await.map_err(|_| McpActorError::ActorStopped)?
    }

    /// Returns a consistent snapshot from the actor owner task.
    ///
    /// # Errors
    /// Returns [`McpActorError::ActorStopped`] when the owner is no longer running.
    pub async fn snapshot(&self) -> Result<McpActorSnapshot, McpActorError> {
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(McpActorCommand::Snapshot { reply })
            .await
            .map_err(|_| McpActorError::ActorStopped)?;
        receiver.await.map_err(|_| McpActorError::ActorStopped)
    }

    /// Stops admission, waits for accepted requests, and closes the transport.
    ///
    /// # Errors
    /// Returns [`McpActorError`] when the actor is stopped or already draining.
    pub async fn drain(&self, timeout: Duration) -> Result<McpDrainOutcome, McpActorError> {
        if timeout.is_zero() {
            return Err(McpActorError::InvalidConfiguration {
                reason_code: "mcp.runtime.actor.zero_drain_timeout",
            });
        }
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(McpActorCommand::Drain { timeout, reply })
            .await
            .map_err(|_| McpActorError::ActorStopped)?;
        receiver.await.map_err(|_| McpActorError::ActorStopped)?
    }
}

/// Actor-owned runtime state snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpActorSnapshot {
    /// Latest committed durable record.
    pub record: McpServerRecordV2,
    /// Requests accepted and awaiting a response.
    pub in_flight_requests: usize,
    /// Notifications dropped because the bounded receiver was full.
    pub dropped_notifications: u64,
    /// Whether new requests are being rejected during drain.
    pub draining: bool,
    /// Last actor-owned transport health projection, when a generation connected.
    pub transport_health: Option<McpTransportHealth>,
    /// Most recent bounded reconnect or quarantine decision.
    pub reconnect_outcome: Option<ReconnectOutcome>,
}

/// Bounded observation emitted by the actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpActorNotification {
    /// A durable lifecycle transition committed.
    LifecycleChanged(McpServerRecordV2),
    /// A validated server notification was accepted.
    Server {
        /// Runtime generation that emitted the notification.
        runtime_generation: u64,
        /// Catalog epoch after applying the notification.
        catalog_epoch: u64,
        /// Server notification.
        notification: McpServerNotification,
    },
    /// A catalog notification advanced the durable epoch.
    CatalogEpochAdvanced {
        /// Runtime generation that owns the epoch.
        runtime_generation: u64,
        /// New durable catalog epoch.
        catalog_epoch: u64,
    },
    /// A late event was rejected without affecting current requests.
    StaleEventRejected {
        /// Generation active in the actor.
        active_runtime_generation: u64,
        /// Generation carried by the rejected event.
        observed_runtime_generation: u64,
    },
    /// A response arrived after its request had already reached a terminal outcome.
    LateResponseRejected {
        /// Actor-issued request identifier.
        request_id: u64,
        /// Runtime generation carried by the response.
        runtime_generation: u64,
    },
    /// Host callback was denied, timed out, or unavailable.
    CallbackRejected {
        /// Server callback identifier.
        callback_id: u64,
        /// Stable host-owned reason.
        reason_code: String,
    },
}

/// Result of a graceful or deadline-forced drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpDrainOutcome {
    /// Requests that did not finish before the drain deadline.
    pub abandoned_requests: usize,
    /// Whether the transport close operation completed successfully.
    pub transport_closed: bool,
}

/// Terminal actor exit reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpActorExit {
    /// Explicit drain completed.
    Drained(McpDrainOutcome),
    /// Every host handle was dropped and the default drain completed.
    HandlesDropped(McpDrainOutcome),
}

/// Spawned actor components retained by the host.
pub struct McpActorParts {
    /// Cloneable command handle.
    pub handle: McpActorHandle,
    /// Single bounded notification receiver.
    pub notifications: mpsc::Receiver<McpActorNotification>,
    /// Owned join handle; callers must observe completion.
    pub join: JoinHandle<Result<McpActorExit, McpActorError>>,
}

/// Namespace for starting a persistent MCP session actor.
pub struct McpSessionActor;

impl McpSessionActor {
    /// Spawns one owner task for a configured MCP server.
    ///
    /// # Errors
    /// Returns [`McpActorError`] for invalid configuration or when no Tokio
    /// runtime is active. The returned join handle must be awaited.
    pub fn spawn(
        config: McpSessionActorConfig,
        connector: Arc<dyn McpSessionConnector>,
        store: Arc<dyn McpRuntimeRecordStore>,
        callbacks: Arc<dyn McpHostCallbackPort>,
    ) -> Result<McpActorParts, McpActorError> {
        config.validate()?;
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| McpActorError::RuntimeUnavailable)?;
        let (command_sender, command_receiver) = mpsc::channel(config.command_queue_capacity);
        let (notification_sender, notification_receiver) =
            mpsc::channel(config.notification_queue_capacity);
        let core = McpActorCore {
            record: config.record.clone(),
            config,
            connector,
            store,
            callbacks,
            commands: command_receiver,
            notifications: notification_sender,
            next_request_id: 0,
            dropped_notifications: 0,
            transport_health: None,
            reconnect_outcome: None,
        };
        let join = runtime.spawn(core.run());
        Ok(McpActorParts {
            handle: McpActorHandle { commands: command_sender },
            notifications: notification_receiver,
            join,
        })
    }
}

#[derive(Debug)]
enum McpActorCommand {
    Request {
        expected_runtime_generation: u64,
        expected_catalog_epoch: u64,
        method: String,
        params_json: Value,
        callback_binding: Option<McpCallbackBinding>,
        reply: oneshot::Sender<Result<Value, McpActorError>>,
    },
    AdvanceHostCatalog {
        expected_runtime_generation: u64,
        expected_catalog_epoch: u64,
        catalog_digest: String,
        reply: oneshot::Sender<Result<McpServerRecordV2, McpActorError>>,
    },
    Snapshot {
        reply: oneshot::Sender<McpActorSnapshot>,
    },
    Drain {
        timeout: Duration,
        reply: oneshot::Sender<Result<McpDrainOutcome, McpActorError>>,
    },
}

struct PendingRequest {
    runtime_generation: u64,
    callback_binding: Option<McpCallbackBinding>,
    deadline: Instant,
    reply: oneshot::Sender<Result<Value, McpActorError>>,
}

struct KeepaliveProbe {
    request_id: u64,
    deadline: Instant,
}

struct DrainState {
    deadline: Instant,
    reply: Option<oneshot::Sender<Result<McpDrainOutcome, McpActorError>>>,
    handles_dropped: bool,
}

struct McpActorCore {
    record: McpServerRecordV2,
    config: McpSessionActorConfig,
    connector: Arc<dyn McpSessionConnector>,
    store: Arc<dyn McpRuntimeRecordStore>,
    callbacks: Arc<dyn McpHostCallbackPort>,
    commands: mpsc::Receiver<McpActorCommand>,
    notifications: mpsc::Sender<McpActorNotification>,
    next_request_id: u64,
    dropped_notifications: u64,
    transport_health: Option<McpTransportHealth>,
    reconnect_outcome: Option<ReconnectOutcome>,
}

enum ConnectedOutcome {
    Reconnect(String),
    Exit(McpActorExit),
}

impl McpActorCore {
    async fn run(mut self) -> Result<McpActorExit, McpActorError> {
        self.normalize_restored_state().await?;
        loop {
            match self.record.lifecycle {
                McpRuntimeLifecycleState::Disabled | McpRuntimeLifecycleState::Quarantined => {
                    if let Some(exit) = self.wait_while_inactive().await? {
                        return Ok(exit);
                    }
                    continue;
                }
                McpRuntimeLifecycleState::Reconnecting => {
                    if let Some(exit) = self.wait_for_reconnect().await? {
                        return Ok(exit);
                    }
                }
                McpRuntimeLifecycleState::Configured | McpRuntimeLifecycleState::Stopped => {}
                McpRuntimeLifecycleState::Starting
                | McpRuntimeLifecycleState::Handshaking
                | McpRuntimeLifecycleState::Ready
                | McpRuntimeLifecycleState::Degraded
                | McpRuntimeLifecycleState::Stopping => {
                    return Err(McpActorError::InvalidConfiguration {
                        reason_code: "mcp.runtime.actor.unexpected_state",
                    });
                }
            }

            let starting = self.record.begin_start(now_unix_ms())?;
            self.commit(starting, "mcp.runtime.session.starting").await?;
            let handshaking = self.record.begin_handshake(now_unix_ms())?;
            self.commit(handshaking, "mcp.runtime.handshake.started").await?;
            let connect_request = McpConnectRequest {
                server_id: self.record.server_id.clone(),
                transport: self.record.transport,
                runtime_generation: self.record.runtime_generation,
                handshake_timeout_ms: duration_millis(self.config.handshake_timeout)?,
                initialize: self.config.initialize.clone(),
            };
            connect_request.validate()?;
            let connected = tokio::time::timeout(
                self.config.handshake_timeout,
                self.connector.connect(&connect_request),
            )
            .await;
            let session = match connected {
                Ok(Ok(session)) => session,
                Ok(Err(error)) => {
                    self.record_transport_failure(error.reason_code()).await?;
                    continue;
                }
                Err(_) => {
                    self.record_transport_failure("mcp.runtime.handshake.timeout").await?;
                    continue;
                }
            };
            let (initialize_result, writer, reader) = session.into_parts();
            let ready = self.record.mark_ready(initialize_result.catalog_digest, now_unix_ms())?;
            self.commit(ready, "mcp.runtime.session.ready").await?;
            let connected_at_unix_ms = now_unix_ms();
            self.transport_health = Some(McpTransportHealth {
                runtime_generation: self.record.runtime_generation,
                state: McpTransportHealthState::Connected,
                connected_at_unix_ms,
                last_activity_at_unix_ms: connected_at_unix_ms,
                last_keepalive_at_unix_ms: None,
                successful_keepalives: 0,
                failed_keepalives: 0,
            });
            match self.run_connected(writer, reader).await? {
                ConnectedOutcome::Reconnect(reason_code) => {
                    self.record_transport_failure(&reason_code).await?;
                }
                ConnectedOutcome::Exit(exit) => return Ok(exit),
            }
        }
    }

    async fn normalize_restored_state(&mut self) -> Result<(), McpActorError> {
        let next = match self.record.lifecycle {
            McpRuntimeLifecycleState::Starting
            | McpRuntimeLifecycleState::Ready
            | McpRuntimeLifecycleState::Handshaking
            | McpRuntimeLifecycleState::Degraded => {
                Some(self.record.recover_after_restart(now_unix_ms())?)
            }
            McpRuntimeLifecycleState::Stopping => Some(self.record.transition(
                McpRuntimeLifecycleState::Stopped,
                now_unix_ms(),
                "mcp.runtime.restart.stopped",
            )?),
            McpRuntimeLifecycleState::Configured
            | McpRuntimeLifecycleState::Reconnecting
            | McpRuntimeLifecycleState::Stopped
            | McpRuntimeLifecycleState::Quarantined
            | McpRuntimeLifecycleState::Disabled => None,
        };
        if let Some(next) = next {
            let reason_code = if next.lifecycle == McpRuntimeLifecycleState::Stopped {
                "mcp.runtime.restart.stopped"
            } else {
                "mcp.runtime.restart.reconnect"
            };
            self.commit(next, reason_code).await?;
        }
        Ok(())
    }

    async fn run_connected(
        &mut self,
        mut writer: Box<dyn McpSessionWriter>,
        mut reader: Box<dyn McpSessionReader>,
    ) -> Result<ConnectedOutcome, McpActorError> {
        let mut pending = BTreeMap::<u64, PendingRequest>::new();
        let mut drain: Option<DrainState> = None;
        let mut keepalive: Option<KeepaliveProbe> = None;
        let mut last_transport_activity = Instant::now();
        let mut timer = tokio::time::interval(TIMER_GRANULARITY);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            if let Some(outcome) =
                self.finish_drain_if_ready(&mut writer, &mut pending, &mut drain).await?
            {
                return Ok(ConnectedOutcome::Exit(outcome));
            }

            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        drain = Some(DrainState {
                            deadline: Instant::now() + self.config.default_drain_timeout,
                            reply: None,
                            handles_dropped: true,
                        });
                        self.begin_drain_transition().await?;
                        continue;
                    };
                    match command {
                        McpActorCommand::Request {
                            expected_runtime_generation,
                            expected_catalog_epoch,
                            method,
                            params_json,
                            callback_binding,
                            reply,
                        } => {
                            if drain.is_some() {
                                let _ = reply.send(Err(McpActorError::Draining));
                                continue;
                            }
                            if expected_runtime_generation != self.record.runtime_generation {
                                let _ = reply.send(Err(McpActorError::StaleGeneration {
                                    active: self.record.runtime_generation,
                                    observed: expected_runtime_generation,
                                }));
                                continue;
                            }
                            if expected_catalog_epoch != self.record.catalog_epoch {
                                let _ = reply.send(Err(McpActorError::StaleCatalogEpoch {
                                    active: self.record.catalog_epoch,
                                    observed: expected_catalog_epoch,
                                }));
                                continue;
                            }
                            if pending.len() >= self.config.max_in_flight_requests {
                                let _ = reply.send(Err(McpActorError::Backpressure));
                                continue;
                            }
                            let request_id = self.next_request_id.checked_add(1)
                                .ok_or(McpActorError::RequestIdExhausted)?;
                            self.next_request_id = request_id;
                            let request = McpSessionRequest {
                                request_id,
                                runtime_generation: self.record.runtime_generation,
                                catalog_epoch: self.record.catalog_epoch,
                                method,
                                params_json,
                            };
                            request.validate()?;
                            if let Err(error) = send_request_with_timeout(
                                &mut writer,
                                request,
                                self.config.transport_operation_timeout,
                            )
                            .await
                            {
                                let reason_code = error.reason_code().to_owned();
                                let _ = reply.send(Err(McpActorError::Transport(error)));
                                fail_pending(&mut pending, &reason_code);
                                close_transport(
                                    &mut writer,
                                    self.config.transport_operation_timeout,
                                )
                                .await;
                                return Ok(ConnectedOutcome::Reconnect(reason_code));
                            }
                            last_transport_activity = Instant::now();
                            self.record_transport_activity(McpTransportHealthState::Connected);
                            pending.insert(
                                request_id,
                                PendingRequest {
                                    runtime_generation: self.record.runtime_generation,
                                    callback_binding,
                                    deadline: Instant::now() + self.config.request_timeout,
                                    reply,
                                },
                            );
                        }
                        McpActorCommand::Snapshot { reply } => {
                            let _ = reply.send(self.snapshot(pending.len(), drain.is_some()));
                        }
                        McpActorCommand::AdvanceHostCatalog {
                            expected_runtime_generation,
                            expected_catalog_epoch,
                            catalog_digest,
                            reply,
                        } => {
                            if drain.is_some() {
                                let _ = reply.send(Err(McpActorError::Draining));
                                continue;
                            }
                            if expected_runtime_generation != self.record.runtime_generation {
                                let _ = reply.send(Err(McpActorError::StaleGeneration {
                                    active: self.record.runtime_generation,
                                    observed: expected_runtime_generation,
                                }));
                                continue;
                            }
                            if expected_catalog_epoch != self.record.catalog_epoch {
                                let _ = reply.send(Err(McpActorError::StaleCatalogEpoch {
                                    active: self.record.catalog_epoch,
                                    observed: expected_catalog_epoch,
                                }));
                                continue;
                            }
                            let next =
                                self.record.advance_catalog(Some(catalog_digest), now_unix_ms())?;
                            self.commit(next, "mcp.runtime.catalog.host_changed").await?;
                            let _ = reply.send(Ok(self.record.clone()));
                        }
                        McpActorCommand::Drain { timeout, reply } => {
                            if drain.is_some() {
                                let _ = reply.send(Err(McpActorError::Draining));
                                continue;
                            }
                            drain = Some(DrainState {
                                deadline: Instant::now() + timeout,
                                reply: Some(reply),
                                handles_dropped: false,
                            });
                            self.begin_drain_transition().await?;
                        }
                    }
                }
                event = reader.next_event() => {
                    let event = match event {
                        Ok(event) => event,
                        Err(error) => {
                            let reason_code = error.reason_code().to_owned();
                            if let Some(drain) = drain.as_mut() {
                                drain.deadline = Instant::now();
                                continue;
                            }
                            fail_pending(&mut pending, &reason_code);
                            close_transport(
                                &mut writer,
                                self.config.transport_operation_timeout,
                            )
                            .await;
                            return Ok(ConnectedOutcome::Reconnect(reason_code));
                        }
                    };
                    last_transport_activity = Instant::now();
                    self.record_transport_activity(
                        keepalive
                            .as_ref()
                            .map_or(McpTransportHealthState::Connected, |_| {
                                McpTransportHealthState::KeepalivePending
                            }),
                    );
                    if let Some(reason_code) =
                        self.handle_transport_event(
                            event,
                            &mut writer,
                            &mut pending,
                            &mut keepalive,
                        )
                        .await?
                    {
                        if let Some(drain) = drain.as_mut() {
                            drain.deadline = Instant::now();
                            continue;
                        }
                        fail_pending(&mut pending, &reason_code);
                        close_transport(&mut writer, self.config.transport_operation_timeout).await;
                        return Ok(ConnectedOutcome::Reconnect(reason_code));
                    }
                }
                _ = timer.tick() => {
                    expire_requests(&mut pending);
                    if drain.is_some() {
                        continue;
                    }
                    let now = Instant::now();
                    if keepalive.as_ref().is_some_and(|probe| probe.deadline <= now) {
                        self.record_keepalive_failure();
                        let reason_code = "mcp.runtime.keepalive.timeout".to_owned();
                        fail_pending(&mut pending, &reason_code);
                        close_transport(&mut writer, self.config.transport_operation_timeout).await;
                        return Ok(ConnectedOutcome::Reconnect(reason_code));
                    }
                    if keepalive.is_none()
                        && now.duration_since(last_transport_activity)
                            >= self.config.keepalive_interval
                    {
                        let request_id = self.next_request_id.checked_add(1)
                            .ok_or(McpActorError::RequestIdExhausted)?;
                        self.next_request_id = request_id;
                        let request = McpSessionRequest {
                            request_id,
                            runtime_generation: self.record.runtime_generation,
                            catalog_epoch: self.record.catalog_epoch,
                            method: "ping".to_owned(),
                            params_json: Value::Object(serde_json::Map::new()),
                        };
                        request.validate()?;
                        if let Err(error) = send_request_with_timeout(
                            &mut writer,
                            request,
                            self.config.transport_operation_timeout,
                        )
                        .await
                        {
                            self.record_keepalive_failure();
                            let reason_code = error.reason_code().to_owned();
                            fail_pending(&mut pending, &reason_code);
                            close_transport(
                                &mut writer,
                                self.config.transport_operation_timeout,
                            )
                            .await;
                            return Ok(ConnectedOutcome::Reconnect(reason_code));
                        }
                        keepalive = Some(KeepaliveProbe {
                            request_id,
                            deadline: now + self.config.keepalive_timeout,
                        });
                        last_transport_activity = now;
                        self.record_keepalive_started();
                    }
                }
            }
        }
    }

    async fn handle_transport_event(
        &mut self,
        event: McpTransportEvent,
        writer: &mut Box<dyn McpSessionWriter>,
        pending: &mut BTreeMap<u64, PendingRequest>,
        keepalive: &mut Option<KeepaliveProbe>,
    ) -> Result<Option<String>, McpActorError> {
        match event {
            McpTransportEvent::Response { request_id, runtime_generation, payload } => {
                if !valid_response_payload(&payload) {
                    return Ok(Some("mcp.runtime.response.invalid".to_owned()));
                }
                if keepalive.as_ref().is_some_and(|probe| probe.request_id == request_id) {
                    if runtime_generation != self.record.runtime_generation {
                        self.publish(McpActorNotification::StaleEventRejected {
                            active_runtime_generation: self.record.runtime_generation,
                            observed_runtime_generation: runtime_generation,
                        });
                        return Ok(None);
                    }
                    *keepalive = None;
                    return match payload {
                        McpResponsePayload::Success(_) => {
                            self.record_keepalive_success();
                            Ok(None)
                        }
                        McpResponsePayload::Error(_) => {
                            self.record_keepalive_failure();
                            Ok(Some("mcp.runtime.keepalive.rejected".to_owned()))
                        }
                    };
                }
                let Some(request) = pending.get(&request_id) else {
                    self.publish(McpActorNotification::LateResponseRejected {
                        request_id,
                        runtime_generation,
                    });
                    return Ok(None);
                };
                if runtime_generation != self.record.runtime_generation
                    || runtime_generation != request.runtime_generation
                {
                    self.publish(McpActorNotification::StaleEventRejected {
                        active_runtime_generation: self.record.runtime_generation,
                        observed_runtime_generation: runtime_generation,
                    });
                    return Ok(None);
                }
                let request =
                    pending.remove(&request_id).ok_or(McpActorError::ProtocolInvariant)?;
                let result = match payload {
                    McpResponsePayload::Success(value) => Ok(value),
                    McpResponsePayload::Error(error) => Err(McpActorError::Remote {
                        code: error.code,
                        safe_message: error.safe_message,
                    }),
                };
                let _ = request.reply.send(result);
            }
            McpTransportEvent::Notification { runtime_generation, notification } => {
                if runtime_generation != self.record.runtime_generation {
                    self.publish(McpActorNotification::StaleEventRejected {
                        active_runtime_generation: self.record.runtime_generation,
                        observed_runtime_generation: runtime_generation,
                    });
                    return Ok(None);
                }
                notification.validate()?;
                if let McpServerNotification::CatalogChanged { catalog_digest, .. } = &notification
                {
                    let next =
                        self.record.advance_catalog(catalog_digest.clone(), now_unix_ms())?;
                    if next.revision != self.record.revision {
                        self.commit(next, "mcp.runtime.catalog.changed").await?;
                        self.publish(McpActorNotification::CatalogEpochAdvanced {
                            runtime_generation: self.record.runtime_generation,
                            catalog_epoch: self.record.catalog_epoch,
                        });
                    }
                }
                self.publish(McpActorNotification::Server {
                    runtime_generation,
                    catalog_epoch: self.record.catalog_epoch,
                    notification,
                });
            }
            McpTransportEvent::Callback(mut request) => {
                // External frames cannot select which host authority receives a
                // callback. Callback-capable actors serialize tool calls, so the
                // sole in-flight request owns the exact host binding.
                let callback_binding = if pending.len() == 1 {
                    pending.values().next().and_then(|pending| pending.callback_binding.as_ref())
                } else {
                    None
                };
                let Some(callback_binding) = callback_binding else {
                    let reason_code = "mcp.runtime.callback.binding_unavailable".to_owned();
                    self.publish(McpActorNotification::CallbackRejected {
                        callback_id: request.callback_id,
                        reason_code: reason_code.clone(),
                    });
                    send_callback_with_timeout(
                        writer,
                        McpServerCallbackResponse {
                            callback_id: request.callback_id,
                            runtime_generation: self.record.runtime_generation,
                            payload: McpCallbackResponsePayload::Rejected {
                                reason_code,
                                safe_message: "callback has no unambiguous host invocation binding"
                                    .to_owned(),
                            },
                        },
                        self.config.transport_operation_timeout,
                    )
                    .await?;
                    return Ok(None);
                };
                request.principal_id = callback_binding.principal_id.clone();
                request.session_id = callback_binding.session_id.clone();
                request.origin = callback_binding.origin.clone();
                request.validate()?;
                if request.runtime_generation != self.record.runtime_generation
                    || request.catalog_epoch != self.record.catalog_epoch
                {
                    self.publish(McpActorNotification::StaleEventRejected {
                        active_runtime_generation: self.record.runtime_generation,
                        observed_runtime_generation: request.runtime_generation,
                    });
                    return Ok(None);
                }
                let callback_id = request.callback_id;
                let callback = tokio::time::timeout(
                    self.config.callback_timeout,
                    self.callbacks.handle_callback(&request),
                )
                .await;
                let payload = match callback {
                    Ok(Ok(payload)) => payload,
                    Ok(Err(McpHostCallbackError::Denied { reason_code, safe_message })) => {
                        self.publish(McpActorNotification::CallbackRejected {
                            callback_id,
                            reason_code: reason_code.clone(),
                        });
                        McpCallbackResponsePayload::Rejected { reason_code, safe_message }
                    }
                    Ok(Err(McpHostCallbackError::Unavailable { reason_code })) => {
                        self.publish(McpActorNotification::CallbackRejected {
                            callback_id,
                            reason_code: reason_code.clone(),
                        });
                        McpCallbackResponsePayload::Rejected {
                            reason_code,
                            safe_message: "host callback service unavailable".to_owned(),
                        }
                    }
                    Err(_) => {
                        let reason_code = "mcp.runtime.callback.timeout".to_owned();
                        self.publish(McpActorNotification::CallbackRejected {
                            callback_id,
                            reason_code: reason_code.clone(),
                        });
                        McpCallbackResponsePayload::Rejected {
                            reason_code,
                            safe_message: "host callback timed out".to_owned(),
                        }
                    }
                };
                let response = McpServerCallbackResponse {
                    callback_id,
                    runtime_generation: self.record.runtime_generation,
                    payload,
                };
                response.validate()?;
                if let Err(error) = send_callback_with_timeout(
                    writer,
                    response,
                    self.config.transport_operation_timeout,
                )
                .await
                {
                    return Ok(Some(error.reason_code().to_owned()));
                }
            }
            McpTransportEvent::Closed { reason_code } => {
                if !valid_reason_code(&reason_code) {
                    return Ok(Some("mcp.runtime.transport.invalid_close_reason".to_owned()));
                }
                return Ok(Some(reason_code));
            }
        }
        Ok(None)
    }

    async fn finish_drain_if_ready(
        &mut self,
        writer: &mut Box<dyn McpSessionWriter>,
        pending: &mut BTreeMap<u64, PendingRequest>,
        drain: &mut Option<DrainState>,
    ) -> Result<Option<McpActorExit>, McpActorError> {
        let Some(state) = drain.as_ref() else {
            return Ok(None);
        };
        if !pending.is_empty() && Instant::now() < state.deadline {
            return Ok(None);
        }
        let abandoned_requests = pending.len();
        for (_, request) in std::mem::take(pending) {
            let _ = request.reply.send(Err(McpActorError::Draining));
        }
        let transport_closed =
            close_transport(writer, self.config.transport_operation_timeout).await;
        if let Some(health) = self.transport_health.as_mut() {
            health.state = if transport_closed {
                McpTransportHealthState::Closed
            } else {
                McpTransportHealthState::Degraded
            };
            health.last_activity_at_unix_ms = now_unix_ms();
        }
        let stopped = self.record.transition(
            McpRuntimeLifecycleState::Stopped,
            now_unix_ms(),
            "mcp.runtime.session.stopped",
        )?;
        self.commit(stopped, "mcp.runtime.session.stopped").await?;
        let outcome = McpDrainOutcome { abandoned_requests, transport_closed };
        let state = drain.take().ok_or(McpActorError::ProtocolInvariant)?;
        if let Some(reply) = state.reply {
            let _ = reply.send(Ok(outcome));
        }
        let exit = if state.handles_dropped {
            McpActorExit::HandlesDropped(outcome)
        } else {
            McpActorExit::Drained(outcome)
        };
        Ok(Some(exit))
    }

    async fn wait_for_reconnect(&mut self) -> Result<Option<McpActorExit>, McpActorError> {
        let retry_at = self.record.next_retry_at_unix_ms.ok_or(McpActorError::ProtocolInvariant)?;
        let delay_ms = retry_at.saturating_sub(now_unix_ms());
        let delay = Duration::from_millis(u64::try_from(delay_ms).unwrap_or(0));
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => return Ok(None),
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        return self.stop_without_session(true, None).await.map(Some);
                    };
                    if let Some(exit) = self.handle_inactive_command(command).await? {
                        return Ok(Some(exit));
                    }
                }
            }
        }
    }

    async fn wait_while_inactive(&mut self) -> Result<Option<McpActorExit>, McpActorError> {
        loop {
            let Some(command) = self.commands.recv().await else {
                return self.stop_without_session(true, None).await.map(Some);
            };
            if let Some(exit) = self.handle_inactive_command(command).await? {
                return Ok(Some(exit));
            }
        }
    }

    async fn handle_inactive_command(
        &mut self,
        command: McpActorCommand,
    ) -> Result<Option<McpActorExit>, McpActorError> {
        match command {
            McpActorCommand::Request { reply, .. } => {
                let _ = reply.send(Err(McpActorError::Unavailable {
                    reason_code: lifecycle_unavailable_reason(self.record.lifecycle).to_owned(),
                }));
                Ok(None)
            }
            McpActorCommand::AdvanceHostCatalog { reply, .. } => {
                let _ = reply.send(Err(McpActorError::Unavailable {
                    reason_code: lifecycle_unavailable_reason(self.record.lifecycle).to_owned(),
                }));
                Ok(None)
            }
            McpActorCommand::Snapshot { reply } => {
                let _ = reply.send(self.snapshot(0, false));
                Ok(None)
            }
            McpActorCommand::Drain { timeout: _, reply } => {
                let exit = self.stop_without_session(false, Some(reply)).await?;
                Ok(Some(exit))
            }
        }
    }

    async fn stop_without_session(
        &mut self,
        handles_dropped: bool,
        reply: Option<oneshot::Sender<Result<McpDrainOutcome, McpActorError>>>,
    ) -> Result<McpActorExit, McpActorError> {
        if !matches!(
            self.record.lifecycle,
            McpRuntimeLifecycleState::Stopped | McpRuntimeLifecycleState::Disabled
        ) {
            let stopping = self.record.transition(
                McpRuntimeLifecycleState::Stopping,
                now_unix_ms(),
                "mcp.runtime.session.stopping",
            )?;
            self.commit(stopping, "mcp.runtime.session.stopping").await?;
            let stopped = self.record.transition(
                McpRuntimeLifecycleState::Stopped,
                now_unix_ms(),
                "mcp.runtime.session.stopped",
            )?;
            self.commit(stopped, "mcp.runtime.session.stopped").await?;
        }
        let outcome = McpDrainOutcome { abandoned_requests: 0, transport_closed: true };
        if let Some(reply) = reply {
            let _ = reply.send(Ok(outcome));
        }
        if let Some(health) = self.transport_health.as_mut() {
            health.state = McpTransportHealthState::Closed;
            health.last_activity_at_unix_ms = now_unix_ms();
        }
        Ok(if handles_dropped {
            McpActorExit::HandlesDropped(outcome)
        } else {
            McpActorExit::Drained(outcome)
        })
    }

    async fn begin_drain_transition(&mut self) -> Result<(), McpActorError> {
        if self.record.lifecycle == McpRuntimeLifecycleState::Ready {
            let stopping = self.record.transition(
                McpRuntimeLifecycleState::Stopping,
                now_unix_ms(),
                "mcp.runtime.session.stopping",
            )?;
            self.commit(stopping, "mcp.runtime.session.stopping").await?;
        }
        Ok(())
    }

    async fn record_transport_failure(&mut self, reason_code: &str) -> Result<(), McpActorError> {
        tracing::warn!(
            server_id = %self.record.server_id,
            runtime_generation = self.record.runtime_generation,
            reason_code,
            "persistent MCP transport entered reconnect"
        );
        let degraded = self.record.mark_degraded(reason_code, now_unix_ms())?;
        self.commit(degraded, reason_code).await?;
        let outcome = self.record.plan_reconnect(
            &self.config.reconnect_policy,
            reason_code,
            now_unix_ms(),
        )?;
        let next = outcome.record.clone();
        self.commit(next, reason_code).await?;
        self.reconnect_outcome = Some(outcome);
        if let Some(health) = self.transport_health.as_mut() {
            health.state = McpTransportHealthState::Degraded;
            health.last_activity_at_unix_ms = now_unix_ms();
        }
        Ok(())
    }

    async fn commit(
        &mut self,
        next: McpServerRecordV2,
        reason_code: &str,
    ) -> Result<(), McpActorError> {
        if next.revision == self.record.revision {
            return Ok(());
        }
        let event = McpRuntimeEventV2::from_transition(&self.record, &next, reason_code)?;
        self.store.persist_transition(self.record.revision, &next, &event).await?;
        self.record = next;
        self.callbacks.runtime_record_committed(&self.record);
        self.publish(McpActorNotification::LifecycleChanged(self.record.clone()));
        Ok(())
    }

    fn snapshot(&self, in_flight_requests: usize, draining: bool) -> McpActorSnapshot {
        McpActorSnapshot {
            record: self.record.clone(),
            in_flight_requests,
            dropped_notifications: self.dropped_notifications,
            draining,
            transport_health: self.transport_health.clone(),
            reconnect_outcome: self.reconnect_outcome.clone(),
        }
    }

    fn record_transport_activity(&mut self, state: McpTransportHealthState) {
        if let Some(health) = self.transport_health.as_mut() {
            health.state = state;
            health.last_activity_at_unix_ms = now_unix_ms();
        }
    }

    fn record_keepalive_started(&mut self) {
        if let Some(health) = self.transport_health.as_mut() {
            let now = now_unix_ms();
            health.state = McpTransportHealthState::KeepalivePending;
            health.last_activity_at_unix_ms = now;
            health.last_keepalive_at_unix_ms = Some(now);
        }
    }

    fn record_keepalive_success(&mut self) {
        if let Some(health) = self.transport_health.as_mut() {
            health.state = McpTransportHealthState::Connected;
            health.last_activity_at_unix_ms = now_unix_ms();
            health.successful_keepalives = health.successful_keepalives.saturating_add(1);
        }
    }

    fn record_keepalive_failure(&mut self) {
        if let Some(health) = self.transport_health.as_mut() {
            health.state = McpTransportHealthState::Degraded;
            health.last_activity_at_unix_ms = now_unix_ms();
            health.failed_keepalives = health.failed_keepalives.saturating_add(1);
        }
    }

    fn publish(&mut self, notification: McpActorNotification) {
        match self.notifications.try_send(notification) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped_notifications = self.dropped_notifications.saturating_add(1);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

/// Persistent MCP actor failure.
#[derive(Debug, Error)]
pub enum McpActorError {
    /// Actor configuration violates a required bound or lifecycle invariant.
    #[error("invalid mcp actor configuration: {reason_code}")]
    InvalidConfiguration {
        /// Stable configuration reason.
        reason_code: &'static str,
    },
    /// No Tokio runtime is available to own the actor.
    #[error("mcp actor requires an active tokio runtime")]
    RuntimeUnavailable,
    /// Actor owner task has stopped.
    #[error("mcp actor stopped")]
    ActorStopped,
    /// Runtime generation differs from the active owner.
    #[error("stale mcp runtime generation: active={active}, observed={observed}")]
    StaleGeneration {
        /// Active actor generation.
        active: u64,
        /// Caller-observed generation.
        observed: u64,
    },
    /// Catalog epoch differs from the active catalog.
    #[error("stale mcp catalog epoch: active={active}, observed={observed}")]
    StaleCatalogEpoch {
        /// Active actor epoch.
        active: u64,
        /// Caller-observed epoch.
        observed: u64,
    },
    /// Actor is not ready to accept requests.
    #[error("mcp actor unavailable: {reason_code}")]
    Unavailable {
        /// Stable lifecycle or transport reason.
        reason_code: String,
    },
    /// Bounded in-flight admission is full.
    #[error("mcp actor request capacity exhausted")]
    Backpressure,
    /// Request exceeded its configured response deadline.
    #[error("mcp actor request timed out")]
    RequestTimedOut,
    /// Actor is draining and no longer accepts work.
    #[error("mcp actor is draining")]
    Draining,
    /// Server returned a redaction-safe JSON-RPC error.
    #[error("mcp server returned error {code}: {safe_message}")]
    Remote {
        /// JSON-RPC error code.
        code: i64,
        /// Sanitized server error message.
        safe_message: String,
    },
    /// Request identifier space was exhausted.
    #[error("mcp actor request identifier exhausted")]
    RequestIdExhausted,
    /// Internal routing invariant failed.
    #[error("mcp actor protocol invariant failed")]
    ProtocolInvariant,
    /// Durable state transition failed.
    #[error(transparent)]
    Supervisor(#[from] McpRuntimeSupervisorError),
    /// Atomic persistence failed.
    #[error(transparent)]
    Store(#[from] McpRuntimeStoreError),
    /// Persistent transport failed.
    #[error(transparent)]
    Transport(#[from] McpTransportError),
}

fn validate_request_input(method: &str, params_json: &Value) -> Result<(), McpActorError> {
    let encoded_len = serde_json::to_vec(params_json)
        .map_err(|_| McpActorError::InvalidConfiguration {
            reason_code: "mcp.runtime.actor.request_json_invalid",
        })?
        .len();
    if method.trim().is_empty()
        || method.len() > MAX_METHOD_BYTES
        || !method.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
        || encoded_len > MAX_REQUEST_PAYLOAD_BYTES
    {
        return Err(McpActorError::InvalidConfiguration {
            reason_code: "mcp.runtime.actor.request_invalid",
        });
    }
    Ok(())
}

fn expire_requests(pending: &mut BTreeMap<u64, PendingRequest>) {
    let now = Instant::now();
    let expired = pending
        .iter()
        .filter_map(|(request_id, request)| (request.deadline <= now).then_some(*request_id))
        .collect::<Vec<_>>();
    for request_id in expired {
        if let Some(request) = pending.remove(&request_id) {
            let _ = request.reply.send(Err(McpActorError::RequestTimedOut));
        }
    }
}

fn fail_pending(pending: &mut BTreeMap<u64, PendingRequest>, reason_code: &str) {
    for (_, request) in std::mem::take(pending) {
        let _ = request
            .reply
            .send(Err(McpActorError::Unavailable { reason_code: reason_code.to_owned() }));
    }
}

async fn send_request_with_timeout(
    writer: &mut Box<dyn McpSessionWriter>,
    request: McpSessionRequest,
    timeout: Duration,
) -> Result<(), McpTransportError> {
    tokio::time::timeout(timeout, writer.send_request(request)).await.map_err(|_| {
        McpTransportError::Unavailable {
            reason_code: "mcp.runtime.transport.write_timeout".to_owned(),
        }
    })?
}

async fn send_callback_with_timeout(
    writer: &mut Box<dyn McpSessionWriter>,
    response: McpServerCallbackResponse,
    timeout: Duration,
) -> Result<(), McpTransportError> {
    tokio::time::timeout(timeout, writer.send_callback_response(response)).await.map_err(|_| {
        McpTransportError::Unavailable {
            reason_code: "mcp.runtime.transport.callback_write_timeout".to_owned(),
        }
    })?
}

async fn close_transport(writer: &mut Box<dyn McpSessionWriter>, timeout: Duration) -> bool {
    matches!(tokio::time::timeout(timeout, writer.close()).await, Ok(Ok(())))
}

fn duration_millis(duration: Duration) -> Result<u64, McpActorError> {
    u64::try_from(duration.as_millis()).map_err(|_| McpActorError::InvalidConfiguration {
        reason_code: "mcp.runtime.actor.duration_overflow",
    })
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(1)
        .max(1)
}

fn valid_reason_code(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 192
        && value.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-')
        })
}

fn valid_binding(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

fn valid_response_payload(payload: &McpResponsePayload) -> bool {
    match payload {
        McpResponsePayload::Success(value) => serde_json::to_vec(value)
            .is_ok_and(|encoded| encoded.len() <= MAX_REQUEST_PAYLOAD_BYTES),
        McpResponsePayload::Error(error) => {
            error.safe_message.len() <= 8 * 1024
                && error.data_json.as_ref().is_none_or(|value| {
                    serde_json::to_vec(value)
                        .is_ok_and(|encoded| encoded.len() <= MAX_REQUEST_PAYLOAD_BYTES)
                })
        }
    }
}

fn lifecycle_unavailable_reason(lifecycle: McpRuntimeLifecycleState) -> &'static str {
    match lifecycle {
        McpRuntimeLifecycleState::Configured => "mcp.runtime.configured",
        McpRuntimeLifecycleState::Starting => "mcp.runtime.starting",
        McpRuntimeLifecycleState::Handshaking => "mcp.runtime.handshaking",
        McpRuntimeLifecycleState::Ready => "mcp.runtime.ready",
        McpRuntimeLifecycleState::Degraded => "mcp.runtime.degraded",
        McpRuntimeLifecycleState::Reconnecting => "mcp.runtime.reconnecting",
        McpRuntimeLifecycleState::Stopping => "mcp.runtime.stopping",
        McpRuntimeLifecycleState::Stopped => "mcp.runtime.stopped",
        McpRuntimeLifecycleState::Quarantined => "mcp.runtime.quarantined",
        McpRuntimeLifecycleState::Disabled => "mcp.runtime.disabled",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::application::mcp_runtime::{
        McpConnectedSession, McpInitializeRequest, McpInitializeResult, McpProtocolCapabilities,
        McpResponsePayload, McpSessionTransportKind, McpTransportSession,
    };

    const CATALOG_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CATALOG_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    struct MemoryStore {
        record: Mutex<McpServerRecordV2>,
        events: Mutex<Vec<McpRuntimeEventV2>>,
    }

    #[async_trait]
    impl McpRuntimeRecordStore for MemoryStore {
        async fn load_all(&self) -> Result<Vec<McpServerRecordV2>, McpRuntimeStoreError> {
            Ok(vec![self.record.lock().expect("store mutex is healthy").clone()])
        }

        async fn insert_configured(
            &self,
            record: &McpServerRecordV2,
        ) -> Result<(), McpRuntimeStoreError> {
            if record.revision != 0 {
                return Err(McpRuntimeStoreError::Corrupt {
                    reason_code: "mcp.runtime.test.insert_nonzero_revision".to_owned(),
                });
            }
            *self.record.lock().expect("store mutex is healthy") = record.clone();
            Ok(())
        }

        async fn persist_transition(
            &self,
            expected_revision: u64,
            record: &McpServerRecordV2,
            event: &McpRuntimeEventV2,
        ) -> Result<(), McpRuntimeStoreError> {
            let mut current = self.record.lock().expect("store mutex is healthy");
            if current.revision != expected_revision {
                return Err(McpRuntimeStoreError::RevisionConflict {
                    expected: expected_revision,
                    actual: Some(current.revision),
                });
            }
            *current = record.clone();
            self.events.lock().expect("events mutex is healthy").push(event.clone());
            Ok(())
        }
    }

    struct FakeWriter {
        requests: mpsc::Sender<McpSessionRequest>,
        callbacks: mpsc::Sender<McpServerCallbackResponse>,
        close: mpsc::Sender<()>,
    }

    #[async_trait]
    impl McpSessionWriter for FakeWriter {
        async fn send_request(
            &mut self,
            request: McpSessionRequest,
        ) -> Result<(), McpTransportError> {
            self.requests.send(request).await.map_err(|_| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.test.request_receiver_closed".to_owned(),
            })
        }

        async fn send_callback_response(
            &mut self,
            response: McpServerCallbackResponse,
        ) -> Result<(), McpTransportError> {
            self.callbacks.send(response).await.map_err(|_| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.test.callback_receiver_closed".to_owned(),
            })
        }

        async fn close(&mut self) -> Result<(), McpTransportError> {
            let _ = self.close.send(()).await;
            Ok(())
        }
    }

    struct FakeReader {
        events: mpsc::Receiver<McpTransportEvent>,
    }

    #[async_trait]
    impl McpSessionReader for FakeReader {
        async fn next_event(&mut self) -> Result<McpTransportEvent, McpTransportError> {
            self.events.recv().await.ok_or_else(|| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.test.event_sender_closed".to_owned(),
            })
        }
    }

    struct FakeConnector {
        session: Mutex<Option<McpConnectedSession>>,
    }

    #[async_trait]
    impl McpSessionConnector for FakeConnector {
        async fn connect(
            &self,
            _request: &McpConnectRequest,
        ) -> Result<Box<dyn McpTransportSession>, McpTransportError> {
            self.session
                .lock()
                .expect("connector mutex is healthy")
                .take()
                .map(|session| Box::new(session) as Box<dyn McpTransportSession>)
                .ok_or_else(|| McpTransportError::Unavailable {
                    reason_code: "mcp.runtime.test.no_session".to_owned(),
                })
        }
    }

    struct RejectingCallbacks;

    #[async_trait]
    impl McpHostCallbackPort for RejectingCallbacks {
        async fn handle_callback(
            &self,
            _request: &McpServerCallbackRequest,
        ) -> Result<McpCallbackResponsePayload, McpHostCallbackError> {
            Err(McpHostCallbackError::Denied {
                reason_code: "mcp.runtime.test.callback_denied".to_owned(),
                safe_message: "callback denied by test policy".to_owned(),
            })
        }
    }

    struct Harness {
        parts: McpActorParts,
        requests: mpsc::Receiver<McpSessionRequest>,
        events: mpsc::Sender<McpTransportEvent>,
        closes: mpsc::Receiver<()>,
    }

    fn configured_record() -> McpServerRecordV2 {
        McpServerRecordV2::configured(
            "server-a".to_owned(),
            McpSessionTransportKind::Stdio,
            Some("vault-scope-a".to_owned()),
            "trusted-local".to_owned(),
            now_unix_ms(),
        )
        .expect("fixture record is valid")
    }

    fn actor_config(record: McpServerRecordV2) -> McpSessionActorConfig {
        McpSessionActorConfig {
            record,
            initialize: McpInitializeRequest {
                client_name: "palyra".to_owned(),
                client_version: "0.1.0".to_owned(),
                supported_protocol_versions: vec!["2025-06-18".to_owned()],
                capabilities: McpProtocolCapabilities {
                    sampling: true,
                    elicitation: true,
                    roots: true,
                    catalog_notifications: true,
                },
            },
            callback_binding: McpCallbackBinding {
                principal_id: "principal-a".to_owned(),
                session_id: "session-a".to_owned(),
                origin: "mcp:test".to_owned(),
            },
            command_queue_capacity: 8,
            notification_queue_capacity: 32,
            max_in_flight_requests: 4,
            request_timeout: Duration::from_secs(1),
            handshake_timeout: Duration::from_secs(1),
            callback_timeout: Duration::from_secs(1),
            transport_operation_timeout: Duration::from_secs(1),
            keepalive_interval: Duration::from_secs(60),
            keepalive_timeout: Duration::from_secs(1),
            default_drain_timeout: Duration::from_secs(1),
            reconnect_policy: McpReconnectPolicy::default(),
        }
    }

    fn spawn_harness() -> Harness {
        spawn_harness_with_keepalive(Duration::from_secs(60), Duration::from_secs(1))
    }

    fn spawn_harness_with_keepalive(
        keepalive_interval: Duration,
        keepalive_timeout: Duration,
    ) -> Harness {
        let record = configured_record();
        let store = Arc::new(MemoryStore {
            record: Mutex::new(record.clone()),
            events: Mutex::new(Vec::new()),
        });
        let (request_sender, request_receiver) = mpsc::channel(8);
        let (callback_sender, _callback_receiver) = mpsc::channel(8);
        let (close_sender, close_receiver) = mpsc::channel(1);
        let (event_sender, event_receiver) = mpsc::channel(8);
        let session = McpConnectedSession::new(
            McpInitializeResult {
                protocol_version: "2025-06-18".to_owned(),
                server_name: "test-server".to_owned(),
                server_version: "1.0.0".to_owned(),
                capabilities_json: serde_json::json!({"tools": {"listChanged": true}}),
                catalog_digest: CATALOG_A.to_owned(),
            },
            Box::new(FakeWriter {
                requests: request_sender,
                callbacks: callback_sender,
                close: close_sender,
            }),
            Box::new(FakeReader { events: event_receiver }),
        )
        .expect("fixture session is valid");
        let connector = Arc::new(FakeConnector { session: Mutex::new(Some(session)) });
        let mut config = actor_config(record);
        config.keepalive_interval = keepalive_interval;
        config.keepalive_timeout = keepalive_timeout;
        let parts = McpSessionActor::spawn(config, connector, store, Arc::new(RejectingCallbacks))
            .expect("actor starts");
        Harness { parts, requests: request_receiver, events: event_sender, closes: close_receiver }
    }

    #[tokio::test]
    async fn idle_session_ping_updates_transport_health() {
        let mut harness =
            spawn_harness_with_keepalive(Duration::from_millis(20), Duration::from_secs(1));
        let snapshot = ready_snapshot(&mut harness.parts).await;
        let ping = tokio::time::timeout(Duration::from_secs(1), harness.requests.recv())
            .await
            .expect("keepalive request is timely")
            .expect("keepalive reaches the transport");
        assert_eq!(ping.method, "ping");
        assert_eq!(ping.runtime_generation, snapshot.record.runtime_generation);
        assert_eq!(ping.catalog_epoch, snapshot.record.catalog_epoch);
        harness
            .events
            .send(McpTransportEvent::Response {
                request_id: ping.request_id,
                runtime_generation: ping.runtime_generation,
                payload: McpResponsePayload::Success(serde_json::json!({})),
            })
            .await
            .expect("keepalive response reaches actor");

        let health = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = harness.parts.handle.snapshot().await.expect("snapshot succeeds");
                if snapshot
                    .transport_health
                    .as_ref()
                    .is_some_and(|health| health.successful_keepalives == 1)
                {
                    break snapshot.transport_health.expect("health is present");
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("keepalive health update is timely");
        assert_eq!(health.state, McpTransportHealthState::Connected);
        assert_eq!(health.failed_keepalives, 0);

        harness.parts.handle.drain(Duration::from_secs(1)).await.expect("drain succeeds");
        harness.parts.join.await.expect("actor joins").expect("actor exits");
    }

    async fn ready_snapshot(parts: &mut McpActorParts) -> McpActorSnapshot {
        loop {
            let notification =
                tokio::time::timeout(Duration::from_secs(1), parts.notifications.recv())
                    .await
                    .expect("ready notification is timely")
                    .expect("notification channel remains open");
            if let McpActorNotification::LifecycleChanged(record) = notification {
                if record.lifecycle == McpRuntimeLifecycleState::Ready {
                    return parts.handle.snapshot().await.expect("snapshot succeeds");
                }
            }
        }
    }

    #[tokio::test]
    async fn actor_routes_responses_and_rejects_stale_generation() {
        let mut harness = spawn_harness();
        let snapshot = ready_snapshot(&mut harness.parts).await;
        assert_eq!(snapshot.record.catalog_epoch, 1);

        let stale = harness
            .parts
            .handle
            .request(
                snapshot.record.runtime_generation.saturating_sub(1),
                snapshot.record.catalog_epoch,
                "tools/call",
                serde_json::json!({}),
            )
            .await;
        assert!(matches!(stale, Err(McpActorError::StaleGeneration { .. })));

        let handle = harness.parts.handle.clone();
        let generation = snapshot.record.runtime_generation;
        let epoch = snapshot.record.catalog_epoch;
        let request = tokio::spawn(async move {
            handle
                .request(generation, epoch, "tools/call", serde_json::json!({"name": "read"}))
                .await
        });
        let routed = harness.requests.recv().await.expect("request reaches transport");
        harness
            .events
            .send(McpTransportEvent::Response {
                request_id: routed.request_id,
                runtime_generation: routed.runtime_generation,
                payload: McpResponsePayload::Success(serde_json::json!({"ok": true})),
            })
            .await
            .expect("response reaches actor");
        assert_eq!(
            request.await.expect("request task joins").expect("request succeeds"),
            serde_json::json!({"ok": true})
        );

        let drain =
            harness.parts.handle.drain(Duration::from_secs(1)).await.expect("drain succeeds");
        assert_eq!(drain.abandoned_requests, 0);
        harness.closes.recv().await.expect("transport closes");
        assert!(matches!(
            harness.parts.join.await.expect("actor joins").expect("actor exits"),
            McpActorExit::Drained(_)
        ));
    }

    #[tokio::test]
    async fn catalog_change_fences_prepared_requests() {
        let mut harness = spawn_harness();
        let snapshot = ready_snapshot(&mut harness.parts).await;
        harness
            .events
            .send(McpTransportEvent::Notification {
                runtime_generation: snapshot.record.runtime_generation,
                notification: McpServerNotification::CatalogChanged {
                    surface: "tools".to_owned(),
                    catalog_digest: Some(CATALOG_B.to_owned()),
                },
            })
            .await
            .expect("notification reaches actor");

        let advanced = loop {
            let notification = harness
                .parts
                .notifications
                .recv()
                .await
                .expect("notification channel remains open");
            if let McpActorNotification::CatalogEpochAdvanced { catalog_epoch, .. } = notification {
                break catalog_epoch;
            }
        };
        assert_eq!(advanced, snapshot.record.catalog_epoch + 1);
        let stale = harness
            .parts
            .handle
            .request(
                snapshot.record.runtime_generation,
                snapshot.record.catalog_epoch,
                "tools/call",
                serde_json::json!({}),
            )
            .await;
        assert!(matches!(stale, Err(McpActorError::StaleCatalogEpoch { .. })));

        harness.parts.handle.drain(Duration::from_secs(1)).await.expect("drain succeeds");
        harness.parts.join.await.expect("actor joins").expect("actor exits");
    }

    #[tokio::test]
    async fn host_catalog_advance_is_generation_and_epoch_fenced() {
        let mut harness = spawn_harness();
        let snapshot = ready_snapshot(&mut harness.parts).await;

        let stale = harness
            .parts
            .handle
            .advance_host_catalog(
                snapshot.record.runtime_generation,
                snapshot.record.catalog_epoch.saturating_add(1),
                CATALOG_B.to_owned(),
            )
            .await;
        assert!(matches!(stale, Err(McpActorError::StaleCatalogEpoch { .. })));

        let advanced = harness
            .parts
            .handle
            .advance_host_catalog(
                snapshot.record.runtime_generation,
                snapshot.record.catalog_epoch,
                CATALOG_B.to_owned(),
            )
            .await
            .expect("current host pin should advance the durable catalog");
        assert_eq!(advanced.runtime_generation, snapshot.record.runtime_generation);
        assert_eq!(advanced.catalog_epoch, snapshot.record.catalog_epoch + 1);
        assert_eq!(advanced.catalog_digest.as_deref(), Some(CATALOG_B));
        assert_eq!(advanced.revision, snapshot.record.revision + 1);

        let stale_generation = harness
            .parts
            .handle
            .advance_host_catalog(
                snapshot.record.runtime_generation.saturating_sub(1),
                advanced.catalog_epoch,
                CATALOG_A.to_owned(),
            )
            .await;
        assert!(matches!(stale_generation, Err(McpActorError::StaleGeneration { .. })));

        harness.parts.handle.drain(Duration::from_secs(1)).await.expect("drain succeeds");
        harness.parts.join.await.expect("actor joins").expect("actor exits");
    }

    #[tokio::test]
    async fn drain_deadline_rejects_unfinished_request() {
        let mut harness = spawn_harness();
        let snapshot = ready_snapshot(&mut harness.parts).await;
        let handle = harness.parts.handle.clone();
        let request = tokio::spawn(async move {
            handle
                .request(
                    snapshot.record.runtime_generation,
                    snapshot.record.catalog_epoch,
                    "tools/call",
                    serde_json::json!({}),
                )
                .await
        });
        harness.requests.recv().await.expect("request reaches transport");

        let drain = harness
            .parts
            .handle
            .drain(Duration::from_millis(30))
            .await
            .expect("forced drain succeeds");
        assert_eq!(drain.abandoned_requests, 1);
        assert!(matches!(request.await.expect("request task joins"), Err(McpActorError::Draining)));
        harness.parts.join.await.expect("actor joins").expect("actor exits");
    }
}
