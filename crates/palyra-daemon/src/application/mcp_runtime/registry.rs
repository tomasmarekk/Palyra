//! Process-local ownership registry for durable MCP server actors.
//!
//! Durable identity and lifecycle state remain in the journal. This registry
//! only prevents duplicate process-local owners, restores actors at startup,
//! and coordinates a bounded concurrent drain.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::future::join_all;
use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::{mpsc, Mutex as AsyncMutex},
    task::JoinHandle,
    time::{sleep, timeout_at, Instant},
};

use super::{
    McpActorError, McpActorExit, McpActorHandle, McpActorNotification, McpActorParts,
    McpCatalogAuthorityError, McpCatalogEpochPin, McpHostCallbackPort, McpRuntimeLifecycleState,
    McpRuntimeRecordStore, McpRuntimeStoreError, McpRuntimeSupervisor, McpRuntimeSupervisorError,
    McpServerRecordV2, McpSessionActor, McpSessionActorConfig, McpSessionConnector,
};

/// Host-prepared dependencies for one durable MCP actor.
pub struct McpActorLaunchPlan {
    config: McpSessionActorConfig,
    connector: Arc<dyn McpSessionConnector>,
    callbacks: Arc<dyn McpHostCallbackPort>,
}

impl McpActorLaunchPlan {
    /// Creates a launch plan from host-owned policy and transport dependencies.
    #[must_use]
    pub fn new(
        config: McpSessionActorConfig,
        connector: Arc<dyn McpSessionConnector>,
        callbacks: Arc<dyn McpHostCallbackPort>,
    ) -> Self {
        Self { config, connector, callbacks }
    }
}

/// Resolves sandboxed or egress-governed dependencies for a restored record.
#[async_trait]
pub trait McpActorRuntimeFactory: Send + Sync {
    /// Prepares a connector and callback boundary without starting a second owner.
    ///
    /// # Errors
    /// Returns a stable host reason when policy, credentials, process planning,
    /// or remote transport planning cannot prepare the actor.
    async fn prepare(
        &self,
        record: &McpServerRecordV2,
    ) -> Result<McpActorLaunchPlan, McpActorFactoryError>;
}

/// Host dependency preparation failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("mcp actor dependency preparation failed: {reason_code}")]
pub struct McpActorFactoryError {
    /// Stable redaction-safe host reason.
    pub reason_code: String,
}

/// One actor's terminal daemon-drain observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpActorDrainRecord {
    /// Durable server identity.
    pub server_id: String,
    /// Requests abandoned at the actor's drain deadline.
    pub abandoned_requests: usize,
    /// Whether the adapter confirmed transport cleanup.
    pub transport_closed: bool,
    /// Whether the registry had to abort the owner task.
    pub forced: bool,
    /// Stable failure reason when graceful drain or actor exit failed.
    pub failure_reason_code: Option<String>,
}

/// Aggregate bounded MCP drain evidence.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpActorRegistryDrainReport {
    /// True when an earlier drain already emptied the registry.
    pub already_drained: bool,
    /// Deterministically server-id-ordered actor outcomes.
    pub actors: Vec<McpActorDrainRecord>,
}

impl McpActorRegistryDrainReport {
    /// Returns whether every registered actor exited with confirmed cleanup.
    #[must_use]
    pub fn clean(&self) -> bool {
        self.actors.iter().all(|actor| {
            !actor.forced && actor.transport_closed && actor.failure_reason_code.is_none()
        })
    }

    /// Returns the total number of requests abandoned during drain.
    #[must_use]
    pub fn abandoned_requests(&self) -> usize {
        self.actors
            .iter()
            .fold(0_usize, |total, actor| total.saturating_add(actor.abandoned_requests))
    }
}

struct RegisteredActor {
    handle: McpActorHandle,
    notifications: Option<mpsc::Receiver<McpActorNotification>>,
    join: JoinHandle<Result<McpActorExit, McpActorError>>,
}

/// Production single-owner registry restored from durable MCP records.
pub struct McpActorRegistry {
    store: Arc<dyn McpRuntimeRecordStore>,
    factory: Arc<dyn McpActorRuntimeFactory>,
    actors: Mutex<BTreeMap<String, RegisteredActor>>,
    start_gate: AsyncMutex<()>,
    drain_gate: AsyncMutex<()>,
    draining: AtomicBool,
}

impl std::fmt::Debug for McpActorRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpActorRegistry")
            .field("actor_count", &self.len())
            .field("draining", &self.draining.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl McpActorRegistry {
    /// Restores every durable record and starts exactly one actor per server.
    ///
    /// Startup is fail-closed: invalid or duplicate records stop restoration.
    /// If dependency preparation fails after earlier actors started, those
    /// actors are bounded-drained before the error is returned.
    ///
    /// # Errors
    /// Returns [`McpActorRegistryError`] for corrupt durable state, dependency
    /// preparation failure, duplicate ownership, or actor startup failure.
    pub async fn restore_and_start(
        store: Arc<dyn McpRuntimeRecordStore>,
        factory: Arc<dyn McpActorRuntimeFactory>,
        rollback_timeout: Duration,
    ) -> Result<Self, McpActorRegistryError> {
        if rollback_timeout.is_zero() {
            return Err(McpActorRegistryError::InvalidDrainTimeout);
        }
        let supervisor = McpRuntimeSupervisor::restore(store.as_ref()).await?;
        let registry = Self {
            store,
            factory,
            actors: Mutex::new(BTreeMap::new()),
            start_gate: AsyncMutex::new(()),
            drain_gate: AsyncMutex::new(()),
            draining: AtomicBool::new(false),
        };
        for record in supervisor.records().cloned() {
            if record.lifecycle == McpRuntimeLifecycleState::Disabled {
                continue;
            }
            if let Err(error) = registry.start_record(record).await {
                let _ = registry.drain(rollback_timeout).await;
                return Err(error);
            }
        }
        Ok(registry)
    }

    /// Persists a revision-zero configuration and starts its actor.
    ///
    /// A preparation failure leaves the durable configured record available
    /// for explicit repair or a later startup restore.
    ///
    /// # Errors
    /// Returns a store, duplicate-owner, factory, or actor startup error.
    pub async fn configure_and_start(
        &self,
        record: McpServerRecordV2,
    ) -> Result<McpActorHandle, McpActorRegistryError> {
        self.store.insert_configured(&record).await?;
        self.start_record(record).await
    }

    /// Starts one actor from an already durable record.
    ///
    /// The start gate covers dependency preparation and insertion, preventing
    /// concurrent callers from creating two transport owners.
    ///
    /// # Errors
    /// Returns a drain-boundary, duplicate-owner, factory, plan, or actor error.
    pub async fn start_record(
        &self,
        record: McpServerRecordV2,
    ) -> Result<McpActorHandle, McpActorRegistryError> {
        let _start_guard = self.start_gate.lock().await;
        if self.draining.load(Ordering::Acquire) {
            return Err(McpActorRegistryError::Draining);
        }
        {
            let actors = self.actors.lock().map_err(|_| McpActorRegistryError::LockPoisoned)?;
            if actors.contains_key(&record.server_id) {
                return Err(McpActorRegistryError::DuplicateOwner { server_id: record.server_id });
            }
        }

        record.validate()?;
        let server_id = record.server_id.clone();
        let plan = self.factory.prepare(&record).await.map_err(|source| {
            McpActorRegistryError::Factory { server_id: server_id.clone(), source }
        })?;
        if plan.config.record != record {
            return Err(McpActorRegistryError::LaunchPlanRecordMismatch { server_id });
        }
        let McpActorParts { handle, notifications, join } = McpSessionActor::spawn(
            plan.config,
            plan.connector,
            Arc::clone(&self.store),
            plan.callbacks,
        )?;
        let registered =
            RegisteredActor { handle: handle.clone(), notifications: Some(notifications), join };
        let mut actors = self.actors.lock().map_err(|_| McpActorRegistryError::LockPoisoned)?;
        match actors.entry(server_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(registered);
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                registered.join.abort();
                return Err(McpActorRegistryError::DuplicateOwner { server_id });
            }
        }
        Ok(handle)
    }

    /// Returns the actor handle for one durable server.
    pub fn handle(&self, server_id: &str) -> Option<McpActorHandle> {
        self.actors.lock().ok()?.get(server_id).map(|actor| actor.handle.clone())
    }

    /// Waits until one restored actor publishes a ready catalog.
    ///
    /// The bounded poll observes the actor's durable projection instead of
    /// introducing a second readiness signal that could race lifecycle CAS
    /// persistence.
    ///
    /// # Errors
    /// Returns an unknown-server, actor, or readiness-timeout error.
    pub async fn wait_until_ready(
        &self,
        server_id: &str,
        timeout: Duration,
    ) -> Result<McpCatalogEpochPin, McpActorRegistryError> {
        if timeout.is_zero() {
            return Err(McpActorRegistryError::ReadyTimeout { server_id: server_id.to_owned() });
        }
        let handle = self.handle(server_id).ok_or_else(|| {
            McpActorRegistryError::UnknownServer { server_id: server_id.to_owned() }
        })?;
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = handle.snapshot().await?;
            if snapshot.record.lifecycle == McpRuntimeLifecycleState::Ready {
                return McpCatalogEpochPin::from_ready_record(&snapshot.record).map_err(Into::into);
            }
            if Instant::now() >= deadline {
                return Err(McpActorRegistryError::ReadyTimeout {
                    server_id: server_id.to_owned(),
                });
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Transfers the actor notification stream to the single refresh owner.
    ///
    /// Returning `None` after the first call prevents duplicate catalog
    /// refresh pumps for one process-local actor.
    pub fn take_notifications(
        &self,
        server_id: &str,
    ) -> Option<mpsc::Receiver<McpActorNotification>> {
        self.actors.lock().ok()?.get_mut(server_id)?.notifications.take()
    }

    /// Captures the current generation, epoch, digest, and durable revision.
    ///
    /// # Errors
    /// Returns an unknown-server, stopped-actor, or non-ready catalog error.
    pub async fn catalog_pin(
        &self,
        server_id: &str,
    ) -> Result<McpCatalogEpochPin, McpActorRegistryError> {
        let handle = self.handle(server_id).ok_or_else(|| {
            McpActorRegistryError::UnknownServer { server_id: server_id.to_owned() }
        })?;
        let snapshot = handle.snapshot().await?;
        McpCatalogEpochPin::from_ready_record(&snapshot.record).map_err(Into::into)
    }

    /// Advances a host-authored catalog only when the complete supplied pin
    /// still identifies the active actor.
    pub async fn advance_host_catalog(
        &self,
        pin: &McpCatalogEpochPin,
        catalog_digest: String,
    ) -> Result<McpCatalogEpochPin, McpActorRegistryError> {
        let handle = self.handle(&pin.server_id).ok_or_else(|| {
            McpActorRegistryError::UnknownServer { server_id: pin.server_id.clone() }
        })?;
        let current = self.catalog_pin(&pin.server_id).await?;
        if current != *pin {
            return Err(McpActorRegistryError::StaleCatalogPin {
                server_id: pin.server_id.clone(),
            });
        }
        let record = handle
            .advance_host_catalog(pin.runtime_generation, pin.catalog_epoch, catalog_digest)
            .await?;
        McpCatalogEpochPin::from_ready_record(&record).map_err(Into::into)
    }

    /// Routes one request only if its complete catalog pin remains current.
    ///
    /// The actor rechecks generation and epoch at admission, closing the race
    /// between this digest/revision comparison and command processing.
    ///
    /// # Errors
    /// Returns an unknown-server, stale-pin, actor, or transport error.
    pub async fn request_pinned(
        &self,
        pin: &McpCatalogEpochPin,
        method: impl Into<String>,
        params_json: Value,
    ) -> Result<Value, McpActorRegistryError> {
        self.request_pinned_with_callback_binding(pin, method, params_json, None).await
    }

    /// Routes one request with a host-owned binding for any nested callbacks.
    pub async fn request_pinned_with_callback_binding(
        &self,
        pin: &McpCatalogEpochPin,
        method: impl Into<String>,
        params_json: Value,
        callback_binding: Option<super::McpCallbackBinding>,
    ) -> Result<Value, McpActorRegistryError> {
        let handle = self.handle(&pin.server_id).ok_or_else(|| {
            McpActorRegistryError::UnknownServer { server_id: pin.server_id.clone() }
        })?;
        let snapshot = handle.snapshot().await?;
        let current = McpCatalogEpochPin::from_ready_record(&snapshot.record)?;
        if current != *pin {
            return Err(McpActorRegistryError::StaleCatalogPin {
                server_id: pin.server_id.clone(),
            });
        }
        handle
            .request_with_callback_binding(
                pin.runtime_generation,
                pin.catalog_epoch,
                method,
                params_json,
                callback_binding,
            )
            .await
            .map_err(Into::into)
    }

    /// Returns active server identities in deterministic order.
    pub fn server_ids(&self) -> Vec<String> {
        self.actors.lock().map(|actors| actors.keys().cloned().collect()).unwrap_or_default()
    }

    /// Returns the number of process-local owners.
    #[must_use]
    pub fn len(&self) -> usize {
        self.actors.lock().map_or(0, |actors| actors.len())
    }

    /// Returns whether no process-local MCP actors are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stops admission and concurrently drains every registered actor.
    ///
    /// The single deadline covers actor drain and owner-task join. Any owner
    /// that misses the deadline is explicitly aborted and awaited so no task
    /// is silently detached.
    pub async fn drain(&self, timeout: Duration) -> McpActorRegistryDrainReport {
        if timeout.is_zero() {
            return McpActorRegistryDrainReport {
                already_drained: false,
                actors: vec![McpActorDrainRecord {
                    server_id: "registry".to_owned(),
                    abandoned_requests: 0,
                    transport_closed: false,
                    forced: false,
                    failure_reason_code: Some(
                        "mcp.runtime.registry.invalid_drain_timeout".to_owned(),
                    ),
                }],
            };
        }
        let _drain_guard = self.drain_gate.lock().await;
        if self.draining.swap(true, Ordering::AcqRel) {
            return McpActorRegistryDrainReport { already_drained: true, actors: Vec::new() };
        }
        let mut actors = match self.actors.lock() {
            Ok(mut registered) => std::mem::take(&mut *registered),
            Err(_) => {
                return McpActorRegistryDrainReport {
                    already_drained: false,
                    actors: vec![McpActorDrainRecord {
                        server_id: "registry".to_owned(),
                        abandoned_requests: 0,
                        transport_closed: false,
                        forced: false,
                        failure_reason_code: Some("mcp.runtime.registry.lock_poisoned".to_owned()),
                    }],
                };
            }
        };
        if actors.is_empty() {
            return McpActorRegistryDrainReport::default();
        }

        let deadline = Instant::now() + timeout;
        let handles = actors
            .iter()
            .map(|(server_id, actor)| (server_id.clone(), actor.handle.clone()))
            .collect::<Vec<_>>();
        let drain_futures =
            handles.iter().map(|(_, handle)| handle.drain(timeout)).collect::<Vec<_>>();
        let drain_results = timeout_at(deadline, join_all(drain_futures)).await;
        let mut records = handles
            .iter()
            .map(|(server_id, _)| {
                (
                    server_id.clone(),
                    McpActorDrainRecord {
                        server_id: server_id.clone(),
                        abandoned_requests: 0,
                        transport_closed: false,
                        forced: false,
                        failure_reason_code: None,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        match drain_results {
            Ok(results) => {
                for ((server_id, _), result) in handles.iter().zip(results) {
                    let record = records
                        .get_mut(server_id)
                        .expect("drain record is initialized for every actor");
                    match result {
                        Ok(outcome) => {
                            record.abandoned_requests = outcome.abandoned_requests;
                            record.transport_closed = outcome.transport_closed;
                            if !outcome.transport_closed {
                                record.failure_reason_code =
                                    Some("mcp.runtime.registry.transport_close_failed".to_owned());
                            }
                        }
                        Err(_) => {
                            record.failure_reason_code =
                                Some("mcp.runtime.registry.actor_drain_failed".to_owned());
                        }
                    }
                }
            }
            Err(_) => {
                for record in records.values_mut() {
                    record.forced = true;
                    record.failure_reason_code =
                        Some("mcp.runtime.registry.drain_deadline_exceeded".to_owned());
                }
            }
        }

        let joined = if Instant::now() < deadline {
            let joins = actors.values_mut().map(|actor| &mut actor.join).collect::<Vec<_>>();
            timeout_at(deadline, join_all(joins)).await.ok()
        } else {
            None
        };
        if let Some(joined) = joined {
            for ((server_id, _), result) in actors.iter().zip(joined) {
                if result.as_ref().is_ok_and(|exit| exit.is_ok()) {
                    continue;
                }
                let record =
                    records.get_mut(server_id).expect("join record is initialized for every actor");
                record.failure_reason_code =
                    Some("mcp.runtime.registry.actor_exit_failed".to_owned());
            }
        } else {
            let unfinished = actors
                .iter()
                .filter_map(|(server_id, actor)| {
                    (!actor.join.is_finished()).then_some(server_id.clone())
                })
                .collect::<BTreeSet<_>>();
            for (server_id, actor) in &actors {
                if unfinished.contains(server_id) {
                    actor.join.abort();
                    let record = records
                        .get_mut(server_id)
                        .expect("forced record is initialized for every actor");
                    record.forced = true;
                    record.failure_reason_code =
                        Some("mcp.runtime.registry.owner_abort_required".to_owned());
                }
            }
            let _ = join_all(actors.values_mut().map(|actor| &mut actor.join)).await;
        }

        McpActorRegistryDrainReport {
            already_drained: false,
            actors: records.into_values().collect(),
        }
    }
}

/// MCP actor registry startup or ownership failure.
#[derive(Debug, Error)]
pub enum McpActorRegistryError {
    /// Registry drain deadline was zero.
    #[error("mcp actor registry drain timeout must be non-zero")]
    InvalidDrainTimeout,
    /// Registry state lock was poisoned.
    #[error("mcp actor registry lock poisoned")]
    LockPoisoned,
    /// Daemon drain has closed actor admission.
    #[error("mcp actor registry is draining")]
    Draining,
    /// A process-local owner already exists for the server.
    #[error("duplicate mcp actor owner for server {server_id}")]
    DuplicateOwner {
        /// Durable server identity.
        server_id: String,
    },
    /// No process-local owner exists for a durable server.
    #[error("unknown mcp actor server {server_id}")]
    UnknownServer {
        /// Durable server identity.
        server_id: String,
    },
    /// Catalog digest, epoch, generation, or durable revision changed.
    #[error("stale mcp catalog pin for server {server_id}")]
    StaleCatalogPin {
        /// Durable server identity.
        server_id: String,
    },
    /// The actor did not publish a ready catalog before the bounded startup deadline.
    #[error("mcp actor server {server_id} did not become ready before its deadline")]
    ReadyTimeout {
        /// Durable server identity.
        server_id: String,
    },
    /// Factory returned dependencies bound to a different durable record.
    #[error("mcp actor launch plan record mismatch for server {server_id}")]
    LaunchPlanRecordMismatch {
        /// Durable server identity.
        server_id: String,
    },
    /// Host dependency preparation failed.
    #[error("failed to prepare mcp actor for server {server_id}")]
    Factory {
        /// Durable server identity.
        server_id: String,
        /// Stable host factory failure.
        #[source]
        source: McpActorFactoryError,
    },
    /// Catalog authority validation failed.
    #[error(transparent)]
    Catalog(#[from] McpCatalogAuthorityError),
    /// Durable supervisor restoration failed.
    #[error(transparent)]
    Supervisor(#[from] McpRuntimeSupervisorError),
    /// Durable insert failed.
    #[error(transparent)]
    Store(#[from] McpRuntimeStoreError),
    /// Actor validation or startup failed.
    #[error(transparent)]
    Actor(#[from] McpActorError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::mcp_runtime::{
        McpCallbackBinding, McpCallbackResponsePayload, McpConnectRequest, McpHostCallbackError,
        McpInitializeRequest, McpProtocolCapabilities, McpReconnectPolicy, McpRuntimeEventV2,
        McpRuntimeLifecycleState, McpServerCallbackRequest, McpSessionTransportKind,
        McpTransportError,
    };

    struct MemoryStore {
        records: Mutex<Vec<McpServerRecordV2>>,
    }

    #[async_trait]
    impl McpRuntimeRecordStore for MemoryStore {
        async fn load_all(&self) -> Result<Vec<McpServerRecordV2>, McpRuntimeStoreError> {
            Ok(self.records.lock().expect("store lock should be healthy").clone())
        }

        async fn insert_configured(
            &self,
            record: &McpServerRecordV2,
        ) -> Result<(), McpRuntimeStoreError> {
            self.records.lock().expect("store lock should be healthy").push(record.clone());
            Ok(())
        }

        async fn persist_transition(
            &self,
            expected_revision: u64,
            record: &McpServerRecordV2,
            _event: &McpRuntimeEventV2,
        ) -> Result<(), McpRuntimeStoreError> {
            let mut records = self.records.lock().expect("store lock should be healthy");
            let current = records
                .iter_mut()
                .find(|candidate| candidate.server_id == record.server_id)
                .ok_or(McpRuntimeStoreError::RevisionConflict {
                    expected: expected_revision,
                    actual: None,
                })?;
            if current.revision != expected_revision {
                return Err(McpRuntimeStoreError::RevisionConflict {
                    expected: expected_revision,
                    actual: Some(current.revision),
                });
            }
            *current = record.clone();
            Ok(())
        }
    }

    struct UnusedConnector;

    #[async_trait]
    impl McpSessionConnector for UnusedConnector {
        async fn connect(
            &self,
            _request: &McpConnectRequest,
        ) -> Result<Box<dyn super::super::McpTransportSession>, McpTransportError> {
            Err(McpTransportError::Unavailable {
                reason_code: "mcp.runtime.test.connector_unused".to_owned(),
            })
        }
    }

    struct DenyCallbacks;

    #[async_trait]
    impl McpHostCallbackPort for DenyCallbacks {
        async fn handle_callback(
            &self,
            _request: &McpServerCallbackRequest,
        ) -> Result<McpCallbackResponsePayload, McpHostCallbackError> {
            Err(McpHostCallbackError::Denied {
                reason_code: "mcp.runtime.test.callback_denied".to_owned(),
                safe_message: "denied".to_owned(),
            })
        }
    }

    struct Factory {
        prepared: Mutex<usize>,
    }

    #[async_trait]
    impl McpActorRuntimeFactory for Factory {
        async fn prepare(
            &self,
            record: &McpServerRecordV2,
        ) -> Result<McpActorLaunchPlan, McpActorFactoryError> {
            *self.prepared.lock().expect("factory lock should be healthy") += 1;
            Ok(McpActorLaunchPlan::new(
                actor_config(record.clone()),
                Arc::new(UnusedConnector),
                Arc::new(DenyCallbacks),
            ))
        }
    }

    fn disabled_record(server_id: &str) -> McpServerRecordV2 {
        configured_record(server_id)
            .transition(McpRuntimeLifecycleState::Disabled, 1_001, "mcp.runtime.test.disabled")
            .expect("disabled transition should validate")
    }

    fn configured_record(server_id: &str) -> McpServerRecordV2 {
        McpServerRecordV2::configured(
            server_id.to_owned(),
            McpSessionTransportKind::Stdio,
            None,
            "trusted-local".to_owned(),
            1_000,
        )
        .expect("configured record should validate")
    }

    fn actor_config(record: McpServerRecordV2) -> McpSessionActorConfig {
        McpSessionActorConfig {
            record,
            initialize: McpInitializeRequest {
                client_name: "palyra".to_owned(),
                client_version: "test".to_owned(),
                supported_protocol_versions: vec!["2025-06-18".to_owned()],
                capabilities: McpProtocolCapabilities::default(),
            },
            callback_binding: McpCallbackBinding {
                principal_id: "principal-a".to_owned(),
                session_id: "session-a".to_owned(),
                origin: "mcp:test".to_owned(),
            },
            command_queue_capacity: 4,
            notification_queue_capacity: 4,
            max_in_flight_requests: 2,
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

    #[tokio::test]
    async fn startup_restores_one_owner_and_daemon_drain_is_idempotent() {
        let store = Arc::new(MemoryStore {
            records: Mutex::new(vec![configured_record("server-a"), configured_record("server-b")]),
        });
        let factory = Arc::new(Factory { prepared: Mutex::new(0) });
        let registry =
            McpActorRegistry::restore_and_start(store, factory.clone(), Duration::from_secs(1))
                .await
                .expect("registry should restore");
        assert_eq!(registry.server_ids(), vec!["server-a".to_owned(), "server-b".to_owned()]);
        assert_eq!(*factory.prepared.lock().expect("factory lock should be healthy"), 2);

        let duplicate = registry.start_record(configured_record("server-a")).await;
        assert!(matches!(
            duplicate,
            Err(McpActorRegistryError::DuplicateOwner { server_id })
                if server_id == "server-a"
        ));

        let report = registry.drain(Duration::from_secs(1)).await;
        assert!(report.clean());
        assert_eq!(report.actors.len(), 2);
        assert!(registry.is_empty());
        assert!(registry.drain(Duration::from_secs(1)).await.already_drained);
    }

    async fn run_actor_capacity_cycle(actor_count: usize) -> usize {
        let records = (0..actor_count)
            .map(|index| configured_record(format!("server-{index}").as_str()))
            .collect();
        let store = Arc::new(MemoryStore { records: Mutex::new(records) });
        let factory = Arc::new(Factory { prepared: Mutex::new(0) });
        let registry =
            McpActorRegistry::restore_and_start(store, factory.clone(), Duration::from_secs(1))
                .await
                .expect("capacity cycle should restore every actor");

        assert_eq!(registry.len(), actor_count);
        assert_eq!(*factory.prepared.lock().expect("factory lock should be healthy"), actor_count);
        for server_id in registry.server_ids() {
            let snapshot = registry
                .handle(server_id.as_str())
                .expect("capacity actor should remain registered")
                .snapshot()
                .await
                .expect("capacity actor should answer a bounded snapshot request");
            assert_eq!(snapshot.record.server_id, server_id);
            assert!(!snapshot.draining);
        }

        let report = registry.drain(Duration::from_secs(1)).await;
        assert!(report.clean(), "actor drain report: {report:?}");
        assert_eq!(report.actors.len(), actor_count);
        assert!(registry.is_empty());
        report
            .actors
            .iter()
            .filter(|actor| {
                actor.forced || !actor.transport_closed || actor.failure_reason_code.is_some()
            })
            .count()
    }

    #[tokio::test]
    async fn capacity_soak_drains_actor_fleet_without_orphans_across_restarts() {
        const LONG_LIVED_ACTOR_CAPACITY: usize = 128;
        const RESTART_CYCLES: usize = 32;
        const ACTORS_PER_RESTART: usize = 4;

        let mut orphaned_actors = run_actor_capacity_cycle(LONG_LIVED_ACTOR_CAPACITY).await;
        for _ in 0..RESTART_CYCLES {
            orphaned_actors += run_actor_capacity_cycle(ACTORS_PER_RESTART).await;
        }

        assert_eq!(orphaned_actors, 0);
    }

    #[tokio::test]
    async fn disabled_records_restore_without_starting_actor_owners() {
        let store =
            Arc::new(MemoryStore { records: Mutex::new(vec![disabled_record("server-a")]) });
        let factory = Arc::new(Factory { prepared: Mutex::new(0) });
        let registry =
            McpActorRegistry::restore_and_start(store, factory.clone(), Duration::from_secs(1))
                .await
                .expect("disabled durable state should restore");

        assert!(registry.is_empty());
        assert_eq!(*factory.prepared.lock().expect("factory lock should be healthy"), 0);
    }

    #[tokio::test]
    async fn duplicate_durable_records_fail_before_any_actor_starts() {
        let record = disabled_record("server-a");
        let store = Arc::new(MemoryStore { records: Mutex::new(vec![record.clone(), record]) });
        let factory = Arc::new(Factory { prepared: Mutex::new(0) });
        let result =
            McpActorRegistry::restore_and_start(store, factory.clone(), Duration::from_secs(1))
                .await;
        assert!(matches!(
            result,
            Err(McpActorRegistryError::Supervisor(
                McpRuntimeSupervisorError::DuplicateServer { .. }
            ))
        ));
        assert_eq!(*factory.prepared.lock().expect("factory lock should be healthy"), 0);
    }
}
