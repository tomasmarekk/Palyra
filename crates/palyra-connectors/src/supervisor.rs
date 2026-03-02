use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::json;
use thiserror::Error;

use crate::{
    protocol::{
        ConnectorInstanceSpec, ConnectorKind, ConnectorLiveness, ConnectorReadiness,
        ConnectorStatusSnapshot, DeliveryOutcome, InboundMessageEvent, OutboundMessageRequest,
        RetryClass, RouteInboundResult,
    },
    storage::{ConnectorStore, ConnectorStoreError, OutboxEntryRecord},
};

#[derive(Debug, Clone)]
pub struct ConnectorSupervisorConfig {
    pub inbound_dedupe_window_ms: i64,
    pub max_inbound_body_bytes: usize,
    pub max_outbound_body_bytes: usize,
    pub max_retry_attempts: u32,
    pub min_retry_delay_ms: u64,
    pub base_retry_delay_ms: u64,
    pub max_retry_delay_ms: u64,
    pub disabled_poll_delay_ms: u64,
    pub immediate_drain_batch_size: usize,
    pub background_drain_batch_size: usize,
}

impl Default for ConnectorSupervisorConfig {
    fn default() -> Self {
        Self {
            inbound_dedupe_window_ms: 7 * 24 * 60 * 60 * 1_000,
            max_inbound_body_bytes: 64 * 1024,
            max_outbound_body_bytes: 64 * 1024,
            max_retry_attempts: 5,
            min_retry_delay_ms: 250,
            base_retry_delay_ms: 1_000,
            max_retry_delay_ms: 60_000,
            disabled_poll_delay_ms: 30_000,
            immediate_drain_batch_size: 64,
            background_drain_batch_size: 128,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispatchResult {
    Delivered,
    Retried,
    DeadLettered,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DrainOutcome {
    pub processed: usize,
    pub delivered: usize,
    pub retried: usize,
    pub dead_lettered: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct InboundIngestOutcome {
    pub accepted: bool,
    pub duplicate: bool,
    pub queued_for_retry: bool,
    pub decision_reason: String,
    pub route_key: Option<String>,
    pub enqueued_outbound: usize,
    pub immediate_delivery: usize,
}

#[derive(Debug, Error)]
pub enum ConnectorRouterError {
    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Error)]
pub enum ConnectorAdapterError {
    #[error("{0}")]
    Backend(String),
}

#[async_trait]
pub trait ConnectorRouter: Send + Sync {
    async fn route_inbound(
        &self,
        principal: &str,
        event: &InboundMessageEvent,
    ) -> Result<RouteInboundResult, ConnectorRouterError>;
}

#[async_trait]
pub trait ConnectorAdapter: Send + Sync {
    fn kind(&self) -> ConnectorKind;

    async fn send_outbound(
        &self,
        request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError>;
}

#[derive(Debug, Error)]
pub enum ConnectorSupervisorError {
    #[error(transparent)]
    Store(#[from] ConnectorStoreError),
    #[error("connector protocol validation failed: {0}")]
    Validation(String),
    #[error("connector instance not found: {0}")]
    NotFound(String),
    #[error("connector adapter missing for kind '{0}'")]
    MissingAdapter(ConnectorKind),
    #[error("router failed: {0}")]
    Router(String),
    #[error("adapter failed: {0}")]
    Adapter(String),
    #[error("failed to read system clock: {0}")]
    Clock(String),
}

pub struct ConnectorSupervisor {
    store: Arc<ConnectorStore>,
    router: Arc<dyn ConnectorRouter>,
    adapters: HashMap<ConnectorKind, Arc<dyn ConnectorAdapter>>,
    config: ConnectorSupervisorConfig,
}

impl ConnectorSupervisor {
    #[must_use]
    pub fn new(
        store: Arc<ConnectorStore>,
        router: Arc<dyn ConnectorRouter>,
        adapters: Vec<Arc<dyn ConnectorAdapter>>,
        config: ConnectorSupervisorConfig,
    ) -> Self {
        let adapters = adapters
            .into_iter()
            .map(|adapter| (adapter.kind(), adapter))
            .collect::<HashMap<_, _>>();
        Self { store, router, adapters, config }
    }

    #[must_use]
    pub fn store(&self) -> &Arc<ConnectorStore> {
        &self.store
    }

    pub fn register_connector(
        &self,
        spec: &ConnectorInstanceSpec,
    ) -> Result<(), ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.upsert_instance(spec, now)?;
        self.store.record_event(
            spec.connector_id.as_str(),
            "connector.registered",
            "info",
            "connector instance registered",
            Some(&json!({
                "connector_id": spec.connector_id,
                "kind": spec.kind.as_str(),
                "enabled": spec.enabled,
            })),
            now,
        )?;
        Ok(())
    }

    pub fn set_enabled(
        &self,
        connector_id: &str,
        enabled: bool,
    ) -> Result<ConnectorStatusSnapshot, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.set_instance_enabled(connector_id, enabled, now)?;
        self.store.record_event(
            connector_id,
            "connector.enabled_changed",
            "info",
            if enabled { "connector enabled" } else { "connector disabled" },
            Some(&json!({ "enabled": enabled })),
            now,
        )?;
        self.status(connector_id)
    }

    pub fn status(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorStatusSnapshot, ConnectorSupervisorError> {
        let Some(instance) = self.store.get_instance(connector_id)? else {
            return Err(ConnectorSupervisorError::NotFound(connector_id.to_owned()));
        };
        let queue_depth = self.store.queue_depth(connector_id)?;
        Ok(ConnectorStatusSnapshot {
            connector_id: instance.connector_id,
            kind: instance.kind,
            principal: instance.principal,
            enabled: instance.enabled,
            readiness: instance.readiness,
            liveness: instance.liveness,
            restart_count: instance.restart_count,
            queue_depth,
            last_error: instance.last_error,
            last_inbound_unix_ms: instance.last_inbound_unix_ms,
            last_outbound_unix_ms: instance.last_outbound_unix_ms,
            updated_at_unix_ms: instance.updated_at_unix_ms,
        })
    }

    pub fn list_status(&self) -> Result<Vec<ConnectorStatusSnapshot>, ConnectorSupervisorError> {
        let instances = self.store.list_instances()?;
        let mut snapshots = Vec::with_capacity(instances.len());
        for instance in instances {
            let queue_depth = self.store.queue_depth(instance.connector_id.as_str())?;
            snapshots.push(ConnectorStatusSnapshot {
                connector_id: instance.connector_id,
                kind: instance.kind,
                principal: instance.principal,
                enabled: instance.enabled,
                readiness: instance.readiness,
                liveness: instance.liveness,
                restart_count: instance.restart_count,
                queue_depth,
                last_error: instance.last_error,
                last_inbound_unix_ms: instance.last_inbound_unix_ms,
                last_outbound_unix_ms: instance.last_outbound_unix_ms,
                updated_at_unix_ms: instance.updated_at_unix_ms,
            });
        }
        Ok(snapshots)
    }

    pub fn list_logs(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<Vec<crate::storage::ConnectorEventRecord>, ConnectorSupervisorError> {
        self.store.list_events(connector_id, limit).map_err(ConnectorSupervisorError::from)
    }

    pub fn list_dead_letters(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<Vec<crate::storage::DeadLetterRecord>, ConnectorSupervisorError> {
        self.store.list_dead_letters(connector_id, limit).map_err(ConnectorSupervisorError::from)
    }

    pub async fn ingest_inbound(
        &self,
        event: InboundMessageEvent,
    ) -> Result<InboundIngestOutcome, ConnectorSupervisorError> {
        event
            .validate(self.config.max_inbound_body_bytes)
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let now = unix_ms_now()?;
        let Some(instance) = self.store.get_instance(event.connector_id.as_str())? else {
            return Err(ConnectorSupervisorError::NotFound(event.connector_id));
        };
        if !instance.enabled {
            self.store.record_event(
                instance.connector_id.as_str(),
                "inbound.rejected",
                "warn",
                "inbound message dropped because connector is disabled",
                Some(&json!({
                    "envelope_id": event.envelope_id,
                })),
                now,
            )?;
            return Ok(InboundIngestOutcome {
                accepted: false,
                duplicate: false,
                queued_for_retry: false,
                decision_reason: "connector_disabled".to_owned(),
                route_key: None,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }

        let is_new = self.store.record_inbound_dedupe_if_new(
            instance.connector_id.as_str(),
            event.envelope_id.as_str(),
            now,
            self.config.inbound_dedupe_window_ms,
        )?;
        if !is_new {
            self.store.record_event(
                instance.connector_id.as_str(),
                "inbound.duplicate",
                "info",
                "inbound duplicate ignored by dedupe window",
                Some(&json!({
                    "envelope_id": event.envelope_id,
                })),
                now,
            )?;
            return Ok(InboundIngestOutcome {
                accepted: true,
                duplicate: true,
                queued_for_retry: false,
                decision_reason: "duplicate_envelope".to_owned(),
                route_key: None,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }
        self.store.record_last_inbound(instance.connector_id.as_str(), now)?;
        self.store.record_event(
            instance.connector_id.as_str(),
            "inbound.received",
            "info",
            "inbound event accepted by supervisor",
            Some(&json!({
                "envelope_id": event.envelope_id,
                "conversation_id": event.conversation_id,
                "is_direct_message": event.is_direct_message,
                "requested_broadcast": event.requested_broadcast,
            })),
            now,
        )?;

        let routed = self
            .router
            .route_inbound(instance.principal.as_str(), &event)
            .await
            .map_err(|error| ConnectorSupervisorError::Router(error.to_string()))?;
        if !routed.accepted {
            self.store.record_event(
                instance.connector_id.as_str(),
                "inbound.not_routed",
                if routed.queued_for_retry { "warn" } else { "info" },
                routed.decision_reason.as_str(),
                Some(&json!({
                    "envelope_id": event.envelope_id,
                    "queued_for_retry": routed.queued_for_retry,
                    "retry_attempt": routed.retry_attempt,
                })),
                now,
            )?;
            return Ok(InboundIngestOutcome {
                accepted: false,
                duplicate: false,
                queued_for_retry: routed.queued_for_retry,
                decision_reason: routed.decision_reason,
                route_key: routed.route_key,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }

        let mut enqueued_outbound = 0usize;
        for (index, output) in routed.outputs.iter().enumerate() {
            let outbound_envelope_id = format!("{}:{index}", event.envelope_id);
            let request = OutboundMessageRequest {
                envelope_id: outbound_envelope_id,
                connector_id: instance.connector_id.clone(),
                conversation_id: event.conversation_id.clone(),
                reply_thread_id: output.thread_id.clone(),
                in_reply_to_message_id: output.in_reply_to_message_id.clone(),
                text: output.text.clone(),
                broadcast: output.broadcast,
                auto_ack_text: output.auto_ack_text.clone(),
                auto_reaction: output.auto_reaction.clone(),
                timeout_ms: 30_000,
                max_payload_bytes: self.config.max_outbound_body_bytes,
            };
            request
                .validate(self.config.max_outbound_body_bytes)
                .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
            let enqueue = self.store.enqueue_outbox_if_absent(
                &request,
                self.config.max_retry_attempts,
                now,
            )?;
            if enqueue.created {
                enqueued_outbound = enqueued_outbound.saturating_add(1);
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.enqueued",
                    "info",
                    "outbound response queued for connector delivery",
                    Some(&json!({
                        "envelope_id": request.envelope_id,
                        "text_bytes": request.text.len(),
                    })),
                    now,
                )?;
            }
        }

        let drain = self
            .drain_due_outbox_for_connector(
                instance.connector_id.as_str(),
                self.config.immediate_drain_batch_size,
            )
            .await?;
        Ok(InboundIngestOutcome {
            accepted: true,
            duplicate: false,
            queued_for_retry: false,
            decision_reason: "routed".to_owned(),
            route_key: routed.route_key,
            enqueued_outbound,
            immediate_delivery: drain.delivered,
        })
    }

    pub async fn drain_due_outbox(
        &self,
        limit: usize,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let entries = self.store.load_due_outbox(now, limit, None)?;
        self.process_due_entries(entries).await
    }

    pub async fn drain_due_outbox_for_connector(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let entries = self.store.load_due_outbox(now, limit, Some(connector_id))?;
        self.process_due_entries(entries).await
    }

    async fn process_due_entries(
        &self,
        entries: Vec<OutboxEntryRecord>,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let mut outcome = DrainOutcome::default();
        for entry in entries {
            outcome.processed = outcome.processed.saturating_add(1);
            match self.dispatch_outbox_entry(entry).await? {
                DispatchResult::Delivered => {
                    outcome.delivered = outcome.delivered.saturating_add(1);
                }
                DispatchResult::Retried => {
                    outcome.retried = outcome.retried.saturating_add(1);
                }
                DispatchResult::DeadLettered => {
                    outcome.dead_lettered = outcome.dead_lettered.saturating_add(1);
                }
            }
        }
        Ok(outcome)
    }

    async fn dispatch_outbox_entry(
        &self,
        entry: OutboxEntryRecord,
    ) -> Result<DispatchResult, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let Some(instance) = self.store.get_instance(entry.connector_id.as_str())? else {
            self.store.move_outbox_to_dead_letter(
                entry.outbox_id,
                "connector instance not found",
                now,
            )?;
            return Ok(DispatchResult::DeadLettered);
        };
        if !instance.enabled {
            let retry_at = now.saturating_add(
                i64::try_from(self.config.disabled_poll_delay_ms).unwrap_or(i64::MAX),
            );
            self.store.schedule_outbox_retry(
                entry.outbox_id,
                entry.attempts,
                "connector disabled",
                retry_at,
            )?;
            return Ok(DispatchResult::Retried);
        }

        let Some(adapter) = self.adapters.get(&instance.kind).cloned() else {
            self.store.move_outbox_to_dead_letter(
                entry.outbox_id,
                "connector adapter implementation missing",
                now,
            )?;
            self.store.record_event(
                instance.connector_id.as_str(),
                "outbox.dead_letter",
                "error",
                "connector adapter implementation missing",
                Some(&json!({
                    "kind": instance.kind.as_str(),
                    "envelope_id": entry.envelope_id,
                })),
                now,
            )?;
            return Ok(DispatchResult::DeadLettered);
        };

        let delivery = match adapter.send_outbound(&entry.payload).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.adapter_error",
                    "warn",
                    "adapter delivery call failed; scheduling retry",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "error": error.to_string(),
                    })),
                    now,
                )?;
                DeliveryOutcome::Retry {
                    class: RetryClass::TransientNetwork,
                    reason: error.to_string(),
                    retry_after_ms: None,
                }
            }
        };
        self.apply_delivery_outcome(&instance, &entry, delivery, now).await
    }

    async fn apply_delivery_outcome(
        &self,
        instance: &crate::storage::ConnectorInstanceRecord,
        entry: &OutboxEntryRecord,
        delivery: DeliveryOutcome,
        now_unix_ms: i64,
    ) -> Result<DispatchResult, ConnectorSupervisorError> {
        match delivery {
            DeliveryOutcome::Delivered { native_message_id } => {
                self.store.mark_outbox_delivered(
                    entry.outbox_id,
                    native_message_id.as_str(),
                    now_unix_ms,
                )?;
                self.store.record_last_outbound(instance.connector_id.as_str(), now_unix_ms)?;
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.delivered",
                    "info",
                    "outbound message delivered",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "native_message_id": native_message_id,
                    })),
                    now_unix_ms,
                )?;
                Ok(DispatchResult::Delivered)
            }
            DeliveryOutcome::Retry { class, reason, retry_after_ms } => {
                let attempts = entry.attempts.saturating_add(1);
                let max_attempts = entry.max_attempts.min(self.config.max_retry_attempts).max(1);
                if attempts >= max_attempts {
                    self.store.move_outbox_to_dead_letter(
                        entry.outbox_id,
                        reason.as_str(),
                        now_unix_ms,
                    )?;
                    self.store.record_event(
                        instance.connector_id.as_str(),
                        "outbox.dead_letter",
                        "warn",
                        "retry budget exhausted; moved to dead letter",
                        Some(&json!({
                            "envelope_id": entry.envelope_id,
                            "attempts": attempts,
                            "reason": reason,
                            "retry_class": format!("{class:?}"),
                        })),
                        now_unix_ms,
                    )?;
                    return Ok(DispatchResult::DeadLettered);
                }

                let delay_ms = self.retry_delay_ms(attempts, retry_after_ms);
                let next_attempt_unix_ms =
                    now_unix_ms.saturating_add(i64::try_from(delay_ms).unwrap_or(i64::MAX));
                self.store.schedule_outbox_retry(
                    entry.outbox_id,
                    attempts,
                    reason.as_str(),
                    next_attempt_unix_ms,
                )?;
                if matches!(class, RetryClass::ConnectorRestarting) {
                    self.store.increment_restart_count(
                        instance.connector_id.as_str(),
                        now_unix_ms,
                        reason.as_str(),
                    )?;
                } else {
                    self.store.set_instance_runtime_state(
                        instance.connector_id.as_str(),
                        ConnectorReadiness::Ready,
                        ConnectorLiveness::Running,
                        Some(reason.as_str()),
                        now_unix_ms,
                    )?;
                }
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.retry",
                    "warn",
                    "connector delivery requested retry",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "attempts": attempts,
                        "next_attempt_unix_ms": next_attempt_unix_ms,
                        "reason": reason,
                        "retry_class": format!("{class:?}"),
                    })),
                    now_unix_ms,
                )?;
                Ok(DispatchResult::Retried)
            }
            DeliveryOutcome::PermanentFailure { reason } => {
                self.store.move_outbox_to_dead_letter(
                    entry.outbox_id,
                    reason.as_str(),
                    now_unix_ms,
                )?;
                self.store.set_instance_runtime_state(
                    instance.connector_id.as_str(),
                    ConnectorReadiness::Misconfigured,
                    ConnectorLiveness::Running,
                    Some(reason.as_str()),
                    now_unix_ms,
                )?;
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.dead_letter",
                    "warn",
                    "connector delivery returned permanent failure",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "reason": reason,
                    })),
                    now_unix_ms,
                )?;
                Ok(DispatchResult::DeadLettered)
            }
        }
    }

    fn retry_delay_ms(&self, attempts: u32, requested_retry_after_ms: Option<u64>) -> u64 {
        let exponent = attempts.saturating_sub(1).min(10);
        let exponential = self
            .config
            .base_retry_delay_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.config.max_retry_delay_ms);
        requested_retry_after_ms
            .unwrap_or(exponential)
            .max(self.config.min_retry_delay_ms)
            .min(self.config.max_retry_delay_ms)
    }
}

fn unix_ms_now() -> Result<i64, ConnectorSupervisorError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ConnectorSupervisorError::Clock(error.to_string()))?;
    Ok(now.as_millis().try_into().unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use tempfile::TempDir;

    use crate::{
        protocol::{
            ConnectorInstanceSpec, ConnectorKind, DeliveryOutcome, RetryClass,
            RoutedOutboundMessage,
        },
        storage::ConnectorStore,
    };

    use super::{
        ConnectorAdapter, ConnectorAdapterError, ConnectorRouter, ConnectorRouterError,
        ConnectorSupervisor, ConnectorSupervisorConfig,
    };
    use async_trait::async_trait;

    struct RouterStub;

    #[async_trait]
    impl ConnectorRouter for RouterStub {
        async fn route_inbound(
            &self,
            _principal: &str,
            event: &crate::protocol::InboundMessageEvent,
        ) -> Result<crate::protocol::RouteInboundResult, ConnectorRouterError> {
            Ok(crate::protocol::RouteInboundResult {
                accepted: true,
                queued_for_retry: false,
                decision_reason: "routed".to_owned(),
                outputs: vec![RoutedOutboundMessage {
                    text: event.body.clone(),
                    thread_id: None,
                    in_reply_to_message_id: event.adapter_message_id.clone(),
                    broadcast: false,
                    auto_ack_text: None,
                    auto_reaction: None,
                }],
                route_key: Some("channel:echo:conversation:c1".to_owned()),
                retry_attempt: 0,
            })
        }
    }

    #[derive(Default)]
    struct FlakyAdapter {
        attempts: Mutex<HashMap<String, usize>>,
    }

    #[async_trait]
    impl ConnectorAdapter for FlakyAdapter {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind::Echo
        }

        async fn send_outbound(
            &self,
            request: &crate::protocol::OutboundMessageRequest,
        ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
            let mut attempts = self.attempts.lock().map_err(|_| {
                ConnectorAdapterError::Backend("flaky adapter attempts lock poisoned".to_owned())
            })?;
            let entry = attempts.entry(request.envelope_id.clone()).or_insert(0);
            *entry += 1;
            if request.text.contains("[connector-crash-once]") && *entry == 1 {
                return Ok(DeliveryOutcome::Retry {
                    class: RetryClass::ConnectorRestarting,
                    reason: "simulated restart".to_owned(),
                    retry_after_ms: Some(1),
                });
            }
            Ok(DeliveryOutcome::Delivered {
                native_message_id: format!("native-{}", request.envelope_id),
            })
        }
    }

    fn open_supervisor() -> (TempDir, ConnectorSupervisor) {
        let tempdir = TempDir::new().expect("tempdir should initialize");
        let store = std::sync::Arc::new(
            ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
                .expect("store should initialize"),
        );
        let supervisor = ConnectorSupervisor::new(
            store,
            std::sync::Arc::new(RouterStub),
            vec![std::sync::Arc::new(FlakyAdapter::default())],
            ConnectorSupervisorConfig {
                min_retry_delay_ms: 1,
                base_retry_delay_ms: 1,
                max_retry_delay_ms: 8,
                ..ConnectorSupervisorConfig::default()
            },
        );
        (tempdir, supervisor)
    }

    fn sample_spec() -> ConnectorInstanceSpec {
        ConnectorInstanceSpec {
            connector_id: "echo:default".to_owned(),
            kind: ConnectorKind::Echo,
            principal: "channel:echo:default".to_owned(),
            auth_profile_ref: None,
            token_vault_ref: None,
            egress_allowlist: Vec::new(),
            enabled: true,
        }
    }

    fn sample_inbound(body: &str) -> crate::protocol::InboundMessageEvent {
        crate::protocol::InboundMessageEvent {
            envelope_id: "env-1".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            thread_id: None,
            sender_id: "u1".to_owned(),
            sender_display: None,
            body: body.to_owned(),
            adapter_message_id: Some("m1".to_owned()),
            adapter_thread_id: None,
            received_at_unix_ms: 1_000,
            is_direct_message: true,
            requested_broadcast: false,
        }
    }

    #[tokio::test]
    async fn duplicate_inbound_does_not_create_duplicate_outbound() {
        let (_tempdir, supervisor) = open_supervisor();
        supervisor.register_connector(&sample_spec()).expect("register should succeed");

        let first = supervisor
            .ingest_inbound(sample_inbound("hello"))
            .await
            .expect("first ingest should succeed");
        let second = supervisor
            .ingest_inbound(sample_inbound("hello"))
            .await
            .expect("duplicate ingest should succeed");

        assert!(first.accepted);
        assert_eq!(first.enqueued_outbound, 1);
        assert!(second.duplicate);
        assert_eq!(second.enqueued_outbound, 0);
    }

    #[tokio::test]
    async fn restart_retry_is_replayed_and_delivered_once() {
        let (_tempdir, supervisor) = open_supervisor();
        supervisor.register_connector(&sample_spec()).expect("register should succeed");

        let ingest = supervisor
            .ingest_inbound(sample_inbound("hello [connector-crash-once]"))
            .await
            .expect("ingest should succeed");
        assert!(ingest.accepted);
        let drained =
            supervisor.drain_due_outbox(16).await.expect("drain should succeed after retry delay");
        let status = supervisor.status("echo:default").expect("status should resolve");
        assert!(drained.delivered >= 1, "retry drain should eventually deliver");
        assert!(status.restart_count >= 1, "restart counter should increment on restart retry");
    }
}
