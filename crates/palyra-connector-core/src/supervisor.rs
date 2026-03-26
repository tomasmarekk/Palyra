use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::{json, Map, Value};
use thiserror::Error;

use crate::{
    protocol::{
        ConnectorAvailability, ConnectorCapabilitySet, ConnectorInstanceSpec, ConnectorKind,
        ConnectorLiveness, ConnectorReadiness, ConnectorStatusSnapshot, DeliveryOutcome,
        InboundMessageEvent, OutboundMessageRequest, RetryClass, RouteInboundResult,
    },
    storage::{
        ConnectorQueueSnapshot, ConnectorStore, ConnectorStoreError, DeadLetterRecord,
        OutboxEnqueueOutcome, OutboxEntryRecord,
    },
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

const CONNECTOR_METRICS_EVENT_WINDOW: usize = 2_048;
const CONNECTOR_POLICY_DENIAL_REASON_LIMIT: usize = 16;

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

#[derive(Debug, Clone, Default, Serialize)]
struct RouteMessageLatencySnapshot {
    sample_count: u64,
    avg_ms: u64,
    max_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct PolicyDenialReasonCount {
    reason: String,
    count: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ConnectorRuntimeMetricsSnapshot {
    event_window_size: u64,
    inbound_events_processed: u64,
    inbound_dedupe_hits: u64,
    outbound_sends_ok: u64,
    outbound_sends_retry: u64,
    outbound_sends_dead_letter: u64,
    route_message_latency_ms: RouteMessageLatencySnapshot,
    policy_denials: Vec<PolicyDenialReasonCount>,
}

#[derive(Debug, Clone, Serialize)]
struct ConnectorSaturationSnapshot {
    state: &'static str,
    reasons: Vec<String>,
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

    fn availability(&self) -> ConnectorAvailability;

    fn capabilities(&self) -> ConnectorCapabilitySet {
        ConnectorCapabilitySet::for_connector(self.kind(), self.availability())
    }

    fn split_outbound(
        &self,
        _instance: &crate::storage::ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<Vec<OutboundMessageRequest>, ConnectorAdapterError> {
        Ok(vec![request.clone()])
    }

    fn runtime_snapshot(
        &self,
        _instance: &crate::storage::ConnectorInstanceRecord,
    ) -> Option<Value> {
        None
    }

    async fn poll_inbound(
        &self,
        _instance: &crate::storage::ConnectorInstanceRecord,
        _limit: usize,
    ) -> Result<Vec<InboundMessageEvent>, ConnectorAdapterError> {
        Ok(Vec::new())
    }

    async fn send_outbound(
        &self,
        instance: &crate::storage::ConnectorInstanceRecord,
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
                "availability": spec.kind.default_availability().as_str(),
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

    pub fn remove_connector(&self, connector_id: &str) -> Result<(), ConnectorSupervisorError> {
        let Some(_instance) = self.store.get_instance(connector_id)? else {
            return Err(ConnectorSupervisorError::NotFound(connector_id.to_owned()));
        };
        self.store.delete_instance(connector_id)?;
        Ok(())
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
            availability: self.connector_availability(instance.kind),
            capabilities: self.connector_capabilities(instance.kind),
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
                availability: self.connector_availability(instance.kind),
                capabilities: self.connector_capabilities(instance.kind),
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

    pub fn runtime_snapshot(
        &self,
        connector_id: &str,
    ) -> Result<Option<Value>, ConnectorSupervisorError> {
        let Some(instance) = self.store.get_instance(connector_id)? else {
            return Err(ConnectorSupervisorError::NotFound(connector_id.to_owned()));
        };
        let queue = self.queue_snapshot(instance.connector_id.as_str())?;
        let adapter_runtime = self
            .adapters
            .get(&instance.kind)
            .and_then(|adapter| adapter.runtime_snapshot(&instance));
        let metrics = self.build_runtime_metrics(instance.connector_id.as_str())?;
        let mut runtime = match adapter_runtime {
            Some(Value::Object(object)) => Value::Object(object),
            Some(other) => {
                let mut object = Map::new();
                object.insert("adapter".to_owned(), other);
                Value::Object(object)
            }
            None => Value::Object(Map::new()),
        };
        if let Some(object) = runtime.as_object_mut() {
            object.insert("metrics".to_owned(), json!(metrics));
            object.insert("queue".to_owned(), json!(queue));
            object.insert("saturation".to_owned(), json!(build_saturation_snapshot(&queue)));
        }
        Ok(Some(runtime))
    }

    fn connector_availability(&self, kind: ConnectorKind) -> ConnectorAvailability {
        self.adapters
            .get(&kind)
            .map(|adapter| adapter.availability())
            .unwrap_or_else(|| kind.default_availability())
    }

    fn connector_capabilities(&self, kind: ConnectorKind) -> ConnectorCapabilitySet {
        self.adapters.get(&kind).map(|adapter| adapter.capabilities()).unwrap_or_else(|| {
            ConnectorCapabilitySet::for_connector(kind, kind.default_availability())
        })
    }

    fn build_runtime_metrics(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorRuntimeMetricsSnapshot, ConnectorSupervisorError> {
        let events = self.store.list_events(connector_id, CONNECTOR_METRICS_EVENT_WINDOW)?;
        let mut metrics = ConnectorRuntimeMetricsSnapshot {
            event_window_size: u64::try_from(events.len()).unwrap_or(u64::MAX),
            ..ConnectorRuntimeMetricsSnapshot::default()
        };
        let mut route_latency_total_ms = 0_u128;
        let mut denial_counts: HashMap<String, u64> = HashMap::new();
        for event in events {
            match event.event_type.as_str() {
                "inbound.received" | "inbound.duplicate" | "inbound.rejected" => {
                    metrics.inbound_events_processed =
                        metrics.inbound_events_processed.saturating_add(1);
                }
                "outbox.delivered" => {
                    metrics.outbound_sends_ok = metrics.outbound_sends_ok.saturating_add(1);
                }
                "outbox.retry" => {
                    metrics.outbound_sends_retry = metrics.outbound_sends_retry.saturating_add(1);
                }
                "outbox.dead_letter" => {
                    metrics.outbound_sends_dead_letter =
                        metrics.outbound_sends_dead_letter.saturating_add(1);
                }
                _ => {}
            }
            if event.event_type == "inbound.duplicate" {
                metrics.inbound_dedupe_hits = metrics.inbound_dedupe_hits.saturating_add(1);
            }
            if matches!(event.event_type.as_str(), "inbound.routed" | "inbound.not_routed") {
                if let Some(latency_ms) =
                    event.details.as_ref().and_then(parse_route_message_latency_ms)
                {
                    metrics.route_message_latency_ms.sample_count =
                        metrics.route_message_latency_ms.sample_count.saturating_add(1);
                    metrics.route_message_latency_ms.max_ms =
                        metrics.route_message_latency_ms.max_ms.max(latency_ms);
                    route_latency_total_ms =
                        route_latency_total_ms.saturating_add(u128::from(latency_ms));
                }
            }
            if event.event_type == "inbound.not_routed"
                && !event
                    .details
                    .as_ref()
                    .and_then(|details| details.get("queued_for_retry"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                && is_policy_denial_reason(event.message.as_str())
            {
                let reason = event.message.trim().to_owned();
                *denial_counts.entry(reason).or_insert(0) += 1;
            }
        }
        if metrics.route_message_latency_ms.sample_count > 0 {
            metrics.route_message_latency_ms.avg_ms = u64::try_from(
                route_latency_total_ms
                    / u128::from(metrics.route_message_latency_ms.sample_count.max(1)),
            )
            .unwrap_or(u64::MAX);
        }
        let mut denials = denial_counts
            .into_iter()
            .map(|(reason, count)| PolicyDenialReasonCount { reason, count })
            .collect::<Vec<_>>();
        denials.sort_by(|left, right| {
            right.count.cmp(&left.count).then_with(|| left.reason.cmp(&right.reason))
        });
        denials.truncate(CONNECTOR_POLICY_DENIAL_REASON_LIMIT);
        metrics.policy_denials = denials;
        Ok(metrics)
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
    ) -> Result<Vec<DeadLetterRecord>, ConnectorSupervisorError> {
        self.store.list_dead_letters(connector_id, limit).map_err(ConnectorSupervisorError::from)
    }

    pub fn queue_snapshot(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorQueueSnapshot, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.queue_snapshot(connector_id, now).map_err(ConnectorSupervisorError::from)
    }

    pub fn set_queue_paused(
        &self,
        connector_id: &str,
        paused: bool,
        reason: Option<&str>,
    ) -> Result<ConnectorQueueSnapshot, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.set_queue_paused(connector_id, paused, reason, now)?;
        self.store.record_event(
            connector_id,
            if paused { "queue.paused" } else { "queue.resumed" },
            "info",
            if paused { "connector outbox queue paused" } else { "connector outbox queue resumed" },
            Some(&json!({
                "paused": paused,
                "reason": reason,
            })),
            now,
        )?;
        self.queue_snapshot(connector_id)
    }

    pub fn replay_dead_letter(
        &self,
        connector_id: &str,
        dead_letter_id: i64,
    ) -> Result<DeadLetterRecord, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let replayed = self.store.replay_dead_letter(
            connector_id,
            dead_letter_id,
            self.config.max_retry_attempts,
            now,
        )?;
        self.store.record_event(
            connector_id,
            "dead_letter.replayed",
            "info",
            "dead-letter entry replayed into outbox",
            Some(&json!({
                "dead_letter_id": dead_letter_id,
                "envelope_id": replayed.envelope_id,
            })),
            now,
        )?;
        Ok(replayed)
    }

    pub fn discard_dead_letter(
        &self,
        connector_id: &str,
        dead_letter_id: i64,
    ) -> Result<DeadLetterRecord, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let discarded = self.store.discard_dead_letter(connector_id, dead_letter_id)?;
        self.store.record_event(
            connector_id,
            "dead_letter.discarded",
            "info",
            "dead-letter entry discarded by operator",
            Some(&json!({
                "dead_letter_id": dead_letter_id,
                "envelope_id": discarded.envelope_id,
            })),
            now,
        )?;
        Ok(discarded)
    }

    pub fn enqueue_outbound(
        &self,
        request: &OutboundMessageRequest,
    ) -> Result<OutboxEnqueueOutcome, ConnectorSupervisorError> {
        request
            .validate(self.config.max_outbound_body_bytes)
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let now = unix_ms_now()?;
        let Some(instance) = self.store.get_instance(request.connector_id.as_str())? else {
            return Err(ConnectorSupervisorError::NotFound(request.connector_id.clone()));
        };
        let outcome =
            self.store.enqueue_outbox_if_absent(request, self.config.max_retry_attempts, now)?;
        if outcome.created {
            self.store.record_event(
                instance.connector_id.as_str(),
                "outbox.enqueued",
                "info",
                "outbound message queued by direct enqueue operation",
                Some(&json!({
                    "envelope_id": request.envelope_id,
                    "conversation_id": request.conversation_id,
                    "text_bytes": request.text.len(),
                })),
                now,
            )?;
        }
        Ok(outcome)
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
                    "route_message_latency_ms": routed.route_message_latency_ms,
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
        self.store.record_event(
            instance.connector_id.as_str(),
            "inbound.routed",
            "info",
            "inbound event routed to gateway",
            Some(&json!({
                "envelope_id": event.envelope_id,
                "route_key": routed.route_key.clone(),
                "outputs": routed.outputs.len(),
                "retry_attempt": routed.retry_attempt,
                "route_message_latency_ms": routed.route_message_latency_ms,
            })),
            now,
        )?;

        let mut enqueued_outbound = 0usize;
        for (index, output) in routed.outputs.iter().enumerate() {
            let base_request = OutboundMessageRequest {
                envelope_id: format!("{}:{index}", event.envelope_id),
                connector_id: instance.connector_id.clone(),
                conversation_id: event.conversation_id.clone(),
                reply_thread_id: output.thread_id.clone(),
                in_reply_to_message_id: output.in_reply_to_message_id.clone(),
                text: output.text.clone(),
                broadcast: output.broadcast,
                auto_ack_text: output.auto_ack_text.clone(),
                auto_reaction: output.auto_reaction.clone(),
                attachments: output.attachments.clone(),
                structured_json: output.structured_json.clone(),
                a2ui_update: output.a2ui_update.clone(),
                timeout_ms: 30_000,
                max_payload_bytes: self.config.max_outbound_body_bytes,
            };
            base_request
                .validate(self.config.max_outbound_body_bytes)
                .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;

            let split_requests = if let Some(adapter) = self.adapters.get(&instance.kind) {
                adapter
                    .split_outbound(&instance, &base_request)
                    .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?
            } else {
                vec![base_request]
            };
            if split_requests.is_empty() {
                continue;
            }
            for request in split_requests {
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

    pub async fn poll_inbound(
        &self,
        per_connector_limit: usize,
    ) -> Result<usize, ConnectorSupervisorError> {
        let limit = per_connector_limit.max(1);
        let instances = self.store.list_instances()?;
        let mut processed = 0_usize;

        for instance in instances {
            if !instance.enabled {
                continue;
            }
            let Some(adapter) = self.adapters.get(&instance.kind) else {
                continue;
            };
            let inbound = match adapter.poll_inbound(&instance, limit).await {
                Ok(inbound) => inbound,
                Err(error) => {
                    let now = unix_ms_now()?;
                    self.store.record_event(
                        instance.connector_id.as_str(),
                        "inbound.poll_error",
                        "warn",
                        "adapter inbound poll failed; continuing with remaining connectors",
                        Some(&json!({
                            "error": error.to_string(),
                        })),
                        now,
                    )?;
                    continue;
                }
            };
            for event in inbound {
                self.ingest_inbound(event).await?;
                processed = processed.saturating_add(1);
            }
        }

        Ok(processed)
    }

    pub async fn drain_due_outbox(
        &self,
        limit: usize,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let entries = self.store.load_due_outbox(now, limit, None, false)?;
        self.process_due_entries(entries).await
    }

    pub async fn drain_due_outbox_for_connector(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let entries = self.store.load_due_outbox(now, limit, Some(connector_id), false)?;
        self.process_due_entries(entries).await
    }

    pub async fn drain_due_outbox_for_connector_force(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let entries = self.store.load_due_outbox(now, limit, Some(connector_id), true)?;
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
                entry.claim_token.as_str(),
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
                entry.claim_token.as_str(),
                entry.attempts,
                "connector disabled",
                retry_at,
            )?;
            return Ok(DispatchResult::Retried);
        }

        let Some(adapter) = self.adapters.get(&instance.kind).cloned() else {
            self.store.move_outbox_to_dead_letter(
                entry.outbox_id,
                entry.claim_token.as_str(),
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

        let delivery = match adapter.send_outbound(&instance, &entry.payload).await {
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
                    entry.claim_token.as_str(),
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
                        entry.claim_token.as_str(),
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
                    entry.claim_token.as_str(),
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
                    entry.claim_token.as_str(),
                    reason.as_str(),
                    now_unix_ms,
                )?;
                let readiness = classify_permanent_failure(reason.as_str());
                self.store.set_instance_runtime_state(
                    instance.connector_id.as_str(),
                    readiness,
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

fn classify_permanent_failure(reason: &str) -> ConnectorReadiness {
    let normalized = reason.trim().to_ascii_lowercase();
    if normalized.contains("credential missing") || normalized.contains("missing credential") {
        return ConnectorReadiness::MissingCredential;
    }
    if normalized.contains("auth")
        || normalized.contains("token")
        || normalized.contains("unauthorized")
        || normalized.contains("forbidden")
    {
        return ConnectorReadiness::AuthFailed;
    }
    ConnectorReadiness::Misconfigured
}

fn parse_route_message_latency_ms(details: &Value) -> Option<u64> {
    details.get("route_message_latency_ms").and_then(Value::as_u64)
}

fn is_policy_denial_reason(reason: &str) -> bool {
    !matches!(
        reason.trim(),
        "backpressure_queue_full"
            | "backpressure_retry_enqueue_failed"
            | "backpressure_poison_quarantine"
    )
}

fn build_saturation_snapshot(queue: &ConnectorQueueSnapshot) -> ConnectorSaturationSnapshot {
    let mut reasons = Vec::new();
    if queue.paused {
        reasons.push("queue_paused".to_owned());
    }
    if queue.due_outbox > 0 {
        reasons.push(format!("due_outbox={}", queue.due_outbox));
    }
    if queue.claimed_outbox > 0 {
        reasons.push(format!("claimed_outbox={}", queue.claimed_outbox));
    }
    if queue.dead_letters > 0 {
        reasons.push(format!("dead_letters={}", queue.dead_letters));
    }
    if queue.pending_outbox > queue.due_outbox && queue.pending_outbox > 0 {
        reasons.push(format!("pending_outbox={}", queue.pending_outbox));
    }
    let state = if queue.paused {
        "paused"
    } else if queue.dead_letters > 0 || queue.claimed_outbox > 0 || queue.due_outbox >= 8 {
        "saturated"
    } else if queue.pending_outbox > 0 || queue.due_outbox > 0 {
        "backlogged"
    } else {
        "nominal"
    };
    ConnectorSaturationSnapshot { state, reasons }
}

fn unix_ms_now() -> Result<i64, ConnectorSupervisorError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ConnectorSupervisorError::Clock(error.to_string()))?;
    Ok(now.as_millis().try_into().unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tempfile::TempDir;

    use crate::{
        protocol::{
            ConnectorAvailability, ConnectorInstanceSpec, ConnectorKind, ConnectorReadiness,
            DeliveryOutcome, OutboundMessageRequest, RetryClass, RoutedOutboundMessage,
        },
        storage::ConnectorStore,
    };

    use super::{
        ConnectorAdapter, ConnectorAdapterError, ConnectorRouter, ConnectorRouterError,
        ConnectorSupervisor, ConnectorSupervisorConfig,
    };
    use async_trait::async_trait;
    use serde_json::Value;

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
                    attachments: Vec::new(),
                    structured_json: None,
                    a2ui_update: None,
                }],
                route_key: Some("channel:echo:conversation:c1".to_owned()),
                retry_attempt: 0,
                route_message_latency_ms: Some(1),
            })
        }
    }

    #[derive(Default)]
    struct FlakyAdapter {
        attempts: Mutex<HashMap<String, usize>>,
        inbound_events: Mutex<VecDeque<crate::protocol::InboundMessageEvent>>,
    }

    impl FlakyAdapter {
        fn push_inbound(&self, event: crate::protocol::InboundMessageEvent) {
            self.inbound_events
                .lock()
                .expect("inbound queue lock should not be poisoned")
                .push_back(event);
        }
    }

    #[async_trait]
    impl ConnectorAdapter for FlakyAdapter {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind::Echo
        }

        fn availability(&self) -> ConnectorAvailability {
            ConnectorAvailability::InternalTestOnly
        }

        async fn poll_inbound(
            &self,
            _instance: &crate::storage::ConnectorInstanceRecord,
            limit: usize,
        ) -> Result<Vec<crate::protocol::InboundMessageEvent>, ConnectorAdapterError> {
            let mut queue = self.inbound_events.lock().map_err(|_| {
                ConnectorAdapterError::Backend(
                    "flaky adapter inbound queue lock poisoned".to_owned(),
                )
            })?;
            let mut events = Vec::new();
            let max = limit.max(1);
            while events.len() < max {
                let Some(event) = queue.pop_front() else {
                    break;
                };
                events.push(event);
            }
            Ok(events)
        }

        async fn send_outbound(
            &self,
            _instance: &crate::storage::ConnectorInstanceRecord,
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

    #[derive(Default)]
    struct PollErrorAdapter;

    #[async_trait]
    impl ConnectorAdapter for PollErrorAdapter {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind::Slack
        }

        fn availability(&self) -> ConnectorAvailability {
            ConnectorAvailability::Deferred
        }

        async fn poll_inbound(
            &self,
            _instance: &crate::storage::ConnectorInstanceRecord,
            _limit: usize,
        ) -> Result<Vec<crate::protocol::InboundMessageEvent>, ConnectorAdapterError> {
            Err(ConnectorAdapterError::Backend("simulated inbound poll failure".to_owned()))
        }

        async fn send_outbound(
            &self,
            _instance: &crate::storage::ConnectorInstanceRecord,
            request: &crate::protocol::OutboundMessageRequest,
        ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
            Ok(DeliveryOutcome::Delivered {
                native_message_id: format!("native-{}", request.envelope_id),
            })
        }
    }

    #[derive(Default)]
    struct SlowCountingAdapter {
        sends: Mutex<HashMap<String, usize>>,
    }

    impl SlowCountingAdapter {
        fn sends_for(&self, envelope_id: &str) -> usize {
            self.sends
                .lock()
                .expect("slow adapter send counter lock should not be poisoned")
                .get(envelope_id)
                .copied()
                .unwrap_or(0)
        }
    }

    #[async_trait]
    impl ConnectorAdapter for SlowCountingAdapter {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind::Echo
        }

        fn availability(&self) -> ConnectorAvailability {
            ConnectorAvailability::InternalTestOnly
        }

        async fn send_outbound(
            &self,
            _instance: &crate::storage::ConnectorInstanceRecord,
            request: &crate::protocol::OutboundMessageRequest,
        ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let mut sends = self.sends.lock().map_err(|_| {
                ConnectorAdapterError::Backend("slow adapter send counter lock poisoned".to_owned())
            })?;
            let entry = sends.entry(request.envelope_id.clone()).or_insert(0);
            *entry = entry.saturating_add(1);
            Ok(DeliveryOutcome::Delivered {
                native_message_id: format!("native-{}-{}", request.envelope_id, *entry),
            })
        }
    }

    struct PermanentFailureAdapter {
        reason: &'static str,
    }

    #[async_trait]
    impl ConnectorAdapter for PermanentFailureAdapter {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind::Echo
        }

        fn availability(&self) -> ConnectorAvailability {
            ConnectorAvailability::InternalTestOnly
        }

        async fn send_outbound(
            &self,
            _instance: &crate::storage::ConnectorInstanceRecord,
            _request: &crate::protocol::OutboundMessageRequest,
        ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
            Ok(DeliveryOutcome::PermanentFailure { reason: self.reason.to_owned() })
        }
    }

    fn open_supervisor() -> (TempDir, ConnectorSupervisor, Arc<FlakyAdapter>) {
        let tempdir = TempDir::new().expect("tempdir should initialize");
        let store = std::sync::Arc::new(
            ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
                .expect("store should initialize"),
        );
        let adapter = Arc::new(FlakyAdapter::default());
        let supervisor = ConnectorSupervisor::new(
            store,
            std::sync::Arc::new(RouterStub),
            vec![adapter.clone()],
            ConnectorSupervisorConfig {
                min_retry_delay_ms: 1,
                base_retry_delay_ms: 1,
                max_retry_delay_ms: 8,
                ..ConnectorSupervisorConfig::default()
            },
        );
        (tempdir, supervisor, adapter)
    }

    fn sample_spec() -> ConnectorInstanceSpec {
        sample_spec_with("echo:default", ConnectorKind::Echo, "channel:echo:default")
    }

    fn sample_spec_with(
        connector_id: &str,
        kind: ConnectorKind,
        principal: &str,
    ) -> ConnectorInstanceSpec {
        ConnectorInstanceSpec {
            connector_id: connector_id.to_owned(),
            kind,
            principal: principal.to_owned(),
            auth_profile_ref: None,
            token_vault_ref: None,
            egress_allowlist: Vec::new(),
            enabled: true,
        }
    }

    fn sample_inbound(body: &str) -> crate::protocol::InboundMessageEvent {
        sample_inbound_for("echo:default", "env-1", body)
    }

    fn sample_inbound_for(
        connector_id: &str,
        envelope_id: &str,
        body: &str,
    ) -> crate::protocol::InboundMessageEvent {
        crate::protocol::InboundMessageEvent {
            envelope_id: envelope_id.to_owned(),
            connector_id: connector_id.to_owned(),
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
            attachments: Vec::new(),
        }
    }

    fn sample_outbound_request(envelope_id: &str, text: &str) -> OutboundMessageRequest {
        OutboundMessageRequest {
            envelope_id: envelope_id.to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: text.to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 30_000,
            max_payload_bytes: 16_384,
        }
    }

    #[tokio::test]
    async fn duplicate_inbound_does_not_create_duplicate_outbound() {
        let (_tempdir, supervisor, _adapter) = open_supervisor();
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
        let (_tempdir, supervisor, _adapter) = open_supervisor();
        supervisor.register_connector(&sample_spec()).expect("register should succeed");

        let ingest = supervisor
            .ingest_inbound(sample_inbound("hello [connector-crash-once]"))
            .await
            .expect("ingest should succeed");
        assert!(ingest.accepted);
        let mut delivered = 0_usize;
        for _ in 0..20 {
            let drained = supervisor
                .drain_due_outbox(16)
                .await
                .expect("drain should succeed while waiting for retry");
            delivered = delivered.saturating_add(drained.delivered);
            if delivered >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let status = supervisor.status("echo:default").expect("status should resolve");
        assert!(delivered >= 1, "retry drain should eventually deliver");
        assert!(status.restart_count >= 1, "restart counter should increment on restart retry");
    }

    #[tokio::test]
    async fn concurrent_drains_do_not_double_send_same_outbox_entry() {
        let tempdir = TempDir::new().expect("tempdir should initialize");
        let store = Arc::new(
            ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
                .expect("store should initialize"),
        );
        let adapter = Arc::new(SlowCountingAdapter::default());
        let supervisor = ConnectorSupervisor::new(
            store,
            Arc::new(RouterStub),
            vec![adapter.clone()],
            ConnectorSupervisorConfig {
                min_retry_delay_ms: 1,
                base_retry_delay_ms: 1,
                max_retry_delay_ms: 8,
                ..ConnectorSupervisorConfig::default()
            },
        );
        supervisor.register_connector(&sample_spec()).expect("register should succeed");

        let outbound = OutboundMessageRequest {
            envelope_id: "env-concurrent-drain".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "concurrent drain".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 30_000,
            max_payload_bytes: 16_384,
        };
        let enqueue =
            supervisor.enqueue_outbound(&outbound).expect("outbox enqueue should succeed");
        assert!(enqueue.created, "first enqueue should create an outbox row");

        let (global_drain, connector_drain) = tokio::join!(
            supervisor.drain_due_outbox(1),
            supervisor.drain_due_outbox_for_connector("echo:default", 1),
        );
        let global_drain = global_drain.expect("global drain should succeed");
        let connector_drain = connector_drain.expect("connector-scoped drain should succeed");
        assert_eq!(
            global_drain.delivered + connector_drain.delivered,
            1,
            "exactly one drain operation should deliver the claimed outbox row"
        );
        assert_eq!(
            adapter.sends_for("env-concurrent-drain"),
            1,
            "adapter send should run exactly once across concurrent drains"
        );
    }

    #[tokio::test]
    async fn poll_inbound_routes_events_from_adapter_queue() {
        let (_tempdir, supervisor, adapter) = open_supervisor();
        supervisor.register_connector(&sample_spec()).expect("register should succeed");
        adapter.push_inbound(sample_inbound("hello from poll"));

        let processed = supervisor.poll_inbound(8).await.expect("poll should succeed");

        assert_eq!(processed, 1, "one inbound event should be processed");
        let status = supervisor.status("echo:default").expect("status should resolve");
        assert!(status.last_inbound_unix_ms.is_some(), "poll should update last inbound timestamp");
    }

    #[tokio::test]
    async fn poll_inbound_continues_after_adapter_error_and_records_warning_event() {
        let tempdir = TempDir::new().expect("tempdir should initialize");
        let store = Arc::new(
            ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
                .expect("store should initialize"),
        );
        let healthy_adapter = Arc::new(FlakyAdapter::default());
        let failing_adapter = Arc::new(PollErrorAdapter);
        let supervisor = ConnectorSupervisor::new(
            store,
            Arc::new(RouterStub),
            vec![healthy_adapter.clone(), failing_adapter],
            ConnectorSupervisorConfig {
                min_retry_delay_ms: 1,
                base_retry_delay_ms: 1,
                max_retry_delay_ms: 8,
                ..ConnectorSupervisorConfig::default()
            },
        );
        supervisor
            .register_connector(&sample_spec_with(
                "a-failing:default",
                ConnectorKind::Slack,
                "channel:slack:default",
            ))
            .expect("failing connector should register");
        supervisor
            .register_connector(&sample_spec_with(
                "z-healthy:default",
                ConnectorKind::Echo,
                "channel:echo:default",
            ))
            .expect("healthy connector should register");
        healthy_adapter.push_inbound(sample_inbound_for(
            "z-healthy:default",
            "env-healthy",
            "hello from healthy poll",
        ));

        let processed =
            supervisor.poll_inbound(8).await.expect("poll should continue after adapter failure");

        assert_eq!(processed, 1, "healthy connector events should still be processed");
        let status = supervisor
            .status("z-healthy:default")
            .expect("healthy connector status should resolve");
        assert!(
            status.last_inbound_unix_ms.is_some(),
            "healthy connector should update last inbound timestamp"
        );
        let logs = supervisor
            .list_logs("a-failing:default", 8)
            .expect("failing connector logs should be readable");
        let poll_error = logs
            .iter()
            .find(|entry| entry.event_type == "inbound.poll_error")
            .expect("poll error warning should be recorded");
        assert_eq!(poll_error.level, "warn");
        assert_eq!(
            poll_error.message,
            "adapter inbound poll failed; continuing with remaining connectors"
        );
        let details =
            poll_error.details.as_ref().expect("poll error should include diagnostic details");
        assert_eq!(
            details.get("error").and_then(Value::as_str),
            Some("simulated inbound poll failure")
        );
    }

    #[tokio::test]
    async fn replay_and_discard_dead_letter_update_queue_state() {
        let (_tempdir, supervisor, _adapter) = open_supervisor();
        supervisor.register_connector(&sample_spec()).expect("register should succeed");

        let outbound = sample_outbound_request("env-dead-letter", "hello [connector-crash-once]");
        supervisor
            .store()
            .enqueue_outbox_if_absent(&outbound, 5, 1_000)
            .expect("enqueue should succeed");
        let due = supervisor
            .store()
            .load_due_outbox(1_000, 1, Some("echo:default"), false)
            .expect("due outbox query should succeed");
        let entry = due.first().expect("entry should be claimed");
        supervisor
            .store()
            .move_outbox_to_dead_letter(
                entry.outbox_id,
                entry.claim_token.as_str(),
                "manual dead",
                1_100,
            )
            .expect("dead letter move should succeed");

        let dead_letter = supervisor
            .list_dead_letters("echo:default", 10)
            .expect("dead letter list should succeed")
            .into_iter()
            .next()
            .expect("dead letter should exist");
        supervisor
            .replay_dead_letter("echo:default", dead_letter.dead_letter_id)
            .expect("replay should succeed");
        let queue_after_replay = supervisor
            .queue_snapshot("echo:default")
            .expect("queue snapshot after replay should succeed");
        assert_eq!(queue_after_replay.pending_outbox, 1);
        assert_eq!(queue_after_replay.dead_letters, 0);

        let replayed_dead = supervisor
            .store()
            .load_due_outbox(
                super::unix_ms_now().expect("clock should be available").saturating_add(1),
                1,
                Some("echo:default"),
                false,
            )
            .expect("replayed outbox should be due")
            .into_iter()
            .next()
            .expect("replayed row should exist");
        supervisor
            .store()
            .move_outbox_to_dead_letter(
                replayed_dead.outbox_id,
                replayed_dead.claim_token.as_str(),
                "dead again",
                1_300,
            )
            .expect("dead letter move after replay should succeed");
        let redied = supervisor
            .list_dead_letters("echo:default", 10)
            .expect("dead letter list should remain readable")
            .into_iter()
            .next()
            .expect("dead letter should exist after replay");
        supervisor
            .discard_dead_letter("echo:default", redied.dead_letter_id)
            .expect("discard should succeed");
        let queue_after_discard = supervisor
            .queue_snapshot("echo:default")
            .expect("queue snapshot after discard should succeed");
        assert_eq!(queue_after_discard.dead_letters, 0);
    }

    #[tokio::test]
    async fn repeated_dead_letter_recovery_cycles_keep_queue_accounting_stable() {
        let (_tempdir, supervisor, _adapter) = open_supervisor();
        supervisor.register_connector(&sample_spec()).expect("register should succeed");

        const CYCLES: usize = 8;
        for cycle in 0..CYCLES {
            let enqueue_time = 2_000_i64 + (cycle as i64 * 100);
            let envelope_id = format!("env-dead-letter-soak-{cycle}");
            let outbound = sample_outbound_request(
                envelope_id.as_str(),
                format!("dead letter cycle {cycle}").as_str(),
            );
            supervisor
                .store()
                .enqueue_outbox_if_absent(&outbound, 5, enqueue_time)
                .expect("enqueue should succeed");
            let claimed = supervisor
                .store()
                .load_due_outbox(enqueue_time, 1, Some("echo:default"), false)
                .expect("due outbox query should succeed")
                .into_iter()
                .find(|entry| entry.envelope_id == envelope_id)
                .expect("cycle outbox row should be claimed");
            supervisor
                .store()
                .move_outbox_to_dead_letter(
                    claimed.outbox_id,
                    claimed.claim_token.as_str(),
                    format!("manual dead {cycle}").as_str(),
                    enqueue_time + 1,
                )
                .expect("dead letter move should succeed");

            let dead_letter = supervisor
                .list_dead_letters("echo:default", 16)
                .expect("dead letter list should succeed")
                .into_iter()
                .find(|entry| entry.envelope_id == envelope_id)
                .expect("cycle dead letter should exist");
            let replayed = supervisor
                .replay_dead_letter("echo:default", dead_letter.dead_letter_id)
                .expect("replay should succeed");
            assert_eq!(replayed.envelope_id, envelope_id);

            let queue_after_replay = supervisor
                .queue_snapshot("echo:default")
                .expect("queue snapshot after replay should succeed");
            assert_eq!(queue_after_replay.pending_outbox, 1);
            assert_eq!(queue_after_replay.dead_letters, 0);

            let replayed_entry = supervisor
                .store()
                .load_due_outbox(
                    super::unix_ms_now().expect("clock should be available").saturating_add(1),
                    1,
                    Some("echo:default"),
                    false,
                )
                .expect("replayed outbox should be due")
                .into_iter()
                .find(|entry| entry.envelope_id == envelope_id)
                .expect("replayed cycle row should be claimed");
            supervisor
                .store()
                .move_outbox_to_dead_letter(
                    replayed_entry.outbox_id,
                    replayed_entry.claim_token.as_str(),
                    format!("dead again {cycle}").as_str(),
                    enqueue_time + 3,
                )
                .expect("replayed outbox should move back to dead letters");
            let redied = supervisor
                .list_dead_letters("echo:default", 16)
                .expect("redied dead letter list should succeed")
                .into_iter()
                .find(|entry| entry.envelope_id == envelope_id)
                .expect("redied dead letter should exist");
            let discarded = supervisor
                .discard_dead_letter("echo:default", redied.dead_letter_id)
                .expect("discard should succeed");
            assert_eq!(discarded.envelope_id, envelope_id);

            let queue_after_discard = supervisor
                .queue_snapshot("echo:default")
                .expect("queue snapshot after discard should succeed");
            assert_eq!(queue_after_discard.pending_outbox, 0);
            assert_eq!(queue_after_discard.due_outbox, 0);
            assert_eq!(queue_after_discard.claimed_outbox, 0);
            assert_eq!(queue_after_discard.dead_letters, 0);
        }
    }

    #[tokio::test]
    async fn permanent_auth_failure_sets_auth_failed_readiness() {
        let tempdir = TempDir::new().expect("tempdir should initialize");
        let store = Arc::new(
            ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
                .expect("store should initialize"),
        );
        let adapter = Arc::new(PermanentFailureAdapter {
            reason: "discord authentication failed during outbound send (status=401): unauthorized",
        });
        let supervisor = ConnectorSupervisor::new(
            store,
            Arc::new(RouterStub),
            vec![adapter],
            ConnectorSupervisorConfig {
                min_retry_delay_ms: 1,
                base_retry_delay_ms: 1,
                max_retry_delay_ms: 8,
                ..ConnectorSupervisorConfig::default()
            },
        );
        supervisor.register_connector(&sample_spec()).expect("register should succeed");
        let outbound = OutboundMessageRequest {
            envelope_id: "env-auth-failure".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "auth failure".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 30_000,
            max_payload_bytes: 16_384,
        };
        supervisor.enqueue_outbound(&outbound).expect("enqueue should succeed");
        let drain = supervisor
            .drain_due_outbox_for_connector("echo:default", 1)
            .await
            .expect("drain should succeed");
        assert_eq!(drain.dead_lettered, 1, "permanent auth failure should dead-letter the entry");
        let status = supervisor.status("echo:default").expect("status should resolve");
        assert_eq!(status.readiness, ConnectorReadiness::AuthFailed);
    }

    #[tokio::test]
    async fn runtime_snapshot_reports_connector_metrics() {
        let (_tempdir, supervisor, _adapter) = open_supervisor();
        supervisor.register_connector(&sample_spec()).expect("register should succeed");
        supervisor
            .ingest_inbound(sample_inbound("hello metrics"))
            .await
            .expect("first ingest should succeed");
        supervisor
            .ingest_inbound(sample_inbound("hello metrics"))
            .await
            .expect("duplicate ingest should succeed");
        let runtime = supervisor
            .runtime_snapshot("echo:default")
            .expect("runtime snapshot should resolve")
            .expect("runtime snapshot should be present");
        let metrics = runtime
            .get("metrics")
            .and_then(Value::as_object)
            .expect("runtime snapshot should include metrics object");
        assert_eq!(
            metrics.get("inbound_events_processed").and_then(Value::as_u64),
            Some(2),
            "received + duplicate should count toward inbound processed window"
        );
        assert_eq!(
            metrics.get("inbound_dedupe_hits").and_then(Value::as_u64),
            Some(1),
            "duplicate event should increment dedupe hit counter"
        );
        assert!(
            metrics.get("outbound_sends_ok").and_then(Value::as_u64).unwrap_or(0) >= 1,
            "first routed message should produce at least one delivered outbound in metrics window"
        );
        let route_latency = metrics
            .get("route_message_latency_ms")
            .and_then(Value::as_object)
            .expect("metrics should include route latency summary");
        assert!(
            route_latency.get("sample_count").and_then(Value::as_u64).unwrap_or(0) >= 1,
            "route latency summary should include at least one sample"
        );
        let queue = runtime
            .get("queue")
            .and_then(Value::as_object)
            .expect("runtime snapshot should include queue object");
        assert_eq!(
            queue.get("pending_outbox").and_then(Value::as_u64),
            Some(0),
            "successful immediate drain should leave no pending outbox entries"
        );
        assert_eq!(
            runtime
                .get("saturation")
                .and_then(Value::as_object)
                .and_then(|value| value.get("state"))
                .and_then(Value::as_str),
            Some("nominal"),
            "empty queue should report nominal saturation"
        );
    }

    #[tokio::test]
    async fn pausing_queue_blocks_background_drain_until_force_drained() {
        let (_tempdir, supervisor, _adapter) = open_supervisor();
        supervisor.register_connector(&sample_spec()).expect("register should succeed");
        supervisor
            .enqueue_outbound(&OutboundMessageRequest {
                envelope_id: "env-pause".to_owned(),
                connector_id: "echo:default".to_owned(),
                conversation_id: "c1".to_owned(),
                reply_thread_id: None,
                in_reply_to_message_id: None,
                text: "pause me".to_owned(),
                broadcast: false,
                auto_ack_text: None,
                auto_reaction: None,
                attachments: Vec::new(),
                structured_json: None,
                a2ui_update: None,
                timeout_ms: 30_000,
                max_payload_bytes: 16_384,
            })
            .expect("enqueue should succeed");
        let paused = supervisor
            .set_queue_paused("echo:default", true, Some("operator_pause"))
            .expect("queue pause should succeed");
        assert!(paused.paused, "queue snapshot should report paused state");

        let background = supervisor
            .drain_due_outbox_for_connector("echo:default", 10)
            .await
            .expect("background drain should succeed");
        assert_eq!(background.processed, 0, "paused queue should not drain in background mode");

        let force = supervisor
            .drain_due_outbox_for_connector_force("echo:default", 10)
            .await
            .expect("force drain should succeed");
        assert_eq!(force.delivered, 1, "force drain should still dispatch queued work");
    }

    #[test]
    fn status_falls_back_to_kind_availability_when_runtime_adapter_is_missing() {
        let (_tempdir, supervisor, _adapter) = open_supervisor();
        supervisor
            .register_connector(&sample_spec_with(
                "slack:default",
                ConnectorKind::Slack,
                "channel:slack:default",
            ))
            .expect("register should succeed without a slack runtime adapter");

        let status = supervisor.status("slack:default").expect("status should resolve");
        assert_eq!(status.kind, ConnectorKind::Slack);
        assert_eq!(status.availability, ConnectorAvailability::Deferred);
    }
}
