//! Unit tests for `ConnectorSupervisor`: inbound ingestion and dedupe, outbox
//! draining, retry/dead-letter rules, and admin operations via stub adapters.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
#[cfg(feature = "qa-fault-injection")]
use palyra_common::qa_fault_injection::{
    DeterministicQaFaultController, QaFaultAction, QaFaultActivation, QaFaultActiveBarrier,
    QaFaultCheckpoint, QaFaultControllerRecord, QaFaultDirective, QaFaultInjectionPlan,
    QaFaultProbe, QaFaultProbeError, QaFaultProbeHandle, QaFaultRecoveryClass,
    QA_FAULT_INJECTION_PLAN_FORMAT, QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
};
use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::Notify;

use crate::{
    protocol::{
        ConnectorAvailability, ConnectorInstanceSpec, ConnectorKind, ConnectorReadiness,
        DeliveryOutcome, OutboundMessageRequest, RetryClass, RoutedOutboundMessage,
    },
    storage::{
        ChannelIngressRecord, ChannelIngressStatus, ConnectorStore, DeliveryIntentDraft,
        DeliveryIntentStatus, OutboxReconciliationEvidence,
    },
};

use super::{
    unix_ms_now, ConnectorAdapter, ConnectorAdapterError, ConnectorRouter, ConnectorRouterError,
    ConnectorSupervisor, ConnectorSupervisorConfig, ConnectorSupervisorError, DeliveryPipelineMode,
};

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
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAS".to_owned()),
            run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAR".to_owned()),
            retry_attempt: 0,
            route_message_latency_ms: Some(1),
        })
    }
}

struct RouteErrorRouter {
    message: &'static str,
}

#[async_trait]
impl ConnectorRouter for RouteErrorRouter {
    async fn route_inbound(
        &self,
        _principal: &str,
        _event: &crate::protocol::InboundMessageEvent,
    ) -> Result<crate::protocol::RouteInboundResult, ConnectorRouterError> {
        Err(ConnectorRouterError::Message(self.message.to_owned()))
    }
}

#[derive(Default)]
struct FlakyAdapter {
    attempts: Mutex<HashMap<String, usize>>,
    inbound_events: Mutex<VecDeque<crate::protocol::InboundMessageEvent>>,
    stopped_connectors: Mutex<Vec<String>>,
}

impl FlakyAdapter {
    fn push_inbound(&self, event: crate::protocol::InboundMessageEvent) {
        self.inbound_events
            .lock()
            .expect("inbound queue lock should not be poisoned")
            .push_back(event);
    }

    fn stopped_connectors(&self) -> Vec<String> {
        self.stopped_connectors
            .lock()
            .expect("stopped connector lock should not be poisoned")
            .clone()
    }

    fn sends_for(&self, envelope_id: &str) -> usize {
        self.attempts
            .lock()
            .expect("attempts lock should not be poisoned")
            .get(envelope_id)
            .copied()
            .unwrap_or(0)
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

    fn stop_runtime(&self, connector_id: &str) -> Result<(), ConnectorAdapterError> {
        self.stopped_connectors
            .lock()
            .map_err(|_| {
                ConnectorAdapterError::Backend(
                    "flaky adapter stopped connector lock poisoned".to_owned(),
                )
            })?
            .push(connector_id.to_owned());
        Ok(())
    }

    async fn poll_inbound(
        &self,
        _instance: &crate::storage::ConnectorInstanceRecord,
        limit: usize,
    ) -> Result<Vec<crate::protocol::InboundMessageEvent>, ConnectorAdapterError> {
        let mut queue = self.inbound_events.lock().map_err(|_| {
            ConnectorAdapterError::Backend("flaky adapter inbound queue lock poisoned".to_owned())
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

#[derive(Default)]
struct GatedAdapter {
    started: Notify,
    release: Notify,
    sends: AtomicUsize,
}

impl GatedAdapter {
    fn release_delivery(&self) {
        self.release.notify_one();
    }

    fn sends(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ConnectorAdapter for GatedAdapter {
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
        self.started.notify_one();
        self.release.notified().await;
        let sequence = self.sends.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(DeliveryOutcome::Delivered {
            native_message_id: format!("native-{}-{sequence}", request.envelope_id),
        })
    }
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

#[derive(Default)]
struct OutcomeUnknownAdapter {
    sends: AtomicUsize,
}

impl OutcomeUnknownAdapter {
    fn sends(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ConnectorAdapter for OutcomeUnknownAdapter {
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
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(DeliveryOutcome::OutcomeUnknown { reason: "response lost after send".to_owned() })
    }
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
    open_supervisor_with_router(Arc::new(RouterStub))
}

fn open_supervisor_with_router(
    router: Arc<dyn ConnectorRouter>,
) -> (TempDir, ConnectorSupervisor, Arc<FlakyAdapter>) {
    open_supervisor_with_router_and_config(
        router,
        ConnectorSupervisorConfig {
            min_retry_delay_ms: 1,
            base_retry_delay_ms: 1,
            max_retry_delay_ms: 8,
            ..ConnectorSupervisorConfig::default()
        },
    )
}

fn open_supervisor_with_router_and_config(
    router: Arc<dyn ConnectorRouter>,
    config: ConnectorSupervisorConfig,
) -> (TempDir, ConnectorSupervisor, Arc<FlakyAdapter>) {
    let tempdir = TempDir::new().expect("tempdir should initialize");
    let store = std::sync::Arc::new(
        ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
            .expect("store should initialize"),
    );
    let adapter = Arc::new(FlakyAdapter::default());
    let supervisor = ConnectorSupervisor::new(store, router, vec![adapter.clone()], config);
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

fn stale_claimed_ingress(
    supervisor: &ConnectorSupervisor,
    event: &crate::protocol::InboundMessageEvent,
) -> ChannelIngressRecord {
    let now = unix_ms_now().expect("clock should be available").saturating_sub(10_000);
    supervisor
        .store()
        .enqueue_channel_ingress_if_absent(event, "channel:echo:default", now, 3, 86_400_000)
        .expect("ingress enqueue should succeed");
    supervisor
        .store()
        .load_due_channel_ingress(now, 1, Some(event.connector_id.as_str()), 1, false)
        .expect("ingress claim should succeed")
        .into_iter()
        .next()
        .expect("ingress row should be claimed")
}

fn delivery_intent_fixture(
    record: &ChannelIngressRecord,
    outbox_envelope_id: &str,
    status: DeliveryIntentStatus,
) -> DeliveryIntentDraft {
    DeliveryIntentDraft {
        intent_id: format!(
            "delivery:{}:{}:{}",
            record.connector_id, record.ingress_event_id, outbox_envelope_id
        ),
        connector_id: record.connector_id.clone(),
        ingress_event_id: record.ingress_event_id,
        ingress_envelope_id: record.envelope_id.clone(),
        session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAS".to_owned()),
        run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAR".to_owned()),
        principal: record.principal.clone(),
        conversation_id: record.conversation_id.clone(),
        outbox_envelope_id: outbox_envelope_id.to_owned(),
        output_index: 0,
        payload_hash: "fixture-payload-hash".to_owned(),
        visible_text_preview: "fixture preview".to_owned(),
        status,
        redaction_summary_json: None,
    }
}

#[test]
fn disabling_connector_stops_adapter_runtime() {
    let (_tempdir, supervisor, adapter) = open_supervisor();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");

    let status = supervisor.set_enabled("echo:default", false).expect("disable should succeed");

    assert!(!status.enabled);
    assert_eq!(adapter.stopped_connectors(), vec!["echo:default".to_owned()]);
}

#[test]
fn removing_connector_stops_adapter_runtime_before_storage_delete() {
    let (_tempdir, supervisor, adapter) = open_supervisor();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");

    supervisor.remove_connector("echo:default").expect("remove should succeed");

    assert_eq!(adapter.stopped_connectors(), vec!["echo:default".to_owned()]);
    assert!(
        supervisor.status("echo:default").is_err(),
        "removed connector should no longer be visible in storage"
    );
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
async fn persisted_ingress_before_route_replays_after_restart() {
    let (_tempdir, supervisor, adapter) = open_supervisor();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");
    let event = sample_inbound_for("echo:default", "env-before-route-crash", "hello persisted");
    let now = 1_000;

    let enqueued = supervisor
        .store()
        .enqueue_channel_ingress_if_absent(&event, "channel:echo:default", now, 3, 60_000)
        .expect("ingress enqueue should succeed");
    let before = supervisor.queue_snapshot("echo:default").expect("queue snapshot should load");
    assert_eq!(before.pending_ingress, 1);
    assert_eq!(adapter.sends_for("env-before-route-crash:0"), 0);

    let outcomes = supervisor
        .process_due_ingress_for_connector("echo:default", 8, false)
        .await
        .expect("persisted ingress should route after restart");

    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0].ingress_status.as_deref(),
        Some(ChannelIngressStatus::Completed.as_str())
    );
    assert_eq!(
        adapter.sends_for("env-before-route-crash:0"),
        1,
        "replayed ingress should produce exactly one physical send"
    );
    let record = supervisor
        .store()
        .get_channel_ingress_event("echo:default", enqueued.record.ingress_event_id)
        .expect("ingress record should be readable");
    assert_eq!(record.status, ChannelIngressStatus::Completed);
}

#[tokio::test]
async fn stale_claim_replays_without_duplicate_send_or_ingress_row() {
    let (_tempdir, supervisor, adapter) = open_supervisor();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");
    let event = sample_inbound_for("echo:default", "env-claimed-crash", "hello claimed");
    let claimed = stale_claimed_ingress(&supervisor, &event);
    assert_eq!(claimed.status, ChannelIngressStatus::Claimed);

    let outcomes = supervisor
        .process_due_ingress_for_connector("echo:default", 8, false)
        .await
        .expect("stale claim should be reclaimed and processed");
    assert_eq!(outcomes.len(), 1);
    let duplicate = supervisor
        .ingest_inbound(event)
        .await
        .expect("completed tombstone should make duplicate ingest safe");

    assert!(duplicate.duplicate);
    assert_eq!(duplicate.enqueued_outbound, 0);
    assert_eq!(
        adapter.sends_for("env-claimed-crash:0"),
        1,
        "completed ingress tombstone should prevent a duplicate send"
    );
    let ingress_rows = supervisor
        .store()
        .list_channel_ingress_events("echo:default", None, 16)
        .expect("ingress rows should be readable");
    assert_eq!(
        ingress_rows.iter().filter(|record| record.envelope_id == "env-claimed-crash").count(),
        1
    );
}

#[tokio::test]
async fn crash_after_intent_replays_outbox_once() {
    let (_tempdir, supervisor, adapter) = open_supervisor();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");
    let event = sample_inbound_for("echo:default", "env-after-intent-crash", "hello intent");
    let claimed = stale_claimed_ingress(&supervisor, &event);
    let draft =
        delivery_intent_fixture(&claimed, "env-after-intent-crash:0", DeliveryIntentStatus::Queued);
    supervisor
        .store()
        .upsert_delivery_intent(&draft, 1_100)
        .expect("pre-crash intent should upsert");

    let outcomes = supervisor
        .process_due_ingress_for_connector("echo:default", 8, false)
        .await
        .expect("reroute should finish missing outbox work");

    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        adapter.sends_for("env-after-intent-crash:0"),
        1,
        "replayed intent should create and deliver one outbox row"
    );
    let intents = supervisor
        .store()
        .list_delivery_intents("echo:default", None, 16)
        .expect("delivery intents should be readable");
    assert_eq!(intents.iter().filter(|intent| intent.intent_id == draft.intent_id).count(), 1);
    let intent = supervisor
        .store()
        .get_delivery_intent(draft.intent_id.as_str())
        .expect("delivery intent should be readable");
    assert_eq!(intent.status, DeliveryIntentStatus::Delivered);
}

#[tokio::test]
async fn crash_after_outbox_enqueue_replays_without_duplicate_physical_send() {
    let (_tempdir, supervisor, adapter) = open_supervisor();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");
    let event = sample_inbound_for("echo:default", "env-after-outbox-crash", "hello outbox");
    let claimed = stale_claimed_ingress(&supervisor, &event);
    let outbox_envelope_id = "env-after-outbox-crash:0";
    let draft = delivery_intent_fixture(&claimed, outbox_envelope_id, DeliveryIntentStatus::Queued);
    let request = sample_outbound_request(outbox_envelope_id, "hello outbox");
    supervisor
        .store()
        .upsert_delivery_intent(&draft, 1_100)
        .expect("pre-crash intent should upsert");
    supervisor
        .store()
        .enqueue_outbox_if_absent(&request, 3, 1_100)
        .expect("pre-crash outbox enqueue should succeed");

    let outcomes = supervisor
        .process_due_ingress_for_connector("echo:default", 8, false)
        .await
        .expect("reroute should reuse existing outbox work");

    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        adapter.sends_for(outbox_envelope_id),
        1,
        "existing outbox row should be delivered once after replay"
    );
    let queue =
        supervisor.queue_snapshot("echo:default").expect("queue snapshot should be readable");
    assert_eq!(queue.pending_outbox + queue.due_outbox + queue.claimed_outbox, 0);
    let intent = supervisor
        .store()
        .get_delivery_intent(draft.intent_id.as_str())
        .expect("delivery intent should be readable");
    assert_eq!(intent.status, DeliveryIntentStatus::Delivered);
}

#[tokio::test]
async fn route_quarantine_does_not_enqueue_or_send_outbound() {
    let (_tempdir, supervisor, adapter) = open_supervisor_with_router(Arc::new(RouteErrorRouter {
        message: "invalid envelope schema",
    }));
    supervisor.register_connector(&sample_spec()).expect("register should succeed");

    let outcome = supervisor
        .ingest_inbound(sample_inbound_for(
            "echo:default",
            "env-quarantine-crash",
            "malformed inbound",
        ))
        .await
        .expect("quarantine route error should be recorded");

    assert_eq!(outcome.ingress_status.as_deref(), Some(ChannelIngressStatus::Quarantined.as_str()));
    assert_eq!(adapter.sends_for("env-quarantine-crash:0"), 0);
    let queue =
        supervisor.queue_snapshot("echo:default").expect("queue snapshot should be readable");
    assert_eq!(queue.quarantined_ingress, 1);
    assert_eq!(queue.pending_outbox + queue.due_outbox + queue.claimed_outbox, 0);
    assert!(
        supervisor
            .store()
            .list_delivery_intents("echo:default", None, 16)
            .expect("delivery intent list should be readable")
            .is_empty(),
        "quarantined ingress must not produce delivery intents"
    );
}

#[tokio::test]
async fn proven_safe_retry_requeues_intent_before_replay_delivery() {
    let (_tempdir, supervisor, adapter) = open_supervisor();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");

    let ingest = supervisor
        .ingest_inbound(sample_inbound_for(
            "echo:default",
            "env-platform-unknown",
            "hello [connector-crash-once]",
        ))
        .await
        .expect("ingest should enqueue and attempt delivery");
    assert!(ingest.accepted);
    assert_eq!(adapter.sends_for("env-platform-unknown:0"), 1);
    let queued_intents = supervisor
        .store()
        .list_delivery_intents("echo:default", Some(DeliveryIntentStatus::Queued), 16)
        .expect("safe retry intents should be readable");
    assert_eq!(queued_intents.len(), 1);
    assert_eq!(queued_intents[0].outbox_envelope_id, "env-platform-unknown:0");

    let mut delivered = 0_usize;
    for _ in 0..20 {
        let drained = supervisor.drain_due_outbox(16).await.expect("retry drain should succeed");
        delivered = delivered.saturating_add(drained.delivered);
        if delivered >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    assert_eq!(delivered, 1);
    assert_eq!(
        adapter.sends_for("env-platform-unknown:0"),
        2,
        "an adapter-proven no-effect outcome may be sent again"
    );
    let intent = supervisor
        .store()
        .get_delivery_intent(queued_intents[0].intent_id.as_str())
        .expect("delivery intent should be readable after retry");
    assert_eq!(intent.status, DeliveryIntentStatus::Delivered);
}

#[tokio::test]
async fn outcome_unknown_stays_parked_until_explicit_reconciliation() {
    let tempdir = TempDir::new().expect("tempdir should initialize");
    let store = Arc::new(
        ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
            .expect("store should initialize"),
    );
    let adapter = Arc::new(OutcomeUnknownAdapter::default());
    let supervisor = ConnectorSupervisor::new(
        store,
        Arc::new(RouterStub),
        vec![adapter.clone()],
        ConnectorSupervisorConfig::default(),
    );
    supervisor.register_connector(&sample_spec()).expect("register should succeed");

    let ingest = supervisor
        .ingest_inbound(sample_inbound_for(
            "echo:default",
            "env-response-lost",
            "hello after ambiguous send",
        ))
        .await
        .expect("ambiguous delivery should be durably parked");
    assert!(ingest.accepted);
    assert_eq!(adapter.sends(), 1);

    let no_blind_retry =
        supervisor.drain_due_outbox(16).await.expect("parked drain should succeed");
    assert_eq!(no_blind_retry.processed, 0);
    assert_eq!(adapter.sends(), 1, "outcome-unknown must not trigger a second physical send");
    let unknown = supervisor
        .store()
        .list_outbox_unknown("echo:default", 10)
        .expect("outcome-unknown row should be operator-visible");
    assert_eq!(unknown.len(), 1);
    let intents = supervisor
        .store()
        .list_delivery_intents(
            "echo:default",
            Some(DeliveryIntentStatus::PlatformOutcomeUnknown),
            10,
        )
        .expect("outcome-unknown intent should be operator-visible");
    assert_eq!(intents.len(), 1);

    supervisor
        .store()
        .reconcile_outbox_unknown(
            unknown[0].outbox_id,
            &OutboxReconciliationEvidence::Delivered {
                native_message_id: "native-reconciled".to_owned(),
            },
            unix_ms_now().expect("clock should resolve"),
        )
        .expect("platform receipt should reconcile delivery");
    let reconciled = supervisor
        .store()
        .get_delivery_intent(intents[0].intent_id.as_str())
        .expect("reconciled intent should load");
    assert_eq!(reconciled.status, DeliveryIntentStatus::Delivered);
    assert_eq!(adapter.sends(), 1, "reconciliation must not call the adapter again");
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
    let enqueue = supervisor.enqueue_outbound(&outbound).expect("outbox enqueue should succeed");
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
async fn expired_in_flight_claim_is_parked_without_a_duplicate_send() {
    let tempdir = TempDir::new().expect("tempdir should initialize");
    let store = Arc::new(
        ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
            .expect("store should initialize"),
    );
    let adapter = Arc::new(GatedAdapter::default());
    let supervisor = Arc::new(ConnectorSupervisor::new(
        store.clone(),
        Arc::new(RouterStub),
        vec![adapter.clone()],
        ConnectorSupervisorConfig::default(),
    ));
    supervisor.register_connector(&sample_spec()).expect("register should succeed");
    supervisor
        .enqueue_outbound(&sample_outbound_request("env-expired-in-flight", "deliver once"))
        .expect("outbox enqueue should succeed");

    let started = adapter.started.notified();
    let drain = tokio::spawn({
        let supervisor = supervisor.clone();
        async move { supervisor.drain_due_outbox(1).await }
    });
    started.await;

    let reclaimed = store
        .load_due_outbox(i64::MAX / 4, 1, Some("echo:default"), false)
        .expect("expired claim scan should succeed");
    assert!(
        reclaimed.is_empty(),
        "an expired effect-started claim must be parked instead of reclaimed"
    );
    adapter.release_delivery();
    let drain_error = drain
        .await
        .expect("drain task should join")
        .expect_err("the stale sender must not acknowledge a parked row");
    assert!(matches!(
        drain_error,
        ConnectorSupervisorError::Store(
            super::super::storage::ConnectorStoreError::OutboxNotFound(_)
        )
    ));
    assert_eq!(adapter.sends(), 1);
    assert_eq!(store.list_outbox_unknown("echo:default", 10).unwrap().len(), 1);

    let unknown = store.list_outbox_unknown("echo:default", 1).unwrap().remove(0);
    store
        .reconcile_outbox_unknown(
            unknown.outbox_id,
            &OutboxReconciliationEvidence::Delivered {
                native_message_id: "native-env-expired-in-flight-1".to_owned(),
            },
            i64::MAX / 4,
        )
        .expect("platform receipt should reconcile the unknown effect");
    let later = supervisor.drain_due_outbox(1).await.expect("later drain should succeed");
    assert_eq!(later.processed, 0);
    assert_eq!(adapter.sends(), 1, "reconciliation must not repeat the physical send");
}

#[cfg(feature = "qa-fault-injection")]
#[tokio::test]
async fn connector_fault_adapter_self_tests_every_registered_outbox_boundary() {
    struct FaultCase {
        point_id: &'static str,
        action: QaFaultAction,
        sends_at_fault: usize,
        outcome_unknown: bool,
        recovery_class: QaFaultRecoveryClass,
    }

    let cases = [
        FaultCase {
            point_id: "connector.outbox.before_intent",
            action: QaFaultAction::Disconnect,
            sends_at_fault: 0,
            outcome_unknown: false,
            recovery_class: QaFaultRecoveryClass::FailedClosed,
        },
        FaultCase {
            point_id: "connector.outbox.after_intent",
            action: QaFaultAction::Disconnect,
            sends_at_fault: 0,
            outcome_unknown: false,
            recovery_class: QaFaultRecoveryClass::FailedClosed,
        },
        FaultCase {
            point_id: "connector.outbox.before_effect",
            action: QaFaultAction::Timeout,
            sends_at_fault: 0,
            outcome_unknown: false,
            recovery_class: QaFaultRecoveryClass::FailedClosed,
        },
        FaultCase {
            point_id: "connector.outbox.during_delivery",
            action: QaFaultAction::Timeout,
            sends_at_fault: 0,
            outcome_unknown: true,
            recovery_class: QaFaultRecoveryClass::OutcomeUnknown,
        },
        FaultCase {
            point_id: "connector.outbox.after_effect_before_ack",
            action: QaFaultAction::Disconnect,
            sends_at_fault: 1,
            outcome_unknown: true,
            recovery_class: QaFaultRecoveryClass::OutcomeUnknown,
        },
        FaultCase {
            point_id: "connector.outbox.after_ack_before_transition",
            action: QaFaultAction::TerminateProcess,
            sends_at_fault: 1,
            outcome_unknown: true,
            recovery_class: QaFaultRecoveryClass::TransitionPending,
        },
    ];

    for (index, case) in cases.into_iter().enumerate() {
        let activation_id = format!("connector-boundary-{index}");
        let envelope_id = format!("env-fault-boundary-{index}");
        let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
            schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
            format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
            seed: 73,
            activations: vec![QaFaultActivation {
                id: activation_id.clone(),
                point_id: case.point_id.to_owned(),
                actor: Some("outbox-1".to_owned()),
                occurrence: 1,
                action: case.action.clone(),
            }],
        })
        .unwrap();
        let probe = QaFaultProbeHandle::from_probe(controller);
        let tempdir = TempDir::new().unwrap();
        let store =
            Arc::new(ConnectorStore::open(tempdir.path().join("connectors.sqlite3")).unwrap());
        let adapter = Arc::new(GatedAdapter::default());
        let supervisor = ConnectorSupervisor::new(
            store.clone(),
            Arc::new(RouterStub),
            vec![adapter.clone()],
            ConnectorSupervisorConfig::default(),
        )
        .with_qa_fault_probe(probe.clone());
        supervisor.register_connector(&sample_spec()).unwrap();
        supervisor
            .enqueue_outbound(&sample_outbound_request(envelope_id.as_str(), "fault me"))
            .unwrap();
        if case.sends_at_fault > 0 {
            adapter.release_delivery();
        }

        let error = supervisor
            .drain_due_outbox(1)
            .await
            .expect_err("the configured boundary must surface its typed fault");
        match error {
            ConnectorSupervisorError::QaFaultActivated { point_id, action, .. } => {
                assert_eq!(point_id, case.point_id);
                assert_eq!(action, case.action);
            }
            other => panic!("unexpected connector fault error: {other:?}"),
        }
        assert_eq!(adapter.sends(), case.sends_at_fault, "point={}", case.point_id);
        assert_eq!(
            probe.records().unwrap()[0].recovery_class,
            Some(case.recovery_class),
            "point={} must record subsystem-owned recovery",
            case.point_id
        );

        if case.outcome_unknown {
            let unknown = store.list_outbox_unknown("echo:default", 1).unwrap().remove(0);
            let no_blind_retry = supervisor.drain_due_outbox(1).await.unwrap();
            assert_eq!(no_blind_retry.processed, 0);
            assert_eq!(adapter.sends(), case.sends_at_fault);

            if case.sends_at_fault == 0 {
                store
                    .reconcile_outbox_unknown(
                        unknown.outbox_id,
                        &OutboxReconciliationEvidence::ConfirmedAbsent,
                        unix_ms_now().unwrap(),
                    )
                    .unwrap();
                adapter.release_delivery();
                let retry = supervisor.drain_due_outbox(1).await.unwrap();
                assert_eq!(retry.delivered, 1);
            } else {
                store
                    .reconcile_outbox_unknown(
                        unknown.outbox_id,
                        &OutboxReconciliationEvidence::Delivered {
                            native_message_id: format!("native-{envelope_id}-1"),
                        },
                        unix_ms_now().unwrap(),
                    )
                    .unwrap();
                assert_eq!(supervisor.drain_due_outbox(1).await.unwrap().processed, 0);
            }
        } else {
            assert!(
                store.list_outbox_unknown("echo:default", 1).unwrap().is_empty(),
                "point={} must remain retry-safe",
                case.point_id
            );
            adapter.release_delivery();
            let retry = supervisor.drain_due_outbox(1).await.unwrap();
            assert_eq!(retry.delivered, 1);
        }
        assert_eq!(adapter.sends(), 1, "point={} must produce one final send", case.point_id);
    }
}

#[cfg(feature = "qa-fault-injection")]
#[tokio::test]
async fn connector_fault_actor_does_not_alias_same_envelope_across_connectors() {
    let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 37,
        activations: vec![QaFaultActivation {
            id: "connector-identity-fault".to_owned(),
            point_id: "connector.outbox.before_effect".to_owned(),
            actor: Some("outbox-1".to_owned()),
            occurrence: 1,
            action: QaFaultAction::Timeout,
        }],
    })
    .unwrap();
    let probe = QaFaultProbeHandle::from_probe(controller);
    let tempdir = TempDir::new().unwrap();
    let store = Arc::new(ConnectorStore::open(tempdir.path().join("connectors.sqlite3")).unwrap());
    let adapter = Arc::new(FlakyAdapter::default());
    let supervisor = ConnectorSupervisor::new(
        store.clone(),
        Arc::new(RouterStub),
        vec![adapter.clone()],
        ConnectorSupervisorConfig::default(),
    )
    .with_qa_fault_probe(probe.clone());
    let mut first_spec = sample_spec();
    first_spec.connector_id = "echo:first".to_owned();
    let mut second_spec = sample_spec();
    second_spec.connector_id = "echo:second".to_owned();
    supervisor.register_connector(&first_spec).unwrap();
    supervisor.register_connector(&second_spec).unwrap();
    let mut first = sample_outbound_request("shared-envelope", "first connector");
    first.connector_id = first_spec.connector_id.clone();
    let mut second = sample_outbound_request("shared-envelope", "second connector");
    second.connector_id = second_spec.connector_id.clone();
    supervisor.enqueue_outbound(&first).unwrap();
    supervisor.enqueue_outbound(&second).unwrap();

    supervisor.drain_due_outbox(2).await.unwrap_err();

    assert_eq!(probe.records().unwrap()[0].actors, ["outbox-1"]);
    assert_eq!(adapter.sends_for("shared-envelope"), 0);
    let first_retry = store
        .load_due_outbox(unix_ms_now().unwrap(), 1, Some(first_spec.connector_id.as_str()), false)
        .unwrap();
    assert_eq!(first_retry.len(), 1);
    let unrelated = supervisor.queue_snapshot(second_spec.connector_id.as_str()).unwrap();
    assert_eq!(unrelated.pending_outbox, 1);
    assert_eq!(unrelated.claimed_outbox, 1);
    assert_eq!(unrelated.dead_letters, 0);
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn startup_reconciliation_releases_only_the_exact_ready_outbox_actor() {
    let tempdir = TempDir::new().unwrap();
    let store = Arc::new(ConnectorStore::open(tempdir.path().join("connectors.sqlite3")).unwrap());
    let supervisor = ConnectorSupervisor::new(
        store.clone(),
        Arc::new(RouterStub),
        vec![Arc::new(FlakyAdapter::default())],
        ConnectorSupervisorConfig::default(),
    );
    supervisor.register_connector(&sample_spec()).unwrap();
    for envelope_id in ["startup-ready-a", "startup-ready-b"] {
        supervisor
            .enqueue_outbound(&sample_outbound_request(envelope_id, "startup recovery"))
            .unwrap();
    }
    let claimed =
        store.load_due_outbox(unix_ms_now().unwrap(), 2, Some("echo:default"), false).unwrap();
    assert_eq!(claimed.len(), 2);

    let recovery = supervisor
        .reconcile_pending_qa_fault_actor("connector.outbox.before_intent", "outbox-1")
        .unwrap();

    assert_eq!(recovery, QaFaultRecoveryClass::FailedClosed);
    assert_eq!(
        supervisor
            .reconcile_pending_qa_fault_actor("connector.outbox.before_intent", "outbox-1")
            .unwrap(),
        QaFaultRecoveryClass::FailedClosed
    );
    let reclaimed =
        store.load_due_outbox(unix_ms_now().unwrap(), 2, Some("echo:default"), false).unwrap();
    assert_eq!(reclaimed.iter().map(|entry| entry.outbox_id).collect::<Vec<_>>(), [1]);
    assert!(supervisor
        .reconcile_pending_qa_fault_actor("connector.outbox.before_intent", "not-an-outbox")
        .is_err());
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn startup_reconciliation_parks_only_provable_effect_started_actors() {
    let tempdir = TempDir::new().unwrap();
    let store = Arc::new(ConnectorStore::open(tempdir.path().join("connectors.sqlite3")).unwrap());
    let supervisor = ConnectorSupervisor::new(
        store.clone(),
        Arc::new(RouterStub),
        vec![Arc::new(FlakyAdapter::default())],
        ConnectorSupervisorConfig::default(),
    );
    supervisor.register_connector(&sample_spec()).unwrap();
    for envelope_id in ["startup-effect-a", "startup-effect-b"] {
        supervisor
            .enqueue_outbound(&sample_outbound_request(envelope_id, "startup recovery"))
            .unwrap();
    }
    let now = unix_ms_now().unwrap();
    let claimed = store.load_due_outbox(now, 2, Some("echo:default"), false).unwrap();
    for entry in &claimed {
        store
            .mark_outbox_delivery_intent_started(entry.outbox_id, entry.claim_token.as_str(), now)
            .unwrap();
        store.mark_outbox_effect_started(entry.outbox_id, entry.claim_token.as_str(), now).unwrap();
    }

    assert_eq!(
        supervisor
            .reconcile_pending_qa_fault_actor("connector.outbox.during_delivery", "outbox-1")
            .unwrap(),
        QaFaultRecoveryClass::OutcomeUnknown
    );
    assert_eq!(
        supervisor
            .reconcile_pending_qa_fault_actor(
                "connector.outbox.after_ack_before_transition",
                "outbox-2",
            )
            .unwrap(),
        QaFaultRecoveryClass::TransitionPending
    );
    assert_eq!(
        supervisor
            .reconcile_pending_qa_fault_actor("connector.outbox.during_delivery", "outbox-1")
            .unwrap(),
        QaFaultRecoveryClass::OutcomeUnknown
    );
    assert_eq!(
        supervisor
            .reconcile_pending_qa_fault_actor(
                "connector.outbox.after_ack_before_transition",
                "outbox-2",
            )
            .unwrap(),
        QaFaultRecoveryClass::TransitionPending
    );
    assert_eq!(store.list_outbox_unknown("echo:default", 10).unwrap().len(), 2);
    assert!(supervisor
        .reconcile_pending_qa_fault_actor("connector.outbox.before_effect", "outbox-1")
        .is_err());
}

#[cfg(feature = "qa-fault-injection")]
#[derive(Clone)]
struct RecoveredBarrierProbe {
    releases: Arc<Mutex<Vec<String>>>,
    recoveries: Arc<Mutex<Vec<(String, QaFaultRecoveryClass)>>>,
    barrier: Arc<Mutex<Option<QaFaultActiveBarrier>>>,
}

#[cfg(feature = "qa-fault-injection")]
impl Default for RecoveredBarrierProbe {
    fn default() -> Self {
        Self::with_released_actors(Vec::new())
    }
}

#[cfg(feature = "qa-fault-injection")]
impl RecoveredBarrierProbe {
    fn fully_released() -> Self {
        Self::with_released_actors(vec!["outbox-2".to_owned(), "outbox-1".to_owned()])
    }

    fn with_released_actors(released_actors: Vec<String>) -> Self {
        Self {
            releases: Arc::new(Mutex::new(Vec::new())),
            recoveries: Arc::new(Mutex::new(Vec::new())),
            barrier: Arc::new(Mutex::new(Some(QaFaultActiveBarrier {
                activation_id: "connector-recovered-barrier".to_owned(),
                point_id: "connector.outbox.batch_before_effect".to_owned(),
                participants: 2,
                actors: vec!["outbox-1".to_owned(), "outbox-2".to_owned()],
                release_order: Some(vec!["outbox-2".to_owned(), "outbox-1".to_owned()]),
                released_actors,
            }))),
        }
    }
}

#[cfg(feature = "qa-fault-injection")]
impl QaFaultProbe for RecoveredBarrierProbe {
    fn checkpoint(
        &self,
        checkpoint: QaFaultCheckpoint<'_>,
    ) -> Result<QaFaultDirective, QaFaultProbeError> {
        if checkpoint.point_id == "connector.outbox.batch_before_effect"
            && matches!(checkpoint.actor, "outbox-1" | "outbox-2")
        {
            self.releases.lock().unwrap().push(checkpoint.actor.to_owned());
            if let Some(barrier) = self.barrier.lock().unwrap().as_mut() {
                barrier.released_actors.push(checkpoint.actor.to_owned());
            }
        }
        Ok(QaFaultDirective::Continue)
    }

    fn record_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), QaFaultProbeError> {
        if self.barrier.lock().unwrap().take().is_none() {
            return Err(QaFaultProbeError::RecoveryAlreadyRecorded(activation_id.to_owned()));
        }
        self.recoveries.lock().unwrap().push((activation_id.to_owned(), recovery_class));
        Ok(())
    }

    fn records(&self) -> Result<Vec<QaFaultControllerRecord>, QaFaultProbeError> {
        Ok(Vec::new())
    }

    fn active_barriers(&self) -> Result<Vec<QaFaultActiveBarrier>, QaFaultProbeError> {
        Ok(self.barrier.lock().unwrap().iter().cloned().collect())
    }
}

#[cfg(feature = "qa-fault-injection")]
#[tokio::test]
async fn recovered_barrier_consumes_terminal_actor_release_without_redispatch() {
    let probe = RecoveredBarrierProbe::default();
    let tempdir = TempDir::new().unwrap();
    let store = Arc::new(ConnectorStore::open(tempdir.path().join("connectors.sqlite3")).unwrap());
    let adapter = Arc::new(FlakyAdapter::default());
    let supervisor = ConnectorSupervisor::new(
        store.clone(),
        Arc::new(RouterStub),
        vec![adapter.clone()],
        ConnectorSupervisorConfig::default(),
    )
    .with_qa_fault_probe(QaFaultProbeHandle::from_probe(probe.clone()));
    supervisor.register_connector(&sample_spec()).unwrap();
    for envelope_id in ["barrier-terminal", "barrier-ready"] {
        supervisor
            .enqueue_outbound(&sample_outbound_request(envelope_id, "barrier restart"))
            .unwrap();
    }
    let now = unix_ms_now().unwrap();
    let claimed = store.load_due_outbox(now, 2, Some("echo:default"), false).unwrap();
    assert_eq!(claimed.iter().map(|entry| entry.outbox_id).collect::<Vec<_>>(), [1, 2]);
    store
        .move_outbox_to_dead_letter(
            claimed[0].outbox_id,
            claimed[0].claim_token.as_str(),
            "qa terminal actor",
            now,
        )
        .unwrap();
    store
        .schedule_outbox_retry(
            claimed[1].outbox_id,
            claimed[1].claim_token.as_str(),
            claimed[1].attempts,
            "qa reclaim ready actor",
            now,
        )
        .unwrap();
    store
        .mark_delivery_intent_retry_queued_for_outbox(
            claimed[1].connector_id.as_str(),
            claimed[1].envelope_id.as_str(),
            "qa reclaim ready actor",
            now,
        )
        .unwrap();

    let outcome = supervisor.drain_due_outbox(2).await.unwrap();

    assert_eq!(outcome.delivered, 1);
    assert_eq!(adapter.sends_for("barrier-terminal"), 0);
    assert_eq!(adapter.sends_for("barrier-ready"), 1);
    assert_eq!(*probe.releases.lock().unwrap(), ["outbox-2", "outbox-1"]);
    assert_eq!(
        *probe.recoveries.lock().unwrap(),
        [("connector-recovered-barrier".to_owned(), QaFaultRecoveryClass::Resumed)]
    );
}

#[cfg(feature = "qa-fault-injection")]
#[tokio::test]
async fn concurrent_adopted_barrier_drains_record_one_recovery_without_claim_race() {
    let probe = RecoveredBarrierProbe::fully_released();
    let tempdir = TempDir::new().unwrap();
    let store = Arc::new(ConnectorStore::open(tempdir.path().join("connectors.sqlite3")).unwrap());
    let adapter = Arc::new(GatedAdapter::default());
    let supervisor = Arc::new(
        ConnectorSupervisor::new(
            store,
            Arc::new(RouterStub),
            vec![adapter.clone()],
            ConnectorSupervisorConfig::default(),
        )
        .with_qa_fault_probe(QaFaultProbeHandle::from_probe(probe.clone())),
    );
    supervisor.register_connector(&sample_spec()).unwrap();
    for envelope_id in ["barrier-concurrent-a", "barrier-concurrent-b"] {
        supervisor
            .enqueue_outbound(&sample_outbound_request(envelope_id, "barrier concurrency"))
            .unwrap();
    }

    let first_started = adapter.started.notified();
    let first_supervisor = supervisor.clone();
    let first = tokio::spawn(async move { first_supervisor.drain_due_outbox(2).await });
    first_started.await;
    let second_supervisor = supervisor.clone();
    let second = tokio::spawn(async move { second_supervisor.drain_due_outbox(2).await });
    tokio::task::yield_now().await;
    assert!(!second.is_finished(), "a concurrent drain must wait for active barrier adoption");
    let second_delivery_started = adapter.started.notified();
    adapter.release_delivery();
    second_delivery_started.await;
    adapter.release_delivery();

    let first_outcome = first.await.unwrap().unwrap();
    let second_outcome = second.await.unwrap().unwrap();
    assert_eq!(first_outcome.delivered + second_outcome.delivered, 2);
    assert_eq!(adapter.sends(), 2);
    assert_eq!(probe.recoveries.lock().unwrap().len(), 1);
    assert_eq!(supervisor.drain_due_outbox(2).await.unwrap().processed, 0);
}

#[cfg(feature = "qa-fault-injection")]
#[tokio::test]
async fn connector_before_effect_barrier_releases_declared_actors_and_skips_batch_overflow() {
    let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 91,
        activations: vec![QaFaultActivation {
            id: "connector-batch-barrier".to_owned(),
            point_id: "connector.outbox.batch_before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 2 },
        }],
    })
    .unwrap();
    let probe = QaFaultProbeHandle::from_probe(controller);
    let tempdir = TempDir::new().unwrap();
    let store = Arc::new(ConnectorStore::open(tempdir.path().join("connectors.sqlite3")).unwrap());
    let adapter = Arc::new(FlakyAdapter::default());
    let supervisor = ConnectorSupervisor::new(
        store,
        Arc::new(RouterStub),
        vec![adapter.clone()],
        ConnectorSupervisorConfig::default(),
    )
    .with_qa_fault_probe(probe.clone());
    supervisor.register_connector(&sample_spec()).unwrap();
    for envelope_id in ["barrier-a", "barrier-b", "barrier-c"] {
        supervisor
            .enqueue_outbound(&sample_outbound_request(envelope_id, "barrier delivery"))
            .unwrap();
    }

    let outcome = supervisor.drain_due_outbox(3).await.unwrap();

    assert_eq!(outcome.delivered, 3);
    for envelope_id in ["barrier-a", "barrier-b", "barrier-c"] {
        assert_eq!(adapter.sends_for(envelope_id), 1);
    }
    let records = probe.records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].actors, ["outbox-1", "outbox-2"]);
    assert_eq!(records[0].recovery_class, Some(QaFaultRecoveryClass::Resumed));
}

#[cfg(feature = "qa-fault-injection")]
#[tokio::test]
async fn incomplete_connector_barrier_releases_claims_without_false_recovery() {
    let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 17,
        activations: vec![QaFaultActivation {
            id: "connector-incomplete-barrier".to_owned(),
            point_id: "connector.outbox.batch_before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 3 },
        }],
    })
    .unwrap();
    let probe = QaFaultProbeHandle::from_probe(controller);
    let tempdir = TempDir::new().unwrap();
    let store = Arc::new(ConnectorStore::open(tempdir.path().join("connectors.sqlite3")).unwrap());
    let adapter = Arc::new(FlakyAdapter::default());
    let supervisor = ConnectorSupervisor::new(
        store.clone(),
        Arc::new(RouterStub),
        vec![adapter.clone()],
        ConnectorSupervisorConfig::default(),
    )
    .with_qa_fault_probe(probe.clone());
    supervisor.register_connector(&sample_spec()).unwrap();
    for envelope_id in ["barrier-incomplete-a", "barrier-incomplete-b"] {
        supervisor
            .enqueue_outbound(&sample_outbound_request(envelope_id, "barrier delivery"))
            .unwrap();
    }

    let error = supervisor.drain_due_outbox(2).await.unwrap_err();

    assert!(matches!(error, ConnectorSupervisorError::Validation(_)));
    assert_eq!(adapter.sends_for("barrier-incomplete-a"), 0);
    assert_eq!(adapter.sends_for("barrier-incomplete-b"), 0);
    assert_eq!(probe.records().unwrap()[0].recovery_class, None);
    let reclaimed =
        store.load_due_outbox(unix_ms_now().unwrap(), 2, Some("echo:default"), false).unwrap();
    assert_eq!(reclaimed.len(), 2, "incomplete barrier claims must be released immediately");
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
    let status =
        supervisor.status("z-healthy:default").expect("healthy connector status should resolve");
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
            unix_ms_now().expect("clock should be available").saturating_add(1),
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
                unix_ms_now().expect("clock should be available").saturating_add(1),
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
async fn runtime_snapshot_reports_legacy_delivery_pipeline_warning() {
    let (_tempdir, supervisor, _adapter) = open_supervisor_with_router_and_config(
        Arc::new(RouterStub),
        ConnectorSupervisorConfig {
            delivery_pipeline_mode: DeliveryPipelineMode::Off,
            ..ConnectorSupervisorConfig::default()
        },
    );
    supervisor.register_connector(&sample_spec()).expect("register should succeed");

    let runtime = supervisor
        .runtime_snapshot("echo:default")
        .expect("runtime snapshot should resolve")
        .expect("runtime snapshot should be present");
    let delivery_pipeline = runtime
        .get("delivery_pipeline")
        .and_then(Value::as_object)
        .expect("runtime snapshot should include delivery pipeline mode");

    assert_eq!(delivery_pipeline.get("mode").and_then(Value::as_str), Some("off"));
    assert!(
        delivery_pipeline
            .get("legacy_warning")
            .and_then(Value::as_str)
            .is_some_and(|warning| warning.contains("production should use enforce")),
        "legacy rollout mode should emit an operator-visible warning"
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
