use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use palyra_connectors::{
    ConnectorAdapter, ConnectorAdapterError, ConnectorInstanceSpec, ConnectorKind, ConnectorRouter,
    ConnectorRouterError, ConnectorStore, ConnectorSupervisor, ConnectorSupervisorConfig,
    DeliveryOutcome, InboundMessageEvent, RetryClass, RouteInboundResult, RoutedOutboundMessage,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Debug, Deserialize)]
struct InboundFixture {
    envelope_id: String,
    conversation_id: String,
    sender_id: String,
    body: String,
    is_direct_message: bool,
    requested_broadcast: bool,
}

struct SimulatorRouter;

#[async_trait]
impl ConnectorRouter for SimulatorRouter {
    async fn route_inbound(
        &self,
        _principal: &str,
        event: &InboundMessageEvent,
    ) -> Result<RouteInboundResult, ConnectorRouterError> {
        Ok(RouteInboundResult {
            accepted: true,
            queued_for_retry: false,
            decision_reason: "routed".to_owned(),
            outputs: vec![RoutedOutboundMessage {
                text: event.body.clone(),
                thread_id: None,
                in_reply_to_message_id: event.adapter_message_id.clone(),
                broadcast: event.requested_broadcast,
                auto_ack_text: None,
                auto_reaction: None,
            }],
            route_key: Some(format!(
                "channel:{}:conversation:{}",
                event.connector_id, event.conversation_id
            )),
            retry_attempt: 0,
        })
    }
}

#[derive(Debug, Default)]
struct MockConnectorServer {
    state: Mutex<MockConnectorServerState>,
}

#[derive(Debug, Default)]
struct MockConnectorServerState {
    attempts: HashMap<String, u32>,
    delivered_native_ids: HashMap<String, String>,
}

impl MockConnectorServer {
    fn attempt_summary(&self) -> BTreeMap<String, u32> {
        self.state
            .lock()
            .map(|state| {
                state
                    .attempts
                    .iter()
                    .map(|(envelope_id, attempts)| (envelope_id.clone(), *attempts))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default()
    }

    fn delivered_count(&self) -> usize {
        self.state.lock().map(|state| state.delivered_native_ids.len()).unwrap_or_default()
    }
}

#[async_trait]
impl ConnectorAdapter for MockConnectorServer {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Echo
    }

    async fn send_outbound(
        &self,
        request: &palyra_connectors::OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        let mut state = self.state.lock().map_err(|_| {
            ConnectorAdapterError::Backend("mock connector server lock poisoned".to_owned())
        })?;
        let attempts = state.attempts.entry(request.envelope_id.clone()).or_insert(0);
        *attempts = attempts.saturating_add(1);
        let attempt_no = *attempts;

        if let Some(native_message_id) =
            state.delivered_native_ids.get(request.envelope_id.as_str())
        {
            return Ok(DeliveryOutcome::Delivered { native_message_id: native_message_id.clone() });
        }

        if request.text.contains("[rate-limit-once]") && attempt_no == 1 {
            return Ok(DeliveryOutcome::Retry {
                class: RetryClass::RateLimit,
                reason: "simulated HTTP 429 retry-after from connector mock server".to_owned(),
                retry_after_ms: Some(1),
            });
        }

        if request.text.contains("[connector-crash-once]") && attempt_no == 1 {
            return Ok(DeliveryOutcome::Retry {
                class: RetryClass::ConnectorRestarting,
                reason: "simulated connector restart during outbound send".to_owned(),
                retry_after_ms: Some(1),
            });
        }

        let native_message_id = format!("mock-native-{}", request.envelope_id.replace(':', "-"));
        state.delivered_native_ids.insert(request.envelope_id.clone(), native_message_id.clone());
        Ok(DeliveryOutcome::Delivered { native_message_id })
    }
}

#[derive(Debug, Serialize)]
struct IngestSnapshot {
    envelope_id: String,
    accepted: bool,
    enqueued_outbound: usize,
}

#[derive(Debug, Serialize)]
struct SimulatorSummary {
    ingests: Vec<IngestSnapshot>,
    attempts_per_envelope: BTreeMap<String, u32>,
    delivered_count: usize,
    restart_count: u32,
    queue_pending: u64,
    queue_dead_letters: u64,
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

fn open_harness() -> (TempDir, ConnectorSupervisor, Arc<MockConnectorServer>) {
    let tempdir = TempDir::new().expect("tempdir should initialize");
    let store = Arc::new(
        ConnectorStore::open(tempdir.path().join("connectors.sqlite3"))
            .expect("connector store should initialize"),
    );
    let connector_server = Arc::new(MockConnectorServer::default());
    let supervisor = ConnectorSupervisor::new(
        store,
        Arc::new(SimulatorRouter),
        vec![connector_server.clone() as Arc<dyn ConnectorAdapter>],
        ConnectorSupervisorConfig {
            min_retry_delay_ms: 1,
            base_retry_delay_ms: 1,
            max_retry_delay_ms: 32,
            ..ConnectorSupervisorConfig::default()
        },
    );
    (tempdir, supervisor, connector_server)
}

fn fixture_to_event(fixture: InboundFixture) -> InboundMessageEvent {
    InboundMessageEvent {
        envelope_id: fixture.envelope_id,
        connector_id: "echo:default".to_owned(),
        conversation_id: fixture.conversation_id,
        thread_id: None,
        sender_id: fixture.sender_id,
        sender_display: Some("simulator".to_owned()),
        body: fixture.body,
        adapter_message_id: Some("adapter-message".to_owned()),
        adapter_thread_id: None,
        received_at_unix_ms: 1_000,
        is_direct_message: fixture.is_direct_message,
        requested_broadcast: fixture.requested_broadcast,
    }
}

#[tokio::test]
async fn simulator_harness_replay_matches_golden_snapshot() {
    let (_tempdir, supervisor, connector_server) = open_harness();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");

    let fixtures: Vec<InboundFixture> =
        serde_json::from_str(include_str!("fixtures/channel_simulator_inbound.json"))
            .expect("fixture JSON should parse");
    let mut ingests = Vec::new();
    for fixture in fixtures {
        let envelope_id = fixture.envelope_id.clone();
        let outcome = supervisor
            .ingest_inbound(fixture_to_event(fixture))
            .await
            .expect("fixture inbound should ingest");
        ingests.push(IngestSnapshot {
            envelope_id,
            accepted: outcome.accepted,
            enqueued_outbound: outcome.enqueued_outbound,
        });
    }

    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let drained =
            supervisor.drain_due_outbox(64).await.expect("background outbox drain should succeed");
        if drained.processed == 0 {
            break;
        }
    }

    let status = supervisor.status("echo:default").expect("status should resolve");
    let summary = SimulatorSummary {
        ingests,
        attempts_per_envelope: connector_server.attempt_summary(),
        delivered_count: connector_server.delivered_count(),
        restart_count: status.restart_count,
        queue_pending: status.queue_depth.pending_outbox,
        queue_dead_letters: status.queue_depth.dead_letters,
    };
    let actual = serde_json::to_value(summary).expect("simulation summary should serialize");
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/channel_simulator_expected.json"))
            .expect("golden JSON should parse");
    assert_eq!(actual, expected, "simulator summary should stay deterministic");
}
