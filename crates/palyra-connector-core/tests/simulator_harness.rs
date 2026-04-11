use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use palyra_connector_core::{
    AttachmentKind, AttachmentRef, ConnectorAdapter, ConnectorAdapterError, ConnectorApprovalMode,
    ConnectorAvailability, ConnectorConversationTarget, ConnectorInstanceSpec, ConnectorKind,
    ConnectorMessageDeleteRequest, ConnectorMessageEditRequest, ConnectorMessageLocator,
    ConnectorMessageMutationDiff, ConnectorMessageMutationResult, ConnectorMessageMutationStatus,
    ConnectorMessageReactionRecord, ConnectorMessageReactionRequest, ConnectorMessageReadRequest,
    ConnectorMessageReadResult, ConnectorMessageRecord, ConnectorMessageSearchRequest,
    ConnectorMessageSearchResult, ConnectorOperationPreflight, ConnectorRiskLevel, ConnectorRouter,
    ConnectorRouterError, ConnectorStore, ConnectorSupervisor, ConnectorSupervisorConfig,
    DeliveryOutcome, InboundMessageEvent, OutboundMessageRequest, RetryClass, RouteInboundResult,
    RoutedOutboundMessage,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

const REFRESH_FIXTURES_ENV: &str = "PALYRA_REFRESH_DETERMINISTIC_FIXTURES";
const EXPECTED_FIXTURE_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/channel_simulator_expected.json");

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
                attachments: Vec::new(),
                structured_json: None,
                a2ui_update: None,
            }],
            route_key: Some(format!(
                "channel:{}:conversation:{}",
                event.connector_id, event.conversation_id
            )),
            retry_attempt: 0,
            route_message_latency_ms: Some(1),
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
    messages: HashMap<String, ConnectorMessageRecord>,
    reaction_attempts: HashMap<String, u32>,
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

    fn delivered_message_id_for(&self, envelope_id: &str) -> Option<String> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.delivered_native_ids.get(envelope_id).cloned())
    }
}

#[async_trait]
impl ConnectorAdapter for MockConnectorServer {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Echo
    }

    fn availability(&self) -> ConnectorAvailability {
        ConnectorAvailability::InternalTestOnly
    }

    async fn send_outbound(
        &self,
        _instance: &palyra_connector_core::ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
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
        let delivered_index = i64::try_from(state.messages.len()).unwrap_or(0);
        state.messages.insert(
            native_message_id.clone(),
            ConnectorMessageRecord {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: request.conversation_id.clone(),
                        thread_id: request.reply_thread_id.clone(),
                    },
                    message_id: native_message_id.clone(),
                },
                sender_id: "echo:connector".to_owned(),
                sender_display: Some("mock connector".to_owned()),
                body: request.text.clone(),
                created_at_unix_ms: 1_000 + delivered_index * 100,
                edited_at_unix_ms: None,
                is_direct_message: true,
                is_connector_authored: true,
                link: Some(format!(
                    "https://example.test/connectors/{}/messages/{}",
                    request.conversation_id, native_message_id
                )),
                attachments: if request.text.contains("backpressure") {
                    vec![AttachmentRef {
                        kind: AttachmentKind::File,
                        filename: Some("backpressure.log".to_owned()),
                        content_type: Some("text/plain".to_owned()),
                        size_bytes: Some(128),
                        origin: Some("mock_connector".to_owned()),
                        ..AttachmentRef::default()
                    }]
                } else {
                    Vec::new()
                },
                reactions: Vec::new(),
            },
        );
        Ok(DeliveryOutcome::Delivered { native_message_id })
    }

    async fn read_messages(
        &self,
        _instance: &palyra_connector_core::ConnectorInstanceRecord,
        request: &ConnectorMessageReadRequest,
    ) -> Result<ConnectorMessageReadResult, ConnectorAdapterError> {
        let state = self.state.lock().map_err(|_| {
            ConnectorAdapterError::Backend("mock connector server lock poisoned".to_owned())
        })?;
        let mut messages = state
            .messages
            .values()
            .filter(|message| {
                message.locator.target.conversation_id == request.target.conversation_id
                    && message.locator.target.thread_id == request.target.thread_id
            })
            .cloned()
            .collect::<Vec<_>>();
        messages.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| right.locator.message_id.cmp(&left.locator.message_id))
        });
        if let Some(message_id) = request.message_id.as_deref() {
            messages.retain(|message| message.locator.message_id == message_id);
        }
        if let Some(before_message_id) = request.before_message_id.as_deref() {
            messages.retain(|message| message.locator.message_id.as_str() < before_message_id);
        }
        if let Some(after_message_id) = request.after_message_id.as_deref() {
            messages.retain(|message| message.locator.message_id.as_str() > after_message_id);
        }
        let truncated = messages.into_iter().take(request.limit).collect::<Vec<_>>();
        let next_before_message_id =
            truncated.last().map(|message| message.locator.message_id.clone());
        let next_after_message_id =
            truncated.first().map(|message| message.locator.message_id.clone());
        Ok(ConnectorMessageReadResult {
            preflight: admin_preflight(
                "channel.message.read",
                "mock.message.read",
                ConnectorApprovalMode::None,
                ConnectorRiskLevel::Low,
                &["ViewChannel", "ReadMessageHistory"],
                None,
            ),
            target: request.target.clone(),
            exact_message_id: request.message_id.clone(),
            messages: truncated,
            next_before_message_id: request
                .message_id
                .is_none()
                .then_some(next_before_message_id)
                .flatten(),
            next_after_message_id: request
                .message_id
                .is_none()
                .then_some(next_after_message_id)
                .flatten(),
        })
    }

    async fn search_messages(
        &self,
        _instance: &palyra_connector_core::ConnectorInstanceRecord,
        request: &ConnectorMessageSearchRequest,
    ) -> Result<ConnectorMessageSearchResult, ConnectorAdapterError> {
        let state = self.state.lock().map_err(|_| {
            ConnectorAdapterError::Backend("mock connector server lock poisoned".to_owned())
        })?;
        let query = request.query.as_deref().map(str::to_ascii_lowercase).unwrap_or_default();
        let mut matches = state
            .messages
            .values()
            .filter(|message| {
                message.locator.target.conversation_id == request.target.conversation_id
                    && message.locator.target.thread_id == request.target.thread_id
                    && (query.is_empty()
                        || message.body.to_ascii_lowercase().contains(query.as_str()))
                    && request
                        .author_id
                        .as_deref()
                        .is_none_or(|author_id| message.sender_id == author_id)
                    && request
                        .has_attachments
                        .is_none_or(|expected| message.attachments.is_empty() != expected)
            })
            .cloned()
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| right.locator.message_id.cmp(&left.locator.message_id))
        });
        if let Some(before_message_id) = request.before_message_id.as_deref() {
            matches.retain(|message| message.locator.message_id.as_str() < before_message_id);
        }
        let truncated = matches.into_iter().take(request.limit).collect::<Vec<_>>();
        let next_before_message_id =
            truncated.last().map(|message| message.locator.message_id.clone());
        Ok(ConnectorMessageSearchResult {
            preflight: admin_preflight(
                "channel.message.search",
                "mock.message.search",
                ConnectorApprovalMode::None,
                ConnectorRiskLevel::Low,
                &["ViewChannel", "ReadMessageHistory"],
                None,
            ),
            target: request.target.clone(),
            query: request.query.clone(),
            author_id: request.author_id.clone(),
            has_attachments: request.has_attachments,
            matches: truncated,
            next_before_message_id,
        })
    }

    async fn edit_message(
        &self,
        _instance: &palyra_connector_core::ConnectorInstanceRecord,
        request: &ConnectorMessageEditRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let mut state = self.state.lock().map_err(|_| {
            ConnectorAdapterError::Backend("mock connector server lock poisoned".to_owned())
        })?;
        let Some(message) = state.messages.get_mut(&request.locator.message_id) else {
            return Ok(denied_mutation(
                request.locator.clone(),
                "channel.message.edit",
                "mock.message.edit",
                "message not found for edit".to_owned(),
            ));
        };
        let before_body = message.body.clone();
        message.body = request.body.clone();
        message.edited_at_unix_ms = Some(message.created_at_unix_ms + 50);
        Ok(ConnectorMessageMutationResult {
            preflight: admin_preflight(
                "channel.message.edit",
                "mock.message.edit",
                ConnectorApprovalMode::Conditional,
                ConnectorRiskLevel::Low,
                &["ManageMessages"],
                None,
            ),
            locator: request.locator.clone(),
            status: ConnectorMessageMutationStatus::Updated,
            reason: Some("message updated in simulator harness".to_owned()),
            message: Some(message.clone()),
            diff: Some(ConnectorMessageMutationDiff {
                before_body: Some(before_body),
                after_body: Some(request.body.clone()),
            }),
        })
    }

    async fn delete_message(
        &self,
        _instance: &palyra_connector_core::ConnectorInstanceRecord,
        request: &ConnectorMessageDeleteRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let mut state = self.state.lock().map_err(|_| {
            ConnectorAdapterError::Backend("mock connector server lock poisoned".to_owned())
        })?;
        if request.reason.as_deref() == Some("stale") {
            return Ok(denied_mutation(
                request.locator.clone(),
                "channel.message.delete",
                "mock.message.delete",
                "stale message requires approval".to_owned(),
            ));
        }
        let Some(message) = state.messages.remove(&request.locator.message_id) else {
            return Ok(denied_mutation(
                request.locator.clone(),
                "channel.message.delete",
                "mock.message.delete",
                "message not found for delete".to_owned(),
            ));
        };
        Ok(ConnectorMessageMutationResult {
            preflight: admin_preflight(
                "channel.message.delete",
                "mock.message.delete",
                ConnectorApprovalMode::Required,
                ConnectorRiskLevel::Medium,
                &["ManageMessages"],
                None,
            ),
            locator: request.locator.clone(),
            status: ConnectorMessageMutationStatus::Deleted,
            reason: Some("message deleted in simulator harness".to_owned()),
            message: Some(message),
            diff: None,
        })
    }

    async fn add_reaction(
        &self,
        _instance: &palyra_connector_core::ConnectorInstanceRecord,
        request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        mutate_reaction_state(&self.state, request, true)
    }

    async fn remove_reaction(
        &self,
        _instance: &palyra_connector_core::ConnectorInstanceRecord,
        request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        mutate_reaction_state(&self.state, request, false)
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
        attachments: Vec::new(),
    }
}

fn admin_preflight(
    policy_action: &str,
    audit_event_type: &str,
    approval_mode: ConnectorApprovalMode,
    risk_level: ConnectorRiskLevel,
    required_permissions: &[&str],
    reason: Option<String>,
) -> ConnectorOperationPreflight {
    ConnectorOperationPreflight {
        allowed: true,
        policy_action: policy_action.to_owned(),
        approval_mode,
        risk_level,
        audit_event_type: audit_event_type.to_owned(),
        required_permissions: required_permissions
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        reason,
    }
}

fn denied_mutation(
    locator: ConnectorMessageLocator,
    policy_action: &str,
    audit_event_type: &str,
    reason: String,
) -> ConnectorMessageMutationResult {
    ConnectorMessageMutationResult {
        preflight: ConnectorOperationPreflight {
            allowed: false,
            policy_action: policy_action.to_owned(),
            approval_mode: ConnectorApprovalMode::Required,
            risk_level: ConnectorRiskLevel::High,
            audit_event_type: audit_event_type.to_owned(),
            required_permissions: vec!["ManageMessages".to_owned()],
            reason: Some(reason.clone()),
        },
        locator,
        status: ConnectorMessageMutationStatus::Denied,
        reason: Some(reason),
        message: None,
        diff: None,
    }
}

fn mutate_reaction_state(
    state: &Mutex<MockConnectorServerState>,
    request: &ConnectorMessageReactionRequest,
    add: bool,
) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
    let mut state = state.lock().map_err(|_| {
        ConnectorAdapterError::Backend("mock connector server lock poisoned".to_owned())
    })?;
    let attempt_key = format!(
        "{}:{}:{}",
        request.locator.message_id,
        request.emoji,
        if add { "add" } else { "remove" }
    );
    let attempts = state.reaction_attempts.entry(attempt_key).or_insert(0);
    *attempts = attempts.saturating_add(1);
    if add && request.emoji == "rate-limit-once" && *attempts == 1 {
        return Err(ConnectorAdapterError::Backend(
            "simulated reaction rate limit from harness".to_owned(),
        ));
    }
    let Some(message) = state.messages.get_mut(&request.locator.message_id) else {
        return Ok(denied_mutation(
            request.locator.clone(),
            if add { "channel.message.react_add" } else { "channel.message.react_remove" },
            if add { "mock.message.react_add" } else { "mock.message.react_remove" },
            "message not found for reaction mutation".to_owned(),
        ));
    };
    let emoji = request.emoji.clone();
    if add {
        if let Some(reaction) =
            message.reactions.iter_mut().find(|reaction| reaction.emoji == emoji)
        {
            reaction.count = reaction.count.saturating_add(1);
            reaction.reacted_by_connector = true;
        } else {
            message.reactions.push(ConnectorMessageReactionRecord {
                emoji: emoji.clone(),
                count: 1,
                reacted_by_connector: true,
            });
        }
    } else if let Some(index) =
        message.reactions.iter().position(|reaction| reaction.emoji == emoji)
    {
        if message.reactions[index].count <= 1 {
            message.reactions.remove(index);
        } else {
            message.reactions[index].count = message.reactions[index].count.saturating_sub(1);
        }
    }
    Ok(ConnectorMessageMutationResult {
        preflight: admin_preflight(
            if add { "channel.message.react_add" } else { "channel.message.react_remove" },
            if add { "mock.message.react_add" } else { "mock.message.react_remove" },
            ConnectorApprovalMode::Conditional,
            ConnectorRiskLevel::Low,
            &["AddReactions"],
            None,
        ),
        locator: request.locator.clone(),
        status: if add {
            ConnectorMessageMutationStatus::ReactionAdded
        } else {
            ConnectorMessageMutationStatus::ReactionRemoved
        },
        reason: Some(if add {
            "reaction added in simulator harness".to_owned()
        } else {
            "reaction removed in simulator harness".to_owned()
        }),
        message: Some(message.clone()),
        diff: None,
    })
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
    assert_or_refresh_expected_fixture(Path::new(EXPECTED_FIXTURE_PATH), &actual);
}

#[tokio::test]
async fn simulator_harness_admin_operations_cover_success_denied_and_retry_flows() {
    let (_tempdir, supervisor, connector_server) = open_harness();
    supervisor.register_connector(&sample_spec()).expect("register should succeed");

    for fixture in serde_json::from_str::<Vec<InboundFixture>>(include_str!(
        "fixtures/channel_simulator_inbound.json"
    ))
    .expect("fixture JSON should parse")
    {
        supervisor
            .ingest_inbound(fixture_to_event(fixture))
            .await
            .expect("fixture inbound should ingest");
    }
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let drained =
            supervisor.drain_due_outbox(64).await.expect("background outbox drain should succeed");
        if drained.processed == 0 {
            break;
        }
    }

    let first_message_id = connector_server
        .delivered_message_id_for("env-1:0")
        .expect("first outbound should have a delivered message id");
    let second_message_id = connector_server
        .delivered_message_id_for("env-2:0")
        .expect("second outbound should have a delivered message id");

    let exact_read = supervisor
        .read_messages(
            "echo:default",
            &ConnectorMessageReadRequest {
                target: ConnectorConversationTarget {
                    conversation_id: "conv-1".to_owned(),
                    thread_id: None,
                },
                message_id: Some(first_message_id.clone()),
                before_message_id: None,
                after_message_id: None,
                around_message_id: None,
                limit: 5,
            },
        )
        .await
        .expect("exact message read should succeed");
    assert_eq!(exact_read.messages.len(), 1);
    assert_eq!(exact_read.messages[0].locator.message_id, first_message_id);
    assert_eq!(exact_read.messages[0].body, "hello from simulator");

    let attachment_search = supervisor
        .search_messages(
            "echo:default",
            &ConnectorMessageSearchRequest {
                target: ConnectorConversationTarget {
                    conversation_id: "conv-1".to_owned(),
                    thread_id: None,
                },
                query: Some("backpressure".to_owned()),
                author_id: None,
                has_attachments: Some(true),
                before_message_id: None,
                limit: 5,
            },
        )
        .await
        .expect("attachment search should succeed");
    assert_eq!(attachment_search.matches.len(), 1);
    assert_eq!(attachment_search.matches[0].locator.message_id, second_message_id);
    assert_eq!(attachment_search.matches[0].attachments.len(), 1);

    let edit_result = supervisor
        .edit_message(
            "echo:default",
            &ConnectorMessageEditRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "conv-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: first_message_id.clone(),
                },
                body: "hello from simulator (edited)".to_owned(),
            },
        )
        .await
        .expect("message edit should succeed");
    assert_eq!(edit_result.status, ConnectorMessageMutationStatus::Updated);
    assert_eq!(
        edit_result.diff.as_ref().and_then(|diff| diff.before_body.as_deref()),
        Some("hello from simulator")
    );
    assert_eq!(
        edit_result.diff.as_ref().and_then(|diff| diff.after_body.as_deref()),
        Some("hello from simulator (edited)")
    );

    let reaction_add = supervisor
        .add_reaction(
            "echo:default",
            &ConnectorMessageReactionRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "conv-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: first_message_id.clone(),
                },
                emoji: "thumbsup".to_owned(),
            },
        )
        .await
        .expect("reaction add should succeed");
    assert_eq!(reaction_add.status, ConnectorMessageMutationStatus::ReactionAdded);
    assert_eq!(reaction_add.message.as_ref().map(|message| message.reactions.len()), Some(1));

    let reaction_remove = supervisor
        .remove_reaction(
            "echo:default",
            &ConnectorMessageReactionRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "conv-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: first_message_id.clone(),
                },
                emoji: "thumbsup".to_owned(),
            },
        )
        .await
        .expect("reaction remove should succeed");
    assert_eq!(reaction_remove.status, ConnectorMessageMutationStatus::ReactionRemoved);
    assert_eq!(reaction_remove.message.as_ref().map(|message| message.reactions.len()), Some(0));

    let stale_delete = supervisor
        .delete_message(
            "echo:default",
            &ConnectorMessageDeleteRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "conv-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: second_message_id.clone(),
                },
                reason: Some("stale".to_owned()),
            },
        )
        .await
        .expect("stale delete should return a denied mutation result");
    assert_eq!(stale_delete.status, ConnectorMessageMutationStatus::Denied);
    assert_eq!(stale_delete.reason.as_deref(), Some("stale message requires approval"));

    let reaction_rate_limit = supervisor
        .add_reaction(
            "echo:default",
            &ConnectorMessageReactionRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "conv-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: first_message_id.clone(),
                },
                emoji: "rate-limit-once".to_owned(),
            },
        )
        .await;
    assert!(
        reaction_rate_limit
            .err()
            .is_some_and(|error| error.to_string().contains("simulated reaction rate limit")),
        "simulated reaction rate limit should propagate to the harness caller"
    );

    let delete_result = supervisor
        .delete_message(
            "echo:default",
            &ConnectorMessageDeleteRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "conv-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: first_message_id.clone(),
                },
                reason: Some("cleanup".to_owned()),
            },
        )
        .await
        .expect("message delete should succeed");
    assert_eq!(delete_result.status, ConnectorMessageMutationStatus::Deleted);
    assert_eq!(
        delete_result.message.as_ref().map(|message| message.locator.message_id.as_str()),
        Some(first_message_id.as_str())
    );

    let deleted_read = supervisor
        .read_messages(
            "echo:default",
            &ConnectorMessageReadRequest {
                target: ConnectorConversationTarget {
                    conversation_id: "conv-1".to_owned(),
                    thread_id: None,
                },
                message_id: Some(first_message_id.clone()),
                before_message_id: None,
                after_message_id: None,
                around_message_id: None,
                limit: 5,
            },
        )
        .await
        .expect("read after delete should succeed");
    assert!(deleted_read.messages.is_empty(), "deleted message should no longer be readable");

    let audit_events = supervisor
        .list_logs("echo:default", 32)
        .expect("message admin logs should be readable")
        .into_iter()
        .map(|entry| entry.event_type)
        .collect::<Vec<_>>();
    for expected_event in [
        "mock.message.read",
        "mock.message.search",
        "mock.message.edit",
        "mock.message.react_add",
        "mock.message.react_remove",
        "mock.message.delete",
    ] {
        assert!(
            audit_events.iter().any(|event| event == expected_event),
            "audit logs should contain {expected_event}, got {audit_events:?}"
        );
    }
}

fn assert_or_refresh_expected_fixture(path: &Path, actual: &serde_json::Value) {
    if std::env::var_os(REFRESH_FIXTURES_ENV).is_some() {
        let rendered = serde_json::to_string_pretty(actual)
            .expect("fixture JSON should serialize to a stable pretty form");
        fs::write(path, format!("{rendered}\n"))
            .unwrap_or_else(|error| panic!("failed to write fixture {}: {error}", path.display()));
        return;
    }

    let expected_text = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let expected: serde_json::Value =
        serde_json::from_str(expected_text.as_str()).unwrap_or_else(|error| {
            panic!("fixture {} must contain valid json: {error}", path.display())
        });
    assert_eq!(actual, &expected, "simulator summary should stay deterministic");
}
