//! Unit tests for the Discord adapter: outbound delivery, chunking, rate limits, gateway
//! envelope handling, and zlib-stream decoding, all against fake transports.

use std::{
    collections::VecDeque,
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use super::{
    chunk_discord_text, decode_gateway_binary_payload, deterministic_inbound_envelope_id,
    handle_gateway_envelope, normalize_discord_message_create, normalize_gateway_ws_url,
    parse_fence_line, run_discord_gateway_transport_loop,
    validate_discord_url_target_with_resolver, ConnectorNetGuard, DiscordAdapterConfig,
    DiscordConnectorAdapter, DiscordCredential, DiscordCredentialResolver, DiscordGatewayEnvelope,
    DiscordGatewayInflater, DiscordGatewayMonitorContext, DiscordGatewayResumeState,
    DiscordRuntimeState, DiscordTransport, DiscordTransportResponse, OpenFence,
    DISCORD_GATEWAY_MAX_COMPRESSED_FRAME_BYTES, DISCORD_GATEWAY_MAX_DECOMPRESSED_FRAME_BYTES,
    DISCORD_GATEWAY_ZLIB_SYNC_FLUSH_SUFFIX,
};
use crate::{
    protocol::{
        AttachmentKind, AttachmentRef, ConnectorKind, DeliveryOutcome, InboundMessageEvent,
        OutboundMessageRequest, RetryClass,
    },
    storage::ConnectorInstanceRecord,
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};
use async_trait::async_trait;
use flate2::{Compress, Compression, FlushCompress, Status};
use futures::stream;
use reqwest::Url;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

#[derive(Debug, Default)]
struct StaticCredentialResolver {
    credential: Option<DiscordCredential>,
}

#[async_trait]
impl DiscordCredentialResolver for StaticCredentialResolver {
    async fn resolve_credential(
        &self,
        _instance: &ConnectorInstanceRecord,
    ) -> Result<DiscordCredential, crate::supervisor::ConnectorAdapterError> {
        self.credential.clone().ok_or_else(|| {
            crate::supervisor::ConnectorAdapterError::Backend("missing credential".to_owned())
        })
    }
}

#[derive(Debug, Clone)]
struct CapturedCall {
    method: String,
    url: String,
    payload: Option<Value>,
    multipart_files: Vec<CapturedMultipartFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedMultipartFile {
    filename: String,
    content_type: String,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct FakeTransport {
    get_responses:
        Mutex<VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>>,
    post_responses:
        Mutex<VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>>,
    post_multipart_responses:
        Mutex<VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>>,
    put_responses:
        Mutex<VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>>,
    patch_responses:
        Mutex<VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>>,
    delete_responses:
        Mutex<VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>>,
    captured: Mutex<Vec<CapturedCall>>,
}

impl FakeTransport {
    fn push_get_response(
        &self,
        response: Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>,
    ) {
        self.get_responses
            .lock()
            .expect("get response lock should not be poisoned")
            .push_back(response);
    }

    fn push_post_response(
        &self,
        response: Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>,
    ) {
        self.post_responses
            .lock()
            .expect("post response lock should not be poisoned")
            .push_back(response);
    }

    fn push_post_multipart_response(
        &self,
        response: Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>,
    ) {
        self.post_multipart_responses
            .lock()
            .expect("multipart response lock should not be poisoned")
            .push_back(response);
    }

    fn push_put_response(
        &self,
        response: Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>,
    ) {
        self.put_responses
            .lock()
            .expect("put response lock should not be poisoned")
            .push_back(response);
    }

    fn captured(&self) -> Vec<CapturedCall> {
        self.captured.lock().expect("captured lock should not be poisoned").clone()
    }
}

#[derive(Clone, Default)]
struct RecordingMessageSink {
    sent: Arc<Mutex<Vec<Message>>>,
}

impl RecordingMessageSink {
    fn sent_messages(&self) -> Vec<Message> {
        self.sent.lock().expect("recording sink lock should not be poisoned").clone()
    }

    fn sent_payloads(&self) -> Vec<Value> {
        self.sent
            .lock()
            .expect("recording sink lock should not be poisoned")
            .iter()
            .filter_map(|message| {
                if let Message::Text(payload) = message {
                    serde_json::from_str::<Value>(payload.as_ref()).ok()
                } else {
                    None
                }
            })
            .collect()
    }
}

impl futures::Sink<Message> for RecordingMessageSink {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.get_mut().sent.lock().expect("recording sink lock should not be poisoned").push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[async_trait]
impl DiscordTransport for FakeTransport {
    async fn get(
        &self,
        url: &Url,
        _token: &str,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
        self.captured.lock().expect("captured lock should not be poisoned").push(CapturedCall {
            method: "GET".to_owned(),
            url: url.to_string(),
            payload: None,
            multipart_files: Vec::new(),
        });
        self.get_responses
            .lock()
            .expect("get response lock should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                Ok(DiscordTransportResponse {
                    status: 200,
                    headers: Default::default(),
                    body: "{}".to_owned(),
                })
            })
    }

    async fn post_json(
        &self,
        url: &Url,
        _token: &str,
        payload: &Value,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
        self.captured.lock().expect("captured lock should not be poisoned").push(CapturedCall {
            method: "POST".to_owned(),
            url: url.to_string(),
            payload: Some(payload.clone()),
            multipart_files: Vec::new(),
        });
        self.post_responses
            .lock()
            .expect("post response lock should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                Ok(DiscordTransportResponse {
                    status: 200,
                    headers: Default::default(),
                    body: "{\"id\":\"native-default\"}".to_owned(),
                })
            })
    }

    async fn post_multipart(
        &self,
        url: &Url,
        _token: &str,
        payload: &Value,
        files: &[super::DiscordMultipartAttachment],
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
        self.captured.lock().expect("captured lock should not be poisoned").push(CapturedCall {
            method: "POST_MULTIPART".to_owned(),
            url: url.to_string(),
            payload: Some(payload.clone()),
            multipart_files: files
                .iter()
                .map(|file| CapturedMultipartFile {
                    filename: file.filename.clone(),
                    content_type: file.content_type.clone(),
                    bytes: file.bytes.clone(),
                })
                .collect(),
        });
        self.post_multipart_responses
            .lock()
            .expect("multipart response lock should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                Ok(DiscordTransportResponse {
                    status: 200,
                    headers: Default::default(),
                    body: "{\"id\":\"native-default\"}".to_owned(),
                })
            })
    }

    async fn put(
        &self,
        url: &Url,
        _token: &str,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
        self.captured.lock().expect("captured lock should not be poisoned").push(CapturedCall {
            method: "PUT".to_owned(),
            url: url.to_string(),
            payload: None,
            multipart_files: Vec::new(),
        });
        self.put_responses
            .lock()
            .expect("put response lock should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                Ok(DiscordTransportResponse {
                    status: 204,
                    headers: Default::default(),
                    body: String::new(),
                })
            })
    }

    async fn patch_json(
        &self,
        url: &Url,
        _token: &str,
        payload: &Value,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
        self.captured.lock().expect("captured lock should not be poisoned").push(CapturedCall {
            method: "PATCH".to_owned(),
            url: url.to_string(),
            payload: Some(payload.clone()),
            multipart_files: Vec::new(),
        });
        self.patch_responses
            .lock()
            .expect("patch response lock should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                Ok(DiscordTransportResponse {
                    status: 200,
                    headers: Default::default(),
                    body: "{\"id\":\"native-default\"}".to_owned(),
                })
            })
    }

    async fn delete(
        &self,
        url: &Url,
        _token: &str,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
        self.captured.lock().expect("captured lock should not be poisoned").push(CapturedCall {
            method: "DELETE".to_owned(),
            url: url.to_string(),
            payload: None,
            multipart_files: Vec::new(),
        });
        self.delete_responses
            .lock()
            .expect("delete response lock should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                Ok(DiscordTransportResponse {
                    status: 204,
                    headers: Default::default(),
                    body: String::new(),
                })
            })
    }
}

fn sample_inline_png_base64() -> String {
    use base64::Engine as _;

    base64::engine::general_purpose::STANDARD.encode([0_u8, 1_u8, 2_u8, 3_u8])
}

fn sample_upload_attachment() -> AttachmentRef {
    AttachmentRef {
        kind: AttachmentKind::Image,
        filename: Some("sample image.png".to_owned()),
        content_type: Some("image/png".to_owned()),
        size_bytes: Some(4),
        upload_requested: true,
        inline_base64: Some(sample_inline_png_base64()),
        ..AttachmentRef::default()
    }
}

fn sample_instance() -> ConnectorInstanceRecord {
    ConnectorInstanceRecord {
        connector_id: "discord:default".to_owned(),
        kind: ConnectorKind::Discord,
        principal: "channel:discord:default".to_owned(),
        auth_profile_ref: Some("discord.default".to_owned()),
        token_vault_ref: Some("global/discord_bot_token".to_owned()),
        egress_allowlist: vec!["discord.com".to_owned(), "*.discord.com".to_owned()],
        enabled: true,
        readiness: crate::protocol::ConnectorReadiness::Ready,
        liveness: crate::protocol::ConnectorLiveness::Running,
        restart_count: 0,
        last_error: None,
        last_inbound_unix_ms: None,
        last_outbound_unix_ms: None,
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
    }
}

fn sample_request(text: &str) -> OutboundMessageRequest {
    OutboundMessageRequest {
        envelope_id: "env-1".to_owned(),
        connector_id: "discord:default".to_owned(),
        conversation_id: "1234567890".to_owned(),
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

fn adapter_with_fake_transport(transport: Arc<FakeTransport>) -> DiscordConnectorAdapter {
    let config = DiscordAdapterConfig {
        request_timeout_ms: 1_000,
        max_chunk_chars: 120,
        max_chunk_lines: 8,
        enable_auto_reactions: true,
        ..DiscordAdapterConfig::default()
    };
    DiscordConnectorAdapter::with_dependencies(
        config,
        transport,
        Arc::new(StaticCredentialResolver {
            credential: Some(DiscordCredential {
                token: "super-secret-token".to_owned(),
                source: "test-resolver".to_owned(),
            }),
        }),
    )
}

fn inbound_monitor_count(adapter: &DiscordConnectorAdapter) -> usize {
    adapter.inbound_monitors.lock().expect("inbound monitor registry should not be poisoned").len()
}

#[tokio::test]
async fn stop_runtime_tears_down_inbound_monitor_for_reonboarding() {
    let adapter = adapter_with_fake_transport(Arc::new(FakeTransport::default()));

    adapter.ensure_inbound_monitor(&sample_instance()).await.expect("initial monitor should start");
    assert_eq!(inbound_monitor_count(&adapter), 1);

    adapter.stop_runtime("discord:default").expect("monitor stop should succeed");
    assert_eq!(inbound_monitor_count(&adapter), 0);

    adapter
        .ensure_inbound_monitor(&sample_instance())
        .await
        .expect("fresh monitor should start after teardown");
    assert_eq!(inbound_monitor_count(&adapter), 1);

    adapter.stop_runtime("discord:default").expect("fresh monitor stop should succeed");
}

fn ok_identity_response() -> DiscordTransportResponse {
    DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"bot-1\",\"username\":\"palyra\"}".to_owned(),
    }
}

fn sample_monitor_context(
    transport: Arc<FakeTransport>,
    sender: mpsc::Sender<InboundMessageEvent>,
) -> (DiscordGatewayMonitorContext, Arc<Mutex<DiscordRuntimeState>>) {
    let runtime_state = Arc::new(Mutex::new(DiscordRuntimeState::default()));
    let context = DiscordGatewayMonitorContext {
        connector_id: "discord:default".to_owned(),
        instance: sample_instance(),
        config: DiscordAdapterConfig::default(),
        transport,
        credential_resolver: Arc::new(StaticCredentialResolver {
            credential: Some(DiscordCredential {
                token: "super-secret-token".to_owned(),
                source: "test-resolver".to_owned(),
            }),
        }),
        runtime_state: Arc::clone(&runtime_state),
        sender,
    };
    (context, runtime_state)
}

#[tokio::test]
async fn gateway_envelope_flow_identifies_then_resumes_after_reconnect() {
    let transport = Arc::new(FakeTransport::default());
    let (sender, mut receiver) = mpsc::channel(4);
    let (context, runtime_state) = sample_monitor_context(transport, sender);
    let mut resume_state = DiscordGatewayResumeState::default();
    let mut heartbeat = interval(Duration::from_millis(1_000));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut hello_received = false;
    let mut sink = RecordingMessageSink::default();

    handle_gateway_envelope(
        &mut sink,
        DiscordGatewayEnvelope {
            op: 10,
            data: json!({ "heartbeat_interval": 5000 }),
            seq: None,
            event_type: None,
        },
        &context,
        "super-secret-token",
        &mut resume_state,
        &mut heartbeat,
        &mut hello_received,
    )
    .await
    .expect("hello envelope should be handled");
    assert!(hello_received, "hello should activate heartbeat loop");
    let sent_payloads = sink.sent_payloads();
    assert_eq!(sent_payloads.len(), 1, "hello should emit one gateway op");
    assert_eq!(sent_payloads[0].get("op").and_then(Value::as_i64), Some(2));

    handle_gateway_envelope(
        &mut sink,
        DiscordGatewayEnvelope {
            op: 0,
            data: json!({
                "session_id": "session-1",
                "user": { "id": "bot-1" },
            }),
            seq: Some(41),
            event_type: Some("READY".to_owned()),
        },
        &context,
        "super-secret-token",
        &mut resume_state,
        &mut heartbeat,
        &mut hello_received,
    )
    .await
    .expect("ready envelope should be handled");
    assert_eq!(resume_state.session_id.as_deref(), Some("session-1"));
    assert_eq!(resume_state.bot_user_id.as_deref(), Some("bot-1"));
    assert_eq!(resume_state.seq, Some(41));

    handle_gateway_envelope(
        &mut sink,
        DiscordGatewayEnvelope {
            op: 0,
            data: json!({
                "id": "175928847299117063",
                "channel_id": "thread-123",
                "guild_id": "guild-1",
                "content": "deploy status?",
                "author": {
                    "id": "user-7",
                    "username": "operator",
                },
                "member": {
                    "nick": "Ops",
                },
                "message_reference": {
                    "channel_id": "parent-1",
                },
            }),
            seq: Some(42),
            event_type: Some("MESSAGE_CREATE".to_owned()),
        },
        &context,
        "super-secret-token",
        &mut resume_state,
        &mut heartbeat,
        &mut hello_received,
    )
    .await
    .expect("message create envelope should be handled");
    let inbound = receiver.try_recv().expect("message create should emit one inbound event");
    assert_eq!(inbound.connector_id, "discord:default");
    assert_eq!(inbound.conversation_id, "thread-123");
    assert_eq!(inbound.adapter_message_id.as_deref(), Some("175928847299117063"));
    assert_eq!(inbound.adapter_thread_id.as_deref(), Some("thread-123"));
    assert_eq!(inbound.thread_id.as_deref(), Some("thread-123"));
    assert_eq!(inbound.sender_id, "user-7");
    assert_eq!(inbound.sender_display.as_deref(), Some("Ops"));
    assert_eq!(
        inbound.envelope_id,
        deterministic_inbound_envelope_id("discord:default", "175928847299117063"),
        "inbound monitor should keep deterministic dedupe envelope IDs across reconnects"
    );
    assert!(
        runtime_state
            .lock()
            .expect("runtime state lock should not be poisoned")
            .last_inbound_unix_ms
            .is_some(),
        "message create should update inbound runtime heartbeat"
    );

    let reconnect_error = handle_gateway_envelope(
        &mut sink,
        DiscordGatewayEnvelope { op: 7, data: Value::Null, seq: None, event_type: None },
        &context,
        "super-secret-token",
        &mut resume_state,
        &mut heartbeat,
        &mut hello_received,
    )
    .await
    .expect_err("reconnect opcode should force session restart");
    assert!(
        reconnect_error.to_string().contains("requested reconnect"),
        "reconnect opcode should return explicit reconnect error"
    );

    handle_gateway_envelope(
        &mut sink,
        DiscordGatewayEnvelope {
            op: 10,
            data: json!({ "heartbeat_interval": 6000 }),
            seq: None,
            event_type: None,
        },
        &context,
        "super-secret-token",
        &mut resume_state,
        &mut heartbeat,
        &mut hello_received,
    )
    .await
    .expect("follow-up hello should trigger resume");
    let sent_payloads = sink.sent_payloads();
    assert_eq!(sent_payloads.len(), 2, "reconnect flow should emit identify then resume payloads");
    let resume_payload =
        sent_payloads[1].get("d").cloned().expect("resume op should include payload");
    assert_eq!(sent_payloads[1].get("op").and_then(Value::as_i64), Some(6));
    assert_eq!(resume_payload.get("session_id").and_then(Value::as_str), Some("session-1"));
    assert_eq!(resume_payload.get("seq").and_then(Value::as_i64), Some(42));
}

#[tokio::test]
async fn gateway_envelope_reconnect_resume_cycles_remain_stable_under_soak() {
    let transport = Arc::new(FakeTransport::default());
    let (sender, mut receiver) = mpsc::channel(64);
    let (context, runtime_state) = sample_monitor_context(transport, sender);
    let mut resume_state = DiscordGatewayResumeState::default();
    let mut heartbeat = interval(Duration::from_millis(1_000));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut hello_received = false;
    let mut sink = RecordingMessageSink::default();

    handle_gateway_envelope(
        &mut sink,
        DiscordGatewayEnvelope {
            op: 10,
            data: json!({ "heartbeat_interval": 5_000 }),
            seq: None,
            event_type: None,
        },
        &context,
        "super-secret-token",
        &mut resume_state,
        &mut heartbeat,
        &mut hello_received,
    )
    .await
    .expect("initial hello should identify");
    handle_gateway_envelope(
        &mut sink,
        DiscordGatewayEnvelope {
            op: 0,
            data: json!({
                "session_id": "session-1",
                "user": { "id": "bot-1" },
            }),
            seq: Some(41),
            event_type: Some("READY".to_owned()),
        },
        &context,
        "super-secret-token",
        &mut resume_state,
        &mut heartbeat,
        &mut hello_received,
    )
    .await
    .expect("ready envelope should establish resume state");

    const CYCLES: usize = 16;
    for cycle in 0..CYCLES {
        let seq = 42_i64 + cycle as i64;
        let message_id = format!("1759288472991170{cycle:02}");
        handle_gateway_envelope(
            &mut sink,
            DiscordGatewayEnvelope {
                op: 0,
                data: json!({
                    "id": message_id,
                    "channel_id": "thread-123",
                    "guild_id": "guild-1",
                    "content": format!("deploy status cycle {cycle}?"),
                    "author": {
                        "id": "user-7",
                        "username": "operator",
                    },
                    "member": {
                        "nick": "Ops",
                    },
                    "message_reference": {
                        "channel_id": "parent-1",
                    },
                }),
                seq: Some(seq),
                event_type: Some("MESSAGE_CREATE".to_owned()),
            },
            &context,
            "super-secret-token",
            &mut resume_state,
            &mut heartbeat,
            &mut hello_received,
        )
        .await
        .expect("message create should remain routable under reconnect churn");
        let reconnect_error = handle_gateway_envelope(
            &mut sink,
            DiscordGatewayEnvelope { op: 7, data: Value::Null, seq: None, event_type: None },
            &context,
            "super-secret-token",
            &mut resume_state,
            &mut heartbeat,
            &mut hello_received,
        )
        .await
        .expect_err("reconnect opcode should request a resumed session");
        assert!(
            reconnect_error.to_string().contains("requested reconnect"),
            "reconnect cycle should surface stable reconnect error"
        );
        handle_gateway_envelope(
            &mut sink,
            DiscordGatewayEnvelope {
                op: 10,
                data: json!({ "heartbeat_interval": 6_000 + cycle as i64 }),
                seq: None,
                event_type: None,
            },
            &context,
            "super-secret-token",
            &mut resume_state,
            &mut heartbeat,
            &mut hello_received,
        )
        .await
        .expect("follow-up hello should continue to resume");
    }

    let sent_payloads = sink.sent_payloads();
    let identify_count = sent_payloads
        .iter()
        .filter(|payload| payload.get("op").and_then(Value::as_i64) == Some(2))
        .count();
    let resume_count = sent_payloads
        .iter()
        .filter(|payload| payload.get("op").and_then(Value::as_i64) == Some(6))
        .count();
    assert_eq!(identify_count, 1, "soak should identify exactly once");
    assert_eq!(resume_count, CYCLES, "every reconnect cycle should resume exactly once");
    assert_eq!(resume_state.session_id.as_deref(), Some("session-1"));
    assert_eq!(resume_state.bot_user_id.as_deref(), Some("bot-1"));
    assert_eq!(resume_state.seq, Some(41 + CYCLES as i64));

    let mut inbound_count = 0_usize;
    while let Ok(inbound) = receiver.try_recv() {
        inbound_count = inbound_count.saturating_add(1);
        assert_eq!(inbound.connector_id, "discord:default");
        assert_eq!(inbound.conversation_id, "thread-123");
        assert_eq!(inbound.sender_id, "user-7");
    }
    assert_eq!(inbound_count, CYCLES, "each cycle should still surface one inbound message");
    let runtime = runtime_state.lock().expect("runtime state lock should not be poisoned");
    assert_eq!(runtime.gateway_last_event_type.as_deref(), Some("MESSAGE_CREATE"));
    assert!(runtime.last_inbound_unix_ms.is_some(), "soak should keep inbound heartbeat fresh");
}

#[tokio::test]
async fn gateway_envelope_invalid_non_resumable_session_clears_resume_cursor() {
    let transport = Arc::new(FakeTransport::default());
    let (sender, _receiver) = mpsc::channel(1);
    let (context, _runtime_state) = sample_monitor_context(transport, sender);
    let mut resume_state = DiscordGatewayResumeState {
        session_id: Some("session-1".to_owned()),
        seq: Some(91),
        bot_user_id: Some("bot-1".to_owned()),
    };
    let mut heartbeat = interval(Duration::from_millis(1_000));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut hello_received = true;
    let mut sink = RecordingMessageSink::default();

    let invalid_error = handle_gateway_envelope(
        &mut sink,
        DiscordGatewayEnvelope { op: 9, data: Value::Bool(false), seq: Some(92), event_type: None },
        &context,
        "super-secret-token",
        &mut resume_state,
        &mut heartbeat,
        &mut hello_received,
    )
    .await
    .expect_err("invalid non-resumable session should fail the current session");
    assert!(
        invalid_error.to_string().contains("invalidated session"),
        "invalid session opcode should surface a stable error message"
    );
    assert!(
        resume_state.session_id.is_none() && resume_state.seq.is_none(),
        "non-resumable invalid session should clear resume cursor"
    );
    assert_eq!(resume_state.bot_user_id.as_deref(), Some("bot-1"));
}

#[tokio::test]
async fn gateway_transport_loop_simulator_handles_reconnect_then_resume() {
    let transport = Arc::new(FakeTransport::default());
    let (sender, mut receiver) = mpsc::channel(4);
    let (context, runtime_state) = sample_monitor_context(transport, sender);
    let mut resume_state = DiscordGatewayResumeState::default();
    let mut sink = RecordingMessageSink::default();

    let mut scripted_stream = stream::iter(vec![
        Ok::<Message, WsError>(Message::Binary(sample_gateway_zlib_frame().into())),
        Ok(Message::Text(
            json!({ "op": 10, "d": { "heartbeat_interval": 5_000 } }).to_string().into(),
        )),
        Ok(Message::Ping(vec![0xCA, 0xFE].into())),
        Ok(Message::Text(
            json!({
                "op": 0,
                "s": 41,
                "t": "READY",
                "d": {
                    "session_id": "session-1",
                    "user": { "id": "bot-1" }
                }
            })
            .to_string()
            .into(),
        )),
        Ok(Message::Text(
            json!({
                "op": 0,
                "s": 42,
                "t": "MESSAGE_CREATE",
                "d": {
                    "id": "175928847299117063",
                    "channel_id": "thread-123",
                    "guild_id": "guild-1",
                    "content": "deploy status?",
                    "author": {
                        "id": "user-7",
                        "username": "operator"
                    },
                    "member": {
                        "nick": "Ops"
                    },
                    "message_reference": {
                        "channel_id": "parent-1"
                    }
                }
            })
            .to_string()
            .into(),
        )),
        Ok(Message::Text(json!({ "op": 7, "d": null }).to_string().into())),
    ]);

    let reconnect_error = run_discord_gateway_transport_loop(
        &mut sink,
        &mut scripted_stream,
        &context,
        "super-secret-token",
        &mut resume_state,
    )
    .await
    .expect_err("scripted reconnect opcode should end the current gateway stream loop");
    assert!(
        reconnect_error.to_string().contains("requested reconnect"),
        "reconnect opcode should bubble stable reconnect error from transport loop"
    );

    let sent_payloads = sink.sent_payloads();
    assert!(
        sent_payloads.iter().any(|payload| payload.get("op").and_then(Value::as_i64) == Some(2)),
        "HELLO from scripted stream should trigger IDENTIFY write"
    );
    assert!(
        sink.sent_messages().iter().any(|message| {
            matches!(
                message,
                Message::Pong(payload) if payload.as_ref() == [0xCA, 0xFE].as_slice()
            )
        }),
        "gateway loop should emit PONG for incoming PING frames"
    );

    let inbound = receiver
        .try_recv()
        .expect("scripted MESSAGE_CREATE should emit a normalized inbound event");
    assert_eq!(inbound.connector_id, "discord:default");
    assert_eq!(inbound.conversation_id, "thread-123");
    assert_eq!(inbound.adapter_message_id.as_deref(), Some("175928847299117063"));
    assert_eq!(inbound.adapter_thread_id.as_deref(), Some("thread-123"));
    assert_eq!(
        inbound.envelope_id,
        deterministic_inbound_envelope_id("discord:default", "175928847299117063"),
        "scripted reconnect scenario must preserve deterministic dedupe envelope IDs"
    );
    assert_eq!(resume_state.session_id.as_deref(), Some("session-1"));
    assert_eq!(resume_state.bot_user_id.as_deref(), Some("bot-1"));
    assert_eq!(resume_state.seq, Some(42));
    assert_eq!(
        runtime_state
            .lock()
            .expect("runtime state lock should not be poisoned")
            .gateway_last_event_type
            .as_deref(),
        Some("MESSAGE_CREATE"),
        "transport loop should update runtime last event marker"
    );

    let mut resume_stream = stream::iter(vec![Ok::<Message, WsError>(Message::Text(
        json!({ "op": 10, "d": { "heartbeat_interval": 6_000 } }).to_string().into(),
    ))]);
    let second_error = run_discord_gateway_transport_loop(
        &mut sink,
        &mut resume_stream,
        &context,
        "super-secret-token",
        &mut resume_state,
    )
    .await
    .expect_err("single HELLO frame should terminate with stream-closed error");
    assert!(
        second_error.to_string().contains("closed the websocket stream"),
        "second scripted stream should end with deterministic close reason"
    );

    let sent_payloads = sink.sent_payloads();
    let resume_payload = sent_payloads
        .iter()
        .find(|payload| payload.get("op").and_then(Value::as_i64) == Some(6))
        .and_then(|payload| payload.get("d"))
        .cloned()
        .expect("second scripted HELLO should emit RESUME payload");
    assert_eq!(resume_payload.get("session_id").and_then(Value::as_str), Some("session-1"));
    assert_eq!(resume_payload.get("seq").and_then(Value::as_i64), Some(42));
}

#[tokio::test]
async fn send_outbound_returns_retry_on_429_with_retry_after() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 429,
        headers: [("retry-after".to_owned(), "1.25".to_owned())].into_iter().collect(),
        body: "{\"message\":\"too many requests\",\"retry_after\":1.25,\"global\":true}".to_owned(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));

    let outcome = adapter
        .send_outbound(&sample_instance(), &sample_request("hello"))
        .await
        .expect("send should produce delivery outcome");
    match outcome {
        DeliveryOutcome::Retry { class, retry_after_ms, .. } => {
            assert_eq!(class, RetryClass::RateLimit);
            assert_eq!(retry_after_ms, Some(1_250));
        }
        other => panic!("expected retry outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn send_outbound_preflight_respects_route_budget_without_explicit_retry_headers() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 429,
        headers: [("x-ratelimit-remaining".to_owned(), "0".to_owned())].into_iter().collect(),
        body: "{\"message\":\"slow down\"}".to_owned(),
    }));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"unexpected-second-send\"}".to_owned(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));
    let request = sample_request("hello");

    let first = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("first send should return rate-limit outcome");
    let second = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("second send should be deferred by local preflight budget");

    let DeliveryOutcome::Retry { class: first_class, retry_after_ms: first_retry_after_ms, .. } =
        first
    else {
        panic!("first outcome should be retry");
    };
    assert_eq!(first_class, RetryClass::RateLimit);
    assert_eq!(
        first_retry_after_ms,
        Some(1_000),
        "missing retry headers should still produce bounded retry_after fallback"
    );

    let DeliveryOutcome::Retry {
        class: second_class,
        reason: second_reason,
        retry_after_ms: second_retry_after_ms,
    } = second
    else {
        panic!("second outcome should be retry");
    };
    assert_eq!(second_class, RetryClass::RateLimit);
    assert!(
        second_reason.contains("local route/global rate-limit budget"),
        "second retry should come from local preflight budget"
    );
    assert!(
        second_retry_after_ms.unwrap_or(0) > 0,
        "preflight fallback budget should report a positive wait"
    );

    let posts =
        transport.captured().into_iter().filter(|entry| entry.method == "POST").collect::<Vec<_>>();
    assert_eq!(
        posts.len(),
        1,
        "second send should be blocked locally instead of issuing another POST"
    );
}

#[tokio::test]
async fn send_outbound_preflight_respects_global_budget_without_retry_headers() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 429,
        headers: Default::default(),
        body: "{\"message\":\"global limited\",\"global\":true}".to_owned(),
    }));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"unexpected-second-send\"}".to_owned(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));
    let request = sample_request("hello");

    let first = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("first send should return rate-limit outcome");
    let second = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("second send should be deferred by global preflight budget");

    assert!(
        matches!(first, DeliveryOutcome::Retry { class: RetryClass::RateLimit, .. }),
        "first outcome should be rate-limit retry"
    );

    let DeliveryOutcome::Retry {
        class: second_class,
        reason: second_reason,
        retry_after_ms: second_retry_after_ms,
    } = second
    else {
        panic!("second outcome should be retry");
    };
    assert_eq!(second_class, RetryClass::RateLimit);
    assert!(
        second_reason.contains("local route/global rate-limit budget"),
        "second retry should come from local preflight budget"
    );
    assert!(
        second_retry_after_ms.unwrap_or(0) > 0,
        "global fallback budget should report a positive wait"
    );

    let posts =
        transport.captured().into_iter().filter(|entry| entry.method == "POST").collect::<Vec<_>>();
    assert_eq!(posts.len(), 1, "global budget fallback should prevent immediate second POST");
}

#[tokio::test]
async fn send_outbound_is_idempotent_for_same_envelope() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"native-1\"}".to_owned(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));
    let request = sample_request("hello");

    let first = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("first send should return outcome");
    let second = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("second send should return outcome");

    let DeliveryOutcome::Delivered { native_message_id: first_id } = first else {
        panic!("first outcome should be delivered");
    };
    let DeliveryOutcome::Delivered { native_message_id: second_id } = second else {
        panic!("second outcome should be delivered");
    };
    assert_eq!(first_id, "native-1");
    assert_eq!(second_id, "native-1");

    let posts =
        transport.captured().into_iter().filter(|entry| entry.method == "POST").collect::<Vec<_>>();
    assert_eq!(posts.len(), 1, "duplicate envelope should not trigger extra POST");
}

#[tokio::test]
async fn send_outbound_idempotency_cache_is_scoped_per_connector_instance() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"native-1\"}".to_owned(),
    }));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"native-2\"}".to_owned(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));

    let default_instance = sample_instance();
    let mut ops_instance = sample_instance();
    ops_instance.connector_id = "discord:ops".to_owned();
    ops_instance.principal = "channel:discord:ops".to_owned();
    ops_instance.auth_profile_ref = Some("discord.ops".to_owned());
    ops_instance.token_vault_ref = Some("global/discord_bot_token.ops".to_owned());

    let request_default = sample_request("hello");
    let mut request_ops = request_default.clone();
    request_ops.connector_id = "discord:ops".to_owned();

    let first = adapter
        .send_outbound(&default_instance, &request_default)
        .await
        .expect("default connector send should return outcome");
    let second = adapter
        .send_outbound(&ops_instance, &request_ops)
        .await
        .expect("ops connector send should return outcome");

    let DeliveryOutcome::Delivered { native_message_id: first_id } = first else {
        panic!("default connector outcome should be delivered");
    };
    let DeliveryOutcome::Delivered { native_message_id: second_id } = second else {
        panic!("ops connector outcome should be delivered");
    };
    assert_eq!(first_id, "native-1");
    assert_eq!(second_id, "native-2");

    let posts =
        transport.captured().into_iter().filter(|entry| entry.method == "POST").collect::<Vec<_>>();
    assert_eq!(
        posts.len(),
        2,
        "matching envelope ids across connector instances must not share idempotency cache entries"
    );
}

#[test]
fn fallback_native_message_id_is_stable_and_sensitive_to_input() {
    let request = sample_request("hello");
    let first = super::fallback_native_message_id(&request);
    let second = super::fallback_native_message_id(&request);
    assert_eq!(first, second, "same payload must produce stable fallback id");
    assert!(first.starts_with("discord-"));
    assert_eq!(first.len(), "discord-".len() + 32);
    assert!(
        first["discord-".len()..].chars().all(|value| value.is_ascii_hexdigit()),
        "fallback id suffix should be hex"
    );

    let mut changed = request.clone();
    changed.reply_thread_id = Some("thread-2".to_owned());
    let changed_id = super::fallback_native_message_id(&changed);
    assert_ne!(first, changed_id, "payload changes should alter fallback id");
}

#[tokio::test]
async fn send_outbound_sets_thread_reference_and_auto_reaction() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"native-2\"}".to_owned(),
    }));
    transport.push_put_response(Ok(DiscordTransportResponse {
        status: 204,
        headers: Default::default(),
        body: String::new(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));
    let mut request = sample_request("hello thread");
    request.reply_thread_id = Some("thread-123".to_owned());
    request.in_reply_to_message_id = Some("origin-42".to_owned());
    request.auto_reaction = Some("✅".to_owned());

    let outcome = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("thread send should succeed");
    assert!(matches!(outcome, DeliveryOutcome::Delivered { .. }));

    let captured = transport.captured();
    let message_post =
        captured.iter().find(|entry| entry.method == "POST").expect("expected POST call");
    assert!(
        message_post.url.contains("/channels/thread-123/messages"),
        "thread replies must target the thread channel route"
    );
    assert!(
        !message_post.url.contains("thread_id="),
        "discord bot token send should not use thread_id query parameter"
    );
    let payload = message_post.payload.as_ref().expect("POST call should include payload");
    assert_eq!(
        payload
            .get("message_reference")
            .and_then(|value| value.get("message_id"))
            .and_then(Value::as_str),
        Some("origin-42"),
        "message reference should be propagated into payload"
    );

    let reaction_put =
        captured.iter().find(|entry| entry.method == "PUT").expect("expected PUT reaction call");
    assert!(reaction_put.url.contains("/reactions/"), "auto reaction should hit reaction endpoint");
}

#[tokio::test]
async fn send_outbound_uses_multipart_upload_for_requested_inline_attachments() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_multipart_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"native-upload\"}".to_owned(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));
    let mut request = sample_request("upload");
    request.attachments = vec![sample_upload_attachment()];

    let outcome = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("multipart send should complete");
    assert!(matches!(
        outcome,
        DeliveryOutcome::Delivered {
            native_message_id
        } if native_message_id == "native-upload"
    ));

    let captured = transport.captured();
    assert_eq!(
        captured.iter().filter(|entry| entry.method == "POST").count(),
        0,
        "upload requests should avoid JSON-only POST"
    );
    let multipart_call = captured
        .iter()
        .find(|entry| entry.method == "POST_MULTIPART")
        .expect("expected multipart POST call");
    let payload = multipart_call.payload.as_ref().expect("multipart call should capture payload");
    assert_eq!(
        payload.get("attachments").and_then(Value::as_array).map(Vec::len),
        Some(1),
        "multipart payload should declare uploaded attachment metadata"
    );
    assert_eq!(
        payload
            .get("attachments")
            .and_then(Value::as_array)
            .and_then(|entries| entries.first())
            .and_then(|entry| entry.get("filename"))
            .and_then(Value::as_str),
        Some("sample_image.png"),
        "multipart payload should sanitize attachment filenames deterministically"
    );
    assert_eq!(multipart_call.multipart_files.len(), 1);
    assert_eq!(multipart_call.multipart_files[0].content_type, "image/png");
    assert_eq!(multipart_call.multipart_files[0].bytes, vec![0_u8, 1_u8, 2_u8, 3_u8]);
}

#[tokio::test]
async fn send_outbound_multipart_upload_is_idempotent_for_same_envelope() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_multipart_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"native-upload-1\"}".to_owned(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));
    let mut request = sample_request("upload");
    request.attachments = vec![sample_upload_attachment()];

    let first = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("first multipart send should succeed");
    let second = adapter
        .send_outbound(&sample_instance(), &request)
        .await
        .expect("second multipart send should hit idempotency cache");

    assert!(matches!(
        first,
        DeliveryOutcome::Delivered {
            native_message_id
        } if native_message_id == "native-upload-1"
    ));
    assert!(matches!(
        second,
        DeliveryOutcome::Delivered {
            native_message_id
        } if native_message_id == "native-upload-1"
    ));
    assert_eq!(
        transport.captured().iter().filter(|entry| entry.method == "POST_MULTIPART").count(),
        1,
        "multipart upload path must not emit duplicate visible sends for the same envelope"
    );
}

#[test]
fn split_outbound_chunks_large_payload_and_keeps_envelope_deterministic() {
    let transport = Arc::new(FakeTransport::default());
    let adapter = adapter_with_fake_transport(transport);
    let text = (0..100).map(|index| format!("line-{index}")).collect::<Vec<_>>().join("\n");
    let request = sample_request(text.as_str());
    let chunks =
        adapter.split_outbound(&sample_instance(), &request).expect("split should succeed");
    assert!(chunks.len() > 1);
    assert_eq!(chunks[0].envelope_id, "env-1");
    assert!(
        chunks[1].envelope_id.starts_with("env-1:chunk"),
        "secondary chunks should use deterministic chunk suffix"
    );
    for chunk in chunks {
        assert!(
            chunk.text.chars().count() <= 120,
            "chunk text should respect max char configuration"
        );
    }
}

#[test]
fn split_outbound_embeds_attachment_metadata_context() {
    let transport = Arc::new(FakeTransport::default());
    let adapter = adapter_with_fake_transport(transport);
    let mut request = sample_request("reply with attachment context");
    request.attachments = vec![AttachmentRef {
        kind: AttachmentKind::Image,
        url: Some("u".to_owned()),
        filename: Some("a".to_owned()),
        content_type: Some("i".to_owned()),
        size_bytes: Some(1),
        ..AttachmentRef::default()
    }];

    let chunks = adapter
        .split_outbound(&sample_instance(), &request)
        .expect("split should succeed with attachment metadata");
    assert_eq!(chunks.len(), 1, "short payload should stay in one chunk");
    let rendered = &chunks[0].text;
    assert!(
        rendered.contains("[attachment-metadata]"),
        "attachment metadata marker should be appended to outbound text"
    );
    assert!(
        rendered.contains("kind=image, filename=a, content_type=i, size_bytes=1, source=u"),
        "attachment metadata should preserve key fields for operator visibility"
    );
}

#[test]
fn split_outbound_keeps_upload_attachments_only_on_first_chunk() {
    let transport = Arc::new(FakeTransport::default());
    let adapter = adapter_with_fake_transport(transport);
    let text = (0..100).map(|index| format!("line-{index}")).collect::<Vec<_>>().join("\n");
    let mut request = sample_request(text.as_str());
    request.attachments = vec![sample_upload_attachment()];

    let chunks = adapter
        .split_outbound(&sample_instance(), &request)
        .expect("split should succeed with upload attachment");
    assert!(chunks.len() > 1, "fixture should force multiple chunks");
    assert_eq!(chunks[0].attachments.len(), 1, "first chunk should retain upload metadata");
    assert!(
        !chunks[0].text.contains("[attachment-metadata]"),
        "upload attachments should not be rendered into textual metadata context"
    );
    assert!(
        chunks[0].text.starts_with("line-0\nline-1"),
        "first chunk should still preserve the leading body content"
    );
    assert!(
        chunks.iter().skip(1).all(|chunk| chunk.attachments.is_empty()),
        "secondary chunks must not re-upload the same attachment"
    );
}

#[test]
fn chunker_preserves_balanced_fences() {
    let code = (0..40).map(|index| format!("console.log({index});")).collect::<Vec<_>>().join("\n");
    let message = format!("Here is code:\\n```js\\n{code}\\n```\\nDone.");
    let chunks = chunk_discord_text(message.as_str(), 80, 8);
    assert!(chunks.len() > 1);
    for chunk in chunks {
        let mut open_fence: Option<OpenFence> = None;
        for line in chunk.lines() {
            let Some(fence_info) = parse_fence_line(line) else {
                continue;
            };
            if open_fence.is_none() {
                open_fence = Some(fence_info);
                continue;
            }
            if let Some(existing) = open_fence.as_ref() {
                if existing.marker == fence_info.marker
                    && fence_info.marker_len >= existing.marker_len
                {
                    open_fence = None;
                }
            }
        }
        assert!(open_fence.is_none(), "fenced blocks should stay balanced in chunk: {chunk}");
    }
}

#[tokio::test]
async fn runtime_snapshot_redacts_token_material() {
    let transport = Arc::new(FakeTransport::default());
    transport.push_get_response(Ok(ok_identity_response()));
    transport.push_post_response(Ok(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"native-3\"}".to_owned(),
    }));
    let adapter = adapter_with_fake_transport(Arc::clone(&transport));
    adapter
        .send_outbound(&sample_instance(), &sample_request("snapshot"))
        .await
        .expect("send should succeed");
    let snapshot =
        adapter.runtime_snapshot(&sample_instance()).expect("runtime snapshot should be available");
    assert_eq!(
        snapshot
            .get("credential")
            .and_then(|value| value.get("token_suffix"))
            .and_then(Value::as_str),
        Some("oken"),
        "snapshot should expose only token suffix"
    );
    assert_eq!(
        snapshot
            .get("bot_identity")
            .and_then(|value| value.get("username"))
            .and_then(Value::as_str),
        Some("palyra"),
        "snapshot should include validated bot identity metadata"
    );
}

#[test]
fn inbound_envelope_id_is_stable_for_same_message() {
    let first = deterministic_inbound_envelope_id("discord:default", "1234567890");
    let second = deterministic_inbound_envelope_id("discord:default", "1234567890");
    let third = deterministic_inbound_envelope_id("discord:default", "1234567891");
    assert_eq!(first, second, "same connector/message pair must produce stable envelope ids");
    assert_ne!(first, third, "different messages must produce different envelope ids");
}

#[test]
fn normalize_message_create_maps_thread_and_attachments() {
    let payload = json!({
        "id": "175928847299117063",
        "channel_id": "thread-123",
        "guild_id": "guild-1",
        "content": "<@bot-1> check this",
        "author": {
            "id": "user-7",
            "username": "operator",
            "global_name": "Operator"
        },
        "member": {
            "nick": "Op"
        },
        "message_reference": {
            "channel_id": "parent-777"
        },
        "attachments": [
            {
                "url": "https://cdn.discordapp.net/attachments/abc/screenshot.png",
                "filename": "screenshot.png",
                "size": 4096,
                "content_type": "image/png"
            }
        ]
    });

    let normalized = normalize_discord_message_create("discord:default", &payload, Some("bot-1"))
        .expect("payload should map to inbound event");
    assert_eq!(normalized.connector_id, "discord:default");
    assert_eq!(normalized.conversation_id, "thread-123");
    assert_eq!(normalized.adapter_message_id.as_deref(), Some("175928847299117063"));
    assert_eq!(normalized.adapter_thread_id.as_deref(), Some("thread-123"));
    assert_eq!(normalized.thread_id.as_deref(), Some("thread-123"));
    assert_eq!(normalized.sender_id, "user-7");
    assert_eq!(normalized.sender_display.as_deref(), Some("Op"));
    assert!(!normalized.is_direct_message, "guild message must not be marked as DM");
    assert_eq!(normalized.attachments.len(), 1);
    assert_eq!(normalized.attachments[0].kind, crate::protocol::AttachmentKind::Image);
}

#[test]
fn normalize_message_create_ignores_self_messages() {
    let payload = json!({
        "id": "175928847299117064",
        "channel_id": "dm-42",
        "content": "hello",
        "author": {
            "id": "bot-1",
            "username": "palyra"
        },
        "attachments": []
    });
    let normalized = normalize_discord_message_create("discord:default", &payload, Some("bot-1"));
    assert!(normalized.is_none(), "connector must ignore its own bot messages");
}

#[test]
fn build_net_guard_default_allowlist_covers_gateway_and_cdn_hosts() {
    let adapter = DiscordConnectorAdapter::new();
    let mut instance = sample_instance();
    instance.egress_allowlist.clear();
    let guard = adapter
        .build_net_guard(&instance)
        .expect("fallback allowlist should build a valid net guard");
    let public = [std::net::IpAddr::V4(std::net::Ipv4Addr::new(93, 184, 216, 34))];
    for host in ["discord.com", "gateway.discord.gg", "cdn.discordapp.net", "media.discordapp.com"]
    {
        guard.validate_target(host, &public).unwrap_or_else(|error| {
            panic!("host '{host}' should pass fallback allowlist: {error}")
        });
    }
}

#[test]
fn validate_url_target_reports_dns_failure_as_egress_deny() {
    let guard = ConnectorNetGuard::new(&["*.invalid".to_owned()]).expect("guard should build");
    let url = Url::parse("https://missing-host.invalid/path").expect("fixture URL should parse");
    let error = super::validate_discord_url_target(&guard, &url)
        .expect_err("DNS failure should deny discord egress target");
    let ConnectorAdapterError::Backend(message) = error;
    assert!(
        message.contains("DNS resolution failed"),
        "error should report DNS resolution failure: {message}"
    );
}

#[test]
fn validate_url_target_reports_empty_dns_results_as_egress_deny() {
    let guard = ConnectorNetGuard::new(&["*.discord.com".to_owned()]).expect("guard should build");
    let url = Url::parse("https://gateway.discord.gg/path").expect("fixture URL should parse");
    let error =
        validate_discord_url_target_with_resolver(&guard, &url, |_host, _port| Ok(Vec::new()))
            .expect_err("empty DNS responses should deny discord egress target");
    let ConnectorAdapterError::Backend(message) = error;
    assert!(
        message.contains("DNS resolution returned no addresses"),
        "error should mention empty DNS response: {message}"
    );
}

#[test]
fn normalize_gateway_ws_url_enforces_zlib_stream_compression() {
    let url =
        Url::parse("https://gateway.discord.gg/?foo=bar").expect("input gateway URL should parse");
    let normalized =
        normalize_gateway_ws_url(url).expect("gateway URL normalization should succeed");
    assert_eq!(normalized.scheme(), "wss");
    assert_eq!(
        normalized.query_pairs().find(|(key, _)| key == "v").map(|(_, value)| value.into_owned()),
        Some("10".to_owned()),
        "normalized URL must set Discord gateway version"
    );
    assert_eq!(
        normalized
            .query_pairs()
            .find(|(key, _)| key == "encoding")
            .map(|(_, value)| value.into_owned()),
        Some("json".to_owned()),
        "normalized URL must set JSON encoding"
    );
    assert_eq!(
        normalized
            .query_pairs()
            .find(|(key, _)| key == "compress")
            .map(|(_, value)| value.into_owned()),
        Some("zlib-stream".to_owned()),
        "normalized URL must request zlib-stream compression"
    );
}

#[test]
fn decode_gateway_binary_payload_decodes_zlib_stream_frame() {
    let raw = r#"{"op":11,"d":null}"#;
    let compressed = sample_gateway_zlib_frame();
    let mut inflater = DiscordGatewayInflater::default();
    let decoded = decode_gateway_binary_payload(compressed.as_slice(), &mut inflater)
        .expect("zlib payload decode should not fail")
        .expect("complete zlib payload should decode immediately");
    assert_eq!(decoded, raw);
}

#[test]
fn decode_gateway_binary_payload_buffers_until_sync_flush_suffix() {
    let raw = r#"{"op":11,"d":null}"#;
    let compressed = sample_gateway_zlib_frame();
    let split_at = compressed.len().saturating_sub(2).max(1);
    let mut inflater = DiscordGatewayInflater::default();
    let first = decode_gateway_binary_payload(&compressed[..split_at], &mut inflater)
        .expect("partial zlib payload should not error");
    assert!(
        first.is_none(),
        "decoder should wait for complete sync-flush suffix before emitting payload"
    );
    let second = decode_gateway_binary_payload(&compressed[split_at..], &mut inflater)
        .expect("remaining zlib payload should decode without error");
    assert_eq!(second.as_deref(), Some(raw));
}

#[test]
fn decode_gateway_binary_payload_rejects_frame_over_decompressed_limit() {
    let oversized = vec![b'a'; DISCORD_GATEWAY_MAX_DECOMPRESSED_FRAME_BYTES.saturating_add(1)];
    let compressed = compress_gateway_zlib_frame(oversized.as_slice());
    assert!(
        compressed.len() < DISCORD_GATEWAY_MAX_COMPRESSED_FRAME_BYTES,
        "oversized decoded payload should stay below compressed frame budget in regression fixture"
    );

    let mut inflater = DiscordGatewayInflater::default();
    let error = decode_gateway_binary_payload(compressed.as_slice(), &mut inflater)
        .expect_err("oversized decoded payload must be rejected before unbounded growth");
    let reason = error.to_string();
    assert!(
        reason.contains("decompressed payload exceeded"),
        "oversized decoded payload should explain the decompressed frame limit"
    );
    assert!(
        inflater.compressed_buffer.is_empty(),
        "inflater should clear buffered compressed bytes after oversized frame rejection"
    );

    let recovered =
        decode_gateway_binary_payload(sample_gateway_zlib_frame().as_slice(), &mut inflater)
            .expect("inflater should remain usable after oversized frame reset")
            .expect("complete zlib payload should decode after reset");
    assert_eq!(recovered, r#"{"op":11,"d":null}"#);
}

#[test]
fn parse_discord_attachments_treats_svg_as_file() {
    let attachments = super::parse_discord_attachments(Some(&json!([{
        "id": "att-1",
        "url": "https://cdn.discordapp.com/attachments/1/2/diagram.svg",
        "filename": "diagram.svg",
        "content_type": "image/svg+xml",
        "size": 128
    }])));
    assert_eq!(attachments.len(), 1);
    assert_eq!(
        attachments[0].kind,
        AttachmentKind::File,
        "SVG attachments must not enter the image/vision pipeline"
    );
}

fn sample_gateway_zlib_frame() -> Vec<u8> {
    vec![
        0x78, 0x9C, 0xAA, 0x56, 0xCA, 0x2F, 0x50, 0xB2, 0x32, 0x34, 0xD4, 0x51, 0x4A, 0x51, 0xB2,
        0xCA, 0x2B, 0xCD, 0xC9, 0xA9, 0x05, 0x00, 0x00, 0x00, 0xFF, 0xFF,
    ]
}

fn compress_gateway_zlib_frame(raw: &[u8]) -> Vec<u8> {
    let mut compressor = Compress::new(Compression::default(), true);
    let mut compressed = Vec::new();
    let mut output_chunk = [0_u8; 4 * 1024];
    let mut input_offset = 0usize;

    loop {
        let total_in_before = compressor.total_in();
        let total_out_before = compressor.total_out();
        let flush_mode =
            if input_offset < raw.len() { FlushCompress::None } else { FlushCompress::Sync };
        let status = compressor
            .compress(&raw[input_offset..], &mut output_chunk, flush_mode)
            .expect("test zlib payload compression should succeed");
        let consumed = usize::try_from(compressor.total_in().saturating_sub(total_in_before))
            .unwrap_or(usize::MAX);
        let produced = usize::try_from(compressor.total_out().saturating_sub(total_out_before))
            .unwrap_or(usize::MAX);
        input_offset = input_offset.saturating_add(consumed);
        if produced > 0 {
            compressed.extend_from_slice(&output_chunk[..produced]);
        }

        if input_offset >= raw.len()
            && compressed.ends_with(&DISCORD_GATEWAY_ZLIB_SYNC_FLUSH_SUFFIX)
        {
            break;
        }

        if consumed == 0 && produced == 0 && matches!(status, Status::BufError) {
            panic!("gateway compression fixture stalled before emitting sync-flush suffix");
        }
    }
    assert!(
        compressed.ends_with(&DISCORD_GATEWAY_ZLIB_SYNC_FLUSH_SUFFIX),
        "gateway compression fixture must preserve the zlib sync-flush suffix"
    );
    compressed
}
