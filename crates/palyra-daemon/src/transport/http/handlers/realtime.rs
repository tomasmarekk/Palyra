use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use palyra_common::runtime_contracts::{
    RealtimeCommandEnvelope, RealtimeErrorEnvelope, RealtimeHandshakeRequest, RealtimeSubscription,
    StableErrorEnvelope,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    app::state::AppState,
    command_router::{dispatch_realtime_command, CommandRouterContext},
    realtime::{
        negotiate_realtime_handshake, realtime_method_descriptors, snapshot_refresh_event,
        RealtimeConnectionContext, RealtimeReplayOutcome, REALTIME_SDK_ABI_VERSION,
    },
    transport::http::handlers::console::diagnostics::authorize_console_session,
};

pub(crate) async fn realtime_ws_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    Ok(ws.on_upgrade(move |socket| realtime_socket(socket, state, session.context)).into_response())
}

pub(crate) async fn realtime_methods_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(json!({
        "protocol": {
            "min": palyra_common::runtime_contracts::REALTIME_PROTOCOL_MIN_VERSION,
            "max": palyra_common::runtime_contracts::REALTIME_PROTOCOL_MAX_VERSION,
        },
        "sdk_abi_version": REALTIME_SDK_ABI_VERSION,
        "methods": realtime_method_descriptors(),
    })))
}

pub(crate) async fn realtime_handshake_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(handshake): Json<RealtimeHandshakeRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let (accepted, _) = negotiate_realtime_handshake(
        handshake,
        session.context.principal.clone(),
        crate::unix_ms_now().unwrap_or(0),
    )
    .map_err(realtime_error_response)?;
    Ok(Json(json!({ "accepted": accepted })))
}

pub(crate) async fn realtime_command_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RealtimeHttpCommandRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let (_, realtime_context) = negotiate_realtime_handshake(
        request.handshake,
        session.context.principal.clone(),
        crate::unix_ms_now().unwrap_or(0),
    )
    .map_err(realtime_error_response)?;
    let router_context =
        CommandRouterContext { request_context: session.context, realtime: realtime_context };
    let result = dispatch_realtime_command(&state, &router_context, request.command).await;
    Ok(Json(json!(result)))
}

async fn realtime_socket(
    mut socket: WebSocket,
    state: AppState,
    request_context: crate::gateway::RequestContext,
) {
    let first_message = match tokio::time::timeout(Duration::from_secs(10), socket.recv()).await {
        Ok(Some(Ok(message))) => message,
        _ => {
            let _ = send_error(
                &mut socket,
                stable_error(
                    "realtime/handshake_timeout",
                    "realtime client did not send a handshake frame in time",
                    "send a handshake frame immediately after opening the WebSocket",
                ),
            )
            .await;
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };
    let handshake = match parse_handshake_message(first_message) {
        Ok(handshake) => handshake,
        Err(error) => {
            let _ = send_error(&mut socket, error).await;
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };
    let now = crate::unix_ms_now().unwrap_or(0);
    let (accepted, mut realtime_context) =
        match negotiate_realtime_handshake(handshake, request_context.principal.clone(), now) {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = send_realtime_error(&mut socket, error).await;
                let _ = socket.send(Message::Close(None)).await;
                return;
            }
        };
    let _ = state
        .runtime
        .record_console_event(
            &request_context,
            "realtime.handshake.accepted",
            json!({
                "client_id": accepted.client_id,
                "role": accepted.role.as_str(),
                "scopes": accepted.scopes,
                "capabilities": accepted.capabilities,
                "commands": accepted.commands,
                "cursor": accepted.cursor,
                "heartbeat_interval_ms": accepted.heartbeat_interval_ms,
            }),
        )
        .await;
    if send_frame(&mut socket, "handshake.accepted", json!(accepted)).await.is_err() {
        return;
    }
    if send_replay_or_snapshot(&mut socket, &state, &realtime_context).await.is_err() {
        return;
    }

    loop {
        let timeout =
            Duration::from_millis(realtime_context.heartbeat_interval_ms.saturating_mul(2));
        let message = match tokio::time::timeout(timeout, socket.recv()).await {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {
                let _ = send_error(
                    &mut socket,
                    stable_error(
                        "realtime/idle_timeout",
                        "realtime connection closed after heartbeat timeout",
                        "send ping frames or commands before the negotiated heartbeat timeout",
                    ),
                )
                .await;
                break;
            }
        };
        match message {
            Message::Text(text) => match serde_json::from_str::<RealtimeClientFrame>(&text) {
                Ok(RealtimeClientFrame::Command(command)) => {
                    let router_context = CommandRouterContext {
                        request_context: request_context.clone(),
                        realtime: realtime_context.clone(),
                    };
                    let result = dispatch_realtime_command(&state, &router_context, command).await;
                    if send_frame(&mut socket, "command.result", json!(result)).await.is_err() {
                        break;
                    }
                }
                Ok(RealtimeClientFrame::Ping(payload)) => {
                    if send_frame(
                        &mut socket,
                        "pong",
                        json!({ "payload": payload, "server_time_unix_ms": crate::unix_ms_now().unwrap_or(0) }),
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
                Ok(RealtimeClientFrame::Subscribe(subscription)) => {
                    realtime_context.subscriptions = vec![subscription];
                    if send_frame(
                        &mut socket,
                        "subscribed",
                        json!({ "subscriptions": realtime_context.subscriptions }),
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
                Ok(RealtimeClientFrame::Handshake(_)) => {
                    let _ = send_error(
                        &mut socket,
                        stable_error(
                            "realtime/handshake_already_completed",
                            "handshake can only be sent as the first realtime frame",
                            "open a new WebSocket to renegotiate realtime grants",
                        ),
                    )
                    .await;
                }
                Err(error) => {
                    let _ = send_error(
                        &mut socket,
                        stable_error(
                            "realtime/invalid_frame",
                            format!("invalid realtime frame: {error}"),
                            "send a JSON frame with type command, ping, or subscribe",
                        ),
                    )
                    .await;
                }
            },
            Message::Ping(bytes) => {
                if socket.send(Message::Pong(bytes)).await.is_err() {
                    break;
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) => break,
            Message::Binary(_) => {
                let _ = send_error(
                    &mut socket,
                    stable_error(
                        "realtime/binary_not_supported",
                        "binary realtime frames are not supported",
                        "send JSON text frames",
                    ),
                )
                .await;
            }
        }
    }
    let _ = socket.send(Message::Close(None)).await;
}

async fn send_replay_or_snapshot(
    socket: &mut WebSocket,
    state: &AppState,
    context: &RealtimeConnectionContext,
) -> Result<(), axum::Error> {
    let outcome = {
        let router = state.realtime_events.lock().unwrap_or_else(|error| error.into_inner());
        router.replay_from(context, context.cursor)
    };
    match outcome {
        RealtimeReplayOutcome::Events(events) => {
            for event in events {
                send_frame(socket, "event", json!(event)).await?;
            }
        }
        RealtimeReplayOutcome::SnapshotRequired { cursor, first_available_sequence } => {
            let mut event = snapshot_refresh_event(
                cursor,
                first_available_sequence,
                crate::unix_ms_now().unwrap_or(0),
            );
            event.sequence = first_available_sequence;
            send_frame(socket, "snapshot.required", json!(event)).await?;
        }
    }
    Ok(())
}

fn parse_handshake_message(
    message: Message,
) -> Result<RealtimeHandshakeRequest, StableErrorEnvelope> {
    let Message::Text(text) = message else {
        return Err(stable_error(
            "realtime/handshake_text_required",
            "realtime handshake must be a JSON text frame",
            "send the handshake as the first text frame",
        ));
    };
    if let Ok(RealtimeClientFrame::Handshake(handshake)) =
        serde_json::from_str::<RealtimeClientFrame>(&text)
    {
        return Ok(handshake);
    }
    serde_json::from_str::<RealtimeHandshakeRequest>(&text).map_err(|error| {
        stable_error(
            "realtime/invalid_handshake",
            format!("invalid realtime handshake: {error}"),
            "send a valid realtime handshake payload",
        )
    })
}

async fn send_frame(
    socket: &mut WebSocket,
    frame_type: &str,
    payload: Value,
) -> Result<(), axum::Error> {
    socket
        .send(Message::Text(
            json!({
                "type": frame_type,
                "payload": payload,
            })
            .to_string()
            .into(),
        ))
        .await
}

async fn send_error(socket: &mut WebSocket, error: StableErrorEnvelope) -> Result<(), axum::Error> {
    send_frame(socket, "error", json!({ "error": error })).await
}

async fn send_realtime_error(
    socket: &mut WebSocket,
    error: RealtimeErrorEnvelope,
) -> Result<(), axum::Error> {
    send_frame(socket, "error", json!(error)).await
}

fn stable_error(
    code: impl Into<String>,
    message: impl Into<String>,
    recovery_hint: impl Into<String>,
) -> StableErrorEnvelope {
    StableErrorEnvelope::new(code, message, recovery_hint)
}

fn realtime_error_response(error: RealtimeErrorEnvelope) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!(error))).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct RealtimeHttpCommandRequest {
    pub(crate) handshake: RealtimeHandshakeRequest,
    pub(crate) command: RealtimeCommandEnvelope,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
enum RealtimeClientFrame {
    Handshake(RealtimeHandshakeRequest),
    Command(RealtimeCommandEnvelope),
    Ping(Value),
    Subscribe(RealtimeSubscription),
}
