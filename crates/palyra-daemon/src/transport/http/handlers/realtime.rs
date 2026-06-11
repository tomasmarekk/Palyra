//! HTTP and WebSocket handlers for the realtime command/event protocol.
//!
//! Console sessions authorize the connection, negotiate protocol capabilities,
//! then dispatch text-frame commands through the command router. The WebSocket
//! path accepts only same-origin browser clients when an `Origin` header is
//! present.

use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use palyra_common::runtime_contracts::{
    RealtimeCommandEnvelope, RealtimeErrorEnvelope, RealtimeHandshakeRequest, RealtimeSubscription,
    StableErrorEnvelope,
};
use reqwest::Url;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    app::state::AppState,
    command_router::{dispatch_realtime_command, CommandRouterContext},
    realtime::{
        negotiate_realtime_handshake, realtime_method_descriptors, snapshot_refresh_event,
        RealtimeConnectionContext, RealtimeReplayOutcome, REALTIME_SDK_ABI_VERSION,
    },
    runtime_status_response,
    transport::http::handlers::console::diagnostics::authorize_console_session,
};

/// Upgrades an authorized realtime request into a WebSocket session.
///
/// # Errors
/// Returns an error response when origin validation or console authorization
/// fails before the WebSocket upgrade.
pub(crate) async fn realtime_ws_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, Response> {
    validate_realtime_ws_origin(&headers).map_err(|response| *response)?;
    let session = authorize_console_session(&state, &headers, false)?;
    Ok(ws.on_upgrade(move |socket| realtime_socket(socket, state, session.context)).into_response())
}

fn validate_realtime_ws_origin(headers: &HeaderMap) -> Result<(), Box<Response>> {
    let Some(origin_header) = headers.get(header::ORIGIN) else {
        return Ok(());
    };
    let origin = origin_header
        .to_str()
        .map(str::trim)
        .map_err(|_| realtime_origin_rejected_response("realtime WebSocket Origin is invalid"))?;
    let origin_host_port = realtime_origin_host_port(origin)
        .ok_or_else(|| realtime_origin_rejected_response("realtime WebSocket Origin is invalid"))?;
    let host_header = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            realtime_origin_rejected_response(
                "realtime WebSocket Host header is required when Origin is present",
            )
        })?;
    let request_host_port = realtime_host_header_host_port(host_header).ok_or_else(|| {
        realtime_origin_rejected_response("realtime WebSocket Host header is invalid")
    })?;
    if origin_host_port != request_host_port {
        return Err(realtime_origin_rejected_response(
            "realtime WebSocket Origin does not match the request host",
        ));
    }
    Ok(())
}

fn realtime_origin_host_port(origin: &str) -> Option<String> {
    let url = Url::parse(origin).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    normalized_host_port(url.host_str()?, url.port_or_known_default()?)
}

fn realtime_host_header_host_port(host_header: &str) -> Option<String> {
    let url = Url::parse(format!("http://{host_header}").as_str()).ok()?;
    normalized_host_port(url.host_str()?, url.port_or_known_default()?)
}

fn normalized_host_port(host: &str, port: u16) -> Option<String> {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    Some(format!("{host}:{port}"))
}

fn realtime_origin_rejected_response(message: &'static str) -> Box<Response> {
    Box::new(runtime_status_response(tonic::Status::permission_denied(message)))
}

/// Returns the supported realtime protocol versions and method descriptors.
///
/// # Errors
/// Returns an error response when console authorization fails.
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

/// Negotiates a realtime HTTP handshake without opening a WebSocket.
///
/// # Errors
/// Returns an error response when console authorization fails or the handshake
/// payload is outside the supported protocol range.
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

/// Dispatches one realtime command over HTTP using the negotiated context.
///
/// # Errors
/// Returns an error response when console authorization fails or the handshake
/// payload is invalid for the current protocol.
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

/// HTTP envelope for one negotiated realtime command dispatch.
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

#[cfg(test)]
mod tests {
    use super::validate_realtime_ws_origin;
    use axum::http::{header, HeaderMap, HeaderValue};

    fn headers(host: &str, origin: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_str(host).expect("host should parse"));
        if let Some(origin) = origin {
            headers.insert(
                header::ORIGIN,
                HeaderValue::from_str(origin).expect("origin should parse"),
            );
        }
        headers
    }

    #[test]
    fn realtime_ws_origin_allows_non_browser_clients_without_origin() {
        let headers = headers("127.0.0.1:7142", None);
        validate_realtime_ws_origin(&headers).expect("missing Origin should be allowed");
    }

    #[test]
    fn realtime_ws_origin_allows_matching_request_host() {
        let headers = headers("127.0.0.1:7142", Some("http://127.0.0.1:7142"));
        validate_realtime_ws_origin(&headers).expect("matching Origin should be allowed");
    }

    #[test]
    fn realtime_ws_origin_rejects_different_port() {
        let headers = headers("127.0.0.1:7142", Some("http://127.0.0.1:4173"));
        validate_realtime_ws_origin(&headers).expect_err("different Origin port must be rejected");
    }

    #[test]
    fn realtime_ws_origin_rejects_invalid_origin() {
        let headers = headers("127.0.0.1:7142", Some("null"));
        validate_realtime_ws_origin(&headers).expect_err("invalid Origin must be rejected");
    }
}
