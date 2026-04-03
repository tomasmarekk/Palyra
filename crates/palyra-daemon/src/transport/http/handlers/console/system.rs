use super::diagnostics::{
    authorize_console_session, build_connector_observability, build_page_info,
    build_support_bundle_observability, collect_console_browser_diagnostics,
    collect_console_deployment_diagnostics, redact_console_diagnostics_value,
};
use crate::gateway::current_unix_ms;
use crate::*;

const DEFAULT_SYSTEM_EVENTS_LIMIT: usize = 200;
const MAX_SYSTEM_EVENTS_LIMIT: usize = 2_000;
const MAX_SYSTEM_EVENT_NAME_BYTES: usize = 64;
const MAX_SYSTEM_EVENT_SUMMARY_BYTES: usize = 240;
const MAX_SYSTEM_EVENT_DETAILS_BYTES: usize = 8 * 1024;

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleSystemEventsQuery {
    limit: Option<usize>,
    kind: Option<i32>,
    principal: Option<String>,
    channel: Option<String>,
    contains: Option<String>,
    event: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleSystemEventEmitRequest {
    name: String,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    details: Option<Value>,
}

pub(crate) async fn console_system_heartbeat_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let status_snapshot = state
        .runtime
        .status_snapshot_async(session.context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let deployment = collect_console_deployment_diagnostics(&state);
    let generated_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;

    Ok(Json(json!({
        "contract": contract_descriptor(),
        "generated_at_unix_ms": generated_at_unix_ms,
        "service": status_snapshot.service,
        "status": status_snapshot.status,
        "version": status_snapshot.version,
        "git_hash": status_snapshot.git_hash,
        "build_profile": status_snapshot.build_profile,
        "uptime_seconds": status_snapshot.uptime_seconds,
        "request_context": status_snapshot.request_context,
        "transport": status_snapshot.transport,
        "security": status_snapshot.security,
        "storage": status_snapshot.storage,
        "counters": {
            "admin_status_requests": status_snapshot.counters.admin_status_requests,
            "denied_requests": status_snapshot.counters.denied_requests,
            "journal_events": status_snapshot.counters.journal_events,
        },
        "deployment": deployment,
    })))
}

pub(crate) async fn console_system_presence_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let status_snapshot = state
        .runtime
        .status_snapshot_async(session.context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let auth_snapshot = state
        .auth_runtime
        .admin_status_snapshot(Arc::clone(&state.runtime))
        .await
        .map_err(runtime_status_response)?;

    let mut auth_payload = serde_json::to_value(&auth_snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize system auth presence payload: {error}"
        )))
    })?;
    redact_console_diagnostics_value(&mut auth_payload, None);

    let browser_payload = collect_console_browser_diagnostics(&state).await;
    let media_payload = state.channels.media_snapshot().map_err(channel_platform_error_response)?;
    let connector_payload =
        build_connector_observability(&state, &media_payload).map_err(|error| *error)?;
    let support_bundle_payload = build_support_bundle_observability(&state);
    let memory_status =
        state.runtime.memory_maintenance_status().await.map_err(runtime_status_response)?;
    let deployment = collect_console_deployment_diagnostics(&state);
    let generated_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;

    Ok(Json(json!({
        "contract": contract_descriptor(),
        "generated_at_unix_ms": generated_at_unix_ms,
        "subsystems": {
            "gateway": {
                "state": status_snapshot.status,
                "service": status_snapshot.service,
                "uptime_seconds": status_snapshot.uptime_seconds,
                "transport": status_snapshot.transport,
            },
            "model_provider": {
                "state": model_provider_presence_state(&status_snapshot.model_provider),
                "kind": status_snapshot.model_provider.kind,
                "auth_profile_id": status_snapshot.model_provider.auth_profile_id,
                "credential_source": status_snapshot.model_provider.credential_source,
                "runtime_metrics": status_snapshot.model_provider.runtime_metrics,
            },
            "auth_profiles": {
                "state": auth_profiles_presence_state(&auth_payload),
                "summary": auth_payload.get("summary").cloned().unwrap_or(Value::Null),
                "refresh_metrics": auth_payload.get("refresh_metrics").cloned().unwrap_or(Value::Null),
            },
            "browserd": {
                "state": browser_presence_state(&browser_payload),
                "status": browser_payload,
            },
            "channels": {
                "state": channel_presence_state(&connector_payload),
                "status": connector_payload,
            },
            "memory": {
                "state": "ok",
                "usage": memory_status.usage,
                "maintenance": {
                    "last_run": memory_status.last_run,
                    "last_vacuum_at_unix_ms": memory_status.last_vacuum_at_unix_ms,
                    "next_vacuum_due_at_unix_ms": memory_status.next_vacuum_due_at_unix_ms,
                    "next_run_at_unix_ms": memory_status.next_maintenance_run_at_unix_ms,
                },
            },
            "support_bundle": {
                "state": support_bundle_presence_state(&support_bundle_payload),
                "status": support_bundle_payload,
            },
        },
        "deployment": deployment,
    })))
}

pub(crate) async fn console_system_events_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSystemEventsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit =
        query.limit.unwrap_or(DEFAULT_SYSTEM_EVENTS_LIMIT).clamp(1, MAX_SYSTEM_EVENTS_LIMIT);
    let snapshot =
        state.runtime.recent_journal_snapshot(limit).await.map_err(runtime_status_response)?;
    let contains = query
        .contains
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    let requested_event = query
        .event
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());

    let events = snapshot
        .events
        .into_iter()
        .filter(|event| query.kind.is_none_or(|kind| event.kind == kind))
        .filter(|event| {
            query
                .principal
                .as_deref()
                .is_none_or(|principal| event.principal.eq_ignore_ascii_case(principal.trim()))
        })
        .filter(|event| {
            query.channel.as_deref().is_none_or(|channel| {
                event
                    .channel
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case(channel.trim()))
            })
        })
        .filter(|event| {
            contains.as_ref().is_none_or(|needle| {
                event.payload_json.to_ascii_lowercase().contains(needle.as_str())
            })
        })
        .filter(|event| {
            requested_event.as_ref().is_none_or(|requested| {
                extract_operator_event_name(event.payload_json.as_str())
                    .is_some_and(|value| value.eq_ignore_ascii_case(requested))
            })
        })
        .map(map_system_event_record)
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "hash_chain_enabled": snapshot.hash_chain_enabled,
        "total_events": snapshot.total_events,
        "returned_events": events.len(),
        "events": events,
        "page": build_page_info(limit, events.len(), None),
    })))
}

pub(crate) async fn console_system_event_emit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleSystemEventEmitRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let name =
        validate_system_event_name(payload.name.as_str()).map_err(runtime_status_response)?;
    let event = format!("system.operator.{name}");
    let summary = payload
        .summary
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if summary.as_deref().is_some_and(|value| value.len() > MAX_SYSTEM_EVENT_SUMMARY_BYTES) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "system event summary must be {} bytes or fewer",
            MAX_SYSTEM_EVENT_SUMMARY_BYTES
        ))));
    }

    let details = payload.details.unwrap_or_else(|| json!({}));
    if !details.is_object() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "system event details must be a JSON object",
        )));
    }
    let details_size =
        serde_json::to_vec(&details).map(|encoded| encoded.len()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to encode system event details: {error}"
            )))
        })?;
    if details_size > MAX_SYSTEM_EVENT_DETAILS_BYTES {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "system event details must be {} bytes or fewer",
            MAX_SYSTEM_EVENT_DETAILS_BYTES
        ))));
    }

    let emitted_at_unix_ms = current_unix_ms();
    let event_details = json!({
        "name": name,
        "summary": summary,
        "details": details,
        "emitted_at_unix_ms": emitted_at_unix_ms,
    });
    state
        .runtime
        .record_console_event(&session.context, event.as_str(), event_details.clone())
        .await
        .map_err(runtime_status_response)?;
    let mut routine_payload = event_details.clone();
    if let Some(object) = routine_payload.as_object_mut() {
        object.insert("event".to_owned(), Value::String(event.clone()));
    }
    let routine_dispatches = super::routines::dispatch_system_event_routines(
        &state,
        session.context.principal.as_str(),
        event.as_str(),
        routine_payload,
    )
    .await?;

    Ok(Json(json!({
        "status": "emitted",
        "event": event,
        "details": event_details,
        "routine_dispatches": routine_dispatches,
    })))
}

fn validate_system_event_name(raw: &str) -> Result<String, tonic::Status> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(tonic::Status::invalid_argument("system event name cannot be empty"));
    }
    if trimmed.len() > MAX_SYSTEM_EVENT_NAME_BYTES {
        return Err(tonic::Status::invalid_argument(format!(
            "system event name must be {} bytes or fewer",
            MAX_SYSTEM_EVENT_NAME_BYTES
        )));
    }
    if !trimmed.bytes().all(|byte| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || matches!(byte, b'.' | b'-' | b'_' | b':')
    }) {
        return Err(tonic::Status::invalid_argument(
            "system event name may only contain lowercase ASCII letters, digits, '.', '-', '_' or ':'",
        ));
    }
    Ok(trimmed)
}

fn map_system_event_record(event: journal::JournalEventRecord) -> Value {
    let operator_event = extract_operator_event_name(event.payload_json.as_str());
    json!({
        "seq": event.seq,
        "event_id": event.event_id,
        "session_id": event.session_id,
        "run_id": event.run_id,
        "kind": event.kind,
        "kind_label": journal_event_kind_label(event.kind),
        "actor": event.actor,
        "actor_label": journal_event_actor_label(event.actor),
        "timestamp_unix_ms": event.timestamp_unix_ms,
        "payload_json": decode_system_event_payload(event.payload_json.as_str()),
        "operator_event": operator_event,
        "redacted": event.redacted,
        "hash": event.hash,
        "prev_hash": event.prev_hash,
        "principal": event.principal,
        "device_id": event.device_id,
        "channel": event.channel,
        "created_at_unix_ms": event.created_at_unix_ms,
    })
}

fn extract_operator_event_name(payload_json: &str) -> Option<String> {
    serde_json::from_str::<Value>(payload_json)
        .ok()
        .and_then(|payload| payload.get("event").and_then(Value::as_str).map(str::to_owned))
}

fn decode_system_event_payload(payload_json: &str) -> Value {
    serde_json::from_str::<Value>(payload_json)
        .unwrap_or_else(|_| Value::String(payload_json.to_owned()))
}

fn journal_event_kind_label(raw: i32) -> &'static str {
    match common_v1::journal_event::EventKind::try_from(raw)
        .unwrap_or(common_v1::journal_event::EventKind::Unspecified)
    {
        common_v1::journal_event::EventKind::MessageReceived => "message_received",
        common_v1::journal_event::EventKind::ModelToken => "model_token",
        common_v1::journal_event::EventKind::ToolProposed => "tool_proposed",
        common_v1::journal_event::EventKind::ToolExecuted => "tool_executed",
        common_v1::journal_event::EventKind::A2uiUpdated => "a2ui_updated",
        common_v1::journal_event::EventKind::RunCompleted => "run_completed",
        common_v1::journal_event::EventKind::RunFailed => "run_failed",
        common_v1::journal_event::EventKind::Unspecified => "unspecified",
    }
}

fn journal_event_actor_label(raw: i32) -> &'static str {
    match common_v1::journal_event::EventActor::try_from(raw)
        .unwrap_or(common_v1::journal_event::EventActor::Unspecified)
    {
        common_v1::journal_event::EventActor::User => "user",
        common_v1::journal_event::EventActor::Agent => "agent",
        common_v1::journal_event::EventActor::System => "system",
        common_v1::journal_event::EventActor::Plugin => "plugin",
        common_v1::journal_event::EventActor::Unspecified => "unspecified",
    }
}

fn model_provider_presence_state(
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
) -> &'static str {
    if snapshot.circuit_breaker.open || snapshot.runtime_metrics.error_count > 0 {
        "degraded"
    } else if snapshot.api_key_configured || snapshot.auth_profile_id.is_some() {
        "ready"
    } else {
        "missing_auth"
    }
}

fn auth_profiles_presence_state(payload: &Value) -> &'static str {
    let missing = payload.pointer("/summary/missing").and_then(Value::as_u64).unwrap_or(0);
    let expired = payload.pointer("/summary/expired").and_then(Value::as_u64).unwrap_or(0);
    let failures =
        payload.pointer("/refresh_metrics/failures").and_then(Value::as_u64).unwrap_or(0);
    if missing > 0 || expired > 0 || failures > 0 {
        "degraded"
    } else {
        "ok"
    }
}

fn browser_presence_state(payload: &Value) -> &'static str {
    if !payload.get("enabled").and_then(Value::as_bool).unwrap_or(false) {
        "disabled"
    } else if payload
        .pointer("/failures/recent_relay_action_failures")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        > 0
        || payload.pointer("/failures/recent_health_failures").and_then(Value::as_u64).unwrap_or(0)
            > 0
    {
        "degraded"
    } else {
        "ok"
    }
}

fn channel_presence_state(payload: &Value) -> &'static str {
    if payload.get("degraded_connectors").and_then(Value::as_u64).unwrap_or(0) > 0
        || payload.get("dead_letters").and_then(Value::as_u64).unwrap_or(0) > 0
    {
        "degraded"
    } else {
        "ok"
    }
}

fn support_bundle_presence_state(payload: &Value) -> &'static str {
    if payload.get("failures").and_then(Value::as_u64).unwrap_or(0) > 0 {
        "degraded"
    } else {
        "ok"
    }
}
