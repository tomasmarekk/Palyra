use serde_json::{json, Value};

use crate::{
    app::state::AppState,
    application::channels::providers::{
        build_channel_provider_health_refresh_payload, build_channel_provider_operations_payload,
        find_matching_message,
    },
    *,
};

#[allow(clippy::result_large_err)]
pub(crate) fn build_channel_status_payload(
    state: &AppState,
    connector_id: &str,
) -> Result<Value, Response> {
    let connector = state.channels.status(connector_id).map_err(channel_platform_error_response)?;
    let runtime =
        state.channels.runtime_snapshot(connector_id).map_err(channel_platform_error_response)?;
    let queue =
        state.channels.queue_snapshot(connector_id).map_err(channel_platform_error_response)?;
    let recent_dead_letters = state
        .channels
        .dead_letters(connector_id, Some(5))
        .map_err(channel_platform_error_response)?;
    Ok(json!({
        "connector": connector,
        "runtime": runtime,
        "operations": build_channel_operations_snapshot(
            connector_id,
            &connector,
            runtime.as_ref(),
            &queue,
            recent_dead_letters.as_slice(),
        ),
    }))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn build_channel_health_refresh_payload(
    state: &AppState,
    connector_id: &str,
    verify_channel_id: Option<String>,
) -> Result<Value, Response> {
    let mut payload = build_channel_status_payload(state, connector_id)?;
    payload["health_refresh"] =
        build_channel_provider_health_refresh_payload(state, connector_id, verify_channel_id)
            .await?;
    if let Some(message) = health_refresh_auth_failure_message(&payload["health_refresh"]) {
        apply_channel_auth_failure_surface(&mut payload, message.as_str());
    }
    Ok(payload)
}

fn build_channel_operations_snapshot(
    connector_id: &str,
    connector: &palyra_connectors::ConnectorStatusSnapshot,
    runtime: Option<&Value>,
    queue: &palyra_connectors::ConnectorQueueSnapshot,
    recent_dead_letters: &[palyra_connectors::DeadLetterRecord],
) -> Value {
    let last_runtime_error = runtime
        .and_then(|payload| payload.get("last_error"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let runtime_global_retry_after_ms = runtime
        .and_then(|payload| payload.get("global_retry_after_ms"))
        .and_then(Value::as_i64)
        .filter(|value| *value > 0);
    let active_route_limits = runtime
        .and_then(|payload| payload.get("route_rate_limits"))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter(|entry| {
                    entry
                        .get("retry_after_ms")
                        .and_then(Value::as_i64)
                        .is_some_and(|value| value > 0)
                })
                .count()
        })
        .unwrap_or(0);
    let last_auth_failure = find_channel_auth_failure(connector, runtime, recent_dead_letters);
    let mut saturation_reasons = Vec::new();
    let saturation_state = if !connector.enabled {
        saturation_reasons.push("connector_disabled".to_owned());
        "paused"
    } else if queue.paused {
        saturation_reasons.push("queue_paused".to_owned());
        if let Some(reason) = queue.pause_reason.as_deref() {
            saturation_reasons.push(format!("pause_reason={reason}"));
        }
        "paused"
    } else if queue.dead_letters > 0 {
        saturation_reasons.push(format!("dead_letters={}", queue.dead_letters));
        "dead_lettered"
    } else if runtime_global_retry_after_ms.is_some() || active_route_limits > 0 {
        if let Some(wait_ms) = runtime_global_retry_after_ms {
            saturation_reasons.push(format!("global_retry_after_ms={wait_ms}"));
        }
        if active_route_limits > 0 {
            saturation_reasons.push(format!("active_route_limits={active_route_limits}"));
        }
        "rate_limited"
    } else if let Some(error) = last_auth_failure.as_deref() {
        saturation_reasons.push(format!("last_auth_failure={error}"));
        "auth_failed"
    } else if queue.claimed_outbox > 0 || queue.due_outbox > 0 {
        if queue.claimed_outbox > 0 {
            saturation_reasons.push(format!("claimed_outbox={}", queue.claimed_outbox));
        }
        if queue.due_outbox > 0 {
            saturation_reasons.push(format!("due_outbox={}", queue.due_outbox));
        }
        "backpressure"
    } else if queue.pending_outbox > 0 {
        saturation_reasons.push(format!("pending_outbox={}", queue.pending_outbox));
        "retrying"
    } else {
        "healthy"
    };
    if let Some(error) = &connector.last_error {
        saturation_reasons.push(format!("last_error={error}"));
    } else if let Some(error) = &last_runtime_error {
        saturation_reasons.push(format!("runtime_error={error}"));
    }
    let provider = build_channel_provider_operations_payload(
        connector_id,
        connector,
        runtime,
        recent_dead_letters,
    );
    json!({
        "queue": {
            "pending_outbox": queue.pending_outbox,
            "due_outbox": queue.due_outbox,
            "claimed_outbox": queue.claimed_outbox,
            "dead_letters": queue.dead_letters,
            "paused": queue.paused,
            "pause_reason": queue.pause_reason,
            "pause_updated_at_unix_ms": queue.pause_updated_at_unix_ms,
            "next_attempt_unix_ms": queue.next_attempt_unix_ms,
            "oldest_pending_created_at_unix_ms": queue.oldest_pending_created_at_unix_ms,
            "latest_dead_letter_unix_ms": queue.latest_dead_letter_unix_ms,
        },
        "saturation": {
            "state": saturation_state,
            "reasons": saturation_reasons,
        },
        "last_auth_failure": last_auth_failure,
        "rate_limits": {
            "global_retry_after_ms": runtime_global_retry_after_ms,
            "active_route_limits": active_route_limits,
            "routes": runtime.and_then(|payload| payload.get("route_rate_limits")).cloned(),
        },
        "discord": provider,
    })
}

fn find_channel_auth_failure(
    connector: &palyra_connectors::ConnectorStatusSnapshot,
    runtime: Option<&Value>,
    recent_dead_letters: &[palyra_connectors::DeadLetterRecord],
) -> Option<String> {
    find_matching_message(
        [
            connector.last_error.as_deref(),
            runtime.and_then(|payload| payload.get("last_error")).and_then(Value::as_str),
            recent_dead_letters.first().map(|entry| entry.reason.as_str()),
        ],
        &[
            "auth",
            "token",
            "unauthorized",
            "credential missing",
            "missing credential",
            "secret not found",
        ],
    )
}

fn health_refresh_auth_failure_message(health_refresh: &Value) -> Option<String> {
    let message = health_refresh.get("message").and_then(Value::as_str)?;
    find_matching_message(
        [Some(message)],
        &[
            "auth",
            "token",
            "unauthorized",
            "credential missing",
            "missing credential",
            "secret not found",
        ],
    )
}

fn apply_channel_auth_failure_surface(payload: &mut Value, message: &str) {
    let readiness = if message.contains("credential missing")
        || message.contains("missing credential")
        || message.contains("secret not found")
    {
        "missing_credential"
    } else {
        "auth_failed"
    };
    payload["connector"]["readiness"] = Value::String(readiness.to_owned());
    payload["connector"]["liveness"] = Value::String("stopped".to_owned());
    if payload["connector"].get("last_error").is_none()
        || payload["connector"]["last_error"].is_null()
        || payload["connector"]["last_error"].as_str().is_some_and(|value| value.trim().is_empty())
    {
        payload["connector"]["last_error"] = Value::String(message.to_owned());
    }
    payload["operations"]["last_auth_failure"] = Value::String(message.to_owned());
    payload["operations"]["saturation"]["state"] = Value::String("auth_failed".to_owned());
    let reasons = payload["operations"]["saturation"]["reasons"]
        .as_array_mut()
        .expect("channel saturation reasons should stay array-backed");
    let failure_reason = format!("last_auth_failure={message}");
    if !reasons.iter().filter_map(Value::as_str).any(|existing| existing == failure_reason) {
        reasons.push(Value::String(failure_reason));
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_channel_auth_failure_surface, build_channel_operations_snapshot};
    use palyra_connectors::{
        ConnectorAvailability, ConnectorKind, ConnectorLiveness, ConnectorQueueDepth,
        ConnectorReadiness, ConnectorStatusSnapshot,
    };
    use serde_json::{json, Value};

    fn sample_connector() -> ConnectorStatusSnapshot {
        ConnectorStatusSnapshot {
            connector_id: "discord:default".to_owned(),
            kind: ConnectorKind::Discord,
            availability: ConnectorAvailability::Supported,
            capabilities: palyra_connectors::providers::provider_capabilities(
                ConnectorKind::Discord,
            ),
            principal: "channel:discord".to_owned(),
            enabled: true,
            readiness: ConnectorReadiness::Ready,
            liveness: ConnectorLiveness::Running,
            restart_count: 0,
            queue_depth: ConnectorQueueDepth { pending_outbox: 0, dead_letters: 0 },
            last_error: Some("discord credential missing for connector discord:default".to_owned()),
            last_inbound_unix_ms: None,
            last_outbound_unix_ms: None,
            updated_at_unix_ms: 0,
        }
    }

    fn sample_queue() -> palyra_connectors::ConnectorQueueSnapshot {
        palyra_connectors::ConnectorQueueSnapshot {
            pending_outbox: 0,
            due_outbox: 0,
            claimed_outbox: 0,
            dead_letters: 0,
            paused: false,
            pause_reason: None,
            pause_updated_at_unix_ms: None,
            next_attempt_unix_ms: None,
            oldest_pending_created_at_unix_ms: None,
            latest_dead_letter_unix_ms: None,
        }
    }

    #[test]
    fn channel_operations_snapshot_fails_closed_on_auth_failure() {
        let payload = build_channel_operations_snapshot(
            "discord:default",
            &sample_connector(),
            None,
            &sample_queue(),
            &[],
        );
        assert_eq!(
            payload.pointer("/saturation/state").and_then(Value::as_str),
            Some("auth_failed")
        );
        assert_eq!(
            payload.get("last_auth_failure").and_then(Value::as_str),
            Some("discord credential missing for connector discord:default")
        );
    }

    #[test]
    fn health_refresh_auth_failure_overrides_ready_running_surface() {
        let mut payload = json!({
            "connector": {
                "enabled": true,
                "readiness": "ready",
                "liveness": "running",
                "last_error": null,
            },
            "operations": {
                "last_auth_failure": null,
                "saturation": {
                    "state": "healthy",
                    "reasons": [],
                },
            },
        });

        apply_channel_auth_failure_surface(
            &mut payload,
            "failed to load Discord token from vault ref 'global/discord_bot_token': secret not found",
        );

        assert_eq!(
            payload.pointer("/connector/readiness").and_then(Value::as_str),
            Some("missing_credential")
        );
        assert_eq!(payload.pointer("/connector/liveness").and_then(Value::as_str), Some("stopped"));
        assert_eq!(
            payload.pointer("/operations/saturation/state").and_then(Value::as_str),
            Some("auth_failed")
        );
    }
}
