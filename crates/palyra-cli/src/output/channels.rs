//! Output rendering for `palyra channels` commands over raw daemon JSON
//! payloads: connector list/status and router rules/warnings/preview/pairings.
//!
//! Text lines and JSON shapes are pinned by CLI parity tests; missing fields
//! render as stable placeholders instead of failing.

use anyhow::Result;
use palyra_common::redaction::REDACTED;
use serde_json::Value;

use super::print_json_pretty;

/// Emits the connector list as pretty JSON or pinned text lines.
///
/// # Errors
/// Returns an error when the JSON payload cannot be encoded.
pub(crate) fn emit_list(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        return print_json_pretty(&payload, "failed to encode channels list payload as JSON");
    }
    for line in render_list_lines(&payload) {
        println!("{line}");
    }
    Ok(())
}

/// Emits one connector's status as pretty JSON or pinned text lines.
///
/// # Errors
/// Returns an error when the JSON payload cannot be encoded.
pub(crate) fn emit_status(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        return print_json_pretty(&payload, "failed to encode channels status payload as JSON");
    }
    for line in render_status_lines(&payload) {
        println!("{line}");
    }
    Ok(())
}

/// Emits the router rules summary as pretty JSON or a pinned text line.
///
/// # Errors
/// Returns an error when the JSON payload cannot be encoded.
pub(crate) fn emit_router_rules(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        return print_json_pretty(
            &payload,
            "failed to encode channel router rules payload as JSON",
        );
    }

    let config_hash = read_json_string(&payload, &["config_hash"]).unwrap_or("unknown");
    let config = payload.get("config").cloned().unwrap_or(Value::Null);
    let enabled = config.get("enabled").and_then(Value::as_bool).unwrap_or(false);
    let channels =
        config.get("channels").and_then(Value::as_array).map(|value| value.len()).unwrap_or(0);
    let default_dm_policy =
        config.get("default_direct_message_policy").and_then(Value::as_str).unwrap_or("unknown");
    let default_broadcast_strategy =
        config.get("default_broadcast_strategy").and_then(Value::as_str).unwrap_or("unknown");

    println!(
        "channels.router.rules config_hash={} enabled={} channels={} default_direct_message_policy={} default_broadcast_strategy={}",
        config_hash, enabled, channels, default_dm_policy, default_broadcast_strategy
    );
    Ok(())
}

/// Emits router warnings as pretty JSON or pinned per-warning text lines.
///
/// # Errors
/// Returns an error when the JSON payload cannot be encoded.
pub(crate) fn emit_router_warnings(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        return print_json_pretty(
            &payload,
            "failed to encode channel router warnings payload as JSON",
        );
    }

    let config_hash = read_json_string(&payload, &["config_hash"]).unwrap_or("unknown");
    let warnings = payload.get("warnings").and_then(Value::as_array).cloned().unwrap_or_default();
    println!("channels.router.warnings config_hash={} count={}", config_hash, warnings.len());
    for warning in warnings {
        if let Some(text) = warning.as_str() {
            println!("channels.router.warning text={text}");
        }
    }
    Ok(())
}

/// Emits a router preview as pretty JSON or a pinned text line; the session
/// key is redacted in both formats before anything is printed.
///
/// # Errors
/// Returns an error when the JSON payload cannot be encoded.
pub(crate) fn emit_router_preview(mut payload: Value, json_output: bool) -> Result<()> {
    redact_router_preview_session_key(&mut payload);

    if json_output {
        return print_json_pretty(
            &payload,
            "failed to encode channel router preview payload as JSON",
        );
    }

    let preview = payload.get("preview").cloned().unwrap_or(payload);
    let accepted = preview.get("accepted").and_then(Value::as_bool).unwrap_or(false);
    let reason = preview.get("reason").and_then(Value::as_str).unwrap_or("unknown");
    let route_key = preview.get("route_key").and_then(Value::as_str).unwrap_or("");
    let sender_identity = preview.get("sender_identity").and_then(Value::as_str).unwrap_or("");
    let config_hash = preview.get("config_hash").and_then(Value::as_str).unwrap_or("unknown");
    println!(
        "channels.router.preview accepted={} reason={} route_key={} sender_identity={} config_hash={}",
        accepted, reason, route_key, sender_identity, config_hash
    );
    Ok(())
}

/// Emits per-channel pairing state (pending, granted, active codes) as pretty
/// JSON or pinned text lines.
///
/// # Errors
/// Returns an error when the JSON payload cannot be encoded.
pub(crate) fn emit_router_pairings(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        return print_json_pretty(
            &payload,
            "failed to encode channel router pairings payload as JSON",
        );
    }

    let config_hash = read_json_string(&payload, &["config_hash"]).unwrap_or("unknown");
    let pairings = payload.get("pairings").and_then(Value::as_array).cloned().unwrap_or_default();
    println!("channels.router.pairings config_hash={} channels={}", config_hash, pairings.len());
    for pairing in pairings {
        let channel = pairing.get("channel").and_then(Value::as_str).unwrap_or("unknown");
        let pending = pairing.get("pending").and_then(Value::as_array).cloned().unwrap_or_default();
        let paired = pairing.get("paired").and_then(Value::as_array).cloned().unwrap_or_default();
        let active_codes =
            pairing.get("active_codes").and_then(Value::as_array).cloned().unwrap_or_default();
        println!(
            "channels.router.pairing channel={} pending={} paired={} active_codes={}",
            channel,
            pending.len(),
            paired.len(),
            active_codes.len()
        );

        for entry in pending {
            let sender = entry.get("sender_identity").and_then(Value::as_str).unwrap_or("unknown");
            let code = entry.get("code").and_then(Value::as_str).unwrap_or("unknown");
            let approval_id = entry.get("approval_id").and_then(Value::as_str).unwrap_or("");
            let expires_at = entry.get("expires_at_unix_ms").and_then(Value::as_i64).unwrap_or(0);
            println!(
                "channels.router.pairing.pending channel={} sender={} code={} approval_id={} expires_at_unix_ms={}",
                channel, sender, code, approval_id, expires_at
            );
        }
        for entry in paired {
            let sender = entry.get("sender_identity").and_then(Value::as_str).unwrap_or("unknown");
            let approval_id = entry.get("approval_id").and_then(Value::as_str).unwrap_or("");
            let expires_at = entry
                .get("expires_at_unix_ms")
                .and_then(Value::as_i64)
                .map(|value| value.to_string())
                .unwrap_or_default();
            println!(
                "channels.router.pairing.grant channel={} sender={} approval_id={} expires_at_unix_ms={}",
                channel, sender, approval_id, expires_at
            );
        }
        for entry in active_codes {
            let code = entry.get("code").and_then(Value::as_str).unwrap_or("unknown");
            let issued_by = entry.get("issued_by").and_then(Value::as_str).unwrap_or("unknown");
            let expires_at = entry.get("expires_at_unix_ms").and_then(Value::as_i64).unwrap_or(0);
            println!(
                "channels.router.pairing.code channel={} code={} issued_by={} expires_at_unix_ms={}",
                channel, code, issued_by, expires_at
            );
        }
    }
    Ok(())
}

/// Emits a newly issued pairing code as pretty JSON or a pinned text line.
///
/// # Errors
/// Returns an error when the JSON payload cannot be encoded.
pub(crate) fn emit_router_pairing_code(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        return print_json_pretty(
            &payload,
            "failed to encode channel router pairing code payload as JSON",
        );
    }

    let config_hash = read_json_string(&payload, &["config_hash"]).unwrap_or("unknown");
    let code = payload.get("code").cloned().unwrap_or(Value::Null);
    let channel = code.get("channel").and_then(Value::as_str).unwrap_or("unknown");
    let code_value = code.get("code").and_then(Value::as_str).unwrap_or("unknown");
    let issued_by = code.get("issued_by").and_then(Value::as_str).unwrap_or("unknown");
    let expires_at = code.get("expires_at_unix_ms").and_then(Value::as_i64).unwrap_or(0);
    println!(
        "channels.router.pairing-code channel={} code={} issued_by={} expires_at_unix_ms={} config_hash={}",
        channel, code_value, issued_by, expires_at, config_hash
    );
    Ok(())
}

/// Replaces the `session_key` value with the redaction placeholder, handling
/// both the nested `preview` object and bare top-level payload shapes.
pub(crate) fn redact_router_preview_session_key(payload: &mut Value) {
    let target = if let Some(preview) = payload.get_mut("preview") { preview } else { payload };
    if let Some(object) = target.as_object_mut() {
        if object.contains_key("session_key") {
            object.insert("session_key".to_owned(), Value::String(REDACTED.to_owned()));
        }
    }
}

/// Renders the pinned text lines for the connector list.
pub(crate) fn render_list_lines(payload: &Value) -> Vec<String> {
    // Deferred connectors are hidden entirely (including from the count):
    // they are unavailable placeholders, not operable channels.
    let connectors = payload
        .get("connectors")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|connector| connector_availability(connector) != "deferred")
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut lines = vec![format!("channels.count={}", connectors.len())];
    for connector in connectors {
        let connector_id =
            connector.get("connector_id").and_then(Value::as_str).unwrap_or("unknown");
        let kind = connector.get("kind").and_then(Value::as_str).unwrap_or("unknown");
        let availability = connector_availability(connector);
        let enabled = connector.get("enabled").and_then(Value::as_bool).unwrap_or(false);
        let readiness = connector.get("readiness").and_then(Value::as_str).unwrap_or("unknown");
        let liveness = connector.get("liveness").and_then(Value::as_str).unwrap_or("unknown");
        lines.push(format!(
            "channels.connector id={} kind={} availability={} enabled={} readiness={} liveness={}",
            connector_id, kind, availability, enabled, readiness, liveness
        ));
    }
    lines
}

/// Renders the pinned text lines for one connector's status, including the
/// optional operations, health-refresh, action, and runtime metrics sections.
pub(crate) fn render_status_lines(payload: &Value) -> Vec<String> {
    // Accept both the wrapped ({"connector": {...}}) and bare payload shapes.
    let connector = payload.get("connector").unwrap_or(payload);
    let connector_id = connector.get("connector_id").and_then(Value::as_str).unwrap_or("unknown");
    let availability = connector_availability(connector);
    let enabled = connector.get("enabled").and_then(Value::as_bool).unwrap_or(false);
    let readiness = connector.get("readiness").and_then(Value::as_str).unwrap_or("unknown");
    let liveness = connector.get("liveness").and_then(Value::as_str).unwrap_or("unknown");
    let pending = connector
        .get("queue_depth")
        .and_then(Value::as_object)
        .and_then(|queue| queue.get("pending_outbox"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let dead_letters = connector
        .get("queue_depth")
        .and_then(Value::as_object)
        .and_then(|queue| queue.get("dead_letters"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let mut lines = vec![format!(
        "channels.status id={} availability={} enabled={} readiness={} liveness={} pending_outbox={} dead_letters={}",
        connector_id, availability, enabled, readiness, liveness, pending, dead_letters
    )];
    if let Some(queue) =
        payload.get("operations").and_then(|value| value.get("queue")).and_then(Value::as_object)
    {
        let paused = queue.get("paused").and_then(Value::as_bool).unwrap_or(false);
        let pause_reason = queue.get("pause_reason").and_then(Value::as_str).unwrap_or("-");
        let pending_outbox = queue.get("pending_outbox").and_then(Value::as_u64).unwrap_or(0);
        let due_outbox = queue.get("due_outbox").and_then(Value::as_u64).unwrap_or(0);
        let claimed_outbox = queue.get("claimed_outbox").and_then(Value::as_u64).unwrap_or(0);
        let pending_ingress = queue.get("pending_ingress").and_then(Value::as_u64).unwrap_or(0);
        let due_ingress = queue.get("due_ingress").and_then(Value::as_u64).unwrap_or(0);
        let claimed_ingress = queue.get("claimed_ingress").and_then(Value::as_u64).unwrap_or(0);
        let retrying_ingress = queue.get("retrying_ingress").and_then(Value::as_u64).unwrap_or(0);
        let failed_ingress = queue.get("failed_ingress").and_then(Value::as_u64).unwrap_or(0);
        let quarantined_ingress =
            queue.get("quarantined_ingress").and_then(Value::as_u64).unwrap_or(0);
        let dead_letters = queue.get("dead_letters").and_then(Value::as_u64).unwrap_or(0);
        lines.push(format!(
            "channels.operations.queue id={} paused={} pause_reason={} pending_ingress={} due_ingress={} claimed_ingress={} retrying_ingress={} failed_ingress={} quarantined_ingress={} pending_outbox={} due_outbox={} claimed_outbox={} dead_letters={}",
            connector_id,
            paused,
            pause_reason,
            pending_ingress,
            due_ingress,
            claimed_ingress,
            retrying_ingress,
            failed_ingress,
            quarantined_ingress,
            pending_outbox,
            due_outbox,
            claimed_outbox,
            dead_letters
        ));
    }
    if let Some(saturation) = payload
        .get("operations")
        .and_then(|value| value.get("saturation"))
        .and_then(Value::as_object)
    {
        let state = saturation.get("state").and_then(Value::as_str).unwrap_or("unknown");
        let reasons = saturation
            .get("reasons")
            .and_then(Value::as_array)
            .map(|entries| entries.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "-".to_owned());
        lines.push(format!(
            "channels.operations.saturation id={} state={} reasons={}",
            connector_id, state, reasons
        ));
    }
    if let Some(auth_failure) = payload
        .get("operations")
        .and_then(|value| value.get("last_auth_failure"))
        .and_then(Value::as_str)
    {
        lines.push(format!(
            "channels.operations.auth_failure id={} message={}",
            connector_id, auth_failure
        ));
    }
    if let Some(rate_limits) = payload
        .get("operations")
        .and_then(|value| value.get("rate_limits"))
        .and_then(Value::as_object)
    {
        let global_retry_after_ms =
            rate_limits.get("global_retry_after_ms").and_then(Value::as_i64).unwrap_or(0);
        let active_route_limits =
            rate_limits.get("active_route_limits").and_then(Value::as_u64).unwrap_or(0);
        lines.push(format!(
            "channels.operations.rate_limits id={} global_retry_after_ms={} active_route_limits={}",
            connector_id, global_retry_after_ms, active_route_limits
        ));
    }
    if let Some(discord) =
        payload.get("operations").and_then(|value| value.get("discord")).and_then(Value::as_object)
    {
        if let Some(permission_failure) =
            discord.get("last_permission_failure").and_then(Value::as_str)
        {
            lines.push(format!(
                "channels.operations.permission_gap id={} message={}",
                connector_id, permission_failure
            ));
        }
    }
    if let Some(health_refresh) = payload.get("health_refresh").and_then(Value::as_object) {
        let supported = health_refresh.get("supported").and_then(Value::as_bool).unwrap_or(false);
        let refreshed = health_refresh.get("refreshed").and_then(Value::as_bool).unwrap_or(false);
        let message = health_refresh.get("message").and_then(Value::as_str).unwrap_or("-");
        lines.push(format!(
            "channels.health_refresh id={} supported={} refreshed={} message={}",
            connector_id, supported, refreshed, message
        ));
    }
    if let Some(action) = payload.get("action").and_then(Value::as_object) {
        let action_type = action.get("type").and_then(Value::as_str).unwrap_or("unknown");
        let message = action.get("message").and_then(Value::as_str).unwrap_or("-");
        lines.push(format!(
            "channels.action id={} type={} message={}",
            connector_id, action_type, message
        ));
    }
    if let Some(metrics) = payload
        .get("runtime")
        .and_then(Value::as_object)
        .and_then(|runtime| runtime.get("metrics"))
        .and_then(Value::as_object)
    {
        let inbound_processed =
            metrics.get("inbound_events_processed").and_then(Value::as_u64).unwrap_or(0);
        let dedupe_hits = metrics.get("inbound_dedupe_hits").and_then(Value::as_u64).unwrap_or(0);
        let outbound_ok = metrics.get("outbound_sends_ok").and_then(Value::as_u64).unwrap_or(0);
        let outbound_retry =
            metrics.get("outbound_sends_retry").and_then(Value::as_u64).unwrap_or(0);
        let outbound_dead_letter =
            metrics.get("outbound_sends_dead_letter").and_then(Value::as_u64).unwrap_or(0);
        lines.push(format!(
            "channels.metrics id={} inbound_processed={} dedupe_hits={} outbound_ok={} outbound_retry={} outbound_dead_letter={}",
            connector_id,
            inbound_processed,
            dedupe_hits,
            outbound_ok,
            outbound_retry,
            outbound_dead_letter
        ));
        if let Some(latency) = metrics.get("route_message_latency_ms").and_then(Value::as_object) {
            let samples = latency.get("sample_count").and_then(Value::as_u64).unwrap_or(0);
            let avg_ms = latency.get("avg_ms").and_then(Value::as_u64).unwrap_or(0);
            let max_ms = latency.get("max_ms").and_then(Value::as_u64).unwrap_or(0);
            lines.push(format!(
                "channels.metrics.route_latency id={} samples={} avg_ms={} max_ms={}",
                connector_id, samples, avg_ms, max_ms
            ));
        }
        if let Some(policy_denials) = metrics.get("policy_denials").and_then(Value::as_array) {
            for entry in policy_denials {
                let reason = entry.get("reason").and_then(Value::as_str).unwrap_or("unknown");
                let count = entry.get("count").and_then(Value::as_u64).unwrap_or(0);
                lines.push(format!(
                    "channels.metrics.policy_denial id={} reason={} count={}",
                    connector_id, reason, count
                ));
            }
        }
    }
    lines
}

fn read_json_string<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_str()
}

fn connector_availability(connector: &Value) -> &str {
    connector.get("availability").and_then(Value::as_str).unwrap_or("supported")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{redact_router_preview_session_key, render_list_lines, render_status_lines};
    use palyra_common::redaction::REDACTED;

    #[test]
    fn render_channels_list_hides_deferred_connectors_and_includes_availability() {
        let lines = render_list_lines(&json!({
            "connectors": [
                {
                    "connector_id": "slack:default",
                    "kind": "slack",
                    "availability": "deferred",
                    "enabled": false,
                    "readiness": "misconfigured",
                    "liveness": "stopped"
                },
                {
                    "connector_id": "echo:default",
                    "kind": "echo",
                    "availability": "internal_test_only",
                    "enabled": true,
                    "readiness": "ready",
                    "liveness": "running"
                }
            ]
        }));

        assert_eq!(lines, vec![
            "channels.count=1".to_owned(),
            "channels.connector id=echo:default kind=echo availability=internal_test_only enabled=true readiness=ready liveness=running"
                .to_owned(),
        ]);
    }

    #[test]
    fn render_channels_status_includes_operations_snapshot() {
        let lines = render_status_lines(&json!({
            "connector": {
                "connector_id": "discord:default",
                "availability": "supported",
                "enabled": false,
                "readiness": "missing_credential",
                "liveness": "stopped",
                "queue_depth": {
                    "pending_outbox": 0,
                    "dead_letters": 0
                }
            },
            "operations": {
                "queue": {
                    "paused": true,
                    "pause_reason": "operator requested queue pause via admin API",
                    "pending_ingress": 6,
                    "due_ingress": 5,
                    "claimed_ingress": 1,
                    "retrying_ingress": 2,
                    "failed_ingress": 1,
                    "quarantined_ingress": 0,
                    "pending_outbox": 4,
                    "due_outbox": 2,
                    "claimed_outbox": 1,
                    "dead_letters": 3
                },
                "saturation": {
                    "state": "paused",
                    "reasons": ["queue_paused", "pause_reason=operator requested queue pause via admin API"]
                },
                "last_auth_failure": "discord token validation failed",
                "rate_limits": {
                    "global_retry_after_ms": 1200,
                    "active_route_limits": 2
                },
                "discord": {
                    "last_permission_failure": "missing permissions: send messages"
                }
            },
            "health_refresh": {
                "supported": true,
                "refreshed": false,
                "message": "discord token missing"
            },
            "action": {
                "type": "queue_pause",
                "message": "queue paused for connector 'discord:default'"
            }
        }));

        assert_eq!(
            lines[0],
            "channels.status id=discord:default availability=supported enabled=false readiness=missing_credential liveness=stopped pending_outbox=0 dead_letters=0"
        );
        assert!(lines.iter().any(|line| {
            line == "channels.operations.queue id=discord:default paused=true pause_reason=operator requested queue pause via admin API pending_ingress=6 due_ingress=5 claimed_ingress=1 retrying_ingress=2 failed_ingress=1 quarantined_ingress=0 pending_outbox=4 due_outbox=2 claimed_outbox=1 dead_letters=3"
        }));
        assert!(lines.iter().any(|line| {
            line == "channels.operations.saturation id=discord:default state=paused reasons=queue_paused,pause_reason=operator requested queue pause via admin API"
        }));
        assert!(lines.iter().any(|line| {
            line == "channels.operations.auth_failure id=discord:default message=discord token validation failed"
        }));
        assert!(lines.iter().any(|line| {
            line == "channels.operations.permission_gap id=discord:default message=missing permissions: send messages"
        }));
        assert!(lines.iter().any(|line| {
            line == "channels.health_refresh id=discord:default supported=true refreshed=false message=discord token missing"
        }));
        assert!(lines.iter().any(|line| {
            line == "channels.action id=discord:default type=queue_pause message=queue paused for connector 'discord:default'"
        }));
    }

    #[test]
    fn redact_channel_router_preview_session_key_redacts_nested_preview_payload() {
        let mut payload = serde_json::json!({
            "preview": {
                "accepted": true,
                "session_key": "session-123",
            }
        });
        redact_router_preview_session_key(&mut payload);
        assert_eq!(
            payload
                .get("preview")
                .and_then(serde_json::Value::as_object)
                .and_then(|preview| preview.get("session_key"))
                .and_then(serde_json::Value::as_str),
            Some(REDACTED),
            "session_key should be redacted in nested preview payloads"
        );
    }

    #[test]
    fn redact_channel_router_preview_session_key_redacts_top_level_payload() {
        let mut payload = serde_json::json!({
            "accepted": true,
            "session_key": "session-123",
        });
        redact_router_preview_session_key(&mut payload);
        assert_eq!(
            payload.get("session_key").and_then(serde_json::Value::as_str),
            Some(REDACTED),
            "session_key should be redacted when preview is represented as top-level payload"
        );
    }
}
