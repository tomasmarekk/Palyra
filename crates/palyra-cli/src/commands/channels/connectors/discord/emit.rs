//! Renderers for Discord onboarding and verify responses.
//!
//! Diagnostics go to stderr so stdout stays reserved for the pinned
//! machine-readable success lines; helpers tolerate both top-level and
//! `preflight`-nested payload shapes returned by the daemon.

use serde_json::Value;

use crate::read_json_string;

/// Prints onboarding warnings and policy warnings to stderr.
pub(crate) fn onboarding_warnings(payload: &Value) {
    let preflight = payload.get("preflight").unwrap_or(payload);
    for warning in preflight.get("warnings").and_then(Value::as_array).into_iter().flatten() {
        if let Some(text) = warning.as_str() {
            eprintln!("warning: {text}");
        }
    }
    for warning in preflight.get("policy_warnings").and_then(Value::as_array).into_iter().flatten()
    {
        if let Some(text) = warning.as_str() {
            eprintln!("policy-warning: {text}");
        }
    }
}

/// Prints the inbound-monitor preflight summary to stderr when present.
pub(crate) fn inbound_monitor_summary(payload: &Value) {
    let inbound_monitor = payload.get("inbound_monitor").or_else(|| {
        payload.get("preflight").and_then(|preflight| preflight.get("inbound_monitor"))
    });
    let Some(summary) = inbound_monitor.and_then(Value::as_object) else {
        return;
    };
    let connector_registered =
        summary.get("connector_registered").and_then(Value::as_bool).unwrap_or(false);
    let gateway_connected =
        summary.get("gateway_connected").and_then(Value::as_bool).unwrap_or(false);
    let recent_inbound = summary.get("recent_inbound").and_then(Value::as_bool).unwrap_or(false);
    let last_inbound_unix_ms =
        summary.get("last_inbound_unix_ms").and_then(Value::as_i64).unwrap_or_default();
    let last_event_type =
        summary.get("last_event_type").and_then(Value::as_str).unwrap_or("unknown");
    eprintln!(
        "discord.inbound_monitor connector_registered={} gateway_connected={} recent_inbound={} last_inbound_unix_ms={} last_event_type={}",
        connector_registered,
        gateway_connected,
        recent_inbound,
        last_inbound_unix_ms,
        last_event_type
    );
}

/// Prints the verify-channel permission check result to stderr when present.
pub(crate) fn channel_permission_check(payload: &Value) {
    let permission_check = payload.get("channel_permission_check").or_else(|| {
        payload.get("preflight").and_then(|preflight| preflight.get("channel_permission_check"))
    });
    let Some(check) = permission_check.and_then(Value::as_object) else {
        return;
    };
    let channel_id = check.get("channel_id").and_then(Value::as_str).unwrap_or("unknown");
    let status = check.get("status").and_then(Value::as_str).unwrap_or("unknown");
    let can_view_channel = check.get("can_view_channel").and_then(Value::as_bool).unwrap_or(false);
    let can_send_messages =
        check.get("can_send_messages").and_then(Value::as_bool).unwrap_or(false);
    let can_read_history =
        check.get("can_read_message_history").and_then(Value::as_bool).unwrap_or(false);
    let can_embed_links = check.get("can_embed_links").and_then(Value::as_bool).unwrap_or(false);
    let can_attach_files = check.get("can_attach_files").and_then(Value::as_bool).unwrap_or(false);
    let can_send_threads =
        check.get("can_send_messages_in_threads").and_then(Value::as_bool).unwrap_or(false);
    eprintln!(
        "discord.channel_permission_check channel_id={} status={} can_view_channel={} can_send_messages={} can_read_message_history={} can_embed_links={} can_attach_files={} can_send_messages_in_threads={}",
        channel_id,
        status,
        can_view_channel,
        can_send_messages,
        can_read_history,
        can_embed_links,
        can_attach_files,
        can_send_threads
    );
}

/// Prints the egress allowlist and security defaults applied by onboarding
/// to stderr.
pub(crate) fn onboarding_defaults(payload: &Value) {
    let preflight = payload.get("preflight").unwrap_or(payload);
    if let Some(allowlist) = preflight.get("egress_allowlist").and_then(Value::as_array) {
        for host in allowlist {
            if let Some(value) = host.as_str() {
                eprintln!("discord.egress_allowlist {value}");
            }
        }
    }
    if let Some(defaults) = preflight.get("security_defaults").and_then(Value::as_array) {
        for entry in defaults {
            if let Some(value) = entry.as_str() {
                eprintln!("discord.security_default {value}");
            }
        }
    }
}

/// Prints the pinned setup success line to stdout.
pub(crate) fn emit_setup_success(connector_id: &str, response: &Value) {
    let token_vault_ref =
        read_json_string(response, &["applied", "token_vault_ref"]).unwrap_or("unknown");
    let config_path = read_json_string(response, &["applied", "config_path"]).unwrap_or("unknown");
    let restart_required = response
        .get("applied")
        .and_then(Value::as_object)
        .and_then(|applied| applied.get("restart_required_for_routing_rules"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    println!(
        "channels.discord.setup connector_id={} token_vault_ref={} config_path={} restart_required_for_routing_rules={}",
        connector_id,
        token_vault_ref,
        config_path,
        restart_required
    );
}

/// Prints the pinned verify dispatch summary line to stdout.
pub(crate) fn emit_verify_success(connector_id: &str, response: &Value) {
    let delivered = response
        .get("dispatch")
        .and_then(Value::as_object)
        .and_then(|dispatch| dispatch.get("delivered"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let retried = response
        .get("dispatch")
        .and_then(Value::as_object)
        .and_then(|dispatch| dispatch.get("retried"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let dead_lettered = response
        .get("dispatch")
        .and_then(Value::as_object)
        .and_then(|dispatch| dispatch.get("dead_lettered"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    println!(
        "channels.discord.verify connector_id={} delivered={} retried={} dead_lettered={}",
        connector_id, delivered, retried, dead_lettered
    );
}
