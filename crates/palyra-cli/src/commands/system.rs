//! `palyra system`: operator views over the daemon's heartbeat, subsystem
//! presence, insights, and the append-only system event feed.
//! All data comes from the `console/v1/system` admin endpoints; `event emit`
//! validates event names locally before posting.

use crate::*;

#[derive(Debug, Serialize)]
struct SystemPresenceEntry {
    subsystem: String,
    state: String,
    detail: String,
}

/// Runs a `palyra system` subcommand on a dedicated Tokio runtime.
///
/// # Errors
/// Returns an error when the admin console connection fails, the console API
/// call fails, or an emitted event name is invalid.
pub(crate) fn run_system(command: SystemCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_system_async(command))
}

async fn run_system_async(command: SystemCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;

    match command {
        SystemCommand::Heartbeat { json } => {
            let payload = context.client.get_json_value("console/v1/system/heartbeat").await?;
            emit_system_heartbeat(&payload, output::preferred_json(json))
        }
        SystemCommand::Presence { json } => {
            let payload = context.client.get_json_value("console/v1/system/presence").await?;
            emit_system_presence(&payload, output::preferred_json(json))
        }
        SystemCommand::Insights { json } => {
            let payload = context.client.get_json_value("console/v1/system/insights").await?;
            emit_system_insights(&payload, output::preferred_json(json))
        }
        SystemCommand::Event { command } => match command
            .unwrap_or(SystemEventCommand::List { limit: None, json: false })
        {
            SystemEventCommand::List { limit, json } => {
                let payload =
                    context.client.get_json_value(build_system_event_list_path(limit)).await?;
                emit_system_events(&payload, output::preferred_json(json))
            }
            SystemEventCommand::Emit { event, message, severity, tag, json } => {
                validate_system_event_name(event.as_str())?;
                let summary =
                    message.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty());
                let payload = context
                    .client
                    .post_json_value(
                        "console/v1/system/events/emit",
                        &json!({
                            "name": event,
                            "summary": summary,
                            "details": {
                                "severity": system_event_severity_label(severity),
                                "tags": tag,
                            },
                        }),
                    )
                    .await?;
                emit_system_event_emit(&payload, output::preferred_json(json))
            }
        },
    }
}

fn build_system_event_list_path(limit: Option<usize>) -> String {
    match limit {
        Some(limit) => format!("console/v1/system/events?limit={}", limit.clamp(1, 2_000)),
        None => "console/v1/system/events".to_owned(),
    }
}

fn emit_system_events(payload: &Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(payload, "failed to encode system event payload as JSON");
    }

    let total_events = payload.get("total_events").and_then(Value::as_u64).unwrap_or(0);
    let hash_chain_enabled =
        payload.get("hash_chain_enabled").and_then(Value::as_bool).unwrap_or(true);
    let events = payload.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
    println!(
        "system.event total_events={} hash_chain_enabled={} returned_events={}",
        total_events,
        hash_chain_enabled,
        events.len()
    );
    for event in events {
        println!(
            "system.event.entry event_id={} kind={} actor={} redacted={} timestamp_unix_ms={} principal={} channel={} hash_present={}",
            event.get("event_id").and_then(Value::as_str).unwrap_or("unknown"),
            event
                .get("kind_label")
                .and_then(Value::as_str)
                .or_else(|| event.get("kind").and_then(Value::as_i64).map(|_| "unknown"))
                .unwrap_or("unknown"),
            event
                .get("actor_label")
                .and_then(Value::as_str)
                .or_else(|| event.get("actor").and_then(Value::as_i64).map(|_| "unknown"))
                .unwrap_or("unknown"),
            event.get("redacted").and_then(Value::as_bool).unwrap_or(false),
            event.get("timestamp_unix_ms").and_then(Value::as_i64).unwrap_or_default(),
            event.get("principal").and_then(Value::as_str).unwrap_or("unknown"),
            event.get("channel").and_then(Value::as_str).unwrap_or("none"),
            event.get("hash").and_then(Value::as_str).is_some()
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_system_event_emit(payload: &Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(
            payload,
            "failed to encode system event emit payload as JSON",
        );
    }

    let details = payload.get("details").unwrap_or(payload);
    println!(
        "system.event.emit status={} event={} recorded_at_unix_ms={} severity={} tags={}",
        payload.get("status").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("event").and_then(Value::as_str).unwrap_or("unknown"),
        details.get("emitted_at_unix_ms").and_then(Value::as_i64).unwrap_or_default(),
        details.pointer("/details/severity").and_then(Value::as_str).unwrap_or("unknown"),
        join_json_string_list(details.pointer("/details/tags"))
    );
    if let Some(summary) = details.get("summary").and_then(Value::as_str) {
        println!("system.event.emit.summary={summary}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn join_json_string_list(value: Option<&Value>) -> String {
    let values = value
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .filter(|entry| !entry.trim().is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(",")
    }
}

fn emit_system_heartbeat(payload: &Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(payload, "failed to encode system heartbeat as JSON");
    }

    let generated_at_unix_ms =
        payload.get("generated_at_unix_ms").and_then(Value::as_i64).unwrap_or_default();
    let transport = payload.get("transport").unwrap_or(&Value::Null);
    let deployment = payload.get("deployment").unwrap_or(&Value::Null);
    let counters = payload.get("counters").unwrap_or(&Value::Null);
    let security = payload.get("security").unwrap_or(&Value::Null);
    let grpc_transport = format!(
        "{}:{}",
        transport.get("grpc_bind_addr").and_then(Value::as_str).unwrap_or("unknown"),
        transport.get("grpc_port").and_then(Value::as_u64).unwrap_or(0)
    );
    let quic_transport = format!(
        "{}:{}",
        transport.get("quic_bind_addr").and_then(Value::as_str).unwrap_or("unknown"),
        transport.get("quic_port").and_then(Value::as_u64).unwrap_or(0)
    );
    println!(
        "system.heartbeat status={} generated_at_unix_ms={} service={} version={} git_hash={} uptime_seconds={}",
        payload.get("status").and_then(Value::as_str).unwrap_or("unknown"),
        generated_at_unix_ms,
        payload.get("service").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("version").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("git_hash").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("uptime_seconds").and_then(Value::as_u64).unwrap_or(0)
    );
    println!(
        "system.heartbeat.transport grpc={} quic={} quic_enabled={}",
        grpc_transport,
        quic_transport,
        transport.get("quic_enabled").and_then(Value::as_bool).unwrap_or(false)
    );
    println!(
        "system.heartbeat.security deny_by_default={} admin_auth_required={} admin_token_configured={} denied_requests={} journal_events={}",
        security.get("deny_by_default").and_then(Value::as_bool).unwrap_or(false),
        security
            .get("admin_auth_required")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        security
            .get("admin_token_configured")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        counters.get("denied_requests").and_then(Value::as_u64).unwrap_or(0),
        counters.get("journal_events").and_then(Value::as_u64).unwrap_or(0)
    );
    println!(
        "system.heartbeat.deployment mode={} bind_profile={} remote_bind_detected={}",
        deployment.get("mode").and_then(Value::as_str).unwrap_or("unknown"),
        deployment.get("bind_profile").and_then(Value::as_str).unwrap_or("unknown"),
        deployment.get("remote_bind_detected").and_then(Value::as_bool).unwrap_or(false)
    );
    for warning in deployment
        .get("warnings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        println!("system.heartbeat.warning={warning}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_system_presence(payload: &Value, json_output: bool) -> Result<()> {
    let generated_at_unix_ms =
        payload.get("generated_at_unix_ms").and_then(Value::as_i64).unwrap_or_default();
    let subsystems_payload = payload.pointer("/subsystems").unwrap_or(&Value::Null);
    let subsystems = vec![
        build_system_presence_entry("gateway", subsystems_payload.get("gateway")),
        build_system_presence_entry("model_provider", subsystems_payload.get("model_provider")),
        build_system_presence_entry("auth_profiles", subsystems_payload.get("auth_profiles")),
        build_system_presence_entry("browserd", subsystems_payload.get("browserd")),
        build_system_presence_entry("channels", subsystems_payload.get("channels")),
        build_system_presence_entry("memory", subsystems_payload.get("memory")),
        build_system_presence_entry("support_bundle", subsystems_payload.get("support_bundle")),
    ];

    if json_output {
        return output::print_json_pretty(
            &json!({
                "generated_at_unix_ms": generated_at_unix_ms,
                "subsystems": subsystems,
            }),
            "failed to encode system presence as JSON",
        );
    }

    println!(
        "system.presence generated_at_unix_ms={} subsystems={} degraded={}",
        generated_at_unix_ms,
        subsystems.len(),
        subsystems.iter().filter(|entry| entry.state == "degraded").count()
    );
    for entry in subsystems {
        println!(
            "system.presence.entry subsystem={} state={} detail={}",
            entry.subsystem, entry.state, entry.detail
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_system_insights(payload: &Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(payload, "failed to encode system insights as JSON");
    }

    let summary = payload.get("summary").unwrap_or(&Value::Null);
    let hotspots = payload.get("hotspots").and_then(Value::as_array).cloned().unwrap_or_default();
    let provider = payload.get("provider_health").unwrap_or(&Value::Null);
    let recall = payload.get("recall").unwrap_or(&Value::Null);
    let compaction = payload.get("compaction").unwrap_or(&Value::Null);
    let safety = payload.get("safety_boundary").unwrap_or(&Value::Null);
    let plugins = payload.get("plugins").unwrap_or(&Value::Null);
    let cron = payload.get("cron").unwrap_or(&Value::Null);
    let reload = payload.get("reload").unwrap_or(&Value::Null);
    println!(
        "system.insights state={} severity={} generated_at_unix_ms={} hotspots={} blocking={} warnings={}",
        summary.get("state").and_then(Value::as_str).unwrap_or("unknown"),
        summary.get("severity").and_then(Value::as_str).unwrap_or("unknown"),
        payload
            .get("generated_at_unix_ms")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        summary.get("hotspot_count").and_then(Value::as_u64).unwrap_or(0),
        summary.get("blocking_hotspots").and_then(Value::as_u64).unwrap_or(0),
        summary.get("warning_hotspots").and_then(Value::as_u64).unwrap_or(0)
    );
    if let Some(recommendation) = summary.get("recommendation").and_then(Value::as_str) {
        println!("system.insights.recommendation={recommendation}");
    }
    println!(
        "system.insights.provider state={} auth_state={} error_rate_bps={} avg_latency_ms={} circuit_open={} cache_hit_rate_bps={}",
        provider.get("state").and_then(Value::as_str).unwrap_or("unknown"),
        provider.get("auth_state").and_then(Value::as_str).unwrap_or("unknown"),
        provider.get("error_rate_bps").and_then(Value::as_u64).unwrap_or(0),
        provider.get("avg_latency_ms").and_then(Value::as_u64).unwrap_or(0),
        provider.get("circuit_open").and_then(Value::as_bool).unwrap_or(false),
        provider
            .get("response_cache_hit_rate_bps")
            .and_then(Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "system.insights.recall state={} zero_hit_rate_bps={} auto_inject_avg_hits={:.2}",
        recall.get("state").and_then(Value::as_str).unwrap_or("unknown"),
        recall.get("explicit_recall_zero_hit_rate_bps").and_then(Value::as_u64).unwrap_or(0),
        recall.get("auto_inject_avg_hits").and_then(Value::as_f64).unwrap_or(0.0)
    );
    println!(
        "system.insights.compaction state={} avg_reduction_bps={} avg_token_delta={}",
        compaction.get("state").and_then(Value::as_str).unwrap_or("unknown"),
        compaction.get("avg_reduction_bps").and_then(Value::as_u64).unwrap_or(0),
        compaction.get("avg_token_delta").and_then(Value::as_i64).unwrap_or_default()
    );
    println!(
        "system.insights.safety state={} deny_rate_bps={} approvals={} denies={}",
        safety.get("state").and_then(Value::as_str).unwrap_or("unknown"),
        safety.get("deny_rate_bps").and_then(Value::as_u64).unwrap_or(0),
        safety.get("approval_required_decisions").and_then(Value::as_u64).unwrap_or(0),
        safety.get("policy_enforced_denies").and_then(Value::as_u64).unwrap_or(0)
    );
    println!(
        "system.insights.plugins state={} unhealthy_bindings={} typed_contract_failures={} config_failures={} discovery_failures={}",
        plugins.get("state").and_then(Value::as_str).unwrap_or("unknown"),
        plugins
            .get("unhealthy_bindings")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        plugins
            .get("typed_contract_failures")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        plugins
            .get("config_failures")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        plugins
            .get("discovery_failures")
            .and_then(Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "system.insights.cron state={} success_rate_bps={} failed_runs={} tool_denies={}",
        cron.get("state").and_then(Value::as_str).unwrap_or("unknown"),
        cron.get("success_rate_bps").and_then(Value::as_u64).unwrap_or(0),
        cron.get("failed_runs").and_then(Value::as_u64).unwrap_or(0),
        cron.get("total_tool_denies").and_then(Value::as_u64).unwrap_or(0)
    );
    println!(
        "system.insights.reload state={} blocking_refs={} warning_refs={}",
        reload.get("state").and_then(Value::as_str).unwrap_or("unknown"),
        reload.get("blocking_refs").and_then(Value::as_u64).unwrap_or(0),
        reload.get("warning_refs").and_then(Value::as_u64).unwrap_or(0)
    );
    for hotspot in hotspots.iter().take(5) {
        println!(
            "system.insights.hotspot hotspot_id={} subsystem={} severity={} state={} summary={} action={}",
            hotspot.get("hotspot_id").and_then(Value::as_str).unwrap_or("unknown"),
            hotspot.get("subsystem").and_then(Value::as_str).unwrap_or("unknown"),
            hotspot.get("severity").and_then(Value::as_str).unwrap_or("unknown"),
            hotspot.get("state").and_then(Value::as_str).unwrap_or("unknown"),
            hotspot.get("summary").and_then(Value::as_str).unwrap_or("none"),
            hotspot
                .get("recommended_action")
                .and_then(Value::as_str)
                .unwrap_or("none")
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_system_presence_entry(subsystem: &str, payload: Option<&Value>) -> SystemPresenceEntry {
    let payload = payload.unwrap_or(&Value::Null);
    let state = payload.get("state").and_then(Value::as_str).unwrap_or("unknown").to_owned();
    let detail = match subsystem {
        "gateway" => format!(
            "service={} uptime_seconds={} grpc={}:{}",
            payload.get("service").and_then(Value::as_str).unwrap_or("unknown"),
            payload.get("uptime_seconds").and_then(Value::as_u64).unwrap_or(0),
            payload
                .pointer("/transport/grpc_bind_addr")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            payload.pointer("/transport/grpc_port").and_then(Value::as_u64).unwrap_or(0)
        ),
        "model_provider" => format!(
            "kind={} auth_profile_id={} error_count={}",
            payload.get("kind").and_then(Value::as_str).unwrap_or("unknown"),
            payload.get("auth_profile_id").and_then(Value::as_str).unwrap_or("none"),
            payload.pointer("/runtime_metrics/error_count").and_then(Value::as_u64).unwrap_or(0)
        ),
        "auth_profiles" => format!(
            "total={} missing={} expired={}",
            payload.pointer("/summary/total").and_then(Value::as_u64).unwrap_or(0),
            payload.pointer("/summary/missing").and_then(Value::as_u64).unwrap_or(0),
            payload.pointer("/summary/expired").and_then(Value::as_u64).unwrap_or(0)
        ),
        "browserd" => format!(
            "enabled={} active_sessions={} health_status={}",
            payload.pointer("/status/enabled").and_then(Value::as_bool).unwrap_or(false),
            payload.pointer("/status/sessions/active").and_then(Value::as_u64).unwrap_or(0),
            payload.pointer("/status/health/status").and_then(Value::as_str).unwrap_or("unknown")
        ),
        "channels" => format!(
            "degraded_connectors={} queue_depth={} dead_letters={}",
            payload.pointer("/status/degraded_connectors").and_then(Value::as_u64).unwrap_or(0),
            payload.pointer("/status/queue_depth").and_then(Value::as_u64).unwrap_or(0),
            payload.pointer("/status/dead_letters").and_then(Value::as_u64).unwrap_or(0)
        ),
        "memory" => format!(
            "entries={} approx_bytes={} next_run_at_unix_ms={}",
            payload.pointer("/usage/entries").and_then(Value::as_u64).unwrap_or(0),
            payload.pointer("/usage/approx_bytes").and_then(Value::as_u64).unwrap_or(0),
            payload
                .pointer("/maintenance/next_run_at_unix_ms")
                .and_then(Value::as_i64)
                .unwrap_or_default()
        ),
        "support_bundle" => format!(
            "failures={} attempts={} successes={}",
            payload.pointer("/status/failures").and_then(Value::as_u64).unwrap_or(0),
            payload.pointer("/status/attempts").and_then(Value::as_u64).unwrap_or(0),
            payload.pointer("/status/successes").and_then(Value::as_u64).unwrap_or(0)
        ),
        _ => "unknown".to_owned(),
    };
    SystemPresenceEntry { subsystem: subsystem.to_owned(), state, detail }
}

// Event names land in the hash-chained journal, so the strict ASCII allowlist
// keeps operator-supplied names from injecting separators or control bytes
// into downstream log lines and queries.
fn validate_system_event_name(event: &str) -> Result<()> {
    let trimmed = event.trim();
    if trimmed.is_empty() {
        anyhow::bail!("system event name cannot be empty");
    }
    if trimmed.len() > 96 {
        anyhow::bail!("system event name must be 96 characters or fewer");
    }
    if !trimmed.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')) {
        anyhow::bail!("system event name may contain only ASCII letters, digits, '.', '_' and '-'");
    }
    Ok(())
}

fn system_event_severity_label(severity: SystemEventSeverityArg) -> &'static str {
    match severity {
        SystemEventSeverityArg::Info => "info",
        SystemEventSeverityArg::Warn => "warn",
        SystemEventSeverityArg::Error => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_system_event_list_path, system_event_severity_label, validate_system_event_name,
    };
    use crate::SystemEventSeverityArg;

    #[test]
    fn system_event_name_validation_rejects_invalid_values() {
        assert!(validate_system_event_name("operator.heartbeat").is_ok());
        assert!(validate_system_event_name("operator heartbeat").is_err());
        assert!(validate_system_event_name("../escape").is_err());
    }

    #[test]
    fn event_list_path_clamps_limit() {
        assert_eq!(build_system_event_list_path(None), "console/v1/system/events");
        assert_eq!(
            build_system_event_list_path(Some(4_000)),
            "console/v1/system/events?limit=2000"
        );
    }

    #[test]
    fn severity_labels_match_contract() {
        assert_eq!(system_event_severity_label(SystemEventSeverityArg::Info), "info");
        assert_eq!(system_event_severity_label(SystemEventSeverityArg::Warn), "warn");
        assert_eq!(system_event_severity_label(SystemEventSeverityArg::Error), "error");
    }
}
