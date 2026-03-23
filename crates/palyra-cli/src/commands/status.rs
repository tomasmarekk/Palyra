use crate::*;

pub(crate) fn run_status(
    url: Option<String>,
    grpc_url: Option<String>,
    admin: bool,
    token: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for status command"))?;
    let http_connection = root_context.resolve_http_connection(
        app::ConnectionOverrides { daemon_url: url, token, principal, device_id, channel, grpc_url: None },
        app::ConnectionDefaults::USER,
    )?;
    let grpc_connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides {
            grpc_url,
            ..app::ConnectionOverrides::default()
        },
        app::ConnectionDefaults::USER,
    )?;
    let status_url = format!("{}/healthz", http_connection.base_url.trim_end_matches('/'));
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let health = fetch_health_with_retry(&http_client, &status_url)?;

    let runtime = build_runtime()?;
    let grpc_health = runtime.block_on(fetch_grpc_health_with_retry(grpc_connection.grpc_url.clone()))?;

    let mut admin_payload = None;
    if admin {
        let admin_response = fetch_admin_status(
            &http_client,
            http_connection.base_url.as_str(),
            http_connection.token.clone(),
            http_connection.principal.clone(),
            http_connection.device_id.clone(),
            Some(http_connection.channel.clone()),
            Some(http_connection.trace_id.clone()),
        )?;
        admin_payload = Some(admin_response);
    }

    if root_context.prefers_json() {
        return output::print_json_pretty(
            &json!({
                "http": {
                    "status": health.status,
                    "service": health.service,
                    "version": health.version,
                    "git_hash": health.git_hash,
                    "uptime_seconds": health.uptime_seconds,
                },
                "grpc": {
                    "status": grpc_health.status,
                    "service": grpc_health.service,
                    "version": grpc_health.version,
                    "git_hash": grpc_health.git_hash,
                    "uptime_seconds": grpc_health.uptime_seconds,
                },
                "admin": admin_payload.as_ref().map(|payload| json!({
                    "status": payload.status,
                    "service": payload.service,
                    "transport": {
                        "grpc_bind_addr": payload.transport.grpc_bind_addr,
                        "grpc_port": payload.transport.grpc_port,
                        "quic_enabled": payload.transport.quic_enabled,
                    },
                    "counters": {
                        "denied_requests": payload.counters.denied_requests,
                        "journal_events": payload.counters.journal_events,
                    },
                })),
            }),
            "failed to encode status output as JSON",
        );
    }
    if root_context.prefers_ndjson() {
        output::print_json_line(
            &json!({
                "type": "http",
                "status": health.status,
                "service": health.service,
                "version": health.version,
                "git_hash": health.git_hash,
                "uptime_seconds": health.uptime_seconds,
            }),
            "failed to encode HTTP status as NDJSON",
        )?;
        output::print_json_line(
            &json!({
                "type": "grpc",
                "status": grpc_health.status,
                "service": grpc_health.service,
                "version": grpc_health.version,
                "git_hash": grpc_health.git_hash,
                "uptime_seconds": grpc_health.uptime_seconds,
            }),
            "failed to encode gRPC status as NDJSON",
        )?;
        if let Some(admin_payload) = admin_payload {
            output::print_json_line(
                &json!({
                    "type": "admin",
                    "status": admin_payload.status,
                    "service": admin_payload.service,
                    "grpc_bind_addr": admin_payload.transport.grpc_bind_addr,
                    "grpc_port": admin_payload.transport.grpc_port,
                    "quic_enabled": admin_payload.transport.quic_enabled,
                    "denied_requests": admin_payload.counters.denied_requests,
                    "journal_events": admin_payload.counters.journal_events,
                }),
                "failed to encode admin status as NDJSON",
            )?;
        }
        return std::io::stdout().flush().context("stdout flush failed");
    }

    println!(
        "status.http={} service={} version={} git_hash={} uptime_seconds={}",
        health.status, health.service, health.version, health.git_hash, health.uptime_seconds
    );
    println!(
        "status.grpc={} service={} version={} git_hash={} uptime_seconds={}",
        grpc_health.status,
        grpc_health.service,
        grpc_health.version,
        grpc_health.git_hash,
        grpc_health.uptime_seconds
    );
    if let Some(admin_response) = admin_payload {
        println!(
            "status.admin={} service={} grpc={}:{} quic_enabled={} denied_requests={} journal_events={}",
            admin_response.status,
            admin_response.service,
            admin_response.transport.grpc_bind_addr,
            admin_response.transport.grpc_port,
            admin_response.transport.quic_enabled,
            admin_response.counters.denied_requests,
            admin_response.counters.journal_events
        );
    }

    std::io::stdout().flush().context("stdout flush failed")
}
