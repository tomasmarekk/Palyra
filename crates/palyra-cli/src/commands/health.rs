use crate::*;

pub(crate) fn run_health(url: Option<String>, grpc_url: Option<String>, json: bool) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for health command"))?;
    let http_connection = root_context.resolve_http_connection(
        app::ConnectionOverrides { daemon_url: url, ..app::ConnectionOverrides::default() },
        app::ConnectionDefaults::USER,
    )?;
    let grpc_connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides { grpc_url, ..app::ConnectionOverrides::default() },
        app::ConnectionDefaults::USER,
    )?;

    let status_url = format!("{}/healthz", http_connection.base_url.trim_end_matches('/'));
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let http = fetch_health_with_retry(&http_client, &status_url)?;
    let runtime = build_runtime()?;
    let grpc = runtime.block_on(fetch_grpc_health_with_retry(grpc_connection.grpc_url.clone()))?;

    if output::preferred_json(json) {
        return output::print_json_pretty(
            &json!({
                "overall": "ok",
                "daemon_url": http_connection.base_url,
                "grpc_url": grpc_connection.grpc_url,
                "http": {
                    "status": http.status,
                    "service": http.service,
                    "version": http.version,
                    "git_hash": http.git_hash,
                    "uptime_seconds": http.uptime_seconds,
                },
                "grpc": {
                    "status": grpc.status,
                    "service": grpc.service,
                    "version": grpc.version,
                    "git_hash": grpc.git_hash,
                    "uptime_seconds": grpc.uptime_seconds,
                },
            }),
            "failed to encode health output as JSON",
        );
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(
            &json!({
                "type": "health",
                "overall": "ok",
                "daemon_url": http_connection.base_url,
                "grpc_url": grpc_connection.grpc_url,
            }),
            "failed to encode health summary as NDJSON",
        )?;
        output::print_json_line(
            &json!({
                "type": "health.http",
                "status": http.status,
                "service": http.service,
                "version": http.version,
                "git_hash": http.git_hash,
                "uptime_seconds": http.uptime_seconds,
            }),
            "failed to encode HTTP health as NDJSON",
        )?;
        output::print_json_line(
            &json!({
                "type": "health.grpc",
                "status": grpc.status,
                "service": grpc.service,
                "version": grpc.version,
                "git_hash": grpc.git_hash,
                "uptime_seconds": grpc.uptime_seconds,
            }),
            "failed to encode gRPC health as NDJSON",
        )?;
        return std::io::stdout().flush().context("stdout flush failed");
    }

    println!(
        "health.overall=ok daemon_url={} grpc_url={}",
        http_connection.base_url, grpc_connection.grpc_url
    );
    println!(
        "health.http={} service={} version={} git_hash={} uptime_seconds={}",
        http.status, http.service, http.version, http.git_hash, http.uptime_seconds
    );
    println!(
        "health.grpc={} service={} version={} git_hash={} uptime_seconds={}",
        grpc.status, grpc.service, grpc.version, grpc.git_hash, grpc.uptime_seconds
    );
    std::io::stdout().flush().context("stdout flush failed")
}
