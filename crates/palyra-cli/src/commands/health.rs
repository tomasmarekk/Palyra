//! Daemon health probe over both transports.
//!
//! Probes the HTTP `/healthz` endpoint and the gateway gRPC health service
//! so transport-specific outages are distinguishable; JSON mode reports
//! partial failures as a structured unavailable payload on stderr.

use crate::*;

/// Runs `palyra health`, probing HTTP and gRPC health and emitting the
/// combined result.
///
/// # Errors
/// Fails when connection resolution fails, a probe fails (text mode), or
/// output encoding fails. In JSON mode probe failures exit through a
/// classified already-emitted error after printing the payload.
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
    let runtime = build_runtime()?;
    let http = fetch_health_with_retry(&http_client, &status_url);
    let grpc = runtime.block_on(fetch_grpc_health_with_retry(grpc_connection.grpc_url.clone()));

    if output::preferred_json(json) {
        let (Ok(http), Ok(grpc)) = (&http, &grpc) else {
            return emit_unavailable_health_json(
                http_connection.base_url.as_str(),
                grpc_connection.grpc_url.as_str(),
                &http,
                &grpc,
            );
        };
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
    let http = http?;
    let grpc = grpc?;
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

// Prints the structured unavailable payload to stderr, then returns an
// already-emitted error so main sets the classified exit code without
// printing the failure a second time.
fn emit_unavailable_health_json(
    daemon_url: &str,
    grpc_url: &str,
    http: &Result<HealthResponse>,
    grpc: &Result<gateway_v1::HealthResponse>,
) -> Result<()> {
    let (exit_code, payload) = unavailable_health_json_payload(
        daemon_url,
        grpc_url,
        http,
        grpc,
        app::current_root_context().as_ref().map(|value| value.trace_id()),
    );

    eprintln!(
        "{}",
        serde_json::to_string_pretty(&payload)
            .context("failed to encode unavailable health output as JSON")?
    );
    Err(output::already_emitted_error(exit_code))
}

fn unavailable_health_json_payload(
    daemon_url: &str,
    grpc_url: &str,
    http: &Result<HealthResponse>,
    grpc: &Result<gateway_v1::HealthResponse>,
    trace_id: Option<&str>,
) -> (output::CliExitCode, Value) {
    let primary_error = http.as_ref().err().or_else(|| grpc.as_ref().err());
    let exit_code =
        primary_error.map(output::classify_error).unwrap_or(output::CliExitCode::Connectivity);
    let message = [http.as_ref().err(), grpc.as_ref().err()]
        .into_iter()
        .flatten()
        .map(format_health_error_for_json)
        .collect::<Vec<_>>()
        .join("; ");
    let message =
        if message.trim().is_empty() { "health check failed".to_owned() } else { message };
    let payload = json!({
        "status": "error",
        "overall": "unavailable",
        "daemon_url": output::sanitize_diagnostic_text(daemon_url),
        "grpc_url": output::sanitize_diagnostic_text(grpc_url),
        "http": health_probe_result_to_json(http),
        "grpc": grpc_health_probe_result_to_json(grpc),
        "error": {
            "kind": exit_code.kind(),
            "message": message,
            "trace_id": trace_id,
        },
    });

    (exit_code, payload)
}

fn format_health_error_for_json(error: &anyhow::Error) -> String {
    output::sanitize_diagnostic_text(format!("{error:#}").as_str())
}

fn health_probe_result_to_json(result: &Result<HealthResponse>) -> Value {
    match result {
        Ok(response) => json!({
            "status": response.status,
            "service": response.service,
            "version": response.version,
            "git_hash": response.git_hash,
            "uptime_seconds": response.uptime_seconds,
        }),
        Err(error) => json!({
            "status": "error",
            "error": {
                "kind": output::classify_error(error).kind(),
                "message": format_health_error_for_json(error),
            },
        }),
    }
}

fn grpc_health_probe_result_to_json(result: &Result<gateway_v1::HealthResponse>) -> Value {
    match result {
        Ok(response) => json!({
            "status": response.status,
            "service": response.service,
            "version": response.version,
            "git_hash": response.git_hash,
            "uptime_seconds": response.uptime_seconds,
        }),
        Err(error) => json!({
            "status": "error",
            "error": {
                "kind": output::classify_error(error).kind(),
                "message": format_health_error_for_json(error),
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_health_json_redacts_urls_and_probe_errors() {
        let http: Result<HealthResponse> = Err(anyhow!(
            "failed to call http://user:HTTP_PASS_123@example.test/healthz?api_key=HTTP_TOKEN_456"
        ));
        let grpc: Result<gateway_v1::HealthResponse> = Err(anyhow!(
            "failed to connect gateway gRPC endpoint http://user:GRPC_PASS_ABC@example.test:7443?token=GRPC_TOKEN_XYZ"
        ));

        let (_exit_code, payload) = unavailable_health_json_payload(
            "http://user:HTTP_PASS_123@example.test?api_key=HTTP_TOKEN_456",
            "http://user:GRPC_PASS_ABC@example.test:7443?token=GRPC_TOKEN_XYZ",
            &http,
            &grpc,
            Some("trace-123"),
        );
        let rendered = serde_json::to_string(&payload).expect("health JSON payload encodes");

        for secret in ["HTTP_PASS_123", "HTTP_TOKEN_456", "GRPC_PASS_ABC", "GRPC_TOKEN_XYZ"] {
            assert!(!rendered.contains(secret));
        }
        assert!(rendered.contains("<redacted>"));
        assert_eq!(payload.pointer("/error/trace_id").and_then(Value::as_str), Some("trace-123"));
    }
}
