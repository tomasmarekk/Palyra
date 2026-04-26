use crate::*;
use palyra_control_plane as control_plane;

#[derive(Debug, Serialize)]
struct StatusReport {
    overall: String,
    gateway: StatusGatewaySnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    service: Option<support::service::GatewayServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deployment: Option<control_plane::DeploymentPostureSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<StatusRuntimeSnapshot>,
    hints: Vec<String>,
}

#[derive(Debug, Serialize)]
struct StatusGatewaySnapshot {
    daemon_url: String,
    grpc_url: String,
    http: StatusHealthSurface,
    grpc: StatusHealthSurface,
    #[serde(skip_serializing_if = "Option::is_none")]
    admin: Option<Value>,
}

#[derive(Debug, Serialize)]
struct StatusHealthSurface {
    status: String,
    service: String,
    version: String,
    git_hash: String,
    uptime_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct StatusRuntimeSnapshot {
    deployment_mode: Option<String>,
    bind_profile: Option<String>,
    remote_bind_detected: Option<bool>,
    auth_state: Option<String>,
    browser_state: Option<String>,
    browser_sessions: Option<u64>,
    connector_degraded: Option<u64>,
    connector_queue_depth: Option<u64>,
    memory_entries: Option<u64>,
    memory_bytes: Option<u64>,
    support_bundle_failures: Option<u64>,
    self_healing_active_incidents: Option<u64>,
    self_healing_resolved_incidents: Option<u64>,
    self_healing_heartbeat_count: Option<u64>,
    diagnostics_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostics_error: Option<String>,
}

pub(crate) struct StatusCommandArgs {
    pub(crate) url: Option<String>,
    pub(crate) grpc_url: Option<String>,
    pub(crate) admin: bool,
    pub(crate) token: Option<String>,
    pub(crate) principal: Option<String>,
    pub(crate) device_id: Option<String>,
    pub(crate) channel: Option<String>,
    pub(crate) json: bool,
}

pub(crate) fn run_status(args: StatusCommandArgs) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for status command"))?;
    let overrides = app::ConnectionOverrides {
        daemon_url: args.url.clone(),
        grpc_url: args.grpc_url.clone(),
        token: args.token.clone(),
        principal: args.principal.clone(),
        device_id: args.device_id.clone(),
        channel: args.channel.clone(),
    };
    let report = build_status_report(root_context, overrides, args.admin)?;

    if output::preferred_json(args.json) {
        return output::print_json_pretty(&report, "failed to encode status output as JSON");
    }
    if output::preferred_ndjson(args.json, false) {
        output::print_json_line(
            &json!({
                "type": "status.gateway.http",
                "status": report.gateway.http.status,
                "service": report.gateway.http.service,
                "version": report.gateway.http.version,
                "git_hash": report.gateway.http.git_hash,
                "uptime_seconds": report.gateway.http.uptime_seconds,
                "error": report.gateway.http.error,
            }),
            "failed to encode HTTP status as NDJSON",
        )?;
        output::print_json_line(
            &json!({
                "type": "status.gateway.grpc",
                "status": report.gateway.grpc.status,
                "service": report.gateway.grpc.service,
                "version": report.gateway.grpc.version,
                "git_hash": report.gateway.grpc.git_hash,
                "uptime_seconds": report.gateway.grpc.uptime_seconds,
                "error": report.gateway.grpc.error,
            }),
            "failed to encode gRPC status as NDJSON",
        )?;
        if let Some(service) = report.service.as_ref() {
            output::print_json_line(
                &json!({
                    "type": "status.service",
                    "installed": service.installed,
                    "running": service.running,
                    "enabled": service.enabled,
                    "manager": service.manager,
                    "service_name": service.service_name,
                    "detail": service.detail,
                }),
                "failed to encode service status as NDJSON",
            )?;
        }
        if let Some(runtime) = report.runtime.as_ref() {
            output::print_json_line(
                &json!({
                    "type": "status.runtime",
                    "deployment_mode": runtime.deployment_mode,
                    "bind_profile": runtime.bind_profile,
                    "remote_bind_detected": runtime.remote_bind_detected,
                    "auth_state": runtime.auth_state,
                    "browser_state": runtime.browser_state,
                    "browser_sessions": runtime.browser_sessions,
                    "connector_degraded": runtime.connector_degraded,
                    "connector_queue_depth": runtime.connector_queue_depth,
                    "memory_entries": runtime.memory_entries,
                    "memory_bytes": runtime.memory_bytes,
                    "support_bundle_failures": runtime.support_bundle_failures,
                    "diagnostics_available": runtime.diagnostics_available,
                    "diagnostics_error": runtime.diagnostics_error,
                }),
                "failed to encode runtime status as NDJSON",
            )?;
        }
        for hint in report.hints.as_slice() {
            output::print_json_line(
                &json!({
                    "type": "status.hint",
                    "message": hint,
                }),
                "failed to encode status hint as NDJSON",
            )?;
        }
        return std::io::stdout().flush().context("stdout flush failed");
    }

    println!(
        "status.overall={} daemon_url={} grpc_url={}",
        report.overall, report.gateway.daemon_url, report.gateway.grpc_url
    );
    println!("status.http={}", report.gateway.http.status);
    println!(
        "status.gateway.http={} service={} version={} git_hash={} uptime_seconds={}",
        report.gateway.http.status,
        report.gateway.http.service,
        report.gateway.http.version,
        report.gateway.http.git_hash,
        report.gateway.http.uptime_seconds
    );
    if let Some(error) = report.gateway.http.error.as_deref() {
        println!("status.gateway.http_error={error}");
    }
    println!("status.grpc={}", report.gateway.grpc.status);
    println!(
        "status.gateway.grpc={} service={} version={} git_hash={} uptime_seconds={}",
        report.gateway.grpc.status,
        report.gateway.grpc.service,
        report.gateway.grpc.version,
        report.gateway.grpc.git_hash,
        report.gateway.grpc.uptime_seconds
    );
    if let Some(error) = report.gateway.grpc.error.as_deref() {
        println!("status.gateway.grpc_error={error}");
    }
    if let Some(admin_payload) = report.gateway.admin.as_ref() {
        println!(
            "status.admin={}",
            admin_payload.get("status").and_then(Value::as_str).unwrap_or("unknown")
        );
        println!(
            "status.gateway.admin={} service={} journal_events={} denied_requests={}",
            admin_payload.get("status").and_then(Value::as_str).unwrap_or("unknown"),
            admin_payload.get("service").and_then(Value::as_str).unwrap_or("unknown"),
            admin_payload.pointer("/counters/journal_events").and_then(Value::as_u64).unwrap_or(0),
            admin_payload.pointer("/counters/denied_requests").and_then(Value::as_u64).unwrap_or(0)
        );
    }
    if let Some(service) = report.service.as_ref() {
        println!(
            "status.service installed={} running={} enabled={} manager={} service_name={}",
            service.installed,
            service.running,
            service.enabled,
            service.manager,
            service.service_name
        );
        if let Some(detail) = service.detail.as_deref() {
            println!("status.service.detail={detail}");
        }
    }
    if let Some(runtime) = report.runtime.as_ref() {
        println!(
            "status.runtime deployment_mode={} bind_profile={} remote_bind_detected={} auth_state={} browser_state={} browser_sessions={} connector_degraded={} connector_queue_depth={} memory_entries={} memory_bytes={} support_bundle_failures={} self_healing_active_incidents={} self_healing_resolved_incidents={} self_healing_heartbeats={} diagnostics_available={}",
            runtime.deployment_mode.as_deref().unwrap_or("unknown"),
            runtime.bind_profile.as_deref().unwrap_or("unknown"),
            runtime.remote_bind_detected.unwrap_or(false),
            runtime.auth_state.as_deref().unwrap_or("unknown"),
            runtime.browser_state.as_deref().unwrap_or("unknown"),
            runtime.browser_sessions.unwrap_or(0),
            runtime.connector_degraded.unwrap_or(0),
            runtime.connector_queue_depth.unwrap_or(0),
            runtime.memory_entries.unwrap_or(0),
            runtime.memory_bytes.unwrap_or(0),
            runtime.support_bundle_failures.unwrap_or(0),
            runtime.self_healing_active_incidents.unwrap_or(0),
            runtime.self_healing_resolved_incidents.unwrap_or(0),
            runtime.self_healing_heartbeat_count.unwrap_or(0),
            runtime.diagnostics_available,
        );
        if let Some(error) = runtime.diagnostics_error.as_deref() {
            println!("status.runtime.error={error}");
        }
    }
    if let Some(deployment) = report.deployment.as_ref() {
        println!(
            "status.deployment mode={} bind_profile={} remote_bind_detected={} gateway_tls_enabled={} admin_auth_required={}",
            deployment.mode,
            deployment.bind_profile,
            deployment.remote_bind_detected,
            deployment.tls.gateway_enabled,
            deployment.admin_auth_required
        );
        for warning in deployment.warnings.as_slice() {
            println!("status.deployment.warning={warning}");
        }
    }
    for hint in report.hints.as_slice() {
        println!("status.hint={hint}");
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn build_status_report(
    root_context: app::RootCommandContext,
    overrides: app::ConnectionOverrides,
    force_admin: bool,
) -> Result<StatusReport> {
    let http_connection = root_context.resolve_http_connection(
        app::ConnectionOverrides {
            daemon_url: overrides.daemon_url.clone(),
            token: overrides.token.clone(),
            principal: overrides.principal.clone(),
            device_id: overrides.device_id.clone(),
            channel: overrides.channel.clone(),
            grpc_url: None,
        },
        app::ConnectionDefaults::USER,
    )?;
    let grpc_connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides {
            grpc_url: overrides.grpc_url.clone(),
            ..app::ConnectionOverrides::default()
        },
        app::ConnectionDefaults::USER,
    )?;
    let status_url = format!("{}/healthz", http_connection.base_url.trim_end_matches('/'));
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let runtime = build_runtime()?;

    let http = match fetch_health_with_retry(&http_client, &status_url) {
        Ok(value) => StatusHealthSurface {
            status: value.status,
            service: value.service,
            version: value.version,
            git_hash: value.git_hash,
            uptime_seconds: value.uptime_seconds,
            error: None,
        },
        Err(error) => unavailable_health_surface(error.to_string().as_str()),
    };
    let grpc =
        match runtime.block_on(fetch_grpc_health_with_retry(grpc_connection.grpc_url.clone())) {
            Ok(value) => StatusHealthSurface {
                status: value.status,
                service: value.service,
                version: value.version,
                git_hash: value.git_hash,
                uptime_seconds: value.uptime_seconds,
                error: None,
            },
            Err(error) => unavailable_health_surface(error.to_string().as_str()),
        };

    let admin_connection =
        root_context.resolve_http_connection(overrides.clone(), app::ConnectionDefaults::ADMIN)?;
    let should_attempt_admin = force_admin || admin_connection.token.is_some();
    let admin_payload = if should_attempt_admin {
        match fetch_admin_status_payload(
            &http_client,
            admin_connection.base_url.as_str(),
            admin_connection.token.clone(),
            admin_connection.principal.clone(),
            admin_connection.device_id.clone(),
            Some(admin_connection.channel.clone()),
            Some(admin_connection.trace_id.clone()),
        ) {
            Ok(payload) => Some(payload),
            Err(error) => {
                if force_admin {
                    return Err(error);
                }
                None
            }
        }
    } else {
        None
    };

    let service = support::service::query_gateway_service_status(root_context.state_root()).ok();
    let runtime_snapshot = if should_attempt_admin {
        Some(runtime.block_on(load_runtime_status_snapshot(overrides.clone())))
    } else {
        Some(StatusRuntimeSnapshot {
            deployment_mode: None,
            bind_profile: None,
            remote_bind_detected: None,
            auth_state: None,
            browser_state: None,
            browser_sessions: None,
            connector_degraded: None,
            connector_queue_depth: None,
            memory_entries: None,
            memory_bytes: None,
            support_bundle_failures: None,
            self_healing_active_incidents: None,
            self_healing_resolved_incidents: None,
            self_healing_heartbeat_count: None,
            diagnostics_available: false,
            diagnostics_error: Some(
                "admin token is unavailable; runtime diagnostics were skipped".to_owned(),
            ),
        })
    };
    let deployment = runtime.block_on(load_runtime_deployment_snapshot(overrides)).unwrap_or(None);

    let mut hints = Vec::new();
    hints.push("Use `palyra health` for a script-friendly liveness check.".to_owned());
    hints.push("Use `palyra logs --follow` for live journal tailing.".to_owned());
    hints.push("Use `palyra doctor` for prioritized remediation guidance.".to_owned());
    if runtime_snapshot.as_ref().is_some_and(|value| !value.diagnostics_available) {
        hints.push(
            "Provide admin auth to unlock diagnostics for auth, browser, channels, memory, and deployment posture.".to_owned(),
        );
    }
    if deployment.as_ref().is_some_and(|value| value.remote_bind_detected) {
        hints.push(
            "Remote bind is active; verify TLS/admin posture and consider `palyra dashboard --verify-remote`."
                .to_owned(),
        );
    }
    if service.as_ref().is_some_and(|value| !value.installed) {
        hints.push(
            "Use `palyra gateway install --start` to register the background gateway service."
                .to_owned(),
        );
    } else if service.as_ref().is_some_and(|value| !value.running) {
        hints.push("Gateway service is installed but not running; use `palyra gateway start` or inspect `palyra logs`.".to_owned());
    }
    if http.error.is_some() || grpc.error.is_some() {
        hints.push(
            "Gateway health probes are degraded; inspect service status, logs, and support bundle before deeper recovery."
                .to_owned(),
        );
    }

    let degraded = http.error.is_some()
        || grpc.error.is_some()
        || service.as_ref().is_some_and(|value| value.installed && !value.running);

    Ok(StatusReport {
        overall: if degraded { "degraded".to_owned() } else { "ok".to_owned() },
        gateway: StatusGatewaySnapshot {
            daemon_url: http_connection.base_url,
            grpc_url: grpc_connection.grpc_url,
            http,
            grpc,
            admin: admin_payload,
        },
        service,
        deployment,
        runtime: runtime_snapshot,
        hints,
    })
}

fn unavailable_health_surface(error: &str) -> StatusHealthSurface {
    StatusHealthSurface {
        status: "unavailable".to_owned(),
        service: "unknown".to_owned(),
        version: "unknown".to_owned(),
        git_hash: "unknown".to_owned(),
        uptime_seconds: 0,
        error: Some(sanitize_diagnostic_error(error)),
    }
}

async fn load_runtime_deployment_snapshot(
    overrides: app::ConnectionOverrides,
) -> Result<Option<control_plane::DeploymentPostureSummary>> {
    let context = match client::control_plane::connect_admin_console(overrides).await {
        Ok(context) => context,
        Err(_) => return Ok(None),
    };
    match context.client.get_deployment_posture().await {
        Ok(deployment) => Ok(Some(deployment)),
        Err(_) => Ok(None),
    }
}

async fn load_runtime_status_snapshot(
    overrides: app::ConnectionOverrides,
) -> StatusRuntimeSnapshot {
    let context = match client::control_plane::connect_admin_console(overrides).await {
        Ok(context) => context,
        Err(error) => {
            return StatusRuntimeSnapshot {
                deployment_mode: None,
                bind_profile: None,
                remote_bind_detected: None,
                auth_state: None,
                browser_state: None,
                browser_sessions: None,
                connector_degraded: None,
                connector_queue_depth: None,
                memory_entries: None,
                memory_bytes: None,
                support_bundle_failures: None,
                self_healing_active_incidents: None,
                self_healing_resolved_incidents: None,
                self_healing_heartbeat_count: None,
                diagnostics_available: false,
                diagnostics_error: Some(redact_auth_error(error.to_string().as_str())),
            };
        }
    };

    let deployment = context.client.get_deployment_posture().await.ok();
    let diagnostics = match context.client.get_diagnostics().await {
        Ok(value) => value,
        Err(error) => {
            return StatusRuntimeSnapshot {
                deployment_mode: deployment.as_ref().map(|value| value.mode.clone()),
                bind_profile: deployment.as_ref().map(|value| value.bind_profile.clone()),
                remote_bind_detected: deployment.as_ref().map(|value| value.remote_bind_detected),
                auth_state: None,
                browser_state: None,
                browser_sessions: None,
                connector_degraded: None,
                connector_queue_depth: None,
                memory_entries: None,
                memory_bytes: None,
                support_bundle_failures: None,
                self_healing_active_incidents: None,
                self_healing_resolved_incidents: None,
                self_healing_heartbeat_count: None,
                diagnostics_available: false,
                diagnostics_error: Some(redact_auth_error(error.to_string().as_str())),
            };
        }
    };

    StatusRuntimeSnapshot {
        deployment_mode: deployment.as_ref().map(|value| value.mode.clone()),
        bind_profile: deployment.as_ref().map(|value| value.bind_profile.clone()),
        remote_bind_detected: deployment.as_ref().map(|value| value.remote_bind_detected),
        auth_state: diagnostics
            .pointer("/auth_profiles/state")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        browser_state: diagnostics
            .pointer("/browserd/health/status")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                diagnostics.pointer("/browserd/enabled").and_then(Value::as_bool).map(|enabled| {
                    if enabled {
                        "configured".to_owned()
                    } else {
                        "disabled".to_owned()
                    }
                })
            }),
        browser_sessions: diagnostics.pointer("/browserd/sessions/active").and_then(Value::as_u64),
        connector_degraded: diagnostics
            .pointer("/observability/connector/degraded_connectors")
            .and_then(Value::as_u64),
        connector_queue_depth: diagnostics
            .pointer("/observability/connector/queue_depth")
            .and_then(Value::as_u64),
        memory_entries: diagnostics.pointer("/memory/usage/entries").and_then(Value::as_u64),
        memory_bytes: diagnostics.pointer("/memory/usage/approx_bytes").and_then(Value::as_u64),
        support_bundle_failures: diagnostics
            .pointer("/observability/support_bundle/failures")
            .and_then(Value::as_u64),
        self_healing_active_incidents: diagnostics
            .pointer("/observability/self_healing/summary/active")
            .and_then(Value::as_u64),
        self_healing_resolved_incidents: diagnostics
            .pointer("/observability/self_healing/summary/resolved")
            .and_then(Value::as_u64),
        self_healing_heartbeat_count: diagnostics
            .pointer("/observability/self_healing/heartbeats")
            .and_then(Value::as_array)
            .and_then(|entries| u64::try_from(entries.len()).ok()),
        diagnostics_available: true,
        diagnostics_error: None,
    }
}
