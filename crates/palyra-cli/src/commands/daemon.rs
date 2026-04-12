use crate::*;

fn root_context() -> Result<app::RootCommandContext> {
    app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for gateway command"))
}

fn apply_http_connection_headers(
    request: reqwest::blocking::RequestBuilder,
    connection: &app::HttpConnection,
) -> reqwest::blocking::RequestBuilder {
    let mut request = request
        .header("x-palyra-principal", connection.principal.clone())
        .header("x-palyra-device-id", connection.device_id.clone())
        .header("x-palyra-channel", connection.channel.clone())
        .header("x-palyra-trace-id", connection.trace_id.clone());
    if let Some(token) = connection.token.as_ref() {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    request
}

pub(crate) fn run_daemon(command: DaemonCommand) -> Result<()> {
    match command {
        DaemonCommand::Run { bin_path } => run_gateway_foreground(bin_path),
        DaemonCommand::Health { url, grpc_url } => super::health::run_health(url, grpc_url),
        DaemonCommand::Probe {
            url,
            grpc_url,
            token,
            principal,
            device_id,
            channel,
            path,
            verify_remote,
            identity_store_dir,
        } => run_gateway_probe(
            url,
            grpc_url,
            token,
            principal,
            device_id,
            channel,
            path,
            verify_remote,
            identity_store_dir,
        ),
        DaemonCommand::Discover { path, verify_remote, identity_store_dir } => {
            run_gateway_discover(path, verify_remote, identity_store_dir)
        }
        DaemonCommand::Call {
            method,
            params,
            url,
            grpc_url,
            token,
            principal,
            device_id,
            channel,
        } => run_gateway_call(method, params, url, grpc_url, token, principal, device_id, channel),
        DaemonCommand::UsageCost { db_path, days } => run_gateway_usage_cost(db_path, days),
        DaemonCommand::Install { service_name, bin_path, log_dir, start } => {
            run_gateway_install(service_name, bin_path, log_dir, start)
        }
        DaemonCommand::Start => run_gateway_service_action("start"),
        DaemonCommand::Stop => run_gateway_service_action("stop"),
        DaemonCommand::Restart => run_gateway_service_action("restart"),
        DaemonCommand::Uninstall => run_gateway_service_action("uninstall"),
        DaemonCommand::Logs { db_path, lines, follow, poll_interval_ms } => {
            super::logs::run_logs(db_path, lines, follow, poll_interval_ms)
        }
        DaemonCommand::Status { url } => run_gateway_status(url),
        DaemonCommand::DashboardUrl { path, verify_remote, identity_store_dir, open, json } => {
            let target = resolve_dashboard_access_target(path)?;
            let verification_report = if verify_remote {
                let _ = verify_dashboard_remote_target(
                    &target,
                    identity_store_dir.and_then(normalize_optional_text_arg),
                )?;
                target
                    .verification
                    .as_ref()
                    .map(|verification| redacted_dashboard_verification_report(verification, true))
            } else {
                None
            };

            if open {
                open_url_in_default_browser(target.url.as_str())
                    .with_context(|| format!("failed to open dashboard URL {}", target.url))?;
            }

            let remote_assist = build_remote_dashboard_assist_payload(&target, verify_remote);
            let output = serde_json::json!({
                "url": target.url,
                "mode": target.mode.as_str(),
                "source": target.source.as_str(),
                "config_path": target.config_path,
                "verification": verification_report,
                "remote_assist": remote_assist,
                "opened": open,
            });

            if output::preferred_json(json) {
                output::print_json_pretty(
                    &output,
                    "failed to encode dashboard URL output as JSON",
                )?;
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &output,
                    "failed to encode dashboard URL output as NDJSON",
                )?;
            } else {
                println!(
                    "daemon.dashboard_url mode={} source={} url={} config_path={}",
                    target.mode.as_str(),
                    target.source.as_str(),
                    target.url,
                    target.config_path.as_deref().unwrap_or("none")
                );
                if let Some(verification_report) = verification_report {
                    println!(
                        "daemon.dashboard_url.verification method={} verified={} expected_sha256={} observed_server_sha256={} gateway_ca_sha256={}",
                        verification_report.method.as_str(),
                        verification_report.verified,
                        verification_report.expected_fingerprint_sha256,
                        verification_report.observed_server_cert_fingerprint_sha256,
                        verification_report.gateway_ca_fingerprint_sha256.as_deref().unwrap_or("none")
                    );
                }
                if let Some(remote_assist) =
                    output.get("remote_assist")
                {
                    emit_remote_dashboard_assist_lines("daemon.dashboard_url", remote_assist);
                }
                if open {
                    println!("daemon.dashboard_url.opened=true");
                }
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        DaemonCommand::AdminStatus { url, token, principal, device_id, channel } => {
            let connection = root_context()?.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    token,
                    principal,
                    device_id,
                    channel,
                    grpc_url: None,
                },
                app::ConnectionDefaults::USER,
            )?;
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let response = fetch_admin_status(
                &client,
                connection.base_url.as_str(),
                connection.token,
                connection.principal,
                connection.device_id,
                Some(connection.channel),
                Some(connection.trace_id),
            )?;

            println!(
                "admin.status={} service={} grpc={}:{} quic_enabled={} denied_requests={} journal_events={}",
                response.status,
                response.service,
                response.transport.grpc_bind_addr,
                response.transport.grpc_port,
                response.transport.quic_enabled,
                response.counters.denied_requests,
                response.counters.journal_events
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        DaemonCommand::JournalRecent { url, token, principal, device_id, channel, limit } => {
            let connection = root_context()?.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    token,
                    principal,
                    device_id,
                    channel,
                    grpc_url: None,
                },
                app::ConnectionDefaults::USER,
            )?;
            let endpoint =
                format!("{}/admin/v1/journal/recent", connection.base_url.trim_end_matches('/'));
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let mut request = apply_http_connection_headers(client.get(endpoint), &connection);
            if let Some(limit) = limit {
                request = request.query(&[("limit", limit)]);
            }

            let response: JournalRecentResponse = request
                .send()
                .context("failed to call daemon journal recent endpoint")?
                .error_for_status()
                .context("daemon journal recent endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon journal recent payload")?;

            println!(
                "journal.total_events={} hash_chain_enabled={} returned_events={}",
                response.total_events,
                response.hash_chain_enabled,
                response.events.len()
            );
            for event in response.events {
                println!(
                    "journal.event event_id={} kind={} actor={} redacted={} timestamp_unix_ms={} hash={}",
                    event.event_id,
                    event.kind,
                    event.actor,
                    event.redacted,
                    event.timestamp_unix_ms,
                    event.hash.as_deref().unwrap_or("none")
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        DaemonCommand::JournalVacuum { db_path } => {
            let db_path = resolve_daemon_journal_db_path(db_path)?;
            ensure_journal_db_exists(db_path.as_path())?;
            let connection = Connection::open(db_path.as_path()).with_context(|| {
                format!("failed to open journal database {}", db_path.display())
            })?;
            connection.execute_batch("PRAGMA busy_timeout = 5000; VACUUM;").with_context(|| {
                format!("failed to run VACUUM on journal database {}", db_path.display())
            })?;
            println!("journal.vacuum db_path={} status=ok", db_path.display());
            std::io::stdout().flush().context("stdout flush failed")
        }
        DaemonCommand::JournalCheckpoint {
            db_path,
            mode,
            sign,
            device_id,
            identity_store_dir,
            attestation_out,
            json,
        } => {
            let db_path = resolve_daemon_journal_db_path(db_path)?;
            ensure_journal_db_exists(db_path.as_path())?;
            let connection = Connection::open(db_path.as_path()).with_context(|| {
                format!("failed to open journal database {}", db_path.display())
            })?;
            connection.execute_batch("PRAGMA busy_timeout = 5000;").with_context(|| {
                format!("failed to configure busy_timeout for {}", db_path.display())
            })?;
            let pragma_sql = format!("PRAGMA wal_checkpoint({});", checkpoint_mode_sql(mode));
            let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = connection
                .query_row(pragma_sql.as_str(), [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .with_context(|| {
                    format!(
                        "failed to run wal_checkpoint({}) on journal database {}",
                        checkpoint_mode_sql(mode),
                        db_path.display()
                    )
                })?;

            let checkpoint = JournalCheckpointOutput {
                db_path: db_path.display().to_string(),
                mode: checkpoint_mode_label(mode).to_owned(),
                busy,
                log_frames,
                checkpointed_frames,
                attestation: None,
            };

            if sign {
                validate_canonical_id(device_id.as_str())
                    .context("--device-id must be a canonical ULID when --sign is set")?;
                let latest_hash = read_latest_journal_hash(&connection)
                    .context("failed to read latest hash-chain root from journal database")?
                    .ok_or_else(|| {
                        anyhow!(
                            "journal hash-chain root is unavailable; enable hash chain and ensure at least one hashed event is present before using --sign"
                        )
                    })?;
                let identity_store_root = resolve_identity_store_root(identity_store_dir)?;
                let identity_store = FilesystemSecretStore::new(identity_store_root.as_path())
                    .with_context(|| {
                        format!(
                            "failed to initialize identity store at {}",
                            identity_store_root.display()
                        )
                    })?;
                let device_identity = DeviceIdentity::load(&identity_store, device_id.as_str())
                    .map_err(|error| {
                        anyhow!(
                            "failed to load device identity {device_id} from {}: {error}",
                            identity_store_root.display()
                        )
                    })?;
                let attestation = build_journal_checkpoint_attestation(
                    &device_identity,
                    JournalCheckpointAttestationRequest {
                        db_path: db_path.as_path(),
                        mode,
                        busy,
                        log_frames,
                        checkpointed_frames,
                        latest_hash: latest_hash.as_str(),
                        signed_at_unix_ms: unix_now_ms(),
                    },
                )
                .context("failed to build journal checkpoint attestation")?;

                if let Some(output_path) = attestation_out.as_ref() {
                    let output_path = PathBuf::from(output_path);
                    let encoded = serde_json::to_vec_pretty(&attestation)
                        .context("failed to serialize journal checkpoint attestation JSON")?;
                    write_file_atomically(output_path.as_path(), encoded.as_slice()).with_context(
                        || {
                            format!(
                                "failed to write journal checkpoint attestation to {}",
                                output_path.display()
                            )
                        },
                    )?;
                }

                if json {
                    let signed_output =
                        JournalCheckpointOutput { attestation: Some(attestation), ..checkpoint };
                    let encoded = serde_json::to_string_pretty(&signed_output)
                        .context("failed to serialize journal checkpoint output as JSON")?;
                    println!("{encoded}");
                } else {
                    println!(
                        "journal.checkpoint db_path={} mode={} busy={} log_frames={} checkpointed_frames={}",
                        checkpoint.db_path,
                        checkpoint.mode,
                        checkpoint.busy,
                        checkpoint.log_frames,
                        checkpoint.checkpointed_frames
                    );
                    println!(
                        "journal.checkpoint.attestation device_id={} key_id={} algorithm={} latest_hash={} payload_sha256={} signature_base64={} attestation_out={}",
                        attestation.payload.device_id,
                        attestation.key_id,
                        attestation.algorithm,
                        attestation.payload.latest_hash,
                        attestation.payload_sha256,
                        attestation.signature_base64,
                        attestation_out.as_deref().unwrap_or("none")
                    );
                }
            } else if json {
                let encoded = serde_json::to_string_pretty(&checkpoint)
                    .context("failed to serialize journal checkpoint output as JSON")?;
                println!("{encoded}");
            } else {
                println!(
                    "journal.checkpoint db_path={} mode={} busy={} log_frames={} checkpointed_frames={}",
                    checkpoint.db_path,
                    checkpoint.mode,
                    checkpoint.busy,
                    checkpoint.log_frames,
                    checkpoint.checkpointed_frames
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        DaemonCommand::RunStatus { url, token, principal, device_id, channel, run_id } => {
            validate_canonical_id(run_id.as_str())
                .context("run_id must be a canonical ULID for daemon run-status")?;
            let connection = root_context()?.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    token,
                    principal,
                    device_id,
                    channel,
                    grpc_url: None,
                },
                app::ConnectionDefaults::USER,
            )?;
            let endpoint =
                format!("{}/admin/v1/runs/{run_id}", connection.base_url.trim_end_matches('/'));
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let request = apply_http_connection_headers(client.get(endpoint), &connection);
            let response: RunStatusResponse = request
                .send()
                .context("failed to call daemon run status endpoint")?
                .error_for_status()
                .context("daemon run status endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon run status payload")?;
            println!(
                "run.status run_id={} state={} cancel_requested={} prompt_tokens={} completion_tokens={} total_tokens={} tape_events={}",
                response.run_id,
                response.state,
                response.cancel_requested,
                response.prompt_tokens,
                response.completion_tokens,
                response.total_tokens,
                response.tape_events
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        DaemonCommand::RunTape {
            url,
            token,
            principal,
            device_id,
            channel,
            run_id,
            after_seq,
            limit,
        } => {
            validate_canonical_id(run_id.as_str())
                .context("run_id must be a canonical ULID for daemon run-tape")?;
            let connection = root_context()?.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    token,
                    principal,
                    device_id,
                    channel,
                    grpc_url: None,
                },
                app::ConnectionDefaults::USER,
            )?;
            let endpoint = format!(
                "{}/admin/v1/runs/{run_id}/tape",
                connection.base_url.trim_end_matches('/')
            );
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let mut request = apply_http_connection_headers(client.get(endpoint), &connection);
            if let Some(after_seq) = after_seq {
                request = request.query(&[("after_seq", after_seq)]);
            }
            if let Some(limit) = limit {
                request = request.query(&[("limit", limit)]);
            }
            let response: RunTapeResponse = request
                .send()
                .context("failed to call daemon run tape endpoint")?
                .error_for_status()
                .context("daemon run tape endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon run tape payload")?;
            println!(
                "run.tape run_id={} events={} returned_bytes={} next_after_seq={}",
                response.run_id,
                response.events.len(),
                response.returned_bytes,
                response
                    .next_after_seq
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned())
            );
            for event in response.events {
                println!(
                    "run.tape.event seq={} type={} payload_json={}",
                    event.seq, event.event_type, event.payload_json
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        DaemonCommand::RunCancel { url, token, principal, device_id, channel, run_id, reason } => {
            validate_canonical_id(run_id.as_str())
                .context("run_id must be a canonical ULID for daemon run-cancel")?;
            let connection = root_context()?.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    token,
                    principal,
                    device_id,
                    channel,
                    grpc_url: None,
                },
                app::ConnectionDefaults::USER,
            )?;
            let endpoint = format!(
                "{}/admin/v1/runs/{run_id}/cancel",
                connection.base_url.trim_end_matches('/')
            );
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let mut request = apply_http_connection_headers(client.post(endpoint), &connection);
            if let Some(reason) = reason {
                request = request.json(&RunCancelRequestBody { reason });
            }
            let response: RunCancelResponse = request
                .send()
                .context("failed to call daemon run cancel endpoint")?
                .error_for_status()
                .context("daemon run cancel endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon run cancel payload")?;
            println!(
                "run.cancel run_id={} cancel_requested={} reason={}",
                response.run_id, response.cancel_requested, response.reason
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

#[derive(Debug, Serialize)]
struct GatewayStatusReport {
    daemon_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    health: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    health_error: Option<String>,
    service: support::service::GatewayServiceStatus,
}

pub(crate) fn resolve_palyrad_binary(bin_path: Option<String>) -> Result<PathBuf> {
    if let Some(path) = bin_path {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        anyhow::bail!("palyrad binary does not exist: {}", path.display());
    }

    let executable = if cfg!(windows) { "palyrad.exe" } else { "palyrad" };
    let current_exe =
        std::env::current_exe().context("failed to resolve current CLI executable")?;
    let mut candidates = Vec::new();
    if let Some(parent) = current_exe.parent() {
        candidates.push(parent.join(executable));
        candidates.push(parent.join("deps").join(executable));
    }
    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    anyhow::bail!(
        "failed to locate palyrad next to the current CLI; pass --bin-path to `palyra gateway run`"
    )
}

fn run_gateway_foreground(bin_path: Option<String>) -> Result<()> {
    let context = root_context()?;
    let binary = resolve_palyrad_binary(bin_path)?;
    let mut command = std::process::Command::new(&binary);
    if let Some(config_path) = context.config_path() {
        command.env("PALYRA_CONFIG", config_path);
    }
    command.env("PALYRA_STATE_ROOT", context.state_root());

    let status = command.status().with_context(|| {
        format!("failed to start palyrad foreground process {}", binary.display())
    })?;
    if !status.success() {
        anyhow::bail!(
            "palyrad exited with status {}",
            status.code().map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned())
        );
    }
    Ok(())
}

fn run_gateway_install(
    service_name: Option<String>,
    bin_path: Option<String>,
    log_dir: Option<String>,
    start: bool,
) -> Result<()> {
    let context = root_context()?;
    let daemon_bin = resolve_palyrad_binary(bin_path)?;
    let request = support::service::GatewayServiceInstallRequest {
        service_name,
        daemon_bin,
        state_root: context.state_root().to_path_buf(),
        config_path: context.config_path().map(Path::to_path_buf),
        log_dir: log_dir.map(PathBuf::from),
        start_now: start,
    };
    let status = support::service::install_gateway_service(&request)?;
    emit_gateway_service_status("gateway.install", &status)
}

fn run_gateway_service_action(action: &str) -> Result<()> {
    let context = root_context()?;
    let status = match action {
        "start" => support::service::start_gateway_service(context.state_root())?,
        "stop" => support::service::stop_gateway_service(context.state_root())?,
        "restart" => support::service::restart_gateway_service(context.state_root())?,
        "uninstall" => support::service::uninstall_gateway_service(context.state_root())?,
        _ => anyhow::bail!("unsupported gateway service action `{action}`"),
    };
    emit_gateway_service_status(format!("gateway.{action}").as_str(), &status)
}

fn emit_gateway_service_status(
    prefix: &str,
    status: &support::service::GatewayServiceStatus,
) -> Result<()> {
    let context = root_context()?;
    if context.prefers_json() {
        return output::print_json_pretty(
            &json!({
                "action": prefix,
                "service": status,
            }),
            "failed to encode gateway service output as JSON",
        );
    }
    if context.prefers_ndjson() {
        return output::print_json_line(
            &json!({
                "action": prefix,
                "installed": status.installed,
                "running": status.running,
                "enabled": status.enabled,
                "manager": status.manager,
                "service_name": status.service_name,
                "definition_path": status.definition_path,
                "stdout_log_path": status.stdout_log_path,
                "stderr_log_path": status.stderr_log_path,
                "detail": status.detail,
            }),
            "failed to encode gateway service output as NDJSON",
        );
    }

    println!(
        "{prefix} installed={} running={} enabled={} manager={} service_name={} definition_path={} stdout_log_path={} stderr_log_path={}",
        status.installed,
        status.running,
        status.enabled,
        status.manager,
        status.service_name,
        status.definition_path.as_deref().unwrap_or("none"),
        status.stdout_log_path.as_deref().unwrap_or("none"),
        status.stderr_log_path.as_deref().unwrap_or("none"),
    );
    if let Some(detail) = status.detail.as_deref() {
        println!("{prefix}.detail={detail}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_gateway_status(url: Option<String>) -> Result<()> {
    let context = root_context()?;
    let connection = context.resolve_http_connection(
        app::ConnectionOverrides { daemon_url: url, ..app::ConnectionOverrides::default() },
        app::ConnectionDefaults::USER,
    )?;
    let status_url = format!("{}/healthz", connection.base_url.trim_end_matches('/'));
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let service = support::service::query_gateway_service_status(context.state_root()).unwrap_or(
        support::service::GatewayServiceStatus {
            installed: false,
            running: false,
            enabled: false,
            manager: "unknown".to_owned(),
            service_name: support::service::default_service_name(),
            definition_path: None,
            stdout_log_path: None,
            stderr_log_path: None,
            detail: Some("service status unavailable".to_owned()),
        },
    );
    let (health, health_error) = match fetch_health_with_retry(&client, &status_url) {
        Ok(response) => (
            Some(json!({
                "status": response.status,
                "service": response.service,
                "version": response.version,
                "git_hash": response.git_hash,
                "uptime_seconds": response.uptime_seconds,
            })),
            None,
        ),
        Err(error) => (None, Some(sanitize_diagnostic_error(error.to_string().as_str()))),
    };

    let report =
        GatewayStatusReport { daemon_url: connection.base_url, health, health_error, service };
    if context.prefers_json() {
        return output::print_json_pretty(
            &report,
            "failed to encode gateway status output as JSON",
        );
    }
    if context.prefers_ndjson() {
        output::print_json_line(&report, "failed to encode gateway status output as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }

    println!(
        "gateway.status daemon_url={} installed={} running={} enabled={} manager={} service_name={}",
        report.daemon_url,
        report.service.installed,
        report.service.running,
        report.service.enabled,
        report.service.manager,
        report.service.service_name
    );
    if let Some(health) = report.health.as_ref() {
        println!(
            "gateway.status.health status={} service={} version={} git_hash={} uptime_seconds={}",
            health.get("status").and_then(Value::as_str).unwrap_or("unknown"),
            health.get("service").and_then(Value::as_str).unwrap_or("unknown"),
            health.get("version").and_then(Value::as_str).unwrap_or("unknown"),
            health.get("git_hash").and_then(Value::as_str).unwrap_or("unknown"),
            health.get("uptime_seconds").and_then(Value::as_u64).unwrap_or(0)
        );
    }
    if let Some(error) = report.health_error.as_deref() {
        println!("gateway.status.health_error={error}");
    }
    if let Some(detail) = report.service.detail.as_deref() {
        println!("gateway.status.service_detail={detail}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn collect_gateway_health(
    url: Option<String>,
    grpc_url: Option<String>,
) -> Result<(app::HttpConnection, AgentConnection, HealthResponse, gateway_v1::HealthResponse)> {
    let context = root_context()?;
    let http_connection = context.resolve_http_connection(
        app::ConnectionOverrides {
            daemon_url: url,
            grpc_url: None,
            token: None,
            principal: None,
            device_id: None,
            channel: None,
        },
        app::ConnectionDefaults::USER,
    )?;
    let grpc_connection = context.resolve_grpc_connection(
        app::ConnectionOverrides {
            daemon_url: None,
            grpc_url,
            token: None,
            principal: None,
            device_id: None,
            channel: None,
        },
        app::ConnectionDefaults::USER,
    )?;
    let status_url = format!("{}/healthz", http_connection.base_url.trim_end_matches('/'));
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let http = fetch_health_with_retry(&client, &status_url)?;
    let runtime = build_runtime()?;
    let grpc = runtime.block_on(fetch_grpc_health_with_retry(grpc_connection.grpc_url.clone()))?;
    Ok((http_connection, grpc_connection, http, grpc))
}

fn build_gateway_discover_payload(
    path: Option<String>,
    verify_remote: bool,
    identity_store_dir: Option<String>,
) -> Result<Value> {
    let context = root_context()?;
    let http_connection = context.resolve_http_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::USER,
    )?;
    let grpc_connection = context.resolve_grpc_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::USER,
    )?;
    let target = resolve_dashboard_access_target(path)?;
    let verification_report = if verify_remote {
        let _ = verify_dashboard_remote_target(
            &target,
            identity_store_dir.and_then(normalize_optional_text_arg),
        )?;
        target
            .verification
            .as_ref()
            .map(|verification| redacted_dashboard_verification_report(verification, true))
    } else {
        None
    };
    let remote_assist = build_remote_dashboard_assist_payload(&target, verify_remote);
    Ok(json!({
        "mode": "config_profile_tunnel_first",
        "profile": context.profile_name(),
        "config_path": context.config_path().map(|value| value.display().to_string()),
        "state_root": context.state_root().display().to_string(),
        "daemon_url": http_connection.base_url,
        "grpc_url": grpc_connection.grpc_url,
        "dashboard": {
            "url": target.url,
            "mode": target.mode.as_str(),
            "source": target.source.as_str(),
            "config_path": target.config_path,
            "verification": verification_report,
        },
        "remote_assist": remote_assist,
        "remote_access_hint": if matches!(target.mode, DashboardAccessMode::Remote) {
            Some("Prefer `palyra tunnel --ssh <user>@<host> --open` unless you intentionally operate a verified HTTPS dashboard endpoint.")
        } else {
            None
        },
    }))
}

fn run_gateway_discover(
    path: Option<String>,
    verify_remote: bool,
    identity_store_dir: Option<String>,
) -> Result<()> {
    let payload = build_gateway_discover_payload(path, verify_remote, identity_store_dir)?;
    let context = root_context()?;
    if context.prefers_json() {
        return output::print_json_pretty(
            &payload,
            "failed to encode gateway discover output as JSON",
        );
    }
    if context.prefers_ndjson() {
        return output::print_json_line(
            &payload,
            "failed to encode gateway discover output as NDJSON",
        );
    }

    println!(
        "gateway.discover mode={} profile={} config_path={} state_root={} daemon_url={} grpc_url={}",
        payload.get("mode").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("profile").and_then(Value::as_str).unwrap_or("none"),
        payload.get("config_path").and_then(Value::as_str).unwrap_or("none"),
        payload.get("state_root").and_then(Value::as_str).unwrap_or("none"),
        payload.get("daemon_url").and_then(Value::as_str).unwrap_or("none"),
        payload.get("grpc_url").and_then(Value::as_str).unwrap_or("none")
    );
    if let Some(dashboard) = payload.get("dashboard") {
        println!(
            "gateway.discover.dashboard mode={} source={} url={}",
            dashboard.get("mode").and_then(Value::as_str).unwrap_or("unknown"),
            dashboard.get("source").and_then(Value::as_str).unwrap_or("unknown"),
            dashboard.get("url").and_then(Value::as_str).unwrap_or("none")
        );
        if let Some(verification) = dashboard.get("verification") {
            println!(
                "gateway.discover.dashboard.verification method={} verified={}",
                verification.get("method").and_then(Value::as_str).unwrap_or("unknown"),
                verification.get("verified").and_then(Value::as_bool).unwrap_or(false)
            );
        }
    }
    if let Some(hint) = payload.get("remote_access_hint").and_then(Value::as_str) {
        println!("gateway.discover.hint={hint}");
    }
    if let Some(remote_assist) = payload.get("remote_assist") {
        emit_remote_dashboard_assist_lines("gateway.discover", remote_assist);
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_remote_dashboard_assist_payload(
    target: &DashboardAccessTarget,
    verify_remote: bool,
) -> Option<Value> {
    if !matches!(target.mode, DashboardAccessMode::Remote) {
        return None;
    }

    let verification_mode =
        target.verification.as_ref().map(|verification| verification.method.as_str().to_owned());
    let trust_state = match (target.verification.as_ref(), verify_remote) {
        (Some(_), true) => "verified",
        (Some(_), false) => "verification_configured",
        (None, _) => "pin_missing",
    };
    Some(json!({
        "trust_state": trust_state,
        "verification_mode": verification_mode,
        "verification_required": target.verification.is_some(),
        "reverify_recommended": target.verification.is_some() && !verify_remote,
        "commands": {
            "verify": "palyra dashboard --verify-remote --json",
            "discover": "palyra gateway discover --verify-remote --json",
            "tunnel": "palyra tunnel --ssh <user>@<host> --remote-port 7142 --local-port 7142 --open",
            "support_bundle": "palyra support-bundle export --output ./artifacts/palyra-support-bundle.zip",
        },
        "troubleshooting": [
            "If trust material changed, rerun remote verification before opening the dashboard again.",
            "Use the SSH tunnel handoff unless you intentionally maintain a verified HTTPS dashboard endpoint.",
            "Export a support bundle after repeated handshake or fingerprint failures so recovery has full diagnostics."
        ],
    }))
}

fn emit_remote_dashboard_assist_lines(prefix: &str, payload: &Value) {
    println!(
        "{prefix}.remote trust_state={} verification_mode={} verification_required={} reverify_recommended={}",
        payload.get("trust_state").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("verification_mode").and_then(Value::as_str).unwrap_or("none"),
        payload
            .get("verification_required")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        payload
            .get("reverify_recommended")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    );
    if let Some(commands) = payload.get("commands").and_then(Value::as_object) {
        for (name, command) in commands {
            if let Some(command) = command.as_str() {
                println!("{prefix}.remote.command {name}=\"{command}\"");
            }
        }
    }
    if let Some(troubleshooting) = payload.get("troubleshooting").and_then(Value::as_array) {
        for (index, item) in troubleshooting.iter().enumerate() {
            if let Some(item) = item.as_str() {
                println!("{prefix}.remote.troubleshooting[{}]={}", index, item);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_gateway_probe(
    url: Option<String>,
    grpc_url: Option<String>,
    token: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
    path: Option<String>,
    verify_remote: bool,
    identity_store_dir: Option<String>,
) -> Result<()> {
    let (http_connection, grpc_connection, http, grpc) =
        collect_gateway_health(url.clone(), grpc_url.clone())?;
    let discover = build_gateway_discover_payload(path, verify_remote, identity_store_dir)?;
    let context = root_context()?;
    let admin_connection = context.resolve_http_connection(
        app::ConnectionOverrides {
            daemon_url: url,
            grpc_url: None,
            token,
            principal,
            device_id,
            channel,
        },
        app::ConnectionDefaults::ADMIN,
    )?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let admin = match fetch_admin_status_payload(
        &client,
        admin_connection.base_url.as_str(),
        admin_connection.token.clone(),
        admin_connection.principal.clone(),
        admin_connection.device_id.clone(),
        Some(admin_connection.channel.clone()),
        Some(admin_connection.trace_id.clone()),
    ) {
        Ok(payload) => json!({
            "available": true,
            "payload": payload,
        }),
        Err(error) => json!({
            "available": false,
            "error": sanitize_diagnostic_error(error.to_string().as_str()),
        }),
    };

    let payload = json!({
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
        "admin": admin,
        "discover": discover,
    });

    if context.prefers_json() {
        return output::print_json_pretty(
            &payload,
            "failed to encode gateway probe output as JSON",
        );
    }
    if context.prefers_ndjson() {
        return output::print_json_line(
            &payload,
            "failed to encode gateway probe output as NDJSON",
        );
    }

    println!(
        "gateway.probe overall=ok daemon_url={} grpc_url={}",
        payload.get("daemon_url").and_then(Value::as_str).unwrap_or("none"),
        payload.get("grpc_url").and_then(Value::as_str).unwrap_or("none")
    );
    println!(
        "gateway.probe.http={} service={} version={} git_hash={} uptime_seconds={}",
        http.status, http.service, http.version, http.git_hash, http.uptime_seconds
    );
    println!(
        "gateway.probe.grpc={} service={} version={} git_hash={} uptime_seconds={}",
        grpc.status, grpc.service, grpc.version, grpc.git_hash, grpc.uptime_seconds
    );
    println!(
        "gateway.probe.dashboard mode={} url={}",
        payload.pointer("/discover/dashboard/mode").and_then(Value::as_str).unwrap_or("unknown"),
        payload.pointer("/discover/dashboard/url").and_then(Value::as_str).unwrap_or("none")
    );
    if payload.pointer("/admin/available").and_then(Value::as_bool).unwrap_or(false) {
        let admin_payload = payload.pointer("/admin/payload").unwrap_or(&Value::Null);
        println!(
            "gateway.probe.admin status={} journal_events={} denied_requests={}",
            admin_payload.get("status").and_then(Value::as_str).unwrap_or("unknown"),
            admin_payload.pointer("/counters/journal_events").and_then(Value::as_u64).unwrap_or(0),
            admin_payload.pointer("/counters/denied_requests").and_then(Value::as_u64).unwrap_or(0)
        );
    } else if let Some(error) = payload.pointer("/admin/error").and_then(Value::as_str) {
        println!("gateway.probe.admin status=unavailable error={error}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

#[allow(clippy::too_many_arguments)]
fn run_gateway_call(
    method: String,
    params: Option<String>,
    url: Option<String>,
    grpc_url: Option<String>,
    token: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
) -> Result<()> {
    let params = match params {
        Some(raw) => serde_json::from_str::<Value>(raw.as_str())
            .with_context(|| format!("failed to parse --params as JSON for method {method}"))?,
        None => json!({}),
    };
    let payload = match method.as_str() {
        "health" => {
            let (http_connection, grpc_connection, http, grpc) = collect_gateway_health(url, grpc_url)?;
            json!({
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
            })
        }
        "discover" => build_gateway_discover_payload(
            params.get("path").and_then(Value::as_str).map(str::to_owned),
            params.get("verify_remote").and_then(Value::as_bool).unwrap_or(false),
            params.get("identity_store_dir").and_then(Value::as_str).map(str::to_owned),
        )?,
        "admin.status" => {
            let context = root_context()?;
            let connection = context.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    grpc_url: None,
                    token,
                    principal,
                    device_id,
                    channel,
                },
                app::ConnectionDefaults::ADMIN,
            )?;
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            fetch_admin_status_payload(
                &client,
                connection.base_url.as_str(),
                connection.token,
                connection.principal,
                connection.device_id,
                Some(connection.channel),
                Some(connection.trace_id),
            )?
        }
        "journal.recent" => {
            let context = root_context()?;
            let connection = context.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    grpc_url: None,
                    token,
                    principal,
                    device_id,
                    channel,
                },
                app::ConnectionDefaults::ADMIN,
            )?;
            let endpoint =
                format!("{}/admin/v1/journal/recent", connection.base_url.trim_end_matches('/'));
            let limit = params.get("limit").and_then(Value::as_u64).unwrap_or(20);
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let response: JournalRecentResponse = apply_http_connection_headers(client.get(endpoint), &connection)
                .query(&[("limit", limit)])
                .send()
                .context("failed to call daemon journal recent endpoint")?
                .error_for_status()
                .context("daemon journal recent endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon journal recent payload")?;
            serde_json::to_value(response).context("failed to encode journal recent payload")?
        }
        "run.status" => {
            let run_id = params
                .get("run_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("gateway call run.status requires params.run_id"))?;
            validate_canonical_id(run_id).context("params.run_id must be a canonical ULID")?;
            let context = root_context()?;
            let connection = context.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    grpc_url: None,
                    token,
                    principal,
                    device_id,
                    channel,
                },
                app::ConnectionDefaults::ADMIN,
            )?;
            let endpoint =
                format!("{}/admin/v1/runs/{run_id}", connection.base_url.trim_end_matches('/'));
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let response: RunStatusResponse = apply_http_connection_headers(client.get(endpoint), &connection)
                .send()
                .context("failed to call daemon run status endpoint")?
                .error_for_status()
                .context("daemon run status endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon run status payload")?;
            serde_json::to_value(response).context("failed to encode run status payload")?
        }
        "run.tape" => {
            let run_id = params
                .get("run_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("gateway call run.tape requires params.run_id"))?;
            validate_canonical_id(run_id).context("params.run_id must be a canonical ULID")?;
            let context = root_context()?;
            let connection = context.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    grpc_url: None,
                    token,
                    principal,
                    device_id,
                    channel,
                },
                app::ConnectionDefaults::ADMIN,
            )?;
            let endpoint = format!(
                "{}/admin/v1/runs/{run_id}/tape",
                connection.base_url.trim_end_matches('/')
            );
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let mut request = apply_http_connection_headers(client.get(endpoint), &connection);
            if let Some(after_seq) = params.get("after_seq").and_then(Value::as_i64) {
                request = request.query(&[("after_seq", after_seq)]);
            }
            if let Some(limit) = params.get("limit").and_then(Value::as_u64) {
                request = request.query(&[("limit", limit)]);
            }
            let response: RunTapeResponse = request
                .send()
                .context("failed to call daemon run tape endpoint")?
                .error_for_status()
                .context("daemon run tape endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon run tape payload")?;
            serde_json::to_value(response).context("failed to encode run tape payload")?
        }
        "dashboard.url" => build_gateway_discover_payload(
            params.get("path").and_then(Value::as_str).map(str::to_owned),
            params.get("verify_remote").and_then(Value::as_bool).unwrap_or(false),
            params.get("identity_store_dir").and_then(Value::as_str).map(str::to_owned),
        )?,
        "usage.cost" => build_gateway_usage_cost_value(
            params.get("db_path").and_then(Value::as_str).map(str::to_owned),
            params.get("days").and_then(Value::as_u64).unwrap_or(30) as u32,
        )?,
        _ => anyhow::bail!(
            "unsupported gateway call method `{method}`; supported methods: health, discover, admin.status, journal.recent, run.status, run.tape, dashboard.url, usage.cost"
        ),
    };

    let context = root_context()?;
    if context.prefers_json() {
        return output::print_json_pretty(&payload, "failed to encode gateway call output as JSON");
    }
    if context.prefers_ndjson() {
        return output::print_json_line(&payload, "failed to encode gateway call output as NDJSON");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "method": method,
            "result": payload,
        }))
        .context("failed to encode gateway call output")?
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_gateway_usage_cost_value(db_path: Option<String>, days: u32) -> Result<Value> {
    let days = days.clamp(1, 365);
    let db_path = resolve_daemon_journal_db_path(db_path)?;
    ensure_journal_db_exists(db_path.as_path())?;
    let connection = Connection::open(db_path.as_path())
        .with_context(|| format!("failed to open journal database {}", db_path.display()))?;
    let lookback_ms =
        now_unix_ms_i64()?.saturating_sub(i64::from(days).saturating_mul(24 * 60 * 60 * 1000));
    let smart_routing_enabled = std::env::var("PALYRA_SMART_ROUTING_ENABLED")
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(true);
    let smart_routing_default_mode = std::env::var("PALYRA_SMART_ROUTING_MODE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "suggest".to_owned());

    let mut pricing_map = std::collections::HashMap::<String, (Option<f64>, Option<f64>)>::new();
    let mut pricing_statement = connection.prepare(
        "SELECT model_id, input_cost_per_million_usd, output_cost_per_million_usd
         FROM usage_pricing_catalog
         ORDER BY effective_from_unix_ms DESC",
    )?;
    let mut pricing_rows = pricing_statement.query([])?;
    while let Some(row) = pricing_rows.next()? {
        let model_id = row.get::<_, String>(0)?;
        pricing_map.entry(model_id).or_insert((row.get(1)?, row.get(2)?));
    }
    let pricing_entries = pricing_map.len() as i64;

    let mut routing_map = std::collections::HashMap::<String, String>::new();
    let mut routing_suggest_runs = 0_i64;
    let mut routing_dry_run_runs = 0_i64;
    let mut routing_enforced_runs = 0_i64;
    let mut routing_overrides = 0_i64;
    let mut routing_statement = connection.prepare(
        "SELECT run_ulid, mode, default_model_id, actual_model_id
         FROM usage_routing_decisions
         WHERE created_at_unix_ms >= ?1
         ORDER BY created_at_unix_ms DESC",
    )?;
    let mut routing_rows = routing_statement.query([lookback_ms])?;
    while let Some(row) = routing_rows.next()? {
        let run_id = row.get::<_, String>(0)?;
        let mode = row.get::<_, String>(1)?;
        let default_model_id = row.get::<_, String>(2)?;
        let actual_model_id = row.get::<_, String>(3)?;
        match mode.as_str() {
            "suggest" => routing_suggest_runs += 1,
            "dry_run" => routing_dry_run_runs += 1,
            "enforced" => routing_enforced_runs += 1,
            _ => {}
        }
        if actual_model_id != default_model_id {
            routing_overrides += 1;
        }
        routing_map.entry(run_id).or_insert(actual_model_id);
    }
    let active_alerts = connection
        .query_row(
            "SELECT COUNT(*) FROM usage_alerts WHERE resolved_at_unix_ms IS NULL AND last_observed_at_unix_ms >= ?1",
            [lookback_ms],
            |row| row.get::<_, i64>(0),
        )
        .context("failed to count active usage alerts")?;

    let mut runs_statement = connection.prepare(
        "SELECT run_ulid, started_at_unix_ms, prompt_tokens, completion_tokens, total_tokens
         FROM orchestrator_runs
         WHERE started_at_unix_ms >= ?1
         ORDER BY started_at_unix_ms ASC",
    )?;
    let mut rows = runs_statement.query([lookback_ms])?;
    let mut totals_runs = 0_i64;
    let mut totals_prompt = 0_i64;
    let mut totals_completion = 0_i64;
    let mut totals_total = 0_i64;
    let mut totals_estimated_cost = 0.0_f64;
    let mut total_estimated_runs = 0_i64;
    let mut daily_map = std::collections::BTreeMap::<String, (i64, i64, i64, i64, f64, i64)>::new();
    let mut daily = Vec::new();
    while let Some(row) = rows.next()? {
        let run_id = row.get::<_, String>(0)?;
        let started_at_unix_ms = row.get::<_, i64>(1)?;
        let prompt_tokens = row.get::<_, i64>(2)?;
        let completion_tokens = row.get::<_, i64>(3)?;
        let total_tokens = row.get::<_, i64>(4)?;
        totals_runs += 1;
        totals_prompt += prompt_tokens;
        totals_completion += completion_tokens;
        totals_total += total_tokens;
        let run_date = connection
            .query_row("SELECT date(?1 / 1000, 'unixepoch')", [started_at_unix_ms], |date_row| {
                date_row.get::<_, String>(0)
            })
            .context("failed to derive run date for usage-cost output")?;
        let mut estimated_cost = Value::Null;
        let mut estimated_cost_raw = 0.0_f64;
        let mut estimated_count = 0_i64;
        if let Some(model_id) = routing_map.get(run_id.as_str()) {
            if let Some((input_rate, output_rate)) = pricing_map.get(model_id.as_str()) {
                let cost = input_rate.unwrap_or(0.0) * (prompt_tokens as f64 / 1_000_000.0)
                    + output_rate.unwrap_or(0.0) * (completion_tokens as f64 / 1_000_000.0);
                estimated_cost_raw = cost;
                estimated_cost = json!(cost);
                totals_estimated_cost += cost;
                total_estimated_runs += 1;
                estimated_count = 1;
            }
        }
        let entry = daily_map.entry(run_date).or_insert((0, 0, 0, 0, 0.0, 0));
        entry.0 += 1;
        entry.1 += prompt_tokens;
        entry.2 += completion_tokens;
        entry.3 += total_tokens;
        entry.4 += estimated_cost_raw;
        entry.5 += estimated_count;
        daily.push(json!({
            "date": connection
                .query_row("SELECT date(?1 / 1000, 'unixepoch')", [started_at_unix_ms], |date_row| {
                    date_row.get::<_, String>(0)
                })?,
            "runs": 1,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "estimated_cost_usd": estimated_cost,
        }));
    }
    daily = daily_map
        .into_iter()
        .map(|(date, (runs, prompt_tokens, completion_tokens, total_tokens, estimated_cost, estimated_runs))| {
            json!({
                "date": date,
                "runs": runs,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "estimated_cost_usd": if estimated_runs > 0 { Some(estimated_cost) } else { None::<f64> },
                "estimated_runs": estimated_runs,
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "db_path": db_path.display().to_string(),
        "days": days,
        "cost_tracking_available": total_estimated_runs > 0,
        "estimated_cost_usd": if total_estimated_runs > 0 { Some(totals_estimated_cost) } else { None::<f64> },
        "smart_routing": {
            "enabled": smart_routing_enabled,
            "default_mode": smart_routing_default_mode,
        },
        "pricing_catalog": {
            "entries": pricing_entries,
            "models": pricing_map.len(),
        },
        "routing": {
            "suggest_runs": routing_suggest_runs,
            "dry_run_runs": routing_dry_run_runs,
            "enforced_runs": routing_enforced_runs,
            "overrides": routing_overrides,
        },
        "alerts": {
            "active": active_alerts,
        },
        "totals": {
            "runs": totals_runs,
            "prompt_tokens": totals_prompt,
            "completion_tokens": totals_completion,
            "total_tokens": totals_total,
            "estimated_runs": total_estimated_runs,
        },
        "daily": daily,
        "notes": [
            "Cost estimates reuse the shared Phase 7 pricing catalog stored in usage_pricing_catalog.",
            "Runs without a recorded routing decision stay excluded from estimated_cost_usd to avoid pretending certainty."
        ],
    }))
}

fn run_gateway_usage_cost(db_path: Option<String>, days: u32) -> Result<()> {
    let payload = build_gateway_usage_cost_value(db_path, days)?;
    let context = root_context()?;
    if context.prefers_json() {
        return output::print_json_pretty(
            &payload,
            "failed to encode gateway usage-cost output as JSON",
        );
    }
    if context.prefers_ndjson() {
        return output::print_json_line(
            &payload,
            "failed to encode gateway usage-cost output as NDJSON",
        );
    }

    println!(
        "gateway.usage_cost days={} db_path={} runs={} prompt_tokens={} completion_tokens={} total_tokens={} estimated_cost_usd={} smart_routing_enabled={} smart_routing_mode={} pricing_entries={} active_alerts={}",
        payload.get("days").and_then(Value::as_u64).unwrap_or(0),
        payload.get("db_path").and_then(Value::as_str).unwrap_or("none"),
        payload.pointer("/totals/runs").and_then(Value::as_i64).unwrap_or(0),
        payload.pointer("/totals/prompt_tokens").and_then(Value::as_i64).unwrap_or(0),
        payload.pointer("/totals/completion_tokens").and_then(Value::as_i64).unwrap_or(0),
        payload.pointer("/totals/total_tokens").and_then(Value::as_i64).unwrap_or(0),
        payload
            .get("estimated_cost_usd")
            .map_or_else(|| "unavailable".to_owned(), Value::to_string),
        payload
            .pointer("/smart_routing/enabled")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        payload
            .pointer("/smart_routing/default_mode")
            .and_then(Value::as_str)
            .unwrap_or("suggest"),
        payload.pointer("/pricing_catalog/entries").and_then(Value::as_i64).unwrap_or(0),
        payload.pointer("/alerts/active").and_then(Value::as_i64).unwrap_or(0),
    );
    println!(
        "gateway.usage_cost.routing suggest_runs={} dry_run_runs={} enforced_runs={} overrides={}",
        payload.pointer("/routing/suggest_runs").and_then(Value::as_i64).unwrap_or(0),
        payload.pointer("/routing/dry_run_runs").and_then(Value::as_i64).unwrap_or(0),
        payload.pointer("/routing/enforced_runs").and_then(Value::as_i64).unwrap_or(0),
        payload.pointer("/routing/overrides").and_then(Value::as_i64).unwrap_or(0),
    );
    if let Some(last_day) =
        payload.get("daily").and_then(Value::as_array).and_then(|entries| entries.last())
    {
        println!(
            "gateway.usage_cost.latest_day date={} runs={} total_tokens={}",
            last_day.get("date").and_then(Value::as_str).unwrap_or("unknown"),
            last_day.get("runs").and_then(Value::as_i64).unwrap_or(0),
            last_day.get("total_tokens").and_then(Value::as_i64).unwrap_or(0)
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}
