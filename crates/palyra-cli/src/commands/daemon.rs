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
        DaemonCommand::Status { url } => {
            let connection = root_context()?.resolve_http_connection(
                app::ConnectionOverrides {
                    daemon_url: url,
                    ..app::ConnectionOverrides::default()
                },
                app::ConnectionDefaults::USER,
            )?;
            let status_url = format!("{}/healthz", connection.base_url.trim_end_matches('/'));
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let response = fetch_health_with_retry(&client, &status_url)?;

            println!(
                "status={} service={} version={} git_hash={} uptime_seconds={}",
                response.status,
                response.service,
                response.version,
                response.git_hash,
                response.uptime_seconds
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
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

            if json {
                let output = serde_json::json!({
                    "url": target.url,
                    "mode": target.mode.as_str(),
                    "source": target.source.as_str(),
                    "config_path": target.config_path,
                    "verification": verification_report,
                    "opened": open,
                });
                println!(
                    "{}",
                    serde_json::to_string_pretty(&output)
                        .context("failed to encode dashboard URL output as JSON")?
                );
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
            let endpoint =
                format!("{}/admin/v1/runs/{run_id}/tape", connection.base_url.trim_end_matches('/'));
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
            let endpoint =
                format!("{}/admin/v1/runs/{run_id}/cancel", connection.base_url.trim_end_matches('/'));
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
