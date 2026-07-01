//! `palyra gateway` daemon commands: lifecycle, health, journal, and run inspection.
//!
//! Talks to a running `palyrad` over its admin HTTP and gRPC surfaces, manages
//! the installed gateway service, and special-cases desktop-control-center
//! managed runtimes whose state lives under `desktop-control-center/runtime`.

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

fn enrich_journal_recent_response(response: &mut JournalRecentResponse) {
    for event in &mut response.events {
        event.kind_label = Some(journal_event_kind_label(event.kind));
        event.actor_label = Some(journal_event_actor_label(event.actor));
    }
}

fn journal_event_kind_label(kind: i32) -> String {
    common_v1::journal_event::EventKind::try_from(kind)
        .map(|kind| proto_enum_label(kind.as_str_name(), "EVENT_KIND_"))
        .unwrap_or_else(|_| format!("unknown_{kind}"))
}

fn journal_event_actor_label(actor: i32) -> String {
    common_v1::journal_event::EventActor::try_from(actor)
        .map(|actor| proto_enum_label(actor.as_str_name(), "EVENT_ACTOR_"))
        .unwrap_or_else(|_| format!("unknown_{actor}"))
}

fn proto_enum_label(name: &str, prefix: &str) -> String {
    name.strip_prefix(prefix).unwrap_or(name).to_ascii_lowercase()
}

const RUN_TAPE_COMPACT_PREVIEW_BYTES: usize = 2048;
const TOOL_CATALOG_SNAPSHOT_EVENT: &str = "tool_catalog_snapshot";
const DESKTOP_MANAGED_RESTART_HEALTH_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(12);
const DESKTOP_MANAGED_RESTART_HEALTH_POLL_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(500);

#[derive(Debug, serde::Serialize)]
struct CompactRunTapeResponse {
    run_id: String,
    returned_bytes: usize,
    next_after_seq: Option<i64>,
    projection: &'static str,
    events: Vec<CompactRunTapeEvent>,
}

#[derive(Debug, serde::Serialize)]
struct CompactRunTapeEvent {
    seq: i64,
    event_type: String,
    payload_bytes: usize,
    payload_truncated: bool,
    payload_omitted: bool,
    payload_preview: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
}

fn compact_run_tape_response(response: RunTapeResponse) -> CompactRunTapeResponse {
    CompactRunTapeResponse {
        run_id: response.run_id,
        returned_bytes: response.returned_bytes,
        next_after_seq: response.next_after_seq,
        projection: "compact",
        events: response.events.into_iter().map(compact_run_tape_event).collect(),
    }
}

fn compact_run_tape_event(event: RunTapeEvent) -> CompactRunTapeEvent {
    let payload_bytes = event.payload_json.len();
    if event.event_type == TOOL_CATALOG_SNAPSHOT_EVENT {
        return CompactRunTapeEvent {
            seq: event.seq,
            event_type: event.event_type,
            payload_bytes,
            payload_truncated: true,
            payload_omitted: true,
            payload_preview: compact_tool_catalog_snapshot_preview(payload_bytes),
            summary: Some(
                "tool catalog snapshot omitted; rerun without --compact to inspect full schemas"
                    .to_owned(),
            ),
        };
    }

    let (payload_preview, payload_truncated) =
        truncate_utf8(event.payload_json.as_str(), RUN_TAPE_COMPACT_PREVIEW_BYTES);
    CompactRunTapeEvent {
        seq: event.seq,
        event_type: event.event_type,
        payload_bytes,
        payload_truncated,
        payload_omitted: false,
        payload_preview,
        summary: None,
    }
}

fn compact_tool_catalog_snapshot_preview(payload_bytes: usize) -> String {
    format!(
        "{{\"omitted_event_type\":\"{TOOL_CATALOG_SNAPSHOT_EVENT}\",\"payload_bytes\":{payload_bytes}}}"
    )
}

/// Truncates to at most `max_bytes` on a char boundary; the flag reports
/// whether truncation occurred.
fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_owned(), false);
    }

    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_owned(), true)
}

#[cfg(test)]
mod tests {
    use super::{
        compact_run_tape_response, enrich_gateway_service_action_error,
        gateway_runtime_root_selection, gateway_status_state_root_scope_note,
        is_desktop_runtime_state_root, journal_event_actor_label, journal_event_kind_label,
        read_remote_dashboard_assist_payload, request_desktop_managed_gateway_restart,
    };
    use crate::{common_v1, HealthResponse, RunTapeEvent, RunTapeResponse};
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn journal_recent_labels_known_proto_enums() {
        assert_eq!(
            journal_event_kind_label(common_v1::journal_event::EventKind::RunFailed as i32),
            "run_failed"
        );
        assert_eq!(
            journal_event_actor_label(common_v1::journal_event::EventActor::System as i32),
            "system"
        );
    }

    #[test]
    fn remote_dashboard_assist_reader_ignores_json_null() {
        assert!(read_remote_dashboard_assist_payload(&json!({})).is_none());
        assert!(read_remote_dashboard_assist_payload(&json!({ "remote_assist": null })).is_none());
        assert!(read_remote_dashboard_assist_payload(&json!({
            "remote_assist": { "trust_state": "verified" }
        }))
        .is_some());
    }

    #[test]
    fn compact_run_tape_omits_tool_catalog_snapshot_payload() {
        let response = compact_run_tape_response(RunTapeResponse {
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            returned_bytes: 5000,
            next_after_seq: Some(12),
            events: vec![
                RunTapeEvent {
                    seq: 11,
                    event_type: "model_token".to_owned(),
                    payload_json: "{\"text\":\"ok\"}".to_owned(),
                },
                RunTapeEvent {
                    seq: 12,
                    event_type: "tool_catalog_snapshot".to_owned(),
                    payload_json: "{\"tools\":[{\"name\":\"palyra.fs.apply_patch\",\"schema\":{\"large\":\"value\"}}]}".repeat(64),
                },
            ],
        });

        assert_eq!(response.projection, "compact");
        assert_eq!(response.events[0].payload_preview, "{\"text\":\"ok\"}");
        assert!(!response.events[0].payload_omitted);
        assert!(response.events[1].payload_omitted);
        assert!(response.events[1].payload_truncated);
        assert!(!response.events[1].payload_preview.contains("palyra.fs.apply_patch"));
        assert!(response.events[1]
            .summary
            .as_deref()
            .is_some_and(|summary| summary.contains("rerun without --compact")));
    }

    #[test]
    fn gateway_runtime_root_selection_prefers_existing_desktop_runtime_child() {
        let temp = tempdir().expect("tempdir should be created");
        let state_root = temp.path().join("state");
        let desktop_runtime = state_root.join("desktop-control-center").join("runtime");
        fs::create_dir_all(desktop_runtime.as_path())
            .expect("desktop runtime directory should be created");

        let selection = gateway_runtime_root_selection(state_root.as_path());

        assert_eq!(selection.requested, state_root);
        assert_eq!(selection.effective, desktop_runtime);
        assert!(selection.note.as_deref().is_some_and(|note| {
            note.contains("desktop-control-center runtime state detected")
        }));
    }

    #[test]
    fn gateway_runtime_root_selection_keeps_explicit_desktop_runtime_root() {
        let state_root =
            std::path::PathBuf::from("state").join("desktop-control-center").join("runtime");

        let selection = gateway_runtime_root_selection(state_root.as_path());

        assert_eq!(selection.requested, state_root);
        assert_eq!(selection.effective, selection.requested);
        assert!(selection.note.is_none());
        assert!(is_desktop_runtime_state_root(selection.effective.as_path()));
    }

    #[test]
    fn gateway_service_action_error_mentions_desktop_runtime_root() {
        let temp = tempdir().expect("tempdir should be created");
        let state_root = temp.path().join("state");
        let desktop_runtime = state_root.join("desktop-control-center").join("runtime");
        fs::create_dir_all(desktop_runtime.as_path())
            .expect("desktop runtime directory should be created");

        let error = enrich_gateway_service_action_error(
            state_root.as_path(),
            "restart",
            anyhow::anyhow!("precondition failed: no managed gateway service metadata exists"),
        );
        let message = error.to_string();

        assert!(message.contains("desktop-control-center runtime state root"), "{message}");
        assert!(message.contains("gateway restart"), "{message}");
        assert!(message.contains("--state-root"), "{message}");
    }

    #[test]
    fn desktop_managed_gateway_restart_touches_config_and_verifies_health() {
        let temp = tempdir().expect("tempdir should be created");
        let state_root = temp.path().join("state");
        let desktop_runtime = state_root.join("desktop-control-center").join("runtime");
        let config_dir = state_root.join("config");
        let config_path = config_dir.join("palyra.toml");
        fs::create_dir_all(desktop_runtime.as_path())
            .expect("desktop runtime directory should be created");
        fs::create_dir_all(config_dir.as_path()).expect("config directory should be created");
        fs::write(config_path.as_path(), "version = 1\n").expect("config should be written");
        let before = fs::metadata(config_path.as_path())
            .expect("config metadata before restart")
            .modified()
            .expect("config mtime before restart");

        std::thread::sleep(std::time::Duration::from_millis(20));
        let status = request_desktop_managed_gateway_restart(
            state_root.as_path(),
            Some(config_path.as_path()),
            || {
                Ok(HealthResponse {
                    service: "palyrad".to_owned(),
                    status: "ok".to_owned(),
                    version: "test".to_owned(),
                    git_hash: "test".to_owned(),
                    build_profile: "debug".to_owned(),
                    uptime_seconds: 1,
                })
            },
        )
        .expect("desktop restart request should succeed")
        .expect("desktop runtime should be restartable");

        assert_eq!(status.manager, "desktop-control-center");
        assert_eq!(status.service_name, "desktop-managed-palyrad");
        assert!(status.installed);
        assert!(
            status.detail.as_deref().is_some_and(|detail| detail.contains("health verified")),
            "status should explain the restart request: {status:?}"
        );
        let after = fs::metadata(config_path.as_path())
            .expect("config metadata after restart")
            .modified()
            .expect("config mtime after restart");
        assert!(after >= before, "touch should not move config mtime backwards");
        assert_eq!(
            fs::read_to_string(config_path.as_path()).expect("config should remain readable"),
            "version = 1\n"
        );
    }

    #[test]
    fn desktop_managed_gateway_restart_fails_when_health_is_not_verified() {
        let temp = tempdir().expect("tempdir should be created");
        let state_root = temp.path().join("state");
        let desktop_runtime = state_root.join("desktop-control-center").join("runtime");
        let config_dir = state_root.join("config");
        let config_path = config_dir.join("palyra.toml");
        fs::create_dir_all(desktop_runtime.as_path())
            .expect("desktop runtime directory should be created");
        fs::create_dir_all(config_dir.as_path()).expect("config directory should be created");
        fs::write(config_path.as_path(), "version = 1\n").expect("config should be written");

        let error = request_desktop_managed_gateway_restart(
            state_root.as_path(),
            Some(config_path.as_path()),
            || anyhow::bail!("connection refused"),
        )
        .expect_err("unverified desktop restart must fail loudly");
        let message = error.to_string();

        assert!(message.contains("health was not confirmed"), "{message}");
        assert!(message.contains("desktop app/test harness"), "{message}");
        assert!(message.contains("--state-root"), "{message}");
    }

    #[test]
    fn gateway_status_scope_note_marks_default_endpoint_for_empty_state_root() {
        let temp = tempdir().expect("tempdir should be created");
        let state_root = temp.path().join("state");

        let note = gateway_status_state_root_scope_note(
            true,
            None,
            state_root.as_path(),
            false,
            "http://127.0.0.1:7142",
            true,
            false,
        )
        .expect("empty explicit state root should produce a scope note");

        assert!(note.contains("requested state root has no config"), "{note}");
        assert!(note.contains("default daemon_url"), "{note}");
        assert!(note.contains("different running runtime"), "{note}");
    }
}

/// Dispatches `palyra gateway` daemon subcommands.
///
/// # Errors
/// Returns an error when the root context or connection cannot be resolved, a
/// daemon endpoint or service action fails, or output encoding fails.
pub(crate) fn run_daemon(command: DaemonCommand) -> Result<()> {
    match command {
        DaemonCommand::Run { bin_path } => run_gateway_foreground(bin_path),
        DaemonCommand::Health { url, grpc_url } => super::health::run_health(url, grpc_url, false),
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
        DaemonCommand::UsageCost { db_path, days, json } => {
            run_gateway_usage_cost(db_path, days, json)
        }
        DaemonCommand::Install { service_name, bin_path, log_dir, start } => {
            run_gateway_install(service_name, bin_path, log_dir, start)
        }
        DaemonCommand::Start => run_gateway_service_action("start"),
        DaemonCommand::Stop => run_gateway_service_action("stop"),
        DaemonCommand::Restart => run_gateway_service_action("restart"),
        DaemonCommand::Uninstall => run_gateway_service_action("uninstall"),
        DaemonCommand::Logs { db_path, lines, follow, poll_interval_ms, json } => {
            super::logs::run_logs(db_path, lines, follow, poll_interval_ms, json)
        }
        DaemonCommand::Status { url, json } => run_gateway_status(url, json),
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
                if let Some(remote_assist) = read_remote_dashboard_assist_payload(&output) {
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

            if output::preferred_json(false) {
                return output::print_json_pretty(
                    &response,
                    "failed to encode admin status output as JSON",
                );
            }
            if output::preferred_ndjson(false, false) {
                output::print_json_line(
                    &response,
                    "failed to encode admin status output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }

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
        DaemonCommand::JournalRecent { url, token, principal, device_id, channel, limit, json } => {
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

            let mut response: JournalRecentResponse = request
                .send()
                .context("failed to call daemon journal recent endpoint")?
                .error_for_status()
                .context("daemon journal recent endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon journal recent payload")?;
            enrich_journal_recent_response(&mut response);

            if output::preferred_json(json) {
                output::print_json_pretty(
                    &response,
                    "failed to encode daemon journal recent output as JSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &response,
                    "failed to encode daemon journal recent output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }

            println!(
                "journal.total_events={} hash_chain_enabled={} returned_events={}",
                response.total_events,
                response.hash_chain_enabled,
                response.events.len()
            );
            for event in response.events {
                println!(
                    "journal.event event_id={} kind={} kind_label={} actor={} actor_label={} redacted={} timestamp_unix_ms={} hash={}",
                    event.event_id,
                    event.kind,
                    event.kind_label.as_deref().unwrap_or("unknown"),
                    event.actor,
                    event.actor_label.as_deref().unwrap_or("unknown"),
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
        DaemonCommand::RunStatus { url, token, principal, device_id, channel, run_id, json } => {
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
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &response,
                    "failed to encode daemon run status output as JSON",
                )?;
            } else {
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
                if let Some(lifecycle_state) = response.lifecycle_state.as_deref() {
                    println!(
                        "run.lifecycle run_id={} state={} lifecycle_state={} continuation_required={} reason_code={}",
                        response.run_id,
                        response.state,
                        lifecycle_state,
                        response.continuation_required.unwrap_or(false),
                        response.reason_code.as_deref().unwrap_or("unknown")
                    );
                }
            }
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
            compact,
            json,
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
            if output::preferred_json(json) {
                if compact {
                    let response = compact_run_tape_response(response);
                    output::print_json_pretty(
                        &response,
                        "failed to encode compact daemon run tape output as JSON",
                    )?;
                } else {
                    output::print_json_pretty(
                        &response,
                        "failed to encode daemon run tape output as JSON",
                    )?;
                }
            } else if compact {
                let response = compact_run_tape_response(response);
                println!(
                    "run.tape run_id={} events={} returned_bytes={} projection={} next_after_seq={}",
                    response.run_id,
                    response.events.len(),
                    response.returned_bytes,
                    response.projection,
                    response
                        .next_after_seq
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "none".to_owned())
                );
                for event in response.events {
                    println!(
                        "run.tape.event seq={} type={} payload_bytes={} payload_truncated={} payload_omitted={} payload_preview={}",
                        event.seq,
                        event.event_type,
                        event.payload_bytes,
                        event.payload_truncated,
                        event.payload_omitted,
                        event.payload_preview
                    );
                    if let Some(summary) = event.summary {
                        println!(
                            "run.tape.event.summary seq={} type={} summary={}",
                            event.seq, event.event_type, summary
                        );
                    }
                }
            } else {
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
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        DaemonCommand::RunCancel {
            url,
            token,
            principal,
            device_id,
            channel,
            run_id,
            reason,
            json,
        } => {
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
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &response,
                    "failed to encode daemon run cancel output as JSON",
                )?;
            } else {
                if let Some(state) = response.state.as_deref() {
                    if response.cleanup_warning.is_empty() {
                        println!(
                            "run.cancel run_id={} state={} cancel_requested={} reason={}",
                            response.run_id, state, response.cancel_requested, response.reason
                        );
                    } else {
                        println!(
                            "run.cancel run_id={} state={} cancel_requested={} reason={} cleanup_warning={}",
                            response.run_id,
                            state,
                            response.cancel_requested,
                            response.reason,
                            response.cleanup_warning
                        );
                    }
                } else {
                    if response.cleanup_warning.is_empty() {
                        println!(
                            "run.cancel run_id={} cancel_requested={} reason={}",
                            response.run_id, response.cancel_requested, response.reason
                        );
                    } else {
                        println!(
                            "run.cancel run_id={} cancel_requested={} reason={} cleanup_warning={}",
                            response.run_id,
                            response.cancel_requested,
                            response.reason,
                            response.cleanup_warning
                        );
                    }
                }
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

#[derive(Debug, Serialize)]
struct GatewayStatusReport {
    daemon_url: String,
    state_root: String,
    effective_state_root: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_root_note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    health: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    health_error: Option<String>,
    service: support::service::GatewayServiceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayRuntimeRootSelection {
    requested: PathBuf,
    effective: PathBuf,
    note: Option<String>,
}

/// Resolves the `palyrad` binary from an explicit override or next to the CLI executable.
///
/// # Errors
/// Returns an error when the explicit path is not a file or no sibling
/// `palyrad` binary can be located.
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
    let runtime_root = gateway_runtime_root_selection(context.state_root());
    let binary = resolve_palyrad_binary(bin_path)?;
    let mut command = std::process::Command::new(&binary);
    if let Some(config_path) = context.config_path() {
        command.env("PALYRA_CONFIG", config_path);
    }
    command.env("PALYRA_STATE_ROOT", runtime_root.effective.as_path());

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
    if action == "restart" {
        if let Some(status) = request_desktop_managed_gateway_restart(
            context.state_root(),
            context.config_path(),
            || verify_desktop_managed_gateway_restart_health(&context),
        )? {
            return emit_gateway_service_status(format!("gateway.{action}").as_str(), &status);
        }
    }
    let status = match action {
        "start" => support::service::start_gateway_service(context.state_root()),
        "stop" => support::service::stop_gateway_service(context.state_root()),
        "restart" => support::service::restart_gateway_service(context.state_root()),
        "uninstall" => support::service::uninstall_gateway_service(context.state_root()),
        _ => anyhow::bail!("unsupported gateway service action `{action}`"),
    }
    .map_err(|error| enrich_gateway_service_action_error(context.state_root(), action, error))?;
    emit_gateway_service_status(format!("gateway.{action}").as_str(), &status)
}

fn request_desktop_managed_gateway_restart(
    requested_state_root: &Path,
    config_path: Option<&Path>,
    verify_health: impl FnOnce() -> Result<HealthResponse>,
) -> Result<Option<support::service::GatewayServiceStatus>> {
    // Only desktop-managed runtimes without their own installed service are
    // restarted by touching the config; the desktop supervisor watches that
    // file and reloads the runtime. Anything else takes the service-manager path.
    if !desktop_runtime_state_root(requested_state_root).is_dir()
        || support::service::load_service_metadata(requested_state_root)?.is_some()
    {
        return Ok(None);
    }

    let config_path = config_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| app::state_root_config_path(requested_state_root));
    if !config_path.is_file() {
        return Ok(None);
    }
    touch_file(config_path.as_path()).with_context(|| {
        format!(
            "failed to request desktop-managed gateway restart through config {}",
            config_path.display()
        )
    })?;
    let desktop_runtime = desktop_runtime_state_root(requested_state_root);
    let health = verify_health().with_context(|| {
        format!(
            "desktop-managed gateway restart was requested by updating {}, but runtime health was not confirmed. Restart the desktop app/test harness or run `palyra --state-root \"{}\" gateway run`.",
            config_path.display(),
            desktop_runtime.display()
        )
    })?;
    if !health.status.eq_ignore_ascii_case("ok") {
        anyhow::bail!(
            "desktop-managed gateway restart was requested by updating {}, but runtime health returned status '{}'. Restart the desktop app/test harness or run `palyra --state-root \"{}\" gateway run`.",
            config_path.display(),
            health.status,
            desktop_runtime.display()
        );
    }

    Ok(Some(support::service::GatewayServiceStatus {
        installed: true,
        running: true,
        enabled: true,
        manager: "desktop-control-center".to_owned(),
        service_name: "desktop-managed-palyrad".to_owned(),
        definition_path: None,
        stdout_log_path: None,
        stderr_log_path: None,
        detail: Some(format!(
            "desktop-managed gateway restart requested by updating {}; HTTP health verified (/healthz) for service {} after supervisor reload; full gRPC/admin readiness may continue warming for a short interval",
            config_path.display(),
            health.service
        )),
    }))
}

fn verify_desktop_managed_gateway_restart_health(
    context: &app::RootCommandContext,
) -> Result<HealthResponse> {
    let connection = context.resolve_http_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::USER,
    )?;
    let status_url = format!("{}/healthz", connection.base_url.trim_end_matches('/'));
    wait_for_desktop_managed_gateway_health(
        status_url.as_str(),
        DESKTOP_MANAGED_RESTART_HEALTH_TIMEOUT,
        DESKTOP_MANAGED_RESTART_HEALTH_POLL_INTERVAL,
    )
    .with_context(|| format!("desktop-managed gateway did not become healthy at {status_url}"))
}

fn wait_for_desktop_managed_gateway_health(
    status_url: &str,
    timeout: std::time::Duration,
    poll_interval: std::time::Duration,
) -> Result<HealthResponse> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let started = std::time::Instant::now();

    loop {
        match client
            .get(status_url)
            .send()
            .context("failed to call daemon health endpoint")
            .and_then(|response| {
                response
                    .error_for_status()
                    .context("daemon health endpoint returned non-success status")
            })
            .and_then(|response| response.json().context("failed to parse daemon health payload"))
        {
            Ok(health) => return Ok(health),
            Err(error) if started.elapsed() >= timeout => return Err(error),
            Err(_) => {}
        }

        std::thread::sleep(poll_interval);
    }
}

/// Bumps the file's mtime by rewriting its current bytes; std offers no
/// portable utimes API, and the contents must stay identical.
fn touch_file(path: &Path) -> Result<()> {
    let contents = fs::read(path)
        .with_context(|| format!("failed to read file before touching {}", path.display()))?;
    fs::write(path, contents.as_slice())
        .with_context(|| format!("failed to touch file {}", path.display()))
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

fn run_gateway_status(url: Option<String>, json: bool) -> Result<()> {
    let context = root_context()?;
    let runtime_root = gateway_runtime_root_selection(context.state_root());
    let explicit_url = url.is_some();
    let connection = context.resolve_http_connection(
        app::ConnectionOverrides { daemon_url: url, ..app::ConnectionOverrides::default() },
        app::ConnectionDefaults::USER,
    )?;
    let status_url = format!("{}/healthz", connection.base_url.trim_end_matches('/'));
    let daemon_url = connection.base_url.clone();
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let service = support::service::query_gateway_service_status(runtime_root.effective.as_path())
        .unwrap_or(support::service::GatewayServiceStatus {
            installed: false,
            running: false,
            enabled: false,
            manager: "unknown".to_owned(),
            service_name: support::service::default_service_name(),
            definition_path: None,
            stdout_log_path: None,
            stderr_log_path: None,
            detail: Some("service status unavailable".to_owned()),
        });
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

    let report = GatewayStatusReport {
        daemon_url: daemon_url.clone(),
        state_root: runtime_root.requested.display().to_string(),
        effective_state_root: runtime_root.effective.display().to_string(),
        state_root_note: runtime_root.note.or_else(|| {
            gateway_status_state_root_scope_note(
                context.state_root_explicit(),
                context.config_path(),
                runtime_root.requested.as_path(),
                explicit_url,
                daemon_url.as_str(),
                health.is_some(),
                service.installed,
            )
        }),
        health,
        health_error,
        service,
    };
    if output::preferred_json(json) {
        return output::print_json_pretty(
            &report,
            "failed to encode gateway status output as JSON",
        );
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(&report, "failed to encode gateway status output as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }

    let runtime_health = report
        .health
        .as_ref()
        .and_then(|health| health.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    println!(
        "gateway.status daemon_url={} state_root={} effective_state_root={} runtime_running={} runtime_health={} service_installed={} service_running={} service_enabled={} manager={} service_name={}",
        report.daemon_url,
        report.state_root,
        report.effective_state_root,
        report.health.is_some(),
        runtime_health,
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
    if let Some(note) = report.state_root_note.as_deref() {
        println!("gateway.status.state_root_note={note}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn gateway_status_state_root_scope_note(
    state_root_explicit: bool,
    config_path: Option<&Path>,
    state_root: &Path,
    explicit_url: bool,
    daemon_url: &str,
    health_present: bool,
    service_installed: bool,
) -> Option<String> {
    if explicit_url
        || !state_root_explicit
        || !health_present
        || service_installed
        || config_path.is_some()
    {
        return None;
    }
    Some(format!(
        "requested state root has no config at {}; health check used default daemon_url {} and may reflect a different running runtime",
        app::state_root_config_path(state_root).display(),
        daemon_url
    ))
}

/// Prefers an existing `desktop-control-center/runtime` child as the effective
/// state root so gateway commands target the desktop-managed runtime.
fn gateway_runtime_root_selection(requested: &Path) -> GatewayRuntimeRootSelection {
    if is_desktop_runtime_state_root(requested) {
        return GatewayRuntimeRootSelection {
            requested: requested.to_path_buf(),
            effective: requested.to_path_buf(),
            note: None,
        };
    }
    let desktop_runtime = desktop_runtime_state_root(requested);
    if desktop_runtime.is_dir() {
        return GatewayRuntimeRootSelection {
            requested: requested.to_path_buf(),
            effective: desktop_runtime.clone(),
            note: Some(format!(
                "desktop-control-center runtime state detected under {}; gateway foreground/status commands target {}",
                requested.display(),
                desktop_runtime.display()
            )),
        };
    }
    GatewayRuntimeRootSelection {
        requested: requested.to_path_buf(),
        effective: requested.to_path_buf(),
        note: None,
    }
}

fn enrich_gateway_service_action_error(
    requested_state_root: &Path,
    action: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    let desktop_runtime = desktop_runtime_state_root(requested_state_root);
    if !desktop_runtime.is_dir() {
        return error;
    }
    anyhow!(
        "{error:#}\nDetected desktop-control-center runtime state root at {}. `palyra gateway {action}` controls only a managed service installed with `palyra gateway install`; for this desktop-managed runtime, restart the desktop app/test harness or run `palyra --state-root \"{}\" gateway run`.",
        desktop_runtime.display(),
        desktop_runtime.display()
    )
}

fn desktop_runtime_state_root(state_root: &Path) -> PathBuf {
    state_root.join("desktop-control-center").join("runtime")
}

fn is_desktop_runtime_state_root(state_root: &Path) -> bool {
    let Some(runtime_dir) = state_root.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(desktop_dir) =
        state_root.parent().and_then(|parent| parent.file_name()).and_then(|value| value.to_str())
    else {
        return false;
    };
    runtime_dir.eq_ignore_ascii_case("runtime")
        && desktop_dir.eq_ignore_ascii_case("desktop-control-center")
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
    if let Some(remote_assist) = read_remote_dashboard_assist_payload(&payload) {
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

fn read_remote_dashboard_assist_payload(payload: &Value) -> Option<&Value> {
    payload.get("remote_assist").filter(|value| value.is_object())
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
        // Truncating cast is acceptable: build_gateway_usage_cost_value clamps
        // days to 1..=365.
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
        let mut estimated_cost_raw = 0.0_f64;
        let mut estimated_count = 0_i64;
        if let Some(model_id) = routing_map.get(run_id.as_str()) {
            if let Some((input_rate, output_rate)) = pricing_map.get(model_id.as_str()) {
                let cost = input_rate.unwrap_or(0.0) * (prompt_tokens as f64 / 1_000_000.0)
                    + output_rate.unwrap_or(0.0) * (completion_tokens as f64 / 1_000_000.0);
                estimated_cost_raw = cost;
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
    }
    let daily = daily_map
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
            "Cost estimates reuse the shared pricing catalog stored in usage_pricing_catalog.",
            "Runs without a recorded routing decision stay excluded from estimated_cost_usd to avoid pretending certainty."
        ],
    }))
}

fn run_gateway_usage_cost(db_path: Option<String>, days: u32, json: bool) -> Result<()> {
    let payload = build_gateway_usage_cost_value(db_path, days)?;
    let context = root_context()?;
    if json || context.prefers_json() {
        return output::print_json_pretty(
            &payload,
            "failed to encode gateway usage-cost output as JSON",
        );
    }
    if output::preferred_ndjson(json, false) {
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
