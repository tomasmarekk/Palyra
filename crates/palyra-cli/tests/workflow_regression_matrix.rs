use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, ChildStderr, ChildStdout, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use palyra_cli::proto::palyra::browser::v1 as browser_v1;
use palyra_cli::proto::palyra::gateway::v1 as gateway_v1;
use reqwest::blocking::Client as BlockingClient;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tonic::{metadata::MetadataValue, transport::Endpoint, Request};
use ulid::Ulid;

const ADMIN_TOKEN: &str = "test-admin-token";
const BROWSER_AUTH_TOKEN: &str = "test-browser-token";
const BROWSER_STATE_KEY_B64: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const STARTUP_RETRY_ATTEMPTS: usize = 8;

#[test]
fn local_remote_and_lifecycle_workflows_are_regression_tested() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let local_config = workdir.path().join("config").join("palyra.toml");
    let remote_config = workdir.path().join("remote").join("palyra.toml");
    let workspace_root = workdir.path().join("workspace");
    fs::create_dir_all(workspace_root.as_path())
        .with_context(|| format!("failed to create {}", workspace_root.display()))?;
    fs::write(workspace_root.join("README.md"), "workflow regression fixture\n")
        .with_context(|| format!("failed to seed {}", workspace_root.display()))?;
    fs::create_dir_all(workdir.path().join("state-root").join("cache"))
        .with_context(|| format!("failed to seed state root under {}", workdir.path().display()))?;
    fs::write(
        workdir.path().join("state-root").join("cache").join("marker.txt"),
        "state fixture\n",
    )
    .with_context(|| format!("failed to seed state fixture under {}", workdir.path().display()))?;

    let local_config_string = local_config.display().to_string();
    let remote_config_string = remote_config.display().to_string();
    let workspace_root_string = workspace_root.display().to_string();
    let cert_path = workdir.path().join("tls").join("gateway.crt");
    let key_path = workdir.path().join("tls").join("gateway.key");
    let cert_path_string = cert_path.display().to_string();
    let key_path_string = key_path.display().to_string();
    let gateway_ca_pin = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let server_cert_pin = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    let setup_output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            local_config_string.as_str(),
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--skip-health",
            "--skip-channels",
            "--skip-skills",
            "--json",
        ],
        &[("OPENAI_API_KEY", "sk-test-workflow")],
    )?;
    let setup_payload = assert_json_success(setup_output, "setup wizard")?;
    assert_eq!(setup_payload.get("status").and_then(Value::as_str), Some("complete"));
    assert_eq!(setup_payload.get("flow").and_then(Value::as_str), Some("quickstart"));
    assert!(local_config.exists(), "setup wizard should create local config");

    let remote_output = run_cli(
        &workdir,
        &[
            "onboarding",
            "wizard",
            "--path",
            remote_config_string.as_str(),
            "--flow",
            "remote",
            "--non-interactive",
            "--accept-risk",
            "--remote-base-url",
            "https://dashboard.example.com/",
            "--remote-verification",
            "server-cert",
            "--pinned-server-cert-sha256",
            server_cert_pin,
            "--admin-token-env",
            "PALYRA_REMOTE_ADMIN_TOKEN",
            "--skip-health",
            "--skip-channels",
            "--skip-skills",
            "--json",
        ],
        &[("PALYRA_REMOTE_ADMIN_TOKEN", "remote-admin-token")],
    )?;
    let remote_payload = assert_json_success(remote_output, "remote onboarding")?;
    assert_eq!(remote_payload.get("flow").and_then(Value::as_str), Some("remote"));
    assert_eq!(
        remote_payload.get("remote_verification").and_then(Value::as_str),
        Some("server_cert")
    );
    assert!(remote_config.exists(), "remote onboarding should create remote config");

    let configure_output = run_cli(
        &workdir,
        &[
            "configure",
            "--path",
            local_config_string.as_str(),
            "--section",
            "gateway",
            "--non-interactive",
            "--accept-risk",
            "--bind-profile",
            "public-tls",
            "--daemon-port",
            "7310",
            "--grpc-port",
            "7610",
            "--quic-port",
            "7611",
            "--tls-scaffold",
            "bring-your-own",
            "--tls-cert-path",
            cert_path_string.as_str(),
            "--tls-key-path",
            key_path_string.as_str(),
            "--remote-base-url",
            "https://dashboard.example.com/",
            "--remote-verification",
            "gateway-ca",
            "--pinned-gateway-ca-sha256",
            gateway_ca_pin,
            "--json",
        ],
        &[],
    )?;
    let configure_payload = assert_json_success(configure_output, "configure gateway")?;
    assert!(
        configure_payload
            .get("changed_sections")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some("gateway"))),
        "configure should report the gateway section change"
    );

    let dashboard_output =
        run_cli(&workdir, &["dashboard", "--path", local_config_string.as_str(), "--json"], &[])?;
    let dashboard_payload = assert_json_success(dashboard_output, "dashboard")?;
    assert_eq!(dashboard_payload.get("source").and_then(Value::as_str), Some("config_remote_url"));
    assert_eq!(
        dashboard_payload.get("url").and_then(Value::as_str),
        Some("https://dashboard.example.com/")
    );

    let backup_archive = workdir.path().join("artifacts").join("workflow-backup.zip");
    let backup_archive_string = backup_archive.display().to_string();
    let backup_create_output = run_cli_json(
        &workdir,
        &[
            "backup",
            "create",
            "--output",
            backup_archive_string.as_str(),
            "--config-path",
            local_config_string.as_str(),
            "--workspace-root",
            workspace_root_string.as_str(),
            "--include",
            "workspace",
            "--include-support-bundle",
            "--force",
        ],
        &[],
    )?;
    let backup_create_payload = assert_json_success(backup_create_output, "backup create")?;
    assert_eq!(
        backup_create_payload.get("included_workspace").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        backup_create_payload.get("included_support_bundle").and_then(Value::as_bool),
        Some(true)
    );
    assert!(backup_archive.is_file(), "backup create should produce an archive");

    let backup_verify_output = run_cli_json(
        &workdir,
        &["backup", "verify", "--archive", backup_archive_string.as_str()],
        &[],
    )?;
    let backup_verify_payload = assert_json_success(backup_verify_output, "backup verify")?;
    assert_eq!(backup_verify_payload.get("ok").and_then(Value::as_bool), Some(true));

    let install_root = workdir.path().join("portable-install");
    seed_install_root(install_root.as_path())?;
    let install_root_string = install_root.display().to_string();
    let canonical_install_root = install_root.canonicalize()?;

    let update_output = run_cli_json(
        &workdir,
        &["update", "--install-root", install_root_string.as_str(), "--check"],
        &[],
    )?;
    let update_payload = assert_json_success(update_output, "update check")?;
    assert_eq!(update_payload.get("mode").and_then(Value::as_str), Some("status-check"));
    assert!(update_payload
        .get("next_steps")
        .and_then(Value::as_array)
        .is_some_and(|steps| !steps.is_empty()));

    let uninstall_output = run_cli_json(
        &workdir,
        &["uninstall", "--install-root", install_root_string.as_str(), "--dry-run"],
        &[],
    )?;
    let uninstall_payload = assert_json_success(uninstall_output, "uninstall dry-run")?;
    assert_eq!(uninstall_payload.get("dry_run").and_then(Value::as_bool), Some(true));
    assert_eq!(
        uninstall_payload.get("install_root").and_then(Value::as_str),
        Some(canonical_install_root.display().to_string().as_str())
    );

    let reset_output = run_cli_json(
        &workdir,
        &[
            "reset",
            "--scope",
            "config",
            "--scope",
            "state",
            "--config-path",
            local_config_string.as_str(),
            "--yes",
        ],
        &[],
    )?;
    let reset_payload = assert_json_success(reset_output, "reset")?;
    let actions = reset_payload
        .get("actions")
        .and_then(Value::as_array)
        .context("reset output should include actions array")?;
    assert_eq!(actions.len(), 2);
    for action in actions {
        assert_eq!(action.get("applied").and_then(Value::as_bool), Some(true));
        let destination = action
            .get("destination")
            .and_then(Value::as_str)
            .context("reset action should include destination")?;
        assert!(Path::new(destination).exists());
    }
    assert!(!local_config.exists());
    assert!(!workdir.path().join("state-root").exists());

    Ok(())
}

#[test]
fn browser_channels_and_session_workflows_are_regression_tested() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let browser_state_dir = workdir.path().join("browserd-state");
    let (browserd_child, browser_health_port, browser_grpc_port) =
        spawn_browserd_with_dynamic_ports(browser_state_dir.as_path())?;
    let _browserd = ChildGuard::new(browserd_child);
    let (daemon_child, admin_port, grpc_port) =
        spawn_palyrad_with_dynamic_ports(Some(browser_grpc_port), Some(BROWSER_AUTH_TOKEN))?;
    let _daemon = ChildGuard::new(daemon_child);

    let base_url = format!("http://127.0.0.1:{admin_port}");
    let browser_endpoint = format!("http://127.0.0.1:{browser_grpc_port}");
    let browser_health_url = format!("http://127.0.0.1:{browser_health_port}");
    let gateway_grpc_url = format!("http://127.0.0.1:{grpc_port}");
    let browser_config_path = workdir.path().join("browser-workflow").join("palyra.toml");
    write_browser_workflow_config(browser_config_path.as_path(), admin_port, grpc_port)?;
    let browser_config_path_string = browser_config_path.display().to_string();
    let browser_cli_env = browser_workflow_envs(
        browser_config_path_string.as_str(),
        gateway_grpc_url.as_str(),
        browser_endpoint.as_str(),
        BROWSER_AUTH_TOKEN,
    );

    let channel_status_output = run_cli(
        &workdir,
        &[
            "channels",
            "discord",
            "status",
            "--url",
            base_url.as_str(),
            "--token",
            ADMIN_TOKEN,
            "--principal",
            "admin:local",
            "--device-id",
            DEVICE_ID,
            "--channel",
            "cli",
        ],
        &browser_cli_env,
    )?;
    assert_success(&channel_status_output, "channels discord status")?;
    let channel_status_stdout = String::from_utf8(channel_status_output.stdout)
        .context("status stdout was not valid UTF-8")?;
    assert!(
        channel_status_stdout.contains("channels.status id=discord:default availability=supported")
    );

    let channel_refresh_output = run_cli(
        &workdir,
        &[
            "channels",
            "discord",
            "health-refresh",
            "--url",
            base_url.as_str(),
            "--token",
            ADMIN_TOKEN,
            "--principal",
            "admin:local",
            "--device-id",
            DEVICE_ID,
            "--channel",
            "cli",
            "--json",
        ],
        &browser_cli_env,
    )?;
    let channel_refresh_payload =
        assert_json_success(channel_refresh_output, "channels discord health-refresh")?;
    assert_eq!(
        channel_refresh_payload.pointer("/connector/connector_id").and_then(Value::as_str),
        Some("discord:default")
    );

    let channel_verify_output = run_cli_json(
        &workdir,
        &[
            "channels",
            "discord",
            "verify",
            "--account-id",
            "default",
            "--to",
            "channel:1234567890",
            "--text",
            "workflow regression verify",
            "--confirm",
            "--url",
            base_url.as_str(),
            "--token",
            ADMIN_TOKEN,
            "--principal",
            "admin:local",
            "--device-id",
            DEVICE_ID,
            "--channel",
            "cli",
        ],
        &browser_cli_env,
    )?;
    let channel_verify_payload =
        assert_json_success(channel_verify_output, "channels discord verify")?;
    assert!(
        channel_verify_payload.get("dispatch").is_some(),
        "channels discord verify should return dispatch payload"
    );
    assert!(
        channel_verify_payload.get("status").is_some(),
        "channels discord verify should return status payload"
    );

    let browser_status_output = run_cli_json(
        &workdir,
        &[
            "browser",
            "status",
            "--endpoint",
            browser_endpoint.as_str(),
            "--health-url",
            browser_health_url.as_str(),
            "--token",
            BROWSER_AUTH_TOKEN,
        ],
        &browser_cli_env,
    )?;
    let browser_status_payload = assert_json_success(browser_status_output, "browser status")?;
    assert_eq!(browser_status_payload.get("health_ok").and_then(Value::as_bool), Some(true));
    assert_eq!(browser_status_payload.get("grpc_ok").and_then(Value::as_bool), Some(true));

    let session_list_before =
        run_cli_json(&workdir, &["browser", "session", "list"], &browser_cli_env)?;
    let session_list_before_payload =
        assert_json_success(session_list_before, "browser session list before")?;
    assert_eq!(
        session_list_before_payload.get("sessions").and_then(Value::as_array).map(Vec::len),
        Some(0)
    );

    let session_create_output = run_cli_json(
        &workdir,
        &[
            "browser",
            "session",
            "create",
            "--principal",
            "user:ops",
            "--channel",
            "cli",
            "--allow-private-targets",
        ],
        &browser_cli_env,
    )?;
    let session_create_payload =
        assert_json_success(session_create_output, "browser session create")?;
    assert_eq!(session_create_payload.get("principal").and_then(Value::as_str), Some("user:ops"));
    assert_eq!(session_create_payload.get("profile_id").and_then(Value::as_str), None);
    assert_eq!(
        session_create_payload.get("persistence_enabled").and_then(Value::as_bool),
        Some(false)
    );
    let redacted_session_id = session_create_payload
        .get("session_id")
        .and_then(Value::as_str)
        .context("browser session create should return redacted session_id text")?;
    assert!(redacted_session_id.starts_with("session-"));
    let session_id = latest_browser_session_id(browser_endpoint.as_str(), BROWSER_AUTH_TOKEN)?;

    let session_list_after =
        run_cli_json(&workdir, &["browser", "session", "list"], &browser_cli_env)?;
    let session_list_after_payload =
        assert_json_success(session_list_after, "browser session list after")?;
    assert_eq!(
        session_list_after_payload.get("sessions").and_then(Value::as_array).map(Vec::len),
        Some(1)
    );
    let sessions = session_list_after_payload
        .get("sessions")
        .and_then(Value::as_array)
        .context("browser session list after should include sessions")?;
    assert_eq!(
        sessions.first().and_then(|session| session.get("principal")).and_then(Value::as_str),
        Some("user:ops")
    );
    assert_eq!(
        sessions.first().and_then(|session| session.get("channel")).and_then(Value::as_str),
        Some("cli")
    );
    assert_eq!(
        sessions.first().and_then(|session| session.get("profile_id")).and_then(Value::as_str),
        None
    );

    let session_show_output = run_cli_json(
        &workdir,
        &["browser", "session", "show", session_id.as_str()],
        &browser_cli_env,
    )?;
    let session_show_payload = assert_json_success(session_show_output, "browser session show")?;
    assert_eq!(
        session_show_payload.pointer("/summary/channel").and_then(Value::as_str),
        Some("cli")
    );

    let tabs_output = run_cli_json(
        &workdir,
        &["browser", "tabs", session_id.as_str(), "list"],
        &browser_cli_env,
    )?;
    let tabs_payload = assert_json_success(tabs_output, "browser tabs list")?;
    assert_eq!(tabs_payload.get("success").and_then(Value::as_bool), Some(true));
    assert!(tabs_payload.get("tabs").and_then(Value::as_array).is_some_and(|tabs| tabs.len() == 1));

    let fixture = StaticHttpFixture::new(
        "<!doctype html><html><head><title>Workflow Fixture</title></head><body><main><h1>Browser Matrix</h1><p id=\"status\">browser matrix ready</p></main></body></html>",
    )?;
    let preflight_status = BlockingClient::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build fixture preflight client")?
        .get(fixture.url())
        .send()
        .context("fixture preflight request should succeed")?
        .error_for_status()
        .context("fixture preflight response should be successful")?
        .status();
    assert!(preflight_status.is_success(), "fixture preflight should return success status");
    let navigate_output = run_cli_json(
        &workdir,
        &[
            "browser",
            "navigate",
            session_id.as_str(),
            "--url",
            fixture.url(),
            "--timeout-ms",
            "2000",
            "--allow-private-targets",
        ],
        &browser_cli_env,
    )?;
    let navigate_payload = assert_json_success(navigate_output, "browser navigate")?;
    assert_eq!(navigate_payload.get("success").and_then(Value::as_bool), Some(true));
    assert_eq!(navigate_payload.get("final_url").and_then(Value::as_str), Some(fixture.url()));
    assert_eq!(navigate_payload.get("title").and_then(Value::as_str), Some("Workflow Fixture"));

    let wait_output = run_cli_json(
        &workdir,
        &[
            "browser",
            "wait",
            session_id.as_str(),
            "--text",
            "browser matrix ready",
            "--timeout-ms",
            "2000",
        ],
        &browser_cli_env,
    )?;
    let wait_payload = assert_json_success(wait_output, "browser wait")?;
    assert_eq!(wait_payload.get("success").and_then(Value::as_bool), Some(true));
    assert_eq!(
        wait_payload.get("matched_text").and_then(Value::as_str),
        Some("browser matrix ready")
    );

    let snapshot_path = workdir.path().join("artifacts").join("browser-snapshot.json");
    let snapshot_path_string = snapshot_path.display().to_string();
    let snapshot_output = run_cli_json(
        &workdir,
        &[
            "browser",
            "snapshot",
            session_id.as_str(),
            "--include-dom-snapshot",
            "--include-visible-text",
            "--output",
            snapshot_path_string.as_str(),
        ],
        &browser_cli_env,
    )?;
    let snapshot_payload = assert_json_success(snapshot_output, "browser snapshot")?;
    assert_eq!(snapshot_payload.get("success").and_then(Value::as_bool), Some(true));
    assert_eq!(
        snapshot_payload.get("output_path").and_then(Value::as_str),
        Some(snapshot_path_string.as_str())
    );
    assert!(snapshot_path.is_file(), "browser snapshot should write an artifact");
    let snapshot_artifact = serde_json::from_slice::<Value>(
        &fs::read(snapshot_path.as_path())
            .with_context(|| format!("failed to read {}", snapshot_path.display()))?,
    )
    .context("browser snapshot artifact should contain valid JSON")?;
    assert_eq!(snapshot_artifact.get("page_url").and_then(Value::as_str), Some(fixture.url()));

    let screenshot_path = workdir.path().join("artifacts").join("browser-screenshot.png");
    let screenshot_path_string = screenshot_path.display().to_string();
    let screenshot_output = run_cli_json(
        &workdir,
        &[
            "browser",
            "screenshot",
            session_id.as_str(),
            "--output",
            screenshot_path_string.as_str(),
        ],
        &browser_cli_env,
    )?;
    let screenshot_payload = assert_json_success(screenshot_output, "browser screenshot")?;
    assert_eq!(screenshot_payload.get("success").and_then(Value::as_bool), Some(true));
    assert_eq!(
        screenshot_payload.get("output_path").and_then(Value::as_str),
        Some(screenshot_path_string.as_str())
    );
    assert_eq!(screenshot_payload.get("mime_type").and_then(Value::as_str), Some("image/png"));
    assert!(screenshot_path.is_file(), "browser screenshot should write an artifact");
    assert!(
        fs::metadata(screenshot_path.as_path())
            .with_context(|| format!("failed to stat {}", screenshot_path.display()))?
            .len()
            > 0
    );

    let network_output = run_cli_json(
        &workdir,
        &["browser", "network", session_id.as_str(), "--include-headers", "--limit", "10"],
        &browser_cli_env,
    )?;
    let network_payload = assert_json_success(network_output, "browser network")?;
    assert_eq!(network_payload.get("success").and_then(Value::as_bool), Some(true));
    assert!(network_payload.get("entries").and_then(Value::as_array).is_some());
    assert_eq!(network_payload.pointer("/page/limit").and_then(Value::as_u64), Some(10));

    let trace_path = workdir.path().join("artifacts").join("browser-trace.json");
    let trace_path_string = trace_path.display().to_string();
    let trace_output = run_cli_json(
        &workdir,
        &["browser", "trace", session_id.as_str(), "--output", trace_path_string.as_str()],
        &browser_cli_env,
    )?;
    let trace_payload = assert_json_success(trace_output, "browser trace")?;
    assert_eq!(
        trace_payload.get("output_path").and_then(Value::as_str),
        Some(trace_path_string.as_str())
    );
    assert!(trace_path.is_file(), "browser trace should write an artifact");
    let trace_artifact = serde_json::from_slice::<Value>(
        &fs::read(trace_path.as_path())
            .with_context(|| format!("failed to read {}", trace_path.display()))?,
    )
    .context("browser trace artifact should contain valid JSON")?;
    assert!(
        trace_artifact
            .get("action_log")
            .and_then(Value::as_array)
            .is_some_and(|entries| !entries.is_empty()),
        "browser trace should capture action log entries"
    );
    assert!(
        trace_artifact
            .get("network_log")
            .and_then(Value::as_array)
            .is_some_and(|entries| !entries.is_empty()),
        "browser trace should capture network log entries"
    );

    let inspect_path = workdir.path().join("artifacts").join("browser-inspect.json");
    let inspect_path_string = inspect_path.display().to_string();
    let inspect_output = run_cli_json(
        &workdir,
        &[
            "browser",
            "session",
            "inspect",
            session_id.as_str(),
            "--include-action-log",
            "--include-network-log",
            "--include-page-snapshot",
            "--output",
            inspect_path_string.as_str(),
        ],
        &browser_cli_env,
    )?;
    let inspect_payload = assert_json_success(inspect_output, "browser session inspect")?;
    assert_eq!(
        inspect_payload.get("output_path").and_then(Value::as_str),
        Some(inspect_path_string.as_str())
    );
    assert!(inspect_path.is_file(), "browser session inspect should write an artifact");
    let inspect_artifact = serde_json::from_slice::<Value>(
        &fs::read(inspect_path.as_path())
            .with_context(|| format!("failed to read {}", inspect_path.display()))?,
    )
    .context("browser session inspect artifact should contain valid JSON")?;
    assert_eq!(
        inspect_artifact.pointer("/session/summary/principal").and_then(Value::as_str),
        Some("user:ops")
    );
    assert!(
        inspect_artifact
            .get("network_log")
            .and_then(Value::as_array)
            .is_some_and(|entries| !entries.is_empty()),
        "browser session inspect should capture network log entries"
    );

    let sessions_resolve_output = run_cli_json(
        &workdir,
        &[
            "sessions",
            "resolve",
            "--session-key",
            "workflow:browser",
            "--session-label",
            "Workflow Browser",
            "--json",
        ],
        &browser_cli_env,
    )?;
    let sessions_resolve_payload =
        assert_json_success(sessions_resolve_output, "sessions resolve")?;
    assert_eq!(sessions_resolve_payload.get("created").and_then(Value::as_bool), Some(true));
    assert_eq!(sessions_resolve_payload.get("reset_applied").and_then(Value::as_bool), Some(false));
    let gateway_session_id = resolve_gateway_session_id(
        gateway_grpc_url.as_str(),
        ADMIN_TOKEN,
        "admin:local",
        DEVICE_ID,
        "cli",
        "workflow:browser",
    )?;

    let sessions_show_output = run_cli_json(
        &workdir,
        &["sessions", "show", "--session-key", "workflow:browser", "--json"],
        &browser_cli_env,
    )?;
    let sessions_show_payload = assert_json_success(sessions_show_output, "sessions show")?;
    assert_eq!(sessions_show_payload.get("created").and_then(Value::as_bool), Some(false));
    assert_eq!(sessions_show_payload.get("reset_applied").and_then(Value::as_bool), Some(false));
    assert!(sessions_show_payload.get("session").is_some());

    let sessions_reset_output = run_cli_json(
        &workdir,
        &["sessions", "reset", gateway_session_id.as_str(), "--json"],
        &browser_cli_env,
    )?;
    let sessions_reset_payload = assert_json_success(sessions_reset_output, "sessions reset")?;
    assert_eq!(sessions_reset_payload.get("reset_applied").and_then(Value::as_bool), Some(true));

    let browser_close_output = run_cli_json(
        &workdir,
        &["browser", "session", "close", session_id.as_str()],
        &browser_cli_env,
    )?;
    let browser_close_payload = assert_json_success(browser_close_output, "browser session close")?;
    assert_eq!(browser_close_payload.get("closed").and_then(Value::as_bool), Some(true));

    Ok(())
}

fn browser_workflow_envs<'a>(
    config_path: &'a str,
    gateway_grpc_url: &'a str,
    browser_grpc_url: &'a str,
    browser_auth_token: &'a str,
) -> [(&'a str, &'a str); 7] {
    [
        ("PALYRA_CONFIG", config_path),
        ("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN),
        ("PALYRA_ADMIN_BOUND_PRINCIPAL", "admin:local"),
        ("PALYRA_GATEWAY_GRPC_URL", gateway_grpc_url),
        ("PALYRA_BROWSER_SERVICE_ENABLED", "true"),
        ("PALYRA_BROWSER_SERVICE_ENDPOINT", browser_grpc_url),
        ("PALYRA_BROWSER_SERVICE_AUTH_TOKEN", browser_auth_token),
    ]
}

fn write_browser_workflow_config(path: &Path, admin_port: u16, grpc_port: u16) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(
        path,
        format!(
            "version = 1\n[daemon]\nbind_addr = \"127.0.0.1\"\nport = {admin_port}\n[gateway]\ngrpc_bind_addr = \"127.0.0.1\"\ngrpc_port = {grpc_port}\n"
        ),
    )
    .with_context(|| format!("failed to write browser workflow config {}", path.display()))
}

fn configure_cli_env(command: &mut Command, workdir: &TempDir) {
    command
        .env("PALYRA_STATE_ROOT", workdir.path().join("state-root"))
        .env("PALYRA_VAULT_DIR", workdir.path().join("vault"))
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env("XDG_STATE_HOME", workdir.path().join("xdg-state"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"));
}

fn run_cli(workdir: &TempDir, args: &[&str], envs: &[(&str, &str)]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

fn run_cli_json(workdir: &TempDir, args: &[&str], envs: &[(&str, &str)]) -> Result<Output> {
    let mut json_args = vec!["--output-format", "json"];
    json_args.extend_from_slice(args);
    run_cli(workdir, json_args.as_slice(), envs)
}

fn assert_success(output: &Output, command_name: &str) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    anyhow::bail!("{command_name} failed: {}", String::from_utf8_lossy(&output.stderr))
}

fn assert_json_success(output: Output, command_name: &str) -> Result<Value> {
    assert_success(&output, command_name)?;
    serde_json::from_slice::<Value>(&output.stdout)
        .with_context(|| format!("{command_name} stdout should be valid JSON"))
}

fn seed_install_root(install_root: &Path) -> Result<()> {
    fs::create_dir_all(install_root)
        .with_context(|| format!("failed to create {}", install_root.display()))?;
    fs::write(
        install_root.join("install-metadata.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "artifact_kind": "headless",
            "installed_at_utc": "2026-03-28T00:00:00Z",
            "install_root": install_root.display().to_string(),
            "state_root": install_root.join("state").display().to_string(),
            "cli_exposure": {
                "command_name": "palyra",
                "command_root": install_root.join("bin").display().to_string(),
                "command_path": install_root.join("bin").join("palyra.cmd").display().to_string(),
                "shim_paths": [],
                "target_binary_path": install_root.join("palyra.exe").display().to_string(),
                "session_path_updated": false,
                "persistent_path_requested": false,
                "persistence_strategy": "none",
                "user_path_updated": false,
                "profile_files": [],
            }
        }))?,
    )
    .with_context(|| format!("failed to write install metadata in {}", install_root.display()))?;
    fs::write(
        install_root.join("release-manifest.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "generated_at_utc": "2026-03-28T00:00:00Z",
            "artifact_kind": "headless",
            "artifact_name": "palyra-headless",
            "version": "0.4.0",
            "platform": "windows-x64",
            "install_mode": "portable",
            "source_sha": null,
            "binaries": [],
            "packaging_boundaries": {
                "excluded_patterns": ["state/**"]
            }
        }))?,
    )
    .with_context(|| format!("failed to write release manifest in {}", install_root.display()))?;
    fs::write(
        install_root.join("ROLLBACK.txt"),
        "Restore the previous archive if the candidate regresses.\n",
    )
    .with_context(|| format!("failed to write rollback hint in {}", install_root.display()))?;
    fs::write(
        install_root.join("MIGRATION_NOTES.txt"),
        "Run config migrate before restarting the gateway.\n",
    )
    .with_context(|| format!("failed to write migration notes in {}", install_root.display()))?;
    Ok(())
}

struct StaticHttpFixture {
    url: String,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl StaticHttpFixture {
    fn new(body: &str) -> Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("failed to bind fixture listener")?;
        let address =
            listener.local_addr().context("failed to resolve fixture listener address")?;
        let body = body.to_owned();
        let max_requests = 2usize;
        let handle = thread::spawn(move || -> Result<()> {
            for _ in 0..max_requests {
                let (mut stream, _) =
                    listener.accept().context("fixture listener failed to accept")?;
                let _ = read_http_request(&mut stream)?;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .context("fixture listener failed to write response")?;
                stream.flush().context("fixture listener failed to flush response")?;
            }
            Ok(())
        });
        Ok(Self { url: format!("http://{address}/"), handle: Some(handle) })
    }

    fn url(&self) -> &str {
        self.url.as_str()
    }
}

impl Drop for StaticHttpFixture {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let bytes_read = stream.read(&mut chunk).context("failed to read HTTP request bytes")?;
        if bytes_read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..bytes_read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    Ok(buffer)
}

fn latest_browser_session_id(grpc_url: &str, auth_token: &str) -> Result<String> {
    let runtime = Runtime::new().context("failed to create Tokio runtime")?;
    runtime.block_on(async move {
        let channel = Endpoint::from_shared(grpc_url.to_owned())
            .context("invalid browser gRPC endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .connect()
            .await
            .context("failed to connect browser gRPC endpoint")?;
        let mut client = browser_v1::browser_service_client::BrowserServiceClient::new(channel);
        let mut request = Request::new(browser_v1::ListSessionsRequest {
            v: 1,
            principal: String::new(),
            limit: 10,
        });
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {auth_token}"))
                .context("invalid authorization metadata")?,
        );
        let response =
            client.list_sessions(request).await.context("list_sessions RPC failed")?.into_inner();
        response
            .sessions
            .first()
            .and_then(|summary| summary.session_id.as_ref())
            .map(|value| value.ulid.clone())
            .context("browser service returned no sessions after CLI create")
    })
}

fn resolve_gateway_session_id(
    grpc_url: &str,
    admin_token: &str,
    principal: &str,
    device_id: &str,
    command_channel: &str,
    session_key: &str,
) -> Result<String> {
    let runtime = Runtime::new().context("failed to create Tokio runtime")?;
    runtime.block_on(async move {
        let grpc_channel = Endpoint::from_shared(grpc_url.to_owned())
            .context("invalid gateway gRPC endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .connect()
            .await
            .context("failed to connect gateway gRPC endpoint")?;
        let mut client =
            gateway_v1::gateway_service_client::GatewayServiceClient::new(grpc_channel);
        let mut request = Request::new(gateway_v1::ResolveSessionRequest {
            v: 1,
            session_id: None,
            session_key: session_key.to_owned(),
            session_label: String::new(),
            require_existing: true,
            reset_session: false,
        });
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {admin_token}"))
                .context("invalid authorization metadata")?,
        );
        request.metadata_mut().insert(
            "x-palyra-principal",
            MetadataValue::try_from(principal).context("invalid principal metadata")?,
        );
        request.metadata_mut().insert(
            "x-palyra-device-id",
            MetadataValue::try_from(device_id).context("invalid device-id metadata")?,
        );
        request.metadata_mut().insert(
            "x-palyra-channel",
            MetadataValue::try_from(command_channel).context("invalid channel metadata")?,
        );
        request.metadata_mut().insert(
            "x-palyra-trace-id",
            MetadataValue::try_from(Ulid::new().to_string())
                .context("invalid trace-id metadata")?,
        );
        let response = client
            .resolve_session(request)
            .await
            .context("resolve_session RPC failed")?
            .into_inner();
        response
            .session
            .as_ref()
            .and_then(|session| session.session_id.as_ref())
            .map(|value| value.ulid.clone())
            .context("gateway returned no session_id for workflow session")
    })
}

fn spawn_palyrad_with_dynamic_ports(
    browser_grpc_port: Option<u16>,
    browser_auth_token: Option<&str>,
) -> Result<(Child, u16, u16)> {
    let mut last_error = None;

    for attempt in 1..=STARTUP_RETRY_ATTEMPTS {
        let journal_db_path = unique_temp_path("palyra-cli-m59-journal", "sqlite3");
        let state_root_dir = unique_temp_dir_path("palyra-cli-m59-state-root");
        let vault_dir = state_root_dir.join("vault");
        let identity_store_dir = unique_temp_dir_path("palyra-cli-m59-identity");
        fs::create_dir_all(&state_root_dir)
            .with_context(|| format!("failed to create {}", state_root_dir.display()))?;
        fs::create_dir_all(&vault_dir)
            .with_context(|| format!("failed to create {}", vault_dir.display()))?;
        fs::create_dir_all(&identity_store_dir)
            .with_context(|| format!("failed to create {}", identity_store_dir.display()))?;

        let mut command = Command::new(resolve_workspace_binary_path("palyrad")?);
        command
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                "0",
                "--grpc-bind",
                "127.0.0.1",
                "--grpc-port",
                "0",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
            .env("PALYRA_ADMIN_BOUND_PRINCIPAL", "admin:local")
            .env("PALYRA_GATEWAY_QUIC_BIND_ADDR", "127.0.0.1")
            .env("PALYRA_GATEWAY_QUIC_PORT", "0")
            .env("PALYRA_STATE_ROOT", state_root_dir.display().to_string())
            .env("PALYRA_VAULT_BACKEND", "file")
            .env("PALYRA_VAULT_DIR", vault_dir.display().to_string())
            .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.display().to_string())
            .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.display().to_string())
            .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
            .env("PALYRA_MODEL_PROVIDER_KIND", "deterministic")
            .env("RUST_LOG", "info");

        if let Some(browser_grpc_port) = browser_grpc_port {
            command.env("PALYRA_BROWSER_SERVICE_ENABLED", "true");
            command.env(
                "PALYRA_BROWSER_SERVICE_ENDPOINT",
                format!("http://127.0.0.1:{browser_grpc_port}"),
            );
        }
        if let Some(browser_auth_token) = browser_auth_token {
            command.env("PALYRA_BROWSER_SERVICE_AUTH_TOKEN", browser_auth_token);
        }

        let mut child = command.spawn().context("failed to spawn palyrad process")?;
        let startup_result = child
            .stdout
            .take()
            .context("failed to capture palyrad stdout")
            .and_then(|stdout| wait_for_listen_ports(stdout, &mut child, STARTUP_TIMEOUT))
            .and_then(|(admin_port, grpc_port)| {
                wait_for_health(admin_port, &mut child, STARTUP_TIMEOUT)?;
                wait_for_tcp_listen(grpc_port, &mut child, STARTUP_TIMEOUT)?;
                Ok((admin_port, grpc_port))
            });
        match startup_result {
            Ok((admin_port, grpc_port)) => return Ok((child, admin_port, grpc_port)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                last_error = Some(error.context(format!(
                    "palyrad startup attempt {attempt}/{STARTUP_RETRY_ATTEMPTS} failed"
                )));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("failed to start palyrad for tests")))
}

fn spawn_browserd_with_dynamic_ports(state_dir: &Path) -> Result<(Child, u16, u16)> {
    let mut last_error = None;

    for attempt in 1..=STARTUP_RETRY_ATTEMPTS {
        fs::create_dir_all(state_dir)
            .with_context(|| format!("failed to create {}", state_dir.display()))?;
        let mut command = Command::new(resolve_workspace_binary_path("palyra-browserd")?);
        command
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                "0",
                "--grpc-bind",
                "127.0.0.1",
                "--grpc-port",
                "0",
                "--auth-token",
                BROWSER_AUTH_TOKEN,
                "--engine-mode",
                "simulated",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("PALYRA_BROWSERD_STATE_DIR", state_dir.display().to_string())
            .env("PALYRA_BROWSERD_STATE_ENCRYPTION_KEY", BROWSER_STATE_KEY_B64)
            .env("PALYRA_BROWSERD_ENGINE_MODE", "simulated")
            .env("RUST_LOG", "info");

        let mut child = command.spawn().context("failed to spawn palyra-browserd process")?;
        let startup_result = child
            .stdout
            .take()
            .context("failed to capture browserd stdout")
            .and_then(|stdout| wait_for_listen_ports(stdout, &mut child, STARTUP_TIMEOUT))
            .and_then(|(health_port, grpc_port)| {
                wait_for_health(health_port, &mut child, STARTUP_TIMEOUT)?;
                wait_for_tcp_listen(grpc_port, &mut child, STARTUP_TIMEOUT)?;
                Ok((health_port, grpc_port))
            });
        match startup_result {
            Ok((health_port, grpc_port)) => return Ok((child, health_port, grpc_port)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                last_error = Some(error.context(format!(
                    "browserd startup attempt {attempt}/{STARTUP_RETRY_ATTEMPTS} failed"
                )));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("failed to start browserd for tests")))
}

fn wait_for_listen_ports(
    stdout: ChildStdout,
    process: &mut Child,
    timeout: Duration,
) -> Result<(u16, u16)> {
    let (sender, receiver) = mpsc::channel::<Result<(u16, u16), String>>();
    thread::spawn(move || {
        let mut sender = Some(sender);
        let mut admin_port = None::<u16>;
        let mut grpc_port = None::<u16>;
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err("failed to read process stdout line".to_owned()));
                }
                return;
            };
            if admin_port.is_none() {
                admin_port = parse_port_from_log(&line, "\"listen_addr\":\"");
            }
            if grpc_port.is_none() {
                grpc_port = parse_port_from_log(&line, "\"grpc_listen_addr\":\"");
            }
            if let (Some(admin_port), Some(grpc_port)) = (admin_port, grpc_port) {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Ok((admin_port, grpc_port)));
                }
                return;
            }
        }
        if let Some(sender) = sender.take() {
            let _ = sender.send(Err(
                "process stdout closed before admin and gRPC listen addresses were published"
                    .to_owned(),
            ));
        }
    });

    let timeout_at = Instant::now() + timeout;
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(ports)) => return Ok(ports),
            Ok(Err(message)) => anyhow::bail!("{message}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("listen-address reader disconnected before publishing ports");
            }
        }

        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for process listen address logs");
        }
        if let Some(status) = process.try_wait().context("failed to check process status")? {
            let stderr = read_child_stderr(process.stderr.take());
            if stderr.is_empty() {
                anyhow::bail!(
                    "process exited before publishing listen addresses with status: {status}"
                );
            }
            anyhow::bail!(
                "process exited before publishing listen addresses with status: {status}; stderr: {stderr}"
            );
        }
    }
}

fn wait_for_health(port: u16, process: &mut Child, timeout: Duration) -> Result<()> {
    let timeout_at = Instant::now() + timeout;
    let url = format!("http://127.0.0.1:{port}/healthz");
    let client = BlockingClient::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .context("failed to build HTTP client")?;

    loop {
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for health endpoint on port {port}");
        }
        if let Some(status) = process.try_wait().context("failed to check process status")? {
            let stderr = read_child_stderr(process.stderr.take());
            if stderr.is_empty() {
                anyhow::bail!("process exited before becoming healthy with status: {status}");
            }
            anyhow::bail!(
                "process exited before becoming healthy with status: {status}; stderr: {stderr}"
            );
        }
        if client.get(&url).send().and_then(|response| response.error_for_status()).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_tcp_listen(port: u16, process: &mut Child, timeout: Duration) -> Result<()> {
    let timeout_at = Instant::now() + timeout;
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    loop {
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for TCP listener on port {port}");
        }
        if let Some(status) = process.try_wait().context("failed to check process status")? {
            let stderr = read_child_stderr(process.stderr.take());
            if stderr.is_empty() {
                anyhow::bail!(
                    "process exited before TCP listener became ready with status: {status}"
                );
            }
            anyhow::bail!(
                "process exited before TCP listener became ready with status: {status}; stderr: {stderr}"
            );
        }
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn read_child_stderr(stderr: Option<ChildStderr>) -> String {
    let Some(mut stderr) = stderr else {
        return String::new();
    };
    let mut output = String::new();
    if stderr.read_to_string(&mut output).is_err() {
        return String::new();
    }
    output.trim().to_owned()
}

fn parse_port_from_log(line: &str, prefix: &str) -> Option<u16> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|address| address.port())
}

fn resolve_workspace_binary_path(base_name: &str) -> Result<PathBuf> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .context("failed to resolve workspace root from CARGO_MANIFEST_DIR")?;
    let executable = if cfg!(windows) { format!("{base_name}.exe") } else { base_name.to_owned() };
    let path = workspace_root.join("target").join("debug").join(executable);
    if path.is_file() {
        Ok(path)
    } else {
        anyhow::bail!(
            "required test binary is missing: {} (build palyrad and palyra-browserd before running this test)",
            path.display()
        );
    }
}

fn unique_temp_path(prefix: &str, extension: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{}.{}",
        std::process::id(),
        Ulid::new(),
        extension
    ))
}

fn unique_temp_dir_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}-{}", std::process::id(), Ulid::new()))
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
