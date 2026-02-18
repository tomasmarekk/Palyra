mod cli;

pub mod proto {
    pub mod palyra {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("palyra.common.v1");
            }
        }

        pub mod gateway {
            pub mod v1 {
                tonic::include_proto!("palyra.gateway.v1");
            }
        }
    }
}

use std::{
    env, fs,
    io::{BufRead, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[cfg(not(windows))]
use std::{ffi::OsString, sync::Arc};

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use cli::{
    AgentCommand, BrowserCommand, ChannelsCommand, Cli, Command as CliCommand, CompletionShell,
    ConfigCommand, CronCommand, DaemonCommand, OnboardingCommand, PolicyCommand, ProtocolCommand,
};
#[cfg(not(windows))]
use cli::{PairingClientKindArg, PairingCommand, PairingMethodArg};
use palyra_common::{
    build_metadata,
    config_system::{
        backup_path, format_toml_value, get_value_at_path, parse_document_with_migration,
        parse_toml_value_literal, recover_config_from_backup, set_value_at_path,
        unset_value_at_path, write_document_with_backups, ConfigMigrationInfo,
    },
    daemon_config_schema::RootFileConfig,
    default_config_search_paths, parse_config_path, parse_daemon_bind_socket,
    validate_canonical_id, HealthResponse, CANONICAL_JSON_ENVELOPE_VERSION,
    CANONICAL_PROTOCOL_MAJOR,
};
#[cfg(not(windows))]
use palyra_identity::FilesystemSecretStore;
#[cfg(not(windows))]
use palyra_identity::{
    DeviceIdentity, IdentityManager, PairingClientKind, PairingMethod, SecretStore,
    DEFAULT_CERT_VALIDITY,
};
use palyra_policy::{evaluate_with_config, PolicyDecision, PolicyEvaluationConfig, PolicyRequest};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::sleep;
use tokio_stream::{iter, StreamExt};
use tonic::Request;
use ulid::Ulid;

use crate::proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};

const MAX_HEALTH_ATTEMPTS: usize = 3;
const BASE_HEALTH_BACKOFF_MS: u64 = 100;
const MAX_GRPC_ATTEMPTS: usize = 3;
const BASE_GRPC_BACKOFF_MS: u64 = 100;
const RUN_STREAM_REQUEST_VERSION: u32 = 1;
const DEFAULT_GATEWAY_GRPC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GATEWAY_GRPC_PORT: u16 = 7443;
const DEFAULT_GATEWAY_QUIC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GATEWAY_QUIC_PORT: u16 = 7444;
const DEFAULT_GATEWAY_QUIC_ENABLED: bool = true;
const DEFAULT_DAEMON_URL: &str = "http://127.0.0.1:7142";
const DEFAULT_BROWSER_URL: &str = "http://127.0.0.1:7143";
const DEFAULT_CHANNEL: &str = "cli";

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        CliCommand::Version => print_version(),
        CliCommand::Doctor { strict } => run_doctor(strict),
        CliCommand::Status { url, grpc_url, admin, token, principal, device_id, channel } => {
            run_status(url, grpc_url, admin, token, principal, device_id, channel)
        }
        CliCommand::Agent { command } => run_agent(command),
        CliCommand::Cron { command } => run_cron(command),
        CliCommand::Channels { command } => run_channels(command),
        CliCommand::Browser { command } => run_browser(command),
        CliCommand::Completion { shell } => run_completion(shell),
        CliCommand::Onboarding { command } => run_onboarding(command),
        CliCommand::Daemon { command } => run_daemon(command),
        CliCommand::Policy { command } => run_policy(command),
        CliCommand::Protocol { command } => run_protocol(command),
        CliCommand::Config { command } => run_config(command),
        #[cfg(not(windows))]
        CliCommand::Pairing { command } => run_pairing(command),
    }
}

fn print_version() -> Result<()> {
    let build = build_metadata();
    println!(
        "name=palyra version={} git_hash={} build_profile={}",
        build.version, build.git_hash, build.build_profile
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_doctor(strict: bool) -> Result<()> {
    let checks = [
        DoctorCheck {
            key: "toolchain_ok",
            ok: command_available("rustc", &["--version"]),
            required: true,
        },
        DoctorCheck {
            key: "cargo_ok",
            ok: command_available("cargo", &["--version"]),
            required: true,
        },
        DoctorCheck {
            key: "workspace_writable",
            ok: is_workspace_writable().unwrap_or(false),
            required: true,
        },
        DoctorCheck { key: "repo_scaffold_ok", ok: required_directories_ok(), required: true },
        DoctorCheck {
            key: "gitleaks_installed",
            ok: command_available("gitleaks", &["--version"]),
            required: false,
        },
        DoctorCheck {
            key: "cargo_audit_installed",
            ok: command_available("cargo", &["audit", "--version"]),
            required: true,
        },
        DoctorCheck {
            key: "cargo_deny_installed",
            ok: command_available("cargo", &["deny", "--version"]),
            required: true,
        },
        DoctorCheck {
            key: "cargo_cyclonedx_installed",
            ok: command_available("cargo", &["cyclonedx", "--version"]),
            required: false,
        },
        DoctorCheck {
            key: "osv_scanner_installed",
            ok: command_available("osv-scanner", &["--version"]),
            required: false,
        },
        DoctorCheck {
            key: "cargo_fuzz_installed",
            ok: command_available("cargo", &["fuzz", "--help"]),
            required: false,
        },
        DoctorCheck {
            key: "protoc_installed",
            ok: command_available("protoc", &["--version"])
                || command_available("protoc.exe", &["--version"]),
            required: true,
        },
        DoctorCheck {
            key: "swiftc_installed",
            ok: command_available("swiftc", &["--version"]),
            required: false,
        },
        DoctorCheck {
            key: "kotlinc_installed",
            ok: command_available("kotlinc", &["-version"]),
            required: false,
        },
        DoctorCheck {
            key: "just_installed",
            ok: command_available("just", &["--version"]),
            required: false,
        },
        DoctorCheck {
            key: "npm_installed",
            ok: command_available("npm", &["--version"]),
            required: false,
        },
        DoctorCheck {
            key: "swiftlint_installed",
            ok: command_available("swiftlint", &["version"]),
            required: false,
        },
        DoctorCheck {
            key: "detekt_installed",
            ok: command_available("detekt", &["--version"]),
            required: false,
        },
    ];

    for check in checks {
        println!("doctor.{}={} required={}", check.key, check.ok, check.required);
    }

    if strict {
        let failing_required = checks.iter().find(|check| check.required && !check.ok);
        if let Some(check) = failing_required {
            anyhow::bail!("strict doctor failed: {}", check.key);
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn run_status(
    url: Option<String>,
    grpc_url: Option<String>,
    admin: bool,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<()> {
    let base_url = url
        .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
        .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
    let status_url = format!("{}/healthz", base_url.trim_end_matches('/'));
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let health = fetch_health_with_retry(&http_client, &status_url)?;
    println!(
        "status.http={} service={} version={} git_hash={} uptime_seconds={}",
        health.status, health.service, health.version, health.git_hash, health.uptime_seconds
    );

    let runtime = build_runtime()?;
    let grpc_health =
        runtime.block_on(fetch_grpc_health_with_retry(resolve_grpc_url(grpc_url)?))?;
    println!(
        "status.grpc={} service={} version={} git_hash={} uptime_seconds={}",
        grpc_health.status,
        grpc_health.service,
        grpc_health.version,
        grpc_health.git_hash,
        grpc_health.uptime_seconds
    );

    if admin {
        let admin_response = fetch_admin_status(
            &http_client,
            base_url.as_str(),
            token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok()),
            principal,
            device_id,
            channel,
        )?;
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

fn run_agent(command: AgentCommand) -> Result<()> {
    match command {
        AgentCommand::Run {
            grpc_url,
            token,
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            prompt,
            prompt_stdin,
            allow_sensitive_tools,
            ndjson,
        } => {
            let input_prompt = resolve_prompt_input(prompt, prompt_stdin)?;
            let connection = AgentConnection {
                grpc_url: resolve_grpc_url(grpc_url)?,
                token: token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok()),
                principal,
                device_id,
                channel,
            };
            let request =
                build_agent_run_input(session_id, run_id, input_prompt, allow_sensitive_tools)?;
            execute_agent_stream(connection, request, ndjson)
        }
        AgentCommand::Interactive {
            grpc_url,
            token,
            principal,
            device_id,
            channel,
            session_id,
            allow_sensitive_tools,
            ndjson,
        } => {
            let connection = AgentConnection {
                grpc_url: resolve_grpc_url(grpc_url)?,
                token: token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok()),
                principal,
                device_id,
                channel,
            };
            let session_id = resolve_or_generate_canonical_id(session_id)?;
            if ndjson {
                eprintln!(
                    "agent.interactive=session_started session_id={} exit_hint=/exit",
                    session_id
                );
                std::io::stderr().flush().context("stderr flush failed")?;
            } else {
                println!(
                    "agent.interactive=session_started session_id={} exit_hint=/exit",
                    session_id
                );
                std::io::stdout().flush().context("stdout flush failed")?;
            }

            let stdin = std::io::stdin();
            for line in stdin.lock().lines() {
                let prompt = line.context("failed to read interactive prompt from stdin")?;
                let prompt = prompt.trim();
                if prompt.is_empty() {
                    continue;
                }
                if prompt.eq_ignore_ascii_case("/exit") {
                    break;
                }
                let request = AgentRunInput {
                    session_id: session_id.clone(),
                    run_id: generate_canonical_ulid(),
                    prompt: prompt.to_owned(),
                    allow_sensitive_tools,
                };
                execute_agent_stream(connection.clone(), request, ndjson)?;
            }
            Ok(())
        }
        AgentCommand::AcpShim {
            grpc_url,
            token,
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            prompt,
            prompt_stdin,
            allow_sensitive_tools,
            ndjson_stdin,
        } => {
            let connection = AgentConnection {
                grpc_url: resolve_grpc_url(grpc_url)?,
                token: token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok()),
                principal,
                device_id,
                channel,
            };
            if ndjson_stdin {
                return run_acp_shim_from_stdin(connection, allow_sensitive_tools);
            }

            let input_prompt = resolve_prompt_input(prompt, prompt_stdin)?;
            let request =
                build_agent_run_input(session_id, run_id, input_prompt, allow_sensitive_tools)?;
            run_agent_stream_as_acp(connection, request)
        }
    }
}

fn run_cron(command: CronCommand) -> Result<()> {
    match command {
        CronCommand::List => {
            println!("cron.list status=stub message=\"scheduler v1 arrives in M16\"");
        }
        CronCommand::Add { schedule, action } => {
            println!(
                "cron.add status=stub schedule=\"{}\" action=\"{}\" message=\"scheduler v1 arrives in M16\"",
                schedule, action
            );
        }
        CronCommand::Remove { id } => {
            println!("cron.remove status=stub id={} message=\"scheduler v1 arrives in M16\"", id);
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_channels(command: ChannelsCommand) -> Result<()> {
    match command {
        ChannelsCommand::List => {
            println!("channels.list status=stub message=\"channel plugins start in M31\"");
        }
        ChannelsCommand::Connect { kind, name } => {
            println!(
                "channels.connect status=stub kind={} name={} message=\"channel plugins start in M31\"",
                kind, name
            );
        }
        ChannelsCommand::Disconnect { name } => {
            println!(
                "channels.disconnect status=stub name={} message=\"channel plugins start in M31\"",
                name
            );
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_browser(command: BrowserCommand) -> Result<()> {
    match command {
        BrowserCommand::Status { url } => {
            let base_url = url.unwrap_or_else(|| DEFAULT_BROWSER_URL.to_owned());
            let status_url = format!("{}/healthz", base_url.trim_end_matches('/'));
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let response = fetch_health_with_retry(&client, &status_url)?;
            println!(
                "browser.status={} service={} version={} git_hash={} uptime_seconds={}",
                response.status,
                response.service,
                response.version,
                response.git_hash,
                response.uptime_seconds
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        BrowserCommand::Open { url } => {
            println!(
                "browser.open status=stub target_url={} message=\"browser action APIs ship in M24-M26\"",
                url
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

fn run_completion(shell: CompletionShell) -> Result<()> {
    let mut command = Cli::command();
    clap_complete::generate(to_clap_shell(shell), &mut command, "palyra", &mut std::io::stdout());
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_onboarding(command: OnboardingCommand) -> Result<()> {
    match command {
        OnboardingCommand::Wizard { path, force, daemon_url, admin_token_env } => {
            if admin_token_env.trim().is_empty() {
                anyhow::bail!("admin token env variable name cannot be empty");
            }

            let config_path = resolve_onboarding_path(path)?;
            if config_path.exists() && !force {
                anyhow::bail!(
                    "onboarding target already exists: {} (use --force to overwrite)",
                    config_path.display()
                );
            }
            if let Some(parent) = config_path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create config directory {}", parent.display())
                    })?;
                }
            }

            let template = onboarding_template();
            let (document, _) = parse_document_with_migration(template)
                .context("failed to validate generated onboarding config")?;
            validate_daemon_compatible_document(&document)
                .context("generated onboarding config does not match daemon schema")?;
            fs::write(&config_path, template).with_context(|| {
                format!("failed to write onboarding config {}", config_path.display())
            })?;

            println!(
                "onboarding.status=complete config_path={} daemon_url={} admin_token_env={}",
                config_path.display(),
                daemon_url,
                admin_token_env
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

fn execute_agent_stream(
    connection: AgentConnection,
    request: AgentRunInput,
    ndjson: bool,
) -> Result<()> {
    stream_agent_events(connection, request, |event| {
        if ndjson {
            emit_acp_event_ndjson(event)
        } else {
            emit_agent_event_text(event)
        }
    })
}

fn run_agent_stream_as_acp(connection: AgentConnection, request: AgentRunInput) -> Result<()> {
    stream_agent_events(connection, request, emit_acp_event_ndjson)
}

fn stream_agent_events<F>(
    connection: AgentConnection,
    request: AgentRunInput,
    mut emit_event: F,
) -> Result<()>
where
    F: FnMut(&common_v1::RunStreamEvent) -> Result<()>,
{
    let runtime = build_runtime()?;
    runtime.block_on(async {
        let mut stream = run_stream_with_retry(&connection, &request).await?;
        while let Some(event) = stream.next().await {
            let event = event.context("failed to read RunStream event")?;
            emit_event(&event)?;
            std::io::stdout().flush().context("stdout flush failed")?;
        }
        Result::<()>::Ok(())
    })
}

fn run_acp_shim_from_stdin(
    connection: AgentConnection,
    default_allow_sensitive_tools: bool,
) -> Result<()> {
    let stdin = std::io::stdin();
    for (line_index, line_result) in stdin.lock().lines().enumerate() {
        let line = line_result.context("failed to read NDJSON ACP input line")?;
        if line.trim().is_empty() {
            continue;
        }
        let request = parse_acp_shim_input_line(
            line.as_str(),
            line_index + 1,
            default_allow_sensitive_tools,
        )?;
        run_agent_stream_as_acp(connection.clone(), request)?;
    }
    Ok(())
}

fn parse_acp_shim_input_line(
    line: &str,
    line_index: usize,
    default_allow_sensitive_tools: bool,
) -> Result<AgentRunInput> {
    let parsed: AcpShimInput = serde_json::from_str(line)
        .with_context(|| format!("failed to parse NDJSON ACP input line {}", line_index))?;
    let prompt =
        parsed.prompt.context("NDJSON ACP input requires `prompt` field with non-empty text")?;
    let prompt = prompt.trim();
    if prompt.is_empty() {
        anyhow::bail!("NDJSON ACP input requires `prompt` field with non-empty text");
    }
    build_agent_run_input(
        parsed.session_id,
        parsed.run_id,
        prompt.to_owned(),
        parsed.allow_sensitive_tools.unwrap_or(default_allow_sensitive_tools),
    )
}

fn resolve_prompt_input(prompt: Option<String>, prompt_stdin: bool) -> Result<String> {
    if prompt_stdin {
        if prompt.is_some() {
            anyhow::bail!("cannot use --prompt together with --prompt-stdin");
        }
        let mut input = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut input)
            .context("failed to read prompt from stdin")?;
        let prompt = input.trim_end_matches(['\r', '\n']).trim();
        if prompt.is_empty() {
            anyhow::bail!("prompt from stdin is empty");
        }
        return Ok(prompt.to_owned());
    }

    let prompt = prompt.context("missing prompt: use --prompt or --prompt-stdin")?;
    let prompt = prompt.trim();
    if prompt.is_empty() {
        anyhow::bail!("prompt cannot be empty");
    }
    Ok(prompt.to_owned())
}

fn build_agent_run_input(
    session_id: Option<String>,
    run_id: Option<String>,
    prompt: String,
    allow_sensitive_tools: bool,
) -> Result<AgentRunInput> {
    Ok(AgentRunInput {
        session_id: resolve_or_generate_canonical_id(session_id)?,
        run_id: resolve_or_generate_canonical_id(run_id)?,
        prompt,
        allow_sensitive_tools,
    })
}

fn resolve_or_generate_canonical_id(value: Option<String>) -> Result<String> {
    let resolved = value.unwrap_or_else(generate_canonical_ulid);
    validate_canonical_id(resolved.as_str())
        .with_context(|| format!("invalid canonical ULID: {}", resolved))?;
    Ok(resolved)
}

fn generate_canonical_ulid() -> String {
    Ulid::new().to_string()
}

fn resolve_grpc_url(explicit: Option<String>) -> Result<String> {
    if let Some(url) = explicit {
        return Ok(url);
    }
    if let Ok(url) = env::var("PALYRA_GATEWAY_GRPC_URL") {
        if !url.trim().is_empty() {
            return Ok(url);
        }
    }
    let bind = env::var("PALYRA_GATEWAY_GRPC_BIND_ADDR")
        .unwrap_or_else(|_| DEFAULT_GATEWAY_GRPC_BIND_ADDR.to_owned());
    let port = env::var("PALYRA_GATEWAY_GRPC_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_GATEWAY_GRPC_PORT);
    let socket = parse_daemon_bind_socket(bind.as_str(), port)
        .context("invalid gateway gRPC bind config")?;
    let socket = normalize_client_socket(socket);
    Ok(format!("http://{socket}"))
}

fn normalize_client_socket(socket: SocketAddr) -> SocketAddr {
    match socket {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4.port())
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6.port())
        }
        other => other,
    }
}

fn build_runtime() -> Result<tokio::runtime::Runtime> {
    RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to initialize async runtime")
}

fn resolve_onboarding_path(path: Option<String>) -> Result<PathBuf> {
    if let Some(path) = path {
        return parse_config_path(path.as_str())
            .with_context(|| format!("onboarding config path is invalid: {}", path));
    }
    Ok(PathBuf::from("palyra.toml"))
}

fn onboarding_template() -> &'static str {
    "version = 1\n\
[daemon]\n\
bind_addr = \"127.0.0.1\"\n\
port = 7142\n\
\n\
[gateway]\n\
grpc_bind_addr = \"127.0.0.1\"\n\
grpc_port = 7443\n\
quic_bind_addr = \"127.0.0.1\"\n\
quic_port = 7444\n\
quic_enabled = true\n\
\n\
[orchestrator]\n\
runloop_v1_enabled = true\n"
}

async fn fetch_grpc_health_with_retry(grpc_url: String) -> Result<gateway_v1::HealthResponse> {
    let mut last_error = None;
    for attempt in 1..=MAX_GRPC_ATTEMPTS {
        match fetch_grpc_health_once(grpc_url.as_str()).await {
            Ok(response) => return Ok(response),
            Err(error) => {
                let retryable = is_retryable_grpc_error(&error);
                last_error = Some(error);
                if attempt < MAX_GRPC_ATTEMPTS && retryable {
                    let delay_ms = BASE_GRPC_BACKOFF_MS * (1_u64 << (attempt - 1));
                    sleep(Duration::from_millis(delay_ms)).await;
                } else {
                    break;
                }
            }
        }
    }

    if let Some(error) = last_error {
        Err(error).context(format!("gRPC health check failed after {MAX_GRPC_ATTEMPTS} attempts"))
    } else {
        anyhow::bail!("gRPC health check failed with no captured error")
    }
}

async fn fetch_grpc_health_once(grpc_url: &str) -> Result<gateway_v1::HealthResponse> {
    let mut client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(grpc_url.to_owned())
            .await
            .with_context(|| format!("failed to connect gateway gRPC endpoint {grpc_url}"))?;
    let response = client
        .get_health(gateway_v1::HealthRequest { v: RUN_STREAM_REQUEST_VERSION })
        .await
        .context("failed to call gateway GetHealth")?;
    Ok(response.into_inner())
}

async fn run_stream_with_retry(
    connection: &AgentConnection,
    request: &AgentRunInput,
) -> Result<tonic::Streaming<common_v1::RunStreamEvent>> {
    let mut last_error = None;
    for attempt in 1..=MAX_GRPC_ATTEMPTS {
        match run_stream_once(connection, request).await {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                let retryable = is_retryable_grpc_error(&error);
                last_error = Some(error);
                if attempt < MAX_GRPC_ATTEMPTS && retryable {
                    let delay_ms = BASE_GRPC_BACKOFF_MS * (1_u64 << (attempt - 1));
                    sleep(Duration::from_millis(delay_ms)).await;
                } else {
                    break;
                }
            }
        }
    }

    if let Some(error) = last_error {
        Err(error).context(format!("agent stream failed after {MAX_GRPC_ATTEMPTS} attempts"))
    } else {
        anyhow::bail!("agent stream failed with no captured error")
    }
}

async fn run_stream_once(
    connection: &AgentConnection,
    input: &AgentRunInput,
) -> Result<tonic::Streaming<common_v1::RunStreamEvent>> {
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(
        connection.grpc_url.clone(),
    )
    .await
    .with_context(|| format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url))?;
    let request = build_run_stream_request(input)?;
    let mut stream_request = Request::new(iter(vec![request]));
    inject_run_stream_metadata(stream_request.metadata_mut(), connection)?;
    let stream = client
        .run_stream(stream_request)
        .await
        .context("failed to call gateway RunStream")?
        .into_inner();
    Ok(stream)
}

fn is_retryable_grpc_error(error: &anyhow::Error) -> bool {
    if error.chain().any(|cause| cause.is::<tonic::transport::Error>()) {
        return true;
    }
    error.chain().find_map(|cause| cause.downcast_ref::<tonic::Status>()).is_some_and(|status| {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::ResourceExhausted
                | tonic::Code::Internal
        )
    })
}

fn inject_run_stream_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    connection: &AgentConnection,
) -> Result<()> {
    if let Some(token) = connection.token.as_ref() {
        metadata.insert(
            "authorization",
            format!("Bearer {token}").parse().context("invalid admin token metadata")?,
        );
    }
    metadata.insert(
        "x-palyra-principal",
        connection.principal.parse().context("invalid principal metadata value")?,
    );
    metadata.insert(
        "x-palyra-device-id",
        connection.device_id.parse().context("invalid device_id metadata value")?,
    );
    metadata.insert(
        "x-palyra-channel",
        connection.channel.parse().context("invalid channel metadata value")?,
    );
    Ok(())
}

fn build_run_stream_request(input: &AgentRunInput) -> Result<common_v1::RunStreamRequest> {
    let timestamp_unix_ms = now_unix_ms_i64()?;
    Ok(common_v1::RunStreamRequest {
        v: RUN_STREAM_REQUEST_VERSION,
        session_id: Some(common_v1::CanonicalId { ulid: input.session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: input.run_id.clone() }),
        input: Some(common_v1::MessageEnvelope {
            v: CANONICAL_JSON_ENVELOPE_VERSION,
            envelope_id: Some(common_v1::CanonicalId { ulid: generate_canonical_ulid() }),
            timestamp_unix_ms,
            origin: Some(common_v1::EnvelopeOrigin {
                r#type: common_v1::envelope_origin::OriginType::Cli as i32,
                channel: DEFAULT_CHANNEL.to_owned(),
                conversation_id: input.session_id.clone(),
                sender_display: "palyra-cli".to_owned(),
                sender_handle: "cli".to_owned(),
                sender_verified: true,
            }),
            content: Some(common_v1::MessageContent {
                text: input.prompt.clone(),
                attachments: Vec::new(),
            }),
            security: None,
            max_payload_bytes: 0,
        }),
        allow_sensitive_tools: input.allow_sensitive_tools,
    })
}

fn emit_agent_event_text(event: &common_v1::RunStreamEvent) -> Result<()> {
    let run_id = event.run_id.as_ref().map(|id| id.ulid.as_str()).unwrap_or("unknown");
    match event.body.as_ref() {
        Some(common_v1::run_stream_event::Body::ModelToken(token)) => {
            println!(
                "agent.token run_id={} token={} final={}",
                run_id, token.token, token.is_final
            );
        }
        Some(common_v1::run_stream_event::Body::Status(status)) => {
            println!(
                "agent.status run_id={} kind={} message={}",
                run_id,
                stream_status_kind_to_text(status.kind),
                status.message
            );
        }
        Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => {
            println!(
                "agent.tool.proposal run_id={} proposal_id={} tool_name={} approval_required={}",
                run_id,
                proposal.proposal_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or("unknown"),
                proposal.tool_name,
                proposal.approval_required
            );
        }
        Some(common_v1::run_stream_event::Body::ToolDecision(decision)) => {
            println!(
                "agent.tool.decision run_id={} proposal_id={} kind={} reason={} approval_required={} policy_enforced={}",
                run_id,
                decision
                    .proposal_id
                    .as_ref()
                    .map(|value| value.ulid.as_str())
                    .unwrap_or("unknown"),
                tool_decision_kind_to_text(decision.kind),
                decision.reason,
                decision.approval_required,
                decision.policy_enforced
            );
        }
        Some(common_v1::run_stream_event::Body::ToolResult(result)) => {
            println!(
                "agent.tool.result run_id={} proposal_id={} success={} error={}",
                run_id,
                result.proposal_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or("unknown"),
                result.success,
                result.error
            );
        }
        Some(common_v1::run_stream_event::Body::ToolAttestation(attestation)) => {
            println!(
                "agent.tool.attestation run_id={} proposal_id={} attestation_id={} timed_out={} executor={}",
                run_id,
                attestation
                    .proposal_id
                    .as_ref()
                    .map(|value| value.ulid.as_str())
                    .unwrap_or("unknown"),
                attestation
                    .attestation_id
                    .as_ref()
                    .map(|value| value.ulid.as_str())
                    .unwrap_or("unknown"),
                attestation.timed_out,
                attestation.executor
            );
        }
        Some(common_v1::run_stream_event::Body::A2uiUpdate(update)) => {
            println!(
                "agent.a2ui.update run_id={} surface={} version={}",
                run_id, update.surface, update.v
            );
        }
        Some(common_v1::run_stream_event::Body::JournalEvent(journal_event)) => {
            println!(
                "agent.journal.event run_id={} event_id={} kind={} actor={}",
                run_id,
                journal_event
                    .event_id
                    .as_ref()
                    .map(|value| value.ulid.as_str())
                    .unwrap_or("unknown"),
                journal_event.kind,
                journal_event.actor
            );
        }
        None => {
            println!("agent.event run_id={} kind=unknown", run_id);
        }
    }
    Ok(())
}

fn emit_acp_event_ndjson(event: &common_v1::RunStreamEvent) -> Result<()> {
    let run_id =
        event.run_id.as_ref().map(|id| id.ulid.clone()).unwrap_or_else(|| "unknown".to_owned());
    let payload = match event.body.as_ref() {
        Some(common_v1::run_stream_event::Body::ModelToken(token)) => json!({
            "type": "model.token",
            "run_id": run_id,
            "token": token.token,
            "is_final": token.is_final,
        }),
        Some(common_v1::run_stream_event::Body::Status(status)) => json!({
            "type": "run.status",
            "run_id": run_id,
            "kind": stream_status_kind_to_text(status.kind),
            "message": status.message,
        }),
        Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => json!({
            "type": "tool.proposal",
            "run_id": run_id,
            "proposal_id": proposal.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "tool_name": proposal.tool_name,
            "approval_required": proposal.approval_required,
            "input_json": proposal.input_json,
        }),
        Some(common_v1::run_stream_event::Body::ToolDecision(decision)) => json!({
            "type": "tool.decision",
            "run_id": run_id,
            "proposal_id": decision.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "kind": tool_decision_kind_to_text(decision.kind),
            "reason": decision.reason,
            "approval_required": decision.approval_required,
            "policy_enforced": decision.policy_enforced,
        }),
        Some(common_v1::run_stream_event::Body::ToolResult(result)) => json!({
            "type": "tool.result",
            "run_id": run_id,
            "proposal_id": result.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "success": result.success,
            "output_json": result.output_json,
            "error": result.error,
        }),
        Some(common_v1::run_stream_event::Body::ToolAttestation(attestation)) => json!({
            "type": "tool.attestation",
            "run_id": run_id,
            "proposal_id": attestation.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "attestation_id": attestation.attestation_id.as_ref().map(|value| value.ulid.clone()),
            "execution_sha256": attestation.execution_sha256,
            "executed_at_unix_ms": attestation.executed_at_unix_ms,
            "timed_out": attestation.timed_out,
            "executor": attestation.executor,
        }),
        Some(common_v1::run_stream_event::Body::A2uiUpdate(update)) => json!({
            "type": "a2ui.update",
            "run_id": run_id,
            "surface": update.surface,
            "version": update.v,
            "patch_json": update.patch_json,
        }),
        Some(common_v1::run_stream_event::Body::JournalEvent(journal_event)) => json!({
            "type": "journal.event",
            "run_id": run_id,
            "event_id": journal_event.event_id.as_ref().map(|value| value.ulid.clone()),
            "kind": journal_event.kind,
            "actor": journal_event.actor,
            "timestamp_unix_ms": journal_event.timestamp_unix_ms,
            "payload_json": journal_event.payload_json,
            "hash": journal_event.hash,
        }),
        None => json!({
            "type": "unknown",
            "run_id": run_id,
        }),
    };
    println!(
        "{}",
        serde_json::to_string(&payload).context("failed to serialize ACP NDJSON event")?
    );
    Ok(())
}

fn stream_status_kind_to_text(kind: i32) -> &'static str {
    if kind == common_v1::stream_status::StatusKind::Unspecified as i32 {
        "unspecified"
    } else if kind == common_v1::stream_status::StatusKind::Accepted as i32 {
        "accepted"
    } else if kind == common_v1::stream_status::StatusKind::InProgress as i32 {
        "in_progress"
    } else if kind == common_v1::stream_status::StatusKind::Done as i32 {
        "done"
    } else if kind == common_v1::stream_status::StatusKind::Failed as i32 {
        "failed"
    } else {
        "unknown"
    }
}

fn tool_decision_kind_to_text(kind: i32) -> &'static str {
    if kind == common_v1::tool_decision::DecisionKind::Unspecified as i32 {
        "unspecified"
    } else if kind == common_v1::tool_decision::DecisionKind::Allow as i32 {
        "allow"
    } else if kind == common_v1::tool_decision::DecisionKind::Deny as i32 {
        "deny"
    } else {
        "unknown"
    }
}

fn now_unix_ms_i64() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?;
    let millis = elapsed.as_millis();
    i64::try_from(millis).context("system clock value exceeds i64 range")
}

fn to_clap_shell(shell: CompletionShell) -> clap_complete::Shell {
    match shell {
        CompletionShell::Bash => clap_complete::Shell::Bash,
        CompletionShell::Zsh => clap_complete::Shell::Zsh,
        CompletionShell::Fish => clap_complete::Shell::Fish,
        CompletionShell::Powershell => clap_complete::Shell::PowerShell,
        CompletionShell::Elvish => clap_complete::Shell::Elvish,
    }
}

fn fetch_admin_status(
    client: &Client,
    base_url: &str,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<AdminStatusResponse> {
    let status_url = format!("{}/admin/v1/status", base_url.trim_end_matches('/'));
    let mut request = client
        .get(status_url)
        .header("x-palyra-principal", principal)
        .header("x-palyra-device-id", device_id);
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    if let Some(channel) = channel {
        request = request.header("x-palyra-channel", channel);
    }

    request
        .send()
        .context("failed to call daemon admin status endpoint")?
        .error_for_status()
        .context("daemon admin status endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon admin status payload")
}

#[derive(Debug, Clone)]
struct AgentConnection {
    grpc_url: String,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: String,
}

#[derive(Debug, Clone)]
struct AgentRunInput {
    session_id: String,
    run_id: String,
    prompt: String,
    allow_sensitive_tools: bool,
}

#[derive(Debug, Deserialize)]
struct AcpShimInput {
    session_id: Option<String>,
    run_id: Option<String>,
    prompt: Option<String>,
    allow_sensitive_tools: Option<bool>,
}

fn run_daemon(command: DaemonCommand) -> Result<()> {
    match command {
        DaemonCommand::Status { url } => {
            let base_url = url
                .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
                .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
            let status_url = format!("{}/healthz", base_url.trim_end_matches('/'));
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
        DaemonCommand::AdminStatus { url, token, principal, device_id, channel } => {
            let base_url = url
                .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
                .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
            let token = token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let response = fetch_admin_status(
                &client,
                base_url.as_str(),
                token,
                principal,
                device_id,
                channel,
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
            let base_url = url
                .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
                .unwrap_or_else(|| "http://127.0.0.1:7142".to_owned());
            let endpoint = format!("{}/admin/v1/journal/recent", base_url.trim_end_matches('/'));
            let token = token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let mut request = client
                .get(endpoint)
                .header("x-palyra-principal", principal)
                .header("x-palyra-device-id", device_id);
            if let Some(token) = token {
                request = request.header("Authorization", format!("Bearer {token}"));
            }
            if let Some(channel) = channel {
                request = request.header("x-palyra-channel", channel);
            }
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
        DaemonCommand::RunStatus { url, token, principal, device_id, channel, run_id } => {
            validate_canonical_id(run_id.as_str())
                .context("run_id must be a canonical ULID for daemon run-status")?;
            let base_url = url
                .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
                .unwrap_or_else(|| "http://127.0.0.1:7142".to_owned());
            let endpoint = format!("{}/admin/v1/runs/{run_id}", base_url.trim_end_matches('/'));
            let token = token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let mut request = client
                .get(endpoint)
                .header("x-palyra-principal", principal)
                .header("x-palyra-device-id", device_id);
            if let Some(token) = token {
                request = request.header("Authorization", format!("Bearer {token}"));
            }
            if let Some(channel) = channel {
                request = request.header("x-palyra-channel", channel);
            }
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
        DaemonCommand::RunTape { url, token, principal, device_id, channel, run_id } => {
            validate_canonical_id(run_id.as_str())
                .context("run_id must be a canonical ULID for daemon run-tape")?;
            let base_url = url
                .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
                .unwrap_or_else(|| "http://127.0.0.1:7142".to_owned());
            let endpoint =
                format!("{}/admin/v1/runs/{run_id}/tape", base_url.trim_end_matches('/'));
            let token = token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let mut request = client
                .get(endpoint)
                .header("x-palyra-principal", principal)
                .header("x-palyra-device-id", device_id);
            if let Some(token) = token {
                request = request.header("Authorization", format!("Bearer {token}"));
            }
            if let Some(channel) = channel {
                request = request.header("x-palyra-channel", channel);
            }
            let response: RunTapeResponse = request
                .send()
                .context("failed to call daemon run tape endpoint")?
                .error_for_status()
                .context("daemon run tape endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon run tape payload")?;
            println!("run.tape run_id={} events={}", response.run_id, response.events.len());
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
            let base_url = url
                .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
                .unwrap_or_else(|| "http://127.0.0.1:7142".to_owned());
            let endpoint =
                format!("{}/admin/v1/runs/{run_id}/cancel", base_url.trim_end_matches('/'));
            let token = token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
            let mut request = client
                .post(endpoint)
                .header("x-palyra-principal", principal)
                .header("x-palyra-device-id", device_id);
            if let Some(token) = token {
                request = request.header("Authorization", format!("Bearer {token}"));
            }
            if let Some(channel) = channel {
                request = request.header("x-palyra-channel", channel);
            }
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

fn run_policy(command: PolicyCommand) -> Result<()> {
    match command {
        PolicyCommand::Explain { principal, action, resource } => {
            let request = PolicyRequest { principal, action, resource };
            let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
                .context("failed to evaluate policy with Cedar engine")?;
            let matched_policies = if evaluation.explanation.matched_policy_ids.is_empty() {
                "none".to_owned()
            } else {
                evaluation.explanation.matched_policy_ids.join(",")
            };
            match evaluation.decision {
                PolicyDecision::Allow => {
                    println!(
                        "decision=allow principal={} action={} resource={} reason={} matched_policies={}",
                        request.principal,
                        request.action,
                        request.resource,
                        evaluation.explanation.reason,
                        matched_policies,
                    );
                }
                PolicyDecision::DenyByDefault { reason } => {
                    println!(
                        "decision=deny_by_default principal={} action={} resource={} approval_required=true reason={} matched_policies={}",
                        request.principal,
                        request.action,
                        request.resource,
                        reason,
                        matched_policies,
                    );
                }
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

fn run_config(command: ConfigCommand) -> Result<()> {
    match command {
        ConfigCommand::Validate { path } => {
            let path = match path {
                Some(explicit) => resolve_config_path(Some(explicit), true)?,
                None => {
                    if let Some(found) = find_default_config_path() {
                        found
                    } else {
                        println!("config=valid source=defaults");
                        return std::io::stdout().flush().context("stdout flush failed");
                    }
                }
            };

            let (document, migration) = load_document_from_existing_path(Path::new(&path))
                .with_context(|| format!("failed to parse {path}"))?;
            validate_daemon_compatible_document(&document)
                .with_context(|| format!("failed to parse {path}"))?;
            println!(
                "config=valid source={path} version={} migrated={}",
                migration.target_version, migration.migrated
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Get { path, key } => {
            let path = resolve_config_path(path, true)?;
            let (document, _) = load_document_from_existing_path(Path::new(&path))
                .with_context(|| format!("failed to parse {path}"))?;
            let value = get_value_at_path(&document, key.as_str())
                .with_context(|| format!("invalid config key path: {}", key))?
                .with_context(|| format!("config key not found: {}", key))?;
            println!("config.get key={} value={} source={}", key, format_toml_value(value), path);
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Set { path, key, value, backups } => {
            let path = resolve_config_path(path, false)?;
            let path_ref = Path::new(&path);
            let (mut document, migration) = load_document_for_mutation(path_ref)
                .with_context(|| format!("failed to parse {}", path_ref.display()))?;
            let literal = parse_toml_value_literal(value.as_str())
                .context("config set value must be a valid TOML literal")?;
            set_value_at_path(&mut document, key.as_str(), literal)
                .with_context(|| format!("invalid config key path: {}", key))?;
            validate_daemon_compatible_document(&document).with_context(|| {
                format!("mutated config {} does not match daemon schema", path_ref.display())
            })?;
            write_document_with_backups(path_ref, &document, backups)
                .with_context(|| format!("failed to persist config {}", path_ref.display()))?;
            println!(
                "config.set key={} source={} backups={} migrated={}",
                key,
                path_ref.display(),
                backups,
                migration.migrated
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Unset { path, key, backups } => {
            let path = resolve_config_path(path, true)?;
            let path_ref = Path::new(&path);
            let (mut document, _) = load_document_from_existing_path(path_ref)
                .with_context(|| format!("failed to parse {}", path_ref.display()))?;
            let removed = unset_value_at_path(&mut document, key.as_str())
                .with_context(|| format!("invalid config key path: {}", key))?;
            if !removed {
                anyhow::bail!("config key not found: {}", key);
            }
            validate_daemon_compatible_document(&document).with_context(|| {
                format!("mutated config {} does not match daemon schema", path_ref.display())
            })?;
            write_document_with_backups(path_ref, &document, backups)
                .with_context(|| format!("failed to persist config {}", path_ref.display()))?;
            println!("config.unset key={} source={} backups={}", key, path_ref.display(), backups);
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Migrate { path, backups } => {
            let path = resolve_config_path(path, true)?;
            let path_ref = Path::new(&path);
            let (document, migration) = load_document_from_existing_path(path_ref)
                .with_context(|| format!("failed to parse {}", path_ref.display()))?;
            validate_daemon_compatible_document(&document).with_context(|| {
                format!("migrated config {} does not match daemon schema", path_ref.display())
            })?;
            if migration.migrated {
                write_document_with_backups(path_ref, &document, backups).with_context(|| {
                    format!("failed to persist migrated config {}", path_ref.display())
                })?;
            }
            println!(
                "config.migrate source={} source_version={} target_version={} migrated={} backups={}",
                path_ref.display(),
                migration.source_version,
                migration.target_version,
                migration.migrated,
                backups
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Recover { path, backup, backups } => {
            let path = resolve_config_path(path, false)?;
            let path_ref = Path::new(&path);
            let candidate_backup = backup_path(path_ref, backup);
            let (backup_document, _) = load_document_from_existing_path(&candidate_backup)
                .with_context(|| {
                    format!("failed to parse backup config {}", candidate_backup.display())
                })?;
            validate_daemon_compatible_document(&backup_document).with_context(|| {
                format!("backup config {} does not match daemon schema", candidate_backup.display())
            })?;
            let recovered =
                recover_config_from_backup(path_ref, backup, backups).with_context(|| {
                    format!(
                        "failed to recover config {} from backup index {}",
                        path_ref.display(),
                        backup
                    )
                })?;
            let (document, _) = load_document_from_existing_path(path_ref).with_context(|| {
                format!("failed to parse recovered config {}", path_ref.display())
            })?;
            validate_daemon_compatible_document(&document).with_context(|| {
                format!("recovered config {} does not match daemon schema", path_ref.display())
            })?;
            println!(
                "config.recover source={} backup={} recovered_from={} backups={}",
                path_ref.display(),
                backup,
                recovered.display(),
                backups
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

fn validate_daemon_compatible_document(document: &toml::Value) -> Result<()> {
    let content =
        toml::to_string(document).context("failed to serialize daemon config document")?;
    let parsed: RootFileConfig =
        toml::from_str(&content).context("invalid daemon config schema")?;
    let bind_addr = parsed
        .daemon
        .as_ref()
        .and_then(|daemon| daemon.bind_addr.as_deref())
        .unwrap_or("127.0.0.1");
    let port = parsed.daemon.as_ref().and_then(|daemon| daemon.port).unwrap_or(7142);
    let _ =
        parse_daemon_bind_socket(bind_addr, port).context("invalid daemon bind address or port")?;

    let grpc_bind_addr = parsed
        .gateway
        .as_ref()
        .and_then(|gateway| gateway.grpc_bind_addr.as_deref())
        .unwrap_or(DEFAULT_GATEWAY_GRPC_BIND_ADDR);
    let grpc_port = parsed
        .gateway
        .as_ref()
        .and_then(|gateway| gateway.grpc_port)
        .unwrap_or(DEFAULT_GATEWAY_GRPC_PORT);
    let _ = parse_daemon_bind_socket(grpc_bind_addr, grpc_port)
        .context("invalid gateway gRPC bind address or port")?;

    let quic_enabled = parsed
        .gateway
        .as_ref()
        .and_then(|gateway| gateway.quic_enabled)
        .unwrap_or(DEFAULT_GATEWAY_QUIC_ENABLED);
    if quic_enabled {
        let quic_bind_addr = parsed
            .gateway
            .as_ref()
            .and_then(|gateway| gateway.quic_bind_addr.as_deref())
            .unwrap_or(DEFAULT_GATEWAY_QUIC_BIND_ADDR);
        let quic_port = parsed
            .gateway
            .as_ref()
            .and_then(|gateway| gateway.quic_port)
            .unwrap_or(DEFAULT_GATEWAY_QUIC_PORT);
        let _ = parse_daemon_bind_socket(quic_bind_addr, quic_port)
            .context("invalid gateway QUIC bind address or port")?;
    }

    Ok(())
}

fn load_document_from_existing_path(path: &Path) -> Result<(toml::Value, ConfigMigrationInfo)> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    parse_document_with_migration(&content).context("failed to migrate config document")
}

fn load_document_for_mutation(path: &Path) -> Result<(toml::Value, ConfigMigrationInfo)> {
    if path.exists() {
        return load_document_from_existing_path(path);
    }
    parse_document_with_migration("").context("failed to initialize config document")
}

fn resolve_config_path(path: Option<String>, require_existing: bool) -> Result<String> {
    let resolved = match path {
        Some(explicit) => {
            let parsed = parse_config_path(&explicit)
                .with_context(|| format!("config path is invalid: {}", explicit))?;
            parsed.to_string_lossy().into_owned()
        }
        None => find_default_config_path()
            .context("no default config file found; pass --path to select a config file")?,
    };

    if require_existing && !Path::new(&resolved).exists() {
        anyhow::bail!("config file does not exist: {}", resolved);
    }

    Ok(resolved)
}

fn find_default_config_path() -> Option<String> {
    for candidate in default_config_search_paths() {
        if candidate.exists() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }

    None
}

#[cfg(not(windows))]
fn run_pairing(command: PairingCommand) -> Result<()> {
    match command {
        PairingCommand::Pair {
            device_id,
            client_kind,
            method,
            proof,
            proof_stdin,
            allow_insecure_proof_arg,
            store_dir,
            approve,
            simulate_rotation,
        } => {
            if !approve {
                anyhow::bail!(
                    "decision=deny_by_default approval_required=true reason=pairing requires explicit --approve"
                );
            }

            let store_root = resolve_identity_store_root(store_dir)?;
            let store = build_identity_store(&store_root)?;
            let mut manager = IdentityManager::with_store(store.clone())
                .context("failed to initialize identity manager")?;
            let proof = resolve_pairing_proof(proof, proof_stdin, allow_insecure_proof_arg)?;
            let pairing_method = build_pairing_method(method, &proof);

            let started_at = SystemTime::now();
            let session = manager
                .start_pairing(to_identity_client_kind(client_kind), pairing_method, started_at)
                .context("failed to start pairing session")?;
            let device = DeviceIdentity::generate(&device_id)
                .context("failed to generate device identity")?;

            let hello = manager
                .build_device_hello(&session, &device, &proof)
                .context("failed to build device pairing hello")?;
            let completed_at = SystemTime::now();
            let result = manager
                .complete_pairing(hello, completed_at)
                .context("failed to complete pairing handshake")?;
            if let Err(store_error) = device.store(store.as_ref()) {
                let rollback = manager.revoke_device(
                    &device_id,
                    "device identity persistence failed after pairing",
                    SystemTime::now(),
                );
                if let Err(rollback_error) = rollback {
                    anyhow::bail!(
                        "failed to persist device identity after pairing ({store_error}); rollback revoke failed ({rollback_error})"
                    );
                }
                anyhow::bail!(
                    "failed to persist device identity after pairing: {store_error}; pairing was rolled back"
                );
            }

            println!(
                "pairing.status=paired device_id={} client_kind={} method={} identity_fingerprint={} signing_public_key_hex={} transcript_hash={} cert_sequence={} cert_expires_at_unix_ms={} store_root={}",
                result.device.device_id,
                result.device.client_kind.as_str(),
                method.as_str(),
                result.device.identity_fingerprint,
                result.device.signing_public_key_hex,
                result.device.transcript_hash_hex,
                result.device.current_certificate.sequence,
                result.device.current_certificate.expires_at_unix_ms,
                store_root.display(),
            );

            if simulate_rotation {
                let rotated = manager
                    .rotate_device_certificate_if_due(
                        &device_id,
                        SystemTime::now() + DEFAULT_CERT_VALIDITY,
                    )
                    .context("failed to rotate certificate in simulation mode")?;
                println!(
                    "pairing.rotation=simulated rotated=true previous_sequence={} current_sequence={}",
                    result.device.current_certificate.sequence, rotated.sequence
                );
            }

            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

#[cfg(not(windows))]
fn resolve_identity_store_root(store_dir: Option<String>) -> Result<PathBuf> {
    if let Some(path) = store_dir {
        return Ok(PathBuf::from(path));
    }
    default_identity_store_root_from_env(env::var_os("XDG_STATE_HOME"), env::var_os("HOME"))
}

#[cfg(not(windows))]
fn default_identity_store_root_from_env(
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf> {
    if let Some(state_home) = xdg_state_home {
        return Ok(PathBuf::from(state_home).join("palyra").join("identity"));
    }

    let home = home.map(PathBuf::from).context("HOME is not set")?;
    Ok(home.join(".local").join("state").join("palyra").join("identity"))
}

#[cfg(not(windows))]
fn build_identity_store(store_root: &Path) -> Result<Arc<dyn SecretStore>> {
    let store = FilesystemSecretStore::new(store_root).with_context(|| {
        format!("failed to initialize secret store at {}", store_root.display())
    })?;
    Ok(Arc::new(store))
}

#[cfg(not(windows))]
fn build_pairing_method(method: PairingMethodArg, proof: &str) -> PairingMethod {
    match method {
        PairingMethodArg::Pin => PairingMethod::Pin { code: proof.to_owned() },
        PairingMethodArg::Qr => PairingMethod::Qr { token: proof.to_owned() },
    }
}

#[cfg(not(windows))]
fn resolve_pairing_proof(
    proof: Option<String>,
    proof_stdin: bool,
    allow_insecure_proof_arg: bool,
) -> Result<String> {
    if proof_stdin {
        let mut input = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut input)
            .context("failed to read pairing proof from stdin")?;
        let proof = input.trim_end_matches(['\r', '\n']);
        if proof.is_empty() {
            anyhow::bail!("pairing proof from stdin is empty");
        }
        return Ok(proof.to_owned());
    }

    if let Some(proof) = proof {
        if !allow_insecure_proof_arg {
            anyhow::bail!(
                "refusing --proof without --allow-insecure-proof-arg; use --proof-stdin instead"
            );
        }
        return Ok(proof);
    }

    anyhow::bail!(
        "missing pairing proof: use --proof-stdin or --proof with --allow-insecure-proof-arg"
    )
}

#[cfg(not(windows))]
fn to_identity_client_kind(value: PairingClientKindArg) -> PairingClientKind {
    match value {
        PairingClientKindArg::Cli => PairingClientKind::Cli,
        PairingClientKindArg::Desktop => PairingClientKind::Desktop,
        PairingClientKindArg::Node => PairingClientKind::Node,
    }
}

fn run_protocol(command: ProtocolCommand) -> Result<()> {
    match command {
        ProtocolCommand::Version => {
            println!(
                "protocol.major={} json.envelope.v={}",
                CANONICAL_PROTOCOL_MAJOR, CANONICAL_JSON_ENVELOPE_VERSION
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        ProtocolCommand::ValidateId { id } => {
            validate_canonical_id(&id).with_context(|| format!("invalid canonical ID: {}", id))?;
            println!("canonical_id.valid=true id={id}");
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

fn is_workspace_writable() -> Result<bool> {
    let probe_path = std::env::current_dir()
        .context("failed to resolve current directory")?
        .join(".palyra-doctor-write-check.tmp");
    fs::write(&probe_path, "probe").context("failed to write probe file")?;
    fs::remove_file(&probe_path).context("failed to clean probe file")?;
    Ok(true)
}

fn required_directories_ok() -> bool {
    [
        "crates/palyra-daemon",
        "crates/palyra-cli",
        "crates/palyra-browserd",
        "crates/palyra-policy",
        "crates/palyra-a2ui",
        "crates/palyra-plugins/runtime",
        "crates/palyra-plugins/sdk",
        "apps/ios",
        "apps/android",
        "apps/desktop",
        "apps/web",
        "schemas/proto",
        "schemas/json",
        "schemas/generated",
        "infra/docker",
        "infra/nix",
        "infra/ci",
        "fuzz/fuzz_targets",
    ]
    .iter()
    .all(|path| Path::new(path).exists())
}

fn command_available(command: &str, args: &[&str]) -> bool {
    Command::new(command).args(args).output().map(|output| output.status.success()).unwrap_or(false)
}

fn fetch_health_with_retry(client: &Client, status_url: &str) -> Result<HealthResponse> {
    let mut last_error = None;
    for attempt in 1..=MAX_HEALTH_ATTEMPTS {
        let result = client
            .get(status_url)
            .send()
            .context("failed to call daemon health endpoint")
            .and_then(|response| {
                response
                    .error_for_status()
                    .context("daemon health endpoint returned non-success status")
            })
            .and_then(|response| response.json().context("failed to parse daemon health payload"));

        match result {
            Ok(response) => return Ok(response),
            Err(error) => {
                last_error = Some(error);
                if attempt < MAX_HEALTH_ATTEMPTS {
                    let delay_ms = BASE_HEALTH_BACKOFF_MS * (1_u64 << (attempt - 1));
                    thread::sleep(Duration::from_millis(delay_ms));
                }
            }
        }
    }

    if let Some(error) = last_error {
        Err(error)
            .context(format!("daemon health check failed after {} attempts", MAX_HEALTH_ATTEMPTS))
    } else {
        anyhow::bail!("daemon health check failed with no captured error")
    }
}

#[derive(Clone, Copy)]
struct DoctorCheck {
    key: &'static str,
    ok: bool,
    required: bool,
}

#[derive(Debug, Deserialize)]
struct AdminStatusResponse {
    service: String,
    status: String,
    transport: AdminTransportSnapshot,
    counters: AdminCountersSnapshot,
}

#[derive(Debug, Deserialize)]
struct AdminTransportSnapshot {
    grpc_bind_addr: String,
    grpc_port: u16,
    quic_enabled: bool,
}

#[derive(Debug, Deserialize)]
struct AdminCountersSnapshot {
    denied_requests: u64,
    journal_events: u64,
}

#[derive(Debug, Deserialize)]
struct JournalRecentResponse {
    total_events: u64,
    hash_chain_enabled: bool,
    events: Vec<JournalRecentEvent>,
}

#[derive(Debug, Deserialize)]
struct JournalRecentEvent {
    event_id: String,
    kind: i32,
    actor: i32,
    redacted: bool,
    timestamp_unix_ms: i64,
    hash: Option<String>,
}

#[derive(Debug, Serialize)]
struct RunCancelRequestBody {
    reason: String,
}

#[derive(Debug, Deserialize)]
struct RunStatusResponse {
    run_id: String,
    state: String,
    cancel_requested: bool,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    tape_events: u64,
}

#[derive(Debug, Deserialize)]
struct RunTapeResponse {
    run_id: String,
    events: Vec<RunTapeEvent>,
}

#[derive(Debug, Deserialize)]
struct RunTapeEvent {
    seq: i64,
    event_type: String,
    payload_json: String,
}

#[derive(Debug, Deserialize)]
struct RunCancelResponse {
    run_id: String,
    cancel_requested: bool,
    reason: String,
}

#[cfg(test)]
mod cli_v1_tests {
    use super::{is_retryable_grpc_error, normalize_client_socket, parse_acp_shim_input_line};
    use std::net::SocketAddr;

    #[test]
    fn ndjson_stdin_uses_top_level_allow_sensitive_tools_default() {
        let request = parse_acp_shim_input_line(
            r#"{"session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAW","run_id":"01ARZ3NDEKTSV4RRFFQ69G5FAX","prompt":"hello"}"#,
            1,
            true,
        )
        .expect("NDJSON line should parse");
        assert!(request.allow_sensitive_tools);
    }

    #[test]
    fn ndjson_stdin_rejects_whitespace_only_prompt() {
        let result = parse_acp_shim_input_line(
            "{\"session_id\":\"01ARZ3NDEKTSV4RRFFQ69G5FAW\",\"run_id\":\"01ARZ3NDEKTSV4RRFFQ69G5FAX\",\"prompt\":\"   \"}",
            1,
            false,
        );
        let error = result.expect_err("whitespace-only prompt must be rejected");
        assert!(error.to_string().contains("non-empty text"), "unexpected error message: {error}");
    }

    #[test]
    fn normalize_client_socket_maps_unspecified_addresses_to_loopback() {
        let ipv4_unspecified: SocketAddr = "0.0.0.0:7443".parse().expect("valid socket addr");
        let ipv6_unspecified: SocketAddr = "[::]:7443".parse().expect("valid socket addr");
        let named: SocketAddr = "127.0.0.1:7443".parse().expect("valid socket addr");

        assert_eq!(normalize_client_socket(ipv4_unspecified).to_string(), "127.0.0.1:7443");
        assert_eq!(normalize_client_socket(ipv6_unspecified).to_string(), "[::1]:7443");
        assert_eq!(normalize_client_socket(named).to_string(), "127.0.0.1:7443");
    }

    #[test]
    fn grpc_retry_only_for_retryable_status_codes() {
        let unavailable = anyhow::Error::new(tonic::Status::unavailable("transient"));
        let invalid_argument = anyhow::Error::new(tonic::Status::invalid_argument("invalid"));

        assert!(is_retryable_grpc_error(&unavailable));
        assert!(!is_retryable_grpc_error(&invalid_argument));
    }
}

#[cfg(all(test, not(windows)))]
mod tests {
    use super::{default_identity_store_root_from_env, resolve_pairing_proof};
    use anyhow::Result;
    use std::ffi::OsString;
    use std::path::PathBuf;

    #[test]
    fn identity_store_defaults_to_xdg_state_home_when_available() -> Result<()> {
        let root = default_identity_store_root_from_env(
            Some(OsString::from("/tmp/xdg-state")),
            Some(OsString::from("/tmp/home")),
        )?;
        assert_eq!(root, PathBuf::from("/tmp/xdg-state").join("palyra").join("identity"));
        Ok(())
    }

    #[test]
    fn identity_store_falls_back_to_home_state_directory() -> Result<()> {
        let root = default_identity_store_root_from_env(None, Some(OsString::from("/tmp/home")))?;
        assert_eq!(
            root,
            PathBuf::from("/tmp/home").join(".local").join("state").join("palyra").join("identity")
        );
        Ok(())
    }

    #[test]
    fn resolve_pairing_proof_accepts_explicit_value() {
        let proof = resolve_pairing_proof(Some("123456".to_owned()), false, true)
            .expect("proof should resolve");
        assert_eq!(proof, "123456");
    }

    #[test]
    fn resolve_pairing_proof_requires_value_or_stdin_flag() {
        let result = resolve_pairing_proof(None, false, false);
        assert!(result.is_err(), "proof resolution should fail without any proof source");
    }

    #[test]
    fn resolve_pairing_proof_rejects_explicit_value_without_insecure_ack() {
        let result = resolve_pairing_proof(Some("123456".to_owned()), false, false);
        assert!(
            result.is_err(),
            "proof from CLI arg must require explicit insecure acknowledgment"
        );
    }
}
