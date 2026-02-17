mod cli;

use std::{env, fs, io::Write, path::Path, process::Command, thread, time::Duration};
#[cfg(not(windows))]
use std::{ffi::OsString, io::BufRead, path::PathBuf, sync::Arc, time::SystemTime};

use anyhow::{Context, Result};
use clap::Parser;
use cli::{
    Cli, Command as CliCommand, ConfigCommand, DaemonCommand, PolicyCommand, ProtocolCommand,
};
#[cfg(not(windows))]
use cli::{PairingClientKindArg, PairingCommand, PairingMethodArg};
use palyra_common::{
    build_metadata,
    config_system::{
        format_toml_value, get_value_at_path, parse_document_with_migration,
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

const MAX_HEALTH_ATTEMPTS: usize = 3;
const BASE_HEALTH_BACKOFF_MS: u64 = 100;
const DEFAULT_GATEWAY_GRPC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GATEWAY_GRPC_PORT: u16 = 7443;
const DEFAULT_GATEWAY_QUIC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GATEWAY_QUIC_PORT: u16 = 7444;
const DEFAULT_GATEWAY_QUIC_ENABLED: bool = true;

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        CliCommand::Version => print_version(),
        CliCommand::Doctor { strict } => run_doctor(strict),
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

fn run_daemon(command: DaemonCommand) -> Result<()> {
    match command {
        DaemonCommand::Status { url } => {
            let base_url = url
                .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
                .unwrap_or_else(|| "http://127.0.0.1:7142".to_owned());
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
                .unwrap_or_else(|| "http://127.0.0.1:7142".to_owned());
            let status_url = format!("{}/admin/v1/status", base_url.trim_end_matches('/'));
            let token = token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .context("failed to build HTTP client")?;
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

            let response: AdminStatusResponse = request
                .send()
                .context("failed to call daemon admin status endpoint")?
                .error_for_status()
                .context("daemon admin status endpoint returned non-success status")?
                .json()
                .context("failed to parse daemon admin status payload")?;

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
