mod cli;

use std::{
    env,
    ffi::OsString,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use clap::Parser;
use cli::{
    Cli, Command as CliCommand, ConfigCommand, DaemonCommand, PairingClientKindArg, PairingCommand,
    PairingMethodArg, PolicyCommand, ProtocolCommand,
};
use palyra_common::{
    build_metadata, validate_canonical_id, HealthResponse, CANONICAL_JSON_ENVELOPE_VERSION,
    CANONICAL_PROTOCOL_MAJOR,
};
#[cfg(not(windows))]
use palyra_identity::FilesystemSecretStore;
use palyra_identity::{
    DeviceIdentity, IdentityManager, PairingClientKind, PairingMethod, SecretStore,
    DEFAULT_CERT_VALIDITY,
};
use palyra_policy::{evaluate, PolicyDecision, PolicyRequest};
use reqwest::blocking::Client;

const MAX_HEALTH_ATTEMPTS: usize = 3;
const BASE_HEALTH_BACKOFF_MS: u64 = 100;

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        CliCommand::Version => print_version(),
        CliCommand::Doctor { strict } => run_doctor(strict),
        CliCommand::Daemon { command } => run_daemon(command),
        CliCommand::Policy { command } => run_policy(command),
        CliCommand::Protocol { command } => run_protocol(command),
        CliCommand::Config { command } => run_config(command),
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
    }
}

fn run_policy(command: PolicyCommand) -> Result<()> {
    match command {
        PolicyCommand::Explain { principal, action, resource } => {
            let request = PolicyRequest { principal, action, resource };
            let decision = evaluate(&request);
            match decision {
                PolicyDecision::Allow => {
                    println!(
                        "decision=allow principal={} action={} resource={}",
                        request.principal, request.action, request.resource
                    );
                }
                PolicyDecision::DenyByDefault { reason } => {
                    println!(
                        "decision=deny_by_default principal={} action={} resource={} approval_required=true reason={}",
                        request.principal, request.action, request.resource, reason
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
                Some(explicit_path) => {
                    if !Path::new(&explicit_path).exists() {
                        anyhow::bail!("config file does not exist: {}", explicit_path);
                    }
                    explicit_path
                }
                None => {
                    if let Some(found) = find_default_config_path() {
                        found
                    } else {
                        println!("config=valid source=defaults");
                        return std::io::stdout().flush().context("stdout flush failed");
                    }
                }
            };

            let content =
                fs::read_to_string(&path).with_context(|| format!("failed to read {}", path))?;
            validate_daemon_compatible_config(&content)
                .with_context(|| format!("failed to parse {}", path))?;
            println!("config=valid source={path}");
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidatedRootConfig {
    #[allow(dead_code)]
    daemon: Option<ValidatedDaemonConfig>,
    #[allow(dead_code)]
    identity: Option<ValidatedIdentityConfig>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidatedDaemonConfig {
    #[allow(dead_code)]
    bind_addr: Option<String>,
    #[allow(dead_code)]
    port: Option<u16>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidatedIdentityConfig {
    #[allow(dead_code)]
    allow_insecure_node_rpc_without_mtls: Option<bool>,
}

fn validate_daemon_compatible_config(content: &str) -> Result<()> {
    let _: ValidatedRootConfig = toml::from_str(content).context("invalid daemon config schema")?;
    Ok(())
}

fn find_default_config_path() -> Option<String> {
    for candidate in ["palyra.toml", "Palyra.toml", "config/palyra.toml"] {
        if Path::new(candidate).exists() {
            return Some(candidate.to_owned());
        }
    }

    None
}

fn run_pairing(command: PairingCommand) -> Result<()> {
    match command {
        PairingCommand::Pair {
            device_id,
            client_kind,
            method,
            proof,
            store_dir,
            approve,
            simulate_rotation,
        } => {
            if !approve {
                anyhow::bail!(
                    "decision=deny_by_default approval_required=true reason=pairing requires explicit --approve"
                );
            }

            let now = SystemTime::now();
            let store_root = resolve_identity_store_root(store_dir)?;
            let store = build_identity_store(&store_root)?;
            let mut manager = IdentityManager::with_store(store.clone())
                .context("failed to initialize identity manager")?;
            let pairing_method = build_pairing_method(method, &proof);

            let session = manager
                .start_pairing(to_identity_client_kind(client_kind), pairing_method, now)
                .context("failed to start pairing session")?;
            let device = DeviceIdentity::generate(&device_id)
                .context("failed to generate device identity")?;
            device.store(store.as_ref()).context("failed to persist device identity")?;

            let hello = manager
                .build_device_hello(&session, &device, &proof)
                .context("failed to build device pairing hello")?;
            let result = manager
                .complete_pairing(hello, now)
                .context("failed to complete pairing handshake")?;

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
                    .rotate_device_certificate_if_due(&device_id, now + DEFAULT_CERT_VALIDITY)
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

fn resolve_identity_store_root(store_dir: Option<String>) -> Result<PathBuf> {
    if let Some(path) = store_dir {
        return Ok(PathBuf::from(path));
    }
    #[cfg(windows)]
    {
        default_identity_store_root_from_env(env::var_os("LOCALAPPDATA"))
    }
    #[cfg(not(windows))]
    {
        default_identity_store_root_from_env(env::var_os("XDG_STATE_HOME"), env::var_os("HOME"))
    }
}

#[cfg(windows)]
fn default_identity_store_root_from_env(localappdata: Option<OsString>) -> Result<PathBuf> {
    let base = localappdata.map(PathBuf::from).context("LOCALAPPDATA is not set")?;
    Ok(base.join("Palyra").join("identity"))
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

fn build_identity_store(store_root: &Path) -> Result<Arc<dyn SecretStore>> {
    #[cfg(windows)]
    {
        anyhow::bail!(
            "persistent identity storage is not available on Windows yet; refusing volatile pairing state at {}",
            store_root.display()
        );
    }
    #[cfg(not(windows))]
    {
        let store = FilesystemSecretStore::new(store_root).with_context(|| {
            format!("failed to initialize secret store at {}", store_root.display())
        })?;
        Ok(Arc::new(store))
    }
}

fn build_pairing_method(method: PairingMethodArg, proof: &str) -> PairingMethod {
    match method {
        PairingMethodArg::Pin => PairingMethod::Pin { code: proof.to_owned() },
        PairingMethodArg::Qr => PairingMethod::Qr { token: proof.to_owned() },
    }
}

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

#[cfg(test)]
mod tests {
    use super::default_identity_store_root_from_env;
    use anyhow::Result;
    use std::ffi::OsString;
    use std::path::PathBuf;

    #[cfg(not(windows))]
    #[test]
    fn identity_store_defaults_to_xdg_state_home_when_available() -> Result<()> {
        let root = default_identity_store_root_from_env(
            Some(OsString::from("/tmp/xdg-state")),
            Some(OsString::from("/tmp/home")),
        )?;
        assert_eq!(root, PathBuf::from("/tmp/xdg-state").join("palyra").join("identity"));
        Ok(())
    }

    #[cfg(not(windows))]
    #[test]
    fn identity_store_falls_back_to_home_state_directory() -> Result<()> {
        let root = default_identity_store_root_from_env(None, Some(OsString::from("/tmp/home")))?;
        assert_eq!(
            root,
            PathBuf::from("/tmp/home").join(".local").join("state").join("palyra").join("identity")
        );
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn identity_store_uses_localappdata_on_windows() -> Result<()> {
        let root = default_identity_store_root_from_env(Some(OsString::from(
            r"C:\Users\Test\AppData\Local",
        )))?;
        assert_eq!(
            root,
            PathBuf::from(r"C:\Users\Test\AppData\Local").join("Palyra").join("identity")
        );
        Ok(())
    }
}
