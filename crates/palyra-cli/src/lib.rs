mod acp_bridge;
pub mod app;
pub mod args;
mod cli;
pub mod cli_parity;
pub mod client;
mod commands;
pub mod domain;
pub mod infra;
pub mod output;
pub mod shared_chat_commands;
pub mod support;
pub mod transport;
mod tui;

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

        pub mod cron {
            pub mod v1 {
                tonic::include_proto!("palyra.cron.v1");
            }
        }

        pub mod memory {
            pub mod v1 {
                tonic::include_proto!("palyra.memory.v1");
            }
        }

        pub mod node {
            pub mod v1 {
                tonic::include_proto!("palyra.node.v1");
            }
        }

        pub mod auth {
            pub mod v1 {
                tonic::include_proto!("palyra.auth.v1");
            }
        }

        pub mod browser {
            pub mod v1 {
                tonic::include_proto!("palyra.browser.v1");
            }
        }
    }
}

use std::sync::Arc;
use std::{
    collections::{BTreeMap, HashSet},
    env, fs,
    io::{BufRead, IsTerminal, Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Component, Path, PathBuf},
    process::Command,
    process::ExitCode,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use clap::{CommandFactory, Parser};
use cli::{
    AcpCommand, AgentCommand, AgentsCommand, ApprovalDecisionArg, ApprovalExportFormatArg,
    ApprovalsCommand, AuthAccessCommand, AuthCommand, AuthCredentialArg, AuthOpenAiCommand,
    AuthProfilesCommand, AuthProviderArg, AuthScopeArg, BrowserCommand, Cli, Command as CliCommand,
    CompletionShell, ConfigCommand, ConfigureSectionArg, CronCommand, DaemonCommand, DocsCommand,
    GatewayBindProfileArg, HooksCommand, InitModeArg, InitTlsScaffoldArg, JournalCheckpointModeArg,
    MemoryCommand, MemoryLearningCommand, MemoryScopeArg, MemorySourceArg, ModelsCommand,
    OnboardingAuthMethodArg, OnboardingCommand, OnboardingFlowArg, PatchCommand, PluginsCommand,
    PolicyCommand, ProtocolCommand, RemoteVerificationModeArg, SandboxCommand, SandboxRuntimeArg,
    SecretsCommand, SecurityCommand, SessionsCommand, SetupWizardOverridesArg, SkillsCommand,
    SkillsPackageCommand, SupportBundleCommand, SystemCommand, SystemEventCommand,
    SystemEventSeverityArg, WebhooksCommand, WizardOverridesArg, WorkspaceRoleArg,
};
use cli::{PairingClientKindArg, PairingCommand, PairingMethodArg};
use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use palyra_common::default_identity_store_root;
use palyra_common::{
    build_metadata,
    config_system::{
        backup_path, format_toml_value, get_value_at_path, parse_document_with_migration,
        parse_toml_value_literal, recover_config_from_backup, serialize_document_pretty,
        set_value_at_path, unset_value_at_path, write_document_with_backups, ConfigMigrationInfo,
    },
    daemon_config_schema::{is_secret_config_path, redact_secret_config_values, RootFileConfig},
    default_config_search_paths, parse_config_path, parse_daemon_bind_socket,
    redaction::{
        is_sensitive_key, redact_auth_error, redact_url, redact_url_segments_in_text, REDACTED,
    },
    validate_canonical_id,
    workspace_patch::{
        apply_workspace_patch, compute_patch_sha256, redact_patch_preview, WorkspacePatchLimits,
        WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
    },
    HealthResponse, CANONICAL_JSON_ENVELOPE_VERSION, CANONICAL_PROTOCOL_MAJOR,
};
use palyra_identity::{
    DeviceIdentity, FilesystemSecretStore, PairingClientKind, PairingMethod, SecretStore,
};
use palyra_policy::{evaluate_with_config, PolicyDecision, PolicyEvaluationConfig, PolicyRequest};
use palyra_skills::{
    audit_skill_artifact_security, build_signed_skill_artifact, inspect_skill_artifact,
    parse_ed25519_signing_key, verify_skill_artifact, ArtifactFile, SkillArtifactBuildRequest,
    SkillArtifactSignature, SkillAuditCheckStatus, SkillManifest, SkillSecurityAuditPolicy,
    SkillSecurityAuditReport, SkillTrustStore, SkillVerificationReport, TrustDecision,
};
use palyra_vault::{
    BackendPreference as VaultBackendPreference, Vault, VaultConfig as VaultConfigOptions,
    VaultError, VaultRef, VaultScope,
};
use reqwest::blocking::Client;
use reqwest::redirect::Policy as RedirectPolicy;
use reqwest::tls::TlsInfo;
use reqwest::Url;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tonic::Request;
use ulid::Ulid;

use crate::proto::palyra::{
    auth::v1 as auth_v1, browser::v1 as browser_v1, common::v1 as common_v1,
    gateway::v1 as gateway_v1, memory::v1 as memory_v1,
};

const MAX_HEALTH_ATTEMPTS: usize = 3;
const BASE_HEALTH_BACKOFF_MS: u64 = 100;
const MAX_GRPC_ATTEMPTS: usize = 3;
const BASE_GRPC_BACKOFF_MS: u64 = 100;
const RUN_STREAM_REQUEST_VERSION: u32 = 1;
const DEFAULT_DAEMON_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_DAEMON_PORT: u16 = 7142;
const DEFAULT_GATEWAY_GRPC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GATEWAY_GRPC_PORT: u16 = 7443;
const DEFAULT_GATEWAY_QUIC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GATEWAY_QUIC_PORT: u16 = 7444;
const DEFAULT_GATEWAY_QUIC_ENABLED: bool = true;
const DEFAULT_GATEWAY_BIND_PROFILE: &str = "loopback_only";
const DEFAULT_DEPLOYMENT_MODE: &str = "local_desktop";
pub(crate) const DEFAULT_DAEMON_URL: &str = "http://127.0.0.1:7142";
const DEFAULT_BROWSER_SERVICE_ENDPOINT: &str = "http://127.0.0.1:7543";
const DEFAULT_JOURNAL_DB_PATH: &str = "data/journal.sqlite3";
const DEFAULT_BROWSER_URL: &str = "http://127.0.0.1:7143";
const DEFAULT_CHANNEL: &str = "cli";
const DEFAULT_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const REDACTED_CONFIG_VALUE: &str = "<redacted>";
const GATEWAY_CA_STATE_KEY: &str = "identity/ca/state.json";
const SKILLS_LAYOUT_VERSION: u32 = 1;
const SKILLS_INDEX_FILE_NAME: &str = "installed-index.json";
const SKILLS_AUDIT_FILE_NAME: &str = "audit.ndjson";
const SKILLS_INSTALL_METADATA_FILE_NAME: &str = "install-metadata.json";
const SKILLS_ARTIFACT_FILE_NAME: &str = "artifact.palyra-skill";
const SKILLS_CURRENT_LINK_NAME: &str = "current";
const REGISTRY_INDEX_FILE_NAME: &str = "index.json";
const REGISTRY_INDEX_SCHEMA_VERSION: u32 = 1;
const REGISTRY_SIGNED_INDEX_SCHEMA_VERSION: u32 = 1;
const MAX_REGISTRY_INDEX_BYTES: usize = 2 * 1024 * 1024;
const MAX_REGISTRY_ENTRIES: usize = 10_000;
const MAX_REGISTRY_PAGES: usize = 20;
const REGISTRY_SIGNATURE_ALGORITHM: &str = "ed25519-sha256";
const JOURNAL_CHECKPOINT_ATTESTATION_SCHEMA_VERSION: u32 = 1;
const JOURNAL_CHECKPOINT_ATTESTATION_ALGORITHM: &str = "ed25519-sha256";
const DANGEROUS_REMOTE_BIND_ACK_ENV: &str = "PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK";
const TRUST_STORE_INTEGRITY_VAULT_SCOPE: VaultScope = VaultScope::Global;
const TRUST_STORE_INTEGRITY_VAULT_KEY_PREFIX: &str = "skills.trust_store.integrity.";

pub fn run() -> ExitCode {
    match run_cli_entrypoint() {
        Ok(()) => output::CliExitCode::Success.as_exit_code(),
        Err(error) => match output::emit_error(&error) {
            Ok(exit_code) => exit_code.as_exit_code(),
            Err(emit_error) => {
                eprintln!("error[internal_error] failed to render CLI error: {emit_error}");
                output::CliExitCode::Internal.as_exit_code()
            }
        },
    }
}

#[cfg(windows)]
fn run_cli_entrypoint() -> Result<()> {
    const CLI_MAIN_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

    thread::Builder::new()
        .name("palyra-cli-main".to_owned())
        .stack_size(CLI_MAIN_STACK_SIZE_BYTES)
        .spawn(run_cli)
        .context("failed to spawn CLI main thread")?
        .join()
        .map_err(|_| anyhow!("CLI main thread panicked"))?
}

#[cfg(not(windows))]
fn run_cli_entrypoint() -> Result<()> {
    run_cli()
}

fn run_cli() -> Result<()> {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) =>
        {
            error.print().context("failed to print clap display output")?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let _root_context = app::install_root_context(cli.root.clone())?;
    enforce_profile_guardrails(&cli.command)?;
    match cli.command {
        CliCommand::Version => print_version(),
        CliCommand::Setup { mode, path, force, tls_scaffold, wizard, wizard_options } => {
            commands::setup::run_setup(mode, path, force, tls_scaffold, wizard, wizard_options)
        }
        CliCommand::Doctor { strict, json, repair, dry_run, force, only, skip, rollback_run } => {
            commands::doctor::run_doctor(commands::doctor::DoctorCommandRequest {
                strict,
                json,
                repair,
                dry_run,
                force,
                only,
                skip,
                rollback_run,
            })
        }
        CliCommand::Health { url, grpc_url } => commands::health::run_health(url, grpc_url),
        CliCommand::Logs { db_path, lines, follow, poll_interval_ms } => {
            commands::logs::run_logs(db_path, lines, follow, poll_interval_ms)
        }
        CliCommand::Status { url, grpc_url, admin, token, principal, device_id, channel } => {
            commands::status::run_status(url, grpc_url, admin, token, principal, device_id, channel)
        }
        CliCommand::Acp { command } => commands::acp::run_acp(command),
        CliCommand::Mcp { command } => commands::mcp::run_mcp(command),
        CliCommand::Agent { command } => commands::agent::run_agent(command),
        CliCommand::Agents { command } => commands::agents::run_agents(command),
        CliCommand::Routines { command } => commands::routines::run_routines(command),
        CliCommand::Objectives { command } => commands::objectives::run_objectives(command),
        CliCommand::Cron { command } => commands::cron::run_cron(command),
        CliCommand::Memory { command } => commands::memory::run_memory(command),
        CliCommand::Message { command } => commands::message::run_message(command),
        CliCommand::Approvals { command } => commands::approvals::run_approvals(command),
        CliCommand::Devices { command } => commands::devices::run_devices(command),
        CliCommand::Sessions { command } => commands::sessions::run_sessions(command),
        CliCommand::Tui { command } => commands::tui::run_tui(command),
        CliCommand::Auth { command } => commands::auth::run_auth(command),
        CliCommand::Channels { command } => commands::channels::run(command),
        CliCommand::Node { command } => commands::node::run_node(command),
        CliCommand::Nodes { command } => commands::nodes::run_nodes(command),
        CliCommand::Webhooks { command } => commands::webhooks::run_webhooks(command),
        CliCommand::Docs { command } => commands::docs::run_docs(command),
        CliCommand::Plugins { command } => commands::plugins::run_plugins(command),
        CliCommand::Hooks { command } => commands::hooks::run_hooks(command),
        CliCommand::Profile { command } => commands::profile::run_profile(command),
        CliCommand::Browser { command } => commands::browser::run_browser(command),
        CliCommand::System { command } => commands::system::run_system(command),
        CliCommand::Sandbox { command } => commands::sandbox::run_sandbox(command),
        CliCommand::Completion { shell } => commands::completion::run_completion(shell),
        CliCommand::Onboarding { command } => commands::onboarding::run_onboarding(command),
        CliCommand::Configure {
            path,
            sections,
            non_interactive,
            accept_risk,
            json,
            workspace_root,
            auth_method,
            api_key_env,
            api_key_stdin,
            api_key_prompt,
            bind_profile,
            daemon_port,
            grpc_port,
            quic_port,
            tls_scaffold,
            tls_cert_path,
            tls_key_path,
            remote_base_url,
            admin_token_env,
            admin_token_stdin,
            admin_token_prompt,
            remote_verification,
            pinned_server_cert_sha256,
            pinned_gateway_ca_sha256,
            ssh_target,
            skip_health,
            skip_channels,
            skip_skills,
        } => commands::configure::run_configure(
            path,
            sections,
            non_interactive,
            accept_risk,
            json,
            workspace_root,
            auth_method,
            api_key_env,
            api_key_stdin,
            api_key_prompt,
            bind_profile,
            daemon_port,
            grpc_port,
            quic_port,
            tls_scaffold,
            tls_cert_path,
            tls_key_path,
            remote_base_url,
            admin_token_env,
            admin_token_stdin,
            admin_token_prompt,
            remote_verification,
            pinned_server_cert_sha256,
            pinned_gateway_ca_sha256,
            ssh_target,
            skip_health,
            skip_channels,
            skip_skills,
        ),
        CliCommand::Gateway { command } => commands::daemon::run_daemon(command),
        CliCommand::Dashboard { path, verify_remote, identity_store_dir, open, json } => {
            commands::daemon::run_daemon(DaemonCommand::DashboardUrl {
                path,
                verify_remote,
                identity_store_dir,
                open,
                json,
            })
        }
        CliCommand::Backup { command } => commands::backup::run_backup(command),
        CliCommand::Reset { command } => commands::reset::run_reset(command),
        CliCommand::Uninstall { command } => commands::uninstall::run_uninstall(command),
        CliCommand::Update { command } => commands::update::run_update(command),
        CliCommand::SupportBundle { command } => {
            commands::support_bundle::run_support_bundle(command)
        }
        CliCommand::Policy { command } => commands::policy::run_policy(command),
        CliCommand::Protocol { command } => commands::protocol::run_protocol(command),
        CliCommand::Config { command } => commands::config::run_config(command),
        CliCommand::Models { command } => commands::models::run_models(command),
        CliCommand::Patch { command } => commands::patch::run_patch(command),
        CliCommand::Skills { command } => commands::skills::run_skills(command),
        CliCommand::Secrets { command } => commands::secrets::run_secrets(command),
        CliCommand::Security { command } => commands::security::run_security(command),
        CliCommand::Pairing { command } => commands::pairing::run_pairing(command),
        CliCommand::Tunnel { ssh, remote_port, local_port, open, identity_file } => {
            commands::tunnel::run_tunnel(ssh, remote_port, local_port, open, identity_file)
        }
    }
}

fn enforce_profile_guardrails(command: &CliCommand) -> Result<()> {
    let Some(context) = app::current_root_context() else {
        return Ok(());
    };
    let Some(profile) = context.active_profile_context() else {
        return Ok(());
    };
    if !profile.strict_mode || context.allow_strict_profile_actions {
        return Ok(());
    }
    if !is_strict_profile_blocked_command(command) {
        return Ok(());
    }
    anyhow::bail!(
        "command is blocked by strict profile posture for `{}` (environment={}, risk_level={}); re-run with --allow-strict-profile-actions if this destructive action is intentional",
        profile.name,
        profile.environment,
        profile.risk_level
    );
}

fn is_strict_profile_blocked_command(command: &CliCommand) -> bool {
    match command {
        CliCommand::Reset { command } => !command.dry_run && command.yes,
        CliCommand::Uninstall { command } => !command.dry_run && command.yes,
        CliCommand::Profile { command } => matches!(
            command,
            crate::cli::ProfileCommand::Delete { yes: true, delete_state_root: true, .. }
                | crate::cli::ProfileCommand::Delete { yes: true, .. }
        ),
        _ => false,
    }
}

fn print_version() -> Result<()> {
    let build = build_metadata();
    if let Some(context) = app::current_root_context() {
        let profile = context.active_profile_context();
        if context.prefers_json() {
            return output::print_json_pretty(
                &json!({
                    "name": "palyra",
                    "version": build.version,
                    "git_hash": build.git_hash,
                    "build_profile": build.build_profile,
                    "trace_id": context.trace_id(),
                    "profile": context.profile_name(),
                    "profile_context": profile,
                    "state_root": context.state_root().display().to_string(),
                    "log_level": format!("{:?}", context.log_level()).to_ascii_lowercase(),
                    "no_color": context.no_color(),
                }),
                "failed to encode version output as JSON",
            );
        }
        if context.prefers_ndjson() {
            return output::print_json_line(
                &json!({
                    "name": "palyra",
                    "version": build.version,
                    "git_hash": build.git_hash,
                    "build_profile": build.build_profile,
                    "trace_id": context.trace_id(),
                    "profile": context.profile_name(),
                    "profile_context": profile,
                    "state_root": context.state_root().display().to_string(),
                    "log_level": format!("{:?}", context.log_level()).to_ascii_lowercase(),
                    "no_color": context.no_color(),
                }),
                "failed to encode version output as NDJSON",
            );
        }
        if let Some(profile) = profile {
            println!(
                "name=palyra version={} git_hash={} build_profile={} profile={} environment={} risk_level={} strict_mode={}",
                build.version,
                build.git_hash,
                build.build_profile,
                profile.label,
                profile.environment,
                profile.risk_level,
                profile.strict_mode
            );
            return std::io::stdout().flush().context("stdout flush failed");
        }
    }
    println!(
        "name=palyra version={} git_hash={} build_profile={}",
        build.version, build.git_hash, build.build_profile
    );
    std::io::stdout().flush().context("stdout flush failed")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitMode {
    LocalDesktop,
    RemoteVps,
}

impl InitMode {
    fn from_arg(value: InitModeArg) -> Self {
        match value {
            InitModeArg::Local => Self::LocalDesktop,
            InitModeArg::Remote => Self::RemoteVps,
        }
    }

    const fn deployment_mode(self) -> &'static str {
        match self {
            Self::LocalDesktop => "local_desktop",
            Self::RemoteVps => "remote_vps",
        }
    }
}

fn resolve_init_path(path: Option<String>) -> Result<PathBuf> {
    if let Some(path) = path {
        return parse_config_path(path.as_str())
            .with_context(|| format!("init config path is invalid: {}", path));
    }
    Ok(PathBuf::from("palyra.toml"))
}

fn resolve_init_state_root() -> Result<PathBuf> {
    if let Some(context) = app::current_root_context() {
        return Ok(context.state_root().to_path_buf());
    }
    app::resolve_cli_state_root(None)
}

fn generate_admin_token() -> String {
    format!("palyra_admin_{}_{}", Ulid::new(), Ulid::new())
}

const DEFAULT_ADMIN_BOUND_PRINCIPAL: &str = "admin:local";

fn build_init_config_document(
    mode: InitMode,
    identity_store_dir: &Path,
    vault_dir: &Path,
    admin_token: &str,
    tls_paths: Option<&(PathBuf, PathBuf)>,
) -> Result<toml::Value> {
    let (mut document, _) =
        parse_document_with_migration("").context("failed to initialize config document")?;
    set_value_at_path(
        &mut document,
        "deployment.mode",
        toml::Value::String(mode.deployment_mode().to_owned()),
    )?;
    set_value_at_path(
        &mut document,
        "deployment.dangerous_remote_bind_ack",
        toml::Value::Boolean(false),
    )?;
    set_value_at_path(
        &mut document,
        "daemon.bind_addr",
        toml::Value::String(DEFAULT_DAEMON_BIND_ADDR.to_owned()),
    )?;
    set_value_at_path(
        &mut document,
        "daemon.port",
        toml::Value::Integer(i64::from(DEFAULT_DAEMON_PORT)),
    )?;
    set_value_at_path(
        &mut document,
        "gateway.grpc_bind_addr",
        toml::Value::String(DEFAULT_GATEWAY_GRPC_BIND_ADDR.to_owned()),
    )?;
    set_value_at_path(
        &mut document,
        "gateway.grpc_port",
        toml::Value::Integer(i64::from(DEFAULT_GATEWAY_GRPC_PORT)),
    )?;
    set_value_at_path(
        &mut document,
        "gateway.quic_bind_addr",
        toml::Value::String(DEFAULT_GATEWAY_QUIC_BIND_ADDR.to_owned()),
    )?;
    set_value_at_path(
        &mut document,
        "gateway.quic_port",
        toml::Value::Integer(i64::from(DEFAULT_GATEWAY_QUIC_PORT)),
    )?;
    set_value_at_path(
        &mut document,
        "gateway.quic_enabled",
        toml::Value::Boolean(DEFAULT_GATEWAY_QUIC_ENABLED),
    )?;
    set_value_at_path(
        &mut document,
        "gateway.bind_profile",
        toml::Value::String("loopback_only".to_owned()),
    )?;
    set_value_at_path(&mut document, "gateway.allow_insecure_remote", toml::Value::Boolean(false))?;
    set_value_at_path(
        &mut document,
        "gateway.identity_store_dir",
        toml::Value::String(identity_store_dir.to_string_lossy().into_owned()),
    )?;
    set_value_at_path(&mut document, "admin.require_auth", toml::Value::Boolean(true))?;
    set_value_at_path(
        &mut document,
        "admin.auth_token",
        toml::Value::String(admin_token.to_owned()),
    )?;
    set_value_at_path(
        &mut document,
        "admin.bound_principal",
        toml::Value::String(DEFAULT_ADMIN_BOUND_PRINCIPAL.to_owned()),
    )?;
    set_value_at_path(
        &mut document,
        "storage.vault_dir",
        toml::Value::String(vault_dir.to_string_lossy().into_owned()),
    )?;
    set_value_at_path(
        &mut document,
        "orchestrator.runloop_v1_enabled",
        toml::Value::Boolean(true),
    )?;

    if let Some((cert_path, key_path)) = tls_paths {
        set_value_at_path(&mut document, "gateway.tls.enabled", toml::Value::Boolean(false))?;
        set_value_at_path(
            &mut document,
            "gateway.tls.cert_path",
            toml::Value::String(cert_path.to_string_lossy().into_owned()),
        )?;
        set_value_at_path(
            &mut document,
            "gateway.tls.key_path",
            toml::Value::String(key_path.to_string_lossy().into_owned()),
        )?;
    }

    Ok(document)
}

fn emit_remote_init_guidance(
    tls_scaffold: InitTlsScaffoldArg,
    tls_paths: Option<&(PathBuf, PathBuf)>,
) -> Result<()> {
    println!("init.remote_guide.ssh_tunnel=ssh -L 7142:127.0.0.1:7142 <user>@<host>");
    println!(
        "init.remote_guide.reverse_proxy=terminate TLS at reverse proxy and forward to http://127.0.0.1:7142"
    );
    println!(
        "init.remote_guide.public_exposure=requires gateway.bind_profile=public_tls + gateway.tls.enabled=true + admin.require_auth=true + deployment.dangerous_remote_bind_ack=true + {}=true",
        DANGEROUS_REMOTE_BIND_ACK_ENV
    );

    if let Some((cert_path, key_path)) = tls_paths {
        println!(
            "init.remote_guide.tls_paths cert_path={} key_path={}",
            cert_path.display(),
            key_path.display()
        );
        if matches!(tls_scaffold, InitTlsScaffoldArg::SelfSigned) {
            println!(
                "init.remote_guide.self_signed_hint=openssl req -x509 -newkey rsa:4096 -keyout {} -out {} -days 365 -nodes -subj \"/CN=palyra.local\"",
                key_path.display(),
                cert_path.display()
            );
        }
    }

    Ok(())
}

fn build_doctor_checks() -> Vec<DoctorCheck> {
    vec![
        DoctorCheck::blocking("toolchain_ok", command_available("rustc", &["--version"]), &[]),
        DoctorCheck::blocking("cargo_ok", command_available("cargo", &["--version"]), &[]),
        DoctorCheck::blocking(
            "workspace_writable",
            is_workspace_writable().unwrap_or(false),
            &["palyra doctor", "palyra support-bundle export --output ./support-bundle.json"],
        ),
        build_doctor_repo_scaffold_check(),
        DoctorCheck::warning(
            "memory_embeddings_model_configured",
            memory_embeddings_model_config_ok(),
            &["palyra config validate", "palyra doctor"],
        ),
        DoctorCheck::warning(
            "process_runner_tier_b_egress_allowlists_preflight_only",
            process_runner_tier_b_allowlist_config_ok(),
            &["palyra security audit", "palyra doctor"],
        ),
        DoctorCheck::warning(
            "process_runner_tier_c_strict_offline_only",
            process_runner_tier_c_strict_offline_config_ok(),
            &["palyra security audit", "palyra doctor"],
        ),
        DoctorCheck::warning(
            "process_runner_tier_c_windows_backend_supported",
            process_runner_tier_c_windows_backend_config_ok(),
            &["palyra doctor", "palyra support-bundle export --output ./support-bundle.json"],
        ),
        DoctorCheck::warning(
            "gitleaks_installed",
            command_available("gitleaks", &["--version"]),
            &["palyra security audit"],
        ),
        DoctorCheck::blocking(
            "cargo_audit_installed",
            command_available("cargo", &["audit", "--version"]),
            &["cargo install cargo-audit"],
        ),
        DoctorCheck::blocking(
            "cargo_deny_installed",
            command_available("cargo", &["deny", "--version"]),
            &["cargo install cargo-deny"],
        ),
        DoctorCheck::info(
            "cargo_cyclonedx_installed",
            command_available("cargo", &["cyclonedx", "--version"]),
            &["cargo install cargo-cyclonedx"],
        ),
        DoctorCheck::warning(
            "osv_scanner_installed",
            command_available("osv-scanner", &["--version"]),
            &["palyra security audit"],
        ),
        DoctorCheck::info(
            "cargo_fuzz_installed",
            command_available("cargo", &["fuzz", "--help"]),
            &["cargo install cargo-fuzz"],
        ),
        DoctorCheck::blocking(
            "protoc_installed",
            command_available("protoc", &["--version"])
                || command_available("protoc.exe", &["--version"]),
            &["bash scripts/protocol/validate-proto.sh"],
        ),
        DoctorCheck::info(
            "swiftc_installed",
            command_available("swiftc", &["--version"]),
            &["bash scripts/protocol/validate-swift-stubs.sh"],
        ),
        DoctorCheck::info(
            "kotlinc_installed",
            command_available("kotlinc", &["-version"]),
            &["bash scripts/protocol/validate-kotlin-stubs.sh"],
        ),
        DoctorCheck::info(
            "just_installed",
            command_available("just", &["--version"]),
            &["just doctor"],
        ),
        DoctorCheck::info(
            "npm_installed",
            command_available("npm", &["--version"]),
            &["npm --prefix apps/web run build"],
        ),
        DoctorCheck::info("swiftlint_installed", command_available("swiftlint", &["version"]), &[]),
        DoctorCheck::info("detekt_installed", command_available("detekt", &["--version"]), &[]),
    ]
}

fn build_doctor_repo_scaffold_check() -> DoctorCheck {
    doctor_repo_scaffold_check(required_directories_ok(), doctor_repo_scaffold_required())
}

fn doctor_repo_scaffold_check(repo_scaffold_ok: bool, repo_scaffold_required: bool) -> DoctorCheck {
    if repo_scaffold_ok || repo_scaffold_required {
        DoctorCheck::blocking("repo_scaffold_ok", repo_scaffold_ok, &[])
    } else {
        DoctorCheck::info("repo_scaffold_ok", repo_scaffold_ok, &[])
    }
}

fn build_doctor_report(checks: &[DoctorCheck]) -> Result<DoctorReport> {
    let generated_at_unix_ms = now_unix_ms_i64()?;
    let profile = app::current_root_context().and_then(|context| context.active_profile_context());
    let config = collect_doctor_config_snapshot();
    let identity = collect_doctor_identity_snapshot();
    let (connectivity, admin_payload, admin_error) = collect_doctor_connectivity_snapshot();
    let provider_auth =
        collect_doctor_provider_auth_snapshot(admin_payload.as_ref(), admin_error.as_deref());
    let browser = collect_doctor_browser_snapshot(admin_payload.as_ref(), admin_error.as_deref());
    let access = collect_doctor_access_snapshot();
    let deployment = collect_doctor_deployment_snapshot();
    let skills = build_default_skills_inventory_snapshot();

    let required_checks_total = checks.iter().filter(|check| check.required).count();
    let required_checks_ok = checks.iter().filter(|check| check.required && check.ok).count();
    let required_checks_failed = required_checks_total.saturating_sub(required_checks_ok);
    let warning_checks_failed = checks
        .iter()
        .filter(|check| check.severity == DoctorSeverity::Warning && !check.ok)
        .count();
    let info_checks_failed =
        checks.iter().filter(|check| check.severity == DoctorSeverity::Info && !check.ok).count();

    Ok(DoctorReport {
        generated_at_unix_ms,
        profile,
        checks: checks.to_vec(),
        summary: DoctorSummary {
            required_checks_total,
            required_checks_ok,
            required_checks_failed,
            warning_checks_failed,
            info_checks_failed,
        },
        config,
        identity,
        connectivity,
        provider_auth,
        browser,
        access,
        skills,
        sandbox: DoctorSandboxSnapshot {
            tier_b_egress_allowlists_preflight_only: process_runner_tier_b_allowlist_config_ok(),
            tier_c_strict_offline_only: process_runner_tier_c_strict_offline_config_ok(),
            tier_c_windows_backend_supported: process_runner_tier_c_windows_backend_config_ok(),
        },
        deployment,
    })
}

fn collect_doctor_config_snapshot() -> DoctorConfigSnapshot {
    let path = doctor_config_path().map(|value| value.to_string_lossy().into_owned());
    let Some(path) = path else {
        return DoctorConfigSnapshot { path: None, exists: false, parsed: false, error: None };
    };
    let path_ref = PathBuf::from(path.as_str());
    if !path_ref.exists() {
        return DoctorConfigSnapshot {
            path: Some(path),
            exists: false,
            parsed: false,
            error: Some("configured path does not exist".to_owned()),
        };
    }

    match read_doctor_root_file_config() {
        Ok(Some(_)) => {
            DoctorConfigSnapshot { path: Some(path), exists: true, parsed: true, error: None }
        }
        Ok(None) => DoctorConfigSnapshot {
            path: Some(path),
            exists: true,
            parsed: false,
            error: Some("config path resolved but no config was loaded".to_owned()),
        },
        Err(error) => DoctorConfigSnapshot {
            path: Some(path),
            exists: true,
            parsed: false,
            error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
        },
    }
}

fn collect_doctor_identity_snapshot() -> DoctorIdentitySnapshot {
    match default_identity_store_root() {
        Ok(store_root) => {
            let exists = store_root.exists();
            let writable = is_directory_writable(store_root.as_path()).unwrap_or(false);
            DoctorIdentitySnapshot {
                store_root: Some(store_root.to_string_lossy().into_owned()),
                exists,
                writable,
                error: None,
            }
        }
        Err(error) => DoctorIdentitySnapshot {
            store_root: None,
            exists: false,
            writable: false,
            error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
        },
    }
}

fn collect_doctor_connectivity_snapshot(
) -> (DoctorConnectivitySnapshot, Option<Value>, Option<String>) {
    let daemon_url = env::var("PALYRA_DAEMON_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
    let status_url = format!("{}/healthz", daemon_url.trim_end_matches('/'));
    let grpc_url = resolve_grpc_url(None).unwrap_or_else(|error| format!("unresolved:{error}"));

    let mut http_probe = DoctorConnectivityProbe { ok: false, message: None };
    let mut grpc_probe = DoctorConnectivityProbe { ok: false, message: None };
    let mut admin_probe = DoctorConnectivityProbe { ok: false, message: None };
    let mut admin_payload = None;
    let mut admin_error = None;

    let http_client = match Client::builder().timeout(Duration::from_secs(2)).build() {
        Ok(client) => Some(client),
        Err(error) => {
            let message = sanitize_diagnostic_error(error.to_string().as_str());
            http_probe.message = Some(format!("http client init failed: {message}"));
            grpc_probe.message = Some(format!("http client init failed: {message}"));
            admin_probe.message = Some(format!("http client init failed: {message}"));
            None
        }
    };

    if let Some(client) = http_client.as_ref() {
        match fetch_health_with_retry(client, status_url.as_str()) {
            Ok(_) => {
                http_probe.ok = true;
            }
            Err(error) => {
                http_probe.message = Some(sanitize_diagnostic_error(error.to_string().as_str()));
            }
        }
    }

    if grpc_url.starts_with("unresolved:") {
        grpc_probe.message =
            Some(sanitize_diagnostic_error(grpc_url.trim_start_matches("unresolved:")));
    } else {
        match build_runtime() {
            Ok(runtime) => match runtime.block_on(fetch_grpc_health_with_retry(grpc_url.clone())) {
                Ok(_) => {
                    grpc_probe.ok = true;
                }
                Err(error) => {
                    grpc_probe.message =
                        Some(sanitize_diagnostic_error(error.to_string().as_str()));
                }
            },
            Err(error) => {
                grpc_probe.message = Some(sanitize_diagnostic_error(error.to_string().as_str()));
            }
        }
    }

    let admin_token = env::var("PALYRA_ADMIN_TOKEN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    if let (Some(client), Some(token)) = (http_client.as_ref(), admin_token) {
        let principal = resolve_doctor_admin_principal();
        match fetch_admin_status_payload(
            client,
            daemon_url.as_str(),
            Some(token),
            principal,
            DEFAULT_DEVICE_ID.to_owned(),
            None,
            None,
        ) {
            Ok(mut payload) => {
                redact_json_value_tree(&mut payload, None);
                admin_probe.ok = true;
                admin_payload = Some(payload);
            }
            Err(error) => {
                let message = sanitize_diagnostic_error(error.to_string().as_str());
                admin_probe.message = Some(message.clone());
                admin_error = Some(message);
            }
        }
    } else {
        admin_probe.message = Some("skipped (PALYRA_ADMIN_TOKEN is not set)".to_owned());
    }

    (
        DoctorConnectivitySnapshot {
            daemon_url,
            grpc_url: if grpc_url.starts_with("unresolved:") {
                "unavailable".to_owned()
            } else {
                grpc_url
            },
            http: http_probe,
            grpc: grpc_probe,
            admin: admin_probe,
        },
        admin_payload,
        admin_error,
    )
}

fn collect_doctor_provider_auth_snapshot(
    admin_payload: Option<&Value>,
    admin_error: Option<&str>,
) -> DoctorProviderAuthSnapshot {
    let Some(payload) = admin_payload else {
        return DoctorProviderAuthSnapshot {
            fetched: false,
            model_provider: None,
            auth_summary: None,
            error: admin_error.map(ToOwned::to_owned),
        };
    };

    let mut model_provider = payload.get("model_provider").cloned();
    if let Some(model_provider_value) = model_provider.as_mut() {
        redact_json_value_tree(model_provider_value, None);
    }
    let mut auth_summary = payload.pointer("/auth/summary").cloned();
    if let Some(summary) = auth_summary.as_mut() {
        redact_json_value_tree(summary, None);
    }
    DoctorProviderAuthSnapshot {
        fetched: true,
        model_provider,
        auth_summary,
        error: admin_error.map(ToOwned::to_owned),
    }
}

fn collect_doctor_browser_snapshot(
    admin_payload: Option<&Value>,
    admin_error: Option<&str>,
) -> DoctorBrowserSnapshot {
    let parsed = read_doctor_root_file_config().ok().flatten();
    let browser_service = parsed
        .as_ref()
        .and_then(|config| config.tool_call.as_ref())
        .and_then(|tool_call| tool_call.browser_service.as_ref());

    let mut configured_enabled = browser_service.and_then(|config| config.enabled).unwrap_or(false);
    if let Ok(raw) = env::var("PALYRA_BROWSER_SERVICE_ENABLED") {
        if let Ok(parsed_bool) = raw.trim().parse::<bool>() {
            configured_enabled = parsed_bool;
        }
    }

    let mut endpoint = browser_service
        .and_then(|config| config.endpoint.as_ref())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_BROWSER_SERVICE_ENDPOINT.to_owned());
    if let Ok(raw) = env::var("PALYRA_BROWSER_SERVICE_ENDPOINT") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            endpoint = trimmed.to_owned();
        }
    }

    let mut auth_token_configured = browser_service
        .and_then(|config| config.auth_token.as_ref())
        .map(|value| value.trim())
        .is_some_and(|value| !value.is_empty());
    if let Ok(raw) = env::var("PALYRA_BROWSER_SERVICE_AUTH_TOKEN") {
        auth_token_configured = !raw.trim().is_empty();
    }

    let mut connect_timeout_ms = browser_service.and_then(|config| config.connect_timeout_ms);
    if let Ok(raw) = env::var("PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS") {
        if let Ok(parsed_timeout) = raw.trim().parse::<u64>() {
            connect_timeout_ms = Some(parsed_timeout);
        }
    }

    let mut request_timeout_ms = browser_service.and_then(|config| config.request_timeout_ms);
    if let Ok(raw) = env::var("PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS") {
        if let Ok(parsed_timeout) = raw.trim().parse::<u64>() {
            request_timeout_ms = Some(parsed_timeout);
        }
    }

    let mut max_screenshot_bytes = browser_service.and_then(|config| config.max_screenshot_bytes);
    if let Ok(raw) = env::var("PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES") {
        if let Ok(parsed_limit) = raw.trim().parse::<u64>() {
            max_screenshot_bytes = Some(parsed_limit);
        }
    }

    let mut max_title_bytes = browser_service.and_then(|config| config.max_title_bytes);
    if let Ok(raw) = env::var("PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES") {
        if let Ok(parsed_limit) = raw.trim().parse::<u64>() {
            max_title_bytes = Some(parsed_limit);
        }
    }

    let state_dir_configured = browser_service
        .and_then(|config| config.state_dir.as_ref())
        .map(|value| value.trim())
        .is_some_and(|value| !value.is_empty())
        || env::var("PALYRA_BROWSERD_STATE_DIR")
            .ok()
            .map(|value| value.trim().to_owned())
            .is_some_and(|value| !value.is_empty());
    let state_key_vault_ref_configured = browser_service
        .and_then(|config| config.state_key_vault_ref.as_ref())
        .map(|value| value.trim())
        .is_some_and(|value| !value.is_empty());

    let browser_payload = admin_payload.and_then(|payload| payload.get("browserd"));
    let diagnostics_fetched = browser_payload.is_some();
    let health_status = browser_payload
        .and_then(|payload| payload.pointer("/health/status"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let active_sessions = browser_payload
        .and_then(|payload| payload.pointer("/sessions/active"))
        .and_then(Value::as_u64)
        .or_else(|| {
            browser_payload
                .and_then(|payload| payload.pointer("/health/active_sessions"))
                .and_then(Value::as_u64)
        });
    let recent_relay_action_failures = browser_payload
        .and_then(|payload| payload.pointer("/failures/recent_relay_action_failures"))
        .and_then(Value::as_u64);
    let recent_health_failures = browser_payload
        .and_then(|payload| payload.pointer("/failures/recent_health_failures"))
        .and_then(Value::as_u64);
    let error = if configured_enabled && !diagnostics_fetched {
        admin_error.map(ToOwned::to_owned).or(Some("browser diagnostics unavailable".to_owned()))
    } else {
        None
    };

    DoctorBrowserSnapshot {
        configured_enabled,
        auth_token_configured,
        endpoint,
        connect_timeout_ms,
        request_timeout_ms,
        max_screenshot_bytes,
        max_title_bytes,
        state_dir_configured,
        state_key_vault_ref_configured,
        diagnostics_fetched,
        health_status,
        active_sessions,
        recent_relay_action_failures,
        recent_health_failures,
        error,
    }
}

fn collect_doctor_access_snapshot() -> DoctorAccessSnapshot {
    let state_root = match app::resolve_cli_state_root(None) {
        Ok(path) => path,
        Err(error) => {
            return DoctorAccessSnapshot {
                registry_path: None,
                registry_exists: false,
                parsed: false,
                compat_api_enabled: false,
                api_tokens_enabled: false,
                team_mode_enabled: false,
                rbac_enabled: false,
                staged_rollout_enabled: false,
                backfill_required: false,
                blocking_issues: 0,
                warning_issues: 0,
                external_api_safe_mode: true,
                team_mode_safe_mode: true,
                error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
            };
        }
    };
    let registry_path = state_root.join("access_registry.json");
    if !registry_path.exists() {
        return DoctorAccessSnapshot {
            registry_path: Some(registry_path.display().to_string()),
            registry_exists: false,
            parsed: false,
            compat_api_enabled: false,
            api_tokens_enabled: false,
            team_mode_enabled: false,
            rbac_enabled: false,
            staged_rollout_enabled: false,
            backfill_required: false,
            blocking_issues: 0,
            warning_issues: 0,
            external_api_safe_mode: true,
            team_mode_safe_mode: true,
            error: None,
        };
    }

    let raw = match fs::read_to_string(&registry_path) {
        Ok(value) => value,
        Err(error) => {
            return DoctorAccessSnapshot {
                registry_path: Some(registry_path.display().to_string()),
                registry_exists: true,
                parsed: false,
                compat_api_enabled: false,
                api_tokens_enabled: false,
                team_mode_enabled: false,
                rbac_enabled: false,
                staged_rollout_enabled: false,
                backfill_required: false,
                blocking_issues: 0,
                warning_issues: 0,
                external_api_safe_mode: true,
                team_mode_safe_mode: true,
                error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
            };
        }
    };
    let value = match serde_json::from_str::<Value>(raw.as_str()) {
        Ok(value) => value,
        Err(error) => {
            return DoctorAccessSnapshot {
                registry_path: Some(registry_path.display().to_string()),
                registry_exists: true,
                parsed: false,
                compat_api_enabled: false,
                api_tokens_enabled: false,
                team_mode_enabled: false,
                rbac_enabled: false,
                staged_rollout_enabled: false,
                backfill_required: true,
                blocking_issues: 1,
                warning_issues: 0,
                external_api_safe_mode: true,
                team_mode_safe_mode: true,
                error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
            };
        }
    };

    let feature_flags =
        value.get("feature_flags").and_then(Value::as_array).cloned().unwrap_or_default();
    let compat_api_enabled = doctor_access_flag_enabled(feature_flags.as_slice(), "compat_api");
    let api_tokens_enabled = doctor_access_flag_enabled(feature_flags.as_slice(), "api_tokens");
    let team_mode_enabled = doctor_access_flag_enabled(feature_flags.as_slice(), "team_mode");
    let rbac_enabled = doctor_access_flag_enabled(feature_flags.as_slice(), "rbac");
    let staged_rollout_enabled =
        doctor_access_flag_enabled(feature_flags.as_slice(), "staged_rollout");

    let missing_flags = ["compat_api", "api_tokens", "team_mode", "rbac", "staged_rollout"]
        .into_iter()
        .filter(|key| {
            !feature_flags.iter().any(|entry| {
                entry.get("key").and_then(Value::as_str).is_some_and(|value| value == *key)
            })
        })
        .count();
    let workspaces_missing_runtime = value
        .get("workspaces")
        .and_then(Value::as_array)
        .map(|workspaces| {
            workspaces
                .iter()
                .filter(|workspace| {
                    workspace
                        .get("runtime_principal")
                        .and_then(Value::as_str)
                        .is_none_or(|value| value.trim().is_empty())
                        || workspace
                            .get("runtime_device_id")
                            .and_then(Value::as_str)
                            .is_none_or(|value| value.trim().is_empty())
                })
                .count()
        })
        .unwrap_or(0);
    let api_tokens_missing_scopes = value
        .get("api_tokens")
        .and_then(Value::as_array)
        .map(|tokens| {
            tokens
                .iter()
                .filter(|token| {
                    token
                        .get("scopes")
                        .and_then(Value::as_array)
                        .is_none_or(|value| value.is_empty())
                })
                .count()
        })
        .unwrap_or(0);
    let workspace_ids = value
        .get("workspaces")
        .and_then(Value::as_array)
        .map(|workspaces| {
            workspaces
                .iter()
                .filter_map(|workspace| {
                    workspace.get("workspace_id").and_then(Value::as_str).map(ToOwned::to_owned)
                })
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();
    let orphaned_memberships = value
        .get("memberships")
        .and_then(Value::as_array)
        .map(|memberships| {
            memberships
                .iter()
                .filter(|membership| {
                    membership
                        .get("workspace_id")
                        .and_then(Value::as_str)
                        .is_some_and(|workspace_id| !workspace_ids.contains(workspace_id))
                })
                .count()
        })
        .unwrap_or(0);
    let orphaned_invitations = value
        .get("invitations")
        .and_then(Value::as_array)
        .map(|invitations| {
            invitations
                .iter()
                .filter(|invitation| {
                    invitation
                        .get("workspace_id")
                        .and_then(Value::as_str)
                        .is_some_and(|workspace_id| !workspace_ids.contains(workspace_id))
                })
                .count()
        })
        .unwrap_or(0);
    let warning_issues = missing_flags + workspaces_missing_runtime + api_tokens_missing_scopes;
    let blocking_issues = orphaned_memberships + orphaned_invitations;

    DoctorAccessSnapshot {
        registry_path: Some(registry_path.display().to_string()),
        registry_exists: true,
        parsed: true,
        compat_api_enabled,
        api_tokens_enabled,
        team_mode_enabled,
        rbac_enabled,
        staged_rollout_enabled,
        backfill_required: warning_issues > 0 || blocking_issues > 0,
        blocking_issues,
        warning_issues,
        external_api_safe_mode: !compat_api_enabled || !api_tokens_enabled,
        team_mode_safe_mode: !team_mode_enabled || !rbac_enabled,
        error: None,
    }
}

fn doctor_access_flag_enabled(flags: &[Value], key: &str) -> bool {
    flags.iter().any(|entry| {
        entry.get("key").and_then(Value::as_str).is_some_and(|value| value == key)
            && entry.get("enabled").and_then(Value::as_bool).unwrap_or(false)
    })
}

fn collect_doctor_deployment_snapshot() -> DoctorDeploymentSnapshot {
    let parsed = read_doctor_root_file_config().ok().flatten();

    let mut mode = parsed
        .as_ref()
        .and_then(|config| config.deployment.as_ref())
        .and_then(|deployment| deployment.mode.as_ref())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_DEPLOYMENT_MODE.to_owned());
    if let Ok(raw) = env::var("PALYRA_DEPLOYMENT_MODE") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            mode = trimmed.to_ascii_lowercase();
        }
    }

    let mut bind_profile = parsed
        .as_ref()
        .and_then(|config| config.gateway.as_ref())
        .and_then(|gateway| gateway.bind_profile.as_ref())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_GATEWAY_BIND_PROFILE.to_owned());
    if let Ok(raw) = env::var("PALYRA_GATEWAY_BIND_PROFILE") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            bind_profile = trimmed.to_ascii_lowercase();
        }
    }

    let mut admin_bind_addr = parsed
        .as_ref()
        .and_then(|config| config.daemon.as_ref())
        .and_then(|daemon| daemon.bind_addr.as_ref())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_DAEMON_BIND_ADDR.to_owned());
    if let Ok(raw) = env::var("PALYRA_DAEMON_BIND_ADDR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            admin_bind_addr = trimmed.to_owned();
        }
    }
    let mut admin_port = parsed
        .as_ref()
        .and_then(|config| config.daemon.as_ref())
        .and_then(|daemon| daemon.port)
        .unwrap_or(DEFAULT_DAEMON_PORT);
    if let Ok(raw) = env::var("PALYRA_DAEMON_PORT") {
        if let Ok(parsed_port) = raw.trim().parse::<u16>() {
            admin_port = parsed_port;
        }
    }

    let mut grpc_bind_addr = parsed
        .as_ref()
        .and_then(|config| config.gateway.as_ref())
        .and_then(|gateway| gateway.grpc_bind_addr.as_ref())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_GATEWAY_GRPC_BIND_ADDR.to_owned());
    if let Ok(raw) = env::var("PALYRA_GATEWAY_GRPC_BIND_ADDR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            grpc_bind_addr = trimmed.to_owned();
        }
    }
    let mut grpc_port = parsed
        .as_ref()
        .and_then(|config| config.gateway.as_ref())
        .and_then(|gateway| gateway.grpc_port)
        .unwrap_or(DEFAULT_GATEWAY_GRPC_PORT);
    if let Ok(raw) = env::var("PALYRA_GATEWAY_GRPC_PORT") {
        if let Ok(parsed_port) = raw.trim().parse::<u16>() {
            grpc_port = parsed_port;
        }
    }

    let mut quic_bind_addr = parsed
        .as_ref()
        .and_then(|config| config.gateway.as_ref())
        .and_then(|gateway| gateway.quic_bind_addr.as_ref())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_GATEWAY_QUIC_BIND_ADDR.to_owned());
    if let Ok(raw) = env::var("PALYRA_GATEWAY_QUIC_BIND_ADDR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            quic_bind_addr = trimmed.to_owned();
        }
    }
    let mut quic_port = parsed
        .as_ref()
        .and_then(|config| config.gateway.as_ref())
        .and_then(|gateway| gateway.quic_port)
        .unwrap_or(DEFAULT_GATEWAY_QUIC_PORT);
    if let Ok(raw) = env::var("PALYRA_GATEWAY_QUIC_PORT") {
        if let Ok(parsed_port) = raw.trim().parse::<u16>() {
            quic_port = parsed_port;
        }
    }
    let mut quic_enabled = parsed
        .as_ref()
        .and_then(|config| config.gateway.as_ref())
        .and_then(|gateway| gateway.quic_enabled)
        .unwrap_or(DEFAULT_GATEWAY_QUIC_ENABLED);
    if let Ok(raw) = env::var("PALYRA_GATEWAY_QUIC_ENABLED") {
        if let Ok(parsed_value) = raw.trim().parse::<bool>() {
            quic_enabled = parsed_value;
        }
    }

    let mut gateway_tls_enabled = parsed
        .as_ref()
        .and_then(|config| config.gateway.as_ref())
        .and_then(|gateway| gateway.tls.as_ref())
        .and_then(|tls| tls.enabled)
        .unwrap_or(false);
    if let Ok(raw) = env::var("PALYRA_GATEWAY_TLS_ENABLED") {
        if let Ok(parsed_value) = raw.trim().parse::<bool>() {
            gateway_tls_enabled = parsed_value;
        }
    }

    let mut admin_auth_required = parsed
        .as_ref()
        .and_then(|config| config.admin.as_ref())
        .and_then(|admin| admin.require_auth)
        .unwrap_or(true);
    if let Ok(raw) = env::var("PALYRA_ADMIN_REQUIRE_AUTH") {
        if let Ok(parsed_value) = raw.trim().parse::<bool>() {
            admin_auth_required = parsed_value;
        }
    }

    let file_admin_token_configured = parsed
        .as_ref()
        .and_then(|config| config.admin.as_ref())
        .and_then(|admin| admin.auth_token.as_ref())
        .map(|token| !token.trim().is_empty())
        .unwrap_or(false);
    let env_admin_token_configured =
        env::var("PALYRA_ADMIN_TOKEN").ok().map(|token| !token.trim().is_empty()).unwrap_or(false);
    let admin_token_configured = file_admin_token_configured || env_admin_token_configured;

    let mut dangerous_remote_bind_ack_config = parsed
        .as_ref()
        .and_then(|config| config.deployment.as_ref())
        .and_then(|deployment| deployment.dangerous_remote_bind_ack)
        .unwrap_or(false);
    if let Ok(raw) = env::var("PALYRA_DEPLOYMENT_DANGEROUS_REMOTE_BIND_ACK") {
        if let Ok(parsed_value) = raw.trim().parse::<bool>() {
            dangerous_remote_bind_ack_config = parsed_value;
        }
    }
    let dangerous_remote_bind_ack_env = env::var(DANGEROUS_REMOTE_BIND_ACK_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<bool>().ok())
        .unwrap_or(false);

    let admin_remote = bind_is_non_loopback(admin_bind_addr.as_str(), admin_port);
    let grpc_remote = bind_is_non_loopback(grpc_bind_addr.as_str(), grpc_port);
    let quic_remote = quic_enabled && bind_is_non_loopback(quic_bind_addr.as_str(), quic_port);
    let remote_bind_detected = admin_remote || grpc_remote || quic_remote;

    let mut warnings = Vec::new();
    if remote_bind_detected && !gateway_tls_enabled {
        warnings.push("Remote bind without TLS blocked".to_owned());
    }
    if remote_bind_detected {
        warnings.push("Dashboard exposed publicly; ensure WAF/reverse proxy".to_owned());
    }
    if remote_bind_detected && (!admin_auth_required || !admin_token_configured) {
        warnings.push("Remote bind requires admin authentication with configured token".to_owned());
    }

    DoctorDeploymentSnapshot {
        mode,
        bind_profile,
        binds: DoctorDeploymentBindSnapshot {
            admin: format!("{admin_bind_addr}:{admin_port}"),
            grpc: format!("{grpc_bind_addr}:{grpc_port}"),
            quic: if quic_enabled {
                format!("{quic_bind_addr}:{quic_port}")
            } else {
                "disabled".to_owned()
            },
        },
        gateway_tls_enabled,
        admin_auth_required,
        admin_token_configured,
        dangerous_remote_bind_ack_config,
        dangerous_remote_bind_ack_env,
        remote_bind_detected,
        warnings,
    }
}

fn bind_is_non_loopback(bind_addr: &str, port: u16) -> bool {
    parse_daemon_bind_socket(bind_addr, port)
        .map(|socket| !socket.ip().is_loopback())
        .unwrap_or(true)
}

fn is_directory_writable(path: &Path) -> Result<bool> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create directory {}", path.display()))?;
    let probe = path.join(".palyra-write-check.tmp");
    fs::write(probe.as_path(), "probe")
        .with_context(|| format!("failed to write probe file {}", probe.display()))?;
    fs::remove_file(probe.as_path())
        .with_context(|| format!("failed to remove probe file {}", probe.display()))?;
    Ok(true)
}

fn resolve_doctor_admin_principal() -> String {
    env::var("PALYRA_ADMIN_BOUND_PRINCIPAL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "admin:doctor".to_owned())
}

fn sanitize_diagnostic_error(raw: &str) -> String {
    let mut sanitized = redact_auth_error(raw);
    sanitized = redact_url_segments_in_text(sanitized.as_str());
    truncate_utf8_chars(sanitized.as_str(), 1_024)
}

fn redact_json_value_tree(value: &mut Value, key_context: Option<&str>) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map {
                if is_sensitive_key(key.as_str()) {
                    *entry = Value::String(REDACTED.to_owned());
                    continue;
                }
                redact_json_value_tree(entry, Some(key.as_str()));
            }
        }
        Value::Array(items) => {
            for entry in items {
                redact_json_value_tree(entry, key_context);
            }
        }
        Value::String(raw) => {
            if key_context.is_some_and(is_sensitive_key) {
                *raw = REDACTED.to_owned();
                return;
            }
            if key_context
                .map(|key| key_contains_any(key, &["url", "uri", "endpoint", "location"]))
                .unwrap_or(false)
            {
                *raw = redact_url(raw.as_str());
                return;
            }
            if key_context
                .map(|key| key_contains_any(key, &["error", "reason", "message", "detail"]))
                .unwrap_or(false)
            {
                *raw = sanitize_diagnostic_error(raw.as_str());
            }
        }
        _ => {}
    }
}

fn key_contains_any(key: &str, needles: &[&str]) -> bool {
    let lowered = key.to_ascii_lowercase();
    needles.iter().any(|needle| lowered.contains(needle))
}

fn resolve_support_bundle_output_path(
    output: Option<String>,
    generated_at_unix_ms: i64,
) -> PathBuf {
    if let Some(output) = output {
        let trimmed = output.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    PathBuf::from(format!("support-bundle-{generated_at_unix_ms}.json"))
}

fn build_support_bundle_config_snapshot() -> SupportBundleConfigSnapshot {
    let path = doctor_config_path().map(|value| value.to_string_lossy().into_owned());
    let Some(path_value) = path.clone() else {
        return SupportBundleConfigSnapshot {
            path,
            redacted_document: None,
            fingerprint_sha256: None,
            error: None,
        };
    };

    let path_ref = PathBuf::from(path_value.as_str());
    if !path_ref.exists() {
        return SupportBundleConfigSnapshot {
            path,
            redacted_document: None,
            fingerprint_sha256: None,
            error: Some("config path does not exist".to_owned()),
        };
    }

    match load_document_from_existing_path(path_ref.as_path()) {
        Ok((mut document, _)) => {
            redact_secret_config_values(&mut document);
            match serde_json::to_value(document) {
                Ok(mut payload) => {
                    redact_json_value_tree(&mut payload, None);
                    let fingerprint_sha256 =
                        serde_json::to_vec(&payload).ok().map(|bytes| sha256_hex(bytes.as_slice()));
                    SupportBundleConfigSnapshot {
                        path,
                        redacted_document: Some(payload),
                        fingerprint_sha256,
                        error: None,
                    }
                }
                Err(error) => SupportBundleConfigSnapshot {
                    path,
                    redacted_document: None,
                    fingerprint_sha256: None,
                    error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
                },
            }
        }
        Err(error) => SupportBundleConfigSnapshot {
            path,
            redacted_document: None,
            fingerprint_sha256: None,
            error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
        },
    }
}

fn build_support_bundle_diagnostics_snapshot() -> SupportBundleDiagnosticsSnapshot {
    let service_status = app::current_root_context()
        .map(|context| support::service::query_gateway_service_status(context.state_root()))
        .transpose()
        .ok()
        .flatten();
    let token = env::var("PALYRA_ADMIN_TOKEN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let Some(token) = token else {
        return SupportBundleDiagnosticsSnapshot {
            gateway_health: build_support_bundle_gateway_health(None),
            service_status,
            browser_status: None,
            node_status: None,
            admin_status: None,
            admin_status_error: Some("skipped (PALYRA_ADMIN_TOKEN is not set)".to_owned()),
            skills: build_default_skills_inventory_snapshot(),
        };
    };

    let client = match Client::builder().timeout(Duration::from_secs(2)).build() {
        Ok(client) => client,
        Err(error) => {
            return SupportBundleDiagnosticsSnapshot {
                gateway_health: None,
                service_status,
                browser_status: None,
                node_status: None,
                admin_status: None,
                admin_status_error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
                skills: build_default_skills_inventory_snapshot(),
            };
        }
    };
    let daemon_url = match resolve_support_bundle_daemon_url() {
        Ok(url) => url,
        Err(error) => {
            return SupportBundleDiagnosticsSnapshot {
                gateway_health: None,
                service_status,
                browser_status: None,
                node_status: None,
                admin_status: None,
                admin_status_error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
                skills: build_default_skills_inventory_snapshot(),
            };
        }
    };
    let gateway_health = build_support_bundle_gateway_health(Some((&client, daemon_url.as_str())));
    let principal = resolve_doctor_admin_principal();
    match fetch_admin_status_payload(
        &client,
        daemon_url.as_str(),
        Some(token),
        principal,
        DEFAULT_DEVICE_ID.to_owned(),
        None,
        None,
    ) {
        Ok(mut payload) => {
            let browser_status = payload.get("browserd").cloned();
            let node_status = payload.get("node").cloned();
            redact_json_value_tree(&mut payload, None);
            SupportBundleDiagnosticsSnapshot {
                gateway_health,
                service_status,
                browser_status,
                node_status,
                admin_status: Some(payload),
                admin_status_error: None,
                skills: build_default_skills_inventory_snapshot(),
            }
        }
        Err(error) => SupportBundleDiagnosticsSnapshot {
            gateway_health,
            service_status,
            browser_status: None,
            node_status: None,
            admin_status: None,
            admin_status_error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
            skills: build_default_skills_inventory_snapshot(),
        },
    }
}

fn build_support_bundle_gateway_health(client_and_url: Option<(&Client, &str)>) -> Option<Value> {
    let (client, daemon_url) = client_and_url?;
    let health_url = format!("{}/healthz", daemon_url.trim_end_matches('/'));
    let response = fetch_health_with_retry(client, health_url.as_str()).ok()?;
    Some(json!({
        "status": response.status,
        "service": response.service,
        "version": response.version,
        "git_hash": response.git_hash,
        "uptime_seconds": response.uptime_seconds,
    }))
}

fn build_support_bundle_observability_snapshot(
    diagnostics: &SupportBundleDiagnosticsSnapshot,
) -> SupportBundleObservabilitySnapshot {
    let summary =
        diagnostics.admin_status.as_ref().and_then(|payload| payload.get("observability")).cloned();
    let recent_failures =
        summary.as_ref().and_then(|payload| payload.get("recent_failures")).cloned();
    SupportBundleObservabilitySnapshot { summary, recent_failures }
}

fn build_support_bundle_triage_snapshot() -> SupportBundleTriageSnapshot {
    SupportBundleTriageSnapshot {
        playbook:
            "docs-codebase/docs-tree/web_console_operator_dashboard/console_sections_and_navigation/support_recovery.md"
                .to_owned(),
        failure_classes: vec![
            "config_failure".to_owned(),
            "upstream_provider_failure".to_owned(),
            "product_failure".to_owned(),
        ],
        common_order: vec![
            "Check deployment posture and operator auth first.".to_owned(),
            "Check OpenAI profile health and refresh metrics next.".to_owned(),
            "Check Discord queue depth, dead letters, and upload failures next.".to_owned(),
            "Check browser relay failures and service health next.".to_owned(),
            "If still unresolved, inspect observability.recent_failures and diagnostics.admin_status."
                .to_owned(),
        ],
    }
}

fn resolve_support_bundle_daemon_url() -> Result<String> {
    let raw = env::var("PALYRA_DAEMON_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
    parse_support_bundle_daemon_url(raw.as_str(), "PALYRA_DAEMON_URL")
}

fn parse_support_bundle_daemon_url(raw: &str, source_name: &str) -> Result<String> {
    let parsed = Url::parse(raw.trim())
        .with_context(|| format!("{source_name} must be a valid absolute URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        anyhow::bail!("{source_name} must use http:// or https://");
    }
    let host = parsed.host_str().ok_or_else(|| anyhow!("{source_name} must include a host"))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("{source_name} must not include embedded credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("{source_name} must not include query or fragment");
    }
    let normalized_host = host.trim_start_matches('[').trim_end_matches(']');
    let is_loopback = normalized_host.eq_ignore_ascii_case("localhost")
        || normalized_host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback());
    if !is_loopback {
        anyhow::bail!("{source_name} must target a loopback host for support-bundle diagnostics");
    }
    Ok(parsed.to_string())
}

fn build_support_bundle_journal_snapshot(
    journal_hash_limit: usize,
    error_limit: usize,
) -> SupportBundleJournalSnapshot {
    let path = match resolve_daemon_journal_db_path(None) {
        Ok(path) => path,
        Err(error) => {
            return SupportBundleJournalSnapshot {
                db_path: DEFAULT_JOURNAL_DB_PATH.to_owned(),
                available: false,
                hash_chain_enabled: false,
                latest_hash: None,
                recent_hashes: Vec::new(),
                last_errors: Vec::new(),
                error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
            };
        }
    };
    let db_path = path.to_string_lossy().into_owned();
    if !path.exists() || !path.is_file() {
        return SupportBundleJournalSnapshot {
            db_path,
            available: false,
            hash_chain_enabled: false,
            latest_hash: None,
            recent_hashes: Vec::new(),
            last_errors: Vec::new(),
            error: Some("journal database is unavailable".to_owned()),
        };
    }

    let connection = match Connection::open(path.as_path()) {
        Ok(connection) => connection,
        Err(error) => {
            return SupportBundleJournalSnapshot {
                db_path,
                available: false,
                hash_chain_enabled: false,
                latest_hash: None,
                recent_hashes: Vec::new(),
                last_errors: Vec::new(),
                error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
            };
        }
    };

    let latest_hash = read_latest_journal_hash(&connection).ok().flatten();
    let recent_hashes =
        read_recent_journal_hashes(&connection, journal_hash_limit).unwrap_or_default();
    let last_errors =
        read_recent_journal_errors(&connection, error_limit.clamp(1, 256)).unwrap_or_default();
    SupportBundleJournalSnapshot {
        db_path,
        available: true,
        hash_chain_enabled: latest_hash.is_some(),
        latest_hash,
        recent_hashes,
        last_errors,
        error: None,
    }
}

fn read_latest_journal_hash(connection: &Connection) -> Result<Option<String>> {
    let latest = connection
        .query_row(
            "SELECT hash FROM journal_events WHERE hash IS NOT NULL ORDER BY seq DESC LIMIT 1",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .or_else(|error| {
            if matches!(error, rusqlite::Error::QueryReturnedNoRows) {
                Ok(None)
            } else {
                Err(error)
            }
        })?;
    Ok(latest)
}

fn read_recent_journal_hashes(connection: &Connection, limit: usize) -> Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT hash FROM journal_events WHERE hash IS NOT NULL ORDER BY seq DESC LIMIT ?1",
    )?;
    let rows = statement.query_map([limit.clamp(1, 512) as i64], |row| row.get::<_, String>(0))?;
    let mut hashes = Vec::new();
    for row in rows {
        hashes.push(row?);
    }
    Ok(hashes)
}

fn read_recent_journal_errors(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<SupportBundleJournalErrorRecord>> {
    let scan_limit = (limit.saturating_mul(24)).clamp(limit, 4096);
    let mut statement = connection.prepare(
        "SELECT event_ulid, kind, timestamp_unix_ms, payload_json
         FROM journal_events
         ORDER BY seq DESC
         LIMIT ?1",
    )?;
    let mut rows = statement.query([scan_limit as i64])?;
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        let payload_json: String = row.get(3)?;
        let Some(message) = extract_support_bundle_error_message(payload_json.as_str()) else {
            continue;
        };
        records.push(SupportBundleJournalErrorRecord {
            event_id: row.get(0)?,
            kind: row.get(1)?,
            timestamp_unix_ms: row.get(2)?,
            message,
        });
        if records.len() >= limit {
            break;
        }
    }
    Ok(records)
}

fn extract_support_bundle_error_message(payload_json: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(payload_json).ok()?;
    let mut candidates = Vec::<String>::new();
    collect_error_strings(&value, None, &mut candidates);
    let first = candidates.into_iter().find(|candidate| !candidate.trim().is_empty())?;
    let sanitized = sanitize_diagnostic_error(first.as_str());
    Some(truncate_utf8_chars(sanitized.as_str(), 512))
}

fn collect_error_strings(value: &Value, key_context: Option<&str>, output: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map {
                if entry.is_string()
                    && key_contains_any(key.as_str(), &["error", "reason", "message", "failure"])
                {
                    if let Some(raw) = entry.as_str() {
                        output.push(raw.to_owned());
                    }
                }
                collect_error_strings(entry, Some(key.as_str()), output);
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                collect_error_strings(entry, key_context, output);
            }
        }
        Value::String(raw) => {
            if key_context
                .map(|key| key_contains_any(key, &["error", "reason", "message", "failure"]))
                .unwrap_or(false)
            {
                output.push(raw.clone());
            }
        }
        _ => {}
    }
}

fn encode_support_bundle_with_cap(bundle: &mut SupportBundle, max_bytes: usize) -> Result<Vec<u8>> {
    let mut value = serde_json::to_value(&*bundle).context("failed to serialize support bundle")?;
    let mut encoded =
        serde_json::to_vec_pretty(&value).context("failed to encode support bundle")?;
    if encoded.len() <= max_bytes {
        return Ok(encoded);
    }

    bundle.truncated = true;
    bundle
        .warnings
        .push(format!("support bundle exceeded {} bytes; trimming verbose sections", max_bytes));
    if let Some(object) = value.as_object_mut() {
        object.insert("truncated".to_owned(), Value::Bool(true));
        object.insert(
            "warnings".to_owned(),
            serde_json::to_value(bundle.warnings.clone())
                .context("failed to serialize support bundle warnings")?,
        );
        if let Some(config) = object.get_mut("config").and_then(Value::as_object_mut) {
            config.remove("redacted_document");
        }
        if let Some(diagnostics) = object.get_mut("diagnostics").and_then(Value::as_object_mut) {
            diagnostics.remove("admin_status");
        }
    }
    encoded = serde_json::to_vec_pretty(&value).context("failed to re-encode support bundle")?;
    if encoded.len() <= max_bytes {
        return Ok(encoded);
    }

    trim_support_bundle_journal_for_cap(&mut value, max_bytes)?;
    encoded =
        serde_json::to_vec_pretty(&value).context("failed to encode trimmed support bundle")?;
    if encoded.len() <= max_bytes {
        return Ok(encoded);
    }

    if let Some(object) = value.as_object_mut() {
        if let Some(doctor) = object.get_mut("doctor").and_then(Value::as_object_mut) {
            doctor.remove("checks");
        }
    }
    encoded =
        serde_json::to_vec_pretty(&value).context("failed to encode minimally trimmed bundle")?;
    if encoded.len() <= max_bytes {
        return Ok(encoded);
    }

    let minimal = json!({
        "schema_version": 1,
        "generated_at_unix_ms": bundle.generated_at_unix_ms,
        "build": bundle.build,
        "platform": bundle.platform,
        "truncated": true,
        "warnings": bundle.warnings,
        "error": "bundle exceeded size cap; emitted minimal summary",
    });
    let minimal_encoded =
        serde_json::to_vec_pretty(&minimal).context("failed to encode minimal support bundle")?;
    if minimal_encoded.len() > max_bytes {
        anyhow::bail!(
            "support bundle cap {} bytes is too small for minimal payload ({} bytes)",
            max_bytes,
            minimal_encoded.len()
        );
    }
    Ok(minimal_encoded)
}

fn trim_support_bundle_journal_for_cap(bundle: &mut Value, max_bytes: usize) -> Result<()> {
    loop {
        let encoded =
            serde_json::to_vec_pretty(bundle).context("failed to encode support bundle")?;
        if encoded.len() <= max_bytes {
            return Ok(());
        }
        let Some(journal) = bundle.get_mut("journal").and_then(Value::as_object_mut) else {
            return Ok(());
        };
        let mut removed = false;
        if let Some(errors) = journal.get_mut("last_errors").and_then(Value::as_array_mut) {
            if !errors.is_empty() {
                errors.pop();
                removed = true;
            }
        }
        if !removed {
            if let Some(hashes) = journal.get_mut("recent_hashes").and_then(Value::as_array_mut) {
                if !hashes.is_empty() {
                    hashes.pop();
                    removed = true;
                }
            }
        }
        if !removed {
            return Ok(());
        }
    }
}

fn truncate_utf8_chars(raw: &str, max_chars: usize) -> String {
    if raw.chars().count() <= max_chars {
        return raw.to_owned();
    }
    let mut output = String::new();
    for ch in raw.chars().take(max_chars) {
        output.push(ch);
    }
    output.push_str("...");
    output
}

fn auth_provider_arg_to_proto(value: AuthProviderArg) -> i32 {
    match value {
        AuthProviderArg::Openai => auth_v1::AuthProviderKind::Openai as i32,
        AuthProviderArg::Anthropic => auth_v1::AuthProviderKind::Anthropic as i32,
        AuthProviderArg::Telegram => auth_v1::AuthProviderKind::Telegram as i32,
        AuthProviderArg::Slack => auth_v1::AuthProviderKind::Slack as i32,
        AuthProviderArg::Discord => auth_v1::AuthProviderKind::Discord as i32,
        AuthProviderArg::Webhook => auth_v1::AuthProviderKind::Webhook as i32,
        AuthProviderArg::Custom => auth_v1::AuthProviderKind::Custom as i32,
    }
}

fn auth_scope_arg_to_proto(value: AuthScopeArg) -> i32 {
    match value {
        AuthScopeArg::Global => auth_v1::AuthScopeKind::Global as i32,
        AuthScopeArg::Agent => auth_v1::AuthScopeKind::Agent as i32,
    }
}

fn auth_provider_to_text(provider: Option<&auth_v1::AuthProvider>) -> String {
    let Some(provider) = provider else {
        return "unspecified".to_owned();
    };
    match auth_v1::AuthProviderKind::try_from(provider.kind)
        .unwrap_or(auth_v1::AuthProviderKind::Unspecified)
    {
        auth_v1::AuthProviderKind::Openai => "openai".to_owned(),
        auth_v1::AuthProviderKind::Anthropic => "anthropic".to_owned(),
        auth_v1::AuthProviderKind::Telegram => "telegram".to_owned(),
        auth_v1::AuthProviderKind::Slack => "slack".to_owned(),
        auth_v1::AuthProviderKind::Discord => "discord".to_owned(),
        auth_v1::AuthProviderKind::Webhook => "webhook".to_owned(),
        auth_v1::AuthProviderKind::Custom => {
            if provider.custom_name.trim().is_empty() {
                "custom".to_owned()
            } else {
                provider.custom_name.to_ascii_lowercase()
            }
        }
        auth_v1::AuthProviderKind::Unspecified => "unspecified".to_owned(),
    }
}

fn auth_scope_to_text(scope: Option<&auth_v1::AuthScope>) -> String {
    let Some(scope) = scope else {
        return "unspecified".to_owned();
    };
    match auth_v1::AuthScopeKind::try_from(scope.kind)
        .unwrap_or(auth_v1::AuthScopeKind::Unspecified)
    {
        auth_v1::AuthScopeKind::Global => "global".to_owned(),
        auth_v1::AuthScopeKind::Agent => {
            if scope.agent_id.trim().is_empty() {
                "agent:<missing>".to_owned()
            } else {
                format!("agent:{}", scope.agent_id)
            }
        }
        auth_v1::AuthScopeKind::Unspecified => "unspecified".to_owned(),
    }
}

fn auth_profile_credential_type(profile: &auth_v1::AuthProfile) -> &'static str {
    match profile.credential.as_ref().and_then(|credential| credential.kind.as_ref()) {
        Some(auth_v1::auth_credential::Kind::ApiKey(_)) => "api_key",
        Some(auth_v1::auth_credential::Kind::Oauth(_)) => "oauth",
        None => "unspecified",
    }
}

fn auth_profile_to_json(profile: &auth_v1::AuthProfile) -> serde_json::Value {
    let credential = match profile.credential.as_ref().and_then(|value| value.kind.as_ref()) {
        Some(auth_v1::auth_credential::Kind::ApiKey(api_key)) => json!({
            "type": "api_key",
            "api_key_vault_ref": api_key.api_key_vault_ref,
        }),
        Some(auth_v1::auth_credential::Kind::Oauth(oauth)) => json!({
            "type": "oauth",
            "access_token_vault_ref": oauth.access_token_vault_ref,
            "refresh_token_vault_ref": oauth.refresh_token_vault_ref,
            "token_endpoint": oauth.token_endpoint,
            "client_id": empty_to_none(oauth.client_id.clone()),
            "client_secret_vault_ref": empty_to_none(oauth.client_secret_vault_ref.clone()),
            "scopes": oauth.scopes,
            "expires_at_unix_ms": if oauth.expires_at_unix_ms > 0 {
                Some(oauth.expires_at_unix_ms)
            } else {
                None
            },
            "refresh_state": oauth.refresh_state.as_ref().map(|state| json!({
                "failure_count": state.failure_count,
                "last_error": empty_to_none(state.last_error.clone()),
                "last_attempt_unix_ms": if state.last_attempt_unix_ms > 0 {
                    Some(state.last_attempt_unix_ms)
                } else {
                    None
                },
                "last_success_unix_ms": if state.last_success_unix_ms > 0 {
                    Some(state.last_success_unix_ms)
                } else {
                    None
                },
                "next_allowed_refresh_unix_ms": if state.next_allowed_refresh_unix_ms > 0 {
                    Some(state.next_allowed_refresh_unix_ms)
                } else {
                    None
                },
            })),
        }),
        None => json!({"type": "unspecified"}),
    };
    json!({
        "profile_id": profile.profile_id,
        "provider": auth_provider_to_text(profile.provider.as_ref()),
        "profile_name": profile.profile_name,
        "scope": auth_scope_to_text(profile.scope.as_ref()),
        "credential": credential,
        "created_at_unix_ms": profile.created_at_unix_ms,
        "updated_at_unix_ms": profile.updated_at_unix_ms,
    })
}

fn auth_health_state_to_text(value: i32) -> &'static str {
    match auth_v1::AuthHealthState::try_from(value).unwrap_or(auth_v1::AuthHealthState::Unspecified)
    {
        auth_v1::AuthHealthState::Ok => "ok",
        auth_v1::AuthHealthState::Expiring => "expiring",
        auth_v1::AuthHealthState::Expired => "expired",
        auth_v1::AuthHealthState::Missing => "missing",
        auth_v1::AuthHealthState::Static => "static",
        auth_v1::AuthHealthState::Unspecified => "unspecified",
    }
}

fn auth_health_profile_to_json(value: &auth_v1::AuthProfileHealth) -> serde_json::Value {
    json!({
        "profile_id": value.profile_id,
        "provider": value.provider,
        "profile_name": value.profile_name,
        "scope": value.scope,
        "credential_type": value.credential_type,
        "state": auth_health_state_to_text(value.state),
        "reason": value.reason,
        "expires_at_unix_ms": if value.expires_at_unix_ms > 0 {
            Some(value.expires_at_unix_ms)
        } else {
            None
        },
    })
}

fn auth_health_summary_to_json(value: &auth_v1::AuthHealthSummary) -> serde_json::Value {
    json!({
        "total": value.total,
        "ok": value.ok,
        "expiring": value.expiring,
        "expired": value.expired,
        "missing": value.missing,
        "static_count": value.static_count,
    })
}

fn auth_expiry_distribution_to_json(value: &auth_v1::AuthExpiryDistribution) -> serde_json::Value {
    json!({
        "expired": value.expired,
        "under_5m": value.under_5m,
        "between_5m_15m": value.between_5m_15m,
        "between_15m_60m": value.between_15m_60m,
        "between_1h_24h": value.between_1h_24h,
        "over_24h": value.over_24h,
        "unknown": value.unknown,
        "static_count": value.static_count,
        "missing": value.missing,
    })
}

fn auth_refresh_metrics_to_json(value: &auth_v1::AuthRefreshMetrics) -> serde_json::Value {
    json!({
        "attempts": value.attempts,
        "successes": value.successes,
        "failures": value.failures,
        "by_provider": value.by_provider.iter().map(|entry| json!({
            "provider": entry.provider,
            "attempts": entry.attempts,
            "successes": entry.successes,
            "failures": entry.failures,
        })).collect::<Vec<_>>(),
    })
}

fn parse_float_arg(
    raw: Option<String>,
    name: &str,
    min: f64,
    max: f64,
    default: Option<f64>,
) -> Result<f64> {
    if let Some(raw) = raw {
        let value =
            raw.parse::<f64>().with_context(|| format!("{name} must be a valid floating value"))?;
        if !value.is_finite() || value < min || value > max {
            anyhow::bail!("{name} must be in range {min}..={max}");
        }
        return Ok(value);
    }
    if let Some(default) = default {
        return Ok(default);
    }
    anyhow::bail!("{name} is required")
}

fn optional_ulid_json_value(value: &Option<common_v1::CanonicalId>) -> Value {
    match value {
        Some(identifier) => Value::String(identifier.ulid.clone()),
        None => Value::Null,
    }
}

fn resolve_memory_scope(
    scope: MemoryScopeArg,
    channel: Option<String>,
    session: Option<String>,
    connection: &AgentConnection,
) -> Result<(Option<String>, Option<String>)> {
    let channel = channel.map(|value| value.trim().to_owned()).and_then(|value| {
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    });
    let session = session.map(|value| value.trim().to_owned()).and_then(|value| {
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    });
    if let Some(session_id) = session.as_deref() {
        validate_canonical_id(session_id).context("memory --session must be a canonical ULID")?;
    }

    match scope {
        MemoryScopeArg::Principal => Ok((None, None)),
        MemoryScopeArg::Channel => Ok((channel.or(Some(connection.channel.clone())), None)),
        MemoryScopeArg::Session => {
            let session = session.ok_or_else(|| {
                anyhow!("memory --scope session requires --session <canonical-ulid>")
            })?;
            Ok((channel.or(Some(connection.channel.clone())), Some(session)))
        }
    }
}

fn memory_source_to_proto(value: MemorySourceArg) -> i32 {
    match value {
        MemorySourceArg::TapeUserMessage => memory_v1::MemorySource::TapeUserMessage as i32,
        MemorySourceArg::TapeToolResult => memory_v1::MemorySource::TapeToolResult as i32,
        MemorySourceArg::Summary => memory_v1::MemorySource::Summary as i32,
        MemorySourceArg::Manual => memory_v1::MemorySource::Manual as i32,
        MemorySourceArg::Import => memory_v1::MemorySource::Import as i32,
    }
}

fn memory_source_to_text(value: i32) -> &'static str {
    match memory_v1::MemorySource::try_from(value).unwrap_or(memory_v1::MemorySource::Unspecified) {
        memory_v1::MemorySource::TapeUserMessage => "tape:user_message",
        memory_v1::MemorySource::TapeToolResult => "tape:tool_result",
        memory_v1::MemorySource::Summary => "summary",
        memory_v1::MemorySource::Manual => "manual",
        memory_v1::MemorySource::Import => "import",
        memory_v1::MemorySource::Unspecified => "unspecified",
    }
}

fn memory_item_to_json(item: &memory_v1::MemoryItem) -> serde_json::Value {
    let session_reference = optional_ulid_json_value(&item.session_id);
    json!({
        "memory_id": item.memory_id.as_ref().map(|value| value.ulid.clone()),
        "principal": item.principal,
        "channel": item.channel,
        "session_id": session_reference,
        "source": memory_source_to_text(item.source),
        "content_text": item.content_text,
        "content_hash": item.content_hash,
        "tags": item.tags,
        "confidence": item.confidence,
        "ttl_unix_ms": item.ttl_unix_ms,
        "created_at_unix_ms": item.created_at_unix_ms,
        "updated_at_unix_ms": item.updated_at_unix_ms,
    })
}

fn memory_search_hit_to_json(hit: &memory_v1::MemorySearchHit) -> serde_json::Value {
    let breakdown = hit.breakdown.as_ref().map(|value| {
        json!({
            "lexical_score": value.lexical_score,
            "vector_score": value.vector_score,
            "recency_score": value.recency_score,
            "final_score": value.final_score,
        })
    });
    json!({
        "item": hit.item.as_ref().map(memory_item_to_json),
        "snippet": hit.snippet,
        "score": hit.score,
        "breakdown": breakdown,
    })
}

fn approval_record_to_json(approval: &gateway_v1::ApprovalRecord) -> serde_json::Value {
    let session_reference = optional_ulid_json_value(&approval.session_id);
    let prompt = approval.prompt.as_ref().map(|prompt| {
        let details_json = if prompt.details_json.is_empty() {
            json!({})
        } else {
            serde_json::from_slice::<serde_json::Value>(prompt.details_json.as_slice())
                .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(prompt.details_json.as_slice()).to_string() }))
        };
        json!({
            "title": prompt.title,
            "risk_level": approval_risk_to_text(prompt.risk_level),
            "subject_id": prompt.subject_id,
            "summary": prompt.summary,
            "timeout_seconds": prompt.timeout_seconds,
            "policy_explanation": prompt.policy_explanation,
            "options": prompt.options.iter().map(|option| json!({
                "option_id": option.option_id,
                "label": option.label,
                "description": option.description,
                "default_selected": option.default_selected,
                "decision_scope": approval_scope_to_text(option.decision_scope),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })).collect::<Vec<_>>(),
            "details_json": details_json,
        })
    });
    json!({
        "approval_id": approval.approval_id.as_ref().map(|value| value.ulid.clone()),
        "session_id": session_reference,
        "run_id": approval.run_id.as_ref().map(|value| value.ulid.clone()),
        "principal": approval.principal,
        "device_id": approval.device_id,
        "channel": approval.channel,
        "requested_at_unix_ms": approval.requested_at_unix_ms,
        "resolved_at_unix_ms": approval.resolved_at_unix_ms,
        "subject_type": approval_subject_type_to_text(approval.subject_type),
        "subject_id": approval.subject_id,
        "request_summary": approval.request_summary,
        "decision": approval_decision_to_text(approval.decision),
        "decision_scope": approval_scope_to_text(approval.decision_scope),
        "decision_reason": approval.decision_reason,
        "decision_scope_ttl_ms": approval.decision_scope_ttl_ms,
        "policy_snapshot": approval.policy_snapshot.as_ref().map(|value| json!({
            "policy_id": value.policy_id,
            "policy_hash": value.policy_hash,
            "evaluation_summary": value.evaluation_summary,
        })),
        "prompt": prompt,
    })
}

fn approval_subject_type_to_text(value: i32) -> &'static str {
    match gateway_v1::ApprovalSubjectType::try_from(value)
        .unwrap_or(gateway_v1::ApprovalSubjectType::Unspecified)
    {
        gateway_v1::ApprovalSubjectType::Tool => "tool",
        gateway_v1::ApprovalSubjectType::ChannelSend => "channel_send",
        gateway_v1::ApprovalSubjectType::SecretAccess => "secret_access",
        gateway_v1::ApprovalSubjectType::BrowserAction => "browser_action",
        gateway_v1::ApprovalSubjectType::NodeCapability => "node_capability",
        gateway_v1::ApprovalSubjectType::DevicePairing => "device_pairing",
        gateway_v1::ApprovalSubjectType::Unspecified => "unspecified",
    }
}

fn approval_decision_to_text(value: i32) -> &'static str {
    match gateway_v1::ApprovalDecision::try_from(value)
        .unwrap_or(gateway_v1::ApprovalDecision::Unspecified)
    {
        gateway_v1::ApprovalDecision::Allow => "allow",
        gateway_v1::ApprovalDecision::Deny => "deny",
        gateway_v1::ApprovalDecision::Timeout => "timeout",
        gateway_v1::ApprovalDecision::Error => "error",
        gateway_v1::ApprovalDecision::Unspecified => "unspecified",
    }
}

fn approval_scope_to_text(value: i32) -> &'static str {
    match common_v1::ApprovalDecisionScope::try_from(value)
        .unwrap_or(common_v1::ApprovalDecisionScope::Unspecified)
    {
        common_v1::ApprovalDecisionScope::Once => "once",
        common_v1::ApprovalDecisionScope::Session => "session",
        common_v1::ApprovalDecisionScope::Timeboxed => "timeboxed",
        common_v1::ApprovalDecisionScope::Unspecified => "unspecified",
    }
}

fn approval_risk_to_text(value: i32) -> &'static str {
    match common_v1::ApprovalRiskLevel::try_from(value)
        .unwrap_or(common_v1::ApprovalRiskLevel::Unspecified)
    {
        common_v1::ApprovalRiskLevel::Low => "low",
        common_v1::ApprovalRiskLevel::Medium => "medium",
        common_v1::ApprovalRiskLevel::High => "high",
        common_v1::ApprovalRiskLevel::Critical => "critical",
        common_v1::ApprovalRiskLevel::Unspecified => "unspecified",
    }
}

fn approval_decision_filter_to_proto(value: Option<ApprovalDecisionArg>) -> i32 {
    match value {
        Some(ApprovalDecisionArg::Allow) => gateway_v1::ApprovalDecision::Allow as i32,
        Some(ApprovalDecisionArg::Deny) => gateway_v1::ApprovalDecision::Deny as i32,
        Some(ApprovalDecisionArg::Timeout) => gateway_v1::ApprovalDecision::Timeout as i32,
        Some(ApprovalDecisionArg::Error) => gateway_v1::ApprovalDecision::Error as i32,
        None => gateway_v1::ApprovalDecision::Unspecified as i32,
    }
}

fn approval_export_format_to_proto(value: ApprovalExportFormatArg) -> i32 {
    match value {
        ApprovalExportFormatArg::Ndjson => gateway_v1::ApprovalExportFormat::Ndjson as i32,
        ApprovalExportFormatArg::Json => gateway_v1::ApprovalExportFormat::Json as i32,
    }
}

fn normalize_agent_id_cli(raw: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        anyhow::bail!("agent id cannot be empty");
    }
    if value.len() > 64 {
        anyhow::bail!("agent id cannot exceed 64 bytes");
    }
    for character in value.chars() {
        if !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')) {
            anyhow::bail!("agent id contains unsupported character '{character}'");
        }
    }
    Ok(value.to_ascii_lowercase())
}

fn empty_to_none(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_owned())
}

fn agent_to_json(agent: &gateway_v1::Agent) -> serde_json::Value {
    json!({
        "agent_id": agent.agent_id,
        "display_name": agent.display_name,
        "agent_dir": agent.agent_dir,
        "workspace_roots": agent.workspace_roots,
        "default_model_profile": agent.default_model_profile,
        "execution_backend_preference": empty_to_none(agent.execution_backend_preference.clone()),
        "default_tool_allowlist": agent.default_tool_allowlist,
        "default_skill_allowlist": agent.default_skill_allowlist,
        "created_at_unix_ms": agent.created_at_unix_ms,
        "updated_at_unix_ms": agent.updated_at_unix_ms,
    })
}

fn open_url_in_default_browser(url: &str) -> Result<()> {
    let normalized_url = normalize_browser_open_url(url)?;

    let commands = browser_open_commands(normalized_url.as_str());
    let mut failures = Vec::with_capacity(commands.len());
    for command in commands {
        match Command::new(command.program).args(&command.args).status() {
            Ok(status) if status.success() => return Ok(()),
            Ok(status) => failures.push(format!(
                "{} exited with status {}",
                command.display(),
                status
                    .code()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_owned())
            )),
            Err(error) => failures.push(format!("{} failed: {error}", command.display())),
        }
    }

    anyhow::bail!("browser open command failed; tried {}", failures.join("; "))
}

fn normalize_browser_open_url(raw: &str) -> Result<String> {
    let parsed = Url::parse(raw).with_context(|| "browser open requires a valid absolute URL")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        anyhow::bail!("browser open only supports http:// and https:// URLs");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("browser open URL must not include embedded credentials");
    }
    Ok(parsed.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BrowserOpenCommand {
    program: &'static str,
    args: Vec<String>,
}

impl BrowserOpenCommand {
    fn display(&self) -> String {
        if self.args.is_empty() {
            return format!("`{}`", self.program);
        }
        format!("`{} {}`", self.program, self.args.join(" "))
    }
}

#[cfg(target_os = "windows")]
fn browser_open_commands(url: &str) -> Vec<BrowserOpenCommand> {
    vec![
        BrowserOpenCommand {
            program: "cmd",
            args: vec!["/C".to_owned(), "start".to_owned(), "\"\"".to_owned(), url.to_owned()],
        },
        BrowserOpenCommand { program: "explorer.exe", args: vec![url.to_owned()] },
    ]
}

#[cfg(target_os = "macos")]
fn browser_open_commands(url: &str) -> Vec<BrowserOpenCommand> {
    vec![BrowserOpenCommand { program: "open", args: vec![url.to_owned()] }]
}

#[cfg(all(unix, not(target_os = "macos")))]
fn browser_open_commands(url: &str) -> Vec<BrowserOpenCommand> {
    vec![BrowserOpenCommand { program: "xdg-open", args: vec![url.to_owned()] }]
}

fn execute_agent_stream(
    connection: AgentConnection,
    request: AgentRunInput,
    ndjson: bool,
) -> Result<()> {
    let runtime = build_runtime()?;
    runtime
        .block_on(async {
            let mut client = client::runtime::GatewayRuntimeClient::connect(connection).await?;
            let _resolved = stream_agent_events_async(&mut client, request, |event| {
                if ndjson {
                    emit_acp_event_ndjson(event)
                } else {
                    emit_agent_event_text(event)
                }
            })
            .await?;
            Result::<()>::Ok(())
        })
        .context("agent stream execution failed")
}

fn run_agent_stream_as_acp(connection: AgentConnection, request: AgentRunInput) -> Result<()> {
    let runtime = build_runtime()?;
    runtime
        .block_on(async {
            let mut client = client::runtime::GatewayRuntimeClient::connect(connection).await?;
            let _resolved =
                stream_agent_events_async(&mut client, request, emit_acp_event_ndjson).await?;
            Result::<()>::Ok(())
        })
        .context("ACP shim stream execution failed")
}

async fn stream_agent_events_async<F>(
    client: &mut client::runtime::GatewayRuntimeClient,
    request: AgentRunInput,
    mut emit_event: F,
) -> Result<ResolvedAgentRunInput>
where
    F: FnMut(&common_v1::RunStreamEvent) -> Result<()>,
{
    let resolved = prepare_agent_run_input(client, request).await?;
    let session_id = session_summary_reference(&resolved.session)?;
    let mut stream = client.open_run_stream(build_resolved_run_stream_request(&resolved)?).await?;
    let mut request_stream_closed = false;
    while let Some(event) = stream.next_event().await? {
        let reached_terminal_status = matches!(
            event.body.as_ref(),
            Some(common_v1::run_stream_event::Body::Status(status))
                if is_terminal_stream_status(status.kind)
        );
        if !request_stream_closed && run_stream_can_close_request_side(&event) {
            stream.close_request_stream().await?;
            request_stream_closed = true;
        }
        emit_event(&event)?;
        std::io::stdout().flush().context("stdout flush failed")?;
        if let Some(common_v1::run_stream_event::Body::ToolApprovalRequest(approval)) =
            event.body.as_ref()
        {
            let decision = prompt_tool_approval_decision(approval)?;
            stream
                .send_tool_approval_response(
                    session_id.ulid.as_str(),
                    resolved.run_id.as_str(),
                    common_v1::ToolApprovalResponse {
                        proposal_id: approval.proposal_id.clone(),
                        approved: decision.approved,
                        reason: decision.reason,
                        approval_id: approval.approval_id.clone(),
                        decision_scope: common_v1::ApprovalDecisionScope::Once as i32,
                        decision_scope_ttl_ms: 0,
                    },
                )
                .await?;
        }
        if reached_terminal_status {
            break;
        }
    }
    Ok(resolved)
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
    build_agent_run_input(AgentRunInputArgs {
        session_id: resolve_optional_canonical_id(parsed.session_id)?,
        session_key: parsed.session_key,
        session_label: parsed.session_label,
        require_existing: parsed.require_existing.unwrap_or(false),
        reset_session: parsed.reset_session.unwrap_or(false),
        run_id: parsed.run_id,
        prompt: prompt.to_owned(),
        allow_sensitive_tools: parsed
            .allow_sensitive_tools
            .unwrap_or(default_allow_sensitive_tools),
        origin_kind: None,
        origin_run_id: None,
        parameter_delta_json: None,
    })
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

struct AgentRunInputArgs {
    session_id: Option<common_v1::CanonicalId>,
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: bool,
    reset_session: bool,
    run_id: Option<String>,
    prompt: String,
    allow_sensitive_tools: bool,
    origin_kind: Option<String>,
    origin_run_id: Option<String>,
    parameter_delta_json: Option<String>,
}

fn build_agent_run_input(input: AgentRunInputArgs) -> Result<AgentRunInput> {
    Ok(AgentRunInput {
        session_id: input.session_id,
        session_key: normalize_optional_owned_text(input.session_key),
        session_label: normalize_optional_owned_text(input.session_label),
        require_existing: input.require_existing,
        reset_session: input.reset_session,
        run_id: resolve_or_generate_canonical_id(input.run_id)?,
        prompt: input.prompt,
        allow_sensitive_tools: input.allow_sensitive_tools,
        origin_kind: normalize_optional_owned_text(input.origin_kind),
        origin_run_id: normalize_optional_owned_text(input.origin_run_id),
        parameter_delta_json: normalize_optional_owned_text(input.parameter_delta_json),
    })
}

async fn prepare_agent_run_input(
    client: &mut client::runtime::GatewayRuntimeClient,
    input: AgentRunInput,
) -> Result<ResolvedAgentRunInput> {
    let response = client
        .resolve_session(SessionResolveInput {
            session_id: input.session_id.clone(),
            session_key: input.session_key.clone().unwrap_or_default(),
            session_label: input.session_label.clone().unwrap_or_default(),
            require_existing: input.require_existing,
            reset_session: input.reset_session,
        })
        .await?;
    let session = response.session.context("ResolveSession returned empty session payload")?;
    Ok(ResolvedAgentRunInput {
        session,
        run_id: input.run_id,
        prompt: input.prompt,
        allow_sensitive_tools: input.allow_sensitive_tools,
        origin_kind: input.origin_kind,
        origin_run_id: input.origin_run_id,
        parameter_delta_json: input.parameter_delta_json,
    })
}

fn normalize_optional_owned_text(value: Option<String>) -> Option<String> {
    value.and_then(|value| empty_to_none(value.trim().to_owned()))
}

fn session_summary_reference(
    session: &gateway_v1::SessionSummary,
) -> Result<common_v1::CanonicalId> {
    session.session_id.clone().context("resolved session is missing session_id")
}

#[derive(Debug, Clone)]
struct ToolApprovalDecision {
    approved: bool,
    reason: String,
}

fn prompt_tool_approval_decision(
    approval: &common_v1::ToolApprovalRequest,
) -> Result<ToolApprovalDecision> {
    let tool_name = approval.tool_name.trim();
    let summary = approval.request_summary.trim();
    if !std::io::stdin().is_terminal() {
        return Ok(ToolApprovalDecision {
            approved: false,
            reason: "approval_required_non_interactive_cli".to_owned(),
        });
    }

    let tool_label = if tool_name.is_empty() { "unknown" } else { tool_name };
    eprintln!(
        "agent.approval.required tool={} summary={}",
        redacted_presence_for_output(tool_label != "unknown"),
        redacted_presence_for_output(!summary.is_empty())
    );
    eprint!("agent.approval.prompt allow_once [y/N]: ");
    std::io::stderr().flush().context("stderr flush failed")?;
    let mut input = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut input)
        .context("failed to read tool approval decision from stdin")?;
    let approved = matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes");
    Ok(ToolApprovalDecision {
        approved,
        reason: if approved {
            "approved_by_cli_terminal".to_owned()
        } else {
            "denied_by_cli_terminal".to_owned()
        },
    })
}

fn resolve_or_generate_canonical_id(value: Option<String>) -> Result<String> {
    let resolved = value.unwrap_or_else(generate_canonical_ulid);
    validate_canonical_id(resolved.as_str()).context("invalid canonical ULID provided")?;
    Ok(resolved)
}

pub(crate) fn resolve_required_canonical_id(value: String) -> Result<common_v1::CanonicalId> {
    resolve_or_generate_canonical_id(Some(value)).map(|ulid| common_v1::CanonicalId { ulid })
}

pub(crate) fn resolve_optional_canonical_id(
    value: Option<String>,
) -> Result<Option<common_v1::CanonicalId>> {
    value.map(resolve_required_canonical_id).transpose()
}

fn generate_canonical_ulid() -> String {
    Ulid::new().to_string()
}

fn resolve_grpc_url(explicit: Option<String>) -> Result<String> {
    client::grpc::resolve_url(explicit)
}

fn normalize_client_socket(socket: SocketAddr) -> SocketAddr {
    client::grpc::normalize_client_socket(socket)
}

fn build_runtime() -> Result<tokio::runtime::Runtime> {
    client::grpc::build_runtime()
}

fn resolve_onboarding_path(path: Option<String>) -> Result<PathBuf> {
    if let Some(path) = path {
        return parse_config_path(path.as_str())
            .with_context(|| format!("onboarding config path is invalid: {}", path));
    }
    Ok(PathBuf::from("palyra.toml"))
}

async fn fetch_grpc_health_with_retry(grpc_url: String) -> Result<gateway_v1::HealthResponse> {
    client::grpc::fetch_health_with_retry(grpc_url).await
}

#[cfg(test)]
fn is_retryable_grpc_error(error: &anyhow::Error) -> bool {
    client::grpc::is_retryable_error(error)
}

fn inject_run_stream_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    connection: &AgentConnection,
) -> Result<()> {
    client::grpc::inject_run_stream_metadata(metadata, connection)
}

fn build_resolved_run_stream_request(
    input: &ResolvedAgentRunInput,
) -> Result<common_v1::RunStreamRequest> {
    let timestamp_unix_ms = now_unix_ms_i64()?;
    let session_id = session_summary_reference(&input.session)?;
    Ok(common_v1::RunStreamRequest {
        v: RUN_STREAM_REQUEST_VERSION,
        session_id: Some(session_id.clone()),
        run_id: Some(common_v1::CanonicalId { ulid: input.run_id.clone() }),
        input: Some(common_v1::MessageEnvelope {
            v: CANONICAL_JSON_ENVELOPE_VERSION,
            envelope_id: Some(common_v1::CanonicalId { ulid: generate_canonical_ulid() }),
            timestamp_unix_ms,
            origin: Some(common_v1::EnvelopeOrigin {
                r#type: common_v1::envelope_origin::OriginType::Cli as i32,
                channel: DEFAULT_CHANNEL.to_owned(),
                conversation_id: REDACTED.to_owned(),
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
        session_key: input.session.session_key.clone(),
        session_label: input.session.session_label.clone(),
        reset_session: false,
        require_existing: true,
        tool_approval_response: None,
        origin_kind: input.origin_kind.clone().unwrap_or_default(),
        origin_run_id: input
            .origin_run_id
            .as_ref()
            .map(|ulid| common_v1::CanonicalId { ulid: ulid.clone() }),
        parameter_delta_json: input
            .parameter_delta_json
            .as_ref()
            .map(|value| value.as_bytes().to_vec())
            .unwrap_or_default(),
        queued_input_id: None,
    })
}

fn build_run_stream_request(input: &AgentRunInput) -> Result<common_v1::RunStreamRequest> {
    let timestamp_unix_ms = now_unix_ms_i64()?;
    let session_id = input
        .session_id
        .clone()
        .unwrap_or_else(|| common_v1::CanonicalId { ulid: generate_canonical_ulid() });
    Ok(common_v1::RunStreamRequest {
        v: RUN_STREAM_REQUEST_VERSION,
        session_id: Some(session_id),
        run_id: Some(common_v1::CanonicalId { ulid: input.run_id.clone() }),
        input: Some(common_v1::MessageEnvelope {
            v: CANONICAL_JSON_ENVELOPE_VERSION,
            envelope_id: Some(common_v1::CanonicalId { ulid: generate_canonical_ulid() }),
            timestamp_unix_ms,
            origin: Some(common_v1::EnvelopeOrigin {
                r#type: common_v1::envelope_origin::OriginType::Cli as i32,
                channel: DEFAULT_CHANNEL.to_owned(),
                conversation_id: REDACTED.to_owned(),
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
        session_key: input.session_key.clone().unwrap_or_default(),
        session_label: input.session_label.clone().unwrap_or_default(),
        reset_session: input.reset_session,
        require_existing: input.require_existing,
        tool_approval_response: None,
        origin_kind: input.origin_kind.clone().unwrap_or_default(),
        origin_run_id: input
            .origin_run_id
            .as_ref()
            .map(|ulid| common_v1::CanonicalId { ulid: ulid.clone() }),
        parameter_delta_json: input
            .parameter_delta_json
            .as_ref()
            .map(|value| value.as_bytes().to_vec())
            .unwrap_or_default(),
        queued_input_id: None,
    })
}

fn emit_agent_event_text(event: &common_v1::RunStreamEvent) -> Result<()> {
    let run_id = redacted_presence_for_output(event.run_id.is_some());
    match event.body.as_ref() {
        Some(common_v1::run_stream_event::Body::ModelToken(token)) => {
            println!(
                "agent.token run_id={} token={} final={}",
                run_id,
                redacted_presence_for_output(!token.token.trim().is_empty()),
                token.is_final
            );
        }
        Some(common_v1::run_stream_event::Body::Status(status)) => {
            println!(
                "agent.status run_id={} kind={} message={}",
                run_id,
                stream_status_kind_to_text(status.kind),
                redacted_presence_for_output(!status.message.trim().is_empty())
            );
        }
        Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => {
            println!(
                "agent.tool.proposal run_id={} proposal_id={} tool_name={} approval_required={}",
                run_id,
                redacted_presence_for_output(proposal.proposal_id.is_some()),
                redacted_presence_for_output(!proposal.tool_name.trim().is_empty()),
                proposal.approval_required
            );
        }
        Some(common_v1::run_stream_event::Body::ToolDecision(decision)) => {
            println!(
                "agent.tool.decision run_id={} proposal_id={} kind={} reason={} approval_required={} policy_enforced={}",
                run_id,
                redacted_presence_for_output(decision.proposal_id.is_some()),
                tool_decision_kind_to_text(decision.kind),
                redacted_presence_for_output(!decision.reason.trim().is_empty()),
                decision.approval_required,
                decision.policy_enforced
            );
        }
        Some(common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request)) => {
            println!(
                "agent.tool.approval.request run_id={} proposal_id={} approval_id={} tool_name={} approval_required={} summary=\"{}\"",
                run_id,
                redacted_presence_for_output(approval_request.proposal_id.is_some()),
                redacted_presence_for_output(approval_request.approval_id.is_some()),
                redacted_presence_for_output(!approval_request.tool_name.trim().is_empty()),
                approval_request.approval_required,
                redacted_presence_for_output(!approval_request.request_summary.trim().is_empty())
            );
        }
        Some(common_v1::run_stream_event::Body::ToolApprovalResponse(approval_response)) => {
            println!(
                "agent.tool.approval.response run_id={} proposal_id={} approval_id={} approved={} scope={} ttl_ms={} reason={}",
                run_id,
                redacted_presence_for_output(approval_response.proposal_id.is_some()),
                redacted_presence_for_output(approval_response.approval_id.is_some()),
                approval_response.approved,
                approval_scope_to_text(approval_response.decision_scope),
                approval_response.decision_scope_ttl_ms,
                redacted_presence_for_output(!approval_response.reason.trim().is_empty())
            );
        }
        Some(common_v1::run_stream_event::Body::ToolResult(result)) => {
            println!(
                "agent.tool.result run_id={} proposal_id={} success={} error={}",
                run_id,
                redacted_presence_for_output(result.proposal_id.is_some()),
                result.success,
                redacted_presence_for_output(!result.error.trim().is_empty())
            );
        }
        Some(common_v1::run_stream_event::Body::ToolAttestation(attestation)) => {
            println!(
                "agent.tool.attestation run_id={} proposal_id={} attestation_id={} timed_out={} executor={}",
                run_id,
                redacted_presence_for_output(attestation.proposal_id.is_some()),
                redacted_presence_for_output(attestation.attestation_id.is_some()),
                attestation.timed_out,
                redacted_presence_for_output(!attestation.executor.trim().is_empty())
            );
        }
        Some(common_v1::run_stream_event::Body::A2uiUpdate(update)) => {
            println!(
                "agent.a2ui.update run_id={} surface={} version={}",
                run_id,
                redacted_presence_for_output(!update.surface.trim().is_empty()),
                update.v
            );
        }
        Some(common_v1::run_stream_event::Body::JournalEvent(journal_event)) => {
            println!(
                "agent.journal.event run_id={} event_id={} kind={} actor={}",
                run_id,
                redacted_presence_for_output(journal_event.event_id.is_some()),
                journal_event.kind,
                redacted_presence_for_output(journal_event.actor != 0)
            );
        }
        None => {
            println!("agent.event run_id={} kind=unknown", run_id);
        }
    }
    Ok(())
}

fn emit_acp_event_ndjson(event: &common_v1::RunStreamEvent) -> Result<()> {
    let run_id = redacted_presence_for_output(event.run_id.is_some());
    let payload = match event.body.as_ref() {
        Some(common_v1::run_stream_event::Body::ModelToken(token)) => json!({
            "type": "model.token",
            "run_id": run_id,
            "token": redacted_presence_json_value(!token.token.trim().is_empty()),
            "is_final": token.is_final,
        }),
        Some(common_v1::run_stream_event::Body::Status(status)) => json!({
            "type": "run.status",
            "run_id": run_id,
            "kind": stream_status_kind_to_text(status.kind),
            "message": redacted_presence_json_value(!status.message.trim().is_empty()),
        }),
        Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => json!({
            "type": "tool.proposal",
            "run_id": run_id,
            "proposal_id": redacted_presence_json_value(proposal.proposal_id.is_some()),
            "tool_name": redacted_presence_json_value(!proposal.tool_name.trim().is_empty()),
            "approval_required": proposal.approval_required,
            "input_json": redacted_presence_json_value(!proposal.input_json.is_empty()),
        }),
        Some(common_v1::run_stream_event::Body::ToolDecision(decision)) => json!({
            "type": "tool.decision",
            "run_id": run_id,
            "proposal_id": redacted_presence_json_value(decision.proposal_id.is_some()),
            "kind": tool_decision_kind_to_text(decision.kind),
            "reason": redacted_presence_json_value(!decision.reason.trim().is_empty()),
            "approval_required": decision.approval_required,
            "policy_enforced": decision.policy_enforced,
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request)) => json!({
            "type": "tool.approval.request",
            "run_id": run_id,
            "proposal_id": redacted_presence_json_value(approval_request.proposal_id.is_some()),
            "approval_id": redacted_presence_json_value(approval_request.approval_id.is_some()),
            "tool_name": redacted_presence_json_value(!approval_request.tool_name.trim().is_empty()),
            "approval_required": approval_request.approval_required,
            "request_summary": redacted_presence_json_value(
                !approval_request.request_summary.trim().is_empty()
            ),
            "prompt": approval_request.prompt.as_ref().map(|prompt| json!({
                "title": redacted_presence_json_value(!prompt.title.trim().is_empty()),
                "risk_level": approval_risk_to_text(prompt.risk_level),
                "subject_id": redacted_presence_json_value(!prompt.subject_id.trim().is_empty()),
                "summary": redacted_presence_json_value(!prompt.summary.trim().is_empty()),
                "policy_explanation": redacted_presence_json_value(
                    !prompt.policy_explanation.trim().is_empty()
                ),
                "timeout_seconds": prompt.timeout_seconds,
                "options": prompt.options.iter().map(|option| json!({
                    "option_id": redacted_presence_json_value(!option.option_id.trim().is_empty()),
                    "label": redacted_presence_json_value(!option.label.trim().is_empty()),
                    "description": redacted_presence_json_value(!option.description.trim().is_empty()),
                    "default_selected": option.default_selected,
                    "decision_scope": approval_scope_to_text(option.decision_scope),
                    "timebox_ttl_ms": option.timebox_ttl_ms,
                })).collect::<Vec<_>>(),
                    "details_json": redacted_presence_json_value(!prompt.details_json.is_empty()),
            })),
            "input_json": redacted_presence_json_value(!approval_request.input_json.is_empty()),
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalResponse(approval_response)) => json!({
            "type": "tool.approval.response",
            "run_id": run_id,
            "proposal_id": redacted_presence_json_value(approval_response.proposal_id.is_some()),
            "approval_id": redacted_presence_json_value(approval_response.approval_id.is_some()),
            "approved": approval_response.approved,
            "reason": redacted_presence_json_value(!approval_response.reason.trim().is_empty()),
            "decision_scope": approval_scope_to_text(approval_response.decision_scope),
            "decision_scope_ttl_ms": approval_response.decision_scope_ttl_ms,
        }),
        Some(common_v1::run_stream_event::Body::ToolResult(result)) => json!({
            "type": "tool.result",
            "run_id": run_id,
            "proposal_id": redacted_presence_json_value(result.proposal_id.is_some()),
            "success": result.success,
            "output_json": redacted_presence_json_value(!result.output_json.is_empty()),
            "error": redacted_presence_json_value(!result.error.trim().is_empty()),
        }),
        Some(common_v1::run_stream_event::Body::ToolAttestation(attestation)) => json!({
            "type": "tool.attestation",
            "run_id": run_id,
            "proposal_id": redacted_presence_json_value(attestation.proposal_id.is_some()),
            "attestation_id": redacted_presence_json_value(attestation.attestation_id.is_some()),
            "execution_sha256": redacted_presence_json_value(!attestation.execution_sha256.trim().is_empty()),
            "executed_at_unix_ms": attestation.executed_at_unix_ms,
            "timed_out": attestation.timed_out,
            "executor": redacted_presence_json_value(!attestation.executor.trim().is_empty()),
        }),
        Some(common_v1::run_stream_event::Body::A2uiUpdate(update)) => json!({
            "type": "a2ui.update",
            "run_id": run_id,
            "surface": redacted_presence_json_value(!update.surface.trim().is_empty()),
            "version": update.v,
            "patch_json": redacted_presence_json_value(!update.patch_json.is_empty()),
        }),
        Some(common_v1::run_stream_event::Body::JournalEvent(journal_event)) => json!({
            "type": "journal.event",
            "run_id": run_id,
            "event_id": redacted_presence_json_value(journal_event.event_id.is_some()),
            "kind": journal_event.kind,
            "actor": redacted_presence_json_value(journal_event.actor != 0),
            "timestamp_unix_ms": journal_event.timestamp_unix_ms,
            "payload_json": redacted_presence_json_value(!journal_event.payload_json.is_empty()),
            "hash": redacted_presence_json_value(!journal_event.hash.trim().is_empty()),
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

fn is_terminal_stream_status(kind: i32) -> bool {
    kind == common_v1::stream_status::StatusKind::Done as i32
        || kind == common_v1::stream_status::StatusKind::Failed as i32
}

fn run_stream_can_close_request_side(event: &common_v1::RunStreamEvent) -> bool {
    matches!(
        event.body.as_ref(),
        Some(common_v1::run_stream_event::Body::ModelToken(token)) if token.is_final
    ) || matches!(
        event.body.as_ref(),
        Some(common_v1::run_stream_event::Body::ToolResult(_))
            | Some(common_v1::run_stream_event::Body::ToolAttestation(_))
    )
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

fn fetch_admin_status_payload(
    client: &Client,
    base_url: &str,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    trace_id: Option<String>,
) -> Result<Value> {
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
    if let Some(trace_id) = trace_id {
        request = request.header("x-palyra-trace-id", trace_id);
    }

    let mut payload: Value = request
        .send()
        .context("failed to call daemon admin status endpoint")?
        .error_for_status()
        .context("daemon admin status endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon admin status payload")?;
    redact_json_value_tree(&mut payload, None);
    Ok(payload)
}

fn fetch_admin_status(
    client: &Client,
    base_url: &str,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    trace_id: Option<String>,
) -> Result<AdminStatusResponse> {
    let payload = fetch_admin_status_payload(
        client, base_url, token, principal, device_id, channel, trace_id,
    )?;
    serde_json::from_value(payload).context("failed to decode daemon admin status summary payload")
}

#[derive(Debug, Clone)]
struct AgentConnection {
    grpc_url: String,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: String,
    trace_id: String,
}

#[derive(Clone)]
struct AgentRunInput {
    session_id: Option<common_v1::CanonicalId>,
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: bool,
    reset_session: bool,
    run_id: String,
    prompt: String,
    allow_sensitive_tools: bool,
    origin_kind: Option<String>,
    origin_run_id: Option<String>,
    parameter_delta_json: Option<String>,
}

#[derive(Clone)]
struct ResolvedAgentRunInput {
    session: gateway_v1::SessionSummary,
    run_id: String,
    prompt: String,
    allow_sensitive_tools: bool,
    origin_kind: Option<String>,
    origin_run_id: Option<String>,
    parameter_delta_json: Option<String>,
}

#[derive(Clone)]
pub(crate) struct SessionResolveInput {
    pub(crate) session_id: Option<common_v1::CanonicalId>,
    pub(crate) session_key: String,
    pub(crate) session_label: String,
    pub(crate) require_existing: bool,
    pub(crate) reset_session: bool,
}

#[derive(Clone)]
pub(crate) struct SessionCleanupInput {
    pub(crate) session_id: Option<common_v1::CanonicalId>,
    pub(crate) session_key: String,
}

#[derive(Clone)]
pub(crate) struct AgentBindingsQueryInput {
    pub(crate) agent_id: String,
    pub(crate) principal: String,
    pub(crate) channel: String,
    pub(crate) session_id: Option<common_v1::CanonicalId>,
    pub(crate) limit: u32,
}

#[derive(Clone)]
pub(crate) struct AgentContextResolveInput {
    pub(crate) principal: String,
    pub(crate) channel: String,
    pub(crate) session_id: Option<common_v1::CanonicalId>,
    pub(crate) preferred_agent_id: String,
    pub(crate) persist_session_binding: bool,
}

#[derive(Deserialize)]
struct AcpShimInput {
    session_id: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: Option<bool>,
    reset_session: Option<bool>,
    run_id: Option<String>,
    prompt: Option<String>,
    allow_sensitive_tools: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DashboardAccessMode {
    Local,
    Remote,
}

impl DashboardAccessMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DashboardAccessSource {
    ConfigRemoteUrl,
    ConfigDaemonBind,
    DefaultLoopback,
}

impl DashboardAccessSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ConfigRemoteUrl => "config_remote_url",
            Self::ConfigDaemonBind => "config_daemon_bind",
            Self::DefaultLoopback => "default_loopback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DashboardRemoteVerificationMethod {
    PinnedServerCertSha256,
    PinnedGatewayCaSha256,
}

impl DashboardRemoteVerificationMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PinnedServerCertSha256 => "pinned_server_cert_sha256",
            Self::PinnedGatewayCaSha256 => "pinned_gateway_ca_sha256",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DashboardRemoteVerification {
    method: DashboardRemoteVerificationMethod,
    expected_fingerprint_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DashboardAccessTarget {
    url: String,
    mode: DashboardAccessMode,
    source: DashboardAccessSource,
    config_path: Option<String>,
    verification: Option<DashboardRemoteVerification>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DashboardVerificationReport {
    method: DashboardRemoteVerificationMethod,
    expected_fingerprint_sha256: String,
    observed_server_cert_fingerprint_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gateway_ca_fingerprint_sha256: Option<String>,
    verified: bool,
}

fn redacted_dashboard_verification_report(
    verification: &DashboardRemoteVerification,
    verified: bool,
) -> DashboardVerificationReport {
    DashboardVerificationReport {
        method: verification.method,
        expected_fingerprint_sha256: REDACTED.to_owned(),
        observed_server_cert_fingerprint_sha256: REDACTED.to_owned(),
        gateway_ca_fingerprint_sha256: matches!(
            verification.method,
            DashboardRemoteVerificationMethod::PinnedGatewayCaSha256
        )
        .then(|| REDACTED.to_owned()),
        verified,
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct StoredGatewayCaState {
    certificate_pem: String,
}

fn resolve_dashboard_access_target(path_override: Option<String>) -> Result<DashboardAccessTarget> {
    let config_path = resolve_dashboard_config_path(path_override)?;
    let config_path_display = config_path.as_ref().map(|path| path.to_string_lossy().into_owned());

    if let Some(config_path) = config_path {
        let parsed = load_root_file_config(config_path.as_path())?;
        let gateway_access = parsed.gateway_access.as_ref();
        let remote_base_url = gateway_access
            .and_then(|access| access.remote_base_url.as_deref())
            .and_then(normalize_optional_text);
        if let Some(remote_base_url) = remote_base_url {
            let normalized_remote_url =
                parse_remote_dashboard_base_url(remote_base_url, "gateway_access.remote_base_url")?;
            let verification = resolve_dashboard_remote_verification(gateway_access)?;
            return Ok(DashboardAccessTarget {
                url: normalized_remote_url,
                mode: DashboardAccessMode::Remote,
                source: DashboardAccessSource::ConfigRemoteUrl,
                config_path: config_path_display,
                verification,
            });
        }

        let local_url = resolve_local_dashboard_url_from_root_config(&parsed)?;
        return Ok(DashboardAccessTarget {
            url: local_url,
            mode: DashboardAccessMode::Local,
            source: DashboardAccessSource::ConfigDaemonBind,
            config_path: config_path_display,
            verification: None,
        });
    }

    Ok(DashboardAccessTarget {
        url: format!("http://127.0.0.1:{DEFAULT_DAEMON_PORT}/"),
        mode: DashboardAccessMode::Local,
        source: DashboardAccessSource::DefaultLoopback,
        config_path: None,
        verification: None,
    })
}

fn resolve_dashboard_config_path(path_override: Option<String>) -> Result<Option<PathBuf>> {
    if let Some(path_override) = path_override {
        let parsed = parse_config_path(path_override.as_str())
            .with_context(|| format!("dashboard config path is invalid: {}", path_override))?;
        if !parsed.exists() {
            anyhow::bail!("dashboard config file does not exist: {}", parsed.display());
        }
        return Ok(Some(parsed));
    }

    if let Ok(explicit) = env::var("PALYRA_CONFIG") {
        let trimmed = explicit.trim();
        if !trimmed.is_empty() {
            let parsed = parse_config_path(trimmed)
                .with_context(|| "PALYRA_CONFIG contains an invalid path")?;
            if parsed.exists() {
                return Ok(Some(parsed));
            }
        }
    }

    for candidate in default_config_search_paths() {
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }

    Ok(None)
}

fn load_root_file_config(path: &Path) -> Result<RootFileConfig> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let (document, _) = parse_document_with_migration(content.as_str())
        .with_context(|| format!("failed to migrate {}", path.display()))?;
    let migrated =
        toml::to_string(&document).context("failed to serialize migrated config document")?;
    toml::from_str(migrated.as_str()).context("invalid daemon config schema")
}

fn resolve_local_dashboard_url_from_root_config(parsed: &RootFileConfig) -> Result<String> {
    let bind_addr = parsed
        .daemon
        .as_ref()
        .and_then(|daemon| daemon.bind_addr.as_deref())
        .unwrap_or(DEFAULT_DAEMON_BIND_ADDR);
    let port = parsed.daemon.as_ref().and_then(|daemon| daemon.port).unwrap_or(DEFAULT_DAEMON_PORT);
    let socket = parse_daemon_bind_socket(bind_addr, port)
        .with_context(|| format!("invalid daemon bind config ({bind_addr}:{port})"))?;
    Ok(format!("http://{}/", normalize_client_socket(socket)))
}

fn parse_remote_dashboard_base_url(raw: &str, source_name: &str) -> Result<String> {
    let parsed =
        Url::parse(raw).with_context(|| format!("{source_name} must be a valid absolute URL"))?;
    if parsed.scheme() != "https" {
        anyhow::bail!("{source_name} must use https://");
    }
    if parsed.host_str().is_none() {
        anyhow::bail!("{source_name} must include a host");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("{source_name} must not include embedded credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("{source_name} must not include query or fragment");
    }
    Ok(parsed.to_string())
}

fn resolve_dashboard_remote_verification(
    gateway_access: Option<&palyra_common::daemon_config_schema::FileGatewayAccessConfig>,
) -> Result<Option<DashboardRemoteVerification>> {
    let Some(gateway_access) = gateway_access else {
        return Ok(None);
    };
    let pinned_server = gateway_access
        .pinned_server_cert_fingerprint_sha256
        .as_deref()
        .and_then(normalize_optional_text);
    let pinned_gateway_ca = gateway_access
        .pinned_gateway_ca_fingerprint_sha256
        .as_deref()
        .and_then(normalize_optional_text);

    if pinned_server.is_some() && pinned_gateway_ca.is_some() {
        anyhow::bail!(
            "gateway_access pins are ambiguous: configure only one of \
             pinned_server_cert_fingerprint_sha256 or pinned_gateway_ca_fingerprint_sha256"
        );
    }

    if let Some(pinned_server) = pinned_server {
        let expected = normalize_sha256_fingerprint(
            pinned_server,
            "gateway_access.pinned_server_cert_fingerprint_sha256",
        )?;
        return Ok(Some(DashboardRemoteVerification {
            method: DashboardRemoteVerificationMethod::PinnedServerCertSha256,
            expected_fingerprint_sha256: expected,
        }));
    }

    if let Some(pinned_gateway_ca) = pinned_gateway_ca {
        let expected = normalize_sha256_fingerprint(
            pinned_gateway_ca,
            "gateway_access.pinned_gateway_ca_fingerprint_sha256",
        )?;
        return Ok(Some(DashboardRemoteVerification {
            method: DashboardRemoteVerificationMethod::PinnedGatewayCaSha256,
            expected_fingerprint_sha256: expected,
        }));
    }

    Ok(None)
}

fn normalize_sha256_fingerprint(raw: &str, source_name: &str) -> Result<String> {
    let normalized = raw
        .chars()
        .filter(|value| !value.is_ascii_whitespace() && *value != ':')
        .map(|value| value.to_ascii_lowercase())
        .collect::<String>();
    if normalized.len() != 64 || !normalized.chars().all(|value| value.is_ascii_hexdigit()) {
        anyhow::bail!("{source_name} must contain exactly 64 hexadecimal characters");
    }
    Ok(normalized)
}

fn verify_dashboard_remote_target(
    target: &DashboardAccessTarget,
    identity_store_dir: Option<String>,
) -> Result<DashboardVerificationReport> {
    if target.mode != DashboardAccessMode::Remote {
        anyhow::bail!("remote dashboard verification requires a remote dashboard URL target");
    }
    let verification =
        target.verification.as_ref().context("remote verification pin is not configured")?;
    match verification.method {
        DashboardRemoteVerificationMethod::PinnedServerCertSha256 => {
            verify_remote_with_server_certificate_pin(
                target.url.as_str(),
                verification.expected_fingerprint_sha256.as_str(),
            )
        }
        DashboardRemoteVerificationMethod::PinnedGatewayCaSha256 => {
            verify_remote_with_gateway_ca_pin(
                target.url.as_str(),
                verification.expected_fingerprint_sha256.as_str(),
                identity_store_dir,
            )
        }
    }
}

fn verify_remote_with_server_certificate_pin(
    url: &str,
    expected_fingerprint_sha256: &str,
) -> Result<DashboardVerificationReport> {
    let parsed = Url::parse(url).with_context(|| format!("invalid remote dashboard URL: {url}"))?;
    if parsed.scheme() != "https" {
        anyhow::bail!("server certificate pin verification requires https:// URL");
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .redirect(RedirectPolicy::none())
        .tls_info(true)
        .tls_danger_accept_invalid_certs(true)
        .build()
        .context("failed to build verification HTTP client")?;
    let response = client
        .get(parsed.clone())
        .send()
        .with_context(|| format!("failed to connect remote dashboard URL {parsed}"))?;
    let observed_server_cert_fingerprint_sha256 =
        extract_peer_certificate_fingerprint_sha256(&response)?;
    if observed_server_cert_fingerprint_sha256 != expected_fingerprint_sha256 {
        anyhow::bail!(
            "server certificate fingerprint mismatch: expected={} observed={}",
            expected_fingerprint_sha256,
            observed_server_cert_fingerprint_sha256
        );
    }
    Ok(DashboardVerificationReport {
        method: DashboardRemoteVerificationMethod::PinnedServerCertSha256,
        expected_fingerprint_sha256: expected_fingerprint_sha256.to_owned(),
        observed_server_cert_fingerprint_sha256,
        gateway_ca_fingerprint_sha256: None,
        verified: true,
    })
}

fn verify_remote_with_gateway_ca_pin(
    url: &str,
    expected_ca_fingerprint_sha256: &str,
    identity_store_dir: Option<String>,
) -> Result<DashboardVerificationReport> {
    let parsed = Url::parse(url).with_context(|| format!("invalid remote dashboard URL: {url}"))?;
    if parsed.scheme() != "https" {
        anyhow::bail!("gateway CA pin verification requires https:// URL");
    }

    let gateway_ca_pem = load_gateway_ca_certificate_pem(identity_store_dir)?;
    let gateway_ca_der = decode_first_pem_certificate_der(gateway_ca_pem.as_str())?;
    let gateway_ca_fingerprint_sha256 = sha256_hex(gateway_ca_der.as_slice());
    if gateway_ca_fingerprint_sha256 != expected_ca_fingerprint_sha256 {
        anyhow::bail!(
            "gateway CA fingerprint mismatch: expected={} local={}",
            expected_ca_fingerprint_sha256,
            gateway_ca_fingerprint_sha256
        );
    }

    let ca_certificate = reqwest::Certificate::from_pem(gateway_ca_pem.as_bytes())
        .context("failed to parse gateway CA certificate PEM")?;
    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .redirect(RedirectPolicy::none())
        .tls_info(true)
        .tls_certs_only([ca_certificate])
        .build()
        .context("failed to build gateway-CA verification HTTP client")?;
    let response = client
        .get(parsed.clone())
        .send()
        .with_context(|| format!("failed to connect remote dashboard URL {parsed}"))?;
    let observed_server_cert_fingerprint_sha256 =
        extract_peer_certificate_fingerprint_sha256(&response)?;
    Ok(DashboardVerificationReport {
        method: DashboardRemoteVerificationMethod::PinnedGatewayCaSha256,
        expected_fingerprint_sha256: expected_ca_fingerprint_sha256.to_owned(),
        observed_server_cert_fingerprint_sha256,
        gateway_ca_fingerprint_sha256: Some(gateway_ca_fingerprint_sha256),
        verified: true,
    })
}

fn extract_peer_certificate_fingerprint_sha256(
    response: &reqwest::blocking::Response,
) -> Result<String> {
    let tls_info = response.extensions().get::<TlsInfo>().ok_or_else(|| {
        anyhow!("TLS peer certificate metadata is unavailable; enable HTTPS and TLS info capture")
    })?;
    let peer_certificate_der = tls_info.peer_certificate().ok_or_else(|| {
        anyhow!("TLS handshake did not expose a peer certificate for fingerprint verification")
    })?;
    Ok(sha256_hex(peer_certificate_der))
}

fn load_gateway_ca_certificate_pem(identity_store_dir: Option<String>) -> Result<String> {
    let identity_store_root = resolve_identity_store_root(identity_store_dir)?;
    let store = FilesystemSecretStore::new(identity_store_root.as_path()).with_context(|| {
        format!("failed to initialize identity store at {}", identity_store_root.display())
    })?;
    let raw = store.read_secret(GATEWAY_CA_STATE_KEY).map_err(anyhow::Error::from).with_context(
        || {
            format!(
                "failed to read gateway CA state from {} key {}",
                identity_store_root.display(),
                GATEWAY_CA_STATE_KEY
            )
        },
    )?;
    let parsed: StoredGatewayCaState = serde_json::from_slice(raw.as_slice())
        .context("failed to parse stored gateway CA state payload")?;
    let certificate_pem = normalize_required_text_arg(parsed.certificate_pem, "gateway CA cert")?;
    Ok(certificate_pem)
}

fn decode_first_pem_certificate_der(pem: &str) -> Result<Vec<u8>> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let begin = pem.find(BEGIN).context("certificate PEM BEGIN marker is missing")?;
    let after_begin = &pem[(begin + BEGIN.len())..];
    let end_offset = after_begin.find(END).context("certificate PEM END marker is missing")?;
    let body = &after_begin[..end_offset];
    let encoded = body.lines().map(str::trim).filter(|line| !line.is_empty()).collect::<String>();
    if encoded.is_empty() {
        anyhow::bail!("certificate PEM body is empty");
    }
    BASE64_STANDARD.decode(encoded.as_bytes()).context("certificate PEM body is not valid base64")
}

fn normalize_optional_text(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct JournalCheckpointOutput {
    db_path: String,
    mode: String,
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attestation: Option<JournalCheckpointAttestation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct JournalCheckpointAttestation {
    schema_version: u32,
    algorithm: String,
    key_id: String,
    public_key_base64: String,
    payload_sha256: String,
    signature_base64: String,
    payload: JournalCheckpointAttestationPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct JournalCheckpointAttestationPayload {
    db_path: String,
    mode: String,
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
    latest_hash: String,
    signed_at_unix_ms: i64,
    device_id: String,
}

#[derive(Debug, Clone, Copy)]
struct JournalCheckpointAttestationRequest<'a> {
    db_path: &'a Path,
    mode: JournalCheckpointModeArg,
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
    latest_hash: &'a str,
    signed_at_unix_ms: i64,
}

fn resolve_daemon_journal_db_path(db_path_override: Option<String>) -> Result<PathBuf> {
    if let Some(db_path_override) = db_path_override {
        let trimmed = db_path_override.trim();
        if trimmed.is_empty() {
            anyhow::bail!("journal database path cannot be empty");
        }
        return Ok(PathBuf::from(trimmed));
    }

    if let Ok(db_path_env) = env::var("PALYRA_JOURNAL_DB_PATH") {
        let trimmed = db_path_env.trim();
        if trimmed.is_empty() {
            anyhow::bail!("PALYRA_JOURNAL_DB_PATH cannot be empty");
        }
        return Ok(PathBuf::from(trimmed));
    }

    if let Some(config_path) = find_default_config_path() {
        let config_path = PathBuf::from(config_path);
        let (document, _) =
            load_document_from_existing_path(config_path.as_path()).with_context(|| {
                format!(
                    "failed to parse {} while resolving journal database path",
                    config_path.display()
                )
            })?;
        let content =
            toml::to_string(&document).context("failed to serialize daemon config document")?;
        let parsed: RootFileConfig = toml::from_str(content.as_str())
            .context("invalid daemon config schema while resolving journal database path")?;
        if let Some(journal_db_path) = parsed
            .storage
            .and_then(|storage| storage.journal_db_path)
            .map(|value| value.trim().to_owned())
        {
            if !journal_db_path.is_empty() {
                return Ok(PathBuf::from(journal_db_path));
            }
        }
    }

    Ok(PathBuf::from(DEFAULT_JOURNAL_DB_PATH))
}

fn ensure_journal_db_exists(db_path: &Path) -> Result<()> {
    if !db_path.exists() {
        anyhow::bail!("journal database path does not exist: {}", db_path.display());
    }
    if !db_path.is_file() {
        anyhow::bail!("journal database path must reference a file: {}", db_path.display());
    }
    Ok(())
}

const fn checkpoint_mode_sql(mode: JournalCheckpointModeArg) -> &'static str {
    match mode {
        JournalCheckpointModeArg::Passive => "PASSIVE",
        JournalCheckpointModeArg::Full => "FULL",
        JournalCheckpointModeArg::Restart => "RESTART",
        JournalCheckpointModeArg::Truncate => "TRUNCATE",
    }
}

const fn checkpoint_mode_label(mode: JournalCheckpointModeArg) -> &'static str {
    match mode {
        JournalCheckpointModeArg::Passive => "passive",
        JournalCheckpointModeArg::Full => "full",
        JournalCheckpointModeArg::Restart => "restart",
        JournalCheckpointModeArg::Truncate => "truncate",
    }
}

fn build_journal_checkpoint_attestation(
    device_identity: &DeviceIdentity,
    request: JournalCheckpointAttestationRequest<'_>,
) -> Result<JournalCheckpointAttestation> {
    let latest_hash = request.latest_hash.trim();
    if latest_hash.is_empty() {
        anyhow::bail!("journal checkpoint attestation requires a non-empty latest hash");
    }
    let payload = JournalCheckpointAttestationPayload {
        db_path: request.db_path.display().to_string(),
        mode: checkpoint_mode_label(request.mode).to_owned(),
        busy: request.busy,
        log_frames: request.log_frames,
        checkpointed_frames: request.checkpointed_frames,
        latest_hash: latest_hash.to_owned(),
        signed_at_unix_ms: request.signed_at_unix_ms,
        device_id: device_identity.device_id.clone(),
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .context("failed to serialize journal checkpoint attestation payload")?;
    let payload_sha256 = sha256_hex(payload_bytes.as_slice());
    let signature = device_identity.signing_key().sign(payload_bytes.as_slice());
    let verifying_key = device_identity.verifying_key();
    Ok(JournalCheckpointAttestation {
        schema_version: JOURNAL_CHECKPOINT_ATTESTATION_SCHEMA_VERSION,
        algorithm: JOURNAL_CHECKPOINT_ATTESTATION_ALGORITHM.to_owned(),
        key_id: registry_key_id_for(&verifying_key),
        public_key_base64: BASE64_STANDARD.encode(verifying_key.as_bytes()),
        payload_sha256,
        signature_base64: BASE64_STANDARD.encode(signature.to_bytes()),
        payload,
    })
}

fn format_config_get_display_value(key: &str, value: &toml::Value, show_secrets: bool) -> String {
    if show_secrets || !is_secret_config_path(key) {
        format_toml_value(value)
    } else {
        format_toml_value(&toml::Value::String(REDACTED_CONFIG_VALUE.to_owned()))
    }
}

fn read_skills_signing_key_source(
    signing_key_vault_ref: Option<&str>,
    signing_key_stdin: bool,
) -> Result<Vec<u8>> {
    match (signing_key_vault_ref, signing_key_stdin) {
        (Some(_), true) => {
            anyhow::bail!(
                "skills package build accepts either --signing-key-vault-ref or --signing-key-stdin"
            );
        }
        (Some(vault_ref_raw), false) => {
            let vault_ref = VaultRef::parse(vault_ref_raw).with_context(|| {
                format!(
                    "invalid --signing-key-vault-ref '{}'; expected '<scope>/<key>'",
                    vault_ref_raw.trim()
                )
            })?;
            let vault = open_cli_vault().context("failed to initialize vault runtime")?;
            vault
                .get_secret(&vault_ref.scope, vault_ref.key.as_str())
                .map_err(anyhow::Error::from)
                .with_context(|| {
                    format!(
                        "failed to load signing key from vault scope={} key={}",
                        vault_ref.scope, vault_ref.key
                    )
                })
        }
        (None, true) => {
            let mut secret = Vec::new();
            std::io::stdin()
                .read_to_end(&mut secret)
                .context("failed to read signing key from stdin")?;
            if secret.is_empty() {
                anyhow::bail!("stdin did not contain any signing key bytes");
            }
            Ok(secret)
        }
        (None, false) => {
            anyhow::bail!(
                "skills package build requires --signing-key-vault-ref <scope/key> or --signing-key-stdin"
            );
        }
    }
}

fn parse_trusted_publisher_arg(raw: &str) -> Result<(&str, &str)> {
    let (publisher, key) = raw.split_once('=').ok_or_else(|| {
        anyhow!(
            "invalid --trusted-publisher value '{}'; expected 'publisher=ed25519_hex_key'",
            raw.trim()
        )
    })?;
    if publisher.trim().is_empty() || key.trim().is_empty() {
        anyhow::bail!(
            "invalid --trusted-publisher value '{}'; expected non-empty 'publisher=ed25519_hex_key'",
            raw.trim()
        );
    }
    Ok((publisher.trim(), key.trim()))
}

fn resolve_skills_trust_store_path(raw: Option<&str>) -> Result<PathBuf> {
    if let Some(value) = raw {
        if value.trim().is_empty() {
            anyhow::bail!("--trust-store path cannot be empty");
        }
        return Ok(PathBuf::from(value));
    }

    match env::var("PALYRA_SKILLS_TRUST_STORE") {
        Ok(value) if !value.trim().is_empty() => Ok(PathBuf::from(value)),
        Ok(_) => anyhow::bail!("PALYRA_SKILLS_TRUST_STORE cannot be empty when set"),
        Err(std::env::VarError::NotPresent) => {
            let identity_root = default_identity_store_root()
                .context("failed to resolve default identity store root")?;
            let state_root = identity_root
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| identity_root.clone());
            Ok(state_root.join("skills").join("trust-store.json"))
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("PALYRA_SKILLS_TRUST_STORE must contain valid UTF-8")
        }
    }
}

fn load_trust_store_with_integrity(path: &Path) -> Result<SkillTrustStore> {
    let store = SkillTrustStore::load(path)?;
    verify_or_initialize_trust_store_integrity(path)?;
    Ok(store)
}

fn save_trust_store_with_integrity(path: &Path, store: &SkillTrustStore) -> Result<()> {
    store.save(path)?;
    update_trust_store_integrity_digest(path)
}

fn verify_or_initialize_trust_store_integrity(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let payload =
        fs::read(path).with_context(|| format!("failed to read trust store {}", path.display()))?;
    let observed_digest = sha256_hex(payload.as_slice());
    let key = trust_store_integrity_vault_key(path);
    let vault = open_cli_vault().context("failed to open vault for trust-store integrity check")?;
    match vault.get_secret(&TRUST_STORE_INTEGRITY_VAULT_SCOPE, key.as_str()) {
        Ok(expected_bytes) => {
            let expected_digest = String::from_utf8(expected_bytes).with_context(|| {
                format!("trust-store integrity record for {} is not valid UTF-8", path.display())
            })?;
            if expected_digest.trim() != observed_digest {
                anyhow::bail!(
                    "trust-store integrity mismatch detected for {} (expected digest {}, observed {})",
                    path.display(),
                    expected_digest.trim(),
                    observed_digest
                );
            }
        }
        Err(VaultError::NotFound) => {
            vault
                .put_secret(
                    &TRUST_STORE_INTEGRITY_VAULT_SCOPE,
                    key.as_str(),
                    observed_digest.as_bytes(),
                )
                .with_context(|| {
                    format!(
                        "failed to initialize trust-store integrity record in vault for {}",
                        path.display()
                    )
                })?;
        }
        Err(error) => {
            return Err(anyhow::Error::from(error)).with_context(|| {
                format!(
                    "failed to load trust-store integrity record from vault for {}",
                    path.display()
                )
            });
        }
    }
    Ok(())
}

fn update_trust_store_integrity_digest(path: &Path) -> Result<()> {
    let payload =
        fs::read(path).with_context(|| format!("failed to read trust store {}", path.display()))?;
    let digest = sha256_hex(payload.as_slice());
    let key = trust_store_integrity_vault_key(path);
    let vault =
        open_cli_vault().context("failed to open vault for trust-store integrity update")?;
    vault
        .put_secret(&TRUST_STORE_INTEGRITY_VAULT_SCOPE, key.as_str(), digest.as_bytes())
        .with_context(|| {
            format!(
                "failed to persist trust-store integrity record in vault for {}",
                path.display()
            )
        })?;
    Ok(())
}

fn trust_store_integrity_vault_key(path: &Path) -> String {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let normalized = canonical.to_string_lossy().to_ascii_lowercase();
    let path_digest = sha256_hex(normalized.as_bytes());
    format!("{}{}", TRUST_STORE_INTEGRITY_VAULT_KEY_PREFIX, &path_digest[..32])
}

fn skill_entry_path_from_cli(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("artifact file path cannot be empty");
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            anyhow::bail!("absolute artifact path '{}' has no file name", path.display());
        };
        return Ok(file_name.to_owned());
    }
    Ok(trimmed.replace('\\', "/"))
}

#[derive(Debug, Clone)]
struct SkillsInstallCommand {
    artifact: Option<String>,
    registry_dir: Option<String>,
    registry_url: Option<String>,
    skill_id: Option<String>,
    version: Option<String>,
    registry_ca_cert: Option<String>,
    skills_dir: Option<String>,
    trust_store: Option<String>,
    trusted_publishers: Vec<String>,
    allow_untrusted: bool,
    non_interactive: bool,
    json: bool,
}

#[derive(Debug, Clone)]
struct SkillsUpdateCommand {
    registry_dir: Option<String>,
    registry_url: Option<String>,
    skill_id: String,
    version: Option<String>,
    registry_ca_cert: Option<String>,
    skills_dir: Option<String>,
    trust_store: Option<String>,
    trusted_publishers: Vec<String>,
    allow_untrusted: bool,
    non_interactive: bool,
    json: bool,
}

#[derive(Debug, Clone)]
struct SkillsAuditCommand {
    skill_id: Option<String>,
    version: Option<String>,
    artifact: Option<String>,
    skills_dir: Option<String>,
    trust_store: Option<String>,
    trusted_publishers: Vec<String>,
    allow_untrusted: bool,
    json: bool,
}

#[derive(Debug, Clone)]
struct SkillsQuarantineCommand {
    skill_id: String,
    version: Option<String>,
    skills_dir: Option<String>,
    reason: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json: bool,
}

#[derive(Debug, Clone)]
struct SkillsEnableCommand {
    skill_id: String,
    version: Option<String>,
    skills_dir: Option<String>,
    override_enabled: bool,
    reason: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillsIndex {
    schema_version: u32,
    updated_at_unix_ms: i64,
    #[serde(default)]
    entries: Vec<InstalledSkillRecord>,
}

impl Default for InstalledSkillsIndex {
    fn default() -> Self {
        Self { schema_version: SKILLS_LAYOUT_VERSION, updated_at_unix_ms: 0, entries: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillRecord {
    skill_id: String,
    version: String,
    publisher: String,
    current: bool,
    installed_at_unix_ms: i64,
    artifact_sha256: String,
    payload_sha256: String,
    signature_key_id: String,
    trust_decision: String,
    source: InstalledSkillSource,
    #[serde(default)]
    missing_secrets: Vec<MissingSkillSecret>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillSource {
    kind: String,
    reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MissingSkillSecret {
    scope: String,
    key: String,
}

#[derive(Debug, Clone, Serialize)]
struct SkillRuntimeStatusSnapshot {
    status: String,
    source: String,
    quarantine_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detected_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operator_principal: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SkillRequirementsSnapshot {
    required_protocol_major: u32,
    min_palyra_version: String,
}

#[derive(Debug, Clone, Serialize)]
struct SkillEligibilitySnapshot {
    status: String,
    eligible: bool,
    reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SkillInventoryEntry {
    #[serde(flatten)]
    record: InstalledSkillRecord,
    install_state: String,
    skill_name: String,
    tool_count: usize,
    runtime_status: SkillRuntimeStatusSnapshot,
    requirements: SkillRequirementsSnapshot,
    eligibility: SkillEligibilitySnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct SkillInfoOutput {
    inventory: SkillInventoryEntry,
    manifest: SkillManifest,
    signature: SkillArtifactSignature,
    artifact_entries: Vec<String>,
    cached_artifact_path: String,
}

#[derive(Debug, Clone, Serialize)]
struct SkillCheckResult {
    inventory: SkillInventoryEntry,
    check_status: String,
    trust_accepted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    trust_error: Option<String>,
    audit_passed: bool,
    quarantine_required: bool,
    failed_checks: usize,
    warning_checks: usize,
    reasons: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification: Option<SkillVerificationReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    audit: Option<SkillSecurityAuditReport>,
}

#[derive(Debug, Clone, Serialize)]
struct SkillsInventorySnapshot {
    skills_root: String,
    installed_total: usize,
    current_total: usize,
    eligible_total: usize,
    quarantined_total: usize,
    disabled_total: usize,
    runtime_unknown_total: usize,
    missing_secrets_total: usize,
    publishers: Vec<String>,
    trust_decisions: BTreeMap<String, usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillInstallMetadata {
    schema_version: u32,
    installed_at_unix_ms: i64,
    source: InstalledSkillSource,
    artifact_sha256: String,
    payload_sha256: String,
    publisher: String,
    signature_key_id: String,
    trust_decision: String,
    #[serde(default)]
    missing_secrets: Vec<MissingSkillSecret>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillRegistryIndex {
    schema_version: u32,
    generated_at_unix_ms: i64,
    #[serde(default)]
    entries: Vec<SkillRegistryEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_page: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillRegistryEntry {
    skill_id: String,
    version: String,
    publisher: String,
    artifact: String,
    artifact_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    artifact_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignedSkillRegistryIndex {
    schema_version: u32,
    index: SkillRegistryIndex,
    signature: RegistrySignature,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrySignature {
    algorithm: String,
    publisher: String,
    key_id: String,
    public_key_base64: String,
    payload_sha256: String,
    signature_base64: String,
    signed_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
struct ResolvedRegistryArtifact {
    entry: SkillRegistryEntry,
    artifact_bytes: Vec<u8>,
    source: InstalledSkillSource,
}

#[derive(Debug, Clone)]
struct RemoteRegistryResolvedEntry {
    entry: SkillRegistryEntry,
    artifact_url: Url,
}

fn trust_decision_label(decision: TrustDecision) -> &'static str {
    match decision {
        TrustDecision::Allowlisted => "allowlisted",
        TrustDecision::TofuPinned => "tofu_pinned",
        TrustDecision::TofuNewlyPinned => "tofu_newly_pinned",
    }
}

fn sha256_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push_str(format!("{byte:02x}").as_str());
    }
    output
}

pub(crate) fn redacted_presence_for_output(present: bool) -> String {
    if present {
        REDACTED.to_owned()
    } else {
        "none".to_owned()
    }
}

pub(crate) fn redacted_presence_json_value(present: bool) -> Value {
    if present {
        Value::String(REDACTED.to_owned())
    } else {
        Value::Null
    }
}

pub(crate) fn redacted_identifier_for_output(value: &str) -> String {
    redacted_presence_for_output(!value.trim().is_empty())
}

pub(crate) fn redacted_optional_identifier_for_output(value: Option<&str>) -> String {
    redacted_presence_for_output(value.is_some_and(|candidate| !candidate.trim().is_empty()))
}

pub(crate) fn redacted_identifier_json_value(value: Option<&str>) -> Value {
    redacted_presence_json_value(value.is_some_and(|candidate| !candidate.trim().is_empty()))
}

fn parse_semver_triplet(raw: &str) -> Option<(u32, u32, u32)> {
    let parts = raw.trim().split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let major = parts[0].parse::<u32>().ok()?;
    let minor = parts[1].parse::<u32>().ok()?;
    let patch = parts[2].parse::<u32>().ok()?;
    Some((major, minor, patch))
}

fn compare_semver_versions(left: &str, right: &str) -> std::cmp::Ordering {
    match (parse_semver_triplet(left), parse_semver_triplet(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => left.cmp(right),
    }
}

fn unix_now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn resolve_skills_status_version(
    skills_root: &Path,
    skill_id: &str,
    version: Option<&str>,
) -> Result<String> {
    if let Some(version) = version {
        if version.trim().is_empty() {
            anyhow::bail!("--version cannot be empty");
        }
        return Ok(version.trim().to_owned());
    }
    let index = load_installed_skills_index(skills_root)?;
    let position = find_installed_skill_record(&index, skill_id, None)?;
    Ok(index.entries[position].version.clone())
}

fn resolve_skills_root(raw: Option<&str>) -> Result<PathBuf> {
    if let Some(raw) = raw {
        if raw.trim().is_empty() {
            anyhow::bail!("--skills-dir path cannot be empty");
        }
        return Ok(PathBuf::from(raw));
    }
    if let Some(context) = app::current_root_context() {
        return Ok(context.state_root().join("skills"));
    }
    let identity_root =
        default_identity_store_root().context("failed to resolve default identity store root")?;
    let state_root =
        identity_root.parent().map(Path::to_path_buf).unwrap_or_else(|| identity_root.clone());
    Ok(state_root.join("skills"))
}

fn skill_install_state_label(record: &InstalledSkillRecord) -> String {
    if record.current {
        "installed_current".to_owned()
    } else {
        "installed_superseded".to_owned()
    }
}

fn load_skill_runtime_status_snapshot(skill_id: &str, version: &str) -> SkillRuntimeStatusSnapshot {
    let journal_path = match resolve_daemon_journal_db_path(None) {
        Ok(path) => path,
        Err(_) => {
            return SkillRuntimeStatusSnapshot {
                status: "unknown".to_owned(),
                source: "unknown".to_owned(),
                quarantine_status: "unknown".to_owned(),
                reason: None,
                detected_at_ms: None,
                operator_principal: None,
            };
        }
    };

    if !journal_path.exists() {
        return SkillRuntimeStatusSnapshot {
            status: "unknown".to_owned(),
            source: "unknown".to_owned(),
            quarantine_status: "unknown".to_owned(),
            reason: None,
            detected_at_ms: None,
            operator_principal: None,
        };
    }

    let connection = match Connection::open(journal_path.as_path()) {
        Ok(connection) => connection,
        Err(_) => {
            return SkillRuntimeStatusSnapshot {
                status: "unknown".to_owned(),
                source: "unknown".to_owned(),
                quarantine_status: "unknown".to_owned(),
                reason: None,
                detected_at_ms: None,
                operator_principal: None,
            };
        }
    };

    let query = connection.query_row(
        r#"
            SELECT
                status,
                reason,
                detected_at_ms,
                operator_principal
            FROM skill_status
            WHERE lower(skill_id) = lower(?1)
              AND version = ?2
            LIMIT 1
        "#,
        rusqlite::params![skill_id, version],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        },
    );

    match query.optional() {
        Ok(Some((status, reason, detected_at_ms, operator_principal))) => {
            let quarantine_status =
                if status == "quarantined" { "quarantined" } else { "not_quarantined" };
            SkillRuntimeStatusSnapshot {
                status,
                source: "journal".to_owned(),
                quarantine_status: quarantine_status.to_owned(),
                reason,
                detected_at_ms: Some(detected_at_ms),
                operator_principal: Some(operator_principal),
            }
        }
        Ok(None) => SkillRuntimeStatusSnapshot {
            status: "active".to_owned(),
            source: "default".to_owned(),
            quarantine_status: "not_quarantined".to_owned(),
            reason: None,
            detected_at_ms: None,
            operator_principal: None,
        },
        Err(_) => SkillRuntimeStatusSnapshot {
            status: "unknown".to_owned(),
            source: "unknown".to_owned(),
            quarantine_status: "unknown".to_owned(),
            reason: None,
            detected_at_ms: None,
            operator_principal: None,
        },
    }
}

fn build_skill_eligibility_snapshot(
    _record: &InstalledSkillRecord,
    requirements: &SkillRequirementsSnapshot,
    runtime_status: &SkillRuntimeStatusSnapshot,
) -> SkillEligibilitySnapshot {
    let mut reasons = Vec::new();

    if requirements.required_protocol_major > CANONICAL_PROTOCOL_MAJOR {
        reasons.push(format!(
            "requires protocol v{} but runtime exposes v{}",
            requirements.required_protocol_major, CANONICAL_PROTOCOL_MAJOR
        ));
    }
    if compare_semver_versions(requirements.min_palyra_version.as_str(), env!("CARGO_PKG_VERSION"))
        .is_gt()
    {
        reasons.push(format!(
            "requires palyra >= {} but current build is {}",
            requirements.min_palyra_version,
            env!("CARGO_PKG_VERSION")
        ));
    }
    match runtime_status.status.as_str() {
        "quarantined" => reasons.push("skill is quarantined".to_owned()),
        "disabled" => reasons.push("skill is disabled".to_owned()),
        _ => {}
    }

    if !reasons.is_empty() {
        return SkillEligibilitySnapshot { status: "blocked".to_owned(), eligible: false, reasons };
    }

    if runtime_status.status == "unknown" {
        return SkillEligibilitySnapshot {
            status: "unknown".to_owned(),
            eligible: false,
            reasons: vec!["runtime status unavailable".to_owned()],
        };
    }

    SkillEligibilitySnapshot { status: "eligible".to_owned(), eligible: true, reasons }
}

fn artifact_path_for_installed_skill(skills_root: &Path, record: &InstalledSkillRecord) -> PathBuf {
    skills_root
        .join(record.skill_id.as_str())
        .join(record.version.as_str())
        .join(SKILLS_ARTIFACT_FILE_NAME)
}

fn build_skill_inventory_entry(
    skills_root: &Path,
    record: &InstalledSkillRecord,
) -> Result<SkillInventoryEntry> {
    let artifact_path = artifact_path_for_installed_skill(skills_root, record);
    let artifact_bytes = fs::read(artifact_path.as_path()).with_context(|| {
        format!("failed to read installed artifact {}", artifact_path.display())
    })?;
    let inspection = inspect_skill_artifact(artifact_bytes.as_slice()).with_context(|| {
        format!("failed to inspect installed artifact {}", artifact_path.display())
    })?;
    let requirements = SkillRequirementsSnapshot {
        required_protocol_major: inspection.manifest.compat.required_protocol_major,
        min_palyra_version: inspection.manifest.compat.min_palyra_version.clone(),
    };
    let runtime_status =
        load_skill_runtime_status_snapshot(record.skill_id.as_str(), record.version.as_str());
    let eligibility = build_skill_eligibility_snapshot(record, &requirements, &runtime_status);

    Ok(SkillInventoryEntry {
        record: record.clone(),
        install_state: skill_install_state_label(record),
        skill_name: inspection.manifest.name,
        tool_count: inspection.manifest.entrypoints.tools.len(),
        runtime_status,
        requirements,
        eligibility,
    })
}

fn collect_installed_skill_inventory(skills_root: &Path) -> Result<Vec<SkillInventoryEntry>> {
    let mut index = load_installed_skills_index(skills_root)?;
    normalize_installed_skills_index(&mut index);
    index.entries.iter().map(|record| build_skill_inventory_entry(skills_root, record)).collect()
}

fn build_skills_inventory_snapshot_for_root(skills_root: &Path) -> Result<SkillsInventorySnapshot> {
    let entries = collect_installed_skill_inventory(skills_root)?;
    let mut publishers =
        entries.iter().map(|entry| entry.record.publisher.clone()).collect::<Vec<_>>();
    publishers.sort();
    publishers.dedup();

    let mut trust_decisions = BTreeMap::new();
    for entry in &entries {
        *trust_decisions.entry(entry.record.trust_decision.clone()).or_insert(0) += 1;
    }

    Ok(SkillsInventorySnapshot {
        skills_root: skills_root.display().to_string(),
        installed_total: entries.len(),
        current_total: entries.iter().filter(|entry| entry.record.current).count(),
        eligible_total: entries.iter().filter(|entry| entry.eligibility.eligible).count(),
        quarantined_total: entries
            .iter()
            .filter(|entry| entry.runtime_status.status == "quarantined")
            .count(),
        disabled_total: entries
            .iter()
            .filter(|entry| entry.runtime_status.status == "disabled")
            .count(),
        runtime_unknown_total: entries
            .iter()
            .filter(|entry| entry.runtime_status.status == "unknown")
            .count(),
        missing_secrets_total: entries
            .iter()
            .filter(|entry| !entry.record.missing_secrets.is_empty())
            .count(),
        publishers,
        trust_decisions,
        error: None,
    })
}

fn build_default_skills_inventory_snapshot() -> SkillsInventorySnapshot {
    match resolve_skills_root(None) {
        Ok(skills_root) => match build_skills_inventory_snapshot_for_root(skills_root.as_path()) {
            Ok(snapshot) => snapshot,
            Err(error) => SkillsInventorySnapshot {
                skills_root: skills_root.display().to_string(),
                installed_total: 0,
                current_total: 0,
                eligible_total: 0,
                quarantined_total: 0,
                disabled_total: 0,
                runtime_unknown_total: 0,
                missing_secrets_total: 0,
                publishers: Vec::new(),
                trust_decisions: BTreeMap::new(),
                error: Some(error.to_string()),
            },
        },
        Err(error) => SkillsInventorySnapshot {
            skills_root: "unavailable".to_owned(),
            installed_total: 0,
            current_total: 0,
            eligible_total: 0,
            quarantined_total: 0,
            disabled_total: 0,
            runtime_unknown_total: 0,
            missing_secrets_total: 0,
            publishers: Vec::new(),
            trust_decisions: BTreeMap::new(),
            error: Some(error.to_string()),
        },
    }
}

fn load_installed_skills_index(skills_root: &Path) -> Result<InstalledSkillsIndex> {
    let index_path = skills_root.join(SKILLS_INDEX_FILE_NAME);
    if !index_path.exists() {
        return Ok(InstalledSkillsIndex::default());
    }
    let payload = fs::read(index_path.as_path()).with_context(|| {
        format!("failed to read installed skills index {}", index_path.display())
    })?;
    let mut index: InstalledSkillsIndex =
        serde_json::from_slice(payload.as_slice()).with_context(|| {
            format!("failed to parse installed skills index {}", index_path.display())
        })?;
    if index.schema_version != SKILLS_LAYOUT_VERSION {
        anyhow::bail!(
            "unsupported installed skills index schema version {}; expected {}",
            index.schema_version,
            SKILLS_LAYOUT_VERSION
        );
    }
    normalize_installed_skills_index(&mut index);
    Ok(index)
}

fn save_installed_skills_index(skills_root: &Path, index: &InstalledSkillsIndex) -> Result<()> {
    let mut normalized = index.clone();
    normalized.schema_version = SKILLS_LAYOUT_VERSION;
    normalized.updated_at_unix_ms = unix_now_ms();
    normalize_installed_skills_index(&mut normalized);
    let payload = serde_json::to_vec_pretty(&normalized)
        .context("failed to serialize installed skills index")?;
    write_file_atomically(skills_root.join(SKILLS_INDEX_FILE_NAME).as_path(), payload.as_slice())
}

fn normalize_installed_skills_index(index: &mut InstalledSkillsIndex) {
    index.entries.sort_by(|left, right| {
        left.skill_id
            .cmp(&right.skill_id)
            .then_with(|| compare_semver_versions(left.version.as_str(), right.version.as_str()))
    });

    let mut skill_ids =
        index.entries.iter().map(|entry| entry.skill_id.clone()).collect::<Vec<_>>();
    skill_ids.sort();
    skill_ids.dedup();
    for skill_id in skill_ids {
        let mut positions = index
            .entries
            .iter()
            .enumerate()
            .filter_map(|(position, entry)| (entry.skill_id == skill_id).then_some(position))
            .collect::<Vec<_>>();
        if positions.is_empty() {
            continue;
        }
        let current_positions = positions
            .iter()
            .copied()
            .filter(|position| index.entries[*position].current)
            .collect::<Vec<_>>();
        if current_positions.len() == 1 {
            continue;
        }
        positions.sort_by(|left, right| {
            compare_semver_versions(
                index.entries[*left].version.as_str(),
                index.entries[*right].version.as_str(),
            )
        });
        for position in &positions {
            index.entries[*position].current = false;
        }
        if let Some(position) = positions.last() {
            index.entries[*position].current = true;
        }
    }
}

fn find_installed_skill_record(
    index: &InstalledSkillsIndex,
    skill_id: &str,
    version: Option<&str>,
) -> Result<usize> {
    if let Some(version) = version {
        return index
            .entries
            .iter()
            .position(|entry| entry.skill_id == skill_id && entry.version == version)
            .ok_or_else(|| anyhow!("skill {} version {} is not installed", skill_id, version));
    }
    index
        .entries
        .iter()
        .position(|entry| entry.skill_id == skill_id && entry.current)
        .ok_or_else(|| anyhow!("skill {} has no current installed version", skill_id))
}

fn append_skills_audit_event(
    skills_root: &Path,
    event_kind: &str,
    payload: serde_json::Value,
) -> Result<()> {
    let audit_path = skills_root.join(SKILLS_AUDIT_FILE_NAME);
    if let Some(parent) = audit_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create skills audit directory {}", parent.display())
        })?;
    }
    let event = json!({
        "event_kind": event_kind,
        "timestamp_unix_ms": unix_now_ms(),
        "payload": payload
    });
    let line = serde_json::to_string(&event).context("failed to serialize skills audit event")?;
    let mut file =
        fs::OpenOptions::new().create(true).append(true).open(audit_path.as_path()).with_context(
            || format!("failed to open skills audit file {}", audit_path.display()),
        )?;
    file.write_all(line.as_bytes()).context("failed to write skills audit event")?;
    file.write_all(b"\n").context("failed to terminate skills audit event line")?;
    Ok(())
}

fn write_file_atomically(path: &Path, payload: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }
    let temporary_path = path.with_extension(format!("tmp.{}.{}", std::process::id(), Ulid::new()));
    fs::write(temporary_path.as_path(), payload)
        .with_context(|| format!("failed to write temporary file {}", temporary_path.display()))?;

    #[cfg(not(windows))]
    {
        fs::rename(temporary_path.as_path(), path).with_context(|| {
            format!(
                "failed to atomically move temporary file {} into {}",
                temporary_path.display(),
                path.display()
            )
        })?;
        Ok(())
    }

    #[cfg(windows)]
    {
        if !path.exists() {
            fs::rename(temporary_path.as_path(), path).with_context(|| {
                format!(
                    "failed to atomically move temporary file {} into {}",
                    temporary_path.display(),
                    path.display()
                )
            })?;
            return Ok(());
        }

        let backup_path =
            path.with_extension(format!("bak.{}.{}", std::process::id(), Ulid::new()));
        fs::rename(path, backup_path.as_path()).with_context(|| {
            format!(
                "failed to stage original file {} into backup {}",
                path.display(),
                backup_path.display()
            )
        })?;

        match fs::rename(temporary_path.as_path(), path) {
            Ok(()) => fs::remove_file(backup_path.as_path()).with_context(|| {
                format!("failed to remove temporary backup file {}", backup_path.display())
            }),
            Err(replace_error) => {
                fs::rename(backup_path.as_path(), path).with_context(|| {
                    format!(
                        "failed to restore original file {} from backup {} after replacement error {}",
                        path.display(),
                        backup_path.display(),
                        replace_error
                    )
                })?;
                anyhow::bail!(
                    "failed to atomically move temporary file {} into {}: {}",
                    temporary_path.display(),
                    path.display(),
                    replace_error
                );
            }
        }
    }
}

struct InstallExecutionOutcome {
    record: InstalledSkillRecord,
    previous_current_version: Option<String>,
}

struct InstallMetadataContext {
    source: InstalledSkillSource,
    artifact_sha256: String,
    missing_secrets: Vec<MissingSkillSecret>,
}

fn install_verified_skill_artifact(
    skills_root: &Path,
    index: &mut InstalledSkillsIndex,
    artifact_bytes: &[u8],
    inspected: &palyra_skills::SkillArtifactInspection,
    verification_report: &palyra_skills::SkillVerificationReport,
    install_context: InstallMetadataContext,
) -> Result<InstallExecutionOutcome> {
    let skill_id = inspected.manifest.skill_id.as_str();
    let version = inspected.manifest.version.as_str();
    let skill_root = skills_root.join(skill_id);
    let final_dir = skill_root.join(version);
    if final_dir.exists() {
        anyhow::bail!(
            "skill {} version {} is already installed at {}",
            skill_id,
            version,
            final_dir.display()
        );
    }
    fs::create_dir_all(skill_root.as_path())
        .with_context(|| format!("failed to create skill root {}", skill_root.display()))?;

    let staging = skill_root.join(format!(".tmp-install-{}", Ulid::new()));
    fs::create_dir_all(staging.as_path()).with_context(|| {
        format!("failed to create skill staging directory {}", staging.display())
    })?;
    extract_inspected_artifact_entries(staging.as_path(), &inspected.entries)?;
    assert_directory_matches_expected_entries(staging.as_path(), &inspected.entries)?;

    let metadata = SkillInstallMetadata {
        schema_version: SKILLS_LAYOUT_VERSION,
        installed_at_unix_ms: unix_now_ms(),
        source: install_context.source.clone(),
        artifact_sha256: install_context.artifact_sha256.clone(),
        payload_sha256: verification_report.payload_sha256.clone(),
        publisher: verification_report.manifest.publisher.clone(),
        signature_key_id: inspected.signature.key_id.clone(),
        trust_decision: trust_decision_label(verification_report.trust_decision).to_owned(),
        missing_secrets: install_context.missing_secrets.clone(),
    };
    let metadata_payload =
        serde_json::to_vec_pretty(&metadata).context("failed to serialize install metadata")?;
    fs::write(staging.join(SKILLS_INSTALL_METADATA_FILE_NAME), metadata_payload.as_slice())
        .with_context(|| format!("failed to write install metadata in {}", staging.display()))?;
    fs::write(staging.join(SKILLS_ARTIFACT_FILE_NAME), artifact_bytes)
        .with_context(|| format!("failed to write artifact cache in {}", staging.display()))?;

    fs::rename(staging.as_path(), final_dir.as_path()).with_context(|| {
        format!(
            "failed to atomically promote staged install from {} to {}",
            staging.display(),
            final_dir.display()
        )
    })?;

    let previous_current_version = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.current)
        .map(|entry| entry.version.clone());
    for entry in &mut index.entries {
        if entry.skill_id == skill_id {
            entry.current = false;
        }
    }
    let record = InstalledSkillRecord {
        skill_id: skill_id.to_owned(),
        version: version.to_owned(),
        publisher: verification_report.manifest.publisher.clone(),
        current: true,
        installed_at_unix_ms: unix_now_ms(),
        artifact_sha256: install_context.artifact_sha256,
        payload_sha256: verification_report.payload_sha256.clone(),
        signature_key_id: inspected.signature.key_id.clone(),
        trust_decision: trust_decision_label(verification_report.trust_decision).to_owned(),
        source: install_context.source,
        missing_secrets: install_context.missing_secrets,
    };
    index.entries.push(record.clone());
    normalize_installed_skills_index(index);
    if let Err(error) = update_skill_current_pointer(skill_root.as_path(), version) {
        eprintln!(
            "warning: failed to update optional '{}' pointer for skill {}: {}",
            SKILLS_CURRENT_LINK_NAME, skill_id, error
        );
    }
    Ok(InstallExecutionOutcome { record, previous_current_version })
}

fn extract_inspected_artifact_entries(
    destination: &Path,
    entries: &std::collections::BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    for (entry_path, payload) in entries {
        let target = safe_join_relative_path(destination, entry_path.as_str())?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create artifact directory {}", parent.display())
            })?;
        }
        fs::write(target.as_path(), payload.as_slice()).with_context(|| {
            format!("failed to write extracted artifact file {}", target.display())
        })?;
    }
    Ok(())
}

fn safe_join_relative_path(base: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative.trim());
    if relative.trim().is_empty() {
        anyhow::bail!("artifact relative path cannot be empty");
    }
    if path.is_absolute() {
        anyhow::bail!("artifact relative path cannot be absolute: {}", relative);
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::Prefix(_) | Component::RootDir | Component::CurDir | Component::ParentDir
        )
    }) {
        anyhow::bail!("artifact relative path is invalid: {}", relative);
    }
    Ok(base.join(path))
}

fn assert_directory_matches_expected_entries(
    root: &Path,
    entries: &std::collections::BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    let expected = entries.keys().cloned().collect::<HashSet<_>>();
    let observed = collect_relative_files(root, root)?;
    if expected != observed {
        anyhow::bail!(
            "extracted artifact tree mismatch: expected {} files, found {} files",
            expected.len(),
            observed.len()
        );
    }
    Ok(())
}

fn collect_relative_files(root: &Path, cursor: &Path) -> Result<HashSet<String>> {
    let mut files = HashSet::new();
    for entry in fs::read_dir(cursor)
        .with_context(|| format!("failed to read directory {}", cursor.display()))?
    {
        let entry = entry.context("failed to read directory entry")?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(collect_relative_files(root, path.as_path())?);
            continue;
        }
        let relative = path.strip_prefix(root).with_context(|| {
            format!("failed to compute relative extracted path {}", path.display())
        })?;
        files.insert(normalize_relative_registry_path(relative)?);
    }
    Ok(files)
}

fn resolve_install_artifact(
    command: &SkillsInstallCommand,
    trust_store: &mut SkillTrustStore,
    allow_untrusted: bool,
) -> Result<ResolvedRegistryArtifact> {
    let use_artifact = command.artifact.is_some();
    let use_registry = command.registry_dir.is_some() || command.registry_url.is_some();
    if use_artifact == use_registry {
        anyhow::bail!(
            "skills install requires either --artifact or a registry source (--registry-dir / --registry-url)"
        );
    }
    if let Some(artifact) = command.artifact.as_deref() {
        let artifact_path = Path::new(artifact);
        let artifact_bytes = fs::read(artifact_path).with_context(|| {
            format!("failed to read skill artifact {}", artifact_path.display())
        })?;
        let inspected = inspect_skill_artifact(artifact_bytes.as_slice())
            .context("skill artifact failed structural verification")?;
        return Ok(ResolvedRegistryArtifact {
            entry: SkillRegistryEntry {
                skill_id: inspected.manifest.skill_id,
                version: inspected.manifest.version,
                publisher: inspected.manifest.publisher,
                artifact: artifact_path.to_string_lossy().into_owned(),
                artifact_sha256: sha256_hex(artifact_bytes.as_slice()),
                artifact_bytes: Some(u64::try_from(artifact_bytes.len()).unwrap_or(u64::MAX)),
            },
            artifact_bytes,
            source: InstalledSkillSource {
                kind: "local_artifact".to_owned(),
                reference: artifact_path.to_string_lossy().into_owned(),
            },
        });
    }
    let skill_id = command
        .skill_id
        .as_deref()
        .ok_or_else(|| anyhow!("skills install from registry requires --skill-id"))?;
    resolve_registry_artifact_for_skill(
        command.registry_dir.as_deref(),
        command.registry_url.as_deref(),
        command.registry_ca_cert.as_deref(),
        skill_id,
        command.version.as_deref(),
        trust_store,
        allow_untrusted,
    )
}

fn resolve_registry_artifact_for_skill(
    registry_dir: Option<&str>,
    registry_url: Option<&str>,
    registry_ca_cert: Option<&str>,
    skill_id: &str,
    version: Option<&str>,
    trust_store: &mut SkillTrustStore,
    allow_untrusted: bool,
) -> Result<ResolvedRegistryArtifact> {
    if registry_dir.is_some() == registry_url.is_some() {
        anyhow::bail!("registry source must be exactly one of --registry-dir or --registry-url");
    }
    if let Some(registry_dir) = registry_dir {
        let root = PathBuf::from(registry_dir);
        if !root.is_dir() {
            anyhow::bail!("registry directory does not exist: {}", root.display());
        }
        let index = build_local_registry_index(root.as_path())?;
        persist_local_registry_index(root.as_path(), &index)?;
        let entry = select_registry_entry(index.entries.as_slice(), skill_id, version)?;
        let artifact_path =
            resolve_local_registry_artifact_path(root.as_path(), entry.artifact.as_str())?;
        let artifact_bytes = fs::read(artifact_path.as_path()).with_context(|| {
            format!("failed to read registry artifact {}", artifact_path.display())
        })?;
        return Ok(ResolvedRegistryArtifact {
            entry,
            artifact_bytes,
            source: InstalledSkillSource {
                kind: "local_registry".to_owned(),
                reference: artifact_path.to_string_lossy().into_owned(),
            },
        });
    }

    let remote_entries = fetch_remote_registry_entries(
        registry_url.expect("checked"),
        registry_ca_cert,
        trust_store,
        allow_untrusted,
    )?;
    let selected = select_remote_registry_entry(remote_entries.as_slice(), skill_id, version)?;
    let client = build_registry_http_client(registry_ca_cert)?;
    let artifact_bytes =
        fetch_limited_bytes(&client, selected.artifact_url.as_str(), MAX_REGISTRY_INDEX_BYTES * 32)
            .with_context(|| format!("failed to fetch artifact {}", selected.artifact_url))?;
    Ok(ResolvedRegistryArtifact {
        entry: selected.entry,
        artifact_bytes,
        source: InstalledSkillSource {
            kind: "remote_registry".to_owned(),
            reference: selected.artifact_url.to_string(),
        },
    })
}

fn build_local_registry_index(registry_dir: &Path) -> Result<SkillRegistryIndex> {
    let mut artifact_paths = Vec::new();
    let mut visited_dirs = HashSet::<PathBuf>::new();
    collect_skill_artifact_paths(
        registry_dir,
        registry_dir,
        &mut artifact_paths,
        &mut visited_dirs,
    )?;
    let mut entries = Vec::new();
    for artifact_path in artifact_paths {
        let artifact_bytes = fs::read(artifact_path.as_path()).with_context(|| {
            format!("failed to read local registry artifact {}", artifact_path.display())
        })?;
        let inspected = inspect_skill_artifact(artifact_bytes.as_slice()).with_context(|| {
            format!(
                "artifact {} failed verification and cannot be indexed",
                artifact_path.display()
            )
        })?;
        let relative = artifact_path.strip_prefix(registry_dir).with_context(|| {
            format!("failed to compute registry-relative path for {}", artifact_path.display())
        })?;
        entries.push(SkillRegistryEntry {
            skill_id: inspected.manifest.skill_id,
            version: inspected.manifest.version,
            publisher: inspected.manifest.publisher,
            artifact: normalize_relative_registry_path(relative)?,
            artifact_sha256: sha256_hex(artifact_bytes.as_slice()),
            artifact_bytes: Some(u64::try_from(artifact_bytes.len()).unwrap_or(u64::MAX)),
        });
    }
    entries.sort_by(|left, right| {
        left.skill_id
            .cmp(&right.skill_id)
            .then_with(|| compare_semver_versions(left.version.as_str(), right.version.as_str()))
    });
    let index = SkillRegistryIndex {
        schema_version: REGISTRY_INDEX_SCHEMA_VERSION,
        generated_at_unix_ms: unix_now_ms(),
        entries,
        next_page: None,
    };
    validate_registry_index(&index)?;
    Ok(index)
}

fn persist_local_registry_index(registry_dir: &Path, index: &SkillRegistryIndex) -> Result<()> {
    let payload =
        serde_json::to_vec_pretty(index).context("failed to serialize local registry index")?;
    write_file_atomically(registry_dir.join(REGISTRY_INDEX_FILE_NAME).as_path(), payload.as_slice())
}

fn collect_skill_artifact_paths(
    root: &Path,
    cursor: &Path,
    output: &mut Vec<PathBuf>,
    visited_dirs: &mut HashSet<PathBuf>,
) -> Result<()> {
    let canonical_cursor = fs::canonicalize(cursor)
        .with_context(|| format!("failed to canonicalize directory {}", cursor.display()))?;
    if !visited_dirs.insert(canonical_cursor) {
        return Ok(());
    }
    for entry in fs::read_dir(cursor)
        .with_context(|| format!("failed to read directory {}", cursor.display()))?
    {
        let entry = entry.context("failed to read directory entry")?;
        let file_type = entry.file_type().context("failed to read directory entry file type")?;
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            collect_skill_artifact_paths(root, path.as_path(), output, visited_dirs)?;
            continue;
        }
        if path
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| value.eq_ignore_ascii_case("palyra-skill"))
        {
            let relative = path.strip_prefix(root).with_context(|| {
                format!("artifact path {} escaped registry root", path.display())
            })?;
            if relative.components().any(|component| matches!(component, Component::ParentDir)) {
                anyhow::bail!("artifact path escapes registry root: {}", path.display());
            }
            output.push(path);
        }
    }
    Ok(())
}

fn normalize_relative_registry_path(path: &Path) -> Result<String> {
    if path.components().any(|component| {
        matches!(
            component,
            Component::Prefix(_) | Component::RootDir | Component::CurDir | Component::ParentDir
        )
    }) {
        anyhow::bail!("registry artifact path is invalid: {}", path.display());
    }
    let mut segments = Vec::new();
    for component in path.components() {
        let Component::Normal(segment) = component else {
            anyhow::bail!("registry artifact path is invalid: {}", path.display());
        };
        let raw = segment.to_string_lossy();
        if raw.is_empty() {
            anyhow::bail!("registry artifact path is invalid: {}", path.display());
        }
        segments.push(raw.to_string());
    }
    if segments.is_empty() {
        anyhow::bail!("registry artifact path cannot be empty");
    }
    Ok(segments.join("/"))
}

fn resolve_local_registry_artifact_path(registry_dir: &Path, raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw.trim());
    if raw.trim().is_empty() {
        anyhow::bail!("registry entry artifact path cannot be empty");
    }
    if path.is_absolute() {
        anyhow::bail!("registry entry artifact path must be relative: {}", raw.trim());
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::Prefix(_) | Component::RootDir | Component::CurDir | Component::ParentDir
        )
    }) {
        anyhow::bail!("registry entry artifact path is invalid: {}", raw.trim());
    }
    Ok(registry_dir.join(path))
}

fn fetch_remote_registry_entries(
    registry_url: &str,
    registry_ca_cert: Option<&str>,
    trust_store: &mut SkillTrustStore,
    allow_untrusted: bool,
) -> Result<Vec<RemoteRegistryResolvedEntry>> {
    let client = build_registry_http_client(registry_ca_cert)?;
    fetch_remote_registry_entries_with_fetcher(
        registry_url,
        trust_store,
        allow_untrusted,
        |page_url| fetch_limited_bytes(&client, page_url.as_str(), MAX_REGISTRY_INDEX_BYTES),
    )
}

fn fetch_remote_registry_entries_with_fetcher<F>(
    registry_url: &str,
    trust_store: &mut SkillTrustStore,
    allow_untrusted: bool,
    mut fetch_payload: F,
) -> Result<Vec<RemoteRegistryResolvedEntry>>
where
    F: FnMut(&Url) -> Result<Vec<u8>>,
{
    let mut page_url = parse_https_url(registry_url, "--registry-url")?;
    let registry_origin = page_url.clone();
    let mut visited_pages = HashSet::<String>::new();
    let mut merged = Vec::<RemoteRegistryResolvedEntry>::new();
    for _ in 0..MAX_REGISTRY_PAGES {
        if !visited_pages.insert(page_url.to_string()) {
            anyhow::bail!("remote registry pagination loop detected at {}", page_url);
        }
        let payload = fetch_payload(&page_url)
            .with_context(|| format!("failed to fetch remote registry index {}", page_url))?;
        let index = parse_and_verify_signed_remote_registry_index(
            payload.as_slice(),
            trust_store,
            allow_untrusted,
        )
        .with_context(|| format!("invalid remote registry index {}", page_url))?;
        validate_registry_index(&index)?;
        for entry in index.entries {
            let artifact_url = page_url.join(entry.artifact.as_str()).with_context(|| {
                format!("failed to resolve artifact URL '{}' against {}", entry.artifact, page_url)
            })?;
            if artifact_url.scheme() != "https" {
                anyhow::bail!("remote registry artifact URL must use https: {}", artifact_url);
            }
            ensure_remote_registry_same_origin(&registry_origin, &artifact_url, "artifact URL")?;
            merged.push(RemoteRegistryResolvedEntry { entry, artifact_url });
        }
        let Some(next_page) = index.next_page else {
            return Ok(merged);
        };
        page_url = page_url.join(next_page.as_str()).with_context(|| {
            format!("failed to resolve next_page '{}' against {}", next_page, page_url)
        })?;
        if page_url.scheme() != "https" {
            anyhow::bail!("remote registry next_page must use https: {}", page_url);
        }
        ensure_remote_registry_same_origin(&registry_origin, &page_url, "next_page URL")?;
    }
    anyhow::bail!("remote registry exceeded max pagination depth of {}", MAX_REGISTRY_PAGES)
}

fn ensure_remote_registry_same_origin(
    registry_origin: &Url,
    candidate: &Url,
    field_label: &str,
) -> Result<()> {
    let same_origin = registry_origin.scheme() == candidate.scheme()
        && registry_origin.host_str() == candidate.host_str()
        && registry_origin.port_or_known_default() == candidate.port_or_known_default();
    if !same_origin {
        anyhow::bail!(
            "remote registry {field_label} must stay on origin {}://{}:{} (resolved {})",
            registry_origin.scheme(),
            registry_origin.host_str().unwrap_or_default(),
            registry_origin.port_or_known_default().unwrap_or_default(),
            candidate
        );
    }
    Ok(())
}

fn parse_and_verify_signed_remote_registry_index(
    payload: &[u8],
    trust_store: &mut SkillTrustStore,
    allow_untrusted: bool,
) -> Result<SkillRegistryIndex> {
    if payload.len() > MAX_REGISTRY_INDEX_BYTES {
        anyhow::bail!(
            "remote registry index exceeds max size ({} > {})",
            payload.len(),
            MAX_REGISTRY_INDEX_BYTES
        );
    }
    let signed: SignedSkillRegistryIndex =
        serde_json::from_slice(payload).context("failed to parse signed registry index JSON")?;
    if signed.schema_version != REGISTRY_SIGNED_INDEX_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported signed registry schema version {}; expected {}",
            signed.schema_version,
            REGISTRY_SIGNED_INDEX_SCHEMA_VERSION
        );
    }
    if signed.index.schema_version != REGISTRY_INDEX_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported registry index schema version {}; expected {}",
            signed.index.schema_version,
            REGISTRY_INDEX_SCHEMA_VERSION
        );
    }
    if signed.signature.algorithm != REGISTRY_SIGNATURE_ALGORITHM {
        anyhow::bail!("unsupported registry signature algorithm '{}'", signed.signature.algorithm);
    }
    let verifying_key = parse_registry_verifying_key(&signed.signature)?;
    let expected_key_id = registry_key_id_for(&verifying_key);
    if signed.signature.key_id != expected_key_id {
        anyhow::bail!(
            "registry signature key_id mismatch: expected {} got {}",
            expected_key_id,
            signed.signature.key_id
        );
    }
    let payload_sha256 = sha256_hex(
        serde_json::to_vec(&signed.index)
            .context("failed to serialize canonical registry index")?
            .as_slice(),
    );
    if payload_sha256 != signed.signature.payload_sha256 {
        anyhow::bail!("registry index payload hash mismatch");
    }
    let signature = parse_registry_signature(&signed.signature)?;
    verifying_key
        .verify(payload_sha256.as_bytes(), &signature)
        .map_err(|_| anyhow!("registry signature verification failed"))?;

    let observed_key_hex = {
        let mut output = String::with_capacity(64);
        for byte in verifying_key.as_bytes() {
            output.push_str(format!("{byte:02x}").as_str());
        }
        output
    };
    let _ = evaluate_trust_for_key(
        trust_store,
        signed.signature.publisher.as_str(),
        observed_key_hex.as_str(),
        allow_untrusted,
        "remote registry signature",
    )?;
    Ok(signed.index)
}

fn parse_registry_verifying_key(signature: &RegistrySignature) -> Result<VerifyingKey> {
    let decoded = BASE64_STANDARD
        .decode(signature.public_key_base64.as_bytes())
        .map_err(|_| anyhow!("registry public key is not valid base64"))?;
    let key_bytes: [u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("registry public key must decode to 32 bytes"))?;
    VerifyingKey::from_bytes(&key_bytes).map_err(|_| anyhow!("registry public key is invalid"))
}

fn parse_registry_signature(signature: &RegistrySignature) -> Result<Signature> {
    let decoded = BASE64_STANDARD
        .decode(signature.signature_base64.as_bytes())
        .map_err(|_| anyhow!("registry signature is not valid base64"))?;
    let signature_bytes: [u8; 64] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("registry signature must decode to 64 bytes"))?;
    Ok(Signature::from_bytes(&signature_bytes))
}

fn registry_key_id_for(key: &VerifyingKey) -> String {
    let digest = Sha256::digest(key.as_bytes());
    let mut suffix = String::with_capacity(16);
    for byte in &digest[..8] {
        suffix.push_str(format!("{byte:02x}").as_str());
    }
    format!("ed25519:{suffix}")
}

fn evaluate_trust_for_key(
    trust_store: &mut SkillTrustStore,
    publisher: &str,
    observed_key_hex: &str,
    allow_untrusted: bool,
    context: &str,
) -> Result<TrustDecision> {
    let publisher = publisher.trim().to_ascii_lowercase();
    if publisher.is_empty() {
        anyhow::bail!("{context} publisher cannot be empty");
    }
    if let Some(keys) = trust_store.trusted_publishers.get(&publisher) {
        if keys.iter().any(|key| key == observed_key_hex) {
            return Ok(TrustDecision::Allowlisted);
        }
        anyhow::bail!("{context} trusted key mismatch for publisher '{}'", publisher);
    }
    if let Some(pinned) = trust_store.tofu_publishers.get(&publisher) {
        if pinned == observed_key_hex {
            return Ok(TrustDecision::TofuPinned);
        }
        anyhow::bail!("{context} TOFU key mismatch for publisher '{}'", publisher);
    }
    if allow_untrusted {
        trust_store.tofu_publishers.insert(publisher.to_owned(), observed_key_hex.to_owned());
        return Ok(TrustDecision::TofuNewlyPinned);
    }
    anyhow::bail!(
        "{context} publisher '{}' is untrusted (pass --allow-untrusted to permit TOFU pinning)",
        publisher
    )
}

fn validate_registry_index(index: &SkillRegistryIndex) -> Result<()> {
    if index.schema_version != REGISTRY_INDEX_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported registry index schema version {}; expected {}",
            index.schema_version,
            REGISTRY_INDEX_SCHEMA_VERSION
        );
    }
    if index.entries.len() > MAX_REGISTRY_ENTRIES {
        anyhow::bail!(
            "registry index contains too many entries ({} > {})",
            index.entries.len(),
            MAX_REGISTRY_ENTRIES
        );
    }
    let mut seen_skill_versions = HashSet::<(String, String)>::new();
    for entry in &index.entries {
        if entry.skill_id.trim().is_empty() {
            anyhow::bail!("registry entry skill_id cannot be empty");
        }
        if parse_semver_triplet(entry.version.as_str()).is_none() {
            anyhow::bail!(
                "registry entry {} has invalid semantic version '{}'",
                entry.skill_id,
                entry.version
            );
        }
        if entry.publisher.trim().is_empty() {
            anyhow::bail!(
                "registry entry {} {} publisher cannot be empty",
                entry.skill_id,
                entry.version
            );
        }
        if entry.artifact.trim().is_empty() {
            anyhow::bail!(
                "registry entry {} {} artifact URL/path cannot be empty",
                entry.skill_id,
                entry.version
            );
        }
        if entry.artifact_sha256.len() != 64
            || !entry.artifact_sha256.chars().all(|ch| ch.is_ascii_hexdigit())
        {
            anyhow::bail!(
                "registry entry {} {} has invalid artifact_sha256 '{}'",
                entry.skill_id,
                entry.version,
                entry.artifact_sha256
            );
        }
        if !seen_skill_versions.insert((entry.skill_id.clone(), entry.version.clone())) {
            anyhow::bail!(
                "registry contains duplicate entry for skill_id={} version={}",
                entry.skill_id,
                entry.version
            );
        }
    }
    Ok(())
}

fn select_registry_entry(
    entries: &[SkillRegistryEntry],
    skill_id: &str,
    version: Option<&str>,
) -> Result<SkillRegistryEntry> {
    let mut candidates =
        entries.iter().filter(|entry| entry.skill_id == skill_id).cloned().collect::<Vec<_>>();
    if candidates.is_empty() {
        anyhow::bail!("registry does not contain skill_id={}", skill_id);
    }
    if let Some(version) = version {
        return candidates
            .into_iter()
            .find(|entry| entry.version == version)
            .ok_or_else(|| anyhow!("registry does not contain {} version {}", skill_id, version));
    }
    candidates.sort_by(|left, right| {
        compare_semver_versions(left.version.as_str(), right.version.as_str())
    });
    candidates
        .into_iter()
        .last()
        .ok_or_else(|| anyhow!("registry does not contain installable versions for {}", skill_id))
}

fn select_remote_registry_entry(
    entries: &[RemoteRegistryResolvedEntry],
    skill_id: &str,
    version: Option<&str>,
) -> Result<RemoteRegistryResolvedEntry> {
    let mut candidates = entries
        .iter()
        .filter(|entry| entry.entry.skill_id == skill_id)
        .cloned()
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        anyhow::bail!("remote registry does not contain skill_id={}", skill_id);
    }
    if let Some(version) = version {
        return candidates.into_iter().find(|entry| entry.entry.version == version).ok_or_else(
            || anyhow!("remote registry does not contain {} version {}", skill_id, version),
        );
    }
    candidates.sort_by(|left, right| {
        compare_semver_versions(left.entry.version.as_str(), right.entry.version.as_str())
    });
    candidates.into_iter().last().ok_or_else(|| {
        anyhow!("remote registry does not contain installable versions for {}", skill_id)
    })
}

fn parse_https_url(raw: &str, label: &str) -> Result<Url> {
    let parsed =
        Url::parse(raw.trim()).with_context(|| format!("{label} must be a valid absolute URL"))?;
    if parsed.scheme() != "https" {
        anyhow::bail!("{label} must use https");
    }
    Ok(parsed)
}

fn build_registry_http_client(registry_ca_cert: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder().https_only(true).timeout(Duration::from_secs(20));
    if let Some(path) = registry_ca_cert {
        let cert_path = Path::new(path);
        let cert_bytes = fs::read(cert_path).with_context(|| {
            format!("failed to read --registry-ca-cert {}", cert_path.display())
        })?;
        let certificate = reqwest::Certificate::from_pem(cert_bytes.as_slice())
            .context("failed to parse --registry-ca-cert PEM")?;
        builder = builder.add_root_certificate(certificate);
    }
    builder.build().context("failed to build registry HTTP client")
}

fn fetch_limited_bytes(client: &Client, url: &str, limit: usize) -> Result<Vec<u8>> {
    let mut response = client
        .get(url)
        .send()
        .with_context(|| format!("failed to fetch {}", url))?
        .error_for_status()
        .with_context(|| format!("remote endpoint returned non-success for {}", url))?;
    if response
        .content_length()
        .is_some_and(|content_length| usize::try_from(content_length).unwrap_or(usize::MAX) > limit)
    {
        anyhow::bail!(
            "remote payload {} exceeds configured limit (content-length > {})",
            url,
            limit
        );
    }
    let mut payload = Vec::with_capacity(limit.min(64 * 1024));
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        let bytes_read = response
            .read(&mut chunk)
            .with_context(|| format!("failed to read response body from {}", url))?;
        if bytes_read == 0 {
            break;
        }
        if payload.len().saturating_add(bytes_read) > limit {
            anyhow::bail!("remote payload {} exceeds configured limit (>{})", url, limit);
        }
        payload.extend_from_slice(&chunk[..bytes_read]);
    }
    Ok(payload)
}

fn resolve_and_prompt_missing_skill_secrets(
    manifest: &palyra_skills::SkillManifest,
    non_interactive: bool,
) -> Result<Vec<MissingSkillSecret>> {
    let requested = manifest
        .capabilities
        .secrets
        .iter()
        .flat_map(|scope| {
            scope
                .key_names
                .iter()
                .map(|key| MissingSkillSecret { scope: scope.scope.clone(), key: key.clone() })
        })
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    let vault =
        open_cli_vault().context("failed to initialize vault runtime for skills secrets")?;
    let interactive =
        !non_interactive && std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
    let mut missing = Vec::new();
    for secret in requested {
        if secret.key.contains('*') {
            missing.push(secret);
            continue;
        }
        let scope = parse_vault_scope(secret.scope.as_str()).with_context(|| {
            format!(
                "skill manifest requested invalid vault scope '{}' for key '{}'",
                secret.scope, secret.key
            )
        })?;
        match vault.get_secret(&scope, secret.key.as_str()) {
            Ok(_) => continue,
            Err(VaultError::NotFound) => {
                if interactive
                    && prompt_yes_no(
                        format!(
                            "Missing skill secret {}/{}. Set now in vault? [y/N]: ",
                            secret.scope, secret.key
                        )
                        .as_str(),
                    )?
                {
                    let value = prompt_secret_value(
                        format!(
                            "Enter value for {}/{} (single line, empty aborts): ",
                            secret.scope, secret.key
                        )
                        .as_str(),
                    )?;
                    if !value.is_empty() {
                        vault
                            .put_secret(&scope, secret.key.as_str(), value.as_bytes())
                            .with_context(|| {
                                format!(
                                    "failed to persist prompted secret {}/{}",
                                    secret.scope, secret.key
                                )
                            })?;
                        continue;
                    }
                }
                missing.push(secret);
            }
            Err(error) => {
                return Err(anyhow!(
                    "failed to read required skill secret {}/{} from vault: {}",
                    secret.scope,
                    secret.key,
                    error
                ));
            }
        }
    }
    Ok(missing)
}

pub(crate) fn prompt_yes_no_default(prompt: &str, default: bool) -> Result<bool> {
    eprint!("{prompt}");
    std::io::stderr().flush().context("stderr flush failed")?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).context("failed to read interactive answer")?;
    let normalized = answer.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(default);
    }
    Ok(matches!(normalized.as_str(), "y" | "yes"))
}

fn read_json_string<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_str()
}

fn normalize_optional_text_arg(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn normalize_required_text_arg(raw: String, name: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{name} cannot be empty");
    }
    Ok(trimmed.to_owned())
}

#[cfg(test)]
fn redact_channel_router_preview_session_key(payload: &mut Value) {
    output::channels::redact_router_preview_session_key(payload);
}

pub(crate) fn prompt_yes_no(prompt: &str) -> Result<bool> {
    eprint!("{prompt}");
    std::io::stderr().flush().context("stderr flush failed")?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).context("failed to read interactive answer")?;
    let normalized = answer.trim().to_ascii_lowercase();
    Ok(matches!(normalized.as_str(), "y" | "yes"))
}

pub(crate) fn prompt_secret_value(prompt: &str) -> Result<String> {
    eprint!("{prompt}");
    std::io::stderr().flush().context("stderr flush failed")?;
    let value = rpassword::read_password().context("failed to read secret value")?;
    Ok(normalize_prompt_secret_value(&value))
}

pub(crate) fn normalize_prompt_secret_value(raw: &str) -> String {
    raw.trim_end_matches(['\r', '\n']).to_owned()
}

fn update_skill_current_pointer(skill_root: &Path, version: &str) -> Result<()> {
    let current = skill_root.join(SKILLS_CURRENT_LINK_NAME);
    if current.exists() {
        if current.is_dir() && !current.is_symlink() {
            fs::remove_dir_all(current.as_path()).with_context(|| {
                format!("failed to remove existing current directory {}", current.display())
            })?;
        } else {
            fs::remove_file(current.as_path()).with_context(|| {
                format!("failed to remove existing current pointer {}", current.display())
            })?;
        }
    } else if fs::symlink_metadata(current.as_path()).is_ok() {
        fs::remove_file(current.as_path()).with_context(|| {
            format!("failed to remove existing current pointer {}", current.display())
        })?;
    }
    let created = create_optional_directory_symlink(version, current.as_path())?;
    if !created {
        eprintln!(
            "warning: could not create optional '{}' symlink for skill root {}",
            SKILLS_CURRENT_LINK_NAME,
            skill_root.display()
        );
    }
    Ok(())
}

fn remove_skill_current_pointer(skill_root: &Path) -> Result<()> {
    let current = skill_root.join(SKILLS_CURRENT_LINK_NAME);
    if !current.exists() && fs::symlink_metadata(current.as_path()).is_err() {
        return Ok(());
    }
    if current.is_dir() && !current.is_symlink() {
        fs::remove_dir_all(current.as_path())
            .with_context(|| format!("failed to remove current directory {}", current.display()))?;
    } else {
        fs::remove_file(current.as_path())
            .with_context(|| format!("failed to remove current pointer {}", current.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
fn create_optional_directory_symlink(target: &str, link: &Path) -> Result<bool> {
    match std::os::unix::fs::symlink(target, link) {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

#[cfg(windows)]
fn create_optional_directory_symlink(target: &str, link: &Path) -> Result<bool> {
    match std::os::windows::fs::symlink_dir(target, link) {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

fn open_cli_vault() -> Result<Vault> {
    let identity_store_root =
        default_identity_store_root().context("failed to resolve default identity store root")?;
    let vault_root = env::var("PALYRA_VAULT_DIR").ok().map(PathBuf::from);
    let backend_preference = parse_cli_vault_backend_preference()?;
    Vault::open_with_config(VaultConfigOptions {
        root: vault_root,
        identity_store_root: Some(identity_store_root),
        backend_preference,
        ..VaultConfigOptions::default()
    })
    .map_err(anyhow::Error::from)
}

fn parse_cli_vault_backend_preference() -> Result<VaultBackendPreference> {
    match env::var("PALYRA_VAULT_BACKEND") {
        Ok(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "" | "auto" | "default" => Ok(VaultBackendPreference::Auto),
                "encrypted_file" => Ok(VaultBackendPreference::EncryptedFile),
                _ => anyhow::bail!("PALYRA_VAULT_BACKEND must be one of: auto | encrypted_file"),
            }
        }
        Err(std::env::VarError::NotPresent) => Ok(VaultBackendPreference::Auto),
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("PALYRA_VAULT_BACKEND must contain valid UTF-8")
        }
    }
}

fn parse_vault_scope(raw: &str) -> Result<VaultScope> {
    raw.parse::<VaultScope>()
        .map_err(anyhow::Error::from)
        .with_context(|| format!("invalid vault scope: {}", raw.trim()))
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
        None => effective_config_path()
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

fn effective_config_path() -> Option<String> {
    if let Some(path) = app::current_root_context()
        .and_then(|context| context.config_path().map(|path| path.to_string_lossy().into_owned()))
    {
        return Some(path);
    }
    find_default_config_path()
}

fn resolve_identity_store_root(store_dir: Option<String>) -> Result<PathBuf> {
    if let Some(path) = store_dir {
        return Ok(PathBuf::from(path));
    }
    default_identity_store_root().context("failed to resolve default identity store root")
}

fn build_identity_store(store_root: &Path) -> Result<Arc<dyn SecretStore>> {
    let store = FilesystemSecretStore::new(store_root).with_context(|| {
        format!("failed to initialize secret store at {}", store_root.display())
    })?;
    Ok(Arc::new(store))
}

fn build_pairing_method(method: PairingMethodArg, proof: &str) -> PairingMethod {
    match method {
        PairingMethodArg::Pin => PairingMethod::Pin { code: proof.to_owned() },
        PairingMethodArg::Qr => PairingMethod::Qr { token: proof.to_owned() },
    }
}

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

fn to_identity_client_kind(value: PairingClientKindArg) -> PairingClientKind {
    match value {
        PairingClientKindArg::Cli => PairingClientKind::Cli,
        PairingClientKindArg::Desktop => PairingClientKind::Desktop,
        PairingClientKindArg::Node => PairingClientKind::Node,
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
        "apps/browser-extension",
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

fn doctor_repo_scaffold_required() -> bool {
    env::current_exe()
        .ok()
        .is_some_and(|path| repo_checkout_detected_from_binary_path(path.as_path()))
}

fn repo_checkout_detected_from_binary_path(binary_path: &Path) -> bool {
    binary_path.ancestors().any(looks_like_repo_root)
}

fn looks_like_repo_root(path: &Path) -> bool {
    ["Cargo.toml", "crates", "apps", "schemas"]
        .iter()
        .all(|entry| path.join(entry).exists())
}

fn memory_embeddings_model_config_ok() -> bool {
    memory_embeddings_model_config_ok_impl().unwrap_or(true)
}

fn memory_embeddings_model_config_ok_impl() -> Result<bool> {
    let Some(parsed) = read_doctor_root_file_config()? else {
        return Ok(true);
    };
    Ok(memory_embeddings_model_configured(&parsed))
}

fn memory_embeddings_model_configured(parsed: &RootFileConfig) -> bool {
    let Some(provider) = parsed.model_provider.as_ref() else {
        return true;
    };
    let kind = provider.kind.as_deref().unwrap_or("deterministic").trim().to_ascii_lowercase();
    let is_openai_compatible =
        kind == "openai_compatible" || kind == "openai-compatible" || kind == "openai";
    if !is_openai_compatible {
        return true;
    }
    provider.openai_embeddings_model.as_ref().map(|value| !value.trim().is_empty()).unwrap_or(false)
}

fn process_runner_tier_b_allowlist_config_ok() -> bool {
    process_runner_tier_b_allowlist_config_ok_impl().unwrap_or(true)
}

fn process_runner_tier_b_allowlist_config_ok_impl() -> Result<bool> {
    let Some(parsed) = read_doctor_root_file_config()? else {
        return Ok(true);
    };
    Ok(process_runner_tier_b_allowlist_preflight_only(&parsed))
}

fn doctor_config_path() -> Option<PathBuf> {
    match env::var("PALYRA_CONFIG") {
        Ok(explicit) => {
            let trimmed = explicit.trim();
            if trimmed.is_empty() {
                return None;
            }
            parse_config_path(trimmed).ok()
        }
        Err(env::VarError::NotPresent) => find_default_config_path().map(PathBuf::from),
        Err(env::VarError::NotUnicode(_)) => None,
    }
}

fn read_doctor_root_file_config() -> Result<Option<RootFileConfig>> {
    let Some(config_path) = doctor_config_path() else {
        return Ok(None);
    };
    if !config_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(config_path.as_path())
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let (document, _) = parse_document_with_migration(content.as_str())
        .context("failed to migrate doctor config document")?;
    let migrated =
        toml::to_string(&document).context("failed to serialize doctor config document")?;
    let parsed: RootFileConfig =
        toml::from_str(migrated.as_str()).context("invalid doctor daemon config schema")?;
    Ok(Some(parsed))
}

fn process_runner_tier_b_allowlist_preflight_only(parsed: &RootFileConfig) -> bool {
    let Some(process_runner) =
        parsed.tool_call.as_ref().and_then(|tool_call| tool_call.process_runner.as_ref())
    else {
        return true;
    };
    let tier = process_runner.tier.as_deref().unwrap_or("b").trim().to_ascii_lowercase();
    if tier != "b" && tier != "tier_b" {
        return true;
    }
    let has_host_allowlists = process_runner
        .allowed_egress_hosts
        .as_ref()
        .map(|hosts| !hosts.is_empty())
        .unwrap_or(false)
        || process_runner
            .allowed_dns_suffixes
            .as_ref()
            .map(|suffixes| !suffixes.is_empty())
            .unwrap_or(false);
    !has_host_allowlists
}

fn process_runner_tier_c_strict_offline_config_ok() -> bool {
    process_runner_tier_c_strict_offline_config_ok_impl().unwrap_or(true)
}

fn process_runner_tier_c_strict_offline_config_ok_impl() -> Result<bool> {
    let Some(parsed) = read_doctor_root_file_config()? else {
        return Ok(true);
    };
    Ok(process_runner_tier_c_strict_offline_allowlists_empty(&parsed))
}

fn process_runner_tier_c_strict_offline_allowlists_empty(parsed: &RootFileConfig) -> bool {
    let Some(process_runner) =
        parsed.tool_call.as_ref().and_then(|tool_call| tool_call.process_runner.as_ref())
    else {
        return true;
    };
    let tier = process_runner.tier.as_deref().unwrap_or("b").trim().to_ascii_lowercase();
    if tier != "c" && tier != "tier_c" {
        return true;
    }
    let mode = process_runner
        .egress_enforcement_mode
        .as_deref()
        .unwrap_or("strict")
        .trim()
        .to_ascii_lowercase();
    if mode != "strict" {
        return true;
    }
    let has_host_allowlists = process_runner
        .allowed_egress_hosts
        .as_ref()
        .map(|hosts| !hosts.is_empty())
        .unwrap_or(false)
        || process_runner
            .allowed_dns_suffixes
            .as_ref()
            .map(|suffixes| !suffixes.is_empty())
            .unwrap_or(false);
    !has_host_allowlists
}

fn process_runner_tier_c_windows_backend_config_ok() -> bool {
    process_runner_tier_c_windows_backend_config_ok_impl().unwrap_or(true)
}

fn process_runner_tier_c_windows_backend_config_ok_impl() -> Result<bool> {
    let Some(parsed) = read_doctor_root_file_config()? else {
        return Ok(true);
    };
    Ok(process_runner_tier_c_windows_backend_supported(&parsed))
}

fn process_runner_tier_c_windows_backend_supported(parsed: &RootFileConfig) -> bool {
    if !cfg!(windows) {
        return true;
    }
    let Some(process_runner) =
        parsed.tool_call.as_ref().and_then(|tool_call| tool_call.process_runner.as_ref())
    else {
        return true;
    };
    let tier = process_runner.tier.as_deref().unwrap_or("b").trim().to_ascii_lowercase();
    tier != "c" && tier != "tier_c"
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

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DoctorSeverity {
    Blocking,
    Warning,
    Info,
}

impl DoctorSeverity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Blocking => "blocking",
            Self::Warning => "warning",
            Self::Info => "info",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
struct DoctorCheck {
    key: &'static str,
    ok: bool,
    required: bool,
    severity: DoctorSeverity,
    remediation: &'static [&'static str],
}

impl DoctorCheck {
    const fn blocking(key: &'static str, ok: bool, remediation: &'static [&'static str]) -> Self {
        Self { key, ok, required: true, severity: DoctorSeverity::Blocking, remediation }
    }

    const fn warning(key: &'static str, ok: bool, remediation: &'static [&'static str]) -> Self {
        Self { key, ok, required: false, severity: DoctorSeverity::Warning, remediation }
    }

    const fn info(key: &'static str, ok: bool, remediation: &'static [&'static str]) -> Self {
        Self { key, ok, required: false, severity: DoctorSeverity::Info, remediation }
    }
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    generated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<app::ActiveProfileContext>,
    checks: Vec<DoctorCheck>,
    summary: DoctorSummary,
    config: DoctorConfigSnapshot,
    identity: DoctorIdentitySnapshot,
    connectivity: DoctorConnectivitySnapshot,
    provider_auth: DoctorProviderAuthSnapshot,
    browser: DoctorBrowserSnapshot,
    access: DoctorAccessSnapshot,
    skills: SkillsInventorySnapshot,
    sandbox: DoctorSandboxSnapshot,
    deployment: DoctorDeploymentSnapshot,
}

#[derive(Debug, Serialize)]
struct DoctorSummary {
    required_checks_total: usize,
    required_checks_ok: usize,
    required_checks_failed: usize,
    warning_checks_failed: usize,
    info_checks_failed: usize,
}

#[derive(Debug, Serialize)]
struct DoctorConfigSnapshot {
    path: Option<String>,
    exists: bool,
    parsed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct DoctorIdentitySnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    store_root: Option<String>,
    exists: bool,
    writable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct DoctorConnectivityProbe {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct DoctorConnectivitySnapshot {
    daemon_url: String,
    grpc_url: String,
    http: DoctorConnectivityProbe,
    grpc: DoctorConnectivityProbe,
    admin: DoctorConnectivityProbe,
}

#[derive(Debug, Serialize)]
struct DoctorProviderAuthSnapshot {
    fetched: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_provider: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_summary: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct DoctorBrowserSnapshot {
    configured_enabled: bool,
    auth_token_configured: bool,
    endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    connect_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_screenshot_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_title_bytes: Option<u64>,
    state_dir_configured: bool,
    state_key_vault_ref_configured: bool,
    diagnostics_fetched: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    health_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_sessions: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recent_relay_action_failures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recent_health_failures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct DoctorAccessSnapshot {
    registry_path: Option<String>,
    registry_exists: bool,
    parsed: bool,
    compat_api_enabled: bool,
    api_tokens_enabled: bool,
    team_mode_enabled: bool,
    rbac_enabled: bool,
    staged_rollout_enabled: bool,
    backfill_required: bool,
    blocking_issues: usize,
    warning_issues: usize,
    external_api_safe_mode: bool,
    team_mode_safe_mode: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct DoctorSandboxSnapshot {
    tier_b_egress_allowlists_preflight_only: bool,
    tier_c_strict_offline_only: bool,
    tier_c_windows_backend_supported: bool,
}

#[derive(Debug, Serialize)]
struct DoctorDeploymentSnapshot {
    mode: String,
    bind_profile: String,
    binds: DoctorDeploymentBindSnapshot,
    gateway_tls_enabled: bool,
    admin_auth_required: bool,
    admin_token_configured: bool,
    dangerous_remote_bind_ack_config: bool,
    dangerous_remote_bind_ack_env: bool,
    remote_bind_detected: bool,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DoctorDeploymentBindSnapshot {
    admin: String,
    grpc: String,
    quic: String,
}

#[derive(Debug, Serialize)]
struct SupportBundle {
    schema_version: u32,
    generated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<app::ActiveProfileContext>,
    build: SupportBundleBuildSnapshot,
    platform: SupportBundlePlatformSnapshot,
    doctor: DoctorReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery: Option<Value>,
    config: SupportBundleConfigSnapshot,
    observability: SupportBundleObservabilitySnapshot,
    triage: SupportBundleTriageSnapshot,
    diagnostics: SupportBundleDiagnosticsSnapshot,
    journal: SupportBundleJournalSnapshot,
    truncated: bool,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SupportBundleBuildSnapshot {
    version: String,
    git_hash: String,
    build_profile: String,
}

#[derive(Debug, Serialize)]
struct SupportBundlePlatformSnapshot {
    os: String,
    family: String,
    arch: String,
}

#[derive(Debug, Serialize)]
struct SupportBundleConfigSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redacted_document: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fingerprint_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct SupportBundleDiagnosticsSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    gateway_health: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    service_status: Option<support::service::GatewayServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    browser_status: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_status: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    admin_status: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    admin_status_error: Option<String>,
    skills: SkillsInventorySnapshot,
}

#[derive(Debug, Serialize)]
struct SupportBundleObservabilitySnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recent_failures: Option<Value>,
}

#[derive(Debug, Serialize)]
struct SupportBundleTriageSnapshot {
    playbook: String,
    failure_classes: Vec<String>,
    common_order: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SupportBundleJournalSnapshot {
    db_path: String,
    available: bool,
    hash_chain_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest_hash: Option<String>,
    recent_hashes: Vec<String>,
    last_errors: Vec<SupportBundleJournalErrorRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct SupportBundleJournalErrorRecord {
    event_id: String,
    kind: i32,
    timestamp_unix_ms: i64,
    message: String,
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

#[derive(Debug, Serialize, Deserialize)]
struct JournalRecentResponse {
    total_events: u64,
    hash_chain_enabled: bool,
    events: Vec<JournalRecentEvent>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
struct RunStatusResponse {
    run_id: String,
    state: String,
    cancel_requested: bool,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    tape_events: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunTapeResponse {
    run_id: String,
    #[serde(default)]
    returned_bytes: usize,
    next_after_seq: Option<i64>,
    events: Vec<RunTapeEvent>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize)]
struct SkillStatusRequestBody {
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(rename = "override", skip_serializing_if = "Option::is_none")]
    override_enabled: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SkillStatusResponse {
    skill_id: String,
    version: String,
    status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    detected_at_ms: i64,
    operator_principal: String,
}

#[cfg(test)]
mod cli_v1_tests {
    #[cfg(target_os = "windows")]
    use super::browser_open_commands;

    use super::{
        build_journal_checkpoint_attestation, build_support_bundle_diagnostics_snapshot,
        compare_semver_versions, ensure_remote_registry_same_origin, fetch_limited_bytes,
        fetch_remote_registry_entries_with_fetcher, is_retryable_grpc_error,
        memory_embeddings_model_configured, normalize_browser_open_url, normalize_client_socket,
        normalize_installed_skills_index, normalize_prompt_secret_value,
        normalize_relative_registry_path, normalize_sha256_fingerprint, parse_acp_shim_input_line,
        parse_and_verify_signed_remote_registry_index, parse_remote_dashboard_base_url,
        parse_support_bundle_daemon_url, process_runner_tier_b_allowlist_preflight_only,
        process_runner_tier_c_strict_offline_allowlists_empty,
        process_runner_tier_c_windows_backend_supported, registry_key_id_for,
        resolve_dashboard_access_target, sha256_hex, trust_store_integrity_vault_key,
        validate_registry_index, verify_or_initialize_trust_store_integrity, write_file_atomically,
        BrowserOpenCommand, DashboardAccessMode, DashboardAccessSource, InstalledSkillRecord,
        InstalledSkillSource, InstalledSkillsIndex, JournalCheckpointAttestationRequest,
        JournalCheckpointModeArg, RegistrySignature, RootFileConfig, SignedSkillRegistryIndex,
        SkillRegistryEntry, SkillRegistryIndex, REGISTRY_INDEX_SCHEMA_VERSION,
        REGISTRY_SIGNATURE_ALGORITHM, REGISTRY_SIGNED_INDEX_SCHEMA_VERSION,
    };
    use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
    use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
    use palyra_identity::DeviceIdentity;
    use palyra_skills::SkillTrustStore;
    use reqwest::Url;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::Duration;

    use crate::common_v1;

    fn spawn_one_shot_http_server(body: Vec<u8>) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test TCP listener should bind");
        let address = listener.local_addr().expect("listener should report local address");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test server should accept one client");
            let mut request_buffer = [0_u8; 512];
            let _ = stream.read(&mut request_buffer);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if stream.write_all(headers.as_bytes()).is_err() {
                return;
            }
            for chunk in body.chunks(1024) {
                if stream.write_all(chunk).is_err() {
                    break;
                }
            }
            let _ = stream.flush();
        });
        (format!("http://{address}/registry/index.json"), handle)
    }

    fn trust_store_with_registry_key(publisher: &str, signing_key: &SigningKey) -> SkillTrustStore {
        let verifying_key = VerifyingKey::from(signing_key);
        let mut key_hex = String::with_capacity(64);
        for byte in verifying_key.as_bytes() {
            key_hex.push_str(format!("{byte:02x}").as_str());
        }
        let mut store = SkillTrustStore::default();
        store.add_trusted_key(publisher, key_hex.as_str()).expect("trusted key should be accepted");
        store
    }

    fn sign_registry_index(
        signing_key: &SigningKey,
        publisher: &str,
        index: SkillRegistryIndex,
    ) -> Vec<u8> {
        let verifying_key = VerifyingKey::from(signing_key);
        let payload_sha256 =
            sha256_hex(serde_json::to_vec(&index).expect("index should serialize").as_slice());
        let signature = signing_key.sign(payload_sha256.as_bytes());
        serde_json::to_vec(&SignedSkillRegistryIndex {
            schema_version: REGISTRY_SIGNED_INDEX_SCHEMA_VERSION,
            index,
            signature: RegistrySignature {
                algorithm: REGISTRY_SIGNATURE_ALGORITHM.to_owned(),
                publisher: publisher.to_owned(),
                key_id: registry_key_id_for(&verifying_key),
                public_key_base64: BASE64_STANDARD.encode(verifying_key.as_bytes()),
                payload_sha256,
                signature_base64: BASE64_STANDARD.encode(signature.to_bytes()),
                signed_at_unix_ms: 1_730_000_000_123,
            },
        })
        .expect("signed registry index should serialize")
    }

    fn test_registry_entry(
        skill_id: &str,
        version: &str,
        artifact: &str,
        sha_seed: char,
    ) -> SkillRegistryEntry {
        SkillRegistryEntry {
            skill_id: skill_id.to_owned(),
            version: version.to_owned(),
            publisher: "acme".to_owned(),
            artifact: artifact.to_owned(),
            artifact_sha256: sha_seed.to_string().repeat(64),
            artifact_bytes: Some(128),
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            // SAFETY: tests serialize env updates via env_lock().
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                // SAFETY: tests serialize env updates via env_lock().
                unsafe {
                    std::env::set_var(self.key, previous);
                }
            } else {
                // SAFETY: tests serialize env updates via env_lock().
                unsafe {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    fn resolve_skills_root_prefers_installed_root_context_state_root() {
        let _lock = env_lock().lock().expect("env lock should be available");
        crate::app::clear_root_context_for_tests();
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let state_root = tempdir.path().join("portable-state");
        let state_root_string = state_root.to_string_lossy().into_owned();
        let context = crate::app::install_root_context(crate::args::RootOptions {
            state_root: Some(state_root_string),
            ..crate::args::RootOptions::default()
        })
        .expect("root context should install");

        let skills_root = super::resolve_skills_root(None).expect("skills root should resolve");
        assert_eq!(skills_root, context.state_root().join("skills"));
        crate::app::clear_root_context_for_tests();
    }

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
        let error = result.err().expect("whitespace-only prompt must be rejected");
        assert!(error.to_string().contains("non-empty text"), "unexpected error message: {error}");
    }

    #[test]
    fn optional_ulid_json_value_preserves_present_and_absent_values() {
        let present = super::optional_ulid_json_value(&Some(common_v1::CanonicalId {
            ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
        }));
        assert_eq!(present, Value::String("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()));

        let absent = super::optional_ulid_json_value(&None);
        assert_eq!(absent, Value::Null);
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
    fn normalize_sha256_fingerprint_accepts_colons_and_uppercase() {
        let normalized = normalize_sha256_fingerprint(
            "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
            "gateway_access.pinned_server_cert_fingerprint_sha256",
        )
        .expect("fingerprint with separators should normalize");
        assert_eq!(normalized, "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899");
    }

    #[test]
    fn normalize_sha256_fingerprint_rejects_invalid_length() {
        let error = normalize_sha256_fingerprint("abc123", "gateway_access.pinned_gateway_ca")
            .expect_err("short fingerprint should fail");
        assert!(error.to_string().contains("64 hexadecimal characters"));
    }

    #[test]
    fn parse_remote_dashboard_base_url_rejects_query_and_fragment() {
        let query_error = parse_remote_dashboard_base_url(
            "https://dashboard.example.com/?token=abc",
            "gateway_access.remote_base_url",
        )
        .expect_err("query parameters must be rejected");
        assert!(query_error.to_string().contains("must not include query or fragment"));

        let fragment_error = parse_remote_dashboard_base_url(
            "https://dashboard.example.com/#frag",
            "gateway_access.remote_base_url",
        )
        .expect_err("fragments must be rejected");
        assert!(fragment_error.to_string().contains("must not include query or fragment"));
    }

    #[test]
    fn parse_support_bundle_daemon_url_accepts_loopback_hosts() {
        let localhost =
            parse_support_bundle_daemon_url("http://localhost:7142", "PALYRA_DAEMON_URL")
                .expect("localhost should be allowed for support-bundle diagnostics");
        assert_eq!(localhost, "http://localhost:7142/");

        let ipv4 = parse_support_bundle_daemon_url("http://127.0.0.1:7142", "PALYRA_DAEMON_URL")
            .expect("loopback IPv4 should be allowed for support-bundle diagnostics");
        assert_eq!(ipv4, "http://127.0.0.1:7142/");

        let ipv6 = parse_support_bundle_daemon_url("http://[::1]:7142", "PALYRA_DAEMON_URL")
            .expect("loopback IPv6 should be allowed for support-bundle diagnostics");
        assert_eq!(ipv6, "http://[::1]:7142/");
    }

    #[test]
    fn parse_support_bundle_daemon_url_rejects_non_loopback_hosts() {
        let error =
            parse_support_bundle_daemon_url("https://example.com:7142", "PALYRA_DAEMON_URL")
                .expect_err("non-loopback host must be rejected for support-bundle diagnostics");
        assert!(error
            .to_string()
            .contains("must target a loopback host for support-bundle diagnostics"));
    }

    #[test]
    fn support_bundle_diagnostics_rejects_non_loopback_daemon_url_before_request() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let _daemon_url = ScopedEnvVar::set("PALYRA_DAEMON_URL", "https://example.com:7142");
        let _admin_token = ScopedEnvVar::set("PALYRA_ADMIN_TOKEN", "test-admin-token");

        let snapshot = build_support_bundle_diagnostics_snapshot();
        assert!(
            snapshot.admin_status.is_none(),
            "non-loopback daemon URL must skip admin status fetch"
        );
        let error = snapshot
            .admin_status_error
            .expect("non-loopback daemon URL should produce a diagnostic error");
        assert!(
            error.contains("must target a loopback host for support-bundle diagnostics"),
            "diagnostic error should mention loopback requirement: {error}"
        );
    }

    #[test]
    fn resolve_dashboard_access_target_prefers_remote_url_from_config() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let config_path = tempdir.path().join("palyra.toml");
        std::fs::write(
            config_path.as_path(),
            r#"
version = 1
[gateway_access]
remote_base_url = "https://dashboard.example.com/"
"#,
        )
        .expect("fixture config should be written");
        let _config = ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());

        let target =
            resolve_dashboard_access_target(None).expect("dashboard access target should resolve");
        assert_eq!(target.url, "https://dashboard.example.com/");
        assert_eq!(target.mode, DashboardAccessMode::Remote);
        assert_eq!(target.source, DashboardAccessSource::ConfigRemoteUrl);
        assert!(target.verification.is_none());
    }

    #[test]
    fn resolve_dashboard_access_target_uses_daemon_bind_when_remote_url_missing() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let config_path = tempdir.path().join("palyra.toml");
        std::fs::write(
            config_path.as_path(),
            r#"
version = 1
[daemon]
bind_addr = "0.0.0.0"
port = 9191
"#,
        )
        .expect("fixture config should be written");
        let _config = ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());

        let target =
            resolve_dashboard_access_target(None).expect("dashboard access target should resolve");
        assert_eq!(target.url, "http://127.0.0.1:9191/");
        assert_eq!(target.mode, DashboardAccessMode::Local);
        assert_eq!(target.source, DashboardAccessSource::ConfigDaemonBind);
    }

    #[test]
    fn resolve_dashboard_access_target_rejects_ambiguous_remote_pin_config() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let config_path = tempdir.path().join("palyra.toml");
        std::fs::write(
            config_path.as_path(),
            r#"
version = 1
[gateway_access]
remote_base_url = "https://dashboard.example.com/"
pinned_server_cert_fingerprint_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
pinned_gateway_ca_fingerprint_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
"#,
        )
        .expect("fixture config should be written");
        let _config = ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());

        let error = resolve_dashboard_access_target(None)
            .expect_err("ambiguous pin configuration must be rejected");
        assert!(error.to_string().contains("pins are ambiguous"));
    }

    #[test]
    fn normalize_browser_open_url_requires_http_or_https() {
        let error = normalize_browser_open_url("file:///tmp/palyra")
            .expect_err("non-http browser handoff should be rejected");
        assert!(error.to_string().contains("only supports http:// and https:// URLs"));
    }

    #[test]
    fn normalize_browser_open_url_rejects_embedded_credentials() {
        let error = normalize_browser_open_url("https://operator:secret@example.com/dashboard")
            .expect_err("credential-bearing browser handoff should be rejected");
        assert!(error.to_string().contains("embedded credentials"));
    }

    #[test]
    fn normalize_browser_open_url_preserves_safe_dashboard_targets() {
        let normalized = normalize_browser_open_url("https://dashboard.example.com/palyra/")
            .expect("safe dashboard URL should normalize");
        assert_eq!(normalized, "https://dashboard.example.com/palyra/");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_browser_open_commands_try_cmd_start_before_explorer() {
        let commands = browser_open_commands("http://127.0.0.1:7142/");
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].program, "cmd");
        assert_eq!(commands[0].args, vec!["/C", "start", "\"\"", "http://127.0.0.1:7142/"]);
        assert_eq!(commands[1].program, "explorer.exe");
        assert_eq!(commands[1].args, vec!["http://127.0.0.1:7142/"]);
    }

    #[test]
    fn browser_open_command_display_includes_program_and_arguments() {
        let command = BrowserOpenCommand {
            program: "cmd",
            args: vec![
                "/C".to_owned(),
                "start".to_owned(),
                "\"\"".to_owned(),
                "http://127.0.0.1:7142/".to_owned(),
            ],
        };
        assert_eq!(command.display(), "`cmd /C start \"\" http://127.0.0.1:7142/`");
    }

    #[test]
    fn grpc_retry_only_for_retryable_status_codes() {
        let unavailable = anyhow::Error::new(tonic::Status::unavailable("transient"));
        let invalid_argument = anyhow::Error::new(tonic::Status::invalid_argument("invalid"));

        assert!(is_retryable_grpc_error(&unavailable));
        assert!(!is_retryable_grpc_error(&invalid_argument));
    }

    #[test]
    fn semver_comparison_uses_numeric_ordering() {
        assert_eq!(compare_semver_versions("1.10.0", "1.2.99"), std::cmp::Ordering::Greater);
        assert_eq!(compare_semver_versions("1.2.0", "1.2.0"), std::cmp::Ordering::Equal);
    }

    #[test]
    fn journal_checkpoint_attestation_signature_verifies() {
        let device_identity = DeviceIdentity::generate("01ARZ3NDEKTSV4RRFFQ69G5FAV")
            .expect("device identity should generate");
        let latest_hash = "a".repeat(64);
        let attestation = build_journal_checkpoint_attestation(
            &device_identity,
            JournalCheckpointAttestationRequest {
                db_path: Path::new("data/journal.sqlite3"),
                mode: JournalCheckpointModeArg::Truncate,
                busy: 0,
                log_frames: 11,
                checkpointed_frames: 11,
                latest_hash: latest_hash.as_str(),
                signed_at_unix_ms: 1_730_000_000_123,
            },
        )
        .expect("journal checkpoint attestation should be built");
        let payload_bytes =
            serde_json::to_vec(&attestation.payload).expect("attestation payload should serialize");
        assert_eq!(
            attestation.payload_sha256,
            sha256_hex(payload_bytes.as_slice()),
            "payload hash must match serialized payload bytes"
        );
        assert_eq!(
            attestation.key_id,
            registry_key_id_for(&device_identity.verifying_key()),
            "key identifier must derive from the device verifying key"
        );
        let signature_bytes = BASE64_STANDARD
            .decode(attestation.signature_base64.as_bytes())
            .expect("signature should decode from base64");
        let signature_bytes: [u8; 64] =
            signature_bytes.as_slice().try_into().expect("signature must be 64 bytes");
        let signature = Signature::from_bytes(&signature_bytes);
        device_identity
            .verifying_key()
            .verify(payload_bytes.as_slice(), &signature)
            .expect("signature must verify against attestation payload");
    }

    #[test]
    fn journal_checkpoint_attestation_rejects_empty_latest_hash() {
        let device_identity = DeviceIdentity::generate("01ARZ3NDEKTSV4RRFFQ69G5FAV")
            .expect("device identity should generate");
        let result = build_journal_checkpoint_attestation(
            &device_identity,
            JournalCheckpointAttestationRequest {
                db_path: Path::new("data/journal.sqlite3"),
                mode: JournalCheckpointModeArg::Truncate,
                busy: 0,
                log_frames: 0,
                checkpointed_frames: 0,
                latest_hash: "   ",
                signed_at_unix_ms: 1_730_000_000_123,
            },
        );
        assert!(result.is_err(), "empty latest hash should fail closed");
    }

    #[test]
    fn trust_store_integrity_vault_key_is_stable_for_same_path() {
        let path = Path::new("/tmp/palyra/skills/trust-store.json");
        let first = trust_store_integrity_vault_key(path);
        let second = trust_store_integrity_vault_key(path);
        assert_eq!(first, second, "vault integrity key should be stable");
        assert!(
            first.starts_with("skills.trust_store.integrity."),
            "vault integrity key should use expected namespace prefix"
        );
    }

    #[test]
    fn trust_store_integrity_check_detects_tampered_file_contents() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let skills_root = tempdir.path().join("skills");
        std::fs::create_dir_all(skills_root.as_path()).expect("skills root should be created");
        let trust_store_path = skills_root.join("trust-store.json");
        let mut store = SkillTrustStore::default();
        store
            .add_trusted_key(
                "acme",
                "1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f",
            )
            .expect("trusted key should be accepted");
        store.save(trust_store_path.as_path()).expect("trust-store fixture should be written");

        let vault_root = tempdir.path().join("vault");
        let state_root = tempdir.path().join("state");
        let _vault_dir =
            ScopedEnvVar::set("PALYRA_VAULT_DIR", vault_root.to_string_lossy().as_ref());
        let _vault_backend = ScopedEnvVar::set("PALYRA_VAULT_BACKEND", "encrypted_file");
        #[cfg(not(windows))]
        let _state_root =
            ScopedEnvVar::set("XDG_STATE_HOME", state_root.to_string_lossy().as_ref());
        #[cfg(not(windows))]
        let _home = ScopedEnvVar::set("HOME", state_root.to_string_lossy().as_ref());
        #[cfg(windows)]
        let _local_app_data =
            ScopedEnvVar::set("LOCALAPPDATA", state_root.to_string_lossy().as_ref());
        #[cfg(windows)]
        let _app_data = ScopedEnvVar::set("APPDATA", state_root.to_string_lossy().as_ref());

        verify_or_initialize_trust_store_integrity(trust_store_path.as_path())
            .expect("initial trust-store digest should be persisted");

        std::fs::write(
            trust_store_path.as_path(),
            br#"{"schema_version":1,"trusted_publishers":{},"tofu_publishers":{}}"#,
        )
        .expect("tampered trust-store fixture should be written");

        let error = verify_or_initialize_trust_store_integrity(trust_store_path.as_path())
            .expect_err("tampered trust-store should fail integrity verification");
        assert!(
            error.to_string().contains("integrity mismatch"),
            "error should mention trust-store integrity mismatch: {error}"
        );
    }

    #[test]
    fn normalize_registry_path_rejects_parent_traversal() {
        let result = normalize_relative_registry_path(Path::new("../artifact.palyra-skill"));
        assert!(result.is_err(), "parent traversal should be rejected");
    }

    #[test]
    fn remote_registry_same_origin_rejects_cross_origin_urls() {
        let registry_origin = Url::parse("https://registry.example/catalog/index.json")
            .expect("origin URL should parse");
        let candidate = Url::parse("https://evil.example/catalog/page-2.json")
            .expect("candidate URL should parse");
        let error =
            ensure_remote_registry_same_origin(&registry_origin, &candidate, "next_page URL")
                .expect_err("cross-origin pagination URL should be rejected");
        assert!(
            error.to_string().contains("must stay on origin"),
            "error should explain same-origin restriction: {error}"
        );
    }

    #[test]
    fn remote_registry_same_origin_accepts_same_origin_urls() {
        let registry_origin = Url::parse("https://registry.example/catalog/index.json")
            .expect("origin URL should parse");
        let candidate = registry_origin
            .join("../artifacts/acme.echo.palyra-skill")
            .expect("relative URL should resolve");
        let result =
            ensure_remote_registry_same_origin(&registry_origin, &candidate, "artifact URL");
        assert!(result.is_ok(), "same-origin artifact URL should be accepted");
    }

    #[test]
    fn remote_registry_fetch_detects_pagination_loops() {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let publisher = "acme-registry";
        let mut trust_store = trust_store_with_registry_key(publisher, &signing_key);

        let root_url = "https://registry.example/catalog/index.json";
        let page_two_url = "https://registry.example/catalog/page-2.json";

        let page_one_payload = sign_registry_index(
            &signing_key,
            publisher,
            SkillRegistryIndex {
                schema_version: REGISTRY_INDEX_SCHEMA_VERSION,
                generated_at_unix_ms: 1_730_000_000_000,
                entries: vec![test_registry_entry(
                    "acme.echo_http",
                    "1.0.0",
                    "../artifacts/acme.echo_http-v1.palyra-skill",
                    '1',
                )],
                next_page: Some("page-2.json".to_owned()),
            },
        );
        let page_two_payload = sign_registry_index(
            &signing_key,
            publisher,
            SkillRegistryIndex {
                schema_version: REGISTRY_INDEX_SCHEMA_VERSION,
                generated_at_unix_ms: 1_730_000_000_100,
                entries: vec![test_registry_entry(
                    "acme.echo_http",
                    "1.1.0",
                    "../artifacts/acme.echo_http-v1_1.palyra-skill",
                    '2',
                )],
                next_page: Some("index.json".to_owned()),
            },
        );
        let mut payloads = HashMap::<String, Vec<u8>>::from([
            (root_url.to_owned(), page_one_payload),
            (page_two_url.to_owned(), page_two_payload),
        ]);
        let mut fetch_count = 0usize;

        let error = fetch_remote_registry_entries_with_fetcher(
            root_url,
            &mut trust_store,
            false,
            |page_url| {
                fetch_count += 1;
                payloads
                    .remove(page_url.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing fixture for {}", page_url))
            },
        )
        .expect_err("pagination loops must be rejected");

        assert!(
            error.to_string().contains("pagination loop detected"),
            "error should mention pagination loop detection: {error}"
        );
        assert_eq!(fetch_count, 2, "fetch should stop before re-fetching looped page");
    }

    #[test]
    fn remote_registry_fetch_rejects_cross_origin_next_page() {
        let signing_key = SigningKey::from_bytes(&[8_u8; 32]);
        let publisher = "acme-registry";
        let mut trust_store = trust_store_with_registry_key(publisher, &signing_key);

        let root_url = "https://registry.example/catalog/index.json";
        let page_one_payload = sign_registry_index(
            &signing_key,
            publisher,
            SkillRegistryIndex {
                schema_version: REGISTRY_INDEX_SCHEMA_VERSION,
                generated_at_unix_ms: 1_730_000_000_000,
                entries: vec![test_registry_entry(
                    "acme.echo_http",
                    "1.0.0",
                    "../artifacts/acme.echo_http-v1.palyra-skill",
                    '3',
                )],
                next_page: Some("https://evil.example/catalog/page-2.json".to_owned()),
            },
        );
        let mut payloads =
            HashMap::<String, Vec<u8>>::from([(root_url.to_owned(), page_one_payload)]);
        let mut fetched_urls = Vec::<String>::new();

        let error = fetch_remote_registry_entries_with_fetcher(
            root_url,
            &mut trust_store,
            false,
            |page_url| {
                fetched_urls.push(page_url.to_string());
                payloads
                    .remove(page_url.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing fixture for {}", page_url))
            },
        )
        .expect_err("cross-origin pagination targets must be rejected");

        assert!(
            error.to_string().contains("next_page URL"),
            "error should identify next_page validation: {error}"
        );
        assert!(
            error.to_string().contains("must stay on origin"),
            "error should mention same-origin enforcement: {error}"
        );
        assert_eq!(fetched_urls.len(), 1, "only the first page should be fetched");
    }

    #[test]
    fn remote_registry_fetch_rejects_cross_origin_artifact_url() {
        let signing_key = SigningKey::from_bytes(&[9_u8; 32]);
        let publisher = "acme-registry";
        let mut trust_store = trust_store_with_registry_key(publisher, &signing_key);

        let root_url = "https://registry.example/catalog/index.json";
        let page_one_payload = sign_registry_index(
            &signing_key,
            publisher,
            SkillRegistryIndex {
                schema_version: REGISTRY_INDEX_SCHEMA_VERSION,
                generated_at_unix_ms: 1_730_000_000_000,
                entries: vec![test_registry_entry(
                    "acme.echo_http",
                    "1.0.0",
                    "https://evil.example/artifacts/acme.echo_http-v1.palyra-skill",
                    '4',
                )],
                next_page: None,
            },
        );
        let mut payloads =
            HashMap::<String, Vec<u8>>::from([(root_url.to_owned(), page_one_payload)]);

        let error = fetch_remote_registry_entries_with_fetcher(
            root_url,
            &mut trust_store,
            false,
            |page_url| {
                payloads
                    .remove(page_url.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing fixture for {}", page_url))
            },
        )
        .expect_err("cross-origin artifact URLs must be rejected");

        assert!(
            error.to_string().contains("artifact URL"),
            "error should identify artifact URL validation: {error}"
        );
        assert!(
            error.to_string().contains("must stay on origin"),
            "error should mention same-origin enforcement: {error}"
        );
    }

    #[test]
    fn validate_registry_index_rejects_duplicate_skill_version_tuples() {
        let duplicate_entries = vec![
            SkillRegistryEntry {
                skill_id: "acme.echo_http".to_owned(),
                version: "1.0.0".to_owned(),
                publisher: "acme".to_owned(),
                artifact: "echo-http-v1.palyra-skill".to_owned(),
                artifact_sha256: "a".repeat(64),
                artifact_bytes: Some(16),
            },
            SkillRegistryEntry {
                skill_id: "acme.echo_http".to_owned(),
                version: "1.0.0".to_owned(),
                publisher: "acme".to_owned(),
                artifact: "echo-http-v1-duplicate.palyra-skill".to_owned(),
                artifact_sha256: "b".repeat(64),
                artifact_bytes: Some(16),
            },
        ];
        let index = SkillRegistryIndex {
            schema_version: REGISTRY_INDEX_SCHEMA_VERSION,
            generated_at_unix_ms: 1_730_000_000_000,
            entries: duplicate_entries,
            next_page: None,
        };

        let error = validate_registry_index(&index)
            .expect_err("duplicate registry tuples must be rejected");
        assert!(
            error.to_string().contains("duplicate entry"),
            "error should mention duplicate entry: {error}"
        );
    }

    #[test]
    fn fetch_limited_bytes_rejects_payloads_above_limit() {
        let (url, server) = spawn_one_shot_http_server(vec![7_u8; 8 * 1024]);
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("HTTP client should build");

        let error = fetch_limited_bytes(&client, url.as_str(), 1024)
            .expect_err("response over limit should fail");
        assert!(
            error.to_string().contains("exceeds configured limit"),
            "error should mention payload limit: {error}"
        );
        server.join().expect("server thread should exit");
    }

    #[test]
    fn fetch_limited_bytes_accepts_payload_equal_to_limit() {
        let expected = vec![5_u8; 2048];
        let (url, server) = spawn_one_shot_http_server(expected.clone());
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("HTTP client should build");

        let payload =
            fetch_limited_bytes(&client, url.as_str(), expected.len()).expect("fetch should pass");
        assert_eq!(payload, expected);
        server.join().expect("server thread should exit");
    }

    #[test]
    fn normalize_prompt_secret_value_trims_only_trailing_line_endings() {
        assert_eq!(normalize_prompt_secret_value("secret\r\n"), "secret");
        assert_eq!(normalize_prompt_secret_value("secret\n"), "secret");
        assert_eq!(normalize_prompt_secret_value("sec ret"), "sec ret");
    }

    #[test]
    fn process_runner_tier_b_allowlists_report_preflight_only_warning() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
[tool_call.process_runner]
tier = "b"
allowed_egress_hosts = ["api.example.com"]
"#,
        )
        .expect("fixture config should parse");
        assert!(
            !process_runner_tier_b_allowlist_preflight_only(&parsed),
            "tier-b with host allowlists should report preflight-only warning"
        );
    }

    #[test]
    fn process_runner_tier_c_allowlists_do_not_report_preflight_only_warning() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
[tool_call.process_runner]
tier = "c"
allowed_egress_hosts = ["api.example.com"]
"#,
        )
        .expect("fixture config should parse");
        assert!(
            process_runner_tier_b_allowlist_preflight_only(&parsed),
            "tier-c policy should not trigger tier-b preflight-only warning"
        );
    }

    #[test]
    fn process_runner_tier_c_strict_allowlists_report_offline_only_warning() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
[tool_call.process_runner]
tier = "c"
egress_enforcement_mode = "strict"
allowed_egress_hosts = ["api.example.com"]
"#,
        )
        .expect("fixture config should parse");
        assert!(
            !process_runner_tier_c_strict_offline_allowlists_empty(&parsed),
            "tier-c strict mode with host allowlists should be flagged as offline-only mismatch"
        );
    }

    #[test]
    fn process_runner_tier_c_preflight_allowlists_pass_offline_only_check() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
[tool_call.process_runner]
tier = "c"
egress_enforcement_mode = "preflight"
allowed_egress_hosts = ["api.example.com"]
"#,
        )
        .expect("fixture config should parse");
        assert!(
            process_runner_tier_c_strict_offline_allowlists_empty(&parsed),
            "tier-c preflight mode should not trigger strict offline-only warning"
        );
    }

    #[test]
    fn memory_embeddings_model_check_requires_openai_model_when_openai_provider_is_selected() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
[model_provider]
kind = "openai_compatible"
"#,
        )
        .expect("fixture config should parse");
        assert!(
            !memory_embeddings_model_configured(&parsed),
            "openai-compatible provider without embeddings model should fail doctor check"
        );
    }

    #[test]
    fn memory_embeddings_model_check_accepts_configured_model_for_openai_provider() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
[model_provider]
kind = "openai_compatible"
openai_embeddings_model = "text-embedding-3-small"
"#,
        )
        .expect("fixture config should parse");
        assert!(
            memory_embeddings_model_configured(&parsed),
            "doctor check should pass when embeddings model is configured"
        );
    }

    #[test]
    fn process_runner_tier_c_windows_backend_check_tracks_platform_support() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
[tool_call.process_runner]
tier = "c"
"#,
        )
        .expect("fixture config should parse");
        let expected = !cfg!(windows);
        assert_eq!(
            process_runner_tier_c_windows_backend_supported(&parsed),
            expected,
            "windows backend doctor check should fail only on windows tier-c configs"
        );
    }

    #[test]
    fn config_get_masks_secret_values_when_show_secrets_is_disabled() {
        let value = toml::Value::String("vault://global/openai".to_owned());
        let rendered = super::format_config_get_display_value(
            "model_provider.openai_api_key_vault_ref",
            &value,
            false,
        );
        assert!(
            rendered.contains(super::REDACTED_CONFIG_VALUE),
            "secret value should be redacted in config.get output"
        );
    }

    #[test]
    fn config_get_keeps_non_secret_values_visible() {
        let value = toml::Value::Integer(7443);
        let rendered = super::format_config_get_display_value("gateway.grpc_port", &value, false);
        assert_eq!(rendered, "7443");
    }

    #[test]
    fn redact_channel_router_preview_session_key_redacts_nested_preview_payload() {
        let mut payload = serde_json::json!({
            "preview": {
                "accepted": true,
                "session_key": "session-123",
            }
        });
        super::redact_channel_router_preview_session_key(&mut payload);
        assert_eq!(
            payload
                .get("preview")
                .and_then(serde_json::Value::as_object)
                .and_then(|preview| preview.get("session_key"))
                .and_then(serde_json::Value::as_str),
            Some(super::REDACTED),
            "session_key should be redacted in nested preview payloads"
        );
    }

    #[test]
    fn redact_channel_router_preview_session_key_redacts_top_level_payload() {
        let mut payload = serde_json::json!({
            "accepted": true,
            "session_key": "session-123",
        });
        super::redact_channel_router_preview_session_key(&mut payload);
        assert_eq!(
            payload.get("session_key").and_then(serde_json::Value::as_str),
            Some(super::REDACTED),
            "session_key should be redacted when preview is represented as top-level payload"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn collect_skill_artifact_paths_skips_symlink_cycles() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let root = tempdir.path();
        std::fs::create_dir_all(root.join("nested")).expect("nested directory should be created");
        symlink(root, root.join("nested").join("loop-to-root"))
            .expect("symlink loop should be created");

        let mut artifact_paths = Vec::new();
        let mut visited_dirs = std::collections::HashSet::new();
        super::collect_skill_artifact_paths(root, root, &mut artifact_paths, &mut visited_dirs)
            .expect("collector should skip symlink loops");
        assert!(artifact_paths.is_empty(), "no artifacts should be discovered");
    }

    #[test]
    fn write_file_atomically_replaces_existing_file_contents() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let target = tempdir.path().join("index.json");
        std::fs::write(target.as_path(), b"{\"old\":true}").expect("seed file should be written");

        write_file_atomically(target.as_path(), b"{\"new\":true}")
            .expect("atomic write should succeed");
        let payload =
            std::fs::read_to_string(target.as_path()).expect("replacement file should be readable");
        assert_eq!(payload, "{\"new\":true}");
    }

    #[test]
    fn signed_registry_index_verification_rejects_payload_hash_mismatch() {
        let signing_key = SigningKey::from_bytes(&[9_u8; 32]);
        let verifying_key = VerifyingKey::from(&signing_key);
        let index = SkillRegistryIndex {
            schema_version: REGISTRY_INDEX_SCHEMA_VERSION,
            generated_at_unix_ms: 1_730_000_000_000,
            entries: vec![SkillRegistryEntry {
                skill_id: "acme.echo_http".to_owned(),
                version: "1.0.0".to_owned(),
                publisher: "acme".to_owned(),
                artifact: "acme.echo_http.palyra-skill".to_owned(),
                artifact_sha256: "0".repeat(64),
                artifact_bytes: Some(128),
            }],
            next_page: None,
        };
        let payload_sha256 =
            sha256_hex(serde_json::to_vec(&index).expect("index should serialize").as_slice());
        let signature = signing_key.sign(payload_sha256.as_bytes());
        let mut signed = SignedSkillRegistryIndex {
            schema_version: REGISTRY_SIGNED_INDEX_SCHEMA_VERSION,
            index,
            signature: RegistrySignature {
                algorithm: REGISTRY_SIGNATURE_ALGORITHM.to_owned(),
                publisher: "acme-registry".to_owned(),
                key_id: registry_key_id_for(&verifying_key),
                public_key_base64: BASE64_STANDARD.encode(verifying_key.as_bytes()),
                payload_sha256: payload_sha256.clone(),
                signature_base64: BASE64_STANDARD.encode(signature.to_bytes()),
                signed_at_unix_ms: 1_730_000_000_123,
            },
        };
        signed.signature.payload_sha256 = "f".repeat(64);
        let payload = serde_json::to_vec(&signed).expect("signed index should serialize");

        let mut store = SkillTrustStore::default();
        let mut key_hex = String::with_capacity(64);
        for byte in verifying_key.as_bytes() {
            key_hex.push_str(format!("{byte:02x}").as_str());
        }
        store
            .add_trusted_key("acme-registry", key_hex.as_str())
            .expect("trusted key should be accepted");
        let error =
            parse_and_verify_signed_remote_registry_index(payload.as_slice(), &mut store, false)
                .expect_err("hash mismatch should fail");
        assert!(
            error.to_string().contains("payload hash mismatch"),
            "error should mention hash mismatch: {error}"
        );
    }

    #[test]
    fn normalize_installed_index_keeps_only_one_current_version() {
        let mut index = InstalledSkillsIndex {
            schema_version: 1,
            updated_at_unix_ms: 0,
            entries: vec![
                InstalledSkillRecord {
                    skill_id: "acme.echo_http".to_owned(),
                    version: "1.0.0".to_owned(),
                    publisher: "acme".to_owned(),
                    current: true,
                    installed_at_unix_ms: 1,
                    artifact_sha256: "0".repeat(64),
                    payload_sha256: "1".repeat(64),
                    signature_key_id: "ed25519:0011223344556677".to_owned(),
                    trust_decision: "allowlisted".to_owned(),
                    source: InstalledSkillSource {
                        kind: "local_artifact".to_owned(),
                        reference: "a".to_owned(),
                    },
                    missing_secrets: Vec::new(),
                },
                InstalledSkillRecord {
                    skill_id: "acme.echo_http".to_owned(),
                    version: "1.1.0".to_owned(),
                    publisher: "acme".to_owned(),
                    current: true,
                    installed_at_unix_ms: 2,
                    artifact_sha256: "2".repeat(64),
                    payload_sha256: "3".repeat(64),
                    signature_key_id: "ed25519:8899aabbccddeeff".to_owned(),
                    trust_decision: "allowlisted".to_owned(),
                    source: InstalledSkillSource {
                        kind: "local_artifact".to_owned(),
                        reference: "b".to_owned(),
                    },
                    missing_secrets: Vec::new(),
                },
            ],
        };

        normalize_installed_skills_index(&mut index);
        let current_versions = index
            .entries
            .iter()
            .filter(|entry| entry.skill_id == "acme.echo_http" && entry.current)
            .map(|entry| entry.version.clone())
            .collect::<Vec<_>>();
        assert_eq!(current_versions, vec!["1.1.0".to_owned()]);
    }
}

#[cfg(test)]
mod init_command_tests {
    use std::path::PathBuf;

    use super::{build_init_config_document, InitMode, DEFAULT_ADMIN_BOUND_PRINCIPAL};

    fn read_string(document: &toml::Value, key: &str) -> Option<String> {
        let mut cursor = document;
        for segment in key.split('.') {
            cursor = cursor.get(segment)?;
        }
        cursor.as_str().map(ToOwned::to_owned)
    }

    fn read_bool(document: &toml::Value, key: &str) -> Option<bool> {
        let mut cursor = document;
        for segment in key.split('.') {
            cursor = cursor.get(segment)?;
        }
        cursor.as_bool()
    }

    #[test]
    fn local_init_document_uses_loopback_defaults() {
        let document = build_init_config_document(
            InitMode::LocalDesktop,
            PathBuf::from("state/identity").as_path(),
            PathBuf::from("state/vault").as_path(),
            "token-local",
            None,
        )
        .expect("local init document should build");

        assert_eq!(read_string(&document, "deployment.mode").as_deref(), Some("local_desktop"));
        assert_eq!(
            read_string(&document, "gateway.bind_profile").as_deref(),
            Some("loopback_only")
        );
        assert_eq!(read_string(&document, "admin.auth_token").as_deref(), Some("token-local"));
        assert_eq!(
            read_string(&document, "admin.bound_principal").as_deref(),
            Some(DEFAULT_ADMIN_BOUND_PRINCIPAL)
        );
        assert_eq!(read_bool(&document, "admin.require_auth"), Some(true));
        assert_eq!(read_bool(&document, "gateway.tls.enabled"), None);
    }

    #[test]
    fn remote_init_document_includes_tls_scaffold_paths_when_requested() {
        let tls_paths = (PathBuf::from("tls/gateway.crt"), PathBuf::from("tls/gateway.key"));
        let document = build_init_config_document(
            InitMode::RemoteVps,
            PathBuf::from("state/identity").as_path(),
            PathBuf::from("state/vault").as_path(),
            "token-remote",
            Some(&tls_paths),
        )
        .expect("remote init document should build");

        assert_eq!(read_string(&document, "deployment.mode").as_deref(), Some("remote_vps"));
        assert_eq!(read_bool(&document, "gateway.tls.enabled"), Some(false));
        assert_eq!(
            read_string(&document, "gateway.tls.cert_path").as_deref(),
            Some("tls/gateway.crt")
        );
        assert_eq!(
            read_string(&document, "gateway.tls.key_path").as_deref(),
            Some("tls/gateway.key")
        );
    }
}

#[cfg(test)]
mod diagnostics_bundle_tests {
    use super::{
        encode_support_bundle_with_cap, extract_support_bundle_error_message, DoctorAccessSnapshot,
        DoctorBrowserSnapshot, DoctorConfigSnapshot, DoctorConnectivityProbe,
        DoctorConnectivitySnapshot, DoctorDeploymentBindSnapshot, DoctorDeploymentSnapshot,
        DoctorIdentitySnapshot, DoctorProviderAuthSnapshot, DoctorReport, DoctorSandboxSnapshot,
        DoctorSummary, SkillsInventorySnapshot, SupportBundle, SupportBundleBuildSnapshot,
        SupportBundleConfigSnapshot, SupportBundleDiagnosticsSnapshot,
        SupportBundleJournalErrorRecord, SupportBundleJournalSnapshot,
        SupportBundleObservabilitySnapshot, SupportBundleTriageSnapshot,
    };
    use serde_json::{json, Value};
    use std::collections::BTreeMap;

    fn minimal_doctor_report() -> DoctorReport {
        DoctorReport {
            generated_at_unix_ms: 1_730_000_000_000,
            profile: Some(crate::app::ActiveProfileContext {
                name: "staging".to_owned(),
                label: "Staging".to_owned(),
                environment: "staging".to_owned(),
                color: "amber".to_owned(),
                risk_level: "elevated".to_owned(),
                strict_mode: true,
                mode: "remote".to_owned(),
            }),
            checks: Vec::new(),
            summary: DoctorSummary {
                required_checks_total: 2,
                required_checks_ok: 2,
                required_checks_failed: 0,
                warning_checks_failed: 0,
                info_checks_failed: 0,
            },
            config: DoctorConfigSnapshot {
                path: Some("palyra.toml".to_owned()),
                exists: true,
                parsed: true,
                error: None,
            },
            identity: DoctorIdentitySnapshot {
                store_root: Some("state/identity".to_owned()),
                exists: true,
                writable: true,
                error: None,
            },
            connectivity: DoctorConnectivitySnapshot {
                daemon_url: "http://127.0.0.1:7142".to_owned(),
                grpc_url: "http://127.0.0.1:7443".to_owned(),
                http: DoctorConnectivityProbe { ok: true, message: None },
                grpc: DoctorConnectivityProbe { ok: true, message: None },
                admin: DoctorConnectivityProbe { ok: true, message: None },
            },
            provider_auth: DoctorProviderAuthSnapshot {
                fetched: true,
                model_provider: Some(json!({ "kind": "openai-compatible" })),
                auth_summary: Some(json!({ "total_profiles": 1 })),
                error: None,
            },
            browser: DoctorBrowserSnapshot {
                configured_enabled: true,
                auth_token_configured: true,
                endpoint: "http://127.0.0.1:7543".to_owned(),
                connect_timeout_ms: Some(1500),
                request_timeout_ms: Some(15000),
                max_screenshot_bytes: Some(262_144),
                max_title_bytes: Some(4096),
                state_dir_configured: false,
                state_key_vault_ref_configured: false,
                diagnostics_fetched: true,
                health_status: Some("ok".to_owned()),
                active_sessions: Some(1),
                recent_relay_action_failures: Some(0),
                recent_health_failures: Some(0),
                error: None,
            },
            access: DoctorAccessSnapshot {
                registry_path: Some("state/access_registry.json".to_owned()),
                registry_exists: true,
                parsed: true,
                compat_api_enabled: false,
                api_tokens_enabled: false,
                team_mode_enabled: false,
                rbac_enabled: false,
                staged_rollout_enabled: false,
                backfill_required: false,
                blocking_issues: 0,
                warning_issues: 0,
                external_api_safe_mode: true,
                team_mode_safe_mode: true,
                error: None,
            },
            skills: SkillsInventorySnapshot {
                skills_root: "state/skills".to_owned(),
                installed_total: 1,
                current_total: 1,
                eligible_total: 1,
                quarantined_total: 0,
                disabled_total: 0,
                runtime_unknown_total: 0,
                missing_secrets_total: 0,
                publishers: vec!["acme".to_owned()],
                trust_decisions: BTreeMap::from([("allowlisted".to_owned(), 1)]),
                error: None,
            },
            sandbox: DoctorSandboxSnapshot {
                tier_b_egress_allowlists_preflight_only: true,
                tier_c_strict_offline_only: true,
                tier_c_windows_backend_supported: true,
            },
            deployment: DoctorDeploymentSnapshot {
                mode: "local_desktop".to_owned(),
                bind_profile: "loopback_only".to_owned(),
                binds: DoctorDeploymentBindSnapshot {
                    admin: "127.0.0.1:7142".to_owned(),
                    grpc: "127.0.0.1:7443".to_owned(),
                    quic: "127.0.0.1:7444".to_owned(),
                },
                gateway_tls_enabled: false,
                admin_auth_required: true,
                admin_token_configured: true,
                dangerous_remote_bind_ack_config: false,
                dangerous_remote_bind_ack_env: false,
                remote_bind_detected: false,
                warnings: Vec::new(),
            },
        }
    }

    fn oversized_bundle() -> SupportBundle {
        let mut hashes = Vec::new();
        for index in 0..128 {
            hashes.push(format!("{index:064x}"));
        }
        let mut errors = Vec::new();
        for index in 0..64 {
            errors.push(SupportBundleJournalErrorRecord {
                event_id: format!("01ARZ3NDEKTSV4RRFFQ69G{index:05}"),
                kind: 2,
                timestamp_unix_ms: 1_730_000_000_000 + index as i64,
                message: format!("provider error token=<redacted> index={index}"),
            });
        }

        SupportBundle {
            schema_version: 1,
            generated_at_unix_ms: 1_730_000_000_000,
            profile: Some(crate::app::ActiveProfileContext {
                name: "staging".to_owned(),
                label: "Staging".to_owned(),
                environment: "staging".to_owned(),
                color: "amber".to_owned(),
                risk_level: "elevated".to_owned(),
                strict_mode: true,
                mode: "remote".to_owned(),
            }),
            build: SupportBundleBuildSnapshot {
                version: "0.1.0".to_owned(),
                git_hash: "deadbeef".to_owned(),
                build_profile: "debug".to_owned(),
            },
            platform: super::SupportBundlePlatformSnapshot {
                os: "linux".to_owned(),
                family: "unix".to_owned(),
                arch: "x86_64".to_owned(),
            },
            doctor: minimal_doctor_report(),
            recovery: Some(json!({
                "schema_version": 1,
                "mode": "repair_preview",
                "planned_steps": [
                    {
                        "id": "config.schema_version",
                        "kind": "config_version_migration"
                    }
                ]
            })),
            config: SupportBundleConfigSnapshot {
                path: Some("palyra.toml".to_owned()),
                redacted_document: Some(json!({
                    "model_provider": {
                        "openai_api_key": "<redacted>",
                        "openai_api_key_vault_ref": "<redacted>",
                        "huge": "x".repeat(24_000),
                    }
                })),
                fingerprint_sha256: Some("f".repeat(64)),
                error: None,
            },
            observability: SupportBundleObservabilitySnapshot {
                summary: Some(json!({
                    "provider_auth": { "attempts": 4, "failures": 1, "failure_rate_bps": 2500 },
                    "recent_failures": [
                        { "operation": "provider_auth.oauth_refresh", "message": "http 503" }
                    ]
                })),
                recent_failures: Some(json!([
                    { "operation": "provider_auth.oauth_refresh", "message": "http 503" }
                ])),
            },
            triage: SupportBundleTriageSnapshot {
                playbook:
                    "docs-codebase/docs-tree/web_console_operator_dashboard/console_sections_and_navigation/support_recovery.md"
                        .to_owned(),
                failure_classes: vec![
                    "config_failure".to_owned(),
                    "upstream_provider_failure".to_owned(),
                    "product_failure".to_owned(),
                ],
                common_order: vec!["Check deployment posture and operator auth first.".to_owned()],
            },
            diagnostics: SupportBundleDiagnosticsSnapshot {
                gateway_health: Some(json!({
                    "status": "ok",
                    "service": "palyrad",
                })),
                service_status: None,
                browser_status: None,
                node_status: None,
                admin_status: Some(json!({
                    "model_provider": {
                        "kind": "openai-compatible",
                        "runtime_metrics": {
                            "request_count": 12345
                        }
                    }
                })),
                admin_status_error: None,
                skills: SkillsInventorySnapshot {
                    skills_root: "state/skills".to_owned(),
                    installed_total: 1,
                    current_total: 1,
                    eligible_total: 1,
                    quarantined_total: 0,
                    disabled_total: 0,
                    runtime_unknown_total: 0,
                    missing_secrets_total: 0,
                    publishers: vec!["acme".to_owned()],
                    trust_decisions: BTreeMap::from([("allowlisted".to_owned(), 1)]),
                    error: None,
                },
            },
            journal: SupportBundleJournalSnapshot {
                db_path: "data/journal.sqlite3".to_owned(),
                available: true,
                hash_chain_enabled: true,
                latest_hash: Some("f".repeat(64)),
                recent_hashes: hashes,
                last_errors: errors,
                error: None,
            },
            truncated: false,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn support_bundle_error_extraction_redacts_secret_values() {
        let payload = r#"{
            "event":"auth.refresh.failed",
            "error":"Bearer topsecret token=abc123",
            "details":{"reason":"refresh_token=qwerty"}
        }"#;
        let extracted = extract_support_bundle_error_message(payload)
            .expect("error payload should produce a support bundle error message");
        assert!(
            extracted.contains("<redacted>"),
            "extracted error message should include redaction marker: {extracted}"
        );
        assert!(
            !extracted.contains("topsecret")
                && !extracted.contains("abc123")
                && !extracted.contains("qwerty"),
            "extracted error message must not leak raw secret values: {extracted}"
        );
    }

    #[test]
    fn support_bundle_error_extraction_redacts_url_query_tokens() {
        let payload = r#"{
            "event":"auth.refresh.failed",
            "error":"callback failed https://example.test/callback?state=ok&access_token=alpha"
        }"#;
        let extracted = extract_support_bundle_error_message(payload)
            .expect("error payload should produce a support bundle error message");
        assert!(
            extracted.contains("state=ok") && extracted.contains("access_token=<redacted>"),
            "embedded URL query tokens should be redacted: {extracted}"
        );
        assert!(
            !extracted.contains("access_token=alpha"),
            "embedded URL query token must not leak: {extracted}"
        );
    }

    #[test]
    fn support_bundle_redacts_browser_and_connector_diagnostics_payloads() {
        let mut payload = serde_json::json!({
            "browserd": {
                "relay_token": "relay-secret",
                "downloads_endpoint": "https://example.test/downloads?token=abc123&mode=ok",
                "last_error": "Bearer browser-secret"
            },
            "channels": {
                "discord:default": {
                    "runtime": {
                        "last_error": "authorization=discord-secret"
                    },
                    "webhook_url": "https://discord.test/api/webhooks/1?token=def456&mode=ok"
                }
            },
            "auth_profiles": {
                "profiles": [
                    {
                        "access_token": "oauth-secret",
                        "refresh_failure_reason": "refresh_token=qwerty"
                    }
                ]
            }
        });
        super::redact_json_value_tree(&mut payload, None);
        assert_eq!(
            payload.pointer("/browserd/relay_token").and_then(Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            payload.pointer("/auth_profiles/profiles/0/access_token").and_then(Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            payload.pointer("/browserd/downloads_endpoint").and_then(Value::as_str),
            Some("https://example.test/downloads?token=<redacted>&mode=ok")
        );
        assert_eq!(
            payload.pointer("/channels/discord:default/webhook_url").and_then(Value::as_str),
            Some("https://discord.test/api/webhooks/1?token=<redacted>&mode=ok")
        );
        let browser_error =
            payload.pointer("/browserd/last_error").and_then(Value::as_str).unwrap_or_default();
        assert!(
            browser_error.contains("<redacted>") && !browser_error.contains("browser-secret"),
            "browser diagnostics error should stay redacted: {browser_error}"
        );
        let connector_error = payload
            .pointer("/channels/discord:default/runtime/last_error")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            connector_error.contains("<redacted>") && !connector_error.contains("discord-secret"),
            "connector diagnostics error should stay redacted: {connector_error}"
        );
    }

    #[test]
    fn support_bundle_redacts_console_session_and_oauth_boundary_material() {
        let mut payload = serde_json::json!({
            "console": {
                "csrf_token": "csrf-secret",
                "session_cookie": "session=alpha",
                "redirect_location": "https://dashboard.example.test/callback?access_token=oauth-secret&mode=ok",
                "last_error": "Bearer browser-secret set-cookie=session=alpha refresh_token=qwerty"
            }
        });
        super::redact_json_value_tree(&mut payload, None);
        assert_eq!(
            payload.pointer("/console/csrf_token").and_then(Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            payload.pointer("/console/session_cookie").and_then(Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            payload.pointer("/console/redirect_location").and_then(Value::as_str),
            Some("https://dashboard.example.test/callback?access_token=<redacted>&mode=ok")
        );
        let error =
            payload.pointer("/console/last_error").and_then(Value::as_str).unwrap_or_default();
        assert!(
            error.contains("<redacted>")
                && !error.contains("browser-secret")
                && !error.contains("session=alpha")
                && !error.contains("qwerty"),
            "console auth/session diagnostics must stay redacted: {error}"
        );
    }

    #[test]
    fn support_bundle_size_cap_trims_payload() {
        let mut bundle = oversized_bundle();
        let encoded = encode_support_bundle_with_cap(&mut bundle, 4096)
            .expect("support bundle should be encoded with cap");
        assert!(encoded.len() <= 4096, "encoded bundle should fit within size cap");
        let parsed: Value = serde_json::from_slice(encoded.as_slice())
            .expect("trimmed support bundle must remain JSON");
        assert_eq!(
            parsed.get("truncated").and_then(Value::as_bool),
            Some(true),
            "trimmed support bundle should mark truncated=true"
        );
    }
}

#[cfg(test)]
mod profile_guardrail_tests {
    use super::is_strict_profile_blocked_command;
    use crate::cli::{
        Command as CliCommand, ProfileCommand, ResetCommand, ResetScopeArg, UninstallCommand,
    };

    #[test]
    fn strict_profile_guard_blocks_destructive_commands_only() {
        assert!(is_strict_profile_blocked_command(&CliCommand::Reset {
            command: ResetCommand {
                scopes: vec![ResetScopeArg::State],
                config_path: None,
                workspace_root: None,
                dry_run: false,
                yes: true,
            },
        }));
        assert!(is_strict_profile_blocked_command(&CliCommand::Uninstall {
            command: UninstallCommand {
                install_root: None,
                remove_state: true,
                dry_run: false,
                yes: true,
            },
        }));
        assert!(is_strict_profile_blocked_command(&CliCommand::Profile {
            command: ProfileCommand::Delete {
                name: "prod".to_owned(),
                yes: true,
                delete_state_root: false,
                json: false,
            },
        }));
        assert!(!is_strict_profile_blocked_command(&CliCommand::Reset {
            command: ResetCommand {
                scopes: vec![ResetScopeArg::State],
                config_path: None,
                workspace_root: None,
                dry_run: true,
                yes: false,
            },
        }));
    }
}

#[cfg(test)]
mod doctor_check_tests {
    use super::{
        doctor_repo_scaffold_check, looks_like_repo_root, repo_checkout_detected_from_binary_path,
        DoctorSeverity,
    };
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn repo_checkout_detection_finds_workspace_root_from_built_binary_path() {
        let temp = tempdir().expect("temp dir");
        for entry in ["crates", "apps", "schemas"] {
            fs::create_dir_all(temp.path().join(entry)).expect("repo marker should be created");
        }
        fs::write(temp.path().join("Cargo.toml"), "[workspace]\nmembers = []\n")
            .expect("cargo manifest should be created");
        let binary_path = temp.path().join("target").join("release").join(if cfg!(windows) {
            "palyra.exe"
        } else {
            "palyra"
        });
        fs::create_dir_all(binary_path.parent().expect("binary parent")).expect("binary dir");
        fs::write(binary_path.as_path(), []).expect("binary placeholder should be created");

        assert!(looks_like_repo_root(temp.path()));
        assert!(repo_checkout_detected_from_binary_path(binary_path.as_path()));
    }

    #[test]
    fn repo_scaffold_check_is_informational_for_installed_artifacts_outside_repo() {
        let check = doctor_repo_scaffold_check(false, false);

        assert!(!check.ok);
        assert_eq!(check.severity, DoctorSeverity::Info);
        assert!(!check.required);
    }
}

#[cfg(all(test, not(windows)))]
mod tests {
    use super::resolve_pairing_proof;
    use anyhow::Result;
    use palyra_common::default_identity_store_root_from_env;
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
