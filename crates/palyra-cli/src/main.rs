mod acp_bridge;
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

        pub mod auth {
            pub mod v1 {
                tonic::include_proto!("palyra.auth.v1");
            }
        }
    }
}

#[cfg(not(windows))]
use std::sync::Arc;
use std::{
    collections::HashSet,
    env, fs,
    io::{BufRead, IsTerminal, Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Component, Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use clap::{CommandFactory, Parser};
use cli::{
    AgentCommand, AgentsCommand, ApprovalDecisionArg, ApprovalExportFormatArg, ApprovalsCommand,
    AuthCommand, AuthCredentialArg, AuthProfilesCommand, AuthProviderArg, AuthScopeArg,
    BrowserCommand, ChannelsCommand, Cli, Command as CliCommand, CompletionShell, ConfigCommand,
    CronCommand, CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg,
    DaemonCommand, JournalCheckpointModeArg, MemoryCommand, MemoryScopeArg, MemorySourceArg,
    OnboardingCommand, PatchCommand, PolicyCommand, ProtocolCommand, SecretsCommand, SkillsCommand,
    SkillsPackageCommand, SupportBundleCommand,
};
#[cfg(not(windows))]
use cli::{PairingClientKindArg, PairingCommand, PairingMethodArg};
use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use palyra_common::default_identity_store_root;
use palyra_common::{
    build_metadata,
    config_system::{
        backup_path, format_toml_value, get_value_at_path, parse_document_with_migration,
        parse_toml_value_literal, recover_config_from_backup, set_value_at_path,
        unset_value_at_path, write_document_with_backups, ConfigMigrationInfo,
    },
    daemon_config_schema::{is_secret_config_path, redact_secret_config_values, RootFileConfig},
    default_config_search_paths, parse_config_path, parse_daemon_bind_socket,
    redaction::{is_sensitive_key, redact_auth_error, redact_url, REDACTED},
    validate_canonical_id,
    workspace_patch::{
        apply_workspace_patch, compute_patch_sha256, redact_patch_preview, WorkspacePatchLimits,
        WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
    },
    HealthResponse, CANONICAL_JSON_ENVELOPE_VERSION, CANONICAL_PROTOCOL_MAJOR,
};
use palyra_identity::DeviceIdentity;
use palyra_identity::FilesystemSecretStore;
#[cfg(not(windows))]
use palyra_identity::{
    IdentityManager, PairingClientKind, PairingMethod, SecretStore, DEFAULT_CERT_VALIDITY,
};
use palyra_policy::{evaluate_with_config, PolicyDecision, PolicyEvaluationConfig, PolicyRequest};
use palyra_skills::{
    audit_skill_artifact_security, build_signed_skill_artifact, inspect_skill_artifact,
    parse_ed25519_signing_key, verify_skill_artifact, ArtifactFile, SkillArtifactBuildRequest,
    SkillAuditCheckStatus, SkillSecurityAuditPolicy, SkillTrustStore, TrustDecision,
};
use palyra_vault::{
    BackendPreference as VaultBackendPreference, Vault, VaultConfig as VaultConfigOptions,
    VaultError, VaultRef, VaultScope,
};
use reqwest::blocking::Client;
use reqwest::Url;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::sleep;
use tokio_stream::{iter, StreamExt};
use tonic::Request;
use ulid::Ulid;

use crate::proto::palyra::{
    auth::v1 as auth_v1, common::v1 as common_v1, cron::v1 as cron_v1, gateway::v1 as gateway_v1,
    memory::v1 as memory_v1,
};

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
const DEFAULT_JOURNAL_DB_PATH: &str = "data/journal.sqlite3";
const DEFAULT_BROWSER_URL: &str = "http://127.0.0.1:7143";
const DEFAULT_CHANNEL: &str = "cli";
const DEFAULT_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const REDACTED_CONFIG_VALUE: &str = "<redacted>";
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
const TRUST_STORE_INTEGRITY_VAULT_SCOPE: VaultScope = VaultScope::Global;
const TRUST_STORE_INTEGRITY_VAULT_KEY_PREFIX: &str = "skills.trust_store.integrity.";

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        CliCommand::Version => print_version(),
        CliCommand::Doctor { strict, json } => run_doctor(strict, json),
        CliCommand::Status { url, grpc_url, admin, token, principal, device_id, channel } => {
            run_status(url, grpc_url, admin, token, principal, device_id, channel)
        }
        CliCommand::Agent { command } => run_agent(command),
        CliCommand::Agents { command } => run_agents(command),
        CliCommand::Cron { command } => run_cron(command),
        CliCommand::Memory { command } => run_memory(command),
        CliCommand::Approvals { command } => run_approvals(command),
        CliCommand::Auth { command } => run_auth(command),
        CliCommand::Channels { command } => run_channels(command),
        CliCommand::Browser { command } => run_browser(command),
        CliCommand::Completion { shell } => run_completion(shell),
        CliCommand::Onboarding { command } => run_onboarding(command),
        CliCommand::Daemon { command } => run_daemon(command),
        CliCommand::SupportBundle { command } => run_support_bundle(command),
        CliCommand::Policy { command } => run_policy(command),
        CliCommand::Protocol { command } => run_protocol(command),
        CliCommand::Config { command } => run_config(command),
        CliCommand::Patch { command } => run_patch(command),
        CliCommand::Skills { command } => run_skills(command),
        CliCommand::Secrets { command } => run_secrets(command),
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

fn run_doctor(strict: bool, json: bool) -> Result<()> {
    let checks = build_doctor_checks();
    let report = build_doctor_report(checks.as_slice())?;

    if json {
        let encoded = serde_json::to_string_pretty(&report)
            .context("failed to serialize doctor JSON report")?;
        println!("{encoded}");
    } else {
        for check in &checks {
            println!("doctor.{}={} required={}", check.key, check.ok, check.required);
        }
        println!(
            "doctor.config path={} exists={} parsed={}",
            report.config.path.as_deref().unwrap_or("none"),
            report.config.exists,
            report.config.parsed
        );
        println!(
            "doctor.identity root={} exists={} writable={}",
            report.identity.store_root.as_deref().unwrap_or("unavailable"),
            report.identity.exists,
            report.identity.writable
        );
        println!(
            "doctor.connectivity daemon_url={} http_ok={} grpc_ok={} admin_ok={}",
            report.connectivity.daemon_url,
            report.connectivity.http.ok,
            report.connectivity.grpc.ok,
            report.provider_auth.fetched
        );
        println!(
            "doctor.sandbox tier_b_preflight_only={} tier_c_strict_offline={} tier_c_windows_backend_supported={}",
            report.sandbox.tier_b_egress_allowlists_preflight_only,
            report.sandbox.tier_c_strict_offline_only,
            report.sandbox.tier_c_windows_backend_supported
        );
    }

    if strict {
        let failing_required = checks.iter().find(|check| check.required && !check.ok);
        if let Some(check) = failing_required {
            anyhow::bail!("strict doctor failed: {}", check.key);
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn build_doctor_checks() -> Vec<DoctorCheck> {
    vec![
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
            key: "process_runner_tier_b_egress_allowlists_preflight_only",
            ok: process_runner_tier_b_allowlist_config_ok(),
            required: false,
        },
        DoctorCheck {
            key: "process_runner_tier_c_strict_offline_only",
            ok: process_runner_tier_c_strict_offline_config_ok(),
            required: false,
        },
        DoctorCheck {
            key: "process_runner_tier_c_windows_backend_supported",
            ok: process_runner_tier_c_windows_backend_config_ok(),
            required: cfg!(windows),
        },
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
    ]
}

fn build_doctor_report(checks: &[DoctorCheck]) -> Result<DoctorReport> {
    let generated_at_unix_ms = now_unix_ms_i64()?;
    let config = collect_doctor_config_snapshot();
    let identity = collect_doctor_identity_snapshot();
    let (connectivity, admin_payload, admin_error) = collect_doctor_connectivity_snapshot();
    let provider_auth = collect_doctor_provider_auth_snapshot(admin_payload, admin_error);

    let required_checks_total = checks.iter().filter(|check| check.required).count();
    let required_checks_ok = checks.iter().filter(|check| check.required && check.ok).count();
    let required_checks_failed = required_checks_total.saturating_sub(required_checks_ok);

    Ok(DoctorReport {
        generated_at_unix_ms,
        checks: checks.to_vec(),
        summary: DoctorSummary {
            required_checks_total,
            required_checks_ok,
            required_checks_failed,
        },
        config,
        identity,
        connectivity,
        provider_auth,
        sandbox: DoctorSandboxSnapshot {
            tier_b_egress_allowlists_preflight_only: process_runner_tier_b_allowlist_config_ok(),
            tier_c_strict_offline_only: process_runner_tier_c_strict_offline_config_ok(),
            tier_c_windows_backend_supported: process_runner_tier_c_windows_backend_config_ok(),
        },
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
    admin_payload: Option<Value>,
    admin_error: Option<String>,
) -> DoctorProviderAuthSnapshot {
    let Some(payload) = admin_payload else {
        return DoctorProviderAuthSnapshot {
            fetched: false,
            model_provider: None,
            auth_summary: None,
            error: admin_error,
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
    DoctorProviderAuthSnapshot { fetched: true, model_provider, auth_summary, error: admin_error }
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

fn redact_url_segments_in_text(raw: &str) -> String {
    let mut output = String::with_capacity(raw.len());
    for (index, token) in raw.split_whitespace().enumerate() {
        if index > 0 {
            output.push(' ');
        }
        if token.contains("://") {
            output.push_str(redact_url(token).as_str());
        } else {
            output.push_str(token);
        }
    }
    output
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

fn run_support_bundle(command: SupportBundleCommand) -> Result<()> {
    match command {
        SupportBundleCommand::Export { output, max_bytes, journal_hash_limit, error_limit } => {
            run_support_bundle_export(output, max_bytes, journal_hash_limit, error_limit)
        }
    }
}

fn run_support_bundle_export(
    output: Option<String>,
    max_bytes: usize,
    journal_hash_limit: usize,
    error_limit: usize,
) -> Result<()> {
    if max_bytes < 2_048 {
        anyhow::bail!("support-bundle max-bytes must be at least 2048");
    }
    let generated_at_unix_ms = now_unix_ms_i64()?;
    let checks = build_doctor_checks();
    let doctor = build_doctor_report(checks.as_slice())?;
    let output_path = resolve_support_bundle_output_path(output, generated_at_unix_ms);

    let build = build_metadata();
    let mut bundle = SupportBundle {
        schema_version: 1,
        generated_at_unix_ms,
        build: SupportBundleBuildSnapshot {
            version: build.version.to_owned(),
            git_hash: build.git_hash.to_owned(),
            build_profile: build.build_profile.to_owned(),
        },
        platform: SupportBundlePlatformSnapshot {
            os: std::env::consts::OS.to_owned(),
            family: std::env::consts::FAMILY.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
        },
        doctor,
        config: build_support_bundle_config_snapshot(),
        diagnostics: build_support_bundle_diagnostics_snapshot(),
        journal: build_support_bundle_journal_snapshot(journal_hash_limit, error_limit),
        truncated: false,
        warnings: Vec::new(),
    };

    let encoded = encode_support_bundle_with_cap(&mut bundle, max_bytes)?;
    if let Some(parent) = output_path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create support-bundle directory {}", parent.display())
        })?;
    }
    fs::write(output_path.as_path(), encoded.as_slice())
        .with_context(|| format!("failed to write support bundle {}", output_path.display()))?;
    println!(
        "support_bundle.export path={} bytes={} truncated={} warnings={}",
        output_path.display(),
        encoded.len(),
        bundle.truncated,
        bundle.warnings.len()
    );
    std::io::stdout().flush().context("stdout flush failed")
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
        return SupportBundleConfigSnapshot { path, redacted_document: None, error: None };
    };

    let path_ref = PathBuf::from(path_value.as_str());
    if !path_ref.exists() {
        return SupportBundleConfigSnapshot {
            path,
            redacted_document: None,
            error: Some("config path does not exist".to_owned()),
        };
    }

    match load_document_from_existing_path(path_ref.as_path()) {
        Ok((mut document, _)) => {
            redact_secret_config_values(&mut document);
            match serde_json::to_value(document) {
                Ok(mut payload) => {
                    redact_json_value_tree(&mut payload, None);
                    SupportBundleConfigSnapshot {
                        path,
                        redacted_document: Some(payload),
                        error: None,
                    }
                }
                Err(error) => SupportBundleConfigSnapshot {
                    path,
                    redacted_document: None,
                    error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
                },
            }
        }
        Err(error) => SupportBundleConfigSnapshot {
            path,
            redacted_document: None,
            error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
        },
    }
}

fn build_support_bundle_diagnostics_snapshot() -> SupportBundleDiagnosticsSnapshot {
    let token = env::var("PALYRA_ADMIN_TOKEN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let Some(token) = token else {
        return SupportBundleDiagnosticsSnapshot {
            admin_status: None,
            admin_status_error: Some("skipped (PALYRA_ADMIN_TOKEN is not set)".to_owned()),
        };
    };

    let client = match Client::builder().timeout(Duration::from_secs(2)).build() {
        Ok(client) => client,
        Err(error) => {
            return SupportBundleDiagnosticsSnapshot {
                admin_status: None,
                admin_status_error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
            };
        }
    };
    let daemon_url = env::var("PALYRA_DAEMON_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
    let principal = resolve_doctor_admin_principal();
    match fetch_admin_status_payload(
        &client,
        daemon_url.as_str(),
        Some(token),
        principal,
        DEFAULT_DEVICE_ID.to_owned(),
        None,
    ) {
        Ok(mut payload) => {
            redact_json_value_tree(&mut payload, None);
            SupportBundleDiagnosticsSnapshot {
                admin_status: Some(payload),
                admin_status_error: None,
            }
        }
        Err(error) => SupportBundleDiagnosticsSnapshot {
            admin_status: None,
            admin_status_error: Some(sanitize_diagnostic_error(error.to_string().as_str())),
        },
    }
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
        AgentCommand::Acp {
            grpc_url,
            token,
            principal,
            device_id,
            channel,
            allow_sensitive_tools,
        } => {
            let connection = AgentConnection {
                grpc_url: resolve_grpc_url(grpc_url)?,
                token: token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok()),
                principal,
                device_id,
                channel,
            };
            acp_bridge::run_agent_acp_bridge(connection, allow_sensitive_tools)
        }
    }
}

fn run_agents(command: AgentsCommand) -> Result<()> {
    let connection = AgentConnection {
        grpc_url: resolve_grpc_url(None)?,
        token: env::var("PALYRA_ADMIN_TOKEN").ok(),
        principal: "admin:local".to_owned(),
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        channel: DEFAULT_CHANNEL.to_owned(),
    };
    let runtime = build_runtime()?;
    runtime.block_on(run_agents_async(command, connection))
}

async fn run_agents_async(command: AgentsCommand, connection: AgentConnection) -> Result<()> {
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(
        connection.grpc_url.clone(),
    )
    .await
    .with_context(|| format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url))?;

    match command {
        AgentsCommand::List { after, limit, json, ndjson } => {
            let mut request = Request::new(gateway_v1::ListAgentsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                limit: limit.unwrap_or(100),
                after_agent_id: after.unwrap_or_default(),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .list_agents(request)
                .await
                .context("failed to call ListAgents")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agents": response.agents.iter().map(agent_to_json).collect::<Vec<_>>(),
                        "default_agent_id": empty_to_none(response.default_agent_id),
                        "next_after_agent_id": empty_to_none(response.next_after_agent_id),
                    }))?
                );
            } else if ndjson {
                for agent in &response.agents {
                    let line = json!({
                        "type": "agent",
                        "agent": agent_to_json(agent),
                        "is_default": response.default_agent_id == agent.agent_id,
                    });
                    println!("{}", serde_json::to_string(&line)?);
                }
            } else {
                println!(
                    "agents.list count={} default={} next_after={}",
                    response.agents.len(),
                    if response.default_agent_id.is_empty() {
                        "none"
                    } else {
                        response.default_agent_id.as_str()
                    },
                    if response.next_after_agent_id.is_empty() {
                        "none"
                    } else {
                        response.next_after_agent_id.as_str()
                    }
                );
                for agent in &response.agents {
                    println!(
                        "agent id={} name={} dir={} workspaces={} model_profile={}",
                        agent.agent_id,
                        agent.display_name,
                        agent.agent_dir,
                        agent.workspace_roots.len(),
                        agent.default_model_profile
                    );
                }
            }
        }
        AgentsCommand::Show { agent_id, json } => {
            let mut request = Request::new(gateway_v1::GetAgentRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                agent_id: normalize_agent_id_cli(agent_id.as_str())?,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.get_agent(request).await.context("failed to call GetAgent")?.into_inner();
            let agent = response.agent.context("GetAgent returned empty agent payload")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agent": agent_to_json(&agent),
                        "is_default": response.is_default,
                    }))?
                );
            } else {
                println!(
                    "agents.show id={} name={} dir={} default={} model_profile={}",
                    agent.agent_id,
                    agent.display_name,
                    agent.agent_dir,
                    response.is_default,
                    agent.default_model_profile
                );
            }
        }
        AgentsCommand::SetDefault { agent_id, json } => {
            let mut request = Request::new(gateway_v1::SetDefaultAgentRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                agent_id: normalize_agent_id_cli(agent_id.as_str())?,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .set_default_agent(request)
                .await
                .context("failed to call SetDefaultAgent")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "previous_agent_id": empty_to_none(response.previous_agent_id),
                        "default_agent_id": response.default_agent_id,
                    }))?
                );
            } else {
                println!(
                    "agents.set_default previous={} default={}",
                    if response.previous_agent_id.is_empty() {
                        "none"
                    } else {
                        response.previous_agent_id.as_str()
                    },
                    response.default_agent_id
                );
            }
        }
        AgentsCommand::Create {
            agent_id,
            display_name,
            agent_dir,
            workspace_root,
            model_profile,
            tool_allow,
            skill_allow,
            set_default,
            allow_absolute_paths,
            json,
        } => {
            let mut request = Request::new(gateway_v1::CreateAgentRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                agent_id: normalize_agent_id_cli(agent_id.as_str())?,
                display_name,
                agent_dir: agent_dir.unwrap_or_default(),
                workspace_roots: workspace_root,
                default_model_profile: model_profile.unwrap_or_default(),
                default_tool_allowlist: tool_allow,
                default_skill_allowlist: skill_allow,
                set_default,
                allow_absolute_paths,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .create_agent(request)
                .await
                .context("failed to call CreateAgent")?
                .into_inner();
            let agent = response.agent.context("CreateAgent returned empty agent payload")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "agent": agent_to_json(&agent),
                        "default_changed": response.default_changed,
                        "default_agent_id": empty_to_none(response.default_agent_id),
                    }))?
                );
            } else {
                println!(
                    "agents.create id={} name={} default_changed={} default={} dir={}",
                    agent.agent_id,
                    agent.display_name,
                    response.default_changed,
                    if response.default_agent_id.is_empty() {
                        "none"
                    } else {
                        response.default_agent_id.as_str()
                    },
                    agent.agent_dir
                );
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn run_cron(command: CronCommand) -> Result<()> {
    let connection = AgentConnection {
        grpc_url: resolve_grpc_url(None)?,
        token: env::var("PALYRA_ADMIN_TOKEN").ok(),
        principal: "user:local".to_owned(),
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        channel: DEFAULT_CHANNEL.to_owned(),
    };
    let runtime = build_runtime()?;
    runtime.block_on(run_cron_async(command, connection))
}

fn run_memory(command: MemoryCommand) -> Result<()> {
    let connection = AgentConnection {
        grpc_url: resolve_grpc_url(None)?,
        token: env::var("PALYRA_ADMIN_TOKEN").ok(),
        principal: "user:local".to_owned(),
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        channel: DEFAULT_CHANNEL.to_owned(),
    };
    let runtime = build_runtime()?;
    runtime.block_on(run_memory_async(command, connection))
}

async fn run_memory_async(command: MemoryCommand, connection: AgentConnection) -> Result<()> {
    let mut client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(connection.grpc_url.clone())
            .await
            .with_context(|| {
                format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url)
            })?;

    match command {
        MemoryCommand::Search {
            query,
            scope,
            session,
            channel,
            top_k,
            min_score,
            tag,
            source,
            include_score_breakdown,
            json,
        } => {
            if query.trim().is_empty() {
                return Err(anyhow!("memory search query cannot be empty"));
            }
            let min_score =
                parse_float_arg(min_score, "memory search --min-score", 0.0, 1.0, Some(0.0))?;
            let (channel_scope, session_scope) =
                resolve_memory_scope(scope, channel, session, &connection)?;
            let mut request = Request::new(memory_v1::SearchMemoryRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                query,
                channel: channel_scope.unwrap_or_default(),
                session_id: session_scope.map(|ulid| common_v1::CanonicalId { ulid }),
                top_k: top_k.unwrap_or(5),
                min_score,
                tags: tag,
                sources: source.into_iter().map(memory_source_to_proto).collect(),
                include_score_breakdown,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .search_memory(request)
                .await
                .context("failed to call memory SearchMemory")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "hits": response.hits.iter().map(memory_search_hit_to_json).collect::<Vec<_>>(),
                    }))
                    .context("failed to serialize JSON output")?
                );
            } else {
                println!("memory.search hits={}", response.hits.len());
                for hit in response.hits {
                    let item = hit.item.as_ref();
                    let id = item
                        .and_then(|value| value.memory_id.as_ref())
                        .map(|value| value.ulid.as_str())
                        .unwrap_or("unknown");
                    let source_label =
                        item.map(|value| memory_source_to_text(value.source)).unwrap_or("unknown");
                    let created_at = item.map(|value| value.created_at_unix_ms).unwrap_or_default();
                    println!(
                        "memory.hit id={} source={} score={:.4} created_at_ms={} snippet={}",
                        id, source_label, hit.score, created_at, hit.snippet
                    );
                }
            }
        }
        MemoryCommand::Purge { session, channel, principal, json } => {
            if !principal && session.is_none() && channel.is_none() {
                return Err(anyhow!(
                    "memory purge requires one of: --principal, --session, or --channel"
                ));
            }
            let session_id = if let Some(session) = session {
                validate_canonical_id(session.as_str())
                    .context("memory purge --session must be a canonical ULID")?;
                Some(common_v1::CanonicalId { ulid: session })
            } else {
                None
            };
            let mut request = Request::new(memory_v1::PurgeMemoryRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                channel: channel.unwrap_or_default(),
                session_id,
                purge_all_principal: principal,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .purge_memory(request)
                .await
                .context("failed to call memory PurgeMemory")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &json!({ "deleted_count": response.deleted_count })
                    )
                    .context("failed to serialize JSON output")?
                );
            } else {
                println!("memory.purge deleted_count={}", response.deleted_count);
            }
        }
        MemoryCommand::Ingest {
            content,
            source,
            session,
            channel,
            tag,
            confidence,
            ttl_unix_ms,
            json,
        } => {
            if content.trim().is_empty() {
                return Err(anyhow!("memory ingest content cannot be empty"));
            }
            let confidence =
                parse_float_arg(confidence, "memory ingest --confidence", 0.0, 1.0, Some(1.0))?;
            let session_id = if let Some(session) = session {
                validate_canonical_id(session.as_str())
                    .context("memory ingest --session must be a canonical ULID")?;
                Some(common_v1::CanonicalId { ulid: session })
            } else {
                None
            };
            let mut request = Request::new(memory_v1::IngestMemoryRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                source: memory_source_to_proto(source),
                content_text: content,
                channel: channel.unwrap_or(connection.channel.clone()),
                session_id,
                tags: tag,
                confidence,
                ttl_unix_ms: ttl_unix_ms.unwrap_or_default(),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .ingest_memory(request)
                .await
                .context("failed to call memory IngestMemory")?
                .into_inner();
            let item = response.item.context("memory IngestMemory returned empty item payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&memory_item_to_json(&item))?);
            } else {
                println!(
                    "memory.ingest id={} source={} created_at_ms={}",
                    item.memory_id.map(|value| value.ulid).unwrap_or_default(),
                    memory_source_to_text(item.source),
                    item.created_at_unix_ms
                );
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

async fn run_cron_async(command: CronCommand, connection: AgentConnection) -> Result<()> {
    let mut client =
        cron_v1::cron_service_client::CronServiceClient::connect(connection.grpc_url.clone())
            .await
            .with_context(|| {
                format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url)
            })?;

    match command {
        CronCommand::List { after, limit, enabled, owner, channel, json } => {
            let mut request = Request::new(cron_v1::ListJobsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                after_job_ulid: after.unwrap_or_default(),
                limit: limit.unwrap_or(100),
                enabled,
                owner_principal: owner,
                channel,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .list_jobs(request)
                .await
                .context("failed to call cron ListJobs")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "jobs": response.jobs.iter().map(cron_job_to_json).collect::<Vec<_>>(),
                        "next_after_job_ulid": response.next_after_job_ulid,
                    }))
                    .context("failed to serialize JSON output")?
                );
            } else {
                println!(
                    "cron.list jobs={} next_after={}",
                    response.jobs.len(),
                    if response.next_after_job_ulid.is_empty() {
                        "none"
                    } else {
                        response.next_after_job_ulid.as_str()
                    }
                );
                for job in response.jobs {
                    let id =
                        job.job_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or("unknown");
                    println!(
                        "cron.job id={} name={} enabled={} owner={} channel={} next_run_at_ms={}",
                        id,
                        job.name,
                        job.enabled,
                        job.owner_principal,
                        job.channel,
                        job.next_run_at_unix_ms
                    );
                }
            }
        }
        CronCommand::Show { id, json } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let mut request = Request::new(cron_v1::GetJobRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id.clone() }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.get_job(request).await.context("failed to call cron GetJob")?.into_inner();
            let job = response.job.context("cron GetJob returned empty job payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                println!(
                    "cron.show id={} name={} enabled={} owner={} channel={} schedule_type={}",
                    id,
                    job.name,
                    job.enabled,
                    job.owner_principal,
                    job.channel,
                    job.schedule.map(|s| s.r#type).unwrap_or_default()
                );
            }
        }
        CronCommand::Add {
            name,
            prompt,
            schedule_type,
            schedule,
            enabled,
            concurrency,
            retry_max_attempts,
            retry_backoff_ms,
            misfire,
            jitter_ms,
            owner,
            channel,
            session_key,
            session_label,
            json,
        } => {
            let schedule = build_cron_schedule(schedule_type, schedule)?;
            let mut request = Request::new(cron_v1::CreateJobRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                name,
                prompt,
                owner_principal: owner.unwrap_or_else(|| connection.principal.clone()),
                channel: channel.unwrap_or_else(|| "system:cron".to_owned()),
                session_key: session_key.unwrap_or_default(),
                session_label: session_label.unwrap_or_default(),
                schedule: Some(schedule),
                enabled,
                concurrency_policy: cron_concurrency_to_proto(concurrency),
                retry_policy: Some(cron_v1::RetryPolicy {
                    max_attempts: retry_max_attempts.max(1),
                    backoff_ms: retry_backoff_ms.max(1),
                }),
                misfire_policy: cron_misfire_to_proto(misfire),
                jitter_ms,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .create_job(request)
                .await
                .context("failed to call cron CreateJob")?
                .into_inner();
            let job = response.job.context("cron CreateJob returned empty job payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                let id = job.job_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or("unknown");
                println!(
                    "cron.add id={} name={} enabled={} owner={} channel={}",
                    id, job.name, job.enabled, job.owner_principal, job.channel
                );
            }
        }
        CronCommand::Update {
            id,
            name,
            prompt,
            schedule_type,
            schedule,
            enabled,
            concurrency,
            retry_max_attempts,
            retry_backoff_ms,
            misfire,
            jitter_ms,
            owner,
            channel,
            session_key,
            session_label,
            json,
        } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let schedule = match (schedule_type, schedule) {
                (Some(schedule_type), Some(schedule)) => {
                    Some(build_cron_schedule(schedule_type, schedule)?)
                }
                (None, None) => None,
                _ => {
                    unreachable!("clap requires schedule-type and schedule to be provided together")
                }
            };
            let retry_policy = match (retry_max_attempts, retry_backoff_ms) {
                (Some(max_attempts), Some(backoff_ms)) => Some(cron_v1::RetryPolicy {
                    max_attempts: max_attempts.max(1),
                    backoff_ms: backoff_ms.max(1),
                }),
                (None, None) => None,
                _ => unreachable!("clap requires retry policy fields to be provided together"),
            };
            let has_changes = name.is_some()
                || prompt.is_some()
                || owner.is_some()
                || channel.is_some()
                || session_key.is_some()
                || session_label.is_some()
                || schedule.is_some()
                || enabled.is_some()
                || concurrency.is_some()
                || retry_policy.is_some()
                || misfire.is_some()
                || jitter_ms.is_some();
            if !has_changes {
                return Err(anyhow!("cron update requires at least one field to change"));
            }

            let mut request = Request::new(cron_v1::UpdateJobRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id }),
                name,
                prompt,
                owner_principal: owner,
                channel,
                session_key,
                session_label,
                schedule,
                enabled,
                concurrency_policy: concurrency.map(cron_concurrency_to_proto),
                retry_policy,
                misfire_policy: misfire.map(cron_misfire_to_proto),
                jitter_ms,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .update_job(request)
                .await
                .context("failed to call cron UpdateJob")?
                .into_inner();
            let job = response.job.context("cron UpdateJob returned empty job payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                println!(
                    "cron.update id={} enabled={} owner={} channel={}",
                    job.job_id.map(|value| value.ulid).unwrap_or_default(),
                    job.enabled,
                    job.owner_principal,
                    job.channel
                );
            }
        }
        CronCommand::Enable { id, json } => {
            let response = update_cron_enabled(&mut client, &connection, id, true).await?;
            if json {
                let job = response.job.context("cron UpdateJob returned empty job payload")?;
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                let job = response.job.context("cron UpdateJob returned empty job payload")?;
                println!(
                    "cron.enable id={} enabled={}",
                    job.job_id.map(|value| value.ulid).unwrap_or_default(),
                    job.enabled
                );
            }
        }
        CronCommand::Disable { id, json } => {
            let response = update_cron_enabled(&mut client, &connection, id, false).await?;
            if json {
                let job = response.job.context("cron UpdateJob returned empty job payload")?;
                println!("{}", serde_json::to_string_pretty(&cron_job_to_json(&job))?);
            } else {
                let job = response.job.context("cron UpdateJob returned empty job payload")?;
                println!(
                    "cron.disable id={} enabled={}",
                    job.job_id.map(|value| value.ulid).unwrap_or_default(),
                    job.enabled
                );
            }
        }
        CronCommand::RunNow { id, json } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let mut request = Request::new(cron_v1::RunJobNowRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id.clone() }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .run_job_now(request)
                .await
                .context("failed to call cron RunJobNow")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "run_id": response.run_id.map(|value| value.ulid),
                        "status": response.status,
                        "message": response.message,
                    }))?
                );
            } else {
                println!(
                    "cron.run_now id={} run_id={} status={} message={}",
                    id,
                    response.run_id.map(|value| value.ulid).unwrap_or_default(),
                    response.status,
                    response.message
                );
            }
        }
        CronCommand::Delete { id, json } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let mut request = Request::new(cron_v1::DeleteJobRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id.clone() }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .delete_job(request)
                .await
                .context("failed to call cron DeleteJob")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "deleted": response.deleted,
                    }))?
                );
            } else {
                println!("cron.delete id={} deleted={}", id, response.deleted);
            }
        }
        CronCommand::Logs { id, after, limit, json } => {
            validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
            let mut request = Request::new(cron_v1::ListJobRunsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                job_id: Some(common_v1::CanonicalId { ulid: id.clone() }),
                after_run_ulid: after.unwrap_or_default(),
                limit: limit.unwrap_or(100),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .list_job_runs(request)
                .await
                .context("failed to call cron ListJobRuns")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "runs": response.runs.iter().map(cron_run_to_json).collect::<Vec<_>>(),
                        "next_after_run_ulid": response.next_after_run_ulid,
                    }))?
                );
            } else {
                println!(
                    "cron.logs id={} runs={} next_after={}",
                    id,
                    response.runs.len(),
                    if response.next_after_run_ulid.is_empty() {
                        "none"
                    } else {
                        response.next_after_run_ulid.as_str()
                    }
                );
                for run in response.runs {
                    println!(
                        "cron.run run_id={} status={} started_at_ms={} finished_at_ms={} tool_calls={} tool_denies={}",
                        run.run_id.map(|value| value.ulid).unwrap_or_default(),
                        run.status,
                        run.started_at_unix_ms,
                        run.finished_at_unix_ms,
                        run.tool_calls,
                        run.tool_denies
                    );
                }
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn run_approvals(command: ApprovalsCommand) -> Result<()> {
    let connection = AgentConnection {
        grpc_url: resolve_grpc_url(None)?,
        token: env::var("PALYRA_ADMIN_TOKEN").ok(),
        principal: "user:local".to_owned(),
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        channel: DEFAULT_CHANNEL.to_owned(),
    };
    let runtime = build_runtime()?;
    runtime.block_on(run_approvals_async(command, connection))
}

async fn run_approvals_async(command: ApprovalsCommand, connection: AgentConnection) -> Result<()> {
    let mut client = gateway_v1::approvals_service_client::ApprovalsServiceClient::connect(
        connection.grpc_url.clone(),
    )
    .await
    .with_context(|| format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url))?;

    match command {
        ApprovalsCommand::List {
            after,
            limit,
            since,
            until,
            subject,
            principal,
            decision,
            json,
        } => {
            if let (Some(since_ms), Some(until_ms)) = (since, until) {
                if since_ms > until_ms {
                    return Err(anyhow!(
                        "approvals list requires --since <= --until when both filters are set"
                    ));
                }
            }
            if let Some(after_value) = after.as_deref() {
                validate_canonical_id(after_value)
                    .context("approval cursor (--after) must be a canonical ULID")?;
            }
            let mut request = Request::new(gateway_v1::ListApprovalsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                after_approval_ulid: after.unwrap_or_default(),
                limit: limit.unwrap_or(100),
                since_unix_ms: since.unwrap_or_default(),
                until_unix_ms: until.unwrap_or_default(),
                subject_id: subject.unwrap_or_default(),
                principal: principal.unwrap_or_default(),
                decision: approval_decision_filter_to_proto(decision),
                subject_type: gateway_v1::ApprovalSubjectType::Unspecified as i32,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .list_approvals(request)
                .await
                .context("failed to call approvals ListApprovals")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "approvals": response.approvals.iter().map(approval_record_to_json).collect::<Vec<_>>(),
                        "next_after_approval_ulid": response.next_after_approval_ulid,
                    }))?
                );
            } else {
                println!(
                    "approvals.list approvals={} next_after={}",
                    response.approvals.len(),
                    if response.next_after_approval_ulid.is_empty() {
                        "none"
                    } else {
                        response.next_after_approval_ulid.as_str()
                    }
                );
                for approval in response.approvals {
                    println!(
                        "approval id={} subject={} decision={} principal={} requested_at_ms={} resolved_at_ms={}",
                        approval
                            .approval_id
                            .as_ref()
                            .map(|value| value.ulid.as_str())
                            .unwrap_or("unknown"),
                        approval.subject_id,
                        approval_decision_to_text(approval.decision),
                        approval.principal,
                        approval.requested_at_unix_ms,
                        approval.resolved_at_unix_ms
                    );
                }
            }
        }
        ApprovalsCommand::Show { approval_id, json } => {
            validate_canonical_id(approval_id.as_str())
                .context("approval id must be a canonical ULID")?;
            let mut request = Request::new(gateway_v1::GetApprovalRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                approval_id: Some(common_v1::CanonicalId { ulid: approval_id.clone() }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .get_approval(request)
                .await
                .context("failed to call approvals GetApproval")?
                .into_inner();
            let approval = response
                .approval
                .context("approvals GetApproval returned empty approval payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&approval_record_to_json(&approval))?);
            } else {
                println!(
                    "approvals.show id={} subject={} decision={} scope={} reason={}",
                    approval
                        .approval_id
                        .as_ref()
                        .map(|value| value.ulid.as_str())
                        .unwrap_or("unknown"),
                    approval.subject_id,
                    approval_decision_to_text(approval.decision),
                    approval_scope_to_text(approval.decision_scope),
                    approval.decision_reason
                );
            }
        }
        ApprovalsCommand::Export { format, limit, since, until, subject, principal, decision } => {
            if let (Some(since_ms), Some(until_ms)) = (since, until) {
                if since_ms > until_ms {
                    return Err(anyhow!(
                        "approvals export requires --since <= --until when both filters are set"
                    ));
                }
            }
            let mut request = Request::new(gateway_v1::ExportApprovalsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                format: approval_export_format_to_proto(format),
                limit: limit.unwrap_or(1_000),
                since_unix_ms: since.unwrap_or_default(),
                until_unix_ms: until.unwrap_or_default(),
                subject_id: subject.unwrap_or_default(),
                principal: principal.unwrap_or_default(),
                decision: approval_decision_filter_to_proto(decision),
                subject_type: gateway_v1::ApprovalSubjectType::Unspecified as i32,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let mut stream = client
                .export_approvals(request)
                .await
                .context("failed to call approvals ExportApprovals")?
                .into_inner();
            while let Some(item) = stream.next().await {
                let chunk = item.context("failed to read approvals export stream chunk")?;
                if !chunk.chunk.is_empty() {
                    std::io::stdout()
                        .write_all(chunk.chunk.as_slice())
                        .context("failed to write approvals export chunk to stdout")?;
                }
                if chunk.done {
                    break;
                }
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn run_auth(command: AuthCommand) -> Result<()> {
    let connection = AgentConnection {
        grpc_url: resolve_grpc_url(None)?,
        token: env::var("PALYRA_ADMIN_TOKEN").ok(),
        principal: "admin:local".to_owned(),
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        channel: DEFAULT_CHANNEL.to_owned(),
    };
    let runtime = build_runtime()?;
    runtime.block_on(run_auth_async(command, connection))
}

async fn run_auth_async(command: AuthCommand, connection: AgentConnection) -> Result<()> {
    let mut client =
        auth_v1::auth_service_client::AuthServiceClient::connect(connection.grpc_url.clone())
            .await
            .with_context(|| {
                format!("failed to connect auth gRPC endpoint {}", connection.grpc_url)
            })?;

    let AuthCommand::Profiles { command } = command;
    match command {
        AuthProfilesCommand::List {
            after,
            limit,
            provider,
            provider_name,
            scope,
            agent_id,
            json,
        } => {
            let mut request = Request::new(auth_v1::ListAuthProfilesRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                after_profile_id: after.unwrap_or_default(),
                limit: limit.unwrap_or(100),
                provider_kind: provider
                    .map(auth_provider_arg_to_proto)
                    .unwrap_or(auth_v1::AuthProviderKind::Unspecified as i32),
                provider_custom_name: provider_name.unwrap_or_default(),
                scope_kind: scope
                    .map(auth_scope_arg_to_proto)
                    .unwrap_or(auth_v1::AuthScopeKind::Unspecified as i32),
                scope_agent_id: agent_id.unwrap_or_default(),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.list_profiles(request).await.context("failed to call auth ListProfiles")?;
            let payload = response.into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "profiles": payload.profiles.iter().map(auth_profile_to_json).collect::<Vec<_>>(),
                        "next_after_profile_id": empty_to_none(payload.next_after_profile_id),
                    }))?
                );
            } else {
                println!(
                    "auth.profiles.list count={} next_after={}",
                    payload.profiles.len(),
                    if payload.next_after_profile_id.is_empty() {
                        "none"
                    } else {
                        payload.next_after_profile_id.as_str()
                    }
                );
                for profile in &payload.profiles {
                    println!(
                        "auth.profile id={} provider={} scope={} credential={}",
                        profile.profile_id,
                        auth_provider_to_text(profile.provider.as_ref()),
                        auth_scope_to_text(profile.scope.as_ref()),
                        auth_profile_credential_type(profile)
                    );
                }
            }
        }
        AuthProfilesCommand::Show { profile_id, json } => {
            let mut request = Request::new(auth_v1::GetAuthProfileRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                profile_id,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.get_profile(request).await.context("failed to call auth GetProfile")?;
            let profile = response
                .into_inner()
                .profile
                .context("auth GetProfile returned empty profile payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&auth_profile_to_json(&profile))?);
            } else {
                println!(
                    "auth.profiles.show id={} provider={} scope={} credential={} updated_at_ms={}",
                    profile.profile_id,
                    auth_provider_to_text(profile.provider.as_ref()),
                    auth_scope_to_text(profile.scope.as_ref()),
                    auth_profile_credential_type(&profile),
                    profile.updated_at_unix_ms
                );
            }
        }
        AuthProfilesCommand::Set {
            profile_id,
            provider,
            provider_name,
            profile_name,
            scope,
            agent_id,
            credential,
            api_key_ref,
            access_token_ref,
            refresh_token_ref,
            token_endpoint,
            client_id,
            client_secret_ref,
            scope_value,
            expires_at_unix_ms,
            json,
        } => {
            let provider_message = auth_v1::AuthProvider {
                kind: auth_provider_arg_to_proto(provider),
                custom_name: provider_name.unwrap_or_default(),
            };
            let scope_message = match scope {
                AuthScopeArg::Global => auth_v1::AuthScope {
                    kind: auth_v1::AuthScopeKind::Global as i32,
                    agent_id: String::new(),
                },
                AuthScopeArg::Agent => auth_v1::AuthScope {
                    kind: auth_v1::AuthScopeKind::Agent as i32,
                    agent_id: agent_id.context("--agent-id is required when --scope=agent")?,
                },
            };
            let credential_message = match credential {
                AuthCredentialArg::ApiKey => auth_v1::AuthCredential {
                    kind: Some(auth_v1::auth_credential::Kind::ApiKey(auth_v1::ApiKeyCredential {
                        api_key_vault_ref: api_key_ref
                            .context("--api-key-ref is required when --credential=api-key")?,
                    })),
                },
                AuthCredentialArg::Oauth => auth_v1::AuthCredential {
                    kind: Some(auth_v1::auth_credential::Kind::Oauth(auth_v1::OAuthCredential {
                        access_token_vault_ref: access_token_ref
                            .context("--access-token-ref is required when --credential=oauth")?,
                        refresh_token_vault_ref: refresh_token_ref
                            .context("--refresh-token-ref is required when --credential=oauth")?,
                        token_endpoint: token_endpoint
                            .context("--token-endpoint is required when --credential=oauth")?,
                        client_id: client_id.unwrap_or_default(),
                        client_secret_vault_ref: client_secret_ref.unwrap_or_default(),
                        scopes: scope_value,
                        expires_at_unix_ms: expires_at_unix_ms.unwrap_or_default(),
                        refresh_state: None,
                    })),
                },
            };
            let mut request = Request::new(auth_v1::SetAuthProfileRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                profile: Some(auth_v1::AuthProfile {
                    profile_id,
                    provider: Some(provider_message),
                    profile_name,
                    scope: Some(scope_message),
                    credential: Some(credential_message),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.set_profile(request).await.context("failed to call auth SetProfile")?;
            let profile = response
                .into_inner()
                .profile
                .context("auth SetProfile returned empty profile payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&auth_profile_to_json(&profile))?);
            } else {
                println!(
                    "auth.profiles.set id={} provider={} scope={} credential={}",
                    profile.profile_id,
                    auth_provider_to_text(profile.provider.as_ref()),
                    auth_scope_to_text(profile.scope.as_ref()),
                    auth_profile_credential_type(&profile)
                );
            }
        }
        AuthProfilesCommand::Delete { profile_id, json } => {
            let mut request = Request::new(auth_v1::DeleteAuthProfileRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                profile_id,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .delete_profile(request)
                .await
                .context("failed to call auth DeleteProfile")?;
            let payload = response.into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({ "deleted": payload.deleted }))?
                );
            } else {
                println!("auth.profiles.delete deleted={}", payload.deleted);
            }
        }
        AuthProfilesCommand::Health { agent_id, include_profiles, json } => {
            let mut request = Request::new(auth_v1::GetAuthHealthRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                agent_id: agent_id.unwrap_or_default(),
                include_profiles,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.get_health(request).await.context("failed to call auth GetHealth")?;
            let payload = response.into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "summary": payload.summary.as_ref().map(auth_health_summary_to_json),
                        "expiry_distribution": payload
                            .expiry_distribution
                            .as_ref()
                            .map(auth_expiry_distribution_to_json),
                        "refresh_metrics": payload.refresh_metrics.as_ref().map(auth_refresh_metrics_to_json),
                        "profiles": payload.profiles.iter().map(auth_health_profile_to_json).collect::<Vec<_>>(),
                    }))?
                );
            } else {
                let summary = payload.summary.unwrap_or_default();
                println!(
                    "auth.profiles.health total={} ok={} expiring={} expired={} missing={} static={}",
                    summary.total,
                    summary.ok,
                    summary.expiring,
                    summary.expired,
                    summary.missing,
                    summary.static_count
                );
                let refresh = payload.refresh_metrics.unwrap_or_default();
                println!(
                    "auth.refresh attempts={} successes={} failures={}",
                    refresh.attempts, refresh.successes, refresh.failures
                );
                if include_profiles {
                    for profile in &payload.profiles {
                        println!(
                            "auth.health.profile id={} provider={} state={} reason={}",
                            profile.profile_id,
                            profile.provider,
                            auth_health_state_to_text(profile.state),
                            profile.reason
                        );
                    }
                }
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
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

async fn update_cron_enabled(
    client: &mut cron_v1::cron_service_client::CronServiceClient<tonic::transport::Channel>,
    connection: &AgentConnection,
    id: String,
    enabled: bool,
) -> Result<cron_v1::UpdateJobResponse> {
    validate_canonical_id(id.as_str()).context("cron job id must be a canonical ULID")?;
    let mut request = Request::new(cron_v1::UpdateJobRequest {
        v: CANONICAL_PROTOCOL_MAJOR,
        job_id: Some(common_v1::CanonicalId { ulid: id }),
        name: None,
        prompt: None,
        owner_principal: None,
        channel: None,
        session_key: None,
        session_label: None,
        schedule: None,
        enabled: Some(enabled),
        concurrency_policy: None,
        retry_policy: None,
        misfire_policy: None,
        jitter_ms: None,
    });
    inject_run_stream_metadata(request.metadata_mut(), connection)?;
    let response =
        client.update_job(request).await.context("failed to call cron UpdateJob")?.into_inner();
    Ok(response)
}

fn build_cron_schedule(
    schedule_type: CronScheduleTypeArg,
    schedule: String,
) -> Result<cron_v1::Schedule> {
    match schedule_type {
        CronScheduleTypeArg::Cron => Ok(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule {
                expression: schedule,
            })),
        }),
        CronScheduleTypeArg::Every => Ok(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: parse_interval_ms(schedule.as_str())?,
            })),
        }),
        CronScheduleTypeArg::At => Ok(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::At as i32,
            spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                timestamp_rfc3339: schedule,
            })),
        }),
    }
}

fn parse_interval_ms(raw: &str) -> Result<u64> {
    let value = raw.trim();
    if value.is_empty() {
        anyhow::bail!("every schedule value cannot be empty");
    }
    if let Some(stripped) = value.strip_suffix("ms") {
        let parsed = stripped
            .trim()
            .parse::<u64>()
            .context("every schedule milliseconds must be a positive integer")?;
        if parsed == 0 {
            anyhow::bail!("every schedule interval must be greater than zero");
        }
        return Ok(parsed);
    }
    if let Some(stripped) = value.strip_suffix('s') {
        let parsed = stripped
            .trim()
            .parse::<u64>()
            .context("every schedule seconds must be a positive integer")?;
        if parsed == 0 {
            anyhow::bail!("every schedule interval must be greater than zero");
        }
        return Ok(parsed.saturating_mul(1_000));
    }
    if let Some(stripped) = value.strip_suffix('m') {
        let parsed = stripped
            .trim()
            .parse::<u64>()
            .context("every schedule minutes must be a positive integer")?;
        if parsed == 0 {
            anyhow::bail!("every schedule interval must be greater than zero");
        }
        return Ok(parsed.saturating_mul(60_000));
    }
    if let Some(stripped) = value.strip_suffix('h') {
        let parsed = stripped
            .trim()
            .parse::<u64>()
            .context("every schedule hours must be a positive integer")?;
        if parsed == 0 {
            anyhow::bail!("every schedule interval must be greater than zero");
        }
        return Ok(parsed.saturating_mul(3_600_000));
    }
    let parsed = value.parse::<u64>().context(
        "every schedule value must be integer milliseconds or include one of suffixes: ms,s,m,h",
    )?;
    if parsed == 0 {
        anyhow::bail!("every schedule interval must be greater than zero");
    }
    Ok(parsed)
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

fn cron_concurrency_to_proto(value: CronConcurrencyPolicyArg) -> i32 {
    match value {
        CronConcurrencyPolicyArg::Forbid => cron_v1::ConcurrencyPolicy::Forbid as i32,
        CronConcurrencyPolicyArg::Replace => cron_v1::ConcurrencyPolicy::Replace as i32,
        CronConcurrencyPolicyArg::QueueOne => cron_v1::ConcurrencyPolicy::QueueOne as i32,
    }
}

fn cron_misfire_to_proto(value: CronMisfirePolicyArg) -> i32 {
    match value {
        CronMisfirePolicyArg::Skip => cron_v1::MisfirePolicy::Skip as i32,
        CronMisfirePolicyArg::CatchUp => cron_v1::MisfirePolicy::CatchUp as i32,
    }
}

fn cron_job_to_json(job: &cron_v1::Job) -> serde_json::Value {
    json!({
        "job_id": job.job_id.as_ref().map(|value| value.ulid.clone()),
        "name": job.name,
        "prompt": job.prompt,
        "owner_principal": job.owner_principal,
        "channel": job.channel,
        "session_key": job.session_key,
        "session_label": job.session_label,
        "schedule": job.schedule.as_ref().map(|schedule| json!({
            "type": schedule.r#type,
            "spec": match schedule.spec.as_ref() {
                Some(cron_v1::schedule::Spec::Cron(value)) => json!({ "cron": { "expression": value.expression } }),
                Some(cron_v1::schedule::Spec::Every(value)) => json!({ "every": { "interval_ms": value.interval_ms } }),
                Some(cron_v1::schedule::Spec::At(value)) => json!({ "at": { "timestamp_rfc3339": value.timestamp_rfc3339 } }),
                None => json!(null),
            },
        })),
        "enabled": job.enabled,
        "concurrency_policy": job.concurrency_policy,
        "retry_policy": job.retry_policy.as_ref().map(|value| json!({
            "max_attempts": value.max_attempts,
            "backoff_ms": value.backoff_ms,
        })),
        "misfire_policy": job.misfire_policy,
        "jitter_ms": job.jitter_ms,
        "next_run_at_unix_ms": job.next_run_at_unix_ms,
        "last_run_at_unix_ms": job.last_run_at_unix_ms,
        "created_at_unix_ms": job.created_at_unix_ms,
        "updated_at_unix_ms": job.updated_at_unix_ms,
    })
}

fn cron_run_to_json(run: &cron_v1::JobRun) -> serde_json::Value {
    json!({
        "run_id": run.run_id.as_ref().map(|value| value.ulid.clone()),
        "job_id": run.job_id.as_ref().map(|value| value.ulid.clone()),
        "session_id": run.session_id.as_ref().map(|value| value.ulid.clone()),
        "orchestrator_run_id": run.orchestrator_run_id.as_ref().map(|value| value.ulid.clone()),
        "attempt": run.attempt,
        "started_at_unix_ms": run.started_at_unix_ms,
        "finished_at_unix_ms": run.finished_at_unix_ms,
        "status": run.status,
        "error_kind": run.error_kind,
        "error_message_redacted": run.error_message_redacted,
        "model_tokens_in": run.model_tokens_in,
        "model_tokens_out": run.model_tokens_out,
        "tool_calls": run.tool_calls,
        "tool_denies": run.tool_denies,
    })
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
    json!({
        "memory_id": item.memory_id.as_ref().map(|value| value.ulid.clone()),
        "principal": item.principal,
        "channel": item.channel,
        "session_id": item.session_id.as_ref().map(|value| value.ulid.clone()),
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
        "session_id": approval.session_id.as_ref().map(|value| value.ulid.clone()),
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
        "default_tool_allowlist": agent.default_tool_allowlist,
        "default_skill_allowlist": agent.default_skill_allowlist,
        "created_at_unix_ms": agent.created_at_unix_ms,
        "updated_at_unix_ms": agent.updated_at_unix_ms,
    })
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
        session_key: String::new(),
        session_label: String::new(),
        reset_session: false,
        require_existing: false,
        tool_approval_response: None,
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
        Some(common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request)) => {
            println!(
                "agent.tool.approval.request run_id={} proposal_id={} approval_id={} tool_name={} approval_required={} summary=\"{}\"",
                run_id,
                approval_request
                    .proposal_id
                    .as_ref()
                    .map(|value| value.ulid.as_str())
                    .unwrap_or("unknown"),
                approval_request
                    .approval_id
                    .as_ref()
                    .map(|value| value.ulid.as_str())
                    .unwrap_or("unknown"),
                approval_request.tool_name,
                approval_request.approval_required,
                approval_request.request_summary
            );
        }
        Some(common_v1::run_stream_event::Body::ToolApprovalResponse(approval_response)) => {
            println!(
                "agent.tool.approval.response run_id={} proposal_id={} approval_id={} approved={} scope={} ttl_ms={} reason={}",
                run_id,
                approval_response
                    .proposal_id
                    .as_ref()
                    .map(|value| value.ulid.as_str())
                    .unwrap_or("unknown"),
                approval_response
                    .approval_id
                    .as_ref()
                    .map(|value| value.ulid.as_str())
                    .unwrap_or("unknown"),
                approval_response.approved,
                approval_scope_to_text(approval_response.decision_scope),
                approval_response.decision_scope_ttl_ms,
                approval_response.reason
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
        Some(common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request)) => json!({
            "type": "tool.approval.request",
            "run_id": run_id,
            "proposal_id": approval_request.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "approval_id": approval_request.approval_id.as_ref().map(|value| value.ulid.clone()),
            "tool_name": approval_request.tool_name,
            "approval_required": approval_request.approval_required,
            "request_summary": approval_request.request_summary,
            "prompt": approval_request.prompt.as_ref().map(|prompt| json!({
                "title": prompt.title,
                "risk_level": approval_risk_to_text(prompt.risk_level),
                "subject_id": prompt.subject_id,
                "summary": prompt.summary,
                "policy_explanation": prompt.policy_explanation,
                "timeout_seconds": prompt.timeout_seconds,
                "options": prompt.options.iter().map(|option| json!({
                    "option_id": option.option_id,
                    "label": option.label,
                    "description": option.description,
                    "default_selected": option.default_selected,
                    "decision_scope": approval_scope_to_text(option.decision_scope),
                    "timebox_ttl_ms": option.timebox_ttl_ms,
                })).collect::<Vec<_>>(),
                    "details_json": if prompt.details_json.is_empty() {
                        json!({})
                    } else {
                        serde_json::from_slice::<serde_json::Value>(prompt.details_json.as_slice())
                            .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(prompt.details_json.as_slice()).to_string() }))
                    },
            })),
            "input_json": approval_request.input_json,
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalResponse(approval_response)) => json!({
            "type": "tool.approval.response",
            "run_id": run_id,
            "proposal_id": approval_response.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "approval_id": approval_response.approval_id.as_ref().map(|value| value.ulid.clone()),
            "approved": approval_response.approved,
            "reason": approval_response.reason,
            "decision_scope": approval_scope_to_text(approval_response.decision_scope),
            "decision_scope_ttl_ms": approval_response.decision_scope_ttl_ms,
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

fn fetch_admin_status_payload(
    client: &Client,
    base_url: &str,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
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
) -> Result<AdminStatusResponse> {
    let payload =
        fetch_admin_status_payload(client, base_url, token, principal, device_id, channel)?;
    serde_json::from_value(payload).context("failed to decode daemon admin status summary payload")
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

fn format_config_get_display_value(key: &str, value: &toml::Value, show_secrets: bool) -> String {
    if show_secrets || !is_secret_config_path(key) {
        format_toml_value(value)
    } else {
        format_toml_value(&toml::Value::String(REDACTED_CONFIG_VALUE.to_owned()))
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
        ConfigCommand::List { path, show_secrets } => {
            let path = resolve_config_path(path, true)?;
            let (mut document, _) = load_document_from_existing_path(Path::new(&path))
                .with_context(|| format!("failed to parse {path}"))?;
            if !show_secrets {
                redact_secret_config_values(&mut document);
            }
            let rendered =
                toml::to_string_pretty(&document).context("failed to serialize config document")?;
            println!("config.list source={} show_secrets={show_secrets}", path);
            print!("{rendered}");
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Get { path, key, show_secrets } => {
            let path = resolve_config_path(path, true)?;
            let (document, _) = load_document_from_existing_path(Path::new(&path))
                .with_context(|| format!("failed to parse {path}"))?;
            let value = get_value_at_path(&document, key.as_str())
                .with_context(|| format!("invalid config key path: {}", key))?
                .with_context(|| format!("config key not found: {}", key))?;
            let display_value = format_config_get_display_value(key.as_str(), value, show_secrets);
            println!(
                "config.get key={} value={} source={} show_secrets={show_secrets}",
                key, display_value, path
            );
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

fn run_patch(command: PatchCommand) -> Result<()> {
    match command {
        PatchCommand::Apply { stdin, dry_run, json } => {
            if !stdin {
                anyhow::bail!("patch apply requires --stdin");
            }
            let mut patch = String::new();
            std::io::stdin()
                .read_to_string(&mut patch)
                .context("failed to read patch from stdin")?;
            if patch.trim().is_empty() {
                anyhow::bail!("patch from stdin is empty");
            }

            let workspace_root = std::env::current_dir()
                .context("failed to resolve current working directory as workspace root")?;
            let limits = WorkspacePatchLimits::default();
            let redaction_policy = WorkspacePatchRedactionPolicy::default();
            let request = WorkspacePatchRequest {
                patch: patch.clone(),
                dry_run,
                redaction_policy: redaction_policy.clone(),
            };

            match apply_workspace_patch(&[workspace_root], &request, &limits) {
                Ok(outcome) => {
                    if json {
                        let rendered = serde_json::to_string_pretty(&outcome)
                            .context("failed to serialize patch apply output")?;
                        println!("{rendered}");
                    } else {
                        println!(
                            "patch.apply success=true dry_run={} files_touched={} patch_sha256={}",
                            outcome.dry_run,
                            outcome.files_touched.len(),
                            outcome.patch_sha256
                        );
                    }
                    std::io::stdout().flush().context("stdout flush failed")
                }
                Err(error) => {
                    let parse_error = error
                        .parse_location()
                        .map(|(line, column)| json!({ "line": line, "column": column }));
                    let payload = json!({
                        "patch_sha256": compute_patch_sha256(patch.as_str()),
                        "dry_run": dry_run,
                        "rollback_performed": error.rollback_performed(),
                        "redacted_preview": redact_patch_preview(
                            patch.as_str(),
                            &redaction_policy,
                            limits.max_preview_bytes,
                        ),
                        "parse_error": parse_error,
                        "error": error.to_string(),
                    });
                    if json {
                        let rendered = serde_json::to_string_pretty(&payload)
                            .context("failed to serialize patch apply failure output")?;
                        println!("{rendered}");
                    } else {
                        println!(
                            "patch.apply success=false dry_run={} rollback_performed={} error={}",
                            dry_run,
                            error.rollback_performed(),
                            error
                        );
                    }
                    std::io::stdout().flush().context("stdout flush failed")?;
                    anyhow::bail!("patch apply failed: {error}");
                }
            }
        }
    }
}

fn run_skills(command: SkillsCommand) -> Result<()> {
    match command {
        SkillsCommand::Package { command } => match command {
            SkillsPackageCommand::Build {
                manifest,
                module,
                asset,
                sbom,
                provenance,
                output,
                signing_key_vault_ref,
                signing_key_stdin,
                json,
            } => {
                if module.is_empty() {
                    anyhow::bail!("skills package build requires at least one --module");
                }
                let manifest_toml = fs::read_to_string(manifest.as_str()).with_context(|| {
                    format!("failed to read skills manifest {}", Path::new(&manifest).display())
                })?;
                let modules = module
                    .iter()
                    .map(|path| {
                        let bytes = fs::read(path).with_context(|| {
                            format!("failed to read module {}", Path::new(path).display())
                        })?;
                        let entry_path = skill_entry_path_from_cli(path)?;
                        Ok(ArtifactFile { path: entry_path, bytes })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let assets = asset
                    .iter()
                    .map(|path| {
                        let bytes = fs::read(path).with_context(|| {
                            format!("failed to read asset {}", Path::new(path).display())
                        })?;
                        let entry_path = skill_entry_path_from_cli(path)?;
                        Ok(ArtifactFile { path: entry_path, bytes })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let sbom_payload = fs::read(sbom.as_str()).with_context(|| {
                    format!("failed to read SBOM {}", Path::new(&sbom).display())
                })?;
                let provenance_payload = fs::read(provenance.as_str()).with_context(|| {
                    format!(
                        "failed to read provenance payload {}",
                        Path::new(&provenance).display()
                    )
                })?;
                let signing_key_secret = read_skills_signing_key_source(
                    signing_key_vault_ref.as_deref(),
                    signing_key_stdin,
                )?;
                let signing_key = parse_ed25519_signing_key(signing_key_secret.as_slice())
                    .context("invalid signing key bytes (expected raw 32-byte, hex, or base64)")?;

                let build_output = build_signed_skill_artifact(SkillArtifactBuildRequest {
                    manifest_toml,
                    modules,
                    assets,
                    sbom_cyclonedx_json: sbom_payload,
                    provenance_json: provenance_payload,
                    signing_key,
                })
                .context("failed to build signed skill artifact")?;

                let output_path = Path::new(&output);
                if let Some(parent) = output_path.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create output directory {}", parent.to_string_lossy())
                    })?;
                }
                fs::write(output_path, build_output.artifact_bytes.as_slice()).with_context(
                    || format!("failed to write skill artifact {}", output_path.display()),
                )?;

                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "artifact_path": output_path,
                            "payload_sha256": build_output.payload_sha256,
                            "publisher": build_output.manifest.publisher,
                            "skill_id": build_output.manifest.skill_id,
                            "version": build_output.manifest.version,
                            "signature_key_id": build_output.signature.key_id,
                            "artifact_bytes": build_output.artifact_bytes.len(),
                        }))?
                    );
                } else {
                    println!(
                        "skills.package.build artifact={} skill_id={} publisher={} version={} payload_sha256={} key_id={} bytes={}",
                        output_path.display(),
                        build_output.manifest.skill_id,
                        build_output.manifest.publisher,
                        build_output.manifest.version,
                        build_output.payload_sha256,
                        build_output.signature.key_id,
                        build_output.artifact_bytes.len(),
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
            SkillsPackageCommand::Verify {
                artifact,
                trust_store,
                trusted_publishers,
                allow_tofu,
                json,
            } => {
                let artifact_path = Path::new(artifact.as_str());
                let artifact_bytes = fs::read(artifact_path).with_context(|| {
                    format!("failed to read skill artifact {}", artifact_path.display())
                })?;
                let trust_store_path = resolve_skills_trust_store_path(trust_store.as_deref())
                    .with_context(|| "failed to resolve skills trust store path".to_owned())?;
                let mut store = load_trust_store_with_integrity(trust_store_path.as_path())?;
                for trusted in trusted_publishers {
                    let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
                    store.add_trusted_key(publisher, key)?;
                }
                let report =
                    verify_skill_artifact(artifact_bytes.as_slice(), &mut store, allow_tofu)
                        .context("failed to verify skill artifact")?;
                save_trust_store_with_integrity(trust_store_path.as_path(), &store)?;

                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "skills.package.verify artifact={} accepted={} trust={} skill_id={} publisher={} version={} payload_sha256={} trust_store={}",
                        artifact_path.display(),
                        report.accepted,
                        match report.trust_decision {
                            palyra_skills::TrustDecision::Allowlisted => "allowlisted",
                            palyra_skills::TrustDecision::TofuPinned => "tofu_pinned",
                            palyra_skills::TrustDecision::TofuNewlyPinned => "tofu_newly_pinned",
                        },
                        report.manifest.skill_id,
                        report.manifest.publisher,
                        report.manifest.version,
                        report.payload_sha256,
                        trust_store_path.display()
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
        },
        SkillsCommand::Install {
            artifact,
            registry_dir,
            registry_url,
            skill_id,
            version,
            registry_ca_cert,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            non_interactive,
            json,
        } => run_skills_install(SkillsInstallCommand {
            artifact,
            registry_dir,
            registry_url,
            skill_id,
            version,
            registry_ca_cert,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            non_interactive,
            json,
        }),
        SkillsCommand::Remove { skill_id, version, skills_dir, json } => {
            run_skills_remove(skill_id, version, skills_dir, json)
        }
        SkillsCommand::List { skills_dir, json } => run_skills_list(skills_dir, json),
        SkillsCommand::Update {
            registry_dir,
            registry_url,
            skill_id,
            version,
            registry_ca_cert,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            non_interactive,
            json,
        } => run_skills_update(SkillsUpdateCommand {
            registry_dir,
            registry_url,
            skill_id,
            version,
            registry_ca_cert,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            non_interactive,
            json,
        }),
        SkillsCommand::Verify {
            skill_id,
            version,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        } => run_skills_verify(
            skill_id,
            version,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        ),
        SkillsCommand::Audit {
            skill_id,
            version,
            artifact,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        } => run_skills_audit(SkillsAuditCommand {
            skill_id,
            version,
            artifact,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        }),
        SkillsCommand::Quarantine {
            skill_id,
            version,
            skills_dir,
            reason,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => run_skills_quarantine(SkillsQuarantineCommand {
            skill_id,
            version,
            skills_dir,
            reason,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        }),
        SkillsCommand::Enable {
            skill_id,
            version,
            skills_dir,
            override_enabled,
            reason,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => run_skills_enable(SkillsEnableCommand {
            skill_id,
            version,
            skills_dir,
            override_enabled,
            reason,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        }),
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

fn run_skills_install(command: SkillsInstallCommand) -> Result<()> {
    let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
    fs::create_dir_all(skills_root.as_path()).with_context(|| {
        format!("failed to create managed skills directory {}", skills_root.display())
    })?;

    let trust_store_path = resolve_skills_trust_store_path(command.trust_store.as_deref())?;
    let mut trust_store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    let trusted_publishers = command.trusted_publishers.clone();
    for trusted in &trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        trust_store.add_trusted_key(publisher, key)?;
    }

    let resolved = resolve_install_artifact(&command, &mut trust_store, command.allow_untrusted)?;
    let artifact_sha256 = sha256_hex(resolved.artifact_bytes.as_slice());
    if artifact_sha256 != resolved.entry.artifact_sha256 {
        anyhow::bail!(
            "registry hash mismatch for {} {}: expected {} got {}",
            resolved.entry.skill_id,
            resolved.entry.version,
            resolved.entry.artifact_sha256,
            artifact_sha256
        );
    }
    let inspected = inspect_skill_artifact(resolved.artifact_bytes.as_slice())
        .context("skill artifact failed structural verification")?;
    if inspected.manifest.skill_id != resolved.entry.skill_id
        || inspected.manifest.version != resolved.entry.version
        || inspected.manifest.publisher != resolved.entry.publisher
    {
        anyhow::bail!(
            "registry metadata mismatch for artifact {}: expected skill_id={} version={} publisher={}, got skill_id={} version={} publisher={}",
            resolved.source.reference,
            resolved.entry.skill_id,
            resolved.entry.version,
            resolved.entry.publisher,
            inspected.manifest.skill_id,
            inspected.manifest.version,
            inspected.manifest.publisher
        );
    }
    let verification_report = verify_skill_artifact(
        resolved.artifact_bytes.as_slice(),
        &mut trust_store,
        command.allow_untrusted,
    )
    .context("failed to verify skill artifact trust policy")?;
    let security_report = audit_skill_artifact_security(
        resolved.artifact_bytes.as_slice(),
        &mut trust_store,
        command.allow_untrusted,
        &SkillSecurityAuditPolicy::default(),
    )
    .context("failed to evaluate skill security audit policy during install")?;
    save_trust_store_with_integrity(trust_store_path.as_path(), &trust_store)?;
    append_skills_audit_event(
        skills_root.as_path(),
        "skill.audit",
        json!({
            "skill_id": verification_report.manifest.skill_id,
            "version": verification_report.manifest.version,
            "publisher": verification_report.manifest.publisher,
            "source": resolved.source.reference,
            "passed": security_report.passed,
            "should_quarantine": security_report.should_quarantine,
            "quarantine_reasons": security_report.quarantine_reasons,
            "checks": security_report.checks,
        }),
    )?;
    if security_report.should_quarantine {
        append_skills_audit_event(
            skills_root.as_path(),
            "skill.quarantined",
            json!({
                "skill_id": verification_report.manifest.skill_id,
                "version": verification_report.manifest.version,
                "publisher": verification_report.manifest.publisher,
                "reason": "static_security_audit_failed",
                "quarantine_reasons": security_report.quarantine_reasons,
            }),
        )?;
        anyhow::bail!(
            "skill security audit requires quarantine for {} {}: {}",
            verification_report.manifest.skill_id,
            verification_report.manifest.version,
            security_report.quarantine_reasons.join(" | ")
        );
    }

    let missing_secrets = resolve_and_prompt_missing_skill_secrets(
        &verification_report.manifest,
        command.non_interactive,
    )?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let outcome = install_verified_skill_artifact(
        skills_root.as_path(),
        &mut index,
        resolved.artifact_bytes.as_slice(),
        &inspected,
        &verification_report,
        InstallMetadataContext {
            source: resolved.source.clone(),
            artifact_sha256,
            missing_secrets,
        },
    )?;
    save_installed_skills_index(skills_root.as_path(), &index)?;

    let event_kind = if outcome.previous_current_version.is_some() {
        "skill.updated"
    } else {
        "skill.installed"
    };
    append_skills_audit_event(
        skills_root.as_path(),
        event_kind,
        json!({
            "skill_id": outcome.record.skill_id,
            "version": outcome.record.version,
            "publisher": outcome.record.publisher,
            "artifact_sha256": outcome.record.artifact_sha256,
            "payload_sha256": outcome.record.payload_sha256,
            "signature_key_id": outcome.record.signature_key_id,
            "trust_decision": outcome.record.trust_decision,
            "source": outcome.record.source,
            "missing_secrets": outcome.record.missing_secrets,
            "previous_version": outcome.previous_current_version,
        }),
    )?;

    if command.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "event_kind": event_kind,
                "skill_id": outcome.record.skill_id,
                "version": outcome.record.version,
                "publisher": outcome.record.publisher,
                "artifact_sha256": outcome.record.artifact_sha256,
                "payload_sha256": outcome.record.payload_sha256,
                "signature_key_id": outcome.record.signature_key_id,
                "trust_decision": outcome.record.trust_decision,
                "source": outcome.record.source,
                "missing_secrets": outcome.record.missing_secrets,
                "skills_root": skills_root,
                "trust_store": trust_store_path,
            }))?
        );
    } else {
        println!(
            "{} skill_id={} version={} publisher={} trust={} source={} missing_secrets={} skills_root={} trust_store={}",
            event_kind,
            outcome.record.skill_id,
            outcome.record.version,
            outcome.record.publisher,
            outcome.record.trust_decision,
            outcome.record.source.reference,
            outcome.record.missing_secrets.len(),
            skills_root.display(),
            trust_store_path.display()
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_update(command: SkillsUpdateCommand) -> Result<()> {
    if command.registry_dir.is_some() == command.registry_url.is_some() {
        anyhow::bail!(
            "skills update requires exactly one source: --registry-dir or --registry-url"
        );
    }
    let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
    fs::create_dir_all(skills_root.as_path()).with_context(|| {
        format!("failed to create managed skills directory {}", skills_root.display())
    })?;
    let index = load_installed_skills_index(skills_root.as_path())?;
    let current_version = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == command.skill_id && entry.current)
        .map(|entry| entry.version.clone());

    let trust_store_path = resolve_skills_trust_store_path(command.trust_store.as_deref())?;
    let mut trust_store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    let trusted_publishers = command.trusted_publishers.clone();
    for trusted in &trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        trust_store.add_trusted_key(publisher, key)?;
    }
    let resolved = resolve_registry_artifact_for_skill(
        command.registry_dir.as_deref(),
        command.registry_url.as_deref(),
        command.registry_ca_cert.as_deref(),
        command.skill_id.as_str(),
        command.version.as_deref(),
        &mut trust_store,
        command.allow_untrusted,
    )?;
    if current_version.as_deref() == Some(resolved.entry.version.as_str()) {
        save_trust_store_with_integrity(trust_store_path.as_path(), &trust_store)?;
        if command.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "event_kind": "skill.updated",
                    "updated": false,
                    "reason": "already_current",
                    "skill_id": command.skill_id,
                    "version": resolved.entry.version,
                    "skills_root": skills_root,
                }))?
            );
        } else {
            println!(
                "skill.updated updated=false reason=already_current skill_id={} version={} skills_root={}",
                command.skill_id,
                resolved.entry.version,
                skills_root.display()
            );
        }
        return std::io::stdout().flush().context("stdout flush failed");
    }

    save_trust_store_with_integrity(trust_store_path.as_path(), &trust_store)?;

    let install_command = SkillsInstallCommand {
        artifact: None,
        registry_dir: command.registry_dir,
        registry_url: command.registry_url,
        skill_id: Some(command.skill_id),
        version: command.version,
        registry_ca_cert: command.registry_ca_cert,
        skills_dir: Some(skills_root.to_string_lossy().into_owned()),
        trust_store: Some(trust_store_path.to_string_lossy().into_owned()),
        trusted_publishers,
        allow_untrusted: command.allow_untrusted,
        non_interactive: command.non_interactive,
        json: command.json,
    };
    run_skills_install(install_command)
}

fn run_skills_remove(
    skill_id: String,
    version: Option<String>,
    skills_dir: Option<String>,
    json_output: bool,
) -> Result<()> {
    let skills_root = resolve_skills_root(skills_dir.as_deref())?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let target_positions = if let Some(version) = version.as_deref() {
        let selected = index
            .entries
            .iter()
            .enumerate()
            .filter_map(|(position, entry)| {
                (entry.skill_id == skill_id && entry.version == version).then_some(position)
            })
            .collect::<Vec<_>>();
        if selected.is_empty() {
            anyhow::bail!("skill {} version {} is not installed", skill_id, version);
        }
        selected
    } else {
        let Some(current_position) =
            index.entries.iter().position(|entry| entry.skill_id == skill_id && entry.current)
        else {
            anyhow::bail!("skill {} has no current installed version; pass --version", skill_id);
        };
        vec![current_position]
    };

    let mut removed_versions = target_positions
        .iter()
        .map(|position| index.entries[*position].version.clone())
        .collect::<Vec<_>>();
    removed_versions.sort();
    removed_versions.dedup();

    for version in &removed_versions {
        let path = skills_root.join(skill_id.as_str()).join(version);
        if path.exists() {
            fs::remove_dir_all(path.as_path()).with_context(|| {
                format!("failed to remove installed skill directory {}", path.display())
            })?;
        }
    }
    index.entries.retain(|entry| {
        !(entry.skill_id == skill_id
            && removed_versions.iter().any(|version| version == &entry.version))
    });
    normalize_installed_skills_index(&mut index);
    if let Some(current) = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.current)
        .map(|entry| entry.version.clone())
    {
        if let Err(error) = update_skill_current_pointer(
            skills_root.join(skill_id.as_str()).as_path(),
            current.as_str(),
        ) {
            eprintln!(
                "warning: failed to update optional '{}' pointer for skill {}: {}",
                SKILLS_CURRENT_LINK_NAME, skill_id, error
            );
        }
    } else if let Err(error) =
        remove_skill_current_pointer(skills_root.join(skill_id.as_str()).as_path())
    {
        eprintln!(
            "warning: failed to remove optional '{}' pointer for skill {}: {}",
            SKILLS_CURRENT_LINK_NAME, skill_id, error
        );
    }
    save_installed_skills_index(skills_root.as_path(), &index)?;
    append_skills_audit_event(
        skills_root.as_path(),
        "skill.removed",
        json!({
            "skill_id": skill_id,
            "removed_versions": removed_versions,
        }),
    )?;

    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "event_kind": "skill.removed",
                "skill_id": skill_id,
                "removed_versions": removed_versions,
                "skills_root": skills_root,
            }))?
        );
    } else {
        println!(
            "skill.removed skill_id={} removed_versions={} skills_root={}",
            skill_id,
            removed_versions.join(","),
            skills_root.display()
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_list(skills_dir: Option<String>, json_output: bool) -> Result<()> {
    let skills_root = resolve_skills_root(skills_dir.as_deref())?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    normalize_installed_skills_index(&mut index);
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "skills_root": skills_root,
                "count": index.entries.len(),
                "entries": index.entries,
            }))?
        );
    } else {
        println!("skills.list root={} count={}", skills_root.display(), index.entries.len());
        for entry in &index.entries {
            println!(
                "skills.entry skill_id={} version={} publisher={} current={} trust={} installed_at_unix_ms={} source={}",
                entry.skill_id,
                entry.version,
                entry.publisher,
                entry.current,
                entry.trust_decision,
                entry.installed_at_unix_ms,
                entry.source.reference
            );
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_verify(
    skill_id: String,
    version: Option<String>,
    skills_dir: Option<String>,
    trust_store: Option<String>,
    trusted_publishers: Vec<String>,
    allow_untrusted: bool,
    json_output: bool,
) -> Result<()> {
    let skills_root = resolve_skills_root(skills_dir.as_deref())?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let record_index = find_installed_skill_record(&index, skill_id.as_str(), version.as_deref())?;
    let record = index.entries[record_index].clone();
    let artifact_path = skills_root
        .join(record.skill_id.as_str())
        .join(record.version.as_str())
        .join(SKILLS_ARTIFACT_FILE_NAME);
    let artifact_bytes = fs::read(artifact_path.as_path()).with_context(|| {
        format!("failed to read installed artifact {}", artifact_path.display())
    })?;

    let trust_store_path = resolve_skills_trust_store_path(trust_store.as_deref())?;
    let mut store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    for trusted in trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        store.add_trusted_key(publisher, key)?;
    }
    let report = verify_skill_artifact(artifact_bytes.as_slice(), &mut store, allow_untrusted)
        .context("failed to verify installed skill artifact")?;
    save_trust_store_with_integrity(trust_store_path.as_path(), &store)?;

    index.entries[record_index].trust_decision =
        trust_decision_label(report.trust_decision).to_owned();
    index.entries[record_index].payload_sha256 = report.payload_sha256.clone();
    save_installed_skills_index(skills_root.as_path(), &index)?;
    append_skills_audit_event(
        skills_root.as_path(),
        "skill.verified",
        json!({
            "skill_id": report.manifest.skill_id,
            "version": report.manifest.version,
            "publisher": report.manifest.publisher,
            "payload_sha256": report.payload_sha256,
            "trust_decision": trust_decision_label(report.trust_decision),
            "accepted": report.accepted,
            "policy_bindings": report.policy_bindings,
        }),
    )?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "skill.verified skill_id={} version={} publisher={} accepted={} trust={} payload_sha256={} trust_store={}",
            report.manifest.skill_id,
            report.manifest.version,
            report.manifest.publisher,
            report.accepted,
            trust_decision_label(report.trust_decision),
            report.payload_sha256,
            trust_store_path.display()
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

#[derive(Debug, Clone)]
struct SkillAuditTarget {
    artifact_path: PathBuf,
    source: String,
    skill_id: Option<String>,
    version: Option<String>,
}

fn run_skills_audit(command: SkillsAuditCommand) -> Result<()> {
    let trust_store_path = resolve_skills_trust_store_path(command.trust_store.as_deref())?;
    let mut store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    for trusted in &command.trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        store.add_trusted_key(publisher, key)?;
    }

    let mut targets = Vec::new();
    let mut managed_skills_root: Option<PathBuf> = None;
    if let Some(artifact) = command.artifact.as_deref() {
        let artifact_path = PathBuf::from(artifact);
        targets.push(SkillAuditTarget {
            artifact_path,
            source: "artifact".to_owned(),
            skill_id: command.skill_id.clone(),
            version: command.version.clone(),
        });
    } else {
        let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
        let index = load_installed_skills_index(skills_root.as_path())?;
        managed_skills_root = Some(skills_root.clone());
        if let Some(skill_id) = command.skill_id.as_deref() {
            let record_index =
                find_installed_skill_record(&index, skill_id, command.version.as_deref())?;
            let record = &index.entries[record_index];
            targets.push(SkillAuditTarget {
                artifact_path: skills_root
                    .join(record.skill_id.as_str())
                    .join(record.version.as_str())
                    .join(SKILLS_ARTIFACT_FILE_NAME),
                source: "installed".to_owned(),
                skill_id: Some(record.skill_id.clone()),
                version: Some(record.version.clone()),
            });
        } else {
            let mut records =
                index.entries.iter().filter(|entry| entry.current).collect::<Vec<_>>();
            if records.is_empty() {
                records = index.entries.iter().collect::<Vec<_>>();
            }
            for record in records {
                targets.push(SkillAuditTarget {
                    artifact_path: skills_root
                        .join(record.skill_id.as_str())
                        .join(record.version.as_str())
                        .join(SKILLS_ARTIFACT_FILE_NAME),
                    source: "installed".to_owned(),
                    skill_id: Some(record.skill_id.clone()),
                    version: Some(record.version.clone()),
                });
            }
        }
    }

    if targets.is_empty() {
        anyhow::bail!(
            "no skill artifacts were selected for audit; pass --artifact or install at least one skill first"
        );
    }

    let mut reports = Vec::new();
    for target in &targets {
        let artifact_bytes = fs::read(target.artifact_path.as_path()).with_context(|| {
            format!("failed to read skill artifact for audit {}", target.artifact_path.display())
        })?;
        let report = audit_skill_artifact_security(
            artifact_bytes.as_slice(),
            &mut store,
            command.allow_untrusted,
            &SkillSecurityAuditPolicy::default(),
        )
        .with_context(|| {
            format!("failed to audit skill artifact security {}", target.artifact_path.display())
        })?;
        reports.push((target.clone(), report));
    }
    save_trust_store_with_integrity(trust_store_path.as_path(), &store)?;

    if let Some(skills_root) = managed_skills_root.as_deref() {
        for (target, report) in &reports {
            append_skills_audit_event(
                skills_root,
                "skill.audit",
                json!({
                    "source": target.source,
                    "artifact": target.artifact_path,
                    "skill_id": target.skill_id,
                    "version": target.version,
                    "should_quarantine": report.should_quarantine,
                    "quarantine_reasons": report.quarantine_reasons,
                    "checks": report.checks,
                }),
            )?;
        }
    }

    let output_payload = json!({
        "trust_store": trust_store_path,
        "audits": reports
            .iter()
            .map(|(target, report)| {
                json!({
                    "source": target.source,
                    "artifact": target.artifact_path,
                    "skill_id": target.skill_id,
                    "version": target.version,
                    "report": report,
                })
            })
            .collect::<Vec<_>>(),
    });
    let quarantine_required = reports.iter().any(|(_, report)| report.should_quarantine);

    if command.json {
        println!("{}", serde_json::to_string_pretty(&output_payload)?);
    } else {
        for (target, report) in &reports {
            let skill_label = target
                .skill_id
                .as_deref()
                .map(|value| value.to_owned())
                .unwrap_or_else(|| "unknown".to_owned());
            let version_label = target
                .version
                .as_deref()
                .map(|value| value.to_owned())
                .unwrap_or_else(|| "unknown".to_owned());
            println!(
                "skill.audit skill_id={} version={} source={} artifact={} passed={} should_quarantine={} failed_checks={} warnings={}",
                skill_label,
                version_label,
                target.source,
                target.artifact_path.display(),
                report.passed,
                report.should_quarantine,
                report
                    .checks
                    .iter()
                    .filter(|check| matches!(check.status, SkillAuditCheckStatus::Fail))
                    .count(),
                report
                    .checks
                    .iter()
                    .filter(|check| matches!(check.status, SkillAuditCheckStatus::Warn))
                    .count()
            );
            if report.should_quarantine && !report.quarantine_reasons.is_empty() {
                println!(
                    "skill.audit.quarantine_reasons {}",
                    report.quarantine_reasons.join(" | ")
                );
            }
        }
    }
    std::io::stdout().flush().context("stdout flush failed")?;
    if quarantine_required {
        anyhow::bail!(
            "one or more audited skills require quarantine; inspect report output for details"
        );
    }
    Ok(())
}

fn run_skills_quarantine(command: SkillsQuarantineCommand) -> Result<()> {
    let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
    let version = resolve_skills_status_version(
        skills_root.as_path(),
        command.skill_id.as_str(),
        command.version.as_deref(),
    )?;
    let base_url = command
        .url
        .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
        .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
    let endpoint = format!(
        "{}/admin/v1/skills/{}/quarantine",
        base_url.trim_end_matches('/'),
        command.skill_id
    );
    let token = command.token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let mut request = client
        .post(endpoint)
        .header("x-palyra-principal", command.principal)
        .header("x-palyra-device-id", command.device_id);
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    if let Some(channel) = command.channel {
        request = request.header("x-palyra-channel", channel);
    }
    let response: SkillStatusResponse = request
        .json(&SkillStatusRequestBody { version, reason: command.reason, override_enabled: None })
        .send()
        .context("failed to call daemon skills quarantine endpoint")?
        .error_for_status()
        .context("daemon skills quarantine endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon skills quarantine response")?;

    append_skills_audit_event(
        skills_root.as_path(),
        "skill.quarantined",
        json!({
            "skill_id": response.skill_id,
            "version": response.version,
            "status": response.status,
            "reason": response.reason,
            "detected_at_ms": response.detected_at_ms,
            "operator_principal": response.operator_principal,
        }),
    )?;

    if command.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!(
            "skill.quarantined skill_id={} version={} status={} reason={} detected_at_ms={} operator_principal={}",
            response.skill_id,
            response.version,
            response.status,
            response.reason.clone().unwrap_or_else(|| "none".to_owned()),
            response.detected_at_ms,
            response.operator_principal,
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_enable(command: SkillsEnableCommand) -> Result<()> {
    if !command.override_enabled {
        anyhow::bail!("skills enable requires --override");
    }
    let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
    let version = resolve_skills_status_version(
        skills_root.as_path(),
        command.skill_id.as_str(),
        command.version.as_deref(),
    )?;
    let base_url = command
        .url
        .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
        .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned());
    let endpoint =
        format!("{}/admin/v1/skills/{}/enable", base_url.trim_end_matches('/'), command.skill_id);
    let token = command.token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok());
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let mut request = client
        .post(endpoint)
        .header("x-palyra-principal", command.principal)
        .header("x-palyra-device-id", command.device_id);
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    if let Some(channel) = command.channel {
        request = request.header("x-palyra-channel", channel);
    }
    let response: SkillStatusResponse = request
        .json(&SkillStatusRequestBody {
            version,
            reason: command.reason,
            override_enabled: Some(true),
        })
        .send()
        .context("failed to call daemon skills enable endpoint")?
        .error_for_status()
        .context("daemon skills enable endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon skills enable response")?;

    append_skills_audit_event(
        skills_root.as_path(),
        "skill.enabled",
        json!({
            "skill_id": response.skill_id,
            "version": response.version,
            "status": response.status,
            "reason": response.reason,
            "detected_at_ms": response.detected_at_ms,
            "operator_principal": response.operator_principal,
        }),
    )?;

    if command.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!(
            "skill.enabled skill_id={} version={} status={} reason={} detected_at_ms={} operator_principal={}",
            response.skill_id,
            response.version,
            response.status,
            response.reason.clone().unwrap_or_else(|| "none".to_owned()),
            response.detected_at_ms,
            response.operator_principal,
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
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
    let identity_root =
        default_identity_store_root().context("failed to resolve default identity store root")?;
    let state_root =
        identity_root.parent().map(Path::to_path_buf).unwrap_or_else(|| identity_root.clone());
    Ok(state_root.join("skills"))
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

fn prompt_yes_no(prompt: &str) -> Result<bool> {
    eprint!("{prompt}");
    std::io::stderr().flush().context("stderr flush failed")?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).context("failed to read interactive answer")?;
    let normalized = answer.trim().to_ascii_lowercase();
    Ok(matches!(normalized.as_str(), "y" | "yes"))
}

fn prompt_secret_value(prompt: &str) -> Result<String> {
    eprint!("{prompt}");
    std::io::stderr().flush().context("stderr flush failed")?;
    let value = rpassword::read_password().context("failed to read secret value")?;
    Ok(normalize_prompt_secret_value(&value))
}

fn normalize_prompt_secret_value(raw: &str) -> String {
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

fn run_secrets(command: SecretsCommand) -> Result<()> {
    let vault = open_cli_vault().context("failed to initialize vault runtime")?;
    match command {
        SecretsCommand::Set { scope, key, value_stdin } => {
            if !value_stdin {
                anyhow::bail!(
                    "secrets set requires --value-stdin to avoid exposing raw values in process args"
                );
            }
            let scope = parse_vault_scope(scope.as_str())?;
            let mut value = Vec::new();
            std::io::stdin()
                .read_to_end(&mut value)
                .context("failed to read secret value from stdin")?;
            if value.is_empty() {
                anyhow::bail!("stdin did not contain any secret bytes");
            }
            let metadata = vault
                .put_secret(&scope, key.as_str(), value.as_slice())
                .with_context(|| format!("failed to store secret key={} scope={scope}", key))?;
            println!(
                "secrets.set scope={} key={} value_bytes={} backend={}",
                scope,
                metadata.key,
                metadata.value_bytes,
                vault.backend_kind().as_str(),
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        SecretsCommand::Get { scope, key, reveal } => {
            let scope = parse_vault_scope(scope.as_str())?;
            let value = vault
                .get_secret(&scope, key.as_str())
                .with_context(|| format!("failed to load secret key={} scope={scope}", key))?;
            if reveal {
                eprintln!(
                    "warning: printing secret bytes to stdout can leak via shell history or logs"
                );
                std::io::stdout()
                    .write_all(value.as_slice())
                    .context("failed to write secret value to stdout")?;
            } else {
                println!(
                    "secrets.get scope={} key={} value=<redacted> value_bytes={} reveal=false",
                    scope,
                    key,
                    value.len()
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        SecretsCommand::List { scope } => {
            let scope = parse_vault_scope(scope.as_str())?;
            let secrets = vault
                .list_secrets(&scope)
                .with_context(|| format!("failed to list secrets for scope={scope}"))?;
            println!(
                "secrets.list scope={} count={} backend={}",
                scope,
                secrets.len(),
                vault.backend_kind().as_str()
            );
            for metadata in secrets {
                println!(
                    "secrets.entry key={} created_at_unix_ms={} updated_at_unix_ms={} value_bytes={}",
                    metadata.key,
                    metadata.created_at_unix_ms,
                    metadata.updated_at_unix_ms,
                    metadata.value_bytes
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        SecretsCommand::Delete { scope, key } => {
            let scope = parse_vault_scope(scope.as_str())?;
            let deleted = vault
                .delete_secret(&scope, key.as_str())
                .with_context(|| format!("failed to delete secret key={} scope={scope}", key))?;
            println!("secrets.delete scope={} key={} deleted={}", scope, key, deleted);
            std::io::stdout().flush().context("stdout flush failed")
        }
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

fn resolve_identity_store_root(store_dir: Option<String>) -> Result<PathBuf> {
    if let Some(path) = store_dir {
        return Ok(PathBuf::from(path));
    }
    default_identity_store_root().context("failed to resolve default identity store root")
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

#[derive(Clone, Copy, Debug, Serialize)]
struct DoctorCheck {
    key: &'static str,
    ok: bool,
    required: bool,
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    generated_at_unix_ms: i64,
    checks: Vec<DoctorCheck>,
    summary: DoctorSummary,
    config: DoctorConfigSnapshot,
    identity: DoctorIdentitySnapshot,
    connectivity: DoctorConnectivitySnapshot,
    provider_auth: DoctorProviderAuthSnapshot,
    sandbox: DoctorSandboxSnapshot,
}

#[derive(Debug, Serialize)]
struct DoctorSummary {
    required_checks_total: usize,
    required_checks_ok: usize,
    required_checks_failed: usize,
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
struct DoctorSandboxSnapshot {
    tier_b_egress_allowlists_preflight_only: bool,
    tier_c_strict_offline_only: bool,
    tier_c_windows_backend_supported: bool,
}

#[derive(Debug, Serialize)]
struct SupportBundle {
    schema_version: u32,
    generated_at_unix_ms: i64,
    build: SupportBundleBuildSnapshot,
    platform: SupportBundlePlatformSnapshot,
    doctor: DoctorReport,
    config: SupportBundleConfigSnapshot,
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
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct SupportBundleDiagnosticsSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    admin_status: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    admin_status_error: Option<String>,
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
    #[serde(default)]
    returned_bytes: usize,
    next_after_seq: Option<i64>,
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
    use super::{
        build_journal_checkpoint_attestation, compare_semver_versions,
        ensure_remote_registry_same_origin, fetch_limited_bytes,
        fetch_remote_registry_entries_with_fetcher, is_retryable_grpc_error,
        normalize_client_socket, normalize_installed_skills_index, normalize_prompt_secret_value,
        normalize_relative_registry_path, parse_acp_shim_input_line,
        parse_and_verify_signed_remote_registry_index,
        process_runner_tier_b_allowlist_preflight_only,
        process_runner_tier_c_strict_offline_allowlists_empty,
        process_runner_tier_c_windows_backend_supported, registry_key_id_for, sha256_hex,
        trust_store_integrity_vault_key, validate_registry_index,
        verify_or_initialize_trust_store_integrity, write_file_atomically, InstalledSkillRecord,
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
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::Duration;

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
mod diagnostics_bundle_tests {
    use super::{
        encode_support_bundle_with_cap, extract_support_bundle_error_message, DoctorConfigSnapshot,
        DoctorConnectivityProbe, DoctorConnectivitySnapshot, DoctorIdentitySnapshot,
        DoctorProviderAuthSnapshot, DoctorReport, DoctorSandboxSnapshot, DoctorSummary,
        SupportBundle, SupportBundleBuildSnapshot, SupportBundleConfigSnapshot,
        SupportBundleDiagnosticsSnapshot, SupportBundleJournalErrorRecord,
        SupportBundleJournalSnapshot,
    };
    use serde_json::{json, Value};

    fn minimal_doctor_report() -> DoctorReport {
        DoctorReport {
            generated_at_unix_ms: 1_730_000_000_000,
            checks: Vec::new(),
            summary: DoctorSummary {
                required_checks_total: 2,
                required_checks_ok: 2,
                required_checks_failed: 0,
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
            sandbox: DoctorSandboxSnapshot {
                tier_b_egress_allowlists_preflight_only: true,
                tier_c_strict_offline_only: true,
                tier_c_windows_backend_supported: true,
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
            config: SupportBundleConfigSnapshot {
                path: Some("palyra.toml".to_owned()),
                redacted_document: Some(json!({
                    "model_provider": {
                        "openai_api_key": "<redacted>",
                        "openai_api_key_vault_ref": "<redacted>",
                        "huge": "x".repeat(24_000),
                    }
                })),
                error: None,
            },
            diagnostics: SupportBundleDiagnosticsSnapshot {
                admin_status: Some(json!({
                    "model_provider": {
                        "kind": "openai-compatible",
                        "runtime_metrics": {
                            "request_count": 12345
                        }
                    }
                })),
                admin_status_error: None,
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
