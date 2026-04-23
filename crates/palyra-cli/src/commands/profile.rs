use std::{
    fs,
    io::{Read, Write},
    num::NonZeroU32,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ring::{
    aead, pbkdf2,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};

use crate::{
    app::{self, CliConnectionProfile, CliProfilesDocument},
    args::{ProfileCommand, ProfileExportModeArg, ProfileModeArg, ProfileRiskLevelArg},
    output, parse_document_with_migration, redact_secret_config_values, sha256_hex,
    validate_daemon_compatible_document, write_document_with_backups, VaultRef,
};

const PROFILE_EXPORT_SCHEMA_VERSION: u32 = 1;
const PROFILE_EXPORT_ENCRYPTED_KIND: &str = "palyra_cli_profile_bundle_encrypted_v1";
const PROFILE_EXPORT_CIPHER: &str = "aes_256_gcm";
const PROFILE_EXPORT_KDF: &str = "pbkdf2_hmac_sha256";
const PROFILE_EXPORT_PBKDF2_ITERATIONS: u32 = 120_000;
const PROFILE_EXPORT_SALT_LEN: usize = 16;
const PROFILE_EXPORT_NONCE_LEN: usize = 12;
const PROFILE_REDACTED_VALUE: &str = "<redacted>";
const PROFILE_CONFIG_WRITE_BACKUPS: usize = 1;
const PROFILE_AEAD_AAD: &[u8] = b"palyra.cli.profile_bundle.v1";

#[derive(Debug, Clone, Serialize)]
struct ProfileListRecord {
    name: String,
    active: bool,
    is_default: bool,
    label: Option<String>,
    environment: Option<String>,
    color: Option<String>,
    risk_level: Option<String>,
    strict_mode: bool,
    mode: Option<String>,
    config_path: Option<String>,
    state_root: Option<String>,
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
    admin_token_env: Option<String>,
    updated_at_unix_ms: Option<i64>,
    last_used_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
struct ProfileListPayload {
    registry_path: String,
    active_profile: Option<String>,
    default_profile: Option<String>,
    profiles: Vec<ProfileListRecord>,
}

#[derive(Debug, Clone, Serialize)]
struct ProfileMutationPayload {
    action: &'static str,
    registry_path: String,
    active_profile: Option<String>,
    default_profile: Option<String>,
    profile: Option<ProfileListRecord>,
    removed_profile: Option<String>,
    removed_state_root: Option<String>,
    source_profile: Option<String>,
    bundle_path: Option<String>,
    validation: Option<ProfileValidationReport>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ProfileValidationFinding {
    severity: String,
    code: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct ProfileValidationSummary {
    blocking_findings: usize,
    warning_findings: usize,
    info_findings: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ProfileValidationReport {
    profile_name: String,
    config_path: Option<String>,
    state_root: Option<String>,
    config_snapshot_written: bool,
    isolated_state_root: bool,
    isolated_config_path: bool,
    findings: Vec<ProfileValidationFinding>,
    summary: ProfileValidationSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PortableProfileRecord {
    name: String,
    label: Option<String>,
    environment: Option<String>,
    color: Option<String>,
    risk_level: Option<String>,
    strict_mode: bool,
    mode: Option<String>,
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    admin_token_env: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
    source_config_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PortableProfileConfig {
    source_path: String,
    redacted: bool,
    sha256: String,
    content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProfileSecretReference {
    component_path: String,
    reference: String,
    scope: String,
    key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProfilePortabilityBundle {
    schema_version: u32,
    generated_at_unix_ms: i64,
    source_profile: String,
    export_mode: String,
    profile: PortableProfileRecord,
    config: Option<PortableProfileConfig>,
    secret_references: Vec<ProfileSecretReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncryptedProfileBundle {
    schema_version: u32,
    kind: String,
    cipher: String,
    kdf: String,
    iterations: u32,
    salt_b64: String,
    nonce_b64: String,
    ciphertext_b64: String,
}

#[derive(Debug)]
struct ProfileCreateRequest {
    name: String,
    mode: ProfileModeArg,
    label: Option<String>,
    environment: Option<String>,
    color: Option<String>,
    risk_level: Option<ProfileRiskLevelArg>,
    strict_mode: bool,
    config_path: Option<String>,
    state_root: Option<String>,
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    admin_token_env: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
    set_default: bool,
    force: bool,
    json: bool,
}

#[derive(Debug)]
struct ProfileCloneRequest {
    source_name: String,
    new_name: String,
    label: Option<String>,
    environment: Option<String>,
    color: Option<String>,
    risk_level: Option<ProfileRiskLevelArg>,
    strict_mode: bool,
    set_default: bool,
    force: bool,
    json: bool,
}

#[derive(Debug)]
struct LoadedProfileConfig {
    source_path: PathBuf,
    rendered_content: String,
    redacted_content: String,
    secret_references: Vec<ProfileSecretReference>,
}

#[derive(Debug)]
enum ProfileConfigLoadOutcome {
    Unconfigured,
    MissingPath { path: String },
    Invalid { path: String, error: String },
    Loaded(LoadedProfileConfig),
}

pub(crate) fn run_profile(command: ProfileCommand) -> Result<()> {
    match command {
        ProfileCommand::List { json, ndjson } => run_profile_list(json, ndjson),
        ProfileCommand::Show { name, json } => run_profile_show(name, json),
        ProfileCommand::Create {
            name,
            mode,
            label,
            environment,
            color,
            risk_level,
            strict_mode,
            config_path,
            state_root,
            daemon_url,
            grpc_url,
            admin_token_env,
            principal,
            device_id,
            channel,
            set_default,
            force,
            json,
        } => run_profile_create(ProfileCreateRequest {
            name,
            mode,
            label,
            environment,
            color,
            risk_level,
            strict_mode,
            config_path,
            state_root,
            daemon_url,
            grpc_url,
            admin_token_env,
            principal,
            device_id,
            channel,
            set_default,
            force,
            json,
        }),
        ProfileCommand::Clone {
            name,
            new_name,
            label,
            environment,
            color,
            risk_level,
            strict_mode,
            set_default,
            force,
            json,
        } => run_profile_clone(ProfileCloneRequest {
            source_name: name,
            new_name,
            label,
            environment,
            color,
            risk_level,
            strict_mode,
            set_default,
            force,
            json,
        }),
        ProfileCommand::Export { name, output, mode, password_stdin, json } => {
            run_profile_export(name, output, mode, password_stdin, json)
        }
        ProfileCommand::Import { input, name, password_stdin, set_default, force, json } => {
            run_profile_import(input, name, password_stdin, set_default, force, json)
        }
        ProfileCommand::Use { name, json } => run_profile_use(name, json),
        ProfileCommand::Rename { name, new_name, json } => run_profile_rename(name, new_name, json),
        ProfileCommand::Delete { name, yes, delete_state_root, json } => {
            run_profile_delete(name, yes, delete_state_root, json)
        }
    }
}

fn run_profile_list(json: bool, ndjson: bool) -> Result<()> {
    let (path, document) = app::load_cli_profiles_registry()?;
    let active_profile = current_profile_name();
    let payload = ProfileListPayload {
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profiles: document
            .profiles
            .iter()
            .map(|(name, profile)| {
                profile_record(
                    name.as_str(),
                    profile,
                    active_profile.as_deref(),
                    document.default_profile.as_deref(),
                )
            })
            .collect(),
    };
    if output::preferred_json(json) {
        return output::print_json_pretty(&payload, "failed to encode profile list output as JSON");
    }
    if output::preferred_ndjson(json, ndjson) {
        for record in payload.profiles.as_slice() {
            output::print_json_line(record, "failed to encode profile list output as NDJSON")?;
        }
        return Ok(());
    }
    println!(
        "profile.list registry_path={} active_profile={} default_profile={}",
        payload.registry_path,
        payload.active_profile.as_deref().unwrap_or("none"),
        payload.default_profile.as_deref().unwrap_or("none")
    );
    if payload.profiles.is_empty() {
        println!("profile.list.empty=true");
    }
    for record in payload.profiles.as_slice() {
        println!(
            "profile name={} active={} default={} environment={} risk_level={} strict_mode={} state_root={} config_path={}",
            record.name,
            record.active,
            record.is_default,
            record.environment.as_deref().unwrap_or("none"),
            record.risk_level.as_deref().unwrap_or("none"),
            record.strict_mode,
            record.state_root.as_deref().unwrap_or("none"),
            record.config_path.as_deref().unwrap_or("none"),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_profile_show(name: Option<String>, json: bool) -> Result<()> {
    let (path, document) = app::load_cli_profiles_registry()?;
    let active_profile = current_profile_name();
    let name = resolve_requested_profile_name(name, active_profile.as_deref(), &document)?;
    let profile = document
        .profiles
        .get(name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {name}"))?;
    let payload = ProfileMutationPayload {
        action: "show",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            name.as_str(),
            profile,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        source_profile: None,
        bundle_path: None,
        validation: None,
        warnings: Vec::new(),
    };
    emit_mutation_payload(&payload, json)
}

fn run_profile_create(request: ProfileCreateRequest) -> Result<()> {
    let name = app::validate_profile_name(request.name.as_str())?;
    let (path, mut document) = app::load_cli_profiles_registry()?;
    if document.profiles.contains_key(name.as_str()) && !request.force {
        anyhow::bail!("CLI profile already exists: {name} (pass --force to replace it)");
    }
    let state_root = resolve_profile_state_root(name.as_str(), request.state_root.as_deref())?;
    let now = now_unix_ms()?;
    let profile = CliConnectionProfile {
        config_path: normalize_optional_path(request.config_path.as_deref())?,
        state_root: Some(state_root.display().to_string()),
        daemon_url: app::normalized_profile_text(request.daemon_url.as_deref()),
        grpc_url: app::normalized_profile_text(request.grpc_url.as_deref()),
        admin_token: None,
        admin_token_env: app::normalized_profile_text(request.admin_token_env.as_deref()),
        principal: app::normalized_profile_text(request.principal.as_deref()),
        device_id: app::normalized_profile_text(request.device_id.as_deref()),
        channel: app::normalized_profile_text(request.channel.as_deref()),
        label: app::normalized_profile_text(request.label.as_deref()),
        environment: Some(
            app::normalized_profile_text(request.environment.as_deref())
                .unwrap_or_else(|| default_environment(request.mode).to_owned()),
        ),
        color: app::normalized_profile_text(request.color.as_deref()),
        risk_level: Some(
            request
                .risk_level
                .map(profile_risk_level_label)
                .unwrap_or_else(|| default_risk_level(request.mode).to_owned()),
        ),
        strict_mode: request.strict_mode || matches!(request.mode, ProfileModeArg::Remote),
        mode: Some(profile_mode_label(request.mode).to_owned()),
        created_at_unix_ms: Some(now),
        updated_at_unix_ms: Some(now),
        last_used_at_unix_ms: None,
    };
    document.profiles.insert(name.clone(), profile.clone());
    if request.set_default || document.default_profile.is_none() {
        document.default_profile = Some(name.clone());
    }
    app::persist_cli_profiles_registry(path.as_path(), &document)?;
    let active_profile =
        if request.set_default { Some(name.clone()) } else { current_profile_name() };
    let payload = ProfileMutationPayload {
        action: "create",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            name.as_str(),
            &profile,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        source_profile: None,
        bundle_path: None,
        validation: None,
        warnings: create_profile_warnings(&profile),
    };
    emit_mutation_payload(&payload, request.json)
}

fn run_profile_clone(request: ProfileCloneRequest) -> Result<()> {
    let source_name = app::validate_profile_name(request.source_name.as_str())?;
    let new_name = app::validate_profile_name(request.new_name.as_str())?;
    let (path, mut document) = app::load_cli_profiles_registry()?;
    if document.profiles.contains_key(new_name.as_str()) && !request.force {
        anyhow::bail!("CLI profile already exists: {new_name} (pass --force to replace it)");
    }
    let source_profile = document
        .profiles
        .get(source_name.as_str())
        .cloned()
        .ok_or_else(|| anyhow!("CLI profile not found: {source_name}"))?;

    let mut warnings = Vec::new();
    let config_outcome = load_profile_config(source_profile.config_path.as_deref())?;
    let config_write = match &config_outcome {
        ProfileConfigLoadOutcome::Loaded(config) => Some(write_profile_config_snapshot(
            new_name.as_str(),
            config.rendered_content.as_str(),
        )?),
        ProfileConfigLoadOutcome::Unconfigured => {
            warnings.push("source profile did not define a config_path to clone".to_owned());
            None
        }
        ProfileConfigLoadOutcome::MissingPath { path } => {
            warnings.push(format!("source profile config_path does not exist: {path}"));
            None
        }
        ProfileConfigLoadOutcome::Invalid { path, error } => {
            warnings.push(format!("source profile config_path is invalid: {path} ({error})"));
            None
        }
    };

    let state_root = resolve_profile_state_root(new_name.as_str(), None)?;
    let mut cloned = source_profile.clone();
    let now = now_unix_ms()?;
    cloned.config_path = config_write.as_ref().map(|value| value.display().to_string());
    cloned.state_root = Some(state_root.display().to_string());
    cloned.label = request.label.or_else(|| cloned.label.clone());
    cloned.environment = request.environment.or_else(|| cloned.environment.clone());
    cloned.color = request.color.or_else(|| cloned.color.clone());
    if let Some(risk_level) = request.risk_level {
        cloned.risk_level = Some(profile_risk_level_label(risk_level));
    }
    if request.strict_mode {
        cloned.strict_mode = true;
    }
    cloned.created_at_unix_ms = Some(now);
    cloned.updated_at_unix_ms = Some(now);
    cloned.last_used_at_unix_ms = None;

    document.profiles.insert(new_name.clone(), cloned.clone());
    if request.set_default || document.default_profile.is_none() {
        document.default_profile = Some(new_name.clone());
    }
    app::persist_cli_profiles_registry(path.as_path(), &document)?;

    let validation = build_clone_validation_report(
        new_name.as_str(),
        &cloned,
        &config_outcome,
        config_write.is_some(),
    );
    let active_profile =
        if request.set_default { Some(new_name.clone()) } else { current_profile_name() };
    let mut payload_warnings = create_profile_warnings(&cloned);
    payload_warnings.extend(warnings);
    let payload = ProfileMutationPayload {
        action: "clone",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            new_name.as_str(),
            &cloned,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        source_profile: Some(source_name),
        bundle_path: None,
        validation: Some(validation),
        warnings: payload_warnings,
    };
    emit_mutation_payload(&payload, request.json)
}

fn run_profile_export(
    name: Option<String>,
    output_path: String,
    mode: ProfileExportModeArg,
    password_stdin: bool,
    json: bool,
) -> Result<()> {
    let (path, document) = app::load_cli_profiles_registry()?;
    let active_profile = current_profile_name();
    let profile_name = resolve_requested_profile_name(name, active_profile.as_deref(), &document)?;
    let profile = document
        .profiles
        .get(profile_name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {profile_name}"))?;
    let output_path = parse_profile_path(output_path.trim(), "profile export output")
        .context("invalid export output path")?;
    if let Some(parent) = output_path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut warnings = Vec::new();
    let config_outcome = load_profile_config(profile.config_path.as_deref())?;
    let (config, secret_references) = match config_outcome {
        ProfileConfigLoadOutcome::Loaded(config) => {
            let content = if matches!(mode, ProfileExportModeArg::Redacted) {
                config.redacted_content
            } else {
                config.rendered_content
            };
            let exported = PortableProfileConfig {
                source_path: config.source_path.display().to_string(),
                redacted: matches!(mode, ProfileExportModeArg::Redacted),
                sha256: sha256_hex(content.as_bytes()),
                content,
            };
            (Some(exported), config.secret_references)
        }
        ProfileConfigLoadOutcome::Unconfigured => {
            warnings
                .push("profile export is metadata-only because config_path is unset".to_owned());
            (None, Vec::new())
        }
        ProfileConfigLoadOutcome::MissingPath { path } => {
            warnings.push(format!("profile export skipped missing config_path: {path}"));
            (None, Vec::new())
        }
        ProfileConfigLoadOutcome::Invalid { path, error } => {
            warnings.push(format!("profile export skipped invalid config_path {path}: {error}"));
            (None, Vec::new())
        }
    };

    let bundle = ProfilePortabilityBundle {
        schema_version: PROFILE_EXPORT_SCHEMA_VERSION,
        generated_at_unix_ms: now_unix_ms()?,
        source_profile: profile_name.clone(),
        export_mode: profile_export_mode_label(mode).to_owned(),
        profile: portable_profile_record(profile_name.as_str(), profile),
        config,
        secret_references,
    };
    let bundle_bytes =
        serde_json::to_vec_pretty(&bundle).context("failed to encode profile export bundle")?;
    let output_bytes = if matches!(mode, ProfileExportModeArg::Encrypted) {
        let password = read_password_from_stdin(password_stdin)?;
        serde_json::to_vec_pretty(&encrypt_profile_bundle(
            bundle_bytes.as_slice(),
            password.as_slice(),
        )?)
        .context("failed to encode encrypted profile bundle")?
    } else {
        if password_stdin {
            warnings.push(
                "--password-stdin was ignored because export mode is redacted, not encrypted"
                    .to_owned(),
            );
        }
        bundle_bytes
    };
    fs::write(output_path.as_path(), output_bytes.as_slice())
        .with_context(|| format!("failed to write {}", output_path.display()))?;

    let payload = ProfileMutationPayload {
        action: "export",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            profile_name.as_str(),
            profile,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        source_profile: Some(profile_name),
        bundle_path: Some(output_path.display().to_string()),
        validation: None,
        warnings,
    };
    emit_mutation_payload(&payload, json)
}

fn run_profile_import(
    input: String,
    name_override: Option<String>,
    password_stdin: bool,
    set_default: bool,
    force: bool,
    json: bool,
) -> Result<()> {
    let input_path = parse_profile_path(input.trim(), "profile import input")
        .context("invalid import input path")?;
    let bundle = read_profile_bundle(input_path.as_path(), password_stdin)?;
    if bundle.schema_version != PROFILE_EXPORT_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported CLI profile export schema version {}; expected {}",
            bundle.schema_version,
            PROFILE_EXPORT_SCHEMA_VERSION
        );
    }
    let target_name = app::validate_profile_name(
        name_override.as_deref().unwrap_or(bundle.profile.name.as_str()),
    )?;
    let (path, mut document) = app::load_cli_profiles_registry()?;
    if document.profiles.contains_key(target_name.as_str()) && !force {
        anyhow::bail!("CLI profile already exists: {target_name} (pass --force to replace it)");
    }

    let mut warnings = Vec::new();
    let state_root = resolve_profile_state_root(target_name.as_str(), None)?;
    let config_write = if let Some(config) = bundle.config.as_ref() {
        Some(write_profile_config_snapshot(target_name.as_str(), config.content.as_str())?)
    } else {
        warnings.push(
            "profile import is metadata-only because bundle did not include a config snapshot"
                .to_owned(),
        );
        None
    };
    let now = now_unix_ms()?;
    let imported = CliConnectionProfile {
        config_path: config_write.as_ref().map(|value| value.display().to_string()),
        state_root: Some(state_root.display().to_string()),
        daemon_url: bundle.profile.daemon_url.clone(),
        grpc_url: bundle.profile.grpc_url.clone(),
        admin_token: None,
        admin_token_env: bundle.profile.admin_token_env.clone(),
        principal: bundle.profile.principal.clone(),
        device_id: bundle.profile.device_id.clone(),
        channel: bundle.profile.channel.clone(),
        label: bundle.profile.label.clone(),
        environment: bundle.profile.environment.clone(),
        color: bundle.profile.color.clone(),
        risk_level: bundle.profile.risk_level.clone(),
        strict_mode: bundle.profile.strict_mode,
        mode: bundle.profile.mode.clone(),
        created_at_unix_ms: Some(now),
        updated_at_unix_ms: Some(now),
        last_used_at_unix_ms: None,
    };
    document.profiles.insert(target_name.clone(), imported.clone());
    if set_default || document.default_profile.is_none() {
        document.default_profile = Some(target_name.clone());
    }
    app::persist_cli_profiles_registry(path.as_path(), &document)?;

    let validation = build_import_validation_report(
        target_name.as_str(),
        &imported,
        bundle.config.as_ref(),
        bundle.secret_references.as_slice(),
        config_write.is_some(),
    );
    let active_profile =
        if set_default { Some(target_name.clone()) } else { current_profile_name() };
    let mut payload_warnings = create_profile_warnings(&imported);
    payload_warnings.extend(warnings);
    let payload = ProfileMutationPayload {
        action: "import",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            target_name.as_str(),
            &imported,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        source_profile: Some(bundle.source_profile),
        bundle_path: Some(input_path.display().to_string()),
        validation: Some(validation),
        warnings: payload_warnings,
    };
    emit_mutation_payload(&payload, json)
}

fn run_profile_use(name: String, json: bool) -> Result<()> {
    let name = app::validate_profile_name(name.as_str())?;
    let (path, mut document) = app::load_cli_profiles_registry()?;
    let profile = document
        .profiles
        .get_mut(name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {name}"))?;
    let now = now_unix_ms()?;
    profile.last_used_at_unix_ms = Some(now);
    profile.updated_at_unix_ms = Some(now);
    document.default_profile = Some(name.clone());
    let profile_snapshot = profile.clone();
    app::persist_cli_profiles_registry(path.as_path(), &document)?;
    let payload = ProfileMutationPayload {
        action: "use",
        registry_path: path.display().to_string(),
        active_profile: Some(name.clone()),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            name.as_str(),
            &profile_snapshot,
            Some(name.as_str()),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        source_profile: None,
        bundle_path: None,
        validation: None,
        warnings: Vec::new(),
    };
    emit_mutation_payload(&payload, json)
}

fn run_profile_rename(name: String, new_name: String, json: bool) -> Result<()> {
    let name = app::validate_profile_name(name.as_str())?;
    let new_name = app::validate_profile_name(new_name.as_str())?;
    let (path, mut document) = app::load_cli_profiles_registry()?;
    if document.profiles.contains_key(new_name.as_str()) {
        anyhow::bail!("CLI profile already exists: {new_name}");
    }
    let mut profile = document
        .profiles
        .remove(name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {name}"))?;
    if let Some(state_root) =
        profile.state_root.as_deref().and_then(|value| app::normalized_profile_text(Some(value)))
    {
        let expected_old = app::default_profile_state_root(name.as_str())?;
        let actual = PathBuf::from(state_root.as_str());
        if paths_equivalent(actual.as_path(), expected_old.as_path()) {
            profile.state_root =
                Some(app::default_profile_state_root(new_name.as_str())?.display().to_string());
        }
    }
    if let Some(config_path) =
        profile.config_path.as_deref().and_then(|value| app::normalized_profile_text(Some(value)))
    {
        let expected_old = app::default_profile_config_path(name.as_str())?;
        let actual = PathBuf::from(config_path.as_str());
        if paths_equivalent(actual.as_path(), expected_old.as_path()) {
            profile.config_path =
                Some(app::default_profile_config_path(new_name.as_str())?.display().to_string());
        }
    }
    let now = now_unix_ms()?;
    profile.updated_at_unix_ms = Some(now);
    document.profiles.insert(new_name.clone(), profile.clone());
    if document.default_profile.as_deref() == Some(name.as_str()) {
        document.default_profile = Some(new_name.clone());
    }
    app::persist_cli_profiles_registry(path.as_path(), &document)?;
    let active_profile =
        current_profile_name().map(|value| if value == name { new_name.clone() } else { value });
    let payload = ProfileMutationPayload {
        action: "rename",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            new_name.as_str(),
            &profile,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        source_profile: Some(name),
        bundle_path: None,
        validation: None,
        warnings: Vec::new(),
    };
    emit_mutation_payload(&payload, json)
}

fn run_profile_delete(name: String, yes: bool, delete_state_root: bool, json: bool) -> Result<()> {
    let name = app::validate_profile_name(name.as_str())?;
    let active_profile = current_profile_name();
    if active_profile.as_deref() == Some(name.as_str()) && !yes {
        anyhow::bail!("refusing to delete the active profile without --yes: {name}");
    }
    let (path, mut document) = app::load_cli_profiles_registry()?;
    let profile = document
        .profiles
        .remove(name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {name}"))?;
    let mut warnings = Vec::new();
    let mut removed_state_root = None;
    if document.default_profile.as_deref() == Some(name.as_str()) {
        document.default_profile = document.profiles.keys().next().cloned();
    }
    if delete_state_root {
        let Some(state_root_raw) = profile.state_root.as_deref() else {
            warnings.push("profile did not define a state root to delete".to_owned());
            app::persist_cli_profiles_registry(path.as_path(), &document)?;
            let payload = ProfileMutationPayload {
                action: "delete",
                registry_path: path.display().to_string(),
                active_profile,
                default_profile: document.default_profile.clone(),
                profile: None,
                removed_profile: Some(name),
                removed_state_root,
                source_profile: None,
                bundle_path: None,
                validation: None,
                warnings,
            };
            return emit_mutation_payload(&payload, json);
        };
        let state_root = PathBuf::from(state_root_raw);
        if state_root.exists() {
            if !yes {
                anyhow::bail!(
                    "profile delete would remove state root {}; pass --yes to confirm",
                    state_root.display()
                );
            }
            ensure_safe_profile_state_root_removal(state_root.as_path())?;
            fs::remove_dir_all(state_root.as_path()).with_context(|| {
                format!("failed to remove profile state root {}", state_root.display())
            })?;
            removed_state_root = Some(state_root.display().to_string());
        } else {
            warnings.push(format!(
                "profile state root did not exist at delete time: {}",
                state_root.display()
            ));
        }
    } else if profile.state_root.is_some() {
        warnings.push(
            "state root was preserved; pass --delete-state-root --yes to remove it explicitly"
                .to_owned(),
        );
    }
    app::persist_cli_profiles_registry(path.as_path(), &document)?;
    let payload = ProfileMutationPayload {
        action: "delete",
        registry_path: path.display().to_string(),
        active_profile,
        default_profile: document.default_profile.clone(),
        profile: None,
        removed_profile: Some(name),
        removed_state_root,
        source_profile: None,
        bundle_path: None,
        validation: None,
        warnings,
    };
    emit_mutation_payload(&payload, json)
}

fn emit_mutation_payload(payload: &ProfileMutationPayload, json: bool) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(payload, "failed to encode profile output as JSON");
    }
    println!(
        "profile.{} registry_path={} active_profile={} default_profile={}",
        payload.action,
        payload.registry_path,
        payload.active_profile.as_deref().unwrap_or("none"),
        payload.default_profile.as_deref().unwrap_or("none"),
    );
    if let Some(profile) = payload.profile.as_ref() {
        println!(
            "profile.name={} environment={} risk_level={} strict_mode={} state_root={} config_path={}",
            profile.name,
            profile.environment.as_deref().unwrap_or("none"),
            profile.risk_level.as_deref().unwrap_or("none"),
            profile.strict_mode,
            profile.state_root.as_deref().unwrap_or("none"),
            profile.config_path.as_deref().unwrap_or("none"),
        );
    }
    if let Some(source_profile) = payload.source_profile.as_deref() {
        println!("profile.source_profile={source_profile}");
    }
    if let Some(bundle_path) = payload.bundle_path.as_deref() {
        println!("profile.bundle_path={bundle_path}");
    }
    if let Some(validation) = payload.validation.as_ref() {
        println!(
            "profile.validation profile_name={} config_snapshot_written={} isolated_state_root={} isolated_config_path={} blocking_findings={} warning_findings={} info_findings={}",
            validation.profile_name,
            validation.config_snapshot_written,
            validation.isolated_state_root,
            validation.isolated_config_path,
            validation.summary.blocking_findings,
            validation.summary.warning_findings,
            validation.summary.info_findings,
        );
        for finding in validation.findings.as_slice() {
            println!(
                "profile.validation.{}={} {}",
                finding.severity, finding.code, finding.message
            );
        }
    }
    if let Some(removed_profile) = payload.removed_profile.as_deref() {
        println!("profile.removed={removed_profile}");
    }
    if let Some(removed_state_root) = payload.removed_state_root.as_deref() {
        println!("profile.removed_state_root={removed_state_root}");
    }
    for warning in payload.warnings.as_slice() {
        println!("profile.warning={warning}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn resolve_requested_profile_name(
    name: Option<String>,
    active_profile: Option<&str>,
    document: &CliProfilesDocument,
) -> Result<String> {
    if let Some(name) = name {
        return app::validate_profile_name(name.as_str());
    }
    if let Some(active_profile) = active_profile {
        return Ok(active_profile.to_owned());
    }
    if let Some(default_profile) = document.default_profile.as_deref() {
        return Ok(default_profile.to_owned());
    }
    anyhow::bail!("no active or default profile is configured; pass a profile name explicitly")
}

fn current_profile_name() -> Option<String> {
    app::current_root_context().and_then(|context| context.profile_name().map(ToOwned::to_owned))
}

fn profile_record(
    name: &str,
    profile: &CliConnectionProfile,
    active_profile: Option<&str>,
    default_profile: Option<&str>,
) -> ProfileListRecord {
    ProfileListRecord {
        name: name.to_owned(),
        active: active_profile == Some(name),
        is_default: default_profile == Some(name),
        label: profile.label.clone(),
        environment: profile.environment.clone(),
        color: profile.color.clone(),
        risk_level: profile.risk_level.clone(),
        strict_mode: profile.strict_mode,
        mode: profile.mode.clone(),
        config_path: profile.config_path.clone(),
        state_root: profile.state_root.clone(),
        daemon_url: profile.daemon_url.clone(),
        grpc_url: profile.grpc_url.clone(),
        principal: profile.principal.clone(),
        device_id: profile.device_id.clone(),
        channel: profile.channel.clone(),
        admin_token_env: profile.admin_token_env.clone(),
        updated_at_unix_ms: profile.updated_at_unix_ms,
        last_used_at_unix_ms: profile.last_used_at_unix_ms,
    }
}

fn portable_profile_record(name: &str, profile: &CliConnectionProfile) -> PortableProfileRecord {
    PortableProfileRecord {
        name: name.to_owned(),
        label: profile.label.clone(),
        environment: profile.environment.clone(),
        color: profile.color.clone(),
        risk_level: profile.risk_level.clone(),
        strict_mode: profile.strict_mode,
        mode: profile.mode.clone(),
        daemon_url: profile.daemon_url.clone(),
        grpc_url: profile.grpc_url.clone(),
        admin_token_env: profile.admin_token_env.clone(),
        principal: profile.principal.clone(),
        device_id: profile.device_id.clone(),
        channel: profile.channel.clone(),
        source_config_path: profile.config_path.clone(),
    }
}

fn create_profile_warnings(profile: &CliConnectionProfile) -> Vec<String> {
    let mut warnings = Vec::new();
    if profile.config_path.is_none() {
        warnings.push(
            "config_path is unset; run `palyra setup --profile <name>` or `palyra configure --profile <name>` to attach a config file"
                .to_owned(),
        );
    }
    if profile.admin_token_env.is_none() && profile.daemon_url.is_some() {
        warnings.push(
            "remote-oriented profile has no admin token environment override configured".to_owned(),
        );
    }
    warnings
}

fn load_profile_config(config_path: Option<&str>) -> Result<ProfileConfigLoadOutcome> {
    let Some(config_path) = config_path.and_then(|value| app::normalized_profile_text(Some(value)))
    else {
        return Ok(ProfileConfigLoadOutcome::Unconfigured);
    };
    let path = parse_profile_path(config_path.as_str(), "profile config_path")?;
    if !path.exists() {
        return Ok(ProfileConfigLoadOutcome::MissingPath { path: path.display().to_string() });
    }
    let content = fs::read_to_string(path.as_path())
        .with_context(|| format!("failed to read {}", path.display()))?;
    let parsed = match parse_document_with_migration(content.as_str()) {
        Ok((document, _)) => match validate_daemon_compatible_document(&document) {
            Ok(()) => document,
            Err(error) => {
                return Ok(ProfileConfigLoadOutcome::Invalid {
                    path: path.display().to_string(),
                    error: error.to_string(),
                });
            }
        },
        Err(error) => {
            return Ok(ProfileConfigLoadOutcome::Invalid {
                path: path.display().to_string(),
                error: error.to_string(),
            });
        }
    };
    let rendered_content =
        toml::to_string_pretty(&parsed).context("failed to serialize profile config snapshot")?;
    let mut redacted = parsed.clone();
    redact_secret_config_values(&mut redacted);
    let redacted_content = toml::to_string_pretty(&redacted)
        .context("failed to serialize redacted profile config snapshot")?;
    Ok(ProfileConfigLoadOutcome::Loaded(LoadedProfileConfig {
        source_path: path,
        rendered_content,
        redacted_content,
        secret_references: collect_secret_references(&parsed),
    }))
}

fn write_profile_config_snapshot(profile_name: &str, content: &str) -> Result<PathBuf> {
    let path = app::default_profile_config_path(profile_name)?;
    let (document, _) = parse_document_with_migration(content)
        .context("failed to parse cloned/imported profile config snapshot")?;
    validate_daemon_compatible_document(&document)
        .context("profile config snapshot does not satisfy daemon schema")?;
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if path.exists() {
        write_document_with_backups(path.as_path(), &document, PROFILE_CONFIG_WRITE_BACKUPS)
            .with_context(|| format!("failed to persist {}", path.display()))?;
    } else {
        fs::write(path.as_path(), content.as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(path)
}

fn build_clone_validation_report(
    profile_name: &str,
    profile: &CliConnectionProfile,
    config_outcome: &ProfileConfigLoadOutcome,
    config_written: bool,
) -> ProfileValidationReport {
    let mut findings = Vec::new();
    match config_outcome {
        ProfileConfigLoadOutcome::Loaded(config) => {
            findings.push(ProfileValidationFinding {
                severity: "info".to_owned(),
                code: "config_snapshot_cloned".to_owned(),
                message: format!(
                    "cloned config snapshot from {} into isolated profile config namespace",
                    config.source_path.display()
                ),
            });
            append_secret_reference_findings(config.secret_references.as_slice(), &mut findings);
        }
        ProfileConfigLoadOutcome::Unconfigured => findings.push(ProfileValidationFinding {
            severity: "warning".to_owned(),
            code: "missing_source_config_path".to_owned(),
            message: "source profile did not define config_path, so clone is metadata-only"
                .to_owned(),
        }),
        ProfileConfigLoadOutcome::MissingPath { path } => findings.push(ProfileValidationFinding {
            severity: "warning".to_owned(),
            code: "missing_source_config_file".to_owned(),
            message: format!("source profile config_path does not exist: {path}"),
        }),
        ProfileConfigLoadOutcome::Invalid { path, error } => {
            findings.push(ProfileValidationFinding {
                severity: "blocking".to_owned(),
                code: "invalid_source_config".to_owned(),
                message: format!("source profile config_path failed validation: {path} ({error})"),
            })
        }
    }
    append_admin_token_env_finding(profile, &mut findings);
    build_validation_report(profile_name, profile, config_written, findings)
}

fn build_import_validation_report(
    profile_name: &str,
    profile: &CliConnectionProfile,
    config: Option<&PortableProfileConfig>,
    secret_references: &[ProfileSecretReference],
    config_written: bool,
) -> ProfileValidationReport {
    let mut findings = Vec::new();
    if let Some(config) = config {
        findings.push(ProfileValidationFinding {
            severity: "info".to_owned(),
            code: "config_snapshot_imported".to_owned(),
            message: format!(
                "imported config snapshot from bundle source {} into isolated profile config namespace",
                config.source_path
            ),
        });
        if config.redacted || config.content.contains(PROFILE_REDACTED_VALUE) {
            findings.push(ProfileValidationFinding {
                severity: "warning".to_owned(),
                code: "redacted_config_snapshot".to_owned(),
                message: "imported config snapshot contains redacted secret placeholders; repair secrets before using the profile in production".to_owned(),
            });
        }
    } else {
        findings.push(ProfileValidationFinding {
            severity: "warning".to_owned(),
            code: "missing_config_snapshot".to_owned(),
            message: "bundle did not include a config snapshot, so import is metadata-only"
                .to_owned(),
        });
    }
    append_secret_reference_findings(secret_references, &mut findings);
    append_admin_token_env_finding(profile, &mut findings);
    build_validation_report(profile_name, profile, config_written, findings)
}

fn append_admin_token_env_finding(
    profile: &CliConnectionProfile,
    findings: &mut Vec<ProfileValidationFinding>,
) {
    let Some(admin_token_env) = profile
        .admin_token_env
        .as_deref()
        .and_then(|value| app::normalized_profile_text(Some(value)))
    else {
        return;
    };
    if std::env::var(&admin_token_env)
        .ok()
        .and_then(|value| {
            let trimmed = value.trim().to_owned();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .is_none()
    {
        findings.push(ProfileValidationFinding {
            severity: "warning".to_owned(),
            code: "missing_admin_token_env".to_owned(),
            message: format!(
                "admin token environment override {admin_token_env} is not set in the current shell"
            ),
        });
    }
}

fn append_secret_reference_findings(
    secret_references: &[ProfileSecretReference],
    findings: &mut Vec<ProfileValidationFinding>,
) {
    if secret_references.is_empty() {
        return;
    }
    let vault = match crate::open_cli_vault() {
        Ok(vault) => Some(vault),
        Err(error) => {
            findings.push(ProfileValidationFinding {
                severity: "warning".to_owned(),
                code: "vault_audit_unavailable".to_owned(),
                message: format!(
                    "local vault audit was unavailable while validating imported references: {error}"
                ),
            });
            None
        }
    };
    for reference in secret_references {
        let parsed = match VaultRef::parse(reference.reference.as_str()) {
            Ok(parsed) => parsed,
            Err(error) => {
                findings.push(ProfileValidationFinding {
                    severity: "blocking".to_owned(),
                    code: "invalid_secret_reference".to_owned(),
                    message: format!(
                        "secret reference at {} is invalid: {} ({error})",
                        reference.component_path, reference.reference
                    ),
                });
                continue;
            }
        };
        let Some(vault) = vault.as_ref() else {
            continue;
        };
        match vault.get_secret(&parsed.scope, parsed.key.as_str()) {
            Ok(value) if !value.is_empty() => {}
            Ok(_) => findings.push(ProfileValidationFinding {
                severity: "blocking".to_owned(),
                code: "empty_secret_reference".to_owned(),
                message: format!(
                    "secret reference {} resolves to an empty value",
                    reference.reference
                ),
            }),
            Err(error) => findings.push(ProfileValidationFinding {
                severity: "blocking".to_owned(),
                code: "missing_secret_reference".to_owned(),
                message: format!(
                    "secret reference {} is missing or unreadable: {error}",
                    reference.reference
                ),
            }),
        }
    }
}

fn build_validation_report(
    profile_name: &str,
    profile: &CliConnectionProfile,
    config_written: bool,
    findings: Vec<ProfileValidationFinding>,
) -> ProfileValidationReport {
    let summary = ProfileValidationSummary {
        blocking_findings: findings.iter().filter(|finding| finding.severity == "blocking").count(),
        warning_findings: findings.iter().filter(|finding| finding.severity == "warning").count(),
        info_findings: findings.iter().filter(|finding| finding.severity == "info").count(),
    };
    ProfileValidationReport {
        profile_name: profile_name.to_owned(),
        config_path: profile.config_path.clone(),
        state_root: profile.state_root.clone(),
        config_snapshot_written: config_written,
        isolated_state_root: profile
            .state_root
            .as_deref()
            .map(|value| {
                paths_equivalent(
                    Path::new(value),
                    app::default_profile_state_root(profile_name)
                        .unwrap_or_else(|_| PathBuf::from(value))
                        .as_path(),
                )
            })
            .unwrap_or(false),
        isolated_config_path: profile
            .config_path
            .as_deref()
            .map(|value| {
                paths_equivalent(
                    Path::new(value),
                    app::default_profile_config_path(profile_name)
                        .unwrap_or_else(|_| PathBuf::from(value))
                        .as_path(),
                )
            })
            .unwrap_or(false),
        findings,
        summary,
    }
}

fn collect_secret_references(document: &toml::Value) -> Vec<ProfileSecretReference> {
    let mut references = Vec::new();
    collect_secret_references_inner(document, "", &mut references);
    references
}

fn collect_secret_references_inner(
    value: &toml::Value,
    current_path: &str,
    references: &mut Vec<ProfileSecretReference>,
) {
    match value {
        toml::Value::Table(table) => {
            for (key, child) in table {
                let child_path = if current_path.is_empty() {
                    key.clone()
                } else {
                    format!("{current_path}.{key}")
                };
                if key.ends_with("_vault_ref") {
                    if let Some(reference) =
                        child.as_str().map(str::trim).filter(|reference| !reference.is_empty())
                    {
                        match VaultRef::parse(reference) {
                            Ok(parsed) => references.push(ProfileSecretReference {
                                component_path: child_path.clone(),
                                reference: reference.to_owned(),
                                scope: parsed.scope.to_string(),
                                key: parsed.key,
                            }),
                            Err(_) => references.push(ProfileSecretReference {
                                component_path: child_path.clone(),
                                reference: reference.to_owned(),
                                scope: "invalid".to_owned(),
                                key: "invalid".to_owned(),
                            }),
                        }
                    }
                }
                collect_secret_references_inner(child, child_path.as_str(), references);
            }
        }
        toml::Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                let child_path = if current_path.is_empty() {
                    format!("[{index}]")
                } else {
                    format!("{current_path}[{index}]")
                };
                collect_secret_references_inner(child, child_path.as_str(), references);
            }
        }
        _ => {}
    }
}

fn read_profile_bundle(path: &Path, password_stdin: bool) -> Result<ProfilePortabilityBundle> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read profile bundle {}", path.display()))?;
    if let Ok(encrypted) = serde_json::from_slice::<EncryptedProfileBundle>(bytes.as_slice()) {
        if encrypted.kind == PROFILE_EXPORT_ENCRYPTED_KIND {
            let password = read_password_from_stdin(password_stdin)?;
            let decrypted = decrypt_profile_bundle(&encrypted, password.as_slice())?;
            return serde_json::from_slice::<ProfilePortabilityBundle>(decrypted.as_slice())
                .context("failed to parse decrypted profile bundle");
        }
    }
    if password_stdin {
        anyhow::bail!("--password-stdin was provided, but the import bundle is not encrypted");
    }
    serde_json::from_slice::<ProfilePortabilityBundle>(bytes.as_slice())
        .context("failed to parse profile bundle")
}

fn encrypt_profile_bundle(plaintext: &[u8], password: &[u8]) -> Result<EncryptedProfileBundle> {
    let iterations =
        NonZeroU32::new(PROFILE_EXPORT_PBKDF2_ITERATIONS).expect("iterations are non-zero");
    let mut salt = [0_u8; PROFILE_EXPORT_SALT_LEN];
    let mut nonce_bytes = [0_u8; PROFILE_EXPORT_NONCE_LEN];
    let rng = SystemRandom::new();
    rng.fill(&mut salt).map_err(|_| anyhow!("failed to generate profile export salt"))?;
    rng.fill(&mut nonce_bytes).map_err(|_| anyhow!("failed to generate profile export nonce"))?;

    let mut key = [0_u8; 32];
    pbkdf2::derive(pbkdf2::PBKDF2_HMAC_SHA256, iterations, salt.as_slice(), password, &mut key);
    let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key)
        .map_err(|_| anyhow!("failed to initialize profile export cipher"))?;
    let key = aead::LessSafeKey::new(unbound);
    let nonce = aead::Nonce::assume_unique_for_key(nonce_bytes);
    let mut in_out = plaintext.to_vec();
    key.seal_in_place_append_tag(nonce, aead::Aad::from(PROFILE_AEAD_AAD), &mut in_out)
        .map_err(|_| anyhow!("failed to encrypt profile export bundle"))?;

    Ok(EncryptedProfileBundle {
        schema_version: PROFILE_EXPORT_SCHEMA_VERSION,
        kind: PROFILE_EXPORT_ENCRYPTED_KIND.to_owned(),
        cipher: PROFILE_EXPORT_CIPHER.to_owned(),
        kdf: PROFILE_EXPORT_KDF.to_owned(),
        iterations: PROFILE_EXPORT_PBKDF2_ITERATIONS,
        salt_b64: BASE64_STANDARD.encode(salt),
        nonce_b64: BASE64_STANDARD.encode(nonce_bytes),
        ciphertext_b64: BASE64_STANDARD.encode(in_out),
    })
}

fn decrypt_profile_bundle(envelope: &EncryptedProfileBundle, password: &[u8]) -> Result<Vec<u8>> {
    if envelope.schema_version != PROFILE_EXPORT_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported encrypted profile bundle schema version {}; expected {}",
            envelope.schema_version,
            PROFILE_EXPORT_SCHEMA_VERSION
        );
    }
    if envelope.kind != PROFILE_EXPORT_ENCRYPTED_KIND
        || envelope.cipher != PROFILE_EXPORT_CIPHER
        || envelope.kdf != PROFILE_EXPORT_KDF
    {
        anyhow::bail!("unsupported encrypted profile bundle parameters");
    }
    let iterations = NonZeroU32::new(envelope.iterations)
        .ok_or_else(|| anyhow!("encrypted profile bundle declared zero PBKDF2 iterations"))?;
    let salt = BASE64_STANDARD
        .decode(envelope.salt_b64.as_bytes())
        .context("failed to decode encrypted profile bundle salt")?;
    let nonce_bytes = BASE64_STANDARD
        .decode(envelope.nonce_b64.as_bytes())
        .context("failed to decode encrypted profile bundle nonce")?;
    let mut ciphertext = BASE64_STANDARD
        .decode(envelope.ciphertext_b64.as_bytes())
        .context("failed to decode encrypted profile bundle ciphertext")?;
    let nonce_bytes: [u8; PROFILE_EXPORT_NONCE_LEN] = nonce_bytes
        .try_into()
        .map_err(|_| anyhow!("encrypted profile bundle nonce has invalid length"))?;

    let mut key = [0_u8; 32];
    pbkdf2::derive(pbkdf2::PBKDF2_HMAC_SHA256, iterations, salt.as_slice(), password, &mut key);
    let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key)
        .map_err(|_| anyhow!("failed to initialize encrypted profile bundle cipher"))?;
    let key = aead::LessSafeKey::new(unbound);
    let nonce = aead::Nonce::assume_unique_for_key(nonce_bytes);
    let plaintext = key
        .open_in_place(nonce, aead::Aad::from(PROFILE_AEAD_AAD), &mut ciphertext)
        .map_err(|_| {
            anyhow!("failed to decrypt profile bundle; password is incorrect or data is corrupted")
        })?;
    Ok(plaintext.to_vec())
}

fn read_password_from_stdin(password_stdin: bool) -> Result<Vec<u8>> {
    if !password_stdin {
        anyhow::bail!(
            "encrypted profile import/export requires --password-stdin to avoid exposing passphrases in process args"
        );
    }
    let mut password = Vec::new();
    std::io::stdin()
        .read_to_end(&mut password)
        .context("failed to read profile bundle passphrase from stdin")?;
    while password.ends_with(b"\n") || password.ends_with(b"\r") {
        password.pop();
    }
    if password.is_empty() {
        anyhow::bail!("stdin did not contain any passphrase bytes");
    }
    Ok(password)
}

fn resolve_profile_state_root(profile_name: &str, explicit: Option<&str>) -> Result<PathBuf> {
    if let Some(explicit) = explicit {
        let normalized = explicit.trim();
        if normalized.is_empty() {
            anyhow::bail!("profile state_root cannot be empty when provided");
        }
        return parse_profile_path(normalized, "profile state_root");
    }
    let state_root = app::default_profile_state_root(profile_name)?;
    if let Some(parent) = state_root.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create profile state root parent {}", parent.display())
        })?;
    }
    Ok(state_root)
}

fn normalize_optional_path(raw: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let normalized = raw.trim();
    if normalized.is_empty() {
        return Ok(None);
    }
    parse_profile_path(normalized, "profile config_path")
        .map(|path| Some(path.display().to_string()))
}

fn parse_profile_path(raw: &str, label: &str) -> Result<PathBuf> {
    palyra_common::parse_config_path(raw).with_context(|| format!("{label} is invalid: {raw}"))
}

fn ensure_safe_profile_state_root_removal(path: &Path) -> Result<()> {
    crate::support::lifecycle::ensure_safe_removal_target(path, "profile state root")?;
    let canonical = crate::support::lifecycle::canonicalize_lossy(path)?;
    let cli_state_root = app::current_root_context()
        .map(|context| context.cli_state_root().to_path_buf())
        .unwrap_or(app::resolve_cli_state_root(None)?);
    let cli_state_root = crate::support::lifecycle::canonicalize_lossy(cli_state_root.as_path())?;
    if !crate::support::lifecycle::path_starts_with(canonical.as_path(), cli_state_root.as_path())
    {
        anyhow::bail!(
            "refusing to remove state root outside the CLI state root namespace: {}",
            canonical.display()
        );
    }
    Ok(())
}

fn profile_export_mode_label(mode: ProfileExportModeArg) -> &'static str {
    match mode {
        ProfileExportModeArg::Redacted => "redacted",
        ProfileExportModeArg::Encrypted => "encrypted",
    }
}

fn profile_mode_label(mode: ProfileModeArg) -> &'static str {
    match mode {
        ProfileModeArg::Local => "local",
        ProfileModeArg::Remote => "remote",
        ProfileModeArg::Custom => "custom",
    }
}

fn default_environment(mode: ProfileModeArg) -> &'static str {
    match mode {
        ProfileModeArg::Local => "local",
        ProfileModeArg::Remote => "remote",
        ProfileModeArg::Custom => "custom",
    }
}

fn default_risk_level(mode: ProfileModeArg) -> &'static str {
    match mode {
        ProfileModeArg::Local => "low",
        ProfileModeArg::Remote => "high",
        ProfileModeArg::Custom => "elevated",
    }
}

fn profile_risk_level_label(level: ProfileRiskLevelArg) -> String {
    match level {
        ProfileRiskLevelArg::Low => "low",
        ProfileRiskLevelArg::Elevated => "elevated",
        ProfileRiskLevelArg::High => "high",
        ProfileRiskLevelArg::Critical => "critical",
    }
    .to_owned()
}

fn now_unix_ms() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is set before UNIX_EPOCH")?;
    i64::try_from(duration.as_millis()).context("system clock exceeds supported timestamp range")
}

fn paths_equivalent(left: &Path, right: &Path) -> bool {
    let left =
        crate::support::lifecycle::canonicalize_lossy(left).unwrap_or_else(|_| left.to_path_buf());
    let right = crate::support::lifecycle::canonicalize_lossy(right)
        .unwrap_or_else(|_| right.to_path_buf());
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .replace('/', "\\")
            .eq_ignore_ascii_case(right.to_string_lossy().replace('/', "\\").as_str())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

#[cfg(test)]
mod tests {
    use super::{
        collect_secret_references, decrypt_profile_bundle, default_environment, default_risk_level,
        encrypt_profile_bundle, ensure_safe_profile_state_root_removal, profile_mode_label,
        PortableProfileConfig,
        ProfilePortabilityBundle, ProfileSecretReference,
    };
    use crate::{
        app,
        args::{ProfileModeArg, RootOptions},
        sha256_hex,
    };
    use anyhow::Result;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn profile_mode_defaults_match_environment_story() {
        assert_eq!(profile_mode_label(ProfileModeArg::Local), "local");
        assert_eq!(default_environment(ProfileModeArg::Remote), "remote");
        assert_eq!(default_risk_level(ProfileModeArg::Custom), "elevated");
    }

    #[test]
    fn collect_secret_references_finds_vault_ref_paths() -> Result<()> {
        let document: toml::Value = toml::from_str(
            r#"
[model_provider]
openai_api_key_vault_ref = "global/openai_api_key"

[tool_call.browser_service]
state_key_vault_ref = "global/browser_state_key"
"#,
        )?;
        let references = collect_secret_references(&document);
        assert_eq!(
            references
                .iter()
                .map(|reference| reference.component_path.as_str())
                .collect::<Vec<_>>(),
            vec![
                "model_provider.openai_api_key_vault_ref",
                "tool_call.browser_service.state_key_vault_ref",
            ]
        );
        Ok(())
    }

    #[test]
    fn encrypted_profile_bundle_round_trips() -> Result<()> {
        let bundle = ProfilePortabilityBundle {
            schema_version: 1,
            generated_at_unix_ms: 1,
            source_profile: "prod".to_owned(),
            export_mode: "encrypted".to_owned(),
            profile: super::PortableProfileRecord {
                name: "prod".to_owned(),
                label: Some("Prod".to_owned()),
                environment: Some("prod".to_owned()),
                color: None,
                risk_level: Some("high".to_owned()),
                strict_mode: true,
                mode: Some("remote".to_owned()),
                daemon_url: Some("https://gateway.example.com".to_owned()),
                grpc_url: None,
                admin_token_env: Some("PALYRA_PROD_ADMIN_TOKEN".to_owned()),
                principal: None,
                device_id: None,
                channel: None,
                source_config_path: Some("prod/palyra.toml".to_owned()),
            },
            config: Some(PortableProfileConfig {
                source_path: "prod/palyra.toml".to_owned(),
                redacted: false,
                sha256: sha256_hex(b"[daemon]\nport = 7142\n"),
                content: "[daemon]\nport = 7142\n".to_owned(),
            }),
            secret_references: vec![ProfileSecretReference {
                component_path: "model_provider.openai_api_key_vault_ref".to_owned(),
                reference: "global/openai_api_key".to_owned(),
                scope: "global".to_owned(),
                key: "openai_api_key".to_owned(),
            }],
        };
        let plaintext = serde_json::to_vec(&bundle)?;
        let password = sha256_hex(plaintext.as_slice());
        let encrypted = encrypt_profile_bundle(plaintext.as_slice(), password.as_bytes())?;
        let decrypted = decrypt_profile_bundle(&encrypted, password.as_bytes())?;
        let round_trip: ProfilePortabilityBundle = serde_json::from_slice(decrypted.as_slice())?;
        assert_eq!(round_trip.source_profile, "prod");
        assert_eq!(round_trip.secret_references.len(), 1);
        assert_eq!(
            round_trip.config.as_ref().map(|config| config.source_path.as_str()),
            Some("prod/palyra.toml")
        );
        Ok(())
    }

    #[test]
    fn delete_state_root_allows_paths_under_explicit_cli_state_root() -> Result<()> {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();

        let temp = tempdir()?;
        let cli_state_root = temp.path().join("state");
        let profile_state_root = cli_state_root.join("profiles").join("demo").join("state");
        fs::create_dir_all(&profile_state_root)?;
        let config_path = temp.path().join("config").join("palyra.toml");
        fs::create_dir_all(config_path.parent().expect("config parent"))?;
        fs::write(&config_path, "[daemon]\nport = 7142\n")?;

        let _context = app::install_root_context(RootOptions {
            config_path: Some(config_path.display().to_string()),
            state_root: Some(cli_state_root.display().to_string()),
            ..RootOptions::default()
        })?;

        ensure_safe_profile_state_root_removal(profile_state_root.as_path())?;

        app::clear_root_context_for_tests();
        Ok(())
    }
}
