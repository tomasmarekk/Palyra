//! `palyra config`: status, path resolution, validation, and key-level
//! get/set/unset/migrate/recover operations on the daemon TOML config.
//! Every mutation revalidates the document against the daemon schema before
//! persisting; secret values are redacted in output unless `--show-secrets`.

use crate::*;
use std::collections::BTreeSet;

/// Runs a `palyra config` subcommand; a missing subcommand defaults to status.
///
/// # Errors
/// Returns an error when the config cannot be resolved or parsed, a key path
/// is invalid or missing, or a mutated document fails daemon-schema validation.
pub(crate) fn run_config(command: Option<ConfigCommand>) -> Result<()> {
    let command = command
        .unwrap_or(ConfigCommand::Status { path: None, json: output::preferred_json(false) });
    match command {
        ConfigCommand::Status { path, json } => {
            let payload = build_config_status_payload(path)?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode config status as JSON")?;
            } else {
                println!(
                    "config.status path={} exists={} parsed={} migrated={} source_version={} target_version={} provider_kind={} auth_profile_id={}",
                    payload.path.as_deref().unwrap_or("none"),
                    payload.exists,
                    payload.parsed,
                    payload.migrated,
                    payload
                        .source_version
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "none".to_owned()),
                    payload
                        .target_version
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "none".to_owned()),
                    payload.provider_kind.as_deref().unwrap_or("none"),
                    payload.auth_profile_id.as_deref().unwrap_or("none")
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Path { path, json } => {
            let resolved = match path {
                Some(explicit) => resolve_config_path(Some(explicit), false)?,
                None => effective_config_path()
                    .context("no default config file found; pass --path to select a config file")?,
            };
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &json!({ "path": resolved }),
                    "failed to encode config path as JSON",
                )?;
            } else {
                println!("config.path path={resolved}");
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Validate { path, json } => {
            let json = output::preferred_json(json);
            let path = match path {
                Some(explicit) => resolve_config_path(Some(explicit), true)?,
                None => {
                    if let Some(found) = effective_config_path() {
                        found
                    } else {
                        if json {
                            return output::print_json_pretty(
                                &json!({
                                    "status": "valid",
                                    "source": "defaults",
                                }),
                                "failed to encode config validation as JSON",
                            );
                        }
                        println!("config=valid source=defaults");
                        return std::io::stdout().flush().context("stdout flush failed");
                    }
                }
            };

            let (document, migration) = load_document_from_existing_path(Path::new(&path))
                .with_context(|| format!("failed to parse {path}"))?;
            validate_daemon_compatible_document(&document)
                .with_context(|| format!("failed to parse {path}"))?;
            let warnings = build_config_validation_warnings(path.as_str());
            if json {
                return output::print_json_pretty(
                    &json!({
                        "status": "valid",
                        "source": path,
                        "version": migration.target_version,
                        "migrated": migration.migrated,
                        "warnings": warnings,
                    }),
                    "failed to encode config validation as JSON",
                );
            }
            println!(
                "config=valid source={path} version={} migrated={}",
                migration.target_version, migration.migrated
            );
            for warning in warnings {
                println!(
                    "config.warning severity={} code={} component={} message=\"{}\" remediation=\"{}\"",
                    warning.severity,
                    warning.code,
                    warning.component,
                    escape_config_warning_text(warning.message.as_str()),
                    escape_config_warning_text(warning.remediation.as_str())
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Explain { path, key, json } => {
            let payload = build_config_explain_report(path, key.as_deref())?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode config explanation as JSON")
            } else {
                render_config_explain_report(&payload)
            }
        }
        ConfigCommand::Doctor { path, json } => {
            let payload = build_config_doctor_report_from_environment(path)?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode config doctor as JSON")
            } else {
                render_config_doctor_report(&payload)
            }
        }
        ConfigCommand::List { path, show_secrets, json } => {
            let json = output::preferred_json(json);
            let path = resolve_config_path(path, true)?;
            let (mut document, _) = load_document_from_existing_path(Path::new(&path))
                .with_context(|| format!("failed to parse {path}"))?;
            if !show_secrets {
                redact_secret_config_values(&mut document);
            }
            if json {
                return output::print_json_pretty(
                    &json!({
                        "source": path,
                        "show_secrets": show_secrets,
                        "document": document,
                    }),
                    "failed to encode config list as JSON",
                );
            }
            let rendered =
                toml::to_string_pretty(&document).context("failed to serialize config document")?;
            println!("config.list source={} show_secrets={show_secrets}", path);
            print!("{rendered}");
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Get { path, key, show_secrets, json } => {
            let json = output::preferred_json(json);
            let path = resolve_config_path(path, true)?;
            let (document, _) = load_document_from_existing_path(Path::new(&path))
                .with_context(|| format!("failed to parse {path}"))?;
            let value = get_value_at_path(&document, key.as_str())
                .with_context(|| format!("invalid config key path: {}", key))?
                .with_context(|| format!("config key not found: {}", key))?;
            let (output_value, redacted) =
                build_config_get_output_value(&document, key.as_str(), value, show_secrets)?;
            if json {
                return output::print_json_pretty(
                    &json!({
                        "key": key,
                        "value": output_value,
                        "source": path,
                        "show_secrets": show_secrets,
                        "redacted": redacted,
                    }),
                    "failed to encode config get as JSON",
                );
            }
            let display_value = format_toml_value(&output_value);
            println!(
                "config.get key={} value={} source={} show_secrets={show_secrets}",
                key, display_value, path
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        ConfigCommand::Set { path, key, value, backups, json } => {
            let path = resolve_config_path(path, false)?;
            let path_ref = Path::new(&path);
            let (mut document, migration) = load_document_for_mutation(path_ref)
                .with_context(|| format!("failed to parse {}", path_ref.display()))?;
            let literal = parse_config_set_value_literal(value.as_str())?;
            validate_config_set_value(key.as_str(), &literal)?;
            set_value_at_path(&mut document, key.as_str(), literal)
                .with_context(|| format!("invalid config key path: {}", key))?;
            validate_daemon_compatible_document(&document).with_context(|| {
                format!("mutated config {} does not match daemon schema", path_ref.display())
            })?;
            write_document_with_backups(path_ref, &document, backups)
                .with_context(|| format!("failed to persist config {}", path_ref.display()))?;
            let source = path_ref.display().to_string();
            let payload = json!({
                "key": key.as_str(),
                "source": source.as_str(),
                "backups": backups,
                "migrated": migration.migrated,
            });
            let json = output::preferred_json(json);
            if json {
                output::print_json_pretty(&payload, "failed to encode config set as JSON")
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(&payload, "failed to encode config set as NDJSON")
            } else {
                println!(
                    "config.set key={} source={} backups={} migrated={}",
                    key, source, backups, migration.migrated
                );
                std::io::stdout().flush().context("stdout flush failed")
            }
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
            // The backup is parsed and schema-validated before any swap so a
            // recover can never replace the active config with a broken one.
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

fn build_config_get_output_value(
    document: &toml::Value,
    key: &str,
    value: &toml::Value,
    show_secrets: bool,
) -> Result<(toml::Value, bool)> {
    if show_secrets {
        return Ok((value.clone(), false));
    }

    // Redaction rules are defined over whole documents, so redact a copy and
    // re-resolve the key in it rather than re-implementing per-key secret
    // classification here.
    let mut redacted_document = document.clone();
    redact_secret_config_values(&mut redacted_document);
    let redacted_value = get_value_at_path(&redacted_document, key)
        .with_context(|| format!("invalid config key path: {key}"))?
        .with_context(|| format!("config key not found after redaction: {key}"))?
        .clone();
    let redacted = &redacted_value != value;
    Ok((redacted_value, redacted))
}

#[derive(Debug, Serialize)]
struct ConfigExplainReport {
    schema_version: u32,
    source: ConfigExplainSource,
    entries: Vec<ConfigExplainEntry>,
}

#[derive(Debug, Serialize)]
struct ConfigExplainSource {
    path: Option<String>,
    exists: bool,
    parsed: bool,
}

#[derive(Debug, Serialize)]
struct ConfigExplainEntry {
    key: String,
    value_type: String,
    default_value: Option<String>,
    env_vars: Vec<String>,
    secret: bool,
    deprecated: bool,
    restart_required: bool,
    category: String,
    description: String,
    effective_source: String,
    effective_value: Option<serde_json::Value>,
    redacted: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ConfigDoctorReport {
    schema_version: u32,
    status: String,
    source: ConfigExplainSource,
    pub(crate) findings: Vec<ConfigDoctorFinding>,
    unknown_env_vars: Vec<String>,
    deprecated_flags: Vec<String>,
    unsafe_dev_flags: Vec<String>,
    missing_secrets: Vec<String>,
    remote_worker_policy_issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ConfigDoctorFinding {
    pub(crate) severity: String,
    pub(crate) code: String,
    pub(crate) key: String,
    pub(crate) message: String,
    pub(crate) remediation: String,
}

fn build_config_explain_report(
    path: Option<String>,
    key: Option<&str>,
) -> Result<ConfigExplainReport> {
    let source = load_config_explain_source(path)?;
    let entries = match key {
        Some(key) => {
            let entry = palyra_common::daemon_config_schema::config_schema_entry(key)
                .with_context(|| format!("unknown config schema key: {key}"))?;
            vec![build_config_explain_entry(entry, &source)?]
        }
        None => palyra_common::daemon_config_schema::config_schema_entries()
            .iter()
            .map(|entry| build_config_explain_entry(entry, &source))
            .collect::<Result<Vec<_>>>()?,
    };
    Ok(ConfigExplainReport { schema_version: 1, source: source.snapshot, entries })
}

pub(crate) fn build_config_doctor_report_from_environment(
    path: Option<String>,
) -> Result<ConfigDoctorReport> {
    let env_vars = env::vars().collect::<Vec<_>>();
    build_config_doctor_report(path, env_vars.as_slice())
}

pub(crate) fn build_config_doctor_report(
    path: Option<String>,
    env_vars: &[(String, String)],
) -> Result<ConfigDoctorReport> {
    let source = load_config_explain_source(path)?;
    let mut findings = Vec::new();
    let unknown_env_vars = unknown_palyra_env_vars(env_vars);
    for name in &unknown_env_vars {
        findings.push(config_doctor_finding(
            "warning",
            "unknown_palyra_env_var",
            name,
            format!("{name} is set but is not in the typed config/env catalog"),
            "Remove stale environment overrides or add the variable to the schema catalog.",
        ));
    }

    let deprecated_flags = deprecated_config_flags(&source, env_vars);
    for flag in &deprecated_flags {
        findings.push(config_doctor_finding(
            "warning",
            "deprecated_config_flag",
            flag,
            format!("{flag} uses a deprecated compatibility surface"),
            "Move to the current config surface before depending on this setting.",
        ));
    }

    let unsafe_dev_flags = unsafe_config_flags(&source, env_vars);
    for flag in &unsafe_dev_flags {
        findings.push(config_doctor_finding(
            "critical",
            "unsafe_dev_flag",
            flag,
            format!("{flag} weakens production isolation"),
            "Disable the escape hatch or keep deployment.mode local-only while testing.",
        ));
    }

    let missing_secrets = missing_config_secrets(&source, env_vars);
    for secret in &missing_secrets {
        findings.push(config_doctor_finding(
            "critical",
            "missing_required_secret",
            secret,
            format!("{secret} is required by the current config posture but is not configured"),
            "Configure a vault-backed secret, structured secret ref, auth profile, or explicit env override.",
        ));
    }

    let remote_worker_policy_issues = remote_worker_policy_issues(&source, env_vars);
    for issue in &remote_worker_policy_issues {
        findings.push(config_doctor_finding(
            "critical",
            "remote_worker_policy_gap",
            issue,
            format!("{issue} is required when networked workers are enabled"),
            "Require worker attestation and pin at least one expected worker digest before enabling remote workers.",
        ));
    }

    let status = if findings.iter().any(|finding| finding.severity == "critical") {
        "fail"
    } else if findings.is_empty() {
        "pass"
    } else {
        "warn"
    };

    Ok(ConfigDoctorReport {
        schema_version: 1,
        status: status.to_owned(),
        source: source.snapshot,
        findings,
        unknown_env_vars,
        deprecated_flags,
        unsafe_dev_flags,
        missing_secrets,
        remote_worker_policy_issues,
    })
}

#[derive(Debug)]
struct LoadedConfigExplainSource {
    snapshot: ConfigExplainSource,
    document: Option<toml::Value>,
}

fn load_config_explain_source(path: Option<String>) -> Result<LoadedConfigExplainSource> {
    let resolved = match path {
        Some(explicit) => Some(resolve_config_path(Some(explicit), false)?),
        None => effective_config_path(),
    };
    let Some(path_value) = resolved else {
        return Ok(LoadedConfigExplainSource {
            snapshot: ConfigExplainSource { path: None, exists: false, parsed: false },
            document: None,
        });
    };
    let path_ref = Path::new(&path_value);
    if !path_ref.exists() {
        return Ok(LoadedConfigExplainSource {
            snapshot: ConfigExplainSource { path: Some(path_value), exists: false, parsed: false },
            document: None,
        });
    }
    let (document, _) = load_document_from_existing_path(path_ref)
        .with_context(|| format!("failed to parse {path_value}"))?;
    Ok(LoadedConfigExplainSource {
        snapshot: ConfigExplainSource { path: Some(path_value), exists: true, parsed: true },
        document: Some(document),
    })
}

fn build_config_explain_entry(
    entry: &palyra_common::daemon_config_schema::ConfigSchemaEntry,
    source: &LoadedConfigExplainSource,
) -> Result<ConfigExplainEntry> {
    let (effective_source, effective_value, redacted) =
        resolve_config_explain_value(entry, source)?;
    Ok(ConfigExplainEntry {
        key: entry.path.to_owned(),
        value_type: entry.value_type.to_owned(),
        default_value: entry.default_value.map(str::to_owned),
        env_vars: entry.env_vars.iter().map(|value| (*value).to_owned()).collect(),
        secret: entry.secret,
        deprecated: entry.deprecated,
        restart_required: entry.restart_required,
        category: entry.category.to_owned(),
        description: entry.description.to_owned(),
        effective_source,
        effective_value,
        redacted,
    })
}

fn resolve_config_explain_value(
    entry: &palyra_common::daemon_config_schema::ConfigSchemaEntry,
    source: &LoadedConfigExplainSource,
) -> Result<(String, Option<serde_json::Value>, bool)> {
    if let Some((env_name, value)) = entry.env_vars.iter().find_map(|name| {
        env::var(name).ok().map(|value| ((*name).to_owned(), toml::Value::String(value)))
    }) {
        return config_explain_output_value(format!("env:{env_name}"), value, entry.secret);
    }

    if let Some(document) = source.document.as_ref() {
        if let Some(value) = get_value_at_path(document, entry.path)
            .with_context(|| format!("invalid config key path: {}", entry.path))?
        {
            return config_explain_output_value(
                format!("config:{}", source.snapshot.path.as_deref().unwrap_or("unknown")),
                value.clone(),
                entry.secret,
            );
        }
    }

    if let Some(default_value) = entry.default_value {
        return config_explain_output_value(
            "default".to_owned(),
            toml::Value::String(default_value.to_owned()),
            entry.secret,
        );
    }

    Ok(("unset".to_owned(), None, false))
}

fn config_explain_output_value(
    effective_source: String,
    value: toml::Value,
    secret: bool,
) -> Result<(String, Option<serde_json::Value>, bool)> {
    if secret {
        return Ok((effective_source, Some(json!("<redacted>")), true));
    }
    let output_value = serde_json::to_value(value).context("failed to encode config value")?;
    Ok((effective_source, Some(output_value), false))
}

fn render_config_explain_report(report: &ConfigExplainReport) -> Result<()> {
    println!(
        "config.explain source={} exists={} parsed={} entries={}",
        report.source.path.as_deref().unwrap_or("defaults"),
        report.source.exists,
        report.source.parsed,
        report.entries.len()
    );
    for entry in &report.entries {
        let value = entry
            .effective_value
            .as_ref()
            .map(serde_json::Value::to_string)
            .unwrap_or_else(|| "null".to_owned());
        println!(
            "config.explain key={} type={} source={} secret={} deprecated={} restart_required={} value={}",
            entry.key,
            entry.value_type,
            entry.effective_source,
            entry.secret,
            entry.deprecated,
            entry.restart_required,
            value
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn render_config_doctor_report(report: &ConfigDoctorReport) -> Result<()> {
    println!(
        "config.doctor status={} source={} findings={}",
        report.status,
        report.source.path.as_deref().unwrap_or("defaults"),
        report.findings.len()
    );
    for finding in &report.findings {
        println!(
            "config.doctor finding severity={} code={} key={} message=\"{}\" remediation=\"{}\"",
            finding.severity,
            finding.code,
            finding.key,
            escape_config_warning_text(finding.message.as_str()),
            escape_config_warning_text(finding.remediation.as_str())
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn unknown_palyra_env_vars(env_vars: &[(String, String)]) -> Vec<String> {
    let known = palyra_common::daemon_config_schema::known_config_env_vars()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut unknown = env_vars
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| name.starts_with("PALYRA_") && !known.contains(name))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    unknown.sort();
    unknown.dedup();
    unknown
}

fn deprecated_config_flags(
    source: &LoadedConfigExplainSource,
    env_vars: &[(String, String)],
) -> Vec<String> {
    let env_names = env_vars.iter().map(|(name, _)| name.as_str()).collect::<BTreeSet<_>>();
    let mut flags = Vec::new();
    for entry in palyra_common::daemon_config_schema::config_schema_entries()
        .iter()
        .filter(|entry| entry.deprecated)
    {
        if let Some(document) = source.document.as_ref() {
            if matches!(get_value_at_path(document, entry.path), Ok(Some(_))) {
                flags.push(entry.path.to_owned());
            }
        }
        for env_var in entry.env_vars {
            if env_names.contains(env_var) {
                flags.push((*env_var).to_owned());
            }
        }
    }
    flags.sort();
    flags.dedup();
    flags
}

fn unsafe_config_flags(
    source: &LoadedConfigExplainSource,
    env_vars: &[(String, String)],
) -> Vec<String> {
    if !config_doctor_is_production_like(source, env_vars) {
        return Vec::new();
    }
    let mut flags = Vec::new();
    for name in [
        "PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE",
        "PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS",
        "PALYRA_HTTP_FETCH_ALLOW_PRIVATE_TARGETS",
    ] {
        if env_bool(env_vars, name).unwrap_or(false) {
            flags.push(name.to_owned());
        }
    }
    for path in [
        "gateway.allow_insecure_remote",
        "identity.allow_insecure_node_rpc_without_mtls",
        "tool_call.http_fetch.allow_private_targets",
    ] {
        if source_config_bool(source, path).unwrap_or(false) {
            flags.push(path.to_owned());
        }
    }
    flags.sort();
    flags.dedup();
    flags
}

fn missing_config_secrets(
    source: &LoadedConfigExplainSource,
    env_vars: &[(String, String)],
) -> Vec<String> {
    let mut missing = Vec::new();
    if source_config_bool(source, "admin.require_auth").unwrap_or(false)
        && env_trimmed(env_vars, "PALYRA_ADMIN_TOKEN").is_none()
        && source_config_string(source, "admin.auth_token").is_none()
        && source_config_value_present(source, "admin.auth_token_secret_ref").is_none()
    {
        missing.push("admin.auth_token".to_owned());
    }

    let provider_kind = env_trimmed(env_vars, "PALYRA_MODEL_PROVIDER_KIND")
        .or_else(|| source_config_string(source, "model_provider.kind"))
        .unwrap_or_else(|| "openai_compatible".to_owned());
    if !provider_kind.eq_ignore_ascii_case("deterministic")
        && env_trimmed(env_vars, "PALYRA_OPENAI_API_KEY").is_none()
        && env_trimmed(env_vars, "PALYRA_ANTHROPIC_API_KEY").is_none()
        && source_config_string(source, "model_provider.openai_api_key").is_none()
        && source_config_value_present(source, "model_provider.openai_api_key_secret_ref").is_none()
        && source_config_string(source, "model_provider.openai_api_key_vault_ref").is_none()
        && source_config_string(source, "model_provider.anthropic_api_key").is_none()
        && source_config_value_present(source, "model_provider.anthropic_api_key_secret_ref")
            .is_none()
        && source_config_string(source, "model_provider.anthropic_api_key_vault_ref").is_none()
        && source_config_string(source, "model_provider.auth_profile_id").is_none()
    {
        missing.push("model_provider.auth".to_owned());
    }
    missing
}

fn remote_worker_policy_issues(
    source: &LoadedConfigExplainSource,
    env_vars: &[(String, String)],
) -> Vec<String> {
    if !networked_workers_enabled(source, env_vars) {
        return Vec::new();
    }
    let mut issues = Vec::new();
    if source_config_bool(source, "networked_workers.require_attestation") == Some(false)
        || env_bool(env_vars, "PALYRA_NETWORKED_WORKERS_REQUIRE_ATTESTATION") == Some(false)
    {
        issues.push("networked_workers.require_attestation".to_owned());
    }
    if source_config_string(source, "networked_workers.expected_image_digest_sha256").is_none()
        && source_config_string(source, "networked_workers.expected_build_digest_sha256").is_none()
        && source_config_string(source, "networked_workers.expected_artifact_digest_sha256")
            .is_none()
        && env_trimmed(env_vars, "PALYRA_NETWORKED_WORKERS_EXPECTED_IMAGE_DIGEST_SHA256").is_none()
    {
        issues.push("networked_workers.expected_digest".to_owned());
    }
    issues
}

fn config_doctor_is_production_like(
    source: &LoadedConfigExplainSource,
    env_vars: &[(String, String)],
) -> bool {
    let mode = env_trimmed(env_vars, "PALYRA_DEPLOYMENT_MODE")
        .or_else(|| source_config_string(source, "deployment.mode"))
        .unwrap_or_else(|| "local_desktop".to_owned());
    matches!(
        mode.trim().to_ascii_lowercase().as_str(),
        "server" | "remote" | "remote_agent" | "production" | "prod"
    )
}

fn networked_workers_enabled(
    source: &LoadedConfigExplainSource,
    env_vars: &[(String, String)],
) -> bool {
    if env_bool(env_vars, "PALYRA_EXPERIMENTAL_NETWORKED_WORKERS").unwrap_or(false)
        || env_bool(env_vars, "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_NETWORKED_WORKER")
            .unwrap_or(false)
    {
        return true;
    }
    if let Some(mode) = env_trimmed(env_vars, "PALYRA_NETWORKED_WORKERS_MODE")
        .or_else(|| source_config_string(source, "networked_workers.mode"))
    {
        return !mode.eq_ignore_ascii_case("disabled");
    }
    source_config_bool(source, "feature_rollouts.networked_workers").unwrap_or(false)
}

fn env_trimmed(env_vars: &[(String, String)], name: &str) -> Option<String> {
    env_vars.iter().find_map(|(candidate, value)| {
        if candidate == name {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        } else {
            None
        }
    })
}

fn env_bool(env_vars: &[(String, String)], name: &str) -> Option<bool> {
    let value = env_trimmed(env_vars, name)?;
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn source_config_value_present<'a>(
    source: &'a LoadedConfigExplainSource,
    path: &str,
) -> Option<&'a toml::Value> {
    source.document.as_ref().and_then(|document| get_value_at_path(document, path).ok().flatten())
}

fn source_config_string(source: &LoadedConfigExplainSource, path: &str) -> Option<String> {
    source_config_value_present(source, path)
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn source_config_bool(source: &LoadedConfigExplainSource, path: &str) -> Option<bool> {
    source_config_value_present(source, path).and_then(toml::Value::as_bool)
}

fn config_doctor_finding(
    severity: &str,
    code: &str,
    key: impl Into<String>,
    message: impl Into<String>,
    remediation: &str,
) -> ConfigDoctorFinding {
    ConfigDoctorFinding {
        severity: severity.to_owned(),
        code: code.to_owned(),
        key: key.into(),
        message: message.into(),
        remediation: remediation.to_owned(),
    }
}

#[derive(Debug, Serialize)]
struct ConfigStatusPayload {
    path: Option<String>,
    exists: bool,
    parsed: bool,
    migrated: bool,
    source_version: Option<u32>,
    target_version: Option<u32>,
    provider_kind: Option<String>,
    auth_profile_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ConfigValidationWarning {
    severity: &'static str,
    code: &'static str,
    component: &'static str,
    message: String,
    remediation: String,
}

fn build_config_validation_warnings(path: &str) -> Vec<ConfigValidationWarning> {
    match commands::models::load_models_status(Some(path.to_owned())) {
        Ok(status) => {
            if model_provider_auth_is_missing(&status) {
                vec![ConfigValidationWarning {
                    severity: "warning",
                    code: "model_provider_missing_auth",
                    component: "model_provider",
                    message: format!(
                        "config is schema-valid, but model provider '{}' ({}) has no inline API key, vault reference, or auth profile; runtime model calls will fail with missing_auth.",
                        status.provider_id,
                        status.provider_kind
                    ),
                    remediation: "Run `palyra models status --json` and configure a vault-backed key or auth profile before starting model-backed runs.".to_owned(),
                }]
            } else {
                Vec::new()
            }
        }
        Err(_) => vec![ConfigValidationWarning {
            severity: "warning",
            code: "model_provider_readiness_unavailable",
            component: "model_provider",
            message: "config is schema-valid, but model-provider readiness could not be evaluated.".to_owned(),
            remediation: "Run `palyra models status --json` for a dedicated model-provider readiness diagnostic.".to_owned(),
        }],
    }
}

fn model_provider_auth_is_missing(status: &commands::models::ModelsStatusPayload) -> bool {
    !status.provider_kind.eq_ignore_ascii_case("deterministic")
        && !status.api_key_configured
        && status
            .auth_profile_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
}

fn escape_config_warning_text(value: &str) -> String {
    value.replace('"', "'")
}

fn build_config_status_payload(path: Option<String>) -> Result<ConfigStatusPayload> {
    let path = match path {
        Some(explicit) => Some(resolve_config_path(Some(explicit), false)?),
        None => effective_config_path(),
    };
    let Some(path_value) = path else {
        return Ok(ConfigStatusPayload {
            path: None,
            exists: false,
            parsed: false,
            migrated: false,
            source_version: None,
            target_version: None,
            provider_kind: None,
            auth_profile_id: None,
        });
    };
    let path_ref = Path::new(&path_value);
    if !path_ref.exists() {
        return Ok(ConfigStatusPayload {
            path: Some(path_value),
            exists: false,
            parsed: false,
            migrated: false,
            source_version: None,
            target_version: None,
            provider_kind: None,
            auth_profile_id: None,
        });
    }
    let (document, migration) = load_document_from_existing_path(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;
    let provider_kind = get_value_at_path(&document, "model_provider.kind")
        .with_context(|| "invalid config key path: model_provider.kind")?
        .and_then(toml::Value::as_str)
        .map(str::to_owned);
    let auth_profile_id = get_value_at_path(&document, "model_provider.auth_profile_id")
        .with_context(|| "invalid config key path: model_provider.auth_profile_id")?
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    Ok(ConfigStatusPayload {
        path: Some(path_value),
        exists: true,
        parsed: true,
        migrated: migration.migrated,
        source_version: Some(migration.source_version),
        target_version: Some(migration.target_version),
        provider_kind,
        auth_profile_id,
    })
}

fn parse_config_set_value_literal(raw: &str) -> Result<toml::Value> {
    match parse_toml_value_literal(raw) {
        Ok(value) => Ok(value),
        Err(_) if can_treat_config_set_value_as_bare_string(raw) => {
            Ok(toml::Value::String(raw.to_owned()))
        }
        Err(error) => Err(error).context("config set value must be a valid TOML literal"),
    }
}

fn validate_config_set_value(key: &str, value: &toml::Value) -> Result<()> {
    if key == "tool_call.browser_service.auth_token" {
        let Some(token) = value.as_str() else {
            anyhow::bail!("tool_call.browser_service.auth_token must be a non-empty string");
        };
        if token.trim().is_empty() {
            anyhow::bail!("tool_call.browser_service.auth_token must not be empty");
        }
    }
    Ok(())
}

// Lets operators write `config set key plain-text` without TOML quoting. The
// first-byte exclusions keep inputs that look like quoted strings, arrays,
// tables, comments, or assignments on the strict TOML-literal path so a typo
// there fails loudly instead of being stored as a string.
fn can_treat_config_set_value_as_bare_string(raw: &str) -> bool {
    let trimmed = raw.trim();
    !trimmed.is_empty()
        && trimmed == raw
        && !trimmed.contains(['\r', '\n'])
        && !matches!(trimmed.as_bytes().first(), Some(b'"' | b'\'' | b'[' | b'{' | b'#' | b'='))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_redacts_secret_config_values() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let path = temp.path().join("palyra.toml");
        fs::write(
            path.as_path(),
            r#"
            version = 1
            [admin.auth_token_secret_ref]
            kind = "env"
            variable = "PALYRA_ADMIN_TOKEN"
            "#,
        )
        .expect("config should be written");

        let report = build_config_explain_report(
            Some(path.to_string_lossy().into_owned()),
            Some("admin.auth_token_secret_ref"),
        )
        .expect("explain report should build");

        let entry = report.entries.first().expect("secret entry should be present");
        assert!(entry.redacted);
        assert_eq!(entry.effective_source, format!("config:{}", path.display()));
        assert_eq!(entry.effective_value, Some(json!("<redacted>")));
    }

    #[test]
    fn config_doctor_flags_unknown_env_and_deprecated_limit() {
        let env_vars = vec![
            ("PALYRA_UNKNOWN_FLAG".to_owned(), "1".to_owned()),
            ("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN".to_owned(), "8".to_owned()),
            ("PALYRA_MODEL_PROVIDER_KIND".to_owned(), "deterministic".to_owned()),
            ("PALYRA_E2E_HOME".to_owned(), "C:\\fixture-home".to_owned()),
            ("PALYRA_E2E_OS_ROOT".to_owned(), "C:\\fixture-os-root".to_owned()),
            ("PALYRA_OS_FILE_ROOTS".to_owned(), "C:\\fixture-home;C:\\fixture-os-root".to_owned()),
        ];

        let report =
            build_config_doctor_report(None, env_vars.as_slice()).expect("doctor should build");

        assert_eq!(report.status, "warn");
        assert_eq!(report.unknown_env_vars, vec!["PALYRA_UNKNOWN_FLAG"]);
        assert!(report
            .deprecated_flags
            .iter()
            .any(|flag| flag == "PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN"));
    }

    #[test]
    fn config_doctor_fails_production_escape_hatches_and_worker_policy_gaps() {
        let env_vars = vec![
            ("PALYRA_DEPLOYMENT_MODE".to_owned(), "production".to_owned()),
            ("PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE".to_owned(), "true".to_owned()),
            ("PALYRA_EXPERIMENTAL_NETWORKED_WORKERS".to_owned(), "true".to_owned()),
            ("PALYRA_NETWORKED_WORKERS_REQUIRE_ATTESTATION".to_owned(), "false".to_owned()),
        ];

        let report =
            build_config_doctor_report(None, env_vars.as_slice()).expect("doctor should build");

        assert_eq!(report.status, "fail");
        assert!(report
            .unsafe_dev_flags
            .iter()
            .any(|flag| flag == "PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE"));
        assert!(report
            .remote_worker_policy_issues
            .iter()
            .any(|issue| issue == "networked_workers.require_attestation"));
        assert!(report
            .remote_worker_policy_issues
            .iter()
            .any(|issue| issue == "networked_workers.expected_digest"));
    }
}
