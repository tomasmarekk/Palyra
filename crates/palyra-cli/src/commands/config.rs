//! `palyra config`: status, path resolution, validation, and key-level
//! get/set/unset/migrate/recover operations on the daemon TOML config.
//! Every mutation revalidates the document against the daemon schema before
//! persisting; secret values are redacted in output unless `--show-secrets`.

use crate::*;

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
