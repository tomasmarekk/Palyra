use crate::*;

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
        ConfigCommand::Validate { path } => {
            let path = match path {
                Some(explicit) => resolve_config_path(Some(explicit), true)?,
                None => {
                    if let Some(found) = effective_config_path() {
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
