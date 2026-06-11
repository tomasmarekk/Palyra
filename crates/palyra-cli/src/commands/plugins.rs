//! `palyra plugins`: manage plugin bindings (list, inspect, check, install,
//! update, enable/disable, remove) through the daemon control plane.
//! Plugin config JSON is parsed and size-capped locally before any daemon
//! connection, and inline `--config-json` must not carry plain secret values.

use palyra_control_plane as control_plane;
use serde_json::{json, Value};

use crate::*;

const MAX_PLUGIN_CONFIG_JSON_BYTES: usize = 128 * 1024;

/// Runs a `palyra plugins` subcommand on a dedicated Tokio runtime.
///
/// # Errors
/// Returns an error when plugin config input is invalid or secret-bearing
/// inline, or when the daemon control-plane call fails.
pub(crate) fn run_plugins(command: PluginsCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_plugins_async(command))
}

async fn run_plugins_async(command: PluginsCommand) -> Result<()> {
    // Parse and validate config input before dialing the daemon so malformed
    // or secret-bearing input fails fast without opening a connection.
    let prevalidated_config = match &command {
        PluginsCommand::Install { config_json, config_json_file, config_json_stdin, .. }
        | PluginsCommand::Update { config_json, config_json_file, config_json_stdin, .. } => {
            Some(parse_config_json_input(
                config_json.as_deref(),
                config_json_file.as_deref(),
                *config_json_stdin,
            )?)
        }
        _ => None,
    };
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        PluginsCommand::List { plugin_id, skill_id, enabled_only, ready_only, json } => {
            let envelope = context
                .client
                .list_plugins(&control_plane::PluginBindingsQuery { plugin_id, skill_id })
                .await?;
            emit_plugin_list("plugins.list", envelope, enabled_only, ready_only, json)
        }
        PluginsCommand::Inspect { plugin_id, json } => {
            let envelope = context.client.get_plugin(plugin_id.as_str()).await?;
            emit_plugin_envelope("plugins.inspect", &envelope, json)
        }
        PluginsCommand::Discover { plugin_id, skill_id, enabled_only, ready_only, json } => {
            let envelope = context
                .client
                .list_plugins(&control_plane::PluginBindingsQuery { plugin_id, skill_id })
                .await?;
            emit_plugin_list("plugins.discover", envelope, enabled_only, ready_only, json)
        }
        PluginsCommand::Check { plugin_id, json } => {
            let envelope = context.client.check_plugin(plugin_id.as_str()).await?;
            emit_plugin_envelope("plugins.check", &envelope, json)
        }
        PluginsCommand::Explain { plugin_id, json } => {
            let envelope = context.client.get_plugin(plugin_id.as_str()).await?;
            emit_plugin_explain(&envelope, json)
        }
        PluginsCommand::Doctor { plugin_id, json } => {
            let envelope = context
                .client
                .list_plugins(&control_plane::PluginBindingsQuery { plugin_id, skill_id: None })
                .await?;
            emit_plugin_doctor(envelope, json)
        }
        PluginsCommand::Install {
            plugin_id,
            skill_id,
            skill_version,
            artifact_path,
            tool_id,
            module_path,
            entrypoint,
            capability_http_hosts,
            capability_secrets,
            capability_storage_prefixes,
            capability_channels,
            display_name,
            notes,
            owner_principal,
            tags,
            config_json: _,
            config_json_file: _,
            config_json_stdin: _,
            clear_config,
            disabled,
            allow_tofu,
            allow_untrusted,
            json,
        } => {
            let envelope = context
                .client
                .upsert_plugin(&control_plane::PluginBindingUpsertRequest {
                    plugin_id,
                    skill_id,
                    skill_version,
                    artifact_path,
                    tool_id,
                    module_path,
                    entrypoint,
                    enabled: Some(!disabled),
                    allow_tofu: Some(allow_tofu),
                    allow_untrusted: allow_untrusted.then_some(true),
                    capability_profile: Some(control_plane::PluginCapabilityProfile {
                        http_hosts: capability_http_hosts,
                        secrets: capability_secrets,
                        storage_prefixes: capability_storage_prefixes,
                        channels: capability_channels,
                    }),
                    config: prevalidated_config
                        .expect("install config is prevalidated before the daemon connection"),
                    clear_config: clear_config.then_some(true),
                    operator: Some(control_plane::PluginOperatorMetadata {
                        display_name,
                        notes,
                        owner_principal,
                        updated_by: None,
                        tags,
                    }),
                })
                .await?;
            emit_plugin_envelope("plugins.install", &envelope, json)
        }
        PluginsCommand::Update {
            plugin_id,
            skill_id,
            skill_version,
            artifact_path,
            tool_id,
            module_path,
            entrypoint,
            capability_http_hosts,
            capability_secrets,
            capability_storage_prefixes,
            capability_channels,
            display_name,
            notes,
            owner_principal,
            tags,
            config_json: _,
            config_json_file: _,
            config_json_stdin: _,
            clear_config,
            disabled,
            allow_tofu,
            allow_untrusted,
            json,
        } => {
            let envelope = context
                .client
                .upsert_plugin(&control_plane::PluginBindingUpsertRequest {
                    plugin_id,
                    skill_id,
                    skill_version,
                    artifact_path,
                    tool_id,
                    module_path,
                    entrypoint,
                    enabled: Some(!disabled),
                    allow_tofu: Some(allow_tofu),
                    allow_untrusted: allow_untrusted.then_some(true),
                    capability_profile: Some(control_plane::PluginCapabilityProfile {
                        http_hosts: capability_http_hosts,
                        secrets: capability_secrets,
                        storage_prefixes: capability_storage_prefixes,
                        channels: capability_channels,
                    }),
                    config: prevalidated_config
                        .expect("update config is prevalidated before the daemon connection"),
                    clear_config: clear_config.then_some(true),
                    operator: Some(control_plane::PluginOperatorMetadata {
                        display_name,
                        notes,
                        owner_principal,
                        updated_by: None,
                        tags,
                    }),
                })
                .await?;
            emit_plugin_envelope("plugins.update", &envelope, json)
        }
        PluginsCommand::Enable { plugin_id, json } => {
            let envelope = context.client.enable_plugin(plugin_id.as_str()).await?;
            emit_plugin_envelope("plugins.enable", &envelope, json)
        }
        PluginsCommand::Disable { plugin_id, json } => {
            let envelope = context.client.disable_plugin(plugin_id.as_str()).await?;
            emit_plugin_envelope("plugins.disable", &envelope, json)
        }
        PluginsCommand::Remove { plugin_id, json } => {
            let envelope = context.client.delete_plugin(plugin_id.as_str()).await?;
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &envelope,
                    "failed to encode plugin delete output as JSON",
                )?;
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &envelope,
                    "failed to encode plugin delete output as NDJSON",
                )?;
            } else {
                println!(
                    "plugins.remove plugin_id={} deleted={} skill_id={} enabled={}",
                    envelope.binding.plugin_id,
                    envelope.deleted,
                    envelope.binding.skill_id,
                    envelope.binding.enabled
                );
                std::io::stdout().flush().context("stdout flush failed")?;
            }
            Ok(())
        }
    }
}

fn emit_plugin_list(
    event: &str,
    mut envelope: control_plane::PluginBindingListEnvelope,
    enabled_only: bool,
    ready_only: bool,
    json: bool,
) -> Result<()> {
    if enabled_only {
        envelope.entries.retain(|entry| entry.binding.enabled);
    }
    if ready_only {
        envelope.entries.retain(|entry| json_bool(entry.check.as_object(), "ready"));
    }

    if output::preferred_json(json) {
        return output::print_json_pretty(&envelope, "failed to encode plugin list as JSON");
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(&envelope, "failed to encode plugin list as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }

    println!(
        "{event} root={} schema_version={} count={}",
        envelope.plugins_root,
        envelope.schema_version,
        envelope.entries.len()
    );
    for entry in &envelope.entries {
        let discovery_state = json_path_string(entry.check.as_object(), &["discovery", "state"])
            .unwrap_or_else(|| "unknown".to_owned());
        let config_state =
            json_path_string(entry.check.as_object(), &["config", "validation", "state"])
                .unwrap_or_else(|| "unknown".to_owned());
        let contract_mode = json_path_string(entry.check.as_object(), &["contracts", "mode"])
            .unwrap_or_else(|| "untyped_legacy".to_owned());
        let contracts_ready = json_path_bool(entry.check.as_object(), &["contracts", "ready"]);
        println!(
            "{event}.entry plugin_id={} enabled={} skill_id={} skill_version={} ready={} discovery={} config={} contracts_mode={} contracts_ready={} tool_id={} module_path={}",
            entry.binding.plugin_id,
            entry.binding.enabled,
            entry.binding.skill_id,
            entry.binding.skill_version.as_deref().unwrap_or("current"),
            json_bool(entry.check.as_object(), "ready"),
            discovery_state,
            config_state,
            contract_mode,
            contracts_ready,
            entry.binding.tool_id.as_deref().unwrap_or("default"),
            entry.binding.module_path.as_deref().unwrap_or("auto"),
        );
        if let Some(reasons) = json_string_array(entry.check.as_object(), "reasons") {
            if !reasons.is_empty() {
                println!(
                    "{event}.entry.reasons plugin_id={} {}",
                    entry.binding.plugin_id,
                    reasons.join(" | ")
                );
            }
        }
        if let Some(remediation) = json_string_array(entry.check.as_object(), "remediation") {
            if !remediation.is_empty() {
                println!(
                    "{event}.entry.remediation plugin_id={} {}",
                    entry.binding.plugin_id,
                    remediation.join(" | ")
                );
            }
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_plugin_envelope(
    event: &str,
    envelope: &control_plane::PluginBindingEnvelope,
    json: bool,
) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(envelope, "failed to encode plugin output as JSON");
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(envelope, "failed to encode plugin output as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }

    println!(
        "{event} plugin_id={} schema_version={} enabled={} skill_id={} skill_version={} ready={} tool_id={} module_path={} entrypoint={}",
        envelope.binding.plugin_id,
        envelope.schema_version,
        envelope.binding.enabled,
        envelope.binding.skill_id,
        envelope.binding.skill_version.as_deref().unwrap_or("current"),
        json_bool(envelope.check.as_object(), "ready"),
        envelope.binding.tool_id.as_deref().unwrap_or("default"),
        envelope.binding.module_path.as_deref().unwrap_or("auto"),
        envelope.binding.entrypoint.as_deref().unwrap_or("run"),
    );
    if let Some(reasons) = json_string_array(envelope.check.as_object(), "reasons") {
        if !reasons.is_empty() {
            println!("{event}.reasons {}", reasons.join(" | "));
        }
    }
    if let Some(remediation) = json_string_array(envelope.check.as_object(), "remediation") {
        if !remediation.is_empty() {
            println!("{event}.remediation {}", remediation.join(" | "));
        }
    }
    if let Some(discovery_state) =
        json_path_string(envelope.check.as_object(), &["discovery", "state"])
    {
        println!("{event}.discovery state={discovery_state}");
    }
    if let Some(config_state) =
        json_path_string(envelope.check.as_object(), &["config", "validation", "state"])
    {
        println!("{event}.config state={config_state}");
    }
    if let Some(contract_mode) =
        json_path_string(envelope.check.as_object(), &["contracts", "mode"])
    {
        println!(
            "{event}.contracts mode={} ready={}",
            contract_mode,
            json_path_bool(envelope.check.as_object(), &["contracts", "ready"])
        );
    }
    emit_plugin_contract_entries(event, envelope.check.as_object());
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_plugin_explain(envelope: &control_plane::PluginBindingEnvelope, json: bool) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(
            envelope,
            "failed to encode plugin explain output as JSON",
        );
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(envelope, "failed to encode plugin explain output as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }
    println!(
        "plugins.explain plugin_id={} schema_version={} ready={} discovery={} config={} skill_id={} version={}",
        envelope.binding.plugin_id,
        envelope.schema_version,
        json_bool(envelope.check.as_object(), "ready"),
        json_path_string(envelope.check.as_object(), &["discovery", "state"]).unwrap_or_else(|| "unknown".to_owned()),
        json_path_string(envelope.check.as_object(), &["config", "validation", "state"]).unwrap_or_else(|| "unknown".to_owned()),
        envelope.binding.skill_id,
        envelope.binding.skill_version.as_deref().unwrap_or("current"),
    );
    if let Some(reasons) = json_string_array(envelope.check.as_object(), "reasons") {
        for reason in reasons {
            println!("plugins.explain.reason {}", reason);
        }
    }
    if let Some(remediation) = json_string_array(envelope.check.as_object(), "remediation") {
        for step in remediation {
            println!("plugins.explain.remediation {}", step);
        }
    }
    if let Some(entries) = json_path_array(envelope.check.as_object(), &["capabilities", "entries"])
    {
        for entry in entries {
            let category = entry.get("category").and_then(Value::as_str).unwrap_or("unknown");
            let capability_kind =
                entry.get("capability_kind").and_then(Value::as_str).unwrap_or("unknown");
            let value = entry.get("value").and_then(Value::as_str).unwrap_or("n/a");
            let message = entry.get("message").and_then(Value::as_str).unwrap_or_default();
            println!(
                "plugins.explain.capability category={} kind={} value={} message={}",
                category, capability_kind, value, message
            );
        }
    }
    emit_plugin_contract_entries("plugins.explain", envelope.check.as_object());
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_plugin_doctor(
    envelope: control_plane::PluginBindingListEnvelope,
    json: bool,
) -> Result<()> {
    let mut total = 0_usize;
    let mut ready = 0_usize;
    let mut unhealthy = 0_usize;
    let mut plugin_reports = Vec::new();
    for entry in envelope.entries {
        total += 1;
        let is_ready = json_bool(entry.check.as_object(), "ready");
        if is_ready {
            ready += 1;
        } else {
            unhealthy += 1;
        }
        plugin_reports.push(json!({
            "plugin_id": entry.binding.plugin_id,
            "skill_id": entry.binding.skill_id,
            "ready": is_ready,
            "discovery": json_path_string(entry.check.as_object(), &["discovery", "state"]),
            "config": json_path_string(entry.check.as_object(), &["config", "validation", "state"]),
            "contracts_mode": json_path_string(entry.check.as_object(), &["contracts", "mode"]),
            "contracts_ready": json_path_bool(entry.check.as_object(), &["contracts", "ready"]),
            "reasons": json_string_array(entry.check.as_object(), "reasons").unwrap_or_default(),
            "remediation": json_string_array(entry.check.as_object(), "remediation").unwrap_or_default(),
        }));
    }
    let summary = json!({
        "schema_version": envelope.schema_version,
        "plugins_root": envelope.plugins_root,
        "total": total,
        "ready": ready,
        "unhealthy": unhealthy,
        "plugins": plugin_reports,
    });
    if output::preferred_json(json) {
        return output::print_json_pretty(
            &summary,
            "failed to encode plugin doctor output as JSON",
        );
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(&summary, "failed to encode plugin doctor output as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }
    println!(
        "plugins.doctor schema_version={} root={} total={} ready={} unhealthy={}",
        envelope.schema_version, envelope.plugins_root, total, ready, unhealthy
    );
    if let Some(plugins) = summary.get("plugins").and_then(Value::as_array) {
        for plugin in plugins {
            println!(
                "plugins.doctor.entry plugin_id={} ready={} discovery={} config={} contracts_mode={} contracts_ready={}",
                plugin.get("plugin_id").and_then(Value::as_str).unwrap_or("unknown"),
                plugin.get("ready").and_then(Value::as_bool).unwrap_or(false),
                plugin.get("discovery").and_then(Value::as_str).unwrap_or("unknown"),
                plugin.get("config").and_then(Value::as_str).unwrap_or("unknown"),
                plugin.get("contracts_mode").and_then(Value::as_str).unwrap_or("unknown"),
                plugin.get("contracts_ready").and_then(Value::as_bool).unwrap_or(false),
            );
            if let Some(reasons) = plugin.get("reasons").and_then(Value::as_array) {
                for reason in reasons.iter().filter_map(Value::as_str) {
                    println!("plugins.doctor.reason {}", reason);
                }
            }
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn json_bool(object: Option<&serde_json::Map<String, Value>>, key: &str) -> bool {
    object.and_then(|value| value.get(key)).and_then(Value::as_bool).unwrap_or(false)
}

fn json_string_array(
    object: Option<&serde_json::Map<String, Value>>,
    key: &str,
) -> Option<Vec<String>> {
    object
        .and_then(|value| value.get(key))
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).map(str::to_owned).collect::<Vec<_>>())
}

fn json_path_string(
    object: Option<&serde_json::Map<String, Value>>,
    path: &[&str],
) -> Option<String> {
    json_path_value(object, path).and_then(Value::as_str).map(str::to_owned)
}

fn json_path_array<'a>(
    object: Option<&'a serde_json::Map<String, Value>>,
    path: &[&str],
) -> Option<&'a Vec<Value>> {
    json_path_value(object, path).and_then(Value::as_array)
}

fn json_path_bool(object: Option<&serde_json::Map<String, Value>>, path: &[&str]) -> bool {
    json_path_value(object, path).and_then(Value::as_bool).unwrap_or(false)
}

fn json_path_value<'a>(
    object: Option<&'a serde_json::Map<String, Value>>,
    path: &[&str],
) -> Option<&'a Value> {
    let mut current = object?.get(path.first().copied()?)?;
    for segment in &path[1..] {
        current = current.as_object()?.get(*segment)?;
    }
    Some(current)
}

fn emit_plugin_contract_entries(event: &str, object: Option<&serde_json::Map<String, Value>>) {
    let Some(entries) = json_path_array(object, &["contracts", "entries"]) else {
        return;
    };
    for entry in entries {
        let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("unknown");
        let requested_version =
            entry.get("requested_version").and_then(Value::as_u64).unwrap_or_default();
        let status = entry.get("status").and_then(Value::as_str).unwrap_or("unknown");
        let adapter = entry.get("adapter").and_then(Value::as_str).unwrap_or("n/a");
        let reasons = entry
            .get("reasons")
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(" | "))
            .unwrap_or_default();
        println!(
            "{event}.contract kind={} requested_version={} status={} adapter={} reasons={}",
            kind, requested_version, status, adapter, reasons
        );
    }
}

fn parse_config_json_input(
    inline_json: Option<&str>,
    file_path: Option<&str>,
    stdin_enabled: bool,
) -> Result<Option<Value>> {
    let inline_json = inline_json.map(str::trim).filter(|value| !value.is_empty());
    let file_path = file_path.map(str::trim).filter(|value| !value.is_empty());
    let source_count = usize::from(inline_json.is_some())
        + usize::from(file_path.is_some())
        + usize::from(stdin_enabled);
    if source_count > 1 {
        anyhow::bail!(
            "choose only one plugin config source: --config-json, --config-json-file, or --config-json-stdin"
        );
    }

    if let Some(raw) = inline_json {
        let value = parse_config_json_source(raw, "--config-json")?;
        if let Some(value) = &value {
            reject_inline_config_secrets(value)?;
        }
        return Ok(value);
    }

    if let Some(path) = file_path {
        let raw = read_config_json_file(path)?;
        return parse_config_json_source(raw.as_str(), "--config-json-file");
    }

    if stdin_enabled {
        let raw = read_config_json_stdin()?;
        return parse_config_json_source(raw.as_str(), "--config-json-stdin");
    }

    Ok(None)
}

fn parse_config_json_source(raw: &str, source: &str) -> Result<Option<Value>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    if raw.len() > MAX_PLUGIN_CONFIG_JSON_BYTES {
        anyhow::bail!(
            "{source} exceeds max bytes ({} > {})",
            raw.len(),
            MAX_PLUGIN_CONFIG_JSON_BYTES
        );
    }
    let value: Value = serde_json::from_str(raw)
        .with_context(|| format!("failed to parse {source} as JSON object"))?;
    if !value.is_object() {
        anyhow::bail!("{source} must be a JSON object");
    }
    Ok(Some(value))
}

fn read_config_json_file(path_raw: &str) -> Result<String> {
    let path = Path::new(path_raw);
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect --config-json-file {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("--config-json-file must not be a symlink: {}", path.display());
    }
    if !metadata.is_file() {
        anyhow::bail!("--config-json-file must point to a regular file: {}", path.display());
    }
    if metadata.len() > MAX_PLUGIN_CONFIG_JSON_BYTES as u64 {
        anyhow::bail!(
            "--config-json-file exceeds max bytes ({} > {})",
            metadata.len(),
            MAX_PLUGIN_CONFIG_JSON_BYTES
        );
    }

    let file = fs::File::open(path)
        .with_context(|| format!("failed to open --config-json-file {}", path.display()))?;
    let mut raw = String::new();
    // take(max + 1) re-checks the size during the read so a file growing after
    // the metadata check above still cannot exceed the cap.
    let mut reader = file.take(MAX_PLUGIN_CONFIG_JSON_BYTES as u64 + 1);
    reader
        .read_to_string(&mut raw)
        .with_context(|| format!("failed to read --config-json-file {}", path.display()))?;
    ensure_config_json_size(raw.as_str(), "--config-json-file")?;
    Ok(raw)
}

fn read_config_json_stdin() -> Result<String> {
    let stdin = std::io::stdin();
    let mut raw = String::new();
    let mut reader = stdin.lock().take(MAX_PLUGIN_CONFIG_JSON_BYTES as u64 + 1);
    reader.read_to_string(&mut raw).context("failed to read --config-json-stdin from stdin")?;
    ensure_config_json_size(raw.as_str(), "--config-json-stdin")?;
    Ok(raw)
}

fn ensure_config_json_size(raw: &str, source: &str) -> Result<()> {
    if raw.len() > MAX_PLUGIN_CONFIG_JSON_BYTES {
        anyhow::bail!(
            "{source} exceeds max bytes ({} > {})",
            raw.len(),
            MAX_PLUGIN_CONFIG_JSON_BYTES
        );
    }
    Ok(())
}

// Inline --config-json travels through argv and shell history, so plain secret
// values are rejected there; files, stdin, and vault references remain the
// supported secret-bearing paths.
fn reject_inline_config_secrets(value: &Value) -> Result<()> {
    let mut rejected_paths = Vec::new();
    collect_inline_config_secret_paths(value, "$", &mut rejected_paths);
    if rejected_paths.is_empty() {
        return Ok(());
    }

    anyhow::bail!(
        "--config-json contains secret-like inline value(s) at {}. Use --config-json-file, --config-json-stdin, or vault references for secret-bearing plugin config.",
        rejected_paths.join(", ")
    );
}

fn collect_inline_config_secret_paths(value: &Value, path: &str, rejected_paths: &mut Vec<String>) {
    match value {
        Value::Object(fields) => {
            for (key, child) in fields {
                let child_path = append_json_path(path, key.as_str());
                if is_inline_secret_config_key(key.as_str())
                    && contains_plain_inline_secret_value(child)
                {
                    rejected_paths.push(child_path);
                    continue;
                }
                collect_inline_config_secret_paths(child, child_path.as_str(), rejected_paths);
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                let child_path = format!("{path}[{index}]");
                collect_inline_config_secret_paths(child, child_path.as_str(), rejected_paths);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn is_inline_secret_config_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '_' })
        .collect::<String>();
    const SECRET_KEY_MARKERS: &[&str] = &[
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "bearer",
        "client_secret",
        "cookie",
        "credential",
        "password",
        "private_key",
        "refresh_token",
        "secret",
        "set_cookie",
        "signature",
        "token",
        "vault_ref",
        "x_amz_credential",
        "x_amz_security_token",
        "x_amz_signature",
        "x_goog_credential",
        "x_goog_signature",
    ];
    SECRET_KEY_MARKERS.iter().any(|marker| normalized.contains(marker))
}

fn contains_plain_inline_secret_value(value: &Value) -> bool {
    match value {
        Value::String(raw) => !is_safe_inline_secret_reference(raw.as_str()),
        Value::Array(values) => values.iter().any(contains_plain_inline_secret_value),
        Value::Object(fields) => {
            !is_structured_vault_reference(fields)
                && fields.values().any(contains_plain_inline_secret_value)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

// Only known vault-reference metadata keys are allowed so an object cannot
// smuggle a plain secret value in an extra field next to a valid vault_ref.
fn is_structured_vault_reference(fields: &serde_json::Map<String, Value>) -> bool {
    const ALLOWED_KEYS: &[&str] = &[
        "kind",
        "vault_ref",
        "required",
        "refresh_policy",
        "snapshot_policy",
        "max_bytes",
        "redaction_label",
        "display_name",
    ];
    if !fields.keys().all(|key| ALLOWED_KEYS.contains(&key.as_str())) {
        return false;
    }

    let Some(vault_ref) = fields.get("vault_ref").and_then(Value::as_str) else {
        return false;
    };
    let kind_is_vault =
        fields.get("kind").and_then(Value::as_str).map(|kind| kind == "vault").unwrap_or(true);
    kind_is_vault && is_safe_inline_secret_reference(vault_ref)
}

fn is_safe_inline_secret_reference(raw: &str) -> bool {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == REDACTED {
        return true;
    }
    let vault_ref = trimmed
        .strip_prefix("vault://")
        .or_else(|| trimmed.strip_prefix("vault:"))
        .unwrap_or(trimmed);
    VaultRef::parse(vault_ref).is_ok()
}

fn append_json_path(parent: &str, key: &str) -> String {
    if key.chars().all(|ch| ch == '_' || ch == '-' || ch.is_ascii_alphanumeric()) {
        format!("{parent}.{key}")
    } else {
        format!("{parent}[{}]", json!(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_plugin_config_rejects_plain_secret_values() {
        let error = parse_config_json_input(
            Some(r#"{"api_token":"sk-test-secret","timeout_ms":1000}"#),
            None,
            false,
        )
        .expect_err("plain inline api token should be rejected")
        .to_string();

        assert!(error.contains("--config-json"), "{error}");
        assert!(error.contains("$.api_token"), "{error}");
        assert!(error.contains("--config-json-file"), "{error}");
        assert!(error.contains("--config-json-stdin"), "{error}");
    }

    #[test]
    fn inline_plugin_config_accepts_vault_refs_for_sensitive_keys() {
        let parsed = parse_config_json_input(
            Some(r#"{"api_token":"vault://global/openai_api_key","nested":{"client_secret":{"kind":"vault","vault_ref":"global/client_secret"}}}"#),
            None,
            false,
        )
        .expect("vault-backed inline config should parse")
        .expect("config should be present");

        assert_eq!(parsed["api_token"], Value::String("vault://global/openai_api_key".to_owned()));
        assert_eq!(parsed["nested"]["client_secret"]["kind"], Value::String("vault".to_owned()));
    }

    #[test]
    fn file_plugin_config_allows_secret_values_outside_argv() {
        let path = unique_plugin_config_path("plain-secret");
        std::fs::write(&path, r#"{"api_token":"sk-test-secret"}"#)
            .expect("write temp plugin config");

        let parsed = parse_config_json_input(
            None,
            Some(path.to_str().expect("temp plugin config path should be UTF-8")),
            false,
        )
        .expect("file-backed config should parse")
        .expect("config should be present");

        let _ = std::fs::remove_file(&path);
        assert_eq!(parsed["api_token"], Value::String("sk-test-secret".to_owned()));
    }

    #[test]
    fn plugin_config_rejects_multiple_sources() {
        let error = parse_config_json_input(Some("{}"), Some("config.json"), false)
            .expect_err("multiple config sources should be rejected")
            .to_string();
        assert!(error.contains("choose only one plugin config source"), "{error}");
    }

    fn unique_plugin_config_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("palyra-plugin-config-{label}-{}-{nonce}.json", std::process::id()))
    }
}
