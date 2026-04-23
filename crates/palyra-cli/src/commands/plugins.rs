use palyra_control_plane as control_plane;
use serde_json::{json, Value};

use crate::*;

pub(crate) fn run_plugins(command: PluginsCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_plugins_async(command))
}

async fn run_plugins_async(command: PluginsCommand) -> Result<()> {
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
            config_json,
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
                    allow_tofu: allow_tofu.then_some(true),
                    allow_untrusted: allow_untrusted.then_some(true),
                    capability_profile: Some(control_plane::PluginCapabilityProfile {
                        http_hosts: capability_http_hosts,
                        secrets: capability_secrets,
                        storage_prefixes: capability_storage_prefixes,
                        channels: capability_channels,
                    }),
                    config: parse_config_json(config_json.as_deref())?,
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
            config_json,
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
                    allow_tofu: allow_tofu.then_some(true),
                    allow_untrusted: allow_untrusted.then_some(true),
                    capability_profile: Some(control_plane::PluginCapabilityProfile {
                        http_hosts: capability_http_hosts,
                        secrets: capability_secrets,
                        storage_prefixes: capability_storage_prefixes,
                        channels: capability_channels,
                    }),
                    config: parse_config_json(config_json.as_deref())?,
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

fn parse_config_json(raw: Option<&str>) -> Result<Option<Value>> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let value: Value = serde_json::from_str(raw)
        .with_context(|| "failed to parse --config-json as JSON object")?;
    if !value.is_object() {
        anyhow::bail!("--config-json must be a JSON object");
    }
    Ok(Some(value))
}
