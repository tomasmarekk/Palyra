use palyra_control_plane as control_plane;
use serde_json::Value;

use crate::*;

pub(crate) fn run_hooks(command: HooksCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_hooks_async(command))
}

async fn run_hooks_async(command: HooksCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        HooksCommand::List { hook_id, plugin_id, event, enabled_only, ready_only, json } => {
            let envelope = context
                .client
                .list_hooks(&control_plane::HookBindingsQuery { hook_id, plugin_id, event })
                .await?;
            emit_hook_list(envelope, enabled_only, ready_only, json)
        }
        HooksCommand::Info { hook_id, json } => {
            let envelope = context.client.get_hook(hook_id.as_str()).await?;
            emit_hook_envelope("hooks.info", &envelope, json)
        }
        HooksCommand::Check { hook_id, json } => {
            let envelope = context.client.check_hook(hook_id.as_str()).await?;
            emit_hook_envelope("hooks.check", &envelope, json)
        }
        HooksCommand::Bind {
            hook_id,
            event,
            plugin_id,
            display_name,
            notes,
            owner_principal,
            disabled,
            json,
        } => {
            let envelope = context
                .client
                .upsert_hook(&control_plane::HookBindingUpsertRequest {
                    hook_id,
                    event,
                    plugin_id,
                    enabled: Some(!disabled),
                    operator: Some(control_plane::HookOperatorMetadata {
                        display_name,
                        notes,
                        owner_principal,
                        updated_by: None,
                    }),
                })
                .await?;
            emit_hook_envelope("hooks.bind", &envelope, json)
        }
        HooksCommand::Enable { hook_id, json } => {
            let envelope = context.client.enable_hook(hook_id.as_str()).await?;
            emit_hook_envelope("hooks.enable", &envelope, json)
        }
        HooksCommand::Disable { hook_id, json } => {
            let envelope = context.client.disable_hook(hook_id.as_str()).await?;
            emit_hook_envelope("hooks.disable", &envelope, json)
        }
        HooksCommand::Remove { hook_id, json } => {
            let envelope = context.client.delete_hook(hook_id.as_str()).await?;
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &envelope,
                    "failed to encode hook delete output as JSON",
                )?;
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &envelope,
                    "failed to encode hook delete output as NDJSON",
                )?;
            } else {
                println!(
                    "hooks.remove hook_id={} deleted={} event={} plugin_id={}",
                    envelope.binding.hook_id,
                    envelope.deleted,
                    envelope.binding.event,
                    envelope.binding.plugin_id
                );
                std::io::stdout().flush().context("stdout flush failed")?;
            }
            Ok(())
        }
    }
}

fn emit_hook_list(
    mut envelope: control_plane::HookBindingListEnvelope,
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
        return output::print_json_pretty(&envelope, "failed to encode hook list as JSON");
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(&envelope, "failed to encode hook list as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }

    println!("hooks.list root={} count={}", envelope.hooks_root, envelope.entries.len());
    for entry in &envelope.entries {
        println!(
            "hooks.entry hook_id={} event={} plugin_id={} enabled={} ready={}",
            entry.binding.hook_id,
            entry.binding.event,
            entry.binding.plugin_id,
            entry.binding.enabled,
            json_bool(entry.check.as_object(), "ready"),
        );
        if let Some(reasons) = json_string_array(entry.check.as_object(), "reasons") {
            if !reasons.is_empty() {
                println!(
                    "hooks.entry.reasons hook_id={} {}",
                    entry.binding.hook_id,
                    reasons.join(" | ")
                );
            }
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_hook_envelope(
    event: &str,
    envelope: &control_plane::HookBindingEnvelope,
    json: bool,
) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(envelope, "failed to encode hook output as JSON");
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(envelope, "failed to encode hook output as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }

    println!(
        "{event} hook_id={} event_name={} plugin_id={} enabled={} ready={}",
        envelope.binding.hook_id,
        envelope.binding.event,
        envelope.binding.plugin_id,
        envelope.binding.enabled,
        json_bool(envelope.check.as_object(), "ready"),
    );
    if let Some(reasons) = json_string_array(envelope.check.as_object(), "reasons") {
        if !reasons.is_empty() {
            println!("{event}.reasons {}", reasons.join(" | "));
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
