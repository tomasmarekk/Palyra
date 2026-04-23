use std::{
    fs,
    io::{Read, Write},
    path::Path,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_control_plane as control_plane;

use crate::*;

pub(crate) fn run_webhooks(command: WebhooksCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_webhooks_async(command))
}

async fn run_webhooks_async(command: WebhooksCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        WebhooksCommand::List { provider, enabled, json } => {
            let mut query = Vec::<String>::new();
            if let Some(provider) = provider.as_deref().filter(|value| !value.trim().is_empty()) {
                query.push(format!("provider={}", provider.trim()));
            }
            if let Some(enabled) = enabled {
                query.push(format!("enabled={enabled}"));
            }
            let envelope = context.client.list_webhooks(query.join("&").as_str()).await?;
            emit_webhook_list(&envelope, json)
        }
        WebhooksCommand::Show { integration_id, json } => {
            let envelope = context.client.get_webhook(integration_id.as_str()).await?;
            emit_webhook_envelope(&envelope, json)
        }
        WebhooksCommand::Add {
            integration_id,
            provider,
            display_name,
            secret_vault_ref,
            allowed_events,
            allowed_sources,
            disabled,
            require_signature,
            max_payload_bytes,
            json,
        } => {
            let envelope = context
                .client
                .upsert_webhook(&control_plane::WebhookIntegrationUpsertRequest {
                    integration_id,
                    provider,
                    display_name,
                    secret_vault_ref,
                    allowed_events,
                    allowed_sources,
                    enabled: Some(!disabled),
                    signature_required: require_signature.then_some(true),
                    max_payload_bytes,
                })
                .await?;
            emit_webhook_envelope(&envelope, json)
        }
        WebhooksCommand::Enable { integration_id, json } => {
            let envelope = context
                .client
                .set_webhook_enabled(
                    integration_id.as_str(),
                    &control_plane::WebhookIntegrationEnabledRequest { enabled: true },
                )
                .await?;
            emit_webhook_envelope(&envelope, json)
        }
        WebhooksCommand::Disable { integration_id, json } => {
            let envelope = context
                .client
                .set_webhook_enabled(
                    integration_id.as_str(),
                    &control_plane::WebhookIntegrationEnabledRequest { enabled: false },
                )
                .await?;
            emit_webhook_envelope(&envelope, json)
        }
        WebhooksCommand::Remove { integration_id, json } => {
            let envelope = context.client.delete_webhook(integration_id.as_str()).await?;
            if json {
                output::print_json_pretty(
                    &envelope,
                    "failed to encode webhook remove output as JSON",
                )?;
            } else {
                println!(
                    "webhooks.remove integration_id={} deleted={}",
                    envelope.integration_id, envelope.deleted
                );
                std::io::stdout().flush().context("stdout flush failed")?;
            }
            Ok(())
        }
        WebhooksCommand::Test { integration_id, payload_stdin, payload_file, json } => {
            let payload = read_test_payload(payload_stdin, payload_file.as_deref())?;
            let envelope = context
                .client
                .test_webhook(
                    integration_id.as_str(),
                    &control_plane::WebhookIntegrationTestRequest {
                        payload_base64: BASE64_STANDARD.encode(payload),
                    },
                )
                .await?;
            if json {
                output::print_json_pretty(
                    &envelope,
                    "failed to encode webhook test output as JSON",
                )?;
            } else {
                println!(
                    "webhooks.test integration_id={} valid={} outcome={} event={} source={} signature_present={} secret_present={}",
                    envelope.result.integration_id,
                    envelope.result.valid,
                    envelope.result.outcome,
                    envelope.result.event.as_deref().unwrap_or("unknown"),
                    envelope.result.source.as_deref().unwrap_or("unknown"),
                    envelope.result.signature_present,
                    envelope.result.secret_present
                );
                println!("webhooks.test.message={}", envelope.result.message);
                std::io::stdout().flush().context("stdout flush failed")?;
            }
            Ok(())
        }
    }
}

fn emit_webhook_list(
    envelope: &control_plane::WebhookIntegrationListEnvelope,
    json: bool,
) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(envelope, "failed to encode webhook list output as JSON");
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(envelope, "failed to encode webhook list output as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }
    println!("webhooks.list count={}", envelope.integrations.len());
    for integration in &envelope.integrations {
        println!(
            "webhook.integration id={} provider={} status={} enabled={} secret_present={} signature_required={}",
            integration.integration_id,
            integration.provider,
            integration.status,
            integration.enabled,
            integration.secret_present,
            integration.signature_required
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_webhook_envelope(
    envelope: &control_plane::WebhookIntegrationEnvelope,
    json: bool,
) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(envelope, "failed to encode webhook output as JSON");
    }
    if output::preferred_ndjson(json, false) {
        output::print_json_line(envelope, "failed to encode webhook output as NDJSON")?;
        return std::io::stdout().flush().context("stdout flush failed");
    }
    let integration = &envelope.integration;
    println!(
        "webhooks.show id={} provider={} status={} enabled={} secret_present={} signature_required={} max_payload_bytes={}",
        integration.integration_id,
        integration.provider,
        integration.status,
        integration.enabled,
        integration.secret_present,
        integration.signature_required,
        integration.max_payload_bytes
    );
    println!(
        "webhooks.show.display_name={} allowed_events={} allowed_sources={}",
        integration.display_name,
        integration.allowed_events.join(","),
        integration.allowed_sources.join(",")
    );
    if let Some(last_test_status) = integration.last_test_status.as_deref() {
        println!(
            "webhooks.show.last_test status={} at_unix_ms={} message={}",
            last_test_status,
            integration.last_test_at_unix_ms.unwrap_or_default(),
            integration.last_test_message.as_deref().unwrap_or(""),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn read_test_payload(payload_stdin: bool, payload_file: Option<&str>) -> Result<Vec<u8>> {
    match (payload_stdin, payload_file) {
        (true, None) => {
            let mut payload = Vec::new();
            std::io::stdin()
                .read_to_end(&mut payload)
                .context("failed to read webhook payload from stdin")?;
            if payload.is_empty() {
                anyhow::bail!("stdin did not contain any webhook payload bytes");
            }
            Ok(payload)
        }
        (false, Some(path)) => {
            let payload = fs::read(Path::new(path))
                .with_context(|| format!("failed to read webhook payload file {path}"))?;
            if payload.is_empty() {
                anyhow::bail!("payload file did not contain any webhook payload bytes");
            }
            Ok(payload)
        }
        (false, None) => anyhow::bail!("webhook test requires --payload-stdin or --payload-file"),
        (true, Some(_)) => anyhow::bail!("--payload-stdin conflicts with --payload-file"),
    }
}
