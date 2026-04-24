use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value};
use std::io::{IsTerminal, Read};

use crate::{
    args::{ChannelProviderArg, ChannelResolveEntityArg},
    client::channels as channels_client,
    normalize_optional_text_arg, normalize_required_text_arg, prompt_secret_value,
};

pub(super) fn load_channel_credential(
    explicit: Option<String>,
    from_stdin: bool,
    from_prompt: bool,
    prompt: &str,
) -> Result<String> {
    let source_count =
        usize::from(explicit.is_some()) + usize::from(from_stdin) + usize::from(from_prompt);
    if source_count != 1 {
        bail!(
            "select exactly one credential source: --credential, --credential-stdin, or --credential-prompt"
        );
    }
    let credential = if let Some(value) = explicit {
        value
    } else if from_stdin {
        let mut value = String::new();
        std::io::stdin()
            .read_to_string(&mut value)
            .context("failed to read credential from stdin")?;
        value
    } else {
        if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
            bail!("credential prompt requires an interactive terminal");
        }
        prompt_secret_value(prompt)?
    };
    let normalized = credential.trim().to_owned();
    if normalized.is_empty() {
        bail!("credential input is empty");
    }
    Ok(normalized)
}

pub(super) fn resolve_connector_selector(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
) -> Result<String> {
    match resolve_optional_connector_selector(connector_id, provider, account_id)? {
        Some(connector_id) => Ok(connector_id),
        None => bail!("connector selector requires connector_id or --provider [--account-id]"),
    }
}

pub(super) fn resolve_optional_connector_selector(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
) -> Result<Option<String>> {
    match (connector_id, provider) {
        (Some(connector_id), None) | (Some(connector_id), Some(_)) => {
            normalize_required_text_arg(connector_id, "connector_id").map(Some)
        }
        (None, Some(provider)) => {
            let account_id = account_id.unwrap_or_else(|| "default".to_owned());
            super::providers::connector_id_for_provider(provider, account_id.as_str()).map(Some)
        }
        (None, None) if account_id.is_some() => {
            bail!("--account-id requires --provider when connector_id is omitted")
        }
        (None, None) => Ok(None),
    }
}

pub(super) fn normalize_generic_account_id(raw: &str, label: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        bail!("{label} cannot be empty");
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':' | '@'))
    {
        bail!("{label} contains unsupported characters");
    }
    Ok(value.to_ascii_lowercase())
}

pub(super) fn unsupported_provider_action(
    surface: &str,
    action: &str,
    provider: ChannelProviderArg,
    connector_id: Option<&str>,
    json_output: bool,
    reason: &str,
) -> Result<()> {
    let payload = json!({
        "surface": surface,
        "action": action,
        "provider": provider_label(provider),
        "connector_id": connector_id,
        "supported": false,
        "reason": reason,
        "supported_providers": ["discord"],
    });
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .context("failed to encode unsupported provider payload as JSON")?
        );
        Ok(())
    } else {
        bail!(
            "unsupported {} action '{}' for provider '{}': {}",
            surface,
            action,
            provider_label(provider),
            reason
        )
    }
}

pub(super) fn normalize_string_list(values: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();
    for value in values {
        if let Some(value) = normalize_optional_text_arg(value) {
            let lowered = value.to_ascii_lowercase();
            if !normalized.iter().any(|existing| existing == &lowered) {
                normalized.push(lowered);
            }
        }
    }
    normalized
}

pub(super) fn provider_label(provider: ChannelProviderArg) -> &'static str {
    super::providers::label(provider)
}

pub(super) fn resolve_entity_label(entity: ChannelResolveEntityArg) -> &'static str {
    match entity {
        ChannelResolveEntityArg::Channel => "channel",
        ChannelResolveEntityArg::Conversation => "conversation",
        ChannelResolveEntityArg::Thread => "thread",
        ChannelResolveEntityArg::User => "user",
    }
}

pub(crate) fn resolve_connector_status(
    connector_id: &str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    error_context: &'static str,
) -> Result<Value> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/{}",
        request_context.base_url.trim_end_matches('/'),
        connector_id
    );
    let client = channels_client::build_client()?;
    channels_client::send_request(client.get(endpoint), request_context, error_context)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn post_connector_action(
    connector_id: &str,
    action_suffix: &str,
    payload: Option<Value>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    error_context: &'static str,
) -> Result<Value> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/{}{}",
        request_context.base_url.trim_end_matches('/'),
        connector_id,
        action_suffix
    );
    let client = channels_client::build_client()?;
    let request = if let Some(payload) = payload {
        client.post(endpoint).json(&payload)
    } else {
        client.post(endpoint)
    };
    channels_client::send_request(request, request_context, error_context)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn post_discord_account_action(
    account_id: &str,
    action: &str,
    payload: Value,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    error_context: &'static str,
) -> Result<Value> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/discord/accounts/{}",
        request_context.base_url.trim_end_matches('/'),
        action
    );
    let mut payload = match payload {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    payload.insert("account_id".to_owned(), Value::String(account_id.to_owned()));
    let client = channels_client::build_client()?;
    channels_client::send_request(
        client.post(endpoint).json(&Value::Object(payload)),
        request_context,
        error_context,
    )
}

#[cfg(test)]
mod tests {
    use super::{resolve_connector_selector, resolve_optional_connector_selector};
    use crate::args::ChannelProviderArg;

    #[test]
    fn optional_connector_selector_accepts_global_selection() {
        let selector = resolve_optional_connector_selector(None, None, None)
            .expect("global channel selection should be accepted");

        assert_eq!(selector, None);
    }

    #[test]
    fn connector_selector_still_requires_an_explicit_selector() {
        let error = resolve_connector_selector(None, None, None)
            .expect_err("selector-only surfaces should keep rejecting global selection");

        assert!(
            error.to_string().contains("connector selector requires"),
            "error should keep selector guidance: {error}"
        );
    }

    #[test]
    fn optional_connector_selector_rejects_account_id_without_provider() {
        let error = resolve_optional_connector_selector(None, None, Some("default".to_owned()))
            .expect_err("--account-id alone is ambiguous");

        assert!(
            error.to_string().contains("--provider"),
            "error should point to the missing provider flag: {error}"
        );
    }

    #[test]
    fn optional_connector_selector_resolves_provider_account_pairs() {
        let selector = resolve_optional_connector_selector(
            None,
            Some(ChannelProviderArg::Discord),
            Some("ops".to_owned()),
        )
        .expect("provider selector should resolve")
        .expect("provider selector should produce connector id");

        assert_eq!(selector, "discord:ops");
    }
}
