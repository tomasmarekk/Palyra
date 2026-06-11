//! Shared helpers for channel CLI commands: credential intake, connector
//! selector resolution, and admin-endpoint transport wrappers.
//!
//! Provider-neutral by design; provider-specific behavior stays in the
//! sibling `providers` and `connectors` modules.

use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value};
use std::io::{IsTerminal, Read};

use crate::{
    args::{ChannelProviderArg, ChannelResolveEntityArg},
    client::channels as channels_client,
    normalize_optional_text_arg, normalize_required_text_arg, prompt_secret_value,
};

/// Resolves a channel credential from exactly one of argv, stdin, or an
/// interactive prompt, returning the trimmed non-empty value.
///
/// Passing the credential on argv is refused unless the caller explicitly
/// acknowledged the risk, because command-line arguments leak through
/// process lists.
///
/// # Errors
/// Fails when zero or multiple sources are selected, when the argv source
/// lacks the insecure-arg acknowledgement, when the prompt source runs
/// without a TTY, when stdin cannot be read, or when the input is empty.
pub(super) fn load_channel_credential(
    explicit: Option<String>,
    from_stdin: bool,
    from_prompt: bool,
    allow_insecure_credential_arg: bool,
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
        if !allow_insecure_credential_arg {
            bail!(
                "refusing --credential without --allow-insecure-credential-arg because command-line arguments can be exposed through process lists; use --credential-stdin or --credential-prompt instead"
            );
        }
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

/// Resolves a connector id like [`resolve_optional_connector_selector`] but
/// rejects the all-connectors (no selector) case.
///
/// # Errors
/// Fails when no selector is provided or when the selector input is invalid.
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

/// Resolves an explicit connector id or a `--provider`/`--account-id` pair to
/// a connector id; `Ok(None)` means the caller selected all connectors.
///
/// An explicit connector id wins over a provider selector when both are set.
///
/// # Errors
/// Fails when the connector id is blank, when `--account-id` is given without
/// `--provider`, or when provider-specific account-id normalization rejects
/// the value.
pub(super) fn resolve_optional_connector_selector(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
) -> Result<Option<String>> {
    match (connector_id, provider) {
        (Some(connector_id), _) => {
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

/// Validates and lowercases an account id for providers without their own
/// normalization rules.
///
/// # Errors
/// Fails when the value is empty or contains characters outside the
/// alphanumeric and `-_.:@` set.
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

/// Reports that a provider does not implement the requested action.
///
/// JSON mode emits a structured `supported: false` payload and succeeds so
/// scripted callers can branch on the field; text mode fails the command.
///
/// # Errors
/// Fails in text mode (by design) and when JSON encoding fails.
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

/// Trims, lowercases, and deduplicates list arguments, dropping blanks while
/// preserving first-seen order.
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

/// Returns the canonical lowercase label for a channel provider.
pub(super) fn provider_label(provider: ChannelProviderArg) -> &'static str {
    super::providers::label(provider)
}

/// Returns the canonical lowercase label for a resolvable channel entity.
pub(super) fn resolve_entity_label(entity: ChannelResolveEntityArg) -> &'static str {
    match entity {
        ChannelResolveEntityArg::Channel => "channel",
        ChannelResolveEntityArg::Conversation => "conversation",
        ChannelResolveEntityArg::Thread => "thread",
        ChannelResolveEntityArg::User => "user",
    }
}

/// Fetches the daemon-side status document for a single connector.
///
/// # Errors
/// Fails when the request context cannot be resolved or the admin endpoint
/// call fails.
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

/// Posts a connector-scoped admin action (for example queue or health
/// operations) and returns the response document.
///
/// # Errors
/// Fails when the request context cannot be resolved or the admin endpoint
/// call fails.
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

/// Posts a Discord account-level action, injecting `account_id` into the
/// payload so callers only supply action-specific fields.
///
/// # Errors
/// Fails when the request context cannot be resolved or the admin endpoint
/// call fails.
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
    use super::{
        load_channel_credential, resolve_connector_selector, resolve_optional_connector_selector,
    };
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

    #[test]
    fn argv_channel_credential_requires_insecure_acknowledgement() {
        let error = load_channel_credential(
            Some("discord-token".to_owned()),
            false,
            false,
            false,
            "Discord bot token: ",
        )
        .expect_err("argv credential must require explicit acknowledgement");

        assert!(
            error.to_string().contains("--allow-insecure-credential-arg"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn acknowledged_argv_channel_credential_is_trimmed() {
        let credential = load_channel_credential(
            Some(" discord-token \n".to_owned()),
            false,
            false,
            true,
            "Discord bot token: ",
        )
        .expect("acknowledged argv credential should resolve");

        assert_eq!(credential, "discord-token");
    }
}
