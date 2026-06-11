//! Canonical identifier derivation for Discord connector instances.
//!
//! Every wiring reference (connector id, principal, vault ref, auth profile ref) is derived
//! from one normalized account id so the daemon, vault, and auth registry stay consistent.

use super::DiscordSemanticsError;

/// Derives the canonical connector id (`discord:<account>`) for an account id.
#[must_use]
pub fn discord_connector_id(account_id: &str) -> String {
    format!("discord:{}", account_id.trim().to_ascii_lowercase())
}

/// Derives the channel principal (`channel:discord:<account>`) for an account id.
#[must_use]
pub fn discord_principal(account_id: &str) -> String {
    format!("channel:{}", discord_connector_id(account_id))
}

/// Derives the vault reference holding the bot token for an account id.
///
/// The `default` account maps to the unsuffixed reference for backward compatibility with
/// single-account deployments.
#[must_use]
pub fn discord_token_vault_ref(account_id: &str) -> String {
    let normalized = account_id.trim().to_ascii_lowercase();
    if normalized == "default" {
        return "global/discord_bot_token".to_owned();
    }
    format!("global/discord_bot_token.{normalized}")
}

/// Derives the auth profile reference (`discord.<account>`) for an account id.
#[must_use]
pub fn discord_auth_profile_ref(account_id: &str) -> String {
    format!("discord.{}", account_id.trim().to_ascii_lowercase())
}

/// Trims and lowercases an operator-supplied account id, enforcing the supported character set
/// (ASCII alphanumerics plus `.`, `_`, `-`).
///
/// # Errors
/// Returns [`DiscordSemanticsError::EmptyAccountId`] for blank input and
/// [`DiscordSemanticsError::InvalidAccountId`] when unsupported characters are present.
pub fn normalize_discord_account_id(raw: &str) -> Result<String, DiscordSemanticsError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(DiscordSemanticsError::EmptyAccountId);
    }
    if !trimmed.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')) {
        return Err(DiscordSemanticsError::InvalidAccountId);
    }
    Ok(trimmed.to_ascii_lowercase())
}
