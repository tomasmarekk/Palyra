use super::DiscordSemanticsError;

#[must_use]
pub fn discord_connector_id(account_id: &str) -> String {
    format!("discord:{}", account_id.trim().to_ascii_lowercase())
}

#[must_use]
pub fn discord_principal(account_id: &str) -> String {
    format!("channel:{}", discord_connector_id(account_id))
}

#[must_use]
pub fn discord_token_vault_ref(account_id: &str) -> String {
    let normalized = account_id.trim().to_ascii_lowercase();
    if normalized == "default" {
        return "global/discord_bot_token".to_owned();
    }
    format!("global/discord_bot_token.{normalized}")
}

#[must_use]
pub fn discord_auth_profile_ref(account_id: &str) -> String {
    format!("discord.{}", account_id.trim().to_ascii_lowercase())
}

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
