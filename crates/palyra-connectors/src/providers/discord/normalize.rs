use super::DiscordSemanticsError;

#[must_use]
pub fn is_discord_connector(connector_id: &str) -> bool {
    connector_id.trim().to_ascii_lowercase().starts_with("discord:")
}

#[must_use]
pub fn canonical_discord_sender_identity(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "discord:user:unknown".to_owned();
    }
    let normalized = trimmed
        .strip_prefix("discord:user:")
        .or_else(|| trimmed.strip_prefix("user:"))
        .map(str::trim)
        .or_else(|| parse_discord_user_mention(trimmed))
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    format!("discord:user:{normalized}")
}

#[must_use]
pub fn canonical_discord_channel_identity(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "discord:channel:unknown".to_owned();
    }
    let normalized = trimmed
        .strip_prefix("discord:channel:")
        .or_else(|| trimmed.strip_prefix("channel:"))
        .or_else(|| trimmed.strip_prefix("thread:"))
        .map(str::trim)
        .or_else(|| parse_discord_channel_mention(trimmed))
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    format!("discord:channel:{normalized}")
}

pub fn normalize_discord_target(raw: &str) -> Result<String, DiscordSemanticsError> {
    let trimmed = raw.trim();
    let normalized = trimmed
        .strip_prefix("channel:")
        .or_else(|| trimmed.strip_prefix("thread:"))
        .map(str::trim)
        .unwrap_or(trimmed);
    if normalized.is_empty() {
        return Err(DiscordSemanticsError::EmptyTarget);
    }
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '.'))
    {
        return Err(DiscordSemanticsError::InvalidTarget);
    }
    Ok(normalized.to_owned())
}

fn parse_discord_user_mention(raw: &str) -> Option<&str> {
    let body = raw.strip_prefix("<@")?.strip_suffix('>')?;
    body.strip_prefix('!').or(Some(body))
}

fn parse_discord_channel_mention(raw: &str) -> Option<&str> {
    raw.strip_prefix("<#")?.strip_suffix('>')
}

#[cfg(test)]
mod tests {
    use super::super::DiscordSemanticsError;
    use super::{
        canonical_discord_channel_identity, canonical_discord_sender_identity,
        is_discord_connector, normalize_discord_target,
    };

    #[test]
    fn canonical_sender_identity_normalizes_prefixes_and_mentions() {
        assert_eq!(
            canonical_discord_sender_identity("discord:user:Ops-Bot"),
            "discord:user:ops-bot"
        );
        assert_eq!(canonical_discord_sender_identity(" user:MixedCase "), "discord:user:mixedcase");
        assert_eq!(canonical_discord_sender_identity("<@!1234567890>"), "discord:user:1234567890");
        assert_eq!(canonical_discord_sender_identity("   "), "discord:user:unknown");
    }

    #[test]
    fn canonical_channel_identity_normalizes_prefixes_and_mentions() {
        assert_eq!(
            canonical_discord_channel_identity("discord:channel:Ops-Room"),
            "discord:channel:ops-room"
        );
        assert_eq!(
            canonical_discord_channel_identity(" thread:Thread-42 "),
            "discord:channel:thread-42"
        );
        assert_eq!(
            canonical_discord_channel_identity("<#1234567890>"),
            "discord:channel:1234567890"
        );
        assert_eq!(canonical_discord_channel_identity(""), "discord:channel:unknown");
    }

    #[test]
    fn normalize_target_accepts_canonical_channel_shapes() {
        assert_eq!(
            normalize_discord_target("channel:ops-room").expect("channel prefix should normalize"),
            "ops-room"
        );
        assert_eq!(
            normalize_discord_target(" thread:thread_42 ").expect("thread prefix should normalize"),
            "thread_42"
        );
        assert_eq!(
            normalize_discord_target("1234567890").expect("snowflake id should pass through"),
            "1234567890"
        );
    }

    #[test]
    fn normalize_target_rejects_blank_and_invalid_characters() {
        assert_eq!(
            normalize_discord_target("   ").expect_err("blank target should fail"),
            DiscordSemanticsError::EmptyTarget
        );
        assert_eq!(
            normalize_discord_target("bad target!")
                .expect_err("punctuation outside allowlist should fail"),
            DiscordSemanticsError::InvalidTarget
        );
    }

    #[test]
    fn connector_detection_is_case_insensitive_for_discord_prefix() {
        assert!(is_discord_connector("Discord:default"));
        assert!(is_discord_connector(" discord:ops "));
        assert!(!is_discord_connector("slack:default"));
    }
}
