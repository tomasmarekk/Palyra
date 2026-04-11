use palyra_connector_core::{ConnectorInstanceSpec, ConnectorKind};

use crate::{
    discord_auth_profile_ref, discord_connector_id, discord_principal, discord_token_vault_ref,
    normalize_discord_account_id, DiscordSemanticsError,
};

const DISCORD_DEFAULT_EGRESS_ALLOWLIST: [&str; 8] = [
    "discord.com",
    "*.discord.com",
    "discordapp.com",
    "*.discordapp.com",
    "discord.gg",
    "*.discord.gg",
    "discordapp.net",
    "*.discordapp.net",
];

#[must_use]
pub fn discord_default_egress_allowlist() -> Vec<String> {
    DISCORD_DEFAULT_EGRESS_ALLOWLIST.iter().map(|entry| (*entry).to_owned()).collect()
}

pub fn discord_connector_spec(
    account_id: &str,
    enabled: bool,
) -> Result<ConnectorInstanceSpec, DiscordSemanticsError> {
    let normalized = normalize_discord_account_id(account_id)?;
    Ok(ConnectorInstanceSpec {
        connector_id: discord_connector_id(normalized.as_str()),
        kind: ConnectorKind::Discord,
        principal: discord_principal(normalized.as_str()),
        auth_profile_ref: Some(discord_auth_profile_ref(normalized.as_str())),
        token_vault_ref: Some(discord_token_vault_ref(normalized.as_str())),
        egress_allowlist: discord_default_egress_allowlist(),
        enabled,
    })
}

#[cfg(test)]
mod tests {
    use palyra_connector_core::ConnectorKind;

    use super::{discord_connector_spec, discord_default_egress_allowlist};

    #[test]
    fn default_egress_allowlist_keeps_discord_domains_explicit() {
        let allowlist = discord_default_egress_allowlist();
        assert_eq!(allowlist.len(), 8, "baseline allowlist should remain stable");
        assert!(allowlist.iter().any(|entry| entry == "discord.com"));
        assert!(allowlist.iter().any(|entry| entry == "*.discord.com"));
        assert!(allowlist.iter().any(|entry| entry == "discordapp.net"));
    }

    #[test]
    fn connector_spec_normalizes_account_identity_and_wiring() {
        let spec = discord_connector_spec(" Ops ", true).expect("spec should build");
        assert_eq!(spec.connector_id, "discord:ops");
        assert_eq!(spec.kind, ConnectorKind::Discord);
        assert_eq!(spec.principal, "channel:discord:ops");
        assert_eq!(spec.auth_profile_ref.as_deref(), Some("discord.ops"));
        assert_eq!(spec.token_vault_ref.as_deref(), Some("global/discord_bot_token.ops"));
        assert!(spec.enabled);
        assert_eq!(spec.egress_allowlist, discord_default_egress_allowlist());
    }
}
