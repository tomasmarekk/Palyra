pub mod connectors;
pub mod core;
pub mod providers;

pub use crate::core::*;

#[cfg(test)]
mod tests {
    use super::{
        core::ConnectorKind,
        providers::discord::{
            discord_connector_spec, discord_policy_action_for_operation, DiscordMessageOperation,
        },
    };

    #[test]
    fn curated_surface_exposes_core_and_discord_provider_modules() {
        let spec = discord_connector_spec("ops", true).expect("spec should build");

        assert_eq!(spec.kind, ConnectorKind::Discord);
        assert_eq!(
            discord_policy_action_for_operation(DiscordMessageOperation::Delete),
            "channel.message.delete"
        );
    }
}
