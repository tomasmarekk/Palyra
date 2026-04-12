use std::sync::Arc;

use crate::core::ConnectorAdapter;

pub mod discord;
mod echo;
mod slack;
mod telegram;

pub use echo::EchoConnectorAdapter;
pub use slack::SlackConnectorAdapter;
pub use telegram::TelegramConnectorAdapter;

#[must_use]
pub fn default_adapters() -> Vec<Arc<dyn ConnectorAdapter>> {
    vec![
        Arc::new(EchoConnectorAdapter::default()),
        Arc::new(discord::DiscordConnectorAdapter::default()),
    ]
}

#[cfg(test)]
mod tests {
    use crate::protocol::{ConnectorAvailability, ConnectorKind};

    use super::default_adapters;

    #[test]
    fn provider_registry_contains_supported_provider_adapters() {
        let adapters = default_adapters();
        let runtime_surface = adapters
            .iter()
            .map(|adapter| (adapter.kind(), adapter.availability()))
            .collect::<Vec<_>>();

        assert_eq!(
            runtime_surface,
            vec![
                (ConnectorKind::Echo, ConnectorAvailability::InternalTestOnly),
                (ConnectorKind::Discord, ConnectorAvailability::Supported),
            ]
        );
    }
}
