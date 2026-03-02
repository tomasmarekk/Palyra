mod discord;
mod echo;
mod slack;
mod telegram;

use std::sync::Arc;

use crate::supervisor::ConnectorAdapter;

pub use discord::DiscordConnectorAdapter;
pub use echo::EchoConnectorAdapter;
pub use slack::SlackConnectorAdapter;
pub use telegram::TelegramConnectorAdapter;

#[must_use]
pub fn default_adapters() -> Vec<Arc<dyn ConnectorAdapter>> {
    vec![
        Arc::new(EchoConnectorAdapter::default()),
        Arc::new(DiscordConnectorAdapter::default()),
        Arc::new(SlackConnectorAdapter),
        Arc::new(TelegramConnectorAdapter),
    ]
}
