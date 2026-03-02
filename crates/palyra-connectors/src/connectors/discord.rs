use async_trait::async_trait;

use crate::{
    protocol::{ConnectorKind, DeliveryOutcome, OutboundMessageRequest},
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};

#[derive(Debug, Default)]
pub struct DiscordConnectorAdapter;

#[async_trait]
impl ConnectorAdapter for DiscordConnectorAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Discord
    }

    async fn send_outbound(
        &self,
        _request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        Ok(DeliveryOutcome::PermanentFailure {
            reason: "discord connector runtime ships in milestone M42".to_owned(),
        })
    }
}
