use async_trait::async_trait;

use crate::{
    protocol::{ConnectorKind, DeliveryOutcome, OutboundMessageRequest},
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};

#[derive(Debug, Default)]
pub struct TelegramConnectorAdapter;

#[async_trait]
impl ConnectorAdapter for TelegramConnectorAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Telegram
    }

    async fn send_outbound(
        &self,
        _request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        Ok(DeliveryOutcome::PermanentFailure {
            reason: "telegram connector is deferred in roadmap and unavailable in M40".to_owned(),
        })
    }
}
