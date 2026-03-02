use async_trait::async_trait;

use crate::{
    protocol::{ConnectorKind, DeliveryOutcome, OutboundMessageRequest},
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};

#[derive(Debug, Default)]
pub struct SlackConnectorAdapter;

#[async_trait]
impl ConnectorAdapter for SlackConnectorAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Slack
    }

    async fn send_outbound(
        &self,
        _request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        Ok(DeliveryOutcome::PermanentFailure {
            reason: "slack connector is deferred in roadmap and unavailable in M40".to_owned(),
        })
    }
}
