use async_trait::async_trait;

use crate::{
    protocol::{ConnectorAvailability, ConnectorKind, DeliveryOutcome, OutboundMessageRequest},
    storage::ConnectorInstanceRecord,
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};

#[derive(Debug, Default)]
pub struct SlackConnectorAdapter;

#[async_trait]
impl ConnectorAdapter for SlackConnectorAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Slack
    }

    fn availability(&self) -> ConnectorAvailability {
        ConnectorAvailability::Deferred
    }

    async fn send_outbound(
        &self,
        _instance: &ConnectorInstanceRecord,
        _request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        Ok(DeliveryOutcome::PermanentFailure {
            reason: "slack connector is deferred in roadmap and unavailable in M40".to_owned(),
        })
    }
}
