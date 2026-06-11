//! Slack connector stub for the deferred-provider roadmap slot.
//!
//! Publishes deferred capability metadata so the registry and console can describe Slack
//! truthfully, while every outbound send permanently fails until a real adapter ships.

use async_trait::async_trait;

use crate::{
    protocol::{
        ConnectorAvailability, ConnectorCapabilitySet, ConnectorCapabilitySupport, ConnectorKind,
        ConnectorMessageCapabilitySet, DeliveryOutcome, OutboundMessageRequest,
    },
    storage::ConnectorInstanceRecord,
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};

use super::ConnectorProviderDescriptor;

/// Placeholder Slack adapter; reports all message capabilities as unsupported.
#[derive(Debug, Default)]
pub struct SlackConnectorAdapter;

/// Registry hook publishing the deferred Slack provider's runtime metadata.
pub(crate) fn provider_descriptor() -> ConnectorProviderDescriptor {
    let reason =
        "slack connector is deferred in the roadmap and unavailable in the current runtime";
    ConnectorProviderDescriptor {
        kind: ConnectorKind::Slack,
        availability: ConnectorAvailability::Deferred,
        capabilities: deferred_capabilities(reason),
        default_instance_spec: None,
    }
}

fn deferred_capabilities(reason: &str) -> ConnectorCapabilitySet {
    ConnectorCapabilitySet {
        lifecycle: ConnectorCapabilitySupport::unsupported(reason),
        status: ConnectorCapabilitySupport::supported(),
        logs: ConnectorCapabilitySupport::supported(),
        health_refresh: ConnectorCapabilitySupport::unsupported(reason),
        resolve: ConnectorCapabilitySupport::unsupported(reason),
        pairings: ConnectorCapabilitySupport::unsupported(reason),
        qr: ConnectorCapabilitySupport::unsupported(reason),
        webhook_ingress: ConnectorCapabilitySupport::unsupported(reason),
        message: ConnectorMessageCapabilitySet {
            send: ConnectorCapabilitySupport::unsupported(reason),
            thread: ConnectorCapabilitySupport::unsupported(reason),
            reply: ConnectorCapabilitySupport::unsupported(reason),
            read: ConnectorCapabilitySupport::unsupported(reason),
            search: ConnectorCapabilitySupport::unsupported(reason),
            edit: ConnectorCapabilitySupport::unsupported(reason),
            delete: ConnectorCapabilitySupport::unsupported(reason),
            react_add: ConnectorCapabilitySupport::unsupported(reason),
            react_remove: ConnectorCapabilitySupport::unsupported(reason),
        },
    }
}

#[async_trait]
impl ConnectorAdapter for SlackConnectorAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Slack
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
