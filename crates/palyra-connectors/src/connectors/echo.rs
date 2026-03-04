use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Mutex,
};

use async_trait::async_trait;
use serde_json::json;

use crate::{
    protocol::{ConnectorKind, DeliveryOutcome, OutboundMessageRequest, RetryClass},
    storage::ConnectorInstanceRecord,
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};

const CRASH_ONCE_MARKER: &str = "[connector-crash-once]";

#[derive(Debug, Default)]
pub struct EchoConnectorAdapter {
    delivered_native_ids: Mutex<HashMap<String, String>>,
    crash_once_seen: Mutex<HashSet<String>>,
}

impl EchoConnectorAdapter {
    #[must_use]
    pub fn delivery_count(&self) -> usize {
        self.delivered_native_ids.lock().map(|guard| guard.len()).unwrap_or_default()
    }
}

#[async_trait]
impl ConnectorAdapter for EchoConnectorAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Echo
    }

    async fn send_outbound(
        &self,
        _instance: &ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        if request.text.contains(CRASH_ONCE_MARKER) {
            let mut seen = self.crash_once_seen.lock().map_err(|_| {
                ConnectorAdapterError::Backend(
                    "echo connector crash marker state lock poisoned".to_owned(),
                )
            })?;
            if seen.insert(request.envelope_id.clone()) {
                return Ok(DeliveryOutcome::Retry {
                    class: RetryClass::ConnectorRestarting,
                    reason: "simulated connector restart during send".to_owned(),
                    retry_after_ms: Some(10),
                });
            }
        }

        let mut delivered = self.delivered_native_ids.lock().map_err(|_| {
            ConnectorAdapterError::Backend(
                "echo connector idempotency map lock poisoned".to_owned(),
            )
        })?;
        if let Some(existing_id) = delivered.get(request.envelope_id.as_str()) {
            return Ok(DeliveryOutcome::Delivered { native_message_id: existing_id.clone() });
        }

        let native_message_id = format!(
            "echo-{:016x}",
            deterministic_hash(json!({
                "envelope_id": request.envelope_id,
                "conversation_id": request.conversation_id,
                "text": request.text,
                "thread_id": request.reply_thread_id,
            }))
        );
        delivered.insert(request.envelope_id.clone(), native_message_id.clone());
        Ok(DeliveryOutcome::Delivered { native_message_id })
    }
}

fn deterministic_hash<T: Hash>(value: T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::EchoConnectorAdapter;
    use crate::{
        protocol::{
            ConnectorLiveness, ConnectorReadiness, DeliveryOutcome, OutboundMessageRequest,
            RetryClass,
        },
        storage::ConnectorInstanceRecord,
        supervisor::ConnectorAdapter,
    };

    fn instance() -> ConnectorInstanceRecord {
        ConnectorInstanceRecord {
            connector_id: "echo:default".to_owned(),
            kind: crate::protocol::ConnectorKind::Echo,
            principal: "channel:echo:default".to_owned(),
            auth_profile_ref: None,
            token_vault_ref: None,
            egress_allowlist: Vec::new(),
            enabled: true,
            readiness: ConnectorReadiness::Ready,
            liveness: ConnectorLiveness::Running,
            restart_count: 0,
            last_error: None,
            last_inbound_unix_ms: None,
            last_outbound_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    fn request(text: &str) -> OutboundMessageRequest {
        OutboundMessageRequest {
            envelope_id: "env-1".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "conv-1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: text.to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 30_000,
            max_payload_bytes: 16_384,
        }
    }

    #[tokio::test]
    async fn preserves_idempotency_per_outbound_envelope() {
        let adapter = EchoConnectorAdapter::default();
        let first = adapter
            .send_outbound(&instance(), &request("ok"))
            .await
            .expect("first send should pass");
        let second = adapter
            .send_outbound(&instance(), &request("ok"))
            .await
            .expect("second send should also pass");
        let DeliveryOutcome::Delivered { native_message_id: first_id } = first else {
            panic!("first result should be delivered");
        };
        let DeliveryOutcome::Delivered { native_message_id: second_id } = second else {
            panic!("second result should be delivered");
        };
        assert_eq!(first_id, second_id);
        assert_eq!(adapter.delivery_count(), 1);
    }

    #[tokio::test]
    async fn simulates_restart_once_when_marker_present() {
        let adapter = EchoConnectorAdapter::default();
        let first = adapter
            .send_outbound(&instance(), &request("hello [connector-crash-once]"))
            .await
            .expect("first send should return retry");
        let second = adapter
            .send_outbound(&instance(), &request("hello [connector-crash-once]"))
            .await
            .expect("second send should recover");

        let DeliveryOutcome::Retry { class, .. } = first else {
            panic!("first send should request retry");
        };
        assert_eq!(class, RetryClass::ConnectorRestarting);
        assert!(matches!(second, DeliveryOutcome::Delivered { .. }));
    }
}
