//! Provider-neutral connector message protocol: envelopes, capabilities,
//! native commands, delivery receipts, and message read/mutation contracts.
//!
//! These types are serde wire shapes shared with the daemon and console APIs;
//! field names and enum casing are part of persisted/transport contracts and
//! must not change. Size limits live in the private `validation` module.

// NOTE: `validation::ProtocolError` is returned by the public
// `validate()` methods below but is not re-exported, so downstream crates can
// only Display it, not match on it. Re-exporting it is a deliberate
// curated-surface decision; do not widen the surface as a drive-by change.
mod attachments;
mod capabilities;
mod commands;
mod delivery;
mod kinds;
mod messages;
mod validation;

pub use attachments::{AttachmentKind, AttachmentRef, OutboundA2uiUpdate, OutboundAttachment};
pub use capabilities::{
    ConnectorApprovalMode, ConnectorCapabilitySet, ConnectorCapabilitySupport,
    ConnectorMessageCapabilitySet, ConnectorOperationPreflight, ConnectorRiskLevel,
};
pub use commands::{
    ChannelCommandArgumentKind, ChannelCommandSyncState, ChannelCommandSyncStatus,
    ChannelNativeCommandArgument, ChannelNativeCommandInvocationPayload, ChannelNativeCommandSpec,
};
pub use delivery::{
    ConnectorInstanceSpec, ConnectorQueueDepth, ConnectorStatusSnapshot, DeliveryOutcome,
    DeliveryReceipt, DeliveryReceiptState, InboundMessageEvent, OutboundMessageRequest, RetryClass,
    RouteInboundResult, RoutedOutboundMessage,
};
pub use kinds::{ConnectorAvailability, ConnectorKind, ConnectorLiveness, ConnectorReadiness};
pub use messages::{
    ConnectorConversationTarget, ConnectorMessageDeleteRequest, ConnectorMessageEditRequest,
    ConnectorMessageLocator, ConnectorMessageMutationDiff, ConnectorMessageMutationResult,
    ConnectorMessageMutationStatus, ConnectorMessageReactionRecord,
    ConnectorMessageReactionRequest, ConnectorMessageReadRequest, ConnectorMessageReadResult,
    ConnectorMessageRecord, ConnectorMessageSearchRequest, ConnectorMessageSearchResult,
};

#[cfg(test)]
mod tests {
    use super::validation::ProtocolError;
    use super::{
        AttachmentKind, AttachmentRef, ConnectorInstanceSpec, ConnectorKind, DeliveryOutcome,
        DeliveryReceiptState, InboundMessageEvent, OutboundA2uiUpdate, OutboundMessageRequest,
        RetryClass,
    };

    #[test]
    fn connector_spec_validation_rejects_malformed_allowlist() {
        let spec = ConnectorInstanceSpec {
            connector_id: "echo:default".to_owned(),
            kind: ConnectorKind::Echo,
            principal: "channel:echo:default".to_owned(),
            auth_profile_ref: None,
            token_vault_ref: None,
            egress_allowlist: vec!["bad host".to_owned()],
            enabled: true,
        };
        assert_eq!(
            spec.validate(),
            Err(ProtocolError::InvalidField {
                field: "egress_allowlist",
                reason: "host pattern contains unsupported characters",
            })
        );
    }

    #[test]
    fn inbound_validation_rejects_empty_payload() {
        let event = InboundMessageEvent {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            thread_id: None,
            sender_id: "sender".to_owned(),
            sender_display: None,
            body: "   ".to_owned(),
            adapter_message_id: None,
            adapter_thread_id: None,
            received_at_unix_ms: 1,
            is_direct_message: true,
            requested_broadcast: false,
            attachments: Vec::new(),
        };
        assert_eq!(
            event.validate(1024),
            Err(ProtocolError::InvalidField { field: "body", reason: "cannot be empty" })
        );
    }

    #[test]
    fn outbound_validation_requires_timeout_and_payload_limit() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 0,
            max_payload_bytes: 0,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "timeout_ms",
                reason: "must be greater than zero"
            })
        );
    }

    #[test]
    fn inbound_validation_rejects_excessive_attachment_count() {
        let mut event = InboundMessageEvent {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            thread_id: None,
            sender_id: "sender".to_owned(),
            sender_display: None,
            body: "hello".to_owned(),
            adapter_message_id: None,
            adapter_thread_id: None,
            received_at_unix_ms: 1,
            is_direct_message: true,
            requested_broadcast: false,
            attachments: Vec::new(),
        };
        event.attachments = (0..33)
            .map(|index| AttachmentRef {
                kind: AttachmentKind::Image,
                url: Some(format!("https://cdn.example.test/{index}.png")),
                filename: Some(format!("{index}.png")),
                content_type: Some("image/png".to_owned()),
                size_bytes: Some(1_024),
                ..AttachmentRef::default()
            })
            .collect();
        assert_eq!(
            event.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "attachments",
                reason: "message exceeds attachment count limit",
            })
        );
    }

    #[test]
    fn outbound_validation_rejects_invalid_structured_json() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: Some(br#"{"missing":"brace""#.to_vec()),
            a2ui_update: None,
            timeout_ms: 1_000,
            max_payload_bytes: 8_192,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "structured_json",
                reason: "value is not valid JSON",
            })
        );
    }

    #[test]
    fn outbound_validation_rejects_structured_json_over_payload_limit() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: Some(br#"{"k":"0123456789"}"#.to_vec()),
            a2ui_update: None,
            timeout_ms: 1_000,
            max_payload_bytes: 8,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "structured_json",
                reason: "value exceeds size limit",
            })
        );
    }

    #[test]
    fn outbound_validation_rejects_a2ui_patch_over_payload_limit() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: Some(OutboundA2uiUpdate {
                surface: "chat".to_owned(),
                patch_json: br#"{"op":"replace","path":"/title","value":"hello"}"#.to_vec(),
            }),
            timeout_ms: 1_000,
            max_payload_bytes: 16,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "a2ui_update.patch_json",
                reason: "value exceeds size limit",
            })
        );
    }

    #[test]
    fn outbound_validation_rejects_invalid_a2ui_patch_json() {
        let request = OutboundMessageRequest {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: Some(OutboundA2uiUpdate {
                surface: "chat".to_owned(),
                patch_json: br#"{"oops":"invalid""#.to_vec(),
            }),
            timeout_ms: 1_000,
            max_payload_bytes: 8_192,
        };
        assert_eq!(
            request.validate(1024),
            Err(ProtocolError::InvalidField {
                field: "a2ui_update.patch_json",
                reason: "value is not valid JSON",
            })
        );
    }

    #[test]
    fn delivery_outcomes_convert_to_ack_nack_unknown_receipts() {
        let request = valid_outbound_request();

        let ack = DeliveryOutcome::Delivered { native_message_id: "native-1".to_owned() }
            .to_receipt(&request);
        assert_eq!(ack.state, DeliveryReceiptState::Ack);
        assert_eq!(ack.external_message_id.as_deref(), Some("native-1"));
        assert_eq!(ack.idempotency_key, "echo:default:env-1:0");

        let unknown = DeliveryOutcome::Retry {
            class: RetryClass::RateLimit,
            reason: "retry later".to_owned(),
            retry_after_ms: Some(250),
        }
        .to_receipt(&request);
        assert_eq!(unknown.state, DeliveryReceiptState::Unknown);
        assert_eq!(unknown.retry_after_ms, Some(250));
        assert!(unknown.reason.as_deref().is_some_and(|reason| reason.contains("rate_limit")));

        let nack = DeliveryOutcome::PermanentFailure { reason: "message rejected".to_owned() }
            .to_receipt(&request);
        assert_eq!(nack.state, DeliveryReceiptState::Nack);
        assert_eq!(nack.reason.as_deref(), Some("message rejected"));
    }

    fn valid_outbound_request() -> OutboundMessageRequest {
        OutboundMessageRequest {
            envelope_id: "env-1:0".to_owned(),
            connector_id: "echo:default".to_owned(),
            conversation_id: "c1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "ok".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 1_000,
            max_payload_bytes: 8_192,
        }
    }
}
