//! Mapping between connector attachment/A2UI types and the gateway proto
//! representation. Proto uses empty strings/zeroes where the connector
//! types use `Option`; these helpers translate in both directions.

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_connectors::{
    AttachmentKind, AttachmentRef, OutboundA2uiUpdate as ConnectorA2uiUpdate, OutboundAttachment,
};

use crate::gateway::proto::palyra::common::v1 as common_v1;

/// Converts a proto string field to `Option`, treating blank as absent.
pub(super) fn non_empty(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// Converts a proto bytes field to `Option`, treating empty as absent.
pub(super) fn non_empty_bytes(raw: Vec<u8>) -> Option<Vec<u8>> {
    if raw.is_empty() {
        None
    } else {
        Some(raw)
    }
}

/// Maps inbound attachment references to proto message attachments,
/// decoding inline base64 payloads to raw bytes (undecodable payloads are
/// dropped to empty rather than failing the whole envelope).
pub(super) fn to_proto_message_attachments(
    attachments: &[AttachmentRef],
) -> Vec<common_v1::MessageAttachment> {
    attachments
        .iter()
        .map(|attachment| {
            let artifact_id = attachment
                .artifact_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| common_v1::CanonicalId { ulid: value.to_owned() });
            common_v1::MessageAttachment {
                kind: attachment_kind_to_proto(attachment.kind),
                artifact_id,
                size_bytes: attachment.size_bytes.unwrap_or_default(),
                attachment_id: attachment.attachment_id.clone().unwrap_or_default(),
                filename: attachment.filename.clone().unwrap_or_default(),
                declared_content_type: attachment.content_type.clone().unwrap_or_default(),
                source_url: attachment.url.clone().unwrap_or_default(),
                content_hash: attachment.content_hash.clone().unwrap_or_default(),
                origin: attachment.origin.clone().unwrap_or_default(),
                policy_context: attachment.policy_context.clone().unwrap_or_default(),
                inline_bytes: attachment
                    .inline_base64
                    .as_deref()
                    .and_then(|value| BASE64_STANDARD.decode(value).ok())
                    .unwrap_or_default(),
                upload_requested: attachment.upload_requested,
                width_px: attachment.width_px.unwrap_or_default(),
                height_px: attachment.height_px.unwrap_or_default(),
            }
        })
        .collect()
}

/// Maps proto message attachments back to outbound connector attachments,
/// re-encoding inline bytes as base64.
pub(super) fn from_proto_message_attachments(
    attachments: &[common_v1::MessageAttachment],
) -> Vec<OutboundAttachment> {
    attachments
        .iter()
        .map(|attachment| OutboundAttachment {
            kind: attachment_kind_from_proto(attachment.kind),
            attachment_id: non_empty(attachment.attachment_id.clone()),
            url: non_empty(attachment.source_url.clone()),
            artifact_ref: attachment.artifact_id.as_ref().map(|value| value.ulid.clone()),
            filename: non_empty(attachment.filename.clone()),
            content_type: non_empty(attachment.declared_content_type.clone()),
            size_bytes: if attachment.size_bytes > 0 { Some(attachment.size_bytes) } else { None },
            content_hash: non_empty(attachment.content_hash.clone()),
            origin: non_empty(attachment.origin.clone()),
            policy_context: non_empty(attachment.policy_context.clone()),
            upload_requested: attachment.upload_requested,
            inline_base64: if attachment.inline_bytes.is_empty() {
                None
            } else {
                Some(BASE64_STANDARD.encode(attachment.inline_bytes.as_slice()))
            },
            width_px: if attachment.width_px > 0 { Some(attachment.width_px) } else { None },
            height_px: if attachment.height_px > 0 { Some(attachment.height_px) } else { None },
        })
        .collect()
}

/// Maps a proto A2UI update to the connector type; updates without a
/// surface or patch payload are discarded as no-ops.
pub(super) fn from_proto_a2ui_update(
    update: Option<common_v1::A2uiUpdate>,
) -> Option<ConnectorA2uiUpdate> {
    let update = update?;
    let surface = update.surface.trim();
    if surface.is_empty() || update.patch_json.is_empty() {
        return None;
    }
    Some(ConnectorA2uiUpdate { surface: surface.to_owned(), patch_json: update.patch_json })
}

fn attachment_kind_to_proto(kind: AttachmentKind) -> i32 {
    match kind {
        AttachmentKind::Image => common_v1::message_attachment::AttachmentKind::Image as i32,
        AttachmentKind::File => common_v1::message_attachment::AttachmentKind::File as i32,
    }
}

fn attachment_kind_from_proto(kind: i32) -> AttachmentKind {
    match common_v1::message_attachment::AttachmentKind::try_from(kind).ok() {
        Some(common_v1::message_attachment::AttachmentKind::Image) => AttachmentKind::Image,
        _ => AttachmentKind::File,
    }
}
