//! Attachment handling for the channel platform.
//!
//! Ingests inbound Discord attachments into the media store, enforces the
//! outbound upload policy (enablement, content-type allowlist, size cap)
//! by neutralizing -- never erroring on -- violating attachments.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_connectors::{AttachmentRef, InboundMessageEvent, OutboundAttachment};

use crate::media::{InboundAttachmentIngestRequest, MediaArtifactStore};

/// Ingests every inbound attachment into the media store and returns the
/// store-backed references that replace the adapter-provided ones.
pub(super) async fn preprocess_discord_inbound_attachments(
    media_store: &Arc<MediaArtifactStore>,
    event: &InboundMessageEvent,
) -> Result<Vec<AttachmentRef>, crate::media::MediaStoreError> {
    let total_declared_bytes =
        event.attachments.iter().filter_map(|attachment| attachment.size_bytes).sum::<u64>();
    let mut prepared = Vec::with_capacity(event.attachments.len());
    for (attachment_index, attachment) in event.attachments.iter().enumerate() {
        prepared.push(
            media_store
                .ingest_inbound_attachment(InboundAttachmentIngestRequest {
                    connector_id: event.connector_id.as_str(),
                    envelope_id: event.envelope_id.as_str(),
                    conversation_id: event.conversation_id.as_str(),
                    adapter_message_id: event.adapter_message_id.as_deref(),
                    attachment,
                    attachment_index,
                    attachment_count: event.attachments.len(),
                    total_declared_bytes,
                })
                .await?,
        );
    }
    Ok(prepared)
}

/// Resolves and policy-checks outbound upload attachments.
///
/// Artifact-backed attachments are inlined from the media store; uploads
/// violating policy (disabled uploads, missing content, blocked content
/// type, oversize payload) are kept in the output but downgraded via
/// [`block_outbound_upload_attachment`] so the message still sends
/// without the upload.
pub(super) fn prepare_outbound_attachments(
    media_store: &Arc<MediaArtifactStore>,
    connector_id: &str,
    attachments: Vec<OutboundAttachment>,
) -> Result<Vec<OutboundAttachment>, crate::media::MediaStoreError> {
    let config = media_store.config().clone();
    let mut prepared = Vec::with_capacity(attachments.len());
    for mut attachment in attachments {
        if !attachment.upload_requested {
            prepared.push(attachment);
            continue;
        }
        if !config.outbound_upload_enabled {
            block_outbound_upload_attachment(
                media_store,
                connector_id,
                &mut attachment,
                "attachment.upload disabled by config",
            )?;
            prepared.push(attachment);
            continue;
        }
        if attachment.inline_base64.is_none() {
            match attachment.artifact_ref.as_deref() {
                Some(artifact_id) => match media_store.load_artifact_payload(artifact_id)? {
                    Some(payload) => {
                        attachment.inline_base64 =
                            Some(BASE64_STANDARD.encode(payload.bytes.as_slice()));
                        attachment.artifact_ref.get_or_insert_with(|| payload.artifact_id.clone());
                        attachment.filename.get_or_insert_with(|| payload.filename.clone());
                        attachment.content_type.get_or_insert_with(|| payload.content_type.clone());
                        attachment.size_bytes.get_or_insert(payload.size_bytes);
                        attachment.content_hash.get_or_insert(payload.sha256.clone());
                        attachment.width_px = attachment.width_px.or(payload.width_px);
                        attachment.height_px = attachment.height_px.or(payload.height_px);
                    }
                    None => {
                        block_outbound_upload_attachment(
                            media_store,
                            connector_id,
                            &mut attachment,
                            "attachment.upload artifact_ref not found",
                        )?;
                        prepared.push(attachment);
                        continue;
                    }
                },
                None => {
                    block_outbound_upload_attachment(
                        media_store,
                        connector_id,
                        &mut attachment,
                        "attachment.upload requires inline content or artifact_ref",
                    )?;
                    prepared.push(attachment);
                    continue;
                }
            }
        }
        let Some(content_type) = attachment
            .content_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            block_outbound_upload_attachment(
                media_store,
                connector_id,
                &mut attachment,
                "attachment.upload content_type is required",
            )?;
            prepared.push(attachment);
            continue;
        };
        if !config.outbound_allowed_content_types.iter().any(|allowed| allowed == &content_type) {
            block_outbound_upload_attachment(
                media_store,
                connector_id,
                &mut attachment,
                format!("attachment.upload content type '{content_type}' is blocked by policy")
                    .as_str(),
            )?;
            prepared.push(attachment);
            continue;
        }
        // Undecodable base64 counts as usize::MAX so it always trips the
        // size gate below -- fail closed instead of sending garbage.
        let decoded_size = attachment
            .inline_base64
            .as_deref()
            .map(|value| {
                BASE64_STANDARD.decode(value).map(|bytes| bytes.len()).unwrap_or(usize::MAX)
            })
            .unwrap_or_default();
        if decoded_size > config.outbound_max_upload_bytes {
            block_outbound_upload_attachment(
                media_store,
                connector_id,
                &mut attachment,
                format!(
                    "attachment.upload exceeds max_upload_bytes ({decoded_size}/{})",
                    config.outbound_max_upload_bytes
                )
                .as_str(),
            )?;
        }
        prepared.push(attachment);
    }
    Ok(prepared)
}

/// Downgrades a policy-violating upload in place (clears the payload,
/// records the reason as policy context) and logs the failure to the media
/// store; the surrounding message is still delivered.
fn block_outbound_upload_attachment(
    media_store: &Arc<MediaArtifactStore>,
    connector_id: &str,
    attachment: &mut OutboundAttachment,
    reason: &str,
) -> Result<(), crate::media::MediaStoreError> {
    attachment.policy_context = Some(reason.to_owned());
    attachment.upload_requested = false;
    attachment.inline_base64 = None;
    media_store.record_upload_failure(
        connector_id,
        attachment.artifact_ref.as_deref(),
        attachment.filename.as_deref(),
        reason,
    )
}
