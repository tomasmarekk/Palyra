//! Provider-view multimodal input planning.
//!
//! The provider request may still carry current-turn inline image bytes for
//! vision-capable models, but recovery and diagnostics need a separate
//! metadata-only view so rejected or shrunken images never leak raw bytes back
//! into model-visible context.

use base64::Engine as _;
use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    gateway::non_empty, media::MediaRuntimeConfig, model_provider::ProviderImageInput,
    transport::grpc::proto::palyra::common::v1 as common_v1,
};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MultimodalImageDisposition {
    SelectedCurrent,
    RejectedCurrent,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MultimodalImageDecision {
    pub(crate) attachment_index: usize,
    pub(crate) source_turn: String,
    pub(crate) disposition: MultimodalImageDisposition,
    pub(crate) reason_code: String,
    pub(crate) mime_type: Option<String>,
    pub(crate) size_bytes: usize,
    pub(crate) width_px: Option<u32>,
    pub(crate) height_px: Option<u32>,
    pub(crate) artifact_id: Option<String>,
    pub(crate) content_hash: Option<String>,
    pub(crate) provider_bytes_included: bool,
    pub(crate) canonical_history_mutated: bool,
    pub(crate) metadata_replacement: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultimodalProviderInputPlan {
    pub(crate) vision_inputs: Vec<ProviderImageInput>,
    pub(crate) decisions: Vec<MultimodalImageDecision>,
}

/// Selects provider-visible current image attachments and records why every
/// considered image was selected or converted to metadata-only context.
#[must_use]
pub(crate) fn build_multimodal_provider_input_plan(
    attachments: &[common_v1::MessageAttachment],
    media_config: &MediaRuntimeConfig,
) -> MultimodalProviderInputPlan {
    let mut vision_inputs = Vec::new();
    let mut decisions = Vec::new();
    let mut total_bytes = 0usize;

    for (attachment_index, attachment) in attachments.iter().enumerate() {
        if attachment.kind != common_v1::message_attachment::AttachmentKind::Image as i32 {
            continue;
        }
        if vision_inputs.len() >= media_config.vision_max_image_count {
            break;
        }

        let Some(mime_type) = non_empty(attachment.declared_content_type.clone()) else {
            decisions.push(rejected_image_decision(
                attachment_index,
                attachment,
                None,
                "missing_content_type",
            ));
            continue;
        };
        if !media_config.vision_allowed_content_types.iter().any(|allowed| allowed == &mime_type) {
            decisions.push(rejected_image_decision(
                attachment_index,
                attachment,
                Some(mime_type),
                "unsupported_content_type",
            ));
            continue;
        }
        if attachment.inline_bytes.is_empty() {
            decisions.push(rejected_image_decision(
                attachment_index,
                attachment,
                Some(mime_type),
                "missing_inline_bytes",
            ));
            continue;
        }
        let image_bytes = attachment.inline_bytes.len();
        if image_bytes > media_config.vision_max_image_bytes {
            decisions.push(rejected_image_decision(
                attachment_index,
                attachment,
                Some(mime_type),
                "image_bytes_exceed_limit",
            ));
            continue;
        }
        if total_bytes.saturating_add(image_bytes) > media_config.vision_max_total_bytes {
            decisions.push(rejected_image_decision(
                attachment_index,
                attachment,
                Some(mime_type),
                "total_image_bytes_exceed_limit",
            ));
            break;
        }
        let width_px = (attachment.width_px > 0).then_some(attachment.width_px);
        let height_px = (attachment.height_px > 0).then_some(attachment.height_px);
        if width_px.is_some_and(|value| value > media_config.vision_max_dimension_px)
            || height_px.is_some_and(|value| value > media_config.vision_max_dimension_px)
        {
            decisions.push(rejected_image_decision(
                attachment_index,
                attachment,
                Some(mime_type),
                "image_dimensions_exceed_limit",
            ));
            continue;
        }

        total_bytes = total_bytes.saturating_add(image_bytes);
        let artifact_id = attachment.artifact_id.as_ref().map(|value| value.ulid.clone());
        vision_inputs.push(ProviderImageInput {
            mime_type: mime_type.clone(),
            bytes_base64: base64::engine::general_purpose::STANDARD
                .encode(attachment.inline_bytes.as_slice()),
            file_name: non_empty(attachment.filename.clone()),
            width_px,
            height_px,
            artifact_id: artifact_id.clone(),
        });
        decisions.push(MultimodalImageDecision {
            attachment_index,
            source_turn: "current".to_owned(),
            disposition: MultimodalImageDisposition::SelectedCurrent,
            reason_code: "selected_current_image".to_owned(),
            mime_type: Some(mime_type),
            size_bytes: image_bytes,
            width_px,
            height_px,
            artifact_id,
            content_hash: non_empty(attachment.content_hash.clone()),
            provider_bytes_included: true,
            canonical_history_mutated: false,
            metadata_replacement: None,
        });
    }

    MultimodalProviderInputPlan { vision_inputs, decisions }
}

fn rejected_image_decision(
    attachment_index: usize,
    attachment: &common_v1::MessageAttachment,
    mime_type: Option<String>,
    reason_code: &str,
) -> MultimodalImageDecision {
    let width_px = (attachment.width_px > 0).then_some(attachment.width_px);
    let height_px = (attachment.height_px > 0).then_some(attachment.height_px);
    let artifact_id = attachment.artifact_id.as_ref().map(|value| value.ulid.clone());
    let content_hash = non_empty(attachment.content_hash.clone());
    let size_bytes = attachment.inline_bytes.len();
    MultimodalImageDecision {
        attachment_index,
        source_turn: "current".to_owned(),
        disposition: MultimodalImageDisposition::RejectedCurrent,
        reason_code: reason_code.to_owned(),
        mime_type: mime_type.clone(),
        size_bytes,
        width_px,
        height_px,
        artifact_id: artifact_id.clone(),
        content_hash: content_hash.clone(),
        provider_bytes_included: false,
        canonical_history_mutated: false,
        metadata_replacement: Some(json!({
            "kind": "image_metadata_replacement",
            "source_turn": "current",
            "reason_code": reason_code,
            "mime_type": mime_type,
            "size_bytes": size_bytes,
            "width_px": width_px,
            "height_px": height_px,
            "artifact_id": artifact_id,
            "content_hash": content_hash,
            "provider_bytes_included": false,
            "canonical_history_mutated": false,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::grpc::proto::palyra::common::v1::CanonicalId;

    fn image_attachment(
        mime_type: &str,
        bytes: &[u8],
        width_px: u32,
        height_px: u32,
    ) -> common_v1::MessageAttachment {
        common_v1::MessageAttachment {
            kind: common_v1::message_attachment::AttachmentKind::Image as i32,
            artifact_id: Some(CanonicalId { ulid: "01J00000000000000000000000".to_owned() }),
            declared_content_type: mime_type.to_owned(),
            filename: "capture.png".to_owned(),
            inline_bytes: bytes.to_vec(),
            width_px,
            height_px,
            content_hash: "sha256:abc123".to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn multimodal_plan_preserves_current_image_selection_contract() {
        let media_config = MediaRuntimeConfig {
            vision_allowed_content_types: vec!["image/png".to_owned()],
            vision_max_image_count: 3,
            vision_max_image_bytes: 8,
            vision_max_total_bytes: 16,
            vision_max_dimension_px: 32,
            ..MediaRuntimeConfig::default()
        };
        let attachments = vec![
            image_attachment("image/png", b"abcd", 10, 10),
            image_attachment("image/jpeg", b"efgh", 10, 10),
            image_attachment("image/png", b"oversized", 10, 10),
            image_attachment("image/png", b"ijkl", 64, 10),
        ];

        let plan = build_multimodal_provider_input_plan(attachments.as_slice(), &media_config);

        assert_eq!(plan.vision_inputs.len(), 1);
        assert_eq!(plan.vision_inputs[0].mime_type, "image/png");
        assert_eq!(plan.vision_inputs[0].width_px, Some(10));
        assert_eq!(plan.decisions.len(), 4);
        assert_eq!(plan.decisions[0].disposition, MultimodalImageDisposition::SelectedCurrent);
        assert_eq!(plan.decisions[1].reason_code, "unsupported_content_type");
        assert_eq!(plan.decisions[2].reason_code, "image_bytes_exceed_limit");
        assert_eq!(plan.decisions[3].reason_code, "image_dimensions_exceed_limit");
    }

    #[test]
    fn multimodal_decisions_never_serialize_raw_image_bytes() {
        let media_config = MediaRuntimeConfig {
            vision_allowed_content_types: vec!["image/png".to_owned()],
            vision_max_image_bytes: 2,
            ..MediaRuntimeConfig::default()
        };
        let attachments = vec![image_attachment("image/png", b"raw-image-secret", 10, 10)];

        let plan = build_multimodal_provider_input_plan(attachments.as_slice(), &media_config);
        let serialized =
            serde_json::to_string(&plan.decisions).expect("decisions should serialize");

        assert_eq!(plan.vision_inputs.len(), 0);
        assert!(!plan.decisions[0].provider_bytes_included);
        assert!(!plan.decisions[0].canonical_history_mutated);
        assert_eq!(
            plan.decisions[0].metadata_replacement.as_ref().unwrap()["kind"],
            "image_metadata_replacement"
        );
        assert!(!serialized.contains("raw-image-secret"));
        assert!(!serialized.contains("cmF3LWltYWdlLXNlY3JldA"));
    }
}
