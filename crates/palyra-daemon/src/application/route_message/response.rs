use std::sync::Arc;

use palyra_common::CANONICAL_PROTOCOL_MAJOR;
use serde_json::Value;
use tonic::Status;

use crate::{
    application::provider_events::{
        process_provider_event_for_surface, ProviderEventSurface, RouteMessageProviderEventSurface,
        RunStreamProviderEventOutcome,
    },
    gateway::GatewayRuntimeState,
    model_provider::ProviderResponse,
    transport::grpc::{
        auth::RequestContext,
        proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    },
};

const DEFAULT_ROUTE_MESSAGE_OUTPUT_MAX_BYTES: usize = 2_000;

#[derive(Debug, Clone, Default)]
pub(crate) struct RouteMessageStructuredOutput {
    pub(crate) structured_json: Vec<u8>,
    pub(crate) a2ui_update: Option<common_v1::A2uiUpdate>,
}

#[derive(Debug, Clone)]
pub(crate) struct RouteProviderResponseOutcome {
    pub(crate) reply_text: String,
    pub(crate) structured_output: RouteMessageStructuredOutput,
    pub(crate) prompt_tokens: u64,
    pub(crate) completion_tokens: u64,
}

pub(crate) struct RouteMessageOutputTemplate<'a> {
    pub(crate) thread_id: &'a str,
    pub(crate) in_reply_to_message_id: &'a str,
    pub(crate) broadcast: bool,
    pub(crate) auto_ack_text: &'a str,
    pub(crate) auto_reaction: &'a str,
    pub(crate) attachments: &'a [common_v1::MessageAttachment],
    pub(crate) structured_json: &'a [u8],
    pub(crate) a2ui_update: Option<&'a common_v1::A2uiUpdate>,
    pub(crate) delivery_metadata: Option<&'a Value>,
}

pub(crate) fn parse_route_message_structured_output(
    reply_text: &str,
    json_mode_requested: bool,
) -> RouteMessageStructuredOutput {
    if !json_mode_requested {
        return RouteMessageStructuredOutput::default();
    }
    let trimmed = reply_text.trim();
    if trimmed.is_empty() {
        return RouteMessageStructuredOutput::default();
    }
    let parsed = match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => value,
        Err(_) => return RouteMessageStructuredOutput::default(),
    };
    let structured_json = match serde_json::to_vec(&parsed) {
        Ok(value) => value,
        Err(_) => return RouteMessageStructuredOutput::default(),
    };
    let a2ui_update = parse_route_message_a2ui_update(&parsed);
    RouteMessageStructuredOutput { structured_json, a2ui_update }
}

pub(crate) fn build_route_message_outputs(
    reply_text: &str,
    max_payload_bytes: u64,
    template: &RouteMessageOutputTemplate<'_>,
) -> Vec<gateway_v1::OutboundMessage> {
    let max_bytes = usize::try_from(max_payload_bytes)
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_ROUTE_MESSAGE_OUTPUT_MAX_BYTES)
        .min(DEFAULT_ROUTE_MESSAGE_OUTPUT_MAX_BYTES);
    let chunks = split_route_message_reply_text(reply_text, max_bytes);
    let first_structured_json = build_route_message_first_structured_json(template);
    let mut outputs = Vec::with_capacity(chunks.len());
    for (index, chunk) in chunks.into_iter().enumerate() {
        outputs.push(gateway_v1::OutboundMessage {
            text: chunk,
            attachments: if index == 0 { template.attachments.to_vec() } else { Vec::new() },
            thread_id: template.thread_id.to_owned(),
            in_reply_to_message_id: template.in_reply_to_message_id.to_owned(),
            broadcast: template.broadcast,
            auto_ack_text: if index == 0 {
                template.auto_ack_text.to_owned()
            } else {
                String::new()
            },
            auto_reaction: if index == 0 {
                template.auto_reaction.to_owned()
            } else {
                String::new()
            },
            structured_json: if index == 0 { first_structured_json.clone() } else { Vec::new() },
            a2ui_update: if index == 0 { template.a2ui_update.cloned() } else { None },
        });
    }
    outputs
}

fn build_route_message_first_structured_json(template: &RouteMessageOutputTemplate<'_>) -> Vec<u8> {
    if !template.structured_json.is_empty() {
        return template.structured_json.to_vec();
    }
    let Some(delivery_metadata) = template.delivery_metadata else {
        return Vec::new();
    };
    serde_json::to_vec(&serde_json::json!({
        "delivery": delivery_metadata,
    }))
    .unwrap_or_default()
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_route_provider_response(
    runtime_state: &Arc<GatewayRuntimeState>,
    route_request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    provider_response: ProviderResponse,
    json_mode_requested: bool,
    response_prefix: Option<&str>,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<RouteProviderResponseOutcome, Status> {
    let mut reply_text = String::new();
    let mut summary_tokens = Vec::new();
    for event in provider_response.events {
        match process_provider_event_for_surface(
            runtime_state,
            session_id,
            run_id,
            event,
            &mut summary_tokens,
            remaining_tool_budget,
            tape_seq,
            ProviderEventSurface::RouteMessage(RouteMessageProviderEventSurface {
                request_context: route_request_context,
                reply_text: &mut reply_text,
            }),
        )
        .await?
        {
            RunStreamProviderEventOutcome::Continue => {}
            RunStreamProviderEventOutcome::Cancelled => {
                return Err(Status::internal(
                    "route provider event processing unexpectedly returned cancelled outcome",
                ));
            }
        }
    }
    if reply_text.trim().is_empty() && !summary_tokens.is_empty() {
        reply_text = summary_tokens.join(" ");
    }
    if reply_text.trim().is_empty() {
        reply_text = "ack".to_owned();
    }
    let structured_output =
        parse_route_message_structured_output(reply_text.as_str(), json_mode_requested);
    if let Some(prefix) = response_prefix {
        reply_text = format!("{prefix}{reply_text}");
    }

    Ok(RouteProviderResponseOutcome {
        reply_text,
        structured_output,
        prompt_tokens: provider_response.prompt_tokens,
        completion_tokens: provider_response.completion_tokens,
    })
}

fn split_route_message_reply_text(reply_text: &str, max_bytes: usize) -> Vec<String> {
    let normalized = reply_text.trim();
    if normalized.is_empty() {
        return vec!["ack".to_owned()];
    }
    let limit = max_bytes.max(1);
    if normalized.len() <= limit {
        return vec![normalized.to_owned()];
    }
    let mut chunks = Vec::new();
    let mut chunk_start = 0_usize;
    let mut chunk_bytes = 0_usize;
    for (index, character) in normalized.char_indices() {
        let character_bytes = character.len_utf8();
        if chunk_bytes + character_bytes > limit && chunk_bytes > 0 {
            chunks.push(normalized[chunk_start..index].to_owned());
            chunk_start = index;
            chunk_bytes = 0;
        }
        chunk_bytes += character_bytes;
    }
    if chunk_start < normalized.len() {
        chunks.push(normalized[chunk_start..].to_owned());
    }
    chunks
}

fn parse_route_message_a2ui_update(value: &Value) -> Option<common_v1::A2uiUpdate> {
    let update = value.get("a2ui_update").or_else(|| value.get("a2ui"))?;
    let update_object = update.as_object()?;
    let surface = update_object
        .get("surface")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_owned();
    let patch_source = update_object.get("patch_json").or_else(|| update_object.get("patch"))?;
    let patch_json = if let Some(raw_patch) = patch_source.as_str() {
        match serde_json::from_str::<Value>(raw_patch) {
            Ok(parsed_patch) => serde_json::to_vec(&parsed_patch).ok()?,
            Err(_) => serde_json::to_vec(patch_source).ok()?,
        }
    } else {
        serde_json::to_vec(patch_source).ok()?
    };
    if patch_json.is_empty() {
        return None;
    }
    Some(common_v1::A2uiUpdate { v: CANONICAL_PROTOCOL_MAJOR, surface, patch_json })
}

#[cfg(test)]
mod tests {
    use super::{build_route_message_outputs, RouteMessageOutputTemplate};
    use serde_json::json;

    fn output_template<'a>(
        structured_json: &'a [u8],
        delivery_metadata: Option<&'a serde_json::Value>,
    ) -> RouteMessageOutputTemplate<'a> {
        RouteMessageOutputTemplate {
            thread_id: "thread",
            in_reply_to_message_id: "message",
            broadcast: false,
            auto_ack_text: "",
            auto_reaction: "",
            attachments: &[],
            structured_json,
            a2ui_update: None,
            delivery_metadata,
        }
    }

    #[test]
    fn route_outputs_attach_delivery_metadata_when_structured_json_is_empty() {
        let delivery_metadata = json!({
            "policy": {
                "policy_id": "delivery_arbitration.v1",
                "surface": "external_channel",
            }
        });
        let outputs = build_route_message_outputs(
            "hello",
            2_000,
            &output_template(&[], Some(&delivery_metadata)),
        );

        assert_eq!(outputs.len(), 1);
        let structured: serde_json::Value =
            serde_json::from_slice(outputs[0].structured_json.as_slice())
                .expect("delivery metadata should be valid json");
        assert_eq!(structured["delivery"], delivery_metadata);
    }

    #[test]
    fn route_outputs_preserve_model_structured_json_over_delivery_metadata() {
        let structured_json = br#"{"model":true}"#;
        let delivery_metadata = json!({ "policy": "delivery_arbitration.v1" });
        let outputs = build_route_message_outputs(
            "hello",
            2_000,
            &output_template(structured_json, Some(&delivery_metadata)),
        );

        assert_eq!(outputs[0].structured_json, structured_json);
    }
}
