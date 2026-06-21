//! Anthropic provider identity and static capability defaults.

use serde_json::{json, Value};

use crate::config::{
    ModelProviderAuthProviderKind, ModelProviderCredentialSource, ProviderCapabilitiesSnapshot,
    ProviderCostTier, ProviderLatencyTier, ProviderMetadataSource,
};
use crate::contract::{
    ProviderMessage, ProviderMessageContentPart, ProviderMessageRole, ProviderMessageToolCall,
    ProviderRequest,
};

pub(crate) const PROVIDER_ID: &str = "anthropic-primary";
pub(crate) const DISPLAY_NAME: &str = "Anthropic";
const DEFAULT_ANTHROPIC_MAX_OUTPUT_TOKENS: u64 = 4_096;

pub(crate) fn chat_capabilities() -> ProviderCapabilitiesSnapshot {
    ProviderCapabilitiesSnapshot {
        streaming_tokens: true,
        tool_calls: true,
        json_mode: true,
        vision: true,
        audio_transcribe: false,
        embeddings: false,
        reasoning: false,
        reasoning_efforts: Vec::new(),
        service_tier: false,
        service_tiers: Vec::new(),
        max_context_tokens: Some(200_000),
        cost_tier: ProviderCostTier::Premium.as_str().to_owned(),
        latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
        recommended_use_cases: vec![
            "long-context reasoning".to_owned(),
            "tool-heavy chat".to_owned(),
        ],
        known_limitations: vec!["audio transcription not supported".to_owned()],
        operator_override: false,
        metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
    }
}

/// Returns whether an Anthropic-compatible provider should use bearer auth.
///
/// MiniMax exposes an Anthropic-compatible messages API with bearer auth, while
/// native Anthropic uses bearer auth only for Claude subscription OAuth tokens.
#[must_use]
pub fn anthropic_compatible_uses_bearer_auth(
    kind: Option<ModelProviderAuthProviderKind>,
    credential_source: Option<ModelProviderCredentialSource>,
) -> bool {
    matches!(kind, Some(ModelProviderAuthProviderKind::Minimax))
        || matches!(
            (kind, credential_source),
            (
                Some(ModelProviderAuthProviderKind::Anthropic),
                Some(ModelProviderCredentialSource::AuthProfileOauthAccessToken)
            )
        )
}

/// Returns whether Anthropic OAuth beta and user-agent headers are required.
#[must_use]
pub fn anthropic_compatible_uses_anthropic_oauth_headers(
    kind: Option<ModelProviderAuthProviderKind>,
    credential_source: Option<ModelProviderCredentialSource>,
) -> bool {
    matches!(
        (kind, credential_source),
        (
            Some(ModelProviderAuthProviderKind::Anthropic),
            Some(ModelProviderCredentialSource::AuthProfileOauthAccessToken)
        )
    )
}

/// Builds the Anthropic messages request body used by Anthropic and MiniMax.
///
/// `tools` must already be projected into the Anthropic tool schema by the
/// caller. This keeps daemon tool-catalog ownership outside the provider crate
/// while centralizing the Anthropic-compatible wire shape here.
#[must_use]
pub fn messages_payload(request: &ProviderRequest, model_name: &str, tools: Vec<Value>) -> Value {
    let (messages, system) = build_anthropic_messages_and_system(request);
    let max_tokens =
        request.max_output_tokens.unwrap_or(DEFAULT_ANTHROPIC_MAX_OUTPUT_TOKENS).max(1);
    let mut body = json!({
        "model": model_name,
        "max_tokens": max_tokens,
        "messages": messages,
    });
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools);
    }
    if let Some(system) = system {
        body["system"] = json!(system);
    }
    body
}

fn build_anthropic_messages_and_system(request: &ProviderRequest) -> (Vec<Value>, Option<String>) {
    let mut system_blocks = Vec::new();
    let mut messages = request.effective_messages();
    if !request.vision_inputs.is_empty() {
        if let Some(last_user) =
            messages.iter_mut().rev().find(|message| message.role == ProviderMessageRole::User)
        {
            for image in &request.vision_inputs {
                last_user.content.push(ProviderMessageContentPart::Image { image: image.clone() });
            }
        }
    }
    let mut provider_messages = Vec::new();
    let mut pending_tool_result_parts = Vec::new();
    let mut expected_tool_result_ids = Vec::<String>::new();
    let flush_pending_tool_result_parts =
        |provider_messages: &mut Vec<Value>, pending_tool_result_parts: &mut Vec<Value>| {
            if pending_tool_result_parts.is_empty() {
                return;
            }
            provider_messages.push(json!({
                "role": "user",
                "content": std::mem::take(pending_tool_result_parts),
            }));
        };
    let mut index = 0;
    while index < messages.len() {
        let message = &messages[index];
        match message.role {
            ProviderMessageRole::System | ProviderMessageRole::Developer => {
                let text = message.text_content();
                if !text.trim().is_empty() {
                    system_blocks.push(text);
                }
            }
            ProviderMessageRole::Tool => {
                let expected_id = expected_tool_result_ids.first().map(String::as_str);
                if message.tool_call_id.as_deref() == expected_id {
                    pending_tool_result_parts
                        .push(build_anthropic_tool_result_content_part(message));
                    expected_tool_result_ids.remove(0);
                } else {
                    flush_pending_tool_result_parts(
                        &mut provider_messages,
                        &mut pending_tool_result_parts,
                    );
                    expected_tool_result_ids.clear();
                    provider_messages.push(json!({
                        "role": "user",
                        "content": [build_anthropic_orphan_tool_result_text_part(message)],
                    }));
                }
            }
            ProviderMessageRole::User | ProviderMessageRole::Assistant => {
                flush_pending_tool_result_parts(
                    &mut provider_messages,
                    &mut pending_tool_result_parts,
                );
                expected_tool_result_ids.clear();
                if let Some(consumed_tool_results) = push_anthropic_expanded_multi_tool_exchange(
                    &mut provider_messages,
                    message,
                    &messages[index + 1..],
                ) {
                    index = index.saturating_add(consumed_tool_results);
                } else {
                    provider_messages.push(json!({
                        "role": message.role.as_anthropic_role(),
                        "content": build_anthropic_content_parts(message),
                    }));
                    if message.role == ProviderMessageRole::Assistant {
                        expected_tool_result_ids = message
                            .tool_calls
                            .iter()
                            .map(|tool_call| tool_call.proposal_id.clone())
                            .collect();
                    }
                }
            }
        }
        index = index.saturating_add(1);
    }
    flush_pending_tool_result_parts(&mut provider_messages, &mut pending_tool_result_parts);
    (provider_messages, (!system_blocks.is_empty()).then(|| system_blocks.join("\n\n")))
}

fn build_anthropic_content_parts(message: &ProviderMessage) -> Vec<Value> {
    if message.role == ProviderMessageRole::Tool {
        return vec![build_anthropic_tool_result_content_part(message)];
    }

    let mut parts = build_anthropic_non_tool_content_parts(message);
    if message.role == ProviderMessageRole::Assistant {
        for tool_call in &message.tool_calls {
            parts.push(build_anthropic_tool_use_content_part(tool_call));
        }
    }
    parts
}

fn build_anthropic_non_tool_content_parts(message: &ProviderMessage) -> Vec<Value> {
    let mut parts = Vec::new();
    for content_part in &message.content {
        match content_part {
            ProviderMessageContentPart::Text { text } => {
                parts.push(json!({
                    "type": "text",
                    "text": text,
                }));
            }
            ProviderMessageContentPart::Image { image } => {
                parts.push(json!({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": image.mime_type,
                        "data": image.bytes_base64,
                    }
                }));
            }
        }
    }
    parts
}

fn build_anthropic_tool_use_content_part(tool_call: &ProviderMessageToolCall) -> Value {
    json!({
        "type": "tool_use",
        "id": tool_call.proposal_id.as_str(),
        "name": tool_call.tool_name.as_str(),
        "input": &tool_call.input_json,
    })
}

fn build_anthropic_tool_result_content_part(message: &ProviderMessage) -> Value {
    json!({
        "type": "tool_result",
        "tool_use_id": message.tool_call_id.as_deref().unwrap_or_default(),
        "content": message.text_content(),
    })
}

fn build_anthropic_orphan_tool_result_text_part(message: &ProviderMessage) -> Value {
    let tool_call_id = message.tool_call_id.as_deref().unwrap_or("unknown");
    json!({
        "type": "text",
        "text": format!("Tool result for {tool_call_id}:\n{}", message.text_content()),
    })
}

fn push_anthropic_expanded_multi_tool_exchange(
    provider_messages: &mut Vec<Value>,
    assistant_message: &ProviderMessage,
    following_messages: &[ProviderMessage],
) -> Option<usize> {
    if assistant_message.role != ProviderMessageRole::Assistant
        || assistant_message.tool_calls.len() < 2
    {
        return None;
    }

    let tool_result_count = following_messages
        .iter()
        .take_while(|message| message.role == ProviderMessageRole::Tool)
        .count();
    if tool_result_count != assistant_message.tool_calls.len() {
        return None;
    }

    let tool_result_messages = &following_messages[..tool_result_count];
    if !assistant_message.tool_calls.iter().zip(tool_result_messages).all(
        |(tool_call, tool_result_message)| {
            tool_result_message.tool_call_id.as_deref() == Some(tool_call.proposal_id.as_str())
        },
    ) {
        return None;
    }

    for (index, (tool_call, tool_result_message)) in
        assistant_message.tool_calls.iter().zip(tool_result_messages).enumerate()
    {
        let mut assistant_content = if index == 0 {
            build_anthropic_non_tool_content_parts(assistant_message)
        } else {
            Vec::new()
        };
        assistant_content.push(build_anthropic_tool_use_content_part(tool_call));
        provider_messages.push(json!({
            "role": "assistant",
            "content": assistant_content,
        }));
        provider_messages.push(json!({
            "role": "user",
            "content": [build_anthropic_tool_result_content_part(tool_result_message)],
        }));
    }

    Some(tool_result_count)
}

#[cfg(test)]
mod tests {
    use super::{
        anthropic_compatible_uses_anthropic_oauth_headers, anthropic_compatible_uses_bearer_auth,
    };
    use crate::config::{ModelProviderAuthProviderKind, ModelProviderCredentialSource};

    #[test]
    fn minimax_anthropic_compatible_transport_uses_bearer_auth() {
        assert!(anthropic_compatible_uses_bearer_auth(
            Some(ModelProviderAuthProviderKind::Minimax),
            Some(ModelProviderCredentialSource::InlineConfig),
        ));
        assert!(!anthropic_compatible_uses_anthropic_oauth_headers(
            Some(ModelProviderAuthProviderKind::Minimax),
            Some(ModelProviderCredentialSource::InlineConfig),
        ));
    }

    #[test]
    fn anthropic_oauth_uses_bearer_auth_and_oauth_headers() {
        let kind = Some(ModelProviderAuthProviderKind::Anthropic);
        let source = Some(ModelProviderCredentialSource::AuthProfileOauthAccessToken);

        assert!(anthropic_compatible_uses_bearer_auth(kind, source));
        assert!(anthropic_compatible_uses_anthropic_oauth_headers(kind, source));
    }

    #[test]
    fn anthropic_api_key_auth_does_not_use_bearer_auth() {
        let kind = Some(ModelProviderAuthProviderKind::Anthropic);
        let source = Some(ModelProviderCredentialSource::InlineConfig);

        assert!(!anthropic_compatible_uses_bearer_auth(kind, source));
        assert!(!anthropic_compatible_uses_anthropic_oauth_headers(kind, source));
    }
}
