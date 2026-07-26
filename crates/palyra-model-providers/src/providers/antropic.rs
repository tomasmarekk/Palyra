//! Anthropic provider identity and static capability defaults.

use std::collections::BTreeSet;

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
        body["system"] = anthropic_system_payload(request, system);
    }
    body
}

fn anthropic_system_payload(request: &ProviderRequest, system: String) -> Value {
    if !request.prompt_cache_policy.enabled
        || request.prompt_cache_report.as_ref().is_none_or(|report| report.cacheable_tokens == 0)
    {
        return json!(system);
    }
    json!([{
        "type": "text",
        "text": system,
        "cache_control": { "type": "ephemeral" },
    }])
}

fn build_anthropic_messages_and_system(request: &ProviderRequest) -> (Vec<Value>, Option<String>) {
    let mut system_blocks = Vec::new();
    let mut messages = crate::project_provider_request_messages(
        request.effective_messages(),
        crate::ProviderTranscriptDialect::AnthropicMessages,
    );
    let cache_control_message_indexes = anthropic_cache_control_message_indexes(request, &messages);
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
                let cache_control = cache_control_message_indexes.contains(&index);
                if let Some(consumed_tool_results) = push_anthropic_expanded_multi_tool_exchange(
                    &mut provider_messages,
                    message,
                    &messages[index + 1..],
                ) {
                    index = index.saturating_add(consumed_tool_results);
                } else {
                    provider_messages.push(json!({
                        "role": message.role.as_anthropic_role(),
                        "content": build_anthropic_content_parts(message, cache_control),
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

fn anthropic_cache_control_message_indexes(
    request: &ProviderRequest,
    messages: &[ProviderMessage],
) -> BTreeSet<usize> {
    if !request.prompt_cache_policy.enabled
        || request.prompt_cache_report.as_ref().is_none_or(|report| report.cacheable_tokens == 0)
    {
        return BTreeSet::new();
    }
    let Some(report) = request.prompt_cache_report.as_ref() else {
        return BTreeSet::new();
    };
    let system_marker_count = usize::from(messages.iter().any(|message| {
        matches!(message.role, ProviderMessageRole::System | ProviderMessageRole::Developer)
            && !message.text_content().trim().is_empty()
    }));
    let marker_budget = request
        .prompt_cache_policy
        .max_breakpoints
        .min(report.breakpoint_count)
        .saturating_sub(system_marker_count);
    if marker_budget == 0 {
        return BTreeSet::new();
    }
    messages
        .iter()
        .enumerate()
        .filter(|(index, message)| {
            index.saturating_add(1) < messages.len()
                && matches!(
                    message.role,
                    ProviderMessageRole::User | ProviderMessageRole::Assistant
                )
                && !message.text_content().trim().is_empty()
        })
        .rev()
        .take(marker_budget)
        .map(|(index, _)| index)
        .collect()
}

fn build_anthropic_content_parts(message: &ProviderMessage, cache_control: bool) -> Vec<Value> {
    if message.role == ProviderMessageRole::Tool {
        return vec![build_anthropic_tool_result_content_part(message)];
    }

    let mut parts = build_anthropic_non_tool_content_parts(message, cache_control);
    if message.role == ProviderMessageRole::Assistant {
        for tool_call in &message.tool_calls {
            parts.push(build_anthropic_tool_use_content_part(tool_call));
        }
    }
    parts
}

fn build_anthropic_non_tool_content_parts(
    message: &ProviderMessage,
    cache_control: bool,
) -> Vec<Value> {
    let mut parts = Vec::new();
    let last_text_part_index = cache_control.then(|| {
        message.content.iter().rposition(|content_part| {
            matches!(content_part, ProviderMessageContentPart::Text { text } if !text.trim().is_empty())
        })
    }).flatten();
    for (index, content_part) in message.content.iter().enumerate() {
        match content_part {
            ProviderMessageContentPart::Text { text } => {
                let mut part = json!({
                    "type": "text",
                    "text": text,
                });
                if last_text_part_index == Some(index) {
                    part["cache_control"] = json!({ "type": "ephemeral" });
                }
                parts.push(part);
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
            build_anthropic_non_tool_content_parts(assistant_message, false)
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
        messages_payload,
    };
    use crate::config::{ModelProviderAuthProviderKind, ModelProviderCredentialSource};
    use crate::{
        PromptCacheReport, PromptCacheStrategy, ProviderMessage, ProviderMessageContentPart,
        ProviderMessageRole, ProviderMessageToolCall, ProviderRequest,
    };

    fn prompt_cache_request(enabled: bool) -> ProviderRequest {
        let mut request =
            ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None);
        request.messages.insert(
            0,
            ProviderMessage {
                role: ProviderMessageRole::System,
                content: vec![ProviderMessageContentPart::text("stable system prompt")],
                name: None,
                tool_call_id: None,
                tool_calls: Vec::new(),
            },
        );
        request.prompt_cache_policy.enabled = enabled;
        request.prompt_cache_policy.strategy = PromptCacheStrategy::SystemAndTool;
        request.prompt_cache_report = Some(PromptCacheReport {
            eligible_bytes: 256,
            invalidated_bytes: 0,
            invalidation_reasons: Vec::new(),
            provider_request_hash:
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
            requested_strategy: PromptCacheStrategy::SystemAndTool,
            applied_strategy: "metadata_only".to_owned(),
            breakpoint_count: 3,
            cacheable_tokens: 64,
            actual_cached_tokens: None,
            prompt_cache_epoch: 7,
            stable_prefix_hash: Some("stable-prefix-hash".to_owned()),
            cache_scope_hash: Some("cache-scope-hash".to_owned()),
            tool_catalog_hash: Some("tool-catalog-hash".to_owned()),
            memory_snapshot_hash: Some("memory-snapshot-hash".to_owned()),
            provider_cache_strategy: "anthropic_cache_control".to_owned(),
        });
        request
    }

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

    #[test]
    fn anthropic_payload_emits_cache_control_for_system_prompt() {
        let payload = messages_payload(&prompt_cache_request(true), "claude-test", Vec::new());

        assert_eq!(payload["system"][0]["type"], "text");
        assert_eq!(payload["system"][0]["text"], "stable system prompt");
        assert_eq!(payload["system"][0]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn anthropic_payload_keeps_system_string_when_cache_disabled() {
        let payload = messages_payload(&prompt_cache_request(false), "claude-test", Vec::new());

        assert_eq!(payload["system"], "stable system prompt");
    }

    #[test]
    fn anthropic_payload_marks_recent_cacheable_non_system_turns() {
        let mut request = prompt_cache_request(true);
        request.messages.push(ProviderMessage::user_text("prior user context"));
        request.messages.push(ProviderMessage {
            role: ProviderMessageRole::Assistant,
            content: vec![ProviderMessageContentPart::text("prior assistant context")],
            name: None,
            tool_call_id: None,
            tool_calls: Vec::new(),
        });
        request.messages.push(ProviderMessage::user_text("current turn must stay volatile"));

        let payload = messages_payload(&request, "claude-test", Vec::new());
        let messages = payload["messages"].as_array().expect("messages should be an array");

        assert!(messages[0].pointer("/content/0/cache_control").is_none());
        assert_eq!(messages[1]["content"][0]["cache_control"]["type"], "ephemeral");
        assert_eq!(messages[2]["content"][0]["cache_control"]["type"], "ephemeral");
        assert!(
            messages
                .last()
                .and_then(|message| message.pointer("/content/0/cache_control"))
                .is_none(),
            "current turn should not receive an Anthropic cache marker"
        );
    }

    #[test]
    fn active_anthropic_payload_synthesizes_a_strict_missing_tool_result() {
        let mut request =
            ProviderRequest::from_input_text("current turn".to_owned(), false, Vec::new(), None);
        request.messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "bad id/with spaces".to_owned(),
                    tool_name: "palyra.status".to_owned(),
                    input_json: serde_json::json!({}),
                }],
            },
            ProviderMessage::user_text("continue safely"),
        ];

        let payload = messages_payload(&request, "claude-test", Vec::new());
        let messages = payload["messages"].as_array().expect("messages should be an array");
        let tool_use = &messages[0]["content"][0];
        let tool_result = &messages[1]["content"][0];
        let normalized_id = tool_use["id"].as_str().expect("Anthropic tool use should have an ID");

        assert!(normalized_id.starts_with("toolu_"));
        assert_eq!(tool_use["type"], "tool_use");
        assert_eq!(tool_result["type"], "tool_result");
        assert_eq!(tool_result["tool_use_id"], normalized_id);
        assert!(tool_result["content"]
            .as_str()
            .is_some_and(|content| content.contains(r#""success":false"#)));
    }
}
