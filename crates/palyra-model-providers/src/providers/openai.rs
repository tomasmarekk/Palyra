//! OpenAI-compatible provider identity and static capability defaults.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::{
    ProviderCapabilitiesSnapshot, ProviderCostTier, ProviderLatencyTier, ProviderMetadataSource,
};
use crate::contract::{
    model_id_supports_reasoning_effort, ProviderImageInput, ProviderMessage,
    ProviderMessageContentPart, ProviderMessageRole, ProviderReasoningEffort, ProviderRequest,
    ProviderServiceTier,
};
use crate::discovery::DiscoveredProviderModel;

pub(crate) const PROVIDER_ID: &str = "openai-primary";
pub(crate) const DISPLAY_NAME: &str = "OpenAI-compatible";
/// Static OpenAI API-key default used when live model discovery is unavailable.
pub const API_DEFAULT_CHAT_MODEL_ID: &str = "gpt-5.5";
const API_CURATED_DEFAULT_ORDER: &[&str] =
    &["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-4.1", "gpt-4o"];
const DEFAULT_OPENAI_RESPONSES_INSTRUCTIONS: &str = "You are a helpful assistant.";

/// Selects the safest curated OpenAI API chat default from live discovery rows.
#[must_use]
pub fn select_api_preferred_model(
    models: &[DiscoveredProviderModel],
) -> Option<&DiscoveredProviderModel> {
    let candidates = models
        .iter()
        .filter(|model| model.can_be_chat_default())
        .filter(|model| !is_dynamic_chat_alias(model.id.as_str()))
        .filter(|model| !is_expensive_or_snapshot_default(model.id.as_str()))
        .collect::<Vec<_>>();
    API_CURATED_DEFAULT_ORDER.iter().find_map(|preferred| {
        candidates.iter().copied().find(|model| model_id_matches(model.id.as_str(), preferred))
    })
}

fn is_dynamic_chat_alias(model_id: &str) -> bool {
    let normalized = model_id.trim().to_ascii_lowercase();
    normalized == "chat-latest" || normalized.ends_with("/chat-latest")
}

fn is_expensive_or_snapshot_default(model_id: &str) -> bool {
    let normalized = model_terminal_id(model_id).to_ascii_lowercase();
    is_pro_model(normalized.as_str()) || has_date_snapshot_suffix(normalized.as_str())
}

fn model_id_matches(model_id: &str, expected: &str) -> bool {
    model_terminal_id(model_id).eq_ignore_ascii_case(expected)
}

fn model_terminal_id(model_id: &str) -> &str {
    model_id.trim().rsplit('/').next().unwrap_or_default().trim()
}

fn is_pro_model(model_id: &str) -> bool {
    model_id.split('-').any(|part| part == "pro")
}

fn has_date_snapshot_suffix(model_id: &str) -> bool {
    let bytes = model_id.as_bytes();
    if bytes.len() < 11 || bytes[bytes.len() - 11] != b'-' {
        return false;
    }
    let suffix = &bytes[bytes.len() - 10..];
    suffix[4] == b'-'
        && suffix[7] == b'-'
        && suffix
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

pub(crate) fn chat_capabilities() -> ProviderCapabilitiesSnapshot {
    ProviderCapabilitiesSnapshot {
        streaming_tokens: true,
        tool_calls: true,
        json_mode: true,
        vision: true,
        audio_transcribe: true,
        embeddings: false,
        reasoning: false,
        reasoning_efforts: Vec::new(),
        service_tier: true,
        service_tiers: default_service_tiers(),
        max_context_tokens: Some(128_000),
        cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
        latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
        recommended_use_cases: vec![
            "general chat".to_owned(),
            "JSON workflows".to_owned(),
            "vision requests".to_owned(),
        ],
        known_limitations: vec![],
        operator_override: false,
        metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
    }
}

pub(crate) fn embeddings_capabilities() -> ProviderCapabilitiesSnapshot {
    ProviderCapabilitiesSnapshot {
        streaming_tokens: false,
        tool_calls: false,
        json_mode: false,
        vision: false,
        audio_transcribe: false,
        embeddings: true,
        reasoning: false,
        reasoning_efforts: Vec::new(),
        service_tier: false,
        service_tiers: Vec::new(),
        max_context_tokens: Some(8_192),
        cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
        latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
        recommended_use_cases: vec!["memory indexing".to_owned()],
        known_limitations: vec!["text embeddings only".to_owned()],
        operator_override: false,
        metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
    }
}

pub(crate) fn audio_transcription_capabilities() -> ProviderCapabilitiesSnapshot {
    ProviderCapabilitiesSnapshot {
        streaming_tokens: false,
        tool_calls: false,
        json_mode: false,
        vision: false,
        audio_transcribe: true,
        embeddings: false,
        reasoning: false,
        reasoning_efforts: Vec::new(),
        service_tier: false,
        service_tiers: Vec::new(),
        max_context_tokens: None,
        cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
        latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
        recommended_use_cases: vec!["audio ingestion".to_owned()],
        known_limitations: vec![],
        operator_override: false,
        metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
    }
}

fn default_service_tiers() -> Vec<String> {
    [
        ProviderServiceTier::Auto,
        ProviderServiceTier::Default,
        ProviderServiceTier::Priority,
        ProviderServiceTier::Flex,
    ]
    .into_iter()
    .map(ProviderServiceTier::as_str)
    .map(str::to_owned)
    .collect()
}

/// Builds the OpenAI-compatible chat-completions request body.
///
/// `tools` must already be projected into the OpenAI-compatible tool schema by
/// the caller. This keeps daemon tool-catalog ownership outside the provider
/// crate while centralizing the provider wire shape here.
#[must_use]
pub fn chat_completions_payload(
    request: &ProviderRequest,
    model_name: &str,
    tools: Vec<Value>,
) -> Value {
    let mut body = json!({
        "model": model_name,
        "messages": build_openai_messages(request),
        "stream": false,
    });
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools);
        body["tool_choice"] = json!("auto");
    }
    if request.json_mode {
        body["response_format"] = json!({"type":"json_object"});
    }
    if let Some(max_output_tokens) = request.max_output_tokens {
        body["max_tokens"] = json!(max_output_tokens.max(1));
    }
    if let Some(reasoning_effort) = openai_reasoning_effort_for_model(request, model_name) {
        body["reasoning_effort"] = json!(reasoning_effort.as_str());
    }
    if let Some(service_tier) = request.service_tier {
        body["service_tier"] = json!(service_tier.as_str());
    }
    if let Some(prompt_cache_key) = openai_prompt_cache_key(request) {
        body["prompt_cache_key"] = json!(prompt_cache_key);
    }
    body
}

/// Responses API payload plus the original-to-wire tool-name map used for
/// decoding function-call output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsesPayload {
    /// Serialized JSON body to send to the Responses endpoint.
    pub body: Value,
    /// Map from original Palyra tool names to Responses-safe wire names.
    pub tool_wire_names: HashMap<String, String>,
}

/// Builds the OpenAI Responses request body used by ChatGPT/Codex OAuth.
///
/// `tools` must already be projected into the OpenAI-compatible function schema
/// by the caller. The returned `tool_wire_names` must be retained by the runtime
/// so function-call responses can be mapped back to original Palyra tool names.
#[must_use]
pub fn responses_payload(
    request: &ProviderRequest,
    model_name: &str,
    tools: Vec<Value>,
) -> ResponsesPayload {
    let tool_wire_names = responses_tool_wire_name_map_from_tools(&tools);
    let response_tools = responses_tools(tools, &tool_wire_names);
    let (input, instructions) =
        build_openai_responses_input_and_instructions(request, &tool_wire_names);
    let mut body = json!({
        "model": model_name,
        "input": input,
        "instructions": instructions.unwrap_or_else(|| DEFAULT_OPENAI_RESPONSES_INSTRUCTIONS.to_owned()),
        "stream": true,
        "store": false,
    });
    if !response_tools.is_empty() {
        body["tools"] = Value::Array(response_tools);
        body["tool_choice"] = json!("auto");
    }
    if let Some(reasoning_effort) = openai_reasoning_effort_for_model(request, model_name) {
        body["reasoning"] = json!({
            "effort": reasoning_effort.as_str(),
            "summary": "auto",
        });
    }
    if let Some(service_tier) = request.service_tier {
        body["service_tier"] = json!(service_tier.as_str());
    }
    if let Some(prompt_cache_key) = openai_prompt_cache_key(request) {
        body["prompt_cache_key"] = json!(prompt_cache_key);
    }
    ResponsesPayload { body, tool_wire_names }
}

fn openai_prompt_cache_key(request: &ProviderRequest) -> Option<String> {
    if !request.prompt_cache_policy.enabled {
        return None;
    }
    let report = request.prompt_cache_report.as_ref()?;
    if report.cacheable_tokens == 0 {
        return None;
    }
    if let (Some(stable_prefix_hash), Some(cache_scope_hash)) =
        (report.stable_prefix_hash.as_deref(), report.cache_scope_hash.as_deref())
    {
        let digest = sha256_hex(
            format!(
                "openai_prompt_cache_key:v1:{}:{}:{}:{}",
                cache_scope_hash,
                stable_prefix_hash,
                report.prompt_cache_epoch,
                report.provider_cache_strategy
            )
            .as_bytes(),
        );
        return Some(format!("palyra:{}", &digest[..32]));
    }
    let hash_prefix =
        report.provider_request_hash.get(..32).unwrap_or(report.provider_request_hash.as_str());
    Some(format!("palyra:{hash_prefix}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn openai_reasoning_effort_for_model(
    request: &ProviderRequest,
    model_name: &str,
) -> Option<ProviderReasoningEffort> {
    let effort = request.reasoning_effort?;
    openai_model_supports_reasoning(model_name).then_some(effort)
}

fn openai_model_supports_reasoning(model_name: &str) -> bool {
    model_id_supports_reasoning_effort(model_name)
}

/// Builds the original-to-wire tool-name map for the Responses dialect from
/// OpenAI-compatible function tool schemas.
#[must_use]
pub fn responses_tool_wire_name_map_from_tools(tools: &[Value]) -> HashMap<String, String> {
    let mut used_wire_names = HashSet::new();
    let mut tool_wire_names = HashMap::new();
    for tool in tools {
        let Some(name) = openai_compatible_tool_name(tool) else {
            continue;
        };
        let wire_name = unique_openai_responses_tool_name(name, &mut used_wire_names);
        tool_wire_names.insert(name.to_owned(), wire_name);
    }
    tool_wire_names
}

fn build_openai_message_content(
    message: &ProviderMessage,
    extra_vision_inputs: &[ProviderImageInput],
) -> Value {
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
                parts.push(openai_image_part(image));
            }
        }
    }
    for image in extra_vision_inputs {
        parts.push(openai_image_part(image));
    }

    if parts.len() == 1 {
        if let Some(text) = parts[0].get("text").and_then(Value::as_str) {
            return Value::String(text.to_owned());
        }
    }
    Value::Array(parts)
}

fn openai_image_part(image: &ProviderImageInput) -> Value {
    json!({
        "type": "image_url",
        "image_url": {
            "url": format!("data:{};base64,{}", image.mime_type, image.bytes_base64),
            "detail": "low",
        }
    })
}

fn build_openai_messages(request: &ProviderRequest) -> Vec<Value> {
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
    messages
        .iter()
        .map(|message| {
            let mut payload = json!({
                "role": message.role.as_openai_role(),
                "content": build_openai_message_content(message, &[]),
            });
            if message.role == ProviderMessageRole::Assistant && !message.tool_calls.is_empty() {
                payload["tool_calls"] = Value::Array(
                    message
                        .tool_calls
                        .iter()
                        .map(|tool_call| {
                            json!({
                                "id": tool_call.proposal_id.as_str(),
                                "type": "function",
                                "function": {
                                    "name": tool_call.tool_name.as_str(),
                                    "arguments": serde_json::to_string(&tool_call.input_json)
                                        .unwrap_or_else(|_| "{}".to_owned()),
                                }
                            })
                        })
                        .collect(),
                );
                if message.content.is_empty() {
                    payload["content"] = Value::Null;
                }
            }
            if let Some(name) = message.name.as_deref() {
                payload["name"] = json!(name);
            }
            if let Some(tool_call_id) = message.tool_call_id.as_deref() {
                payload["tool_call_id"] = json!(tool_call_id);
            }
            payload
        })
        .collect()
}

fn build_openai_responses_input_and_instructions(
    request: &ProviderRequest,
    tool_wire_names: &HashMap<String, String>,
) -> (Vec<Value>, Option<String>) {
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

    let mut instructions = Vec::new();
    let mut input = Vec::new();
    for message in messages {
        match message.role {
            ProviderMessageRole::System | ProviderMessageRole::Developer => {
                let text = message.text_content();
                if !text.trim().is_empty() {
                    instructions.push(text);
                }
            }
            ProviderMessageRole::User | ProviderMessageRole::Assistant => {
                push_openai_responses_message(&mut input, &message);
                if message.role == ProviderMessageRole::Assistant {
                    for tool_call in &message.tool_calls {
                        let tool_name = tool_wire_names
                            .get(tool_call.tool_name.as_str())
                            .cloned()
                            .unwrap_or_else(|| {
                                responses_safe_tool_name(tool_call.tool_name.as_str())
                            });
                        input.push(json!({
                            "type": "function_call",
                            "call_id": tool_call.proposal_id.as_str(),
                            "name": tool_name,
                            "arguments": serde_json::to_string(&tool_call.input_json)
                                .unwrap_or_else(|_| "{}".to_owned()),
                        }));
                    }
                }
            }
            ProviderMessageRole::Tool => {
                let Some(tool_call_id) = message.tool_call_id.as_deref() else {
                    continue;
                };
                if tool_call_id.trim().is_empty() {
                    continue;
                }
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": tool_call_id,
                    "output": message.text_content(),
                }));
            }
        }
    }

    if input.is_empty() {
        input.push(json!({
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": request.input_text.as_str(),
            }],
        }));
    }

    (input, (!instructions.is_empty()).then(|| instructions.join("\n\n")))
}

fn push_openai_responses_message(input: &mut Vec<Value>, message: &ProviderMessage) {
    let content = build_openai_responses_content_parts(message);
    if content.is_empty() {
        return;
    }
    input.push(json!({
        "role": message.role.as_openai_role(),
        "content": content,
    }));
}

fn build_openai_responses_content_parts(message: &ProviderMessage) -> Vec<Value> {
    let mut parts = Vec::new();
    for content_part in &message.content {
        match content_part {
            ProviderMessageContentPart::Text { text } => {
                if text.is_empty() {
                    continue;
                }
                let part_type = if message.role == ProviderMessageRole::Assistant {
                    "output_text"
                } else {
                    "input_text"
                };
                parts.push(json!({
                    "type": part_type,
                    "text": text,
                }));
            }
            ProviderMessageContentPart::Image { image } => {
                parts.push(json!({
                    "type": "input_image",
                    "image_url": format!("data:{};base64,{}", image.mime_type, image.bytes_base64),
                    "detail": "low",
                }));
            }
        }
    }
    parts
}

fn responses_tools(tools: Vec<Value>, tool_wire_names: &HashMap<String, String>) -> Vec<Value> {
    tools.into_iter().filter_map(|tool| responses_tool(tool, tool_wire_names)).collect()
}

fn responses_tool(tool: Value, tool_wire_names: &HashMap<String, String>) -> Option<Value> {
    let function = tool.get("function")?;
    let name = openai_compatible_tool_name(&tool)?;
    let wire_name =
        tool_wire_names.get(name).cloned().unwrap_or_else(|| responses_safe_tool_name(name));
    Some(json!({
        "type": "function",
        "name": wire_name,
        "description": function.get("description").cloned().unwrap_or_else(|| json!("")),
        "parameters": function.get("parameters").cloned().unwrap_or_else(|| json!({})),
    }))
}

fn openai_compatible_tool_name(tool: &Value) -> Option<&str> {
    tool.get("function")?
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
}

fn unique_openai_responses_tool_name(
    original_name: &str,
    used_wire_names: &mut HashSet<String>,
) -> String {
    let base = responses_safe_tool_name(original_name);
    if used_wire_names.insert(base.clone()) {
        return base;
    }
    let mut suffix = 2_u64;
    loop {
        let candidate = format!("{base}_{suffix}");
        if used_wire_names.insert(candidate.clone()) {
            return candidate;
        }
        suffix = suffix.saturating_add(1);
    }
}

fn responses_safe_tool_name(original_name: &str) -> String {
    let safe_name = original_name
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if safe_name.is_empty() {
        "tool".to_owned()
    } else {
        safe_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reasoning_request(effort: ProviderReasoningEffort) -> ProviderRequest {
        let mut request =
            ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None);
        request.reasoning_effort = Some(effort);
        request
    }

    fn service_tier_request(tier: ProviderServiceTier) -> ProviderRequest {
        let mut request =
            ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None);
        request.service_tier = Some(tier);
        request
    }

    fn prompt_cache_request() -> ProviderRequest {
        let mut request = ProviderRequest::from_input_text(
            "secret user prompt should not appear in cache key".to_owned(),
            false,
            Vec::new(),
            None,
        );
        request.prompt_cache_policy.enabled = true;
        request.prompt_cache_policy.strategy = crate::PromptCacheStrategy::StablePrefix;
        request.prompt_cache_report = Some(crate::PromptCacheReport {
            eligible_bytes: 128,
            invalidated_bytes: 32,
            invalidation_reasons: vec!["current_turn_changes".to_owned()],
            provider_request_hash:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            requested_strategy: crate::PromptCacheStrategy::StablePrefix,
            applied_strategy: "metadata_only".to_owned(),
            breakpoint_count: 2,
            cacheable_tokens: 32,
            actual_cached_tokens: None,
            prompt_cache_epoch: 42,
            stable_prefix_hash: Some("stable-prefix-hash".to_owned()),
            cache_scope_hash: Some("cache-scope-hash".to_owned()),
            tool_catalog_hash: Some("tool-catalog-hash".to_owned()),
            memory_snapshot_hash: Some("memory-snapshot-hash".to_owned()),
            provider_cache_strategy: "openai_prompt_cache_key".to_owned(),
        });
        request
    }

    #[test]
    fn chat_completions_payload_adds_reasoning_effort_for_reasoning_model() {
        let payload = chat_completions_payload(
            &reasoning_request(ProviderReasoningEffort::Low),
            "gpt-5.5",
            Vec::new(),
        );

        assert_eq!(payload["reasoning_effort"], "low");
    }

    #[test]
    fn responses_payload_adds_reasoning_object_for_reasoning_model() {
        let payload = responses_payload(
            &reasoning_request(ProviderReasoningEffort::XHigh),
            "gpt-5.5",
            Vec::new(),
        );

        assert_eq!(payload.body["reasoning"]["effort"], "xhigh");
        assert_eq!(payload.body["reasoning"]["summary"], "auto");
    }

    #[test]
    fn openai_payloads_omit_reasoning_for_non_reasoning_model() {
        let request = reasoning_request(ProviderReasoningEffort::High);

        let chat_payload = chat_completions_payload(&request, "gpt-4o-mini", Vec::new());
        let responses_payload = responses_payload(&request, "gpt-4o-mini", Vec::new());

        assert!(chat_payload.get("reasoning_effort").is_none());
        assert!(responses_payload.body.get("reasoning").is_none());
    }

    #[test]
    fn openai_payloads_add_service_tier() {
        let request = service_tier_request(ProviderServiceTier::Priority);

        let chat_payload = chat_completions_payload(&request, "gpt-5.5", Vec::new());
        let responses_payload = responses_payload(&request, "gpt-5.5", Vec::new());

        assert_eq!(chat_payload["service_tier"], "priority");
        assert_eq!(responses_payload.body["service_tier"], "priority");
    }

    #[test]
    fn openai_payloads_emit_redacted_prompt_cache_key() {
        let request = prompt_cache_request();

        let chat_payload = chat_completions_payload(&request, "gpt-5.5", Vec::new());
        let responses_payload = responses_payload(&request, "gpt-5.5", Vec::new());

        let expected = format!(
            "palyra:{}",
            &sha256_hex(
                "openai_prompt_cache_key:v1:cache-scope-hash:stable-prefix-hash:42:openai_prompt_cache_key"
                    .as_bytes()
            )[..32]
        );

        assert_eq!(chat_payload["prompt_cache_key"], expected);
        assert_eq!(responses_payload.body["prompt_cache_key"], chat_payload["prompt_cache_key"]);
        assert!(!chat_payload["prompt_cache_key"].as_str().unwrap().contains("secret user prompt"));
    }

    #[test]
    fn openai_prompt_cache_key_ignores_current_turn_text() {
        let first = prompt_cache_request();
        let mut second = prompt_cache_request();
        second.input_text = "different current turn".to_owned();

        let first_payload = chat_completions_payload(&first, "gpt-5.5", Vec::new());
        let second_payload = chat_completions_payload(&second, "gpt-5.5", Vec::new());

        assert_eq!(first_payload["prompt_cache_key"], second_payload["prompt_cache_key"]);
    }

    #[test]
    fn openai_payloads_omit_prompt_cache_key_when_disabled() {
        let mut request = prompt_cache_request();
        request.prompt_cache_policy.enabled = false;

        let payload = chat_completions_payload(&request, "gpt-5.5", Vec::new());

        assert!(payload.get("prompt_cache_key").is_none());
    }
}
