//! OpenAI-compatible provider identity and static capability defaults.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use crate::config::{
    ProviderCapabilitiesSnapshot, ProviderCostTier, ProviderLatencyTier, ProviderMetadataSource,
};
use crate::contract::{
    model_id_supports_reasoning_effort, ProviderImageInput, ProviderMessage,
    ProviderMessageContentPart, ProviderMessageRole, ProviderReasoningEffort, ProviderRequest,
};

pub(crate) const PROVIDER_ID: &str = "openai-primary";
pub(crate) const DISPLAY_NAME: &str = "OpenAI-compatible";
const DEFAULT_OPENAI_RESPONSES_INSTRUCTIONS: &str = "You are a helpful assistant.";

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
        max_context_tokens: None,
        cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
        latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
        recommended_use_cases: vec!["audio ingestion".to_owned()],
        known_limitations: vec![],
        operator_override: false,
        metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
    }
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
    ResponsesPayload { body, tool_wire_names }
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
}
