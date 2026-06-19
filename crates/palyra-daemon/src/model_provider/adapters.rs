//! Request payload builders for provider chat dialects.
//!
//! [`ProviderChatAdapter`] turns a provider-neutral [`ProviderRequest`] into
//! the JSON body each HTTP backend posts. OpenAI-compatible chat completions
//! are a near-direct mapping; ChatGPT/Codex OAuth uses the Responses dialect;
//! Anthropic requires reshaping: system and developer turns move into the
//! top-level system block, tool results travel as user-turn content blocks
//! adjacent to their tool_use, and orphan tool results degrade to plain text.
//! Field names and value shapes here are wire contracts pinned by tests.
use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use crate::application::tool_registry::{provider_tools_from_catalog_snapshot, ToolSchemaDialect};

use super::{
    ProviderImageInput, ProviderMessage, ProviderMessageContentPart, ProviderMessageRole,
    ProviderMessageToolCall, ProviderRequest,
};

// Anthropic requires max_tokens on every request; this default applies when
// the caller sets no explicit output budget.
const DEFAULT_ANTHROPIC_MAX_OUTPUT_TOKENS: u64 = 4_096;
const DEFAULT_OPENAI_RESPONSES_INSTRUCTIONS: &str = "You are a helpful assistant.";

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

    // A single text part collapses to a plain string: maximally compatible
    // with OpenAI-likes that reject array content for text-only messages.
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

/// Builds the OpenAI-compatible `messages` array, attaching request-level
/// vision inputs to the most recent user turn (providers only accept images
/// inside user messages).
pub(super) fn build_openai_messages(request: &ProviderRequest) -> Vec<Value> {
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
                // Tool-call-only assistant turns must carry null content, not
                // an empty array, to satisfy strict OpenAI-compatible parsers.
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

// Anthropic rejects tool_result blocks whose tool_use_id has no matching
// tool_use in the previous assistant turn; orphaned results are downgraded
// to labeled plain text so the conversation still round-trips.
fn build_anthropic_orphan_tool_result_text_part(message: &ProviderMessage) -> Value {
    let tool_call_id = message.tool_call_id.as_deref().unwrap_or("unknown");
    json!({
        "type": "text",
        "text": format!("Tool result for {tool_call_id}:\n{}", message.text_content()),
    })
}

// Some Anthropic-compatible endpoints validate flattened content block order and reject
// `tool_use, tool_use, tool_result, tool_result`, even when official clients accept it.
// When an assistant turn with 2+ tool calls is followed by exactly matching tool results
// in order, expand it into interleaved assistant(tool_use)/user(tool_result) pairs and
// return how many tool result messages were consumed; otherwise return None and let the
// caller emit the message unchanged.
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

/// Builds the Anthropic `messages` array and the extracted system prompt.
///
/// Reshaping rules: system/developer turns concatenate into the returned
/// system string; consecutive tool results matching the preceding assistant
/// turn's tool calls stay adjacent (batched into one user turn, or expanded
/// pairwise for multi-tool exchanges); unmatched tool results become labeled
/// text; vision inputs attach to the most recent user turn.
pub(super) fn build_anthropic_messages_and_system(
    request: &ProviderRequest,
) -> (Vec<Value>, Option<String>) {
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
                // Results must answer the preceding assistant turn's tool
                // calls in order; matching results batch into one user turn,
                // anything else is treated as an orphan.
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

/// Builds the dialect-specific JSON body for one chat completion request.
pub(super) trait ProviderChatAdapter {
    /// Serializes `request` into the wire payload targeting `model_name`.
    fn request_payload(&self, request: &ProviderRequest, model_name: &str) -> Value;
}

/// Adapter for the OpenAI chat-completions dialect (also used by compatible
/// third-party endpoints).
pub(super) struct OpenAiCompatibleChatAdapter;

impl ProviderChatAdapter for OpenAiCompatibleChatAdapter {
    fn request_payload(&self, request: &ProviderRequest, model_name: &str) -> Value {
        let mut body = json!({
            "model": model_name,
            "messages": build_openai_messages(request),
            "stream": false,
        });
        if let Some(snapshot) = request.tool_catalog_snapshot.as_ref() {
            let tools =
                provider_tools_from_catalog_snapshot(snapshot, ToolSchemaDialect::OpenAiCompatible);
            if !tools.is_empty() {
                body["tools"] = Value::Array(tools);
                body["tool_choice"] = json!("auto");
            }
        }
        if request.json_mode {
            body["response_format"] = json!({"type":"json_object"});
        }
        if let Some(max_output_tokens) = request.max_output_tokens {
            body["max_tokens"] = json!(max_output_tokens.max(1));
        }
        body
    }
}

/// Adapter for the OpenAI Responses dialect used by ChatGPT/Codex OAuth.
pub(super) struct OpenAiResponsesChatAdapter;

impl OpenAiResponsesChatAdapter {
    /// Serializes `request` into the Responses API payload targeting
    /// `model_name`.
    pub(super) fn request_payload(&self, request: &ProviderRequest, model_name: &str) -> Value {
        let mut response_tools = Vec::new();
        let mut tool_wire_names = HashMap::new();
        if let Some(snapshot) = request.tool_catalog_snapshot.as_ref() {
            tool_wire_names = openai_responses_tool_wire_name_map(snapshot);
            let tools =
                provider_tools_from_catalog_snapshot(snapshot, ToolSchemaDialect::OpenAiCompatible);
            response_tools = openai_responses_tools(tools, &tool_wire_names);
        }
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
        body
    }
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
                                openai_responses_safe_tool_name(tool_call.tool_name.as_str())
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

pub(super) fn openai_responses_tool_wire_name_map(snapshot: &Value) -> HashMap<String, String> {
    let tools = provider_tools_from_catalog_snapshot(snapshot, ToolSchemaDialect::OpenAiCompatible);
    let mut used_wire_names = HashSet::new();
    let mut tool_wire_names = HashMap::new();
    for tool in tools {
        let Some(name) = openai_compatible_tool_name(&tool) else {
            continue;
        };
        let wire_name = unique_openai_responses_tool_name(name, &mut used_wire_names);
        tool_wire_names.insert(name.to_owned(), wire_name);
    }
    tool_wire_names
}

fn openai_responses_tools(
    tools: Vec<Value>,
    tool_wire_names: &HashMap<String, String>,
) -> Vec<Value> {
    tools.into_iter().filter_map(|tool| openai_responses_tool(tool, tool_wire_names)).collect()
}

fn openai_responses_tool(tool: Value, tool_wire_names: &HashMap<String, String>) -> Option<Value> {
    let function = tool.get("function")?;
    let name = openai_compatible_tool_name(&tool)?;
    let wire_name =
        tool_wire_names.get(name).cloned().unwrap_or_else(|| openai_responses_safe_tool_name(name));
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
    let base = openai_responses_safe_tool_name(original_name);
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

fn openai_responses_safe_tool_name(original_name: &str) -> String {
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

/// Adapter for the Anthropic messages dialect (Anthropic and MiniMax).
pub(super) struct AnthropicCompatibleChatAdapter;

impl ProviderChatAdapter for AnthropicCompatibleChatAdapter {
    fn request_payload(&self, request: &ProviderRequest, model_name: &str) -> Value {
        let (messages, system) = build_anthropic_messages_and_system(request);
        let max_tokens =
            request.max_output_tokens.unwrap_or(DEFAULT_ANTHROPIC_MAX_OUTPUT_TOKENS).max(1);
        let mut body = json!({
            "model": model_name,
            "max_tokens": max_tokens,
            "messages": messages,
        });
        if let Some(snapshot) = request.tool_catalog_snapshot.as_ref() {
            let tools =
                provider_tools_from_catalog_snapshot(snapshot, ToolSchemaDialect::Anthropic);
            if !tools.is_empty() {
                body["tools"] = Value::Array(tools);
            }
        }
        // Anthropic has no response_format parameter; JSON mode is enforced
        // through a system-prompt instruction instead.
        let system = if request.json_mode {
            Some(system.map_or_else(
                || "Return valid JSON only.".to_owned(),
                |existing| format!("{existing}\n\nReturn valid JSON only."),
            ))
        } else {
            system
        };
        if let Some(system) = system {
            body["system"] = json!(system);
        }
        body
    }
}
