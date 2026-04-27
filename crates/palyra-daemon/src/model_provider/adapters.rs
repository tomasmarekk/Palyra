use serde_json::{json, Value};

use crate::application::tool_registry::{provider_tools_from_catalog_snapshot, ToolSchemaDialect};

use super::{
    ProviderImageInput, ProviderMessage, ProviderMessageContentPart, ProviderMessageRole,
    ProviderRequest,
};

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
        let content = message.text_content();
        return vec![json!({
            "type": "tool_result",
            "tool_use_id": message.tool_call_id.as_deref().unwrap_or_default(),
            "content": content,
        })];
    }

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
    if message.role == ProviderMessageRole::Assistant {
        for tool_call in &message.tool_calls {
            parts.push(json!({
                "type": "tool_use",
                "id": tool_call.proposal_id.as_str(),
                "name": tool_call.tool_name.as_str(),
                "input": &tool_call.input_json,
            }));
        }
    }
    parts
}

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
    for message in &messages {
        match message.role {
            ProviderMessageRole::System | ProviderMessageRole::Developer => {
                let text = message.text_content();
                if !text.trim().is_empty() {
                    system_blocks.push(text);
                }
            }
            ProviderMessageRole::User
            | ProviderMessageRole::Assistant
            | ProviderMessageRole::Tool => {
                provider_messages.push(json!({
                    "role": message.role.as_anthropic_role(),
                    "content": build_anthropic_content_parts(message),
                }));
            }
        }
    }
    (provider_messages, (!system_blocks.is_empty()).then(|| system_blocks.join("\n\n")))
}

pub(super) trait ProviderChatAdapter {
    fn request_payload(&self, request: &ProviderRequest, model_name: &str) -> Value;
}

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
        body
    }
}

pub(super) struct AnthropicCompatibleChatAdapter;

impl ProviderChatAdapter for AnthropicCompatibleChatAdapter {
    fn request_payload(&self, request: &ProviderRequest, model_name: &str) -> Value {
        let (messages, system) = build_anthropic_messages_and_system(request);
        let mut body = json!({
            "model": model_name,
            "max_tokens": 2048,
            "messages": messages,
        });
        if let Some(snapshot) = request.tool_catalog_snapshot.as_ref() {
            let tools =
                provider_tools_from_catalog_snapshot(snapshot, ToolSchemaDialect::Anthropic);
            if !tools.is_empty() {
                body["tools"] = Value::Array(tools);
            }
        }
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
