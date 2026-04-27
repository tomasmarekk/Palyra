use serde_json::{json, Value};

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
