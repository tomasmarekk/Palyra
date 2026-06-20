//! Compatibility wrappers around provider-crate chat payload builders.
//!
//! The daemon still owns tool-catalog projection because catalog snapshots are
//! daemon application data. Provider-specific wire payload construction lives
//! in `palyra-model-providers`.

use std::collections::HashMap;

use serde_json::Value;

use crate::application::tool_registry::{provider_tools_from_catalog_snapshot, ToolSchemaDialect};

use super::ProviderRequest;

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
        let tools = provider_tools_for_request(request, ToolSchemaDialect::OpenAiCompatible);
        palyra_model_providers::openai_chat_completions_payload(request, model_name, tools)
    }
}

/// Adapter for the OpenAI Responses dialect used by ChatGPT/Codex OAuth.
pub(super) struct OpenAiResponsesChatAdapter;

impl OpenAiResponsesChatAdapter {
    /// Serializes `request` into the Responses API payload targeting
    /// `model_name`.
    pub(super) fn request_payload(&self, request: &ProviderRequest, model_name: &str) -> Value {
        let tools = provider_tools_for_request(request, ToolSchemaDialect::OpenAiCompatible);
        palyra_model_providers::openai_responses_payload(request, model_name, tools).body
    }
}

pub(super) fn openai_responses_tool_wire_name_map(snapshot: &Value) -> HashMap<String, String> {
    let tools = provider_tools_from_catalog_snapshot(snapshot, ToolSchemaDialect::OpenAiCompatible);
    palyra_model_providers::openai_responses_tool_wire_name_map_from_tools(&tools)
}

/// Adapter for the Anthropic messages dialect (Anthropic and MiniMax).
pub(super) struct AnthropicCompatibleChatAdapter;

impl ProviderChatAdapter for AnthropicCompatibleChatAdapter {
    fn request_payload(&self, request: &ProviderRequest, model_name: &str) -> Value {
        let tools = provider_tools_for_request(request, ToolSchemaDialect::Anthropic);
        palyra_model_providers::anthropic_messages_payload(request, model_name, tools)
    }
}

fn provider_tools_for_request(request: &ProviderRequest, dialect: ToolSchemaDialect) -> Vec<Value> {
    request
        .tool_catalog_snapshot
        .as_ref()
        .map(|snapshot| provider_tools_from_catalog_snapshot(snapshot, dialect))
        .unwrap_or_default()
}
