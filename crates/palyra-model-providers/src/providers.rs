//! Provider-specific identity and capability defaults.
//!
//! The legacy config shape still exposes transport families such as
//! `openai_compatible` and `anthropic`. This module keeps provider identities
//! such as xAI, OpenRouter, Gemini, and MiniMax separated so their behavior can
//! diverge without growing the config normalizer into a provider monolith.

use crate::config::{
    ModelProviderAuthProviderKind, ModelProviderKind, ProviderCapabilitiesSnapshot,
    ProviderCostTier, ProviderLatencyTier, ProviderMetadataSource, ProviderModelRole,
};
use serde_json::{json, Value};

#[path = "providers/antropic.rs"]
pub mod anthropic;
pub mod google;
pub mod minimax;
pub mod openai;
pub mod openrouter;
pub mod xai;

pub use anthropic::{
    anthropic_compatible_uses_anthropic_oauth_headers, anthropic_compatible_uses_bearer_auth,
    messages_payload as anthropic_messages_payload,
};
pub use minimax::{coerce_raw_tool_call_markup, RawToolCallMarkupExtraction};
pub use openai::{
    chat_completions_payload as openai_chat_completions_payload,
    responses_payload as openai_responses_payload,
    responses_tool_wire_name_map_from_tools as openai_responses_tool_wire_name_map_from_tools,
    select_api_preferred_model as select_openai_api_preferred_model,
    ResponsesPayload as OpenAiResponsesPayload,
    API_DEFAULT_CHAT_MODEL_ID as OPENAI_API_DEFAULT_CHAT_MODEL_ID,
};
pub use xai::{
    is_trusted_oauth_host as is_trusted_xai_oauth_host,
    normalize_oauth_endpoint as normalize_xai_oauth_endpoint, API_BASE_URL as XAI_DEFAULT_BASE_URL,
    DEFAULT_CHAT_MODEL_ID as XAI_DEFAULT_CHAT_MODEL_ID,
    GROK_OAUTH_BASE_URL as XAI_GROK_OAUTH_BASE_URL,
    OAUTH_CALLBACK_CORS_ORIGIN_ALLOWLIST as XAI_OAUTH_CALLBACK_CORS_ORIGIN_ALLOWLIST,
    OAUTH_CALLBACK_HOST as XAI_OAUTH_CALLBACK_HOST, OAUTH_CALLBACK_PATH as XAI_OAUTH_CALLBACK_PATH,
    OAUTH_CALLBACK_PORT as XAI_OAUTH_CALLBACK_PORT, OAUTH_CLIENT_ID as XAI_OAUTH_CLIENT_ID,
    OAUTH_DISCOVERY_URL as XAI_OAUTH_DISCOVERY_URL, OAUTH_ISSUER as XAI_OAUTH_ISSUER,
    OAUTH_REDIRECT_URI as XAI_OAUTH_REDIRECT_URI, OAUTH_SCOPE as XAI_OAUTH_SCOPE,
};

const DETERMINISTIC_PROVIDER_ID: &str = "deterministic-primary";
const DETERMINISTIC_DISPLAY_NAME: &str = "Deterministic";

/// Maximum serialized tool-argument payload accepted from provider responses.
pub const MAX_TOOL_ARGUMENT_BYTES: usize = 512 * 1024;

/// Returns the synthesized provider id used when legacy flat config is
/// normalized into a registry entry.
#[must_use]
pub fn legacy_provider_id(
    kind: ModelProviderKind,
    auth_provider_kind: Option<ModelProviderAuthProviderKind>,
) -> &'static str {
    match kind {
        ModelProviderKind::Deterministic => DETERMINISTIC_PROVIDER_ID,
        ModelProviderKind::OpenAiCompatible => match auth_provider_kind {
            Some(ModelProviderAuthProviderKind::Xai) => xai::PROVIDER_ID,
            Some(
                ModelProviderAuthProviderKind::GoogleGemini
                | ModelProviderAuthProviderKind::GoogleGeminiCli,
            ) => google::PROVIDER_ID,
            Some(ModelProviderAuthProviderKind::Openrouter) => openrouter::PROVIDER_ID,
            _ => openai::PROVIDER_ID,
        },
        ModelProviderKind::Anthropic => match auth_provider_kind {
            Some(ModelProviderAuthProviderKind::Minimax) => minimax::PROVIDER_ID,
            _ => anthropic::PROVIDER_ID,
        },
    }
}

/// Returns the display name synthesized for legacy flat config providers.
#[must_use]
pub fn legacy_display_name(
    kind: ModelProviderKind,
    auth_provider_kind: Option<ModelProviderAuthProviderKind>,
) -> &'static str {
    match kind {
        ModelProviderKind::Deterministic => DETERMINISTIC_DISPLAY_NAME,
        ModelProviderKind::OpenAiCompatible => match auth_provider_kind {
            Some(ModelProviderAuthProviderKind::Xai) => xai::DISPLAY_NAME,
            Some(
                ModelProviderAuthProviderKind::GoogleGemini
                | ModelProviderAuthProviderKind::GoogleGeminiCli,
            ) => google::DISPLAY_NAME,
            Some(ModelProviderAuthProviderKind::Openrouter) => openrouter::DISPLAY_NAME,
            _ => openai::DISPLAY_NAME,
        },
        ModelProviderKind::Anthropic => match auth_provider_kind {
            Some(ModelProviderAuthProviderKind::Minimax) => minimax::DISPLAY_NAME,
            _ => anthropic::DISPLAY_NAME,
        },
    }
}

/// Returns the registry auth-provider kind used when legacy flat config does
/// not define one explicitly.
#[must_use]
pub fn default_auth_provider_kind(
    kind: ModelProviderKind,
    auth_provider_kind: Option<ModelProviderAuthProviderKind>,
) -> Option<ModelProviderAuthProviderKind> {
    match kind {
        ModelProviderKind::Deterministic => None,
        ModelProviderKind::OpenAiCompatible => {
            Some(auth_provider_kind.unwrap_or(ModelProviderAuthProviderKind::Openai))
        }
        ModelProviderKind::Anthropic => {
            Some(auth_provider_kind.unwrap_or(ModelProviderAuthProviderKind::Anthropic))
        }
    }
}

/// Returns the default capability envelope for a transport family and model role.
#[must_use]
pub fn capability_defaults_for_kind(
    kind: ModelProviderKind,
    role: ProviderModelRole,
) -> ProviderCapabilitiesSnapshot {
    match role {
        ProviderModelRole::Chat => match kind {
            ModelProviderKind::Deterministic => deterministic_chat_capabilities(),
            ModelProviderKind::OpenAiCompatible => openai::chat_capabilities(),
            ModelProviderKind::Anthropic => anthropic::chat_capabilities(),
        },
        ProviderModelRole::Embeddings => openai::embeddings_capabilities(),
        ProviderModelRole::AudioTranscription => openai::audio_transcription_capabilities(),
    }
}

/// Returns default capabilities adjusted for provider identity quirks.
#[must_use]
pub fn capability_defaults_for_provider(
    kind: ModelProviderKind,
    role: ProviderModelRole,
    auth_provider_kind: Option<ModelProviderAuthProviderKind>,
) -> ProviderCapabilitiesSnapshot {
    match (kind, role, auth_provider_kind) {
        (
            ModelProviderKind::OpenAiCompatible,
            ProviderModelRole::Chat,
            Some(
                ModelProviderAuthProviderKind::Xai
                | ModelProviderAuthProviderKind::GoogleGemini
                | ModelProviderAuthProviderKind::GoogleGeminiCli
                | ModelProviderAuthProviderKind::Openrouter,
            ),
        ) => service_tier_disabled(capability_defaults_for_kind(kind, role)),
        (
            ModelProviderKind::Anthropic,
            ProviderModelRole::Chat,
            Some(ModelProviderAuthProviderKind::Minimax),
        ) => minimax::chat_capabilities(),
        _ => capability_defaults_for_kind(kind, role),
    }
}

fn service_tier_disabled(
    mut capabilities: ProviderCapabilitiesSnapshot,
) -> ProviderCapabilitiesSnapshot {
    capabilities.service_tier = false;
    capabilities.service_tiers.clear();
    capabilities
}

fn deterministic_chat_capabilities() -> ProviderCapabilitiesSnapshot {
    ProviderCapabilitiesSnapshot {
        streaming_tokens: true,
        tool_calls: true,
        json_mode: true,
        vision: false,
        audio_transcribe: false,
        embeddings: false,
        reasoning: false,
        reasoning_efforts: Vec::new(),
        service_tier: false,
        service_tiers: Vec::new(),
        max_context_tokens: Some(8_192),
        cost_tier: ProviderCostTier::Low.as_str().to_owned(),
        latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
        recommended_use_cases: vec![
            "offline testing".to_owned(),
            "scripted tool-call regression".to_owned(),
            "deterministic smoke flows".to_owned(),
        ],
        known_limitations: vec![
            "scripted fixture responses only".to_owned(),
            "no real provider auth".to_owned(),
            "no vision".to_owned(),
        ],
        operator_override: false,
        metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
    }
}

/// Normalizes raw provider tool arguments into JSON bytes.
///
/// Arguments that already parse as JSON are preserved byte-for-byte. Non-JSON
/// arguments are wrapped as `{"raw": ...}` so downstream consumers always
/// receive valid JSON. The input and normalized output are capped to avoid
/// oversized journal and tool dispatch payloads.
///
/// # Errors
/// Returns an error when the raw or normalized argument payload exceeds
/// [`MAX_TOOL_ARGUMENT_BYTES`].
pub fn normalize_tool_arguments(raw: &str) -> Result<Vec<u8>, String> {
    if raw.trim().is_empty() {
        return Ok(b"{}".to_vec());
    }
    if raw.len() > MAX_TOOL_ARGUMENT_BYTES {
        return Err(format!(
            "tool arguments exceed {MAX_TOOL_ARGUMENT_BYTES} bytes before normalization"
        ));
    }
    if serde_json::from_str::<Value>(raw).is_ok() {
        return Ok(raw.as_bytes().to_vec());
    }
    let normalized = json!({ "raw": raw }).to_string().into_bytes();
    if normalized.len() > MAX_TOOL_ARGUMENT_BYTES {
        return Err(format!(
            "tool arguments exceed {MAX_TOOL_ARGUMENT_BYTES} bytes after normalization"
        ));
    }
    Ok(normalized)
}

/// Serializes structured provider tool input into bounded JSON bytes.
///
/// # Errors
/// Returns an error when JSON serialization fails or the serialized payload
/// exceeds [`MAX_TOOL_ARGUMENT_BYTES`].
pub fn normalize_tool_input_value(value: &Value) -> Result<Vec<u8>, String> {
    let normalized = serde_json::to_vec(value)
        .map_err(|error| format!("tool arguments could not be serialized: {error}"))?;
    if normalized.len() > MAX_TOOL_ARGUMENT_BYTES {
        return Err(format!(
            "tool arguments exceed {MAX_TOOL_ARGUMENT_BYTES} bytes after serialization"
        ));
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::{normalize_tool_arguments, normalize_tool_input_value, MAX_TOOL_ARGUMENT_BYTES};
    use serde_json::json;

    #[test]
    fn normalize_tool_arguments_preserves_json_payload_bytes() {
        let raw = r#"{"path":"app.js"}"#;

        let normalized = normalize_tool_arguments(raw).expect("valid JSON should normalize");

        assert_eq!(normalized, raw.as_bytes());
    }

    #[test]
    fn normalize_tool_arguments_wraps_non_json_payload() {
        let normalized = normalize_tool_arguments("inspect app.js")
            .expect("plain text arguments should be wrapped");
        let parsed: serde_json::Value =
            serde_json::from_slice(normalized.as_slice()).expect("wrapped payload is JSON");

        assert_eq!(parsed, json!({ "raw": "inspect app.js" }));
    }

    #[test]
    fn normalize_tool_arguments_accepts_large_json_payload_within_limit() {
        let json_overhead = r#"{"text":""}"#.len();
        let payload =
            format!(r#"{{"text":"{}"}}"#, "a".repeat(MAX_TOOL_ARGUMENT_BYTES - json_overhead));

        let normalized = normalize_tool_arguments(payload.as_str())
            .expect("payload within byte limit should be accepted");

        assert_eq!(normalized.len(), MAX_TOOL_ARGUMENT_BYTES);
    }

    #[test]
    fn normalize_tool_arguments_rejects_oversized_payload() {
        let oversized = "a".repeat(MAX_TOOL_ARGUMENT_BYTES + 1);

        let error =
            normalize_tool_arguments(oversized.as_str()).expect_err("oversized payload must fail");

        assert!(error.contains("tool arguments exceed"), "error should mention byte limit");
    }

    #[test]
    fn normalize_tool_input_value_rejects_oversized_serialized_payload() {
        let oversized = json!({
            "text": "a".repeat(MAX_TOOL_ARGUMENT_BYTES + 1),
        });

        let error = normalize_tool_input_value(&oversized)
            .expect_err("oversized serialized payload must fail");

        assert!(error.contains("tool arguments exceed"), "error should mention byte limit");
    }
}
