//! OpenAI-compatible model discovery helpers used by auth/profile selection.
//!
//! The helpers choose only from provider-advertised model IDs. They never
//! synthesize a fallback model name from local constants or model-id patterns.

use std::time::Duration;

use anyhow::Result;
use palyra_model_providers::{
    provider_models_endpoint, provider_models_endpoint_for_probe,
    select_preferred_discovered_model_id_from_response,
    select_preferred_tool_capable_discovered_model_id_from_response, ProviderModelsResponseFormat,
};
use reqwest::Url;

use crate::{
    bounded_http_body::{
        read_response_text_limited, MAX_PROVIDER_DISCOVERY_RESPONSE_BYTES,
        MAX_REMOTE_ERROR_RESPONSE_BYTES,
    },
    model_provider::{build_provider_http_client, sanitize_remote_error},
    openai_auth::OpenAiCredentialValidationError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenAiModelDiscoveryFormat {
    OpenAiCompatible,
    OpenAiCompatibleExplicitToolCapabilities,
    ChatGptCodex,
}

pub(crate) async fn discover_preferred_openai_compatible_model_id(
    base_url: &str,
    bearer_token: &str,
    allow_private_base_url: bool,
    timeout: Duration,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    let endpoint = provider_models_endpoint(base_url)
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    discover_preferred_openai_model_id_from_endpoint(
        endpoint,
        bearer_token,
        allow_private_base_url,
        timeout,
        OpenAiModelDiscoveryFormat::OpenAiCompatible,
    )
    .await
}

pub(crate) async fn discover_explicit_tool_capable_openai_compatible_model_id(
    base_url: &str,
    bearer_token: &str,
    allow_private_base_url: bool,
    timeout: Duration,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    let endpoint = provider_models_endpoint(base_url)
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    discover_preferred_openai_model_id_from_endpoint(
        endpoint,
        bearer_token,
        allow_private_base_url,
        timeout,
        OpenAiModelDiscoveryFormat::OpenAiCompatibleExplicitToolCapabilities,
    )
    .await
}

pub(crate) async fn discover_preferred_openai_chatgpt_codex_model_id(
    bearer_token: &str,
    timeout: Duration,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    let endpoint = provider_models_endpoint_for_probe("https://api.openai.com/v1", true)
        .map(|endpoint| endpoint.url)
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    discover_preferred_openai_model_id_from_endpoint(
        endpoint,
        bearer_token,
        false,
        timeout,
        OpenAiModelDiscoveryFormat::ChatGptCodex,
    )
    .await
}

async fn discover_preferred_openai_model_id_from_endpoint(
    endpoint: Url,
    bearer_token: &str,
    allow_private_base_url: bool,
    timeout: Duration,
    format: OpenAiModelDiscoveryFormat,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    let client = build_provider_http_client(&[endpoint.as_str()], allow_private_base_url, timeout)
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    let response = client
        .get(endpoint)
        .bearer_auth(bearer_token)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|_| OpenAiCredentialValidationError::ProviderUnavailable)?;
    let status = response.status();
    let body_limit = if status.is_success() {
        MAX_PROVIDER_DISCOVERY_RESPONSE_BYTES
    } else {
        MAX_REMOTE_ERROR_RESPONSE_BYTES
    };
    let body =
        match read_response_text_limited(response, body_limit, "provider model discovery").await {
            Ok(body) => body,
            Err(error) => {
                return Err(OpenAiCredentialValidationError::Unexpected(format!(
                    "model discovery response could not be read: {error}"
                )));
            }
        };
    if status.is_success() {
        return preferred_openai_model_id_from_body(body.as_str(), format).map_err(|error| {
            OpenAiCredentialValidationError::Unexpected(format!(
                "model discovery response could not be parsed: {error}"
            ))
        });
    }
    let sanitized = sanitize_remote_error(body.as_str());
    match status.as_u16() {
        401 | 403 => Err(OpenAiCredentialValidationError::InvalidCredential),
        429 => Err(OpenAiCredentialValidationError::RateLimited),
        500 | 502 | 503 | 504 => Err(OpenAiCredentialValidationError::ProviderUnavailable),
        _ => Err(OpenAiCredentialValidationError::Unexpected(format!(
            "model discovery endpoint returned status {}: {}",
            status.as_u16(),
            sanitized
        ))),
    }
}

fn preferred_openai_model_id_from_body(
    body: &str,
    format: OpenAiModelDiscoveryFormat,
) -> Result<Option<String>> {
    match format {
        OpenAiModelDiscoveryFormat::OpenAiCompatible => {
            select_preferred_discovered_model_id_from_response(
                body,
                ProviderModelsResponseFormat::OpenAiCompatible,
            )
        }
        OpenAiModelDiscoveryFormat::OpenAiCompatibleExplicitToolCapabilities => {
            select_preferred_tool_capable_discovered_model_id_from_response(body)
        }
        OpenAiModelDiscoveryFormat::ChatGptCodex => {
            select_preferred_discovered_model_id_from_response(
                body,
                ProviderModelsResponseFormat::OpenAiCodexBackend,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_model_discovery_prefers_provider_recency_metadata() {
        let body =
            r#"{"data":[{"id":"older","created":1700000000},{"id":"newer","created":1800000000}]}"#;

        let model_id =
            preferred_openai_model_id_from_body(body, OpenAiModelDiscoveryFormat::OpenAiCompatible)
                .expect("provider model response should parse");

        assert_eq!(model_id.as_deref(), Some("newer"));
    }

    #[test]
    fn openai_model_discovery_rejects_media_output_defaults() {
        let body = r#"{"data":[{"id":"grok-imagine-video-1.5","created":1800000000,"supported_parameters":["tools"],"architecture":{"output_modalities":["video"]}},{"id":"grok-4.3","created":1700000000,"supported_parameters":["tools"],"architecture":{"output_modalities":["text"]}}]}"#;

        let model_id =
            preferred_openai_model_id_from_body(body, OpenAiModelDiscoveryFormat::OpenAiCompatible)
                .expect("provider model response should parse");

        assert_eq!(model_id.as_deref(), Some("grok-4.3"));
    }

    #[test]
    fn openai_model_discovery_returns_none_for_media_only_inventory() {
        let body = r#"{"data":[{"id":"google/gemini-3-pro-image","created":1800000000,"architecture":{"output_modalities":["image"]}},{"id":"grok-imagine-video-1.5","created":1700000000,"architecture":{"output_modalities":["video"]}}]}"#;

        let model_id =
            preferred_openai_model_id_from_body(body, OpenAiModelDiscoveryFormat::OpenAiCompatible)
                .expect("provider model response should parse");

        assert_eq!(model_id, None);
    }

    #[test]
    fn openai_public_discovery_requires_explicit_tool_metadata_for_default() {
        let body = r#"{"data":[{"id":"gpt-realtime-whisper","created":1800000000},{"id":"gpt-chat-candidate","created":1700000000}]}"#;

        let model_id = preferred_openai_model_id_from_body(
            body,
            OpenAiModelDiscoveryFormat::OpenAiCompatibleExplicitToolCapabilities,
        )
        .expect("provider model response should parse");

        assert_eq!(model_id, None);
    }

    #[test]
    fn openai_public_discovery_prefers_explicit_tool_capable_model() {
        let body = r#"{"data":[{"id":"newer-non-tool","created":1800000000,"supported_parameters":["temperature"]},{"id":"tool-capable","created":1700000000,"supported_parameters":["tools","response_format"]}]}"#;

        let model_id = preferred_openai_model_id_from_body(
            body,
            OpenAiModelDiscoveryFormat::OpenAiCompatibleExplicitToolCapabilities,
        )
        .expect("provider model response should parse");

        assert_eq!(model_id.as_deref(), Some("tool-capable"));
    }

    #[test]
    fn openai_public_discovery_rejects_tool_capable_media_output_default() {
        let body = r#"{"data":[{"id":"google/gemini-3-pro-image","created":1800000000,"supported_parameters":["tools"],"architecture":{"output_modalities":["image"]}},{"id":"tool-capable","created":1700000000,"supported_parameters":["tools","response_format"],"architecture":{"output_modalities":["text"]}}]}"#;

        let model_id = preferred_openai_model_id_from_body(
            body,
            OpenAiModelDiscoveryFormat::OpenAiCompatibleExplicitToolCapabilities,
        )
        .expect("provider model response should parse");

        assert_eq!(model_id.as_deref(), Some("tool-capable"));
    }

    #[test]
    fn codex_model_discovery_prefers_provider_priority() {
        let body = r#"{"models":[{"slug":"secondary","priority":20},{"slug":"primary","priority":10},{"slug":"hidden","priority":1,"visibility":"hidden"}]}"#;

        let model_id =
            preferred_openai_model_id_from_body(body, OpenAiModelDiscoveryFormat::ChatGptCodex)
                .expect("Codex model response should parse");

        assert_eq!(model_id.as_deref(), Some("primary"));
    }
}
