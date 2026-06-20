//! OpenAI-compatible model discovery helpers used by auth/profile selection.
//!
//! The helpers choose only from provider-advertised model IDs. They never
//! synthesize a fallback model name from local constants or model-id patterns.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::{Client as ReqwestClient, Url};

use crate::{model_provider::sanitize_remote_error, openai_auth::OpenAiCredentialValidationError};

const OPENAI_CHATGPT_CODEX_MODELS_ENDPOINT: &str =
    "https://chatgpt.com/backend-api/codex/models?client_version=1.0.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenAiModelDiscoveryFormat {
    OpenAiCompatible,
    OpenAiCompatibleExplicitToolCapabilities,
    ChatGptCodex,
}

pub(crate) async fn discover_preferred_openai_compatible_model_id(
    base_url: &str,
    bearer_token: &str,
    timeout: Duration,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    let endpoint = openai_compatible_models_endpoint(base_url)
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    discover_preferred_openai_model_id_from_endpoint(
        endpoint,
        bearer_token,
        timeout,
        OpenAiModelDiscoveryFormat::OpenAiCompatible,
    )
    .await
}

pub(crate) async fn discover_explicit_tool_capable_openai_compatible_model_id(
    base_url: &str,
    bearer_token: &str,
    timeout: Duration,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    let endpoint = openai_compatible_models_endpoint(base_url)
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    discover_preferred_openai_model_id_from_endpoint(
        endpoint,
        bearer_token,
        timeout,
        OpenAiModelDiscoveryFormat::OpenAiCompatibleExplicitToolCapabilities,
    )
    .await
}

pub(crate) async fn discover_preferred_openai_chatgpt_codex_model_id(
    bearer_token: &str,
    timeout: Duration,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    let endpoint = Url::parse(OPENAI_CHATGPT_CODEX_MODELS_ENDPOINT)
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    discover_preferred_openai_model_id_from_endpoint(
        endpoint,
        bearer_token,
        timeout,
        OpenAiModelDiscoveryFormat::ChatGptCodex,
    )
    .await
}

async fn discover_preferred_openai_model_id_from_endpoint(
    endpoint: Url,
    bearer_token: &str,
    timeout: Duration,
    format: OpenAiModelDiscoveryFormat,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    let client = ReqwestClient::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    let response = client
        .get(endpoint)
        .bearer_auth(bearer_token)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|_| OpenAiCredentialValidationError::ProviderUnavailable)?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
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

fn openai_compatible_models_endpoint(base_url: &str) -> Result<Url> {
    let trimmed = base_url.trim().trim_end_matches('/');
    let raw = if trimmed.ends_with("/v1") || trimmed.ends_with("/openai") {
        format!("{trimmed}/models")
    } else {
        format!("{trimmed}/v1/models")
    };
    Url::parse(raw.as_str()).with_context(|| format!("invalid provider base_url: {base_url}"))
}

fn preferred_openai_model_id_from_body(
    body: &str,
    format: OpenAiModelDiscoveryFormat,
) -> Result<Option<String>> {
    match format {
        OpenAiModelDiscoveryFormat::OpenAiCompatible => {
            preferred_openai_compatible_model_id_from_body(body)
        }
        OpenAiModelDiscoveryFormat::OpenAiCompatibleExplicitToolCapabilities => {
            preferred_explicit_tool_capable_openai_compatible_model_id_from_body(body)
        }
        OpenAiModelDiscoveryFormat::ChatGptCodex => preferred_codex_model_id_from_body(body),
    }
}

fn preferred_openai_compatible_model_id_from_body(body: &str) -> Result<Option<String>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    let Some(entries) = value.get("data").and_then(serde_json::Value::as_array) else {
        return Ok(None);
    };
    let candidates = entries
        .iter()
        .filter_map(|entry| {
            let model_id = entry
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?;
            Some((model_id.to_owned(), model_recency_rank(entry)))
        })
        .collect::<Vec<_>>();
    Ok(preferred_model_id_from_candidates(candidates))
}

fn preferred_explicit_tool_capable_openai_compatible_model_id_from_body(
    body: &str,
) -> Result<Option<String>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    let Some(entries) = value.get("data").and_then(serde_json::Value::as_array) else {
        return Ok(None);
    };
    let candidates = entries
        .iter()
        .filter_map(|entry| {
            if model_supported_parameter(entry, &["tools", "tool_choice"]) != Some(true) {
                return None;
            }
            let model_id = entry
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?;
            Some((model_id.to_owned(), model_recency_rank(entry)))
        })
        .collect::<Vec<_>>();
    Ok(preferred_model_id_from_candidates(candidates))
}

fn preferred_codex_model_id_from_body(body: &str) -> Result<Option<String>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    let Some(entries) = value.get("models").and_then(serde_json::Value::as_array) else {
        return preferred_openai_compatible_model_id_from_body(body);
    };

    let mut candidates = entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            if codex_model_is_hidden(entry) {
                return None;
            }
            let model_id = entry
                .get("slug")
                .or_else(|| entry.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?;
            Some((codex_model_priority(entry), index, model_id.to_owned()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    Ok(candidates.into_iter().map(|(_, _, model_id)| model_id).next())
}

fn preferred_model_id_from_candidates(candidates: Vec<(String, Option<i64>)>) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }
    if candidates.iter().all(|(_, recency_rank)| recency_rank.is_some()) {
        return candidates
            .into_iter()
            .max_by_key(|(_, recency_rank)| recency_rank.unwrap_or(i64::MIN))
            .map(|(model_id, _)| model_id);
    }
    candidates.into_iter().map(|(model_id, _)| model_id).next()
}

fn model_recency_rank(entry: &serde_json::Value) -> Option<i64> {
    const RECENCY_FIELDS: &[&str] = &[
        "created",
        "created_at",
        "createdAt",
        "created_unix_ms",
        "createdUnixMs",
        "released_at",
        "releasedAt",
        "release_unix_ms",
        "releaseUnixMs",
    ];
    RECENCY_FIELDS
        .iter()
        .find_map(|field| entry.get(*field).and_then(model_recency_rank_from_value))
}

fn model_recency_rank_from_value(value: &serde_json::Value) -> Option<i64> {
    if let Some(raw) = value.as_i64() {
        return normalize_numeric_model_recency_rank(raw);
    }
    if let Some(raw) = value.as_u64().and_then(|raw| i64::try_from(raw).ok()) {
        return normalize_numeric_model_recency_rank(raw);
    }
    value
        .as_str()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .and_then(normalize_numeric_model_recency_rank)
}

fn normalize_numeric_model_recency_rank(raw: i64) -> Option<i64> {
    if raw <= 0 {
        return None;
    }
    if raw < 10_000_000_000 {
        Some(raw.saturating_mul(1_000))
    } else {
        Some(raw)
    }
}

fn model_supported_parameter(entry: &serde_json::Value, names: &[&str]) -> Option<bool> {
    let supported = entry.get("supported_parameters")?.as_array()?;
    let parameters = supported
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    Some(names.iter().any(|name| {
        let normalized = name.to_ascii_lowercase();
        parameters.iter().any(|parameter| parameter == &normalized)
    }))
}

fn codex_model_is_hidden(entry: &serde_json::Value) -> bool {
    entry
        .get("visibility")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .is_some_and(|visibility| matches!(visibility.as_str(), "hide" | "hidden"))
}

fn codex_model_priority(entry: &serde_json::Value) -> i64 {
    entry
        .get("priority")
        .and_then(|value| {
            value.as_i64().or_else(|| value.as_u64().and_then(|raw| i64::try_from(raw).ok()))
        })
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_model_discovery_prefers_provider_recency_metadata() {
        let body =
            r#"{"data":[{"id":"older","created":1700000000},{"id":"newer","created":1800000000}]}"#;

        let model_id = preferred_openai_compatible_model_id_from_body(body)
            .expect("provider model response should parse");

        assert_eq!(model_id.as_deref(), Some("newer"));
    }

    #[test]
    fn openai_public_discovery_requires_explicit_tool_metadata_for_default() {
        let body = r#"{"data":[{"id":"gpt-realtime-whisper","created":1800000000},{"id":"gpt-chat-candidate","created":1700000000}]}"#;

        let model_id = preferred_explicit_tool_capable_openai_compatible_model_id_from_body(body)
            .expect("provider model response should parse");

        assert_eq!(model_id, None);
    }

    #[test]
    fn openai_public_discovery_prefers_explicit_tool_capable_model() {
        let body = r#"{"data":[{"id":"newer-non-tool","created":1800000000,"supported_parameters":["temperature"]},{"id":"tool-capable","created":1700000000,"supported_parameters":["tools","response_format"]}]}"#;

        let model_id = preferred_explicit_tool_capable_openai_compatible_model_id_from_body(body)
            .expect("provider model response should parse");

        assert_eq!(model_id.as_deref(), Some("tool-capable"));
    }

    #[test]
    fn codex_model_discovery_prefers_provider_priority() {
        let body = r#"{"models":[{"slug":"secondary","priority":20},{"slug":"primary","priority":10},{"slug":"hidden","priority":1,"visibility":"hidden"}]}"#;

        let model_id =
            preferred_codex_model_id_from_body(body).expect("Codex model response should parse");

        assert_eq!(model_id.as_deref(), Some("primary"));
    }
}
