//! Provider model-discovery endpoint and response helpers.
//!
//! CLI and daemon surfaces own IO, credentials, caching, and output formatting;
//! this module owns provider-specific URL shapes and model-list parsing so
//! those rules stay consistent across all operator paths.

use anyhow::{Context, Result};

/// OpenAI ChatGPT/Codex OAuth public client id used by the browser login flow.
pub const OPENAI_CHATGPT_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
/// Base URL for the ChatGPT/Codex backend used by ChatGPT Login OAuth tokens.
pub const OPENAI_CODEX_BACKEND_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
/// Models endpoint for the ChatGPT/Codex backend.
pub const OPENAI_CODEX_MODELS_ENDPOINT: &str =
    "https://chatgpt.com/backend-api/codex/models?client_version=1.0.0";
/// Anthropic messages API version required by Anthropic-compatible providers.
pub const ANTHROPIC_API_VERSION: &str = "2023-06-01";

/// Parsed models endpoint plus the response shape expected from that endpoint.
#[derive(Debug, Clone)]
pub struct ProviderModelsEndpoint {
    /// Fully-qualified URL to request for model discovery.
    pub url: reqwest::Url,
    /// Normalized provider base URL represented by the endpoint.
    pub base_url: String,
    /// JSON response shape expected from [`ProviderModelsEndpoint::url`].
    pub response_format: ProviderModelsResponseFormat,
}

/// Model-discovery response dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderModelsResponseFormat {
    /// OpenAI-compatible `{"data":[{"id": "..."}]}` model list.
    OpenAiCompatible,
    /// ChatGPT/Codex backend `{"models":[...]}` model list.
    OpenAiCodexBackend,
}

/// Model id advertised by a provider discovery endpoint, with optional recency
/// and capability metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredProviderModel {
    /// Provider model identifier.
    pub id: String,
    recency_rank: Option<i64>,
    chat_default_eligible: bool,
    /// Whether the provider says this model supports tool calls.
    pub supports_tool_calls: Option<bool>,
    /// Whether the provider says this model supports JSON response mode.
    pub supports_json_mode: Option<bool>,
    /// Whether the provider says this model supports vision/image input.
    pub supports_vision: Option<bool>,
}

impl DiscoveredProviderModel {
    /// Returns whether this model should be considered for chat defaults.
    #[must_use]
    pub const fn can_be_chat_default(&self) -> bool {
        self.chat_default_eligible
    }
}

/// Returns whether an OAuth profile client id is Palyra's ChatGPT Login client.
#[must_use]
pub fn is_openai_chatgpt_oauth_client_id(client_id: Option<&str>) -> bool {
    client_id.map(str::trim).is_some_and(|value| value == OPENAI_CHATGPT_OAUTH_CLIENT_ID)
}

/// Builds the models endpoint for a provider credential.
///
/// # Errors
/// Returns an error when the selected endpoint URL is invalid.
pub fn provider_models_endpoint_for_probe(
    base_url: &str,
    uses_openai_chatgpt_oauth: bool,
) -> Result<ProviderModelsEndpoint> {
    if uses_openai_chatgpt_oauth {
        let url = reqwest::Url::parse(OPENAI_CODEX_MODELS_ENDPOINT)
            .context("invalid OpenAI Codex models endpoint")?;
        return Ok(ProviderModelsEndpoint {
            url,
            base_url: OPENAI_CODEX_BACKEND_BASE_URL.to_owned(),
            response_format: ProviderModelsResponseFormat::OpenAiCodexBackend,
        });
    }

    Ok(ProviderModelsEndpoint {
        url: provider_models_endpoint(base_url)?,
        base_url: base_url.trim().trim_end_matches('/').to_owned(),
        response_format: ProviderModelsResponseFormat::OpenAiCompatible,
    })
}

/// Builds an OpenAI-compatible `/models` endpoint for a provider base URL.
///
/// Configured base URLs appear both with and without a trailing `/v1` or
/// `/openai` segment; this normalizes either form to one models endpoint.
///
/// # Errors
/// Returns an error when the resulting URL cannot be parsed.
pub fn provider_models_endpoint(base_url: &str) -> Result<reqwest::Url> {
    let trimmed = base_url.trim().trim_end_matches('/');
    let raw = if trimmed.ends_with("/v1") || trimmed.ends_with("/openai") {
        format!("{trimmed}/models")
    } else {
        format!("{trimmed}/v1/models")
    };
    reqwest::Url::parse(raw.as_str())
        .with_context(|| format!("invalid provider base_url: {base_url}"))
}

/// Parses a discovery response and keeps only the advertised model ids.
///
/// # Errors
/// Returns an error when the body is not valid JSON for the selected response
/// shape.
pub fn parse_discovered_model_ids(
    body: &str,
    response_format: ProviderModelsResponseFormat,
) -> Result<Vec<String>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    match response_format {
        ProviderModelsResponseFormat::OpenAiCompatible => {
            Ok(parse_discovered_provider_models_from_value(&value)
                .into_iter()
                .map(|model| model.id)
                .collect())
        }
        ProviderModelsResponseFormat::OpenAiCodexBackend => {
            let Some(entries) = value.get("models").and_then(serde_json::Value::as_array) else {
                return Ok(parse_discovered_provider_models_from_value(&value)
                    .into_iter()
                    .map(|model| model.id)
                    .collect());
            };
            Ok(parse_openai_codex_model_ids_from_entries(entries))
        }
    }
}

/// Parses an OpenAI-style `{"data": [...]}` discovery response into model entries.
///
/// # Errors
/// Returns an error when the body is not valid JSON; a JSON body without the
/// expected shape yields an empty list.
pub fn parse_discovered_provider_models(body: &str) -> Result<Vec<DiscoveredProviderModel>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    Ok(parse_discovered_provider_models_from_value(&value))
}

fn parse_discovered_provider_models_from_value(
    value: &serde_json::Value,
) -> Vec<DiscoveredProviderModel> {
    value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .map(|entries| {
            entries.iter().filter_map(parse_discovered_provider_model).collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// Parses a discovery response and selects the preferred chat model id.
///
/// # Errors
/// Returns an error when the body is not valid JSON for the selected response
/// shape.
pub fn select_preferred_discovered_model_id_from_response(
    body: &str,
    response_format: ProviderModelsResponseFormat,
) -> Result<Option<String>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    match response_format {
        ProviderModelsResponseFormat::OpenAiCompatible => {
            let models = parse_discovered_provider_models_from_value(&value);
            Ok(select_preferred_discovered_model_id(models.as_slice()))
        }
        ProviderModelsResponseFormat::OpenAiCodexBackend => {
            let Some(entries) = value.get("models").and_then(serde_json::Value::as_array) else {
                let models = parse_discovered_provider_models_from_value(&value);
                return Ok(select_preferred_discovered_model_id(models.as_slice()));
            };
            Ok(parse_openai_codex_model_ids_from_entries(entries).into_iter().next())
        }
    }
}

/// Parses an OpenAI-compatible discovery response and selects the preferred
/// model id that explicitly advertises tool-call support.
///
/// # Errors
/// Returns an error when the body is not valid JSON.
pub fn select_preferred_tool_capable_discovered_model_id_from_response(
    body: &str,
) -> Result<Option<String>> {
    let models = parse_discovered_provider_models(body)?;
    Ok(select_preferred_tool_capable_discovered_model_id(models.as_slice()))
}

/// Selects the preferred model id from a discovery response.
///
/// Prefers the newest entry when every model carries recency metadata;
/// otherwise preserves the provider's response order and takes the first id.
#[must_use]
pub fn select_preferred_discovered_model_id(models: &[DiscoveredProviderModel]) -> Option<String> {
    select_preferred_discovered_model(models).map(|model| model.id.clone())
}

/// Selects the preferred model id from candidates that explicitly advertise
/// tool-call support.
#[must_use]
pub fn select_preferred_tool_capable_discovered_model_id(
    models: &[DiscoveredProviderModel],
) -> Option<String> {
    select_preferred_tool_capable_discovered_model(models).map(|model| model.id.clone())
}

/// Selects the preferred model entry from a provider discovery response.
///
/// Capability metadata wins over raw provider order when the provider exposes
/// it: Palyra agents need tool-capable chat models, and providers such as
/// OpenRouter advertise that via `supported_parameters`.
#[must_use]
pub fn select_preferred_discovered_model(
    models: &[DiscoveredProviderModel],
) -> Option<&DiscoveredProviderModel> {
    let candidates = models
        .iter()
        .filter(|model| !model.id.trim().is_empty() && model.can_be_chat_default())
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return None;
    }
    let candidate_pool = if candidates.iter().any(|model| model.supports_tool_calls == Some(true)) {
        candidates
            .iter()
            .copied()
            .filter(|model| model.supports_tool_calls == Some(true))
            .collect::<Vec<_>>()
    } else {
        candidates
    };
    select_preferred_candidate(candidate_pool)
}

/// Selects the preferred model entry from candidates that explicitly advertise
/// tool-call support.
#[must_use]
pub fn select_preferred_tool_capable_discovered_model(
    models: &[DiscoveredProviderModel],
) -> Option<&DiscoveredProviderModel> {
    let candidates = models
        .iter()
        .filter(|model| {
            !model.id.trim().is_empty()
                && model.can_be_chat_default()
                && model.supports_tool_calls == Some(true)
        })
        .collect::<Vec<_>>();
    select_preferred_candidate(candidates)
}

fn select_preferred_candidate(
    candidate_pool: Vec<&DiscoveredProviderModel>,
) -> Option<&DiscoveredProviderModel> {
    if candidate_pool.is_empty() {
        return None;
    }
    // Provider timestamps are the only freshness signal accepted here; model
    // id strings are intentionally ignored because vendor naming is unstable.
    if candidate_pool.iter().all(|model| model.recency_rank.is_some()) {
        let (mut selected, mut selected_rank) =
            (candidate_pool[0], candidate_pool[0].recency_rank?);
        for model in candidate_pool.iter().skip(1) {
            let recency_rank = model.recency_rank?;
            if recency_rank > selected_rank {
                selected = model;
                selected_rank = recency_rank;
            }
        }
        return Some(selected);
    }

    Some(candidate_pool[0])
}

fn parse_openai_codex_model_ids_from_entries(entries: &[serde_json::Value]) -> Vec<String> {
    let mut sortable = Vec::<(i64, usize, String)>::new();
    for (index, entry) in entries.iter().enumerate() {
        if codex_model_is_hidden(entry) {
            continue;
        }
        let Some(model_id) = entry
            .get("slug")
            .or_else(|| entry.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if sortable.iter().any(|(_, _, existing)| existing == model_id) {
            continue;
        }
        sortable.push((codex_model_priority(entry), index, model_id.to_owned()));
    }
    sortable.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    sortable.into_iter().map(|(_, _, model_id)| model_id).collect()
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

fn parse_discovered_provider_model(entry: &serde_json::Value) -> Option<DiscoveredProviderModel> {
    let id = entry
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())?;
    Some(DiscoveredProviderModel {
        id: id.to_owned(),
        recency_rank: discovered_model_recency_rank(entry),
        chat_default_eligible: discovered_model_can_be_chat_default(entry, id),
        supports_tool_calls: model_supported_parameter(entry, &["tools", "tool_choice"]),
        supports_json_mode: model_supported_parameter(entry, &["response_format"]),
        supports_vision: discovered_model_supports_vision(entry),
    })
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

fn discovered_model_supports_vision(entry: &serde_json::Value) -> Option<bool> {
    let modalities = discovered_model_modalities(entry, "input_modalities")?;
    Some(modalities.iter().any(|modality| matches!(modality.as_str(), "image" | "vision")))
}

fn discovered_model_can_be_chat_default(entry: &serde_json::Value, model_id: &str) -> bool {
    if model_id_looks_non_chat_default(model_id) {
        return false;
    }

    let Some(output_modalities) = discovered_model_modalities(entry, "output_modalities") else {
        return true;
    };
    let has_text_output = output_modalities.iter().any(|modality| modality == "text");
    let has_media_output = output_modalities
        .iter()
        .any(|modality| matches!(modality.as_str(), "image" | "video" | "audio"));
    has_text_output && !has_media_output
}

fn discovered_model_modalities(entry: &serde_json::Value, field: &str) -> Option<Vec<String>> {
    let modalities = entry
        .get("architecture")
        .and_then(|architecture| architecture.get(field))
        .and_then(serde_json::Value::as_array)?;
    Some(
        modalities
            .iter()
            .filter_map(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|modality| !modality.is_empty())
            .map(str::to_ascii_lowercase)
            .collect(),
    )
}

fn model_id_looks_non_chat_default(model_id: &str) -> bool {
    let normalized = model_id.trim().to_ascii_lowercase();
    [
        "audio",
        "embed",
        "embedding",
        "image",
        "imagine",
        "moderation",
        "realtime",
        "speech",
        "transcrib",
        "tts",
        "video",
        "whisper",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn discovered_model_recency_rank(entry: &serde_json::Value) -> Option<i64> {
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
        return normalize_numeric_recency_rank(raw);
    }
    if let Some(raw) = value.as_u64().and_then(|raw| i64::try_from(raw).ok()) {
        return normalize_numeric_recency_rank(raw);
    }
    value
        .as_str()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .and_then(normalize_numeric_recency_rank)
}

fn normalize_numeric_recency_rank(raw: i64) -> Option<i64> {
    const EPOCH_SECONDS_UPPER_BOUND: i64 = 100_000_000_000;
    if raw <= 0 {
        return None;
    }
    if raw < EPOCH_SECONDS_UPPER_BOUND {
        return raw.checked_mul(1000);
    }
    Some(raw)
}

#[cfg(test)]
mod tests {
    use super::{
        is_openai_chatgpt_oauth_client_id, parse_discovered_model_ids,
        parse_discovered_provider_models, provider_models_endpoint,
        provider_models_endpoint_for_probe, select_preferred_discovered_model,
        select_preferred_discovered_model_id_from_response,
        select_preferred_tool_capable_discovered_model_id_from_response,
        ProviderModelsResponseFormat, OPENAI_CHATGPT_OAUTH_CLIENT_ID,
        OPENAI_CODEX_BACKEND_BASE_URL, OPENAI_CODEX_MODELS_ENDPOINT,
    };

    #[test]
    fn provider_models_endpoint_preserves_versioned_openai_base_paths() {
        assert_eq!(
            provider_models_endpoint("https://generativelanguage.googleapis.com/v1beta/openai/")
                .expect("Google Gemini endpoint should parse")
                .as_str(),
            "https://generativelanguage.googleapis.com/v1beta/openai/models"
        );
        assert_eq!(
            provider_models_endpoint("https://openrouter.ai/api/v1")
                .expect("OpenRouter endpoint should parse")
                .as_str(),
            "https://openrouter.ai/api/v1/models"
        );
    }

    #[test]
    fn chatgpt_oauth_probe_uses_codex_models_endpoint() {
        let endpoint = provider_models_endpoint_for_probe("https://api.openai.com/v1", true)
            .expect("ChatGPT OAuth should produce a Codex models endpoint");

        assert_eq!(endpoint.url.as_str(), OPENAI_CODEX_MODELS_ENDPOINT);
        assert_eq!(endpoint.base_url, OPENAI_CODEX_BACKEND_BASE_URL);
        assert_eq!(endpoint.response_format, ProviderModelsResponseFormat::OpenAiCodexBackend);
    }

    #[test]
    fn openai_api_key_probe_keeps_openai_compatible_models_endpoint() {
        let endpoint = provider_models_endpoint_for_probe("https://api.openai.com/v1", false)
            .expect("OpenAI API key should produce the public models endpoint");

        assert_eq!(endpoint.url.as_str(), "https://api.openai.com/v1/models");
        assert_eq!(endpoint.response_format, ProviderModelsResponseFormat::OpenAiCompatible);
    }

    #[test]
    fn parses_openai_compatible_discovery_capabilities() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"image-only-newer","created":1800000000,"supported_parameters":["temperature"],"architecture":{"output_modalities":["image"]}},{"id":"tool-chat","created":1700000000,"supported_parameters":["tools","response_format"],"architecture":{"input_modalities":["text","image"],"output_modalities":["text"]}}]}"#,
        )
        .expect("discovery response should parse");

        let selected = select_preferred_discovered_model(models.as_slice())
            .expect("tool-capable chat model should be selected");
        assert_eq!(selected.id, "tool-chat");
        assert_eq!(selected.supports_tool_calls, Some(true));
        assert_eq!(selected.supports_json_mode, Some(true));
        assert_eq!(selected.supports_vision, Some(true));
    }

    #[test]
    fn explicit_tool_capable_selector_requires_tool_metadata() {
        let model_id = select_preferred_tool_capable_discovered_model_id_from_response(
            r#"{"data":[{"id":"plain-chat","created":1800000000},{"id":"media-tools","created":1700000000,"supported_parameters":["tools"],"architecture":{"output_modalities":["image"]}}]}"#,
        )
        .expect("discovery response should parse");

        assert_eq!(model_id, None);
    }

    #[test]
    fn codex_selector_falls_back_to_openai_compatible_selection() {
        let model_id = select_preferred_discovered_model_id_from_response(
            r#"{"data":[{"id":"image-only-newer","created":1800000000,"architecture":{"output_modalities":["image"]}},{"id":"chat-older","created":1700000000,"architecture":{"output_modalities":["text"]}}]}"#,
            ProviderModelsResponseFormat::OpenAiCodexBackend,
        )
        .expect("fallback response should parse");

        assert_eq!(model_id.as_deref(), Some("chat-older"));
    }

    #[test]
    fn codex_models_parser_uses_visible_slugs_sorted_by_priority() {
        let body = serde_json::json!({
            "models": [
                {"slug": "gpt-5.3-codex", "priority": 20},
                {"slug": "gpt-hidden", "priority": 1, "visibility": "hidden"},
                {"slug": "gpt-5.4", "priority": 10}
            ]
        })
        .to_string();

        let discovered = parse_discovered_model_ids(
            body.as_str(),
            ProviderModelsResponseFormat::OpenAiCodexBackend,
        )
        .expect("Codex model response should parse");

        assert_eq!(discovered, vec!["gpt-5.4", "gpt-5.3-codex"]);
    }

    #[test]
    fn chatgpt_oauth_client_id_detection_trims_input() {
        assert!(is_openai_chatgpt_oauth_client_id(Some(&format!(
            " {OPENAI_CHATGPT_OAUTH_CLIENT_ID} "
        ))));
        assert!(!is_openai_chatgpt_oauth_client_id(Some("different-client")));
    }
}
