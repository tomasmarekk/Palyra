//! Console model-provider connectivity probes.
//!
//! Serves `POST /console/v1/models/test-connection` and
//! `POST /console/v1/models/discover`. Builds probe targets from the on-disk
//! daemon config overlaid with the live runtime provider snapshot, resolves a
//! credential (auth profile, then inline config key, then vault ref), and
//! calls each provider's models endpoint. Per-provider failures are reported
//! inside the envelope payload, not as HTTP errors, and every outbound error
//! string is redacted before it reaches the console wire contract.

use std::{path::Path, time::Instant};

use palyra_auth::{AuthCredential, AuthProviderKind};
use palyra_common::daemon_config_schema::{FileModelProviderConfig, RootFileConfig};
use palyra_common::redaction::redact_auth_error;
use palyra_vault::{Vault, VaultConfig, VaultRef};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION};
use serde::{Deserialize, Serialize};

use crate::app::state::AppState;
use crate::*;

const OPENAI_COMPATIBLE_PROVIDER_KIND: &str = "openai_compatible";
const ANTHROPIC_PROVIDER_KIND: &str = "anthropic";
const DETERMINISTIC_PROVIDER_KIND: &str = "deterministic";
const OPENAI_DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const OPENAI_CHATGPT_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const OPENAI_CODEX_BACKEND_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
const OPENAI_CODEX_MODELS_ENDPOINT: &str =
    "https://chatgpt.com/backend-api/codex/models?client_version=1.0.0";
const ANTHROPIC_DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_API_VERSION: &str = "2023-06-01";
const DEFAULT_PROVIDER_PROBE_TIMEOUT_MS: u64 = 5_000;

/// Request body shared by the test-connection and discover endpoints:
/// an optional provider filter (id or kind) and an optional probe timeout.
#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct ConsoleProviderProbeRequest {
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Wire envelope for probe results returned to the web console.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ConsoleProviderProbeEnvelope {
    pub contract: control_plane::ContractDescriptor,
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_filter: Option<String>,
    pub timeout_ms: u64,
    pub provider_count: usize,
    pub providers: Vec<ConsoleProviderProbePayload>,
}

/// Per-provider probe outcome: a state label (`ok`, `auth_failed`,
/// `endpoint_failed`, ...), a redacted human-readable message, and the
/// discovered/configured model id sets.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ConsoleProviderProbePayload {
    pub provider_id: String,
    pub kind: String,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_base_url: Option<String>,
    pub credential_source: String,
    pub state: String,
    pub message: String,
    pub checked_at_unix_ms: i64,
    pub cache_status: String,
    pub discovery_source: String,
    pub discovered_model_ids: Vec<String>,
    pub configured_model_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

/// One provider to probe, with config-derived endpoint and credential wiring.
#[derive(Debug, Clone)]
struct ConsoleProbeTarget {
    provider_id: String,
    kind: String,
    enabled: bool,
    endpoint_base_url: Option<String>,
    allow_private_base_url: bool,
    auth_profile_id: Option<String>,
    auth_profile_provider_kind: Option<String>,
    inline_api_key: Option<String>,
    vault_ref: Option<String>,
    configured_model_ids: Vec<String>,
}

/// Credential resolved for a probe, tagged with how it is sent (API-key
/// header vs Bearer) and where it came from (`source` is echoed on the wire).
#[derive(Debug, Clone)]
enum ResolvedCredential {
    ApiKey { token: String, source: String },
    Bearer { token: String, source: String, oauth_kind: Option<ResolvedOauthProfileKind> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedOauthProfileKind {
    OpenAiChatGptLogin,
}

#[derive(Debug, Clone)]
struct ProviderModelsEndpoint {
    url: reqwest::Url,
    base_url: String,
    response_format: ProviderModelsResponseFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderModelsResponseFormat {
    OpenAiCompatible,
    OpenAiCodexBackend,
}

/// Probes provider connectivity and credentials without model discovery.
///
/// # Errors
/// Returns an error response when the console session/CSRF check fails, the
/// daemon config snapshot cannot be loaded or parsed, no provider matches the
/// requested filter, or the system clock is unavailable. Individual provider
/// failures are reported inside the envelope instead.
pub(crate) async fn console_models_test_connection_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleProviderProbeRequest>,
) -> Result<Json<ConsoleProviderProbeEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    run_console_provider_probe(&state, &session.context, payload, false).await.map(Json)
}

/// Probes provider connectivity and additionally discovers selectable model
/// ids from each provider's models endpoint.
///
/// # Errors
/// Same failure modes as [`console_models_test_connection_handler`].
pub(crate) async fn console_models_discover_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleProviderProbeRequest>,
) -> Result<Json<ConsoleProviderProbeEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    run_console_provider_probe(&state, &session.context, payload, true).await.map(Json)
}

async fn run_console_provider_probe(
    state: &AppState,
    _context: &gateway::RequestContext,
    payload: ConsoleProviderProbeRequest,
    discover: bool,
) -> Result<ConsoleProviderProbeEnvelope, Response> {
    let configured_path = std::env::var("PALYRA_CONFIG").ok();
    let (document, _, _) = load_console_config_snapshot(configured_path.as_deref(), true)?;
    let content = toml::to_string(&document).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize daemon config snapshot: {error}"
        )))
    })?;
    let parsed: RootFileConfig = toml::from_str(&content).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "invalid daemon config schema: {error}"
        )))
    })?;
    let model_provider = parsed.model_provider.unwrap_or_default();
    let provider_filter =
        payload.provider_id.as_deref().and_then(normalize_optional_text).map(str::to_owned);
    let timeout_ms =
        payload.timeout_ms.unwrap_or(DEFAULT_PROVIDER_PROBE_TIMEOUT_MS).clamp(500, 30_000);
    let runtime_snapshot = state.runtime.model_provider_status_snapshot();
    let targets = overlay_probe_targets_with_runtime_snapshot(
        build_console_probe_targets(&model_provider),
        &runtime_snapshot,
    );
    let filtered_targets = targets
        .into_iter()
        .filter(|target| {
            provider_filter
                .as_deref()
                .map(|filter| provider_matches_filter(target, filter))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    if filtered_targets.is_empty() {
        return Err(runtime_status_response(tonic::Status::not_found(format!(
            "no provider matched '{}'",
            provider_filter.as_deref().unwrap_or("configured registry")
        ))));
    }

    let now_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let mut results = Vec::with_capacity(filtered_targets.len());
    for target in filtered_targets {
        results
            .push(probe_console_provider(state, &target, timeout_ms, now_unix_ms, discover).await);
    }

    Ok(ConsoleProviderProbeEnvelope {
        contract: contract_descriptor(),
        mode: if discover { "discover".to_owned() } else { "test_connection".to_owned() },
        provider_filter,
        timeout_ms,
        provider_count: results.len(),
        providers: results,
    })
}

fn build_console_probe_targets(config: &FileModelProviderConfig) -> Vec<ConsoleProbeTarget> {
    let allow_private_env = std::env::var("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL").ok();
    build_console_probe_targets_with_env(config, allow_private_env.as_deref())
}

fn build_console_probe_targets_with_env(
    config: &FileModelProviderConfig,
    allow_private_env: Option<&str>,
) -> Vec<ConsoleProbeTarget> {
    let provider_kind =
        config.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let models_by_provider = registry_model_ids_by_provider(config);
    let global_allow_private_base_url =
        effective_global_allow_private_base_url(config, allow_private_env);

    if let Some(entries) = config.providers.as_ref() {
        let default_provider_id =
            default_provider_id_from_config(config, &models_by_provider).map(str::to_owned);
        return entries
            .iter()
            .map(|entry| {
                let provider_id = entry.provider_id.clone().unwrap_or_default();
                let kind = entry.kind.clone().unwrap_or_else(|| provider_kind.clone());
                // Global base-url/auth settings describe one provider, not the
                // whole registry: only the provider serving the default chat
                // model inherits them (or every entry when no default resolves,
                // matching legacy single-provider configs).
                let inherit_globals = default_provider_id
                    .as_deref()
                    .is_some_and(|candidate| candidate == provider_id)
                    || default_provider_id.is_none();
                ConsoleProbeTarget {
                    provider_id: provider_id.clone(),
                    kind: kind.clone(),
                    enabled: entry.enabled.unwrap_or(true),
                    endpoint_base_url: entry.base_url.clone().or_else(|| {
                        if inherit_globals {
                            default_base_url_for_kind(kind.as_str(), config)
                        } else {
                            None
                        }
                    }),
                    allow_private_base_url: entry
                        .allow_private_base_url
                        .unwrap_or(global_allow_private_base_url),
                    auth_profile_id: entry.auth_profile_id.clone().or_else(|| {
                        inherit_globals.then(|| config.auth_profile_id.clone()).flatten()
                    }),
                    auth_profile_provider_kind: entry.auth_provider_kind.clone().or_else(|| {
                        inherit_globals.then(|| config.auth_provider_kind.clone()).flatten()
                    }),
                    inline_api_key: entry.api_key.clone().or_else(|| {
                        inherit_globals
                            .then(|| inline_api_key_for_kind(kind.as_str(), config))
                            .flatten()
                    }),
                    vault_ref: entry.api_key_vault_ref.clone().or_else(|| {
                        inherit_globals.then(|| vault_ref_for_kind(kind.as_str(), config)).flatten()
                    }),
                    configured_model_ids: models_by_provider
                        .iter()
                        .find(|(candidate, _)| candidate == &provider_id)
                        .map(|(_, models)| models.clone())
                        .unwrap_or_default(),
                }
            })
            .collect();
    }

    let provider_id =
        legacy_provider_id(provider_kind.as_str(), config.auth_provider_kind.as_deref()).to_owned();
    vec![ConsoleProbeTarget {
        provider_id: provider_id.clone(),
        kind: provider_kind.clone(),
        enabled: true,
        endpoint_base_url: default_base_url_for_kind(provider_kind.as_str(), config),
        allow_private_base_url: global_allow_private_base_url,
        auth_profile_id: config.auth_profile_id.clone(),
        auth_profile_provider_kind: config.auth_provider_kind.clone(),
        inline_api_key: inline_api_key_for_kind(provider_kind.as_str(), config),
        vault_ref: vault_ref_for_kind(provider_kind.as_str(), config),
        configured_model_ids: models_by_provider
            .iter()
            .find(|(candidate, _)| candidate == &provider_id)
            .map(|(_, models)| models.clone())
            .unwrap_or_default(),
    }]
}

fn effective_global_allow_private_base_url(
    config: &FileModelProviderConfig,
    env_override: Option<&str>,
) -> bool {
    if let Some(value) = env_override {
        // A malformed env override fails closed: private base URLs stay blocked.
        return value.trim().parse::<bool>().unwrap_or(false);
    }
    config.allow_private_base_url.unwrap_or(false)
}

// The runtime snapshot wins over the on-disk config wherever it carries a
// value, so probes exercise the endpoints and auth wiring the daemon is
// actually using (including hot-reloaded changes).
fn overlay_probe_targets_with_runtime_snapshot(
    mut targets: Vec<ConsoleProbeTarget>,
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
) -> Vec<ConsoleProbeTarget> {
    for target in &mut targets {
        if let Some(runtime_provider) = snapshot
            .registry
            .providers
            .iter()
            .find(|provider| provider.provider_id == target.provider_id)
        {
            target.kind = runtime_provider.kind.clone();
            target.enabled = runtime_provider.enabled;
            if runtime_provider.endpoint_base_url.is_some() {
                target.endpoint_base_url = runtime_provider.endpoint_base_url.clone();
            }
            if runtime_provider.auth_profile_id.is_some() {
                target.auth_profile_id = runtime_provider.auth_profile_id.clone();
            }
            if runtime_provider.auth_profile_provider_kind.is_some() {
                target.auth_profile_provider_kind =
                    runtime_provider.auth_profile_provider_kind.clone();
            }
        }

        if target.provider_id == snapshot.provider_id {
            target.kind = snapshot.kind.clone();
            if let Some(endpoint_base_url) =
                snapshot.openai_base_url.clone().or_else(|| snapshot.anthropic_base_url.clone())
            {
                target.endpoint_base_url = Some(endpoint_base_url);
            }
            if snapshot.auth_profile_id.is_some() {
                target.auth_profile_id = snapshot.auth_profile_id.clone();
            }
            if snapshot.auth_profile_provider_kind.is_some() {
                target.auth_profile_provider_kind = snapshot.auth_profile_provider_kind.clone();
            }
        }
    }
    targets
}

fn registry_model_ids_by_provider(config: &FileModelProviderConfig) -> Vec<(String, Vec<String>)> {
    if let Some(entries) = config.models.as_ref() {
        let mut grouped = Vec::<(String, Vec<String>)>::new();
        for entry in entries {
            let provider_id = entry.provider_id.clone().unwrap_or_default();
            let model_id = entry.model_id.clone().unwrap_or_default();
            if provider_id.is_empty() || model_id.is_empty() {
                continue;
            }
            if let Some((_, models)) =
                grouped.iter_mut().find(|(candidate, _)| *candidate == provider_id)
            {
                models.push(model_id);
            } else {
                grouped.push((provider_id, vec![model_id]));
            }
        }
        return grouped;
    }

    let kind = config.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let provider_id =
        legacy_provider_id(kind.as_str(), config.auth_provider_kind.as_deref()).to_owned();
    let mut models = Vec::new();
    if let Some(model_id) = config.openai_model.clone().or_else(|| config.anthropic_model.clone()) {
        models.push(model_id);
    }
    if let Some(model_id) = config.openai_embeddings_model.clone() {
        models.push(model_id);
    }
    vec![(provider_id, models)]
}

fn default_provider_id_from_config<'a>(
    config: &FileModelProviderConfig,
    models_by_provider: &'a [(String, Vec<String>)],
) -> Option<&'a str> {
    let default_model_id = config
        .default_chat_model_id
        .as_deref()
        .or(config.openai_model.as_deref())
        .or(config.anthropic_model.as_deref());
    let default_model_id = default_model_id?;
    models_by_provider.iter().find_map(|(provider_id, models)| {
        models.iter().any(|model_id| model_id == default_model_id).then_some(provider_id.as_str())
    })
}

fn provider_matches_filter(target: &ConsoleProbeTarget, filter: &str) -> bool {
    let normalized_filter = normalize_provider_filter_alias(filter);
    [
        Some(target.provider_id.as_str()),
        Some(target.kind.as_str()),
        target.auth_profile_provider_kind.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|candidate| normalize_provider_filter_alias(candidate) == normalized_filter)
}

fn normalize_provider_filter_alias(raw: &str) -> String {
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "x_ai" | "grok" => "xai".to_owned(),
        "open_router" => "openrouter".to_owned(),
        "gemini" => "google_gemini".to_owned(),
        "gemini_cli" => "google_gemini_cli".to_owned(),
        _ => normalized,
    }
}

fn legacy_provider_id(provider_kind: &str, auth_provider_kind: Option<&str>) -> &'static str {
    if provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND {
        if let Some(auth_provider_kind) = auth_provider_kind {
            let normalized_auth_kind = normalize_provider_filter_alias(auth_provider_kind);
            match normalized_auth_kind.as_str() {
                "xai" | "grok" => return "xai-primary",
                "google_gemini" | "google_gemini_cli" | "gemini" | "gemini_cli" => {
                    return "google-gemini-primary";
                }
                "openrouter" | "open_router" => return "openrouter-primary",
                _ => {}
            }
        }
    }
    match provider_kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => "openai-primary",
        ANTHROPIC_PROVIDER_KIND => "anthropic-primary",
        _ => "deterministic-primary",
    }
}

fn default_base_url_for_kind(kind: &str, config: &FileModelProviderConfig) -> Option<String> {
    match kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => std::env::var("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL")
            .ok()
            .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
            .or_else(|| config.openai_base_url.clone())
            .or_else(|| Some(OPENAI_DEFAULT_BASE_URL.to_owned())),
        ANTHROPIC_PROVIDER_KIND => std::env::var("PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL")
            .ok()
            .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
            .or_else(|| config.anthropic_base_url.clone())
            .or_else(|| Some(ANTHROPIC_DEFAULT_BASE_URL.to_owned())),
        _ => None,
    }
}

fn inline_api_key_for_kind(kind: &str, config: &FileModelProviderConfig) -> Option<String> {
    match kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => config
            .openai_api_key
            .clone()
            .or_else(|| std::env::var("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY").ok()),
        ANTHROPIC_PROVIDER_KIND => config.anthropic_api_key.clone(),
        _ => None,
    }
}

fn vault_ref_for_kind(kind: &str, config: &FileModelProviderConfig) -> Option<String> {
    match kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => config
            .openai_api_key_vault_ref
            .clone()
            .or_else(|| std::env::var("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY_VAULT_REF").ok()),
        ANTHROPIC_PROVIDER_KIND => config.anthropic_api_key_vault_ref.clone(),
        _ => None,
    }
}

async fn probe_console_provider(
    state: &AppState,
    target: &ConsoleProbeTarget,
    timeout_ms: u64,
    now_unix_ms: i64,
    discover: bool,
) -> ConsoleProviderProbePayload {
    let mut payload = ConsoleProviderProbePayload {
        provider_id: target.provider_id.clone(),
        kind: target.kind.clone(),
        enabled: target.enabled,
        endpoint_base_url: target.endpoint_base_url.clone(),
        credential_source: "none".to_owned(),
        state: "unknown".to_owned(),
        message: "provider has not been checked yet".to_owned(),
        checked_at_unix_ms: now_unix_ms,
        cache_status: "live".to_owned(),
        discovery_source: if discover { "live".to_owned() } else { "skipped".to_owned() },
        discovered_model_ids: Vec::new(),
        configured_model_ids: target.configured_model_ids.clone(),
        latency_ms: None,
    };
    if !target.enabled {
        payload.state = "disabled".to_owned();
        payload.message = "provider is disabled in the registry".to_owned();
        payload.discovery_source = "registry".to_owned();
        return payload;
    }
    if target.kind == DETERMINISTIC_PROVIDER_KIND {
        payload.state = "unsupported".to_owned();
        payload.message =
            "deterministic provider does not expose a remote models endpoint".to_owned();
        payload.discovery_source = "registry".to_owned();
        payload.discovered_model_ids = target.configured_model_ids.clone();
        return payload;
    }
    let Some(base_url) = target.endpoint_base_url.as_deref() else {
        payload.state = "endpoint_missing".to_owned();
        payload.message = "provider base_url is not configured".to_owned();
        return payload;
    };
    if let Err(error) = validate_console_probe_endpoint_policy(target, base_url) {
        payload.state = "endpoint_failed".to_owned();
        payload.message = sanitize_probe_error(error.to_string().as_str());
        return payload;
    }

    let credential = match resolve_provider_credential(state, target) {
        Ok(Some(credential)) => credential,
        Ok(None) => {
            payload.state = "missing_auth".to_owned();
            payload.message = "provider does not have a usable API credential".to_owned();
            return payload;
        }
        Err(error) => {
            payload.state = "missing_auth".to_owned();
            payload.message = sanitize_probe_error(error.to_string().as_str());
            return payload;
        }
    };
    payload.credential_source = match &credential {
        ResolvedCredential::ApiKey { source, .. } | ResolvedCredential::Bearer { source, .. } => {
            source.clone()
        }
    };

    let endpoint = match provider_models_endpoint_for_probe(target, base_url, &credential) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            payload.state = "endpoint_failed".to_owned();
            payload.message = sanitize_probe_error(error.to_string().as_str());
            return payload;
        }
    };
    payload.endpoint_base_url = Some(endpoint.base_url.clone());

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            payload.state = "endpoint_failed".to_owned();
            payload.message = sanitize_probe_error(error.to_string().as_str());
            return payload;
        }
    };
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    // Anthropic-kind targets authenticate via the x-api-key scheme, except
    // MiniMax-compatible endpoints (detected through the auth profile provider
    // kind), which expect a standard Bearer token instead. Tokens that contain
    // non-header-safe bytes are swapped for a redaction placeholder so the
    // probe fails as an auth error rather than panicking or leaking bytes.
    match &credential {
        ResolvedCredential::ApiKey { token, .. }
            if target.kind == ANTHROPIC_PROVIDER_KIND && !target_uses_minimax_auth(target) =>
        {
            headers.insert(
                "x-api-key",
                HeaderValue::from_str(token.as_str())
                    .unwrap_or_else(|_| HeaderValue::from_static("<redacted>")),
            );
            headers.insert("anthropic-version", HeaderValue::from_static(ANTHROPIC_API_VERSION));
        }
        ResolvedCredential::ApiKey { token, .. } | ResolvedCredential::Bearer { token, .. } => {
            let bearer = format!("Bearer {token}");
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(bearer.as_str())
                    .unwrap_or_else(|_| HeaderValue::from_static("Bearer <redacted>")),
            );
        }
    }

    let started_at = Instant::now();
    match client.get(endpoint.url).headers(headers).send().await {
        Ok(response) => {
            payload.latency_ms =
                Some(started_at.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            if status.is_success() {
                payload.state = "ok".to_owned();
                if discover {
                    // Unparseable discovery bodies fall back to the configured
                    // registry. The equality check below therefore also labels
                    // a live response that exactly matches the registry as
                    // "registry_fallback" -- indistinguishable by design.
                    let discovered =
                        parse_discovered_model_ids(body.as_str(), endpoint.response_format)
                            .unwrap_or_else(|_| target.configured_model_ids.clone());
                    payload.discovered_model_ids = discovered;
                    payload.discovery_source =
                        if payload.discovered_model_ids == target.configured_model_ids {
                            "registry_fallback".to_owned()
                        } else {
                            "live".to_owned()
                        };
                    let empty_minimax_discovery_with_configured_models =
                        payload.discovered_model_ids.is_empty()
                            && target_uses_minimax_auth(target)
                            && !target.configured_model_ids.is_empty();
                    payload.state = if payload.discovered_model_ids.is_empty() {
                        if empty_minimax_discovery_with_configured_models {
                            "ok".to_owned()
                        } else {
                            "partial".to_owned()
                        }
                    } else {
                        "ok".to_owned()
                    };
                    payload.message = if payload.discovered_model_ids.is_empty() {
                        if empty_minimax_discovery_with_configured_models {
                            "provider connection succeeded; MiniMax-compatible model discovery returned no ids, so configured model registry remains the source of selectable models"
                                .to_owned()
                        } else {
                            "provider connection succeeded but model discovery returned no ids"
                                .to_owned()
                        }
                    } else {
                        format!(
                            "provider connection succeeded and discovered {} model(s)",
                            payload.discovered_model_ids.len()
                        )
                    };
                } else {
                    payload.discovery_source = "skipped".to_owned();
                    payload.message = "provider connection succeeded".to_owned();
                }
            } else {
                payload.state = classify_provider_failure(status.as_u16());
                payload.message = sanitize_provider_error(body.as_str(), status.as_u16());
            }
        }
        Err(error) => {
            payload.latency_ms =
                Some(started_at.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
            payload.state = if error.is_timeout() {
                "degraded".to_owned()
            } else {
                "endpoint_failed".to_owned()
            };
            payload.message = sanitize_probe_error(error.to_string().as_str());
        }
    }

    payload
}

fn validate_console_probe_endpoint_policy(
    target: &ConsoleProbeTarget,
    base_url: &str,
) -> Result<(), anyhow::Error> {
    crate::model_provider::validate_openai_base_url_network_policy(
        base_url,
        target.allow_private_base_url,
    )
}

/// Resolves the probe credential with the same precedence the model runtime
/// uses: auth profile, then inline config key, then config vault ref.
///
/// Returns `Ok(None)` when the target has no credential configured at all.
///
/// # Errors
/// Fails when the referenced auth profile is missing or belongs to a different
/// provider kind than the target, or when a vault secret cannot be loaded.
fn resolve_provider_credential(
    state: &AppState,
    target: &ConsoleProbeTarget,
) -> Result<Option<ResolvedCredential>, anyhow::Error> {
    if let Some(profile_id) = target.auth_profile_id.as_deref() {
        return resolve_auth_profile_credential(state.auth_runtime.registry(), target, profile_id)
            .map(Some);
    }

    if let Some(api_key) = target.inline_api_key.as_deref().and_then(normalize_optional_text) {
        return Ok(Some(ResolvedCredential::ApiKey {
            token: api_key.to_owned(),
            source: "config_inline".to_owned(),
        }));
    }
    if let Some(vault_ref) = target.vault_ref.as_deref() {
        let token = load_vault_secret_utf8(&state.vault, vault_ref)?;
        return Ok(Some(ResolvedCredential::ApiKey {
            token,
            source: "config_vault_ref".to_owned(),
        }));
    }
    Ok(None)
}

fn resolve_auth_profile_credential(
    auth_registry: &palyra_auth::AuthProfileRegistry,
    target: &ConsoleProbeTarget,
    profile_id: &str,
) -> Result<ResolvedCredential, anyhow::Error> {
    let profile = auth_registry
        .get_profile(profile_id)
        .with_context(|| format!("failed to load auth profile '{profile_id}'"))?
        .ok_or_else(|| anyhow::anyhow!("auth profile not found: {profile_id}"))?;
    let expected_provider = expected_auth_provider_for_probe_target(target);
    if let Some(expected_provider) = expected_provider {
        let expected_custom_name = expected_custom_auth_provider_name_for_probe_target(target);
        let matches_expected = if expected_provider == AuthProviderKind::Custom {
            matches!(profile.provider.kind, AuthProviderKind::Custom)
                && expected_custom_name.is_some_and(|expected_name| {
                    profile
                        .provider
                        .custom_name
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(expected_name))
                })
        } else {
            profile.provider.kind == expected_provider
        };
        if !matches_expected {
            anyhow::bail!(
                "auth profile '{}' belongs to provider '{}' instead of '{}'",
                profile_id,
                profile.provider.label(),
                target.kind
            );
        }
    }

    let vault = open_auth_profile_vault(auth_registry.state_root())?;
    match profile.credential {
        AuthCredential::ApiKey { api_key_vault_ref } => {
            let token = load_vault_secret_utf8(&vault, api_key_vault_ref.as_str())?;
            Ok(ResolvedCredential::ApiKey { token, source: "auth_profile".to_owned() })
        }
        AuthCredential::Oauth { access_token_vault_ref, client_id, .. } => {
            let token = load_vault_secret_utf8(&vault, access_token_vault_ref.as_str())?;
            Ok(ResolvedCredential::Bearer {
                token,
                source: "auth_profile".to_owned(),
                oauth_kind: oauth_kind_for_profile(&profile.provider.kind, client_id.as_deref()),
            })
        }
    }
}

fn open_auth_profile_vault(state_root: &Path) -> Result<Vault, anyhow::Error> {
    Vault::open_with_config(VaultConfig {
        root: Some(state_root.join("vault")),
        identity_store_root: Some(state_root.join("identity")),
        ..VaultConfig::default()
    })
    .with_context(|| format!("failed to open auth profile vault at {}", state_root.display()))
}

fn target_uses_minimax_auth(target: &ConsoleProbeTarget) -> bool {
    target
        .auth_profile_provider_kind
        .as_deref()
        .is_some_and(|kind| kind.eq_ignore_ascii_case("minimax"))
}

fn target_uses_openai_chatgpt_oauth(
    target: &ConsoleProbeTarget,
    credential: &ResolvedCredential,
) -> bool {
    target.kind == OPENAI_COMPATIBLE_PROVIDER_KIND
        && matches!(
            credential,
            ResolvedCredential::Bearer {
                oauth_kind: Some(ResolvedOauthProfileKind::OpenAiChatGptLogin),
                ..
            }
        )
}

fn oauth_kind_for_profile(
    provider_kind: &AuthProviderKind,
    client_id: Option<&str>,
) -> Option<ResolvedOauthProfileKind> {
    let is_chatgpt_login = provider_kind == &AuthProviderKind::Openai
        && client_id.map(str::trim).is_some_and(|value| value == OPENAI_CHATGPT_OAUTH_CLIENT_ID);
    is_chatgpt_login.then_some(ResolvedOauthProfileKind::OpenAiChatGptLogin)
}

fn expected_auth_provider_for_probe_target(
    target: &ConsoleProbeTarget,
) -> Option<AuthProviderKind> {
    if expected_custom_auth_provider_name_for_probe_target(target).is_some() {
        return Some(AuthProviderKind::Custom);
    }
    match target.kind.as_str() {
        OPENAI_COMPATIBLE_PROVIDER_KIND => Some(AuthProviderKind::Openai),
        ANTHROPIC_PROVIDER_KIND => Some(AuthProviderKind::Anthropic),
        _ => None,
    }
}

fn expected_custom_auth_provider_name_for_probe_target(
    target: &ConsoleProbeTarget,
) -> Option<&'static str> {
    let auth_provider_kind =
        target.auth_profile_provider_kind.as_deref()?.trim().to_ascii_lowercase();
    match auth_provider_kind.as_str() {
        "minimax" | "minimax-portal" => Some("minimax"),
        "xai" | "x-ai" | "grok" => Some("xai"),
        "google_gemini" | "google-gemini" | "gemini" => Some("google_gemini"),
        "google_gemini_cli" | "google-gemini-cli" | "gemini_cli" | "gemini-cli" => {
            Some("google_gemini_cli")
        }
        "openrouter" | "open-router" => Some("openrouter"),
        _ => None,
    }
}

fn load_vault_secret_utf8(
    vault: &palyra_vault::Vault,
    vault_ref: &str,
) -> Result<String, anyhow::Error> {
    let parsed =
        VaultRef::parse(vault_ref).with_context(|| format!("invalid vault ref '{}'", vault_ref))?;
    let bytes = vault
        .get_secret(&parsed.scope, parsed.key.as_str())
        .with_context(|| format!("failed to load vault secret '{}'", parsed.key))?;
    String::from_utf8(bytes)
        .with_context(|| format!("vault secret '{}' must contain valid UTF-8", parsed.key))
}

fn provider_models_endpoint_for_probe(
    target: &ConsoleProbeTarget,
    base_url: &str,
    credential: &ResolvedCredential,
) -> Result<ProviderModelsEndpoint, anyhow::Error> {
    if target_uses_openai_chatgpt_oauth(target, credential) {
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

// Configured base URLs appear both with and without a trailing `/v1` or
// `/openai` segment; normalize so either form probes the same models endpoint.
fn provider_models_endpoint(base_url: &str) -> Result<reqwest::Url, anyhow::Error> {
    let trimmed = base_url.trim().trim_end_matches('/');
    let raw = if trimmed.ends_with("/v1") || trimmed.ends_with("/openai") {
        format!("{trimmed}/models")
    } else {
        format!("{trimmed}/v1/models")
    };
    reqwest::Url::parse(raw.as_str())
        .with_context(|| format!("invalid provider base_url: {base_url}"))
}

fn parse_discovered_model_ids(
    body: &str,
    response_format: ProviderModelsResponseFormat,
) -> Result<Vec<String>, anyhow::Error> {
    match response_format {
        ProviderModelsResponseFormat::OpenAiCompatible => parse_openai_compatible_model_ids(body),
        ProviderModelsResponseFormat::OpenAiCodexBackend => parse_openai_codex_model_ids(body),
    }
}

fn parse_openai_compatible_model_ids(body: &str) -> Result<Vec<String>, anyhow::Error> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    Ok(value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.get("id").and_then(serde_json::Value::as_str))
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default())
}

fn parse_openai_codex_model_ids(body: &str) -> Result<Vec<String>, anyhow::Error> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    let Some(entries) = value.get("models").and_then(serde_json::Value::as_array) else {
        return parse_openai_compatible_model_ids(body);
    };

    let mut sortable = Vec::<(i64, String)>::new();
    for entry in entries {
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
        if sortable.iter().any(|(_, existing)| existing == model_id) {
            continue;
        }
        sortable.push((codex_model_priority(entry), model_id.to_owned()));
    }
    sortable.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    Ok(sortable.into_iter().map(|(_, model_id)| model_id).collect())
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

fn classify_provider_failure(status_code: u16) -> String {
    match status_code {
        401 | 403 => "auth_failed".to_owned(),
        429 => "rate_limited".to_owned(),
        500..=599 => "endpoint_failed".to_owned(),
        _ => "unexpected_response".to_owned(),
    }
}

fn sanitize_provider_error(body: &str, status_code: u16) -> String {
    let trimmed = redact_auth_error(body).trim().to_owned();
    if trimmed.is_empty() {
        format!("provider returned HTTP {status_code}")
    } else {
        format!("provider returned HTTP {status_code}: {trimmed}")
    }
}

fn sanitize_probe_error(message: &str) -> String {
    let trimmed = redact_auth_error(message).trim().to_owned();
    if trimmed.is_empty() {
        "provider probe failed".to_owned()
    } else {
        trimmed
    }
}

fn normalize_optional_text(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_auth::{AuthCredential, AuthProfileRegistry, AuthProviderKind};
    use palyra_common::daemon_config_schema::FileModelProviderRegistryEntry;
    use palyra_vault::{BackendPreference, VaultConfig, VaultScope};

    fn sample_probe_target(allow_private_base_url: bool) -> ConsoleProbeTarget {
        ConsoleProbeTarget {
            provider_id: "openai-primary".to_owned(),
            kind: OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned(),
            enabled: true,
            endpoint_base_url: Some("http://127.0.0.1:11434/v1".to_owned()),
            allow_private_base_url,
            auth_profile_id: None,
            auth_profile_provider_kind: None,
            inline_api_key: None,
            vault_ref: None,
            configured_model_ids: Vec::new(),
        }
    }

    #[test]
    fn console_probe_targets_inherit_global_private_base_url_policy() {
        let config = FileModelProviderConfig {
            kind: Some(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
            allow_private_base_url: Some(true),
            providers: Some(vec![FileModelProviderRegistryEntry {
                provider_id: Some("local-provider".to_owned()),
                kind: Some(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
                base_url: Some("http://127.0.0.1:11434/v1".to_owned()),
                ..FileModelProviderRegistryEntry::default()
            }]),
            ..FileModelProviderConfig::default()
        };

        let targets = build_console_probe_targets_with_env(&config, None);

        assert_eq!(targets.len(), 1);
        assert!(
            targets[0].allow_private_base_url,
            "provider probes should carry the same private-network opt-in as model runtime"
        );
    }

    #[test]
    fn console_probe_targets_allow_provider_private_base_url_override() {
        let config = FileModelProviderConfig {
            kind: Some(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
            allow_private_base_url: Some(true),
            providers: Some(vec![FileModelProviderRegistryEntry {
                provider_id: Some("public-provider".to_owned()),
                kind: Some(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
                base_url: Some("https://api.openai.com/v1".to_owned()),
                allow_private_base_url: Some(false),
                ..FileModelProviderRegistryEntry::default()
            }]),
            ..FileModelProviderConfig::default()
        };

        let targets = build_console_probe_targets_with_env(&config, None);

        assert_eq!(targets.len(), 1);
        assert!(
            !targets[0].allow_private_base_url,
            "provider-level allow_private_base_url=false should override the global opt-in"
        );
    }

    #[test]
    fn console_probe_targets_use_legacy_xai_identity_and_alias_filter() {
        let config = FileModelProviderConfig {
            kind: Some(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
            openai_base_url: Some("https://api.x.ai/v1".to_owned()),
            auth_profile_id: Some("xai-oauth-test".to_owned()),
            auth_provider_kind: Some("xai".to_owned()),
            ..FileModelProviderConfig::default()
        };

        let targets = build_console_probe_targets_with_env(&config, None);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].provider_id, "xai-primary");
        assert_eq!(targets[0].auth_profile_id.as_deref(), Some("xai-oauth-test"));
        assert_eq!(targets[0].auth_profile_provider_kind.as_deref(), Some("xai"));
        assert!(provider_matches_filter(&targets[0], "xai"));
        assert!(provider_matches_filter(&targets[0], "grok"));
    }

    #[test]
    fn console_probe_endpoint_policy_blocks_private_base_url_without_opt_in() {
        let target = sample_probe_target(false);

        let error = validate_console_probe_endpoint_policy(
            &target,
            target.endpoint_base_url.as_deref().expect("test target should include base URL"),
        )
        .expect_err("private probe target should be rejected without explicit opt-in");

        assert!(error.to_string().contains("allow_private_base_url"));
    }

    #[test]
    fn console_probe_endpoint_policy_allows_private_base_url_with_opt_in() {
        let target = sample_probe_target(true);

        validate_console_probe_endpoint_policy(
            &target,
            target.endpoint_base_url.as_deref().expect("test target should include base URL"),
        )
        .expect("private probe target should be allowed with explicit opt-in");
    }

    #[test]
    fn chatgpt_oauth_probe_uses_codex_models_endpoint() {
        let target = ConsoleProbeTarget {
            endpoint_base_url: Some("https://api.openai.com/v1".to_owned()),
            ..sample_probe_target(false)
        };
        let credential = ResolvedCredential::Bearer {
            token: "token".to_owned(),
            source: "auth_profile".to_owned(),
            oauth_kind: Some(ResolvedOauthProfileKind::OpenAiChatGptLogin),
        };

        let endpoint = provider_models_endpoint_for_probe(
            &target,
            target.endpoint_base_url.as_deref().expect("test target should include base URL"),
            &credential,
        )
        .expect("ChatGPT OAuth should produce a Codex models endpoint");

        assert_eq!(endpoint.url.as_str(), OPENAI_CODEX_MODELS_ENDPOINT);
        assert_eq!(endpoint.base_url, OPENAI_CODEX_BACKEND_BASE_URL);
        assert_eq!(endpoint.response_format, ProviderModelsResponseFormat::OpenAiCodexBackend);
    }

    #[test]
    fn openai_api_key_probe_keeps_openai_compatible_models_endpoint() {
        let target = ConsoleProbeTarget {
            endpoint_base_url: Some("https://api.openai.com/v1".to_owned()),
            ..sample_probe_target(false)
        };
        let credential = ResolvedCredential::ApiKey {
            token: "token".to_owned(),
            source: "auth_profile".to_owned(),
        };

        let endpoint = provider_models_endpoint_for_probe(
            &target,
            target.endpoint_base_url.as_deref().expect("test target should include base URL"),
            &credential,
        )
        .expect("OpenAI API key should produce the public models endpoint");

        assert_eq!(endpoint.url.as_str(), "https://api.openai.com/v1/models");
        assert_eq!(endpoint.response_format, ProviderModelsResponseFormat::OpenAiCompatible);
    }

    #[test]
    fn auth_profile_probe_resolves_xai_oauth_from_registry_state_root_vault() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let state_root = tempdir.path().join("state");
        let identity_root = state_root.join("identity");
        let registry =
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should open");
        registry
            .set_profile(palyra_auth::AuthProfileSetRequest {
                profile_id: "xai-oauth-test".to_owned(),
                provider: palyra_auth::AuthProvider {
                    kind: AuthProviderKind::Custom,
                    custom_name: Some("xai".to_owned()),
                },
                profile_name: "xAI OAuth".to_owned(),
                scope: palyra_auth::AuthProfileScope::Global,
                credential: AuthCredential::Oauth {
                    access_token_vault_ref: "global/xai_access".to_owned(),
                    refresh_token_vault_ref: "global/xai_refresh".to_owned(),
                    token_endpoint: "https://auth.x.ai/oauth/token".to_owned(),
                    client_id: Some("grok-cli".to_owned()),
                    client_secret_vault_ref: None,
                    scopes: Vec::new(),
                    expires_at_unix_ms: None,
                    refresh_state: Default::default(),
                },
            })
            .expect("profile should persist");
        let vault = palyra_vault::Vault::open_with_config(VaultConfig {
            root: Some(state_root.join("vault")),
            identity_store_root: Some(identity_root),
            backend_preference: BackendPreference::EncryptedFile,
            ..VaultConfig::default()
        })
        .expect("runtime vault should open");
        let scope = "global".parse::<VaultScope>().expect("scope should parse");
        vault
            .put_secret(&scope, "xai_access", b"runtime-xai-oauth-token")
            .expect("access token should persist");

        let target = ConsoleProbeTarget {
            provider_id: "xai-primary".to_owned(),
            kind: OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned(),
            enabled: true,
            endpoint_base_url: Some("https://api.x.ai/v1".to_owned()),
            allow_private_base_url: false,
            auth_profile_id: Some("xai-oauth-test".to_owned()),
            auth_profile_provider_kind: Some("xai".to_owned()),
            inline_api_key: None,
            vault_ref: None,
            configured_model_ids: Vec::new(),
        };

        let credential = resolve_auth_profile_credential(&registry, &target, "xai-oauth-test")
            .expect("credential lookup should succeed");

        match credential {
            ResolvedCredential::Bearer { token, source, oauth_kind } => {
                assert_eq!(token, "runtime-xai-oauth-token");
                assert_eq!(source, "auth_profile");
                assert_eq!(oauth_kind, None);
            }
            ResolvedCredential::ApiKey { .. } => panic!("xAI OAuth profile should be bearer"),
        }
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

        let discovered =
            parse_discovered_model_ids(&body, ProviderModelsResponseFormat::OpenAiCodexBackend)
                .expect("Codex model response should parse");

        assert_eq!(discovered, vec!["gpt-5.4", "gpt-5.3-codex"]);
    }
}
