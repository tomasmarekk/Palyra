use crate::*;
use palyra_auth::{AuthCredential, AuthProfileRegistry, AuthProviderKind};
use palyra_common::daemon_config_schema::FileModelProviderConfig;
use palyra_common::redaction::redact_auth_error;
use palyra_vault::VaultRef;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION};
use std::{
    collections::BTreeMap,
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const OPENAI_COMPATIBLE_PROVIDER_KIND: &str = "openai_compatible";
const ANTHROPIC_PROVIDER_KIND: &str = "anthropic";
const DETERMINISTIC_PROVIDER_KIND: &str = "deterministic";
const OPENAI_DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const ANTHROPIC_DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_API_VERSION: &str = "2023-06-01";
const MINIMAX_AUTH_PROVIDER_KIND: &str = "minimax";
const PROVIDER_CHECKS_CACHE_PATH: &str = "models/provider_checks.json";
const CURATED_TEXT_MODELS: &[&str] = &["gpt-4o-mini", "gpt-4.1-mini"];
const CURATED_EMBEDDING_MODELS: &[&str] = &["text-embedding-3-small", "text-embedding-3-large"];

#[derive(Debug, Serialize)]
pub(crate) struct ModelsStatusPayload {
    pub(crate) path: String,
    pub(crate) provider_kind: String,
    pub(crate) openai_base_url: Option<String>,
    pub(crate) text_model: Option<String>,
    pub(crate) embeddings_model: Option<String>,
    pub(crate) embeddings_dims: Option<u32>,
    pub(crate) auth_profile_id: Option<String>,
    pub(crate) api_key_configured: bool,
    pub(crate) default_chat_model_id: Option<String>,
    pub(crate) default_embeddings_model_id: Option<String>,
    pub(crate) failover_enabled: bool,
    pub(crate) response_cache_enabled: bool,
    pub(crate) registry_provider_count: usize,
    pub(crate) registry_model_count: usize,
    pub(crate) registry_valid: bool,
    pub(crate) validation_issues: Vec<String>,
    pub(crate) migrated: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ModelCatalogEntry<'a> {
    pub(crate) target: &'a str,
    pub(crate) id: String,
    pub(crate) configured: bool,
    pub(crate) preferred: bool,
    pub(crate) source: &'a str,
}

#[derive(Debug, Serialize)]
pub(crate) struct RegistryProviderEntry {
    pub(crate) provider_id: String,
    pub(crate) display_name: Option<String>,
    pub(crate) kind: String,
    pub(crate) base_url: Option<String>,
    pub(crate) enabled: bool,
    pub(crate) auth_profile_id: Option<String>,
    pub(crate) auth_provider_kind: Option<String>,
    pub(crate) api_key_configured: bool,
    pub(crate) source: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RegistryModelEntry {
    pub(crate) model_id: String,
    pub(crate) provider_id: String,
    pub(crate) role: String,
    pub(crate) enabled: bool,
    pub(crate) metadata_source: String,
    pub(crate) operator_override: bool,
    pub(crate) tool_calls: bool,
    pub(crate) json_mode: bool,
    pub(crate) vision: bool,
    pub(crate) audio_transcribe: bool,
    pub(crate) embeddings: bool,
    pub(crate) max_context_tokens: Option<u32>,
    pub(crate) cost_tier: String,
    pub(crate) latency_tier: String,
    pub(crate) recommended_use_cases: Vec<String>,
    pub(crate) known_limitations: Vec<String>,
    pub(crate) source: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct ModelsListPayload {
    pub(crate) status: ModelsStatusPayload,
    pub(crate) models: Vec<ModelCatalogEntry<'static>>,
    pub(crate) providers: Vec<RegistryProviderEntry>,
    pub(crate) registry_models: Vec<RegistryModelEntry>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ModelsMutationPayload {
    pub(crate) path: String,
    pub(crate) provider_kind: String,
    pub(crate) target: &'static str,
    pub(crate) model: String,
    pub(crate) embeddings_dims: Option<u32>,
    pub(crate) backups: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProviderConnectionCheckPayload {
    pub(crate) provider_id: String,
    pub(crate) kind: String,
    pub(crate) enabled: bool,
    pub(crate) endpoint_base_url: Option<String>,
    pub(crate) credential_source: String,
    pub(crate) state: String,
    pub(crate) message: String,
    pub(crate) checked_at_unix_ms: i64,
    pub(crate) cache_status: String,
    pub(crate) live_discovery_verified: bool,
    pub(crate) discovery_source: String,
    pub(crate) discovered_model_ids: Vec<String>,
    pub(crate) configured_model_ids: Vec<String>,
    pub(crate) latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelsConnectionPayload {
    pub(crate) path: String,
    pub(crate) mode: &'static str,
    pub(crate) timeout_ms: u64,
    pub(crate) provider_filter: Option<String>,
    pub(crate) provider_count: usize,
    pub(crate) providers: Vec<ProviderConnectionCheckPayload>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelsExplainCandidatePayload {
    pub(crate) order: usize,
    pub(crate) provider_id: String,
    pub(crate) provider_kind: String,
    pub(crate) model_id: String,
    pub(crate) role: String,
    pub(crate) selected: bool,
    pub(crate) reason: String,
    pub(crate) cost_tier: String,
    pub(crate) latency_tier: String,
    pub(crate) tool_calls: bool,
    pub(crate) json_mode: bool,
    pub(crate) vision: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelsExplainPayload {
    pub(crate) path: String,
    pub(crate) requested_model_id: Option<String>,
    pub(crate) resolved_model_id: Option<String>,
    pub(crate) json_mode: bool,
    pub(crate) vision: bool,
    pub(crate) failover_enabled: bool,
    pub(crate) response_cache_enabled: bool,
    pub(crate) explanation: Vec<String>,
    pub(crate) candidates: Vec<ModelsExplainCandidatePayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ProviderChecksCacheDocument {
    entries: BTreeMap<String, CachedProviderCheckEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedProviderCheckEntry {
    expires_at_unix_ms: i64,
    payload: ProviderConnectionCheckPayload,
}

#[derive(Debug, Clone)]
struct ProbeableProvider {
    provider_id: String,
    kind: String,
    enabled: bool,
    endpoint_base_url: Option<String>,
    auth_profile_id: Option<String>,
    auth_provider_kind: Option<String>,
    inline_api_key: Option<String>,
    vault_ref: Option<String>,
    configured_model_ids: Vec<String>,
}

#[derive(Debug, Clone)]
enum ResolvedCredential {
    ApiKey { token: String, source: String },
    Bearer { token: String, source: String },
}

pub(crate) fn run_models(command: ModelsCommand) -> Result<()> {
    match command {
        ModelsCommand::Status { path, json } => {
            let payload = load_models_status(path)?;
            emit_models_status(&payload, output::preferred_json(json))
        }
        ModelsCommand::List { path, json } => {
            let payload = build_models_list(path)?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode models list as JSON")?;
            } else {
                println!(
                    "models.list provider_kind={} text_model={} embeddings_model={} auth_profile_id={} registry_providers={} registry_models={} registry_valid={}",
                    payload.status.provider_kind,
                    payload.status.text_model.as_deref().unwrap_or("none"),
                    payload.status.embeddings_model.as_deref().unwrap_or("none"),
                    payload.status.auth_profile_id.as_deref().unwrap_or("none"),
                    payload.providers.len(),
                    payload.registry_models.len(),
                    payload.status.registry_valid
                );
                for entry in payload.providers {
                    println!(
                        "models.provider id={} kind={} enabled={} auth_profile_id={} api_key_configured={} source={}",
                        entry.provider_id,
                        entry.kind,
                        entry.enabled,
                        entry.auth_profile_id.as_deref().unwrap_or("none"),
                        entry.api_key_configured,
                        entry.source
                    );
                }
                for entry in payload.registry_models {
                    println!(
                        "models.registry_model id={} provider_id={} role={} enabled={} json_mode={} vision={} embeddings={} source={}",
                        entry.model_id,
                        entry.provider_id,
                        entry.role,
                        entry.enabled,
                        entry.json_mode,
                        entry.vision,
                        entry.embeddings,
                        entry.source
                    );
                }
                for entry in payload.models {
                    println!(
                        "models.entry target={} id={} configured={} preferred={} source={}",
                        entry.target, entry.id, entry.configured, entry.preferred, entry.source
                    );
                }
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        ModelsCommand::TestConnection { path, provider, timeout_ms, refresh, json } => {
            let payload = run_provider_checks(path, provider, timeout_ms, refresh, false)?;
            emit_models_connection(&payload, output::preferred_json(json))
        }
        ModelsCommand::Discover { path, provider, timeout_ms, refresh, json } => {
            let payload = run_provider_checks(path, provider, timeout_ms, refresh, true)?;
            emit_models_connection(&payload, output::preferred_json(json))
        }
        ModelsCommand::Explain { path, model, json_mode, vision, json } => {
            let payload = explain_models_routing(path, model, json_mode, vision)?;
            emit_models_explain(&payload, output::preferred_json(json))
        }
        ModelsCommand::Set { model, path, backups, json } => {
            let payload = mutate_model_defaults(path, backups, "text", model, None)?;
            emit_models_mutation(&payload, output::preferred_json(json))
        }
        ModelsCommand::SetEmbeddings { model, dims, path, backups, json } => {
            let payload = mutate_model_defaults(path, backups, "embeddings", model, dims)?;
            emit_models_mutation(&payload, output::preferred_json(json))
        }
    }
}

fn emit_models_status(payload: &ModelsStatusPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(payload, "failed to encode models status as JSON")?;
    } else {
        println!(
            "models.status path={} provider_kind={} text_model={} embeddings_model={} auth_profile_id={} api_key_configured={} migrated={}",
            payload.path,
            payload.provider_kind,
            payload.text_model.as_deref().unwrap_or("none"),
            payload.embeddings_model.as_deref().unwrap_or("none"),
            payload.auth_profile_id.as_deref().unwrap_or("none"),
            payload.api_key_configured,
            payload.migrated
        );
        println!(
            "models.status.provider base_url={} embeddings_dims={} default_chat_model={} default_embeddings_model={} registry_providers={} registry_models={} failover_enabled={} response_cache_enabled={} registry_valid={}",
            payload.openai_base_url.as_deref().unwrap_or("none"),
            payload
                .embeddings_dims
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            payload.default_chat_model_id.as_deref().unwrap_or("none"),
            payload.default_embeddings_model_id.as_deref().unwrap_or("none"),
            payload.registry_provider_count,
            payload.registry_model_count,
            payload.failover_enabled,
            payload.response_cache_enabled,
            payload.registry_valid
        );
        for issue in &payload.validation_issues {
            println!("models.status.validation issue={issue}");
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_models_mutation(payload: &ModelsMutationPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(payload, "failed to encode models mutation as JSON")?;
    } else {
        println!(
            "models.set path={} provider_kind={} target={} model={} embeddings_dims={} backups={}",
            payload.path,
            payload.provider_kind,
            payload.target,
            payload.model,
            payload
                .embeddings_dims
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            payload.backups
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_models_connection(payload: &ModelsConnectionPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(payload, "failed to encode models connection payload as JSON")?;
    } else {
        println!(
            "models.{} path={} provider_filter={} timeout_ms={} providers={}",
            payload.mode,
            payload.path,
            payload.provider_filter.as_deref().unwrap_or("all"),
            payload.timeout_ms,
            payload.provider_count
        );
        for provider in &payload.providers {
            println!(
                "models.{}.provider id={} kind={} enabled={} state={} latency_ms={} cache_status={} credential_source={} discovered_models={} message={}",
                payload.mode,
                provider.provider_id,
                provider.kind,
                provider.enabled,
                provider.state,
                provider
                    .latency_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
                provider.cache_status,
                provider.credential_source,
                provider.discovered_model_ids.len(),
                provider.message
            );
            println!(
                "models.{}.provider_status id={} live_discovery_verified={} registry_fallback_used={} configured_models={}",
                payload.mode,
                provider.provider_id,
                provider.live_discovery_verified,
                provider.discovery_source == "registry_fallback",
                provider.configured_model_ids.len()
            );
            for model_id in &provider.discovered_model_ids {
                println!(
                    "models.{}.model provider_id={} id={} source={}",
                    payload.mode, provider.provider_id, model_id, provider.discovery_source
                );
            }
            if provider.discovery_source == "registry_fallback" {
                for model_id in &provider.configured_model_ids {
                    println!(
                        "models.{}.registry_model provider_id={} id={} source=registry_fallback",
                        payload.mode, provider.provider_id, model_id
                    );
                }
            }
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_models_explain(payload: &ModelsExplainPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(payload, "failed to encode models explain payload as JSON")?;
    } else {
        println!(
            "models.explain path={} requested_model={} resolved_model={} json_mode={} vision={} failover_enabled={} response_cache_enabled={} candidates={}",
            payload.path,
            payload.requested_model_id.as_deref().unwrap_or("default"),
            payload.resolved_model_id.as_deref().unwrap_or("none"),
            payload.json_mode,
            payload.vision,
            payload.failover_enabled,
            payload.response_cache_enabled,
            payload.candidates.len()
        );
        for line in &payload.explanation {
            println!("models.explain.detail {line}");
        }
        for candidate in &payload.candidates {
            println!(
                "models.explain.candidate order={} provider_id={} provider_kind={} model_id={} selected={} reason={}",
                candidate.order,
                candidate.provider_id,
                candidate.provider_kind,
                candidate.model_id,
                candidate.selected,
                candidate.reason
            );
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

pub(crate) fn build_models_list(path: Option<String>) -> Result<ModelsListPayload> {
    let overview = load_models_overview(path)?;
    let status = overview.status;
    let mut models = Vec::new();
    append_catalog_entries(
        &mut models,
        "text",
        CURATED_TEXT_MODELS,
        status.text_model.as_deref(),
        Some("gpt-4o-mini"),
    );
    append_catalog_entries(
        &mut models,
        "embeddings",
        CURATED_EMBEDDING_MODELS,
        status.embeddings_model.as_deref(),
        Some("text-embedding-3-small"),
    );
    if let Some(configured) = status.text_model.as_deref() {
        append_ad_hoc_entry(&mut models, "text", configured);
    }
    if let Some(configured) = status.embeddings_model.as_deref() {
        append_ad_hoc_entry(&mut models, "embeddings", configured);
    }
    Ok(ModelsListPayload {
        status,
        models,
        providers: overview.providers,
        registry_models: overview.models,
    })
}

fn append_catalog_entries(
    target_entries: &mut Vec<ModelCatalogEntry<'static>>,
    target: &'static str,
    catalog: &[&str],
    configured: Option<&str>,
    preferred: Option<&str>,
) {
    for model in catalog {
        target_entries.push(ModelCatalogEntry {
            target,
            id: (*model).to_owned(),
            configured: configured.is_some_and(|value| value == *model),
            preferred: preferred.is_some_and(|value| value == *model),
            source: "curated",
        });
    }
}

fn append_ad_hoc_entry(
    target_entries: &mut Vec<ModelCatalogEntry<'static>>,
    target: &'static str,
    configured: &str,
) {
    if target_entries.iter().any(|entry| entry.target == target && entry.id == configured) {
        return;
    }
    target_entries.push(ModelCatalogEntry {
        target,
        id: configured.to_owned(),
        configured: true,
        preferred: false,
        source: "configured",
    });
}

pub(crate) fn mutate_model_defaults(
    path: Option<String>,
    backups: usize,
    target: &'static str,
    model: String,
    dims: Option<u32>,
) -> Result<ModelsMutationPayload> {
    let path = resolve_config_path(path, false)?;
    let path_ref = Path::new(&path);
    let (mut document, _) = load_document_for_mutation(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;
    let has_registry = registry_configured(&document)?;
    if !has_registry {
        set_value_at_path(
            &mut document,
            "model_provider.kind",
            toml::Value::String(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
        )
        .context("invalid config key path: model_provider.kind")?;
        let existing_base_url =
            get_string_value_at_path(&document, "model_provider.openai_base_url")?;
        if existing_base_url.is_none() {
            set_value_at_path(
                &mut document,
                "model_provider.openai_base_url",
                toml::Value::String(OPENAI_DEFAULT_BASE_URL.to_owned()),
            )
            .context("invalid config key path: model_provider.openai_base_url")?;
        }
    }

    match target {
        "text" => {
            let key = if has_registry {
                "model_provider.default_chat_model_id"
            } else {
                "model_provider.openai_model"
            };
            set_value_at_path(&mut document, key, toml::Value::String(model.clone()))
                .with_context(|| format!("invalid config key path: {key}"))?;
        }
        "embeddings" => {
            let key = if has_registry {
                "model_provider.default_embeddings_model_id"
            } else {
                "model_provider.openai_embeddings_model"
            };
            set_value_at_path(&mut document, key, toml::Value::String(model.clone()))
                .with_context(|| format!("invalid config key path: {key}"))?;
            if let Some(value) = dims {
                set_value_at_path(
                    &mut document,
                    "model_provider.openai_embeddings_dims",
                    toml::Value::Integer(i64::from(value)),
                )
                .context("invalid config key path: model_provider.openai_embeddings_dims")?;
            }
        }
        _ => anyhow::bail!("unsupported model target: {target}"),
    }

    validate_daemon_compatible_document(&document).with_context(|| {
        format!("mutated config {} does not match daemon schema", path_ref.display())
    })?;
    write_document_with_backups(path_ref, &document, backups)
        .with_context(|| format!("failed to persist config {}", path_ref.display()))?;
    Ok(ModelsMutationPayload {
        path,
        provider_kind: get_string_value_at_path(&document, "model_provider.kind")?
            .unwrap_or_else(|| OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
        target,
        model,
        embeddings_dims: dims,
        backups,
    })
}

pub(crate) fn load_models_status(path: Option<String>) -> Result<ModelsStatusPayload> {
    Ok(load_models_overview(path)?.status)
}

fn run_provider_checks(
    path: Option<String>,
    provider_filter: Option<String>,
    timeout_ms: u64,
    refresh: bool,
    discover: bool,
) -> Result<ModelsConnectionPayload> {
    let overview = load_models_overview(path)?;
    let mode = if discover { "discover" } else { "test_connection" };
    let provider_targets = build_probeable_providers(&overview)?;
    let provider_filter =
        provider_filter.as_deref().and_then(normalize_optional_text).map(str::to_owned);
    let provider_filter_ref = provider_filter.as_deref();
    let filtered_targets = provider_targets
        .into_iter()
        .filter(|provider| {
            provider_filter_ref
                .map(|filter| provider.provider_id == filter || provider.kind == filter)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    if filtered_targets.is_empty() {
        anyhow::bail!(
            "no provider matched '{}'",
            provider_filter.as_deref().unwrap_or("configured registry")
        );
    }

    let now_unix_ms = unix_timestamp_ms()?;
    let mut cache = load_provider_checks_cache()?;
    let mut auth_registry = None;
    let mut vault = None;
    let mut providers = Vec::with_capacity(filtered_targets.len());
    for target in filtered_targets {
        let cache_key = provider_check_cache_key(mode, &target);
        if !refresh {
            if let Some(cached) =
                read_cached_provider_check(&cache, cache_key.as_str(), now_unix_ms)
            {
                providers.push(cached);
                continue;
            }
        }

        let ttl_ms = provider_check_ttl_ms(&overview, discover);
        let payload = probe_provider(
            &target,
            timeout_ms,
            now_unix_ms,
            discover,
            &mut auth_registry,
            &mut vault,
        );
        write_cached_provider_check(&mut cache, cache_key, payload.clone(), ttl_ms, now_unix_ms);
        providers.push(payload);
    }
    persist_provider_checks_cache(&cache)?;

    Ok(ModelsConnectionPayload {
        path: overview.status.path,
        mode,
        timeout_ms,
        provider_filter,
        provider_count: providers.len(),
        providers,
    })
}

fn explain_models_routing(
    path: Option<String>,
    requested_model_id: Option<String>,
    json_mode: bool,
    vision: bool,
) -> Result<ModelsExplainPayload> {
    let overview = load_models_overview(path)?;
    let requested_model_id =
        requested_model_id.as_deref().and_then(normalize_optional_text).map(str::to_owned);
    let provider_kind_by_id = overview
        .providers
        .iter()
        .map(|provider| (provider.provider_id.clone(), provider.kind.clone()))
        .collect::<BTreeMap<_, _>>();
    let requested = requested_model_id
        .clone()
        .or_else(|| overview.status.default_chat_model_id.clone())
        .or_else(|| overview.status.text_model.clone());

    let compatible = overview
        .models
        .iter()
        .filter(|model| {
            model.enabled
                && model.role == "chat"
                && provider_enabled(overview.providers.as_slice(), model.provider_id.as_str())
                && (!json_mode || model.json_mode)
                && (!vision || model.vision)
        })
        .cloned()
        .collect::<Vec<_>>();

    let mut explanation = Vec::new();
    if compatible.is_empty() {
        explanation
            .push("No enabled chat model satisfies the requested capability envelope.".to_owned());
        return Ok(ModelsExplainPayload {
            path: overview.status.path,
            requested_model_id,
            resolved_model_id: None,
            json_mode,
            vision,
            failover_enabled: overview.status.failover_enabled,
            response_cache_enabled: overview.status.response_cache_enabled,
            explanation,
            candidates: Vec::new(),
        });
    }

    let selected = requested
        .as_deref()
        .and_then(|model_id| compatible.iter().find(|model| model.model_id == model_id).cloned());
    let Some(primary) = selected.or_else(|| compatible.first().cloned()) else {
        unreachable!("compatible models list cannot be empty");
    };
    if let Some(requested_model_id) = requested.as_deref() {
        if primary.model_id == requested_model_id {
            explanation.push(format!(
                "Primary selection '{}' came from the requested/default chat model.",
                primary.model_id
            ));
        } else {
            explanation.push(format!(
                "Requested model '{}' was unavailable, so the first compatible chat model '{}' was selected.",
                requested_model_id, primary.model_id
            ));
        }
    }
    if overview.status.failover_enabled && requested_model_id.is_none() {
        explanation.push(
            "Failover is enabled, so compatible models from other providers remain in the fallback chain.".to_owned(),
        );
    } else if requested_model_id.is_some() {
        explanation.push(
            "An explicit model pin suppresses cross-provider failover so the command explains only the pinned choice.".to_owned(),
        );
    } else {
        explanation.push(
            "Failover is disabled, so only the primary compatible model remains eligible."
                .to_owned(),
        );
    }
    if overview.status.response_cache_enabled {
        explanation.push(
            "Response cache is enabled for compatible read-only requests; tool-bearing responses stay uncached.".to_owned(),
        );
    }

    let mut fallbacks = compatible
        .into_iter()
        .filter(|model| model.model_id != primary.model_id)
        .collect::<Vec<_>>();
    fallbacks.sort_by(|left, right| {
        fallback_cost_rank(left.cost_tier.as_str())
            .cmp(&fallback_cost_rank(right.cost_tier.as_str()))
            .then(
                fallback_latency_rank(left.latency_tier.as_str())
                    .cmp(&fallback_latency_rank(right.latency_tier.as_str())),
            )
            .then_with(|| left.model_id.cmp(&right.model_id))
    });

    let mut candidates = vec![ModelsExplainCandidatePayload {
        order: 1,
        provider_id: primary.provider_id.clone(),
        provider_kind: provider_kind_by_id
            .get(primary.provider_id.as_str())
            .cloned()
            .unwrap_or_else(|| "unknown".to_owned()),
        model_id: primary.model_id.clone(),
        role: primary.role.clone(),
        selected: true,
        reason: if requested_model_id.is_some() {
            "selected explicit/default model".to_owned()
        } else {
            "selected first compatible default".to_owned()
        },
        cost_tier: primary.cost_tier.clone(),
        latency_tier: primary.latency_tier.clone(),
        tool_calls: primary.tool_calls,
        json_mode: primary.json_mode,
        vision: primary.vision,
    }];
    if overview.status.failover_enabled && requested_model_id.is_none() {
        candidates.extend(
            fallbacks
                .into_iter()
                .filter(|candidate| candidate.provider_id != primary.provider_id)
                .enumerate()
                .map(|(index, candidate)| ModelsExplainCandidatePayload {
                    order: index + 2,
                    provider_id: candidate.provider_id.clone(),
                    provider_kind: provider_kind_by_id
                        .get(candidate.provider_id.as_str())
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned()),
                    model_id: candidate.model_id.clone(),
                    role: candidate.role.clone(),
                    selected: false,
                    reason: "eligible cross-provider fallback ranked by cost and latency"
                        .to_owned(),
                    cost_tier: candidate.cost_tier.clone(),
                    latency_tier: candidate.latency_tier.clone(),
                    tool_calls: candidate.tool_calls,
                    json_mode: candidate.json_mode,
                    vision: candidate.vision,
                }),
        );
    }

    Ok(ModelsExplainPayload {
        path: overview.status.path,
        requested_model_id,
        resolved_model_id: Some(primary.model_id),
        json_mode,
        vision,
        failover_enabled: overview.status.failover_enabled,
        response_cache_enabled: overview.status.response_cache_enabled,
        explanation,
        candidates,
    })
}

struct ModelsOverview {
    status: ModelsStatusPayload,
    providers: Vec<RegistryProviderEntry>,
    models: Vec<RegistryModelEntry>,
}

fn load_models_overview(path: Option<String>) -> Result<ModelsOverview> {
    let path = resolve_config_path(path, true)?;
    let (document, migration) = load_document_from_existing_path(Path::new(&path))
        .with_context(|| format!("failed to parse {path}"))?;
    let root_config = parse_root_file_config(&document)?;
    let model_provider = root_config.model_provider.unwrap_or_default();
    let provider_kind =
        model_provider.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let (providers, models) = registry_views_from_config(&model_provider);
    let validation_issues = validate_registry_views(
        providers.as_slice(),
        models.as_slice(),
        model_provider.default_chat_model_id.as_deref(),
        model_provider.default_embeddings_model_id.as_deref(),
    );
    let openai_base_url = model_provider.openai_base_url.clone();
    let text_model = model_provider
        .default_chat_model_id
        .clone()
        .or_else(|| model_provider.openai_model.clone())
        .or_else(|| model_provider.anthropic_model.clone());
    let embeddings_model = model_provider
        .default_embeddings_model_id
        .clone()
        .or_else(|| model_provider.openai_embeddings_model.clone());
    let embeddings_dims = model_provider.openai_embeddings_dims;
    let auth_profile_id = model_provider.auth_profile_id.clone().or_else(|| {
        providers
            .iter()
            .find(|entry| {
                Some(entry.provider_id.as_str())
                    == default_provider_id(models.as_slice(), &model_provider)
            })
            .and_then(|entry| entry.auth_profile_id.clone())
    });
    let api_key_configured =
        model_provider.openai_api_key.as_deref().filter(|value| !value.trim().is_empty()).is_some()
            || model_provider
                .openai_api_key_vault_ref
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .is_some()
            || model_provider
                .anthropic_api_key
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .is_some()
            || model_provider
                .anthropic_api_key_vault_ref
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .is_some()
            || providers.iter().any(|entry| entry.api_key_configured);
    Ok(ModelsOverview {
        status: ModelsStatusPayload {
            path,
            provider_kind,
            openai_base_url,
            text_model,
            embeddings_model,
            embeddings_dims,
            auth_profile_id,
            api_key_configured,
            default_chat_model_id: model_provider.default_chat_model_id,
            default_embeddings_model_id: model_provider.default_embeddings_model_id,
            failover_enabled: model_provider.failover_enabled.unwrap_or(true),
            response_cache_enabled: model_provider.response_cache_enabled.unwrap_or(true),
            registry_provider_count: providers.len(),
            registry_model_count: models.len(),
            registry_valid: validation_issues.is_empty(),
            validation_issues,
            migrated: migration.migrated,
        },
        providers,
        models,
    })
}

fn build_probeable_providers(overview: &ModelsOverview) -> Result<Vec<ProbeableProvider>> {
    let path_ref = Path::new(overview.status.path.as_str());
    let (document, _) = load_document_from_existing_path(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;
    let root_config = parse_root_file_config(&document)?;
    let model_provider = root_config.model_provider.unwrap_or_default();
    let provider_kind =
        model_provider.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let default_provider_id =
        default_provider_id(overview.models.as_slice(), &model_provider).map(str::to_owned);
    let models_by_provider =
        overview.models.iter().fold(BTreeMap::<String, Vec<String>>::new(), |mut acc, model| {
            acc.entry(model.provider_id.clone()).or_default().push(model.model_id.clone());
            acc
        });

    if let Some(entries) = model_provider.providers.as_ref() {
        return Ok(entries
            .iter()
            .map(|entry| {
                let provider_id = entry.provider_id.clone().unwrap_or_default();
                let kind = entry.kind.clone().unwrap_or_else(|| provider_kind.clone());
                let inherit_globals = default_provider_id
                    .as_deref()
                    .is_some_and(|candidate| candidate == provider_id)
                    || default_provider_id.is_none();
                ProbeableProvider {
                    provider_id: provider_id.clone(),
                    kind: kind.clone(),
                    enabled: entry.enabled.unwrap_or(true),
                    endpoint_base_url: entry.base_url.clone().or_else(|| {
                        if inherit_globals {
                            default_base_url_for_kind(kind.as_str(), &model_provider)
                        } else {
                            None
                        }
                    }),
                    auth_profile_id: entry.auth_profile_id.clone().or_else(|| {
                        inherit_globals.then(|| model_provider.auth_profile_id.clone()).flatten()
                    }),
                    auth_provider_kind: entry.auth_provider_kind.clone().or_else(|| {
                        inherit_globals.then(|| model_provider.auth_provider_kind.clone()).flatten()
                    }),
                    inline_api_key: entry.api_key.clone().or_else(|| {
                        inherit_globals
                            .then(|| inline_api_key_for_kind(kind.as_str(), &model_provider))
                            .flatten()
                    }),
                    vault_ref: entry.api_key_vault_ref.clone().or_else(|| {
                        inherit_globals
                            .then(|| vault_ref_for_kind(kind.as_str(), &model_provider))
                            .flatten()
                    }),
                    configured_model_ids: models_by_provider
                        .get(provider_id.as_str())
                        .cloned()
                        .unwrap_or_default(),
                }
            })
            .collect());
    }

    let provider_id = legacy_provider_id(provider_kind.as_str()).to_owned();
    Ok(vec![ProbeableProvider {
        provider_id: provider_id.clone(),
        kind: provider_kind.clone(),
        enabled: true,
        endpoint_base_url: default_base_url_for_kind(provider_kind.as_str(), &model_provider),
        auth_profile_id: model_provider.auth_profile_id.clone(),
        auth_provider_kind: model_provider.auth_provider_kind.clone(),
        inline_api_key: inline_api_key_for_kind(provider_kind.as_str(), &model_provider),
        vault_ref: vault_ref_for_kind(provider_kind.as_str(), &model_provider),
        configured_model_ids: models_by_provider
            .get(provider_id.as_str())
            .cloned()
            .unwrap_or_default(),
    }])
}

fn default_base_url_for_kind(kind: &str, config: &FileModelProviderConfig) -> Option<String> {
    match kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => {
            config.openai_base_url.clone().or_else(|| Some(OPENAI_DEFAULT_BASE_URL.to_owned()))
        }
        ANTHROPIC_PROVIDER_KIND => config
            .anthropic_base_url
            .clone()
            .or_else(|| Some(ANTHROPIC_DEFAULT_BASE_URL.to_owned())),
        _ => None,
    }
}

fn inline_api_key_for_kind(kind: &str, config: &FileModelProviderConfig) -> Option<String> {
    match kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => config.openai_api_key.clone(),
        ANTHROPIC_PROVIDER_KIND => config.anthropic_api_key.clone(),
        _ => None,
    }
}

fn vault_ref_for_kind(kind: &str, config: &FileModelProviderConfig) -> Option<String> {
    match kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => config.openai_api_key_vault_ref.clone(),
        ANTHROPIC_PROVIDER_KIND => config.anthropic_api_key_vault_ref.clone(),
        _ => None,
    }
}

fn provider_enabled(providers: &[RegistryProviderEntry], provider_id: &str) -> bool {
    providers
        .iter()
        .find(|provider| provider.provider_id == provider_id)
        .map(|provider| provider.enabled)
        .unwrap_or(false)
}

fn fallback_cost_rank(cost_tier: &str) -> u8 {
    match cost_tier {
        "low" => 0,
        "standard" => 1,
        "premium" => 2,
        _ => 3,
    }
}

fn fallback_latency_rank(latency_tier: &str) -> u8 {
    match latency_tier {
        "low" => 0,
        "standard" => 1,
        "high" => 2,
        _ => 3,
    }
}

fn parse_root_file_config(document: &toml::Value) -> Result<RootFileConfig> {
    let serialized = toml::to_string(document)
        .context("failed to serialize config document for model parsing")?;
    toml::from_str(&serialized).context("failed to parse model provider config snapshot")
}

fn registry_configured(document: &toml::Value) -> Result<bool> {
    Ok(get_value_at_path(document, "model_provider.providers")
        .with_context(|| "invalid config key path: model_provider.providers")?
        .is_some()
        || get_value_at_path(document, "model_provider.models")
            .with_context(|| "invalid config key path: model_provider.models")?
            .is_some())
}

fn registry_views_from_config(
    config: &FileModelProviderConfig,
) -> (Vec<RegistryProviderEntry>, Vec<RegistryModelEntry>) {
    let providers = config
        .providers
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| RegistryProviderEntry {
                    provider_id: entry.provider_id.clone().unwrap_or_default(),
                    display_name: entry.display_name.clone(),
                    kind: entry.kind.clone().unwrap_or_else(|| {
                        config
                            .kind
                            .clone()
                            .unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned())
                    }),
                    base_url: entry.base_url.clone(),
                    enabled: entry.enabled.unwrap_or(true),
                    auth_profile_id: entry.auth_profile_id.clone(),
                    auth_provider_kind: entry.auth_provider_kind.clone(),
                    api_key_configured: entry
                        .api_key
                        .as_deref()
                        .filter(|value| !value.trim().is_empty())
                        .is_some()
                        || entry
                            .api_key_vault_ref
                            .as_deref()
                            .filter(|value| !value.trim().is_empty())
                            .is_some(),
                    source: "registry",
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| legacy_provider_entries(config));
    let models = config
        .models
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| RegistryModelEntry {
                    model_id: entry.model_id.clone().unwrap_or_default(),
                    provider_id: entry.provider_id.clone().unwrap_or_default(),
                    role: entry.role.clone().unwrap_or_else(|| "chat".to_owned()),
                    enabled: entry.enabled.unwrap_or(true),
                    metadata_source: entry
                        .metadata_source
                        .clone()
                        .unwrap_or_else(|| "static".to_owned()),
                    operator_override: entry.operator_override.unwrap_or(false),
                    tool_calls: entry.tool_calls.unwrap_or(false),
                    json_mode: entry.json_mode.unwrap_or(false),
                    vision: entry.vision.unwrap_or(false),
                    audio_transcribe: entry.audio_transcribe.unwrap_or(false),
                    embeddings: entry.embeddings.unwrap_or(false),
                    max_context_tokens: entry.max_context_tokens,
                    cost_tier: entry.cost_tier.clone().unwrap_or_else(|| "standard".to_owned()),
                    latency_tier: entry
                        .latency_tier
                        .clone()
                        .unwrap_or_else(|| "standard".to_owned()),
                    recommended_use_cases: entry.recommended_use_cases.clone().unwrap_or_default(),
                    known_limitations: entry.known_limitations.clone().unwrap_or_default(),
                    source: "registry",
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| legacy_model_entries(config));
    (providers, models)
}

fn legacy_provider_entries(config: &FileModelProviderConfig) -> Vec<RegistryProviderEntry> {
    let kind = config.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let provider_id = legacy_provider_id(kind.as_str()).to_owned();
    vec![RegistryProviderEntry {
        provider_id,
        display_name: Some(kind.replace('_', " ")),
        kind,
        base_url: config.openai_base_url.clone().or_else(|| config.anthropic_base_url.clone()),
        enabled: true,
        auth_profile_id: config.auth_profile_id.clone(),
        auth_provider_kind: config.auth_provider_kind.clone(),
        api_key_configured: config
            .openai_api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .is_some()
            || config
                .openai_api_key_vault_ref
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .is_some()
            || config
                .anthropic_api_key
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .is_some()
            || config
                .anthropic_api_key_vault_ref
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .is_some(),
        source: "legacy",
    }]
}

fn legacy_model_entries(config: &FileModelProviderConfig) -> Vec<RegistryModelEntry> {
    let kind = config.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let provider_id = legacy_provider_id(kind.as_str()).to_owned();
    let mut models = Vec::new();
    if let Some(model_id) = config
        .openai_model
        .clone()
        .or_else(|| config.anthropic_model.clone())
        .or_else(|| Some("deterministic".to_owned()))
    {
        models.push(legacy_registry_model(model_id, provider_id.clone(), "chat", kind.as_str()));
    }
    if let Some(model_id) = config.openai_embeddings_model.clone() {
        models.push(legacy_registry_model(
            model_id,
            provider_id,
            "embeddings",
            OPENAI_COMPATIBLE_PROVIDER_KIND,
        ));
    }
    models
}

fn legacy_registry_model(
    model_id: String,
    provider_id: String,
    role: &str,
    provider_kind: &str,
) -> RegistryModelEntry {
    let is_chat = role == "chat";
    RegistryModelEntry {
        model_id,
        provider_id,
        role: role.to_owned(),
        enabled: true,
        metadata_source: "legacy_migration".to_owned(),
        operator_override: false,
        tool_calls: is_chat,
        json_mode: is_chat && provider_kind != DETERMINISTIC_PROVIDER_KIND,
        vision: is_chat && provider_kind != DETERMINISTIC_PROVIDER_KIND,
        audio_transcribe: is_chat && provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND,
        embeddings: role == "embeddings",
        max_context_tokens: if provider_kind == DETERMINISTIC_PROVIDER_KIND {
            None
        } else {
            Some(128_000)
        },
        cost_tier: if provider_kind == ANTHROPIC_PROVIDER_KIND {
            "premium".to_owned()
        } else if provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND && role == "embeddings" {
            "low".to_owned()
        } else {
            "standard".to_owned()
        },
        latency_tier: if provider_kind == ANTHROPIC_PROVIDER_KIND {
            "high".to_owned()
        } else {
            "standard".to_owned()
        },
        recommended_use_cases: if role == "embeddings" {
            vec!["memory retrieval".to_owned()]
        } else {
            vec!["general chat".to_owned()]
        },
        known_limitations: Vec::new(),
        source: "legacy",
    }
}

fn legacy_provider_id(provider_kind: &str) -> &'static str {
    match provider_kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => "openai-primary",
        ANTHROPIC_PROVIDER_KIND => "anthropic-primary",
        _ => "deterministic-primary",
    }
}

fn validate_registry_views(
    providers: &[RegistryProviderEntry],
    models: &[RegistryModelEntry],
    default_chat_model_id: Option<&str>,
    default_embeddings_model_id: Option<&str>,
) -> Vec<String> {
    let mut issues = Vec::new();
    if providers.is_empty() {
        issues.push("provider registry does not define any providers".to_owned());
    }
    if models.is_empty() {
        issues.push("provider registry does not define any models".to_owned());
    }

    let mut provider_ids = std::collections::HashSet::new();
    for provider in providers {
        if provider.provider_id.trim().is_empty() {
            issues.push("provider registry entry is missing provider_id".to_owned());
        } else if !provider_ids.insert(provider.provider_id.clone()) {
            issues.push(format!("duplicate provider id '{}'", provider.provider_id));
        }
    }

    let mut model_ids = std::collections::HashSet::new();
    for model in models {
        if model.model_id.trim().is_empty() {
            issues.push("provider registry model is missing model_id".to_owned());
        } else if !model_ids.insert(model.model_id.clone()) {
            issues.push(format!("duplicate model id '{}'", model.model_id));
        }
        if !provider_ids.contains(model.provider_id.as_str()) {
            issues.push(format!(
                "model '{}' references unknown provider '{}'",
                model.model_id, model.provider_id
            ));
        }
    }

    if let Some(model_id) = default_chat_model_id {
        if !models.iter().any(|entry| entry.model_id == model_id && entry.role == "chat") {
            issues.push(format!(
                "default chat model '{}' was not found among configured chat models",
                model_id
            ));
        }
    }
    if let Some(model_id) = default_embeddings_model_id {
        if !models.iter().any(|entry| entry.model_id == model_id && entry.role == "embeddings") {
            issues.push(format!(
                "default embeddings model '{}' was not found among configured embeddings models",
                model_id
            ));
        }
    }

    issues
}

fn default_provider_id<'a>(
    models: &'a [RegistryModelEntry],
    config: &FileModelProviderConfig,
) -> Option<&'a str> {
    let default_chat_model_id = config
        .default_chat_model_id
        .as_deref()
        .or(config.openai_model.as_deref())
        .or(config.anthropic_model.as_deref());
    default_chat_model_id.and_then(|model_id| {
        models
            .iter()
            .find(|entry| entry.model_id == model_id && entry.role == "chat")
            .map(|entry| entry.provider_id.as_str())
    })
}

fn provider_check_ttl_ms(overview: &ModelsOverview, discover: bool) -> i64 {
    let path_ref = Path::new(overview.status.path.as_str());
    let Ok((document, _)) = load_document_from_existing_path(path_ref) else {
        return 0;
    };
    let Ok(root_config) = parse_root_file_config(&document) else {
        return 0;
    };
    let model_provider = root_config.model_provider.unwrap_or_default();
    let raw = if discover {
        model_provider.discovery_ttl_ms.unwrap_or(300_000)
    } else {
        model_provider.health_ttl_ms.unwrap_or(60_000)
    };
    i64::try_from(raw).unwrap_or(i64::MAX)
}

fn provider_check_cache_key(mode: &str, provider: &ProbeableProvider) -> String {
    let mut hasher = DefaultHasher::new();
    mode.hash(&mut hasher);
    provider.provider_id.hash(&mut hasher);
    provider.kind.hash(&mut hasher);
    provider.endpoint_base_url.hash(&mut hasher);
    provider.auth_profile_id.hash(&mut hasher);
    provider.auth_provider_kind.hash(&mut hasher);
    provider.vault_ref.hash(&mut hasher);
    provider.configured_model_ids.hash(&mut hasher);
    format!("{mode}:{:016x}", hasher.finish())
}

fn load_provider_checks_cache() -> Result<ProviderChecksCacheDocument> {
    let cache_path = provider_checks_cache_path()?;
    if !cache_path.exists() {
        return Ok(ProviderChecksCacheDocument::default());
    }
    let raw = fs::read_to_string(&cache_path).with_context(|| {
        format!("failed to read provider checks cache {}", cache_path.display())
    })?;
    serde_json::from_str(raw.as_str())
        .with_context(|| format!("failed to parse provider checks cache {}", cache_path.display()))
}

fn persist_provider_checks_cache(cache: &ProviderChecksCacheDocument) -> Result<()> {
    let cache_path = provider_checks_cache_path()?;
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let body =
        serde_json::to_string_pretty(cache).context("failed to serialize provider checks cache")?;
    fs::write(&cache_path, body)
        .with_context(|| format!("failed to write provider checks cache {}", cache_path.display()))
}

fn provider_checks_cache_path() -> Result<PathBuf> {
    if let Some(context) = app::current_root_context() {
        return Ok(context.state_root().join(PROVIDER_CHECKS_CACHE_PATH));
    }
    Ok(app::resolve_cli_state_root(None)?.join(PROVIDER_CHECKS_CACHE_PATH))
}

fn read_cached_provider_check(
    cache: &ProviderChecksCacheDocument,
    cache_key: &str,
    now_unix_ms: i64,
) -> Option<ProviderConnectionCheckPayload> {
    let cached = cache.entries.get(cache_key)?.clone();
    if cached.expires_at_unix_ms <= now_unix_ms {
        return None;
    }
    Some(ProviderConnectionCheckPayload { cache_status: "hit".to_owned(), ..cached.payload })
}

fn write_cached_provider_check(
    cache: &mut ProviderChecksCacheDocument,
    cache_key: String,
    payload: ProviderConnectionCheckPayload,
    ttl_ms: i64,
    now_unix_ms: i64,
) {
    if ttl_ms <= 0 {
        return;
    }
    let expires_at_unix_ms = now_unix_ms.saturating_add(ttl_ms);
    cache.entries.insert(
        cache_key,
        CachedProviderCheckEntry {
            expires_at_unix_ms,
            payload: ProviderConnectionCheckPayload { cache_status: "fresh".to_owned(), ..payload },
        },
    );
}

fn probe_provider(
    target: &ProbeableProvider,
    timeout_ms: u64,
    now_unix_ms: i64,
    discover: bool,
    auth_registry: &mut Option<AuthProfileRegistry>,
    vault: &mut Option<palyra_vault::Vault>,
) -> ProviderConnectionCheckPayload {
    let mut payload = ProviderConnectionCheckPayload {
        provider_id: target.provider_id.clone(),
        kind: target.kind.clone(),
        enabled: target.enabled,
        endpoint_base_url: target.endpoint_base_url.clone(),
        credential_source: "none".to_owned(),
        state: "unknown".to_owned(),
        message: "provider has not been checked yet".to_owned(),
        checked_at_unix_ms: now_unix_ms,
        cache_status: "miss".to_owned(),
        live_discovery_verified: false,
        discovery_source: "live".to_owned(),
        discovered_model_ids: Vec::new(),
        configured_model_ids: target.configured_model_ids.clone(),
        latency_ms: None,
    };
    if !target.enabled {
        payload.state = "disabled".to_owned();
        payload.message = "provider is disabled in the registry".to_owned();
        return payload;
    }
    if target.kind == DETERMINISTIC_PROVIDER_KIND {
        payload.state = "unsupported".to_owned();
        payload.message =
            "deterministic provider does not expose a remote models endpoint".to_owned();
        payload.discovery_source = "registry".to_owned();
        return payload;
    }
    let Some(base_url) = target.endpoint_base_url.as_deref() else {
        payload.state = "endpoint_missing".to_owned();
        payload.message = "provider base_url is not configured".to_owned();
        return payload;
    };
    let credential = match resolve_provider_credential(target, auth_registry, vault) {
        Ok(Some(credential)) => credential,
        Ok(None) => {
            payload.state = "missing_auth".to_owned();
            payload.message = "provider does not have a usable API credential".to_owned();
            return payload;
        }
        Err(error) => {
            payload.state = "missing_auth".to_owned();
            payload.message = sanitize_diagnostic_error(error.to_string().as_str());
            return payload;
        }
    };
    payload.credential_source = match &credential {
        ResolvedCredential::ApiKey { source, .. } | ResolvedCredential::Bearer { source, .. } => {
            source.clone()
        }
    };

    let endpoint = match provider_models_endpoint(base_url) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            payload.state = "endpoint_failed".to_owned();
            payload.message = sanitize_diagnostic_error(error.to_string().as_str());
            return payload;
        }
    };

    let client = match Client::builder().timeout(Duration::from_millis(timeout_ms)).build() {
        Ok(client) => client,
        Err(error) => {
            payload.state = "endpoint_failed".to_owned();
            payload.message = sanitize_diagnostic_error(error.to_string().as_str());
            return payload;
        }
    };
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
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
    match client.get(endpoint).headers(headers).send() {
        Ok(response) => {
            payload.latency_ms =
                Some(started_at.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
            let status = response.status();
            let body = response.text().unwrap_or_default();
            if status.is_success() {
                match parse_discovered_model_ids(body.as_str()) {
                    Ok(discovered) => {
                        payload.live_discovery_verified = true;
                        payload.discovery_source = "live".to_owned();
                        payload.discovered_model_ids = discovered;
                        payload.state = if payload.discovered_model_ids.is_empty() {
                            "partial".to_owned()
                        } else {
                            "ok".to_owned()
                        };
                        payload.message = if payload.discovered_model_ids.is_empty() {
                            "provider connection succeeded but model discovery returned no ids"
                                .to_owned()
                        } else {
                            format!(
                                "provider connection succeeded and discovered {} model(s)",
                                payload.discovered_model_ids.len()
                            )
                        };
                    }
                    Err(_) => {
                        payload.discovery_source = "registry_fallback".to_owned();
                        payload.state = if discover {
                            "discovery_parse_failed".to_owned()
                        } else {
                            "verification_incomplete".to_owned()
                        };
                        payload.message = if discover {
                            "provider connection succeeded but discovery response could not be parsed; using configured model registry for reference only"
                                .to_owned()
                        } else {
                            "provider connection succeeded but live model discovery could not be parsed; using configured model registry for reference only and not verifying model usability"
                                .to_owned()
                        };
                    }
                }
            } else if status.as_u16() == 404 && !target.configured_model_ids.is_empty() {
                payload.discovery_source = "registry_fallback".to_owned();
                payload.state =
                    if discover { "discovery_unsupported".to_owned() } else { "ok".to_owned() };
                payload.message = if discover {
                    "provider returned HTTP 404 for model discovery; using configured model registry for reference only"
                        .to_owned()
                } else {
                    "provider connection succeeded, but live model discovery is unsupported (HTTP 404); showing configured model registry for reference only. This confirms endpoint and credentials, not model usability."
                        .to_owned()
                };
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
            payload.message = sanitize_diagnostic_error(error.to_string().as_str());
        }
    }

    payload
}

fn resolve_provider_credential(
    target: &ProbeableProvider,
    auth_registry: &mut Option<AuthProfileRegistry>,
    vault: &mut Option<palyra_vault::Vault>,
) -> Result<Option<ResolvedCredential>> {
    if let Some(profile_id) = target.auth_profile_id.as_deref() {
        let registry = auth_registry
            .get_or_insert(AuthProfileRegistry::open(default_identity_store_root()?.as_path())?);
        let Some(profile) = registry.get_profile(profile_id)? else {
            anyhow::bail!("auth profile not found: {profile_id}");
        };
        let expected_provider = expected_auth_provider_for_probe_target(target);
        if let Some(expected_provider) = expected_provider {
            let matches_expected =
                if expected_provider == AuthProviderKind::Custom {
                    matches!(profile.provider.kind, AuthProviderKind::Custom)
                        && profile.provider.custom_name.as_deref().is_some_and(|name| {
                            name.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND)
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
        let vault_instance = vault.get_or_insert(open_cli_vault()?);
        return match profile.credential {
            AuthCredential::ApiKey { api_key_vault_ref } => {
                let token = load_vault_secret_utf8(vault_instance, api_key_vault_ref.as_str())?;
                Ok(Some(ResolvedCredential::ApiKey { token, source: "auth_profile".to_owned() }))
            }
            AuthCredential::Oauth { access_token_vault_ref, .. } => {
                let token =
                    load_vault_secret_utf8(vault_instance, access_token_vault_ref.as_str())?;
                Ok(Some(ResolvedCredential::Bearer { token, source: "auth_profile".to_owned() }))
            }
        };
    }

    if let Some(api_key) = target.inline_api_key.as_deref().and_then(normalize_optional_text) {
        return Ok(Some(ResolvedCredential::ApiKey {
            token: api_key.to_owned(),
            source: "config_inline".to_owned(),
        }));
    }
    if let Some(vault_ref) = target.vault_ref.as_deref() {
        let vault_instance = vault.get_or_insert(open_cli_vault()?);
        let token = load_vault_secret_utf8(vault_instance, vault_ref)?;
        return Ok(Some(ResolvedCredential::ApiKey {
            token,
            source: "config_vault_ref".to_owned(),
        }));
    }
    Ok(None)
}

fn target_uses_minimax_auth(target: &ProbeableProvider) -> bool {
    target
        .auth_provider_kind
        .as_deref()
        .is_some_and(|kind| kind.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND))
}

fn expected_auth_provider_for_probe_target(target: &ProbeableProvider) -> Option<AuthProviderKind> {
    if target_uses_minimax_auth(target) {
        return Some(AuthProviderKind::Custom);
    }
    match target.kind.as_str() {
        OPENAI_COMPATIBLE_PROVIDER_KIND => Some(AuthProviderKind::Openai),
        ANTHROPIC_PROVIDER_KIND => Some(AuthProviderKind::Anthropic),
        _ => None,
    }
}

fn load_vault_secret_utf8(vault: &palyra_vault::Vault, vault_ref: &str) -> Result<String> {
    let parsed = VaultRef::parse(vault_ref)?;
    let bytes =
        vault.get_secret(&parsed.scope, parsed.key.as_str()).map_err(anyhow::Error::from)?;
    String::from_utf8(bytes).context("vault secret must contain valid UTF-8")
}

fn provider_models_endpoint(base_url: &str) -> Result<reqwest::Url> {
    let trimmed = base_url.trim().trim_end_matches('/');
    let raw = if trimmed.ends_with("/v1") {
        format!("{trimmed}/models")
    } else {
        format!("{trimmed}/v1/models")
    };
    reqwest::Url::parse(raw.as_str())
        .with_context(|| format!("invalid provider base_url: {base_url}"))
}

fn parse_discovered_model_ids(body: &str) -> Result<Vec<String>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("provider returned invalid JSON for model discovery")?;
    let discovered = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.get("id").and_then(serde_json::Value::as_str))
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(discovered)
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

fn unix_timestamp_ms() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock drifted before unix epoch")?
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX))
}

fn get_string_value_at_path(document: &toml::Value, key: &str) -> Result<Option<String>> {
    Ok(get_value_at_path(document, key)
        .with_context(|| format!("invalid config key path: {key}"))?
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned))
}
