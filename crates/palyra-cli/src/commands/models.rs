//! `palyra models` command: provider/model registry status, selection, and probes.
//!
//! Reads the daemon config in either the provider-registry or legacy
//! single-provider layout, mutates default model selections with rotated
//! backups, and probes provider endpoints behind a private-network guard with
//! a TTL cache for connection and discovery checks.

use crate::*;
use palyra_auth::{AuthCredential, AuthProfileRegistry, AuthProviderKind};
use palyra_common::daemon_config_schema::FileModelProviderConfig;
use palyra_common::redaction::redact_auth_error;
use palyra_model_providers::{
    is_openai_chatgpt_oauth_client_id, legacy_provider_identity_for_file_config_kind,
    model_id_supports_reasoning_effort, normalized_provider_filter_alias,
    parse_discovered_model_ids,
    provider_models_endpoint_for_probe as build_provider_models_endpoint_for_probe,
    ProviderModelsEndpoint, ANTHROPIC_API_VERSION, OPENAI_API_DEFAULT_CHAT_MODEL_ID,
    OPENAI_CODEX_BACKEND_BASE_URL,
};
use palyra_vault::{Vault, VaultConfig as VaultConfigOptions, VaultRef};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION};
use std::{
    collections::BTreeMap,
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    net::{IpAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const OPENAI_COMPATIBLE_PROVIDER_KIND: &str = "openai_compatible";
const ANTHROPIC_PROVIDER_KIND: &str = "anthropic";
const DETERMINISTIC_PROVIDER_KIND: &str = "deterministic";
const OPENAI_DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const ANTHROPIC_DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const MINIMAX_AUTH_PROVIDER_KIND: &str = "minimax";
const DESKTOP_CONTROL_CENTER_DIR: &str = "desktop-control-center";
const DESKTOP_RUNTIME_DIR: &str = "runtime";
const PROVIDER_CHECKS_CACHE_PATH: &str = "models/provider_checks.json";

/// Snapshot of the effective model-provider configuration for `models status`.
#[derive(Debug, Serialize)]
pub(crate) struct ModelsStatusPayload {
    pub(crate) path: String,
    pub(crate) provider_id: String,
    pub(crate) provider_display_name: String,
    pub(crate) protocol_compatibility: String,
    pub(crate) provider_kind: String,
    pub(crate) auth_provider_kind: Option<String>,
    pub(crate) endpoint_base_url: Option<String>,
    pub(crate) openai_base_url: Option<String>,
    pub(crate) text_model: Option<String>,
    pub(crate) embeddings_model: Option<String>,
    pub(crate) embeddings_dims: Option<u32>,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) service_tier: Option<String>,
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

/// Curated or operator-configured model entry surfaced by `models list`.
#[derive(Debug, Serialize)]
pub(crate) struct ModelCatalogEntry<'a> {
    pub(crate) target: &'a str,
    pub(crate) id: String,
    pub(crate) configured: bool,
    pub(crate) preferred: bool,
    pub(crate) source: &'a str,
}

/// Provider row resolved from the registry or the legacy single-provider config.
#[derive(Debug, Serialize)]
pub(crate) struct RegistryProviderEntry {
    pub(crate) provider_id: String,
    pub(crate) display_name: Option<String>,
    pub(crate) kind: String,
    pub(crate) protocol_compatibility: String,
    pub(crate) base_url: Option<String>,
    pub(crate) enabled: bool,
    pub(crate) auth_profile_id: Option<String>,
    pub(crate) auth_provider_kind: Option<String>,
    pub(crate) api_key_configured: bool,
    pub(crate) source: &'static str,
}

/// Model row with capability metadata, resolved from the registry or legacy config.
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
    pub(crate) reasoning: bool,
    pub(crate) reasoning_efforts: Vec<String>,
    pub(crate) service_tier: bool,
    pub(crate) service_tiers: Vec<String>,
    pub(crate) max_context_tokens: Option<u32>,
    pub(crate) cost_tier: String,
    pub(crate) latency_tier: String,
    pub(crate) recommended_use_cases: Vec<String>,
    pub(crate) known_limitations: Vec<String>,
    pub(crate) source: &'static str,
}

/// Aggregate payload for `models list`: status plus catalog, provider, and model views.
#[derive(Debug, Serialize)]
pub(crate) struct ModelsListPayload {
    pub(crate) status: ModelsStatusPayload,
    pub(crate) models: Vec<ModelCatalogEntry<'static>>,
    pub(crate) providers: Vec<RegistryProviderEntry>,
    pub(crate) registry_models: Vec<RegistryModelEntry>,
}

/// Result payload for `models set` and `models set-embeddings` mutations.
#[derive(Debug, Serialize)]
pub(crate) struct ModelsMutationPayload {
    pub(crate) path: String,
    pub(crate) provider_kind: String,
    pub(crate) target: &'static str,
    pub(crate) model: String,
    pub(crate) embeddings_dims: Option<u32>,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) service_tier: Option<String>,
    pub(crate) backups: usize,
    pub(crate) runtime_reload: crate::commands::runtime_reload::RuntimeConfigReloadOutcome,
}

struct ModelsDefaultMutationRequest {
    path: Option<String>,
    backups: usize,
    target: &'static str,
    model: String,
    dims: Option<u32>,
    reasoning: Option<String>,
    fast: bool,
    no_fast: bool,
    service_tier: Option<String>,
    allow_custom: bool,
}

/// Outcome of one provider connection/discovery probe; also the cached entry shape.
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
    #[serde(default)]
    pub(crate) live_discovery_verified: bool,
    pub(crate) discovery_source: String,
    pub(crate) discovered_model_ids: Vec<String>,
    pub(crate) configured_model_ids: Vec<String>,
    pub(crate) latency_ms: Option<u64>,
}

/// Aggregate payload for `models test-connection` and `models discover`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelsConnectionPayload {
    pub(crate) path: String,
    pub(crate) mode: &'static str,
    pub(crate) timeout_ms: u64,
    pub(crate) provider_filter: Option<String>,
    pub(crate) provider_count: usize,
    pub(crate) providers: Vec<ProviderConnectionCheckPayload>,
}

#[derive(Debug, Deserialize)]
struct ConsoleModelsConnectionEnvelope {
    timeout_ms: u64,
    provider_filter: Option<String>,
    provider_count: usize,
    providers: Vec<ConsoleProviderConnectionPayload>,
}

#[derive(Debug, Deserialize)]
struct ConsoleProviderConnectionPayload {
    provider_id: String,
    kind: String,
    enabled: bool,
    endpoint_base_url: Option<String>,
    credential_source: String,
    state: String,
    message: String,
    checked_at_unix_ms: i64,
    cache_status: String,
    discovery_source: String,
    discovered_model_ids: Vec<String>,
    configured_model_ids: Vec<String>,
    latency_ms: Option<u64>,
}

/// One routing candidate (primary or fallback) in the `models explain` output.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelsExplainCandidatePayload {
    pub(crate) order: usize,
    pub(crate) provider_id: String,
    pub(crate) provider_display_name: String,
    pub(crate) protocol_compatibility: String,
    pub(crate) provider_kind: String,
    pub(crate) auth_provider_kind: Option<String>,
    pub(crate) model_id: String,
    pub(crate) role: String,
    pub(crate) selected: bool,
    pub(crate) reason: String,
    pub(crate) cost_tier: String,
    pub(crate) latency_tier: String,
    pub(crate) tool_calls: bool,
    pub(crate) json_mode: bool,
    pub(crate) vision: bool,
    pub(crate) reasoning: bool,
    pub(crate) reasoning_efforts: Vec<String>,
    pub(crate) service_tier: bool,
    pub(crate) service_tiers: Vec<String>,
}

/// Aggregate payload for `models explain`: resolved routing plus its rationale.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelsExplainPayload {
    pub(crate) path: String,
    pub(crate) requested_model_id: Option<String>,
    pub(crate) resolved_model_id: Option<String>,
    pub(crate) json_mode: bool,
    pub(crate) vision: bool,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) service_tier: Option<String>,
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

/// Provider candidate flattened from config for connection/discovery probing.
#[derive(Debug, Clone)]
struct ProbeableProvider {
    provider_id: String,
    kind: String,
    enabled: bool,
    endpoint_base_url: Option<String>,
    allow_private_base_url: bool,
    auth_profile_id: Option<String>,
    auth_state_roots: Vec<PathBuf>,
    auth_vault_candidates: Vec<ProbeAuthVaultCandidate>,
    auth_provider_kind: Option<String>,
    inline_api_key: Option<String>,
    vault_ref: Option<String>,
    configured_model_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProbeAuthVaultCandidate {
    vault_root: PathBuf,
    identity_store_root: PathBuf,
}

/// API credential resolved from an auth profile, inline config, or vault reference.
#[derive(Debug, Clone)]
enum ResolvedCredential {
    ApiKey { token: String, source: String },
    Bearer { token: String, source: String, oauth_kind: Option<ResolvedOauthProfileKind> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedOauthProfileKind {
    OpenAiChatGptLogin,
}

/// Dispatches `palyra models` subcommands.
///
/// # Errors
/// Returns an error when the config cannot be loaded or parsed, a mutation is
/// rejected by validation, provider probing cannot start, or output encoding
/// fails.
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
                    "models.list provider_id={} provider_display_name={} provider_kind={} protocol_compatibility={} auth_provider_kind={} text_model={} embeddings_model={} reasoning_effort={} service_tier={} auth_profile_id={} registry_providers={} registry_models={} registry_valid={}",
                    payload.status.provider_id,
                    payload.status.provider_display_name,
                    payload.status.provider_kind,
                    payload.status.protocol_compatibility,
                    payload.status.auth_provider_kind.as_deref().unwrap_or("none"),
                    payload.status.text_model.as_deref().unwrap_or("none"),
                    payload.status.embeddings_model.as_deref().unwrap_or("none"),
                    payload.status.reasoning_effort.as_deref().unwrap_or("none"),
                    payload.status.service_tier.as_deref().unwrap_or("none"),
                    payload.status.auth_profile_id.as_deref().unwrap_or("none"),
                    payload.providers.len(),
                    payload.registry_models.len(),
                    payload.status.registry_valid
                );
                for entry in payload.providers {
                    println!(
                        "models.provider id={} display_name={} kind={} protocol_compatibility={} auth_provider_kind={} enabled={} auth_profile_id={} api_key_configured={} source={}",
                        entry.provider_id,
                        entry.display_name.as_deref().unwrap_or("none"),
                        entry.kind,
                        entry.protocol_compatibility,
                        entry.auth_provider_kind.as_deref().unwrap_or("none"),
                        entry.enabled,
                        entry.auth_profile_id.as_deref().unwrap_or("none"),
                        entry.api_key_configured,
                        entry.source
                    );
                }
                for entry in payload.registry_models {
                    println!(
                        "models.registry_model id={} provider_id={} role={} enabled={} json_mode={} vision={} embeddings={} reasoning={} reasoning_efforts={} service_tier={} service_tiers={} source={}",
                        entry.model_id,
                        entry.provider_id,
                        entry.role,
                        entry.enabled,
                        entry.json_mode,
                        entry.vision,
                        entry.embeddings,
                        entry.reasoning,
                        entry.reasoning_efforts.join(","),
                        entry.service_tier,
                        entry.service_tiers.join(","),
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
        ModelsCommand::Set {
            model,
            path,
            reasoning,
            fast,
            no_fast,
            service_tier,
            backups,
            allow_custom,
            json,
        } => {
            let payload = mutate_model_defaults(ModelsDefaultMutationRequest {
                path,
                backups,
                target: "text",
                model,
                dims: None,
                reasoning,
                fast,
                no_fast,
                service_tier,
                allow_custom,
            })?;
            emit_models_mutation(&payload, output::preferred_json(json))
        }
        ModelsCommand::SetEmbeddings { model, dims, path, backups, allow_custom, json } => {
            let payload = mutate_model_defaults(ModelsDefaultMutationRequest {
                path,
                backups,
                target: "embeddings",
                model,
                dims,
                reasoning: None,
                fast: false,
                no_fast: false,
                service_tier: None,
                allow_custom,
            })?;
            emit_models_mutation(&payload, output::preferred_json(json))
        }
    }
}

fn emit_models_status(payload: &ModelsStatusPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(payload, "failed to encode models status as JSON")?;
    } else {
        println!(
            "models.status path={} provider_id={} provider_display_name={} provider_kind={} protocol_compatibility={} auth_provider_kind={} text_model={} embeddings_model={} reasoning_effort={} service_tier={} auth_profile_id={} api_key_configured={} migrated={}",
            payload.path,
            payload.provider_id,
            payload.provider_display_name,
            payload.provider_kind,
            payload.protocol_compatibility,
            payload.auth_provider_kind.as_deref().unwrap_or("none"),
            payload.text_model.as_deref().unwrap_or("none"),
            payload.embeddings_model.as_deref().unwrap_or("none"),
            payload.reasoning_effort.as_deref().unwrap_or("none"),
            payload.service_tier.as_deref().unwrap_or("none"),
            payload.auth_profile_id.as_deref().unwrap_or("none"),
            payload.api_key_configured,
            payload.migrated
        );
        println!(
            "models.status.provider base_url={} openai_base_url={} embeddings_dims={} default_chat_model={} registry_default_embeddings_model={} registry_providers={} registry_models={} failover_enabled={} response_cache_enabled={} registry_valid={}",
            payload.endpoint_base_url.as_deref().unwrap_or("none"),
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
            "models.set path={} provider_kind={} target={} model={} embeddings_dims={} reasoning_effort={} service_tier={} backups={}",
            payload.path,
            payload.provider_kind,
            payload.target,
            payload.model,
            payload
                .embeddings_dims
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            payload.reasoning_effort.as_deref().unwrap_or("none"),
            payload.service_tier.as_deref().unwrap_or("none"),
            payload.backups
        );
        println!(
            "{}",
            crate::commands::runtime_reload::reload_text_line(
                "models.set",
                &payload.runtime_reload
            )
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
            "models.explain path={} requested_model={} resolved_model={} json_mode={} vision={} reasoning_effort={} service_tier={} failover_enabled={} response_cache_enabled={} candidates={}",
            payload.path,
            payload.requested_model_id.as_deref().unwrap_or("default"),
            payload.resolved_model_id.as_deref().unwrap_or("none"),
            payload.json_mode,
            payload.vision,
            payload.reasoning_effort.as_deref().unwrap_or("none"),
            payload.service_tier.as_deref().unwrap_or("none"),
            payload.failover_enabled,
            payload.response_cache_enabled,
            payload.candidates.len()
        );
        for line in &payload.explanation {
            println!("models.explain.detail {line}");
        }
        for candidate in &payload.candidates {
            println!(
                "models.explain.candidate order={} provider_id={} provider_display_name={} provider_kind={} protocol_compatibility={} auth_provider_kind={} model_id={} selected={} reasoning={} reasoning_efforts={} service_tier={} service_tiers={} reason={}",
                candidate.order,
                candidate.provider_id,
                candidate.provider_display_name,
                candidate.provider_kind,
                candidate.protocol_compatibility,
                candidate.auth_provider_kind.as_deref().unwrap_or("none"),
                candidate.model_id,
                candidate.selected,
                candidate.reasoning,
                candidate.reasoning_efforts.join(","),
                candidate.service_tier,
                candidate.service_tiers.join(","),
                candidate.reason
            );
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

/// Builds the `models list` payload from configured defaults plus the provider registry.
///
/// # Errors
/// Returns an error when the config cannot be loaded or parsed.
pub(crate) fn build_models_list(path: Option<String>) -> Result<ModelsListPayload> {
    let overview = load_models_overview(path)?;
    let status = overview.status;
    let mut models = Vec::new();
    if let Some(configured) = status.default_chat_model_id.as_deref() {
        append_ad_hoc_entry(&mut models, "text", configured);
    }
    if let Some(configured) = status.text_model.as_deref() {
        append_ad_hoc_entry(&mut models, "text", configured);
    }
    if let Some(configured) = status.default_embeddings_model_id.as_deref() {
        append_ad_hoc_entry(&mut models, "embeddings", configured);
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
        preferred: true,
        source: "configured",
    });
}

/// Sets the default text or embeddings model in the config, keeping rotated backups.
///
/// Model IDs are intentionally not checked against a local allowlist. Providers
/// are the source of truth; Palyra only rejects empty or unsafe identifiers here.
///
/// # Errors
/// Returns an error when the config cannot be parsed, the model id fails
/// validation, the mutated document no longer matches the daemon schema, or
/// the file cannot be persisted.
fn mutate_model_defaults(request: ModelsDefaultMutationRequest) -> Result<ModelsMutationPayload> {
    let ModelsDefaultMutationRequest {
        path,
        backups,
        target,
        model,
        dims,
        reasoning,
        fast,
        no_fast,
        service_tier,
        allow_custom: _allow_custom,
    } = request;
    let path = resolve_config_path(path, false)?;
    let path_ref = Path::new(&path);
    let (mut document, _) = load_document_for_mutation(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;
    let model = validate_model_id_for_mutation(target, model.as_str())?;
    let has_registry = registry_configured(&document)?;
    if !has_registry {
        let provider_kind = legacy_provider_kind_for_mutation(&document)?;
        if get_string_value_at_path(&document, "model_provider.kind")?.is_none() {
            set_value_at_path(
                &mut document,
                "model_provider.kind",
                toml::Value::String(provider_kind.clone()),
            )
            .context("invalid config key path: model_provider.kind")?;
        }
        if provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND {
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
    }

    match target {
        "text" => {
            let reasoning_effort = normalize_reasoning_effort_for_models_set(reasoning.as_deref())?;
            let service_tier =
                normalize_service_tier_for_models_set(fast, no_fast, service_tier.as_deref())?;
            let legacy_provider_kind = if !has_registry {
                Some(legacy_provider_kind_for_mutation(&document)?)
            } else {
                None
            };
            let key = if let Some(provider_kind) = legacy_provider_kind.as_deref() {
                legacy_text_model_key(provider_kind)?
            } else {
                "model_provider.default_chat_model_id"
            };
            set_value_at_path(&mut document, key, toml::Value::String(model.clone()))
                .with_context(|| format!("invalid config key path: {key}"))?;
            if let Some(provider_kind) = legacy_provider_kind.as_deref() {
                clear_conflicting_legacy_text_model(&mut document, provider_kind)?;
            }
            if let Some(reasoning_effort) = reasoning_effort.as_deref() {
                set_value_at_path(
                    &mut document,
                    "model_provider.reasoning_effort",
                    toml::Value::String(reasoning_effort.to_owned()),
                )
                .context("invalid config key path: model_provider.reasoning_effort")?;
            }
            ensure_document_default_model_supports_service_tier(
                path_ref,
                &document,
                service_tier.as_deref(),
            )?;
            if let Some(service_tier) = service_tier.as_deref() {
                set_value_at_path(
                    &mut document,
                    "model_provider.service_tier",
                    toml::Value::String(service_tier.to_owned()),
                )
                .context("invalid config key path: model_provider.service_tier")?;
            }
        }
        "embeddings" => {
            let key = if !has_registry {
                let provider_kind = legacy_provider_kind_for_mutation(&document)?;
                legacy_embeddings_model_key(provider_kind.as_str())?
            } else {
                "model_provider.default_embeddings_model_id"
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
    let runtime_reload = crate::commands::runtime_reload::try_apply_active_config_reload_blocking(
        Some(path.clone()),
    );
    Ok(ModelsMutationPayload {
        path,
        provider_kind: get_string_value_at_path(&document, "model_provider.kind")?
            .unwrap_or_else(|| OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
        target,
        model,
        embeddings_dims: dims,
        reasoning_effort: get_string_value_at_path(&document, "model_provider.reasoning_effort")?,
        service_tier: get_string_value_at_path(&document, "model_provider.service_tier")?,
        backups,
        runtime_reload,
    })
}

fn normalize_reasoning_effort_for_models_set(raw: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let normalized = match raw.to_ascii_lowercase().replace(['-', '_'], "").as_str() {
        "none" | "off" | "disabled" | "false" => "none",
        "minimal" | "min" => "minimal",
        "low" => "low",
        "medium" | "med" => "medium",
        "high" => "high",
        "xhigh" | "extra" | "extrahigh" => "xhigh",
        _ => {
            anyhow::bail!(
                "unsupported --reasoning value '{raw}'; expected one of none, minimal, low, medium, high, xhigh"
            )
        }
    };
    Ok(Some(normalized.to_owned()))
}

fn normalize_service_tier_for_models_set(
    fast: bool,
    no_fast: bool,
    raw: Option<&str>,
) -> Result<Option<String>> {
    match (fast, no_fast, raw.map(str::trim).filter(|value| !value.is_empty())) {
        (true, false, None) => Ok(Some("priority".to_owned())),
        (false, true, None) => Ok(Some("default".to_owned())),
        (false, false, Some(raw)) => normalize_service_tier_value(raw).map(Some),
        (false, false, None) => Ok(None),
        _ => anyhow::bail!("select at most one of --fast, --no-fast, or --service-tier"),
    }
}

fn normalize_service_tier_value(raw: &str) -> Result<String> {
    let normalized = match raw.to_ascii_lowercase().replace(['-', '_'], "").as_str() {
        "auto" => "auto",
        "default" | "standard" | "normal" | "off" | "false" | "nofast" => "default",
        "priority" | "fast" | "on" | "true" => "priority",
        "flex" | "lowcost" | "cheap" => "flex",
        _ => {
            anyhow::bail!(
                "unsupported service tier '{raw}'; expected one of auto, default, priority, flex"
            )
        }
    };
    Ok(normalized.to_owned())
}

fn validate_model_id_for_mutation(target: &'static str, model: &str) -> Result<String> {
    let normalized = normalize_optional_text(model)
        .ok_or_else(|| anyhow::anyhow!("invalid {target} model id: model id must not be empty"))?;
    if normalized.chars().any(char::is_control) {
        anyhow::bail!("invalid {target} model id: model id must not contain control characters");
    }
    Ok(normalized.to_owned())
}

fn legacy_provider_kind_for_mutation(document: &toml::Value) -> Result<String> {
    Ok(get_string_value_at_path(document, "model_provider.kind")?
        .as_deref()
        .map(normalize_legacy_provider_kind)
        .transpose()?
        .unwrap_or_else(|| OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()))
}

fn normalize_legacy_provider_kind(raw: &str) -> Result<String> {
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        OPENAI_COMPATIBLE_PROVIDER_KIND | "openai" => {
            Ok(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned())
        }
        ANTHROPIC_PROVIDER_KIND => Ok(ANTHROPIC_PROVIDER_KIND.to_owned()),
        DETERMINISTIC_PROVIDER_KIND => Ok(DETERMINISTIC_PROVIDER_KIND.to_owned()),
        _ => anyhow::bail!(
            "model_provider.kind must be one of deterministic, openai_compatible, or anthropic"
        ),
    }
}

fn legacy_text_model_key(provider_kind: &str) -> Result<&'static str> {
    match provider_kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND | DETERMINISTIC_PROVIDER_KIND => {
            Ok("model_provider.openai_model")
        }
        ANTHROPIC_PROVIDER_KIND => Ok("model_provider.anthropic_model"),
        _ => anyhow::bail!(
            "model_provider.kind must be one of deterministic, openai_compatible, or anthropic"
        ),
    }
}

fn clear_conflicting_legacy_text_model(
    document: &mut toml::Value,
    provider_kind: &str,
) -> Result<()> {
    match provider_kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND | DETERMINISTIC_PROVIDER_KIND => {
            unset_value_at_path(document, "model_provider.anthropic_model")?;
        }
        ANTHROPIC_PROVIDER_KIND => {
            unset_value_at_path(document, "model_provider.openai_model")?;
        }
        _ => {}
    }
    Ok(())
}

fn legacy_embeddings_model_key(provider_kind: &str) -> Result<&'static str> {
    match provider_kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND | DETERMINISTIC_PROVIDER_KIND => {
            Ok("model_provider.openai_embeddings_model")
        }
        ANTHROPIC_PROVIDER_KIND => anyhow::bail!(
            "models set-embeddings requires an OpenAI-compatible provider or a provider registry entry with an embeddings model; legacy Anthropic-compatible providers such as MiniMax cannot use OpenAI embeddings directly. Configure model_provider.providers/model_provider.models with an embeddings-capable OpenAI-compatible provider and set model_provider.default_embeddings_model_id, or leave memory in hash fallback."
        ),
        _ => anyhow::bail!(
            "model_provider.kind must be one of deterministic, openai_compatible, or anthropic"
        ),
    }
}

/// Loads the `models status` payload for the given (or default) config path.
///
/// # Errors
/// Returns an error when the config cannot be located, read, or parsed.
pub(crate) fn load_models_status(path: Option<String>) -> Result<ModelsStatusPayload> {
    Ok(load_models_overview(path)?.status)
}

pub(crate) fn ensure_default_model_supports_service_tier(service_tier: Option<&str>) -> Result<()> {
    if service_tier_is_default_or_unset(service_tier) {
        return Ok(());
    }
    ensure_overview_default_model_supports_service_tier(&load_models_overview(None)?, service_tier)
}

fn run_provider_checks(
    path: Option<String>,
    provider_filter: Option<String>,
    timeout_ms: u64,
    refresh: bool,
    discover: bool,
) -> Result<ModelsConnectionPayload> {
    let mut force_local_refresh = refresh;
    if path.is_none() {
        if let Ok(payload) =
            run_provider_checks_via_console(provider_filter.clone(), timeout_ms, discover)
        {
            if !console_missing_auth_needs_local_probe(payload.providers.as_slice())? {
                return Ok(payload);
            }
            force_local_refresh = true;
        }
    }
    run_provider_checks_local(path, provider_filter, timeout_ms, force_local_refresh, discover)
}

fn run_provider_checks_via_console(
    provider_filter: Option<String>,
    timeout_ms: u64,
    discover: bool,
) -> Result<ModelsConnectionPayload> {
    let overview = load_models_overview(None)?;
    let runtime = build_runtime()?;
    let request = json!({
        "provider_id": provider_filter,
        "timeout_ms": timeout_ms,
    });
    let envelope = runtime.block_on(async {
        let context = client::control_plane::connect_admin_console_with_request_timeout(
            app::ConnectionOverrides::default(),
            Some(Duration::from_millis(timeout_ms.saturating_add(5_000))),
        )
        .await?;
        let payload = if discover {
            context.client.discover_model_provider_models(&request).await?
        } else {
            context.client.test_model_provider_connection(&request).await?
        };
        serde_json::from_value::<ConsoleModelsConnectionEnvelope>(payload)
            .context("failed to decode console model-provider probe response")
    })?;
    Ok(console_probe_envelope_to_models_payload(overview.status.path, envelope, discover))
}

fn console_probe_envelope_to_models_payload(
    path: String,
    envelope: ConsoleModelsConnectionEnvelope,
    discover: bool,
) -> ModelsConnectionPayload {
    let mode = if discover { "discover" } else { "test_connection" };
    let providers = envelope
        .providers
        .into_iter()
        .map(|provider| {
            let live_discovery_verified = discover
                && provider.discovery_source == "live"
                && matches!(provider.state.as_str(), "ok" | "partial");
            ProviderConnectionCheckPayload {
                provider_id: provider.provider_id,
                kind: provider.kind,
                enabled: provider.enabled,
                endpoint_base_url: provider.endpoint_base_url,
                credential_source: provider.credential_source,
                state: provider.state,
                message: provider.message,
                checked_at_unix_ms: provider.checked_at_unix_ms,
                cache_status: provider.cache_status,
                live_discovery_verified,
                discovery_source: provider.discovery_source,
                discovered_model_ids: provider.discovered_model_ids,
                configured_model_ids: provider.configured_model_ids,
                latency_ms: provider.latency_ms,
            }
        })
        .collect::<Vec<_>>();
    ModelsConnectionPayload {
        path,
        mode,
        timeout_ms: envelope.timeout_ms,
        provider_filter: envelope.provider_filter,
        provider_count: envelope.provider_count,
        providers,
    }
}

fn console_missing_auth_needs_local_probe(
    console_providers: &[ProviderConnectionCheckPayload],
) -> Result<bool> {
    if !console_providers.iter().any(provider_probe_reports_missing_auth) {
        return Ok(false);
    }

    let overview = load_models_overview(None)?;
    let targets = build_probeable_providers(&overview)?;
    Ok(console_missing_auth_matches_local_credentials(console_providers, targets.as_slice()))
}

fn console_missing_auth_matches_local_credentials(
    console_providers: &[ProviderConnectionCheckPayload],
    local_targets: &[ProbeableProvider],
) -> bool {
    console_providers.iter().filter(|provider| provider_probe_reports_missing_auth(provider)).any(
        |provider| {
            local_targets.iter().any(|target| {
                target.provider_id == provider.provider_id
                    && probeable_provider_has_configured_credential(target)
            })
        },
    )
}

fn provider_probe_reports_missing_auth(provider: &ProviderConnectionCheckPayload) -> bool {
    provider.state == "missing_auth" && provider.credential_source == "none"
}

fn probeable_provider_has_configured_credential(provider: &ProbeableProvider) -> bool {
    provider.auth_profile_id.as_deref().and_then(normalize_optional_text).is_some()
        || provider.inline_api_key.as_deref().and_then(normalize_optional_text).is_some()
        || provider.vault_ref.as_deref().and_then(normalize_optional_text).is_some()
}

fn run_provider_checks_local(
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
                .map(|filter| provider_matches_filter(provider, filter))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    if filtered_targets.is_empty() {
        anyhow::bail!(
            "invalid provider filter '{}': no configured provider matched; use `palyra models list --json` to inspect provider_id values and aliases",
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

fn provider_matches_filter(provider: &ProbeableProvider, filter: &str) -> bool {
    let normalized_filter = normalized_provider_filter_alias(filter);
    [
        Some(provider.provider_id.as_str()),
        Some(provider.kind.as_str()),
        provider.auth_provider_kind.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|candidate| normalized_provider_filter_alias(candidate) == normalized_filter)
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
    let provider_identity_by_id = overview
        .providers
        .iter()
        .map(|provider| {
            (
                provider.provider_id.clone(),
                (
                    provider_display_name(provider),
                    provider.protocol_compatibility.clone(),
                    provider.auth_provider_kind.clone(),
                ),
            )
        })
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
            reasoning_effort: overview.status.reasoning_effort.clone(),
            service_tier: overview.status.service_tier.clone(),
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
        provider_display_name: provider_identity_by_id
            .get(primary.provider_id.as_str())
            .map(|(display_name, _, _)| display_name.clone())
            .unwrap_or_else(|| "Unknown".to_owned()),
        protocol_compatibility: provider_identity_by_id
            .get(primary.provider_id.as_str())
            .map(|(_, compatibility, _)| compatibility.clone())
            .unwrap_or_else(|| "unknown".to_owned()),
        auth_provider_kind: provider_identity_by_id
            .get(primary.provider_id.as_str())
            .and_then(|(_, _, auth_provider_kind)| auth_provider_kind.clone()),
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
        reasoning: primary.reasoning,
        reasoning_efforts: primary.reasoning_efforts.clone(),
        service_tier: primary.service_tier,
        service_tiers: primary.service_tiers.clone(),
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
                    provider_display_name: provider_identity_by_id
                        .get(candidate.provider_id.as_str())
                        .map(|(display_name, _, _)| display_name.clone())
                        .unwrap_or_else(|| "Unknown".to_owned()),
                    protocol_compatibility: provider_identity_by_id
                        .get(candidate.provider_id.as_str())
                        .map(|(_, compatibility, _)| compatibility.clone())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    auth_provider_kind: provider_identity_by_id
                        .get(candidate.provider_id.as_str())
                        .and_then(|(_, _, auth_provider_kind)| auth_provider_kind.clone()),
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
                    reasoning: candidate.reasoning,
                    reasoning_efforts: candidate.reasoning_efforts.clone(),
                    service_tier: candidate.service_tier,
                    service_tiers: candidate.service_tiers.clone(),
                }),
        );
    }

    Ok(ModelsExplainPayload {
        path: overview.status.path,
        requested_model_id,
        resolved_model_id: Some(primary.model_id),
        json_mode,
        vision,
        reasoning_effort: overview.status.reasoning_effort,
        service_tier: overview.status.service_tier,
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
    let config_path = Path::new(&path);
    let (document, migration) = load_document_from_existing_path(Path::new(&path))
        .with_context(|| format!("failed to parse {path}"))?;
    models_overview_from_document(path.clone(), config_path, &document, migration.migrated)
}

fn models_overview_from_document(
    path: String,
    config_path: &Path,
    document: &toml::Value,
    migrated: bool,
) -> Result<ModelsOverview> {
    let root_config = parse_root_file_config(document)?;
    let auth_state_roots = status_auth_profile_state_roots(&root_config, config_path);
    let model_provider = root_config.model_provider.unwrap_or_default();
    let provider_kind =
        model_provider.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let (providers, mut models) = registry_views_from_config(&model_provider);
    append_pending_openai_api_default_model(providers.as_slice(), &mut models);
    let validation_issues = validate_registry_views(
        providers.as_slice(),
        models.as_slice(),
        model_provider.default_chat_model_id.as_deref(),
        model_provider.default_embeddings_model_id.as_deref(),
    );
    let default_chat_model_id = effective_default_chat_model_id(&model_provider, models.as_slice());
    let configured_openai_base_url = if provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND {
        model_provider.openai_base_url.clone()
    } else {
        None
    };
    let text_model = model_provider
        .default_chat_model_id
        .clone()
        .or_else(|| legacy_chat_model_for_kind(provider_kind.as_str(), &model_provider))
        .or_else(|| default_chat_model_id.clone());
    let embeddings_model = model_provider.default_embeddings_model_id.clone().or_else(|| {
        (provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND)
            .then(|| model_provider.openai_embeddings_model.clone())
            .flatten()
    });
    let embeddings_dims = (provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND)
        .then_some(model_provider.openai_embeddings_dims)
        .flatten();
    let provider_id = default_provider_id(models.as_slice(), &model_provider)
        .map(str::to_owned)
        .or_else(|| providers.first().map(|entry| entry.provider_id.clone()))
        .unwrap_or_else(|| "unknown".to_owned());
    let provider_entry = providers.iter().find(|entry| entry.provider_id == provider_id);
    let provider_kind_for_status =
        provider_entry.map(|entry| entry.kind.clone()).unwrap_or_else(|| provider_kind.clone());
    let provider_display_name = provider_entry.map(provider_display_name).unwrap_or_else(|| {
        provider_display_name_from_kind(provider_kind_for_status.as_str()).to_owned()
    });
    let protocol_compatibility =
        provider_entry.map(|entry| entry.protocol_compatibility.clone()).unwrap_or_else(|| {
            protocol_compatibility_for_kind(provider_kind_for_status.as_str()).to_owned()
        });
    let auth_provider_kind =
        provider_entry.and_then(|entry| entry.auth_provider_kind.clone()).or_else(|| {
            (provider_kind_for_status != DETERMINISTIC_PROVIDER_KIND)
                .then(|| model_provider.auth_provider_kind.clone())
                .flatten()
        });
    let endpoint_base_url = provider_entry
        .and_then(|entry| entry.base_url.clone())
        .or_else(|| default_base_url_for_kind(provider_kind_for_status.as_str(), &model_provider));
    let auth_profile_id = provider_entry
        .and_then(|entry| entry.auth_profile_id.clone())
        .or_else(|| {
            (provider_kind_for_status != DETERMINISTIC_PROVIDER_KIND)
                .then(|| model_provider.auth_profile_id.clone())
                .flatten()
        })
        .or_else(|| {
            providers
                .iter()
                .find(|entry| {
                    Some(entry.provider_id.as_str())
                        == default_provider_id(models.as_slice(), &model_provider)
                })
                .and_then(|entry| entry.auth_profile_id.clone())
        });
    let api_key_configured =
        provider_entry.map(|entry| entry.api_key_configured).unwrap_or_else(|| {
            credential_configured_for_kind(provider_kind_for_status.as_str(), &model_provider)
        });
    let uses_chatgpt_oauth = status_uses_openai_chatgpt_oauth(
        provider_kind_for_status.as_str(),
        auth_profile_id.as_deref(),
        auth_state_roots.as_slice(),
    );
    let (endpoint_base_url, openai_base_url) = effective_status_base_urls(
        provider_kind_for_status.as_str(),
        endpoint_base_url,
        configured_openai_base_url,
        uses_chatgpt_oauth,
    );
    Ok(ModelsOverview {
        status: ModelsStatusPayload {
            path,
            provider_kind: provider_kind_for_status,
            provider_id,
            provider_display_name,
            protocol_compatibility,
            auth_provider_kind,
            endpoint_base_url,
            openai_base_url,
            text_model,
            embeddings_model,
            embeddings_dims,
            reasoning_effort: model_provider.reasoning_effort,
            service_tier: model_provider.service_tier,
            auth_profile_id,
            api_key_configured,
            default_chat_model_id,
            default_embeddings_model_id: model_provider.default_embeddings_model_id,
            failover_enabled: model_provider.failover_enabled.unwrap_or(true),
            response_cache_enabled: model_provider.response_cache_enabled.unwrap_or(true),
            registry_provider_count: providers.len(),
            registry_model_count: models.len(),
            registry_valid: validation_issues.is_empty(),
            validation_issues,
            migrated,
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
    let auth_state_roots = status_auth_profile_state_roots(&root_config, path_ref);
    let auth_vault_candidates =
        status_auth_vault_candidates(&root_config, path_ref, auth_state_roots.as_slice());
    let model_provider = root_config.model_provider.unwrap_or_default();
    let provider_kind =
        model_provider.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let allow_private_env = std::env::var("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL").ok();
    let global_allow_private_base_url =
        effective_global_allow_private_base_url(&model_provider, allow_private_env.as_deref());
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
                    allow_private_base_url: entry
                        .allow_private_base_url
                        .unwrap_or(global_allow_private_base_url),
                    auth_profile_id: entry.auth_profile_id.clone().or_else(|| {
                        inherit_globals.then(|| model_provider.auth_profile_id.clone()).flatten()
                    }),
                    auth_state_roots: auth_state_roots.clone(),
                    auth_vault_candidates: auth_vault_candidates.clone(),
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

    let (provider_id, _) = legacy_provider_identity(
        provider_kind.as_str(),
        model_provider.auth_provider_kind.as_deref(),
    );
    Ok(vec![ProbeableProvider {
        provider_id: provider_id.to_owned(),
        kind: provider_kind.clone(),
        enabled: true,
        endpoint_base_url: default_base_url_for_kind(provider_kind.as_str(), &model_provider),
        allow_private_base_url: global_allow_private_base_url,
        auth_profile_id: model_provider.auth_profile_id.clone(),
        auth_state_roots,
        auth_vault_candidates,
        auth_provider_kind: model_provider.auth_provider_kind.clone(),
        inline_api_key: inline_api_key_for_kind(provider_kind.as_str(), &model_provider),
        vault_ref: vault_ref_for_kind(provider_kind.as_str(), &model_provider),
        configured_model_ids: models_by_provider.get(provider_id).cloned().unwrap_or_default(),
    }])
}

fn effective_global_allow_private_base_url(
    config: &FileModelProviderConfig,
    env_override: Option<&str>,
) -> bool {
    if let Some(value) = env_override {
        return value.trim().parse::<bool>().unwrap_or(false);
    }
    config.allow_private_base_url.unwrap_or(false)
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

fn effective_status_base_urls(
    provider_kind: &str,
    endpoint_base_url: Option<String>,
    openai_base_url: Option<String>,
    uses_chatgpt_oauth: bool,
) -> (Option<String>, Option<String>) {
    if provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND && uses_chatgpt_oauth {
        let base_url = Some(chatgpt_oauth_status_base_url(
            endpoint_base_url.as_deref().or(openai_base_url.as_deref()),
        ));
        return (base_url.clone(), base_url);
    }
    (endpoint_base_url, openai_base_url)
}

fn chatgpt_oauth_status_base_url(configured_base_url: Option<&str>) -> String {
    let configured = configured_base_url
        .and_then(normalize_optional_text)
        .unwrap_or(OPENAI_DEFAULT_BASE_URL)
        .trim_end_matches('/');
    if configured.to_ascii_lowercase().contains("api.openai.com") {
        OPENAI_CODEX_BACKEND_BASE_URL.to_owned()
    } else {
        configured.to_owned()
    }
}

fn ensure_document_default_model_supports_service_tier(
    config_path: &Path,
    document: &toml::Value,
    service_tier: Option<&str>,
) -> Result<()> {
    if service_tier_is_default_or_unset(service_tier) {
        return Ok(());
    }
    let overview = models_overview_from_document(
        config_path.display().to_string(),
        config_path,
        document,
        false,
    )?;
    ensure_overview_default_model_supports_service_tier(&overview, service_tier)
}

fn ensure_overview_default_model_supports_service_tier(
    overview: &ModelsOverview,
    service_tier: Option<&str>,
) -> Result<()> {
    let Some(service_tier) = service_tier
        .and_then(normalize_optional_text)
        .filter(|tier| !tier.eq_ignore_ascii_case("default"))
    else {
        return Ok(());
    };
    let default_model_id = overview
        .status
        .default_chat_model_id
        .as_deref()
        .or(overview.status.text_model.as_deref())
        .and_then(normalize_optional_text)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cannot use service_tier={service_tier}: no default chat model is configured"
            )
        })?;
    let model = overview
        .models
        .iter()
        .find(|model| model.enabled && model.role == "chat" && model.model_id == default_model_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cannot use service_tier={service_tier}: default chat model '{default_model_id}' is not present in the model registry"
            )
        })?;
    let status_supports_service_tier = status_supports_openai_service_tier(&overview.status);
    let model_supports_service_tier =
        model.service_tier && model.service_tiers.iter().any(|tier| tier == service_tier);
    if status_supports_service_tier && model_supports_service_tier {
        return Ok(());
    }
    anyhow::bail!(
        "cannot use service_tier={service_tier}: default chat model '{}' on provider '{}' does not support this tier; use `palyra models list --json` to inspect service_tiers or select --no-fast/default",
        model.model_id,
        overview.status.provider_id,
    )
}

fn service_tier_is_default_or_unset(service_tier: Option<&str>) -> bool {
    service_tier
        .is_none_or(|tier| tier.trim().is_empty() || tier.trim().eq_ignore_ascii_case("default"))
}

fn status_supports_openai_service_tier(status: &ModelsStatusPayload) -> bool {
    status.provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND
        && auth_provider_kind_allows_openai_service_tier(status.auth_provider_kind.as_deref())
        && base_url_supports_openai_service_tier(
            status.endpoint_base_url.as_deref().or(status.openai_base_url.as_deref()),
        )
}

fn status_auth_profile_state_roots(
    config: &palyra_common::daemon_config_schema::RootFileConfig,
    config_path: &Path,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(identity_store_dir) = config
        .gateway
        .as_ref()
        .and_then(|gateway| gateway.identity_store_dir.as_deref())
        .and_then(normalize_optional_text)
    {
        push_unique_path(
            &mut roots,
            state_root_for_identity_store(PathBuf::from(identity_store_dir)),
        );
    }
    if let Ok(identity_store_root) = resolve_cli_identity_store_root() {
        push_unique_path(&mut roots, state_root_for_identity_store(identity_store_root));
    }
    if let Some(state_root) = status_state_root(config_path) {
        push_unique_path(&mut roots, state_root.clone());
        // Desktop-managed daemons keep runtime auth state under this child while
        // the CLI-visible config remains under the parent state root.
        push_unique_path(
            &mut roots,
            state_root.join(DESKTOP_CONTROL_CENTER_DIR).join(DESKTOP_RUNTIME_DIR),
        );
    }
    roots
}

fn status_auth_vault_candidates(
    config: &palyra_common::daemon_config_schema::RootFileConfig,
    config_path: &Path,
    auth_state_roots: &[PathBuf],
) -> Vec<ProbeAuthVaultCandidate> {
    let mut candidates = Vec::new();
    let configured_identity_store_root = config
        .gateway
        .as_ref()
        .and_then(|gateway| gateway.identity_store_dir.as_deref())
        .and_then(normalize_optional_text)
        .map(PathBuf::from);
    let base_state_root = configured_identity_store_root
        .as_ref()
        .map(|identity_store_root| state_root_for_identity_store(identity_store_root.clone()))
        .or_else(|| status_state_root(config_path))
        .or_else(|| auth_state_roots.first().cloned());
    let configured_identity_store_root = configured_identity_store_root
        .or_else(|| base_state_root.as_ref().map(|state_root| state_root.join("identity")));

    // Desktop-managed daemons can keep auth profiles in a runtime state root
    // while using the parent configured vault for the actual token objects.
    if let Some(configured_vault_root) =
        configured_auth_vault_root(config, base_state_root.as_deref())
    {
        if let Some(identity_store_root) = configured_identity_store_root {
            push_unique_auth_vault_candidate(
                &mut candidates,
                configured_vault_root,
                identity_store_root,
            );
        }
    }

    for state_root in auth_state_roots {
        push_unique_auth_vault_candidate(
            &mut candidates,
            state_root.join("vault"),
            state_root.join("identity"),
        );
    }

    candidates
}

fn configured_auth_vault_root(
    config: &palyra_common::daemon_config_schema::RootFileConfig,
    base_state_root: Option<&Path>,
) -> Option<PathBuf> {
    let env_vault_dir = std::env::var("PALYRA_VAULT_DIR").ok();
    let raw = env_vault_dir.as_deref().and_then(normalize_optional_text).or_else(|| {
        config
            .storage
            .as_ref()
            .and_then(|storage| storage.vault_dir.as_deref())
            .and_then(normalize_optional_text)
    })?;
    Some(resolve_state_relative_path(base_state_root, PathBuf::from(raw)))
}

fn resolve_state_relative_path(base_state_root: Option<&Path>, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    base_state_root.map(|state_root| state_root.join(path.as_path())).unwrap_or(path)
}

fn push_unique_auth_vault_candidate(
    candidates: &mut Vec<ProbeAuthVaultCandidate>,
    vault_root: PathBuf,
    identity_store_root: PathBuf,
) {
    let candidate = ProbeAuthVaultCandidate { vault_root, identity_store_root };
    if !candidates.iter().any(|existing| existing == &candidate) {
        candidates.push(candidate);
    }
}

fn state_root_for_identity_store(identity_store_root: PathBuf) -> PathBuf {
    identity_store_root
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or(identity_store_root)
}

fn status_state_root(config_path: &Path) -> Option<PathBuf> {
    let config_dir = config_path.parent()?;
    if config_dir.file_name().and_then(|name| name.to_str()) == Some("config") {
        if let Some(state_root) = config_dir.parent() {
            return Some(state_root.to_path_buf());
        }
    }
    if let Some(context) = app::current_root_context() {
        return Some(context.state_root().to_path_buf());
    }
    app::resolve_cli_state_root(None).ok()
}

fn push_unique_path(paths: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !paths.iter().any(|path| path == &candidate) {
        paths.push(candidate);
    }
}

fn status_uses_openai_chatgpt_oauth(
    provider_kind: &str,
    auth_profile_id: Option<&str>,
    auth_state_roots: &[PathBuf],
) -> bool {
    if provider_kind != OPENAI_COMPATIBLE_PROVIDER_KIND {
        return false;
    }
    let Some(profile_id) = auth_profile_id.and_then(normalize_optional_text) else {
        return false;
    };
    auth_state_roots.iter().any(|state_root| {
        let Ok(Some(profile)) = AuthProfileRegistry::get_profile_readonly_at_state_root(
            state_root.as_path(),
            profile_id,
        ) else {
            return false;
        };
        auth_credential_uses_openai_chatgpt_oauth(&profile.provider.kind, &profile.credential)
    })
}

fn auth_credential_uses_openai_chatgpt_oauth(
    provider_kind: &AuthProviderKind,
    credential: &AuthCredential,
) -> bool {
    matches!(
        credential,
        AuthCredential::Oauth { client_id, .. }
            if oauth_kind_for_profile(provider_kind, client_id.as_deref())
                == Some(ResolvedOauthProfileKind::OpenAiChatGptLogin)
    )
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
    let mut providers = config
        .providers
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let kind = entry.kind.clone().unwrap_or_else(|| {
                        config
                            .kind
                            .clone()
                            .unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned())
                    });
                    RegistryProviderEntry {
                        provider_id: entry.provider_id.clone().unwrap_or_default(),
                        display_name: entry.display_name.clone(),
                        protocol_compatibility: protocol_compatibility_for_kind(kind.as_str())
                            .to_owned(),
                        kind,
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
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| legacy_provider_entries(config));
    let mut models = config
        .models
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let provider_id = entry.provider_id.clone().unwrap_or_default();
                    let provider =
                        providers.iter().find(|provider| provider.provider_id == provider_id);
                    let reasoning = entry.reasoning.unwrap_or_else(|| {
                        entry.model_id.as_deref().is_some_and(model_id_supports_reasoning_effort)
                    });
                    let service_tier = entry.service_tier.unwrap_or_else(|| {
                        entry.service_tiers.as_ref().is_some_and(|values| !values.is_empty())
                            || provider.is_some_and(provider_supports_openai_service_tier)
                    });
                    RegistryModelEntry {
                        model_id: entry.model_id.clone().unwrap_or_default(),
                        provider_id,
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
                        reasoning,
                        reasoning_efforts: entry.reasoning_efforts.clone().unwrap_or_else(|| {
                            if reasoning {
                                default_reasoning_efforts()
                            } else {
                                Vec::new()
                            }
                        }),
                        service_tier,
                        service_tiers: entry.service_tiers.clone().unwrap_or_else(|| {
                            if service_tier {
                                default_service_tiers()
                            } else {
                                Vec::new()
                            }
                        }),
                        max_context_tokens: entry.max_context_tokens,
                        cost_tier: entry.cost_tier.clone().unwrap_or_else(|| "standard".to_owned()),
                        latency_tier: entry
                            .latency_tier
                            .clone()
                            .unwrap_or_else(|| "standard".to_owned()),
                        recommended_use_cases: entry
                            .recommended_use_cases
                            .clone()
                            .unwrap_or_default(),
                        known_limitations: entry.known_limitations.clone().unwrap_or_default(),
                        source: "registry",
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| legacy_model_entries(config));
    append_synthetic_default_model_entry(
        providers.as_slice(),
        &mut models,
        config.default_chat_model_id.as_deref(),
        "chat",
    );
    append_synthetic_default_model_entry(
        providers.as_slice(),
        &mut models,
        config.default_embeddings_model_id.as_deref(),
        "embeddings",
    );
    apply_global_auth_to_registry_provider_views(&mut providers, models.as_slice(), config);
    (providers, models)
}

fn apply_global_auth_to_registry_provider_views(
    providers: &mut [RegistryProviderEntry],
    models: &[RegistryModelEntry],
    config: &FileModelProviderConfig,
) {
    let default_provider_id = default_provider_id(models, config);
    let has_single_provider = providers.len() == 1;
    for provider in providers.iter_mut() {
        if provider.kind == DETERMINISTIC_PROVIDER_KIND {
            continue;
        }
        let inherits_global_auth = default_provider_id
            .is_some_and(|provider_id| provider_id == provider.provider_id)
            || (default_provider_id.is_none() && has_single_provider);
        if !inherits_global_auth {
            continue;
        }
        if provider.auth_profile_id.is_none() {
            provider.auth_profile_id = config.auth_profile_id.clone();
        }
        if provider.auth_provider_kind.is_none() {
            provider.auth_provider_kind = config.auth_provider_kind.clone();
        }
        if !provider.api_key_configured {
            provider.api_key_configured =
                credential_configured_for_kind(provider.kind.as_str(), config);
        }
    }
}

fn append_synthetic_default_model_entry(
    providers: &[RegistryProviderEntry],
    models: &mut Vec<RegistryModelEntry>,
    model_id: Option<&str>,
    role: &str,
) {
    let Some(model_id) = model_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    if models.iter().any(|entry| entry.model_id == model_id) {
        return;
    }
    let Some(provider) = provider_for_synthetic_default_model(providers, models.as_slice(), role)
    else {
        return;
    };
    models.push(synthetic_default_registry_model(model_id, provider, role));
}

fn append_pending_openai_api_default_model(
    providers: &[RegistryProviderEntry],
    models: &mut Vec<RegistryModelEntry>,
) {
    if !models.is_empty() {
        return;
    }
    let Some(provider) = pending_openai_api_key_provider(providers) else {
        return;
    };
    models.push(synthetic_default_registry_model(
        OPENAI_API_DEFAULT_CHAT_MODEL_ID,
        provider,
        "chat",
    ));
}

fn pending_openai_api_key_provider(
    providers: &[RegistryProviderEntry],
) -> Option<&RegistryProviderEntry> {
    let mut selected = None;
    for provider in providers.iter().filter(|provider| {
        provider.enabled
            && provider.kind == OPENAI_COMPATIBLE_PROVIDER_KIND
            && provider.api_key_configured
            && auth_provider_kind_allows_openai_api_default(provider.auth_provider_kind.as_deref())
            && base_url_is_official_openai_api(provider.base_url.as_deref())
    }) {
        if selected.is_some() {
            return None;
        }
        selected = Some(provider);
    }
    selected
}

fn auth_provider_kind_allows_openai_api_default(auth_provider_kind: Option<&str>) -> bool {
    auth_provider_kind.is_none_or(|kind| {
        kind.eq_ignore_ascii_case("openai")
            || kind.eq_ignore_ascii_case("openai_compatible")
            || kind.eq_ignore_ascii_case("openai-compatible")
    })
}

fn base_url_is_official_openai_api(base_url: Option<&str>) -> bool {
    let Some(url) = base_url.and_then(|value| reqwest::Url::parse(value).ok()) else {
        return false;
    };
    url.scheme() == "https"
        && url.host_str().is_some_and(|host| host.eq_ignore_ascii_case("api.openai.com"))
        && url.path().trim_end_matches('/').eq_ignore_ascii_case("/v1")
}

fn provider_for_synthetic_default_model<'a>(
    providers: &'a [RegistryProviderEntry],
    models: &[RegistryModelEntry],
    role: &str,
) -> Option<&'a RegistryProviderEntry> {
    let enabled_providers =
        providers.iter().filter(|provider| provider.enabled).collect::<Vec<_>>();
    if enabled_providers.len() == 1 {
        return enabled_providers.first().copied();
    }
    models
        .iter()
        .find(|model| model.enabled && model.role == role)
        .and_then(|model| {
            providers
                .iter()
                .find(|provider| provider.provider_id == model.provider_id && provider.enabled)
        })
        .or_else(|| enabled_providers.first().copied())
}

fn default_reasoning_efforts() -> Vec<String> {
    ["none", "minimal", "low", "medium", "high", "xhigh"]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
}

fn default_service_tiers() -> Vec<String> {
    ["auto", "default", "priority", "flex"].into_iter().map(ToOwned::to_owned).collect()
}

fn provider_supports_openai_service_tier(provider: &RegistryProviderEntry) -> bool {
    provider.kind == OPENAI_COMPATIBLE_PROVIDER_KIND
        && auth_provider_kind_allows_openai_service_tier(provider.auth_provider_kind.as_deref())
        && base_url_supports_openai_service_tier(provider.base_url.as_deref())
}

fn legacy_config_supports_openai_service_tier(
    provider_kind: &str,
    config: &FileModelProviderConfig,
) -> bool {
    provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND
        && auth_provider_kind_allows_openai_service_tier(config.auth_provider_kind.as_deref())
        && base_url_supports_openai_service_tier(
            default_base_url_for_kind(provider_kind, config).as_deref(),
        )
}

fn auth_provider_kind_allows_openai_service_tier(auth_provider_kind: Option<&str>) -> bool {
    auth_provider_kind.is_none_or(|kind| {
        kind.eq_ignore_ascii_case("openai")
            || kind.eq_ignore_ascii_case("openai_compatible")
            || kind.eq_ignore_ascii_case("openai-compatible")
    })
}

fn base_url_supports_openai_service_tier(base_url: Option<&str>) -> bool {
    let Some(url) = base_url.and_then(|value| reqwest::Url::parse(value).ok()) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("api.openai.com") {
        return true;
    }
    host.eq_ignore_ascii_case("chatgpt.com")
        && url.path().trim_end_matches('/').eq_ignore_ascii_case("/backend-api/codex")
}

fn synthetic_default_registry_model(
    model_id: &str,
    provider: &RegistryProviderEntry,
    role: &str,
) -> RegistryModelEntry {
    let is_chat = role == "chat";
    let is_embeddings = role == "embeddings";
    let is_minimax_chat = is_chat
        && provider.kind == ANTHROPIC_PROVIDER_KIND
        && provider
            .auth_provider_kind
            .as_deref()
            .is_some_and(|kind| kind.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND));
    let reasoning = is_chat && model_id_supports_reasoning_effort(model_id);
    let service_tier = is_chat && provider_supports_openai_service_tier(provider);
    RegistryModelEntry {
        model_id: model_id.to_owned(),
        provider_id: provider.provider_id.clone(),
        role: role.to_owned(),
        enabled: true,
        metadata_source: "operator_override".to_owned(),
        operator_override: true,
        tool_calls: is_chat,
        json_mode: is_chat && provider.kind != DETERMINISTIC_PROVIDER_KIND,
        vision: is_chat && provider.kind != DETERMINISTIC_PROVIDER_KIND && !is_minimax_chat,
        audio_transcribe: is_chat && provider.kind == OPENAI_COMPATIBLE_PROVIDER_KIND,
        embeddings: is_embeddings,
        reasoning,
        reasoning_efforts: if reasoning { default_reasoning_efforts() } else { Vec::new() },
        service_tier,
        service_tiers: if service_tier { default_service_tiers() } else { Vec::new() },
        max_context_tokens: if provider.kind == DETERMINISTIC_PROVIDER_KIND {
            None
        } else if is_embeddings {
            Some(8_192)
        } else {
            Some(128_000)
        },
        cost_tier: "standard".to_owned(),
        latency_tier: "standard".to_owned(),
        recommended_use_cases: if is_embeddings {
            vec!["memory retrieval".to_owned()]
        } else {
            vec!["provider-selected chat".to_owned()]
        },
        known_limitations: if is_minimax_chat {
            vec!["vision unsupported by MiniMax Anthropic-compatible chat".to_owned()]
        } else {
            Vec::new()
        },
        source: "synthetic_default",
    }
}

fn legacy_provider_entries(config: &FileModelProviderConfig) -> Vec<RegistryProviderEntry> {
    let kind = config.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let (provider_id, display_name) =
        legacy_provider_identity(kind.as_str(), config.auth_provider_kind.as_deref());
    vec![RegistryProviderEntry {
        provider_id: provider_id.to_owned(),
        display_name: Some(display_name.to_owned()),
        protocol_compatibility: protocol_compatibility_for_kind(kind.as_str()).to_owned(),
        kind: kind.clone(),
        base_url: default_base_url_for_kind(kind.as_str(), config),
        enabled: true,
        auth_profile_id: (kind != DETERMINISTIC_PROVIDER_KIND)
            .then(|| config.auth_profile_id.clone())
            .flatten(),
        auth_provider_kind: (kind != DETERMINISTIC_PROVIDER_KIND)
            .then(|| config.auth_provider_kind.clone())
            .flatten(),
        api_key_configured: credential_configured_for_kind(kind.as_str(), config),
        source: "legacy",
    }]
}

fn legacy_model_entries(config: &FileModelProviderConfig) -> Vec<RegistryModelEntry> {
    let kind = config.kind.clone().unwrap_or_else(|| DETERMINISTIC_PROVIDER_KIND.to_owned());
    let (provider_id, _) =
        legacy_provider_identity(kind.as_str(), config.auth_provider_kind.as_deref());
    let mut models = Vec::new();
    if let Some(model_id) = legacy_chat_model_for_kind(kind.as_str(), config) {
        models.push(legacy_registry_model(
            model_id,
            provider_id.to_owned(),
            "chat",
            kind.as_str(),
            config.auth_provider_kind.as_deref(),
            legacy_config_supports_openai_service_tier(kind.as_str(), config),
        ));
    }
    if let Some(model_id) = config.openai_embeddings_model.clone() {
        models.push(legacy_registry_model(
            model_id,
            provider_id.to_owned(),
            "embeddings",
            OPENAI_COMPATIBLE_PROVIDER_KIND,
            None,
            false,
        ));
    }
    models
}

fn legacy_chat_model_for_kind(kind: &str, config: &FileModelProviderConfig) -> Option<String> {
    match kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => config.openai_model.clone(),
        ANTHROPIC_PROVIDER_KIND => config.anthropic_model.clone(),
        DETERMINISTIC_PROVIDER_KIND => {
            config.openai_model.clone().or_else(|| Some(DETERMINISTIC_PROVIDER_KIND.to_owned()))
        }
        _ => None,
    }
}

fn credential_configured_for_kind(kind: &str, config: &FileModelProviderConfig) -> bool {
    match kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => {
            config.openai_api_key.as_deref().filter(|value| !value.trim().is_empty()).is_some()
                || config
                    .openai_api_key_vault_ref
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .is_some()
        }
        ANTHROPIC_PROVIDER_KIND => {
            config.anthropic_api_key.as_deref().filter(|value| !value.trim().is_empty()).is_some()
                || config
                    .anthropic_api_key_vault_ref
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .is_some()
        }
        _ => false,
    }
}

fn legacy_registry_model(
    model_id: String,
    provider_id: String,
    role: &str,
    provider_kind: &str,
    auth_provider_kind: Option<&str>,
    supports_service_tier: bool,
) -> RegistryModelEntry {
    let is_chat = role == "chat";
    let is_minimax_chat = is_chat
        && provider_kind == ANTHROPIC_PROVIDER_KIND
        && auth_provider_kind
            .is_some_and(|kind| kind.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND));
    let reasoning = is_chat && model_id_supports_reasoning_effort(model_id.as_str());
    let service_tier = is_chat && supports_service_tier;
    RegistryModelEntry {
        model_id,
        provider_id,
        role: role.to_owned(),
        enabled: true,
        metadata_source: "legacy_migration".to_owned(),
        operator_override: false,
        tool_calls: is_chat,
        json_mode: is_chat && provider_kind != DETERMINISTIC_PROVIDER_KIND,
        vision: is_chat && provider_kind != DETERMINISTIC_PROVIDER_KIND && !is_minimax_chat,
        audio_transcribe: is_chat && provider_kind == OPENAI_COMPATIBLE_PROVIDER_KIND,
        embeddings: role == "embeddings",
        reasoning,
        reasoning_efforts: if reasoning { default_reasoning_efforts() } else { Vec::new() },
        service_tier,
        service_tiers: if service_tier { default_service_tiers() } else { Vec::new() },
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
        known_limitations: if is_minimax_chat {
            vec!["vision unsupported by MiniMax Anthropic-compatible chat".to_owned()]
        } else {
            Vec::new()
        },
        source: "legacy",
    }
}

fn legacy_provider_identity(
    provider_kind: &str,
    auth_provider_kind: Option<&str>,
) -> (&'static str, &'static str) {
    legacy_provider_identity_for_file_config_kind(provider_kind, auth_provider_kind)
}

fn provider_display_name(provider: &RegistryProviderEntry) -> String {
    provider
        .display_name
        .clone()
        .unwrap_or_else(|| provider_display_name_from_kind(provider.kind.as_str()).to_owned())
}

fn provider_display_name_from_kind(provider_kind: &str) -> &'static str {
    match provider_kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => "OpenAI-compatible",
        ANTHROPIC_PROVIDER_KIND => "Anthropic-compatible",
        DETERMINISTIC_PROVIDER_KIND => "Deterministic",
        _ => "Unknown",
    }
}

fn protocol_compatibility_for_kind(provider_kind: &str) -> &'static str {
    match provider_kind {
        OPENAI_COMPATIBLE_PROVIDER_KIND => "openai_compatible",
        ANTHROPIC_PROVIDER_KIND => "anthropic_compatible",
        DETERMINISTIC_PROVIDER_KIND => "deterministic",
        _ => "unknown",
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
    if let Some(model_id) = default_chat_model_id {
        return models
            .iter()
            .find(|entry| entry.model_id == model_id && entry.role == "chat")
            .map(|entry| entry.provider_id.as_str());
    }
    models
        .iter()
        .find(|entry| entry.enabled && entry.role == "chat")
        .map(|entry| entry.provider_id.as_str())
}

fn effective_default_chat_model_id(
    config: &FileModelProviderConfig,
    models: &[RegistryModelEntry],
) -> Option<String> {
    config.default_chat_model_id.clone().or_else(|| {
        models
            .iter()
            .find(|entry| entry.enabled && entry.role == "chat")
            .map(|entry| entry.model_id.clone())
    })
}

fn provider_check_ttl_ms(overview: &ModelsOverview, discover: bool) -> i64 {
    let path_ref = Path::new(overview.status.path.as_str());
    // An unreadable config disables caching (TTL 0) instead of failing the
    // probe: the probe result is still useful, it just will not be reused.
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
    provider.allow_private_base_url.hash(&mut hasher);
    provider.auth_profile_id.hash(&mut hasher);
    provider.auth_vault_candidates.hash(&mut hasher);
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

/// Probes one provider's models endpoint and classifies the outcome into a
/// stable state string without ever surfacing credential material.
///
/// Endpoint policy is validated before credentials are resolved so an unsafe
/// base_url can never trigger auth-registry or vault access (pinned by test).
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
    if let Err(error) = validate_cli_probe_endpoint_policy(target, base_url) {
        payload.state = "endpoint_failed".to_owned();
        payload.message = sanitize_diagnostic_error(error.to_string().as_str());
        return payload;
    }
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

    let endpoint = match provider_models_endpoint_for_probe(target, base_url, &credential) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            payload.state = "endpoint_failed".to_owned();
            payload.message = sanitize_diagnostic_error(error.to_string().as_str());
            return payload;
        }
    };
    payload.endpoint_base_url = Some(endpoint.base_url.clone());

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
    // Native Anthropic endpoints expect x-api-key plus anthropic-version;
    // MiniMax's Anthropic-compatible endpoint authenticates with Bearer instead.
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
    match client.get(endpoint.url).headers(headers).send() {
        Ok(response) => {
            payload.latency_ms =
                Some(started_at.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
            let status = response.status();
            let body = response.text().unwrap_or_default();
            if status.is_success() {
                match parse_discovered_model_ids(body.as_str(), endpoint.response_format) {
                    Ok(discovered) => {
                        payload.live_discovery_verified = true;
                        payload.discovery_source = "live".to_owned();
                        payload.discovered_model_ids = discovered;
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
                                "provider connection succeeded; MiniMax-compatible model discovery returned no ids, so configured model registry remains the source of selectable models. This probe does not verify chat generation quota or model-turn usability."
                                    .to_owned()
                            } else {
                                "provider connection succeeded but model discovery returned no ids. This probe does not verify chat generation quota or model-turn usability."
                                    .to_owned()
                            }
                        } else {
                            format!(
                                "provider connection and live model discovery succeeded with {} model(s). This probe does not verify chat generation quota or model-turn usability.",
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
                payload.state = if discover {
                    "discovery_unsupported".to_owned()
                } else {
                    "verification_incomplete".to_owned()
                };
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

fn validate_cli_probe_endpoint_policy(target: &ProbeableProvider, base_url: &str) -> Result<()> {
    validate_provider_probe_base_url(base_url, target.allow_private_base_url)
}

fn validate_provider_probe_base_url(base_url: &str, allow_private_base_url: bool) -> Result<()> {
    let parsed = parse_provider_probe_base_url(base_url)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("provider base_url must include a host"))?;

    if allow_private_base_url {
        return Ok(());
    }

    if palyra_common::netguard::is_localhost_hostname(host) {
        anyhow::bail!(
            "provider base_url host '{}' targets localhost/private network; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
            host
        );
    }
    if let Some(address) =
        palyra_common::netguard::parse_host_ip_literal(host).map_err(anyhow::Error::msg)?
    {
        if palyra_common::netguard::is_private_or_local_ip(address) {
            anyhow::bail!(
                "provider base_url host '{}' targets localhost/private network; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
                host
            );
        }
        return Ok(());
    }

    let port = parsed.port_or_known_default().ok_or_else(|| {
        anyhow::anyhow!("provider base_url must include an explicit port for unknown URL schemes")
    })?;
    let resolved_addresses = resolve_hostname_ip_addrs(host, port).map_err(|error| {
        anyhow::anyhow!(
            "provider base_url host '{}' could not be resolved to enforce private-network guard: {}; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
            host,
            error
        )
    })?;
    if resolved_addresses.is_empty() {
        anyhow::bail!(
            "provider base_url host '{}' resolved with no addresses; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
            host
        );
    }
    if let Some(address) = resolved_addresses
        .into_iter()
        .find(|address| palyra_common::netguard::is_private_or_local_ip(*address))
    {
        anyhow::bail!(
            "provider base_url host '{}' resolves to private/local address '{}'; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
            host,
            address
        );
    }
    Ok(())
}

fn parse_provider_probe_base_url(base_url: &str) -> Result<reqwest::Url> {
    let normalized = base_url.trim();
    if normalized.is_empty() {
        anyhow::bail!("provider base_url cannot be empty");
    }
    let parsed = reqwest::Url::parse(normalized)
        .with_context(|| format!("invalid provider base_url: {base_url}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("provider base_url must include a host"))?;
    let host_ip_literal =
        palyra_common::netguard::parse_host_ip_literal(host).map_err(anyhow::Error::msg)?;
    let loopback_http_allowed = palyra_common::netguard::is_localhost_hostname(host)
        || host_ip_literal.is_some_and(|address| address.is_loopback());
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback_http_allowed) {
        anyhow::bail!(
            "provider base_url must use https (http is only allowed for loopback hosts with allow_private_base_url)"
        );
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("provider base_url must not embed credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("provider base_url must not include query or fragment");
    }
    Ok(parsed)
}

fn resolve_hostname_ip_addrs(host: &str, port: u16) -> std::io::Result<Vec<IpAddr>> {
    (host, port)
        .to_socket_addrs()
        .map(|socket_addrs| socket_addrs.map(|socket_addr| socket_addr.ip()).collect())
}

fn resolve_provider_credential(
    target: &ProbeableProvider,
    _auth_registry: &mut Option<AuthProfileRegistry>,
    vault: &mut Option<palyra_vault::Vault>,
) -> Result<Option<ResolvedCredential>> {
    if let Some(profile_id) = target.auth_profile_id.as_deref() {
        let (profile, state_root) = find_auth_profile_for_probe(target, profile_id)?;
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
        return match profile.credential {
            AuthCredential::ApiKey { api_key_vault_ref } => {
                let token = load_auth_profile_secret_utf8(
                    target,
                    state_root.as_path(),
                    api_key_vault_ref.as_str(),
                )?;
                Ok(Some(ResolvedCredential::ApiKey { token, source: "auth_profile".to_owned() }))
            }
            AuthCredential::Oauth { access_token_vault_ref, client_id, .. } => {
                let token = load_auth_profile_secret_utf8(
                    target,
                    state_root.as_path(),
                    access_token_vault_ref.as_str(),
                )?;
                Ok(Some(ResolvedCredential::Bearer {
                    token,
                    source: "auth_profile".to_owned(),
                    oauth_kind: oauth_kind_for_profile(
                        &profile.provider.kind,
                        client_id.as_deref(),
                    ),
                }))
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

fn find_auth_profile_for_probe(
    target: &ProbeableProvider,
    profile_id: &str,
) -> Result<(palyra_auth::AuthProfileRecord, PathBuf)> {
    let mut searched = Vec::new();
    for state_root in &target.auth_state_roots {
        searched.push(state_root.display().to_string());
        let profile = AuthProfileRegistry::get_profile_readonly_at_state_root(
            state_root.as_path(),
            profile_id,
        )
        .with_context(|| {
            format!("failed to load auth profile '{profile_id}' from {}", state_root.display())
        })?;
        if let Some(profile) = profile {
            return Ok((profile, state_root.clone()));
        }
    }
    anyhow::bail!(
        "auth profile not found: {profile_id} (searched state roots: {})",
        searched.join(", ")
    );
}

fn load_auth_profile_secret_utf8(
    target: &ProbeableProvider,
    profile_state_root: &Path,
    vault_ref: &str,
) -> Result<String> {
    let mut candidates = target.auth_vault_candidates.clone();
    push_unique_auth_vault_candidate(
        &mut candidates,
        profile_state_root.join("vault"),
        profile_state_root.join("identity"),
    );
    if candidates.is_empty() {
        anyhow::bail!(
            "no auth-profile vault roots configured for profile state root {}",
            profile_state_root.display()
        );
    }

    let mut errors = Vec::new();
    for candidate in candidates {
        let loaded = open_auth_vault_candidate(&candidate)
            .and_then(|vault_instance| load_vault_secret_utf8(&vault_instance, vault_ref));
        match loaded {
            Ok(token) => return Ok(token),
            Err(error) => errors.push(format!("{} ({error:#})", candidate.vault_root.display())),
        }
    }
    anyhow::bail!(
        "failed to load auth profile secret from candidate vault roots: {}",
        errors.join("; ")
    )
}

fn open_auth_vault_candidate(candidate: &ProbeAuthVaultCandidate) -> Result<Vault> {
    Vault::open_with_config(VaultConfigOptions {
        root: Some(candidate.vault_root.clone()),
        identity_store_root: Some(candidate.identity_store_root.clone()),
        backend_preference: parse_cli_vault_backend_preference()?,
        ..VaultConfigOptions::default()
    })
    .map_err(anyhow::Error::from)
}

fn target_uses_minimax_auth(target: &ProbeableProvider) -> bool {
    target
        .auth_provider_kind
        .as_deref()
        .is_some_and(|kind| kind.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND))
}

fn target_uses_openai_chatgpt_oauth(
    target: &ProbeableProvider,
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
    let is_chatgpt_login =
        provider_kind == &AuthProviderKind::Openai && is_openai_chatgpt_oauth_client_id(client_id);
    is_chatgpt_login.then_some(ResolvedOauthProfileKind::OpenAiChatGptLogin)
}

fn expected_auth_provider_for_probe_target(target: &ProbeableProvider) -> Option<AuthProviderKind> {
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
    target: &ProbeableProvider,
) -> Option<&'static str> {
    let auth_provider_kind = target.auth_provider_kind.as_deref()?.trim().to_ascii_lowercase();
    match auth_provider_kind.as_str() {
        "minimax" | "minimax-portal" => Some(MINIMAX_AUTH_PROVIDER_KIND),
        "xai" | "x-ai" | "grok" => Some("xai"),
        "google_gemini" | "google-gemini" | "gemini" => Some("google_gemini"),
        "google_gemini_cli" | "google-gemini-cli" | "gemini_cli" | "gemini-cli" => {
            Some("google_gemini_cli")
        }
        "openrouter" | "open-router" => Some("openrouter"),
        _ => None,
    }
}

fn load_vault_secret_utf8(vault: &palyra_vault::Vault, vault_ref: &str) -> Result<String> {
    let parsed = VaultRef::parse(vault_ref)?;
    let bytes =
        vault.get_secret(&parsed.scope, parsed.key.as_str()).map_err(anyhow::Error::from)?;
    String::from_utf8(bytes).context("vault secret must contain valid UTF-8")
}

fn provider_models_endpoint_for_probe(
    target: &ProbeableProvider,
    base_url: &str,
    credential: &ResolvedCredential,
) -> Result<ProviderModelsEndpoint> {
    build_provider_models_endpoint_for_probe(
        base_url,
        target_uses_openai_chatgpt_oauth(target, credential),
    )
}

fn classify_provider_failure(status_code: u16) -> String {
    match status_code {
        401 | 403 => "auth_failed".to_owned(),
        429 => "rate_limited".to_owned(),
        500..=599 => "endpoint_failed".to_owned(),
        _ => "unexpected_response".to_owned(),
    }
}

/// Formats a provider HTTP error body into a redacted, single-line message.
pub(crate) fn sanitize_provider_error(body: &str, status_code: u16) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_common::daemon_config_schema::{
        FileGatewayConfig, FileModelProviderRegistryEntry, FileStorageConfig, RootFileConfig,
    };
    use palyra_model_providers::{
        parse_discovered_provider_models, provider_models_endpoint,
        select_preferred_discovered_model_id, ProviderModelsResponseFormat,
        OPENAI_CHATGPT_OAUTH_CLIENT_ID, OPENAI_CODEX_MODELS_ENDPOINT,
    };

    fn sample_probe_target(base_url: &str, allow_private_base_url: bool) -> ProbeableProvider {
        ProbeableProvider {
            provider_id: "openai-primary".to_owned(),
            kind: OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned(),
            enabled: true,
            endpoint_base_url: Some(base_url.to_owned()),
            allow_private_base_url,
            auth_profile_id: Some("missing-auth-profile".to_owned()),
            auth_state_roots: Vec::new(),
            auth_vault_candidates: Vec::new(),
            auth_provider_kind: None,
            inline_api_key: None,
            vault_ref: None,
            configured_model_ids: Vec::new(),
        }
    }

    fn sample_provider_check(
        state: &str,
        credential_source: &str,
    ) -> ProviderConnectionCheckPayload {
        ProviderConnectionCheckPayload {
            provider_id: "openai-primary".to_owned(),
            kind: OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned(),
            enabled: true,
            endpoint_base_url: Some("https://api.openai.com/v1".to_owned()),
            credential_source: credential_source.to_owned(),
            state: state.to_owned(),
            message: "test".to_owned(),
            checked_at_unix_ms: 1,
            cache_status: "miss".to_owned(),
            live_discovery_verified: false,
            discovery_source: "live".to_owned(),
            discovered_model_ids: Vec::new(),
            configured_model_ids: Vec::new(),
            latency_ms: None,
        }
    }

    #[test]
    fn pending_openai_api_key_registry_uses_provider_default_for_status() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let config_path = tempdir.path().join("palyra.toml");
        let document = toml::from_str::<toml::Value>(
            r#"
version = 1

[model_provider]
kind = "openai_compatible"
auth_provider_kind = "openai"
openai_base_url = "https://api.openai.com/v1"
openai_api_key_vault_ref = "global/openai_api_key"

[[model_provider.providers]]
provider_id = "openai-primary"
display_name = "OpenAI"
kind = "openai_compatible"
base_url = "https://api.openai.com/v1"
enabled = true
"#,
        )
        .expect("config should parse");

        let overview = models_overview_from_document(
            config_path.display().to_string(),
            config_path.as_path(),
            &document,
            false,
        )
        .expect("models overview should load");

        assert_eq!(overview.status.text_model.as_deref(), Some(OPENAI_API_DEFAULT_CHAT_MODEL_ID));
        assert_eq!(
            overview.status.default_chat_model_id.as_deref(),
            Some(OPENAI_API_DEFAULT_CHAT_MODEL_ID)
        );
        assert!(overview.status.registry_valid, "{:?}", overview.status.validation_issues);
        assert_eq!(overview.status.registry_model_count, 1);
        assert_eq!(overview.models[0].model_id, OPENAI_API_DEFAULT_CHAT_MODEL_ID);
        assert_eq!(overview.models[0].source, "synthetic_default");
    }

    #[test]
    fn discovered_model_selection_uses_complete_provider_recency_metadata() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"provider-stable","created":1700000000},{"id":"provider-current","created":1800000000}]}"#,
        )
        .expect("provider discovery payload should parse");

        assert_eq!(
            select_preferred_discovered_model_id(models.as_slice()).as_deref(),
            Some("provider-current")
        );
    }

    #[test]
    fn discovered_model_selection_prefers_tool_capable_provider_metadata() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"provider-newer-no-tools","created":1800000000,"supported_parameters":["temperature"]},{"id":"provider-tools","created":1700000000,"supported_parameters":["tools","response_format"],"architecture":{"input_modalities":["text"]}}]}"#,
        )
        .expect("provider discovery payload should parse");

        assert_eq!(
            select_preferred_discovered_model_id(models.as_slice()).as_deref(),
            Some("provider-tools")
        );
    }

    #[test]
    fn discovered_model_selection_rejects_image_output_default() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"google/gemini-3-pro-image","created":1800000000,"supported_parameters":["tools"],"architecture":{"input_modalities":["text"],"output_modalities":["image"]}},{"id":"deepseek/deepseek-v4-flash","created":1700000000,"supported_parameters":["tools","response_format"],"architecture":{"input_modalities":["text"],"output_modalities":["text"]}}]}"#,
        )
        .expect("provider discovery payload should parse");

        assert_eq!(
            select_preferred_discovered_model_id(models.as_slice()).as_deref(),
            Some("deepseek/deepseek-v4-flash")
        );
    }

    #[test]
    fn discovered_model_selection_rejects_video_output_default() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"grok-imagine-video-1.5","created":1800000000,"supported_parameters":["tools"],"architecture":{"input_modalities":["text"],"output_modalities":["video"]}},{"id":"grok-4.3","created":1700000000,"supported_parameters":["tools"],"architecture":{"input_modalities":["text"],"output_modalities":["text"]}}]}"#,
        )
        .expect("provider discovery payload should parse");

        assert_eq!(
            select_preferred_discovered_model_id(models.as_slice()).as_deref(),
            Some("grok-4.3")
        );
    }

    #[test]
    fn discovered_model_selection_returns_none_for_media_only_inventory() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"gpt-image-2","created":1800000000,"architecture":{"output_modalities":["image"]}},{"id":"grok-imagine-video-1.5","created":1700000000,"architecture":{"output_modalities":["video"]}}]}"#,
        )
        .expect("provider discovery payload should parse");

        assert_eq!(select_preferred_discovered_model_id(models.as_slice()), None);
    }

    #[test]
    fn discovered_model_selection_preserves_provider_order_without_complete_recency_metadata() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"MiniMax-M3"},{"id":"MiniMax-M2.7","created":1700000000},{"id":"MiniMax-M2.5"}]}"#,
        )
        .expect("provider discovery payload should parse");

        assert_eq!(
            select_preferred_discovered_model_id(models.as_slice()).as_deref(),
            Some("MiniMax-M3")
        );
    }

    #[test]
    fn registry_provider_view_inherits_global_xai_oauth_profile_for_single_provider() {
        let config = FileModelProviderConfig {
            kind: Some(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
            auth_profile_id: Some("xai-oauth-test".to_owned()),
            auth_provider_kind: Some("xai".to_owned()),
            providers: Some(vec![FileModelProviderRegistryEntry {
                provider_id: Some("xai-primary".to_owned()),
                kind: Some(OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned()),
                base_url: Some("https://api.x.ai/v1".to_owned()),
                ..FileModelProviderRegistryEntry::default()
            }]),
            ..FileModelProviderConfig::default()
        };

        let (providers, models) = registry_views_from_config(&config);

        assert!(models.is_empty(), "test covers empty registry state after OAuth");
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].auth_profile_id.as_deref(), Some("xai-oauth-test"));
        assert_eq!(providers[0].auth_provider_kind.as_deref(), Some("xai"));
    }

    #[test]
    fn console_missing_auth_with_local_profile_requires_local_probe() {
        let console_provider = sample_provider_check("missing_auth", "none");
        let target = sample_probe_target("https://api.openai.com/v1", false);

        assert!(console_missing_auth_matches_local_credentials(&[console_provider], &[target]));
    }

    #[test]
    fn console_missing_auth_without_local_credential_keeps_console_result() {
        let console_provider = sample_provider_check("missing_auth", "none");
        let mut target = sample_probe_target("https://api.openai.com/v1", false);
        target.auth_profile_id = None;

        assert!(!console_missing_auth_matches_local_credentials(&[console_provider], &[target]));
    }

    #[test]
    fn console_ok_probe_does_not_retry_locally() {
        let console_provider = sample_provider_check("ok", "auth_profile");
        let target = sample_probe_target("https://api.openai.com/v1", false);

        assert!(!console_missing_auth_matches_local_credentials(&[console_provider], &[target]));
    }

    #[test]
    fn cli_probe_endpoint_policy_blocks_private_base_url_without_opt_in() {
        let error = validate_provider_probe_base_url("http://127.0.0.1:11434/v1", false)
            .expect_err("private provider targets require explicit opt-in");

        assert!(
            error.to_string().contains("allow_private_base_url"),
            "error should point operators at the explicit private-target opt-in: {error}"
        );
    }

    #[test]
    fn cli_probe_endpoint_policy_allows_private_base_url_with_opt_in() {
        validate_provider_probe_base_url("http://127.0.0.1:11434/v1", true)
            .expect("trusted local provider targets should be allowed with explicit opt-in");
    }

    #[test]
    fn cli_probe_endpoint_policy_rejects_public_http_even_with_opt_in() {
        let error = validate_provider_probe_base_url("http://example.com/v1", true)
            .expect_err("public provider targets must use HTTPS");

        assert!(
            error.to_string().contains("must use https"),
            "error should explain the HTTPS requirement: {error}"
        );
    }

    #[test]
    fn cli_probe_endpoint_policy_rejects_credentials_query_and_fragment() {
        for base_url in [
            "https://user:pass@example.com/v1",
            "https://example.com/v1?api_key=secret",
            "https://example.com/v1#fragment",
        ] {
            validate_provider_probe_base_url(base_url, true)
                .expect_err("provider base_url must not carry credential-bearing URL parts");
        }
    }

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
    fn models_status_base_urls_use_codex_backend_for_chatgpt_oauth() {
        let (endpoint_base_url, openai_base_url) = effective_status_base_urls(
            OPENAI_COMPATIBLE_PROVIDER_KIND,
            Some("https://api.openai.com/v1".to_owned()),
            Some("https://api.openai.com/v1".to_owned()),
            true,
        );

        assert_eq!(endpoint_base_url.as_deref(), Some(OPENAI_CODEX_BACKEND_BASE_URL));
        assert_eq!(openai_base_url.as_deref(), Some(OPENAI_CODEX_BACKEND_BASE_URL));
    }

    #[test]
    fn models_status_base_urls_keep_custom_chatgpt_oauth_destination() {
        let (endpoint_base_url, openai_base_url) = effective_status_base_urls(
            OPENAI_COMPATIBLE_PROVIDER_KIND,
            Some("https://proxy.example.test/openai".to_owned()),
            Some("https://proxy.example.test/openai".to_owned()),
            true,
        );

        assert_eq!(endpoint_base_url.as_deref(), Some("https://proxy.example.test/openai"));
        assert_eq!(openai_base_url.as_deref(), Some("https://proxy.example.test/openai"));
    }

    #[test]
    fn models_status_base_urls_keep_configured_url_without_chatgpt_oauth() {
        let (endpoint_base_url, openai_base_url) = effective_status_base_urls(
            OPENAI_COMPATIBLE_PROVIDER_KIND,
            Some("https://api.x.ai/v1".to_owned()),
            Some("https://api.x.ai/v1".to_owned()),
            false,
        );

        assert_eq!(endpoint_base_url.as_deref(), Some("https://api.x.ai/v1"));
        assert_eq!(openai_base_url.as_deref(), Some("https://api.x.ai/v1"));
    }

    #[test]
    fn openai_chatgpt_oauth_detection_requires_chatgpt_client_id() {
        let chatgpt_credential = AuthCredential::Oauth {
            access_token_vault_ref: "global/openai_access".to_owned(),
            refresh_token_vault_ref: "global/openai_refresh".to_owned(),
            token_endpoint: "https://auth.openai.com/oauth/token".to_owned(),
            client_id: Some(OPENAI_CHATGPT_OAUTH_CLIENT_ID.to_owned()),
            client_secret_vault_ref: None,
            scopes: Vec::new(),
            expires_at_unix_ms: None,
            refresh_state: Default::default(),
        };
        let api_key_credential =
            AuthCredential::ApiKey { api_key_vault_ref: "global/openai_key".to_owned() };

        assert!(auth_credential_uses_openai_chatgpt_oauth(
            &AuthProviderKind::Openai,
            &chatgpt_credential
        ));
        assert!(!auth_credential_uses_openai_chatgpt_oauth(
            &AuthProviderKind::Openai,
            &api_key_credential
        ));
    }

    #[test]
    fn openai_chatgpt_oauth_status_detection_checks_desktop_runtime_registry() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let state_root = tempdir.path().join("state");
        let desktop_runtime = state_root.join(DESKTOP_CONTROL_CENTER_DIR).join(DESKTOP_RUNTIME_DIR);
        let desktop_identity = desktop_runtime.join("identity");
        let registry =
            AuthProfileRegistry::open(desktop_identity.as_path()).expect("registry should open");
        registry
            .set_profile(palyra_auth::AuthProfileSetRequest {
                profile_id: "chatgpt-login-test".to_owned(),
                provider: palyra_auth::AuthProvider::known(AuthProviderKind::Openai),
                profile_name: "ChatGPT Login".to_owned(),
                scope: palyra_auth::AuthProfileScope::Global,
                credential: AuthCredential::Oauth {
                    access_token_vault_ref: "global/openai_access".to_owned(),
                    refresh_token_vault_ref: "global/openai_refresh".to_owned(),
                    token_endpoint: "https://auth.openai.com/oauth/token".to_owned(),
                    client_id: Some(OPENAI_CHATGPT_OAUTH_CLIENT_ID.to_owned()),
                    client_secret_vault_ref: None,
                    scopes: Vec::new(),
                    expires_at_unix_ms: None,
                    refresh_state: Default::default(),
                },
            })
            .expect("profile should persist");

        assert!(status_uses_openai_chatgpt_oauth(
            OPENAI_COMPATIBLE_PROVIDER_KIND,
            Some("chatgpt-login-test"),
            &[state_root, desktop_runtime]
        ));
    }

    #[test]
    fn auth_vault_candidates_include_configured_storage_vault_with_gateway_identity() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let state_root = tempdir.path().join("state");
        let identity_store_root = state_root.join("identity");
        let configured_vault_root = state_root.join("vault");
        let desktop_runtime = state_root.join(DESKTOP_CONTROL_CENTER_DIR).join(DESKTOP_RUNTIME_DIR);
        let config_path = state_root.join("config").join("palyra.toml");
        let config = RootFileConfig {
            gateway: Some(FileGatewayConfig {
                identity_store_dir: Some(identity_store_root.display().to_string()),
                ..FileGatewayConfig::default()
            }),
            storage: Some(FileStorageConfig {
                vault_dir: Some(configured_vault_root.display().to_string()),
                ..FileStorageConfig::default()
            }),
            ..RootFileConfig::default()
        };

        let candidates =
            status_auth_vault_candidates(&config, config_path.as_path(), &[desktop_runtime]);

        assert!(
            candidates.iter().any(|candidate| {
                candidate.vault_root == configured_vault_root
                    && candidate.identity_store_root == identity_store_root
            }),
            "configured daemon vault must be available for desktop runtime auth profiles"
        );
    }

    #[test]
    fn auth_profile_probe_resolves_desktop_runtime_profile_from_configured_parent_vault() {
        let _env_guard =
            crate::app::test_env_lock_for_tests().lock().expect("env lock should be available");
        let _vault_backend = ScopedVaultBackend::encrypted_file();
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let state_root = tempdir.path().join("state");
        let parent_identity = state_root.join("identity");
        let parent_vault = state_root.join("vault");
        let desktop_runtime = state_root.join(DESKTOP_CONTROL_CENTER_DIR).join(DESKTOP_RUNTIME_DIR);
        let desktop_identity = desktop_runtime.join("identity");
        let registry =
            AuthProfileRegistry::open(desktop_identity.as_path()).expect("registry should open");
        registry
            .set_profile(palyra_auth::AuthProfileSetRequest {
                profile_id: "chatgpt-login-test".to_owned(),
                provider: palyra_auth::AuthProvider::known(AuthProviderKind::Openai),
                profile_name: "ChatGPT Login".to_owned(),
                scope: palyra_auth::AuthProfileScope::Global,
                credential: AuthCredential::Oauth {
                    access_token_vault_ref: "global/openai_access".to_owned(),
                    refresh_token_vault_ref: "global/openai_refresh".to_owned(),
                    token_endpoint: "https://auth.openai.com/oauth/token".to_owned(),
                    client_id: Some(OPENAI_CHATGPT_OAUTH_CLIENT_ID.to_owned()),
                    client_secret_vault_ref: None,
                    scopes: Vec::new(),
                    expires_at_unix_ms: None,
                    refresh_state: Default::default(),
                },
            })
            .expect("profile should persist");
        let scope = "global".parse::<palyra_vault::VaultScope>().expect("scope should parse");
        let vault = Vault::open_with_config(VaultConfigOptions {
            root: Some(parent_vault.clone()),
            identity_store_root: Some(parent_identity.clone()),
            backend_preference: palyra_vault::BackendPreference::EncryptedFile,
            ..VaultConfigOptions::default()
        })
        .expect("runtime vault should open");
        vault
            .put_secret(&scope, "openai_access", b"runtime-oauth-token")
            .expect("access token should persist");

        let mut target = sample_probe_target("https://api.openai.com/v1", true);
        target.auth_profile_id = Some("chatgpt-login-test".to_owned());
        target.auth_state_roots = vec![state_root, desktop_runtime];
        target.auth_vault_candidates = vec![ProbeAuthVaultCandidate {
            vault_root: parent_vault,
            identity_store_root: parent_identity,
        }];
        let mut auth_registry = None;
        let mut vault = None;
        let credential = resolve_provider_credential(&target, &mut auth_registry, &mut vault)
            .expect("credential lookup should succeed")
            .expect("credential should be resolved");

        assert!(auth_registry.is_none(), "profile lookup should use explicit state roots");
        assert!(vault.is_none(), "auth-profile secrets should use their matching state root vault");
        match credential {
            ResolvedCredential::Bearer { token, oauth_kind, .. } => {
                assert_eq!(token, "runtime-oauth-token");
                assert_eq!(oauth_kind, Some(ResolvedOauthProfileKind::OpenAiChatGptLogin));
            }
            ResolvedCredential::ApiKey { .. } => panic!("ChatGPT OAuth profile should be bearer"),
        }
    }

    #[test]
    fn auth_profile_probe_resolves_xai_runtime_profile_from_configured_parent_vault() {
        let _env_guard =
            crate::app::test_env_lock_for_tests().lock().expect("env lock should be available");
        let _vault_backend = ScopedVaultBackend::encrypted_file();
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let state_root = tempdir.path().join("state");
        let parent_identity = state_root.join("identity");
        let parent_vault = state_root.join("vault");
        let desktop_runtime = state_root.join(DESKTOP_CONTROL_CENTER_DIR).join(DESKTOP_RUNTIME_DIR);
        let desktop_identity = desktop_runtime.join("identity");
        let registry =
            AuthProfileRegistry::open(desktop_identity.as_path()).expect("registry should open");
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
        let scope = "global".parse::<palyra_vault::VaultScope>().expect("scope should parse");
        let vault = Vault::open_with_config(VaultConfigOptions {
            root: Some(parent_vault.clone()),
            identity_store_root: Some(parent_identity.clone()),
            backend_preference: palyra_vault::BackendPreference::EncryptedFile,
            ..VaultConfigOptions::default()
        })
        .expect("runtime vault should open");
        vault
            .put_secret(&scope, "xai_access", b"runtime-xai-oauth-token")
            .expect("access token should persist");

        let mut target = sample_probe_target("https://api.x.ai/v1", true);
        target.provider_id = "xai-primary".to_owned();
        target.auth_provider_kind = Some("xai".to_owned());
        target.auth_profile_id = Some("xai-oauth-test".to_owned());
        target.auth_state_roots = vec![state_root, desktop_runtime];
        target.auth_vault_candidates = vec![ProbeAuthVaultCandidate {
            vault_root: parent_vault,
            identity_store_root: parent_identity,
        }];
        let mut auth_registry = None;
        let mut vault = None;
        let credential = resolve_provider_credential(&target, &mut auth_registry, &mut vault)
            .expect("credential lookup should succeed")
            .expect("credential should be resolved");

        assert!(auth_registry.is_none(), "profile lookup should use explicit state roots");
        assert!(vault.is_none(), "auth-profile secrets should use their matching state root vault");
        match credential {
            ResolvedCredential::Bearer { token, source, oauth_kind } => {
                assert_eq!(token, "runtime-xai-oauth-token");
                assert_eq!(source, "auth_profile");
                assert_eq!(oauth_kind, None);
            }
            ResolvedCredential::ApiKey { .. } => panic!("xAI OAuth profile should be bearer"),
        }
    }

    struct ScopedVaultBackend {
        previous: Option<std::ffi::OsString>,
    }

    impl ScopedVaultBackend {
        fn encrypted_file() -> Self {
            let previous = std::env::var_os("PALYRA_VAULT_BACKEND");
            // SAFETY: this test holds the shared CLI test env lock while the override is active.
            unsafe {
                std::env::set_var("PALYRA_VAULT_BACKEND", "encrypted_file");
            }
            Self { previous }
        }
    }

    impl Drop for ScopedVaultBackend {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                // SAFETY: this test holds the shared CLI test env lock while the override is active.
                unsafe {
                    std::env::set_var("PALYRA_VAULT_BACKEND", previous);
                }
            } else {
                // SAFETY: this test holds the shared CLI test env lock while the override is active.
                unsafe {
                    std::env::remove_var("PALYRA_VAULT_BACKEND");
                }
            }
        }
    }

    #[test]
    fn models_status_state_root_is_derived_from_explicit_config_path() {
        let config_path = Path::new("C:/isolated-state/config/palyra.toml");

        assert_eq!(status_state_root(config_path).as_deref(), Some(Path::new("C:/isolated-state")));
    }

    #[test]
    fn chatgpt_oauth_probe_uses_codex_models_endpoint() {
        let target = sample_probe_target("https://api.openai.com/v1", false);
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
        let target = sample_probe_target("https://api.openai.com/v1", false);
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

    #[test]
    fn probe_rejects_unsafe_endpoint_before_resolving_credentials() {
        let target = sample_probe_target("http://127.0.0.1:11434/v1", false);
        let mut auth_registry = None;
        let mut vault = None;

        let payload = probe_provider(&target, 100, 1, false, &mut auth_registry, &mut vault);

        assert_eq!(payload.state, "endpoint_failed");
        assert_eq!(payload.credential_source, "none");
        assert!(
            payload.message.contains("allow_private_base_url"),
            "policy failure should be reported before auth profile lookup: {payload:?}"
        );
        assert!(
            auth_registry.is_none() && vault.is_none(),
            "unsafe endpoints must be rejected before opening auth registry or vault"
        );
    }

    #[test]
    fn console_probe_envelope_maps_to_cli_payload_shape() {
        let payload = console_probe_envelope_to_models_payload(
            "C:/state/config/palyra.toml".to_owned(),
            ConsoleModelsConnectionEnvelope {
                timeout_ms: 5_000,
                provider_filter: Some("openai-primary".to_owned()),
                provider_count: 1,
                providers: vec![ConsoleProviderConnectionPayload {
                    provider_id: "openai-primary".to_owned(),
                    kind: OPENAI_COMPATIBLE_PROVIDER_KIND.to_owned(),
                    enabled: true,
                    endpoint_base_url: Some("https://api.openai.com/v1".to_owned()),
                    credential_source: "auth_profile".to_owned(),
                    state: "ok".to_owned(),
                    message: "provider connection succeeded and discovered 2 model(s)".to_owned(),
                    checked_at_unix_ms: 1_700_000_000_000,
                    cache_status: "miss".to_owned(),
                    discovery_source: "live".to_owned(),
                    discovered_model_ids: vec!["gpt-a".to_owned(), "gpt-b".to_owned()],
                    configured_model_ids: vec!["gpt-a".to_owned()],
                    latency_ms: Some(42),
                }],
            },
            true,
        );

        assert_eq!(payload.path, "C:/state/config/palyra.toml");
        assert_eq!(payload.mode, "discover");
        assert_eq!(payload.provider_filter.as_deref(), Some("openai-primary"));
        assert_eq!(payload.providers[0].credential_source, "auth_profile");
        assert!(payload.providers[0].live_discovery_verified);
        assert_eq!(payload.providers[0].discovered_model_ids, ["gpt-a", "gpt-b"]);
    }

    #[test]
    fn provider_checks_cache_accepts_legacy_payload_without_live_discovery_flag() {
        let raw = serde_json::json!({
            "entries": {
                "test-connection:legacy": {
                    "expires_at_unix_ms": 4_102_444_800_000_i64,
                    "payload": {
                        "provider_id": "openai-primary",
                        "kind": "openai_compatible",
                        "enabled": true,
                        "endpoint_base_url": "https://api.openai.com/v1",
                        "credential_source": "vault",
                        "state": "ok",
                        "message": "connection ok",
                        "checked_at_unix_ms": 1_700_000_000_000_i64,
                        "cache_status": "fresh",
                        "discovery_source": "configured",
                        "discovered_model_ids": [],
                        "configured_model_ids": ["gpt-4o-mini"],
                        "latency_ms": 42
                    }
                }
            }
        });

        let cache: ProviderChecksCacheDocument =
            serde_json::from_value(raw).expect("legacy provider check cache should deserialize");
        let entry = cache
            .entries
            .get("test-connection:legacy")
            .expect("legacy cache entry should be retained");

        assert!(
            !entry.payload.live_discovery_verified,
            "legacy cache entries should default missing live discovery verification to false"
        );
    }
}
