use std::{collections::BTreeSet, fs, path::PathBuf};

use palyra_skills::{verify_skill_artifact, SkillTrustStore};

use crate::{
    hooks::{hooks_for_plugin, load_hook_bindings_index, resolve_hooks_root},
    plugins::{
        delete_plugin_binding, load_plugin_bindings_index, normalize_plugin_binding_upsert,
        plugin_binding, resolve_plugins_root, save_plugin_bindings_index,
        set_plugin_binding_enabled, upsert_plugin_binding, PluginBindingRecord,
        PluginBindingUpsert, PluginCapabilityProfile, PluginOperatorMetadata,
    },
    wasm_plugin_runner::{resolve_installed_skill_module, ResolvedInstalledSkillModule},
    *,
};

#[derive(Debug, Deserialize)]
pub(crate) struct ConsolePluginsListQuery {
    plugin_id: Option<String>,
    skill_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsolePluginInstallOrBindRequest {
    plugin_id: String,
    #[serde(default)]
    skill_id: Option<String>,
    #[serde(default)]
    skill_version: Option<String>,
    #[serde(default)]
    artifact_path: Option<String>,
    #[serde(default)]
    tool_id: Option<String>,
    #[serde(default)]
    module_path: Option<String>,
    #[serde(default)]
    entrypoint: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    allow_tofu: Option<bool>,
    #[serde(default)]
    allow_untrusted: Option<bool>,
    #[serde(default)]
    capability_profile: Option<PluginCapabilityProfile>,
    #[serde(default)]
    operator: Option<PluginOperatorMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleToggleRequest {}

pub(crate) async fn console_plugins_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsolePluginsListQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let mut index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    if let Some(plugin_id) =
        query.plugin_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        index.entries.retain(|entry| entry.plugin_id == plugin_id.to_ascii_lowercase());
    }
    if let Some(skill_id) =
        query.skill_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        index.entries.retain(|entry| entry.skill_id == skill_id.to_ascii_lowercase());
    }
    let mut entries = Vec::with_capacity(index.entries.len());
    for binding in index.entries {
        let check = build_plugin_binding_check(&state, &binding).await;
        entries.push(json!({
            "binding": binding,
            "check": check,
        }));
    }
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "plugins_root": plugins_root,
        "count": entries.len(),
        "entries": entries,
        "page": build_page_info(entries.len().max(1), entries.len(), None),
    })))
}

pub(crate) async fn console_plugin_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let binding = plugin_binding(&index, plugin_id.as_str()).map_err(not_found_console_error)?;
    let check = build_plugin_binding_check(&state, &binding).await;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding,
        "check": check,
    })))
}

pub(crate) async fn console_plugins_install_or_bind_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsolePluginInstallOrBindRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let mut skill_id = payload.skill_id.and_then(trim_to_option);
    let mut skill_version = payload.skill_version.and_then(trim_to_option);
    let mut installed_record = None::<InstalledSkillRecord>;

    if let Some(artifact_path) =
        payload.artifact_path.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        let record = install_skill_artifact_for_plugin_binding(
            PathBuf::from(artifact_path),
            payload.allow_tofu.unwrap_or(true),
            payload.allow_untrusted.unwrap_or(false),
        )?;
        if let Some(expected) = skill_id.as_deref() {
            if expected.to_ascii_lowercase() != record.skill_id {
                return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
                    "artifact skill_id '{}' does not match requested skill_id '{}'",
                    record.skill_id, expected
                ))));
            }
        }
        if let Some(expected) = skill_version.as_deref() {
            if expected != record.version {
                return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
                    "artifact version '{}' does not match requested skill_version '{}'",
                    record.version, expected
                ))));
            }
        }
        skill_id = Some(record.skill_id.clone());
        skill_version = Some(record.version.clone());
        installed_record = Some(record);
    }

    let skill_id = skill_id.ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "skill_id is required when artifact_path is not provided",
        ))
    })?;
    let operator = payload.operator.unwrap_or_default();
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let mut index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let existing = index
        .entries
        .iter()
        .find(|entry| entry.plugin_id == payload.plugin_id.trim().to_ascii_lowercase());
    let upsert = PluginBindingUpsert {
        plugin_id: payload.plugin_id,
        enabled: payload.enabled.unwrap_or(true),
        skill_id,
        skill_version,
        tool_id: payload.tool_id,
        module_path: payload.module_path,
        entrypoint: payload.entrypoint,
        capability_profile: payload.capability_profile.unwrap_or_default(),
        operator: PluginOperatorMetadata {
            updated_by: Some(session.context.principal.clone()),
            owner_principal: operator
                .owner_principal
                .or_else(|| Some(session.context.principal.clone())),
            display_name: operator.display_name,
            notes: operator.notes,
            tags: operator.tags,
        },
    };
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let binding =
        normalize_plugin_binding_upsert(upsert, now, existing).map_err(internal_console_error)?;
    let _ = resolve_installed_skill_module(
        binding.skill_id.as_str(),
        binding.skill_version.as_deref(),
        binding.module_path.as_deref(),
        binding.entrypoint.as_deref(),
        binding.tool_id.as_deref(),
    )
    .map_err(|error| runtime_status_response(tonic::Status::invalid_argument(error.message)))?;
    let binding = upsert_plugin_binding(&mut index, binding);
    save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    let check = build_plugin_binding_check(&state, &binding).await;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding,
        "installed_skill": installed_record,
        "check": check,
    })))
}

pub(crate) async fn console_plugin_check_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let binding = plugin_binding(&index, plugin_id.as_str()).map_err(not_found_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding.clone(),
        "check": build_plugin_binding_check(&state, &binding).await,
    })))
}

pub(crate) async fn console_plugin_enable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
    Json(_payload): Json<ConsoleToggleRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let mut index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let binding = set_plugin_binding_enabled(
        &mut index,
        plugin_id.as_str(),
        true,
        Some(session.context.principal.as_str()),
    )
    .map_err(not_found_console_error)?;
    save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding.clone(),
        "check": build_plugin_binding_check(&state, &binding).await,
    })))
}

pub(crate) async fn console_plugin_disable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
    Json(_payload): Json<ConsoleToggleRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let mut index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let binding = set_plugin_binding_enabled(
        &mut index,
        plugin_id.as_str(),
        false,
        Some(session.context.principal.as_str()),
    )
    .map_err(not_found_console_error)?;
    save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding.clone(),
        "check": build_plugin_binding_check(&state, &binding).await,
    })))
}

pub(crate) async fn console_plugin_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let hooks_index =
        load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    let referenced_by_hooks = hooks_for_plugin(&hooks_index, plugin_id.as_str());
    if !referenced_by_hooks.is_empty() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "plugin binding '{}' is still referenced by {} hook(s)",
            plugin_id,
            referenced_by_hooks.len()
        ))));
    }
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let mut index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let binding =
        delete_plugin_binding(&mut index, plugin_id.as_str()).map_err(not_found_console_error)?;
    save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "deleted": true,
        "binding": binding,
    })))
}

async fn build_plugin_binding_check(state: &AppState, binding: &PluginBindingRecord) -> Value {
    let mut ready = binding.enabled;
    let mut reasons = Vec::<String>::new();
    if !binding.enabled {
        reasons.push("plugin binding is disabled".to_owned());
    }

    let resolved = match resolve_installed_skill_module(
        binding.skill_id.as_str(),
        binding.skill_version.as_deref(),
        binding.module_path.as_deref(),
        binding.entrypoint.as_deref(),
        binding.tool_id.as_deref(),
    ) {
        Ok(resolved) => Some(resolved),
        Err(error) => {
            ready = false;
            reasons.push(sanitize_http_error_message(error.message.as_str()));
            None
        }
    };

    let mut skill_status_payload = Value::Null;
    let mut capability_payload = json!({
        "valid": true,
        "denied_http_hosts": [],
        "denied_secrets": [],
        "denied_storage_prefixes": [],
        "denied_channels": [],
    });
    let mut resolved_payload = Value::Null;

    if let Some(resolved) = resolved.as_ref() {
        match state
            .runtime
            .skill_status(resolved.skill_id.clone(), resolved.skill_version.clone())
            .await
        {
            Ok(status) => {
                if let Some(record) = status.as_ref() {
                    if matches!(
                        record.status,
                        SkillExecutionStatus::Quarantined | SkillExecutionStatus::Disabled
                    ) {
                        ready = false;
                        reasons.push(format!("skill status is {}", record.status.as_str()));
                    }
                }
                skill_status_payload = serde_json::to_value(status).unwrap_or_else(|_| Value::Null);
            }
            Err(error) => {
                ready = false;
                reasons.push(format!("failed to load skill status: {error}"));
            }
        }

        capability_payload =
            build_capability_profile_payload(&binding.capability_profile, resolved);
        if !capability_payload.get("valid").and_then(Value::as_bool).unwrap_or(false) {
            ready = false;
            reasons.push("plugin capability profile exceeds signed skill manifest".to_owned());
        }
        resolved_payload = json!({
            "skill_id": resolved.skill_id,
            "skill_version": resolved.skill_version,
            "module_path": resolved.module_path,
            "entrypoint": resolved.entrypoint,
            "tool_id": resolved.selected_tool.as_ref().map(|tool| tool.id.clone()),
            "tool_name": resolved.selected_tool.as_ref().map(|tool| tool.name.clone()),
            "publisher": resolved.manifest.publisher,
            "current_binding_version": binding.skill_version.is_none(),
        });
    }

    reasons.sort();
    reasons.dedup();
    json!({
        "ready": ready,
        "reasons": reasons,
        "skill_status": skill_status_payload,
        "resolved": resolved_payload,
        "capabilities": capability_payload,
    })
}

fn build_capability_profile_payload(
    profile: &PluginCapabilityProfile,
    resolved: &ResolvedInstalledSkillModule,
) -> Value {
    let declared_http =
        resolved.capability_grants.http_hosts.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let declared_secrets =
        resolved.capability_grants.secret_keys.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let declared_storage = resolved
        .capability_grants
        .storage_prefixes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let declared_channels =
        resolved.capability_grants.channels.iter().map(String::as_str).collect::<BTreeSet<_>>();

    let denied_http_hosts = profile
        .http_hosts
        .iter()
        .filter(|value| !declared_http.contains(value.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let denied_secrets = profile
        .secrets
        .iter()
        .filter(|value| !declared_secrets.contains(value.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let denied_storage_prefixes = profile
        .storage_prefixes
        .iter()
        .filter(|value| !declared_storage.contains(value.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let denied_channels = profile
        .channels
        .iter()
        .filter(|value| !declared_channels.contains(value.as_str()))
        .cloned()
        .collect::<Vec<_>>();

    json!({
        "valid": denied_http_hosts.is_empty()
            && denied_secrets.is_empty()
            && denied_storage_prefixes.is_empty()
            && denied_channels.is_empty(),
        "requested": profile,
        "declared": resolved.capability_grants,
        "denied_http_hosts": denied_http_hosts,
        "denied_secrets": denied_secrets,
        "denied_storage_prefixes": denied_storage_prefixes,
        "denied_channels": denied_channels,
    })
}

#[allow(clippy::result_large_err)]
fn install_skill_artifact_for_plugin_binding(
    artifact_path: PathBuf,
    allow_tofu: bool,
    allow_untrusted: bool,
) -> Result<InstalledSkillRecord, Response> {
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read artifact {}: {error}",
            artifact_path.display()
        )))
    })?;
    let inspection = inspect_skill_artifact(artifact_bytes.as_slice()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill artifact inspection failed: {error}"
        )))
    })?;

    let skills_root = resolve_skills_root()?;
    fs::create_dir_all(skills_root.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to create skills root {}: {error}",
            skills_root.display()
        )))
    })?;
    let trust_store_path = resolve_skills_trust_store_path(skills_root.as_path());
    let mut trust_store = load_trust_store_for_plugins(trust_store_path.as_path())?;
    let verification =
        match verify_skill_artifact(artifact_bytes.as_slice(), &mut trust_store, allow_tofu) {
            Ok(report) => Some(report),
            Err(error) if allow_untrusted => {
                tracing::warn!(
                    error = %error,
                    artifact_path = %artifact_path.display(),
                    "plugin install-or-bind proceeding with allow_untrusted override"
                );
                None
            }
            Err(error) => {
                return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
                    "skill artifact verification failed: {error}"
                ))));
            }
        };
    save_trust_store_for_plugins(trust_store_path.as_path(), &trust_store)?;

    let skill_id = inspection.manifest.skill_id.clone();
    let version = inspection.manifest.version.clone();
    let managed_artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    if let Some(parent) = managed_artifact_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to create managed skill directory {}: {error}",
                parent.display()
            )))
        })?;
    }
    fs::write(managed_artifact_path.as_path(), artifact_bytes.as_slice()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist managed artifact {}: {error}",
            managed_artifact_path.display()
        )))
    })?;

    let mut index = load_installed_skills_index(skills_root.as_path())?;
    index.entries.retain(|entry| !(entry.skill_id == skill_id && entry.version == version));
    for entry in &mut index.entries {
        if entry.skill_id == skill_id {
            entry.current = false;
        }
    }
    let record = InstalledSkillRecord {
        skill_id: skill_id.clone(),
        version: version.clone(),
        publisher: inspection.manifest.publisher.clone(),
        current: true,
        installed_at_unix_ms: unix_ms_now().map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to read system clock: {error}"
            )))
        })?,
        artifact_sha256: sha256_hex(artifact_bytes.as_slice()),
        payload_sha256: verification
            .as_ref()
            .map(|report| report.payload_sha256.clone())
            .unwrap_or_else(|| inspection.payload_sha256.clone()),
        signature_key_id: inspection.signature.key_id.clone(),
        trust_decision: verification
            .as_ref()
            .map(|report| trust_decision_label(report.trust_decision))
            .unwrap_or_else(|| "untrusted_override".to_owned()),
        source: InstalledSkillSource {
            kind: "managed_artifact".to_owned(),
            reference: artifact_path.to_string_lossy().into_owned(),
        },
        missing_secrets: Vec::new(),
    };
    index.entries.push(record.clone());
    save_installed_skills_index(skills_root.as_path(), &index)?;
    Ok(record)
}

#[allow(clippy::result_large_err)]
fn load_trust_store_for_plugins(path: &FsPath) -> Result<SkillTrustStore, Response> {
    if !path.exists() {
        return Ok(SkillTrustStore::default());
    }
    SkillTrustStore::load(path).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to load trust store {}: {error}",
            path.display()
        )))
    })
}

#[allow(clippy::result_large_err)]
fn save_trust_store_for_plugins(path: &FsPath, store: &SkillTrustStore) -> Result<(), Response> {
    store.save(path).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist trust store {}: {error}",
            path.display()
        )))
    })
}

fn internal_console_error(error: anyhow::Error) -> Response {
    runtime_status_response(tonic::Status::internal(sanitize_http_error_message(
        error.to_string().as_str(),
    )))
}

fn not_found_console_error(error: anyhow::Error) -> Response {
    runtime_status_response(tonic::Status::not_found(sanitize_http_error_message(
        error.to_string().as_str(),
    )))
}
