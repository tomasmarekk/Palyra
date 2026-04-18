use std::{collections::BTreeMap, fs, path::PathBuf};

use anyhow::anyhow;
use palyra_skills::{verify_skill_artifact, SkillTrustStore};

use crate::{
    hooks::{hooks_for_plugin, load_hook_bindings_index, resolve_hooks_root},
    plugins::{
        build_plugin_capability_diff, build_plugin_discovery_snapshot, delete_plugin_binding,
        inspect_plugin_filesystem_safety, load_plugin_bindings_index, load_plugin_config_instance,
        normalize_plugin_binding_upsert, plugin_binding, prepare_plugin_root,
        redact_plugin_config_values, remove_plugin_config_instance, resolve_plugins_root,
        save_plugin_bindings_index, save_plugin_config_instance, set_plugin_binding_enabled,
        upsert_plugin_binding, validate_plugin_config_instance, PluginBindingRecord,
        PluginBindingUpsert, PluginCapabilityProfile, PluginConfigInstance, PluginConfigInstanceRef,
        PluginConfigValidationState, PluginOperatorMetadata,
    },
    wasm_plugin_runner::{
        resolve_installed_skill_module, ResolvedInstalledSkillModule, WasmPluginRunnerPolicy,
    },
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
    #[serde(default)]
    config: Option<Value>,
    #[serde(default)]
    clear_config: Option<bool>,
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
    let mut dirty = false;
    let mut entries = Vec::with_capacity(index.entries.len());
    for binding in &mut index.entries {
        let (updated_binding, check, _) =
            evaluate_plugin_binding(&state, plugins_root.as_path(), binding).await?;
        if *binding != updated_binding {
            *binding = updated_binding.clone();
            dirty = true;
        }
        entries.push(json!({
            "binding": updated_binding,
            "check": check,
        }));
    }
    if dirty {
        save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    }
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
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
    let mut index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let binding = plugin_binding(&index, plugin_id.as_str()).map_err(not_found_console_error)?;
    let position = index
        .entries
        .iter()
        .position(|entry| entry.plugin_id == binding.plugin_id)
        .ok_or_else(|| not_found_console_error(anyhow!("plugin binding not found")))?;
    let (binding, check, installed_skill) =
        evaluate_plugin_binding(&state, plugins_root.as_path(), &binding).await?;
    if index.entries[position] != binding {
        index.entries[position] = binding.clone();
        save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    }
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding,
        "check": check,
        "installed_skill": installed_skill,
    })))
}

pub(crate) async fn console_plugins_install_or_bind_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsolePluginInstallOrBindRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let ConsolePluginInstallOrBindRequest {
        plugin_id,
        skill_id: requested_skill_id,
        skill_version: requested_skill_version,
        artifact_path,
        tool_id,
        module_path,
        entrypoint,
        enabled,
        allow_tofu,
        allow_untrusted,
        capability_profile,
        operator,
        config,
        clear_config,
    } = payload;
    let config_payload = normalize_plugin_config_payload(config)?;
    let clear_config = clear_config.unwrap_or(false);
    let requested_capability_profile = capability_profile.clone();
    let mut skill_id = requested_skill_id.and_then(trim_to_option);
    let mut skill_version = requested_skill_version.and_then(trim_to_option);
    let mut installed_record = None::<InstalledSkillRecord>;

    if let Some(artifact_path) =
        artifact_path.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        let record = install_skill_artifact_for_plugin_binding(
            PathBuf::from(artifact_path),
            allow_tofu.unwrap_or(true),
            allow_untrusted.unwrap_or(false),
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
    let operator = operator.unwrap_or_default();
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let mut index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let existing = index
        .entries
        .iter()
        .find(|entry| entry.plugin_id == plugin_id.trim().to_ascii_lowercase());
    let upsert = PluginBindingUpsert {
        plugin_id,
        enabled: enabled.unwrap_or(true),
        skill_id,
        skill_version,
        tool_id,
        module_path,
        entrypoint,
        capability_profile: capability_profile.unwrap_or_default(),
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
    let mut binding =
        normalize_plugin_binding_upsert(upsert, now, existing).map_err(internal_console_error)?;
    let mut resolved = resolve_installed_skill_module(
        binding.skill_id.as_str(),
        binding.skill_version.as_deref(),
        binding.module_path.as_deref(),
        binding.entrypoint.as_deref(),
        binding.tool_id.as_deref(),
    )
    .map_err(|error| runtime_status_response(tonic::Status::invalid_argument(error.message)))?;
    apply_manifest_binding_defaults(&mut binding, &resolved);
    if requested_capability_profile.is_none() && binding.capability_profile.is_empty() {
        binding.capability_profile =
            crate::plugins::plugin_capability_profile_from_manifest(&resolved.manifest);
    }
    resolved = resolve_installed_skill_module(
        binding.skill_id.as_str(),
        binding.skill_version.as_deref(),
        binding.module_path.as_deref(),
        binding.entrypoint.as_deref(),
        binding.tool_id.as_deref(),
    )
    .map_err(|error| runtime_status_response(tonic::Status::invalid_argument(error.message)))?;
    let current_installed_record = if installed_record.is_none() {
        lookup_installed_skill_record(binding.skill_id.as_str(), binding.skill_version.as_deref())?
    } else {
        None
    };
    let manifest_payload_sha256 = installed_record
        .as_ref()
        .or(current_installed_record.as_ref())
        .map(|record| record.payload_sha256.as_str());
    let resolved_skill_version = binding.skill_version.clone();
    persist_binding_config_instance(
        plugins_root.as_path(),
        &mut binding,
        &resolved.manifest,
        resolved_skill_version.as_deref(),
        config_payload.as_ref(),
        clear_config,
        manifest_payload_sha256,
        now,
    )?;
    let binding = upsert_plugin_binding(&mut index, binding);
    let (binding, check, installed_skill) =
        evaluate_plugin_binding(&state, plugins_root.as_path(), &binding).await?;
    upsert_plugin_binding(&mut index, binding.clone());
    save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding,
        "installed_skill": installed_skill,
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
    let mut index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let binding = plugin_binding(&index, plugin_id.as_str()).map_err(not_found_console_error)?;
    let position = index
        .entries
        .iter()
        .position(|entry| entry.plugin_id == binding.plugin_id)
        .ok_or_else(|| not_found_console_error(anyhow!("plugin binding not found")))?;
    let (binding, check, installed_skill) =
        evaluate_plugin_binding(&state, plugins_root.as_path(), &binding).await?;
    if index.entries[position] != binding {
        index.entries[position] = binding.clone();
        save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    }
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding.clone(),
        "installed_skill": installed_skill,
        "check": check,
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
    let (binding, check, installed_skill) =
        evaluate_plugin_binding(&state, plugins_root.as_path(), &binding).await?;
    upsert_plugin_binding(&mut index, binding.clone());
    save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding.clone(),
        "installed_skill": installed_skill,
        "check": check,
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
    let (binding, check, installed_skill) =
        evaluate_plugin_binding(&state, plugins_root.as_path(), &binding).await?;
    upsert_plugin_binding(&mut index, binding.clone());
    save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding.clone(),
        "installed_skill": installed_skill,
        "check": check,
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
    remove_plugin_config_instance(plugins_root.as_path(), binding.plugin_id.as_str())
        .map_err(internal_console_error)?;
    save_plugin_bindings_index(plugins_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "deleted": true,
        "binding": binding,
    })))
}

async fn evaluate_plugin_binding(
    state: &AppState,
    plugins_root: &FsPath,
    binding: &PluginBindingRecord,
) -> Result<(PluginBindingRecord, Value, Option<Value>), Response> {
    let mut binding = binding.clone();
    let mut ready = binding.enabled;
    let mut reasons = Vec::<String>::new();
    let mut remediation = Vec::<String>::new();
    if !binding.enabled {
        reasons.push("plugin binding is disabled".to_owned());
        remediation.push("Enable the plugin binding.".to_owned());
    }

    let filesystem = inspect_plugin_filesystem_safety(plugins_root, binding.plugin_id.as_str());
    if !filesystem.safe {
        ready = false;
        reasons.extend(filesystem.issues.iter().map(|issue| issue.message.clone()));
        remediation.extend(filesystem.issues.iter().map(|issue| issue.remediation.clone()));
    }

    let installed_skill = lookup_installed_skill_record(
        binding.skill_id.as_str(),
        binding.skill_version.as_deref(),
    )?;
    if matches!(
        installed_skill.as_ref().map(|record| record.trust_decision.as_str()),
        Some("untrusted_override")
    ) {
        ready = false;
        reasons.push("installed skill artifact is running under untrusted override".to_owned());
        remediation.push(
            "Reinstall or rebind the plugin from a verified signed artifact without --allow-untrusted."
                .to_owned(),
        );
    }
    let installed_skill_payload = installed_skill
        .as_ref()
        .and_then(|record| serde_json::to_value(record).ok());
    let manifest_payload_sha256 =
        installed_skill.as_ref().map(|record| record.payload_sha256.as_str());
    let config_instance =
        load_plugin_config_instance(plugins_root, binding.plugin_id.as_str()).map_err(internal_console_error)?;

    let mut skill_status_payload = Value::Null;
    let mut capability_payload = serde_json::to_value(&binding.capability_diff).unwrap_or(Value::Null);
    let mut config_payload = Value::Null;
    let mut resolved_payload = Value::Null;

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
            let error_message = sanitize_http_error_message(error.message.as_str());
            reasons.push(error_message.clone());
            remediation.push(
                "Repair the selected tool/module/entrypoint or reinstall the referenced skill artifact."
                    .to_owned(),
            );
            binding.discovery = build_plugin_discovery_snapshot(
                &binding,
                false,
                Some(error_message.as_str()),
                installed_skill.as_ref().map(|record| record.trust_decision.as_str()),
                filesystem.clone(),
                None,
            );
            None
        }
    };

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
                        remediation.push(
                            "Enable or unquarantine the installed skill before running the plugin."
                                .to_owned(),
                        );
                    }
                }
                skill_status_payload = serde_json::to_value(status).unwrap_or_else(|_| Value::Null);
            }
            Err(error) => {
                ready = false;
                reasons.push(format!("failed to load skill status: {error}"));
            }
        }

        let wasm_policy = build_wasm_policy(state)?;
        binding.capability_diff =
            build_plugin_capability_diff(&resolved.manifest, &binding.capability_profile, &wasm_policy);
        if !binding.capability_diff.valid {
            ready = false;
            reasons.push("plugin capability profile has binding/policy drift".to_owned());
            remediation.push(
                "Align binding grants with the signed manifest and current wasm runtime policy."
                    .to_owned(),
            );
        }
        capability_payload = serde_json::to_value(&binding.capability_diff).unwrap_or(Value::Null);

        let (config_validation, effective_config) =
            validate_plugin_config_instance(&resolved.manifest, config_instance.as_ref(), manifest_payload_sha256);
        binding.config = if resolved.manifest.operator.config.is_some() || config_instance.is_some() {
            Some(PluginConfigInstanceRef {
                schema_version: 1,
                path: format!("{}/config.json", binding.plugin_id),
                contract_schema_version: resolved
                    .manifest
                    .operator
                    .config
                    .as_ref()
                    .map(|contract| contract.schema_version),
                manifest_payload_sha256: manifest_payload_sha256.map(ToOwned::to_owned),
                validation: config_validation.clone(),
            })
        } else {
            None
        };
        if !matches!(config_validation.state, PluginConfigValidationState::Valid) {
            ready = false;
            reasons.extend(config_validation.issues.iter().cloned());
            remediation.push(
                "Update the plugin config instance so it satisfies the manifest contract and current schema."
                    .to_owned(),
            );
        }
        config_payload = json!({
            "path": binding.config.as_ref().map(|config| config.path.clone()),
            "validation": config_validation,
            "configured": config_instance
                .as_ref()
                .map(|instance| redact_plugin_config_values(&instance.values, binding.config.as_ref().map(|config| &config.validation).expect("validation must exist when config instance exists"))),
            "effective": redact_plugin_config_values(
                &effective_config,
                binding
                    .config
                    .as_ref()
                    .map(|config| &config.validation)
                    .unwrap_or(&crate::plugins::PluginConfigValidationSnapshot::default()),
            ),
        });
        resolved_payload = json!({
            "skill_id": resolved.skill_id,
            "skill_version": resolved.skill_version,
            "module_path": resolved.module_path,
            "entrypoint": resolved.entrypoint,
            "tool_id": resolved.selected_tool.as_ref().map(|tool| tool.id.clone()),
            "tool_name": resolved.selected_tool.as_ref().map(|tool| tool.name.clone()),
            "publisher": resolved.manifest.publisher,
            "manifest_version": resolved.manifest.manifest_version,
            "operator": resolved.manifest.operator,
            "current_binding_version": binding.skill_version.is_none(),
        });
        binding.discovery = build_plugin_discovery_snapshot(
            &binding,
            true,
            None,
            installed_skill.as_ref().map(|record| record.trust_decision.as_str()),
            filesystem,
            binding.config.as_ref().map(|config| &config.validation),
        );
        reasons.extend(binding.discovery.reasons.iter().cloned());
    }

    reasons.sort();
    reasons.dedup();
    remediation.sort();
    remediation.dedup();
    Ok((
        binding.clone(),
        json!({
            "ready": ready,
            "reasons": reasons,
            "remediation": remediation,
            "skill_status": skill_status_payload,
            "resolved": resolved_payload,
            "capabilities": capability_payload,
            "config": config_payload,
            "discovery": binding.discovery,
        }),
        installed_skill_payload,
    ))
}

fn build_wasm_policy(state: &AppState) -> Result<WasmPluginRunnerPolicy, Response> {
    let loaded = state.loaded_config.lock().map_err(|_| {
        runtime_status_response(tonic::Status::internal("loaded_config lock poisoned"))
    })?;
    Ok(WasmPluginRunnerPolicy {
        enabled: loaded.tool_call.wasm_runtime.enabled,
        allow_inline_modules: loaded.tool_call.wasm_runtime.allow_inline_modules,
        max_module_size_bytes: loaded.tool_call.wasm_runtime.max_module_size_bytes,
        fuel_budget: loaded.tool_call.wasm_runtime.fuel_budget,
        max_memory_bytes: loaded.tool_call.wasm_runtime.max_memory_bytes,
        max_table_elements: loaded.tool_call.wasm_runtime.max_table_elements,
        max_instances: loaded.tool_call.wasm_runtime.max_instances,
        allowed_http_hosts: loaded.tool_call.wasm_runtime.allowed_http_hosts.clone(),
        allowed_secrets: loaded.tool_call.wasm_runtime.allowed_secrets.clone(),
        allowed_storage_prefixes: loaded.tool_call.wasm_runtime.allowed_storage_prefixes.clone(),
        allowed_channels: loaded.tool_call.wasm_runtime.allowed_channels.clone(),
    })
}

fn lookup_installed_skill_record(
    skill_id: &str,
    skill_version: Option<&str>,
) -> Result<Option<InstalledSkillRecord>, Response> {
    let skills_root = resolve_skills_root()?;
    let index = load_installed_skills_index(skills_root.as_path())?;
    let resolved_version = match resolve_skill_version(&index, skill_id, skill_version) {
        Ok(version) => version,
        Err(response) if response.status() == StatusCode::NOT_FOUND => return Ok(None),
        Err(response) => return Err(response),
    };
    Ok(index
        .entries
        .into_iter()
        .find(|record| record.skill_id == skill_id && record.version == resolved_version))
}

fn normalize_plugin_config_payload(
    payload: Option<Value>,
) -> Result<Option<BTreeMap<String, Value>>, Response> {
    match payload {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(object)) => Ok(Some(
            object
                .into_iter()
                .filter(|(_, value)| !value.is_null())
                .collect::<BTreeMap<_, _>>(),
        )),
        Some(_) => Err(runtime_status_response(tonic::Status::invalid_argument(
            "plugin config payload must be a JSON object",
        ))),
    }
}

fn apply_manifest_binding_defaults(binding: &mut PluginBindingRecord, resolved: &ResolvedInstalledSkillModule) {
    if binding.tool_id.is_none() {
        binding.tool_id = resolved.manifest.operator.plugin.default_tool_id.clone();
    }
    if binding.module_path.is_none() {
        binding.module_path = resolved.manifest.operator.plugin.default_module_path.clone();
    }
    if binding.entrypoint.is_none() {
        binding.entrypoint = resolved.manifest.operator.plugin.default_entrypoint.clone();
    }
    if binding.operator.display_name.is_none() {
        binding.operator.display_name = resolved.manifest.operator.display_name.clone();
    }
    if binding.operator.tags.is_empty() {
        binding.operator.tags = resolved.manifest.operator.tags.clone();
    }
}

#[allow(clippy::result_large_err)]
fn persist_binding_config_instance(
    plugins_root: &FsPath,
    binding: &mut PluginBindingRecord,
    manifest: &palyra_skills::SkillManifest,
    skill_version: Option<&str>,
    config_payload: Option<&BTreeMap<String, Value>>,
    clear_config: bool,
    manifest_payload_sha256: Option<&str>,
    now_unix_ms: i64,
) -> Result<(), Response> {
    if clear_config {
        remove_plugin_config_instance(plugins_root, binding.plugin_id.as_str())
            .map_err(internal_console_error)?;
        binding.config = None;
        if manifest.operator.config.is_none() {
            return Ok(());
        }
    }
    if manifest.operator.config.is_none() {
        if config_payload.is_some() {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "plugin config payload was provided but the installed skill manifest does not declare operator.config",
            )));
        }
        if let Some(existing) = load_plugin_config_instance(plugins_root, binding.plugin_id.as_str())
            .map_err(internal_console_error)?
        {
            let (validation, _) =
                validate_plugin_config_instance(manifest, Some(&existing), manifest_payload_sha256);
            binding.config = Some(PluginConfigInstanceRef {
                schema_version: 1,
                path: format!("{}/config.json", binding.plugin_id),
                contract_schema_version: existing.contract_schema_version,
                manifest_payload_sha256: existing.manifest_payload_sha256,
                validation,
            });
        } else {
            binding.config = None;
        }
        return Ok(());
    }

    prepare_plugin_root(plugins_root, binding.plugin_id.as_str()).map_err(internal_console_error)?;
    let existing =
        load_plugin_config_instance(plugins_root, binding.plugin_id.as_str()).map_err(internal_console_error)?;
    let values = config_payload
        .cloned()
        .or_else(|| existing.as_ref().map(|instance| instance.values.clone()))
        .unwrap_or_default();
    let instance = PluginConfigInstance {
        schema_version: 1,
        plugin_id: binding.plugin_id.clone(),
        skill_id: binding.skill_id.clone(),
        skill_version: skill_version.map(ToOwned::to_owned),
        contract_schema_version: manifest.operator.config.as_ref().map(|contract| contract.schema_version),
        manifest_payload_sha256: manifest_payload_sha256.map(ToOwned::to_owned),
        values,
        updated_at_unix_ms: now_unix_ms,
    };
    save_plugin_config_instance(plugins_root, &instance).map_err(internal_console_error)?;
    let (validation, _) =
        validate_plugin_config_instance(manifest, Some(&instance), manifest_payload_sha256);
    binding.config = Some(PluginConfigInstanceRef {
        schema_version: 1,
        path: format!("{}/config.json", binding.plugin_id),
        contract_schema_version: instance.contract_schema_version,
        manifest_payload_sha256: instance.manifest_payload_sha256.clone(),
        validation,
    });
    Ok(())
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
