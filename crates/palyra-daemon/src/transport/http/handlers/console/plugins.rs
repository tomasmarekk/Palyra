//! Console plugin binding, installation, validation, and lifecycle handlers.
//!
//! Plugin bindings connect installed skill artifacts to runtime plugin config.
//! The install path deliberately keeps artifact security audit and binding
//! mutation in one flow so the console never enables unaudited code.

#![allow(clippy::result_large_err)]

use std::{collections::BTreeMap, fs, path::PathBuf};

use anyhow::anyhow;
use palyra_skills::{
    audit_skill_artifact_security, SkillAuditCheckStatus, SkillSecurityAuditPolicy,
    SkillSecurityAuditReport, SkillTrustStore,
};

use crate::{
    hooks::{hooks_for_plugin, load_hook_bindings_index, resolve_hooks_root},
    plugins::{
        build_plugin_capability_diff, build_plugin_discovery_snapshot, delete_plugin_binding,
        inspect_plugin_filesystem_safety, load_plugin_bindings_index, load_plugin_config_instance,
        negotiate_plugin_typed_contracts, normalize_plugin_binding_upsert, plugin_binding,
        prepare_plugin_root, redact_plugin_config_values, remove_plugin_config_instance,
        resolve_plugins_root, save_plugin_bindings_index, save_plugin_config_instance,
        set_plugin_binding_enabled, upsert_plugin_binding, validate_plugin_config_instance,
        PluginBindingRecord, PluginBindingUpsert, PluginCapabilityProfile, PluginConfigInstance,
        PluginConfigInstanceRef, PluginConfigValidationState, PluginOperatorMetadata,
    },
    wasm_plugin_runner::{
        resolve_installed_skill_module, ResolvedInstalledSkillModule, WasmPluginRunnerPolicy,
    },
    *,
};

/// Query parameters for filtering console plugin bindings.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsolePluginsListQuery {
    plugin_id: Option<String>,
    skill_id: Option<String>,
}

/// Request body for installing an artifact and/or upserting a plugin binding.
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

/// Empty request body used by plugin toggle actions.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleToggleRequest {}

/// Lists plugin bindings and their current validation checks.
///
/// # Errors
/// Returns an error response when session authorization, plugin-root
/// resolution, binding-index IO, validation, or index persistence fails.
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
        save_plugin_bindings_index(plugins_root.as_path(), &index)
            .map_err(internal_console_error)?;
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

/// Returns the current plugin health sentinel view.
///
/// # Errors
/// Returns an error response when session authorization, plugin-root
/// resolution, validation, or index persistence fails.
pub(crate) async fn console_plugins_health_handler(
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
    let mut ready = 0_usize;
    let mut unhealthy = 0_usize;
    let mut statuses = BTreeMap::<String, usize>::new();
    let mut entries = Vec::with_capacity(index.entries.len());
    for binding in &mut index.entries {
        let (updated_binding, check, _) =
            evaluate_plugin_binding(&state, plugins_root.as_path(), binding).await?;
        if *binding != updated_binding {
            *binding = updated_binding.clone();
            dirty = true;
        }
        let entry = build_plugin_health_entry(&updated_binding, &check);
        let status =
            entry.get("status").and_then(Value::as_str).unwrap_or("failed_init").to_owned();
        *statuses.entry(status).or_default() += 1;
        if entry.get("ready").and_then(Value::as_bool).unwrap_or(false) {
            ready += 1;
        } else {
            unhealthy += 1;
        }
        entries.push(entry);
    }
    if dirty {
        save_plugin_bindings_index(plugins_root.as_path(), &index)
            .map_err(internal_console_error)?;
    }

    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "plugins_root": plugins_root,
        "summary": {
            "total": entries.len(),
            "ready": ready,
            "unhealthy": unhealthy,
            "statuses": statuses,
        },
        "policy": plugin_health_policy_contract(),
        "entries": entries,
        "page": build_page_info(entries.len().max(1), entries.len(), None),
    })))
}

/// Returns one plugin binding with validation and installed-skill details.
///
/// # Errors
/// Returns an error response when session authorization, plugin-root
/// resolution, binding lookup, validation, or index persistence fails.
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
        save_plugin_bindings_index(plugins_root.as_path(), &index)
            .map_err(internal_console_error)?;
    }
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding,
        "check": check,
        "installed_skill": installed_skill,
    })))
}

/// Installs a skill artifact when supplied and upserts the plugin binding.
///
/// # Errors
/// Returns an error response when session authorization, artifact install,
/// config validation, binding normalization, plugin-root preparation, binding
/// persistence, or audit-event recording fails.
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
    let existing =
        index.entries.iter().find(|entry| entry.plugin_id == plugin_id.trim().to_ascii_lowercase());
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
    resolved = resolve_installed_skill_module(
        binding.skill_id.as_str(),
        binding.skill_version.as_deref(),
        binding.module_path.as_deref(),
        binding.entrypoint.as_deref(),
        binding.tool_id.as_deref(),
    )
    .map_err(|error| runtime_status_response(tonic::Status::invalid_argument(error.message)))?;
    let current_installed_record = installed_record
        .is_none()
        .then(|| {
            lookup_installed_skill_record(
                binding.skill_id.as_str(),
                binding.skill_version.as_deref(),
            )
        })
        .transpose()?
        .flatten();
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
    refresh_plugin_runtime_health(&state, &index)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding,
        "installed_skill": installed_skill,
        "check": check,
    })))
}

/// Re-runs validation for one plugin binding.
///
/// # Errors
/// Returns an error response when session authorization, plugin lookup,
/// validation, or index persistence fails.
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
        save_plugin_bindings_index(plugins_root.as_path(), &index)
            .map_err(internal_console_error)?;
    }
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding.clone(),
        "installed_skill": installed_skill,
        "check": check,
    })))
}

/// Enables one plugin binding.
///
/// # Errors
/// Returns an error response when session authorization, plugin lookup,
/// validation, mutation, or audit-event recording fails.
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
    refresh_plugin_runtime_health(&state, &index)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding.clone(),
        "installed_skill": installed_skill,
        "check": check,
    })))
}

/// Disables one plugin binding.
///
/// # Errors
/// Returns an error response when session authorization, plugin lookup,
/// mutation, or audit-event recording fails.
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
    refresh_plugin_runtime_health(&state, &index)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": index.schema_version,
        "binding": binding.clone(),
        "installed_skill": installed_skill,
        "check": check,
    })))
}

/// Deletes one plugin binding and any associated config instance.
///
/// # Errors
/// Returns an error response when session authorization, plugin deletion,
/// config removal, or audit-event recording fails.
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
    refresh_plugin_runtime_health(&state, &index)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "deleted": true,
        "binding": binding,
    })))
}

fn refresh_plugin_runtime_health(
    state: &AppState,
    index: &crate::plugins::PluginBindingsIndex,
) -> Result<(), Response> {
    let enabled_plugin_ids =
        index.entries.iter().filter(|entry| entry.enabled).map(|entry| entry.plugin_id.clone());
    state.runtime.try_configure_plugin_runtime_health(enabled_plugin_ids).map_err(|error| {
        internal_console_error(anyhow!("failed to activate plugin runtime health: {error}"))
    })
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

    let installed_skill =
        lookup_installed_skill_record(binding.skill_id.as_str(), binding.skill_version.as_deref())?;
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
    let installed_skill_payload =
        installed_skill.as_ref().and_then(|record| serde_json::to_value(record).ok());
    let manifest_payload_sha256 =
        installed_skill.as_ref().map(|record| record.payload_sha256.as_str());
    let config_instance = load_plugin_config_instance(plugins_root, binding.plugin_id.as_str())
        .map_err(internal_console_error)?;

    let mut skill_status_payload = Value::Null;
    let mut capability_payload =
        serde_json::to_value(&binding.capability_diff).unwrap_or(Value::Null);
    let mut contracts_payload = build_contracts_payload(&binding.typed_contracts, Value::Null);
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
        binding.capability_diff = build_plugin_capability_diff(
            &resolved.manifest,
            &binding.capability_profile,
            &wasm_policy,
        );
        if !binding.capability_diff.valid {
            ready = false;
            reasons.push("plugin capability profile has binding/policy drift".to_owned());
            remediation.push(
                "Align binding grants with the signed manifest and current wasm runtime policy."
                    .to_owned(),
            );
        }
        capability_payload = serde_json::to_value(&binding.capability_diff).unwrap_or(Value::Null);

        binding.typed_contracts =
            negotiate_plugin_typed_contracts(&resolved.manifest, &binding.capability_profile);
        if binding.typed_contracts.mode == palyra_plugins_runtime::TypedPluginContractMode::Typed
            && !binding.typed_contracts.ready
        {
            ready = false;
            reasons.push("typed plugin contract negotiation failed".to_owned());
            remediation.push(
                "Align declared typed contracts with a supported daemon adapter version and compatible capability classes."
                    .to_owned(),
            );
        }
        let host_adapters = collect_typed_contract_host_adapters(state).await?;
        contracts_payload = build_contracts_payload(&binding.typed_contracts, host_adapters);

        let (config_validation, effective_config) = validate_plugin_config_instance(
            &resolved.manifest,
            config_instance.as_ref(),
            manifest_payload_sha256,
        );
        binding.config = if resolved.manifest.operator.config.is_some() || config_instance.is_some()
        {
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
            "contracts": contracts_payload,
            "config": config_payload,
            "discovery": binding.discovery,
        }),
        installed_skill_payload,
    ))
}

fn build_plugin_health_entry(binding: &PluginBindingRecord, check: &Value) -> Value {
    let object = check.as_object();
    let ready = json_bool(object, "ready");
    let reasons = json_string_array(object, "reasons").unwrap_or_default();
    let remediation = json_string_array(object, "remediation").unwrap_or_default();
    let discovery_state = json_path_string(object, &["discovery", "state"]);
    let config_state = json_path_string(object, &["config", "validation", "state"]);
    let contracts_mode = json_path_string(object, &["contracts", "mode"]);
    let contracts_ready = json_path_bool_opt(object, &["contracts", "ready"]);
    let current_binding_version =
        json_path_bool_opt(object, &["resolved", "current_binding_version"]);
    let status = classify_plugin_health_status(
        binding.enabled,
        ready,
        reasons.as_slice(),
        discovery_state.as_deref(),
        current_binding_version,
    );
    json!({
        "plugin_id": binding.plugin_id,
        "skill_id": binding.skill_id,
        "skill_version": binding.skill_version.as_deref().unwrap_or("current"),
        "enabled": binding.enabled,
        "ready": ready,
        "status": status,
        "discovery": discovery_state,
        "config": config_state,
        "contracts_mode": contracts_mode,
        "contracts_ready": contracts_ready,
        "reasons": reasons,
        "remediation": remediation,
        "sentinel": {
            "filesystem_checked": object.and_then(|value| value.get("discovery")).is_some(),
            "config_validated": object.and_then(|value| value.get("config")).is_some(),
            "capability_diff_checked": object.and_then(|value| value.get("capabilities")).is_some(),
            "typed_contracts_checked": object.and_then(|value| value.get("contracts")).is_some(),
            "hook_timeout_status": if status == "hook_timeout" { "tripped" } else { "clear" },
        },
        "hot_reload": plugin_hot_reload_decision(status),
    })
}

fn classify_plugin_health_status(
    enabled: bool,
    ready: bool,
    reasons: &[String],
    discovery_state: Option<&str>,
    current_binding_version: Option<bool>,
) -> &'static str {
    if !enabled {
        return "disabled";
    }
    let reason_text = reasons.join("\n").to_ascii_lowercase();
    if reason_text.contains("quarantined") {
        return "quarantined";
    }
    if reason_text.contains("timeout") {
        return "hook_timeout";
    }
    if reason_text.contains("capability") || reason_text.contains("binding/policy drift") {
        return "capability_denied";
    }
    if current_binding_version == Some(false)
        || discovery_state
            .map(|state| state.to_ascii_lowercase().contains("stale"))
            .unwrap_or(false)
    {
        return "stale_version";
    }
    if ready {
        return "loaded";
    }
    "failed_init"
}

fn plugin_hot_reload_decision(status: &str) -> Value {
    let reload_mode = match status {
        "loaded" => "eligible",
        "disabled" => "blocked_until_enabled",
        "stale_version" => "reload_after_rebind",
        "quarantined" => "blocked_until_unquarantined",
        _ => "blocked_until_healthy",
    };
    json!({
        "reload_mode": reload_mode,
        "in_flight_run_policy": "snapshot_current_plugin_until_run_end",
        "lifecycle_hook_policy": "defer_reload_until_no_active_invocation",
        "breaking_capability_drift": "fail_closed",
    })
}

fn plugin_health_policy_contract() -> Value {
    json!({
        "status_vocabulary": [
            "loaded",
            "disabled",
            "quarantined",
            "failed_init",
            "hook_timeout",
            "capability_denied",
            "stale_version",
        ],
        "sentinel_sources": [
            "binding_index",
            "filesystem_safety",
            "installed_skill_status",
            "capability_diff",
            "typed_contract_negotiation",
            "config_validation",
        ],
        "hot_reload": {
            "in_flight_run_policy": "snapshot_current_plugin_until_run_end",
            "plugin_lifecycle_hook": "defer_reload_until_no_active_invocation",
            "delivery_adapter": "fail_closed_on_breaking_capability_drift",
        },
        "install_race": {
            "binding_write": "atomic_index_rewrite",
            "plugin_root": "prepare_before_enable",
            "config_instance": "validate_against_manifest_payload_sha256",
        },
    })
}

fn json_bool(object: Option<&serde_json::Map<String, Value>>, key: &str) -> bool {
    object.and_then(|value| value.get(key)).and_then(Value::as_bool).unwrap_or(false)
}

fn json_string_array(
    object: Option<&serde_json::Map<String, Value>>,
    key: &str,
) -> Option<Vec<String>> {
    object
        .and_then(|value| value.get(key))
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).map(str::to_owned).collect::<Vec<_>>())
}

fn json_path_string(
    object: Option<&serde_json::Map<String, Value>>,
    path: &[&str],
) -> Option<String> {
    json_path_value(object, path).and_then(Value::as_str).map(str::to_owned)
}

fn json_path_bool_opt(
    object: Option<&serde_json::Map<String, Value>>,
    path: &[&str],
) -> Option<bool> {
    json_path_value(object, path).and_then(Value::as_bool)
}

fn json_path_value<'a>(
    object: Option<&'a serde_json::Map<String, Value>>,
    path: &[&str],
) -> Option<&'a Value> {
    let mut current = object?.get(path.first().copied()?)?;
    for segment in &path[1..] {
        current = current.as_object()?.get(*segment)?;
    }
    Some(current)
}

fn build_contracts_payload(
    report: &palyra_plugins_runtime::TypedPluginContractNegotiationReport,
    host_adapters: Value,
) -> Value {
    json!({
        "mode": report.mode,
        "ready": report.ready,
        "entries": report.entries,
        "host_adapters": host_adapters,
    })
}

async fn collect_typed_contract_host_adapters(state: &AppState) -> Result<Value, Response> {
    let memory_embeddings =
        state.runtime.memory_embeddings_status().await.map_err(runtime_status_response)?;
    Ok(json!({
        "memory_provider": {
            "adapter": "journal.memory_embedding_provider",
            "mode": memory_embeddings.mode,
            "posture": memory_embeddings.posture,
            "target_model_id": memory_embeddings.target_model_id,
            "target_dims": memory_embeddings.target_dims,
            "backfill_strategy": memory_embeddings.backfill_strategy,
        },
        "context_engine": {
            "adapter": "application.context_engine",
            "strategies": [
                "noop",
                "checkpoint_aware",
                "summarizing",
                "cost_aware",
                "provider_aware",
            ],
            "stable_extension_boundary": [
                "strategy_selection",
                "segment_explain",
                "drop_reason",
            ],
        },
        "routing_strategy": {
            "adapter": "usage_governance.routing",
            "default_mode": state.runtime.config.smart_routing.default_mode,
            "auxiliary_routing_enabled": state.runtime.config.smart_routing.auxiliary_routing_enabled,
            "stable_extension_boundary": [
                "candidate_selection",
                "budget_gate",
                "approval_gate",
            ],
        },
        "run_lifecycle_hook": {
            "adapter": "runtime.run_lifecycle_hook",
            "phases": [
                "before_run",
                "before_tool",
                "after_tool",
                "before_delivery",
                "after_run",
            ],
            "capability_posture": "minimal",
            "terminal_decision_rules": "daemon_resolved",
        },
        "compaction_provider": {
            "adapter": "application.compaction_provider",
            "stable_extension_boundary": [
                "summary_candidate",
                "token_budget",
                "redaction_preservation",
            ],
        },
        "diagnostics_provider": {
            "adapter": "operator.diagnostics_provider",
            "redaction_required": true,
            "stable_extension_boundary": [
                "snapshot_projection",
                "safe_identifier_fields",
            ],
        },
        "policy_signal_provider": {
            "adapter": "policy.signal_provider",
            "decision_authority": "signals_only",
            "stable_extension_boundary": [
                "risk_score",
                "policy_label",
                "explainable_signal",
            ],
        },
        "channel_binding_provider": {
            "adapter": "channels.binding_provider",
            "operations": [
                "discover",
                "validate",
                "repair_hint",
                "explain",
            ],
            "durable_write_authority": "daemon_runtime",
        },
        "delivery_adapter": {
            "adapter": "channels.delivery_adapter",
            "receipt_states": [
                "ack",
                "nack",
                "unknown",
            ],
            "decision_authority": "delivery_only",
        },
        "scheduler_task_provider": {
            "adapter": "runtime.scheduler_task_provider",
            "wake_gate_authority": "daemon_runtime",
            "default_posture": "disabled_until_bound",
        },
        "model_auth_provider": {
            "adapter": "auth.model_auth_provider",
            "secret_material_posture": "credential_handles_only",
            "default_posture": "disabled_until_bound",
        },
        "connector_adapter": {
            "adapter": "connectors.adapter_sdk",
            "operations": [
                "inbound",
                "outbound",
                "binding",
                "rate_limit",
                "error_mapping",
            ],
            "binding_authority": "conversation_binding_record",
        },
    }))
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
            object.into_iter().filter(|(_, value)| !value.is_null()).collect::<BTreeMap<_, _>>(),
        )),
        Some(_) => Err(runtime_status_response(tonic::Status::invalid_argument(
            "plugin config payload must be a JSON object",
        ))),
    }
}

fn apply_manifest_binding_defaults(
    binding: &mut PluginBindingRecord,
    resolved: &ResolvedInstalledSkillModule,
) {
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
        binding.operator.display_name.clone_from(&resolved.manifest.operator.display_name);
    }
    if binding.operator.tags.is_empty() {
        binding.operator.tags.clone_from(&resolved.manifest.operator.tags);
    }
}

#[allow(clippy::too_many_arguments)]
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
        if let Some(existing) =
            load_plugin_config_instance(plugins_root, binding.plugin_id.as_str())
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

    prepare_plugin_root(plugins_root, binding.plugin_id.as_str())
        .map_err(internal_console_error)?;
    let existing = load_plugin_config_instance(plugins_root, binding.plugin_id.as_str())
        .map_err(internal_console_error)?;
    let values = config_payload
        .cloned()
        .or_else(|| existing.as_ref().map(|instance| instance.values.clone()))
        .unwrap_or_default();
    let instance = PluginConfigInstance {
        schema_version: 1,
        plugin_id: binding.plugin_id.clone(),
        skill_id: binding.skill_id.clone(),
        skill_version: skill_version.map(ToOwned::to_owned),
        contract_schema_version: manifest
            .operator
            .config
            .as_ref()
            .map(|contract| contract.schema_version),
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
    if allow_untrusted {
        tracing::warn!(
            artifact_path = %artifact_path.display(),
            "plugin install-or-bind allow_untrusted override does not bypass security audit"
        );
    }
    let audit_report = audit_skill_artifact_security(
        artifact_bytes.as_slice(),
        &mut trust_store,
        allow_tofu,
        &SkillSecurityAuditPolicy::default(),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill artifact security audit failed: {error}"
        )))
    })?;
    if audit_report.should_quarantine {
        let reason = if audit_report.quarantine_reasons.is_empty() {
            "skill artifact security audit requested quarantine".to_owned()
        } else {
            audit_report.quarantine_reasons.join(" | ")
        };
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill artifact security audit failed: {reason}"
        ))));
    }
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
    let previous_current = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.current && entry.version != version)
        .cloned();
    index.entries.retain(|entry| !(entry.skill_id == skill_id && entry.version == version));
    for entry in &mut index.entries {
        if entry.skill_id == skill_id {
            entry.current = false;
        }
    }
    let installed_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let record = InstalledSkillRecord {
        skill_id: skill_id.clone(),
        version: version.clone(),
        publisher: inspection.manifest.publisher.clone(),
        current: true,
        installed_at_unix_ms,
        artifact_sha256: sha256_hex(artifact_bytes.as_slice()),
        payload_sha256: audit_report.payload_sha256.clone(),
        signature_key_id: inspection.signature.key_id.clone(),
        trust_decision: trust_decision_label(audit_report.trust_decision),
        source: InstalledSkillSource {
            kind: "managed_artifact".to_owned(),
            reference: artifact_path.to_string_lossy().into_owned(),
        },
        missing_secrets: Vec::new(),
        security_scan: installed_skill_security_snapshot(&audit_report),
        rollback_snapshot: previous_current
            .as_ref()
            .map(|entry| installed_skill_rollback_snapshot(entry, installed_at_unix_ms)),
    };
    index.entries.push(record.clone());
    save_installed_skills_index(skills_root.as_path(), &index)?;
    Ok(record)
}

fn installed_skill_security_snapshot(
    report: &SkillSecurityAuditReport,
) -> InstalledSkillSecuritySnapshot {
    InstalledSkillSecuritySnapshot {
        schema_version: 1,
        accepted: report.accepted,
        passed: report.passed,
        should_quarantine: report.should_quarantine,
        generated_at_unix_ms: report.generated_at_unix_ms,
        payload_sha256: report.payload_sha256.clone(),
        trust_decision: trust_decision_label(report.trust_decision),
        check_count: report.checks.len(),
        failed_checks: audit_check_ids_by_status(report, SkillAuditCheckStatus::Fail),
        warning_checks: audit_check_ids_by_status(report, SkillAuditCheckStatus::Warn),
        quarantine_reasons: report.quarantine_reasons.clone(),
        policy: report.policy.clone(),
    }
}

fn audit_check_ids_by_status(
    report: &SkillSecurityAuditReport,
    status: SkillAuditCheckStatus,
) -> Vec<String> {
    report
        .checks
        .iter()
        .filter(|check| check.status == status)
        .map(|check| check.check_id.clone())
        .collect()
}

fn installed_skill_rollback_snapshot(
    previous: &InstalledSkillRecord,
    captured_at_unix_ms: i64,
) -> InstalledSkillRollbackSnapshot {
    InstalledSkillRollbackSnapshot {
        schema_version: 1,
        previous_version: previous.version.clone(),
        previous_artifact_sha256: previous.artifact_sha256.clone(),
        previous_payload_sha256: previous.payload_sha256.clone(),
        captured_at_unix_ms,
    }
}

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

#[cfg(test)]
mod tests {
    use super::classify_plugin_health_status;

    #[test]
    fn plugin_health_status_disabled_wins() {
        let reasons = vec!["typed plugin contract negotiation failed".to_owned()];

        let status = classify_plugin_health_status(false, false, reasons.as_slice(), None, None);

        assert_eq!(status, "disabled");
    }

    #[test]
    fn plugin_health_status_detects_capability_denial() {
        let reasons = vec!["plugin capability profile has binding/policy drift".to_owned()];

        let status = classify_plugin_health_status(true, false, reasons.as_slice(), None, None);

        assert_eq!(status, "capability_denied");
    }

    #[test]
    fn plugin_health_status_detects_stale_version() {
        let status = classify_plugin_health_status(true, false, &[], Some("valid"), Some(false));

        assert_eq!(status, "stale_version");
    }
}
