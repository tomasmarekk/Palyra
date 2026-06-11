//! Console HTTP handlers for hook bindings (`/console/v1/hooks*`): list, get,
//! bind, check, enable/disable, delete, and fire.
//!
//! Bindings live in a file-backed index under the hooks root and connect a
//! hook id and event to a plugin binding. Firing a hook does not execute the
//! plugin directly; it fans out to matching hook-triggered routines via
//! [`super::routines::dispatch_hook_event_routines`]. Every read response
//! carries a readiness `check` derived from the referenced plugin binding.
//!
//! Response JSON shapes are part of the `/console/v1` wire contract consumed
//! by `apps/web`; field names, status codes, and error strings must stay
//! byte-identical.

use crate::{
    hooks::{
        delete_hook_binding, hook_binding, load_hook_bindings_index, normalize_hook_binding_upsert,
        normalize_hook_event, resolve_hooks_root, save_hook_bindings_index,
        set_hook_binding_enabled, upsert_hook_binding, HookBindingRecord, HookBindingUpsert,
        HookOperatorMetadata,
    },
    plugins::{load_plugin_bindings_index, plugin_binding, resolve_plugins_root},
    *,
};

/// Query filters for `GET /console/v1/hooks`; ids match case-insensitively
/// and the event filter is normalized before comparison.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleHooksListQuery {
    hook_id: Option<String>,
    plugin_id: Option<String>,
    event: Option<String>,
}

/// Body for `POST /console/v1/hooks/bind`; the referenced plugin binding must
/// already exist. Unknown fields are rejected.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleHookBindRequest {
    hook_id: String,
    event: String,
    plugin_id: String,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    operator: Option<HookOperatorMetadata>,
}

/// Deliberately empty body for hook enable/disable: keeps the mutations as
/// JSON POSTs with strict bodies (`{}`) so the CSRF and content-type checks
/// of the console surface apply.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleHookToggleRequest {}

/// Body for `POST /console/v1/hooks/{hook_id}/fire`; the event defaults to
/// the binding's configured event and the payload to `{}`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleHookFireRequest {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    payload: Option<Value>,
    #[serde(default)]
    dedupe_key: Option<String>,
}

/// Lists hook bindings with optional hook/plugin/event filters
/// (`GET /console/v1/hooks`).
///
/// # Errors
/// Returns an unauthorized response when console authorization fails, or an
/// internal error response when the bindings index cannot be loaded or the
/// event filter cannot be normalized.
pub(crate) async fn console_hooks_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleHooksListQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let mut index =
        load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    if let Some(hook_id) = query.hook_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        // Stored ids are lowercase; lowercasing the filter once keeps the
        // comparison out of the retain loop.
        let hook_id = hook_id.to_ascii_lowercase();
        index.entries.retain(|entry| entry.hook_id == hook_id);
    }
    if let Some(plugin_id) =
        query.plugin_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        let plugin_id = plugin_id.to_ascii_lowercase();
        index.entries.retain(|entry| entry.plugin_id == plugin_id);
    }
    if let Some(event) = query.event.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = normalize_hook_event(event).map_err(internal_console_error)?;
        index.entries.retain(|entry| entry.event == normalized);
    }

    let mut entries = Vec::with_capacity(index.entries.len());
    for binding in index.entries {
        entries.push(json!({
            "binding": binding.clone(),
            "check": build_hook_binding_check(binding).await,
        }));
    }
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "hooks_root": hooks_root,
        "count": entries.len(),
        "entries": entries,
        // The hooks index is small and unpaginated; the page envelope is
        // synthesized for consistency with other console list endpoints.
        "page": build_page_info(entries.len().max(1), entries.len(), None),
    })))
}

/// Returns one hook binding with its readiness check
/// (`GET /console/v1/hooks/{hook_id}`).
///
/// # Errors
/// Returns not-found for unknown hook ids, or an internal error response when
/// the bindings index cannot be loaded.
pub(crate) async fn console_hook_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(hook_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let index = load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    let binding = hook_binding(&index, hook_id.as_str()).map_err(not_found_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding.clone(),
        "check": build_hook_binding_check(binding).await,
    })))
}

/// Creates or updates a hook binding after verifying the referenced plugin
/// binding exists (`POST /console/v1/hooks/bind`).
///
/// Operator ownership defaults to the session principal when not supplied,
/// and `updated_by` always records the session principal.
///
/// # Errors
/// Returns not-found for unknown plugin ids, or an internal error response
/// for index load/normalize/save failures.
pub(crate) async fn console_hooks_bind_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleHookBindRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let plugins_root = resolve_plugins_root().map_err(internal_console_error)?;
    let plugins_index =
        load_plugin_bindings_index(plugins_root.as_path()).map_err(internal_console_error)?;
    let _plugin = plugin_binding(&plugins_index, payload.plugin_id.as_str())
        .map_err(not_found_console_error)?;

    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let mut index =
        load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    let existing = index
        .entries
        .iter()
        .find(|entry| entry.hook_id == payload.hook_id.trim().to_ascii_lowercase());
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let binding = normalize_hook_binding_upsert(
        HookBindingUpsert {
            hook_id: payload.hook_id,
            event: payload.event,
            plugin_id: payload.plugin_id,
            enabled: payload.enabled.unwrap_or(true),
            operator: HookOperatorMetadata {
                display_name: payload
                    .operator
                    .as_ref()
                    .and_then(|operator| operator.display_name.clone()),
                notes: payload.operator.as_ref().and_then(|operator| operator.notes.clone()),
                owner_principal: payload
                    .operator
                    .as_ref()
                    .and_then(|operator| operator.owner_principal.clone())
                    .or_else(|| Some(session.context.principal.clone())),
                updated_by: Some(session.context.principal.clone()),
            },
        },
        now,
        existing,
    )
    .map_err(internal_console_error)?;
    let binding = upsert_hook_binding(&mut index, binding);
    save_hook_bindings_index(hooks_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding.clone(),
        "check": build_hook_binding_check(binding).await,
    })))
}

/// Re-evaluates a hook binding's readiness without mutating it
/// (`GET /console/v1/hooks/{hook_id}/check`).
///
/// # Errors
/// Returns not-found for unknown hook ids, or an internal error response when
/// the bindings index cannot be loaded.
pub(crate) async fn console_hook_check_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(hook_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let index = load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    let binding = hook_binding(&index, hook_id.as_str()).map_err(not_found_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding.clone(),
        "check": build_hook_binding_check(binding).await,
    })))
}

/// Enables a hook binding (`POST /console/v1/hooks/{hook_id}/enable`).
///
/// # Errors
/// Returns not-found for unknown hook ids, or an internal error response for
/// index load/save failures.
pub(crate) async fn console_hook_enable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(hook_id): Path<String>,
    Json(_payload): Json<ConsoleHookToggleRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let mut index =
        load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    let binding = set_hook_binding_enabled(
        &mut index,
        hook_id.as_str(),
        true,
        Some(session.context.principal.as_str()),
    )
    .map_err(not_found_console_error)?;
    save_hook_bindings_index(hooks_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding.clone(),
        "check": build_hook_binding_check(binding).await,
    })))
}

/// Disables a hook binding (`POST /console/v1/hooks/{hook_id}/disable`).
///
/// # Errors
/// Returns not-found for unknown hook ids, or an internal error response for
/// index load/save failures.
pub(crate) async fn console_hook_disable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(hook_id): Path<String>,
    Json(_payload): Json<ConsoleHookToggleRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let mut index =
        load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    let binding = set_hook_binding_enabled(
        &mut index,
        hook_id.as_str(),
        false,
        Some(session.context.principal.as_str()),
    )
    .map_err(not_found_console_error)?;
    save_hook_bindings_index(hooks_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding.clone(),
        "check": build_hook_binding_check(binding).await,
    })))
}

/// Deletes a hook binding and returns the removed record
/// (`POST /console/v1/hooks/{hook_id}/delete`).
///
/// # Errors
/// Returns not-found for unknown hook ids, or an internal error response for
/// index load/save failures.
pub(crate) async fn console_hook_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(hook_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let mut index =
        load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    let binding =
        delete_hook_binding(&mut index, hook_id.as_str()).map_err(not_found_console_error)?;
    save_hook_bindings_index(hooks_root.as_path(), &index).map_err(internal_console_error)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "deleted": true,
        "binding": binding,
    })))
}

/// Fires a hook event, dispatching every matching hook-triggered routine of
/// the session principal (`POST /console/v1/hooks/{hook_id}/fire`).
///
/// Bindings owned by another principal cannot be fired; unowned bindings
/// default to the session principal.
///
/// # Errors
/// Returns not-found for unknown hook ids, permission-denied for foreign
/// owners, or a mapped error response from event normalization or routine
/// dispatch.
pub(crate) async fn console_hook_fire_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(hook_id): Path<String>,
    Json(payload): Json<ConsoleHookFireRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let hooks_root = resolve_hooks_root().map_err(internal_console_error)?;
    let index = load_hook_bindings_index(hooks_root.as_path()).map_err(internal_console_error)?;
    let binding = hook_binding(&index, hook_id.as_str()).map_err(not_found_console_error)?;
    let owner_principal = binding
        .operator
        .owner_principal
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(session.context.principal.as_str());
    if owner_principal != session.context.principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "hook owner principal does not match authenticated session principal",
        )));
    }
    let event = payload
        .event
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(binding.event.as_str());
    let normalized_event = normalize_hook_event(event).map_err(internal_console_error)?;
    let dispatches = super::routines::dispatch_hook_event_routines(
        &state,
        session.context.principal.as_str(),
        binding.hook_id.as_str(),
        normalized_event,
        json!({
            "hook_id": binding.hook_id,
            "event": normalized_event,
            "plugin_id": binding.plugin_id,
            "payload": payload.payload.unwrap_or_else(|| json!({})),
        }),
        payload.dedupe_key,
    )
    .await?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "binding": binding,
        "dispatches": dispatches,
    })))
}

// Computes the readiness payload for a binding: not ready when the binding or
// its referenced plugin is disabled, missing, or unresolvable. Failure
// reasons are sanitized before they reach the response.
async fn build_hook_binding_check(binding: HookBindingRecord) -> Value {
    let mut ready = binding.enabled;
    let mut reasons = Vec::<String>::new();
    if !binding.enabled {
        reasons.push("hook binding is disabled".to_owned());
    }
    let plugins_root = match resolve_plugins_root() {
        Ok(path) => path,
        Err(error) => {
            ready = false;
            reasons.push(sanitize_http_error_message(error.to_string().as_str()));
            return json!({
                "ready": ready,
                "reasons": reasons,
                "event_supported": false,
                "plugin": Value::Null,
            });
        }
    };
    let plugins_index = match load_plugin_bindings_index(plugins_root.as_path()) {
        Ok(index) => index,
        Err(error) => {
            ready = false;
            reasons.push(sanitize_http_error_message(error.to_string().as_str()));
            return json!({
                "ready": ready,
                "reasons": reasons,
                "event_supported": false,
                "plugin": Value::Null,
            });
        }
    };
    let plugin = match plugin_binding(&plugins_index, binding.plugin_id.as_str()) {
        Ok(binding) => binding,
        Err(error) => {
            ready = false;
            reasons.push(sanitize_http_error_message(error.to_string().as_str()));
            return json!({
                "ready": ready,
                "reasons": reasons,
                "event_supported": false,
                "plugin": Value::Null,
            });
        }
    };
    if !plugin.enabled {
        ready = false;
        reasons.push("referenced plugin binding is disabled".to_owned());
    }
    reasons.sort();
    reasons.dedup();
    json!({
        "ready": ready,
        "reasons": reasons,
        "event_supported": true,
        "plugin": {
            "plugin_id": plugin.plugin_id,
            "enabled": plugin.enabled,
            "skill_id": plugin.skill_id,
            "skill_version": plugin.skill_version,
        },
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
