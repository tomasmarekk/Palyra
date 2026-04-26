use crate::*;

pub(crate) async fn console_config_inspect_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ConfigInspectRequest>,
) -> Result<Json<control_plane::ConfigDocumentSnapshot>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let (mut document, migration, source_path) =
        load_console_config_snapshot(payload.path.as_deref(), true)?;
    if !payload.show_secrets {
        redact_secret_config_values(&mut document);
    }
    let rendered = serialize_document_pretty(&document).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize config document: {error}"
        )))
    })?;
    Ok(Json(control_plane::ConfigDocumentSnapshot {
        contract: contract_descriptor(),
        source_path: source_path.clone(),
        config_version: migration.target_version,
        migrated_from_version: migration.migrated.then_some(migration.source_version),
        redacted: !payload.show_secrets,
        document_toml: rendered,
        backups: config_backup_records(Some(source_path.as_str()), payload.backups.max(1), false)?,
    }))
}

pub(crate) async fn console_config_validate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ConfigValidateRequest>,
) -> Result<Json<control_plane::ConfigValidationEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let (document, migration, source_path) =
        load_console_config_snapshot(payload.path.as_deref(), false)?;
    validate_daemon_compatible_document(&document)?;
    Ok(Json(control_plane::ConfigValidationEnvelope {
        contract: contract_descriptor(),
        source_path,
        valid: true,
        config_version: migration.target_version,
        migrated_from_version: migration.migrated.then_some(migration.source_version),
    }))
}

pub(crate) async fn console_config_mutate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ConfigMutationRequest>,
) -> Result<Json<control_plane::ConfigMutationEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let path = resolve_console_config_path(payload.path.as_deref(), false)?.ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "config path could not be resolved",
        ))
    })?;
    let path_ref = FsPath::new(path.as_str());
    let (mut document, migration) = load_console_document_for_mutation(path_ref)?;
    let operation = if let Some(value) = payload.value.as_deref() {
        let literal = parse_toml_value_literal(value).map_err(|error| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "config value must be a valid TOML literal: {error}"
            )))
        })?;
        set_value_at_path(&mut document, payload.key.as_str(), literal).map_err(|error| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "invalid config key path: {error}"
            )))
        })?;
        "set"
    } else {
        let removed =
            unset_value_at_path(&mut document, payload.key.as_str()).map_err(|error| {
                runtime_status_response(tonic::Status::invalid_argument(format!(
                    "invalid config key path: {error}"
                )))
            })?;
        if !removed {
            return Err(runtime_status_response(tonic::Status::not_found(format!(
                "config key not found: {}",
                payload.key
            ))));
        }
        "unset"
    };
    validate_daemon_compatible_document(&document)?;
    write_document_with_backups(path_ref, &document, payload.backups).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist config {}: {error}",
            path_ref.display()
        )))
    })?;
    Ok(Json(control_plane::ConfigMutationEnvelope {
        contract: contract_descriptor(),
        operation: operation.to_owned(),
        source_path: path,
        backups_retained: payload.backups,
        config_version: migration.target_version,
        migrated_from_version: migration.migrated.then_some(migration.source_version),
        changed_key: Some(payload.key),
    }))
}

pub(crate) async fn console_config_migrate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ConfigInspectRequest>,
) -> Result<Json<control_plane::ConfigMutationEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let path = resolve_console_config_path(payload.path.as_deref(), true)?.ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "no daemon config file found to migrate",
        ))
    })?;
    let path_ref = FsPath::new(path.as_str());
    let (document, migration) = load_console_document_from_existing_path(path_ref)?;
    validate_daemon_compatible_document(&document)?;
    if migration.migrated {
        write_document_with_backups(path_ref, &document, payload.backups).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to persist migrated config {}: {error}",
                path_ref.display()
            )))
        })?;
    }
    Ok(Json(control_plane::ConfigMutationEnvelope {
        contract: contract_descriptor(),
        operation: "migrate".to_owned(),
        source_path: path,
        backups_retained: payload.backups,
        config_version: migration.target_version,
        migrated_from_version: migration.migrated.then_some(migration.source_version),
        changed_key: None,
    }))
}

pub(crate) async fn console_config_recover_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ConfigRecoverRequest>,
) -> Result<Json<control_plane::ConfigMutationEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let path = resolve_console_config_path(payload.path.as_deref(), false)?.ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "config path could not be resolved",
        ))
    })?;
    let path_ref = FsPath::new(path.as_str());
    let candidate_backup = backup_path(path_ref, payload.backup);
    let (backup_document, _) =
        load_console_document_from_existing_path(candidate_backup.as_path())?;
    validate_daemon_compatible_document(&backup_document)?;
    recover_config_from_backup(path_ref, payload.backup, payload.backups).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to recover config {} from backup {}: {error}",
            path_ref.display(),
            payload.backup
        )))
    })?;
    let (document, migration) = load_console_document_from_existing_path(path_ref)?;
    validate_daemon_compatible_document(&document)?;
    Ok(Json(control_plane::ConfigMutationEnvelope {
        contract: contract_descriptor(),
        operation: "recover".to_owned(),
        source_path: path,
        backups_retained: payload.backups,
        config_version: migration.target_version,
        migrated_from_version: migration.migrated.then_some(migration.source_version),
        changed_key: None,
    }))
}

pub(crate) async fn console_config_reload_plan_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ConfigReloadPlanRequest>,
) -> Result<Json<control_plane::ConfigReloadPlanEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(plan_config_reload_for_context(&state, &session.context, payload).await?))
}

pub(crate) async fn plan_config_reload_for_context(
    state: &AppState,
    context: &gateway::RequestContext,
    payload: control_plane::ConfigReloadPlanRequest,
) -> Result<control_plane::ConfigReloadPlanEnvelope, Response> {
    let current = state.loaded_config.lock().unwrap_or_else(|error| error.into_inner()).clone();
    let source_path = validate_requested_reload_path(payload.path.as_deref(), &current)
        .map_err(|response| *response)?;
    let candidate = crate::config::load_config().map_err(|error| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "failed to load candidate config for reload planning: {error}"
        )))
    })?;
    let plan = build_reload_plan(&current, &candidate, source_path, estimate_active_runs(state));
    state.reload_state.lock().unwrap_or_else(|error| error.into_inner()).latest_plan =
        Some(plan.clone());
    state
        .runtime
        .record_console_event(
            context,
            "reload_plan_created",
            json!({
                "plan_id": plan.plan_id,
                "source_path": plan.source_path,
                "summary": plan.summary,
                "actor": {
                    "principal": context.principal.as_str(),
                    "device_id": context.device_id.as_str(),
                    "channel": context.channel.as_deref(),
                },
                "redacted_diff": plan.steps.iter().map(|step| json!({
                    "component": step.component,
                    "config_path": step.config_path,
                    "category": step.category,
                    "sensitivity": step.sensitivity,
                    "reloadability": step.reloadability,
                    "impact": step.impact,
                    "redacted_diff": step.redacted_diff,
                })).collect::<Vec<_>>(),
                "steps": plan.steps,
            }),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(plan)
}

pub(crate) async fn console_config_reload_apply_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ConfigReloadApplyRequest>,
) -> Result<Json<control_plane::ConfigReloadApplyEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    Ok(Json(apply_config_reload_for_context(&state, &session.context, payload).await?))
}

pub(crate) async fn apply_config_reload_for_context(
    state: &AppState,
    context: &gateway::RequestContext,
    payload: control_plane::ConfigReloadApplyRequest,
) -> Result<control_plane::ConfigReloadApplyEnvelope, Response> {
    let current = state.loaded_config.lock().unwrap_or_else(|error| error.into_inner()).clone();
    let source_path = validate_requested_reload_path(payload.path.as_deref(), &current)
        .map_err(|response| *response)?;
    let candidate = crate::config::load_config().map_err(|error| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "failed to load candidate config for reload apply: {error}"
        )))
    })?;
    let plan = build_reload_plan(&current, &candidate, source_path, estimate_active_runs(state));
    validate_reload_plan_reference(state, payload.plan_id.as_deref(), payload.force)?;

    let hot_safe_steps =
        plan.steps.iter().filter(|step| step.category == "hot_safe").cloned().collect::<Vec<_>>();
    let skipped_steps =
        plan.steps.iter().filter(|step| step.category != "hot_safe").cloned().collect::<Vec<_>>();

    let mut applied_steps = Vec::new();

    let (outcome, message) = if payload.dry_run {
        let message = if plan.steps.is_empty() {
            "reload dry-run found no changes".to_owned()
        } else {
            "reload dry-run generated a plan without applying any runtime changes".to_owned()
        };
        ("dry_run".to_owned(), message)
    } else if hot_safe_steps.is_empty() {
        let message = if skipped_steps.is_empty() {
            "reload had nothing to apply".to_owned()
        } else {
            "reload plan contains no hot-safe steps; review restart-required or manual-review actions"
                .to_owned()
        };
        ("rejected".to_owned(), message)
    } else {
        let mut next_loaded = current.clone();
        if current.memory != candidate.memory {
            state.runtime.configure_memory(memory_runtime_config_from_loaded(&candidate));
            next_loaded.memory = candidate.memory.clone();
            if let Some(step) = plan.steps.iter().find(|step| step.config_path == "memory") {
                applied_steps.push(step.clone());
            }
        }
        let next_generation = state
            .configured_secrets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot_generation
            .saturating_add(1);
        let resolver = SecretResolver::with_working_dir(
            Some(state.vault.as_ref()),
            secret_resolution_working_dir(&next_loaded).map_err(|error| {
                runtime_status_response(tonic::Status::failed_precondition(format!(
                    "failed to resolve reload working directory: {error}"
                )))
            })?,
        );
        let configured_secrets =
            build_configured_secrets_state(&next_loaded, &resolver, next_generation, "reload")
                .map_err(|error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to rebuild configured secret snapshot after reload: {error}"
                    )))
                })?;
        {
            let mut loaded_guard =
                state.loaded_config.lock().unwrap_or_else(|error| error.into_inner());
            *loaded_guard = next_loaded;
        }
        {
            let mut secret_guard =
                state.configured_secrets.lock().unwrap_or_else(|error| error.into_inner());
            *secret_guard = configured_secrets;
        }
        let outcome = if skipped_steps.is_empty() {
            "applied".to_owned()
        } else {
            "applied_partial".to_owned()
        };
        let message = if skipped_steps.is_empty() {
            "all hot-safe reload steps were applied successfully".to_owned()
        } else {
            "hot-safe reload steps were applied; remaining steps still require restart or manual review"
                .to_owned()
        };
        (outcome, message)
    };

    let envelope = control_plane::ConfigReloadApplyEnvelope {
        contract: contract_descriptor(),
        outcome,
        message,
        plan: plan.clone(),
        applied_steps,
        skipped_steps,
    };
    {
        let mut reload_guard = state.reload_state.lock().unwrap_or_else(|error| error.into_inner());
        reload_guard.latest_plan = Some(plan.clone());
        reload_guard.recent_events.push_front(envelope.clone());
        while reload_guard.recent_events.len() > 10 {
            reload_guard.recent_events.pop_back();
        }
    }
    let event_name = match envelope.outcome.as_str() {
        "applied" | "applied_partial" => "reload_applied",
        "rejected" => "reload_rejected",
        _ => "reload_plan_created",
    };
    state
        .runtime
        .record_console_event(
            context,
            event_name,
            json!({
                "plan_id": plan.plan_id,
                "outcome": envelope.outcome,
                "message": envelope.message,
                "actor": {
                    "principal": context.principal.as_str(),
                    "device_id": context.device_id.as_str(),
                    "channel": context.channel.as_deref(),
                },
                "idempotency_key_present": payload.idempotency_key.as_deref().is_some_and(|value| !value.trim().is_empty()),
                "redacted_diff": plan.steps.iter().map(|step| json!({
                    "component": step.component,
                    "config_path": step.config_path,
                    "category": step.category,
                    "sensitivity": step.sensitivity,
                    "reloadability": step.reloadability,
                    "impact": step.impact,
                    "redacted_diff": step.redacted_diff,
                })).collect::<Vec<_>>(),
                "applied_steps": envelope.applied_steps,
                "skipped_steps": envelope.skipped_steps,
            }),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(envelope)
}

fn validate_requested_reload_path(
    requested_path: Option<&str>,
    current: &crate::config::LoadedConfig,
) -> Result<String, Box<Response>> {
    let active_path = current
        .source
        .split(" +env(")
        .next()
        .map(str::trim)
        .unwrap_or(current.source.as_str())
        .to_owned();
    if let Some(requested_path) = requested_path.map(str::trim).filter(|value| !value.is_empty()) {
        let resolved_requested = resolve_console_config_path(Some(requested_path), false)?
            .ok_or_else(|| {
                Box::new(runtime_status_response(tonic::Status::failed_precondition(
                    "config path could not be resolved for reload planning",
                )))
            })?;
        if !resolved_requested.eq_ignore_ascii_case(active_path.as_str()) {
            return Err(Box::new(runtime_status_response(tonic::Status::failed_precondition(
                "reload planning currently supports only the active daemon config path",
            ))));
        }
    }
    Ok(active_path)
}

fn validate_reload_plan_reference(
    state: &AppState,
    requested_plan_id: Option<&str>,
    force: bool,
) -> Result<(), Response> {
    let Some(requested_plan_id) =
        requested_plan_id.map(str::trim).filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let latest_plan_id = state
        .reload_state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .latest_plan
        .as_ref()
        .map(|plan| plan.plan_id.clone());
    if latest_plan_id.as_deref() == Some(requested_plan_id) || force {
        return Ok(());
    }
    Err(runtime_status_response(tonic::Status::failed_precondition(
        "reload plan_id is stale or unknown; create a new plan or pass force=true after review",
    )))
}

#[allow(clippy::too_many_arguments)]
fn reload_plan_step(
    component: &str,
    config_path: &str,
    category: &str,
    reason: &str,
    default_value: &str,
    validator: &str,
    sensitivity: &str,
    reloadability: &str,
    impact: &str,
    redacted_diff: &str,
) -> control_plane::ConfigReloadPlanStep {
    control_plane::ConfigReloadPlanStep {
        component: component.to_owned(),
        config_path: config_path.to_owned(),
        category: category.to_owned(),
        reason: reason.to_owned(),
        default_value: default_value.to_owned(),
        validator: validator.to_owned(),
        sensitivity: sensitivity.to_owned(),
        reloadability: reloadability.to_owned(),
        impact: impact.to_owned(),
        redacted_diff: redacted_diff.to_owned(),
    }
}

fn build_reload_plan(
    current: &crate::config::LoadedConfig,
    candidate: &crate::config::LoadedConfig,
    source_path: String,
    active_runs: u64,
) -> control_plane::ConfigReloadPlanEnvelope {
    let current = normalized_reload_config(current);
    let candidate = normalized_reload_config(candidate);
    let mut steps = Vec::new();

    if current.memory != candidate.memory {
        steps.push(reload_plan_step(
            "memory_runtime",
            "memory",
            "hot_safe",
            "memory quotas, retention, and auto-inject settings can reload in place",
            "daemon defaults from memory config schema",
            "LoadedConfig memory validation plus runtime quota bounds",
            "operational",
            "hot_safe",
            "updates memory quotas, retention, and auto-inject settings without daemon restart",
            "memory config changed; values redacted from reload audit",
        ));
    }
    if current.model_provider != candidate.model_provider {
        let category =
            if active_runs > 0 { "blocked_while_runs_active" } else { "restart_required" };
        let reason = if active_runs > 0 {
            "provider credentials and routing changed while runs are active"
        } else {
            "provider runtime must be rebuilt to pick up credential or routing changes"
        };
        steps.push(reload_plan_step(
            "model_provider",
            "model_provider",
            category,
            reason,
            "provider registry defaults",
            "provider registry, auth profile, and private base URL validation",
            "secret_refs_redacted",
            category,
            "changes model credentials, routing, registry, or provider network targets",
            "model provider config changed; credential values and URLs are redacted",
        ));
    }
    if current.tool_call.browser_service != candidate.tool_call.browser_service {
        steps.push(reload_plan_step(
            "browser_service",
            "tool_call.browser_service",
            "restart_required",
            "browser service endpoint/auth changes are consumed by long-lived runtime clients",
            "browser service disabled with bounded timeouts by default",
            "browser service endpoint, auth token, timeout, and output-size validation",
            "secret_refs_redacted",
            "restart_required",
            "changes long-lived browser service client connectivity",
            "browser service config changed; auth token values are redacted",
        ));
    }
    if current.admin != candidate.admin {
        steps.push(reload_plan_step(
            "admin_auth",
            "admin",
            "restart_required",
            "admin and connector auth tokens are captured during daemon bootstrap",
            "admin auth required in hardened profiles",
            "admin token, connector token, and auth requirement validation",
            "secret_refs_redacted",
            "restart_required",
            "changes control-plane authentication and connector token posture",
            "admin auth config changed; token values are redacted",
        ));
    }
    if current.deployment != candidate.deployment
        || current.daemon != candidate.daemon
        || current.gateway != candidate.gateway
        || current.identity != candidate.identity
        || current.storage != candidate.storage
        || current.canvas_host != candidate.canvas_host
    {
        steps.push(reload_plan_step(
            "daemon_runtime",
            "deployment/daemon/gateway/storage",
            "restart_required",
            "bind, identity, storage, or canvas hosting changes require daemon restart",
            "loopback-only local deployment defaults",
            "deployment, bind, TLS, storage, and identity fail-closed validation",
            "operational",
            "restart_required",
            "changes daemon bind, TLS, storage, identity, or canvas hosting behavior",
            "daemon runtime config changed; bind/storage details summarized without secrets",
        ));
    }
    if current.feature_rollouts != candidate.feature_rollouts
        || current.cron != candidate.cron
        || current.orchestrator != candidate.orchestrator
        || current.media != candidate.media
        || current.tool_call.allowed_tools != candidate.tool_call.allowed_tools
        || current.tool_call.max_calls_per_run != candidate.tool_call.max_calls_per_run
        || current.tool_call.execution_timeout_ms != candidate.tool_call.execution_timeout_ms
        || current.tool_call.process_runner != candidate.tool_call.process_runner
        || current.tool_call.wasm_runtime != candidate.tool_call.wasm_runtime
        || current.tool_call.http_fetch != candidate.tool_call.http_fetch
        || current.channel_router != candidate.channel_router
    {
        steps.push(reload_plan_step(
            "safety_and_routing",
            "tool_call/channel_router/feature_rollouts",
            "manual_review",
            "safety posture, rollout, or routing changes need explicit operator review before hot reload",
            "deny-by-default tool posture with conservative rollout defaults",
            "tool, sandbox, fetch, routing, and rollout policy validation",
            "policy",
            "manual_review",
            "changes sensitive action policy, routing, rollout, or channel behavior",
            "safety or routing config changed; policy diff is summarized without secret values",
        ));
    }

    let summary = control_plane::ConfigReloadPlanSummary {
        hot_safe: u32::try_from(steps.iter().filter(|step| step.category == "hot_safe").count())
            .unwrap_or(u32::MAX),
        restart_required: u32::try_from(
            steps.iter().filter(|step| step.category == "restart_required").count(),
        )
        .unwrap_or(u32::MAX),
        blocked_while_runs_active: u32::try_from(
            steps.iter().filter(|step| step.category == "blocked_while_runs_active").count(),
        )
        .unwrap_or(u32::MAX),
        manual_review: u32::try_from(
            steps.iter().filter(|step| step.category == "manual_review").count(),
        )
        .unwrap_or(u32::MAX),
    };

    control_plane::ConfigReloadPlanEnvelope {
        contract: contract_descriptor(),
        plan_id: Ulid::new().to_string(),
        source_path,
        generated_at_unix_ms: unix_ms_now().unwrap_or(0),
        active_runs,
        requires_restart: steps.iter().any(|step| step.category == "restart_required"),
        hot_safe_applicable: steps.iter().any(|step| step.category == "hot_safe"),
        summary,
        steps,
    }
}

fn normalized_reload_config(config: &crate::config::LoadedConfig) -> crate::config::LoadedConfig {
    let mut normalized = config.clone();
    if normalized.model_provider.openai_api_key_secret_ref.is_some()
        || normalized.model_provider.auth_profile_id.is_some()
    {
        normalized.model_provider.openai_api_key = None;
        normalized.model_provider.credential_source = None;
    }
    if normalized.model_provider.anthropic_api_key_secret_ref.is_some()
        || normalized.model_provider.auth_profile_id.is_some()
    {
        normalized.model_provider.anthropic_api_key = None;
        normalized.model_provider.credential_source = None;
    }
    for provider in &mut normalized.model_provider.registry.providers {
        if provider.api_key_secret_ref.is_some() || provider.auth_profile_id.is_some() {
            provider.api_key = None;
            provider.credential_source = None;
        }
    }
    if normalized.admin.auth_token_secret_ref.is_some() {
        normalized.admin.auth_token = None;
    }
    if normalized.admin.connector_token_secret_ref.is_some() {
        normalized.admin.connector_token = None;
    }
    if normalized.tool_call.browser_service.auth_token_secret_ref.is_some() {
        normalized.tool_call.browser_service.auth_token = None;
    }
    normalized
}

fn estimate_active_runs(state: &AppState) -> u64 {
    let counters = state.runtime.counters.snapshot();
    counters
        .orchestrator_runs_started
        .saturating_sub(counters.orchestrator_runs_completed)
        .saturating_sub(counters.orchestrator_runs_cancelled)
}

fn memory_runtime_config_from_loaded(loaded: &crate::config::LoadedConfig) -> MemoryRuntimeConfig {
    MemoryRuntimeConfig {
        max_item_bytes: loaded.memory.max_item_bytes,
        max_item_tokens: loaded.memory.max_item_tokens,
        auto_inject_enabled: loaded.memory.auto_inject.enabled,
        auto_inject_max_items: loaded.memory.auto_inject.max_items,
        default_ttl_ms: loaded.memory.default_ttl_ms,
        retention_max_entries: loaded.memory.retention.max_entries,
        retention_max_bytes: loaded.memory.retention.max_bytes,
        retention_ttl_days: loaded.memory.retention.ttl_days,
        retention_vacuum_schedule: loaded.memory.retention.vacuum_schedule.clone(),
    }
}
