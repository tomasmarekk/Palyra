//! Console objective handlers for the `/console/v1/objectives` route family.
//!
//! An objective bundles three persisted artifacts that this module keeps in
//! sync: the objective record itself, a backing cron job plus routine
//! metadata that drives automation, and managed-block projections into
//! workspace documents. Lifecycle actions fail closed in opposite
//! directions: `fire`/`resume` preflight the workspace projection and refuse
//! to dispatch when it is malformed, while `pause`/`cancel`/`archive` always
//! apply and downgrade a projection failure to a response warning. Response
//! shapes are part of the `/console/v1` wire contract consumed by
//! `apps/web`.

use std::sync::Arc;

use serde::Deserialize;

use super::diagnostics::{authorize_console_session, build_page_info};

use crate::{
    cron::{self, CronTimezoneMode},
    domain::workspace::{
        objective_workspace_document_path, sync_workspace_managed_block,
        WorkspaceManagedBlockUpdate, WorkspaceManagedEntry,
    },
    gateway::proto::palyra::cron::v1 as cron_v1,
    journal::{
        CronConcurrencyPolicy, CronJobCreateRequest, CronJobRecord, CronJobUpdatePatch,
        CronMisfirePolicy, CronRetryPolicy, CronScheduleType, OrchestratorCancelRequest,
        WorkspaceDocumentWriteRequest,
    },
    objective_judge::ObjectiveJudgeInput,
    objectives::{
        render_objective_contract_context_block, ObjectiveApproachKind, ObjectiveApproachRecord,
        ObjectiveAttemptRecord, ObjectiveAutomationBinding, ObjectiveBudget, ObjectiveContract,
        ObjectiveKind, ObjectiveLifecycleRecord, ObjectivePriority, ObjectiveRecord,
        ObjectiveRegistryError, ObjectiveState, ObjectiveUpsert, ObjectiveWorkspaceBinding,
    },
    routines::{
        default_outcome_from_cron_status, join_run_metadata, natural_language_schedule_preview,
        shadow_manual_schedule_payload_json, RoutineApprovalMode, RoutineApprovalPolicy,
        RoutineDeliveryConfig, RoutineDeliveryMode, RoutineDispatchMode, RoutineExecutionConfig,
        RoutineExecutionPosture, RoutineQuietHours, RoutineRunMetadataUpsert, RoutineSilentPolicy,
        RoutineTriggerKind,
    },
    *,
};

const DEFAULT_OBJECTIVE_CHANNEL: &str = "system:objectives";
const DEFAULT_OBJECTIVE_PAGE_LIMIT: usize = 100;
const MAX_OBJECTIVE_PAGE_LIMIT: usize = 500;
/// Default heartbeat cadence when the payload supplies no schedule.
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 60 * 60 * 1_000;
const OBJECTIVE_FOCUS_BLOCK_ID: &str = "objective-focus";
const OBJECTIVE_HEARTBEAT_BLOCK_ID: &str = "objective-heartbeats";
const OBJECTIVE_INBOX_BLOCK_ID: &str = "objective-inbox";
/// Warning attached to stop-action responses when the action applied but the
/// workspace projection could not be rewritten. Tests pin its wording, so
/// keep "action was applied" and "workspace projection did not update".
const OBJECTIVE_WORKSPACE_PROJECTION_WARNING: &str = "Objective lifecycle action was applied, but workspace projection did not update. Repair malformed Palyra managed blocks or retry after workspace storage recovers.";

/// Classifies each objective endpoint so CSRF enforcement stays declarative:
/// mutating request kinds require a CSRF-validated console session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsoleObjectiveRequestKind {
    List,
    Get,
    Upsert,
    Lifecycle,
    Attempt,
    Approach,
    Summary,
}

impl ConsoleObjectiveRequestKind {
    fn requires_csrf(self) -> bool {
        matches!(self, Self::Upsert | Self::Lifecycle | Self::Attempt | Self::Approach)
    }
}

#[allow(clippy::result_large_err)]
fn authorize_objective_console_session(
    state: &AppState,
    headers: &HeaderMap,
    request_kind: ConsoleObjectiveRequestKind,
) -> Result<ConsoleSession, Response> {
    authorize_console_session(state, headers, request_kind.requires_csrf())
}

/// Query parameters for the objective list endpoint; `kind` and `state`
/// filter case-insensitively and `after_objective_id` enables keyset paging.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleObjectiveListQuery {
    #[serde(default)]
    after_objective_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

/// Budget fields accepted on upsert; omitted fields keep the stored budget
/// values (see `normalize_budget`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleObjectiveBudgetPayload {
    #[serde(default)]
    max_runs: Option<u32>,
    #[serde(default)]
    max_tokens: Option<u64>,
    #[serde(default)]
    notes: Option<String>,
}

/// Body for objective create/update. `objective_id` selects update-in-place;
/// most optional fields fall back to the existing record on update so a
/// partial payload does not erase stored values. Schedule fields feed
/// `resolve_objective_schedule` and are mutually layered, not combined.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleObjectiveUpsertRequest {
    #[serde(default)]
    objective_id: Option<String>,
    kind: String,
    name: String,
    prompt: String,
    #[serde(default)]
    owner_principal: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    session_key: Option<String>,
    #[serde(default)]
    session_label: Option<String>,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    budget: Option<ConsoleObjectiveBudgetPayload>,
    #[serde(default)]
    current_focus: Option<String>,
    #[serde(default)]
    success_criteria: Option<String>,
    #[serde(default)]
    contract: Option<ObjectiveContract>,
    #[serde(default)]
    exit_condition: Option<String>,
    #[serde(default)]
    next_recommended_step: Option<String>,
    #[serde(default)]
    standing_order: Option<String>,
    #[serde(default)]
    related_document_paths: Option<Vec<String>>,
    #[serde(default)]
    related_memory_ids: Option<Vec<String>>,
    #[serde(default)]
    related_session_ids: Option<Vec<String>>,
    #[serde(default)]
    linked_artifact_paths: Option<Vec<String>>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    natural_language_schedule: Option<String>,
    #[serde(default)]
    schedule_type: Option<String>,
    #[serde(default)]
    cron_expression: Option<String>,
    #[serde(default)]
    every_interval_ms: Option<u64>,
    #[serde(default)]
    at_timestamp_rfc3339: Option<String>,
    #[serde(default)]
    delivery_mode: Option<String>,
    #[serde(default)]
    delivery_channel: Option<String>,
    #[serde(default)]
    execution_posture: Option<String>,
    #[serde(default)]
    quiet_hours_start: Option<String>,
    #[serde(default)]
    quiet_hours_end: Option<String>,
    #[serde(default)]
    quiet_hours_timezone: Option<String>,
    #[serde(default)]
    cooldown_ms: Option<u64>,
    #[serde(default)]
    approval_mode: Option<String>,
    #[serde(default)]
    template_id: Option<String>,
}

/// Body for lifecycle actions: `action` is one of
/// fire|pause|resume|cancel|archive and `reason` is an optional operator note
/// capped at 500 bytes.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleObjectiveLifecycleRequest {
    action: String,
    #[serde(default)]
    reason: Option<String>,
}

/// Body for manually recording an attempt against an objective, optionally
/// linking it to an existing run and session.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleObjectiveAttemptRequest {
    status: String,
    summary: String,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    outcome_kind: Option<String>,
    #[serde(default)]
    learned: Option<String>,
    #[serde(default)]
    recommended_next_step: Option<String>,
    #[serde(default)]
    completed_at_unix_ms: Option<i64>,
}

/// Body for appending an approach-history entry (attempted, learned, failed
/// approach, recommended next step, or standing order).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleObjectiveApproachRequest {
    kind: String,
    summary: String,
    #[serde(default)]
    run_id: Option<String>,
}

/// Handles `GET /console/v1/objectives`: lists the caller's objectives with
/// optional kind/state filters and keyset paging by objective id.
///
/// # Errors
/// Returns an error response when console authorization fails or when the
/// objective registry or any per-objective view source cannot be read.
pub(crate) async fn console_objectives_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleObjectiveListQuery>,
) -> Result<Json<Value>, Response> {
    let session =
        authorize_objective_console_session(&state, &headers, ConsoleObjectiveRequestKind::List)?;
    let limit =
        query.limit.unwrap_or(DEFAULT_OBJECTIVE_PAGE_LIMIT).clamp(1, MAX_OBJECTIVE_PAGE_LIMIT);
    let kind_filter = query
        .kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    let state_filter = query
        .state
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    let after_objective_id =
        query.after_objective_id.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let (objectives, next_after_objective_id) = list_objective_views(
        &state,
        session.context.principal.as_str(),
        kind_filter.as_deref(),
        state_filter.as_deref(),
        after_objective_id,
        limit,
    )
    .await?;
    Ok(Json(json!({
        "objectives": objectives,
        "next_after_objective_id": next_after_objective_id,
        "page": build_page_info(limit, objectives.len(), next_after_objective_id.clone()),
    })))
}

/// Handles `GET /console/v1/objectives/{objective_id}`: returns one owned
/// objective view.
///
/// # Errors
/// Returns an error response when console authorization fails, when the
/// objective is missing, or when it is owned by a different principal.
pub(crate) async fn console_objective_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(objective_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session =
        authorize_objective_console_session(&state, &headers, ConsoleObjectiveRequestKind::Get)?;
    let objective =
        load_objective_for_owner(&state, objective_id.as_str(), session.context.principal.as_str())
            .await?;
    Ok(Json(json!({ "objective": objective })))
}

/// Handles `POST /console/v1/objectives`: creates or updates an objective
/// together with its backing cron job, routine metadata, and workspace
/// projection.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the payload is invalid (kind, priority, schedule, delivery,
/// quiet hours, approval mode), when the caller does not own an existing
/// objective, or when any persistence step fails.
pub(crate) async fn console_objective_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleObjectiveUpsertRequest>,
) -> Result<Json<Value>, Response> {
    let session =
        authorize_objective_console_session(&state, &headers, ConsoleObjectiveRequestKind::Upsert)?;
    let owner_principal =
        normalize_owner_principal(&payload.owner_principal, session.context.principal.as_str())?;
    let kind = parse_objective_kind(payload.kind.as_str())?;
    let priority = parse_objective_priority(payload.priority.as_deref())?;
    let channel = normalize_channel(payload.channel.as_deref(), session.context.channel.as_deref());
    let existing = if let Some(objective_id) = payload.objective_id.as_deref() {
        state.objectives.get_objective(objective_id).map_err(objective_registry_error_response)?
    } else {
        None
    };
    if let Some(existing_objective) = existing.as_ref() {
        ensure_objective_owner(existing_objective, owner_principal.as_str())?;
    }
    let objective_id = existing
        .as_ref()
        .map(|entry| entry.objective_id.clone())
        .unwrap_or_else(|| Ulid::new().to_string());
    let workspace_document_path = existing
        .as_ref()
        .map(|entry| entry.workspace.workspace_document_path.clone())
        .unwrap_or_else(|| {
            objective_workspace_document_path(objective_id.as_str())
                .expect("objective ids are ULIDs, which always form a valid workspace path")
        });
    let schedule = resolve_objective_schedule(&payload, kind, state.cron_timezone_mode)?;
    let execution = parse_objective_execution_config(
        payload.execution_posture.as_deref(),
        kind,
        existing.as_ref(),
    )?;
    let automation = ObjectiveAutomationBinding {
        routine_id: Some(
            existing
                .as_ref()
                .and_then(|entry| entry.automation.routine_id.clone())
                .unwrap_or_else(|| objective_id.clone()),
        ),
        // Updates keep the stored flag unless the payload overrides it. New
        // heartbeats start enabled only when a schedule resolved; every
        // other kind starts enabled.
        enabled: payload.enabled.unwrap_or_else(|| {
            existing
                .as_ref()
                .map(|entry| entry.automation.enabled)
                .unwrap_or(kind != ObjectiveKind::Heartbeat || schedule.is_scheduled)
        }),
        trigger_kind: schedule.trigger_kind,
        schedule_type: schedule.schedule_type.as_str().to_owned(),
        schedule_payload_json: schedule.schedule_payload_json.clone(),
        execution,
        delivery: parse_delivery(
            payload.delivery_mode.as_deref(),
            payload.delivery_channel.clone(),
        )?,
        quiet_hours: parse_quiet_hours(
            payload.quiet_hours_start.as_deref(),
            payload.quiet_hours_end.as_deref(),
            payload.quiet_hours_timezone.clone(),
        )?,
        cooldown_ms: payload.cooldown_ms.unwrap_or_else(|| {
            existing.as_ref().map(|entry| entry.automation.cooldown_ms).unwrap_or(0)
        }),
        approval_policy: parse_approval_policy(payload.approval_mode.as_deref())?,
        template_id: payload.template_id.clone().or_else(|| {
            if kind == ObjectiveKind::Heartbeat {
                Some("heartbeat".to_owned())
            } else {
                existing.as_ref().and_then(|entry| entry.automation.template_id.clone())
            }
        }),
    };
    // routine_id is always Some by construction above; this guard is a
    // defensive fail-loud path, not a reachable state.
    let routine_id = automation.routine_id.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::internal(
            "objective automation routine id should always exist",
        ))
    })?;
    let is_new_objective = existing.is_none();
    let routine_id_for_rollback = routine_id.clone();
    let cron_job = persist_objective_job(
        &state,
        existing.as_ref().and_then(|entry| entry.automation.routine_id.clone()),
        ObjectiveJobUpsert {
            routine_id,
            name: payload.name.clone(),
            prompt: payload.prompt.clone(),
            owner_principal: owner_principal.clone(),
            channel: channel.clone(),
            session_key: payload.session_key.clone(),
            session_label: payload.session_label.clone(),
            schedule_type: schedule.schedule_type,
            schedule_payload_json: schedule.schedule_payload_json,
            enabled: automation.enabled,
            next_run_at_unix_ms: schedule.next_run_at_unix_ms,
        },
    )
    .await?;
    persist_objective_routine_metadata(&state, &automation)
        .map_err(routine_registry_error_response)?;
    let now_unix_ms = unix_ms_now().map_err(internal_console_error)?;
    let initial_state = initial_objective_state(existing.as_ref(), payload.enabled);
    let mut lifecycle_history =
        existing.as_ref().map(|entry| entry.lifecycle_history.clone()).unwrap_or_default();
    // Only creation seeds a lifecycle event; updates leave history untouched
    // so the audit trail reflects explicit lifecycle actions only.
    if is_new_objective {
        lifecycle_history.push(ObjectiveLifecycleRecord {
            event_id: Ulid::new().to_string(),
            action: "created".to_owned(),
            from_state: None,
            to_state: initial_state,
            reason: Some(format!("kind={}", kind.as_str())),
            run_id: None,
            occurred_at_unix_ms: now_unix_ms,
        });
    }
    let objective = match state.objectives.upsert_objective(ObjectiveUpsert {
        record: ObjectiveRecord {
            objective_id: objective_id.clone(),
            kind,
            state: initial_state,
            name: payload.name,
            prompt: payload.prompt,
            owner_principal: owner_principal.clone(),
            channel: Some(channel.clone()),
            priority,
            budget: normalize_budget(payload.budget, existing.as_ref()),
            current_focus: payload
                .current_focus
                .or_else(|| existing.as_ref().and_then(|entry| entry.current_focus.clone())),
            success_criteria: payload
                .success_criteria
                .or_else(|| existing.as_ref().and_then(|entry| entry.success_criteria.clone())),
            contract: payload
                .contract
                .or_else(|| existing.as_ref().map(|entry| entry.contract.clone()))
                .unwrap_or_default(),
            contract_history: existing
                .as_ref()
                .map(|entry| entry.contract_history.clone())
                .unwrap_or_default(),
            exit_condition: payload
                .exit_condition
                .or_else(|| existing.as_ref().and_then(|entry| entry.exit_condition.clone())),
            next_recommended_step: payload.next_recommended_step.or_else(|| {
                existing.as_ref().and_then(|entry| entry.next_recommended_step.clone())
            }),
            standing_order: payload
                .standing_order
                .or_else(|| existing.as_ref().and_then(|entry| entry.standing_order.clone())),
            workspace: ObjectiveWorkspaceBinding {
                workspace_document_path,
                session_key: payload.session_key.or_else(|| {
                    existing.as_ref().and_then(|entry| entry.workspace.session_key.clone())
                }),
                session_label: payload.session_label.or_else(|| {
                    existing.as_ref().and_then(|entry| entry.workspace.session_label.clone())
                }),
                related_document_paths: payload.related_document_paths.unwrap_or_else(|| {
                    existing
                        .as_ref()
                        .map(|entry| entry.workspace.related_document_paths.clone())
                        .unwrap_or_default()
                }),
                related_memory_ids: payload.related_memory_ids.unwrap_or_else(|| {
                    existing
                        .as_ref()
                        .map(|entry| entry.workspace.related_memory_ids.clone())
                        .unwrap_or_default()
                }),
                related_session_ids: payload.related_session_ids.unwrap_or_else(|| {
                    existing
                        .as_ref()
                        .map(|entry| entry.workspace.related_session_ids.clone())
                        .unwrap_or_default()
                }),
            },
            automation,
            last_attempt: existing.as_ref().and_then(|entry| entry.last_attempt.clone()),
            attempt_history: existing
                .as_ref()
                .map(|entry| entry.attempt_history.clone())
                .unwrap_or_default(),
            approach_history: existing
                .as_ref()
                .map(|entry| entry.approach_history.clone())
                .unwrap_or_default(),
            lifecycle_history,
            linked_run_ids: existing
                .as_ref()
                .map(|entry| entry.linked_run_ids.clone())
                .unwrap_or_default(),
            linked_artifact_paths: payload.linked_artifact_paths.unwrap_or_else(|| {
                existing
                    .as_ref()
                    .map(|entry| entry.linked_artifact_paths.clone())
                    .unwrap_or_default()
            }),
            created_at_unix_ms: existing
                .as_ref()
                .map(|entry| entry.created_at_unix_ms)
                .unwrap_or(now_unix_ms),
            updated_at_unix_ms: now_unix_ms,
            archived_at_unix_ms: existing.and_then(|entry| entry.archived_at_unix_ms),
        },
    }) {
        Ok(objective) => objective,
        Err(error) => {
            if is_new_objective {
                rollback_created_objective_companions(&state, routine_id_for_rollback.as_str())
                    .await?;
            }
            return Err(objective_registry_error_response(error));
        }
    };
    project_objective_workspace(
        &state,
        owner_principal.as_str(),
        Some(channel.as_str()),
        Some(cron_job.session_key.as_deref().unwrap_or_default()).filter(|value| !value.is_empty()),
        &objective,
    )
    .await?;
    let objective_view = build_objective_view(&state, objective).await?;
    Ok(Json(json!({ "objective": objective_view })))
}

/// Handles `POST /console/v1/objectives/{objective_id}/lifecycle`: applies a
/// fire/pause/resume/cancel/archive action.
///
/// Start actions (`fire`, `resume`) preflight the workspace projection and
/// fail before any side effect when it is malformed; stop actions always
/// apply and report a projection failure via
/// `workspace_projection_warning` instead of an error.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the action is unknown or invalid for the current state, or
/// when persisting the transition fails.
pub(crate) async fn console_objective_lifecycle_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(objective_id): Path<String>,
    Json(payload): Json<ConsoleObjectiveLifecycleRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_objective_console_session(
        &state,
        &headers,
        ConsoleObjectiveRequestKind::Lifecycle,
    )?;
    let mut objective =
        load_objective_record(&state, objective_id.as_str(), session.context.principal.as_str())?;
    let action = payload.action.trim().to_ascii_lowercase();
    let reason = normalize_lifecycle_reason(payload.reason)?;
    // Run the pure state projection on a clone first: it validates the
    // action against the current state without side effects, and for start
    // actions the workspace projection must be writable before any dispatch
    // or cron mutation happens.
    let mut preflight_objective = objective.clone();
    apply_lifecycle_workspace_projection(action.as_str(), &mut preflight_objective)
        .map_err(runtime_status_response)?;
    if lifecycle_action_requires_workspace_preflight(action.as_str()) {
        preflight_objective_workspace_projection(&state, &preflight_objective).await?;
    }
    match action.as_str() {
        "fire" => apply_fire_action(&state, &mut objective, reason).await?,
        "pause" => apply_pause_action(&state, &mut objective, reason).await?,
        "resume" => apply_resume_action(&state, &mut objective, reason).await?,
        "cancel" => apply_cancel_action(&state, &mut objective, reason).await?,
        "archive" => apply_archive_action(&state, &mut objective, reason).await?,
        _ => {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "action must be one of fire|pause|resume|cancel|archive",
            )));
        }
    }
    let updated = state
        .objectives
        .upsert_objective(ObjectiveUpsert { record: objective })
        .map_err(objective_registry_error_response)?;
    // Stop actions must never be rolled back because a workspace document is
    // malformed; their projection failure is downgraded to a warning.
    let workspace_projection_warning = match project_objective_workspace(
        &state,
        updated.owner_principal.as_str(),
        updated.channel.as_deref(),
        updated.workspace.session_key.as_deref(),
        &updated,
    )
    .await
    {
        Ok(()) => None,
        Err(_) if lifecycle_action_tolerates_workspace_projection_failure(action.as_str()) => {
            Some(OBJECTIVE_WORKSPACE_PROJECTION_WARNING)
        }
        Err(error) => return Err(error),
    };
    let objective_view = build_objective_view(&state, updated).await?;
    let mut response = json!({ "objective": objective_view });
    if let Some(warning) = workspace_projection_warning {
        response["workspace_projection_warning"] = json!(warning);
    }
    Ok(Json(response))
}

/// Handles `POST /console/v1/objectives/{objective_id}/attempts`: appends an
/// attempt record, links its run id, and refreshes the workspace projection.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the objective is missing or not owned by the caller, or when
/// persistence or projection fails.
pub(crate) async fn console_objective_attempt_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(objective_id): Path<String>,
    Json(payload): Json<ConsoleObjectiveAttemptRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_objective_console_session(
        &state,
        &headers,
        ConsoleObjectiveRequestKind::Attempt,
    )?;
    let mut objective =
        load_objective_record(&state, objective_id.as_str(), session.context.principal.as_str())?;
    let now_unix_ms = unix_ms_now().map_err(internal_console_error)?;
    let attempt = ObjectiveAttemptRecord {
        attempt_id: Ulid::new().to_string(),
        run_id: normalize_optional_text(payload.run_id.as_deref()),
        session_id: normalize_optional_text(payload.session_id.as_deref()),
        status: payload.status,
        outcome_kind: payload.outcome_kind,
        summary: payload.summary,
        learned: payload.learned,
        recommended_next_step: payload
            .recommended_next_step
            .or_else(|| objective.next_recommended_step.clone()),
        created_at_unix_ms: now_unix_ms,
        completed_at_unix_ms: payload.completed_at_unix_ms,
    };
    if let Some(run_id) = attempt.run_id.as_ref() {
        objective.linked_run_ids.push(run_id.clone());
    }
    if let Some(next_step) = attempt.recommended_next_step.clone() {
        objective.next_recommended_step = Some(next_step);
    }
    objective.last_attempt = Some(attempt.clone());
    objective.attempt_history.push(attempt);
    let updated = state
        .objectives
        .upsert_objective(ObjectiveUpsert { record: objective })
        .map_err(objective_registry_error_response)?;
    project_objective_workspace(
        &state,
        updated.owner_principal.as_str(),
        updated.channel.as_deref(),
        updated.workspace.session_key.as_deref(),
        &updated,
    )
    .await?;
    Ok(Json(json!({ "objective": build_objective_view(&state, updated).await? })))
}

/// Handles `POST /console/v1/objectives/{objective_id}/approach`: appends an
/// approach-history entry and refreshes the workspace projection.
///
/// # Errors
/// Returns an error response when console authorization or CSRF validation
/// fails, when the approach kind is unknown, when the objective is missing or
/// not owned by the caller, or when persistence fails.
pub(crate) async fn console_objective_approach_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(objective_id): Path<String>,
    Json(payload): Json<ConsoleObjectiveApproachRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_objective_console_session(
        &state,
        &headers,
        ConsoleObjectiveRequestKind::Approach,
    )?;
    let mut objective =
        load_objective_record(&state, objective_id.as_str(), session.context.principal.as_str())?;
    objective.approach_history.push(ObjectiveApproachRecord {
        entry_id: Ulid::new().to_string(),
        kind: parse_approach_kind(payload.kind.as_str())?,
        summary: payload.summary,
        run_id: normalize_optional_text(payload.run_id.as_deref()),
        created_at_unix_ms: unix_ms_now().map_err(internal_console_error)?,
    });
    let updated = state
        .objectives
        .upsert_objective(ObjectiveUpsert { record: objective })
        .map_err(objective_registry_error_response)?;
    project_objective_workspace(
        &state,
        updated.owner_principal.as_str(),
        updated.channel.as_deref(),
        updated.workspace.session_key.as_deref(),
        &updated,
    )
    .await?;
    Ok(Json(json!({ "objective": build_objective_view(&state, updated).await? })))
}

/// Handles `GET /console/v1/objectives/{objective_id}/summary`: returns the
/// objective view plus a rendered markdown digest of its core fields.
///
/// # Errors
/// Returns an error response when console authorization fails or when the
/// objective is missing or not owned by the caller.
pub(crate) async fn console_objective_summary_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(objective_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_objective_console_session(
        &state,
        &headers,
        ConsoleObjectiveRequestKind::Summary,
    )?;
    let objective =
        load_objective_record(&state, objective_id.as_str(), session.context.principal.as_str())?;
    let view = build_objective_view(&state, objective.clone()).await?;
    Ok(Json(json!({
        "objective_id": objective.objective_id,
        "summary_markdown": render_objective_summary_markdown(&view),
        "summary": view,
    })))
}

/// Outcome of schedule resolution; `is_scheduled` is false only for the
/// manual (shadow) schedule used by unscheduled objectives.
#[derive(Debug, Clone)]
struct ObjectiveScheduleResolution {
    trigger_kind: RoutineTriggerKind,
    schedule_type: CronScheduleType,
    schedule_payload_json: String,
    next_run_at_unix_ms: Option<i64>,
    is_scheduled: bool,
}

/// Fields forwarded to the cron registry when creating or updating the
/// backing job for an objective.
#[derive(Debug, Clone)]
struct ObjectiveJobUpsert {
    routine_id: String,
    name: String,
    prompt: String,
    owner_principal: String,
    channel: String,
    session_key: Option<String>,
    session_label: Option<String>,
    schedule_type: CronScheduleType,
    schedule_payload_json: String,
    enabled: bool,
    next_run_at_unix_ms: Option<i64>,
}

#[derive(Debug)]
struct ObjectiveRecordPage {
    records: Vec<ObjectiveRecord>,
    next_after_objective_id: Option<String>,
}

async fn load_objective_for_owner(
    state: &AppState,
    objective_id: &str,
    owner_principal: &str,
) -> Result<Value, Response> {
    let objective = load_objective_record(state, objective_id, owner_principal)?;
    build_objective_view(state, objective).await
}

/// Loads one objective and enforces ownership.
///
/// # Errors
/// Returns not-found when the objective does not exist and
/// permission-denied when it is owned by a different principal.
#[allow(clippy::result_large_err)]
fn load_objective_record(
    state: &AppState,
    objective_id: &str,
    owner_principal: &str,
) -> Result<ObjectiveRecord, Response> {
    let objective = state
        .objectives
        .get_objective(objective_id)
        .map_err(objective_registry_error_response)?
        .ok_or_else(|| runtime_status_response(tonic::Status::not_found("objective not found")))?;
    ensure_objective_owner(&objective, owner_principal)?;
    Ok(objective)
}

/// Builds views for the requested page of objectives owned by the principal.
async fn list_objective_views(
    state: &AppState,
    principal: &str,
    kind_filter: Option<&str>,
    state_filter: Option<&str>,
    after_objective_id: Option<&str>,
    limit: usize,
) -> Result<(Vec<Value>, Option<String>), Response> {
    let page = page_objective_records(
        state.objectives.list_objectives().map_err(objective_registry_error_response)?,
        principal,
        kind_filter,
        state_filter,
        after_objective_id,
        limit,
    );
    let mut objectives = Vec::new();
    for objective in page.records {
        objectives.push(build_objective_view(state, objective).await?);
    }
    Ok((objectives, page.next_after_objective_id))
}

fn page_objective_records(
    records: Vec<ObjectiveRecord>,
    principal: &str,
    kind_filter: Option<&str>,
    state_filter: Option<&str>,
    after_objective_id: Option<&str>,
    limit: usize,
) -> ObjectiveRecordPage {
    let mut records = records
        .into_iter()
        .filter(|entry| entry.owner_principal == principal)
        .filter(|entry| kind_filter.is_none_or(|expected| entry.kind.as_str() == expected))
        .filter(|entry| state_filter.is_none_or(|expected| entry.state.as_str() == expected))
        .filter(|entry| after_objective_id.is_none_or(|after| entry.objective_id.as_str() > after))
        .collect::<Vec<_>>();
    records.sort_by(|left, right| left.objective_id.cmp(&right.objective_id));
    let has_more = records.len() > limit;
    if has_more {
        records.truncate(limit);
    }
    let next_after_objective_id =
        has_more.then(|| records.last().map(|objective| objective.objective_id.clone())).flatten();
    ObjectiveRecordPage { records, next_after_objective_id }
}

/// Renders the wire-facing objective view: the record joined with its linked
/// routine snapshot, latest run, derived health badge, and attempt records
/// reconciled against the latest run status.
async fn build_objective_view(
    state: &AppState,
    objective: ObjectiveRecord,
) -> Result<Value, Response> {
    let routine = load_objective_routine(state, &objective).await?;
    let latest_run = latest_objective_run(state, &objective).await?;
    let latest_run = latest_run.or_else(|| preserved_run_from_objective_attempt(&objective));
    let health = compute_objective_health(&objective, routine.as_ref(), latest_run.as_ref());
    let (last_attempt, attempt_history) =
        objective_attempts_for_view(&objective, latest_run.as_ref());
    let contract_context = render_objective_contract_context_block(&objective);
    let judge_input_preview = ObjectiveJudgeInput::from_objective(&objective, None, Vec::new());
    Ok(json!({
        "objective_id": objective.objective_id,
        "kind": objective.kind.as_str(),
        "state": objective.state.as_str(),
        "name": objective.name,
        "prompt": objective.prompt,
        "owner_principal": objective.owner_principal,
        "channel": objective.channel,
        "priority": objective.priority.as_str(),
        "budget": {
            "max_runs": objective.budget.max_runs,
            "max_tokens": objective.budget.max_tokens,
            "notes": objective.budget.notes,
        },
        "current_focus": objective.current_focus,
        "success_criteria": objective.success_criteria,
        "contract": objective.contract,
        "contract_context": contract_context,
        "contract_history": objective.contract_history,
        "objective_judge": {
            "rollout_enabled": state.runtime.config.feature_rollouts.objective_judge.enabled,
            "auxiliary_task_kind": "objective_judge",
            "input_preview": judge_input_preview,
        },
        "exit_condition": objective.exit_condition,
        "next_recommended_step": objective.next_recommended_step,
        "standing_order": objective.standing_order,
        "workspace": {
            "workspace_document_path": objective.workspace.workspace_document_path,
            "session_key": objective.workspace.session_key,
            "session_label": objective.workspace.session_label,
            "related_document_paths": objective.workspace.related_document_paths,
            "related_memory_ids": objective.workspace.related_memory_ids,
            "related_session_ids": objective.workspace.related_session_ids,
        },
        "automation": {
            "routine_id": objective.automation.routine_id,
            "enabled": objective.automation.enabled,
            "trigger_kind": objective.automation.trigger_kind.as_str(),
            "schedule_type": objective.automation.schedule_type,
            "schedule_payload": serde_json::from_str::<Value>(objective.automation.schedule_payload_json.as_str())
                .unwrap_or_else(|_| json!({ "raw": objective.automation.schedule_payload_json })),
            "delivery_mode": objective.automation.delivery.mode.as_str(),
            "delivery_channel": objective.automation.delivery.channel,
            "run_mode": objective.automation.execution.run_mode.as_str(),
            "execution_posture": objective.automation.execution.execution_posture.as_str(),
            "procedure_profile_id": objective.automation.execution.procedure_profile_id.clone(),
            "skill_profile_id": objective.automation.execution.skill_profile_id.clone(),
            "provider_profile_id": objective.automation.execution.provider_profile_id.clone(),
            "cooldown_ms": objective.automation.cooldown_ms,
            "approval_mode": objective.automation.approval_policy.mode.as_str(),
            "template_id": objective.automation.template_id,
        },
        "linked_run_ids": objective.linked_run_ids,
        "linked_artifact_paths": objective.linked_artifact_paths,
        "last_attempt": last_attempt,
        "attempt_history": attempt_history,
        "approach_history": objective.approach_history,
        "lifecycle_history": objective.lifecycle_history,
        "linked_routine": routine,
        "last_run": latest_run,
        "health": health,
        "created_at_unix_ms": objective.created_at_unix_ms,
        "updated_at_unix_ms": objective.updated_at_unix_ms,
        "archived_at_unix_ms": objective.archived_at_unix_ms,
    }))
}

/// New objectives start `Active` only when explicitly enabled and `Draft`
/// otherwise; updates always keep the stored state so upserts cannot bypass
/// lifecycle actions.
fn initial_objective_state(
    existing: Option<&ObjectiveRecord>,
    requested_enabled: Option<bool>,
) -> ObjectiveState {
    existing.map(|entry| entry.state).unwrap_or_else(|| {
        if requested_enabled == Some(true) {
            ObjectiveState::Active
        } else {
            ObjectiveState::Draft
        }
    })
}

/// Returns copies of the attempt records with any attempt that references the
/// latest run updated to that run's terminal status, so the console never
/// shows a stale "running" attempt next to a finished run.
fn objective_attempts_for_view(
    objective: &ObjectiveRecord,
    latest_run: Option<&Value>,
) -> (Option<ObjectiveAttemptRecord>, Vec<ObjectiveAttemptRecord>) {
    let mut last_attempt = objective.last_attempt.clone();
    let mut attempt_history = objective.attempt_history.clone();
    let Some(run) = latest_run else {
        return (last_attempt, attempt_history);
    };
    let Some(run_id) = run.get("run_id").and_then(Value::as_str) else {
        return (last_attempt, attempt_history);
    };

    if let Some(attempt) =
        last_attempt.as_mut().filter(|attempt| attempt.run_id.as_deref() == Some(run_id))
    {
        reconcile_attempt_with_run(attempt, run);
    }
    if let Some(attempt) =
        attempt_history.iter_mut().find(|attempt| attempt.run_id.as_deref() == Some(run_id))
    {
        reconcile_attempt_with_run(attempt, run);
    }
    (last_attempt, attempt_history)
}

/// Persists the run-reconciled attempt records back onto the objective; used
/// before archiving so the stored snapshot keeps the terminal run outcome.
fn reconcile_objective_attempts_with_latest_run(
    objective: &mut ObjectiveRecord,
    latest_run: Option<&Value>,
) {
    let (last_attempt, attempt_history) = objective_attempts_for_view(objective, latest_run);
    objective.last_attempt = last_attempt;
    objective.attempt_history = attempt_history;
}

/// Synthesizes a last-run view from the stored attempt when no cron run is
/// available anymore (for example after the backing job was removed), so
/// archived objectives keep showing their final outcome.
fn preserved_run_from_objective_attempt(objective: &ObjectiveRecord) -> Option<Value> {
    let attempt = objective.last_attempt.as_ref()?;
    let run_id = attempt.run_id.as_deref()?.trim();
    if run_id.is_empty() {
        return None;
    }
    Some(json!({
        "routine_id": objective.automation.routine_id.clone(),
        "run_id": run_id,
        "status": attempt.status.clone(),
        "outcome_kind": attempt.outcome_kind.clone(),
        "outcome_message": attempt.summary.clone(),
        "session_id": attempt.session_id.clone(),
        "orchestrator_run_id": Value::Null,
        "started_at_unix_ms": attempt.created_at_unix_ms,
        "finished_at_unix_ms": attempt.completed_at_unix_ms,
        "source": "objective_attempt_history",
    }))
}

/// Copies the run's status/outcome fields onto the attempt; session id and
/// completion time are only filled in when the attempt lacks them so manually
/// recorded data wins.
fn reconcile_attempt_with_run(attempt: &mut ObjectiveAttemptRecord, run: &Value) {
    if let Some(status) =
        run.get("status").and_then(Value::as_str).filter(|value| !value.is_empty())
    {
        attempt.status = status.to_owned();
    }
    if let Some(outcome_kind) =
        run.get("outcome_kind").and_then(Value::as_str).filter(|value| !value.is_empty())
    {
        attempt.outcome_kind = Some(outcome_kind.to_owned());
    }
    if let Some(message) =
        run.get("outcome_message").and_then(Value::as_str).filter(|value| !value.is_empty())
    {
        attempt.summary = message.to_owned();
    }
    if attempt.session_id.is_none() {
        attempt.session_id = run
            .get("session_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
    }
    if attempt.completed_at_unix_ms.is_none() {
        attempt.completed_at_unix_ms = run.get("finished_at_unix_ms").and_then(Value::as_i64);
    }
}

/// Loads the live cron job linked to the objective; when the job no longer
/// exists, falls back to a snapshot rebuilt from the stored automation
/// binding so the view keeps its routine context.
async fn load_objective_routine(
    state: &AppState,
    objective: &ObjectiveRecord,
) -> Result<Option<Value>, Response> {
    let Some(routine_id) = objective.automation.routine_id.as_ref() else {
        return Ok(None);
    };
    let Some(job) =
        state.runtime.cron_job(routine_id.clone()).await.map_err(runtime_status_response)?
    else {
        return Ok(objective_routine_snapshot_from_binding(objective));
    };
    Ok(Some(json!({
        "job_id": job.job_id,
        "name": job.name,
        "prompt": job.prompt,
        "enabled": cron::visible_cron_job_enabled(&job),
        "channel": job.channel,
        "session_key": job.session_key,
        "session_label": job.session_label,
        "schedule_type": job.schedule_type.as_str(),
        "schedule_payload": serde_json::from_str::<Value>(job.schedule_payload_json.as_str())
            .unwrap_or_else(|_| json!({ "raw": job.schedule_payload_json })),
        "next_run_at_unix_ms": cron::visible_next_run_at_unix_ms(&job),
        "last_run_at_unix_ms": job.last_run_at_unix_ms,
        "queued_run": job.queued_run,
    })))
}

/// Rebuilds a routine view from the automation binding alone; `source` marks
/// it as a snapshot so the console can distinguish it from a live job.
fn objective_routine_snapshot_from_binding(objective: &ObjectiveRecord) -> Option<Value> {
    let routine_id = objective.automation.routine_id.as_ref()?;
    Some(json!({
        "job_id": routine_id,
        "name": objective.name.clone(),
        "prompt": objective.prompt.clone(),
        "enabled": objective.automation.enabled,
        "channel": objective.channel.clone(),
        "session_key": objective.workspace.session_key.clone(),
        "session_label": objective.workspace.session_label.clone(),
        "schedule_type": objective.automation.schedule_type.clone(),
        "schedule_payload": serde_json::from_str::<Value>(objective.automation.schedule_payload_json.as_str())
            .unwrap_or_else(|_| json!({ "raw": objective.automation.schedule_payload_json })),
        "next_run_at_unix_ms": Value::Null,
        "last_run_at_unix_ms": Value::Null,
        "queued_run": Value::Null,
        "archived_snapshot": objective.state == ObjectiveState::Archived,
        "source": "objective_automation_binding",
    }))
}

/// Fetches the most recent cron run for the objective's routine, joined with
/// its routine run metadata.
async fn latest_objective_run(
    state: &AppState,
    objective: &ObjectiveRecord,
) -> Result<Option<Value>, Response> {
    let Some(routine_id) = objective.automation.routine_id.as_ref() else {
        return Ok(None);
    };
    let (runs, _) = state
        .runtime
        .list_cron_runs(Some(routine_id.clone()), None, Some(1))
        .await
        .map_err(runtime_status_response)?;
    let Some(run) = runs.last() else {
        return Ok(None);
    };
    let metadata = state
        .routines
        .find_run_metadata(run.run_id.as_str())
        .map_err(routine_registry_error_response)?;
    Ok(Some(join_run_metadata(
        routine_id.as_str(),
        run,
        metadata.as_ref(),
        Some(&objective.automation.approval_policy),
        Some(unix_ms_now().map_err(internal_console_error)?),
    )))
}

/// Maps objective state plus last-run status onto the console health badge.
/// `next_run_at_unix_ms` is surfaced only for active, enabled objectives so
/// paused or terminal objectives never advertise a future run.
fn compute_objective_health(
    objective: &ObjectiveRecord,
    routine: Option<&Value>,
    last_run: Option<&Value>,
) -> Value {
    let status = last_run
        .and_then(|entry| entry.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("never_run");
    let health = match objective.state {
        ObjectiveState::Archived => "archived",
        ObjectiveState::Cancelled => "cancelled",
        ObjectiveState::Paused => "paused",
        ObjectiveState::Draft => "draft",
        ObjectiveState::Active => match status {
            "succeeded" => "healthy",
            "failed" => "attention",
            "running" => "running",
            "queued" => "queued",
            "skipped" => "degraded",
            _ => "active",
        },
    };
    let next_run_at_unix_ms = if objective.state == ObjectiveState::Active
        && objective.automation.enabled
    {
        routine.and_then(|entry| entry.get("next_run_at_unix_ms")).cloned().unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    json!({
        "state": health,
        "last_run_status": status,
        "next_run_at_unix_ms": next_run_at_unix_ms,
    })
}

/// Creates or updates the backing cron job. Objective jobs are pinned to
/// forbid-concurrency, no-retry, skip-misfire, zero-jitter policies so an
/// objective can never stack overlapping or replayed runs.
async fn persist_objective_job(
    state: &AppState,
    existing_routine_id: Option<String>,
    request: ObjectiveJobUpsert,
) -> Result<CronJobRecord, Response> {
    if let Some(routine_id) = existing_routine_id {
        state
            .runtime
            .update_cron_job(
                routine_id,
                CronJobUpdatePatch {
                    name: Some(request.name),
                    prompt: Some(request.prompt),
                    owner_principal: Some(request.owner_principal),
                    channel: Some(request.channel),
                    session_key: Some(request.session_key),
                    session_label: Some(request.session_label),
                    workdir: Some(None),
                    schedule_type: Some(request.schedule_type),
                    schedule_payload_json: Some(request.schedule_payload_json),
                    enabled: Some(request.enabled),
                    concurrency_policy: Some(CronConcurrencyPolicy::Forbid),
                    retry_policy: Some(CronRetryPolicy { max_attempts: 0, backoff_ms: 0 }),
                    misfire_policy: Some(CronMisfirePolicy::Skip),
                    jitter_ms: Some(0),
                    next_run_at_unix_ms: Some(request.next_run_at_unix_ms),
                    queued_run: Some(false),
                },
            )
            .await
            .map_err(runtime_status_response)
    } else {
        state
            .runtime
            .create_cron_job(CronJobCreateRequest {
                job_id: request.routine_id,
                name: request.name,
                prompt: request.prompt,
                owner_principal: request.owner_principal,
                channel: request.channel,
                session_key: request.session_key,
                session_label: request.session_label,
                workdir: None,
                schedule_type: request.schedule_type,
                schedule_payload_json: request.schedule_payload_json,
                enabled: request.enabled,
                concurrency_policy: CronConcurrencyPolicy::Forbid,
                retry_policy: CronRetryPolicy { max_attempts: 0, backoff_ms: 0 },
                misfire_policy: CronMisfirePolicy::Skip,
                jitter_ms: 0,
                next_run_at_unix_ms: request.next_run_at_unix_ms,
            })
            .await
            .map_err(runtime_status_response)
    }
}

/// Mirrors the automation binding into the routine registry so routine-level
/// tooling (delivery, quiet hours, approvals) sees objective routines.
///
/// # Panics
/// Panics when the binding has no routine id; upsert always constructs the
/// binding with one, so a missing id is a programming error.
fn persist_objective_routine_metadata(
    state: &AppState,
    automation: &ObjectiveAutomationBinding,
) -> Result<(), crate::routines::RoutineRegistryError> {
    let routine_id = automation
        .routine_id
        .clone()
        .expect("objective automation bindings are always constructed with a routine id");
    state.routines.upsert_routine(crate::routines::RoutineMetadataUpsert {
        routine_id,
        trigger_kind: automation.trigger_kind,
        trigger_payload_json: automation.schedule_payload_json.clone(),
        execution: automation.execution.clone(),
        delivery: automation.delivery.clone(),
        quiet_hours: automation.quiet_hours.clone(),
        cooldown_ms: automation.cooldown_ms,
        approval_policy: automation.approval_policy.clone(),
        template_id: automation.template_id.clone(),
    })?;
    Ok(())
}

async fn rollback_created_objective_companions(
    state: &AppState,
    routine_id: &str,
) -> Result<(), Response> {
    let cron_result =
        state.runtime.delete_cron_job(routine_id.to_owned()).await.map_err(runtime_status_response);
    let routine_result = state
        .routines
        .delete_routine(routine_id)
        .map(|_| ())
        .map_err(routine_registry_error_response);
    match (cron_result, routine_result) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}

/// Toggles the backing cron job for pause/resume, recomputing the next run
/// time for the new enabled state.
///
/// # Errors
/// Returns not-found when the linked job is missing: pause/resume operate on
/// live automation, unlike the archive path which tolerates a removed job.
async fn set_objective_job_enabled(
    state: &AppState,
    objective: &ObjectiveRecord,
    enabled: bool,
) -> Result<(), Response> {
    let Some(routine_id) = objective.automation.routine_id.as_ref() else {
        return Ok(());
    };
    let job = state
        .runtime
        .cron_job(routine_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "objective routine not found: {routine_id}"
            )))
        })?;
    let next_run_at_unix_ms = cron::next_run_at_for_enabled_state(
        &job,
        enabled,
        crate::gateway::current_unix_ms_status().map_err(runtime_status_response)?,
    )
    .map_err(runtime_status_response)?;
    state
        .runtime
        .update_cron_job(
            routine_id.clone(),
            CronJobUpdatePatch {
                enabled: Some(enabled),
                next_run_at_unix_ms: Some(next_run_at_unix_ms),
                ..CronJobUpdatePatch::default()
            },
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(())
}

/// Best-effort job disable used by archive: a missing job is fine because
/// archiving must succeed even after the automation was already deleted.
async fn disable_objective_job_for_archive(
    state: &AppState,
    objective: &ObjectiveRecord,
) -> Result<(), Response> {
    let Some(routine_id) = objective.automation.routine_id.as_ref() else {
        return Ok(());
    };
    let Some(job) =
        state.runtime.cron_job(routine_id.clone()).await.map_err(runtime_status_response)?
    else {
        return Ok(());
    };
    let next_run_at_unix_ms = cron::next_run_at_for_enabled_state(
        &job,
        false,
        crate::gateway::current_unix_ms_status().map_err(runtime_status_response)?,
    )
    .map_err(runtime_status_response)?;
    state
        .runtime
        .update_cron_job(
            routine_id.clone(),
            CronJobUpdatePatch {
                enabled: Some(false),
                next_run_at_unix_ms: Some(next_run_at_unix_ms),
                ..CronJobUpdatePatch::default()
            },
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(())
}

/// Dispatches the objective's routine immediately (the `fire` action) and
/// records manual-trigger run metadata for the spawned run.
///
/// # Errors
/// Returns failed-precondition when the objective has no linked automation,
/// not-found when the linked job is missing, and the mapped runtime error
/// when dispatch fails.
async fn trigger_objective_now(
    state: &AppState,
    objective: &ObjectiveRecord,
) -> Result<Value, Response> {
    let routine_id = objective.automation.routine_id.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "objective is missing linked automation",
        ))
    })?;
    let job = state
        .runtime
        .cron_job(routine_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("linked routine job not found"))
        })?;
    let outcome = cron::trigger_job_now(
        Arc::clone(&state.runtime),
        state.auth.clone(),
        state.grpc_url.clone(),
        job,
        Arc::clone(&state.scheduler_wake),
    )
    .await
    .map_err(runtime_status_response)?;
    if let Some(run_id) = outcome.run_id.as_ref() {
        state
            .routines
            .upsert_run_metadata(RoutineRunMetadataUpsert {
                run_id: run_id.clone(),
                routine_id,
                trigger_kind: RoutineTriggerKind::Manual,
                trigger_reason: Some("objective fire".to_owned()),
                trigger_payload_json: json!({ "source": "objective" }).to_string(),
                trigger_dedupe_key: None,
                execution: objective.automation.execution.clone(),
                delivery: objective.automation.delivery.clone(),
                dispatch_mode: RoutineDispatchMode::Normal,
                source_run_id: None,
                outcome_override: Some(default_outcome_from_cron_status(outcome.status)),
                outcome_message: Some(outcome.message.clone()),
                output_delivered: Some(true),
                skip_reason: None,
                delivery_reason: None,
                approval_note: None,
                safety_note: None,
            })
            .map_err(routine_registry_error_response)?;
    }
    Ok(json!({
        "run_id": outcome.run_id,
        "status": outcome.status.as_str(),
        "message": outcome.message,
    }))
}

/// Applies the `fire` action: dispatches the routine immediately and records
/// the resulting attempt and lifecycle event on the objective.
async fn apply_fire_action(
    state: &AppState,
    objective: &mut ObjectiveRecord,
    reason: Option<String>,
) -> Result<(), Response> {
    if matches!(objective.state, ObjectiveState::Cancelled | ObjectiveState::Archived) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "cancelled or archived objectives cannot be fired",
        )));
    }
    if !objective.automation.enabled {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "objective automation is paused or disabled",
        )));
    }
    let from_state = objective.state;
    let now_unix_ms = unix_ms_now().map_err(internal_console_error)?;
    // Dispatch before mutating state so a failed trigger leaves the
    // objective untouched.
    let outcome = trigger_objective_now(state, objective).await?;
    objective.state = ObjectiveState::Active;
    // Seed an attempt from the dispatch outcome so the console shows the run
    // immediately; later reads reconcile it with the real run status.
    if let Some(run_id) = outcome.get("run_id").and_then(Value::as_str) {
        objective.linked_run_ids.push(run_id.to_owned());
        let attempt = ObjectiveAttemptRecord {
            attempt_id: Ulid::new().to_string(),
            run_id: Some(run_id.to_owned()),
            session_id: None,
            status: outcome.get("status").and_then(Value::as_str).unwrap_or("scheduled").to_owned(),
            outcome_kind: outcome.get("status").and_then(Value::as_str).map(ToOwned::to_owned),
            summary: outcome
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("objective dispatched")
                .to_owned(),
            learned: None,
            recommended_next_step: objective.next_recommended_step.clone(),
            created_at_unix_ms: now_unix_ms,
            completed_at_unix_ms: None,
        };
        objective.last_attempt = Some(attempt.clone());
        objective.attempt_history.push(attempt);
    }
    objective.lifecycle_history.push(ObjectiveLifecycleRecord {
        event_id: Ulid::new().to_string(),
        action: "fire".to_owned(),
        from_state: Some(from_state),
        to_state: ObjectiveState::Active,
        reason,
        run_id: outcome.get("run_id").and_then(Value::as_str).map(ToOwned::to_owned),
        occurred_at_unix_ms: now_unix_ms,
    });
    Ok(())
}

/// Applies the `pause` action: disables automation and records the
/// lifecycle event; any state may be paused.
async fn apply_pause_action(
    state: &AppState,
    objective: &mut ObjectiveRecord,
    reason: Option<String>,
) -> Result<(), Response> {
    let from_state = objective.state;
    objective.state = ObjectiveState::Paused;
    objective.automation.enabled = false;
    set_objective_job_enabled(state, objective, false).await?;
    objective.lifecycle_history.push(ObjectiveLifecycleRecord {
        event_id: Ulid::new().to_string(),
        action: "pause".to_owned(),
        from_state: Some(from_state),
        to_state: ObjectiveState::Paused,
        reason,
        run_id: None,
        occurred_at_unix_ms: unix_ms_now().map_err(internal_console_error)?,
    });
    Ok(())
}

/// Applies the `resume` action: re-enables automation and reactivates the
/// objective; archived objectives stay archived.
async fn apply_resume_action(
    state: &AppState,
    objective: &mut ObjectiveRecord,
    reason: Option<String>,
) -> Result<(), Response> {
    if objective.state == ObjectiveState::Archived {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "archived objectives cannot be resumed",
        )));
    }
    let from_state = objective.state;
    objective.state = ObjectiveState::Active;
    objective.automation.enabled = true;
    set_objective_job_enabled(state, objective, true).await?;
    objective.lifecycle_history.push(ObjectiveLifecycleRecord {
        event_id: Ulid::new().to_string(),
        action: "resume".to_owned(),
        from_state: Some(from_state),
        to_state: ObjectiveState::Active,
        reason,
        run_id: None,
        occurred_at_unix_ms: unix_ms_now().map_err(internal_console_error)?,
    });
    Ok(())
}

/// Applies the `cancel` action: disables automation, requests best-effort
/// cancellation of the latest linked run, and records the lifecycle event.
async fn apply_cancel_action(
    state: &AppState,
    objective: &mut ObjectiveRecord,
    reason: Option<String>,
) -> Result<(), Response> {
    let from_state = objective.state;
    objective.state = ObjectiveState::Cancelled;
    objective.automation.enabled = false;
    set_objective_job_enabled(state, objective, false).await?;
    // Cancelling the most recent linked run is best-effort: the objective
    // transitions to cancelled even when no run is active anymore.
    if let Some(run_id) = objective.linked_run_ids.last() {
        let _ = state
            .runtime
            .request_orchestrator_cancel(OrchestratorCancelRequest {
                run_id: run_id.clone(),
                reason: reason.clone().unwrap_or_else(|| "objective cancelled".to_owned()),
            })
            .await;
    }
    objective.lifecycle_history.push(ObjectiveLifecycleRecord {
        event_id: Ulid::new().to_string(),
        action: "cancel".to_owned(),
        from_state: Some(from_state),
        to_state: ObjectiveState::Cancelled,
        reason,
        run_id: objective.linked_run_ids.last().cloned(),
        occurred_at_unix_ms: unix_ms_now().map_err(internal_console_error)?,
    });
    Ok(())
}

/// Applies the `archive` action: freezes the objective with its terminal run
/// outcome and disables the backing job if it still exists.
async fn apply_archive_action(
    state: &AppState,
    objective: &mut ObjectiveRecord,
    reason: Option<String>,
) -> Result<(), Response> {
    let from_state = objective.state;
    // Fold the latest run status into attempt history before archiving so
    // the stored snapshot keeps the terminal outcome even if the cron job
    // and its run history are removed later.
    let latest_run = latest_objective_run(state, objective).await?;
    reconcile_objective_attempts_with_latest_run(objective, latest_run.as_ref());
    let now_unix_ms = unix_ms_now().map_err(internal_console_error)?;
    objective.state = ObjectiveState::Archived;
    objective.automation.enabled = false;
    objective.archived_at_unix_ms = Some(now_unix_ms);
    disable_objective_job_for_archive(state, objective).await?;
    objective.lifecycle_history.push(ObjectiveLifecycleRecord {
        event_id: Ulid::new().to_string(),
        action: "archive".to_owned(),
        from_state: Some(from_state),
        to_state: ObjectiveState::Archived,
        reason,
        run_id: objective.last_attempt.as_ref().and_then(|attempt| attempt.run_id.clone()),
        occurred_at_unix_ms: now_unix_ms,
    });
    Ok(())
}

/// Dry-runs every workspace write a lifecycle action would perform so a
/// malformed managed block fails the request before any side effect.
async fn preflight_objective_workspace_projection(
    state: &AppState,
    objective: &ObjectiveRecord,
) -> Result<(), Response> {
    validate_objective_document_projection(state, objective).await?;
    validate_owner_objective_blocks_projection(state, objective).await
}

/// Validates that the objective's own workspace document accepts a
/// managed-block sync without writing anything.
async fn validate_objective_document_projection(
    state: &AppState,
    objective: &ObjectiveRecord,
) -> Result<(), Response> {
    let current = state
        .runtime
        .workspace_document_by_path(
            objective.owner_principal.clone(),
            objective.channel.clone(),
            None,
            objective.workspace.workspace_document_path.clone(),
            false,
        )
        .await
        .map_err(runtime_status_response)?;
    let content = current
        .as_ref()
        .map(|entry| entry.content_text.as_str())
        .unwrap_or_else(|| objective_document_heading(objective.kind));
    sync_workspace_managed_block(content, &objective_record_block(objective))
        .map_err(objective_workspace_error_response)?;
    Ok(())
}

/// Validates the owner-wide projection docs (focus, heartbeats, inbox) with
/// the pending objective substituted in, again without writing anything.
async fn validate_owner_objective_blocks_projection(
    state: &AppState,
    objective: &ObjectiveRecord,
) -> Result<(), Response> {
    let mut objectives =
        state.objectives.list_objectives().map_err(objective_registry_error_response)?;
    // Substitute the not-yet-persisted objective so the preflight sees the
    // exact document state the post-action projection would produce.
    if let Some(existing) =
        objectives.iter_mut().find(|entry| entry.objective_id == objective.objective_id)
    {
        *existing = objective.clone();
    } else {
        objectives.push(objective.clone());
    }
    let owner_objectives = objectives
        .into_iter()
        .filter(|entry| entry.owner_principal == objective.owner_principal)
        .collect::<Vec<_>>();
    for (path, update) in owner_objective_block_updates(&owner_objectives) {
        let current = state
            .runtime
            .workspace_document_by_path(
                objective.owner_principal.clone(),
                objective.channel.clone(),
                None,
                path.to_owned(),
                false,
            )
            .await
            .map_err(runtime_status_response)?;
        let existing_content =
            current.as_ref().map(|entry| entry.content_text.as_str()).unwrap_or_default();
        sync_workspace_managed_block(existing_content, &update)
            .map_err(objective_workspace_error_response)?;
    }
    Ok(())
}

/// Pure lifecycle transition used for preflight: validates the action
/// against the current state and applies the target state and automation
/// flag without touching history or any external system.
///
/// Must stay side-effect free; the test
/// `lifecycle_projection_is_pure_before_side_effects` pins this, and the
/// apply_* functions re-run the real mutation with history and external
/// effects afterwards.
///
/// # Errors
/// Returns failed-precondition for transitions the lifecycle rules forbid
/// and invalid-argument for unknown actions, mirroring the handler errors.
fn apply_lifecycle_workspace_projection(
    action: &str,
    objective: &mut ObjectiveRecord,
) -> Result<(), tonic::Status> {
    match action {
        "fire" => {
            if matches!(objective.state, ObjectiveState::Cancelled | ObjectiveState::Archived) {
                return Err(tonic::Status::failed_precondition(
                    "cancelled or archived objectives cannot be fired",
                ));
            }
            if !objective.automation.enabled {
                return Err(tonic::Status::failed_precondition(
                    "objective automation is paused or disabled",
                ));
            }
            objective.state = ObjectiveState::Active;
        }
        "pause" => {
            objective.state = ObjectiveState::Paused;
            objective.automation.enabled = false;
        }
        "resume" => {
            if objective.state == ObjectiveState::Archived {
                return Err(tonic::Status::failed_precondition(
                    "archived objectives cannot be resumed",
                ));
            }
            objective.state = ObjectiveState::Active;
            objective.automation.enabled = true;
        }
        "cancel" => {
            objective.state = ObjectiveState::Cancelled;
            objective.automation.enabled = false;
        }
        "archive" => {
            objective.state = ObjectiveState::Archived;
            objective.automation.enabled = false;
        }
        _ => {
            return Err(tonic::Status::invalid_argument(
                "action must be one of fire|pause|resume|cancel|archive",
            ));
        }
    }
    Ok(())
}

/// Start actions must not dispatch or re-enable automation while the
/// workspace projection is broken, so they preflight it.
fn lifecycle_action_requires_workspace_preflight(action: &str) -> bool {
    matches!(action, "fire" | "resume")
}

/// Stop actions always apply; a failed projection afterwards becomes a
/// response warning instead of an error.
fn lifecycle_action_tolerates_workspace_projection_failure(action: &str) -> bool {
    matches!(action, "pause" | "cancel" | "archive")
}

/// Rewrites the objective's own document plus the owner-wide projection docs
/// after a mutation so workspace views stay in sync with the registry.
async fn project_objective_workspace(
    state: &AppState,
    owner_principal: &str,
    channel: Option<&str>,
    _session_key: Option<&str>,
    objective: &ObjectiveRecord,
) -> Result<(), Response> {
    write_objective_document(state, owner_principal, channel, objective).await?;
    sync_owner_objective_blocks(state, owner_principal, channel).await
}

/// Syncs the objective-record managed block into the objective's workspace
/// document, creating the document with a kind-specific heading on first
/// write; manual notes outside the managed block are preserved.
async fn write_objective_document(
    state: &AppState,
    owner_principal: &str,
    channel: Option<&str>,
    objective: &ObjectiveRecord,
) -> Result<(), Response> {
    let current = state
        .runtime
        .workspace_document_by_path(
            owner_principal.to_owned(),
            channel.map(ToOwned::to_owned),
            None,
            objective.workspace.workspace_document_path.clone(),
            false,
        )
        .await
        .map_err(runtime_status_response)?;
    let content = current
        .as_ref()
        .map(|entry| entry.content_text.as_str())
        .unwrap_or_else(|| objective_document_heading(objective.kind));
    let next_content = sync_workspace_managed_block(content, &objective_record_block(objective))
        .map_err(objective_workspace_error_response)?
        .content_text;
    state
        .runtime
        .upsert_workspace_document(WorkspaceDocumentWriteRequest {
            document_id: current.as_ref().map(|entry| entry.document_id.clone()),
            principal: owner_principal.to_owned(),
            channel: channel.map(ToOwned::to_owned),
            agent_id: None,
            session_id: None,
            path: objective.workspace.workspace_document_path.clone(),
            title: Some(objective.name.clone()),
            content_text: next_content,
            template_id: None,
            template_version: None,
            template_content_hash: None,
            source_memory_id: None,
            manual_override: false,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(())
}

/// Rewrites the owner-wide projection docs from the full set of the owner's
/// objectives; each doc gets its managed block replaced wholesale.
async fn sync_owner_objective_blocks(
    state: &AppState,
    owner_principal: &str,
    channel: Option<&str>,
) -> Result<(), Response> {
    let objectives = state
        .objectives
        .list_objectives()
        .map_err(objective_registry_error_response)?
        .into_iter()
        .filter(|entry| entry.owner_principal == owner_principal)
        .collect::<Vec<_>>();
    for (path, update) in owner_objective_block_updates(&objectives) {
        sync_owner_block(state, owner_principal, channel, path, update).await?;
    }
    Ok(())
}

/// Applies one managed-block update to one workspace document, creating the
/// document when it does not exist yet.
async fn sync_owner_block(
    state: &AppState,
    owner_principal: &str,
    channel: Option<&str>,
    path: &str,
    update: WorkspaceManagedBlockUpdate,
) -> Result<(), Response> {
    let current = state
        .runtime
        .workspace_document_by_path(
            owner_principal.to_owned(),
            channel.map(ToOwned::to_owned),
            None,
            path.to_owned(),
            false,
        )
        .await
        .map_err(runtime_status_response)?;
    let existing_content =
        current.as_ref().map(|entry| entry.content_text.as_str()).unwrap_or_default();
    let next_content = sync_workspace_managed_block(existing_content, &update)
        .map_err(objective_workspace_error_response)?
        .content_text;
    state
        .runtime
        .upsert_workspace_document(WorkspaceDocumentWriteRequest {
            document_id: current.as_ref().map(|entry| entry.document_id.clone()),
            principal: owner_principal.to_owned(),
            channel: channel.map(ToOwned::to_owned),
            agent_id: None,
            session_id: None,
            path: path.to_owned(),
            title: None,
            content_text: next_content,
            template_id: None,
            template_version: None,
            template_content_hash: None,
            source_memory_id: None,
            manual_override: false,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(())
}

fn managed_entry(entry_id: &str, content: String) -> WorkspaceManagedEntry {
    WorkspaceManagedEntry { entry_id: entry_id.to_owned(), label: "objective".to_owned(), content }
}

fn objective_document_heading(kind: ObjectiveKind) -> &'static str {
    if kind == ObjectiveKind::Heartbeat {
        "# Heartbeat Objective\n\nManual notes outside the managed block are preserved.\n"
    } else {
        "# Objective\n\nManual notes outside the managed block are preserved.\n"
    }
}

fn objective_record_block(objective: &ObjectiveRecord) -> WorkspaceManagedBlockUpdate {
    WorkspaceManagedBlockUpdate {
        block_id: "objective-record".to_owned(),
        heading: format!("{} Summary", objective.name),
        entries: vec![
            managed_entry("objective", format!("[{}] {}", objective.kind.as_str(), objective.name)),
            managed_entry("state", format!("state: {}", objective.state.as_str())),
            managed_entry(
                "focus",
                objective
                    .current_focus
                    .clone()
                    .unwrap_or_else(|| "No current focus recorded.".to_owned()),
            ),
            managed_entry(
                "success",
                objective
                    .success_criteria
                    .clone()
                    .unwrap_or_else(|| "No success criteria recorded.".to_owned()),
            ),
            managed_entry(
                "next_step",
                objective
                    .next_recommended_step
                    .clone()
                    .unwrap_or_else(|| "No next recommended step recorded.".to_owned()),
            ),
        ],
    }
}

/// Computes the managed-block updates projected into the owner's shared
/// workspace docs. Per-doc state filters decide visibility: the focus doc
/// hides archived/cancelled objectives, the heartbeat doc lists heartbeats
/// of any state, and the inbox lists draft/active/paused objectives.
fn owner_objective_block_updates(
    objectives: &[ObjectiveRecord],
) -> Vec<(&'static str, WorkspaceManagedBlockUpdate)> {
    vec![
        (
            "context/current-focus.md",
            WorkspaceManagedBlockUpdate {
                block_id: OBJECTIVE_FOCUS_BLOCK_ID.to_owned(),
                heading: "Objective Focus".to_owned(),
                entries: objectives
                    .iter()
                    .filter(|entry| {
                        !matches!(entry.state, ObjectiveState::Archived | ObjectiveState::Cancelled)
                    })
                    .map(|entry| {
                        managed_entry(
                            entry.objective_id.as_str(),
                            format!(
                                "[{}] {}: {}",
                                entry.state.as_str(),
                                entry.name,
                                entry
                                    .current_focus
                                    .clone()
                                    .unwrap_or_else(|| "No current focus recorded.".to_owned())
                            ),
                        )
                    })
                    .collect(),
            },
        ),
        (
            "HEARTBEAT.md",
            WorkspaceManagedBlockUpdate {
                block_id: OBJECTIVE_HEARTBEAT_BLOCK_ID.to_owned(),
                heading: "Objective Heartbeats".to_owned(),
                entries: objectives
                    .iter()
                    .filter(|entry| entry.kind == ObjectiveKind::Heartbeat)
                    .map(|entry| {
                        managed_entry(
                            entry.objective_id.as_str(),
                            format!(
                                "[{}] {} -> next step: {}",
                                entry.state.as_str(),
                                entry.name,
                                entry
                                    .next_recommended_step
                                    .clone()
                                    .unwrap_or_else(|| "No next step recorded.".to_owned())
                            ),
                        )
                    })
                    .collect(),
            },
        ),
        (
            "projects/inbox.md",
            WorkspaceManagedBlockUpdate {
                block_id: OBJECTIVE_INBOX_BLOCK_ID.to_owned(),
                heading: "Objective Inbox".to_owned(),
                entries: objectives
                    .iter()
                    .filter(|entry| {
                        matches!(
                            entry.state,
                            ObjectiveState::Draft | ObjectiveState::Active | ObjectiveState::Paused
                        )
                    })
                    .map(|entry| {
                        managed_entry(
                            entry.objective_id.as_str(),
                            format!(
                                "{} -> {}",
                                entry.name,
                                entry
                                    .next_recommended_step
                                    .clone()
                                    .unwrap_or_else(|| "Review the objective summary.".to_owned())
                            ),
                        )
                    })
                    .collect(),
            },
        ),
    ]
}

/// Trims the operator-supplied lifecycle reason, dropping blank values.
///
/// # Errors
/// Returns a validation error when the trimmed reason exceeds 500 bytes;
/// this runs before any lifecycle side effect.
#[allow(clippy::result_large_err)]
fn normalize_lifecycle_reason(reason: Option<String>) -> Result<Option<String>, Response> {
    let Some(reason) = reason else {
        return Ok(None);
    };
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > 500 {
        return Err(validation_error_response(
            "reason",
            "too_large",
            "reason must be at most 500 bytes",
        ));
    }
    Ok(Some(trimmed.to_owned()))
}

/// Merges the budget payload over the existing budget: provided fields
/// override, omitted fields keep their stored values. Fields cannot be
/// cleared through this endpoint.
fn normalize_budget(
    budget: Option<ConsoleObjectiveBudgetPayload>,
    existing: Option<&ObjectiveRecord>,
) -> ObjectiveBudget {
    let mut normalized = existing.map(|entry| entry.budget.clone()).unwrap_or_default();
    if let Some(payload) = budget {
        normalized.max_runs = payload.max_runs.or(normalized.max_runs);
        normalized.max_tokens = payload.max_tokens.or(normalized.max_tokens);
        normalized.notes = payload.notes.or(normalized.notes);
    }
    normalized
}

#[allow(clippy::result_large_err)]
fn parse_objective_kind(value: &str) -> Result<ObjectiveKind, Response> {
    ObjectiveKind::from_str(value).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "kind must be one of objective|heartbeat|standing_order|program",
        ))
    })
}

#[allow(clippy::result_large_err)]
fn parse_objective_priority(value: Option<&str>) -> Result<ObjectivePriority, Response> {
    match value.map(str::trim).filter(|entry| !entry.is_empty()) {
        None => Ok(ObjectivePriority::Normal),
        Some(value) => ObjectivePriority::from_str(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "priority must be one of low|normal|high|critical",
            ))
        }),
    }
}

/// Resolves the routine execution config: keeps the stored config on update
/// and only replaces the posture when the payload supplies one.
///
/// # Errors
/// Returns an invalid-argument response for unknown posture values.
#[allow(clippy::result_large_err)]
fn parse_objective_execution_config(
    execution_posture: Option<&str>,
    kind: ObjectiveKind,
    existing: Option<&ObjectiveRecord>,
) -> Result<RoutineExecutionConfig, Response> {
    let mut execution =
        existing.map(|entry| entry.automation.execution.clone()).unwrap_or_else(|| {
            RoutineExecutionConfig {
                execution_posture: default_objective_execution_posture(kind),
                ..RoutineExecutionConfig::default()
            }
        });
    execution.execution_posture =
        match execution_posture.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => RoutineExecutionPosture::from_str(value).ok_or_else(|| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "execution_posture must be one of standard|sensitive_tools",
                ))
            })?,
            None => execution.execution_posture,
        };
    Ok(execution)
}

/// All kinds currently default to the standard posture; the kind parameter
/// is kept so a future kind-specific default stays a one-line change.
fn default_objective_execution_posture(_kind: ObjectiveKind) -> RoutineExecutionPosture {
    RoutineExecutionPosture::Standard
}

#[allow(clippy::result_large_err)]
fn parse_approach_kind(value: &str) -> Result<ObjectiveApproachKind, Response> {
    match value.trim().to_ascii_lowercase().as_str() {
        "attempted" => Ok(ObjectiveApproachKind::Attempted),
        "learned" => Ok(ObjectiveApproachKind::Learned),
        "failed_approach" => Ok(ObjectiveApproachKind::FailedApproach),
        "recommended_next_step" => Ok(ObjectiveApproachKind::RecommendedNextStep),
        "standing_order" => Ok(ObjectiveApproachKind::StandingOrder),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "kind must be one of attempted|learned|failed_approach|recommended_next_step|standing_order",
        ))),
    }
}

/// Resolves the effective owner principal: console callers may only operate
/// on their own objectives, so an explicit `owner_principal` must equal the
/// session principal.
///
/// # Errors
/// Returns permission-denied when a different principal is requested.
#[allow(clippy::result_large_err)]
fn normalize_owner_principal(
    requested: &Option<String>,
    session_principal: &str,
) -> Result<String, Response> {
    match requested.as_deref().map(str::trim) {
        Some("") | None => Ok(session_principal.to_owned()),
        Some(owner_principal) if owner_principal == session_principal => {
            Ok(owner_principal.to_owned())
        }
        Some(_) => Err(runtime_status_response(tonic::Status::permission_denied(
            "owner_principal must match authenticated session principal",
        ))),
    }
}

/// Picks the objective channel: explicit payload value, then the session
/// channel, then the shared system objectives channel.
fn normalize_channel(requested: Option<&str>, session_channel: Option<&str>) -> String {
    requested
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| session_channel.map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_OBJECTIVE_CHANNEL.to_owned())
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|entry| !entry.is_empty()).map(ToOwned::to_owned)
}

/// Enforces objective ownership for the authenticated principal.
///
/// # Errors
/// Returns permission-denied on owner mismatch.
#[allow(clippy::result_large_err)]
fn ensure_objective_owner(objective: &ObjectiveRecord, principal: &str) -> Result<(), Response> {
    if objective.owner_principal != principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "objective owner mismatch for authenticated principal",
        )));
    }
    Ok(())
}

/// Resolves the requested schedule with layered precedence: a natural
/// language phrase wins, then an explicit `schedule_type` payload, then the
/// hourly heartbeat default (heartbeats must always tick), and finally the
/// manual (shadow) schedule for unscheduled objectives.
///
/// # Errors
/// Returns an invalid-argument response when the phrase cannot be parsed,
/// when the schedule payload is incomplete for its type, or when the
/// schedule fails cron normalization.
#[allow(clippy::result_large_err)]
fn resolve_objective_schedule(
    payload: &ConsoleObjectiveUpsertRequest,
    kind: ObjectiveKind,
    timezone_mode: CronTimezoneMode,
) -> Result<ObjectiveScheduleResolution, Response> {
    let natural_language_schedule = payload
        .natural_language_schedule
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(phrase) = natural_language_schedule {
        let preview = natural_language_schedule_preview(
            phrase,
            timezone_mode,
            unix_ms_now().map_err(internal_console_error)?,
        )
        .map_err(routine_registry_error_response)?;
        return Ok(ObjectiveScheduleResolution {
            trigger_kind: RoutineTriggerKind::Schedule,
            schedule_type: parse_schedule_type(preview.schedule_type.as_str())?,
            schedule_payload_json: preview.schedule_payload_json,
            next_run_at_unix_ms: preview.next_run_at_unix_ms,
            is_scheduled: true,
        });
    }
    let schedule_type =
        payload.schedule_type.as_deref().map(str::trim).filter(|value| !value.is_empty());
    if let Some(schedule_type) = schedule_type {
        let schedule = build_console_schedule(schedule_type, payload)?;
        let normalized = cron::normalize_schedule(
            Some(schedule),
            unix_ms_now().map_err(internal_console_error)?,
            timezone_mode,
        )
        .map_err(runtime_status_response)?;
        return Ok(ObjectiveScheduleResolution {
            trigger_kind: RoutineTriggerKind::Schedule,
            schedule_type: normalized.schedule_type,
            schedule_payload_json: normalized.schedule_payload_json,
            next_run_at_unix_ms: normalized.next_run_at_unix_ms,
            is_scheduled: true,
        });
    }
    if kind == ObjectiveKind::Heartbeat {
        let normalized = cron::normalize_schedule(
            Some(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Every as i32,
                spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                    interval_ms: payload.every_interval_ms.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS),
                })),
            }),
            unix_ms_now().map_err(internal_console_error)?,
            timezone_mode,
        )
        .map_err(runtime_status_response)?;
        return Ok(ObjectiveScheduleResolution {
            trigger_kind: RoutineTriggerKind::Schedule,
            schedule_type: normalized.schedule_type,
            schedule_payload_json: normalized.schedule_payload_json,
            next_run_at_unix_ms: normalized.next_run_at_unix_ms,
            is_scheduled: true,
        });
    }
    Ok(ObjectiveScheduleResolution {
        trigger_kind: RoutineTriggerKind::Manual,
        schedule_type: CronScheduleType::At,
        schedule_payload_json: shadow_manual_schedule_payload_json(),
        next_run_at_unix_ms: None,
        is_scheduled: false,
    })
}

/// Builds the protobuf schedule for an explicit `schedule_type`, requiring
/// the matching spec field (`cron_expression`, `every_interval_ms`, or
/// `at_timestamp_rfc3339`).
///
/// # Errors
/// Returns an invalid-argument response for unknown types or missing spec
/// fields.
#[allow(clippy::result_large_err)]
fn build_console_schedule(
    schedule_type_raw: &str,
    payload: &ConsoleObjectiveUpsertRequest,
) -> Result<cron_v1::Schedule, Response> {
    match schedule_type_raw.trim().to_ascii_lowercase().as_str() {
        "cron" => {
            let expression = payload
                .cron_expression
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "cron_expression is required for schedule_type=cron",
                    ))
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Cron as i32,
                spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule {
                    expression: expression.to_owned(),
                })),
            })
        }
        "every" => Ok(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: payload.every_interval_ms.ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "every_interval_ms is required for schedule_type=every",
                    ))
                })?,
            })),
        }),
        "at" => {
            let timestamp = payload
                .at_timestamp_rfc3339
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "at_timestamp_rfc3339 is required for schedule_type=at",
                    ))
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::At as i32,
                spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                    timestamp_rfc3339: timestamp.to_owned(),
                })),
            })
        }
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "schedule_type must be one of cron|every|at",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn parse_schedule_type(value: &str) -> Result<CronScheduleType, Response> {
    match value.trim().to_ascii_lowercase().as_str() {
        "cron" => Ok(CronScheduleType::Cron),
        "every" => Ok(CronScheduleType::Every),
        "at" => Ok(CronScheduleType::At),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "schedule_type must be one of cron|every|at",
        ))),
    }
}

/// Parses the delivery configuration; the mode defaults to same-channel.
///
/// # Errors
/// Returns an invalid-argument response for unknown modes or when
/// `specific_channel` is requested without a delivery channel.
#[allow(clippy::result_large_err)]
fn parse_delivery(
    mode: Option<&str>,
    channel: Option<String>,
) -> Result<RoutineDeliveryConfig, Response> {
    let mode =
        match mode.map(str::trim).filter(|value| !value.is_empty()) {
            None => RoutineDeliveryMode::SameChannel,
            Some("same_channel") => RoutineDeliveryMode::SameChannel,
            Some("specific_channel") => RoutineDeliveryMode::SpecificChannel,
            Some("local_only") => RoutineDeliveryMode::LocalOnly,
            Some("logs_only") => RoutineDeliveryMode::LogsOnly,
            Some(_) => return Err(runtime_status_response(tonic::Status::invalid_argument(
                "delivery_mode must be one of same_channel|specific_channel|local_only|logs_only",
            ))),
        };
    let delivery_channel =
        channel.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty());
    if mode == RoutineDeliveryMode::SpecificChannel && delivery_channel.is_none() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "delivery_channel is required for delivery_mode=specific_channel",
        )));
    }
    Ok(RoutineDeliveryConfig {
        mode,
        channel: delivery_channel,
        failure_mode: None,
        failure_channel: None,
        silent_policy: RoutineSilentPolicy::Noisy,
    })
}

/// Parses optional quiet hours; start and end must be provided together so a
/// half-open window can never be stored.
///
/// # Errors
/// Returns an invalid-argument response when only one bound is provided or
/// when either bound is not a valid `HH:MM` time of day.
#[allow(clippy::result_large_err)]
fn parse_quiet_hours(
    start: Option<&str>,
    end: Option<&str>,
    timezone: Option<String>,
) -> Result<Option<RoutineQuietHours>, Response> {
    let start = start.map(str::trim).filter(|value| !value.is_empty());
    let end = end.map(str::trim).filter(|value| !value.is_empty());
    match (start, end) {
        (None, None) => Ok(None),
        (Some(start), Some(end)) => Ok(Some(RoutineQuietHours {
            start_minute_of_day: parse_minute_of_day(start)?,
            end_minute_of_day: parse_minute_of_day(end)?,
            timezone: normalize_optional_text(timezone.as_deref()),
        })),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "quiet_hours_start and quiet_hours_end must be provided together",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn parse_minute_of_day(value: &str) -> Result<u16, Response> {
    let (hour, minute) = value.split_once(':').ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "quiet hour values must use HH:MM format",
        ))
    })?;
    let hour = hour.parse::<u16>().map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "quiet hour hour component must be numeric",
        ))
    })?;
    let minute = minute.parse::<u16>().map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "quiet hour minute component must be numeric",
        ))
    })?;
    if hour > 23 || minute > 59 {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "quiet hour values must stay within 00:00-23:59",
        )));
    }
    Ok(hour * 60 + minute)
}

#[allow(clippy::result_large_err)]
fn parse_approval_policy(value: Option<&str>) -> Result<RoutineApprovalPolicy, Response> {
    let mode = match value.map(str::trim).filter(|entry| !entry.is_empty()) {
        None | Some("none") => RoutineApprovalMode::None,
        Some("before_enable") => RoutineApprovalMode::BeforeEnable,
        Some("before_first_run") => RoutineApprovalMode::BeforeFirstRun,
        Some(_) => {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "approval_mode must be one of none|before_enable|before_first_run",
            )))
        }
    };
    Ok(RoutineApprovalPolicy { mode })
}

/// Renders the operator-facing markdown digest for the summary endpoint from
/// an already-built objective view.
fn render_objective_summary_markdown(view: &Value) -> String {
    let objective_id = read_string_value(view, "objective_id");
    let kind = read_string_value(view, "kind");
    let name = read_string_value(view, "name");
    let state = read_string_value(view, "state");
    let current_focus =
        view.get("current_focus").and_then(Value::as_str).unwrap_or("No current focus recorded.");
    let success_criteria = view
        .get("success_criteria")
        .and_then(Value::as_str)
        .unwrap_or("No success criteria recorded.");
    let next_step = view
        .get("next_recommended_step")
        .and_then(Value::as_str)
        .unwrap_or("No next recommended step recorded.");
    let health = view
        .get("health")
        .and_then(|entry| entry.get("state"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    format!(
        "# {name}\n\n- objective_id: {objective_id}\n- kind: {kind}\n- state: {state}\n- health: {health}\n- current_focus: {current_focus}\n- success_criteria: {success_criteria}\n- next_recommended_step: {next_step}\n"
    )
}

fn read_string_value(record: &Value, key: &str) -> String {
    record.get(key).and_then(Value::as_str).unwrap_or_default().to_owned()
}

/// Maps registry errors to the wire contract: field validation failures
/// become structured validation errors, everything else an internal status.
fn objective_registry_error_response(error: ObjectiveRegistryError) -> Response {
    match error {
        ObjectiveRegistryError::InvalidField { field, message } => {
            validation_error_response(field, "invalid", message.as_str())
        }
        other => runtime_status_response(tonic::Status::internal(other.to_string())),
    }
}

/// Routine-registry counterpart of [`objective_registry_error_response`].
fn routine_registry_error_response(error: crate::routines::RoutineRegistryError) -> Response {
    match error {
        crate::routines::RoutineRegistryError::InvalidField { field, message } => {
            validation_error_response(field, "invalid", message.as_str())
        }
        other => runtime_status_response(tonic::Status::internal(other.to_string())),
    }
}

/// Maps a managed-block failure to a failed-precondition response with
/// repair instructions, used by both preflight and post-action projection.
fn objective_workspace_error_response(
    error: crate::domain::workspace::WorkspaceManagedBlockError,
) -> Response {
    runtime_status_response(tonic::Status::failed_precondition(format!(
        "objective workspace projection blocked before lifecycle mutation: {error}. Repair the affected Palyra managed block by removing manual edits between PALYRA markers or delete the malformed managed block, then retry the objective command."
    )))
}

/// Wraps unexpected errors as sanitized internal statuses so raw error text
/// never reaches the console.
fn internal_console_error(error: anyhow::Error) -> Response {
    runtime_status_response(tonic::Status::internal(sanitize_http_error_message(
        error.to_string().as_str(),
    )))
}

#[cfg(test)]
mod tests {
    use super::{
        apply_lifecycle_workspace_projection, compute_objective_health,
        default_objective_execution_posture, initial_objective_state,
        lifecycle_action_requires_workspace_preflight,
        lifecycle_action_tolerates_workspace_projection_failure, managed_entry, normalize_budget,
        normalize_lifecycle_reason, objective_attempts_for_view, objective_record_block,
        objective_routine_snapshot_from_binding, owner_objective_block_updates,
        page_objective_records, parse_objective_execution_config, parse_objective_kind,
        parse_objective_priority, preserved_run_from_objective_attempt,
        reconcile_objective_attempts_with_latest_run, render_objective_summary_markdown,
        ConsoleObjectiveBudgetPayload, ConsoleObjectiveRequestKind,
        OBJECTIVE_WORKSPACE_PROJECTION_WARNING,
    };
    use crate::domain::workspace::{
        sync_workspace_managed_block, WorkspaceManagedBlockError, WorkspaceManagedBlockUpdate,
    };
    use crate::objectives::{
        ObjectiveAttemptRecord, ObjectiveAutomationBinding, ObjectiveBudget, ObjectiveContract,
        ObjectiveKind, ObjectivePriority, ObjectiveRecord, ObjectiveState,
        ObjectiveWorkspaceBinding,
    };
    use crate::routines::{
        shadow_manual_schedule_payload_json, RoutineApprovalPolicy, RoutineDeliveryConfig,
        RoutineExecutionConfig, RoutineExecutionPosture, RoutineTriggerKind,
    };
    use serde_json::{json, Value};
    use ulid::Ulid;

    fn sample_objective() -> ObjectiveRecord {
        ObjectiveRecord {
            objective_id: Ulid::new().to_string(),
            kind: ObjectiveKind::Heartbeat,
            state: ObjectiveState::Active,
            name: "Daily heartbeat".to_owned(),
            prompt: "Summarize the current focus every morning.".to_owned(),
            owner_principal: "user:ops".to_owned(),
            channel: Some("console".to_owned()),
            priority: ObjectivePriority::Normal,
            budget: ObjectiveBudget::default(),
            current_focus: Some("Keep the board current.".to_owned()),
            success_criteria: Some("Operators can see the latest summary.".to_owned()),
            contract: ObjectiveContract::default(),
            contract_history: vec![],
            exit_condition: None,
            next_recommended_step: Some("Review the newest summary.".to_owned()),
            standing_order: None,
            workspace: ObjectiveWorkspaceBinding {
                workspace_document_path: "projects/objectives/demo.md".to_owned(),
                session_key: None,
                session_label: None,
                related_document_paths: vec![],
                related_memory_ids: vec![],
                related_session_ids: vec![],
            },
            automation: ObjectiveAutomationBinding {
                routine_id: Some(Ulid::new().to_string()),
                enabled: true,
                trigger_kind: RoutineTriggerKind::Schedule,
                schedule_type: "every".to_owned(),
                schedule_payload_json: shadow_manual_schedule_payload_json(),
                execution: RoutineExecutionConfig::default(),
                delivery: RoutineDeliveryConfig::default(),
                quiet_hours: None,
                cooldown_ms: 0,
                approval_policy: RoutineApprovalPolicy::default(),
                template_id: Some("heartbeat".to_owned()),
            },
            last_attempt: None,
            attempt_history: vec![],
            approach_history: vec![],
            lifecycle_history: vec![],
            linked_run_ids: vec![],
            linked_artifact_paths: vec![],
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            archived_at_unix_ms: None,
        }
    }

    fn sample_objective_with_id(objective_id: &str) -> ObjectiveRecord {
        let mut objective = sample_objective();
        objective.objective_id = objective_id.to_owned();
        objective
    }

    #[test]
    fn kind_parser_accepts_product_terms() {
        assert_eq!(
            parse_objective_kind("standing_order").expect("kind should parse"),
            ObjectiveKind::StandingOrder
        );
    }

    #[test]
    fn priority_parser_defaults_to_normal() {
        assert_eq!(
            parse_objective_priority(None).expect("priority should default"),
            ObjectivePriority::Normal
        );
    }

    #[test]
    fn objective_console_mutation_requests_require_csrf() {
        for request_kind in [
            ConsoleObjectiveRequestKind::Upsert,
            ConsoleObjectiveRequestKind::Lifecycle,
            ConsoleObjectiveRequestKind::Attempt,
            ConsoleObjectiveRequestKind::Approach,
        ] {
            assert!(request_kind.requires_csrf());
        }

        for request_kind in [
            ConsoleObjectiveRequestKind::List,
            ConsoleObjectiveRequestKind::Get,
            ConsoleObjectiveRequestKind::Summary,
        ] {
            assert!(!request_kind.requires_csrf());
        }
    }

    #[test]
    fn objective_record_page_filters_and_pages_before_view_assembly() {
        let first = sample_objective_with_id("objective-a");
        let second = sample_objective_with_id("objective-b");
        let third = sample_objective_with_id("objective-c");
        let mut paused = sample_objective_with_id("objective-d");
        paused.state = ObjectiveState::Paused;
        let mut foreign = sample_objective_with_id("objective-e");
        foreign.owner_principal = "user:other".to_owned();

        let page = page_objective_records(
            vec![foreign, third, paused, second, first],
            "user:ops",
            Some("heartbeat"),
            Some("active"),
            Some("objective-a"),
            1,
        );

        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].objective_id, "objective-b");
        assert_eq!(page.next_after_objective_id.as_deref(), Some("objective-b"));
    }

    #[test]
    fn objective_execution_posture_defaults_to_standard_for_all_kinds() {
        assert_eq!(
            default_objective_execution_posture(ObjectiveKind::Heartbeat),
            RoutineExecutionPosture::Standard
        );
        assert_eq!(
            default_objective_execution_posture(ObjectiveKind::StandingOrder),
            RoutineExecutionPosture::Standard
        );
        assert_eq!(
            default_objective_execution_posture(ObjectiveKind::Objective),
            RoutineExecutionPosture::Standard
        );
        assert_eq!(
            default_objective_execution_posture(ObjectiveKind::Program),
            RoutineExecutionPosture::Standard
        );
    }

    #[test]
    fn objective_execution_posture_can_be_explicitly_standard() {
        let execution =
            parse_objective_execution_config(Some("standard"), ObjectiveKind::Heartbeat, None)
                .expect("explicit standard posture should parse");

        assert_eq!(execution.execution_posture, RoutineExecutionPosture::Standard);
    }

    #[test]
    fn objective_execution_posture_can_be_explicitly_sensitive_tools() {
        let execution = parse_objective_execution_config(
            Some("sensitive_tools"),
            ObjectiveKind::Heartbeat,
            None,
        )
        .expect("explicit sensitive_tools posture should parse");

        assert_eq!(execution.execution_posture, RoutineExecutionPosture::SensitiveTools);
    }

    #[test]
    fn objective_execution_posture_preserves_existing_value_when_omitted() {
        let mut existing = sample_objective();
        existing.automation.execution.execution_posture = RoutineExecutionPosture::Standard;

        let execution =
            parse_objective_execution_config(None, ObjectiveKind::Heartbeat, Some(&existing))
                .expect("omitted posture should preserve existing value");

        assert_eq!(execution.execution_posture, RoutineExecutionPosture::Standard);
    }

    #[test]
    fn budget_merge_overrides_existing_fields() {
        let existing = sample_objective();
        let merged = normalize_budget(
            Some(ConsoleObjectiveBudgetPayload {
                max_runs: Some(7),
                max_tokens: None,
                notes: Some("Watch cost.".to_owned()),
            }),
            Some(&existing),
        );
        assert_eq!(merged.max_runs, Some(7));
        assert_eq!(merged.notes.as_deref(), Some("Watch cost."));
    }

    #[test]
    fn objective_summary_markdown_surfaces_core_fields() {
        let markdown = render_objective_summary_markdown(&json!({
            "objective_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "kind": "heartbeat",
            "name": "Daily heartbeat",
            "state": "active",
            "current_focus": "Keep the board current.",
            "success_criteria": "Operators can see the latest summary.",
            "next_recommended_step": "Review the newest summary.",
            "health": { "state": "healthy" }
        }));
        assert!(markdown.contains("Daily heartbeat"));
        assert!(markdown.contains("health: healthy"));
    }

    #[test]
    fn health_uses_last_run_status_for_active_objectives() {
        let objective = sample_objective();
        let health = compute_objective_health(
            &objective,
            Some(&json!({ "next_run_at_unix_ms": 10 })),
            Some(&json!({ "status": "failed" })),
        );
        assert_eq!(health.get("state").and_then(serde_json::Value::as_str), Some("attention"));
    }

    #[test]
    fn health_hides_next_run_for_paused_objectives() {
        let mut objective = sample_objective();
        objective.state = ObjectiveState::Paused;
        objective.automation.enabled = false;

        let health =
            compute_objective_health(&objective, Some(&json!({ "next_run_at_unix_ms": 10 })), None);

        assert!(health.get("next_run_at_unix_ms").is_some_and(serde_json::Value::is_null));
    }

    #[test]
    fn enabled_objectives_start_active_when_requested() {
        assert_eq!(initial_objective_state(None, Some(true)), ObjectiveState::Active);
        assert_eq!(initial_objective_state(None, Some(false)), ObjectiveState::Draft);

        let mut existing = sample_objective();
        existing.state = ObjectiveState::Paused;
        assert_eq!(initial_objective_state(Some(&existing), Some(true)), ObjectiveState::Paused);
    }

    #[test]
    fn objective_view_reconciles_running_attempt_with_terminal_run() {
        let mut objective = sample_objective();
        let run_id = Ulid::new().to_string();
        let attempt = ObjectiveAttemptRecord {
            attempt_id: Ulid::new().to_string(),
            run_id: Some(run_id.clone()),
            session_id: None,
            status: "running".to_owned(),
            outcome_kind: Some("success_with_output".to_owned()),
            summary: "objective dispatched".to_owned(),
            learned: None,
            recommended_next_step: None,
            created_at_unix_ms: 1,
            completed_at_unix_ms: None,
        };
        objective.last_attempt = Some(attempt.clone());
        objective.attempt_history = vec![attempt];

        let (last_attempt, attempt_history) = objective_attempts_for_view(
            &objective,
            Some(&json!({
                "run_id": run_id,
                "status": "succeeded",
                "outcome_kind": "success_with_output",
                "outcome_message": "heartbeat finished",
                "session_id": "session-1",
                "finished_at_unix_ms": 42
            })),
        );

        let last_attempt = last_attempt.expect("last attempt should be present");
        assert_eq!(last_attempt.status, "succeeded");
        assert_eq!(last_attempt.summary, "heartbeat finished");
        assert_eq!(last_attempt.session_id.as_deref(), Some("session-1"));
        assert_eq!(last_attempt.completed_at_unix_ms, Some(42));
        assert_eq!(attempt_history[0].status, "succeeded");
    }

    #[test]
    fn archive_snapshot_preserves_terminal_attempt_as_last_run() {
        let mut objective = sample_objective();
        let run_id = Ulid::new().to_string();
        let attempt = ObjectiveAttemptRecord {
            attempt_id: Ulid::new().to_string(),
            run_id: Some(run_id.clone()),
            session_id: None,
            status: "running".to_owned(),
            outcome_kind: Some("running".to_owned()),
            summary: "objective dispatched".to_owned(),
            learned: None,
            recommended_next_step: None,
            created_at_unix_ms: 1,
            completed_at_unix_ms: None,
        };
        objective.last_attempt = Some(attempt.clone());
        objective.attempt_history = vec![attempt];

        reconcile_objective_attempts_with_latest_run(
            &mut objective,
            Some(&json!({
                "run_id": run_id,
                "status": "succeeded",
                "outcome_kind": "success_with_output",
                "outcome_message": "PALYRA_HEARTBEAT_OK",
                "session_id": "session-1",
                "finished_at_unix_ms": 42
            })),
        );
        objective.state = ObjectiveState::Archived;

        let preserved =
            preserved_run_from_objective_attempt(&objective).expect("last run should be preserved");
        assert_eq!(preserved.get("status").and_then(serde_json::Value::as_str), Some("succeeded"));
        assert_eq!(
            preserved.get("outcome_message").and_then(serde_json::Value::as_str),
            Some("PALYRA_HEARTBEAT_OK")
        );
        let health = compute_objective_health(&objective, None, Some(&preserved));
        assert_eq!(
            health.get("last_run_status").and_then(serde_json::Value::as_str),
            Some("succeeded")
        );
    }

    #[test]
    fn managed_entry_uses_objective_label() {
        let entry = managed_entry("demo", "Track the next step".to_owned());
        assert_eq!(entry.label, "objective");
    }

    #[test]
    fn lifecycle_projection_is_pure_before_side_effects() {
        let mut objective = sample_objective();
        apply_lifecycle_workspace_projection("pause", &mut objective)
            .expect("pause projection should succeed");
        assert_eq!(objective.state, ObjectiveState::Paused);
        assert!(!objective.automation.enabled);
        assert!(objective.lifecycle_history.is_empty());
    }

    #[test]
    fn stop_lifecycle_actions_do_not_require_workspace_preflight() {
        for action in ["pause", "cancel", "archive"] {
            assert!(
                !lifecycle_action_requires_workspace_preflight(action),
                "{action} must not let malformed workspace blocks block stop side effects"
            );
            assert!(
                lifecycle_action_tolerates_workspace_projection_failure(action),
                "{action} should report projection failures as warnings after the action applies"
            );
        }
        for action in ["fire", "resume"] {
            assert!(
                lifecycle_action_requires_workspace_preflight(action),
                "{action} should still fail before enabling or dispatching automation"
            );
            assert!(
                !lifecycle_action_tolerates_workspace_projection_failure(action),
                "{action} should not hide workspace projection failures"
            );
        }
    }

    #[test]
    fn stop_lifecycle_workspace_projection_warning_is_explicit() {
        assert!(
            OBJECTIVE_WORKSPACE_PROJECTION_WARNING.contains("action was applied"),
            "warning must make clear the stop action was not rolled back"
        );
        assert!(
            OBJECTIVE_WORKSPACE_PROJECTION_WARNING.contains("workspace projection did not update"),
            "warning must make the projection failure explicit"
        );
    }

    #[test]
    fn archive_projection_disables_automation_without_losing_routine_history_link() {
        let mut objective = sample_objective();
        let routine_id = objective.automation.routine_id.clone();
        let run_id = Ulid::new().to_string();
        objective.linked_run_ids.push(run_id.clone());
        apply_lifecycle_workspace_projection("archive", &mut objective)
            .expect("archive projection should succeed");
        assert_eq!(objective.state, ObjectiveState::Archived);
        assert!(!objective.automation.enabled);
        assert_eq!(objective.automation.routine_id, routine_id);
        assert_eq!(objective.linked_run_ids, vec![run_id]);
        assert!(objective.lifecycle_history.is_empty());
    }

    #[test]
    fn archived_objective_routine_snapshot_surfaces_retained_binding_when_job_missing() {
        let mut objective = sample_objective();
        objective.state = ObjectiveState::Archived;
        objective.automation.enabled = false;
        objective.archived_at_unix_ms = Some(42);
        let routine_id = objective
            .automation
            .routine_id
            .clone()
            .expect("sample objective should have a routine");

        let snapshot = objective_routine_snapshot_from_binding(&objective)
            .expect("automation binding should produce a routine snapshot");

        assert_eq!(snapshot.get("job_id").and_then(Value::as_str), Some(routine_id.as_str()));
        assert_eq!(snapshot.get("archived_snapshot").and_then(Value::as_bool), Some(true));
        assert_eq!(
            snapshot.get("source").and_then(Value::as_str),
            Some("objective_automation_binding")
        );
        assert_eq!(
            snapshot.get("schedule_type").and_then(Value::as_str),
            Some(objective.automation.schedule_type.as_str())
        );
    }

    #[test]
    fn lifecycle_reason_is_normalized_before_mutation() {
        assert_eq!(
            normalize_lifecycle_reason(Some("  operator pause  ".to_owned()))
                .expect("reason should normalize")
                .as_deref(),
            Some("operator pause")
        );
        assert!(
            normalize_lifecycle_reason(Some("x".repeat(501))).is_err(),
            "oversized reasons must fail before lifecycle side effects"
        );
    }

    #[test]
    fn objective_projection_rejects_malformed_managed_block() {
        let objective = sample_objective();
        let malformed = "\
# Heartbeat Objective

## Daily heartbeat Summary
<!-- PALYRA:BEGIN objective-record -->
manual edit inside managed block
<!-- PALYRA:END objective-record -->
";
        let error = sync_workspace_managed_block(malformed, &objective_record_block(&objective))
            .expect_err("malformed managed content should block projection");
        assert!(matches!(error, WorkspaceManagedBlockError::MalformedItem { .. }));
    }

    #[test]
    fn owner_projection_blocks_cover_expected_workspace_docs() {
        let objective = sample_objective();
        let updates = owner_objective_block_updates(&[objective]);
        let paths = updates.iter().map(|(path, _)| *path).collect::<Vec<_>>();
        assert_eq!(paths, vec!["context/current-focus.md", "HEARTBEAT.md", "projects/inbox.md"]);
        assert!(
            updates.iter().all(|(_, update): &(&str, WorkspaceManagedBlockUpdate)| {
                !update.block_id.trim().is_empty()
            }),
            "all owner projection updates should target a managed block"
        );
    }
}
