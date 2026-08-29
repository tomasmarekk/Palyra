//! Console HTTP handlers for routines: operator-defined automations that pair
//! a backing cron job (schedule, retries, ownership) with routine metadata
//! (trigger matching, execution posture, delivery, quiet hours, approvals).
//!
//! Route surface: `/console/v1/routines*` (list, get, upsert, import/export,
//! delete, enable, run-now, test-run, dispatch, runs, templates, and natural
//! language schedule preview). Also exposes the `dispatch_*_routines` entry
//! points used by the hook, webhook, and system handlers, plus
//! [`apply_routine_approval_decision`] invoked by the approvals handler when
//! an operator allows a pending routine approval.
//!
//! Response JSON shapes here are part of the `/console/v1` wire contract
//! consumed by `apps/web`; field names, status codes, and error strings must
//! stay byte-identical.

use std::{collections::HashMap, sync::Arc};

use chrono::{TimeZone, Timelike};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::{
    access::lock_access_registry,
    diagnostics::{authorize_console_session, build_page_info},
};

use crate::{
    access_control::FEATURE_ROUTINES_AUTOMATION,
    cron::{self, CronTimezoneMode},
    gateway::{proto::palyra::cron::v1 as cron_v1, validate_cron_job_channel_context},
    journal::{
        ApprovalCreateRequest, ApprovalDecision, ApprovalPolicySnapshot, ApprovalPromptOption,
        ApprovalPromptRecord, ApprovalRecord, ApprovalRiskLevel, ApprovalSubjectType,
        CronConcurrencyPolicy, CronJobCreateRequest, CronJobRecord, CronJobUpdatePatch,
        CronMisfirePolicy, CronRetryPolicy, CronRunFinalizeRequest, CronRunStartRequest,
        CronRunStatus, CronScheduleType,
    },
    routines::{
        apply_routine_execution_governance, build_routine_export_bundle,
        cron_routine_preview_audit_projection, default_outcome_from_cron_status, join_run_metadata,
        natural_language_schedule_preview, normalize_file_watch_trigger_payload,
        preserve_routine_execution_governance, routine_allows_sensitive_tools,
        routine_capability_profile_projection, routine_delivery_preview,
        routine_execution_governance_projection, routine_templates,
        shadow_manual_schedule_payload_json, validate_routine_export_bundle,
        validate_routine_prompt_self_contained, CronRoutinePreviewAuditInput, RoutineApprovalMode,
        RoutineApprovalPolicy, RoutineDeliveryConfig, RoutineDeliveryMode, RoutineDispatchMode,
        RoutineExecutionConfig, RoutineExecutionGovernanceOverrides, RoutineExecutionPosture,
        RoutineExportBundle, RoutineMetadataRecord, RoutineMetadataUpsert, RoutineQuietHours,
        RoutineRegistryError, RoutineRunMetadataUpsert, RoutineRunMode, RoutineRunOutcomeKind,
        RoutineSilentPolicy, RoutineTriggerKind, ROUTINE_TEMPLATE_PACK_VERSION,
    },
    *,
};

const DEFAULT_ROUTINE_CHANNEL: &str = "system:routines";
const DEFAULT_ROUTINE_PAGE_LIMIT: usize = 100;
const MAX_ROUTINE_PAGE_LIMIT: usize = 500;
const ROUTINE_APPROVAL_TIMEOUT_SECONDS: u32 = 900;
const ROUTINE_APPROVAL_DEVICE_ID: &str = "system:routines";
const ROUTINE_APPROVAL_POLICY_ID: &str = "routine.approval.v1";
const DEFAULT_ROUTINE_RETRY_MAX_ATTEMPTS: u32 = 1;
const DEFAULT_ROUTINE_RETRY_BACKOFF_MS: u64 = 1_000;

/// Query filters for `GET /console/v1/routines`; all filters are optional and
/// string matches are case-insensitive.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleRoutineListQuery {
    #[serde(default)]
    after_routine_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    trigger_kind: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    template_id: Option<String>,
}

/// Create-or-update payload for `POST /console/v1/routines`.
///
/// Omitted optional fields fall back to trigger-kind-specific defaults (run
/// mode, execution posture, delivery, approval policy); `routine_id` is
/// generated when absent. Unknown fields are rejected.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineUpsertRequest {
    #[serde(default)]
    routine_id: Option<String>,
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
    workdir: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    trigger_kind: String,
    #[serde(default)]
    trigger_payload: Option<Value>,
    #[serde(default)]
    natural_language_schedule: Option<String>,
    #[serde(default)]
    schedule_type: Option<String>,
    #[serde(default)]
    schedule_timezone: Option<String>,
    #[serde(default)]
    cron_expression: Option<String>,
    #[serde(default)]
    every_interval_ms: Option<u64>,
    #[serde(default)]
    at_timestamp_rfc3339: Option<String>,
    #[serde(default)]
    concurrency_policy: Option<String>,
    #[serde(default)]
    retry_max_attempts: Option<u32>,
    #[serde(default)]
    retry_backoff_ms: Option<u64>,
    #[serde(default)]
    misfire_policy: Option<String>,
    #[serde(default)]
    jitter_ms: Option<u64>,
    #[serde(default)]
    max_runs: Option<u32>,
    #[serde(default)]
    delivery_mode: Option<String>,
    #[serde(default)]
    delivery_channel: Option<String>,
    #[serde(default)]
    delivery_failure_mode: Option<String>,
    #[serde(default)]
    delivery_failure_channel: Option<String>,
    #[serde(default)]
    silent_policy: Option<String>,
    #[serde(default)]
    run_mode: Option<String>,
    #[serde(default)]
    procedure_profile_id: Option<String>,
    #[serde(default)]
    skill_profile_id: Option<String>,
    #[serde(default)]
    provider_profile_id: Option<String>,
    #[serde(default)]
    execution_posture: Option<String>,
    #[serde(default)]
    execution_governance: Option<RoutineExecutionGovernanceOverrides>,
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

/// Body for `POST /console/v1/routines/{routine_id}/enabled`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineEnabledRequest {
    enabled: bool,
}

/// Cursor pagination for `GET /console/v1/routines/{routine_id}/runs`.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleRoutineRunsQuery {
    #[serde(default)]
    after_run_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

/// Body for `POST /console/v1/routines/{routine_id}/dispatch`; trigger kind
/// defaults to `manual` when omitted.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineDispatchRequest {
    #[serde(default)]
    trigger_kind: Option<String>,
    #[serde(default)]
    trigger_reason: Option<String>,
    #[serde(default)]
    trigger_payload: Option<Value>,
    #[serde(default)]
    trigger_dedupe_key: Option<String>,
}

/// Body for `POST /console/v1/routines/{routine_id}/test-run`; setting
/// `source_run_id` replays an archived run's trigger payload instead of the
/// routine's configured one.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineTestRunRequest {
    #[serde(default)]
    source_run_id: Option<String>,
    #[serde(default)]
    trigger_reason: Option<String>,
    #[serde(default)]
    trigger_payload: Option<Value>,
}

/// Body for `POST /console/v1/routines/schedule-preview`: a natural language
/// phrase plus an optional timezone override.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineSchedulePreviewRequest {
    phrase: String,
    #[serde(default)]
    timezone: Option<String>,
}

/// Body for `POST /console/v1/routines/import`: a validated export bundle with
/// optional overrides for the target routine id and enabled state.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineImportRequest {
    export: RoutineExportBundle,
    #[serde(default)]
    routine_id: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

/// Lists the authenticated principal's routines with filters and cursor
/// pagination (`GET /console/v1/routines`).
///
/// # Errors
/// Returns an unauthorized response when console authorization fails, or a
/// mapped runtime/registry error response when loading routines fails.
pub(crate) async fn console_routines_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleRoutineListQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let jobs = synchronize_schedule_routines(&state).await?;
    let routine_views =
        routine_views_for_principal(&state, jobs, session.context.principal.as_str()).await?;
    let limit = query.limit.unwrap_or(DEFAULT_ROUTINE_PAGE_LIMIT).clamp(1, MAX_ROUTINE_PAGE_LIMIT);
    let trigger_kind_filter = query
        .trigger_kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    let template_filter = query
        .template_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let channel_filter = query
        .channel
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let mut filtered = routine_views
        .into_iter()
        .filter(|routine| {
            trigger_kind_filter.as_ref().is_none_or(|expected| {
                routine
                    .get("trigger_kind")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case(expected.as_str()))
            })
        })
        .filter(|routine| {
            query.enabled.is_none_or(|enabled| {
                routine.get("enabled").and_then(Value::as_bool).unwrap_or(false) == enabled
            })
        })
        .filter(|routine| {
            channel_filter.as_ref().is_none_or(|expected| {
                routine
                    .get("channel")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case(expected.as_str()))
            })
        })
        .filter(|routine| {
            template_filter.as_ref().is_none_or(|expected| {
                routine
                    .get("template_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case(expected.as_str()))
            })
        })
        .collect::<Vec<_>>();
    filtered.sort_by(|left, right| {
        read_string_value(left, "routine_id").cmp(&read_string_value(right, "routine_id"))
    });
    if let Some(after_routine_id) =
        query.after_routine_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        filtered
            .retain(|routine| read_string_value(routine, "routine_id").as_str() > after_routine_id);
    }
    let has_more = filtered.len() > limit;
    if has_more {
        filtered.truncate(limit);
    }
    let next_after_routine_id = if has_more {
        filtered
            .last()
            .and_then(|routine| routine.get("routine_id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    } else {
        None
    };

    Ok(Json(json!({
        "routines": filtered,
        "next_after_routine_id": next_after_routine_id,
        "page": build_page_info(limit, filtered.len(), next_after_routine_id.clone()),
    })))
}

/// Returns one owned routine enriched with latest-run and troubleshooting
/// data (`GET /console/v1/routines/{routine_id}`).
///
/// # Errors
/// Returns not-found when the routine or its backing cron job is missing,
/// permission-denied for non-owners, or a mapped runtime error response.
pub(crate) async fn console_routine_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let routine = load_routine_view_for_owner(
        &state,
        routine_id.as_str(),
        session.context.principal.as_str(),
    )
    .await?;
    Ok(Json(json!({ "routine": routine })))
}

/// Builds a portable export bundle for an owned routine
/// (`GET /console/v1/routines/{routine_id}/export`).
///
/// # Errors
/// Returns not-found or permission-denied for missing/foreign routines, or a
/// validation error response when the bundle cannot be built.
pub(crate) async fn console_routine_export_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let routine = load_routine_parts_for_owner(
        &state,
        routine_id.as_str(),
        session.context.principal.as_str(),
    )
    .await?;
    let export = build_routine_export_bundle(&routine.job, &routine.metadata)
        .map_err(routine_registry_error_response)?;
    Ok(Json(json!({ "export": export })))
}

/// Imports an export bundle as a new or updated routine owned by the caller
/// (`POST /console/v1/routines/import`).
///
/// Approval policy is re-evaluated on import: when the bundle requires a
/// before-enable approval that has not been granted yet, the routine is
/// persisted disabled and a pending approval record is returned alongside it.
///
/// # Errors
/// Returns invalid-argument for malformed bundles or routine ids,
/// permission-denied for ownership/channel/feature-flag violations, or a
/// mapped runtime error response.
pub(crate) async fn console_routine_import_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleRoutineImportRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_routine_export_bundle(&payload.export).map_err(routine_registry_error_response)?;
    let bundle = payload.export;
    let routine_id = match payload.routine_id.as_deref().map(str::trim) {
        Some("") | None => bundle.job.job_id.clone(),
        Some(value) => {
            validate_canonical_id(value).map_err(|_| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "routine_id must be a canonical ULID when provided",
                ))
            })?;
            value.to_owned()
        }
    };
    let owner_principal = session.context.principal.clone();
    let channel =
        normalize_channel(Some(bundle.job.channel.as_str()), session.context.channel.as_deref())?;
    let requested_enabled = payload.enabled.unwrap_or(bundle.job.enabled);
    let existing_job =
        state.runtime.cron_job(routine_id.clone()).await.map_err(runtime_status_response)?;
    if let Some(job) = existing_job.as_ref() {
        ensure_job_owner(job, session.context.principal.as_str())?;
    }
    let approval_policy = bundle.routine.approval_policy.clone();
    // Before-enable approvals gate activation, not persistence: the routine is
    // stored disabled and re-enabled later by apply_routine_approval_decision.
    let approval_required =
        requested_enabled && approval_policy.mode == RoutineApprovalMode::BeforeEnable;
    let effective_enabled = requested_enabled && !approval_required;
    require_routines_automation_enabled_for_write(&state, effective_enabled)?;
    let job_record = persist_routine_job(
        &state,
        existing_job,
        RoutineJobUpsert {
            routine_id: routine_id.clone(),
            name: bundle.job.name.clone(),
            prompt: bundle.job.prompt.clone(),
            owner_principal: owner_principal.clone(),
            channel,
            session_key: bundle.job.session_key.clone(),
            session_label: bundle.job.session_label.clone(),
            workdir: bundle.job.workdir.clone(),
            schedule_type: bundle.job.schedule_type,
            schedule_payload_json: bundle.job.schedule_payload_json.clone(),
            enabled: effective_enabled,
            concurrency_policy: bundle.job.concurrency_policy,
            retry_policy: bundle.job.retry_policy.clone(),
            misfire_policy: bundle.job.misfire_policy,
            jitter_ms: bundle.job.jitter_ms,
            next_run_at_unix_ms: bundle.job.next_run_at_unix_ms,
        },
    )
    .await?;
    let metadata = state
        .routines
        .upsert_routine(RoutineMetadataUpsert {
            routine_id: routine_id.clone(),
            trigger_kind: bundle.routine.trigger_kind,
            trigger_payload_json: bundle.routine.trigger_payload_json.clone(),
            execution: bundle.routine.execution.clone(),
            delivery: bundle.routine.delivery.clone(),
            quiet_hours: bundle.routine.quiet_hours.clone(),
            cooldown_ms: bundle.routine.cooldown_ms,
            approval_policy,
            template_id: bundle.routine.template_id.clone(),
        })
        .map_err(routine_registry_error_response)?;
    let approval = if approval_required {
        Some(
            ensure_routine_approval_requested(
                &state,
                owner_principal.as_str(),
                Some(job_record.channel.as_str()),
                &job_record,
                &metadata,
                RoutineApprovalMode::BeforeEnable,
            )
            .await?,
        )
    } else {
        None
    };
    Ok(Json(json!({
        "routine": routine_view_from_parts(&job_record, &metadata),
        "approval": approval,
        "imported_from": bundle.job.job_id,
    })))
}

/// Creates or updates a routine and its backing cron job
/// (`POST /console/v1/routines`).
///
/// When the resolved approval policy is `before_enable` and no approval has
/// been granted, the routine is persisted disabled and the pending approval
/// record is returned in the `approval` field.
///
/// # Errors
/// Returns invalid-argument for unparseable fields or schedules,
/// permission-denied for ownership/channel/feature-flag violations, or a
/// mapped runtime/registry error response.
pub(crate) async fn console_routine_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleRoutineUpsertRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let routine_id = match payload.routine_id.as_deref().map(str::trim) {
        Some("") | None => Ulid::generate().to_string(),
        Some(value) => {
            validate_canonical_id(value).map_err(|_| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "routine_id must be a canonical ULID when provided",
                ))
            })?;
            value.to_owned()
        }
    };
    let trigger_kind = parse_routine_trigger_kind(payload.trigger_kind.as_str())
        .map_err(runtime_status_response)?;
    let owner_principal =
        normalize_owner_principal(&payload.owner_principal, session.context.principal.as_str())?;
    let channel =
        normalize_channel(payload.channel.as_deref(), session.context.channel.as_deref())?;
    let enabled = payload.enabled.unwrap_or(true);

    let existing_job =
        state.runtime.cron_job(routine_id.clone()).await.map_err(runtime_status_response)?;
    if let Some(job) = existing_job.as_ref() {
        ensure_job_owner(job, session.context.principal.as_str())?;
    }
    let existing_metadata =
        state.routines.get_routine(routine_id.as_str()).map_err(routine_registry_error_response)?;

    let schedule = resolve_routine_schedule(
        &payload,
        trigger_kind,
        state.cron_timezone_mode,
        existing_job.as_ref(),
    )?;
    let workdir = if payload.workdir.is_some() {
        normalize_optional_workdir(payload.workdir.as_deref())?
    } else {
        existing_job.as_ref().and_then(|job| job.workdir.clone())
    };
    let execution = parse_execution_config(
        payload.run_mode.as_deref(),
        default_run_mode_for_trigger_kind(trigger_kind),
        default_execution_posture_for_trigger_kind(trigger_kind, workdir.as_deref()),
        payload.procedure_profile_id.clone(),
        payload.skill_profile_id.clone(),
        payload.provider_profile_id.clone(),
        payload.execution_posture.as_deref(),
    )?;
    let execution = if let Some(governance) = payload.execution_governance {
        apply_routine_execution_governance(execution, governance)
            .map_err(routine_registry_error_response)?
    } else if let Some(metadata) = existing_metadata.as_ref() {
        preserve_routine_execution_governance(execution, &metadata.execution)
    } else {
        execution
    };
    validate_routine_prompt_self_contained(payload.prompt.as_str(), &execution)
        .map_err(routine_registry_error_response)?;
    let delivery = parse_delivery(
        payload.delivery_mode.as_deref(),
        payload.delivery_channel,
        payload.delivery_failure_mode.as_deref(),
        payload.delivery_failure_channel,
        payload.silent_policy.as_deref(),
    )?;
    let quiet_hours = parse_quiet_hours(
        payload.quiet_hours_start.as_deref(),
        payload.quiet_hours_end.as_deref(),
        payload.quiet_hours_timezone,
    )?;
    let approval_policy = resolve_upsert_approval_policy(
        payload.approval_mode.as_deref(),
        existing_metadata.as_ref(),
    )?;
    let concurrency_policy = parse_concurrency_policy(payload.concurrency_policy.as_deref())?;
    let retry_policy = parse_retry_policy(payload.retry_max_attempts, payload.retry_backoff_ms)?;
    let misfire_policy = parse_misfire_policy(payload.misfire_policy.as_deref())?;
    // Before-enable approvals gate activation, not persistence: the routine is
    // stored disabled and re-enabled later by apply_routine_approval_decision.
    let approval_required = enabled && approval_policy.mode == RoutineApprovalMode::BeforeEnable;
    let effective_enabled = enabled && !approval_required;
    require_routines_automation_enabled_for_write(&state, effective_enabled)?;
    let job_record = persist_routine_job(
        &state,
        existing_job,
        RoutineJobUpsert {
            routine_id: routine_id.clone(),
            name: payload.name.trim().to_owned(),
            prompt: payload.prompt.trim().to_owned(),
            owner_principal: owner_principal.clone(),
            channel: channel.clone(),
            session_key: normalize_optional_text(payload.session_key.as_deref()),
            session_label: normalize_optional_text(payload.session_label.as_deref()),
            workdir,
            schedule_type: schedule.schedule_type,
            schedule_payload_json: schedule.schedule_payload_json.clone(),
            enabled: effective_enabled,
            concurrency_policy,
            retry_policy: retry_policy.clone(),
            misfire_policy,
            jitter_ms: payload.jitter_ms.unwrap_or(0),
            next_run_at_unix_ms: schedule.next_run_at_unix_ms,
        },
    )
    .await?;
    state.scheduler_wake.notify_one();

    let trigger_payload_json =
        if let Some(trigger_payload_json) = schedule.trigger_payload_json_override {
            trigger_payload_json
        } else if trigger_kind == RoutineTriggerKind::Schedule {
            build_schedule_trigger_payload(&job_record)
        } else {
            serde_json::to_string(payload.trigger_payload.as_ref().unwrap_or(&json!({}))).map_err(
                |error| {
                    runtime_status_response(tonic::Status::invalid_argument(format!(
                        "trigger payload must be valid JSON: {error}"
                    )))
                },
            )?
        };
    let metadata = state
        .routines
        .upsert_routine(RoutineMetadataUpsert {
            routine_id: routine_id.clone(),
            trigger_kind,
            trigger_payload_json,
            execution,
            delivery,
            quiet_hours,
            cooldown_ms: payload.cooldown_ms.unwrap_or(0),
            approval_policy,
            template_id: normalize_optional_text(payload.template_id.as_deref()),
        })
        .map_err(routine_registry_error_response)?;
    let approval = if approval_required {
        Some(
            ensure_routine_approval_requested(
                &state,
                owner_principal.as_str(),
                Some(job_record.channel.as_str()),
                &job_record,
                &metadata,
                RoutineApprovalMode::BeforeEnable,
            )
            .await?,
        )
    } else {
        None
    };

    Ok(Json(json!({
        "routine": routine_view_from_parts(&job_record, &metadata),
        "approval": approval,
    })))
}

/// Deletes an owned routine: the backing cron job plus its metadata record
/// (`POST /console/v1/routines/{routine_id}/delete`).
///
/// # Errors
/// Returns not-found or permission-denied for missing/foreign routines, or a
/// mapped runtime/registry error response.
pub(crate) async fn console_routine_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let routine = load_routine_parts_for_owner(
        &state,
        routine_id.as_str(),
        session.context.principal.as_str(),
    )
    .await?;
    let (deleted, invalidated_pending_approvals) =
        cron::delete_cron_job_and_invalidate_pending_routine_approvals(
            &state.runtime,
            routine.job.job_id.as_str(),
            routine.metadata.routine_id.as_str(),
            session.context.principal.as_str(),
        )
        .await
        .map_err(runtime_status_response)?;
    let _ = state
        .routines
        .delete_routine(routine.metadata.routine_id.as_str())
        .map_err(routine_registry_error_response)?;
    Ok(Json(json!({
        "deleted": deleted,
        "routine_id": routine.metadata.routine_id,
        "invalidated_pending_approvals": invalidated_pending_approvals,
    })))
}

/// Enables or disables an owned routine, recomputing the next run timestamp
/// (`POST /console/v1/routines/{routine_id}/enabled`).
///
/// Enable requests subject to a pending before-enable approval keep the
/// routine disabled and return the approval record instead.
///
/// # Errors
/// Returns not-found or permission-denied for missing/foreign routines,
/// permission-denied when the routines automation feature flag blocks the
/// write, or a mapped runtime error response.
pub(crate) async fn console_routine_set_enabled_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
    Json(payload): Json<ConsoleRoutineEnabledRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let routine = load_routine_parts_for_owner(
        &state,
        routine_id.as_str(),
        session.context.principal.as_str(),
    )
    .await?;
    let approval_policy = routine.metadata.approval_policy.clone();
    let metadata = routine.metadata;
    let approval_required = payload.enabled
        && !routine.job.enabled
        && approval_policy.mode == RoutineApprovalMode::BeforeEnable;
    let effective_enabled = payload.enabled && !approval_required;
    require_routines_automation_enabled_for_write(&state, effective_enabled)?;
    let next_run_at_unix_ms = cron::next_run_at_for_enabled_state(
        &routine.job,
        effective_enabled,
        crate::gateway::current_unix_ms_status().map_err(runtime_status_response)?,
    )
    .map_err(runtime_status_response)?;
    let updated = state
        .runtime
        .update_cron_job(
            routine.job.job_id.clone(),
            CronJobUpdatePatch {
                enabled: Some(effective_enabled),
                next_run_at_unix_ms: Some(next_run_at_unix_ms),
                ..CronJobUpdatePatch::default()
            },
        )
        .await
        .map_err(runtime_status_response)?;
    state.scheduler_wake.notify_one();
    Ok(Json(json!({
        "routine": routine_view_from_parts(&updated, &metadata),
        "approval": if approval_required {
            Some(
                ensure_routine_approval_requested(
                    &state,
                    session.context.principal.as_str(),
                    Some(updated.channel.as_str()),
                    &updated,
                    &metadata,
                    RoutineApprovalMode::BeforeEnable,
                )
                .await?,
            )
        } else {
            None
        },
    })))
}

/// Rejects writes that would leave a routine effectively enabled while the
/// routines automation rollout flag is disabled; disabled-routine edits stay
/// allowed so operators can fix definitions before rollout.
#[allow(clippy::result_large_err)]
fn require_routines_automation_enabled_for_write(
    state: &AppState,
    effective_enabled: bool,
) -> Result<(), Response> {
    if routine_automation_flag_permits_enabled_write(
        routines_automation_feature_enabled(state),
        effective_enabled,
    ) {
        return Ok(());
    }
    Err(runtime_status_response(tonic::Status::permission_denied(format!(
        "feature flag '{FEATURE_ROUTINES_AUTOMATION}' is disabled"
    ))))
}

fn routines_automation_feature_enabled(state: &AppState) -> bool {
    let registry = lock_access_registry(&state.access_registry);
    registry.is_feature_enabled(FEATURE_ROUTINES_AUTOMATION)
}

const fn routine_automation_flag_permits_enabled_write(
    feature_enabled: bool,
    effective_enabled: bool,
) -> bool {
    !effective_enabled || feature_enabled
}

/// Dispatches an owned routine immediately with a manual trigger
/// (`POST /console/v1/routines/{routine_id}/run-now`).
///
/// # Errors
/// Returns not-found or permission-denied for missing/foreign routines, or a
/// mapped runtime/registry error response from dispatch.
pub(crate) async fn console_routine_run_now_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let outcome = dispatch_single_routine(
        &state,
        routine_id.as_str(),
        session.context.principal.as_str(),
        RoutineDispatchRequest {
            trigger_kind: RoutineTriggerKind::Manual,
            trigger_reason: Some("manual run-now".to_owned()),
            trigger_payload: json!({ "source": "manual_run_now" }),
            trigger_dedupe_key: None,
            dispatch_mode: RoutineDispatchMode::Normal,
            source_run_id: None,
            execution_override: None,
            delivery_override: None,
            approval_note: None,
            safety_note: None,
            bypass_operator_gates: false,
        },
    )
    .await?;
    Ok(Json(outcome))
}

/// Runs a no-delivery diagnostic of an owned routine, optionally replaying an
/// archived run (`POST /console/v1/routines/{routine_id}/test-run`).
///
/// # Errors
/// Returns not-found for missing routines or source runs, invalid-argument
/// when the source run belongs to another routine, or a mapped runtime error
/// response from dispatch.
pub(crate) async fn console_routine_test_run_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
    Json(payload): Json<ConsoleRoutineTestRunRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let outcome = dispatch_routine_test_run(
        &state,
        routine_id.as_str(),
        session.context.principal.as_str(),
        payload,
    )
    .await?;
    Ok(Json(outcome))
}

/// Lists an owned routine's run history joined with routine run metadata and
/// output previews (`GET /console/v1/routines/{routine_id}/runs`).
///
/// # Errors
/// Returns not-found or permission-denied for missing/foreign routines, or a
/// mapped runtime/registry error response.
pub(crate) async fn console_routine_runs_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
    Query(query): Query<ConsoleRoutineRunsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let routine = load_routine_parts_for_owner(
        &state,
        routine_id.as_str(),
        session.context.principal.as_str(),
    )
    .await?;
    let limit = query.limit.unwrap_or(DEFAULT_ROUTINE_PAGE_LIMIT).clamp(1, MAX_ROUTINE_PAGE_LIMIT);
    let (runs, next_after_run_id) = state
        .runtime
        .list_cron_runs(Some(routine.job.job_id.clone()), query.after_run_id.clone(), Some(limit))
        .await
        .map_err(runtime_status_response)?;
    let now_unix_ms = unix_ms_now().map_err(internal_console_error)?;
    let mut mapped_runs = Vec::with_capacity(runs.len());
    for run in &runs {
        let metadata = state
            .routines
            .find_run_metadata(run.run_id.as_str())
            .map_err(routine_registry_error_response)?;
        let mut mapped = join_run_metadata(
            routine.metadata.routine_id.as_str(),
            run,
            metadata.as_ref(),
            Some(&routine.metadata.approval_policy),
            Some(now_unix_ms),
        );
        enrich_routine_run_output_fields(&state, routine.job.owner_principal.as_str(), &mut mapped)
            .await?;
        mapped_runs.push(mapped);
    }
    Ok(Json(json!({
        "runs": mapped_runs,
        "next_after_run_id": next_after_run_id,
        "page": build_page_info(limit, mapped_runs.len(), next_after_run_id.clone()),
    })))
}

async fn enrich_routine_run_output_fields(
    state: &AppState,
    owner_principal: &str,
    run: &mut Value,
) -> Result<(), Response> {
    let orchestrator_run_id = run
        .pointer("/orchestrator_run_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let Some(session_id) = run
        .pointer("/session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(session) = state
        .runtime
        .orchestrator_session_by_id(session_id.to_owned())
        .await
        .map_err(runtime_status_response)?
    else {
        return Ok(());
    };
    let tape_output =
        routine_output_text_from_run_tape(state, orchestrator_run_id.as_deref()).await?;
    let Some(fields) = routine_output_fields_from_session(
        session_id,
        owner_principal,
        orchestrator_run_id.as_deref(),
        tape_output.as_deref(),
        routine_run_allows_output_preview(run),
        &session,
    ) else {
        return Ok(());
    };
    if let Some(object) = run.as_object_mut() {
        for (key, value) in fields {
            object.insert(key, value);
        }
    }
    Ok(())
}

async fn routine_output_text_from_run_tape(
    state: &AppState,
    orchestrator_run_id: Option<&str>,
) -> Result<Option<String>, Response> {
    let Some(run_id) = orchestrator_run_id else {
        return Ok(None);
    };
    let tape =
        match state.runtime.orchestrator_tape_snapshot(run_id.to_owned(), None, Some(128)).await {
            Ok(tape) => tape,
            Err(error) if error.code() == tonic::Code::NotFound => return Ok(None),
            Err(error) => return Err(runtime_status_response(error)),
        };
    Ok(routine_output_text_from_tape_events(tape.events.as_slice()))
}

fn routine_output_text_from_tape_events(
    events: &[crate::journal::OrchestratorTapeRecord],
) -> Option<String> {
    events
        .iter()
        .rev()
        .find(|event| event.event_type == "message.replied")
        .and_then(|event| routine_output_text_from_replied_payload(event.payload_json.as_str()))
}

fn routine_output_text_from_replied_payload(payload_json: &str) -> Option<String> {
    let payload = serde_json::from_str::<Value>(payload_json).ok()?;
    let reply_text = payload.get("reply_text").and_then(Value::as_str)?;
    normalize_routine_output_text(Some(reply_text))
}

/// Builds the `output_*` fields attached to a routine run view.
///
/// Source precedence (each branch tagged via `output_source`): suppression for
/// denied/failed runs, then the immutable run tape, then the live session
/// preview. Returns `None` when the session belongs to another principal.
fn routine_output_fields_from_session(
    session_id: &str,
    owner_principal: &str,
    orchestrator_run_id: Option<&str>,
    tape_output: Option<&str>,
    allow_preview: bool,
    session: &crate::journal::OrchestratorSessionRecord,
) -> Option<Map<String, Value>> {
    // Routine run views must never leak output across principals, even though
    // the caller already filtered the routine by owner.
    if session.principal != owner_principal {
        return None;
    }

    let mut fields = Map::new();
    fields.insert("session_key".to_owned(), json!(session.session_key.as_str()));
    fields.insert(
        "output_lookup".to_owned(),
        json!({
            "session_id": session_id,
            "session_key": session.session_key.as_str(),
        }),
    );
    if !allow_preview {
        fields.insert("output_source".to_owned(), json!("suppressed_terminal_failure"));
        return Some(fields);
    }
    if let Some(output_preview) =
        tape_output.and_then(|value| normalize_routine_output_text(Some(value)))
    {
        fields.insert("output_preview".to_owned(), json!(output_preview));
        fields.insert("output_source".to_owned(), json!("run_tape"));
        return Some(fields);
    }

    // Session previews are mutable shared-session state: copying one onto an
    // older run would misattribute output, so previews are only surfaced when
    // the session's last run is the run being viewed.
    let session_matches_run =
        orchestrator_run_id.is_some_and(|run_id| session.last_run_id.as_deref() == Some(run_id));
    if !session_matches_run {
        fields.insert("output_source".to_owned(), json!("session_lookup"));
        return Some(fields);
    }

    let preview = normalize_routine_output_text(session.preview.as_deref());
    let summary = normalize_routine_output_text(session.last_summary.as_deref());
    if let Some(output_preview) = preview.clone().or_else(|| summary.clone()) {
        fields.insert("output_preview".to_owned(), json!(output_preview));
    }
    if let Some(output_summary) = summary {
        fields.insert("output_summary".to_owned(), json!(output_summary));
    }
    fields.insert("output_source".to_owned(), json!("session_preview"));
    Some(fields)
}

// Denied and failed runs must not surface success-looking tape replies, so
// their previews are suppressed entirely.
fn routine_run_allows_output_preview(run: &Value) -> bool {
    !matches!(run.pointer("/outcome_kind").and_then(Value::as_str), Some("denied" | "failed"))
}

// Redacts the complete input before truncation, then renders one bounded line
// so transcript content cannot expose credentials or forge log records.
fn normalize_routine_output_text(value: Option<&str>) -> Option<String> {
    let redacted = palyra_common::redaction::redact_url_segments_in_text(
        palyra_common::redaction::redact_auth_error(value?).as_str(),
    );
    let terminal_safe =
        redacted.chars().map(|ch| if ch.is_control() { ' ' } else { ch }).collect::<String>();
    let normalized = terminal_safe
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(2_000)
        .collect::<String>();
    (!normalized.is_empty()).then_some(normalized)
}

/// Dispatches an owned routine with a caller-supplied trigger payload
/// (`POST /console/v1/routines/{routine_id}/dispatch`).
///
/// # Errors
/// Returns invalid-argument for unknown trigger kinds or kind mismatches,
/// not-found or permission-denied for missing/foreign routines, or a mapped
/// runtime/registry error response from dispatch.
pub(crate) async fn console_routine_dispatch_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
    Json(payload): Json<ConsoleRoutineDispatchRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let trigger_kind = payload
        .trigger_kind
        .as_deref()
        .map(parse_routine_trigger_kind)
        .transpose()
        .map_err(runtime_status_response)?
        .unwrap_or(RoutineTriggerKind::Manual);
    let outcome = dispatch_single_routine(
        &state,
        routine_id.as_str(),
        session.context.principal.as_str(),
        RoutineDispatchRequest {
            trigger_kind,
            trigger_reason: payload.trigger_reason,
            trigger_payload: payload.trigger_payload.unwrap_or_else(|| json!({})),
            trigger_dedupe_key: payload.trigger_dedupe_key,
            dispatch_mode: RoutineDispatchMode::Normal,
            source_run_id: None,
            execution_override: None,
            delivery_override: None,
            approval_note: None,
            safety_note: None,
            bypass_operator_gates: false,
        },
    )
    .await?;
    Ok(Json(outcome))
}

/// Returns the built-in routine template pack
/// (`GET /console/v1/routines/templates`).
///
/// # Errors
/// Returns an unauthorized response when console authorization fails.
pub(crate) async fn console_routine_templates_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(json!({
        "version": ROUTINE_TEMPLATE_PACK_VERSION,
        "templates": routine_templates(),
    })))
}

/// Previews the schedule a natural language phrase resolves to without
/// persisting anything (`POST /console/v1/routines/schedule-preview`).
///
/// # Errors
/// Returns invalid-argument for unknown timezones or unparseable phrases, or
/// an internal error response when the system clock cannot be read.
pub(crate) async fn console_routine_schedule_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleRoutineSchedulePreviewRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let timezone_mode = parse_timezone_mode(payload.timezone.as_deref())?;
    let preview = natural_language_schedule_preview(
        payload.phrase.as_str(),
        timezone_mode,
        unix_ms_now().map_err(internal_console_error)?,
    )
    .map_err(routine_registry_error_response)?;
    Ok(Json(json!({ "preview": preview })))
}

/// Dispatches every routine of the principal whose `system_event` trigger
/// matches `event`. Used by the system console handler.
///
/// # Errors
/// Returns a mapped runtime/registry error response when routine lookup or
/// dispatch fails; per-routine gate outcomes are reported in the result list.
pub(crate) async fn dispatch_system_event_routines(
    state: &AppState,
    principal: &str,
    event: &str,
    payload: Value,
) -> Result<Vec<Value>, Response> {
    dispatch_matching_routines(
        state,
        principal,
        RoutineTriggerKind::SystemEvent,
        Some(format!("system event {event}")),
        payload,
        None,
    )
    .await
}

/// Dispatches every routine of the principal whose `hook` trigger matches the
/// fired hook and event. Used by the hooks console handler.
///
/// # Errors
/// Returns a mapped runtime/registry error response when routine lookup or
/// dispatch fails; per-routine gate outcomes are reported in the result list.
pub(crate) async fn dispatch_hook_event_routines(
    state: &AppState,
    principal: &str,
    hook_id: &str,
    event: &str,
    payload: Value,
    dedupe_key: Option<String>,
) -> Result<Vec<Value>, Response> {
    let mut trigger_payload = payload;
    if let Some(object) = trigger_payload.as_object_mut() {
        object.insert("hook_id".to_owned(), Value::String(hook_id.to_owned()));
        object.insert("event".to_owned(), Value::String(event.to_owned()));
    }
    dispatch_matching_routines(
        state,
        principal,
        RoutineTriggerKind::Hook,
        Some(format!("hook event {hook_id}:{event}")),
        trigger_payload,
        dedupe_key,
    )
    .await
}

/// Dispatches every routine of the principal whose `webhook` trigger matches
/// the integration, provider, and event. Used by the webhooks handler.
///
/// # Errors
/// Returns a mapped runtime/registry error response when routine lookup or
/// dispatch fails; per-routine gate outcomes are reported in the result list.
pub(crate) async fn dispatch_webhook_event_routines(
    state: &AppState,
    principal: &str,
    integration_id: &str,
    provider: &str,
    event: &str,
    payload: Value,
    dedupe_key: Option<String>,
) -> Result<Vec<Value>, Response> {
    let mut trigger_payload = payload;
    if let Some(object) = trigger_payload.as_object_mut() {
        object.insert("integration_id".to_owned(), Value::String(integration_id.to_owned()));
        object.insert("provider".to_owned(), Value::String(provider.to_owned()));
        object.insert("event".to_owned(), Value::String(event.to_owned()));
    }
    dispatch_matching_routines(
        state,
        principal,
        RoutineTriggerKind::Webhook,
        Some(format!("webhook event {integration_id}:{event}")),
        trigger_payload,
        dedupe_key,
    )
    .await
}

#[derive(Debug)]
struct RoutineParts {
    job: CronJobRecord,
    metadata: RoutineMetadataRecord,
}

#[derive(Debug)]
struct ScheduleResolution {
    schedule_type: CronScheduleType,
    schedule_payload_json: String,
    next_run_at_unix_ms: Option<i64>,
    // Set when schedule resolution also normalizes the trigger payload (file
    // watch), in which case it replaces the caller-supplied payload verbatim.
    trigger_payload_json_override: Option<String>,
}

#[derive(Debug, Clone)]
struct RoutineJobUpsert {
    routine_id: String,
    name: String,
    prompt: String,
    owner_principal: String,
    channel: String,
    session_key: Option<String>,
    session_label: Option<String>,
    workdir: Option<String>,
    schedule_type: CronScheduleType,
    schedule_payload_json: String,
    enabled: bool,
    concurrency_policy: CronConcurrencyPolicy,
    retry_policy: CronRetryPolicy,
    misfire_policy: CronMisfirePolicy,
    jitter_ms: u64,
    next_run_at_unix_ms: Option<i64>,
}

// Cron jobs can be created outside the routines surface (CLI, gRPC); syncing
// before reads keeps schedule-kind routine metadata mirrored onto them.
async fn synchronize_schedule_routines(state: &AppState) -> Result<Vec<CronJobRecord>, Response> {
    let jobs = list_all_cron_jobs(state).await?;
    state
        .routines
        .sync_schedule_routines(jobs.as_slice())
        .map_err(routine_registry_error_response)?;
    Ok(jobs)
}

async fn list_all_cron_jobs(state: &AppState) -> Result<Vec<CronJobRecord>, Response> {
    let mut jobs = Vec::new();
    let mut after_job_id = None::<String>;
    loop {
        let (mut page, next_after_job_id) = state
            .runtime
            .list_cron_jobs(after_job_id.clone(), Some(MAX_ROUTINE_PAGE_LIMIT), None, None, None)
            .await
            .map_err(runtime_status_response)?;
        if page.is_empty() {
            break;
        }
        jobs.append(&mut page);
        let Some(next_after_job_id) = next_after_job_id else {
            break;
        };
        after_job_id = Some(next_after_job_id);
    }
    Ok(jobs)
}

async fn routine_views_for_principal(
    state: &AppState,
    jobs: Vec<CronJobRecord>,
    principal: &str,
) -> Result<Vec<Value>, Response> {
    let job_map = jobs.into_iter().map(|job| (job.job_id.clone(), job)).collect::<HashMap<_, _>>();
    let mut routines = Vec::new();
    for metadata in
        state.routines.list_routines().map_err(routine_registry_error_response)?.into_iter()
    {
        let Some(job) = job_map.get(metadata.routine_id.as_str()) else {
            continue;
        };
        if job.owner_principal != principal {
            continue;
        }
        let view = enrich_routine_view_with_latest_run(
            state,
            routine_view_from_parts(job, &metadata),
            metadata.routine_id.as_str(),
            job.job_id.as_str(),
            &metadata.approval_policy,
        )
        .await?;
        routines.push(view);
    }
    routines.sort_by(|left, right| {
        read_string_value(left, "routine_id").cmp(&read_string_value(right, "routine_id"))
    });
    Ok(routines)
}

async fn load_routine_view_for_owner(
    state: &AppState,
    routine_id: &str,
    principal: &str,
) -> Result<Value, Response> {
    let parts = load_routine_parts_for_owner(state, routine_id, principal).await?;
    enrich_routine_view_with_latest_run(
        state,
        routine_view_from_parts(&parts.job, &parts.metadata),
        parts.metadata.routine_id.as_str(),
        parts.job.job_id.as_str(),
        &parts.metadata.approval_policy,
    )
    .await
}

async fn load_routine_parts_for_owner(
    state: &AppState,
    routine_id: &str,
    principal: &str,
) -> Result<RoutineParts, Response> {
    synchronize_schedule_routines(state).await?;
    let metadata = state
        .routines
        .get_routine(routine_id)
        .map_err(routine_registry_error_response)?
        .ok_or_else(|| runtime_status_response(tonic::Status::not_found("routine not found")))?;
    let job = state
        .runtime
        .cron_job(metadata.routine_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("routine backing cron job not found"))
        })?;
    ensure_job_owner(&job, principal)?;
    Ok(RoutineParts { job, metadata })
}

async fn persist_routine_job(
    state: &AppState,
    existing_job: Option<CronJobRecord>,
    request: RoutineJobUpsert,
) -> Result<CronJobRecord, Response> {
    if existing_job.is_some() {
        state
            .runtime
            .update_cron_job(
                request.routine_id.clone(),
                CronJobUpdatePatch {
                    name: Some(request.name),
                    prompt: Some(request.prompt),
                    owner_principal: Some(request.owner_principal),
                    channel: Some(request.channel),
                    session_key: Some(request.session_key),
                    session_label: Some(request.session_label),
                    workdir: Some(request.workdir),
                    schedule_type: Some(request.schedule_type),
                    schedule_payload_json: Some(request.schedule_payload_json),
                    enabled: Some(request.enabled),
                    concurrency_policy: Some(request.concurrency_policy),
                    retry_policy: Some(request.retry_policy),
                    misfire_policy: Some(request.misfire_policy),
                    jitter_ms: Some(request.jitter_ms),
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
                workdir: request.workdir,
                schedule_type: request.schedule_type,
                schedule_payload_json: request.schedule_payload_json,
                enabled: request.enabled,
                concurrency_policy: request.concurrency_policy,
                retry_policy: request.retry_policy,
                misfire_policy: request.misfire_policy,
                jitter_ms: request.jitter_ms,
                next_run_at_unix_ms: request.next_run_at_unix_ms,
            })
            .await
            .map_err(runtime_status_response)
    }
}

// Attaches `automation`, `last_run`, and a troubleshooting summary computed
// over the 10 most recent runs to a routine view.
async fn enrich_routine_view_with_latest_run(
    state: &AppState,
    mut view: Value,
    routine_id: &str,
    job_id: &str,
    approval_policy: &RoutineApprovalPolicy,
) -> Result<Value, Response> {
    let automation_enabled = routines_automation_feature_enabled(state);
    if let Some(object) = view.as_object_mut() {
        object.insert(
            "automation".to_owned(),
            json!({
                "feature_flag": FEATURE_ROUTINES_AUTOMATION,
                "routines_automation_enabled": automation_enabled,
            }),
        );
    }
    let (runs, _) = state
        .runtime
        .list_latest_cron_runs(job_id.to_owned(), 10)
        .await
        .map_err(runtime_status_response)?;
    if runs.is_empty() && !automation_enabled {
        if let Some(object) = view.as_object_mut() {
            object.insert(
                "troubleshooting".to_owned(),
                json!({
                    "window_size": 0,
                    "failed_runs": 0,
                    "skipped_runs": 0,
                    "denied_runs": 0,
                    "recommended_action": routine_troubleshooting_recommended_action(
                        automation_enabled,
                        0,
                        0,
                        0,
                    ),
                }),
            );
        }
    }
    let Some(run) = runs.last() else {
        return Ok(view);
    };
    let metadata = state
        .routines
        .find_run_metadata(run.run_id.as_str())
        .map_err(routine_registry_error_response)?;
    let latest_run = join_run_metadata(
        routine_id,
        run,
        metadata.as_ref(),
        Some(approval_policy),
        Some(unix_ms_now().map_err(internal_console_error)?),
    );
    if let Some(object) = view.as_object_mut() {
        object.insert("last_run".to_owned(), latest_run.clone());
        object.insert(
            "last_outcome_kind".to_owned(),
            latest_run.get("outcome_kind").cloned().unwrap_or(Value::Null),
        );
        object.insert(
            "last_outcome_message".to_owned(),
            latest_run.get("outcome_message").cloned().unwrap_or(Value::Null),
        );
        let failure_count =
            runs.iter().filter(|entry| matches!(entry.status, CronRunStatus::Failed)).count();
        let skip_count =
            runs.iter().filter(|entry| matches!(entry.status, CronRunStatus::Skipped)).count();
        let denied_count =
            runs.iter().filter(|entry| matches!(entry.status, CronRunStatus::Denied)).count();
        object.insert(
            "troubleshooting".to_owned(),
            json!({
                "window_size": runs.len(),
                "failed_runs": failure_count,
                "skipped_runs": skip_count,
                "denied_runs": denied_count,
                "recommended_action": routine_troubleshooting_recommended_action(
                    automation_enabled,
                    failure_count,
                    denied_count,
                    skip_count,
                ),
            }),
        );
    }
    Ok(view)
}

fn routine_troubleshooting_recommended_action(
    automation_enabled: bool,
    failure_count: usize,
    denied_count: usize,
    skip_count: usize,
) -> &'static str {
    if !automation_enabled {
        return "Enable the routines_automation feature flag before enabling or running scheduled routines.";
    }
    if failure_count > 0 {
        "Inspect the latest run details, provider routing, and delivery preview before re-running."
    } else if denied_count > 0 {
        "Resolve the approval or safety gate and use safe test-run before re-enabling production delivery."
    } else if skip_count > 0 {
        "Review cooldown, quiet hours, or trigger dedupe rules that may be suppressing execution."
    } else {
        "Routine is healthy; use safe test-run when you need a no-delivery diagnostic run."
    }
}

// Routine approvals ride on Tool-subject approval records; the subject id
// encodes "routine:{routine_id}:{mode}" so decisions can be routed back here.
fn routine_approval_subject_id(routine_id: &str, mode: RoutineApprovalMode) -> String {
    format!("routine:{routine_id}:{}", mode.as_str())
}

fn parse_routine_approval_subject(subject_id: &str) -> Option<(String, RoutineApprovalMode)> {
    let (routine_id, mode) = subject_id.strip_prefix("routine:")?.rsplit_once(':')?;
    if validate_canonical_id(routine_id).is_err() {
        return None;
    }
    Some((routine_id.to_owned(), RoutineApprovalMode::from_str(mode)?))
}

async fn routine_approval_granted(
    state: &AppState,
    principal: &str,
    job: &CronJobRecord,
    metadata: &RoutineMetadataRecord,
    mode: RoutineApprovalMode,
) -> Result<bool, Response> {
    let subject_id = routine_approval_subject_id(metadata.routine_id.as_str(), mode);
    let (approvals, _) = state
        .runtime
        .list_approval_records(
            None,
            Some(25),
            None,
            None,
            Some(subject_id),
            Some(principal.to_owned()),
            Some(ApprovalDecision::Allow),
            Some(ApprovalSubjectType::Tool),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(approvals.into_iter().any(|approval| {
        matches!(approval.decision, Some(ApprovalDecision::Allow))
            && routine_approval_matches_definition(&approval, principal, job, metadata, mode)
    }))
}

fn routine_approval_details_json(
    job: &CronJobRecord,
    metadata: &RoutineMetadataRecord,
    mode: RoutineApprovalMode,
) -> String {
    json!({
        "routine_id": metadata.routine_id.as_str(),
        "name": job.name.as_str(),
        "approval_mode": mode.as_str(),
        "trigger_kind": metadata.trigger_kind.as_str(),
        "workdir": job.workdir.as_deref(),
        "run_mode": metadata.execution.run_mode.as_str(),
        "execution_posture": metadata.execution.execution_posture.as_str(),
        "allow_sensitive_tools": routine_allows_sensitive_tools(&metadata.execution),
        "procedure_profile_id": metadata.execution.procedure_profile_id.as_deref(),
        "skill_profile_id": metadata.execution.skill_profile_id.as_deref(),
        "provider_profile_id": metadata.execution.provider_profile_id.as_deref(),
        "delivery_mode": metadata.delivery.mode.as_str(),
        "channel": job.channel.as_str(),
        "template_id": metadata.template_id.as_deref(),
        "definition_sha256": routine_approval_definition_hash(job, metadata),
    })
    .to_string()
}

fn routine_approval_definition_hash(
    job: &CronJobRecord,
    metadata: &RoutineMetadataRecord,
) -> String {
    let definition = json!({
        "job": {
            "job_id": job.job_id.as_str(),
            "name": job.name.as_str(),
            "prompt": job.prompt.as_str(),
            "owner_principal": job.owner_principal.as_str(),
            "channel": job.channel.as_str(),
            "session_key": job.session_key.as_deref(),
            "session_label": job.session_label.as_deref(),
            "workdir": job.workdir.as_deref(),
            "schedule_type": job.schedule_type,
            "schedule_payload_json": job.schedule_payload_json.as_str(),
            "concurrency_policy": job.concurrency_policy,
            "retry_policy": &job.retry_policy,
            "misfire_policy": job.misfire_policy,
            "jitter_ms": job.jitter_ms,
        },
        "metadata": {
            "routine_id": metadata.routine_id.as_str(),
            "trigger_kind": metadata.trigger_kind,
            "trigger_payload_json": metadata.trigger_payload_json.as_str(),
            "execution": &metadata.execution,
            "delivery": &metadata.delivery,
            "quiet_hours": &metadata.quiet_hours,
            "cooldown_ms": metadata.cooldown_ms,
            "approval_policy": &metadata.approval_policy,
            "template_id": metadata.template_id.as_deref(),
        },
    });
    hex::encode(Sha256::digest(definition.to_string().as_bytes()))
}

fn routine_approval_policy_hash(details_json: &str) -> String {
    hex::encode(Sha256::digest(details_json.as_bytes()))
}

// Returns the existing undecided approval for this routine/mode if one is
// pending; otherwise creates a fresh approval prompt so repeated enable
// attempts do not stack duplicate operator prompts.
async fn ensure_routine_approval_requested(
    state: &AppState,
    principal: &str,
    channel: Option<&str>,
    job: &CronJobRecord,
    metadata: &RoutineMetadataRecord,
    mode: RoutineApprovalMode,
) -> Result<Value, Response> {
    let subject_id = routine_approval_subject_id(metadata.routine_id.as_str(), mode);
    let (existing, _) = state
        .runtime
        .list_approval_records(
            None,
            Some(25),
            None,
            None,
            Some(subject_id.clone()),
            Some(principal.to_owned()),
            None,
            Some(ApprovalSubjectType::Tool),
        )
        .await
        .map_err(runtime_status_response)?;
    if let Some(approval) = existing.into_iter().rev().find(|approval| {
        approval.subject_id == subject_id
            && approval.decision.is_none()
            && routine_approval_matches_definition(approval, principal, job, metadata, mode)
    }) {
        return serde_json::to_value(approval).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to serialize routine approval record: {error}"
            )))
        });
    }

    let details_json = routine_approval_details_json(job, metadata, mode);
    let prompt = ApprovalPromptRecord {
        title: format!("Approve routine {}", job.name),
        risk_level: ApprovalRiskLevel::High,
        subject_id: subject_id.clone(),
        summary: format!(
            "Routine `{}` requires explicit approval for `{}`.",
            job.name,
            mode.as_str()
        ),
        options: vec![
            ApprovalPromptOption {
                option_id: "allow_once".to_owned(),
                label: "Approve routine".to_owned(),
                description: "Allow the routine to proceed with the requested automation action."
                    .to_owned(),
                default_selected: true,
                decision_scope: ApprovalDecisionScope::Once,
                timebox_ttl_ms: None,
            },
            ApprovalPromptOption {
                option_id: "deny_once".to_owned(),
                label: "Keep blocked".to_owned(),
                description: "Leave the routine blocked until an operator approves it later."
                    .to_owned(),
                default_selected: false,
                decision_scope: ApprovalDecisionScope::Once,
                timebox_ttl_ms: None,
            },
        ],
        timeout_seconds: ROUTINE_APPROVAL_TIMEOUT_SECONDS,
        details_json: details_json.clone(),
        policy_explanation:
            "Routine approvals are explicit operator gates for sensitive automation activation."
                .to_owned(),
    };
    let policy_hash = routine_approval_policy_hash(details_json.as_str());
    let record = state
        .runtime
        .create_approval_record(ApprovalCreateRequest {
            approval_id: Ulid::generate().to_string(),
            session_id: Ulid::generate().to_string(),
            run_id: Ulid::generate().to_string(),
            principal: principal.to_owned(),
            device_id: ROUTINE_APPROVAL_DEVICE_ID.to_owned(),
            channel: channel.map(ToOwned::to_owned),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: subject_id.clone(),
            request_summary: format!(
                "routine_id={} routine_name={} approval_mode={} execution_posture={}",
                metadata.routine_id,
                job.name,
                mode.as_str(),
                metadata.execution.execution_posture.as_str()
            ),
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: ROUTINE_APPROVAL_POLICY_ID.to_owned(),
                policy_hash,
                evaluation_summary: format!(
                    "routine approval required mode={} trigger={} execution_posture={} delivery={}",
                    mode.as_str(),
                    metadata.trigger_kind.as_str(),
                    metadata.execution.execution_posture.as_str(),
                    metadata.delivery.mode.as_str()
                ),
            },
            prompt,
        })
        .await
        .map_err(runtime_status_response)?;
    serde_json::to_value(record).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize routine approval record: {error}"
        )))
    })
}

/// Authorizes a console principal to resolve a routine approval before the
/// approval record is mutated.
///
/// # Errors
/// Returns permission-denied when the approval provenance does not match the
/// authenticated owner and current routine definition, or a mapped runtime
/// error.
pub(crate) async fn authorize_routine_approval_decision(
    state: &AppState,
    approval: &ApprovalRecord,
    principal: &str,
) -> Result<(), Response> {
    if approval.subject_type != ApprovalSubjectType::Tool {
        return Ok(());
    }
    let Some((routine_id, _)) = parse_routine_approval_subject(approval.subject_id.as_str()) else {
        return Ok(());
    };
    let routine = load_routine_parts_for_owner(state, routine_id.as_str(), principal).await?;
    ensure_routine_approval_matches_definition(approval, principal, &routine.job, &routine.metadata)
}

/// Applies the side effect of an allowed routine approval: re-enables a
/// before-enable routine or schedules the first run of a before-first-run
/// routine. Invoked by the approvals handler after a decision is recorded.
///
/// Returns `Ok(None)` for decisions that are not routine approvals (denials,
/// non-tool subjects, or foreign subject ids).
///
/// # Errors
/// Returns permission-denied when the approval provenance does not match the
/// authenticated owner and current routine definition, when the routines
/// automation feature flag is disabled, or when a runtime/registry operation
/// fails.
pub(crate) async fn apply_routine_approval_decision(
    state: &AppState,
    approval: &ApprovalRecord,
    principal: &str,
) -> Result<Option<Value>, Response> {
    if !matches!(approval.decision, Some(ApprovalDecision::Allow))
        || approval.subject_type != ApprovalSubjectType::Tool
    {
        return Ok(None);
    }
    let Some((routine_id, mode)) = parse_routine_approval_subject(approval.subject_id.as_str())
    else {
        return Ok(None);
    };
    let routine = load_routine_parts_for_owner(state, routine_id.as_str(), principal).await?;
    ensure_routine_approval_matches_definition(
        approval,
        principal,
        &routine.job,
        &routine.metadata,
    )?;
    match mode {
        RoutineApprovalMode::BeforeEnable => {
            apply_before_enable_routine_approval(state, &routine.job).await
        }
        RoutineApprovalMode::BeforeFirstRun => {
            apply_before_first_run_routine_approval(state, &routine.job).await
        }
        RoutineApprovalMode::None => Ok(None),
    }
}

fn routine_approval_matches_definition(
    approval: &ApprovalRecord,
    principal: &str,
    job: &CronJobRecord,
    metadata: &RoutineMetadataRecord,
    mode: RoutineApprovalMode,
) -> bool {
    let subject_id = routine_approval_subject_id(metadata.routine_id.as_str(), mode);
    let details_json = routine_approval_details_json(job, metadata, mode);
    let policy_hash = routine_approval_policy_hash(details_json.as_str());
    approval.subject_type == ApprovalSubjectType::Tool
        && approval.subject_id == subject_id
        && approval.prompt.subject_id == subject_id
        && approval.principal == principal
        && approval.device_id == ROUTINE_APPROVAL_DEVICE_ID
        && approval.channel.as_deref() == Some(job.channel.as_str())
        && job.owner_principal == principal
        && job.job_id == metadata.routine_id
        && metadata.approval_policy.mode == mode
        && approval.policy_snapshot.policy_id == ROUTINE_APPROVAL_POLICY_ID
        && approval.policy_snapshot.policy_hash == policy_hash
        && approval.prompt.details_json == details_json
}

#[allow(clippy::result_large_err)]
fn ensure_routine_approval_matches_definition(
    approval: &ApprovalRecord,
    principal: &str,
    job: &CronJobRecord,
    metadata: &RoutineMetadataRecord,
) -> Result<(), Response> {
    let Some((_, mode)) = parse_routine_approval_subject(approval.subject_id.as_str()) else {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "routine approval provenance does not match the current routine definition",
        )));
    };
    if !routine_approval_matches_definition(approval, principal, job, metadata, mode) {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "routine approval provenance does not match the current routine definition",
        )));
    }
    Ok(())
}

async fn apply_before_enable_routine_approval(
    state: &AppState,
    job: &CronJobRecord,
) -> Result<Option<Value>, Response> {
    require_routines_automation_enabled_for_write(state, true)?;
    let next_run_at_unix_ms = cron::next_run_at_for_enabled_state(
        job,
        true,
        crate::gateway::current_unix_ms_status().map_err(runtime_status_response)?,
    )
    .map_err(runtime_status_response)?;
    let updated = state
        .runtime
        .update_cron_job(
            job.job_id.clone(),
            CronJobUpdatePatch {
                enabled: Some(true),
                next_run_at_unix_ms: Some(next_run_at_unix_ms),
                queued_run: Some(false),
                ..CronJobUpdatePatch::default()
            },
        )
        .await
        .map_err(runtime_status_response)?;
    state.scheduler_wake.notify_one();
    routine_approval_apply_outcome(state, "enabled", &updated)
}

async fn apply_before_first_run_routine_approval(
    state: &AppState,
    job: &CronJobRecord,
) -> Result<Option<Value>, Response> {
    if !job.enabled {
        return routine_approval_apply_outcome(state, "routine_disabled", job);
    }
    if job.next_run_at_unix_ms.is_some() {
        return routine_approval_apply_outcome(state, "already_scheduled", job);
    }
    require_routines_automation_enabled_for_write(state, true)?;
    let updated = state
        .runtime
        .update_cron_job(
            job.job_id.clone(),
            CronJobUpdatePatch {
                next_run_at_unix_ms: Some(Some(
                    crate::gateway::current_unix_ms_status().map_err(runtime_status_response)?,
                )),
                queued_run: Some(false),
                ..CronJobUpdatePatch::default()
            },
        )
        .await
        .map_err(runtime_status_response)?;
    state.scheduler_wake.notify_one();
    routine_approval_apply_outcome(state, "scheduled_first_run", &updated)
}

#[allow(clippy::result_large_err)]
fn routine_approval_apply_outcome(
    state: &AppState,
    action: &str,
    job: &CronJobRecord,
) -> Result<Option<Value>, Response> {
    let metadata =
        state.routines.get_routine(job.job_id.as_str()).map_err(routine_registry_error_response)?;
    Ok(Some(match metadata {
        Some(metadata) => json!({
            "action": action,
            "routine": routine_view_from_parts(job, &metadata),
        }),
        None => json!({
            "action": action,
            "routine_id": job.job_id,
            "enabled": cron::visible_cron_job_enabled(job),
            "next_run_at_unix_ms": cron::visible_next_run_at_unix_ms(job),
        }),
    }))
}

#[derive(Debug, Clone)]
struct RoutineDispatchRequest {
    trigger_kind: RoutineTriggerKind,
    trigger_reason: Option<String>,
    trigger_payload: Value,
    trigger_dedupe_key: Option<String>,
    dispatch_mode: RoutineDispatchMode,
    source_run_id: Option<String>,
    execution_override: Option<RoutineExecutionConfig>,
    delivery_override: Option<RoutineDeliveryConfig>,
    approval_note: Option<String>,
    safety_note: Option<String>,
    // Skips the trigger-kind match, dedupe, cooldown, and quiet-hours gates.
    // Only safe test-runs/replays set this; approval gates still apply.
    bypass_operator_gates: bool,
}

/// Dispatches a safe diagnostic run of an owned routine: forces a fresh
/// session and audit-only (no-delivery) output, optionally replaying the
/// trigger payload of an archived run identified by `source_run_id`.
///
/// # Errors
/// Returns not-found for missing routines or source runs, invalid-argument
/// when the source run belongs to another routine, or a mapped
/// runtime/registry error response from dispatch.
pub(crate) async fn dispatch_routine_test_run(
    state: &AppState,
    routine_id: &str,
    principal: &str,
    payload: ConsoleRoutineTestRunRequest,
) -> Result<Value, Response> {
    let routine = load_routine_parts_for_owner(state, routine_id, principal).await?;
    let replay_source =
        match payload.source_run_id.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
            Some(run_id) => {
                let entry = state
                    .routines
                    .find_run_metadata(run_id)
                    .map_err(routine_registry_error_response)?
                    .ok_or_else(|| {
                        runtime_status_response(tonic::Status::not_found("source run not found"))
                    })?;
                if entry.routine_id != routine.metadata.routine_id {
                    return Err(runtime_status_response(tonic::Status::invalid_argument(
                        "source_run_id must belong to the selected routine",
                    )));
                }
                Some(entry)
            }
            None => None,
        };
    let trigger_payload = if let Some(value) = payload.trigger_payload {
        value
    } else if let Some(source) = replay_source.as_ref() {
        serde_json::from_str(source.trigger_payload_json.as_str()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to parse archived trigger payload: {error}"
            )))
        })?
    } else {
        serde_json::from_str(routine.metadata.trigger_payload_json.as_str()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to parse routine trigger payload: {error}"
            )))
        })?
    };
    let dispatch_mode = if replay_source.is_some() {
        RoutineDispatchMode::Replay
    } else {
        RoutineDispatchMode::TestRun
    };
    let safety_note = Some(match dispatch_mode {
        RoutineDispatchMode::Replay => {
            "safe replay forced fresh_session and audit-only delivery".to_owned()
        }
        _ => "safe test-run forced fresh_session and audit-only delivery".to_owned(),
    });
    dispatch_single_routine(
        state,
        routine_id,
        principal,
        RoutineDispatchRequest {
            trigger_kind: replay_source
                .as_ref()
                .map(|entry| entry.trigger_kind)
                .unwrap_or(routine.metadata.trigger_kind),
            trigger_reason: payload
                .trigger_reason
                .or_else(|| replay_source.as_ref().and_then(|entry| entry.trigger_reason.clone()))
                .or_else(|| Some("safe test-run".to_owned())),
            trigger_payload,
            trigger_dedupe_key: None,
            dispatch_mode,
            source_run_id: replay_source.as_ref().map(|entry| entry.run_id.clone()),
            execution_override: Some(RoutineExecutionConfig {
                run_mode: RoutineRunMode::FreshSession,
                ..routine.metadata.execution.clone()
            }),
            delivery_override: Some(build_safe_test_delivery()),
            approval_note: None,
            safety_note,
            bypass_operator_gates: true,
        },
    )
    .await
}

// Runs the routine gate pipeline, then triggers the backing cron job. Gate
// order matters: disabled state and pending before-first-run approval are
// checked before the bypassable operator gates (trigger-kind match, dedupe,
// cooldown, quiet hours), so even safe test-runs cannot run an unapproved
// routine. Gate hits are recorded as terminal runs instead of erroring.
async fn dispatch_single_routine(
    state: &AppState,
    routine_id: &str,
    principal: &str,
    request: RoutineDispatchRequest,
) -> Result<Value, Response> {
    let routine = load_routine_parts_for_owner(state, routine_id, principal).await?;
    let execution =
        request.execution_override.clone().unwrap_or_else(|| routine.metadata.execution.clone());
    let delivery =
        request.delivery_override.clone().unwrap_or_else(|| routine.metadata.delivery.clone());

    if !routine.job.enabled {
        return register_terminal_routine_run(
            state,
            TerminalRoutineRunRequest {
                routine_id: routine.job.job_id.as_str(),
                trigger_kind: request.trigger_kind,
                trigger_reason: request.trigger_reason,
                trigger_payload: &request.trigger_payload,
                trigger_dedupe_key: request.trigger_dedupe_key,
                execution,
                delivery,
                dispatch_mode: request.dispatch_mode,
                source_run_id: request.source_run_id,
                status: CronRunStatus::Skipped,
                outcome_override: RoutineRunOutcomeKind::Skipped,
                message: "routine is disabled",
                skip_reason: Some("routine_disabled".to_owned()),
                delivery_reason: None,
                approval_note: request.approval_note,
                safety_note: request.safety_note,
            },
        )
        .await;
    }
    if routine.metadata.approval_policy.mode == RoutineApprovalMode::BeforeFirstRun
        && !routine_approval_granted(
            state,
            principal,
            &routine.job,
            &routine.metadata,
            RoutineApprovalMode::BeforeFirstRun,
        )
        .await?
    {
        let approval = ensure_routine_approval_requested(
            state,
            principal,
            Some(routine.job.channel.as_str()),
            &routine.job,
            &routine.metadata,
            RoutineApprovalMode::BeforeFirstRun,
        )
        .await?;
        let mut response = register_terminal_routine_run(
            state,
            TerminalRoutineRunRequest {
                routine_id: routine.job.job_id.as_str(),
                trigger_kind: request.trigger_kind,
                trigger_reason: request.trigger_reason,
                trigger_payload: &request.trigger_payload,
                trigger_dedupe_key: request.trigger_dedupe_key,
                execution,
                delivery,
                dispatch_mode: request.dispatch_mode,
                source_run_id: request.source_run_id,
                status: CronRunStatus::Denied,
                outcome_override: RoutineRunOutcomeKind::Denied,
                message: "routine approval is required before the first run",
                skip_reason: Some("approval_required".to_owned()),
                delivery_reason: None,
                approval_note: Some("before_first_run approval is still pending".to_owned()),
                safety_note: request.safety_note,
            },
        )
        .await?;
        if let Some(object) = response.as_object_mut() {
            object.insert("approval".to_owned(), approval);
        }
        return Ok(response);
    }
    if !request.bypass_operator_gates {
        if routine.metadata.trigger_kind != request.trigger_kind
            && request.trigger_kind != RoutineTriggerKind::Manual
        {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "trigger_kind does not match routine definition",
            )));
        }
        if let Some(dedupe_key) = request.trigger_dedupe_key.as_deref() {
            let seen = state
                .routines
                .seen_dedupe_key(routine.metadata.routine_id.as_str(), dedupe_key)
                .map_err(routine_registry_error_response)?;
            if seen {
                return register_terminal_routine_run(
                    state,
                    TerminalRoutineRunRequest {
                        routine_id: routine.job.job_id.as_str(),
                        trigger_kind: request.trigger_kind,
                        trigger_reason: request.trigger_reason,
                        trigger_payload: &request.trigger_payload,
                        trigger_dedupe_key: Some(dedupe_key.to_owned()),
                        execution,
                        delivery,
                        dispatch_mode: request.dispatch_mode,
                        source_run_id: request.source_run_id,
                        status: CronRunStatus::Skipped,
                        outcome_override: RoutineRunOutcomeKind::Throttled,
                        message: "duplicate trigger dedupe key already processed",
                        skip_reason: Some("duplicate_trigger_dedupe_key".to_owned()),
                        delivery_reason: None,
                        approval_note: request.approval_note,
                        safety_note: request.safety_note,
                    },
                )
                .await;
            }
        }

        let now_unix_ms = unix_ms_now().map_err(internal_console_error)?;
        if routine.metadata.cooldown_ms > 0 {
            let latest = state
                .routines
                .list_run_metadata(Some(routine.metadata.routine_id.as_str()), 1)
                .map_err(routine_registry_error_response)?
                .into_iter()
                .last();
            if latest.as_ref().is_some_and(|entry| {
                routine_cooldown_deadline_is_after(
                    entry.created_at_unix_ms,
                    routine.metadata.cooldown_ms,
                    now_unix_ms,
                )
            }) {
                return register_terminal_routine_run(
                    state,
                    TerminalRoutineRunRequest {
                        routine_id: routine.job.job_id.as_str(),
                        trigger_kind: request.trigger_kind,
                        trigger_reason: request.trigger_reason,
                        trigger_payload: &request.trigger_payload,
                        trigger_dedupe_key: request.trigger_dedupe_key,
                        execution,
                        delivery,
                        dispatch_mode: request.dispatch_mode,
                        source_run_id: request.source_run_id,
                        status: CronRunStatus::Skipped,
                        outcome_override: RoutineRunOutcomeKind::Throttled,
                        message: "routine cooldown window is still active",
                        skip_reason: Some("cooldown_active".to_owned()),
                        delivery_reason: None,
                        approval_note: request.approval_note,
                        safety_note: request.safety_note,
                    },
                )
                .await;
            }
        }
        if is_in_quiet_hours(routine.metadata.quiet_hours.as_ref(), now_unix_ms)? {
            return register_terminal_routine_run(
                state,
                TerminalRoutineRunRequest {
                    routine_id: routine.job.job_id.as_str(),
                    trigger_kind: request.trigger_kind,
                    trigger_reason: request.trigger_reason,
                    trigger_payload: &request.trigger_payload,
                    trigger_dedupe_key: request.trigger_dedupe_key,
                    execution,
                    delivery,
                    dispatch_mode: request.dispatch_mode,
                    source_run_id: request.source_run_id,
                    status: CronRunStatus::Skipped,
                    outcome_override: RoutineRunOutcomeKind::Skipped,
                    message: "routine quiet hours suppress execution",
                    skip_reason: Some("quiet_hours".to_owned()),
                    delivery_reason: None,
                    approval_note: request.approval_note,
                    safety_note: request.safety_note,
                },
            )
            .await;
        }
    }

    let outcome = cron::trigger_job_now_with_options(
        Arc::clone(&state.runtime),
        state.auth.clone(),
        state.grpc_url.clone(),
        routine.job.clone(),
        Arc::clone(&state.scheduler_wake),
        build_cron_trigger_options(
            &execution,
            request.dispatch_mode,
            request.source_run_id.as_deref(),
            request.safety_note.as_deref(),
        ),
    )
    .await
    .map_err(runtime_status_response)?;
    if let Some(run_id) = outcome.run_id.as_ref() {
        let outcome_kind = default_outcome_from_cron_status(outcome.status);
        let _ = state
            .routines
            .upsert_run_metadata(RoutineRunMetadataUpsert {
                run_id: run_id.clone(),
                routine_id: routine.metadata.routine_id.clone(),
                trigger_kind: request.trigger_kind,
                trigger_reason: request.trigger_reason,
                trigger_payload_json: serde_json::to_string(&request.trigger_payload).map_err(
                    |error| {
                        runtime_status_response(tonic::Status::internal(format!(
                            "failed to serialize trigger payload: {error}"
                        )))
                    },
                )?,
                trigger_dedupe_key: request.trigger_dedupe_key,
                execution,
                delivery: delivery.clone(),
                dispatch_mode: request.dispatch_mode,
                source_run_id: request.source_run_id,
                outcome_override: Some(outcome_kind),
                outcome_message: Some(outcome.message.clone()),
                output_delivered: Some(output_delivered_for_outcome(&delivery, outcome_kind)),
                skip_reason: if matches!(outcome.status, CronRunStatus::Skipped) {
                    Some("scheduler_skipped".to_owned())
                } else {
                    None
                },
                delivery_reason: Some(delivery_reason_for_outcome(&delivery, outcome_kind)),
                approval_note: request.approval_note,
                safety_note: request.safety_note,
            })
            .map_err(routine_registry_error_response)?;
    }
    let output_lookup = outcome.session_key.as_ref().map(|session_key| {
        json!({
            "session_key": session_key,
        })
    });
    Ok(json!({
        "routine_id": routine.metadata.routine_id,
        "run_id": outcome.run_id,
        "status": outcome.status.as_str(),
        "message": outcome.message,
        "session_key": outcome.session_key,
        "session_label": outcome.session_label,
        "output_lookup": output_lookup,
        "dispatch_mode": request.dispatch_mode.as_str(),
        "delivery_preview": routine_delivery_preview(&delivery),
    }))
}

async fn dispatch_matching_routines(
    state: &AppState,
    principal: &str,
    trigger_kind: RoutineTriggerKind,
    trigger_reason: Option<String>,
    trigger_payload: Value,
    trigger_dedupe_key: Option<String>,
) -> Result<Vec<Value>, Response> {
    let jobs = synchronize_schedule_routines(state).await?;
    let job_map = jobs.into_iter().map(|job| (job.job_id.clone(), job)).collect::<HashMap<_, _>>();
    let routines = state
        .routines
        .list_routines()
        .map_err(routine_registry_error_response)?
        .into_iter()
        .filter(|routine| routine.trigger_kind == trigger_kind)
        .collect::<Vec<_>>();
    let mut outcomes = Vec::new();
    for metadata in routines {
        let Some(job) = job_map.get(metadata.routine_id.as_str()) else {
            continue;
        };
        if job.owner_principal != principal || !routine_matches_trigger(&metadata, &trigger_payload)
        {
            continue;
        }
        let outcome = dispatch_single_routine(
            state,
            metadata.routine_id.as_str(),
            principal,
            RoutineDispatchRequest {
                trigger_kind,
                trigger_reason: trigger_reason.clone(),
                trigger_payload: trigger_payload.clone(),
                trigger_dedupe_key: trigger_dedupe_key.clone(),
                dispatch_mode: RoutineDispatchMode::Normal,
                source_run_id: None,
                execution_override: None,
                delivery_override: None,
                approval_note: None,
                safety_note: None,
                bypass_operator_gates: false,
            },
        )
        .await?;
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

// Matches an incoming trigger payload against a routine's configured trigger.
// Unset configured matchers act as wildcards; schedule routines never match
// event dispatch because the scheduler owns their execution.
fn routine_matches_trigger(metadata: &RoutineMetadataRecord, payload: &Value) -> bool {
    let configured = serde_json::from_str::<Value>(metadata.trigger_payload_json.as_str())
        .unwrap_or_else(|_| json!({}));
    match metadata.trigger_kind {
        RoutineTriggerKind::SystemEvent => compare_optional_matchers(
            configured.get("event").and_then(Value::as_str),
            payload.get("event").and_then(Value::as_str),
        ),
        RoutineTriggerKind::Hook => {
            compare_optional_matchers(
                configured.get("hook_id").and_then(Value::as_str),
                payload.get("hook_id").and_then(Value::as_str),
            ) && compare_optional_matchers(
                configured.get("event").and_then(Value::as_str),
                payload.get("event").and_then(Value::as_str),
            )
        }
        RoutineTriggerKind::Webhook => {
            compare_optional_matchers(
                configured.get("integration_id").and_then(Value::as_str),
                payload.get("integration_id").and_then(Value::as_str),
            ) && compare_optional_matchers(
                configured.get("provider").and_then(Value::as_str),
                payload.get("provider").and_then(Value::as_str),
            ) && compare_optional_matchers(
                configured.get("event").and_then(Value::as_str),
                payload.get("event").and_then(Value::as_str),
            )
        }
        RoutineTriggerKind::Manual => true,
        RoutineTriggerKind::FileWatch => compare_optional_matchers(
            configured.get("path").and_then(Value::as_str),
            payload.get("path").and_then(Value::as_str),
        ),
        RoutineTriggerKind::Schedule => false,
    }
}

fn compare_optional_matchers(expected: Option<&str>, actual: Option<&str>) -> bool {
    expected
        .is_none_or(|expected| actual.is_some_and(|actual| expected.eq_ignore_ascii_case(actual)))
}

fn routine_cooldown_deadline_is_after(
    created_at_unix_ms: i64,
    cooldown_ms: u64,
    now_unix_ms: i64,
) -> bool {
    let cooldown_ms = i64::try_from(cooldown_ms).unwrap_or(i64::MAX);
    created_at_unix_ms.saturating_add(cooldown_ms) > now_unix_ms
}

struct TerminalRoutineRunRequest<'a> {
    routine_id: &'a str,
    trigger_kind: RoutineTriggerKind,
    trigger_reason: Option<String>,
    trigger_payload: &'a Value,
    trigger_dedupe_key: Option<String>,
    execution: RoutineExecutionConfig,
    delivery: RoutineDeliveryConfig,
    dispatch_mode: RoutineDispatchMode,
    source_run_id: Option<String>,
    status: CronRunStatus,
    outcome_override: RoutineRunOutcomeKind,
    message: &'a str,
    skip_reason: Option<String>,
    delivery_reason: Option<String>,
    approval_note: Option<String>,
    safety_note: Option<String>,
}

// Synthesizes a start+finalize cron run pair plus run metadata for a routine
// that was stopped by a gate (disabled, approval, dedupe, cooldown, quiet
// hours), so gate outcomes stay visible in run history without invoking the
// scheduler.
async fn register_terminal_routine_run(
    state: &AppState,
    request: TerminalRoutineRunRequest<'_>,
) -> Result<Value, Response> {
    let run_id = Ulid::generate().to_string();
    let delivery_preview = routine_delivery_preview(&request.delivery);
    let error_kind = cron::routine_gate_error_kind(request.skip_reason.as_deref()).to_owned();
    state
        .runtime
        .start_cron_run(CronRunStartRequest {
            run_id: run_id.clone(),
            job_id: request.routine_id.to_owned(),
            attempt: 1,
            session_id: None,
            orchestrator_run_id: None,
            status: request.status,
            error_kind: Some(error_kind.clone()),
            error_message_redacted: Some(request.message.to_owned()),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .finalize_cron_run(CronRunFinalizeRequest {
            run_id: run_id.clone(),
            status: request.status,
            error_kind: Some(error_kind),
            error_message_redacted: Some(request.message.to_owned()),
            model_tokens_in: 0,
            model_tokens_out: 0,
            tool_calls: 0,
            tool_denies: 0,
            orchestrator_run_id: None,
            session_id: None,
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .routines
        .upsert_run_metadata(RoutineRunMetadataUpsert {
            run_id: run_id.clone(),
            routine_id: request.routine_id.to_owned(),
            trigger_kind: request.trigger_kind,
            trigger_reason: request.trigger_reason,
            trigger_payload_json: serde_json::to_string(request.trigger_payload).map_err(
                |error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to serialize trigger payload: {error}"
                    )))
                },
            )?,
            trigger_dedupe_key: request.trigger_dedupe_key,
            execution: request.execution,
            delivery: request.delivery,
            dispatch_mode: request.dispatch_mode,
            source_run_id: request.source_run_id,
            outcome_override: Some(request.outcome_override),
            outcome_message: Some(request.message.to_owned()),
            output_delivered: Some(false),
            skip_reason: request.skip_reason,
            delivery_reason: request.delivery_reason,
            approval_note: request.approval_note,
            safety_note: request.safety_note,
        })
        .map_err(routine_registry_error_response)?;
    Ok(json!({
        "routine_id": request.routine_id,
        "run_id": run_id,
        "status": request.status.as_str(),
        "message": request.message,
        "dispatch_mode": request.dispatch_mode.as_str(),
        "delivery_preview": delivery_preview,
    }))
}

fn routine_view_from_parts(job: &CronJobRecord, metadata: &RoutineMetadataRecord) -> Value {
    json!({
        "routine_id": metadata.routine_id,
        "job_id": job.job_id,
        "name": job.name,
        "prompt": job.prompt,
        "owner_principal": job.owner_principal,
        "channel": job.channel,
        "session_key": job.session_key,
        "session_label": job.session_label,
        "workdir": job.workdir,
        "enabled": crate::routines::visible_routine_enabled(job, metadata.trigger_kind),
        "schedule_type": job.schedule_type.as_str(),
        "schedule_payload": serde_json::from_str::<Value>(job.schedule_payload_json.as_str()).unwrap_or_else(|_| json!({ "raw": job.schedule_payload_json })),
        "next_run_at_unix_ms": cron::visible_next_run_at_unix_ms(job),
        "last_run_at_unix_ms": job.last_run_at_unix_ms,
        "queued_run": job.queued_run,
        "concurrency_policy": job.concurrency_policy.as_str(),
        "retry_policy": {
            "max_attempts": job.retry_policy.max_attempts,
            "backoff_ms": job.retry_policy.backoff_ms,
        },
        "misfire_policy": job.misfire_policy.as_str(),
        "jitter_ms": job.jitter_ms,
        "max_runs": cron::max_runs_for_job(job),
        "trigger_kind": metadata.trigger_kind.as_str(),
        "trigger_payload": serde_json::from_str::<Value>(metadata.trigger_payload_json.as_str()).unwrap_or_else(|_| json!({ "raw": metadata.trigger_payload_json })),
        "run_mode": metadata.execution.run_mode.as_str(),
        "execution_posture": metadata.execution.execution_posture.as_str(),
        "allow_sensitive_tools": routine_allows_sensitive_tools(&metadata.execution),
        "execution_governance": routine_execution_governance_projection(&metadata.execution),
        "procedure_profile_id": metadata.execution.procedure_profile_id,
        "skill_profile_id": metadata.execution.skill_profile_id,
        "provider_profile_id": metadata.execution.provider_profile_id,
        "delivery_mode": metadata.delivery.mode.as_str(),
        "delivery_channel": metadata.delivery.channel,
        "delivery_failure_mode": metadata.delivery.failure_mode.map(RoutineDeliveryMode::as_str),
        "delivery_failure_channel": metadata.delivery.failure_channel,
        "silent_policy": metadata.delivery.silent_policy.as_str(),
        "delivery_preview": routine_delivery_preview(&metadata.delivery),
        "preview_audit": cron_routine_preview_audit_projection(CronRoutinePreviewAuditInput {
            routine_id: metadata.routine_id.as_str(),
            trigger_kind: metadata.trigger_kind,
            trigger_reason: None,
            trigger_payload_json: Some(metadata.trigger_payload_json.as_str()),
            delivery: &metadata.delivery,
            schedule_type: Some(job.schedule_type),
            schedule_payload_json: Some(job.schedule_payload_json.as_str()),
            require_trigger_reason: false,
        }),
        "capability_profile": routine_capability_profile_projection(
            metadata.trigger_kind,
            &metadata.execution,
            &metadata.approval_policy,
        ),
        "quiet_hours": metadata.quiet_hours.as_ref().map(|quiet_hours| json!({
            "start_minute_of_day": quiet_hours.start_minute_of_day,
            "end_minute_of_day": quiet_hours.end_minute_of_day,
            "timezone": quiet_hours.timezone,
        })),
        "cooldown_ms": metadata.cooldown_ms,
        "approval_mode": metadata.approval_policy.mode.as_str(),
        "template_id": metadata.template_id,
        "created_at_unix_ms": metadata.created_at_unix_ms,
        "updated_at_unix_ms": metadata.updated_at_unix_ms,
    })
}

// Resolves the cron schedule backing a routine upsert. Resolution precedence
// for schedule triggers is explicit `schedule_type` first, then the natural
// language phrase; file-watch triggers always derive an `every` poll schedule
// from their normalized payload, and other trigger kinds get an inert shadow
// schedule.
#[allow(clippy::result_large_err)]
fn resolve_routine_schedule(
    payload: &ConsoleRoutineUpsertRequest,
    trigger_kind: RoutineTriggerKind,
    timezone_mode: CronTimezoneMode,
    existing_job: Option<&CronJobRecord>,
) -> Result<ScheduleResolution, Response> {
    if trigger_kind == RoutineTriggerKind::FileWatch {
        let config = normalize_file_watch_trigger_payload(payload.trigger_payload.as_ref())
            .map_err(routine_registry_error_response)?;
        let normalized = cron::normalize_schedule(
            Some(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Every as i32,
                spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                    interval_ms: config.poll_interval_ms,
                })),
            }),
            unix_ms_now().map_err(internal_console_error)?,
            CronTimezoneMode::Utc,
        )
        .map_err(runtime_status_response)?;
        let schedule_payload_json = schedule_payload_with_max_runs(
            normalized.schedule_payload_json,
            normalized.schedule_type,
            payload.max_runs,
            existing_job,
        )?;
        let trigger_payload_json = serde_json::to_string(&config).map_err(|error| {
            internal_console_error(anyhow::anyhow!(
                "failed to encode file_watch trigger payload: {error}"
            ))
        })?;
        return Ok(ScheduleResolution {
            schedule_type: normalized.schedule_type,
            schedule_payload_json,
            next_run_at_unix_ms: normalized.next_run_at_unix_ms,
            trigger_payload_json_override: Some(trigger_payload_json),
        });
    }
    if trigger_kind != RoutineTriggerKind::Schedule {
        if payload.max_runs.is_some() {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "max_runs is only supported for scheduled and file_watch routines",
            )));
        }
        // Every routine is backed by a cron job row; event-triggered routines
        // get a one-shot shadow schedule with no next run so the scheduler
        // never fires them on its own.
        return Ok(ScheduleResolution {
            schedule_type: CronScheduleType::At,
            schedule_payload_json: shadow_manual_schedule_payload_json(),
            next_run_at_unix_ms: None,
            trigger_payload_json_override: None,
        });
    }
    let schedule_timezone_mode =
        parse_optional_schedule_timezone_mode(payload.schedule_timezone.as_deref(), timezone_mode)?;
    let explicit_schedule_type =
        payload.schedule_type.as_deref().map(str::trim).filter(|value| !value.is_empty());
    // The natural language phrase is consulted only when no explicit
    // schedule_type was supplied; an explicit schedule must never be
    // downgraded by leftover phrase text (pinned by tests).
    if explicit_schedule_type.is_none() {
        if let Some(phrase) = payload
            .natural_language_schedule
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let preview = natural_language_schedule_preview(
                phrase,
                schedule_timezone_mode,
                unix_ms_now().map_err(internal_console_error)?,
            )
            .map_err(routine_registry_error_response)?;
            let schedule_type = parse_schedule_type(preview.schedule_type.as_str())?;
            let schedule_payload_json = schedule_payload_with_max_runs(
                preview.schedule_payload_json,
                schedule_type,
                payload.max_runs,
                existing_job,
            )?;
            return Ok(ScheduleResolution {
                schedule_type,
                schedule_payload_json,
                next_run_at_unix_ms: preview.next_run_at_unix_ms,
                trigger_payload_json_override: None,
            });
        }
    }
    let schedule =
        build_console_schedule(explicit_schedule_type, payload).map_err(runtime_status_response)?;
    let normalized = cron::normalize_schedule(
        Some(schedule),
        unix_ms_now().map_err(internal_console_error)?,
        schedule_timezone_mode,
    )
    .map_err(runtime_status_response)?;
    let schedule_payload_json = schedule_payload_with_max_runs(
        normalized.schedule_payload_json,
        normalized.schedule_type,
        payload.max_runs,
        existing_job,
    )?;
    Ok(ScheduleResolution {
        schedule_type: normalized.schedule_type,
        schedule_payload_json,
        next_run_at_unix_ms: normalized.next_run_at_unix_ms,
        trigger_payload_json_override: None,
    })
}

// Embeds the run-count cap into the schedule payload JSON. A missing request
// value preserves the cap already stored on the existing job, so updates that
// omit max_runs do not silently drop the limit.
#[allow(clippy::result_large_err)]
fn schedule_payload_with_max_runs(
    schedule_payload_json: String,
    schedule_type: CronScheduleType,
    requested_max_runs: Option<u32>,
    existing_job: Option<&CronJobRecord>,
) -> Result<String, Response> {
    let max_runs = match requested_max_runs {
        Some(0) => {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "max_runs must be greater than zero",
            )));
        }
        Some(value) => Some(value),
        None => existing_job.and_then(cron::max_runs_for_job),
    };
    let Some(max_runs) = max_runs else {
        return Ok(schedule_payload_json);
    };
    if schedule_type == CronScheduleType::At && max_runs > 1 {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "max_runs greater than 1 is not supported for schedule_type=at; use schedule_type=every for repeated runs",
        )));
    }
    let mut schedule_payload = serde_json::from_str::<Value>(schedule_payload_json.as_str())
        .map_err(|error| internal_console_error(anyhow::Error::new(error)))?;
    let Some(object) = schedule_payload.as_object_mut() else {
        return Err(runtime_status_response(tonic::Status::internal(
            "schedule payload must be a JSON object",
        )));
    };
    object.insert("max_runs".to_owned(), Value::from(max_runs));
    serde_json::to_string(&schedule_payload)
        .map_err(|error| internal_console_error(anyhow::Error::new(error)))
}

fn build_console_schedule(
    schedule_type_raw: Option<&str>,
    payload: &ConsoleRoutineUpsertRequest,
) -> Result<cron_v1::Schedule, tonic::Status> {
    let schedule_type = schedule_type_raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| tonic::Status::invalid_argument("schedule_type is required"))?;
    match schedule_type.to_ascii_lowercase().as_str() {
        "cron" => {
            let expression = payload
                .cron_expression
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    tonic::Status::invalid_argument(
                        "cron_expression is required for schedule_type=cron",
                    )
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Cron as i32,
                spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule {
                    expression: expression.to_owned(),
                })),
            })
        }
        "every" => {
            let interval_ms =
                payload.every_interval_ms.filter(|value| *value > 0).ok_or_else(|| {
                    tonic::Status::invalid_argument(
                        "every_interval_ms must be greater than zero for schedule_type=every",
                    )
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Every as i32,
                spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule { interval_ms })),
            })
        }
        "at" => {
            let timestamp = payload
                .at_timestamp_rfc3339
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    tonic::Status::invalid_argument(
                        "at_timestamp_rfc3339 is required for schedule_type=at",
                    )
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::At as i32,
                spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                    timestamp_rfc3339: timestamp.to_owned(),
                })),
            })
        }
        _ => Err(tonic::Status::invalid_argument("schedule_type must be one of cron|every|at")),
    }
}

fn build_schedule_trigger_payload(job: &CronJobRecord) -> String {
    json!({
        "schedule_type": job.schedule_type.as_str(),
        "workdir": job.workdir,
        "schedule_payload": serde_json::from_str::<Value>(job.schedule_payload_json.as_str()).unwrap_or_else(|_| json!({ "raw": job.schedule_payload_json })),
    })
    .to_string()
}

fn build_safe_test_delivery() -> RoutineDeliveryConfig {
    RoutineDeliveryConfig {
        mode: RoutineDeliveryMode::LogsOnly,
        channel: None,
        failure_mode: Some(RoutineDeliveryMode::LogsOnly),
        failure_channel: None,
        silent_policy: RoutineSilentPolicy::AuditOnly,
    }
}

fn build_cron_trigger_options(
    execution: &RoutineExecutionConfig,
    dispatch_mode: RoutineDispatchMode,
    source_run_id: Option<&str>,
    safety_note: Option<&str>,
) -> cron::TriggerJobOptions {
    cron::TriggerJobOptions {
        force_run_mode: Some(execution.run_mode),
        allow_sensitive_tools: Some(routine_allows_sensitive_tools(execution)),
        model_profile_override: execution.provider_profile_id.clone(),
        parameter_delta_json: Some(
            json!({
                "routine": {
                    "run_mode": execution.run_mode.as_str(),
                    "execution_posture": execution.execution_posture.as_str(),
                    "procedure_profile_id": execution.procedure_profile_id,
                    "skill_profile_id": execution.skill_profile_id,
                    "provider_profile_id": execution.provider_profile_id,
                },
                "dispatch_mode": dispatch_mode.as_str(),
                "source_run_id": source_run_id,
                "safety_note": safety_note,
            })
            .to_string(),
        ),
        origin_kind: Some(match dispatch_mode {
            RoutineDispatchMode::Normal => "manual".to_owned(),
            RoutineDispatchMode::TestRun => "routine_test_run".to_owned(),
            RoutineDispatchMode::Replay => "routine_replay".to_owned(),
        }),
        ..cron::TriggerJobOptions::default()
    }
}

fn output_delivered_for_outcome(
    delivery: &RoutineDeliveryConfig,
    outcome_kind: RoutineRunOutcomeKind,
) -> bool {
    if matches!(outcome_kind, RoutineRunOutcomeKind::Denied) {
        return false;
    }
    if delivery.silent_policy == RoutineSilentPolicy::AuditOnly {
        return false;
    }
    let failure_path =
        matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied);
    if !failure_path && delivery.silent_policy == RoutineSilentPolicy::FailureOnly {
        return false;
    }
    let effective_mode =
        if failure_path { delivery.failure_mode.unwrap_or(delivery.mode) } else { delivery.mode };
    !matches!(effective_mode, RoutineDeliveryMode::LocalOnly | RoutineDeliveryMode::LogsOnly)
}

fn delivery_reason_for_outcome(
    delivery: &RoutineDeliveryConfig,
    outcome_kind: RoutineRunOutcomeKind,
) -> String {
    let preview = routine_delivery_preview(delivery);
    let key =
        if matches!(outcome_kind, RoutineRunOutcomeKind::Failed | RoutineRunOutcomeKind::Denied) {
            "failure"
        } else {
            "success"
        };
    preview
        .get(key)
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or("delivery preview unavailable")
        .to_owned()
}

fn parse_routine_trigger_kind(value: &str) -> Result<RoutineTriggerKind, tonic::Status> {
    RoutineTriggerKind::from_str(value).ok_or_else(|| {
        tonic::Status::invalid_argument(
            "trigger_kind must be one of schedule|hook|webhook|system_event|file_watch|manual",
        )
    })
}

fn default_run_mode_for_trigger_kind(trigger_kind: RoutineTriggerKind) -> RoutineRunMode {
    match trigger_kind {
        RoutineTriggerKind::Schedule | RoutineTriggerKind::FileWatch => {
            RoutineRunMode::FreshSession
        }
        RoutineTriggerKind::Hook
        | RoutineTriggerKind::Webhook
        | RoutineTriggerKind::SystemEvent
        | RoutineTriggerKind::Manual => RoutineRunMode::SameSession,
    }
}

fn default_execution_posture_for_trigger_kind(
    trigger_kind: RoutineTriggerKind,
    workdir: Option<&str>,
) -> RoutineExecutionPosture {
    match trigger_kind {
        RoutineTriggerKind::Schedule if workdir.is_some() => {
            RoutineExecutionPosture::SensitiveTools
        }
        RoutineTriggerKind::FileWatch => RoutineExecutionPosture::SensitiveTools,
        RoutineTriggerKind::Schedule
        | RoutineTriggerKind::Hook
        | RoutineTriggerKind::Webhook
        | RoutineTriggerKind::SystemEvent
        | RoutineTriggerKind::Manual => RoutineExecutionPosture::Standard,
    }
}

#[allow(clippy::result_large_err)]
fn parse_schedule_type(value: &str) -> Result<CronScheduleType, Response> {
    CronScheduleType::from_str(value).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "schedule type must be one of cron|every|at",
        ))
    })
}

#[allow(clippy::result_large_err)]
fn parse_concurrency_policy(value: Option<&str>) -> Result<CronConcurrencyPolicy, Response> {
    let normalized = value.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("forbid");
    CronConcurrencyPolicy::from_str(normalized).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "concurrency_policy must be one of forbid|replace|queue(1)",
        ))
    })
}

#[allow(clippy::result_large_err)]
fn parse_retry_policy(
    max_attempts: Option<u32>,
    backoff_ms: Option<u64>,
) -> Result<CronRetryPolicy, Response> {
    Ok(CronRetryPolicy {
        max_attempts: max_attempts.unwrap_or(DEFAULT_ROUTINE_RETRY_MAX_ATTEMPTS).max(1),
        backoff_ms: backoff_ms.unwrap_or(DEFAULT_ROUTINE_RETRY_BACKOFF_MS).max(1),
    })
}

#[allow(clippy::result_large_err)]
fn parse_misfire_policy(value: Option<&str>) -> Result<CronMisfirePolicy, Response> {
    let normalized = value.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("skip");
    CronMisfirePolicy::from_str(normalized).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "misfire_policy must be one of skip|catch_up",
        ))
    })
}

#[allow(clippy::result_large_err)]
fn parse_execution_config(
    run_mode: Option<&str>,
    default_run_mode: RoutineRunMode,
    default_execution_posture: RoutineExecutionPosture,
    procedure_profile_id: Option<String>,
    skill_profile_id: Option<String>,
    provider_profile_id: Option<String>,
    execution_posture: Option<&str>,
) -> Result<RoutineExecutionConfig, Response> {
    let run_mode = match run_mode.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => RoutineRunMode::from_str(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "run_mode must be one of same_session|fresh_session",
            ))
        })?,
        None => default_run_mode,
    };
    let execution_posture = match execution_posture.map(str::trim).filter(|value| !value.is_empty())
    {
        Some(value) => RoutineExecutionPosture::from_str(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "execution_posture must be one of standard|sensitive_tools",
            ))
        })?,
        None => default_execution_posture,
    };
    Ok(RoutineExecutionConfig {
        run_mode,
        procedure_profile_id: normalize_optional_text(procedure_profile_id.as_deref()),
        skill_profile_id: normalize_optional_text(skill_profile_id.as_deref()),
        provider_profile_id: normalize_optional_text(provider_profile_id.as_deref()),
        execution_posture,
        ..RoutineExecutionConfig::default()
    })
}

#[allow(clippy::result_large_err)]
fn parse_delivery(
    mode: Option<&str>,
    channel: Option<String>,
    failure_mode: Option<&str>,
    failure_channel: Option<String>,
    silent_policy: Option<&str>,
) -> Result<RoutineDeliveryConfig, Response> {
    let normalized_mode =
        mode.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("same_channel");
    let mode = RoutineDeliveryMode::from_str(normalized_mode).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "delivery_mode must be one of same_channel|specific_channel|local_only|logs_only",
        ))
    })?;
    let failure_mode = match failure_mode.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => Some(RoutineDeliveryMode::from_str(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "delivery_failure_mode must be one of same_channel|specific_channel|local_only|logs_only",
            ))
        })?),
        None => None,
    };
    let silent_policy = match silent_policy.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => RoutineSilentPolicy::from_str(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "silent_policy must be one of noisy|failure_only|audit_only",
            ))
        })?,
        None => RoutineSilentPolicy::Noisy,
    };
    let channel = normalize_optional_text(channel.as_deref());
    let failure_channel = normalize_optional_text(failure_channel.as_deref());
    if matches!(mode, RoutineDeliveryMode::SpecificChannel) && channel.is_none() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "delivery_channel is required for delivery_mode=specific_channel",
        )));
    }
    if matches!(failure_mode, Some(RoutineDeliveryMode::SpecificChannel))
        && failure_channel.as_ref().or(channel.as_ref()).is_none()
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "delivery_failure_channel or delivery_channel is required for delivery_failure_mode=specific_channel",
        )));
    }
    Ok(RoutineDeliveryConfig { mode, channel, failure_mode, failure_channel, silent_policy })
}

#[allow(clippy::result_large_err)]
fn parse_quiet_hours(
    start: Option<&str>,
    end: Option<&str>,
    timezone: Option<String>,
) -> Result<Option<RoutineQuietHours>, Response> {
    let Some(start) = start.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let end = end.map(str::trim).filter(|value| !value.is_empty()).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "quiet_hours_end is required when quiet_hours_start is provided",
        ))
    })?;
    let start_minute_of_day = parse_time_of_day_to_minutes(start)?;
    let end_minute_of_day = parse_time_of_day_to_minutes(end)?;
    let timezone = normalize_quiet_hours_timezone(timezone)?;
    Ok(Some(RoutineQuietHours { start_minute_of_day, end_minute_of_day, timezone }))
}

#[allow(clippy::result_large_err)]
fn normalize_quiet_hours_timezone(timezone: Option<String>) -> Result<Option<String>, Response> {
    let Some(raw) = timezone else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let mode = CronTimezoneMode::from_str(trimmed).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "quiet_hours timezone must be local, utc, or an IANA timezone",
        ))
    })?;
    Ok(Some(mode.as_str().to_owned()))
}

#[allow(clippy::result_large_err)]
fn parse_time_of_day_to_minutes(value: &str) -> Result<u16, Response> {
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
    let normalized = value.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("none");
    let mode = RoutineApprovalMode::from_str(normalized).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "approval_mode must be one of none|before_enable|before_first_run",
        ))
    })?;
    Ok(RoutineApprovalPolicy { mode })
}

#[allow(clippy::result_large_err)]
fn resolve_upsert_approval_policy(
    requested_mode: Option<&str>,
    existing_metadata: Option<&RoutineMetadataRecord>,
) -> Result<RoutineApprovalPolicy, Response> {
    if requested_mode.is_none() {
        if let Some(metadata) = existing_metadata {
            return Ok(metadata.approval_policy.clone());
        }
    }
    parse_approval_policy(requested_mode)
}

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

#[allow(clippy::result_large_err)]
fn normalize_channel(
    requested: Option<&str>,
    session_channel: Option<&str>,
) -> Result<String, Response> {
    let requested_channel = requested.map(str::trim).filter(|value| !value.is_empty());
    validate_cron_job_channel_context(session_channel, requested_channel)
        .map_err(runtime_status_response)?;
    Ok(requested_channel
        .map(ToOwned::to_owned)
        .or_else(|| session_channel.map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_ROUTINE_CHANNEL.to_owned()))
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned)
}

#[allow(clippy::result_large_err)]
fn normalize_optional_workdir(value: Option<&str>) -> Result<Option<String>, Response> {
    let Some(workdir) = normalize_optional_text(value) else {
        return Ok(None);
    };
    if workdir.len() > 4096 {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "workdir must be 4096 characters or fewer",
        )));
    }
    if workdir.contains('\0') {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "workdir must not contain NUL bytes",
        )));
    }
    Ok(Some(workdir))
}

#[allow(clippy::result_large_err)]
fn ensure_job_owner(job: &CronJobRecord, principal: &str) -> Result<(), Response> {
    if job.owner_principal != principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "routine owner mismatch for authenticated principal",
        )));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn parse_timezone_mode(value: Option<&str>) -> Result<CronTimezoneMode, Response> {
    let value = value.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("local");
    CronTimezoneMode::from_str(value).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "timezone must be local, utc, or an IANA timezone",
        ))
    })
}

#[allow(clippy::result_large_err)]
fn parse_optional_schedule_timezone_mode(
    value: Option<&str>,
    default_timezone_mode: CronTimezoneMode,
) -> Result<CronTimezoneMode, Response> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => CronTimezoneMode::from_str(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "schedule_timezone must be local, utc, or an IANA timezone",
            ))
        }),
        None => Ok(default_timezone_mode),
    }
}

#[allow(clippy::result_large_err)]
fn is_in_quiet_hours(
    quiet_hours: Option<&RoutineQuietHours>,
    now_unix_ms: i64,
) -> Result<bool, Response> {
    let Some(quiet_hours) = quiet_hours else {
        return Ok(false);
    };
    let timezone_mode = CronTimezoneMode::from_str(
        quiet_hours.timezone.as_deref().unwrap_or("local"),
    )
    .ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "quiet_hours timezone must be local, utc, or an IANA timezone",
        ))
    })?;
    let minute_of_day = match timezone_mode {
        CronTimezoneMode::Utc => {
            let value =
                chrono::Utc.timestamp_millis_opt(now_unix_ms).single().ok_or_else(|| {
                    runtime_status_response(tonic::Status::internal(
                        "failed to resolve UTC quiet-hour timestamp",
                    ))
                })?;
            value.hour() as u16 * 60 + value.minute() as u16
        }
        CronTimezoneMode::Local => {
            let value =
                chrono::Local.timestamp_millis_opt(now_unix_ms).single().ok_or_else(|| {
                    runtime_status_response(tonic::Status::internal(
                        "failed to resolve local quiet-hour timestamp",
                    ))
                })?;
            value.hour() as u16 * 60 + value.minute() as u16
        }
        CronTimezoneMode::Named(timezone) => {
            let value = timezone.timestamp_millis_opt(now_unix_ms).single().ok_or_else(|| {
                runtime_status_response(tonic::Status::internal(
                    "failed to resolve named quiet-hour timestamp",
                ))
            })?;
            value.hour() as u16 * 60 + value.minute() as u16
        }
    };
    let start = quiet_hours.start_minute_of_day;
    let end = quiet_hours.end_minute_of_day;
    // Equal endpoints mean all-day quiet; start > end is a window that wraps
    // past midnight (e.g. 22:00-06:00). The end minute is exclusive.
    Ok(if start == end {
        true
    } else if start < end {
        minute_of_day >= start && minute_of_day < end
    } else {
        minute_of_day >= start || minute_of_day < end
    })
}

fn read_string_value(record: &Value, key: &str) -> String {
    record.get(key).and_then(Value::as_str).unwrap_or_default().to_owned()
}

// Field-level registry validation maps to a structured 400 payload; every
// other registry failure is an internal error.
fn routine_registry_error_response(error: RoutineRegistryError) -> Response {
    match error {
        RoutineRegistryError::InvalidField { field, message } => {
            validation_error_response(field, "invalid", message.as_str())
        }
        other => runtime_status_response(tonic::Status::internal(other.to_string())),
    }
}

fn internal_console_error(error: anyhow::Error) -> Response {
    runtime_status_response(tonic::Status::internal(sanitize_http_error_message(
        error.to_string().as_str(),
    )))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        compare_optional_matchers, is_in_quiet_hours, normalize_channel,
        normalize_routine_output_text, output_delivered_for_outcome, parse_delivery,
        parse_execution_config, parse_optional_schedule_timezone_mode, parse_quiet_hours,
        parse_routine_approval_subject, parse_timezone_mode, routine_approval_details_json,
        routine_approval_matches_definition, routine_approval_policy_hash,
        routine_approval_subject_id, routine_automation_flag_permits_enabled_write,
        routine_cooldown_deadline_is_after, routine_matches_trigger,
        routine_output_fields_from_session, routine_output_text_from_tape_events,
        routine_run_allows_output_preview, routine_troubleshooting_recommended_action,
    };
    use crate::cron::CronTimezoneMode;
    use crate::journal::{
        ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptRecord,
        ApprovalRecord, ApprovalRiskLevel, ApprovalSubjectType, CronConcurrencyPolicy,
        CronJobRecord, CronMisfirePolicy, CronRetryPolicy, CronScheduleType,
        OrchestratorSessionRecord, OrchestratorTapeRecord,
    };
    use crate::routines::{
        RoutineApprovalMode, RoutineApprovalPolicy, RoutineDeliveryConfig, RoutineDeliveryMode,
        RoutineExecutionConfig, RoutineExecutionPosture, RoutineMetadataRecord, RoutineRunMode,
        RoutineRunOutcomeKind, RoutineSilentPolicy, RoutineTriggerKind,
    };
    use axum::http::StatusCode;
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use ulid::Ulid;

    #[test]
    fn parse_retry_policy_defaults_to_single_attempt() {
        let retry = super::parse_retry_policy(None, None).expect("default retry should parse");

        assert_eq!(retry.max_attempts, 1);
        assert_eq!(retry.backoff_ms, 1_000);
    }

    #[test]
    fn webhook_matcher_requires_matching_identifiers() {
        let metadata = RoutineMetadataRecord {
            routine_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            trigger_kind: RoutineTriggerKind::Webhook,
            trigger_payload_json: json!({
                "integration_id": "repo-a",
                "provider": "github",
                "event": "push",
            })
            .to_string(),
            execution: RoutineExecutionConfig::default(),
            delivery: super::RoutineDeliveryConfig::default(),
            quiet_hours: None,
            cooldown_ms: 0,
            approval_policy: RoutineApprovalPolicy::default(),
            template_id: None,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        assert!(routine_matches_trigger(
            &metadata,
            &json!({ "integration_id": "repo-a", "provider": "github", "event": "push" }),
        ));
        assert!(!routine_matches_trigger(
            &metadata,
            &json!({ "integration_id": "repo-a", "provider": "github", "event": "pull_request" }),
        ));
    }

    #[test]
    fn routine_automation_flag_blocks_enabled_writes_only() {
        assert!(
            !routine_automation_flag_permits_enabled_write(false, true),
            "disabled rollout flag should block writing an effectively enabled routine"
        );
        assert!(
            routine_automation_flag_permits_enabled_write(false, false),
            "disabled routines should remain editable while the rollout flag is disabled"
        );
        assert!(
            routine_automation_flag_permits_enabled_write(true, true),
            "enabled rollout flag should permit enabled routine writes"
        );
    }

    #[test]
    fn routine_cooldown_deadline_saturates_huge_values() {
        assert!(routine_cooldown_deadline_is_after(100, u64::MAX, i64::MAX - 1));
        assert!(!routine_cooldown_deadline_is_after(100, 25, 125));
    }

    #[test]
    fn routine_troubleshooting_prioritizes_disabled_automation_flag() {
        assert_eq!(
            routine_troubleshooting_recommended_action(false, 0, 0, 1),
            "Enable the routines_automation feature flag before enabling or running scheduled routines."
        );
        assert_eq!(
            routine_troubleshooting_recommended_action(true, 0, 0, 1),
            "Review cooldown, quiet hours, or trigger dedupe rules that may be suppressing execution."
        );
    }

    #[test]
    fn routine_approval_subject_round_trips_enable_and_first_run_modes() {
        let routine_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        assert_eq!(
            parse_routine_approval_subject(
                routine_approval_subject_id(routine_id, RoutineApprovalMode::BeforeEnable).as_str()
            ),
            Some((routine_id.to_owned(), RoutineApprovalMode::BeforeEnable))
        );
        assert_eq!(
            parse_routine_approval_subject(
                routine_approval_subject_id(routine_id, RoutineApprovalMode::BeforeFirstRun)
                    .as_str()
            ),
            Some((routine_id.to_owned(), RoutineApprovalMode::BeforeFirstRun))
        );
        assert_eq!(parse_routine_approval_subject("tool:other"), None);
        assert_eq!(parse_routine_approval_subject("routine:not-a-ulid:before_enable"), None);
    }

    #[test]
    fn routine_approval_requires_host_provenance_and_current_definition_hash() {
        let routine_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let principal = "user:ops";
        let mode = RoutineApprovalMode::BeforeEnable;
        let job = CronJobRecord {
            job_id: routine_id.to_owned(),
            name: "health-check".to_owned(),
            prompt: "check health".to_owned(),
            owner_principal: principal.to_owned(),
            channel: "system:routines".to_owned(),
            session_key: None,
            session_label: None,
            workdir: None,
            schedule_type: CronScheduleType::Every,
            schedule_payload_json: json!({ "interval_ms": 60_000_u64 }).to_string(),
            enabled: false,
            concurrency_policy: CronConcurrencyPolicy::Forbid,
            retry_policy: CronRetryPolicy { max_attempts: 1, backoff_ms: 1_000 },
            misfire_policy: CronMisfirePolicy::Skip,
            jitter_ms: 0,
            next_run_at_unix_ms: None,
            last_run_at_unix_ms: None,
            queued_run: false,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        let metadata = RoutineMetadataRecord {
            routine_id: routine_id.to_owned(),
            trigger_kind: RoutineTriggerKind::Schedule,
            trigger_payload_json: json!({}).to_string(),
            execution: RoutineExecutionConfig::default(),
            delivery: RoutineDeliveryConfig::default(),
            quiet_hours: None,
            cooldown_ms: 0,
            approval_policy: RoutineApprovalPolicy { mode },
            template_id: None,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        let subject_id = routine_approval_subject_id(routine_id, mode);
        let details_json = routine_approval_details_json(&job, &metadata, mode);
        let approval = ApprovalRecord {
            approval_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAY".to_owned(),
            principal: principal.to_owned(),
            device_id: super::ROUTINE_APPROVAL_DEVICE_ID.to_owned(),
            channel: Some(job.channel.clone()),
            requested_at_unix_ms: 0,
            resolved_at_unix_ms: Some(1),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: subject_id.clone(),
            request_summary: "routine approval".to_owned(),
            decision: Some(ApprovalDecision::Allow),
            decision_scope: Some(ApprovalDecisionScope::Once),
            decision_reason: Some("approved".to_owned()),
            decision_scope_ttl_ms: None,
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: super::ROUTINE_APPROVAL_POLICY_ID.to_owned(),
                policy_hash: routine_approval_policy_hash(details_json.as_str()),
                evaluation_summary: "routine approval required".to_owned(),
            },
            prompt: ApprovalPromptRecord {
                title: "Approve routine".to_owned(),
                risk_level: ApprovalRiskLevel::High,
                subject_id,
                summary: "Approve routine".to_owned(),
                options: Vec::new(),
                timeout_seconds: 900,
                details_json,
                policy_explanation: "Routine approval".to_owned(),
            },
            created_at_unix_ms: 0,
            updated_at_unix_ms: 1,
        };

        assert!(routine_approval_matches_definition(&approval, principal, &job, &metadata, mode));

        let mut forged = approval.clone();
        forged.device_id = "acp-client".to_owned();
        assert!(!routine_approval_matches_definition(&forged, principal, &job, &metadata, mode));

        let mut stale = metadata.clone();
        stale.execution.execution_posture = RoutineExecutionPosture::SensitiveTools;
        assert!(!routine_approval_matches_definition(&approval, principal, &job, &stale, mode));

        let mut changed_job = job.clone();
        changed_job.prompt = "run a different task".to_owned();
        assert!(!routine_approval_matches_definition(
            &approval,
            principal,
            &changed_job,
            &metadata,
            mode
        ));
    }

    #[test]
    fn schedule_timezone_override_defaults_to_daemon_mode_when_absent() {
        assert_eq!(
            parse_timezone_mode(Some("Europe/Prague"))
                .expect("schedule preview should accept IANA timezone")
                .as_str(),
            "Europe/Prague"
        );
        assert_eq!(
            parse_optional_schedule_timezone_mode(None, CronTimezoneMode::Utc)
                .expect("missing override should use daemon default"),
            CronTimezoneMode::Utc
        );
        assert_eq!(
            parse_optional_schedule_timezone_mode(Some("local"), CronTimezoneMode::Utc)
                .expect("local override should parse"),
            CronTimezoneMode::Local
        );
        assert_eq!(
            parse_optional_schedule_timezone_mode(Some("utc"), CronTimezoneMode::Local)
                .expect("utc override should parse"),
            CronTimezoneMode::Utc
        );
        assert_eq!(
            parse_optional_schedule_timezone_mode(Some("Europe/Prague"), CronTimezoneMode::Utc)
                .expect("IANA timezone override should parse")
                .as_str(),
            "Europe/Prague"
        );
    }

    #[test]
    fn routine_schedule_resolution_applies_explicit_timezone_override() {
        let mut payload = schedule_upsert_payload();
        payload.schedule_type = Some("cron".to_owned());
        payload.cron_expression = Some("0 9 * * 1".to_owned());
        payload.schedule_timezone = Some("local".to_owned());

        let schedule = super::resolve_routine_schedule(
            &payload,
            RoutineTriggerKind::Schedule,
            CronTimezoneMode::Utc,
            None,
        )
        .expect("explicit schedule timezone should override daemon default");
        let schedule_payload: serde_json::Value =
            serde_json::from_str(schedule.schedule_payload_json.as_str())
                .expect("schedule payload should be valid json");
        assert_eq!(schedule_payload["expression"], json!("0 9 * * 1"));
        assert_eq!(schedule_payload["timezone"], json!("local"));
    }

    #[test]
    fn routine_schedule_resolution_embeds_max_runs_limit() {
        let mut payload = schedule_upsert_payload();
        payload.schedule_type = Some("every".to_owned());
        payload.every_interval_ms = Some(60_000);
        payload.max_runs = Some(2);

        let schedule = super::resolve_routine_schedule(
            &payload,
            RoutineTriggerKind::Schedule,
            CronTimezoneMode::Utc,
            None,
        )
        .expect("max_runs should be accepted for scheduled routines");
        let schedule_payload: serde_json::Value =
            serde_json::from_str(schedule.schedule_payload_json.as_str())
                .expect("schedule payload should be valid json");
        assert_eq!(schedule_payload["interval_ms"], json!(60_000));
        assert_eq!(schedule_payload["max_runs"], json!(2));
    }

    #[test]
    fn routine_schedule_resolution_rejects_repeated_one_shot_at_schedule() {
        let mut payload = schedule_upsert_payload();
        payload.schedule_type = Some("at".to_owned());
        payload.at_timestamp_rfc3339 = Some("2100-01-01T00:00:00Z".to_owned());
        payload.max_runs = Some(2);

        let response = super::resolve_routine_schedule(
            &payload,
            RoutineTriggerKind::Schedule,
            CronTimezoneMode::Utc,
            None,
        )
        .expect_err("one-shot schedules should reject repeated max_runs");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn routine_schedule_resolution_prefers_explicit_schedule_over_natural_language_phrase() {
        let mut payload = schedule_upsert_payload();
        payload.schedule_type = Some("every".to_owned());
        payload.every_interval_ms = Some(30_000);
        payload.natural_language_schedule = Some("in 1 second".to_owned());
        payload.max_runs = Some(2);

        let schedule = super::resolve_routine_schedule(
            &payload,
            RoutineTriggerKind::Schedule,
            CronTimezoneMode::Utc,
            None,
        )
        .expect("explicit every schedule should not be downgraded by natural language text");
        let schedule_payload: serde_json::Value =
            serde_json::from_str(schedule.schedule_payload_json.as_str())
                .expect("schedule payload should be valid json");

        assert_eq!(schedule.schedule_type, CronScheduleType::Every);
        assert_eq!(schedule_payload["interval_ms"], json!(30_000));
        assert_eq!(schedule_payload["max_runs"], json!(2));
    }

    #[test]
    fn routine_schedule_resolution_builds_file_watch_poll_schedule() {
        let watched_path =
            std::env::temp_dir().join(format!("palyra-file-watch-{}.txt", Ulid::generate()));
        fs::write(watched_path.as_path(), "baseline").expect("watched fixture should write");
        let mut payload = schedule_upsert_payload();
        payload.trigger_kind = "file_watch".to_owned();
        payload.trigger_payload = Some(json!({
            "path": watched_path.to_string_lossy(),
            "poll_interval_ms": 30_000,
        }));
        payload.max_runs = Some(2);

        let schedule = super::resolve_routine_schedule(
            &payload,
            RoutineTriggerKind::FileWatch,
            CronTimezoneMode::Utc,
            None,
        )
        .expect("file_watch should build a poll schedule");
        let schedule_payload: serde_json::Value =
            serde_json::from_str(schedule.schedule_payload_json.as_str())
                .expect("schedule payload should be valid json");
        let trigger_payload: serde_json::Value = serde_json::from_str(
            schedule
                .trigger_payload_json_override
                .as_deref()
                .expect("file_watch should override trigger payload"),
        )
        .expect("file_watch trigger payload should be valid json");

        assert_eq!(schedule.schedule_type, CronScheduleType::Every);
        assert_eq!(schedule_payload["interval_ms"], json!(30_000));
        assert_eq!(schedule_payload["max_runs"], json!(2));
        assert_eq!(trigger_payload["path"], json!(watched_path.to_string_lossy()));
        assert_eq!(trigger_payload["last_observed"]["exists"], json!(true));

        let _ = fs::remove_file(watched_path);
    }

    #[test]
    fn routine_schedule_resolution_rejects_max_runs_for_non_schedule_trigger() {
        let mut payload = schedule_upsert_payload();
        payload.max_runs = Some(2);

        let response = super::resolve_routine_schedule(
            &payload,
            RoutineTriggerKind::Manual,
            CronTimezoneMode::Utc,
            None,
        )
        .expect_err("max_runs should not be silently ignored outside scheduled routines");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn specific_channel_delivery_requires_explicit_channel() {
        let response = parse_delivery(Some("specific_channel"), None, None, None, None)
            .expect_err("channel should be required");
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        let delivery = parse_delivery(
            Some("specific_channel"),
            Some("ops:routines".to_owned()),
            Some("logs_only"),
            None,
            Some("failure_only"),
        )
        .expect("delivery should parse");
        assert_eq!(delivery.mode, RoutineDeliveryMode::SpecificChannel);
        assert_eq!(delivery.failure_mode, Some(RoutineDeliveryMode::LogsOnly));
        assert_eq!(delivery.silent_policy, RoutineSilentPolicy::FailureOnly);
    }

    fn schedule_upsert_payload() -> super::ConsoleRoutineUpsertRequest {
        super::ConsoleRoutineUpsertRequest {
            routine_id: None,
            name: "schedule".to_owned(),
            prompt: "run schedule".to_owned(),
            owner_principal: None,
            channel: None,
            session_key: None,
            session_label: None,
            workdir: None,
            enabled: None,
            trigger_kind: "schedule".to_owned(),
            trigger_payload: None,
            natural_language_schedule: None,
            schedule_type: None,
            schedule_timezone: None,
            cron_expression: None,
            every_interval_ms: None,
            at_timestamp_rfc3339: None,
            concurrency_policy: None,
            retry_max_attempts: None,
            retry_backoff_ms: None,
            misfire_policy: None,
            jitter_ms: None,
            max_runs: None,
            delivery_mode: None,
            delivery_channel: None,
            delivery_failure_mode: None,
            delivery_failure_channel: None,
            silent_policy: None,
            run_mode: None,
            procedure_profile_id: None,
            skill_profile_id: None,
            provider_profile_id: None,
            execution_posture: None,
            execution_governance: None,
            quiet_hours_start: None,
            quiet_hours_end: None,
            quiet_hours_timezone: None,
            cooldown_ms: None,
            approval_mode: None,
            template_id: None,
        }
    }

    #[test]
    fn normalize_channel_rejects_cross_channel_routine_writes() {
        let response = normalize_channel(Some("tenant:victim"), Some("tenant:operator"))
            .expect_err("requested channel should match session channel");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn normalize_channel_preserves_authorized_routine_channels() {
        assert_eq!(
            normalize_channel(None, Some("tenant:operator")).expect("session channel is valid"),
            "tenant:operator"
        );
        assert_eq!(
            normalize_channel(Some("tenant:operator"), Some("tenant:operator"))
                .expect("matching requested channel is valid"),
            "tenant:operator"
        );
        assert_eq!(
            normalize_channel(Some("system:cron"), Some("tenant:operator"))
                .expect("system cron channel is allowed for scheduler compatibility"),
            "system:cron"
        );
    }

    #[test]
    fn parse_execution_config_accepts_fresh_sensitive_profiled_runs() {
        let execution = parse_execution_config(
            Some("fresh_session"),
            RoutineRunMode::SameSession,
            RoutineExecutionPosture::Standard,
            Some("01ARZ3NDEKTSV4RRFFQ69G5FC0".to_owned()),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FC1".to_owned()),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FC2".to_owned()),
            Some("sensitive_tools"),
        )
        .expect("execution config should parse");
        assert_eq!(execution.run_mode.as_str(), "fresh_session");
        assert_eq!(execution.execution_posture.as_str(), "sensitive_tools");
        assert_eq!(execution.provider_profile_id.as_deref(), Some("01ARZ3NDEKTSV4RRFFQ69G5FC2"));
    }

    #[test]
    fn parse_execution_config_uses_schedule_safe_defaults() {
        let trigger_kind = RoutineTriggerKind::Schedule;
        let execution = parse_execution_config(
            None,
            super::default_run_mode_for_trigger_kind(trigger_kind),
            super::default_execution_posture_for_trigger_kind(trigger_kind, None),
            None,
            None,
            None,
            None,
        )
        .expect("execution config should parse");

        assert_eq!(execution.run_mode, RoutineRunMode::FreshSession);
        assert_eq!(execution.execution_posture, RoutineExecutionPosture::Standard);
    }

    #[test]
    fn parse_execution_config_defaults_schedule_workdirs_to_sensitive_tools() {
        let trigger_kind = RoutineTriggerKind::Schedule;
        let execution = parse_execution_config(
            None,
            super::default_run_mode_for_trigger_kind(trigger_kind),
            super::default_execution_posture_for_trigger_kind(trigger_kind, Some("C:\\workspace")),
            None,
            None,
            None,
            None,
        )
        .expect("execution config should parse");

        assert_eq!(execution.run_mode, RoutineRunMode::FreshSession);
        assert_eq!(execution.execution_posture, RoutineExecutionPosture::SensitiveTools);
    }

    #[test]
    fn parse_execution_config_defaults_file_watch_to_fresh_sensitive_runs() {
        let trigger_kind = RoutineTriggerKind::FileWatch;
        let execution = parse_execution_config(
            None,
            super::default_run_mode_for_trigger_kind(trigger_kind),
            super::default_execution_posture_for_trigger_kind(trigger_kind, None),
            None,
            None,
            None,
            None,
        )
        .expect("file watch execution config should parse");

        assert_eq!(execution.run_mode, RoutineRunMode::FreshSession);
        assert_eq!(execution.execution_posture, RoutineExecutionPosture::SensitiveTools);
    }

    #[test]
    fn parse_execution_config_preserves_explicit_same_session() {
        let execution = parse_execution_config(
            Some("same_session"),
            RoutineRunMode::FreshSession,
            RoutineExecutionPosture::SensitiveTools,
            None,
            None,
            None,
            None,
        )
        .expect("execution config should parse");

        assert_eq!(execution.run_mode, RoutineRunMode::SameSession);
        assert_eq!(execution.execution_posture, RoutineExecutionPosture::SensitiveTools);
    }

    #[test]
    fn parse_execution_config_preserves_explicit_standard_posture() {
        let execution = parse_execution_config(
            None,
            RoutineRunMode::FreshSession,
            RoutineExecutionPosture::SensitiveTools,
            None,
            None,
            None,
            Some("standard"),
        )
        .expect("execution config should parse");

        assert_eq!(execution.execution_posture, RoutineExecutionPosture::Standard);
    }

    #[test]
    fn parse_execution_config_preserves_explicit_sensitive_tools_posture() {
        let execution = parse_execution_config(
            None,
            RoutineRunMode::FreshSession,
            RoutineExecutionPosture::Standard,
            None,
            None,
            None,
            Some("sensitive_tools"),
        )
        .expect("execution config should parse");

        assert_eq!(execution.execution_posture, RoutineExecutionPosture::SensitiveTools);
    }

    #[test]
    fn resolve_upsert_approval_policy_defaults_new_routines_to_none() {
        let approval_policy = super::resolve_upsert_approval_policy(None, None)
            .expect("omitted approval should use the approval-free default");

        assert_eq!(approval_policy.mode, RoutineApprovalMode::None);
    }

    #[test]
    fn resolve_upsert_approval_policy_preserves_and_clears_explicit_safe_mode() {
        let metadata = RoutineMetadataRecord {
            routine_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            trigger_kind: RoutineTriggerKind::Schedule,
            trigger_payload_json: json!({"schedule_type": "every"}).to_string(),
            execution: RoutineExecutionConfig::default(),
            delivery: RoutineDeliveryConfig::default(),
            quiet_hours: None,
            cooldown_ms: 0,
            approval_policy: RoutineApprovalPolicy { mode: RoutineApprovalMode::BeforeFirstRun },
            template_id: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        };
        let preserved = super::resolve_upsert_approval_policy(None, Some(&metadata))
            .expect("omitted approval should preserve an existing explicit safe mode");
        let cleared = super::resolve_upsert_approval_policy(Some("none"), Some(&metadata))
            .expect("explicit none should clear an existing safe mode");

        assert_eq!(preserved.mode, RoutineApprovalMode::BeforeFirstRun);
        assert_eq!(cleared.mode, RoutineApprovalMode::None);
    }

    #[test]
    fn cron_trigger_options_allow_configured_sensitive_tools_without_approval() {
        let execution = RoutineExecutionConfig {
            run_mode: RoutineRunMode::FreshSession,
            execution_posture: RoutineExecutionPosture::SensitiveTools,
            ..RoutineExecutionConfig::default()
        };

        let options = super::build_cron_trigger_options(
            &execution,
            super::RoutineDispatchMode::Normal,
            None,
            None,
        );
        assert_eq!(options.allow_sensitive_tools, Some(true));
    }

    #[test]
    fn quiet_hours_wrap_midnight_in_utc() {
        let quiet_hours = parse_quiet_hours(Some("22:00"), Some("06:00"), Some("utc".to_owned()))
            .expect("quiet hours should parse")
            .expect("quiet hours should exist");
        let late_evening = Utc
            .with_ymd_and_hms(2026, 4, 3, 23, 15, 0)
            .single()
            .expect("timestamp should build")
            .timestamp_millis();
        let morning = Utc
            .with_ymd_and_hms(2026, 4, 4, 5, 30, 0)
            .single()
            .expect("timestamp should build")
            .timestamp_millis();
        let noon = Utc
            .with_ymd_and_hms(2026, 4, 4, 12, 0, 0)
            .single()
            .expect("timestamp should build")
            .timestamp_millis();
        assert!(is_in_quiet_hours(Some(&quiet_hours), late_evening)
            .expect("quiet hours should evaluate"));
        assert!(
            is_in_quiet_hours(Some(&quiet_hours), morning).expect("quiet hours should evaluate")
        );
        assert!(!is_in_quiet_hours(Some(&quiet_hours), noon).expect("quiet hours should evaluate"));
    }

    #[test]
    fn quiet_hours_accept_named_iana_timezone() {
        let quiet_hours =
            parse_quiet_hours(Some("09:00"), Some("10:00"), Some("Europe/Prague".to_owned()))
                .expect("named quiet hours timezone should parse")
                .expect("quiet hours should exist");
        assert_eq!(quiet_hours.timezone.as_deref(), Some("Europe/Prague"));
        let prague_morning = Utc
            .with_ymd_and_hms(2026, 5, 24, 7, 30, 0)
            .single()
            .expect("timestamp should build")
            .timestamp_millis();

        assert!(is_in_quiet_hours(Some(&quiet_hours), prague_morning)
            .expect("named quiet hours should evaluate"));
    }

    #[test]
    fn compare_optional_matchers_allows_unset_expectation() {
        assert!(compare_optional_matchers(None, Some("anything")));
        assert!(compare_optional_matchers(Some("push"), Some("push")));
        assert!(!compare_optional_matchers(Some("push"), Some("deploy")));
    }

    #[test]
    fn routine_output_fields_expose_owner_session_preview() {
        let session = OrchestratorSessionRecord {
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            session_key: "cron:daily; echo injected".to_owned(),
            session_label: Some("Daily routine".to_owned()),
            principal: "operator".to_owned(),
            device_id: "system:scheduler".to_owned(),
            channel: Some("system:routines".to_owned()),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            last_run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned()),
            archived_at_unix_ms: None,
            auto_title: None,
            auto_title_source: None,
            auto_title_generator_version: None,
            auto_title_updated_at_unix_ms: None,
            title_generation_state: "manual".to_owned(),
            manual_title_locked: false,
            manual_title_updated_at_unix_ms: None,
            model_profile_override: None,
            thinking_override: None,
            trace_override: None,
            verbose_override: None,
            title: "Daily routine".to_owned(),
            title_source: "manual".to_owned(),
            title_generator_version: None,
            preview: Some("ROUTINE_OK".to_owned()),
            last_intent: None,
            last_summary: Some("ROUTINE_OK".to_owned()),
            match_snippet: None,
            branch_state: "root".to_owned(),
            parent_session_id: None,
            branch_origin_run_id: None,
            last_run_state: Some("succeeded".to_owned()),
        };

        let fields = routine_output_fields_from_session(
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "operator",
            Some("01ARZ3NDEKTSV4RRFFQ69G5FB0"),
            None,
            true,
            &session,
        )
        .expect("owner session output should be visible");

        assert_eq!(
            fields.get("output_preview").and_then(|value| value.as_str()),
            Some("ROUTINE_OK")
        );
        assert_eq!(
            fields.get("output_summary").and_then(|value| value.as_str()),
            Some("ROUTINE_OK")
        );
        assert_eq!(
            fields.get("output_source").and_then(|value| value.as_str()),
            Some("session_preview")
        );
        assert_eq!(
            fields
                .get("output_lookup")
                .and_then(|value| value.get("session_id"))
                .and_then(|value| value.as_str()),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAW")
        );
        assert_eq!(
            fields.get("session_key").and_then(|value| value.as_str()),
            Some("cron:daily; echo injected")
        );
        assert_eq!(
            fields
                .get("output_lookup")
                .and_then(|value| value.get("session_key"))
                .and_then(|value| value.as_str()),
            Some("cron:daily; echo injected")
        );
        assert!(fields.get("output_lookup").and_then(|value| value.get("command")).is_none());
        assert!(
            routine_output_fields_from_session(
                "01ARZ3NDEKTSV4RRFFQ69G5FAW",
                "other",
                Some("01ARZ3NDEKTSV4RRFFQ69G5FB0"),
                None,
                true,
                &session,
            )
            .is_none(),
            "routine logs must not expose output from another principal"
        );

        let mut stale_session = session.clone();
        stale_session.last_run_id = Some("01ARZ3NDEKTSV4RRFFQ69G5FC0".to_owned());
        let stale_fields = routine_output_fields_from_session(
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "operator",
            Some("01ARZ3NDEKTSV4RRFFQ69G5FB0"),
            None,
            true,
            &stale_session,
        )
        .expect("owner lookup should still be visible");

        assert!(
            stale_fields.get("output_preview").is_none(),
            "stale same-session previews must not be copied onto older routine runs"
        );
        assert_eq!(
            stale_fields.get("output_source").and_then(|value| value.as_str()),
            Some("session_lookup")
        );

        let tape_fields = routine_output_fields_from_session(
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "operator",
            Some("01ARZ3NDEKTSV4RRFFQ69G5FB0"),
            Some("IMMUTABLE_RUN_OUTPUT"),
            true,
            &stale_session,
        )
        .expect("owner tape output should be visible");
        assert_eq!(
            tape_fields.get("output_preview").and_then(|value| value.as_str()),
            Some("IMMUTABLE_RUN_OUTPUT")
        );
        assert_eq!(
            tape_fields.get("output_source").and_then(|value| value.as_str()),
            Some("run_tape")
        );

        let suppressed_fields = routine_output_fields_from_session(
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "operator",
            Some("01ARZ3NDEKTSV4RRFFQ69G5FB0"),
            Some("WATCH_KEEP_DONE"),
            false,
            &stale_session,
        )
        .expect("owner lookup should remain visible when preview is suppressed");
        assert!(
            suppressed_fields.get("output_preview").is_none(),
            "denied and failed routine runs must not surface success-looking tape replies"
        );
        assert_eq!(
            suppressed_fields.get("output_source").and_then(|value| value.as_str()),
            Some("suppressed_terminal_failure")
        );
    }

    #[test]
    fn routine_output_text_redacts_secrets_and_line_controls() {
        let normalized = normalize_routine_output_text(Some(
            "done\nAuthorization: Bearer sk-live-abcdefghijklmnopqrstuvwx\t\
             https://example.test/callback?token=secret-value",
        ))
        .expect("redacted preview should remain present");

        assert!(normalized.starts_with("done "));
        assert!(!normalized.contains("sk-live-abcdefghijklmnopqrstuvwx"));
        assert!(!normalized.contains("secret-value"));
        assert!(!normalized.chars().any(char::is_control));
    }

    #[test]
    fn denied_routine_output_is_not_delivered_or_previewed() {
        let delivery = RoutineDeliveryConfig::default();

        assert!(!output_delivered_for_outcome(&delivery, RoutineRunOutcomeKind::Denied));
        assert!(output_delivered_for_outcome(&delivery, RoutineRunOutcomeKind::SuccessWithOutput));
        assert!(!routine_run_allows_output_preview(&json!({
            "outcome_kind": "denied"
        })));
        assert!(!routine_run_allows_output_preview(&json!({
            "outcome_kind": "failed"
        })));
        assert!(routine_run_allows_output_preview(&json!({
            "outcome_kind": "success_with_output"
        })));
    }

    #[test]
    fn routine_output_text_from_tape_events_uses_latest_run_reply() {
        let events = vec![
            OrchestratorTapeRecord {
                seq: 1,
                event_type: "message.replied".to_owned(),
                payload_json: r#"{"reply_text":"FIRST"}"#.to_owned(),
            },
            OrchestratorTapeRecord {
                seq: 2,
                event_type: "tool_result".to_owned(),
                payload_json: r#"{"text":"ignored"}"#.to_owned(),
            },
            OrchestratorTapeRecord {
                seq: 3,
                event_type: "message.replied".to_owned(),
                payload_json: r#"{"reply_text":"SECOND"}"#.to_owned(),
            },
        ];

        assert_eq!(
            routine_output_text_from_tape_events(events.as_slice()).as_deref(),
            Some("SECOND")
        );
    }
}
