use std::{collections::HashMap, sync::Arc};

use chrono::{TimeZone, Timelike};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::diagnostics::{authorize_console_session, build_page_info};

use crate::{
    cron::{self, CronTimezoneMode},
    gateway::proto::palyra::cron::v1 as cron_v1,
    journal::{
        ApprovalCreateRequest, ApprovalPolicySnapshot, ApprovalPromptOption,
        ApprovalPromptRecord, ApprovalRiskLevel, CronConcurrencyPolicy, CronJobCreateRequest,
        CronJobRecord, CronJobUpdatePatch, CronMisfirePolicy, CronRetryPolicy,
        CronRunFinalizeRequest, CronRunStartRequest, CronRunStatus, CronScheduleType,
    },
    routines::{
        build_routine_export_bundle, default_outcome_from_cron_status, join_run_metadata,
        natural_language_schedule_preview, routine_templates, shadow_manual_schedule_payload_json,
        validate_routine_export_bundle, RoutineApprovalMode, RoutineApprovalPolicy,
        RoutineDeliveryConfig, RoutineDeliveryMode, RoutineExportBundle, RoutineMetadataRecord,
        RoutineMetadataUpsert, RoutineQuietHours, RoutineRegistryError, RoutineRunMetadataUpsert,
        RoutineRunOutcomeKind, RoutineTriggerKind, ROUTINE_TEMPLATE_PACK_VERSION,
    },
    *,
};

const DEFAULT_ROUTINE_CHANNEL: &str = "system:routines";
const DEFAULT_ROUTINE_PAGE_LIMIT: usize = 100;
const MAX_ROUTINE_PAGE_LIMIT: usize = 500;
const ROUTINE_APPROVAL_TIMEOUT_SECONDS: u32 = 900;
const ROUTINE_APPROVAL_DEVICE_ID: &str = "system:routines";

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
    enabled: Option<bool>,
    trigger_kind: String,
    #[serde(default)]
    trigger_payload: Option<Value>,
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
    delivery_mode: Option<String>,
    #[serde(default)]
    delivery_channel: Option<String>,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineEnabledRequest {
    enabled: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleRoutineRunsQuery {
    #[serde(default)]
    after_run_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineSchedulePreviewRequest {
    phrase: String,
    #[serde(default)]
    timezone: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleRoutineImportRequest {
    export: RoutineExportBundle,
    #[serde(default)]
    routine_id: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

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
    if let Some(after_routine_id) = query
        .after_routine_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        filtered.retain(|routine| read_string_value(routine, "routine_id").as_str() > after_routine_id);
    }
    let has_more = filtered.len() > limit;
    if has_more {
        filtered.truncate(limit);
    }
    let next_after_routine_id =
        if has_more { filtered.last().and_then(|routine| routine.get("routine_id")).and_then(Value::as_str).map(ToOwned::to_owned) } else { None };

    Ok(Json(json!({
        "routines": filtered,
        "next_after_routine_id": next_after_routine_id,
        "page": build_page_info(limit, filtered.len(), next_after_routine_id.clone()),
    })))
}

pub(crate) async fn console_routine_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let routine =
        load_routine_view_for_owner(&state, routine_id.as_str(), session.context.principal.as_str())
            .await?;
    Ok(Json(json!({ "routine": routine })))
}

pub(crate) async fn console_routine_export_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let routine =
        load_routine_parts_for_owner(&state, routine_id.as_str(), session.context.principal.as_str())
            .await?;
    let export = build_routine_export_bundle(&routine.job, &routine.metadata)
        .map_err(routine_registry_error_response)?;
    Ok(Json(json!({ "export": export })))
}

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
    let channel = normalize_channel(Some(bundle.job.channel.as_str()), session.context.channel.as_deref());
    let requested_enabled = payload.enabled.unwrap_or(bundle.job.enabled);
    let approval_required = requested_enabled
        && bundle.routine.approval_policy.mode == RoutineApprovalMode::BeforeEnable
        && !routine_approval_granted(
            &state,
            routine_approval_subject_id(routine_id.as_str(), RoutineApprovalMode::BeforeEnable),
        )
        .await?;
    let job_record = persist_routine_job(
        &state,
        state.runtime.cron_job(routine_id.clone()).await.map_err(runtime_status_response)?,
        RoutineJobUpsert {
            routine_id: routine_id.clone(),
            name: bundle.job.name.clone(),
            prompt: bundle.job.prompt.clone(),
            owner_principal: owner_principal.clone(),
            channel,
            session_key: bundle.job.session_key.clone(),
            session_label: bundle.job.session_label.clone(),
            schedule_type: bundle.job.schedule_type,
            schedule_payload_json: bundle.job.schedule_payload_json.clone(),
            enabled: requested_enabled && !approval_required,
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
            delivery: bundle.routine.delivery.clone(),
            quiet_hours: bundle.routine.quiet_hours.clone(),
            cooldown_ms: bundle.routine.cooldown_ms,
            approval_policy: bundle.routine.approval_policy.clone(),
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

pub(crate) async fn console_routine_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleRoutineUpsertRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let routine_id = match payload.routine_id.as_deref().map(str::trim) {
        Some("") | None => Ulid::new().to_string(),
        Some(value) => {
            validate_canonical_id(value).map_err(|_| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "routine_id must be a canonical ULID when provided",
                ))
            })?;
            value.to_owned()
        }
    };
    let trigger_kind =
        parse_routine_trigger_kind(payload.trigger_kind.as_str()).map_err(runtime_status_response)?;
    let owner_principal =
        normalize_owner_principal(&payload.owner_principal, session.context.principal.as_str())?;
    let channel = normalize_channel(payload.channel.as_deref(), session.context.channel.as_deref());
    let enabled = payload.enabled.unwrap_or(true);

    let existing_job = state
        .runtime
        .cron_job(routine_id.clone())
        .await
        .map_err(runtime_status_response)?;
    if let Some(job) = existing_job.as_ref() {
        ensure_job_owner(job, session.context.principal.as_str())?;
    }

    let schedule = resolve_routine_schedule(&payload, trigger_kind, state.cron_timezone_mode)?;
    let delivery = parse_delivery(payload.delivery_mode.as_deref(), payload.delivery_channel)?;
    let quiet_hours = parse_quiet_hours(
        payload.quiet_hours_start.as_deref(),
        payload.quiet_hours_end.as_deref(),
        payload.quiet_hours_timezone,
    )?;
    let approval_policy = parse_approval_policy(payload.approval_mode.as_deref())?;
    let concurrency_policy = parse_concurrency_policy(payload.concurrency_policy.as_deref())?;
    let retry_policy =
        parse_retry_policy(payload.retry_max_attempts, payload.retry_backoff_ms)?;
    let misfire_policy = parse_misfire_policy(payload.misfire_policy.as_deref())?;
    let approval_required = enabled
        && approval_policy.mode == RoutineApprovalMode::BeforeEnable
        && !routine_approval_granted(
            &state,
            routine_approval_subject_id(routine_id.as_str(), RoutineApprovalMode::BeforeEnable),
        )
        .await?;
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
            schedule_type: schedule.schedule_type,
            schedule_payload_json: schedule.schedule_payload_json.clone(),
            enabled: enabled && !approval_required,
            concurrency_policy,
            retry_policy: retry_policy.clone(),
            misfire_policy,
            jitter_ms: payload.jitter_ms.unwrap_or(0),
            next_run_at_unix_ms: schedule.next_run_at_unix_ms,
        },
    )
    .await?;
    state.scheduler_wake.notify_one();

    let trigger_payload_json = if trigger_kind == RoutineTriggerKind::Schedule {
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

pub(crate) async fn console_routine_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let routine =
        load_routine_parts_for_owner(&state, routine_id.as_str(), session.context.principal.as_str())
            .await?;
    let deleted = state
        .runtime
        .delete_cron_job(routine.job.job_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let _ = state
        .routines
        .delete_routine(routine.metadata.routine_id.as_str())
        .map_err(routine_registry_error_response)?;
    Ok(Json(json!({
        "deleted": deleted,
        "routine_id": routine.metadata.routine_id,
    })))
}

pub(crate) async fn console_routine_set_enabled_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
    Json(payload): Json<ConsoleRoutineEnabledRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let routine =
        load_routine_parts_for_owner(&state, routine_id.as_str(), session.context.principal.as_str())
            .await?;
    let approval_required = payload.enabled
        && routine.metadata.approval_policy.mode == RoutineApprovalMode::BeforeEnable
        && !routine_approval_granted(
            &state,
            routine_approval_subject_id(
                routine.metadata.routine_id.as_str(),
                RoutineApprovalMode::BeforeEnable,
            ),
        )
        .await?;
    let updated = state
        .runtime
        .update_cron_job(
            routine.job.job_id.clone(),
            CronJobUpdatePatch {
                enabled: Some(payload.enabled && !approval_required),
                ..CronJobUpdatePatch::default()
            },
        )
        .await
        .map_err(runtime_status_response)?;
    state.scheduler_wake.notify_one();
    Ok(Json(json!({
        "routine": routine_view_from_parts(&updated, &routine.metadata),
        "approval": if approval_required {
            Some(
                ensure_routine_approval_requested(
                    &state,
                    session.context.principal.as_str(),
                    session.context.channel.as_deref(),
                    &updated,
                    &routine.metadata,
                    RoutineApprovalMode::BeforeEnable,
                )
                .await?,
            )
        } else {
            None
        },
    })))
}

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
        RoutineTriggerKind::Manual,
        Some("manual run-now".to_owned()),
        json!({ "source": "manual_run_now" }),
        None,
    )
    .await?;
    Ok(Json(outcome))
}

pub(crate) async fn console_routine_runs_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
    Query(query): Query<ConsoleRoutineRunsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let routine =
        load_routine_parts_for_owner(&state, routine_id.as_str(), session.context.principal.as_str())
            .await?;
    let limit = query.limit.unwrap_or(DEFAULT_ROUTINE_PAGE_LIMIT).clamp(1, MAX_ROUTINE_PAGE_LIMIT);
    let (runs, next_after_run_id) = state
        .runtime
        .list_cron_runs(Some(routine.job.job_id.clone()), query.after_run_id.clone(), Some(limit))
        .await
        .map_err(runtime_status_response)?;
    let mapped_runs = runs
        .iter()
        .map(|run| {
            let metadata = state
                .routines
                .find_run_metadata(run.run_id.as_str())
                .map_err(routine_registry_error_response)?;
            Ok(join_run_metadata(routine.metadata.routine_id.as_str(), run, metadata.as_ref()))
        })
        .collect::<Result<Vec<_>, Response>>()?;
    Ok(Json(json!({
        "runs": mapped_runs,
        "next_after_run_id": next_after_run_id,
        "page": build_page_info(limit, mapped_runs.len(), next_after_run_id.clone()),
    })))
}

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
        trigger_kind,
        payload.trigger_reason,
        payload.trigger_payload.unwrap_or_else(|| json!({})),
        payload.trigger_dedupe_key,
    )
    .await?;
    Ok(Json(outcome))
}

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
        object.insert(
            "integration_id".to_owned(),
            Value::String(integration_id.to_owned()),
        );
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
    schedule_type: CronScheduleType,
    schedule_payload_json: String,
    enabled: bool,
    concurrency_policy: CronConcurrencyPolicy,
    retry_policy: CronRetryPolicy,
    misfire_policy: CronMisfirePolicy,
    jitter_ms: u64,
    next_run_at_unix_ms: Option<i64>,
}

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
    let job_map =
        jobs.into_iter().map(|job| (job.job_id.clone(), job)).collect::<HashMap<_, _>>();
    let mut routines = Vec::new();
    for metadata in state
        .routines
        .list_routines()
        .map_err(routine_registry_error_response)?
        .into_iter()
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
            runtime_status_response(tonic::Status::not_found(
                "routine backing cron job not found",
            ))
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

async fn enrich_routine_view_with_latest_run(
    state: &AppState,
    mut view: Value,
    routine_id: &str,
    job_id: &str,
) -> Result<Value, Response> {
    let (runs, _) = state
        .runtime
        .list_cron_runs(Some(job_id.to_owned()), None, Some(1))
        .await
        .map_err(runtime_status_response)?;
    let Some(run) = runs.last() else {
        return Ok(view);
    };
    let metadata = state
        .routines
        .find_run_metadata(run.run_id.as_str())
        .map_err(routine_registry_error_response)?;
    let latest_run = join_run_metadata(routine_id, run, metadata.as_ref());
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
    }
    Ok(view)
}

fn routine_approval_subject_id(routine_id: &str, mode: RoutineApprovalMode) -> String {
    format!("routine:{routine_id}:{}", mode.as_str())
}

async fn routine_approval_granted(
    state: &AppState,
    subject_id: String,
) -> Result<bool, Response> {
    let (approvals, _) = state
        .runtime
        .list_approval_records(
            None,
            Some(25),
            None,
            None,
            Some(subject_id),
            None,
            Some(ApprovalDecision::Allow),
            Some(ApprovalSubjectType::Tool),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(approvals
        .into_iter()
        .any(|approval| matches!(approval.decision, Some(ApprovalDecision::Allow))))
}

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
    if let Some(approval) = existing
        .into_iter()
        .rev()
        .find(|approval| approval.subject_id == subject_id && approval.decision.is_none())
    {
        return serde_json::to_value(approval).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to serialize routine approval record: {error}"
            )))
        });
    }

    let details_json = json!({
        "routine_id": metadata.routine_id,
        "name": job.name,
        "approval_mode": mode.as_str(),
        "trigger_kind": metadata.trigger_kind.as_str(),
        "delivery_mode": metadata.delivery.mode.as_str(),
        "channel": job.channel,
        "template_id": metadata.template_id,
    })
    .to_string();
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
    let policy_hash = hex::encode(Sha256::digest(details_json.as_bytes()));
    let record = state
        .runtime
        .create_approval_record(ApprovalCreateRequest {
            approval_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            principal: principal.to_owned(),
            device_id: ROUTINE_APPROVAL_DEVICE_ID.to_owned(),
            channel: channel.map(ToOwned::to_owned),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: subject_id.clone(),
            request_summary: format!(
                "routine_id={} routine_name={} approval_mode={}",
                metadata.routine_id,
                job.name,
                mode.as_str()
            ),
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: "routine.approval.v1".to_owned(),
                policy_hash,
                evaluation_summary: format!(
                    "routine approval required mode={} trigger={} delivery={}",
                    mode.as_str(),
                    metadata.trigger_kind.as_str(),
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

async fn dispatch_single_routine(
    state: &AppState,
    routine_id: &str,
    principal: &str,
    trigger_kind: RoutineTriggerKind,
    trigger_reason: Option<String>,
    trigger_payload: Value,
    trigger_dedupe_key: Option<String>,
) -> Result<Value, Response> {
    let routine = load_routine_parts_for_owner(state, routine_id, principal).await?;
    if !routine.job.enabled {
        return register_terminal_routine_run(
            state,
            routine.job.job_id.as_str(),
            trigger_kind,
            trigger_reason,
            &trigger_payload,
            trigger_dedupe_key,
            routine.metadata.delivery.clone(),
            CronRunStatus::Skipped,
            RoutineRunOutcomeKind::Skipped,
            "routine is disabled",
        )
        .await;
    }
    if routine.metadata.approval_policy.mode == RoutineApprovalMode::BeforeFirstRun
        && !routine_approval_granted(
            state,
            routine_approval_subject_id(
                routine.metadata.routine_id.as_str(),
                RoutineApprovalMode::BeforeFirstRun,
            ),
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
            routine.job.job_id.as_str(),
            trigger_kind,
            trigger_reason,
            &trigger_payload,
            trigger_dedupe_key,
            routine.metadata.delivery.clone(),
            CronRunStatus::Denied,
            RoutineRunOutcomeKind::Denied,
            "routine approval is required before the first run",
        )
        .await?;
        if let Some(object) = response.as_object_mut() {
            object.insert("approval".to_owned(), approval);
        }
        return Ok(response);
    }
    if routine.metadata.trigger_kind != trigger_kind && trigger_kind != RoutineTriggerKind::Manual {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "trigger_kind does not match routine definition",
        )));
    }
    if let Some(dedupe_key) = trigger_dedupe_key.as_deref() {
        let seen = state
            .routines
            .seen_dedupe_key(routine.metadata.routine_id.as_str(), dedupe_key)
            .map_err(routine_registry_error_response)?;
        if seen {
            return register_terminal_routine_run(
                state,
                routine.job.job_id.as_str(),
                trigger_kind,
                trigger_reason,
                &trigger_payload,
                Some(dedupe_key.to_owned()),
                routine.metadata.delivery.clone(),
                CronRunStatus::Skipped,
                RoutineRunOutcomeKind::Throttled,
                "duplicate trigger dedupe key already processed",
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
            entry.created_at_unix_ms + routine.metadata.cooldown_ms as i64 > now_unix_ms
        }) {
            return register_terminal_routine_run(
                state,
                routine.job.job_id.as_str(),
                trigger_kind,
                trigger_reason,
                &trigger_payload,
                trigger_dedupe_key,
                routine.metadata.delivery.clone(),
                CronRunStatus::Skipped,
                RoutineRunOutcomeKind::Throttled,
                "routine cooldown window is still active",
            )
            .await;
        }
    }
    if is_in_quiet_hours(routine.metadata.quiet_hours.as_ref(), now_unix_ms)? {
        return register_terminal_routine_run(
            state,
            routine.job.job_id.as_str(),
            trigger_kind,
            trigger_reason,
            &trigger_payload,
            trigger_dedupe_key,
            routine.metadata.delivery.clone(),
            CronRunStatus::Skipped,
            RoutineRunOutcomeKind::Skipped,
            "routine quiet hours suppress execution",
        )
        .await;
    }

    let outcome = cron::trigger_job_now(
        Arc::clone(&state.runtime),
        state.auth.clone(),
        state.grpc_url.clone(),
        routine.job.clone(),
        Arc::clone(&state.scheduler_wake),
    )
    .await
    .map_err(runtime_status_response)?;
    if let Some(run_id) = outcome.run_id.as_ref() {
        let _ = state
            .routines
            .upsert_run_metadata(RoutineRunMetadataUpsert {
                run_id: run_id.clone(),
                routine_id: routine.metadata.routine_id.clone(),
                trigger_kind,
                trigger_reason,
                trigger_payload_json: serde_json::to_string(&trigger_payload).map_err(|error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to serialize trigger payload: {error}"
                    )))
                })?,
                trigger_dedupe_key,
                delivery: routine.metadata.delivery.clone(),
                outcome_override: Some(default_outcome_from_cron_status(outcome.status)),
                outcome_message: Some(outcome.message.clone()),
                output_delivered: Some(!matches!(
                    routine.metadata.delivery.mode,
                    RoutineDeliveryMode::LogsOnly | RoutineDeliveryMode::LocalOnly
                )),
            })
            .map_err(routine_registry_error_response)?;
    }
    Ok(json!({
        "routine_id": routine.metadata.routine_id,
        "run_id": outcome.run_id,
        "status": outcome.status.as_str(),
        "message": outcome.message,
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
    let job_map =
        jobs.into_iter().map(|job| (job.job_id.clone(), job)).collect::<HashMap<_, _>>();
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
        if job.owner_principal != principal || !routine_matches_trigger(&metadata, &trigger_payload) {
            continue;
        }
        let outcome = dispatch_single_routine(
            state,
            metadata.routine_id.as_str(),
            principal,
            trigger_kind,
            trigger_reason.clone(),
            trigger_payload.clone(),
            trigger_dedupe_key.clone(),
        )
        .await?;
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

fn routine_matches_trigger(metadata: &RoutineMetadataRecord, payload: &Value) -> bool {
    let configured =
        serde_json::from_str::<Value>(metadata.trigger_payload_json.as_str()).unwrap_or_else(|_| {
            json!({})
        });
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
        RoutineTriggerKind::Schedule => false,
    }
}

fn compare_optional_matchers(expected: Option<&str>, actual: Option<&str>) -> bool {
    expected.is_none_or(|expected| actual.is_some_and(|actual| expected.eq_ignore_ascii_case(actual)))
}

async fn register_terminal_routine_run(
    state: &AppState,
    routine_id: &str,
    trigger_kind: RoutineTriggerKind,
    trigger_reason: Option<String>,
    trigger_payload: &Value,
    trigger_dedupe_key: Option<String>,
    delivery: RoutineDeliveryConfig,
    status: CronRunStatus,
    outcome_override: RoutineRunOutcomeKind,
    message: &str,
) -> Result<Value, Response> {
    let run_id = Ulid::new().to_string();
    state
        .runtime
        .start_cron_run(CronRunStartRequest {
            run_id: run_id.clone(),
            job_id: routine_id.to_owned(),
            attempt: 1,
            session_id: None,
            orchestrator_run_id: None,
            status,
            error_kind: Some("routine_gate".to_owned()),
            error_message_redacted: Some(message.to_owned()),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .finalize_cron_run(CronRunFinalizeRequest {
            run_id: run_id.clone(),
            status,
            error_kind: Some("routine_gate".to_owned()),
            error_message_redacted: Some(message.to_owned()),
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
            routine_id: routine_id.to_owned(),
            trigger_kind,
            trigger_reason,
            trigger_payload_json: serde_json::to_string(trigger_payload).map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to serialize trigger payload: {error}"
                )))
            })?,
            trigger_dedupe_key,
            delivery,
            outcome_override: Some(outcome_override),
            outcome_message: Some(message.to_owned()),
            output_delivered: Some(false),
        })
        .map_err(routine_registry_error_response)?;
    Ok(json!({
        "routine_id": routine_id,
        "run_id": run_id,
        "status": status.as_str(),
        "message": message,
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
        "enabled": job.enabled,
        "schedule_type": job.schedule_type.as_str(),
        "schedule_payload": serde_json::from_str::<Value>(job.schedule_payload_json.as_str()).unwrap_or_else(|_| json!({ "raw": job.schedule_payload_json })),
        "next_run_at_unix_ms": job.next_run_at_unix_ms,
        "last_run_at_unix_ms": job.last_run_at_unix_ms,
        "queued_run": job.queued_run,
        "concurrency_policy": job.concurrency_policy.as_str(),
        "retry_policy": {
            "max_attempts": job.retry_policy.max_attempts,
            "backoff_ms": job.retry_policy.backoff_ms,
        },
        "misfire_policy": job.misfire_policy.as_str(),
        "jitter_ms": job.jitter_ms,
        "trigger_kind": metadata.trigger_kind.as_str(),
        "trigger_payload": serde_json::from_str::<Value>(metadata.trigger_payload_json.as_str()).unwrap_or_else(|_| json!({ "raw": metadata.trigger_payload_json })),
        "delivery_mode": metadata.delivery.mode.as_str(),
        "delivery_channel": metadata.delivery.channel,
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

fn resolve_routine_schedule(
    payload: &ConsoleRoutineUpsertRequest,
    trigger_kind: RoutineTriggerKind,
    timezone_mode: CronTimezoneMode,
) -> Result<ScheduleResolution, Response> {
    if trigger_kind != RoutineTriggerKind::Schedule {
        return Ok(ScheduleResolution {
            schedule_type: CronScheduleType::At,
            schedule_payload_json: shadow_manual_schedule_payload_json(),
            next_run_at_unix_ms: None,
        });
    }
    if let Some(phrase) = payload
        .natural_language_schedule
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let preview = natural_language_schedule_preview(
            phrase,
            timezone_mode,
            unix_ms_now().map_err(internal_console_error)?,
        )
        .map_err(routine_registry_error_response)?;
        return Ok(ScheduleResolution {
            schedule_type: parse_schedule_type(preview.schedule_type.as_str())?,
            schedule_payload_json: preview.schedule_payload_json,
            next_run_at_unix_ms: preview.next_run_at_unix_ms,
        });
    }
    let schedule =
        build_console_schedule(payload.schedule_type.as_deref(), payload).map_err(runtime_status_response)?;
    let normalized =
        cron::normalize_schedule(Some(schedule), unix_ms_now().map_err(internal_console_error)?, timezone_mode)
            .map_err(runtime_status_response)?;
    Ok(ScheduleResolution {
        schedule_type: normalized.schedule_type,
        schedule_payload_json: normalized.schedule_payload_json,
        next_run_at_unix_ms: normalized.next_run_at_unix_ms,
    })
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
        _ => Err(tonic::Status::invalid_argument(
            "schedule_type must be one of cron|every|at",
        )),
    }
}

fn build_schedule_trigger_payload(job: &CronJobRecord) -> String {
    json!({
        "schedule_type": job.schedule_type.as_str(),
        "schedule_payload": serde_json::from_str::<Value>(job.schedule_payload_json.as_str()).unwrap_or_else(|_| json!({ "raw": job.schedule_payload_json })),
    })
    .to_string()
}

fn parse_routine_trigger_kind(value: &str) -> Result<RoutineTriggerKind, tonic::Status> {
    RoutineTriggerKind::from_str(value).ok_or_else(|| {
        tonic::Status::invalid_argument(
            "trigger_kind must be one of schedule|hook|webhook|system_event|manual",
        )
    })
}

fn parse_schedule_type(value: &str) -> Result<CronScheduleType, Response> {
    CronScheduleType::from_str(value).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "schedule type must be one of cron|every|at",
        ))
    })
}

fn parse_concurrency_policy(value: Option<&str>) -> Result<CronConcurrencyPolicy, Response> {
    let normalized = value.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("forbid");
    CronConcurrencyPolicy::from_str(normalized).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "concurrency_policy must be one of forbid|replace|queue(1)",
        ))
    })
}

fn parse_retry_policy(
    max_attempts: Option<u32>,
    backoff_ms: Option<u64>,
) -> Result<CronRetryPolicy, Response> {
    Ok(CronRetryPolicy {
        max_attempts: max_attempts.unwrap_or(1).max(1),
        backoff_ms: backoff_ms.unwrap_or(1_000).max(1),
    })
}

fn parse_misfire_policy(value: Option<&str>) -> Result<CronMisfirePolicy, Response> {
    let normalized = value.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("skip");
    CronMisfirePolicy::from_str(normalized).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "misfire_policy must be one of skip|catch_up",
        ))
    })
}

fn parse_delivery(
    mode: Option<&str>,
    channel: Option<String>,
) -> Result<RoutineDeliveryConfig, Response> {
    let normalized_mode = mode.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("same_channel");
    let mode = RoutineDeliveryMode::from_str(normalized_mode).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "delivery_mode must be one of same_channel|specific_channel|local_only|logs_only",
        ))
    })?;
    let channel = channel.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) }
    });
    if matches!(mode, RoutineDeliveryMode::SpecificChannel) && channel.is_none() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "delivery_channel is required for delivery_mode=specific_channel",
        )));
    }
    Ok(RoutineDeliveryConfig { mode, channel })
}

fn parse_quiet_hours(
    start: Option<&str>,
    end: Option<&str>,
    timezone: Option<String>,
) -> Result<Option<RoutineQuietHours>, Response> {
    let Some(start) = start.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let end = end
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "quiet_hours_end is required when quiet_hours_start is provided",
            ))
        })?;
    let start_minute_of_day = parse_time_of_day_to_minutes(start)?;
    let end_minute_of_day = parse_time_of_day_to_minutes(end)?;
    let timezone = timezone.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() { None } else { Some(trimmed.to_ascii_lowercase()) }
    });
    Ok(Some(RoutineQuietHours {
        start_minute_of_day,
        end_minute_of_day,
        timezone,
    }))
}

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

fn parse_approval_policy(value: Option<&str>) -> Result<RoutineApprovalPolicy, Response> {
    let normalized = value.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("none");
    let mode = RoutineApprovalMode::from_str(normalized).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument(
            "approval_mode must be one of none|before_enable|before_first_run",
        ))
    })?;
    Ok(RoutineApprovalPolicy { mode })
}

fn normalize_owner_principal(
    requested: &Option<String>,
    session_principal: &str,
) -> Result<String, Response> {
    match requested.as_deref().map(str::trim) {
        Some("") | None => Ok(session_principal.to_owned()),
        Some(owner_principal) if owner_principal == session_principal => Ok(owner_principal.to_owned()),
        Some(_) => Err(runtime_status_response(tonic::Status::permission_denied(
            "owner_principal must match authenticated session principal",
        ))),
    }
}

fn normalize_channel(requested: Option<&str>, session_channel: Option<&str>) -> String {
    requested
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| session_channel.map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_ROUTINE_CHANNEL.to_owned())
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned)
}

fn ensure_job_owner(job: &CronJobRecord, principal: &str) -> Result<(), Response> {
    if job.owner_principal != principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "routine owner mismatch for authenticated principal",
        )));
    }
    Ok(())
}

fn parse_timezone_mode(value: Option<&str>) -> Result<CronTimezoneMode, Response> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some("local") | None => Ok(CronTimezoneMode::Local),
        Some("utc") => Ok(CronTimezoneMode::Utc),
        Some(_) => Err(runtime_status_response(tonic::Status::invalid_argument(
            "timezone must be one of local|utc",
        ))),
    }
}

fn is_in_quiet_hours(quiet_hours: Option<&RoutineQuietHours>, now_unix_ms: i64) -> Result<bool, Response> {
    let Some(quiet_hours) = quiet_hours else {
        return Ok(false);
    };
    let minute_of_day = match quiet_hours.timezone.as_deref().unwrap_or("local") {
        "utc" => {
            let value = chrono::Utc
                .timestamp_millis_opt(now_unix_ms)
                .single()
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::internal(
                        "failed to resolve UTC quiet-hour timestamp",
                    ))
                })?;
            value.hour() as u16 * 60 + value.minute() as u16
        }
        "local" => {
            let value = chrono::Local
                .timestamp_millis_opt(now_unix_ms)
                .single()
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::internal(
                        "failed to resolve local quiet-hour timestamp",
                    ))
                })?;
            value.hour() as u16 * 60 + value.minute() as u16
        }
        _ => {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "quiet_hours timezone must be one of local|utc",
            )))
        }
    };
    let start = quiet_hours.start_minute_of_day;
    let end = quiet_hours.end_minute_of_day;
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
    use super::{
        compare_optional_matchers, is_in_quiet_hours, parse_delivery, parse_quiet_hours,
        routine_matches_trigger,
    };
    use crate::routines::{
        RoutineApprovalPolicy, RoutineDeliveryMode, RoutineMetadataRecord, RoutineTriggerKind,
    };
    use chrono::{TimeZone, Utc};
    use serde_json::json;

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
    fn specific_channel_delivery_requires_explicit_channel() {
        let response =
            parse_delivery(Some("specific_channel"), None).expect_err("channel should be required");
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        let delivery = parse_delivery(
            Some("specific_channel"),
            Some("ops:routines".to_owned()),
        )
            .expect("delivery should parse");
        assert_eq!(delivery.mode, RoutineDeliveryMode::SpecificChannel);
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
        assert!(is_in_quiet_hours(Some(&quiet_hours), late_evening).expect("quiet hours should evaluate"));
        assert!(is_in_quiet_hours(Some(&quiet_hours), morning).expect("quiet hours should evaluate"));
        assert!(!is_in_quiet_hours(Some(&quiet_hours), noon).expect("quiet hours should evaluate"));
    }

    #[test]
    fn compare_optional_matchers_allows_unset_expectation() {
        assert!(compare_optional_matchers(None, Some("anything")));
        assert!(compare_optional_matchers(Some("push"), Some("push")));
        assert!(!compare_optional_matchers(Some("push"), Some("deploy")));
    }
}
