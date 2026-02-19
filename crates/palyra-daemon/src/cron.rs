#![allow(clippy::result_large_err)]

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use palyra_policy::{evaluate_with_config, PolicyDecision, PolicyEvaluationConfig, PolicyRequest};
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio_stream::StreamExt;
use tonic::{Request, Status};
use tracing::warn;
use ulid::Ulid;

use crate::{
    gateway::{
        proto::palyra::{common::v1 as common_v1, cron::v1 as cron_v1, gateway::v1 as gateway_v1},
        GatewayAuthConfig, GatewayRuntimeState, HEADER_CHANNEL, HEADER_DEVICE_ID, HEADER_PRINCIPAL,
    },
    journal::{
        CronConcurrencyPolicy, CronJobRecord, CronMisfirePolicy, CronRunFinalizeRequest,
        CronRunStartRequest, CronRunStatus, CronScheduleType, OrchestratorCancelRequest,
        OrchestratorRunStatusSnapshot,
    },
};

const SCHEDULER_IDLE_SLEEP: Duration = Duration::from_secs(15);
const SCHEDULER_MAX_DUE_BATCH: usize = 64;
const SCHEDULER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const DEFAULT_CRON_CHANNEL: &str = "system:cron";
const MAX_CRON_LOOKAHEAD_MINUTES: i64 = 60 * 24 * 370;
const MAX_RETRY_ATTEMPTS: u32 = 16;
const MAX_RETRY_BACKOFF_MS: u64 = 60_000;

#[derive(Debug, Clone)]
pub struct ScheduleNormalization {
    pub schedule_type: CronScheduleType,
    pub schedule_payload_json: String,
    pub next_run_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct DispatchOutcome {
    pub run_id: Option<String>,
    pub status: CronRunStatus,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedSchedule {
    Cron { expression: String, matcher: CronMatcher },
    Every { interval_ms: i64 },
    At { at_unix_ms: i64, timestamp_rfc3339: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CronMatcher {
    minutes: Vec<bool>,
    hours: Vec<bool>,
    day_of_month: Vec<bool>,
    months: Vec<bool>,
    weekdays: Vec<bool>,
}

impl CronMatcher {
    fn parse(expression: &str) -> Result<Self, String> {
        let parts = expression.split_whitespace().collect::<Vec<_>>();
        if parts.len() != 5 {
            return Err(
                "cron expression must have 5 fields (minute hour day month weekday)".to_owned()
            );
        }
        Ok(Self {
            minutes: parse_cron_field(parts[0], 0, 59, false)?,
            hours: parse_cron_field(parts[1], 0, 23, false)?,
            day_of_month: parse_cron_field(parts[2], 1, 31, false)?,
            months: parse_cron_field(parts[3], 1, 12, false)?,
            weekdays: parse_cron_field(parts[4], 0, 6, true)?,
        })
    }

    fn next_after(&self, after_unix_ms: i64) -> Option<i64> {
        let after_seconds = after_unix_ms.div_euclid(1_000);
        let mut cursor =
            Utc.timestamp_opt(after_seconds, 0).single()?.with_second(0)?.with_nanosecond(0)?
                + chrono::Duration::minutes(1);
        for _ in 0..MAX_CRON_LOOKAHEAD_MINUTES {
            if self.matches(cursor) {
                return Some(cursor.timestamp_millis());
            }
            cursor += chrono::Duration::minutes(1);
        }
        None
    }

    fn matches(&self, value: DateTime<Utc>) -> bool {
        let minute = value.minute() as usize;
        let hour = value.hour() as usize;
        let day = value.day() as usize;
        let month = value.month() as usize;
        let weekday = value.weekday().num_days_from_sunday() as usize;
        self.minutes[minute]
            && self.hours[hour]
            && self.day_of_month[day - 1]
            && self.months[month - 1]
            && self.weekdays[weekday]
    }
}

fn parse_cron_field(
    field: &str,
    min: i32,
    max: i32,
    normalize_weekday_seven: bool,
) -> Result<Vec<bool>, String> {
    let mut values = vec![false; (max - min + 1) as usize];
    for item in field.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err("cron field cannot contain empty list items".to_owned());
        }
        let (base, step) = if let Some((lhs, rhs)) = item.split_once('/') {
            let step =
                rhs.parse::<i32>().map_err(|_| format!("invalid cron step value '{rhs}'"))?;
            if step <= 0 {
                return Err("cron step value must be greater than zero".to_owned());
            }
            (lhs, step)
        } else {
            (item, 1)
        };

        let (start, end) = if base == "*" {
            (min, max)
        } else if let Some((lhs, rhs)) = base.split_once('-') {
            let lhs = parse_cron_value(lhs, min, max, normalize_weekday_seven)?;
            let rhs = parse_cron_value(rhs, min, max, normalize_weekday_seven)?;
            if lhs > rhs {
                return Err(format!("invalid cron range '{base}'"));
            }
            (lhs, rhs)
        } else {
            let single = parse_cron_value(base, min, max, normalize_weekday_seven)?;
            (single, single)
        };

        let mut value = start;
        while value <= end {
            values[(value - min) as usize] = true;
            value += step;
        }
    }

    if values.iter().all(|selected| !selected) {
        return Err("cron field produced no selectable values".to_owned());
    }
    Ok(values)
}

fn parse_cron_value(
    raw: &str,
    min: i32,
    max: i32,
    normalize_weekday_seven: bool,
) -> Result<i32, String> {
    let parsed = raw.parse::<i32>().map_err(|_| format!("invalid cron value '{raw}'"))?;
    if normalize_weekday_seven && parsed == 7 {
        return Ok(0);
    }
    if parsed < min || parsed > max {
        return Err(format!("cron value {parsed} out of range ({min}-{max})"));
    }
    Ok(parsed)
}

impl ParsedSchedule {
    fn next_after(&self, after_unix_ms: i64) -> Option<i64> {
        match self {
            Self::Cron { matcher, .. } => matcher.next_after(after_unix_ms),
            Self::Every { interval_ms } => Some(after_unix_ms.saturating_add(*interval_ms)),
            Self::At { at_unix_ms, .. } => {
                if *at_unix_ms > after_unix_ms {
                    Some(*at_unix_ms)
                } else {
                    None
                }
            }
        }
    }
}

pub fn normalize_schedule(
    schedule: Option<cron_v1::Schedule>,
    now_unix_ms: i64,
) -> Result<ScheduleNormalization, Status> {
    let schedule = schedule.ok_or_else(|| Status::invalid_argument("schedule is required"))?;
    let schedule_type = cron_v1::ScheduleType::try_from(schedule.r#type)
        .unwrap_or(cron_v1::ScheduleType::Unspecified);

    match schedule_type {
        cron_v1::ScheduleType::Cron => {
            let expression = match schedule.spec {
                Some(cron_v1::schedule::Spec::Cron(cron)) => cron.expression,
                _ => {
                    return Err(Status::invalid_argument("schedule.cron is required for type=CRON"))
                }
            };
            let expression = expression.trim();
            if expression.is_empty() {
                return Err(Status::invalid_argument("cron expression cannot be empty"));
            }
            let matcher = CronMatcher::parse(expression).map_err(Status::invalid_argument)?;
            let next_run_at_unix_ms = matcher.next_after(now_unix_ms);
            Ok(ScheduleNormalization {
                schedule_type: CronScheduleType::Cron,
                schedule_payload_json: json!({ "expression": expression }).to_string(),
                next_run_at_unix_ms,
            })
        }
        cron_v1::ScheduleType::Every => {
            let every = match schedule.spec {
                Some(cron_v1::schedule::Spec::Every(every)) => every,
                _ => {
                    return Err(Status::invalid_argument(
                        "schedule.every is required for type=EVERY",
                    ))
                }
            };
            let interval_ms = i64::try_from(every.interval_ms)
                .map_err(|_| Status::invalid_argument("every.interval_ms is too large"))?;
            if interval_ms <= 0 {
                return Err(Status::invalid_argument(
                    "every.interval_ms must be greater than zero",
                ));
            }
            Ok(ScheduleNormalization {
                schedule_type: CronScheduleType::Every,
                schedule_payload_json: json!({ "interval_ms": interval_ms }).to_string(),
                next_run_at_unix_ms: Some(now_unix_ms.saturating_add(interval_ms)),
            })
        }
        cron_v1::ScheduleType::At => {
            let at = match schedule.spec {
                Some(cron_v1::schedule::Spec::At(at)) => at,
                _ => return Err(Status::invalid_argument("schedule.at is required for type=AT")),
            };
            let timestamp_rfc3339 = at.timestamp_rfc3339.trim();
            if timestamp_rfc3339.is_empty() {
                return Err(Status::invalid_argument("at.timestamp_rfc3339 cannot be empty"));
            }
            let parsed = DateTime::parse_from_rfc3339(timestamp_rfc3339).map_err(|error| {
                Status::invalid_argument(format!("invalid at timestamp: {error}"))
            })?;
            let at_unix_ms = parsed.with_timezone(&Utc).timestamp_millis();
            if at_unix_ms <= now_unix_ms {
                return Err(Status::invalid_argument("at timestamp must be in the future"));
            }
            Ok(ScheduleNormalization {
                schedule_type: CronScheduleType::At,
                schedule_payload_json: json!({
                    "timestamp_rfc3339": parsed.to_rfc3339(),
                    "at_unix_ms": at_unix_ms
                })
                .to_string(),
                next_run_at_unix_ms: Some(at_unix_ms),
            })
        }
        cron_v1::ScheduleType::Unspecified => {
            Err(Status::invalid_argument("schedule.type must be specified"))
        }
    }
}

pub fn schedule_to_proto(
    schedule_type: CronScheduleType,
    schedule_payload_json: &str,
) -> Result<cron_v1::Schedule, Status> {
    let parsed = parse_schedule_payload(schedule_type, schedule_payload_json)?;
    match parsed {
        ParsedSchedule::Cron { expression, .. } => Ok(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule { expression })),
        }),
        ParsedSchedule::Every { interval_ms } => Ok(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: interval_ms as u64,
            })),
        }),
        ParsedSchedule::At { timestamp_rfc3339, .. } => Ok(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::At as i32,
            spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule { timestamp_rfc3339 })),
        }),
    }
}

pub fn compute_next_run_after(
    job: &CronJobRecord,
    reference_unix_ms: i64,
    now_unix_ms: i64,
) -> Result<Option<i64>, Status> {
    let parsed = parse_schedule_payload(job.schedule_type, job.schedule_payload_json.as_str())?;
    let mut next = parsed.next_after(reference_unix_ms);
    if job.misfire_policy == CronMisfirePolicy::Skip {
        while let Some(next_value) = next {
            if next_value > now_unix_ms {
                break;
            }
            next = parsed.next_after(next_value);
        }
    }
    if let Some(next_value) = next {
        if job.jitter_ms > 0 {
            let jitter = deterministic_jitter_ms(job.job_id.as_str(), next_value, job.jitter_ms);
            return Ok(Some(next_value.saturating_add(jitter as i64)));
        }
        return Ok(Some(next_value));
    }
    Ok(None)
}

fn parse_schedule_payload(
    schedule_type: CronScheduleType,
    schedule_payload_json: &str,
) -> Result<ParsedSchedule, Status> {
    let payload: Value = serde_json::from_str(schedule_payload_json)
        .map_err(|error| Status::internal(format!("invalid schedule payload json: {error}")))?;
    match schedule_type {
        CronScheduleType::Cron => {
            let expression = payload
                .get("expression")
                .and_then(Value::as_str)
                .ok_or_else(|| Status::internal("cron schedule payload missing expression"))?
                .trim()
                .to_owned();
            let matcher = CronMatcher::parse(expression.as_str())
                .map_err(|error| Status::internal(format!("invalid cron expression: {error}")))?;
            Ok(ParsedSchedule::Cron { expression, matcher })
        }
        CronScheduleType::Every => {
            let interval_ms = payload
                .get("interval_ms")
                .and_then(Value::as_i64)
                .ok_or_else(|| Status::internal("every schedule payload missing interval_ms"))?;
            if interval_ms <= 0 {
                return Err(Status::internal("every schedule interval must be positive"));
            }
            Ok(ParsedSchedule::Every { interval_ms })
        }
        CronScheduleType::At => {
            let at_unix_ms = payload
                .get("at_unix_ms")
                .and_then(Value::as_i64)
                .ok_or_else(|| Status::internal("at schedule payload missing at_unix_ms"))?;
            let timestamp_rfc3339 = payload
                .get("timestamp_rfc3339")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            Ok(ParsedSchedule::At { at_unix_ms, timestamp_rfc3339 })
        }
    }
}

fn deterministic_jitter_ms(job_id: &str, seed: i64, max_jitter_ms: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    job_id.hash(&mut hasher);
    seed.hash(&mut hasher);
    hasher.finish() % (max_jitter_ms.saturating_add(1))
}

pub fn spawn_scheduler_loop(
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    wake_signal: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(error) = process_due_jobs(
                Arc::clone(&state),
                auth.clone(),
                grpc_url.clone(),
                Arc::clone(&wake_signal),
            )
            .await
            {
                warn!(error = %error, "cron scheduler failed to process due jobs");
            }

            if let Err(error) = process_queued_jobs(
                Arc::clone(&state),
                auth.clone(),
                grpc_url.clone(),
                Arc::clone(&wake_signal),
            )
            .await
            {
                warn!(error = %error, "cron scheduler failed to process queued jobs");
            }

            let sleep_duration = match state.first_due_cron_job_time().await {
                Ok(Some(next_due_ms)) => {
                    let now = now_unix_ms().unwrap_or(next_due_ms);
                    if next_due_ms <= now {
                        Duration::from_millis(10)
                    } else {
                        Duration::from_millis((next_due_ms - now) as u64)
                    }
                }
                Ok(None) => SCHEDULER_IDLE_SLEEP,
                Err(error) => {
                    warn!(error = %error, "cron scheduler failed to compute next wake time");
                    Duration::from_secs(1)
                }
            };

            tokio::select! {
                _ = tokio::time::sleep(sleep_duration) => {}
                _ = wake_signal.notified() => {}
            }
        }
    })
}

pub async fn trigger_job_now(
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    job: CronJobRecord,
    wake_signal: Arc<Notify>,
) -> Result<DispatchOutcome, Status> {
    dispatch_job(state, auth, grpc_url, job, wake_signal, true).await
}

async fn process_due_jobs(
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    wake_signal: Arc<Notify>,
) -> Result<(), Status> {
    let now_unix_ms = now_unix_ms()?;
    let jobs = state.list_due_cron_jobs(now_unix_ms, SCHEDULER_MAX_DUE_BATCH).await?;
    for job in jobs {
        let reference_unix_ms = job.next_run_at_unix_ms.unwrap_or(now_unix_ms);
        let next_run_at_unix_ms = compute_next_run_after(&job, reference_unix_ms, now_unix_ms)?;
        state
            .set_cron_job_next_run(job.job_id.clone(), next_run_at_unix_ms, Some(reference_unix_ms))
            .await?;
        state.record_cron_trigger_fired();
        let _outcome = dispatch_job(
            Arc::clone(&state),
            auth.clone(),
            grpc_url.clone(),
            job,
            Arc::clone(&wake_signal),
            false,
        )
        .await?;
        if next_run_at_unix_ms.is_some_and(|value| value <= now_unix_ms) {
            wake_signal.notify_one();
        }
    }
    Ok(())
}

async fn process_queued_jobs(
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    wake_signal: Arc<Notify>,
) -> Result<(), Status> {
    let mut after_job_id = None::<String>;
    loop {
        let (jobs, next_after_job_id) =
            state.list_cron_jobs(after_job_id.clone(), Some(500), Some(true), None, None).await?;
        if jobs.is_empty() {
            break;
        }

        for job in jobs {
            if !job.queued_run {
                continue;
            }
            if state.active_cron_run_for_job(job.job_id.clone()).await?.is_some() {
                continue;
            }
            match dispatch_job(
                Arc::clone(&state),
                auth.clone(),
                grpc_url.clone(),
                job.clone(),
                Arc::clone(&wake_signal),
                false,
            )
            .await
            {
                Ok(outcome) => {
                    if outcome.status == CronRunStatus::Accepted && outcome.run_id.is_none() {
                        continue;
                    }
                    state.set_cron_job_queue_state(job.job_id.clone(), false).await?;
                }
                Err(error) => {
                    warn!(
                        job_id = %job.job_id,
                        error = %error,
                        "failed to dispatch queued cron run; keeping queued marker for retry"
                    );
                }
            }
        }

        let Some(next_after_job_id) = next_after_job_id else {
            break;
        };
        after_job_id = Some(next_after_job_id);
    }
    Ok(())
}

async fn dispatch_job(
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    job: CronJobRecord,
    wake_signal: Arc<Notify>,
    manual_trigger: bool,
) -> Result<DispatchOutcome, Status> {
    let policy = evaluate_with_config(
        &PolicyRequest {
            principal: job.owner_principal.clone(),
            action: "cron.run".to_owned(),
            resource: format!("cron:{}", job.job_id),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| Status::internal(format!("failed to evaluate cron run policy: {error}")))?;
    if let PolicyDecision::DenyByDefault { reason } = policy.decision {
        return register_terminal(
            Arc::clone(&state),
            &job.job_id,
            CronRunStatus::Denied,
            "policy_denied",
            reason.as_str(),
        )
        .await;
    }

    let active_run = state.active_cron_run_for_job(job.job_id.clone()).await?;
    if let Some(active) = active_run {
        match job.concurrency_policy {
            CronConcurrencyPolicy::Forbid => {
                return register_terminal(
                    Arc::clone(&state),
                    &job.job_id,
                    CronRunStatus::Skipped,
                    "concurrency_forbid",
                    "concurrency policy forbids overlapping runs",
                )
                .await;
            }
            CronConcurrencyPolicy::QueueOne => {
                if job.queued_run {
                    if !manual_trigger {
                        return Ok(DispatchOutcome {
                            run_id: None,
                            status: CronRunStatus::Accepted,
                            message: "run remains queued until active execution completes"
                                .to_owned(),
                        });
                    }
                    return register_terminal(
                        Arc::clone(&state),
                        &job.job_id,
                        CronRunStatus::Skipped,
                        "concurrency_queue_full",
                        "queue(1) already has one pending run",
                    )
                    .await;
                }
                state.set_cron_job_queue_state(job.job_id.clone(), true).await?;
                return Ok(DispatchOutcome {
                    run_id: None,
                    status: CronRunStatus::Accepted,
                    message: "run queued due to active execution".to_owned(),
                });
            }
            CronConcurrencyPolicy::Replace => {
                if let Some(orchestrator_run_id) = active.orchestrator_run_id {
                    let _ = state
                        .request_orchestrator_cancel(OrchestratorCancelRequest {
                            run_id: orchestrator_run_id,
                            reason: "cron replace policy preemption".to_owned(),
                        })
                        .await;
                }
            }
        }
    }

    let run_id = Ulid::new().to_string();
    state
        .start_cron_run(CronRunStartRequest {
            run_id: run_id.clone(),
            job_id: job.job_id.clone(),
            attempt: 1,
            session_id: None,
            orchestrator_run_id: None,
            status: CronRunStatus::Accepted,
            error_kind: None,
            error_message_redacted: None,
        })
        .await?;

    let dispatch_run_id = run_id.clone();
    tokio::spawn(async move {
        if let Err(error) = run_job_with_retries(
            Arc::clone(&state),
            auth,
            grpc_url,
            job,
            run_id,
            Arc::clone(&wake_signal),
        )
        .await
        {
            warn!(error = %error, "cron execution task failed");
        }
    });

    Ok(DispatchOutcome {
        run_id: Some(dispatch_run_id),
        status: CronRunStatus::Running,
        message: if manual_trigger {
            "manual run dispatched".to_owned()
        } else {
            "scheduled run dispatched".to_owned()
        },
    })
}

async fn register_terminal(
    state: Arc<GatewayRuntimeState>,
    job_id: &str,
    status: CronRunStatus,
    error_kind: &str,
    message: &str,
) -> Result<DispatchOutcome, Status> {
    let run_id = Ulid::new().to_string();
    state
        .start_cron_run(CronRunStartRequest {
            run_id: run_id.clone(),
            job_id: job_id.to_owned(),
            attempt: 1,
            session_id: None,
            orchestrator_run_id: None,
            status,
            error_kind: Some(error_kind.to_owned()),
            error_message_redacted: Some(message.to_owned()),
        })
        .await?;
    state
        .finalize_cron_run(CronRunFinalizeRequest {
            run_id: run_id.clone(),
            status,
            error_kind: Some(error_kind.to_owned()),
            error_message_redacted: Some(message.to_owned()),
            model_tokens_in: 0,
            model_tokens_out: 0,
            tool_calls: 0,
            tool_denies: 0,
            orchestrator_run_id: None,
            session_id: None,
        })
        .await?;
    Ok(DispatchOutcome { run_id: Some(run_id), status, message: message.to_owned() })
}

async fn run_job_with_retries(
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    job: CronJobRecord,
    first_run_id: String,
    wake_signal: Arc<Notify>,
) -> Result<(), Status> {
    let max_attempts = job.retry_policy.max_attempts.clamp(1, MAX_RETRY_ATTEMPTS);
    let base_backoff_ms = job.retry_policy.backoff_ms.min(MAX_RETRY_BACKOFF_MS);

    let mut run_id = first_run_id;
    for attempt in 1..=max_attempts {
        if attempt > 1 {
            run_id = Ulid::new().to_string();
            state
                .start_cron_run(CronRunStartRequest {
                    run_id: run_id.clone(),
                    job_id: job.job_id.clone(),
                    attempt,
                    session_id: None,
                    orchestrator_run_id: None,
                    status: CronRunStatus::Accepted,
                    error_kind: None,
                    error_message_redacted: None,
                })
                .await?;
        }

        let result = execute_single_job_attempt(
            Arc::clone(&state),
            auth.clone(),
            grpc_url.clone(),
            &job,
            run_id.clone(),
            attempt,
        )
        .await;

        match result {
            Ok(CronRunStatus::Succeeded) => {
                wake_signal.notify_one();
                return Ok(());
            }
            Ok(terminal_status) => {
                if attempt >= max_attempts || terminal_status == CronRunStatus::Denied {
                    wake_signal.notify_one();
                    return Ok(());
                }
            }
            Err(error) => {
                warn!(
                    job_id = %job.job_id,
                    run_id = %run_id,
                    attempt,
                    error = %error,
                    "cron attempt failed before completion"
                );
                state
                    .finalize_cron_run(CronRunFinalizeRequest {
                        run_id: run_id.clone(),
                        status: CronRunStatus::Failed,
                        error_kind: Some("scheduler_internal".to_owned()),
                        error_message_redacted: Some(format!("cron attempt {attempt} failed")),
                        model_tokens_in: 0,
                        model_tokens_out: 0,
                        tool_calls: 0,
                        tool_denies: 0,
                        orchestrator_run_id: None,
                        session_id: None,
                    })
                    .await?;
                if attempt >= max_attempts {
                    wake_signal.notify_one();
                    return Ok(());
                }
            }
        }

        let backoff_multiplier = 1_u64 << u64::from(attempt.saturating_sub(1));
        let delay = base_backoff_ms.saturating_mul(backoff_multiplier);
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    Ok(())
}

async fn execute_single_job_attempt(
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    job: &CronJobRecord,
    run_id: String,
    attempt: u32,
) -> Result<CronRunStatus, Status> {
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(grpc_url)
        .await
        .map_err(|error| Status::unavailable(format!("failed to connect gateway: {error}")))?;

    let session_key = job.session_key.clone().unwrap_or_else(|| format!("cron:{}", job.job_id));
    let mut resolve_request = Request::new(gateway_v1::ResolveSessionRequest {
        v: 1,
        session_id: None,
        session_key,
        session_label: job.session_label.clone().unwrap_or_else(|| job.name.clone()),
        require_existing: false,
        reset_session: false,
    });
    inject_scheduler_metadata(
        resolve_request.metadata_mut(),
        &auth,
        job.owner_principal.as_str(),
        job.channel.as_str(),
    )?;
    let resolved = client
        .resolve_session(resolve_request)
        .await
        .map_err(|error| Status::internal(format!("ResolveSession failed: {error}")))?
        .into_inner();
    let session = resolved
        .session
        .ok_or_else(|| Status::internal("ResolveSession returned empty session"))?;
    let session_id = session
        .session_id
        .map(|value| value.ulid)
        .ok_or_else(|| Status::internal("ResolveSession returned session without session_id"))?;
    let orchestrator_run_id = Ulid::new().to_string();

    state
        .finalize_cron_run(CronRunFinalizeRequest {
            run_id: run_id.clone(),
            status: CronRunStatus::Running,
            error_kind: None,
            error_message_redacted: None,
            model_tokens_in: 0,
            model_tokens_out: 0,
            tool_calls: 0,
            tool_denies: 0,
            orchestrator_run_id: Some(orchestrator_run_id.clone()),
            session_id: Some(session_id.clone()),
        })
        .await?;

    let mut append_request = Request::new(gateway_v1::AppendEventRequest {
        v: 1,
        event: Some(common_v1::JournalEvent {
            v: 1,
            event_id: Some(common_v1::CanonicalId { ulid: Ulid::new().to_string() }),
            session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
            run_id: Some(common_v1::CanonicalId { ulid: orchestrator_run_id.clone() }),
            kind: common_v1::journal_event::EventKind::MessageReceived as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: now_unix_ms()?,
            payload_json: json!({
                "origin": "cron",
                "job_id": job.job_id,
                "job_name": job.name,
                "attempt": attempt,
            })
            .to_string()
            .into_bytes(),
            hash: String::new(),
            prev_hash: String::new(),
        }),
    });
    inject_scheduler_metadata(
        append_request.metadata_mut(),
        &auth,
        job.owner_principal.as_str(),
        job.channel.as_str(),
    )?;
    client
        .append_event(append_request)
        .await
        .map_err(|error| Status::internal(format!("AppendEvent failed: {error}")))?;

    let prompt = format!("[cron job {}] {}", job.name, job.prompt);
    let mut stream_request = Request::new(tokio_stream::iter(vec![common_v1::RunStreamRequest {
        v: 1,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: orchestrator_run_id.clone() }),
        input: Some(common_v1::MessageEnvelope {
            v: 1,
            envelope_id: Some(common_v1::CanonicalId { ulid: Ulid::new().to_string() }),
            timestamp_unix_ms: now_unix_ms()?,
            origin: Some(common_v1::EnvelopeOrigin {
                r#type: common_v1::envelope_origin::OriginType::System as i32,
                channel: job.channel.clone(),
                conversation_id: job.job_id.clone(),
                sender_display: "palyra-cron".to_owned(),
                sender_handle: "cron".to_owned(),
                sender_verified: true,
            }),
            content: Some(common_v1::MessageContent { text: prompt, attachments: Vec::new() }),
            security: None,
            max_payload_bytes: 0,
        }),
        allow_sensitive_tools: false,
        session_key: String::new(),
        session_label: String::new(),
        reset_session: false,
        require_existing: false,
        tool_approval_response: None,
    }]));
    inject_scheduler_metadata(
        stream_request.metadata_mut(),
        &auth,
        job.owner_principal.as_str(),
        job.channel.as_str(),
    )?;

    let mut stream = client
        .run_stream(stream_request)
        .await
        .map_err(|error| Status::internal(format!("RunStream failed: {error}")))?
        .into_inner();

    let mut saw_done = false;
    let mut saw_failed = false;
    let mut tool_calls = 0_u64;
    let mut tool_denies = 0_u64;
    while let Some(event) = stream.next().await {
        let event =
            event.map_err(|error| Status::internal(format!("run stream read failed: {error}")))?;
        match event.body {
            Some(common_v1::run_stream_event::Body::ToolResult(_)) => {
                tool_calls = tool_calls.saturating_add(1);
            }
            Some(common_v1::run_stream_event::Body::ToolDecision(decision))
                if decision.kind == common_v1::tool_decision::DecisionKind::Deny as i32 =>
            {
                tool_denies = tool_denies.saturating_add(1);
            }
            Some(common_v1::run_stream_event::Body::Status(status))
                if status.kind == common_v1::stream_status::StatusKind::Done as i32 =>
            {
                saw_done = true;
            }
            Some(common_v1::run_stream_event::Body::Status(status))
                if status.kind == common_v1::stream_status::StatusKind::Failed as i32 =>
            {
                saw_failed = true;
            }
            _ => {}
        }
    }

    let usage = state
        .orchestrator_run_status_snapshot(orchestrator_run_id.clone())
        .await?
        .unwrap_or_else(|| fallback_usage_snapshot(&orchestrator_run_id, &session_id, job));

    let terminal_status = if saw_done {
        CronRunStatus::Succeeded
    } else if saw_failed && tool_denies > 0 {
        CronRunStatus::Denied
    } else {
        CronRunStatus::Failed
    };

    let error_kind = if terminal_status == CronRunStatus::Succeeded {
        None
    } else if terminal_status == CronRunStatus::Denied {
        Some("policy_denied".to_owned())
    } else {
        Some("run_failed".to_owned())
    };
    let error_message = if terminal_status == CronRunStatus::Succeeded {
        None
    } else {
        Some(format!("cron attempt {attempt} failed"))
    };

    state
        .finalize_cron_run(CronRunFinalizeRequest {
            run_id,
            status: terminal_status,
            error_kind,
            error_message_redacted: error_message,
            model_tokens_in: usage.prompt_tokens,
            model_tokens_out: usage.completion_tokens,
            tool_calls,
            tool_denies,
            orchestrator_run_id: Some(orchestrator_run_id),
            session_id: Some(session_id),
        })
        .await?;

    Ok(terminal_status)
}

fn fallback_usage_snapshot(
    run_id: &str,
    session_id: &str,
    job: &CronJobRecord,
) -> OrchestratorRunStatusSnapshot {
    let now = now_unix_ms().unwrap_or_default();
    OrchestratorRunStatusSnapshot {
        run_id: run_id.to_owned(),
        session_id: session_id.to_owned(),
        state: "unknown".to_owned(),
        cancel_requested: false,
        cancel_reason: None,
        principal: job.owner_principal.clone(),
        device_id: SCHEDULER_DEVICE_ID.to_owned(),
        channel: Some(job.channel.clone()),
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
        created_at_unix_ms: now,
        started_at_unix_ms: now,
        completed_at_unix_ms: None,
        updated_at_unix_ms: now,
        last_error: None,
        tape_events: 0,
    }
}

fn inject_scheduler_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    auth: &GatewayAuthConfig,
    principal: &str,
    channel: &str,
) -> Result<(), Status> {
    if auth.require_auth {
        let token = auth.admin_token.as_ref().ok_or_else(|| {
            Status::permission_denied("admin token is required for scheduler auth")
        })?;
        metadata.insert(
            "authorization",
            format!("Bearer {token}").parse().map_err(|_| {
                Status::internal("failed to encode scheduler authorization metadata")
            })?,
        );
    }
    metadata.insert(
        HEADER_PRINCIPAL,
        principal
            .parse()
            .map_err(|_| Status::invalid_argument("scheduler principal metadata is invalid"))?,
    );
    metadata.insert(
        HEADER_DEVICE_ID,
        SCHEDULER_DEVICE_ID
            .parse()
            .map_err(|_| Status::internal("scheduler device_id metadata is invalid"))?,
    );
    let header_channel = if channel.trim().is_empty() { DEFAULT_CRON_CHANNEL } else { channel };
    metadata.insert(
        HEADER_CHANNEL,
        header_channel
            .parse()
            .map_err(|_| Status::invalid_argument("scheduler channel metadata is invalid"))?,
    );
    Ok(())
}

fn now_unix_ms() -> Result<i64, Status> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Status::internal(format!("system time before unix epoch: {error}")))?;
    Ok(elapsed.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::{compute_next_run_after, normalize_schedule, CronMatcher};
    use crate::gateway::proto::palyra::cron::v1 as cron_v1;
    use crate::journal::{
        CronConcurrencyPolicy, CronJobRecord, CronMisfirePolicy, CronRetryPolicy, CronScheduleType,
    };
    use serde_json::json;

    #[test]
    fn cron_matcher_accepts_step_and_list_fields() {
        let matcher = CronMatcher::parse("*/15 9-17/2 * * 1,3,5").expect("cron should parse");
        let now = 1_730_000_000_000_i64;
        let next = matcher.next_after(now).expect("next fire should exist");
        assert!(next > now, "next fire should be in the future");
    }

    #[test]
    fn normalize_schedule_rejects_invalid_cron_expression() {
        let schedule = cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule {
                expression: "*/0 * * * *".to_owned(),
            })),
        };
        let error = normalize_schedule(Some(schedule), 1_730_000_000_000)
            .expect_err("invalid expression must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn normalize_schedule_rejects_past_at_timestamp() {
        let schedule = cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::At as i32,
            spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                timestamp_rfc3339: "2020-01-01T00:00:00Z".to_owned(),
            })),
        };
        let error = normalize_schedule(Some(schedule), 1_730_000_000_000)
            .expect_err("past at schedule must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn compute_next_run_after_respects_misfire_policy() {
        let catch_up_job = CronJobRecord {
            job_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            name: "misfire-check".to_owned(),
            prompt: "test".to_owned(),
            owner_principal: "user:ops".to_owned(),
            channel: "system:cron".to_owned(),
            session_key: None,
            session_label: None,
            schedule_type: CronScheduleType::Every,
            schedule_payload_json: json!({ "interval_ms": 1_000_i64 }).to_string(),
            enabled: true,
            concurrency_policy: CronConcurrencyPolicy::Forbid,
            retry_policy: CronRetryPolicy { max_attempts: 1, backoff_ms: 1 },
            misfire_policy: CronMisfirePolicy::CatchUp,
            jitter_ms: 0,
            next_run_at_unix_ms: None,
            last_run_at_unix_ms: None,
            queued_run: false,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        let catch_up_next = compute_next_run_after(&catch_up_job, 1_000, 3_500)
            .expect("catch-up policy should compute next run")
            .expect("every schedule should return next run");
        assert_eq!(
            catch_up_next, 2_000,
            "catch-up policy should keep the first missed slot for immediate processing"
        );

        let mut skip_job = catch_up_job.clone();
        skip_job.misfire_policy = CronMisfirePolicy::Skip;
        let skip_next = compute_next_run_after(&skip_job, 1_000, 3_500)
            .expect("skip policy should compute next run")
            .expect("every schedule should return next run");
        assert_eq!(skip_next, 4_000, "skip policy should advance past missed slots");
    }
}
