use std::io::Write;

use serde_json::{json, Map, Value};

use crate::cli::{CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg};
use crate::*;

use super::routines::{
    delete_routine_value, get_routine_value, json_bool_at, json_i64_at, json_optional_string_at,
    json_value_at, list_routine_runs_value, list_routines_value, run_routine_now_value,
    set_routine_enabled_value, upsert_routine_value,
};

pub(crate) fn run_cron(command: CronCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_cron_async(command))
}

pub(crate) async fn run_cron_async(command: CronCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        CronCommand::Status { after, limit, enabled, owner, channel, json } => {
            let payload = schedule_routines_payload(
                &context.client,
                after.as_deref(),
                limit,
                enabled,
                channel.as_deref(),
                owner.as_deref(),
            )
            .await?;
            emit_cron_status(&payload, output::preferred_json(json))
        }
        CronCommand::List { after, limit, enabled, owner, channel, json } => {
            let payload = schedule_routines_payload(
                &context.client,
                after.as_deref(),
                limit,
                enabled,
                channel.as_deref(),
                owner.as_deref(),
            )
            .await?;
            emit_cron_list(&payload, output::preferred_json(json))
        }
        CronCommand::Show { id, json } => {
            let payload = get_routine_value(&context.client, id.as_str()).await?;
            emit_cron_show(&payload, output::preferred_json(json))
        }
        CronCommand::Add {
            name,
            prompt,
            schedule_type,
            schedule,
            enabled,
            concurrency,
            retry_max_attempts,
            retry_backoff_ms,
            misfire,
            jitter_ms,
            owner,
            channel,
            session_key,
            session_label,
            json,
        } => {
            let payload = build_schedule_routine_payload(
                None,
                ScheduleRoutineConfig {
                    name,
                    prompt,
                    schedule_type,
                    schedule,
                    enabled: Some(enabled),
                    concurrency,
                    retry_max_attempts,
                    retry_backoff_ms,
                    misfire,
                    jitter_ms,
                    owner,
                    channel,
                    session_key,
                    session_label,
                },
            )?;
            let response = upsert_routine_value(&context.client, &payload).await?;
            emit_cron_mutation("cron.add", &response, output::preferred_json(json))
        }
        CronCommand::Update {
            id,
            name,
            prompt,
            schedule_type,
            schedule,
            enabled,
            concurrency,
            retry_max_attempts,
            retry_backoff_ms,
            misfire,
            jitter_ms,
            owner,
            channel,
            session_key,
            session_label,
            json,
        } => {
            let existing = get_routine_value(&context.client, id.as_str()).await?;
            let routine = existing
                .pointer("/routine")
                .ok_or_else(|| anyhow!("routine response is missing the routine payload"))?;
            let payload = build_schedule_routine_payload(
                Some(routine),
                ScheduleRoutineConfig {
                    name: name.unwrap_or_else(|| {
                        json_optional_string_at(routine, "/name").unwrap_or_default()
                    }),
                    prompt: prompt.unwrap_or_else(|| {
                        json_optional_string_at(routine, "/prompt").unwrap_or_default()
                    }),
                    schedule_type: schedule_type.unwrap_or_else(|| existing_schedule_type(routine)),
                    schedule: schedule.unwrap_or_else(|| existing_schedule_value(routine)),
                    enabled: Some(
                        enabled
                            .unwrap_or_else(|| json_bool_at(routine, "/enabled").unwrap_or(true)),
                    ),
                    concurrency: concurrency
                        .unwrap_or_else(|| existing_concurrency_policy(routine)),
                    retry_max_attempts: retry_max_attempts.unwrap_or_else(|| {
                        json_i64_at(routine, "/retry_policy/max_attempts").unwrap_or(1).max(1)
                            as u32
                    }),
                    retry_backoff_ms: retry_backoff_ms.unwrap_or_else(|| {
                        json_i64_at(routine, "/retry_policy/backoff_ms").unwrap_or(1000).max(1)
                            as u64
                    }),
                    misfire: misfire.unwrap_or_else(|| existing_misfire_policy(routine)),
                    jitter_ms: jitter_ms.unwrap_or_else(|| {
                        json_i64_at(routine, "/jitter_ms").unwrap_or_default().max(0) as u64
                    }),
                    owner: Some(owner.unwrap_or_else(|| {
                        json_optional_string_at(routine, "/owner_principal").unwrap_or_default()
                    })),
                    channel: Some(channel.unwrap_or_else(|| {
                        json_optional_string_at(routine, "/channel").unwrap_or_default()
                    })),
                    session_key: Some(session_key.unwrap_or_else(|| {
                        json_optional_string_at(routine, "/session_key").unwrap_or_default()
                    })),
                    session_label: Some(session_label.unwrap_or_else(|| {
                        json_optional_string_at(routine, "/session_label").unwrap_or_default()
                    })),
                },
            )?;
            let response = upsert_routine_value(&context.client, &payload).await?;
            emit_cron_mutation("cron.update", &response, output::preferred_json(json))
        }
        CronCommand::Enable { id, json } => {
            let payload = set_routine_enabled_value(&context.client, id.as_str(), true).await?;
            emit_cron_mutation("cron.enable", &payload, output::preferred_json(json))
        }
        CronCommand::Disable { id, json } => {
            let payload = set_routine_enabled_value(&context.client, id.as_str(), false).await?;
            emit_cron_mutation("cron.disable", &payload, output::preferred_json(json))
        }
        CronCommand::RunNow { id, json } => {
            let payload = run_routine_now_value(&context.client, id.as_str()).await?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode cron run-now output as JSON")
            } else {
                println!(
                    "cron.run_now id={} run_id={} status={} message={}",
                    id,
                    json_optional_string_at(&payload, "/run_id").unwrap_or_default(),
                    json_optional_string_at(&payload, "/status")
                        .unwrap_or_else(|| "unknown".to_owned()),
                    json_optional_string_at(&payload, "/message").unwrap_or_default(),
                );
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        CronCommand::Delete { id, json } => {
            let payload = delete_routine_value(&context.client, id.as_str()).await?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode cron delete output as JSON")
            } else {
                println!(
                    "cron.delete id={} deleted={}",
                    id,
                    json_bool_at(&payload, "/deleted").unwrap_or(false)
                );
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        CronCommand::Logs { id, after, limit, json } => {
            let payload =
                list_routine_runs_value(&context.client, id.as_str(), after.as_deref(), limit)
                    .await?;
            emit_cron_runs(id.as_str(), &payload, output::preferred_json(json))
        }
    }
}

async fn schedule_routines_payload(
    client: &palyra_control_plane::ControlPlaneClient,
    after: Option<&str>,
    limit: Option<u32>,
    enabled: Option<bool>,
    channel: Option<&str>,
    owner: Option<&str>,
) -> Result<Value> {
    let mut payload =
        list_routines_value(client, after, limit, Some("schedule"), enabled, channel, None).await?;
    if let Some(owner) = owner.map(str::trim).filter(|value| !value.is_empty()) {
        if let Some(routines) = payload.get_mut("routines").and_then(Value::as_array_mut) {
            routines.retain(|routine| {
                json_optional_string_at(routine, "/owner_principal")
                    .is_some_and(|candidate| candidate == owner)
            });
        }
    }
    Ok(payload)
}

fn build_schedule_routine_payload(
    existing: Option<&Value>,
    config: ScheduleRoutineConfig,
) -> Result<Map<String, Value>> {
    if config.name.trim().is_empty() {
        anyhow::bail!("cron routine name cannot be empty");
    }
    if config.prompt.trim().is_empty() {
        anyhow::bail!("cron routine prompt cannot be empty");
    }
    let mut payload = Map::new();
    if let Some(existing) = existing {
        insert_optional_string(
            &mut payload,
            "routine_id",
            json_optional_string_at(existing, "/routine_id"),
        );
    }
    payload.insert("name".to_owned(), Value::String(config.name));
    payload.insert("prompt".to_owned(), Value::String(config.prompt));
    payload.insert("trigger_kind".to_owned(), Value::String("schedule".to_owned()));
    insert_optional_string(&mut payload, "owner_principal", config.owner);
    insert_optional_string(&mut payload, "channel", config.channel);
    insert_optional_string(&mut payload, "session_key", config.session_key);
    insert_optional_string(&mut payload, "session_label", config.session_label);
    if let Some(enabled) = config.enabled {
        payload.insert("enabled".to_owned(), Value::Bool(enabled));
    }
    payload.insert(
        "schedule_type".to_owned(),
        Value::String(cron_schedule_type_text(config.schedule_type).to_owned()),
    );
    insert_schedule_value(&mut payload, config.schedule_type, config.schedule)?;
    payload.insert(
        "concurrency_policy".to_owned(),
        Value::String(cron_concurrency_policy_text(config.concurrency).to_owned()),
    );
    payload.insert("retry_max_attempts".to_owned(), Value::from(config.retry_max_attempts.max(1)));
    payload.insert("retry_backoff_ms".to_owned(), Value::from(config.retry_backoff_ms.max(1)));
    payload.insert(
        "misfire_policy".to_owned(),
        Value::String(cron_misfire_policy_text(config.misfire).to_owned()),
    );
    payload.insert("jitter_ms".to_owned(), Value::from(config.jitter_ms));

    if let Some(existing) = existing {
        preserve_existing_routine_fields(existing, &mut payload);
    }

    Ok(payload)
}

fn preserve_existing_routine_fields(existing: &Value, payload: &mut Map<String, Value>) {
    payload.insert(
        "delivery_mode".to_owned(),
        Value::String(
            json_optional_string_at(existing, "/delivery_mode")
                .unwrap_or_else(|| "same_channel".to_owned()),
        ),
    );
    insert_optional_string(
        payload,
        "delivery_channel",
        json_optional_string_at(existing, "/delivery_channel"),
    );
    insert_optional_string(
        payload,
        "quiet_hours_start",
        json_i64_at(existing, "/quiet_hours/start_minute_of_day").map(minute_of_day_to_clock),
    );
    insert_optional_string(
        payload,
        "quiet_hours_end",
        json_i64_at(existing, "/quiet_hours/end_minute_of_day").map(minute_of_day_to_clock),
    );
    insert_optional_string(
        payload,
        "quiet_hours_timezone",
        json_optional_string_at(existing, "/quiet_hours/timezone"),
    );
    payload.insert(
        "cooldown_ms".to_owned(),
        Value::from(json_i64_at(existing, "/cooldown_ms").unwrap_or_default().max(0) as u64),
    );
    payload.insert(
        "approval_mode".to_owned(),
        Value::String(
            json_optional_string_at(existing, "/approval_mode")
                .unwrap_or_else(|| "none".to_owned()),
        ),
    );
    insert_optional_string(
        payload,
        "template_id",
        json_optional_string_at(existing, "/template_id"),
    );
}

fn emit_cron_status(payload: &Value, json: bool) -> Result<()> {
    let routines = schedule_routine_array(payload);
    let now_unix_ms = unix_now_ms();
    let mut enabled_jobs = 0_u64;
    let mut disabled_jobs = 0_u64;
    let mut overdue_jobs = 0_u64;
    let mut due_soon_jobs = 0_u64;
    let mut succeeded_jobs = 0_u64;
    let mut failed_jobs = 0_u64;
    let mut skipped_jobs = 0_u64;
    let mut throttled_jobs = 0_u64;
    let mut denied_jobs = 0_u64;
    let mut jobs_payload = Vec::with_capacity(routines.len());

    for routine in routines {
        let next_run_at_unix_ms = json_i64_at(routine, "/next_run_at_unix_ms").unwrap_or_default();
        let enabled = json_bool_at(routine, "/enabled").unwrap_or(false);
        let overdue = enabled && next_run_at_unix_ms > 0 && next_run_at_unix_ms <= now_unix_ms;
        let due_soon = enabled
            && next_run_at_unix_ms > now_unix_ms
            && next_run_at_unix_ms.saturating_sub(now_unix_ms) <= 15 * 60 * 1_000;
        let late_by_ms = overdue.then_some(now_unix_ms.saturating_sub(next_run_at_unix_ms));

        if enabled {
            enabled_jobs = enabled_jobs.saturating_add(1);
        } else {
            disabled_jobs = disabled_jobs.saturating_add(1);
        }
        match json_optional_string_at(routine, "/last_outcome_kind").as_deref() {
            Some("success_with_output") | Some("success_no_op") => {
                succeeded_jobs = succeeded_jobs.saturating_add(1)
            }
            Some("failed") => failed_jobs = failed_jobs.saturating_add(1),
            Some("skipped") => skipped_jobs = skipped_jobs.saturating_add(1),
            Some("throttled") => throttled_jobs = throttled_jobs.saturating_add(1),
            Some("denied") => denied_jobs = denied_jobs.saturating_add(1),
            _ => {}
        }
        if overdue {
            overdue_jobs = overdue_jobs.saturating_add(1);
        }
        if due_soon {
            due_soon_jobs = due_soon_jobs.saturating_add(1);
        }

        jobs_payload.push(json!({
            "job": routine,
            "recent_run": json_value_at(routine, "/last_run").cloned(),
            "last_status": json_optional_string_at(routine, "/last_outcome_kind"),
            "last_outcome_message": json_optional_string_at(routine, "/last_outcome_message"),
            "overdue": overdue,
            "due_soon": due_soon,
            "late_by_ms": late_by_ms,
        }));
    }

    let summary = json!({
        "total_jobs": enabled_jobs + disabled_jobs,
        "enabled_jobs": enabled_jobs,
        "disabled_jobs": disabled_jobs,
        "overdue_jobs": overdue_jobs,
        "due_soon_jobs": due_soon_jobs,
        "succeeded_jobs": succeeded_jobs,
        "failed_jobs": failed_jobs,
        "skipped_jobs": skipped_jobs,
        "throttled_jobs": throttled_jobs,
        "denied_jobs": denied_jobs,
        "evaluated_at_unix_ms": now_unix_ms,
    });

    if json {
        return output::print_json_pretty(
            &json!({
                "summary": summary,
                "jobs": jobs_payload,
                "next_after_job_ulid": json_optional_string_at(payload, "/next_after_routine_id"),
            }),
            "failed to encode cron status output as JSON",
        );
    }

    println!(
        "cron.status total_jobs={} enabled_jobs={} disabled_jobs={} overdue_jobs={} due_soon_jobs={} succeeded_jobs={} failed_jobs={} skipped_jobs={} throttled_jobs={} denied_jobs={}",
        enabled_jobs + disabled_jobs,
        enabled_jobs,
        disabled_jobs,
        overdue_jobs,
        due_soon_jobs,
        succeeded_jobs,
        failed_jobs,
        skipped_jobs,
        throttled_jobs,
        denied_jobs
    );
    for job in &jobs_payload {
        let item = job.pointer("/job").unwrap_or(job);
        println!(
            "cron.job id={} name={} enabled={} next_run_at_unix_ms={} last_status={} overdue={} due_soon={} late_by_ms={}",
            json_optional_string_at(item, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(item, "/name").unwrap_or_else(|| "unknown".to_owned()),
            json_bool_at(item, "/enabled").unwrap_or(false),
            json_i64_at(item, "/next_run_at_unix_ms").unwrap_or_default(),
            json_optional_string_at(job, "/last_status").unwrap_or_else(|| "none".to_owned()),
            json_bool_at(job, "/overdue").unwrap_or(false),
            json_bool_at(job, "/due_soon").unwrap_or(false),
            json_i64_at(job, "/late_by_ms")
                .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_cron_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(
            &json!({
                "jobs": schedule_routine_array(payload),
                "next_after_job_ulid": json_optional_string_at(payload, "/next_after_routine_id"),
            }),
            "failed to encode cron list output as JSON",
        );
    }
    let jobs = schedule_routine_array(payload);
    println!(
        "cron.list jobs={} next_after={}",
        jobs.len(),
        json_optional_string_at(payload, "/next_after_routine_id")
            .unwrap_or_else(|| "none".to_owned()),
    );
    for job in jobs {
        println!(
            "cron.job id={} name={} enabled={} owner={} channel={} next_run_at_ms={}",
            json_optional_string_at(job, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(job, "/name").unwrap_or_else(|| "unknown".to_owned()),
            json_bool_at(job, "/enabled").unwrap_or(false),
            json_optional_string_at(job, "/owner_principal").unwrap_or_default(),
            json_optional_string_at(job, "/channel").unwrap_or_default(),
            json_i64_at(job, "/next_run_at_unix_ms").unwrap_or_default(),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_cron_show(payload: &Value, json: bool) -> Result<()> {
    let routine = payload.pointer("/routine").unwrap_or(payload);
    if json {
        return output::print_json_pretty(routine, "failed to encode cron show output as JSON");
    }
    println!(
        "cron.show id={} name={} enabled={} owner={} channel={} schedule_type={}",
        json_optional_string_at(routine, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(routine, "/name").unwrap_or_else(|| "unknown".to_owned()),
        json_bool_at(routine, "/enabled").unwrap_or(false),
        json_optional_string_at(routine, "/owner_principal").unwrap_or_default(),
        json_optional_string_at(routine, "/channel").unwrap_or_default(),
        json_optional_string_at(routine, "/schedule_type").unwrap_or_else(|| "unknown".to_owned()),
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_cron_mutation(event: &str, payload: &Value, json: bool) -> Result<()> {
    let routine = payload.pointer("/routine").unwrap_or(payload);
    if json {
        return output::print_json_pretty(routine, "failed to encode cron mutation output as JSON");
    }
    println!(
        "{event} id={} enabled={} owner={} channel={}",
        json_optional_string_at(routine, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
        json_bool_at(routine, "/enabled").unwrap_or(false),
        json_optional_string_at(routine, "/owner_principal").unwrap_or_default(),
        json_optional_string_at(routine, "/channel").unwrap_or_default(),
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_cron_runs(id: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(
            &json!({
                "runs": payload.pointer("/runs").cloned().unwrap_or_else(|| json!([])),
                "next_after_run_ulid": json_optional_string_at(payload, "/next_after_run_id"),
            }),
            "failed to encode cron runs output as JSON",
        );
    }
    let runs = payload.pointer("/runs").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
    println!(
        "cron.logs id={} runs={} next_after={}",
        id,
        runs.len(),
        json_optional_string_at(payload, "/next_after_run_id").unwrap_or_else(|| "none".to_owned())
    );
    for run in runs {
        println!(
            "cron.run run_id={} status={} started_at_ms={} finished_at_ms={} tool_calls={} tool_denies={}",
            json_optional_string_at(run, "/run_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(run, "/status").unwrap_or_else(|| "unknown".to_owned()),
            json_i64_at(run, "/started_at_unix_ms").unwrap_or_default(),
            json_i64_at(run, "/finished_at_unix_ms").unwrap_or_default(),
            json_i64_at(run, "/tool_calls").unwrap_or_default(),
            json_i64_at(run, "/tool_denies").unwrap_or_default(),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn schedule_routine_array(payload: &Value) -> &[Value] {
    payload.pointer("/routines").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

fn existing_schedule_type(routine: &Value) -> CronScheduleTypeArg {
    match json_optional_string_at(routine, "/schedule_type").as_deref() {
        Some("every") => CronScheduleTypeArg::Every,
        Some("at") => CronScheduleTypeArg::At,
        _ => CronScheduleTypeArg::Cron,
    }
}

fn existing_schedule_value(routine: &Value) -> String {
    match existing_schedule_type(routine) {
        CronScheduleTypeArg::Cron => {
            json_optional_string_at(routine, "/schedule_payload/expression").unwrap_or_default()
        }
        CronScheduleTypeArg::Every => {
            json_i64_at(routine, "/schedule_payload/interval_ms").unwrap_or_default().to_string()
        }
        CronScheduleTypeArg::At => {
            json_optional_string_at(routine, "/schedule_payload/timestamp_rfc3339")
                .unwrap_or_default()
        }
    }
}

fn existing_concurrency_policy(routine: &Value) -> CronConcurrencyPolicyArg {
    match json_optional_string_at(routine, "/concurrency_policy").as_deref() {
        Some("replace") => CronConcurrencyPolicyArg::Replace,
        Some("queue_one") => CronConcurrencyPolicyArg::QueueOne,
        _ => CronConcurrencyPolicyArg::Forbid,
    }
}

fn existing_misfire_policy(routine: &Value) -> CronMisfirePolicyArg {
    match json_optional_string_at(routine, "/misfire_policy").as_deref() {
        Some("catch_up") => CronMisfirePolicyArg::CatchUp,
        _ => CronMisfirePolicyArg::Skip,
    }
}

fn insert_schedule_value(
    payload: &mut Map<String, Value>,
    schedule_type: CronScheduleTypeArg,
    schedule: String,
) -> Result<()> {
    match schedule_type {
        CronScheduleTypeArg::Cron => {
            payload.insert("cron_expression".to_owned(), Value::String(schedule));
        }
        CronScheduleTypeArg::Every => {
            let interval_ms = schedule.parse::<u64>().with_context(|| {
                format!("failed to parse --schedule as milliseconds for schedule-type=every: {schedule}")
            })?;
            payload.insert("every_interval_ms".to_owned(), Value::from(interval_ms));
        }
        CronScheduleTypeArg::At => {
            payload.insert("at_timestamp_rfc3339".to_owned(), Value::String(schedule));
        }
    }
    Ok(())
}

fn minute_of_day_to_clock(value: i64) -> String {
    let total = value.rem_euclid(24 * 60);
    let hours = total / 60;
    let minutes = total % 60;
    format!("{hours:02}:{minutes:02}")
}

fn insert_optional_string(payload: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value
        .as_deref()
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(ToOwned::to_owned)
    {
        payload.insert(key.to_owned(), Value::String(value));
    }
}

fn cron_schedule_type_text(value: CronScheduleTypeArg) -> &'static str {
    match value {
        CronScheduleTypeArg::Cron => "cron",
        CronScheduleTypeArg::Every => "every",
        CronScheduleTypeArg::At => "at",
    }
}

fn cron_concurrency_policy_text(value: CronConcurrencyPolicyArg) -> &'static str {
    match value {
        CronConcurrencyPolicyArg::Forbid => "forbid",
        CronConcurrencyPolicyArg::Replace => "replace",
        CronConcurrencyPolicyArg::QueueOne => "queue_one",
    }
}

fn cron_misfire_policy_text(value: CronMisfirePolicyArg) -> &'static str {
    match value {
        CronMisfirePolicyArg::Skip => "skip",
        CronMisfirePolicyArg::CatchUp => "catch_up",
    }
}

struct ScheduleRoutineConfig {
    name: String,
    prompt: String,
    schedule_type: CronScheduleTypeArg,
    schedule: String,
    enabled: Option<bool>,
    concurrency: CronConcurrencyPolicyArg,
    retry_max_attempts: u32,
    retry_backoff_ms: u64,
    misfire: CronMisfirePolicyArg,
    jitter_ms: u64,
    owner: Option<String>,
    channel: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
}
