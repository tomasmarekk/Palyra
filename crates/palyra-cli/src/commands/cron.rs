//! Cron command surface: a schedule-focused view over daemon routines.
//!
//! Every cron job is a `trigger_kind=schedule` routine on the `/console/v1/routines` API;
//! this module reuses the request helpers in [`super::routines`] and only adds cron-flavored
//! payload shaping and output. Text output lines are pinned by CLI parity tests.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use serde_json::{json, Map, Value};

use crate::cli::{
    CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg, RoutineApprovalModeArg,
    RoutineExecutionPostureArg, RoutinePreviewTimezoneArg,
};
use crate::*;

use super::routines::{
    delete_routine_value, get_routine_value, json_bool_at, json_i64_at, json_optional_string_at,
    json_value_at, list_routine_runs_value, list_routines_value, parse_every_schedule_interval_ms,
    run_routine_now_value, set_routine_enabled_value, upsert_routine_value,
};

// Mirrors ROUTINE_DUE_SOON_WINDOW_MS in routines.rs so `cron status` and
// `routines status` agree on what counts as due soon.
const CRON_DUE_SOON_WINDOW_MS: i64 = 15 * 60 * 1_000;

/// Runs a `palyra cron` subcommand on a fresh Tokio runtime.
///
/// # Errors
/// Returns an error when the runtime cannot be built or the subcommand fails.
pub(crate) fn run_cron(command: CronCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_cron_async(command))
}

/// Dispatches a `palyra cron` subcommand against the daemon admin console.
///
/// # Errors
/// Returns an error when the admin-console connection, the request, or output
/// encoding fails, or when subcommand input validation rejects the arguments.
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
            let payload = get_routine_value(&context.client, id.value()).await?;
            emit_cron_show(&payload, output::preferred_json(json))
        }
        CronCommand::Add {
            name,
            prompt,
            prompt_stdin,
            schedule_type,
            schedule,
            timezone,
            enabled,
            concurrency,
            retry_max_attempts,
            retry_backoff_ms,
            misfire,
            jitter_ms,
            max_runs,
            owner,
            channel,
            session_key,
            session_label,
            workdir,
            execution_posture,
            approval_mode,
            json,
        } => {
            let prompt = resolve_prompt_input(prompt, prompt_stdin)?;
            let workdir = resolve_cron_workdir(workdir)?;
            let payload = build_schedule_routine_payload(
                None,
                ScheduleRoutineConfig {
                    name,
                    prompt,
                    schedule_type,
                    schedule,
                    schedule_timezone: Some(timezone),
                    enabled: Some(enabled),
                    concurrency,
                    retry_max_attempts,
                    retry_backoff_ms,
                    misfire,
                    jitter_ms,
                    max_runs,
                    owner,
                    channel,
                    session_key,
                    session_label,
                    workdir,
                    execution_posture,
                    approval_mode,
                },
            )?;
            let response = upsert_routine_value(&context.client, &payload).await?;
            emit_cron_mutation("cron.add", &response, output::preferred_json(json))
        }
        CronCommand::Update {
            id,
            name,
            prompt,
            prompt_stdin,
            schedule_type,
            schedule,
            timezone,
            enabled,
            concurrency,
            retry_max_attempts,
            retry_backoff_ms,
            misfire,
            jitter_ms,
            max_runs,
            owner,
            channel,
            session_key,
            session_label,
            workdir,
            execution_posture,
            approval_mode,
            json,
        } => {
            let routine_id = id.value();
            let any_other_field = name.is_some()
                || prompt.is_some()
                || prompt_stdin
                || schedule_type.is_some()
                || schedule.is_some()
                || timezone.is_some()
                || concurrency.is_some()
                || retry_max_attempts.is_some()
                || retry_backoff_ms.is_some()
                || misfire.is_some()
                || jitter_ms.is_some()
                || max_runs.is_some()
                || owner.is_some()
                || channel.is_some()
                || session_key.is_some()
                || session_label.is_some()
                || workdir.is_some()
                || execution_posture.is_some()
                || approval_mode.is_some();
            // Enabled-only updates use the dedicated enabled endpoint instead of the full
            // upsert, so no other routine fields are read back and rewritten.
            if cron_update_only_changes_enabled(enabled, any_other_field) {
                let enabled =
                    enabled.expect("cron_update_only_changes_enabled guarantees enabled is set");
                let response =
                    set_routine_enabled_value(&context.client, routine_id, enabled).await?;
                return emit_cron_mutation("cron.update", &response, output::preferred_json(json));
            }
            let existing = get_routine_value(&context.client, routine_id).await?;
            let routine = existing
                .pointer("/routine")
                .ok_or_else(|| anyhow!("routine response is missing the routine payload"))?;
            let prompt = resolve_optional_prompt_input(prompt, prompt_stdin)?;
            let workdir = resolve_cron_workdir(workdir)?;
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
                    schedule_timezone: Some(
                        timezone.unwrap_or_else(|| existing_schedule_timezone(routine)),
                    ),
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
                    max_runs: max_runs.or_else(|| {
                        json_i64_at(routine, "/max_runs")
                            .and_then(|value| u32::try_from(value).ok())
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
                    workdir: workdir.or_else(|| json_optional_string_at(routine, "/workdir")),
                    execution_posture,
                    approval_mode,
                },
            )?;
            let response = upsert_routine_value(&context.client, &payload).await?;
            emit_cron_mutation("cron.update", &response, output::preferred_json(json))
        }
        CronCommand::Enable { id, json } => {
            let payload = set_routine_enabled_value(&context.client, id.value(), true).await?;
            emit_cron_mutation("cron.enable", &payload, output::preferred_json(json))
        }
        CronCommand::Disable { id, json } => {
            let payload = set_routine_enabled_value(&context.client, id.value(), false).await?;
            emit_cron_mutation("cron.disable", &payload, output::preferred_json(json))
        }
        CronCommand::RunNow { id, json } => {
            let routine_id = id.value();
            let payload = run_routine_now_value(&context.client, routine_id).await?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode cron run-now output as JSON")
            } else {
                println!(
                    "cron.run_now id={} run_id={} status={} session_key={} message={}",
                    routine_id,
                    json_optional_string_at(&payload, "/run_id").unwrap_or_default(),
                    json_optional_string_at(&payload, "/status")
                        .unwrap_or_else(|| "unknown".to_owned()),
                    cron_run_session_key_display(&payload),
                    json_optional_string_at(&payload, "/message").unwrap_or_default(),
                );
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        CronCommand::Delete { id, json } => {
            let routine_id = id.value();
            let payload = delete_routine_value(&context.client, routine_id).await?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode cron delete output as JSON")
            } else {
                println!(
                    "cron.delete id={} deleted={}",
                    routine_id,
                    json_bool_at(&payload, "/deleted").unwrap_or(false)
                );
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        CronCommand::Logs { id, after, limit, json } => {
            let routine_id = id.value();
            let payload =
                list_routine_runs_value(&context.client, routine_id, after.as_deref(), limit)
                    .await?;
            emit_cron_runs(routine_id, &payload, output::preferred_json(json))
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
    // The routines list endpoint has no owner filter, so the owner constraint is
    // applied client-side on the returned page.
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

/// Builds the routine upsert payload for `cron add`/`cron update`.
///
/// `existing` is the current routine value during updates; routine fields that the
/// cron CLI does not expose are carried over from it so the upsert does not reset them.
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
    insert_optional_string(&mut payload, "workdir", config.workdir);
    insert_optional_string(
        &mut payload,
        "execution_posture",
        config.execution_posture.map(|value| value.as_str().to_owned()),
    );
    insert_optional_string(
        &mut payload,
        "approval_mode",
        config.approval_mode.map(|value| value.as_str().to_owned()),
    );
    if let Some(enabled) = config.enabled {
        payload.insert("enabled".to_owned(), Value::Bool(enabled));
    }
    payload.insert(
        "schedule_type".to_owned(),
        Value::String(cron_schedule_type_text(config.schedule_type).to_owned()),
    );
    insert_optional_string(
        &mut payload,
        "schedule_timezone",
        config.schedule_timezone.map(|value| value.as_str().to_owned()),
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
    insert_optional_u32(&mut payload, "max_runs", config.max_runs)?;

    if let Some(existing) = existing {
        preserve_existing_routine_fields(existing, &mut payload);
    }

    Ok(payload)
}

/// Normalizes `--workdir` to an existing canonical directory path.
///
/// Validation happens CLI-side so a bad path is rejected before the routine is
/// stored, rather than surfacing later as a failed scheduled run.
fn resolve_cron_workdir(workdir: Option<String>) -> Result<Option<String>> {
    let Some(raw) = workdir.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    if raw.contains('\0') {
        anyhow::bail!("--workdir must not contain NUL bytes");
    }
    let path = PathBuf::from(raw.as_str());
    let resolved = if path.is_absolute() { path } else { std::env::current_dir()?.join(path) };
    let canonical = fs::canonicalize(resolved.as_path()).with_context(|| {
        format!("failed to resolve --workdir {}", display_path(resolved.as_path()))
    })?;
    let metadata = fs::metadata(canonical.as_path()).with_context(|| {
        format!("failed to inspect --workdir {}", display_path(canonical.as_path()))
    })?;
    if !metadata.is_dir() {
        anyhow::bail!(
            "--workdir must resolve to a directory: {}",
            display_path(canonical.as_path())
        );
    }
    Ok(Some(canonical.to_string_lossy().into_owned()))
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}

/// Carries routine fields the cron CLI has no flags for (delivery, quiet hours,
/// cooldown, template) from the existing routine into an update payload.
/// Without this, a `cron update` upsert would reset them to daemon defaults.
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
    if !payload.contains_key("approval_mode") {
        payload.insert(
            "approval_mode".to_owned(),
            Value::String(
                json_optional_string_at(existing, "/approval_mode")
                    .unwrap_or_else(|| "none".to_owned()),
            ),
        );
    }
    if !payload.contains_key("execution_posture") {
        payload.insert(
            "execution_posture".to_owned(),
            Value::String(
                json_optional_string_at(existing, "/execution_posture")
                    .unwrap_or_else(|| "standard".to_owned()),
            ),
        );
    }
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
            && next_run_at_unix_ms.saturating_sub(now_unix_ms) <= CRON_DUE_SOON_WINDOW_MS;
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
            "cron.job id={} name={} enabled={} workdir={} next_run_at_unix_ms={} last_status={} overdue={} due_soon={} late_by_ms={}",
            json_optional_string_at(item, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(item, "/name").unwrap_or_else(|| "unknown".to_owned()),
            json_bool_at(item, "/enabled").unwrap_or(false),
            json_optional_string_at(item, "/workdir").unwrap_or_else(|| "none".to_owned()),
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
            "cron.job id={} name={} enabled={} owner={} channel={} workdir={} next_run_at_ms={}",
            json_optional_string_at(job, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(job, "/name").unwrap_or_else(|| "unknown".to_owned()),
            json_bool_at(job, "/enabled").unwrap_or(false),
            json_optional_string_at(job, "/owner_principal").unwrap_or_default(),
            json_optional_string_at(job, "/channel").unwrap_or_default(),
            json_optional_string_at(job, "/workdir").unwrap_or_else(|| "none".to_owned()),
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
        "cron.show id={} name={} enabled={} owner={} channel={} workdir={} schedule_type={}",
        json_optional_string_at(routine, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(routine, "/name").unwrap_or_else(|| "unknown".to_owned()),
        json_bool_at(routine, "/enabled").unwrap_or(false),
        json_optional_string_at(routine, "/owner_principal").unwrap_or_default(),
        json_optional_string_at(routine, "/channel").unwrap_or_default(),
        json_optional_string_at(routine, "/workdir").unwrap_or_else(|| "none".to_owned()),
        json_optional_string_at(routine, "/schedule_type").unwrap_or_else(|| "unknown".to_owned()),
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_cron_mutation(event: &str, payload: &Value, json: bool) -> Result<()> {
    let routine = payload.pointer("/routine").unwrap_or(payload);
    if json {
        return output::print_json_pretty(
            &cron_mutation_json_payload(routine, payload),
            "failed to encode cron mutation output as JSON",
        );
    }
    let approval_fragment = payload
        .pointer("/approval")
        .filter(|approval| !approval.is_null())
        .and_then(|approval| json_optional_string_at(approval, "/approval_id"))
        .map(|approval_id| format!(" approval_pending=true approval_id={approval_id}"))
        .unwrap_or_default();
    println!(
        "{event} id={} enabled={} owner={} channel={} workdir={}{}",
        json_optional_string_at(routine, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
        json_bool_at(routine, "/enabled").unwrap_or(false),
        json_optional_string_at(routine, "/owner_principal").unwrap_or_default(),
        json_optional_string_at(routine, "/channel").unwrap_or_default(),
        json_optional_string_at(routine, "/workdir").unwrap_or_else(|| "none".to_owned()),
        approval_fragment,
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn cron_mutation_json_payload(routine: &Value, payload: &Value) -> Value {
    let Some(approval) = payload.pointer("/approval").filter(|approval| !approval.is_null()) else {
        return routine.clone();
    };
    let mut output = routine.clone();
    if let Some(object) = output.as_object_mut() {
        object.insert("approval".to_owned(), approval.clone());
    }
    output
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
            "cron.run run_id={} status={} session_key={} workdir={} started_at_ms={} finished_at_ms={} tool_calls={} tool_denies={}",
            json_optional_string_at(run, "/run_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(run, "/status").unwrap_or_else(|| "unknown".to_owned()),
            cron_run_session_key_display(run),
            json_optional_string_at(run, "/trigger_payload/workdir")
                .or_else(|| json_optional_string_at(run, "/workdir"))
                .unwrap_or_else(|| "none".to_owned()),
            json_i64_at(run, "/started_at_unix_ms").unwrap_or_default(),
            json_i64_at(run, "/finished_at_unix_ms").unwrap_or_default(),
            json_i64_at(run, "/tool_calls").unwrap_or_default(),
            json_i64_at(run, "/tool_denies").unwrap_or_default(),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn cron_run_session_key(run: &Value) -> Option<String> {
    json_optional_string_at(run, "/session_key")
        .or_else(|| json_optional_string_at(run, "/output_lookup/session_key"))
}

// Session keys can encode private routing/identity details, so text output only
// reports their presence; `--json` consumers receive the raw payload instead.
fn cron_run_session_key_display(run: &Value) -> &'static str {
    if cron_run_session_key(run).is_some() {
        "<redacted>"
    } else {
        "none"
    }
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

fn existing_schedule_timezone(routine: &Value) -> RoutinePreviewTimezoneArg {
    json_optional_string_at(routine, "/schedule_payload/timezone")
        .and_then(|value| value.parse::<RoutinePreviewTimezoneArg>().ok())
        .unwrap_or_else(RoutinePreviewTimezoneArg::utc)
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
            let interval_ms = parse_every_schedule_interval_ms(schedule.as_str())?;
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

fn insert_optional_u32(
    payload: &mut Map<String, Value>,
    key: &str,
    value: Option<u32>,
) -> Result<()> {
    if let Some(value) = value {
        if value == 0 {
            anyhow::bail!("--{} must be greater than zero", key.replace('_', "-"));
        }
        payload.insert(key.to_owned(), Value::from(value));
    }
    Ok(())
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

fn cron_update_only_changes_enabled(enabled: Option<bool>, any_other_field: bool) -> bool {
    enabled.is_some() && !any_other_field
}

struct ScheduleRoutineConfig {
    name: String,
    prompt: String,
    schedule_type: CronScheduleTypeArg,
    schedule: String,
    schedule_timezone: Option<RoutinePreviewTimezoneArg>,
    enabled: Option<bool>,
    concurrency: CronConcurrencyPolicyArg,
    retry_max_attempts: u32,
    retry_backoff_ms: u64,
    misfire: CronMisfirePolicyArg,
    jitter_ms: u64,
    max_runs: Option<u32>,
    owner: Option<String>,
    channel: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    workdir: Option<String>,
    execution_posture: Option<RoutineExecutionPostureArg>,
    approval_mode: Option<RoutineApprovalModeArg>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        build_schedule_routine_payload, cron_mutation_json_payload, cron_run_session_key,
        cron_run_session_key_display, cron_update_only_changes_enabled, ScheduleRoutineConfig,
    };
    use crate::cli::{
        CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg,
        RoutineApprovalModeArg, RoutineExecutionPostureArg, RoutinePreviewTimezoneArg,
    };

    #[test]
    fn cron_update_enabled_only_uses_enabled_endpoint() {
        assert!(cron_update_only_changes_enabled(Some(false), false));
        assert!(cron_update_only_changes_enabled(Some(true), false));
    }

    #[test]
    fn cron_update_with_other_fields_uses_full_upsert() {
        assert!(!cron_update_only_changes_enabled(Some(false), true));
        assert!(!cron_update_only_changes_enabled(None, false));
    }

    #[test]
    fn cron_add_payload_leaves_workdir_sensitive_posture_to_daemon_default() {
        let payload = build_schedule_routine_payload(
            None,
            ScheduleRoutineConfig {
                name: "workdir job".to_owned(),
                prompt: "write report".to_owned(),
                schedule_type: CronScheduleTypeArg::Every,
                schedule: "1m".to_owned(),
                schedule_timezone: Some(RoutinePreviewTimezoneArg::local()),
                enabled: Some(true),
                concurrency: CronConcurrencyPolicyArg::Forbid,
                retry_max_attempts: 1,
                retry_backoff_ms: 1_000,
                misfire: CronMisfirePolicyArg::Skip,
                jitter_ms: 0,
                max_runs: Some(2),
                owner: None,
                channel: None,
                session_key: None,
                session_label: None,
                workdir: Some("C:\\workspace".to_owned()),
                execution_posture: None,
                approval_mode: None,
            },
        )
        .expect("cron payload should build");

        assert_eq!(
            payload.get("workdir").and_then(serde_json::Value::as_str),
            Some("C:\\workspace")
        );
        assert!(payload.get("execution_posture").is_none());
        assert!(payload.get("approval_mode").is_none());
        assert_eq!(
            payload.get("schedule_timezone").and_then(serde_json::Value::as_str),
            Some("local")
        );
        assert_eq!(payload.get("max_runs").and_then(serde_json::Value::as_u64), Some(2));
    }

    #[test]
    fn cron_add_payload_preserves_named_timezone() {
        let payload = build_schedule_routine_payload(
            None,
            ScheduleRoutineConfig {
                name: "weekly digest".to_owned(),
                prompt: "write digest".to_owned(),
                schedule_type: CronScheduleTypeArg::Cron,
                schedule: "0 9 * * 1".to_owned(),
                schedule_timezone: Some(
                    "Europe/Prague".parse().expect("named timezone arg should parse"),
                ),
                enabled: Some(false),
                concurrency: CronConcurrencyPolicyArg::Forbid,
                retry_max_attempts: 1,
                retry_backoff_ms: 1_000,
                misfire: CronMisfirePolicyArg::Skip,
                jitter_ms: 0,
                max_runs: None,
                owner: None,
                channel: None,
                session_key: None,
                session_label: None,
                workdir: None,
                execution_posture: None,
                approval_mode: None,
            },
        )
        .expect("cron payload should build");

        assert_eq!(
            payload.get("schedule_timezone").and_then(serde_json::Value::as_str),
            Some("Europe/Prague")
        );
    }

    #[test]
    fn cron_update_payload_preserves_or_overrides_execution_controls() {
        let existing = json!({
            "routine_id": "01TEST",
            "execution_posture": "sensitive_tools",
            "approval_mode": "before_enable"
        });
        let preserved = build_schedule_routine_payload(
            Some(&existing),
            ScheduleRoutineConfig {
                name: "workdir job".to_owned(),
                prompt: "write report".to_owned(),
                schedule_type: CronScheduleTypeArg::Every,
                schedule: "1m".to_owned(),
                schedule_timezone: Some(RoutinePreviewTimezoneArg::local()),
                enabled: Some(true),
                concurrency: CronConcurrencyPolicyArg::Forbid,
                retry_max_attempts: 1,
                retry_backoff_ms: 1_000,
                misfire: CronMisfirePolicyArg::Skip,
                jitter_ms: 0,
                max_runs: None,
                owner: None,
                channel: None,
                session_key: None,
                session_label: None,
                workdir: None,
                execution_posture: None,
                approval_mode: None,
            },
        )
        .expect("cron payload should build");
        assert_eq!(
            preserved.get("execution_posture").and_then(serde_json::Value::as_str),
            Some("sensitive_tools")
        );
        assert_eq!(
            preserved.get("approval_mode").and_then(serde_json::Value::as_str),
            Some("before_enable")
        );

        let overridden = build_schedule_routine_payload(
            Some(&existing),
            ScheduleRoutineConfig {
                execution_posture: Some(RoutineExecutionPostureArg::Standard),
                approval_mode: Some(RoutineApprovalModeArg::None),
                ..ScheduleRoutineConfig {
                    name: "workdir job".to_owned(),
                    prompt: "write report".to_owned(),
                    schedule_type: CronScheduleTypeArg::Every,
                    schedule: "1m".to_owned(),
                    schedule_timezone: Some(RoutinePreviewTimezoneArg::local()),
                    enabled: Some(true),
                    concurrency: CronConcurrencyPolicyArg::Forbid,
                    retry_max_attempts: 1,
                    retry_backoff_ms: 1_000,
                    misfire: CronMisfirePolicyArg::Skip,
                    jitter_ms: 0,
                    max_runs: None,
                    owner: None,
                    channel: None,
                    session_key: None,
                    session_label: None,
                    workdir: None,
                    execution_posture: None,
                    approval_mode: None,
                }
            },
        )
        .expect("cron payload should build");
        assert_eq!(
            overridden.get("execution_posture").and_then(serde_json::Value::as_str),
            Some("standard")
        );
        assert_eq!(
            overridden.get("approval_mode").and_then(serde_json::Value::as_str),
            Some("none")
        );
    }

    #[test]
    fn cron_mutation_json_payload_surfaces_pending_approval() {
        let payload = json!({
            "routine": {
                "routine_id": "01TEST",
                "enabled": false
            },
            "approval": {
                "approval_id": "01APPROVAL",
                "subject_id": "routine:01TEST:before_enable"
            }
        });
        let routine = payload.pointer("/routine").expect("routine should exist");
        let output = cron_mutation_json_payload(routine, &payload);

        assert_eq!(
            output.pointer("/approval/approval_id").and_then(serde_json::Value::as_str),
            Some("01APPROVAL")
        );
    }

    #[test]
    fn cron_run_session_key_reads_top_level_and_lookup_key() {
        let top_level = json!({ "session_key": "cron:daily:run-1" });
        assert_eq!(cron_run_session_key(&top_level).as_deref(), Some("cron:daily:run-1"));

        let lookup = json!({
            "output_lookup": {
                "session_key": "cron:daily:run-2"
            }
        });
        assert_eq!(cron_run_session_key(&lookup).as_deref(), Some("cron:daily:run-2"));
    }

    #[test]
    fn cron_run_session_key_display_redacts_present_keys() {
        let top_level = json!({ "session_key": "cron:daily:run-1" });
        assert_eq!(cron_run_session_key_display(&top_level), "<redacted>");

        let lookup = json!({
            "output_lookup": {
                "session_key": "cron:daily:run-2"
            }
        });
        assert_eq!(cron_run_session_key_display(&lookup), "<redacted>");

        assert_eq!(cron_run_session_key_display(&json!({})), "none");
    }
}
