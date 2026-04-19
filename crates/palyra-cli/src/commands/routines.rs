use std::{
    fs,
    io::{Read, Write},
    path::Path,
};

use palyra_control_plane as control_plane;
use serde_json::{json, Map, Value};

use crate::cli::{
    CronConcurrencyPolicyArg, CronMisfirePolicyArg, CronScheduleTypeArg, RoutineApprovalModeArg,
    RoutineDeliveryModeArg, RoutineExecutionPostureArg, RoutinePreviewTimezoneArg,
    RoutineRunModeArg, RoutineSilentPolicyArg, RoutineTriggerKindArg, RoutinesCommand,
};
use crate::*;

const ROUTINE_DUE_SOON_WINDOW_MS: i64 = 15 * 60 * 1_000;

pub(crate) fn run_routines(command: RoutinesCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_routines_async(command))
}

pub(crate) async fn run_routines_async(command: RoutinesCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        RoutinesCommand::Status {
            after,
            limit,
            trigger_kind,
            enabled,
            channel,
            template_id,
            json,
        } => {
            let payload = list_routines_value(
                &context.client,
                after.as_deref(),
                limit,
                trigger_kind.map(RoutineTriggerKindArg::as_str),
                enabled,
                channel.as_deref(),
                template_id.as_deref(),
            )
            .await?;
            emit_routines_status(&payload, output::preferred_json(json))
        }
        RoutinesCommand::List {
            after,
            limit,
            trigger_kind,
            enabled,
            channel,
            template_id,
            json,
        } => {
            let payload = list_routines_value(
                &context.client,
                after.as_deref(),
                limit,
                trigger_kind.map(RoutineTriggerKindArg::as_str),
                enabled,
                channel.as_deref(),
                template_id.as_deref(),
            )
            .await?;
            emit_routines_list(&payload, output::preferred_json(json))
        }
        RoutinesCommand::Show { id, json } => {
            let payload = get_routine_value(&context.client, id.as_str()).await?;
            emit_routine_envelope("routines.show", &payload, output::preferred_json(json))
        }
        RoutinesCommand::Upsert(args) => {
            let crate::cli::RoutineUpsertCommand {
                id,
                name,
                prompt,
                trigger_kind,
                owner,
                channel,
                session_key,
                session_label,
                enabled,
                natural_language_schedule,
                schedule_type,
                schedule,
                trigger_payload,
                trigger_payload_stdin,
                concurrency,
                retry_max_attempts,
                retry_backoff_ms,
                misfire,
                jitter_ms,
                delivery_mode,
                delivery_channel,
                delivery_failure_mode,
                delivery_failure_channel,
                silent_policy,
                run_mode,
                procedure_profile_id,
                skill_profile_id,
                provider_profile_id,
                execution_posture,
                quiet_hours_start,
                quiet_hours_end,
                quiet_hours_timezone,
                cooldown_ms,
                approval_mode,
                template_id,
                json,
            } = *args;
            let trigger_payload = read_optional_json_object(
                "routine trigger payload",
                trigger_payload,
                trigger_payload_stdin,
            )?;
            let payload = build_routine_upsert_payload(RoutineUpsertArgs {
                id,
                name,
                prompt,
                trigger_kind,
                owner,
                channel,
                session_key,
                session_label,
                enabled,
                natural_language_schedule,
                schedule_type,
                schedule,
                trigger_payload,
                concurrency,
                retry_max_attempts,
                retry_backoff_ms,
                misfire,
                jitter_ms,
                delivery_mode,
                delivery_channel,
                delivery_failure_mode,
                delivery_failure_channel,
                silent_policy,
                run_mode,
                procedure_profile_id,
                skill_profile_id,
                provider_profile_id,
                execution_posture,
                quiet_hours_start,
                quiet_hours_end,
                quiet_hours_timezone,
                cooldown_ms,
                approval_mode,
                template_id,
            })?;
            let response = upsert_routine_value(&context.client, &payload).await?;
            emit_routine_envelope("routines.upsert", &response, output::preferred_json(json))
        }
        RoutinesCommand::CreateFromTemplate {
            template_id,
            id,
            name,
            prompt,
            owner,
            channel,
            session_key,
            session_label,
            enabled,
            natural_language_schedule,
            delivery_channel,
            trigger_payload,
            trigger_payload_stdin,
            json,
        } => {
            let templates_payload = list_routine_templates_value(&context.client).await?;
            let template = template_from_payload(&templates_payload, template_id.as_str())?;
            let trigger_payload = read_optional_json_object(
                "template trigger payload",
                trigger_payload,
                trigger_payload_stdin,
            )?;
            let payload = build_template_upsert_payload(
                template,
                TemplateRoutineArgs {
                    id,
                    name,
                    prompt,
                    owner,
                    channel,
                    session_key,
                    session_label,
                    enabled,
                    natural_language_schedule,
                    delivery_channel,
                    trigger_payload,
                },
            )?;
            let response = upsert_routine_value(&context.client, &payload).await?;
            emit_routine_envelope(
                "routines.create_from_template",
                &response,
                output::preferred_json(json),
            )
        }
        RoutinesCommand::Enable { id, json } => {
            let payload = set_routine_enabled_value(&context.client, id.as_str(), true).await?;
            emit_routine_envelope("routines.enable", &payload, output::preferred_json(json))
        }
        RoutinesCommand::Disable { id, json } => {
            let payload = set_routine_enabled_value(&context.client, id.as_str(), false).await?;
            emit_routine_envelope("routines.disable", &payload, output::preferred_json(json))
        }
        RoutinesCommand::RunNow { id, json } => {
            let payload = run_routine_now_value(&context.client, id.as_str()).await?;
            emit_routine_run_action(
                "routines.run_now",
                id.as_str(),
                &payload,
                output::preferred_json(json),
            )
        }
        RoutinesCommand::TestRun {
            id,
            source_run_id,
            trigger_reason,
            trigger_payload,
            trigger_payload_stdin,
            json,
        } => {
            let trigger_payload = read_optional_json_object(
                "routine test-run payload",
                trigger_payload,
                trigger_payload_stdin,
            )?;
            let payload = test_run_routine_value(
                &context.client,
                id.as_str(),
                source_run_id,
                trigger_reason,
                trigger_payload,
            )
            .await?;
            emit_routine_run_action(
                "routines.test_run",
                id.as_str(),
                &payload,
                output::preferred_json(json),
            )
        }
        RoutinesCommand::Logs { id, after, limit, json } => {
            let payload =
                list_routine_runs_value(&context.client, id.as_str(), after.as_deref(), limit)
                    .await?;
            emit_routine_runs(id.as_str(), &payload, output::preferred_json(json))
        }
        RoutinesCommand::Dispatch {
            id,
            trigger_kind,
            trigger_reason,
            trigger_payload,
            trigger_payload_stdin,
            trigger_dedupe_key,
            json,
        } => {
            let trigger_payload = read_optional_json_object(
                "routine dispatch payload",
                trigger_payload,
                trigger_payload_stdin,
            )?
            .unwrap_or_else(|| Value::Object(Map::new()));
            let payload = dispatch_routine_value(
                &context.client,
                id.as_str(),
                trigger_kind.map(RoutineTriggerKindArg::as_str),
                trigger_reason,
                trigger_payload,
                trigger_dedupe_key,
            )
            .await?;
            emit_routine_run_action(
                "routines.dispatch",
                id.as_str(),
                &payload,
                output::preferred_json(json),
            )
        }
        RoutinesCommand::Delete { id, json } => {
            let payload = delete_routine_value(&context.client, id.as_str()).await?;
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &payload,
                    "failed to encode routine delete output as JSON",
                )
            } else {
                println!(
                    "routines.delete id={} deleted={}",
                    id,
                    json_bool_at(&payload, "/deleted").unwrap_or(false)
                );
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        RoutinesCommand::Templates { json } => {
            let payload = list_routine_templates_value(&context.client).await?;
            emit_routine_templates(&payload, output::preferred_json(json))
        }
        RoutinesCommand::SchedulePreview { phrase, timezone, json } => {
            let payload =
                preview_routine_schedule_value(&context.client, phrase.as_str(), timezone.as_str())
                    .await?;
            emit_routine_schedule_preview(&payload, output::preferred_json(json))
        }
        RoutinesCommand::Export { id, json: _ } => {
            let payload = export_routine_value(&context.client, id.as_str()).await?;
            let export = payload.pointer("/export").cloned().unwrap_or(payload);
            output::print_json_pretty(&export, "failed to encode routine export bundle as JSON")
        }
        RoutinesCommand::Import { file, stdin, id, enabled, json } => {
            let export = read_import_bundle(file.as_deref(), stdin)?;
            let payload = import_routine_value(&context.client, export, id, enabled).await?;
            emit_routine_import(&payload, output::preferred_json(json))
        }
    }
}

pub(crate) async fn list_routines_value(
    client: &control_plane::ControlPlaneClient,
    after: Option<&str>,
    limit: Option<u32>,
    trigger_kind: Option<&str>,
    enabled: Option<bool>,
    channel: Option<&str>,
    template_id: Option<&str>,
) -> Result<Value> {
    let path = build_query_path(
        "console/v1/routines",
        vec![
            ("after_routine_id", after.map(str::to_owned)),
            ("limit", limit.map(|value| value.to_string())),
            ("trigger_kind", trigger_kind.map(str::to_owned)),
            ("enabled", enabled.map(|value| value.to_string())),
            ("channel", channel.map(str::to_owned)),
            ("template_id", template_id.map(str::to_owned)),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

pub(crate) async fn get_routine_value(
    client: &control_plane::ControlPlaneClient,
    routine_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!("console/v1/routines/{}", percent_encode_component(routine_id)))
        .await
        .map_err(Into::into)
}

pub(crate) async fn upsert_routine_value(
    client: &control_plane::ControlPlaneClient,
    payload: &Map<String, Value>,
) -> Result<Value> {
    client.post_json_value("console/v1/routines", payload).await.map_err(Into::into)
}

pub(crate) async fn delete_routine_value(
    client: &control_plane::ControlPlaneClient,
    routine_id: &str,
) -> Result<Value> {
    client
        .post_json_value(
            format!("console/v1/routines/{}/delete", percent_encode_component(routine_id)),
            &json!({}),
        )
        .await
        .map_err(Into::into)
}

pub(crate) async fn set_routine_enabled_value(
    client: &control_plane::ControlPlaneClient,
    routine_id: &str,
    enabled: bool,
) -> Result<Value> {
    client
        .post_json_value(
            format!("console/v1/routines/{}/enabled", percent_encode_component(routine_id)),
            &json!({ "enabled": enabled }),
        )
        .await
        .map_err(Into::into)
}

pub(crate) async fn run_routine_now_value(
    client: &control_plane::ControlPlaneClient,
    routine_id: &str,
) -> Result<Value> {
    client
        .post_json_value(
            format!("console/v1/routines/{}/run-now", percent_encode_component(routine_id)),
            &json!({}),
        )
        .await
        .map_err(Into::into)
}

pub(crate) async fn test_run_routine_value(
    client: &control_plane::ControlPlaneClient,
    routine_id: &str,
    source_run_id: Option<String>,
    trigger_reason: Option<String>,
    trigger_payload: Option<Value>,
) -> Result<Value> {
    let mut payload = Map::new();
    insert_optional_string(&mut payload, "source_run_id", source_run_id);
    insert_optional_string(&mut payload, "trigger_reason", trigger_reason);
    if let Some(trigger_payload) = trigger_payload {
        payload.insert("trigger_payload".to_owned(), trigger_payload);
    }
    client
        .post_json_value(
            format!("console/v1/routines/{}/test-run", percent_encode_component(routine_id)),
            &payload,
        )
        .await
        .map_err(Into::into)
}

pub(crate) async fn list_routine_runs_value(
    client: &control_plane::ControlPlaneClient,
    routine_id: &str,
    after: Option<&str>,
    limit: Option<u32>,
) -> Result<Value> {
    let path = build_query_path(
        format!("console/v1/routines/{}/runs", percent_encode_component(routine_id)).as_str(),
        vec![
            ("after_run_id", after.map(str::to_owned)),
            ("limit", limit.map(|value| value.to_string())),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

pub(crate) async fn dispatch_routine_value(
    client: &control_plane::ControlPlaneClient,
    routine_id: &str,
    trigger_kind: Option<&str>,
    trigger_reason: Option<String>,
    trigger_payload: Value,
    trigger_dedupe_key: Option<String>,
) -> Result<Value> {
    let mut payload = Map::new();
    insert_optional_string(&mut payload, "trigger_kind", trigger_kind.map(str::to_owned));
    insert_optional_string(&mut payload, "trigger_reason", trigger_reason);
    payload.insert("trigger_payload".to_owned(), trigger_payload);
    insert_optional_string(&mut payload, "trigger_dedupe_key", trigger_dedupe_key);
    client
        .post_json_value(
            format!("console/v1/routines/{}/dispatch", percent_encode_component(routine_id)),
            &payload,
        )
        .await
        .map_err(Into::into)
}

pub(crate) async fn list_routine_templates_value(
    client: &control_plane::ControlPlaneClient,
) -> Result<Value> {
    client.get_json_value("console/v1/routines/templates").await.map_err(Into::into)
}

pub(crate) async fn preview_routine_schedule_value(
    client: &control_plane::ControlPlaneClient,
    phrase: &str,
    timezone: &str,
) -> Result<Value> {
    client
        .post_json_value(
            "console/v1/routines/schedule-preview",
            &json!({
                "phrase": phrase,
                "timezone": timezone,
            }),
        )
        .await
        .map_err(Into::into)
}

pub(crate) async fn export_routine_value(
    client: &control_plane::ControlPlaneClient,
    routine_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!(
            "console/v1/routines/{}/export",
            percent_encode_component(routine_id)
        ))
        .await
        .map_err(Into::into)
}

pub(crate) async fn import_routine_value(
    client: &control_plane::ControlPlaneClient,
    export: Value,
    routine_id: Option<String>,
    enabled: Option<bool>,
) -> Result<Value> {
    let mut payload = Map::new();
    payload.insert("export".to_owned(), export);
    insert_optional_string(&mut payload, "routine_id", routine_id);
    insert_optional_bool(&mut payload, "enabled", enabled);
    client.post_json_value("console/v1/routines/import", &payload).await.map_err(Into::into)
}

pub(crate) fn json_optional_string_at(value: &Value, pointer: &str) -> Option<String> {
    value.pointer(pointer).and_then(Value::as_str).map(ToOwned::to_owned)
}

pub(crate) fn json_bool_at(value: &Value, pointer: &str) -> Option<bool> {
    value.pointer(pointer).and_then(Value::as_bool)
}

pub(crate) fn json_i64_at(value: &Value, pointer: &str) -> Option<i64> {
    value.pointer(pointer).and_then(Value::as_i64)
}

pub(crate) fn json_value_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a Value> {
    value.pointer(pointer)
}

fn emit_routines_status(payload: &Value, json: bool) -> Result<()> {
    let routines = routine_array(payload);
    let now_unix_ms = unix_now_ms();
    let mut enabled_count = 0_u64;
    let mut disabled_count = 0_u64;
    let mut overdue_count = 0_u64;
    let mut due_soon_count = 0_u64;
    let mut outcome_counts = Map::new();
    let mut trigger_counts = Map::new();

    for routine in routines {
        if json_bool_at(routine, "/enabled").unwrap_or(false) {
            enabled_count = enabled_count.saturating_add(1);
        } else {
            disabled_count = disabled_count.saturating_add(1);
        }

        if let Some(trigger_kind) = json_optional_string_at(routine, "/trigger_kind") {
            bump_counter(&mut trigger_counts, trigger_kind.as_str());
        }
        if let Some(outcome_kind) = json_optional_string_at(routine, "/last_outcome_kind") {
            bump_counter(&mut outcome_counts, outcome_kind.as_str());
        }

        let next_run_at_unix_ms = json_i64_at(routine, "/next_run_at_unix_ms").unwrap_or_default();
        if json_bool_at(routine, "/enabled").unwrap_or(false)
            && next_run_at_unix_ms > 0
            && next_run_at_unix_ms <= now_unix_ms
        {
            overdue_count = overdue_count.saturating_add(1);
        }
        if json_bool_at(routine, "/enabled").unwrap_or(false)
            && next_run_at_unix_ms > now_unix_ms
            && next_run_at_unix_ms.saturating_sub(now_unix_ms) <= ROUTINE_DUE_SOON_WINDOW_MS
        {
            due_soon_count = due_soon_count.saturating_add(1);
        }
    }

    let summary = json!({
        "total_routines": routines.len(),
        "enabled_routines": enabled_count,
        "disabled_routines": disabled_count,
        "overdue_routines": overdue_count,
        "due_soon_routines": due_soon_count,
        "trigger_counts": trigger_counts,
        "outcome_counts": outcome_counts,
        "evaluated_at_unix_ms": now_unix_ms,
    });

    if json {
        return output::print_json_pretty(
            &json!({
                "summary": summary,
                "routines": routines,
                "next_after_routine_id": json_optional_string_at(payload, "/next_after_routine_id"),
            }),
            "failed to encode routine status output as JSON",
        );
    }

    println!(
        "routines.status total={} enabled={} disabled={} overdue={} due_soon={} success_with_output={} success_no_op={} skipped={} throttled={} failed={} denied={}",
        routines.len(),
        enabled_count,
        disabled_count,
        overdue_count,
        due_soon_count,
        counter_value(&outcome_counts, "success_with_output"),
        counter_value(&outcome_counts, "success_no_op"),
        counter_value(&outcome_counts, "skipped"),
        counter_value(&outcome_counts, "throttled"),
        counter_value(&outcome_counts, "failed"),
        counter_value(&outcome_counts, "denied"),
    );
    for routine in routines {
        emit_routine_list_line("routines.routine", routine)?;
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_routines_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode routine list output as JSON");
    }
    let routines = routine_array(payload);
    println!(
        "routines.list count={} next_after={}",
        routines.len(),
        json_optional_string_at(payload, "/next_after_routine_id")
            .unwrap_or_else(|| "none".to_owned())
    );
    for routine in routines {
        emit_routine_list_line("routines.routine", routine)?;
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_routine_list_line(prefix: &str, routine: &Value) -> Result<()> {
    println!(
        "{prefix} id={} name={} enabled={} trigger_kind={} summary=\"{}\" next_run_at_unix_ms={} last_outcome={} run_mode={} delivery={} silent_policy={} template_id={}",
        json_optional_string_at(routine, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(routine, "/name").unwrap_or_else(|| "unknown".to_owned()),
        json_bool_at(routine, "/enabled").unwrap_or(false),
        json_optional_string_at(routine, "/trigger_kind").unwrap_or_else(|| "unknown".to_owned()),
        routine_summary(routine),
        json_i64_at(routine, "/next_run_at_unix_ms").unwrap_or_default(),
        json_optional_string_at(routine, "/last_outcome_kind").unwrap_or_else(|| "none".to_owned()),
        json_optional_string_at(routine, "/run_mode").unwrap_or_else(|| "same_session".to_owned()),
        json_optional_string_at(routine, "/delivery_mode")
            .unwrap_or_else(|| "same_channel".to_owned()),
        json_optional_string_at(routine, "/silent_policy").unwrap_or_else(|| "noisy".to_owned()),
        json_optional_string_at(routine, "/template_id").unwrap_or_else(|| "none".to_owned()),
    );
    if let Some(message) =
        json_optional_string_at(routine, "/last_outcome_message").filter(|value| !value.is_empty())
    {
        println!(
            "{prefix}.last_outcome_message id={} {}",
            json_optional_string_at(routine, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
            message
        );
    }
    Ok(())
}

fn emit_routine_envelope(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode routine output as JSON");
    }
    let routine = payload.pointer("/routine").unwrap_or(payload);
    println!(
        "{event} id={} name={} enabled={} trigger_kind={} summary=\"{}\" run_mode={} delivery={} approval={} last_outcome={}",
        json_optional_string_at(routine, "/routine_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(routine, "/name").unwrap_or_else(|| "unknown".to_owned()),
        json_bool_at(routine, "/enabled").unwrap_or(false),
        json_optional_string_at(routine, "/trigger_kind").unwrap_or_else(|| "unknown".to_owned()),
        routine_summary(routine),
        json_optional_string_at(routine, "/run_mode").unwrap_or_else(|| "same_session".to_owned()),
        json_optional_string_at(routine, "/delivery_mode")
            .unwrap_or_else(|| "same_channel".to_owned()),
        json_optional_string_at(routine, "/approval_mode").unwrap_or_else(|| "none".to_owned()),
        json_optional_string_at(routine, "/last_outcome_kind").unwrap_or_else(|| "none".to_owned()),
    );
    println!(
        "{event}.details channel={} session_key={} session_label={} cooldown_ms={} quiet_hours={} execution_posture={} procedure_profile_id={} skill_profile_id={} provider_profile_id={} delivery_channel={} failure_delivery={} failure_channel={} silent_policy={} trigger_payload={}",
        json_optional_string_at(routine, "/channel").unwrap_or_default(),
        json_optional_string_at(routine, "/session_key").unwrap_or_default(),
        json_optional_string_at(routine, "/session_label").unwrap_or_default(),
        json_i64_at(routine, "/cooldown_ms").unwrap_or_default(),
        quiet_hours_summary(routine),
        json_optional_string_at(routine, "/execution_posture")
            .unwrap_or_else(|| "standard".to_owned()),
        json_optional_string_at(routine, "/procedure_profile_id").unwrap_or_default(),
        json_optional_string_at(routine, "/skill_profile_id").unwrap_or_default(),
        json_optional_string_at(routine, "/provider_profile_id").unwrap_or_default(),
        json_optional_string_at(routine, "/delivery_channel").unwrap_or_default(),
        json_optional_string_at(routine, "/delivery_failure_mode").unwrap_or_default(),
        json_optional_string_at(routine, "/delivery_failure_channel").unwrap_or_default(),
        json_optional_string_at(routine, "/silent_policy").unwrap_or_else(|| "noisy".to_owned()),
        compact_json(json_value_at(routine, "/trigger_payload").unwrap_or(&Value::Null)),
    );
    if let Some(success_reason) =
        json_optional_string_at(routine, "/delivery_preview/success/reason")
    {
        println!(
            "{event}.delivery success_announced={} failure_announced={} success_reason={} failure_reason={}",
            json_bool_at(routine, "/delivery_preview/success/announced").unwrap_or(false),
            json_bool_at(routine, "/delivery_preview/failure/announced").unwrap_or(false),
            success_reason,
            json_optional_string_at(routine, "/delivery_preview/failure/reason").unwrap_or_default(),
        );
    }
    if let Some(message) =
        json_optional_string_at(routine, "/last_outcome_message").filter(|value| !value.is_empty())
    {
        println!("{event}.last_outcome_message {message}");
    }
    if let Some(recommended_action) =
        json_optional_string_at(routine, "/troubleshooting/recommended_action")
    {
        println!(
            "{event}.troubleshooting failed_runs={} skipped_runs={} denied_runs={} recommended_action={}",
            json_i64_at(routine, "/troubleshooting/failed_runs").unwrap_or_default(),
            json_i64_at(routine, "/troubleshooting/skipped_runs").unwrap_or_default(),
            json_i64_at(routine, "/troubleshooting/denied_runs").unwrap_or_default(),
            recommended_action,
        );
    }
    if let Some(approval) = payload.pointer("/approval") {
        println!(
            "{event}.approval pending=true approval_id={} status={}",
            json_optional_string_at(approval, "/approval_id")
                .unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(approval, "/status").unwrap_or_else(|| "pending".to_owned()),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_routine_import(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(
            payload,
            "failed to encode routine import output as JSON",
        );
    }
    emit_routine_envelope("routines.import", payload, false)?;
    println!(
        "routines.import.source imported_from={}",
        json_optional_string_at(payload, "/imported_from").unwrap_or_else(|| "unknown".to_owned())
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_routine_run_action(
    event: &str,
    routine_id: &str,
    payload: &Value,
    json: bool,
) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode routine run output as JSON");
    }
    println!(
        "{event} id={} run_id={} status={} dispatch_mode={} message={}",
        routine_id,
        json_optional_string_at(payload, "/run_id").unwrap_or_default(),
        json_optional_string_at(payload, "/status").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(payload, "/dispatch_mode").unwrap_or_else(|| "normal".to_owned()),
        json_optional_string_at(payload, "/message").unwrap_or_default(),
    );
    if let Some(success_reason) =
        json_optional_string_at(payload, "/delivery_preview/success/reason")
    {
        println!(
            "{event}.delivery success_mode={} success_announced={} failure_mode={} failure_announced={} success_reason={} failure_reason={}",
            json_optional_string_at(payload, "/delivery_preview/success/mode").unwrap_or_default(),
            json_bool_at(payload, "/delivery_preview/success/announced").unwrap_or(false),
            json_optional_string_at(payload, "/delivery_preview/failure/mode").unwrap_or_default(),
            json_bool_at(payload, "/delivery_preview/failure/announced").unwrap_or(false),
            success_reason,
            json_optional_string_at(payload, "/delivery_preview/failure/reason")
                .unwrap_or_default(),
        );
    }
    if let Some(approval) = payload.pointer("/approval") {
        println!(
            "{event}.approval pending=true approval_id={} status={}",
            json_optional_string_at(approval, "/approval_id")
                .unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(approval, "/status").unwrap_or_else(|| "pending".to_owned()),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_routine_runs(routine_id: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode routine runs output as JSON");
    }
    let runs = payload.pointer("/runs").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
    println!(
        "routines.logs id={} runs={} next_after={}",
        routine_id,
        runs.len(),
        json_optional_string_at(payload, "/next_after_run_id").unwrap_or_else(|| "none".to_owned())
    );
    for run in runs {
        println!(
            "routines.run run_id={} status={} outcome={} trigger_kind={} dispatch_mode={} run_mode={} output_delivered={} started_at_ms={} finished_at_ms={} tool_calls={} tool_denies={}",
            json_optional_string_at(run, "/run_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(run, "/status").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(run, "/outcome_kind").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(run, "/trigger_kind").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(run, "/dispatch_mode").unwrap_or_else(|| "normal".to_owned()),
            json_optional_string_at(run, "/run_mode").unwrap_or_else(|| "same_session".to_owned()),
            json_bool_at(run, "/output_delivered").unwrap_or(false),
            json_i64_at(run, "/started_at_unix_ms").unwrap_or_default(),
            json_i64_at(run, "/finished_at_unix_ms").unwrap_or_default(),
            json_i64_at(run, "/tool_calls").unwrap_or_default(),
            json_i64_at(run, "/tool_denies").unwrap_or_default(),
        );
        if let Some(message) =
            json_optional_string_at(run, "/outcome_message").filter(|value| !value.is_empty())
        {
            println!(
                "routines.run.message run_id={} {}",
                json_optional_string_at(run, "/run_id").unwrap_or_else(|| "unknown".to_owned()),
                message
            );
        }
        println!(
            "routines.run.details run_id={} execution_posture={} provider_profile_id={} delivery_reason={} skip_reason={} approval_note={} safety_note={}",
            json_optional_string_at(run, "/run_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(run, "/execution_posture")
                .unwrap_or_else(|| "standard".to_owned()),
            json_optional_string_at(run, "/provider_profile_id").unwrap_or_default(),
            json_optional_string_at(run, "/delivery_reason").unwrap_or_default(),
            json_optional_string_at(run, "/skip_reason").unwrap_or_default(),
            json_optional_string_at(run, "/approval_note").unwrap_or_default(),
            json_optional_string_at(run, "/safety_note").unwrap_or_default(),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_routine_templates(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode routine templates as JSON");
    }
    let templates =
        payload.pointer("/templates").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
    println!(
        "routines.templates version={} count={}",
        json_i64_at(payload, "/version").unwrap_or_default(),
        templates.len(),
    );
    for template in templates {
        println!(
            "routines.template id={} title={} trigger_kind={} delivery={} approval={} default_name={} tags={}",
            json_optional_string_at(template, "/template_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(template, "/title").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(template, "/trigger_kind").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(template, "/delivery_mode")
                .unwrap_or_else(|| "same_channel".to_owned()),
            json_optional_string_at(template, "/approval_mode").unwrap_or_else(|| "none".to_owned()),
            json_optional_string_at(template, "/default_name").unwrap_or_default(),
            template
                .pointer("/tags")
                .and_then(Value::as_array)
                .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","))
                .unwrap_or_default(),
        );
        if let Some(schedule) = json_optional_string_at(template, "/natural_language_schedule") {
            println!(
                "routines.template.schedule id={} phrase={}",
                json_optional_string_at(template, "/template_id")
                    .unwrap_or_else(|| "unknown".to_owned()),
                schedule
            );
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_routine_schedule_preview(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(
            payload,
            "failed to encode routine schedule preview as JSON",
        );
    }
    let preview = payload.pointer("/preview").unwrap_or(payload);
    println!(
        "routines.schedule_preview phrase={} normalized_text={} schedule_type={} timezone={} next_run_at_unix_ms={}",
        json_optional_string_at(preview, "/phrase").unwrap_or_default(),
        json_optional_string_at(preview, "/normalized_text").unwrap_or_default(),
        json_optional_string_at(preview, "/schedule_type").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(preview, "/timezone").unwrap_or_else(|| "unknown".to_owned()),
        json_i64_at(preview, "/next_run_at_unix_ms").unwrap_or_default(),
    );
    println!(
        "routines.schedule_preview.explanation {}",
        json_optional_string_at(preview, "/explanation").unwrap_or_default()
    );
    println!(
        "routines.schedule_preview.payload {}",
        compact_json(json_value_at(preview, "/schedule_payload").unwrap_or(&Value::Null))
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_routine_upsert_payload(args: RoutineUpsertArgs) -> Result<Map<String, Value>> {
    let RoutineUpsertArgs {
        id,
        name,
        prompt,
        trigger_kind,
        owner,
        channel,
        session_key,
        session_label,
        enabled,
        natural_language_schedule,
        schedule_type,
        schedule,
        trigger_payload,
        concurrency,
        retry_max_attempts,
        retry_backoff_ms,
        misfire,
        jitter_ms,
        delivery_mode,
        delivery_channel,
        delivery_failure_mode,
        delivery_failure_channel,
        silent_policy,
        run_mode,
        procedure_profile_id,
        skill_profile_id,
        provider_profile_id,
        execution_posture,
        quiet_hours_start,
        quiet_hours_end,
        quiet_hours_timezone,
        cooldown_ms,
        approval_mode,
        template_id,
    } = args;

    if name.trim().is_empty() {
        anyhow::bail!("routine name cannot be empty");
    }
    if prompt.trim().is_empty() {
        anyhow::bail!("routine prompt cannot be empty");
    }

    let mut payload = Map::new();
    insert_optional_string(&mut payload, "routine_id", id);
    payload.insert("name".to_owned(), Value::String(name));
    payload.insert("prompt".to_owned(), Value::String(prompt));
    payload.insert("trigger_kind".to_owned(), Value::String(trigger_kind.as_str().to_owned()));
    insert_optional_string(&mut payload, "owner_principal", owner);
    insert_optional_string(&mut payload, "channel", channel);
    insert_optional_string(&mut payload, "session_key", session_key);
    insert_optional_string(&mut payload, "session_label", session_label);
    insert_optional_bool(&mut payload, "enabled", enabled);
    insert_schedule_fields(
        &mut payload,
        trigger_kind,
        natural_language_schedule,
        schedule_type,
        schedule,
    )?;
    if trigger_kind != RoutineTriggerKindArg::Schedule {
        payload.insert(
            "trigger_payload".to_owned(),
            trigger_payload.unwrap_or_else(|| Value::Object(Map::new())),
        );
    }
    payload.insert(
        "concurrency_policy".to_owned(),
        Value::String(cron_concurrency_policy_text(concurrency).to_owned()),
    );
    payload.insert("retry_max_attempts".to_owned(), Value::from(retry_max_attempts.max(1)));
    payload.insert("retry_backoff_ms".to_owned(), Value::from(retry_backoff_ms.max(1)));
    payload.insert(
        "misfire_policy".to_owned(),
        Value::String(cron_misfire_policy_text(misfire).to_owned()),
    );
    payload.insert("jitter_ms".to_owned(), Value::from(jitter_ms));
    payload.insert("delivery_mode".to_owned(), Value::String(delivery_mode.as_str().to_owned()));
    insert_optional_string(&mut payload, "delivery_channel", delivery_channel);
    insert_optional_string(
        &mut payload,
        "delivery_failure_mode",
        delivery_failure_mode.map(|value| value.as_str().to_owned()),
    );
    insert_optional_string(&mut payload, "delivery_failure_channel", delivery_failure_channel);
    payload.insert("silent_policy".to_owned(), Value::String(silent_policy.as_str().to_owned()));
    payload.insert("run_mode".to_owned(), Value::String(run_mode.as_str().to_owned()));
    insert_optional_string(&mut payload, "procedure_profile_id", procedure_profile_id);
    insert_optional_string(&mut payload, "skill_profile_id", skill_profile_id);
    insert_optional_string(&mut payload, "provider_profile_id", provider_profile_id);
    payload.insert(
        "execution_posture".to_owned(),
        Value::String(execution_posture.as_str().to_owned()),
    );
    insert_optional_string(&mut payload, "quiet_hours_start", quiet_hours_start);
    insert_optional_string(&mut payload, "quiet_hours_end", quiet_hours_end);
    insert_optional_string(
        &mut payload,
        "quiet_hours_timezone",
        quiet_hours_timezone.map(|value| value.as_str().to_owned()),
    );
    payload.insert("cooldown_ms".to_owned(), Value::from(cooldown_ms));
    payload.insert("approval_mode".to_owned(), Value::String(approval_mode.as_str().to_owned()));
    insert_optional_string(&mut payload, "template_id", template_id);
    Ok(payload)
}

fn build_template_upsert_payload(
    template: &Value,
    args: TemplateRoutineArgs,
) -> Result<Map<String, Value>> {
    let mut payload = Map::new();
    insert_optional_string(&mut payload, "routine_id", args.id);
    payload.insert(
        "name".to_owned(),
        Value::String(
            args.name.or_else(|| json_optional_string_at(template, "/default_name")).ok_or_else(
                || {
                    anyhow!(
                        "template {} is missing default_name",
                        json_optional_string_at(template, "/template_id")
                            .unwrap_or_else(|| "unknown".to_owned())
                    )
                },
            )?,
        ),
    );
    payload.insert(
        "prompt".to_owned(),
        Value::String(
            args.prompt.or_else(|| json_optional_string_at(template, "/prompt")).ok_or_else(
                || {
                    anyhow!(
                        "template {} is missing prompt",
                        json_optional_string_at(template, "/template_id")
                            .unwrap_or_else(|| "unknown".to_owned())
                    )
                },
            )?,
        ),
    );
    let trigger_kind = json_optional_string_at(template, "/trigger_kind")
        .ok_or_else(|| anyhow!("template is missing trigger_kind"))?;
    payload.insert("trigger_kind".to_owned(), Value::String(trigger_kind.clone()));
    insert_optional_string(&mut payload, "owner_principal", args.owner);
    insert_optional_string(&mut payload, "channel", args.channel.clone());
    insert_optional_string(&mut payload, "session_key", args.session_key);
    insert_optional_string(&mut payload, "session_label", args.session_label);
    insert_optional_bool(&mut payload, "enabled", args.enabled);
    if trigger_kind == "schedule" {
        let phrase = args
            .natural_language_schedule
            .or_else(|| json_optional_string_at(template, "/natural_language_schedule"))
            .ok_or_else(|| anyhow!("schedule template requires a natural-language schedule"))?;
        payload.insert("natural_language_schedule".to_owned(), Value::String(phrase));
    } else {
        payload.insert(
            "trigger_payload".to_owned(),
            args.trigger_payload.unwrap_or_else(|| Value::Object(Map::new())),
        );
    }

    let delivery_mode = json_optional_string_at(template, "/delivery_mode")
        .ok_or_else(|| anyhow!("template is missing delivery_mode"))?;
    payload.insert("delivery_mode".to_owned(), Value::String(delivery_mode.clone()));
    if delivery_mode == "specific_channel" {
        let resolved_delivery_channel =
            args.delivery_channel.or(args.channel).ok_or_else(|| {
                anyhow!(
                    "template {} requires --delivery-channel or --channel",
                    json_optional_string_at(template, "/template_id")
                        .unwrap_or_else(|| "unknown".to_owned())
                )
            })?;
        payload.insert("delivery_channel".to_owned(), Value::String(resolved_delivery_channel));
    }
    payload.insert(
        "approval_mode".to_owned(),
        Value::String(
            json_optional_string_at(template, "/approval_mode")
                .unwrap_or_else(|| "none".to_owned()),
        ),
    );
    insert_optional_string(
        &mut payload,
        "template_id",
        json_optional_string_at(template, "/template_id"),
    );
    Ok(payload)
}

fn insert_schedule_fields(
    payload: &mut Map<String, Value>,
    trigger_kind: RoutineTriggerKindArg,
    natural_language_schedule: Option<String>,
    schedule_type: Option<CronScheduleTypeArg>,
    schedule: Option<String>,
) -> Result<()> {
    if trigger_kind != RoutineTriggerKindArg::Schedule {
        return Ok(());
    }
    if let Some(phrase) =
        natural_language_schedule.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        payload.insert("natural_language_schedule".to_owned(), Value::String(phrase.to_owned()));
        return Ok(());
    }
    let schedule_type = schedule_type.ok_or_else(|| {
        anyhow!(
            "schedule routines require --natural-language-schedule or --schedule-type/--schedule"
        )
    })?;
    let schedule =
        schedule.as_deref().map(str::trim).filter(|value| !value.is_empty()).ok_or_else(|| {
            anyhow!("schedule routines require --schedule when --schedule-type is used")
        })?;
    payload.insert(
        "schedule_type".to_owned(),
        Value::String(cron_schedule_type_text(schedule_type).to_owned()),
    );
    match schedule_type {
        CronScheduleTypeArg::Cron => {
            payload.insert("cron_expression".to_owned(), Value::String(schedule.to_owned()));
        }
        CronScheduleTypeArg::Every => {
            let interval_ms = schedule.parse::<u64>().with_context(|| {
                format!(
                    "failed to parse --schedule as milliseconds for schedule-type=every: {schedule}"
                )
            })?;
            payload.insert("every_interval_ms".to_owned(), Value::from(interval_ms));
        }
        CronScheduleTypeArg::At => {
            payload.insert("at_timestamp_rfc3339".to_owned(), Value::String(schedule.to_owned()));
        }
    }
    Ok(())
}

fn template_from_payload<'a>(payload: &'a Value, template_id: &str) -> Result<&'a Value> {
    payload
        .pointer("/templates")
        .and_then(Value::as_array)
        .and_then(|templates| {
            templates.iter().find(|template| {
                json_optional_string_at(template, "/template_id")
                    .is_some_and(|candidate| candidate.eq_ignore_ascii_case(template_id))
            })
        })
        .ok_or_else(|| anyhow!("routine template not found: {template_id}"))
}

fn read_optional_json_object(
    label: &str,
    inline: Option<String>,
    from_stdin: bool,
) -> Result<Option<Value>> {
    match (inline, from_stdin) {
        (Some(text), false) => Ok(Some(Value::Object(parse_json_object(text.as_str(), label)?))),
        (None, true) => {
            let mut buffer = String::new();
            std::io::stdin()
                .read_to_string(&mut buffer)
                .with_context(|| format!("failed to read {label} from stdin"))?;
            Ok(Some(Value::Object(parse_json_object(buffer.as_str(), label)?)))
        }
        (None, false) => Ok(None),
        (Some(_), true) => {
            anyhow::bail!("--trigger-payload conflicts with --trigger-payload-stdin")
        }
    }
}

fn read_import_bundle(file: Option<&str>, stdin: bool) -> Result<Value> {
    let text = match (file, stdin) {
        (Some(path), false) => fs::read_to_string(Path::new(path))
            .with_context(|| format!("failed to read routine import file {path}"))?,
        (None, true) => {
            let mut buffer = String::new();
            std::io::stdin()
                .read_to_string(&mut buffer)
                .context("failed to read routine import bundle from stdin")?;
            buffer
        }
        (Some(_), true) => anyhow::bail!("--file conflicts with --stdin"),
        (None, false) => anyhow::bail!("routine import requires --file or --stdin"),
    };
    serde_json::from_str(text.as_str()).with_context(|| "routine import bundle must be valid JSON")
}

fn parse_json_object(text: &str, label: &str) -> Result<Map<String, Value>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Map::new());
    }
    let value: Value =
        serde_json::from_str(trimmed).with_context(|| format!("{label} must be valid JSON"))?;
    match value {
        Value::Object(object) => Ok(object),
        _ => anyhow::bail!("{label} must be a JSON object"),
    }
}

fn build_query_path(path: &str, pairs: Vec<(&str, Option<String>)>) -> String {
    let encoded = pairs
        .into_iter()
        .filter_map(|(key, value)| {
            value
                .as_deref()
                .map(str::trim)
                .filter(|candidate| !candidate.is_empty())
                .map(|candidate| format!("{key}={}", percent_encode_component(candidate)))
        })
        .collect::<Vec<_>>();
    if encoded.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{}", encoded.join("&"))
    }
}

fn percent_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(*byte));
            }
            other => {
                encoded.push('%');
                encoded.push_str(format!("{other:02X}").as_str());
            }
        }
    }
    encoded
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

fn insert_optional_bool(payload: &mut Map<String, Value>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        payload.insert(key.to_owned(), Value::Bool(value));
    }
}

fn routine_array(payload: &Value) -> &[Value] {
    payload.pointer("/routines").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

fn routine_summary(routine: &Value) -> String {
    let trigger_kind =
        json_optional_string_at(routine, "/trigger_kind").unwrap_or_else(|| "manual".to_owned());
    if trigger_kind != "schedule" {
        let event = json_optional_string_at(routine, "/trigger_payload/event")
            .or_else(|| json_optional_string_at(routine, "/trigger_payload/hook_id"))
            .or_else(|| json_optional_string_at(routine, "/trigger_payload/integration_id"))
            .unwrap_or_else(|| "custom matcher".to_owned());
        return format!("{trigger_kind} · {event}");
    }
    let schedule_type =
        json_optional_string_at(routine, "/schedule_type").unwrap_or_else(|| "schedule".to_owned());
    match schedule_type.as_str() {
        "every" => format!(
            "every {}",
            milliseconds_summary(json_i64_at(routine, "/schedule_payload/interval_ms"))
        ),
        "cron" => json_optional_string_at(routine, "/schedule_payload/expression")
            .unwrap_or_else(|| "cron expression unavailable".to_owned()),
        "at" => json_optional_string_at(routine, "/schedule_payload/timestamp_rfc3339")
            .unwrap_or_else(|| "one-off timestamp unavailable".to_owned()),
        _ => schedule_type,
    }
}

fn quiet_hours_summary(routine: &Value) -> String {
    let start = json_i64_at(routine, "/quiet_hours/start_minute_of_day");
    let end = json_i64_at(routine, "/quiet_hours/end_minute_of_day");
    match (start, end) {
        (Some(start), Some(end)) => format!(
            "{}-{} {}",
            minute_of_day_to_clock(start),
            minute_of_day_to_clock(end),
            json_optional_string_at(routine, "/quiet_hours/timezone")
                .unwrap_or_else(|| "local".to_owned())
        ),
        _ => "none".to_owned(),
    }
}

fn minute_of_day_to_clock(value: i64) -> String {
    let total = value.rem_euclid(24 * 60);
    let hours = total / 60;
    let minutes = total % 60;
    format!("{hours:02}:{minutes:02}")
}

fn milliseconds_summary(value: Option<i64>) -> String {
    let Some(value) = value.filter(|value| *value > 0) else {
        return "0 ms".to_owned();
    };
    if value % 3_600_000 == 0 {
        return format!("{}h", value / 3_600_000);
    }
    if value % 60_000 == 0 {
        return format!("{}m", value / 60_000);
    }
    if value % 1_000 == 0 {
        return format!("{}s", value / 1_000);
    }
    format!("{value} ms")
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
}
fn bump_counter(map: &mut Map<String, Value>, key: &str) {
    let current = map.get(key).and_then(Value::as_u64).unwrap_or(0);
    map.insert(key.to_owned(), Value::from(current.saturating_add(1)));
}

fn counter_value(map: &Map<String, Value>, key: &str) -> u64 {
    map.get(key).and_then(Value::as_u64).unwrap_or(0)
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

#[derive(Debug)]
struct RoutineUpsertArgs {
    id: Option<String>,
    name: String,
    prompt: String,
    trigger_kind: RoutineTriggerKindArg,
    owner: Option<String>,
    channel: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    enabled: Option<bool>,
    natural_language_schedule: Option<String>,
    schedule_type: Option<CronScheduleTypeArg>,
    schedule: Option<String>,
    trigger_payload: Option<Value>,
    concurrency: CronConcurrencyPolicyArg,
    retry_max_attempts: u32,
    retry_backoff_ms: u64,
    misfire: CronMisfirePolicyArg,
    jitter_ms: u64,
    delivery_mode: RoutineDeliveryModeArg,
    delivery_channel: Option<String>,
    delivery_failure_mode: Option<RoutineDeliveryModeArg>,
    delivery_failure_channel: Option<String>,
    silent_policy: RoutineSilentPolicyArg,
    run_mode: RoutineRunModeArg,
    procedure_profile_id: Option<String>,
    skill_profile_id: Option<String>,
    provider_profile_id: Option<String>,
    execution_posture: RoutineExecutionPostureArg,
    quiet_hours_start: Option<String>,
    quiet_hours_end: Option<String>,
    quiet_hours_timezone: Option<RoutinePreviewTimezoneArg>,
    cooldown_ms: u64,
    approval_mode: RoutineApprovalModeArg,
    template_id: Option<String>,
}

#[derive(Debug)]
struct TemplateRoutineArgs {
    id: Option<String>,
    name: Option<String>,
    prompt: Option<String>,
    owner: Option<String>,
    channel: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    enabled: Option<bool>,
    natural_language_schedule: Option<String>,
    delivery_channel: Option<String>,
    trigger_payload: Option<Value>,
}
