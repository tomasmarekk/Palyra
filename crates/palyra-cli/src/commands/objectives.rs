use palyra_control_plane as control_plane;
use serde_json::{json, Map, Value};

use crate::cli::{
    ObjectiveKindArg, ObjectivePriorityArg, ObjectiveScheduleTypeArg, ObjectiveStateArg,
    ObjectiveUpsertCommandArgs, ObjectivesCommand, RoutineApprovalModeArg, RoutineDeliveryModeArg,
    RoutinePreviewTimezoneArg,
};
use crate::commands::routines::json_optional_string_at;
use crate::*;

pub(crate) fn run_objectives(command: ObjectivesCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_objectives_async(command))
}

pub(crate) async fn run_objectives_async(command: ObjectivesCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        ObjectivesCommand::Status { after, limit, kind, state, json }
        | ObjectivesCommand::List { after, limit, kind, state, json } => {
            let payload = list_objectives_value(
                &context.client,
                after.as_deref(),
                limit,
                kind.map(ObjectiveKindArg::as_str),
                state.map(ObjectiveStateArg::as_str),
            )
            .await?;
            emit_objectives_list(&payload, output::preferred_json(json))
        }
        ObjectivesCommand::Show { id, json } => {
            let payload = get_objective_value(&context.client, id.as_str()).await?;
            emit_objective_envelope("objectives.show", &payload, output::preferred_json(json))
        }
        ObjectivesCommand::Summary { id, json } => {
            let payload = get_objective_summary_value(&context.client, id.as_str()).await?;
            if output::preferred_json(json) {
                output::print_json_pretty(&payload, "failed to encode objective summary as JSON")
            } else {
                println!(
                    "{}",
                    payload
                        .pointer("/summary_markdown")
                        .and_then(Value::as_str)
                        .unwrap_or("Objective summary is unavailable.")
                );
                Ok(())
            }
        }
        ObjectivesCommand::Upsert(args) => {
            let ObjectiveUpsertCommandArgs {
                id,
                kind,
                name,
                prompt,
                owner,
                channel,
                session_key,
                session_label,
                priority,
                max_runs,
                max_tokens,
                budget_notes,
                current_focus,
                success_criteria,
                exit_condition,
                next_recommended_step,
                standing_order,
                enabled,
                natural_language_schedule,
                schedule_type,
                schedule,
                delivery_mode,
                delivery_channel,
                quiet_hours_start,
                quiet_hours_end,
                quiet_hours_timezone,
                cooldown_ms,
                approval_mode,
                json,
            } = *args;
            let payload = build_objective_upsert_payload(ObjectiveUpsertArgs {
                id,
                kind,
                name,
                prompt,
                owner,
                channel,
                session_key,
                session_label,
                priority,
                max_runs,
                max_tokens,
                budget_notes,
                current_focus,
                success_criteria,
                exit_condition,
                next_recommended_step,
                standing_order,
                enabled,
                natural_language_schedule,
                schedule_type,
                schedule,
                delivery_mode,
                delivery_channel,
                quiet_hours_start,
                quiet_hours_end,
                quiet_hours_timezone,
                cooldown_ms,
                approval_mode,
            })?;
            let response = upsert_objective_value(&context.client, &payload).await?;
            emit_objective_envelope("objectives.upsert", &response, output::preferred_json(json))
        }
        ObjectivesCommand::Fire { id, reason, json } => {
            let payload =
                objective_lifecycle_value(&context.client, id.as_str(), "fire", reason).await?;
            emit_objective_envelope("objectives.fire", &payload, output::preferred_json(json))
        }
        ObjectivesCommand::Pause { id, reason, json } => {
            let payload =
                objective_lifecycle_value(&context.client, id.as_str(), "pause", reason).await?;
            emit_objective_envelope("objectives.pause", &payload, output::preferred_json(json))
        }
        ObjectivesCommand::Resume { id, reason, json } => {
            let payload =
                objective_lifecycle_value(&context.client, id.as_str(), "resume", reason).await?;
            emit_objective_envelope("objectives.resume", &payload, output::preferred_json(json))
        }
        ObjectivesCommand::Cancel { id, reason, json } => {
            let payload =
                objective_lifecycle_value(&context.client, id.as_str(), "cancel", reason).await?;
            emit_objective_envelope("objectives.cancel", &payload, output::preferred_json(json))
        }
        ObjectivesCommand::Archive { id, reason, json } => {
            let payload =
                objective_lifecycle_value(&context.client, id.as_str(), "archive", reason).await?;
            emit_objective_envelope("objectives.archive", &payload, output::preferred_json(json))
        }
    }
}

pub(crate) async fn list_objectives_value(
    client: &control_plane::ControlPlaneClient,
    after: Option<&str>,
    limit: Option<u32>,
    kind: Option<&str>,
    state: Option<&str>,
) -> Result<Value> {
    let path = build_query_path(
        "console/v1/objectives",
        vec![
            ("after_objective_id", after.map(str::to_owned)),
            ("limit", limit.map(|value| value.to_string())),
            ("kind", kind.map(str::to_owned)),
            ("state", state.map(str::to_owned)),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

pub(crate) async fn get_objective_value(
    client: &control_plane::ControlPlaneClient,
    objective_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!("console/v1/objectives/{}", percent_encode_component(objective_id)))
        .await
        .map_err(Into::into)
}

pub(crate) async fn get_objective_summary_value(
    client: &control_plane::ControlPlaneClient,
    objective_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!(
            "console/v1/objectives/{}/summary",
            percent_encode_component(objective_id)
        ))
        .await
        .map_err(Into::into)
}

pub(crate) async fn upsert_objective_value(
    client: &control_plane::ControlPlaneClient,
    payload: &Map<String, Value>,
) -> Result<Value> {
    client.post_json_value("console/v1/objectives", payload).await.map_err(Into::into)
}

pub(crate) async fn objective_lifecycle_value(
    client: &control_plane::ControlPlaneClient,
    objective_id: &str,
    action: &str,
    reason: Option<String>,
) -> Result<Value> {
    client
        .post_json_value(
            format!("console/v1/objectives/{}/lifecycle", percent_encode_component(objective_id)),
            &json!({
                "action": action,
                "reason": reason,
            }),
        )
        .await
        .map_err(Into::into)
}

struct ObjectiveUpsertArgs {
    id: Option<String>,
    kind: ObjectiveKindArg,
    name: String,
    prompt: String,
    owner: Option<String>,
    channel: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    priority: ObjectivePriorityArg,
    max_runs: Option<u32>,
    max_tokens: Option<u64>,
    budget_notes: Option<String>,
    current_focus: Option<String>,
    success_criteria: Option<String>,
    exit_condition: Option<String>,
    next_recommended_step: Option<String>,
    standing_order: Option<String>,
    enabled: Option<bool>,
    natural_language_schedule: Option<String>,
    schedule_type: Option<ObjectiveScheduleTypeArg>,
    schedule: Option<String>,
    delivery_mode: RoutineDeliveryModeArg,
    delivery_channel: Option<String>,
    quiet_hours_start: Option<String>,
    quiet_hours_end: Option<String>,
    quiet_hours_timezone: Option<RoutinePreviewTimezoneArg>,
    cooldown_ms: u64,
    approval_mode: RoutineApprovalModeArg,
}

fn build_objective_upsert_payload(args: ObjectiveUpsertArgs) -> Result<Map<String, Value>> {
    let mut payload = Map::new();
    insert_optional_string(&mut payload, "objective_id", args.id);
    payload.insert("kind".to_owned(), Value::String(args.kind.as_str().to_owned()));
    payload.insert("name".to_owned(), Value::String(args.name));
    payload.insert("prompt".to_owned(), Value::String(args.prompt));
    insert_optional_string(&mut payload, "owner_principal", args.owner);
    insert_optional_string(&mut payload, "channel", args.channel);
    insert_optional_string(&mut payload, "session_key", args.session_key);
    insert_optional_string(&mut payload, "session_label", args.session_label);
    payload.insert("priority".to_owned(), Value::String(args.priority.as_str().to_owned()));
    if args.max_runs.is_some() || args.max_tokens.is_some() || args.budget_notes.is_some() {
        payload.insert(
            "budget".to_owned(),
            json!({
                "max_runs": args.max_runs,
                "max_tokens": args.max_tokens,
                "notes": args.budget_notes,
            }),
        );
    }
    insert_optional_string(&mut payload, "current_focus", args.current_focus);
    insert_optional_string(&mut payload, "success_criteria", args.success_criteria);
    insert_optional_string(&mut payload, "exit_condition", args.exit_condition);
    insert_optional_string(&mut payload, "next_recommended_step", args.next_recommended_step);
    insert_optional_string(&mut payload, "standing_order", args.standing_order);
    insert_optional_bool(&mut payload, "enabled", args.enabled);
    insert_optional_string(
        &mut payload,
        "natural_language_schedule",
        args.natural_language_schedule,
    );
    insert_optional_string(
        &mut payload,
        "schedule_type",
        args.schedule_type.map(|value| value.as_str().to_owned()),
    );
    if let Some(schedule) = args.schedule {
        match args.schedule_type {
            Some(ObjectiveScheduleTypeArg::Cron) => {
                payload.insert("cron_expression".to_owned(), Value::String(schedule));
            }
            Some(ObjectiveScheduleTypeArg::Every) => {
                let interval_ms = schedule.parse::<u64>().context(
                    "objective schedule must be an integer interval in milliseconds for schedule_type=every",
                )?;
                payload.insert("every_interval_ms".to_owned(), Value::from(interval_ms));
            }
            Some(ObjectiveScheduleTypeArg::At) => {
                payload.insert("at_timestamp_rfc3339".to_owned(), Value::String(schedule));
            }
            None => {}
        }
    }
    payload
        .insert("delivery_mode".to_owned(), Value::String(args.delivery_mode.as_str().to_owned()));
    insert_optional_string(&mut payload, "delivery_channel", args.delivery_channel);
    insert_optional_string(&mut payload, "quiet_hours_start", args.quiet_hours_start);
    insert_optional_string(&mut payload, "quiet_hours_end", args.quiet_hours_end);
    insert_optional_string(
        &mut payload,
        "quiet_hours_timezone",
        args.quiet_hours_timezone.map(|value| value.as_str().to_owned()),
    );
    payload.insert("cooldown_ms".to_owned(), Value::from(args.cooldown_ms));
    payload
        .insert("approval_mode".to_owned(), Value::String(args.approval_mode.as_str().to_owned()));
    Ok(payload)
}

fn emit_objectives_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode objectives list as JSON");
    }
    let objectives =
        payload.pointer("/objectives").and_then(Value::as_array).cloned().unwrap_or_default();
    if objectives.is_empty() {
        println!("No objectives found.");
        return Ok(());
    }
    for objective in objectives {
        let objective_id = json_optional_string_at(&objective, "/objective_id").unwrap_or_default();
        let kind =
            json_optional_string_at(&objective, "/kind").unwrap_or_else(|| "unknown".to_owned());
        let state =
            json_optional_string_at(&objective, "/state").unwrap_or_else(|| "unknown".to_owned());
        let name = json_optional_string_at(&objective, "/name").unwrap_or_default();
        let focus = json_optional_string_at(&objective, "/current_focus")
            .unwrap_or_else(|| "No focus.".to_owned());
        println!("{objective_id} [{kind}/{state}] {name}");
        println!("  focus: {focus}");
    }
    Ok(())
}

fn emit_objective_envelope(_event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode objective output as JSON");
    }
    let objective = payload.pointer("/objective").unwrap_or(payload);
    let objective_id = json_optional_string_at(objective, "/objective_id").unwrap_or_default();
    let kind = json_optional_string_at(objective, "/kind").unwrap_or_else(|| "unknown".to_owned());
    let state =
        json_optional_string_at(objective, "/state").unwrap_or_else(|| "unknown".to_owned());
    let name = json_optional_string_at(objective, "/name").unwrap_or_default();
    println!("{objective_id} [{kind}/{state}] {name}");
    if let Some(focus) = json_optional_string_at(objective, "/current_focus") {
        println!("focus: {focus}");
    }
    if let Some(next_step) = json_optional_string_at(objective, "/next_recommended_step") {
        println!("next: {next_step}");
    }
    Ok(())
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
