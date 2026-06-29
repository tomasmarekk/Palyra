//! TaskRuntime and WorkBoard CLI commands over the daemon console API.

use palyra_control_plane as control_plane;
use serde_json::{json, Value};

use crate::cli::{TasksCommand, WorkboardCommand};
use crate::commands::routines::{json_optional_string_at, json_value_at};
use crate::*;

/// Runs a `palyra tasks` subcommand on a fresh Tokio runtime.
///
/// # Errors
/// Fails when the runtime cannot be built or the async handler fails.
pub(crate) fn run_tasks(command: TasksCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_tasks_async(command))
}

/// Dispatches a `palyra tasks` subcommand against the admin console API.
///
/// # Errors
/// Fails when the console request or output encoding fails.
pub(crate) async fn run_tasks_async(command: TasksCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        TasksCommand::List { limit, state, include_terminal, json } => {
            let payload = list_tasks_value(&context.client, limit, state, include_terminal).await?;
            emit_tasks_list(&payload, output::preferred_json(json))
        }
        TasksCommand::Show { id, json } => {
            let payload = get_task_value(&context.client, id.as_str()).await?;
            emit_task_envelope("tasks.show", &payload, output::preferred_json(json))
        }
        TasksCommand::Timeline { id, json } => {
            let payload = task_timeline_value(&context.client, id.as_str()).await?;
            emit_task_timeline(&payload, output::preferred_json(json))
        }
        TasksCommand::Cancel { id, reason, json } => {
            let payload = task_action_value(&context.client, id.as_str(), "cancel", reason).await?;
            emit_task_envelope("tasks.cancel", &payload, output::preferred_json(json))
        }
        TasksCommand::Pause { id, reason, json } => {
            let payload = task_action_value(&context.client, id.as_str(), "pause", reason).await?;
            emit_task_envelope("tasks.pause", &payload, output::preferred_json(json))
        }
        TasksCommand::Retry { id, reason, json } => {
            let payload = task_action_value(&context.client, id.as_str(), "retry", reason).await?;
            emit_task_envelope("tasks.retry", &payload, output::preferred_json(json))
        }
        TasksCommand::Workboard { command } => run_workboard_async(&context.client, command).await,
    }
}

async fn run_workboard_async(
    client: &control_plane::ControlPlaneClient,
    command: WorkboardCommand,
) -> Result<()> {
    match command {
        WorkboardCommand::List { limit, state, include_terminal, json } => {
            let payload = list_workboard_value(client, limit, state, include_terminal).await?;
            emit_workboard_list(&payload, output::preferred_json(json))
        }
        WorkboardCommand::Create { title, summary, priority, session_id, run_id, json } => {
            let payload = client
                .post_json_value(
                    "console/v1/workboard/items".to_owned(),
                    &json!({
                        "title": title,
                        "summary": summary,
                        "priority": priority,
                        "session_id": session_id,
                        "run_id": run_id,
                    }),
                )
                .await?;
            emit_workboard_item("tasks.workboard.create", &payload, output::preferred_json(json))
        }
        WorkboardCommand::Claim { id, worker, lease_ms, json } => {
            let payload = client
                .post_json_value(
                    format!(
                        "console/v1/workboard/items/{}/claim",
                        percent_encode_component(id.as_str())
                    ),
                    &json!({ "worker": worker, "lease_ms": lease_ms }),
                )
                .await?;
            emit_workboard_item("tasks.workboard.claim", &payload, output::preferred_json(json))
        }
        WorkboardCommand::Heartbeat { id, json } => {
            let payload = client
                .post_json_value(
                    format!(
                        "console/v1/workboard/items/{}/heartbeat",
                        percent_encode_component(id.as_str())
                    ),
                    &json!({}),
                )
                .await?;
            emit_workboard_item("tasks.workboard.heartbeat", &payload, output::preferred_json(json))
        }
        WorkboardCommand::Complete { id, reason, json } => {
            let payload = client
                .post_json_value(
                    format!(
                        "console/v1/workboard/items/{}/complete",
                        percent_encode_component(id.as_str())
                    ),
                    &reason_payload(reason),
                )
                .await?;
            emit_workboard_item("tasks.workboard.complete", &payload, output::preferred_json(json))
        }
    }
}

async fn list_tasks_value(
    client: &control_plane::ControlPlaneClient,
    limit: Option<u32>,
    state: Option<String>,
    include_terminal: bool,
) -> Result<Value> {
    let path = build_query_path(
        "console/v1/tasks",
        vec![
            ("limit", limit.map(|value| value.to_string())),
            ("state", state),
            ("include_terminal", Some(include_terminal.to_string())),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

async fn get_task_value(
    client: &control_plane::ControlPlaneClient,
    task_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!("console/v1/tasks/{}", percent_encode_component(task_id)))
        .await
        .map_err(Into::into)
}

async fn task_timeline_value(
    client: &control_plane::ControlPlaneClient,
    task_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!("console/v1/tasks/{}/timeline", percent_encode_component(task_id)))
        .await
        .map_err(Into::into)
}

async fn task_action_value(
    client: &control_plane::ControlPlaneClient,
    task_id: &str,
    action: &str,
    reason: Option<String>,
) -> Result<Value> {
    client
        .post_json_value(
            format!(
                "console/v1/tasks/{}/{}",
                percent_encode_component(task_id),
                percent_encode_component(action)
            ),
            &reason_payload(reason),
        )
        .await
        .map_err(Into::into)
}

async fn list_workboard_value(
    client: &control_plane::ControlPlaneClient,
    limit: Option<u32>,
    state: Option<String>,
    include_terminal: bool,
) -> Result<Value> {
    let path = build_query_path(
        "console/v1/workboard",
        vec![
            ("limit", limit.map(|value| value.to_string())),
            ("state", state),
            ("include_terminal", Some(include_terminal.to_string())),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

fn emit_tasks_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode tasks list as JSON");
    }
    let tasks = json_array_at(payload, "/tasks");
    let summary = json_value_at(payload, "/summary").unwrap_or(&Value::Null);
    println!(
        "tasks.list count={} active={} blocked={} failed={} terminal={}",
        tasks.len(),
        json_number_at(summary, "/active"),
        json_number_at(summary, "/blocked"),
        json_number_at(summary, "/failed"),
        json_number_at(summary, "/terminal")
    );
    for task in tasks {
        println!(
            "tasks.task id={} source={} state={} title={} updated_at_ms={}",
            json_optional_string_at(task, "/task_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(task, "/source_kind").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(task, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(task, "/title").unwrap_or_default(),
            json_number_at(task, "/updated_at_unix_ms")
        );
    }
    Ok(())
}

fn emit_task_envelope(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode task envelope as JSON");
    }
    let task = json_value_at(payload, "/task").unwrap_or(payload);
    println!(
        "{event} id={} source={} state={} title={}",
        json_optional_string_at(task, "/task_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(task, "/source_kind").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(task, "/state").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(task, "/title").unwrap_or_default()
    );
    Ok(())
}

fn emit_task_timeline(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode task timeline as JSON");
    }
    emit_task_envelope("tasks.timeline", payload, false)?;
    for event in json_array_at(payload, "/events") {
        println!(
            "tasks.event id={} type={} from={} to={} at_ms={} summary={}",
            json_optional_string_at(event, "/event_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(event, "/event_type").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(event, "/from_state").unwrap_or_else(|| "none".to_owned()),
            json_optional_string_at(event, "/to_state").unwrap_or_else(|| "none".to_owned()),
            json_number_at(event, "/created_at_unix_ms"),
            json_optional_string_at(event, "/summary").unwrap_or_default()
        );
    }
    Ok(())
}

fn emit_workboard_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode WorkBoard list as JSON");
    }
    let items = json_array_at(payload, "/items");
    println!("tasks.workboard.list count={}", items.len());
    for item in items {
        println!(
            "tasks.workboard.item id={} state={} priority={} title={} claim_owner={}",
            json_optional_string_at(item, "/work_item_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(item, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_number_at(item, "/priority"),
            json_optional_string_at(item, "/title").unwrap_or_default(),
            json_optional_string_at(item, "/claim_owner").unwrap_or_else(|| "none".to_owned())
        );
    }
    Ok(())
}

fn emit_workboard_item(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode WorkBoard item as JSON");
    }
    let item = json_value_at(payload, "/item").unwrap_or(payload);
    println!(
        "{event} id={} state={} title={} priority={}",
        json_optional_string_at(item, "/work_item_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(item, "/state").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(item, "/title").unwrap_or_default(),
        json_number_at(item, "/priority")
    );
    Ok(())
}

fn reason_payload(reason: Option<String>) -> Value {
    match reason.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    }) {
        Some(reason) => json!({ "reason": reason }),
        None => json!({}),
    }
}

fn json_array_at<'a>(value: &'a Value, pointer: &str) -> &'a [Value] {
    value.pointer(pointer).and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

fn json_number_at(value: &Value, pointer: &str) -> i64 {
    value.pointer(pointer).and_then(Value::as_i64).unwrap_or(0)
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
