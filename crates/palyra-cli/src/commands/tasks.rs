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
        WorkboardCommand::List {
            limit,
            state,
            parent_work_item_id,
            objective_id,
            routine_id,
            include_terminal,
            json,
        } => {
            let payload = list_workboard_value(
                client,
                limit,
                state,
                parent_work_item_id,
                objective_id,
                routine_id,
                include_terminal,
            )
            .await?;
            emit_workboard_list(&payload, output::preferred_json(json))
        }
        WorkboardCommand::Create {
            title,
            summary,
            priority,
            session_id,
            run_id,
            parent_work_item_id,
            objective_id,
            routine_id,
            verification_state,
            dependencies_json,
            evidence_refs_json,
            artifact_refs_json,
            blocker_json,
            metadata_json,
            json,
        } => {
            let body = build_workboard_create_payload(WorkboardCreatePayloadInput {
                title,
                summary,
                priority,
                session_id,
                run_id,
                parent_work_item_id,
                objective_id,
                routine_id,
                verification_state,
                dependencies_json,
                evidence_refs_json,
                artifact_refs_json,
                blocker_json,
                metadata_json,
            })?;
            let payload =
                client.post_json_value("console/v1/workboard/items".to_owned(), &body).await?;
            emit_workboard_item("tasks.workboard.create", &payload, output::preferred_json(json))
        }
        WorkboardCommand::Show { id, json } => {
            let payload = get_workboard_item_value(client, id.as_str()).await?;
            emit_workboard_item("tasks.workboard.show", &payload, output::preferred_json(json))
        }
        WorkboardCommand::Update {
            id,
            state,
            priority,
            assigned_worker,
            clear_assigned_worker,
            parent_work_item_id,
            clear_parent_work_item,
            objective_id,
            clear_objective,
            routine_id,
            clear_routine,
            verification_state,
            dependencies_json,
            evidence_refs_json,
            artifact_refs_json,
            blocker_json,
            metadata_json,
            reason,
            json,
        } => {
            let body = build_workboard_update_payload(WorkboardUpdatePayloadInput {
                state,
                priority,
                assigned_worker,
                clear_assigned_worker,
                parent_work_item_id,
                clear_parent_work_item,
                objective_id,
                clear_objective,
                routine_id,
                clear_routine,
                verification_state,
                dependencies_json,
                evidence_refs_json,
                artifact_refs_json,
                blocker_json,
                metadata_json,
                reason,
            })?;
            let payload = client
                .post_json_value(
                    format!("console/v1/workboard/items/{}", percent_encode_component(id.as_str())),
                    &body,
                )
                .await?;
            emit_workboard_item("tasks.workboard.update", &payload, output::preferred_json(json))
        }
        WorkboardCommand::Block { id, reason, blocker_json, evidence_refs_json, json } => {
            let body = build_workboard_block_payload(reason, blocker_json, evidence_refs_json)?;
            let payload = client
                .post_json_value(
                    format!(
                        "console/v1/workboard/items/{}/block",
                        percent_encode_component(id.as_str())
                    ),
                    &body,
                )
                .await?;
            emit_workboard_item("tasks.workboard.block", &payload, output::preferred_json(json))
        }
        WorkboardCommand::LinkArtifact { id, artifact_ref_json, reason, json } => {
            let body = json!({
                "artifact_ref": parse_json_arg("--artifact-ref-json", artifact_ref_json.as_str())?,
                "reason": reason,
            });
            let payload = client
                .post_json_value(
                    format!(
                        "console/v1/workboard/items/{}/artifacts",
                        percent_encode_component(id.as_str())
                    ),
                    &body,
                )
                .await?;
            emit_workboard_item(
                "tasks.workboard.link-artifact",
                &payload,
                output::preferred_json(json),
            )
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
        WorkboardCommand::Complete {
            id,
            reason,
            evidence_refs_json,
            artifact_refs_json,
            verification_state,
            json,
        } => {
            let body = build_workboard_complete_payload(
                reason,
                evidence_refs_json,
                artifact_refs_json,
                verification_state,
            )?;
            let payload = client
                .post_json_value(
                    format!(
                        "console/v1/workboard/items/{}/complete",
                        percent_encode_component(id.as_str())
                    ),
                    &body,
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
    parent_work_item_id: Option<String>,
    objective_id: Option<String>,
    routine_id: Option<String>,
    include_terminal: bool,
) -> Result<Value> {
    let path = build_query_path(
        "console/v1/workboard",
        vec![
            ("limit", limit.map(|value| value.to_string())),
            ("state", state),
            ("parent_work_item_id", parent_work_item_id),
            ("objective_id", objective_id),
            ("routine_id", routine_id),
            ("include_terminal", Some(include_terminal.to_string())),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

async fn get_workboard_item_value(
    client: &control_plane::ControlPlaneClient,
    item_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!("console/v1/workboard/items/{}", percent_encode_component(item_id)))
        .await
        .map_err(Into::into)
}

struct WorkboardCreatePayloadInput {
    title: String,
    summary: Option<String>,
    priority: Option<i64>,
    session_id: Option<String>,
    run_id: Option<String>,
    parent_work_item_id: Option<String>,
    objective_id: Option<String>,
    routine_id: Option<String>,
    verification_state: Option<String>,
    dependencies_json: Option<String>,
    evidence_refs_json: Option<String>,
    artifact_refs_json: Option<String>,
    blocker_json: Option<String>,
    metadata_json: Option<String>,
}

fn build_workboard_create_payload(input: WorkboardCreatePayloadInput) -> Result<Value> {
    let mut body = serde_json::Map::new();
    body.insert("title".to_owned(), Value::String(input.title));
    insert_optional_string(&mut body, "summary", input.summary);
    insert_optional_i64(&mut body, "priority", input.priority);
    insert_optional_string(&mut body, "session_id", input.session_id);
    insert_optional_string(&mut body, "run_id", input.run_id);
    insert_optional_string(&mut body, "parent_work_item_id", input.parent_work_item_id);
    insert_optional_string(&mut body, "objective_id", input.objective_id);
    insert_optional_string(&mut body, "routine_id", input.routine_id);
    insert_optional_string(&mut body, "verification_state", input.verification_state);
    insert_optional_json(
        &mut body,
        "dependencies",
        "--dependencies-json",
        input.dependencies_json,
    )?;
    insert_optional_json(
        &mut body,
        "evidence_refs",
        "--evidence-refs-json",
        input.evidence_refs_json,
    )?;
    insert_optional_json(
        &mut body,
        "artifact_refs",
        "--artifact-refs-json",
        input.artifact_refs_json,
    )?;
    insert_optional_json(&mut body, "blocker", "--blocker-json", input.blocker_json)?;
    insert_optional_json(&mut body, "metadata", "--metadata-json", input.metadata_json)?;
    Ok(Value::Object(body))
}

struct WorkboardUpdatePayloadInput {
    state: Option<String>,
    priority: Option<i64>,
    assigned_worker: Option<String>,
    clear_assigned_worker: bool,
    parent_work_item_id: Option<String>,
    clear_parent_work_item: bool,
    objective_id: Option<String>,
    clear_objective: bool,
    routine_id: Option<String>,
    clear_routine: bool,
    verification_state: Option<String>,
    dependencies_json: Option<String>,
    evidence_refs_json: Option<String>,
    artifact_refs_json: Option<String>,
    blocker_json: Option<String>,
    metadata_json: Option<String>,
    reason: Option<String>,
}

fn build_workboard_update_payload(input: WorkboardUpdatePayloadInput) -> Result<Value> {
    let mut body = serde_json::Map::new();
    insert_optional_string(&mut body, "state", input.state);
    insert_optional_i64(&mut body, "priority", input.priority);
    insert_nullable_string(
        &mut body,
        "assigned_worker",
        "--assigned-worker",
        "--clear-assigned-worker",
        input.assigned_worker,
        input.clear_assigned_worker,
    )?;
    insert_nullable_string(
        &mut body,
        "parent_work_item_id",
        "--parent-work-item-id",
        "--clear-parent-work-item",
        input.parent_work_item_id,
        input.clear_parent_work_item,
    )?;
    insert_nullable_string(
        &mut body,
        "objective_id",
        "--objective-id",
        "--clear-objective",
        input.objective_id,
        input.clear_objective,
    )?;
    insert_nullable_string(
        &mut body,
        "routine_id",
        "--routine-id",
        "--clear-routine",
        input.routine_id,
        input.clear_routine,
    )?;
    insert_optional_string(&mut body, "verification_state", input.verification_state);
    insert_optional_json(
        &mut body,
        "dependencies",
        "--dependencies-json",
        input.dependencies_json,
    )?;
    insert_optional_json(
        &mut body,
        "evidence_refs",
        "--evidence-refs-json",
        input.evidence_refs_json,
    )?;
    insert_optional_json(
        &mut body,
        "artifact_refs",
        "--artifact-refs-json",
        input.artifact_refs_json,
    )?;
    insert_optional_json(&mut body, "blocker", "--blocker-json", input.blocker_json)?;
    insert_optional_json(&mut body, "metadata", "--metadata-json", input.metadata_json)?;
    insert_optional_string(&mut body, "reason", input.reason);
    Ok(Value::Object(body))
}

fn build_workboard_block_payload(
    reason: Option<String>,
    blocker_json: Option<String>,
    evidence_refs_json: Option<String>,
) -> Result<Value> {
    let mut body = serde_json::Map::new();
    insert_optional_string(&mut body, "reason", reason);
    insert_optional_json(&mut body, "blocker", "--blocker-json", blocker_json)?;
    insert_optional_json(&mut body, "evidence_refs", "--evidence-refs-json", evidence_refs_json)?;
    Ok(Value::Object(body))
}

fn build_workboard_complete_payload(
    reason: Option<String>,
    evidence_refs_json: Option<String>,
    artifact_refs_json: Option<String>,
    verification_state: Option<String>,
) -> Result<Value> {
    let mut body = serde_json::Map::new();
    insert_optional_string(&mut body, "reason", reason);
    insert_optional_json(&mut body, "evidence_refs", "--evidence-refs-json", evidence_refs_json)?;
    insert_optional_json(&mut body, "artifact_refs", "--artifact-refs-json", artifact_refs_json)?;
    insert_optional_string(&mut body, "verification_state", verification_state);
    Ok(Value::Object(body))
}

fn emit_tasks_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode tasks list as JSON");
    }
    let tasks = json_array_at(payload, "/tasks");
    let summary = json_value_at(payload, "/summary").unwrap_or(&Value::Null);
    output::print_text_line(
        format!(
            "tasks.list count={} active={} blocked={} failed={} terminal={}",
            tasks.len(),
            json_number_at(summary, "/active"),
            json_number_at(summary, "/blocked"),
            json_number_at(summary, "/failed"),
            json_number_at(summary, "/terminal")
        )
        .as_str(),
    )?;
    for task in tasks {
        output::print_text_line(
            format!(
                "tasks.task id={} source={} state={} title={} updated_at_ms={}",
                json_optional_string_at(task, "/task_id").unwrap_or_else(|| "unknown".to_owned()),
                json_optional_string_at(task, "/source_kind")
                    .unwrap_or_else(|| "unknown".to_owned()),
                json_optional_string_at(task, "/state").unwrap_or_else(|| "unknown".to_owned()),
                json_optional_string_at(task, "/title").unwrap_or_default(),
                json_number_at(task, "/updated_at_unix_ms")
            )
            .as_str(),
        )?;
    }
    Ok(())
}

fn emit_task_envelope(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode task envelope as JSON");
    }
    let task = json_value_at(payload, "/task").unwrap_or(payload);
    output::print_text_line(
        format!(
            "{event} id={} source={} state={} title={}",
            json_optional_string_at(task, "/task_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(task, "/source_kind").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(task, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(task, "/title").unwrap_or_default()
        )
        .as_str(),
    )?;
    Ok(())
}

fn emit_task_timeline(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode task timeline as JSON");
    }
    emit_task_envelope("tasks.timeline", payload, false)?;
    for event in json_array_at(payload, "/events") {
        output::print_text_line(
            format!(
                "tasks.event id={} type={} from={} to={} at_ms={} summary={}",
                json_optional_string_at(event, "/event_id").unwrap_or_else(|| "unknown".to_owned()),
                json_optional_string_at(event, "/event_type")
                    .unwrap_or_else(|| "unknown".to_owned()),
                json_optional_string_at(event, "/from_state").unwrap_or_else(|| "none".to_owned()),
                json_optional_string_at(event, "/to_state").unwrap_or_else(|| "none".to_owned()),
                json_number_at(event, "/created_at_unix_ms"),
                json_optional_string_at(event, "/summary").unwrap_or_default()
            )
            .as_str(),
        )?;
    }
    Ok(())
}

fn emit_workboard_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode WorkBoard list as JSON");
    }
    let items = json_array_at(payload, "/items");
    output::print_text_line(format!("tasks.workboard.list count={}", items.len()).as_str())?;
    for item in items {
        output::print_text_line(
            format!(
                "tasks.workboard.item id={} state={} priority={} title={} claim_owner={}",
                json_optional_string_at(item, "/work_item_id")
                    .unwrap_or_else(|| "unknown".to_owned()),
                json_optional_string_at(item, "/state").unwrap_or_else(|| "unknown".to_owned()),
                json_number_at(item, "/priority"),
                json_optional_string_at(item, "/title").unwrap_or_default(),
                json_optional_string_at(item, "/claim_owner").unwrap_or_else(|| "none".to_owned())
            )
            .as_str(),
        )?;
    }
    Ok(())
}

fn emit_workboard_item(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode WorkBoard item as JSON");
    }
    let item = json_value_at(payload, "/item").unwrap_or(payload);
    output::print_text_line(
        format!(
            "{event} id={} state={} title={} priority={}",
            json_optional_string_at(item, "/work_item_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(item, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(item, "/title").unwrap_or_default(),
            json_number_at(item, "/priority")
        )
        .as_str(),
    )?;
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

fn insert_optional_string(
    body: &mut serde_json::Map<String, Value>,
    key: &'static str,
    value: Option<String>,
) {
    if let Some(value) = value {
        body.insert(key.to_owned(), Value::String(value));
    }
}

fn insert_optional_i64(
    body: &mut serde_json::Map<String, Value>,
    key: &'static str,
    value: Option<i64>,
) {
    if let Some(value) = value {
        body.insert(key.to_owned(), Value::Number(value.into()));
    }
}

fn insert_nullable_string(
    body: &mut serde_json::Map<String, Value>,
    key: &'static str,
    set_arg_name: &'static str,
    clear_arg_name: &'static str,
    value: Option<String>,
    clear: bool,
) -> Result<()> {
    if clear && value.is_some() {
        anyhow::bail!("cannot combine {set_arg_name} with {clear_arg_name}");
    }
    if clear {
        body.insert(key.to_owned(), Value::Null);
    } else {
        insert_optional_string(body, key, value);
    }
    Ok(())
}

fn insert_optional_json(
    body: &mut serde_json::Map<String, Value>,
    key: &'static str,
    arg_name: &'static str,
    value: Option<String>,
) -> Result<()> {
    if let Some(value) = value {
        body.insert(key.to_owned(), parse_json_arg(arg_name, value.as_str())?);
    }
    Ok(())
}

fn parse_json_arg(arg_name: &'static str, raw: &str) -> Result<Value> {
    serde_json::from_str::<Value>(raw)
        .map_err(|error| anyhow::anyhow!("{arg_name} must be valid JSON: {error}"))
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
