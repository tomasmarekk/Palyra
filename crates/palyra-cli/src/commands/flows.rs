//! Flow orchestration commands over the daemon console API: list, show,
//! pause/resume/cancel, and per-step retry/skip/compensate actions.
//!
//! Payloads stay as raw JSON values so the CLI tolerates console schema
//! additions; the fetch helpers are shared with other command surfaces.

use palyra_control_plane as control_plane;
use serde_json::{json, Value};

use crate::cli::{FlowStateArg, FlowsCommand};
use crate::commands::routines::{json_optional_string_at, json_value_at};
use crate::*;

/// Runs a `palyra flows` subcommand on a fresh Tokio runtime.
///
/// # Errors
/// Fails when the runtime cannot be built or the async handler fails.
pub(crate) fn run_flows(command: FlowsCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_flows_async(command))
}

/// Dispatches a `palyra flows` subcommand against the admin console API.
///
/// # Errors
/// Fails when the console connection, the endpoint call, or output encoding
/// fails.
pub(crate) async fn run_flows_async(command: FlowsCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        FlowsCommand::List { limit, state, active_only, json } => {
            let payload = list_flows_value(&context.client, limit, state, !active_only).await?;
            emit_flows_list(&payload, output::preferred_json(json))
        }
        FlowsCommand::Show { id, json } => {
            let payload = get_flow_value(&context.client, id.as_str()).await?;
            emit_flow_envelope("flows.show", &payload, output::preferred_json(json))
        }
        FlowsCommand::Pause { id, reason, json } => {
            let payload = flow_action_value(&context.client, id.as_str(), "pause", reason).await?;
            emit_flow_envelope("flows.pause", &payload, output::preferred_json(json))
        }
        FlowsCommand::Resume { id, reason, json } => {
            let payload = flow_action_value(&context.client, id.as_str(), "resume", reason).await?;
            emit_flow_envelope("flows.resume", &payload, output::preferred_json(json))
        }
        FlowsCommand::Cancel { id, reason, json } => {
            let payload = flow_action_value(&context.client, id.as_str(), "cancel", reason).await?;
            emit_flow_envelope("flows.cancel", &payload, output::preferred_json(json))
        }
        FlowsCommand::RetryStep { id, step_id, reason, json } => {
            let payload =
                step_action_value(&context.client, id.as_str(), step_id.as_str(), "retry", reason)
                    .await?;
            emit_flow_envelope("flows.retry_step", &payload, output::preferred_json(json))
        }
        FlowsCommand::SkipStep { id, step_id, reason, json } => {
            let payload =
                step_action_value(&context.client, id.as_str(), step_id.as_str(), "skip", reason)
                    .await?;
            emit_flow_envelope("flows.skip_step", &payload, output::preferred_json(json))
        }
        FlowsCommand::CompensateStep { id, step_id, reason, json } => {
            let payload = step_action_value(
                &context.client,
                id.as_str(),
                step_id.as_str(),
                "compensate",
                reason,
            )
            .await?;
            emit_flow_envelope("flows.compensate_step", &payload, output::preferred_json(json))
        }
    }
}

/// Fetches the flows list document with optional limit and state filters.
///
/// # Errors
/// Fails when the console request fails.
pub(crate) async fn list_flows_value(
    client: &control_plane::ControlPlaneClient,
    limit: Option<u32>,
    state: Option<FlowStateArg>,
    include_terminal: bool,
) -> Result<Value> {
    let path = build_query_path(
        "console/v1/flows",
        vec![
            ("limit", limit.map(|value| value.to_string())),
            ("state", state.map(|value| value.as_str().to_owned())),
            ("include_terminal", Some(include_terminal.to_string())),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

/// Fetches one flow document by id.
///
/// # Errors
/// Fails when the console request fails.
pub(crate) async fn get_flow_value(
    client: &control_plane::ControlPlaneClient,
    flow_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!("console/v1/flows/{}", percent_encode_component(flow_id)))
        .await
        .map_err(Into::into)
}

/// Posts a flow-level action (pause/resume/cancel) with an optional reason.
///
/// # Errors
/// Fails when the console request fails.
pub(crate) async fn flow_action_value(
    client: &control_plane::ControlPlaneClient,
    flow_id: &str,
    action: &str,
    reason: Option<String>,
) -> Result<Value> {
    let payload = reason_payload(reason);
    client
        .post_json_value(
            format!(
                "console/v1/flows/{}/{}",
                percent_encode_component(flow_id),
                percent_encode_component(action)
            ),
            &payload,
        )
        .await
        .map_err(Into::into)
}

/// Posts a step-level action (retry/skip/compensate) with an optional
/// reason.
///
/// # Errors
/// Fails when the console request fails.
pub(crate) async fn step_action_value(
    client: &control_plane::ControlPlaneClient,
    flow_id: &str,
    step_id: &str,
    action: &str,
    reason: Option<String>,
) -> Result<Value> {
    let payload = reason_payload(reason);
    client
        .post_json_value(
            format!(
                "console/v1/flows/{}/steps/{}/{}",
                percent_encode_component(flow_id),
                percent_encode_component(step_id),
                percent_encode_component(action)
            ),
            &payload,
        )
        .await
        .map_err(Into::into)
}

fn emit_flows_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode flows list as JSON");
    }
    let flows = flow_array(payload);
    let summary = json_value_at(payload, "/summary").unwrap_or(&Value::Null);
    println!(
        "flows.list count={} active={} blocked={} waiting_for_approval={}",
        flows.len(),
        json_number_at(summary, "/active"),
        json_number_at(summary, "/blocked"),
        json_number_at(summary, "/waiting_for_approval")
    );
    for flow in flows {
        println!(
            "flows.flow id={} state={} mode={} title={} current_step={} revision={} updated_at_ms={}",
            json_optional_string_at(flow, "/flow_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(flow, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(flow, "/mode").unwrap_or_else(|| "managed".to_owned()),
            json_optional_string_at(flow, "/title").unwrap_or_else(|| "untitled".to_owned()),
            json_optional_string_at(flow, "/current_step_id").unwrap_or_else(|| "none".to_owned()),
            json_number_at(flow, "/revision"),
            json_number_at(flow, "/updated_at_unix_ms")
        );
    }
    Ok(())
}

fn emit_flow_envelope(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode flow envelope as JSON");
    }
    let flow = json_value_at(payload, "/flow").unwrap_or(payload);
    println!(
        "{event} id={} state={} mode={} current_step={} revision={} steps={} events={} blockers={}",
        json_optional_string_at(flow, "/flow_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(flow, "/state").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(flow, "/mode").unwrap_or_else(|| "managed".to_owned()),
        json_optional_string_at(flow, "/current_step_id").unwrap_or_else(|| "none".to_owned()),
        json_number_at(flow, "/revision"),
        json_array_at(payload, "/steps").len(),
        json_array_at(payload, "/events").len(),
        json_array_at(payload, "/blockers").len()
    );
    for step in json_array_at(payload, "/steps") {
        println!(
            "flows.step id={} index={} adapter={} state={} title={} attempts={}/{} wait={} error={}",
            json_optional_string_at(step, "/step_id").unwrap_or_else(|| "unknown".to_owned()),
            json_number_at(step, "/step_index"),
            json_optional_string_at(step, "/adapter").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(step, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(step, "/title").unwrap_or_else(|| "untitled".to_owned()),
            json_number_at(step, "/attempt_count"),
            json_number_at(step, "/max_attempts"),
            json_optional_string_at(step, "/waiting_reason").unwrap_or_default(),
            json_optional_string_at(step, "/last_error").unwrap_or_default()
        );
    }
    for blocker in json_array_at(payload, "/blockers") {
        println!(
            "flows.blocker step_id={} state={} reason={}",
            json_optional_string_at(blocker, "/step_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(blocker, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(blocker, "/reason").unwrap_or_default()
        );
    }
    for event in json_array_at(payload, "/events") {
        println!(
            "flows.event id={} type={} step_id={} from={} to={} at_ms={} summary={}",
            json_optional_string_at(event, "/event_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(event, "/event_type").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(event, "/step_id").unwrap_or_else(|| "none".to_owned()),
            json_optional_string_at(event, "/from_state").unwrap_or_else(|| "none".to_owned()),
            json_optional_string_at(event, "/to_state").unwrap_or_else(|| "none".to_owned()),
            json_number_at(event, "/created_at_unix_ms"),
            json_optional_string_at(event, "/summary").unwrap_or_default()
        );
    }
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

fn flow_array(payload: &Value) -> &[Value] {
    payload.pointer("/flows").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

fn json_array_at<'a>(value: &'a Value, pointer: &str) -> &'a [Value] {
    value.pointer(pointer).and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

fn json_number_at(value: &Value, pointer: &str) -> i64 {
    value.pointer(pointer).and_then(Value::as_i64).unwrap_or_default()
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
