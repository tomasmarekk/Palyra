//! Commitment ledger CLI commands over the daemon console API.

use palyra_control_plane as control_plane;
use serde_json::{json, Value};

use crate::cli::CommitmentsCommand;
use crate::commands::routines::{json_optional_string_at, json_value_at};
use crate::*;

/// Runs a `palyra commitments` subcommand on a fresh Tokio runtime.
///
/// # Errors
/// Fails when the runtime cannot be built or the async handler fails.
pub(crate) fn run_commitments(command: CommitmentsCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_commitments_async(command))
}

/// Dispatches a `palyra commitments` subcommand against the admin console API.
///
/// # Errors
/// Fails when the console request or output encoding fails.
pub(crate) async fn run_commitments_async(command: CommitmentsCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        CommitmentsCommand::List { limit, status, due_before_unix_ms, include_terminal, json } => {
            let payload = list_commitments_value(
                &context.client,
                limit,
                status,
                due_before_unix_ms,
                include_terminal,
            )
            .await?;
            emit_commitments_list(&payload, output::preferred_json(json))
        }
        CommitmentsCommand::Show { id, json } => {
            let payload = get_commitment_value(&context.client, id.as_str()).await?;
            emit_commitment_envelope("commitments.show", &payload, output::preferred_json(json))
        }
        CommitmentsCommand::Sources { id, json } => {
            let payload = commitment_sources_value(&context.client, id.as_str()).await?;
            emit_commitment_sources(&payload, output::preferred_json(json))
        }
        CommitmentsCommand::Explain { id, json } => {
            let payload = commitment_explain_value(&context.client, id.as_str()).await?;
            emit_commitment_explain(&payload, output::preferred_json(json))
        }
        CommitmentsCommand::Extract {
            text,
            session_id,
            run_id,
            extraction_model,
            include_inferred,
            json,
        } => {
            let payload = context
                .client
                .post_json_value(
                    "console/v1/commitments/extract".to_owned(),
                    &json!({
                        "source_text": text,
                        "session_id": session_id,
                        "run_id": run_id,
                        "extraction_model": extraction_model,
                        "include_inferred": include_inferred,
                    }),
                )
                .await?;
            emit_commitments_list(&payload, output::preferred_json(json))
        }
        CommitmentsCommand::Approve { id, reason, due_at_unix_ms, json } => {
            let payload = commitment_action_value(
                &context.client,
                id.as_str(),
                "approve",
                reason,
                due_at_unix_ms,
            )
            .await?;
            emit_commitment_envelope("commitments.approve", &payload, output::preferred_json(json))
        }
        CommitmentsCommand::Dismiss { id, reason, json } => {
            let payload =
                commitment_action_value(&context.client, id.as_str(), "dismiss", reason, None)
                    .await?;
            emit_commitment_envelope("commitments.dismiss", &payload, output::preferred_json(json))
        }
        CommitmentsCommand::Snooze { id, reason, due_at_unix_ms, json } => {
            let payload = commitment_action_value(
                &context.client,
                id.as_str(),
                "snooze",
                reason,
                due_at_unix_ms,
            )
            .await?;
            emit_commitment_envelope("commitments.snooze", &payload, output::preferred_json(json))
        }
        CommitmentsCommand::Edit {
            id,
            user_wording,
            normalized_action,
            due_at_unix_ms,
            privacy_label,
            reason,
            json,
        } => {
            let payload = context
                .client
                .post_json_value(
                    format!(
                        "console/v1/commitments/{}/edit",
                        percent_encode_component(id.as_str())
                    ),
                    &json!({
                        "user_wording": user_wording,
                        "normalized_action": normalized_action,
                        "due_at_unix_ms": due_at_unix_ms,
                        "privacy_label": privacy_label,
                        "reason": reason,
                    }),
                )
                .await?;
            emit_commitment_envelope("commitments.edit", &payload, output::preferred_json(json))
        }
        CommitmentsCommand::Schedule { id, reason, due_at_unix_ms, json } => {
            let payload = commitment_action_value(
                &context.client,
                id.as_str(),
                "schedule",
                reason,
                due_at_unix_ms,
            )
            .await?;
            emit_commitment_envelope("commitments.schedule", &payload, output::preferred_json(json))
        }
    }
}

async fn list_commitments_value(
    client: &control_plane::ControlPlaneClient,
    limit: Option<u32>,
    status: Option<String>,
    due_before_unix_ms: Option<i64>,
    include_terminal: bool,
) -> Result<Value> {
    let path = build_query_path(
        "console/v1/commitments",
        vec![
            ("limit", limit.map(|value| value.to_string())),
            ("status", status),
            ("due_before_unix_ms", due_before_unix_ms.map(|value| value.to_string())),
            ("include_terminal", Some(include_terminal.to_string())),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

async fn get_commitment_value(
    client: &control_plane::ControlPlaneClient,
    commitment_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!(
            "console/v1/commitments/{}",
            percent_encode_component(commitment_id)
        ))
        .await
        .map_err(Into::into)
}

async fn commitment_sources_value(
    client: &control_plane::ControlPlaneClient,
    commitment_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!(
            "console/v1/commitments/{}/sources",
            percent_encode_component(commitment_id)
        ))
        .await
        .map_err(Into::into)
}

async fn commitment_explain_value(
    client: &control_plane::ControlPlaneClient,
    commitment_id: &str,
) -> Result<Value> {
    client
        .get_json_value(format!(
            "console/v1/commitments/{}/explain",
            percent_encode_component(commitment_id)
        ))
        .await
        .map_err(Into::into)
}

async fn commitment_action_value(
    client: &control_plane::ControlPlaneClient,
    commitment_id: &str,
    action: &str,
    reason: Option<String>,
    due_at_unix_ms: Option<i64>,
) -> Result<Value> {
    client
        .post_json_value(
            format!(
                "console/v1/commitments/{}/{}",
                percent_encode_component(commitment_id),
                percent_encode_component(action)
            ),
            &json!({ "reason": reason, "due_at_unix_ms": due_at_unix_ms }),
        )
        .await
        .map_err(Into::into)
}

fn emit_commitments_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode commitments list as JSON");
    }
    let commitments = json_array_at(payload, "/commitments");
    println!(
        "commitments.list count={} extracted={}",
        commitments.len(),
        json_number_at(payload, "/extracted_count")
    );
    for commitment in commitments {
        println!(
            "commitments.commitment id={} status={} action={} due_at_ms={} privacy={}",
            json_optional_string_at(commitment, "/commitment_id")
                .unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(commitment, "/status").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(commitment, "/normalized_action").unwrap_or_default(),
            json_number_at(commitment, "/due_at_unix_ms"),
            json_optional_string_at(commitment, "/privacy_label").unwrap_or_default()
        );
    }
    Ok(())
}

fn emit_commitment_envelope(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode commitment as JSON");
    }
    let commitment = json_value_at(payload, "/commitment").unwrap_or(payload);
    println!(
        "{event} id={} status={} action={} due_at_ms={} scheduled_at_ms={}",
        json_optional_string_at(commitment, "/commitment_id")
            .unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(commitment, "/status").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(commitment, "/normalized_action").unwrap_or_default(),
        json_number_at(commitment, "/due_at_unix_ms"),
        json_number_at(commitment, "/scheduled_at_unix_ms")
    );
    Ok(())
}

fn emit_commitment_sources(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode commitment sources as JSON");
    }
    emit_commitment_envelope("commitments.sources", payload, false)?;
    for source in json_array_at(payload, "/sources") {
        println!(
            "commitments.source id={} kind={} run={} start_seq={} end_seq={}",
            json_optional_string_at(source, "/source_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(source, "/source_kind").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(source, "/run_id").unwrap_or_else(|| "none".to_owned()),
            json_number_at(source, "/tape_start_seq"),
            json_number_at(source, "/tape_end_seq")
        );
    }
    Ok(())
}

fn emit_commitment_explain(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode commitment explain as JSON");
    }
    println!(
        "commitments.explain id={} status={} reason={} approval={} privacy={}",
        json_optional_string_at(payload, "/commitment_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(payload, "/status").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(payload, "/review_reason").unwrap_or_default(),
        json_optional_string_at(payload, "/approval_requirement").unwrap_or_default(),
        json_optional_string_at(payload, "/privacy_label").unwrap_or_default()
    );
    Ok(())
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
