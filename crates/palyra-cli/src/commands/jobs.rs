//! Background-job operations over the daemon console API: list, show, tail,
//! lifecycle actions (cancel/drain/resume/retry/attach/release), and
//! maintenance sweeps.
//!
//! All payloads stay as raw JSON values so the CLI tolerates console schema
//! additions without churn.

use palyra_control_plane as control_plane;
use serde_json::{json, Value};

use crate::cli::JobsCommand;
use crate::commands::routines::{json_optional_string_at, json_value_at};
use crate::*;

/// Runs a `palyra jobs` subcommand on a fresh Tokio runtime.
///
/// # Errors
/// Fails when the runtime cannot be built or the async handler fails.
pub(crate) fn run_jobs(command: JobsCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_jobs_async(command))
}

/// Dispatches a `palyra jobs` subcommand against the admin console API.
///
/// # Errors
/// Fails when the console connection, the endpoint call, or output encoding
/// fails.
pub(crate) async fn run_jobs_async(command: JobsCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        JobsCommand::List { limit, session_id, run_id, include_terminal, json } => {
            let payload =
                list_jobs_value(&context.client, limit, session_id, run_id, include_terminal)
                    .await?;
            emit_jobs_list(&payload, output::preferred_json(json))
        }
        JobsCommand::Show { id, json } => {
            let payload = get_job_value(&context.client, id.as_str()).await?;
            emit_job_envelope("jobs.show", &payload, output::preferred_json(json))
        }
        JobsCommand::Tail { id, offset, limit, max_bytes, json } => {
            let payload =
                tail_job_value(&context.client, id.as_str(), offset, limit, max_bytes).await?;
            emit_job_tail(&payload, output::preferred_json(json))
        }
        JobsCommand::Cancel { id, reason, json } => {
            let payload = job_action_value(&context.client, id.as_str(), "cancel", reason).await?;
            emit_job_envelope("jobs.cancel", &payload, output::preferred_json(json))
        }
        JobsCommand::Drain { id, reason, json } => {
            let payload = job_action_value(&context.client, id.as_str(), "drain", reason).await?;
            emit_job_envelope("jobs.drain", &payload, output::preferred_json(json))
        }
        JobsCommand::Resume { id, reason, json } => {
            let payload = job_action_value(&context.client, id.as_str(), "resume", reason).await?;
            emit_job_envelope("jobs.resume", &payload, output::preferred_json(json))
        }
        JobsCommand::Retry { id, idempotency_key, reason, json } => {
            let payload =
                retry_job_value(&context.client, id.as_str(), idempotency_key, reason).await?;
            emit_job_envelope("jobs.retry", &payload, output::preferred_json(json))
        }
        JobsCommand::Attach { id, json } => {
            let payload = attach_job_value(&context.client, id.as_str()).await?;
            emit_job_envelope("jobs.attach", &payload, output::preferred_json(json))
        }
        JobsCommand::Release { id, json } => {
            let payload = release_job_value(&context.client, id.as_str()).await?;
            emit_job_envelope("jobs.release", &payload, output::preferred_json(json))
        }
        JobsCommand::SweepExpired { now_unix_ms, limit, json } => {
            let payload = jobs_maintenance_value(
                &context.client,
                "sweep-expired",
                json!({"now_unix_ms": now_unix_ms, "limit": limit}),
            )
            .await?;
            emit_jobs_list(&payload, output::preferred_json(json))
        }
        JobsCommand::RecoverStale { now_unix_ms, stale_after_ms, limit, json } => {
            let payload = jobs_maintenance_value(
                &context.client,
                "recover-stale",
                json!({
                    "now_unix_ms": now_unix_ms,
                    "stale_after_ms": stale_after_ms,
                    "limit": limit,
                }),
            )
            .await?;
            emit_jobs_list(&payload, output::preferred_json(json))
        }
    }
}

async fn list_jobs_value(
    client: &control_plane::ControlPlaneClient,
    limit: Option<u32>,
    session_id: Option<String>,
    run_id: Option<String>,
    include_terminal: bool,
) -> Result<Value> {
    let path = build_query_path(
        "console/v1/jobs",
        vec![
            ("limit", limit.map(|value| value.to_string())),
            ("session_id", session_id),
            ("run_id", run_id),
            ("include_terminal", Some(include_terminal.to_string())),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

async fn get_job_value(client: &control_plane::ControlPlaneClient, job_id: &str) -> Result<Value> {
    client
        .get_json_value(format!("console/v1/jobs/{}", percent_encode_component(job_id)))
        .await
        .map_err(Into::into)
}

async fn tail_job_value(
    client: &control_plane::ControlPlaneClient,
    job_id: &str,
    offset: Option<i64>,
    limit: Option<u32>,
    max_bytes: Option<u32>,
) -> Result<Value> {
    let path = build_query_path(
        format!("console/v1/jobs/{}/tail", percent_encode_component(job_id)).as_str(),
        vec![
            ("offset", offset.map(|value| value.to_string())),
            ("limit", limit.map(|value| value.to_string())),
            ("max_bytes", max_bytes.map(|value| value.to_string())),
        ],
    );
    client.get_json_value(path).await.map_err(Into::into)
}

async fn job_action_value(
    client: &control_plane::ControlPlaneClient,
    job_id: &str,
    action: &str,
    reason: Option<String>,
) -> Result<Value> {
    client
        .post_json_value(
            format!(
                "console/v1/jobs/{}/{}",
                percent_encode_component(job_id),
                percent_encode_component(action)
            ),
            &json!({ "reason": reason }),
        )
        .await
        .map_err(Into::into)
}

async fn retry_job_value(
    client: &control_plane::ControlPlaneClient,
    job_id: &str,
    idempotency_key: Option<String>,
    reason: Option<String>,
) -> Result<Value> {
    client
        .post_json_value(
            format!("console/v1/jobs/{}/retry", percent_encode_component(job_id)),
            &json!({ "reason": reason, "idempotency_key": idempotency_key }),
        )
        .await
        .map_err(Into::into)
}

async fn attach_job_value(
    client: &control_plane::ControlPlaneClient,
    job_id: &str,
) -> Result<Value> {
    client
        .post_json_value(
            format!("console/v1/jobs/{}/attach", percent_encode_component(job_id)),
            &json!({}),
        )
        .await
        .map_err(Into::into)
}

async fn release_job_value(
    client: &control_plane::ControlPlaneClient,
    job_id: &str,
) -> Result<Value> {
    client
        .post_json_value(
            format!("console/v1/jobs/{}/release", percent_encode_component(job_id)),
            &json!({}),
        )
        .await
        .map_err(Into::into)
}

async fn jobs_maintenance_value(
    client: &control_plane::ControlPlaneClient,
    action: &str,
    payload: Value,
) -> Result<Value> {
    client.post_json_value(format!("console/v1/jobs/{action}"), &payload).await.map_err(Into::into)
}

fn emit_jobs_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode jobs list as JSON");
    }
    let jobs = json_array_at(payload, "/jobs");
    println!("jobs.list count={}", jobs.len());
    for job in jobs {
        println!(
            "jobs.job id={} state={} tool={} backend={} run={} updated_at_ms={} refs={}",
            json_optional_string_at(job, "/job_id").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(job, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(job, "/tool_name").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(job, "/backend").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_at(job, "/run_id").unwrap_or_else(|| "unknown".to_owned()),
            json_number_at(job, "/updated_at_unix_ms"),
            json_number_at(job, "/active_ref_count")
        );
    }
    Ok(())
}

fn emit_job_envelope(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode job envelope as JSON");
    }
    let job = json_value_at(payload, "/job").unwrap_or(payload);
    println!(
        "{event} id={} state={} tool={} backend={} run={} attempts={}/{} updated_at_ms={}",
        json_optional_string_at(job, "/job_id").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(job, "/state").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(job, "/tool_name").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(job, "/backend").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_at(job, "/run_id").unwrap_or_else(|| "unknown".to_owned()),
        json_number_at(job, "/attempt_count"),
        json_number_at(job, "/max_attempts"),
        json_number_at(job, "/updated_at_unix_ms")
    );
    if let Some(preview) = json_optional_string_at(job, "/tail_preview") {
        if !preview.is_empty() {
            println!("jobs.preview {}", preview.replace('\n', "\\n"));
        }
    }
    Ok(())
}

fn emit_job_tail(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode job tail as JSON");
    }
    let tail = json_value_at(payload, "/tail").unwrap_or(payload);
    println!(
        "jobs.tail id={} next_offset={} eof={} truncated={}",
        json_optional_string_at(tail, "/job_id").unwrap_or_else(|| "unknown".to_owned()),
        json_number_at(tail, "/next_offset"),
        tail.pointer("/eof").and_then(Value::as_bool).unwrap_or(false),
        tail.pointer("/truncated").and_then(Value::as_bool).unwrap_or(false)
    );
    for entry in json_array_at(tail, "/entries") {
        println!(
            "jobs.tail.entry seq={} stream={} text={}",
            json_number_at(entry, "/seq"),
            json_optional_string_at(entry, "/stream").unwrap_or_else(|| "event".to_owned()),
            json_optional_string_at(entry, "/chunk_redacted")
                .unwrap_or_default()
                .replace('\n', "\\n")
        );
    }
    Ok(())
}

fn json_array_at<'a>(value: &'a Value, pointer: &str) -> Vec<&'a Value> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_default()
}

fn json_number_at(value: &Value, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(Value::as_i64)
        .map(|number| number.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

fn build_query_path(path: &str, pairs: Vec<(&str, Option<String>)>) -> String {
    let query = pairs
        .into_iter()
        .filter_map(|(key, value)| {
            value.map(|value| format!("{key}={}", percent_encode_component(value.as_str())))
        })
        .collect::<Vec<_>>();
    if query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{}", query.join("&"))
    }
}

fn percent_encode_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}
