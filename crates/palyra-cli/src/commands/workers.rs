//! Networked-worker diagnostics and cleanup actions over the daemon console API.
//!
//! The daemon owns worker state and cleanup auditing; the CLI keeps this as a
//! thin transport surface and prints raw JSON when requested so console schema
//! additions remain backward-compatible.

use palyra_control_plane as control_plane;
use serde_json::{json, Value};

use crate::cli::WorkersCommand;
use crate::*;

/// Runs a `palyra workers` subcommand on a fresh Tokio runtime.
///
/// # Errors
/// Fails when the runtime cannot be built, console connection fails, or the
/// daemon rejects the requested worker action.
pub(crate) fn run_workers(command: WorkersCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_workers_async(command))
}

async fn run_workers_async(command: WorkersCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        WorkersCommand::List { json } => {
            let payload = worker_diagnostics_value(&context.client).await?;
            emit_workers_list(&payload, output::preferred_json(json))
        }
        WorkersCommand::Doctor { json } => {
            let payload = worker_diagnostics_value(&context.client).await?;
            emit_workers_doctor(&payload, output::preferred_json(json))
        }
        WorkersCommand::Leases { json } => {
            let payload = worker_diagnostics_value(&context.client).await?;
            emit_workers_leases(&payload, output::preferred_json(json))
        }
        WorkersCommand::Cleanup {
            worker_id,
            removed_workspace_scope,
            removed_artifacts,
            removed_logs,
            failure_reason,
            confirm,
            json,
        } => {
            if !confirm {
                anyhow::bail!("workers cleanup requires --confirm with explicit cleanup evidence");
            }
            let payload = worker_force_cleanup_value(
                &context.client,
                worker_id.as_str(),
                removed_workspace_scope,
                removed_artifacts,
                removed_logs,
                failure_reason,
            )
            .await?;
            emit_worker_action("workers.cleanup", &payload, output::preferred_json(json))
        }
    }
}

async fn worker_diagnostics_value(client: &control_plane::ControlPlaneClient) -> Result<Value> {
    client
        .get_json_value("console/v1/diagnostics")
        .await
        .map(|payload| payload.pointer("/networked_workers").cloned().unwrap_or(payload))
        .map_err(Into::into)
}

async fn worker_force_cleanup_value(
    client: &control_plane::ControlPlaneClient,
    worker_id: &str,
    removed_workspace_scope: bool,
    removed_artifacts: bool,
    removed_logs: bool,
    failure_reason: Option<String>,
) -> Result<Value> {
    client
        .post_json_value(
            format!(
                "console/v1/networked-workers/{}/force-cleanup",
                percent_encode_component(worker_id)
            ),
            &json!({
                "removed_workspace_scope": removed_workspace_scope,
                "removed_artifacts": removed_artifacts,
                "removed_logs": removed_logs,
                "failure_reason": failure_reason,
            }),
        )
        .await
        .map_err(Into::into)
}

fn emit_workers_list(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode workers list as JSON");
    }
    let snapshot = payload.pointer("/snapshot").unwrap_or(payload);
    println!(
        "workers.list state={} registered={} attested={} active_leases={} orphaned={} failed_closed={}",
        json_string_at(payload, "/state"),
        json_number_at(snapshot, "/registered_workers"),
        json_number_at(snapshot, "/attested_workers"),
        json_number_at(snapshot, "/active_leases"),
        json_number_at(snapshot, "/orphaned_workers"),
        json_number_at(snapshot, "/failed_closed_workers")
    );
    Ok(())
}

fn emit_workers_doctor(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode workers doctor as JSON");
    }
    println!(
        "workers.doctor state={} mode={} lease_failures={} transport_failures={} fallback_bps={}",
        json_string_at(payload, "/state"),
        json_string_at(payload, "/mode"),
        json_number_at(payload, "/metrics/lease_failures"),
        json_number_at(payload, "/metrics/transport_failures"),
        json_number_at(payload, "/metrics/fallback_to_local_rate_bps")
    );
    for action in json_array_at(payload, "/recovery/recommended_actions") {
        if let Some(action) = action.as_str() {
            println!("workers.recommendation {action}");
        }
    }
    Ok(())
}

fn emit_workers_leases(payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode workers leases as JSON");
    }
    println!(
        "workers.leases active={} ttl_ms={} orphaned={} cleanup_pending={}",
        json_number_at(payload, "/snapshot/active_leases"),
        json_number_at(payload, "/policy/lease_ttl_ms"),
        json_number_at(payload, "/snapshot/orphaned_workers"),
        json_number_at(payload, "/snapshot/failed_closed_workers")
    );
    Ok(())
}

fn emit_worker_action(event: &str, payload: &Value, json: bool) -> Result<()> {
    if json {
        return output::print_json_pretty(payload, "failed to encode worker action as JSON");
    }
    println!(
        "{event} action={} events={} state={}",
        json_string_at(payload, "/action"),
        json_number_at(payload, "/event_count"),
        json_string_at(payload, "/diagnostics/state")
    );
    Ok(())
}

fn json_string_at(value: &Value, pointer: &str) -> String {
    value.pointer(pointer).and_then(Value::as_str).unwrap_or("unknown").to_owned()
}

fn json_number_at(value: &Value, pointer: &str) -> u64 {
    value.pointer(pointer).and_then(Value::as_u64).unwrap_or_default()
}

fn json_array_at<'a>(value: &'a Value, pointer: &str) -> &'a [Value] {
    value.pointer(pointer).and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

fn percent_encode_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}
