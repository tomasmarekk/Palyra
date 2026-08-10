//! `palyra run`: run trajectory export for audit and eval workflows.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use crate::{commands::support_bundle, *};
use palyra_common::replay_bundle::{
    canonical_replay_bundle_bytes, replay_bundle_offline, ReplayBundle, ReplayRunStatus,
    ReplayToolExchange,
};
use serde_json::{json, Value};

const RUN_TRAJECTORY_JSONL_FORMAT: &str = "palyra-run-trajectory-jsonl";
const LARGE_TRAJECTORY_TOOL_OUTPUT_BYTES: usize = 1024;

/// Runs a `palyra run` subcommand.
///
/// # Errors
/// Returns an error when the journal cannot be read, the replay bundle fails
/// deterministic validation, or the requested artifact cannot be written.
pub(crate) fn run_run(command: RunCommand) -> Result<()> {
    match command {
        RunCommand::Wait { run_id, timeout_ms, return_on_waiting, json } => {
            run_wait(run_id, timeout_ms, return_on_waiting, json)
        }
        RunCommand::Control {
            run_id,
            command,
            active_phase,
            instruction,
            queued_input_id,
            priority_lane,
            reason,
            dry_run,
            json,
        } => run_control(
            run_id,
            command,
            active_phase,
            instruction,
            queued_input_id,
            priority_lane,
            reason,
            dry_run,
            json,
        ),
        RunCommand::Export {
            run_id,
            output,
            format,
            redacted,
            journal_db,
            max_events,
            trajectory,
        } => run_export(run_id, output, format, redacted, journal_db, max_events, trajectory),
        RunCommand::Replay { input, golden, diff_output, json } => {
            run_replay(input, golden, diff_output, json)
        }
    }
}

fn run_wait(
    run_id: String,
    timeout_ms: u64,
    return_on_waiting: bool,
    json_output: bool,
) -> Result<()> {
    validate_canonical_id(run_id.as_str())
        .context("run_id must be a canonical ULID for run wait")?;
    let timeout_ms = timeout_ms.clamp(1, 120_000);
    let connection = run_root_context()?.resolve_http_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::ADMIN,
    )?;
    let endpoint = format!(
        "{}/admin/v1/runs/{}/wait",
        connection.base_url.trim_end_matches('/'),
        percent_encode_component(run_id.as_str())
    );
    let request_timeout = Duration::from_millis(timeout_ms.saturating_add(5_000));
    let client = reqwest::blocking::Client::builder()
        .timeout(request_timeout)
        .build()
        .context("failed to build run wait HTTP client")?;
    let response: Value = apply_run_http_connection_headers(client.post(endpoint), &connection)
        .json(&json!({
            "timeout_ms": timeout_ms,
            "return_on_waiting": return_on_waiting,
        }))
        .send()
        .context("failed to call daemon run wait endpoint")?
        .error_for_status()
        .context("daemon run wait endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon run wait payload")?;
    if output::preferred_json(json_output) {
        output::print_json_pretty(&response, "failed to encode run wait output as JSON")?;
    } else {
        let status = response.get("status").and_then(Value::as_str).unwrap_or("unknown");
        let state = response.pointer("/run/state").and_then(Value::as_str).unwrap_or("unknown");
        let session_id =
            response.pointer("/run/session_id").and_then(Value::as_str).unwrap_or("none");
        println!(
            "run.wait run_id={} status={} state={} timed_out={} timeout_ms={} session_id={}",
            response.get("run_id").and_then(Value::as_str).unwrap_or(run_id.as_str()),
            status,
            state,
            response.get("timed_out").and_then(Value::as_bool).unwrap_or(false),
            response.get("timeout_ms").and_then(Value::as_u64).unwrap_or(timeout_ms),
            session_id
        );
        std::io::stdout().flush().context("stdout flush failed")?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_control(
    run_id: String,
    command: RunControlCommandArg,
    active_phase: Option<RunControlActivePhaseArg>,
    instruction: Option<String>,
    queued_input_id: Option<String>,
    priority_lane: Option<String>,
    reason: Option<String>,
    dry_run: bool,
    json_output: bool,
) -> Result<()> {
    validate_canonical_id(run_id.as_str())
        .context("run_id must be a canonical ULID for run control")?;
    if command == RunControlCommandArg::Redirect
        && instruction.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none()
    {
        anyhow::bail!("run control --command redirect requires --instruction");
    }
    if command == RunControlCommandArg::Steer {
        if queued_input_id.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none() {
            anyhow::bail!("run control --command steer requires --queued-input-id");
        }
        if priority_lane.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none() {
            anyhow::bail!("run control --command steer requires --priority-lane");
        }
    }
    let connection = run_root_context()?.resolve_http_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::ADMIN,
    )?;
    let endpoint = format!(
        "{}/admin/v1/runs/{}/control",
        connection.base_url.trim_end_matches('/'),
        percent_encode_component(run_id.as_str())
    );
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build run control HTTP client")?;
    let response: Value = apply_run_http_connection_headers(client.post(endpoint), &connection)
        .json(&json!({
            "command": command.as_str(),
            "active_phase": active_phase.map(RunControlActivePhaseArg::as_str),
            "instruction": instruction,
            "queued_input_id": queued_input_id,
            "priority_lane": priority_lane,
            "reason": reason,
            "dry_run": dry_run,
        }))
        .send()
        .context("failed to call daemon run control endpoint")?
        .error_for_status()
        .context("daemon run control endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon run control payload")?;
    if output::preferred_json(json_output) {
        output::print_json_pretty(&response, "failed to encode run control output as JSON")?;
    } else {
        let accepted =
            response.pointer("/turn_control/accepted").and_then(Value::as_bool).unwrap_or(false);
        let action =
            response.pointer("/turn_control/action").and_then(Value::as_str).unwrap_or("unknown");
        let reason_code = response
            .pointer("/turn_control/reason_code")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        println!(
            "run.control run_id={} command={} accepted={} action={} reason_code={}",
            response.get("run_id").and_then(Value::as_str).unwrap_or(run_id.as_str()),
            command.as_str(),
            accepted,
            action,
            reason_code
        );
        std::io::stdout().flush().context("stdout flush failed")?;
    }
    Ok(())
}

fn run_export(
    run_id: String,
    output: String,
    format: RunExportFormatArg,
    redacted: bool,
    journal_db: Option<String>,
    max_events: usize,
    trajectory: bool,
) -> Result<()> {
    if !redacted {
        anyhow::bail!(
            "non-redacted run exports are not supported by the local CLI; rerun with --redacted true"
        );
    }
    if max_events == 0 || max_events > 4_096 {
        anyhow::bail!("run export --max-events must be in range 1..=4096");
    }

    let bundle =
        support_bundle::build_replay_bundle_from_journal(run_id.as_str(), journal_db, max_events)?;
    let output_path = PathBuf::from(output);
    if trajectory {
        let (bytes, manifest_hash) = build_run_trajectory_jsonl(&bundle, format, redacted)?;
        support_bundle::write_replay_artifact(output_path.as_path(), bytes.as_slice())?;
        println!(
            "run.export path={} run_id={} format={} trajectory=true bytes={} manifest_hash_sha256={}",
            output_path.display(),
            run_id,
            RUN_TRAJECTORY_JSONL_FORMAT,
            bytes.len(),
            manifest_hash
        );
    } else {
        let payload = build_run_export_payload(&bundle, format, redacted)?;
        let bytes = serde_json::to_vec_pretty(&payload).context("failed to encode run export")?;
        support_bundle::write_replay_artifact(output_path.as_path(), bytes.as_slice())?;
        println!(
            "run.export path={} run_id={} format={} bytes={} canonical_sha256={}",
            output_path.display(),
            run_id,
            format.as_str(),
            bytes.len(),
            payload
                .get("manifest")
                .and_then(|manifest| manifest.get("replay_bundle_sha256"))
                .and_then(Value::as_str)
                .unwrap_or("<missing>")
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_root_context() -> Result<app::RootCommandContext> {
    app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for run command"))
}

fn apply_run_http_connection_headers(
    request: reqwest::blocking::RequestBuilder,
    connection: &app::HttpConnection,
) -> reqwest::blocking::RequestBuilder {
    let mut request = request
        .header("x-palyra-principal", connection.principal.clone())
        .header("x-palyra-device-id", connection.device_id.clone())
        .header("x-palyra-channel", connection.channel.clone())
        .header("x-palyra-trace-id", connection.trace_id.clone());
    if let Some(token) = connection.token.as_ref() {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    request
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

fn run_replay(
    input: String,
    golden: Option<String>,
    diff_output: Option<String>,
    json: bool,
) -> Result<()> {
    let input_path = PathBuf::from(input);
    let bytes = fs::read(input_path.as_path())
        .with_context(|| format!("failed to read trajectory {}", input_path.display()))?;
    let document = parse_run_trajectory_jsonl(bytes.as_slice())?;
    let golden = golden
        .map(|path| {
            let path = PathBuf::from(path);
            let bytes = fs::read(path.as_path())
                .with_context(|| format!("failed to read replay golden {}", path.display()))?;
            let value = serde_json::from_slice::<Value>(bytes.as_slice())
                .with_context(|| format!("failed to parse replay golden {}", path.display()))?;
            Ok::<_, anyhow::Error>(value)
        })
        .transpose()?;
    let report = replay_trajectory_document(&document, golden.as_ref())?;
    if let Some(output) = diff_output {
        let output_path = PathBuf::from(output);
        let markdown = render_replay_diff_markdown(&report);
        support_bundle::write_replay_artifact(output_path.as_path(), markdown.as_bytes())?;
    }
    if output::preferred_json(json) {
        output::print_json_pretty(&report, "failed to encode run replay report as JSON")?;
    } else {
        println!(
            "run.replay status={} events={} tool_proposals={} diffs={} unsafe_mutations={}",
            report.status,
            report.summary.event_count,
            report.summary.tool_proposals.len(),
            report.diffs.len(),
            report.unsafe_mutations.len()
        );
        std::io::stdout().flush().context("stdout flush failed")?;
    }
    if report.status != "passed" {
        anyhow::bail!("run replay failed with {} diff(s)", report.diffs.len());
    }
    Ok(())
}

fn build_run_trajectory_jsonl(
    bundle: &ReplayBundle,
    format: RunExportFormatArg,
    redacted: bool,
) -> Result<(Vec<u8>, String)> {
    let export_manifest = build_run_export_manifest(bundle, format, redacted)?;
    let mut artifact_index = bundle
        .artifact_refs
        .iter()
        .map(|artifact| {
            json!({
                "artifact_id": artifact.artifact_id,
                "kind": artifact.kind,
                "reference": artifact.reference,
                "sha256": artifact.sha256,
                "size_bytes": artifact.size_bytes,
            })
        })
        .collect::<Vec<_>>();
    let events = build_run_trajectory_events(bundle, &mut artifact_index)?;
    let mut manifest_line = json!({
        "schema_version": 1,
        "line_type": "manifest",
        "format": RUN_TRAJECTORY_JSONL_FORMAT,
        "requested_format": format.as_str(),
        "redacted": redacted,
        "run_id": bundle.source.run_id,
        "session_id": bundle.source.session_id,
        "manifest": export_manifest,
        "event_count": events.len(),
        "artifact_index": artifact_index,
        "large_output_projection": {
            "threshold_bytes": LARGE_TRAJECTORY_TOOL_OUTPUT_BYTES,
            "mode": "artifact_ref"
        },
        "event_classes": [
            "user_message",
            "compiled_instructions",
            "provider_selection",
            "provider_stream",
            "tool_proposal",
            "approval",
            "tool_output",
            "memory_recall",
            "compaction_boundary",
            "subagent",
            "mcp_import",
            "policy_decision",
            "verification_summary",
            "final_answer",
            "usage",
            "artifact"
        ],
    });
    let manifest_hash = sha256_json_value(&manifest_line)?;
    if let Some(object) = manifest_line.as_object_mut() {
        object.insert("manifest_hash_sha256".to_owned(), Value::String(manifest_hash.clone()));
    }

    let mut lines = Vec::with_capacity(events.len().saturating_add(1));
    lines.push(
        serde_json::to_string(&manifest_line).context("failed to encode trajectory manifest")?,
    );
    for event in events {
        lines.push(serde_json::to_string(&event).context("failed to encode trajectory event")?);
    }
    let mut bytes = lines.join("\n").into_bytes();
    bytes.push(b'\n');
    Ok((bytes, manifest_hash))
}

fn build_run_trajectory_events(
    bundle: &ReplayBundle,
    artifact_index: &mut Vec<Value>,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut seq = 0_i64;
    push_trajectory_row(
        &mut rows,
        &mut seq,
        "user_message",
        "user_message",
        json!({
            "input": bundle.run.normalized_user_input,
            "input_hash_sha256": bundle
                .run
                .normalized_user_input
                .as_ref()
                .map(sha256_json_value)
                .transpose()?,
        }),
    );
    push_trajectory_row(
        &mut rows,
        &mut seq,
        "compiled_instructions",
        "compiled_instructions",
        json!({
            "instruction_hash_sha256": bundle
                .run
                .normalized_user_input
                .as_ref()
                .map(sha256_json_value)
                .transpose()?,
            "context_manifest_hash_sha256": sha256_json_value(&bundle.config_snapshot)?,
        }),
    );
    for exchange in &bundle.model_exchanges {
        push_trajectory_row(
            &mut rows,
            &mut seq,
            "provider_selection",
            "provider_selection",
            json!({
                "exchange_id": exchange.exchange_id,
                "provider": exchange.provider,
                "model": exchange.model,
                "provider_request_hash_sha256": sha256_json_value(&exchange.request_metadata)?,
                "provider_response_hash_sha256": sha256_json_value(&exchange.response)?,
            }),
        );
    }
    for event in &bundle.tape_events {
        push_trajectory_row(
            &mut rows,
            &mut seq,
            event.event_type.as_str(),
            trajectory_event_category(event.event_type.as_str()),
            json!({
                "tape_seq": event.seq,
                "payload": project_tape_payload(event.event_type.as_str(), &event.payload),
                "payload_hash_sha256": sha256_json_value(&event.payload)?,
            }),
        );
    }
    for exchange in &bundle.tool_exchanges {
        push_tool_exchange_rows(&mut rows, &mut seq, exchange, artifact_index)?;
    }
    for approval in &bundle.approvals {
        push_trajectory_row(
            &mut rows,
            &mut seq,
            "approval",
            "approval",
            json!({
                "approval_id": approval.approval_id,
                "proposal_id": approval.proposal_id,
                "request": approval.request,
                "response": approval.response,
            }),
        );
    }
    push_trajectory_row(
        &mut rows,
        &mut seq,
        "verification_summary",
        "verification_summary",
        build_run_verification_summary(bundle),
    );
    push_trajectory_row(
        &mut rows,
        &mut seq,
        "policy_decisions",
        "policy_decision",
        json!({
            "approvals": bundle.approvals,
            "queue_decisions": bundle.queue_decisions,
            "auxiliary_tasks": bundle.auxiliary_tasks,
            "flow_events": bundle.flow_events,
        }),
    );
    push_trajectory_row(
        &mut rows,
        &mut seq,
        "final_answer",
        "final_answer",
        json!({
            "summary": bundle.expected.final_answer_summary,
            "sha256": bundle.expected.final_answer_sha256,
        }),
    );
    push_trajectory_row(
        &mut rows,
        &mut seq,
        "usage",
        "usage",
        json!({
            "prompt_tokens": bundle.run.prompt_tokens,
            "completion_tokens": bundle.run.completion_tokens,
            "total_tokens": bundle.run.total_tokens,
        }),
    );
    for artifact in artifact_index.iter() {
        push_trajectory_row(&mut rows, &mut seq, "artifact_ref", "artifact", artifact.clone());
    }
    Ok(rows)
}

fn push_tool_exchange_rows(
    rows: &mut Vec<Value>,
    seq: &mut i64,
    exchange: &ReplayToolExchange,
    artifact_index: &mut Vec<Value>,
) -> Result<()> {
    let replay_safety_class =
        replay_safety_class_for_tool_input(exchange.tool_name.as_str(), &exchange.input);
    push_trajectory_row(
        rows,
        seq,
        "tool_proposal",
        "tool_proposal",
        json!({
            "proposal_id": exchange.proposal_id,
            "tool_name": exchange.tool_name,
            "input": exchange.input,
            "input_hash_sha256": sha256_json_value(&exchange.input)?,
            "replay_safety_class": replay_safety_class,
        }),
    );
    if let Some(decision) = exchange.decision.as_ref() {
        push_trajectory_row(
            rows,
            seq,
            "tool_decision",
            "approval",
            json!({
                "proposal_id": exchange.proposal_id,
                "tool_name": exchange.tool_name,
                "decision": decision,
            }),
        );
    }
    if let Some(result) = exchange.result.as_ref() {
        push_trajectory_row(
            rows,
            seq,
            "tool_output",
            "tool_output",
            json!({
                "proposal_id": exchange.proposal_id,
                "tool_name": exchange.tool_name,
                "result": project_tool_result_for_trajectory(exchange, result, artifact_index)?,
            }),
        );
    }
    if let Some(attestation) = exchange.attestation.as_ref() {
        push_trajectory_row(
            rows,
            seq,
            "tool_attestation",
            "tool_output",
            json!({
                "proposal_id": exchange.proposal_id,
                "tool_name": exchange.tool_name,
                "attestation": attestation,
            }),
        );
    }
    Ok(())
}

fn project_tool_result_for_trajectory(
    exchange: &ReplayToolExchange,
    result: &Value,
    artifact_index: &mut Vec<Value>,
) -> Result<Value> {
    let bytes =
        serde_json::to_vec(result).context("failed to encode tool result for trajectory")?;
    if bytes.len() <= LARGE_TRAJECTORY_TOOL_OUTPUT_BYTES {
        return Ok(result.clone());
    }
    let artifact = json!({
        "artifact_id": format!("trajectory.tool_output.{}", exchange.proposal_id),
        "kind": "large_tool_output",
        "reference": format!("trajectory://tool-output/{}", exchange.proposal_id),
        "sha256": crate::sha256_hex(bytes.as_slice()),
        "size_bytes": bytes.len(),
    });
    artifact_index.push(artifact.clone());
    Ok(json!({
        "success": result.get("success").and_then(Value::as_bool),
        "error": result.get("error").cloned().unwrap_or(Value::Null),
        "output_artifact_ref": artifact,
    }))
}

fn project_tape_payload(event_type: &str, payload: &Value) -> Value {
    if matches!(event_type, "model_token" | "provider.stream.delta") {
        return json!({
            "token_sha256": payload
                .get("token")
                .and_then(Value::as_str)
                .map(|token| crate::sha256_hex(token.as_bytes())),
            "is_final": payload.get("is_final").and_then(Value::as_bool),
        });
    }
    if event_type == "tool_result" {
        if let Ok(bytes) = serde_json::to_vec(payload) {
            if bytes.len() > LARGE_TRAJECTORY_TOOL_OUTPUT_BYTES {
                return json!({
                    "large_payload_redacted": true,
                    "payload_sha256": crate::sha256_hex(bytes.as_slice()),
                    "size_bytes": bytes.len(),
                });
            }
        }
    }
    if event_type.contains("mcp") {
        return project_mcp_public_payload(payload);
    }
    payload.clone()
}

fn build_run_verification_summary(bundle: &ReplayBundle) -> Value {
    for event in &bundle.tape_events {
        if event.event_type == "verification_summary" && event.payload.is_object() {
            return event.payload.clone();
        }
        if let Some(summary) =
            event.payload.get("verification_summary").filter(|value| value.is_object())
        {
            return summary.clone();
        }
    }

    let mut changed_files = Vec::new();
    let mut commands_executed = Vec::new();
    let mut command_classification = Vec::new();
    let mut unverified_mutations = Vec::new();
    let mut stale_evidence_reasons = Vec::new();
    let mut diagnostics = Vec::new();
    let mut evidence_refs = Vec::new();
    let mut reason_codes = Vec::new();
    let mut finalizer = Value::Null;

    for event in &bundle.tape_events {
        let payload = &event.payload;
        if let Some(projection_type) = payload.get("event_type").and_then(Value::as_str) {
            reason_codes.extend(json_string_array(payload.get("reason_codes")));
            evidence_refs.extend(json_string_array(payload.get("evidence_refs")));
            if projection_type == "verification.command.classified" {
                if let Some(classification) = payload.get("classification") {
                    command_classification.push(public_verification_classification(
                        classification,
                        payload.get("created_at_unix_ms").and_then(Value::as_i64).unwrap_or(0),
                    ));
                    if !classification
                        .get("is_verification")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                    {
                        commands_executed.push(public_verification_command_from_classification(
                            classification,
                            payload.get("created_at_unix_ms").and_then(Value::as_i64).unwrap_or(0),
                            payload.get("evidence_refs"),
                        ));
                    }
                }
            }
            if projection_type == "verification.event.recorded" {
                if let Some(verification_event) = payload.get("event") {
                    changed_files.extend(
                        verification_event
                            .get("changed_paths")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .map(public_verification_path),
                    );
                    commands_executed
                        .push(public_verification_command_from_event(verification_event));
                }
            }
            if projection_type == "verification.state.stale" {
                if let Some(state) = payload.get("state") {
                    changed_files.extend(
                        state
                            .pointer("/requirement/changed_paths")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .map(public_verification_path),
                    );
                    let mutation = public_unverified_mutation_from_state(state);
                    stale_evidence_reasons
                        .extend(json_string_array(state.pointer("/freshness/reason_codes")));
                    if let Some(reason) =
                        state.pointer("/requirement/reason_code").and_then(Value::as_str)
                    {
                        stale_evidence_reasons.push(reason.to_owned());
                    }
                    unverified_mutations.push(mutation);
                }
            }
            if projection_type == "verification.freshness.checked"
                && payload
                    .pointer("/freshness/status")
                    .and_then(Value::as_str)
                    .is_some_and(|status| status != "fresh")
            {
                stale_evidence_reasons
                    .extend(json_string_array(payload.pointer("/freshness/reason_codes")));
            }
        }

        if let Some(event_name) = payload.get("event").and_then(Value::as_str) {
            if event_name.contains("diagnostics.delta") {
                let diagnostic = json!({
                    "event_type": event_name,
                    "new_errors": payload.get("new_errors").and_then(Value::as_u64).unwrap_or(0),
                    "new_warnings": payload.get("new_warnings").and_then(Value::as_u64).unwrap_or(0),
                    "degraded": payload.get("degraded").and_then(Value::as_bool).unwrap_or(false),
                    "reason_codes": json_string_array(payload.get("reason_codes")),
                    "evidence_refs": json_string_array(payload.get("evidence_refs")),
                });
                if diagnostic.get("degraded").and_then(Value::as_bool).unwrap_or(false)
                    || diagnostic.get("new_errors").and_then(Value::as_u64).unwrap_or(0) > 0
                {
                    stale_evidence_reasons
                        .extend(json_string_array(diagnostic.get("reason_codes")));
                }
                evidence_refs.extend(json_string_array(diagnostic.get("evidence_refs")));
                reason_codes.extend(json_string_array(diagnostic.get("reason_codes")));
                diagnostics.push(diagnostic);
            }
        }

        if let Some(value) = payload.pointer("/finalization/verification_finalizer") {
            finalizer = value.clone();
        }
    }

    let final_answer = public_final_answer_verification(&finalizer);
    evidence_refs.extend(json_string_array(final_answer.get("evidence_refs")));
    if let Some(reason) = final_answer.get("reason_code").and_then(Value::as_str) {
        reason_codes.push(reason.to_owned());
        if !final_answer.get("allowed").and_then(Value::as_bool).unwrap_or(false) {
            stale_evidence_reasons.push(reason.to_owned());
        }
    }
    let latest_status = latest_verification_status(
        commands_executed.as_slice(),
        unverified_mutations.as_slice(),
        command_classification.len(),
    );
    let final_answer_allowed =
        final_answer.get("allowed").and_then(Value::as_bool).unwrap_or(false);
    let final_answer_allowed_because = final_answer
        .get("allowed_because")
        .and_then(Value::as_str)
        .unwrap_or("verification.finalizer.not_observed")
        .to_owned();
    let state = if commands_executed.is_empty()
        && command_classification.is_empty()
        && unverified_mutations.is_empty()
        && diagnostics.is_empty()
        && !final_answer.get("observed").and_then(Value::as_bool).unwrap_or(false)
    {
        "not_available"
    } else {
        "available"
    };

    json!({
        "schema_version": 1,
        "state": state,
        "rollout_enabled": true,
        "changed_files": normalize_strings(changed_files),
        "commands_executed": commands_executed,
        "command_classification": command_classification,
        "latest_verification_status": latest_status,
        "unverified_mutations": unverified_mutations,
        "stale_evidence_reasons": normalize_strings(stale_evidence_reasons),
        "diagnostics": diagnostics,
        "final_answer": final_answer,
        "final_answer_allowed": final_answer_allowed,
        "final_answer_allowed_because": final_answer_allowed_because,
        "evidence_refs": normalize_strings(evidence_refs),
        "reason_codes": normalize_strings(reason_codes),
        "redaction_level": "metadata_and_redacted_summary",
    })
}

fn public_verification_classification(classification: &Value, created_at_unix_ms: i64) -> Value {
    json!({
        "command": classification
            .pointer("/canonical_command/display")
            .and_then(Value::as_str)
            .unwrap_or(""),
        "is_verification": classification
            .get("is_verification")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "kind": classification.get("kind").and_then(Value::as_str).unwrap_or("unknown"),
        "scope": classification.get("scope").and_then(Value::as_str).unwrap_or("unknown"),
        "created_at_unix_ms": created_at_unix_ms,
        "reason_codes": json_string_array(classification.get("reason_codes")),
    })
}

fn public_verification_command_from_classification(
    classification: &Value,
    created_at_unix_ms: i64,
    evidence_refs: Option<&Value>,
) -> Value {
    json!({
        "command": classification
            .pointer("/canonical_command/display")
            .and_then(Value::as_str)
            .unwrap_or(""),
        "is_verification": false,
        "kind": classification.get("kind").and_then(Value::as_str).unwrap_or("unknown"),
        "scope": classification.get("scope").and_then(Value::as_str).unwrap_or("unknown"),
        "status": Value::Null,
        "exit_code": Value::Null,
        "created_at_unix_ms": created_at_unix_ms,
        "evidence_refs": json_string_array(evidence_refs),
        "reason_codes": json_string_array(classification.get("reason_codes")),
    })
}

fn public_verification_command_from_event(event: &Value) -> Value {
    json!({
        "command": event
            .get("canonical_command")
            .and_then(Value::as_str)
            .or_else(|| event.get("command").and_then(Value::as_str))
            .unwrap_or(""),
        "is_verification": true,
        "kind": event.get("kind").and_then(Value::as_str).unwrap_or("unknown"),
        "scope": event.get("scope").and_then(Value::as_str).unwrap_or("unknown"),
        "status": event.get("status").and_then(Value::as_str),
        "exit_code": event.get("exit_code").and_then(Value::as_i64),
        "created_at_unix_ms": event.get("created_at_unix_ms").and_then(Value::as_i64).unwrap_or(0),
        "evidence_refs": json_string_array(event.get("evidence_refs")),
        "reason_codes": json_string_array(event.get("reason_codes")),
    })
}

fn public_unverified_mutation_from_state(state: &Value) -> Value {
    json!({
        "requirement_id": state
            .pointer("/requirement/requirement_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        "required_kind": state
            .pointer("/requirement/required_kind")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        "changed_files": normalize_strings(
            state
                .pointer("/requirement/changed_paths")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(public_verification_path)
                .collect(),
        ),
        "freshness_status": state
            .pointer("/freshness/status")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        "min_created_at_unix_ms": state
            .pointer("/requirement/min_created_at_unix_ms")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        "reason_codes": normalize_strings(
            json_string_array(state.pointer("/freshness/reason_codes"))
                .into_iter()
                .chain(
                    state
                        .pointer("/requirement/reason_code")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                )
                .collect(),
        ),
    })
}

fn public_final_answer_verification(finalizer: &Value) -> Value {
    let status = finalizer.get("status").and_then(Value::as_str);
    let reason_code = finalizer.get("reason_code").and_then(Value::as_str);
    let unverified_reason = finalizer.get("unverified_reason").and_then(Value::as_str);
    let allowed = matches!(status, Some("not_required" | "verified" | "unverified_allowed"));
    let allowed_because = match status {
        Some("verified") => "verification.finalizer.fresh_verification_found".to_owned(),
        Some("unverified_allowed") => unverified_reason
            .map(|reason| format!("verification.finalizer.unverified_allowed:{reason}"))
            .unwrap_or_else(|| "verification.finalizer.unverified_allowed".to_owned()),
        Some("not_required") => {
            reason_code.unwrap_or("verification.finalizer.not_required").to_owned()
        }
        Some("nudge_required") => {
            reason_code.unwrap_or("verification.finalizer.nudge_required").to_owned()
        }
        Some(_) => reason_code.unwrap_or("verification.finalizer.unknown").to_owned(),
        None => "verification.finalizer.not_observed".to_owned(),
    };
    json!({
        "observed": finalizer.is_object(),
        "status": status,
        "reason_code": reason_code,
        "allowed": allowed,
        "allowed_because": allowed_because,
        "pending_requirement_count": finalizer.get("pending_requirement_count").and_then(Value::as_u64),
        "satisfied_requirement_count": finalizer.get("satisfied_requirement_count").and_then(Value::as_u64),
        "evidence_refs": json_string_array(finalizer.get("evidence_refs")),
        "nudge": finalizer.get("nudge").and_then(Value::as_str),
        "unverified_reason": unverified_reason,
    })
}

fn latest_verification_status(
    commands_executed: &[Value],
    unverified_mutations: &[Value],
    classified_commands: usize,
) -> Value {
    let failed_events = commands_executed
        .iter()
        .filter(|command| {
            command
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| matches!(status, "failed" | "timed_out" | "cancelled"))
        })
        .count();
    let passing_events = commands_executed
        .iter()
        .filter(|command| command.get("status").and_then(Value::as_str) == Some("passed"))
        .count();
    let stale_requirements = unverified_mutations.len();
    let decision = if stale_requirements > 0 {
        "stale"
    } else if failed_events > 0 {
        "failed"
    } else if passing_events > 0 {
        "fresh"
    } else if classified_commands == 0 {
        "no_evidence"
    } else {
        "unknown"
    };
    json!({
        "schema_version": 1,
        "decision": decision,
        "rollout_enabled": true,
        "journal_total_events": Value::Null,
        "journal_window_events": Value::Null,
        "verification_projection_events": Value::Null,
        "classified_commands": classified_commands,
        "recorded_events": commands_executed
            .iter()
            .filter(|command| command.get("is_verification").and_then(Value::as_bool).unwrap_or(false))
            .count(),
        "passing_events": passing_events,
        "failed_events": failed_events,
        "stale_requirements": stale_requirements,
        "fresh_requirements": 0,
        "unknown_requirements": 0,
        "latest_event_type": Value::Null,
        "latest_status": commands_executed
            .iter()
            .rev()
            .find_map(|command| command.get("status").and_then(Value::as_str)),
        "latest_created_at_unix_ms": commands_executed
            .iter()
            .filter_map(|command| command.get("created_at_unix_ms").and_then(Value::as_i64))
            .max(),
        "reason_codes": [format!("verification.status.replay_export.{decision}")],
        "journal_events": [
            "verification.command.classified",
            "verification.event.recorded",
            "verification.state.stale",
            "verification.freshness.checked",
        ],
        "redaction_level": "metadata_and_redacted_summary",
    })
}

fn public_verification_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return ".".to_owned();
    }
    let path = trimmed.replace('\\', "/");
    let lower = path.to_ascii_lowercase();
    let has_drive_prefix = path.as_bytes().get(1).is_some_and(|byte| *byte == b':');
    let looks_absolute = path.starts_with('/') || has_drive_prefix || path.starts_with("~/");
    let contains_private_home =
        lower.contains("/users/") || lower.contains("/home/") || lower.contains("/desktop/");
    if looks_absolute || contains_private_home || path.contains("..") {
        "<redacted:path>".to_owned()
    } else {
        path.trim_start_matches("./").to_owned()
    }
}

fn json_string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).map(str::to_owned).collect())
        .unwrap_or_default()
}

fn normalize_strings(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn trajectory_event_category(event_type: &str) -> &'static str {
    match event_type {
        "verification_summary" => "verification_summary",
        "model_token" | "provider.stream.delta" | "provider.stream.completed" => "provider_stream",
        "tool_proposal" => "tool_proposal",
        "tool_result" | "tool_attestation" => "tool_output",
        "tool_decision" | "approval_request" | "approval_response" => "approval",
        value if value.contains("memory") || value.contains("recall") => "memory_recall",
        value if value.contains("compact") => "compaction_boundary",
        value if value.contains("subagent") || value.contains("delegation") => "subagent",
        value if value.contains("mcp") => "mcp_import",
        value if value.contains("verification") => "verification_summary",
        value if value.contains("policy") || value.contains("decision") => "policy_decision",
        "final_answer" | "run_completed" => "final_answer",
        _ => "provider_stream",
    }
}

fn push_trajectory_row(
    rows: &mut Vec<Value>,
    seq: &mut i64,
    event_type: &str,
    category: &str,
    payload: Value,
) {
    rows.push(json!({
        "schema_version": 1,
        "line_type": "event",
        "seq": *seq,
        "event_type": event_type,
        "category": category,
        "payload": payload,
    }));
    *seq += 1;
}

fn sha256_json_value(value: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(value).context("failed to encode JSON value for hashing")?;
    Ok(crate::sha256_hex(bytes.as_slice()))
}

#[derive(Debug, Clone)]
struct RunTrajectoryDocument {
    manifest: Value,
    events: Vec<Value>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RunReplayReport {
    schema_version: u32,
    format: &'static str,
    status: &'static str,
    summary: RunReplaySummary,
    diffs: Vec<RunReplayDiff>,
    unsafe_mutations: Vec<RunReplayUnsafeMutation>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RunReplaySummary {
    event_count: usize,
    event_categories: BTreeMap<String, usize>,
    public_events: Vec<RunReplayPublicEvent>,
    mcp_imports: Vec<RunReplayMcpImport>,
    tool_proposals: Vec<RunReplayToolProposal>,
    tool_outputs: Vec<String>,
    final_answer_sha256: Option<String>,
    artifact_count: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RunReplayPublicEvent {
    event_type: String,
    category: String,
    stable_payload: Value,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RunReplayToolProposal {
    proposal_id: String,
    tool_name: String,
    replay_safety_class: String,
    mutation_class: String,
    recorded_output: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RunReplayMcpImport {
    event_type: String,
    server_name: Option<String>,
    catalog_generation: Option<u64>,
    imported_tools: Vec<String>,
    tool_count: usize,
    status: Option<String>,
    reason_code: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RunReplayDiff {
    path: String,
    expected: String,
    actual: String,
    context: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RunReplayUnsafeMutation {
    proposal_id: String,
    tool_name: String,
    reason: String,
}

fn parse_run_trajectory_jsonl(bytes: &[u8]) -> Result<RunTrajectoryDocument> {
    let text = std::str::from_utf8(bytes).context("trajectory JSONL must be UTF-8")?;
    let mut rows = text.lines().enumerate().filter(|(_, line)| !line.trim().is_empty()).map(
        |(index, line)| {
            serde_json::from_str::<Value>(line)
                .with_context(|| format!("failed to parse trajectory JSONL line {}", index + 1))
        },
    );
    let manifest = rows.next().context("trajectory JSONL is empty")??;
    if manifest.get("format").and_then(Value::as_str) != Some(RUN_TRAJECTORY_JSONL_FORMAT) {
        anyhow::bail!("trajectory manifest format must be {}", RUN_TRAJECTORY_JSONL_FORMAT);
    }
    verify_trajectory_manifest_hash(&manifest)?;
    let mut events = Vec::new();
    for row in rows {
        let row = row?;
        if row.get("line_type").and_then(Value::as_str) != Some("event") {
            anyhow::bail!("trajectory JSONL contains non-event row after manifest");
        }
        events.push(row);
    }
    Ok(RunTrajectoryDocument { manifest, events })
}

fn verify_trajectory_manifest_hash(manifest: &Value) -> Result<()> {
    let expected = manifest
        .get("manifest_hash_sha256")
        .and_then(Value::as_str)
        .context("trajectory manifest is missing manifest_hash_sha256")?;
    let mut without_hash = manifest.clone();
    if let Some(object) = without_hash.as_object_mut() {
        object.remove("manifest_hash_sha256");
    }
    let actual = sha256_json_value(&without_hash)?;
    if expected != actual {
        anyhow::bail!("trajectory manifest hash mismatch");
    }
    Ok(())
}

fn replay_trajectory_document(
    document: &RunTrajectoryDocument,
    golden: Option<&Value>,
) -> Result<RunReplayReport> {
    let summary = summarize_run_trajectory(document);
    let unsafe_mutations = unsafe_mutations_without_recorded_outputs(&summary);
    let mut diffs = Vec::new();
    if !unsafe_mutations.is_empty() {
        diffs.push(RunReplayDiff {
            path: "$.tool_proposals".to_owned(),
            expected: "mutating tools require recorded tool output".to_owned(),
            actual: format!(
                "{} unsafe mutation(s) without recorded output",
                unsafe_mutations.len()
            ),
            context: "offline replay refuses to re-run mutating tools without recorded evidence"
                .to_owned(),
        });
    }
    if let Some(golden) = golden {
        diffs.extend(compare_trajectory_summary_to_golden(&summary, golden)?);
    }
    let status = if diffs.is_empty() { "passed" } else { "failed" };
    Ok(RunReplayReport {
        schema_version: 1,
        format: "palyra-run-replay-report",
        status,
        summary,
        diffs,
        unsafe_mutations,
    })
}

fn summarize_run_trajectory(document: &RunTrajectoryDocument) -> RunReplaySummary {
    let mut event_categories = BTreeMap::<String, usize>::new();
    let mut public_events = Vec::with_capacity(document.events.len());
    let mut mcp_imports = Vec::new();
    let mut tool_proposals = BTreeMap::<String, RunReplayToolProposal>::new();
    let mut tool_outputs = BTreeSet::<String>::new();
    let mut final_answer_sha256 = None;
    let artifact_count =
        document.manifest.get("artifact_index").and_then(Value::as_array).map_or(0, Vec::len);

    for row in &document.events {
        let category = row.get("category").and_then(Value::as_str).unwrap_or("unknown");
        let event_type = row.get("event_type").and_then(Value::as_str).unwrap_or("unknown");
        *event_categories.entry(category.to_owned()).or_insert(0) += 1;
        let public_event = public_event_for_row(row, event_type, category);
        if category == "mcp_import" {
            if let Some(import) = mcp_import_from_public_event(&public_event) {
                mcp_imports.push(import);
            }
        }
        public_events.push(public_event);
        match event_type {
            "tool_proposal" => {
                if let Some(payload) = row.get("payload") {
                    if let (Some(proposal_id), Some(tool_name)) =
                        (payload_str(payload, "proposal_id"), payload_str(payload, "tool_name"))
                    {
                        tool_proposals.entry(proposal_id.to_owned()).or_insert_with(|| {
                            let replay_safety_class =
                                replay_safety_class_for_tool(tool_name, payload);
                            RunReplayToolProposal {
                                proposal_id: proposal_id.to_owned(),
                                tool_name: tool_name.to_owned(),
                                mutation_class: mutation_class_for_replay_safety_class(
                                    replay_safety_class.as_str(),
                                )
                                .to_owned(),
                                replay_safety_class,
                                recorded_output: false,
                            }
                        });
                    }
                }
            }
            "tool_output" => {
                if let Some(proposal_id) =
                    row.get("payload").and_then(|payload| payload_str(payload, "proposal_id"))
                {
                    tool_outputs.insert(proposal_id.to_owned());
                    if let Some(proposal) = tool_proposals.get_mut(proposal_id) {
                        proposal.recorded_output = true;
                    }
                }
            }
            "final_answer" => {
                final_answer_sha256 = row
                    .get("payload")
                    .and_then(|payload| payload.get("sha256"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
            }
            _ => {}
        }
    }
    let mut proposals = tool_proposals.into_values().collect::<Vec<_>>();
    proposals.sort_by(|left, right| left.proposal_id.cmp(&right.proposal_id));
    RunReplaySummary {
        event_count: document.events.len(),
        event_categories,
        public_events,
        mcp_imports,
        tool_proposals: proposals,
        tool_outputs: tool_outputs.into_iter().collect(),
        final_answer_sha256,
        artifact_count,
    }
}

fn public_event_for_row(row: &Value, event_type: &str, category: &str) -> RunReplayPublicEvent {
    let payload = row.get("payload").unwrap_or(&Value::Null);
    let stable_payload = match event_type {
        "tool_proposal" => public_tool_proposal_payload(payload),
        "tool_output" => json!({
            "tool_name": payload_str(payload, "tool_name"),
            "recorded_output": payload_str(payload, "proposal_id").is_some(),
            "has_output_artifact_ref": payload.pointer("/result/output_artifact_ref").is_some()
                || payload.pointer("/payload/output_artifact_ref").is_some(),
        }),
        "final_answer" => json!({
            "sha256": payload.get("sha256").and_then(Value::as_str),
        }),
        "usage" => json!({
            "prompt_tokens": payload.get("prompt_tokens").and_then(Value::as_i64),
            "completion_tokens": payload.get("completion_tokens").and_then(Value::as_i64),
            "total_tokens": payload.get("total_tokens").and_then(Value::as_i64),
        }),
        "verification_summary" => json!({
            "decision": payload
                .pointer("/latest_verification_status/decision")
                .and_then(Value::as_str),
            "commands": payload
                .get("commands_executed")
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
            "unverified_mutations": payload
                .get("unverified_mutations")
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
            "final_answer_allowed": payload.get("final_answer_allowed").and_then(Value::as_bool),
            "final_answer_allowed_because": payload
                .get("final_answer_allowed_because")
                .and_then(Value::as_str),
        }),
        value if value.contains("mcp") => project_mcp_public_payload(payload),
        _ => json!({}),
    };
    RunReplayPublicEvent {
        event_type: event_type.to_owned(),
        category: category.to_owned(),
        stable_payload,
    }
}

fn public_tool_proposal_payload(payload: &Value) -> Value {
    let replay_safety_class = payload_str(payload, "tool_name")
        .map(|tool_name| replay_safety_class_for_tool(tool_name, payload));
    json!({
        "tool_name": payload_str(payload, "tool_name"),
        "mutation_class": replay_safety_class
            .as_deref()
            .map(mutation_class_for_replay_safety_class),
        "replay_safety_class": replay_safety_class,
    })
}

fn project_mcp_public_payload(payload: &Value) -> Value {
    let imported_tools = collect_mcp_tool_names(payload);
    let tool_count = payload_u64(payload, "tool_count")
        .or_else(|| payload_u64(payload, "imported_tool_count"))
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(imported_tools.len());
    json!({
        "server_name": payload_str(payload, "server_name")
            .or_else(|| payload_str(payload, "server_id"))
            .or_else(|| payload_str(payload, "namespace")),
        "catalog_generation": payload_u64(payload, "catalog_generation"),
        "imported_tools": imported_tools,
        "tool_count": tool_count,
        "status": payload_str(payload, "status")
            .or_else(|| payload_str(payload, "state")),
        "reason_code": payload_str(payload, "reason_code")
            .or_else(|| payload_str(payload, "code")),
    })
}

fn mcp_import_from_public_event(event: &RunReplayPublicEvent) -> Option<RunReplayMcpImport> {
    if !matches!(event.event_type.as_str(), "mcp.discovery.snapshot" | "mcp.tool_import.snapshot") {
        return None;
    }
    let payload = &event.stable_payload;
    let imported_tools = payload
        .get("imported_tools")
        .and_then(Value::as_array)
        .map(|tools| {
            tools.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let tool_count = payload
        .get("tool_count")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(imported_tools.len());
    Some(RunReplayMcpImport {
        event_type: event.event_type.clone(),
        server_name: payload.get("server_name").and_then(Value::as_str).map(ToOwned::to_owned),
        catalog_generation: payload.get("catalog_generation").and_then(Value::as_u64),
        imported_tools,
        tool_count,
        status: payload.get("status").and_then(Value::as_str).map(ToOwned::to_owned),
        reason_code: payload.get("reason_code").and_then(Value::as_str).map(ToOwned::to_owned),
    })
}

fn payload_str<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload.get(key).and_then(Value::as_str).or_else(|| {
        payload.get("payload").and_then(|nested| nested.get(key)).and_then(Value::as_str)
    })
}

fn payload_u64(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(Value::as_u64).or_else(|| {
        payload.get("payload").and_then(|nested| nested.get(key)).and_then(Value::as_u64)
    })
}

fn collect_mcp_tool_names(payload: &Value) -> Vec<String> {
    let mut names = BTreeSet::new();
    for pointer in [
        "/tools",
        "/imported_tools",
        "/tool_imports",
        "/registry_entries",
        "/catalog/tools",
        "/snapshot/tools",
        "/payload/tools",
        "/payload/imported_tools",
        "/payload/tool_imports",
        "/payload/registry_entries",
        "/payload/catalog/tools",
        "/payload/snapshot/tools",
    ] {
        if let Some(value) = payload.pointer(pointer) {
            collect_mcp_tool_names_from_value(value, &mut names);
        }
    }
    names.into_iter().collect()
}

fn collect_mcp_tool_names_from_value(value: &Value, names: &mut BTreeSet<String>) {
    if let Some(array) = value.as_array() {
        for item in array {
            if let Some(name) = item
                .as_str()
                .or_else(|| payload_str(item, "name"))
                .or_else(|| payload_str(item, "tool_name"))
            {
                names.insert(name.to_owned());
            }
        }
    }
}

fn replay_safety_class_for_tool(tool_name: &str, payload: &Value) -> String {
    let input = payload.get("input").unwrap_or(&Value::Null);
    let mut safety_class = replay_safety_class_for_tool_input(tool_name, input);
    strengthen_replay_safety_class(
        &mut safety_class,
        payload
            .get("replay_safety_class")
            .and_then(Value::as_str)
            .and_then(normalize_replay_safety_class),
    );
    safety_class
}

fn replay_safety_class_for_tool_input(tool_name: &str, input: &Value) -> String {
    let normalized = tool_name.to_ascii_lowercase();
    let mut safety_class = if normalized.contains("browser.")
        || normalized.contains("process.")
        || normalized.contains("shell")
        || normalized.contains("exec")
    {
        "external_side_effect"
    } else if ["write", "patch", "delete", "remove", "move", "rename", "shell", "exec"]
        .iter()
        .any(|needle| normalized.contains(needle))
    {
        "non_idempotent_write"
    } else {
        "read_only"
    }
    .to_owned();

    // Trajectory fields are untrusted evidence. They may strengthen the
    // tool-name safety floor, but must never downgrade it.
    for explicit_class in [
        input
            .get("replay_safety_class")
            .and_then(Value::as_str)
            .and_then(normalize_replay_safety_class),
        input.get("mutation_class").and_then(Value::as_str).and_then(normalize_replay_safety_class),
    ] {
        strengthen_replay_safety_class(&mut safety_class, explicit_class);
    }
    safety_class
}

fn strengthen_replay_safety_class(current: &mut String, candidate: Option<&str>) {
    let Some(candidate) = candidate else {
        return;
    };
    if replay_safety_class_rank(candidate) > replay_safety_class_rank(current.as_str()) {
        *current = candidate.to_owned();
    }
}

fn replay_safety_class_rank(value: &str) -> u8 {
    match value {
        "read_only" => 0,
        "idempotent_write" => 1,
        "non_idempotent_write" => 2,
        "external_side_effect" => 3,
        "requires_human_confirmation" => 4,
        _ => 4,
    }
}

fn normalize_replay_safety_class(value: &str) -> Option<&'static str> {
    match value.trim() {
        "read_only" | "replay_safe" => Some("read_only"),
        "idempotent" | "idempotent_write" => Some("idempotent_write"),
        "non_idempotent" | "non_idempotent_write" => Some("non_idempotent_write"),
        "external_side_effect" => Some("external_side_effect"),
        "requires_human_confirmation" => Some("requires_human_confirmation"),
        _ => None,
    }
}

fn mutation_class_for_replay_safety_class(replay_safety_class: &str) -> &'static str {
    if replay_safety_class_requires_recorded_evidence(replay_safety_class) {
        "non_idempotent"
    } else {
        "replay_safe"
    }
}

fn replay_safety_class_requires_recorded_evidence(replay_safety_class: &str) -> bool {
    matches!(
        replay_safety_class,
        "non_idempotent_write" | "external_side_effect" | "requires_human_confirmation"
    )
}

fn unsafe_mutations_without_recorded_outputs(
    summary: &RunReplaySummary,
) -> Vec<RunReplayUnsafeMutation> {
    summary
        .tool_proposals
        .iter()
        .filter(|proposal| {
            replay_safety_class_requires_recorded_evidence(proposal.replay_safety_class.as_str())
                && !proposal.recorded_output
        })
        .map(|proposal| RunReplayUnsafeMutation {
            proposal_id: proposal.proposal_id.clone(),
            tool_name: proposal.tool_name.clone(),
            reason: format!(
                "{} replay safety class lacks recorded tool output evidence",
                proposal.replay_safety_class
            ),
        })
        .collect()
}

fn compare_trajectory_summary_to_golden(
    summary: &RunReplaySummary,
    golden: &Value,
) -> Result<Vec<RunReplayDiff>> {
    let expected = golden.get("expected").unwrap_or(golden);
    let mut diffs = Vec::new();
    compare_optional_usize(
        &mut diffs,
        "$.expected.event_count",
        expected.get("event_count").and_then(Value::as_u64).map(|value| value as usize),
        summary.event_count,
        "trajectory event count",
    );
    if let Some(expected_hash) = expected.get("final_answer_sha256").and_then(Value::as_str) {
        if summary.final_answer_sha256.as_deref() != Some(expected_hash) {
            diffs.push(RunReplayDiff {
                path: "$.expected.final_answer_sha256".to_owned(),
                expected: expected_hash.to_owned(),
                actual: summary
                    .final_answer_sha256
                    .clone()
                    .unwrap_or_else(|| "<missing>".to_owned()),
                context: "final answer hash changed".to_owned(),
            });
        }
    }
    if let Some(expected_tools) = expected.get("tool_proposals").and_then(Value::as_array) {
        let actual_tools = summary
            .tool_proposals
            .iter()
            .map(|proposal| proposal.tool_name.clone())
            .collect::<Vec<_>>();
        let expected_tools = expected_tools
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if expected_tools != actual_tools {
            diffs.push(RunReplayDiff {
                path: "$.expected.tool_proposals".to_owned(),
                expected: format!("{expected_tools:?}"),
                actual: format!("{actual_tools:?}"),
                context: "tool proposal stream changed".to_owned(),
            });
        }
    }
    if let Some(expected_imports) = expected.get("mcp_imports").and_then(Value::as_array) {
        diffs.extend(compare_mcp_imports_to_golden(&summary.mcp_imports, expected_imports)?);
    }
    if let Some(expected_events) =
        expected.get("events").or_else(|| expected.get("public_events")).and_then(Value::as_array)
    {
        diffs.extend(compare_public_events_to_golden(&summary.public_events, expected_events)?);
    }
    Ok(diffs)
}

fn compare_mcp_imports_to_golden(
    actual_imports: &[RunReplayMcpImport],
    expected_imports: &[Value],
) -> Result<Vec<RunReplayDiff>> {
    if expected_imports.len() != actual_imports.len() {
        return Ok(vec![RunReplayDiff {
            path: "$.expected.mcp_imports".to_owned(),
            expected: format!("{} MCP import event(s)", expected_imports.len()),
            actual: format!("{} MCP import event(s)", actual_imports.len()),
            context: "MCP import snapshot stream changed".to_owned(),
        }]);
    }
    for (index, (actual, expected)) in actual_imports.iter().zip(expected_imports).enumerate() {
        if !mcp_import_matches_expected(actual, expected)? {
            let actual = serde_json::to_string(actual)
                .context("failed to encode actual MCP import replay event")?;
            return Ok(vec![RunReplayDiff {
                path: format!("$.expected.mcp_imports[{index}]"),
                expected: expected.to_string(),
                actual,
                context: "MCP import snapshot changed".to_owned(),
            }]);
        }
    }
    Ok(Vec::new())
}

fn mcp_import_matches_expected(actual: &RunReplayMcpImport, expected: &Value) -> Result<bool> {
    if let Some(event_type) = expected.as_str() {
        return Ok(actual.event_type == event_type);
    }
    let Some(expected_object) = expected.as_object() else {
        return Ok(false);
    };
    let actual_value =
        serde_json::to_value(actual).context("failed to encode actual MCP import replay event")?;
    Ok(expected_object
        .iter()
        .all(|(key, expected_value)| actual_value.get(key) == Some(expected_value)))
}

fn compare_public_events_to_golden(
    actual_events: &[RunReplayPublicEvent],
    expected_events: &[Value],
) -> Result<Vec<RunReplayDiff>> {
    if expected_events.len() != actual_events.len() {
        return Ok(vec![RunReplayDiff {
            path: "$.expected.events".to_owned(),
            expected: format!("{} public event(s)", expected_events.len()),
            actual: format!("{} public event(s)", actual_events.len()),
            context: "public event stream length changed".to_owned(),
        }]);
    }
    for (index, (actual, expected)) in actual_events.iter().zip(expected_events).enumerate() {
        if !public_event_matches_expected(actual, expected)? {
            let actual = serde_json::to_string(actual)
                .context("failed to encode actual public replay event")?;
            return Ok(vec![RunReplayDiff {
                path: format!("$.expected.events[{index}]"),
                expected: expected.to_string(),
                actual,
                context: "public event stream changed".to_owned(),
            }]);
        }
    }
    Ok(Vec::new())
}

fn public_event_matches_expected(actual: &RunReplayPublicEvent, expected: &Value) -> Result<bool> {
    if let Some(event_type) = expected.as_str() {
        return Ok(actual.event_type == event_type);
    }
    let Some(expected_object) = expected.as_object() else {
        return Ok(false);
    };
    let actual_value =
        serde_json::to_value(actual).context("failed to encode actual public replay event")?;
    Ok(expected_object
        .iter()
        .all(|(key, expected_value)| actual_value.get(key) == Some(expected_value)))
}

fn compare_optional_usize(
    diffs: &mut Vec<RunReplayDiff>,
    path: &str,
    expected: Option<usize>,
    actual: usize,
    context: &str,
) {
    if let Some(expected) = expected {
        if expected != actual {
            diffs.push(RunReplayDiff {
                path: path.to_owned(),
                expected: expected.to_string(),
                actual: actual.to_string(),
                context: context.to_owned(),
            });
        }
    }
}

fn render_replay_diff_markdown(report: &RunReplayReport) -> String {
    let mut output = String::new();
    output.push_str("# Run Replay Diff\n\n");
    output.push_str(format!("- Status: `{}`\n", report.status).as_str());
    output.push_str(format!("- Events: `{}`\n", report.summary.event_count).as_str());
    output.push_str(format!("- Diffs: `{}`\n", report.diffs.len()).as_str());
    if report.diffs.is_empty() {
        output.push_str("\nNo meaningful trajectory differences found.\n");
        return output;
    }
    output.push_str("\n| Path | Expected | Actual | Context |\n");
    output.push_str("| --- | --- | --- | --- |\n");
    for diff in &report.diffs {
        output.push_str(
            format!(
                "| `{}` | `{}` | `{}` | {} |\n",
                markdown_escape(diff.path.as_str()),
                markdown_escape(diff.expected.as_str()),
                markdown_escape(diff.actual.as_str()),
                markdown_escape(diff.context.as_str())
            )
            .as_str(),
        );
    }
    output
}

fn markdown_escape(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn build_run_export_payload(
    bundle: &ReplayBundle,
    format: RunExportFormatArg,
    redacted: bool,
) -> Result<Value> {
    let manifest = build_run_export_manifest(bundle, format, redacted)?;
    let verification_summary = build_run_verification_summary(bundle);
    let payload = match format {
        RunExportFormatArg::PalyraAttested => json!({
            "schema_version": 1,
            "format": format.as_str(),
            "redacted": redacted,
            "manifest": manifest,
            "trajectory": {
                "source": bundle.source,
                "run": bundle.run,
                "context_manifest": bundle.config_snapshot,
                "tape_events": bundle.tape_events,
                "model_exchanges": bundle.model_exchanges,
                "tool_calls": bundle.tool_exchanges,
                "policy_decisions": {
                    "approvals": bundle.approvals,
                    "queue_decisions": bundle.queue_decisions,
                    "auxiliary_tasks": bundle.auxiliary_tasks,
                    "flow_events": bundle.flow_events,
                },
                "attestations": build_tool_attestation_index(bundle),
                "verification_summary": verification_summary,
                "artifact_refs": bundle.artifact_refs,
                "expected": bundle.expected,
                "redaction_manifest": bundle.redaction,
                "termination": {
                    "state": bundle.run.state,
                    "last_error": bundle.run.last_error,
                },
                "usage": {
                    "prompt_tokens": bundle.run.prompt_tokens,
                    "completion_tokens": bundle.run.completion_tokens,
                    "total_tokens": bundle.run.total_tokens,
                },
                "routing": {
                    "origin_kind": bundle.source.origin_kind,
                    "principal": bundle.run.principal,
                    "device_id": bundle.run.device_id,
                    "channel": bundle.run.channel,
                },
            },
            "replay_bundle": bundle,
        }),
        RunExportFormatArg::Sharegpt => json!({
            "schema_version": 1,
            "format": format.as_str(),
            "redacted": redacted,
            "manifest": manifest,
            "conversations": [
                {
                    "from": "human",
                    "value": user_input_text(bundle).unwrap_or_else(|| "[redacted input unavailable]".to_owned()),
                },
                {
                    "from": "gpt",
                    "value": bundle.expected.final_answer_summary.clone().unwrap_or_else(|| "[final answer unavailable]".to_owned()),
                },
            ],
        }),
        RunExportFormatArg::Atropos => json!({
            "schema_version": 1,
            "format": format.as_str(),
            "redacted": redacted,
            "manifest": manifest,
            "trajectory": {
                "id": bundle.bundle_id,
                "messages": [
                    {
                        "role": "user",
                        "content": user_input_text(bundle).unwrap_or_else(|| "[redacted input unavailable]".to_owned()),
                    },
                    {
                        "role": "assistant",
                        "content": bundle.expected.final_answer_summary.clone().unwrap_or_else(|| "[final answer unavailable]".to_owned()),
                    },
                ],
                "events": bundle.tape_events,
                "tool_calls": bundle.tool_exchanges,
                "approvals": bundle.approvals,
                "attestations": build_tool_attestation_index(bundle),
                "verification_summary": verification_summary,
                "artifacts": bundle.artifact_refs,
            },
        }),
    };
    Ok(payload)
}

fn build_run_export_manifest(
    bundle: &ReplayBundle,
    format: RunExportFormatArg,
    redacted: bool,
) -> Result<Value> {
    let mut digest_bundle = bundle.clone();
    digest_bundle.integrity.canonical_sha256 = None;
    let canonical_bytes = canonical_replay_bundle_bytes(&digest_bundle)?;
    let canonical_sha256 = crate::sha256_hex(canonical_bytes.as_slice());
    let embedded_sha256 = bundle
        .integrity
        .canonical_sha256
        .as_deref()
        .context("replay bundle is missing canonical integrity hash")?;
    if embedded_sha256 != canonical_sha256 {
        anyhow::bail!("replay bundle canonical digest verification failed");
    }
    let replay_report = replay_bundle_offline(bundle);
    if replay_report.status != ReplayRunStatus::Passed {
        anyhow::bail!(
            "replay bundle offline verification failed with {} diffs and {} validation issues",
            replay_report.diffs.len(),
            replay_report.validation.issues.len()
        );
    }
    let instruction_hash = bundle
        .run
        .normalized_user_input
        .as_ref()
        .map(|value| crate::sha256_hex(value.to_string().as_bytes()));
    Ok(json!({
        "schema_version": 1,
        "format": format.as_str(),
        "redacted": redacted,
        "replay_bundle_schema_version": bundle.schema_version,
        "replay_bundle_contract_version": bundle.contract_version,
        "replay_bundle_sha256": canonical_sha256,
        "digest_verified": true,
        "offline_replay_status": "passed",
        "instruction_hash_sha256": instruction_hash,
        "includes": {
            "user_input": bundle.run.normalized_user_input.is_some(),
            "context_manifest": true,
            "tool_catalog_snapshot": bundle.config_snapshot.get("contract").is_some(),
            "policy_decisions": !bundle.approvals.is_empty()
                || !bundle.queue_decisions.is_empty()
                || !bundle.auxiliary_tasks.is_empty()
                || !bundle.flow_events.is_empty(),
            "approvals": !bundle.approvals.is_empty(),
            "tool_calls": !bundle.tool_exchanges.is_empty(),
            "attestations": bundle.tool_exchanges.iter().any(|exchange| exchange.attestation.is_some()),
            "verification_summary": true,
            "artifact_ids": !bundle.artifact_refs.is_empty(),
            "redaction_manifest": true,
            "final_response": bundle.expected.final_answer_summary.is_some()
                || bundle.expected.final_answer_sha256.is_some(),
            "termination_reason": true,
            "usage": true,
            "routing_metadata": true,
        },
        "allowed_nondeterminism": [
            "wall_clock_timestamps_normalized",
            "pseudonymized_identifiers",
        ],
        "required_capabilities": [
            "offline_replay",
            "deterministic_provider_or_fake_provider",
            "redaction_policy_enforced",
        ],
        "redaction": bundle.redaction,
    }))
}

fn build_tool_attestation_index(bundle: &ReplayBundle) -> Vec<Value> {
    bundle
        .tool_exchanges
        .iter()
        .filter_map(|exchange| {
            exchange.attestation.as_ref().map(|attestation| {
                json!({
                    "proposal_id": exchange.proposal_id,
                    "tool_name": exchange.tool_name,
                    "attestation": attestation,
                })
            })
        })
        .collect()
}

fn user_input_text(bundle: &ReplayBundle) -> Option<String> {
    let value = bundle.run.normalized_user_input.as_ref()?;
    match value {
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_common::replay_bundle::{
        build_replay_bundle, ReplayBundleBuildInput, ReplayCaptureMetadata, ReplayRunSnapshot,
        ReplaySource, ReplayTapeEvent,
    };

    #[test]
    fn palyra_attested_export_includes_digest_and_redaction_manifest() {
        let bundle = fixture_bundle();

        let payload =
            build_run_export_payload(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();

        assert_eq!(payload.get("format").and_then(Value::as_str), Some("palyra-attested"));
        let manifest = payload.get("manifest").expect("manifest should be present");
        assert_eq!(manifest.get("digest_verified").and_then(Value::as_bool), Some(true));
        assert_eq!(
            manifest
                .get("includes")
                .and_then(|value| value.get("tool_calls"))
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(!payload.to_string().contains("secret-token"));
    }

    #[test]
    fn sharegpt_export_uses_redacted_replay_content() {
        let bundle = fixture_bundle();

        let payload =
            build_run_export_payload(&bundle, RunExportFormatArg::Sharegpt, true).unwrap();

        assert_eq!(payload.get("format").and_then(Value::as_str), Some("sharegpt"));
        assert!(payload
            .get("conversations")
            .and_then(Value::as_array)
            .is_some_and(|entries| entries.len() == 2));
        assert!(!payload.to_string().contains("secret-token"));
    }

    #[test]
    fn trajectory_jsonl_includes_manifest_hash_and_event_classes() {
        let bundle = fixture_bundle();

        let (bytes, manifest_hash) =
            build_run_trajectory_jsonl(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();
        let text = String::from_utf8(bytes).expect("trajectory should be UTF-8");
        let lines = text.lines().collect::<Vec<_>>();
        let manifest: Value =
            serde_json::from_str(lines.first().expect("manifest line should exist"))
                .expect("manifest line should parse");

        assert_eq!(
            manifest.get("format").and_then(Value::as_str),
            Some(RUN_TRAJECTORY_JSONL_FORMAT)
        );
        assert_eq!(
            manifest.get("manifest_hash_sha256").and_then(Value::as_str),
            Some(manifest_hash.as_str())
        );
        assert!(lines.iter().skip(1).any(|line| {
            let event = serde_json::from_str::<Value>(line).expect("event line should parse");
            event.get("category").and_then(Value::as_str) == Some("tool_output")
        }));
        assert!(
            lines.iter().skip(1).any(|line| {
                let event = serde_json::from_str::<Value>(line).expect("event line should parse");
                event.get("event_type").and_then(Value::as_str) == Some("tool_attestation")
                    && (event
                        .pointer("/payload/payload/execution_manifest/schema_version")
                        .and_then(Value::as_u64)
                        == Some(1)
                        || event
                            .pointer("/payload/attestation/execution_manifest/schema_version")
                            .and_then(Value::as_u64)
                            == Some(1))
            }),
            "trajectory JSONL should export backend execution manifests"
        );
        assert!(!text.contains("secret-token"));
    }

    #[test]
    fn trajectory_jsonl_includes_verification_summary() {
        let bundle = fixture_verification_bundle();

        let (bytes, _) =
            build_run_trajectory_jsonl(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();
        let values = String::from_utf8(bytes)
            .expect("trajectory should be UTF-8")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("jsonl row should parse"))
            .collect::<Vec<_>>();
        let manifest = &values[0];
        let summary_event = values
            .iter()
            .find(|row| {
                row.get("event_type").and_then(Value::as_str) == Some("verification_summary")
            })
            .expect("verification summary event should be exported");

        assert!(manifest
            .get("event_classes")
            .and_then(Value::as_array)
            .is_some_and(|classes| classes.iter().any(|class| class == "verification_summary")));
        assert_eq!(
            summary_event.get("category").and_then(Value::as_str),
            Some("verification_summary")
        );
        assert_eq!(
            summary_event.pointer("/payload/latest_verification_status/decision"),
            Some(&json!("stale"))
        );
        assert_eq!(
            summary_event.pointer("/payload/commands_executed/0/status"),
            Some(&json!("passed"))
        );
        assert_eq!(
            summary_event.pointer("/payload/changed_files/0"),
            Some(&json!("<redacted:path>"))
        );
        assert_eq!(summary_event.pointer("/payload/final_answer_allowed"), Some(&json!(false)));
        assert_eq!(
            summary_event.pointer("/payload/final_answer_allowed_because"),
            Some(&json!("verification.finalizer.stale_after_code_mutation"))
        );

        let export =
            build_run_export_payload(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();
        assert_eq!(export.pointer("/manifest/includes/verification_summary"), Some(&json!(true)));
        assert_eq!(
            export.pointer("/trajectory/verification_summary/latest_verification_status/decision"),
            Some(&json!("stale"))
        );
    }

    #[test]
    fn trajectory_jsonl_projects_large_tool_output_to_artifact_ref() {
        let bundle = fixture_bundle_with_tool_output(json!({
            "success": true,
            "output_json": { "body": "x".repeat(LARGE_TRAJECTORY_TOOL_OUTPUT_BYTES + 32) },
            "error": "",
        }));

        let (bytes, _) =
            build_run_trajectory_jsonl(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();
        let text = String::from_utf8(bytes).expect("trajectory should be UTF-8");
        let values = text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("jsonl row should parse"))
            .collect::<Vec<_>>();

        assert!(values[0].pointer("/artifact_index").and_then(Value::as_array).is_some_and(
            |artifacts| artifacts.iter().any(|artifact| {
                artifact.get("kind").and_then(Value::as_str) == Some("large_tool_output")
            })
        ));
        assert!(values.iter().skip(1).any(|row| {
            row.pointer("/payload/result/output_artifact_ref/kind").and_then(Value::as_str)
                == Some("large_tool_output")
        }));
        assert!(!text.contains(&"x".repeat(LARGE_TRAJECTORY_TOOL_OUTPUT_BYTES + 32)));
    }

    #[test]
    fn replay_simple_text_trajectory_passes() {
        let document = RunTrajectoryDocument {
            manifest: json!({ "artifact_index": [] }),
            events: vec![json!({
                "schema_version": 1,
                "line_type": "event",
                "seq": 0,
                "event_type": "final_answer",
                "category": "final_answer",
                "payload": { "sha256": "b".repeat(64) },
            })],
        };

        let report = replay_trajectory_document(&document, None).unwrap();
        let expected_hash = "b".repeat(64);

        assert_eq!(report.status, "passed");
        assert_eq!(report.summary.event_count, 1);
        assert_eq!(report.summary.final_answer_sha256.as_deref(), Some(expected_hash.as_str()));
    }

    #[test]
    fn replay_tool_call_trajectory_passes_with_recorded_output() {
        let bundle = fixture_bundle();
        let (bytes, _) =
            build_run_trajectory_jsonl(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();
        let document = parse_run_trajectory_jsonl(bytes.as_slice()).unwrap();

        let report = replay_trajectory_document(&document, None).unwrap();

        assert_eq!(report.status, "passed");
        assert!(report
            .summary
            .tool_proposals
            .iter()
            .any(|proposal| { proposal.tool_name == "palyra.shell" && proposal.recorded_output }));
        assert!(report.unsafe_mutations.is_empty());
    }

    #[test]
    fn replay_mcp_trajectory_uses_recorded_outputs_and_import_snapshots() {
        let bundle = fixture_mcp_bundle();
        let (bytes, _) =
            build_run_trajectory_jsonl(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();
        let text = String::from_utf8(bytes.clone()).expect("trajectory should be UTF-8");
        let document = parse_run_trajectory_jsonl(bytes.as_slice()).unwrap();

        let report = replay_trajectory_document(
            &document,
            Some(&json!({
                "expected": {
                    "mcp_imports": [
                        {
                            "event_type": "mcp.discovery.snapshot",
                            "server_name": "docs",
                            "tool_count": 2,
                            "imported_tools": ["mcp.docs.search", "mcp.docs.write_note"]
                        },
                        {
                            "event_type": "mcp.tool_import.snapshot",
                            "server_name": "docs",
                            "catalog_generation": 7,
                            "status": "available"
                        }
                    ]
                }
            })),
        )
        .unwrap();

        assert_eq!(report.status, "passed");
        assert_eq!(report.summary.mcp_imports.len(), 2);
        assert!(report.summary.event_categories.contains_key("mcp_import"));
        assert!(report.summary.tool_proposals.iter().any(|proposal| {
            proposal.tool_name == "mcp.docs.write_note"
                && proposal.mutation_class == "non_idempotent"
                && proposal.recorded_output
        }));
        assert!(report.unsafe_mutations.is_empty());
        assert!(text.contains("mcp.discovery.snapshot"));
        assert!(text.contains("mcp.tool_import.snapshot"));
        assert!(text.contains("mcp.docs.search"));
        assert!(!text.contains("mcp-secret-token"));
    }

    #[test]
    fn replay_golden_compare_accepts_stable_public_event_stream() {
        let bundle = fixture_bundle();
        let (bytes, _) =
            build_run_trajectory_jsonl(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();
        let document = parse_run_trajectory_jsonl(bytes.as_slice()).unwrap();
        let baseline = replay_trajectory_document(&document, None).unwrap();
        let expected_events = baseline
            .summary
            .public_events
            .iter()
            .map(|event| {
                json!({
                    "event_type": event.event_type.as_str(),
                    "category": event.category.as_str(),
                })
            })
            .collect::<Vec<_>>();

        let report = replay_trajectory_document(
            &document,
            Some(&json!({ "expected": { "events": expected_events } })),
        )
        .unwrap();

        assert_eq!(report.status, "passed");
    }

    #[test]
    fn replay_rejects_non_idempotent_mutation_without_recorded_evidence() {
        let document = RunTrajectoryDocument {
            manifest: json!({ "artifact_index": [] }),
            events: vec![json!({
                "schema_version": 1,
                "line_type": "event",
                "seq": 0,
                "event_type": "tool_proposal",
                "category": "tool_proposal",
                "payload": {
                    "proposal_id": "proposal-unsafe",
                    "tool_name": "palyra.fs.write_file",
                    "input": { "path": "state.json", "mutation_class": "non_idempotent" },
                },
            })],
        };

        let report = replay_trajectory_document(&document, None).unwrap();

        assert_eq!(report.status, "failed");
        assert_eq!(report.unsafe_mutations.len(), 1);
        assert_eq!(report.unsafe_mutations[0].proposal_id, "proposal-unsafe");
        assert!(report.diffs.iter().any(|diff| diff.path == "$.tool_proposals"));
    }

    #[test]
    fn replay_rejects_side_effect_class_without_recorded_evidence() {
        let document = RunTrajectoryDocument {
            manifest: json!({ "artifact_index": [] }),
            events: vec![json!({
                "schema_version": 1,
                "line_type": "event",
                "seq": 0,
                "event_type": "tool_proposal",
                "category": "tool_proposal",
                "payload": {
                    "proposal_id": "proposal-side-effect",
                    "tool_name": "palyra.browser.click",
                    "replay_safety_class": "external_side_effect",
                    "input": { "selector": "#submit" },
                },
            })],
        };

        let report = replay_trajectory_document(&document, None).unwrap();

        assert_eq!(report.status, "failed");
        assert_eq!(report.unsafe_mutations.len(), 1);
        assert_eq!(report.summary.tool_proposals[0].replay_safety_class, "external_side_effect");
        assert_eq!(report.summary.tool_proposals[0].mutation_class, "non_idempotent");
        assert_eq!(report.unsafe_mutations[0].proposal_id, "proposal-side-effect");
        assert!(report.unsafe_mutations[0].reason.contains("external_side_effect"));
    }

    #[test]
    fn replay_payload_cannot_downgrade_dangerous_tool_safety() {
        let document = RunTrajectoryDocument {
            manifest: json!({ "artifact_index": [] }),
            events: vec![
                json!({
                    "schema_version": 1,
                    "line_type": "event",
                    "seq": 0,
                    "event_type": "tool_proposal",
                    "category": "tool_proposal",
                    "payload": {
                        "proposal_id": "proposal-process",
                        "tool_name": "palyra.process.run",
                        "replay_safety_class": "read_only",
                        "input": {
                            "command": "echo",
                            "mutation_class": "replay_safe",
                            "replay_safety_class": "idempotent"
                        },
                    },
                }),
                json!({
                    "schema_version": 1,
                    "line_type": "event",
                    "seq": 1,
                    "event_type": "tool_proposal",
                    "category": "tool_proposal",
                    "payload": {
                        "proposal_id": "proposal-browser",
                        "tool_name": "palyra.browser.click",
                        "replay_safety_class": "idempotent_write",
                        "input": {
                            "selector": "#submit",
                            "mutation_class": "read_only"
                        },
                    },
                }),
            ],
        };

        let report = replay_trajectory_document(&document, None).unwrap();

        assert_eq!(report.status, "failed");
        assert_eq!(report.unsafe_mutations.len(), 2);
        assert!(report.summary.tool_proposals.iter().all(|proposal| {
            proposal.replay_safety_class == "external_side_effect"
                && proposal.mutation_class == "non_idempotent"
                && !proposal.recorded_output
        }));
    }

    #[test]
    fn replay_golden_diff_matches_snapshot() {
        let bundle = fixture_bundle();
        let (bytes, _) =
            build_run_trajectory_jsonl(&bundle, RunExportFormatArg::PalyraAttested, true).unwrap();
        let document = parse_run_trajectory_jsonl(bytes.as_slice()).unwrap();
        let report = replay_trajectory_document(
            &document,
            Some(&json!({
                "expected": {
                    "event_count": 1,
                    "tool_proposals": ["palyra.safe.read"],
                }
            })),
        )
        .unwrap();

        let markdown = render_replay_diff_markdown(&report);

        assert_eq!(
            markdown,
            include_str!("../../../../fixtures/golden/run_replay_diff.md").replace("\r\n", "\n")
        );
    }

    fn fixture_bundle() -> ReplayBundle {
        fixture_bundle_with_tool_output(json!({
            "proposal_id": "proposal-1",
            "success": true,
            "output_json": "done",
            "error": "",
            "attestation": { "execution_sha256": "a".repeat(64) },
        }))
    }

    fn fixture_bundle_with_tool_output(tool_output: Value) -> ReplayBundle {
        fixture_bundle_with_named_tool("palyra.shell", tool_output, Vec::new())
    }

    fn fixture_mcp_bundle() -> ReplayBundle {
        fixture_bundle_with_named_tool(
            "mcp.docs.write_note",
            json!({
                "proposal_id": "proposal-1",
                "success": true,
                "output_json": {
                    "artifact": "trajectory://mcp/docs/write-note",
                    "status": "written"
                },
                "error": "tool completed after token=mcp-secret-token was redacted",
                "attestation": { "execution_sha256": "c".repeat(64) },
            }),
            vec![
                ReplayTapeEvent {
                    seq: 1,
                    event_type: "mcp.discovery.snapshot".to_owned(),
                    payload: json!({
                        "server_name": "docs",
                        "catalog_generation": 7,
                        "status": "healthy",
                        "tools": [
                            { "name": "mcp.docs.search", "input_schema_sha256": "d".repeat(64) },
                            { "name": "mcp.docs.write_note", "input_schema_sha256": "e".repeat(64) }
                        ],
                        "diagnostic": "probe succeeded with token=mcp-secret-token"
                    }),
                },
                ReplayTapeEvent {
                    seq: 2,
                    event_type: "mcp.tool_import.snapshot".to_owned(),
                    payload: json!({
                        "server_name": "docs",
                        "catalog_generation": 7,
                        "status": "available",
                        "imported_tools": ["mcp.docs.search", "mcp.docs.write_note"]
                    }),
                },
            ],
        )
    }

    fn fixture_verification_bundle() -> ReplayBundle {
        fixture_bundle_with_named_tool(
            "palyra.shell",
            json!({
                "proposal_id": "proposal-1",
                "success": true,
                "output_json": "done",
                "error": "",
                "attestation": { "execution_sha256": "a".repeat(64) },
            }),
            vec![
                ReplayTapeEvent {
                    seq: 1,
                    event_type: "verification.command.classified".to_owned(),
                    payload: json!({
                        "event_type": "verification.command.classified",
                        "created_at_unix_ms": 1_000,
                        "reason_codes": ["verification.command_classified", "verification.command_supported"],
                        "evidence_refs": ["tool_call:process-1"],
                        "classification": {
                            "canonical_command": {
                                "display": "cargo test --workspace"
                            },
                            "is_verification": true,
                            "kind": "test",
                            "scope": "workspace",
                            "reason_codes": ["verification.command_classified", "verification.command_supported"]
                        }
                    }),
                },
                ReplayTapeEvent {
                    seq: 2,
                    event_type: "verification.event.recorded".to_owned(),
                    payload: json!({
                        "event_type": "verification.event.recorded",
                        "created_at_unix_ms": 1_100,
                        "reason_codes": ["verification.event_recorded", "verification.event_status_passed"],
                        "evidence_refs": ["tool_call:process-1"],
                        "event": {
                            "canonical_command": "cargo test --workspace",
                            "kind": "test",
                            "scope": "workspace",
                            "status": "passed",
                            "exit_code": 0,
                            "changed_paths": [r"C:\Users\Palo\Desktop\palyra-repo\palyra\src\lib.rs"],
                            "created_at_unix_ms": 1_100,
                            "evidence_refs": ["tool_call:process-1"],
                            "reason_codes": ["verification.event_recorded", "verification.event_status_passed"]
                        }
                    }),
                },
                ReplayTapeEvent {
                    seq: 3,
                    event_type: "verification.state.stale".to_owned(),
                    payload: json!({
                        "event_type": "verification.state.stale",
                        "created_at_unix_ms": 1_200,
                        "reason_codes": ["verification.required_after_patch", "verification.state_stale"],
                        "evidence_refs": ["workspace_patch:mutation-1"],
                        "state": {
                            "requirement": {
                                "requirement_id": "verification.required_after_patch.lint",
                                "required_kind": "lint",
                                "changed_paths": ["src/lib.rs"],
                                "min_created_at_unix_ms": 1_150,
                                "reason_code": "verification.required_after_patch"
                            },
                            "freshness": {
                                "status": "stale",
                                "reason_codes": ["verification.no_passing_evidence", "verification.state_stale"]
                            }
                        }
                    }),
                },
                ReplayTapeEvent {
                    seq: 4,
                    event_type: "code_intel.diagnostics.delta".to_owned(),
                    payload: json!({
                        "event": "code_intel.diagnostics.delta",
                        "new_errors": 1,
                        "new_warnings": 0,
                        "degraded": false,
                        "reason_codes": ["code_intel.diagnostics.new_errors"],
                        "evidence_refs": ["diagnostics:after_patch"]
                    }),
                },
                ReplayTapeEvent {
                    seq: 5,
                    event_type: "agent_loop.terminated".to_owned(),
                    payload: json!({
                        "event": "agent_loop.terminated",
                        "finalization": {
                            "verification_finalizer": {
                                "status": "nudge_required",
                                "reason_code": "verification.finalizer.stale_after_code_mutation",
                                "pending_requirement_count": 1,
                                "satisfied_requirement_count": 0,
                                "evidence_refs": ["verification_state:lint"],
                                "nudge": "Run lint before final answer."
                            }
                        }
                    }),
                },
            ],
        )
    }

    fn fixture_bundle_with_named_tool(
        tool_name: &str,
        tool_output: Value,
        mut extra_tape_events: Vec<ReplayTapeEvent>,
    ) -> ReplayBundle {
        extra_tape_events.extend([
            ReplayTapeEvent {
                seq: 10,
                event_type: "tool_proposal".to_owned(),
                payload: json!({
                    "proposal_id": "proposal-1",
                    "tool_name": tool_name,
                    "input": { "token": "secret-token" },
                }),
            },
            ReplayTapeEvent {
                seq: 11,
                event_type: "tool_result".to_owned(),
                payload: with_proposal_id(tool_output, "proposal-1"),
            },
            ReplayTapeEvent {
                seq: 12,
                event_type: "tool_attestation".to_owned(),
                payload: json!({
                    "proposal_id": "proposal-1",
                    "tool_name": tool_name,
                    "execution_sha256": "a".repeat(64),
                    "executor": "sandbox_tier_b",
                    "sandbox_enforcement": "preflight",
                    "execution_manifest": fixture_execution_manifest(),
                }),
            },
            ReplayTapeEvent {
                seq: 13,
                event_type: "final_answer".to_owned(),
                payload: json!({ "text": "done" }),
            },
        ]);
        build_replay_bundle(ReplayBundleBuildInput {
            generated_at_unix_ms: 1_700_000_000_000,
            source: ReplaySource {
                product: "palyra".to_owned(),
                run_id: "01BX5ZZKBKACTAV9WEVGEMMVRZ".to_owned(),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                origin_kind: "cli".to_owned(),
                schema_policy: "reject_future_schema_versions_additive_backward_compat".to_owned(),
            },
            capture: ReplayCaptureMetadata {
                captured_at_unix_ms: 1_700_000_000_000,
                capture_mode: "test".to_owned(),
                max_events_per_run: 128,
                truncated: false,
                inline_sections: vec!["run".to_owned(), "tape_events".to_owned()],
                referenced_sections: Vec::new(),
                warnings: Vec::new(),
            },
            run: ReplayRunSnapshot {
                state: "completed".to_owned(),
                principal: "operator".to_owned(),
                device_id: "desktop".to_owned(),
                channel: None,
                normalized_user_input: Some(json!({"prompt": "summarize safely"})),
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                last_error: None,
                parent_run_id: None,
                origin_run_id: None,
                parameter_delta: None,
            },
            config_snapshot: json!({
                "contract": { "format": "palyra incident replay bundle" },
                "tool_catalog": [tool_name],
            }),
            tape_events: extra_tape_events,
            lifecycle_transitions: Vec::new(),
            idempotency_records: Vec::new(),
            artifact_refs: Vec::new(),
        })
        .expect("fixture bundle should build")
    }

    fn with_proposal_id(mut value: Value, proposal_id: &str) -> Value {
        if let Some(object) = value.as_object_mut() {
            object.insert("proposal_id".to_owned(), Value::String(proposal_id.to_owned()));
        }
        value
    }

    fn fixture_execution_manifest() -> Value {
        json!({
            "schema_version": 1,
            "manifest_sha256": "f".repeat(64),
            "backend_id": "local_sandbox",
            "runner_id": "local_sandbox_runner",
            "runner_version": "v1",
            "workspace_strategy_digest": "1".repeat(64),
            "input_manifest_sha256": "2".repeat(64),
            "output_manifest_sha256": "3".repeat(64),
            "cleanup": {
                "strategy": "local_sandbox_process_lifecycle",
                "success": true,
                "reason_code": "local_sandbox.cleanup.ok",
                "resources": [
                    {
                        "kind": "process_tree",
                        "status": "foreground_process_reaped",
                        "cleanup_required": true,
                        "cleanup_verified": true
                    }
                ]
            },
            "egress_posture": "process_runner_egress:preflight"
        })
    }
}
