//! `palyra run`: run trajectory export for audit and eval workflows.

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
        RunCommand::Export {
            run_id,
            output,
            format,
            redacted,
            journal_db,
            max_events,
            trajectory,
        } => run_export(run_id, output, format, redacted, journal_db, max_events, trajectory),
    }
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
    payload.clone()
}

fn trajectory_event_category(event_type: &str) -> &'static str {
    match event_type {
        "model_token" | "provider.stream.delta" | "provider.stream.completed" => "provider_stream",
        "tool_proposal" => "tool_proposal",
        "tool_result" | "tool_attestation" => "tool_output",
        "tool_decision" | "approval_request" | "approval_response" => "approval",
        value if value.contains("memory") || value.contains("recall") => "memory_recall",
        value if value.contains("compact") => "compaction_boundary",
        value if value.contains("subagent") || value.contains("delegation") => "subagent",
        value if value.contains("mcp") => "mcp_import",
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

fn build_run_export_payload(
    bundle: &ReplayBundle,
    format: RunExportFormatArg,
    redacted: bool,
) -> Result<Value> {
    let manifest = build_run_export_manifest(bundle, format, redacted)?;
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
        assert!(!text.contains("secret-token"));
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
                "tool_catalog": ["palyra.shell"],
            }),
            tape_events: vec![
                ReplayTapeEvent {
                    seq: 1,
                    event_type: "tool_proposal".to_owned(),
                    payload: json!({
                        "proposal_id": "proposal-1",
                        "tool_name": "palyra.shell",
                        "input": { "token": "secret-token" },
                    }),
                },
                ReplayTapeEvent {
                    seq: 2,
                    event_type: "tool_result".to_owned(),
                    payload: with_proposal_id(tool_output, "proposal-1"),
                },
                ReplayTapeEvent {
                    seq: 3,
                    event_type: "final_answer".to_owned(),
                    payload: json!({ "text": "done" }),
                },
            ],
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
}
