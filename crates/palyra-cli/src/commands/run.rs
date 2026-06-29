//! `palyra run`: run trajectory export for audit and eval workflows.

use crate::{commands::support_bundle, *};
use palyra_common::replay_bundle::{
    canonical_replay_bundle_bytes, replay_bundle_offline, ReplayBundle, ReplayRunStatus,
};
use serde_json::{json, Value};

/// Runs a `palyra run` subcommand.
///
/// # Errors
/// Returns an error when the journal cannot be read, the replay bundle fails
/// deterministic validation, or the requested artifact cannot be written.
pub(crate) fn run_run(command: RunCommand) -> Result<()> {
    match command {
        RunCommand::Export { run_id, output, format, redacted, journal_db, max_events } => {
            run_export(run_id, output, format, redacted, journal_db, max_events)
        }
    }
}

fn run_export(
    run_id: String,
    output: String,
    format: RunExportFormatArg,
    redacted: bool,
    journal_db: Option<String>,
    max_events: usize,
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
    let payload = build_run_export_payload(&bundle, format, redacted)?;
    let bytes = serde_json::to_vec_pretty(&payload).context("failed to encode run export")?;
    let output_path = PathBuf::from(output);
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
    std::io::stdout().flush().context("stdout flush failed")
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

    fn fixture_bundle() -> ReplayBundle {
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
                    payload: json!({
                        "proposal_id": "proposal-1",
                        "success": true,
                        "output": "done",
                        "attestation": { "execution_sha256": "a".repeat(64) },
                    }),
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
}
