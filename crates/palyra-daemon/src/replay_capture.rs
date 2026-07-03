//! Incident replay-bundle export: assembles a run's journal state (status
//! snapshot, ordered tape events, lifecycle transitions, idempotency records,
//! artifact references) into an offline-replayable [`ReplayBundle`].
//!
//! Determinism matters here: the same run must always export the same bundle,
//! so tape events are sorted by sequence before truncation. Secret redaction
//! is delegated to `build_replay_bundle` in `palyra_common::replay_bundle`;
//! this module only shapes journal rows into its input.

use anyhow::{Context, Result};
use palyra_common::replay_bundle::{
    build_replay_bundle, ReplayArtifactRef, ReplayBundle, ReplayBundleBuildInput,
    ReplayCaptureMetadata, ReplayRunSnapshot, ReplaySource, ReplayTapeEvent,
};
use palyra_common::runtime_contracts::ToolResultArtifactRef;
use serde_json::{json, Value};

use crate::{
    config::{FeatureRolloutsConfig, ReplayCaptureConfig},
    journal::{JournalStore, OrchestratorRunStatusSnapshot},
};

/// Inputs for one bundle export; `max_events` caps the tape so a runaway run
/// cannot produce an unbounded bundle (truncation is recorded as a warning).
pub(crate) struct IncidentReplayCaptureRequest<'a> {
    pub journal_store: &'a JournalStore,
    pub replay_capture: &'a ReplayCaptureConfig,
    pub feature_rollouts: &'a FeatureRolloutsConfig,
    pub run_id: &'a str,
    pub generated_at_unix_ms: i64,
    pub max_events: usize,
}

/// Exports one orchestrator run from the journal as a redacted, offline
/// replayable bundle.
///
/// # Errors
/// Fails when the run id is unknown or any journal read (snapshot, tape,
/// lifecycle, idempotency, tool-result artifacts) fails; each error carries
/// the run id as context.
pub(crate) fn capture_incident_replay_bundle(
    request: IncidentReplayCaptureRequest<'_>,
) -> Result<ReplayBundle> {
    let run = request
        .journal_store
        .orchestrator_run_status_snapshot(request.run_id)
        .with_context(|| format!("failed to load run snapshot for {}", request.run_id))?
        .with_context(|| format!("orchestrator run not found: {}", request.run_id))?;
    let mut tape = request
        .journal_store
        .orchestrator_tape(request.run_id)
        .with_context(|| format!("failed to load tape for {}", request.run_id))?;
    // Sort before truncating so the cap always keeps the earliest events;
    // relying on journal read order would make truncated bundles
    // non-deterministic.
    tape.sort_by_key(|event| event.seq);
    let truncated = tape.len() > request.max_events;
    tape.truncate(request.max_events);

    let tape_events = tape
        .into_iter()
        .map(|record| {
            // Malformed payload JSON is wrapped as a raw string instead of
            // failing the export: an incident bundle missing one event body
            // is far more useful than no bundle at all.
            let payload = serde_json::from_str::<Value>(record.payload_json.as_str())
                .unwrap_or_else(|_| json!({ "raw": record.payload_json }));
            ReplayTapeEvent { seq: record.seq, event_type: record.event_type, payload }
        })
        .collect::<Vec<_>>();
    let lifecycle_transitions = request
        .journal_store
        .list_run_lifecycle_events(request.run_id)
        .with_context(|| format!("failed to load lifecycle transitions for {}", request.run_id))?;
    let idempotency_records = request
        .journal_store
        .list_idempotency_records_for_run(request.run_id)
        .with_context(|| format!("failed to load idempotency records for {}", request.run_id))?;
    let mut artifact_refs = replay_artifact_refs(&run);
    artifact_refs.extend(
        request
            .journal_store
            .list_tool_result_artifacts_for_run(request.run_id)
            .with_context(|| {
                format!("failed to load tool result artifacts for {}", request.run_id)
            })?
            .iter()
            .map(replay_tool_result_artifact_ref),
    );

    build_replay_bundle(ReplayBundleBuildInput {
        generated_at_unix_ms: request.generated_at_unix_ms,
        source: ReplaySource {
            product: "palyra".to_owned(),
            run_id: run.run_id.clone(),
            session_id: Some(run.session_id.clone()),
            origin_kind: run.origin_kind.clone(),
            schema_policy: "reject_future_schema_versions_additive_backward_compat".to_owned(),
        },
        capture: ReplayCaptureMetadata {
            captured_at_unix_ms: request.generated_at_unix_ms,
            capture_mode: "daemon_journal_export".to_owned(),
            max_events_per_run: request.max_events,
            truncated,
            inline_sections: vec![
                "run".to_owned(),
                "config_snapshot".to_owned(),
                "tape_events".to_owned(),
                "tool_exchanges".to_owned(),
                "http_exchanges".to_owned(),
                "approvals".to_owned(),
                "expected".to_owned(),
            ],
            referenced_sections: vec![
                "large_binary_artifacts".to_owned(),
                "workspace_files".to_owned(),
                "journal_events_outside_run".to_owned(),
            ],
            warnings: if truncated {
                vec![format!(
                    "tape truncated at {} events for replay bundle export",
                    request.max_events
                )]
            } else {
                Vec::new()
            },
        },
        run: replay_run_snapshot(&run),
        config_snapshot: replay_config_snapshot(request.replay_capture, request.feature_rollouts),
        tape_events,
        lifecycle_transitions,
        idempotency_records,
        artifact_refs,
    })
}

fn replay_run_snapshot(run: &OrchestratorRunStatusSnapshot) -> ReplayRunSnapshot {
    ReplayRunSnapshot {
        state: run.state.clone(),
        principal: run.principal.clone(),
        device_id: run.device_id.clone(),
        channel: run.channel.clone(),
        normalized_user_input: extract_normalized_user_input(run.parameter_delta_json.as_deref()),
        prompt_tokens: run.prompt_tokens,
        completion_tokens: run.completion_tokens,
        total_tokens: run.total_tokens,
        last_error: run.last_error.clone(),
        parent_run_id: run.parent_run_id.clone(),
        origin_run_id: run.origin_run_id.clone(),
        parameter_delta: run
            .parameter_delta_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok()),
    }
}

fn extract_normalized_user_input(parameter_delta_json: Option<&str>) -> Option<Value> {
    let value = parameter_delta_json.and_then(|raw| serde_json::from_str::<Value>(raw).ok())?;
    value.get("user_input").or_else(|| value.get("input")).or_else(|| value.get("prompt")).cloned()
}

fn replay_config_snapshot(
    replay_capture: &ReplayCaptureConfig,
    feature_rollouts: &FeatureRolloutsConfig,
) -> Value {
    json!({
        "replay_capture": {
            "mode": replay_capture.mode.as_str(),
            "capture_runtime_decisions": replay_capture.capture_runtime_decisions,
            "max_events_per_run": replay_capture.max_events_per_run,
        },
        "feature_rollouts": {
            "replay_capture": {
                "enabled": feature_rollouts.replay_capture.enabled,
                "source": feature_rollouts.replay_capture.source,
            },
            "auxiliary_executor": {
                "enabled": feature_rollouts.auxiliary_executor.enabled,
                "source": feature_rollouts.auxiliary_executor.source,
            },
            "flow_orchestration": {
                "enabled": feature_rollouts.flow_orchestration.enabled,
                "source": feature_rollouts.flow_orchestration.source,
            },
        },
        "network_policy": {
            "offline_replay_requires_live_network": false,
            "offline_replay_requires_live_provider": false,
        },
        "mcp": {
            "offline_replay_requires_live_server": false,
            "capture_discovery_snapshots": true,
            "capture_tool_import_snapshots": true,
        },
    })
}

fn replay_artifact_refs(run: &OrchestratorRunStatusSnapshot) -> Vec<ReplayArtifactRef> {
    let mut refs = Vec::new();
    if let Some(delegation) = run.delegation.as_ref() {
        refs.push(ReplayArtifactRef {
            artifact_id: format!("delegation:{}", run.run_id),
            kind: "delegation_snapshot".to_owned(),
            reference: format!("journal://orchestrator_runs/{}/delegation_json", run.run_id),
            sha256: serde_json::to_vec(delegation).ok().map(|bytes| sha256_hex(bytes.as_slice())),
            size_bytes: serde_json::to_vec(delegation)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok()),
        });
    }
    if let Some(merge_result) = run.merge_result.as_ref() {
        refs.push(ReplayArtifactRef {
            artifact_id: format!("merge:{}", run.run_id),
            kind: "delegation_merge_result".to_owned(),
            reference: format!("journal://orchestrator_runs/{}/merge_result_json", run.run_id),
            sha256: serde_json::to_vec(merge_result).ok().map(|bytes| sha256_hex(bytes.as_slice())),
            size_bytes: serde_json::to_vec(merge_result)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok()),
        });
    }
    refs
}

fn replay_tool_result_artifact_ref(artifact: &ToolResultArtifactRef) -> ReplayArtifactRef {
    ReplayArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: "tool_result".to_owned(),
        reference: format!(
            "tool-result-artifact://{}/{}",
            artifact.storage_backend, artifact.artifact_id
        ),
        sha256: Some(artifact.digest_sha256.clone()),
        size_bytes: Some(artifact.size_bytes),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use palyra_common::replay_bundle::{replay_bundle_offline, ReplayRunStatus};
    use serde_json::json;

    use super::*;
    use crate::{
        config::{FeatureRolloutsConfig, ReplayCaptureConfig},
        journal::{
            JournalConfig, OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest,
            OrchestratorTapeAppendRequest,
        },
    };

    #[test]
    fn capture_incident_replay_bundle_exports_redacted_offline_replayable_run() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let store = JournalStore::open(JournalConfig {
            db_path: temp.path().join("journal.sqlite3"),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 1_000,
        })
        .expect("journal should open");
        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1".to_owned(),
                session_key: "session:replay-test".to_owned(),
                session_label: Some("Replay test".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "device:local".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("session should be created");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1".to_owned(),
                origin_kind: "run_stream".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:ops".to_owned()),
                parameter_delta_json: Some(
                    json!({ "user_input": { "text": "call https://example.test?token=secret" } })
                        .to_string(),
                ),
            })
            .expect("run should start");
        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned(),
                seq: 0,
                event_type: "tool_proposal".to_owned(),
                payload_json: json!({
                    "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FA3",
                    "tool_name": "palyra.http.fetch",
                    "input_json": {
                        "url": "https://example.test/callback?access_token=raw&mode=ok",
                        "headers": { "authorization": "Bearer raw" }
                    }
                })
                .to_string(),
            })
            .expect("proposal should append");
        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned(),
                seq: 1,
                event_type: "tool_result".to_owned(),
                payload_json: json!({
                    "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FA3",
                    "success": true,
                    "output_json": { "status": 200 },
                    "error": ""
                })
                .to_string(),
            })
            .expect("result should append");

        let bundle = capture_incident_replay_bundle(IncidentReplayCaptureRequest {
            journal_store: &store,
            replay_capture: &ReplayCaptureConfig::default(),
            feature_rollouts: &FeatureRolloutsConfig::default(),
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            generated_at_unix_ms: 1_730_000_000_000,
            max_events: 128,
        })
        .expect("bundle should capture");
        let encoded = serde_json::to_string(&bundle).expect("bundle should serialize");
        assert!(!encoded.contains("access_token=raw"));
        assert!(!encoded.contains("Bearer raw"));
        assert_eq!(
            bundle.config_snapshot.pointer("/mcp/offline_replay_requires_live_server"),
            Some(&json!(false))
        );
        assert_eq!(
            bundle.config_snapshot.pointer("/mcp/capture_tool_import_snapshots"),
            Some(&json!(true))
        );
        assert_eq!(replay_bundle_offline(&bundle).status, ReplayRunStatus::Passed);
    }
}
