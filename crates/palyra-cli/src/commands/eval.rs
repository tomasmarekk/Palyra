//! `palyra eval`: local eval bundle creation for release and regression gates.

use crate::{commands::support_bundle, workflow_regression, *};
use palyra_common::replay_bundle::{
    canonical_replay_bundle_bytes, parse_replay_bundle, replay_bundle_offline, ReplayBundle,
    ReplayRunStatus,
};
use serde::Serialize;
use serde_json::{json, Value};

/// Runs a `palyra eval` subcommand.
///
/// # Errors
/// Returns an error when an input artifact cannot be read, a replay bundle
/// fails offline verification, or the output artifact cannot be written.
pub(crate) fn run_eval(command: EvalCommand) -> Result<()> {
    match command {
        EvalCommand::Bundle { command } => match command {
            EvalBundleCommand::Create {
                name,
                output,
                run_id,
                run_export,
                replay_bundle,
                scenario_manifest,
                memory_fixture,
                journal_db,
                max_events,
                fake_provider,
                json,
            } => run_bundle_create(BundleCreateRequest {
                name,
                output,
                run_ids: run_id,
                run_exports: run_export,
                replay_bundles: replay_bundle,
                scenario_manifest,
                memory_fixtures: memory_fixture,
                journal_db,
                max_events,
                fake_provider,
                json,
            }),
        },
    }
}

#[derive(Debug)]
struct BundleCreateRequest {
    name: String,
    output: String,
    run_ids: Vec<String>,
    run_exports: Vec<String>,
    replay_bundles: Vec<String>,
    scenario_manifest: Option<String>,
    memory_fixtures: Vec<String>,
    journal_db: Option<String>,
    max_events: usize,
    fake_provider: bool,
    json: bool,
}

#[derive(Debug, Serialize)]
struct EvalBundleArtifact {
    schema_version: u32,
    format: &'static str,
    name: String,
    generated_at_unix_ms: i64,
    manifest: EvalBundleManifest,
    inputs: EvalBundleInputs,
    expected_outcomes: Vec<EvalExpectedOutcome>,
    runner: EvalRunnerContract,
}

#[derive(Debug, Serialize)]
struct EvalBundleManifest {
    bundle_sha256: String,
    redaction_policy: &'static str,
    allowed_nondeterminism: Vec<&'static str>,
    required_capabilities: Vec<&'static str>,
}

#[derive(Debug, Default, Serialize)]
struct EvalBundleInputs {
    runs: Vec<EvalRunInput>,
    scenarios: Option<EvalScenarioInput>,
    memory_fixtures: Vec<EvalInputArtifact>,
}

#[derive(Debug, Serialize)]
struct EvalRunInput {
    source: String,
    replay_bundle_id: String,
    replay_bundle_sha256: String,
    run_state: String,
    tape_event_count: usize,
    tool_call_count: usize,
    approval_count: usize,
    artifact_ref_count: usize,
}

#[derive(Debug, Serialize)]
struct EvalScenarioInput {
    path: String,
    sha256: String,
    profiles: usize,
    scenarios: usize,
    required_subsystems: usize,
}

#[derive(Debug, Serialize)]
struct EvalInputArtifact {
    path: String,
    sha256: String,
    size_bytes: usize,
}

#[derive(Debug, Serialize)]
struct EvalExpectedOutcome {
    source: String,
    replay_bundle_id: String,
    offline_replay_status: &'static str,
    tape_event_count: usize,
    tool_outcomes: usize,
    approval_decisions: usize,
    final_answer_sha256: Option<String>,
}

#[derive(Debug, Serialize)]
struct EvalRunnerContract {
    offline_replay: EvalRunnerMode,
    fake_provider_replay: EvalRunnerMode,
    release_gate_statuses: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct EvalRunnerMode {
    enabled: bool,
    deterministic: bool,
    external_network: bool,
}

fn run_bundle_create(request: BundleCreateRequest) -> Result<()> {
    if request.max_events == 0 || request.max_events > 4_096 {
        anyhow::bail!("eval bundle create --max-events must be in range 1..=4096");
    }
    let mut replay_inputs = Vec::new();
    for run_id in &request.run_ids {
        let bundle = support_bundle::build_replay_bundle_from_journal(
            run_id.as_str(),
            request.journal_db.clone(),
            request.max_events,
        )?;
        replay_inputs.push((format!("journal:{run_id}"), bundle));
    }
    for path in &request.replay_bundles {
        replay_inputs.push((path.clone(), read_replay_bundle_path(path)?));
    }
    for path in &request.run_exports {
        replay_inputs.push((path.clone(), read_run_export_replay_bundle(path)?));
    }
    if replay_inputs.is_empty()
        && request.scenario_manifest.is_none()
        && request.memory_fixtures.is_empty()
    {
        anyhow::bail!(
            "eval bundle create requires at least one --run-id, --replay-bundle, --run-export, --scenario-manifest, or --memory-fixture"
        );
    }

    let scenario =
        request.scenario_manifest.as_deref().map(read_scenario_manifest_artifact).transpose()?;
    let memory_fixtures = request
        .memory_fixtures
        .iter()
        .map(|path| read_input_artifact(path))
        .collect::<Result<Vec<_>>>()?;
    let bundle = build_eval_bundle(
        request.name,
        replay_inputs.as_slice(),
        scenario,
        memory_fixtures,
        request.fake_provider,
        now_unix_ms_i64()?,
    )?;
    let encoded = serde_json::to_vec_pretty(&bundle).context("failed to encode eval bundle")?;
    let output_path = PathBuf::from(request.output);
    support_bundle::write_replay_artifact(output_path.as_path(), encoded.as_slice())?;
    if request.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "path": output_path.display().to_string(),
                "bundle_sha256": crate::sha256_hex(encoded.as_slice()),
                "run_inputs": bundle.inputs.runs.len(),
                "scenario_manifest": bundle.inputs.scenarios.is_some(),
                "memory_fixtures": bundle.inputs.memory_fixtures.len(),
            }))
            .context("failed to encode eval bundle summary")?
        );
    } else {
        println!(
            "eval.bundle.create path={} bundle_sha256={} run_inputs={} scenario_manifest={} memory_fixtures={}",
            output_path.display(),
            crate::sha256_hex(encoded.as_slice()),
            bundle.inputs.runs.len(),
            bundle.inputs.scenarios.is_some(),
            bundle.inputs.memory_fixtures.len(),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_eval_bundle(
    name: String,
    replay_inputs: &[(String, ReplayBundle)],
    scenario: Option<EvalScenarioInput>,
    memory_fixtures: Vec<EvalInputArtifact>,
    fake_provider: bool,
    generated_at_unix_ms: i64,
) -> Result<EvalBundleArtifact> {
    let mut runs = Vec::with_capacity(replay_inputs.len());
    let mut expected_outcomes = Vec::with_capacity(replay_inputs.len());
    for (source, bundle) in replay_inputs {
        let replay_report = replay_bundle_offline(bundle);
        if replay_report.status != ReplayRunStatus::Passed {
            anyhow::bail!(
                "replay bundle '{}' failed offline verification with {} diffs and {} validation issues",
                source,
                replay_report.diffs.len(),
                replay_report.validation.issues.len()
            );
        }
        let replay_bundle_sha256 = verified_replay_bundle_sha256(bundle)?;
        runs.push(EvalRunInput {
            source: source.clone(),
            replay_bundle_id: bundle.bundle_id.clone(),
            replay_bundle_sha256: replay_bundle_sha256.clone(),
            run_state: bundle.run.state.clone(),
            tape_event_count: bundle.tape_events.len(),
            tool_call_count: bundle.tool_exchanges.len(),
            approval_count: bundle.approvals.len(),
            artifact_ref_count: bundle.artifact_refs.len(),
        });
        expected_outcomes.push(EvalExpectedOutcome {
            source: source.clone(),
            replay_bundle_id: bundle.bundle_id.clone(),
            offline_replay_status: "passed",
            tape_event_count: bundle.expected.tape_event_count,
            tool_outcomes: bundle.expected.tool_outcomes.len(),
            approval_decisions: bundle.expected.approval_decisions.len(),
            final_answer_sha256: bundle.expected.final_answer_sha256.clone(),
        });
    }

    let mut bundle = EvalBundleArtifact {
        schema_version: 1,
        format: "palyra-eval-bundle",
        name,
        generated_at_unix_ms,
        manifest: EvalBundleManifest {
            bundle_sha256: String::new(),
            redaction_policy: "inputs_are_redacted_or_digest_only",
            allowed_nondeterminism: vec![
                "wall_clock_timestamps_normalized",
                "pseudonymized_identifiers",
                "fake_provider_replay_outputs",
            ],
            required_capabilities: vec![
                "offline_replay",
                "fake_provider_replay",
                "workflow_regression_matrix",
                "memory_fixture_digest",
            ],
        },
        inputs: EvalBundleInputs { runs, scenarios: scenario, memory_fixtures },
        expected_outcomes,
        runner: EvalRunnerContract {
            offline_replay: EvalRunnerMode {
                enabled: true,
                deterministic: true,
                external_network: false,
            },
            fake_provider_replay: EvalRunnerMode {
                enabled: fake_provider,
                deterministic: true,
                external_network: false,
            },
            release_gate_statuses: vec!["pass", "warn", "fail", "manual_review"],
        },
    };
    let digest_payload = serde_json::to_vec(&bundle).context("failed to digest eval bundle")?;
    bundle.manifest.bundle_sha256 = crate::sha256_hex(digest_payload.as_slice());
    Ok(bundle)
}

fn verified_replay_bundle_sha256(bundle: &ReplayBundle) -> Result<String> {
    let mut digest_bundle = bundle.clone();
    digest_bundle.integrity.canonical_sha256 = None;
    let canonical_bytes = canonical_replay_bundle_bytes(&digest_bundle)?;
    let canonical_sha256 = crate::sha256_hex(canonical_bytes.as_slice());
    if let Some(embedded) = bundle.integrity.canonical_sha256.as_deref() {
        if embedded != canonical_sha256 {
            anyhow::bail!("replay bundle canonical digest verification failed");
        }
    }
    Ok(canonical_sha256)
}

fn read_replay_bundle_path(path: &str) -> Result<ReplayBundle> {
    let bytes = fs::read(path).with_context(|| format!("failed to read replay bundle {path}"))?;
    parse_replay_bundle(bytes.as_slice())
}

fn read_run_export_replay_bundle(path: &str) -> Result<ReplayBundle> {
    let bytes = fs::read(path).with_context(|| format!("failed to read run export {path}"))?;
    let value: Value = serde_json::from_slice(bytes.as_slice())
        .with_context(|| format!("failed to parse run export {path}"))?;
    let replay = value
        .get("replay_bundle")
        .cloned()
        .with_context(|| format!("run export {path} does not contain a replay_bundle section"))?;
    serde_json::from_value(replay)
        .with_context(|| format!("failed to parse replay_bundle from run export {path}"))
}

fn read_scenario_manifest_artifact(path: &str) -> Result<EvalScenarioInput> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read scenario manifest {path}"))?;
    let manifest = workflow_regression::load_workflow_regression_manifest(Path::new(path))?;
    workflow_regression::validate_workflow_regression_manifest(&manifest)?;
    Ok(EvalScenarioInput {
        path: path.to_owned(),
        sha256: crate::sha256_hex(bytes.as_slice()),
        profiles: manifest.profiles.len(),
        scenarios: manifest.scenarios.len(),
        required_subsystems: manifest.required_subsystems.len(),
    })
}

fn read_input_artifact(path: &str) -> Result<EvalInputArtifact> {
    let bytes = fs::read(path).with_context(|| format!("failed to read eval input {path}"))?;
    Ok(EvalInputArtifact {
        path: path.to_owned(),
        sha256: crate::sha256_hex(bytes.as_slice()),
        size_bytes: bytes.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_common::replay_bundle::{
        build_replay_bundle, ReplayBundleBuildInput, ReplayCaptureMetadata, ReplayRunSnapshot,
        ReplaySource, ReplayTapeEvent,
    };

    #[test]
    fn eval_bundle_records_replay_expected_outcomes_and_digest_only_memory_fixture() {
        let replay = fixture_bundle();
        let memory_fixture = EvalInputArtifact {
            path: "memory-fixture.json".to_owned(),
            sha256: crate::sha256_hex(b"{\"token\":\"secret-token\"}"),
            size_bytes: 24,
        };

        let bundle = build_eval_bundle(
            "release-smoke".to_owned(),
            &[("fixture".to_owned(), replay)],
            None,
            vec![memory_fixture],
            true,
            1_700_000_000_000,
        )
        .expect("eval bundle should build");

        assert_eq!(bundle.format, "palyra-eval-bundle");
        assert_eq!(bundle.inputs.runs.len(), 1);
        assert_eq!(bundle.expected_outcomes.len(), 1);
        assert!(bundle.runner.offline_replay.enabled);
        assert!(bundle.runner.fake_provider_replay.enabled);
        assert!(!serde_json::to_string(&bundle).unwrap().contains("secret-token"));
        assert_eq!(
            bundle.runner.release_gate_statuses,
            vec!["pass", "warn", "fail", "manual_review"]
        );
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
            config_snapshot: json!({"contract": { "format": "palyra incident replay bundle" }}),
            tape_events: vec![ReplayTapeEvent {
                seq: 1,
                event_type: "final_answer".to_owned(),
                payload: json!({ "text": "done" }),
            }],
            lifecycle_transitions: Vec::new(),
            idempotency_records: Vec::new(),
            artifact_refs: Vec::new(),
        })
        .expect("fixture bundle should build")
    }
}
