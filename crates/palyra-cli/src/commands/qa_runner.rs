//! Black-box QA scenario execution over an isolated `palyrad` process.
//!
//! The runner coordinates production transports and persistence; it does not
//! implement a second agent loop. Each fixture scenario receives a fresh
//! daemon, state root, principal, session, and workspace before the existing
//! evidence engine evaluates observations collected from that runtime.

mod observations;
mod process;

use std::{
    fmt::Write as _,
    fs,
    io::Write as IoWrite,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result};
use palyra_common::{
    qa_evidence::{build_qa_evidence_bundle, QaArtifactEvidence, QaEvidenceBundle},
    qa_scenarios::{QaScenarioArtifactKind, QaScenarioManifest, QaScenarioRunnerMode},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

#[cfg(test)]
use serde_json::{json, Value};

use self::{
    observations::collect_scenario_observations,
    process::{QaDaemonSandbox, QaDaemonShutdown},
};

const EXECUTION_RESULT_SCHEMA_VERSION: u32 = 1;
const EXECUTION_RESULT_FORMAT: &str = "palyra-qa-scenario-execution-result";

/// Compact artifact provenance retained by the aggregate gate report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QaExecutionArtifactRef {
    pub(crate) path: String,
    pub(crate) kind: String,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
}

/// Explicit teardown outcome for the isolated scenario runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QaScenarioCleanupResult {
    pub(crate) run_terminal_observed: bool,
    pub(crate) session_cleaned: bool,
    pub(crate) daemon_terminated: bool,
    pub(crate) workspace_removed: bool,
    pub(crate) verified: bool,
    pub(crate) reason_codes: Vec<String>,
}

/// Durable, bounded result for one real QA scenario execution.
///
/// Transcript and tape payloads deliberately remain in the separately stored,
/// redacted evidence bundle. This descriptor carries only opaque runtime IDs,
/// stable reason codes, artifact digests, and cleanup state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QaScenarioExecutionResult {
    pub(crate) schema_version: u32,
    pub(crate) format: String,
    pub(crate) execution_id: String,
    pub(crate) scenario_id: String,
    pub(crate) runner_mode: String,
    pub(crate) verdict: String,
    pub(crate) reason_codes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_state: Option<String>,
    pub(crate) evidence_artifacts: Vec<QaExecutionArtifactRef>,
    pub(crate) cleanup: QaScenarioCleanupResult,
}

/// Aggregate-report projection of a scenario result plus its descriptor file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QaScenarioExecutionReport {
    #[serde(flatten)]
    pub(crate) result: QaScenarioExecutionResult,
    pub(crate) result_artifact: QaExecutionArtifactRef,
}

/// Returns the pinned on-disk execution-result contract used by QA tooling.
#[cfg(test)]
pub(crate) fn qa_scenario_execution_result_schema_snapshot() -> Value {
    json!({
        "schema_version": EXECUTION_RESULT_SCHEMA_VERSION,
        "format": EXECUTION_RESULT_FORMAT,
        "artifact_reference_base": "qa_gate_report.artifact_reference_base",
        "required_fields": [
            "schema_version",
            "format",
            "execution_id",
            "scenario_id",
            "runner_mode",
            "verdict",
            "reason_codes",
            "evidence_artifacts",
            "cleanup"
        ],
        "optional_runtime_fields": ["run_id", "session_id", "terminal_state"],
        "artifact_reference": {
            "path": "relative_no_parent_components",
            "sha256": "lowercase_hex_64",
            "size_bytes": "u64",
            "write_policy": "create_new_no_overwrite"
        },
        "cleanup_fields": [
            "run_terminal_observed",
            "session_cleaned",
            "daemon_terminated",
            "workspace_removed",
            "verified",
            "reason_codes"
        ],
        "excluded_payloads": ["raw_transcript", "raw_tape", "provider_secrets", "absolute_paths"]
    })
}

/// Executes one fixture-backed scenario through a fresh production daemon.
///
/// # Errors
/// Returns an error when the manifest is not a fixture runner, the child
/// daemon cannot be isolated or reached, the console stream is malformed, or
/// evidence/result artifacts cannot be persisted. The process guard still
/// terminates the child and removes the temporary root on every error path.
pub(crate) async fn execute_fixture_scenario(
    manifest: &QaScenarioManifest,
    artifact_root: &Path,
) -> Result<QaScenarioExecutionReport> {
    if manifest.mode.runner != QaScenarioRunnerMode::Fixture {
        anyhow::bail!(
            "qa.runner.unsupported_mode: fixture executor cannot run {}",
            manifest.mode.runner.as_str()
        );
    }
    let runner = manifest.runner.as_ref().ok_or_else(|| {
        anyhow::anyhow!("qa.runner.missing_config: fixture scenario has no runner config")
    })?;
    let repository_root = fs::canonicalize(
        std::env::current_dir().context(
            "qa.runner.repository_root_unavailable: failed to resolve current directory",
        )?,
    )
    .context("qa.runner.repository_root_unavailable: failed to canonicalize current directory")?;
    let provider_fixture = resolve_runner_path(
        repository_root.as_path(),
        runner.provider_fixture.as_str(),
        "provider fixture",
    )?;
    let workspace_fixture = runner
        .workspace_fixture
        .as_deref()
        .map(|path| resolve_runner_path(repository_root.as_path(), path, "workspace fixture"))
        .transpose()?;

    let execution_id = Ulid::new().to_string();
    let evidence_path =
        scenario_artifact_path(execution_id.as_str(), manifest.id.as_str(), "evidence.json");
    let result_path =
        scenario_artifact_path(execution_id.as_str(), manifest.id.as_str(), "result.json");
    let mut sandbox =
        QaDaemonSandbox::spawn(manifest, provider_fixture.as_path(), workspace_fixture.as_deref())?;
    let observation_result = collect_scenario_observations(manifest, &mut sandbox).await;
    let terminal_observed =
        observation_result.as_ref().is_ok_and(|observations| observations.terminal_observed);
    let run_id = observation_result
        .as_ref()
        .ok()
        .map(|observations| observations.run_id.clone())
        .or_else(|| sandbox.active_run_id().map(str::to_owned));
    let session_id = observation_result
        .as_ref()
        .ok()
        .map(|observations| observations.session_id.clone())
        .or_else(|| sandbox.active_session_id().map(str::to_owned));
    let terminal_state =
        observation_result.as_ref().ok().map(|observations| observations.terminal_state.clone());

    // Teardown is deliberately performed before any fallible artifact write,
    // so a full disk or invalid destination cannot strand the child runtime.
    let session_cleaned = sandbox.cleanup_active_session().await;
    let shutdown = sandbox.shutdown();
    let cleanup = cleanup_result(terminal_observed, session_cleaned, shutdown);
    let mut reason_codes = cleanup.reason_codes.clone();
    let mut evidence_artifacts = Vec::new();
    let mut evidence_passed = false;
    match observation_result {
        Ok(observations) => {
            let mut evidence_input = observations.evidence;
            let logical_evidence_path = manifest
                .artifacts
                .iter()
                .find(|artifact| artifact.kind == QaScenarioArtifactKind::Evidence)
                .map(|artifact| artifact.path.clone())
                .unwrap_or_else(|| display_path_slash(evidence_path.as_path()));
            evidence_input.artifacts.push(QaArtifactEvidence {
                path: logical_evidence_path,
                kind: QaScenarioArtifactKind::Evidence.as_str().to_owned(),
                present: true,
                // The evidence builder normalizes hashes in its serialized
                // artifact index, so this placeholder avoids a self-hash cycle.
                sha256: Some("0".repeat(64)),
                size_bytes: None,
            });
            let evidence = build_qa_evidence_bundle(manifest, evidence_input);
            reason_codes.extend(evidence_reason_codes(&evidence));
            evidence_passed = evidence.summary.verdict.as_str() == "passed";
            match write_json_artifact(
                artifact_root,
                evidence_path.as_path(),
                QaScenarioArtifactKind::Evidence.as_str(),
                &evidence,
            ) {
                Ok(reference) => evidence_artifacts.push(reference),
                Err(error) => {
                    evidence_passed = false;
                    reason_codes.push(stable_runner_error_code(&error));
                }
            }
        }
        Err(error) => reason_codes.push(stable_runner_error_code(&error)),
    }
    reason_codes.sort();
    reason_codes.dedup();
    let verdict = if evidence_passed && cleanup.verified { "passed" } else { "failed" };
    let result = QaScenarioExecutionResult {
        schema_version: EXECUTION_RESULT_SCHEMA_VERSION,
        format: EXECUTION_RESULT_FORMAT.to_owned(),
        execution_id,
        scenario_id: manifest.id.clone(),
        runner_mode: manifest.mode.runner.as_str().to_owned(),
        verdict: verdict.to_owned(),
        reason_codes,
        run_id,
        session_id,
        terminal_state,
        evidence_artifacts,
        cleanup,
    };
    validate_execution_result(&result)?;
    let result_artifact =
        write_json_artifact(artifact_root, result_path.as_path(), "execution_result", &result)?;
    Ok(QaScenarioExecutionReport { result, result_artifact })
}

fn validate_execution_result(result: &QaScenarioExecutionResult) -> Result<()> {
    if result.schema_version != EXECUTION_RESULT_SCHEMA_VERSION
        || result.format != EXECUTION_RESULT_FORMAT
        || result.execution_id.trim().is_empty()
        || result.scenario_id.trim().is_empty()
        || !matches!(result.verdict.as_str(), "passed" | "failed")
    {
        anyhow::bail!("qa.runner.execution_result_invalid");
    }
    if result.verdict == "passed"
        && (result.run_id.is_none()
            || result.session_id.is_none()
            || result.terminal_state.is_none()
            || result.evidence_artifacts.is_empty()
            || !result.cleanup.verified)
    {
        anyhow::bail!("qa.runner.execution_result_incomplete");
    }
    for reference in &result.evidence_artifacts {
        let path = Path::new(reference.path.as_str());
        if path.is_absolute()
            || path.components().any(|component| !matches!(component, Component::Normal(_)))
            || reference.sha256.len() != 64
            || !reference.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            anyhow::bail!("qa.runner.execution_artifact_reference_invalid");
        }
    }
    Ok(())
}

fn resolve_runner_path(root: &Path, relative: &str, label: &str) -> Result<PathBuf> {
    let path = root.join(relative);
    let canonical = fs::canonicalize(path.as_path()).with_context(|| {
        format!("qa.runner.fixture_unavailable: failed to resolve {label} {relative}")
    })?;
    if !canonical.starts_with(root) {
        anyhow::bail!("qa.runner.fixture_outside_repository: {label} must stay inside repository");
    }
    Ok(canonical)
}

fn scenario_artifact_path(execution_id: &str, scenario_id: &str, filename: &str) -> PathBuf {
    let stem = scenario_id
        .chars()
        .take(64)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let digest = sha256_hex(scenario_id.as_bytes());
    PathBuf::from("executions")
        .join(execution_id)
        .join(format!("{stem}-{}", &digest[..12]))
        .join(filename)
}

fn write_json_artifact<T: Serialize>(
    root: &Path,
    relative_path: &Path,
    kind: &str,
    value: &T,
) -> Result<QaExecutionArtifactRef> {
    if relative_path.is_absolute()
        || relative_path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        anyhow::bail!("qa.runner.artifact_path_invalid");
    }
    let path = root.join(relative_path);
    let bytes = serde_json::to_vec_pretty(value).context("qa.runner.artifact_encode_failed")?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("qa.runner.artifact_directory_create_failed: {}", parent.display())
        })?;
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path.as_path())
        .with_context(|| format!("qa.runner.artifact_create_failed: {}", path.display()))?;
    file.write_all(bytes.as_slice())
        .with_context(|| format!("qa.runner.artifact_write_failed: {}", path.display()))?;
    Ok(QaExecutionArtifactRef {
        path: display_path_slash(relative_path),
        kind: kind.to_owned(),
        sha256: sha256_hex(bytes.as_slice()),
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().fold(String::with_capacity(64), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    })
}

fn stable_runner_error_code(error: &anyhow::Error) -> String {
    error
        .chain()
        .filter_map(|cause| cause.to_string().split(':').next().map(str::trim).map(str::to_owned))
        .find(|candidate| {
            candidate.starts_with("qa.runner.")
                && candidate.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
                })
        })
        .unwrap_or_else(|| "qa.runner.execution_failed".to_owned())
}

fn evidence_reason_codes(evidence: &QaEvidenceBundle) -> Vec<String> {
    let mut codes = evidence
        .checks
        .iter()
        .flat_map(|check| check.issues.iter().map(|issue| issue.code.clone()))
        .collect::<Vec<_>>();
    codes.sort();
    codes.dedup();
    if codes.is_empty() {
        codes.push("qa.runner.assertions_passed".to_owned());
    }
    codes
}

fn cleanup_result(
    terminal_observed: bool,
    session_cleaned: bool,
    shutdown: QaDaemonShutdown,
) -> QaScenarioCleanupResult {
    let mut reason_codes = Vec::new();
    if !terminal_observed {
        reason_codes.push("qa.runner.terminal_not_observed".to_owned());
    }
    if !session_cleaned {
        reason_codes.push("qa.runner.session_cleanup_failed".to_owned());
    }
    if !shutdown.daemon_terminated {
        reason_codes.push("qa.runner.daemon_termination_failed".to_owned());
    }
    if !shutdown.workspace_removed {
        reason_codes.push("qa.runner.workspace_cleanup_failed".to_owned());
    }
    // Runtime terminalization is reported independently: a stream failure can
    // hide the terminal event while session/process/workspace teardown still
    // completes and must remain auditable as verified cleanup.
    let verified = session_cleaned && shutdown.daemon_terminated && shutdown.workspace_removed;
    if verified {
        reason_codes.push("qa.runner.cleanup_verified".to_owned());
    }
    QaScenarioCleanupResult {
        run_terminal_observed: terminal_observed,
        session_cleaned,
        daemon_terminated: shutdown.daemon_terminated,
        workspace_removed: shutdown.workspace_removed,
        verified,
        reason_codes,
    }
}

fn display_path_slash(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn artifact_paths_are_execution_unique_and_collision_resistant() {
        let dotted = scenario_artifact_path("execution-a", "qa.a", "evidence.json");
        let underscored = scenario_artifact_path("execution-a", "qa_a", "evidence.json");
        let repeated = scenario_artifact_path("execution-b", "qa.a", "evidence.json");

        assert_ne!(dotted, underscored);
        assert_ne!(dotted, repeated);
        assert!(display_path_slash(dotted.as_path()).starts_with("executions/execution-a/qa.a-"));
    }

    #[test]
    fn artifact_reference_is_relative_and_matches_persisted_hash() {
        let root = tempfile::tempdir().expect("artifact root should be available");
        let relative = scenario_artifact_path("execution-a", "qa.absolute", "result.json");

        let reference = write_json_artifact(
            root.path(),
            relative.as_path(),
            "execution_result",
            &json!({"status": "passed"}),
        )
        .expect("artifact should be persisted");
        let persisted = fs::read(root.path().join(reference.path.as_str()))
            .expect("referenced artifact should remain readable");

        assert!(!Path::new(reference.path.as_str()).is_absolute());
        assert!(!reference.path.contains(root.path().to_string_lossy().as_ref()));
        assert_eq!(reference.sha256, sha256_hex(persisted.as_slice()));
        assert!(write_json_artifact(
            root.path(),
            relative.as_path(),
            "execution_result",
            &json!({"status": "overwritten"}),
        )
        .is_err());
    }

    #[test]
    fn execution_result_round_trips_without_raw_evidence_payloads() {
        let result = QaScenarioExecutionResult {
            schema_version: EXECUTION_RESULT_SCHEMA_VERSION,
            format: EXECUTION_RESULT_FORMAT.to_owned(),
            execution_id: "01ARZ3NDEKTSV4RRFFQ69G5FAT".to_owned(),
            scenario_id: "qa.result".to_owned(),
            runner_mode: "fixture".to_owned(),
            verdict: "failed".to_owned(),
            reason_codes: vec!["qa.runner.run_timeout".to_owned()],
            run_id: None,
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAU".to_owned()),
            terminal_state: None,
            evidence_artifacts: Vec::new(),
            cleanup: QaScenarioCleanupResult {
                run_terminal_observed: false,
                session_cleaned: true,
                daemon_terminated: true,
                workspace_removed: true,
                verified: false,
                reason_codes: vec!["qa.runner.terminal_not_observed".to_owned()],
            },
        };

        let value = serde_json::to_value(&result).expect("result should serialize");
        let decoded: QaScenarioExecutionResult =
            serde_json::from_value(value.clone()).expect("result should deserialize");

        assert_eq!(decoded, result);
        assert!(value.get("transcript").is_none());
        assert!(value.get("tape_events").is_none());
    }

    #[test]
    fn execution_result_schema_matches_golden() {
        let golden: Value = serde_json::from_str(include_str!(
            "../../../../fixtures/golden/qa_scenario_execution_result_schema.json"
        ))
        .expect("execution result schema golden should parse");

        assert_eq!(qa_scenario_execution_result_schema_snapshot(), golden);
    }

    #[test]
    fn cleanup_verification_is_independent_from_terminal_observation() {
        let cleanup = cleanup_result(
            false,
            true,
            QaDaemonShutdown { daemon_terminated: true, workspace_removed: true },
        );

        assert!(cleanup.verified);
        assert!(cleanup.reason_codes.iter().any(|code| code == "qa.runner.cleanup_verified"));
        assert!(cleanup.reason_codes.iter().any(|code| code == "qa.runner.terminal_not_observed"));
    }
}
