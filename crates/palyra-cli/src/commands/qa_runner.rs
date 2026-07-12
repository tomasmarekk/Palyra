//! Black-box QA scenario execution over an isolated `palyrad` process.
//!
//! The runner coordinates production transports and persistence; it does not
//! implement a second agent loop. Each fixture scenario receives a fresh
//! daemon, state root, principal, session, and workspace before the existing
//! evidence engine evaluates observations collected from that runtime.

mod observations;
mod process;
mod runtime_path;

use std::{
    collections::BTreeSet,
    env,
    fmt::Write as _,
    fs,
    io::{self, Read, Write as IoWrite},
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result};
use palyra_auth::{AuthProfileRecord, AuthProfileRegistry, AuthProviderKind};
use palyra_common::{
    build_metadata, default_identity_store_root,
    qa_evidence::{
        build_qa_evidence_bundle, qa_fault_injection_evidence_from_sidecar, QaEvidenceBundle,
    },
    qa_fault_injection::{
        QaFaultAction, QaFaultEvidenceSidecarRecord, QaFaultInjectionPlan,
        QA_FAULT_TERMINATE_EXIT_CODE,
    },
    qa_runtime_path::{
        qa_live_provider_base_url_sha256, qa_live_provider_binding_sha256,
        qa_provider_binding_sha256, ProviderLiveBindingMetadata, RuntimePathEvidence,
        QA_PROVIDER_FIXTURE_MATERIALIZATION, QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION,
    },
    qa_scenarios::{
        QaScenarioArtifactKind, QaScenarioLiveProviderKind, QaScenarioManifest,
        QaScenarioRunnerMode,
    },
    redaction::{is_sensitive_key, redact_diagnostic_text},
    runtime_contracts::PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
};
use palyra_model_providers::{
    parse_qa_mock_provider_fixture_yaml, ModelProviderAuthProviderKind, ModelProviderConfig,
    ModelProviderKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::commands;

#[cfg(test)]
use palyra_common::qa_runtime_path::RuntimePathComponentEvidence;
#[cfg(test)]
use serde_json::json;

use self::{
    observations::{
        collect_recovered_scenario_observations, collect_scenario_observations, QaRunDeadline,
        QaScenarioObservations,
    },
    process::{QaDaemonSandbox, QaDaemonShutdown, QaFailureDiagnostics},
    runtime_path::{extract_runtime_path_evidence, RuntimePathExtractionInput},
};

const EXECUTION_RESULT_SCHEMA_VERSION: u32 = 3;
const EXECUTION_RESULT_FORMAT: &str = "palyra-qa-scenario-execution-result";
const EXECUTION_KEY_SCHEMA_VERSION: u32 = 1;
const EXECUTION_KEY_FORMAT: &str = "palyra-qa-scenario-execution-key";
pub(crate) const QA_RUNNER_CONTRACT_VERSION: &str = "qa-runner.v4";
const QA_DAEMON_BIN_ENV: &str = "PALYRA_QA_PALYRAD_BIN";
const MAX_EVIDENCE_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_FAILURE_DIAGNOSTICS_ARTIFACT_BYTES: usize = 4 * 1024 * 1024;
const QA_LIVE_MATERIALIZED_PROFILE_ID: &str = "qa-live-selected";

/// Content-addressed identity for one scenario/runtime/provider binding.
///
/// Every field is safe to persist. Live credential values never participate;
/// only a digest of selected public profile metadata is retained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QaScenarioExecutionKey {
    pub(crate) schema_version: u32,
    pub(crate) format: String,
    pub(crate) digest: String,
    pub(crate) normalized_manifest_sha256: String,
    pub(crate) fixture_set_sha256: String,
    pub(crate) runtime_version: String,
    pub(crate) runtime_contract_version: String,
    pub(crate) runner_version: String,
    pub(crate) provider_lane: String,
    pub(crate) provider_binding_sha256: String,
}

impl QaScenarioExecutionKey {
    pub(crate) fn validate_shape(&self) -> Result<()> {
        if self.schema_version != EXECUTION_KEY_SCHEMA_VERSION
            || self.format != EXECUTION_KEY_FORMAT
            || self.runtime_version.trim().is_empty()
            || self.runtime_contract_version.trim().is_empty()
            || self.runner_version.trim().is_empty()
            || !matches!(self.provider_lane.as_str(), "fixture" | "record_replay" | "live")
        {
            anyhow::bail!("qa.runner.execution_key_invalid");
        }
        for digest in [
            self.digest.as_str(),
            self.normalized_manifest_sha256.as_str(),
            self.fixture_set_sha256.as_str(),
            self.provider_binding_sha256.as_str(),
        ] {
            if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                anyhow::bail!("qa.runner.execution_key_invalid");
            }
        }
        Ok(())
    }
}

/// Compact artifact provenance retained by the aggregate gate report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QaExecutionArtifactRef {
    pub(crate) path: String,
    pub(crate) kind: String,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
}

/// Manifest-facing alias bound to one physically persisted evidence artifact.
///
/// The alias is never presented as a filesystem path containing the physical
/// bytes. Its nested reference carries the only digest of the immutable file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QaEvidenceOutputBinding {
    /// Manifest-declared logical output name; it is not read as a physical file.
    pub(crate) logical_alias: String,
    /// Immutable artifact that contains the bytes and their exact digest.
    pub(crate) artifact: QaExecutionArtifactRef,
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

/// Attempt-scoped runtime provenance safe for durable reports and checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QaScenarioAttemptProvenance {
    pub(crate) generation: u64,
    pub(crate) runner_version: String,
    pub(crate) runtime_version: String,
    pub(crate) runtime_contract_version: String,
    pub(crate) palyrad_binary_sha256: String,
    pub(crate) palyrad_version: String,
    pub(crate) palyrad_git_hash: String,
    pub(crate) palyrad_build_profile: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_result_artifact: Option<QaExecutionArtifactRef>,
}

/// Parent-issued execution request; workers cannot choose attempt identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QaScenarioExecutionRequest {
    pub(crate) execution_key: QaScenarioExecutionKey,
    pub(crate) execution_id: String,
    pub(crate) attempt_generation: u64,
    pub(crate) runner_version: String,
    pub(crate) previous_result_artifact: Option<QaExecutionArtifactRef>,
}

/// Fully resolved, immutable execution inputs prepared by the parent.
// INTENTIONAL: no `Debug`; live bindings retain a credential-reference profile in memory.
pub(crate) struct QaPreparedScenarioExecution {
    pub(crate) execution_key: QaScenarioExecutionKey,
    pub(crate) runner_version: String,
    pub(crate) runtime_version: String,
    pub(crate) runtime_contract_version: String,
    pub(crate) expected_palyrad_git_hash: String,
    pub(crate) palyrad_binary: PathBuf,
    pub(crate) palyrad_binary_sha256: String,
    repository_root: PathBuf,
    fixture_paths: Vec<String>,
    workspace_fixture: Option<String>,
    binding: QaPreparedRunnerBinding,
}

// INTENTIONAL: no `Debug`; the live variant contains vault references from a selected profile.
enum QaPreparedRunnerBinding {
    Fixture { provider_fixture: String },
    RecordReplay { replay_fixture: String },
    Live(Box<QaPreparedLiveBinding>),
}

// INTENTIONAL: no `Debug`; auth profiles contain credential references that diagnostics omit.
struct QaPreparedLiveBinding {
    profile: AuthProfileRecord,
    provider_kind: QaScenarioLiveProviderKind,
    auth_provider_kind: String,
    model: String,
    base_url: Option<String>,
}

impl QaPreparedScenarioExecution {
    pub(crate) fn provider_lane(&self) -> &'static str {
        match &self.binding {
            QaPreparedRunnerBinding::Fixture { .. } => "fixture",
            QaPreparedRunnerBinding::RecordReplay { .. } => "record_replay",
            QaPreparedRunnerBinding::Live(_) => "live",
        }
    }
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
    pub(crate) execution_key: QaScenarioExecutionKey,
    pub(crate) attempt: QaScenarioAttemptProvenance,
    pub(crate) execution_id: String,
    pub(crate) scenario_id: String,
    pub(crate) runner_mode: String,
    pub(crate) verdict: String,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) runtime_path: RuntimePathEvidence,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_state: Option<String>,
    pub(crate) evidence_artifacts: Vec<QaExecutionArtifactRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) evidence_output_bindings: Vec<QaEvidenceOutputBinding>,
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
            "execution_key",
            "attempt",
            "execution_id",
            "scenario_id",
            "runner_mode",
            "verdict",
            "reason_codes",
            "runtime_path",
            "evidence_artifacts",
            "cleanup"
        ],
        "execution_key_fields": [
            "schema_version",
            "format",
            "digest",
            "normalized_manifest_sha256",
            "fixture_set_sha256",
            "runtime_version",
            "runtime_contract_version",
            "runner_version",
            "provider_lane",
            "provider_binding_sha256"
        ],
        "attempt_fields": [
            "generation",
            "runner_version",
            "runtime_version",
            "runtime_contract_version",
            "palyrad_binary_sha256",
            "palyrad_version",
            "palyrad_git_hash",
            "palyrad_build_profile"
        ],
        "optional_attempt_fields": ["previous_result_artifact"],
        "runtime_path_fields": [
            "schema_version",
            "runtime_version",
            "runtime_contract_version",
            "runner_version",
            "provider_lane",
            "attempt_owner",
            "harness",
            "context_engine",
            "complete",
            "source_events",
            "reason_codes",
            "fallbacks",
            "fallback_count"
        ],
        "optional_runtime_path_fields": ["mcp_transport_mode"],
        "evidence_bundle_schema_version": palyra_common::qa_evidence::QA_EVIDENCE_BUNDLE_SCHEMA_VERSION,
        "metadata_trace_fields": [
            "schema_version",
            "run_id_sha256",
            "session_id_sha256",
            "segments"
        ],
        "optional_runtime_fields": ["run_id", "session_id", "terminal_state"],
        "optional_result_fields": ["evidence_output_bindings"],
        "evidence_output_binding": {
            "logical_alias": "manifest_relative_path",
            "artifact": "artifact_reference"
        },
        "artifact_reference": {
            "path": "relative_no_parent_components",
            "sha256": "lowercase_hex_64",
            "size_bytes": "u64",
            "write_policy": "same_directory_temp_sync_atomic_no_clobber"
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

/// Resolves every mutable execution input before the parent creates an attempt.
///
/// # Errors
/// Returns an error when fixtures escape the repository, a replay fixture is not
/// redacted, a live profile is missing or incompatible, or the daemon binary
/// cannot be resolved and hashed.
pub(crate) fn prepare_scenario_execution(
    manifest: &QaScenarioManifest,
) -> Result<QaPreparedScenarioExecution> {
    let runner = manifest.runner.as_ref().ok_or_else(|| {
        anyhow::anyhow!("qa.runner.missing_config: scenario has no runner config")
    })?;
    if runner.runner_mode() != manifest.mode.runner {
        anyhow::bail!("qa.runner.mode_binding_mismatch");
    }
    let repository_root = fs::canonicalize(
        std::env::current_dir().context(
            "qa.runner.repository_root_unavailable: failed to resolve current directory",
        )?,
    )
    .context("qa.runner.repository_root_unavailable: failed to canonicalize current directory")?;
    let workspace_fixture = runner.workspace_fixture().map(str::to_owned);
    if let Some(path) = workspace_fixture.as_deref() {
        resolve_runner_path(repository_root.as_path(), path, "workspace fixture")?;
    }

    let mut fixture_paths = manifest.requires.fixtures.iter().cloned().collect::<BTreeSet<_>>();
    if let Some(workspace) = runner.workspace_fixture() {
        fixture_paths.insert(workspace.to_owned());
    }
    let (binding, provider_binding_sha256) = match manifest.mode.runner {
        QaScenarioRunnerMode::Fixture => {
            let relative = runner.provider_fixture().ok_or_else(|| {
                anyhow::anyhow!("qa.runner.missing_config: fixture binding is incomplete")
            })?;
            fixture_paths.insert(relative.to_owned());
            let provider_fixture =
                resolve_runner_path(repository_root.as_path(), relative, "provider fixture")?;
            let materialized_input_sha256 = sha256_file(provider_fixture.as_path())
                .context("qa.runner.provider_fixture_hash_failed")?;
            (
                QaPreparedRunnerBinding::Fixture { provider_fixture: relative.to_owned() },
                qa_provider_binding_sha256(
                    "fixture",
                    QA_PROVIDER_FIXTURE_MATERIALIZATION,
                    materialized_input_sha256.as_str(),
                )
                .context("qa.runner.provider_binding_hash_failed")?,
            )
        }
        QaScenarioRunnerMode::RecordReplay => {
            let relative = runner.replay_fixture().ok_or_else(|| {
                anyhow::anyhow!("qa.runner.missing_config: record-replay binding is incomplete")
            })?;
            fixture_paths.insert(relative.to_owned());
            let replay_fixture =
                resolve_runner_path(repository_root.as_path(), relative, "replay fixture")?;
            validate_redacted_replay_fixture(replay_fixture.as_path())?;
            let materialized_input_sha256 = sha256_file(replay_fixture.as_path())
                .context("qa.runner.replay_fixture_hash_failed")?;
            (
                QaPreparedRunnerBinding::RecordReplay { replay_fixture: relative.to_owned() },
                qa_provider_binding_sha256(
                    "record_replay",
                    QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION,
                    materialized_input_sha256.as_str(),
                )
                .context("qa.runner.provider_binding_hash_failed")?,
            )
        }
        QaScenarioRunnerMode::Live => {
            let profile_env = runner.live_secret_profile_env().ok_or_else(|| {
                anyhow::anyhow!("qa.runner.missing_config: live binding has no profile env")
            })?;
            let profile_id = env::var(profile_env)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "qa.runner.live_profile_unavailable: required profile env {profile_env} is unset"
                    )
                })?;
            let identity_root = default_identity_store_root()
                .context("qa.runner.live_identity_root_unavailable")?;
            let profile = AuthProfileRegistry::get_profile_readonly(
                identity_root.as_path(),
                profile_id.as_str(),
            )
            .context("qa.runner.live_profile_lookup_failed")?
            .ok_or_else(|| anyhow::anyhow!("qa.runner.live_profile_not_found"))?;
            let provider_kind = runner.live_provider_kind().ok_or_else(|| {
                anyhow::anyhow!("qa.runner.missing_config: live binding has no provider kind")
            })?;
            let base_url = runner.live_base_url().map(str::to_owned);
            let auth_provider_kind =
                validate_live_profile_provider(&profile, provider_kind, base_url.as_deref())?;
            let model = runner
                .live_model()
                .ok_or_else(|| {
                    anyhow::anyhow!("qa.runner.missing_config: live binding has no model")
                })?
                .to_owned();
            let model_provider_kind = match provider_kind {
                QaScenarioLiveProviderKind::OpenAiCompatible => ModelProviderKind::OpenAiCompatible,
                QaScenarioLiveProviderKind::Anthropic => ModelProviderKind::Anthropic,
            };
            let auth_provider = ModelProviderAuthProviderKind::parse(auth_provider_kind.as_str())
                .context("qa.runner.live_profile_provider_kind_invalid")?;
            let defaults = ModelProviderConfig::default();
            let effective_base_url = base_url.as_deref().unwrap_or(match model_provider_kind {
                ModelProviderKind::OpenAiCompatible => defaults.openai_base_url.as_str(),
                ModelProviderKind::Anthropic => defaults.anthropic_base_url.as_str(),
                ModelProviderKind::Deterministic => {
                    unreachable!("live QA provider kind cannot be deterministic")
                }
            });
            let live_binding_metadata = ProviderLiveBindingMetadata {
                provider_kind: model_provider_kind.as_str().to_owned(),
                auth_profile_id: QA_LIVE_MATERIALIZED_PROFILE_ID.to_owned(),
                auth_provider_kind: auth_provider.as_str().to_owned(),
                base_url_sha256: qa_live_provider_base_url_sha256(effective_base_url)
                    .context("qa.runner.provider_base_url_hash_failed")?,
                raw_payload_storage: false,
            };
            let provider_id = palyra_model_providers::providers::legacy_provider_id(
                model_provider_kind,
                Some(auth_provider),
            );
            let binding_digest = qa_live_provider_binding_sha256(
                provider_id,
                model.as_str(),
                &live_binding_metadata,
            )
            .context("qa.runner.provider_binding_hash_failed")?;
            (
                QaPreparedRunnerBinding::Live(Box::new(QaPreparedLiveBinding {
                    profile,
                    provider_kind,
                    auth_provider_kind,
                    model,
                    base_url,
                })),
                binding_digest,
            )
        }
    };

    let binary_override = env::var(QA_DAEMON_BIN_ENV).ok().filter(|value| !value.trim().is_empty());
    let palyrad_binary = commands::daemon::resolve_palyrad_binary(binary_override).context(
        "qa.runner.daemon_binary_unavailable: failed to resolve isolated palyrad binary",
    )?;
    let palyrad_binary = fs::canonicalize(palyrad_binary.as_path())
        .context("qa.runner.daemon_binary_unavailable: failed to canonicalize palyrad binary")?;
    let palyrad_binary_sha256 =
        sha256_file(palyrad_binary.as_path()).context("qa.runner.daemon_binary_hash_failed")?;
    let runner_build = build_metadata();
    let runner_version = qa_runner_version();
    let runtime_version = format!("palyrad-sha256:{palyrad_binary_sha256}");
    let runtime_contract_version = PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION.to_owned();
    let fixture_set_sha256 = digest_repository_fixture_set(
        repository_root.as_path(),
        fixture_paths.iter().map(String::as_str),
    )?;
    let execution_key = build_execution_key(
        manifest,
        fixture_set_sha256,
        runtime_version.as_str(),
        runtime_contract_version.as_str(),
        runner_version.as_str(),
        provider_binding_sha256,
    )?;

    Ok(QaPreparedScenarioExecution {
        execution_key,
        runner_version,
        runtime_version,
        runtime_contract_version,
        expected_palyrad_git_hash: runner_build.git_hash.to_owned(),
        palyrad_binary,
        palyrad_binary_sha256,
        repository_root,
        fixture_paths: fixture_paths.into_iter().collect(),
        workspace_fixture,
        binding,
    })
}

pub(crate) fn qa_runner_version() -> String {
    let build = build_metadata();
    format!(
        "{QA_RUNNER_CONTRACT_VERSION}/{}-{}-{}",
        build.version, build.git_hash, build.build_profile
    )
}

fn validate_live_profile_provider(
    profile: &AuthProfileRecord,
    provider_kind: QaScenarioLiveProviderKind,
    base_url: Option<&str>,
) -> Result<String> {
    if base_url.is_some() {
        anyhow::bail!("qa.runner.live_custom_endpoint_forbidden");
    }
    let provider_label = profile.provider.label();
    let matches = match provider_kind {
        QaScenarioLiveProviderKind::OpenAiCompatible => {
            profile.provider.kind == AuthProviderKind::Openai
        }
        QaScenarioLiveProviderKind::Anthropic => {
            profile.provider.kind == AuthProviderKind::Anthropic
        }
    };
    if !matches {
        anyhow::bail!("qa.runner.live_profile_provider_mismatch");
    }
    Ok(provider_label)
}

fn validate_redacted_replay_fixture(path: &Path) -> Result<()> {
    let text = fs::read_to_string(path).context("qa.runner.replay_fixture_read_failed")?;
    let fixture = parse_qa_mock_provider_fixture_yaml(text.as_str())
        .map_err(|_| anyhow::anyhow!("qa.runner.replay_fixture_invalid"))?;
    let provenance = fixture
        .capture_provenance()
        .ok_or_else(|| anyhow::anyhow!("qa.runner.replay_fixture_provenance_missing"))?;
    if provenance.raw_payloads_stored()
        || provenance.redaction_contract() != "palyra-provider-replay.v1"
    {
        anyhow::bail!("qa.runner.replay_fixture_provenance_invalid");
    }
    let value = yaml_serde::from_str::<Value>(text.as_str())
        .map_err(|_| anyhow::anyhow!("qa.runner.replay_fixture_invalid"))?;
    validate_redacted_replay_value(&value)?;
    for comment in text.lines().filter_map(yaml_comment_text) {
        if redact_diagnostic_text(comment) != comment {
            anyhow::bail!("qa.runner.replay_fixture_secret_material");
        }
    }
    Ok(())
}

fn validate_redacted_replay_value(value: &Value) -> Result<()> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if is_replay_sensitive_key(key) {
                    match value.as_str().map(str::trim) {
                        Some("<redacted>" | "[redacted]" | "[REDACTED]") => {}
                        Some(_) | None => {
                            anyhow::bail!("qa.runner.replay_fixture_secret_material");
                        }
                    }
                }
                validate_redacted_replay_value(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_redacted_replay_value(value)?;
            }
        }
        Value::String(text) if redact_diagnostic_text(text) != *text => {
            anyhow::bail!("qa.runner.replay_fixture_secret_material");
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn yaml_comment_text(line: &str) -> Option<&str> {
    // Scan ambiguous hash markers inside quoted scalars too. Parsed string values
    // already use the same redactor, while this fail-closed rule also catches
    // comments after apostrophes or unmatched quotes in valid plain scalars.
    // YAML accepts a UTF-8 BOM before the first token, so it must not hide a
    // first-line comment from the separation check.
    let line = line.strip_prefix('\u{feff}').unwrap_or(line);
    line.match_indices('#').find_map(|(index, _)| {
        let previous = line[..index].chars().next_back();
        previous.is_none_or(char::is_whitespace).then_some(&line[index + 1..])
    })
}

fn is_replay_sensitive_key(key: &str) -> bool {
    !matches!(key, "prompt_tokens" | "completion_tokens") && is_sensitive_key(key)
}

fn build_execution_key(
    manifest: &QaScenarioManifest,
    fixture_set_sha256: String,
    runtime_version: &str,
    runtime_contract_version: &str,
    runner_version: &str,
    provider_binding_sha256: String,
) -> Result<QaScenarioExecutionKey> {
    let normalized_manifest = canonical_json_bytes(manifest)?;
    let mut execution_key = QaScenarioExecutionKey {
        schema_version: EXECUTION_KEY_SCHEMA_VERSION,
        format: EXECUTION_KEY_FORMAT.to_owned(),
        digest: String::new(),
        normalized_manifest_sha256: sha256_hex(normalized_manifest.as_slice()),
        fixture_set_sha256,
        runtime_version: runtime_version.to_owned(),
        runtime_contract_version: runtime_contract_version.to_owned(),
        runner_version: runner_version.to_owned(),
        provider_lane: manifest.mode.runner.as_str().to_owned(),
        provider_binding_sha256,
    };
    execution_key.digest = execution_key_digest(&execution_key);
    execution_key.validate_shape()?;
    Ok(execution_key)
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value).context("qa.runner.execution_key_encode_failed")?;
    let mut output = String::new();
    write_canonical_json(&value, &mut output)?;
    Ok(output.into_bytes())
}

fn write_canonical_json(value: &Value, output: &mut String) -> Result<()> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => output.push_str(value.to_string().as_str()),
        Value::String(value) => output.push_str(
            serde_json::to_string(value).context("qa.runner.execution_key_encode_failed")?.as_str(),
        ),
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(']');
        }
        Value::Object(object) => {
            output.push('{');
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(
                    serde_json::to_string(key)
                        .context("qa.runner.execution_key_encode_failed")?
                        .as_str(),
                );
                output.push(':');
                write_canonical_json(&object[key], output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

fn execution_key_digest(key: &QaScenarioExecutionKey) -> String {
    hash_labeled_fields(&[
        ("format", EXECUTION_KEY_FORMAT),
        ("schema_version", "1"),
        ("normalized_manifest_sha256", key.normalized_manifest_sha256.as_str()),
        ("fixture_set_sha256", key.fixture_set_sha256.as_str()),
        ("runtime_version", key.runtime_version.as_str()),
        ("runtime_contract_version", key.runtime_contract_version.as_str()),
        ("runner_version", key.runner_version.as_str()),
        ("provider_lane", key.provider_lane.as_str()),
        ("provider_binding_sha256", key.provider_binding_sha256.as_str()),
    ])
}

fn hash_labeled_fields(fields: &[(&str, &str)]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra-domain-separated-sha256-v1\0");
    for (label, value) in fields {
        hasher.update((label.len() as u64).to_be_bytes());
        hasher.update(label.as_bytes());
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    digest_to_hex(hasher.finalize().as_slice())
}

fn digest_repository_fixture_set<'a>(
    repository_root: &Path,
    paths: impl IntoIterator<Item = &'a str>,
) -> Result<String> {
    let entries = paths
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|relative| {
            Ok((
                relative.to_owned(),
                resolve_runner_path(repository_root, relative, "declared fixture")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    digest_materialized_fixture_set(entries.as_slice())
}

pub(super) fn digest_materialized_fixture_set(entries: &[(String, PathBuf)]) -> Result<String> {
    if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        anyhow::bail!("qa.runner.fixture_set_order_invalid");
    }
    let mut hasher = Sha256::new();
    hasher.update(b"palyra-qa-fixture-set-v1\0");
    for (logical_path, materialized_path) in entries {
        hash_fixture_tree(Path::new(logical_path), materialized_path.as_path(), &mut hasher)?;
    }
    Ok(digest_to_hex(hasher.finalize().as_slice()))
}

fn hash_fixture_tree(
    logical_path: &Path,
    materialized_path: &Path,
    hasher: &mut Sha256,
) -> Result<()> {
    let metadata =
        fs::symlink_metadata(materialized_path).context("qa.runner.fixture_metadata_failed")?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("qa.runner.fixture_symlink_denied");
    }
    let logical_path = display_path_slash(logical_path);
    if metadata.is_file() {
        hash_fixture_entry(hasher, b"file", logical_path.as_bytes());
        hasher.update(metadata.len().to_be_bytes());
        let mut file =
            fs::File::open(materialized_path).context("qa.runner.fixture_read_failed")?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer).context("qa.runner.fixture_read_failed")?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        return Ok(());
    }
    if !metadata.is_dir() {
        anyhow::bail!("qa.runner.fixture_special_file_denied");
    }
    hash_fixture_entry(hasher, b"directory", logical_path.as_bytes());
    let mut children = fs::read_dir(materialized_path)
        .context("qa.runner.fixture_directory_read_failed")?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .context("qa.runner.fixture_directory_read_failed")?;
    children.sort_by_key(|path| path.file_name().map(std::ffi::OsStr::to_os_string));
    for child in children {
        let child_name =
            child.file_name().ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_path_invalid"))?;
        hash_fixture_tree(
            Path::new(logical_path.as_str()).join(child_name).as_path(),
            child.as_path(),
            hasher,
        )?;
    }
    Ok(())
}

fn hash_fixture_entry(hasher: &mut Sha256, kind: &[u8], relative_path: &[u8]) {
    hasher.update((kind.len() as u64).to_be_bytes());
    hasher.update(kind);
    hasher.update((relative_path.len() as u64).to_be_bytes());
    hasher.update(relative_path);
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(digest_to_hex(hasher.finalize().as_slice()))
}

fn digest_to_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    })
}

/// Executes one prepared scenario through a fresh production daemon.
///
/// # Errors
/// Returns an error when parent-issued identity does not match the prepared
/// inputs, the child daemon cannot be isolated or reached, the console stream
/// is malformed, or evidence/result artifacts cannot be persisted. The process
/// guard still terminates the child and removes the temporary root on every path.
pub(crate) async fn execute_prepared_scenario(
    manifest: &QaScenarioManifest,
    prepared: &QaPreparedScenarioExecution,
    request: QaScenarioExecutionRequest,
    artifact_root: &Path,
) -> Result<QaScenarioExecutionReport> {
    if request.execution_key != prepared.execution_key
        || request.runner_version != prepared.runner_version
        || request.attempt_generation == 0
        || request.execution_id.trim().is_empty()
        || prepared.provider_lane() != manifest.mode.runner.as_str()
    {
        anyhow::bail!("qa.runner.parent_execution_request_invalid");
    }
    let evidence_path = scenario_artifact_path(
        request.execution_id.as_str(),
        request.execution_key.digest.as_str(),
        "evidence.json",
    );
    let result_path =
        scenario_result_artifact_path(request.execution_id.as_str(), &request.execution_key);
    let failure_diagnostics_path = scenario_artifact_path(
        request.execution_id.as_str(),
        request.execution_key.digest.as_str(),
        "failure-diagnostics.json",
    );
    let mut sandbox = QaDaemonSandbox::spawn(manifest, prepared)?;
    let deadline = QaRunDeadline::new(manifest)?;
    let mut observation_result =
        collect_observations_with_fault_recovery(manifest, &mut sandbox, deadline).await;
    let mut runtime_path = extract_runtime_path_evidence(RuntimePathExtractionInput {
        tape_events: &[],
        tool_calls: &[],
        runtime_version: prepared.runtime_version.as_str(),
        runtime_contract_version: prepared.runtime_contract_version.as_str(),
        runner_version: request.runner_version.as_str(),
        expected_provider_lane: prepared.provider_lane(),
        execution_key_digest: request.execution_key.digest.as_str(),
        provider_binding_sha256: request.execution_key.provider_binding_sha256.as_str(),
    });
    if let Ok(observations) = observation_result.as_mut() {
        runtime_path = extract_runtime_path_evidence(RuntimePathExtractionInput {
            tape_events: observations.evidence.tape_events.as_slice(),
            tool_calls: observations.evidence.tool_calls.as_slice(),
            runtime_version: prepared.runtime_version.as_str(),
            runtime_contract_version: prepared.runtime_contract_version.as_str(),
            runner_version: request.runner_version.as_str(),
            expected_provider_lane: prepared.provider_lane(),
            execution_key_digest: request.execution_key.digest.as_str(),
            provider_binding_sha256: request.execution_key.provider_binding_sha256.as_str(),
        });
        observations.evidence.runtime_path = Some(runtime_path.clone());
    }
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
    let runtime_health = sandbox.runtime_health().clone();
    let observation_failure_code = observation_result.as_ref().err().map(stable_runner_error_code);
    let mut failure_diagnostics_artifact = None;
    let mut failure_diagnostics_write_error = None;
    let (session_cleaned, shutdown) = if let Some(failure_reason_code) =
        observation_failure_code.as_deref()
    {
        // Failed streams can leave the control plane unable to clean up itself. Quiesce the
        // child before reading its durable state, but retain the TempDir until the bundle lands.
        let session_was_absent = sandbox.active_session_id().is_none();
        let daemon_terminated = sandbox.terminate_for_failure_diagnostics();
        let diagnostics = sandbox.failure_diagnostics(
            request.runner_version.as_str(),
            prepared.runtime_version.as_str(),
            failure_reason_code,
            daemon_terminated,
        );
        let diagnostics_write =
            encode_failure_diagnostics_artifact(&diagnostics).and_then(|bytes| {
                if sandbox.contains_secret(bytes.as_slice()) {
                    Err(anyhow::anyhow!("qa.runner.secret_leak_detected"))
                } else {
                    write_artifact_bytes(
                        artifact_root,
                        failure_diagnostics_path.as_path(),
                        "failure_diagnostics",
                        bytes.as_slice(),
                    )
                }
            });
        match diagnostics_write {
            Ok(reference) => failure_diagnostics_artifact = Some(reference),
            Err(error) => failure_diagnostics_write_error = Some(stable_runner_error_code(&error)),
        }
        let workspace_removed = sandbox.remove_state_root();
        // Once the isolated daemon is reaped and its state root is gone, an
        // active session cannot remain live or durable even when its API was
        // unavailable during failure diagnostics.
        let session_cleaned = session_was_absent || (daemon_terminated && workspace_removed);
        (session_cleaned, QaDaemonShutdown { daemon_terminated, workspace_removed })
    } else {
        // Successful observations retain the established API cleanup path and still remove the
        // isolated root before any evidence write can fail.
        let session_cleaned = sandbox.cleanup_active_session().await;
        let shutdown = sandbox.shutdown();
        (session_cleaned, shutdown)
    };
    let cleanup = cleanup_result(terminal_observed, session_cleaned, shutdown);
    let mut reason_codes = cleanup.reason_codes.clone();
    reason_codes.extend(failure_diagnostics_write_error);
    let mut evidence_artifacts = failure_diagnostics_artifact.into_iter().collect::<Vec<_>>();
    let mut evidence_output_bindings = Vec::new();
    let mut evidence_passed = false;
    match observation_result {
        Ok(observations) => {
            let mut runtime_manifest = manifest.clone();
            // Runner-owned evidence bytes do not exist until this bundle is
            // serialized. Their output contract is checked against the
            // immutable write below, outside the runtime-observation engine.
            runtime_manifest
                .artifacts
                .retain(|artifact| artifact.kind != QaScenarioArtifactKind::Evidence);
            let evidence = build_qa_evidence_bundle(&runtime_manifest, observations.evidence);
            reason_codes.extend(evidence_reason_codes(&evidence));
            let runtime_evidence_passed = evidence.summary.verdict.as_str() == "passed";
            let evidence_bytes = encode_evidence_artifact(&evidence)?;
            let evidence_write = if sandbox.contains_secret(evidence_bytes.as_slice()) {
                Err(anyhow::anyhow!("qa.runner.secret_leak_detected"))
            } else {
                write_artifact_bytes(
                    artifact_root,
                    evidence_path.as_path(),
                    QaScenarioArtifactKind::Evidence.as_str(),
                    evidence_bytes.as_slice(),
                )
            };
            match evidence_write {
                Ok(reference) => {
                    let output_contract = bind_evidence_outputs(manifest, &reference);
                    evidence_passed =
                        runtime_evidence_passed && output_contract.reason_codes.is_empty();
                    reason_codes.extend(output_contract.reason_codes);
                    evidence_output_bindings = output_contract.bindings;
                    evidence_artifacts.push(reference);
                }
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
        execution_key: request.execution_key,
        attempt: QaScenarioAttemptProvenance {
            generation: request.attempt_generation,
            runner_version: request.runner_version,
            runtime_version: prepared.runtime_version.clone(),
            runtime_contract_version: runtime_health.public_runtime_contract_version,
            palyrad_binary_sha256: prepared.palyrad_binary_sha256.clone(),
            palyrad_version: runtime_health.version,
            palyrad_git_hash: runtime_health.git_hash,
            palyrad_build_profile: runtime_health.build_profile,
            previous_result_artifact: request.previous_result_artifact,
        },
        execution_id: request.execution_id,
        scenario_id: manifest.id.clone(),
        runner_mode: manifest.mode.runner.as_str().to_owned(),
        verdict: verdict.to_owned(),
        reason_codes,
        runtime_path,
        run_id,
        session_id,
        terminal_state,
        evidence_artifacts,
        evidence_output_bindings,
        cleanup,
    };
    validate_execution_result(&result)?;
    let result_bytes =
        serde_json::to_vec_pretty(&result).context("qa.runner.artifact_encode_failed")?;
    if sandbox.contains_secret(result_bytes.as_slice()) {
        anyhow::bail!("qa.runner.secret_leak_detected");
    }
    let result_artifact = write_artifact_bytes(
        artifact_root,
        result_path.as_path(),
        "execution_result",
        result_bytes.as_slice(),
    )?;
    Ok(QaScenarioExecutionReport { result, result_artifact })
}

async fn collect_observations_with_fault_recovery(
    manifest: &QaScenarioManifest,
    sandbox: &mut QaDaemonSandbox,
    deadline: QaRunDeadline,
) -> Result<QaScenarioObservations> {
    let initial = collect_scenario_observations(manifest, sandbox, deadline).await;
    let Some(plan) = manifest.fault_injection.as_ref() else {
        return initial;
    };
    let expected =
        manifest.expect.fault_injection.as_ref().context("qa.runner.fault_expectations_missing")?;
    if expected.daemon_restarts == 0 {
        let mut observations = initial?;
        attach_fault_evidence(plan, sandbox, &mut observations)?;
        return Ok(observations);
    }
    if expected.daemon_restarts != 1
        || plan
            .activations
            .iter()
            .filter(|activation| matches!(&activation.action, QaFaultAction::TerminateProcess))
            .count()
            != 1
    {
        anyhow::bail!("qa.runner.unsupported_fault_restart_plan");
    }
    let initial_error = match initial {
        Ok(_) => anyhow::bail!("qa.runner.expected_fault_exit_not_observed"),
        Err(error) => error,
    };
    let terminate_activation_id = plan
        .activations
        .iter()
        .find(|activation| matches!(&activation.action, QaFaultAction::TerminateProcess))
        .map(|activation| activation.id.as_str())
        .context("qa.runner.expected_fault_activation_missing")?;
    let exit_budget = deadline.step_budget()?;
    deadline.normalize_sync_result(
        sandbox.wait_for_expected_exit(QA_FAULT_TERMINATE_EXIT_CODE, exit_budget),
    )?;
    let sidecar = match sandbox.fault_evidence_sidecar() {
        Ok(Some(sidecar)) => sidecar,
        Ok(None) | Err(_) => return Err(initial_error),
    };
    // A complete sidecar read is trustworthy only after the writer has exited and the
    // child has been reaped. The durable activation then binds the stream failure to the
    // declared terminate rule instead of an unrelated observation failure.
    if !fault_activation_recorded(sidecar.records(), terminate_activation_id) {
        return Err(initial_error);
    }
    let restart_budget = deadline.step_budget()?;
    deadline.normalize_sync_result(sandbox.restart_preserving_state(restart_budget))?;
    let mut observations = collect_recovered_scenario_observations(sandbox, deadline).await?;
    attach_fault_evidence(plan, sandbox, &mut observations)?;
    Ok(observations)
}

fn fault_activation_recorded(
    records: &[QaFaultEvidenceSidecarRecord],
    activation_id: &str,
) -> bool {
    records.iter().any(|record| {
        matches!(
            record,
            QaFaultEvidenceSidecarRecord::RuleActivated(activation)
                if activation.activation_id == activation_id
                    && matches!(&activation.action, QaFaultAction::TerminateProcess)
        )
    })
}

fn attach_fault_evidence(
    plan: &QaFaultInjectionPlan,
    sandbox: &QaDaemonSandbox,
    observations: &mut QaScenarioObservations,
) -> Result<()> {
    let sidecar = sandbox.fault_evidence_sidecar()?.context("qa.runner.fault_evidence_missing")?;
    observations.evidence.fault_injections =
        qa_fault_injection_evidence_from_sidecar(&sidecar, plan)
            .context("qa.runner.fault_evidence_projection_failed")?;
    observations.evidence.daemon_restart_count = sandbox.daemon_restarts();
    Ok(())
}

fn validate_execution_result(result: &QaScenarioExecutionResult) -> Result<()> {
    result.execution_key.validate_shape()?;
    if result.schema_version != EXECUTION_RESULT_SCHEMA_VERSION
        || result.format != EXECUTION_RESULT_FORMAT
        || result.execution_key.runner_version != result.attempt.runner_version
        || result.execution_key.runtime_version != result.attempt.runtime_version
        || result.execution_key.runtime_contract_version != result.attempt.runtime_contract_version
        || result.attempt.generation == 0
        || result.attempt.palyrad_version.trim().is_empty()
        || result.attempt.palyrad_git_hash.trim().is_empty()
        || result.attempt.palyrad_build_profile.trim().is_empty()
        || result.attempt.palyrad_binary_sha256.len() != 64
        || !result.attempt.palyrad_binary_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        || result.execution_id.trim().is_empty()
        || result.scenario_id.trim().is_empty()
        || !matches!(result.verdict.as_str(), "passed" | "failed")
        || result.runtime_path.validate_shape().is_err()
        || result.runtime_path.runtime_version != result.execution_key.runtime_version
        || result.runtime_path.runtime_contract_version
            != result.execution_key.runtime_contract_version
        || result.runtime_path.runner_version != result.execution_key.runner_version
    {
        anyhow::bail!("qa.runner.execution_result_invalid");
    }
    if result.verdict == "passed"
        && (result.run_id.is_none()
            || result.session_id.is_none()
            || result.terminal_state.is_none()
            || !result
                .evidence_artifacts
                .iter()
                .any(|artifact| artifact.kind == QaScenarioArtifactKind::Evidence.as_str())
            || !result.cleanup.verified)
    {
        anyhow::bail!("qa.runner.execution_result_incomplete");
    }
    for reference in &result.evidence_artifacts {
        validate_execution_artifact_reference(reference)?;
    }
    let mut logical_aliases = BTreeSet::new();
    for binding in &result.evidence_output_bindings {
        let alias = Path::new(binding.logical_alias.as_str());
        if binding.logical_alias.trim().is_empty()
            || alias.is_absolute()
            || alias.components().any(|component| !matches!(component, Component::Normal(_)))
            || !logical_aliases.insert(binding.logical_alias.as_str())
            || binding.artifact.kind != QaScenarioArtifactKind::Evidence.as_str()
            || !result.evidence_artifacts.contains(&binding.artifact)
        {
            anyhow::bail!("qa.runner.evidence_output_binding_invalid");
        }
        validate_execution_artifact_reference(&binding.artifact)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_runtime_path_evidence(
    runtime_version: &str,
    runtime_contract_version: &str,
    runner_version: &str,
    provider_lane: &str,
) -> RuntimePathEvidence {
    RuntimePathEvidence {
        schema_version: palyra_common::qa_runtime_path::QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION,
        runtime_version: runtime_version.to_owned(),
        runtime_contract_version: runtime_contract_version.to_owned(),
        runner_version: runner_version.to_owned(),
        provider_lane: provider_lane.to_owned(),
        attempt_owner: "embedded_run_stream".to_owned(),
        harness: RuntimePathComponentEvidence {
            id: "embedded_run_stream".to_owned(),
            source_event: "run.runtime_path_summary".to_owned(),
            reason_code: "runtime_path.harness.embedded_selected".to_owned(),
        },
        context_engine: RuntimePathComponentEvidence {
            id: "legacy_provider_input".to_owned(),
            source_event: "run.runtime_path_summary".to_owned(),
            reason_code: "runtime_path.context.legacy_selected".to_owned(),
        },
        mcp_transport_mode: None,
        complete: true,
        source_events: vec![
            "runner.execution_key".to_owned(),
            "run.runtime_path_summary".to_owned(),
        ],
        reason_codes: vec!["qa.runner.runtime_path_complete".to_owned()],
        fallbacks: Vec::new(),
        fallback_count: 0,
    }
}

fn validate_execution_artifact_reference(reference: &QaExecutionArtifactRef) -> Result<()> {
    let path = Path::new(reference.path.as_str());
    if path.is_absolute()
        || path.components().any(|component| !matches!(component, Component::Normal(_)))
        || reference.kind.trim().is_empty()
        || reference.sha256.len() != 64
        || !reference.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        anyhow::bail!("qa.runner.execution_artifact_reference_invalid");
    }
    Ok(())
}

fn resolve_runner_path(root: &Path, relative: &str, label: &str) -> Result<PathBuf> {
    let path = root.join(relative);
    let metadata = fs::symlink_metadata(path.as_path()).with_context(|| {
        format!("qa.runner.fixture_unavailable: failed to inspect {label} {relative}")
    })?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("qa.runner.fixture_symlink_denied: {label} must not be a symlink");
    }
    let canonical = fs::canonicalize(path.as_path()).with_context(|| {
        format!("qa.runner.fixture_unavailable: failed to resolve {label} {relative}")
    })?;
    if !canonical.starts_with(root) {
        anyhow::bail!("qa.runner.fixture_outside_repository: {label} must stay inside repository");
    }
    Ok(canonical)
}

fn scenario_artifact_path(execution_id: &str, execution_key: &str, filename: &str) -> PathBuf {
    PathBuf::from("executions").join(execution_id).join(execution_key).join(filename)
}

pub(crate) fn scenario_result_artifact_path(
    execution_id: &str,
    execution_key: &QaScenarioExecutionKey,
) -> PathBuf {
    scenario_artifact_path(execution_id, execution_key.digest.as_str(), "result.json")
}

fn encode_evidence_artifact(evidence: &QaEvidenceBundle) -> Result<Vec<u8>> {
    encode_evidence_artifact_with_limit(evidence, MAX_EVIDENCE_ARTIFACT_BYTES)
}

fn encode_evidence_artifact_with_limit<T: Serialize + ?Sized>(
    value: &T,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    encode_bounded_json_artifact(value, max_bytes, "qa.runner.evidence_artifact_limit_exceeded")
}

fn encode_failure_diagnostics_artifact(diagnostics: &QaFailureDiagnostics) -> Result<Vec<u8>> {
    encode_bounded_json_artifact(
        diagnostics,
        MAX_FAILURE_DIAGNOSTICS_ARTIFACT_BYTES,
        "qa.runner.failure_diagnostics_artifact_limit_exceeded",
    )
}

fn encode_bounded_json_artifact<T: Serialize + ?Sized>(
    value: &T,
    max_bytes: usize,
    limit_error_code: &'static str,
) -> Result<Vec<u8>> {
    let mut output = BoundedJsonBuffer::new(max_bytes);
    let serialization = serde_json::to_writer_pretty(&mut output, value);
    if output.limit_exceeded {
        anyhow::bail!("{limit_error_code}");
    }
    serialization.context("qa.runner.artifact_encode_failed")?;
    Ok(output.bytes)
}

struct BoundedJsonBuffer {
    bytes: Vec<u8>,
    max_bytes: usize,
    limit_exceeded: bool,
}

impl BoundedJsonBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(16 * 1024)),
            max_bytes,
            limit_exceeded: false,
        }
    }
}

impl IoWrite for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if json_write_exceeds_limit(self.bytes.len(), buffer.len(), self.max_bytes) {
            self.limit_exceeded = true;
            return Err(io::Error::other("bounded evidence JSON limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json_write_exceeds_limit(current: usize, incoming: usize, max_bytes: usize) -> bool {
    incoming > max_bytes.saturating_sub(current)
}

#[cfg(test)]
fn write_json_artifact<T: Serialize>(
    root: &Path,
    relative_path: &Path,
    kind: &str,
    value: &T,
) -> Result<QaExecutionArtifactRef> {
    let bytes = serde_json::to_vec_pretty(value).context("qa.runner.artifact_encode_failed")?;
    write_artifact_bytes(root, relative_path, kind, bytes.as_slice())
}

pub(super) fn write_artifact_bytes(
    root: &Path,
    relative_path: &Path,
    kind: &str,
    bytes: &[u8],
) -> Result<QaExecutionArtifactRef> {
    if relative_path.is_absolute()
        || relative_path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        anyhow::bail!("qa.runner.artifact_path_invalid");
    }
    let path = root.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("qa.runner.artifact_directory_create_failed: {}", parent.display())
        })?;
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow::anyhow!("qa.runner.artifact_path_invalid"))?;
    let temporary_path = path.with_file_name(format!(".{file_name}.{}.tmp", Ulid::new()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary_path.as_path())
        .with_context(|| {
            format!("qa.runner.artifact_temp_create_failed: {}", temporary_path.display())
        })?;
    let publish_result = (|| -> Result<()> {
        file.write_all(bytes).with_context(|| {
            format!("qa.runner.artifact_temp_write_failed: {}", temporary_path.display())
        })?;
        file.flush().with_context(|| {
            format!("qa.runner.artifact_temp_flush_failed: {}", temporary_path.display())
        })?;
        file.sync_all().with_context(|| {
            format!("qa.runner.artifact_temp_sync_failed: {}", temporary_path.display())
        })?;
        drop(file);
        publish_immutable_artifact(temporary_path.as_path(), path.as_path())?;
        fs::remove_file(temporary_path.as_path()).with_context(|| {
            format!("qa.runner.artifact_temp_cleanup_failed: {}", temporary_path.display())
        })?;
        sync_parent_directory(path.as_path())?;
        let persisted = fs::read(path.as_path()).with_context(|| {
            format!("qa.runner.artifact_verify_read_failed: {}", path.display())
        })?;
        if persisted.as_slice() != bytes {
            anyhow::bail!("qa.runner.artifact_verify_failed: {}", path.display());
        }
        Ok(())
    })();
    if publish_result.is_err() {
        let _ = fs::remove_file(temporary_path.as_path());
    }
    publish_result?;
    Ok(QaExecutionArtifactRef {
        path: display_path_slash(relative_path),
        kind: kind.to_owned(),
        sha256: sha256_hex(bytes),
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    })
}

pub(super) fn read_verified_artifact(
    root: &Path,
    reference: &QaExecutionArtifactRef,
) -> Result<Vec<u8>> {
    let relative_path = Path::new(reference.path.as_str());
    if relative_path.is_absolute()
        || relative_path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        anyhow::bail!("qa.runner.execution_artifact_reference_invalid");
    }
    let bytes =
        fs::read(root.join(relative_path)).context("qa.runner.execution_artifact_read_failed")?;
    if reference.size_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || reference.sha256 != sha256_hex(bytes.as_slice())
    {
        anyhow::bail!("qa.runner.execution_artifact_digest_mismatch");
    }
    Ok(bytes)
}

pub(crate) fn load_execution_report(
    artifact_root: &Path,
    result_artifact: QaExecutionArtifactRef,
    expected_key: &QaScenarioExecutionKey,
    expected_attempt_generation: u64,
    expected_execution_id: &str,
) -> Result<QaScenarioExecutionReport> {
    if result_artifact.kind != "execution_result" {
        anyhow::bail!("qa.resume.result_artifact_kind_invalid");
    }
    let bytes = read_verified_artifact(artifact_root, &result_artifact)?;
    let result = serde_json::from_slice::<QaScenarioExecutionResult>(bytes.as_slice())
        .context("qa.resume.result_parse_failed")?;
    validate_execution_result(&result)?;
    if result.execution_key != *expected_key
        || result.attempt.generation != expected_attempt_generation
        || result.execution_id != expected_execution_id
        || result.runner_mode != expected_key.provider_lane
    {
        anyhow::bail!("qa.resume.result_provenance_mismatch");
    }
    let expected_prefix =
        PathBuf::from("executions").join(expected_execution_id).join(expected_key.digest.as_str());
    if !Path::new(result_artifact.path.as_str()).starts_with(expected_prefix.as_path()) {
        anyhow::bail!("qa.resume.result_path_mismatch");
    }
    for evidence in &result.evidence_artifacts {
        if !Path::new(evidence.path.as_str()).starts_with(expected_prefix.as_path()) {
            anyhow::bail!("qa.resume.evidence_path_mismatch");
        }
        read_verified_artifact(artifact_root, evidence)?;
    }
    Ok(QaScenarioExecutionReport { result, result_artifact })
}

pub(crate) fn recover_execution_report(
    artifact_root: &Path,
    relative_result_path: &str,
    expected_key: &QaScenarioExecutionKey,
    expected_attempt_generation: u64,
    expected_execution_id: &str,
) -> Result<QaScenarioExecutionReport> {
    let path = Path::new(relative_result_path);
    if path.is_absolute()
        || path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        anyhow::bail!("qa.resume.result_path_invalid");
    }
    let bytes =
        fs::read(artifact_root.join(path)).context("qa.resume.result_artifact_unavailable")?;
    let result_artifact = QaExecutionArtifactRef {
        path: display_path_slash(path),
        kind: "execution_result".to_owned(),
        sha256: sha256_hex(bytes.as_slice()),
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    };
    load_execution_report(
        artifact_root,
        result_artifact,
        expected_key,
        expected_attempt_generation,
        expected_execution_id,
    )
}

/// Publishes a fully synced temp file without an overwrite race.
///
/// A same-filesystem hard link creates the destination atomically only when it
/// is absent; unlike Unix `rename`, it never replaces an existing artifact.
fn publish_immutable_artifact(source: &Path, destination: &Path) -> Result<()> {
    match fs::hard_link(source, destination) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            anyhow::bail!("qa.runner.artifact_already_exists: {}", destination.display())
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "qa.runner.artifact_publish_failed: {} -> {}",
                source.display(),
                destination.display()
            )
        }),
    }
}

fn sync_parent_directory(
    #[cfg_attr(not(unix), allow(unused_variables))] path: &Path,
) -> Result<()> {
    #[cfg(unix)]
    {
        let parent =
            path.parent().ok_or_else(|| anyhow::anyhow!("qa.runner.artifact_path_invalid"))?;
        fs::File::open(parent).and_then(|directory| directory.sync_all()).with_context(|| {
            format!("qa.runner.artifact_directory_sync_failed: {}", parent.display())
        })?;
    }
    Ok(())
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
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

struct QaEvidenceOutputContract {
    bindings: Vec<QaEvidenceOutputBinding>,
    reason_codes: Vec<String>,
}

fn bind_evidence_outputs(
    manifest: &QaScenarioManifest,
    physical: &QaExecutionArtifactRef,
) -> QaEvidenceOutputContract {
    let mut bindings = Vec::new();
    let mut reason_codes = Vec::new();
    for expected in manifest
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == QaScenarioArtifactKind::Evidence)
    {
        bindings.push(QaEvidenceOutputBinding {
            logical_alias: expected.path.clone(),
            artifact: physical.clone(),
        });
        if expected.required
            && expected.sha256.as_deref().is_some_and(|digest| digest != physical.sha256)
        {
            reason_codes.push("artifact_digest_mismatch".to_owned());
        }
    }
    reason_codes.sort();
    reason_codes.dedup();
    QaEvidenceOutputContract { bindings, reason_codes }
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
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use serde_json::json;

    use super::*;

    #[test]
    fn fault_recovery_requires_matching_terminate_activation_evidence() {
        let activation = QaFaultEvidenceSidecarRecord::RuleActivated(
            palyra_common::qa_fault_injection::QaFaultRuleActivatedRecord {
                schema_version: 1,
                sequence: 2,
                launch_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                plan_sha256: "a".repeat(64),
                activation_id: "tool-effect-before-ack".to_owned(),
                point_id: "tool.after_effect_before_ack".to_owned(),
                actors: vec!["qa-fault-mutation".to_owned()],
                occurrence: 1,
                action: QaFaultAction::TerminateProcess,
                activation_sequence: 1,
                release_order: vec!["qa-fault-mutation".to_owned()],
            },
        );

        assert!(!fault_activation_recorded(&[], "tool-effect-before-ack"));
        assert!(!fault_activation_recorded(
            std::slice::from_ref(&activation),
            "different-activation"
        ));
        assert!(fault_activation_recorded(
            std::slice::from_ref(&activation),
            "tool-effect-before-ack"
        ));
    }

    #[test]
    fn artifact_paths_are_execution_unique_and_collision_resistant() {
        let dotted = scenario_artifact_path("execution-a", "qa.a", "evidence.json");
        let underscored = scenario_artifact_path("execution-a", "qa_a", "evidence.json");
        let repeated = scenario_artifact_path("execution-b", "qa.a", "evidence.json");

        assert_ne!(dotted, underscored);
        assert_ne!(dotted, repeated);
        assert!(display_path_slash(dotted.as_path()).starts_with("executions/execution-a/qa.a/"));
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
        let overwrite_error = write_json_artifact(
            root.path(),
            relative.as_path(),
            "execution_result",
            &json!({"status": "overwritten"}),
        )
        .expect_err("an immutable artifact must not be overwritten");
        assert!(overwrite_error.to_string().contains("qa.runner.artifact_already_exists"));
        assert_eq!(
            fs::read(root.path().join(reference.path.as_str()))
                .expect("original artifact should remain readable"),
            persisted
        );
    }

    #[test]
    fn concurrent_artifact_writers_publish_one_payload_without_clobbering() {
        const WRITER_COUNT: usize = 8;

        let root = tempfile::tempdir().expect("artifact root should be available");
        let relative = scenario_artifact_path("execution-race", "qa.race", "result.json");
        let barrier = Arc::new(Barrier::new(WRITER_COUNT));
        let handles = (0..WRITER_COUNT)
            .map(|writer_index| {
                let root = root.path().to_path_buf();
                let relative = relative.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let payload = format!("writer-{writer_index:02}-payload").into_bytes();
                    barrier.wait();
                    let result = write_artifact_bytes(
                        root.as_path(),
                        relative.as_path(),
                        "execution_result",
                        payload.as_slice(),
                    )
                    .map_err(|error| error.to_string());
                    (writer_index, payload, result)
                })
            })
            .collect::<Vec<_>>();

        let mut winner = None;
        let mut loser_errors = Vec::new();
        for handle in handles {
            let outcome = handle.join().expect("artifact writer thread should finish");
            match outcome.2 {
                Ok(reference) => {
                    assert!(
                        winner.replace((outcome.0, outcome.1, reference)).is_none(),
                        "only one writer may publish the final path"
                    );
                }
                Err(error) => loser_errors.push(error),
            }
        }

        let (_, winning_payload, reference) = winner.expect("one writer should publish");
        assert_eq!(loser_errors.len(), WRITER_COUNT - 1);
        assert!(loser_errors
            .iter()
            .all(|error| error.contains("qa.runner.artifact_already_exists")));
        let persisted = fs::read(root.path().join(relative.as_path()))
            .expect("winning artifact should remain readable");
        assert_eq!(persisted, winning_payload);
        assert_eq!(reference.sha256, sha256_hex(persisted.as_slice()));
        let temporary_files = fs::read_dir(
            root.path().join(relative.parent().expect("race artifact should have a parent")),
        )
        .expect("artifact directory should remain readable")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
        .count();
        assert_eq!(temporary_files, 0);
    }

    #[test]
    fn execution_result_round_trips_without_raw_evidence_payloads() {
        let result = QaScenarioExecutionResult {
            schema_version: EXECUTION_RESULT_SCHEMA_VERSION,
            format: EXECUTION_RESULT_FORMAT.to_owned(),
            execution_key: test_execution_key(),
            attempt: QaScenarioAttemptProvenance {
                generation: 1,
                runner_version: "qa-runner.test".to_owned(),
                runtime_version: "palyrad-test".to_owned(),
                runtime_contract_version: "runtime-contracts.test".to_owned(),
                palyrad_binary_sha256: "1".repeat(64),
                palyrad_version: "0.1.0".to_owned(),
                palyrad_git_hash: "test".to_owned(),
                palyrad_build_profile: "debug".to_owned(),
                previous_result_artifact: None,
            },
            execution_id: "01ARZ3NDEKTSV4RRFFQ69G5FAT".to_owned(),
            scenario_id: "qa.result".to_owned(),
            runner_mode: "fixture".to_owned(),
            verdict: "failed".to_owned(),
            reason_codes: vec!["qa.runner.run_timeout".to_owned()],
            runtime_path: test_runtime_path_evidence(
                "palyrad-test",
                "runtime-contracts.test",
                "qa-runner.test",
                "fixture",
            ),
            run_id: None,
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAU".to_owned()),
            terminal_state: None,
            evidence_artifacts: Vec::new(),
            evidence_output_bindings: Vec::new(),
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
    fn passed_execution_result_requires_a_physical_evidence_artifact() {
        let failure_diagnostics = QaExecutionArtifactRef {
            path: "executions/test/failure-diagnostics.json".to_owned(),
            kind: "failure_diagnostics".to_owned(),
            sha256: "2".repeat(64),
            size_bytes: 128,
        };
        let mut result = QaScenarioExecutionResult {
            schema_version: EXECUTION_RESULT_SCHEMA_VERSION,
            format: EXECUTION_RESULT_FORMAT.to_owned(),
            execution_key: test_execution_key(),
            attempt: QaScenarioAttemptProvenance {
                generation: 1,
                runner_version: "qa-runner.test".to_owned(),
                runtime_version: "palyrad-test".to_owned(),
                runtime_contract_version: "runtime-contracts.test".to_owned(),
                palyrad_binary_sha256: "1".repeat(64),
                palyrad_version: "0.1.0".to_owned(),
                palyrad_git_hash: "test".to_owned(),
                palyrad_build_profile: "debug".to_owned(),
                previous_result_artifact: None,
            },
            execution_id: "01ARZ3NDEKTSV4RRFFQ69G5FAT".to_owned(),
            scenario_id: "qa.result".to_owned(),
            runner_mode: "fixture".to_owned(),
            verdict: "passed".to_owned(),
            reason_codes: Vec::new(),
            runtime_path: test_runtime_path_evidence(
                "palyrad-test",
                "runtime-contracts.test",
                "qa-runner.test",
                "fixture",
            ),
            run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAU".to_owned()),
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            terminal_state: Some("completed".to_owned()),
            evidence_artifacts: vec![failure_diagnostics],
            evidence_output_bindings: Vec::new(),
            cleanup: QaScenarioCleanupResult {
                run_terminal_observed: true,
                session_cleaned: true,
                daemon_terminated: true,
                workspace_removed: true,
                verified: true,
                reason_codes: vec!["qa.runner.cleanup_verified".to_owned()],
            },
        };

        let error = validate_execution_result(&result)
            .expect_err("failure diagnostics alone must not prove a passing execution");
        assert_eq!(error.to_string(), "qa.runner.execution_result_incomplete");

        let mut failed = result.clone();
        failed.verdict = "failed".to_owned();
        validate_execution_result(&failed)
            .expect("v3 failed results may retain only failure diagnostics");

        let mut missing_attestation = failed.clone();
        missing_attestation.runtime_path.complete = false;
        missing_attestation.runtime_path.provider_lane = "unobserved".to_owned();
        missing_attestation.runtime_path.reason_codes =
            vec!["qa.runner.runtime_path_provider_attestation_missing".to_owned()];
        validate_execution_result(&missing_attestation)
            .expect("missing provider attestation must remain a durable failed descriptor");

        let mut mismatched_attestation = failed.clone();
        mismatched_attestation.runtime_path.complete = false;
        mismatched_attestation.runtime_path.provider_lane = "record_replay".to_owned();
        mismatched_attestation.runtime_path.reason_codes =
            vec!["qa.runner.runtime_path_provider_lane_mismatch".to_owned()];
        validate_execution_result(&mismatched_attestation)
            .expect("provider lane mismatch must remain a durable failed descriptor");

        result.evidence_artifacts.push(QaExecutionArtifactRef {
            path: "executions/test/evidence.json".to_owned(),
            kind: QaScenarioArtifactKind::Evidence.as_str().to_owned(),
            sha256: "3".repeat(64),
            size_bytes: 256,
        });
        validate_execution_result(&result)
            .expect("a physical evidence artifact should satisfy the passing v3 contract");
    }

    #[test]
    fn evidence_output_binding_keeps_the_alias_distinct_from_physical_bytes() {
        let manifest = palyra_common::qa_scenarios::parse_qa_scenario_manifest_yaml(include_str!(
            "../../../../qa/scenarios/real_runtime/text_exact.yaml"
        ))
        .expect("scenario manifest should parse");
        let physical = QaExecutionArtifactRef {
            path: "executions/execution-id/key/evidence.json".to_owned(),
            kind: QaScenarioArtifactKind::Evidence.as_str().to_owned(),
            sha256: "a".repeat(64),
            size_bytes: 128,
        };

        let contract = bind_evidence_outputs(&manifest, &physical);

        assert!(contract.reason_codes.is_empty());
        assert_eq!(contract.bindings.len(), 1);
        assert_eq!(
            contract.bindings[0].logical_alias,
            "qa/reports/real_runtime/text_exact.evidence.json"
        );
        assert_ne!(contract.bindings[0].logical_alias, physical.path);
        assert_eq!(contract.bindings[0].artifact, physical);
    }

    #[test]
    fn evidence_output_digest_is_checked_against_the_physical_reference() {
        let mut manifest = palyra_common::qa_scenarios::parse_qa_scenario_manifest_yaml(
            include_str!("../../../../qa/scenarios/real_runtime/text_exact.yaml"),
        )
        .expect("scenario manifest should parse");
        manifest
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.kind == QaScenarioArtifactKind::Evidence)
            .expect("scenario should declare evidence output")
            .sha256 = Some("b".repeat(64));
        let physical = QaExecutionArtifactRef {
            path: "executions/execution-id/key/evidence.json".to_owned(),
            kind: QaScenarioArtifactKind::Evidence.as_str().to_owned(),
            sha256: "a".repeat(64),
            size_bytes: 128,
        };

        let contract = bind_evidence_outputs(&manifest, &physical);

        assert_eq!(contract.reason_codes, vec!["artifact_digest_mismatch"]);
        assert_eq!(contract.bindings[0].artifact.sha256, physical.sha256);
    }

    #[test]
    fn evidence_json_writer_rejects_before_crossing_the_64_mib_cap() {
        assert_eq!(MAX_EVIDENCE_ARTIFACT_BYTES, 64 * 1024 * 1024);
        assert!(json_write_exceeds_limit(
            MAX_EVIDENCE_ARTIFACT_BYTES,
            1,
            MAX_EVIDENCE_ARTIFACT_BYTES,
        ));
        let payload = json!({"payload": "x".repeat(128)});

        let error = encode_evidence_artifact_with_limit(&payload, 64)
            .expect_err("pretty JSON larger than the configured cap must be rejected");

        assert_eq!(error.to_string(), "qa.runner.evidence_artifact_limit_exceeded");
    }

    fn test_execution_key() -> QaScenarioExecutionKey {
        QaScenarioExecutionKey {
            schema_version: EXECUTION_KEY_SCHEMA_VERSION,
            format: EXECUTION_KEY_FORMAT.to_owned(),
            digest: "2".repeat(64),
            normalized_manifest_sha256: "3".repeat(64),
            fixture_set_sha256: "4".repeat(64),
            runtime_version: "palyrad-test".to_owned(),
            runtime_contract_version: "runtime-contracts.test".to_owned(),
            runner_version: "qa-runner.test".to_owned(),
            provider_lane: "fixture".to_owned(),
            provider_binding_sha256: "5".repeat(64),
        }
    }

    fn test_auth_profile(kind: AuthProviderKind) -> AuthProfileRecord {
        palyra_auth::AuthProfileRecord {
            profile_id: "qa-live-test".to_owned(),
            provider: palyra_auth::AuthProvider::known(kind),
            profile_name: "QA live test".to_owned(),
            scope: palyra_auth::AuthProfileScope::Global,
            credential: palyra_auth::AuthCredential::ApiKey {
                api_key_vault_ref: "global/qa-live-test".to_owned(),
            },
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
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
    fn execution_key_changes_with_manifest_and_runtime_contract() {
        let mut changed_manifest = palyra_common::qa_scenarios::parse_qa_scenario_manifest_yaml(
            include_str!("../../../../qa/scenarios/real_runtime/text_exact.yaml"),
        )
        .expect("baseline manifest should parse");
        let baseline_manifest = changed_manifest.clone();
        changed_manifest.timeout.run_ms += 1;
        let fixture_digest = "4".repeat(64);
        let binding_digest = "5".repeat(64);
        let baseline = build_execution_key(
            &baseline_manifest,
            fixture_digest.clone(),
            "palyrad-test",
            "runtime-contracts.test",
            "qa-runner.test",
            binding_digest.clone(),
        )
        .expect("baseline key should build");
        let changed_manifest = build_execution_key(
            &changed_manifest,
            fixture_digest.clone(),
            "palyrad-test",
            "runtime-contracts.test",
            "qa-runner.test",
            binding_digest.clone(),
        )
        .expect("changed manifest key should build");
        let changed_contract = build_execution_key(
            &baseline_manifest,
            fixture_digest,
            "palyrad-test",
            "runtime-contracts.next",
            "qa-runner.test",
            binding_digest,
        )
        .expect("changed contract key should build");

        assert_ne!(
            baseline.normalized_manifest_sha256,
            changed_manifest.normalized_manifest_sha256
        );
        assert_ne!(baseline.digest, changed_manifest.digest);
        assert_ne!(baseline.digest, changed_contract.digest);
    }

    #[test]
    fn replay_redaction_rejects_every_non_string_sensitive_value() {
        for value in [
            json!({"input_json": {"api_key": 123456789}}),
            json!({"input_json": {"authorization": {"encoded": "opaque"}}}),
            json!({"input_json": {"access_token": [115, 107, 45]}}),
            json!({"text": "provider error token=abc"}),
            json!({"text": "provider endpoint https://operator:secret@example.invalid/v1"}),
            json!({"text": "provider endpoint https://example.invalid/v1?api_key=abc"}),
            json!({"text": "provider endpoint https://example.invalid/v1#access_token=abc"}),
        ] {
            assert!(
                validate_redacted_replay_value(&value).is_err(),
                "non-string sensitive values must fail closed"
            );
        }
        validate_redacted_replay_value(&json!({
            "input_json": {
                "api_key": "<redacted>",
                "authorization": "[REDACTED]"
            },
            "prompt_tokens": 10,
            "completion_tokens": 7
        }))
        .expect("explicit redaction sentinels should remain valid");
    }

    #[test]
    fn replay_redaction_rejects_inline_comment_secrets_without_misreading_quotes() {
        let fixture =
            include_str!("../../../../qa/fixtures/record_replay/real_agent_runner_replay.yaml");
        let root = tempfile::tempdir().expect("replay fixture root should be available");
        let inline_secret =
            fixture.replacen("latency_ms: 2", "latency_ms: 2 # api_key=sk-inline-secret", 1);
        let inline_secret_path = root.path().join("inline-secret.yaml");
        fs::write(&inline_secret_path, inline_secret)
            .expect("inline secret fixture should be written");

        assert_eq!(
            validate_redacted_replay_fixture(&inline_secret_path)
                .expect_err("inline comment secret must fail closed")
                .to_string(),
            "qa.runner.replay_fixture_secret_material"
        );
        for line in ["text: 'literal # section'", "text: \"literal # section\""] {
            let candidate = yaml_comment_text(line).expect("ambiguous hash should be scanned");
            assert_eq!(redact_diagnostic_text(candidate), candidate);
        }
        assert_eq!(yaml_comment_text("text: 'literal ''#'' section'"), None);
        assert_eq!(yaml_comment_text("text: \"literal \\\"#\\\" section\""), None);
        assert_eq!(yaml_comment_text("text: safe # token=unsafe"), Some(" token=unsafe"));
        assert_eq!(
            yaml_comment_text("text: don't leak # api_key=sk-inline-secret"),
            Some(" api_key=sk-inline-secret")
        );
        assert_eq!(
            yaml_comment_text("text: unmatched \" quote # token=inline-secret"),
            Some(" token=inline-secret")
        );
        assert_eq!(
            yaml_comment_text("text: 'it''s safe' # api_key=sk-inline-secret"),
            Some(" api_key=sk-inline-secret")
        );
        assert_eq!(
            yaml_comment_text("text: \"say \\\"hello\\\"\" # token=inline-secret"),
            Some(" token=inline-secret")
        );
        assert_eq!(yaml_comment_text("text: value#not-a-comment"), None);
        assert_eq!(
            yaml_comment_text("\u{feff}# api_key=sk-bom-secret"),
            Some(" api_key=sk-bom-secret")
        );

        let bom_secret_path = root.path().join("bom-secret.yaml");
        fs::write(&bom_secret_path, format!("\u{feff}# api_key=sk-bom-secret\n{fixture}"))
            .expect("BOM-prefixed secret fixture should be written");
        assert_eq!(
            validate_redacted_replay_fixture(&bom_secret_path)
                .expect_err("BOM-prefixed comment secret must fail closed")
                .to_string(),
            "qa.runner.replay_fixture_secret_material"
        );
    }

    #[test]
    fn live_profile_binding_rejects_cross_vendor_and_custom_endpoints() {
        let openai = test_auth_profile(AuthProviderKind::Openai);
        let anthropic = test_auth_profile(AuthProviderKind::Anthropic);

        assert_eq!(
            validate_live_profile_provider(
                &openai,
                QaScenarioLiveProviderKind::OpenAiCompatible,
                None,
            )
            .expect("OpenAI profile should bind to OpenAI-compatible transport"),
            "openai"
        );
        assert_eq!(
            validate_live_profile_provider(
                &anthropic,
                QaScenarioLiveProviderKind::Anthropic,
                None,
            )
            .expect("Anthropic profile should bind to Anthropic transport"),
            "anthropic"
        );
        assert!(validate_live_profile_provider(
            &openai,
            QaScenarioLiveProviderKind::Anthropic,
            None,
        )
        .is_err());
        assert!(validate_live_profile_provider(
            &openai,
            QaScenarioLiveProviderKind::OpenAiCompatible,
            Some("https://example.invalid/v1"),
        )
        .is_err());
    }

    #[test]
    fn fixture_set_digest_is_order_independent_and_content_addressed() {
        let root = tempfile::tempdir().expect("fixture root should be available");
        let root = fs::canonicalize(root.path()).expect("fixture root should canonicalize");
        let fixture_dir = root.join("fixtures");
        fs::create_dir_all(fixture_dir.as_path()).expect("fixture directory should be created");
        fs::write(fixture_dir.join("first.json"), b"first")
            .expect("first fixture should be written");
        fs::write(fixture_dir.join("second.json"), b"second")
            .expect("second fixture should be written");

        let forward = digest_repository_fixture_set(
            root.as_path(),
            ["fixtures/first.json", "fixtures/second.json"],
        )
        .expect("forward fixture set should hash");
        let reversed = digest_repository_fixture_set(
            root.as_path(),
            ["fixtures/second.json", "fixtures/first.json"],
        )
        .expect("reversed fixture set should hash");
        fs::write(fixture_dir.join("second.json"), b"changed")
            .expect("second fixture should be updated");
        let changed = digest_repository_fixture_set(
            root.as_path(),
            ["fixtures/first.json", "fixtures/second.json"],
        )
        .expect("changed fixture set should hash");

        assert_eq!(forward, reversed);
        assert_ne!(forward, changed);
    }

    #[test]
    fn fault_campaign_fixture_digest_is_platform_stable() {
        let repository_root = fs::canonicalize(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(Path::parent)
                .expect("QA fixtures should have a repository root"),
        )
        .expect("QA fixture repository root should canonicalize");

        let digest = digest_repository_fixture_set(
            repository_root.as_path(),
            [
                "qa/fixtures/fault_injection_runner.yaml",
                "qa/fixtures/sandbox_workspaces/repo_basic",
            ],
        )
        .expect("fault campaign fixture set should hash");

        assert_eq!(digest, "ab13275c48c5ea1b098833a83d38ea93aa7e15bb854d61bf8d51fed7fbdb44ca");
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
