//! Crash-safe, parent-owned checkpoints for long-running QA campaigns.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, TryLockError},
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::qa_runner::{
    read_verified_artifact, sha256_hex, write_artifact_bytes, QaExecutionArtifactRef,
    QaScenarioExecutionKey, QaScenarioExecutionResult,
};

const CHECKPOINT_SCHEMA_VERSION: u32 = 1;
const CHECKPOINT_FORMAT: &str = "palyra-qa-campaign-checkpoint";
const CHECKPOINT_ARTIFACT_KIND: &str = "campaign_checkpoint";
const EXECUTION_RESULT_SCHEMA_VERSION: u32 = 3;
const EXECUTION_RESULT_FORMAT: &str = "palyra-qa-scenario-execution-result";
const EXECUTION_RESULT_ARTIFACT_KIND: &str = "execution_result";
const MAX_ATTEMPT_REASON_CODES: usize = 64;
const COORDINATOR_LOCK_ACQUIRE_GRACE: Duration = Duration::from_millis(250);
const COORDINATOR_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(10);

/// Returns the pinned durable checkpoint contract used by QA recovery tooling.
#[cfg(test)]
pub(crate) fn qa_campaign_checkpoint_schema_snapshot() -> serde_json::Value {
    serde_json::json!({
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "format": CHECKPOINT_FORMAT,
        "artifact_kind": CHECKPOINT_ARTIFACT_KIND,
        "top_level_fields": [
            "schema_version",
            "format",
            "campaign_key",
            "suite_id",
            "generation",
            "runner_version",
            "entries"
        ],
        "entry_map_key": "execution_key.digest",
        "entry_fields": [
            "execution_key",
            "scenario_id",
            "current_attempt_generation",
            "attempts"
        ],
        "execution_key_contract": "palyra-qa-scenario-execution-key/v1",
        "attempt_fields": [
            "generation",
            "execution_id",
            "state",
            "runner_version",
            "resume_reason_code",
            "expected_result_path",
            "result_artifact",
            "previous_result_artifact",
            "reason_codes"
        ],
        "optional_attempt_fields": [
            "result_artifact",
            "previous_result_artifact"
        ],
        "attempt_states": ["partial", "passed", "failed"],
        "artifact_reference": {
            "fields": ["path", "kind", "sha256", "size_bytes"],
            "path": "relative_no_parent_components",
            "sha256": "lowercase_hex_64",
            "size_bytes": "positive_u64",
            "execution_result_kind": EXECUTION_RESULT_ARTIFACT_KIND
        },
        "write_posture": {
            "checkpoint_file_name": "zero_padded_generation-sha256.json",
            "checkpoint_publication": "same_directory_temp_sync_atomic_no_clobber",
            "append_only": true,
            "coordinator": "exclusive_os_file_lock"
        },
        "recovery_posture": {
            "highest_valid_snapshot": "selected",
            "invalid_at_or_after_latest": "force_rerun_all",
            "conflicting_generation": "force_rerun_all",
            "temporary_files": "ignored",
            "partial_attempt": "rerun",
            "failed_attempt": "rerun",
            "passed_attempt": "reuse_only_after_result_and_evidence_verification"
        }
    })
}

/// Stable campaign identity derived from suite content and the runner version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct QaCampaignIdentity {
    pub(crate) suite_id: String,
    pub(crate) campaign_key: String,
    pub(crate) runner_version: String,
}

impl QaCampaignIdentity {
    pub(crate) fn new(
        suite_id: &str,
        normalized_suite: &[u8],
        runner_version: &str,
    ) -> Result<Self> {
        let suite_id = suite_id.trim();
        let runner_version = runner_version.trim();
        if suite_id.is_empty() || normalized_suite.is_empty() || runner_version.is_empty() {
            anyhow::bail!("qa.resume.campaign_identity_invalid");
        }
        let mut material =
            Vec::with_capacity(suite_id.len() + normalized_suite.len() + runner_version.len() + 64);
        append_hash_field(&mut material, b"format", b"palyra-qa-campaign-key-v1");
        append_hash_field(&mut material, b"suite_id", suite_id.as_bytes());
        append_hash_field(&mut material, b"suite", normalized_suite);
        append_hash_field(&mut material, b"runner_version", runner_version.as_bytes());
        let identity = Self {
            suite_id: suite_id.to_owned(),
            campaign_key: sha256_hex(material.as_slice()),
            runner_version: runner_version.to_owned(),
        };
        validate_campaign_identity(&identity)?;
        Ok(identity)
    }
}

/// Suite-level resume posture. Failed and partial attempts always rerun.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QaResumePolicy {
    pub(crate) enabled: bool,
    pub(crate) reuse_passed: bool,
}

/// Durable lifecycle state for one attempt generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QaCheckpointAttemptState {
    Partial,
    Passed,
    Failed,
}

impl QaCheckpointAttemptState {
    fn verdict(self) -> Option<&'static str> {
        match self {
            Self::Partial => None,
            Self::Passed => Some("passed"),
            Self::Failed => Some("failed"),
        }
    }
}

/// One immutable attempt record retained in scenario history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QaCheckpointAttempt {
    pub(crate) generation: u64,
    pub(crate) execution_id: String,
    pub(crate) state: QaCheckpointAttemptState,
    pub(crate) runner_version: String,
    pub(crate) resume_reason_code: String,
    pub(crate) expected_result_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) result_artifact: Option<QaExecutionArtifactRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) previous_result_artifact: Option<QaExecutionArtifactRef>,
    pub(crate) reason_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaCheckpointEntry {
    execution_key: QaScenarioExecutionKey,
    scenario_id: String,
    current_attempt_generation: u64,
    attempts: Vec<QaCheckpointAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaCampaignCheckpoint {
    schema_version: u32,
    format: String,
    campaign_key: String,
    suite_id: String,
    generation: u64,
    runner_version: String,
    entries: BTreeMap<String, QaCheckpointEntry>,
}

impl QaCampaignCheckpoint {
    fn empty(identity: &QaCampaignIdentity) -> Self {
        Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            format: CHECKPOINT_FORMAT.to_owned(),
            campaign_key: identity.campaign_key.clone(),
            suite_id: identity.suite_id.clone(),
            generation: 0,
            runner_version: identity.runner_version.clone(),
            entries: BTreeMap::new(),
        }
    }
}

/// Parent-issued attempt identity passed to a scenario worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QaAttemptToken {
    pub(crate) execution_key: QaScenarioExecutionKey,
    pub(crate) attempt_generation: u64,
    pub(crate) execution_id: String,
    pub(crate) runner_version: String,
    pub(crate) expected_result_path: String,
    pub(crate) previous_result_artifact: Option<QaExecutionArtifactRef>,
}

/// Validated worker descriptor used to finish a parent-owned attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QaAttemptCompletion {
    pub(crate) execution_key: QaScenarioExecutionKey,
    pub(crate) attempt_generation: u64,
    pub(crate) execution_id: String,
    pub(crate) runner_version: String,
    pub(crate) verdict: String,
    pub(crate) result_artifact: QaExecutionArtifactRef,
    pub(crate) previous_result_artifact: Option<QaExecutionArtifactRef>,
    pub(crate) reason_codes: Vec<String>,
}

/// Parent decision before an attempt is started or a passed result is reused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QaResumeDecision {
    Run {
        reason_code: String,
    },
    Reuse {
        reason_code: String,
        attempt_generation: u64,
        execution_id: String,
        result_artifact: QaExecutionArtifactRef,
    },
}

/// Redacted aggregate-report projection of checkpoint state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct QaCampaignCheckpointReport {
    pub(crate) campaign_key: String,
    pub(crate) checkpoint_generation: u64,
    pub(crate) runner_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) checkpoint_artifact: Option<QaExecutionArtifactRef>,
    pub(crate) recovery_reason_codes: Vec<String>,
}

/// Exclusive operating-system lock retained for the coordinator lifetime.
struct QaCheckpointCoordinatorLock {
    _file: fs::File,
}

impl QaCheckpointCoordinatorLock {
    fn acquire(artifact_root: &Path, campaign_key: &str) -> Result<Self> {
        let directory = artifact_root.join(campaign_directory(campaign_key));
        fs::create_dir_all(directory.as_path()).with_context(|| {
            format!("qa.resume.coordinator_directory_create_failed: {}", directory.display())
        })?;
        let lock_path = directory.join("coordinator.lock");
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path.as_path())
            .with_context(|| {
                format!("qa.resume.coordinator_lock_open_failed: {}", lock_path.display())
            })?;
        let started_at = Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock)
                    if started_at.elapsed() < COORDINATOR_LOCK_ACQUIRE_GRACE =>
                {
                    // A close or terminated coordinator can become observable just after the
                    // holder disappears. Retrying only the exclusive non-blocking lock preserves
                    // single-owner authority while avoiding a false restart refusal.
                    std::thread::sleep(COORDINATOR_LOCK_RETRY_INTERVAL);
                }
                Err(error) => {
                    return Err(anyhow::anyhow!("qa.resume.coordinator_lock_unavailable: {error}"));
                }
            }
        }
        Ok(Self { _file: file })
    }
}

/// Mutable in-memory coordinator state; only this parent publishes snapshots.
pub(crate) struct QaCheckpointStore {
    artifact_root: PathBuf,
    identity: QaCampaignIdentity,
    checkpoint: QaCampaignCheckpoint,
    latest_checkpoint_artifact: Option<QaExecutionArtifactRef>,
    recovery_reason_codes: BTreeSet<String>,
    force_rerun_all: bool,
    _coordinator_lock: QaCheckpointCoordinatorLock,
}

impl QaCheckpointStore {
    pub(crate) fn load(artifact_root: &Path, identity: QaCampaignIdentity) -> Result<Self> {
        validate_campaign_identity(&identity)?;
        let coordinator_lock =
            QaCheckpointCoordinatorLock::acquire(artifact_root, identity.campaign_key.as_str())?;
        let checkpoint_directory = checkpoint_directory(identity.campaign_key.as_str());
        let absolute_directory = artifact_root.join(checkpoint_directory.as_path());
        if !absolute_directory.exists() {
            return Ok(Self::fresh(artifact_root, identity, coordinator_lock));
        }
        if !absolute_directory.is_dir() {
            anyhow::bail!("qa.resume.checkpoint_directory_invalid");
        }

        let mut valid_snapshots = Vec::new();
        let mut invalid_generations = Vec::new();
        for entry in fs::read_dir(absolute_directory.as_path()).with_context(|| {
            format!("qa.resume.checkpoint_directory_read_failed: {}", absolute_directory.display())
        })? {
            let entry = entry.context("qa.resume.checkpoint_entry_read_failed")?;
            if !entry.file_type().context("qa.resume.checkpoint_entry_stat_failed")?.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some((generation, expected_digest)) = parse_checkpoint_file_name(file_name) else {
                // Temporary and unrelated files are never completion evidence.
                continue;
            };
            match load_checkpoint_snapshot(
                artifact_root,
                checkpoint_directory.as_path(),
                file_name,
                generation,
                expected_digest,
                &identity,
            ) {
                Ok(snapshot) => valid_snapshots.push(snapshot),
                Err(_) => invalid_generations.push(generation),
            }
        }

        valid_snapshots.sort_by(|left, right| {
            left.0.generation.cmp(&right.0.generation).then(left.1.path.cmp(&right.1.path))
        });
        let newest_valid_generation =
            valid_snapshots.last().map_or(0, |(checkpoint, _)| checkpoint.generation);
        let conflicting_generation = valid_snapshots
            .windows(2)
            .any(|snapshots| snapshots[0].0.generation == snapshots[1].0.generation);

        let (checkpoint, latest_checkpoint_artifact) = match valid_snapshots.pop() {
            Some((checkpoint, reference)) => (checkpoint, Some(reference)),
            None => (QaCampaignCheckpoint::empty(&identity), None),
        };
        let mut recovery_reason_codes = BTreeSet::new();
        if !invalid_generations.is_empty() {
            recovery_reason_codes.insert("qa.resume.checkpoint_corrupt".to_owned());
        }
        if conflicting_generation {
            recovery_reason_codes.insert("qa.resume.checkpoint_conflict".to_owned());
        }
        let invalid_at_or_after_latest =
            invalid_generations.iter().any(|generation| *generation >= newest_valid_generation);
        let force_rerun_all = invalid_at_or_after_latest || conflicting_generation;

        Ok(Self {
            artifact_root: artifact_root.to_path_buf(),
            identity,
            checkpoint,
            latest_checkpoint_artifact,
            recovery_reason_codes,
            force_rerun_all,
            _coordinator_lock: coordinator_lock,
        })
    }

    fn fresh(
        artifact_root: &Path,
        identity: QaCampaignIdentity,
        coordinator_lock: QaCheckpointCoordinatorLock,
    ) -> Self {
        Self {
            artifact_root: artifact_root.to_path_buf(),
            checkpoint: QaCampaignCheckpoint::empty(&identity),
            identity,
            latest_checkpoint_artifact: None,
            recovery_reason_codes: BTreeSet::new(),
            force_rerun_all: false,
            _coordinator_lock: coordinator_lock,
        }
    }

    pub(crate) fn resume_decision(
        &self,
        execution_key: &QaScenarioExecutionKey,
        policy: QaResumePolicy,
        force_rerun: bool,
    ) -> QaResumeDecision {
        if force_rerun {
            return run_decision("qa.resume.force_rerun");
        }
        if self.force_rerun_all {
            return run_decision("qa.resume.checkpoint_untrusted");
        }
        if !policy.enabled {
            return run_decision("qa.resume.disabled");
        }
        let Some(entry) = self.checkpoint.entries.get(execution_key.digest.as_str()) else {
            return run_decision("qa.resume.execution_key_new");
        };
        if entry.execution_key != *execution_key {
            return run_decision("qa.resume.execution_key_collision");
        }
        let Some(attempt) = current_attempt(entry) else {
            return run_decision("qa.resume.checkpoint_incomplete");
        };
        match attempt.state {
            QaCheckpointAttemptState::Partial => run_decision("qa.resume.partial_rerun"),
            QaCheckpointAttemptState::Failed => run_decision("qa.resume.failed_rerun"),
            QaCheckpointAttemptState::Passed if !policy.reuse_passed => {
                run_decision("qa.resume.passed_rerun")
            }
            QaCheckpointAttemptState::Passed => match attempt.result_artifact.as_ref() {
                Some(result_artifact)
                    if verify_reusable_result(self.artifact_root.as_path(), entry, attempt)
                        .is_ok() =>
                {
                    QaResumeDecision::Reuse {
                        reason_code: "qa.resume.passed_reused".to_owned(),
                        attempt_generation: attempt.generation,
                        execution_id: attempt.execution_id.clone(),
                        result_artifact: result_artifact.clone(),
                    }
                }
                Some(_) => run_decision("qa.resume.result_untrusted"),
                None => run_decision("qa.resume.result_missing"),
            },
        }
    }

    pub(crate) fn current_partial_attempt(
        &self,
        execution_key: &QaScenarioExecutionKey,
    ) -> Option<&QaCheckpointAttempt> {
        let entry = self.checkpoint.entries.get(execution_key.digest.as_str())?;
        if entry.execution_key != *execution_key {
            return None;
        }
        current_attempt(entry).filter(|attempt| attempt.state == QaCheckpointAttemptState::Partial)
    }

    /// Reconstructs the exact parent token for crash recovery of a partial attempt.
    pub(crate) fn current_partial_attempt_token(
        &self,
        execution_key: &QaScenarioExecutionKey,
    ) -> Option<QaAttemptToken> {
        self.current_partial_attempt(execution_key).map(|attempt| QaAttemptToken {
            execution_key: execution_key.clone(),
            attempt_generation: attempt.generation,
            execution_id: attempt.execution_id.clone(),
            runner_version: attempt.runner_version.clone(),
            expected_result_path: attempt.expected_result_path.clone(),
            previous_result_artifact: attempt.previous_result_artifact.clone(),
        })
    }

    pub(crate) fn begin_attempt(
        &mut self,
        execution_key: QaScenarioExecutionKey,
        scenario_id: &str,
        execution_id: &str,
        expected_result_path: &str,
        resume_reason_code: &str,
    ) -> Result<QaAttemptToken> {
        validate_execution_key(&execution_key)?;
        if execution_key.runner_version != self.identity.runner_version {
            anyhow::bail!("qa.resume.runner_version_mismatch");
        }
        validate_nonempty_identifier(scenario_id, "qa.resume.scenario_id_invalid")?;
        validate_nonempty_identifier(execution_id, "qa.resume.execution_id_invalid")?;
        validate_reason_code(resume_reason_code)?;
        validate_relative_artifact_path(expected_result_path)?;

        let previous_checkpoint = self.checkpoint.clone();
        let token = {
            let entry =
                self.checkpoint.entries.entry(execution_key.digest.clone()).or_insert_with(|| {
                    QaCheckpointEntry {
                        execution_key: execution_key.clone(),
                        scenario_id: scenario_id.to_owned(),
                        current_attempt_generation: 0,
                        attempts: Vec::new(),
                    }
                });
            if entry.execution_key != execution_key || entry.scenario_id != scenario_id {
                anyhow::bail!("qa.resume.execution_key_collision");
            }
            if entry.attempts.iter().any(|attempt| attempt.execution_id == execution_id) {
                anyhow::bail!("qa.resume.execution_id_reused");
            }
            if entry
                .attempts
                .iter()
                .any(|attempt| attempt.expected_result_path == expected_result_path)
            {
                anyhow::bail!("qa.resume.result_path_reused");
            }
            let attempt_generation = match entry.attempts.last() {
                Some(attempt) => attempt
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("qa.resume.attempt_generation_exhausted"))?,
                None => 1,
            };
            let previous_result_artifact =
                entry.attempts.iter().rev().find_map(|attempt| attempt.result_artifact.clone());
            entry.attempts.push(QaCheckpointAttempt {
                generation: attempt_generation,
                execution_id: execution_id.to_owned(),
                state: QaCheckpointAttemptState::Partial,
                runner_version: self.identity.runner_version.clone(),
                resume_reason_code: resume_reason_code.to_owned(),
                expected_result_path: expected_result_path.to_owned(),
                result_artifact: None,
                previous_result_artifact: previous_result_artifact.clone(),
                reason_codes: vec!["qa.resume.attempt_started".to_owned()],
            });
            entry.current_attempt_generation = attempt_generation;
            QaAttemptToken {
                execution_key,
                attempt_generation,
                execution_id: execution_id.to_owned(),
                runner_version: self.identity.runner_version.clone(),
                expected_result_path: expected_result_path.to_owned(),
                previous_result_artifact,
            }
        };
        if let Err(error) = self.publish_checkpoint() {
            self.checkpoint = previous_checkpoint;
            return Err(error);
        }
        Ok(token)
    }

    pub(crate) fn complete_attempt(
        &mut self,
        token: &QaAttemptToken,
        completion: QaAttemptCompletion,
    ) -> Result<()> {
        validate_completion(token, &completion)?;
        let scenario_id = {
            let entry = self
                .checkpoint
                .entries
                .get(token.execution_key.digest.as_str())
                .ok_or_else(|| anyhow::anyhow!("qa.resume.attempt_not_found"))?;
            validate_active_attempt(entry, token)?;
            entry.scenario_id.clone()
        };
        verify_result_artifact(
            self.artifact_root.as_path(),
            QaResultExpectation {
                execution_key: &token.execution_key,
                scenario_id: scenario_id.as_str(),
                generation: token.attempt_generation,
                execution_id: token.execution_id.as_str(),
                runner_version: token.runner_version.as_str(),
                verdict: completion.verdict.as_str(),
                expected_result_path: token.expected_result_path.as_str(),
                result_artifact: &completion.result_artifact,
                previous_result_artifact: &token.previous_result_artifact,
                reason_codes: completion.reason_codes.as_slice(),
            },
        )?;
        let previous_checkpoint = self.checkpoint.clone();
        {
            let entry = self
                .checkpoint
                .entries
                .get_mut(token.execution_key.digest.as_str())
                .ok_or_else(|| anyhow::anyhow!("qa.resume.attempt_not_found"))?;
            let attempt = entry
                .attempts
                .last_mut()
                .filter(|attempt| attempt.generation == token.attempt_generation)
                .ok_or_else(|| anyhow::anyhow!("qa.resume.attempt_not_found"))?;
            if attempt.state != QaCheckpointAttemptState::Partial
                || attempt.execution_id != token.execution_id
            {
                anyhow::bail!("qa.resume.attempt_not_partial");
            }
            attempt.state = if completion.verdict == "passed" {
                QaCheckpointAttemptState::Passed
            } else {
                QaCheckpointAttemptState::Failed
            };
            attempt.result_artifact = Some(completion.result_artifact);
            attempt.reason_codes = completion.reason_codes;
        }
        if let Err(error) = self.publish_checkpoint() {
            self.checkpoint = previous_checkpoint;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn recover_published_attempt(
        &mut self,
        token: &QaAttemptToken,
        completion: QaAttemptCompletion,
    ) -> Result<()> {
        self.complete_attempt(token, completion)
    }

    pub(crate) fn report(&self) -> QaCampaignCheckpointReport {
        QaCampaignCheckpointReport {
            campaign_key: self.identity.campaign_key.clone(),
            checkpoint_generation: self.checkpoint.generation,
            runner_version: self.identity.runner_version.clone(),
            checkpoint_artifact: self.latest_checkpoint_artifact.clone(),
            recovery_reason_codes: self.recovery_reason_codes.iter().cloned().collect(),
        }
    }

    fn publish_checkpoint(&mut self) -> Result<()> {
        let mut candidate = self.checkpoint.clone();
        candidate.generation = candidate
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("qa.resume.checkpoint_generation_exhausted"))?;
        validate_checkpoint(&candidate, &self.identity)?;
        let bytes =
            serde_json::to_vec_pretty(&candidate).context("qa.resume.checkpoint_encode_failed")?;
        let digest = sha256_hex(bytes.as_slice());
        let relative_path = checkpoint_directory(self.identity.campaign_key.as_str())
            .join(format!("{:020}-{digest}.json", candidate.generation));
        let reference = write_artifact_bytes(
            self.artifact_root.as_path(),
            relative_path.as_path(),
            CHECKPOINT_ARTIFACT_KIND,
            bytes.as_slice(),
        )?;
        let verified = read_verified_artifact(self.artifact_root.as_path(), &reference)?;
        if verified != bytes {
            anyhow::bail!("qa.resume.checkpoint_verify_failed");
        }
        self.checkpoint = candidate;
        self.latest_checkpoint_artifact = Some(reference);
        Ok(())
    }
}

fn validate_completion(token: &QaAttemptToken, completion: &QaAttemptCompletion) -> Result<()> {
    validate_attempt_token(token)?;
    validate_execution_key(&completion.execution_key)?;
    if completion.execution_key != token.execution_key
        || completion.attempt_generation != token.attempt_generation
        || completion.execution_id != token.execution_id
        || completion.runner_version != token.runner_version
        || completion.result_artifact.path != token.expected_result_path
        || completion.previous_result_artifact != token.previous_result_artifact
        || !matches!(completion.verdict.as_str(), "passed" | "failed")
    {
        anyhow::bail!("qa.resume.attempt_completion_invalid");
    }
    validate_result_artifact_reference(&completion.result_artifact)?;
    validate_reason_codes(completion.reason_codes.as_slice())?;
    Ok(())
}

struct QaResultExpectation<'a> {
    execution_key: &'a QaScenarioExecutionKey,
    scenario_id: &'a str,
    generation: u64,
    execution_id: &'a str,
    runner_version: &'a str,
    verdict: &'a str,
    expected_result_path: &'a str,
    result_artifact: &'a QaExecutionArtifactRef,
    previous_result_artifact: &'a Option<QaExecutionArtifactRef>,
    reason_codes: &'a [String],
}

fn verify_reusable_result(
    artifact_root: &Path,
    entry: &QaCheckpointEntry,
    attempt: &QaCheckpointAttempt,
) -> Result<()> {
    let result_artifact = attempt
        .result_artifact
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("qa.resume.result_missing"))?;
    let verdict =
        attempt.state.verdict().ok_or_else(|| anyhow::anyhow!("qa.resume.result_partial"))?;
    verify_result_artifact(
        artifact_root,
        QaResultExpectation {
            execution_key: &entry.execution_key,
            scenario_id: entry.scenario_id.as_str(),
            generation: attempt.generation,
            execution_id: attempt.execution_id.as_str(),
            runner_version: attempt.runner_version.as_str(),
            verdict,
            expected_result_path: attempt.expected_result_path.as_str(),
            result_artifact,
            previous_result_artifact: &attempt.previous_result_artifact,
            reason_codes: attempt.reason_codes.as_slice(),
        },
    )
}

fn verify_result_artifact(artifact_root: &Path, expected: QaResultExpectation<'_>) -> Result<()> {
    validate_result_artifact_reference(expected.result_artifact)?;
    if expected.result_artifact.path != expected.expected_result_path {
        anyhow::bail!("qa.resume.result_path_mismatch");
    }
    let bytes = read_verified_artifact(artifact_root, expected.result_artifact)?;
    let result = serde_json::from_slice::<QaScenarioExecutionResult>(bytes.as_slice())
        .context("qa.resume.execution_result_parse_failed")?;
    validate_execution_key(&result.execution_key)?;
    validate_sha256(result.attempt.palyrad_binary_sha256.as_str())?;
    if result.schema_version != EXECUTION_RESULT_SCHEMA_VERSION
        || result.format != EXECUTION_RESULT_FORMAT
        || &result.execution_key != expected.execution_key
        || result.attempt.generation != expected.generation
        || result.attempt.runner_version != expected.runner_version
        || result.attempt.runtime_version != expected.execution_key.runtime_version
        || result.attempt.runtime_contract_version
            != expected.execution_key.runtime_contract_version
        || result.attempt.previous_result_artifact.as_ref()
            != expected.previous_result_artifact.as_ref()
        || result.execution_id != expected.execution_id
        || result.scenario_id != expected.scenario_id
        || result.runner_mode != expected.execution_key.provider_lane
        || result.verdict != expected.verdict
        || result.reason_codes.as_slice() != expected.reason_codes
        || result.runtime_path.validate_shape().is_err()
        || result.runtime_path.runtime_version != expected.execution_key.runtime_version
        || result.runtime_path.runtime_contract_version
            != expected.execution_key.runtime_contract_version
        || result.runtime_path.runner_version != expected.runner_version
    {
        anyhow::bail!("qa.resume.execution_result_mismatch");
    }
    if result.verdict == "passed"
        && (result.run_id.is_none()
            || result.session_id.is_none()
            || result.terminal_state.is_none()
            || result.evidence_artifacts.is_empty()
            || !result.cleanup.verified)
    {
        anyhow::bail!("qa.resume.execution_result_incomplete");
    }
    for reference in &result.evidence_artifacts {
        validate_artifact_reference(reference)?;
        read_verified_artifact(artifact_root, reference)?;
    }
    Ok(())
}

fn validate_active_attempt(entry: &QaCheckpointEntry, token: &QaAttemptToken) -> Result<()> {
    if entry.execution_key != token.execution_key
        || entry.current_attempt_generation != token.attempt_generation
    {
        anyhow::bail!("qa.resume.attempt_token_stale");
    }
    let attempt =
        current_attempt(entry).ok_or_else(|| anyhow::anyhow!("qa.resume.attempt_not_found"))?;
    if attempt.state != QaCheckpointAttemptState::Partial
        || attempt.execution_id != token.execution_id
        || attempt.runner_version != token.runner_version
        || attempt.expected_result_path != token.expected_result_path
        || attempt.previous_result_artifact != token.previous_result_artifact
    {
        anyhow::bail!("qa.resume.attempt_not_partial");
    }
    Ok(())
}

fn validate_attempt_token(token: &QaAttemptToken) -> Result<()> {
    validate_execution_key(&token.execution_key)?;
    validate_nonempty_identifier(token.execution_id.as_str(), "qa.resume.execution_id_invalid")?;
    validate_nonempty_identifier(
        token.runner_version.as_str(),
        "qa.resume.runner_version_invalid",
    )?;
    validate_relative_artifact_path(token.expected_result_path.as_str())?;
    if token.attempt_generation == 0 || token.runner_version != token.execution_key.runner_version {
        anyhow::bail!("qa.resume.attempt_token_invalid");
    }
    if let Some(reference) = &token.previous_result_artifact {
        validate_result_artifact_reference(reference)?;
    }
    Ok(())
}

fn load_checkpoint_snapshot(
    artifact_root: &Path,
    checkpoint_directory: &Path,
    file_name: &str,
    generation: u64,
    expected_digest: &str,
    identity: &QaCampaignIdentity,
) -> Result<(QaCampaignCheckpoint, QaExecutionArtifactRef)> {
    let relative_path = checkpoint_directory.join(file_name);
    let bytes = fs::read(artifact_root.join(relative_path.as_path()))
        .context("qa.resume.checkpoint_read_failed")?;
    let actual_digest = sha256_hex(bytes.as_slice());
    if actual_digest != expected_digest {
        anyhow::bail!("qa.resume.checkpoint_digest_mismatch");
    }
    let checkpoint = serde_json::from_slice::<QaCampaignCheckpoint>(bytes.as_slice())
        .context("qa.resume.checkpoint_parse_failed")?;
    if checkpoint.generation != generation {
        anyhow::bail!("qa.resume.checkpoint_generation_mismatch");
    }
    validate_checkpoint(&checkpoint, identity)?;
    let reference = QaExecutionArtifactRef {
        path: display_path_slash(relative_path.as_path()),
        kind: CHECKPOINT_ARTIFACT_KIND.to_owned(),
        sha256: actual_digest,
        size_bytes: u64::try_from(bytes.len()).context("qa.resume.checkpoint_size_invalid")?,
    };
    Ok((checkpoint, reference))
}

fn validate_checkpoint(
    checkpoint: &QaCampaignCheckpoint,
    identity: &QaCampaignIdentity,
) -> Result<()> {
    validate_campaign_identity(identity)?;
    if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION
        || checkpoint.format != CHECKPOINT_FORMAT
        || checkpoint.campaign_key != identity.campaign_key
        || checkpoint.suite_id != identity.suite_id
        || checkpoint.runner_version != identity.runner_version
        || checkpoint.generation == 0
    {
        anyhow::bail!("qa.resume.checkpoint_invalid");
    }
    for (digest, entry) in &checkpoint.entries {
        validate_execution_key(&entry.execution_key)?;
        if digest != &entry.execution_key.digest
            || entry.execution_key.runner_version != identity.runner_version
            || entry.attempts.is_empty()
            || entry.current_attempt_generation
                != entry.attempts.last().map_or(0, |attempt| attempt.generation)
        {
            anyhow::bail!("qa.resume.checkpoint_entry_invalid");
        }
        validate_nonempty_identifier(
            entry.scenario_id.as_str(),
            "qa.resume.checkpoint_entry_invalid",
        )?;
        let mut expected_generation = 1_u64;
        let mut previous_result_artifact = None;
        let mut execution_ids = BTreeSet::new();
        let mut result_paths = BTreeSet::new();
        for attempt in &entry.attempts {
            if attempt.generation != expected_generation
                || attempt.runner_version != identity.runner_version
                || !execution_ids.insert(attempt.execution_id.as_str())
                || !result_paths.insert(attempt.expected_result_path.as_str())
                || attempt.previous_result_artifact.as_ref() != previous_result_artifact.as_ref()
            {
                anyhow::bail!("qa.resume.checkpoint_attempt_invalid");
            }
            validate_nonempty_identifier(
                attempt.execution_id.as_str(),
                "qa.resume.checkpoint_attempt_invalid",
            )?;
            validate_reason_code(attempt.resume_reason_code.as_str())?;
            validate_relative_artifact_path(attempt.expected_result_path.as_str())?;
            validate_reason_codes(attempt.reason_codes.as_slice())?;
            match (&attempt.state, &attempt.result_artifact) {
                (QaCheckpointAttemptState::Partial, None)
                | (QaCheckpointAttemptState::Passed | QaCheckpointAttemptState::Failed, Some(_)) => {
                }
                _ => anyhow::bail!("qa.resume.checkpoint_attempt_invalid"),
            }
            if let Some(reference) = &attempt.result_artifact {
                validate_result_artifact_reference(reference)?;
                if reference.path != attempt.expected_result_path {
                    anyhow::bail!("qa.resume.checkpoint_attempt_invalid");
                }
                previous_result_artifact = Some(reference.clone());
            }
            if let Some(reference) = &attempt.previous_result_artifact {
                validate_result_artifact_reference(reference)?;
            }
            expected_generation = expected_generation
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("qa.resume.attempt_generation_exhausted"))?;
        }
    }
    Ok(())
}

fn current_attempt(entry: &QaCheckpointEntry) -> Option<&QaCheckpointAttempt> {
    entry.attempts.last().filter(|attempt| attempt.generation == entry.current_attempt_generation)
}

fn run_decision(reason_code: &str) -> QaResumeDecision {
    QaResumeDecision::Run { reason_code: reason_code.to_owned() }
}

fn campaign_directory(campaign_key: &str) -> PathBuf {
    PathBuf::from("campaigns").join(campaign_key)
}

fn checkpoint_directory(campaign_key: &str) -> PathBuf {
    campaign_directory(campaign_key).join("checkpoints")
}

fn parse_checkpoint_file_name(file_name: &str) -> Option<(u64, &str)> {
    let stem = file_name.strip_suffix(".json")?;
    let (generation, digest) = stem.split_once('-')?;
    if generation.len() != 20
        || !generation.bytes().all(|byte| byte.is_ascii_digit())
        || validate_sha256(digest).is_err()
    {
        return None;
    }
    Some((generation.parse().ok()?, digest))
}

fn validate_artifact_reference(reference: &QaExecutionArtifactRef) -> Result<()> {
    validate_relative_artifact_path(reference.path.as_str())?;
    validate_sha256(reference.sha256.as_str())?;
    if reference.kind.trim().is_empty()
        || reference.kind.chars().any(char::is_control)
        || reference.size_bytes == 0
    {
        anyhow::bail!("qa.resume.artifact_reference_invalid");
    }
    Ok(())
}

fn validate_result_artifact_reference(reference: &QaExecutionArtifactRef) -> Result<()> {
    validate_artifact_reference(reference)?;
    if reference.kind != EXECUTION_RESULT_ARTIFACT_KIND {
        anyhow::bail!("qa.resume.result_artifact_kind_invalid");
    }
    Ok(())
}

fn validate_campaign_identity(identity: &QaCampaignIdentity) -> Result<()> {
    validate_nonempty_identifier(
        identity.suite_id.as_str(),
        "qa.resume.campaign_identity_invalid",
    )?;
    validate_nonempty_identifier(
        identity.runner_version.as_str(),
        "qa.resume.campaign_identity_invalid",
    )?;
    validate_sha256(identity.campaign_key.as_str()).context("qa.resume.campaign_identity_invalid")
}

fn validate_execution_key(execution_key: &QaScenarioExecutionKey) -> Result<()> {
    execution_key.validate_shape()?;
    for digest in [
        execution_key.digest.as_str(),
        execution_key.normalized_manifest_sha256.as_str(),
        execution_key.fixture_set_sha256.as_str(),
        execution_key.provider_binding_sha256.as_str(),
    ] {
        validate_sha256(digest).context("qa.resume.execution_key_invalid")?;
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        anyhow::bail!("qa.resume.sha256_invalid");
    }
    Ok(())
}

fn validate_relative_artifact_path(value: &str) -> Result<()> {
    let path = Path::new(value);
    if value.trim().is_empty()
        || path.is_absolute()
        || path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        anyhow::bail!("qa.resume.artifact_path_invalid");
    }
    Ok(())
}

fn validate_nonempty_identifier(value: &str, reason_code: &'static str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        anyhow::bail!(reason_code);
    }
    Ok(())
}

fn validate_reason_code(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 160
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
    {
        anyhow::bail!("qa.resume.reason_code_invalid");
    }
    Ok(())
}

fn validate_reason_codes(reason_codes: &[String]) -> Result<()> {
    if reason_codes.len() > MAX_ATTEMPT_REASON_CODES
        || reason_codes.windows(2).any(|codes| codes[0] >= codes[1])
    {
        anyhow::bail!("qa.resume.reason_codes_invalid");
    }
    for reason_code in reason_codes {
        validate_reason_code(reason_code)?;
    }
    Ok(())
}

fn append_hash_field(material: &mut Vec<u8>, label: &[u8], value: &[u8]) {
    material.extend_from_slice(&(label.len() as u64).to_be_bytes());
    material.extend_from_slice(label);
    material.extend_from_slice(&(value.len() as u64).to_be_bytes());
    material.extend_from_slice(value);
}

fn display_path_slash(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        process::{Child, Command, ExitStatus, Stdio},
        thread,
        time::{Duration, Instant},
    };

    use super::super::qa_runner::{QaScenarioAttemptProvenance, QaScenarioCleanupResult};
    use super::*;

    const CRASH_HELPER_ENV: &str = "PALYRA_QA_CHECKPOINT_CRASH_HELPER";
    const CRASH_HELPER_ROOT_ENV: &str = "PALYRA_QA_CHECKPOINT_CRASH_ROOT";
    const CRASH_HELPER_READY_PATH: &str = "checkpoint-crash-helper.ready";
    const CRASH_HELPER_TEST_NAME: &str =
        "commands::qa_checkpoint::tests::checkpoint_coordinator_crash_child_process";
    const TEST_RUNNER_VERSION: &str = "qa-runner.v2";

    struct KillOnDropChild {
        child: Option<Child>,
    }

    impl KillOnDropChild {
        fn spawn(artifact_root: &Path) -> Self {
            let child = Command::new(env::current_exe().expect("test executable should resolve"))
                .arg("--exact")
                .arg(CRASH_HELPER_TEST_NAME)
                .arg("--ignored")
                .arg("--nocapture")
                .env(CRASH_HELPER_ENV, "1")
                .env(CRASH_HELPER_ROOT_ENV, artifact_root)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("checkpoint crash helper should spawn");
            Self { child: Some(child) }
        }

        fn try_wait(&mut self) -> Option<ExitStatus> {
            self.child
                .as_mut()
                .expect("child should still be owned")
                .try_wait()
                .expect("checkpoint crash helper status should be readable")
        }

        fn terminate_and_wait(&mut self) -> ExitStatus {
            let mut child = self.child.take().expect("child should still be owned");
            if child
                .try_wait()
                .expect("checkpoint crash helper status should be readable")
                .is_none()
            {
                child.kill().expect("checkpoint crash helper should terminate");
            }
            child.wait().expect("checkpoint crash helper should be reaped")
        }
    }

    impl Drop for KillOnDropChild {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn campaign_identity() -> QaCampaignIdentity {
        QaCampaignIdentity::new(
            "qa.checkpoint.test",
            br#"{"suite":"qa.checkpoint.test"}"#,
            TEST_RUNNER_VERSION,
        )
        .expect("test campaign identity should be valid")
    }

    fn execution_key(seed: &str) -> QaScenarioExecutionKey {
        QaScenarioExecutionKey {
            schema_version: 1,
            format: "palyra-qa-scenario-execution-key".to_owned(),
            digest: sha256_hex(format!("execution:{seed}").as_bytes()),
            normalized_manifest_sha256: sha256_hex(format!("manifest:{seed}").as_bytes()),
            fixture_set_sha256: sha256_hex(format!("fixtures:{seed}").as_bytes()),
            runtime_version: "0.1.0-test".to_owned(),
            runtime_contract_version: "runtime-contracts.v8".to_owned(),
            runner_version: TEST_RUNNER_VERSION.to_owned(),
            provider_lane: "fixture".to_owned(),
            provider_binding_sha256: sha256_hex(format!("binding:{seed}").as_bytes()),
        }
    }

    fn resume_policy() -> QaResumePolicy {
        QaResumePolicy { enabled: true, reuse_passed: true }
    }

    fn begin_attempt(
        store: &mut QaCheckpointStore,
        execution_key: &QaScenarioExecutionKey,
        scenario_id: &str,
        generation: u64,
        reason_code: &str,
    ) -> QaAttemptToken {
        let execution_id = format!("execution-{}-{generation}", &execution_key.digest[..12]);
        let result_path =
            format!("scenarios/{}/attempts/{generation:020}/result.json", execution_key.digest);
        store
            .begin_attempt(
                execution_key.clone(),
                scenario_id,
                execution_id.as_str(),
                result_path.as_str(),
                reason_code,
            )
            .expect("attempt should begin")
    }

    fn write_result(
        artifact_root: &Path,
        token: &QaAttemptToken,
        scenario_id: &str,
        verdict: &str,
        reason_codes: &[&str],
    ) -> (QaExecutionArtifactRef, Vec<String>) {
        let evidence_path =
            Path::new(token.expected_result_path.as_str()).with_file_name("evidence.json");
        let evidence_artifact = write_artifact_bytes(
            artifact_root,
            evidence_path.as_path(),
            "evidence",
            br#"{"verdict":"bounded"}"#,
        )
        .expect("evidence should be written");
        let reason_codes = reason_codes.iter().map(ToString::to_string).collect::<Vec<_>>();
        let passed = verdict == "passed";
        let result = QaScenarioExecutionResult {
            schema_version: EXECUTION_RESULT_SCHEMA_VERSION,
            format: EXECUTION_RESULT_FORMAT.to_owned(),
            execution_key: token.execution_key.clone(),
            attempt: QaScenarioAttemptProvenance {
                generation: token.attempt_generation,
                runner_version: token.runner_version.clone(),
                runtime_version: token.execution_key.runtime_version.clone(),
                runtime_contract_version: token.execution_key.runtime_contract_version.clone(),
                palyrad_binary_sha256: sha256_hex(b"test-palyrad-binary"),
                palyrad_version: "0.1.0".to_owned(),
                palyrad_git_hash: "test".to_owned(),
                palyrad_build_profile: "debug".to_owned(),
                previous_result_artifact: token.previous_result_artifact.clone(),
            },
            execution_id: token.execution_id.clone(),
            scenario_id: scenario_id.to_owned(),
            runner_mode: token.execution_key.provider_lane.clone(),
            verdict: verdict.to_owned(),
            reason_codes: reason_codes.clone(),
            runtime_path: crate::commands::qa_runner::test_runtime_path_evidence(
                token.execution_key.runtime_version.as_str(),
                token.execution_key.runtime_contract_version.as_str(),
                token.runner_version.as_str(),
                token.execution_key.provider_lane.as_str(),
            ),
            run_id: passed.then_some("run-1".to_owned()),
            session_id: passed.then_some("session-1".to_owned()),
            terminal_state: passed.then_some("completed".to_owned()),
            evidence_artifacts: vec![evidence_artifact],
            evidence_output_bindings: Vec::new(),
            cleanup: QaScenarioCleanupResult {
                run_terminal_observed: passed,
                session_cleaned: true,
                daemon_terminated: true,
                workspace_removed: true,
                verified: passed,
                reason_codes: vec![if passed {
                    "qa.runner.cleanup_verified".to_owned()
                } else {
                    "qa.runner.cleanup_failed".to_owned()
                }],
            },
        };
        let bytes = serde_json::to_vec_pretty(&result).expect("result should serialize");
        let result_artifact = write_artifact_bytes(
            artifact_root,
            Path::new(token.expected_result_path.as_str()),
            EXECUTION_RESULT_ARTIFACT_KIND,
            bytes.as_slice(),
        )
        .expect("result should be written");
        (result_artifact, reason_codes)
    }

    fn completion(
        token: &QaAttemptToken,
        verdict: &str,
        result_artifact: QaExecutionArtifactRef,
        reason_codes: Vec<String>,
    ) -> QaAttemptCompletion {
        QaAttemptCompletion {
            execution_key: token.execution_key.clone(),
            attempt_generation: token.attempt_generation,
            execution_id: token.execution_id.clone(),
            runner_version: token.runner_version.clone(),
            verdict: verdict.to_owned(),
            result_artifact,
            previous_result_artifact: token.previous_result_artifact.clone(),
            reason_codes,
        }
    }

    fn assert_run_reason(decision: QaResumeDecision, expected: &str) {
        match decision {
            QaResumeDecision::Run { reason_code } => assert_eq!(reason_code, expected),
            QaResumeDecision::Reuse { .. } => panic!("expected a run decision"),
        }
    }

    fn wait_for_crash_helper_ready(child: &mut KillOnDropChild, ready_path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if ready_path.is_file() {
                return;
            }
            if let Some(status) = child.try_wait() {
                panic!("checkpoint crash helper exited before readiness with {status}");
            }
            assert!(Instant::now() < deadline, "checkpoint crash helper did not become ready");
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn checkpoint_files_are_append_only_and_content_addressed() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let identity = campaign_identity();
        let mut store =
            QaCheckpointStore::load(root.path(), identity).expect("checkpoint store should load");
        let key = execution_key("append-only");
        let token =
            begin_attempt(&mut store, &key, "qa.append-only", 1, "qa.resume.execution_key_new");
        let first =
            store.report().checkpoint_artifact.expect("partial checkpoint should be published");
        let (result_artifact, reason_codes) =
            write_result(root.path(), &token, "qa.append-only", "passed", &["qa.passed"]);
        store
            .complete_attempt(&token, completion(&token, "passed", result_artifact, reason_codes))
            .expect("attempt should complete");
        let second =
            store.report().checkpoint_artifact.expect("terminal checkpoint should be published");

        assert_ne!(first.path, second.path);
        for (reference, expected_generation) in [(&first, 1_u64), (&second, 2_u64)] {
            let path = root.path().join(reference.path.as_str());
            let bytes = fs::read(path.as_path()).expect("checkpoint should remain readable");
            let file_name = path
                .file_name()
                .and_then(|value| value.to_str())
                .expect("checkpoint filename should be UTF-8");
            let (generation, digest) =
                parse_checkpoint_file_name(file_name).expect("checkpoint filename should parse");
            assert_eq!(generation, expected_generation);
            assert_eq!(digest, sha256_hex(bytes.as_slice()));
            assert_eq!(reference.sha256, digest);
        }
        assert!(root.path().join(first.path).is_file());
        assert!(root.path().join(second.path).is_file());
    }

    #[test]
    fn checkpoint_schema_matches_golden() {
        let golden: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/golden/qa_campaign_checkpoint_schema.json"
        ))
        .expect("checkpoint schema golden should parse");

        assert_eq!(qa_campaign_checkpoint_schema_snapshot(), golden);
    }

    #[test]
    fn incompatible_checkpoint_schema_version_forces_a_fresh_rerun() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let identity = campaign_identity();
        let key = execution_key("future-checkpoint-schema");
        let mut incompatible = QaCampaignCheckpoint::empty(&identity);
        incompatible.schema_version = CHECKPOINT_SCHEMA_VERSION
            .checked_add(1)
            .expect("checkpoint schema version should have a successor");
        incompatible.generation = 1;
        let error = validate_checkpoint(&incompatible, &identity)
            .expect_err("a future checkpoint schema must fail closed");
        assert!(error.to_string().contains("qa.resume.checkpoint_invalid"));

        let bytes = serde_json::to_vec_pretty(&incompatible)
            .expect("incompatible checkpoint should serialize for the loader test");
        let directory = root.path().join(checkpoint_directory(identity.campaign_key.as_str()));
        fs::create_dir_all(directory.as_path()).expect("checkpoint directory should be created");
        let file_name =
            format!("{:020}-{}.json", incompatible.generation, sha256_hex(bytes.as_slice()));
        fs::write(directory.join(file_name), bytes)
            .expect("incompatible checkpoint should be persisted");

        let store = QaCheckpointStore::load(root.path(), identity)
            .expect("an incompatible checkpoint should recover as untrusted state");
        let report = store.report();
        assert_eq!(report.checkpoint_generation, 0);
        assert_eq!(report.checkpoint_artifact, None);
        assert_eq!(report.recovery_reason_codes, vec!["qa.resume.checkpoint_corrupt".to_owned()]);
        assert_run_reason(
            store.resume_decision(&key, resume_policy(), false),
            "qa.resume.checkpoint_untrusted",
        );
    }

    #[test]
    fn btree_entries_serialize_deterministically_across_insertion_order() {
        let left_root = tempfile::tempdir().expect("left artifact root should be available");
        let right_root = tempfile::tempdir().expect("right artifact root should be available");
        let identity = campaign_identity();
        let mut left = QaCheckpointStore::load(left_root.path(), identity.clone())
            .expect("left store should load");
        let mut right =
            QaCheckpointStore::load(right_root.path(), identity).expect("right store should load");
        let alpha = execution_key("alpha");
        let beta = execution_key("beta");

        begin_attempt(&mut left, &alpha, "qa.alpha", 1, "qa.resume.execution_key_new");
        begin_attempt(&mut left, &beta, "qa.beta", 1, "qa.resume.execution_key_new");
        begin_attempt(&mut right, &beta, "qa.beta", 1, "qa.resume.execution_key_new");
        begin_attempt(&mut right, &alpha, "qa.alpha", 1, "qa.resume.execution_key_new");

        assert_eq!(
            serde_json::to_vec(&left.checkpoint).expect("left checkpoint should serialize"),
            serde_json::to_vec(&right.checkpoint).expect("right checkpoint should serialize")
        );
    }

    #[test]
    fn partial_attempt_is_never_reusable() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let mut store = QaCheckpointStore::load(root.path(), campaign_identity())
            .expect("checkpoint store should load");
        let key = execution_key("partial");
        begin_attempt(&mut store, &key, "qa.partial", 1, "qa.resume.execution_key_new");

        assert_run_reason(
            store.resume_decision(&key, resume_policy(), false),
            "qa.resume.partial_rerun",
        );
    }

    #[test]
    fn passed_candidate_requires_verified_result_and_evidence() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let mut store = QaCheckpointStore::load(root.path(), campaign_identity())
            .expect("checkpoint store should load");
        let key = execution_key("passed");
        let token = begin_attempt(&mut store, &key, "qa.passed", 1, "qa.resume.execution_key_new");
        let (result_artifact, reason_codes) =
            write_result(root.path(), &token, "qa.passed", "passed", &["qa.passed"]);
        store
            .complete_attempt(
                &token,
                completion(&token, "passed", result_artifact.clone(), reason_codes),
            )
            .expect("attempt should complete");

        assert_eq!(
            store.resume_decision(&key, resume_policy(), false),
            QaResumeDecision::Reuse {
                reason_code: "qa.resume.passed_reused".to_owned(),
                attempt_generation: 1,
                execution_id: token.execution_id.clone(),
                result_artifact,
            }
        );
        let result_bytes = read_verified_artifact(
            root.path(),
            store
                .checkpoint
                .entries
                .get(key.digest.as_str())
                .and_then(current_attempt)
                .and_then(|attempt| attempt.result_artifact.as_ref())
                .expect("passed result reference should exist"),
        )
        .expect("result should remain readable");
        let result: QaScenarioExecutionResult =
            serde_json::from_slice(result_bytes.as_slice()).expect("result should parse");
        let evidence =
            result.evidence_artifacts.first().expect("passed result should reference evidence");
        fs::write(root.path().join(evidence.path.as_str()), b"corrupt")
            .expect("test should corrupt evidence");
        assert_run_reason(
            store.resume_decision(&key, resume_policy(), false),
            "qa.resume.result_untrusted",
        );
    }

    #[test]
    fn force_rerun_overrides_a_valid_passed_candidate() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let mut store = QaCheckpointStore::load(root.path(), campaign_identity())
            .expect("checkpoint store should load");
        let key = execution_key("force");
        let token = begin_attempt(&mut store, &key, "qa.force", 1, "qa.resume.execution_key_new");
        let (result_artifact, reason_codes) =
            write_result(root.path(), &token, "qa.force", "passed", &["qa.passed"]);
        store
            .complete_attempt(&token, completion(&token, "passed", result_artifact, reason_codes))
            .expect("attempt should complete");

        assert_run_reason(
            store.resume_decision(&key, resume_policy(), true),
            "qa.resume.force_rerun",
        );
    }

    #[test]
    fn failed_rerun_retains_history_and_previous_result_reference() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let mut store = QaCheckpointStore::load(root.path(), campaign_identity())
            .expect("checkpoint store should load");
        let key = execution_key("failed-rerun");
        let first =
            begin_attempt(&mut store, &key, "qa.failed-rerun", 1, "qa.resume.execution_key_new");
        let (first_result, reason_codes) =
            write_result(root.path(), &first, "qa.failed-rerun", "failed", &["qa.failed"]);
        store
            .complete_attempt(
                &first,
                completion(&first, "failed", first_result.clone(), reason_codes),
            )
            .expect("failed attempt should complete");
        assert_run_reason(
            store.resume_decision(&key, resume_policy(), false),
            "qa.resume.failed_rerun",
        );

        let second =
            begin_attempt(&mut store, &key, "qa.failed-rerun", 2, "qa.resume.failed_rerun");
        assert_eq!(second.attempt_generation, 2);
        assert_eq!(second.previous_result_artifact, Some(first_result.clone()));
        let entry = store
            .checkpoint
            .entries
            .get(key.digest.as_str())
            .expect("scenario history should exist");
        assert_eq!(entry.attempts.len(), 2);
        assert_eq!(entry.attempts[0].state, QaCheckpointAttemptState::Failed);
        assert_eq!(entry.attempts[0].result_artifact, Some(first_result.clone()));
        assert_eq!(entry.attempts[1].state, QaCheckpointAttemptState::Partial);
        assert_eq!(entry.attempts[1].previous_result_artifact, Some(first_result));
    }

    #[test]
    fn corrupt_highest_checkpoint_falls_back_and_forces_rerun() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let key = execution_key("corrupt-highest");
        let highest_path = {
            let mut store = QaCheckpointStore::load(root.path(), campaign_identity())
                .expect("checkpoint store should load");
            let token = begin_attempt(
                &mut store,
                &key,
                "qa.corrupt-highest",
                1,
                "qa.resume.execution_key_new",
            );
            let (result_artifact, reason_codes) =
                write_result(root.path(), &token, "qa.corrupt-highest", "passed", &["qa.passed"]);
            store
                .complete_attempt(
                    &token,
                    completion(&token, "passed", result_artifact, reason_codes),
                )
                .expect("attempt should complete");
            store.report().checkpoint_artifact.expect("terminal checkpoint should exist").path
        };
        fs::write(root.path().join(highest_path), b"corrupt checkpoint")
            .expect("test should corrupt highest checkpoint");

        let store = QaCheckpointStore::load(root.path(), campaign_identity())
            .expect("store should fall back to the prior checkpoint");
        let report = store.report();
        assert_eq!(report.checkpoint_generation, 1);
        assert!(report
            .recovery_reason_codes
            .iter()
            .any(|code| code == "qa.resume.checkpoint_corrupt"));
        assert_run_reason(
            store.resume_decision(&key, resume_policy(), false),
            "qa.resume.checkpoint_untrusted",
        );
    }

    #[test]
    fn temporary_checkpoint_files_are_ignored() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let identity = campaign_identity();
        let key = execution_key("temporary");
        {
            let mut store = QaCheckpointStore::load(root.path(), identity.clone())
                .expect("checkpoint store should load");
            let token =
                begin_attempt(&mut store, &key, "qa.temporary", 1, "qa.resume.execution_key_new");
            let (result_artifact, reason_codes) =
                write_result(root.path(), &token, "qa.temporary", "passed", &["qa.passed"]);
            store
                .complete_attempt(
                    &token,
                    completion(&token, "passed", result_artifact, reason_codes),
                )
                .expect("attempt should complete");
        }
        let directory = root.path().join(checkpoint_directory(identity.campaign_key.as_str()));
        fs::write(directory.join(".00000000000000000003-deadbeef.json.tmp"), b"partial")
            .expect("temporary checkpoint should be created");

        let store = QaCheckpointStore::load(root.path(), identity)
            .expect("temporary file should not affect loading");
        assert!(store.report().recovery_reason_codes.is_empty());
        assert!(matches!(
            store.resume_decision(&key, resume_policy(), false),
            QaResumeDecision::Reuse { .. }
        ));
    }

    #[test]
    fn mismatched_completion_token_is_rejected_without_state_change() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let mut store = QaCheckpointStore::load(root.path(), campaign_identity())
            .expect("checkpoint store should load");
        let key = execution_key("token-mismatch");
        let token =
            begin_attempt(&mut store, &key, "qa.token-mismatch", 1, "qa.resume.execution_key_new");
        let (result_artifact, reason_codes) =
            write_result(root.path(), &token, "qa.token-mismatch", "passed", &["qa.passed"]);
        let mut mismatched = completion(&token, "passed", result_artifact, reason_codes);
        mismatched.execution_id = "execution-from-another-worker".to_owned();

        let error = store
            .complete_attempt(&token, mismatched)
            .expect_err("mismatched completion must be rejected");
        assert!(error.to_string().contains("qa.resume.attempt_completion_invalid"));
        assert_eq!(store.report().checkpoint_generation, 1);
        assert_eq!(
            store.current_partial_attempt(&key).expect("attempt should remain partial").state,
            QaCheckpointAttemptState::Partial
        );
    }

    #[test]
    fn published_orphan_result_completes_after_restart() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let identity = campaign_identity();
        let key = execution_key("orphan");
        let completion = {
            let mut store = QaCheckpointStore::load(root.path(), identity.clone())
                .expect("checkpoint store should load");
            let token =
                begin_attempt(&mut store, &key, "qa.orphan", 1, "qa.resume.execution_key_new");
            let (result_artifact, reason_codes) =
                write_result(root.path(), &token, "qa.orphan", "passed", &["qa.passed"]);
            completion(&token, "passed", result_artifact, reason_codes)
        };

        let mut recovered = QaCheckpointStore::load(root.path(), identity)
            .expect("checkpoint store should recover");
        let recovered_token = recovered
            .current_partial_attempt_token(&key)
            .expect("partial token should be reconstructed");
        recovered
            .recover_published_attempt(&recovered_token, completion)
            .expect("published orphan result should complete");
        assert_eq!(recovered.report().checkpoint_generation, 2);
        assert!(matches!(
            recovered.resume_decision(&key, resume_policy(), false),
            QaResumeDecision::Reuse { attempt_generation: 1, .. }
        ));
    }

    #[test]
    #[ignore = "spawned by the cross-process checkpoint crash test"]
    fn checkpoint_coordinator_crash_child_process() {
        if env::var_os(CRASH_HELPER_ENV).is_none() {
            return;
        }
        let artifact_root = PathBuf::from(
            env::var_os(CRASH_HELPER_ROOT_ENV)
                .expect("checkpoint crash helper root should be provided"),
        );
        let mut store = QaCheckpointStore::load(artifact_root.as_path(), campaign_identity())
            .expect("child coordinator should acquire the lock");
        let completed_key = execution_key("cross-process-completed");
        let completed = begin_attempt(
            &mut store,
            &completed_key,
            "qa.cross-process-completed",
            1,
            "qa.resume.execution_key_new",
        );
        let (result_artifact, reason_codes) = write_result(
            artifact_root.as_path(),
            &completed,
            "qa.cross-process-completed",
            "passed",
            &["qa.passed"],
        );
        store
            .complete_attempt(
                &completed,
                completion(&completed, "passed", result_artifact, reason_codes),
            )
            .expect("completed subset scenario should become durable");

        let partial_key = execution_key("cross-process-partial");
        begin_attempt(
            &mut store,
            &partial_key,
            "qa.cross-process-partial",
            1,
            "qa.resume.execution_key_new",
        );

        // Readiness follows both the completed result and the next partial
        // checkpoint, so the parent kills a quiescent coordinator at a real
        // campaign-subset boundary.
        write_artifact_bytes(
            artifact_root.as_path(),
            Path::new(CRASH_HELPER_READY_PATH),
            "test_signal",
            b"ready",
        )
        .expect("checkpoint crash helper should publish readiness");

        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            thread::sleep(Duration::from_millis(25));
        }
        panic!("checkpoint crash helper was not terminated by its parent");
    }

    #[test]
    fn coordinator_crash_reuses_completed_subset_and_reruns_partial_scenario() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let identity = campaign_identity();
        let completed_key = execution_key("cross-process-completed");
        let partial_key = execution_key("cross-process-partial");
        let ready_path = root.path().join(CRASH_HELPER_READY_PATH);
        let mut child = KillOnDropChild::spawn(root.path());
        wait_for_crash_helper_ready(&mut child, ready_path.as_path());

        let lock_error = match QaCheckpointStore::load(root.path(), identity.clone()) {
            Ok(_) => panic!("live child coordinator lock must exclude the parent"),
            Err(error) => error,
        };
        assert!(lock_error.to_string().contains("qa.resume.coordinator_lock_unavailable"));
        let status = child.terminate_and_wait();
        assert!(!status.success(), "crash helper should be force-terminated");

        let mut recovered = QaCheckpointStore::load(root.path(), identity)
            .expect("terminated child should release its coordinator lock");
        let first_report = recovered.report();
        assert_eq!(first_report.checkpoint_generation, 3);
        assert!(first_report.recovery_reason_codes.is_empty());
        let reusable_completed = recovered.resume_decision(&completed_key, resume_policy(), false);
        let completed_result_artifact = match &reusable_completed {
            QaResumeDecision::Reuse {
                reason_code,
                attempt_generation,
                execution_id,
                result_artifact,
            } => {
                assert_eq!(reason_code, "qa.resume.passed_reused");
                assert_eq!(*attempt_generation, 1);
                assert_eq!(execution_id, &format!("execution-{}-1", &completed_key.digest[..12]));
                result_artifact.clone()
            }
            QaResumeDecision::Run { reason_code } => {
                panic!("completed subset scenario must be reusable, got {reason_code}")
            }
        };
        let completed_result_bytes =
            read_verified_artifact(root.path(), &completed_result_artifact)
                .expect("completed subset result should remain verified");
        assert_run_reason(
            recovered.resume_decision(&partial_key, resume_policy(), false),
            "qa.resume.partial_rerun",
        );
        let first_reference =
            first_report.checkpoint_artifact.expect("campaign subset checkpoint should recover");
        let first_bytes = read_verified_artifact(root.path(), &first_reference)
            .expect("recovered checkpoint snapshot should verify");

        let rerun = begin_attempt(
            &mut recovered,
            &partial_key,
            "qa.cross-process-partial",
            2,
            "qa.resume.partial_rerun",
        );
        assert_eq!(rerun.attempt_generation, 2);
        assert_eq!(recovered.report().checkpoint_generation, 4);
        assert_eq!(
            recovered.resume_decision(&completed_key, resume_policy(), false),
            reusable_completed
        );
        assert_eq!(
            read_verified_artifact(root.path(), &completed_result_artifact)
                .expect("completed subset result should remain verified after partial rerun"),
            completed_result_bytes
        );
        let completed_entry = recovered
            .checkpoint
            .entries
            .get(completed_key.digest.as_str())
            .expect("completed subset history should remain present");
        assert_eq!(completed_entry.attempts.len(), 1);
        assert_eq!(completed_entry.attempts[0].state, QaCheckpointAttemptState::Passed);
        let partial_entry = recovered
            .checkpoint
            .entries
            .get(partial_key.digest.as_str())
            .expect("partial scenario history should remain present");
        assert_eq!(partial_entry.attempts.len(), 2);
        assert!(partial_entry
            .attempts
            .iter()
            .all(|attempt| attempt.state == QaCheckpointAttemptState::Partial));
        assert_eq!(
            read_verified_artifact(root.path(), &first_reference)
                .expect("prior immutable checkpoint should remain verifiable"),
            first_bytes
        );
    }

    #[test]
    fn coordinator_lock_excludes_a_second_store_and_releases_on_drop() {
        let root = tempfile::tempdir().expect("temporary artifact root should be available");
        let identity = campaign_identity();
        let first = QaCheckpointStore::load(root.path(), identity.clone())
            .expect("first coordinator should acquire the lock");
        let error = match QaCheckpointStore::load(root.path(), identity.clone()) {
            Ok(_) => panic!("second coordinator must not acquire the lock"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("qa.resume.coordinator_lock_unavailable"));
        drop(first);
        QaCheckpointStore::load(root.path(), identity)
            .expect("coordinator lock should release when the store drops");
    }
}
