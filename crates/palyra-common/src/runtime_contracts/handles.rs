//! Opaque runtime handles, process leases, provenance, and cleanup evidence.
//!
//! Durable records describe ownership and verification metadata only. Live OS
//! handles, raw environment values, and serialized pointers never cross this boundary.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{RuntimeGeneration, RuntimeInstanceId, RuntimeLeaseId, RuntimeRunId, RuntimeSessionId};

/// Schema version for runtime handle and cleanup contracts.
pub const RUNTIME_HANDLE_SCHEMA_VERSION: u32 = 1;

runtime_contract_enum! {
    /// Long-lived runtime families sharing the handle lifecycle contract.
    pub enum RuntimeHandleKind {
        Process => "process",
        Harness => "harness",
        Acp => "acp",
        Mcp => "mcp",
        Lsp => "lsp",
        Pty => "pty",
        Worker => "worker",
        Plugin => "plugin"
    }
}

runtime_contract_enum! {
    /// Persisted handle lifecycle state.
    pub enum RuntimeHandleState {
        Starting => "starting",
        Running => "running",
        Draining => "draining",
        Cleaning => "cleaning",
        Closed => "closed",
        Orphaned => "orphaned",
        Quarantined => "quarantined"
    }
}

runtime_contract_enum! {
    /// Platform ownership primitive used to verify process-tree provenance.
    pub enum ProcessOwnershipKind {
        UnixProcessGroup => "unix_process_group",
        WindowsJobObject => "windows_job_object",
        RemoteExecutionInstance => "remote_execution_instance"
    }
}

runtime_contract_enum! {
    /// Required cleanup steps in execution order.
    pub enum CleanupStepKind {
        GracefulStop => "graceful_stop",
        CloseIo => "close_io",
        KillTree => "kill_tree",
        RemoveTemp => "remove_temp",
        Unmount => "unmount",
        ReleaseLease => "release_lease",
        VerifyAbsence => "verify_absence"
    }
}

runtime_contract_enum! {
    /// Result of one cleanup step.
    pub enum CleanupStepDisposition {
        Completed => "completed",
        SkippedNotRequired => "skipped_not_required",
        Failed => "failed",
        Unknown => "unknown"
    }
}

runtime_contract_enum! {
    /// Aggregate cleanup result.
    pub enum CleanupOutcome {
        Completed => "completed",
        Partial => "partial",
        Unknown => "unknown"
    }
}

runtime_contract_enum! {
    /// Result of verifying process provenance before adoption or termination.
    pub enum ProcessProvenanceDisposition {
        Match => "match",
        Missing => "missing",
        Mismatch => "mismatch",
        Unsupported => "unsupported"
    }
}

/// Opaque descriptor for a live or recoverable runtime instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeHandleDescriptorV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Opaque instance identity.
    pub instance_id: RuntimeInstanceId,
    /// Runtime family.
    pub kind: RuntimeHandleKind,
    /// Owning session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<RuntimeSessionId>,
    /// Owning run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RuntimeRunId>,
    /// Active generation.
    pub generation: RuntimeGeneration,
    /// Host owner label.
    pub owner: String,
    /// Current lifecycle state.
    pub state: RuntimeHandleState,
    /// Bounded redaction-safe resume metadata JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_metadata_json: Option<String>,
    /// Creation timestamp.
    pub created_at_unix_ms: i64,
    /// Last update timestamp.
    pub updated_at_unix_ms: i64,
}

impl RuntimeHandleDescriptorV1 {
    /// Validates the persisted handle descriptor and bounded resume metadata.
    ///
    /// # Errors
    /// Returns [`RuntimeHandleError::InvalidDescriptor`] when ownership, timing,
    /// schema version, or resume metadata violates the durable contract.
    pub fn validate(&self) -> Result<(), RuntimeHandleError> {
        if self.schema_version != RUNTIME_HANDLE_SCHEMA_VERSION
            || self.owner.trim().is_empty()
            || self.owner.len() > 128
            || self.created_at_unix_ms < 0
            || self.updated_at_unix_ms < self.created_at_unix_ms
            || self.resume_metadata_json.as_deref().is_some_and(|metadata| {
                metadata.len() > 8 * 1024
                    || serde_json::from_str::<serde_json::Value>(metadata).is_err()
            })
        {
            return Err(RuntimeHandleError::InvalidDescriptor);
        }
        Ok(())
    }
}

/// OS or remote process provenance required before adoption or termination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessProvenance {
    /// Platform ownership primitive.
    pub ownership_kind: ProcessOwnershipKind,
    /// Stable platform start token or remote execution instance hash.
    pub start_token: String,
    /// SHA-256 digest of the executable or runtime artifact.
    pub executable_sha256: String,
    /// Host-issued random owner nonce.
    pub owner_nonce: String,
    /// Process-group, Job Object, or remote instance identity hash.
    pub ownership_identity_sha256: String,
}

impl ProcessProvenance {
    /// Validates bounded identity and digest fields.
    ///
    /// # Errors
    /// Returns [`RuntimeHandleError::InvalidProvenance`] for missing or malformed evidence.
    pub fn validate(&self) -> Result<(), RuntimeHandleError> {
        if self.start_token.trim().is_empty()
            || self.owner_nonce.trim().is_empty()
            || self.start_token == "<redacted>"
            || self.owner_nonce == "<redacted>"
            || self.start_token.len() > 128
            || self.owner_nonce.len() > 128
            || !is_sha256(self.executable_sha256.as_str())
            || !is_sha256(self.ownership_identity_sha256.as_str())
        {
            return Err(RuntimeHandleError::InvalidProvenance);
        }
        Ok(())
    }
}

/// Durable lease for one process-backed runtime instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessLeaseV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Lease identity.
    pub lease_id: RuntimeLeaseId,
    /// Runtime instance identity.
    pub instance_id: RuntimeInstanceId,
    /// Active generation.
    pub generation: RuntimeGeneration,
    /// Operating-system process id. It is never sufficient by itself.
    pub pid: u32,
    /// Verified process provenance.
    pub provenance: ProcessProvenance,
    /// Lease issue timestamp.
    pub issued_at_unix_ms: i64,
    /// Lease expiry timestamp.
    pub expires_at_unix_ms: i64,
    /// Most recent provenance verification timestamp.
    pub verified_at_unix_ms: i64,
}

impl ProcessLeaseV1 {
    /// Validates lease timing and mandatory provenance.
    ///
    /// # Errors
    /// Returns [`RuntimeHandleError`] when PID, timing, or provenance is invalid.
    pub fn validate(&self) -> Result<(), RuntimeHandleError> {
        self.provenance.validate()?;
        if self.schema_version != RUNTIME_HANDLE_SCHEMA_VERSION
            || self.pid == 0
            || self.issued_at_unix_ms < 0
            || self.verified_at_unix_ms < self.issued_at_unix_ms
            || self.expires_at_unix_ms <= self.issued_at_unix_ms
        {
            return Err(RuntimeHandleError::InvalidLease);
        }
        Ok(())
    }

    /// Returns whether current provenance authorizes an ownership-sensitive action.
    #[must_use]
    pub const fn authorizes(
        &self,
        disposition: ProcessProvenanceDisposition,
        now_unix_ms: i64,
    ) -> bool {
        matches!(disposition, ProcessProvenanceDisposition::Match)
            && now_unix_ms < self.expires_at_unix_ms
    }
}

/// One ordered cleanup action and its evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupStepRecord {
    /// Zero-based step ordinal.
    pub ordinal: u32,
    /// Cleanup operation.
    pub step: CleanupStepKind,
    /// Operation result.
    pub disposition: CleanupStepDisposition,
    /// Stable reason code.
    pub reason_code: String,
    /// Evidence digest when observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_sha256: Option<String>,
    /// Completion timestamp.
    pub completed_at_unix_ms: i64,
}

/// Structured cleanup evidence for one runtime handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupReportV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Cleanup report identity.
    pub report_id: String,
    /// Runtime instance being cleaned.
    pub instance_id: RuntimeInstanceId,
    /// Process lease when cleanup is process-backed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<RuntimeLeaseId>,
    /// Aggregate result.
    pub outcome: CleanupOutcome,
    /// Ordered steps.
    pub steps: Vec<CleanupStepRecord>,
    /// Stable aggregate reason code.
    pub reason_code: String,
    /// Report timestamp.
    pub completed_at_unix_ms: i64,
}

impl CleanupReportV1 {
    /// Validates step ordering and aggregate outcome consistency.
    ///
    /// # Errors
    /// Returns [`RuntimeHandleError::InvalidCleanupReport`] for malformed or
    /// overstated cleanup evidence.
    pub fn validate(&self) -> Result<(), RuntimeHandleError> {
        if self.schema_version != RUNTIME_HANDLE_SCHEMA_VERSION
            || self.report_id.trim().is_empty()
            || self.reason_code.trim().is_empty()
            || self.completed_at_unix_ms < 0
            || self.steps.len() > 16
        {
            return Err(RuntimeHandleError::InvalidCleanupReport);
        }
        let mut previous_rank = None;
        for (expected, step) in self.steps.iter().enumerate() {
            let rank = cleanup_step_rank(step.step);
            if step.ordinal
                != u32::try_from(expected).map_err(|_| RuntimeHandleError::InvalidCleanupReport)?
                || step.reason_code.trim().is_empty()
                || step.completed_at_unix_ms < 0
                || step.evidence_sha256.as_deref().is_some_and(|value| !is_sha256(value))
                || previous_rank.is_some_and(|previous| rank <= previous)
            {
                return Err(RuntimeHandleError::InvalidCleanupReport);
            }
            previous_rank = Some(rank);
        }
        if self.lease_id.is_some()
            && self.steps.last().is_none_or(|step| step.step != CleanupStepKind::VerifyAbsence)
        {
            return Err(RuntimeHandleError::InvalidCleanupReport);
        }
        let any_failed = self
            .steps
            .iter()
            .any(|step| matches!(step.disposition, CleanupStepDisposition::Failed));
        let any_unknown = self
            .steps
            .iter()
            .any(|step| matches!(step.disposition, CleanupStepDisposition::Unknown));
        let unrecovered_failure = self.steps.iter().enumerate().any(|(index, step)| {
            if step.disposition != CleanupStepDisposition::Failed {
                return false;
            }
            step.step != CleanupStepKind::GracefulStop
                || !self.steps[index.saturating_add(1)..].iter().any(|later| {
                    later.step == CleanupStepKind::KillTree
                        && later.disposition == CleanupStepDisposition::Completed
                })
        });
        match self.outcome {
            CleanupOutcome::Completed if unrecovered_failure || any_unknown => {
                Err(RuntimeHandleError::InvalidCleanupReport)
            }
            CleanupOutcome::Partial if !any_failed => Err(RuntimeHandleError::InvalidCleanupReport),
            CleanupOutcome::Unknown if !any_unknown => {
                Err(RuntimeHandleError::InvalidCleanupReport)
            }
            _ => Ok(()),
        }
    }
}

/// Runtime handle and cleanup validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeHandleError {
    /// Persisted runtime handle descriptor is malformed.
    #[error("runtime handle descriptor is invalid")]
    InvalidDescriptor,
    /// Process provenance cannot establish ownership.
    #[error("process provenance is invalid")]
    InvalidProvenance,
    /// Process lease is malformed.
    #[error("process lease is invalid")]
    InvalidLease,
    /// Cleanup report overstates or misorders evidence.
    #[error("cleanup report is invalid")]
    InvalidCleanupReport,
}

const fn cleanup_step_rank(step: CleanupStepKind) -> u8 {
    match step {
        CleanupStepKind::GracefulStop => 0,
        CleanupStepKind::CloseIo => 1,
        CleanupStepKind::KillTree => 2,
        CleanupStepKind::RemoveTemp => 3,
        CleanupStepKind::Unmount => 4,
        CleanupStepKind::ReleaseLease => 5,
        CleanupStepKind::VerifyAbsence => 6,
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease() -> ProcessLeaseV1 {
        ProcessLeaseV1 {
            schema_version: 1,
            lease_id: RuntimeLeaseId::parse("lease_01").expect("lease id"),
            instance_id: RuntimeInstanceId::parse("instance_01").expect("instance id"),
            generation: RuntimeGeneration::new(1).expect("generation"),
            pid: 42,
            provenance: ProcessProvenance {
                ownership_kind: ProcessOwnershipKind::UnixProcessGroup,
                start_token: "12345".to_owned(),
                executable_sha256: "a".repeat(64),
                owner_nonce: "nonce_01".to_owned(),
                ownership_identity_sha256: "b".repeat(64),
            },
            issued_at_unix_ms: 10,
            expires_at_unix_ms: 100,
            verified_at_unix_ms: 10,
        }
    }

    #[test]
    fn redacted_process_identity_cannot_become_authority() {
        let mut lease = lease();
        lease.provenance.start_token = "<redacted>".to_owned();
        assert_eq!(lease.validate(), Err(RuntimeHandleError::InvalidProvenance));
        lease = self::lease();
        lease.provenance.owner_nonce = "<redacted>".to_owned();
        assert_eq!(lease.validate(), Err(RuntimeHandleError::InvalidProvenance));
    }

    #[test]
    fn pid_without_matching_provenance_never_authorizes_cleanup() {
        let lease = lease();
        assert!(!lease.authorizes(ProcessProvenanceDisposition::Mismatch, 20));
        assert!(!lease.authorizes(ProcessProvenanceDisposition::Missing, 20));
        assert!(lease.authorizes(ProcessProvenanceDisposition::Match, 20));
    }

    #[test]
    fn process_cleanup_requires_canonical_order_and_final_absence_check() {
        let mut report = CleanupReportV1 {
            schema_version: 1,
            report_id: "cleanup_order".to_owned(),
            instance_id: RuntimeInstanceId::parse("instance_01").expect("instance id"),
            lease_id: Some(RuntimeLeaseId::parse("lease_01").expect("lease id")),
            outcome: CleanupOutcome::Completed,
            steps: vec![
                CleanupStepRecord {
                    ordinal: 0,
                    step: CleanupStepKind::VerifyAbsence,
                    disposition: CleanupStepDisposition::Completed,
                    reason_code: "runtime.cleanup.absence_verified".to_owned(),
                    evidence_sha256: Some("a".repeat(64)),
                    completed_at_unix_ms: 20,
                },
                CleanupStepRecord {
                    ordinal: 1,
                    step: CleanupStepKind::KillTree,
                    disposition: CleanupStepDisposition::Completed,
                    reason_code: "runtime.cleanup.kill_tree".to_owned(),
                    evidence_sha256: None,
                    completed_at_unix_ms: 20,
                },
            ],
            reason_code: "runtime.cleanup.completed".to_owned(),
            completed_at_unix_ms: 20,
        };
        assert_eq!(report.validate(), Err(RuntimeHandleError::InvalidCleanupReport));

        report.steps = vec![CleanupStepRecord {
            ordinal: 0,
            step: CleanupStepKind::KillTree,
            disposition: CleanupStepDisposition::Completed,
            reason_code: "runtime.cleanup.kill_tree".to_owned(),
            evidence_sha256: None,
            completed_at_unix_ms: 20,
        }];
        assert_eq!(report.validate(), Err(RuntimeHandleError::InvalidCleanupReport));
    }

    #[test]
    fn completed_report_cannot_hide_unknown_step() {
        let report = CleanupReportV1 {
            schema_version: 1,
            report_id: "cleanup_01".to_owned(),
            instance_id: RuntimeInstanceId::parse("instance_01").expect("instance id"),
            lease_id: Some(RuntimeLeaseId::parse("lease_01").expect("lease id")),
            outcome: CleanupOutcome::Completed,
            steps: vec![CleanupStepRecord {
                ordinal: 0,
                step: CleanupStepKind::VerifyAbsence,
                disposition: CleanupStepDisposition::Unknown,
                reason_code: "runtime.cleanup.unverified".to_owned(),
                evidence_sha256: None,
                completed_at_unix_ms: 20,
            }],
            reason_code: "runtime.cleanup.completed".to_owned(),
            completed_at_unix_ms: 20,
        };
        assert_eq!(report.validate(), Err(RuntimeHandleError::InvalidCleanupReport));
    }

    #[test]
    fn verified_hard_kill_can_recover_a_failed_graceful_stop() {
        let report = CleanupReportV1 {
            schema_version: 1,
            report_id: "cleanup_graceful_fallback".to_owned(),
            instance_id: RuntimeInstanceId::parse("instance_01").expect("instance id"),
            lease_id: Some(RuntimeLeaseId::parse("lease_01").expect("lease id")),
            outcome: CleanupOutcome::Completed,
            steps: vec![
                CleanupStepRecord {
                    ordinal: 0,
                    step: CleanupStepKind::GracefulStop,
                    disposition: CleanupStepDisposition::Failed,
                    reason_code: "runtime.cleanup.graceful_stop_timed_out".to_owned(),
                    evidence_sha256: None,
                    completed_at_unix_ms: 20,
                },
                CleanupStepRecord {
                    ordinal: 1,
                    step: CleanupStepKind::KillTree,
                    disposition: CleanupStepDisposition::Completed,
                    reason_code: "runtime.cleanup.kill_tree_completed".to_owned(),
                    evidence_sha256: None,
                    completed_at_unix_ms: 21,
                },
                CleanupStepRecord {
                    ordinal: 2,
                    step: CleanupStepKind::VerifyAbsence,
                    disposition: CleanupStepDisposition::Completed,
                    reason_code: "runtime.cleanup.absence_verified".to_owned(),
                    evidence_sha256: Some("a".repeat(64)),
                    completed_at_unix_ms: 22,
                },
            ],
            reason_code: "runtime.cleanup.completed_after_hard_kill".to_owned(),
            completed_at_unix_ms: 22,
        };

        assert_eq!(report.validate(), Ok(()));
    }
}
