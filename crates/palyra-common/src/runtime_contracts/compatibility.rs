//! Shared runtime-state compatibility and quarantine reports.
//!
//! Unknown future schemas remain opaque evidence and block affected admission;
//! corrupt records are quarantined without destructive cleanup.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Schema version for [`RuntimeStateCompatibilityReport`].
pub const RUNTIME_STATE_COMPATIBILITY_SCHEMA_VERSION: u32 = 1;
/// Maximum findings retained in one startup report.
pub const MAX_RUNTIME_COMPATIBILITY_FINDINGS: usize = 256;
/// Maximum distinct reason codes rendered in a redacted startup summary.
pub const MAX_RUNTIME_COMPATIBILITY_SUMMARY_REASONS: usize = 8;

const INVALID_SUMMARY_REASON_CODE: &str = "runtime.compatibility.invalid_reason_code";

runtime_contract_enum! {
    /// Compatibility outcome for one durable runtime record or contract family.
    pub enum RuntimeStateCompatibilityOutcome {
        Migrated => "migrated",
        ReadableLegacy => "readable_legacy",
        BlockedNewerSchema => "blocked_newer_schema",
        QuarantinedCorrupt => "quarantined_corrupt"
    }
}

runtime_contract_enum! {
    /// Admission posture derived from the startup compatibility scan.
    ///
    /// `ReadOnly` preserves downgrade evidence for offline inspection and migration tooling. It
    /// does not authorize a partially writable daemon serving mode.
    pub enum RuntimeStateAdmissionPosture {
        Ready => "ready",
        ReadOnly => "read_only",
        Blocked => "blocked"
    }
}

/// One bounded compatibility finding without raw payload data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeStateCompatibilityFinding {
    /// Contract or table family.
    pub contract: String,
    /// Opaque record hash or id hash.
    pub record_ref_sha256: String,
    /// Observed schema version, absent for unparseable corruption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_schema_version: Option<u32>,
    /// Highest schema version understood by this binary.
    pub supported_schema_version: u32,
    /// Compatibility outcome.
    pub outcome: RuntimeStateCompatibilityOutcome,
    /// Stable reason code.
    pub reason_code: String,
    /// Whether this finding blocks affected runtime admission.
    pub blocks_admission: bool,
    /// Observed payload byte count.
    pub payload_bytes: u64,
}

impl RuntimeStateCompatibilityFinding {
    /// Validates reason, version, and hash fields.
    ///
    /// # Errors
    /// Returns [`RuntimeStateCompatibilityError::InvalidFinding`] for malformed metadata
    /// or a fail-open newer-schema classification.
    pub fn validate(&self) -> Result<(), RuntimeStateCompatibilityError> {
        if self.contract.trim().is_empty()
            || self.reason_code.trim().is_empty()
            || !is_sha256(self.record_ref_sha256.as_str())
            || self.supported_schema_version == 0
        {
            return Err(RuntimeStateCompatibilityError::InvalidFinding);
        }
        if self.outcome == RuntimeStateCompatibilityOutcome::BlockedNewerSchema
            && !self.blocks_admission
        {
            return Err(RuntimeStateCompatibilityError::FutureSchemaMustBlock);
        }
        Ok(())
    }
}

/// Aggregate startup compatibility report generated before runtime admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeStateCompatibilityReport {
    /// Contract schema version.
    pub schema_version: u32,
    /// Aggregate admission posture.
    pub admission: RuntimeStateAdmissionPosture,
    /// Bounded findings sorted by contract and record hash.
    pub findings: Vec<RuntimeStateCompatibilityFinding>,
    /// Whether downgrade must preserve unsupported records read-only.
    pub preserve_unknown_evidence: bool,
    /// Report timestamp.
    pub generated_at_unix_ms: i64,
}

impl RuntimeStateCompatibilityReport {
    /// Builds an aggregate report and derives fail-closed admission posture.
    ///
    /// # Errors
    /// Returns [`RuntimeStateCompatibilityError`] when findings exceed bounds or are invalid.
    pub fn from_findings(
        mut findings: Vec<RuntimeStateCompatibilityFinding>,
        generated_at_unix_ms: i64,
    ) -> Result<Self, RuntimeStateCompatibilityError> {
        if findings.len() > MAX_RUNTIME_COMPATIBILITY_FINDINGS || generated_at_unix_ms < 0 {
            return Err(RuntimeStateCompatibilityError::InvalidReport);
        }
        for finding in &findings {
            finding.validate()?;
        }
        findings.sort_by(|left, right| {
            left.contract
                .cmp(&right.contract)
                .then_with(|| left.record_ref_sha256.cmp(&right.record_ref_sha256))
        });
        let admission = if findings.iter().any(|finding| finding.blocks_admission) {
            RuntimeStateAdmissionPosture::Blocked
        } else if findings.iter().any(|finding| {
            matches!(
                finding.outcome,
                RuntimeStateCompatibilityOutcome::ReadableLegacy
                    | RuntimeStateCompatibilityOutcome::QuarantinedCorrupt
            )
        }) {
            RuntimeStateAdmissionPosture::ReadOnly
        } else {
            RuntimeStateAdmissionPosture::Ready
        };
        Ok(Self {
            schema_version: RUNTIME_STATE_COMPATIBILITY_SCHEMA_VERSION,
            admission,
            findings,
            preserve_unknown_evidence: true,
            generated_at_unix_ms,
        })
    }

    /// Returns whether normal runtime admission may open.
    ///
    /// Read-only compatibility is intentionally excluded because the daemon does not have a
    /// complete mutation gate for a partially serving read-only mode.
    #[must_use]
    pub const fn permits_admission(&self) -> bool {
        matches!(self.admission, RuntimeStateAdmissionPosture::Ready)
    }

    /// Returns whether offline inspection or migration tooling may open without mutating the
    /// incompatible records.
    #[must_use]
    pub const fn permits_offline_inspection(&self) -> bool {
        matches!(
            self.admission,
            RuntimeStateAdmissionPosture::Ready | RuntimeStateAdmissionPosture::ReadOnly
        )
    }

    /// Returns whether a writable opener may proceed only to the migration boundary.
    ///
    /// This is narrower than normal admission: every non-ready finding must be a
    /// nonblocking legacy schema that this binary explicitly knows how to migrate.
    #[must_use]
    pub fn permits_writable_migration(&self) -> bool {
        self.permits_admission()
            || (self.admission == RuntimeStateAdmissionPosture::ReadOnly
                && !self.findings.is_empty()
                && self.findings.iter().all(|finding| {
                    finding.contract == "schema_migrations"
                        && finding.outcome == RuntimeStateCompatibilityOutcome::ReadableLegacy
                        && !finding.blocks_admission
                        && finding
                            .observed_schema_version
                            .is_some_and(|observed| observed < finding.supported_schema_version)
                }))
    }

    /// Renders a bounded startup summary containing only posture and safe reason counts.
    ///
    /// Record hashes, contract identifiers, payload sizes, and raw payloads are
    /// intentionally excluded. Malformed reason codes collapse into one stable
    /// placeholder rather than reaching logs or terminal output.
    #[must_use]
    pub fn redacted_reason_summary(&self) -> String {
        let counts = self.findings.iter().fold(BTreeMap::new(), |mut counts, finding| {
            let reason_code = if is_summary_safe_reason_code(finding.reason_code.as_str()) {
                finding.reason_code.as_str()
            } else {
                INVALID_SUMMARY_REASON_CODE
            };
            let count = counts.entry(reason_code.to_owned()).or_insert(0_u64);
            *count = count.saturating_add(1);
            counts
        });
        let omitted_reason_kinds =
            counts.len().saturating_sub(MAX_RUNTIME_COMPATIBILITY_SUMMARY_REASONS);
        let rendered_reasons = counts
            .iter()
            .take(MAX_RUNTIME_COMPATIBILITY_SUMMARY_REASONS)
            .map(|(reason, count)| format!("{reason}:{count}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "admission={} findings={} reasons=[{}] omitted_reason_kinds={}",
            self.admission.as_str(),
            self.findings.len(),
            rendered_reasons,
            omitted_reason_kinds
        )
    }
}

/// Compatibility report validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeStateCompatibilityError {
    /// Finding metadata is malformed.
    #[error("runtime compatibility finding is invalid")]
    InvalidFinding,
    /// Future schema was classified without a blocker.
    #[error("unknown newer runtime schema must block admission")]
    FutureSchemaMustBlock,
    /// Aggregate report exceeded bounds or had an invalid timestamp.
    #[error("runtime compatibility report is invalid")]
    InvalidReport,
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_summary_safe_reason_code(value: &str) -> bool {
    let bytes = value.as_bytes();
    (3..=128).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'/' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn future_schema_is_fail_closed() {
        let finding = RuntimeStateCompatibilityFinding {
            contract: "runtime_event_envelope".to_owned(),
            record_ref_sha256: "a".repeat(64),
            observed_schema_version: Some(3),
            supported_schema_version: 2,
            outcome: RuntimeStateCompatibilityOutcome::BlockedNewerSchema,
            reason_code: "runtime.compatibility.newer_schema".to_owned(),
            blocks_admission: true,
            payload_bytes: 128,
        };
        let report = RuntimeStateCompatibilityReport::from_findings(vec![finding], 42)
            .expect("report should validate");
        assert_eq!(report.admission, RuntimeStateAdmissionPosture::Blocked);
        assert!(!report.permits_admission());
        assert!(!report.permits_offline_inspection());
    }

    #[test]
    fn corrupt_payload_is_quarantined_without_ready_admission() {
        let finding = RuntimeStateCompatibilityFinding {
            contract: "side_effect_fence".to_owned(),
            record_ref_sha256: "b".repeat(64),
            observed_schema_version: None,
            supported_schema_version: 1,
            outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
            reason_code: "runtime.compatibility.corrupt_quarantined".to_owned(),
            blocks_admission: false,
            payload_bytes: 64,
        };
        let report = RuntimeStateCompatibilityReport::from_findings(vec![finding], 42)
            .expect("report should validate");
        assert_eq!(report.admission, RuntimeStateAdmissionPosture::ReadOnly);
        assert!(!report.permits_admission());
        assert!(report.permits_offline_inspection());
        assert!(!report.permits_writable_migration());
    }

    #[test]
    fn readable_legacy_is_limited_to_the_migration_boundary() {
        let finding = RuntimeStateCompatibilityFinding {
            contract: "schema_migrations".to_owned(),
            record_ref_sha256: "c".repeat(64),
            observed_schema_version: Some(44),
            supported_schema_version: 70,
            outcome: RuntimeStateCompatibilityOutcome::ReadableLegacy,
            reason_code: "runtime.compatibility.legacy_journal_schema".to_owned(),
            blocks_admission: false,
            payload_bytes: 1024,
        };
        let report = RuntimeStateCompatibilityReport::from_findings(vec![finding.clone()], 42)
            .expect("report should validate");

        assert_eq!(report.admission, RuntimeStateAdmissionPosture::ReadOnly);
        assert!(!report.permits_admission());
        assert!(report.permits_offline_inspection());
        assert!(report.permits_writable_migration());

        for unsafe_finding in [
            RuntimeStateCompatibilityFinding {
                contract: "runtime_events_v2".to_owned(),
                ..finding.clone()
            },
            RuntimeStateCompatibilityFinding {
                observed_schema_version: Some(70),
                ..finding.clone()
            },
            RuntimeStateCompatibilityFinding { blocks_admission: true, ..finding },
        ] {
            let unsafe_report =
                RuntimeStateCompatibilityReport::from_findings(vec![unsafe_finding], 42)
                    .expect("legacy report should validate");
            assert!(
                !unsafe_report.permits_writable_migration(),
                "only an older nonblocking schema_migrations finding may reach writable migration"
            );
        }
    }

    #[test]
    fn startup_reason_summary_is_bounded_and_redacted() {
        let mut findings = Vec::new();
        for index in 0..12 {
            findings.push(RuntimeStateCompatibilityFinding {
                contract: format!("contract_{index}"),
                record_ref_sha256: format!("{index:064x}"),
                observed_schema_version: Some(1),
                supported_schema_version: 2,
                outcome: RuntimeStateCompatibilityOutcome::ReadableLegacy,
                reason_code: format!("runtime.compatibility.reason_{index}"),
                blocks_admission: false,
                payload_bytes: 42,
            });
        }
        findings.push(RuntimeStateCompatibilityFinding {
            contract: "malformed".to_owned(),
            record_ref_sha256: "d".repeat(64),
            observed_schema_version: Some(1),
            supported_schema_version: 2,
            outcome: RuntimeStateCompatibilityOutcome::ReadableLegacy,
            reason_code: "unsafe\nterminal text".to_owned(),
            blocks_admission: false,
            payload_bytes: 9_999,
        });
        let report = RuntimeStateCompatibilityReport::from_findings(findings, 42)
            .expect("report should validate");

        let summary = report.redacted_reason_summary();
        assert!(summary.starts_with("admission=read_only findings=13 reasons=["));
        assert!(summary.contains("omitted_reason_kinds=5"));
        assert!(!summary.contains("unsafe"));
        assert!(!summary.contains("9999"));
        assert!(!summary.contains("contract_"));
    }
}
