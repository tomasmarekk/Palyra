//! Shared health-endpoint response shape for daemon and browserd services.
//!
//! Combines build metadata with uptime so every service reports identical health JSON.

use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::build::build_metadata;

/// JSON body returned by service health endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub service: String,
    pub status: String,
    pub version: String,
    pub git_hash: String,
    pub build_profile: String,
    pub uptime_seconds: u64,
}

/// Builds an "ok" health response for `service` using its process start instant.
#[must_use]
pub fn health_response(service: &'static str, started_at: Instant) -> HealthResponse {
    let metadata = build_metadata();
    HealthResponse {
        service: service.to_owned(),
        status: "ok".to_owned(),
        version: metadata.version.to_owned(),
        git_hash: metadata.git_hash.to_owned(),
        build_profile: metadata.build_profile.to_owned(),
        uptime_seconds: started_at.elapsed().as_secs(),
    }
}

/// Stable severity vocabulary for state doctor findings.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum StateHealthSeverity {
    /// No action is required.
    #[default]
    Ok,
    /// The subsystem is usable but should be inspected.
    Warning,
    /// The subsystem is operating in a degraded posture.
    Degraded,
    /// The subsystem must not continue normal writes until an operator acts.
    Critical,
}

impl StateHealthSeverity {
    /// Returns the stable wire label for this severity.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Degraded => "degraded",
            Self::Critical => "critical",
        }
    }

    /// Combines two severities, preserving the worse one.
    #[must_use]
    pub fn max(self, other: Self) -> Self {
        if self >= other {
            self
        } else {
            other
        }
    }
}

/// Safe, support-bundle-ready evidence pointer for a state health finding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateHealthEvidenceRef {
    pub kind: String,
    pub code: String,
    pub summary: String,
}

impl StateHealthEvidenceRef {
    /// Builds an evidence reference from stable machine-readable fields.
    #[must_use]
    pub fn new(
        kind: impl Into<String>,
        code: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self { kind: kind.into(), code: code.into(), summary: summary.into() }
    }
}

/// Machine-readable state doctor finding shared by CLI and daemon reports.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateHealthFinding {
    pub severity: StateHealthSeverity,
    pub subsystem: String,
    pub code: String,
    pub summary: String,
    pub fix_hint: String,
    pub evidence: Vec<StateHealthEvidenceRef>,
}

impl StateHealthFinding {
    /// Builds a state health finding with stable subsystem and reason-code labels.
    #[must_use]
    pub fn new(
        severity: StateHealthSeverity,
        subsystem: impl Into<String>,
        code: impl Into<String>,
        summary: impl Into<String>,
        fix_hint: impl Into<String>,
        evidence: Vec<StateHealthEvidenceRef>,
    ) -> Self {
        Self {
            severity,
            subsystem: subsystem.into(),
            code: code.into(),
            summary: summary.into(),
            fix_hint: fix_hint.into(),
            evidence,
        }
    }
}

/// Returns the highest severity in a finding list.
#[must_use]
pub fn highest_state_health_severity(findings: &[StateHealthFinding]) -> StateHealthSeverity {
    findings
        .iter()
        .map(|finding| finding.severity)
        .fold(StateHealthSeverity::Ok, StateHealthSeverity::max)
}

#[cfg(test)]
mod state_health_tests {
    use super::*;

    #[test]
    fn highest_state_health_severity_prefers_critical_findings() {
        let findings = vec![
            StateHealthFinding::new(
                StateHealthSeverity::Warning,
                "journal",
                "journal.wal.not_wal",
                "journal is not running in WAL mode",
                "move the state path to a local filesystem and rerun state doctor",
                Vec::new(),
            ),
            StateHealthFinding::new(
                StateHealthSeverity::Critical,
                "journal",
                "journal.hash_chain.mismatch",
                "journal hash chain verification failed",
                "stop writes and restore from backup or inspect offline",
                Vec::new(),
            ),
        ];

        assert_eq!(
            highest_state_health_severity(findings.as_slice()),
            StateHealthSeverity::Critical
        );
    }
}
