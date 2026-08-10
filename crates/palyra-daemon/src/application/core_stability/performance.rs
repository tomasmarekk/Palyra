//! Versioned performance, capacity, and soak qualification for the core runtime.
//!
//! The evaluator rejects incomplete stage coverage, hidden tail regressions,
//! resource leaks, and unbounded platform waivers before release promotion.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

const CORE_PERFORMANCE_SCHEMA_VERSION: u32 = 1;
const CORE_RUNTIME_CONTRACT_VERSION: &str = "runtime-contracts.v16";
const MIN_LONG_LIVED_ACTOR_CAPACITY: u32 = 100;
const MAX_METADATA_TRACE_OVERHEAD_BPS: u32 = 500;

const BUILTIN_BASELINE_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../infra/release/core-performance-baseline.json"
));

/// Representative workload classes required by the core release gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CoreLoadProfile {
    Interactive,
    LongRunningCoding,
    Fanout,
    McpHeavy,
    RestartRecovery,
}

/// Runtime stages whose tail latency must remain explicitly bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CorePerformanceStage {
    Admission,
    Context,
    FirstToken,
    ToolGate,
    Journal,
    Delivery,
    Plugin,
    Pty,
    Lsp,
    WorkGraph,
    Mcp,
    Cleanup,
}

impl CorePerformanceStage {
    const ALL: [Self; 12] = [
        Self::Admission,
        Self::Context,
        Self::FirstToken,
        Self::ToolGate,
        Self::Journal,
        Self::Delivery,
        Self::Plugin,
        Self::Pty,
        Self::Lsp,
        Self::WorkGraph,
        Self::Mcp,
        Self::Cleanup,
    ];
}

/// Latency observations and release limits for one measured runtime stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StageLatencyBaseline {
    pub(crate) stage: CorePerformanceStage,
    pub(crate) p50_ms: u64,
    pub(crate) p95_ms: u64,
    pub(crate) p99_ms: u64,
    pub(crate) release_limit_p99_ms: u64,
    pub(crate) sample_count: u64,
    pub(crate) evidence_ref: String,
}

/// A bounded, owned exception for a measured platform difference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlatformGuardrailWaiver {
    pub(crate) platform: String,
    pub(crate) stage: CorePerformanceStage,
    pub(crate) replacement_limit_p99_ms: u64,
    pub(crate) reason: String,
    pub(crate) owner: String,
    pub(crate) expires_on: String,
}

/// Capacity and resource limits evaluated after the deterministic soak.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapacityGuardrail {
    pub(crate) long_lived_actor_capacity: u32,
    pub(crate) max_event_queue_depth: u32,
    pub(crate) max_process_count: u32,
    pub(crate) max_rss_growth_mib: u64,
    pub(crate) max_fd_growth: u32,
    pub(crate) max_spool_bytes: u64,
    pub(crate) max_provider_attempts_per_turn: u32,
    pub(crate) max_cleanup_duration_ms: u64,
    pub(crate) observed_orphan_resources: u32,
    pub(crate) observed_restart_cycles: u32,
}

/// A bounded incident discovered during a capacity or soak qualification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SoakIncidentRecord {
    pub(crate) incident_id: String,
    pub(crate) severity: String,
    pub(crate) root_cause: String,
    pub(crate) evidence_ref: String,
    pub(crate) orphan_resources: u32,
    pub(crate) release_blocker: bool,
}

/// Canonical versioned evidence used by the core performance release gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CorePerformanceBaseline {
    pub(crate) schema_version: u32,
    pub(crate) runtime_contract_version: String,
    pub(crate) generated_from: String,
    pub(crate) as_of: String,
    pub(crate) load_profiles: BTreeSet<CoreLoadProfile>,
    pub(crate) stage_latencies: Vec<StageLatencyBaseline>,
    pub(crate) capacity: CapacityGuardrail,
    pub(crate) metadata_trace_overhead_bps: u32,
    pub(crate) platform_waivers: Vec<PlatformGuardrailWaiver>,
    pub(crate) soak_incidents: Vec<SoakIncidentRecord>,
    pub(crate) uses_production_secrets: bool,
}

/// Stable failure raised while parsing the canonical performance evidence.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CorePerformanceEvidenceError {
    /// The repository-owned JSON does not match the versioned Rust contract.
    #[error("core performance baseline is invalid")]
    InvalidBaseline(#[source] serde_json::Error),
}

/// Severity assigned to one performance qualification issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QualificationIssueSeverity {
    Blocker,
}

/// One stable, redacted reason why performance qualification did not pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PerformanceQualificationIssue {
    pub(crate) code: &'static str,
    pub(crate) severity: QualificationIssueSeverity,
    pub(crate) subject: String,
}

/// Release decision derived from the complete performance baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CorePerformanceQualification {
    pub(crate) schema_version: u32,
    pub(crate) qualified: bool,
    pub(crate) reason_code: &'static str,
    pub(crate) issues: Vec<PerformanceQualificationIssue>,
}

/// Parses the repository-owned core performance baseline.
///
/// # Errors
/// Returns [`CorePerformanceEvidenceError`] when the embedded evidence does
/// not match the closed schema.
pub(crate) fn builtin_core_performance_baseline(
) -> Result<CorePerformanceBaseline, CorePerformanceEvidenceError> {
    serde_json::from_str(BUILTIN_BASELINE_JSON)
        .map_err(CorePerformanceEvidenceError::InvalidBaseline)
}

/// Builds the redacted release-qualification view exposed by diagnostics.
///
/// A malformed repository-owned baseline fails closed without exposing parser
/// details or the embedded evidence payload.
#[must_use]
pub(crate) fn build_core_performance_qualification_snapshot() -> serde_json::Value {
    let Ok(baseline) = builtin_core_performance_baseline() else {
        return serde_json::json!({
            "schema_version": CORE_PERFORMANCE_SCHEMA_VERSION,
            "qualified": false,
            "reason_code": "core_performance.evidence_invalid",
            "issues": [{
                "code": "core_performance.evidence_invalid",
                "severity": "blocker",
                "subject": "canonical_baseline",
            }],
        });
    };
    let qualification = evaluate_core_performance(&baseline);

    serde_json::json!({
        "schema_version": qualification.schema_version,
        "qualified": qualification.qualified,
        "reason_code": qualification.reason_code,
        "issues": qualification.issues,
        "runtime_contract_version": baseline.runtime_contract_version,
        "as_of": baseline.as_of,
        "load_profile_count": baseline.load_profiles.len(),
        "measured_stage_count": baseline.stage_latencies.len(),
        "long_lived_actor_capacity": baseline.capacity.long_lived_actor_capacity,
        "observed_restart_cycles": baseline.capacity.observed_restart_cycles,
        "observed_orphan_resources": baseline.capacity.observed_orphan_resources,
        "metadata_trace_overhead_bps": baseline.metadata_trace_overhead_bps,
    })
}

/// Evaluates release-blocking performance, capacity, and soak invariants.
#[must_use]
pub(crate) fn evaluate_core_performance(
    baseline: &CorePerformanceBaseline,
) -> CorePerformanceQualification {
    evaluate_core_performance_for_platform(baseline, std::env::consts::OS)
}

fn evaluate_core_performance_for_platform(
    baseline: &CorePerformanceBaseline,
    platform: &str,
) -> CorePerformanceQualification {
    let mut issues = Vec::new();

    if baseline.schema_version != CORE_PERFORMANCE_SCHEMA_VERSION {
        blocker(
            &mut issues,
            "core_performance.schema_version_unsupported",
            baseline.schema_version.to_string(),
        );
    }
    if baseline.runtime_contract_version != CORE_RUNTIME_CONTRACT_VERSION {
        blocker(
            &mut issues,
            "core_performance.runtime_contract_mismatch",
            baseline.runtime_contract_version.clone(),
        );
    }
    if baseline.uses_production_secrets {
        blocker(
            &mut issues,
            "core_performance.production_secret_fixture",
            "canonical_baseline".to_owned(),
        );
    }

    for required in [
        CoreLoadProfile::Interactive,
        CoreLoadProfile::LongRunningCoding,
        CoreLoadProfile::Fanout,
        CoreLoadProfile::McpHeavy,
        CoreLoadProfile::RestartRecovery,
    ] {
        if !baseline.load_profiles.contains(&required) {
            blocker(&mut issues, "core_performance.load_profile_missing", format!("{required:?}"));
        }
    }

    let mut observed_stages = BTreeSet::new();
    for latency in &baseline.stage_latencies {
        if !observed_stages.insert(latency.stage) {
            blocker(
                &mut issues,
                "core_performance.stage_duplicate",
                format!("{:?}", latency.stage),
            );
        }
        if latency.sample_count == 0
            || latency.p50_ms > latency.p95_ms
            || latency.p95_ms > latency.p99_ms
        {
            blocker(
                &mut issues,
                "core_performance.percentiles_invalid",
                format!("{:?}", latency.stage),
            );
        }
        if latency.p99_ms > effective_p99_limit(baseline, latency, platform) {
            blocker(
                &mut issues,
                "core_performance.tail_latency_regressed",
                format!("{:?}", latency.stage),
            );
        }
        if latency.evidence_ref.trim().is_empty() {
            blocker(
                &mut issues,
                "core_performance.evidence_missing",
                format!("{:?}", latency.stage),
            );
        }
    }
    for required in CorePerformanceStage::ALL {
        if !observed_stages.contains(&required) {
            blocker(&mut issues, "core_performance.stage_missing", format!("{required:?}"));
        }
    }

    if baseline.capacity.long_lived_actor_capacity < MIN_LONG_LIVED_ACTOR_CAPACITY {
        blocker(
            &mut issues,
            "core_performance.actor_capacity_insufficient",
            baseline.capacity.long_lived_actor_capacity.to_string(),
        );
    }
    if baseline.capacity.observed_restart_cycles == 0 {
        blocker(
            &mut issues,
            "core_performance.restart_soak_missing",
            "restart_recovery".to_owned(),
        );
    }
    if baseline.capacity.observed_orphan_resources != 0 {
        blocker(
            &mut issues,
            "core_performance.orphan_resource_detected",
            baseline.capacity.observed_orphan_resources.to_string(),
        );
    }
    if baseline.metadata_trace_overhead_bps > MAX_METADATA_TRACE_OVERHEAD_BPS {
        blocker(
            &mut issues,
            "core_performance.metadata_trace_overhead_exceeded",
            baseline.metadata_trace_overhead_bps.to_string(),
        );
    }

    for waiver in &baseline.platform_waivers {
        if waiver.platform.trim().is_empty()
            || waiver.reason.trim().is_empty()
            || waiver.owner.trim().is_empty()
            || waiver.expires_on.as_str() < baseline.as_of.as_str()
        {
            blocker(
                &mut issues,
                "core_performance.platform_waiver_invalid",
                format!("{:?}", waiver.stage),
            );
        }
    }
    for incident in &baseline.soak_incidents {
        if incident.root_cause.trim().is_empty() || incident.evidence_ref.trim().is_empty() {
            blocker(
                &mut issues,
                "core_performance.soak_incident_incomplete",
                incident.incident_id.clone(),
            );
        }
        if incident.release_blocker || incident.orphan_resources != 0 {
            blocker(
                &mut issues,
                "core_performance.soak_incident_blocks_release",
                incident.incident_id.clone(),
            );
        }
    }

    let qualified =
        !issues.iter().any(|issue| issue.severity == QualificationIssueSeverity::Blocker);
    CorePerformanceQualification {
        schema_version: CORE_PERFORMANCE_SCHEMA_VERSION,
        qualified,
        reason_code: if qualified {
            "core_performance.qualified"
        } else {
            "core_performance.release_blocked"
        },
        issues,
    }
}

fn effective_p99_limit(
    baseline: &CorePerformanceBaseline,
    latency: &StageLatencyBaseline,
    platform: &str,
) -> u64 {
    baseline
        .platform_waivers
        .iter()
        .filter(|waiver| {
            waiver.stage == latency.stage
                && waiver.expires_on >= baseline.as_of
                && waiver.platform.trim().eq_ignore_ascii_case(platform)
        })
        .map(|waiver| waiver.replacement_limit_p99_ms)
        .max()
        .unwrap_or(latency.release_limit_p99_ms)
}

fn blocker(issues: &mut Vec<PerformanceQualificationIssue>, code: &'static str, subject: String) {
    issues.push(PerformanceQualificationIssue {
        code,
        severity: QualificationIssueSeverity::Blocker,
        subject,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_performance_baseline_is_release_qualified() {
        let baseline =
            builtin_core_performance_baseline().expect("built-in performance evidence must parse");
        let qualification = evaluate_core_performance(&baseline);

        assert!(qualification.qualified, "unexpected issues: {:?}", qualification.issues);
        assert_eq!(qualification.reason_code, "core_performance.qualified");
        assert!(baseline.capacity.long_lived_actor_capacity >= 100);
        assert_eq!(baseline.capacity.observed_orphan_resources, 0);
    }

    #[test]
    fn diagnostics_snapshot_reports_the_release_decision() {
        let snapshot = build_core_performance_qualification_snapshot();

        assert_eq!(snapshot["qualified"], true);
        assert_eq!(snapshot["reason_code"], "core_performance.qualified");
        assert_eq!(snapshot["runtime_contract_version"], "runtime-contracts.v16");
        assert_eq!(snapshot["load_profile_count"], 5);
        assert_eq!(snapshot["measured_stage_count"], 12);
    }

    #[test]
    fn tail_regression_cannot_hide_behind_lower_percentiles() {
        let mut baseline =
            builtin_core_performance_baseline().expect("built-in performance evidence must parse");
        let admission =
            baseline.stage_latencies.first_mut().expect("baseline must include admission");
        admission.p99_ms = admission.release_limit_p99_ms.saturating_add(1);

        let qualification = evaluate_core_performance(&baseline);

        assert!(!qualification.qualified);
        assert!(qualification
            .issues
            .iter()
            .any(|issue| issue.code == "core_performance.tail_latency_regressed"));
    }

    #[test]
    fn orphan_trend_and_expired_waiver_block_release() {
        let mut baseline =
            builtin_core_performance_baseline().expect("built-in performance evidence must parse");
        baseline.capacity.observed_orphan_resources = 1;
        baseline.platform_waivers.push(PlatformGuardrailWaiver {
            platform: "windows".to_owned(),
            stage: CorePerformanceStage::Pty,
            replacement_limit_p99_ms: 9_000,
            reason: "bounded platform variance".to_owned(),
            owner: "runtime".to_owned(),
            expires_on: "2026-01-01".to_owned(),
        });

        let qualification = evaluate_core_performance(&baseline);

        assert!(!qualification.qualified);
        assert!(qualification
            .issues
            .iter()
            .any(|issue| issue.code == "core_performance.orphan_resource_detected"));
        assert!(qualification
            .issues
            .iter()
            .any(|issue| issue.code == "core_performance.platform_waiver_invalid"));
    }

    #[test]
    fn platform_waiver_applies_only_to_its_target_platform() {
        let mut baseline =
            builtin_core_performance_baseline().expect("built-in performance evidence must parse");
        let (stage, waived_p99_ms) = {
            let admission =
                baseline.stage_latencies.first_mut().expect("baseline must include admission");
            admission.p99_ms = admission.release_limit_p99_ms.saturating_add(1);
            (admission.stage, admission.p99_ms)
        };
        baseline.platform_waivers.push(PlatformGuardrailWaiver {
            platform: "windows".to_owned(),
            stage,
            replacement_limit_p99_ms: waived_p99_ms,
            reason: "bounded Windows variance".to_owned(),
            owner: "runtime".to_owned(),
            expires_on: "2099-12-31".to_owned(),
        });

        let linux = evaluate_core_performance_for_platform(&baseline, "linux");
        let windows = evaluate_core_performance_for_platform(&baseline, "windows");

        assert!(linux
            .issues
            .iter()
            .any(|issue| issue.code == "core_performance.tail_latency_regressed"));
        assert!(windows.qualified, "unexpected issues: {:?}", windows.issues);
    }
}
