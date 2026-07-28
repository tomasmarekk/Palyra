//! Closed execution modes and admission settings for autonomous routines.

use std::collections::BTreeSet;

use chrono::{TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    normalize_freeform_identifier, normalize_identifier, RoutineExecutionPosture,
    RoutineRegistryError, RoutineRunMode,
};

/// Selects whether a routine invokes an agent before or after a closed host probe.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutineExecutionMode {
    #[default]
    Agent,
    NoAgent,
    ProbeThenAgent,
}

impl RoutineExecutionMode {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::NoAgent => "no_agent",
            Self::ProbeThenAgent => "probe_then_agent",
        }
    }
}

/// Closed probe kinds that never execute an operator-supplied command.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineProbeKind {
    DaemonHealth,
}

/// Bounded declaration for the host-owned preflight probe.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutinePreflightProbe {
    pub kind: RoutineProbeKind,
    pub timeout_ms: u64,
    pub output_max_bytes: usize,
}

/// Closed predicate AST evaluated over a redacted probe observation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operator", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoutineWakePredicate {
    ProbeHealthy,
    JsonPointerEquals { pointer: String, expected: Value },
}

/// Artifact reference that may be projected into a probe-then-agent context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineContextSourceArtifact {
    pub artifact_id: String,
    pub sha256: String,
    pub sensitivity: String,
    pub max_bytes: usize,
}

/// Per-routine tool allowlist. It can only narrow the daemon's global catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineToolProfile {
    pub profile_id: String,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
}

/// Optional timezone-aware window during which autonomous dispatch is allowed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineActiveHours {
    pub start_minute_of_day: u16,
    pub end_minute_of_day: u16,
    pub timezone: String,
}

/// Predicate decision emitted by the governed probe path.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WakePredicateDecision {
    Matched,
    NotMatched,
    Failed,
}

/// Bounded, redacted predicate result persisted with autonomous wake provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WakePredicateOutcome {
    pub schema_version: u64,
    pub decision: WakePredicateDecision,
    pub reason_code: String,
    pub output_sha256: String,
    pub summary: String,
    pub duration_ms: u64,
}

/// Host observation consumed by [`evaluate_routine_wake_predicate`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineProbeObservation {
    pub healthy: bool,
    pub output: Value,
    pub output_sha256: String,
    pub summary: String,
    pub duration_ms: u64,
}

/// Additive governance fields accepted by console and model-visible routine APIs.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineExecutionGovernanceOverrides {
    #[serde(default)]
    pub authoritative: Option<bool>,
    #[serde(default)]
    pub execution_mode: Option<RoutineExecutionMode>,
    #[serde(default)]
    pub preflight_probe: Option<RoutinePreflightProbe>,
    #[serde(default)]
    pub wake_predicate: Option<RoutineWakePredicate>,
    #[serde(default)]
    pub context_sources: Option<Vec<RoutineContextSourceArtifact>>,
    #[serde(default)]
    pub tool_profile: Option<RoutineToolProfile>,
    #[serde(default)]
    pub active_hours: Option<RoutineActiveHours>,
    #[serde(default)]
    pub flood_window_ms: Option<u64>,
    #[serde(default)]
    pub flood_max_wakes: Option<u32>,
}

/// Execution settings for session reuse, pinned profiles, and autonomous wake governance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineExecutionConfig {
    #[serde(default)]
    pub run_mode: RoutineRunMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub procedure_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_profile_id: Option<String>,
    #[serde(default)]
    pub execution_posture: RoutineExecutionPosture,
    #[serde(default)]
    pub execution_mode: RoutineExecutionMode,
    #[serde(default)]
    pub wake_governance_authoritative: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_probe: Option<RoutinePreflightProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake_predicate: Option<RoutineWakePredicate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_sources: Vec<RoutineContextSourceArtifact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_profile: Option<RoutineToolProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_hours: Option<RoutineActiveHours>,
    #[serde(default = "default_routine_flood_window_ms")]
    pub flood_window_ms: u64,
    #[serde(default = "default_routine_flood_max_wakes")]
    pub flood_max_wakes: u32,
}

impl Default for RoutineExecutionConfig {
    fn default() -> Self {
        Self {
            run_mode: RoutineRunMode::SameSession,
            procedure_profile_id: None,
            skill_profile_id: None,
            provider_profile_id: None,
            execution_posture: RoutineExecutionPosture::Standard,
            execution_mode: RoutineExecutionMode::Agent,
            wake_governance_authoritative: false,
            preflight_probe: None,
            wake_predicate: None,
            context_sources: Vec::new(),
            tool_profile: None,
            active_hours: None,
            flood_window_ms: default_routine_flood_window_ms(),
            flood_max_wakes: default_routine_flood_max_wakes(),
        }
    }
}

const fn default_routine_flood_window_ms() -> u64 {
    60 * 60 * 1_000
}

const fn default_routine_flood_max_wakes() -> u32 {
    8
}

/// Evaluates a closed predicate over a bounded, already-redacted observation.
#[must_use]
pub fn evaluate_routine_wake_predicate(
    predicate: &RoutineWakePredicate,
    observation: &RoutineProbeObservation,
) -> WakePredicateOutcome {
    let matched = match predicate {
        RoutineWakePredicate::ProbeHealthy => observation.healthy,
        RoutineWakePredicate::JsonPointerEquals { pointer, expected } => {
            observation.output.pointer(pointer).is_some_and(|actual| actual == expected)
        }
    };
    WakePredicateOutcome {
        schema_version: 1,
        decision: if matched {
            WakePredicateDecision::Matched
        } else {
            WakePredicateDecision::NotMatched
        },
        reason_code: if matched {
            "wake.predicate_matched".to_owned()
        } else {
            "wake.predicate_not_matched".to_owned()
        },
        output_sha256: observation.output_sha256.clone(),
        summary: observation.summary.clone(),
        duration_ms: observation.duration_ms,
    }
}

/// Applies additive governance overrides and validates the resulting closed contract.
pub fn apply_routine_execution_governance(
    mut execution: RoutineExecutionConfig,
    overrides: RoutineExecutionGovernanceOverrides,
) -> Result<RoutineExecutionConfig, RoutineRegistryError> {
    execution.wake_governance_authoritative = overrides.authoritative.unwrap_or(true);
    if let Some(execution_mode) = overrides.execution_mode {
        execution.execution_mode = execution_mode;
        if execution_mode == RoutineExecutionMode::Agent {
            execution.preflight_probe = None;
            execution.wake_predicate = None;
        }
    }
    if overrides.preflight_probe.is_some() {
        execution.preflight_probe = overrides.preflight_probe;
    }
    if overrides.wake_predicate.is_some() {
        execution.wake_predicate = overrides.wake_predicate;
    }
    if let Some(context_sources) = overrides.context_sources {
        execution.context_sources = context_sources;
    }
    if overrides.tool_profile.is_some() {
        execution.tool_profile = overrides.tool_profile;
    }
    if overrides.active_hours.is_some() {
        execution.active_hours = overrides.active_hours;
    }
    if let Some(flood_window_ms) = overrides.flood_window_ms {
        execution.flood_window_ms = flood_window_ms;
    }
    if let Some(flood_max_wakes) = overrides.flood_max_wakes {
        execution.flood_max_wakes = flood_max_wakes;
    }
    normalize_execution(execution)
}

/// Preserves additive governance while an older full-upsert surface changes legacy fields.
#[must_use]
pub fn preserve_routine_execution_governance(
    mut requested: RoutineExecutionConfig,
    existing: &RoutineExecutionConfig,
) -> RoutineExecutionConfig {
    requested.execution_mode = existing.execution_mode;
    requested.wake_governance_authoritative = existing.wake_governance_authoritative;
    requested.preflight_probe.clone_from(&existing.preflight_probe);
    requested.wake_predicate.clone_from(&existing.wake_predicate);
    requested.context_sources.clone_from(&existing.context_sources);
    requested.tool_profile.clone_from(&existing.tool_profile);
    requested.active_hours.clone_from(&existing.active_hours);
    requested.flood_window_ms = existing.flood_window_ms;
    requested.flood_max_wakes = existing.flood_max_wakes;
    requested
}

/// Builds the stable operator-visible projection of autonomous execution settings.
#[must_use]
pub fn routine_execution_governance_projection(execution: &RoutineExecutionConfig) -> Value {
    json!({
        "schema_version": 1,
        "rollout_mode": if execution.wake_governance_authoritative {
            "authoritative"
        } else {
            "shadow"
        },
        "authoritative": execution.wake_governance_authoritative,
        "execution_mode": execution.execution_mode.as_str(),
        "preflight_probe": execution.preflight_probe,
        "wake_predicate": execution.wake_predicate,
        "context_sources": execution.context_sources,
        "tool_profile": execution.tool_profile,
        "active_hours": execution.active_hours,
        "flood_window_ms": execution.flood_window_ms,
        "flood_max_wakes": execution.flood_max_wakes,
    })
}

/// Returns whether `now_unix_ms` falls inside a timezone-aware active window.
pub fn routine_active_hours_contains(
    active_hours: &RoutineActiveHours,
    now_unix_ms: i64,
) -> Result<bool, RoutineRegistryError> {
    validate_active_hours(active_hours)?;
    let utc = Utc.timestamp_millis_opt(now_unix_ms).single().ok_or_else(|| {
        RoutineRegistryError::InvalidField {
            field: "execution.active_hours",
            message: "active-hours timestamp is outside the supported range".to_owned(),
        }
    })?;
    let minute_of_day = if active_hours.timezone.eq_ignore_ascii_case("utc") {
        utc.hour() as u16 * 60 + utc.minute() as u16
    } else {
        let timezone = active_hours.timezone.parse::<Tz>().map_err(|_| {
            RoutineRegistryError::InvalidField {
                field: "execution.active_hours.timezone",
                message: "timezone must be utc or a valid IANA timezone".to_owned(),
            }
        })?;
        let local = utc.with_timezone(&timezone);
        local.hour() as u16 * 60 + local.minute() as u16
    };
    let start = active_hours.start_minute_of_day;
    let end = active_hours.end_minute_of_day;
    Ok(if start == end {
        true
    } else if start < end {
        (start..end).contains(&minute_of_day)
    } else {
        minute_of_day >= start || minute_of_day < end
    })
}

pub(super) fn normalize_execution(
    execution: RoutineExecutionConfig,
) -> Result<RoutineExecutionConfig, RoutineRegistryError> {
    validate_execution_governance(&execution)?;
    Ok(RoutineExecutionConfig {
        run_mode: execution.run_mode,
        procedure_profile_id: execution
            .procedure_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.procedure_profile_id"))
            .transpose()?,
        skill_profile_id: execution
            .skill_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.skill_profile_id"))
            .transpose()?,
        provider_profile_id: execution
            .provider_profile_id
            .map(|value| normalize_identifier(value.as_str(), "execution.provider_profile_id"))
            .transpose()?,
        execution_posture: execution.execution_posture,
        execution_mode: execution.execution_mode,
        wake_governance_authoritative: execution.wake_governance_authoritative,
        preflight_probe: execution.preflight_probe,
        wake_predicate: execution.wake_predicate,
        context_sources: execution
            .context_sources
            .into_iter()
            .map(normalize_context_source)
            .collect::<Result<Vec<_>, _>>()?,
        tool_profile: execution.tool_profile.map(normalize_tool_profile).transpose()?,
        active_hours: execution.active_hours.map(normalize_active_hours).transpose()?,
        flood_window_ms: execution.flood_window_ms,
        flood_max_wakes: execution.flood_max_wakes,
    })
}

fn validate_execution_governance(
    execution: &RoutineExecutionConfig,
) -> Result<(), RoutineRegistryError> {
    if execution.execution_mode != RoutineExecutionMode::Agent
        && !execution.wake_governance_authoritative
    {
        return invalid_execution_field(
            "execution.wake_governance_authoritative",
            "no_agent and probe_then_agent require explicit execution_governance activation",
        );
    }
    match execution.execution_mode {
        RoutineExecutionMode::Agent => {
            if execution.preflight_probe.is_some() || execution.wake_predicate.is_some() {
                return invalid_execution_field(
                    "execution.preflight_probe",
                    "agent mode cannot declare a preflight probe or wake predicate",
                );
            }
        }
        RoutineExecutionMode::NoAgent | RoutineExecutionMode::ProbeThenAgent => {
            let probe = execution.preflight_probe.as_ref().ok_or_else(|| {
                RoutineRegistryError::InvalidField {
                    field: "execution.preflight_probe",
                    message: "no_agent and probe_then_agent require a preflight probe".to_owned(),
                }
            })?;
            validate_preflight_probe(probe)?;
            let predicate = execution.wake_predicate.as_ref().ok_or_else(|| {
                RoutineRegistryError::InvalidField {
                    field: "execution.wake_predicate",
                    message: "no_agent and probe_then_agent require a wake predicate".to_owned(),
                }
            })?;
            validate_wake_predicate(predicate)?;
        }
    }
    if execution.execution_mode == RoutineExecutionMode::NoAgent {
        if !execution.context_sources.is_empty() {
            return invalid_execution_field(
                "execution.context_sources",
                "no_agent mode cannot project context into an agent",
            );
        }
        if execution.tool_profile.as_ref().is_some_and(|profile| !profile.allowed_tools.is_empty())
        {
            return invalid_execution_field(
                "execution.tool_profile",
                "no_agent mode cannot invoke agent tools",
            );
        }
    }
    if execution.context_sources.len() > 16 {
        return invalid_execution_field(
            "execution.context_sources",
            "at most 16 context source artifacts are allowed",
        );
    }
    if let Some(active_hours) = execution.active_hours.as_ref() {
        validate_active_hours(active_hours)?;
    }
    if !(60_000..=24 * 60 * 60 * 1_000).contains(&execution.flood_window_ms) {
        return invalid_execution_field(
            "execution.flood_window_ms",
            "flood_window_ms must be between 60000 and 86400000",
        );
    }
    if !(1..=100).contains(&execution.flood_max_wakes) {
        return invalid_execution_field(
            "execution.flood_max_wakes",
            "flood_max_wakes must be between 1 and 100",
        );
    }
    Ok(())
}

fn validate_preflight_probe(probe: &RoutinePreflightProbe) -> Result<(), RoutineRegistryError> {
    if !(1..=30_000).contains(&probe.timeout_ms) {
        return invalid_execution_field(
            "execution.preflight_probe.timeout_ms",
            "probe timeout must be between 1 and 30000 milliseconds",
        );
    }
    if !(1..=16 * 1024).contains(&probe.output_max_bytes) {
        return invalid_execution_field(
            "execution.preflight_probe.output_max_bytes",
            "probe output bound must be between 1 and 16384 bytes",
        );
    }
    match probe.kind {
        RoutineProbeKind::DaemonHealth => Ok(()),
    }
}

fn validate_wake_predicate(predicate: &RoutineWakePredicate) -> Result<(), RoutineRegistryError> {
    match predicate {
        RoutineWakePredicate::ProbeHealthy => Ok(()),
        RoutineWakePredicate::JsonPointerEquals { pointer, expected } => {
            if !pointer.starts_with('/') || pointer.len() > 256 {
                return invalid_execution_field(
                    "execution.wake_predicate.pointer",
                    "JSON pointer must start with '/' and be at most 256 bytes",
                );
            }
            if expected.to_string().len() > 4_096 {
                return invalid_execution_field(
                    "execution.wake_predicate.expected",
                    "predicate expected value exceeds 4096 bytes",
                );
            }
            Ok(())
        }
    }
}

fn normalize_context_source(
    source: RoutineContextSourceArtifact,
) -> Result<RoutineContextSourceArtifact, RoutineRegistryError> {
    let artifact_id = normalize_freeform_identifier(
        source.artifact_id.as_str(),
        "execution.context.artifact_id",
    )?;
    if !is_lowercase_sha256(source.sha256.as_str()) {
        return invalid_execution_field(
            "execution.context.sha256",
            "artifact sha256 must be a lowercase SHA-256 digest",
        );
    }
    let sensitivity = source.sensitivity.trim().to_ascii_lowercase();
    if !matches!(sensitivity.as_str(), "public" | "personal" | "sensitive") {
        return invalid_execution_field(
            "execution.context.sensitivity",
            "sensitivity must be public, personal, or sensitive",
        );
    }
    if !(1..=256 * 1024).contains(&source.max_bytes) {
        return invalid_execution_field(
            "execution.context.max_bytes",
            "context artifact max_bytes must be between 1 and 262144",
        );
    }
    Ok(RoutineContextSourceArtifact {
        artifact_id,
        sha256: source.sha256,
        sensitivity,
        max_bytes: source.max_bytes,
    })
}

fn normalize_tool_profile(
    profile: RoutineToolProfile,
) -> Result<RoutineToolProfile, RoutineRegistryError> {
    let profile_id =
        normalize_freeform_identifier(profile.profile_id.as_str(), "execution.tool_profile.id")?;
    if profile.allowed_tools.len() > 64 {
        return invalid_execution_field(
            "execution.tool_profile.allowed_tools",
            "tool profile may allow at most 64 tools",
        );
    }
    let mut allowed_tools = BTreeSet::new();
    for tool in profile.allowed_tools {
        let normalized = tool.trim().to_ascii_lowercase();
        if normalized.is_empty() || normalized == "*" || normalized.len() > 256 {
            return invalid_execution_field(
                "execution.tool_profile.allowed_tools",
                "tool names must be explicit, non-empty, and at most 256 bytes",
            );
        }
        allowed_tools.insert(normalized);
    }
    Ok(RoutineToolProfile { profile_id, allowed_tools: allowed_tools.into_iter().collect() })
}

fn validate_active_hours(active_hours: &RoutineActiveHours) -> Result<(), RoutineRegistryError> {
    if active_hours.start_minute_of_day >= 1_440 || active_hours.end_minute_of_day >= 1_440 {
        return invalid_execution_field(
            "execution.active_hours",
            "active-hours minute-of-day values must be between 0 and 1439",
        );
    }
    let timezone = active_hours.timezone.trim();
    if timezone.is_empty()
        || (!timezone.eq_ignore_ascii_case("utc") && timezone.parse::<Tz>().is_err())
    {
        return invalid_execution_field(
            "execution.active_hours.timezone",
            "timezone must be utc or a valid IANA timezone",
        );
    }
    Ok(())
}

fn normalize_active_hours(
    active_hours: RoutineActiveHours,
) -> Result<RoutineActiveHours, RoutineRegistryError> {
    validate_active_hours(&active_hours)?;
    let timezone = active_hours.timezone.trim();
    Ok(RoutineActiveHours {
        start_minute_of_day: active_hours.start_minute_of_day,
        end_minute_of_day: active_hours.end_minute_of_day,
        timezone: if timezone.eq_ignore_ascii_case("utc") {
            "UTC".to_owned()
        } else {
            timezone.to_owned()
        },
    })
}

fn invalid_execution_field<T>(
    field: &'static str,
    message: &str,
) -> Result<T, RoutineRegistryError> {
    Err(RoutineRegistryError::InvalidField { field, message: message.to_owned() })
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    #[test]
    fn execution_governance_defaults_legacy_records_to_agent_mode() {
        let execution = serde_json::from_value::<RoutineExecutionConfig>(json!({
            "run_mode": "same_session",
            "execution_posture": "standard"
        }))
        .expect("legacy execution config should deserialize");

        assert_eq!(execution.execution_mode, RoutineExecutionMode::Agent);
        assert!(!execution.wake_governance_authoritative);
        assert_eq!(execution.flood_window_ms, 3_600_000);
        assert_eq!(execution.flood_max_wakes, 8);
        let governed = apply_routine_execution_governance(
            execution,
            RoutineExecutionGovernanceOverrides {
                active_hours: Some(RoutineActiveHours {
                    start_minute_of_day: 8 * 60,
                    end_minute_of_day: 18 * 60,
                    timezone: "  Europe/Prague  ".to_owned(),
                }),
                ..RoutineExecutionGovernanceOverrides::default()
            },
        )
        .expect("explicit governance block should activate authoritative wake gates");
        assert!(governed.wake_governance_authoritative);
        assert_eq!(
            governed.active_hours.as_ref().map(|window| window.timezone.as_str()),
            Some("Europe/Prague")
        );
        assert_eq!(
            routine_execution_governance_projection(&governed)
                .pointer("/rollout_mode")
                .and_then(Value::as_str),
            Some("authoritative")
        );
        let preserved = preserve_routine_execution_governance(
            RoutineExecutionConfig {
                run_mode: RoutineRunMode::FreshSession,
                ..RoutineExecutionConfig::default()
            },
            &governed,
        );
        assert_eq!(preserved.run_mode, RoutineRunMode::FreshSession);
        assert!(preserved.wake_governance_authoritative);
        assert_eq!(preserved.active_hours, governed.active_hours);
        let shadow = apply_routine_execution_governance(
            governed,
            RoutineExecutionGovernanceOverrides {
                authoritative: Some(false),
                execution_mode: Some(RoutineExecutionMode::Agent),
                ..RoutineExecutionGovernanceOverrides::default()
            },
        )
        .expect("explicit rollback should retain shadow-only governance");
        assert!(!shadow.wake_governance_authoritative);
    }

    #[test]
    fn no_agent_requires_closed_probe_and_never_accepts_tools() {
        let missing_probe = apply_routine_execution_governance(
            RoutineExecutionConfig::default(),
            RoutineExecutionGovernanceOverrides {
                execution_mode: Some(RoutineExecutionMode::NoAgent),
                ..RoutineExecutionGovernanceOverrides::default()
            },
        );
        assert!(matches!(
            missing_probe,
            Err(RoutineRegistryError::InvalidField { field: "execution.preflight_probe", .. })
        ));

        let wildcard_tool = apply_routine_execution_governance(
            RoutineExecutionConfig::default(),
            RoutineExecutionGovernanceOverrides {
                execution_mode: Some(RoutineExecutionMode::NoAgent),
                preflight_probe: Some(RoutinePreflightProbe {
                    kind: RoutineProbeKind::DaemonHealth,
                    timeout_ms: 1_000,
                    output_max_bytes: 1_024,
                }),
                wake_predicate: Some(RoutineWakePredicate::ProbeHealthy),
                tool_profile: Some(RoutineToolProfile {
                    profile_id: "health".to_owned(),
                    allowed_tools: vec!["*".to_owned()],
                }),
                ..RoutineExecutionGovernanceOverrides::default()
            },
        );
        assert!(wildcard_tool.is_err());
    }

    #[test]
    fn wake_predicate_reports_false_and_true_with_stable_reasons() {
        let predicate = RoutineWakePredicate::JsonPointerEquals {
            pointer: "/healthy".to_owned(),
            expected: json!(true),
        };
        let observation = |healthy| RoutineProbeObservation {
            healthy,
            output: json!({ "healthy": healthy }),
            output_sha256: "a".repeat(64),
            summary: "bounded probe".to_owned(),
            duration_ms: 2,
        };

        let denied = evaluate_routine_wake_predicate(&predicate, &observation(false));
        let allowed = evaluate_routine_wake_predicate(&predicate, &observation(true));

        assert_eq!(denied.decision, WakePredicateDecision::NotMatched);
        assert_eq!(denied.reason_code, "wake.predicate_not_matched");
        assert_eq!(allowed.decision, WakePredicateDecision::Matched);
        assert_eq!(allowed.reason_code, "wake.predicate_matched");
    }

    #[test]
    fn active_hours_use_iana_timezone_across_utc_offsets() {
        let active_hours = RoutineActiveHours {
            start_minute_of_day: 8 * 60,
            end_minute_of_day: 18 * 60,
            timezone: "Europe/Prague".to_owned(),
        };
        let summer_morning = DateTime::parse_from_rfc3339("2026-07-28T07:00:00Z")
            .expect("timestamp")
            .timestamp_millis();
        let summer_evening = DateTime::parse_from_rfc3339("2026-07-28T18:00:00Z")
            .expect("timestamp")
            .timestamp_millis();
        let winter_morning = DateTime::parse_from_rfc3339("2026-01-28T07:00:00Z")
            .expect("timestamp")
            .timestamp_millis();

        assert!(routine_active_hours_contains(&active_hours, summer_morning).expect("summer"));
        assert!(!routine_active_hours_contains(&active_hours, summer_evening).expect("evening"));
        assert!(routine_active_hours_contains(&active_hours, winter_morning).expect("winter"));
    }
}
