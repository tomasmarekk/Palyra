//! Operator-facing rollout stage and rollback decision projection.
//!
//! The projection combines the closed runtime profile with release-blocking
//! performance and security qualifications without exposing session identity.

use serde::Serialize;
use serde_json::Value;

use super::profile_resolver::ResolvedRuntimeProfileV1;

/// Closed deployment stage derived from the configured runtime profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum V2RolloutStage {
    Legacy,
    Shadow,
    Canary,
    DefaultOn,
}

impl V2RolloutStage {
    fn from_profile(profile: &str) -> Option<Self> {
        match profile {
            "legacy" => Some(Self::Legacy),
            "v2_shadow" => Some(Self::Shadow),
            "v2_canary" => Some(Self::Canary),
            "v2" => Some(Self::DefaultOn),
            _ => None,
        }
    }
}

/// Current operator action derived from rollout blockers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RollbackDecision {
    Continue,
    Hold,
    Rollback,
}

impl RollbackDecision {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Hold => "hold",
            Self::Rollback => "rollback",
        }
    }
}

/// Builds the bounded rollout snapshot consumed by diagnostics and runbooks.
#[must_use]
pub(crate) fn build_v2_rollout_snapshot(
    profile: &ResolvedRuntimeProfileV1,
    performance: &Value,
    security: &Value,
) -> Value {
    let Ok(profile) = serde_json::to_value(profile) else {
        return invalid_profile_snapshot();
    };
    let Some(profile_name) = profile.get("profile").and_then(Value::as_str) else {
        return invalid_profile_snapshot();
    };
    let Some(stage) = V2RolloutStage::from_profile(profile_name) else {
        return invalid_profile_snapshot();
    };
    let performance_qualified = qualification_passed(performance);
    let security_qualified = qualification_passed(security);
    let release_blocked = !performance_qualified || !security_qualified;
    let decision = match (stage, release_blocked) {
        (V2RolloutStage::Legacy, _) => RollbackDecision::Hold,
        (V2RolloutStage::Shadow, true) => RollbackDecision::Hold,
        (V2RolloutStage::Shadow, false) => RollbackDecision::Continue,
        (V2RolloutStage::Canary | V2RolloutStage::DefaultOn, true) => RollbackDecision::Rollback,
        (V2RolloutStage::Canary | V2RolloutStage::DefaultOn, false) => RollbackDecision::Continue,
    };
    let reason_code = match decision {
        RollbackDecision::Continue => "runtime.rollout.continue",
        RollbackDecision::Hold => "runtime.rollout.hold",
        RollbackDecision::Rollback => "runtime.rollout.rollback_required",
    };

    serde_json::json!({
        "schema_version": 1,
        "stage": stage,
        "decision": decision.as_str(),
        "reason_code": reason_code,
        "new_session_profile": profile_name,
        "existing_session_policy": profile.get("existing_session_policy"),
        "rollback_policy": profile.get("rollback_policy"),
        "qualifications": {
            "performance": performance_qualified,
            "security": security_qualified,
        },
        "stop_conditions": [
            "core_performance.release_blocked",
            "core_security.release_blocked",
            "runtime.invariant_violation",
            "runtime.hidden_fallback_detected",
            "runtime.orphan_resource_detected",
        ],
    })
}

fn qualification_passed(value: &Value) -> bool {
    value.get("qualified").and_then(Value::as_bool) == Some(true)
}

fn invalid_profile_snapshot() -> Value {
    serde_json::json!({
        "schema_version": 1,
        "stage": "unknown",
        "decision": "rollback",
        "reason_code": "runtime.rollout.profile_projection_invalid",
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        application::runtime_kernel_v2::profile_resolver::{
            ExistingSessionBinding, RuntimeProfileResolver,
        },
        config::{
            ExistingSessionMigrationPolicy, FeatureRolloutsConfig, RuntimeKernelConfig,
            RuntimeKernelProfile,
        },
    };

    use super::*;

    fn qualified(reason_code: &str) -> Value {
        serde_json::json!({"qualified": true, "reason_code": reason_code})
    }

    #[test]
    fn default_on_routes_new_sessions_to_v2_and_keeps_existing_sessions_pinned() {
        let config = RuntimeKernelConfig::default();
        assert_eq!(config.profile, RuntimeKernelProfile::V2);
        assert_eq!(config.existing_session_policy, ExistingSessionMigrationPolicy::KeepPinned);
        let resolver =
            RuntimeProfileResolver::resolve(&config, &FeatureRolloutsConfig::default(), None)
                .expect("default V2 profile should resolve");

        let new_session = resolver
            .profile_for_session(ExistingSessionBinding::New)
            .expect("new session should resolve");
        let existing_session = resolver
            .profile_for_session(ExistingSessionBinding::Existing {
                pinned_profile: None,
                at_safe_boundary: true,
            })
            .expect("existing session should keep its compatibility pin");

        assert_eq!(
            new_session.profile(),
            crate::application::runtime_kernel_v2::RuntimeKernelVersion::V2
        );
        assert_eq!(
            existing_session.profile(),
            crate::application::runtime_kernel_v2::RuntimeKernelVersion::Legacy
        );
    }

    #[test]
    fn default_on_rolls_back_when_a_release_gate_fails() {
        let resolver = RuntimeProfileResolver::resolve(
            &RuntimeKernelConfig::default(),
            &FeatureRolloutsConfig::default(),
            None,
        )
        .expect("default V2 profile should resolve");
        let snapshot = build_v2_rollout_snapshot(
            resolver.diagnostics(),
            &serde_json::json!({"qualified": false}),
            &qualified("core_security.qualified"),
        );

        assert_eq!(snapshot["stage"], "default_on");
        assert_eq!(snapshot["decision"], "rollback");
        assert_eq!(snapshot["reason_code"], "runtime.rollout.rollback_required");
        assert_eq!(snapshot["existing_session_policy"], "keep_pinned");
    }

    #[test]
    fn default_on_continues_only_with_both_release_qualifications() {
        let resolver = RuntimeProfileResolver::resolve(
            &RuntimeKernelConfig::default(),
            &FeatureRolloutsConfig::default(),
            None,
        )
        .expect("default V2 profile should resolve");
        let snapshot = build_v2_rollout_snapshot(
            resolver.diagnostics(),
            &qualified("core_performance.qualified"),
            &qualified("core_security.qualified"),
        );

        assert_eq!(snapshot["decision"], "continue");
        assert_eq!(snapshot["new_session_profile"], "v2");
    }
}
