//! Unit tests pinning which connector states count as operator-actionable
//! degradations in console connector observability.

use super::connector_has_actionable_degradation;
use palyra_connectors::{
    ConnectorAvailability, ConnectorKind, ConnectorLiveness, ConnectorQueueDepth,
    ConnectorReadiness, ConnectorStatusSnapshot,
};
use serde_json::json;

fn connector(
    kind: ConnectorKind,
    availability: ConnectorAvailability,
    enabled: bool,
    readiness: ConnectorReadiness,
    liveness: ConnectorLiveness,
    last_error: Option<&str>,
) -> ConnectorStatusSnapshot {
    ConnectorStatusSnapshot {
        connector_id: format!("{}:default", kind.as_str()),
        kind,
        availability,
        capabilities: palyra_connectors::providers::provider_capabilities(kind),
        principal: format!("channel:{}", kind.as_str()),
        enabled,
        readiness,
        liveness,
        restart_count: 0,
        queue_depth: ConnectorQueueDepth { pending_outbox: 0, dead_letters: 0 },
        last_error: last_error.map(ToOwned::to_owned),
        last_inbound_unix_ms: None,
        last_outbound_unix_ms: None,
        updated_at_unix_ms: 0,
    }
}

#[test]
fn idle_disabled_supported_or_internal_test_connectors_are_not_actionable_degradations() {
    let disabled_supported = connector(
        ConnectorKind::Slack,
        ConnectorAvailability::Supported,
        false,
        ConnectorReadiness::Ready,
        ConnectorLiveness::Crashed,
        None,
    );
    assert!(
        !connector_has_actionable_degradation(&disabled_supported, None),
        "disabled optional connectors should not make fresh installs degraded"
    );

    let internal_echo = connector(
        ConnectorKind::Echo,
        ConnectorAvailability::InternalTestOnly,
        true,
        ConnectorReadiness::Ready,
        ConnectorLiveness::Restarting,
        None,
    );
    assert!(
        !connector_has_actionable_degradation(&internal_echo, None),
        "internal-test-only idle connectors should not affect operator health"
    );
}

#[test]
fn enabled_supported_non_running_connectors_are_actionable_degradations() {
    for liveness in
        [ConnectorLiveness::Stopped, ConnectorLiveness::Restarting, ConnectorLiveness::Crashed]
    {
        let status = connector(
            ConnectorKind::Slack,
            ConnectorAvailability::Supported,
            true,
            ConnectorReadiness::Ready,
            liveness,
            None,
        );

        assert!(
            connector_has_actionable_degradation(&status, None),
            "enabled supported connector with {liveness:?} liveness should be degraded"
        );
    }

    let running = connector(
        ConnectorKind::Slack,
        ConnectorAvailability::Supported,
        true,
        ConnectorReadiness::Ready,
        ConnectorLiveness::Running,
        None,
    );
    assert!(!connector_has_actionable_degradation(&running, None));
}

#[test]
fn readiness_and_runtime_errors_remain_actionable_degradations() {
    let missing_credential = connector(
        ConnectorKind::Slack,
        ConnectorAvailability::Supported,
        true,
        ConnectorReadiness::MissingCredential,
        ConnectorLiveness::Stopped,
        None,
    );
    assert!(connector_has_actionable_degradation(&missing_credential, None));

    let runtime_error = connector(
        ConnectorKind::Slack,
        ConnectorAvailability::Supported,
        true,
        ConnectorReadiness::Ready,
        ConnectorLiveness::Stopped,
        None,
    );
    let runtime = json!({ "last_error": "provider token validation failed" });
    assert!(connector_has_actionable_degradation(&runtime_error, Some(&runtime)));
}
