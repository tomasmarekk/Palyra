//! Best-effort runtime reload helpers for CLI commands that persist active
//! model-provider configuration.

use crate::{app, client};
use anyhow::Result;
use palyra_common::redaction::{redact_auth_error, redact_url_segments_in_text};
use palyra_control_plane as control_plane;
use serde::Serialize;

/// Result of attempting to apply the active config to a running daemon.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct RuntimeConfigReloadOutcome {
    pub(crate) reload_state: String,
    pub(crate) message: String,
    pub(crate) active_runs: Option<u64>,
    pub(crate) applied_steps: usize,
    pub(crate) skipped_steps: usize,
    pub(crate) requires_restart: Option<bool>,
}

impl RuntimeConfigReloadOutcome {
    fn unavailable(error: anyhow::Error) -> Self {
        Self {
            reload_state: "unavailable".to_owned(),
            message: sanitize_reload_error(error),
            active_runs: None,
            applied_steps: 0,
            skipped_steps: 0,
            requires_restart: Some(true),
        }
    }

    fn from_apply(envelope: control_plane::ConfigReloadApplyEnvelope) -> Self {
        Self {
            reload_state: envelope.outcome,
            message: envelope.message,
            active_runs: Some(envelope.plan.active_runs),
            applied_steps: envelope.applied_steps.len(),
            skipped_steps: envelope.skipped_steps.len(),
            requires_restart: Some(envelope.plan.requires_restart),
        }
    }
}

/// Applies a config reload over the current admin console when one is reachable.
///
/// Transport/setup failures are returned as an explicit `unavailable` outcome so
/// local config writers can still operate before a daemon has been started.
#[must_use]
pub(crate) async fn try_apply_active_config_reload(
    path: Option<String>,
) -> RuntimeConfigReloadOutcome {
    match apply_active_config_reload(path).await {
        Ok(outcome) => outcome,
        Err(error) => RuntimeConfigReloadOutcome::unavailable(error),
    }
}

/// Synchronous wrapper for command surfaces that are not already async.
#[must_use]
pub(crate) fn try_apply_active_config_reload_blocking(
    path: Option<String>,
) -> RuntimeConfigReloadOutcome {
    match crate::build_runtime() {
        Ok(runtime) => runtime.block_on(try_apply_active_config_reload(path)),
        Err(error) => RuntimeConfigReloadOutcome::unavailable(error),
    }
}

/// Formats one terse text line for non-JSON command output.
#[must_use]
pub(crate) fn reload_text_line(prefix: &str, outcome: &RuntimeConfigReloadOutcome) -> String {
    format!(
        "{prefix}.reload state={} active_runs={} applied_steps={} skipped_steps={} requires_restart={} message=\"{}\"",
        outcome.reload_state,
        outcome.active_runs.map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned()),
        outcome.applied_steps,
        outcome.skipped_steps,
        outcome
            .requires_restart
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        outcome.message.replace('"', "'")
    )
}

async fn apply_active_config_reload(path: Option<String>) -> Result<RuntimeConfigReloadOutcome> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let envelope = context
        .client
        .apply_config_reload(&control_plane::ConfigReloadApplyRequest {
            path,
            plan_id: None,
            idempotency_key: None,
            dry_run: false,
            force: false,
        })
        .await?;
    Ok(RuntimeConfigReloadOutcome::from_apply(envelope))
}

fn sanitize_reload_error(error: anyhow::Error) -> String {
    let raw = error.to_string();
    redact_auth_error(redact_url_segments_in_text(raw.as_str()).as_str())
}

#[cfg(test)]
mod tests {
    use super::RuntimeConfigReloadOutcome;

    #[test]
    fn unavailable_reload_marks_restart_required() {
        let outcome = RuntimeConfigReloadOutcome::unavailable(anyhow::anyhow!(
            "failed to establish authenticated console session"
        ));

        assert_eq!(outcome.reload_state, "unavailable");
        assert_eq!(outcome.requires_restart, Some(true));
    }
}
