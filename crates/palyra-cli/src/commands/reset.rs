//! Destructive reset of config, state, workspace, and service scopes.
//!
//! Resets move paths aside with a timestamped suffix instead of deleting
//! them, so every action stays manually reversible; the gateway service
//! must be included when runtime scopes are reset while it is installed.

use std::{fs, path::PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::Serialize;

use crate::cli::{ResetCommand, ResetScopeArg};
use crate::*;

/// Outcome of one reset scope; field names are part of the pinned JSON
/// output shape.
#[derive(Debug, Clone, Serialize)]
struct ResetActionReport {
    scope: String,
    source: String,
    destination: Option<String>,
    action: String,
    applied: bool,
    detail: Option<String>,
}

/// Aggregate reset report with rollback guidance; field names are part of
/// the pinned JSON output shape.
#[derive(Debug, Clone, Serialize)]
struct ResetReport {
    dry_run: bool,
    actions: Vec<ResetActionReport>,
    rollback_hint: String,
    next_steps: Vec<String>,
}

/// Runs `palyra reset`, moving the selected scopes aside (or previewing in
/// dry-run mode) and emitting a rollback-oriented report.
///
/// # Errors
/// Fails when no scope is given, the destructive run lacks `--yes`, a
/// runtime scope is reset while the installed service scope is omitted, a
/// move target is unsafe or already exists, or a rename fails.
pub(crate) fn run_reset(command: ResetCommand) -> Result<()> {
    let context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for reset command"))?;
    if command.scopes.is_empty() {
        anyhow::bail!("reset requires at least one --scope");
    }
    if !command.dry_run && !command.yes {
        anyhow::bail!("reset is destructive; re-run with --yes or use --dry-run");
    }

    let service_status = support::service::query_gateway_service_status(context.state_root()).ok();
    let resets_runtime = command.scopes.iter().any(|scope| {
        matches!(scope, ResetScopeArg::Config | ResetScopeArg::State | ResetScopeArg::Workspace)
    });
    let includes_service =
        command.scopes.iter().any(|scope| matches!(scope, ResetScopeArg::Service));
    // An installed service would keep running against moved-aside paths;
    // force the operator to include the service scope so it is removed
    // first.
    if resets_runtime
        && service_status.as_ref().is_some_and(|value| value.installed)
        && !includes_service
    {
        anyhow::bail!(
            "reset of config/state requires --scope service when the gateway service is installed"
        );
    }

    let stamp = now_unix_ms_i64()?;
    let mut actions = Vec::new();
    for scope in command.scopes {
        match scope {
            ResetScopeArg::Service => {
                let status = service_status.clone().unwrap_or_else(|| {
                    support::service::GatewayServiceStatus {
                        installed: false,
                        running: false,
                        enabled: false,
                        manager: "unknown".to_owned(),
                        service_name: "palyra-gateway".to_owned(),
                        definition_path: None,
                        stdout_log_path: None,
                        stderr_log_path: None,
                        detail: Some("service status unavailable".to_owned()),
                    }
                });
                if command.dry_run || !status.installed {
                    actions.push(ResetActionReport {
                        scope: "service".to_owned(),
                        source: status.service_name,
                        destination: None,
                        action: "uninstall".to_owned(),
                        applied: false,
                        detail: status.detail,
                    });
                } else {
                    let status = support::service::uninstall_gateway_service(context.state_root())?;
                    actions.push(ResetActionReport {
                        scope: "service".to_owned(),
                        source: status.service_name,
                        destination: None,
                        action: "uninstall".to_owned(),
                        applied: true,
                        detail: status.detail,
                    });
                }
            }
            ResetScopeArg::Config => {
                let config_path = command
                    .config_path
                    .clone()
                    .map(PathBuf::from)
                    .or_else(|| context.config_path().map(PathBuf::from))
                    .ok_or_else(|| {
                        anyhow!(
                            "reset --scope config requires --config-path or an active config path"
                        )
                    })?;
                let destination = rename_destination(config_path.as_path(), stamp, "reset")?;
                actions.push(apply_move_action(
                    "config",
                    config_path,
                    destination,
                    command.dry_run,
                )?);
            }
            ResetScopeArg::State => {
                let state_root = context.state_root().to_path_buf();
                let destination = rename_destination(state_root.as_path(), stamp, "reset")?;
                actions.push(apply_move_action("state", state_root, destination, command.dry_run)?);
            }
            ResetScopeArg::Workspace => {
                let workspace_root =
                    command.workspace_root.clone().map(PathBuf::from).ok_or_else(|| {
                        anyhow!("reset --scope workspace requires --workspace-root")
                    })?;
                let destination = rename_destination(workspace_root.as_path(), stamp, "reset")?;
                actions.push(apply_move_action(
                    "workspace",
                    workspace_root,
                    destination,
                    command.dry_run,
                )?);
            }
        }
    }

    let report = ResetReport {
        dry_run: command.dry_run,
        actions,
        rollback_hint:
            "Restore the moved paths back to their original locations if you need to roll back the reset."
                .to_owned(),
        next_steps: vec![
            "Run `palyra health` after rebuilding the installation.".to_owned(),
            "Run `palyra support-bundle export --output ./support-bundle.json` before deeper troubleshooting if the reset was triggered by runtime failures.".to_owned(),
        ],
    };
    emit_reset_report(&report)
}

// Moves one scope path aside (never deletes), reporting a non-applied action
// when the source is missing or in dry-run mode.
fn apply_move_action(
    scope: &str,
    source: PathBuf,
    destination: PathBuf,
    dry_run: bool,
) -> Result<ResetActionReport> {
    if !source.exists() {
        return Ok(ResetActionReport {
            scope: scope.to_owned(),
            source: source.display().to_string(),
            destination: Some(destination.display().to_string()),
            action: "move-aside".to_owned(),
            applied: false,
            detail: Some("source does not exist".to_owned()),
        });
    }
    support::lifecycle::ensure_safe_removal_target(source.as_path(), scope)?;
    if destination.exists() {
        anyhow::bail!("reset destination already exists for {}: {}", scope, destination.display());
    }
    if !dry_run {
        fs::rename(source.as_path(), destination.as_path()).with_context(|| {
            format!(
                "failed to move {} from {} to {}",
                scope,
                source.display(),
                destination.display()
            )
        })?;
    }
    Ok(ResetActionReport {
        scope: scope.to_owned(),
        source: source.display().to_string(),
        destination: Some(destination.display().to_string()),
        action: "move-aside".to_owned(),
        applied: !dry_run,
        detail: None,
    })
}

fn rename_destination(source: &std::path::Path, stamp: i64, suffix: &str) -> Result<PathBuf> {
    let parent =
        source.parent().ok_or_else(|| anyhow!("path has no parent: {}", source.display()))?;
    let file_name = source
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("path is not valid UTF-8: {}", source.display()))?;
    Ok(parent.join(format!("{file_name}.{suffix}-{stamp}")))
}

fn emit_reset_report(report: &ResetReport) -> Result<()> {
    let context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for reset command"))?;
    if context.prefers_json() {
        return output::print_json_pretty(report, "failed to encode reset output as JSON");
    }
    if context.prefers_ndjson() {
        for action in report.actions.as_slice() {
            output::print_json_line(action, "failed to encode reset action as NDJSON")?;
        }
        return std::io::stdout().flush().context("stdout flush failed");
    }
    println!("reset.dry_run={}", report.dry_run);
    for action in report.actions.as_slice() {
        println!(
            "reset.action scope={} action={} applied={} source={} destination={}",
            action.scope,
            action.action,
            action.applied,
            action.source,
            action.destination.as_deref().unwrap_or("none")
        );
        if let Some(detail) = action.detail.as_deref() {
            println!("reset.action.detail={detail}");
        }
    }
    println!("reset.rollback_hint={}", report.rollback_hint);
    for step in report.next_steps.as_slice() {
        println!("reset.next_step={step}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

#[cfg(test)]
mod tests {
    use super::apply_move_action;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn apply_move_action_dry_run_preserves_source() {
        let tempdir = tempdir().expect("tempdir");
        let source = tempdir.path().join("state");
        let destination = tempdir.path().join("state.reset-123");
        fs::create_dir_all(source.as_path()).expect("create source");

        let report =
            apply_move_action("state", source.clone(), destination.clone(), true).expect("dry run");
        assert!(!report.applied, "dry-run must not apply changes");
        assert!(source.exists(), "source should remain in place for dry-run");
        assert!(!destination.exists(), "dry-run must not create destination");
        assert_eq!(report.destination.as_deref(), Some(destination.display().to_string().as_str()));
    }
}
