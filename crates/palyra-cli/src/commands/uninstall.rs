//! Destructive uninstall of a portable install root and, optionally, its
//! state root.
//!
//! Refuses to remove the install root the running binary lives in, and only
//! removes paths that pass the shared safe-removal-target checks.

use std::{fs, path::PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::Serialize;

use crate::cli::UninstallCommand;
use crate::*;

/// Uninstall outcome; field names are part of the pinned JSON output shape.
#[derive(Debug, Clone, Serialize)]
struct UninstallReport {
    dry_run: bool,
    install_root: String,
    state_root: Option<String>,
    install_root_removed: bool,
    state_root_removed: bool,
    cli_cleanup: Option<support::lifecycle::CliExposureCleanupReport>,
    rollback_hint: Option<String>,
    migration_notes: Option<String>,
    windows_path_cleanup_required: bool,
    next_steps: Vec<String>,
}

/// Runs `palyra uninstall`, removing the install root (and state root with
/// `--remove-state`) or previewing in dry-run mode.
///
/// # Errors
/// Fails when the destructive run lacks `--yes`, the active binary lives
/// inside the target install root, a removal target is unsafe, or a
/// filesystem removal fails.
pub(crate) fn run_uninstall(command: UninstallCommand) -> Result<()> {
    if !command.dry_run && !command.yes {
        anyhow::bail!("uninstall is destructive; re-run with --yes or use --dry-run");
    }

    let install_root = support::lifecycle::resolve_install_root(command.install_root)?;
    let current_exe = support::lifecycle::current_cli_binary_path()?;
    if !command.dry_run
        && support::lifecycle::path_starts_with(current_exe.as_path(), install_root.as_path())
    {
        anyhow::bail!(
            "uninstall cannot remove the active install root {}; run the command from another CLI binary or use --dry-run",
            install_root.display()
        );
    }

    let metadata = support::lifecycle::load_install_metadata(install_root.as_path())?;
    let rollback_hint =
        support::lifecycle::load_release_note(install_root.as_path(), "ROLLBACK.txt")?;
    let migration_notes =
        support::lifecycle::load_release_note(install_root.as_path(), "MIGRATION_NOTES.txt")?;
    let state_root =
        metadata.as_ref().and_then(|value| value.state_root.as_deref()).map(PathBuf::from);

    let cli_cleanup = if command.dry_run {
        None
    } else if let Some(exposure) = metadata.as_ref().and_then(|value| value.cli_exposure.as_ref()) {
        Some(support::lifecycle::remove_cli_exposure(exposure)?)
    } else {
        None
    };

    if !command.dry_run {
        if let Some(state_root) = state_root.as_ref() {
            // Best effort: a missing or already-removed service must not
            // block removal of the install root itself.
            let _ = support::service::uninstall_gateway_service(state_root.as_path());
        }
        support::lifecycle::ensure_safe_removal_target(install_root.as_path(), "install_root")?;
        if install_root.exists() {
            fs::remove_dir_all(install_root.as_path()).with_context(|| {
                format!("failed to remove install root {}", install_root.display())
            })?;
        }
        if command.remove_state {
            if let Some(state_root) = state_root.as_ref() {
                if state_root.exists() {
                    support::lifecycle::ensure_safe_removal_target(
                        state_root.as_path(),
                        "state_root",
                    )?;
                    fs::remove_dir_all(state_root.as_path()).with_context(|| {
                        format!("failed to remove state root {}", state_root.display())
                    })?;
                }
            }
        }
    }

    let report = UninstallReport {
        dry_run: command.dry_run,
        install_root: install_root.display().to_string(),
        state_root: state_root.as_ref().map(|value| value.display().to_string()),
        install_root_removed: !command.dry_run,
        state_root_removed: !command.dry_run && command.remove_state && state_root.is_some(),
        windows_path_cleanup_required: cli_cleanup
            .as_ref()
            .is_some_and(|value| value.windows_path_cleanup_required),
        cli_cleanup,
        rollback_hint,
        migration_notes,
        next_steps: vec![
            "Export a support bundle before repeating the installation if the uninstall was triggered by runtime regressions.".to_owned(),
            "Reinstall from a portable archive and verify with `palyra doctor --json`.".to_owned(),
        ],
    };
    emit_uninstall_report(&report)
}

fn emit_uninstall_report(report: &UninstallReport) -> Result<()> {
    let context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for uninstall command"))?;
    if context.prefers_json() {
        return output::print_json_pretty(report, "failed to encode uninstall output as JSON");
    }
    if context.prefers_ndjson() {
        return output::print_json_line(report, "failed to encode uninstall output as NDJSON");
    }
    println!(
        "uninstall dry_run={} install_root={} install_root_removed={} state_root={} state_root_removed={}",
        report.dry_run,
        report.install_root,
        report.install_root_removed,
        report.state_root.as_deref().unwrap_or("none"),
        report.state_root_removed
    );
    if let Some(cleanup) = report.cli_cleanup.as_ref() {
        println!(
            "uninstall.cli_cleanup removed_shims={} removed_profiles={} command_root_removed={} windows_path_cleanup_required={}",
            cleanup.removed_shim_paths.len(),
            cleanup.removed_profile_files.len(),
            cleanup.command_root_removed,
            cleanup.windows_path_cleanup_required
        );
    }
    if let Some(rollback_hint) = report.rollback_hint.as_deref() {
        println!("uninstall.rollback_hint={}", rollback_hint.replace('\n', " | "));
    }
    if let Some(migration_notes) = report.migration_notes.as_deref() {
        println!("uninstall.migration_notes={}", migration_notes.replace('\n', " | "));
    }
    for step in report.next_steps.as_slice() {
        println!("uninstall.next_step={step}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}
