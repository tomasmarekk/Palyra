//! Update planning for portable installs: inspects the current install and
//! an optional candidate archive, then emits manual next steps.
//!
//! In-place self-update is intentionally unimplemented; applying with
//! `--yes` fails closed so the trust chain stays a manual operator action.

use std::{fs, io::Read, path::PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use zip::read::ZipArchive;

use crate::cli::UpdateCommand;
use crate::*;

/// Manifest and release-note details read from a candidate update archive.
#[derive(Debug, Clone, Serialize)]
struct UpdateArchiveSnapshot {
    archive_path: String,
    manifest_version: Option<String>,
    artifact_kind: Option<String>,
    platform: Option<String>,
    rollback_hint: Option<String>,
    migration_notes: Option<String>,
}

/// Full update plan emitted to the operator; field names are part of the
/// pinned JSON output shape.
#[derive(Debug, Clone, Serialize)]
struct UpdateReport {
    mode: String,
    install_root: String,
    current_version: Option<String>,
    current_artifact_kind: Option<String>,
    state_root: Option<String>,
    service_installed: Option<bool>,
    service_running: Option<bool>,
    rollback_hint: Option<String>,
    migration_notes: Option<String>,
    candidate: Option<UpdateArchiveSnapshot>,
    apply_supported: bool,
    next_steps: Vec<String>,
}

/// Runs `palyra update`, producing a status check, plan, or candidate plan
/// without modifying the installation.
///
/// # Errors
/// Fails when the install root cannot be resolved, the candidate archive is
/// unreadable, `--yes` requests the unimplemented in-place apply, or output
/// encoding fails.
pub(crate) fn run_update(command: UpdateCommand) -> Result<()> {
    let install_root = support::lifecycle::resolve_install_root(command.install_root)?;
    let metadata = support::lifecycle::load_install_metadata(install_root.as_path())?;
    let manifest = support::lifecycle::load_release_manifest(install_root.as_path())?;
    let rollback_hint =
        support::lifecycle::load_release_note(install_root.as_path(), "ROLLBACK.txt")?;
    let migration_notes =
        support::lifecycle::load_release_note(install_root.as_path(), "MIGRATION_NOTES.txt")?;

    let state_root =
        metadata.as_ref().and_then(|value| value.state_root.as_deref()).map(PathBuf::from);
    let service = state_root
        .as_ref()
        .and_then(|value| support::service::query_gateway_service_status(value.as_path()).ok());
    let candidate = command.archive.map(load_candidate_archive_snapshot).transpose()?;

    if command.yes && !command.dry_run {
        anyhow::bail!(
            "in-place self-update is not implemented yet; use `palyra update --archive <zip> --dry-run` to validate the candidate and follow the emitted manual steps"
        );
    }

    let report = UpdateReport {
        mode: if candidate.is_some() {
            "candidate-plan".to_owned()
        } else if command.check {
            "status-check".to_owned()
        } else {
            "plan".to_owned()
        },
        install_root: install_root.display().to_string(),
        current_version: manifest.as_ref().map(|value| value.version.clone()),
        current_artifact_kind: manifest.as_ref().map(|value| value.artifact_kind.clone()),
        state_root: state_root.as_ref().map(|value| value.display().to_string()),
        service_installed: service.as_ref().map(|value| value.installed),
        service_running: service.as_ref().map(|value| value.running),
        rollback_hint,
        migration_notes,
        candidate,
        apply_supported: false,
        next_steps: build_update_next_steps(
            manifest.as_ref(),
            service.as_ref(),
            command.skip_service_restart,
        ),
    };
    emit_update_report(&report, command.json)
}

fn load_candidate_archive_snapshot(archive_path: String) -> Result<UpdateArchiveSnapshot> {
    let archive_path_buf = PathBuf::from(archive_path.as_str());
    let file = fs::File::open(archive_path_buf.as_path())
        .with_context(|| format!("failed to open update archive {}", archive_path_buf.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to read update archive {}", archive_path_buf.display()))?;
    let manifest = read_optional_zip_json::<support::lifecycle::ReleaseManifest>(
        &mut archive,
        "release-manifest.json",
    )?;
    let rollback_hint = read_optional_zip_text(&mut archive, "ROLLBACK.txt")?;
    let migration_notes = read_optional_zip_text(&mut archive, "MIGRATION_NOTES.txt")?;
    Ok(UpdateArchiveSnapshot {
        archive_path,
        manifest_version: manifest.as_ref().map(|value| value.version.clone()),
        artifact_kind: manifest.as_ref().map(|value| value.artifact_kind.clone()),
        platform: manifest.as_ref().map(|value| value.platform.clone()),
        rollback_hint,
        migration_notes,
    })
}

fn build_update_next_steps(
    manifest: Option<&support::lifecycle::ReleaseManifest>,
    service: Option<&support::service::GatewayServiceStatus>,
    skip_service_restart: bool,
) -> Vec<String> {
    let mut steps = Vec::new();
    if !skip_service_restart && service.is_some_and(|value| value.running) {
        steps.push("Stop the gateway service before replacing installed binaries.".to_owned());
    } else if skip_service_restart {
        steps.push("Service restart handling was intentionally skipped; verify runtime health manually after replacing binaries.".to_owned());
    }
    if manifest.is_some_and(|value| value.artifact_kind == "headless") {
        steps.push("After unpacking the new portable archive, run `palyra config migrate --path <config>` before restart.".to_owned());
    } else {
        steps.push(
            "Replace the portable install directory contents while preserving the state root."
                .to_owned(),
        );
    }
    steps.push("Run `palyra doctor --json` after the update and export a support bundle if regressions remain.".to_owned());
    steps
}

fn emit_update_report(report: &UpdateReport, json: bool) -> Result<()> {
    let context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for update command"))?;
    if json || context.prefers_json() {
        return output::print_json_pretty(report, "failed to encode update output as JSON");
    }
    if context.prefers_ndjson() {
        return output::print_json_line(report, "failed to encode update output as NDJSON");
    }
    println!(
        "update mode={} install_root={} current_version={} current_artifact_kind={} service_installed={} service_running={} apply_supported={}",
        report.mode,
        report.install_root,
        report.current_version.as_deref().unwrap_or("unknown"),
        report.current_artifact_kind.as_deref().unwrap_or("unknown"),
        report.service_installed.unwrap_or(false),
        report.service_running.unwrap_or(false),
        report.apply_supported
    );
    if let Some(candidate) = report.candidate.as_ref() {
        println!(
            "update.candidate archive_path={} version={} artifact_kind={} platform={}",
            candidate.archive_path,
            candidate.manifest_version.as_deref().unwrap_or("unknown"),
            candidate.artifact_kind.as_deref().unwrap_or("unknown"),
            candidate.platform.as_deref().unwrap_or("unknown")
        );
    }
    if let Some(rollback_hint) = report.rollback_hint.as_deref() {
        println!("update.rollback_hint={}", rollback_hint.replace('\n', " | "));
    }
    if let Some(candidate) = report.candidate.as_ref() {
        if let Some(rollback_hint) = candidate.rollback_hint.as_deref() {
            println!("update.candidate.rollback_hint={}", rollback_hint.replace('\n', " | "));
        }
        if let Some(migration_notes) = candidate.migration_notes.as_deref() {
            println!("update.candidate.migration_notes={}", migration_notes.replace('\n', " | "));
        }
    }
    for step in report.next_steps.as_slice() {
        println!("update.next_step={step}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn read_optional_zip_json<T>(archive: &mut ZipArchive<fs::File>, path: &str) -> Result<Option<T>>
where
    T: for<'de> serde::Deserialize<'de>,
{
    // Manifests and release notes are optional archive members; a lookup
    // failure means the candidate simply does not ship that file.
    let Ok(mut file) = archive.by_name(path) else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {path} from update archive"))?;
    let parsed = serde_json::from_slice::<T>(bytes.as_slice())
        .with_context(|| format!("failed to parse {path} from update archive"))?;
    Ok(Some(parsed))
}

fn read_optional_zip_text(
    archive: &mut ZipArchive<fs::File>,
    path: &str,
) -> Result<Option<String>> {
    let Ok(mut file) = archive.by_name(path) else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {path} from update archive"))?;
    String::from_utf8(bytes).with_context(|| format!("{path} is not valid UTF-8")).map(Some)
}

#[cfg(test)]
mod tests {
    use super::build_update_next_steps;
    use crate::support::{lifecycle::ReleaseManifest, service::GatewayServiceStatus};

    fn manifest(artifact_kind: &str) -> ReleaseManifest {
        ReleaseManifest {
            schema_version: 1,
            generated_at_utc: "2026-03-25T00:00:00Z".to_owned(),
            artifact_kind: artifact_kind.to_owned(),
            artifact_name: "palyra-portable".to_owned(),
            version: "0.4.0".to_owned(),
            platform: "windows-x64".to_owned(),
            install_mode: Some("portable".to_owned()),
            source_sha: None,
            binaries: Vec::new(),
            packaging_boundaries: None,
        }
    }

    fn running_service() -> GatewayServiceStatus {
        GatewayServiceStatus {
            installed: true,
            running: true,
            enabled: true,
            manager: "schtasks".to_owned(),
            service_name: "PalyraGateway".to_owned(),
            definition_path: None,
            stdout_log_path: None,
            stderr_log_path: None,
            detail: None,
        }
    }

    #[test]
    fn build_update_next_steps_includes_service_stop_for_running_service() {
        let steps =
            build_update_next_steps(Some(&manifest("headless")), Some(&running_service()), false);
        assert!(
            steps.iter().any(|step| step.contains("Stop the gateway service")),
            "running services should produce an explicit stop step"
        );
        assert!(
            steps.iter().any(|step| step.contains("config migrate")),
            "headless artifacts should keep migration guidance"
        );
    }

    #[test]
    fn build_update_next_steps_honors_skip_service_restart() {
        let steps =
            build_update_next_steps(Some(&manifest("desktop")), Some(&running_service()), true);
        assert!(
            steps.iter().any(|step| step.contains("restart handling was intentionally skipped")),
            "skip_service_restart should be reflected in the plan"
        );
        assert!(
            steps.iter().any(|step| step.contains("support bundle")),
            "support bundle escalation should remain in the plan"
        );
    }
}
