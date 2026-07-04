//! Update planning for portable installs: inspects the current install and
//! an optional candidate archive, then emits manual next steps.
//!
//! In-place self-update is intentionally unimplemented; applying with
//! `--yes` fails closed so the trust chain stays a manual operator action.

use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
};

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
    manifest_binary_hash_count: usize,
    sbom_present: bool,
    provenance_present: bool,
    checksum_present: bool,
    signature_present: bool,
    rollback_hint: Option<String>,
    migration_notes: Option<String>,
}

/// One release gate entry in the readiness scorecard.
#[derive(Debug, Clone, Serialize)]
struct ReleaseReadinessGate {
    gate: String,
    state: String,
    evidence: Vec<String>,
    blockers: Vec<String>,
}

/// Maturity state for one product/runtime area.
#[derive(Debug, Clone, Serialize)]
struct ReleaseReadinessArea {
    area: String,
    maturity_state: String,
    passed_scenarios: Vec<String>,
    failed_scenarios: Vec<String>,
    open_blockers: Vec<String>,
    owner_components: Vec<String>,
    rollout_recommendation: String,
}

/// Production rollout checklist carried in release artifacts.
#[derive(Debug, Clone, Serialize)]
struct ReleaseRolloutChecklist {
    config_defaults: String,
    docs: String,
    migration_notes: String,
    support_bundle: String,
    doctor_checks: String,
    rollback_plan: String,
}

/// Release readiness summary for current install or a candidate archive.
#[derive(Debug, Clone, Serialize)]
struct ReleaseReadinessScorecard {
    schema_version: u32,
    overall_state: String,
    release_target: String,
    gates: Vec<ReleaseReadinessGate>,
    areas: Vec<ReleaseReadinessArea>,
    rollout_checklist: ReleaseRolloutChecklist,
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
    release_readiness: Option<ReleaseReadinessScorecard>,
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
    let release_readiness = command.release_readiness.then(|| {
        build_release_readiness_scorecard(
            manifest.as_ref(),
            rollback_hint.as_deref(),
            migration_notes.as_deref(),
            candidate.as_ref(),
        )
    });

    if command.yes && !command.dry_run {
        anyhow::bail!(
            "in-place self-update is not implemented yet; use `palyra update --archive <zip> --dry-run` to validate the candidate and follow the emitted manual steps"
        );
    }

    let report = UpdateReport {
        mode: if command.release_readiness && candidate.is_some() {
            "candidate-release-readiness".to_owned()
        } else if candidate.is_some() {
            "candidate-plan".to_owned()
        } else if command.release_readiness {
            "release-readiness".to_owned()
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
        release_readiness,
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
    let archive_members = zip_member_names(&mut archive)?;
    let manifest = read_optional_zip_json::<support::lifecycle::ReleaseManifest>(
        &mut archive,
        "release-manifest.json",
    )?;
    let rollback_hint = read_optional_zip_text(&mut archive, "ROLLBACK.txt")?;
    let migration_notes = read_optional_zip_text(&mut archive, "MIGRATION_NOTES.txt")?;
    let manifest_binary_hash_count =
        manifest.as_ref().map(release_manifest_hash_count).unwrap_or_default();
    Ok(UpdateArchiveSnapshot {
        archive_path,
        manifest_version: manifest.as_ref().map(|value| value.version.clone()),
        artifact_kind: manifest.as_ref().map(|value| value.artifact_kind.clone()),
        platform: manifest.as_ref().map(|value| value.platform.clone()),
        manifest_binary_hash_count,
        sbom_present: archive_members.iter().any(|name| is_sbom_member(name.as_str()))
            || release_sidecar_present(
                archive_path_buf.as_path(),
                &[".sbom.json", "-sbom.json", ".cdx.json"],
            ),
        provenance_present: archive_members.iter().any(|name| name.ends_with(".provenance.json"))
            || release_sidecar_present(archive_path_buf.as_path(), &[".provenance.json"]),
        checksum_present: release_sidecar_present(
            archive_path_buf.as_path(),
            &[".sha256", ".zip.sha256", ".sha256.txt"],
        ),
        signature_present: release_sidecar_present(
            archive_path_buf.as_path(),
            &[".sig", ".minisig", ".sigstore", ".intoto.jsonl"],
        ),
        rollback_hint,
        migration_notes,
    })
}

fn zip_member_names(archive: &mut ZipArchive<fs::File>) -> Result<Vec<String>> {
    let mut names = Vec::with_capacity(archive.len());
    for index in 0..archive.len() {
        let file = archive
            .by_index(index)
            .with_context(|| format!("failed to inspect zip member at index {index}"))?;
        names.push(file.name().to_ascii_lowercase());
    }
    Ok(names)
}

fn is_sbom_member(name: &str) -> bool {
    let file_name = Path::new(name).file_name().and_then(|value| value.to_str()).unwrap_or(name);
    file_name.contains("sbom") && file_name.ends_with(".json")
}

fn release_sidecar_present(archive_path: &Path, suffixes: &[&str]) -> bool {
    let parent = archive_path.parent().unwrap_or_else(|| Path::new("."));
    let Some(file_name) = archive_path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let stem = file_name.strip_suffix(".zip").unwrap_or(file_name);
    suffixes.iter().any(|suffix| {
        parent.join(format!("{file_name}{suffix}")).is_file()
            || parent.join(format!("{stem}{suffix}")).is_file()
    })
}

fn release_manifest_hash_count(manifest: &support::lifecycle::ReleaseManifest) -> usize {
    manifest
        .binaries
        .iter()
        .filter(|binary| {
            binary.sha256.len() == 64 && binary.sha256.chars().all(|ch| ch.is_ascii_hexdigit())
        })
        .count()
}

fn build_release_readiness_scorecard(
    manifest: Option<&support::lifecycle::ReleaseManifest>,
    rollback_hint: Option<&str>,
    migration_notes: Option<&str>,
    candidate: Option<&UpdateArchiveSnapshot>,
) -> ReleaseReadinessScorecard {
    let target = candidate
        .map(|value| value.archive_path.clone())
        .or_else(|| manifest.map(|value| value.artifact_name.clone()))
        .unwrap_or_else(|| "current-install".to_owned());
    let hash_count = candidate
        .map(|value| value.manifest_binary_hash_count)
        .or_else(|| manifest.map(release_manifest_hash_count))
        .unwrap_or_default();
    let manifest_present = candidate
        .map(|value| value.manifest_version.is_some())
        .unwrap_or_else(|| manifest.is_some());
    let rollback_present =
        candidate.and_then(|value| value.rollback_hint.as_deref()).or(rollback_hint).is_some();
    let migration_present =
        candidate.and_then(|value| value.migration_notes.as_deref()).or(migration_notes).is_some();
    let sbom_present = candidate.map(|value| value.sbom_present).unwrap_or(false);
    let provenance_present = candidate
        .map(|value| value.provenance_present)
        .unwrap_or_else(|| manifest.and_then(|value| value.source_sha.as_deref()).is_some());
    let checksum_present = candidate.map(|value| value.checksum_present).unwrap_or(hash_count > 0);
    let signature_present = candidate.map(|value| value.signature_present).unwrap_or(false);

    let gates = vec![
        readiness_gate(
            "release_manifest",
            manifest_present,
            vec![if manifest_present {
                "release-manifest.json present"
            } else {
                "release-manifest.json missing"
            }],
            vec!["release manifest is required for installation provenance"],
        ),
        readiness_gate(
            "artifact_hashes",
            hash_count > 0 && checksum_present,
            vec![
                format!("manifest_binary_hash_count={hash_count}"),
                format!("checksum_present={checksum_present}"),
            ],
            vec!["release artifact hashes or checksum sidecar are missing"],
        ),
        readiness_gate(
            "sbom",
            sbom_present,
            vec![format!("sbom_present={sbom_present}")],
            vec!["SBOM evidence is required before production release"],
        ),
        readiness_gate(
            "provenance",
            provenance_present,
            vec![format!("provenance_present={provenance_present}")],
            vec!["build provenance sidecar or source_sha is required"],
        ),
        readiness_gate(
            "signed_artifact",
            signature_present,
            vec![format!("signature_present={signature_present}")],
            vec!["signed artifact or attestation sidecar is required"],
        ),
        readiness_gate(
            "rollback_plan",
            rollback_present,
            vec![format!("rollback_hint_present={rollback_present}")],
            vec!["ROLLBACK.txt or installed rollback hint is required"],
        ),
        readiness_gate(
            "migration_notes",
            migration_present,
            vec![format!("migration_notes_present={migration_present}")],
            vec!["migration notes must be explicit even when there is no migration"],
        ),
    ];
    let overall_state = if gates.iter().any(|gate| gate.state == "blocked") {
        "blocked"
    } else if gates.iter().any(|gate| gate.state == "review") {
        "review"
    } else {
        "ready"
    }
    .to_owned();

    ReleaseReadinessScorecard {
        schema_version: 1,
        overall_state: overall_state.clone(),
        release_target: target,
        gates,
        areas: release_readiness_areas(overall_state.as_str(), sbom_present, provenance_present),
        rollout_checklist: ReleaseRolloutChecklist {
            config_defaults: "confirm production defaults and keep preview-only features behind flags".to_owned(),
            docs: "publish operator docs for install, update, rollback, support bundle, and known preview surfaces".to_owned(),
            migration_notes: "ship MIGRATION_NOTES.txt; use 'no migration required' explicitly when empty".to_owned(),
            support_bundle: "run doctor and support bundle after install smoke; verify hashes are included without secrets".to_owned(),
            doctor_checks: "run palyra doctor --json plus release smoke before promotion".to_owned(),
            rollback_plan: "ship ROLLBACK.txt and validate uninstall/reinstall path before production rollout".to_owned(),
        },
    }
}

fn readiness_gate(
    gate: &str,
    passed: bool,
    evidence: Vec<impl Into<String>>,
    blockers: Vec<&str>,
) -> ReleaseReadinessGate {
    ReleaseReadinessGate {
        gate: gate.to_owned(),
        state: if passed { "passed" } else { "blocked" }.to_owned(),
        evidence: evidence.into_iter().map(Into::into).collect(),
        blockers: if passed {
            Vec::new()
        } else {
            blockers.into_iter().map(str::to_owned).collect()
        },
    }
}

fn release_readiness_areas(
    overall_state: &str,
    sbom_present: bool,
    provenance_present: bool,
) -> Vec<ReleaseReadinessArea> {
    vec![
        readiness_area(
            "api",
            "production_candidate",
            &["public contract snapshots"],
            &[],
            &[],
            &["crates/palyra-control-plane", "crates/palyra-daemon"],
            "promote only when contract snapshots stay stable",
        ),
        readiness_area(
            "qa_lab",
            "production_candidate",
            &["pr smoke", "workflow regression", "deterministic core"],
            &[],
            &[],
            &["qa", "crates/palyra-cli"],
            "keep main CI green before phase promotion",
        ),
        readiness_area(
            "execution_backends",
            "preview",
            &["worker attestation", "remote tool contract"],
            &[],
            &["trusted endpoint runtime wiring remains preview"],
            &["crates/palyra-workerd", "crates/palyra-daemon"],
            "keep networked workers behind explicit trust and health gates",
        ),
        readiness_area(
            "doctor",
            "production_candidate",
            &["doctor support bundle redaction", "update release plan"],
            &[],
            &[],
            &["crates/palyra-cli", "crates/palyra-daemon"],
            "ship doctor/support-bundle evidence with release artifacts",
        ),
        readiness_area(
            "supply_chain",
            if sbom_present && provenance_present { "production_candidate" } else { "blocked" },
            &["release manifest hashes"],
            &[],
            if sbom_present && provenance_present {
                &[]
            } else {
                &["SBOM and provenance evidence must be present"]
            },
            &["scripts/release", ".github/workflows"],
            "block production promotion until SBOM, provenance, hashes, and signatures are present",
        ),
        readiness_area(
            "rollout",
            overall_state,
            &["rollback checklist", "migration notes"],
            &[],
            if overall_state == "ready" { &[] } else { &["release gates are not all passing"] },
            &["release", "ops"],
            "promote only when scorecard overall_state is ready",
        ),
    ]
}

fn readiness_area(
    area: &str,
    maturity_state: &str,
    passed_scenarios: &[&str],
    failed_scenarios: &[&str],
    open_blockers: &[&str],
    owner_components: &[&str],
    rollout_recommendation: &str,
) -> ReleaseReadinessArea {
    ReleaseReadinessArea {
        area: area.to_owned(),
        maturity_state: maturity_state.to_owned(),
        passed_scenarios: passed_scenarios.iter().map(|value| (*value).to_owned()).collect(),
        failed_scenarios: failed_scenarios.iter().map(|value| (*value).to_owned()).collect(),
        open_blockers: open_blockers.iter().map(|value| (*value).to_owned()).collect(),
        owner_components: owner_components.iter().map(|value| (*value).to_owned()).collect(),
        rollout_recommendation: rollout_recommendation.to_owned(),
    }
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
            "update.candidate archive_path={} version={} artifact_kind={} platform={} hashes={} sbom={} provenance={} checksum={} signature={}",
            candidate.archive_path,
            candidate.manifest_version.as_deref().unwrap_or("unknown"),
            candidate.artifact_kind.as_deref().unwrap_or("unknown"),
            candidate.platform.as_deref().unwrap_or("unknown"),
            candidate.manifest_binary_hash_count,
            candidate.sbom_present,
            candidate.provenance_present,
            candidate.checksum_present,
            candidate.signature_present
        );
    }
    if let Some(scorecard) = report.release_readiness.as_ref() {
        println!(
            "update.release_readiness target={} overall_state={} gates={}",
            scorecard.release_target,
            scorecard.overall_state,
            scorecard.gates.len()
        );
        for gate in scorecard.gates.as_slice() {
            println!("update.release_readiness.gate name={} state={}", gate.gate, gate.state);
            for blocker in gate.blockers.as_slice() {
                println!("update.release_readiness.blocker gate={} {}", gate.gate, blocker);
            }
        }
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
    use super::{
        build_release_readiness_scorecard, build_update_next_steps, UpdateArchiveSnapshot,
    };
    use crate::support::{
        lifecycle::{ReleaseManifest, ReleaseManifestBinaryEntry},
        service::GatewayServiceStatus,
    };

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
            binaries: vec![ReleaseManifestBinaryEntry {
                logical_name: "palyra".to_owned(),
                file_name: "palyra.exe".to_owned(),
                sha256: "a".repeat(64),
                size_bytes: 42,
            }],
            packaging_boundaries: None,
        }
    }

    fn candidate(sbom_present: bool, signature_present: bool) -> UpdateArchiveSnapshot {
        UpdateArchiveSnapshot {
            archive_path: "dist/palyra-headless.zip".to_owned(),
            manifest_version: Some("0.4.0".to_owned()),
            artifact_kind: Some("headless".to_owned()),
            platform: Some("windows-x64".to_owned()),
            manifest_binary_hash_count: 1,
            sbom_present,
            provenance_present: true,
            checksum_present: true,
            signature_present,
            rollback_hint: Some("restore previous archive".to_owned()),
            migration_notes: Some("no migration required".to_owned()),
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

    #[test]
    fn release_readiness_scorecard_ready_when_candidate_has_release_evidence() {
        let manifest = manifest("headless");
        let candidate = candidate(true, true);

        let scorecard = build_release_readiness_scorecard(
            Some(&manifest),
            Some("rollback"),
            Some("migration"),
            Some(&candidate),
        );

        assert_eq!(scorecard.overall_state, "ready");
        assert!(
            scorecard.gates.iter().all(|gate| gate.state == "passed"),
            "complete release evidence should pass every gate"
        );
    }

    #[test]
    fn release_readiness_scorecard_blocks_missing_supply_chain_evidence() {
        let manifest = manifest("headless");
        let candidate = candidate(false, false);

        let scorecard = build_release_readiness_scorecard(
            Some(&manifest),
            Some("rollback"),
            Some("migration"),
            Some(&candidate),
        );

        assert_eq!(scorecard.overall_state, "blocked");
        assert!(
            scorecard.gates.iter().any(|gate| gate.gate == "sbom" && gate.state == "blocked"),
            "missing SBOM must block production readiness"
        );
        assert!(
            scorecard
                .areas
                .iter()
                .any(|area| area.area == "supply_chain" && area.maturity_state == "blocked"),
            "supply-chain area should remain blocked without SBOM/provenance evidence"
        );
    }
}
