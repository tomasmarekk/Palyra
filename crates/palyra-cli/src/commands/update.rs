//! Update planning for portable installs: inspects the current install and
//! an optional candidate archive, then emits manual next steps.
//!
//! In-place self-update is intentionally unimplemented; applying with
//! `--yes` fails closed so the trust chain stays a manual operator action.

use std::{
    collections::HashSet,
    env, fs,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cli::UpdateCommand;
use crate::commands::archive::{
    BoundedZipArchive, MAX_ARCHIVE_MEMBER_BYTES, MAX_UPDATE_MANIFEST_BYTES, MAX_UPDATE_TEXT_BYTES,
};
use crate::*;

const RELEASE_PUBLISHER_KEY_ENV: &str = "PALYRA_RELEASE_PUBLISHER_ED25519_PUBLIC_KEY";
const MAX_RELEASE_SIDECAR_BYTES: u64 = 64 * 1024;
const RELEASE_SIGNATURE_ALGORITHM: &str = "ed25519-sha256";
const RELEASE_SIGNATURE_CONTEXT: &str = "palyra.release-signature.v1";

/// Manifest and release-note details read from a candidate update archive.
#[derive(Debug, Clone, Serialize)]
struct UpdateArchiveSnapshot {
    archive_path: String,
    manifest_version: Option<String>,
    artifact_kind: Option<String>,
    platform: Option<String>,
    manifest_binary_hash_count: usize,
    verified_manifest_binary_hash_count: usize,
    manifest_binary_hashes_verified: bool,
    archive_sha256: String,
    sbom_present: bool,
    provenance_present: bool,
    checksum_present: bool,
    checksum_verified: bool,
    signature_present: bool,
    signature_verified: bool,
    verification_details: Vec<String>,
    rollback_hint: Option<String>,
    migration_notes: Option<String>,
}

#[derive(Debug, Clone)]
struct VerificationEvidence {
    present: bool,
    verified: bool,
    detail: String,
}

#[derive(Debug, Clone)]
struct ManifestHashEvidence {
    verified_count: usize,
    verified: bool,
    detail: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReleaseSignatureEnvelope {
    schema_version: u32,
    algorithm: String,
    artifact_file_name: String,
    artifact_sha256: String,
    publisher_key_sha256: String,
    signature_hex: String,
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
    let next_steps = build_update_next_steps(
        manifest.as_ref(),
        service.as_ref(),
        command.skip_service_restart,
        candidate.as_ref(),
    );

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
        next_steps,
    };
    emit_update_report(&report, command.json)
}

fn load_candidate_archive_snapshot(archive_path: String) -> Result<UpdateArchiveSnapshot> {
    let trusted_publisher_key = env::var(RELEASE_PUBLISHER_KEY_ENV).ok();
    load_candidate_archive_snapshot_with_key(archive_path, trusted_publisher_key.as_deref())
}

fn load_candidate_archive_snapshot_with_key(
    archive_path: String,
    trusted_publisher_key: Option<&str>,
) -> Result<UpdateArchiveSnapshot> {
    let archive_path_buf = PathBuf::from(archive_path.as_str());
    let mut archive = BoundedZipArchive::open(archive_path_buf.as_path()).with_context(|| {
        format!("failed to inspect update archive {}", archive_path_buf.display())
    })?;
    let archive_members =
        archive.member_names().iter().map(|name| name.to_ascii_lowercase()).collect::<Vec<_>>();
    let manifest = read_optional_zip_json::<support::lifecycle::ReleaseManifest>(
        &mut archive,
        "release-manifest.json",
    )?;
    let manifest_hashes = verify_manifest_binary_hashes(&mut archive, manifest.as_ref());
    let rollback_hint = read_optional_zip_text(&mut archive, "ROLLBACK.txt")?;
    let migration_notes = read_optional_zip_text(&mut archive, "MIGRATION_NOTES.txt")?;
    let manifest_binary_hash_count =
        manifest.as_ref().map(release_manifest_hash_count).unwrap_or_default();
    let archive_sha256 = archive.into_sha256()?;
    let checksum = verify_checksum_sidecar(archive_path_buf.as_path(), archive_sha256.as_str())?;
    let signature = verify_signature_sidecar(
        archive_path_buf.as_path(),
        archive_sha256.as_str(),
        trusted_publisher_key,
    )?;
    let verification_details =
        vec![manifest_hashes.detail.clone(), checksum.detail.clone(), signature.detail.clone()];
    Ok(UpdateArchiveSnapshot {
        archive_path,
        manifest_version: manifest.as_ref().map(|value| value.version.clone()),
        artifact_kind: manifest.as_ref().map(|value| value.artifact_kind.clone()),
        platform: manifest.as_ref().map(|value| value.platform.clone()),
        manifest_binary_hash_count,
        verified_manifest_binary_hash_count: manifest_hashes.verified_count,
        manifest_binary_hashes_verified: manifest_hashes.verified,
        archive_sha256,
        sbom_present: archive_members.iter().any(|name| is_sbom_member(name.as_str()))
            || archive_members.iter().any(|name| name == "sbom.json"),
        provenance_present: archive_members
            .iter()
            .any(|name| name.ends_with(".provenance.json") || name == "provenance.json"),
        checksum_present: checksum.present,
        checksum_verified: checksum.verified,
        signature_present: signature.present,
        signature_verified: signature.verified,
        verification_details,
        rollback_hint,
        migration_notes,
    })
}

fn is_sbom_member(name: &str) -> bool {
    let file_name = Path::new(name).file_name().and_then(|value| value.to_str()).unwrap_or(name);
    file_name.contains("sbom") && file_name.ends_with(".json")
}

fn verify_manifest_binary_hashes(
    archive: &mut BoundedZipArchive,
    manifest: Option<&support::lifecycle::ReleaseManifest>,
) -> ManifestHashEvidence {
    let Some(manifest) = manifest else {
        return ManifestHashEvidence {
            verified_count: 0,
            verified: false,
            detail: "release manifest is missing; no binary hashes were verified".to_owned(),
        };
    };
    if manifest.binaries.is_empty() {
        return ManifestHashEvidence {
            verified_count: 0,
            verified: false,
            detail: "release manifest contains no binary hashes".to_owned(),
        };
    }

    let mut file_names = HashSet::with_capacity(manifest.binaries.len());
    let mut logical_names = HashSet::with_capacity(manifest.binaries.len());
    let mut verified_count = 0_usize;
    for binary in manifest.binaries.as_slice() {
        let file_name = binary.file_name.trim();
        if binary.logical_name.trim().is_empty() || !is_safe_release_member_name(file_name) {
            return ManifestHashEvidence {
                verified_count,
                verified: false,
                detail: format!(
                    "manifest binary {} has an unsafe archive member name",
                    binary.logical_name
                ),
            };
        }
        if !file_names.insert(file_name.to_ascii_lowercase())
            || !logical_names.insert(binary.logical_name.trim().to_ascii_lowercase())
        {
            return ManifestHashEvidence {
                verified_count,
                verified: false,
                detail: "release manifest contains duplicate binary identities".to_owned(),
            };
        }
        if binary.size_bytes > MAX_ARCHIVE_MEMBER_BYTES {
            return ManifestHashEvidence {
                verified_count,
                verified: false,
                detail: format!("manifest binary {file_name} exceeds the verification size limit"),
            };
        }
        if binary.sha256.len() != 64 || !binary.sha256.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return ManifestHashEvidence {
                verified_count,
                verified: false,
                detail: format!("manifest binary {file_name} has an invalid SHA-256 value"),
            };
        }

        let mut hasher = Sha256::new();
        let actual_size =
            match archive.read_required_with(file_name, MAX_ARCHIVE_MEMBER_BYTES, |chunk| {
                hasher.update(chunk);
                Ok(())
            }) {
                Ok(size) => size,
                Err(error) => {
                    return ManifestHashEvidence {
                        verified_count,
                        verified: false,
                        detail: format!(
                            "manifest binary {file_name} could not be verified: {error:#}"
                        ),
                    };
                }
            };
        let actual_hash = hex::encode(hasher.finalize());
        if actual_size != binary.size_bytes
            || !actual_hash.eq_ignore_ascii_case(binary.sha256.as_str())
        {
            return ManifestHashEvidence {
                verified_count,
                verified: false,
                detail: format!("manifest binary {file_name} failed size or SHA-256 verification"),
            };
        }
        verified_count = verified_count.saturating_add(1);
    }

    ManifestHashEvidence {
        verified_count,
        verified: true,
        detail: format!("verified {verified_count} manifest binary hashes"),
    }
}

fn is_safe_release_member_name(file_name: &str) -> bool {
    !file_name.is_empty()
        && file_name != "."
        && file_name != ".."
        && !file_name.contains(['/', '\\', ':'])
        && Path::new(file_name).file_name().and_then(|value| value.to_str()) == Some(file_name)
}

fn verify_checksum_sidecar(
    archive_path: &Path,
    archive_sha256: &str,
) -> Result<VerificationEvidence> {
    let sidecar = match select_adjacent_sidecar(archive_path, ".sha256")? {
        SidecarSelection::Missing => {
            return Ok(VerificationEvidence {
                present: false,
                verified: false,
                detail: "archive checksum sidecar is missing".to_owned(),
            });
        }
        SidecarSelection::Invalid(detail) => {
            return Ok(VerificationEvidence { present: true, verified: false, detail });
        }
        SidecarSelection::Selected(path) => path,
    };
    let bytes = match read_bounded_sidecar(sidecar.as_path()) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Ok(VerificationEvidence {
                present: true,
                verified: false,
                detail: format!("checksum sidecar could not be read: {error:#}"),
            });
        }
    };
    let text = match std::str::from_utf8(bytes.as_slice()) {
        Ok(text) => text,
        Err(_) => {
            return Ok(VerificationEvidence {
                present: true,
                verified: false,
                detail: "checksum sidecar is not valid UTF-8".to_owned(),
            });
        }
    };
    let lines = text.lines().map(str::trim).filter(|line| !line.is_empty()).collect::<Vec<_>>();
    if lines.len() != 1 {
        return Ok(VerificationEvidence {
            present: true,
            verified: false,
            detail: "checksum sidecar must contain exactly one non-empty record".to_owned(),
        });
    }
    let mut fields = lines[0].split_whitespace();
    let Some(expected_hash) = fields.next() else {
        return Ok(VerificationEvidence {
            present: true,
            verified: false,
            detail: "checksum sidecar is missing the SHA-256 value".to_owned(),
        });
    };
    let Some(target_name) = fields.next().map(|value| value.trim_start_matches('*')) else {
        return Ok(VerificationEvidence {
            present: true,
            verified: false,
            detail: "checksum sidecar is not bound to an artifact file name".to_owned(),
        });
    };
    let archive_name =
        archive_path.file_name().and_then(|value| value.to_str()).unwrap_or_default();
    let valid = fields.next().is_none()
        && expected_hash.len() == 64
        && expected_hash.chars().all(|ch| ch.is_ascii_hexdigit())
        && target_name == archive_name
        && expected_hash.eq_ignore_ascii_case(archive_sha256);
    Ok(VerificationEvidence {
        present: true,
        verified: valid,
        detail: if valid {
            format!("checksum sidecar verified {archive_name}")
        } else {
            "checksum sidecar does not match the exact candidate archive".to_owned()
        },
    })
}

fn verify_signature_sidecar(
    archive_path: &Path,
    archive_sha256: &str,
    trusted_publisher_key: Option<&str>,
) -> Result<VerificationEvidence> {
    let sidecar = match select_adjacent_sidecar(archive_path, ".sig")? {
        SidecarSelection::Missing => {
            return Ok(VerificationEvidence {
                present: false,
                verified: false,
                detail: "supported release signature sidecar is missing".to_owned(),
            });
        }
        SidecarSelection::Invalid(detail) => {
            return Ok(VerificationEvidence { present: true, verified: false, detail });
        }
        SidecarSelection::Selected(path) => path,
    };
    let Some(trusted_publisher_key) =
        trusted_publisher_key.map(str::trim).filter(|value| !value.is_empty())
    else {
        return Ok(VerificationEvidence {
            present: true,
            verified: false,
            detail: format!(
                "signature is present but {RELEASE_PUBLISHER_KEY_ENV} is not configured"
            ),
        });
    };
    let bytes = match read_bounded_sidecar(sidecar.as_path()) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Ok(VerificationEvidence {
                present: true,
                verified: false,
                detail: format!("signature sidecar could not be read: {error:#}"),
            });
        }
    };
    let envelope = match serde_json::from_slice::<ReleaseSignatureEnvelope>(bytes.as_slice()) {
        Ok(envelope) => envelope,
        Err(_) => {
            return Ok(VerificationEvidence {
                present: true,
                verified: false,
                detail: "signature sidecar is not a supported JSON envelope".to_owned(),
            });
        }
    };
    let verifying_key_bytes = match parse_hex_array::<32>(trusted_publisher_key) {
        Ok(bytes) => bytes,
        Err(_) => {
            return Ok(VerificationEvidence {
                present: true,
                verified: false,
                detail: format!("{RELEASE_PUBLISHER_KEY_ENV} is not a 32-byte hex key"),
            });
        }
    };
    let verifying_key = match VerifyingKey::from_bytes(&verifying_key_bytes) {
        Ok(key) => key,
        Err(_) => {
            return Ok(VerificationEvidence {
                present: true,
                verified: false,
                detail: format!("{RELEASE_PUBLISHER_KEY_ENV} is not a valid Ed25519 key"),
            });
        }
    };
    let publisher_key_sha256 = hex::encode(Sha256::digest(verifying_key_bytes));
    let archive_name =
        archive_path.file_name().and_then(|value| value.to_str()).unwrap_or_default();
    if envelope.schema_version != 1
        || envelope.algorithm != RELEASE_SIGNATURE_ALGORITHM
        || envelope.artifact_file_name != archive_name
        || !envelope.artifact_sha256.eq_ignore_ascii_case(archive_sha256)
        || !envelope.publisher_key_sha256.eq_ignore_ascii_case(&publisher_key_sha256)
    {
        return Ok(VerificationEvidence {
            present: true,
            verified: false,
            detail: "signature envelope is not bound to the candidate and trusted publisher"
                .to_owned(),
        });
    }
    let signature_bytes = match parse_hex_array::<64>(envelope.signature_hex.as_str()) {
        Ok(bytes) => bytes,
        Err(_) => {
            return Ok(VerificationEvidence {
                present: true,
                verified: false,
                detail: "signature envelope contains an invalid Ed25519 signature".to_owned(),
            });
        }
    };
    let signature = Signature::from_bytes(&signature_bytes);
    let signed_message =
        release_signature_message(archive_name, archive_sha256, publisher_key_sha256.as_str());
    let verified = verifying_key.verify(signed_message.as_bytes(), &signature).is_ok();
    Ok(VerificationEvidence {
        present: true,
        verified,
        detail: if verified {
            format!("verified Ed25519 signature from publisher {publisher_key_sha256}")
        } else {
            "Ed25519 signature verification failed".to_owned()
        },
    })
}

fn release_signature_message(
    artifact_file_name: &str,
    artifact_sha256: &str,
    publisher_key_sha256: &str,
) -> String {
    format!(
        "{RELEASE_SIGNATURE_CONTEXT}\n{artifact_file_name}\n{artifact_sha256}\n{publisher_key_sha256}\n"
    )
}

enum SidecarSelection {
    Missing,
    Selected(PathBuf),
    Invalid(String),
}

fn select_adjacent_sidecar(archive_path: &Path, suffix: &str) -> Result<SidecarSelection> {
    let parent = archive_path.parent().unwrap_or_else(|| Path::new("."));
    let Some(file_name) = archive_path.file_name().and_then(|value| value.to_str()) else {
        return Ok(SidecarSelection::Invalid(
            "candidate archive file name is not valid UTF-8".to_owned(),
        ));
    };
    let stem = file_name.strip_suffix(".zip").unwrap_or(file_name);
    let mut candidates =
        [parent.join(format!("{file_name}{suffix}")), parent.join(format!("{stem}{suffix}"))]
            .into_iter()
            .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();

    let mut existing = Vec::new();
    for candidate in candidates {
        match fs::symlink_metadata(candidate.as_path()) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
                    return Ok(SidecarSelection::Invalid(format!(
                        "release sidecar {} is not a non-symlink regular file",
                        candidate.display()
                    )));
                }
                existing.push(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect sidecar {}", candidate.display()));
            }
        }
    }
    match existing.as_slice() {
        [] => Ok(SidecarSelection::Missing),
        [path] => Ok(SidecarSelection::Selected(path.clone())),
        _ => Ok(SidecarSelection::Invalid(
            "multiple adjacent sidecars make the release evidence ambiguous".to_owned(),
        )),
    }
}

fn read_bounded_sidecar(path: &Path) -> Result<Vec<u8>> {
    let metadata =
        fs::metadata(path).with_context(|| format!("failed to inspect {}", path.display()))?;
    if metadata.len() > MAX_RELEASE_SIDECAR_BYTES {
        anyhow::bail!(
            "sidecar {} exceeds the {} byte limit",
            path.display(),
            MAX_RELEASE_SIDECAR_BYTES
        );
    }
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut limited = file.take(MAX_RELEASE_SIDECAR_BYTES.saturating_add(1));
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    limited
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.len() as u64 > MAX_RELEASE_SIDECAR_BYTES {
        anyhow::bail!(
            "sidecar {} exceeds the {} byte limit",
            path.display(),
            MAX_RELEASE_SIDECAR_BYTES
        );
    }
    Ok(bytes)
}

fn parse_hex_array<const N: usize>(value: &str) -> Result<[u8; N]> {
    let bytes = hex::decode(value).context("invalid hex")?;
    bytes.try_into().map_err(|_| anyhow!("expected {N} bytes"))
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
    let checksum_verified =
        candidate.map(|value| value.checksum_verified).unwrap_or(hash_count > 0);
    let signature_present = candidate.map(|value| value.signature_present).unwrap_or(false);
    let signature_verified = candidate.map(|value| value.signature_verified).unwrap_or(false);
    let manifest_hashes_verified =
        candidate.map(|value| value.manifest_binary_hashes_verified).unwrap_or(hash_count > 0);

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
            hash_count > 0 && manifest_hashes_verified && checksum_verified,
            vec![
                format!("manifest_binary_hash_count={hash_count}"),
                format!("checksum_present={checksum_present}"),
                format!("manifest_binary_hashes_verified={manifest_hashes_verified}"),
                format!("checksum_verified={checksum_verified}"),
            ],
            vec!["release member hashes and the exact archive checksum must verify"],
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
            signature_verified,
            vec![
                format!("signature_present={signature_present}"),
                format!("signature_verified={signature_verified}"),
            ],
            vec!["artifact signature must verify against the configured publisher identity"],
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
    candidate: Option<&UpdateArchiveSnapshot>,
) -> Vec<String> {
    if let Some(candidate) = candidate.filter(|candidate| !candidate_is_trusted(candidate)) {
        return vec![
            format!(
                "Do not replace the installation with {}; the candidate is not cryptographically verified.",
                candidate.archive_path
            ),
            format!(
                "Provide a matching SHA-256 sidecar and a supported Ed25519 signature from the publisher configured in {RELEASE_PUBLISHER_KEY_ENV}, then inspect the candidate again."
            ),
        ];
    }

    let mut steps = Vec::new();
    if !skip_service_restart && service.is_some_and(|value| value.running) {
        steps.push("Stop the gateway service before replacing installed binaries.".to_owned());
    } else if skip_service_restart {
        steps.push("Service restart handling was intentionally skipped; verify runtime health manually after replacing binaries.".to_owned());
    }
    let artifact_kind = candidate
        .and_then(|value| value.artifact_kind.as_deref())
        .or_else(|| manifest.map(|value| value.artifact_kind.as_str()));
    if artifact_kind == Some("headless") {
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

fn candidate_is_trusted(candidate: &UpdateArchiveSnapshot) -> bool {
    candidate.manifest_version.is_some()
        && candidate.manifest_binary_hash_count > 0
        && candidate.manifest_binary_hashes_verified
        && candidate.checksum_verified
        && candidate.signature_verified
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
            "update.candidate archive_path={} version={} artifact_kind={} platform={} hashes={} verified_hashes={} sbom={} provenance={} checksum_present={} checksum_verified={} signature_present={} signature_verified={}",
            candidate.archive_path,
            candidate.manifest_version.as_deref().unwrap_or("unknown"),
            candidate.artifact_kind.as_deref().unwrap_or("unknown"),
            candidate.platform.as_deref().unwrap_or("unknown"),
            candidate.manifest_binary_hash_count,
            candidate.verified_manifest_binary_hash_count,
            candidate.sbom_present,
            candidate.provenance_present,
            candidate.checksum_present,
            candidate.checksum_verified,
            candidate.signature_present,
            candidate.signature_verified
        );
        for detail in candidate.verification_details.as_slice() {
            println!("update.candidate.verification={detail}");
        }
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

fn read_optional_zip_json<T>(archive: &mut BoundedZipArchive, path: &str) -> Result<Option<T>>
where
    T: for<'de> serde::Deserialize<'de>,
{
    // Manifests and release notes are optional archive members; a lookup
    // failure means the candidate simply does not ship that file.
    let Some(bytes) = archive.read_optional_bytes(path, MAX_UPDATE_MANIFEST_BYTES)? else {
        return Ok(None);
    };
    let parsed = serde_json::from_slice::<T>(bytes.as_slice())
        .with_context(|| format!("failed to parse {path} from update archive"))?;
    Ok(Some(parsed))
}

fn read_optional_zip_text(archive: &mut BoundedZipArchive, path: &str) -> Result<Option<String>> {
    let Some(bytes) = archive.read_optional_bytes(path, MAX_UPDATE_TEXT_BYTES)? else {
        return Ok(None);
    };
    String::from_utf8(bytes).with_context(|| format!("{path} is not valid UTF-8")).map(Some)
}

#[cfg(test)]
mod tests {
    use super::{
        build_release_readiness_scorecard, build_update_next_steps,
        load_candidate_archive_snapshot_with_key, release_signature_message,
        ReleaseSignatureEnvelope, UpdateArchiveSnapshot, RELEASE_SIGNATURE_ALGORITHM,
    };
    use crate::support::{
        lifecycle::{ReleaseManifest, ReleaseManifestBinaryEntry},
        service::GatewayServiceStatus,
    };
    use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
    use sha2::{Digest, Sha256};
    use std::{fs, io::Write, path::PathBuf};
    use tempfile::{tempdir, TempDir};
    use zip::{
        write::{SimpleFileOptions, ZipWriter},
        CompressionMethod,
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
            verified_manifest_binary_hash_count: 1,
            manifest_binary_hashes_verified: true,
            archive_sha256: "b".repeat(64),
            sbom_present,
            provenance_present: true,
            checksum_present: true,
            checksum_verified: true,
            signature_present,
            signature_verified: signature_present,
            verification_details: Vec::new(),
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

    fn signed_candidate_fixture() -> anyhow::Result<(TempDir, PathBuf, String)> {
        let temp = tempdir()?;
        let archive_path = temp.path().join("palyra-headless.zip");
        let binary = b"verified-palyra-binary";
        let mut release_manifest = manifest("headless");
        release_manifest.binaries[0].sha256 = hex::encode(Sha256::digest(binary));
        release_manifest.binaries[0].size_bytes = binary.len() as u64;

        let file = fs::File::create(archive_path.as_path())?;
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        writer.start_file("palyra.exe", options)?;
        writer.write_all(binary)?;
        writer.start_file("release-manifest.json", options)?;
        writer.write_all(serde_json::to_vec(&release_manifest)?.as_slice())?;
        writer.start_file("ROLLBACK.txt", options)?;
        writer.write_all(b"restore previous archive")?;
        writer.start_file("MIGRATION_NOTES.txt", options)?;
        writer.write_all(b"no migration required")?;
        writer.start_file("sbom.json", options)?;
        writer.write_all(b"{}")?;
        writer.start_file("provenance.json", options)?;
        writer.write_all(b"{}")?;
        writer.finish()?;

        let archive_sha256 = hex::encode(Sha256::digest(fs::read(archive_path.as_path())?));
        let archive_name =
            archive_path.file_name().and_then(|value| value.to_str()).expect("UTF-8 test name");
        fs::write(
            temp.path().join(format!("{archive_name}.sha256")),
            format!("{archive_sha256}  {archive_name}"),
        )?;

        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let verifying_key = VerifyingKey::from(&signing_key);
        let publisher_key_sha256 = hex::encode(Sha256::digest(verifying_key.as_bytes()));
        let message = release_signature_message(
            archive_name,
            archive_sha256.as_str(),
            publisher_key_sha256.as_str(),
        );
        let signature = signing_key.sign(message.as_bytes());
        let envelope = ReleaseSignatureEnvelope {
            schema_version: 1,
            algorithm: RELEASE_SIGNATURE_ALGORITHM.to_owned(),
            artifact_file_name: archive_name.to_owned(),
            artifact_sha256: archive_sha256,
            publisher_key_sha256,
            signature_hex: hex::encode(signature.to_bytes()),
        };
        fs::write(temp.path().join(format!("{archive_name}.sig")), serde_json::to_vec(&envelope)?)?;
        Ok((temp, archive_path, hex::encode(verifying_key.as_bytes())))
    }

    #[test]
    fn build_update_next_steps_includes_service_stop_for_running_service() {
        let steps = build_update_next_steps(
            Some(&manifest("headless")),
            Some(&running_service()),
            false,
            None,
        );
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
        let steps = build_update_next_steps(
            Some(&manifest("desktop")),
            Some(&running_service()),
            true,
            None,
        );
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

    #[test]
    fn signed_candidate_verifies_archive_members_checksum_and_publisher() -> anyhow::Result<()> {
        let (_temp, archive_path, publisher_key) = signed_candidate_fixture()?;

        let candidate = load_candidate_archive_snapshot_with_key(
            archive_path.display().to_string(),
            Some(publisher_key.as_str()),
        )?;
        let scorecard = build_release_readiness_scorecard(None, None, None, Some(&candidate));

        assert!(candidate.manifest_binary_hashes_verified);
        assert_eq!(candidate.verified_manifest_binary_hash_count, 1);
        assert!(candidate.checksum_verified);
        assert!(candidate.signature_verified);
        assert_eq!(scorecard.overall_state, "ready");
        Ok(())
    }

    #[test]
    fn present_but_unverified_sidecars_block_readiness_and_replacement_guidance() {
        let mut candidate = candidate(true, true);
        candidate.checksum_verified = false;
        candidate.signature_verified = false;

        let scorecard = build_release_readiness_scorecard(None, None, None, Some(&candidate));
        let steps = build_update_next_steps(
            Some(&manifest("headless")),
            Some(&running_service()),
            false,
            Some(&candidate),
        );

        assert_eq!(scorecard.overall_state, "blocked");
        assert!(scorecard
            .gates
            .iter()
            .any(|gate| { gate.gate == "artifact_hashes" && gate.state == "blocked" }));
        assert!(scorecard
            .gates
            .iter()
            .any(|gate| { gate.gate == "signed_artifact" && gate.state == "blocked" }));
        assert!(steps.iter().any(|step| step.starts_with("Do not replace")));
        assert!(steps.iter().all(|step| !step.starts_with("Stop the gateway")));
    }

    #[test]
    fn tampering_after_signing_invalidates_candidate_evidence() -> anyhow::Result<()> {
        let (_temp, archive_path, publisher_key) = signed_candidate_fixture()?;
        let mut file = fs::OpenOptions::new().append(true).open(archive_path.as_path())?;
        file.write_all(b"tampered")?;
        drop(file);

        let candidate = load_candidate_archive_snapshot_with_key(
            archive_path.display().to_string(),
            Some(publisher_key.as_str()),
        )?;

        assert!(!candidate.checksum_verified);
        assert!(!candidate.signature_verified);
        assert!(!super::candidate_is_trusted(&candidate));
        Ok(())
    }
}
