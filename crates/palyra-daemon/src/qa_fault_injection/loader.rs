//! Authenticated QA fault-plan loading and state-root confinement.
//!
//! This module consumes the one-shot launch capability and constructs the runtime.
//! Durable sidecar replay and append semantics remain owned by `persistence`.

use std::{
    fs::{self, OpenOptions},
    io::{self, Read as _},
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_common::qa_fault_injection::{
    parse_qa_fault_injection_plan_yaml, parse_qa_fault_launch_document_json,
    DeterministicQaFaultController, DeterministicQaFaultScheduler, QaFaultAction,
    QaFaultEvidenceSidecarRecord, QaFaultLaunchDocument, QaFaultProbeHandle,
    QaFaultRuleActivatedRecord, QA_FAULT_CAPABILITY_PREFIX,
    QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION, QA_FAULT_LAUNCH_SCHEMA_VERSION,
};
use palyra_vault::{ensure_owner_only_dir, ensure_owner_only_file};
use sha2::{Digest as _, Sha256};

use super::{
    occurrence_targets,
    persistence::{append_evidence_record, append_loaded_record, read_durable_evidence},
    DurableEvidenceSnapshot, QaFaultActivationError, QaFaultEvidenceState, QaFaultRuntime,
    QA_FAULT_CAPABILITY_PATH_ENV, QA_FAULT_LAUNCH_PATH_ENV,
};
use crate::gateway::constant_time_eq;

const MAX_LAUNCH_LIFETIME_MS: i64 = 5 * 60 * 1_000;
const MAX_LAUNCH_BYTES: u64 = 16 * 1_024;
const MAX_PLAN_BYTES: u64 = 256 * 1_024;
const MAX_CAPABILITY_BYTES: u64 = 128;
const QA_LAB_MODE_ENV: &str = "PALYRA_QA_LAB_MODE";
const QA_LAB_PREVIEW_MODE: &str = "preview_only";
pub(super) const INVALID_ACTIVATION_REASON_CODE: &str = "qa_fault.activation_invalid";
const INCOMPLETE_ACTIVATION_REASON_CODE: &str = "qa_fault.activation_incomplete";
const EXPIRED_ACTIVATION_REASON_CODE: &str = "qa_fault.activation_expired";

pub(crate) fn load_fault_injection(
    state_root: &Path,
) -> Result<QaFaultRuntime, QaFaultActivationError> {
    let launch_relative = env_relative_path(QA_FAULT_LAUNCH_PATH_ENV)?;
    let capability_relative = env_relative_path(QA_FAULT_CAPABILITY_PATH_ENV)?;
    let (launch_relative, capability_relative) = match (launch_relative, capability_relative) {
        (None, None) => return Ok(QaFaultRuntime::default()),
        (Some(launch_relative), Some(capability_relative)) => {
            (launch_relative, capability_relative)
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(activation_error(
                INCOMPLETE_ACTIVATION_REASON_CODE,
                format!(
                    "{QA_FAULT_LAUNCH_PATH_ENV} and {QA_FAULT_CAPABILITY_PATH_ENV} must be provided together"
                ),
            ));
        }
    };
    if std::env::var(QA_LAB_MODE_ENV).ok().as_deref() != Some(QA_LAB_PREVIEW_MODE) {
        return Err(activation_error(
            "qa_fault.preview_gate_required",
            format!("{QA_LAB_MODE_ENV} must be exactly {QA_LAB_PREVIEW_MODE} for fault injection"),
        ));
    }

    reject_link_or_reparse(state_root, "state root")?;
    ensure_owner_only_dir(state_root).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to harden QA fault state root: {error}"),
        )
    })?;
    let state_root = fs::canonicalize(state_root).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to canonicalize QA fault state root: {error}"),
        )
    })?;

    let launch_path =
        secure_existing_file(state_root.as_path(), launch_relative.as_path(), "launch document")?;
    let capability_path = secure_existing_file(
        state_root.as_path(),
        capability_relative.as_path(),
        "capability file",
    )?;
    let launch_bytes = read_bounded_file(&launch_path, MAX_LAUNCH_BYTES, "launch document")?;
    let launch = parse_qa_fault_launch_document_json(launch_bytes.as_slice()).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to parse QA fault launch document: {error}"),
        )
    })?;
    validate_launch_document(&launch)?;

    let plan_path = secure_absolute_existing_file(
        state_root.as_path(),
        Path::new(launch.plan_path.as_str()),
        "plan file",
    )?;
    let evidence_path = secure_evidence_path(
        state_root.as_path(),
        Path::new(launch.evidence_path.as_str()),
        launch.launch_id.as_str(),
    )?;

    let capability_bytes =
        read_bounded_file(&capability_path, MAX_CAPABILITY_BYTES, "capability file")?;
    let capability = parse_capability(capability_bytes.as_slice())?;
    verify_hash(&capability, launch.capability_sha256.as_str(), "capability")?;

    let plan_bytes = read_bounded_file(&plan_path, MAX_PLAN_BYTES, "plan")?;

    let plan_text = std::str::from_utf8(plan_bytes.as_slice()).map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault plan must contain UTF-8 YAML or JSON",
        )
    })?;
    let mut plan = parse_qa_fault_injection_plan_yaml(plan_text).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to parse or validate QA fault plan: {error}"),
        )
    })?;
    let canonical_plan_sha256 = plan.canonical_sha256().map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to digest QA fault plan: {error}"),
        )
    })?;
    if canonical_plan_sha256 != launch.plan_sha256 {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault canonical plan digest does not match the launch document",
        ));
    }
    let prior_evidence = read_durable_evidence(&evidence_path, &launch, &plan, true)?;
    let loaded_sequence = u32::try_from(prior_evidence.record_count + 1).map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence sequence exceeds the supported range",
        )
    })?;
    append_loaded_record(&evidence_path, &launch, loaded_sequence)?;
    let mut durable_evidence = read_durable_evidence(&evidence_path, &launch, &plan, false)?;
    if complete_joined_barrier_activations(
        evidence_path.as_path(),
        &launch,
        &plan,
        &durable_evidence,
    )? {
        durable_evidence = read_durable_evidence(&evidence_path, &launch, &plan, false)?;
    }
    fs::remove_file(&capability_path).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to consume QA fault capability file after activation: {error}"),
        )
    })?;

    let seed = plan.seed;
    let next_sequence =
        u32::try_from(durable_evidence.record_count.saturating_add(1)).map_err(|_| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                "QA fault evidence sequence exceeds the supported range",
            )
        })?;
    let barrier_participants = plan
        .activations
        .iter()
        .filter_map(|activation| match activation.action {
            QaFaultAction::Barrier { participants } => Some((activation.id.clone(), participants)),
            _ => None,
        })
        .collect();
    plan.activations.retain(|activation| {
        !durable_evidence.activated_rules.contains_key(activation.id.as_str())
    });
    let remaining_occurrence_targets = occurrence_targets(&plan);
    let probe = if plan.activations.is_empty() {
        QaFaultProbeHandle::default()
    } else {
        QaFaultProbeHandle::from_probe(
            DeterministicQaFaultController::new_resumed(
                plan,
                durable_evidence.controller_resume_state.clone(),
            )
            .map_err(|error| {
                activation_error(
                    INVALID_ACTIVATION_REASON_CODE,
                    format!("failed to initialize deterministic QA fault controller: {error}"),
                )
            })?,
        )
    };
    Ok(QaFaultRuntime::active(
        probe,
        QaFaultEvidenceState {
            launch,
            path: evidence_path,
            next_sequence,
            activated_rules: durable_evidence.activated_rules,
            activation_actors: durable_evidence.activation_actors,
            barrier_joins: durable_evidence.barrier_joins,
            barrier_join_points: durable_evidence.barrier_join_points,
            barrier_participants,
            barrier_release_orders: durable_evidence.barrier_release_orders,
            barrier_releases: durable_evidence.barrier_releases,
            observed_occurrences: durable_evidence.observed_occurrences,
            occurrence_targets: remaining_occurrence_targets,
            recovered_rule_ids: durable_evidence.recovered_rule_ids,
        },
        seed,
    ))
}

fn complete_joined_barrier_activations(
    evidence_path: &Path,
    launch: &QaFaultLaunchDocument,
    plan: &palyra_common::qa_fault_injection::QaFaultInjectionPlan,
    snapshot: &DurableEvidenceSnapshot,
) -> Result<bool, QaFaultActivationError> {
    let mut next_sequence =
        u32::try_from(snapshot.record_count.saturating_add(1)).map_err(|_| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                "QA fault evidence sequence exceeds the supported range",
            )
        })?;
    let mut next_activation_sequence =
        snapshot.highest_activation_sequence.checked_add(1).ok_or_else(|| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                "QA fault activation sequence exceeds the supported range",
            )
        })?;
    let mut appended = false;

    for activation in &plan.activations {
        let QaFaultAction::Barrier { participants } = activation.action else {
            continue;
        };
        if snapshot.activated_rules.contains_key(activation.id.as_str()) {
            continue;
        }
        let Some(actors) = snapshot.barrier_joins.get(activation.id.as_str()) else {
            continue;
        };
        if actors.len() != usize::from(participants) {
            continue;
        }
        let release_order = DeterministicQaFaultScheduler::new(plan.seed)
            .release_order(activation, actors.as_slice())
            .map_err(|error| {
                activation_error(
                    INVALID_ACTIVATION_REASON_CODE,
                    format!("failed to recover barrier activation {}: {error}", activation.id),
                )
            })?;
        let record = QaFaultEvidenceSidecarRecord::RuleActivated(QaFaultRuleActivatedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: next_sequence,
            launch_id: launch.launch_id.clone(),
            plan_sha256: launch.plan_sha256.clone(),
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actors: actors.clone(),
            occurrence: activation.occurrence,
            action: activation.action.clone(),
            activation_sequence: next_activation_sequence,
            release_order,
        });
        append_evidence_record(evidence_path, &record)?;
        appended = true;
        next_sequence = next_sequence.checked_add(1).ok_or_else(|| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                "QA fault evidence sequence exceeds the supported range",
            )
        })?;
        next_activation_sequence = next_activation_sequence.checked_add(1).ok_or_else(|| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                "QA fault activation sequence exceeds the supported range",
            )
        })?;
    }
    Ok(appended)
}

fn env_relative_path(name: &'static str) -> Result<Option<PathBuf>, QaFaultActivationError> {
    let Some(raw) = std::env::var_os(name) else {
        return Ok(None);
    };
    let raw = raw.into_string().map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("{name} must contain UTF-8 state-root-relative path data"),
        )
    })?;
    normalized_relative_path(raw.as_str(), name).map(Some)
}

fn normalized_relative_path(
    raw: &str,
    label: &'static str,
) -> Result<PathBuf, QaFaultActivationError> {
    let path = Path::new(raw);
    if raw.trim().is_empty() || path.is_absolute() {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("{label} must be a non-empty state-root-relative path"),
        ));
    }
    if path.components().any(|component| !matches!(component, Component::Normal(_))) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!(
                "{label} must not contain root, prefix, current-directory, or parent-directory components"
            ),
        ));
    }
    Ok(path.to_path_buf())
}

fn secure_existing_file(
    root: &Path,
    relative: &Path,
    label: &'static str,
) -> Result<PathBuf, QaFaultActivationError> {
    validate_secure_components(root, relative, false, label)?;
    let path = root.join(relative);
    ensure_owner_only_file(path.as_path()).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to harden QA fault {label}: {error}"),
        )
    })?;
    let canonical = fs::canonicalize(path.as_path()).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to canonicalize QA fault {label}: {error}"),
        )
    })?;
    if !canonical.starts_with(root) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("QA fault {label} escapes the state root"),
        ));
    }
    Ok(canonical)
}

fn secure_absolute_existing_file(
    root: &Path,
    path: &Path,
    label: &'static str,
) -> Result<PathBuf, QaFaultActivationError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("QA fault {label} must be confined beneath the canonical state root"),
        )
    })?;
    validate_secure_components(root, relative, false, label)?;
    ensure_owner_only_file(path).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to harden QA fault {label}: {error}"),
        )
    })?;
    let canonical = fs::canonicalize(path).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to canonicalize QA fault {label}: {error}"),
        )
    })?;
    if !canonical.starts_with(root) || canonical != path {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("QA fault {label} must already be canonical and remain inside state root"),
        ));
    }
    Ok(canonical)
}

fn secure_evidence_path(
    root: &Path,
    evidence_path: &Path,
    launch_id: &str,
) -> Result<PathBuf, QaFaultActivationError> {
    let relative = evidence_path.strip_prefix(root).map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence path must be confined beneath the canonical state root",
        )
    })?;
    let parent = relative.parent().ok_or_else(|| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "evidence_path must have an owner-only parent directory",
        )
    })?;
    validate_secure_components(root, parent, false, "evidence directory")?;
    let parent_path = root.join(parent);
    ensure_owner_only_dir(parent_path.as_path()).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to harden QA fault evidence directory: {error}"),
        )
    })?;
    if evidence_path.exists() {
        validate_secure_components(root, relative, false, "evidence file")?;
        ensure_owner_only_file(evidence_path).map_err(|error| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!("failed to harden QA fault evidence file: {error}"),
            )
        })?;
    } else {
        OpenOptions::new().write(true).create_new(true).open(evidence_path).map_err(|error| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!("failed to create QA fault evidence file: {error}"),
            )
        })?;
        ensure_owner_only_file(evidence_path).map_err(|error| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!("failed to harden new QA fault evidence file: {error}"),
            )
        })?;
    }
    let canonical = fs::canonicalize(evidence_path).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to canonicalize QA fault evidence file: {error}"),
        )
    })?;
    if !canonical.starts_with(root) || canonical != evidence_path {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence file must already be canonical and remain inside state root",
        ));
    }
    if canonical.file_name().is_none_or(|name| name.is_empty()) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("QA fault evidence path for launch {launch_id} has no file name"),
        ));
    }
    Ok(canonical)
}

fn validate_secure_components(
    root: &Path,
    relative: &Path,
    allow_missing_leaf: bool,
    label: &'static str,
) -> Result<(), QaFaultActivationError> {
    let mut candidate = root.to_path_buf();
    for (index, component) in relative.components().enumerate() {
        let Component::Normal(component) = component else {
            return Err(activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!("QA fault {label} contains an invalid path component"),
            ));
        };
        candidate.push(component);
        match fs::symlink_metadata(candidate.as_path()) {
            Ok(metadata) => reject_link_metadata(&metadata, label)?,
            Err(error)
                if allow_missing_leaf
                    && error.kind() == io::ErrorKind::NotFound
                    && index + 1 == relative.components().count() => {}
            Err(error) => {
                return Err(activation_error(
                    INVALID_ACTIVATION_REASON_CODE,
                    format!("failed to inspect QA fault {label} path: {error}"),
                ));
            }
        }
    }
    Ok(())
}

fn reject_link_or_reparse(path: &Path, label: &'static str) -> Result<(), QaFaultActivationError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to inspect QA fault {label}: {error}"),
        )
    })?;
    reject_link_metadata(&metadata, label)
}

fn reject_link_metadata(
    metadata: &fs::Metadata,
    label: &'static str,
) -> Result<(), QaFaultActivationError> {
    if metadata.file_type().is_symlink() || metadata_is_windows_reparse_point(metadata) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("QA fault {label} must not contain symlink or reparse-point components"),
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn metadata_is_windows_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn metadata_is_windows_reparse_point(_metadata: &fs::Metadata) -> bool {
    false
}

fn validate_launch_document(launch: &QaFaultLaunchDocument) -> Result<(), QaFaultActivationError> {
    if launch.schema_version != QA_FAULT_LAUNCH_SCHEMA_VERSION {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!(
                "unsupported QA fault launch schema_version {}; expected {QA_FAULT_LAUNCH_SCHEMA_VERSION}",
                launch.schema_version
            ),
        ));
    }
    let now_unix_ms = current_unix_ms()?;
    if launch.expires_at_unix_ms < now_unix_ms {
        return Err(activation_error(
            EXPIRED_ACTIVATION_REASON_CODE,
            "QA fault launch document has expired",
        ));
    }
    if launch.expires_at_unix_ms.saturating_sub(now_unix_ms) > MAX_LAUNCH_LIFETIME_MS {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault launch expiry exceeds the five-minute activation window",
        ));
    }
    Ok(())
}

fn validate_sha256(raw: &str, label: &'static str) -> Result<(), QaFaultActivationError> {
    if raw.len() != 64
        || !raw.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("{label} must be a lowercase 64-character SHA-256 digest"),
        ));
    }
    Ok(())
}

fn parse_capability(bytes: &[u8]) -> Result<Vec<u8>, QaFaultActivationError> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault capability file must contain UTF-8 capability data",
        )
    })?;
    let text = text.strip_suffix("\r\n").or_else(|| text.strip_suffix('\n')).unwrap_or(text);
    let encoded = text.strip_prefix(QA_FAULT_CAPABILITY_PREFIX).ok_or_else(|| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault capability file has an unsupported format",
        )
    })?;
    validate_sha256(encoded, "capability token")?;
    hex::decode(encoded).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to decode QA fault capability token: {error}"),
        )
    })
}

fn verify_hash(
    bytes: &[u8],
    expected_hex: &str,
    label: &'static str,
) -> Result<(), QaFaultActivationError> {
    let expected = hex::decode(expected_hex).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to decode {label} SHA-256 digest: {error}"),
        )
    })?;
    let actual = Sha256::digest(bytes);
    if constant_time_eq(actual.as_slice(), expected.as_slice()) {
        Ok(())
    } else {
        Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("QA fault {label} SHA-256 digest does not match the launch document"),
        ))
    }
}

pub(super) fn read_bounded_file(
    path: &Path,
    max_bytes: u64,
    label: &'static str,
) -> Result<Vec<u8>, QaFaultActivationError> {
    let metadata = fs::metadata(path).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to inspect QA fault {label}: {error}"),
        )
    })?;
    if metadata.len() > max_bytes {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("QA fault {label} exceeds {max_bytes} bytes"),
        ));
    }
    let file = fs::File::open(path).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to open QA fault {label}: {error}"),
        )
    })?;
    let read_limit = max_bytes.checked_add(1).unwrap_or(max_bytes);
    let mut bytes = Vec::new();
    file.take(read_limit).read_to_end(&mut bytes).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to read QA fault {label}: {error}"),
        )
    })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("QA fault {label} exceeds {max_bytes} bytes"),
        ));
    }
    Ok(bytes)
}

fn current_unix_ms() -> Result<i64, QaFaultActivationError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!("system clock is before Unix epoch: {error}"),
            )
        })
        .and_then(|duration| {
            i64::try_from(duration.as_millis()).map_err(|_| {
                activation_error(
                    INVALID_ACTIVATION_REASON_CODE,
                    "system time exceeds the supported millisecond range",
                )
            })
        })
}

pub(super) fn activation_error(
    reason_code: &'static str,
    message: impl Into<String>,
) -> QaFaultActivationError {
    QaFaultActivationError::new(reason_code, message)
}
