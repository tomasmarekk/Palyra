use std::{fs, io, path::Path};

use anyhow::{Context, Result};
use palyra_common::qa_fault_injection::QaFaultEvidenceSidecarRecord;

#[cfg(unix)]
use super::open_file_link_count;
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
use super::QA_OS_NO_FOLLOW;
use super::{
    same_open_file_identity, PinnedStateRoot, QaDaemonSandbox, QaFailureFaultRecord,
    QaFailureJournalProjection, QaFailureWorkspaceProjection, StateRootOwnership,
};

pub(super) fn project_failure_fault_record(
    sandbox: &QaDaemonSandbox,
    record: &QaFaultEvidenceSidecarRecord,
) -> QaFailureFaultRecord {
    match record {
        QaFaultEvidenceSidecarRecord::LaunchLoaded(record) => QaFailureFaultRecord {
            record_type: "launch_loaded",
            sequence: record.sequence,
            launch_id: sandbox.sanitize_diagnostic_text(record.launch_id.as_str()),
            plan_sha256: sandbox.sanitize_diagnostic_text(record.plan_sha256.as_str()),
            capability_sha256: Some(
                sandbox.sanitize_diagnostic_text(record.capability_sha256.as_str()),
            ),
            activation_id: None,
            point_id: None,
            actor: None,
            actor_count: None,
            occurrence: None,
            action: None,
            activation_sequence: None,
            release_position: None,
            recovery_class: None,
            reason_code: None,
        },
        QaFaultEvidenceSidecarRecord::CheckpointObserved(record) => QaFailureFaultRecord {
            record_type: "checkpoint_observed",
            sequence: record.sequence,
            launch_id: sandbox.sanitize_diagnostic_text(record.launch_id.as_str()),
            plan_sha256: sandbox.sanitize_diagnostic_text(record.plan_sha256.as_str()),
            capability_sha256: None,
            activation_id: None,
            point_id: Some(sandbox.sanitize_diagnostic_text(record.point_id.as_str())),
            actor: Some(sandbox.sanitize_diagnostic_text(record.actor.as_str())),
            actor_count: None,
            occurrence: Some(record.occurrence),
            action: None,
            activation_sequence: None,
            release_position: None,
            recovery_class: None,
            reason_code: None,
        },
        QaFaultEvidenceSidecarRecord::BarrierJoined(record) => QaFailureFaultRecord {
            record_type: "barrier_joined",
            sequence: record.sequence,
            launch_id: sandbox.sanitize_diagnostic_text(record.launch_id.as_str()),
            plan_sha256: sandbox.sanitize_diagnostic_text(record.plan_sha256.as_str()),
            capability_sha256: None,
            activation_id: Some(sandbox.sanitize_diagnostic_text(record.activation_id.as_str())),
            point_id: Some(sandbox.sanitize_diagnostic_text(record.point_id.as_str())),
            actor: Some(sandbox.sanitize_diagnostic_text(record.actor.as_str())),
            actor_count: None,
            occurrence: Some(record.occurrence),
            action: None,
            activation_sequence: None,
            release_position: None,
            recovery_class: None,
            reason_code: None,
        },
        QaFaultEvidenceSidecarRecord::RuleActivated(record) => QaFailureFaultRecord {
            record_type: "rule_activated",
            sequence: record.sequence,
            launch_id: sandbox.sanitize_diagnostic_text(record.launch_id.as_str()),
            plan_sha256: sandbox.sanitize_diagnostic_text(record.plan_sha256.as_str()),
            capability_sha256: None,
            activation_id: Some(sandbox.sanitize_diagnostic_text(record.activation_id.as_str())),
            point_id: Some(sandbox.sanitize_diagnostic_text(record.point_id.as_str())),
            actor: None,
            actor_count: Some(record.actors.len()),
            occurrence: Some(record.occurrence),
            action: Some(record.action.kind().as_str()),
            activation_sequence: Some(record.activation_sequence),
            release_position: None,
            recovery_class: None,
            reason_code: None,
        },
        QaFaultEvidenceSidecarRecord::BarrierReleased(record) => QaFailureFaultRecord {
            record_type: "barrier_released",
            sequence: record.sequence,
            launch_id: sandbox.sanitize_diagnostic_text(record.launch_id.as_str()),
            plan_sha256: sandbox.sanitize_diagnostic_text(record.plan_sha256.as_str()),
            capability_sha256: None,
            activation_id: Some(sandbox.sanitize_diagnostic_text(record.activation_id.as_str())),
            point_id: Some(sandbox.sanitize_diagnostic_text(record.point_id.as_str())),
            actor: Some(sandbox.sanitize_diagnostic_text(record.actor.as_str())),
            actor_count: None,
            occurrence: None,
            action: None,
            activation_sequence: None,
            release_position: Some(record.release_position),
            recovery_class: None,
            reason_code: None,
        },
        QaFaultEvidenceSidecarRecord::RecoveryRecorded(record) => QaFailureFaultRecord {
            record_type: "recovery_recorded",
            sequence: record.sequence,
            launch_id: sandbox.sanitize_diagnostic_text(record.launch_id.as_str()),
            plan_sha256: sandbox.sanitize_diagnostic_text(record.plan_sha256.as_str()),
            capability_sha256: None,
            activation_id: Some(sandbox.sanitize_diagnostic_text(record.activation_id.as_str())),
            point_id: None,
            actor: None,
            actor_count: None,
            occurrence: None,
            action: None,
            activation_sequence: None,
            release_position: None,
            recovery_class: Some(record.recovery_class.as_str()),
            reason_code: Some(sandbox.sanitize_diagnostic_text(record.reason_code.as_str())),
        },
    }
}

pub(super) fn unavailable_failure_journal(reason_code: &'static str) -> QaFailureJournalProjection {
    QaFailureJournalProjection { status: "unavailable", reason_code: Some(reason_code), run: None }
}

pub(super) fn unavailable_failure_workspace(
    reason_code: &'static str,
) -> QaFailureWorkspaceProjection {
    QaFailureWorkspaceProjection {
        status: "unavailable",
        reason_code: Some(reason_code),
        artifacts_complete: false,
        hashed_bytes: 0,
        artifacts: Vec::new(),
    }
}

pub(super) fn pin_state_root(path: &Path) -> Result<PinnedStateRoot> {
    let directory = open_directory_no_follow(path)
        .context("qa.runner.failure_diagnostics_state_root_pin_failed")?;
    let metadata =
        directory.metadata().context("qa.runner.failure_diagnostics_state_root_pin_failed")?;
    if !metadata.is_dir() || metadata_is_indirection(&metadata) {
        anyhow::bail!("qa.runner.failure_diagnostics_state_root_invalid");
    }
    Ok(PinnedStateRoot { directory })
}

impl StateRootOwnership {
    pub(super) fn verify_identity(&mut self) -> Result<()> {
        if self.startup_cleanup_delegated {
            anyhow::bail!("qa.runner.state_root_startup_cleanup_in_progress");
        }
        self.verify_path_identity()
    }

    pub(super) fn verify_path_identity(&mut self) -> Result<()> {
        if self.path_substituted {
            anyhow::bail!("qa.runner.state_root_path_substituted");
        }
        let root = self.root.as_ref().context("qa.runner.state_root_removed")?;
        let Some(pin) = self.pin.as_ref() else {
            anyhow::bail!("qa.runner.state_root_pin_missing");
        };
        let current = match open_directory_no_follow(root.path()) {
            Ok(current) => current,
            Err(error) => {
                self.path_substituted = true;
                return Err(error).context("qa.runner.state_root_identity_unavailable");
            }
        };
        let identity_matches = match same_open_file_identity(&pin.directory, &current) {
            Ok(identity_matches) => identity_matches,
            Err(error) => {
                self.path_substituted = true;
                return Err(error).context("qa.runner.state_root_identity_unavailable");
            }
        };
        if !identity_matches {
            self.path_substituted = true;
            anyhow::bail!("qa.runner.state_root_path_substituted");
        }
        Ok(())
    }

    pub(super) fn remove_verified(&mut self) -> bool {
        if self.startup_cleanup_delegated {
            return false;
        }
        self.remove_path_verified()
    }

    pub(super) fn remove_after_startup_cleanup(&mut self) -> bool {
        let removed = self.remove_path_verified();
        if removed {
            self.startup_cleanup_delegated = false;
        }
        removed
    }

    fn remove_path_verified(&mut self) -> bool {
        if self.root.is_none() {
            return self.pin.is_none();
        }
        if self.verify_path_identity().is_err() {
            return false;
        }
        let Some(root) = self.root.as_ref() else {
            return true;
        };
        let root_path = root.path().to_path_buf();
        match fs::remove_dir_all(root_path.as_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return false,
        }
        let removed = matches!(
            fs::symlink_metadata(root_path.as_path()),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        ) && self
            .pin
            .as_ref()
            .is_some_and(|pin| pinned_directory_removed(&pin.directory).unwrap_or(false));
        if !removed {
            self.path_substituted = true;
            return false;
        }
        self.pin.take();
        self.root.take();
        true
    }

    pub(super) fn is_removed(&self) -> bool {
        self.root.is_none() && self.pin.is_none()
    }
}

impl Drop for StateRootOwnership {
    fn drop(&mut self) {
        // Once ownership is shared with startup cleanup, implicit TempDir removal could race an
        // unverified process or substituted path. All deletion therefore goes through the methods
        // above; a remaining tree is retained as explicit cleanup failure evidence.
        if let Some(root) = self.root.take() {
            let _retained_state_root = root.keep();
        }
        self.pin.take();
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios"))]
pub(super) fn open_directory_no_follow(path: &Path) -> Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    fs::OpenOptions::new()
        .read(true)
        .custom_flags(QA_OS_NO_FOLLOW)
        .open(path)
        .context("qa.runner.failure_diagnostics_directory_no_follow_open_failed")
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios"))
))]
pub(super) fn open_directory_no_follow(_path: &Path) -> Result<fs::File> {
    anyhow::bail!("qa.runner.failure_diagnostics_directory_no_follow_unsupported")
}

#[cfg(windows)]
pub(super) fn open_directory_no_follow(path: &Path) -> Result<fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .context("qa.runner.failure_diagnostics_directory_no_follow_open_failed")
}

#[cfg(not(any(unix, windows)))]
pub(super) fn open_directory_no_follow(_path: &Path) -> Result<fs::File> {
    anyhow::bail!("qa.runner.failure_diagnostics_directory_no_follow_unsupported")
}

#[cfg(unix)]
pub(super) fn pinned_directory_removed(directory: &fs::File) -> Result<bool> {
    Ok(open_file_link_count(directory)? == 0)
}

#[cfg(windows)]
pub(super) fn pinned_directory_removed(directory: &fs::File) -> Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FileStandardInfo, GetFileInformationByHandleEx, FILE_STANDARD_INFO,
    };

    let mut information = FILE_STANDARD_INFO::default();
    let information_bytes = u32::try_from(std::mem::size_of_val(&information))
        .context("qa.runner.failure_diagnostics_state_root_identity_failed")?;
    // SAFETY: the pinned directory owns a live handle and `information` is the exact writable ABI
    // structure required by `FileStandardInfo` for the duration of the call.
    if unsafe {
        GetFileInformationByHandleEx(
            directory.as_raw_handle(),
            FileStandardInfo,
            std::ptr::from_mut(&mut information).cast(),
            information_bytes,
        )
    } == 0
    {
        return Err(io::Error::last_os_error())
            .context("qa.runner.failure_diagnostics_state_root_identity_failed");
    }
    Ok(information.DeletePending || information.NumberOfLinks == 0)
}

#[cfg(not(any(unix, windows)))]
pub(super) fn pinned_directory_removed(_directory: &fs::File) -> Result<bool> {
    Ok(false)
}

pub(super) fn metadata_is_indirection(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        metadata.file_attributes()
            & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
            != 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}
