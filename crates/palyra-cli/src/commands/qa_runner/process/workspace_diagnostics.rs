use super::*;

pub(super) fn load_failure_workspace_projection(
    sandbox: &QaDaemonSandbox,
) -> Result<QaFailureWorkspaceProjection> {
    load_failure_workspace_projection_with_hook(sandbox, || Ok(()))
}

pub(super) fn load_failure_workspace_projection_with_hook<Hook>(
    sandbox: &QaDaemonSandbox,
    after_initial_identity: Hook,
) -> Result<QaFailureWorkspaceProjection>
where
    Hook: FnOnce() -> Result<()>,
{
    sandbox.with_pinned_state_root_read(
        "qa.runner.failure_diagnostics_state_root_identity_invalid",
        |state_root| {
            after_initial_identity()?;
            load_failure_workspace_projection_inner(sandbox, state_root)
        },
    )
}

fn load_failure_workspace_projection_inner(
    sandbox: &QaDaemonSandbox,
    state_root: &Path,
) -> Result<QaFailureWorkspaceProjection> {
    let workspace = state_root.join("workspace");
    validate_existing_path_components(state_root, workspace.as_path())?;
    let metadata = fs::symlink_metadata(workspace.as_path())
        .context("qa.runner.failure_diagnostics_workspace_unavailable")?;
    if !metadata.is_dir() || metadata_is_indirection(&metadata) {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_invalid");
    }
    let canonical_state_root = fs::canonicalize(state_root)
        .context("qa.runner.failure_diagnostics_state_root_unavailable")?;
    let canonical_workspace = fs::canonicalize(workspace.as_path())
        .context("qa.runner.failure_diagnostics_workspace_unavailable")?;
    if !canonical_workspace.starts_with(canonical_state_root.as_path()) {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_outside_state_root");
    }
    let mut budget = FailureWorkspaceBudget { entries_seen: 0, hashed_bytes: 0, complete: true };
    let mut artifacts = Vec::new();
    hash_failure_workspace_directory(
        sandbox,
        workspace.as_path(),
        canonical_workspace.as_path(),
        workspace.as_path(),
        &mut budget,
        &mut artifacts,
        0,
    )?;
    artifacts.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(QaFailureWorkspaceProjection {
        status: "available",
        reason_code: None,
        artifacts_complete: budget.complete,
        hashed_bytes: budget.hashed_bytes,
        artifacts,
    })
}

fn hash_failure_workspace_directory(
    sandbox: &QaDaemonSandbox,
    workspace_root: &Path,
    canonical_workspace_root: &Path,
    directory: &Path,
    budget: &mut FailureWorkspaceBudget,
    artifacts: &mut Vec<QaFailureWorkspaceArtifact>,
    depth: usize,
) -> Result<()> {
    if depth > MAX_WORKSPACE_DEPTH {
        budget.complete = false;
        return Ok(());
    }
    validate_existing_path_components(workspace_root, directory)?;
    let directory_metadata = fs::symlink_metadata(directory)
        .context("qa.runner.failure_diagnostics_workspace_unavailable")?;
    if !directory_metadata.is_dir() || metadata_is_indirection(&directory_metadata) {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_indirection_denied");
    }
    let canonical_directory = fs::canonicalize(directory)
        .context("qa.runner.failure_diagnostics_workspace_unavailable")?;
    if !canonical_directory.starts_with(canonical_workspace_root) {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_outside_root");
    }
    let mut paths = Vec::new();
    for entry in
        fs::read_dir(directory).context("qa.runner.failure_diagnostics_workspace_unavailable")?
    {
        if budget.entries_seen >= MAX_WORKSPACE_ENTRIES {
            budget.complete = false;
            break;
        }
        let entry = entry.context("qa.runner.failure_diagnostics_workspace_unavailable")?;
        budget.entries_seen = budget.entries_seen.saturating_add(1);
        paths.push(entry.path());
    }
    paths.sort();
    for path in paths {
        validate_existing_path_components(workspace_root, path.as_path())?;
        let metadata = fs::symlink_metadata(path.as_path())
            .context("qa.runner.failure_diagnostics_workspace_unavailable")?;
        if metadata_is_indirection(&metadata) {
            anyhow::bail!("qa.runner.failure_diagnostics_workspace_indirection_denied");
        }
        let canonical_path = fs::canonicalize(path.as_path())
            .context("qa.runner.failure_diagnostics_workspace_unavailable")?;
        if !canonical_path.starts_with(canonical_workspace_root) {
            anyhow::bail!("qa.runner.failure_diagnostics_workspace_outside_root");
        }
        if metadata.is_dir() {
            hash_failure_workspace_directory(
                sandbox,
                workspace_root,
                canonical_workspace_root,
                path.as_path(),
                budget,
                artifacts,
                depth.saturating_add(1),
            )?;
            continue;
        }
        if !metadata.is_file() {
            budget.complete = false;
            continue;
        }
        if artifacts.len() >= MAX_FAILURE_WORKSPACE_ARTIFACTS {
            budget.complete = false;
            continue;
        }
        let (mut file, opened_metadata) =
            open_contained_failure_workspace_file(path.as_path(), canonical_workspace_root)?;
        let Some(next_hashed_bytes) = budget.hashed_bytes.checked_add(opened_metadata.len()) else {
            budget.complete = false;
            continue;
        };
        if next_hashed_bytes > MAX_FAILURE_WORKSPACE_BYTES {
            budget.complete = false;
            continue;
        }
        let relative = path
            .strip_prefix(workspace_root)
            .context("qa.runner.failure_diagnostics_workspace_path_invalid")?;
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            budget.complete = false;
            continue;
        }
        let relative_path = relative
            .iter()
            .map(|component| component.to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        let sha256 = hash_failure_workspace_file(&mut file, &opened_metadata)?;
        artifacts.push(QaFailureWorkspaceArtifact {
            path: sandbox.sanitize_diagnostic_text(relative_path.as_str()),
            sha256,
            size_bytes: opened_metadata.len(),
        });
        budget.hashed_bytes = next_hashed_bytes;
    }
    Ok(())
}

fn open_contained_failure_workspace_file(
    path: &Path,
    canonical_workspace_root: &Path,
) -> Result<(fs::File, fs::Metadata)> {
    let canonical_before =
        fs::canonicalize(path).context("qa.runner.failure_diagnostics_workspace_unavailable")?;
    if !canonical_before.starts_with(canonical_workspace_root) {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_outside_root");
    }
    let file = open_failure_workspace_file_no_follow(path)?;
    let opened_metadata =
        file.metadata().context("qa.runner.failure_diagnostics_workspace_hash_failed")?;
    if !opened_metadata.is_file() || metadata_is_indirection(&opened_metadata) {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_indirection_denied");
    }
    if open_file_link_count(&file)? != 1 {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_hard_link_denied");
    }
    let canonical_after =
        fs::canonicalize(path).context("qa.runner.failure_diagnostics_workspace_unavailable")?;
    if canonical_after != canonical_before || !canonical_after.starts_with(canonical_workspace_root)
    {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_changed");
    }
    let comparison_file = open_failure_workspace_file_no_follow(canonical_after.as_path())?;
    let comparison_metadata = comparison_file
        .metadata()
        .context("qa.runner.failure_diagnostics_workspace_unavailable")?;
    if !same_open_file_identity(&file, &comparison_file)?
        || opened_metadata.len() != comparison_metadata.len()
        || open_file_link_count(&comparison_file)? != 1
    {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_changed");
    }
    Ok((file, opened_metadata))
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios"))]
pub(super) fn open_failure_workspace_file_no_follow(path: &Path) -> Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    fs::OpenOptions::new()
        .read(true)
        .custom_flags(QA_OS_NO_FOLLOW)
        .open(path)
        .context("qa.runner.failure_diagnostics_workspace_no_follow_open_failed")
}

// These are stable OS ABI values consumed by `OpenOptionsExt`; other Unix targets
// fail closed below instead of silently opening through an unknown link contract.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) const QA_OS_NO_FOLLOW: i32 = 0x0002_0000;

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub(super) const QA_OS_NO_FOLLOW: i32 = 0x0000_0100;

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios"))
))]
pub(super) fn open_failure_workspace_file_no_follow(_path: &Path) -> Result<fs::File> {
    anyhow::bail!("qa.runner.failure_diagnostics_workspace_no_follow_unsupported")
}

#[cfg(windows)]
pub(super) fn open_failure_workspace_file_no_follow(path: &Path) -> Result<fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .context("qa.runner.failure_diagnostics_workspace_no_follow_open_failed")
}

#[cfg(not(any(unix, windows)))]
pub(super) fn open_failure_workspace_file_no_follow(_path: &Path) -> Result<fs::File> {
    anyhow::bail!("qa.runner.failure_diagnostics_workspace_no_follow_unsupported")
}

#[cfg(unix)]
pub(super) fn same_open_file_identity(left: &fs::File, right: &fs::File) -> Result<bool> {
    Ok(open_file_identity(left)? == open_file_identity(right)?)
}

#[cfg(windows)]
pub(super) fn same_open_file_identity(left: &fs::File, right: &fs::File) -> Result<bool> {
    Ok(open_file_identity(left)? == open_file_identity(right)?)
}

#[cfg(not(any(unix, windows)))]
pub(super) fn same_open_file_identity(_left: &fs::File, _right: &fs::File) -> Result<bool> {
    Ok(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OpenFileIdentity {
    volume: u64,
    pub(super) file: u64,
}

#[cfg(unix)]
pub(super) fn open_file_identity(file: &fs::File) -> Result<OpenFileIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata().context("qa.runner.failure_diagnostics_file_identity_failed")?;
    Ok(OpenFileIdentity { volume: metadata.dev(), file: metadata.ino() })
}

#[cfg(windows)]
pub(super) fn open_file_identity(file: &fs::File) -> Result<OpenFileIdentity> {
    let information = windows_file_information(file)?;
    Ok(OpenFileIdentity {
        volume: u64::from(information.dwVolumeSerialNumber),
        file: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
    })
}

#[cfg(not(any(unix, windows)))]
pub(super) fn open_file_identity(_file: &fs::File) -> Result<OpenFileIdentity> {
    anyhow::bail!("qa.runner.failure_diagnostics_file_identity_unsupported")
}

#[cfg(unix)]
pub(super) fn open_file_link_count(file: &fs::File) -> Result<u64> {
    use std::os::unix::fs::MetadataExt;

    file.metadata()
        .map(|metadata| metadata.nlink())
        .context("qa.runner.failure_diagnostics_file_identity_failed")
}

#[cfg(windows)]
pub(super) fn open_file_link_count(file: &fs::File) -> Result<u64> {
    windows_file_information(file).map(|information| u64::from(information.nNumberOfLinks))
}

#[cfg(not(any(unix, windows)))]
pub(super) fn open_file_link_count(_file: &fs::File) -> Result<u64> {
    anyhow::bail!("qa.runner.failure_diagnostics_file_identity_unsupported")
}

#[cfg(windows)]
fn windows_file_information(
    file: &fs::File,
) -> Result<windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };

    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: `file` owns a live kernel handle and `information` points to a writable
    // structure of the exact ABI type required for the duration of the call.
    let succeeded = unsafe {
        GetFileInformationByHandle(file.as_raw_handle(), std::ptr::from_mut(&mut information))
    };
    if succeeded == 0 {
        return Err(io::Error::last_os_error())
            .context("qa.runner.failure_diagnostics_workspace_identity_failed");
    }
    Ok(information)
}

fn hash_failure_workspace_file(
    file: &mut fs::File,
    opened_metadata: &fs::Metadata,
) -> Result<String> {
    let expected_bytes = opened_metadata.len();
    let mut hasher = Sha256::new();
    let mut observed_bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .context("qa.runner.failure_diagnostics_workspace_hash_failed")?;
        if read == 0 {
            break;
        }
        observed_bytes = observed_bytes
            .checked_add(
                u64::try_from(read)
                    .context("qa.runner.failure_diagnostics_workspace_hash_failed")?,
            )
            .context("qa.runner.failure_diagnostics_workspace_hash_failed")?;
        if observed_bytes > expected_bytes {
            anyhow::bail!("qa.runner.failure_diagnostics_workspace_changed");
        }
        hasher.update(&buffer[..read]);
    }
    if observed_bytes != expected_bytes {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_changed");
    }
    let final_metadata =
        file.metadata().context("qa.runner.failure_diagnostics_workspace_hash_failed")?;
    if final_metadata.len() != expected_bytes {
        anyhow::bail!("qa.runner.failure_diagnostics_workspace_changed");
    }
    Ok(digest_to_hex(hasher.finalize().as_slice()))
}

pub(super) fn contains_absolute_path_marker(text: &str) -> bool {
    let bytes = text.as_bytes();
    for index in 0..bytes.len() {
        if index.saturating_add(2) < bytes.len()
            && bytes[index].is_ascii_alphabetic()
            && bytes[index + 1] == b':'
            && matches!(bytes[index + 2], b'/' | b'\\')
        {
            return true;
        }
        if matches!(bytes[index], b'/' | b'\\')
            && bytes.get(index.saturating_add(1)).is_some_and(|next| *next == bytes[index])
        {
            return true;
        }
    }
    let mut previous = None;
    for character in text.chars() {
        if matches!(character, '/' | '\\')
            && previous.is_none_or(is_absolute_path_leading_delimiter)
        {
            return true;
        }
        previous = Some(character);
    }
    false
}

fn is_absolute_path_leading_delimiter(character: char) -> bool {
    character.is_whitespace() || (!character.is_alphanumeric() && character != '_')
}
