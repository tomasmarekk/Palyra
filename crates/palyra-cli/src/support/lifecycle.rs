//! Install lifecycle metadata and CLI-exposure cleanup for installed builds.
//!
//! Reads `install-metadata.json` / `release-manifest.json` next to the binary
//! and removes shims, command roots, and managed shell-profile blocks during
//! uninstall, with guards against removing paths the installer does not own.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

// Markers delimiting the shell-profile block the installer owns; cleanup only
// ever rewrites the text between (and including) these lines.
const CLI_PROFILE_START_MARKER: &str = "# >>> Palyra CLI >>>";
const CLI_PROFILE_END_MARKER: &str = "# <<< Palyra CLI <<<";

/// How the installed CLI was exposed on PATH: shims, command root, and which
/// shell profiles or user PATH entries the installer touched.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct CliExposureMetadata {
    pub(crate) command_name: Option<String>,
    pub(crate) command_root: Option<String>,
    pub(crate) command_path: Option<String>,
    #[serde(default)]
    pub(crate) shim_paths: Vec<String>,
    pub(crate) target_binary_path: Option<String>,
    #[serde(default)]
    pub(crate) session_path_updated: bool,
    #[serde(default)]
    pub(crate) persistent_path_requested: bool,
    pub(crate) persistence_strategy: Option<String>,
    #[serde(default)]
    pub(crate) user_path_updated: bool,
    #[serde(default)]
    pub(crate) profile_files: Vec<String>,
}

/// Contents of `install-metadata.json` written by the installer; all fields
/// are optional so older installs keep parsing.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct InstallMetadata {
    pub(crate) schema_version: Option<u32>,
    pub(crate) artifact_kind: Option<String>,
    pub(crate) installed_at_utc: Option<String>,
    pub(crate) archive_path: Option<String>,
    pub(crate) install_root: Option<String>,
    pub(crate) config_path: Option<String>,
    pub(crate) state_root: Option<String>,
    pub(crate) cli_exposure: Option<CliExposureMetadata>,
}

/// One binary listed in the release manifest with its integrity hash and size.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ReleaseManifestBinaryEntry {
    pub(crate) logical_name: String,
    pub(crate) file_name: String,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
}

/// Patterns the release packaging deliberately excluded from the artifact.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ReleaseManifestPackagingBoundaries {
    #[serde(default)]
    pub(crate) excluded_patterns: Vec<String>,
}

/// Contents of `release-manifest.json` describing the installed artifact.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ReleaseManifest {
    pub(crate) schema_version: u32,
    pub(crate) generated_at_utc: String,
    pub(crate) artifact_kind: String,
    pub(crate) artifact_name: String,
    pub(crate) version: String,
    pub(crate) platform: String,
    pub(crate) install_mode: Option<String>,
    pub(crate) source_sha: Option<String>,
    #[serde(default)]
    pub(crate) binaries: Vec<ReleaseManifestBinaryEntry>,
    pub(crate) packaging_boundaries: Option<ReleaseManifestPackagingBoundaries>,
}

/// What [`remove_cli_exposure`] actually removed, plus whether the operator
/// still has to clean a persistent Windows user PATH entry manually.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CliExposureCleanupReport {
    pub(crate) removed_shim_paths: Vec<String>,
    pub(crate) removed_profile_files: Vec<String>,
    pub(crate) command_root_removed: bool,
    pub(crate) windows_path_cleanup_required: bool,
}

/// Returns the path of the currently running CLI executable.
///
/// # Errors
/// Returns an error when the OS cannot report the current executable path.
pub(crate) fn current_cli_binary_path() -> Result<PathBuf> {
    env::current_exe().context("failed to resolve current CLI executable")
}

/// Resolves the install root from an explicit path (directory, or a file's
/// parent) or from the directory containing the running binary.
///
/// # Errors
/// Returns an error when the explicit path does not exist or no parent
/// directory can be derived.
pub(crate) fn resolve_install_root(explicit: Option<String>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        let path = PathBuf::from(path);
        let resolved = if path.is_dir() {
            path
        } else if path.is_file() {
            path.parent()
                .map(Path::to_path_buf)
                .ok_or_else(|| anyhow!("install root cannot be derived from {}", path.display()))?
        } else {
            anyhow::bail!("install root does not exist: {}", path.display());
        };
        return canonicalize_lossy(resolved.as_path());
    }

    let current = current_cli_binary_path()?;
    let Some(parent) = current.parent() else {
        anyhow::bail!("install root cannot be derived from current CLI path {}", current.display());
    };
    canonicalize_lossy(parent)
}

/// Returns the `install-metadata.json` path inside an install root.
pub(crate) fn install_metadata_path(install_root: &Path) -> PathBuf {
    install_root.join("install-metadata.json")
}

/// Returns the `release-manifest.json` path inside an install root.
pub(crate) fn release_manifest_path(install_root: &Path) -> PathBuf {
    install_root.join("release-manifest.json")
}

/// Returns the path of a release note file inside an install root.
pub(crate) fn release_note_path(install_root: &Path, file_name: &str) -> PathBuf {
    install_root.join(file_name)
}

/// Loads install metadata; `Ok(None)` when no metadata file exists.
///
/// # Errors
/// Returns an error when the file exists but cannot be read or parsed.
pub(crate) fn load_install_metadata(install_root: &Path) -> Result<Option<InstallMetadata>> {
    read_json_file::<InstallMetadata>(install_metadata_path(install_root).as_path())
}

/// Loads the release manifest; `Ok(None)` when no manifest file exists.
///
/// # Errors
/// Returns an error when the file exists but cannot be read or parsed.
pub(crate) fn load_release_manifest(install_root: &Path) -> Result<Option<ReleaseManifest>> {
    read_json_file::<ReleaseManifest>(release_manifest_path(install_root).as_path())
}

/// Loads a release note; `Ok(None)` when the file does not exist.
///
/// # Errors
/// Returns an error when the file exists but cannot be read.
pub(crate) fn load_release_note(install_root: &Path, file_name: &str) -> Result<Option<String>> {
    let path = release_note_path(install_root, file_name);
    if !path.is_file() {
        return Ok(None);
    }
    fs::read_to_string(path.as_path())
        .with_context(|| format!("failed to read {}", path.display()))
        .map(Some)
}

/// Canonicalizes a path, degrading gracefully to an absolute best-effort path
/// when canonicalization fails (typically because the path no longer exists);
/// uninstall flows must still be able to reason about already-removed paths.
///
/// # Errors
/// Returns an error only when the current directory itself cannot be resolved.
pub(crate) fn canonicalize_lossy(path: &Path) -> Result<PathBuf> {
    match path.canonicalize() {
        Ok(value) => Ok(value),
        Err(_) => {
            if path.is_absolute() {
                Ok(path.to_path_buf())
            } else {
                env::current_dir()
                    .context("failed to resolve current directory")?
                    .join(path)
                    .canonicalize()
                    .or_else(|_| Ok(env::current_dir()?.join(path)))
            }
        }
    }
}

/// Rejects removal targets that resolve to a filesystem root.
///
/// # Errors
/// Returns an error when the path has no parent after canonicalization (it is
/// a root such as `/` or a drive root) and therefore must not be removed.
pub(crate) fn ensure_safe_removal_target(path: &Path, label: &str) -> Result<()> {
    let path = canonicalize_lossy(path)?;
    let mut ancestors = path.ancestors();
    // ancestors() yields the path itself first; a missing second entry means
    // the path has no parent, i.e. it is a filesystem root.
    let _self = ancestors.next();
    if ancestors.next().is_none() {
        anyhow::bail!(
            "{label} resolves to filesystem root and cannot be removed: {}",
            path.display()
        );
    }
    Ok(())
}

/// Prefix-compares paths; on Windows the comparison is separator-normalized
/// and case-insensitive to match filesystem semantics.
pub(crate) fn path_starts_with(candidate: &Path, prefix: &Path) -> bool {
    #[cfg(windows)]
    {
        let left = normalize_path_compare(candidate);
        let right = normalize_path_compare(prefix);
        left.starts_with(right.as_str())
    }
    #[cfg(not(windows))]
    {
        candidate.starts_with(prefix)
    }
}

/// Removes recorded CLI exposure: shim files, the command root directory (only
/// when empty), and managed shell-profile blocks where applicable.
///
/// # Errors
/// Returns an error when a shim, directory, or profile file that should be
/// removed or rewritten cannot be modified.
pub(crate) fn remove_cli_exposure(
    exposure: &CliExposureMetadata,
) -> Result<CliExposureCleanupReport> {
    let target_binary =
        exposure.target_binary_path.as_ref().map(|value| normalize_path_compare(Path::new(value)));
    // Older installs recorded a single command_path instead of shim_paths.
    let shim_paths = if exposure.shim_paths.is_empty() {
        exposure.command_path.clone().into_iter().collect::<Vec<_>>()
    } else {
        exposure.shim_paths.clone()
    };

    let mut removed_shim_paths = Vec::new();
    for shim in shim_paths {
        let shim_path = PathBuf::from(shim);
        if !shim_path.is_file() {
            continue;
        }
        // A shim is removed when it points at our binary. An unreadable shim
        // at a recorded path is still treated as ours: failing open here would
        // strand stale shims that shadow future installs.
        let should_remove = match target_binary.as_ref() {
            Some(target_binary) => fs::read_to_string(shim_path.as_path())
                .map(|content| content.contains(target_binary.as_str()))
                .unwrap_or(true),
            None => true,
        };
        if should_remove {
            fs::remove_file(shim_path.as_path())
                .with_context(|| format!("failed to remove CLI shim {}", shim_path.display()))?;
            removed_shim_paths.push(shim_path.display().to_string());
        }
    }

    let mut command_root_removed = false;
    let mut command_root_empty = false;
    if let Some(command_root) = exposure.command_root.as_deref() {
        let command_root = PathBuf::from(command_root);
        if command_root.is_dir() {
            command_root_empty = directory_is_empty(command_root.as_path())?;
            if command_root_empty {
                fs::remove_dir(command_root.as_path()).with_context(|| {
                    format!("failed to remove CLI command root {}", command_root.display())
                })?;
                command_root_removed = true;
            }
        } else {
            command_root_empty = true;
        }
    }

    let mut removed_profile_files = Vec::new();
    // Profile blocks are only cleaned when the command root is gone or empty
    // (another install may still rely on the PATH entry). Windows persists
    // PATH in the registry, not shell profiles, so cleanup is reported via
    // `windows_path_cleanup_required` instead of performed here.
    if exposure.persistent_path_requested && command_root_empty && !cfg!(windows) {
        for profile in exposure.profile_files.as_slice() {
            let profile_path = PathBuf::from(profile);
            if remove_profile_block(profile_path.as_path())? {
                removed_profile_files.push(profile_path.display().to_string());
            }
        }
    }

    Ok(CliExposureCleanupReport {
        removed_shim_paths,
        removed_profile_files,
        command_root_removed,
        windows_path_cleanup_required: cfg!(windows)
            && exposure.persistent_path_requested
            && exposure.user_path_updated
            && command_root_empty,
    })
}

fn read_json_file<T>(path: &Path) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.is_file() {
        return Ok(None);
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed = serde_json::from_str::<T>(raw.as_str())
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(parsed))
}

fn directory_is_empty(path: &Path) -> Result<bool> {
    let mut entries = fs::read_dir(path)
        .with_context(|| format!("failed to list directory {}", path.display()))?;
    Ok(entries.next().transpose()?.is_none())
}

// Strips the marker-delimited managed block from a shell profile; removes the
// file entirely when nothing but whitespace remains. Returns whether a block
// was found and removed.
fn remove_profile_block(path: &Path) -> Result<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    let original = fs::read_to_string(path)
        .with_context(|| format!("failed to read profile file {}", path.display()))?;
    let Some(start) = original.find(CLI_PROFILE_START_MARKER) else {
        return Ok(false);
    };
    let search_start = start + CLI_PROFILE_START_MARKER.len();
    let Some(end_relative) = original[search_start..].find(CLI_PROFILE_END_MARKER) else {
        return Ok(false);
    };
    let end = search_start + end_relative + CLI_PROFILE_END_MARKER.len();
    let mut updated = String::new();
    updated.push_str(original[..start].trim_end());
    if !updated.is_empty() && !updated.ends_with('\n') {
        updated.push('\n');
    }
    let trailing = original[end..].trim_start_matches(['\r', '\n']);
    if !trailing.is_empty() {
        updated.push_str(trailing);
    }

    if updated.trim().is_empty() {
        fs::remove_file(path)
            .with_context(|| format!("failed to remove empty profile file {}", path.display()))?;
    } else {
        fs::write(path, updated.as_bytes())
            .with_context(|| format!("failed to rewrite profile file {}", path.display()))?;
    }
    Ok(true)
}

#[cfg(windows)]
fn normalize_path_compare(path: &Path) -> String {
    path.to_string_lossy().replace('/', "\\").to_ascii_lowercase()
}

#[cfg(not(windows))]
fn normalize_path_compare(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn remove_profile_block_strips_managed_section() -> Result<()> {
        let temp = tempdir()?;
        let profile = temp.path().join(".profile");
        let body = format!(
            "export PATH=\"/usr/bin:$PATH\"\n\n{CLI_PROFILE_START_MARKER}\nmanaged\n{CLI_PROFILE_END_MARKER}\n"
        );
        fs::write(profile.as_path(), body.as_bytes())?;
        assert!(remove_profile_block(profile.as_path())?);
        let updated = fs::read_to_string(profile.as_path())?;
        assert!(!updated.contains(CLI_PROFILE_START_MARKER));
        assert!(updated.contains("export PATH"));
        Ok(())
    }

    #[test]
    fn safe_removal_rejects_root_like_targets() {
        let root = PathBuf::from(std::path::MAIN_SEPARATOR.to_string());
        assert!(ensure_safe_removal_target(root.as_path(), "test").is_err());
    }
}
