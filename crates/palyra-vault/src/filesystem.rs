//! Filesystem hardening and path-boundary helpers for vault storage.
//!
//! Enforces owner-only permissions (POSIX modes on Unix, owner-SID ACLs on Windows) and rejects
//! parent-traversal/boundary-escaping paths before any vault file is touched. Every on-disk
//! artifact the crate creates goes through these helpers.

#[cfg(windows)]
use palyra_common::windows_security;
#[cfg(windows)]
use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};

use crate::VaultError;

#[cfg(windows)]
static WINDOWS_CURRENT_USER_SID: OnceLock<String> = OnceLock::new();
#[cfg(windows)]
static HARDENED_WINDOWS_PATHS: OnceLock<Mutex<HashSet<(PathBuf, bool)>>> = OnceLock::new();

/// Derives the default vault root from the identity store root.
///
/// The identity store conventionally lives at `<state_root>/identity`; in that layout the vault
/// becomes a sibling `<state_root>/vault` rather than nesting secrets under identity state.
pub(crate) fn default_vault_root(identity_store_root: &Path) -> PathBuf {
    if identity_store_root.file_name().is_some_and(|name| name == "identity") {
        if let Some(parent) = identity_store_root.parent() {
            return parent.join("vault");
        }
    }
    identity_store_root.join("vault")
}

/// Absolutizes and component-normalizes a caller-supplied vault root before it is created.
pub(crate) fn normalize_vault_root_path(raw: PathBuf) -> Result<PathBuf, VaultError> {
    if raw.as_os_str().is_empty() {
        return Err(VaultError::InvalidKey("vault root path cannot be empty".to_owned()));
    }
    let normalized = if raw.is_absolute() {
        raw
    } else {
        let current_dir = std::env::current_dir().map_err(|error| {
            VaultError::Io(format!("failed to resolve current directory for vault root: {error}"))
        })?;
        current_dir.join(raw)
    };
    normalize_path_components(normalized.as_path(), "vault root path")
}

/// Canonicalizes `path` (resolving symlinks) and verifies it is an existing directory.
pub(crate) fn canonicalize_existing_dir(
    path: &Path,
    label: &'static str,
) -> Result<PathBuf, VaultError> {
    let canonical = fs::canonicalize(path).map_err(|error| {
        VaultError::Io(format!("failed to canonicalize {label} {}: {error}", path.display()))
    })?;
    if !canonical.is_dir() {
        return Err(VaultError::Io(format!("{label} {} is not a directory", canonical.display())));
    }
    Ok(canonical)
}

/// Rejects paths containing `..` components (see [`normalize_path_components`]).
pub(crate) fn validate_no_parent_components(
    path: &Path,
    label: &'static str,
) -> Result<(), VaultError> {
    normalize_path_components(path, label).map(|_| ())
}

/// Rebuilds `path` without `.` components, rejecting any `..` outright.
///
/// `..` is rejected rather than resolved because lexically collapsing it ignores symlinks and
/// would let crafted inputs sidestep the [`ensure_path_within_root`] boundary check.
fn normalize_path_components(path: &Path, label: &'static str) -> Result<PathBuf, VaultError> {
    if path.as_os_str().is_empty() {
        return Err(VaultError::Io(format!("{label} cannot be empty")));
    }

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                return Err(VaultError::Io(format!(
                    "{label} cannot contain parent directory traversal components"
                )));
            }
            Component::CurDir => {}
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir | Component::Normal(_) => normalized.push(component.as_os_str()),
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(VaultError::Io(format!("{label} cannot resolve to the current directory")));
    }
    Ok(normalized)
}

/// Verifies `path` stays inside `root` after both are checked for traversal components.
///
/// The check is lexical (`starts_with`), so callers must pass pre-canonicalized paths for it to
/// also defeat symlink escapes — vault call sites canonicalize the root first.
pub(crate) fn ensure_path_within_root(
    root: &Path,
    path: &Path,
    label: &'static str,
) -> Result<(), VaultError> {
    validate_no_parent_components(root, "vault root path")?;
    validate_no_parent_components(path, label)?;
    if !path.starts_with(root) {
        return Err(VaultError::Io(format!("{label} escapes the vault root boundary")));
    }
    Ok(())
}

/// Creates `path` (and parents) if needed and restricts it to the current user.
///
/// Applies mode `0o700` on Unix and an owner-SID-only ACL on Windows (the Windows ACL pass is
/// cached per process; see `harden_windows_path_permissions`).
///
/// # Errors
/// Returns [`VaultError::Io`] when the path contains traversal components or creation,
/// canonicalization, or permission enforcement fails.
pub fn ensure_owner_only_dir(path: &Path) -> Result<(), VaultError> {
    let normalized = normalize_path_components(path, "owner-only directory path")?;
    fs::create_dir_all(normalized.as_path()).map_err(|error| {
        VaultError::Io(format!("failed to create directory {}: {error}", normalized.display()))
    })?;
    let canonical = canonicalize_existing_dir(normalized.as_path(), "owner-only directory path")?;
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(canonical.as_path(), owner_sid.as_str(), true)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(canonical.as_path(), fs::Permissions::from_mode(0o700)).map_err(
            |error| {
                VaultError::Io(format!(
                    "failed to enforce owner-only directory permissions on {}: {error}",
                    canonical.display()
                ))
            },
        )?;
    }
    Ok(())
}

/// Restricts an existing file to the current user (mode `0o600` on Unix, owner-SID ACL on
/// Windows).
///
/// # Errors
/// Returns [`VaultError::Io`] when the path contains traversal components, does not point at an
/// existing regular file, or permission enforcement fails.
pub fn ensure_owner_only_file(path: &Path) -> Result<(), VaultError> {
    validate_no_parent_components(path, "owner-only file path")?;
    let canonical = fs::canonicalize(path).map_err(|error| {
        VaultError::Io(format!(
            "failed to canonicalize owner-only file path {}: {error}",
            path.display()
        ))
    })?;
    if !canonical.is_file() {
        return Err(VaultError::Io(format!(
            "owner-only file path {} is not a file",
            canonical.display()
        )));
    }
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(canonical.as_path(), owner_sid.as_str(), false)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(canonical.as_path(), fs::Permissions::from_mode(0o600)).map_err(
            |error| {
                VaultError::Io(format!(
                    "failed to enforce owner-only file permissions on {}: {error}",
                    canonical.display()
                ))
            },
        )?;
    }
    Ok(())
}

/// Creates a new owner-only regular file and writes `contents` only after its
/// permissions have been hardened.
pub(crate) fn write_new_owner_only_file(path: &Path, contents: &[u8]) -> Result<(), VaultError> {
    validate_no_parent_components(path, "owner-only file path")?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|error| {
        VaultError::Io(format!("failed to create owner-only file {}: {error}", path.display()))
    })?;
    if let Err(error) = ensure_owner_only_file(path) {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(error);
    }
    if let Err(error) = file.write_all(contents) {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(VaultError::Io(format!(
            "failed to write owner-only file {}: {error}",
            path.display()
        )));
    }
    Ok(())
}

/// Returns the current user's SID, cached for the process lifetime (it cannot change mid-run).
#[cfg(windows)]
pub(crate) fn current_user_sid() -> Result<String, VaultError> {
    if let Some(value) = WINDOWS_CURRENT_USER_SID.get() {
        return Ok(value.clone());
    }
    let resolved = current_user_sid_uncached()?;
    // A racing `set` just means another thread resolved the same SID first; ignore the result.
    let _ = WINDOWS_CURRENT_USER_SID.set(resolved.clone());
    Ok(resolved)
}

#[cfg(windows)]
fn current_user_sid_uncached() -> Result<String, VaultError> {
    windows_security::current_user_sid().map_err(|error| {
        VaultError::Io(format!("failed to resolve current user SID for vault ACL: {error}"))
    })
}

/// Extracts the SID column from `whoami /user /fo csv` output (quoted, comma-separated).
///
/// Retained as the parsing reference for the `whoami` fallback path exercised by tests; only
/// test code calls it, hence the `dead_code` allow (an `expect` would warn in test builds where
/// the lint does not fire).
#[cfg(windows)]
#[allow(dead_code)]
pub(crate) fn parse_whoami_sid_csv(raw: &str) -> Option<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in raw.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.trim().to_owned());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current.trim().to_owned());
    if fields.len() < 2 {
        return None;
    }
    let sid = fields[1].trim().trim_matches('"').to_owned();
    if sid.starts_with("S-1-") {
        Some(sid)
    } else {
        None
    }
}

/// Applies an owner-SID-only ACL to `path`, deduplicated per process.
///
/// Vault operations re-touch the same few paths constantly, so paths already hardened by this
/// process are cached and the Win32 DACL rewrite is skipped on later calls. Trade-off:
/// permissions loosened out-of-band are not re-hardened until the process restarts.
#[cfg(windows)]
pub(crate) fn harden_windows_path_permissions(
    path: &Path,
    owner_sid: &str,
    is_directory: bool,
) -> Result<(), VaultError> {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let cache = HARDENED_WINDOWS_PATHS.get_or_init(|| Mutex::new(HashSet::new()));
    {
        let cache = cache
            .lock()
            .map_err(|_| VaultError::Io("vault path hardening cache poisoned".to_owned()))?;
        if cache.contains(&(canonical.clone(), is_directory)) {
            return Ok(());
        }
    }
    windows_security::harden_windows_path_permissions(path, owner_sid, is_directory).map_err(
        |error| {
            VaultError::Io(format!(
                "failed to harden Windows permissions for {}: {error}",
                path.display()
            ))
        },
    )?;
    cache
        .lock()
        .map_err(|_| VaultError::Io("vault path hardening cache poisoned".to_owned()))?
        .insert((canonical, is_directory));
    Ok(())
}
