#[cfg(windows)]
use palyra_common::windows_security;
#[cfg(windows)]
use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};
use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use crate::VaultError;

#[cfg(windows)]
static WINDOWS_CURRENT_USER_SID: OnceLock<String> = OnceLock::new();
#[cfg(windows)]
static HARDENED_WINDOWS_PATHS: OnceLock<Mutex<HashSet<(PathBuf, bool)>>> = OnceLock::new();

pub(crate) fn default_vault_root(identity_store_root: &Path) -> PathBuf {
    if identity_store_root.file_name().is_some_and(|name| name == "identity") {
        if let Some(parent) = identity_store_root.parent() {
            return parent.join("vault");
        }
    }
    identity_store_root.join("vault")
}

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
    validate_no_parent_components(normalized.as_path(), "vault root path")?;
    Ok(normalized)
}

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

pub(crate) fn validate_no_parent_components(
    path: &Path,
    label: &'static str,
) -> Result<(), VaultError> {
    if path.as_os_str().is_empty() {
        return Err(VaultError::Io(format!("{label} cannot be empty")));
    }
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(VaultError::Io(format!(
            "{label} cannot contain parent directory traversal components"
        )));
    }
    Ok(())
}

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

pub fn ensure_owner_only_dir(path: &Path) -> Result<(), VaultError> {
    validate_no_parent_components(path, "owner-only directory path")?;
    fs::create_dir_all(path).map_err(|error| {
        VaultError::Io(format!("failed to create directory {}: {error}", path.display()))
    })?;
    let canonical = canonicalize_existing_dir(path, "owner-only directory path")?;
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

#[cfg(windows)]
pub(crate) fn current_user_sid() -> Result<String, VaultError> {
    if let Some(value) = WINDOWS_CURRENT_USER_SID.get() {
        return Ok(value.clone());
    }
    let resolved = current_user_sid_uncached()?;
    let _ = WINDOWS_CURRENT_USER_SID.set(resolved.clone());
    Ok(resolved)
}

#[cfg(windows)]
fn current_user_sid_uncached() -> Result<String, VaultError> {
    windows_security::current_user_sid().map_err(|error| {
        VaultError::Io(format!("failed to resolve current user SID for vault ACL: {error}"))
    })
}

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
