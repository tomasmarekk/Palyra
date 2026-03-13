use std::{
    fs,
    path::{Component, Path, PathBuf},
};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

#[cfg(windows)]
use std::process::Command;

use crate::VaultError;

#[cfg(windows)]
const WINDOWS_SYSTEM_SID: &str = "S-1-5-18";
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

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
    let output = windows_background_command("whoami")
        .args(["/user", "/fo", "csv", "/nh"])
        .output()
        .map_err(|error| {
            VaultError::Io(format!("failed to execute whoami for vault ACL: {error}"))
        })?;
    if !output.status.success() {
        return Err(VaultError::Io(format!(
            "whoami returned non-success status {} while resolving vault ACL user SID: stdout={} stderr={}",
            output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        )));
    }
    parse_whoami_sid_csv(String::from_utf8_lossy(&output.stdout).trim()).ok_or_else(|| {
        VaultError::Io("failed to parse current user SID from whoami output".to_owned())
    })
}

#[cfg(windows)]
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
    let grant_mask = if is_directory { "(OI)(CI)F" } else { "F" };
    let owner_grant = format!("*{owner_sid}:{grant_mask}");
    let system_grant = format!("*{WINDOWS_SYSTEM_SID}:{grant_mask}");
    let output = windows_background_command("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r"])
        .arg(owner_grant)
        .args(["/grant:r"])
        .arg(system_grant)
        .output()
        .map_err(|error| {
            VaultError::Io(format!("failed to execute icacls for {}: {error}", path.display()))
        })?;
    if !output.status.success() {
        return Err(VaultError::Io(format!(
            "icacls returned non-success status {} for {}: stdout={} stderr={}",
            output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
            path.display(),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        )));
    }
    Ok(())
}

#[cfg(windows)]
fn windows_background_command(program: &str) -> Command {
    let mut command = Command::new(program);
    command.creation_flags(CREATE_NO_WINDOW);
    command
}
