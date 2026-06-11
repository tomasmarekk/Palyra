//! Default config, identity-store, and state-root path resolution per platform.
//!
//! Windows resolves through APPDATA/LOCALAPPDATA/PROGRAMDATA; other platforms follow the
//! XDG base-directory spec with HOME fallbacks. `parse_config_path` validates
//! operator-supplied config paths and is exercised by
//! `fuzz/fuzz_targets/config_path_parser.rs`.

use std::{
    env,
    ffi::OsString,
    path::{Component, PathBuf},
};

use thiserror::Error;

/// Why an operator-supplied config path was rejected.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigPathParseError {
    #[error("config path cannot be empty")]
    Empty,
    #[error("config path contains an embedded NUL byte")]
    EmbeddedNul,
    #[error("config path cannot contain parent directory traversal ('..')")]
    ParentTraversal,
}

/// The environment lacks the variables needed to derive a default state path.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum IdentityStorePathError {
    #[cfg(not(windows))]
    #[error("HOME is not set")]
    HomeNotSet,
    #[cfg(windows)]
    #[error("LOCALAPPDATA and APPDATA are both unset")]
    AppDataNotSet,
}

/// Validates and parses an operator-supplied config path.
///
/// Parent-directory traversal is rejected so a config path passed via env/CLI cannot
/// escape the directory the operator believes it points into.
///
/// # Errors
/// Returns a [`ConfigPathParseError`] for empty input, embedded NUL bytes, or `..`
/// components.
pub fn parse_config_path(raw: &str) -> Result<PathBuf, ConfigPathParseError> {
    if raw.trim().is_empty() {
        return Err(ConfigPathParseError::Empty);
    }
    if raw.contains('\0') {
        return Err(ConfigPathParseError::EmbeddedNul);
    }

    let path = PathBuf::from(raw);
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(ConfigPathParseError::ParentTraversal);
    }

    Ok(path)
}

/// Returns the ordered candidate locations of `palyra.toml` (user config before system).
#[must_use]
pub fn default_config_search_paths() -> Vec<PathBuf> {
    #[cfg(windows)]
    {
        default_config_search_paths_from_env(env::var_os("APPDATA"), env::var_os("PROGRAMDATA"))
    }
    #[cfg(not(windows))]
    {
        default_config_search_paths_from_env(env::var_os("XDG_CONFIG_HOME"), env::var_os("HOME"))
    }
}

/// Returns the default identity-store directory derived from the process environment.
///
/// # Errors
/// Returns [`IdentityStorePathError`] when the platform base-directory variables are unset.
pub fn default_identity_store_root() -> Result<PathBuf, IdentityStorePathError> {
    #[cfg(windows)]
    {
        default_identity_store_root_from_env(env::var_os("LOCALAPPDATA"), env::var_os("APPDATA"))
    }
    #[cfg(not(windows))]
    {
        default_identity_store_root_from_env(env::var_os("XDG_STATE_HOME"), env::var_os("HOME"))
    }
}

/// Returns the default state root (the parent of the identity store).
///
/// # Errors
/// Returns [`IdentityStorePathError`] when the platform base-directory variables are unset.
pub fn default_state_root() -> Result<PathBuf, IdentityStorePathError> {
    #[cfg(windows)]
    {
        default_state_root_from_env(env::var_os("LOCALAPPDATA"), env::var_os("APPDATA"))
    }
    #[cfg(not(windows))]
    {
        default_state_root_from_env(env::var_os("XDG_STATE_HOME"), env::var_os("HOME"))
    }
}

/// Injectable-environment variant of [`default_identity_store_root`] for tests.
///
/// # Errors
/// Returns [`IdentityStorePathError::AppDataNotSet`] when both inputs are `None`.
#[cfg(windows)]
pub fn default_identity_store_root_from_env(
    local_appdata: Option<OsString>,
    appdata: Option<OsString>,
) -> Result<PathBuf, IdentityStorePathError> {
    if let Some(local_appdata) = local_appdata {
        return Ok(PathBuf::from(local_appdata).join("Palyra").join("identity"));
    }
    if let Some(appdata) = appdata {
        return Ok(PathBuf::from(appdata).join("Palyra").join("identity"));
    }
    Err(IdentityStorePathError::AppDataNotSet)
}

/// Injectable-environment variant of [`default_state_root`] for tests.
///
/// # Errors
/// Returns [`IdentityStorePathError::AppDataNotSet`] when both inputs are `None`.
#[cfg(windows)]
pub fn default_state_root_from_env(
    local_appdata: Option<OsString>,
    appdata: Option<OsString>,
) -> Result<PathBuf, IdentityStorePathError> {
    let identity_root = default_identity_store_root_from_env(local_appdata, appdata)?;
    Ok(identity_root.parent().map(PathBuf::from).unwrap_or(identity_root))
}

/// Injectable-environment variant of [`default_identity_store_root`] for tests.
///
/// # Errors
/// Returns [`IdentityStorePathError::HomeNotSet`] when both inputs are `None`.
#[cfg(not(windows))]
pub fn default_identity_store_root_from_env(
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf, IdentityStorePathError> {
    if let Some(xdg_state_home) = xdg_state_home {
        return Ok(PathBuf::from(xdg_state_home).join("palyra").join("identity"));
    }
    let home = home.map(PathBuf::from).ok_or(IdentityStorePathError::HomeNotSet)?;
    Ok(home.join(".local").join("state").join("palyra").join("identity"))
}

/// Injectable-environment variant of [`default_state_root`] for tests.
///
/// # Errors
/// Returns [`IdentityStorePathError::HomeNotSet`] when both inputs are `None`.
#[cfg(not(windows))]
pub fn default_state_root_from_env(
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf, IdentityStorePathError> {
    let identity_root = default_identity_store_root_from_env(xdg_state_home, home)?;
    Ok(identity_root.parent().map(PathBuf::from).unwrap_or(identity_root))
}

#[cfg(windows)]
pub(crate) fn default_config_search_paths_from_env(
    appdata: Option<OsString>,
    programdata: Option<OsString>,
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(appdata) = appdata {
        paths.push(PathBuf::from(appdata).join("Palyra").join("palyra.toml"));
    }
    if let Some(programdata) = programdata {
        paths.push(PathBuf::from(programdata).join("Palyra").join("palyra.toml"));
    }
    paths
}

#[cfg(not(windows))]
pub(crate) fn default_config_search_paths_from_env(
    xdg_config_home: Option<OsString>,
    home: Option<OsString>,
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(xdg_config_home) = xdg_config_home {
        paths.push(PathBuf::from(xdg_config_home).join("palyra").join("palyra.toml"));
    } else if let Some(home) = home {
        paths.push(PathBuf::from(home).join(".config").join("palyra").join("palyra.toml"));
    }
    paths.push(PathBuf::from("/etc/palyra/palyra.toml"));
    paths
}
