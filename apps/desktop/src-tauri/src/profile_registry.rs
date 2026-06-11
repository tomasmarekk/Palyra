//! Desktop profile registry loader.
//!
//! Profiles are read from the CLI registry when present and supplemented with
//! an implicit local desktop profile so the control center can start cleanly on
//! first run.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use palyra_common::parse_config_path;
use palyra_control_plane as control_plane;
use serde::Deserialize;

use crate::desktop_state::IMPLICIT_DESKTOP_PROFILE_NAME;

const CLI_PROFILES_PATH_ENV: &str = "PALYRA_CLI_PROFILES_PATH";
const CLI_PROFILES_RELATIVE_PATH: &str = "cli/profiles.toml";
const CLI_PROFILE_SCHEMA_VERSION: u32 = 1;
const PALYRA_CONFIG_ENV: &str = "PALYRA_CONFIG";

/// Resolved desktop connection profile with optional config and state roots.
#[derive(Debug, Clone)]
pub(crate) struct DesktopResolvedProfile {
    pub(crate) context: control_plane::ConsoleProfileContext,
    pub(crate) config_path: Option<PathBuf>,
    pub(crate) state_root: Option<PathBuf>,
    pub(crate) last_used_at_unix_ms: Option<i64>,
    pub(crate) implicit: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DesktopCliProfilesDocument {
    version: Option<u32>,
    default_profile: Option<String>,
    #[serde(default)]
    profiles: BTreeMap<String, DesktopCliConnectionProfile>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct DesktopCliConnectionProfile {
    config_path: Option<String>,
    state_root: Option<String>,
    daemon_url: Option<String>,
    label: Option<String>,
    environment: Option<String>,
    color: Option<String>,
    risk_level: Option<String>,
    #[serde(default)]
    strict_mode: bool,
    mode: Option<String>,
    last_used_at_unix_ms: Option<i64>,
}

/// Catalog of resolved desktop profiles keyed by profile name.
#[derive(Debug, Clone)]
pub(crate) struct DesktopProfileCatalog {
    pub(crate) default_profile_name: Option<String>,
    pub(crate) profiles: BTreeMap<String, DesktopResolvedProfile>,
}

impl DesktopProfileCatalog {
    /// Loads the CLI profile registry and inserts the implicit desktop profile.
    ///
    /// # Errors
    /// Returns an error when the profile registry path, document, schema
    /// version, or configured paths are invalid.
    pub(crate) fn load(base_state_root: &Path) -> Result<Self> {
        let registry_path = resolve_cli_profiles_registry_path(base_state_root)?;
        let document = load_profiles_document(registry_path.as_deref())?;
        let mut profiles = BTreeMap::new();
        for (name, profile) in document.profiles {
            profiles.insert(name.clone(), resolve_profile(name.as_str(), profile)?);
        }
        if !profiles.contains_key(IMPLICIT_DESKTOP_PROFILE_NAME) {
            profiles.insert(
                IMPLICIT_DESKTOP_PROFILE_NAME.to_owned(),
                implicit_profile_from_environment(IMPLICIT_DESKTOP_PROFILE_NAME)?,
            );
        }
        Ok(Self {
            default_profile_name: normalize_optional_text(document.default_profile.as_deref()),
            profiles,
        })
    }
}

fn resolve_cli_profiles_registry_path(base_state_root: &Path) -> Result<Option<PathBuf>> {
    if let Ok(raw) = env::var(CLI_PROFILES_PATH_ENV) {
        let Some(path) = normalize_optional_text(Some(raw.as_str())) else {
            return Ok(None);
        };
        return parse_config_path(path.as_str())
            .with_context(|| format!("{CLI_PROFILES_PATH_ENV} contains an invalid path"))
            .map(Some);
    }

    let default_path = base_state_root.join(CLI_PROFILES_RELATIVE_PATH);
    if default_path.exists() {
        Ok(Some(default_path))
    } else {
        Ok(None)
    }
}

fn load_profiles_document(path: Option<&Path>) -> Result<DesktopCliProfilesDocument> {
    let Some(path) = path else {
        return Ok(DesktopCliProfilesDocument::default());
    };
    if !path.exists() {
        return Ok(DesktopCliProfilesDocument::default());
    }
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read CLI profiles {}", path.display()))?;
    let document: DesktopCliProfilesDocument = toml::from_str(content.as_str())
        .with_context(|| format!("failed to parse CLI profiles {}", path.display()))?;
    if let Some(version) = document.version {
        if version != CLI_PROFILE_SCHEMA_VERSION {
            return Err(anyhow!(
                "unsupported CLI profile schema version {version}; expected {CLI_PROFILE_SCHEMA_VERSION}"
            ));
        }
    }
    Ok(document)
}

fn resolve_profile(
    name: &str,
    profile: DesktopCliConnectionProfile,
) -> Result<DesktopResolvedProfile> {
    let mode = normalize_optional_text(profile.mode.as_deref()).unwrap_or_else(|| {
        if profile
            .daemon_url
            .as_deref()
            .is_some_and(|value| !value.contains("127.0.0.1") && !value.contains("localhost"))
        {
            "remote".to_owned()
        } else {
            "local".to_owned()
        }
    });
    let strict_mode = profile.strict_mode;
    let environment =
        normalize_optional_text(profile.environment.as_deref()).unwrap_or_else(|| {
            if strict_mode || mode.eq_ignore_ascii_case("remote") {
                "production".to_owned()
            } else {
                "local".to_owned()
            }
        });
    let risk_level = normalize_optional_text(profile.risk_level.as_deref()).unwrap_or_else(|| {
        if strict_mode {
            "high".to_owned()
        } else if mode.eq_ignore_ascii_case("remote") {
            "elevated".to_owned()
        } else {
            "low".to_owned()
        }
    });
    let color =
        normalize_optional_text(profile.color.as_deref()).unwrap_or_else(|| {
            match risk_level.as_str() {
                "critical" | "high" => "red".to_owned(),
                "elevated" | "medium" => "amber".to_owned(),
                _ => "emerald".to_owned(),
            }
        });
    let label =
        normalize_optional_text(profile.label.as_deref()).unwrap_or_else(|| name.to_owned());
    Ok(DesktopResolvedProfile {
        context: control_plane::ConsoleProfileContext {
            name: name.to_owned(),
            label,
            environment,
            color,
            risk_level,
            strict_mode,
            mode,
        },
        config_path: parse_optional_path(profile.config_path.as_deref())?,
        state_root: parse_optional_path(profile.state_root.as_deref())?,
        last_used_at_unix_ms: profile.last_used_at_unix_ms,
        implicit: false,
    })
}

/// Builds the implicit local desktop profile.
pub(crate) fn implicit_profile(name: &str) -> DesktopResolvedProfile {
    DesktopResolvedProfile {
        context: control_plane::ConsoleProfileContext {
            name: name.to_owned(),
            label: "Desktop local".to_owned(),
            environment: "local".to_owned(),
            color: "emerald".to_owned(),
            risk_level: "low".to_owned(),
            strict_mode: false,
            mode: "local".to_owned(),
        },
        config_path: None,
        state_root: None,
        last_used_at_unix_ms: None,
        implicit: true,
    }
}

fn implicit_profile_from_environment(name: &str) -> Result<DesktopResolvedProfile> {
    let mut profile = implicit_profile(name);
    profile.config_path = parse_env_config_path()?;
    Ok(profile)
}

fn parse_env_config_path() -> Result<Option<PathBuf>> {
    let Ok(raw) = env::var(PALYRA_CONFIG_ENV) else {
        return Ok(None);
    };
    let Some(value) = normalize_optional_text(Some(raw.as_str())) else {
        return Ok(None);
    };
    parse_config_path(value.as_str())
        .with_context(|| format!("{PALYRA_CONFIG_ENV} contains an invalid path"))
        .map(Some)
}

fn parse_optional_path(raw: Option<&str>) -> Result<Option<PathBuf>> {
    let Some(value) = normalize_optional_text(raw) else {
        return Ok(None);
    };
    parse_config_path(value.as_str())
        .with_context(|| format!("profile path is invalid: {value}"))
        .map(Some)
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|candidate| !candidate.is_empty()).map(ToOwned::to_owned)
}
