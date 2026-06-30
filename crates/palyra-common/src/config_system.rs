//! Versioned TOML config document engine shared by the daemon and CLI.
//!
//! Owns parse/migration to the supported config version, dot-path
//! get/set/unset with safe-segment validation, and atomic file writes with
//! rotating backups plus owner-only permissions for secret-bearing files.

use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use toml::{map::Entry, Value};

/// The only config document version currently supported.
pub const CONFIG_VERSION_V1: u32 = 1;
/// Default number of rotating `.bak.N` copies kept beside a config file.
pub const DEFAULT_CONFIG_BACKUP_ROTATION: usize = 5;
/// Audit event emitted after an operator promotes a validated config baseline.
pub const CONFIG_LAST_GOOD_PROMOTED_EVENT_TYPE: &str = "config.last_good.promoted";
/// Audit event emitted before applying a last-known-good restore.
pub const CONFIG_LAST_GOOD_RESTORE_PLANNED_EVENT_TYPE: &str = "config.last_good.restore_planned";
// Rejected defensively: config paths round-trip through JSON/JS consumers
// (web console, import/export), where these keys enable prototype pollution.
const FORBIDDEN_PATH_SEGMENTS: &[&str] = &["__proto__", "prototype", "constructor"];

/// Outcome of bringing a parsed config document up to the supported version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigMigrationInfo {
    pub source_version: u32,
    pub target_version: u32,
    pub migrated: bool,
}

/// Errors from config document parsing, editing, and persistence.
#[derive(Debug, Error)]
pub enum ConfigSystemError {
    #[error("failed to parse config document: {source}")]
    ParseDocument {
        #[source]
        source: toml::de::Error,
    },
    #[error("config document must be a TOML table")]
    DocumentNotTable,
    #[error("config version must be a positive integer")]
    InvalidVersionType,
    #[error("config version must be a positive integer, got {value}")]
    InvalidVersionValue { value: i64 },
    #[error("unsupported config version {version}; supported version is {supported}")]
    UnsupportedVersion { version: u32, supported: u32 },
    #[error("config key path cannot be empty")]
    EmptyPath,
    #[error("config key path segment '{segment}' is invalid: {reason}")]
    InvalidPathSegment { segment: String, reason: &'static str },
    #[error("config key path '{path}' crosses a non-table value at segment '{segment}'")]
    PathCrossesScalar { path: String, segment: String },
    #[error("config set value cannot be empty")]
    EmptyValueLiteral,
    #[error("failed to parse TOML value literal: {source}")]
    ParseValueLiteral {
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize config document: {source}")]
    SerializeDocument {
        #[source]
        source: toml::ser::Error,
    },
    #[error("failed to create config directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read config file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write config file {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to rotate backup from {from} to {to}: {source}")]
    RotateBackup {
        from: PathBuf,
        to: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("backup index must be >= 1")]
    InvalidBackupIndex,
    #[error("backup file not found: {path}")]
    BackupNotFound { path: PathBuf },
    #[error("last-known-good config restore rejected: {reason}")]
    LastKnownGoodRestoreRejected { reason: String },
}

/// Metadata for a config backup promoted or inspected as a last-known-good candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastKnownGoodConfig {
    /// Schema version of this metadata record.
    pub schema_version: u32,
    /// Active config path the backup belongs to.
    pub source_path: String,
    /// Backup file path used as the candidate.
    pub backup_path: String,
    /// Numbered `.bak.N` slot used by the candidate.
    pub backup_index: usize,
    /// Config schema version after parsing and supported migration.
    pub config_version: u32,
    /// Original version when parsing had to migrate the candidate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrated_from_version: Option<u32>,
    /// SHA-256 of the candidate bytes before migration.
    pub content_sha256: String,
    /// Filesystem modification time for the candidate, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_modified_at_unix_ms: Option<i64>,
    /// Event type to record when this candidate is promoted.
    pub promoted_event_type: String,
    /// Event type to record before restoring this candidate.
    pub restore_planned_event_type: String,
}

/// Restore status for a last-known-good config candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LastKnownGoodRestoreStatus {
    /// Candidate may be restored without a schema migration.
    Restorable,
    /// Candidate is valid but needs explicit migration before restore.
    MigrationRequired,
    /// Candidate schema does not match the active runtime contract.
    Rejected,
}

/// Plan for restoring a last-known-good config candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastKnownGoodRestorePlan {
    /// Candidate metadata.
    pub candidate: LastKnownGoodConfig,
    /// Restore verdict.
    pub status: LastKnownGoodRestoreStatus,
    /// Stable reason code.
    pub reason_code: String,
    /// Operator-facing remediation hint.
    pub remediation_hint: String,
}

/// Parses TOML config content and migrates it to the supported version.
///
/// Empty or whitespace-only content yields an empty, freshly versioned
/// document.
///
/// # Errors
/// Returns an error when the content is not valid TOML or declares an
/// unsupported `version`.
pub fn parse_document_with_migration(
    content: &str,
) -> Result<(Value, ConfigMigrationInfo), ConfigSystemError> {
    let mut document = if content.trim().is_empty() {
        Value::Table(Default::default())
    } else {
        toml::from_str(content).map_err(|source| ConfigSystemError::ParseDocument { source })?
    };
    let migration = ensure_document_version(&mut document)?;
    Ok((document, migration))
}

/// Stamps a missing `version` field and validates an existing one.
///
/// A document without `version` is treated as legacy (version 0) and
/// migrated in place to [`CONFIG_VERSION_V1`].
///
/// # Errors
/// Returns an error when the document is not a table, the version is not a
/// positive integer, or the version is newer than [`CONFIG_VERSION_V1`].
pub fn ensure_document_version(
    document: &mut Value,
) -> Result<ConfigMigrationInfo, ConfigSystemError> {
    let table = document.as_table_mut().ok_or(ConfigSystemError::DocumentNotTable)?;
    let source_version =
        if let Some(version) = table.get("version") { parse_version(version)? } else { 0 };

    if source_version == 0 {
        table.insert("version".to_owned(), Value::Integer(i64::from(CONFIG_VERSION_V1)));
        return Ok(ConfigMigrationInfo {
            source_version,
            target_version: CONFIG_VERSION_V1,
            migrated: true,
        });
    }

    if source_version == CONFIG_VERSION_V1 {
        return Ok(ConfigMigrationInfo {
            source_version,
            target_version: CONFIG_VERSION_V1,
            migrated: false,
        });
    }

    Err(ConfigSystemError::UnsupportedVersion {
        version: source_version,
        supported: CONFIG_VERSION_V1,
    })
}

/// Serializes a table document to pretty-printed TOML.
///
/// # Errors
/// Returns an error when the document is not a table or cannot be serialized.
pub fn serialize_document_pretty(document: &Value) -> Result<String, ConfigSystemError> {
    if !document.is_table() {
        return Err(ConfigSystemError::DocumentNotTable);
    }
    toml::to_string_pretty(document)
        .map_err(|source| ConfigSystemError::SerializeDocument { source })
}

/// Parses a bare TOML value literal (e.g. `true`, `42`, `"text"`, `[1, 2]`).
///
/// # Errors
/// Returns an error when the literal is empty or not a valid TOML value.
pub fn parse_toml_value_literal(raw: &str) -> Result<Value, ConfigSystemError> {
    if raw.trim().is_empty() {
        return Err(ConfigSystemError::EmptyValueLiteral);
    }
    // TOML has no bare-value grammar, so wrap the literal in a dummy key to
    // reuse the document parser.
    let wrapped = format!("value = {raw}");
    let mut table: toml::Table = toml::from_str(&wrapped)
        .map_err(|source| ConfigSystemError::ParseValueLiteral { source })?;
    table.remove("value").ok_or(ConfigSystemError::EmptyValueLiteral)
}

/// Formats a TOML value using its canonical literal representation.
pub fn format_toml_value(value: &Value) -> String {
    value.to_string()
}

/// Looks up the value at a dot-separated path; `Ok(None)` when absent.
///
/// # Errors
/// Returns an error when the path is empty, contains an invalid segment, or
/// crosses a non-table value.
pub fn get_value_at_path<'a>(
    document: &'a Value,
    path: &str,
) -> Result<Option<&'a Value>, ConfigSystemError> {
    let segments = parse_path_segments(path)?;
    let mut cursor = document;
    for segment in segments {
        let Some(table) = cursor.as_table() else {
            return Err(ConfigSystemError::PathCrossesScalar {
                path: path.to_owned(),
                segment: segment.to_owned(),
            });
        };
        let Some(next) = table.get(segment) else {
            return Ok(None);
        };
        cursor = next;
    }
    Ok(Some(cursor))
}

/// Sets the value at a dot-separated path, creating intermediate tables.
///
/// # Errors
/// Returns an error when the path is empty, contains an invalid segment, or
/// crosses an existing non-table value.
pub fn set_value_at_path(
    document: &mut Value,
    path: &str,
    value: Value,
) -> Result<(), ConfigSystemError> {
    let segments = parse_path_segments(path)?;
    let Some((last, parent_segments)) = segments.split_last() else {
        return Err(ConfigSystemError::EmptyPath);
    };
    let mut cursor = document.as_table_mut().ok_or(ConfigSystemError::DocumentNotTable)?;
    for segment in parent_segments {
        cursor = match cursor.entry((*segment).to_owned()) {
            Entry::Occupied(entry) => {
                let node = entry.into_mut();
                let Some(table) = node.as_table_mut() else {
                    return Err(ConfigSystemError::PathCrossesScalar {
                        path: path.to_owned(),
                        segment: (*segment).to_owned(),
                    });
                };
                table
            }
            Entry::Vacant(entry) => {
                let node = entry.insert(Value::Table(Default::default()));
                node.as_table_mut().expect("newly inserted table must be a table")
            }
        };
    }
    cursor.insert((*last).to_owned(), value);
    Ok(())
}

/// Removes the value at a dot-separated path; returns whether anything was
/// removed.
///
/// # Errors
/// Returns an error when the path is empty, contains an invalid segment, or
/// crosses an existing non-table value.
pub fn unset_value_at_path(document: &mut Value, path: &str) -> Result<bool, ConfigSystemError> {
    let segments = parse_path_segments(path)?;
    let Some((last, parent_segments)) = segments.split_last() else {
        return Err(ConfigSystemError::EmptyPath);
    };
    let mut cursor = document.as_table_mut().ok_or(ConfigSystemError::DocumentNotTable)?;
    for segment in parent_segments {
        let Some(node) = cursor.get_mut(*segment) else {
            return Ok(false);
        };
        let Some(table) = node.as_table_mut() else {
            return Err(ConfigSystemError::PathCrossesScalar {
                path: path.to_owned(),
                segment: (*segment).to_owned(),
            });
        };
        cursor = table;
    }
    Ok(cursor.remove(*last).is_some())
}

/// Serializes and writes a config document atomically, rotating backups.
///
/// # Errors
/// Returns an error when serialization, backup rotation, or the atomic write
/// fails.
pub fn write_document_with_backups(
    path: &Path,
    document: &Value,
    max_backups: usize,
) -> Result<(), ConfigSystemError> {
    let content = serialize_document_pretty(document)?;
    write_content_with_backups(path, &content, max_backups)
}

/// Like [`write_document_with_backups`], but enforces owner-only permissions
/// for secret-bearing files.
///
/// # Errors
/// Returns an error when serialization, permission tightening, backup
/// rotation, or the atomic write fails.
pub fn write_secret_document_with_backups(
    path: &Path,
    document: &Value,
    max_backups: usize,
) -> Result<(), ConfigSystemError> {
    let content = serialize_document_pretty(document)?;
    write_secret_content_with_backups(path, &content, max_backups)
}

/// Writes raw content atomically, rotating backups first.
///
/// Existing file permissions are preserved; new files get secure defaults
/// (owner-only on Unix).
///
/// # Errors
/// Returns an error when permissions cannot be resolved, backup rotation
/// fails, or the atomic write fails.
pub fn write_content_with_backups(
    path: &Path,
    content: &str,
    max_backups: usize,
) -> Result<(), ConfigSystemError> {
    let target_permissions = resolve_target_permissions(path)?;
    if path.exists() {
        rotate_backups(path, max_backups)?;
    }
    write_atomically(path, content, target_permissions)
}

/// Writes secret-bearing content atomically with owner-only permissions.
///
/// Unlike [`write_content_with_backups`], an existing file's looser
/// permissions are not preserved but tightened to the secure default.
///
/// # Errors
/// Returns an error when permission tightening, backup rotation, or the
/// atomic write fails.
pub fn write_secret_content_with_backups(
    path: &Path,
    content: &str,
    max_backups: usize,
) -> Result<(), ConfigSystemError> {
    let target_permissions = default_secure_permissions()?;
    // Tighten before rotating so the rotated backup inherits owner-only
    // permissions rather than the file's previous mode.
    tighten_existing_file_permissions(path, target_permissions.as_ref())?;
    if path.exists() {
        rotate_backups(path, max_backups)?;
    }
    write_atomically(path, content, target_permissions)
}

/// Shifts `path` into `.bak.1` and each `.bak.N` into `.bak.N+1`, dropping
/// the copy beyond `max_backups`.
///
/// A `max_backups` of zero or a missing file is a no-op.
///
/// # Errors
/// Returns an error when a backup file cannot be removed or renamed.
pub fn rotate_backups(path: &Path, max_backups: usize) -> Result<(), ConfigSystemError> {
    if max_backups == 0 || !path.exists() {
        return Ok(());
    }

    for index in (1..=max_backups).rev() {
        let source = if index == 1 { path.to_path_buf() } else { backup_path(path, index - 1) };
        if !source.exists() {
            continue;
        }

        let destination = backup_path(path, index);
        remove_file_if_exists(&destination).map_err(|source_error| {
            ConfigSystemError::RotateBackup {
                from: source.clone(),
                to: destination.clone(),
                source: source_error,
            }
        })?;
        fs::rename(&source, &destination).map_err(|source_error| {
            ConfigSystemError::RotateBackup { from: source, to: destination, source: source_error }
        })?;
    }

    Ok(())
}

/// Restores config content from `.bak.{backup_index}` and returns the backup
/// path used; the current file is rotated into the backups first.
///
/// # Errors
/// Returns an error when `backup_index` is zero, the backup does not exist,
/// or reading/writing the config file fails.
pub fn recover_config_from_backup(
    path: &Path,
    backup_index: usize,
    max_backups: usize,
) -> Result<PathBuf, ConfigSystemError> {
    if backup_index == 0 {
        return Err(ConfigSystemError::InvalidBackupIndex);
    }

    let source_path = backup_path(path, backup_index);
    if !source_path.exists() {
        return Err(ConfigSystemError::BackupNotFound { path: source_path });
    }
    let content = fs::read_to_string(&source_path)
        .map_err(|source| ConfigSystemError::ReadFile { path: source_path.clone(), source })?;
    write_content_with_backups(path, &content, max_backups)?;
    Ok(source_path)
}

/// Returns the `.bak.{index}` sibling path for a config file.
pub fn backup_path(path: &Path, index: usize) -> PathBuf {
    let mut raw: OsString = path.as_os_str().to_os_string();
    raw.push(format!(".bak.{index}"));
    PathBuf::from(raw)
}

/// Inspects a numbered config backup as a last-known-good candidate.
///
/// The returned metadata contains a SHA-256 of the original backup bytes and
/// records whether parsing required a schema migration. Call
/// [`plan_last_known_good_restore`] before any restore so candidates with a
/// mismatched schema are rejected explicitly instead of being applied silently.
///
/// # Errors
/// Returns an error when `backup_index` is zero, the backup is missing, the
/// bytes are not UTF-8, or the candidate is not a supported config document.
pub fn inspect_last_known_good_config(
    path: &Path,
    backup_index: usize,
) -> Result<LastKnownGoodConfig, ConfigSystemError> {
    if backup_index == 0 {
        return Err(ConfigSystemError::InvalidBackupIndex);
    }
    let candidate_path = backup_path(path, backup_index);
    if !candidate_path.exists() {
        return Err(ConfigSystemError::BackupNotFound { path: candidate_path });
    }
    let bytes = fs::read(&candidate_path)
        .map_err(|source| ConfigSystemError::ReadFile { path: candidate_path.clone(), source })?;
    let content = std::str::from_utf8(bytes.as_slice()).map_err(|source| {
        ConfigSystemError::LastKnownGoodRestoreRejected {
            reason: format!("candidate backup is not valid UTF-8 TOML: {source}"),
        }
    })?;
    let (_, migration) = parse_document_with_migration(content)?;
    let metadata = fs::metadata(&candidate_path).ok();
    Ok(LastKnownGoodConfig {
        schema_version: 1,
        source_path: path.display().to_string(),
        backup_path: candidate_path.display().to_string(),
        backup_index,
        config_version: migration.target_version,
        migrated_from_version: migration.migrated.then_some(migration.source_version),
        content_sha256: sha256_hex(bytes.as_slice()),
        candidate_modified_at_unix_ms: metadata
            .and_then(|metadata| metadata.modified().ok())
            .and_then(system_time_to_unix_ms),
        promoted_event_type: CONFIG_LAST_GOOD_PROMOTED_EVENT_TYPE.to_owned(),
        restore_planned_event_type: CONFIG_LAST_GOOD_RESTORE_PLANNED_EVENT_TYPE.to_owned(),
    })
}

/// Plans a last-known-good restore without mutating the config file.
///
/// A candidate is restorable only when its config schema version exactly
/// matches the active runtime version and parsing did not require migration.
#[must_use]
pub fn plan_last_known_good_restore(
    candidate: LastKnownGoodConfig,
    active_config_version: u32,
) -> LastKnownGoodRestorePlan {
    if candidate.config_version != active_config_version {
        return LastKnownGoodRestorePlan {
            candidate,
            status: LastKnownGoodRestoreStatus::Rejected,
            reason_code: "config.last_good.schema_version_mismatch".to_owned(),
            remediation_hint:
                "Validate and migrate the backup explicitly before restoring it as last-known-good."
                    .to_owned(),
        };
    }
    if candidate.migrated_from_version.is_some() {
        return LastKnownGoodRestorePlan {
            candidate,
            status: LastKnownGoodRestoreStatus::MigrationRequired,
            reason_code: "config.last_good.migration_required".to_owned(),
            remediation_hint:
                "Run config migration on the candidate, then promote the migrated document."
                    .to_owned(),
        };
    }
    LastKnownGoodRestorePlan {
        candidate,
        status: LastKnownGoodRestoreStatus::Restorable,
        reason_code: "config.last_good.restorable".to_owned(),
        remediation_hint: "Restore can proceed after the operator confirms the selected backup."
            .to_owned(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn system_time_to_unix_ms(time: SystemTime) -> Option<i64> {
    let millis = time.duration_since(UNIX_EPOCH).ok()?.as_millis();
    i64::try_from(millis).ok()
}

fn parse_version(value: &Value) -> Result<u32, ConfigSystemError> {
    let raw = value.as_integer().ok_or(ConfigSystemError::InvalidVersionType)?;
    if raw <= 0 {
        return Err(ConfigSystemError::InvalidVersionValue { value: raw });
    }
    u32::try_from(raw).map_err(|_| ConfigSystemError::InvalidVersionValue { value: raw })
}

fn parse_path_segments(path: &str) -> Result<Vec<&str>, ConfigSystemError> {
    if path.trim().is_empty() {
        return Err(ConfigSystemError::EmptyPath);
    }

    let mut segments = Vec::new();
    for segment in path.split('.') {
        validate_segment(segment)?;
        segments.push(segment);
    }

    if segments.is_empty() {
        return Err(ConfigSystemError::EmptyPath);
    }
    Ok(segments)
}

fn validate_segment(segment: &str) -> Result<(), ConfigSystemError> {
    if segment.is_empty() {
        return Err(ConfigSystemError::InvalidPathSegment {
            segment: segment.to_owned(),
            reason: "segment cannot be empty",
        });
    }
    if FORBIDDEN_PATH_SEGMENTS.contains(&segment) {
        return Err(ConfigSystemError::InvalidPathSegment {
            segment: segment.to_owned(),
            reason: "segment is forbidden by safe-path policy",
        });
    }
    if !segment.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')) {
        return Err(ConfigSystemError::InvalidPathSegment {
            segment: segment.to_owned(),
            reason: "segment must use only ASCII letters, digits, '_' or '-'",
        });
    }
    Ok(())
}

fn write_atomically(
    path: &Path,
    content: &str,
    target_permissions: Option<fs::Permissions>,
) -> Result<(), ConfigSystemError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|source| ConfigSystemError::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            })?;
        }
    }

    // The timestamp only disambiguates temporary file names alongside the
    // process id; a pre-epoch clock harmlessly degrades to 0.
    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".tmp.{}.{}", std::process::id(), timestamp_ns));
    let temporary_path = PathBuf::from(temporary_name);

    fs::write(&temporary_path, content)
        .map_err(|source| ConfigSystemError::WriteFile { path: temporary_path.clone(), source })?;
    if let Some(permissions) = target_permissions {
        fs::set_permissions(&temporary_path, permissions).map_err(|source| {
            ConfigSystemError::WriteFile { path: temporary_path.clone(), source }
        })?;
    }

    // On Windows, rename fails when the destination exists: swap the current
    // file aside, move the new content in, then drop the old copy — restoring
    // the original if the second rename fails.
    if let Err(source) = fs::rename(&temporary_path, path) {
        if !path.exists() || !path.is_file() {
            let _ = remove_file_if_exists(&temporary_path);
            return Err(ConfigSystemError::WriteFile { path: path.to_path_buf(), source });
        }

        let rollback_path = swap_path(path, timestamp_ns);
        if let Err(source) = fs::rename(path, &rollback_path) {
            let _ = remove_file_if_exists(&temporary_path);
            return Err(ConfigSystemError::WriteFile { path: path.to_path_buf(), source });
        }
        if let Err(source) = fs::rename(&temporary_path, path) {
            let _ = fs::rename(&rollback_path, path);
            let _ = remove_file_if_exists(&temporary_path);
            return Err(ConfigSystemError::WriteFile { path: path.to_path_buf(), source });
        }
        let _ = remove_file_if_exists(&rollback_path);
    }
    Ok(())
}

fn resolve_target_permissions(path: &Path) -> Result<Option<fs::Permissions>, ConfigSystemError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata.permissions())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => default_secure_permissions(),
        Err(source) => Err(ConfigSystemError::ReadFile { path: path.to_path_buf(), source }),
    }
}

fn tighten_existing_file_permissions(
    path: &Path,
    permissions: Option<&fs::Permissions>,
) -> Result<(), ConfigSystemError> {
    if !path.exists() {
        return Ok(());
    }
    let Some(permissions) = permissions else {
        return Ok(());
    };
    fs::set_permissions(path, permissions.clone())
        .map_err(|source| ConfigSystemError::WriteFile { path: path.to_path_buf(), source })
}

#[cfg(unix)]
fn default_secure_permissions() -> Result<Option<fs::Permissions>, ConfigSystemError> {
    Ok(Some(fs::Permissions::from_mode(0o600)))
}

#[cfg(not(unix))]
fn default_secure_permissions() -> Result<Option<fs::Permissions>, ConfigSystemError> {
    Ok(None)
}

fn swap_path(path: &Path, timestamp_ns: u128) -> PathBuf {
    let mut rollback_name = path.as_os_str().to_os_string();
    rollback_name.push(format!(".swap.{}.{}", std::process::id(), timestamp_ns));
    PathBuf::from(rollback_name)
}

fn remove_file_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use anyhow::Result;
    use tempfile::TempDir;
    use toml::Value;

    use super::{
        backup_path, format_toml_value, get_value_at_path, inspect_last_known_good_config,
        parse_document_with_migration, parse_toml_value_literal, plan_last_known_good_restore,
        recover_config_from_backup, set_value_at_path, unset_value_at_path,
        write_document_with_backups, write_secret_document_with_backups, ConfigMigrationInfo,
        ConfigSystemError, LastKnownGoodRestoreStatus, CONFIG_LAST_GOOD_PROMOTED_EVENT_TYPE,
        CONFIG_LAST_GOOD_RESTORE_PLANNED_EVENT_TYPE, CONFIG_VERSION_V1,
    };

    #[test]
    fn parse_document_with_migration_adds_version_to_legacy_documents() -> Result<()> {
        let (document, migration) = parse_document_with_migration("[daemon]\nport = 7142\n")
            .expect("document should parse");
        assert_eq!(
            migration,
            ConfigMigrationInfo {
                source_version: 0,
                target_version: CONFIG_VERSION_V1,
                migrated: true
            }
        );
        assert_eq!(
            document.as_table().and_then(|table| table.get("version")).and_then(Value::as_integer),
            Some(i64::from(CONFIG_VERSION_V1))
        );
        Ok(())
    }

    #[test]
    fn parse_document_with_migration_rejects_unsupported_version() {
        let result = parse_document_with_migration("version = 2\n");
        assert!(matches!(
            result,
            Err(ConfigSystemError::UnsupportedVersion { version: 2, supported: 1 })
        ));
    }

    #[test]
    fn set_get_and_unset_support_nested_path_operations() -> Result<()> {
        let (mut document, _) = parse_document_with_migration("version = 1\n")?;
        set_value_at_path(&mut document, "daemon.port", Value::Integer(7443))?;
        let value =
            get_value_at_path(&document, "daemon.port")?.expect("daemon.port should be present");
        assert_eq!(value.as_integer(), Some(7443));

        let removed = unset_value_at_path(&mut document, "daemon.port")?;
        assert!(removed, "daemon.port should be removed");
        assert!(get_value_at_path(&document, "daemon.port")?.is_none());
        Ok(())
    }

    #[test]
    fn safe_path_rejects_prototype_pollution_segments() {
        let (mut document, _) =
            parse_document_with_migration("version = 1\n").expect("document should parse");
        let result =
            set_value_at_path(&mut document, "tool_call.__proto__.enabled", Value::Boolean(true));
        assert!(matches!(result, Err(ConfigSystemError::InvalidPathSegment { .. })));
    }

    #[test]
    fn safe_path_rejects_invalid_characters() {
        let (mut document, _) =
            parse_document_with_migration("version = 1\n").expect("document should parse");
        let result = set_value_at_path(&mut document, "daemon.port;", Value::Integer(7142));
        assert!(matches!(result, Err(ConfigSystemError::InvalidPathSegment { .. })));
    }

    #[test]
    fn parse_toml_value_literal_supports_typed_values() -> Result<()> {
        let parsed_bool = parse_toml_value_literal("true")?;
        assert_eq!(parsed_bool, Value::Boolean(true));

        let parsed_string = parse_toml_value_literal("\"ops\"")?;
        assert_eq!(format_toml_value(&parsed_string), "\"ops\"");
        Ok(())
    }

    #[test]
    fn write_document_with_backups_rotates_previous_versions() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        fs::write(&config_path, "version = 1\n[daemon]\nport = 7000\n")?;

        let (mut first_doc, _) = parse_document_with_migration(&fs::read_to_string(&config_path)?)?;
        set_value_at_path(&mut first_doc, "daemon.port", Value::Integer(7001))?;
        write_document_with_backups(&config_path, &first_doc, 2)?;
        assert!(backup_path(&config_path, 1).exists(), "first backup should be created");

        let (mut second_doc, _) =
            parse_document_with_migration(&fs::read_to_string(&config_path)?)?;
        set_value_at_path(&mut second_doc, "daemon.port", Value::Integer(7002))?;
        write_document_with_backups(&config_path, &second_doc, 2)?;

        assert!(backup_path(&config_path, 2).exists(), "second backup should be created");
        let backup_1 = fs::read_to_string(backup_path(&config_path, 1))?;
        let backup_2 = fs::read_to_string(backup_path(&config_path, 2))?;
        assert!(backup_1.contains("7001"));
        assert!(backup_2.contains("7000"));
        Ok(())
    }

    #[test]
    fn write_document_with_backups_replaces_file_without_temporary_artifacts() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        fs::write(&config_path, "version = 1\n[daemon]\nport = 7000\n")?;

        let (mut document, _) = parse_document_with_migration(&fs::read_to_string(&config_path)?)?;
        set_value_at_path(&mut document, "daemon.port", Value::Integer(7443))?;
        write_document_with_backups(&config_path, &document, 0)?;

        let persisted = fs::read_to_string(&config_path)?;
        assert!(persisted.contains("7443"), "updated config should be persisted");

        for entry in fs::read_dir(tempdir.path())? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                !file_name.contains(".tmp."),
                "temporary writer artifact should be removed: {file_name}"
            );
            assert!(
                !file_name.contains(".swap."),
                "rollback artifact should be removed: {file_name}"
            );
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn write_document_with_backups_preserves_existing_file_permissions() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        fs::write(&config_path, "version = 1\n[daemon]\nport = 7000\n")?;
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600))?;

        let (mut document, _) = parse_document_with_migration(&fs::read_to_string(&config_path)?)?;
        set_value_at_path(&mut document, "daemon.port", Value::Integer(7443))?;
        write_document_with_backups(&config_path, &document, 2)?;

        let mode = fs::metadata(&config_path)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "config rewrites must preserve locked-down file permissions");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn write_document_with_backups_uses_secure_default_permissions_for_new_file() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        let mut document = Value::Table(Default::default());
        set_value_at_path(&mut document, "daemon.port", Value::Integer(7142))?;
        write_document_with_backups(&config_path, &document, 0)?;

        let mode = fs::metadata(&config_path)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "new config writes must default to owner-only permissions");
        Ok(())
    }

    #[test]
    fn write_secret_document_with_backups_persists_document() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        let mut document = Value::Table(Default::default());
        set_value_at_path(&mut document, "admin.auth_token", Value::String("secret".to_owned()))?;

        write_secret_document_with_backups(&config_path, &document, 0)?;

        let persisted = fs::read_to_string(&config_path)?;
        assert!(persisted.contains("auth_token = \"secret\""));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn write_secret_document_with_backups_tightens_existing_file_permissions() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        fs::write(&config_path, "version = 1\n")?;
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o644))?;
        let mut document = Value::Table(Default::default());
        set_value_at_path(&mut document, "admin.auth_token", Value::String("secret".to_owned()))?;

        write_secret_document_with_backups(&config_path, &document, 1)?;

        let mode = fs::metadata(&config_path)?.permissions().mode() & 0o777;
        let backup_mode = fs::metadata(backup_path(&config_path, 1))?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "secret-bearing config must be owner-only after rewrite");
        assert_eq!(backup_mode, 0o600, "rotated backups must inherit owner-only permissions");
        Ok(())
    }

    #[test]
    fn recover_config_from_backup_restores_selected_version() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        fs::write(&config_path, "version = 1\n[daemon]\nport = 7000\n")?;

        let (mut first_doc, _) = parse_document_with_migration(&fs::read_to_string(&config_path)?)?;
        set_value_at_path(&mut first_doc, "daemon.port", Value::Integer(7001))?;
        write_document_with_backups(&config_path, &first_doc, 2)?;

        let (mut second_doc, _) =
            parse_document_with_migration(&fs::read_to_string(&config_path)?)?;
        set_value_at_path(&mut second_doc, "daemon.port", Value::Integer(7002))?;
        write_document_with_backups(&config_path, &second_doc, 2)?;

        let recovered = recover_config_from_backup(&config_path, 2, 2)?;
        assert_eq!(recovered, backup_path(&config_path, 2));
        let restored = fs::read_to_string(&config_path)?;
        assert!(restored.contains("7000"), "recover should restore the selected backup content");
        Ok(())
    }

    #[test]
    fn last_known_good_inspection_reports_restorable_backup_metadata() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        fs::write(&config_path, "version = 1\n[daemon]\nport = 7000\n")?;
        let (mut document, _) = parse_document_with_migration(&fs::read_to_string(&config_path)?)?;
        set_value_at_path(&mut document, "daemon.port", Value::Integer(7001))?;
        write_document_with_backups(&config_path, &document, 2)?;

        let candidate = inspect_last_known_good_config(&config_path, 1)?;
        let plan = plan_last_known_good_restore(candidate.clone(), CONFIG_VERSION_V1);

        assert_eq!(candidate.backup_index, 1);
        assert_eq!(candidate.config_version, CONFIG_VERSION_V1);
        assert_eq!(candidate.migrated_from_version, None);
        assert_eq!(candidate.content_sha256.len(), 64);
        assert_eq!(candidate.promoted_event_type, CONFIG_LAST_GOOD_PROMOTED_EVENT_TYPE);
        assert_eq!(
            candidate.restore_planned_event_type,
            CONFIG_LAST_GOOD_RESTORE_PLANNED_EVENT_TYPE
        );
        assert_eq!(plan.status, LastKnownGoodRestoreStatus::Restorable);
        assert_eq!(plan.reason_code, "config.last_good.restorable");
        Ok(())
    }

    #[test]
    fn last_known_good_restore_plan_rejects_migration_required_candidate() -> Result<()> {
        let tempdir = TempDir::new().expect("failed to create tempdir");
        let config_path = tempdir.path().join("palyra.toml");
        fs::write(&config_path, "[daemon]\nport = 7000\n")?;
        let mut document = Value::Table(Default::default());
        set_value_at_path(&mut document, "daemon.port", Value::Integer(7001))?;
        write_document_with_backups(&config_path, &document, 2)?;

        let candidate = inspect_last_known_good_config(&config_path, 1)?;
        let plan = plan_last_known_good_restore(candidate, CONFIG_VERSION_V1);

        assert_eq!(plan.status, LastKnownGoodRestoreStatus::MigrationRequired);
        assert_eq!(plan.reason_code, "config.last_good.migration_required");
        Ok(())
    }
}
