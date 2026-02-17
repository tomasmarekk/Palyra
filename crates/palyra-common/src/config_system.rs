use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use thiserror::Error;
use toml::{map::Entry, Value};

pub const CONFIG_VERSION_V1: u32 = 1;
pub const DEFAULT_CONFIG_BACKUP_ROTATION: usize = 5;
const FORBIDDEN_PATH_SEGMENTS: &[&str] = &["__proto__", "prototype", "constructor"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigMigrationInfo {
    pub source_version: u32,
    pub target_version: u32,
    pub migrated: bool,
}

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
}

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

pub fn serialize_document_pretty(document: &Value) -> Result<String, ConfigSystemError> {
    if !document.is_table() {
        return Err(ConfigSystemError::DocumentNotTable);
    }
    toml::to_string_pretty(document)
        .map_err(|source| ConfigSystemError::SerializeDocument { source })
}

pub fn parse_toml_value_literal(raw: &str) -> Result<Value, ConfigSystemError> {
    if raw.trim().is_empty() {
        return Err(ConfigSystemError::EmptyValueLiteral);
    }
    let wrapped = format!("value = {raw}");
    let mut table: toml::Table = toml::from_str(&wrapped)
        .map_err(|source| ConfigSystemError::ParseValueLiteral { source })?;
    table.remove("value").ok_or(ConfigSystemError::EmptyValueLiteral)
}

pub fn format_toml_value(value: &Value) -> String {
    value.to_string()
}

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

pub fn write_document_with_backups(
    path: &Path,
    document: &Value,
    max_backups: usize,
) -> Result<(), ConfigSystemError> {
    let content = serialize_document_pretty(document)?;
    write_content_with_backups(path, &content, max_backups)
}

pub fn write_content_with_backups(
    path: &Path,
    content: &str,
    max_backups: usize,
) -> Result<(), ConfigSystemError> {
    if path.exists() {
        rotate_backups(path, max_backups)?;
    }
    write_atomically(path, content)
}

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
    let source_for_read = source_path.clone();
    let content = fs::read_to_string(&source_path)
        .map_err(|source| ConfigSystemError::ReadFile { path: source_for_read, source })?;
    write_content_with_backups(path, &content, max_backups)?;
    Ok(source_path)
}

pub fn backup_path(path: &Path, index: usize) -> PathBuf {
    let mut raw: OsString = path.as_os_str().to_os_string();
    raw.push(format!(".bak.{index}"));
    PathBuf::from(raw)
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

fn write_atomically(path: &Path, content: &str) -> Result<(), ConfigSystemError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|source| ConfigSystemError::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            })?;
        }
    }

    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".tmp.{}.{}", std::process::id(), timestamp_ns));
    let temporary_path = PathBuf::from(temporary_name);

    fs::write(&temporary_path, content)
        .map_err(|source| ConfigSystemError::WriteFile { path: temporary_path.clone(), source })?;

    if path.exists() {
        remove_file_if_exists(path)
            .map_err(|source| ConfigSystemError::WriteFile { path: path.to_path_buf(), source })?;
    }
    if let Err(source) = fs::rename(&temporary_path, path) {
        let _ = remove_file_if_exists(&temporary_path);
        return Err(ConfigSystemError::WriteFile { path: path.to_path_buf(), source });
    }
    Ok(())
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

    use anyhow::Result;
    use tempfile::TempDir;
    use toml::Value;

    use super::{
        backup_path, format_toml_value, get_value_at_path, parse_document_with_migration,
        parse_toml_value_literal, recover_config_from_backup, set_value_at_path,
        unset_value_at_path, write_document_with_backups, ConfigMigrationInfo, ConfigSystemError,
        CONFIG_VERSION_V1,
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
}
