//! Schema-versioned JSON parsing with stepwise forward migrations.
//!
//! Persisted JSON documents carry a `schema_version` field; loading applies one migration
//! per version step until the current version is reached, so old state files written by
//! earlier builds stay readable. Documents newer than the running build are rejected.

use anyhow::{anyhow, bail, Context, Result};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

/// Field carrying the document schema version in every versioned JSON format.
pub const SCHEMA_VERSION_FIELD: &str = "schema_version";
/// Shared metadata field for last-update wall-clock time in unix milliseconds.
pub const UPDATED_AT_UNIX_MS_FIELD: &str = "updated_at_unix_ms";

/// A single in-place migration step from one schema version to the next.
pub type JsonMigrationFn = fn(&mut Map<String, Value>) -> Result<()>;

/// Identity of a versioned JSON format: a diagnostic name and its current version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedJsonFormat {
    pub format_name: &'static str,
    pub current_version: u32,
}

impl VersionedJsonFormat {
    /// Creates a format descriptor for `format_name` at `current_version`.
    #[must_use]
    pub const fn new(format_name: &'static str, current_version: u32) -> Self {
        Self { format_name, current_version }
    }
}

/// Inserts `default` under `field_name` if the field is absent (migration helper).
pub fn ensure_i64_field(object: &mut Map<String, Value>, field_name: &'static str, default: i64) {
    object.entry(field_name.to_owned()).or_insert_with(|| Value::from(default));
}

/// Shared v0→v1 migration: backfills [`UPDATED_AT_UNIX_MS_FIELD`] with `0`.
///
/// # Errors
/// Never fails; the `Result` only satisfies the [`JsonMigrationFn`] signature.
pub fn migrate_updated_at_metadata_v0_to_v1(object: &mut Map<String, Value>) -> Result<()> {
    ensure_i64_field(object, UPDATED_AT_UNIX_MS_FIELD, 0);
    Ok(())
}

/// Parses bytes as `format`, migrating older documents forward before deserializing.
///
/// # Errors
/// Fails on malformed JSON, an unsupported or future schema version, a failing migration
/// step, or a post-migration document that does not deserialize into `T`.
pub fn parse_versioned_json<T>(
    bytes: &[u8],
    format: VersionedJsonFormat,
    migrations: &[(u32, JsonMigrationFn)],
) -> Result<T>
where
    T: DeserializeOwned,
{
    let value = serde_json::from_slice::<Value>(bytes)
        .with_context(|| format!("failed to parse {} json", format.format_name))?;
    let migrated = migrate_versioned_json_value(value, format, migrations)?;
    serde_json::from_value::<T>(migrated)
        .with_context(|| format!("failed to deserialize {}", format.format_name))
}

/// Migrates a JSON value to `format.current_version`, one version step at a time.
///
/// # Errors
/// Fails when the value is not an object, the version field is malformed, the document is
/// newer than `current_version`, a migration step is missing, or a step returns an error.
pub fn migrate_versioned_json_value(
    value: Value,
    format: VersionedJsonFormat,
    migrations: &[(u32, JsonMigrationFn)],
) -> Result<Value> {
    let mut object = match value {
        Value::Object(object) => object,
        _ => bail!("{} must be a JSON object", format.format_name),
    };
    // Documents predating versioning carry no schema_version field; treat them as v0.
    let mut version = match object.get(SCHEMA_VERSION_FIELD) {
        Some(value) => parse_schema_version(value, format.format_name)?,
        None => 0,
    };
    if version > format.current_version {
        bail!("unsupported {} schema version {}", format.format_name, version);
    }
    while version < format.current_version {
        let Some((_, migration)) =
            migrations.iter().find(|(from_version, _)| *from_version == version)
        else {
            bail!("unsupported {} schema version {}", format.format_name, version);
        };
        migration(&mut object).with_context(|| {
            format!("failed to migrate {} from schema version {}", format.format_name, version)
        })?;
        version += 1;
        object.insert(SCHEMA_VERSION_FIELD.to_owned(), Value::from(version));
    }
    Ok(Value::Object(object))
}

fn parse_schema_version(value: &Value, format_name: &str) -> Result<u32> {
    match value {
        Value::Number(number) => {
            number.as_u64().and_then(|parsed| u32::try_from(parsed).ok()).ok_or_else(|| {
                anyhow!("{format_name} {SCHEMA_VERSION_FIELD} must be an unsigned 32-bit integer")
            })
        }
        _ => bail!("{format_name} {SCHEMA_VERSION_FIELD} must be an unsigned 32-bit integer"),
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{
        migrate_updated_at_metadata_v0_to_v1, parse_versioned_json, JsonMigrationFn,
        VersionedJsonFormat,
    };

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct ExampleIndex {
        schema_version: u32,
        updated_at_unix_ms: i64,
        #[serde(default)]
        entries: Vec<String>,
    }

    #[test]
    fn versioned_json_migrates_legacy_payload_without_schema_version() {
        let parsed: ExampleIndex = parse_versioned_json(
            br#"{"entries":["alpha"]}"#,
            VersionedJsonFormat::new("example index", 1),
            &[(0, migrate_updated_at_metadata_v0_to_v1 as JsonMigrationFn)],
        )
        .expect("legacy payload should migrate to the current schema");
        assert_eq!(
            parsed,
            ExampleIndex {
                schema_version: 1,
                updated_at_unix_ms: 0,
                entries: vec!["alpha".to_owned()],
            }
        );
    }

    #[test]
    fn versioned_json_rejects_future_schema_version() {
        let error = parse_versioned_json::<ExampleIndex>(
            br#"{"schema_version":2,"updated_at_unix_ms":0,"entries":[]}"#,
            VersionedJsonFormat::new("example index", 1),
            &[(0, migrate_updated_at_metadata_v0_to_v1 as JsonMigrationFn)],
        )
        .expect_err("future schema version should be rejected");
        assert!(
            error.to_string().contains("unsupported example index schema version 2"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn versioned_json_rejects_non_integer_schema_version() {
        let error = parse_versioned_json::<ExampleIndex>(
            br#"{"schema_version":"v1","updated_at_unix_ms":0,"entries":[]}"#,
            VersionedJsonFormat::new("example index", 1),
            &[(0, migrate_updated_at_metadata_v0_to_v1 as JsonMigrationFn)],
        )
        .expect_err("non-integer schema version should be rejected");
        assert!(
            error.to_string().contains("schema_version must be an unsigned 32-bit integer"),
            "unexpected error: {error:#}"
        );
    }
}
