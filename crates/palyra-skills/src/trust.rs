//! Trust-store persistence and normalization (publisher allowlist + TOFU
//! pins), plus the builder-output review gate.
//!
//! Every load/save path funnels through [`SkillTrustStore::normalize`] so
//! publishers and keys are always in canonical form before any trust
//! comparison happens.

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use crate::error::SkillPackagingError;
use crate::manifest::{normalize_identifier, normalize_public_key_hex};
use crate::models::{SkillManifest, SkillTrustStore};

#[cfg(unix)]
const TRUST_STORE_FILE_MODE: u32 = 0o600;

impl SkillTrustStore {
    /// Loads and normalizes a trust store from a JSON file.
    ///
    /// A missing or whitespace-only file yields an empty store (the
    /// uninitialized state, in which nothing is trusted yet); any other
    /// unreadable or malformed content is an error rather than an empty
    /// store, so corruption can never silently widen or reset trust.
    ///
    /// # Errors
    /// Returns [`SkillPackagingError::Io`] when the file cannot be read and
    /// [`SkillPackagingError::Serialization`] when the JSON does not parse or
    /// contains invalid publishers/keys.
    pub fn load(path: &Path) -> Result<Self, SkillPackagingError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let payload = fs::read(path).map_err(|error| {
            SkillPackagingError::Io(format!(
                "failed to read trust store {}: {error}",
                path.display()
            ))
        })?;
        if payload.iter().all(u8::is_ascii_whitespace) {
            return Ok(Self::default());
        }
        let mut trust_store =
            serde_json::from_slice::<Self>(payload.as_slice()).map_err(|error| {
                SkillPackagingError::Serialization(format!(
                    "failed to parse trust store {}: {error}",
                    path.display()
                ))
            })?;
        trust_store.normalize()?;
        Ok(trust_store)
    }

    /// Writes the normalized trust store as pretty-printed JSON, creating
    /// parent directories as needed.
    ///
    /// # Errors
    /// Returns [`SkillPackagingError::Io`] on directory-creation or write
    /// failures and [`SkillPackagingError::Serialization`] when the store
    /// contains entries that fail normalization.
    pub fn save(&self, path: &Path) -> Result<(), SkillPackagingError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                SkillPackagingError::Io(format!(
                    "failed to create trust store directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let mut normalized = self.clone();
        normalized.normalize()?;
        let payload = serde_json::to_vec_pretty(&normalized).map_err(|error| {
            SkillPackagingError::Serialization(format!("failed to serialize trust store: {error}"))
        })?;
        write_trust_store_atomically(path, payload.as_slice())
    }

    /// Adds a key to the publisher's allowlist (idempotent; keys stay sorted).
    ///
    /// Allowlisting takes precedence over any TOFU pin for the same publisher
    /// during verification.
    ///
    /// # Errors
    /// Returns [`SkillPackagingError::ManifestValidation`] when the publisher
    /// identifier or the hex key fails normalization.
    pub fn add_trusted_key(
        &mut self,
        publisher: &str,
        public_key_hex: &str,
    ) -> Result<(), SkillPackagingError> {
        let publisher = normalize_identifier(publisher, "publisher")?;
        let key = normalize_public_key_hex(public_key_hex)?;
        let keys = self.trusted_publishers.entry(publisher).or_default();
        if !keys.iter().any(|existing| existing == &key) {
            keys.push(key);
            keys.sort();
            keys.dedup();
        }
        Ok(())
    }

    /// Re-canonicalizes all publishers and keys, rejecting invalid entries.
    ///
    /// Distinct raw map keys can collapse to one canonical publisher (e.g.
    /// only differing by surrounding whitespace), which is why the merge logic
    /// below deduplicates allowlist keys and treats colliding TOFU pins with
    /// different keys as an error instead of silently keeping one.
    pub(crate) fn normalize(&mut self) -> Result<(), SkillPackagingError> {
        let mut trusted_publishers = BTreeMap::<String, Vec<String>>::new();
        for (publisher_raw, keys_raw) in &self.trusted_publishers {
            let publisher = normalize_identifier(publisher_raw, "publisher").map_err(|error| {
                SkillPackagingError::Serialization(format!(
                    "invalid trust-store publisher '{publisher_raw}': {error}"
                ))
            })?;
            let mut normalized_keys = Vec::with_capacity(keys_raw.len());
            for key_raw in keys_raw {
                let key = normalize_public_key_hex(key_raw).map_err(|error| {
                    SkillPackagingError::Serialization(format!(
                        "invalid trusted key for publisher '{publisher}': {error}"
                    ))
                })?;
                normalized_keys.push(key);
            }
            if normalized_keys.is_empty() {
                return Err(SkillPackagingError::Serialization(format!(
                    "trusted publisher '{publisher}' must include at least one key"
                )));
            }
            let keys = trusted_publishers.entry(publisher).or_default();
            keys.extend(normalized_keys);
            keys.sort();
            keys.dedup();
        }

        let mut tofu_publishers = BTreeMap::<String, String>::new();
        for (publisher_raw, key_raw) in &self.tofu_publishers {
            let publisher = normalize_identifier(publisher_raw, "publisher").map_err(|error| {
                SkillPackagingError::Serialization(format!(
                    "invalid trust-store TOFU publisher '{publisher_raw}': {error}"
                ))
            })?;
            let key = normalize_public_key_hex(key_raw).map_err(|error| {
                SkillPackagingError::Serialization(format!(
                    "invalid TOFU key for publisher '{publisher}': {error}"
                ))
            })?;
            if let Some(existing) = tofu_publishers.get(&publisher) {
                if existing != &key {
                    return Err(SkillPackagingError::Serialization(format!(
                        "conflicting TOFU keys for publisher '{publisher}'"
                    )));
                }
            }
            tofu_publishers.insert(publisher, key);
        }

        self.trusted_publishers = trusted_publishers;
        self.tofu_publishers = tofu_publishers;
        Ok(())
    }
}

fn write_trust_store_atomically(path: &Path, payload: &[u8]) -> Result<(), SkillPackagingError> {
    let temp_path = trust_store_temp_path(path)?;
    let mut created_temp = false;
    let result = (|| {
        let mut temp_file = create_trust_store_temp_file(temp_path.as_path())?;
        created_temp = true;
        temp_file.write_all(payload).map_err(|error| {
            SkillPackagingError::Io(format!(
                "failed to write temporary trust store {}: {error}",
                temp_path.display()
            ))
        })?;
        temp_file.sync_all().map_err(|error| {
            SkillPackagingError::Io(format!(
                "failed to sync temporary trust store {}: {error}",
                temp_path.display()
            ))
        })?;
        drop(temp_file);
        fs::rename(temp_path.as_path(), path).map_err(|error| {
            SkillPackagingError::Io(format!(
                "failed to replace trust store {} with {}: {error}",
                path.display(),
                temp_path.display()
            ))
        })?;
        harden_trust_store_file_permissions(path)
    })();
    if result.is_err() && created_temp {
        let _ = fs::remove_file(temp_path);
    }
    result
}

fn create_trust_store_temp_file(path: &Path) -> Result<fs::File, SkillPackagingError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(TRUST_STORE_FILE_MODE);
    }
    options.open(path).map_err(|error| {
        SkillPackagingError::Io(format!(
            "failed to create temporary trust store {}: {error}",
            path.display()
        ))
    })
}

#[cfg(unix)]
fn harden_trust_store_file_permissions(path: &Path) -> Result<(), SkillPackagingError> {
    fs::set_permissions(path, fs::Permissions::from_mode(TRUST_STORE_FILE_MODE)).map_err(|error| {
        SkillPackagingError::Io(format!(
            "failed to set secure permissions on trust store {}: {error}",
            path.display()
        ))
    })
}

#[cfg(not(unix))]
fn harden_trust_store_file_permissions(_path: &Path) -> Result<(), SkillPackagingError> {
    Ok(())
}

fn trust_store_temp_path(path: &Path) -> Result<PathBuf, SkillPackagingError> {
    let file_name = path.file_name().ok_or_else(|| {
        SkillPackagingError::Io(format!("trust store path {} has no file name", path.display()))
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let mut temp_file_name = OsString::from(".");
    temp_file_name.push(file_name);
    temp_file_name.push(OsStr::new(format!(".{}.{}.tmp", std::process::id(), nonce).as_str()));
    let mut temp_path = path.to_path_buf();
    temp_path.set_file_name(temp_file_name);
    Ok(temp_path)
}

/// Returns `true` when a builder-generated skill still needs human review.
///
/// Experimental builder outputs are held until `review_status` reads
/// `approved` or `signed` (case-insensitive); manifests without builder
/// metadata never require this gate.
#[must_use]
pub fn builder_manifest_requires_review(manifest: &SkillManifest) -> bool {
    manifest.builder.as_ref().is_some_and(|builder| {
        builder.experimental
            && !matches!(
                builder.review_status.trim().to_ascii_lowercase().as_str(),
                "approved" | "signed"
            )
    })
}
