use std::{collections::BTreeMap, fs, path::Path};

use crate::error::SkillPackagingError;
use crate::manifest::{normalize_identifier, normalize_public_key_hex};
use crate::models::{SkillManifest, SkillTrustStore};

impl SkillTrustStore {
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
        fs::write(path, payload).map_err(|error| {
            SkillPackagingError::Io(format!(
                "failed to write trust store {}: {error}",
                path.display()
            ))
        })
    }

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
