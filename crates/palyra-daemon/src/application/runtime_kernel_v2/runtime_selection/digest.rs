//! Canonical domain-separated digests used by runtime-selection contracts.
//!
//! Digests detect accidental drift and corruption. Journal integrity and the
//! host-issued admission proof, not an unkeyed digest, are the authenticity boundary.

use serde::{Deserialize, Deserializer, Serialize};

use crate::application::tool_registry::{canonical_json_bytes, stable_hash_bytes};

use super::service::RuntimeSelectionError;

/// A validated lowercase SHA-256 value.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub(crate) struct SelectionDigest(String);

impl std::fmt::Debug for SelectionDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("SelectionDigest").field(&self.0).finish()
    }
}

impl<'de> Deserialize<'de> for SelectionDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

impl SelectionDigest {
    /// Parses a canonical lowercase SHA-256 digest.
    ///
    /// # Errors
    /// Returns [`RuntimeSelectionError::InvalidDigest`] for a non-canonical value.
    pub(crate) fn parse(value: String) -> Result<Self, RuntimeSelectionError> {
        if value.len() != 64
            || !value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(RuntimeSelectionError::InvalidDigest);
        }
        Ok(Self(value))
    }

    /// Hashes raw bytes with a mandatory domain separator.
    #[must_use]
    pub(crate) fn from_domain_bytes(domain: &[u8], bytes: &[u8]) -> Self {
        let mut payload = Vec::with_capacity(domain.len().saturating_add(bytes.len()));
        payload.extend_from_slice(domain);
        payload.extend_from_slice(bytes);
        Self(stable_hash_bytes(payload.as_slice()))
    }

    /// Returns the lowercase hexadecimal digest.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

pub(super) fn digest_serializable<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<SelectionDigest, RuntimeSelectionError> {
    let value = serde_json::to_value(value).map_err(|_| RuntimeSelectionError::Serialization)?;
    Ok(SelectionDigest::from_domain_bytes(domain, canonical_json_bytes(&value).as_slice()))
}
