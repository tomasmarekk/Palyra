//! Vault scope model: the namespace a secret belongs to (global, principal, channel, skill).
//!
//! The storage-string form feeds AAD construction and object-id hashing, so its format is part
//! of the on-disk contract — existing secrets become unreachable if it changes.

use std::fmt;
use std::str::FromStr;

use crate::VaultError;

/// Maximum byte length accepted for a single scope segment (ids and channel names).
pub const MAX_SCOPE_SEGMENT_BYTES: usize = 256;

/// Namespace a secret is stored under; secrets in different scopes never collide.
///
/// Parse from strings of the form `global`, `principal:<id>`, `channel:<name>:<account_id>`,
/// or `skill:<skill_id>` via [`FromStr`]. Serde representation is pinned by config/import
/// contracts — do not rename variants or fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VaultScope {
    /// Device-wide secrets not tied to any principal, channel, or skill.
    Global,
    /// Secrets owned by a single principal (operator or agent identity).
    Principal {
        /// Principal identifier; may itself contain `:` (e.g. `user:ops`).
        principal_id: String,
    },
    /// Secrets bound to one account on one communication channel.
    Channel {
        /// Channel name (e.g. `discord`). Parsing splits at the first `:`, so names containing
        /// `:` do not round-trip through [`VaultScope::as_storage_str`].
        channel_name: String,
        /// Account identifier within the channel.
        account_id: String,
    },
    /// Secrets granted to a specific installed skill.
    Skill {
        /// Skill identifier.
        skill_id: String,
    },
}

impl VaultScope {
    /// Renders the canonical storage string used for AAD binding and object-id hashing.
    #[must_use]
    pub fn as_storage_str(&self) -> String {
        match self {
            Self::Global => "global".to_owned(),
            Self::Principal { principal_id } => format!("principal:{principal_id}"),
            Self::Channel { channel_name, account_id } => {
                format!("channel:{channel_name}:{account_id}")
            }
            Self::Skill { skill_id } => format!("skill:{skill_id}"),
        }
    }
}

impl fmt::Display for VaultScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_storage_str().as_str())
    }
}

impl FromStr for VaultScope {
    type Err = VaultError;

    /// Parses the storage-string forms listed on [`VaultScope`].
    ///
    /// # Errors
    /// Returns [`VaultError::InvalidScope`] for unknown prefixes and for empty, oversized, or
    /// illegal segments.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let normalized = raw.trim();
        if normalized.eq_ignore_ascii_case("global") {
            return Ok(Self::Global);
        }

        if let Some(rest) = normalized.strip_prefix("principal:") {
            let principal_id = validate_scope_segment(rest, "principal_id")?;
            return Ok(Self::Principal { principal_id });
        }

        if let Some(rest) = normalized.strip_prefix("channel:") {
            let mut parts = rest.splitn(2, ':');
            let channel_name = parts.next().unwrap_or_default();
            let account_id = parts.next().unwrap_or_default();
            if account_id.is_empty() {
                return Err(VaultError::InvalidScope(
                    "channel scope must be channel:<name>:<account_id>".to_owned(),
                ));
            }
            return Ok(Self::Channel {
                channel_name: validate_scope_segment(channel_name, "channel_name")?,
                account_id: validate_scope_segment(account_id, "account_id")?,
            });
        }

        if let Some(rest) = normalized.strip_prefix("skill:") {
            let skill_id = validate_scope_segment(rest, "skill_id")?;
            return Ok(Self::Skill { skill_id });
        }

        Err(VaultError::InvalidScope(
            "scope must be one of: global | principal:<id> | channel:<name>:<account_id> | skill:<skill_id>"
                .to_owned(),
        ))
    }
}

/// Trims and validates one scope segment: non-empty, size-capped, and free of NUL and slashes.
///
/// `/` and `\` are banned because the scope is the left half of `<scope>/<key>` vault
/// references ([`crate::VaultRef::parse`] splits at the first `/`); the NUL ban is hostile-input
/// hygiene for downstream C APIs and log output.
fn validate_scope_segment(raw: &str, label: &str) -> Result<String, VaultError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(VaultError::InvalidScope(format!("{label} cannot be empty")));
    }
    if value.len() > MAX_SCOPE_SEGMENT_BYTES {
        return Err(VaultError::InvalidScope(format!(
            "{label} exceeds max bytes ({} > {})",
            value.len(),
            MAX_SCOPE_SEGMENT_BYTES
        )));
    }
    if value.contains('\0') || value.contains('/') || value.contains('\\') {
        return Err(VaultError::InvalidScope(format!("{label} contains invalid characters")));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::VaultScope;

    #[test]
    fn scope_parsing_accepts_all_supported_shapes() {
        assert_eq!(
            "global".parse::<VaultScope>().expect("global should parse"),
            VaultScope::Global
        );
        assert_eq!(
            "principal:user:ops".parse::<VaultScope>().expect("principal should parse"),
            VaultScope::Principal { principal_id: "user:ops".to_owned() }
        );
        assert_eq!(
            "channel:slack:acct-1".parse::<VaultScope>().expect("channel should parse"),
            VaultScope::Channel {
                channel_name: "slack".to_owned(),
                account_id: "acct-1".to_owned()
            }
        );
        assert_eq!(
            "skill:extractor".parse::<VaultScope>().expect("skill should parse"),
            VaultScope::Skill { skill_id: "extractor".to_owned() }
        );
    }
}
