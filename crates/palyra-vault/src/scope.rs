use std::fmt;
use std::str::FromStr;

use crate::{VaultError, MAX_SCOPE_SEGMENT_BYTES};

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VaultScope {
    Global,
    Principal { principal_id: String },
    Channel { channel_name: String, account_id: String },
    Skill { skill_id: String },
}

impl VaultScope {
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
