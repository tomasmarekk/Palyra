use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DEFAULT_SECRET_MAX_BYTES: u64 = 64 * 1024;
const DEFAULT_SECRET_REQUIRED: bool = true;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SecretRefreshPolicy {
    #[default]
    OnStartup,
    OnReload,
    PerRun,
    PerUse,
}

impl SecretRefreshPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OnStartup => "on_startup",
            Self::OnReload => "on_reload",
            Self::PerRun => "per_run",
            Self::PerUse => "per_use",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SecretSnapshotPolicy {
    #[default]
    FreezeUntilReload,
    RefreshPerRun,
    RefreshPerUse,
}

impl SecretSnapshotPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FreezeUntilReload => "freeze_until_reload",
            Self::RefreshPerRun => "refresh_per_run",
            Self::RefreshPerUse => "refresh_per_use",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SecretSource {
    Vault {
        vault_ref: String,
    },
    Env {
        variable: String,
    },
    File {
        path: String,
        #[serde(default)]
        trusted_dirs: Vec<String>,
        #[serde(default)]
        allow_symlinks: bool,
    },
    Exec {
        command: Vec<String>,
        #[serde(default)]
        inherited_env: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },
}

impl SecretSource {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Vault { .. } => "vault",
            Self::Env { .. } => "env",
            Self::File { .. } => "file",
            Self::Exec { .. } => "exec",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef {
    #[serde(flatten)]
    pub source: SecretSource,
    #[serde(default = "default_secret_required")]
    pub required: bool,
    #[serde(default)]
    pub refresh_policy: SecretRefreshPolicy,
    #[serde(default)]
    pub snapshot_policy: SecretSnapshotPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redaction_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretRefRedactedView {
    pub kind: String,
    pub fingerprint: String,
    pub required: bool,
    pub refresh_policy: String,
    pub snapshot_policy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redaction_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub source: SecretSourceDisplay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretSourceDisplay {
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trusted_dir_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited_env_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_symlinks: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecretRefValidationError {
    #[error("{field}: {message}")]
    InvalidField { field: &'static str, message: String },
}

impl SecretRef {
    #[must_use]
    pub fn from_legacy_vault_ref(vault_ref: impl Into<String>) -> Self {
        Self {
            source: SecretSource::Vault { vault_ref: vault_ref.into() },
            required: true,
            refresh_policy: SecretRefreshPolicy::OnStartup,
            snapshot_policy: SecretSnapshotPolicy::FreezeUntilReload,
            max_bytes: Some(DEFAULT_SECRET_MAX_BYTES),
            exec_timeout_ms: None,
            redaction_label: None,
            display_name: None,
        }
    }

    pub fn validate(&self) -> Result<(), SecretRefValidationError> {
        if let Some(max_bytes) = self.max_bytes {
            if max_bytes == 0 {
                return Err(invalid_field("max_bytes", "must be greater than zero"));
            }
        }
        if let Some(exec_timeout_ms) = self.exec_timeout_ms {
            if exec_timeout_ms == 0 {
                return Err(invalid_field("exec_timeout_ms", "must be greater than zero"));
            }
            if !matches!(self.source, SecretSource::Exec { .. }) {
                return Err(invalid_field(
                    "exec_timeout_ms",
                    "can only be set for exec secret sources",
                ));
            }
        }

        validate_optional_text(self.redaction_label.as_deref(), "redaction_label", 128)?;
        validate_optional_text(self.display_name.as_deref(), "display_name", 128)?;

        match &self.source {
            SecretSource::Vault { vault_ref } => {
                validate_non_empty_text(vault_ref.as_str(), "source.vault_ref", 512)?;
            }
            SecretSource::Env { variable } => {
                validate_env_var_name(variable.as_str(), "source.variable")?;
            }
            SecretSource::File { path, trusted_dirs, .. } => {
                validate_non_empty_text(path.as_str(), "source.path", 4096)?;
                if trusted_dirs.is_empty() {
                    return Err(invalid_field(
                        "source.trusted_dirs",
                        "must include at least one trusted directory",
                    ));
                }
                for trusted_dir in trusted_dirs {
                    validate_non_empty_text(trusted_dir.as_str(), "source.trusted_dirs", 4096)?;
                }
            }
            SecretSource::Exec { command, inherited_env, cwd } => {
                if command.is_empty() {
                    return Err(invalid_field(
                        "source.command",
                        "must include at least one argv element",
                    ));
                }
                for argument in command {
                    validate_non_empty_text(argument.as_str(), "source.command", 4096)?;
                }
                for variable in inherited_env {
                    validate_env_var_name(variable.as_str(), "source.inherited_env")?;
                }
                validate_optional_text(cwd.as_deref(), "source.cwd", 4096)?;
            }
        }

        Ok(())
    }

    #[must_use]
    pub fn source_kind(&self) -> &'static str {
        self.source.kind()
    }

    #[must_use]
    pub fn fingerprint(&self) -> String {
        let encoded = serde_json::to_vec(self).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(b"palyra.secret_ref.v1");
        hasher.update(encoded);
        let digest = hex::encode(hasher.finalize());
        digest.chars().take(16).collect()
    }

    #[must_use]
    pub fn effective_max_bytes(&self) -> u64 {
        self.max_bytes.unwrap_or(DEFAULT_SECRET_MAX_BYTES)
    }

    #[must_use]
    pub fn redacted_view(&self) -> SecretRefRedactedView {
        let source = match &self.source {
            SecretSource::Vault { .. } => SecretSourceDisplay {
                description: "vault reference".to_owned(),
                trusted_dir_count: None,
                inherited_env_count: None,
                allow_symlinks: None,
            },
            SecretSource::Env { .. } => SecretSourceDisplay {
                description: "environment variable".to_owned(),
                trusted_dir_count: None,
                inherited_env_count: None,
                allow_symlinks: None,
            },
            SecretSource::File { trusted_dirs, allow_symlinks, .. } => SecretSourceDisplay {
                description: "file-backed secret".to_owned(),
                trusted_dir_count: Some(trusted_dirs.len()),
                inherited_env_count: None,
                allow_symlinks: Some(*allow_symlinks),
            },
            SecretSource::Exec { command, inherited_env, .. } => SecretSourceDisplay {
                description: format!("exec-backed secret (argv={})", command.len()),
                trusted_dir_count: None,
                inherited_env_count: Some(inherited_env.len()),
                allow_symlinks: None,
            },
        };

        SecretRefRedactedView {
            kind: self.source_kind().to_owned(),
            fingerprint: self.fingerprint(),
            required: self.required,
            refresh_policy: self.refresh_policy.as_str().to_owned(),
            snapshot_policy: self.snapshot_policy.as_str().to_owned(),
            max_bytes: self.max_bytes,
            exec_timeout_ms: self.exec_timeout_ms,
            redaction_label: self.redaction_label.clone(),
            display_name: self.display_name.clone(),
            source,
        }
    }
}

impl Default for SecretRef {
    fn default() -> Self {
        Self::from_legacy_vault_ref("global/default")
    }
}

fn default_secret_required() -> bool {
    DEFAULT_SECRET_REQUIRED
}

fn invalid_field(field: &'static str, message: impl Into<String>) -> SecretRefValidationError {
    SecretRefValidationError::InvalidField { field, message: message.into() }
}

fn validate_optional_text(
    raw: Option<&str>,
    field: &'static str,
    max_bytes: usize,
) -> Result<(), SecretRefValidationError> {
    if let Some(value) = raw {
        validate_non_empty_text(value, field, max_bytes)?;
    }
    Ok(())
}

fn validate_non_empty_text(
    raw: &str,
    field: &'static str,
    max_bytes: usize,
) -> Result<(), SecretRefValidationError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid_field(field, "cannot be empty"));
    }
    if trimmed.contains('\0') {
        return Err(invalid_field(field, "cannot contain embedded NUL byte"));
    }
    if trimmed.len() > max_bytes {
        return Err(invalid_field(
            field,
            format!("exceeds maximum bytes ({} > {max_bytes})", trimmed.len()),
        ));
    }
    Ok(())
}

fn validate_env_var_name(raw: &str, field: &'static str) -> Result<(), SecretRefValidationError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid_field(field, "cannot be empty"));
    }
    if trimmed.len() > 256 {
        return Err(invalid_field(
            field,
            format!("exceeds maximum bytes ({} > 256)", trimmed.len()),
        ));
    }
    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return Err(invalid_field(field, "cannot be empty"));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(invalid_field(field, "must start with an ASCII letter or underscore"));
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(invalid_field(field, "can only contain ASCII letters, digits, or underscores"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        SecretRef, SecretRefValidationError, SecretRefreshPolicy, SecretSnapshotPolicy,
        SecretSource,
    };

    #[test]
    fn legacy_vault_refs_map_to_structured_secret_ref() {
        let reference = SecretRef::from_legacy_vault_ref("global/openai_api_key");
        assert_eq!(
            reference.source,
            SecretSource::Vault { vault_ref: "global/openai_api_key".to_owned() }
        );
        assert!(reference.required);
        assert_eq!(reference.refresh_policy, SecretRefreshPolicy::OnStartup);
        assert_eq!(reference.snapshot_policy, SecretSnapshotPolicy::FreezeUntilReload);
        assert_eq!(reference.max_bytes, Some(64 * 1024));
    }

    #[test]
    fn redacted_view_hides_source_specific_values() {
        let reference = SecretRef {
            source: SecretSource::Exec {
                command: vec!["git".to_owned(), "credential".to_owned()],
                inherited_env: vec!["PATH".to_owned(), "HOME".to_owned()],
                cwd: Some("/tmp".to_owned()),
            },
            required: false,
            refresh_policy: SecretRefreshPolicy::PerUse,
            snapshot_policy: SecretSnapshotPolicy::RefreshPerUse,
            max_bytes: Some(2048),
            exec_timeout_ms: Some(250),
            redaction_label: Some("model_provider.openai_api_key".to_owned()),
            display_name: Some("OpenAI API key".to_owned()),
        };

        let view = reference.redacted_view();
        let serialized = serde_json::to_string(&view).expect("redacted view should serialize");
        assert!(serialized.contains("exec-backed secret"));
        assert!(!serialized.contains("credential"));
        assert!(!serialized.contains("/tmp"));
        assert_eq!(view.source.inherited_env_count, Some(2));
    }

    #[test]
    fn validation_rejects_invalid_exec_metadata() {
        let reference = SecretRef {
            source: SecretSource::Exec {
                command: vec!["git".to_owned()],
                inherited_env: vec![],
                cwd: None,
            },
            required: true,
            refresh_policy: SecretRefreshPolicy::OnStartup,
            snapshot_policy: SecretSnapshotPolicy::FreezeUntilReload,
            max_bytes: Some(0),
            exec_timeout_ms: None,
            redaction_label: None,
            display_name: None,
        };

        let error = reference.validate().expect_err("zero max bytes must fail validation");
        assert_eq!(
            error,
            SecretRefValidationError::InvalidField {
                field: "max_bytes",
                message: "must be greater than zero".to_owned(),
            }
        );
    }

    #[test]
    fn validation_rejects_file_source_without_trusted_dirs() {
        let reference = SecretRef {
            source: SecretSource::File {
                path: "secrets/openai.txt".to_owned(),
                trusted_dirs: Vec::new(),
                allow_symlinks: false,
            },
            required: true,
            refresh_policy: SecretRefreshPolicy::OnReload,
            snapshot_policy: SecretSnapshotPolicy::FreezeUntilReload,
            max_bytes: None,
            exec_timeout_ms: None,
            redaction_label: None,
            display_name: None,
        };

        let error = reference
            .validate()
            .expect_err("file source without trusted dirs must fail validation");
        assert_eq!(
            error,
            SecretRefValidationError::InvalidField {
                field: "source.trusted_dirs",
                message: "must include at least one trusted directory".to_owned(),
            }
        );
    }

    #[test]
    fn validation_accepts_structured_env_source() {
        let reference = SecretRef {
            source: SecretSource::Env { variable: "PALYRA_OPENAI_API_KEY".to_owned() },
            required: true,
            refresh_policy: SecretRefreshPolicy::OnReload,
            snapshot_policy: SecretSnapshotPolicy::FreezeUntilReload,
            max_bytes: Some(4096),
            exec_timeout_ms: None,
            redaction_label: Some("model_provider.openai_api_key".to_owned()),
            display_name: Some("OpenAI API key".to_owned()),
        };

        reference.validate().expect("well-formed env source should validate");
    }
}
