//! Channel-native command contracts: command specs published to providers,
//! sync-state bookkeeping, and inbound invocation payloads.
//!
//! These shapes describe slash-command-style integrations registered with a
//! channel provider; the daemon owns the catalog and tracks drift through
//! [`ChannelCommandSyncState`].

use serde::{Deserialize, Serialize};

use super::validation::{
    validate_json_bytes, validate_non_empty_identifier, validate_optional_field, ProtocolError,
    MAX_CONNECTOR_ID_BYTES, MAX_OPERATION_REASON_BYTES,
};

const MAX_COMMAND_NAME_BYTES: usize = 64;
const MAX_ARGUMENT_NAME_BYTES: usize = 64;
const MAX_ARGUMENT_DESCRIPTION_BYTES: usize = 512;
const MAX_COMMAND_DESCRIPTION_BYTES: usize = 1_024;
const MAX_COMMAND_CATALOG_HASH_BYTES: usize = 128;
const MAX_NATIVE_INVOCATION_BYTES: usize = 32 * 1024;

/// Argument type of a channel-native command parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelCommandArgumentKind {
    String,
    Enum,
    Bool,
    Int,
    Duration,
    IdRef,
    FreeformTail,
}

impl ChannelCommandArgumentKind {
    /// Returns the stable snake_case label matching the serde encoding.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Enum => "enum",
            Self::Bool => "bool",
            Self::Int => "int",
            Self::Duration => "duration",
            Self::IdRef => "id_ref",
            Self::FreeformTail => "freeform_tail",
        }
    }
}

/// One declared parameter of a channel-native command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelNativeCommandArgument {
    pub name: String,
    pub kind: ChannelCommandArgumentKind,
    pub required: bool,
    /// Allowed values; required (non-empty) when `kind` is `Enum`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_values: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ChannelNativeCommandArgument {
    /// Validates argument naming, description limits, and enum-value rules.
    ///
    /// # Errors
    /// Returns a protocol error naming the first invalid field.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.name.as_str(),
            "command.argument.name",
            MAX_ARGUMENT_NAME_BYTES,
        )?;
        validate_optional_field(
            self.description.as_deref(),
            "command.argument.description",
            MAX_ARGUMENT_DESCRIPTION_BYTES,
        )?;
        if self.kind == ChannelCommandArgumentKind::Enum && self.enum_values.is_empty() {
            return Err(ProtocolError::InvalidField {
                field: "command.argument.enum_values",
                reason: "enum arguments require at least one value",
            });
        }
        for value in &self.enum_values {
            validate_non_empty_identifier(
                value.as_str(),
                "command.argument.enum_values",
                MAX_ARGUMENT_NAME_BYTES,
            )?;
        }
        Ok(())
    }
}

/// Specification of one command as registered with the channel provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelNativeCommandSpec {
    pub name: String,
    pub description: String,
    /// Policy action evaluated before the command executes.
    pub policy_action: String,
    pub side_effecting: bool,
    #[serde(default)]
    pub arguments: Vec<ChannelNativeCommandArgument>,
}

impl ChannelNativeCommandSpec {
    /// Validates the command identifiers and every declared argument.
    ///
    /// # Errors
    /// Returns a protocol error naming the first invalid field.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(self.name.as_str(), "command.name", MAX_COMMAND_NAME_BYTES)?;
        validate_non_empty_identifier(
            self.policy_action.as_str(),
            "command.policy_action",
            MAX_OPERATION_REASON_BYTES,
        )?;
        validate_non_empty_identifier(
            self.description.as_str(),
            "command.description",
            MAX_COMMAND_DESCRIPTION_BYTES,
        )?;
        for argument in &self.arguments {
            argument.validate()?;
        }
        Ok(())
    }
}

/// Catalog synchronization state between the daemon and the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelCommandSyncStatus {
    Pending,
    Synced,
    Drifted,
    Unsupported,
    Failed,
}

impl ChannelCommandSyncStatus {
    /// Returns the stable snake_case label matching the serde encoding.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Synced => "synced",
            Self::Drifted => "drifted",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
        }
    }
}

/// Last known command-catalog sync result for one connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelCommandSyncState {
    pub connector_id: String,
    /// Hash of the daemon-side catalog the provider state was synced against;
    /// a mismatch marks the state as drifted.
    pub command_catalog_hash: String,
    pub status: ChannelCommandSyncStatus,
    pub updated_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl ChannelCommandSyncState {
    /// Validates identifier fields and size limits.
    ///
    /// # Errors
    /// Returns a protocol error naming the first invalid field.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.connector_id.as_str(),
            "connector_id",
            MAX_CONNECTOR_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.command_catalog_hash.as_str(),
            "command_catalog_hash",
            MAX_COMMAND_CATALOG_HASH_BYTES,
        )?;
        validate_optional_field(
            self.native_revision.as_deref(),
            "native_revision",
            MAX_OPERATION_REASON_BYTES,
        )?;
        validate_optional_field(
            self.last_error.as_deref(),
            "last_error",
            MAX_OPERATION_REASON_BYTES,
        )?;
        Ok(())
    }
}

/// Inbound invocation of a channel-native command by a provider user.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelNativeCommandInvocationPayload {
    pub command: String,
    /// Raw JSON-encoded arguments; empty means the command takes no arguments.
    #[serde(default)]
    pub args_json: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_interaction_id: Option<String>,
}

impl ChannelNativeCommandInvocationPayload {
    /// Validates the command name and, when present, the JSON arguments.
    ///
    /// # Errors
    /// Returns a protocol error naming the first invalid field.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.command.as_str(),
            "native_command.command",
            MAX_COMMAND_NAME_BYTES,
        )?;
        if !self.args_json.is_empty() {
            validate_json_bytes(
                self.args_json.as_slice(),
                "native_command.args_json",
                MAX_NATIVE_INVOCATION_BYTES,
            )?;
        }
        validate_optional_field(
            self.native_interaction_id.as_deref(),
            "native_command.native_interaction_id",
            MAX_OPERATION_REASON_BYTES,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChannelCommandArgumentKind, ChannelNativeCommandArgument,
        ChannelNativeCommandInvocationPayload, ChannelNativeCommandSpec,
    };

    #[test]
    fn native_command_spec_requires_enum_values() {
        let spec = ChannelNativeCommandSpec {
            name: "status".to_owned(),
            description: "Show status".to_owned(),
            policy_action: "channel.command.status".to_owned(),
            side_effecting: false,
            arguments: vec![ChannelNativeCommandArgument {
                name: "scope".to_owned(),
                kind: ChannelCommandArgumentKind::Enum,
                required: false,
                enum_values: Vec::new(),
                description: None,
            }],
        };

        assert!(spec.validate().is_err());
    }

    #[test]
    fn native_invocation_rejects_invalid_json_args() {
        let payload = ChannelNativeCommandInvocationPayload {
            command: "status".to_owned(),
            args_json: br#"{"scope":"session""#.to_vec(),
            native_interaction_id: None,
        };

        assert!(payload.validate().is_err());
    }
}
