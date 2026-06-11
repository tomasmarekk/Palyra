//! Closed enumerations describing connector identity and health.
//!
//! The snake_case labels produced by `as_str`/`parse` are persisted in sqlite
//! and exposed over console APIs, so they must stay stable; `parse` accepts
//! the same labels case-insensitively.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

/// Supported connector provider families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorKind {
    Echo,
    Discord,
    Telegram,
    Slack,
}

impl ConnectorKind {
    /// Returns the stable snake_case label matching the serde encoding.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Echo => "echo",
            Self::Discord => "discord",
            Self::Telegram => "telegram",
            Self::Slack => "slack",
        }
    }

    /// Parses a stored label back into the enum; `None` for unknown values.
    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "echo" => Some(Self::Echo),
            "discord" => Some(Self::Discord),
            "telegram" => Some(Self::Telegram),
            "slack" => Some(Self::Slack),
            _ => None,
        }
    }
}

impl Display for ConnectorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Product-level support tier of a connector kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorAvailability {
    Supported,
    InternalTestOnly,
    Deferred,
}

impl ConnectorAvailability {
    /// Returns the stable snake_case label matching the serde encoding.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::InternalTestOnly => "internal_test_only",
            Self::Deferred => "deferred",
        }
    }

    /// Parses a stored label back into the enum; `None` for unknown values.
    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "supported" => Some(Self::Supported),
            "internal_test_only" => Some(Self::InternalTestOnly),
            "deferred" => Some(Self::Deferred),
            _ => None,
        }
    }
}

impl Display for ConnectorAvailability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a connector instance is configured well enough to deliver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorReadiness {
    Ready,
    MissingCredential,
    AuthFailed,
    Misconfigured,
}

impl ConnectorReadiness {
    /// Returns the stable snake_case label matching the serde encoding.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::MissingCredential => "missing_credential",
            Self::AuthFailed => "auth_failed",
            Self::Misconfigured => "misconfigured",
        }
    }

    /// Parses a stored label back into the enum; `None` for unknown values.
    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "ready" => Some(Self::Ready),
            "missing_credential" => Some(Self::MissingCredential),
            "auth_failed" => Some(Self::AuthFailed),
            "misconfigured" => Some(Self::Misconfigured),
            _ => None,
        }
    }
}

/// Runtime lifecycle state of a connector instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorLiveness {
    Stopped,
    Running,
    Restarting,
    Crashed,
}

impl ConnectorLiveness {
    /// Returns the stable snake_case label matching the serde encoding.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Running => "running",
            Self::Restarting => "restarting",
            Self::Crashed => "crashed",
        }
    }

    /// Parses a stored label back into the enum; `None` for unknown values.
    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "stopped" => Some(Self::Stopped),
            "running" => Some(Self::Running),
            "restarting" => Some(Self::Restarting),
            "crashed" => Some(Self::Crashed),
            _ => None,
        }
    }
}
