//! Error envelope wire shape and the client-side error type.
//!
//! [`ErrorEnvelope`] mirrors the JSON error body the daemon returns for every
//! non-success status; [`ControlPlaneClientError`] is what the client surfaces
//! to callers, carrying the parsed envelope when one was present.

use thiserror::Error;

use serde::{Deserialize, Serialize};

/// Coarse failure classification shared across all control-plane surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Auth,
    Validation,
    Policy,
    NotFound,
    Conflict,
    Dependency,
    Availability,
    Internal,
}

/// One field-level validation failure inside an [`ErrorEnvelope`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationIssue {
    /// Path of the offending request field.
    pub field: String,
    /// Stable machine-readable issue code.
    pub code: String,
    /// Human-readable description of the issue.
    pub message: String,
}

/// Canonical JSON error body returned by the daemon for non-success responses.
///
/// `redacted` and `validation_errors` fall back to defaults when absent so
/// envelopes from older daemons keep decoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    /// Human-readable error message.
    pub error: String,
    /// Stable machine-readable error code.
    pub code: String,
    /// Coarse failure classification.
    pub category: ErrorCategory,
    /// Whether the caller may retry the same request.
    pub retryable: bool,
    /// Whether sensitive detail was stripped from the message.
    #[serde(default)]
    pub redacted: bool,
    /// Field-level issues; populated for [`ErrorCategory::Validation`] failures.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validation_errors: Vec<ValidationIssue>,
}

impl ErrorEnvelope {
    /// Returns the human-readable error message.
    #[must_use]
    pub fn message(&self) -> &str {
        self.error.as_str()
    }
}

/// Failures surfaced by [`ControlPlaneClient`](crate::ControlPlaneClient) operations.
#[derive(Debug, Error)]
pub enum ControlPlaneClientError {
    /// The configured base URL (or a path joined onto it) could not be parsed.
    #[error("invalid control-plane base URL: {0}")]
    InvalidBaseUrl(String),
    /// The underlying HTTP client could not be constructed.
    #[error("HTTP client initialization failed: {0}")]
    ClientInit(String),
    /// The request could not be sent or no response arrived.
    #[error("request failed: {0}")]
    Transport(String),
    /// The daemon answered with a non-success status; `envelope` is present when
    /// the body parsed as an [`ErrorEnvelope`].
    #[error("request failed with HTTP {status}: {message}")]
    Http { status: u16, message: String, envelope: Option<ErrorEnvelope> },
    /// The response body could not be read or did not match the expected shape.
    #[error("response decoding failed: {0}")]
    Decode(String),
}
