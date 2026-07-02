//! Error envelope wire shape and the client-side error type.
//!
//! [`ErrorEnvelope`] mirrors the JSON error body the daemon returns for every
//! non-success status; [`ControlPlaneClientError`] is what the client surfaces
//! to callers, carrying the parsed envelope when one was present.

use palyra_common::runtime_contracts::{
    PalyraErrorCategory, PalyraErrorEnvelope, PalyraValidationIssue,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

impl From<PalyraErrorEnvelope> for ErrorEnvelope {
    fn from(envelope: PalyraErrorEnvelope) -> Self {
        Self {
            error: envelope.message,
            code: envelope.code,
            category: control_plane_category_from_palyra(envelope.category),
            retryable: envelope.retryable,
            redacted: envelope.redacted,
            validation_errors: envelope
                .validation_errors
                .into_iter()
                .map(ValidationIssue::from)
                .collect(),
        }
    }
}

impl From<ErrorEnvelope> for PalyraErrorEnvelope {
    fn from(envelope: ErrorEnvelope) -> Self {
        PalyraErrorEnvelope::new(
            palyra_category_from_control_plane(&envelope.category),
            envelope.code,
            envelope.error,
            "inspect the response code and retry only when retryable is true",
            envelope.retryable,
            envelope.redacted,
        )
        .with_validation_errors(
            envelope.validation_errors.into_iter().map(PalyraValidationIssue::from).collect(),
        )
    }
}

impl From<PalyraValidationIssue> for ValidationIssue {
    fn from(issue: PalyraValidationIssue) -> Self {
        Self { field: issue.field, code: issue.code, message: issue.message }
    }
}

impl From<ValidationIssue> for PalyraValidationIssue {
    fn from(issue: ValidationIssue) -> Self {
        Self { field: issue.field, code: issue.code, message: issue.message }
    }
}

fn control_plane_category_from_palyra(category: PalyraErrorCategory) -> ErrorCategory {
    match category {
        PalyraErrorCategory::Auth => ErrorCategory::Auth,
        PalyraErrorCategory::Validation => ErrorCategory::Validation,
        PalyraErrorCategory::Policy | PalyraErrorCategory::Approval => ErrorCategory::Policy,
        PalyraErrorCategory::Conflict => ErrorCategory::Conflict,
        PalyraErrorCategory::NotFound => ErrorCategory::NotFound,
        PalyraErrorCategory::RateLimit | PalyraErrorCategory::Availability => {
            ErrorCategory::Availability
        }
        PalyraErrorCategory::Provider
        | PalyraErrorCategory::Sandbox
        | PalyraErrorCategory::Mcp
        | PalyraErrorCategory::ExecutionBackend
        | PalyraErrorCategory::Tool
        | PalyraErrorCategory::Dependency => ErrorCategory::Dependency,
        PalyraErrorCategory::Internal => ErrorCategory::Internal,
    }
}

fn palyra_category_from_control_plane(category: &ErrorCategory) -> PalyraErrorCategory {
    match category {
        ErrorCategory::Auth => PalyraErrorCategory::Auth,
        ErrorCategory::Validation => PalyraErrorCategory::Validation,
        ErrorCategory::Policy => PalyraErrorCategory::Policy,
        ErrorCategory::NotFound => PalyraErrorCategory::NotFound,
        ErrorCategory::Conflict => PalyraErrorCategory::Conflict,
        ErrorCategory::Dependency => PalyraErrorCategory::Dependency,
        ErrorCategory::Availability => PalyraErrorCategory::Availability,
        ErrorCategory::Internal => PalyraErrorCategory::Internal,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_palyra_error_envelope_converts_to_control_plane_shape() {
        let canonical = PalyraErrorEnvelope::new(
            PalyraErrorCategory::Provider,
            "provider/auth_failed",
            "provider credentials were rejected",
            "refresh the provider credential",
            false,
            true,
        )
        .with_validation_errors(vec![PalyraValidationIssue {
            field: "model".to_owned(),
            code: "unsupported_model".to_owned(),
            message: "model is not available".to_owned(),
        }]);
        let envelope = ErrorEnvelope::from(canonical);
        let encoded = serde_json::to_value(&envelope).expect("envelope should serialize");

        assert_eq!(encoded["error"], "provider credentials were rejected");
        assert_eq!(encoded["code"], "provider/auth_failed");
        assert_eq!(encoded["category"], "dependency");
        assert_eq!(encoded["redacted"], true);
        assert_eq!(encoded["validation_errors"][0]["field"], "model");
    }
}
