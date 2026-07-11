//! Private launch-handshake schema and its structural validation.
//!
//! Filesystem confinement and permission checks remain caller responsibilities.

use std::{
    error::Error,
    fmt,
    path::{Component, Path},
};

use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

use super::{is_bounded_actor, MAX_PRIVATE_PATH_BYTES, QA_FAULT_LAUNCH_SCHEMA_VERSION};

/// Private runner-to-daemon launch handshake for one isolated QA process.
///
/// The capability itself is deliberately absent. The document carries only
/// its digest and the runner passes the capability through a separate
/// owner-restricted file named by
/// [`QA_FAULT_CAPABILITY_PATH_ENV`](super::QA_FAULT_CAPABILITY_PATH_ENV).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultLaunchDocument {
    /// Handshake schema version.
    pub schema_version: u32,
    /// Bounded launch identifier used to prevent cross-run reuse.
    pub launch_id: String,
    /// Absolute private path to the validated plan document.
    pub plan_path: String,
    /// Canonical fault-plan digest expected by both processes.
    pub plan_sha256: String,
    /// SHA-256 digest of the separately stored launch capability.
    pub capability_sha256: String,
    /// Absolute private path for append-only activation evidence.
    pub evidence_path: String,
    /// Absolute expiration instant for replay prevention.
    pub expires_at_unix_ms: i64,
}

impl QaFaultLaunchDocument {
    /// Validates the private launch contract without reading either file.
    ///
    /// Callers must additionally confine both paths beneath their isolated
    /// state root and verify owner-only filesystem permissions.
    ///
    /// # Errors
    /// Returns every structural security issue found in the document.
    pub fn validate(&self) -> Result<(), QaFaultLaunchValidationError> {
        let mut issues = Vec::new();
        if self.schema_version != QA_FAULT_LAUNCH_SCHEMA_VERSION {
            push_launch_issue(
                &mut issues,
                "unsupported_schema_version",
                "$.schema_version",
                format!(
                    "expected schema_version {QA_FAULT_LAUNCH_SCHEMA_VERSION}, got {}",
                    self.schema_version
                ),
            );
        }
        if !is_bounded_actor(self.launch_id.as_str()) {
            push_launch_issue(
                &mut issues,
                "invalid_launch_id",
                "$.launch_id",
                "launch_id must be a bounded non-secret ASCII identifier",
            );
        }
        validate_private_absolute_path(self.plan_path.as_str(), "$.plan_path", &mut issues);
        validate_private_absolute_path(self.evidence_path.as_str(), "$.evidence_path", &mut issues);
        if self.plan_path == self.evidence_path {
            push_launch_issue(
                &mut issues,
                "launch_paths_collide",
                "$.evidence_path",
                "plan_path and evidence_path must be different files",
            );
        }
        validate_sha256(
            self.plan_sha256.as_str(),
            "$.plan_sha256",
            "invalid_plan_sha256",
            &mut issues,
        );
        validate_sha256(
            self.capability_sha256.as_str(),
            "$.capability_sha256",
            "invalid_capability_sha256",
            &mut issues,
        );
        if self.expires_at_unix_ms <= 0 {
            push_launch_issue(
                &mut issues,
                "invalid_expiration",
                "$.expires_at_unix_ms",
                "expires_at_unix_ms must be a positive absolute timestamp",
            );
        }
        if issues.is_empty() {
            Ok(())
        } else {
            Err(QaFaultLaunchValidationError::new(issues))
        }
    }
}

/// One path-qualified private launch-document issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaFaultLaunchIssue {
    /// Stable machine-readable reason code.
    pub code: String,
    /// JSON path to the invalid field.
    pub path: String,
    /// Bounded operator-facing explanation.
    pub message: String,
}

/// Collection of launch-document security issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaFaultLaunchValidationError {
    issues: Vec<QaFaultLaunchIssue>,
}

impl QaFaultLaunchValidationError {
    fn new(issues: Vec<QaFaultLaunchIssue>) -> Self {
        debug_assert!(!issues.is_empty());
        Self { issues }
    }

    /// Returns every collected issue.
    #[must_use]
    pub fn issues(&self) -> &[QaFaultLaunchIssue] {
        self.issues.as_slice()
    }
}

impl fmt::Display for QaFaultLaunchValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(issue) = self.issues.first() {
            write!(formatter, "{} at {}: {}", issue.code, issue.path, issue.message)
        } else {
            formatter.write_str("QA fault launch validation failed")
        }
    }
}

impl Error for QaFaultLaunchValidationError {}

/// Strictly decodes and validates a private launch document from JSON.
///
/// # Errors
/// Returns syntax errors for unknown or malformed fields and semantic errors
/// for invalid versions, identifiers, paths, digests, or expiration.
pub fn parse_qa_fault_launch_document_json(
    bytes: &[u8],
) -> Result<QaFaultLaunchDocument, QaFaultLaunchParseError> {
    let document = serde_json::from_slice::<QaFaultLaunchDocument>(bytes)
        .map_err(QaFaultLaunchParseError::Parse)?;
    document.validate().map_err(QaFaultLaunchParseError::Invalid)?;
    Ok(document)
}

/// Private launch-document decode or semantic validation failure.
#[derive(Debug, ThisError)]
pub enum QaFaultLaunchParseError {
    /// Strict JSON decoding failed.
    #[error("failed to parse QA fault launch document: {0}")]
    Parse(#[source] serde_json::Error),
    /// Decoding succeeded but security validation failed.
    #[error("invalid QA fault launch document: {0}")]
    Invalid(#[source] QaFaultLaunchValidationError),
}

fn validate_private_absolute_path(value: &str, path: &str, issues: &mut Vec<QaFaultLaunchIssue>) {
    let candidate = Path::new(value);
    if value.is_empty()
        || value.len() > MAX_PRIVATE_PATH_BYTES
        || value.contains('\0')
        || !candidate.is_absolute()
        || candidate
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        push_launch_issue(
            issues,
            "invalid_private_path",
            path,
            "path must be bounded, absolute, normalized, and free of traversal components",
        );
    }
}

fn validate_sha256(value: &str, path: &str, code: &str, issues: &mut Vec<QaFaultLaunchIssue>) {
    if !is_lowercase_sha256(value) {
        push_launch_issue(
            issues,
            code,
            path,
            "digest must contain exactly 64 lowercase hexadecimal characters",
        );
    }
}

pub(super) fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn push_launch_issue(
    issues: &mut Vec<QaFaultLaunchIssue>,
    code: impl Into<String>,
    path: impl Into<String>,
    message: impl Into<String>,
) {
    issues.push(QaFaultLaunchIssue {
        code: code.into(),
        path: path.into(),
        message: message.into(),
    });
}
