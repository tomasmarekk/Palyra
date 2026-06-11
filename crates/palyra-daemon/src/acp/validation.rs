//! Input validation helpers for the ACP runtime: state-root path hardening
//! and scope-list normalization. Split out of `acp::mod` so the security
//! checks stay small and individually testable.

use std::path::{Component, Path, PathBuf};

use super::{normalize_text, AcpRuntimeError, AcpRuntimeResult};

const MAX_ACP_SCOPE_COUNT: usize = 128;

/// Validates and absolutizes the configured ACP state root.
///
/// Parent-directory components are rejected (rather than resolved) so a
/// hostile or mistyped configuration cannot point ACP state outside the
/// daemon state tree.
///
/// # Errors
/// Returns an invalid-field error for an empty root or one containing `..`
/// components, and an IO error when the current directory cannot be resolved
/// for a relative root.
pub(super) fn normalize_state_root(root: &Path) -> AcpRuntimeResult<PathBuf> {
    if root.as_os_str().is_empty() {
        return Err(AcpRuntimeError::InvalidField {
            field: "state_root",
            message: "ACP state root cannot be empty".to_owned(),
        });
    }
    if root.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(AcpRuntimeError::InvalidField {
            field: "state_root",
            message: "ACP state root cannot contain parent directory traversal components"
                .to_owned(),
        });
    }
    if root.is_absolute() {
        return Ok(root.to_path_buf());
    }
    let current_dir = std::env::current_dir().map_err(|source| AcpRuntimeError::Io {
        operation: "resolve_current_dir",
        path: root.to_path_buf(),
        source,
    })?;
    Ok(current_dir.join(root))
}

/// Normalizes a free-form scope list: trims, validates each entry, then sorts
/// and deduplicates. The count cap is enforced before normalization so an
/// oversized list is rejected instead of silently shrunk by dedup.
///
/// # Errors
/// Returns an invalid-field error when the list exceeds the cap or any scope
/// is empty, oversized, or contains control characters.
pub(super) fn normalize_scope_strings(scopes: Vec<String>) -> AcpRuntimeResult<Vec<String>> {
    if scopes.len() > MAX_ACP_SCOPE_COUNT {
        return Err(AcpRuntimeError::InvalidField {
            field: "scopes",
            message: format!("scope list exceeds {MAX_ACP_SCOPE_COUNT} entries"),
        });
    }
    let mut normalized = Vec::new();
    for scope in scopes {
        normalized.push(normalize_text(scope.as_str(), "scope", 128)?);
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}
