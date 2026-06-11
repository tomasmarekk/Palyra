//! Shared-token authorization for the browserd gRPC surface.
//!
//! Verifies the request authorization metadata against the daemon's optional
//! auth token using a constant-time comparison. When no token is configured,
//! bootstrap restricts the listeners to loopback instead.

use crate::*;

impl BrowserRuntimeState {
    /// Authorizes a gRPC request against the daemon's optional shared auth token.
    ///
    /// When no token is configured every request is accepted; that mode is only
    /// reachable on loopback binds (enforced at startup).
    ///
    /// # Errors
    /// Returns `Status::unauthenticated` when a token is configured and the
    /// request's authorization metadata is missing or does not match.
    pub(crate) async fn authorize(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<(), Status> {
        let Some(expected_token) = self.auth_token.as_ref() else {
            return Ok(());
        };
        let supplied = metadata
            .get(AUTHORIZATION_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let expected = format!("Bearer {expected_token}");
        if !constant_time_eq_bytes(supplied.trim().as_bytes(), expected.as_bytes()) {
            return Err(Status::unauthenticated("missing or invalid browser service token"));
        }
        Ok(())
    }
}

/// Compares two byte slices in time independent of where they first differ.
///
/// Both inputs are always scanned to the longer length and a length mismatch
/// only flips bits in the accumulator, so there is no early return an attacker
/// could time to recover the expected token byte by byte.
pub(crate) fn constant_time_eq_bytes(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}
