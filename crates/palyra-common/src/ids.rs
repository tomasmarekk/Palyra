//! Validation for canonical Palyra identifiers (ULID strings).
//!
//! Canonical IDs are 26-character uppercase Crockford Base32 ULIDs; every untrusted ID
//! crossing a protocol boundary (e.g. webhook envelopes) is validated here.

use thiserror::Error;

/// Why a candidate canonical ID was rejected.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CanonicalIdError {
    #[error("canonical ID must be exactly 26 characters")]
    InvalidLength,
    #[error("canonical ID contains invalid character '{0}'")]
    InvalidCharacter(char),
}

/// Validates that `input` is a canonical 26-character uppercase Crockford Base32 ULID.
///
/// # Errors
/// Returns [`CanonicalIdError::InvalidLength`] for any other length and
/// [`CanonicalIdError::InvalidCharacter`] for characters outside the alphabet
/// (Crockford Base32 excludes I, L, O, and U).
pub fn validate_canonical_id(input: &str) -> Result<(), CanonicalIdError> {
    if input.len() != 26 {
        return Err(CanonicalIdError::InvalidLength);
    }
    for ch in input.chars() {
        if !is_valid_crockford_char(ch) {
            return Err(CanonicalIdError::InvalidCharacter(ch));
        }
    }
    Ok(())
}

fn is_valid_crockford_char(ch: char) -> bool {
    ch.is_ascii_digit() || matches!(ch, 'A'..='H' | 'J'..='K' | 'M'..='N' | 'P'..='T' | 'V'..='Z')
}
