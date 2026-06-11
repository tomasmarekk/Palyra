//! Error type for Discord-owned identity and target semantics.
//!
//! Raised by the normalization helpers in `ids` and `normalize`; the variants are stable
//! contract surface matched by the daemon and CLI when reporting invalid operator input.

use thiserror::Error;

/// Validation failure for Discord account ids and conversation targets.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DiscordSemanticsError {
    /// The account id was blank after trimming.
    #[error("discord account_id cannot be empty")]
    EmptyAccountId,
    /// The account id contained characters outside the supported set.
    #[error("discord account_id contains unsupported characters")]
    InvalidAccountId,
    /// The conversation target was blank after trimming.
    #[error("discord test target cannot be empty")]
    EmptyTarget,
    /// The conversation target contained characters outside the supported set.
    #[error("discord test target contains unsupported characters")]
    InvalidTarget,
}
