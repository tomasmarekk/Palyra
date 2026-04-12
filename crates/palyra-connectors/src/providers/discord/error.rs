use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DiscordSemanticsError {
    #[error("discord account_id cannot be empty")]
    EmptyAccountId,
    #[error("discord account_id contains unsupported characters")]
    InvalidAccountId,
    #[error("discord test target cannot be empty")]
    EmptyTarget,
    #[error("discord test target contains unsupported characters")]
    InvalidTarget,
}
