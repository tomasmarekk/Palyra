mod build;
mod config;
pub mod config_system;
pub mod daemon_config_schema;
mod health;
mod ids;
mod net;
pub mod netguard;
pub mod process_runner_input;
pub mod redaction;
mod webhook;
#[cfg(windows)]
pub mod windows_security;
pub mod workspace_patch;

pub use build::{build_metadata, BuildMetadata};
pub use config::{
    default_config_search_paths, default_identity_store_root, default_identity_store_root_from_env,
    default_state_root, default_state_root_from_env, parse_config_path, ConfigPathParseError,
    IdentityStorePathError,
};
pub use health::{health_response, HealthResponse};
pub use ids::{validate_canonical_id, CanonicalIdError};
pub use net::parse_daemon_bind_socket;
pub use webhook::{
    parse_webhook_payload, verify_webhook_payload, ReplayNonceStore, ReplayProtection,
    WebhookEnvelope, WebhookPayloadError, WebhookSignatureVerifier,
};

pub const CANONICAL_PROTOCOL_MAJOR: u32 = 1;
pub const CANONICAL_JSON_ENVELOPE_VERSION: u32 = 1;

#[cfg(test)]
mod tests;
