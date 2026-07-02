//! Shared foundation crate for the Palyra workspace.
//!
//! Hosts cross-service primitives with no daemon dependencies: configuration and path
//! resolution, canonical IDs, secret redaction and references, network/SSRF guards,
//! webhook envelope parsing, tool catalog metadata, versioned JSON migration, workspace
//! patching, replay bundles, and runtime contracts. Security-sensitive parsers here are
//! fuzzed from `fuzz/fuzz_targets/` and pinned by golden fixtures.

mod build;
mod config;
pub mod config_system;
pub mod context_references;
pub mod daemon_config_schema;
pub mod deployment_profiles;
pub mod feature_rollouts;
mod health;
mod ids;
pub mod local_runtime_ports;
mod net;
pub mod netguard;
pub mod process_risk;
pub mod process_runner_input;
pub mod project_context;
pub mod qa_scenarios;
pub mod redaction;
pub mod release_evals;
pub mod replay_bundle;
pub mod runtime_contracts;
pub mod runtime_preview;
pub mod runtime_roadmap;
pub mod secret_refs;
pub mod security_posture;
pub mod tool_catalog;
pub mod versioned_json;
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
pub use health::{
    health_response, highest_state_health_severity, HealthResponse, StateHealthEvidenceRef,
    StateHealthFinding, StateHealthSeverity,
};
pub use ids::{validate_canonical_id, CanonicalIdError};
pub use net::parse_daemon_bind_socket;
pub use webhook::{
    parse_webhook_payload, verify_webhook_payload, ReplayNonceStore, ReplayProtection,
    WebhookEnvelope, WebhookPayloadError, WebhookSignatureVerifier,
};

/// Major version of the canonical protobuf protocol (`palyra.*.v1`).
pub const CANONICAL_PROTOCOL_MAJOR: u32 = 1;
/// Pinned `v` value required in every canonical JSON envelope.
pub const CANONICAL_JSON_ENVELOPE_VERSION: u32 = 1;

#[cfg(test)]
mod tests;
