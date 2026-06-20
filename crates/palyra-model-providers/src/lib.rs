//! Provider-domain contracts shared by Palyra daemon and operator surfaces.
//!
//! This crate is intentionally independent from `palyra-daemon`, `palyra-cli`,
//! and app crates. It owns provider-neutral request/output contracts and
//! security projections that must stay identical across runtime surfaces.

pub mod config;
pub mod contract;
pub mod error_envelope;
pub mod errors;
pub mod streaming;

mod redaction;

pub use config::{
    capability_defaults_for_kind, capability_defaults_for_provider, configured_model_id,
    validate_openai_base_url_network_policy, validate_openai_base_url_network_policy_with_resolver,
    ModelProviderAuthProviderKind, ModelProviderConfig, ModelProviderCredentialSource,
    ModelProviderKind, ModelProviderRegistryConfig, ProviderCapabilitiesSnapshot, ProviderCostTier,
    ProviderLatencyTier, ProviderMetadataSource, ProviderModelEntryConfig, ProviderModelRole,
    ProviderRegistryEntryConfig, DEFAULT_MODEL_PROVIDER_REQUEST_TIMEOUT_MS,
    DEFAULT_PROVIDER_DISCOVERY_TTL_MS, DEFAULT_PROVIDER_HEALTH_TTL_MS,
    DEFAULT_PROVIDER_RESPONSE_CACHE_MAX_ENTRIES, DEFAULT_PROVIDER_RESPONSE_CACHE_TTL_MS,
};
pub use contract::{
    append_provider_text_with_hard_limit, bounded_provider_turn_output_for_persistence,
    provider_events_from_output, provider_request_has_vision, ProviderAttemptSummary,
    ProviderEvent, ProviderFinishReason, ProviderImageInput, ProviderMessage,
    ProviderMessageContentPart, ProviderMessageRole, ProviderMessageToolCall,
    ProviderOutputContentPart, ProviderRawProviderRefs, ProviderRedactionState, ProviderRequest,
    ProviderResponse, ProviderTurnOutput, ProviderUsage,
    DEFAULT_PROVIDER_STREAM_EVENT_TOKEN_CHUNK_SIZE, MAX_PROVIDER_TURN_TEXT_BYTES,
};
pub use error_envelope::{
    ProviderErrorEnvelope, ProviderErrorKind, ProviderErrorSeverity, ProviderRetryability,
};
pub use errors::{
    classify_http_provider_failure, classify_network_provider_failure,
    classify_reqwest_provider_failure, classify_transport_provider_failure,
    fail_closed_provider_classification, failover_provider_classification,
    invalid_response_classification, retry_provider_classification,
    retryable_invalid_response_classification, user_action_provider_classification, ProviderError,
    ProviderFailureAction, ProviderFailureCategory, ProviderFailureClass,
    ProviderFailureClassification, ProviderFailureSnapshot, ProviderRecoveryAction,
    ProviderRecoveryPlanSnapshot,
};
pub use redaction::sanitize_remote_error;
pub use streaming::{
    provider_output_from_text_and_tools, ProviderStreamAccumulator, ProviderStreamEvent,
};
