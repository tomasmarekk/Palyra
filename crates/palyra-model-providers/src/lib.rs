//! Provider-domain contracts shared by Palyra daemon and operator surfaces.
//!
//! This crate is intentionally independent from `palyra-daemon`, `palyra-cli`,
//! and app crates. It owns provider-neutral request/output contracts and
//! security projections that must stay identical across runtime surfaces.

pub mod config;
pub mod contract;
pub mod discovery;
pub mod error_envelope;
pub mod errors;
pub mod providers;
pub mod snapshots;
pub mod streaming;
pub mod tool_repair;

mod redaction;

pub use config::{
    capability_defaults_for_kind, capability_defaults_for_provider, configured_model_id,
    configured_model_ids_by_provider, default_provider_id_for_configured_models,
    legacy_provider_id_for_file_config_kind, legacy_provider_identity_for_file_config_kind,
    normalized_provider_filter_alias, preserved_unresolved_default_chat_model_for_provider,
    validate_model_provider_config, validate_openai_base_url_network_policy,
    validate_openai_base_url_network_policy_with_resolver, ConfiguredProviderModelIds,
    ModelProviderAuthProviderKind, ModelProviderConfig, ModelProviderCredentialSource,
    ModelProviderKind, ModelProviderRegistryConfig, ProviderCapabilitiesSnapshot, ProviderCostTier,
    ProviderLatencyTier, ProviderMetadataSource, ProviderModelEntryConfig, ProviderModelRole,
    ProviderRegistryEntryConfig, DEFAULT_MODEL_PROVIDER_REQUEST_TIMEOUT_MS,
    DEFAULT_PROVIDER_DISCOVERY_TTL_MS, DEFAULT_PROVIDER_HEALTH_TTL_MS,
    DEFAULT_PROVIDER_RESPONSE_CACHE_MAX_ENTRIES, DEFAULT_PROVIDER_RESPONSE_CACHE_TTL_MS,
};
pub use contract::{
    append_provider_text_with_hard_limit, bounded_provider_turn_output_for_persistence,
    model_id_supports_reasoning_effort, provider_events_from_output, provider_request_has_vision,
    AudioTranscriptionRequest, AudioTranscriptionResponse, AudioTranscriptionSegment,
    EmbeddingsRequest, EmbeddingsResponse, PromptCachePolicy, PromptCacheReport,
    PromptCacheStrategy, ProviderAttemptSummary, ProviderEvent, ProviderFinishReason,
    ProviderImageInput, ProviderMessage, ProviderMessageContentPart, ProviderMessageRole,
    ProviderMessageToolCall, ProviderOutputContentPart, ProviderPromptCacheHint,
    ProviderPromptSegment, ProviderPromptSegmentKind, ProviderRawProviderRefs,
    ProviderReasoningEffort, ProviderRedactionState, ProviderRequest, ProviderResponse,
    ProviderServiceTier, ProviderTurnOutput, ProviderUsage,
    DEFAULT_PROVIDER_STREAM_EVENT_TOKEN_CHUNK_SIZE, MAX_PROVIDER_TURN_TEXT_BYTES,
};
pub use discovery::{
    is_openai_chatgpt_oauth_client_id, parse_discovered_model_ids,
    parse_discovered_provider_models, provider_models_endpoint, provider_models_endpoint_for_probe,
    select_preferred_discovered_model, select_preferred_discovered_model_id,
    select_preferred_discovered_model_id_from_response,
    select_preferred_tool_capable_discovered_model,
    select_preferred_tool_capable_discovered_model_id,
    select_preferred_tool_capable_discovered_model_id_from_response, DiscoveredProviderModel,
    ProviderModelsEndpoint, ProviderModelsResponseFormat, ANTHROPIC_API_VERSION,
    OPENAI_CHATGPT_OAUTH_CLIENT_ID, OPENAI_CODEX_BACKEND_BASE_URL, OPENAI_CODEX_MODELS_ENDPOINT,
};
pub use error_envelope::{
    ProviderErrorEnvelope, ProviderErrorKind, ProviderErrorSeverity, ProviderRecoveryDecision,
    ProviderRecoveryDecisionKind, ProviderRetryability, PROVIDER_RECOVERY_DECISION_EVENT,
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
pub use providers::{
    anthropic_compatible_uses_anthropic_oauth_headers, anthropic_compatible_uses_bearer_auth,
    anthropic_messages_payload, coerce_raw_tool_call_markup, normalize_tool_arguments,
    normalize_tool_input_value, openai_chat_completions_payload, openai_responses_payload,
    openai_responses_tool_wire_name_map_from_tools, OpenAiResponsesPayload,
    RawToolCallMarkupExtraction, MAX_TOOL_ARGUMENT_BYTES,
};
pub use redaction::sanitize_remote_error;
pub use snapshots::{
    ProviderCircuitBreakerSnapshot, ProviderCredentialCapabilitySummary, ProviderDiscoverySnapshot,
    ProviderHealthProbeSnapshot, ProviderRegistryCredentialSnapshot, ProviderRegistryModelSnapshot,
    ProviderRegistryProviderSnapshot, ProviderRegistrySnapshot, ProviderResponseCacheSnapshot,
    ProviderRetryPolicySnapshot, ProviderRouteCandidateTrace, ProviderRouteSelectionTrace,
    ProviderRuntimeMetricsSnapshot, ProviderStatusSnapshot,
};
pub use streaming::{
    provider_output_from_text_and_tools, ProviderStreamAccumulator, ProviderStreamEvent,
};
pub use tool_repair::{
    decide_tool_repair_candidate, normalize_assistant_output_for_tool_repair,
    tool_repair_audit_events_for_decision, NormalizedAssistantOutput, ProviderNeutralStreamEvent,
    ProviderStreamSegment, ToolProposalCandidate, ToolRepairAuditEvent, ToolRepairBoundary,
    ToolRepairBoundaryState, ToolRepairCandidate, ToolRepairCandidateFormat, ToolRepairDecision,
    ToolRepairDecisionStatus, ToolRepairStreamNormalizer, DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
    PROVIDER_STREAM_NORMALIZED_EVENT, PROVIDER_STREAM_REPAIR_BOUNDARY_CLOSED_EVENT,
    TOOL_REPAIR_ACCEPTED_EVENT, TOOL_REPAIR_CANDIDATE_DETECTED_EVENT, TOOL_REPAIR_REJECTED_EVENT,
};
