//! OpenAI-compatible HTTP handlers backed by the Palyra gateway runtime.
//!
//! This module translates `/v1/*` compatibility requests into authenticated
//! Palyra sessions and run-stream calls. JSON shapes and error codes are
//! externally visible compatibility contract; keep behavior changes paired with
//! parity fixtures and client updates.

use crate::{
    access_control::{
        AccessRegistry, AccessRegistryError, AuthenticatedApiToken, FeatureFlagRecord,
        FEATURE_API_TOKENS, FEATURE_COMPAT_API, FEATURE_COMPAT_EMBEDDINGS_API,
        FEATURE_COMPAT_TOOLS_INVOKE, PERMISSION_COMPAT_CHAT_CREATE,
        PERMISSION_COMPAT_EMBEDDINGS_CREATE, PERMISSION_COMPAT_MODELS_READ,
        PERMISSION_COMPAT_RESPONSES_CREATE, PERMISSION_COMPAT_TOOLS_INVOKE,
    },
    app::state::CompatApiRateLimitEntry,
    *,
};
use palyra_common::{
    runtime_contracts::{IdempotencyReplayDecision, StableErrorEnvelope},
    runtime_preview::{
        RuntimeDecisionActor, RuntimeDecisionActorKind, RuntimeDecisionEventType,
        RuntimeDecisionPayload, RuntimeDecisionTiming, RuntimeEntityRef,
    },
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};

const COMPAT_API_CHANNEL: &str = "compat-api";
const COMPAT_IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const COMPAT_RESPONSE_IDEMPOTENCY_TTL_MS: i64 = 24 * 60 * 60 * 1_000;
const COMPAT_RESPONSE_RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const COMPAT_RUN_IDEMPOTENCY_TTL_MS: i64 = 24 * 60 * 60 * 1_000;
const COMPAT_RUN_EVENTS_PAGE_LIMIT_DEFAULT: usize = 128;
const COMPAT_RUN_EVENTS_PAGE_LIMIT_MAX: usize = 512;
const COMPAT_SSE_CHANNEL_CAPACITY: usize = 32;
const COMPAT_SSE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const COMPAT_SSE_SEND_TIMEOUT: Duration = Duration::from_secs(5);

struct CompatHttpError(Box<Response>);

impl From<Response> for CompatHttpError {
    fn from(response: Response) -> Self {
        Self(Box::new(response))
    }
}

impl From<CompatHttpError> for Response {
    fn from(error: CompatHttpError) -> Self {
        *error.0
    }
}

type CompatHttpResult<T> = Result<T, CompatHttpError>;

/// Request body for the compatibility chat-completions endpoint.
#[derive(Debug, Deserialize)]
pub(crate) struct CompatChatCompletionsRequest {
    model: Option<String>,
    messages: Vec<CompatChatMessage>,
    stream: Option<bool>,
    cancel_on_disconnect: Option<bool>,
    user: Option<String>,
    metadata: Option<Value>,
}

/// Request body for the compatibility responses endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct CompatResponsesRequest {
    model: Option<String>,
    input: CompatResponsesInput,
    stream: Option<bool>,
    user: Option<String>,
    metadata: Option<Value>,
}

/// Request body for the public runs endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct CompatRunsCreateRequest {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    input: Option<CompatResponsesInput>,
    #[serde(default)]
    messages: Option<Vec<CompatChatMessage>>,
    #[serde(default)]
    instructions: Option<String>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    session: Option<CompatRunsSessionRequest>,
    #[serde(default)]
    tools: Option<Value>,
    #[serde(default)]
    tool_exposure_policy: Option<String>,
    #[serde(default)]
    metadata: Option<Value>,
}

/// Query parameters for creating a public run.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct CompatRunsCreateQuery {
    #[serde(default)]
    mode: Option<String>,
}

/// Optional session selector for the public runs endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct CompatRunsSessionRequest {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    require_existing: Option<bool>,
    #[serde(default)]
    reset: Option<bool>,
}

/// Query parameters for replaying a run's public event stream.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CompatRunEventsQuery {
    #[serde(default)]
    after_seq: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
}

/// Request body for waiting on a public run.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct CompatRunWaitRequest {
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    return_on_waiting: Option<bool>,
}

/// Request body for stopping a public run.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CompatRunStopRequest {
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    cleanup_policy: Option<String>,
}

/// Request body for detaching from a public run stream.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CompatRunDetachRequest {
    #[serde(default)]
    reason: Option<String>,
}

/// Request body for resolving a run approval through the public API.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CompatRunApprovalRequest {
    #[serde(default)]
    approval_id: Option<String>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    decision: Option<String>,
    #[serde(default)]
    approved: Option<bool>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    decision_scope: Option<String>,
    #[serde(default)]
    decision_scope_ttl_ms: Option<i64>,
    #[serde(default)]
    expected_version: Option<u64>,
}

/// Request body for the compatibility embeddings endpoint.
#[derive(Debug, Deserialize)]
pub(crate) struct CompatEmbeddingsRequest {
    model: Option<String>,
    input: CompatEmbeddingsInput,
    #[serde(default)]
    encoding_format: Option<String>,
    #[serde(default)]
    dimensions: Option<u32>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    metadata: Option<Value>,
}

/// One chat-style message accepted by compatibility chat and responses inputs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct CompatChatMessage {
    role: String,
    content: CompatMessageContent,
    #[serde(default)]
    name: Option<String>,
}

/// Input forms accepted by the compatibility responses endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum CompatResponsesInput {
    Text(String),
    Messages(Vec<CompatResponseInputItem>),
}

/// Input forms accepted by the compatibility embeddings endpoint.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum CompatEmbeddingsInput {
    Text(String),
    Texts(Vec<String>),
}

/// One structured response-input message item.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct CompatResponseInputItem {
    role: Option<String>,
    content: CompatMessageContent,
    #[serde(default)]
    name: Option<String>,
}

/// Text-bearing message content accepted by compatibility request formats.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum CompatMessageContent {
    Text(String),
    Parts(Vec<CompatMessagePart>),
    Json(Value),
}

/// One typed content part inside a compatibility message.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct CompatMessagePart {
    #[serde(rename = "type")]
    kind: String,
    text: Option<String>,
    input_text: Option<String>,
}

#[derive(Debug, Default)]
struct CompatRequestOverrides {
    session_id: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: bool,
    reset_session: bool,
}

#[derive(Debug)]
struct CompatAuthorizedRunContext {
    token: AuthenticatedApiToken,
    provider_kind: String,
    model_name: String,
    overrides: CompatRequestOverrides,
    principal: String,
    device_id: String,
}

#[derive(Debug)]
struct CompatPreparedRun {
    token: AuthenticatedApiToken,
    provider_kind: String,
    model_name: String,
    run_id: String,
    session_id: String,
    principal: String,
    device_id: String,
    created_at_unix_ms: i64,
    request_sender: mpsc::Sender<common_v1::RunStreamRequest>,
    run_request: TonicRequest<ReceiverStream<common_v1::RunStreamRequest>>,
}

#[derive(Debug, Clone)]
struct CompatResponseIdempotencyReservation {
    storage_key: String,
}

#[derive(Debug)]
enum CompatResponseIdempotencyBegin {
    Reserved(CompatResponseIdempotencyReservation),
    Replay(Value),
}

#[derive(Debug, Clone)]
struct CompatRunIdempotencyReservation {
    storage_key: String,
}

#[derive(Debug)]
enum CompatRunIdempotencyBegin {
    Reserved(CompatRunIdempotencyReservation),
    Replay(Value),
}

#[derive(Debug, Clone)]
struct CompatResponsePersistRequest {
    response_id: String,
    session_id: String,
    run_id: String,
    owner_principal: String,
    device_id: String,
    status: String,
    created_at_unix_ms: i64,
    completed_at_unix_ms: Option<i64>,
    payload: Value,
}

#[derive(Debug)]
struct CompatExecutionResult {
    content: String,
    tool_calls: Vec<CompatToolCall>,
    finish_reason: &'static str,
    snapshot: journal::OrchestratorRunStatusSnapshot,
}

#[derive(Debug, Clone)]
struct CompatToolCall {
    id: String,
    name: String,
    arguments: String,
}

#[derive(Debug, Clone)]
struct CompatStreamToolCall {
    id: String,
    name: String,
    arguments: String,
    output_index: u64,
}

#[derive(Debug, Clone)]
struct CompatChatStreamContext {
    token_id: String,
    run_id: String,
    session_id: String,
    principal: String,
    device_id: String,
    cancel_on_disconnect: bool,
    created_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
struct CompatModelDescriptor {
    id: String,
    role: &'static str,
    provider_kind: String,
    health_status: String,
    discovery_status: String,
    default_model: bool,
    enabled: bool,
    dimensions: Option<u32>,
    capabilities: Option<model_provider::ProviderCapabilitiesSnapshot>,
}

/// Handles `GET /v1/models` for the compatibility API.
///
/// # Errors
/// Returns an error response when API-token authorization fails, the compat
/// rate limit is exhausted, or the system clock cannot be read.
pub(crate) async fn compat_models_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let token =
        authorize_compat_api_token(&state, &headers, PERMISSION_COMPAT_MODELS_READ, None, now)?;
    enforce_compat_rate_limit(&state, token.token_id.as_str(), token.rate_limit_per_minute)?;
    let provider = state.runtime.model_provider_status_snapshot();
    let models = build_compat_models(&provider);
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "read",
        "models_listed",
        Some(provider.kind.as_str()),
        now,
    );
    Ok(Json(json!({
        "object": "list",
        "data": models,
    })))
}

/// Handles `GET /v1/models/{model_id}` for the compatibility API.
///
/// # Errors
/// Returns an error response when API-token authorization fails, the compat
/// rate limit is exhausted, the requested model is not published, or the system
/// clock cannot be read.
pub(crate) async fn compat_model_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(model_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let token =
        authorize_compat_api_token(&state, &headers, PERMISSION_COMPAT_MODELS_READ, None, now)?;
    enforce_compat_rate_limit(&state, token.token_id.as_str(), token.rate_limit_per_minute)?;
    let provider = state.runtime.model_provider_status_snapshot();
    let descriptor = build_compat_model_descriptors(&provider)
        .into_iter()
        .find(|candidate| candidate.id == model_id)
        .ok_or_else(|| compat_model_not_found_response(model_id.as_str()))?;
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "read",
        "model_detail_read",
        Some(model_id.as_str()),
        now,
    );
    Ok(Json(compat_model_json(&descriptor)))
}

/// Handles `GET /v1/capabilities` for compatibility client discovery.
///
/// # Errors
/// Returns an error response when API-token authorization, rate limiting,
/// embeddings posture lookup, or the system clock fails.
pub(crate) async fn compat_capabilities_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let token =
        authorize_compat_api_token(&state, &headers, PERMISSION_COMPAT_MODELS_READ, None, now)?;
    enforce_compat_rate_limit(&state, token.token_id.as_str(), token.rate_limit_per_minute)?;
    let provider = state.runtime.model_provider_status_snapshot();
    let embeddings_status =
        state.runtime.memory_embeddings_status().await.map_err(runtime_status_response)?;
    let feature_flags = {
        let registry = lock_access_registry(&state.access_registry);
        registry.snapshot(token.principal.as_str()).feature_flags
    };
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "read",
        "capabilities_read",
        Some(provider.kind.as_str()),
        now,
    );
    Ok(Json(build_compat_capabilities_payload(
        &provider,
        &embeddings_status,
        feature_flags.as_slice(),
        now,
    )))
}

/// Handles `POST /v1/embeddings` using the configured embeddings provider.
///
/// # Errors
/// Returns an error response when authorization or rate limiting fails, the
/// embeddings provider is unavailable, the requested model/dimensions are
/// invalid, or provider execution fails.
pub(crate) async fn compat_embeddings_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CompatEmbeddingsRequest>,
) -> Result<Json<Value>, Response> {
    let _ = payload.user.as_deref();
    let _ = payload.metadata.as_ref();
    if payload.encoding_format.as_deref().is_some_and(|value| !value.eq_ignore_ascii_case("float"))
    {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "unsupported_encoding_format",
            "encoding_format must be omitted or set to 'float'",
        ));
    }

    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let token = authorize_compat_api_token(
        &state,
        &headers,
        PERMISSION_COMPAT_EMBEDDINGS_CREATE,
        Some(FEATURE_COMPAT_EMBEDDINGS_API),
        now,
    )?;
    enforce_compat_rate_limit(&state, token.token_id.as_str(), token.rate_limit_per_minute)?;

    let embeddings_status =
        state.runtime.memory_embeddings_status().await.map_err(runtime_status_response)?;
    if !embeddings_status.production_default_active {
        let warning = embeddings_status.warning.unwrap_or_else(|| {
            "compat embeddings are unavailable because the runtime is operating in a degraded embeddings posture"
                .to_owned()
        });
        touch_compat_api_token(
            &state,
            token.token_id.as_str(),
            "run",
            "embeddings_degraded",
            embeddings_status.degraded_reason_code.as_deref(),
            now,
        );
        return Err(compat_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            "embeddings_degraded",
            warning,
        ));
    }

    let requested_inputs = normalize_compat_embeddings_input(payload.input)?;
    let prompt_tokens = requested_inputs
        .iter()
        .map(|input| crate::orchestrator::estimate_token_count(input))
        .sum::<u64>();
    let loaded_config = load_model_provider_config(&state);
    let mut provider_config = crate::retrieval::resolve_embeddings_provider_config(&loaded_config)
        .map_err(internal_runtime_error_response)?
        .ok_or_else(|| {
            compat_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "server_error",
                "embeddings_unavailable",
                "compat embeddings require a production embeddings-capable provider selection",
            )
        })?;
    let available_model = provider_config.openai_embeddings_model.clone().ok_or_else(|| {
        compat_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            "embeddings_unavailable",
            "compat embeddings model is not configured",
        )
    })?;

    if let Some(requested_model) =
        payload.model.as_deref().and_then(|value| trim_to_option(value.to_owned()))
    {
        if requested_model != available_model {
            touch_compat_api_token(
                &state,
                token.token_id.as_str(),
                "run",
                "embeddings_model_rejected",
                Some(requested_model.as_str()),
                now,
            );
            return Err(compat_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "model_not_available",
                format!(
                    "requested embeddings model '{requested_model}' is not available through the current compat provider"
                ),
            ));
        }
    }
    if let Some(dimensions) = payload.dimensions {
        if dimensions == 0 {
            return Err(compat_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "invalid_dimensions",
                "dimensions must be greater than 0 when provided",
            ));
        }
        provider_config.openai_embeddings_dims = Some(dimensions);
    }

    let provider =
        crate::model_provider::build_embeddings_provider(&provider_config).map_err(|error| {
            compat_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "server_error",
                "embeddings_provider_unavailable",
                error.to_string(),
            )
        })?;
    let response = match provider
        .embed(crate::model_provider::EmbeddingsRequest { inputs: requested_inputs.clone() })
        .await
    {
        Ok(response) => response,
        Err(error) => {
            touch_compat_api_token(
                &state,
                token.token_id.as_str(),
                "run",
                "embeddings_failed",
                Some(available_model.as_str()),
                now,
            );
            return Err(compat_embeddings_provider_error_response(error));
        }
    };

    tracing::info!(
        compat_model = %response.model_name,
        input_count = requested_inputs.len(),
        prompt_tokens,
        embedding_dimensions = response.dimensions,
        retry_count = response.retry_count,
        "compat embeddings request completed"
    );
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "run",
        "embeddings_completed",
        Some(response.model_name.as_str()),
        now,
    );
    Ok(Json(build_compat_embeddings_payload(prompt_tokens, &response)))
}

/// Handles `POST /v1/tools/invoke`.
///
/// # Errors
/// Always returns a compatibility error after authorization because direct
/// tool invocation stays disabled until it can be approval-bound.
pub(crate) async fn compat_tools_invoke_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(_payload): Json<Value>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let token = authorize_compat_api_token(
        &state,
        &headers,
        PERMISSION_COMPAT_TOOLS_INVOKE,
        Some(FEATURE_COMPAT_TOOLS_INVOKE),
        now,
    )?;
    enforce_compat_rate_limit(&state, token.token_id.as_str(), token.rate_limit_per_minute)?;
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "run",
        "tools_invoke_refused",
        None,
        now,
    );
    Err(compat_error_response(
        StatusCode::NOT_IMPLEMENTED,
        "invalid_request_error",
        "tools_invoke_disabled",
        "compat /v1/tools/invoke is intentionally gated off until an approval-bound execution surface is ready",
    ))
}

/// Handles `POST /v1/chat/completions`.
///
/// # Errors
/// Returns an error response when request rendering, API-token authorization,
/// rate limiting, gateway connection, run execution, or final status lookup
/// fails.
pub(crate) async fn compat_chat_completions_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CompatChatCompletionsRequest>,
) -> Result<Response, Response> {
    let prompt_text = render_compat_messages_prompt(payload.messages.as_slice())?;
    let prepared = prepare_compat_run(
        &state,
        &headers,
        payload.model.as_deref(),
        payload.user.as_deref(),
        payload.metadata.as_ref(),
        prompt_text,
        PERMISSION_COMPAT_CHAT_CREATE,
    )
    .await?;
    if payload.stream.unwrap_or(false) {
        return Ok(build_compat_chat_streaming_response(
            state,
            prepared,
            payload.cancel_on_disconnect.unwrap_or(true),
        ));
    }
    let token_id = prepared.token.token_id.clone();
    let run_id = prepared.run_id.clone();
    let execution = execute_compat_run(&state, prepared).await;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    match execution {
        Ok(result) => {
            touch_compat_api_token(
                &state,
                token_id.as_str(),
                "run",
                "chat_completed",
                Some(run_id.as_str()),
                now,
            );
            Ok(Json(build_compat_chat_completion_payload(&result)).into_response())
        }
        Err(response) => {
            touch_compat_api_token(
                &state,
                token_id.as_str(),
                "run",
                "chat_failed",
                Some(run_id.as_str()),
                now,
            );
            Err(response)
        }
    }
}

/// Handles `POST /v1/responses`.
///
/// # Errors
/// Returns an error response when the request contains no text-bearing input,
/// fails authorization/rate limiting, or the backing gateway run fails before
/// a streaming response can be opened.
pub(crate) async fn compat_responses_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CompatResponsesRequest>,
) -> Result<Response, Response> {
    let request_payload_bytes =
        serde_json::to_vec(&payload).map_err(internal_runtime_error_response)?;
    let idempotency_key = compat_idempotency_key(&headers)?;
    let stream = payload.stream.unwrap_or(false);
    if stream && idempotency_key.is_some() {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "streaming_idempotency_unsupported",
            "Idempotency-Key is supported for non-streaming /v1/responses create requests",
        ));
    }
    let prompt_text = match payload.input {
        CompatResponsesInput::Text(text) => trim_to_option(text).ok_or_else(|| {
            compat_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "empty_input",
                "input cannot be empty",
            )
        })?,
        CompatResponsesInput::Messages(messages) => {
            let rendered = messages
                .into_iter()
                .map(|item| CompatChatMessage {
                    role: item.role.unwrap_or_else(|| "user".to_owned()),
                    content: item.content,
                    name: item.name,
                })
                .collect::<Vec<_>>();
            render_compat_messages_prompt(rendered.as_slice())?
        }
    };
    let authorized = authorize_compat_run_context(
        &state,
        &headers,
        payload.model.as_deref(),
        payload.metadata.as_ref(),
        PERMISSION_COMPAT_RESPONSES_CREATE,
    )?;
    let idempotency = if let Some(raw_key) = idempotency_key.as_deref() {
        match begin_compat_responses_idempotency(
            &state,
            &authorized,
            raw_key,
            request_payload_bytes.as_slice(),
        )
        .await?
        {
            CompatResponseIdempotencyBegin::Reserved(reservation) => Some(reservation),
            CompatResponseIdempotencyBegin::Replay(payload) => {
                return Ok(Json(payload).into_response());
            }
        }
    } else {
        None
    };
    let prepared =
        prepare_compat_run_from_context(&state, authorized, payload.user.as_deref(), prompt_text)
            .await?;
    if stream {
        return Ok(build_compat_responses_streaming_response(state, prepared));
    }
    let token_id = prepared.token.token_id.clone();
    let run_id = prepared.run_id.clone();
    let owner_principal = prepared.principal.clone();
    let device_id = prepared.device_id.clone();
    let execution = execute_compat_run(&state, prepared).await;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    match execution {
        Ok(result) => {
            touch_compat_api_token(
                &state,
                token_id.as_str(),
                "run",
                "responses_completed",
                Some(run_id.as_str()),
                now,
            );
            let response_payload = build_compat_responses_payload(&result);
            persist_compat_response_payload(
                &state,
                CompatResponsePersistRequest {
                    response_id: response_payload
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    session_id: result.snapshot.session_id.clone(),
                    run_id: result.snapshot.run_id.clone(),
                    owner_principal,
                    device_id,
                    status: "completed".to_owned(),
                    created_at_unix_ms: result.snapshot.created_at_unix_ms,
                    completed_at_unix_ms: result.snapshot.completed_at_unix_ms,
                    payload: response_payload.clone(),
                },
            )
            .await?;
            if let Some(reservation) = idempotency {
                complete_compat_response_idempotency(
                    &state,
                    &reservation,
                    response_payload.get("id").and_then(Value::as_str).unwrap_or_default(),
                    result.snapshot.run_id.as_str(),
                    result.snapshot.session_id.as_str(),
                )
                .await?;
            }
            Ok(Json(response_payload).into_response())
        }
        Err(response) => {
            touch_compat_api_token(
                &state,
                token_id.as_str(),
                "run",
                "responses_failed",
                Some(run_id.as_str()),
                now,
            );
            if let Some(reservation) = idempotency {
                let _ = fail_compat_response_idempotency(
                    &state,
                    &reservation,
                    "compat.responses/run_failed",
                    "compat responses run failed",
                    "retry with the same idempotency key after the transient failure is resolved",
                )
                .await;
            }
            Err(response)
        }
    }
}

/// Handles `POST /v1/runs`.
///
/// # Errors
/// Returns an error response when request validation, API-token authorization,
/// idempotency reservation, session resolution, or run queueing fails.
pub(crate) async fn compat_runs_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CompatRunsCreateQuery>,
    Json(payload): Json<CompatRunsCreateRequest>,
) -> Result<Response, Response> {
    validate_compat_run_create_mode(query.mode.as_deref(), payload.mode.as_deref())?;
    let request_payload_bytes =
        serde_json::to_vec(&payload).map_err(internal_runtime_error_response)?;
    validate_compat_run_tool_request(&payload)?;
    let idempotency_key = compat_idempotency_key(&headers)?;
    let prompt_text = render_compat_runs_prompt(&payload)?;
    let metadata = compat_runs_effective_metadata(&payload)?;
    let authorized = authorize_compat_run_context(
        &state,
        &headers,
        payload.model.as_deref(),
        metadata.as_ref(),
        PERMISSION_COMPAT_RESPONSES_CREATE,
    )?;
    let idempotency = if let Some(raw_key) = idempotency_key.as_deref() {
        match begin_compat_runs_idempotency(
            &state,
            &authorized,
            raw_key,
            request_payload_bytes.as_slice(),
        )
        .await?
        {
            CompatRunIdempotencyBegin::Reserved(reservation) => Some(reservation),
            CompatRunIdempotencyBegin::Replay(payload) => {
                return Ok(Json(payload).into_response());
            }
        }
    } else {
        None
    };
    let prepared =
        prepare_compat_run_from_context(&state, authorized, payload.user.as_deref(), prompt_text)
            .await?;
    let token_id = prepared.token.token_id.clone();
    let run_id = prepared.run_id.clone();
    let session_id = prepared.session_id.clone();
    let principal = prepared.principal.clone();
    let accepted_payload = build_compat_run_status_payload_from_prepared(&prepared);

    if let Some(reservation) = idempotency {
        complete_compat_run_idempotency(&state, &reservation, run_id.as_str(), session_id.as_str())
            .await?;
    }

    let background_state = state.clone();
    let background_token_id = token_id.clone();
    let background_run_id = run_id.clone();
    tokio::spawn(async move {
        let execution = execute_compat_run(&background_state, prepared).await;
        let now = unix_ms_now().unwrap_or_default();
        match execution {
            Ok(_) => {
                touch_compat_api_token(
                    &background_state,
                    background_token_id.as_str(),
                    "run",
                    "runs_completed",
                    Some(background_run_id.as_str()),
                    now,
                );
            }
            Err(response) => {
                tracing::warn!(
                    run_id = %background_run_id,
                    error = %compat_error_body_from_response(&response),
                    "public runs API background execution failed"
                );
                touch_compat_api_token(
                    &background_state,
                    background_token_id.as_str(),
                    "run",
                    "runs_failed",
                    Some(background_run_id.as_str()),
                    now,
                );
            }
        }
    });

    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    touch_compat_api_token(
        &state,
        token_id.as_str(),
        "run",
        "runs_accepted",
        Some(run_id.as_str()),
        now,
    );
    tracing::info!(run_id = %run_id, principal = %principal, "public runs API request accepted");
    Ok(Json(accepted_payload).into_response())
}

/// Handles `GET /v1/runs/{run_id}`.
///
/// # Errors
/// Returns an error response when the token is unauthorized or the run does
/// not exist for the token owner.
pub(crate) async fn compat_run_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let (token, owner_principal) = authorize_compat_response_record_access(&state, &headers, now)?;
    let payload =
        load_compat_run_status_payload(&state, run_id.as_str(), owner_principal.as_str()).await?;
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "runs_get",
        "run_loaded",
        Some(run_id.as_str()),
        now,
    );
    Ok(Json(payload).into_response())
}

/// Handles `GET /v1/runs/{run_id}/events`.
///
/// # Errors
/// Returns an error response when the token is unauthorized or the run does
/// not exist for the token owner.
pub(crate) async fn compat_run_events_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<CompatRunEventsQuery>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let (token, owner_principal, device_id) = authorize_compat_run_access(&state, &headers, now)?;
    let snapshot =
        load_compat_run_snapshot_for_owner(&state, run_id.as_str(), owner_principal.as_str())
            .await?;
    record_compat_run_flow_audit(
        &state,
        &snapshot,
        owner_principal.as_str(),
        device_id.as_str(),
        "compat_run_events_opened",
        json!({
            "action": "events_stream_opened",
            "disconnect_policy": "detach",
            "cancel_on_disconnect": false,
            "after_seq": query.after_seq,
            "limit": query.limit,
        }),
        now,
    )
    .await?;
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "runs_events",
        "run_events_opened",
        Some(run_id.as_str()),
        now,
    );
    Ok(build_compat_run_events_streaming_response(state, snapshot, query.after_seq, query.limit))
}

/// Handles `POST /v1/runs/{run_id}/wait`.
///
/// # Errors
/// Returns an error response when authorization, run ownership validation, or
/// runtime status loading fails. A deadline expiry is returned as a successful
/// wait payload with the current run status so clients can retry without
/// mutating the run.
pub(crate) async fn compat_run_wait_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    payload: Option<Json<CompatRunWaitRequest>>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let (token, owner_principal, _device_id) = authorize_compat_run_access(&state, &headers, now)?;
    let request = payload.map(|Json(payload)| payload).unwrap_or_default();
    let timeout_ms = compat_run_wait_timeout_ms(request.timeout_ms);
    let return_on_waiting = request.return_on_waiting.unwrap_or(false);

    let initial_snapshot =
        load_compat_run_snapshot_for_owner(&state, run_id.as_str(), owner_principal.as_str())
            .await?;
    let outcome = state
        .runtime
        .wait_for_orchestrator_run(crate::gateway::OrchestratorRunWaitRequest {
            run_id: initial_snapshot.run_id.clone(),
            timeout: Duration::from_millis(timeout_ms),
            poll_interval: Duration::from_millis(250),
            return_on_waiting,
        })
        .await;

    let payload = match outcome {
        Ok(outcome) => {
            if outcome.snapshot.principal != owner_principal {
                return Err(compat_run_not_found_response(run_id.as_str()));
            }
            let run = build_compat_run_status_payload_for_snapshot(
                &state,
                &outcome.snapshot,
                owner_principal.as_str(),
            )
            .await?;
            build_compat_run_wait_payload(
                outcome.snapshot.run_id.as_str(),
                timeout_ms,
                false,
                Some(outcome.canonical_state.as_str()),
                run,
            )
        }
        Err(error) if error.code() == tonic::Code::DeadlineExceeded => {
            let run =
                load_compat_run_status_payload(&state, run_id.as_str(), owner_principal.as_str())
                    .await?;
            build_compat_run_wait_payload(run_id.as_str(), timeout_ms, true, None, run)
        }
        Err(error) => return Err(runtime_status_response(error)),
    };

    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "runs_wait",
        if payload.get("timed_out").and_then(Value::as_bool) == Some(true) {
            "run_wait_timeout"
        } else {
            "run_wait_completed"
        },
        Some(run_id.as_str()),
        now,
    );
    Ok(Json(payload).into_response())
}

/// Handles `POST /v1/runs/{run_id}/stop`.
///
/// # Errors
/// Returns an error response when authorization, run ownership validation,
/// stop request validation, or the runtime cancel operation fails.
pub(crate) async fn compat_run_stop_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(payload): Json<CompatRunStopRequest>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let (token, owner_principal, _device_id) = authorize_compat_run_access(&state, &headers, now)?;
    let snapshot =
        load_compat_run_snapshot_for_owner(&state, run_id.as_str(), owner_principal.as_str())
            .await?;
    let stop_mode = parse_compat_run_stop_mode(payload.mode.as_deref())?;
    let cleanup_policy = parse_compat_run_cleanup_policy(payload.cleanup_policy.as_deref())?;
    let reason = payload
        .reason
        .and_then(trim_to_option)
        .unwrap_or_else(|| format!("compat_runs_stop:{stop_mode}"));
    let outcome = state
        .runtime
        .apply_turn_control(crate::application::turn_control::TurnControlRequest {
            operation: crate::application::turn_control::TurnControlOperation::CancelRun,
            actor_principal: owner_principal.clone(),
            active_phase: None,
            session_id: Some(snapshot.session_id.clone()),
            run_id: Some(snapshot.run_id.clone()),
            queued_input_id: None,
            priority_lane: None,
            instruction: None,
            reason: Some(reason.clone()),
            dry_run: false,
        })
        .await
        .map_err(runtime_status_response)?;
    let status =
        load_compat_run_status_payload(&state, run_id.as_str(), owner_principal.as_str()).await?;
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "runs_stop",
        "run_stop_requested",
        Some(run_id.as_str()),
        now,
    );
    Ok(Json(json!({
        "id": run_id,
        "object": "run.stop",
        "stopped": outcome.effect.get("cancel_requested").and_then(Value::as_bool).unwrap_or(false),
        "mode": stop_mode,
        "cleanup_policy": cleanup_policy,
        "reason": reason,
        "run": status,
        "_palyra": {
            "turn_control": outcome.decision,
            "effect": outcome.effect,
        },
    }))
    .into_response())
}

/// Handles `POST /v1/runs/{run_id}/detach`.
///
/// # Errors
/// Returns an error response when authorization, run ownership validation, or
/// detach audit persistence fails.
pub(crate) async fn compat_run_detach_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(payload): Json<CompatRunDetachRequest>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let (token, owner_principal, device_id) = authorize_compat_run_access(&state, &headers, now)?;
    let snapshot =
        load_compat_run_snapshot_for_owner(&state, run_id.as_str(), owner_principal.as_str())
            .await?;
    let reason =
        payload.reason.and_then(trim_to_option).unwrap_or_else(|| "compat_runs_detach".to_owned());
    record_compat_run_flow_audit(
        &state,
        &snapshot,
        owner_principal.as_str(),
        device_id.as_str(),
        reason.as_str(),
        json!({
            "action": "detach",
            "disconnect_policy": "detach",
            "cancel_on_disconnect": false,
        }),
        now,
    )
    .await?;
    let status = build_compat_run_status_payload(&snapshot, None);
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "runs_detach",
        "run_detached",
        Some(run_id.as_str()),
        now,
    );
    Ok(Json(json!({
        "id": run_id,
        "object": "run.detach",
        "detached": true,
        "disconnect_policy": "detach",
        "cancel_on_disconnect": false,
        "reason": reason,
        "run": status,
    }))
    .into_response())
}

/// Handles `POST /v1/runs/{run_id}/approval`.
///
/// # Errors
/// Returns an error response when authorization, approval ownership, decision
/// validation, conflict checks, or approval persistence fails.
pub(crate) async fn compat_run_approval_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(payload): Json<CompatRunApprovalRequest>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let (token, owner_principal, device_id) = authorize_compat_run_access(&state, &headers, now)?;
    let snapshot =
        load_compat_run_snapshot_for_owner(&state, run_id.as_str(), owner_principal.as_str())
            .await?;
    let decision = parse_compat_run_approval_decision(&payload)?;
    let decision_scope = parse_compat_approval_decision_scope(payload.decision_scope.as_deref())?;
    validate_compat_approval_ttl(decision_scope, payload.decision_scope_ttl_ms)?;
    let reason =
        compat_approval_reason(payload.reason.clone(), decision, "compat_runs_approval_decision");
    let approval = load_compat_run_approval_target(
        &state,
        &payload,
        run_id.as_str(),
        owner_principal.as_str(),
    )
    .await?;

    if let Some(existing_decision) = approval.decision {
        return compat_resolved_approval_replay_response(
            &state,
            &token,
            run_id,
            &approval,
            existing_decision,
            decision,
            now,
        )
        .await;
    }
    ensure_compat_approval_expected_version(&approval, payload.expected_version)?;
    if compat_approval_is_expired(&approval, now) {
        return Err(compat_error_response(
            StatusCode::GONE,
            "invalid_request_error",
            "approval_expired",
            "approval prompt expired before the public runs API decision was received",
        ));
    }
    if decision == journal::ApprovalDecision::Allow && snapshot.cancel_requested {
        return Err(compat_error_response(
            StatusCode::CONFLICT,
            "invalid_request_error",
            "approval_abort_race",
            "approval allow raced with an already requested run stop",
        ));
    }

    let resolved = state
        .runtime
        .resolve_approval_record(journal::ApprovalResolveRequest {
            approval_id: approval.approval_id.clone(),
            decision,
            decision_scope,
            decision_reason: reason.clone(),
            decision_scope_ttl_ms: payload.decision_scope_ttl_ms,
        })
        .await
        .map_err(runtime_status_response)?;
    let context = RequestContext {
        principal: owner_principal.clone(),
        device_id,
        channel: Some(COMPAT_API_CHANNEL.to_owned()),
    };
    crate::application::approvals::record_approval_resolved_journal_event(
        &state.runtime,
        &context,
        resolved.session_id.as_str(),
        resolved.run_id.as_str(),
        None,
        resolved.approval_id.as_str(),
        decision,
        decision_scope,
        payload.decision_scope_ttl_ms,
        reason.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    let status =
        load_compat_run_status_payload(&state, run_id.as_str(), owner_principal.as_str()).await?;
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "runs_approval",
        "approval_resolved",
        Some(run_id.as_str()),
        now,
    );
    Ok(Json(json!({
        "id": run_id,
        "object": "run.approval",
        "approval": compat_run_approval_payload(&resolved),
        "run": status,
        "_palyra": {
            "idempotent_replay": false,
        },
    }))
    .into_response())
}

/// Handles `GET /v1/responses/{response_id}`.
///
/// # Errors
/// Returns an error response when the token is unauthorized or the response
/// public view does not exist for the token owner.
pub(crate) async fn compat_response_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(response_id): Path<String>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let (token, owner_principal) = authorize_compat_response_record_access(&state, &headers, now)?;
    let payload =
        load_compat_response_payload(&state, response_id.as_str(), owner_principal.as_str())
            .await?;
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "responses_get",
        "response_loaded",
        Some(response_id.as_str()),
        now,
    );
    Ok(Json(payload).into_response())
}

/// Handles `DELETE /v1/responses/{response_id}`.
///
/// # Errors
/// Returns an error response when the token is unauthorized or the response
/// does not belong to the token owner.
pub(crate) async fn compat_response_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(response_id): Path<String>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let (token, owner_principal) = authorize_compat_response_record_access(&state, &headers, now)?;
    let outcome =
        delete_compat_response_public_view(&state, response_id.as_str(), owner_principal.as_str())
            .await?;
    touch_compat_api_token(
        &state,
        token.token_id.as_str(),
        "responses_delete",
        if outcome.already_deleted { "already_deleted" } else { "deleted" },
        Some(response_id.as_str()),
        now,
    );
    Ok(Json(json!({
        "id": outcome.response_id,
        "object": "response.deleted",
        "deleted": outcome.deleted,
    }))
    .into_response())
}

fn authorize_compat_response_record_access(
    state: &AppState,
    headers: &HeaderMap,
    now: i64,
) -> CompatHttpResult<(AuthenticatedApiToken, String)> {
    let (token, principal, _) = authorize_compat_run_access(state, headers, now)?;
    Ok((token, principal))
}

fn authorize_compat_run_access(
    state: &AppState,
    headers: &HeaderMap,
    now: i64,
) -> CompatHttpResult<(AuthenticatedApiToken, String, String)> {
    let token =
        authorize_compat_api_token(state, headers, PERMISSION_COMPAT_RESPONSES_CREATE, None, now)?;
    enforce_compat_rate_limit(state, token.token_id.as_str(), token.rate_limit_per_minute)?;
    let (principal, device_id) =
        resolve_compat_runtime_identity(state, &token, PERMISSION_COMPAT_RESPONSES_CREATE)?;
    Ok((token, principal, device_id))
}

fn compat_idempotency_key(headers: &HeaderMap) -> CompatHttpResult<Option<String>> {
    let Some(value) = headers.get(COMPAT_IDEMPOTENCY_KEY_HEADER) else {
        return Ok(None);
    };
    let raw = value.to_str().map_err(|_| {
        compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_idempotency_key",
            "Idempotency-Key must be valid visible ASCII",
        )
    })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > 256 {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "idempotency_key_too_long",
            "Idempotency-Key must be at most 256 characters",
        )
        .into());
    }
    Ok(Some(trimmed.to_owned()))
}

async fn begin_compat_responses_idempotency(
    state: &AppState,
    context: &CompatAuthorizedRunContext,
    raw_key: &str,
    request_payload: &[u8],
) -> Result<CompatResponseIdempotencyBegin, Response> {
    let storage_key = compat_response_idempotency_storage_key(
        context.principal.as_str(),
        context.token.token_id.as_str(),
        raw_key,
    );
    let payload_sha256 = crate::sha256_hex(request_payload);
    let runtime = Arc::clone(&state.runtime);
    let begin_key = storage_key.clone();
    let begin = tokio::task::spawn_blocking(move || {
        runtime.journal_store.begin_idempotency_operation(&journal::IdempotencyBeginRequest {
            key: begin_key,
            scope: "compat.responses".to_owned(),
            operation_kind: "responses.create".to_owned(),
            payload_sha256,
            expires_at_unix_ms: Some(
                unix_ms_now().unwrap_or(0).saturating_add(COMPAT_RESPONSE_IDEMPOTENCY_TTL_MS),
            ),
        })
    })
    .await
    .map_err(|_| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_worker_failed",
            "idempotency worker panicked",
        )
    })?
    .map_err(compat_journal_error_response)?;

    match begin.decision {
        IdempotencyReplayDecision::CompletedReplayResult => {
            let response_id = compat_response_id_from_idempotency_record(begin.record.as_ref())?;
            let payload = load_compat_response_payload(
                state,
                response_id.as_str(),
                context.principal.as_str(),
            )
            .await?;
            Ok(CompatResponseIdempotencyBegin::Replay(payload))
        }
        IdempotencyReplayDecision::ConflictingPayload => Err(compat_error_response(
            StatusCode::CONFLICT,
            "idempotency_error",
            "idempotency_conflict",
            "Idempotency-Key was reused with a different /v1/responses request payload",
        )),
        IdempotencyReplayDecision::SamePayloadRetry => Err(compat_error_response(
            StatusCode::CONFLICT,
            "idempotency_error",
            "idempotency_in_progress",
            "Idempotency-Key already has an in-progress /v1/responses request",
        )),
        IdempotencyReplayDecision::Reserved | IdempotencyReplayDecision::ExpiredRetry => {
            Ok(CompatResponseIdempotencyBegin::Reserved(CompatResponseIdempotencyReservation {
                storage_key,
            }))
        }
    }
}

fn compat_response_idempotency_storage_key(
    owner_principal: &str,
    token_id: &str,
    raw_key: &str,
) -> String {
    let material = format!(
        "surface=compat.responses\nowner={owner_principal}\ntoken={token_id}\nraw_key={raw_key}"
    );
    let digest = crate::sha256_hex(material.as_bytes());
    format!("compat.responses:{}", &digest[..32])
}

fn compat_response_id_from_idempotency_record(
    record: Option<&palyra_common::runtime_contracts::IdempotencyRecordSnapshot>,
) -> CompatHttpResult<String> {
    let result_json = record.and_then(|record| record.result_json.as_deref()).ok_or_else(|| {
        CompatHttpError::from(compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_result_missing",
            "stored idempotency result is missing a response id",
        ))
    })?;
    let value = serde_json::from_str::<Value>(result_json).map_err(|error| {
        CompatHttpError::from(compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_result_invalid",
            format!("stored idempotency result is not valid JSON: {error}"),
        ))
    })?;
    value
        .get("response_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            CompatHttpError::from(compat_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "idempotency_result_invalid",
                "stored idempotency result does not contain response_id",
            ))
        })
}

async fn begin_compat_runs_idempotency(
    state: &AppState,
    context: &CompatAuthorizedRunContext,
    raw_key: &str,
    request_payload: &[u8],
) -> Result<CompatRunIdempotencyBegin, Response> {
    let storage_key = compat_run_idempotency_storage_key(
        context.principal.as_str(),
        context.token.token_id.as_str(),
        raw_key,
    );
    let payload_sha256 = crate::sha256_hex(request_payload);
    let runtime = Arc::clone(&state.runtime);
    let begin_key = storage_key.clone();
    let begin = tokio::task::spawn_blocking(move || {
        runtime.journal_store.begin_idempotency_operation(&journal::IdempotencyBeginRequest {
            key: begin_key,
            scope: "compat.runs".to_owned(),
            operation_kind: "runs.create".to_owned(),
            payload_sha256,
            expires_at_unix_ms: Some(
                unix_ms_now().unwrap_or(0).saturating_add(COMPAT_RUN_IDEMPOTENCY_TTL_MS),
            ),
        })
    })
    .await
    .map_err(|_| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_worker_failed",
            "idempotency worker panicked",
        )
    })?
    .map_err(compat_journal_error_response)?;

    match begin.decision {
        IdempotencyReplayDecision::CompletedReplayResult => {
            let run_id = compat_run_id_from_idempotency_record(begin.record.as_ref())?;
            let payload =
                load_compat_run_status_payload(state, run_id.as_str(), context.principal.as_str())
                    .await?;
            Ok(CompatRunIdempotencyBegin::Replay(payload))
        }
        IdempotencyReplayDecision::ConflictingPayload => Err(compat_error_response(
            StatusCode::CONFLICT,
            "idempotency_error",
            "idempotency_conflict",
            "Idempotency-Key was reused with a different /v1/runs request payload",
        )),
        IdempotencyReplayDecision::SamePayloadRetry => Err(compat_error_response(
            StatusCode::CONFLICT,
            "idempotency_error",
            "idempotency_in_progress",
            "Idempotency-Key already has an in-progress /v1/runs request",
        )),
        IdempotencyReplayDecision::Reserved | IdempotencyReplayDecision::ExpiredRetry => {
            Ok(CompatRunIdempotencyBegin::Reserved(CompatRunIdempotencyReservation { storage_key }))
        }
    }
}

fn compat_run_idempotency_storage_key(
    owner_principal: &str,
    token_id: &str,
    raw_key: &str,
) -> String {
    let material = format!(
        "surface=compat.runs\nowner={owner_principal}\ntoken={token_id}\nraw_key={raw_key}"
    );
    let digest = crate::sha256_hex(material.as_bytes());
    format!("compat.runs:{}", &digest[..32])
}

fn compat_run_id_from_idempotency_record(
    record: Option<&palyra_common::runtime_contracts::IdempotencyRecordSnapshot>,
) -> CompatHttpResult<String> {
    let result_json = record.and_then(|record| record.result_json.as_deref()).ok_or_else(|| {
        CompatHttpError::from(compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_result_missing",
            "stored idempotency result is missing a run id",
        ))
    })?;
    let value = serde_json::from_str::<Value>(result_json).map_err(|error| {
        CompatHttpError::from(compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_result_invalid",
            format!("stored idempotency result is not valid JSON: {error}"),
        ))
    })?;
    value
        .get("run_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            CompatHttpError::from(compat_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "idempotency_result_invalid",
                "stored idempotency result does not contain run_id",
            ))
        })
}

async fn complete_compat_run_idempotency(
    state: &AppState,
    reservation: &CompatRunIdempotencyReservation,
    run_id: &str,
    session_id: &str,
) -> Result<(), Response> {
    let result_json = json!({
        "run_id": run_id,
        "session_id": session_id,
    })
    .to_string();
    let runtime = Arc::clone(&state.runtime);
    let key = reservation.storage_key.clone();
    tokio::task::spawn_blocking(move || {
        runtime.journal_store.complete_idempotency_operation(&journal::IdempotencyCompleteRequest {
            key,
            result_json,
        })
    })
    .await
    .map_err(|_| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_worker_failed",
            "idempotency worker panicked",
        )
    })?
    .map(|_| ())
    .map_err(compat_journal_error_response)
}

async fn complete_compat_response_idempotency(
    state: &AppState,
    reservation: &CompatResponseIdempotencyReservation,
    response_id: &str,
    run_id: &str,
    session_id: &str,
) -> Result<(), Response> {
    let result_json = json!({
        "response_id": response_id,
        "run_id": run_id,
        "session_id": session_id,
    })
    .to_string();
    let runtime = Arc::clone(&state.runtime);
    let key = reservation.storage_key.clone();
    tokio::task::spawn_blocking(move || {
        runtime.journal_store.complete_idempotency_operation(&journal::IdempotencyCompleteRequest {
            key,
            result_json,
        })
    })
    .await
    .map_err(|_| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_worker_failed",
            "idempotency worker panicked",
        )
    })?
    .map(|_| ())
    .map_err(compat_journal_error_response)
}

async fn fail_compat_response_idempotency(
    state: &AppState,
    reservation: &CompatResponseIdempotencyReservation,
    code: &str,
    message: &str,
    recovery_hint: &str,
) -> Result<(), Response> {
    let runtime = Arc::clone(&state.runtime);
    let key = reservation.storage_key.clone();
    let error = StableErrorEnvelope::new(code, message, recovery_hint);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .fail_idempotency_operation(&journal::IdempotencyFailRequest { key, error })
    })
    .await
    .map_err(|_| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "idempotency_worker_failed",
            "idempotency worker panicked",
        )
    })?
    .map(|_| ())
    .map_err(compat_journal_error_response)
}

async fn persist_compat_response_payload(
    state: &AppState,
    request: CompatResponsePersistRequest,
) -> Result<(), Response> {
    if request.response_id.trim().is_empty() {
        return Err(compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "missing_response_id",
            "compat response payload did not include an id",
        ));
    }
    let error_json = request.payload.get("error").map(Value::to_string);
    let response_json = request.payload.to_string();
    let redaction_state_json = json!({
        "policy": "compat_response_store.v1",
        "public_view": "sanitized",
        "raw_tool_output": "artifact_ref_or_withheld",
    })
    .to_string();
    let retention_expires_at_unix_ms =
        Some(request.created_at_unix_ms.saturating_add(COMPAT_RESPONSE_RETENTION_MS));
    let runtime = Arc::clone(&state.runtime);
    tokio::task::spawn_blocking(move || {
        runtime.journal_store.upsert_compat_response_record(&journal::CompatResponseUpsertRequest {
            response_id: request.response_id,
            session_id: request.session_id,
            run_id: request.run_id,
            owner_principal: request.owner_principal,
            device_id: request.device_id,
            channel: Some(COMPAT_API_CHANNEL.to_owned()),
            status: request.status,
            response_json,
            error_json,
            redaction_state_json,
            created_at_unix_ms: request.created_at_unix_ms,
            completed_at_unix_ms: request.completed_at_unix_ms,
            retention_expires_at_unix_ms,
        })
    })
    .await
    .map_err(|_| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "response_store_worker_failed",
            "response store worker panicked",
        )
    })?
    .map(|_| ())
    .map_err(compat_journal_error_response)
}

async fn load_compat_response_payload(
    state: &AppState,
    response_id: &str,
    owner_principal: &str,
) -> Result<Value, Response> {
    let runtime = Arc::clone(&state.runtime);
    let response_id = response_id.to_owned();
    let owner_principal = owner_principal.to_owned();
    let record = tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .compat_response_record_for_owner(response_id.as_str(), owner_principal.as_str())
    })
    .await
    .map_err(|_| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "response_store_worker_failed",
            "response store worker panicked",
        )
    })?
    .map_err(compat_journal_error_response)?;
    serde_json::from_str::<Value>(record.response_json.as_str()).map_err(|error| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "response_store_corrupt",
            format!("stored response JSON is invalid: {error}"),
        )
    })
}

async fn delete_compat_response_public_view(
    state: &AppState,
    response_id: &str,
    owner_principal: &str,
) -> Result<journal::CompatResponseDeleteOutcome, Response> {
    let runtime = Arc::clone(&state.runtime);
    let response_id = response_id.to_owned();
    let owner_principal = owner_principal.to_owned();
    tokio::task::spawn_blocking(move || {
        runtime.journal_store.delete_compat_response_public_view(
            response_id.as_str(),
            owner_principal.as_str(),
            "compat_response_delete",
        )
    })
    .await
    .map_err(|_| {
        compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "response_store_worker_failed",
            "response store worker panicked",
        )
    })?
    .map_err(compat_journal_error_response)
}

async fn load_compat_run_status_payload(
    state: &AppState,
    run_id: &str,
    owner_principal: &str,
) -> Result<Value, Response> {
    let snapshot = load_compat_run_snapshot_for_owner(state, run_id, owner_principal).await?;
    build_compat_run_status_payload_for_snapshot(state, &snapshot, owner_principal).await
}

async fn build_compat_run_status_payload_for_snapshot(
    state: &AppState,
    snapshot: &journal::OrchestratorRunStatusSnapshot,
    owner_principal: &str,
) -> Result<Value, Response> {
    let pending_approval =
        load_compat_pending_approval_for_run(state, snapshot.run_id.as_str(), owner_principal)
            .await?;
    Ok(build_compat_run_status_payload(snapshot, pending_approval.as_ref()))
}

async fn load_compat_run_snapshot_for_owner(
    state: &AppState,
    run_id: &str,
    owner_principal: &str,
) -> Result<journal::OrchestratorRunStatusSnapshot, Response> {
    validate_canonical_id(run_id).map_err(|_| {
        compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_run_id",
            "run_id must be a canonical ULID",
        )
    })?;
    let snapshot = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| compat_run_not_found_response(run_id))?;
    if snapshot.principal != owner_principal {
        return Err(compat_run_not_found_response(run_id));
    }
    Ok(snapshot)
}

async fn load_compat_pending_approval_for_run(
    state: &AppState,
    run_id: &str,
    owner_principal: &str,
) -> Result<Option<journal::ApprovalRecord>, Response> {
    let (approvals, _) = state
        .runtime
        .list_approval_records(
            None,
            Some(100),
            None,
            None,
            None,
            Some(owner_principal.to_owned()),
            None,
            None,
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(approvals
        .into_iter()
        .find(|approval| approval.run_id == run_id && approval.decision.is_none()))
}

async fn load_compat_run_approval_target(
    state: &AppState,
    payload: &CompatRunApprovalRequest,
    run_id: &str,
    owner_principal: &str,
) -> Result<journal::ApprovalRecord, Response> {
    if let Some(approval_id) =
        payload.approval_id.as_ref().and_then(|value| trim_to_option(value.clone()))
    {
        validate_canonical_id(approval_id.as_str()).map_err(|_| {
            compat_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "invalid_approval_id",
                "approval_id must be a canonical ULID",
            )
        })?;
        let approval = state
            .runtime
            .approval_record(approval_id.clone())
            .await
            .map_err(runtime_status_response)?
            .ok_or_else(|| compat_approval_not_found_response(run_id))?;
        if approval.run_id != run_id || approval.principal != owner_principal {
            return Err(compat_approval_not_found_response(run_id));
        }
        return Ok(approval);
    }

    load_compat_pending_approval_for_run(state, run_id, owner_principal)
        .await?
        .ok_or_else(|| compat_approval_not_found_response(run_id))
}

fn parse_compat_run_stop_mode(raw: Option<&str>) -> CompatHttpResult<&'static str> {
    let Some(mode) = raw.and_then(|value| trim_to_option(value.to_owned())) else {
        return Ok("cancel");
    };
    match mode.to_ascii_lowercase().as_str() {
        "cancel" | "graceful" | "cooperative" => Ok("cancel"),
        _ => Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_stop_mode",
            "mode must be omitted or one of cancel|graceful|cooperative",
        )
        .into()),
    }
}

fn parse_compat_run_cleanup_policy(raw: Option<&str>) -> CompatHttpResult<&'static str> {
    let Some(policy) = raw.and_then(|value| trim_to_option(value.to_owned())) else {
        return Ok("runtime_default");
    };
    match policy.to_ascii_lowercase().as_str() {
        "runtime_default" | "default" => Ok("runtime_default"),
        "none" | "noop" => Ok("none"),
        _ => Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_cleanup_policy",
            "cleanup_policy must be omitted or one of runtime_default|default|none|noop",
        )
        .into()),
    }
}

fn parse_compat_run_approval_decision(
    payload: &CompatRunApprovalRequest,
) -> CompatHttpResult<journal::ApprovalDecision> {
    let text_decision = payload
        .action
        .as_ref()
        .or(payload.decision.as_ref())
        .and_then(|value| trim_to_option(value.clone()))
        .map(|value| parse_compat_run_approval_decision_text(value.as_str()))
        .transpose()?;
    let bool_decision = payload.approved.map(|approved| {
        if approved {
            journal::ApprovalDecision::Allow
        } else {
            journal::ApprovalDecision::Deny
        }
    });
    match (text_decision, bool_decision) {
        (Some(text), Some(from_bool)) if text != from_bool => Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "conflicting_approval_decision",
            "approved conflicts with action/decision",
        )
        .into()),
        (Some(decision), _) | (None, Some(decision)) => Ok(decision),
        (None, None) => Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "missing_approval_decision",
            "approval request requires action, decision, or approved",
        )
        .into()),
    }
}

fn parse_compat_run_approval_decision_text(
    value: &str,
) -> CompatHttpResult<journal::ApprovalDecision> {
    match value.to_ascii_lowercase().as_str() {
        "approve" | "allow" | "approved" => Ok(journal::ApprovalDecision::Allow),
        "deny" | "reject" | "rejected" => Ok(journal::ApprovalDecision::Deny),
        "timeout" | "timed_out" => Ok(journal::ApprovalDecision::Timeout),
        "modify" | "modified" => Err(compat_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request_error",
            "approval_modify_unsupported",
            "approval modify is not supported by the current Palyra approval store; approve, deny, or timeout explicitly",
        )
        .into()),
        _ => Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_approval_decision",
            "approval decision must be approve|allow|deny|reject|timeout",
        )
        .into()),
    }
}

fn parse_compat_approval_decision_scope(
    raw: Option<&str>,
) -> CompatHttpResult<journal::ApprovalDecisionScope> {
    let Some(scope) = raw.and_then(|value| trim_to_option(value.to_owned())) else {
        return Ok(journal::ApprovalDecisionScope::Once);
    };
    journal::ApprovalDecisionScope::from_str(scope.to_ascii_lowercase().as_str()).ok_or_else(|| {
        compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_decision_scope",
            "decision_scope must be omitted or one of once|session|timeboxed",
        )
        .into()
    })
}

fn validate_compat_approval_ttl(
    scope: journal::ApprovalDecisionScope,
    ttl_ms: Option<i64>,
) -> CompatHttpResult<()> {
    if ttl_ms.is_some_and(|value| value <= 0) {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_decision_scope_ttl",
            "decision_scope_ttl_ms must be greater than zero when provided",
        )
        .into());
    }
    if scope == journal::ApprovalDecisionScope::Timeboxed && ttl_ms.is_none() {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "missing_decision_scope_ttl",
            "timeboxed approval decisions require decision_scope_ttl_ms",
        )
        .into());
    }
    Ok(())
}

fn ensure_compat_approval_expected_version(
    approval: &journal::ApprovalRecord,
    expected_version: Option<u64>,
) -> CompatHttpResult<()> {
    let Some(expected_version) = expected_version else {
        return Ok(());
    };
    if u64::try_from(approval.updated_at_unix_ms).ok() == Some(expected_version) {
        return Ok(());
    }
    Err(compat_error_response(
        StatusCode::CONFLICT,
        "invalid_request_error",
        "approval_version_conflict",
        "approval version changed before the public runs API decision",
    )
    .into())
}

fn compat_approval_is_expired(approval: &journal::ApprovalRecord, now: i64) -> bool {
    if approval.prompt.timeout_seconds == 0 {
        return false;
    }
    let timeout_ms = i64::from(approval.prompt.timeout_seconds).saturating_mul(1_000);
    now > approval.requested_at_unix_ms.saturating_add(timeout_ms)
}

fn compat_approval_reason(
    raw_reason: Option<String>,
    decision: journal::ApprovalDecision,
    default_prefix: &str,
) -> String {
    raw_reason
        .and_then(trim_to_option)
        .unwrap_or_else(|| format!("{default_prefix}:{}", decision.as_str()))
}

async fn compat_resolved_approval_replay_response(
    state: &AppState,
    token: &AuthenticatedApiToken,
    run_id: String,
    approval: &journal::ApprovalRecord,
    existing_decision: journal::ApprovalDecision,
    requested_decision: journal::ApprovalDecision,
    now: i64,
) -> Result<Response, Response> {
    if existing_decision != requested_decision {
        return Err(compat_error_response(
            StatusCode::CONFLICT,
            "invalid_request_error",
            "approval_already_resolved",
            "approval has already reached a different terminal decision",
        ));
    }
    let status =
        load_compat_run_status_payload(state, run_id.as_str(), approval.principal.as_str()).await?;
    touch_compat_api_token(
        state,
        token.token_id.as_str(),
        "runs_approval",
        "approval_replayed",
        Some(run_id.as_str()),
        now,
    );
    Ok(Json(json!({
        "id": run_id,
        "object": "run.approval",
        "approval": compat_run_approval_payload(approval),
        "run": status,
        "_palyra": {
            "idempotent_replay": true,
        },
    }))
    .into_response())
}

fn compat_run_approval_payload(approval: &journal::ApprovalRecord) -> Value {
    json!({
        "approval_id": approval.approval_id,
        "run_id": approval.run_id,
        "session_id": approval.session_id,
        "subject_type": approval.subject_type.as_str(),
        "subject_id": approval.subject_id,
        "request_summary": approval.request_summary,
        "risk_level": approval.prompt.risk_level.as_str(),
        "requested_at_unix_ms": approval.requested_at_unix_ms,
        "resolved_at_unix_ms": approval.resolved_at_unix_ms,
        "decision": approval.decision.map(|decision| decision.as_str()),
        "decision_scope": approval.decision_scope.map(|scope| scope.as_str()),
        "decision_reason": approval.decision_reason,
        "decision_scope_ttl_ms": approval.decision_scope_ttl_ms,
        "version": approval.updated_at_unix_ms,
    })
}

async fn record_compat_run_flow_audit(
    state: &AppState,
    snapshot: &journal::OrchestratorRunStatusSnapshot,
    principal: &str,
    device_id: &str,
    reason: &str,
    details: Value,
    observed_at_unix_ms: i64,
) -> Result<(), Response> {
    let context = RequestContext {
        principal: principal.to_owned(),
        device_id: device_id.to_owned(),
        channel: Some(COMPAT_API_CHANNEL.to_owned()),
    };
    let payload = RuntimeDecisionPayload::new(
        RuntimeDecisionEventType::FlowLifecycle,
        RuntimeDecisionActor::new(
            RuntimeDecisionActorKind::Operator,
            principal,
            device_id,
            Some(COMPAT_API_CHANNEL.to_owned()),
        ),
        reason,
        "compat.runs.lifecycle.v1",
        RuntimeDecisionTiming::observed(observed_at_unix_ms),
    )
    .with_input(
        RuntimeEntityRef::new("target", "run", snapshot.run_id.clone())
            .with_state(snapshot.state.clone()),
    )
    .with_details(details);
    state
        .runtime
        .record_runtime_decision_event(
            &context,
            Some(snapshot.session_id.as_str()),
            Some(snapshot.run_id.as_str()),
            payload,
        )
        .await
        .map_err(runtime_status_response)
}

fn compat_approval_not_found_response(run_id: &str) -> Response {
    compat_error_response(
        StatusCode::NOT_FOUND,
        "invalid_request_error",
        "approval_not_found",
        format!("no pending approval was found for run '{run_id}'"),
    )
}

fn validate_compat_run_create_mode(
    query_mode: Option<&str>,
    body_mode: Option<&str>,
) -> CompatHttpResult<()> {
    for raw_mode in [query_mode, body_mode].into_iter().flatten() {
        let mode = raw_mode.trim();
        if mode.is_empty() || mode.eq_ignore_ascii_case("accepted") {
            continue;
        }
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_run_mode",
            "run create mode must be 'accepted' when provided",
        )
        .into());
    }
    Ok(())
}

fn compat_run_wait_timeout_ms(raw_timeout_ms: Option<u64>) -> u64 {
    raw_timeout_ms.unwrap_or(30_000).clamp(1, 120_000)
}

fn compat_run_status_url(run_id: &str) -> String {
    format!("/v1/runs/{run_id}")
}

fn compat_run_events_url(run_id: &str) -> String {
    format!("/v1/runs/{run_id}/events")
}

fn build_compat_run_wait_payload(
    run_id: &str,
    timeout_ms: u64,
    timed_out: bool,
    canonical_state: Option<&str>,
    run: Value,
) -> Value {
    let status = if timed_out {
        "timeout"
    } else {
        run.get("status").and_then(Value::as_str).unwrap_or("unknown")
    };
    json!({
        "id": run_id,
        "run_id": run_id,
        "object": "run.wait",
        "status": status,
        "timed_out": timed_out,
        "timeout_ms": timeout_ms,
        "canonical_state": canonical_state,
        "status_url": compat_run_status_url(run_id),
        "events_url": compat_run_events_url(run_id),
        "run": run,
    })
}

fn build_compat_run_status_payload_from_prepared(prepared: &CompatPreparedRun) -> Value {
    json!({
        "id": prepared.run_id,
        "run_id": prepared.run_id,
        "object": "run",
        "status": "queued",
        "queue_state": "accepted",
        "active_phase": "queued",
        "accepted_at": prepared.created_at_unix_ms / 1_000,
        "accepted_at_unix_ms": prepared.created_at_unix_ms,
        "session_id": prepared.session_id,
        "status_url": compat_run_status_url(prepared.run_id.as_str()),
        "events_url": compat_run_events_url(prepared.run_id.as_str()),
        "model": prepared.model_name,
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
        "pending_approval": Value::Null,
        "verification_summary": compat_run_verification_summary(None, None),
        "last_error": Value::Null,
        "_palyra": {
            "principal": prepared.principal,
            "device_id": prepared.device_id,
            "channel": COMPAT_API_CHANNEL,
            "origin": "runs_api",
        },
    })
}

fn build_compat_run_status_payload(
    snapshot: &journal::OrchestratorRunStatusSnapshot,
    pending_approval: Option<&journal::ApprovalRecord>,
) -> Value {
    json!({
        "id": snapshot.run_id,
        "run_id": snapshot.run_id,
        "object": "run",
        "status": compat_run_public_status(snapshot.state.as_str()),
        "queue_state": compat_run_queue_state(snapshot.state.as_str()),
        "active_phase": compat_run_active_phase(snapshot.state.as_str(), pending_approval.is_some()),
        "accepted_at": snapshot.created_at_unix_ms / 1_000,
        "accepted_at_unix_ms": snapshot.created_at_unix_ms,
        "started_at_unix_ms": snapshot.started_at_unix_ms,
        "completed_at_unix_ms": snapshot.completed_at_unix_ms,
        "session_id": snapshot.session_id,
        "status_url": compat_run_status_url(snapshot.run_id.as_str()),
        "events_url": compat_run_events_url(snapshot.run_id.as_str()),
        "usage": {
            "prompt_tokens": snapshot.prompt_tokens,
            "completion_tokens": snapshot.completion_tokens,
            "total_tokens": snapshot.total_tokens,
        },
        "pending_approval": pending_approval.map(compat_run_pending_approval_payload),
        "verification_summary": compat_run_verification_summary(
            snapshot.delegation.as_ref(),
            snapshot.merge_result.as_ref(),
        ),
        "last_error": snapshot.last_error,
        "_palyra": {
            "wire_state": snapshot.state,
            "cancel_requested": snapshot.cancel_requested,
            "cancel_reason": snapshot.cancel_reason,
            "origin_kind": snapshot.origin_kind,
            "origin_run_id": snapshot.origin_run_id,
            "parent_run_id": snapshot.parent_run_id,
            "triggered_by_principal": snapshot.triggered_by_principal,
            "tape_events": snapshot.tape_events,
        },
    })
}

fn compat_run_public_status(state: &str) -> &'static str {
    match state {
        "pending" | "accepted" => "queued",
        "in_progress" => "running",
        "done" => "completed",
        "failed" => "failed",
        "cancelled" => "cancelled",
        _ => "unknown",
    }
}

fn compat_run_queue_state(state: &str) -> &'static str {
    match state {
        "pending" => "pending",
        "accepted" => "accepted",
        "in_progress" => "draining",
        "done" | "failed" | "cancelled" => "empty",
        _ => "unknown",
    }
}

fn compat_run_active_phase(state: &str, pending_approval: bool) -> &'static str {
    if pending_approval {
        return "approval_pending";
    }
    match state {
        "pending" | "accepted" => "queued",
        "in_progress" => "running",
        "done" => "completed",
        "failed" => "failed",
        "cancelled" => "cancelled",
        _ => "unknown",
    }
}

fn compat_run_pending_approval_payload(approval: &journal::ApprovalRecord) -> Value {
    json!({
        "approval_id": approval.approval_id,
        "subject_type": approval.subject_type.as_str(),
        "subject_id": approval.subject_id,
        "request_summary": approval.request_summary,
        "risk_level": approval.prompt.risk_level.as_str(),
        "requested_at_unix_ms": approval.requested_at_unix_ms,
    })
}

fn compat_run_verification_summary(
    delegation: Option<&crate::delegation::DelegationSnapshot>,
    merge_result: Option<&crate::delegation::DelegationMergeResult>,
) -> Value {
    let delegation = delegation.and_then(|value| serde_json::to_value(value).ok());
    let merge_result = merge_result.and_then(|value| serde_json::to_value(value).ok());
    json!({
        "state": if merge_result.is_some() || delegation.is_some() {
            "available"
        } else {
            "not_available"
        },
        "delegation": delegation,
        "merge_result": merge_result,
    })
}

fn build_compat_run_events_streaming_response(
    state: AppState,
    initial_snapshot: journal::OrchestratorRunStatusSnapshot,
    after_seq: Option<i64>,
    requested_limit: Option<usize>,
) -> Response {
    let (sender, receiver) =
        mpsc::channel::<Result<Bytes, Infallible>>(COMPAT_SSE_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let run_id = initial_snapshot.run_id.clone();
        let session_id = initial_snapshot.session_id.clone();
        let created_at_unix_ms = initial_snapshot.created_at_unix_ms;
        let limit = requested_limit
            .unwrap_or(COMPAT_RUN_EVENTS_PAGE_LIMIT_DEFAULT)
            .clamp(1, COMPAT_RUN_EVENTS_PAGE_LIMIT_MAX);
        let mut cursor = after_seq;

        loop {
            let page = match state
                .runtime
                .orchestrator_tape_snapshot(run_id.clone(), cursor, Some(limit))
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    let _ = send_sse_event(
                        &sender,
                        "run.failed",
                        compat_error_payload(
                            "server_error",
                            "run_events_failed",
                            sanitize_http_error_message(error.message()),
                        ),
                    )
                    .await;
                    let _ = send_sse_done(&sender).await;
                    return;
                }
            };

            for record in page.events {
                cursor = Some(record.seq);
                let Some(public_event) = public_runtime_event_json_from_tape_record(
                    run_id.as_str(),
                    session_id.as_str(),
                    created_at_unix_ms,
                    &record,
                ) else {
                    continue;
                };
                let event_name = public_event
                    .get("event")
                    .and_then(Value::as_str)
                    .unwrap_or("runtime.event")
                    .to_owned();
                if !send_sse_event(&sender, event_name.as_str(), public_event).await {
                    return;
                }
            }

            if page.next_after_seq.is_some() {
                continue;
            }
            let snapshot =
                match state.runtime.orchestrator_run_status_snapshot(run_id.clone()).await {
                    Ok(Some(current)) => current,
                    Ok(None) => {
                        let _ = send_sse_done(&sender).await;
                        return;
                    }
                    Err(error) => {
                        let _ = send_sse_event(
                            &sender,
                            "run.failed",
                            compat_error_payload(
                                "server_error",
                                "run_events_status_failed",
                                sanitize_http_error_message(error.message()),
                            ),
                        )
                        .await;
                        let _ = send_sse_done(&sender).await;
                        return;
                    }
                };
            if compat_run_is_terminal(snapshot.state.as_str()) {
                let _ = send_sse_done(&sender).await;
                return;
            }
            if !send_sse_comment(&sender, "keepalive").await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream; charset=utf-8"));
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn public_runtime_event_json_from_tape_record(
    run_id: &str,
    session_id: &str,
    created_at_unix_ms: i64,
    record: &journal::OrchestratorTapeRecord,
) -> Option<Value> {
    let event = run_stream_event_from_tape_record(run_id, record)?;
    let sequence = u64::try_from(record.seq.saturating_add(1)).ok()?;
    let event_id =
        crate::application::run_stream::public_events::run_stream_public_event_id(run_id, sequence);
    crate::application::run_stream::public_events::public_runtime_event_json_from_run_stream_event(
        &event,
        crate::application::run_stream::public_events::PublicRunStreamEventContext {
            event_id: event_id.as_str(),
            session_id,
            occurred_at_unix_ms: created_at_unix_ms.saturating_add(record.seq.max(0)),
            request_id: None,
        },
    )
}

fn run_stream_event_from_tape_record(
    run_id: &str,
    record: &journal::OrchestratorTapeRecord,
) -> Option<common_v1::RunStreamEvent> {
    let payload = serde_json::from_str::<Value>(record.payload_json.as_str()).ok()?;
    let run_id = Some(common_v1::CanonicalId { ulid: run_id.to_owned() });
    let body = match record.event_type.as_str() {
        "status" => common_v1::run_stream_event::Body::Status(common_v1::StreamStatus {
            kind: compat_tape_status_kind(&payload) as i32,
            message: payload.get("message")?.as_str()?.to_owned(),
        }),
        "model_token" => common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
            token: payload.get("token")?.as_str()?.to_owned(),
            is_final: payload.get("is_final").and_then(Value::as_bool).unwrap_or(false),
        }),
        "tool_proposal" => {
            common_v1::run_stream_event::Body::ToolProposal(common_v1::ToolProposal {
                proposal_id: compat_tape_canonical_id(&payload, "proposal_id"),
                tool_name: payload.get("tool_name")?.as_str()?.to_owned(),
                input_json: compat_tape_json_bytes(payload.get("input_json")),
                approval_required: payload
                    .get("approval_required")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        }
        "tool_decision" => {
            common_v1::run_stream_event::Body::ToolDecision(common_v1::ToolDecision {
                proposal_id: compat_tape_canonical_id(&payload, "proposal_id"),
                kind: if payload.get("kind").and_then(Value::as_str) == Some("allow") {
                    common_v1::tool_decision::DecisionKind::Allow as i32
                } else {
                    common_v1::tool_decision::DecisionKind::Deny as i32
                },
                reason: payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                approval_required: payload
                    .get("approval_required")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                policy_enforced: payload
                    .get("policy_enforced")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        }
        "tool_result" => common_v1::run_stream_event::Body::ToolResult(common_v1::ToolResult {
            proposal_id: compat_tape_canonical_id(&payload, "proposal_id"),
            success: payload.get("success").and_then(Value::as_bool).unwrap_or(false),
            output_json: compat_tape_json_bytes(payload.get("output_json")),
            error: payload.get("error").and_then(Value::as_str).unwrap_or_default().to_owned(),
        }),
        "tool_approval_request" => {
            common_v1::run_stream_event::Body::ToolApprovalRequest(common_v1::ToolApprovalRequest {
                proposal_id: compat_tape_canonical_id(&payload, "proposal_id"),
                tool_name: payload.get("tool_name")?.as_str()?.to_owned(),
                input_json: compat_tape_json_bytes(payload.get("input_json")),
                approval_required: payload
                    .get("approval_required")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                approval_id: compat_tape_canonical_id(&payload, "approval_id"),
                prompt: compat_tape_approval_prompt(payload.get("prompt")),
                request_summary: payload
                    .get("request_summary")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            })
        }
        "tool_approval_response" => common_v1::run_stream_event::Body::ToolApprovalResponse(
            common_v1::ToolApprovalResponse {
                proposal_id: compat_tape_canonical_id(&payload, "proposal_id"),
                approved: payload.get("approved").and_then(Value::as_bool).unwrap_or(false),
                reason: payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                approval_id: compat_tape_canonical_id(&payload, "approval_id"),
                decision_scope: compat_tape_approval_scope(
                    payload.get("decision_scope").and_then(Value::as_str),
                ) as i32,
                decision_scope_ttl_ms: payload
                    .get("decision_scope_ttl_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
            },
        ),
        _ => return None,
    };
    Some(common_v1::RunStreamEvent {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        run_id,
        body: Some(body),
    })
}

fn compat_tape_status_kind(payload: &Value) -> common_v1::stream_status::StatusKind {
    match payload
        .get("wire_kind")
        .or_else(|| payload.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "accepted" => common_v1::stream_status::StatusKind::Accepted,
        "in_progress" => common_v1::stream_status::StatusKind::InProgress,
        "done" => common_v1::stream_status::StatusKind::Done,
        "failed" | "cancelled" | "needs_continuation" => {
            common_v1::stream_status::StatusKind::Failed
        }
        _ => common_v1::stream_status::StatusKind::Unspecified,
    }
}

fn compat_tape_canonical_id(payload: &Value, field: &str) -> Option<common_v1::CanonicalId> {
    payload.get(field).and_then(Value::as_str).and_then(|value| {
        trim_to_option(value.to_owned()).map(|ulid| common_v1::CanonicalId { ulid })
    })
}

fn compat_tape_json_bytes(value: Option<&Value>) -> Vec<u8> {
    serde_json::to_vec(value.unwrap_or(&Value::Null)).unwrap_or_else(|_| b"null".to_vec())
}

fn compat_tape_approval_prompt(value: Option<&Value>) -> Option<common_v1::ApprovalPrompt> {
    let value = value?;
    let options = value
        .get("options")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|option| common_v1::ApprovalOption {
                    option_id: option
                        .get("option_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    label: option
                        .get("label")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    description: option
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    default_selected: option
                        .get("default_selected")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    decision_scope: compat_tape_approval_scope(
                        option.get("decision_scope").and_then(Value::as_str),
                    ) as i32,
                    timebox_ttl_ms: option
                        .get("timebox_ttl_ms")
                        .and_then(Value::as_i64)
                        .unwrap_or_default(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(common_v1::ApprovalPrompt {
        title: value.get("title").and_then(Value::as_str).unwrap_or_default().to_owned(),
        risk_level: compat_tape_approval_risk(value.get("risk_level").and_then(Value::as_str))
            as i32,
        subject_id: value.get("subject_id").and_then(Value::as_str).unwrap_or_default().to_owned(),
        summary: value.get("summary").and_then(Value::as_str).unwrap_or_default().to_owned(),
        options,
        timeout_seconds: value.get("timeout_seconds").and_then(Value::as_u64).unwrap_or(0) as u32,
        details_json: compat_tape_json_bytes(value.get("details_json")),
        policy_explanation: value
            .get("policy_explanation")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    })
}

fn compat_tape_approval_scope(value: Option<&str>) -> common_v1::ApprovalDecisionScope {
    match value.unwrap_or_default() {
        "once" => common_v1::ApprovalDecisionScope::Once,
        "session" => common_v1::ApprovalDecisionScope::Session,
        "timeboxed" => common_v1::ApprovalDecisionScope::Timeboxed,
        _ => common_v1::ApprovalDecisionScope::Unspecified,
    }
}

fn compat_tape_approval_risk(value: Option<&str>) -> common_v1::ApprovalRiskLevel {
    match value.unwrap_or_default() {
        "low" => common_v1::ApprovalRiskLevel::Low,
        "medium" => common_v1::ApprovalRiskLevel::Medium,
        "high" => common_v1::ApprovalRiskLevel::High,
        "critical" => common_v1::ApprovalRiskLevel::Critical,
        _ => common_v1::ApprovalRiskLevel::Unspecified,
    }
}

fn compat_run_is_terminal(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled")
}

fn compat_run_not_found_response(run_id: &str) -> Response {
    compat_error_response(
        StatusCode::NOT_FOUND,
        "invalid_request_error",
        "run_not_found",
        format!("run was not found: {run_id}"),
    )
}

fn compat_journal_error_response(error: journal::JournalError) -> Response {
    match error {
        journal::JournalError::CompatResponseNotFound { .. } => compat_error_response(
            StatusCode::NOT_FOUND,
            "invalid_request_error",
            "response_not_found",
            "response was not found or its public view was deleted",
        ),
        journal::JournalError::CompatResponseScopeMismatch { .. } => compat_error_response(
            StatusCode::NOT_FOUND,
            "invalid_request_error",
            "response_not_found",
            "response was not found or its public view was deleted",
        ),
        journal::JournalError::PayloadTooLarge { .. } => compat_error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "invalid_request_error",
            "response_store_payload_too_large",
            error.to_string(),
        ),
        journal::JournalError::InvalidArgument(_) => compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "response_store_invalid_request",
            error.to_string(),
        ),
        _ => compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "response_store_failed",
            error.to_string(),
        ),
    }
}

async fn prepare_compat_run(
    state: &AppState,
    headers: &HeaderMap,
    requested_model: Option<&str>,
    user: Option<&str>,
    metadata: Option<&Value>,
    prompt_text: String,
    required_scope: &str,
) -> Result<CompatPreparedRun, Response> {
    let context =
        authorize_compat_run_context(state, headers, requested_model, metadata, required_scope)?;
    prepare_compat_run_from_context(state, context, user, prompt_text).await
}

fn authorize_compat_run_context(
    state: &AppState,
    headers: &HeaderMap,
    requested_model: Option<&str>,
    metadata: Option<&Value>,
    required_scope: &str,
) -> CompatHttpResult<CompatAuthorizedRunContext> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let token = authorize_compat_api_token(state, headers, required_scope, None, now)?;
    enforce_compat_rate_limit(state, token.token_id.as_str(), token.rate_limit_per_minute)?;

    let provider = state.runtime.model_provider_status_snapshot();
    let model_name = validate_compat_requested_model(&provider, requested_model)?;
    let provider_kind = provider.kind;
    let overrides = parse_compat_request_overrides(metadata)?;
    let (principal, device_id) = resolve_compat_runtime_identity(state, &token, required_scope)?;
    Ok(CompatAuthorizedRunContext {
        token,
        provider_kind,
        model_name,
        overrides,
        principal,
        device_id,
    })
}

async fn prepare_compat_run_from_context(
    state: &AppState,
    context: CompatAuthorizedRunContext,
    user: Option<&str>,
    prompt_text: String,
) -> Result<CompatPreparedRun, Response> {
    let CompatAuthorizedRunContext {
        token,
        provider_kind,
        model_name,
        overrides,
        principal,
        device_id,
    } = context;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let session_id = overrides.session_id.clone();
    let session_key = if session_id.is_some() && overrides.session_key.is_none() {
        None
    } else {
        Some(derive_compat_session_key(&token, user, overrides.session_key.as_deref()))
    };
    let session = state
        .runtime
        .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
            session_id,
            session_key,
            session_label: overrides.session_label,
            principal: principal.clone(),
            device_id: device_id.clone(),
            channel: Some(COMPAT_API_CHANNEL.to_owned()),
            require_existing: overrides.require_existing,
            reset_session: overrides.reset_session,
        })
        .await
        .map_err(runtime_status_response)?;
    let run_id = Ulid::new().to_string();
    let created_at_unix_ms = now;
    let (request_sender, request_receiver) = mpsc::channel::<common_v1::RunStreamRequest>(8);
    request_sender
        .send(common_v1::RunStreamRequest {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(common_v1::CanonicalId { ulid: session.session.session_id.clone() }),
            run_id: Some(common_v1::CanonicalId { ulid: run_id.clone() }),
            input: Some(build_compat_message_envelope(
                session.session.session_id.as_str(),
                token.label.as_str(),
                token.principal.as_str(),
                prompt_text,
                created_at_unix_ms,
            )),
            allow_sensitive_tools: false,
            session_key: String::new(),
            session_label: String::new(),
            reset_session: false,
            require_existing: true,
            tool_approval_response: None,
            origin_kind: "compat_api".to_owned(),
            origin_run_id: None,
            parameter_delta_json: Vec::new(),
            queued_input_id: None,
        })
        .await
        .map_err(|_| {
            compat_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "queue_failed",
                "failed to queue compat run request",
            )
        })?;
    let mut run_request = TonicRequest::new(ReceiverStream::new(request_receiver));
    apply_console_request_context(
        state,
        principal.as_str(),
        device_id.as_str(),
        Some(COMPAT_API_CHANNEL),
        run_request.metadata_mut(),
    )?;
    Ok(CompatPreparedRun {
        token,
        provider_kind,
        model_name,
        run_id,
        session_id: session.session.session_id,
        principal,
        device_id,
        created_at_unix_ms,
        request_sender,
        run_request,
    })
}

fn resolve_compat_runtime_identity(
    state: &AppState,
    token: &AuthenticatedApiToken,
    required_scope: &str,
) -> CompatHttpResult<(String, String)> {
    let registry = lock_access_registry(&state.access_registry);
    let workspace_access = registry
        .resolve_workspace_access_for_token(token, required_scope)
        .map_err(access_registry_to_compat_response)?;
    if let Some(workspace_access) = workspace_access {
        Ok((workspace_access.runtime_principal, workspace_access.runtime_device_id))
    } else {
        Ok((token.principal.clone(), token.token_id.clone()))
    }
}

async fn execute_compat_run(
    state: &AppState,
    prepared: CompatPreparedRun,
) -> Result<CompatExecutionResult, Response> {
    let CompatPreparedRun { run_id, session_id, request_sender, run_request, .. } = prepared;
    let gateway_client = build_compat_gateway_endpoint(state).map_err(|error| {
        compat_error_response(StatusCode::BAD_GATEWAY, "server_error", "gateway_unavailable", error)
    })?;
    let channel = gateway_client.connect().await.map_err(|error| {
        compat_error_response(
            StatusCode::BAD_GATEWAY,
            "server_error",
            "gateway_unavailable",
            format!("failed to connect compat API to gateway: {error}"),
        )
    })?;
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::new(channel);
    let mut stream = client
        .run_stream(run_request)
        .await
        .map_err(|error| {
            compat_error_response(
                StatusCode::BAD_GATEWAY,
                "server_error",
                "gateway_stream_failed",
                sanitize_http_error_message(error.message()),
            )
        })?
        .into_inner();

    let mut content = String::new();
    let mut tool_calls = Vec::new();
    let mut finish_reason = "stop";
    let mut final_error = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => match event.body {
                Some(common_v1::run_stream_event::Body::ModelToken(token)) => {
                    content.push_str(token.token.as_str());
                }
                Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => {
                    finish_reason = "tool_calls";
                    tool_calls.push(CompatToolCall {
                        id: proposal
                            .proposal_id
                            .as_ref()
                            .map(|value| value.ulid.clone())
                            .unwrap_or_else(|| Ulid::new().to_string()),
                        name: proposal.tool_name,
                        arguments: json_string_from_bytes(proposal.input_json.as_slice()),
                    });
                }
                Some(common_v1::run_stream_event::Body::ToolApprovalRequest(request)) => {
                    auto_deny_compat_tool_approval(
                        &request_sender,
                        session_id.as_str(),
                        run_id.as_str(),
                        &request,
                    )
                    .await;
                }
                Some(common_v1::run_stream_event::Body::Status(status))
                    if common_v1::stream_status::StatusKind::try_from(status.kind)
                        .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
                        == common_v1::stream_status::StatusKind::Failed =>
                {
                    final_error =
                        Some(sanitize_http_error_message(status.message.as_str()).to_owned());
                }
                _ => {}
            },
            Err(error) => {
                final_error = Some(sanitize_http_error_message(error.message()).to_owned());
                break;
            }
        }
    }

    let snapshot = stateful_run_snapshot(state, run_id.as_str()).await?;
    if let Some(error) = final_error.or_else(|| snapshot.last_error.clone()) {
        return Err(compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "run_failed",
            error,
        ));
    }
    Ok(CompatExecutionResult { content, tool_calls, finish_reason, snapshot })
}

fn build_compat_chat_completion_payload(result: &CompatExecutionResult) -> Value {
    json!({
        "id": compat_completion_id(result.snapshot.run_id.as_str()),
        "object": "chat.completion",
        "created": result.snapshot.created_at_unix_ms / 1_000,
        "model": "palyra-compat",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": if result.content.is_empty() { Value::Null } else { Value::String(result.content.clone()) },
                "tool_calls": result.tool_calls.iter().map(compat_tool_call_json).collect::<Vec<Value>>(),
            },
            "finish_reason": result.finish_reason,
        }],
        "usage": compat_usage_json(&result.snapshot),
        "_palyra": compat_interop_json(&result.snapshot),
    })
}

fn build_compat_responses_payload(result: &CompatExecutionResult) -> Value {
    build_compat_responses_payload_from_parts(
        format!("resp_{}", result.snapshot.run_id),
        &result.snapshot,
        "completed",
        result.content.clone(),
        result.tool_calls.as_slice(),
        None,
    )
}

fn build_compat_responses_payload_from_parts(
    response_id: String,
    snapshot: &journal::OrchestratorRunStatusSnapshot,
    status: &str,
    content: String,
    tool_calls: &[CompatToolCall],
    error: Option<Value>,
) -> Value {
    let mut payload = json!({
        "id": response_id,
        "object": "response",
        "created": snapshot.created_at_unix_ms / 1_000,
        "status": status,
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": content,
            }],
        }],
        "tool_calls": tool_calls.iter().map(compat_tool_call_json).collect::<Vec<Value>>(),
        "usage": compat_usage_json(snapshot),
        "_palyra": compat_interop_json(snapshot),
    });
    if let Some(error) = error {
        payload["error"] = error;
    }
    payload
}

fn build_compat_responses_created_stream_payload(
    response_id: &str,
    run_id: &str,
    session_id: &str,
    created_seconds: i64,
    model_name: &str,
) -> Value {
    json!({
        "type": "response.created",
        "response": {
            "id": response_id,
            "object": "response",
            "created": created_seconds,
            "status": "in_progress",
            "model": model_name,
            "output": [],
            "usage": Value::Null,
            "_palyra": compat_stream_palyra_metadata(run_id, session_id, None),
        },
    })
}

fn build_compat_responses_tool_call_added_stream_payload(
    response_id: &str,
    run_id: &str,
    session_id: &str,
    tool_call: &CompatStreamToolCall,
    public_event: Option<&Value>,
) -> Value {
    json!({
        "type": "response.output_item.added",
        "response_id": response_id,
        "output_index": tool_call.output_index,
        "item": {
            "id": tool_call.id,
            "type": "function_call",
            "status": "in_progress",
            "call_id": tool_call.id,
            "name": tool_call.name,
            "arguments": "",
        },
        "_palyra": compat_stream_palyra_metadata_sanitized(
            run_id,
            session_id,
            public_event
        ),
    })
}

fn build_compat_responses_tool_call_arguments_delta_stream_payload(
    response_id: &str,
    run_id: &str,
    session_id: &str,
    tool_call: &CompatStreamToolCall,
    public_event: Option<&Value>,
) -> Value {
    json!({
        "type": "response.function_call_arguments.delta",
        "response_id": response_id,
        "item_id": tool_call.id,
        "output_index": tool_call.output_index,
        "delta": tool_call.arguments,
        "_palyra": compat_stream_palyra_metadata_sanitized(
            run_id,
            session_id,
            public_event
        ),
    })
}

fn build_compat_responses_tool_call_arguments_done_stream_payload(
    response_id: &str,
    run_id: &str,
    session_id: &str,
    tool_call: &CompatStreamToolCall,
    public_event: Option<&Value>,
) -> Value {
    json!({
        "type": "response.function_call_arguments.done",
        "response_id": response_id,
        "item_id": tool_call.id,
        "output_index": tool_call.output_index,
        "arguments": tool_call.arguments,
        "_palyra": compat_stream_palyra_metadata_sanitized(
            run_id,
            session_id,
            public_event
        ),
    })
}

fn build_compat_responses_tool_result_stream_payload(
    response_id: &str,
    run_id: &str,
    session_id: &str,
    tool_call: Option<&CompatStreamToolCall>,
    result: &common_v1::ToolResult,
    public_event: Option<&Value>,
) -> Value {
    let fallback_id = compat_tool_call_id(result.proposal_id.as_ref())
        .unwrap_or_else(|| "tool_unknown".to_owned());
    let item_id = tool_call.map(|value| value.id.as_str()).unwrap_or(fallback_id.as_str());
    let name = tool_call.map(|value| value.name.as_str()).unwrap_or("unknown");
    let arguments = tool_call.map(|value| value.arguments.as_str()).unwrap_or_default();
    let output_index = tool_call.map(|value| value.output_index).unwrap_or(1);
    let error = if result.error.trim().is_empty() {
        Value::Null
    } else {
        Value::String(sanitize_http_error_message(result.error.as_str()))
    };
    let has_output = !result.output_json.is_empty();
    let output_ref = if has_output {
        json!({
            "kind": "run_journal_tool_output",
            "run_id": run_id,
            "tool_call_id": item_id,
        })
    } else {
        Value::Null
    };

    json!({
        "type": "response.output_item.done",
        "response_id": response_id,
        "output_index": output_index,
        "item": {
            "id": item_id,
            "type": "function_call",
            "status": if result.success { "completed" } else { "failed" },
            "call_id": item_id,
            "name": name,
            "arguments": arguments,
        },
        "tool_result": {
            "success": result.success,
            "error": error,
            "output_visibility": if has_output { "artifact_ref" } else { "none" },
            "output_bytes": result.output_json.len(),
            "output_ref": output_ref,
        },
        "_palyra": compat_stream_palyra_metadata_sanitized(
            run_id,
            session_id,
            public_event
        ),
    })
}

fn build_compat_responses_approval_required_stream_payload(
    response_id: &str,
    run_id: &str,
    session_id: &str,
    request: &common_v1::ToolApprovalRequest,
    public_event: Option<&Value>,
) -> Value {
    let approval_id =
        compat_approval_response_id(request.approval_id.as_ref(), request.proposal_id.as_ref());
    let tool_call_id = compat_tool_call_id(request.proposal_id.as_ref())
        .unwrap_or_else(|| "tool_unknown".to_owned());
    let prompt = request.prompt.as_ref();
    let summary = if request.request_summary.trim().is_empty() {
        prompt.map(|value| value.summary.as_str()).unwrap_or_default()
    } else {
        request.request_summary.as_str()
    };

    json!({
        "type": "approval.required",
        "response_id": response_id,
        "approval_id": approval_id,
        "tool_call_id": tool_call_id,
        "tool_name": request.tool_name,
        "summary": summary,
        "approval_required": request.approval_required,
        "risk_level": prompt
            .map(|value| compat_approval_risk_level_label(value.risk_level))
            .unwrap_or("unspecified"),
        "prompt": prompt.map(|value| {
            json!({
                "title": value.title,
                "summary": value.summary,
                "subject_id": value.subject_id,
                "timeout_seconds": value.timeout_seconds,
                "policy_explanation": value.policy_explanation,
                "options": value.options.iter().map(|option| {
                    json!({
                        "option_id": option.option_id,
                        "label": option.label,
                        "description": option.description,
                        "default_selected": option.default_selected,
                        "decision_scope": compat_approval_scope_label(option.decision_scope),
                        "timebox_ttl_ms": option.timebox_ttl_ms,
                    })
                }).collect::<Vec<_>>(),
            })
        }),
        "_palyra": compat_stream_palyra_metadata_sanitized(
            run_id,
            session_id,
            public_event
        ),
    })
}

fn build_compat_responses_approval_resolved_stream_payload(
    response_id: &str,
    run_id: &str,
    session_id: &str,
    response: &common_v1::ToolApprovalResponse,
    public_event: Option<&Value>,
) -> Value {
    json!({
        "type": "approval.resolved",
        "response_id": response_id,
        "approval_id": compat_approval_response_id(
            response.approval_id.as_ref(),
            response.proposal_id.as_ref()
        ),
        "tool_call_id": compat_tool_call_id(response.proposal_id.as_ref())
            .unwrap_or_else(|| "tool_unknown".to_owned()),
        "approved": response.approved,
        "reason": sanitize_http_error_message(response.reason.as_str()),
        "decision_scope": compat_approval_scope_label(response.decision_scope),
        "decision_scope_ttl_ms": response.decision_scope_ttl_ms,
        "_palyra": compat_stream_palyra_metadata_sanitized(
            run_id,
            session_id,
            public_event
        ),
    })
}

fn compat_tool_call_json(tool_call: &CompatToolCall) -> Value {
    json!({
        "id": tool_call.id,
        "type": "function",
        "function": {
            "name": tool_call.name,
            "arguments": tool_call.arguments,
        },
    })
}

fn compat_usage_json(snapshot: &journal::OrchestratorRunStatusSnapshot) -> Value {
    json!({
        "prompt_tokens": snapshot.prompt_tokens,
        "completion_tokens": snapshot.completion_tokens,
        "total_tokens": snapshot.total_tokens,
    })
}

fn compat_interop_json(snapshot: &journal::OrchestratorRunStatusSnapshot) -> Value {
    json!({
        "origin": "compat_api",
        "run_id": snapshot.run_id,
        "session_id": snapshot.session_id,
        "approval_mode": "shared_palyra_approvals",
    })
}

fn compat_stream_palyra_metadata(
    run_id: &str,
    session_id: &str,
    public_event: Option<&Value>,
) -> Value {
    let mut metadata = json!({
        "origin": "compat_api",
        "run_id": run_id,
        "session_id": session_id,
        "approval_mode": "shared_palyra_approvals",
    });
    if let (Some(object), Some(public_event)) = (metadata.as_object_mut(), public_event) {
        object.insert("public_event_type".to_owned(), public_event["event"].clone());
        object.insert("public_event".to_owned(), public_event.clone());
    }
    metadata
}

fn compat_stream_palyra_metadata_sanitized(
    run_id: &str,
    session_id: &str,
    public_event: Option<&Value>,
) -> Value {
    let sanitized_event = public_event.map(sanitize_public_event_for_compat_stream);
    compat_stream_palyra_metadata(run_id, session_id, sanitized_event.as_ref())
}

fn sanitize_public_event_for_compat_stream(public_event: &Value) -> Value {
    let mut sanitized = public_event.clone();
    let event_name = sanitized.get("event").and_then(Value::as_str).map(str::to_owned);
    if let Some(payload) = sanitized.get_mut("payload").and_then(Value::as_object_mut) {
        match event_name.as_deref() {
            Some("tool.call.started") => {
                payload.insert("input_json".to_owned(), json!({ "visibility": "withheld" }));
            }
            Some("tool.call.completed") => {
                payload.insert("output_json".to_owned(), json!({ "visibility": "artifact_ref" }));
            }
            Some("approval.required") => {
                payload.insert("input_json".to_owned(), json!({ "visibility": "withheld" }));
                if let Some(prompt) = payload.get_mut("prompt").and_then(Value::as_object_mut) {
                    prompt.insert("details_json".to_owned(), json!({ "visibility": "withheld" }));
                }
            }
            _ => {}
        }
    }
    sanitized
}

fn compat_tool_call_id(id: Option<&common_v1::CanonicalId>) -> Option<String> {
    id.and_then(|value| {
        let ulid = value.ulid.trim();
        (!ulid.is_empty()).then(|| ulid.to_owned())
    })
}

fn compat_approval_response_id(
    approval_id: Option<&common_v1::CanonicalId>,
    proposal_id: Option<&common_v1::CanonicalId>,
) -> String {
    compat_tool_call_id(approval_id).unwrap_or_else(|| {
        let tool_call_id =
            compat_tool_call_id(proposal_id).unwrap_or_else(|| "tool_unknown".to_owned());
        format!("approval_{tool_call_id}")
    })
}

fn compat_approval_scope_label(raw: i32) -> &'static str {
    match common_v1::ApprovalDecisionScope::try_from(raw)
        .unwrap_or(common_v1::ApprovalDecisionScope::Unspecified)
    {
        common_v1::ApprovalDecisionScope::Once => "once",
        common_v1::ApprovalDecisionScope::Session => "session",
        common_v1::ApprovalDecisionScope::Timeboxed => "timeboxed",
        common_v1::ApprovalDecisionScope::Unspecified => "unspecified",
    }
}

fn compat_approval_risk_level_label(raw: i32) -> &'static str {
    match common_v1::ApprovalRiskLevel::try_from(raw)
        .unwrap_or(common_v1::ApprovalRiskLevel::Unspecified)
    {
        common_v1::ApprovalRiskLevel::Low => "low",
        common_v1::ApprovalRiskLevel::Medium => "medium",
        common_v1::ApprovalRiskLevel::High => "high",
        common_v1::ApprovalRiskLevel::Critical => "critical",
        common_v1::ApprovalRiskLevel::Unspecified => "unspecified",
    }
}

fn build_compat_chat_streaming_response(
    state: AppState,
    prepared: CompatPreparedRun,
    cancel_on_disconnect: bool,
) -> Response {
    let (sender, receiver) =
        mpsc::channel::<Result<Bytes, Infallible>>(COMPAT_SSE_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let CompatPreparedRun {
            token,
            provider_kind,
            model_name,
            run_id,
            session_id,
            principal,
            device_id,
            created_at_unix_ms,
            request_sender,
            run_request,
        } = prepared;
        let stream_context = CompatChatStreamContext {
            token_id: token.token_id.clone(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            principal,
            device_id,
            cancel_on_disconnect,
            created_at_unix_ms,
        };
        let response_id = compat_completion_id(run_id.as_str());
        let created_seconds = created_at_unix_ms / 1_000;
        let mut finish_reason = "stop";
        let mut stream_error = None::<String>;
        let mut tool_call_index = 0usize;
        let mut public_event_sequence = 0_u64;
        let mut last_public_terminal_event = None::<Value>;

        if !send_compat_chat_sse_data(
            &sender,
            &state,
            &stream_context,
            json!({
                "id": response_id,
                "object": "chat.completion.chunk",
                "created": created_seconds,
                "model": model_name,
                "system_fingerprint": provider_kind,
                "choices": [{
                    "index": 0,
                    "delta": { "role": "assistant" },
                    "finish_reason": Value::Null,
                }],
                "_palyra": compat_stream_palyra_metadata(run_id.as_str(), session_id.as_str(), None),
            }),
        )
        .await
        {
            return;
        }
        if !send_compat_chat_sse_comment(&sender, &state, &stream_context, "keepalive").await {
            return;
        }

        let endpoint = match build_compat_gateway_endpoint(&state) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                let _ = send_compat_chat_failed_stream_event(
                    &sender,
                    &state,
                    &stream_context,
                    CompatChatFailedStreamEvent {
                        response_id: response_id.as_str(),
                        created_seconds,
                        model_name: model_name.as_str(),
                        code: "gateway_unavailable",
                        message: error,
                        public_event: None,
                    },
                )
                .await;
                let _ = send_sse_done(&sender).await;
                return;
            }
        };
        let channel = match endpoint.connect().await {
            Ok(channel) => channel,
            Err(error) => {
                let _ = send_compat_chat_failed_stream_event(
                    &sender,
                    &state,
                    &stream_context,
                    CompatChatFailedStreamEvent {
                        response_id: response_id.as_str(),
                        created_seconds,
                        model_name: model_name.as_str(),
                        code: "gateway_unavailable",
                        message: format!("failed to connect compat API to gateway: {error}"),
                        public_event: None,
                    },
                )
                .await;
                let _ = send_sse_done(&sender).await;
                return;
            }
        };
        let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::new(channel);
        let mut stream = match client.run_stream(run_request).await {
            Ok(response) => response.into_inner(),
            Err(error) => {
                let _ = send_compat_chat_failed_stream_event(
                    &sender,
                    &state,
                    &stream_context,
                    CompatChatFailedStreamEvent {
                        response_id: response_id.as_str(),
                        created_seconds,
                        model_name: model_name.as_str(),
                        code: "gateway_stream_failed",
                        message: sanitize_http_error_message(error.message()).to_owned(),
                        public_event: None,
                    },
                )
                .await;
                let _ = send_sse_done(&sender).await;
                return;
            }
        };

        let mut keepalive = tokio::time::interval(COMPAT_SSE_KEEPALIVE_INTERVAL);
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = keepalive.tick() => {
                    if !send_compat_chat_sse_comment(&sender, &state, &stream_context, "keepalive").await {
                        return;
                    }
                }
                item = stream.next() => {
                    let Some(item) = item else {
                        break;
                    };
                    match item {
                Ok(event) => {
                    public_event_sequence = public_event_sequence.saturating_add(1);
                    let public_event_id =
                        crate::application::run_stream::public_events::run_stream_public_event_id(
                            run_id.as_str(),
                            public_event_sequence,
                        );
                    let public_event = crate::application::run_stream::public_events::
                        public_runtime_event_json_from_run_stream_event(
                            &event,
                            crate::application::run_stream::public_events::PublicRunStreamEventContext {
                                event_id: public_event_id.as_str(),
                                session_id: session_id.as_str(),
                                occurred_at_unix_ms: unix_ms_now().unwrap_or(created_at_unix_ms),
                                request_id: None,
                            },
                        );
                    match event.body {
                        Some(common_v1::run_stream_event::Body::ModelToken(token_event))
                            if !send_compat_chat_sse_data(
                                &sender,
                                &state,
                                &stream_context,
                                json!({
                                    "id": response_id,
                                    "object": "chat.completion.chunk",
                                    "created": created_seconds,
                                    "model": model_name,
                                    "choices": [{
                                        "index": 0,
                                        "delta": { "content": token_event.token },
                                        "finish_reason": Value::Null,
                                    }],
                                    "_palyra": compat_stream_palyra_metadata(
                                        run_id.as_str(),
                                        session_id.as_str(),
                                        public_event.as_ref()
                                    ),
                                }),
                            )
                            .await =>
                        {
                            return;
                        }
                        Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => {
                            finish_reason = "tool_calls";
                            let tool_call_id = proposal
                                .proposal_id
                                .as_ref()
                                .map(|value| value.ulid.clone())
                                .unwrap_or_else(|| Ulid::new().to_string());
                            if !send_compat_chat_sse_data(
                                &sender,
                                &state,
                                &stream_context,
                                json!({
                                    "id": response_id,
                                    "object": "chat.completion.chunk",
                                    "created": created_seconds,
                                    "model": model_name,
                                    "choices": [{
                                        "index": 0,
                                        "delta": {
                                            "tool_calls": [{
                                                "index": tool_call_index,
                                                "id": tool_call_id,
                                                "type": "function",
                                                "function": {
                                                    "name": proposal.tool_name,
                                                    "arguments": json_string_from_bytes(proposal.input_json.as_slice()),
                                                },
                                            }],
                                        },
                                        "finish_reason": Value::Null,
                                    }],
                                    "_palyra": compat_stream_palyra_metadata(
                                        run_id.as_str(),
                                        session_id.as_str(),
                                        public_event.as_ref()
                                    ),
                                }),
                            )
                            .await
                            {
                                return;
                            }
                            tool_call_index = tool_call_index.saturating_add(1);
                        }
                        Some(common_v1::run_stream_event::Body::ToolApprovalRequest(request)) => {
                            finish_reason = "approval_required";
                            auto_deny_compat_tool_approval(
                                &request_sender,
                                session_id.as_str(),
                                run_id.as_str(),
                                &request,
                            )
                            .await;
                        }
                        Some(common_v1::run_stream_event::Body::Status(status))
                            if common_v1::stream_status::StatusKind::try_from(status.kind)
                                .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
                                == common_v1::stream_status::StatusKind::Failed =>
                        {
                            last_public_terminal_event = public_event;
                            stream_error = Some(
                                sanitize_http_error_message(status.message.as_str()).to_owned(),
                            );
                            break;
                        }
                        Some(common_v1::run_stream_event::Body::Status(status))
                            if common_v1::stream_status::StatusKind::try_from(status.kind)
                                .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
                                == common_v1::stream_status::StatusKind::Done =>
                        {
                            last_public_terminal_event = public_event;
                        }
                        _ => {}
                    }
                }
                Err(error) => {
                    stream_error = Some(sanitize_http_error_message(error.message()).to_owned());
                    break;
                }
            }
                }
            }
        }

        let now = unix_ms_now().unwrap_or(created_at_unix_ms);
        match stateful_run_snapshot(&state, run_id.as_str()).await {
            Ok(snapshot) => {
                if let Some(error) = stream_error.or_else(|| snapshot.last_error.clone()) {
                    touch_compat_api_token(
                        &state,
                        stream_context.token_id.as_str(),
                        "run",
                        "chat_failed",
                        Some(run_id.as_str()),
                        now,
                    );
                    let _ = send_compat_chat_failed_stream_event(
                        &sender,
                        &state,
                        &stream_context,
                        CompatChatFailedStreamEvent {
                            response_id: response_id.as_str(),
                            created_seconds,
                            model_name: model_name.as_str(),
                            code: "run_failed",
                            message: error,
                            public_event: last_public_terminal_event.as_ref(),
                        },
                    )
                    .await;
                    let _ = send_sse_done(&sender).await;
                    return;
                }
                touch_compat_api_token(
                    &state,
                    stream_context.token_id.as_str(),
                    "run",
                    "chat_completed",
                    Some(run_id.as_str()),
                    now,
                );
                let final_finish_reason =
                    if snapshot.cancel_requested { "cancelled" } else { finish_reason };
                let _ = send_compat_chat_sse_data(
                    &sender,
                    &state,
                    &stream_context,
                    json!({
                        "id": response_id,
                        "object": "chat.completion.chunk",
                        "created": created_seconds,
                        "model": model_name,
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": final_finish_reason,
                        }],
                        "_palyra": compat_stream_palyra_metadata(
                            run_id.as_str(),
                            session_id.as_str(),
                            last_public_terminal_event.as_ref()
                        ),
                    }),
                )
                .await;
                let _ = send_sse_done(&sender).await;
            }
            Err(response) => {
                touch_compat_api_token(
                    &state,
                    stream_context.token_id.as_str(),
                    "run",
                    "chat_failed",
                    Some(run_id.as_str()),
                    now,
                );
                let body = compat_error_body_from_response(&response);
                let _ = send_compat_chat_failed_stream_event(
                    &sender,
                    &state,
                    &stream_context,
                    CompatChatFailedStreamEvent {
                        response_id: response_id.as_str(),
                        created_seconds,
                        model_name: model_name.as_str(),
                        code: body
                            .pointer("/error/code")
                            .and_then(Value::as_str)
                            .unwrap_or("request_failed"),
                        message: body
                            .pointer("/error/message")
                            .and_then(Value::as_str)
                            .unwrap_or("compat API request failed")
                            .to_owned(),
                        public_event: None,
                    },
                )
                .await;
                let _ = send_sse_done(&sender).await;
            }
        }
    });

    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream; charset=utf-8"));
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

struct CompatChatFailedStreamEvent<'a> {
    response_id: &'a str,
    created_seconds: i64,
    model_name: &'a str,
    code: &'a str,
    message: String,
    public_event: Option<&'a Value>,
}

async fn send_compat_chat_sse_data(
    sender: &mpsc::Sender<Result<Bytes, Infallible>>,
    state: &AppState,
    context: &CompatChatStreamContext,
    payload: Value,
) -> bool {
    if send_sse_data(sender, payload).await {
        true
    } else {
        handle_compat_chat_stream_disconnect(state, context).await;
        false
    }
}

async fn send_compat_chat_sse_comment(
    sender: &mpsc::Sender<Result<Bytes, Infallible>>,
    state: &AppState,
    context: &CompatChatStreamContext,
    comment: &str,
) -> bool {
    if send_sse_comment(sender, comment).await {
        true
    } else {
        handle_compat_chat_stream_disconnect(state, context).await;
        false
    }
}

async fn send_compat_chat_failed_stream_event(
    sender: &mpsc::Sender<Result<Bytes, Infallible>>,
    state: &AppState,
    context: &CompatChatStreamContext,
    event: CompatChatFailedStreamEvent<'_>,
) -> bool {
    let error = json!({
        "type": "server_error",
        "code": event.code,
        "message": event.message,
    });
    if send_sse_event(
        sender,
        "chat.failed",
        json!({
            "id": event.response_id,
            "object": "chat.completion.chunk",
            "created": event.created_seconds,
            "model": event.model_name,
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "error",
            }],
            "error": error,
            "_palyra": compat_stream_palyra_metadata(
                context.run_id.as_str(),
                context.session_id.as_str(),
                event.public_event,
            ),
        }),
    )
    .await
    {
        true
    } else {
        handle_compat_chat_stream_disconnect(state, context).await;
        false
    }
}

async fn handle_compat_chat_stream_disconnect(state: &AppState, context: &CompatChatStreamContext) {
    let now = unix_ms_now().unwrap_or(context.created_at_unix_ms);
    if context.cancel_on_disconnect {
        match state
            .runtime
            .apply_turn_control(crate::application::turn_control::TurnControlRequest {
                operation: crate::application::turn_control::TurnControlOperation::CancelRun,
                actor_principal: context.principal.clone(),
                active_phase: None,
                session_id: Some(context.session_id.clone()),
                run_id: Some(context.run_id.clone()),
                queued_input_id: None,
                priority_lane: None,
                instruction: None,
                reason: Some("compat_chat_stream_client_disconnect".to_owned()),
                dry_run: false,
            })
            .await
        {
            Ok(outcome) => {
                let cancel_requested = outcome
                    .effect
                    .get("cancel_requested")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                tracing::info!(
                    run_id = %context.run_id,
                    cancel_requested,
                    "compat chat stream disconnected; requested run cancellation"
                );
            }
            Err(error) => {
                tracing::warn!(
                    run_id = %context.run_id,
                    error = %error,
                    "failed to cancel compat chat run after client disconnect"
                );
            }
        }
        touch_compat_api_token(
            state,
            context.token_id.as_str(),
            "run",
            "chat_disconnected_cancelled",
            Some(context.run_id.as_str()),
            now,
        );
        return;
    }

    match stateful_run_snapshot(state, context.run_id.as_str()).await {
        Ok(snapshot) => {
            if let Err(response) = record_compat_run_flow_audit(
                state,
                &snapshot,
                context.principal.as_str(),
                context.device_id.as_str(),
                "compat_chat_stream_client_disconnect",
                json!({
                    "action": "client_disconnect",
                    "disconnect_policy": "detach",
                    "cancel_on_disconnect": false,
                }),
                now,
            )
            .await
            {
                tracing::warn!(
                    run_id = %context.run_id,
                    status = %response.status(),
                    "failed to audit compat chat detach after client disconnect"
                );
            }
        }
        Err(response) => {
            tracing::warn!(
                run_id = %context.run_id,
                status = %response.status(),
                "failed to load compat chat run snapshot after client disconnect"
            );
        }
    }
    touch_compat_api_token(
        state,
        context.token_id.as_str(),
        "run",
        "chat_disconnected_detached",
        Some(context.run_id.as_str()),
        now,
    );
}

fn build_compat_responses_streaming_response(
    state: AppState,
    prepared: CompatPreparedRun,
) -> Response {
    let (sender, receiver) =
        mpsc::channel::<Result<Bytes, Infallible>>(COMPAT_SSE_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let CompatPreparedRun {
            token,
            provider_kind: _,
            model_name,
            run_id,
            session_id,
            principal,
            device_id,
            created_at_unix_ms,
            request_sender,
            run_request,
        } = prepared;
        let response_id = format!("resp_{run_id}");
        let created_seconds = created_at_unix_ms / 1_000;
        let message_item_id = format!("msg_{run_id}");
        let mut content = String::new();
        let mut tool_calls = Vec::new();
        let mut stream_tool_calls = HashMap::<String, CompatStreamToolCall>::new();
        let mut emitted_approval_resolutions = HashSet::<String>::new();
        let mut stream_error = None::<String>;
        let mut public_event_sequence = 0_u64;
        let mut last_public_terminal_event = None::<Value>;

        if !send_sse_event(
            &sender,
            "response.created",
            build_compat_responses_created_stream_payload(
                response_id.as_str(),
                run_id.as_str(),
                session_id.as_str(),
                created_seconds,
                model_name.as_str(),
            ),
        )
        .await
        {
            return;
        }
        if !send_sse_comment(&sender, "keepalive").await {
            return;
        }

        let endpoint = match build_compat_gateway_endpoint(&state) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                let _ = send_compat_responses_failed_stream_event(
                    &sender,
                    CompatResponsesFailedStreamEvent {
                        response_id: response_id.as_str(),
                        run_id: run_id.as_str(),
                        session_id: session_id.as_str(),
                        created_at_unix_ms,
                        code: "gateway_unavailable",
                        message: error,
                        public_event: None,
                    },
                )
                .await;
                let _ = send_sse_done(&sender).await;
                return;
            }
        };
        let channel = match endpoint.connect().await {
            Ok(channel) => channel,
            Err(error) => {
                let _ = send_compat_responses_failed_stream_event(
                    &sender,
                    CompatResponsesFailedStreamEvent {
                        response_id: response_id.as_str(),
                        run_id: run_id.as_str(),
                        session_id: session_id.as_str(),
                        created_at_unix_ms,
                        code: "gateway_unavailable",
                        message: format!("failed to connect compat API to gateway: {error}"),
                        public_event: None,
                    },
                )
                .await;
                let _ = send_sse_done(&sender).await;
                return;
            }
        };
        let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::new(channel);
        let mut stream = match client.run_stream(run_request).await {
            Ok(response) => response.into_inner(),
            Err(error) => {
                let _ = send_compat_responses_failed_stream_event(
                    &sender,
                    CompatResponsesFailedStreamEvent {
                        response_id: response_id.as_str(),
                        run_id: run_id.as_str(),
                        session_id: session_id.as_str(),
                        created_at_unix_ms,
                        code: "gateway_stream_failed",
                        message: sanitize_http_error_message(error.message()).to_owned(),
                        public_event: None,
                    },
                )
                .await;
                let _ = send_sse_done(&sender).await;
                return;
            }
        };

        let mut keepalive = tokio::time::interval(Duration::from_secs(15));
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = keepalive.tick() => {
                    if !send_sse_comment(&sender, "keepalive").await {
                        return;
                    }
                }
                item = stream.next() => {
                    let Some(item) = item else {
                        break;
                    };
                    match item {
                        Ok(event) => {
                            public_event_sequence = public_event_sequence.saturating_add(1);
                            let public_event_id =
                                crate::application::run_stream::public_events::run_stream_public_event_id(
                                    run_id.as_str(),
                                    public_event_sequence,
                                );
                            let public_event = crate::application::run_stream::public_events::
                                public_runtime_event_json_from_run_stream_event(
                                    &event,
                                    crate::application::run_stream::public_events::PublicRunStreamEventContext {
                                        event_id: public_event_id.as_str(),
                                        session_id: session_id.as_str(),
                                        occurred_at_unix_ms: unix_ms_now().unwrap_or(created_at_unix_ms),
                                        request_id: None,
                                    },
                                );
                            match event.body {
                                Some(common_v1::run_stream_event::Body::ModelToken(token_event)) => {
                                    content.push_str(token_event.token.as_str());
                                    if !send_sse_event(
                                        &sender,
                                        "response.output_text.delta",
                                        json!({
                                            "type": "response.output_text.delta",
                                            "response_id": response_id,
                                            "item_id": message_item_id,
                                            "output_index": 0,
                                            "content_index": 0,
                                            "delta": token_event.token,
                                            "_palyra": compat_stream_palyra_metadata(
                                                run_id.as_str(),
                                                session_id.as_str(),
                                                public_event.as_ref()
                                            ),
                                        }),
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                }
                                Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => {
                                    let tool_call = CompatStreamToolCall {
                                        id: proposal
                                            .proposal_id
                                            .as_ref()
                                            .map(|value| value.ulid.clone())
                                            .unwrap_or_else(|| Ulid::new().to_string()),
                                        name: proposal.tool_name,
                                        arguments: json_string_from_bytes(proposal.input_json.as_slice()),
                                        output_index: tool_calls.len().saturating_add(1) as u64,
                                    };
                                    tool_calls.push(CompatToolCall {
                                        id: tool_call.id.clone(),
                                        name: tool_call.name.clone(),
                                        arguments: tool_call.arguments.clone(),
                                    });
                                    stream_tool_calls
                                        .insert(tool_call.id.clone(), tool_call.clone());
                                    if !send_sse_event(
                                        &sender,
                                        "response.output_item.added",
                                        build_compat_responses_tool_call_added_stream_payload(
                                            response_id.as_str(),
                                            run_id.as_str(),
                                            session_id.as_str(),
                                            &tool_call,
                                            public_event.as_ref(),
                                        ),
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                    if !send_sse_event(
                                        &sender,
                                        "response.function_call_arguments.delta",
                                        build_compat_responses_tool_call_arguments_delta_stream_payload(
                                            response_id.as_str(),
                                            run_id.as_str(),
                                            session_id.as_str(),
                                            &tool_call,
                                            public_event.as_ref(),
                                        ),
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                    if !send_sse_event(
                                        &sender,
                                        "response.function_call_arguments.done",
                                        build_compat_responses_tool_call_arguments_done_stream_payload(
                                            response_id.as_str(),
                                            run_id.as_str(),
                                            session_id.as_str(),
                                            &tool_call,
                                            public_event.as_ref(),
                                        ),
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                }
                                Some(common_v1::run_stream_event::Body::ToolApprovalRequest(request)) => {
                                    if !send_sse_event(
                                        &sender,
                                        "approval.required",
                                        build_compat_responses_approval_required_stream_payload(
                                            response_id.as_str(),
                                            run_id.as_str(),
                                            session_id.as_str(),
                                            &request,
                                            public_event.as_ref(),
                                        ),
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                    let response = auto_deny_compat_tool_approval(
                                        &request_sender,
                                        session_id.as_str(),
                                        run_id.as_str(),
                                        &request,
                                    )
                                    .await;
                                    let approval_id =
                                        compat_approval_response_id(response.approval_id.as_ref(), response.proposal_id.as_ref());
                                    emitted_approval_resolutions.insert(approval_id);
                                    if !send_sse_event(
                                        &sender,
                                        "approval.resolved",
                                        build_compat_responses_approval_resolved_stream_payload(
                                            response_id.as_str(),
                                            run_id.as_str(),
                                            session_id.as_str(),
                                            &response,
                                            public_event.as_ref(),
                                        ),
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                }
                                Some(common_v1::run_stream_event::Body::ToolApprovalResponse(response)) => {
                                    let approval_id =
                                        compat_approval_response_id(response.approval_id.as_ref(), response.proposal_id.as_ref());
                                    if emitted_approval_resolutions.insert(approval_id)
                                        && !send_sse_event(
                                            &sender,
                                            "approval.resolved",
                                            build_compat_responses_approval_resolved_stream_payload(
                                                response_id.as_str(),
                                                run_id.as_str(),
                                                session_id.as_str(),
                                                &response,
                                                public_event.as_ref(),
                                            ),
                                        )
                                        .await
                                    {
                                        return;
                                    }
                                }
                                Some(common_v1::run_stream_event::Body::ToolResult(result)) => {
                                    let tool_call_id = compat_tool_call_id(result.proposal_id.as_ref())
                                        .unwrap_or_else(|| "tool_unknown".to_owned());
                                    let tool_call = stream_tool_calls.get(tool_call_id.as_str());
                                    if !send_sse_event(
                                        &sender,
                                        "response.output_item.done",
                                        build_compat_responses_tool_result_stream_payload(
                                            response_id.as_str(),
                                            run_id.as_str(),
                                            session_id.as_str(),
                                            tool_call,
                                            &result,
                                            public_event.as_ref(),
                                        ),
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                }
                                Some(common_v1::run_stream_event::Body::Status(status))
                                    if common_v1::stream_status::StatusKind::try_from(status.kind)
                                        .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
                                        == common_v1::stream_status::StatusKind::Failed =>
                                {
                                    last_public_terminal_event = public_event;
                                    stream_error = Some(
                                        sanitize_http_error_message(status.message.as_str()).to_owned(),
                                    );
                                    break;
                                }
                                Some(common_v1::run_stream_event::Body::Status(status))
                                    if common_v1::stream_status::StatusKind::try_from(status.kind)
                                        .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
                                        == common_v1::stream_status::StatusKind::Done =>
                                {
                                    last_public_terminal_event = public_event;
                                }
                                _ => {}
                            }
                        }
                        Err(error) => {
                            stream_error = Some(sanitize_http_error_message(error.message()).to_owned());
                            break;
                        }
                    }
                }
            }
        }

        let now = unix_ms_now().unwrap_or(created_at_unix_ms);
        match stateful_run_snapshot(&state, run_id.as_str()).await {
            Ok(snapshot) => {
                if let Some(error) = stream_error.or_else(|| snapshot.last_error.clone()) {
                    touch_compat_api_token(
                        &state,
                        token.token_id.as_str(),
                        "run",
                        "responses_failed",
                        Some(run_id.as_str()),
                        now,
                    );
                    let _ = send_compat_responses_failed_stream_event(
                        &sender,
                        CompatResponsesFailedStreamEvent {
                            response_id: response_id.as_str(),
                            run_id: run_id.as_str(),
                            session_id: session_id.as_str(),
                            created_at_unix_ms: snapshot.created_at_unix_ms,
                            code: "run_failed",
                            message: error,
                            public_event: last_public_terminal_event.as_ref(),
                        },
                    )
                    .await;
                    let _ = send_sse_done(&sender).await;
                    return;
                }
                touch_compat_api_token(
                    &state,
                    token.token_id.as_str(),
                    "run",
                    "responses_completed",
                    Some(run_id.as_str()),
                    now,
                );
                let response = build_compat_responses_payload_from_parts(
                    response_id,
                    &snapshot,
                    "completed",
                    content,
                    tool_calls.as_slice(),
                    None,
                );
                if let Err(response_store_error) = persist_compat_response_payload(
                    &state,
                    CompatResponsePersistRequest {
                        response_id: response
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        session_id: snapshot.session_id.clone(),
                        run_id: snapshot.run_id.clone(),
                        owner_principal: principal,
                        device_id,
                        status: "completed".to_owned(),
                        created_at_unix_ms: snapshot.created_at_unix_ms,
                        completed_at_unix_ms: snapshot.completed_at_unix_ms,
                        payload: response.clone(),
                    },
                )
                .await
                {
                    let body = compat_error_body_from_response(&response_store_error);
                    let _ = send_sse_event(
                        &sender,
                        "response.failed",
                        json!({
                            "type": "response.failed",
                            "response": {
                                "id": response.get("id").and_then(Value::as_str).unwrap_or_default(),
                                "object": "response",
                                "created": snapshot.created_at_unix_ms / 1_000,
                                "status": "failed",
                                "output": [],
                                "usage": Value::Null,
                                "error": body["error"].clone(),
                                "_palyra": compat_stream_palyra_metadata(
                                    run_id.as_str(),
                                    session_id.as_str(),
                                    last_public_terminal_event.as_ref()
                                ),
                            },
                            "error": body["error"].clone(),
                        }),
                    )
                    .await;
                    let _ = send_sse_done(&sender).await;
                    return;
                }
                let _ = send_sse_event(
                    &sender,
                    "response.completed",
                    json!({
                        "type": "response.completed",
                        "response": response,
                        "_palyra": compat_stream_palyra_metadata(
                            run_id.as_str(),
                            session_id.as_str(),
                            last_public_terminal_event.as_ref()
                        ),
                    }),
                )
                .await;
                let _ = send_sse_done(&sender).await;
            }
            Err(response) => {
                touch_compat_api_token(
                    &state,
                    token.token_id.as_str(),
                    "run",
                    "responses_failed",
                    Some(run_id.as_str()),
                    now,
                );
                let body = compat_error_body_from_response(&response);
                let _ = send_sse_event(
                    &sender,
                    "response.failed",
                    json!({
                        "type": "response.failed",
                        "response": {
                            "id": response_id,
                            "object": "response",
                            "created": created_seconds,
                            "status": "failed",
                            "output": [],
                            "usage": Value::Null,
                            "error": body["error"].clone(),
                            "_palyra": compat_stream_palyra_metadata(run_id.as_str(), session_id.as_str(), None),
                        },
                        "error": body["error"].clone(),
                    }),
                )
                .await;
                let _ = send_sse_done(&sender).await;
            }
        }
    });

    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream; charset=utf-8"));
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

struct CompatResponsesFailedStreamEvent<'a> {
    response_id: &'a str,
    run_id: &'a str,
    session_id: &'a str,
    created_at_unix_ms: i64,
    code: &'a str,
    message: String,
    public_event: Option<&'a Value>,
}

async fn send_compat_responses_failed_stream_event(
    sender: &mpsc::Sender<Result<Bytes, Infallible>>,
    event: CompatResponsesFailedStreamEvent<'_>,
) -> bool {
    let error = json!({
        "type": "server_error",
        "code": event.code,
        "message": event.message,
    });
    send_sse_event(
        sender,
        "response.failed",
        json!({
            "type": "response.failed",
            "response": {
                "id": event.response_id,
                "object": "response",
                "created": event.created_at_unix_ms / 1_000,
                "status": "failed",
                "output": [],
                "usage": Value::Null,
                "error": error,
                "_palyra": compat_stream_palyra_metadata(
                    event.run_id,
                    event.session_id,
                    event.public_event
                ),
            },
            "error": error,
        }),
    )
    .await
}

fn build_compat_models(provider: &model_provider::ProviderStatusSnapshot) -> Vec<Value> {
    build_compat_model_descriptors(provider)
        .into_iter()
        .map(|descriptor| compat_model_json(&descriptor))
        .collect()
}

fn build_compat_capabilities_payload(
    provider: &model_provider::ProviderStatusSnapshot,
    embeddings_status: &journal::MemoryEmbeddingsStatus,
    feature_flags: &[FeatureFlagRecord],
    generated_at_unix_ms: i64,
) -> Value {
    let models = build_compat_model_descriptors(provider);
    let default_chat = models.iter().find(|model| model.role == "chat");
    let default_chat_capabilities = default_chat.and_then(|model| model.capabilities.as_ref());
    let compat_tools_invoke_enabled =
        compat_feature_flag_enabled(feature_flags, FEATURE_COMPAT_TOOLS_INVOKE);
    json!({
        "object": "capabilities",
        "schema_version": 1,
        "generated_at": generated_at_unix_ms / 1_000,
        "generated_at_unix_ms": generated_at_unix_ms,
        "provider": {
            "kind": provider.kind,
            "health_status": provider.health.state,
            "discovery_status": provider.discovery.status,
            "default_model": default_chat.map(|model| model.id.clone()),
            "models": models.iter().map(compat_model_capability_summary_json).collect::<Vec<_>>(),
        },
        "runtime": {
            "maturity": "preview",
            "feature_flags": {
                "compat_api": compat_feature_flag_json(feature_flags, FEATURE_COMPAT_API),
                "compat_embeddings_api": compat_feature_flag_json(feature_flags, FEATURE_COMPAT_EMBEDDINGS_API),
                "compat_tools_invoke": compat_feature_flag_json(feature_flags, FEATURE_COMPAT_TOOLS_INVOKE),
            },
            "capabilities": build_compat_runtime_capabilities(
                default_chat_capabilities,
                embeddings_status,
                compat_tools_invoke_enabled,
            ),
        },
        "method_registry": compat_method_registry_json(),
    })
}

fn compat_model_capability_summary_json(model: &CompatModelDescriptor) -> Value {
    json!({
        "id": model.id,
        "role": model.role,
        "default": model.default_model,
        "enabled": model.enabled,
        "health_status": model.health_status,
        "discovery_status": model.discovery_status,
        "capabilities": compat_model_capabilities_json(model.capabilities.as_ref()),
    })
}

fn compat_model_capabilities_json(
    capabilities: Option<&model_provider::ProviderCapabilitiesSnapshot>,
) -> Value {
    json!({
        "streaming_tokens": capabilities.is_some_and(|value| value.streaming_tokens),
        "tool_calls": capabilities.is_some_and(|value| value.tool_calls),
        "structured_outputs": {
            "supported": capabilities.is_some_and(|value| value.json_mode),
            "source": "json_mode",
            "schema_dialect": compat_schema_dialect(capabilities),
            "strict_schema": false,
        },
        "json_mode": capabilities.is_some_and(|value| value.json_mode),
        "reasoning": {
            "supported": capabilities.is_some_and(|value| value.reasoning),
            "posture": compat_reasoning_posture(capabilities),
            "efforts": capabilities.map(|value| value.reasoning_efforts.clone()).unwrap_or_default(),
        },
        "vision": capabilities.is_some_and(|value| value.vision),
        "audio_transcribe": capabilities.is_some_and(|value| value.audio_transcribe),
        "embeddings": capabilities.is_some_and(|value| value.embeddings),
        "service_tier": capabilities.is_some_and(|value| value.service_tier),
        "service_tiers": capabilities.map(|value| value.service_tiers.clone()).unwrap_or_default(),
        "max_context_tokens": capabilities.and_then(|value| value.max_context_tokens),
        "cost_tier": capabilities.map(|value| value.cost_tier.clone()),
        "latency_tier": capabilities.map(|value| value.latency_tier.clone()),
        "recommended_use_cases": capabilities.map(|value| value.recommended_use_cases.clone()).unwrap_or_default(),
        "known_limitations": capabilities.map(|value| value.known_limitations.clone()).unwrap_or_default(),
        "operator_override": capabilities.is_some_and(|value| value.operator_override),
        "metadata_source": capabilities.map(|value| value.metadata_source.clone()),
    })
}

fn build_compat_runtime_capabilities(
    default_chat_capabilities: Option<&model_provider::ProviderCapabilitiesSnapshot>,
    embeddings_status: &journal::MemoryEmbeddingsStatus,
    compat_tools_invoke_enabled: bool,
) -> Vec<Value> {
    let streaming_supported = default_chat_capabilities.is_some_and(|value| value.streaming_tokens);
    let tool_calls_supported = default_chat_capabilities.is_some_and(|value| value.tool_calls);
    let structured_outputs_supported =
        default_chat_capabilities.is_some_and(|value| value.json_mode);
    vec![
        compat_runtime_capability_json(
            "models",
            true,
            "stable",
            None,
            None,
            &["GET /v1/models", "GET /v1/models/{model_id}"],
            json!({ "scope": PERMISSION_COMPAT_MODELS_READ }),
        ),
        compat_runtime_capability_json(
            "chat_completions",
            true,
            "stable",
            None,
            None,
            &["POST /v1/chat/completions"],
            json!({ "scope": PERMISSION_COMPAT_CHAT_CREATE }),
        ),
        compat_runtime_capability_json(
            "streaming_tokens",
            streaming_supported,
            "stable",
            (!streaming_supported).then_some("model_streaming_tokens_disabled"),
            (!streaming_supported).then_some("Select a chat model with streaming_tokens support."),
            &["POST /v1/chat/completions", "POST /v1/responses", "GET /v1/runs/{run_id}/events"],
            json!({ "source": "provider_capabilities.streaming_tokens" }),
        ),
        compat_runtime_capability_json(
            "tool_calls",
            tool_calls_supported,
            "preview",
            (!tool_calls_supported).then_some("model_tool_calls_disabled"),
            (!tool_calls_supported).then_some("Select a chat model with tool_calls support."),
            &["POST /v1/responses"],
            json!({ "source": "provider_capabilities.tool_calls" }),
        ),
        compat_runtime_capability_json(
            "structured_outputs",
            structured_outputs_supported,
            "preview",
            (!structured_outputs_supported).then_some("model_json_mode_disabled"),
            (!structured_outputs_supported).then_some("Select a chat model with json_mode support."),
            &["POST /v1/chat/completions", "POST /v1/responses"],
            json!({
                "source": "provider_capabilities.json_mode",
                "schema_dialect": compat_schema_dialect(default_chat_capabilities),
                "strict_schema": false,
            }),
        ),
        compat_runtime_capability_json(
            "embeddings",
            embeddings_status.production_default_active,
            "preview",
            (!embeddings_status.production_default_active)
                .then_some(embeddings_status.degraded_reason_code.as_deref())
                .flatten(),
            (!embeddings_status.production_default_active)
                .then_some(embeddings_status.remediation.as_deref())
                .flatten(),
            &["POST /v1/embeddings"],
            json!({
                "quality": embeddings_status.quality,
                "mode": embeddings_status.mode,
            }),
        ),
        compat_runtime_capability_json(
            "responses",
            true,
            "preview",
            None,
            None,
            &["POST /v1/responses", "GET /v1/responses/{response_id}", "DELETE /v1/responses/{response_id}"],
            json!({ "response_store": true, "idempotency": true, "sse": true }),
        ),
        compat_runtime_capability_json(
            "sessions",
            true,
            "preview",
            None,
            None,
            &["POST /v1/runs"],
            json!({ "selectors": ["session.id", "session.key", "session.label"] }),
        ),
        compat_runtime_capability_json(
            "runs",
            true,
            "preview",
            None,
            None,
            &[
                "POST /v1/runs",
                "GET /v1/runs/{run_id}",
                "GET /v1/runs/{run_id}/events",
                "POST /v1/runs/{run_id}/wait",
                "POST /v1/runs/{run_id}/stop",
                "POST /v1/runs/{run_id}/detach",
            ],
            json!({
                "durable_status": true,
                "event_replay": true,
                "wait": true,
                "stop": true,
                "detach": true,
            }),
        ),
        compat_runtime_capability_json(
            "approvals",
            true,
            "preview",
            None,
            None,
            &["POST /v1/runs/{run_id}/approval"],
            json!({ "decisions": ["approve", "deny", "timeout"], "modify": false }),
        ),
        compat_runtime_capability_json(
            "direct_tool_invoke",
            false,
            "lab",
            Some(if compat_tools_invoke_enabled {
                "approval_bound_execution_not_ready"
            } else {
                "feature_flag_disabled"
            }),
            Some(if compat_tools_invoke_enabled {
                "Use Responses or Runs tool-call flows until direct tool invocation is approval-bound."
            } else {
                "Enable compat_tools_invoke only for explicit operator testing."
            }),
            &["POST /v1/tools/invoke"],
            json!({ "feature_flag_enabled": compat_tools_invoke_enabled }),
        ),
        compat_runtime_capability_json(
            "mcp",
            false,
            "planned",
            Some("not_exposed_on_compat_api"),
            Some("Use native Palyra tool/runtime surfaces until MCP is published on the compat facade."),
            &[],
            Value::Null,
        ),
        compat_runtime_capability_json(
            "subagents",
            false,
            "planned",
            Some("not_exposed_on_compat_api"),
            Some("Use native Palyra delegation surfaces until subagents are published on the compat facade."),
            &[],
            Value::Null,
        ),
    ]
}

fn compat_runtime_capability_json(
    id: &str,
    supported: bool,
    maturity: &str,
    disabled_reason_code: Option<&str>,
    repair_hint: Option<&str>,
    routes: &[&str],
    details: Value,
) -> Value {
    json!({
        "id": id,
        "supported": supported,
        "maturity": maturity,
        "disabled_reason_code": disabled_reason_code,
        "repair_hint": repair_hint,
        "routes": routes,
        "details": details,
    })
}

fn compat_method_registry_json() -> Value {
    let registry = crate::method_registry::build_method_registry_snapshot();
    let methods = registry
        .methods
        .iter()
        .filter(|method| method.surface == "compat")
        .map(|method| {
            json!({
                "method_name": method.method_name,
                "route": method.route,
                "http_method": method.http_method,
                "stability": method.stability,
                "required_scope": method.required_scope,
                "request_schema_id": method.request_schema_id,
                "response_schema_id": method.response_schema_id,
                "streaming_supported": method.streaming_supported,
                "idempotency_supported": method.idempotency_supported,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "schema_version": registry.schema_version,
        "registry_version": registry.registry_version,
        "methods": methods,
    })
}

fn compat_feature_flag_json(feature_flags: &[FeatureFlagRecord], key: &str) -> Value {
    feature_flags
        .iter()
        .find(|flag| flag.key == key)
        .map(|flag| {
            json!({
                "enabled": flag.enabled,
                "stage": flag.stage,
                "depends_on": flag.depends_on,
            })
        })
        .unwrap_or_else(|| {
            json!({
                "enabled": false,
                "stage": "missing",
                "depends_on": [],
            })
        })
}

fn compat_feature_flag_enabled(feature_flags: &[FeatureFlagRecord], key: &str) -> bool {
    feature_flags.iter().any(|flag| flag.key == key && flag.enabled)
}

fn compat_schema_dialect(
    capabilities: Option<&model_provider::ProviderCapabilitiesSnapshot>,
) -> Option<&'static str> {
    capabilities.is_some_and(|value| value.json_mode).then_some("json_schema_draft_2020_12_subset")
}

fn compat_reasoning_posture(
    capabilities: Option<&model_provider::ProviderCapabilitiesSnapshot>,
) -> &'static str {
    match capabilities {
        Some(value) if value.reasoning => "native_reasoning",
        Some(_) => "not_supported",
        None => "unknown",
    }
}

fn build_compat_model_descriptors(
    provider: &model_provider::ProviderStatusSnapshot,
) -> Vec<CompatModelDescriptor> {
    let mut descriptors = Vec::new();
    let chat_model_id = current_compat_chat_model_id(provider);
    descriptors.push(build_chat_model_descriptor(provider, chat_model_id.as_str()));
    if let Some(model_id) = current_compat_embeddings_model_id(provider) {
        descriptors.push(build_embeddings_model_descriptor(provider, model_id.as_str()));
    }
    descriptors
}

fn build_chat_model_descriptor(
    provider: &model_provider::ProviderStatusSnapshot,
    model_id: &str,
) -> CompatModelDescriptor {
    let registry_model = provider
        .registry
        .models
        .iter()
        .find(|entry| entry.model_id == model_id && entry.role == "chat");
    CompatModelDescriptor {
        id: model_id.to_owned(),
        role: "chat",
        provider_kind: provider.kind.clone(),
        health_status: provider.health.state.clone(),
        discovery_status: provider.discovery.status.clone(),
        default_model: true,
        enabled: registry_model.map(|entry| entry.enabled).unwrap_or(true),
        dimensions: None,
        capabilities: Some(
            registry_model
                .map(|entry| entry.capabilities.clone())
                .unwrap_or_else(|| provider.capabilities.clone()),
        ),
    }
}

fn build_embeddings_model_descriptor(
    provider: &model_provider::ProviderStatusSnapshot,
    model_id: &str,
) -> CompatModelDescriptor {
    let registry_model = provider
        .registry
        .models
        .iter()
        .find(|entry| entry.model_id == model_id && entry.role == "embeddings");
    CompatModelDescriptor {
        id: model_id.to_owned(),
        role: "embeddings",
        provider_kind: provider.kind.clone(),
        health_status: provider.health.state.clone(),
        discovery_status: provider.discovery.status.clone(),
        default_model: provider
            .registry
            .default_embeddings_model_id
            .as_deref()
            .is_some_and(|candidate| candidate == model_id),
        enabled: registry_model.map(|entry| entry.enabled).unwrap_or(true),
        dimensions: provider.openai_embeddings_dims,
        capabilities: registry_model.map(|entry| entry.capabilities.clone()),
    }
}

fn compat_model_json(model: &CompatModelDescriptor) -> Value {
    json!({
        "id": model.id,
        "object": "model",
        "created": 0,
        "owned_by": "palyra",
        "root": model.id,
        "parent": Value::Null,
        "metadata": {
            "provider_kind": model.provider_kind,
            "role": model.role,
            "default": model.default_model,
            "enabled": model.enabled,
            "health_status": model.health_status,
            "discovery_status": model.discovery_status,
            "dimensions": model.dimensions,
            "supports_streaming_tokens": model.capabilities.as_ref().map(|value| value.streaming_tokens),
            "supports_tool_calls": model.capabilities.as_ref().map(|value| value.tool_calls),
            "supports_structured_outputs": model.capabilities.as_ref().is_some_and(|value| value.json_mode),
            "supports_json_mode": model.capabilities.as_ref().map(|value| value.json_mode),
            "schema_dialect": compat_schema_dialect(model.capabilities.as_ref()),
            "reasoning_posture": compat_reasoning_posture(model.capabilities.as_ref()),
            "reasoning_efforts": model.capabilities.as_ref().map(|value| value.reasoning_efforts.clone()).unwrap_or_default(),
            "supports_vision": model.capabilities.as_ref().map(|value| value.vision),
            "supports_audio_transcribe": model.capabilities.as_ref().map(|value| value.audio_transcribe),
            "supports_embeddings": model.capabilities.as_ref().map(|value| value.embeddings),
            "max_context_tokens": model.capabilities.as_ref().and_then(|value| value.max_context_tokens),
            "cost_tier": model.capabilities.as_ref().map(|value| value.cost_tier.clone()),
            "latency_tier": model.capabilities.as_ref().map(|value| value.latency_tier.clone()),
            "recommended_use_cases": model.capabilities.as_ref().map(|value| value.recommended_use_cases.clone()).unwrap_or_default(),
            "known_limitations": model.capabilities.as_ref().map(|value| value.known_limitations.clone()).unwrap_or_default(),
            "metadata_source": model.capabilities.as_ref().map(|value| value.metadata_source.clone()),
            "capabilities": compat_model_capabilities_json(model.capabilities.as_ref()),
        }
    })
}

fn current_compat_chat_model_id(provider: &model_provider::ProviderStatusSnapshot) -> String {
    provider.openai_model.clone().unwrap_or_else(|| format!("palyra-{}", provider.kind))
}

fn current_compat_embeddings_model_id(
    provider: &model_provider::ProviderStatusSnapshot,
) -> Option<String> {
    provider.openai_embeddings_model.clone()
}

fn compat_model_not_found_response(model_id: &str) -> Response {
    compat_error_response(
        StatusCode::NOT_FOUND,
        "invalid_request_error",
        "model_not_found",
        format!("requested model '{model_id}' is not published by the current compat provider"),
    )
}

fn build_compat_embeddings_payload(
    prompt_tokens: u64,
    response: &crate::model_provider::EmbeddingsResponse,
) -> Value {
    json!({
        "object": "list",
        "data": response
            .vectors
            .iter()
            .enumerate()
            .map(|(index, embedding)| {
                json!({
                    "object": "embedding",
                    "index": index,
                    "embedding": embedding,
                })
            })
            .collect::<Vec<_>>(),
        "model": response.model_name,
        "usage": {
            "prompt_tokens": prompt_tokens,
            "total_tokens": prompt_tokens,
        }
    })
}

#[allow(clippy::result_large_err)]
fn normalize_compat_embeddings_input(
    input: CompatEmbeddingsInput,
) -> Result<Vec<String>, Response> {
    let values = match input {
        CompatEmbeddingsInput::Text(text) => vec![text],
        CompatEmbeddingsInput::Texts(texts) => texts,
    };
    let normalized = values.into_iter().filter_map(trim_to_option).collect::<Vec<_>>();
    if normalized.is_empty() {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "empty_input",
            "input cannot be empty",
        ));
    }
    Ok(normalized)
}

fn compat_embeddings_provider_error_response(
    error: crate::model_provider::ProviderError,
) -> Response {
    match error {
        crate::model_provider::ProviderError::MissingEmbeddingsModel => compat_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            "embeddings_unavailable",
            "compat embeddings model is not configured",
        ),
        crate::model_provider::ProviderError::InvalidEmbeddingsRequest { message } => {
            compat_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "invalid_embeddings_request",
                message,
            )
        }
        crate::model_provider::ProviderError::CircuitOpen { retry_after_ms } => {
            compat_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "server_error",
                "provider_circuit_open",
                format!(
                    "embeddings provider circuit breaker is open; retry after {retry_after_ms}ms"
                ),
            )
        }
        crate::model_provider::ProviderError::RequestFailed { message, .. } => {
            compat_error_response(
                StatusCode::BAD_GATEWAY,
                "server_error",
                "provider_request_failed",
                message,
            )
        }
        crate::model_provider::ProviderError::InvalidResponse { message, .. } => {
            compat_error_response(
                StatusCode::BAD_GATEWAY,
                "server_error",
                "provider_invalid_response",
                message,
            )
        }
        other => compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "provider_error",
            other.to_string(),
        ),
    }
}

fn validate_compat_run_tool_request(payload: &CompatRunsCreateRequest) -> CompatHttpResult<()> {
    if payload.tools.is_some() {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "custom_tools_unsupported",
            "/v1/runs uses the configured Palyra tool catalog; per-request tool schemas are not supported",
        )
        .into());
    }
    match payload
        .tool_exposure_policy
        .as_deref()
        .and_then(|value| trim_to_option(value.to_owned()))
        .as_deref()
    {
        None | Some("default" | "configured") => Ok(()),
        Some("none") => Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "tool_exposure_policy_unsupported",
            "tool_exposure_policy='none' is not supported because the runtime tool catalog is policy-controlled",
        )
        .into()),
        Some(_) => Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_tool_exposure_policy",
            "tool_exposure_policy must be omitted, 'default', or 'configured'",
        )
        .into()),
    }
}

fn render_compat_runs_prompt(payload: &CompatRunsCreateRequest) -> CompatHttpResult<String> {
    let mut sections = Vec::new();
    if let Some(instructions) =
        payload.instructions.as_ref().and_then(|value| trim_to_option(value.clone()))
    {
        sections.push(format!("SYSTEM:\n{instructions}"));
    }
    if let Some(messages) = payload.messages.as_ref() {
        sections.push(render_compat_messages_prompt(messages.as_slice())?);
    }
    if let Some(input) = payload.input.as_ref() {
        sections.push(render_compat_responses_input_prompt(input)?);
    }
    if sections.is_empty() {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "empty_run_input",
            "/v1/runs requires instructions, messages, or input",
        )
        .into());
    }
    Ok(sections.join("\n\n"))
}

fn render_compat_responses_input_prompt(input: &CompatResponsesInput) -> CompatHttpResult<String> {
    match input {
        CompatResponsesInput::Text(text) => trim_to_option(text.clone()).ok_or_else(|| {
            CompatHttpError::from(compat_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "empty_input",
                "input cannot be empty",
            ))
        }),
        CompatResponsesInput::Messages(messages) => {
            let rendered = messages
                .iter()
                .cloned()
                .map(|item| CompatChatMessage {
                    role: item.role.unwrap_or_else(|| "user".to_owned()),
                    content: item.content,
                    name: item.name,
                })
                .collect::<Vec<_>>();
            Ok(render_compat_messages_prompt(rendered.as_slice())?)
        }
    }
}

fn compat_runs_effective_metadata(
    payload: &CompatRunsCreateRequest,
) -> CompatHttpResult<Option<Value>> {
    let mut object = match payload.metadata.as_ref() {
        Some(Value::Object(object)) => object.clone(),
        Some(_) => {
            return Err(compat_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "invalid_metadata",
                "metadata must be a JSON object when provided",
            )
            .into());
        }
        None => serde_json::Map::new(),
    };

    if let Some(session) = payload.session.as_ref() {
        if let Some(session_id) =
            session.id.as_ref().and_then(|value| trim_to_option(value.clone()))
        {
            validate_canonical_id(session_id.as_str()).map_err(|_| {
                CompatHttpError::from(compat_error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    "invalid_session_id",
                    "session.id must be a canonical ULID",
                ))
            })?;
            object.insert("palyra_session_id".to_owned(), Value::String(session_id));
        }
        if let Some(session_key) =
            session.key.as_ref().and_then(|value| trim_to_option(value.clone()))
        {
            object.insert("palyra_session_key".to_owned(), Value::String(session_key));
        }
        if let Some(session_label) =
            session.label.as_ref().and_then(|value| trim_to_option(value.clone()))
        {
            object.insert("palyra_session_label".to_owned(), Value::String(session_label));
        }
        if let Some(require_existing) = session.require_existing {
            object.insert("palyra_require_existing".to_owned(), Value::Bool(require_existing));
        }
        if let Some(reset_session) = session.reset {
            object.insert("palyra_reset_session".to_owned(), Value::Bool(reset_session));
        }
    }

    if let Some(policy) =
        payload.tool_exposure_policy.as_ref().and_then(|value| trim_to_option(value.clone()))
    {
        object.insert("palyra_tool_exposure_policy".to_owned(), Value::String(policy));
    }

    if object.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Value::Object(object)))
    }
}

#[allow(clippy::result_large_err)]
fn validate_compat_requested_model(
    provider: &model_provider::ProviderStatusSnapshot,
    requested_model: Option<&str>,
) -> Result<String, Response> {
    let available =
        provider.openai_model.clone().unwrap_or_else(|| format!("palyra-{}", provider.kind));
    let Some(requested_model) = requested_model.and_then(|value| trim_to_option(value.to_owned()))
    else {
        return Ok(available);
    };
    if requested_model == available {
        Ok(available)
    } else {
        Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "model_not_available",
            format!(
                "requested model '{requested_model}' is not available through the current compat provider"
            ),
        ))
    }
}

#[allow(clippy::result_large_err)]
fn render_compat_messages_prompt(messages: &[CompatChatMessage]) -> Result<String, Response> {
    if messages.is_empty() {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "empty_messages",
            "messages cannot be empty",
        ));
    }
    let mut rendered = Vec::new();
    for message in messages {
        let content = render_compat_message_content(&message.content);
        if content.is_empty() {
            continue;
        }
        let role = message.role.trim().to_ascii_uppercase();
        let name = message
            .name
            .as_deref()
            .and_then(|value| trim_to_option(value.to_owned()))
            .map(|value| format!(" ({value})"))
            .unwrap_or_default();
        rendered.push(format!("{role}{name}:\n{content}"));
    }
    if rendered.is_empty() {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "empty_messages",
            "messages must include at least one text-bearing item",
        ));
    }
    Ok(rendered.join("\n\n"))
}

fn render_compat_message_content(content: &CompatMessageContent) -> String {
    match content {
        CompatMessageContent::Text(text) => trim_to_option(text.clone()).unwrap_or_default(),
        CompatMessageContent::Parts(parts) => parts
            .iter()
            .filter_map(|part| match part.kind.as_str() {
                "text" | "input_text" | "output_text" => {
                    part.text.clone().or_else(|| part.input_text.clone()).and_then(trim_to_option)
                }
                "image_url" | "input_image" => Some("[image content omitted]".to_owned()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        CompatMessageContent::Json(value) => match value {
            Value::String(text) => trim_to_option(text.clone()).unwrap_or_default(),
            Value::Array(items) => items
                .iter()
                .filter_map(|item| item.as_str().map(ToOwned::to_owned))
                .collect::<Vec<_>>()
                .join("\n"),
            other => serde_json::to_string(other).unwrap_or_default(),
        },
    }
}

#[allow(clippy::result_large_err)]
fn parse_compat_request_overrides(
    metadata: Option<&Value>,
) -> Result<CompatRequestOverrides, Response> {
    let Some(metadata) = metadata else {
        return Ok(CompatRequestOverrides::default());
    };
    let Some(metadata_object) = metadata.as_object() else {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_metadata",
            "metadata must be a JSON object when provided",
        ));
    };
    Ok(CompatRequestOverrides {
        session_id: metadata_object
            .get("palyra_session_id")
            .and_then(Value::as_str)
            .and_then(|value| trim_to_option(value.to_owned())),
        session_key: metadata_object
            .get("palyra_session_key")
            .and_then(Value::as_str)
            .and_then(|value| trim_to_option(value.to_owned())),
        session_label: metadata_object
            .get("palyra_session_label")
            .and_then(Value::as_str)
            .and_then(|value| trim_to_option(value.to_owned())),
        require_existing: metadata_object
            .get("palyra_require_existing")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        reset_session: metadata_object
            .get("palyra_reset_session")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn derive_compat_session_key(
    token: &AuthenticatedApiToken,
    user: Option<&str>,
    explicit_session_key: Option<&str>,
) -> String {
    if let Some(explicit_session_key) =
        explicit_session_key.and_then(|value| trim_to_option(value.to_owned()))
    {
        return explicit_session_key;
    }
    if let Some(user) = user.and_then(|value| trim_to_option(value.to_owned())) {
        let normalized = user
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '-' })
            .collect::<String>()
            .trim_matches('-')
            .to_owned();
        if !normalized.is_empty() {
            return format!("compat:{}:{normalized}", token.token_id);
        }
    }
    format!("compat:{}:{}", token.token_id, Ulid::new())
}

fn build_compat_message_envelope(
    session_id: &str,
    sender_display: &str,
    sender_handle: &str,
    text: String,
    timestamp_unix_ms: i64,
) -> common_v1::MessageEnvelope {
    common_v1::MessageEnvelope {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        envelope_id: Some(common_v1::CanonicalId { ulid: Ulid::new().to_string() }),
        timestamp_unix_ms,
        origin: Some(common_v1::EnvelopeOrigin {
            r#type: common_v1::envelope_origin::OriginType::Channel as i32,
            channel: COMPAT_API_CHANNEL.to_owned(),
            conversation_id: session_id.to_owned(),
            sender_display: sender_display.to_owned(),
            sender_handle: sender_handle.to_owned(),
            sender_verified: true,
        }),
        content: Some(common_v1::MessageContent { text, attachments: Vec::new() }),
        security: None,
        max_payload_bytes: 0,
    }
}

#[allow(clippy::result_large_err)]
fn authorize_compat_api_token(
    state: &AppState,
    headers: &HeaderMap,
    required_scope: &str,
    additional_feature_flag: Option<&str>,
    now: i64,
) -> Result<AuthenticatedApiToken, Response> {
    let raw_token = extract_bearer_token(headers)?;
    let registry = lock_access_registry(&state.access_registry);
    registry
        .require_feature_enabled(FEATURE_COMPAT_API)
        .map_err(access_registry_to_compat_response)?;
    registry
        .require_feature_enabled(FEATURE_API_TOKENS)
        .map_err(access_registry_to_compat_response)?;
    if let Some(feature_key) = additional_feature_flag {
        registry
            .require_feature_enabled(feature_key)
            .map_err(access_registry_to_compat_response)?;
    }
    registry
        .authenticate_api_token(raw_token.as_str(), required_scope, now)
        .map_err(access_registry_to_compat_response)
}

fn load_model_provider_config(state: &AppState) -> crate::model_provider::ModelProviderConfig {
    match state.loaded_config.lock() {
        Ok(guard) => guard.model_provider.clone(),
        Err(poisoned) => {
            tracing::warn!("loaded config lock poisoned while reading compat embeddings config");
            poisoned.into_inner().model_provider.clone()
        }
    }
}

#[allow(clippy::result_large_err)]
fn extract_bearer_token(headers: &HeaderMap) -> Result<String, Response> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(|value| trim_to_option(value.to_owned()))
        .ok_or_else(|| {
            compat_error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "missing_bearer_token",
                "missing Authorization: Bearer <token> header",
            )
        })
}

#[allow(clippy::result_large_err)]
fn enforce_compat_rate_limit(
    state: &AppState,
    token_id: &str,
    rate_limit_per_minute: u32,
) -> Result<(), Response> {
    let mut buckets = lock_compat_rate_limit_map(&state.compat_api_rate_limit);
    let bucket = buckets.entry(token_id.to_owned()).or_insert_with(|| CompatApiRateLimitEntry {
        window_started_at: Instant::now(),
        requests_in_window: 0,
    });
    if bucket.window_started_at.elapsed() >= Duration::from_secs(60) {
        bucket.window_started_at = Instant::now();
        bucket.requests_in_window = 0;
    }
    if bucket.requests_in_window >= rate_limit_per_minute {
        return Err(compat_error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "rate_limit_exceeded",
            format!(
                "compat API token exceeded the configured limit of {rate_limit_per_minute} requests per minute"
            ),
        ));
    }
    bucket.requests_in_window = bucket.requests_in_window.saturating_add(1);
    Ok(())
}

fn touch_compat_api_token(
    state: &AppState,
    token_id: &str,
    category: &str,
    outcome: &str,
    detail: Option<&str>,
    now: i64,
) {
    let result = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.touch_api_token(token_id, FEATURE_COMPAT_API, category, outcome, detail, now)
    };
    if let Err(error) = result {
        tracing::warn!(
            token_id = %token_id,
            error = %error,
            "failed to record compat API token activity"
        );
    }
}

async fn auto_deny_compat_tool_approval(
    request_sender: &mpsc::Sender<common_v1::RunStreamRequest>,
    session_id: &str,
    run_id: &str,
    request: &common_v1::ToolApprovalRequest,
) -> common_v1::ToolApprovalResponse {
    let response = common_v1::ToolApprovalResponse {
        proposal_id: request.proposal_id.clone(),
        approved: false,
        reason: "interactive_tool_approval_not_supported_for_compat_api".to_owned(),
        approval_id: request.approval_id.clone(),
        decision_scope: common_v1::ApprovalDecisionScope::Once as i32,
        decision_scope_ttl_ms: 0,
    };
    let response_for_request = response.clone();
    let _ = request_sender
        .send(common_v1::RunStreamRequest {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
            run_id: Some(common_v1::CanonicalId { ulid: run_id.to_owned() }),
            input: None,
            allow_sensitive_tools: false,
            session_key: String::new(),
            session_label: String::new(),
            reset_session: false,
            require_existing: true,
            tool_approval_response: Some(response_for_request),
            origin_kind: String::new(),
            origin_run_id: None,
            parameter_delta_json: Vec::new(),
            queued_input_id: None,
        })
        .await;
    response
}

fn build_compat_gateway_endpoint(state: &AppState) -> Result<tonic::transport::Endpoint, String> {
    tonic::transport::Endpoint::from_shared(state.grpc_url.clone())
        .map_err(|error| format!("invalid gateway endpoint: {error}"))
        .map(|endpoint| {
            endpoint.connect_timeout(Duration::from_secs(2)).timeout(Duration::from_secs(90))
        })
}

async fn stateful_run_snapshot(
    state: &AppState,
    run_id: &str,
) -> Result<journal::OrchestratorRunStatusSnapshot, Response> {
    state
        .runtime
        .orchestrator_run_status_snapshot(run_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            compat_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "missing_run_status",
                format!("run status snapshot missing for compat run {run_id}"),
            )
        })
}

fn lock_access_registry<'a>(
    registry: &'a Arc<Mutex<AccessRegistry>>,
) -> std::sync::MutexGuard<'a, AccessRegistry> {
    match registry.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("access registry lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

fn lock_compat_rate_limit_map<'a>(
    buckets: &'a Arc<Mutex<HashMap<String, CompatApiRateLimitEntry>>>,
) -> std::sync::MutexGuard<'a, HashMap<String, CompatApiRateLimitEntry>> {
    match buckets.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("compat API rate limit map lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

fn access_registry_to_compat_response(error: AccessRegistryError) -> Response {
    match error {
        AccessRegistryError::InvalidApiToken => compat_error_response(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "invalid_api_token",
            "API token is invalid, expired, or revoked",
        ),
        AccessRegistryError::MissingScope(scope) => compat_error_response(
            StatusCode::FORBIDDEN,
            "access_error",
            "missing_scope",
            format!("API token is missing required scope '{scope}'"),
        ),
        AccessRegistryError::FeatureDisabled(feature) => compat_error_response(
            StatusCode::FORBIDDEN,
            "access_error",
            "feature_disabled",
            format!("feature '{feature}' is disabled for the compat API"),
        ),
        AccessRegistryError::AccessDenied(message) => {
            compat_error_response(StatusCode::FORBIDDEN, "access_error", "access_denied", message)
        }
        AccessRegistryError::InvalidField { field, message } => {
            compat_error_response(StatusCode::BAD_REQUEST, "invalid_request_error", field, message)
        }
        other => compat_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "registry_error",
            other.to_string(),
        ),
    }
}

fn compat_error_response(
    status: StatusCode,
    error_type: &str,
    code: &str,
    message: impl Into<String>,
) -> Response {
    let body = compat_error_payload(error_type, code, message);
    (status, Json(body)).into_response()
}

fn compat_error_payload(error_type: &str, code: &str, message: impl Into<String>) -> Value {
    json!({
        "error": {
            "message": message.into(),
            "type": error_type,
            "param": Value::Null,
            "code": code,
        }
    })
}

fn compat_error_body_from_response(response: &Response) -> Value {
    json!({
        "error": {
            "message": format!("compat API request failed with status {}", response.status()),
            "type": "server_error",
            "param": Value::Null,
            "code": "request_failed",
        }
    })
}

fn compat_completion_id(run_id: &str) -> String {
    format!("chatcmpl_{run_id}")
}

fn json_string_from_bytes(bytes: &[u8]) -> String {
    serde_json::from_slice::<Value>(bytes)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned())
}

async fn send_sse_data(sender: &mpsc::Sender<Result<Bytes, Infallible>>, payload: Value) -> bool {
    let mut encoded = b"data: ".to_vec();
    let mut body = match serde_json::to_vec(&payload) {
        Ok(body) => body,
        Err(error) => {
            tracing::error!(error = %error, "failed to serialize compat SSE data payload");
            return false;
        }
    };
    encoded.append(&mut body);
    encoded.extend_from_slice(b"\n\n");
    send_sse_bytes(sender, Bytes::from(encoded), "data").await
}

async fn send_sse_event(
    sender: &mpsc::Sender<Result<Bytes, Infallible>>,
    event: &str,
    payload: Value,
) -> bool {
    let mut body = match serde_json::to_vec(&payload) {
        Ok(body) => body,
        Err(error) => {
            tracing::error!(
                error = %error,
                event,
                "failed to serialize compat SSE event payload"
            );
            return false;
        }
    };
    let mut encoded = format!("event: {event}\ndata: ").into_bytes();
    encoded.append(&mut body);
    encoded.extend_from_slice(b"\n\n");
    send_sse_bytes(sender, Bytes::from(encoded), event).await
}

async fn send_sse_comment(sender: &mpsc::Sender<Result<Bytes, Infallible>>, comment: &str) -> bool {
    send_sse_bytes(sender, Bytes::from(format!(": {comment}\n\n")), "comment").await
}

async fn send_sse_done(sender: &mpsc::Sender<Result<Bytes, Infallible>>) -> bool {
    send_sse_bytes(sender, Bytes::from_static(b"data: [DONE]\n\n"), "done").await
}

async fn send_sse_bytes(
    sender: &mpsc::Sender<Result<Bytes, Infallible>>,
    bytes: Bytes,
    frame_kind: &str,
) -> bool {
    send_sse_bytes_with_timeout(sender, bytes, frame_kind, COMPAT_SSE_SEND_TIMEOUT).await
}

async fn send_sse_bytes_with_timeout(
    sender: &mpsc::Sender<Result<Bytes, Infallible>>,
    bytes: Bytes,
    frame_kind: &str,
    timeout: Duration,
) -> bool {
    match tokio::time::timeout(timeout, sender.send(Ok(bytes))).await {
        Ok(Ok(())) => true,
        Ok(Err(_closed)) => false,
        Err(_elapsed) => {
            tracing::warn!(
                frame_kind,
                timeout_ms = timeout.as_millis(),
                "compat SSE stream buffer did not drain before timeout"
            );
            false
        }
    }
}

fn internal_clock_error_response(error: impl std::fmt::Display) -> Response {
    compat_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "server_error",
        "clock_error",
        format!("failed to read system clock: {error}"),
    )
}

fn internal_runtime_error_response(error: impl std::fmt::Display) -> Response {
    compat_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "server_error",
        "runtime_error",
        error.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_compat_messages_prompt_keeps_roles_and_names() {
        let prompt = render_compat_messages_prompt(&[
            CompatChatMessage {
                role: "system".to_owned(),
                content: CompatMessageContent::Text("Set the tone.".to_owned()),
                name: None,
            },
            CompatChatMessage {
                role: "user".to_owned(),
                content: CompatMessageContent::Parts(vec![CompatMessagePart {
                    kind: "input_text".to_owned(),
                    text: None,
                    input_text: Some("Explain rollout.".to_owned()),
                }]),
                name: Some("alice".to_owned()),
            },
        ])
        .expect("prompt should render");

        assert!(prompt.contains("SYSTEM:"));
        assert!(prompt.contains("USER (alice):"));
        assert!(prompt.contains("Explain rollout."));
    }

    #[test]
    fn parse_compat_request_overrides_reads_palyra_metadata_keys() {
        let overrides = parse_compat_request_overrides(Some(&json!({
            "palyra_session_key": "release-rollout",
            "palyra_session_label": "Release rollout",
            "palyra_require_existing": true,
            "palyra_reset_session": false
        })))
        .expect("metadata should parse");

        assert_eq!(overrides.session_key.as_deref(), Some("release-rollout"));
        assert_eq!(overrides.session_label.as_deref(), Some("Release rollout"));
        assert!(overrides.require_existing);
        assert!(!overrides.reset_session);
    }

    #[test]
    fn extract_bearer_token_requires_bearer_prefix() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer palyra_test"));
        assert_eq!(
            extract_bearer_token(&headers).expect("bearer token should parse"),
            "palyra_test"
        );

        let mut invalid_headers = HeaderMap::new();
        invalid_headers.insert("authorization", HeaderValue::from_static("Basic abc"));
        assert!(extract_bearer_token(&invalid_headers).is_err());
    }

    #[tokio::test]
    async fn sse_send_returns_false_when_bounded_buffer_does_not_drain() {
        let (sender, _receiver) = mpsc::channel::<Result<Bytes, Infallible>>(1);
        assert!(
            send_sse_bytes_with_timeout(
                &sender,
                Bytes::from_static(b"data: first\n\n"),
                "test",
                Duration::from_millis(1),
            )
            .await,
            "first frame should fit in the bounded SSE buffer"
        );
        assert!(
            !send_sse_bytes_with_timeout(
                &sender,
                Bytes::from_static(b"data: second\n\n"),
                "test",
                Duration::from_millis(1),
            )
            .await,
            "full SSE buffer should fail instead of blocking indefinitely"
        );
    }

    #[tokio::test]
    async fn responses_failed_stream_event_emits_terminal_sse_frame() {
        let (sender, mut receiver) = mpsc::channel::<Result<Bytes, Infallible>>(1);

        assert!(
            send_compat_responses_failed_stream_event(
                &sender,
                CompatResponsesFailedStreamEvent {
                    response_id: "resp_01",
                    run_id: "run_01",
                    session_id: "session_01",
                    created_at_unix_ms: 42_000,
                    code: "provider.stream.malformed_chunk",
                    message: "provider stream emitted malformed SSE".to_owned(),
                    public_event: None,
                },
            )
            .await,
            "failed stream event should fit in the bounded SSE buffer"
        );

        let frame = receiver.recv().await.expect("failed SSE frame should be sent");
        let bytes = frame.expect("failed SSE frame should carry bytes");
        let encoded = std::str::from_utf8(bytes.as_ref()).expect("SSE frame should be UTF-8");
        let data = encoded
            .strip_prefix("event: response.failed\ndata: ")
            .expect("frame should use response.failed SSE event")
            .trim();
        let payload: Value = serde_json::from_str(data).expect("SSE payload should be JSON");

        assert_eq!(payload.pointer("/type").and_then(Value::as_str), Some("response.failed"));
        assert_eq!(payload.pointer("/response/status").and_then(Value::as_str), Some("failed"));
        assert_eq!(
            payload.pointer("/response/error/code").and_then(Value::as_str),
            Some("provider.stream.malformed_chunk")
        );
        assert_eq!(
            payload.pointer("/response/_palyra/run_id").and_then(Value::as_str),
            Some("run_01")
        );
        assert_eq!(
            payload.pointer("/response/_palyra/session_id").and_then(Value::as_str),
            Some("session_01")
        );
    }

    #[test]
    fn responses_tool_result_stream_payload_uses_artifact_ref_without_raw_output() {
        let tool_call = CompatStreamToolCall {
            id: "tool_01".to_owned(),
            name: "palyra.fs.read_file".to_owned(),
            arguments: r#"{"path":"report.md"}"#.to_owned(),
            output_index: 1,
        };
        let public_event = json!({
            "event": "tool.call.completed",
            "payload": {
                "success": true,
                "output_json": { "secret": "sk-test-secret" },
                "error": ""
            }
        });
        let result = common_v1::ToolResult {
            proposal_id: Some(common_v1::CanonicalId { ulid: "tool_01".to_owned() }),
            success: true,
            output_json: br#"{"secret":"sk-test-secret"}"#.to_vec(),
            error: String::new(),
        };

        let payload = build_compat_responses_tool_result_stream_payload(
            "resp_01",
            "run_01",
            "session_01",
            Some(&tool_call),
            &result,
            Some(&public_event),
        );
        let encoded = payload.to_string();

        assert_eq!(payload.pointer("/tool_result/success").and_then(Value::as_bool), Some(true));
        assert_eq!(
            payload.pointer("/tool_result/output_visibility").and_then(Value::as_str),
            Some("artifact_ref")
        );
        assert_eq!(
            payload.pointer("/tool_result/output_ref/kind").and_then(Value::as_str),
            Some("run_journal_tool_output")
        );
        assert_eq!(
            payload
                .pointer("/_palyra/public_event/payload/output_json/visibility")
                .and_then(Value::as_str),
            Some("artifact_ref")
        );
        assert!(
            !encoded.contains("sk-test-secret"),
            "Responses SSE tool result payload must not contain raw tool output: {encoded}"
        );
    }

    #[test]
    fn responses_approval_required_stream_payload_withholds_raw_input_details() {
        let public_event = json!({
            "event": "approval.required",
            "payload": {
                "input_json": { "secret": "raw-command" },
                "prompt": {
                    "details_json": { "secret": "raw-details" }
                }
            }
        });
        let request = common_v1::ToolApprovalRequest {
            proposal_id: Some(common_v1::CanonicalId { ulid: "tool_01".to_owned() }),
            tool_name: "palyra.fs.apply_patch".to_owned(),
            input_json: br#"{"secret":"raw-command"}"#.to_vec(),
            approval_required: true,
            approval_id: Some(common_v1::CanonicalId { ulid: "approval_01".to_owned() }),
            prompt: Some(common_v1::ApprovalPrompt {
                title: "Apply patch".to_owned(),
                risk_level: common_v1::ApprovalRiskLevel::Medium as i32,
                subject_id: "tool_01".to_owned(),
                summary: "Patch workspace".to_owned(),
                options: Vec::new(),
                timeout_seconds: 30,
                details_json: br#"{"secret":"raw-details"}"#.to_vec(),
                policy_explanation: "workspace write requires approval".to_owned(),
            }),
            request_summary: "Patch workspace".to_owned(),
        };

        let payload = build_compat_responses_approval_required_stream_payload(
            "resp_01",
            "run_01",
            "session_01",
            &request,
            Some(&public_event),
        );
        let encoded = payload.to_string();

        assert_eq!(payload.get("type").and_then(Value::as_str), Some("approval.required"));
        assert_eq!(payload.get("approval_id").and_then(Value::as_str), Some("approval_01"));
        assert_eq!(payload.get("tool_call_id").and_then(Value::as_str), Some("tool_01"));
        assert_eq!(payload.get("risk_level").and_then(Value::as_str), Some("medium"));
        assert_eq!(
            payload
                .pointer("/_palyra/public_event/payload/input_json/visibility")
                .and_then(Value::as_str),
            Some("withheld")
        );
        assert_eq!(
            payload
                .pointer("/_palyra/public_event/payload/prompt/details_json/visibility")
                .and_then(Value::as_str),
            Some("withheld")
        );
        assert!(
            !encoded.contains("raw-command") && !encoded.contains("raw-details"),
            "approval SSE payload must not contain raw approval input details: {encoded}"
        );
    }

    #[test]
    fn run_approval_validation_accepts_supported_decisions_and_rejects_unsafe_forms() {
        let allow = CompatRunApprovalRequest {
            action: Some("approve".to_owned()),
            decision: None,
            approved: Some(true),
            approval_id: None,
            reason: None,
            decision_scope: Some("once".to_owned()),
            decision_scope_ttl_ms: None,
            expected_version: None,
        };
        assert_eq!(
            parse_compat_run_approval_decision(&allow)
                .unwrap_or_else(|_| panic!("approve should parse")),
            journal::ApprovalDecision::Allow
        );
        assert_eq!(
            parse_compat_approval_decision_scope(allow.decision_scope.as_deref())
                .unwrap_or_else(|_| panic!("once scope should parse")),
            journal::ApprovalDecisionScope::Once
        );

        let deny = CompatRunApprovalRequest {
            action: Some("deny".to_owned()),
            decision: None,
            approved: Some(false),
            approval_id: None,
            reason: None,
            decision_scope: Some("session".to_owned()),
            decision_scope_ttl_ms: None,
            expected_version: None,
        };
        assert_eq!(
            parse_compat_run_approval_decision(&deny)
                .unwrap_or_else(|_| panic!("deny should parse")),
            journal::ApprovalDecision::Deny
        );

        let timeout = CompatRunApprovalRequest {
            action: Some("timeout".to_owned()),
            decision: None,
            approved: None,
            approval_id: None,
            reason: None,
            decision_scope: Some("timeboxed".to_owned()),
            decision_scope_ttl_ms: Some(60_000),
            expected_version: None,
        };
        let timeout_scope = parse_compat_approval_decision_scope(timeout.decision_scope.as_deref())
            .unwrap_or_else(|_| panic!("timeboxed scope should parse"));
        assert_eq!(
            parse_compat_run_approval_decision(&timeout)
                .unwrap_or_else(|_| panic!("timeout should parse")),
            journal::ApprovalDecision::Timeout
        );
        validate_compat_approval_ttl(timeout_scope, timeout.decision_scope_ttl_ms)
            .unwrap_or_else(|_| panic!("timeboxed scope with ttl should pass"));

        let conflicting = CompatRunApprovalRequest {
            action: Some("approve".to_owned()),
            decision: None,
            approved: Some(false),
            approval_id: None,
            reason: None,
            decision_scope: None,
            decision_scope_ttl_ms: None,
            expected_version: None,
        };
        assert!(
            parse_compat_run_approval_decision(&conflicting).is_err(),
            "conflicting approved/action inputs must fail closed"
        );

        let modify = CompatRunApprovalRequest {
            action: Some("modify".to_owned()),
            decision: None,
            approved: None,
            approval_id: None,
            reason: None,
            decision_scope: None,
            decision_scope_ttl_ms: None,
            expected_version: None,
        };
        assert!(
            parse_compat_run_approval_decision(&modify).is_err(),
            "modify is unsupported until the approval store can persist safe input deltas"
        );

        validate_compat_approval_ttl(journal::ApprovalDecisionScope::Timeboxed, None)
            .expect_err("timeboxed approvals require an explicit ttl");
    }
}
