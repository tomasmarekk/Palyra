use crate::{
    access_control::{
        AccessRegistry, AccessRegistryError, AuthenticatedApiToken, FEATURE_API_TOKENS,
        FEATURE_COMPAT_API, FEATURE_COMPAT_EMBEDDINGS_API, FEATURE_COMPAT_TOOLS_INVOKE,
        PERMISSION_COMPAT_CHAT_CREATE, PERMISSION_COMPAT_EMBEDDINGS_CREATE,
        PERMISSION_COMPAT_MODELS_READ, PERMISSION_COMPAT_RESPONSES_CREATE,
        PERMISSION_COMPAT_TOOLS_INVOKE,
    },
    app::state::CompatApiRateLimitEntry,
    *,
};

const COMPAT_API_CHANNEL: &str = "compat-api";

#[derive(Debug, Deserialize)]
pub(crate) struct CompatChatCompletionsRequest {
    model: Option<String>,
    messages: Vec<CompatChatMessage>,
    stream: Option<bool>,
    user: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CompatResponsesRequest {
    model: Option<String>,
    input: CompatResponsesInput,
    stream: Option<bool>,
    user: Option<String>,
    metadata: Option<Value>,
}

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

#[derive(Debug, Deserialize)]
pub(crate) struct CompatChatMessage {
    role: String,
    content: CompatMessageContent,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum CompatResponsesInput {
    Text(String),
    Messages(Vec<CompatResponseInputItem>),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum CompatEmbeddingsInput {
    Text(String),
    Texts(Vec<String>),
}

#[derive(Debug, Deserialize)]
pub(crate) struct CompatResponseInputItem {
    role: Option<String>,
    content: CompatMessageContent,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum CompatMessageContent {
    Text(String),
    Parts(Vec<CompatMessagePart>),
    Json(Value),
}

#[derive(Debug, Deserialize)]
pub(crate) struct CompatMessagePart {
    #[serde(rename = "type")]
    kind: String,
    text: Option<String>,
    input_text: Option<String>,
}

#[derive(Debug, Default)]
struct CompatRequestOverrides {
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: bool,
    reset_session: bool,
}

#[derive(Debug)]
struct CompatPreparedRun {
    token: AuthenticatedApiToken,
    provider_kind: String,
    model_name: String,
    run_id: String,
    session_id: String,
    created_at_unix_ms: i64,
    request_sender: mpsc::Sender<common_v1::RunStreamRequest>,
    run_request: TonicRequest<ReceiverStream<common_v1::RunStreamRequest>>,
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
struct CompatModelDescriptor {
    id: String,
    role: &'static str,
    provider_kind: String,
    provider_id: String,
    credential_id: String,
    health_status: String,
    discovery_status: String,
    default_model: bool,
    enabled: bool,
    dimensions: Option<u32>,
    capabilities: Option<model_provider::ProviderCapabilitiesSnapshot>,
}

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
        return Ok(build_compat_chat_streaming_response(state, prepared));
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

pub(crate) async fn compat_responses_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CompatResponsesRequest>,
) -> Result<Response, Response> {
    if payload.stream.unwrap_or(false) {
        return Err(compat_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "unsupported_stream",
            "stream=true is not supported yet for /v1/responses",
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
    let prepared = prepare_compat_run(
        &state,
        &headers,
        payload.model.as_deref(),
        payload.user.as_deref(),
        payload.metadata.as_ref(),
        prompt_text,
        PERMISSION_COMPAT_RESPONSES_CREATE,
    )
    .await?;
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
                "responses_completed",
                Some(run_id.as_str()),
                now,
            );
            Ok(Json(build_compat_responses_payload(&result)).into_response())
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
            Err(response)
        }
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
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let token = authorize_compat_api_token(state, headers, required_scope, None, now)?;
    enforce_compat_rate_limit(state, token.token_id.as_str(), token.rate_limit_per_minute)?;

    let provider = state.runtime.model_provider_status_snapshot();
    let model_name = validate_compat_requested_model(&provider, requested_model)?;
    let overrides = parse_compat_request_overrides(metadata)?;
    let (principal, device_id) = {
        let registry = lock_access_registry(&state.access_registry);
        let workspace_access = registry
            .resolve_workspace_access_for_token(&token, required_scope)
            .map_err(access_registry_to_compat_response)?;
        if let Some(workspace_access) = workspace_access {
            (workspace_access.runtime_principal, workspace_access.runtime_device_id)
        } else {
            (token.principal.clone(), token.token_id.clone())
        }
    };
    let session_key = derive_compat_session_key(&token, user, overrides.session_key.as_deref());
    let session = state
        .runtime
        .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
            session_id: None,
            session_key: Some(session_key),
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
        provider_kind: provider.kind,
        model_name,
        run_id,
        session_id: session.session.session_id,
        created_at_unix_ms,
        request_sender,
        run_request,
    })
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
                Some(common_v1::run_stream_event::Body::Status(status)) => {
                    if common_v1::stream_status::StatusKind::try_from(status.kind)
                        .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
                        == common_v1::stream_status::StatusKind::Failed
                    {
                        final_error =
                            Some(sanitize_http_error_message(status.message.as_str()).to_owned());
                    }
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
    json!({
        "id": format!("resp_{}", result.snapshot.run_id),
        "object": "response",
        "created": result.snapshot.created_at_unix_ms / 1_000,
        "status": "completed",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": result.content,
            }],
        }],
        "tool_calls": result.tool_calls.iter().map(compat_tool_call_json).collect::<Vec<Value>>(),
        "usage": compat_usage_json(&result.snapshot),
        "_palyra": compat_interop_json(&result.snapshot),
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

fn build_compat_chat_streaming_response(state: AppState, prepared: CompatPreparedRun) -> Response {
    let (sender, receiver) = mpsc::channel::<Result<Bytes, Infallible>>(32);
    tokio::spawn(async move {
        let CompatPreparedRun {
            token,
            provider_kind,
            model_name,
            run_id,
            session_id,
            created_at_unix_ms,
            request_sender,
            run_request,
        } = prepared;
        let response_id = compat_completion_id(run_id.as_str());
        let created_seconds = created_at_unix_ms / 1_000;
        let mut finish_reason = "stop";
        let mut stream_error = None::<String>;
        let mut tool_call_index = 0usize;

        if !send_sse_data(
            &sender,
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
                "_palyra": {
                    "origin": "compat_api",
                    "run_id": run_id,
                    "session_id": session_id,
                    "approval_mode": "shared_palyra_approvals",
                },
            }),
        )
        .await
        {
            return;
        }

        let endpoint = match build_compat_gateway_endpoint(&state) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                let _ = send_sse_data(
                    &sender,
                    compat_error_payload("server_error", "gateway_unavailable", error),
                )
                .await;
                let _ = send_sse_done(&sender).await;
                return;
            }
        };
        let channel = match endpoint.connect().await {
            Ok(channel) => channel,
            Err(error) => {
                let _ = send_sse_data(
                    &sender,
                    compat_error_payload(
                        "server_error",
                        "gateway_unavailable",
                        format!("failed to connect compat API to gateway: {error}"),
                    ),
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
                let _ = send_sse_data(
                    &sender,
                    compat_error_payload(
                        "server_error",
                        "gateway_stream_failed",
                        sanitize_http_error_message(error.message()),
                    ),
                )
                .await;
                let _ = send_sse_done(&sender).await;
                return;
            }
        };

        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => match event.body {
                    Some(common_v1::run_stream_event::Body::ModelToken(token_event)) => {
                        if !send_sse_data(
                            &sender,
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
                            }),
                        )
                        .await
                        {
                            return;
                        }
                    }
                    Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => {
                        finish_reason = "tool_calls";
                        let tool_call_id = proposal
                            .proposal_id
                            .as_ref()
                            .map(|value| value.ulid.clone())
                            .unwrap_or_else(|| Ulid::new().to_string());
                        if !send_sse_data(
                            &sender,
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
                            }),
                        )
                        .await
                        {
                            return;
                        }
                        tool_call_index = tool_call_index.saturating_add(1);
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
                    Some(common_v1::run_stream_event::Body::Status(status)) => {
                        if common_v1::stream_status::StatusKind::try_from(status.kind)
                            .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
                            == common_v1::stream_status::StatusKind::Failed
                        {
                            stream_error = Some(
                                sanitize_http_error_message(status.message.as_str()).to_owned(),
                            );
                            break;
                        }
                    }
                    _ => {}
                },
                Err(error) => {
                    stream_error = Some(sanitize_http_error_message(error.message()).to_owned());
                    break;
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
                        "chat_failed",
                        Some(run_id.as_str()),
                        now,
                    );
                    let _ = send_sse_data(
                        &sender,
                        compat_error_payload("server_error", "run_failed", error),
                    )
                    .await;
                    let _ = send_sse_done(&sender).await;
                    return;
                }
                touch_compat_api_token(
                    &state,
                    token.token_id.as_str(),
                    "run",
                    "chat_completed",
                    Some(run_id.as_str()),
                    now,
                );
                let _ = send_sse_data(
                    &sender,
                    json!({
                        "id": response_id,
                        "object": "chat.completion.chunk",
                        "created": created_seconds,
                        "model": model_name,
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": finish_reason,
                        }],
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
                    "chat_failed",
                    Some(run_id.as_str()),
                    now,
                );
                let body = compat_error_body_from_response(&response);
                let _ = send_sse_data(&sender, body).await;
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

fn build_compat_models(provider: &model_provider::ProviderStatusSnapshot) -> Vec<Value> {
    build_compat_model_descriptors(provider)
        .into_iter()
        .map(|descriptor| compat_model_json(&descriptor))
        .collect()
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
        provider_id: registry_model
            .map(|entry| entry.provider_id.clone())
            .unwrap_or_else(|| provider.provider_id.clone()),
        credential_id: provider.credential_id.clone(),
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
        provider_id: registry_model
            .map(|entry| entry.provider_id.clone())
            .unwrap_or_else(|| provider.provider_id.clone()),
        credential_id: provider.credential_id.clone(),
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
            "provider_id": model.provider_id,
            "credential_id": model.credential_id,
            "role": model.role,
            "default": model.default_model,
            "enabled": model.enabled,
            "health_status": model.health_status,
            "discovery_status": model.discovery_status,
            "dimensions": model.dimensions,
            "supports_streaming_tokens": model.capabilities.as_ref().map(|value| value.streaming_tokens),
            "supports_tool_calls": model.capabilities.as_ref().map(|value| value.tool_calls),
            "supports_json_mode": model.capabilities.as_ref().map(|value| value.json_mode),
            "supports_vision": model.capabilities.as_ref().map(|value| value.vision),
            "supports_audio_transcribe": model.capabilities.as_ref().map(|value| value.audio_transcribe),
            "supports_embeddings": model.capabilities.as_ref().map(|value| value.embeddings),
            "max_context_tokens": model.capabilities.as_ref().and_then(|value| value.max_context_tokens),
            "cost_tier": model.capabilities.as_ref().map(|value| value.cost_tier.clone()),
            "latency_tier": model.capabilities.as_ref().map(|value| value.latency_tier.clone()),
            "recommended_use_cases": model.capabilities.as_ref().map(|value| value.recommended_use_cases.clone()).unwrap_or_default(),
            "known_limitations": model.capabilities.as_ref().map(|value| value.known_limitations.clone()).unwrap_or_default(),
            "metadata_source": model.capabilities.as_ref().map(|value| value.metadata_source.clone()),
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
) {
    let response = common_v1::ToolApprovalResponse {
        proposal_id: request.proposal_id.clone(),
        approved: false,
        reason: "interactive_tool_approval_not_supported_for_compat_api".to_owned(),
        approval_id: request.approval_id.clone(),
        decision_scope: common_v1::ApprovalDecisionScope::Once as i32,
        decision_scope_ttl_ms: 0,
    };
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
            tool_approval_response: Some(response),
            origin_kind: String::new(),
            origin_run_id: None,
            parameter_delta_json: Vec::new(),
            queued_input_id: None,
        })
        .await;
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
    let Ok(mut body) = serde_json::to_vec(&payload) else {
        return true;
    };
    encoded.append(&mut body);
    encoded.extend_from_slice(b"\n\n");
    sender.send(Ok(Bytes::from(encoded))).await.is_ok()
}

async fn send_sse_done(sender: &mpsc::Sender<Result<Bytes, Infallible>>) -> bool {
    sender.send(Ok(Bytes::from_static(b"data: [DONE]\n\n"))).await.is_ok()
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
}
