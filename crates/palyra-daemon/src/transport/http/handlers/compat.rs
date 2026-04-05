use crate::{
    access_control::{
        AccessRegistry, AccessRegistryError, AuthenticatedApiToken, FEATURE_API_TOKENS,
        FEATURE_COMPAT_API, PERMISSION_COMPAT_CHAT_CREATE, PERMISSION_COMPAT_MODELS_READ,
        PERMISSION_COMPAT_RESPONSES_CREATE,
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

pub(crate) async fn compat_models_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let token = authorize_compat_api_token(&state, &headers, PERMISSION_COMPAT_MODELS_READ, now)?;
    enforce_compat_rate_limit(&state, token.token_id.as_str(), token.rate_limit_per_minute)?;
    let provider = state.runtime.model_provider_status_snapshot();
    let models = build_compat_models(provider.clone());
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
    let token = authorize_compat_api_token(state, headers, required_scope, now)?;
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

fn build_compat_models(provider: model_provider::ProviderStatusSnapshot) -> Vec<Value> {
    let mut models = Vec::new();
    let chat_model =
        provider.openai_model.clone().unwrap_or_else(|| format!("palyra-{}", provider.kind));
    models.push(json!({
        "id": chat_model,
        "object": "model",
        "created": 0,
        "owned_by": "palyra",
        "metadata": {
            "provider_kind": provider.kind,
            "supports_streaming_tokens": provider.capabilities.streaming_tokens,
            "supports_tool_calls": provider.capabilities.tool_calls,
            "supports_json_mode": provider.capabilities.json_mode,
            "supports_vision": provider.capabilities.vision,
        }
    }));
    if let Some(embeddings_model) = provider.openai_embeddings_model {
        models.push(json!({
            "id": embeddings_model,
            "object": "model",
            "created": 0,
            "owned_by": "palyra",
            "metadata": {
                "provider_kind": "embeddings",
                "dimensions": provider.openai_embeddings_dims,
            }
        }));
    }
    models
}

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

fn authorize_compat_api_token(
    state: &AppState,
    headers: &HeaderMap,
    required_scope: &str,
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
    registry
        .authenticate_api_token(raw_token.as_str(), required_scope, now)
        .map_err(access_registry_to_compat_response)
}

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
            "palyra_session_key": "phase10",
            "palyra_session_label": "Phase 10 rollout",
            "palyra_require_existing": true,
            "palyra_reset_session": false
        })))
        .expect("metadata should parse");

        assert_eq!(overrides.session_key.as_deref(), Some("phase10"));
        assert_eq!(overrides.session_label.as_deref(), Some("Phase 10 rollout"));
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
