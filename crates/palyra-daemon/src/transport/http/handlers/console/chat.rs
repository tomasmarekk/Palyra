use crate::*;
use base64::Engine as _;

pub(crate) async fn console_chat_sessions_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleChatSessionsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(32).clamp(1, 128);
    let (sessions, next_after_session_key) = state
        .runtime
        .list_orchestrator_sessions(gateway::ListOrchestratorSessionsRequest {
            after_session_key: query.after_session_key,
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            include_archived: false,
            requested_limit: Some(limit),
            search_query: None,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "sessions": sessions,
        "next_after_session_key": next_after_session_key,
        "page": build_page_info(limit, sessions.len(), next_after_session_key.clone()),
    })))
}

pub(crate) async fn console_chat_session_resolve_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleChatSessionResolveRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_id = payload.session_id.and_then(trim_to_option);
    if let Some(session_id) = session_id.as_deref() {
        validate_canonical_id(session_id).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let outcome = state
        .runtime
        .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
            session_id,
            session_key: payload.session_key.and_then(trim_to_option),
            session_label: payload.session_label.and_then(trim_to_option),
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            require_existing: payload.require_existing.unwrap_or(false),
            reset_session: payload.reset_session.unwrap_or(false),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": outcome.session,
        "created": outcome.created,
        "reset_applied": outcome.reset_applied,
    })))
}

pub(crate) async fn console_chat_session_rename_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatRenameSessionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let session_label = trim_to_option(payload.session_label).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("session_label cannot be empty"))
    })?;
    let outcome = state
        .runtime
        .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
            session_id: Some(session_id),
            session_key: None,
            session_label: Some(session_label),
            principal: session.context.principal,
            device_id: session.context.device_id,
            channel: session.context.channel,
            require_existing: true,
            reset_session: false,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": outcome.session,
        "created": outcome.created,
        "reset_applied": outcome.reset_applied,
    })))
}

pub(crate) async fn console_chat_session_reset_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let outcome = state
        .runtime
        .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
            session_id: Some(session_id),
            session_key: None,
            session_label: None,
            principal: session.context.principal,
            device_id: session.context.device_id,
            channel: session.context.channel,
            require_existing: true,
            reset_session: true,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": outcome.session,
        "created": outcome.created,
        "reset_applied": outcome.reset_applied,
    })))
}

pub(crate) async fn console_chat_message_stream_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatMessageRequest>,
) -> Result<Response, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let text = trim_to_option(payload.text).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("text cannot be empty"))
    })?;
    let attachments = load_console_chat_message_attachments(
        &state,
        &session.context,
        session_id.as_str(),
        payload.attachments.as_slice(),
    )
    .map_err(|response| *response)?;
    let timestamp_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let run_id = Ulid::new().to_string();

    let (request_sender, request_receiver) = mpsc::channel::<common_v1::RunStreamRequest>(16);
    let pending_approvals = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut streams = lock_console_chat_streams(&state.console_chat_streams);
        streams.insert(
            run_id.clone(),
            ConsoleChatRunStream {
                session_id: session_id.clone(),
                request_sender: request_sender.clone(),
                pending_approvals: Arc::clone(&pending_approvals),
            },
        );
    }

    let initial_request = common_v1::RunStreamRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: run_id.clone() }),
        input: Some(build_console_chat_message_envelope(
            &session,
            session_id.as_str(),
            text,
            timestamp_unix_ms,
            attachments,
        )),
        allow_sensitive_tools: payload.allow_sensitive_tools.unwrap_or(false),
        session_key: String::new(),
        session_label: payload.session_label.and_then(trim_to_option).unwrap_or_default(),
        reset_session: false,
        require_existing: true,
        tool_approval_response: None,
        origin_kind: payload.origin_kind.and_then(trim_to_option).unwrap_or_default(),
        origin_run_id: payload
            .origin_run_id
            .and_then(trim_to_option)
            .map(|ulid| common_v1::CanonicalId { ulid }),
        parameter_delta_json: payload
            .parameter_delta
            .as_ref()
            .and_then(|value| serde_json::to_vec(value).ok())
            .unwrap_or_default(),
        queued_input_id: payload
            .queued_input_id
            .and_then(trim_to_option)
            .map(|ulid| common_v1::CanonicalId { ulid }),
    };
    request_sender.send(initial_request).await.map_err(|_| {
        {
            let mut streams = lock_console_chat_streams(&state.console_chat_streams);
            streams.remove(run_id.as_str());
        }
        runtime_status_response(tonic::Status::internal("failed to queue initial chat run request"))
    })?;

    let mut run_request = TonicRequest::new(ReceiverStream::new(request_receiver));
    if let Err(error_response) =
        apply_console_rpc_context(&state, &session, run_request.metadata_mut())
    {
        let mut streams = lock_console_chat_streams(&state.console_chat_streams);
        streams.remove(run_id.as_str());
        return Err(error_response);
    }

    let (line_sender, line_receiver) = mpsc::channel::<Result<Bytes, Infallible>>(32);
    let run_id_for_task = run_id.clone();
    let session_id_for_task = session_id.clone();
    let state_for_task = state.clone();
    tokio::spawn(async move {
        let mut final_status = "unknown".to_owned();
        if !send_console_chat_line(
            &line_sender,
            json!({
                "type": "meta",
                "run_id": run_id_for_task.clone(),
                "session_id": session_id_for_task.clone(),
            }),
        )
        .await
        {
            let mut streams = lock_console_chat_streams(&state_for_task.console_chat_streams);
            streams.remove(run_id_for_task.as_str());
            return;
        }

        let mut gateway_client = match build_console_gateway_client(&state_for_task).await {
            Ok(client) => client,
            Err(error) => {
                final_status = "failed".to_owned();
                let _ = send_console_chat_line(
                    &line_sender,
                    json!({
                        "type": "error",
                        "run_id": run_id_for_task.clone(),
                        "error": error,
                    }),
                )
                .await;
                let _ = send_console_chat_line(
                    &line_sender,
                    json!({
                        "type": "complete",
                        "run_id": run_id_for_task.clone(),
                        "status": final_status.clone(),
                    }),
                )
                .await;
                let mut streams = lock_console_chat_streams(&state_for_task.console_chat_streams);
                streams.remove(run_id_for_task.as_str());
                return;
            }
        };

        let mut stream = match gateway_client.run_stream(run_request).await {
            Ok(response) => response.into_inner(),
            Err(error) => {
                final_status = "failed".to_owned();
                let _ = send_console_chat_line(
                    &line_sender,
                    json!({
                        "type": "error",
                        "run_id": run_id_for_task.clone(),
                        "error": sanitize_http_error_message(error.message()),
                    }),
                )
                .await;
                let _ = send_console_chat_line(
                    &line_sender,
                    json!({
                        "type": "complete",
                        "run_id": run_id_for_task.clone(),
                        "status": final_status.clone(),
                    }),
                )
                .await;
                let mut streams = lock_console_chat_streams(&state_for_task.console_chat_streams);
                streams.remove(run_id_for_task.as_str());
                return;
            }
        };

        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    if let Some((approval_id, proposal_id)) =
                        run_stream_event_approval_mapping(&event)
                    {
                        let stream_entry = {
                            let streams =
                                lock_console_chat_streams(&state_for_task.console_chat_streams);
                            streams.get(run_id_for_task.as_str()).cloned()
                        };
                        if let Some(stream_entry) = stream_entry {
                            let mut approvals = lock_console_chat_pending_approvals(
                                &stream_entry.pending_approvals,
                            );
                            approvals.insert(approval_id, proposal_id);
                        }
                    }
                    if let Some(kind) = run_stream_status_kind(&event) {
                        final_status = kind.to_owned();
                    }
                    if !send_console_chat_line(
                        &line_sender,
                        json!({
                            "type": "event",
                            "event": console_run_stream_event_to_json(&event),
                        }),
                    )
                    .await
                    {
                        break;
                    }
                }
                Err(error) => {
                    final_status = "failed".to_owned();
                    let _ = send_console_chat_line(
                        &line_sender,
                        json!({
                            "type": "error",
                            "run_id": run_id_for_task.clone(),
                            "error": sanitize_http_error_message(error.message()),
                        }),
                    )
                    .await;
                    break;
                }
            }
        }

        let _ = send_console_chat_line(
            &line_sender,
            json!({
                "type": "complete",
                "run_id": run_id_for_task.clone(),
                "status": final_status.clone(),
            }),
        )
        .await;
        let mut streams = lock_console_chat_streams(&state_for_task.console_chat_streams);
        streams.remove(run_id_for_task.as_str());
    });

    let mut response = Response::new(Body::from_stream(ReceiverStream::new(line_receiver)));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson; charset=utf-8"));
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

pub(crate) async fn console_chat_attachment_upload_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatAttachmentUploadRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let filename = trim_to_option(payload.filename).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("filename cannot be empty"))
    })?;
    let content_type = trim_to_option(payload.content_type).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("content_type cannot be empty"))
    })?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload.bytes_base64.as_bytes())
        .map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "bytes_base64 must be valid base64",
        ))
    })?;
    let artifact = state
        .channels
        .store_console_chat_attachment(channels::ConsoleChatAttachmentStoreRequestView {
            session_id: session_id.as_str(),
            principal: session.context.principal.as_str(),
            device_id: session.context.device_id.as_str(),
            channel: session.context.channel.as_deref(),
            filename: filename.as_str(),
            declared_content_type: content_type.as_str(),
            bytes: bytes.as_slice(),
        })
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "attachment": console_chat_attachment_payload_to_json(&artifact),
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_run_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    let run = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {run_id}"
            )))
        })?;
    if !run_matches_console_context(&run, &session.context) {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "chat run does not belong to the authenticated console session context",
        )));
    }
    Ok(Json(json!({ "run": run })))
}

pub(crate) async fn console_chat_run_events_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<ConsoleChatRunEventsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    let run = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {run_id}"
            )))
        })?;
    if !run_matches_console_context(&run, &session.context) {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "chat run does not belong to the authenticated console session context",
        )));
    }
    let tape = state
        .runtime
        .orchestrator_tape_snapshot(run_id, query.after_seq, query.limit)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "run": run,
        "tape": tape,
    })))
}

pub(crate) async fn console_chat_retry_prepare_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatRetryRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let base_session =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let last_run_id = base_session.last_run_id.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "retry requires a session with a completed turn",
        ))
    })?;
    let run = state
        .runtime
        .orchestrator_run_status_snapshot(last_run_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {last_run_id}"
            )))
        })?;
    if !run_matches_console_context(&run, &session.context) {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "chat run does not belong to the authenticated console session context",
        )));
    }
    if !is_terminal_run_state(run.state.as_str()) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "retry requires the latest run to be terminal",
        )));
    }
    let text = load_last_user_turn_text(&state, session_id.as_str(), Some(last_run_id.as_str()))
        .await?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::failed_precondition(
                "retry requires a persisted user turn in the latest run",
            ))
        })?;
    Ok(Json(json!({
        "session": base_session,
        "text": text,
        "origin_kind": "retry",
        "origin_run_id": last_run_id,
        "parameter_delta": payload.parameter_delta,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_branch_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatBranchRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let source_session =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let source_run_id = source_session.last_run_id.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "branching requires a source run in the current session",
        ))
    })?;
    let source_run = state
        .runtime
        .orchestrator_run_status_snapshot(source_run_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {source_run_id}"
            )))
        })?;
    if !is_terminal_run_state(source_run.state.as_str()) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "branching requires the latest run to be terminal",
        )));
    }

    let branched = state
        .runtime
        .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
            session_id: None,
            session_key: None,
            session_label: payload.session_label.and_then(trim_to_option),
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            require_existing: false,
            reset_session: false,
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .update_orchestrator_session_lineage(journal::OrchestratorSessionLineageUpdateRequest {
            session_id: branched.session.session_id.clone(),
            branch_state: "active_branch".to_owned(),
            parent_session_id: Some(source_session.session_id.clone()),
            branch_origin_run_id: Some(source_run_id.clone()),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .update_orchestrator_session_lineage(journal::OrchestratorSessionLineageUpdateRequest {
            session_id: source_session.session_id.clone(),
            branch_state: "branch_source".to_owned(),
            parent_session_id: source_session.parent_session_id.clone(),
            branch_origin_run_id: source_session.branch_origin_run_id.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .append_orchestrator_tape_event(journal::OrchestratorTapeAppendRequest {
            run_id: source_run_id.clone(),
            seq: source_run.tape_events as i64,
            event_type: "rollback.marker".to_owned(),
            payload_json: json!({
                "event": "rollback.marker",
                "source_session_id": source_session.session_id,
                "branched_session_id": branched.session.session_id,
                "source_run_id": source_run_id,
                "actor_principal": session.context.principal,
            })
            .to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    let branch_session = load_console_chat_session(
        &state,
        &session.context,
        branched.session.session_id.as_str(),
        true,
    )
    .await?;
    Ok(Json(json!({
        "session": branch_session,
        "source_run_id": source_run_id,
        "action": "branch",
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_queue_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(payload): Json<ConsoleChatQueueRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    let text = trim_to_option(payload.text).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("text cannot be empty"))
    })?;
    let stream = {
        let streams = lock_console_chat_streams(&state.console_chat_streams);
        streams.get(run_id.as_str()).cloned()
    }
    .ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "queued follow-up requires an active run stream",
        ))
    })?;
    if !lock_console_chat_pending_approvals(&stream.pending_approvals).is_empty() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "cannot queue a follow-up while approval decisions are still pending",
        )));
    }
    let timestamp_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let queued_input_id = Ulid::new().to_string();
    let mut queued = state
        .runtime
        .create_orchestrator_queued_input(journal::OrchestratorQueuedInputCreateRequest {
            queued_input_id: queued_input_id.clone(),
            run_id: run_id.clone(),
            session_id: stream.session_id.clone(),
            text: text.clone(),
            origin_run_id: Some(run_id.clone()),
        })
        .await
        .map_err(runtime_status_response)?;
    let request = common_v1::RunStreamRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: stream.session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: run_id.clone() }),
        input: Some(build_console_chat_message_envelope(
            &session,
            stream.session_id.as_str(),
            text,
            timestamp_unix_ms,
            Vec::new(),
        )),
        allow_sensitive_tools: false,
        session_key: String::new(),
        session_label: String::new(),
        reset_session: false,
        require_existing: true,
        tool_approval_response: None,
        origin_kind: "queued".to_owned(),
        origin_run_id: Some(common_v1::CanonicalId { ulid: run_id.clone() }),
        parameter_delta_json: Vec::new(),
        queued_input_id: Some(common_v1::CanonicalId { ulid: queued_input_id.clone() }),
    };
    if stream.request_sender.send(request).await.is_err() {
        state
            .runtime
            .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
                queued_input_id,
                state: "delivery_failed".to_owned(),
            })
            .await
            .map_err(runtime_status_response)?;
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "failed to forward queued follow-up to the active run stream",
        )));
    }
    state
        .runtime
        .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
            queued_input_id: queued.queued_input_id.clone(),
            state: "forwarded".to_owned(),
        })
        .await
        .map_err(runtime_status_response)?;
    queued.state = "forwarded".to_owned();
    Ok(Json(json!({
        "queued_input": queued,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_transcript_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let transcript = state
        .runtime
        .list_orchestrator_session_transcript(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let pins = state
        .runtime
        .list_orchestrator_session_pins(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let queued_inputs = state
        .runtime
        .list_orchestrator_queued_inputs(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": session_record,
        "records": transcript,
        "pins": pins,
        "queued_inputs": queued_inputs,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_transcript_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleChatTranscriptSearchQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let search = query.q.trim();
    if search.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument("q cannot be empty")));
    }
    let transcript = state
        .runtime
        .list_orchestrator_session_transcript(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let normalized = search.to_ascii_lowercase();
    let matches = transcript
        .into_iter()
        .filter_map(|record| {
            let text = extract_transcript_search_text(&record)?;
            if !text.to_ascii_lowercase().contains(normalized.as_str()) {
                return None;
            }
            Some(json!({
                "session_id": record.session_id,
                "run_id": record.run_id,
                "seq": record.seq,
                "event_type": record.event_type,
                "created_at_unix_ms": record.created_at_unix_ms,
                "origin_kind": record.origin_kind,
                "origin_run_id": record.origin_run_id,
                "snippet": text,
            }))
        })
        .collect::<Vec<Value>>();
    Ok(Json(json!({
        "session": session_record,
        "query": search,
        "matches": matches,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_export_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleChatTranscriptExportQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let transcript = state
        .runtime
        .list_orchestrator_session_transcript(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let pins = state
        .runtime
        .list_orchestrator_session_pins(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let format =
        query.format.as_deref().map(str::trim).filter(|value| !value.is_empty()).unwrap_or("json");
    if format.eq_ignore_ascii_case("markdown") {
        return Ok(Json(json!({
            "format": "markdown",
            "content": render_session_export_markdown(&session_record, transcript.as_slice(), pins.as_slice()),
            "contract": contract_descriptor(),
        })));
    }
    Ok(Json(json!({
        "format": "json",
        "content": {
            "session": session_record,
            "records": transcript,
            "pins": pins,
        },
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_pins_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let pins = state
        .runtime
        .list_orchestrator_session_pins(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": session_record,
        "pins": pins,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_pin_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatPinRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    validate_canonical_id(payload.run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    let title = trim_to_option(payload.title).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("title cannot be empty"))
    })?;
    let pin = state
        .runtime
        .create_orchestrator_session_pin(journal::OrchestratorSessionPinCreateRequest {
            pin_id: Ulid::new().to_string(),
            session_id: session_record.session_id.clone(),
            run_id: payload.run_id,
            tape_seq: payload.tape_seq,
            title,
            note: payload.note.and_then(trim_to_option),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "pin": pin,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_chat_pin_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, pin_id)): Path<(String, String)>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let _session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let deleted = state
        .runtime
        .delete_orchestrator_session_pin(pin_id)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "deleted": deleted,
        "contract": contract_descriptor(),
    })))
}

fn run_matches_console_context(
    run: &journal::OrchestratorRunStatusSnapshot,
    context: &gateway::RequestContext,
) -> bool {
    if run.principal != context.principal || run.device_id != context.device_id {
        return false;
    }
    match (&run.channel, &context.channel) {
        (Some(left), Some(right)) => left == right,
        (None, None) => true,
        _ => false,
    }
}

fn load_console_chat_message_attachments(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    attachments: &[ConsoleChatAttachmentReference],
) -> Result<Vec<common_v1::MessageAttachment>, Box<Response>> {
    let mut resolved = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let artifact_id = trim_to_option(attachment.artifact_id.clone()).ok_or_else(|| {
            Box::new(runtime_status_response(tonic::Status::invalid_argument(
                "attachment artifact_id cannot be empty",
            )))
        })?;
        validate_canonical_id(artifact_id.as_str()).map_err(|_| {
            Box::new(runtime_status_response(tonic::Status::invalid_argument(
                "attachment artifact_id must be a canonical ULID",
            )))
        })?;
        let payload = state
            .channels
            .load_console_chat_attachment(
                artifact_id.as_str(),
                session_id,
                context.principal.as_str(),
                context.device_id.as_str(),
                context.channel.as_deref(),
            )
            .map_err(channel_platform_error_response)?
            .ok_or_else(|| {
                Box::new(runtime_status_response(tonic::Status::not_found(format!(
                    "console chat attachment not found: {artifact_id}"
                ))))
            })?;
        resolved.push(common_v1::MessageAttachment {
            kind: console_chat_attachment_kind(payload.content_type.as_str()) as i32,
            artifact_id: Some(common_v1::CanonicalId { ulid: payload.artifact_id.clone() }),
            size_bytes: payload.size_bytes,
            attachment_id: payload.artifact_id.clone(),
            filename: payload.filename.clone(),
            declared_content_type: payload.content_type.clone(),
            source_url: String::new(),
            content_hash: payload.sha256.clone(),
            origin: "console_chat_upload".to_owned(),
            policy_context: "attachment.upload.allowed".to_owned(),
            inline_bytes: payload.bytes.clone(),
            upload_requested: true,
            width_px: payload.width_px.unwrap_or_default(),
            height_px: payload.height_px.unwrap_or_default(),
        });
    }
    Ok(resolved)
}

async fn load_console_chat_session(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    require_write: bool,
) -> Result<journal::OrchestratorSessionRecord, Response> {
    let _ = require_write;
    validate_canonical_id(session_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let response = state
        .runtime
        .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
            session_id: Some(session_id.to_owned()),
            session_key: None,
            session_label: None,
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
            require_existing: true,
            reset_session: false,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(response.session)
}

fn is_terminal_run_state(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled")
}

async fn load_last_user_turn_text(
    state: &AppState,
    session_id: &str,
    restrict_run_id: Option<&str>,
) -> Result<Option<String>, Response> {
    let transcript = state
        .runtime
        .list_orchestrator_session_transcript(session_id.to_owned())
        .await
        .map_err(runtime_status_response)?;
    Ok(transcript
        .iter()
        .rev()
        .find(|record| {
            record.event_type == "message.received"
                && restrict_run_id.map(|value| record.run_id == value).unwrap_or(true)
        })
        .and_then(|record| extract_transcript_text(record, "text")))
}

fn extract_transcript_text(
    record: &journal::OrchestratorSessionTranscriptRecord,
    key: &str,
) -> Option<String> {
    serde_json::from_str::<Value>(record.payload_json.as_str())
        .ok()?
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn extract_transcript_search_text(
    record: &journal::OrchestratorSessionTranscriptRecord,
) -> Option<String> {
    match record.event_type.as_str() {
        "message.received" | "queued.input" => extract_transcript_text(record, "text"),
        "message.replied" => extract_transcript_text(record, "reply_text"),
        "rollback.marker" => {
            serde_json::from_str::<Value>(record.payload_json.as_str()).ok().and_then(|payload| {
                payload.get("event").and_then(Value::as_str).map(ToOwned::to_owned)
            })
        }
        _ => None,
    }
}

fn render_session_export_markdown(
    session: &journal::OrchestratorSessionRecord,
    transcript: &[journal::OrchestratorSessionTranscriptRecord],
    pins: &[journal::OrchestratorSessionPinRecord],
) -> String {
    let mut document = String::new();
    let title = if !session.title.trim().is_empty() {
        session.title.as_str()
    } else {
        session.session_id.as_str()
    };
    document.push_str("# ");
    document.push_str(title);
    document.push_str("\n\n");
    document.push_str("- Session ID: `");
    document.push_str(session.session_id.as_str());
    document.push_str("`\n");
    document.push_str("- Branch state: `");
    document.push_str(session.branch_state.as_str());
    document.push_str("`\n");
    if let Some(parent_session_id) = session.parent_session_id.as_deref() {
        document.push_str("- Parent session: `");
        document.push_str(parent_session_id);
        document.push_str("`\n");
    }
    if !pins.is_empty() {
        document.push_str("\n## Pins\n\n");
        for pin in pins {
            document.push_str("- ");
            document.push_str(pin.title.as_str());
            if let Some(note) = pin.note.as_deref() {
                document.push_str(" — ");
                document.push_str(note);
            }
            document.push('\n');
        }
    }
    document.push_str("\n## Transcript\n\n");
    for record in transcript {
        if let Some(text) = extract_transcript_search_text(record) {
            document.push_str("- [");
            document.push_str(record.event_type.as_str());
            document.push_str("] ");
            document.push_str(text.as_str());
            document.push('\n');
        }
    }
    document
}

async fn send_console_chat_line(
    sender: &mpsc::Sender<Result<Bytes, Infallible>>,
    payload: Value,
) -> bool {
    let Some(line) = encode_console_chat_line(payload) else {
        return true;
    };
    sender.send(Ok(line)).await.is_ok()
}

fn encode_console_chat_line(payload: Value) -> Option<Bytes> {
    let mut encoded = serde_json::to_vec(&payload).ok()?;
    encoded.push(b'\n');
    Some(Bytes::from(encoded))
}

fn run_stream_event_approval_mapping(
    event: &common_v1::RunStreamEvent,
) -> Option<(String, String)> {
    let common_v1::run_stream_event::Body::ToolApprovalRequest(request) = event.body.as_ref()?
    else {
        return None;
    };
    let approval_id = request.approval_id.as_ref().map(|value| value.ulid.clone())?;
    let proposal_id = request.proposal_id.as_ref().map(|value| value.ulid.clone())?;
    if approval_id.is_empty() || proposal_id.is_empty() {
        return None;
    }
    Some((approval_id, proposal_id))
}

fn run_stream_status_kind(event: &common_v1::RunStreamEvent) -> Option<&'static str> {
    let common_v1::run_stream_event::Body::Status(status) = event.body.as_ref()? else {
        return None;
    };
    Some(stream_status_kind_label(status.kind))
}

fn console_run_stream_event_to_json(event: &common_v1::RunStreamEvent) -> Value {
    let run_id = event.run_id.as_ref().map(|value| value.ulid.clone()).unwrap_or_default();
    match event.body.as_ref() {
        Some(common_v1::run_stream_event::Body::ModelToken(model_token)) => json!({
            "run_id": run_id,
            "event_type": "model_token",
            "model_token": {
                "token": model_token.token,
                "is_final": model_token.is_final,
            },
        }),
        Some(common_v1::run_stream_event::Body::Status(status)) => json!({
            "run_id": run_id,
            "event_type": "status",
            "status": {
                "kind": stream_status_kind_label(status.kind),
                "message": status.message,
            },
        }),
        Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => json!({
            "run_id": run_id,
            "event_type": "tool_proposal",
            "tool_proposal": {
                "proposal_id": proposal.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "tool_name": proposal.tool_name,
                "input_json": decode_json_bytes_for_console(proposal.input_json.as_slice()),
                "approval_required": proposal.approval_required,
            },
        }),
        Some(common_v1::run_stream_event::Body::ToolDecision(decision)) => json!({
            "run_id": run_id,
            "event_type": "tool_decision",
            "tool_decision": {
                "proposal_id": decision.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "kind": tool_decision_kind_label(decision.kind),
                "reason": decision.reason,
                "approval_required": decision.approval_required,
                "policy_enforced": decision.policy_enforced,
            },
        }),
        Some(common_v1::run_stream_event::Body::ToolResult(result)) => json!({
            "run_id": run_id,
            "event_type": "tool_result",
            "tool_result": {
                "proposal_id": result.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "success": result.success,
                "output_json": decode_json_bytes_for_console(result.output_json.as_slice()),
                "error": result.error,
            },
        }),
        Some(common_v1::run_stream_event::Body::ToolAttestation(attestation)) => json!({
            "run_id": run_id,
            "event_type": "tool_attestation",
            "tool_attestation": {
                "proposal_id": attestation.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "attestation_id": attestation.attestation_id.as_ref().map(|value| value.ulid.clone()),
                "execution_sha256": attestation.execution_sha256,
                "executed_at_unix_ms": attestation.executed_at_unix_ms,
                "timed_out": attestation.timed_out,
                "executor": attestation.executor,
            },
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalRequest(request)) => json!({
            "run_id": run_id,
            "event_type": "tool_approval_request",
            "tool_approval_request": {
                "proposal_id": request.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "approval_id": request.approval_id.as_ref().map(|value| value.ulid.clone()),
                "tool_name": request.tool_name,
                "input_json": decode_json_bytes_for_console(request.input_json.as_slice()),
                "approval_required": request.approval_required,
                "request_summary": request.request_summary,
                "prompt": request.prompt.as_ref().map(|prompt| {
                    json!({
                        "title": prompt.title,
                        "risk_level": approval_risk_level_label(prompt.risk_level),
                        "subject_id": prompt.subject_id,
                        "summary": prompt.summary,
                        "timeout_seconds": prompt.timeout_seconds,
                        "details_json": decode_json_bytes_for_console(prompt.details_json.as_slice()),
                        "policy_explanation": prompt.policy_explanation,
                        "options": prompt.options.iter().map(|option| {
                            json!({
                                "option_id": option.option_id,
                                "label": option.label,
                                "description": option.description,
                                "default_selected": option.default_selected,
                                "decision_scope": approval_scope_label(option.decision_scope),
                                "timebox_ttl_ms": option.timebox_ttl_ms,
                            })
                        }).collect::<Vec<Value>>(),
                    })
                }),
            },
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalResponse(response)) => json!({
            "run_id": run_id,
            "event_type": "tool_approval_response",
            "tool_approval_response": {
                "proposal_id": response.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "approval_id": response.approval_id.as_ref().map(|value| value.ulid.clone()),
                "approved": response.approved,
                "reason": response.reason,
                "decision_scope": approval_scope_label(response.decision_scope),
                "decision_scope_ttl_ms": response.decision_scope_ttl_ms,
            },
        }),
        Some(common_v1::run_stream_event::Body::JournalEvent(journal_event)) => json!({
            "run_id": run_id,
            "event_type": "journal_event",
            "journal_event": {
                "event_id": journal_event.event_id.as_ref().map(|value| value.ulid.clone()),
                "session_id": "<redacted>",
                "run_id": journal_event.run_id.as_ref().map(|value| value.ulid.clone()),
                "kind": journal_event_kind_label(journal_event.kind),
                "actor": journal_event_actor_label(journal_event.actor),
                "timestamp_unix_ms": journal_event.timestamp_unix_ms,
                "payload_json": decode_json_bytes_for_console(journal_event.payload_json.as_slice()),
                "hash": journal_event.hash,
                "prev_hash": journal_event.prev_hash,
            },
        }),
        Some(common_v1::run_stream_event::Body::A2uiUpdate(update)) => json!({
            "run_id": run_id,
            "event_type": "a2ui_update",
            "a2ui_update": {
                "surface": update.surface,
                "patch_json": decode_json_bytes_for_console(update.patch_json.as_slice()),
            },
        }),
        None => json!({
            "run_id": run_id,
            "event_type": "unspecified",
        }),
    }
}

fn stream_status_kind_label(raw: i32) -> &'static str {
    match common_v1::stream_status::StatusKind::try_from(raw)
        .unwrap_or(common_v1::stream_status::StatusKind::Unspecified)
    {
        common_v1::stream_status::StatusKind::Accepted => "accepted",
        common_v1::stream_status::StatusKind::InProgress => "in_progress",
        common_v1::stream_status::StatusKind::Done => "done",
        common_v1::stream_status::StatusKind::Failed => "failed",
        common_v1::stream_status::StatusKind::Unspecified => "unspecified",
    }
}

fn tool_decision_kind_label(raw: i32) -> &'static str {
    match common_v1::tool_decision::DecisionKind::try_from(raw)
        .unwrap_or(common_v1::tool_decision::DecisionKind::Unspecified)
    {
        common_v1::tool_decision::DecisionKind::Allow => "allow",
        common_v1::tool_decision::DecisionKind::Deny => "deny",
        common_v1::tool_decision::DecisionKind::Unspecified => "unspecified",
    }
}

fn approval_scope_label(raw: i32) -> &'static str {
    match common_v1::ApprovalDecisionScope::try_from(raw)
        .unwrap_or(common_v1::ApprovalDecisionScope::Unspecified)
    {
        common_v1::ApprovalDecisionScope::Once => "once",
        common_v1::ApprovalDecisionScope::Session => "session",
        common_v1::ApprovalDecisionScope::Timeboxed => "timeboxed",
        common_v1::ApprovalDecisionScope::Unspecified => "unspecified",
    }
}

fn approval_risk_level_label(raw: i32) -> &'static str {
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

fn journal_event_kind_label(raw: i32) -> &'static str {
    match common_v1::journal_event::EventKind::try_from(raw)
        .unwrap_or(common_v1::journal_event::EventKind::Unspecified)
    {
        common_v1::journal_event::EventKind::MessageReceived => "message_received",
        common_v1::journal_event::EventKind::ModelToken => "model_token",
        common_v1::journal_event::EventKind::ToolProposed => "tool_proposed",
        common_v1::journal_event::EventKind::ToolExecuted => "tool_executed",
        common_v1::journal_event::EventKind::A2uiUpdated => "a2ui_updated",
        common_v1::journal_event::EventKind::RunCompleted => "run_completed",
        common_v1::journal_event::EventKind::RunFailed => "run_failed",
        common_v1::journal_event::EventKind::Unspecified => "unspecified",
    }
}

fn journal_event_actor_label(raw: i32) -> &'static str {
    match common_v1::journal_event::EventActor::try_from(raw)
        .unwrap_or(common_v1::journal_event::EventActor::Unspecified)
    {
        common_v1::journal_event::EventActor::User => "user",
        common_v1::journal_event::EventActor::Agent => "agent",
        common_v1::journal_event::EventActor::System => "system",
        common_v1::journal_event::EventActor::Plugin => "plugin",
        common_v1::journal_event::EventActor::Unspecified => "unspecified",
    }
}

fn decode_json_bytes_for_console(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        return Value::Null;
    }
    if let Ok(parsed) = serde_json::from_slice::<Value>(bytes) {
        return parsed;
    }
    if let Ok(text) = std::str::from_utf8(bytes) {
        return Value::String(text.to_owned());
    }
    json!({
        "base64": base64::engine::general_purpose::STANDARD.encode(bytes),
    })
}

pub(crate) fn lock_console_chat_streams<'a>(
    streams: &'a Arc<Mutex<HashMap<String, ConsoleChatRunStream>>>,
) -> std::sync::MutexGuard<'a, HashMap<String, ConsoleChatRunStream>> {
    match streams.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("console chat stream map lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

fn lock_console_chat_pending_approvals<'a>(
    approvals: &'a Arc<Mutex<HashMap<String, String>>>,
) -> std::sync::MutexGuard<'a, HashMap<String, String>> {
    match approvals.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("console chat approval map lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

async fn build_console_gateway_client(
    state: &AppState,
) -> Result<
    gateway_v1::gateway_service_client::GatewayServiceClient<tonic::transport::Channel>,
    String,
> {
    let endpoint = tonic::transport::Endpoint::from_shared(state.grpc_url.clone())
        .map_err(|error| format!("invalid gateway endpoint '{}': {error}", state.grpc_url))?
        .connect_timeout(std::time::Duration::from_secs(2))
        .timeout(std::time::Duration::from_secs(90));
    let channel = endpoint.connect().await.map_err(|error| {
        format!("failed to connect to gateway endpoint '{}': {error}", state.grpc_url)
    })?;
    Ok(gateway_v1::gateway_service_client::GatewayServiceClient::new(channel))
}

fn build_console_chat_message_envelope(
    session: &ConsoleSession,
    session_id: &str,
    text: String,
    timestamp_unix_ms: i64,
    attachments: Vec<common_v1::MessageAttachment>,
) -> common_v1::MessageEnvelope {
    common_v1::MessageEnvelope {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        envelope_id: Some(common_v1::CanonicalId { ulid: Ulid::new().to_string() }),
        timestamp_unix_ms,
        origin: Some(common_v1::EnvelopeOrigin {
            r#type: common_v1::envelope_origin::OriginType::Web as i32,
            channel: session.context.channel.clone().unwrap_or_else(|| "web".to_owned()),
            conversation_id: session_id.to_owned(),
            sender_display: session.context.principal.clone(),
            sender_handle: session.context.principal.clone(),
            sender_verified: true,
        }),
        content: Some(common_v1::MessageContent { text, attachments }),
        security: None,
        max_payload_bytes: 0,
    }
}

fn console_chat_attachment_payload_to_json(payload: &media::MediaArtifactPayload) -> Value {
    json!({
        "artifact_id": payload.artifact_id,
        "attachment_id": payload.artifact_id,
        "filename": payload.filename,
        "declared_content_type": payload.content_type,
        "content_hash": payload.sha256,
        "size_bytes": payload.size_bytes,
        "width_px": payload.width_px,
        "height_px": payload.height_px,
        "kind": console_chat_attachment_kind_label(payload.content_type.as_str()),
        "budget_tokens": estimate_console_chat_attachment_tokens(payload),
    })
}

fn console_chat_attachment_kind(
    content_type: &str,
) -> common_v1::message_attachment::AttachmentKind {
    if content_type.starts_with("image/") {
        common_v1::message_attachment::AttachmentKind::Image
    } else if content_type.starts_with("audio/") {
        common_v1::message_attachment::AttachmentKind::Audio
    } else if content_type.starts_with("video/") {
        common_v1::message_attachment::AttachmentKind::Video
    } else {
        common_v1::message_attachment::AttachmentKind::File
    }
}

fn console_chat_attachment_kind_label(content_type: &str) -> &'static str {
    match console_chat_attachment_kind(content_type) {
        common_v1::message_attachment::AttachmentKind::Image => "image",
        common_v1::message_attachment::AttachmentKind::Audio => "audio",
        common_v1::message_attachment::AttachmentKind::Video => "video",
        common_v1::message_attachment::AttachmentKind::File
        | common_v1::message_attachment::AttachmentKind::Unspecified => "file",
    }
}

fn estimate_console_chat_attachment_tokens(payload: &media::MediaArtifactPayload) -> u64 {
    if payload.content_type.starts_with("image/") {
        850
    } else {
        payload.size_bytes / 4
    }
}

pub(crate) async fn sync_console_chat_approval_to_stream(
    state: &AppState,
    record: &journal::ApprovalRecord,
) -> bool {
    let approved = match record.decision {
        Some(ApprovalDecision::Allow) => true,
        Some(ApprovalDecision::Deny) => false,
        _ => return false,
    };

    let stream = {
        let streams = lock_console_chat_streams(&state.console_chat_streams);
        streams.get(record.run_id.as_str()).cloned()
    };
    let Some(stream) = stream else {
        return false;
    };
    if stream.session_id != record.session_id {
        return false;
    }

    let proposal_id = {
        let mut pending = lock_console_chat_pending_approvals(&stream.pending_approvals);
        pending.remove(record.approval_id.as_str())
    };
    let Some(proposal_id) = proposal_id else {
        return false;
    };

    let reason = record.decision_reason.clone().unwrap_or_else(|| {
        if approved {
            "approved_by_console".to_owned()
        } else {
            "denied_by_console".to_owned()
        }
    });
    let response = common_v1::ToolApprovalResponse {
        proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id }),
        approved,
        reason,
        approval_id: Some(common_v1::CanonicalId { ulid: record.approval_id.clone() }),
        decision_scope: approval_scope_to_proto(record.decision_scope),
        decision_scope_ttl_ms: record.decision_scope_ttl_ms.unwrap_or_default(),
    };
    let request = common_v1::RunStreamRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: record.session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: record.run_id.clone() }),
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
    };
    if stream.request_sender.send(request).await.is_err() {
        tracing::warn!(
            run_id = %record.run_id,
            approval_id = %record.approval_id,
            "failed to forward console approval decision to active chat stream"
        );
        return false;
    }
    true
}

fn approval_scope_to_proto(scope: Option<ApprovalDecisionScope>) -> i32 {
    match scope.unwrap_or(ApprovalDecisionScope::Once) {
        ApprovalDecisionScope::Once => common_v1::ApprovalDecisionScope::Once as i32,
        ApprovalDecisionScope::Session => common_v1::ApprovalDecisionScope::Session as i32,
        ApprovalDecisionScope::Timeboxed => common_v1::ApprovalDecisionScope::Timeboxed as i32,
    }
}
