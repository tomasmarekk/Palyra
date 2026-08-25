//! Console chat HTTP handlers for the `/console/v1/chat/*` route surface.
//!
//! Implements the web console's chat workflows: session lifecycle
//! (list/resolve/rename/reset/branch), message submission with NDJSON
//! streaming, attachments and derived artifacts, run status/tape/workspace
//! inspection, retry/checkpoint/compaction, background tasks, the session
//! input queue, canvases, transcript search/export, and pins.
//!
//! Message streaming bridges HTTP to the gateway gRPC `run_stream` RPC: the
//! handler opens a client-side request stream, registers it in
//! `AppState::console_chat_streams` keyed by run id, and relays
//! `RunStreamEvent`s back to the browser as newline-delimited JSON. Follow-up
//! queue submissions and approval decisions are injected into the same gRPC
//! stream while the run is active.
//!
//! Response JSON shapes are a wire contract consumed by `apps/web`
//! (`consoleApi*`); field names, status codes, and error strings must stay
//! stable.

use crate::{
    application::session_compaction::{
        apply_session_compaction, preview_session_compaction, SessionCompactionApplyRequest,
    },
    application::session_queue::{
        analyze_session_queue, build_queue_collect_summary, decide_session_queue_mode,
        pending_queue_depth, queue_outcome, QueueSteeringRequest, SessionQueuePolicy,
        SessionQueueSafeBoundary,
    },
    *,
};
use async_trait::async_trait;
use base64::Engine as _;
use palyra_common::{
    runtime_contracts::{
        AuxiliaryTaskKind, AuxiliaryTaskState, QueueDecision, QueueMode,
        QueuedInputDeliveryBoundary, QueuedInputState,
    },
    runtime_preview::{
        RuntimeDecisionActorKind, RuntimeDecisionEventType, RuntimeDecisionPayload,
        RuntimeDecisionTiming, RuntimeEntityRef, RuntimePreviewCapability, RuntimeResourceBudget,
    },
};
use serde::Serialize;

// Placeholder body for workspace/memory index documents derived from
// attachments: the extracted text itself stays in the device-scoped derived
// artifact store so cross-device surfaces never receive attachment content.
const ATTACHMENT_DERIVED_INDEX_OMITTED_MESSAGE: &str =
    "attachment-derived content omitted; use device-scoped derived artifact endpoints";
// Background budgets meter the compiled instructions and tool schemas on every
// provider turn, not only the operator's task text. Reserve enough for several
// ordinary tool-loop turns while keeping explicit operator budgets authoritative.
const DEFAULT_CONSOLE_BACKGROUND_TASK_BUDGET_TOKENS: u64 = 65_536;

/// Transcript provenance for a canvas: the latest tape event that referenced
/// the canvas frame URL, if any.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
struct ConsoleChatCanvasTranscriptReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    source_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_tape_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_event_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    origin_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    origin_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_referenced_at_unix_ms: Option<i64>,
}

/// Wire-facing canvas summary returned by the canvas list/detail handlers.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ConsoleChatCanvasSummary {
    canvas_id: String,
    session_id: String,
    state_version: u64,
    state_schema_version: u64,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    expires_at_unix_ms: i64,
    closed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    close_reason: Option<String>,
    runtime_status: String,
    reference: ConsoleChatCanvasTranscriptReference,
}

// --- Session lifecycle handlers ---

/// `GET /console/v1/chat/sessions` - lists chat sessions visible to the
/// authenticated console context, with cursor pagination.
///
/// # Errors
/// Returns an error `Response` when console authorization fails or the
/// runtime listing fails.
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

/// `POST /console/v1/chat/sessions` - resolves (and optionally creates or
/// resets) a chat session by id, key, or label.
///
/// # Errors
/// Returns an error `Response` when authorization fails, `session_id` is not
/// a canonical ULID, or session resolution fails (for example
/// `require_existing` on a missing session).
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

/// `POST /console/v1/chat/sessions/{session_id}/rename` - updates the session
/// label/title and the manual-title lock, then journals the rename.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the id is malformed,
/// the session is not visible to this context, or an empty label is combined
/// with `manual_title_locked = true`.
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
    let existing_session =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let session_label = payload.session_label.and_then(trim_to_option);
    let manual_title_locked = payload.manual_title_locked.unwrap_or(session_label.is_some());
    if manual_title_locked && session_label.is_none() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "session_label cannot be empty when manual_title_locked is true",
        )));
    }
    let updated_session = state
        .runtime
        .update_orchestrator_session_title(journal::OrchestratorSessionTitleUpdateRequest {
            session_id: session_id.clone(),
            session_label: session_label.clone(),
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            manual_title_locked,
        })
        .await
        .map_err(runtime_status_response)?;
    let _ = crate::gateway::record_agent_journal_event(
        &state.runtime,
        &session.context,
        json!({
            "event": "session.title.updated",
            "session_id": session_id,
            "previous_session_label": existing_session.session_label,
            "session_label": session_label,
            "previous_title": existing_session.title,
            "title": updated_session.title,
            "manual_title_locked": updated_session.manual_title_locked,
            "title_generation_state": updated_session.title_generation_state,
        }),
    )
    .await;
    Ok(Json(json!({
        "session": updated_session,
        "created": false,
        "reset_applied": false,
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/reset` - resets an existing
/// session's conversational state in place.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the id is malformed,
/// or the session does not exist for this context.
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
    let _cancelled_media_jobs =
        state.audio_sessions.cancel_session(outcome.session.session_id.as_str());
    Ok(Json(json!({
        "session": outcome.session,
        "created": outcome.created,
        "reset_applied": outcome.reset_applied,
    })))
}

// --- Message submission and run-stream bridging ---

/// `POST /console/v1/chat/sessions/{session_id}/messages/stream` - submits a
/// user message and streams the run back as NDJSON
/// (`application/x-ndjson`) lines of `{"type": "meta"|"event"|"error"|"complete", ...}`.
///
/// The handler enriches the message with attachment recall, project context,
/// and `@`-reference parameter deltas, then proxies the gateway `run_stream`
/// RPC. The run is registered in `console_chat_streams` so the queue and
/// approval endpoints can inject follow-up requests while it is active.
///
/// # Errors
/// Returns an error `Response` (before streaming starts) when authorization
/// fails, ids or text are invalid, attachments cannot be resolved, or
/// parameter-delta enrichment fails. Failures after the response starts are
/// reported in-band as `error`/`complete` NDJSON lines.
pub(crate) async fn console_chat_message_stream_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatMessageRequest>,
) -> Result<Response, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let audio_output_request = payload.audio_output.clone();
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
    let parameter_delta = build_console_attachment_parameter_delta(
        &state,
        payload.parameter_delta.as_ref(),
        text.as_str(),
        attachments.as_slice(),
    )
    .map_err(|response| *response)?;
    let parameter_delta = build_console_project_context_parameter_delta(
        &state,
        &session.context,
        session_id.as_str(),
        text.as_str(),
        parameter_delta,
    )
    .await
    .map_err(runtime_status_response)?;
    let parameter_delta = build_console_context_reference_parameter_delta(
        &state,
        &session.context,
        session_id.as_str(),
        text.as_str(),
        parameter_delta,
    )
    .await
    .map_err(runtime_status_response)?;
    let timestamp_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let run_id = Ulid::generate().to_string();
    let audio_rollout_enabled = state.runtime.config.feature_rollouts.audio_pipeline.enabled;
    let audio_destination_scope_sha256 = crate::sha256_hex(
        format!(
            "{}\n{}\n{}",
            session.context.principal,
            session.context.device_id,
            session.context.channel.as_deref().unwrap_or_default()
        )
        .as_bytes(),
    );

    // Register the run before sending anything so queue/approval endpoints can
    // find it as soon as the client learns the run id from the "meta" line.
    // Every exit path below must remove this entry again.
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
        parameter_delta_json: parameter_delta
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

    // The gRPC relay runs in a detached task: it owns the gateway stream and
    // pushes encoded NDJSON lines into a bounded channel that backs the HTTP
    // response body. A `false` return from send_console_chat_line means the
    // client disconnected (receiver dropped), which ends the relay.
    let (line_sender, line_receiver) = mpsc::channel::<Result<Bytes, Infallible>>(32);
    let run_id_for_task = run_id.clone();
    let session_id_for_task = session_id.clone();
    let state_for_task = state.clone();
    tokio::spawn(async move {
        let mut final_status = "unknown".to_owned();
        let mut delivered_final_text = String::new();
        let mut delivered_final_text_complete = false;
        let mut delivered_final_text_overflowed = false;
        let mut relay_delivery_intact = true;
        if !send_console_chat_line(
            &line_sender,
            json!({
                "type": "meta",
                "run_id": run_id_for_task,
                "session_id": session_id_for_task,
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
                        "run_id": run_id_for_task,
                        "error": error,
                    }),
                )
                .await;
                let _ = send_console_chat_line(
                    &line_sender,
                    json!({
                        "type": "complete",
                        "run_id": run_id_for_task,
                        "status": final_status,
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
                        "run_id": run_id_for_task,
                        "error": sanitize_http_error_message(error.message()),
                    }),
                )
                .await;
                let _ = send_console_chat_line(
                    &line_sender,
                    json!({
                        "type": "complete",
                        "run_id": run_id_for_task,
                        "status": final_status,
                    }),
                )
                .await;
                let mut streams = lock_console_chat_streams(&state_for_task.console_chat_streams);
                streams.remove(run_id_for_task.as_str());
                return;
            }
        };

        // The stream task creates its generation after this HTTP task starts, so resolve it
        // lazily instead of permanently losing public projection to the startup race.
        let mut public_event_generation = None;
        let mut public_event_sequence = 0_u64;
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    if public_event_generation.is_none() {
                        public_event_generation = state_for_task
                            .runtime
                            .persisted_runtime_generation_for_run(run_id_for_task.clone())
                            .await
                            .ok()
                            .flatten()
                            .and_then(|(generation_session_id, generation)| {
                                (generation_session_id == session_id_for_task).then_some(generation)
                            });
                    }
                    // Track approval_id -> proposal_id so a later console
                    // approval decision can be translated back into the
                    // ToolApprovalResponse this gRPC stream expects.
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
                    public_event_sequence = public_event_sequence.saturating_add(1);
                    let public_event_id =
                        crate::application::run_stream::public_events::run_stream_public_event_id(
                            run_id_for_task.as_str(),
                            public_event_sequence,
                        );
                    let public_event = public_event_generation.and_then(|generation| {
                        crate::application::run_stream::public_events::
                            public_runtime_event_json_from_run_stream_event(
                            &event,
                            crate::application::run_stream::public_events::PublicRunStreamEventContext {
                                event_id: public_event_id.as_str(),
                                session_id: session_id_for_task.as_str(),
                                generation,
                                sequence: public_event_sequence,
                                occurred_at_unix_ms: unix_ms_now().unwrap_or_default(),
                                causal_parent_event_id: None,
                                request_id: None,
                            },
                        )
                    });
                    let mut event_json = console_run_stream_event_to_json(&event);
                    if let (Some(object), Some(public_event)) =
                        (event_json.as_object_mut(), public_event)
                    {
                        object
                            .insert("public_event_type".to_owned(), public_event["event"].clone());
                        object.insert("public_event".to_owned(), public_event);
                    }
                    if !send_console_chat_line(
                        &line_sender,
                        json!({
                            "type": "event",
                            "event": event_json,
                        }),
                    )
                    .await
                    {
                        relay_delivery_intact = false;
                        break;
                    }
                    observe_delivered_model_text(
                        &event,
                        &mut delivered_final_text,
                        &mut delivered_final_text_complete,
                        &mut delivered_final_text_overflowed,
                    );
                }
                Err(error) => {
                    final_status = "failed".to_owned();
                    let _ = send_console_chat_line(
                        &line_sender,
                        json!({
                            "type": "error",
                            "run_id": run_id_for_task,
                            "error": sanitize_http_error_message(error.message()),
                        }),
                    )
                    .await;
                    break;
                }
            }
        }

        if let Some(audio_output) = execute_console_post_delivery_audio(
            &state_for_task,
            ConsolePostDeliveryAudioContext {
                session_id: session_id_for_task.as_str(),
                run_id: run_id_for_task.as_str(),
                destination_scope_sha256: audio_destination_scope_sha256.as_str(),
                rollout_enabled: audio_rollout_enabled,
                request: audio_output_request,
                text_delivery_settled: final_status == "done"
                    && relay_delivery_intact
                    && delivered_final_text_complete
                    && !delivered_final_text_overflowed,
                final_text: delivered_final_text,
            },
        )
        .await
        {
            let _ = send_console_chat_line(
                &line_sender,
                json!({
                    "type": "audio_output",
                    "run_id": run_id_for_task,
                    "audio_output": audio_output,
                }),
            )
            .await;
        }
        let _ = send_console_chat_line(
            &line_sender,
            json!({
                "type": "complete",
                "run_id": run_id_for_task,
                "status": final_status,
            }),
        )
        .await;
        let mut streams = lock_console_chat_streams(&state_for_task.console_chat_streams);
        streams.remove(run_id_for_task.as_str());
    });

    // no-store keeps intermediaries from buffering or replaying the
    // incremental NDJSON body.
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(line_receiver)));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson; charset=utf-8"));
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

// --- Prompt enrichment previews and delegation catalog ---

/// `POST /console/v1/chat/sessions/{session_id}/references/preview` - resolves
/// `@`-style context references in a draft message without sending it.
///
/// # Errors
/// Returns an error `Response` when authorization fails, ids or text are
/// invalid, or reference resolution fails.
pub(crate) async fn console_chat_context_reference_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatContextReferencePreviewRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let text = trim_to_option(payload.text).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("text cannot be empty"))
    })?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), false).await?;
    let preview = crate::application::context_references::preview_context_references(
        &state.runtime,
        &session.context,
        session_record.session_id.as_str(),
        text.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "clean_prompt": preview.clean_prompt,
        "references": preview.references,
        "total_estimated_tokens": preview.total_estimated_tokens,
        "warnings": preview.warnings,
        "errors": preview.errors,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/project-context/preview` -
/// previews the project context entries and rendered prompt for a draft
/// message.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the id is malformed,
/// or the preview computation fails.
pub(crate) async fn console_chat_project_context_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatProjectContextPreviewRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let preview = crate::application::project_context::preview_project_context(
        &state.runtime,
        &session.context,
        session_id.as_str(),
        payload.text.trim(),
        false,
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "preview": preview.clone(),
        "prompt_preview": crate::application::project_context::render_project_context_prompt(
            &preview,
            "{{user_prompt}}",
        ),
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/delegation/catalog` - returns the built-in delegation
/// profile catalog.
///
/// # Errors
/// Returns an error `Response` when console authorization fails.
pub(crate) async fn console_chat_delegation_catalog_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(json!({
        "catalog": crate::delegation::built_in_delegation_catalog(),
        "contract": contract_descriptor(),
    })))
}

// --- Attachments and derived artifacts ---

/// `POST /console/v1/chat/sessions/{session_id}/attachments` - stores a
/// base64-encoded attachment and synchronously derives its artifacts
/// (metadata summary, extracted text, transcript) under a tracked background
/// task record.
///
/// # Errors
/// Returns an error `Response` when authorization fails, inputs are empty or
/// not valid base64, the attachment store rejects the payload (size/type
/// caps), or derivation bookkeeping fails.
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
    let task = state
        .runtime
        .create_orchestrator_background_task(journal::OrchestratorBackgroundTaskCreateRequest {
            task_id: Ulid::generate().to_string(),
            task_kind: AuxiliaryTaskKind::AttachmentDerivation.as_str().to_owned(),
            session_id: session_id.clone(),
            child_session_id: None,
            parent_run_id: None,
            target_run_id: None,
            planned_child_run_id: None,
            queued_input_id: None,
            owner_principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: 50,
            max_attempts: 1,
            budget_tokens: estimate_console_chat_attachment_tokens(&artifact),
            delegation: None,
            cancellation_context: None,
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some(filename.clone()),
            payload_json: Some(
                json!({
                    "source_artifact_id": artifact.artifact_id,
                    "content_type": artifact.content_type,
                    "filename": artifact.filename,
                })
                .to_string(),
            ),
        })
        .await
        .map_err(runtime_status_response)?;
    let task_started_at = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let claimed_task = state
        .runtime
        .claim_orchestrator_background_task(journal::OrchestratorBackgroundTaskClaimRequest {
            task_id: task.task_id.clone(),
            expected_revision: task.revision,
            started_at_unix_ms: task_started_at,
        })
        .await
        .map_err(runtime_status_response)?;
    let derived_artifacts = match derive_console_attachment_artifacts(
        &state,
        &session,
        session_id.as_str(),
        &artifact,
        task.task_id.as_str(),
    )
    .await
    {
        Ok(records) => {
            let completed_at_unix_ms = unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?;
            state
                .runtime
                .update_orchestrator_background_task_from_worker(
                    journal::OrchestratorBackgroundTaskWorkerUpdateRequest {
                        task_id: claimed_task.task_id.clone(),
                        execution_generation: claimed_task.execution_generation,
                        state: Some(AuxiliaryTaskState::Succeeded.as_str().to_owned()),
                        target_run_id: None,
                        last_error: Some(None),
                        result_json: Some(Some(
                            json!({
                                "derived_count": records.len(),
                                "artifact_id": artifact.artifact_id,
                            })
                            .to_string(),
                        )),
                        started_at_unix_ms: None,
                        completed_at_unix_ms: Some(Some(completed_at_unix_ms)),
                    },
                )
                .await
                .map_err(runtime_status_response)?;
            records
        }
        Err(error) => {
            let completed_at_unix_ms = unix_ms_now().map_err(|clock_error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {clock_error}"
                )))
            })?;
            state
                .runtime
                .update_orchestrator_background_task_from_worker(
                    journal::OrchestratorBackgroundTaskWorkerUpdateRequest {
                        task_id: claimed_task.task_id.clone(),
                        execution_generation: claimed_task.execution_generation,
                        state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
                        target_run_id: None,
                        last_error: Some(Some(error.to_string())),
                        result_json: None,
                        started_at_unix_ms: None,
                        completed_at_unix_ms: Some(Some(completed_at_unix_ms)),
                    },
                )
                .await
                .map_err(runtime_status_response)?;
            return Err(runtime_status_response(tonic::Status::internal(error.to_string())));
        }
    };
    Ok(Json(json!({
        "attachment": console_chat_attachment_payload_to_json(&artifact),
        "derived_artifacts": derived_artifacts,
        "task": claimed_task,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/sessions/{session_id}/derived-artifacts` - lists the
/// session's derived artifacts, optionally filtered by `kind` and `state`.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the session/store
/// lookup fails.
pub(crate) async fn console_chat_derived_artifacts_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleChatDerivedArtifactsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), false).await?;
    let kind_filter = query.kind.and_then(trim_to_option).map(|value| value.to_ascii_lowercase());
    let state_filter = query.state.and_then(trim_to_option).map(|value| value.to_ascii_lowercase());
    let derived_artifacts = state
        .channels
        .list_console_chat_derived_artifacts(
            session_record.session_id.as_str(),
            session.context.principal.as_str(),
            session.context.device_id.as_str(),
            session.context.channel.as_deref(),
        )
        .map_err(channel_platform_error_response)?
        .into_iter()
        .filter(|record| {
            kind_filter
                .as_deref()
                .map(|expected| record.kind.eq_ignore_ascii_case(expected))
                .unwrap_or(true)
        })
        .filter(|record| {
            state_filter
                .as_deref()
                .map(|expected| record.state.eq_ignore_ascii_case(expected))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "session": session_record,
        "derived_artifacts": derived_artifacts,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/attachments/{artifact_id}/derived-artifacts` - lists
/// derived artifacts for one source attachment, enforcing the device
/// boundary.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the id is malformed,
/// or no derived artifacts are visible to this context (`not_found`).
pub(crate) async fn console_chat_attachment_derived_artifacts_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(artifact_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(artifact_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "artifact_id must be a canonical ULID",
        ))
    })?;
    let derived_artifacts = filter_console_derived_artifact_records(
        state
            .channels
            .list_attachment_derived_artifacts(artifact_id.as_str())
            .map_err(channel_platform_error_response)?,
        &session.context,
        true,
    );
    if derived_artifacts.is_empty() {
        return Err(runtime_status_response(tonic::Status::not_found(
            "attachment derived artifacts not found for current console context",
        )));
    }
    Ok(Json(json!({
        "source_artifact_id": artifact_id,
        "derived_artifacts": derived_artifacts,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/derived-artifacts/{derived_artifact_id}` - returns a
/// single derived artifact, including extracted content, for the owning
/// device.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the id is malformed,
/// or the artifact is not visible to this context (`not_found`).
pub(crate) async fn console_chat_derived_artifact_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(derived_artifact_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(derived_artifact_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "derived_artifact_id must be a canonical ULID",
        ))
    })?;
    let derived_artifact =
        load_console_derived_artifact(&state, &session.context, derived_artifact_id.as_str(), true)
            .map_err(|response| *response)?;
    Ok(Json(json!({
        "derived_artifact": derived_artifact,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/derived-artifacts/{derived_artifact_id}/quarantine`
/// - marks a derived artifact quarantined with an optional reason.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the artifact is
/// not visible to this context.
pub(crate) async fn console_chat_derived_artifact_quarantine_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(derived_artifact_id): Path<String>,
    Json(payload): Json<ConsoleDerivedArtifactLifecycleRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let _existing =
        load_console_derived_artifact(&state, &session.context, derived_artifact_id.as_str(), true)
            .map_err(|response| *response)?;
    let reason = payload.reason.and_then(trim_to_option);
    let derived_artifact = state
        .channels
        .quarantine_derived_artifact(derived_artifact_id.as_str(), reason.as_deref())
        .map_err(channel_platform_error_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "derived artifact not found: {derived_artifact_id}"
            )))
        })?;
    Ok(Json(json!({
        "derived_artifact": derived_artifact,
        "action": "quarantine",
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/derived-artifacts/{derived_artifact_id}/release` -
/// releases a quarantined derived artifact back into normal use.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the artifact is
/// not visible to this context.
pub(crate) async fn console_chat_derived_artifact_release_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(derived_artifact_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let _existing =
        load_console_derived_artifact(&state, &session.context, derived_artifact_id.as_str(), true)
            .map_err(|response| *response)?;
    let derived_artifact = state
        .channels
        .release_derived_artifact(derived_artifact_id.as_str())
        .map_err(channel_platform_error_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "derived artifact not found: {derived_artifact_id}"
            )))
        })?;
    Ok(Json(json!({
        "derived_artifact": derived_artifact,
        "action": "release",
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/derived-artifacts/{derived_artifact_id}/recompute` -
/// re-runs derivation from the source attachment under a new background task.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the artifact has no
/// session or source attachment, or recompute/derivation fails (the
/// recompute-required flag stays set so the artifact can be retried).
pub(crate) async fn console_chat_derived_artifact_recompute_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(derived_artifact_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let existing =
        load_console_derived_artifact(&state, &session.context, derived_artifact_id.as_str(), true)
            .map_err(|response| *response)?;
    let session_id = existing.session_id.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "derived artifact is not attached to a chat session",
        ))
    })?;
    state
        .channels
        .mark_derived_artifact_recompute_required(derived_artifact_id.as_str(), true)
        .map_err(channel_platform_error_response)?;
    let source_attachment = state
        .channels
        .load_console_chat_attachment(
            existing.source_artifact_id.as_str(),
            session_id.as_str(),
            session.context.principal.as_str(),
            session.context.device_id.as_str(),
            session.context.channel.as_deref(),
        )
        .map_err(channel_platform_error_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "source attachment not found for derived artifact: {}",
                existing.source_artifact_id
            )))
        })?;
    let task = state
        .runtime
        .create_orchestrator_background_task(journal::OrchestratorBackgroundTaskCreateRequest {
            task_id: Ulid::generate().to_string(),
            task_kind: AuxiliaryTaskKind::AttachmentRecompute.as_str().to_owned(),
            session_id: session_id.clone(),
            child_session_id: None,
            parent_run_id: None,
            target_run_id: None,
            planned_child_run_id: None,
            queued_input_id: None,
            owner_principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: 40,
            max_attempts: 1,
            budget_tokens: estimate_console_chat_attachment_tokens(&source_attachment),
            delegation: None,
            cancellation_context: None,
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some(existing.filename.clone()),
            payload_json: Some(
                json!({
                    "source_artifact_id": existing.source_artifact_id,
                    "derived_artifact_id": existing.derived_artifact_id,
                    "kind": existing.kind,
                })
                .to_string(),
            ),
        })
        .await
        .map_err(runtime_status_response)?;
    let started_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let claimed_task = state
        .runtime
        .claim_orchestrator_background_task(journal::OrchestratorBackgroundTaskClaimRequest {
            task_id: task.task_id.clone(),
            expected_revision: task.revision,
            started_at_unix_ms,
        })
        .await
        .map_err(runtime_status_response)?;
    let derived_artifacts = match derive_console_attachment_artifacts(
        &state,
        &session,
        session_id.as_str(),
        &source_attachment,
        task.task_id.as_str(),
    )
    .await
    {
        Ok(records) => {
            let completed_at_unix_ms = unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?;
            state
                .runtime
                .update_orchestrator_background_task_from_worker(
                    journal::OrchestratorBackgroundTaskWorkerUpdateRequest {
                        task_id: claimed_task.task_id.clone(),
                        execution_generation: claimed_task.execution_generation,
                        state: Some(AuxiliaryTaskState::Succeeded.as_str().to_owned()),
                        target_run_id: None,
                        last_error: Some(None),
                        result_json: Some(Some(
                            json!({
                                "source_artifact_id": existing.source_artifact_id,
                                "derived_count": records.len(),
                            })
                            .to_string(),
                        )),
                        started_at_unix_ms: None,
                        completed_at_unix_ms: Some(Some(completed_at_unix_ms)),
                    },
                )
                .await
                .map_err(runtime_status_response)?;
            records
        }
        Err(error) => {
            let completed_at_unix_ms = unix_ms_now().map_err(|clock_error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {clock_error}"
                )))
            })?;
            state
                .runtime
                .update_orchestrator_background_task_from_worker(
                    journal::OrchestratorBackgroundTaskWorkerUpdateRequest {
                        task_id: claimed_task.task_id.clone(),
                        execution_generation: claimed_task.execution_generation,
                        state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
                        target_run_id: None,
                        last_error: Some(Some(error.to_string())),
                        result_json: None,
                        started_at_unix_ms: None,
                        completed_at_unix_ms: Some(Some(completed_at_unix_ms)),
                    },
                )
                .await
                .map_err(runtime_status_response)?;
            state
                .channels
                .mark_derived_artifact_recompute_required(derived_artifact_id.as_str(), true)
                .map_err(channel_platform_error_response)?;
            return Err(runtime_status_response(tonic::Status::internal(error.to_string())));
        }
    };
    let derived_artifact =
        load_console_derived_artifact(&state, &session.context, derived_artifact_id.as_str(), true)
            .map_err(|response| *response)?;
    Ok(Json(json!({
        "task": claimed_task,
        "derived_artifact": derived_artifact,
        "derived_artifacts": derived_artifacts,
        "action": "recompute",
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/derived-artifacts/{derived_artifact_id}/purge` -
/// purges a derived artifact and best-effort deletes its linked memory item
/// and workspace index document.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the purge itself
/// fails; linked memory/workspace cleanup failures are intentionally ignored.
pub(crate) async fn console_chat_derived_artifact_purge_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(derived_artifact_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let existing =
        load_console_derived_artifact(&state, &session.context, derived_artifact_id.as_str(), true)
            .map_err(|response| *response)?;
    if let Some(memory_item_id) = existing.memory_item_id.as_deref() {
        let _ = state
            .runtime
            .delete_memory_item(
                memory_item_id.to_owned(),
                session.context.principal.clone(),
                session.context.channel.clone(),
            )
            .await;
    }
    if let Some(session_id) = existing.session_id.as_deref() {
        let _ = state
            .runtime
            .soft_delete_workspace_document(journal::WorkspaceDocumentDeleteRequest {
                principal: session.context.principal.clone(),
                channel: session.context.channel.clone(),
                agent_id: None,
                session_id: Some(session_id.to_owned()),
                path: console_attachment_workspace_path(
                    session_id,
                    existing.source_artifact_id.as_str(),
                    existing.kind.as_str(),
                ),
            })
            .await;
    }
    let derived_artifact = state
        .channels
        .purge_derived_artifact(derived_artifact_id.as_str())
        .map_err(channel_platform_error_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "derived artifact not found: {derived_artifact_id}"
            )))
        })?;
    Ok(Json(json!({
        "derived_artifact": derived_artifact,
        "action": "purge",
        "contract": contract_descriptor(),
    })))
}

// --- Run inspection (status, tape events, workspace) ---

/// `GET /console/v1/chat/runs/{run_id}/status` - returns a run snapshot and
/// lineage, optionally long-polling (`wait=true`) until the run reaches a
/// terminal or waiting state.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the id is malformed,
/// the run is unknown, or it belongs to a different console context.
pub(crate) async fn console_chat_run_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<ConsoleChatRunStatusQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    let mut run = state
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
    let wait_result = if query.wait {
        let timeout_ms = query.timeout_ms.unwrap_or(30_000).clamp(25, 120_000);
        let outcome = state
            .runtime
            .wait_for_orchestrator_run(crate::gateway::OrchestratorRunWaitRequest {
                run_id: run_id.clone(),
                timeout: std::time::Duration::from_millis(timeout_ms),
                poll_interval: std::time::Duration::from_millis(250),
                return_on_waiting: query.return_on_waiting,
            })
            .await
            .map_err(runtime_status_response)?;
        run = outcome.snapshot;
        Some(json!({
            "waited": true,
            "timeout_ms": timeout_ms,
            "canonical_state": outcome.canonical_state.as_str(),
        }))
    } else {
        None
    };
    let lineage = load_console_run_lineage(&state, &session.context, &run).await?;
    Ok(Json(json!({
        "run": run,
        "lineage": lineage,
        "run_wait": wait_result,
    })))
}

/// Query parameters for [`console_chat_run_status_handler`].
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub(crate) struct ConsoleChatRunStatusQuery {
    #[serde(default)]
    wait: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    return_on_waiting: bool,
}

/// `GET /console/v1/chat/runs/{run_id}/events` - returns a paged tape
/// snapshot (`after_seq`/`limit`) plus the run and its lineage.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the id is malformed,
/// the run is unknown, or it belongs to a different console context.
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
    let lineage = load_console_run_lineage(&state, &session.context, &run).await?;
    Ok(Json(json!({
        "run": run,
        "tape": tape,
        "lineage": lineage,
    })))
}

/// `GET /console/v1/chat/runs/{run_id}/workspace` - lists workspace artifacts
/// produced by a run, with optional substring query and limit.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the run is unknown
/// or foreign, or the workspace listing fails.
pub(crate) async fn console_chat_run_workspace_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<ConsoleChatRunWorkspaceQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let run = load_console_chat_run(&state, &session.context, run_id.as_str()).await?;
    let workspace = crate::application::workspace_observability::load_run_workspace_artifacts(
        &state.runtime,
        &run,
        crate::application::workspace_observability::WorkspaceArtifactListQuery {
            query: query.q.as_deref().map(str::trim).filter(|value| !value.is_empty()),
            limit: query.limit.unwrap_or(128),
        },
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "run": run,
        "workspace": workspace,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/runs/{run_id}/workspace/artifacts/{artifact_id}` -
/// returns one workspace artifact, optionally including its content.
///
/// # Errors
/// Returns an error `Response` when authorization fails, ids are malformed,
/// or the run/artifact lookup fails.
pub(crate) async fn console_chat_run_workspace_artifact_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((run_id, artifact_id)): Path<(String, String)>,
    Query(query): Query<ConsoleChatWorkspaceArtifactQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(artifact_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "artifact_id must be a canonical ULID",
        ))
    })?;
    let run = load_console_chat_run(&state, &session.context, run_id.as_str()).await?;
    let detail = crate::application::workspace_observability::load_workspace_artifact_detail(
        &state.runtime,
        &run,
        artifact_id.as_str(),
        query.include_content.unwrap_or(false),
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "run": run,
        "detail": detail,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/workspace/compare` - diffs two workspace anchors
/// (run or checkpoint per side), authorizing each anchor independently.
///
/// # Errors
/// Returns an error `Response` when authorization fails, an anchor is
/// ambiguous or missing, or the diff computation fails.
pub(crate) async fn console_chat_workspace_compare_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleChatWorkspaceCompareRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let left =
        parse_workspace_compare_anchor(payload.left_run_id, payload.left_checkpoint_id, "left")
            .map_err(runtime_status_response)?;
    let right =
        parse_workspace_compare_anchor(payload.right_run_id, payload.right_checkpoint_id, "right")
            .map_err(runtime_status_response)?;
    authorize_workspace_compare_anchor(&state, &session.context, &left).await?;
    authorize_workspace_compare_anchor(&state, &session.context, &right).await?;
    let diff = crate::application::workspace_observability::compare_workspace_anchors(
        &state.runtime,
        left,
        right,
        payload.limit.unwrap_or(64),
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "diff": diff,
        "contract": contract_descriptor(),
    })))
}

// --- Retry, branch, compaction, and checkpoints ---

/// `POST /console/v1/chat/sessions/{session_id}/retry` - prepares (without
/// starting) a retry of the latest terminal run: returns the original user
/// text plus the parameter delta to resubmit through the stream endpoint.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the session has no
/// terminal latest run, no persisted user turn exists, or the stored
/// parameter delta cannot be parsed.
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
    let parameter_delta = retry_parameter_delta_from_payload_or_run(payload.parameter_delta, &run)
        .map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to prepare retry parameter_delta for run {last_run_id}: {error}"
            )))
        })?;
    Ok(Json(json!({
        "session": base_session,
        "text": text,
        "origin_kind": "retry",
        "origin_run_id": last_run_id,
        "parameter_delta": parameter_delta,
        "contract": contract_descriptor(),
    })))
}

/// Prefers an explicit non-null payload delta; otherwise falls back to the
/// parameter delta persisted on the run being retried, so retries keep the
/// original CLI/workspace context.
fn retry_parameter_delta_from_payload_or_run(
    payload_parameter_delta: Option<Value>,
    run: &journal::OrchestratorRunStatusSnapshot,
) -> Result<Option<Value>, serde_json::Error> {
    if payload_parameter_delta.as_ref().is_some_and(|value| !value.is_null()) {
        return Ok(payload_parameter_delta);
    }
    let Some(raw_parameter_delta) =
        run.parameter_delta_json.as_deref().map(str::trim).filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let parameter_delta = serde_json::from_str::<Value>(raw_parameter_delta)?;
    Ok((!parameter_delta.is_null()).then_some(parameter_delta))
}

/// `POST /console/v1/chat/sessions/{session_id}/branch` - forks a new session
/// off the source session's latest terminal run, wiring lineage on both
/// sides, appending a `rollback.marker` tape event, and copying project
/// context.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the source session
/// has no run, or the latest run is not terminal.
pub(crate) async fn console_chat_branch_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatBranchRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let source_session =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let requested_session_label = payload.session_label.and_then(trim_to_option);
    let family_title_seed =
        load_lineage_title_seed(&state, &session.context, &source_session).await?;
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
        .commit_orchestrator_session_branch(journal::OrchestratorSessionBranchCommitRequest {
            source_session_id: source_session.session_id.clone(),
            source_run_id: source_run_id.clone(),
            session_label: requested_session_label.clone(),
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            suggested_auto_title: Some(family_title_seed.suggested_title.clone()),
            kind: journal::OrchestratorSessionBranchKind::Branch,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": branched.session,
        "source_run_id": source_run_id,
        "suggested_session_label": family_title_seed.suggested_title,
        "action": "branch",
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/compactions/preview` -
/// computes a compaction plan for the session without applying it.
///
/// # Errors
/// Returns an error `Response` when authorization fails or plan computation
/// fails.
pub(crate) async fn console_chat_compaction_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatCompactionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), false).await?;
    let plan = preview_session_compaction(
        &state.runtime,
        &session_record,
        payload.trigger_reason.as_deref(),
        payload.trigger_policy.as_deref(),
        payload.operator_instruction.as_deref(),
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": session_record,
        "preview": plan.to_response_json(),
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/compactions` - applies a
/// manual session compaction (with per-candidate accept/reject overrides) and
/// records the pruning decision event.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the pruning preview
/// capability is disabled, or the compaction/decision write fails.
pub(crate) async fn console_chat_compaction_apply_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatCompactionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::PruningPolicyMatrix,
    )?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let execution = apply_session_compaction(SessionCompactionApplyRequest {
        runtime_state: &state.runtime,
        session: &session_record,
        actor_principal: session.context.principal.as_str(),
        run_id: session_record.last_run_id.as_deref(),
        usage_observation_run_id: None,
        mode: "manual",
        trigger_reason: payload.trigger_reason.as_deref(),
        trigger_policy: payload.trigger_policy.as_deref(),
        operator_instruction: payload.operator_instruction.as_deref(),
        accept_candidate_ids: payload.accept_candidate_ids.as_slice(),
        reject_candidate_ids: payload.reject_candidate_ids.as_slice(),
    })
    .await
    .map_err(runtime_status_response)?;
    state
        .runtime
        .record_runtime_decision_event(
            &session.context,
            Some(session_record.session_id.as_str()),
            execution.checkpoint.run_id.as_deref().or(session_record.last_run_id.as_deref()),
            RuntimeDecisionPayload::new(
                RuntimeDecisionEventType::PruningApply,
                state.runtime.runtime_decision_actor_from_context(
                    &session.context,
                    RuntimeDecisionActorKind::Operator,
                ),
                payload
                    .trigger_reason
                    .clone()
                    .unwrap_or_else(|| "session_compaction_applied".to_owned()),
                payload
                    .trigger_policy
                    .clone()
                    .unwrap_or_else(|| "pruning.preview.session_compaction".to_owned()),
                RuntimeDecisionTiming::observed(execution.artifact.created_at_unix_ms),
            )
            .with_input(RuntimeEntityRef::new(
                "session",
                "session",
                session_record.session_id.clone(),
            ))
            .with_output(RuntimeEntityRef::new(
                "checkpoint",
                "checkpoint",
                execution.checkpoint.checkpoint_id.clone(),
            ))
            .with_resource_budget(RuntimeResourceBudget {
                queue_depth: None,
                token_budget: None,
                pruning_token_delta: Some(
                    execution
                        .plan
                        .estimated_input_tokens
                        .saturating_sub(execution.plan.estimated_output_tokens),
                ),
                retrieval_branch_latency_ms: None,
                retry_count: None,
                suppression_count: None,
            })
            .with_related_entity(RuntimeEntityRef::new(
                "artifact",
                "compaction_artifact",
                execution.artifact.artifact_id.clone(),
            ))
            .with_details(json!({
                "checkpoint_id": execution.checkpoint.checkpoint_id,
                "pre_checkpoint_id": execution.pre_checkpoint.checkpoint_id,
                "post_checkpoint_id": execution.post_checkpoint.checkpoint_id,
                "artifact_id": execution.artifact.artifact_id,
                "checkpoint_pair": execution.checkpoint_pair.journal_projection,
                "compaction_safeguard": execution.safeguard,
                "successor_transcript": &execution.plan.successor_transcript,
                "identifier_evidence": &execution.plan.identifier_evidence,
                "candidate_count": execution.plan.candidates.len(),
                "write_count": execution.writes.len(),
                "mode": "manual",
            })),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": session_record,
        "artifact": execution.artifact,
        "checkpoint": execution.checkpoint,
        "pre_checkpoint": execution.pre_checkpoint,
        "post_checkpoint": execution.post_checkpoint,
        "checkpoint_pair": execution.checkpoint_pair,
        "compaction_safeguard": execution.safeguard,
        "preview": execution.plan.to_response_json(),
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/compactions/{artifact_id}` - returns one compaction
/// artifact plus the checkpoints that reference it. Session visibility is
/// enforced by loading the owning session in the caller's context.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the artifact is
/// unknown, or the owning session is not visible to this context.
pub(crate) async fn console_chat_compaction_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(artifact_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let artifact = state
        .runtime
        .get_orchestrator_compaction_artifact(artifact_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "compaction artifact not found: {artifact_id}"
            )))
        })?;
    let session_record =
        load_console_chat_session(&state, &session.context, artifact.session_id.as_str(), false)
            .await?;
    let checkpoint_pair_id = compaction_checkpoint_pair_id(artifact.summary_json.as_str());
    let related_checkpoints = state
        .runtime
        .list_orchestrator_checkpoints(artifact.session_id.clone())
        .await
        .map_err(runtime_status_response)?
        .into_iter()
        .filter(|checkpoint| {
            checkpoint_matches_compaction_artifact(
                checkpoint,
                &artifact,
                checkpoint_pair_id.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "session": session_record,
        "artifact": artifact,
        "checkpoint_pair_id": checkpoint_pair_id,
        "related_checkpoints": related_checkpoints,
        "contract": contract_descriptor(),
    })))
}

fn compaction_checkpoint_pair_id(summary_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(summary_json)
        .ok()?
        .pointer("/checkpoint_pair/journal_projection/pair_id")?
        .as_str()
        .map(ToOwned::to_owned)
}

fn checkpoint_matches_compaction_artifact(
    checkpoint: &journal::OrchestratorCheckpointRecord,
    artifact: &journal::OrchestratorCompactionArtifactRecord,
    checkpoint_pair_id: Option<&str>,
) -> bool {
    let references_artifact =
        serde_json::from_str::<Vec<String>>(checkpoint.referenced_compaction_ids_json.as_str())
            .ok()
            .is_some_and(|references| {
                references.iter().any(|value| value == &artifact.artifact_id)
            });
    references_artifact
        || checkpoint_pair_id.is_some_and(|pair_id| checkpoint_tags_include(checkpoint, pair_id))
}

fn checkpoint_tags_include(
    checkpoint: &journal::OrchestratorCheckpointRecord,
    value: &str,
) -> bool {
    serde_json::from_str::<Vec<String>>(checkpoint.tags_json.as_str())
        .ok()
        .is_some_and(|tags| tags.iter().any(|tag| tag == value))
}

/// `POST /console/v1/chat/sessions/{session_id}/checkpoints` - creates a
/// named conversation checkpoint referencing recent compactions and the
/// session's pinned/owned workspace documents.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the auxiliary
/// executor capability is disabled, the name is empty, or persistence fails.
pub(crate) async fn console_chat_checkpoint_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatCheckpointRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(&state, RuntimePreviewCapability::AuxiliaryExecutor)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let name = trim_to_option(payload.name).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("checkpoint name cannot be empty"))
    })?;
    let compactions = state
        .runtime
        .list_orchestrator_compaction_artifacts(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let workspace_documents = state
        .runtime
        .list_workspace_documents(journal::WorkspaceDocumentListFilter {
            principal: session.context.principal.clone(),
            channel: session.context.channel.clone(),
            agent_id: None,
            prefix: None,
            include_deleted: false,
            limit: 64,
        })
        .await
        .map_err(runtime_status_response)?;
    let workspace_paths = workspace_documents
        .into_iter()
        .filter(|document| {
            document.latest_session_id.as_deref() == Some(session_record.session_id.as_str())
                || document.pinned
        })
        .map(|document| document.path)
        .collect::<Vec<_>>();
    let checkpoint = state
        .runtime
        .create_orchestrator_checkpoint(journal::OrchestratorCheckpointCreateRequest {
            checkpoint_id: Ulid::generate().to_string(),
            session_id: session_record.session_id.clone(),
            run_id: session_record
                .last_run_id
                .clone()
                .or(session_record.branch_origin_run_id.clone()),
            name,
            tags_json: serde_json::to_string(&normalize_checkpoint_tags(payload.tags.as_slice()))
                .map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to encode checkpoint tags: {error}"
                )))
            })?,
            note: payload.note.and_then(trim_to_option),
            branch_state: session_record.branch_state.clone(),
            parent_session_id: session_record.parent_session_id.clone(),
            referenced_compaction_ids_json: serde_json::to_string(
                &compactions
                    .iter()
                    .take(8)
                    .map(|artifact| artifact.artifact_id.clone())
                    .collect::<Vec<_>>(),
            )
            .map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to encode checkpoint compaction references: {error}"
                )))
            })?,
            workspace_paths_json: serde_json::to_string(&workspace_paths).map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to encode checkpoint workspace paths: {error}"
                )))
            })?,
            created_by_principal: session.context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": session_record,
        "checkpoint": checkpoint,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/checkpoints/{checkpoint_id}` - returns one
/// conversation checkpoint plus its session record.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the checkpoint is
/// unknown, or its session is not visible to this context.
pub(crate) async fn console_chat_checkpoint_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(checkpoint_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let checkpoint = state
        .runtime
        .get_orchestrator_checkpoint(checkpoint_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "checkpoint not found: {checkpoint_id}"
            )))
        })?;
    let session_record =
        load_console_chat_session(&state, &session.context, checkpoint.session_id.as_str(), false)
            .await?;
    Ok(Json(json!({
        "session": session_record,
        "checkpoint": checkpoint,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/workspace-checkpoints/{checkpoint_id}` - returns a
/// workspace checkpoint with its captured files and recent restore reports.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the checkpoint or
/// its session is not visible to this context.
pub(crate) async fn console_chat_workspace_checkpoint_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(checkpoint_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let checkpoint =
        load_console_workspace_checkpoint(&state, &session.context, checkpoint_id.as_str(), false)
            .await?;
    let session_record =
        load_console_chat_session(&state, &session.context, checkpoint.session_id.as_str(), false)
            .await?;
    let files = state
        .runtime
        .list_workspace_checkpoint_files(checkpoint.checkpoint_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let restore_reports = state
        .runtime
        .list_workspace_restore_reports(crate::journal::WorkspaceRestoreReportListFilter {
            checkpoint_id: Some(checkpoint.checkpoint_id.clone()),
            session_id: None,
            run_id: None,
            device_id: None,
            limit: Some(32),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "session": session_record,
        "checkpoint": checkpoint,
        "files": files,
        "restore_reports": restore_reports,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/workspace-restore-reports/{report_id}` - returns one
/// workspace restore report, re-authorizing both the session and the
/// checkpoint it points at.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the id is malformed,
/// or the report/checkpoint/session chain is not visible to this context.
pub(crate) async fn console_chat_workspace_restore_report_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(report_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(report_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "report_id must be a canonical ULID",
        ))
    })?;
    let detail = crate::application::workspace_observability::load_workspace_restore_report_detail(
        &state.runtime,
        report_id.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    let session_record = load_console_chat_session(
        &state,
        &session.context,
        detail.checkpoint.session_id.as_str(),
        false,
    )
    .await?;
    let _ = load_console_workspace_checkpoint(
        &state,
        &session.context,
        detail.checkpoint.checkpoint_id.as_str(),
        false,
    )
    .await?;
    Ok(Json(json!({
        "session": session_record,
        "detail": detail,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/checkpoints/{checkpoint_id}/restore` - restores a
/// conversation checkpoint into a new branched session (lineage update, tape
/// marker on the anchor run, project-context copy) and marks the checkpoint
/// restored.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the checkpoint or
/// its anchor run is unknown, or any lineage/journal write fails.
pub(crate) async fn console_chat_checkpoint_restore_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(checkpoint_id): Path<String>,
    Json(payload): Json<ConsoleChatCheckpointRestoreRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let checkpoint = state
        .runtime
        .get_orchestrator_checkpoint(checkpoint_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "checkpoint not found: {checkpoint_id}"
            )))
        })?;
    let source_session =
        load_console_chat_session(&state, &session.context, checkpoint.session_id.as_str(), true)
            .await?;
    let requested_session_label = payload.session_label.and_then(trim_to_option);
    let family_title_seed =
        load_lineage_title_seed(&state, &session.context, &source_session).await?;
    let source_run_id =
        checkpoint.run_id.clone().or(source_session.last_run_id.clone()).ok_or_else(|| {
            runtime_status_response(tonic::Status::failed_precondition(
                "checkpoint restore requires a stored anchor run",
            ))
        })?;
    let restored = state
        .runtime
        .commit_orchestrator_session_branch(journal::OrchestratorSessionBranchCommitRequest {
            source_session_id: source_session.session_id.clone(),
            source_run_id,
            session_label: requested_session_label.clone(),
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            suggested_auto_title: Some(family_title_seed.suggested_title.clone()),
            kind: journal::OrchestratorSessionBranchKind::CheckpointRestore {
                checkpoint_id: checkpoint.checkpoint_id.clone(),
            },
        })
        .await
        .map_err(runtime_status_response)?;
    let mut checkpoint = checkpoint;
    checkpoint.restore_count = checkpoint.restore_count.saturating_add(1);
    checkpoint.last_restored_at_unix_ms = restored.checkpoint_restored_at_unix_ms;
    Ok(Json(json!({
        "session": restored.session,
        "checkpoint": checkpoint,
        "suggested_session_label": family_title_seed.suggested_title,
        "action": "checkpoint_restore",
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/workspace-checkpoints/{checkpoint_id}/restore` -
/// restores workspace files from a checkpoint, by default into a new branched
/// session (`branch_session=false` restores in place). Emits start/complete
/// (or failure) journal events and refreshes project context afterwards.
///
/// # Errors
/// Returns an error `Response` when authorization fails, `session_label` is
/// supplied without branching, or the restore itself fails. Project-context
/// copy/refresh failures are reported in the response body, not as errors.
pub(crate) async fn console_chat_workspace_checkpoint_restore_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(checkpoint_id): Path<String>,
    Json(payload): Json<ConsoleChatWorkspaceRestoreRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let checkpoint =
        load_console_workspace_checkpoint(&state, &session.context, checkpoint_id.as_str(), true)
            .await?;
    let source_session =
        load_console_chat_session(&state, &session.context, checkpoint.session_id.as_str(), true)
            .await?;
    let branch_session = payload.branch_session.unwrap_or(true);
    let requested_session_label = payload.session_label.and_then(trim_to_option);
    if !branch_session && requested_session_label.is_some() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "session_label is only supported when branch_session is true",
        )));
    }
    let family_title_seed =
        branch_session.then(|| load_lineage_title_seed(&state, &session.context, &source_session));
    let mut project_context_copy_error = None::<String>;

    let (target_session_id, suggested_session_label, branched_session_id) = if branch_session {
        let family_title_seed = family_title_seed
            .expect("family title seed future exists when branch_session is true")
            .await?;
        let branched = state
            .runtime
            .resolve_orchestrator_session(journal::OrchestratorSessionResolveRequest {
                session_id: None,
                session_key: None,
                session_label: requested_session_label.clone(),
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
                branch_origin_run_id: Some(checkpoint.run_id.clone()),
                suggested_auto_title: requested_session_label
                    .is_none()
                    .then(|| family_title_seed.suggested_title.clone()),
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
                suggested_auto_title: None,
            })
            .await
            .map_err(runtime_status_response)?;
        if let Err(status) = crate::application::project_context::copy_project_context_state(
            &state.runtime,
            source_session.session_id.as_str(),
            branched.session.session_id.as_str(),
        )
        .await
        {
            project_context_copy_error = Some(status.message().to_owned());
        }
        (
            branched.session.session_id.clone(),
            Some(family_title_seed.suggested_title),
            Some(branched.session.session_id),
        )
    } else {
        (source_session.session_id.clone(), None, None)
    };

    let scope_kind = payload
        .scope_kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("workspace")
        .to_owned();
    let target_path = payload.target_path.and_then(trim_to_option);
    let _ = crate::gateway::record_agent_journal_event(
        &state.runtime,
        &session.context,
        json!({
            "event": "workspace.restore.started",
            "checkpoint_id": checkpoint.checkpoint_id,
            "source_session_id": source_session.session_id,
            "target_session_id": target_session_id,
            "branched_session_id": branched_session_id,
            "run_id": checkpoint.run_id,
            "scope_kind": scope_kind,
            "target_path": target_path,
            "target_workspace_root_index": payload.target_workspace_root_index,
        }),
    )
    .await;
    let restore = match crate::application::workspace_observability::restore_workspace_checkpoint(
        &state.runtime,
        crate::application::workspace_observability::WorkspaceRestoreRequest {
            principal: session.context.principal.as_str(),
            device_id: session.context.device_id.as_str(),
            channel: session.context.channel.as_deref(),
            target_session_id: target_session_id.as_str(),
            checkpoint: checkpoint.clone(),
            scope_kind: scope_kind.as_str(),
            target_path: target_path.as_deref(),
            target_workspace_root_index: payload.target_workspace_root_index,
            branched_session_id: branched_session_id.as_deref(),
        },
    )
    .await
    {
        Ok(restore) => restore,
        Err(status) => {
            let _ = crate::gateway::record_agent_journal_event(
                &state.runtime,
                &session.context,
                json!({
                    "event": "workspace.restore.failed",
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "source_session_id": source_session.session_id,
                    "target_session_id": target_session_id,
                    "branched_session_id": branched_session_id,
                    "run_id": checkpoint.run_id,
                    "scope_kind": scope_kind,
                    "target_path": target_path,
                    "target_workspace_root_index": payload.target_workspace_root_index,
                    "error": status.message(),
                }),
            )
            .await;
            return Err(runtime_status_response(status));
        }
    };
    let (project_context_refresh, project_context_refresh_error) =
        match crate::application::project_context::refresh_project_context(
            &state.runtime,
            &session.context,
            target_session_id.as_str(),
        )
        .await
        {
            Ok(preview) => (
                Some(json!({
                    "session_id": target_session_id,
                    "preview": preview,
                })),
                None,
            ),
            Err(status) => (None, Some(status.message().to_owned())),
        };
    let _ = crate::gateway::record_agent_journal_event(
        &state.runtime,
        &session.context,
        json!({
            "event": "workspace.restore.completed",
            "checkpoint_id": checkpoint.checkpoint_id,
            "source_session_id": source_session.session_id,
            "target_session_id": target_session_id,
            "branched_session_id": branched_session_id,
            "run_id": checkpoint.run_id,
            "scope_kind": restore.scope_kind,
            "target_path": restore.target_path,
            "target_workspace_root_index": restore.target_workspace_root_index,
            "restored_paths": restore.restored_paths,
            "failed_paths": restore.failed_paths,
            "affects_context_stack": restore.affects_context_stack,
            "restore_report_id": restore.report.report_id,
            "result_state": restore.report.result_state,
            "reconciliation_summary": restore.report.reconciliation_summary,
            "reconciliation_prompt": restore.report.reconciliation_prompt,
            "project_context_refresh_error": project_context_refresh_error,
            "project_context_copy_error": project_context_copy_error,
        }),
    )
    .await;
    let checkpoint = state
        .runtime
        .get_workspace_checkpoint(checkpoint.checkpoint_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(
                "workspace checkpoint disappeared after restore",
            ))
        })?;
    let target_session =
        load_console_chat_session(&state, &session.context, target_session_id.as_str(), true)
            .await?;
    Ok(Json(json!({
        "session": target_session,
        "source_session": source_session,
        "checkpoint": checkpoint,
        "restore": restore,
        "project_context_refresh": project_context_refresh,
        "project_context_refresh_error": project_context_refresh_error,
        "project_context_copy_error": project_context_copy_error,
        "suggested_session_label": suggested_session_label,
        "action": "workspace_restore",
        "contract": contract_descriptor(),
    })))
}

// --- Background tasks ---

/// `POST /console/v1/chat/sessions/{session_id}/background-tasks` - enqueues
/// an auxiliary background task for the session and records a runtime-preview
/// lifecycle event.
///
/// # Errors
/// Returns an error `Response` when authorization fails, text is empty,
/// delegation is requested outside an admitted run, the task kind is reserved
/// or inconsistent with delegation, or persistence fails.
pub(crate) async fn console_chat_background_task_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatBackgroundTaskCreateRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let text = trim_to_option(payload.text).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("text cannot be empty"))
    })?;
    let requested_budget_tokens =
        console_background_task_budget_tokens(payload.budget_tokens, text.as_str());
    let requested_max_attempts = payload.max_attempts.unwrap_or(3).clamp(1, 16);
    if payload.delegation.is_some() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "console background delegation requires admitted Run-root cancellation authority",
        )));
    }
    let requested_task_kind = payload.task_kind.clone().and_then(trim_to_option);
    let task_kind = resolve_console_background_task_kind(requested_task_kind.as_deref())
        .map_err(runtime_status_response)?;
    let task_budget_tokens = requested_budget_tokens;
    let task_max_attempts = requested_max_attempts;
    let payload_json =
        build_console_background_task_payload_json(payload.parameter_delta.as_ref(), None)?;
    let task = state
        .runtime
        .create_orchestrator_background_task(journal::OrchestratorBackgroundTaskCreateRequest {
            task_id: Ulid::generate().to_string(),
            task_kind,
            session_id: session_record.session_id.clone(),
            child_session_id: None,
            parent_run_id: session_record.last_run_id.clone(),
            target_run_id: None,
            planned_child_run_id: None,
            queued_input_id: None,
            owner_principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: payload.priority.unwrap_or(0).clamp(-10, 10),
            max_attempts: task_max_attempts,
            budget_tokens: task_budget_tokens,
            delegation: None,
            cancellation_context: None,
            not_before_unix_ms: payload.not_before_unix_ms,
            expires_at_unix_ms: payload.expires_at_unix_ms,
            notification_target_json: payload
                .notification_target
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to encode background notification target: {error}"
                    )))
                })?,
            input_text: Some(text),
            payload_json,
        })
        .await
        .map_err(runtime_status_response)?;
    record_background_task_runtime_preview(
        &state,
        &session.context,
        &task,
        "background_task_created",
        None,
    )
    .await?;
    Ok(Json(json!({
        "session": session_record,
        "task": task,
        "live_steering": background_task_live_steering_status(&session_record),
        "contract": contract_descriptor(),
    })))
}

/// Tells the web client that background-enqueue never live-redirects an
/// active run, so it can label the action accurately.
fn background_task_live_steering_status(
    session: &journal::OrchestratorSessionRecord,
) -> serde_json::Value {
    let parent_run_state = session.last_run_state.as_deref().unwrap_or("unknown");
    let parent_run_active = matches!(parent_run_state, "accepted" | "in_progress");
    json!({
        "supported": false,
        "mode": "background_task",
        "parent_run_id": session.last_run_id.as_deref(),
        "parent_run_state": parent_run_state,
        "parent_run_active": parent_run_active,
        "message": if parent_run_active {
            "background-enqueue creates an independent follow-up task; it does not live-redirect the active run"
        } else {
            "background-enqueue creates an independent follow-up task for the session"
        },
    })
}

/// Validates the task kind for a non-delegated background task, rejecting
/// kinds reserved for internal runtime use (attachment derivation/recompute,
/// post-run reflection).
fn resolve_console_background_task_kind(value: Option<&str>) -> Result<String, tonic::Status> {
    let Some(raw) = value else {
        return Ok(AuxiliaryTaskKind::BackgroundPrompt.as_str().to_owned());
    };
    let Some(kind) = AuxiliaryTaskKind::from_str(raw) else {
        return Err(tonic::Status::invalid_argument(format!(
            "unsupported auxiliary task_kind: {raw}"
        )));
    };
    match kind {
        AuxiliaryTaskKind::BackgroundPrompt
        | AuxiliaryTaskKind::Summary
        | AuxiliaryTaskKind::RecallSearch
        | AuxiliaryTaskKind::Classification
        | AuxiliaryTaskKind::Extraction
        | AuxiliaryTaskKind::Vision => Ok(kind.as_str().to_owned()),
        AuxiliaryTaskKind::DelegationPrompt => {
            Err(tonic::Status::invalid_argument("task_kind=delegation_prompt requires delegation"))
        }
        AuxiliaryTaskKind::AttachmentDerivation
        | AuxiliaryTaskKind::ObjectiveJudge
        | AuxiliaryTaskKind::AttachmentRecompute
        | AuxiliaryTaskKind::PostRunReflection => Err(tonic::Status::invalid_argument(
            "requested task_kind is reserved for internal runtime tasks",
        )),
    }
}

/// `GET /console/v1/chat/background-tasks` - lists background tasks owned by
/// the console context, optionally filtered by session.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the listing fails.
pub(crate) async fn console_chat_background_tasks_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleChatBackgroundTasksQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let tasks = state
        .runtime
        .list_orchestrator_background_tasks(journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: Some(session.context.device_id.clone()),
            channel: session.context.channel.clone(),
            session_id: query.session_id.and_then(trim_to_option),
            include_completed: query.include_completed.unwrap_or(false),
            limit: query.limit.unwrap_or(32).clamp(1, 128),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "tasks": tasks,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/background-tasks/{task_id}` - returns one background
/// task and, if it spawned a run, that run's snapshot.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the task is not
/// visible to this context (`not_found`).
pub(crate) async fn console_chat_background_task_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let task = load_console_background_task(&state, &session.context, task_id.as_str()).await?;
    let run = if let Some(target_run_id) = task.target_run_id.clone() {
        state
            .runtime
            .orchestrator_run_status_snapshot(target_run_id)
            .await
            .map_err(runtime_status_response)?
    } else {
        None
    };
    Ok(Json(json!({
        "task": task,
        "run": run,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/background-tasks/{task_id}/pause` - pauses a queued
/// or failed background task.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the capability is
/// disabled, or the task is not in a pausable state.
pub(crate) async fn console_chat_background_task_pause_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(&state, RuntimePreviewCapability::AuxiliaryExecutor)?;
    let task = load_console_background_task(&state, &session.context, task_id.as_str()).await?;
    if !matches!(
        AuxiliaryTaskState::from_str(task.state.as_str()),
        Some(AuxiliaryTaskState::Queued | AuxiliaryTaskState::Failed)
    ) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "only queued or failed background tasks can be paused",
        )));
    }
    state
        .runtime
        .update_orchestrator_background_task(journal::OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            expected_revision: task.revision,
            state: Some(AuxiliaryTaskState::Paused.as_str().to_owned()),
            ..Default::default()
        })
        .await
        .map_err(runtime_status_response)?;
    let task =
        load_console_background_task(&state, &session.context, task.task_id.as_str()).await?;
    record_background_task_runtime_preview(
        &state,
        &session.context,
        &task,
        "background_task_paused",
        None,
    )
    .await?;
    Ok(Json(json!({
        "task": task,
        "action": "pause",
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/background-tasks/{task_id}/resume` - re-queues a
/// paused background task.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the capability is
/// disabled, or the task is not paused.
pub(crate) async fn console_chat_background_task_resume_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(&state, RuntimePreviewCapability::AuxiliaryExecutor)?;
    let task = load_console_background_task(&state, &session.context, task_id.as_str()).await?;
    if AuxiliaryTaskState::from_str(task.state.as_str()) != Some(AuxiliaryTaskState::Paused) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "only paused background tasks can be resumed",
        )));
    }
    state
        .runtime
        .update_orchestrator_background_task(journal::OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            expected_revision: task.revision,
            state: Some(AuxiliaryTaskState::Queued.as_str().to_owned()),
            completed_at_unix_ms: Some(None),
            ..Default::default()
        })
        .await
        .map_err(runtime_status_response)?;
    let task =
        load_console_background_task(&state, &session.context, task.task_id.as_str()).await?;
    record_background_task_runtime_preview(
        &state,
        &session.context,
        &task,
        "background_task_resumed",
        None,
    )
    .await?;
    Ok(Json(json!({
        "task": task,
        "action": "resume",
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/background-tasks/{task_id}/retry` - re-queues a
/// failed, cancelled, or expired task, clearing its previous outcome fields.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the capability is
/// disabled, or the task is not in a retryable state.
pub(crate) async fn console_chat_background_task_retry_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(&state, RuntimePreviewCapability::AuxiliaryExecutor)?;
    let task = load_console_background_task(&state, &session.context, task_id.as_str()).await?;
    if !matches!(
        AuxiliaryTaskState::from_str(task.state.as_str()),
        Some(
            AuxiliaryTaskState::Failed
                | AuxiliaryTaskState::Cancelled
                | AuxiliaryTaskState::Expired
        )
    ) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "only failed, cancelled, or expired background tasks can be retried",
        )));
    }
    // max_attempts == 0 means unlimited retries, matching the queue scheduler.
    if task.max_attempts > 0 && task.attempt_count >= task.max_attempts {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "background task retry budget is exhausted",
        )));
    }
    if let Some(reconciled) = state
        .runtime
        .reconcile_background_task_before_retry(task.task_id.clone(), task.state.clone())
        .await
        .map_err(runtime_status_response)?
    {
        record_background_task_runtime_preview(
            &state,
            &session.context,
            &reconciled,
            "background_task_existing_child_reconciled",
            None,
        )
        .await?;
        return Ok(Json(json!({
            "task": reconciled,
            "action": "reconciled_existing_child",
            "contract": contract_descriptor(),
        })));
    }
    state
        .runtime
        .update_orchestrator_background_task(journal::OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            expected_revision: task.revision,
            state: Some(AuxiliaryTaskState::Queued.as_str().to_owned()),
            target_run_id: Some(None),
            last_error: Some(None),
            result_json: Some(None),
            started_at_unix_ms: Some(None),
            completed_at_unix_ms: Some(None),
        })
        .await
        .map_err(runtime_status_response)?;
    let task =
        load_console_background_task(&state, &session.context, task.task_id.as_str()).await?;
    record_background_task_runtime_preview(
        &state,
        &session.context,
        &task,
        "background_task_requeued",
        Some(1),
    )
    .await?;
    Ok(Json(json!({
        "task": task,
        "action": "retry",
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/background-tasks/{task_id}/cancel` - cancels a
/// background task. A running task with a target run transitions to
/// `cancel_requested` (the run is asked to cancel asynchronously); anything
/// else is cancelled immediately.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the capability is
/// disabled, or a state update fails.
pub(crate) async fn console_chat_background_task_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(&state, RuntimePreviewCapability::AuxiliaryExecutor)?;
    let task = load_console_background_task(&state, &session.context, task_id.as_str()).await?;
    let _media_job_cancelled =
        state.audio_sessions.cancel_job(task.session_id.as_str(), task.task_id.as_str());
    let task_state = AuxiliaryTaskState::from_str(task.state.as_str());
    if matches!(task_state, Some(AuxiliaryTaskState::Running | AuxiliaryTaskState::CancelRequested))
    {
        if let Some(target_run_id) = task.target_run_id.clone() {
            state
                .runtime
                .request_orchestrator_cancel(journal::OrchestratorCancelRequest {
                    run_id: target_run_id,
                    reason: "background_task_cancelled_by_operator".to_owned(),
                })
                .await
                .map_err(runtime_status_response)?;
            state
                .runtime
                .update_orchestrator_background_task(build_background_task_cancel_requested_update(
                    task.task_id.as_str(),
                    task.revision,
                ))
                .await
                .map_err(runtime_status_response)?;
        } else {
            let has_attach_pending_work = task_state == Some(AuxiliaryTaskState::CancelRequested)
                && task.started_at_unix_ms.is_some();
            if task_state == Some(AuxiliaryTaskState::Running) || has_attach_pending_work {
                state
                    .runtime
                    .update_orchestrator_background_task(
                        build_background_task_cancel_requested_update(
                            task.task_id.as_str(),
                            task.revision,
                        ),
                    )
                    .await
                    .map_err(runtime_status_response)?;
            } else {
                state
                    .runtime
                    .update_orchestrator_background_task(build_background_task_cancelled_update(
                        task.task_id.as_str(),
                        task.revision,
                    ))
                    .await
                    .map_err(runtime_status_response)?;
            }
        }
    } else {
        state
            .runtime
            .update_orchestrator_background_task(build_background_task_cancelled_update(
                task.task_id.as_str(),
                task.revision,
            ))
            .await
            .map_err(runtime_status_response)?;
    }
    let task =
        load_console_background_task(&state, &session.context, task.task_id.as_str()).await?;
    record_background_task_runtime_preview(
        &state,
        &session.context,
        &task,
        "background_task_cancelled",
        None,
    )
    .await?;
    Ok(Json(json!({
        "task": task,
        "action": "cancel",
        "contract": contract_descriptor(),
    })))
}

fn build_background_task_cancel_requested_update(
    task_id: &str,
    expected_revision: u64,
) -> journal::OrchestratorBackgroundTaskUpdateRequest {
    journal::OrchestratorBackgroundTaskUpdateRequest {
        task_id: task_id.to_owned(),
        expected_revision,
        state: Some(AuxiliaryTaskState::CancelRequested.as_str().to_owned()),
        result_json: Some(Some(build_background_task_cancel_requested_result_json(task_id))),
        ..Default::default()
    }
}

fn build_background_task_cancelled_update(
    task_id: &str,
    expected_revision: u64,
) -> journal::OrchestratorBackgroundTaskUpdateRequest {
    journal::OrchestratorBackgroundTaskUpdateRequest {
        task_id: task_id.to_owned(),
        expected_revision,
        state: Some(AuxiliaryTaskState::Cancelled.as_str().to_owned()),
        completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
        last_error: Some(Some("cancelled_by_operator".to_owned())),
        result_json: Some(Some(build_background_task_cancelled_result_json(task_id))),
        ..Default::default()
    }
}

fn build_background_task_cancel_requested_result_json(task_id: &str) -> String {
    json!({
        "status": "cancel_requested",
        "task_id": task_id,
        "reason": "cancelled_by_operator",
    })
    .to_string()
}

fn build_background_task_cancelled_result_json(task_id: &str) -> String {
    json!({
        "status": "cancelled",
        "task_id": task_id,
        "reason": "cancelled_by_operator",
    })
    .to_string()
}

// --- Session input queue (admission and operator controls) ---

/// `POST /console/v1/chat/runs/{run_id}/queue` - submits a follow-up message
/// while a run is still streaming. Followups enter the request stream for the
/// next turn; steer and interrupt inputs remain journal-owned until the run
/// loop claims their exact generation boundary; collect inputs stay in bounded
/// attachment-aware backlog summaries. Every decision is journaled as a
/// queued-input record plus runtime-decision and tape events.
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail,
/// the run has no active stream, the run belongs to another context, or
/// persistence fails. A forward attempt to a closed stream marks the input
/// `delivery_failed` and returns `failed_precondition`.
pub(crate) async fn console_chat_queue_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(payload): Json<ConsoleChatQueueRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    ensure_console_runtime_preview_capability(&state, RuntimePreviewCapability::FlowOrchestration)?;
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
    let (generation_session_id, active_generation) = state
        .runtime
        .persisted_runtime_generation_for_run(run_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::failed_precondition(
                "queued input requires an active runtime generation",
            ))
        })?;
    if generation_session_id != stream.session_id {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "queued input runtime generation belongs to another session",
        )));
    }
    let expected_active_generation = i64::try_from(active_generation.get()).map_err(|_| {
        runtime_status_response(tonic::Status::failed_precondition(
            "runtime generation exceeds the journal integer range",
        ))
    })?;
    let attachment_refs_json = serde_json::to_string(&payload.attachments).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "attachments could not be serialized: {error}"
        )))
    })?;
    let resolved_attachments = load_console_chat_message_attachments(
        &state,
        &session.context,
        stream.session_id.as_str(),
        payload.attachments.as_slice(),
    )
    .map_err(|response| *response)?;
    // A pending tool approval is an unsafe boundary: forwarding a follow-up
    // mid-approval could race the approval response, so the decision below
    // degrades to enqueue/collect while any approval is outstanding.
    let pending_approval =
        !lock_console_chat_pending_approvals(&stream.pending_approvals).is_empty();
    let timestamp_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let requested_mode = payload.queue_mode.as_deref().and_then(QueueMode::parse);
    let existing_queued_inputs = state
        .runtime
        .list_orchestrator_queued_inputs(stream.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let policy = SessionQueuePolicy::from_config(
        &state.runtime.config.session_queue_policy,
        stream.session_id.as_str(),
        session.context.channel.as_deref(),
        None,
    );
    let queue_control = state
        .runtime
        .get_orchestrator_session_queue_control(stream.session_id.clone())
        .await
        .map_err(runtime_status_response)?
        .unwrap_or_else(|| default_session_queue_control(stream.session_id.clone()));
    let coalescing_group = policy.coalescing_group.clone();
    let current_depth =
        pending_queue_depth(existing_queued_inputs.as_slice(), Some(coalescing_group.as_str()));
    let safe_boundary = SessionQueueSafeBoundary::active(true, pending_approval);
    let mut queue_decision = decide_session_queue_mode(
        policy,
        if queue_control.paused { Some(QueueMode::Collect) } else { requested_mode },
        safe_boundary,
        current_depth,
    );
    // Operator pause overrides everything except overflow: inputs are still
    // accepted (collect mode) but never forwarded until the queue resumes.
    if queue_control.paused && queue_decision.decision != QueueDecision::Overflow {
        queue_decision.decision = QueueDecision::Defer;
        queue_decision.mode = QueueMode::Collect;
        queue_decision.reason = "session_queue_paused".to_owned();
        queue_decision.delivery_boundary = QueuedInputDeliveryBoundary::BacklogSummary;
    }
    let queued_input_id = Ulid::generate().to_string();
    let pending_group_inputs = existing_queued_inputs
        .iter()
        .filter(|queued| {
            QueuedInputState::parse(queued.state.as_str()).is_some_and(QueuedInputState::is_active)
                && queued.coalescing_group.as_deref() == Some(coalescing_group.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    // Overflow and collect-with-backlog both coalesce the pending group into
    // one summary input; the merged originals are marked overflowed/merged
    // below so only the summary remains pending.
    let should_collect_summary = queue_decision.decision == QueueDecision::Overflow
        || (queue_decision.mode == QueueMode::Collect && !pending_group_inputs.is_empty());
    let mut effective_text = text.clone();
    let mut effective_attachments_json = attachment_refs_json.clone();
    let mut overflow_summary_ref = None;
    if should_collect_summary {
        if queue_decision.decision != QueueDecision::Overflow {
            queue_decision.decision = QueueDecision::Merge;
            queue_decision.reason = "collect_coalesced_with_pending_backlog".to_owned();
        }
        let summary_ref = format!("queue-summary:{queued_input_id}");
        let mut summary_sources = pending_group_inputs.clone();
        summary_sources.push(journal::OrchestratorQueuedInputRecord {
            queued_input_id: queued_input_id.clone(),
            run_id: run_id.clone(),
            session_id: stream.session_id.clone(),
            state: if queue_decision.accepted {
                QueuedInputState::Pending.as_str().to_owned()
            } else {
                QueuedInputState::Overflowed.as_str().to_owned()
            },
            queue_mode: queue_decision.mode.as_str().to_owned(),
            delivery_boundary: queue_decision.delivery_boundary.as_str().to_owned(),
            expected_active_generation: Some(expected_active_generation),
            claimed_active_generation: None,
            lifecycle_revision: 0,
            priority_lane: queue_decision.policy.priority_lane.clone(),
            coalescing_group: Some(coalescing_group.clone()),
            overflow_summary_ref: Some(summary_ref.clone()),
            safe_boundary_flags_json: serde_json::to_string(&queue_decision.safe_boundary)
                .unwrap_or_else(|_| "{}".to_owned()),
            decision_reason: queue_decision.reason.clone(),
            text: text.clone(),
            attachments_json: attachment_refs_json.clone(),
            queue_outcome_json: "{}".to_owned(),
            accepted_at_unix_ms: queue_decision.accepted.then_some(timestamp_unix_ms),
            coalesced_at_unix_ms: None,
            forwarded_at_unix_ms: None,
            terminal_at_unix_ms: None,
            policy_snapshot_json: queue_decision.policy.snapshot_json().to_string(),
            explain_json: queue_decision.explain_json().to_string(),
            created_at_unix_ms: timestamp_unix_ms,
            updated_at_unix_ms: timestamp_unix_ms,
            origin_run_id: Some(run_id.clone()),
        });
        let collect_summary = build_queue_collect_summary(
            summary_ref.clone(),
            summary_sources.as_slice(),
            queue_decision.reason.as_str(),
        );
        effective_text = collect_summary.text;
        effective_attachments_json = collect_summary.attachment_refs_json.to_string();
        overflow_summary_ref = Some(collect_summary.summary_ref);
    }
    let initial_queued_state = if !queue_decision.accepted {
        QueuedInputState::Overflowed
    } else if queue_decision.decision == QueueDecision::Defer {
        QueuedInputState::Deferred
    } else {
        QueuedInputState::Pending
    };
    let initial_queue_outcome = queue_outcome(
        queued_input_id.clone(),
        initial_queued_state,
        queue_decision.delivery_boundary,
        Some(active_generation.get()),
        Some(active_generation.get()),
        queue_decision.accepted,
        queue_decision.reason.clone(),
    );
    let mut queued = state
        .runtime
        .create_orchestrator_queued_input(journal::OrchestratorQueuedInputCreateRequest {
            queued_input_id: queued_input_id.clone(),
            run_id: run_id.clone(),
            session_id: stream.session_id.clone(),
            state: initial_queued_state.as_str().to_owned(),
            text: effective_text.clone(),
            origin_run_id: Some(run_id.clone()),
            queue_mode: queue_decision.mode.as_str().to_owned(),
            delivery_boundary: queue_decision.delivery_boundary.as_str().to_owned(),
            expected_active_generation: Some(expected_active_generation),
            priority_lane: queue_decision.policy.priority_lane.clone(),
            coalescing_group: Some(queue_decision.policy.coalescing_group.clone()),
            overflow_summary_ref: overflow_summary_ref.clone(),
            safe_boundary_flags_json: serde_json::to_string(&queue_decision.safe_boundary)
                .unwrap_or_else(|_| "{}".to_owned()),
            decision_reason: queue_decision.reason.clone(),
            attachments_json: effective_attachments_json,
            queue_outcome_json: serde_json::to_string(&initial_queue_outcome).map_err(|error| {
                runtime_status_response(tonic::Status::internal(error.to_string()))
            })?,
            accepted_at_unix_ms: queue_decision.accepted.then_some(timestamp_unix_ms),
            policy_snapshot_json: queue_decision.policy.snapshot_json().to_string(),
            explain_json: queue_decision.explain_json().to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    for pending in &pending_group_inputs {
        let merged_state = if queue_decision.decision == QueueDecision::Overflow {
            QueuedInputState::Overflowed
        } else {
            QueuedInputState::Merged
        };
        let pending_boundary =
            QueuedInputDeliveryBoundary::parse(pending.delivery_boundary.as_str())
                .unwrap_or(QueuedInputDeliveryBoundary::BacklogSummary);
        let pending_outcome = queue_outcome(
            pending.queued_input_id.clone(),
            merged_state,
            pending_boundary,
            pending.expected_active_generation.and_then(|value| u64::try_from(value).ok()),
            Some(active_generation.get()),
            false,
            queue_decision.reason.clone(),
        );
        state
            .runtime
            .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
                queued_input_id: pending.queued_input_id.clone(),
                expected_state: pending.state.clone(),
                expected_revision: pending.lifecycle_revision,
                state: merged_state.as_str().to_owned(),
                claimed_active_generation: None,
                overflow_summary_ref: overflow_summary_ref.clone(),
                decision_reason: Some(queue_decision.reason.clone()),
                explain_json: Some(queue_decision.explain_json().to_string()),
                queue_outcome_json: Some(serde_json::to_string(&pending_outcome).map_err(
                    |error| runtime_status_response(tonic::Status::internal(error.to_string())),
                )?),
            })
            .await
            .map_err(runtime_status_response)?;
    }
    let queue_event_type = match queue_decision.decision {
        QueueDecision::Overflow => RuntimeDecisionEventType::QueueOverflow,
        QueueDecision::Steer | QueueDecision::SteerBacklog => RuntimeDecisionEventType::QueueSteer,
        QueueDecision::Interrupt => RuntimeDecisionEventType::QueueInterrupt,
        QueueDecision::Merge => RuntimeDecisionEventType::QueueMerge,
        QueueDecision::Enqueue | QueueDecision::Defer => RuntimeDecisionEventType::QueueEnqueue,
    };
    let queue_enqueue_payload = RuntimeDecisionPayload::new(
        queue_event_type,
        state.runtime.runtime_decision_actor_from_context(
            &session.context,
            RuntimeDecisionActorKind::Operator,
        ),
        queue_decision.reason.clone(),
        queue_decision.policy.policy_id.clone(),
        RuntimeDecisionTiming::observed(timestamp_unix_ms),
    )
    .with_input(
        RuntimeEntityRef::new("queued_input", "queued_input", queued.queued_input_id.clone())
            .with_state(queued.state.as_str()),
    )
    .with_output(RuntimeEntityRef::new("run", "run", run_id.clone()).with_state("active"))
    .with_resource_budget(RuntimeResourceBudget {
        queue_depth: Some(observed_queue_depth_after_decision(current_depth, &queue_decision)),
        token_budget: None,
        pruning_token_delta: None,
        retrieval_branch_latency_ms: None,
        retry_count: None,
        suppression_count: None,
    })
    .with_related_entity(RuntimeEntityRef::new("session", "session", stream.session_id.clone()))
    .with_details(json!({
        "origin_kind": "queued",
        "origin_run_id": run_id,
        "decision": queue_decision.decision.as_str(),
        "queue_mode": queue_decision.mode.as_str(),
        "delivery_boundary": queue_decision.delivery_boundary.as_str(),
        "queue_outcome": initial_queue_outcome,
        "safe_boundary": queue_decision.safe_boundary,
        "policy": queue_decision.policy.snapshot_json(),
    }));
    state
        .runtime
        .record_runtime_decision_event(
            &session.context,
            Some(stream.session_id.as_str()),
            Some(run_id.as_str()),
            queue_enqueue_payload,
        )
        .await
        .map_err(runtime_status_response)?;
    state.observability.observe_runtime_queue_depth(observed_queue_depth_after_decision(
        current_depth,
        &queue_decision,
    ));
    // Follow-ups enter the request stream behind the active turn. Steering
    // and interrupts stay journal-owned until the run loop claims them at
    // their generation-bound provider boundary.
    if queue_decision.mode != QueueMode::Followup || pending_approval {
        return Ok(Json(json!({
            "queued_input": queued,
            "decision": queue_decision.explain_json(),
            "queue_outcome": initial_queue_outcome,
            "policy": queue_decision.policy.snapshot_json(),
            "contract": contract_descriptor(),
        })));
    }
    let claimed_outcome = queue_outcome(
        queued.queued_input_id.clone(),
        QueuedInputState::Claimed,
        queue_decision.delivery_boundary,
        Some(active_generation.get()),
        Some(active_generation.get()),
        true,
        "queue.next_turn.claimed",
    );
    let claimed = state
        .runtime
        .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
            queued_input_id: queued.queued_input_id.clone(),
            expected_state: queued.state.clone(),
            expected_revision: queued.lifecycle_revision,
            state: QueuedInputState::Claimed.as_str().to_owned(),
            claimed_active_generation: Some(expected_active_generation),
            overflow_summary_ref: None,
            decision_reason: Some("queue.next_turn.claimed".to_owned()),
            explain_json: None,
            queue_outcome_json: Some(serde_json::to_string(&claimed_outcome).map_err(|error| {
                runtime_status_response(tonic::Status::internal(error.to_string()))
            })?),
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
            resolved_attachments,
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
        let failed_outcome = queue_outcome(
            claimed.queued_input_id.clone(),
            QueuedInputState::DeliveryFailed,
            queue_decision.delivery_boundary,
            Some(active_generation.get()),
            Some(active_generation.get()),
            false,
            "queue.next_turn.delivery_failed",
        );
        state
            .runtime
            .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
                queued_input_id: claimed.queued_input_id.clone(),
                expected_state: claimed.state.clone(),
                expected_revision: claimed.lifecycle_revision,
                state: QueuedInputState::DeliveryFailed.as_str().to_owned(),
                claimed_active_generation: None,
                overflow_summary_ref: None,
                decision_reason: Some("queue.next_turn.delivery_failed".to_owned()),
                explain_json: None,
                queue_outcome_json: Some(serde_json::to_string(&failed_outcome).map_err(
                    |error| runtime_status_response(tonic::Status::internal(error.to_string())),
                )?),
            })
            .await
            .map_err(runtime_status_response)?;
        let delivery_failed_payload = RuntimeDecisionPayload::new(
            RuntimeDecisionEventType::FlowLifecycle,
            state.runtime.runtime_decision_actor_from_context(
                &session.context,
                RuntimeDecisionActorKind::Operator,
            ),
            "queued_followup_delivery_failed",
            "flow_orchestration.preview.queue_delivery",
            RuntimeDecisionTiming::observed(crate::gateway::current_unix_ms()),
        )
        .with_input(
            RuntimeEntityRef::new("queued_input", "queued_input", queued.queued_input_id.clone())
                .with_state(QueuedInputState::DeliveryFailed.as_str()),
        )
        .with_output(RuntimeEntityRef::new("run", "run", run_id.clone()).with_state("active"))
        .with_resource_budget(RuntimeResourceBudget {
            queue_depth: Some(0),
            token_budget: None,
            pruning_token_delta: None,
            retrieval_branch_latency_ms: None,
            retry_count: Some(0),
            suppression_count: None,
        })
        .with_related_entity(RuntimeEntityRef::new("session", "session", stream.session_id.clone()))
        .with_details(json!({
            "queued_input_state": QueuedInputState::DeliveryFailed.as_str(),
        }));
        state
            .runtime
            .record_runtime_decision_event(
                &session.context,
                Some(stream.session_id.as_str()),
                Some(run_id.as_str()),
                delivery_failed_payload,
            )
            .await
            .map_err(runtime_status_response)?;
        state.observability.observe_runtime_queue_depth(0);
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "failed to forward queued follow-up to the active run stream",
        )));
    }
    let forwarded_outcome = queue_outcome(
        claimed.queued_input_id.clone(),
        QueuedInputState::Forwarded,
        queue_decision.delivery_boundary,
        Some(active_generation.get()),
        Some(active_generation.get()),
        true,
        "queue.next_turn.forwarded",
    );
    queued = state
        .runtime
        .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
            queued_input_id: claimed.queued_input_id.clone(),
            expected_state: claimed.state.clone(),
            expected_revision: claimed.lifecycle_revision,
            state: QueuedInputState::Forwarded.as_str().to_owned(),
            claimed_active_generation: None,
            overflow_summary_ref: None,
            decision_reason: Some("queue.next_turn.forwarded".to_owned()),
            explain_json: None,
            queue_outcome_json: Some(serde_json::to_string(&forwarded_outcome).map_err(
                |error| runtime_status_response(tonic::Status::internal(error.to_string())),
            )?),
        })
        .await
        .map_err(runtime_status_response)?;
    let forwarded_payload = RuntimeDecisionPayload::new(
        RuntimeDecisionEventType::FlowLifecycle,
        state.runtime.runtime_decision_actor_from_context(
            &session.context,
            RuntimeDecisionActorKind::Operator,
        ),
        "queued_followup_forwarded",
        "flow_orchestration.preview.queue_delivery",
        RuntimeDecisionTiming::observed(crate::gateway::current_unix_ms()),
    )
    .with_input(
        RuntimeEntityRef::new("queued_input", "queued_input", queued.queued_input_id.clone())
            .with_state(QueuedInputState::Forwarded.as_str()),
    )
    .with_output(RuntimeEntityRef::new("run", "run", run_id.clone()).with_state("active"))
    .with_resource_budget(RuntimeResourceBudget {
        queue_depth: Some(0),
        token_budget: None,
        pruning_token_delta: None,
        retrieval_branch_latency_ms: None,
        retry_count: Some(0),
        suppression_count: None,
    })
    .with_related_entity(RuntimeEntityRef::new("session", "session", stream.session_id.clone()))
    .with_details(json!({
        "queued_input_state": QueuedInputState::Forwarded.as_str(),
    }));
    state
        .runtime
        .record_runtime_decision_event(
            &session.context,
            Some(stream.session_id.as_str()),
            Some(run_id.as_str()),
            forwarded_payload,
        )
        .await
        .map_err(runtime_status_response)?;
    state.observability.observe_runtime_queue_depth(0);
    Ok(Json(json!({
        "queued_input": queued,
        "decision": queue_decision.explain_json(),
        "queue_outcome": forwarded_outcome,
        "policy": queue_decision.policy.snapshot_json(),
        "contract": contract_descriptor(),
    })))
}

/// Queue depth as the operator will observe it after the decision: overflow
/// clears the group, merge leaves exactly the summary input.
fn observed_queue_depth_after_decision(
    current_depth: usize,
    queue_decision: &crate::application::session_queue::SessionQueueDecision,
) -> u64 {
    match queue_decision.decision {
        QueueDecision::Overflow => 0,
        QueueDecision::Merge => 1,
        _ if queue_decision.accepted => current_depth.saturating_add(1) as u64,
        _ => current_depth as u64,
    }
}

/// `GET /console/v1/chat/sessions/{session_id}/queue/policy` - returns the
/// session queue snapshot: control state, policy, pending inputs, analysis,
/// and a preview of the next admission decision.
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail
/// or the snapshot cannot be loaded.
pub(crate) async fn console_chat_queue_policy_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    let snapshot =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), false)
            .await?;
    Ok(Json(session_queue_snapshot_json(snapshot)))
}

/// `POST /console/v1/chat/sessions/{session_id}/queue/pause` - pauses queue
/// forwarding for the session and records the operator decision.
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail
/// or the control update fails.
pub(crate) async fn console_chat_queue_pause_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatQueueControlRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let reason = payload
        .reason
        .and_then(trim_to_option)
        .unwrap_or_else(|| "operator_paused_queue".to_owned());
    let control = state
        .runtime
        .upsert_orchestrator_session_queue_control(
            journal::OrchestratorSessionQueueControlUpdateRequest {
                session_id: session_record.session_id.clone(),
                paused: true,
                pause_reason: Some(reason.clone()),
            },
        )
        .await
        .map_err(runtime_status_response)?;
    let snapshot = load_console_session_queue_snapshot(
        &state,
        &session.context,
        session_record.session_id.as_str(),
        false,
    )
    .await?;
    let pending_depth = pending_queue_depth(
        snapshot.queued_inputs.as_slice(),
        Some(snapshot.policy.coalescing_group.as_str()),
    );
    record_session_queue_operator_event(
        &state,
        &session.context,
        session_record.session_id.as_str(),
        snapshot.active_run_id.as_deref(),
        "pause",
        reason.as_str(),
        None,
        pending_depth,
        json!({
            "paused": control.paused,
            "pause_reason": control.pause_reason,
        }),
    )
    .await?;
    Ok(Json(json!({
        "action": "pause",
        "control": control,
        "queue": session_queue_snapshot_json(snapshot),
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/queue/resume` - resumes a
/// paused session queue and records the operator decision.
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail
/// or the control update fails.
pub(crate) async fn console_chat_queue_resume_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let reason = "operator_resumed_queue";
    let control = state
        .runtime
        .upsert_orchestrator_session_queue_control(
            journal::OrchestratorSessionQueueControlUpdateRequest {
                session_id: session_record.session_id.clone(),
                paused: false,
                pause_reason: None,
            },
        )
        .await
        .map_err(runtime_status_response)?;
    let snapshot = load_console_session_queue_snapshot(
        &state,
        &session.context,
        session_record.session_id.as_str(),
        false,
    )
    .await?;
    let pending_depth = pending_queue_depth(
        snapshot.queued_inputs.as_slice(),
        Some(snapshot.policy.coalescing_group.as_str()),
    );
    record_session_queue_operator_event(
        &state,
        &session.context,
        session_record.session_id.as_str(),
        snapshot.active_run_id.as_deref(),
        "resume",
        reason,
        None,
        pending_depth,
        json!({
            "paused": control.paused,
        }),
    )
    .await?;
    Ok(Json(json!({
        "action": "resume",
        "control": control,
        "queue": session_queue_snapshot_json(snapshot),
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/queue/drain` - cancels every
/// pending queued input for the session and records the operator decision.
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail
/// or a queued-input update fails.
pub(crate) async fn console_chat_queue_drain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatQueueControlRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    let snapshot =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), true)
            .await?;
    let reason = payload
        .reason
        .and_then(trim_to_option)
        .unwrap_or_else(|| "queue_drained_by_operator".to_owned());
    let pending_inputs = snapshot.pending_inputs();
    for queued in &pending_inputs {
        let outcome =
            queued_record_outcome(queued, QueuedInputState::Cancelled, false, reason.as_str());
        state
            .runtime
            .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
                queued_input_id: queued.queued_input_id.clone(),
                expected_state: queued.state.clone(),
                expected_revision: queued.lifecycle_revision,
                state: QueuedInputState::Cancelled.as_str().to_owned(),
                claimed_active_generation: None,
                overflow_summary_ref: None,
                decision_reason: Some(reason.clone()),
                explain_json: Some(
                    json!({
                        "decision": "cancel",
                        "reason": reason.as_str(),
                        "queue_mode": queued.queue_mode.as_str(),
                    })
                    .to_string(),
                ),
                queue_outcome_json: Some(serde_json::to_string(&outcome).map_err(|error| {
                    runtime_status_response(tonic::Status::internal(error.to_string()))
                })?),
            })
            .await
            .map_err(runtime_status_response)?;
    }
    state.observability.observe_runtime_queue_depth(0);
    let refreshed =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), false)
            .await?;
    record_session_queue_operator_event(
        &state,
        &session.context,
        session_id.as_str(),
        snapshot.active_run_id.as_deref(),
        "drain",
        reason.as_str(),
        None,
        0,
        json!({
            "drained_count": pending_inputs.len(),
        }),
    )
    .await?;
    Ok(Json(json!({
        "action": "drain",
        "drained_count": pending_inputs.len(),
        "queue": session_queue_snapshot_json(refreshed),
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/queue/items/{queued_input_id}/cancel`
/// - cancels one pending queued input.
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail,
/// the input is unknown, or it is no longer pending.
pub(crate) async fn console_chat_queue_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, queued_input_id)): Path<(String, String)>,
    Json(payload): Json<ConsoleChatQueueControlRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    validate_canonical_id(queued_input_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "queued_input_id must be a canonical ULID",
        ))
    })?;
    let snapshot =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), true)
            .await?;
    let queued = snapshot
        .queued_inputs
        .iter()
        .find(|queued| queued.queued_input_id == queued_input_id)
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "queued input not found: {queued_input_id}"
            )))
        })?;
    if !matches!(
        QueuedInputState::parse(queued.state.as_str()),
        Some(QueuedInputState::Pending | QueuedInputState::Deferred)
    ) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "only pending queued inputs can be cancelled",
        )));
    }
    let reason = payload
        .reason
        .and_then(trim_to_option)
        .unwrap_or_else(|| "queued_input_cancelled_by_operator".to_owned());
    let outcome =
        queued_record_outcome(queued, QueuedInputState::Cancelled, false, reason.as_str());
    state
        .runtime
        .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
            queued_input_id: queued_input_id.clone(),
            expected_state: queued.state.clone(),
            expected_revision: queued.lifecycle_revision,
            state: QueuedInputState::Cancelled.as_str().to_owned(),
            claimed_active_generation: None,
            overflow_summary_ref: None,
            decision_reason: Some(reason.clone()),
            explain_json: Some(
                json!({
                    "decision": "cancel",
                    "reason": reason.as_str(),
                    "queue_mode": queued.queue_mode.as_str(),
                })
                .to_string(),
            ),
            queue_outcome_json: Some(serde_json::to_string(&outcome).map_err(|error| {
                runtime_status_response(tonic::Status::internal(error.to_string()))
            })?),
        })
        .await
        .map_err(runtime_status_response)?;
    let remaining_depth = snapshot.pending_inputs().len().saturating_sub(1);
    record_session_queue_operator_event(
        &state,
        &session.context,
        session_id.as_str(),
        snapshot.active_run_id.as_deref(),
        "cancel",
        reason.as_str(),
        Some(queued_input_id.as_str()),
        remaining_depth,
        json!({
            "queued_input_state": QueuedInputState::Cancelled.as_str(),
        }),
    )
    .await?;
    let refreshed =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), false)
            .await?;
    Ok(Json(json!({
        "action": "cancel",
        "queued_input_id": queued_input_id,
        "queue": session_queue_snapshot_json(refreshed),
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/queue/items/{queued_input_id}/reject`
/// - rejects one pending queued input (terminal, distinct from cancel for
///   audit purposes).
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail,
/// the input is unknown, or it is no longer pending.
pub(crate) async fn console_chat_queue_reject_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, queued_input_id)): Path<(String, String)>,
    Json(payload): Json<ConsoleChatQueueControlRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    validate_canonical_id(queued_input_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "queued_input_id must be a canonical ULID",
        ))
    })?;
    let snapshot =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), true)
            .await?;
    let queued = snapshot
        .queued_inputs
        .iter()
        .find(|queued| queued.queued_input_id == queued_input_id)
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "queued input not found: {queued_input_id}"
            )))
        })?;
    if !matches!(
        QueuedInputState::parse(queued.state.as_str()),
        Some(QueuedInputState::Pending | QueuedInputState::Deferred)
    ) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "only pending queued inputs can be rejected",
        )));
    }
    let reason = payload
        .reason
        .and_then(trim_to_option)
        .unwrap_or_else(|| "queued_input_rejected_by_operator".to_owned());
    let outcome = queued_record_outcome(queued, QueuedInputState::Rejected, false, reason.as_str());
    state
        .runtime
        .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
            queued_input_id: queued_input_id.clone(),
            expected_state: queued.state.clone(),
            expected_revision: queued.lifecycle_revision,
            state: QueuedInputState::Rejected.as_str().to_owned(),
            claimed_active_generation: None,
            overflow_summary_ref: None,
            decision_reason: Some(reason.clone()),
            explain_json: Some(
                json!({
                    "decision": "reject",
                    "reason": reason.as_str(),
                    "queue_mode": queued.queue_mode.as_str(),
                })
                .to_string(),
            ),
            queue_outcome_json: Some(serde_json::to_string(&outcome).map_err(|error| {
                runtime_status_response(tonic::Status::internal(error.to_string()))
            })?),
        })
        .await
        .map_err(runtime_status_response)?;
    let remaining_depth = snapshot.pending_inputs().len().saturating_sub(1);
    record_session_queue_operator_event(
        &state,
        &session.context,
        session_id.as_str(),
        snapshot.active_run_id.as_deref(),
        "reject",
        reason.as_str(),
        Some(queued_input_id.as_str()),
        remaining_depth,
        json!({
            "queued_input_state": QueuedInputState::Rejected.as_str(),
        }),
    )
    .await?;
    let refreshed =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), false)
            .await?;
    Ok(Json(json!({
        "action": "reject",
        "queued_input_id": queued_input_id,
        "queue": session_queue_snapshot_json(refreshed),
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/queue/items/{queued_input_id}/prioritize`
/// - moves one pending queued input into a different priority lane.
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail,
/// the lane name is invalid, the input is unknown, or it is no longer
/// pending.
pub(crate) async fn console_chat_queue_prioritize_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, queued_input_id)): Path<(String, String)>,
    Json(payload): Json<ConsoleChatQueueControlRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    validate_canonical_id(queued_input_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "queued_input_id must be a canonical ULID",
        ))
    })?;
    let priority_lane = normalize_priority_lane(payload.priority_lane)?;
    let reason = payload
        .reason
        .and_then(trim_to_option)
        .unwrap_or_else(|| "queued_input_prioritized_by_operator".to_owned());
    let snapshot =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), true)
            .await?;
    let queued = snapshot
        .queued_inputs
        .iter()
        .find(|queued| queued.queued_input_id == queued_input_id)
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "queued input not found: {queued_input_id}"
            )))
        })?;
    let previous_priority_lane = queued.priority_lane.clone();
    let queued_queue_mode = queued.queue_mode.clone();
    let steering_decision = state
        .runtime
        .steer_orchestrator_queued_input(
            session_id.clone(),
            queued_input_id.clone(),
            QueueSteeringRequest {
                actor_principal: session.context.principal.clone(),
                requested_priority_lane: priority_lane.clone(),
                reason: Some(reason.clone()),
            },
        )
        .await
        .map_err(runtime_status_response)?;
    if !steering_decision.accepted {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "queued input priority rejected: {}",
            steering_decision.reason_code
        ))));
    }
    record_session_queue_operator_event(
        &state,
        &session.context,
        session_id.as_str(),
        snapshot.active_run_id.as_deref(),
        "prioritize",
        reason.as_str(),
        Some(queued_input_id.as_str()),
        snapshot.pending_inputs().len(),
        json!({
            "priority_lane": priority_lane.as_str(),
            "previous_priority_lane": previous_priority_lane.as_str(),
            "queue_mode": queued_queue_mode.as_str(),
            "queue_steering": {
                "action": steering_decision.action.as_str(),
                "reason_code": steering_decision.reason_code.as_str(),
            },
        }),
    )
    .await?;
    let refreshed =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), false)
            .await?;
    Ok(Json(json!({
        "action": "prioritize",
        "queued_input_id": queued_input_id,
        "priority_lane": priority_lane,
        "queue_steering": steering_decision,
        "queue": session_queue_snapshot_json(refreshed),
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/queue/collect-summary` -
/// forces all pending queued inputs to be merged into a single collect
/// summary input.
///
/// # Errors
/// Returns an error `Response` when authorization or capability checks fail,
/// the queue has no pending inputs, or persistence fails.
pub(crate) async fn console_chat_queue_collect_summary_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleChatQueueControlRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_console_runtime_preview_capability(
        &state,
        RuntimePreviewCapability::SessionQueuePolicy,
    )?;
    let snapshot =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), true)
            .await?;
    let pending_inputs = snapshot.pending_inputs();
    if pending_inputs.is_empty() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "queue has no pending inputs to summarize",
        )));
    }
    let timestamp_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let reason = payload
        .reason
        .and_then(trim_to_option)
        .unwrap_or_else(|| "operator_forced_collect_summary".to_owned());
    let queued_input_id = Ulid::generate().to_string();
    let summary_ref = format!("queue-summary:{queued_input_id}");
    let collect_summary = build_queue_collect_summary(
        summary_ref.clone(),
        pending_inputs.as_slice(),
        reason.as_str(),
    );
    let run_id = pending_inputs.first().map(|queued| queued.run_id.clone()).ok_or_else(|| {
        runtime_status_response(tonic::Status::internal("pending queue vanished"))
    })?;
    let expected_active_generation =
        pending_inputs.first().and_then(|queued| queued.expected_active_generation);
    let explain_json = json!({
        "decision": "merge",
        "mode": QueueMode::Collect.as_str(),
        "accepted": true,
        "reason": reason.as_str(),
        "safe_boundary": &snapshot.safe_boundary,
        "policy": snapshot.policy.snapshot_json(),
        "summary": collect_summary.provenance_json,
    });
    let summary_outcome = queue_outcome(
        queued_input_id.clone(),
        QueuedInputState::Pending,
        QueuedInputDeliveryBoundary::BacklogSummary,
        expected_active_generation.and_then(|value| u64::try_from(value).ok()),
        expected_active_generation.and_then(|value| u64::try_from(value).ok()),
        true,
        reason.clone(),
    );
    let summary_attachments_json = collect_summary.attachment_refs_json.to_string();
    let summary_input = state
        .runtime
        .create_orchestrator_queued_input(journal::OrchestratorQueuedInputCreateRequest {
            queued_input_id: queued_input_id.clone(),
            run_id: run_id.clone(),
            session_id: snapshot.session_record.session_id.clone(),
            state: QueuedInputState::Pending.as_str().to_owned(),
            text: collect_summary.text,
            origin_run_id: Some(run_id),
            queue_mode: QueueMode::Collect.as_str().to_owned(),
            delivery_boundary: QueuedInputDeliveryBoundary::BacklogSummary.as_str().to_owned(),
            expected_active_generation,
            priority_lane: snapshot.policy.priority_lane.clone(),
            coalescing_group: Some(snapshot.policy.coalescing_group.clone()),
            overflow_summary_ref: Some(summary_ref.clone()),
            safe_boundary_flags_json: serde_json::to_string(&snapshot.safe_boundary)
                .unwrap_or_else(|_| "{}".to_owned()),
            decision_reason: reason.clone(),
            attachments_json: summary_attachments_json,
            queue_outcome_json: serde_json::to_string(&summary_outcome).map_err(|error| {
                runtime_status_response(tonic::Status::internal(error.to_string()))
            })?,
            accepted_at_unix_ms: Some(timestamp_unix_ms),
            policy_snapshot_json: snapshot.policy.snapshot_json().to_string(),
            explain_json: explain_json.to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    for queued in &pending_inputs {
        let outcome =
            queued_record_outcome(queued, QueuedInputState::Merged, false, reason.as_str());
        state
            .runtime
            .update_orchestrator_queued_input_state(journal::OrchestratorQueuedInputUpdateRequest {
                queued_input_id: queued.queued_input_id.clone(),
                expected_state: queued.state.clone(),
                expected_revision: queued.lifecycle_revision,
                state: QueuedInputState::Merged.as_str().to_owned(),
                claimed_active_generation: None,
                overflow_summary_ref: Some(summary_ref.clone()),
                decision_reason: Some(reason.clone()),
                explain_json: Some(explain_json.to_string()),
                queue_outcome_json: Some(serde_json::to_string(&outcome).map_err(|error| {
                    runtime_status_response(tonic::Status::internal(error.to_string()))
                })?),
            })
            .await
            .map_err(runtime_status_response)?;
    }
    let refreshed =
        load_console_session_queue_snapshot(&state, &session.context, session_id.as_str(), false)
            .await?;
    record_session_queue_operator_event(
        &state,
        &session.context,
        session_id.as_str(),
        snapshot.active_run_id.as_deref(),
        "collect_summary",
        reason.as_str(),
        Some(summary_input.queued_input_id.as_str()),
        pending_queue_depth(
            refreshed.queued_inputs.as_slice(),
            Some(refreshed.policy.coalescing_group.as_str()),
        ),
        json!({
            "summary_ref": summary_ref,
            "merged_count": pending_inputs.len(),
        }),
    )
    .await?;
    Ok(Json(json!({
        "action": "collect_summary",
        "queued_input": summary_input,
        "merged_count": pending_inputs.len(),
        "queue": session_queue_snapshot_json(refreshed),
        "contract": contract_descriptor(),
    })))
}

// --- Transcript, canvases, export, and pins ---

/// `GET /console/v1/chat/sessions/{session_id}/transcript` - returns the full
/// session view used to hydrate the chat UI: transcript records plus
/// attachments, derived artifacts, pins, compactions, checkpoints, queued
/// inputs, runs, and background tasks.
///
/// # Errors
/// Returns an error `Response` when authorization fails or any of the
/// underlying listings fail.
pub(crate) async fn console_chat_transcript_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), false).await?;
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
    let compactions = state
        .runtime
        .list_orchestrator_compaction_artifacts(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let checkpoints = state
        .runtime
        .list_orchestrator_checkpoints(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let queued_inputs = state
        .runtime
        .list_orchestrator_queued_inputs(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let background_tasks = state
        .runtime
        .list_orchestrator_background_tasks(journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: Some(session.context.device_id.clone()),
            channel: session.context.channel.clone(),
            session_id: Some(session_record.session_id.clone()),
            include_completed: true,
            limit: 64,
        })
        .await
        .map_err(runtime_status_response)?;
    let subagent_records = super::sessions::load_session_subagent_records(
        &state,
        &session.context,
        session_record.session_id.as_str(),
    )
    .await?;
    let runs = state
        .runtime
        .list_orchestrator_session_runs(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let attachments = state
        .channels
        .list_console_chat_attachments(
            session_record.session_id.as_str(),
            session.context.principal.as_str(),
            session.context.device_id.as_str(),
            session.context.channel.as_deref(),
        )
        .map_err(channel_platform_error_response)?;
    let derived_artifacts = state
        .channels
        .list_console_chat_derived_artifacts(
            session_record.session_id.as_str(),
            session.context.principal.as_str(),
            session.context.device_id.as_str(),
            session.context.channel.as_deref(),
        )
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "session": session_record,
        "records": transcript,
        "attachments": attachments
            .iter()
            .map(console_chat_attachment_payload_to_json)
            .collect::<Vec<_>>(),
        "derived_artifacts": derived_artifacts,
        "pins": pins,
        "compactions": compactions,
        "checkpoints": checkpoints,
        "queued_inputs": queued_inputs,
        "runs": runs,
        "background_tasks": background_tasks,
        "subagent_records": subagent_records,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/sessions/{session_id}/canvases` - lists the
/// session's canvases with transcript provenance.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the canvas or
/// transcript listing fails.
pub(crate) async fn console_chat_canvas_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), false).await?;
    let transcript = state
        .runtime
        .list_orchestrator_session_transcript(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let canvases = load_console_chat_canvas_summaries(
        &state,
        &session.context,
        session_record.session_id.as_str(),
        transcript.as_slice(),
    )
    .map_err(|response| *response)?;
    Ok(Json(json!({
        "session": session_record,
        "canvases": canvases,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/sessions/{session_id}/canvases/{canvas_id}` -
/// returns one canvas: summary, current state JSON, patch history, and a
/// runtime frame descriptor when the canvas host is available.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the canvas belongs
/// to a different session, or persisted state cannot be decoded.
pub(crate) async fn console_chat_canvas_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, canvas_id)): Path<(String, String)>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), false).await?;
    let canvas = load_console_chat_canvas(
        &state,
        &session.context,
        session_record.session_id.as_str(),
        canvas_id.as_str(),
    )
    .await?;
    let transcript = state
        .runtime
        .list_orchestrator_session_transcript(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let reference =
        derive_canvas_transcript_reference(transcript.as_slice(), canvas.canvas_id.as_str());
    let summary = build_console_chat_canvas_summary(&canvas, reference);
    let state_payload =
        serde_json::from_slice::<Value>(canvas.state_json.as_slice()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "persisted canvas state JSON is invalid: {error}"
            )))
        })?;
    let revisions = state
        .runtime
        .load_canvas_patch_history(canvas.canvas_id.as_str())
        .map_err(runtime_status_response)?;
    let (runtime_descriptor, runtime_error) = resolve_console_chat_canvas_runtime_descriptor(
        &state,
        &session.context,
        canvas.canvas_id.as_str(),
    )
    .map_err(|response| *response)?;
    Ok(Json(json!({
        "session": session_record,
        "canvas": summary,
        "runtime": runtime_descriptor,
        "runtime_error": runtime_error,
        "state": state_payload,
        "revisions": revisions,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/chat/sessions/{session_id}/canvases/{canvas_id}/restore`
/// - restores a canvas to an earlier `state_version` and journals the
///   restore.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the canvas belongs
/// to a different session, the version is unknown, or the restore fails.
pub(crate) async fn console_chat_canvas_restore_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, canvas_id)): Path<(String, String)>,
    Json(payload): Json<ConsoleChatCanvasRestoreRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let current = load_console_chat_canvas(
        &state,
        &session.context,
        session_record.session_id.as_str(),
        canvas_id.as_str(),
    )
    .await?;
    let restored = state
        .runtime
        .restore_canvas_state(&session.context, current.canvas_id.as_str(), payload.state_version)
        .map_err(runtime_status_response)?;
    let transcript = state
        .runtime
        .list_orchestrator_session_transcript(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let reference =
        derive_canvas_transcript_reference(transcript.as_slice(), restored.canvas_id.as_str());
    let summary = build_console_chat_canvas_summary(&restored, reference.clone());
    let state_payload =
        serde_json::from_slice::<Value>(restored.state_json.as_slice()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "persisted canvas state JSON is invalid: {error}"
            )))
        })?;
    let revisions = state
        .runtime
        .load_canvas_patch_history(restored.canvas_id.as_str())
        .map_err(runtime_status_response)?;
    let (runtime_descriptor, runtime_error) = resolve_console_chat_canvas_runtime_descriptor(
        &state,
        &session.context,
        restored.canvas_id.as_str(),
    )
    .map_err(|response| *response)?;
    let _ = crate::gateway::record_agent_journal_event(
        &state.runtime,
        &session.context,
        json!({
            "event": "canvas.restore.completed",
            "session_id": session_record.session_id,
            "canvas_id": restored.canvas_id,
            "previous_state_version": current.state_version,
            "restored_from_state_version": payload.state_version,
            "restored_to_state_version": restored.state_version,
            "source_run_id": reference.source_run_id,
            "runtime_available": runtime_descriptor.is_some(),
        }),
    )
    .await;
    Ok(Json(json!({
        "session": session_record,
        "canvas": summary,
        "runtime": runtime_descriptor,
        "runtime_error": runtime_error,
        "state": state_payload,
        "revisions": revisions,
        "restored_from_state_version": payload.state_version,
        "previous_state_version": current.state_version,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/sessions/{session_id}/transcript/search` - performs
/// a case-insensitive substring search over the textual transcript events.
///
/// # Errors
/// Returns an error `Response` when authorization fails, `q` is empty, or the
/// transcript listing fails.
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

/// `GET /console/v1/chat/sessions/{session_id}/export` - exports the session
/// as structured JSON (default) or a rendered Markdown document
/// (`format=markdown`).
///
/// # Errors
/// Returns an error `Response` when authorization fails or any underlying
/// listing fails.
pub(crate) async fn console_chat_export_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleChatTranscriptExportQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), false).await?;
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
            "content": render_session_export_markdown(
                &session_record,
                transcript.as_slice(),
                pins.as_slice(),
                state
                    .runtime
                    .list_orchestrator_compaction_artifacts(session_record.session_id.clone())
                    .await
                    .map_err(runtime_status_response)?
                    .as_slice(),
                state
                    .runtime
                    .list_orchestrator_checkpoints(session_record.session_id.clone())
                    .await
                    .map_err(runtime_status_response)?
                    .as_slice(),
            ),
            "contract": contract_descriptor(),
        })));
    }
    let compactions = state
        .runtime
        .list_orchestrator_compaction_artifacts(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let checkpoints = state
        .runtime
        .list_orchestrator_checkpoints(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let background_tasks = state
        .runtime
        .list_orchestrator_background_tasks(journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: Some(session.context.device_id.clone()),
            channel: session.context.channel.clone(),
            session_id: Some(session_record.session_id.clone()),
            include_completed: true,
            limit: 64,
        })
        .await
        .map_err(runtime_status_response)?;
    let subagent_records = super::sessions::load_session_subagent_records(
        &state,
        &session.context,
        session_record.session_id.as_str(),
    )
    .await?;
    Ok(Json(json!({
        "format": "json",
        "content": {
            "session": session_record,
            "records": transcript,
            "pins": pins,
            "compactions": compactions,
            "checkpoints": checkpoints,
            "background_tasks": background_tasks,
            "subagent_records": subagent_records,
        },
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/chat/sessions/{session_id}/pins` - lists the session's
/// transcript pins.
///
/// # Errors
/// Returns an error `Response` when authorization fails or the listing fails.
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

/// `POST /console/v1/chat/sessions/{session_id}/pins` - pins a tape position
/// (run id + sequence) with a title and optional note.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the run id is
/// malformed, the title is empty, or persistence fails.
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
            pin_id: Ulid::generate().to_string(),
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

/// `POST /console/v1/chat/sessions/{session_id}/pins/{pin_id}` - deletes a
/// transcript pin; `deleted` reports whether it existed.
///
/// # Errors
/// Returns an error `Response` when authorization fails, the session is not
/// visible to this context, or the delete fails.
pub(crate) async fn console_chat_pin_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, pin_id)): Path<(String, String)>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_record =
        load_console_chat_session(&state, &session.context, session_id.as_str(), true).await?;
    let deleted = state
        .runtime
        .delete_orchestrator_session_pin(session_record.session_id, pin_id)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "deleted": deleted,
        "contract": contract_descriptor(),
    })))
}

// --- Authorization and loader helpers ---

/// Returns whether a run belongs to the console context: principal, device,
/// and channel must all match (channel `None` only matches `None`).
pub(super) fn run_matches_console_context(
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

/// Resolves attachment references into protocol `MessageAttachment`s with
/// inline bytes, enforcing session/context ownership per artifact.
///
/// The error is boxed to keep the `Result` small (`clippy::result_large_err`).
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

/// Injects an `attachment_recall` block (query-relevant derived chunks) into
/// the parameter delta when the message references attachments. A non-object
/// delta is preserved under `prior_parameter_delta` rather than overwritten.
fn build_console_attachment_parameter_delta(
    state: &AppState,
    parameter_delta: Option<&Value>,
    query_text: &str,
    attachments: &[common_v1::MessageAttachment],
) -> Result<Option<Value>, Box<Response>> {
    let artifact_ids = attachments
        .iter()
        .filter_map(|attachment| attachment.artifact_id.as_ref().map(|value| value.ulid.clone()))
        .collect::<Vec<_>>();
    if artifact_ids.is_empty() {
        return Ok(parameter_delta.cloned());
    }
    let trimmed_query = query_text.trim();
    if trimmed_query.is_empty() {
        return Ok(parameter_delta.cloned());
    }
    let selected_chunks = state
        .channels
        .select_console_chat_derived_chunks(artifact_ids.as_slice(), trimmed_query, Some(1_600))
        .map_err(|error| Box::new(channel_platform_error_response(error)))?;
    if selected_chunks.is_empty() {
        return Ok(parameter_delta.cloned());
    }
    let mut next_delta = parameter_delta.cloned().unwrap_or_else(|| json!({}));
    if !next_delta.is_object() {
        next_delta = json!({ "prior_parameter_delta": next_delta });
    }
    if let Some(object) = next_delta.as_object_mut() {
        object.insert(
            "attachment_recall".to_owned(),
            json!({
                "query": trimmed_query,
                "source_artifact_ids": artifact_ids,
                "chunks": selected_chunks,
            }),
        );
    }
    Ok(Some(next_delta))
}

/// Injects the `project_context` preview into the parameter delta when the
/// session has project context entries or warnings.
async fn build_console_project_context_parameter_delta(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    text: &str,
    parameter_delta: Option<Value>,
) -> Result<Option<Value>, tonic::Status> {
    let preview = crate::application::project_context::preview_project_context(
        &state.runtime,
        context,
        session_id,
        text,
        true,
    )
    .await?;
    if preview.entries.is_empty() && preview.warnings.is_empty() {
        return Ok(parameter_delta);
    }
    let mut next_delta = parameter_delta.unwrap_or_else(|| json!({}));
    if !next_delta.is_object() {
        next_delta = json!({ "prior_parameter_delta": next_delta });
    }
    if let Some(object) = next_delta.as_object_mut() {
        let preview_value = serde_json::to_value(&preview).map_err(|error| {
            tonic::Status::internal(format!("failed to encode project context preview: {error}"))
        })?;
        object.insert("project_context".to_owned(), preview_value);
    }
    Ok(Some(next_delta))
}

/// Injects resolved `@`-style `context_references` into the parameter delta;
/// any reference resolution error rejects the whole message so the user can
/// fix the reference instead of silently dropping it.
async fn build_console_context_reference_parameter_delta(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    text: &str,
    parameter_delta: Option<Value>,
) -> Result<Option<Value>, tonic::Status> {
    let preview = crate::application::context_references::preview_context_references(
        &state.runtime,
        context,
        session_id,
        text,
    )
    .await?;
    if !preview.errors.is_empty() {
        return Err(tonic::Status::invalid_argument(preview.errors[0].message.clone()));
    }
    if preview.references.is_empty() {
        return Ok(parameter_delta);
    }
    let mut next_delta = parameter_delta.unwrap_or_else(|| json!({}));
    if !next_delta.is_object() {
        next_delta = json!({ "prior_parameter_delta": next_delta });
    }
    if let Some(object) = next_delta.as_object_mut() {
        object.insert("context_references".to_owned(), json!(preview));
    }
    Ok(Some(next_delta))
}

/// Ownership check for derived artifacts. `require_device_match` is true for
/// detail/lifecycle endpoints (attachment content stays device-scoped) and
/// false for cross-device listings.
fn derived_artifact_matches_console_context(
    record: &media::MediaDerivedArtifactRecord,
    context: &gateway::RequestContext,
    require_device_match: bool,
) -> bool {
    if record.principal.as_deref() != Some(context.principal.as_str()) {
        return false;
    }
    if record.channel.as_deref() != context.channel.as_deref() {
        return false;
    }
    if require_device_match {
        return record.device_id.as_deref() == Some(context.device_id.as_str());
    }
    true
}

fn filter_console_derived_artifact_records(
    records: Vec<media::MediaDerivedArtifactRecord>,
    context: &gateway::RequestContext,
    require_device_match: bool,
) -> Vec<media::MediaDerivedArtifactRecord> {
    records
        .into_iter()
        .filter(|record| {
            derived_artifact_matches_console_context(record, context, require_device_match)
        })
        .collect()
}

/// Loads a run snapshot after validating the id and console ownership.
async fn load_console_chat_run(
    state: &AppState,
    context: &gateway::RequestContext,
    run_id: &str,
) -> Result<journal::OrchestratorRunStatusSnapshot, Response> {
    validate_canonical_id(run_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    let run = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {run_id}"
            )))
        })?;
    if !run_matches_console_context(&run, context) {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "chat run does not belong to the authenticated console session context",
        )));
    }
    Ok(run)
}

/// Loads a workspace checkpoint and authorizes it by loading its owning
/// session in the caller's context.
async fn load_console_workspace_checkpoint(
    state: &AppState,
    context: &gateway::RequestContext,
    checkpoint_id: &str,
    require_write: bool,
) -> Result<journal::WorkspaceCheckpointRecord, Response> {
    validate_canonical_id(checkpoint_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "checkpoint_id must be a canonical ULID",
        ))
    })?;
    let checkpoint = state
        .runtime
        .get_workspace_checkpoint(checkpoint_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "workspace checkpoint not found: {checkpoint_id}"
            )))
        })?;
    let _session =
        load_console_chat_session(state, context, checkpoint.session_id.as_str(), require_write)
            .await?;
    Ok(checkpoint)
}

/// Parses one side of a workspace compare request into a run or checkpoint
/// anchor; exactly one of the two ids must be set.
fn parse_workspace_compare_anchor(
    run_id: Option<String>,
    checkpoint_id: Option<String>,
    side: &str,
) -> Result<crate::application::workspace_observability::WorkspaceCompareAnchor, tonic::Status> {
    let run_id = run_id.and_then(trim_to_option);
    let checkpoint_id = checkpoint_id.and_then(trim_to_option);
    match (run_id, checkpoint_id) {
        (Some(run_id), None) => {
            validate_canonical_id(run_id.as_str()).map_err(|_| {
                tonic::Status::invalid_argument(format!("{side}_run_id must be a canonical ULID"))
            })?;
            Ok(crate::application::workspace_observability::WorkspaceCompareAnchor::Run(run_id))
        }
        (None, Some(checkpoint_id)) => {
            validate_canonical_id(checkpoint_id.as_str()).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "{side}_checkpoint_id must be a canonical ULID"
                ))
            })?;
            Ok(crate::application::workspace_observability::WorkspaceCompareAnchor::Checkpoint(
                checkpoint_id,
            ))
        }
        (Some(_), Some(_)) => Err(tonic::Status::invalid_argument(format!(
            "{side} compare anchor must set only one of {side}_run_id or {side}_checkpoint_id"
        ))),
        (None, None) => Err(tonic::Status::invalid_argument(format!(
            "{side} compare anchor requires {side}_run_id or {side}_checkpoint_id"
        ))),
    }
}

/// Authorizes a compare anchor by loading the referenced run or checkpoint
/// in the caller's context.
async fn authorize_workspace_compare_anchor(
    state: &AppState,
    context: &gateway::RequestContext,
    anchor: &crate::application::workspace_observability::WorkspaceCompareAnchor,
) -> Result<(), Response> {
    match anchor {
        crate::application::workspace_observability::WorkspaceCompareAnchor::Run(run_id) => {
            let _ = load_console_chat_run(state, context, run_id.as_str()).await?;
        }
        crate::application::workspace_observability::WorkspaceCompareAnchor::Checkpoint(
            checkpoint_id,
        ) => {
            let _ =
                load_console_workspace_checkpoint(state, context, checkpoint_id.as_str(), false)
                    .await?;
        }
    }
    Ok(())
}

/// Loads a session after validating the id and console ownership.
///
/// Read paths use a cheap snapshot lookup with an explicit
/// principal/device/channel check; write paths go through session resolution
/// (`require_existing`) so the runtime applies its own ownership and
/// liveness rules before the handler mutates anything.
async fn load_console_chat_session(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    require_write: bool,
) -> Result<journal::OrchestratorSessionRecord, Response> {
    validate_canonical_id(session_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    if !require_write {
        let session_record = state
            .runtime
            .orchestrator_session_by_id_snapshot(session_id.to_owned())
            .await
            .map_err(runtime_status_response)?
            .ok_or_else(|| {
                runtime_status_response(tonic::Status::not_found(format!(
                    "session not found: {session_id}"
                )))
            })?;
        if session_record.principal != context.principal
            || session_record.device_id != context.device_id
            || session_record.channel != context.channel
        {
            return Err(runtime_status_response(tonic::Status::permission_denied(
                "session belongs to a different principal, device, or channel",
            )));
        }
        return Ok(session_record);
    }

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

/// Consistent view of a session's queue used by the queue handlers: session
/// record, queued inputs, operator control, effective policy, and the safe
/// boundary derived from the live stream registry.
#[derive(Debug, Clone)]
struct ConsoleSessionQueueSnapshot {
    session_record: journal::OrchestratorSessionRecord,
    queued_inputs: Vec<journal::OrchestratorQueuedInputRecord>,
    control: journal::OrchestratorSessionQueueControlRecord,
    policy: SessionQueuePolicy,
    safe_boundary: SessionQueueSafeBoundary,
    active_run_id: Option<String>,
}

impl ConsoleSessionQueueSnapshot {
    fn pending_inputs(&self) -> Vec<journal::OrchestratorQueuedInputRecord> {
        self.queued_inputs
            .iter()
            .filter(|queued| {
                matches!(
                    QueuedInputState::parse(queued.state.as_str()),
                    Some(QueuedInputState::Pending | QueuedInputState::Deferred)
                )
            })
            .cloned()
            .collect()
    }
}

fn queued_record_outcome(
    queued: &journal::OrchestratorQueuedInputRecord,
    lifecycle_state: QueuedInputState,
    accepted: bool,
    reason_code: &str,
) -> palyra_common::runtime_contracts::QueueOutcome {
    let delivery_boundary = QueuedInputDeliveryBoundary::parse(queued.delivery_boundary.as_str())
        .unwrap_or_else(|| {
            match QueueMode::parse(queued.queue_mode.as_str()).unwrap_or(QueueMode::Followup) {
                QueueMode::Followup => QueuedInputDeliveryBoundary::NextTurn,
                QueueMode::Steer => QueuedInputDeliveryBoundary::CurrentRunBeforeProvider,
                QueueMode::Interrupt => QueuedInputDeliveryBoundary::CancelThenNextTurn,
                QueueMode::Collect | QueueMode::SteerBacklog => {
                    QueuedInputDeliveryBoundary::BacklogSummary
                }
            }
        });
    let expected_generation =
        queued.expected_active_generation.and_then(|value| u64::try_from(value).ok());
    let observed_generation = queued
        .claimed_active_generation
        .and_then(|value| u64::try_from(value).ok())
        .or(expected_generation);
    queue_outcome(
        queued.queued_input_id.clone(),
        lifecycle_state,
        delivery_boundary,
        expected_generation,
        observed_generation,
        accepted,
        reason_code,
    )
}

async fn load_console_session_queue_snapshot(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    require_write: bool,
) -> Result<ConsoleSessionQueueSnapshot, Response> {
    let session_record =
        load_console_chat_session(state, context, session_id, require_write).await?;
    let queued_inputs = state
        .runtime
        .list_orchestrator_queued_inputs(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let control = state
        .runtime
        .get_orchestrator_session_queue_control(session_record.session_id.clone())
        .await
        .map_err(runtime_status_response)?
        .unwrap_or_else(|| default_session_queue_control(session_record.session_id.clone()));
    let policy = SessionQueuePolicy::from_config(
        &state.runtime.config.session_queue_policy,
        session_record.session_id.as_str(),
        context.channel.as_deref(),
        None,
    );
    let (active_run_stream, pending_approval, active_run_id) =
        active_session_queue_boundary(state, session_record.session_id.as_str());
    let safe_boundary = SessionQueueSafeBoundary::active(active_run_stream, pending_approval);
    Ok(ConsoleSessionQueueSnapshot {
        session_record,
        queued_inputs,
        control,
        policy,
        safe_boundary,
        active_run_id,
    })
}

/// Returns (active stream exists, approval pending, active run id) for a
/// session by inspecting the in-process run-stream registry.
pub(super) fn active_session_queue_boundary(
    state: &AppState,
    session_id: &str,
) -> (bool, bool, Option<String>) {
    let active_stream = {
        let streams = lock_console_chat_streams(&state.console_chat_streams);
        streams
            .iter()
            .find(|(_, stream)| stream.session_id == session_id)
            .map(|(run_id, stream)| (run_id.clone(), stream.clone()))
    };
    let Some((run_id, stream)) = active_stream else {
        return (false, false, None);
    };
    let pending_approval =
        !lock_console_chat_pending_approvals(&stream.pending_approvals).is_empty();
    (true, pending_approval, Some(run_id))
}

fn default_session_queue_control(
    session_id: String,
) -> journal::OrchestratorSessionQueueControlRecord {
    journal::OrchestratorSessionQueueControlRecord {
        session_id,
        paused: false,
        pause_reason: None,
        updated_at_unix_ms: 0,
    }
}

/// Records a `QueueControl` runtime-decision event for an operator queue
/// action (pause/resume/drain/cancel/reject/prioritize/collect_summary).
#[allow(clippy::too_many_arguments)]
async fn record_session_queue_operator_event(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    run_id: Option<&str>,
    action: &str,
    reason: &str,
    queued_input_id: Option<&str>,
    queue_depth: usize,
    details: Value,
) -> Result<(), Response> {
    let mut payload = RuntimeDecisionPayload::new(
        RuntimeDecisionEventType::QueueControl,
        state
            .runtime
            .runtime_decision_actor_from_context(context, RuntimeDecisionActorKind::Operator),
        reason,
        "session_queue.operator_control.v1",
        RuntimeDecisionTiming::observed(crate::gateway::current_unix_ms()),
    )
    .with_input(RuntimeEntityRef::new("session", "session", session_id).with_state(action))
    .with_resource_budget(RuntimeResourceBudget {
        queue_depth: Some(queue_depth as u64),
        token_budget: None,
        pruning_token_delta: None,
        retrieval_branch_latency_ms: None,
        retry_count: None,
        suppression_count: None,
    })
    .with_details(json!({
        "action": action,
        "queued_input_id": queued_input_id,
        "details": details,
    }));
    if let Some(run_id) = run_id {
        payload =
            payload.with_output(RuntimeEntityRef::new("run", "run", run_id).with_state("active"));
    }
    if let Some(queued_input_id) = queued_input_id {
        payload = payload.with_related_entity(RuntimeEntityRef::new(
            "queued_input",
            "queued_input",
            queued_input_id,
        ));
    }
    state
        .runtime
        .record_runtime_decision_event(context, Some(session_id), run_id, payload)
        .await
        .map_err(runtime_status_response)
}

/// Validates an operator-supplied priority lane name (1..=32 chars of ASCII
/// alphanumerics, `_`, `-`), defaulting to `operator_priority`.
#[allow(clippy::result_large_err)]
fn normalize_priority_lane(raw: Option<String>) -> Result<String, Response> {
    let lane = raw.and_then(trim_to_option).unwrap_or_else(|| "operator_priority".to_owned());
    if lane.len() > 32
        || !lane
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "priority_lane must be 1..=32 ASCII letters, digits, '_' or '-'",
        )));
    }
    Ok(lane)
}

/// Serializes a queue snapshot to the wire shape shared by all queue
/// endpoints, including analysis metrics and a preview admission decision.
fn session_queue_snapshot_json(snapshot: ConsoleSessionQueueSnapshot) -> Value {
    let pending_depth = pending_queue_depth(
        snapshot.queued_inputs.as_slice(),
        Some(snapshot.policy.coalescing_group.as_str()),
    );
    let analysis = analyze_session_queue(
        snapshot.queued_inputs.as_slice(),
        &snapshot.policy,
        &snapshot.safe_boundary,
        snapshot.control.paused,
        crate::gateway::current_unix_ms(),
    );
    let preview_mode = if snapshot.control.paused { Some(QueueMode::Collect) } else { None };
    let preview_decision = decide_session_queue_mode(
        snapshot.policy.clone(),
        preview_mode,
        snapshot.safe_boundary.clone(),
        pending_depth,
    );
    let analysis_json = analysis.snapshot_json();
    let busy_state = analysis.busy_state.as_str();
    let recommendation = analysis.recommendation.clone();
    let metrics_json = analysis.metrics.snapshot_json();
    json!({
        "session_id": snapshot.session_record.session_id,
        "control": snapshot.control,
        "policy": snapshot.policy.snapshot_json(),
        "safe_boundary": snapshot.safe_boundary,
        "active_run_id": snapshot.active_run_id,
        "queued_inputs": snapshot.queued_inputs,
        "busy_state": busy_state,
        "recommendation": recommendation,
        "metrics": metrics_json,
        "analysis": analysis_json,
        "decision_preview": preview_decision.explain_json(),
        "contract": contract_descriptor(),
    })
}

// --- Canvas helpers ---

/// Builds canvas summaries for a session, attaching transcript provenance to
/// each canvas.
fn load_console_chat_canvas_summaries(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    transcript: &[journal::OrchestratorSessionTranscriptRecord],
) -> Result<Vec<ConsoleChatCanvasSummary>, Box<Response>> {
    let canvases = state
        .runtime
        .list_session_canvases(context, session_id)
        .map_err(|error| Box::new(runtime_status_response(error)))?;
    Ok(canvases
        .iter()
        .map(|canvas| {
            build_console_chat_canvas_summary(
                canvas,
                derive_canvas_transcript_reference(transcript, canvas.canvas_id.as_str()),
            )
        })
        .collect())
}

/// Loads a canvas in the caller's context and rejects canvases that belong
/// to a different session than the request path claims.
async fn load_console_chat_canvas(
    state: &AppState,
    context: &gateway::RequestContext,
    session_id: &str,
    canvas_id: &str,
) -> Result<gateway::CanvasRecord, Response> {
    let canvas = state.runtime.get_canvas(context, canvas_id).map_err(runtime_status_response)?;
    if canvas.session_id != session_id {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "canvas does not belong to the requested session",
        )));
    }
    Ok(canvas)
}

/// Issues a canvas runtime frame descriptor. `failed_precondition` (canvas
/// host disabled/expired) is reported as a soft `runtime_error` string so the
/// detail endpoints still return canvas state; other errors propagate.
fn resolve_console_chat_canvas_runtime_descriptor(
    state: &AppState,
    context: &gateway::RequestContext,
    canvas_id: &str,
) -> Result<(Option<gateway::CanvasRuntimeDescriptor>, Option<String>), Box<Response>> {
    match state.runtime.issue_canvas_runtime_descriptor(context, canvas_id, None) {
        Ok(runtime) => Ok((Some(runtime), None)),
        Err(error) if error.code() == tonic::Code::FailedPrecondition => {
            Ok((None, Some(sanitize_http_error_message(error.message()))))
        }
        Err(error) => Err(Box::new(runtime_status_response(error))),
    }
}

fn build_console_chat_canvas_summary(
    canvas: &gateway::CanvasRecord,
    reference: ConsoleChatCanvasTranscriptReference,
) -> ConsoleChatCanvasSummary {
    let runtime_status = match unix_ms_now() {
        Ok(_) if canvas.closed => "closed",
        Ok(now_unix_ms) if canvas.expires_at_unix_ms <= now_unix_ms => "expired",
        Ok(_) => "ready",
        Err(_) => "unknown",
    };
    ConsoleChatCanvasSummary {
        canvas_id: canvas.canvas_id.clone(),
        session_id: canvas.session_id.clone(),
        state_version: canvas.state_version,
        state_schema_version: canvas.state_schema_version,
        created_at_unix_ms: canvas.created_at_unix_ms,
        updated_at_unix_ms: canvas.updated_at_unix_ms,
        expires_at_unix_ms: canvas.expires_at_unix_ms,
        closed: canvas.closed,
        close_reason: canvas.close_reason.clone(),
        runtime_status: runtime_status.to_owned(),
        reference,
    }
}

/// Finds the most recent transcript event whose payload references the
/// canvas frame URL and converts it into provenance metadata.
fn derive_canvas_transcript_reference(
    transcript: &[journal::OrchestratorSessionTranscriptRecord],
    canvas_id: &str,
) -> ConsoleChatCanvasTranscriptReference {
    transcript
        .iter()
        .rev()
        .find(|record| payload_references_canvas(record.payload_json.as_str(), canvas_id))
        .map(|record| ConsoleChatCanvasTranscriptReference {
            source_run_id: Some(record.run_id.clone()),
            source_tape_seq: Some(record.seq),
            source_event_type: Some(record.event_type.clone()),
            origin_kind: if record.origin_kind.trim().is_empty() {
                None
            } else {
                Some(record.origin_kind.clone())
            },
            origin_run_id: record.origin_run_id.clone(),
            last_referenced_at_unix_ms: Some(record.created_at_unix_ms),
        })
        .unwrap_or_default()
}

fn payload_references_canvas(payload_json: &str, canvas_id: &str) -> bool {
    serde_json::from_str::<Value>(payload_json)
        .ok()
        .is_some_and(|payload| json_value_references_canvas(&payload, canvas_id))
}

fn json_value_references_canvas(value: &Value, canvas_id: &str) -> bool {
    match value {
        Value::String(raw) => extract_canvas_id_from_frame_reference(raw)
            .is_some_and(|candidate| candidate == canvas_id),
        Value::Array(entries) => {
            entries.iter().any(|entry| json_value_references_canvas(entry, canvas_id))
        }
        Value::Object(entries) => {
            entries.values().any(|entry| json_value_references_canvas(entry, canvas_id))
        }
        _ => false,
    }
}

/// Extracts a canonical canvas id from a `/canvas/v1/frame/<id>` URL embedded
/// anywhere in a string (absolute or relative form).
fn extract_canvas_id_from_frame_reference(raw: &str) -> Option<&str> {
    const CANVAS_FRAME_MARKER: &str = "/canvas/v1/frame/";
    let start = raw.find(CANVAS_FRAME_MARKER)?;
    let remainder = &raw[start + CANVAS_FRAME_MARKER.len()..];
    let end = remainder.find(['?', '#', '/']).unwrap_or(remainder.len());
    let candidate = &remainder[..end];
    validate_canonical_id(candidate).ok()?;
    Some(candidate)
}

// --- Session lineage title helpers ---

/// Suggested auto-title for a branched/restored session, e.g. `Root #3`.
#[derive(Debug, Clone)]
struct LineageTitleSeed {
    suggested_title: String,
}

/// Derives a branch title seed by walking the session family to its root
/// title and counting existing family members.
async fn load_lineage_title_seed(
    state: &AppState,
    context: &gateway::RequestContext,
    session: &journal::OrchestratorSessionRecord,
) -> Result<LineageTitleSeed, Response> {
    let sessions = load_console_chat_scoped_sessions(state, context).await?;
    let sessions_by_id = sessions
        .iter()
        .map(|entry| (entry.session_id.as_str(), entry))
        .collect::<std::collections::HashMap<_, _>>();
    let family_root = session_family_root(session.session_id.as_str(), &sessions_by_id)
        .unwrap_or_else(|| session.title.clone());
    let family_size = sessions
        .iter()
        .filter(|entry| {
            session_family_root(entry.session_id.as_str(), &sessions_by_id)
                .is_some_and(|root| root == family_root)
        })
        .count()
        .max(1);
    let suggested_title = format!("{} #{}", family_root, family_size + 1);
    Ok(LineageTitleSeed { suggested_title })
}

/// Pages through every session (including archived) visible to the console
/// context; lineage walks need the complete family, not one page.
async fn load_console_chat_scoped_sessions(
    state: &AppState,
    context: &gateway::RequestContext,
) -> Result<Vec<journal::OrchestratorSessionRecord>, Response> {
    let mut sessions = Vec::new();
    let mut after_session_key = None::<String>;
    loop {
        let (mut page, next_after_session_key) = state
            .runtime
            .list_orchestrator_sessions(gateway::ListOrchestratorSessionsRequest {
                after_session_key: after_session_key.clone(),
                principal: context.principal.clone(),
                device_id: context.device_id.clone(),
                channel: context.channel.clone(),
                include_archived: true,
                requested_limit: Some(100),
                search_query: None,
            })
            .await
            .map_err(runtime_status_response)?;
        sessions.append(&mut page);
        let Some(next_after_session_key) = next_after_session_key else {
            break;
        };
        after_session_key = Some(next_after_session_key);
    }
    Ok(sessions)
}

/// Walks `parent_session_id` links to the family root and returns its
/// normalized title (falling back to label, then session key).
fn session_family_root<'a>(
    session_id: &str,
    sessions_by_id: &std::collections::HashMap<&'a str, &'a journal::OrchestratorSessionRecord>,
) -> Option<String> {
    let mut current = sessions_by_id.get(session_id).copied()?;
    loop {
        let title_root = normalize_title_family_root(current.title.as_str())
            .or_else(|| current.session_label.as_deref().and_then(normalize_title_family_root))
            .unwrap_or_else(|| current.session_key.clone());
        let Some(parent_session_id) = current.parent_session_id.as_deref() else {
            return Some(title_root);
        };
        let Some(parent) = sessions_by_id.get(parent_session_id).copied() else {
            return Some(title_root);
        };
        current = parent;
    }
}

/// Strips a trailing `#<digits>` branch counter so `Title #2` and `Title #3`
/// share the family root `Title`.
fn normalize_title_family_root(raw: &str) -> Option<String> {
    let normalized = trim_to_option(raw.to_owned())?;
    let Some((prefix, suffix)) = normalized.rsplit_once('#') else {
        return Some(normalized);
    };
    if suffix.trim().chars().all(|value| value.is_ascii_digit()) {
        trim_to_option(prefix.trim().to_owned()).or(Some(normalized))
    } else {
        Some(normalized)
    }
}

// --- Background-task and capability helpers ---

/// Loads a background task; ownership mismatches are reported as `not_found`
/// (not `permission_denied`) so foreign task ids are not enumerable.
async fn load_console_background_task(
    state: &AppState,
    context: &gateway::RequestContext,
    task_id: &str,
) -> Result<journal::OrchestratorBackgroundTaskRecord, Response> {
    let task = state
        .runtime
        .get_orchestrator_background_task(task_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "background task not found: {task_id}"
            )))
        })?;
    let same_principal = task.owner_principal == context.principal;
    let same_device = task.device_id == context.device_id;
    let same_channel = task.channel == context.channel;
    if !same_principal || !same_device || !same_channel {
        return Err(runtime_status_response(tonic::Status::not_found(
            "background task not found for current console context",
        )));
    }
    Ok(task)
}

/// Records an `AuxiliaryTaskLifecycle` runtime-decision event for a
/// background-task transition (created/paused/resumed/requeued/cancelled).
#[allow(clippy::result_large_err)]
async fn record_background_task_runtime_preview(
    state: &AppState,
    context: &gateway::RequestContext,
    task: &journal::OrchestratorBackgroundTaskRecord,
    reason: &str,
    retry_count: Option<u32>,
) -> Result<(), Response> {
    let anchor_run_id = task.target_run_id.as_deref().or(task.parent_run_id.as_deref());
    state
        .runtime
        .record_runtime_decision_event(
            context,
            Some(task.session_id.as_str()),
            anchor_run_id,
            RuntimeDecisionPayload::new(
                RuntimeDecisionEventType::AuxiliaryTaskLifecycle,
                state.runtime.runtime_decision_actor_from_context(
                    context,
                    RuntimeDecisionActorKind::Operator,
                ),
                reason,
                "auxiliary_executor.preview.lifecycle",
                RuntimeDecisionTiming::observed(task.updated_at_unix_ms),
            )
            .with_input(RuntimeEntityRef::new("task", "background_task", task.task_id.clone()))
            .with_output(
                RuntimeEntityRef::new("task", "background_task", task.task_id.clone())
                    .with_state(task.state.clone()),
            )
            .with_resource_budget(RuntimeResourceBudget {
                queue_depth: None,
                token_budget: Some(task.budget_tokens),
                pruning_token_delta: None,
                retrieval_branch_latency_ms: None,
                retry_count,
                suppression_count: None,
            })
            .with_related_entity(RuntimeEntityRef::new(
                "session",
                "session",
                task.session_id.clone(),
            ))
            .with_details(json!({
                "task_kind": task.task_kind,
                "attempt_count": task.attempt_count,
                "max_attempts": task.max_attempts,
                "queued_input_id": task.queued_input_id,
                "target_run_id": task.target_run_id,
            })),
        )
        .await
        .map_err(runtime_status_response)
}
/// Gates preview-stage features: rejects the request when the capability is
/// disabled by config, or when the session-queue rollout guardrail has
/// auto-disabled queueing after repeated delivery failures.
#[allow(clippy::result_large_err)]
fn ensure_console_runtime_preview_capability(
    state: &AppState,
    capability: RuntimePreviewCapability,
) -> Result<(), Response> {
    if capability == RuntimePreviewCapability::SessionQueuePolicy
        && state.observability.session_queue_auto_disable_active()
    {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "session_queue_policy auto-disabled by rollout guardrail after repeated delivery failures",
        )));
    }
    if let Some(message) = crate::runtime_preview_controls::capability_blocker_message(
        &state.runtime.config,
        capability,
    ) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(message)));
    }
    Ok(())
}

/// Loads a derived artifact; context mismatches are reported as `not_found`
/// so foreign artifact ids are not enumerable.
fn load_console_derived_artifact(
    state: &AppState,
    context: &gateway::RequestContext,
    derived_artifact_id: &str,
    require_device_match: bool,
) -> Result<media::MediaDerivedArtifactRecord, Box<Response>> {
    let record = state
        .channels
        .get_derived_artifact(derived_artifact_id)
        .map_err(|error| Box::new(channel_platform_error_response(error)))?
        .ok_or_else(|| {
            Box::new(runtime_status_response(tonic::Status::not_found(format!(
                "derived artifact not found: {derived_artifact_id}"
            ))))
        })?;
    if !derived_artifact_matches_console_context(&record, context, require_device_match) {
        return Err(Box::new(runtime_status_response(tonic::Status::not_found(
            "derived artifact not found for current console context",
        ))));
    }
    Ok(record)
}

/// Returns whether a run state string is terminal (retry/branch anchors must
/// not target in-flight runs).
fn is_terminal_run_state(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled")
}

// --- Transcript extraction and export rendering ---

/// Finds the newest persisted `message.received` text, optionally restricted
/// to one run; used to rebuild the prompt for retry.
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

/// Pulls the human-searchable text out of a transcript record, keyed by
/// event type; non-textual events yield `None` and are skipped by
/// search/export.
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

/// Renders the Markdown export document: header metadata, pins, compactions,
/// checkpoints, and the textual transcript.
fn render_session_export_markdown(
    session: &journal::OrchestratorSessionRecord,
    transcript: &[journal::OrchestratorSessionTranscriptRecord],
    pins: &[journal::OrchestratorSessionPinRecord],
    compactions: &[journal::OrchestratorCompactionArtifactRecord],
    checkpoints: &[journal::OrchestratorCheckpointRecord],
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
    if !compactions.is_empty() {
        document.push_str("\n## Compactions\n\n");
        for artifact in compactions {
            document.push_str("- ");
            document.push_str(artifact.summary_preview.as_str());
            document.push_str(" (`");
            document.push_str(artifact.mode.as_str());
            document.push_str("`, tokens ");
            document.push_str(artifact.estimated_input_tokens.to_string().as_str());
            document.push_str(" -> ");
            document.push_str(artifact.estimated_output_tokens.to_string().as_str());
            document.push_str(")\n");
        }
    }
    if !checkpoints.is_empty() {
        document.push_str("\n## Checkpoints\n\n");
        for checkpoint in checkpoints {
            document.push_str("- ");
            document.push_str(checkpoint.name.as_str());
            if let Some(note) = checkpoint.note.as_deref() {
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

/// Lowercases, trims, sorts, and dedupes checkpoint tags so stored tag sets
/// are canonical and comparable.
fn normalize_checkpoint_tags(tags: &[String]) -> Vec<String> {
    let mut normalized = tags
        .iter()
        .map(|tag| tag.trim().to_ascii_lowercase())
        .filter(|tag| !tag.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

// --- NDJSON streaming plumbing ---

/// Sends one NDJSON line to the HTTP response stream. Returns `false` only
/// when the client disconnected; an unencodable payload is skipped (returns
/// `true`) so a single bad event does not kill the stream.
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

/// Extracts the `(approval_id, proposal_id)` pair from a tool approval
/// request event, if both ids are present and non-empty.
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

/// Returns the status label of a status event; the relay task tracks the
/// last one seen as the run's final NDJSON `complete` status.
fn run_stream_status_kind(event: &common_v1::RunStreamEvent) -> Option<&'static str> {
    let common_v1::run_stream_event::Body::Status(status) = event.body.as_ref()? else {
        return None;
    };
    Some(stream_status_kind_label(status.kind))
}

/// Converts a protobuf `RunStreamEvent` into the JSON event shape consumed by
/// the web console. Field names and enum labels here are wire contract.
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
                // INTENTIONAL: the journal session id is withheld from the
                // browser payload; clients correlate via run_id instead.
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

// Proto-enum to wire-label mappings. The string labels below are part of the
// /console/v1 wire contract; unknown enum values map to "unspecified" rather
// than erroring so newer daemons stay readable by older consoles.

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

/// Decodes opaque payload bytes for the console: JSON when parseable, then a
/// UTF-8 string, then a `{"base64": ...}` wrapper so binary payloads survive
/// the JSON transport.
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

/// Locks the run-stream registry, recovering from poisoning instead of
/// panicking: the map only tracks active streams, so stale state after a
/// panicked holder is preferable to taking down every chat handler.
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

/// Locks a stream's pending-approval map with the same poison-recovery policy
/// as [`lock_console_chat_streams`].
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

/// Connects a gateway gRPC client (self-dial to the daemon's own gateway
/// endpoint) with explicit connect and request timeouts.
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

// --- Attachment derivation pipeline ---

/// Bridges admitted media jobs to the existing provider-health runtime.
struct GatewayAudioTranscriptionAdapter {
    runtime: Arc<gateway::GatewayRuntimeState>,
}

#[async_trait]
impl application::audio_pipeline::AudioTranscriptionBackend for GatewayAudioTranscriptionAdapter {
    async fn transcribe(
        &self,
        request: application::audio_pipeline::AudioTranscriptionJobRequest,
    ) -> Result<application::audio_pipeline::AudioTranscriptionBackendResult, String> {
        self.runtime
            .execute_audio_transcription(crate::model_provider::AudioTranscriptionRequest {
                file_name: request.file_name,
                content_type: request.content_type,
                bytes: request.bytes,
                prompt: None,
                language: request.language_hint,
            })
            .await
            .map(Into::into)
            .map_err(|status| status.message().to_owned())
    }
}

/// Bridges post-delivery speech jobs to the provider-health gateway.
struct GatewayAudioSynthesisAdapter {
    runtime: Arc<gateway::GatewayRuntimeState>,
}

#[async_trait]
impl application::audio_pipeline::AudioSynthesisBackend for GatewayAudioSynthesisAdapter {
    async fn synthesize(
        &self,
        request: application::audio_pipeline::AudioSynthesisJobRequest,
    ) -> Result<
        application::audio_pipeline::AudioSynthesisBackendResult,
        application::audio_pipeline::AudioSynthesisBackendError,
    > {
        self.runtime
            .execute_audio_synthesis(crate::model_provider::AudioSynthesisRequest {
                text: request.text,
                voice: request.voice_id,
                codec: request.codec,
            })
            .await
            .map(Into::into)
            .map_err(|status| {
                if status.code() == tonic::Code::Unimplemented {
                    application::audio_pipeline::AudioSynthesisBackendError::UnsupportedProvider
                } else {
                    application::audio_pipeline::AudioSynthesisBackendError::Failed
                }
            })
    }
}

enum ConsoleAudioOutputGate {
    NotRequested,
    Blocked(&'static str),
    Ready(application::audio_pipeline::AudioOutputRequestV1),
}

fn console_audio_output_gate(
    rollout_enabled: bool,
    request: Option<ConsoleChatAudioOutputRequest>,
    text_delivery_settled: bool,
) -> ConsoleAudioOutputGate {
    let Some(request) = request else {
        return ConsoleAudioOutputGate::NotRequested;
    };
    if !rollout_enabled {
        return ConsoleAudioOutputGate::Blocked("tts_rollout_disabled");
    }
    if !text_delivery_settled {
        return ConsoleAudioOutputGate::Blocked("tts_text_delivery_not_successful");
    }
    if request.voice.trim().is_empty()
        || request.voice.len() > 128
        || !request
            .voice
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
        || application::audio_pipeline::audio_output_content_type(request.codec.as_str()).is_none()
        || application::audio_pipeline::audio_output_file_extension(request.codec.as_str())
            .is_none()
    {
        return ConsoleAudioOutputGate::Blocked("tts_request_contract_invalid");
    }
    ConsoleAudioOutputGate::Ready(application::audio_pipeline::AudioOutputRequestV1 {
        voice_id: request.voice,
        codec: request.codec,
    })
}

fn observe_delivered_model_text(
    event: &common_v1::RunStreamEvent,
    final_text: &mut String,
    final_text_complete: &mut bool,
    overflowed: &mut bool,
) {
    let Some(common_v1::run_stream_event::Body::ModelToken(token)) = event.body.as_ref() else {
        return;
    };
    if *final_text_complete {
        final_text.clear();
        *final_text_complete = false;
        *overflowed = false;
    }
    if !*overflowed {
        if final_text.len().saturating_add(token.token.len())
            > application::audio_pipeline::MAX_SYNTHESIS_TEXT_BYTES
        {
            final_text.clear();
            *overflowed = true;
        } else {
            final_text.push_str(token.token.as_str());
        }
    }
    if token.is_final {
        *final_text_complete = true;
    }
}

struct ConsolePostDeliveryAudioContext<'a> {
    session_id: &'a str,
    run_id: &'a str,
    destination_scope_sha256: &'a str,
    rollout_enabled: bool,
    request: Option<ConsoleChatAudioOutputRequest>,
    text_delivery_settled: bool,
    final_text: String,
}

async fn execute_console_post_delivery_audio(
    state: &AppState,
    context: ConsolePostDeliveryAudioContext<'_>,
) -> Option<Value> {
    let output_request = match console_audio_output_gate(
        context.rollout_enabled,
        context.request,
        context.text_delivery_settled,
    ) {
        ConsoleAudioOutputGate::NotRequested => return None,
        ConsoleAudioOutputGate::Blocked(reason_code) => {
            return Some(json!({
                "state": "blocked",
                "reason_code": reason_code,
                "text_run_success": context.text_delivery_settled,
                "artifact": Value::Null,
            }));
        }
        ConsoleAudioOutputGate::Ready(request) => request,
    };
    let content_type =
        application::audio_pipeline::audio_output_content_type(output_request.codec.as_str())
            .expect("ready audio output gate validates the codec");
    let extension =
        application::audio_pipeline::audio_output_file_extension(output_request.codec.as_str())
            .expect("ready audio output gate validates the codec");
    let job_id = format!("tts:{}", context.run_id);
    let mut job = match state.audio_sessions.begin_job(context.session_id, job_id.as_str()) {
        Ok(job) => job,
        Err(error) => {
            return Some(json!({
                "state": "blocked",
                "reason_code": error.reason_code(),
                "text_run_success": true,
                "artifact": Value::Null,
            }));
        }
    };
    let cancellation = job.cancellation();
    let receipt = application::audio_pipeline::TextDeliveryReceipt {
        run_id: context.run_id.to_owned(),
        text: context.final_text,
        success: true,
        delivered_at_unix_ms: u64::try_from(gateway::current_unix_ms()).unwrap_or_default(),
    };
    let source_text_sha256 = crate::sha256_hex(receipt.text.as_bytes());
    let backend = GatewayAudioSynthesisAdapter { runtime: Arc::clone(&state.runtime) };
    let mut outcome = job
        .pipeline_mut()
        .synthesize_after_delivery(
            &receipt,
            &output_request,
            application::audio_pipeline::MediaDeliveryDescriptor {
                delivery_key: job_id,
                destination_scope_sha256: context.destination_scope_sha256.to_owned(),
                content_type: content_type.to_owned(),
                file_name: format!("reply-{}.{extension}", context.run_id),
            },
            &backend,
            &cancellation,
        )
        .await;

    let usage = outcome.artifact.as_ref().map(|artifact| artifact.usage).unwrap_or_default();
    let mut output_sha256 = outcome.artifact.as_ref().map(|artifact| artifact.sha256.clone());
    if outcome.state == application::audio_pipeline::MediaJobState::Succeeded {
        let persistence = outcome
            .artifact
            .as_ref()
            .zip(outcome.payload.as_deref())
            .ok_or_else(|| "successful synthesis omitted its artifact payload".to_owned())
            .and_then(|(artifact, payload)| {
                state
                    .channels
                    .store_audio_output(media::MediaAudioOutputStoreRequest {
                        artifact,
                        payload,
                        session_id: context.session_id,
                    })
                    .map_err(|_| "synthesized audio persistence failed".to_owned())
            });
        if persistence.is_err() {
            outcome.state = application::audio_pipeline::MediaJobState::Failed;
            outcome.reason_code = "tts_artifact_persistence_failed".to_owned();
            outcome.artifact = None;
            outcome.payload = None;
            output_sha256 = None;
        }
    }
    let diagnostics = state.audio_sessions.diagnostics(context.session_id);
    let event_persisted = diagnostics.is_some_and(|diagnostics| {
        state
            .channels
            .record_audio_job_event(media::MediaAudioJobEventRequest {
                source_artifact_id: context.run_id,
                source_artifact_sha256: source_text_sha256.as_str(),
                session_id: context.session_id,
                job_kind: "synthesis",
                state: media_job_state_label(outcome.state),
                reason_code: outcome.reason_code.as_str(),
                derived_artifact_sha256: output_sha256.as_deref(),
                input_bytes: usage.input_bytes,
                output_bytes: usage.output_bytes,
                audio_duration_ms: usage.audio_duration_ms,
                billable_units: usage.billable_units,
                estimated_cost_microunits: usage.estimated_cost_microunits,
                session_bytes: diagnostics.usage.bytes,
                session_duration_ms: diagnostics.usage.duration_ms,
                active_jobs: diagnostics.active_jobs,
            })
            .is_ok()
    });
    if !event_persisted {
        outcome.state = application::audio_pipeline::MediaJobState::Failed;
        outcome.reason_code = "tts_event_persistence_failed".to_owned();
        outcome.artifact = None;
        outcome.payload = None;
    }
    Some(json!({
        "state": media_job_state_label(outcome.state),
        "reason_code": outcome.reason_code,
        "text_run_success": outcome.text_run_success,
        "artifact": outcome.artifact,
    }))
}

const fn media_job_state_label(state: application::audio_pipeline::MediaJobState) -> &'static str {
    match state {
        application::audio_pipeline::MediaJobState::Succeeded => "succeeded",
        application::audio_pipeline::MediaJobState::Failed => "failed",
        application::audio_pipeline::MediaJobState::TimedOut => "timed_out",
        application::audio_pipeline::MediaJobState::Cancelled => "cancelled",
        application::audio_pipeline::MediaJobState::Blocked => "blocked",
    }
}

fn record_console_audio_job_event(
    state: &AppState,
    session_id: &str,
    source_artifact: &media::MediaArtifactPayload,
    job_state: &str,
    reason_code: &str,
    derived_artifact_sha256: Option<&str>,
    usage: application::audio_pipeline::MediaUsage,
) -> Result<(), String> {
    let session = state.audio_sessions.diagnostics(session_id).ok_or_else(|| {
        "audio session diagnostics disappeared before durable event settlement".to_owned()
    })?;
    state
        .channels
        .record_audio_job_event(media::MediaAudioJobEventRequest {
            source_artifact_id: source_artifact.artifact_id.as_str(),
            source_artifact_sha256: source_artifact.sha256.as_str(),
            session_id,
            job_kind: "transcription",
            state: job_state,
            reason_code,
            derived_artifact_sha256,
            input_bytes: usage.input_bytes,
            output_bytes: usage.output_bytes,
            audio_duration_ms: usage.audio_duration_ms,
            billable_units: usage.billable_units,
            estimated_cost_microunits: usage.estimated_cost_microunits,
            session_bytes: session.usage.bytes,
            session_duration_ms: session.usage.duration_ms,
            active_jobs: session.active_jobs,
        })
        .map_err(|error| error.to_string())
}

fn audio_job_state_for_error(
    error: &application::audio_pipeline::AudioPipelineError,
) -> &'static str {
    use application::audio_pipeline::AudioPipelineError;

    match error {
        AudioPipelineError::Cancelled => "cancelled",
        AudioPipelineError::TimedOut => "timed_out",
        AudioPipelineError::Backend(_) => "failed",
        AudioPipelineError::JobByteBudgetExceeded
        | AudioPipelineError::JobDurationBudgetExceeded
        | AudioPipelineError::SessionByteBudgetExceeded
        | AudioPipelineError::SessionDurationBudgetExceeded
        | AudioPipelineError::InvalidMediaIdentity
        | AudioPipelineError::DuplicateMediaJob
        | AudioPipelineError::SessionRegistryCapacityExceeded
        | AudioPipelineError::SessionJobLimitExceeded
        | AudioPipelineError::ArtifactIntegrityMismatch
        | AudioPipelineError::ArtifactContractInvalid
        | AudioPipelineError::TranscriptBudgetExceeded
        | AudioPipelineError::BackendOutputInvalid => "blocked",
    }
}

/// Derives artifacts for an uploaded attachment: always a metadata summary,
/// plus extracted text for documents and a transcript for audio when
/// supported. Per-kind extraction failures are persisted as failed derived
/// artifact records rather than failing the upload.
async fn derive_console_attachment_artifacts(
    state: &AppState,
    session: &ConsoleSession,
    session_id: &str,
    artifact: &media::MediaArtifactPayload,
    background_task_id: &str,
) -> Result<Vec<media::MediaDerivedArtifactRecord>, Box<dyn std::error::Error + Send + Sync>> {
    let mut persisted = Vec::new();
    let metadata = crate::media_derived::build_metadata_summary_content(
        artifact.filename.as_str(),
        artifact.content_type.as_str(),
        artifact.size_bytes,
        artifact.sha256.as_str(),
        artifact.width_px,
        artifact.height_px,
    );
    let metadata_record = state
        .channels
        .upsert_console_chat_derived_artifact(media::MediaDerivedArtifactUpsertRequest {
            source_artifact_id: artifact.artifact_id.as_str(),
            attachment_id: Some(artifact.artifact_id.as_str()),
            session_id: Some(session_id),
            principal: Some(session.context.principal.as_str()),
            device_id: Some(session.context.device_id.as_str()),
            channel: session.context.channel.as_deref(),
            filename: artifact.filename.as_str(),
            declared_content_type: artifact.content_type.as_str(),
            source_content_hash: artifact.sha256.as_str(),
            background_task_id: Some(background_task_id),
            derived: &metadata,
        })
        .map_err(|error| error.to_string())?;
    index_derived_artifact_targets(state, session, session_id, artifact, &metadata_record).await?;
    persisted.push(metadata_record);

    if crate::media_derived::supports_document_extraction(artifact.content_type.as_str()) {
        match crate::media_derived::document::extract_document_content_bounded(
            crate::media_derived::document::DocumentExtractionRequest {
                source_artifact_id: artifact.artifact_id.clone(),
                filename: artifact.filename.clone(),
                content_type: artifact.content_type.clone(),
                expected_source_sha256: Some(artifact.sha256.clone()),
                bytes: artifact.bytes.clone(),
                limits: crate::media_derived::document::DocumentExtractionLimits::default(),
            },
        )
        .await
        {
            Ok(extraction) => {
                let derived = extraction.content;
                let record = state
                    .channels
                    .upsert_console_chat_derived_artifact(
                        media::MediaDerivedArtifactUpsertRequest {
                            source_artifact_id: artifact.artifact_id.as_str(),
                            attachment_id: Some(artifact.artifact_id.as_str()),
                            session_id: Some(session_id),
                            principal: Some(session.context.principal.as_str()),
                            device_id: Some(session.context.device_id.as_str()),
                            channel: session.context.channel.as_deref(),
                            filename: artifact.filename.as_str(),
                            declared_content_type: artifact.content_type.as_str(),
                            source_content_hash: artifact.sha256.as_str(),
                            background_task_id: Some(background_task_id),
                            derived: &derived,
                        },
                    )
                    .map_err(|error| error.to_string())?;
                index_derived_artifact_targets(state, session, session_id, artifact, &record)
                    .await?;
                persisted.push(record);
            }
            Err(error) => {
                let failure_reason =
                    format!("{}:{}:{}", error.status.as_str(), error.reason_code, error.message);
                persisted.push(
                    state
                        .channels
                        .upsert_console_chat_failed_derived_artifact(
                            media::MediaFailedDerivedArtifactUpsertRequest {
                                source_artifact_id: artifact.artifact_id.as_str(),
                                attachment_id: Some(artifact.artifact_id.as_str()),
                                session_id: Some(session_id),
                                principal: Some(session.context.principal.as_str()),
                                device_id: Some(session.context.device_id.as_str()),
                                channel: session.context.channel.as_deref(),
                                filename: artifact.filename.as_str(),
                                declared_content_type: artifact.content_type.as_str(),
                                source_content_hash: artifact.sha256.as_str(),
                                kind: crate::media_derived::DerivedArtifactKind::ExtractedText,
                                parser_name: crate::media_derived::DOCUMENT_EXTRACTOR_PARSER_NAME,
                                parser_version:
                                    crate::media_derived::DOCUMENT_EXTRACTOR_PARSER_VERSION,
                                background_task_id: Some(background_task_id),
                                failure_reason: failure_reason.as_str(),
                            },
                        )
                        .map_err(|error| error.to_string())?,
                );
            }
        }
    }

    if should_transcribe_audio_attachment(
        state.runtime.config.feature_rollouts.audio_pipeline.enabled,
        artifact.content_type.as_str(),
    ) {
        let transcription_started_at = std::time::Instant::now();
        let retention = application::audio_pipeline::MediaRetentionPolicy::default();
        let input_artifact = application::audio_pipeline::AudioInputArtifactV1::from_payload(
            application::audio_pipeline::AudioInputDescriptor {
                file_name: artifact.filename.clone(),
                content_type: artifact.content_type.clone(),
                codec: artifact
                    .content_type
                    .split_once('/')
                    .map(|(_, codec)| codec)
                    .unwrap_or("unknown")
                    .to_owned(),
                duration_ms: 0,
                language_hint: None,
            },
            artifact.bytes.as_slice(),
            application::audio_pipeline::AudioArtifactProvenance {
                source_kind: "console_attachment".to_owned(),
                source_reference_sha256: artifact.sha256.clone(),
                received_at_unix_ms: u64::try_from(gateway::current_unix_ms()).unwrap_or_default(),
                principal_scope_sha256: crate::sha256_hex(session.context.principal.as_bytes()),
                session_id: session_id.to_owned(),
            },
            retention,
        );
        let mut audio_job = state
            .audio_sessions
            .begin_job(session_id, background_task_id)
            .map_err(|error| format!("{}:{error}", error.reason_code()))?;
        let cancellation = audio_job.cancellation();
        let backend = GatewayAudioTranscriptionAdapter { runtime: Arc::clone(&state.runtime) };
        match audio_job
            .pipeline_mut()
            .transcribe(&input_artifact, artifact.bytes.as_slice(), &backend, &cancellation)
            .await
        {
            Ok(transcript) => {
                let transcript_sha256 = transcript.transcript_sha256.clone();
                let transcript_usage = transcript.usage;
                match crate::media_derived::build_transcription_content(
                    crate::model_provider::AudioTranscriptionResponse {
                        text: transcript.text,
                        language: transcript.detected_language,
                        duration_ms: Some(transcript.duration_ms),
                        model_name: transcript.model_name,
                        retry_count: 0,
                        segments: Vec::new(),
                    },
                    transcription_started_at.elapsed().as_millis() as u64,
                ) {
                    Ok(derived) => {
                        let record = state
                            .channels
                            .upsert_console_chat_derived_artifact(
                                media::MediaDerivedArtifactUpsertRequest {
                                    source_artifact_id: artifact.artifact_id.as_str(),
                                    attachment_id: Some(artifact.artifact_id.as_str()),
                                    session_id: Some(session_id),
                                    principal: Some(session.context.principal.as_str()),
                                    device_id: Some(session.context.device_id.as_str()),
                                    channel: session.context.channel.as_deref(),
                                    filename: artifact.filename.as_str(),
                                    declared_content_type: artifact.content_type.as_str(),
                                    source_content_hash: artifact.sha256.as_str(),
                                    background_task_id: Some(background_task_id),
                                    derived: &derived,
                                },
                            )
                            .map_err(|error| error.to_string())?;
                        index_derived_artifact_targets(
                            state, session, session_id, artifact, &record,
                        )
                        .await?;
                        record_console_audio_job_event(
                            state,
                            session_id,
                            artifact,
                            "succeeded",
                            "audio.transcription.succeeded",
                            Some(transcript_sha256.as_str()),
                            transcript_usage,
                        )?;
                        persisted.push(record);
                    }
                    Err(error) => {
                        record_console_audio_job_event(
                            state,
                            session_id,
                            artifact,
                            "failed",
                            "audio.transcription.derived_artifact_failed",
                            None,
                            transcript_usage,
                        )?;
                        persisted.push(
                            state
                                .channels
                                .upsert_console_chat_failed_derived_artifact(
                                    media::MediaFailedDerivedArtifactUpsertRequest {
                                        source_artifact_id: artifact.artifact_id.as_str(),
                                        attachment_id: Some(artifact.artifact_id.as_str()),
                                        session_id: Some(session_id),
                                        principal: Some(session.context.principal.as_str()),
                                        device_id: Some(session.context.device_id.as_str()),
                                        channel: session.context.channel.as_deref(),
                                        filename: artifact.filename.as_str(),
                                        declared_content_type: artifact.content_type.as_str(),
                                        source_content_hash: artifact.sha256.as_str(),
                                        kind: crate::media_derived::DerivedArtifactKind::Transcript,
                                        parser_name:
                                            crate::media_derived::AUDIO_TRANSCRIBER_PARSER_NAME,
                                        parser_version:
                                            crate::media_derived::AUDIO_TRANSCRIBER_PARSER_VERSION,
                                        background_task_id: Some(background_task_id),
                                        failure_reason: error.as_str(),
                                    },
                                )
                                .map_err(|error| error.to_string())?,
                        );
                    }
                }
            }
            Err(error) => {
                record_console_audio_job_event(
                    state,
                    session_id,
                    artifact,
                    audio_job_state_for_error(&error),
                    error.reason_code(),
                    None,
                    application::audio_pipeline::MediaUsage::default(),
                )?;
                let failure_message = format!("{}:{error}", error.reason_code());
                persisted.push(
                    state
                        .channels
                        .upsert_console_chat_failed_derived_artifact(
                            media::MediaFailedDerivedArtifactUpsertRequest {
                                source_artifact_id: artifact.artifact_id.as_str(),
                                attachment_id: Some(artifact.artifact_id.as_str()),
                                session_id: Some(session_id),
                                principal: Some(session.context.principal.as_str()),
                                device_id: Some(session.context.device_id.as_str()),
                                channel: session.context.channel.as_deref(),
                                filename: artifact.filename.as_str(),
                                declared_content_type: artifact.content_type.as_str(),
                                source_content_hash: artifact.sha256.as_str(),
                                kind: crate::media_derived::DerivedArtifactKind::Transcript,
                                parser_name: crate::media_derived::AUDIO_TRANSCRIBER_PARSER_NAME,
                                parser_version:
                                    crate::media_derived::AUDIO_TRANSCRIBER_PARSER_VERSION,
                                background_task_id: Some(background_task_id),
                                failure_reason: failure_message.as_str(),
                            },
                        )
                        .map_err(|error| error.to_string())?,
                );
            }
        }
    }

    Ok(persisted)
}

fn should_transcribe_audio_attachment(rollout_enabled: bool, content_type: &str) -> bool {
    rollout_enabled && crate::media_derived::supports_audio_transcription(content_type)
}

/// Indexes a derived artifact for retrieval by upserting a workspace document
/// and a memory item, then linking them to the artifact record. Both targets
/// receive only provenance metadata (see
/// [`ATTACHMENT_DERIVED_INDEX_OMITTED_MESSAGE`]), never the extracted text.
async fn index_derived_artifact_targets(
    state: &AppState,
    session: &ConsoleSession,
    session_id: &str,
    artifact: &media::MediaArtifactPayload,
    record: &media::MediaDerivedArtifactRecord,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Only artifacts with extracted text are worth indexing; the text itself
    // is still never copied into the index targets.
    let Some(_content_text) = record.content_text.as_deref() else {
        return Ok(());
    };

    // Drop the previous memory item (recompute path) before re-ingesting so
    // stale provenance does not accumulate; absence is not an error.
    if let Some(memory_item_id) = record.memory_item_id.as_deref() {
        let _ = state
            .runtime
            .delete_memory_item(
                memory_item_id.to_owned(),
                session.context.principal.clone(),
                session.context.channel.clone(),
            )
            .await;
    }

    let workspace_content = derived_artifact_index_content(
        artifact.artifact_id.as_str(),
        record.kind.as_str(),
        artifact.filename.as_str(),
        artifact.content_type.as_str(),
    );
    let workspace_record = state
        .runtime
        .upsert_workspace_document(journal::WorkspaceDocumentWriteRequest {
            document_id: record.workspace_document_id.clone(),
            principal: session.context.principal.clone(),
            channel: session.context.channel.clone(),
            agent_id: None,
            session_id: Some(session_id.to_owned()),
            path: console_attachment_workspace_path(
                session_id,
                artifact.artifact_id.as_str(),
                record.kind.as_str(),
            ),
            title: Some(format!("{} ({})", artifact.filename, record.kind)),
            content_text: workspace_content.clone(),
            template_id: None,
            template_version: None,
            template_content_hash: None,
            source_memory_id: None,
            manual_override: false,
        })
        .await?;
    let memory_id = record.memory_item_id.clone().unwrap_or_else(|| Ulid::generate().to_string());
    let _memory_item = state
        .runtime
        .ingest_memory_item(journal::MemoryItemCreateRequest {
            memory_id: memory_id.clone(),
            principal: session.context.principal.clone(),
            channel: session.context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            source: journal::MemorySource::Import,
            content_text: workspace_content,
            tags: vec![
                "attachment".to_owned(),
                format!("artifact:{}", artifact.artifact_id),
                format!("derived:{}", record.kind),
            ],
            confidence: None,
            ttl_unix_ms: None,
        })
        .await?;
    state
        .channels
        .link_derived_artifact_targets(
            record.derived_artifact_id.as_str(),
            Some(workspace_record.document_id.as_str()),
            Some(memory_id.as_str()),
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn console_attachment_workspace_path(session_id: &str, artifact_id: &str, kind: &str) -> String {
    format!("projects/attachments/{session_id}/{artifact_id}/{kind}.md")
}

/// Builds the provenance-only body stored in workspace/memory indexes for an
/// attachment-derived artifact; the test suite pins that extracted text never
/// appears here.
fn derived_artifact_index_content(
    artifact_id: &str,
    kind: &str,
    filename: &str,
    content_type: &str,
) -> String {
    format!(
        "source_artifact_id: {artifact_id}\nkind: {kind}\nfilename: {filename}\ncontent_type: {content_type}\n\n{ATTACHMENT_DERIVED_INDEX_OMITTED_MESSAGE}"
    )
}

// --- Envelope, attachment, and approval bridging helpers ---

/// Builds the canonical `MessageEnvelope` for a console-submitted message;
/// the console always reports a verified web origin.
fn build_console_chat_message_envelope(
    session: &ConsoleSession,
    session_id: &str,
    text: String,
    timestamp_unix_ms: i64,
    attachments: Vec<common_v1::MessageAttachment>,
) -> common_v1::MessageEnvelope {
    common_v1::MessageEnvelope {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        envelope_id: Some(common_v1::CanonicalId { ulid: Ulid::generate().to_string() }),
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

/// Serializes an attachment to its wire shape (metadata only, no bytes).
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

/// Maps a declared content type onto the protocol attachment kind by
/// top-level MIME family, defaulting to `File`.
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

/// Coarse token-budget heuristic for attachment processing: a flat estimate
/// for images (vision-model tile cost), otherwise the usual ~4 bytes/token
/// approximation on the raw size.
fn estimate_console_chat_attachment_tokens(payload: &media::MediaArtifactPayload) -> u64 {
    if payload.content_type.starts_with("image/") {
        850
    } else {
        payload.size_bytes / 4
    }
}

/// Forwards a console approval decision into the matching active run stream
/// as a `ToolApprovalResponse`, consuming the pending approval mapping.
///
/// Returns `false` (without error) whenever the decision cannot be delivered:
/// no decision yet, no active stream for the run, session mismatch, unknown
/// approval id, or a closed stream. Called from the approvals handler, which
/// treats delivery as best-effort.
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

/// Converts a stored approval scope to its protobuf value; an unset scope
/// conservatively means a one-time approval.
fn approval_scope_to_proto(scope: Option<ApprovalDecisionScope>) -> i32 {
    match scope.unwrap_or(ApprovalDecisionScope::Once) {
        ApprovalDecisionScope::Once => common_v1::ApprovalDecisionScope::Once as i32,
        ApprovalDecisionScope::Session => common_v1::ApprovalDecisionScope::Session as i32,
        ApprovalDecisionScope::Timeboxed => common_v1::ApprovalDecisionScope::Timeboxed as i32,
    }
}

/// Effective token budget for a console background task: the requested value
/// verbatim, or the default multi-turn floor raised to at least the task text's
/// estimated size.
fn console_background_task_budget_tokens(requested: Option<u64>, text: &str) -> u64 {
    requested.unwrap_or_else(|| {
        DEFAULT_CONSOLE_BACKGROUND_TASK_BUDGET_TOKENS
            .max(crate::orchestrator::estimate_token_count(text))
            .max(1)
    })
}

/// Encodes the optional background-task payload (`parameter_delta` and/or
/// `delegation`) as a JSON string; `None` when neither is present.
#[allow(clippy::result_large_err)]
fn build_console_background_task_payload_json(
    parameter_delta: Option<&Value>,
    delegation: Option<&crate::delegation::DelegationSnapshot>,
) -> Result<Option<String>, Response> {
    if parameter_delta.is_none() && delegation.is_none() {
        return Ok(None);
    }
    let mut payload = serde_json::Map::new();
    if let Some(parameter_delta) = parameter_delta.cloned() {
        payload.insert("parameter_delta".to_owned(), parameter_delta);
    }
    if let Some(delegation) = delegation {
        let delegation_value = serde_json::to_value(delegation).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to encode delegation background payload: {error}"
            )))
        })?;
        payload.insert("delegation".to_owned(), delegation_value);
    }
    Ok(Some(Value::Object(payload).to_string()))
}

/// Loads the run lineage payload for a session, refusing to reveal it if any
/// run in the session belongs to a different console context.
async fn load_console_run_lineage(
    state: &AppState,
    context: &gateway::RequestContext,
    run: &journal::OrchestratorRunStatusSnapshot,
) -> Result<Value, Response> {
    let runs = state
        .runtime
        .list_orchestrator_session_runs(run.session_id.clone())
        .await
        .map_err(runtime_status_response)?;
    if runs.iter().any(|candidate| !run_matches_console_context(candidate, context)) {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "chat lineage does not belong to the authenticated console session context",
        )));
    }
    Ok(build_console_run_lineage_payload(run.run_id.as_str(), runs.as_slice()))
}

/// Walks `parent_run_id` links from the focus run to find the lineage root;
/// the `seen` set guards against parent cycles in persisted data.
fn build_console_run_lineage_payload(
    focus_run_id: &str,
    runs: &[journal::OrchestratorRunStatusSnapshot],
) -> Value {
    let parents = runs
        .iter()
        .map(|run| (run.run_id.clone(), run.parent_run_id.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    let mut current_run_id = focus_run_id.to_owned();
    let mut root_run_id = focus_run_id.to_owned();
    let mut seen = std::collections::HashSet::new();
    while seen.insert(current_run_id.clone()) {
        let Some(Some(parent_run_id)) = parents.get(current_run_id.as_str()) else {
            break;
        };
        root_run_id = parent_run_id.clone();
        current_run_id = parent_run_id.clone();
    }
    json!({
        "focus_run_id": focus_run_id,
        "root_run_id": root_run_id,
        "runs": runs,
    })
}

/// Unit tests for the pure helpers: ownership checks, canvas frame parsing,
/// budget defaults, retry parameter-delta selection, and the privacy
/// guarantee that attachment index content omits extracted text.
#[cfg(test)]
mod tests {
    const CHAT_SOURCE: &str = include_str!("chat.rs");

    use super::{
        build_background_task_cancel_requested_result_json,
        build_background_task_cancel_requested_update, build_background_task_cancelled_result_json,
        build_background_task_cancelled_update, build_console_chat_message_envelope,
        console_attachment_workspace_path, console_audio_output_gate,
        console_background_task_budget_tokens, derive_canvas_transcript_reference,
        derived_artifact_index_content, derived_artifact_matches_console_context,
        extract_canvas_id_from_frame_reference, observe_delivered_model_text,
        resolve_console_background_task_kind, retry_parameter_delta_from_payload_or_run,
        run_matches_console_context, should_transcribe_audio_attachment, ConsoleAudioOutputGate,
    };
    use crate::{
        app::state::ConsoleSession, domain::workspace::normalize_workspace_path, gateway, journal,
        media, transport::grpc::proto::palyra::common::v1 as common_v1,
        ConsoleChatAudioOutputRequest,
    };
    use palyra_common::runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState};

    #[test]
    fn session_reset_authorizes_owner_before_cancelling_audio() {
        let handler_start = CHAT_SOURCE
            .find("pub(crate) async fn console_chat_session_reset_handler")
            .expect("session reset handler should exist");
        let handler_end = CHAT_SOURCE[handler_start..]
            .find("// --- Message submission and run-stream bridging ---")
            .map(|offset| handler_start + offset)
            .expect("message submission section should follow reset handler");
        let handler = &CHAT_SOURCE[handler_start..handler_end];
        let ownership_resolution = handler
            .find(".resolve_orchestrator_session(")
            .expect("session reset should resolve ownership");
        let audio_cancellation =
            handler.find(".cancel_session(").expect("session reset should cancel audio jobs");

        assert!(ownership_resolution < audio_cancellation);
        assert!(handler[ownership_resolution..audio_cancellation]
            .contains(".map_err(runtime_status_response)?;"));
        assert!(handler[audio_cancellation..].contains("outcome.session.session_id.as_str()"));
    }

    #[test]
    fn audio_transcription_requires_its_product_rollout() {
        assert!(!should_transcribe_audio_attachment(false, "audio/ogg"));
        assert!(should_transcribe_audio_attachment(true, "audio/ogg"));
        assert!(!should_transcribe_audio_attachment(true, "image/png"));
    }

    #[test]
    fn post_delivery_speech_requires_rollout_and_explicit_request() {
        assert!(matches!(
            console_audio_output_gate(true, None, true),
            ConsoleAudioOutputGate::NotRequested
        ));
        assert!(matches!(
            console_audio_output_gate(
                false,
                Some(ConsoleChatAudioOutputRequest {
                    voice: "alloy".to_owned(),
                    codec: "mp3".to_owned(),
                }),
                true,
            ),
            ConsoleAudioOutputGate::Blocked("tts_rollout_disabled")
        ));
        assert!(matches!(
            console_audio_output_gate(
                true,
                Some(ConsoleChatAudioOutputRequest {
                    voice: "alloy".to_owned(),
                    codec: "mp3".to_owned(),
                }),
                false,
            ),
            ConsoleAudioOutputGate::Blocked("tts_text_delivery_not_successful")
        ));
        assert!(matches!(
            console_audio_output_gate(
                true,
                Some(ConsoleChatAudioOutputRequest {
                    voice: "alloy".to_owned(),
                    codec: "mp3".to_owned(),
                }),
                true,
            ),
            ConsoleAudioOutputGate::Ready(_)
        ));
    }

    #[test]
    fn delivered_text_projection_keeps_only_the_last_completed_turn() {
        let model_token = |token: &str, is_final: bool| common_v1::RunStreamEvent {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            run_id: None,
            body: Some(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
                token: token.to_owned(),
                is_final,
            })),
        };
        let mut text = String::new();
        let mut complete = false;
        let mut overflowed = false;
        observe_delivered_model_text(
            &model_token("intermediate", true),
            &mut text,
            &mut complete,
            &mut overflowed,
        );
        observe_delivered_model_text(
            &model_token("final ", false),
            &mut text,
            &mut complete,
            &mut overflowed,
        );
        observe_delivered_model_text(
            &model_token("answer", true),
            &mut text,
            &mut complete,
            &mut overflowed,
        );

        assert_eq!(text, "final answer");
        assert!(complete);
        assert!(!overflowed);
    }

    #[test]
    fn run_matches_console_context_rejects_mismatched_principal() {
        let run = sample_run_status();
        let context = gateway::RequestContext {
            principal: "admin:web-auditor".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("web".to_owned()),
        };

        assert!(
            !run_matches_console_context(&run, &context),
            "run ownership check must reject mismatched principals"
        );
    }

    #[test]
    fn run_matches_console_context_accepts_matching_console_context() {
        let run = sample_run_status();
        let context = gateway::RequestContext {
            principal: "admin:web-console".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("web".to_owned()),
        };

        assert!(
            run_matches_console_context(&run, &context),
            "run ownership check should allow the originating console context"
        );
    }

    #[test]
    fn derived_artifact_context_requires_device_when_requested() {
        let record = sample_derived_artifact_record();
        let same_device = gateway::RequestContext {
            principal: "admin:web-console".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("web".to_owned()),
        };
        let other_device = gateway::RequestContext {
            principal: "admin:web-console".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
            channel: Some("web".to_owned()),
        };

        assert!(derived_artifact_matches_console_context(&record, &same_device, true));
        assert!(derived_artifact_matches_console_context(&record, &other_device, false));
        assert!(
            !derived_artifact_matches_console_context(&record, &other_device, true),
            "derived artifact detail and lifecycle APIs must preserve the attachment device boundary"
        );
    }

    #[test]
    fn derived_artifact_index_content_omits_extracted_attachment_text() {
        let sensitive_text = "sensitive attachment body should not be copied into workspace memory";
        let content =
            derived_artifact_index_content("artifact-1", "text", "contract.txt", "text/plain");

        assert!(content.contains("source_artifact_id: artifact-1"));
        assert!(content.contains(super::ATTACHMENT_DERIVED_INDEX_OMITTED_MESSAGE));
        assert!(!content.contains(sensitive_text));
    }

    #[test]
    fn extract_canvas_id_from_frame_reference_accepts_absolute_and_relative_urls() {
        assert_eq!(
            extract_canvas_id_from_frame_reference(
                "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=abc"
            ),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FB1")
        );
        assert_eq!(
            extract_canvas_id_from_frame_reference(
                "https://console.example.com/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB2?token=def"
            ),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FB2")
        );
        assert_eq!(extract_canvas_id_from_frame_reference("not-a-canvas-url"), None);
    }

    #[test]
    fn console_attachment_workspace_path_uses_allowed_workspace_root() {
        let path = console_attachment_workspace_path(
            "01ARZ3NDEKTSV4RRFFQ69G5FA1",
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "metadata_summary",
        );

        assert_eq!(
            path,
            "projects/attachments/01ARZ3NDEKTSV4RRFFQ69G5FA1/01ARZ3NDEKTSV4RRFFQ69G5FA2/metadata_summary.md"
        );
        assert_eq!(
            normalize_workspace_path(path.as_str())
                .expect("path should be accepted")
                .normalized_path,
            path
        );
    }

    #[test]
    fn queued_followup_envelope_preserves_attachment_payload() {
        let session = ConsoleSession {
            session_token_hash_sha256: "hash".to_owned(),
            csrf_token: "csrf".to_owned(),
            context: gateway::RequestContext {
                principal: "admin:web-console".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("web".to_owned()),
            },
            issued_at_unix_ms: 1,
            expires_at_unix_ms: 2,
        };
        let attachment_id = "01ARZ3NDEKTSV4RRFFQ69G5FB7";
        let attachment = common_v1::MessageAttachment {
            kind: common_v1::message_attachment::AttachmentKind::File as i32,
            artifact_id: Some(common_v1::CanonicalId { ulid: attachment_id.to_owned() }),
            size_bytes: 7,
            attachment_id: attachment_id.to_owned(),
            filename: "notes.txt".to_owned(),
            declared_content_type: "text/plain".to_owned(),
            source_url: String::new(),
            content_hash: "sha256:test".to_owned(),
            origin: "console_chat_upload".to_owned(),
            policy_context: "attachment.upload.allowed".to_owned(),
            inline_bytes: b"followup".to_vec(),
            upload_requested: true,
            width_px: 0,
            height_px: 0,
        };

        let envelope = build_console_chat_message_envelope(
            &session,
            "01ARZ3NDEKTSV4RRFFQ69G5FB8",
            "continue with the attachment".to_owned(),
            3,
            vec![attachment],
        );
        let content = envelope.content.expect("followup envelope should contain content");

        assert_eq!(content.attachments.len(), 1);
        assert_eq!(content.attachments[0].attachment_id, attachment_id);
        assert_eq!(content.attachments[0].inline_bytes, b"followup");
        assert_eq!(content.attachments[0].origin, "console_chat_upload");
    }

    #[test]
    fn console_background_task_kind_rejects_delegation_without_run_authority() {
        assert_eq!(
            resolve_console_background_task_kind(Some("delegation_prompt"))
                .expect_err("console delegation must not bypass admitted Run-root authority")
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            resolve_console_background_task_kind(Some("auxiliary_summary"))
                .expect("recognized aliases should canonicalize"),
            AuxiliaryTaskKind::Summary.as_str()
        );
    }

    #[test]
    fn background_task_budget_defaults_above_prompt_estimate() {
        assert_eq!(console_background_task_budget_tokens(Some(1_000), "short task"), 1_000);
        assert_eq!(
            console_background_task_budget_tokens(None, "short task"),
            super::DEFAULT_CONSOLE_BACKGROUND_TASK_BUDGET_TOKENS
        );
        let long_task = vec!["word"; 4_200].join(" ");
        assert_eq!(
            console_background_task_budget_tokens(None, long_task.as_str()),
            super::DEFAULT_CONSOLE_BACKGROUND_TASK_BUDGET_TOKENS
        );
        let oversized_task = vec!["word"; 70_000].join(" ");
        assert_eq!(console_background_task_budget_tokens(None, oversized_task.as_str()), 70_000);
    }

    #[test]
    fn derive_canvas_transcript_reference_prefers_latest_matching_event() {
        let transcript = vec![
            journal::OrchestratorSessionTranscriptRecord {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1".to_owned(),
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned(),
                seq: 1,
                event_type: "tool_result".to_owned(),
                payload_json: serde_json::json!({
                    "frame_url": "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=one"
                })
                .to_string(),
                created_at_unix_ms: 10,
                origin_kind: "console".to_owned(),
                origin_run_id: None,
            },
            journal::OrchestratorSessionTranscriptRecord {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1".to_owned(),
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA3".to_owned(),
                seq: 7,
                event_type: "tool_result".to_owned(),
                payload_json: serde_json::json!({
                    "nested": {
                        "frame_url": "https://console.example.com/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=two"
                    }
                })
                .to_string(),
                created_at_unix_ms: 20,
                origin_kind: "retry".to_owned(),
                origin_run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned()),
            },
        ];

        let reference =
            derive_canvas_transcript_reference(transcript.as_slice(), "01ARZ3NDEKTSV4RRFFQ69G5FB1");

        assert_eq!(reference.source_run_id.as_deref(), Some("01ARZ3NDEKTSV4RRFFQ69G5FA3"));
        assert_eq!(reference.source_tape_seq, Some(7));
        assert_eq!(reference.origin_kind.as_deref(), Some("retry"));
        assert_eq!(reference.origin_run_id.as_deref(), Some("01ARZ3NDEKTSV4RRFFQ69G5FA2"));
        assert_eq!(reference.last_referenced_at_unix_ms, Some(20));
    }

    #[test]
    fn retry_parameter_delta_falls_back_to_persisted_run_context() {
        let mut run = sample_run_status();
        run.parameter_delta_json = Some(
            serde_json::json!({
                "cli_context": {
                    "launch_cwd": "C:/work/project",
                    "workspace_roots": ["C:/work/project"]
                }
            })
            .to_string(),
        );

        let parameter_delta = retry_parameter_delta_from_payload_or_run(None, &run)
            .expect("persisted retry parameter_delta should parse")
            .expect("persisted retry parameter_delta should be present");

        assert_eq!(
            parameter_delta.pointer("/cli_context/launch_cwd").and_then(serde_json::Value::as_str),
            Some("C:/work/project")
        );
        assert_eq!(
            parameter_delta
                .pointer("/cli_context/workspace_roots/0")
                .and_then(serde_json::Value::as_str),
            Some("C:/work/project")
        );
    }

    #[test]
    fn retry_parameter_delta_prefers_explicit_payload_override() {
        let mut run = sample_run_status();
        run.parameter_delta_json = Some(
            serde_json::json!({
                "cli_context": {
                    "launch_cwd": "C:/old/project",
                    "workspace_roots": ["C:/old/project"]
                }
            })
            .to_string(),
        );
        let override_delta = serde_json::json!({
            "cli_context": {
                "launch_cwd": "C:/new/project",
                "workspace_roots": ["C:/new/project"]
            }
        });

        let parameter_delta = retry_parameter_delta_from_payload_or_run(Some(override_delta), &run)
            .expect("override retry parameter_delta should parse")
            .expect("override retry parameter_delta should be present");

        assert_eq!(
            parameter_delta.pointer("/cli_context/launch_cwd").and_then(serde_json::Value::as_str),
            Some("C:/new/project")
        );
    }

    #[test]
    fn background_task_cancel_result_payloads_are_consistent_terminal_states() {
        let cancel_requested: serde_json::Value = serde_json::from_str(
            build_background_task_cancel_requested_result_json("task-1").as_str(),
        )
        .expect("cancel_requested result JSON should parse");
        assert_eq!(
            cancel_requested.get("status").and_then(serde_json::Value::as_str),
            Some("cancel_requested")
        );
        assert_eq!(
            cancel_requested.get("task_id").and_then(serde_json::Value::as_str),
            Some("task-1")
        );
        assert_eq!(
            cancel_requested.get("reason").and_then(serde_json::Value::as_str),
            Some("cancelled_by_operator")
        );

        let cancelled: serde_json::Value =
            serde_json::from_str(build_background_task_cancelled_result_json("task-1").as_str())
                .expect("cancelled result JSON should parse");
        assert_eq!(cancelled.get("status").and_then(serde_json::Value::as_str), Some("cancelled"));
        assert_eq!(cancelled.get("task_id").and_then(serde_json::Value::as_str), Some("task-1"));
        assert_eq!(
            cancelled.get("reason").and_then(serde_json::Value::as_str),
            Some("cancelled_by_operator")
        );
    }

    #[test]
    fn background_task_cancel_updates_keep_pending_and_terminal_states_distinct() {
        let pending = build_background_task_cancel_requested_update("task-1", 7);
        assert_eq!(pending.task_id, "task-1");
        assert_eq!(pending.state.as_deref(), Some(AuxiliaryTaskState::CancelRequested.as_str()));
        assert_eq!(pending.completed_at_unix_ms, None);
        assert_eq!(pending.last_error, None);

        let cancelled = build_background_task_cancelled_update("task-1", 7);
        assert_eq!(cancelled.task_id, "task-1");
        assert_eq!(cancelled.state.as_deref(), Some(AuxiliaryTaskState::Cancelled.as_str()));
        assert!(cancelled.completed_at_unix_ms.is_some());
        assert_eq!(
            cancelled.last_error.as_ref().and_then(|value| value.as_deref()),
            Some("cancelled_by_operator")
        );
    }

    fn sample_run_status() -> journal::OrchestratorRunStatusSnapshot {
        journal::OrchestratorRunStatusSnapshot {
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAY".to_owned(),
            state: "running".to_owned(),
            cancel_requested: false,
            cancel_reason: None,
            principal: "admin:web-console".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("web".to_owned()),
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            created_at_unix_ms: 1,
            started_at_unix_ms: 1,
            completed_at_unix_ms: None,
            updated_at_unix_ms: 1,
            last_error: None,
            origin_kind: "console".to_owned(),
            origin_run_id: None,
            parent_run_id: None,
            triggered_by_principal: None,
            parameter_delta_json: None,
            delegation: None,
            merge_result: None,
            tape_events: 0,
        }
    }

    fn sample_derived_artifact_record() -> media::MediaDerivedArtifactRecord {
        media::MediaDerivedArtifactRecord {
            derived_artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned(),
            source_artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FB2".to_owned(),
            attachment_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB3".to_owned()),
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB4".to_owned()),
            principal: Some("admin:web-console".to_owned()),
            device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            channel: Some("web".to_owned()),
            filename: "contract.txt".to_owned(),
            declared_content_type: "text/plain".to_owned(),
            kind: "text".to_owned(),
            state: "succeeded".to_owned(),
            parser_name: "test-parser".to_owned(),
            parser_version: "1".to_owned(),
            source_content_hash: "source-hash".to_owned(),
            content_hash: Some("content-hash".to_owned()),
            content_text: Some("attachment text".to_owned()),
            summary_text: None,
            language: None,
            duration_ms: None,
            processing_ms: None,
            warnings: Vec::new(),
            anchors: Vec::new(),
            failure_reason: None,
            quarantine_reason: None,
            workspace_document_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB5".to_owned()),
            memory_item_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB6".to_owned()),
            background_task_id: None,
            recompute_required: false,
            orphaned: false,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            purged_at_unix_ms: None,
        }
    }
}
