use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use agent_client_protocol::{self as acp, Client as _};
use anyhow::{Context, Result};
use futures::io::AllowStdIo;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::Request;

use crate::{
    build_agent_run_input, build_run_stream_request, build_runtime, inject_run_stream_metadata,
    proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    AgentConnection,
};

const META_SESSION_KEY: &str = "sessionKey";
const META_SESSION_LABEL: &str = "sessionLabel";
const META_RESET_SESSION: &str = "resetSession";
const META_REQUIRE_EXISTING: &str = "requireExisting";

const PERMISSION_ALLOW_ONCE: &str = "allow-once";
const PERMISSION_ALLOW_ALWAYS: &str = "allow-always";
const PERMISSION_REJECT_ONCE: &str = "reject-once";
const PERMISSION_REJECT_ALWAYS: &str = "reject-always";

#[derive(Debug, Clone)]
struct SessionBinding {
    gateway_session_id_ulid: String,
    session_key: String,
    session_label: Option<String>,
    cwd: PathBuf,
}

#[derive(Debug, Default)]
struct BridgeState {
    sessions: HashMap<String, SessionBinding>,
    active_runs: HashMap<String, String>,
}

enum ClientBridgeRequest {
    SessionUpdate {
        notification: acp::SessionNotification,
        response_tx: oneshot::Sender<acp::Result<()>>,
    },
    RequestPermission {
        request: acp::RequestPermissionRequest,
        response_tx: oneshot::Sender<acp::Result<acp::RequestPermissionResponse>>,
    },
}

#[derive(Clone)]
struct PalyraAcpAgent {
    connection: AgentConnection,
    allow_sensitive_tools: bool,
    state: Arc<Mutex<BridgeState>>,
    client_request_tx: mpsc::UnboundedSender<ClientBridgeRequest>,
    default_cwd: PathBuf,
}

#[derive(Debug, Default, Clone)]
struct SessionMetaOverrides {
    session_key: Option<String>,
    session_label: Option<String>,
    reset_session: Option<bool>,
    require_existing: Option<bool>,
}

impl PalyraAcpAgent {
    fn new(
        connection: AgentConnection,
        allow_sensitive_tools: bool,
        state: Arc<Mutex<BridgeState>>,
        client_request_tx: mpsc::UnboundedSender<ClientBridgeRequest>,
        default_cwd: PathBuf,
    ) -> Self {
        Self { connection, allow_sensitive_tools, state, client_request_tx, default_cwd }
    }

    async fn send_session_update(
        &self,
        session_id: &acp::SessionId,
        update: acp::SessionUpdate,
    ) -> acp::Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        let request = ClientBridgeRequest::SessionUpdate {
            notification: acp::SessionNotification::new(session_id.clone(), update),
            response_tx,
        };
        self.client_request_tx.send(request).map_err(|_| {
            acp::Error::new(-32603, "ACP session update dispatch channel is closed")
        })?;
        response_rx.await.map_err(|_| {
            acp::Error::new(-32603, "ACP session update dispatch response channel dropped")
        })?
    }

    async fn request_permission(
        &self,
        request: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
        let (response_tx, response_rx) = oneshot::channel();
        self.client_request_tx
            .send(ClientBridgeRequest::RequestPermission { request, response_tx })
            .map_err(|_| {
                acp::Error::new(-32603, "ACP permission request dispatch channel is closed")
            })?;
        response_rx.await.map_err(|_| {
            acp::Error::new(-32603, "ACP permission response channel dropped before completion")
        })?
    }

    fn read_meta_string(meta: &Option<acp::Meta>, key: &str) -> acp::Result<Option<String>> {
        let Some(meta) = meta else {
            return Ok(None);
        };
        let Some(value) = meta.get(key) else {
            return Ok(None);
        };
        let value = value.as_str().ok_or_else(|| {
            acp::Error::new(-32602, format!("_meta.{key} must be a string when provided"))
        })?;
        let value = value.trim();
        if value.is_empty() {
            return Ok(None);
        }
        Ok(Some(value.to_owned()))
    }

    fn read_meta_bool(meta: &Option<acp::Meta>, key: &str) -> acp::Result<Option<bool>> {
        let Some(meta) = meta else {
            return Ok(None);
        };
        let Some(value) = meta.get(key) else {
            return Ok(None);
        };
        let value = value.as_bool().ok_or_else(|| {
            acp::Error::new(-32602, format!("_meta.{key} must be a boolean when provided"))
        })?;
        Ok(Some(value))
    }

    fn session_overrides(meta: &Option<acp::Meta>) -> acp::Result<SessionMetaOverrides> {
        Ok(SessionMetaOverrides {
            session_key: Self::read_meta_string(meta, META_SESSION_KEY)?,
            session_label: Self::read_meta_string(meta, META_SESSION_LABEL)?,
            reset_session: Self::read_meta_bool(meta, META_RESET_SESSION)?,
            require_existing: Self::read_meta_bool(meta, META_REQUIRE_EXISTING)?,
        })
    }

    fn format_resource_link_block(link: &acp::ResourceLink) -> Option<String> {
        let uri = link.uri.trim();
        if uri.is_empty() {
            return None;
        }
        let label = link
            .title
            .as_deref()
            .or(link.description.as_deref())
            .unwrap_or(link.name.as_str())
            .trim();
        if label.is_empty() {
            Some(format!("link: {uri}"))
        } else {
            Some(format!("link ({label}): {uri}"))
        }
    }

    fn prompt_text(prompt: &[acp::ContentBlock]) -> String {
        let mut chunks = Vec::new();
        for block in prompt {
            match block {
                acp::ContentBlock::Text(text) => {
                    let trimmed = text.text.trim();
                    if !trimmed.is_empty() {
                        chunks.push(trimmed.to_owned());
                    }
                }
                acp::ContentBlock::Resource(resource) => {
                    if let acp::EmbeddedResourceResource::TextResourceContents(contents) =
                        &resource.resource
                    {
                        let trimmed = contents.text.trim();
                        if !trimmed.is_empty() {
                            chunks.push(trimmed.to_owned());
                        }
                    }
                }
                acp::ContentBlock::ResourceLink(link) => {
                    if let Some(serialized_link) = Self::format_resource_link_block(link) {
                        chunks.push(serialized_link);
                    }
                }
                _ => {}
            }
        }
        chunks.join("\n")
    }

    fn lock_state(&self) -> acp::Result<std::sync::MutexGuard<'_, BridgeState>> {
        self.state.lock().map_err(|_| acp::Error::new(-32603, "ACP bridge state lock poisoned"))
    }

    async fn resolve_gateway_session(
        &self,
        requested_session_key: String,
        session_label: Option<String>,
        require_existing: bool,
        reset_session: bool,
    ) -> acp::Result<SessionBinding> {
        let mut client =
            connect_gateway_client(&self.connection).await.map_err(acp_internal_error)?;
        let mut request = Request::new(gateway_v1::ResolveSessionRequest {
            v: 1,
            session_id: None,
            session_key: requested_session_key.clone(),
            session_label: session_label.clone().unwrap_or_default(),
            require_existing,
            reset_session,
        });
        inject_run_stream_metadata(request.metadata_mut(), &self.connection)
            .map_err(acp_internal_error)?;
        let response =
            client.resolve_session(request).await.map_err(map_gateway_status_error)?.into_inner();
        let session = response.session.ok_or_else(|| {
            acp::Error::new(-32603, "gateway ResolveSession returned empty session")
        })?;
        let gateway_session_id_ulid = session
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .ok_or_else(|| {
            acp::Error::new(-32603, "gateway ResolveSession returned session without session_id")
        })?;
        let session_key = if session.session_key.trim().is_empty() {
            requested_session_key
        } else {
            session.session_key
        };
        Ok(SessionBinding {
            gateway_session_id_ulid,
            session_key,
            session_label: non_empty(
                session_label.or_else(|| non_empty(Some(session.session_label))),
            ),
            cwd: self.default_cwd.clone(),
        })
    }

    async fn ensure_binding_for_acp_session(
        &self,
        acp_session_id: &acp::SessionId,
        overrides: &SessionMetaOverrides,
        default_require_existing: bool,
    ) -> acp::Result<SessionBinding> {
        let session_id_value = acp_session_id.0.as_ref().to_owned();
        if let Some(binding) = self.lock_state()?.sessions.get(&session_id_value).cloned() {
            if overrides.session_key.is_none()
                && overrides.session_label.is_none()
                && overrides.reset_session.is_none()
                && overrides.require_existing.is_none()
            {
                return Ok(binding);
            }
        }

        let requested_session_key =
            overrides.session_key.clone().unwrap_or_else(|| session_id_value.clone());
        let binding = self
            .resolve_gateway_session(
                requested_session_key,
                overrides.session_label.clone(),
                overrides.require_existing.unwrap_or(default_require_existing),
                overrides.reset_session.unwrap_or(false),
            )
            .await?;
        self.lock_state()?.sessions.insert(session_id_value, binding.clone());
        Ok(binding)
    }

    async fn list_gateway_sessions(
        &self,
        cursor: Option<String>,
    ) -> acp::Result<gateway_v1::ListSessionsResponse> {
        let mut client =
            connect_gateway_client(&self.connection).await.map_err(acp_internal_error)?;
        let mut request = Request::new(gateway_v1::ListSessionsRequest {
            v: 1,
            after_session_key: cursor.unwrap_or_default(),
            limit: 100,
        });
        inject_run_stream_metadata(request.metadata_mut(), &self.connection)
            .map_err(acp_internal_error)?;
        let response =
            client.list_sessions(request).await.map_err(map_gateway_status_error)?.into_inner();
        Ok(response)
    }

    async fn abort_run_for_session(&self, acp_session_id: &acp::SessionId) -> acp::Result<()> {
        let session_id = acp_session_id.0.as_ref().to_owned();
        let run_id = {
            let state = self.lock_state()?;
            state.active_runs.get(&session_id).cloned()
        };
        let Some(run_id) = run_id else {
            return Ok(());
        };

        let mut client =
            connect_gateway_client(&self.connection).await.map_err(acp_internal_error)?;
        let mut request = Request::new(gateway_v1::AbortRunRequest {
            v: 1,
            run_id: Some(common_v1::CanonicalId { ulid: run_id }),
            reason: "acp_session_cancel".to_owned(),
        });
        inject_run_stream_metadata(request.metadata_mut(), &self.connection)
            .map_err(acp_internal_error)?;
        let _ = client.abort_run(request).await.map_err(map_gateway_status_error)?.into_inner();
        Ok(())
    }

    async fn execute_prompt(
        &self,
        arguments: acp::PromptRequest,
    ) -> acp::Result<acp::PromptResponse> {
        let session_overrides = Self::session_overrides(&arguments.meta)?;
        let binding = self
            .ensure_binding_for_acp_session(&arguments.session_id, &session_overrides, false)
            .await?;
        let prompt = Self::prompt_text(&arguments.prompt);
        if prompt.trim().is_empty() {
            return Err(acp::Error::new(
                -32602,
                "session/prompt requires at least one non-empty text content block",
            ));
        }

        let run_input = build_agent_run_input(
            Some(binding.gateway_session_id_ulid.clone()),
            None,
            prompt,
            self.allow_sensitive_tools,
        )
        .map_err(acp_internal_error)?;
        let mut initial_request =
            build_run_stream_request(&run_input).map_err(acp_internal_error)?;
        initial_request.session_key = binding.session_key.clone();
        initial_request.session_label = binding.session_label.clone().unwrap_or_default();
        initial_request.reset_session = session_overrides.reset_session.unwrap_or(false);
        initial_request.require_existing = session_overrides.require_existing.unwrap_or(false);

        let (mut event_stream, request_tx) =
            open_run_stream(&self.connection, initial_request).await.map_err(acp_internal_error)?;

        {
            let mut state = self.lock_state()?;
            state
                .active_runs
                .insert(arguments.session_id.0.as_ref().to_owned(), run_input.run_id.clone());
        }

        let mut stop_reason = acp::StopReason::EndTurn;
        while let Some(event) = event_stream.next().await {
            let event = match event {
                Ok(value) => value,
                Err(status) => {
                    stop_reason = if status.code() == tonic::Code::Cancelled {
                        acp::StopReason::Cancelled
                    } else {
                        acp::StopReason::Refusal
                    };
                    self.send_session_update(
                        &arguments.session_id,
                        acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(
                            acp::ContentBlock::from(format!(
                                "Palyra gateway stream error: {}",
                                status.message()
                            )),
                        )),
                    )
                    .await?;
                    break;
                }
            };

            match event.body {
                Some(common_v1::run_stream_event::Body::ModelToken(token)) => {
                    if !token.token.is_empty() {
                        self.send_session_update(
                            &arguments.session_id,
                            acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(
                                acp::ContentBlock::from(token.token),
                            )),
                        )
                        .await?;
                    }
                }
                Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => {
                    let tool_call_id = proposal
                        .proposal_id
                        .as_ref()
                        .map(|value| value.ulid.clone())
                        .unwrap_or_else(|| format!("tool-{}", run_input.run_id));
                    let mut tool_call = acp::ToolCall::new(
                        acp::ToolCallId::new(tool_call_id),
                        format!("Execute {}", proposal.tool_name),
                    )
                    .kind(acp::ToolKind::Execute)
                    .status(acp::ToolCallStatus::Pending);
                    if let Some(input_json) = parse_json_bytes(proposal.input_json.as_slice()) {
                        tool_call = tool_call.raw_input(input_json);
                    }
                    self.send_session_update(
                        &arguments.session_id,
                        acp::SessionUpdate::ToolCall(tool_call),
                    )
                    .await?;
                }
                Some(common_v1::run_stream_event::Body::ToolApprovalRequest(approval)) => {
                    if let Some(request) =
                        build_tool_permission_request(&arguments.session_id, &approval)?
                    {
                        let permission = self.request_permission(request).await?;
                        let (approved, reason, decision_scope, decision_scope_ttl_ms) =
                            map_permission_outcome(permission);
                        let approval_request = build_tool_approval_stream_request(
                            binding.gateway_session_id_ulid.as_str(),
                            run_input.run_id.as_str(),
                            approval
                                .proposal_id
                                .as_ref()
                                .map(|value| value.ulid.as_str())
                                .unwrap_or(""),
                            approval.approval_id.as_ref().map(|value| value.ulid.as_str()),
                            approved,
                            reason.as_str(),
                            decision_scope,
                            decision_scope_ttl_ms,
                        )?;
                        request_tx.send(approval_request).await.map_err(|_| {
                            acp::Error::new(
                                -32603,
                                "gateway approval request channel closed before sending response",
                            )
                        })?;
                    }
                }
                Some(common_v1::run_stream_event::Body::ToolResult(result)) => {
                    let tool_call_id = result
                        .proposal_id
                        .as_ref()
                        .map(|value| value.ulid.clone())
                        .unwrap_or_else(|| format!("tool-{}", run_input.run_id));
                    let mut update_fields =
                        acp::ToolCallUpdateFields::new().status(if result.success {
                            acp::ToolCallStatus::Completed
                        } else {
                            acp::ToolCallStatus::Failed
                        });
                    if let Some(output_json) = parse_json_bytes(result.output_json.as_slice()) {
                        update_fields = update_fields.raw_output(output_json);
                    } else if !result.error.trim().is_empty() {
                        update_fields =
                            update_fields.raw_output(Value::String(result.error.clone()));
                    }
                    let update =
                        acp::ToolCallUpdate::new(acp::ToolCallId::new(tool_call_id), update_fields);
                    self.send_session_update(
                        &arguments.session_id,
                        acp::SessionUpdate::ToolCallUpdate(update),
                    )
                    .await?;
                }
                Some(common_v1::run_stream_event::Body::Status(status)) => {
                    if status.kind == common_v1::stream_status::StatusKind::Failed as i32 {
                        stop_reason = if status.message.to_ascii_lowercase().contains("cancel") {
                            acp::StopReason::Cancelled
                        } else {
                            acp::StopReason::Refusal
                        };
                    } else if status.kind == common_v1::stream_status::StatusKind::Done as i32 {
                        stop_reason = acp::StopReason::EndTurn;
                    }
                }
                _ => {}
            }
        }

        self.lock_state()?.active_runs.remove(arguments.session_id.0.as_ref());
        Ok(acp::PromptResponse::new(stop_reason))
    }
}

#[async_trait::async_trait(?Send)]
impl acp::Agent for PalyraAcpAgent {
    async fn initialize(
        &self,
        arguments: acp::InitializeRequest,
    ) -> acp::Result<acp::InitializeResponse> {
        let capabilities = acp::AgentCapabilities::new().load_session(true).session_capabilities(
            acp::SessionCapabilities::new().list(acp::SessionListCapabilities::new()),
        );
        Ok(acp::InitializeResponse::new(arguments.protocol_version)
            .agent_capabilities(capabilities)
            .agent_info(
                acp::Implementation::new("palyra-cli", env!("CARGO_PKG_VERSION"))
                    .title("Palyra ACP Bridge"),
            ))
    }

    async fn authenticate(
        &self,
        _arguments: acp::AuthenticateRequest,
    ) -> acp::Result<acp::AuthenticateResponse> {
        Ok(acp::AuthenticateResponse::default())
    }

    async fn new_session(
        &self,
        arguments: acp::NewSessionRequest,
    ) -> acp::Result<acp::NewSessionResponse> {
        let overrides = Self::session_overrides(&arguments.meta)?;
        let requested_session_key =
            overrides.session_key.unwrap_or_else(|| format!("agent:main:{}", ulid::Ulid::new()));
        let mut binding = self
            .resolve_gateway_session(
                requested_session_key,
                overrides.session_label,
                overrides.require_existing.unwrap_or(false),
                overrides.reset_session.unwrap_or(false),
            )
            .await?;
        binding.cwd = arguments.cwd;

        self.lock_state()?.sessions.insert(binding.session_key.clone(), binding.clone());

        Ok(acp::NewSessionResponse::new(acp::SessionId::new(binding.session_key)))
    }

    async fn load_session(
        &self,
        arguments: acp::LoadSessionRequest,
    ) -> acp::Result<acp::LoadSessionResponse> {
        let overrides = Self::session_overrides(&arguments.meta)?;
        let mut binding = self
            .resolve_gateway_session(
                overrides.session_key.unwrap_or_else(|| arguments.session_id.0.as_ref().to_owned()),
                overrides.session_label,
                overrides.require_existing.unwrap_or(true),
                overrides.reset_session.unwrap_or(false),
            )
            .await?;
        binding.cwd = arguments.cwd;

        self.lock_state()?.sessions.insert(arguments.session_id.0.as_ref().to_owned(), binding);
        Ok(acp::LoadSessionResponse::new())
    }

    async fn prompt(&self, arguments: acp::PromptRequest) -> acp::Result<acp::PromptResponse> {
        self.execute_prompt(arguments).await
    }

    async fn cancel(&self, arguments: acp::CancelNotification) -> acp::Result<()> {
        self.abort_run_for_session(&arguments.session_id).await
    }

    async fn set_session_mode(
        &self,
        _arguments: acp::SetSessionModeRequest,
    ) -> acp::Result<acp::SetSessionModeResponse> {
        Ok(acp::SetSessionModeResponse::new())
    }

    async fn set_session_config_option(
        &self,
        _arguments: acp::SetSessionConfigOptionRequest,
    ) -> acp::Result<acp::SetSessionConfigOptionResponse> {
        Ok(acp::SetSessionConfigOptionResponse::new(Vec::new()))
    }

    async fn list_sessions(
        &self,
        arguments: acp::ListSessionsRequest,
    ) -> acp::Result<acp::ListSessionsResponse> {
        let response = self.list_gateway_sessions(arguments.cursor).await?;
        let state = self.lock_state()?;
        let sessions = response
            .sessions
            .into_iter()
            .map(|session| {
                let session_key = if session.session_key.trim().is_empty() {
                    session
                        .session_id
                        .as_ref()
                        .map(|value| value.ulid.clone())
                        .unwrap_or_else(|| format!("session-{}", ulid::Ulid::new()))
                } else {
                    session.session_key
                };
                let cwd = state
                    .sessions
                    .get(&session_key)
                    .map(|binding| binding.cwd.clone())
                    .unwrap_or_else(|| self.default_cwd.clone());
                acp::SessionInfo::new(acp::SessionId::new(session_key), cwd)
                    .title(non_empty(Some(session.session_label)))
            })
            .collect::<Vec<_>>();
        Ok(acp::ListSessionsResponse::new(sessions)
            .next_cursor(non_empty(Some(response.next_after_session_key))))
    }
}

pub fn run_agent_acp_bridge(
    connection: AgentConnection,
    allow_sensitive_tools: bool,
) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(async move {
        let local_set = tokio::task::LocalSet::new();
        local_set
            .run_until(async move {
                let (client_request_tx, mut client_request_rx) =
                    mpsc::unbounded_channel::<ClientBridgeRequest>();
                let state = Arc::new(Mutex::new(BridgeState::default()));
                let default_cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
                let bridge = PalyraAcpAgent::new(
                    connection,
                    allow_sensitive_tools,
                    state,
                    client_request_tx,
                    default_cwd,
                );

                let outgoing = AllowStdIo::new(std::io::stdout());
                let incoming = AllowStdIo::new(std::io::stdin());
                let (conn, handle_io) =
                    acp::AgentSideConnection::new(bridge, outgoing, incoming, |fut| {
                        tokio::task::spawn_local(fut);
                    });

                let client_side_worker = tokio::task::spawn_local(async move {
                    while let Some(request) = client_request_rx.recv().await {
                        match request {
                            ClientBridgeRequest::SessionUpdate { notification, response_tx } => {
                                let _ =
                                    response_tx.send(conn.session_notification(notification).await);
                            }
                            ClientBridgeRequest::RequestPermission { request, response_tx } => {
                                let _ = response_tx.send(conn.request_permission(request).await);
                            }
                        }
                    }
                });

                let io_result = handle_io.await;
                client_side_worker.abort();
                io_result.context("ACP stdio bridge I/O loop failed")
            })
            .await
    })
}

fn acp_internal_error(error: impl std::fmt::Display) -> acp::Error {
    acp::Error::new(-32603, error.to_string())
}

fn map_gateway_status_error(status: tonic::Status) -> acp::Error {
    if status.code() == tonic::Code::InvalidArgument {
        return acp::Error::new(-32602, status.message().to_owned());
    }
    acp::Error::new(-32603, status.message().to_owned())
}

async fn connect_gateway_client(
    connection: &AgentConnection,
) -> Result<gateway_v1::gateway_service_client::GatewayServiceClient<tonic::transport::Channel>> {
    gateway_v1::gateway_service_client::GatewayServiceClient::connect(connection.grpc_url.clone())
        .await
        .with_context(|| format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url))
}

async fn open_run_stream(
    connection: &AgentConnection,
    initial_request: common_v1::RunStreamRequest,
) -> Result<(tonic::Streaming<common_v1::RunStreamEvent>, mpsc::Sender<common_v1::RunStreamRequest>)>
{
    let mut client = connect_gateway_client(connection).await?;
    let (request_tx, request_rx) = mpsc::channel(16);
    request_tx
        .send(initial_request)
        .await
        .map_err(|_| anyhow::anyhow!("failed to queue initial RunStream request message"))?;

    let mut stream_request = Request::new(ReceiverStream::new(request_rx));
    inject_run_stream_metadata(stream_request.metadata_mut(), connection)
        .context("failed to inject gRPC metadata for RunStream")?;
    let stream = client
        .run_stream(stream_request)
        .await
        .context("failed to call gateway RunStream")?
        .into_inner();
    Ok((stream, request_tx))
}

fn parse_json_bytes(raw: &[u8]) -> Option<Value> {
    if raw.is_empty() {
        return None;
    }
    serde_json::from_slice::<Value>(raw).ok()
}

fn build_tool_permission_request(
    session_id: &acp::SessionId,
    approval: &common_v1::ToolApprovalRequest,
) -> acp::Result<Option<acp::RequestPermissionRequest>> {
    let Some(proposal_id) = approval.proposal_id.as_ref().map(|value| value.ulid.clone()) else {
        return Ok(None);
    };

    let prompt = approval.prompt.as_ref();
    let title = prompt
        .map(|value| value.title.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("Execute {}", approval.tool_name));
    let mut fields = acp::ToolCallUpdateFields::new()
        .title(title)
        .kind(acp::ToolKind::Execute)
        .status(acp::ToolCallStatus::Pending);
    if let Some(prompt) = prompt {
        let details_json = parse_json_bytes(prompt.details_json.as_slice())
            .unwrap_or_else(|| json!({ "raw": String::from_utf8_lossy(prompt.details_json.as_slice()).to_string() }));
        fields = fields.raw_input(json!({
            "tool_name": approval.tool_name,
            "request_summary": approval.request_summary,
            "prompt": {
                "subject_id": prompt.subject_id,
                "summary": prompt.summary,
                "risk_level": prompt.risk_level,
                "policy_explanation": prompt.policy_explanation,
                "details_json": details_json,
            }
        }));
    } else if let Some(input_json) = parse_json_bytes(approval.input_json.as_slice()) {
        fields = fields.raw_input(input_json);
    }

    let tool_call = acp::ToolCallUpdate::new(acp::ToolCallId::new(proposal_id), fields);
    let options = vec![
        acp::PermissionOption::new(
            acp::PermissionOptionId::new(PERMISSION_ALLOW_ONCE),
            "Allow once",
            acp::PermissionOptionKind::AllowOnce,
        ),
        acp::PermissionOption::new(
            acp::PermissionOptionId::new(PERMISSION_ALLOW_ALWAYS),
            "Allow always",
            acp::PermissionOptionKind::AllowAlways,
        ),
        acp::PermissionOption::new(
            acp::PermissionOptionId::new(PERMISSION_REJECT_ONCE),
            "Reject once",
            acp::PermissionOptionKind::RejectOnce,
        ),
        acp::PermissionOption::new(
            acp::PermissionOptionId::new(PERMISSION_REJECT_ALWAYS),
            "Reject always",
            acp::PermissionOptionKind::RejectAlways,
        ),
    ];
    Ok(Some(acp::RequestPermissionRequest::new(session_id.clone(), tool_call, options)))
}

fn map_permission_outcome(
    response: acp::RequestPermissionResponse,
) -> (bool, String, i32, Option<i64>) {
    match response.outcome {
        acp::RequestPermissionOutcome::Cancelled => (
            false,
            "cancelled_by_client".to_owned(),
            common_v1::ApprovalDecisionScope::Once as i32,
            None,
        ),
        acp::RequestPermissionOutcome::Selected(selection) => {
            let option_id = selection.option_id.0.as_ref();
            if option_id == PERMISSION_ALLOW_ONCE {
                (
                    true,
                    format!("approved:{option_id}"),
                    common_v1::ApprovalDecisionScope::Once as i32,
                    None,
                )
            } else if option_id == PERMISSION_ALLOW_ALWAYS {
                (
                    true,
                    format!("approved:{option_id}"),
                    common_v1::ApprovalDecisionScope::Session as i32,
                    None,
                )
            } else if option_id == PERMISSION_REJECT_ALWAYS {
                (
                    false,
                    format!("denied:{option_id}"),
                    common_v1::ApprovalDecisionScope::Session as i32,
                    None,
                )
            } else {
                (
                    false,
                    format!("denied:{option_id}"),
                    common_v1::ApprovalDecisionScope::Once as i32,
                    None,
                )
            }
        }
        _ => (
            false,
            "denied:unsupported_permission_outcome".to_owned(),
            common_v1::ApprovalDecisionScope::Once as i32,
            None,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_tool_approval_stream_request(
    session_id_ulid: &str,
    run_id_ulid: &str,
    proposal_id_ulid: &str,
    approval_id_ulid: Option<&str>,
    approved: bool,
    reason: &str,
    decision_scope: i32,
    decision_scope_ttl_ms: Option<i64>,
) -> acp::Result<common_v1::RunStreamRequest> {
    if proposal_id_ulid.trim().is_empty() {
        return Err(acp::Error::new(
            -32602,
            "cannot forward tool approval response without proposal_id",
        ));
    }
    Ok(common_v1::RunStreamRequest {
        v: 1,
        session_id: Some(common_v1::CanonicalId { ulid: session_id_ulid.to_owned() }),
        run_id: Some(common_v1::CanonicalId { ulid: run_id_ulid.to_owned() }),
        input: None,
        allow_sensitive_tools: false,
        session_key: String::new(),
        session_label: String::new(),
        reset_session: false,
        require_existing: false,
        tool_approval_response: Some(common_v1::ToolApprovalResponse {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id_ulid.to_owned() }),
            approved,
            reason: reason.to_owned(),
            approval_id: approval_id_ulid
                .map(|value| common_v1::CanonicalId { ulid: value.to_owned() }),
            decision_scope,
            decision_scope_ttl_ms: decision_scope_ttl_ms.unwrap_or_default(),
        }),
    })
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.and_then(|candidate| {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{acp, AgentConnection, BridgeState, ClientBridgeRequest, PalyraAcpAgent};
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    };
    use tokio::sync::mpsc;

    fn test_agent() -> PalyraAcpAgent {
        let (client_request_tx, _client_request_rx) =
            mpsc::unbounded_channel::<ClientBridgeRequest>();
        PalyraAcpAgent::new(
            AgentConnection {
                grpc_url: "http://127.0.0.1:7443".to_owned(),
                token: None,
                principal: "user:test".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: "cli".to_owned(),
            },
            false,
            Arc::new(Mutex::new(BridgeState::default())),
            client_request_tx,
            PathBuf::from("."),
        )
    }

    #[test]
    fn prompt_text_includes_resource_link_blocks() {
        let prompt = vec![
            acp::ContentBlock::from("Summarize the context"),
            acp::ContentBlock::ResourceLink(
                acp::ResourceLink::new("runbook", "https://example.test/runbook").title("Runbook"),
            ),
        ];

        let prompt = PalyraAcpAgent::prompt_text(&prompt);

        assert_eq!(prompt, "Summarize the context\nlink (Runbook): https://example.test/runbook");
    }

    #[tokio::test]
    async fn initialize_negotiates_requested_protocol_version() {
        let agent = test_agent();
        let requested_version = acp::ProtocolVersion::V0;
        let response = <PalyraAcpAgent as acp::Agent>::initialize(
            &agent,
            acp::InitializeRequest::new(requested_version.clone()),
        )
        .await
        .expect("initialize must succeed");

        assert_eq!(response.protocol_version, requested_version);
    }
}
