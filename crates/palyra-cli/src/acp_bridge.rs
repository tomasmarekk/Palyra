//! ACP (Agent Client Protocol) stdio bridge: exposes the Palyra gateway as an
//! ACP agent so external editors and IDE clients can drive sessions, prompts,
//! tool approvals, and session listings over stdin/stdout JSON-RPC. Bindings
//! map ACP session ids onto gateway sessions and optional daemon-side state.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use agent_client_protocol::{self as acp, Client as _};
use anyhow::{Context, Result};
use futures::io::AllowStdIo;
use palyra_control_plane::ControlPlaneClient;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};

use crate::{
    app, build_agent_run_input, build_run_stream_request, build_runtime,
    client::{control_plane, runtime::GatewayRuntimeClient},
    proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    AgentApprovalMode, AgentConnection, AgentRunInputArgs, SessionResolveInput,
};

// `_meta` extension keys accepted on session/new, session/load, and
// session/prompt to override the CLI-provided session defaults per request.
const META_SESSION_KEY: &str = "sessionKey";
const META_SESSION_LABEL: &str = "sessionLabel";
const META_RESET_SESSION: &str = "resetSession";
const META_REQUIRE_EXISTING: &str = "requireExisting";

// Permission option ids surfaced to the ACP client; the `-always` variants map
// to a session-scoped `ApprovalDecisionScope` when forwarded to the gateway.
const PERMISSION_ALLOW_ONCE: &str = "allow-once";
const PERMISSION_ALLOW_ALWAYS: &str = "allow-always";
const PERMISSION_REJECT_ONCE: &str = "reject-once";
const PERMISSION_REJECT_ALWAYS: &str = "reject-always";

/// Resolved link between an ACP session id and its gateway session identity.
#[derive(Debug, Clone)]
struct SessionBinding {
    gateway_session_id_ulid: String,
    session_key: String,
    session_label: Option<String>,
    cwd: PathBuf,
}

/// Optional daemon control-plane channel used to persist session bindings and
/// session config beyond the lifetime of this bridge process.
#[derive(Clone)]
struct AcpDaemonControl {
    client: Arc<TokioMutex<ControlPlaneClient>>,
    principal: String,
    device_id: String,
    channel: Option<String>,
}

impl AcpDaemonControl {
    /// Sends one `console/v1/acp/command` request and unwraps its envelope.
    async fn command(&self, command: &str, params: Value) -> acp::Result<Value> {
        // The `client` block restates the full handshake (scopes plus
        // capabilities) on every command because the console ACP endpoint is
        // stateless across requests.
        let payload = json!({
            "client": {
                "protocol_version": 1,
                "client_id": "palyra-cli-acp",
                "transport": "stdio",
                "owner_principal": self.principal.clone(),
                "device_id": self.device_id.clone(),
                "channel": self.channel.clone(),
                "scopes": [
                    "sessions:read",
                    "sessions:write",
                    "runs:read",
                    "runs:write",
                    "approvals:read",
                    "approvals:write",
                    "bindings:read",
                    "bindings:write",
                    "events:read",
                    "events:sensitive"
                ],
                "capabilities": [
                    "session_list",
                    "session_load",
                    "session_new",
                    "session_replay",
                    "run_control",
                    "approval_bridge",
                    "pending_prompts",
                    "session_config",
                    "session_fork",
                    "session_compact",
                    "session_explain",
                    "conversation_bindings",
                    "binding_repair",
                    "sensitive_replay"
                ]
            },
            "command": {
                "request_id": format!("acp_req_{}", ulid::Ulid::new()),
                "command": command,
                "params": params,
                "idempotency_key": format!("acp_idem_{}", ulid::Ulid::new())
            }
        });
        let client = self.client.lock().await;
        let response =
            client.post_json_value("console/v1/acp/command", &payload).await.map_err(|error| {
                acp::Error::new(-32603, format!("daemon ACP control-plane request failed: {error}"))
            })?;
        if response.get("ok").and_then(Value::as_bool).unwrap_or(false) {
            return Ok(response.get("result").cloned().unwrap_or_else(|| json!({})));
        }
        let error = response.get("error").cloned().unwrap_or_else(|| json!({}));
        let code = error.get("code").and_then(Value::as_str).unwrap_or("acp/daemon_error");
        let message =
            error.get("message").and_then(Value::as_str).unwrap_or("daemon ACP command failed");
        Err(acp::Error::new(-32603, format!("{code}: {message}")))
    }
}

/// In-memory binding and active-run registry shared across ACP handlers.
///
/// Bindings are indexed three ways because clients may echo any of the three
/// identifiers (ACP session id, gateway session key, gateway session ulid) as
/// the ACP `SessionId` on follow-up requests.
#[derive(Debug, Default)]
struct BridgeState {
    bindings_by_acp_session_id: HashMap<String, SessionBinding>,
    bindings_by_session_key: HashMap<String, SessionBinding>,
    bindings_by_gateway_session_id: HashMap<String, SessionBinding>,
    active_runs: HashMap<String, String>,
}

impl BridgeState {
    fn remember_binding(&mut self, acp_session_id: &str, binding: SessionBinding) {
        self.bindings_by_acp_session_id.insert(acp_session_id.to_owned(), binding.clone());
        self.bindings_by_session_key.insert(binding.session_key.clone(), binding.clone());
        self.bindings_by_gateway_session_id
            .insert(binding.gateway_session_id_ulid.clone(), binding);
    }

    fn lookup_binding(&self, acp_session_id: &str) -> Option<SessionBinding> {
        self.bindings_by_acp_session_id
            .get(acp_session_id)
            .or_else(|| self.bindings_by_session_key.get(acp_session_id))
            .or_else(|| self.bindings_by_gateway_session_id.get(acp_session_id))
            .cloned()
    }

    fn lookup_binding_for_gateway_session(
        &self,
        session: &gateway_v1::SessionSummary,
    ) -> Option<SessionBinding> {
        let gateway_session_id = session.session_id.as_ref().map(|value| value.ulid.as_str());
        let session_key = non_empty(Some(session.session_key.clone()));
        session_key
            .as_deref()
            .and_then(|value| self.bindings_by_session_key.get(value))
            .or_else(|| {
                gateway_session_id.and_then(|value| self.bindings_by_gateway_session_id.get(value))
            })
            .cloned()
    }
}

struct ActiveRunGuard {
    state: Arc<Mutex<BridgeState>>,
    acp_session_id: String,
}

impl ActiveRunGuard {
    fn insert(
        state: &Arc<Mutex<BridgeState>>,
        acp_session_id: &str,
        run_id: String,
    ) -> acp::Result<Self> {
        let mut bridge_state = state.lock().map_err(|_| {
            acp::Error::new(-32603, "ACP bridge state lock poisoned while registering active run")
        })?;
        bridge_state.active_runs.insert(acp_session_id.to_owned(), run_id);
        drop(bridge_state);
        Ok(Self { state: Arc::clone(state), acp_session_id: acp_session_id.to_owned() })
    }
}

impl Drop for ActiveRunGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.active_runs.remove(self.acp_session_id.as_str());
        }
    }
}

/// Client-directed calls funneled through one dispatch task so agent handlers
/// can stay `Clone` while `AgentSideConnection` is owned by a single task.
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

/// ACP `Agent` implementation backed by the gateway gRPC runtime client.
#[derive(Clone)]
struct PalyraAcpAgent {
    connection: AgentConnection,
    daemon: Option<AcpDaemonControl>,
    allow_sensitive_tools: bool,
    session_defaults: AcpSessionDefaults,
    state: Arc<Mutex<BridgeState>>,
    client_request_tx: mpsc::UnboundedSender<ClientBridgeRequest>,
    default_cwd: PathBuf,
}

/// Per-request `_meta` overrides parsed from an ACP request, if any.
#[derive(Debug, Default, Clone)]
struct SessionMetaOverrides {
    session_key: Option<String>,
    session_label: Option<String>,
    reset_session: Option<bool>,
    require_existing: Option<bool>,
}

/// Session defaults supplied via CLI flags, applied whenever an ACP request
/// does not carry the corresponding `_meta` override.
#[derive(Debug, Default, Clone)]
pub(crate) struct AcpSessionDefaults {
    pub(crate) session_key: Option<String>,
    pub(crate) session_label: Option<String>,
    pub(crate) require_existing: bool,
    pub(crate) reset_session: bool,
}

impl PalyraAcpAgent {
    fn new(
        connection: AgentConnection,
        daemon: Option<AcpDaemonControl>,
        allow_sensitive_tools: bool,
        session_defaults: AcpSessionDefaults,
        state: Arc<Mutex<BridgeState>>,
        client_request_tx: mpsc::UnboundedSender<ClientBridgeRequest>,
        default_cwd: PathBuf,
    ) -> Self {
        Self {
            connection,
            daemon,
            allow_sensitive_tools,
            session_defaults,
            state,
            client_request_tx,
            default_cwd,
        }
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

    // Gateway runs accept plain text, so only text-bearing blocks are kept:
    // text, embedded text resources, and resource links (serialized inline).
    // Image/audio blocks are intentionally dropped.
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
        let mut client = GatewayRuntimeClient::connect(self.connection.clone())
            .await
            .map_err(acp_internal_error)?;
        let response = client
            .resolve_session(SessionResolveInput {
                session_id: None,
                session_key: requested_session_key.clone(),
                session_label: session_label.clone().unwrap_or_default(),
                require_existing,
                reset_session,
            })
            .await
            .map_err(acp_internal_error)?;
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

    async fn persist_daemon_binding(
        &self,
        acp_session_id: &str,
        binding: &SessionBinding,
        config: Value,
    ) -> acp::Result<()> {
        let Some(daemon) = &self.daemon else {
            return Ok(());
        };
        daemon
            .command(
                "session.load",
                json!({
                    "session_id": binding.gateway_session_id_ulid.clone(),
                    "acp_session_id": acp_session_id,
                    "config": config,
                }),
            )
            .await?;
        Ok(())
    }

    async fn ensure_binding_for_acp_session(
        &self,
        acp_session_id: &acp::SessionId,
        overrides: &SessionMetaOverrides,
        default_require_existing: bool,
    ) -> acp::Result<SessionBinding> {
        let session_id_value = acp_session_id.0.as_ref().to_owned();
        if let Some(binding) = self.lock_state()?.lookup_binding(session_id_value.as_str()) {
            if overrides.session_key.is_none()
                && overrides.session_label.is_none()
                && overrides.reset_session.is_none()
                && overrides.require_existing.is_none()
            {
                return Ok(binding);
            }
        }

        let requested_session_key = overrides
            .session_key
            .clone()
            .or_else(|| self.session_defaults.session_key.clone())
            .unwrap_or_else(|| session_id_value.clone());
        let binding = self
            .resolve_gateway_session(
                requested_session_key,
                overrides
                    .session_label
                    .clone()
                    .or_else(|| self.session_defaults.session_label.clone()),
                overrides
                    .require_existing
                    .unwrap_or(default_require_existing || self.session_defaults.require_existing),
                overrides.reset_session.unwrap_or(self.session_defaults.reset_session),
            )
            .await?;
        self.persist_daemon_binding(
            session_id_value.as_str(),
            &binding,
            json!({
                "session_key": binding.session_key.clone(),
                "session_label": binding.session_label.clone(),
                "source": "prompt",
            }),
        )
        .await?;
        self.lock_state()?.remember_binding(session_id_value.as_str(), binding.clone());
        Ok(binding)
    }

    async fn list_daemon_sessions(
        &self,
        cursor: Option<String>,
    ) -> acp::Result<Option<acp::ListSessionsResponse>> {
        let Some(daemon) = &self.daemon else {
            return Ok(None);
        };
        let response = daemon
            .command(
                "session.list",
                json!({
                    "after_session_key": cursor,
                    "limit": 100,
                    "include_archived": false,
                }),
            )
            .await?;
        Ok(Some(map_daemon_sessions_response(response, &self.default_cwd)))
    }

    async fn list_gateway_sessions(
        &self,
        cursor: Option<String>,
    ) -> acp::Result<gateway_v1::ListSessionsResponse> {
        let mut client = GatewayRuntimeClient::connect(self.connection.clone())
            .await
            .map_err(acp_internal_error)?;
        client.list_sessions(cursor, false, Some(100), None).await.map_err(acp_internal_error)
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

        let mut client = GatewayRuntimeClient::connect(self.connection.clone())
            .await
            .map_err(acp_internal_error)?;
        let _ = client
            .abort_run(run_id, Some("acp_session_cancel".to_owned()))
            .await
            .map_err(acp_internal_error)?;
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

        let run_input = build_agent_run_input(AgentRunInputArgs {
            session_id: Some(common_v1::CanonicalId {
                ulid: binding.gateway_session_id_ulid.clone(),
            }),
            session_key: None,
            session_label: None,
            require_existing: false,
            reset_session: false,
            run_id: None,
            prompt,
            allow_sensitive_tools: self.allow_sensitive_tools,
            interrupt_active_run: false,
            approval_mode: AgentApprovalMode::Prompt,
            origin_kind: None,
            origin_run_id: None,
            parameter_delta_json: None,
        })
        .map_err(acp_internal_error)?;
        let mut initial_request =
            build_run_stream_request(&run_input).map_err(acp_internal_error)?;
        initial_request.session_key = binding.session_key.clone();
        initial_request.session_label = binding.session_label.clone().unwrap_or_default();
        initial_request.reset_session = session_overrides.reset_session.unwrap_or(false);
        initial_request.require_existing = session_overrides.require_existing.unwrap_or(false);

        let mut client = GatewayRuntimeClient::connect(self.connection.clone())
            .await
            .map_err(acp_internal_error)?;
        let mut run_stream =
            client.open_run_stream(initial_request).await.map_err(acp_internal_error)?;

        let _active_run_guard = ActiveRunGuard::insert(
            &self.state,
            arguments.session_id.0.as_ref(),
            run_input.run_id.clone(),
        )?;

        let mut stop_reason = acp::StopReason::EndTurn;
        while let Some(event) = run_stream.next_event().await.map_err(acp_internal_error)? {
            match event.body {
                Some(common_v1::run_stream_event::Body::ModelToken(token))
                    if !token.token.is_empty() =>
                {
                    self.send_session_update(
                        &arguments.session_id,
                        acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(
                            acp::ContentBlock::from(token.token),
                        )),
                    )
                    .await?;
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
                        run_stream
                            .send_request(approval_request)
                            .await
                            .map_err(acp_internal_error)?;
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
                        // The stream contract has no structured cancelled
                        // status; cancellations arrive as Failed with a
                        // cancel-flavored message, hence the text heuristic.
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
        self.send_verification_summary_update(&arguments.session_id, run_input.run_id.as_str())
            .await?;

        Ok(acp::PromptResponse::new(stop_reason))
    }

    async fn send_verification_summary_update(
        &self,
        session_id: &acp::SessionId,
        run_id: &str,
    ) -> acp::Result<()> {
        let Some(text) = self.verification_summary_text_for_run(run_id).await? else {
            return Ok(());
        };
        self.send_session_update(
            session_id,
            acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(acp::ContentBlock::from(
                text,
            ))),
        )
        .await
    }

    async fn verification_summary_text_for_run(&self, run_id: &str) -> acp::Result<Option<String>> {
        let Some(daemon) = &self.daemon else {
            return Ok(None);
        };
        match daemon.command("run.get", json!({ "run_id": run_id })).await {
            Ok(response) => {
                Ok(response.get("verification_summary").and_then(format_acp_verification_summary))
            }
            Err(_) => Ok(Some(
                "Verification evidence: unavailable; reason=verification.status.fetch_failed."
                    .to_owned(),
            )),
        }
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
        let requested_session_key = format!("agent:main:{}", ulid::Ulid::new());
        let mut binding = self
            .resolve_gateway_session(
                overrides
                    .session_key
                    .or_else(|| self.session_defaults.session_key.clone())
                    .unwrap_or(requested_session_key),
                overrides.session_label.or_else(|| self.session_defaults.session_label.clone()),
                overrides.require_existing.unwrap_or(self.session_defaults.require_existing),
                overrides.reset_session.unwrap_or(self.session_defaults.reset_session),
            )
            .await?;
        binding.cwd = arguments.cwd;

        self.persist_daemon_binding(
            binding.session_key.as_str(),
            &binding,
            json!({
                "session_key": binding.session_key.clone(),
                "session_label": binding.session_label.clone(),
                "source": "new_session",
            }),
        )
        .await?;
        self.lock_state()?.remember_binding(binding.session_key.as_str(), binding.clone());

        Ok(acp::NewSessionResponse::new(acp::SessionId::new(binding.session_key)))
    }

    async fn load_session(
        &self,
        arguments: acp::LoadSessionRequest,
    ) -> acp::Result<acp::LoadSessionResponse> {
        let overrides = Self::session_overrides(&arguments.meta)?;
        let mut binding = self
            .resolve_gateway_session(
                overrides
                    .session_key
                    .or_else(|| self.session_defaults.session_key.clone())
                    .unwrap_or_else(|| arguments.session_id.0.as_ref().to_owned()),
                overrides.session_label.or_else(|| self.session_defaults.session_label.clone()),
                overrides.require_existing.unwrap_or(true),
                overrides.reset_session.unwrap_or(self.session_defaults.reset_session),
            )
            .await?;
        binding.cwd = arguments.cwd;

        self.persist_daemon_binding(
            arguments.session_id.0.as_ref(),
            &binding,
            json!({
                "session_key": binding.session_key.clone(),
                "session_label": binding.session_label.clone(),
                "source": "load_session",
            }),
        )
        .await?;
        self.lock_state()?.remember_binding(arguments.session_id.0.as_ref(), binding);
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
        arguments: acp::SetSessionModeRequest,
    ) -> acp::Result<acp::SetSessionModeResponse> {
        if let Some(daemon) = &self.daemon {
            daemon
                .command(
                    "session.mode.set",
                    json!({
                        "acp_session_id": arguments.session_id.0.as_ref(),
                        "mode": arguments.mode_id.0.as_ref(),
                    }),
                )
                .await?;
        }
        Ok(acp::SetSessionModeResponse::new())
    }

    async fn set_session_config_option(
        &self,
        arguments: acp::SetSessionConfigOptionRequest,
    ) -> acp::Result<acp::SetSessionConfigOptionResponse> {
        if let Some(daemon) = &self.daemon {
            let mut config = serde_json::Map::new();
            config.insert(
                arguments.config_id.0.as_ref().to_owned(),
                Value::String(arguments.value.0.as_ref().to_owned()),
            );
            daemon
                .command(
                    "session.config.set",
                    json!({
                        "acp_session_id": arguments.session_id.0.as_ref(),
                        "config": Value::Object(config),
                    }),
                )
                .await?;
        }
        Ok(acp::SetSessionConfigOptionResponse::new(Vec::new()))
    }

    async fn list_sessions(
        &self,
        arguments: acp::ListSessionsRequest,
    ) -> acp::Result<acp::ListSessionsResponse> {
        // Prefer the daemon listing (it includes persisted bindings); fall
        // back to the gateway list when no daemon control-plane is attached.
        if let Some(response) = self.list_daemon_sessions(arguments.cursor.clone()).await? {
            return Ok(response);
        }
        let response = self.list_gateway_sessions(arguments.cursor).await?;
        let state = self.lock_state()?;
        Ok(map_list_sessions_response(response, &state, &self.default_cwd))
    }
}

/// Runs the ACP stdio bridge until the client closes the stream.
///
/// Blocks the calling thread on a dedicated Tokio runtime; the connection
/// futures are `!Send`, so everything is driven on a single-thread `LocalSet`.
///
/// # Errors
/// Returns an error when the runtime cannot be built, the daemon ACP
/// control-plane connection fails, or the stdio I/O loop terminates abnormally.
pub fn run_agent_acp_bridge(
    connection: AgentConnection,
    control_plane_overrides: app::ConnectionOverrides,
    allow_sensitive_tools: bool,
    session_defaults: AcpSessionDefaults,
) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(async move {
        let local_set = tokio::task::LocalSet::new();
        local_set
            .run_until(async move {
                // Unbounded is safe here: every sender awaits its oneshot
                // response before sending again, so the queue depth is bounded
                // by the number of in-flight ACP requests.
                let (client_request_tx, mut client_request_rx) =
                    mpsc::unbounded_channel::<ClientBridgeRequest>();
                let state = Arc::new(Mutex::new(BridgeState::default()));
                let default_cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
                let daemon_context = control_plane::connect_admin_console(control_plane_overrides)
                    .await
                    .context(
                        "failed to connect daemon ACP control-plane; ensure palyrad is running and ACP is enabled",
                    )?;
                let daemon = AcpDaemonControl {
                    client: Arc::new(TokioMutex::new(daemon_context.client)),
                    principal: connection.principal.clone(),
                    device_id: connection.device_id.clone(),
                    channel: Some(connection.channel.clone()),
                };
                let bridge = PalyraAcpAgent::new(
                    connection,
                    Some(daemon),
                    allow_sensitive_tools,
                    session_defaults,
                    state,
                    client_request_tx,
                    default_cwd,
                );

                // Blocking stdio wrapped as async is acceptable: this runtime
                // exists solely for the bridge, so a stalled read cannot
                // starve unrelated tasks.
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

fn parse_json_bytes(raw: &[u8]) -> Option<Value> {
    if raw.is_empty() {
        return None;
    }
    serde_json::from_slice::<Value>(raw).ok()
}

fn format_acp_verification_summary(summary: &Value) -> Option<String> {
    if !summary.is_object() {
        return None;
    }
    let decision = summary
        .pointer("/latest_verification_status/decision")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let state = summary.get("state").and_then(Value::as_str).unwrap_or("unknown");
    let final_answer_allowed = summary
        .get("final_answer_allowed")
        .and_then(Value::as_bool)
        .map(|allowed| allowed.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let reason = summary
        .get("final_answer_allowed_because")
        .and_then(Value::as_str)
        .or_else(|| {
            summary.pointer("/latest_verification_status/reason_codes/0").and_then(Value::as_str)
        })
        .unwrap_or("verification.status.unknown");
    let command_count =
        summary.get("commands_executed").and_then(Value::as_array).map(Vec::len).unwrap_or(0);
    let unverified_mutations =
        summary.get("unverified_mutations").and_then(Value::as_array).map(Vec::len).unwrap_or(0);
    let stale_requirements =
        summary.get("stale_evidence_reasons").and_then(Value::as_array).map(Vec::len).unwrap_or(0);
    Some(format!(
        "\nVerification evidence: decision={decision}; state={state}; final_answer_allowed={final_answer_allowed}; reason={reason}; commands={command_count}; unverified_mutations={unverified_mutations}; stale_requirements={stale_requirements}.\n"
    ))
}

/// Maps a gateway tool-approval request onto an ACP permission request.
///
/// Returns `Ok(None)` when the approval carries no proposal id: without it the
/// client's decision could not be correlated back to the gateway proposal.
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

/// Translates an ACP permission outcome into the gateway approval fields
/// `(approved, reason, decision_scope, decision_scope_ttl_ms)`.
///
/// Unknown option ids and non-selection outcomes fail closed as a once-scoped
/// denial; the TTL is never set so the gateway default applies.
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

/// Maps gateway session summaries to ACP session infos, preferring locally
/// remembered bindings for cwd/label and falling back to gateway fields.
fn map_list_sessions_response(
    response: gateway_v1::ListSessionsResponse,
    state: &BridgeState,
    default_cwd: &Path,
) -> acp::ListSessionsResponse {
    let sessions = response
        .sessions
        .into_iter()
        .map(|session| {
            let binding = state.lookup_binding_for_gateway_session(&session);
            let session_key = if session.session_key.trim().is_empty() {
                session
                    .session_id
                    .as_ref()
                    .map(|value| value.ulid.clone())
                    .unwrap_or_else(|| format!("session-{}", ulid::Ulid::new()))
            } else {
                session.session_key
            };
            let cwd = binding
                .as_ref()
                .map(|binding| binding.cwd.clone())
                .unwrap_or_else(|| default_cwd.to_path_buf());
            acp::SessionInfo::new(acp::SessionId::new(session_key), cwd).title(
                binding
                    .as_ref()
                    .and_then(|binding| binding.session_label.clone())
                    .or_else(|| non_empty(Some(session.session_label))),
            )
        })
        .collect::<Vec<_>>();
    acp::ListSessionsResponse::new(sessions)
        .next_cursor(non_empty(Some(response.next_after_session_key)))
}

/// Maps a daemon `session.list` JSON response to ACP session infos; unnamed
/// sessions fall back to their id and the bridge's default cwd.
fn map_daemon_sessions_response(response: Value, default_cwd: &Path) -> acp::ListSessionsResponse {
    let sessions = response
        .get("sessions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|session| {
            let session_key = session
                .get("session_key")
                .and_then(Value::as_str)
                .or_else(|| session.get("session_id").and_then(Value::as_str))
                .unwrap_or("session")
                .to_owned();
            let title = session
                .get("session_label")
                .and_then(Value::as_str)
                .or_else(|| session.get("title").and_then(Value::as_str))
                .map(ToOwned::to_owned);
            acp::SessionInfo::new(acp::SessionId::new(session_key), default_cwd.to_path_buf())
                .title(title)
        })
        .collect::<Vec<_>>();
    acp::ListSessionsResponse::new(sessions).next_cursor(
        response.get("next_after_session_key").and_then(Value::as_str).map(ToOwned::to_owned),
    )
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
        origin_kind: String::new(),
        origin_run_id: None,
        parameter_delta_json: Vec::new(),
        queued_input_id: None,
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
mod tests;
