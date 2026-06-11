//! Operator-facing runtime facade used by interactive commands and the TUI.
//!
//! Bundles one [`AgentConnection`] identity and fans out to the gateway gRPC
//! API, the control-plane admin console, and the blocking message helpers.
//! [`ManagedRunStream`] pumps a gateway run stream from a background task so
//! UI loops only deal with a single event channel.

use anyhow::{anyhow, Context, Result};
use palyra_control_plane::{
    ApprovalDecisionEnvelope, ApprovalDecisionRequest, SessionCatalogDetailEnvelope,
    SessionCatalogListEnvelope, SessionCatalogMutationEnvelope, SessionQuickControlsUpdateRequest,
};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    client::{control_plane, message, runtime::GatewayRuntimeClient},
    commands::models,
    *,
};

/// Operator command surface bound to one resolved gateway connection identity.
#[derive(Debug, Clone)]
pub(crate) struct OperatorRuntime {
    connection: AgentConnection,
}

// Control messages flowing from the UI into the background stream task.
#[derive(Debug)]
enum RunStreamControl {
    Approval(common_v1::ToolApprovalResponse),
}

#[derive(Debug)]
enum ManagedRunStreamEvent {
    Event(Box<common_v1::RunStreamEvent>),
    Finished,
    Failed(String),
}

/// Consumer handle for a run stream pumped by a background task: events are
/// received on one channel, approval decisions are sent back on another.
pub(crate) struct ManagedRunStream {
    run_id: String,
    event_rx: mpsc::UnboundedReceiver<ManagedRunStreamEvent>,
    control_tx: mpsc::UnboundedSender<RunStreamControl>,
}

impl ManagedRunStream {
    /// Returns the run id assigned to this stream.
    pub(crate) fn run_id(&self) -> &str {
        self.run_id.as_str()
    }

    /// Awaits the next run event; `Ok(None)` means the run finished or the
    /// background task ended.
    ///
    /// # Errors
    /// Returns an error when the background task reported a stream failure.
    pub(crate) async fn next_event(&mut self) -> Result<Option<common_v1::RunStreamEvent>> {
        match self.event_rx.recv().await {
            Some(ManagedRunStreamEvent::Event(event)) => Ok(Some(*event)),
            Some(ManagedRunStreamEvent::Finished) | None => Ok(None),
            Some(ManagedRunStreamEvent::Failed(error)) => Err(anyhow!("{error}")),
        }
    }

    /// Queues a tool approval decision for delivery on the run stream.
    ///
    /// # Errors
    /// Returns an error when the background stream task has already exited.
    pub(crate) fn send_tool_approval_decision(
        &self,
        approval_request: &common_v1::ToolApprovalRequest,
        approved: bool,
        reason: String,
        decision_scope: i32,
        decision_scope_ttl_ms: i64,
    ) -> Result<()> {
        self.control_tx
            .send(RunStreamControl::Approval(common_v1::ToolApprovalResponse {
                proposal_id: approval_request.proposal_id.clone(),
                approved,
                reason,
                approval_id: approval_request.approval_id.clone(),
                decision_scope,
                decision_scope_ttl_ms,
            }))
            .context("failed to queue tool approval response")
    }
}

impl OperatorRuntime {
    /// Creates a runtime facade for the given connection identity.
    pub(crate) fn new(connection: AgentConnection) -> Self {
        Self { connection }
    }

    /// Returns the connection identity this runtime operates as.
    pub(crate) fn connection(&self) -> &AgentConnection {
        &self.connection
    }

    // Each call dials a fresh gateway client; connections are cheap enough for
    // operator-paced commands and avoid holding a stale channel between calls.
    async fn connect_gateway(&self) -> Result<GatewayRuntimeClient> {
        GatewayRuntimeClient::connect(self.connection.clone()).await
    }

    // Maps this runtime's gRPC identity onto control-plane connection overrides
    // so console calls run as the same operator.
    fn admin_console_overrides(&self) -> app::ConnectionOverrides {
        app::ConnectionOverrides {
            grpc_url: Some(self.connection.grpc_url.clone()),
            daemon_url: None,
            token: self.connection.token.clone(),
            principal: Some(self.connection.principal.clone()),
            device_id: Some(self.connection.device_id.clone()),
            channel: Some(self.connection.channel.clone()),
        }
    }

    /// Lists agents through the gateway.
    ///
    /// # Errors
    /// Returns an error when the gateway connection or the RPC fails.
    pub(crate) async fn list_agents(
        &self,
        after_agent_id: Option<String>,
        limit: Option<u32>,
    ) -> Result<gateway_v1::ListAgentsResponse> {
        let mut client = self.connect_gateway().await?;
        client.list_agents(after_agent_id, limit).await
    }

    /// Resolves the agent for a routing context through the gateway.
    ///
    /// # Errors
    /// Returns an error when the gateway connection or the RPC fails.
    pub(crate) async fn resolve_agent_for_context(
        &self,
        input: AgentContextResolveInput,
    ) -> Result<gateway_v1::ResolveAgentForContextResponse> {
        let mut client = self.connect_gateway().await?;
        client.resolve_agent_for_context(input).await
    }

    /// Lists sessions through the gateway.
    ///
    /// # Errors
    /// Returns an error when the gateway connection or the RPC fails.
    pub(crate) async fn list_sessions(
        &self,
        after_session_key: Option<String>,
        include_archived: bool,
        limit: Option<u32>,
        q: Option<String>,
    ) -> Result<gateway_v1::ListSessionsResponse> {
        let mut client = self.connect_gateway().await?;
        client.list_sessions(after_session_key, include_archived, limit, q).await
    }

    /// Resolves a session selector through the gateway.
    ///
    /// # Errors
    /// Returns an error when the gateway connection or the RPC fails.
    pub(crate) async fn resolve_session(
        &self,
        input: SessionResolveInput,
    ) -> Result<gateway_v1::ResolveSessionResponse> {
        let mut client = self.connect_gateway().await?;
        client.resolve_session(input).await
    }

    /// Aborts a run through the gateway.
    ///
    /// # Errors
    /// Returns an error when the gateway connection or the RPC fails.
    pub(crate) async fn abort_run(
        &self,
        run_id: String,
        reason: Option<String>,
    ) -> Result<gateway_v1::AbortRunResponse> {
        let mut client = self.connect_gateway().await?;
        client.abort_run(run_id, reason).await
    }

    /// Cleans up a session through the gateway.
    ///
    /// # Errors
    /// Returns an error when the gateway connection or the RPC fails.
    pub(crate) async fn cleanup_session(
        &self,
        input: SessionCleanupInput,
    ) -> Result<gateway_v1::CleanupSessionResponse> {
        let mut client = self.connect_gateway().await?;
        client.cleanup_session(input).await
    }

    /// Starts an agent run and spawns the background task that pumps stream
    /// events and forwards approval decisions.
    ///
    /// # Errors
    /// Returns an error when run preparation or opening the stream fails;
    /// failures after that point surface through [`ManagedRunStream::next_event`].
    pub(crate) async fn start_run_stream(
        &self,
        request: AgentRunInput,
    ) -> Result<ManagedRunStream> {
        let mut client = self.connect_gateway().await?;
        let resolved = prepare_agent_run_input(&mut client, request).await?;
        let session_id = session_summary_reference(&resolved.session)?;
        let run_id = resolved.request.run_id.clone();
        let mut stream =
            client.open_run_stream(build_resolved_run_stream_request(&resolved)?).await?;
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let background_session_id = session_id.ulid.clone();
        let background_run_id = run_id.clone();
        // Pump task: owns the gRPC stream and exits when the run reaches a
        // terminal status, the stream ends or fails, or the consumer drops its
        // receiver. Both select! branches (mpsc recv and stream next) are
        // cancel-safe, so no progress is lost between loop iterations.
        tokio::spawn(async move {
            let mut request_stream_closed = false;
            loop {
                tokio::select! {
                    maybe_control = control_rx.recv() => {
                        let Some(control) = maybe_control else {
                            break;
                        };
                        match control {
                            RunStreamControl::Approval(response) => {
                                if let Err(error) = stream
                                    .send_tool_approval_response(
                                        background_session_id.as_str(),
                                        background_run_id.as_str(),
                                        response,
                                    )
                                    .await
                                {
                                    let _ = event_tx.send(ManagedRunStreamEvent::Failed(error.to_string()));
                                    break;
                                }
                            }
                        }
                    }
                    next_event = stream.next_event() => {
                        match next_event {
                            Ok(Some(event)) => {
                                let reached_terminal_status = matches!(
                                    event.body.as_ref(),
                                    Some(common_v1::run_stream_event::Body::Status(status))
                                        if is_terminal_stream_status(status.kind)
                                );
                                // Half-close the request side once the run hit a
                                // terminal status, before forwarding the event,
                                // so the server can complete the stream cleanly.
                                if !request_stream_closed
                                    && run_stream_can_close_request_side(&event)
                                {
                                    if let Err(error) = stream.close_request_stream().await {
                                        let _ = event_tx.send(ManagedRunStreamEvent::Failed(error.to_string()));
                                        break;
                                    }
                                    request_stream_closed = true;
                                }
                                if event_tx
                                    .send(ManagedRunStreamEvent::Event(Box::new(event)))
                                    .is_err()
                                {
                                    break;
                                }
                                if reached_terminal_status {
                                    let _ = event_tx.send(ManagedRunStreamEvent::Finished);
                                    break;
                                }
                            }
                            Ok(None) => {
                                let _ = event_tx.send(ManagedRunStreamEvent::Finished);
                                break;
                            }
                            Err(error) => {
                                let _ = event_tx.send(ManagedRunStreamEvent::Failed(error.to_string()));
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(ManagedRunStream { run_id, event_rx, control_tx })
    }

    /// Records an approval decision through the control-plane console.
    ///
    /// # Errors
    /// Returns an error when the console session or the decision call fails.
    pub(crate) async fn decide_approval(
        &self,
        approval_id: String,
        approved: bool,
        decision_scope: String,
        decision_scope_ttl_ms: Option<i64>,
        reason: Option<String>,
    ) -> Result<ApprovalDecisionEnvelope> {
        let context = control_plane::connect_admin_console(self.admin_console_overrides()).await?;
        context
            .client
            .decide_approval(
                approval_id.as_str(),
                &ApprovalDecisionRequest {
                    approved,
                    reason,
                    decision_scope: Some(decision_scope),
                    decision_scope_ttl_ms,
                },
            )
            .await
            .with_context(|| format!("failed to resolve approval {approval_id}"))
    }

    /// Lists the session catalog through the control-plane console.
    ///
    /// # Errors
    /// Returns an error when the console session or the list call fails.
    pub(crate) async fn list_session_catalog(
        &self,
        query: Vec<(&str, Option<String>)>,
    ) -> Result<SessionCatalogListEnvelope> {
        let context = control_plane::connect_admin_console(self.admin_console_overrides()).await?;
        context.client.list_session_catalog(query).await.context("failed to list session catalog")
    }

    /// Loads one session catalog entry through the control-plane console.
    ///
    /// # Errors
    /// Returns an error when the console session or the lookup fails.
    pub(crate) async fn get_session_catalog_entry(
        &self,
        session_id: &str,
    ) -> Result<SessionCatalogDetailEnvelope> {
        let context = control_plane::connect_admin_console(self.admin_console_overrides()).await?;
        context
            .client
            .get_session_catalog_entry(session_id)
            .await
            .with_context(|| format!("failed to load session catalog entry {session_id}"))
    }

    /// Updates session quick controls through the control-plane console.
    ///
    /// # Errors
    /// Returns an error when the console session or the update call fails.
    pub(crate) async fn update_session_quick_controls(
        &self,
        session_id: &str,
        request: &SessionQuickControlsUpdateRequest,
    ) -> Result<SessionCatalogMutationEnvelope> {
        let context = control_plane::connect_admin_console(self.admin_console_overrides()).await?;
        context
            .client
            .update_session_quick_controls(session_id, request)
            .await
            .with_context(|| format!("failed to update quick controls for session {session_id}"))
    }

    // The message::* helpers use blocking reqwest, so every wrapper below runs
    // them on the blocking pool to keep the async runtime workers unblocked.

    /// Loads connector message capabilities on the blocking pool.
    ///
    /// # Errors
    /// Returns an error when the worker panics or the capability lookup fails.
    pub(crate) async fn message_capabilities(
        &self,
        connector_id: String,
        url: Option<String>,
        token: Option<String>,
        principal: String,
        device_id: String,
        channel: Option<String>,
    ) -> Result<message::MessageCapabilities> {
        tokio::task::spawn_blocking(move || {
            message::load_capabilities(
                connector_id.as_str(),
                url,
                token,
                principal,
                device_id,
                channel,
            )
        })
        .await
        .context("message capabilities worker failed")?
    }

    /// Sends a connector message on the blocking pool.
    ///
    /// # Errors
    /// Returns an error when the worker panics or the dispatch fails.
    pub(crate) async fn send_message(
        &self,
        options: message::MessageDispatchOptions,
    ) -> Result<Value> {
        tokio::task::spawn_blocking(move || message::send_message(options))
            .await
            .context("message dispatch worker failed")?
    }

    /// Reads connector messages on the blocking pool.
    ///
    /// # Errors
    /// Returns an error when the worker panics or the read fails.
    pub(crate) async fn read_messages(
        &self,
        options: message::MessageReadOptions,
    ) -> Result<Value> {
        tokio::task::spawn_blocking(move || message::read_messages(options))
            .await
            .context("message read worker failed")?
    }

    /// Searches connector messages on the blocking pool.
    ///
    /// # Errors
    /// Returns an error when the worker panics or the search fails.
    pub(crate) async fn search_messages(
        &self,
        options: message::MessageSearchOptions,
    ) -> Result<Value> {
        tokio::task::spawn_blocking(move || message::search_messages(options))
            .await
            .context("message search worker failed")?
    }

    /// Edits a connector message on the blocking pool.
    ///
    /// # Errors
    /// Returns an error when the worker panics or the edit fails.
    pub(crate) async fn edit_message(&self, options: message::MessageEditOptions) -> Result<Value> {
        tokio::task::spawn_blocking(move || message::edit_message(options))
            .await
            .context("message edit worker failed")?
    }

    /// Deletes a connector message on the blocking pool.
    ///
    /// # Errors
    /// Returns an error when the worker panics or the delete fails.
    pub(crate) async fn delete_message(
        &self,
        options: message::MessageDeleteOptions,
    ) -> Result<Value> {
        tokio::task::spawn_blocking(move || message::delete_message(options))
            .await
            .context("message delete worker failed")?
    }

    /// Adds a message reaction on the blocking pool.
    ///
    /// # Errors
    /// Returns an error when the worker panics or the reaction call fails.
    pub(crate) async fn add_reaction(
        &self,
        options: message::MessageReactionOptions,
    ) -> Result<Value> {
        tokio::task::spawn_blocking(move || message::add_reaction(options))
            .await
            .context("message reaction-add worker failed")?
    }

    /// Removes a message reaction on the blocking pool.
    ///
    /// # Errors
    /// Returns an error when the worker panics or the reaction call fails.
    pub(crate) async fn remove_reaction(
        &self,
        options: message::MessageReactionOptions,
    ) -> Result<Value> {
        tokio::task::spawn_blocking(move || message::remove_reaction(options))
            .await
            .context("message reaction-remove worker failed")?
    }

    /// Builds the models list payload from local configuration.
    ///
    /// # Errors
    /// Returns an error when the models configuration cannot be loaded.
    pub(crate) fn list_models(&self, path: Option<String>) -> Result<models::ModelsListPayload> {
        models::build_models_list(path)
    }
}
