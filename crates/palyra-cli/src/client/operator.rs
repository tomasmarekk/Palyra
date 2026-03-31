use anyhow::{anyhow, Context, Result};
use palyra_control_plane::{
    ApprovalDecisionEnvelope, ApprovalDecisionRequest, SessionCatalogListEnvelope,
};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    client::{control_plane, message, runtime::GatewayRuntimeClient},
    commands::models,
    *,
};

#[derive(Debug, Clone)]
pub(crate) struct OperatorRuntime {
    connection: AgentConnection,
}

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

pub(crate) struct ManagedRunStream {
    run_id: String,
    event_rx: mpsc::UnboundedReceiver<ManagedRunStreamEvent>,
    control_tx: mpsc::UnboundedSender<RunStreamControl>,
}

impl ManagedRunStream {
    pub(crate) fn run_id(&self) -> &str {
        self.run_id.as_str()
    }

    pub(crate) async fn next_event(&mut self) -> Result<Option<common_v1::RunStreamEvent>> {
        match self.event_rx.recv().await {
            Some(ManagedRunStreamEvent::Event(event)) => Ok(Some(*event)),
            Some(ManagedRunStreamEvent::Finished) | None => Ok(None),
            Some(ManagedRunStreamEvent::Failed(error)) => Err(anyhow!("{error}")),
        }
    }

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
    pub(crate) fn new(connection: AgentConnection) -> Self {
        Self { connection }
    }

    pub(crate) fn connection(&self) -> &AgentConnection {
        &self.connection
    }

    async fn connect_gateway(&self) -> Result<GatewayRuntimeClient> {
        GatewayRuntimeClient::connect(self.connection.clone()).await
    }

    pub(crate) async fn list_agents(
        &self,
        after_agent_id: Option<String>,
        limit: Option<u32>,
    ) -> Result<gateway_v1::ListAgentsResponse> {
        let mut client = self.connect_gateway().await?;
        client.list_agents(after_agent_id, limit).await
    }

    pub(crate) async fn resolve_agent_for_context(
        &self,
        input: AgentContextResolveInput,
    ) -> Result<gateway_v1::ResolveAgentForContextResponse> {
        let mut client = self.connect_gateway().await?;
        client.resolve_agent_for_context(input).await
    }

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

    pub(crate) async fn resolve_session(
        &self,
        input: SessionResolveInput,
    ) -> Result<gateway_v1::ResolveSessionResponse> {
        let mut client = self.connect_gateway().await?;
        client.resolve_session(input).await
    }

    pub(crate) async fn abort_run(
        &self,
        run_id: String,
        reason: Option<String>,
    ) -> Result<gateway_v1::AbortRunResponse> {
        let mut client = self.connect_gateway().await?;
        client.abort_run(run_id, reason).await
    }

    pub(crate) async fn cleanup_session(
        &self,
        input: SessionCleanupInput,
    ) -> Result<gateway_v1::CleanupSessionResponse> {
        let mut client = self.connect_gateway().await?;
        client.cleanup_session(input).await
    }

    pub(crate) async fn start_run_stream(
        &self,
        request: AgentRunInput,
    ) -> Result<ManagedRunStream> {
        let mut client = self.connect_gateway().await?;
        let resolved = prepare_agent_run_input(&mut client, request).await?;
        let session_id = session_summary_reference(&resolved.session)?;
        let run_id = resolved.run_id.clone();
        let mut stream =
            client.open_run_stream(build_resolved_run_stream_request(&resolved)?).await?;
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let background_session_id = session_id.ulid.clone();
        let background_run_id = run_id.clone();
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

    pub(crate) async fn decide_approval(
        &self,
        approval_id: String,
        approved: bool,
        decision_scope: String,
        decision_scope_ttl_ms: Option<i64>,
        reason: Option<String>,
    ) -> Result<ApprovalDecisionEnvelope> {
        let context = control_plane::connect_admin_console(app::ConnectionOverrides {
            grpc_url: Some(self.connection.grpc_url.clone()),
            daemon_url: None,
            token: self.connection.token.clone(),
            principal: Some(self.connection.principal.clone()),
            device_id: Some(self.connection.device_id.clone()),
            channel: Some(self.connection.channel.clone()),
        })
        .await?;
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

    pub(crate) async fn list_session_catalog(
        &self,
        query: Vec<(&str, Option<String>)>,
    ) -> Result<SessionCatalogListEnvelope> {
        let context = control_plane::connect_admin_console(app::ConnectionOverrides {
            grpc_url: Some(self.connection.grpc_url.clone()),
            daemon_url: None,
            token: self.connection.token.clone(),
            principal: Some(self.connection.principal.clone()),
            device_id: Some(self.connection.device_id.clone()),
            channel: Some(self.connection.channel.clone()),
        })
        .await?;
        context.client.list_session_catalog(query).await.context("failed to list session catalog")
    }

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

    pub(crate) async fn send_message(
        &self,
        options: message::MessageDispatchOptions,
    ) -> Result<Value> {
        tokio::task::spawn_blocking(move || message::send_message(options))
            .await
            .context("message dispatch worker failed")?
    }

    pub(crate) fn list_models(&self, path: Option<String>) -> Result<models::ModelsListPayload> {
        models::build_models_list(path)
    }

    pub(crate) fn set_text_model(
        &self,
        path: Option<String>,
        backups: usize,
        model: String,
    ) -> Result<models::ModelsMutationPayload> {
        models::mutate_model_defaults(path, backups, "text", model, None)
    }
}
