use anyhow::{Context, Result};
use futures::stream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Request;

use crate::*;

pub(crate) struct GatewayRuntimeClient {
    client: gateway_v1::gateway_service_client::GatewayServiceClient<tonic::transport::Channel>,
    connection: AgentConnection,
}

pub(crate) struct GatewayRunStream {
    event_stream: tonic::Streaming<common_v1::RunStreamEvent>,
    request_sender: mpsc::Sender<RunStreamRequestEnvelope>,
}

enum RunStreamRequestEnvelope {
    Request(Box<common_v1::RunStreamRequest>),
    Close,
}

impl GatewayRunStream {
    pub(crate) async fn next_event(&mut self) -> Result<Option<common_v1::RunStreamEvent>> {
        match self.event_stream.next().await {
            Some(event) => event.context("failed to read RunStream event").map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn send_request(&self, request: common_v1::RunStreamRequest) -> Result<()> {
        self.request_sender
            .send(RunStreamRequestEnvelope::Request(Box::new(request)))
            .await
            .context("failed to queue RunStream request message")
    }

    pub(crate) async fn close_request_stream(&self) -> Result<()> {
        self.request_sender
            .send(RunStreamRequestEnvelope::Close)
            .await
            .context("failed to close RunStream request stream")
    }

    pub(crate) async fn send_tool_approval_response(
        &self,
        session_id: &str,
        run_id: &str,
        response: common_v1::ToolApprovalResponse,
    ) -> Result<()> {
        self.send_request(common_v1::RunStreamRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
            run_id: Some(common_v1::CanonicalId { ulid: run_id.to_owned() }),
            input: None,
            allow_sensitive_tools: false,
            session_key: String::new(),
            session_label: String::new(),
            reset_session: false,
            require_existing: true,
            tool_approval_response: Some(response),
        })
        .await
    }
}

impl GatewayRuntimeClient {
    pub(crate) async fn connect(connection: AgentConnection) -> Result<Self> {
        let client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(
            connection.grpc_url.clone(),
        )
        .await
        .with_context(|| {
            format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url)
        })?;
        Ok(Self { client, connection })
    }

    fn request<T>(&self, payload: T) -> Result<Request<T>> {
        let mut request = Request::new(payload);
        inject_run_stream_metadata(request.metadata_mut(), &self.connection)?;
        Ok(request)
    }

    pub(crate) async fn list_agents(
        &mut self,
        after_agent_id: Option<String>,
        limit: Option<u32>,
    ) -> Result<gateway_v1::ListAgentsResponse> {
        let request = self.request(gateway_v1::ListAgentsRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            limit: limit.unwrap_or(100),
            after_agent_id: after_agent_id.unwrap_or_default(),
        })?;
        self.client
            .list_agents(request)
            .await
            .context("failed to call ListAgents")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn get_agent(
        &mut self,
        agent_id: String,
    ) -> Result<gateway_v1::GetAgentResponse> {
        let request =
            self.request(gateway_v1::GetAgentRequest { v: CANONICAL_PROTOCOL_MAJOR, agent_id })?;
        self.client
            .get_agent(request)
            .await
            .context("failed to call GetAgent")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn create_agent(
        &mut self,
        request: gateway_v1::CreateAgentRequest,
    ) -> Result<gateway_v1::CreateAgentResponse> {
        let request = self.request(request)?;
        self.client
            .create_agent(request)
            .await
            .context("failed to call CreateAgent")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn delete_agent(
        &mut self,
        agent_id: String,
    ) -> Result<gateway_v1::DeleteAgentResponse> {
        let request =
            self.request(gateway_v1::DeleteAgentRequest { v: CANONICAL_PROTOCOL_MAJOR, agent_id })?;
        self.client
            .delete_agent(request)
            .await
            .context("failed to call DeleteAgent")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn set_default_agent(
        &mut self,
        agent_id: String,
    ) -> Result<gateway_v1::SetDefaultAgentResponse> {
        let request = self.request(gateway_v1::SetDefaultAgentRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            agent_id,
        })?;
        self.client
            .set_default_agent(request)
            .await
            .context("failed to call SetDefaultAgent")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn list_agent_bindings(
        &mut self,
        input: AgentBindingsQueryInput,
    ) -> Result<gateway_v1::ListAgentBindingsResponse> {
        let request = self.request(gateway_v1::ListAgentBindingsRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            agent_id: input.agent_id,
            principal: input.principal,
            channel: input.channel,
            session_id: input.session_id,
            limit: input.limit,
        })?;
        self.client
            .list_agent_bindings(request)
            .await
            .context("failed to call ListAgentBindings")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn bind_agent_for_context(
        &mut self,
        request: gateway_v1::BindAgentForContextRequest,
    ) -> Result<gateway_v1::BindAgentForContextResponse> {
        let request = self.request(request)?;
        self.client
            .bind_agent_for_context(request)
            .await
            .context("failed to call BindAgentForContext")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn unbind_agent_for_context(
        &mut self,
        request: gateway_v1::UnbindAgentForContextRequest,
    ) -> Result<gateway_v1::UnbindAgentForContextResponse> {
        let request = self.request(request)?;
        self.client
            .unbind_agent_for_context(request)
            .await
            .context("failed to call UnbindAgentForContext")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn resolve_agent_for_context(
        &mut self,
        input: AgentContextResolveInput,
    ) -> Result<gateway_v1::ResolveAgentForContextResponse> {
        let request = self.request(gateway_v1::ResolveAgentForContextRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            principal: input.principal,
            channel: input.channel,
            session_id: input.session_id,
            preferred_agent_id: input.preferred_agent_id,
            persist_session_binding: input.persist_session_binding,
        })?;
        self.client
            .resolve_agent_for_context(request)
            .await
            .context("failed to call ResolveAgentForContext")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn list_sessions(
        &mut self,
        after_session_key: Option<String>,
        include_archived: bool,
        limit: Option<u32>,
    ) -> Result<gateway_v1::ListSessionsResponse> {
        let request = self.request(gateway_v1::ListSessionsRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            after_session_key: after_session_key.unwrap_or_default(),
            limit: limit.unwrap_or(100),
            include_archived,
        })?;
        self.client
            .list_sessions(request)
            .await
            .context("failed to call ListSessions")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn resolve_session(
        &mut self,
        input: SessionResolveInput,
    ) -> Result<gateway_v1::ResolveSessionResponse> {
        let request = self.request(gateway_v1::ResolveSessionRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: input.session_id,
            session_key: input.session_key,
            session_label: input.session_label,
            require_existing: input.require_existing,
            reset_session: input.reset_session,
        })?;
        self.client
            .resolve_session(request)
            .await
            .context("failed to call ResolveSession")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn abort_run(
        &mut self,
        run_id: String,
        reason: Option<String>,
    ) -> Result<gateway_v1::AbortRunResponse> {
        let request = self.request(gateway_v1::AbortRunRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            run_id: Some(common_v1::CanonicalId { ulid: run_id }),
            reason: reason.unwrap_or_default(),
        })?;
        self.client
            .abort_run(request)
            .await
            .context("failed to call AbortRun")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn cleanup_session(
        &mut self,
        input: SessionCleanupInput,
    ) -> Result<gateway_v1::CleanupSessionResponse> {
        let request = self.request(gateway_v1::CleanupSessionRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: input.session_id,
            session_key: input.session_key,
        })?;
        self.client
            .cleanup_session(request)
            .await
            .context("failed to call CleanupSession")
            .map(|response| response.into_inner())
    }

    pub(crate) async fn open_run_stream(
        &mut self,
        initial_request: common_v1::RunStreamRequest,
    ) -> Result<GatewayRunStream> {
        let (request_sender, request_receiver) = mpsc::channel(16);
        request_sender
            .send(RunStreamRequestEnvelope::Request(Box::new(initial_request)))
            .await
            .context("failed to queue initial RunStream request message")?;
        let request_stream = stream::unfold(request_receiver, |mut receiver| async move {
            match receiver.recv().await {
                Some(RunStreamRequestEnvelope::Request(request)) => Some((*request, receiver)),
                Some(RunStreamRequestEnvelope::Close) | None => None,
            }
        });
        let mut request = Request::new(request_stream);
        inject_run_stream_metadata(request.metadata_mut(), &self.connection)?;
        let event_stream =
            self.client.run_stream(request).await.context("failed to call RunStream")?.into_inner();
        Ok(GatewayRunStream { event_stream, request_sender })
    }
}
