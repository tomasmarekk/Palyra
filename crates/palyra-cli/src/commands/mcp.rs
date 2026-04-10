use std::io::{self, BufRead, BufReader, Write};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Map, Value};
use tonic::Request;

use crate::cli::{AcpConnectionArgs, AcpSessionDefaultsArgs, McpCommand, McpSubcommand};
use crate::*;

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const JSONRPC_VERSION: &str = "2.0";

const TOOL_SESSIONS_LIST: &str = "sessions_list";
const TOOL_SESSION_TRANSCRIPT_READ: &str = "session_transcript_read";
const TOOL_SESSION_EXPORT: &str = "session_export";
const TOOL_MEMORY_SEARCH: &str = "memory_search";
const TOOL_APPROVALS_LIST: &str = "approvals_list";
const TOOL_SESSION_CREATE: &str = "session_create";
const TOOL_SESSION_PROMPT: &str = "session_prompt";
const TOOL_APPROVAL_DECIDE: &str = "approval_decide";

pub(crate) fn run_mcp(command: McpCommand) -> Result<()> {
    match command.subcommand {
        McpSubcommand::Serve { connection, session_defaults, read_only, allow_sensitive_tools } => {
            run_mcp_serve(connection, session_defaults, read_only, allow_sensitive_tools)
        }
    }
}

fn run_mcp_serve(
    connection: AcpConnectionArgs,
    session_defaults: AcpSessionDefaultsArgs,
    read_only: bool,
    allow_sensitive_tools: bool,
) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for MCP command"))?;
    let overrides = app::ConnectionOverrides {
        grpc_url: connection.grpc_url,
        token: connection.token,
        principal: connection.principal,
        device_id: connection.device_id,
        channel: connection.channel,
        daemon_url: None,
    };
    let user_connection =
        root_context.resolve_grpc_connection(overrides.clone(), app::ConnectionDefaults::USER)?;
    let admin_connection =
        root_context.resolve_grpc_connection(overrides.clone(), app::ConnectionDefaults::ADMIN)?;
    let mut backend = LiveMcpBackend {
        runtime: build_runtime()?,
        user_connection,
        admin_connection,
        control_plane_overrides: overrides,
        session_defaults: acp_bridge::AcpSessionDefaults {
            session_key: session_defaults.session_key,
            session_label: session_defaults.session_label,
            require_existing: session_defaults.require_existing,
            reset_session: session_defaults.reset_session,
        },
        read_only,
        allow_sensitive_tools,
    };
    let mut reader = BufReader::new(io::stdin().lock());
    let mut writer = io::stdout().lock();
    while let Some(request) = read_mcp_message(&mut reader)? {
        if let Some(response) = handle_mcp_request(&mut backend, request)? {
            write_mcp_message(&mut writer, &response)?;
        }
    }
    Ok(())
}

trait McpBackend {
    fn read_only(&self) -> bool;

    fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<Value>;
}

struct LiveMcpBackend {
    runtime: tokio::runtime::Runtime,
    user_connection: AgentConnection,
    admin_connection: AgentConnection,
    control_plane_overrides: app::ConnectionOverrides,
    session_defaults: acp_bridge::AcpSessionDefaults,
    read_only: bool,
    allow_sensitive_tools: bool,
}

impl McpBackend for LiveMcpBackend {
    fn read_only(&self) -> bool {
        self.read_only
    }

    fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<Value> {
        match name {
            TOOL_SESSIONS_LIST => self.sessions_list(arguments),
            TOOL_SESSION_TRANSCRIPT_READ => self.session_transcript_read(arguments),
            TOOL_SESSION_EXPORT => self.session_export(arguments),
            TOOL_MEMORY_SEARCH => self.memory_search(arguments),
            TOOL_APPROVALS_LIST => self.approvals_list(arguments),
            TOOL_SESSION_CREATE => self.session_create(arguments),
            TOOL_SESSION_PROMPT => self.session_prompt(arguments),
            TOOL_APPROVAL_DECIDE => self.approval_decide(arguments),
            other => anyhow::bail!("unknown MCP tool `{other}`"),
        }
    }
}

impl LiveMcpBackend {
    fn operator_runtime(&self) -> client::operator::OperatorRuntime {
        client::operator::OperatorRuntime::new(self.user_connection.clone())
    }

    fn sessions_list(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSIONS_LIST)?;
        let after_session_key = opt_string_arg(args, "after_session_key")?;
        let include_archived = opt_bool_arg(args, "include_archived")?.unwrap_or(false);
        let limit = opt_u32_arg(args, "limit")?;
        let query = opt_string_arg(args, "query")?;
        let response = self.runtime.block_on(async {
            self.operator_runtime()
                .list_sessions(after_session_key, include_archived, limit, query)
                .await
        })?;
        Ok(json!({
            "sessions": response
                .sessions
                .iter()
                .map(session_summary_to_json)
                .collect::<Vec<Value>>(),
            "next_after_session_key": normalize_optional_text(response.next_after_session_key.as_str()),
            "include_archived": include_archived,
        }))
    }

    fn session_transcript_read(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSION_TRANSCRIPT_READ)?;
        let resolved = self.resolve_session_from_args(args)?;
        let session_id = resolved
            .session
            .as_ref()
            .and_then(|value| value.session_id.as_ref())
            .map(|value| value.ulid.clone())
            .context("resolved session is missing session_id")?;
        let path = format!(
            "console/v1/chat/sessions/{}/transcript",
            percent_encode_component(session_id.as_str())
        );
        self.runtime.block_on(async {
            let context =
                client::control_plane::connect_admin_console(self.control_plane_overrides.clone())
                    .await?;
            context.client.get_json_value(path.as_str()).await.map_err(Into::into)
        })
    }

    fn session_export(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSION_EXPORT)?;
        let format = opt_string_arg(args, "format")?.unwrap_or_else(|| "json".to_owned());
        if !format.eq_ignore_ascii_case("json") && !format.eq_ignore_ascii_case("markdown") {
            anyhow::bail!("session_export format must be one of: json, markdown");
        }
        let resolved = self.resolve_session_from_args(args)?;
        let session_id = resolved
            .session
            .as_ref()
            .and_then(|value| value.session_id.as_ref())
            .map(|value| value.ulid.clone())
            .context("resolved session is missing session_id")?;
        let path = format!(
            "console/v1/chat/sessions/{}/export?format={}",
            percent_encode_component(session_id.as_str()),
            percent_encode_component(format.as_str())
        );
        self.runtime.block_on(async {
            let context =
                client::control_plane::connect_admin_console(self.control_plane_overrides.clone())
                    .await?;
            context.client.get_json_value(path.as_str()).await.map_err(Into::into)
        })
    }

    fn memory_search(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_MEMORY_SEARCH)?;
        let query = required_string_arg(args, "query")?;
        if query.trim().is_empty() {
            anyhow::bail!("memory_search query cannot be empty");
        }
        let scope = opt_string_arg(args, "scope")?.unwrap_or_else(|| "principal".to_owned());
        let top_k = opt_u32_arg(args, "top_k")?.unwrap_or(5);
        let min_score = opt_f64_arg(args, "min_score")?.unwrap_or(0.0);
        if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
            anyhow::bail!("memory_search.min_score must be in range 0.0..=1.0");
        }
        let include_score_breakdown =
            opt_bool_arg(args, "include_score_breakdown")?.unwrap_or(false);
        let tags = opt_string_vec_arg(args, "tags")?;
        let sources = opt_string_vec_arg(args, "sources")?;
        let channel_arg = opt_string_arg(args, "channel")?;
        let session_arg = opt_string_arg(args, "session_id")?;
        let (channel, session_id) = resolve_memory_scope_for_mcp(
            scope.as_str(),
            channel_arg,
            session_arg,
            &self.user_connection,
        )?;
        let source_values = sources
            .into_iter()
            .map(|value| parse_memory_source_arg(value.as_str()).map(memory_source_to_proto))
            .collect::<Result<Vec<i32>>>()?;
        let mut request = Request::new(memory_v1::SearchMemoryRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            query,
            channel: channel.unwrap_or_default(),
            session_id: session_id.map(|ulid| common_v1::CanonicalId { ulid }),
            top_k,
            min_score,
            tags,
            sources: source_values,
            include_score_breakdown,
        });
        inject_run_stream_metadata(request.metadata_mut(), &self.user_connection)?;
        let grpc_url = self.user_connection.grpc_url.clone();
        let response = self.runtime.block_on(async move {
            let mut client =
                memory_v1::memory_service_client::MemoryServiceClient::connect(grpc_url.clone())
                    .await
                    .with_context(|| {
                        format!("failed to connect gateway gRPC endpoint {grpc_url}")
                    })?;
            client
                .search_memory(request)
                .await
                .context("failed to call memory SearchMemory")
                .map(|value| value.into_inner())
        })?;
        Ok(json!({
            "hits": response.hits.iter().map(memory_search_hit_to_json).collect::<Vec<Value>>(),
        }))
    }

    fn approvals_list(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_APPROVALS_LIST)?;
        let after = opt_string_arg(args, "after_approval_id")?;
        if let Some(value) = after.as_deref() {
            validate_canonical_id(value)
                .context("approvals_list.after_approval_id must be a canonical ULID")?;
        }
        let limit = opt_u32_arg(args, "limit")?.unwrap_or(50);
        let since = opt_i64_arg(args, "since_unix_ms")?.unwrap_or_default();
        let until = opt_i64_arg(args, "until_unix_ms")?.unwrap_or_default();
        let subject = opt_string_arg(args, "subject_id")?;
        let principal = opt_string_arg(args, "principal")?;
        let decision = approval_decision_filter_arg(args, "decision")?;
        let subject_type = approval_subject_type_filter_arg(args, "subject_type")?;
        let mut request = Request::new(gateway_v1::ListApprovalsRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            after_approval_ulid: after.unwrap_or_default(),
            limit,
            since_unix_ms: since,
            until_unix_ms: until,
            subject_id: subject.unwrap_or_default(),
            principal: principal.unwrap_or_default(),
            decision,
            subject_type,
        });
        inject_run_stream_metadata(request.metadata_mut(), &self.admin_connection)?;
        let grpc_url = self.admin_connection.grpc_url.clone();
        let response = self.runtime.block_on(async move {
            let mut client = gateway_v1::approvals_service_client::ApprovalsServiceClient::connect(
                grpc_url.clone(),
            )
            .await
            .with_context(|| format!("failed to connect gateway gRPC endpoint {grpc_url}"))?;
            client
                .list_approvals(request)
                .await
                .context("failed to call approvals ListApprovals")
                .map(|value| value.into_inner())
        })?;
        Ok(json!({
            "approvals": response
                .approvals
                .iter()
                .map(approval_record_to_json)
                .collect::<Vec<Value>>(),
            "next_after_approval_id": normalize_optional_text(response.next_after_approval_ulid.as_str()),
        }))
    }

    fn session_create(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSION_CREATE)?;
        let response = self.resolve_session_with_defaults(args, false)?;
        let session =
            response.session.as_ref().context("ResolveSession returned empty session payload")?;
        Ok(json!({
            "session": session_summary_to_json(session),
            "created": response.created,
            "reset_applied": response.reset_applied,
        }))
    }

    fn session_prompt(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSION_PROMPT)?;
        let prompt = required_string_arg(args, "prompt")?;
        if prompt.trim().is_empty() {
            anyhow::bail!("session_prompt prompt cannot be empty");
        }
        let allow_sensitive_tools = self.allow_sensitive_tools
            && opt_bool_arg(args, "allow_sensitive_tools")?.unwrap_or(false);
        let resolved = self.resolve_session_with_defaults(args, false)?;
        let session = resolved
            .session
            .as_ref()
            .context("ResolveSession returned empty session payload")?
            .clone();
        let run_input = build_agent_run_input(AgentRunInputArgs {
            session_id: session.session_id.clone(),
            session_key: None,
            session_label: None,
            require_existing: true,
            reset_session: false,
            run_id: None,
            prompt,
            allow_sensitive_tools,
            origin_kind: Some("mcp_stdio".to_owned()),
            origin_run_id: None,
            parameter_delta_json: None,
        })?;
        self.runtime.block_on(async {
            let runtime = self.operator_runtime();
            let mut stream = runtime.start_run_stream(run_input).await?;
            collect_mcp_run_stream(&session, &resolved, &mut stream).await
        })
    }

    fn approval_decide(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_APPROVAL_DECIDE)?;
        let approval_id = required_string_arg(args, "approval_id")?;
        validate_canonical_id(approval_id.as_str())
            .context("approval_decide.approval_id must be a canonical ULID")?;
        let approved = required_bool_arg(args, "approved")?;
        let decision_scope =
            opt_string_arg(args, "decision_scope")?.unwrap_or_else(|| "once".to_owned());
        if !matches!(decision_scope.as_str(), "once" | "session" | "timeboxed") {
            anyhow::bail!(
                "approval_decide.decision_scope must be one of: once, session, timeboxed"
            );
        }
        let ttl_ms = opt_i64_arg(args, "decision_scope_ttl_ms")?;
        validate_mcp_approval_scope(decision_scope.as_str(), ttl_ms)?;
        let reason = opt_string_arg(args, "reason")?;
        let payload = self.runtime.block_on(async {
            self.operator_runtime()
                .decide_approval(approval_id, approved, decision_scope, ttl_ms, reason)
                .await
        })?;
        Ok(json!({
            "approval": payload.approval,
            "dm_pairing": payload.dm_pairing,
        }))
    }

    fn resolve_session_from_args(
        &mut self,
        args: &Map<String, Value>,
    ) -> Result<gateway_v1::ResolveSessionResponse> {
        self.resolve_session_with_defaults(args, true)
    }

    fn resolve_session_with_defaults(
        &mut self,
        args: &Map<String, Value>,
        require_existing_default: bool,
    ) -> Result<gateway_v1::ResolveSessionResponse> {
        let session_id = opt_string_arg(args, "session_id")?;
        let session_key = opt_string_arg(args, "session_key")?
            .or_else(|| self.session_defaults.session_key.clone())
            .unwrap_or_default();
        let session_label = opt_string_arg(args, "session_label")?
            .or_else(|| self.session_defaults.session_label.clone())
            .unwrap_or_default();
        let require_existing = opt_bool_arg(args, "require_existing")?
            .unwrap_or(require_existing_default || self.session_defaults.require_existing);
        let reset_session =
            opt_bool_arg(args, "reset_session")?.unwrap_or(self.session_defaults.reset_session);
        let request = SessionResolveInput {
            session_id: resolve_optional_canonical_id(session_id)?,
            session_key,
            session_label,
            require_existing,
            reset_session,
        };
        self.runtime.block_on(async { self.operator_runtime().resolve_session(request).await })
    }
}

async fn collect_mcp_run_stream(
    session: &gateway_v1::SessionSummary,
    resolved: &gateway_v1::ResolveSessionResponse,
    stream: &mut client::operator::ManagedRunStream,
) -> Result<Value> {
    let mut events = Vec::new();
    let mut assistant_text = String::new();
    let mut terminal_status = None::<String>;
    let mut approval_request = None::<Value>;
    while let Some(event) = stream.next_event().await? {
        events.push(mcp_run_stream_event_to_json(&event));
        match event.body.as_ref() {
            Some(common_v1::run_stream_event::Body::ModelToken(token)) => {
                if !token.token.is_empty() {
                    assistant_text.push_str(token.token.as_str());
                }
            }
            Some(common_v1::run_stream_event::Body::ToolApprovalRequest(request)) => {
                approval_request = Some(tool_approval_request_to_json(request));
                break;
            }
            Some(common_v1::run_stream_event::Body::Status(status)) => {
                terminal_status = Some(stream_status_kind_to_text(status.kind).to_owned());
                if is_terminal_stream_status(status.kind) {
                    break;
                }
            }
            _ => {}
        }
    }

    let status = if approval_request.is_some() {
        "approval_required"
    } else if terminal_status.as_deref() == Some("failed") {
        "failed"
    } else {
        "completed"
    };

    Ok(json!({
        "status": status,
        "run_id": stream.run_id(),
        "session": session_summary_to_json(session),
        "created": resolved.created,
        "reset_applied": resolved.reset_applied,
        "assistant_text": if assistant_text.is_empty() { None::<String> } else { Some(assistant_text) },
        "approval_request": approval_request,
        "events": events,
    }))
}

fn handle_mcp_request(backend: &mut dyn McpBackend, request: Value) -> Result<Option<Value>> {
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return Ok(request
            .get("id")
            .cloned()
            .map(|id| rpc_error(id, -32600, "invalid_request", "request is missing method")));
    };
    let id = request.get("id").cloned();
    match method {
        "initialize" => Ok(id.map(|request_id| {
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": request_id,
                "result": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {
                        "tools": {
                            "listChanged": false,
                        }
                    },
                    "serverInfo": {
                        "name": "palyra-cli",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "instructions": if backend.read_only() {
                        "Read-only MCP facade over Palyra sessions, transcripts, approvals, and memory."
                    } else {
                        "MCP facade over Palyra sessions, approvals, memory, and approval-aware mutations."
                    },
                }
            })
        })),
        "notifications/initialized" => Ok(None),
        "ping" => Ok(id.map(|request_id| {
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": request_id,
                "result": {}
            })
        })),
        "tools/list" => Ok(id.map(|request_id| {
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": request_id,
                "result": {
                    "tools": registered_tools(backend.read_only()),
                }
            })
        })),
        "tools/call" => {
            let Some(request_id) = id else {
                return Ok(None);
            };
            let params = request.get("params").and_then(Value::as_object);
            let Some(params) = params else {
                return Ok(Some(rpc_error(
                    request_id,
                    -32602,
                    "invalid_params",
                    "tools/call requires params object",
                )));
            };
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return Ok(Some(rpc_error(
                    request_id,
                    -32602,
                    "invalid_params",
                    "tools/call params.name must be a string",
                )));
            };
            let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            if backend.read_only() && is_mutating_tool(name) {
                return Ok(Some(json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": request_id,
                    "result": tool_error_payload(format!(
                        "tool `{name}` is unavailable because the MCP server is running in --read-only mode"
                    )),
                })));
            }
            let tool_result = match backend.call_tool(name, &arguments) {
                Ok(value) => tool_success_payload(value),
                Err(error) => tool_error_payload(error.to_string()),
            };
            Ok(Some(json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": request_id,
                "result": tool_result,
            })))
        }
        _ => Ok(id.map(|request_id| {
            rpc_error(
                request_id,
                -32601,
                "method_not_found",
                format!("unsupported MCP method `{method}`"),
            )
        })),
    }
}

fn read_mcp_message(reader: &mut dyn BufRead) -> Result<Option<Value>> {
    let mut content_length = None::<usize>;
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).context("failed to read MCP header line")?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        let Some((name, value)) = trimmed.split_once(':') else {
            anyhow::bail!("invalid MCP header line `{trimmed}`");
        };
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .with_context(|| format!("invalid Content-Length value `{}`", value.trim()))?,
            );
        }
    }
    let content_length = content_length.context("missing Content-Length header")?;
    let mut payload = vec![0_u8; content_length];
    reader.read_exact(payload.as_mut_slice()).context("failed to read framed MCP payload")?;
    serde_json::from_slice::<Value>(payload.as_slice())
        .context("failed to parse MCP JSON payload")
        .map(Some)
}

fn write_mcp_message(writer: &mut dyn Write, payload: &Value) -> Result<()> {
    let encoded =
        serde_json::to_vec(payload).context("failed to serialize MCP response payload")?;
    write!(writer, "Content-Length: {}\r\n\r\n", encoded.len())
        .context("failed to write MCP response header")?;
    writer.write_all(encoded.as_slice()).context("failed to write MCP response body")?;
    writer.flush().context("failed to flush MCP response")
}

fn registered_tools(read_only: bool) -> Vec<Value> {
    let tools = [
        TOOL_SESSIONS_LIST,
        TOOL_SESSION_TRANSCRIPT_READ,
        TOOL_SESSION_EXPORT,
        TOOL_MEMORY_SEARCH,
        TOOL_APPROVALS_LIST,
        TOOL_SESSION_CREATE,
        TOOL_SESSION_PROMPT,
        TOOL_APPROVAL_DECIDE,
    ];
    tools
        .into_iter()
        .filter(|name| !read_only || !is_mutating_tool(name))
        .map(tool_definition)
        .collect()
}

fn tool_definition(name: &str) -> Value {
    match name {
        TOOL_SESSIONS_LIST => json!({
            "name": TOOL_SESSIONS_LIST,
            "title": "List sessions",
            "description": "List visible Palyra sessions for the current principal and channel scope.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "after_session_key": { "type": "string" },
                    "include_archived": { "type": "boolean" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200 },
                    "query": { "type": "string" },
                },
                "additionalProperties": false,
            },
        }),
        TOOL_SESSION_TRANSCRIPT_READ => json!({
            "name": TOOL_SESSION_TRANSCRIPT_READ,
            "title": "Read session transcript",
            "description": "Read the transcript payload for a resolved session.",
            "inputSchema": session_locator_schema(),
        }),
        TOOL_SESSION_EXPORT => json!({
            "name": TOOL_SESSION_EXPORT,
            "title": "Export session",
            "description": "Export a resolved session as JSON or Markdown.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string" },
                    "session_key": { "type": "string" },
                    "session_label": { "type": "string" },
                    "require_existing": { "type": "boolean" },
                    "reset_session": { "type": "boolean" },
                    "format": {
                        "type": "string",
                        "enum": ["json", "markdown"],
                    },
                },
                "additionalProperties": false,
            },
        }),
        TOOL_MEMORY_SEARCH => json!({
            "name": TOOL_MEMORY_SEARCH,
            "title": "Search memory",
            "description": "Search scoped Palyra memory with the same access controls used by the CLI.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "scope": { "type": "string", "enum": ["principal", "channel", "session"] },
                    "channel": { "type": "string" },
                    "session_id": { "type": "string" },
                    "top_k": { "type": "integer", "minimum": 1, "maximum": 100 },
                    "min_score": { "type": "number", "minimum": 0, "maximum": 1 },
                    "include_score_breakdown": { "type": "boolean" },
                    "tags": { "type": "array", "items": { "type": "string" } },
                    "sources": { "type": "array", "items": { "type": "string" } },
                },
                "required": ["query"],
                "additionalProperties": false,
            },
        }),
        TOOL_APPROVALS_LIST => json!({
            "name": TOOL_APPROVALS_LIST,
            "title": "List approvals",
            "description": "List approval records visible to the current admin-capable connection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "after_approval_id": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200 },
                    "since_unix_ms": { "type": "integer" },
                    "until_unix_ms": { "type": "integer" },
                    "subject_id": { "type": "string" },
                    "principal": { "type": "string" },
                    "decision": {
                        "type": "string",
                        "enum": ["allow", "deny", "timeout", "error"],
                    },
                    "subject_type": {
                        "type": "string",
                        "enum": [
                            "tool",
                            "channel_send",
                            "secret_access",
                            "browser_action",
                            "node_capability",
                            "device_pairing"
                        ],
                    },
                },
                "additionalProperties": false,
            },
        }),
        TOOL_SESSION_CREATE => json!({
            "name": TOOL_SESSION_CREATE,
            "title": "Create or resolve session",
            "description": "Resolve a Palyra session using the same defaults and binding rules as ACP.",
            "inputSchema": session_locator_schema(),
        }),
        TOOL_SESSION_PROMPT => json!({
            "name": TOOL_SESSION_PROMPT,
            "title": "Send prompt",
            "description": "Send a prompt into a resolved session and stream until completion or approval wait.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string" },
                    "session_key": { "type": "string" },
                    "session_label": { "type": "string" },
                    "require_existing": { "type": "boolean" },
                    "reset_session": { "type": "boolean" },
                    "prompt": { "type": "string" },
                    "allow_sensitive_tools": { "type": "boolean" },
                },
                "required": ["prompt"],
                "additionalProperties": false,
            },
        }),
        TOOL_APPROVAL_DECIDE => json!({
            "name": TOOL_APPROVAL_DECIDE,
            "title": "Resolve approval",
            "description": "Approve or deny a pending approval using the existing Palyra approval model.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "approval_id": { "type": "string" },
                    "approved": { "type": "boolean" },
                    "decision_scope": {
                        "type": "string",
                        "enum": ["once", "session", "timeboxed"],
                    },
                    "decision_scope_ttl_ms": { "type": "integer", "minimum": 1 },
                    "reason": { "type": "string" },
                },
                "required": ["approval_id", "approved"],
                "additionalProperties": false,
            },
        }),
        other => json!({
            "name": other,
            "title": other,
            "description": "Undocumented MCP tool",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false,
            },
        }),
    }
}

fn session_locator_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "session_id": { "type": "string" },
            "session_key": { "type": "string" },
            "session_label": { "type": "string" },
            "require_existing": { "type": "boolean" },
            "reset_session": { "type": "boolean" },
        },
        "additionalProperties": false,
    })
}

fn rpc_error(id: Value, code: i64, kind: &str, message: impl Into<String>) -> Value {
    let message = message.into();
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": {
            "code": code,
            "message": message,
            "data": {
                "kind": kind,
            },
        },
    })
}

fn tool_success_payload(value: Value) -> Value {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
    json!({
        "content": [{
            "type": "text",
            "text": text,
        }],
        "structuredContent": value,
        "isError": false,
    })
}

fn tool_error_payload(message: impl Into<String>) -> Value {
    let message = message.into();
    json!({
        "content": [{
            "type": "text",
            "text": message,
        }],
        "isError": true,
    })
}

fn is_mutating_tool(name: &str) -> bool {
    matches!(name, TOOL_SESSION_CREATE | TOOL_SESSION_PROMPT | TOOL_APPROVAL_DECIDE)
}

fn expect_arguments_object<'a>(
    value: &'a Value,
    tool_name: &str,
) -> Result<&'a Map<String, Value>> {
    value.as_object().ok_or_else(|| anyhow!("tool `{tool_name}` requires an arguments object"))
}

fn required_string_arg(args: &Map<String, Value>, key: &str) -> Result<String> {
    opt_string_arg(args, key)?.ok_or_else(|| anyhow!("missing required string argument `{key}`"))
}

fn opt_string_arg(args: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(normalize_optional_text(value.as_str())),
        Some(_) => anyhow::bail!("argument `{key}` must be a string"),
    }
}

fn required_bool_arg(args: &Map<String, Value>, key: &str) -> Result<bool> {
    opt_bool_arg(args, key)?.ok_or_else(|| anyhow!("missing required boolean argument `{key}`"))
}

fn opt_bool_arg(args: &Map<String, Value>, key: &str) -> Result<Option<bool>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => anyhow::bail!("argument `{key}` must be a boolean"),
    }
}

fn opt_u32_arg(args: &Map<String, Value>, key: &str) -> Result<Option<u32>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            let value = value
                .as_u64()
                .ok_or_else(|| anyhow!("argument `{key}` must be an unsigned integer"))?;
            u32::try_from(value)
                .map(Some)
                .with_context(|| format!("argument `{key}` exceeds u32 range"))
        }
        Some(_) => anyhow::bail!("argument `{key}` must be an unsigned integer"),
    }
}

fn opt_i64_arg(args: &Map<String, Value>, key: &str) -> Result<Option<i64>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| anyhow!("argument `{key}` must be a signed integer")),
        Some(_) => anyhow::bail!("argument `{key}` must be a signed integer"),
    }
}

fn opt_f64_arg(args: &Map<String, Value>, key: &str) -> Result<Option<f64>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            value.as_f64().map(Some).ok_or_else(|| anyhow!("argument `{key}` must be a number"))
        }
        Some(_) => anyhow::bail!("argument `{key}` must be a number"),
    }
}

fn opt_string_vec_arg(args: &Map<String, Value>, key: &str) -> Result<Vec<String>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(values)) => values
            .iter()
            .enumerate()
            .map(|(index, value)| match value {
                Value::String(value) => {
                    Ok(normalize_optional_text(value.as_str()).unwrap_or_default())
                }
                _ => anyhow::bail!("argument `{key}[{index}]` must be a string"),
            })
            .filter(|entry| match entry {
                Ok(value) => !value.is_empty(),
                Err(_) => true,
            })
            .collect(),
        Some(_) => anyhow::bail!("argument `{key}` must be an array of strings"),
    }
}

fn resolve_memory_scope_for_mcp(
    scope: &str,
    channel: Option<String>,
    session_id: Option<String>,
    connection: &AgentConnection,
) -> Result<(Option<String>, Option<String>)> {
    let scope = scope.trim().to_ascii_lowercase();
    let channel = normalize_optional_owned_text(channel);
    let session_id = normalize_optional_owned_text(session_id);
    match scope.as_str() {
        "principal" | "" => Ok((channel, None)),
        "channel" => {
            let channel = channel.or_else(|| Some(connection.channel.clone()));
            Ok((channel, None))
        }
        "session" => {
            let session_id = session_id
                .ok_or_else(|| anyhow!("memory_search scope=session requires session_id"))?;
            validate_canonical_id(session_id.as_str())
                .context("memory_search.session_id must be a canonical ULID")?;
            Ok((channel.or_else(|| Some(connection.channel.clone())), Some(session_id)))
        }
        other => anyhow::bail!(
            "memory_search scope must be one of: principal, channel, session (got `{other}`)"
        ),
    }
}

fn parse_memory_source_arg(value: &str) -> Result<MemorySourceArg> {
    match value.trim().to_ascii_lowercase().as_str() {
        "tapeusermessage" | "tape_user_message" | "tape:user_message" => {
            Ok(MemorySourceArg::TapeUserMessage)
        }
        "tapetoolresult" | "tape_tool_result" | "tape:tool_result" => {
            Ok(MemorySourceArg::TapeToolResult)
        }
        "summary" => Ok(MemorySourceArg::Summary),
        "manual" => Ok(MemorySourceArg::Manual),
        "import" => Ok(MemorySourceArg::Import),
        other => anyhow::bail!(
            "unsupported memory source `{other}`; expected tape_user_message, tape_tool_result, summary, manual, or import"
        ),
    }
}

fn approval_decision_filter_arg(args: &Map<String, Value>, key: &str) -> Result<i32> {
    let value = opt_string_arg(args, key)?;
    Ok(match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        None => gateway_v1::ApprovalDecision::Unspecified as i32,
        Some("allow") => gateway_v1::ApprovalDecision::Allow as i32,
        Some("deny") => gateway_v1::ApprovalDecision::Deny as i32,
        Some("timeout") => gateway_v1::ApprovalDecision::Timeout as i32,
        Some("error") => gateway_v1::ApprovalDecision::Error as i32,
        Some(other) => anyhow::bail!(
            "unsupported approval decision `{other}`; expected allow, deny, timeout, or error"
        ),
    })
}

fn approval_subject_type_filter_arg(args: &Map<String, Value>, key: &str) -> Result<i32> {
    let value = opt_string_arg(args, key)?;
    Ok(match value
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None => gateway_v1::ApprovalSubjectType::Unspecified as i32,
        Some("tool") => gateway_v1::ApprovalSubjectType::Tool as i32,
        Some("channelsend") | Some("channel_send") => {
            gateway_v1::ApprovalSubjectType::ChannelSend as i32
        }
        Some("secretaccess") | Some("secret_access") => {
            gateway_v1::ApprovalSubjectType::SecretAccess as i32
        }
        Some("browseraction") | Some("browser_action") => {
            gateway_v1::ApprovalSubjectType::BrowserAction as i32
        }
        Some("nodecapability") | Some("node_capability") => {
            gateway_v1::ApprovalSubjectType::NodeCapability as i32
        }
        Some("devicepairing") | Some("device_pairing") => {
            gateway_v1::ApprovalSubjectType::DevicePairing as i32
        }
        Some(other) => anyhow::bail!(
            "unsupported approval subject type `{other}`; expected tool, channel_send, secret_access, browser_action, node_capability, or device_pairing"
        ),
    })
}

fn validate_mcp_approval_scope(scope: &str, ttl_ms: Option<i64>) -> Result<()> {
    match scope {
        "once" | "session" => {
            if ttl_ms.is_some() {
                anyhow::bail!(
                    "approval_decide.decision_scope_ttl_ms is only valid when decision_scope=timeboxed"
                );
            }
        }
        "timeboxed" => {
            let ttl_ms = ttl_ms.ok_or_else(|| {
                anyhow!("approval_decide.decision_scope_ttl_ms is required for timeboxed decisions")
            })?;
            if ttl_ms <= 0 {
                anyhow::bail!("approval_decide.decision_scope_ttl_ms must be greater than zero");
            }
        }
        other => anyhow::bail!(
            "approval_decide.decision_scope must be one of: once, session, timeboxed (got `{other}`)"
        ),
    }
    Ok(())
}

fn session_summary_to_json(session: &gateway_v1::SessionSummary) -> Value {
    json!({
        "session_id": session.session_id.as_ref().map(|value| value.ulid.clone()),
        "session_key": normalize_optional_text(session.session_key.as_str()),
        "session_label": normalize_optional_text(session.session_label.as_str()),
        "title": normalize_optional_text(session.title.as_str()),
        "title_source": normalize_optional_text(session.title_source.as_str()),
        "title_generator_version": normalize_optional_text(session.title_generator_version.as_str()),
        "preview": normalize_optional_text(session.preview.as_str()),
        "preview_state": normalize_optional_text(session.preview_state.as_str()),
        "last_intent": normalize_optional_text(session.last_intent.as_str()),
        "last_summary": normalize_optional_text(session.last_summary.as_str()),
        "match_snippet": normalize_optional_text(session.match_snippet.as_str()),
        "branch_state": normalize_optional_text(session.branch_state.as_str()),
        "parent_session_id": session.parent_session_id.as_ref().map(|value| value.ulid.clone()),
        "last_run_state": normalize_optional_text(session.last_run_state.as_str()),
        "created_at_unix_ms": session.created_at_unix_ms,
        "updated_at_unix_ms": session.updated_at_unix_ms,
        "last_run_id": session.last_run_id.as_ref().map(|value| value.ulid.clone()),
        "archived_at_unix_ms": optional_unix_ms_json_value(session.archived_at_unix_ms),
    })
}

fn optional_unix_ms_json_value(value: i64) -> Value {
    if value <= 0 {
        Value::Null
    } else {
        json!(value)
    }
}

fn tool_approval_request_to_json(request: &common_v1::ToolApprovalRequest) -> Value {
    let prompt = request.prompt.as_ref().map(|prompt| {
        let details_json = if prompt.details_json.is_empty() {
            json!({})
        } else {
            serde_json::from_slice::<Value>(prompt.details_json.as_slice()).unwrap_or_else(|_| {
                json!({
                    "raw": String::from_utf8_lossy(prompt.details_json.as_slice()).to_string(),
                })
            })
        };
        json!({
            "title": normalize_optional_text(prompt.title.as_str()),
            "risk_level": approval_risk_to_text(prompt.risk_level),
            "subject_id": normalize_optional_text(prompt.subject_id.as_str()),
            "summary": normalize_optional_text(prompt.summary.as_str()),
            "timeout_seconds": prompt.timeout_seconds,
            "policy_explanation": normalize_optional_text(prompt.policy_explanation.as_str()),
            "options": prompt.options.iter().map(|option| json!({
                "option_id": normalize_optional_text(option.option_id.as_str()),
                "label": normalize_optional_text(option.label.as_str()),
                "description": normalize_optional_text(option.description.as_str()),
                "default_selected": option.default_selected,
                "decision_scope": approval_scope_to_text(option.decision_scope),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })).collect::<Vec<Value>>(),
            "details_json": details_json,
        })
    });
    json!({
        "proposal_id": request.proposal_id.as_ref().map(|value| value.ulid.clone()),
        "approval_id": request.approval_id.as_ref().map(|value| value.ulid.clone()),
        "tool_name": normalize_optional_text(request.tool_name.as_str()),
        "request_summary": normalize_optional_text(request.request_summary.as_str()),
        "approval_required": request.approval_required,
        "prompt": prompt,
    })
}

fn mcp_run_stream_event_to_json(event: &common_v1::RunStreamEvent) -> Value {
    let run_id = event.run_id.as_ref().map(|value| value.ulid.clone());
    match event.body.as_ref() {
        Some(common_v1::run_stream_event::Body::ModelToken(token)) => json!({
            "type": "model_token",
            "run_id": run_id,
            "token": token.token,
            "is_final": token.is_final,
        }),
        Some(common_v1::run_stream_event::Body::Status(status)) => json!({
            "type": "status",
            "run_id": run_id,
            "kind": stream_status_kind_to_text(status.kind),
            "message": normalize_optional_text(status.message.as_str()),
        }),
        Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => json!({
            "type": "tool_proposal",
            "run_id": run_id,
            "proposal_id": proposal.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "tool_name": normalize_optional_text(proposal.tool_name.as_str()),
            "approval_required": proposal.approval_required,
        }),
        Some(common_v1::run_stream_event::Body::ToolDecision(decision)) => json!({
            "type": "tool_decision",
            "run_id": run_id,
            "proposal_id": decision.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "kind": tool_decision_kind_to_text(decision.kind),
            "reason": normalize_optional_text(decision.reason.as_str()),
            "approval_required": decision.approval_required,
            "policy_enforced": decision.policy_enforced,
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalRequest(request)) => json!({
            "type": "tool_approval_request",
            "run_id": run_id,
            "request": tool_approval_request_to_json(request),
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalResponse(response)) => json!({
            "type": "tool_approval_response",
            "run_id": run_id,
            "proposal_id": response.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "approval_id": response.approval_id.as_ref().map(|value| value.ulid.clone()),
            "approved": response.approved,
            "decision_scope": approval_scope_to_text(response.decision_scope),
            "decision_scope_ttl_ms": response.decision_scope_ttl_ms,
            "reason": normalize_optional_text(response.reason.as_str()),
        }),
        Some(common_v1::run_stream_event::Body::ToolResult(result)) => json!({
            "type": "tool_result",
            "run_id": run_id,
            "proposal_id": result.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "success": result.success,
            "error": normalize_optional_text(result.error.as_str()),
        }),
        Some(common_v1::run_stream_event::Body::ToolAttestation(attestation)) => json!({
            "type": "tool_attestation",
            "run_id": run_id,
            "proposal_id": attestation.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "attestation_id": attestation.attestation_id.as_ref().map(|value| value.ulid.clone()),
            "timed_out": attestation.timed_out,
            "executor": normalize_optional_text(attestation.executor.as_str()),
        }),
        Some(common_v1::run_stream_event::Body::A2uiUpdate(update)) => json!({
            "type": "a2ui_update",
            "run_id": run_id,
            "surface": normalize_optional_text(update.surface.as_str()),
            "version": update.v,
        }),
        Some(common_v1::run_stream_event::Body::JournalEvent(event)) => json!({
            "type": "journal_event",
            "run_id": run_id,
            "event_id": event.event_id.as_ref().map(|value| value.ulid.clone()),
            "kind": event.kind,
            "actor": event.actor,
        }),
        None => json!({
            "type": "unknown",
            "run_id": run_id,
        }),
    }
}

fn percent_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push_str(format!("{byte:02X}").as_str());
        }
    }
    encoded
}

fn normalize_optional_text(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestBackend {
        read_only: bool,
        last_call: Option<(String, Value)>,
        response: Value,
    }

    impl McpBackend for TestBackend {
        fn read_only(&self) -> bool {
            self.read_only
        }

        fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<Value> {
            self.last_call = Some((name.to_owned(), arguments.clone()));
            Ok(self.response.clone())
        }
    }

    #[test]
    fn tools_list_hides_mutations_in_read_only_mode() {
        let tools = registered_tools(true);
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert!(names.contains(&TOOL_SESSIONS_LIST));
        assert!(!names.contains(&TOOL_SESSION_PROMPT));
        assert!(!names.contains(&TOOL_APPROVAL_DECIDE));
    }

    #[test]
    fn tools_call_rejects_mutation_when_read_only() {
        let mut backend =
            TestBackend { read_only: true, last_call: None, response: json!({"ok": true}) };
        let response = handle_mcp_request(
            &mut backend,
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": TOOL_SESSION_PROMPT,
                    "arguments": { "prompt": "hi" }
                }
            }),
        )
        .expect("request should succeed")
        .expect("response should be present");
        assert_eq!(backend.last_call, None);
        assert_eq!(response["result"]["isError"], Value::Bool(true));
    }

    #[test]
    fn framing_round_trip_parses_single_message() {
        let body = json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": 1,
            "method": "ping"
        });
        let encoded = serde_json::to_vec(&body).expect("serialize");
        let frame = format!("Content-Length: {}\r\n\r\n", encoded.len());
        let mut bytes = frame.into_bytes();
        bytes.extend_from_slice(encoded.as_slice());
        let mut cursor = std::io::Cursor::new(bytes);
        let parsed = read_mcp_message(&mut cursor)
            .expect("frame should parse")
            .expect("message should exist");
        assert_eq!(parsed, body);
    }
}
