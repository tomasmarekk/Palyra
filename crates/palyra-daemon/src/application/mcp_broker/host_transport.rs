//! Runtime MCP transport implementation and JSON-RPC response parsing.

use super::host_types::default_resource_mime_type;
use super::*;

impl McpTransport for McpRuntimeTransport {
    fn start(&self, manifest: &McpServerManifest) -> Result<(), McpBrokerError> {
        match &manifest.transport {
            McpTransportManifest::Stdio { .. } => {
                execute_stdio_jsonrpc(manifest, None, manifest.start_timeout_ms).map(|_| ())
            }
            McpTransportManifest::Http { .. } | McpTransportManifest::Sse { .. } => {
                execute_remote_jsonrpc(
                    manifest,
                    "initialize",
                    mcp_initialize_params(),
                    manifest.start_timeout_ms,
                )
                .map(|_| ())
            }
        }
        .map_err(Into::into)
    }

    fn list_tools(
        &self,
        manifest: &McpServerManifest,
    ) -> Result<Vec<McpDiscoveredTool>, McpBrokerError> {
        let result = match &manifest.transport {
            McpTransportManifest::Stdio { .. } => execute_stdio_jsonrpc(
                manifest,
                Some(("tools/list", json!({}))),
                manifest.timeout_ms,
            ),
            McpTransportManifest::Http { .. } | McpTransportManifest::Sse { .. } => {
                execute_remote_jsonrpc(manifest, "tools/list", json!({}), manifest.timeout_ms)
            }
        }?;
        tools_from_mcp_result(&result).map_err(Into::into)
    }

    fn list_resources(
        &self,
        manifest: &McpServerManifest,
    ) -> Result<Vec<McpDiscoveredResource>, McpBrokerError> {
        let result = match &manifest.transport {
            McpTransportManifest::Stdio { .. } => execute_stdio_jsonrpc(
                manifest,
                Some(("resources/list", json!({}))),
                manifest.timeout_ms,
            ),
            McpTransportManifest::Http { .. } | McpTransportManifest::Sse { .. } => {
                execute_remote_jsonrpc(manifest, "resources/list", json!({}), manifest.timeout_ms)
            }
        }?;
        resources_from_mcp_result(&result).map_err(Into::into)
    }

    fn read_resource(
        &self,
        manifest: &McpServerManifest,
        request: &McpResourceReadRequest,
    ) -> Result<McpResourceReadPayload, McpBrokerError> {
        let params = json!({ "uri": request.uri.as_str() });
        let result = match &manifest.transport {
            McpTransportManifest::Stdio { .. } => execute_stdio_jsonrpc(
                manifest,
                Some(("resources/read", params)),
                manifest.timeout_ms,
            ),
            McpTransportManifest::Http { .. } | McpTransportManifest::Sse { .. } => {
                execute_remote_jsonrpc(manifest, "resources/read", params, manifest.timeout_ms)
            }
        }?;
        resource_payload_from_mcp_result(&result, request.uri.as_str()).map_err(Into::into)
    }

    fn list_prompts(
        &self,
        manifest: &McpServerManifest,
    ) -> Result<Vec<McpDiscoveredPrompt>, McpBrokerError> {
        let result = match &manifest.transport {
            McpTransportManifest::Stdio { .. } => execute_stdio_jsonrpc(
                manifest,
                Some(("prompts/list", json!({}))),
                manifest.timeout_ms,
            ),
            McpTransportManifest::Http { .. } | McpTransportManifest::Sse { .. } => {
                execute_remote_jsonrpc(manifest, "prompts/list", json!({}), manifest.timeout_ms)
            }
        }?;
        prompts_from_mcp_result(&result).map_err(Into::into)
    }

    fn get_prompt(
        &self,
        manifest: &McpServerManifest,
        request: &McpPromptGetRequest,
    ) -> Result<McpPromptPayload, McpBrokerError> {
        let params = json!({
            "name": request.name.as_str(),
            "arguments": request.arguments.clone(),
        });
        let result = match &manifest.transport {
            McpTransportManifest::Stdio { .. } => {
                execute_stdio_jsonrpc(manifest, Some(("prompts/get", params)), manifest.timeout_ms)
            }
            McpTransportManifest::Http { .. } | McpTransportManifest::Sse { .. } => {
                execute_remote_jsonrpc(manifest, "prompts/get", params, manifest.timeout_ms)
            }
        }?;
        prompt_payload_from_mcp_result(&result, request.name.as_str()).map_err(Into::into)
    }

    fn call_tool(
        &self,
        manifest: &McpServerManifest,
        request: &McpToolCallRequest,
    ) -> Result<McpToolResponse, McpBrokerError> {
        let params = json!({
            "name": request.tool_name,
            "arguments": request.input,
        });
        let result = match &manifest.transport {
            McpTransportManifest::Stdio { .. } => {
                execute_stdio_jsonrpc(manifest, Some(("tools/call", params)), manifest.timeout_ms)
            }
            McpTransportManifest::Http { .. } | McpTransportManifest::Sse { .. } => {
                execute_remote_jsonrpc(manifest, "tools/call", params, manifest.timeout_ms)
            }
        }?;
        Ok(McpToolResponse {
            output: result,
            sampling_requested: false,
            sampling_model_capability: None,
            egress_host_requested: None,
        })
    }
}

pub(super) fn resources_from_mcp_result(
    result: &Value,
) -> Result<Vec<McpDiscoveredResource>, McpTransportError> {
    let resources = result.get("resources").and_then(Value::as_array).ok_or_else(|| {
        McpTransportError::new(
            "mcp.transport_invalid_response",
            "MCP resources/list result missing resources array",
        )
    })?;
    let mut discovered = Vec::with_capacity(resources.len());
    for resource in resources {
        let uri = resource.get("uri").and_then(Value::as_str).ok_or_else(|| {
            McpTransportError::new(
                "mcp.transport_invalid_response",
                "MCP discovered resource missing string uri",
            )
        })?;
        let name = resource.get("name").and_then(Value::as_str).unwrap_or(uri).to_owned();
        let description =
            resource.get("description").and_then(Value::as_str).unwrap_or_default().to_owned();
        let mime_type = resource
            .get("mimeType")
            .or_else(|| resource.get("mime_type"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(default_resource_mime_type);
        let schema_hash = resource
            .get("schemaHash")
            .or_else(|| resource.get("schema_hash"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| stable_hash_value(resource));
        let size_bytes = resource
            .get("sizeBytes")
            .or_else(|| resource.get("size_bytes"))
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok());
        let egress_host = resource
            .get("egressHost")
            .or_else(|| resource.get("egress_host"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        discovered.push(McpDiscoveredResource {
            uri: uri.to_owned(),
            name,
            description,
            mime_type,
            schema_hash,
            size_bytes,
            egress_host,
        });
    }
    Ok(discovered)
}

pub(super) fn resource_payload_from_mcp_result(
    result: &Value,
    requested_uri: &str,
) -> Result<McpResourceReadPayload, McpTransportError> {
    let content_value = if let Some(contents) = result.get("contents").and_then(Value::as_array) {
        let first = contents.first().ok_or_else(|| {
            McpTransportError::new(
                "mcp.transport_invalid_response",
                "MCP resources/read result returned an empty contents array",
            )
        })?;
        first.clone()
    } else {
        result.clone()
    };
    let uri = content_value.get("uri").and_then(Value::as_str).unwrap_or(requested_uri).to_owned();
    let mime_type = content_value
        .get("mimeType")
        .or_else(|| content_value.get("mime_type"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(default_resource_mime_type);
    let content = content_value
        .get("text")
        .or_else(|| content_value.get("blob"))
        .or_else(|| content_value.get("content"))
        .cloned()
        .unwrap_or(content_value.clone());
    let egress_host_requested = content_value
        .get("egressHost")
        .or_else(|| content_value.get("egress_host_requested"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Ok(McpResourceReadPayload { uri, mime_type, content, egress_host_requested })
}

pub(super) fn prompts_from_mcp_result(
    result: &Value,
) -> Result<Vec<McpDiscoveredPrompt>, McpTransportError> {
    let prompts = result.get("prompts").and_then(Value::as_array).ok_or_else(|| {
        McpTransportError::new(
            "mcp.transport_invalid_response",
            "MCP prompts/list result missing prompts array",
        )
    })?;
    let mut discovered = Vec::with_capacity(prompts.len());
    for prompt in prompts {
        let name = prompt.get("name").and_then(Value::as_str).ok_or_else(|| {
            McpTransportError::new(
                "mcp.transport_invalid_response",
                "MCP discovered prompt missing string name",
            )
        })?;
        let description =
            prompt.get("description").and_then(Value::as_str).unwrap_or_default().to_owned();
        let argument_schema = prompt
            .get("argumentsSchema")
            .or_else(|| prompt.get("argument_schema"))
            .cloned()
            .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
        let schema_hash = prompt
            .get("schemaHash")
            .or_else(|| prompt.get("schema_hash"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| stable_hash_value(&argument_schema));
        discovered.push(McpDiscoveredPrompt {
            name: name.to_owned(),
            description,
            schema_hash,
            argument_schema,
        });
    }
    Ok(discovered)
}

pub(super) fn prompt_payload_from_mcp_result(
    result: &Value,
    requested_name: &str,
) -> Result<McpPromptPayload, McpTransportError> {
    let name = result.get("name").and_then(Value::as_str).unwrap_or(requested_name).to_owned();
    let messages =
        result.get("messages").cloned().or_else(|| result.get("description").cloned()).ok_or_else(
            || {
                McpTransportError::new(
                    "mcp.transport_invalid_response",
                    "MCP prompts/get result missing messages",
                )
            },
        )?;
    Ok(McpPromptPayload { name, messages })
}
