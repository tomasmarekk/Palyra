//! Prepared MCP invocation snapshots used by catalog refresh handling.
//!
//! Keeping preparation separate ensures list-changed notifications affect future calls
//! without mutating the manifest and schema already pinned by an in-flight call.

use super::*;

impl McpBroker {
    pub(super) fn prepare_tool_invocation(
        &self,
        request: McpToolCallRequest,
    ) -> Result<McpToolInvocationPreparation, McpBrokerError> {
        let server_id = normalize_mcp_identifier(request.server_name.as_str(), "server_name")?;
        let namespaced_tool_id =
            namespaced_tool_id(request.server_name.as_str(), request.tool_name.as_str())?;
        let record = self.server_record(server_id.as_str())?.clone();
        let transport_id = transport_id_for_manifest(&record.manifest);
        let mut audit_context = McpInvocationAuditContext::new(
            server_id.clone(),
            namespaced_tool_id,
            transport_id,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        );
        if record.state != McpServerLifecycleState::Healthy {
            return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                &request,
                &audit_context,
                "mcp.server_not_ready",
                format!("MCP server is {}", record.state.as_str()).as_str(),
            )));
        }
        if !tool_allowed_by_manifest(&record.manifest, request.tool_name.as_str()) {
            return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                &request,
                &audit_context,
                "mcp.tool_not_allowed",
                "tool is not allowed by the MCP server manifest",
            )));
        }
        let Some(registry_entry) =
            record.imported_tools.get(audit_context.namespaced_tool_id.as_str())
        else {
            return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                &request,
                &audit_context,
                "mcp.tool_not_discovered",
                "tool must be discovered and cataloged before invocation",
            )));
        };
        audit_context.result_projection = registry_entry.projection_policy;
        if request.schema_hash != registry_entry.schema_hash {
            return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                &request,
                &audit_context,
                "mcp.schema_hash_mismatch",
                "request schema_hash does not match the discovered MCP tool schema",
            )));
        }
        if !request.policy.allowed {
            return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                &request,
                &audit_context,
                "mcp.policy_denied",
                request.policy.reason.as_str(),
            )));
        }
        let approval_required = request.policy.approval_required
            || mcp_registry_entry_requires_approval(registry_entry);
        if approval_required && !request.approval_granted {
            return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                &request,
                &audit_context,
                "mcp.approval_required",
                "operator approval is required before this MCP tool may execute",
            )));
        }
        if approval_required && !valid_optional_invocation_id(request.approval_id.as_deref()) {
            return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                &request,
                &audit_context,
                "mcp.approval_id_required",
                "operator approval must include a bounded approval id",
            )));
        }
        if !request.input.is_object() {
            return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                &request,
                &audit_context,
                "mcp.input_not_object",
                "MCP tool input must be a JSON object",
            )));
        }
        match evaluate_mcp_oauth_grant(&record.manifest, current_unix_ms()) {
            Ok(grant_id) => {
                audit_context.oauth_grant_id = grant_id;
            }
            Err(error) => {
                return Ok(McpToolInvocationPreparation::denied(denied_invocation_with_hint(
                    &request,
                    &audit_context,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                    Some(error.repair_hint.as_str()),
                )));
            }
        }
        match resolve_scoped_vault_grants(
            request.vault_refs_requested.as_slice(),
            record.manifest.vault_refs.as_slice(),
            request.vault_scoped_grants.as_slice(),
        ) {
            Ok(vault_grant_ids) => {
                audit_context.vault_grant_ids = vault_grant_ids;
            }
            Err(error) => {
                return Ok(McpToolInvocationPreparation::denied(denied_invocation(
                    &request,
                    &audit_context,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                )));
            }
        }

        let catalog_generation = record.catalog_generation;
        Ok(McpToolInvocationPreparation::Ready(Box::new(McpPreparedToolInvocation {
            request,
            server_id,
            catalog_generation,
            record,
            audit_context,
        })))
    }

    pub(super) fn execute_prepared_tool_invocation(
        &mut self,
        prepared: McpPreparedToolInvocation,
        transport: &dyn McpTransport,
    ) -> Result<McpToolInvocationOutcome, McpBrokerError> {
        let McpPreparedToolInvocation {
            request,
            server_id,
            catalog_generation,
            record,
            mut audit_context,
        } = prepared;
        debug_assert!(catalog_generation > 0, "prepared MCP calls require a discovered catalog");

        audit_context.transport_mode = Some(transport.invocation_mode(&record.manifest));
        let response = match transport.call_tool(&record.manifest, &request) {
            Ok(response) => response,
            Err(error) => {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_invocation(
                    &request,
                    &audit_context,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                ));
            }
        };
        if response.sampling_requested {
            audit_context.sampling_model_capability = response.sampling_model_capability.clone();
            if !host_policy::sampling_allowed_by_manifest(
                &record.manifest,
                response.sampling_model_capability.as_deref(),
            ) {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_invocation_with_hint(
                    &request,
                    &audit_context,
                    "mcp.sampling_denied",
                    "MCP sampling is denied unless this server allowlists the requested model capability",
                    Some(
                        "set mcp.servers[].sampling_policy.mode=allowlist and add the model capability",
                    ),
                ));
            }
        }
        if let Some(host) = response.egress_host_requested.as_deref() {
            if !host_allowed_by_manifest(host, record.manifest.egress_allowlist.as_slice()) {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_invocation(
                    &request,
                    &audit_context,
                    "mcp.egress_denied",
                    "MCP tool attempted egress outside its manifest allowlist",
                ));
            }
        }
        let projected_output = project_mcp_output(
            &response.output,
            record.manifest.max_response_bytes,
            audit_context.result_projection,
        );
        Ok(McpToolInvocationOutcome {
            success: true,
            output_json: projected_output.output_json.clone(),
            error: None,
            attestation: invocation_attestation(
                &request,
                &audit_context,
                &response.output,
                "allowed",
                projected_output.output_truncated,
            ),
        })
    }

    /// Records a protocol violation and quarantines repeated offenders.
    pub fn record_protocol_violation(
        &mut self,
        server_name: &str,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        self.record_protocol_violation_with_policy(server_name, true)
    }
}
