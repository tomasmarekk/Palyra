//! Host-mediated MCP resource, prompt, sampling, and elicitation broker methods.

use super::host_policy::{normalize_sampling_model_capability, sampling_allowed_by_manifest};
use super::*;

impl McpBroker {
    /// Lists manifest-approved MCP resources through host policy mediation.
    ///
    /// # Errors
    /// Returns an error only when the server name is malformed or unknown.
    pub fn list_resources(
        &mut self,
        request: McpUtilityListRequest,
        transport: &dyn McpTransport,
    ) -> Result<McpUtilityOutcome, McpBrokerError> {
        let server_id = normalize_mcp_identifier(request.server_name.as_str(), "server_name")?;
        let record = self.server_record(server_id.as_str())?.clone();
        let input_json = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let prompt_cache_epoch = mcp_prompt_cache_epoch(&record.manifest, 0);
        let operation = "resources.list";
        if record.state != McpServerLifecycleState::Healthy {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.server_name.as_str(),
                None,
                &input_json,
                "mcp.server_not_ready",
                format!("MCP server is {}", record.state.as_str()).as_str(),
                None,
                prompt_cache_epoch,
            ));
        }
        if !request.policy.allowed {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.server_name.as_str(),
                None,
                &input_json,
                "mcp.policy_denied",
                request.policy.reason.as_str(),
                None,
                prompt_cache_epoch,
            ));
        }
        let discovered = match transport.list_resources(&record.manifest) {
            Ok(resources) => resources,
            Err(error) => {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_utility_outcome(
                    &record.manifest,
                    server_id.as_str(),
                    operation,
                    request.server_name.as_str(),
                    None,
                    &input_json,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                    None,
                    prompt_cache_epoch,
                ));
            }
        };
        let (resources, filtered_resources, protocol_violation) =
            filter_mcp_resources(&record.manifest, discovered.as_slice());
        if protocol_violation {
            self.record_discovery_protocol_violation(server_id.as_str())?;
        }
        let output_json = json!({
            "schema_version": MCP_SCHEMA_VERSION,
            "server_name": record.manifest.name.as_str(),
            "prompt_cache_epoch": prompt_cache_epoch,
            "resources": resources,
            "filtered_resources": filtered_resources,
        });
        Ok(allowed_utility_outcome(
            &record.manifest,
            server_id.as_str(),
            operation,
            request.server_name.as_str(),
            None,
            &input_json,
            output_json,
            None,
            prompt_cache_epoch,
            Vec::new(),
            false,
            false,
        ))
    }

    /// Reads one manifest-approved MCP resource through host policy mediation.
    ///
    /// # Errors
    /// Returns an error only when the server name is malformed or unknown.
    pub fn read_resource(
        &mut self,
        request: McpResourceReadRequest,
        transport: &dyn McpTransport,
    ) -> Result<McpUtilityOutcome, McpBrokerError> {
        let server_id = normalize_mcp_identifier(request.server_name.as_str(), "server_name")?;
        let record = self.server_record(server_id.as_str())?.clone();
        let input_json = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let prompt_cache_epoch = mcp_prompt_cache_epoch(&record.manifest, 0);
        let operation = "resources.read";
        let Some(resource) = manifest_resource(record.manifest.resources.as_slice(), &request.uri)
        else {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.uri.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                "mcp.resource_not_manifested",
                "resource must be declared by the host-reviewed MCP manifest",
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        };
        if request.schema_hash != resource.schema_hash {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.uri.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                "mcp.schema_hash_mismatch",
                "request schema_hash does not match the manifest-approved MCP resource schema",
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        }
        if record.state != McpServerLifecycleState::Healthy {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.uri.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                "mcp.server_not_ready",
                format!("MCP server is {}", record.state.as_str()).as_str(),
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        }
        if let Some(reason_code) = utility_policy_denial(
            &request.policy,
            resource_requires_approval(resource),
            request.approval_granted,
            request.approval_id.as_deref(),
        ) {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.uri.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                reason_code,
                utility_policy_denial_message(reason_code, request.policy.reason.as_str()),
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        }
        let payload = match transport.read_resource(&record.manifest, &request) {
            Ok(payload) => payload,
            Err(error) => {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_utility_outcome(
                    &record.manifest,
                    server_id.as_str(),
                    operation,
                    request.uri.as_str(),
                    Some(request.schema_hash.as_str()),
                    &input_json,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                    request.approval_id.as_deref(),
                    prompt_cache_epoch,
                ));
            }
        };
        if payload.uri != request.uri {
            self.record_protocol_violation(server_id.as_str())?;
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.uri.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                "mcp.resource_uri_mismatch",
                "MCP resource read returned a different URI than requested",
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        }
        if let Some(host) =
            payload.egress_host_requested.as_deref().or(resource.egress_host.as_deref())
        {
            if !host_allowed_by_manifest(host, record.manifest.egress_allowlist.as_slice()) {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_utility_outcome(
                    &record.manifest,
                    server_id.as_str(),
                    operation,
                    request.uri.as_str(),
                    Some(request.schema_hash.as_str()),
                    &input_json,
                    "mcp.egress_denied",
                    "MCP resource attempted egress outside its manifest allowlist",
                    request.approval_id.as_deref(),
                    prompt_cache_epoch,
                ));
            }
        }
        let max_bytes = request
            .max_bytes
            .unwrap_or(resource.max_read_bytes)
            .min(resource.max_read_bytes)
            .min(record.manifest.max_response_bytes);
        let (safe_payload, safety_findings) = sanitize_mcp_resource_payload(&payload);
        let projected = project_mcp_output(
            &safe_payload,
            max_bytes,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        );
        Ok(allowed_utility_outcome(
            &record.manifest,
            server_id.as_str(),
            operation,
            request.uri.as_str(),
            Some(request.schema_hash.as_str()),
            &input_json,
            projected.output_json,
            request.approval_id.as_deref(),
            prompt_cache_epoch,
            safety_findings,
            projected.output_truncated,
            projected.output_truncated,
        ))
    }

    /// Lists manifest-approved MCP prompts through host policy mediation.
    ///
    /// # Errors
    /// Returns an error only when the server name is malformed or unknown.
    pub fn list_prompts(
        &mut self,
        request: McpUtilityListRequest,
        transport: &dyn McpTransport,
    ) -> Result<McpUtilityOutcome, McpBrokerError> {
        let server_id = normalize_mcp_identifier(request.server_name.as_str(), "server_name")?;
        let record = self.server_record(server_id.as_str())?.clone();
        let input_json = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let prompt_cache_epoch = mcp_prompt_cache_epoch(&record.manifest, 0);
        let operation = "prompts.list";
        if record.state != McpServerLifecycleState::Healthy {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.server_name.as_str(),
                None,
                &input_json,
                "mcp.server_not_ready",
                format!("MCP server is {}", record.state.as_str()).as_str(),
                None,
                prompt_cache_epoch,
            ));
        }
        if !request.policy.allowed {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.server_name.as_str(),
                None,
                &input_json,
                "mcp.policy_denied",
                request.policy.reason.as_str(),
                None,
                prompt_cache_epoch,
            ));
        }
        let discovered = match transport.list_prompts(&record.manifest) {
            Ok(prompts) => prompts,
            Err(error) => {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_utility_outcome(
                    &record.manifest,
                    server_id.as_str(),
                    operation,
                    request.server_name.as_str(),
                    None,
                    &input_json,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                    None,
                    prompt_cache_epoch,
                ));
            }
        };
        let (prompts, filtered_prompts, protocol_violation) =
            filter_mcp_prompts(&record.manifest, discovered.as_slice());
        if protocol_violation {
            self.record_discovery_protocol_violation(server_id.as_str())?;
        }
        let output_json = json!({
            "schema_version": MCP_SCHEMA_VERSION,
            "server_name": record.manifest.name.as_str(),
            "prompt_cache_epoch": prompt_cache_epoch,
            "prompts": prompts,
            "filtered_prompts": filtered_prompts,
        });
        Ok(allowed_utility_outcome(
            &record.manifest,
            server_id.as_str(),
            operation,
            request.server_name.as_str(),
            None,
            &input_json,
            output_json,
            None,
            prompt_cache_epoch,
            Vec::new(),
            false,
            false,
        ))
    }

    /// Resolves one manifest-approved MCP prompt through host policy mediation.
    ///
    /// # Errors
    /// Returns an error only when the server name is malformed or unknown.
    pub fn get_prompt(
        &mut self,
        request: McpPromptGetRequest,
        transport: &dyn McpTransport,
    ) -> Result<McpUtilityOutcome, McpBrokerError> {
        let server_id = normalize_mcp_identifier(request.server_name.as_str(), "server_name")?;
        let record = self.server_record(server_id.as_str())?.clone();
        let input_json = serde_json::to_value(&request).unwrap_or_else(|_| json!({}));
        let prompt_cache_epoch = mcp_prompt_cache_epoch(&record.manifest, 0);
        let operation = "prompts.get";
        let Some(prompt) = manifest_prompt(record.manifest.prompts.as_slice(), &request.name)
        else {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.name.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                "mcp.prompt_not_manifested",
                "prompt must be declared by the host-reviewed MCP manifest",
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        };
        if request.schema_hash != prompt.schema_hash {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.name.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                "mcp.schema_hash_mismatch",
                "request schema_hash does not match the manifest-approved MCP prompt schema",
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        }
        if record.state != McpServerLifecycleState::Healthy {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.name.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                "mcp.server_not_ready",
                format!("MCP server is {}", record.state.as_str()).as_str(),
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        }
        if let Some(reason_code) = utility_policy_denial(
            &request.policy,
            prompt_requires_approval(prompt),
            request.approval_granted,
            request.approval_id.as_deref(),
        ) {
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.name.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                reason_code,
                utility_policy_denial_message(reason_code, request.policy.reason.as_str()),
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        }
        let payload = match transport.get_prompt(&record.manifest, &request) {
            Ok(payload) => payload,
            Err(error) => {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_utility_outcome(
                    &record.manifest,
                    server_id.as_str(),
                    operation,
                    request.name.as_str(),
                    Some(request.schema_hash.as_str()),
                    &input_json,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                    request.approval_id.as_deref(),
                    prompt_cache_epoch,
                ));
            }
        };
        if payload.name != request.name {
            self.record_protocol_violation(server_id.as_str())?;
            return Ok(denied_utility_outcome(
                &record.manifest,
                server_id.as_str(),
                operation,
                request.name.as_str(),
                Some(request.schema_hash.as_str()),
                &input_json,
                "mcp.prompt_name_mismatch",
                "MCP prompt/get returned a different prompt than requested",
                request.approval_id.as_deref(),
                prompt_cache_epoch,
            ));
        }
        let (safe_payload, safety_findings) = sanitize_mcp_prompt_payload(&payload);
        let projected = project_mcp_output(
            &safe_payload,
            prompt.max_prompt_bytes.min(record.manifest.max_response_bytes),
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        );
        Ok(allowed_utility_outcome(
            &record.manifest,
            server_id.as_str(),
            operation,
            request.name.as_str(),
            Some(request.schema_hash.as_str()),
            &input_json,
            projected.output_json,
            request.approval_id.as_deref(),
            prompt_cache_epoch,
            safety_findings,
            projected.output_truncated,
            projected.output_truncated,
        ))
    }

    /// Handles an MCP sampling/createMessage request without letting the MCP
    /// server choose provider, model, budget, or redaction behavior.
    ///
    /// # Errors
    /// Returns an error only when the server name is malformed or unknown.
    pub fn handle_sampling_create_message(
        &self,
        request: McpSamplingCreateMessageRequest,
        host_policy: &McpHostSamplingPolicy,
    ) -> Result<McpSamplingOutcome, McpBrokerError> {
        let server_id = normalize_mcp_identifier(request.server_name.as_str(), "server_name")?;
        let record = self.server_record(server_id.as_str())?;
        if !request.policy.allowed {
            return Ok(denied_sampling_outcome(
                &request,
                host_policy,
                "mcp.policy_denied",
                request.policy.reason.as_str(),
            ));
        }
        if !sampling_allowed_by_manifest(
            &record.manifest,
            request.requested_model_capability.as_deref(),
        ) {
            return Ok(denied_sampling_outcome(
            &request,
            host_policy,
            "mcp.sampling_denied",
            "MCP sampling requires a manifest allowlist entry for the requested model capability",
        ));
        }
        if let Some(provider_id) = request.requested_provider_id.as_deref() {
            if provider_id != host_policy.provider_id {
                return Ok(denied_sampling_outcome(
                    &request,
                    host_policy,
                    "mcp.sampling_provider_denied",
                    "MCP server cannot choose a provider outside host policy",
                ));
            }
        }
        if let Some(model_id) = request.requested_model_id.as_deref() {
            if model_id != host_policy.model_id {
                return Ok(denied_sampling_outcome(
                    &request,
                    host_policy,
                    "mcp.sampling_model_denied",
                    "MCP server cannot choose a model outside host policy",
                ));
            }
        }
        if let Some(capability) = request.requested_model_capability.as_deref() {
            let normalized = normalize_sampling_model_capability(capability)?;
            let allowed = host_policy
                .allowed_model_capabilities
                .iter()
                .filter_map(|candidate| normalize_sampling_model_capability(candidate).ok())
                .any(|candidate| candidate == normalized);
            if !allowed {
                return Ok(denied_sampling_outcome(
                    &request,
                    host_policy,
                    "mcp.sampling_capability_denied",
                    "requested model capability is not allowed by host policy",
                ));
            }
        }
        if request.prompt.len() > host_policy.max_prompt_bytes {
            return Ok(denied_sampling_outcome(
                &request,
                host_policy,
                "mcp.sampling_prompt_too_large",
                "sampling prompt exceeds the host-owned prompt byte limit",
            ));
        }
        if request.max_output_tokens > host_policy.max_output_tokens
            || request.max_output_tokens > host_policy.remaining_budget_tokens
        {
            return Ok(denied_sampling_outcome(
                &request,
                host_policy,
                "mcp.sampling_budget_exceeded",
                "sampling max_output_tokens exceeds host-owned budget",
            ));
        }
        let transform = transform_text_for_prompt(
            request.prompt.as_str(),
            SafetySourceKind::ToolOutput,
            SafetyContentKind::PlainText,
            TrustLabel::ExternalUntrusted,
        );
        let safety_findings = transform.scan.finding_codes();
        if matches!(transform.scan.recommended_action, SafetyAction::Block) || transform.blocked {
            return Ok(denied_sampling_outcome_with_findings(
                &request,
                host_policy,
                "mcp.sampling_prompt_blocked",
                "sampling prompt was blocked by the safety boundary",
                safety_findings,
            ));
        }
        let output_text = format!(
            "MCP sampling request accepted for host-owned provider '{}' and model '{}'.",
            host_policy.provider_id, host_policy.model_id
        );
        Ok(McpSamplingOutcome {
            schema_version: MCP_SAMPLING_SCHEMA_VERSION,
            success: true,
            status: "allowed".to_owned(),
            reason_code: "mcp.sampling_allowed".to_owned(),
            provider_id: host_policy.provider_id.clone(),
            model_id: host_policy.model_id.clone(),
            requested_model_capability: request.requested_model_capability.clone(),
            max_output_tokens: request.max_output_tokens,
            prompt_hash: stable_hash_bytes(request.prompt.as_bytes()),
            redacted_prompt_preview: truncate_at_char_boundary(
                sanitize_mcp_transport_message(transform.transformed_text.as_str()).as_str(),
                256,
            ),
            output_hash: stable_hash_bytes(output_text.as_bytes()),
            output_text,
            safety_findings,
        })
    }

    /// Handles an MCP elicitation request through host routing and bounded response policy.
    ///
    /// # Errors
    /// Returns an error only when the server name is malformed or unknown.
    pub fn handle_elicitation(
        &self,
        request: McpElicitationRequest,
        policy: &McpElicitationPolicy,
        host_response: Option<McpElicitationHostResponse>,
    ) -> Result<McpElicitationOutcome, McpBrokerError> {
        let server_id = normalize_mcp_identifier(request.server_name.as_str(), "server_name")?;
        let _record = self.server_record(server_id.as_str())?;
        let prompt_hash = stable_hash_bytes(request.prompt.as_bytes());
        let purpose_hash = stable_hash_bytes(request.purpose.as_bytes());
        let schema_hash = stable_hash_value(&request.schema);
        let schema_bytes = serde_json::to_vec(&request.schema).unwrap_or_default().len();
        let approval_required = policy.approval_required_for_sensitive
            && matches!(
                request.data_sensitivity,
                McpToolSensitivity::Sensitive | McpToolSensitivity::Secret
            );
        if schema_bytes > policy.max_schema_bytes || !valid_elicitation_schema(&request.schema) {
            return Ok(elicitation_outcome(
                &request,
                policy,
                approval_required,
                "denied",
                "mcp.elicitation_schema_invalid",
                prompt_hash,
                purpose_hash,
                schema_hash,
                Value::Null,
            ));
        }
        if request.policy.as_ref().is_some_and(|decision| !decision.allowed) {
            return Ok(elicitation_outcome(
                &request,
                policy,
                approval_required,
                "denied",
                "mcp.policy_denied",
                prompt_hash,
                purpose_hash,
                schema_hash,
                Value::Null,
            ));
        }
        if approval_required && policy.route != McpElicitationRoute::ApprovalQueue {
            return Ok(elicitation_outcome(
                &request,
                policy,
                approval_required,
                "denied",
                "mcp.elicitation_sensitive_requires_approval",
                prompt_hash,
                purpose_hash,
                schema_hash,
                Value::Null,
            ));
        }
        if policy.route == McpElicitationRoute::Deny {
            return Ok(elicitation_outcome(
                &request,
                policy,
                approval_required,
                "denied",
                "mcp.elicitation_denied_by_host",
                prompt_hash,
                purpose_hash,
                schema_hash,
                Value::Null,
            ));
        }
        let Some(host_response) = host_response else {
            return Ok(elicitation_outcome(
                &request,
                policy,
                approval_required,
                "denied",
                "mcp.elicitation_timeout",
                prompt_hash,
                purpose_hash,
                schema_hash,
                Value::Null,
            ));
        };
        if !host_response.accepted {
            return Ok(elicitation_outcome(
                &request,
                policy,
                approval_required,
                "denied",
                "mcp.elicitation_refused",
                prompt_hash,
                purpose_hash,
                schema_hash,
                Value::Null,
            ));
        }
        let response_bytes = serde_json::to_vec(&host_response.response).unwrap_or_default().len();
        if response_bytes > policy.max_response_bytes {
            return Ok(elicitation_outcome(
                &request,
                policy,
                approval_required,
                "denied",
                "mcp.elicitation_response_too_large",
                prompt_hash,
                purpose_hash,
                schema_hash,
                Value::Null,
            ));
        }
        Ok(elicitation_outcome(
            &request,
            policy,
            approval_required,
            "allowed",
            "mcp.elicitation_allowed",
            prompt_hash,
            purpose_hash,
            schema_hash,
            host_response.response,
        ))
    }
}

/// Computes the deterministic cache epoch contribution for MCP utility surface changes.
#[must_use]
pub fn mcp_prompt_cache_epoch(manifest: &McpServerManifest, catalog_generation: u64) -> u64 {
    let payload = json!({
        "catalog_generation": catalog_generation,
        "resources": manifest.resources.iter().map(|resource| {
            json!({
                "uri": resource.uri.as_str(),
                "schema_hash": resource.schema_hash.as_str(),
                "max_read_bytes": resource.max_read_bytes,
                "approval_policy": resource.approval_policy.as_str(),
                "sensitivity": resource.sensitivity.as_str(),
            })
        }).collect::<Vec<_>>(),
        "prompts": manifest.prompts.iter().map(|prompt| {
            json!({
                "name": prompt.name.as_str(),
                "schema_hash": prompt.schema_hash.as_str(),
                "max_prompt_bytes": prompt.max_prompt_bytes,
                "approval_policy": prompt.approval_policy.as_str(),
                "sensitivity": prompt.sensitivity.as_str(),
            })
        }).collect::<Vec<_>>(),
    });
    let hash = stable_hash_value(&payload);
    u64::from_str_radix(&hash[..16], 16).unwrap_or(0)
}

fn manifest_resource<'a>(
    resources: &'a [McpResourceManifest],
    uri: &str,
) -> Option<&'a McpResourceManifest> {
    resources.iter().find(|resource| resource.uri == uri)
}

fn manifest_prompt<'a>(
    prompts: &'a [McpPromptManifest],
    name: &str,
) -> Option<&'a McpPromptManifest> {
    prompts.iter().find(|prompt| prompt.name == name)
}

fn resource_requires_approval(resource: &McpResourceManifest) -> bool {
    resource.approval_policy == McpApprovalPolicy::RequireApproval
        || matches!(
            resource.sensitivity,
            McpToolSensitivity::Sensitive | McpToolSensitivity::Secret
        )
}

fn prompt_requires_approval(prompt: &McpPromptManifest) -> bool {
    prompt.approval_policy == McpApprovalPolicy::RequireApproval
        || matches!(prompt.sensitivity, McpToolSensitivity::Sensitive | McpToolSensitivity::Secret)
}

fn utility_policy_denial(
    policy: &McpInvocationPolicyDecision,
    manifest_requires_approval: bool,
    approval_granted: bool,
    approval_id: Option<&str>,
) -> Option<&'static str> {
    if !policy.allowed {
        return Some("mcp.policy_denied");
    }
    if (policy.approval_required || manifest_requires_approval) && !approval_granted {
        return Some("mcp.approval_required");
    }
    if (policy.approval_required || manifest_requires_approval)
        && !valid_optional_invocation_id(approval_id)
    {
        return Some("mcp.approval_id_required");
    }
    None
}

fn utility_policy_denial_message<'a>(reason_code: &str, policy_reason: &'a str) -> &'a str {
    match reason_code {
        "mcp.policy_denied" => policy_reason,
        "mcp.approval_required" => {
            "operator approval is required before this MCP utility operation may execute"
        }
        "mcp.approval_id_required" => "operator approval must include a bounded approval id",
        _ => "MCP utility operation denied by host policy",
    }
}

fn filter_mcp_resources(
    manifest: &McpServerManifest,
    discovered: &[McpDiscoveredResource],
) -> (Vec<Value>, Vec<McpFilteredTool>, bool) {
    let mut resources = Vec::new();
    let mut filtered = Vec::new();
    let mut protocol_violation = false;
    for resource in discovered {
        let Some(manifest_resource) =
            manifest_resource(manifest.resources.as_slice(), &resource.uri)
        else {
            filtered.push(McpFilteredTool {
                raw_name: resource.uri.clone(),
                reason_code: "mcp.resource_not_manifested".to_owned(),
                message: "resource is not declared by the host-reviewed manifest".to_owned(),
            });
            continue;
        };
        if resource.schema_hash != manifest_resource.schema_hash {
            protocol_violation = true;
            filtered.push(McpFilteredTool {
                raw_name: resource.uri.clone(),
                reason_code: "mcp.schema_hash_mismatch".to_owned(),
                message: "resource schema hash does not match the host-reviewed manifest"
                    .to_owned(),
            });
            continue;
        }
        if let Some(host) =
            resource.egress_host.as_deref().or(manifest_resource.egress_host.as_deref())
        {
            if !host_allowed_by_manifest(host, manifest.egress_allowlist.as_slice()) {
                protocol_violation = true;
                filtered.push(McpFilteredTool {
                    raw_name: resource.uri.clone(),
                    reason_code: "mcp.egress_denied".to_owned(),
                    message: "resource egress host is outside the manifest allowlist".to_owned(),
                });
                continue;
            }
        }
        if resource.size_bytes.is_some_and(|size| size > manifest_resource.max_read_bytes) {
            filtered.push(McpFilteredTool {
                raw_name: resource.uri.clone(),
                reason_code: "mcp.resource_too_large".to_owned(),
                message: "resource exceeds the manifest read cap".to_owned(),
            });
            continue;
        }
        resources.push(json!({
            "uri": resource.uri.as_str(),
            "name": if resource.name.is_empty() { manifest_resource.name.as_str() } else { resource.name.as_str() },
            "description": if resource.description.is_empty() {
                manifest_resource.description.as_str()
            } else {
                resource.description.as_str()
            },
            "mime_type": if resource.mime_type.is_empty() {
                manifest_resource.mime_type.as_str()
            } else {
                resource.mime_type.as_str()
            },
            "schema_hash": resource.schema_hash.as_str(),
            "size_bytes": resource.size_bytes,
            "max_read_bytes": manifest_resource.max_read_bytes,
            "approval_required": resource_requires_approval(manifest_resource),
            "sensitivity": manifest_resource.sensitivity.as_str(),
        }));
    }
    resources.sort_by(|left, right| {
        left.get("uri")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .cmp(right.get("uri").and_then(Value::as_str).unwrap_or_default())
    });
    filtered.sort_by(|left, right| left.raw_name.cmp(&right.raw_name));
    (resources, filtered, protocol_violation)
}

fn filter_mcp_prompts(
    manifest: &McpServerManifest,
    discovered: &[McpDiscoveredPrompt],
) -> (Vec<Value>, Vec<McpFilteredTool>, bool) {
    let mut prompts = Vec::new();
    let mut filtered = Vec::new();
    let mut protocol_violation = false;
    for prompt in discovered {
        let Some(manifest_prompt) = manifest_prompt(manifest.prompts.as_slice(), &prompt.name)
        else {
            filtered.push(McpFilteredTool {
                raw_name: prompt.name.clone(),
                reason_code: "mcp.prompt_not_manifested".to_owned(),
                message: "prompt is not declared by the host-reviewed manifest".to_owned(),
            });
            continue;
        };
        if prompt.schema_hash != manifest_prompt.schema_hash {
            protocol_violation = true;
            filtered.push(McpFilteredTool {
                raw_name: prompt.name.clone(),
                reason_code: "mcp.schema_hash_mismatch".to_owned(),
                message: "prompt schema hash does not match the host-reviewed manifest".to_owned(),
            });
            continue;
        }
        prompts.push(json!({
            "name": prompt.name.as_str(),
            "description": if prompt.description.is_empty() {
                manifest_prompt.description.as_str()
            } else {
                prompt.description.as_str()
            },
            "schema_hash": prompt.schema_hash.as_str(),
            "argument_schema_hash": stable_hash_value(&prompt.argument_schema),
            "max_prompt_bytes": manifest_prompt.max_prompt_bytes,
            "approval_required": prompt_requires_approval(manifest_prompt),
            "sensitivity": manifest_prompt.sensitivity.as_str(),
        }));
    }
    prompts.sort_by(|left, right| {
        left.get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .cmp(right.get("name").and_then(Value::as_str).unwrap_or_default())
    });
    filtered.sort_by(|left, right| left.raw_name.cmp(&right.raw_name));
    (prompts, filtered, protocol_violation)
}

fn sanitize_mcp_resource_payload(payload: &McpResourceReadPayload) -> (Value, Vec<String>) {
    let raw = serde_json::to_string(&payload.content).unwrap_or_else(|_| "null".to_owned());
    let transform = transform_text_for_prompt(
        raw.as_str(),
        SafetySourceKind::ToolOutput,
        SafetyContentKind::PlainText,
        TrustLabel::ExternalUntrusted,
    );
    let safety_findings = transform.scan.finding_codes();
    (
        json!({
            "schema_version": MCP_SCHEMA_VERSION,
            "uri": payload.uri.as_str(),
            "mime_type": payload.mime_type.as_str(),
            "content_sha256": stable_hash_value(&payload.content),
            "content": transform.transformed_text,
            "trust_label": TrustLabel::ExternalUntrusted.as_str(),
            "safety_action": transform.scan.recommended_action.as_str(),
            "safety_findings": safety_findings.clone(),
        }),
        safety_findings,
    )
}

fn sanitize_mcp_prompt_payload(payload: &McpPromptPayload) -> (Value, Vec<String>) {
    let raw = serde_json::to_string(&payload.messages).unwrap_or_else(|_| "null".to_owned());
    let transform = transform_text_for_prompt(
        raw.as_str(),
        SafetySourceKind::ToolOutput,
        SafetyContentKind::PlainText,
        TrustLabel::ExternalUntrusted,
    );
    let safety_findings = transform.scan.finding_codes();
    (
        json!({
            "schema_version": MCP_SCHEMA_VERSION,
            "name": payload.name.as_str(),
            "messages_sha256": stable_hash_value(&payload.messages),
            "messages": transform.transformed_text,
            "trust_label": TrustLabel::ExternalUntrusted.as_str(),
            "safety_action": transform.scan.recommended_action.as_str(),
            "safety_findings": safety_findings.clone(),
        }),
        safety_findings,
    )
}

#[allow(clippy::too_many_arguments)]
fn allowed_utility_outcome(
    manifest: &McpServerManifest,
    server_id: &str,
    operation: &str,
    target: &str,
    schema_hash: Option<&str>,
    input_json: &Value,
    output_json: Value,
    approval_id: Option<&str>,
    prompt_cache_epoch: u64,
    safety_findings: Vec<String>,
    output_truncated: bool,
    artifact_required: bool,
) -> McpUtilityOutcome {
    McpUtilityOutcome {
        success: true,
        error: None,
        audit: utility_audit_record(
            manifest,
            server_id,
            operation,
            target,
            schema_hash,
            input_json,
            &output_json,
            "allowed",
            approval_id,
            prompt_cache_epoch,
            output_truncated,
            artifact_required,
            safety_findings,
        ),
        output_json,
    }
}

#[allow(clippy::too_many_arguments)]
fn denied_utility_outcome(
    manifest: &McpServerManifest,
    server_id: &str,
    operation: &str,
    target: &str,
    schema_hash: Option<&str>,
    input_json: &Value,
    reason_code: &str,
    message: &str,
    approval_id: Option<&str>,
    prompt_cache_epoch: u64,
) -> McpUtilityOutcome {
    let output_json = json!({
        "success": false,
        "reason_code": reason_code,
        "message": sanitize_mcp_transport_message(message),
    });
    let audit = utility_audit_record(
        manifest,
        server_id,
        operation,
        target,
        schema_hash,
        input_json,
        &output_json,
        reason_code,
        approval_id,
        prompt_cache_epoch,
        false,
        false,
        Vec::new(),
    );
    McpUtilityOutcome {
        success: false,
        output_json,
        error: Some(sanitize_mcp_transport_message(message)),
        audit,
    }
}

#[allow(clippy::too_many_arguments)]
fn utility_audit_record(
    manifest: &McpServerManifest,
    server_id: &str,
    operation: &str,
    target: &str,
    schema_hash: Option<&str>,
    input_json: &Value,
    output_json: &Value,
    policy_outcome: &str,
    approval_id: Option<&str>,
    prompt_cache_epoch: u64,
    output_truncated: bool,
    artifact_required: bool,
    mut safety_findings: Vec<String>,
) -> McpUtilityAuditRecord {
    let executed_at_unix_ms = current_unix_ms();
    let input_hash = stable_hash_value(input_json);
    let output_hash = stable_hash_value(output_json);
    safety_findings.sort();
    safety_findings.dedup();
    let seed = format!(
        "{server_id}:{operation}:{target}:{}:{input_hash}:{output_hash}:{policy_outcome}:{executed_at_unix_ms}",
        schema_hash.unwrap_or_default()
    );
    McpUtilityAuditRecord {
        schema_version: MCP_UTILITY_AUDIT_SCHEMA_VERSION,
        audit_id: format!("mcputil_{}", &stable_hash_bytes(seed.as_bytes())[..16]),
        server_id: server_id.to_owned(),
        server_name: manifest.name.clone(),
        operation: operation.to_owned(),
        target: target.to_owned(),
        schema_hash: schema_hash.map(ToOwned::to_owned),
        input_hash,
        output_hash,
        policy_outcome: policy_outcome.to_owned(),
        approval_id: approval_id.map(ToOwned::to_owned),
        prompt_cache_epoch,
        output_truncated,
        artifact_required,
        safety_findings,
        executed_at_unix_ms,
    }
}

fn denied_sampling_outcome(
    request: &McpSamplingCreateMessageRequest,
    host_policy: &McpHostSamplingPolicy,
    reason_code: &str,
    message: &str,
) -> McpSamplingOutcome {
    denied_sampling_outcome_with_findings(request, host_policy, reason_code, message, Vec::new())
}

fn denied_sampling_outcome_with_findings(
    request: &McpSamplingCreateMessageRequest,
    host_policy: &McpHostSamplingPolicy,
    reason_code: &str,
    message: &str,
    mut safety_findings: Vec<String>,
) -> McpSamplingOutcome {
    safety_findings.sort();
    safety_findings.dedup();
    let output_text = format!("MCP sampling denied: {}", sanitize_mcp_transport_message(message));
    McpSamplingOutcome {
        schema_version: MCP_SAMPLING_SCHEMA_VERSION,
        success: false,
        status: "denied".to_owned(),
        reason_code: reason_code.to_owned(),
        provider_id: host_policy.provider_id.clone(),
        model_id: host_policy.model_id.clone(),
        requested_model_capability: request.requested_model_capability.clone(),
        max_output_tokens: 0,
        prompt_hash: stable_hash_bytes(request.prompt.as_bytes()),
        redacted_prompt_preview: truncate_at_char_boundary(
            sanitize_mcp_transport_message(request.prompt.as_str()).as_str(),
            256,
        ),
        output_hash: stable_hash_bytes(output_text.as_bytes()),
        output_text,
        safety_findings,
    }
}

pub(super) fn valid_elicitation_schema(schema: &Value) -> bool {
    schema.get("type").and_then(Value::as_str) == Some("object")
        && schema.get("properties").is_some_and(Value::is_object)
}

#[allow(clippy::too_many_arguments)]
fn elicitation_outcome(
    request: &McpElicitationRequest,
    policy: &McpElicitationPolicy,
    approval_required: bool,
    status: &str,
    reason_code: &str,
    prompt_hash: String,
    purpose_hash: String,
    schema_hash: String,
    response: Value,
) -> McpElicitationOutcome {
    let response_bytes = serde_json::to_vec(&response).unwrap_or_default().len();
    McpElicitationOutcome {
        schema_version: MCP_ELICITATION_SCHEMA_VERSION,
        success: status == "allowed",
        status: status.to_owned(),
        reason_code: reason_code.to_owned(),
        route: policy.route,
        purpose_hash,
        prompt_hash,
        schema_hash,
        response_hash: stable_hash_value(&response),
        response_bytes,
        data_sensitivity: request.data_sensitivity,
        approval_required,
    }
}
