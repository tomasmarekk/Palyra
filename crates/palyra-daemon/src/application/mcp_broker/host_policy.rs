//! MCP host policy validation and manifest-gated helper functions.

use super::host_surfaces::valid_elicitation_schema;
use super::*;

pub(super) fn validate_sampling_policy(
    policy: &McpSamplingPolicy,
    findings: &mut Vec<McpValidationFinding>,
) {
    match policy.mode {
        McpSamplingMode::Deny => {
            if !policy.allowed_model_capabilities.is_empty() {
                findings.push(finding(
                    McpFindingSeverity::Error,
                    "mcp.sampling_allowlist_unused",
                    "sampling allowlist entries cannot be set while sampling mode is deny",
                    "remove allowed_model_capabilities or set sampling_policy.mode=allowlist",
                ));
            }
        }
        McpSamplingMode::Allowlist => {
            if policy.allowed_model_capabilities.is_empty() {
                findings.push(finding(
                    McpFindingSeverity::Error,
                    "mcp.sampling_allowlist_empty",
                    "sampling allowlist mode requires at least one model capability",
                    "add allowed_model_capabilities for the specific model capability",
                ));
            }
        }
    }
}

pub(super) fn validate_resource_manifests(
    manifest: &McpServerManifest,
    findings: &mut Vec<McpValidationFinding>,
) {
    let mut uris = BTreeSet::new();
    for resource in &manifest.resources {
        let normalized_uri = resource.uri.trim().to_ascii_lowercase();
        if !valid_resource_uri(resource.uri.as_str()) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.resource_uri_invalid",
                "resource URIs must be bounded ASCII URIs with a scheme",
                "use a stable ASCII URI such as docs://guide",
            ));
        }
        if !uris.insert(normalized_uri) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.resource_duplicate",
                "resource URIs must be unique within one MCP manifest",
                "remove or rename duplicate resource descriptors",
            ));
        }
        if resource.name.trim().is_empty() || resource.name.len() > 128 {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.resource_name_invalid",
                "resource names must be non-empty and bounded",
                "set a concise operator-visible resource name",
            ));
        }
        if !valid_mcp_schema_hash_label(resource.schema_hash.as_str()) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.resource_schema_hash_invalid",
                "resource schema_hash must be a bounded ASCII label",
                "pin the host-reviewed resource schema hash",
            ));
        }
        if resource.max_read_bytes == 0 || resource.max_read_bytes > manifest.max_response_bytes {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.resource_read_cap_invalid",
                "resource read caps must be positive and within the server response cap",
                "lower max_read_bytes or raise the reviewed server max_response_bytes",
            ));
        }
        if resource.mime_type.trim().is_empty()
            || resource.mime_type.len() > 128
            || resource.mime_type.chars().any(char::is_control)
        {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.resource_mime_type_invalid",
                "resource MIME type must be a bounded printable label",
                "set a concrete MIME type such as text/plain",
            ));
        }
        if let Some(host) = resource.egress_host.as_deref() {
            if !valid_host(host)
                || !host_allowed_by_manifest(host, manifest.egress_allowlist.as_slice())
            {
                findings.push(finding(
                    McpFindingSeverity::Error,
                    "mcp.resource_egress_host_invalid",
                    "resource egress host must be valid and included in the manifest allowlist",
                    "add the exact host to egress_allowlist or remove the resource egress host",
                ));
            }
        }
    }
}

pub(super) fn validate_prompt_manifests(
    manifest: &McpServerManifest,
    findings: &mut Vec<McpValidationFinding>,
) {
    let mut names = BTreeSet::new();
    for prompt in &manifest.prompts {
        let normalized_name = prompt.name.trim().to_ascii_lowercase();
        if normalize_mcp_identifier(prompt.name.as_str(), "prompt_name").is_err() {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.prompt_name_invalid",
                "prompt names must use non-empty [a-z0-9._-] identifiers",
                "rename the MCP prompt descriptor",
            ));
        }
        if !names.insert(normalized_name) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.prompt_duplicate",
                "prompt names must be unique within one MCP manifest",
                "remove or rename duplicate prompt descriptors",
            ));
        }
        if !valid_mcp_schema_hash_label(prompt.schema_hash.as_str()) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.prompt_schema_hash_invalid",
                "prompt schema_hash must be a bounded ASCII label",
                "pin the host-reviewed prompt schema hash",
            ));
        }
        if prompt.max_prompt_bytes == 0 || prompt.max_prompt_bytes > manifest.max_response_bytes {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.prompt_read_cap_invalid",
                "prompt read caps must be positive and within the server response cap",
                "lower max_prompt_bytes or raise the reviewed server max_response_bytes",
            ));
        }
        if prompt.description.len() > 512 {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.prompt_description_invalid",
                "prompt descriptions must be bounded",
                "shorten the MCP prompt descriptor description",
            ));
        }
        if !valid_elicitation_schema(&prompt.argument_schema) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.prompt_argument_schema_invalid",
                "prompt argument_schema must be an object schema with properties",
                "use a JSON object schema for prompt arguments",
            ));
        }
    }
}

pub(super) fn sampling_allowed_by_manifest(
    manifest: &McpServerManifest,
    requested_model_capability: Option<&str>,
) -> bool {
    if manifest.sampling_enabled {
        return false;
    }
    if !matches!(manifest.sampling_policy.mode, McpSamplingMode::Allowlist) {
        return false;
    }
    let Some(requested) = requested_model_capability
        .and_then(|value| normalize_sampling_model_capability(value).ok())
    else {
        return false;
    };
    manifest
        .sampling_policy
        .allowed_model_capabilities
        .iter()
        .filter_map(|capability| normalize_sampling_model_capability(capability).ok())
        .any(|allowed| allowed == requested)
}

pub(super) fn normalize_sampling_model_capability(raw: &str) -> Result<String, McpBrokerError> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 128
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | ':' | '/'))
    {
        return Err(McpBrokerError::new(
            "mcp.sampling_capability_invalid",
            "sampling model capability must use bounded ASCII label syntax",
        ));
    }
    Ok(normalized)
}

fn valid_resource_uri(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 2048
        && trimmed.contains("://")
        && trimmed == value
        && trimmed
            .chars()
            .all(|ch| ch.is_ascii_graphic() && !matches!(ch, '"' | '\'' | '<' | '>' | '`'))
}
