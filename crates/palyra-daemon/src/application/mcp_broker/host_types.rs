//! MCP host-reviewed manifest and utility DTOs.

use super::*;

/// Host-reviewed resource descriptor a server may expose through utility tools.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpResourceManifest {
    pub uri: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_resource_mime_type")]
    pub mime_type: String,
    pub schema_hash: String,
    #[serde(default = "default_max_resource_read_bytes")]
    pub max_read_bytes: usize,
    #[serde(default)]
    pub sensitivity: McpToolSensitivity,
    #[serde(default)]
    pub approval_policy: McpApprovalPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_host: Option<String>,
}

/// Host-reviewed prompt descriptor a server may expose through utility tools.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpPromptManifest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub schema_hash: String,
    #[serde(default)]
    pub argument_schema: Value,
    #[serde(default = "default_max_prompt_bytes")]
    pub max_prompt_bytes: usize,
    #[serde(default)]
    pub sensitivity: McpToolSensitivity,
    #[serde(default)]
    pub approval_policy: McpApprovalPolicy,
}

/// Host-owned sampling policy for one MCP server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpSamplingPolicy {
    pub mode: McpSamplingMode,
    #[serde(default)]
    pub allowed_model_capabilities: Vec<String>,
}

impl Default for McpSamplingPolicy {
    fn default() -> Self {
        Self { mode: McpSamplingMode::Deny, allowed_model_capabilities: Vec::new() }
    }
}

/// Sampling decision mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum McpSamplingMode {
    #[default]
    Deny,
    Allowlist,
}

/// Resource descriptor discovered from an MCP server and matched to a manifest entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveredResource {
    pub uri: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_resource_mime_type")]
    pub mime_type: String,
    pub schema_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_host: Option<String>,
}

/// Prompt descriptor discovered from an MCP server and matched to a manifest entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveredPrompt {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub schema_hash: String,
    #[serde(default)]
    pub argument_schema: Value,
}

/// Input for listing resources or prompts through host-mediated utility tools.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpUtilityListRequest {
    pub server_name: String,
    pub policy: McpInvocationPolicyDecision,
}

/// Input for reading one MCP resource through the host-mediated utility tool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpResourceReadRequest {
    pub server_name: String,
    pub uri: String,
    pub schema_hash: String,
    pub policy: McpInvocationPolicyDecision,
    #[serde(default)]
    pub approval_granted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
}

/// Input for resolving one MCP prompt through the host-mediated utility tool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpPromptGetRequest {
    pub server_name: String,
    pub name: String,
    pub schema_hash: String,
    #[serde(default)]
    pub arguments: Value,
    pub policy: McpInvocationPolicyDecision,
    #[serde(default)]
    pub approval_granted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
}

/// Resource read payload returned by a transport before projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpResourceReadPayload {
    pub uri: String,
    #[serde(default = "default_resource_mime_type")]
    pub mime_type: String,
    pub content: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_host_requested: Option<String>,
}

/// Prompt payload returned by a transport before projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpPromptPayload {
    pub name: String,
    pub messages: Value,
}

/// Hash-only audit record for MCP utility tool operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpUtilityAuditRecord {
    pub schema_version: u32,
    pub audit_id: String,
    pub server_id: String,
    pub server_name: String,
    pub operation: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
    pub input_hash: String,
    pub output_hash: String,
    pub policy_outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    pub prompt_cache_epoch: u64,
    pub output_truncated: bool,
    pub artifact_required: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub safety_findings: Vec<String>,
    pub executed_at_unix_ms: i64,
}

/// Final host-mediated utility outcome for resource and prompt operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpUtilityOutcome {
    pub success: bool,
    pub output_json: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub audit: McpUtilityAuditRecord,
}

/// Host-owned policy envelope for MCP sampling createMessage requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpHostSamplingPolicy {
    pub allowed_model_capabilities: Vec<String>,
    pub provider_id: String,
    pub model_id: String,
    pub max_output_tokens: u64,
    pub remaining_budget_tokens: u64,
    #[serde(default = "default_max_sampling_prompt_bytes")]
    pub max_prompt_bytes: usize,
}

/// MCP sampling request after transport parsing but before host mediation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpSamplingCreateMessageRequest {
    pub server_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model_capability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model_id: Option<String>,
    pub prompt: String,
    pub max_output_tokens: u64,
    pub policy: McpInvocationPolicyDecision,
}

/// Audit-safe sampling result returned to the MCP transport boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpSamplingOutcome {
    pub schema_version: u32,
    pub success: bool,
    pub status: String,
    pub reason_code: String,
    pub provider_id: String,
    pub model_id: String,
    pub requested_model_capability: Option<String>,
    pub max_output_tokens: u64,
    pub prompt_hash: String,
    pub redacted_prompt_preview: String,
    pub output_text: String,
    pub output_hash: String,
    #[serde(default)]
    pub safety_findings: Vec<String>,
}

/// Host-owned routing decision for an MCP elicitation request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpElicitationRoute {
    User,
    ApprovalQueue,
    Deny,
}

/// Host-owned elicitation policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpElicitationPolicy {
    pub route: McpElicitationRoute,
    pub timeout_ms: u64,
    #[serde(default)]
    pub approval_required_for_sensitive: bool,
    #[serde(default = "default_max_elicitation_schema_bytes")]
    pub max_schema_bytes: usize,
    #[serde(default = "default_max_elicitation_response_bytes")]
    pub max_response_bytes: usize,
}

/// MCP elicitation request after transport parsing but before host mediation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpElicitationRequest {
    pub server_name: String,
    pub prompt: String,
    pub schema: Value,
    pub purpose: String,
    pub data_sensitivity: McpToolSensitivity,
    #[serde(default)]
    pub policy: Option<McpInvocationPolicyDecision>,
}

/// Host-provided elicitation response metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpElicitationHostResponse {
    pub accepted: bool,
    #[serde(default)]
    pub response: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal_reason: Option<String>,
}

/// Audit-safe elicitation result returned to the MCP transport boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpElicitationOutcome {
    pub schema_version: u32,
    pub success: bool,
    pub status: String,
    pub reason_code: String,
    pub route: McpElicitationRoute,
    pub purpose_hash: String,
    pub prompt_hash: String,
    pub schema_hash: String,
    pub response_hash: String,
    pub response_bytes: usize,
    pub data_sensitivity: McpToolSensitivity,
    pub approval_required: bool,
}

pub(super) fn default_max_resource_read_bytes() -> usize {
    DEFAULT_MAX_RESOURCE_READ_BYTES
}

pub(super) fn default_max_prompt_bytes() -> usize {
    DEFAULT_MAX_PROMPT_BYTES
}

pub(super) fn default_max_sampling_prompt_bytes() -> usize {
    DEFAULT_MAX_SAMPLING_PROMPT_BYTES
}

pub(super) fn default_max_elicitation_schema_bytes() -> usize {
    DEFAULT_MAX_ELICITATION_SCHEMA_BYTES
}

pub(super) fn default_max_elicitation_response_bytes() -> usize {
    DEFAULT_MAX_ELICITATION_RESPONSE_BYTES
}

pub(super) fn default_resource_mime_type() -> String {
    "application/octet-stream".to_owned()
}
