//! MCP tool invocation wire contracts and audit-safe transport projections.
//!
//! These types contain no transport or tape side effects; the broker executes calls,
//! while the owning run assigns sequence numbers and persists projected evidence.

use palyra_common::qa_runtime_path::{
    McpTransportInvocationEvent, McpTransportInvocationMode, MCP_TRANSPORT_INVOCATION_EVENT,
    MCP_TRANSPORT_INVOCATION_EVENT_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Result of a transport invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpToolResponse {
    pub output: Value,
    #[serde(default)]
    pub sampling_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_model_capability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_host_requested: Option<String>,
}

/// Invocation policy decision supplied by the host before any transport call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpInvocationPolicyDecision {
    pub allowed: bool,
    pub approval_required: bool,
    pub reason: String,
}

/// Scoped, host-issued grant for one logical MCP vault reference.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpVaultScopedGrant {
    pub name: String,
    pub grant_id: String,
}

/// Broker input for one MCP tool call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpToolCallRequest {
    pub server_name: String,
    pub tool_name: String,
    pub input: Value,
    pub schema_hash: String,
    pub policy: McpInvocationPolicyDecision,
    #[serde(default)]
    pub approval_granted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default)]
    pub vault_refs_requested: Vec<String>,
    #[serde(default)]
    pub vault_scoped_grants: Vec<McpVaultScopedGrant>,
}

/// Hash-anchored audit record for an MCP invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpInvocationAttestation {
    pub attestation_id: String,
    pub server_id: String,
    pub server_name: String,
    pub tool_name: String,
    pub namespaced_tool_id: String,
    pub schema_hash: String,
    pub input_hash: String,
    pub output_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_grant_id: Option<String>,
    pub transport_id: String,
    /// Connection lifecycle reported only when the transport boundary was crossed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_mode: Option<McpTransportInvocationMode>,
    pub result_projection: String,
    pub policy_outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_model_capability: Option<String>,
    pub executed_at_unix_ms: i64,
    pub output_truncated: bool,
    #[serde(default)]
    pub vault_grant_ids: Vec<String>,
}

impl McpInvocationAttestation {
    /// Projects a transport-crossing tools/call attestation into the canonical QA tape payload.
    ///
    /// Policy denials that happen before transport return `None`; callers must
    /// not emit a transport invocation event for those outcomes. This pure
    /// projection does not append to a run tape: the run owner assigns the tape
    /// sequence and persists the returned payload exactly once.
    #[must_use]
    pub fn transport_invocation_event(&self) -> Option<McpTransportInvocationEvent> {
        self.transport_mode.map(|transport_mode| McpTransportInvocationEvent {
            schema_version: MCP_TRANSPORT_INVOCATION_EVENT_SCHEMA_VERSION,
            event_name: MCP_TRANSPORT_INVOCATION_EVENT.to_owned(),
            attestation_id: self.attestation_id.clone(),
            transport_id: self.transport_id.clone(),
            namespaced_tool_id: self.namespaced_tool_id.clone(),
            transport_mode,
        })
    }
}

/// Final broker outcome for an MCP tool call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpToolInvocationOutcome {
    pub success: bool,
    pub output_json: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub attestation: McpInvocationAttestation,
}

impl McpToolInvocationOutcome {
    /// Projects this broker outcome into metadata-only transport evidence.
    ///
    /// Returns `None` when policy or validation denied the call before the
    /// transport boundary. Persistence and tape sequencing remain owned by the
    /// run that requested the invocation.
    #[must_use]
    pub fn transport_invocation_event(&self) -> Option<McpTransportInvocationEvent> {
        self.attestation.transport_invocation_event()
    }
}
