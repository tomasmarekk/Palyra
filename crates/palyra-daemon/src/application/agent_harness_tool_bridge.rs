//! Host-owned validation boundary for tool calls proposed by an agent harness.
//!
//! This module deliberately stops before executor dispatch. It normalizes the
//! policy decision that later runtime wiring must feed into the existing
//! catalog, approval, execution-gate, and projection paths.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Tool call proposed by a harness against a catalog snapshot it observed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessToolCallRequest {
    pub harness_id: String,
    pub run_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub raw_args: Value,
    pub catalog_snapshot_id: String,
    pub replay_metadata: HarnessToolReplayMetadata,
    pub mutating: bool,
}

/// Replay metadata carried with a harness tool request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessToolReplayMetadata {
    pub replay_safe: bool,
    pub tool_surface_hash: String,
}

/// Host policy inputs required before a harness tool call can proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessToolBridgePolicy {
    visible_tools: BTreeSet<String>,
    pub catalog_snapshot_id: String,
    pub approval_required_for_mutation: bool,
    pub execution_gate_required: bool,
}

impl HarnessToolBridgePolicy {
    /// Builds policy from the exact tool names visible in a catalog snapshot.
    #[must_use]
    pub fn new(
        visible_tools: impl IntoIterator<Item = impl Into<String>>,
        catalog_snapshot_id: impl Into<String>,
    ) -> Self {
        Self {
            visible_tools: visible_tools.into_iter().map(Into::into).collect(),
            catalog_snapshot_id: catalog_snapshot_id.into(),
            approval_required_for_mutation: true,
            execution_gate_required: true,
        }
    }
}

/// Safe decision emitted before any executor side effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessToolBridgeDecision {
    pub allowed: bool,
    pub reason_code: String,
    pub normalized_tool_name: String,
    pub approval_required: bool,
    pub execution_gate_required: bool,
    pub harness_visible_result: Option<HarnessVisibleToolResult>,
}

/// Redacted tool result that can be returned to a harness without model-visible escalation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessVisibleToolResult {
    pub status: String,
    pub summary: String,
}

/// Bridge validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HarnessToolBridgeError {
    #[error("tool name is empty")]
    EmptyToolName,
    #[error("tool call id is empty")]
    EmptyToolCallId,
    #[error("catalog snapshot mismatch")]
    CatalogSnapshotMismatch,
}

/// Evaluates a harness tool call against the host-owned policy boundary.
///
/// # Errors
/// Returns [`HarnessToolBridgeError`] for malformed request metadata that
/// cannot be safely turned into a denied tool result.
pub fn evaluate_harness_tool_call(
    request: &HarnessToolCallRequest,
    policy: &HarnessToolBridgePolicy,
) -> Result<HarnessToolBridgeDecision, HarnessToolBridgeError> {
    let normalized_tool_name = request.tool_name.trim().to_owned();
    if normalized_tool_name.is_empty() {
        return Err(HarnessToolBridgeError::EmptyToolName);
    }
    if request.tool_call_id.trim().is_empty() {
        return Err(HarnessToolBridgeError::EmptyToolCallId);
    }
    if request.catalog_snapshot_id != policy.catalog_snapshot_id {
        return Err(HarnessToolBridgeError::CatalogSnapshotMismatch);
    }
    if !policy.visible_tools.contains(normalized_tool_name.as_str()) {
        return Ok(denied_decision(normalized_tool_name, "harness_tool.not_in_catalog_snapshot"));
    }
    if !policy.execution_gate_required {
        return Ok(denied_decision(normalized_tool_name, "harness_tool.execution_gate_required"));
    }

    let approval_required = request.mutating && policy.approval_required_for_mutation;
    if request.mutating && !approval_required {
        return Ok(denied_decision(
            normalized_tool_name,
            "harness_tool.mutating_approval_required",
        ));
    }

    Ok(HarnessToolBridgeDecision {
        allowed: true,
        reason_code: "harness_tool.allowed_for_host_execution".to_owned(),
        normalized_tool_name,
        approval_required,
        execution_gate_required: true,
        harness_visible_result: None,
    })
}

fn denied_decision(normalized_tool_name: String, reason_code: &str) -> HarnessToolBridgeDecision {
    HarnessToolBridgeDecision {
        allowed: false,
        reason_code: reason_code.to_owned(),
        normalized_tool_name,
        approval_required: false,
        execution_gate_required: true,
        harness_visible_result: Some(HarnessVisibleToolResult {
            status: "denied".to_owned(),
            summary: "Tool call was denied by host policy before execution.".to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(tool_name: &str, mutating: bool) -> HarnessToolCallRequest {
        HarnessToolCallRequest {
            harness_id: "embedded_palyra".to_owned(),
            run_id: "run-1".to_owned(),
            tool_call_id: "call-1".to_owned(),
            tool_name: tool_name.to_owned(),
            raw_args: json!({"path":"README.md"}),
            catalog_snapshot_id: "catalog-1".to_owned(),
            replay_metadata: HarnessToolReplayMetadata {
                replay_safe: true,
                tool_surface_hash: "sha256:abc".to_owned(),
            },
            mutating,
        }
    }

    #[test]
    fn bridge_allows_read_only_tool_from_snapshot_through_host_gate() {
        let policy = HarnessToolBridgePolicy::new(["palyra.fs.read_file"], "catalog-1");

        let decision =
            evaluate_harness_tool_call(&request(" palyra.fs.read_file ", false), &policy)
                .expect("read-only call should evaluate");

        assert!(decision.allowed);
        assert_eq!(decision.normalized_tool_name, "palyra.fs.read_file");
        assert!(decision.execution_gate_required);
        assert!(!decision.approval_required);
    }

    #[test]
    fn bridge_denies_tool_not_visible_in_snapshot() {
        let policy = HarnessToolBridgePolicy::new(["palyra.fs.read_file"], "catalog-1");

        let decision = evaluate_harness_tool_call(&request("palyra.process.run", false), &policy)
            .expect("unknown tool should become denied result");

        assert!(!decision.allowed);
        assert_eq!(decision.reason_code, "harness_tool.not_in_catalog_snapshot");
        assert!(decision.harness_visible_result.is_some());
    }

    #[test]
    fn bridge_denies_when_execution_gate_is_not_required() {
        let mut policy = HarnessToolBridgePolicy::new(["palyra.fs.apply_patch"], "catalog-1");
        policy.execution_gate_required = false;

        let decision = evaluate_harness_tool_call(&request("palyra.fs.apply_patch", true), &policy)
            .expect("mutating call should evaluate");

        assert!(!decision.allowed);
        assert_eq!(decision.reason_code, "harness_tool.execution_gate_required");
    }

    #[test]
    fn bridge_requires_snapshot_id_match() {
        let policy = HarnessToolBridgePolicy::new(["palyra.fs.read_file"], "other-catalog");

        let error = evaluate_harness_tool_call(&request("palyra.fs.read_file", false), &policy)
            .expect_err("snapshot mismatch should fail before projection");

        assert_eq!(error, HarnessToolBridgeError::CatalogSnapshotMismatch);
    }
}
