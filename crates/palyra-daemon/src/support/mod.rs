//! Daemon support-bundle and shutdown forensic contracts.
//!
//! The helpers in this module produce redacted, machine-readable support
//! snapshots from already-collected runtime state. They do not perform process
//! I/O directly; callers own the actual cleanup actions.

use palyra_common::redaction::{
    is_sensitive_key, redact_diagnostic_text, redact_internal_runtime_paths,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const SHUTDOWN_FORENSIC_SCHEMA_VERSION: u32 = 1;
pub(crate) const SUPPORT_RUNTIME_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Input values collected immediately before controlled daemon shutdown.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShutdownForensicInput {
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) active_sessions: u64,
    pub(crate) active_runs: u64,
    pub(crate) queue_depth: u64,
    pub(crate) pending_approvals: u64,
    pub(crate) provider_lease_state: Value,
    pub(crate) active_tool_jobs: Value,
    pub(crate) child_process_tree: Value,
    pub(crate) mcp_state: Value,
    pub(crate) worker_leases: Value,
    pub(crate) recent_runtime_errors: Vec<String>,
}

/// Redacted shutdown forensic payload persisted into journal/support bundles.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShutdownForensicSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) redaction_level: String,
    pub(crate) active_sessions: u64,
    pub(crate) active_runs: u64,
    pub(crate) queue_depth: u64,
    pub(crate) pending_approvals: u64,
    pub(crate) provider_lease_state: Value,
    pub(crate) active_tool_jobs: Value,
    pub(crate) child_process_tree: Value,
    pub(crate) mcp_state: Value,
    pub(crate) worker_leases: Value,
    pub(crate) recent_runtime_errors: Vec<String>,
    pub(crate) cleanup_strategy: Vec<String>,
    pub(crate) cleanup_status: String,
}

/// Builds the redacted shutdown snapshot. Callers may persist the returned
/// value before starting actual process cleanup.
pub(crate) fn build_shutdown_forensic_snapshot(
    input: ShutdownForensicInput,
) -> ShutdownForensicSnapshot {
    let mut provider_lease_state = input.provider_lease_state;
    let mut active_tool_jobs = input.active_tool_jobs;
    let mut child_process_tree = input.child_process_tree;
    let mut mcp_state = input.mcp_state;
    let mut worker_leases = input.worker_leases;
    for value in [
        &mut provider_lease_state,
        &mut active_tool_jobs,
        &mut child_process_tree,
        &mut mcp_state,
        &mut worker_leases,
    ] {
        redact_support_value(value, None);
    }
    let recent_runtime_errors = input
        .recent_runtime_errors
        .into_iter()
        .map(|error| sanitize_support_text(error.as_str(), Some("error")))
        .collect::<Vec<_>>();
    ShutdownForensicSnapshot {
        schema_version: SHUTDOWN_FORENSIC_SCHEMA_VERSION,
        generated_at_unix_ms: input.generated_at_unix_ms,
        redaction_level: "support_bundle_strict".to_owned(),
        active_sessions: input.active_sessions,
        active_runs: input.active_runs,
        queue_depth: input.queue_depth,
        pending_approvals: input.pending_approvals,
        provider_lease_state,
        active_tool_jobs,
        child_process_tree,
        mcp_state,
        worker_leases,
        recent_runtime_errors,
        cleanup_strategy: platform_cleanup_strategy(),
        cleanup_status: "snapshot_recorded_cleanup_delegated_to_runtime".to_owned(),
    }
}

/// Produces a compact runtime support snapshot from already redacted inputs.
pub(crate) fn build_support_runtime_snapshot(
    generated_at_unix_ms: i64,
    enabled_modules: Vec<String>,
    runtime: Value,
) -> Value {
    let mut runtime = runtime;
    redact_support_value(&mut runtime, None);
    json!({
        "schema_version": SUPPORT_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
        "generated_at_unix_ms": generated_at_unix_ms,
        "enabled_modules": enabled_modules,
        "runtime": runtime,
        "redaction": {
            "level": "support_bundle_strict",
            "raw_provider_payloads": false,
            "raw_oauth_grants": false,
            "raw_vault_refs": false,
            "raw_paths": false,
        },
    })
}

fn platform_cleanup_strategy() -> Vec<String> {
    if cfg!(target_os = "linux") {
        return vec![
            "send graceful cancellation to tracked sandbox/tool processes".to_owned(),
            "escalate via process group or systemd cgroup when available".to_owned(),
            "remove temporary artifact directories only after manifest accounting".to_owned(),
        ];
    }
    if cfg!(target_os = "windows") {
        return vec![
            "send graceful cancellation to tracked sandbox/tool processes".to_owned(),
            "escalate through Windows job objects when the process was registered there".to_owned(),
            "fall back to per-process handles for legacy child processes".to_owned(),
        ];
    }
    vec![
        "send graceful cancellation to tracked sandbox/tool processes".to_owned(),
        "escalate with platform process handles when available".to_owned(),
        "leave cleanup failure visible in the forensic snapshot".to_owned(),
    ]
}

fn redact_support_value(value: &mut Value, key_context: Option<&str>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object.iter_mut() {
                redact_support_value(child, Some(key.as_str()));
            }
        }
        Value::Array(items) => {
            for child in items {
                redact_support_value(child, key_context);
            }
        }
        Value::String(raw) => {
            *raw = sanitize_support_text(raw.as_str(), key_context);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn sanitize_support_text(raw: &str, key_context: Option<&str>) -> String {
    if key_context.is_some_and(is_sensitive_key) {
        return "<redacted>".to_owned();
    }
    let redacted = redact_diagnostic_text(raw);
    let redacted = redact_internal_runtime_paths(redacted.as_str());
    if redacted.contains("vault://") || redacted.contains("vault:") {
        return "<vault_ref:redacted>".to_owned();
    }
    redact_absolute_path_tokens(redacted.as_str())
}

fn redact_absolute_path_tokens(raw: &str) -> String {
    raw.split_whitespace()
        .map(|token| {
            if looks_like_absolute_path(
                token.trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == ','),
            ) {
                "<path:redacted>".to_owned()
            } else {
                token.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn looks_like_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/'))
        || value.starts_with('/')
        || value.starts_with("\\\\")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_forensics_redacts_secrets_and_paths() {
        let snapshot = build_shutdown_forensic_snapshot(ShutdownForensicInput {
            generated_at_unix_ms: 1_730_000_000_000,
            active_sessions: 2,
            active_runs: 1,
            queue_depth: 3,
            pending_approvals: 1,
            provider_lease_state: json!({
                "provider": "openai",
                "authorization": "Bearer raw-secret",
            }),
            active_tool_jobs: json!([
                {"job_id": "job-1", "path": "C:\\Users\\Palo\\secret\\tool.exe"}
            ]),
            child_process_tree: json!({
                "pid": 42,
                "command": "tool --token=abc123",
            }),
            mcp_state: json!({"state": "running"}),
            worker_leases: json!({"vault_ref": "vault://provider/openai"}),
            recent_runtime_errors: vec![
                "failed with token=abc123 at C:\\Users\\Palo\\secret\\log.txt".to_owned(),
            ],
        });

        let encoded = serde_json::to_string(&snapshot).expect("snapshot should serialize");
        assert!(encoded.contains("support_bundle_strict"));
        assert!(!encoded.contains("raw-secret"));
        assert!(!encoded.contains("abc123"));
        assert!(!encoded.contains("Palo"));
        assert!(!encoded.contains("vault://provider/openai"));
        assert!(!snapshot.cleanup_strategy.is_empty());
    }

    #[test]
    fn support_runtime_snapshot_redacts_runtime_payload() {
        let snapshot = build_support_runtime_snapshot(
            1_730_000_000_000,
            vec!["daemon".to_owned()],
            json!({
                "oauth": {"refresh_token": "raw-refresh"},
                "path": "/home/palo/.palyra/state",
            }),
        );
        let encoded = serde_json::to_string(&snapshot).expect("snapshot should serialize");
        assert!(encoded.contains("support_bundle_strict"));
        assert!(snapshot.get("config_hash_sha256").is_none());
        assert!(!encoded.contains("raw-refresh"));
        assert!(!encoded.contains("/home/palo"));
    }
}
