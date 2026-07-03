//! Secure Model Context Protocol broker contracts for external tool servers.
//!
//! The broker imports externally-discovered tools only after manifest,
//! namespace, policy, schema, output-size, approval, and vault-reference gates
//! pass. This module intentionally keeps transport side effects behind
//! [`McpTransport`] so catalog import and invocation decisions remain
//! deterministic and testable.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_common::{redaction::redact_diagnostic_text, runtime_preview::RuntimePreviewMode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::{McpServerConfig, McpServerTransport, McpServersConfig};

use super::tool_registry::{
    sanitize_schema_for_provider, stable_hash_bytes, stable_hash_value, ToolApprovalPosture,
    ToolExposureSurface, ToolParallelismPolicy, ToolRegistryEntry, ToolResultProjectionPolicy,
    ToolSchemaDialect,
};

const MCP_SCHEMA_VERSION: u32 = 1;
const MCP_RUNTIME_SUPERVISOR_SCHEMA_VERSION: u32 = 1;
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_START_TIMEOUT_MS: u64 = 2_500;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_IDENTIFIER_LEN: usize = 64;
const QUARANTINE_AFTER_VIOLATIONS: u32 = 3;
const DEFAULT_SUPERVISOR_MAX_RETRIES: u32 = 3;
const DEFAULT_SUPERVISOR_BASE_BACKOFF_MS: i64 = 1_000;
const DEFAULT_SUPERVISOR_MAX_BACKOFF_MS: i64 = 30_000;
const DEFAULT_SUPERVISOR_STDERR_TAIL_BYTES: usize = 4 * 1024;

/// Broker-wide policy that is not trusted from server-authored manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpBrokerPolicy {
    pub allowed_stdio_commands: Vec<String>,
    pub max_timeout_ms: u64,
    pub max_start_timeout_ms: u64,
    pub max_response_bytes: usize,
}

impl Default for McpBrokerPolicy {
    fn default() -> Self {
        Self {
            allowed_stdio_commands: Vec::new(),
            max_timeout_ms: 30_000,
            max_start_timeout_ms: 10_000,
            max_response_bytes: 4 * 1024 * 1024,
        }
    }
}

/// A validated MCP server manifest supplied by operator configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpServerManifest {
    pub name: String,
    pub transport: McpTransportManifest,
    #[serde(default)]
    pub vault_refs: Vec<McpVaultRefGrant>,
    #[serde(default)]
    pub egress_allowlist: Vec<String>,
    #[serde(default)]
    pub tool_allowlist: Vec<String>,
    #[serde(default)]
    pub tool_denylist: Vec<String>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_start_timeout_ms")]
    pub start_timeout_ms: u64,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
    #[serde(default)]
    pub sensitivity_default: McpToolSensitivity,
    #[serde(default)]
    pub approval_policy: McpApprovalPolicy,
    #[serde(default)]
    pub sampling_enabled: bool,
}

/// Transport declaration for an MCP server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpTransportManifest {
    Stdio {
        command: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
    },
    Http {
        url: String,
    },
    Sse {
        url: String,
    },
}

/// Vault reference a server may request by logical name during invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpVaultRefGrant {
    pub name: String,
    pub vault_ref: String,
}

/// Default sensitivity applied to tools discovered from a server.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
#[serde(rename_all = "snake_case")]
pub enum McpToolSensitivity {
    Public,
    #[default]
    Internal,
    Sensitive,
    Secret,
}

impl McpToolSensitivity {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Sensitive => "sensitive",
            Self::Secret => "secret",
        }
    }
}

/// Approval posture applied to discovered tools unless a tool declares a stricter posture.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
#[serde(rename_all = "snake_case")]
pub enum McpApprovalPolicy {
    #[default]
    Safe,
    RequireApproval,
}

impl McpApprovalPolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::RequireApproval => "require_approval",
        }
    }
}

/// Lifecycle state for one configured MCP server.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum McpServerLifecycleState {
    Stopped,
    Starting,
    Healthy,
    Degraded,
    Backoff,
    Disabled,
    Quarantined,
}

impl McpServerLifecycleState {
    /// Returns the canonical snake_case wire label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Starting => "starting",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Backoff => "backoff",
            Self::Disabled => "disabled",
            Self::Quarantined => "quarantined",
        }
    }
}

/// Host-owned lifecycle policy for supervised MCP runtime entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRuntimeSupervisorPolicy {
    pub max_retries: u32,
    pub base_backoff_ms: i64,
    pub max_backoff_ms: i64,
    pub stderr_tail_bytes: usize,
}

impl Default for McpRuntimeSupervisorPolicy {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_SUPERVISOR_MAX_RETRIES,
            base_backoff_ms: DEFAULT_SUPERVISOR_BASE_BACKOFF_MS,
            max_backoff_ms: DEFAULT_SUPERVISOR_MAX_BACKOFF_MS,
            stderr_tail_bytes: DEFAULT_SUPERVISOR_STDERR_TAIL_BYTES,
        }
    }
}

/// Redacted lifecycle snapshot for every configured MCP server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpRuntimeSupervisorSnapshot {
    pub schema_version: u32,
    pub generated_at_unix_ms: i64,
    pub mode: String,
    pub total_servers: usize,
    pub enabled_servers: usize,
    pub healthy_servers: usize,
    pub degraded_servers: usize,
    pub backoff_servers: usize,
    pub quarantined_servers: usize,
    pub disabled_servers: usize,
    pub servers: Vec<McpRuntimeServerSnapshot>,
}

/// Redacted lifecycle snapshot for one configured MCP server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpRuntimeServerSnapshot {
    pub id: String,
    pub namespace: String,
    pub transport: String,
    pub enabled: bool,
    pub state: McpServerLifecycleState,
    pub consecutive_failures: u32,
    pub total_failures: u64,
    pub restart_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr_tail_redacted: Option<String>,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
struct McpRuntimeServerRecord {
    id: String,
    namespace: String,
    transport: McpServerTransport,
    enabled: bool,
    state: McpServerLifecycleState,
    consecutive_failures: u32,
    total_failures: u64,
    restart_count: u64,
    next_retry_at_unix_ms: Option<i64>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
    stderr_tail_redacted: Option<String>,
    updated_at_unix_ms: i64,
}

/// In-memory supervisor for configured MCP server runtime state.
#[derive(Debug, Clone)]
pub struct McpRuntimeSupervisor {
    mode: RuntimePreviewMode,
    policy: McpRuntimeSupervisorPolicy,
    servers: BTreeMap<String, McpRuntimeServerRecord>,
}

impl McpRuntimeSupervisor {
    /// Builds a supervisor from the validated daemon MCP configuration.
    #[must_use]
    pub fn from_config(config: &McpServersConfig) -> Self {
        Self::from_config_with_policy(config, McpRuntimeSupervisorPolicy::default())
    }

    /// Builds a supervisor from config and explicit host-owned lifecycle policy.
    #[must_use]
    pub fn from_config_with_policy(
        config: &McpServersConfig,
        policy: McpRuntimeSupervisorPolicy,
    ) -> Self {
        let servers = config
            .servers
            .iter()
            .map(|server| {
                let record = McpRuntimeServerRecord::from_config(
                    server,
                    config.mode != RuntimePreviewMode::Disabled,
                );
                (record.id.clone(), record)
            })
            .collect();
        Self { mode: config.mode, policy, servers }
    }

    /// Returns a deterministic redacted supervisor snapshot.
    #[must_use]
    pub fn snapshot(&self, generated_at_unix_ms: i64) -> McpRuntimeSupervisorSnapshot {
        let servers =
            self.servers.values().map(McpRuntimeServerRecord::snapshot).collect::<Vec<_>>();
        McpRuntimeSupervisorSnapshot {
            schema_version: MCP_RUNTIME_SUPERVISOR_SCHEMA_VERSION,
            generated_at_unix_ms,
            mode: self.mode.as_str().to_owned(),
            total_servers: servers.len(),
            enabled_servers: servers.iter().filter(|server| server.enabled).count(),
            healthy_servers: servers
                .iter()
                .filter(|server| server.state == McpServerLifecycleState::Healthy)
                .count(),
            degraded_servers: servers
                .iter()
                .filter(|server| server.state == McpServerLifecycleState::Degraded)
                .count(),
            backoff_servers: servers
                .iter()
                .filter(|server| server.state == McpServerLifecycleState::Backoff)
                .count(),
            quarantined_servers: servers
                .iter()
                .filter(|server| server.state == McpServerLifecycleState::Quarantined)
                .count(),
            disabled_servers: servers
                .iter()
                .filter(|server| server.state == McpServerLifecycleState::Disabled)
                .count(),
            servers,
        }
    }

    /// Marks a server as starting if its lifecycle policy allows a start attempt.
    ///
    /// # Errors
    /// Returns an error when the server is unknown, disabled, quarantined, or
    /// still inside its retry backoff window.
    pub fn start_server(
        &mut self,
        server_id: &str,
        now_unix_ms: i64,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let record = self.server_record_mut(server_id)?;
        match record.state {
            McpServerLifecycleState::Disabled => {
                return Err(McpBrokerError::new(
                    "mcp.server_disabled",
                    format!("MCP server '{}' is disabled by configuration", record.id),
                ));
            }
            McpServerLifecycleState::Quarantined => {
                return Err(McpBrokerError::new(
                    "mcp.server_quarantined",
                    format!("MCP server '{}' is quarantined", record.id),
                ));
            }
            McpServerLifecycleState::Backoff => {
                if record.next_retry_at_unix_ms.is_some_and(|retry_at| retry_at > now_unix_ms) {
                    return Err(McpBrokerError::new(
                        "mcp.server_backoff",
                        format!("MCP server '{}' is waiting for retry backoff", record.id),
                    ));
                }
            }
            McpServerLifecycleState::Stopped
            | McpServerLifecycleState::Starting
            | McpServerLifecycleState::Healthy
            | McpServerLifecycleState::Degraded => {}
        }
        record.state = McpServerLifecycleState::Starting;
        record.updated_at_unix_ms = now_unix_ms;
        Ok(record.state)
    }

    /// Records a successful start or reconnect attempt.
    ///
    /// # Errors
    /// Returns an error when `server_id` is not registered.
    pub fn record_start_success(
        &mut self,
        server_id: &str,
        now_unix_ms: i64,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let record = self.server_record_mut(server_id)?;
        record.state = McpServerLifecycleState::Healthy;
        record.consecutive_failures = 0;
        record.next_retry_at_unix_ms = None;
        record.last_error_code = None;
        record.last_error_message = None;
        record.updated_at_unix_ms = now_unix_ms;
        Ok(record.state)
    }

    /// Records a failed start, reconnect, health check, or process exit.
    ///
    /// # Errors
    /// Returns an error when `server_id` is not registered.
    pub fn record_failure(
        &mut self,
        server_id: &str,
        reason_code: &str,
        message: &str,
        stderr: Option<&str>,
        now_unix_ms: i64,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let max_retries = self.policy.max_retries;
        let backoff_ms = self.next_backoff_ms(server_id)?;
        let stderr_tail_bytes = self.policy.stderr_tail_bytes;
        let record = self.server_record_mut(server_id)?;
        append_redacted_stderr(record, stderr, stderr_tail_bytes);
        record.consecutive_failures = record.consecutive_failures.saturating_add(1);
        record.total_failures = record.total_failures.saturating_add(1);
        record.last_error_code = Some(reason_code.trim().to_owned());
        record.last_error_message = Some(redact_diagnostic_text(message));
        record.updated_at_unix_ms = now_unix_ms;
        if record.consecutive_failures >= max_retries {
            record.state = McpServerLifecycleState::Quarantined;
            record.next_retry_at_unix_ms = None;
        } else {
            record.state = McpServerLifecycleState::Backoff;
            record.next_retry_at_unix_ms = Some(now_unix_ms.saturating_add(backoff_ms));
        }
        Ok(record.state)
    }

    /// Records a non-terminal health degradation without scheduling a restart.
    ///
    /// # Errors
    /// Returns an error when `server_id` is not registered.
    pub fn record_degraded(
        &mut self,
        server_id: &str,
        reason_code: &str,
        message: &str,
        now_unix_ms: i64,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let record = self.server_record_mut(server_id)?;
        if !matches!(
            record.state,
            McpServerLifecycleState::Disabled | McpServerLifecycleState::Quarantined
        ) {
            record.state = McpServerLifecycleState::Degraded;
        }
        record.last_error_code = Some(reason_code.trim().to_owned());
        record.last_error_message = Some(redact_diagnostic_text(message));
        record.updated_at_unix_ms = now_unix_ms;
        Ok(record.state)
    }

    /// Appends stderr to the bounded redacted per-server tail.
    ///
    /// # Errors
    /// Returns an error when `server_id` is not registered.
    pub fn record_stderr(
        &mut self,
        server_id: &str,
        stderr: &str,
        now_unix_ms: i64,
    ) -> Result<(), McpBrokerError> {
        let stderr_tail_bytes = self.policy.stderr_tail_bytes;
        let record = self.server_record_mut(server_id)?;
        append_redacted_stderr(record, Some(stderr), stderr_tail_bytes);
        record.updated_at_unix_ms = now_unix_ms;
        Ok(())
    }

    /// Stops a server without clearing its failure evidence.
    ///
    /// # Errors
    /// Returns an error when `server_id` is not registered.
    pub fn stop_server(
        &mut self,
        server_id: &str,
        now_unix_ms: i64,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let record = self.server_record_mut(server_id)?;
        if !matches!(
            record.state,
            McpServerLifecycleState::Disabled | McpServerLifecycleState::Quarantined
        ) {
            record.state = McpServerLifecycleState::Stopped;
            record.next_retry_at_unix_ms = None;
        }
        record.updated_at_unix_ms = now_unix_ms;
        Ok(record.state)
    }

    /// Starts a new lifecycle attempt and increments restart evidence.
    ///
    /// # Errors
    /// Returns an error when [`Self::start_server`] rejects the attempt.
    pub fn restart_server(
        &mut self,
        server_id: &str,
        now_unix_ms: i64,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let state = self.start_server(server_id, now_unix_ms)?;
        let record = self.server_record_mut(server_id)?;
        record.restart_count = record.restart_count.saturating_add(1);
        Ok(state)
    }

    fn server_record_mut(
        &mut self,
        server_id: &str,
    ) -> Result<&mut McpRuntimeServerRecord, McpBrokerError> {
        let normalized = normalize_mcp_identifier(server_id, "server_name")?;
        self.servers.get_mut(normalized.as_str()).ok_or_else(|| {
            McpBrokerError::new(
                "mcp.server_unknown",
                format!("MCP server '{normalized}' is not registered"),
            )
        })
    }

    fn next_backoff_ms(&self, server_id: &str) -> Result<i64, McpBrokerError> {
        let normalized = normalize_mcp_identifier(server_id, "server_name")?;
        let record = self.servers.get(normalized.as_str()).ok_or_else(|| {
            McpBrokerError::new(
                "mcp.server_unknown",
                format!("MCP server '{normalized}' is not registered"),
            )
        })?;
        let exponent = record.consecutive_failures.min(20);
        let exponential = self.policy.base_backoff_ms.saturating_mul(1_i64 << exponent);
        let capped = exponential.min(self.policy.max_backoff_ms).max(1);
        Ok(capped.saturating_add(deterministic_backoff_jitter_ms(record.id.as_str(), exponent)))
    }
}

impl McpRuntimeServerRecord {
    fn from_config(config: &McpServerConfig, global_enabled: bool) -> Self {
        let enabled = global_enabled && config.enabled;
        Self {
            id: config.id.clone(),
            namespace: config.namespace.clone(),
            transport: config.transport,
            enabled,
            state: if enabled {
                McpServerLifecycleState::Stopped
            } else {
                McpServerLifecycleState::Disabled
            },
            consecutive_failures: 0,
            total_failures: 0,
            restart_count: 0,
            next_retry_at_unix_ms: None,
            last_error_code: None,
            last_error_message: None,
            stderr_tail_redacted: None,
            updated_at_unix_ms: 0,
        }
    }

    fn snapshot(&self) -> McpRuntimeServerSnapshot {
        McpRuntimeServerSnapshot {
            id: self.id.clone(),
            namespace: self.namespace.clone(),
            transport: self.transport.as_str().to_owned(),
            enabled: self.enabled,
            state: self.state,
            consecutive_failures: self.consecutive_failures,
            total_failures: self.total_failures,
            restart_count: self.restart_count,
            next_retry_at_unix_ms: self.next_retry_at_unix_ms,
            last_error_code: self.last_error_code.clone(),
            last_error_message: self.last_error_message.clone(),
            stderr_tail_redacted: self.stderr_tail_redacted.clone(),
            updated_at_unix_ms: self.updated_at_unix_ms,
        }
    }
}

/// Severity of a manifest or discovery finding.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum McpFindingSeverity {
    Warning,
    Error,
}

/// One actionable validation finding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpValidationFinding {
    pub severity: McpFindingSeverity,
    pub code: String,
    pub message: String,
    pub fix_hint: String,
}

/// Validation report for one manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpManifestValidationReport {
    pub schema_version: u32,
    pub server_name: String,
    pub valid: bool,
    pub findings: Vec<McpValidationFinding>,
}

/// Transport-neutral description of an MCP tool returned by discovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveredTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub sensitivity: Option<McpToolSensitivity>,
    #[serde(default)]
    pub approval_policy: Option<McpApprovalPolicy>,
}

/// One discovery exclusion with a stable reason code.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpFilteredTool {
    pub raw_name: String,
    pub reason_code: String,
    pub message: String,
}

/// Imported MCP tools ready to feed into the model-visible catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpToolDiscoveryReport {
    pub schema_version: u32,
    pub server_name: String,
    pub state: McpServerLifecycleState,
    pub imported_count: usize,
    pub filtered_tools: Vec<McpFilteredTool>,
    pub(crate) registry_entries: Vec<ToolRegistryEntry>,
}

/// Result of a transport invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpToolResponse {
    pub output: Value,
    #[serde(default)]
    pub sampling_requested: bool,
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
    #[serde(default)]
    pub vault_refs_requested: Vec<String>,
}

/// Hash-anchored audit record for an MCP invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpInvocationAttestation {
    pub attestation_id: String,
    pub server_name: String,
    pub tool_name: String,
    pub namespaced_tool_id: String,
    pub schema_hash: String,
    pub input_hash: String,
    pub output_hash: String,
    pub policy_outcome: String,
    pub executed_at_unix_ms: i64,
    pub output_truncated: bool,
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

/// Side-effect boundary for MCP transports.
pub trait McpTransport {
    fn start(&self, manifest: &McpServerManifest) -> Result<(), McpBrokerError>;

    fn list_tools(
        &self,
        manifest: &McpServerManifest,
    ) -> Result<Vec<McpDiscoveredTool>, McpBrokerError>;

    fn call_tool(
        &self,
        manifest: &McpServerManifest,
        request: &McpToolCallRequest,
    ) -> Result<McpToolResponse, McpBrokerError>;
}

/// Fail-closed broker error with safe operator-facing context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpBrokerError {
    pub reason_code: String,
    pub message: String,
}

impl McpBrokerError {
    fn new(reason_code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { reason_code: reason_code.into(), message: message.into() }
    }
}

impl fmt::Display for McpBrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.reason_code, self.message)
    }
}

impl std::error::Error for McpBrokerError {}

#[derive(Debug, Clone)]
struct McpServerRecord {
    manifest: McpServerManifest,
    state: McpServerLifecycleState,
    protocol_violations: u32,
}

/// In-process MCP broker state.
#[derive(Debug, Clone)]
pub struct McpBroker {
    policy: McpBrokerPolicy,
    servers: BTreeMap<String, McpServerRecord>,
}

impl McpBroker {
    /// Creates an empty broker with host-owned policy.
    pub fn new(policy: McpBrokerPolicy) -> Self {
        Self { policy, servers: BTreeMap::new() }
    }

    /// Validates and registers one server manifest.
    ///
    /// # Errors
    /// Returns an error when the manifest is invalid or collides with an
    /// already-registered normalized server id.
    pub fn register_manifest(
        &mut self,
        manifest: McpServerManifest,
    ) -> Result<McpManifestValidationReport, McpBrokerError> {
        let report = validate_mcp_server_manifest(&manifest, &self.policy);
        if !report.valid {
            return Err(McpBrokerError::new(
                "mcp.manifest_invalid",
                format!("MCP manifest '{}' failed validation", manifest.name),
            ));
        }
        let server_id = normalize_mcp_identifier(manifest.name.as_str(), "server_name")?;
        if self.servers.contains_key(server_id.as_str()) {
            return Err(McpBrokerError::new(
                "mcp.server_collision",
                format!("MCP server '{}' collides after normalization", manifest.name),
            ));
        }
        self.servers.insert(
            server_id,
            McpServerRecord {
                manifest,
                state: McpServerLifecycleState::Stopped,
                protocol_violations: 0,
            },
        );
        Ok(report)
    }

    /// Starts a configured server through the provided transport.
    ///
    /// # Errors
    /// Returns a fail-closed error if the server is unknown, quarantined,
    /// disabled, or the transport start fails.
    pub fn start_server(
        &mut self,
        server_name: &str,
        transport: &dyn McpTransport,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let server_id = normalize_mcp_identifier(server_name, "server_name")?;
        let record = self.server_record_mut(server_id.as_str())?;
        if matches!(
            record.state,
            McpServerLifecycleState::Backoff
                | McpServerLifecycleState::Disabled
                | McpServerLifecycleState::Quarantined
        ) {
            return Err(McpBrokerError::new(
                "mcp.server_unavailable",
                format!("MCP server '{}' is {}", server_name, record.state.as_str()),
            ));
        }
        record.state = McpServerLifecycleState::Starting;
        match transport.start(&record.manifest) {
            Ok(()) => {
                record.state = McpServerLifecycleState::Healthy;
                Ok(record.state)
            }
            Err(error) => {
                record.state = McpServerLifecycleState::Degraded;
                Err(error)
            }
        }
    }

    /// Imports ready-server tools as catalog registry entries.
    ///
    /// # Errors
    /// Returns an error when the server is unknown or not ready. Individual
    /// invalid tools are reported in `filtered_tools` and do not fail the
    /// entire discovery pass.
    pub fn discover_tools(
        &mut self,
        server_name: &str,
        transport: &dyn McpTransport,
    ) -> Result<McpToolDiscoveryReport, McpBrokerError> {
        let server_id = normalize_mcp_identifier(server_name, "server_name")?;
        let record = self.server_record(server_id.as_str())?;
        if record.state != McpServerLifecycleState::Healthy {
            return Err(McpBrokerError::new(
                "mcp.server_not_ready",
                format!("MCP server '{}' is {}", server_name, record.state.as_str()),
            ));
        }
        let tools = transport.list_tools(&record.manifest)?;
        let report = import_discovered_tools(&record.manifest, record.state, tools);
        Ok(report)
    }

    /// Invokes one MCP tool through host policy, approval, vault, and output gates.
    ///
    /// # Errors
    /// Returns an error only for unknown server or malformed request metadata;
    /// policy denials and transport failures are returned as attested outcomes.
    pub fn invoke_tool(
        &mut self,
        request: McpToolCallRequest,
        transport: &dyn McpTransport,
    ) -> Result<McpToolInvocationOutcome, McpBrokerError> {
        let server_id = normalize_mcp_identifier(request.server_name.as_str(), "server_name")?;
        let namespaced_tool_id =
            namespaced_tool_id(request.server_name.as_str(), request.tool_name.as_str())?;
        let record = self.server_record(server_id.as_str())?.clone();
        if record.state != McpServerLifecycleState::Healthy {
            return Ok(denied_invocation(
                &request,
                namespaced_tool_id,
                "mcp.server_not_ready",
                format!("MCP server is {}", record.state.as_str()).as_str(),
            ));
        }
        if !tool_allowed_by_manifest(&record.manifest, request.tool_name.as_str()) {
            return Ok(denied_invocation(
                &request,
                namespaced_tool_id,
                "mcp.tool_not_allowed",
                "tool is not allowed by the MCP server manifest",
            ));
        }
        if !request.policy.allowed {
            return Ok(denied_invocation(
                &request,
                namespaced_tool_id,
                "mcp.policy_denied",
                request.policy.reason.as_str(),
            ));
        }
        if request.policy.approval_required && !request.approval_granted {
            return Ok(denied_invocation(
                &request,
                namespaced_tool_id,
                "mcp.approval_required",
                "operator approval is required before this MCP tool may execute",
            ));
        }
        if !request.input.is_object() {
            return Ok(denied_invocation(
                &request,
                namespaced_tool_id,
                "mcp.input_not_object",
                "MCP tool input must be a JSON object",
            ));
        }
        if let Some(denied) = first_ungranted_vault_ref(
            request.vault_refs_requested.as_slice(),
            record.manifest.vault_refs.as_slice(),
        ) {
            return Ok(denied_invocation(
                &request,
                namespaced_tool_id,
                "mcp.vault_ref_not_granted",
                format!("vault reference '{denied}' is not granted by the MCP manifest").as_str(),
            ));
        }

        let response = match transport.call_tool(&record.manifest, &request) {
            Ok(response) => response,
            Err(error) => {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_invocation(
                    &request,
                    namespaced_tool_id,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                ));
            }
        };
        if response.sampling_requested {
            self.record_protocol_violation(server_id.as_str())?;
            return Ok(denied_invocation(
                &request,
                namespaced_tool_id,
                "mcp.sampling_denied",
                "MCP sampling is denied by default and cannot be requested by tools",
            ));
        }
        if let Some(host) = response.egress_host_requested.as_deref() {
            if !host_allowed_by_manifest(host, record.manifest.egress_allowlist.as_slice()) {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(denied_invocation(
                    &request,
                    namespaced_tool_id,
                    "mcp.egress_denied",
                    "MCP tool attempted egress outside its manifest allowlist",
                ));
            }
        }
        let output_json = match bounded_output(
            &response.output,
            record.manifest.max_response_bytes,
            &request,
            namespaced_tool_id.as_str(),
        ) {
            Ok(output_json) => output_json,
            Err(outcome) => {
                self.record_protocol_violation(server_id.as_str())?;
                return Ok(*outcome);
            }
        };
        Ok(McpToolInvocationOutcome {
            success: true,
            output_json: output_json.clone(),
            error: None,
            attestation: invocation_attestation(
                &request,
                namespaced_tool_id,
                output_json,
                "allowed",
                false,
            ),
        })
    }

    /// Records a protocol violation and quarantines repeated offenders.
    pub fn record_protocol_violation(
        &mut self,
        server_name: &str,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let server_id = normalize_mcp_identifier(server_name, "server_name")?;
        let record = self.server_record_mut(server_id.as_str())?;
        record.protocol_violations = record.protocol_violations.saturating_add(1);
        if record.protocol_violations >= QUARANTINE_AFTER_VIOLATIONS {
            record.state = McpServerLifecycleState::Quarantined;
        } else if record.state == McpServerLifecycleState::Healthy {
            record.state = McpServerLifecycleState::Degraded;
        }
        Ok(record.state)
    }

    pub fn state(&self, server_name: &str) -> Result<McpServerLifecycleState, McpBrokerError> {
        let server_id = normalize_mcp_identifier(server_name, "server_name")?;
        Ok(self.server_record(server_id.as_str())?.state)
    }

    fn server_record(&self, server_id: &str) -> Result<&McpServerRecord, McpBrokerError> {
        self.servers.get(server_id).ok_or_else(|| {
            McpBrokerError::new(
                "mcp.server_unknown",
                format!("MCP server '{server_id}' is not registered"),
            )
        })
    }

    fn server_record_mut(
        &mut self,
        server_id: &str,
    ) -> Result<&mut McpServerRecord, McpBrokerError> {
        self.servers.get_mut(server_id).ok_or_else(|| {
            McpBrokerError::new(
                "mcp.server_unknown",
                format!("MCP server '{server_id}' is not registered"),
            )
        })
    }
}

/// Validates one manifest against host-owned broker policy.
#[must_use]
pub fn validate_mcp_server_manifest(
    manifest: &McpServerManifest,
    policy: &McpBrokerPolicy,
) -> McpManifestValidationReport {
    let mut findings = Vec::new();
    if let Err(error) = normalize_mcp_identifier(manifest.name.as_str(), "server_name") {
        findings.push(finding(
            McpFindingSeverity::Error,
            error.reason_code.as_str(),
            error.message.as_str(),
            "use a non-empty lowercase [a-z0-9._-] server name",
        ));
    }
    validate_transport(&manifest.transport, policy, &mut findings);
    validate_timeouts(manifest, policy, &mut findings);
    validate_vault_refs(manifest.vault_refs.as_slice(), &mut findings);
    validate_tool_filters(manifest, &mut findings);
    validate_egress_allowlist(manifest.egress_allowlist.as_slice(), &mut findings);
    if manifest.sampling_enabled {
        findings.push(finding(
            McpFindingSeverity::Error,
            "mcp.sampling_denied",
            "MCP sampling is denied by default for external servers",
            "remove sampling_enabled or keep it false",
        ));
    }
    let valid = !findings.iter().any(|finding| finding.severity == McpFindingSeverity::Error);
    McpManifestValidationReport {
        schema_version: MCP_SCHEMA_VERSION,
        server_name: manifest.name.clone(),
        valid,
        findings,
    }
}

/// Returns a redacted environment map suitable for diagnostics.
///
/// Only manifest-declared variables are surfaced and every value is replaced;
/// no inherited process environment is copied into the diagnostic view.
#[must_use]
pub fn scrub_stdio_env(manifest: &McpServerManifest) -> BTreeMap<String, String> {
    let McpTransportManifest::Stdio { env, .. } = &manifest.transport else {
        return BTreeMap::new();
    };
    env.keys().map(|key| (key.clone(), "<redacted>".to_owned())).collect()
}

/// Builds a stable model-visible tool name from server and raw tool names.
pub fn namespaced_tool_id(server_name: &str, tool_name: &str) -> Result<String, McpBrokerError> {
    let server = normalize_mcp_identifier(server_name, "server_name")?;
    let tool = normalize_mcp_identifier(tool_name, "tool_name")?;
    Ok(format!("mcp.{server}.{tool}"))
}

fn import_discovered_tools(
    manifest: &McpServerManifest,
    state: McpServerLifecycleState,
    tools: Vec<McpDiscoveredTool>,
) -> McpToolDiscoveryReport {
    let mut entries = Vec::new();
    let mut filtered_tools = Vec::new();
    let mut imported_names = BTreeSet::new();
    for tool in tools {
        let namespaced = match namespaced_tool_id(manifest.name.as_str(), tool.name.as_str()) {
            Ok(value) => value,
            Err(error) => {
                filtered_tools.push(McpFilteredTool {
                    raw_name: tool.name,
                    reason_code: error.reason_code,
                    message: error.message,
                });
                continue;
            }
        };
        if !tool_allowed_by_manifest(manifest, tool.name.as_str()) {
            filtered_tools.push(McpFilteredTool {
                raw_name: tool.name,
                reason_code: "mcp.tool_filtered_by_manifest".to_owned(),
                message: "tool is excluded by allowlist or denylist".to_owned(),
            });
            continue;
        }
        if !imported_names.insert(namespaced.clone()) {
            filtered_tools.push(McpFilteredTool {
                raw_name: tool.name,
                reason_code: "mcp.tool_collision".to_owned(),
                message: "tool collides with another tool after namespacing".to_owned(),
            });
            continue;
        }
        if let Err(error) =
            sanitize_schema_for_provider(&tool.input_schema, ToolSchemaDialect::OpenAiCompatible)
        {
            filtered_tools.push(McpFilteredTool {
                raw_name: tool.name,
                reason_code: error.reason_code,
                message: error.message,
            });
            continue;
        }
        entries.push(registry_entry_from_mcp_tool(manifest, &tool, namespaced));
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    filtered_tools.sort_by(|left, right| left.raw_name.cmp(&right.raw_name));
    McpToolDiscoveryReport {
        schema_version: MCP_SCHEMA_VERSION,
        server_name: manifest.name.clone(),
        state,
        imported_count: entries.len(),
        filtered_tools,
        registry_entries: entries,
    }
}

fn registry_entry_from_mcp_tool(
    manifest: &McpServerManifest,
    tool: &McpDiscoveredTool,
    namespaced_tool_id: String,
) -> ToolRegistryEntry {
    let sensitivity = tool.sensitivity.unwrap_or(manifest.sensitivity_default);
    let approval_policy = tool.approval_policy.unwrap_or(manifest.approval_policy);
    let approval_posture = if approval_policy == McpApprovalPolicy::RequireApproval
        || matches!(sensitivity, McpToolSensitivity::Sensitive | McpToolSensitivity::Secret)
    {
        ToolApprovalPosture::ApprovalRequired
    } else {
        ToolApprovalPosture::Safe
    };
    let projection_policy = match sensitivity {
        McpToolSensitivity::Public | McpToolSensitivity::Internal => {
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact
        }
        McpToolSensitivity::Sensitive | McpToolSensitivity::Secret => {
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact
        }
    };
    let mut capabilities = BTreeSet::from([
        "external".to_owned(),
        "mcp".to_owned(),
        format!("approval:{}", approval_policy.as_str()),
        format!("sensitivity:{}", sensitivity.as_str()),
    ]);
    capabilities.extend(
        tool.capabilities
            .iter()
            .map(|capability| capability.trim().to_owned())
            .filter(|capability| !capability.is_empty()),
    );
    ToolRegistryEntry {
        name: namespaced_tool_id,
        description: tool.description.trim().to_owned(),
        version: 1,
        provenance: format!("mcp:{}", manifest.name),
        schema_hash: stable_hash_value(&tool.input_schema),
        input_schema: tool.input_schema.clone(),
        capabilities: capabilities.into_iter().collect(),
        approval_posture,
        projection_policy,
        parallelism_policy: ToolParallelismPolicy::Exclusive,
        target_surfaces: vec![ToolExposureSurface::RunStream, ToolExposureSurface::RouteMessage],
    }
}

fn validate_transport(
    transport: &McpTransportManifest,
    policy: &McpBrokerPolicy,
    findings: &mut Vec<McpValidationFinding>,
) {
    match transport {
        McpTransportManifest::Stdio { command, env } => {
            if command.is_empty() || command.iter().any(|part| part.trim().is_empty()) {
                findings.push(finding(
                    McpFindingSeverity::Error,
                    "mcp.stdio_command_empty",
                    "stdio transport command must be non-empty",
                    "set transport.command to an allowlisted executable and arguments",
                ));
            } else if !stdio_command_allowed(
                command[0].as_str(),
                policy.allowed_stdio_commands.as_slice(),
            ) {
                findings.push(finding(
                    McpFindingSeverity::Error,
                    "mcp.stdio_command_not_allowlisted",
                    "stdio transport executable is not allowlisted by host policy",
                    "add the executable to the host MCP stdio allowlist",
                ));
            }
            for (key, value) in env {
                if key.trim().is_empty() {
                    findings.push(finding(
                        McpFindingSeverity::Error,
                        "mcp.env_key_empty",
                        "stdio environment keys must be non-empty",
                        "remove empty environment keys",
                    ));
                }
                if looks_secretish(key.as_str()) && !is_vault_ref(value.as_str()) {
                    findings.push(finding(
                        McpFindingSeverity::Error,
                        "mcp.inline_secret_rejected",
                        "secret-like stdio environment values must be vault references",
                        "replace inline secret values with vault:// references",
                    ));
                }
            }
        }
        McpTransportManifest::Http { url } | McpTransportManifest::Sse { url } => {
            if !url.starts_with("https://")
                && !url.starts_with("http://127.0.0.1")
                && !url.starts_with("http://localhost")
            {
                findings.push(finding(
                    McpFindingSeverity::Error,
                    "mcp.remote_transport_url_rejected",
                    "remote MCP transports must use https or loopback HTTP",
                    "use https:// or a loopback URL and declare egress allowlist entries",
                ));
            }
        }
    }
}

fn validate_timeouts(
    manifest: &McpServerManifest,
    policy: &McpBrokerPolicy,
    findings: &mut Vec<McpValidationFinding>,
) {
    if manifest.timeout_ms == 0 || manifest.timeout_ms > policy.max_timeout_ms {
        findings.push(finding(
            McpFindingSeverity::Error,
            "mcp.timeout_invalid",
            "MCP tool timeout must be within host policy bounds",
            "set timeout_ms to a positive value not exceeding host policy",
        ));
    }
    if manifest.start_timeout_ms == 0 || manifest.start_timeout_ms > policy.max_start_timeout_ms {
        findings.push(finding(
            McpFindingSeverity::Error,
            "mcp.start_timeout_invalid",
            "MCP server start timeout must be within host policy bounds",
            "set start_timeout_ms to a positive value not exceeding host policy",
        ));
    }
    if manifest.max_response_bytes == 0 || manifest.max_response_bytes > policy.max_response_bytes {
        findings.push(finding(
            McpFindingSeverity::Error,
            "mcp.max_response_bytes_invalid",
            "MCP max response bytes must be within host policy bounds",
            "set max_response_bytes to a positive bounded value",
        ));
    }
}

fn validate_vault_refs(grants: &[McpVaultRefGrant], findings: &mut Vec<McpValidationFinding>) {
    let mut names = BTreeSet::new();
    for grant in grants {
        if normalize_mcp_identifier(grant.name.as_str(), "vault_ref_name").is_err() {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.vault_ref_name_invalid",
                "vault reference logical names must use [a-z0-9._-]",
                "rename the vault reference grant",
            ));
        }
        if !names.insert(grant.name.trim().to_ascii_lowercase()) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.vault_ref_duplicate",
                "vault reference logical names must be unique",
                "remove duplicate vault reference grants",
            ));
        }
        if !is_vault_ref(grant.vault_ref.as_str()) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.vault_ref_invalid",
                "vault references must use vault:// or vault: syntax",
                "store credentials in the vault and reference them by vault URI",
            ));
        }
    }
}

fn validate_tool_filters(manifest: &McpServerManifest, findings: &mut Vec<McpValidationFinding>) {
    for tool in manifest.tool_allowlist.iter().chain(manifest.tool_denylist.iter()) {
        if normalize_mcp_identifier(tool.as_str(), "tool_filter").is_err() {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.tool_filter_invalid",
                "tool filters must use non-empty [a-z0-9._-] names",
                "fix or remove invalid tool allowlist/denylist entries",
            ));
        }
    }
    if manifest.tool_allowlist.is_empty() {
        findings.push(finding(
            McpFindingSeverity::Warning,
            "mcp.tool_allowlist_missing",
            "manifest does not restrict discovered tool names",
            "set tool_allowlist for production MCP servers",
        ));
    }
}

fn validate_egress_allowlist(hosts: &[String], findings: &mut Vec<McpValidationFinding>) {
    if hosts.is_empty() {
        findings.push(finding(
            McpFindingSeverity::Warning,
            "mcp.egress_allowlist_missing",
            "manifest does not declare an egress allowlist",
            "declare every host the MCP server may contact",
        ));
        return;
    }
    for host in hosts {
        if !valid_host(host.as_str()) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.egress_host_invalid",
                "egress allowlist entries must be exact hostnames",
                "use lowercase hostnames without wildcards or URL schemes",
            ));
        }
    }
}

fn tool_allowed_by_manifest(manifest: &McpServerManifest, raw_tool_name: &str) -> bool {
    let Ok(tool_name) = normalize_mcp_identifier(raw_tool_name, "tool_name") else {
        return false;
    };
    if manifest
        .tool_denylist
        .iter()
        .filter_map(|tool| normalize_mcp_identifier(tool, "tool_name").ok())
        .any(|denied| denied == tool_name)
    {
        return false;
    }
    manifest.tool_allowlist.is_empty()
        || manifest
            .tool_allowlist
            .iter()
            .filter_map(|tool| normalize_mcp_identifier(tool, "tool_name").ok())
            .any(|allowed| allowed == tool_name)
}

fn host_allowed_by_manifest(host: &str, allowlist: &[String]) -> bool {
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    allowlist.iter().any(|allowed| allowed.trim().trim_end_matches('.').eq(&normalized))
}

fn first_ungranted_vault_ref<'a>(
    requested: &'a [String],
    grants: &[McpVaultRefGrant],
) -> Option<&'a str> {
    let granted =
        grants.iter().map(|grant| grant.name.trim().to_ascii_lowercase()).collect::<BTreeSet<_>>();
    requested
        .iter()
        .map(String::as_str)
        .find(|requested| !granted.contains(&requested.trim().to_ascii_lowercase()))
}

fn bounded_output(
    output: &Value,
    max_response_bytes: usize,
    request: &McpToolCallRequest,
    namespaced_tool_id: &str,
) -> Result<Value, Box<McpToolInvocationOutcome>> {
    let bytes = serde_json::to_vec(output).unwrap_or_else(|_| b"null".to_vec());
    if bytes.len() <= max_response_bytes {
        return Ok(output.clone());
    }
    Err(Box::new(McpToolInvocationOutcome {
        success: false,
        output_json: json!({}),
        error: Some(format!(
            "MCP tool response exceeded max_response_bytes ({} > {})",
            bytes.len(),
            max_response_bytes
        )),
        attestation: invocation_attestation(
            request,
            namespaced_tool_id.to_owned(),
            json!({}),
            "output_too_large",
            true,
        ),
    }))
}

fn denied_invocation(
    request: &McpToolCallRequest,
    namespaced_tool_id: String,
    reason_code: &str,
    message: &str,
) -> McpToolInvocationOutcome {
    let output_json = json!({
        "success": false,
        "reason_code": reason_code,
        "message": message,
    });
    McpToolInvocationOutcome {
        success: false,
        output_json: output_json.clone(),
        error: Some(message.to_owned()),
        attestation: invocation_attestation(
            request,
            namespaced_tool_id,
            output_json,
            reason_code,
            false,
        ),
    }
}

fn invocation_attestation(
    request: &McpToolCallRequest,
    namespaced_tool_id: String,
    output_json: Value,
    policy_outcome: &str,
    output_truncated: bool,
) -> McpInvocationAttestation {
    let executed_at_unix_ms = current_unix_ms();
    let input_hash = stable_hash_value(&request.input);
    let output_hash = stable_hash_value(&output_json);
    let attestation_seed = format!(
        "{}:{}:{}:{}:{}:{}",
        request.server_name,
        request.tool_name,
        request.schema_hash,
        input_hash,
        output_hash,
        executed_at_unix_ms
    );
    McpInvocationAttestation {
        attestation_id: format!("mcpatt_{}", &stable_hash_bytes(attestation_seed.as_bytes())[..16]),
        server_name: request.server_name.clone(),
        tool_name: request.tool_name.clone(),
        namespaced_tool_id,
        schema_hash: request.schema_hash.clone(),
        input_hash,
        output_hash,
        policy_outcome: policy_outcome.to_owned(),
        executed_at_unix_ms,
        output_truncated,
    }
}

fn normalize_mcp_identifier(raw: &str, field_name: &str) -> Result<String, McpBrokerError> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > MAX_IDENTIFIER_LEN
        || normalized.starts_with('.')
        || normalized.starts_with(':')
        || normalized.ends_with('.')
        || normalized.ends_with(':')
        || normalized.contains("..")
        || normalized.contains("::")
        || !normalized.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-' | ':')
        })
    {
        return Err(McpBrokerError::new(
            "mcp.identifier_invalid",
            format!("{field_name} must use non-empty [a-z0-9._:-] segments"),
        ));
    }
    Ok(normalized)
}

fn append_redacted_stderr(
    record: &mut McpRuntimeServerRecord,
    stderr: Option<&str>,
    max_tail_bytes: usize,
) {
    let Some(stderr) = stderr else {
        return;
    };
    let redacted = redact_diagnostic_text(stderr).replace("\r\n", "\n").replace('\r', "\n");
    if redacted.trim().is_empty() || max_tail_bytes == 0 {
        return;
    }
    let combined = match record.stderr_tail_redacted.as_deref() {
        Some(previous) if !previous.is_empty() => format!("{previous}\n{redacted}"),
        _ => redacted,
    };
    record.stderr_tail_redacted = Some(tail_on_char_boundary(combined.as_str(), max_tail_bytes));
}

fn tail_on_char_boundary(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut start = value.len().saturating_sub(max_bytes);
    while start < value.len() && !value.is_char_boundary(start) {
        start = start.saturating_add(1);
    }
    format!("...{}", &value[start..])
}

fn deterministic_backoff_jitter_ms(server_id: &str, attempt_index: u32) -> i64 {
    let seed = server_id.bytes().fold(u64::from(attempt_index), |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(u64::from(byte))
    });
    i64::try_from(seed % 250).unwrap_or(0)
}

fn finding(
    severity: McpFindingSeverity,
    code: &str,
    message: &str,
    fix_hint: &str,
) -> McpValidationFinding {
    McpValidationFinding {
        severity,
        code: code.to_owned(),
        message: message.to_owned(),
        fix_hint: fix_hint.to_owned(),
    }
}

fn stdio_command_allowed(command: &str, allowlist: &[String]) -> bool {
    let command = command.trim().to_ascii_lowercase();
    allowlist.iter().any(|allowed| allowed.trim().to_ascii_lowercase() == command)
}

fn valid_host(host: &str) -> bool {
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    !normalized.is_empty()
        && !normalized.contains("..")
        && !normalized.contains('*')
        && !normalized.contains('/')
        && !normalized.starts_with('-')
        && !normalized.ends_with('-')
        && normalized
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '-'))
}

fn looks_secretish(key: &str) -> bool {
    let normalized = key
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '_' })
        .collect::<String>();
    [
        "api_key",
        "apikey",
        "authorization",
        "bearer",
        "client_secret",
        "password",
        "private_key",
        "refresh_token",
        "secret",
        "token",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn is_vault_ref(value: &str) -> bool {
    let value = value.trim();
    value.starts_with("vault://") || value.starts_with("vault:")
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

fn default_start_timeout_ms() -> u64 {
    DEFAULT_START_TIMEOUT_MS
}

fn default_max_response_bytes() -> usize {
    DEFAULT_MAX_RESPONSE_BYTES
}

fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use palyra_common::tool_catalog::ToolCatalogExposureMode;

    use super::*;
    use crate::{
        application::tool_registry::{
            build_model_visible_tool_catalog_snapshot_with_external_tools,
            ToolCatalogPolicySnapshot,
        },
        config::{
            McpServerApprovalProfile, McpServerEgressPolicy, McpServerEnvVaultRef,
            McpServerTrustLevel,
        },
        sandbox_runner::{
            EgressEnforcementMode, PathAccessMode, SandboxProcessRunnerPolicy,
            SandboxProcessRunnerTier,
        },
        tool_protocol::{ToolCallConfig, ToolRequestContext},
        wasm_plugin_runner::WasmPluginRunnerPolicy,
    };

    #[derive(Default)]
    struct FakeTransport {
        tools: Vec<McpDiscoveredTool>,
        response: Option<McpToolResponse>,
        start_error: Option<McpBrokerError>,
        call_count: Cell<u32>,
    }

    impl McpTransport for FakeTransport {
        fn start(&self, _manifest: &McpServerManifest) -> Result<(), McpBrokerError> {
            if let Some(error) = &self.start_error {
                return Err(error.clone());
            }
            Ok(())
        }

        fn list_tools(
            &self,
            _manifest: &McpServerManifest,
        ) -> Result<Vec<McpDiscoveredTool>, McpBrokerError> {
            Ok(self.tools.clone())
        }

        fn call_tool(
            &self,
            _manifest: &McpServerManifest,
            _request: &McpToolCallRequest,
        ) -> Result<McpToolResponse, McpBrokerError> {
            self.call_count.set(self.call_count.get().saturating_add(1));
            Ok(self.response.clone().unwrap_or(McpToolResponse {
                output: json!({"ok": true}),
                sampling_requested: false,
                egress_host_requested: None,
            }))
        }
    }

    fn policy() -> McpBrokerPolicy {
        McpBrokerPolicy {
            allowed_stdio_commands: vec!["node".to_owned()],
            max_timeout_ms: 10_000,
            max_start_timeout_ms: 5_000,
            max_response_bytes: 1024,
        }
    }

    fn manifest() -> McpServerManifest {
        McpServerManifest {
            name: "docs".to_owned(),
            transport: McpTransportManifest::Stdio {
                command: vec!["node".to_owned(), "server.js".to_owned()],
                env: BTreeMap::from([(
                    "API_TOKEN".to_owned(),
                    "vault://global/docs_api_token".to_owned(),
                )]),
            },
            vault_refs: vec![McpVaultRefGrant {
                name: "api_token".to_owned(),
                vault_ref: "vault://global/docs_api_token".to_owned(),
            }],
            egress_allowlist: vec!["api.example.com".to_owned()],
            tool_allowlist: vec!["search".to_owned(), "write_note".to_owned()],
            tool_denylist: Vec::new(),
            timeout_ms: 1_000,
            start_timeout_ms: 500,
            max_response_bytes: 512,
            sensitivity_default: McpToolSensitivity::Internal,
            approval_policy: McpApprovalPolicy::Safe,
            sampling_enabled: false,
        }
    }

    fn discovered_tool(name: &str) -> McpDiscoveredTool {
        McpDiscoveredTool {
            name: name.to_owned(),
            description: format!("{name} test tool"),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
            capabilities: vec!["docs".to_owned()],
            sensitivity: None,
            approval_policy: None,
        }
    }

    fn broker_with_ready_manifest(transport: &dyn McpTransport) -> McpBroker {
        let mut broker = McpBroker::new(policy());
        broker.register_manifest(manifest()).expect("manifest should register");
        broker.start_server("docs", transport).expect("server should start");
        broker
    }

    fn invocation_request() -> McpToolCallRequest {
        McpToolCallRequest {
            server_name: "docs".to_owned(),
            tool_name: "search".to_owned(),
            input: json!({"query": "rust"}),
            schema_hash: "schema_hash".to_owned(),
            policy: McpInvocationPolicyDecision {
                allowed: true,
                approval_required: false,
                reason: "allowlisted".to_owned(),
            },
            approval_granted: false,
            vault_refs_requested: vec!["api_token".to_owned()],
        }
    }

    fn runtime_server(id: &str, enabled: bool) -> McpServerConfig {
        McpServerConfig {
            id: id.to_owned(),
            enabled,
            namespace: id.replace(':', "_"),
            transport: McpServerTransport::Stdio,
            command: Some(vec!["node".to_owned(), "server.js".to_owned()]),
            url: None,
            env_vault_refs: Vec::<McpServerEnvVaultRef>::new(),
            trust_level: McpServerTrustLevel::Workspace,
            approval_profile: McpServerApprovalProfile::RequireApproval,
            egress_policy: McpServerEgressPolicy::DenyAll,
            egress_allowlist: Vec::new(),
            tool_allowlist: Vec::new(),
            tool_denylist: Vec::new(),
        }
    }

    fn runtime_config(servers: Vec<McpServerConfig>) -> McpServersConfig {
        McpServersConfig { mode: RuntimePreviewMode::PreviewOnly, servers }
    }

    fn runtime_server_snapshot<'a>(
        snapshot: &'a McpRuntimeSupervisorSnapshot,
        id: &str,
    ) -> &'a McpRuntimeServerSnapshot {
        snapshot
            .servers
            .iter()
            .find(|server| server.id == id)
            .expect("server snapshot should exist")
    }

    #[test]
    fn runtime_supervisor_initializes_configured_servers() {
        let supervisor = McpRuntimeSupervisor::from_config(&runtime_config(vec![
            runtime_server("docs", true),
            runtime_server("ops:search", false),
        ]));

        let snapshot = supervisor.snapshot(42);

        assert_eq!(snapshot.schema_version, 1);
        assert_eq!(snapshot.generated_at_unix_ms, 42);
        assert_eq!(snapshot.mode, "preview_only");
        assert_eq!(snapshot.total_servers, 2);
        assert_eq!(snapshot.enabled_servers, 1);
        assert_eq!(snapshot.disabled_servers, 1);
        assert_eq!(
            runtime_server_snapshot(&snapshot, "docs").state,
            McpServerLifecycleState::Stopped
        );
        assert_eq!(
            runtime_server_snapshot(&snapshot, "ops:search").state,
            McpServerLifecycleState::Disabled
        );
    }

    #[test]
    fn runtime_supervisor_backoff_blocks_retry_and_quarantines_after_repeated_failures() {
        let policy = McpRuntimeSupervisorPolicy {
            max_retries: 3,
            base_backoff_ms: 100,
            max_backoff_ms: 1_000,
            stderr_tail_bytes: 256,
        };
        let mut supervisor = McpRuntimeSupervisor::from_config_with_policy(
            &runtime_config(vec![runtime_server("docs", true)]),
            policy,
        );

        assert_eq!(
            supervisor.start_server("docs", 1_000).expect("server should start"),
            McpServerLifecycleState::Starting
        );
        assert_eq!(
            supervisor
                .record_failure("docs", "mcp.process_exit", "process exited", None, 1_100)
                .expect("failure should enter backoff"),
            McpServerLifecycleState::Backoff
        );
        let retry_at = runtime_server_snapshot(&supervisor.snapshot(1_100), "docs")
            .next_retry_at_unix_ms
            .expect("backoff should set retry time");
        let retry_error = supervisor
            .start_server("docs", retry_at.saturating_sub(1))
            .expect_err("backoff should block early retry");
        assert_eq!(retry_error.reason_code, "mcp.server_backoff");

        supervisor.start_server("docs", retry_at).expect("retry should be allowed after backoff");
        supervisor
            .record_failure("docs", "mcp.process_exit", "process exited", None, retry_at + 1)
            .expect("second failure should enter backoff");
        let retry_at = runtime_server_snapshot(&supervisor.snapshot(retry_at + 1), "docs")
            .next_retry_at_unix_ms
            .expect("second backoff should set retry time");
        supervisor
            .start_server("docs", retry_at)
            .expect("second retry should be allowed after backoff");
        assert_eq!(
            supervisor
                .record_failure("docs", "mcp.process_exit", "process exited", None, retry_at + 1)
                .expect("third failure should quarantine"),
            McpServerLifecycleState::Quarantined
        );

        let snapshot = supervisor.snapshot(retry_at + 1);
        let server = runtime_server_snapshot(&snapshot, "docs");
        assert_eq!(server.state, McpServerLifecycleState::Quarantined);
        assert_eq!(server.consecutive_failures, 3);
        assert_eq!(server.total_failures, 3);
        assert!(server.next_retry_at_unix_ms.is_none());
        assert_eq!(
            supervisor
                .start_server("docs", retry_at + 2)
                .expect_err("quarantine blocks starts")
                .reason_code,
            "mcp.server_quarantined"
        );
    }

    #[test]
    fn runtime_supervisor_restart_and_stop_update_lifecycle_evidence() {
        let mut supervisor =
            McpRuntimeSupervisor::from_config(&runtime_config(vec![runtime_server("docs", true)]));

        assert_eq!(
            supervisor.restart_server("docs", 100).expect("restart should start server"),
            McpServerLifecycleState::Starting
        );
        assert_eq!(
            supervisor
                .record_start_success("docs", 110)
                .expect("start success should mark healthy"),
            McpServerLifecycleState::Healthy
        );
        assert_eq!(
            supervisor.stop_server("docs", 120).expect("stop should mark stopped"),
            McpServerLifecycleState::Stopped
        );

        let snapshot = supervisor.snapshot(120);
        let server = runtime_server_snapshot(&snapshot, "docs");
        assert_eq!(server.restart_count, 1);
        assert_eq!(server.state, McpServerLifecycleState::Stopped);
        assert_eq!(server.updated_at_unix_ms, 120);
    }

    #[test]
    fn runtime_supervisor_stderr_tail_is_redacted_and_bounded() {
        let policy = McpRuntimeSupervisorPolicy {
            max_retries: 5,
            base_backoff_ms: 100,
            max_backoff_ms: 1_000,
            stderr_tail_bytes: 48,
        };
        let mut supervisor = McpRuntimeSupervisor::from_config_with_policy(
            &runtime_config(vec![runtime_server("docs", true)]),
            policy,
        );
        let stderr = format!("{}\napi_key=super-secret-value", "prefix".repeat(24));

        supervisor
            .record_stderr("docs", stderr.as_str(), 1_000)
            .expect("stderr should be recorded");

        let snapshot = supervisor.snapshot(1_000);
        let tail = runtime_server_snapshot(&snapshot, "docs")
            .stderr_tail_redacted
            .as_ref()
            .expect("stderr tail should be present");
        assert!(tail.len() <= 51, "tail should stay within max plus ellipsis: {tail}");
        assert!(tail.starts_with("..."));
        assert!(!tail.contains("super-secret-value"));
    }

    #[test]
    fn mcp_identifier_normalizer_accepts_registry_style_colon_ids() {
        assert_eq!(
            normalize_mcp_identifier("Ops:Search", "server_name")
                .expect("colon id should normalize"),
            "ops:search"
        );
        assert!(normalize_mcp_identifier("ops::search", "server_name").is_err());
    }

    #[test]
    fn manifest_validation_rejects_inline_secret_and_sampling() {
        let mut manifest = manifest();
        let McpTransportManifest::Stdio { env, .. } = &mut manifest.transport else {
            panic!("test manifest should use stdio");
        };
        env.insert("PASSWORD".to_owned(), "plain-text".to_owned());
        manifest.sampling_enabled = true;

        let report = validate_mcp_server_manifest(&manifest, &policy());

        assert!(!report.valid);
        assert!(report.findings.iter().any(|finding| finding.code == "mcp.inline_secret_rejected"));
        assert!(report.findings.iter().any(|finding| finding.code == "mcp.sampling_denied"));
    }

    #[test]
    fn manifest_validation_requires_stdio_command_allowlist() {
        let mut manifest = manifest();
        manifest.transport = McpTransportManifest::Stdio {
            command: vec!["python".to_owned(), "server.py".to_owned()],
            env: BTreeMap::new(),
        };

        let report = validate_mcp_server_manifest(&manifest, &policy());

        assert!(!report.valid);
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "mcp.stdio_command_not_allowlisted"));
    }

    #[test]
    fn broker_lifecycle_reports_start_failure_as_degraded() {
        let transport = FakeTransport {
            start_error: Some(McpBrokerError::new("mcp.start_timeout", "start timed out")),
            ..FakeTransport::default()
        };
        let mut broker = McpBroker::new(policy());
        broker.register_manifest(manifest()).expect("manifest should register");

        let error = broker.start_server("docs", &transport).expect_err("start should fail");

        assert_eq!(error.reason_code, "mcp.start_timeout");
        assert_eq!(
            broker.state("docs").expect("server state should be readable"),
            McpServerLifecycleState::Degraded
        );
    }

    #[test]
    fn discovery_filters_malicious_schema_and_imports_namespaced_tool() {
        let mut malicious = discovered_tool("write_note");
        malicious
            .input_schema
            .as_object_mut()
            .expect("schema object")
            .insert("$ref".to_owned(), Value::String("https://attacker.example/schema".to_owned()));
        let transport = FakeTransport {
            tools: vec![discovered_tool("search"), malicious, discovered_tool("hidden")],
            ..FakeTransport::default()
        };
        let mut broker = broker_with_ready_manifest(&transport);

        let report = broker.discover_tools("docs", &transport).expect("discovery should run");

        assert_eq!(report.imported_count, 1);
        assert_eq!(report.registry_entries[0].name, "mcp.docs.search");
        assert_eq!(report.registry_entries[0].provenance, "mcp:docs");
        assert!(report
            .filtered_tools
            .iter()
            .any(|tool| tool.reason_code == "schema.unsupported_keyword"));
        assert!(report.filtered_tools.iter().any(|tool| tool.raw_name == "hidden"));
    }

    #[test]
    fn imported_mcp_tool_enters_model_catalog_when_allowlisted() {
        let transport =
            FakeTransport { tools: vec![discovered_tool("search")], ..Default::default() };
        let mut broker = broker_with_ready_manifest(&transport);
        let report = broker.discover_tools("docs", &transport).expect("discovery should run");
        let config = tool_config(&["mcp.docs.search"]);
        let snapshot = build_model_visible_tool_catalog_snapshot_with_external_tools(
            crate::application::tool_registry::ToolCatalogBuildRequest {
                config: &config,
                catalog_policy: &ToolCatalogPolicySnapshot {
                    profile_expansion: palyra_common::tool_catalog::expand_toolset_profiles(
                        &[],
                        &config.allowed_tools,
                        &[],
                        &[],
                    )
                    .expect("profile expansion should succeed"),
                    exposure_mode: ToolCatalogExposureMode::Direct,
                    compact_tool_threshold: 16,
                },
                browser_service_enabled: false,
                browser_service_configured: false,
                request_context: &ToolRequestContext {
                    principal: "user:test".to_owned(),
                    device_id: None,
                    channel: Some("console".to_owned()),
                    session_id: None,
                    run_id: None,
                    skill_id: None,
                },
                provider_kind: "openai_compatible",
                provider_model_id: None,
                surface: ToolExposureSurface::RunStream,
                remaining_tool_budget: None,
                created_at_unix_ms: 42,
            },
            report.registry_entries.as_slice(),
        );

        assert!(snapshot.tools.iter().any(|tool| tool.name == "mcp.docs.search"));
        assert!(snapshot.indexed_tools.iter().any(|tool| tool.provenance == "mcp:docs"));
    }

    #[test]
    fn invocation_denies_policy_before_transport_call() {
        let transport = FakeTransport::default();
        let mut broker = broker_with_ready_manifest(&transport);
        let mut request = invocation_request();
        request.policy.allowed = false;
        request.policy.reason = "deny by policy".to_owned();

        let outcome = broker.invoke_tool(request, &transport).expect("denial should be attested");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.policy_denied");
        assert_eq!(transport.call_count.get(), 0);
    }

    #[test]
    fn invocation_requires_approval_when_policy_says_so() {
        let transport = FakeTransport::default();
        let mut broker = broker_with_ready_manifest(&transport);
        let mut request = invocation_request();
        request.policy.approval_required = true;

        let outcome = broker.invoke_tool(request, &transport).expect("denial should be attested");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.approval_required");
        assert_eq!(transport.call_count.get(), 0);
    }

    #[test]
    fn invocation_rejects_sampling_and_quarantines_after_repeated_violations() {
        let transport = FakeTransport {
            response: Some(McpToolResponse {
                output: json!({"ok": true}),
                sampling_requested: true,
                egress_host_requested: None,
            }),
            ..Default::default()
        };
        let mut broker = broker_with_ready_manifest(&transport);

        for _ in 0..QUARANTINE_AFTER_VIOLATIONS {
            let outcome = broker
                .invoke_tool(invocation_request(), &transport)
                .expect("sampling denial should be attested");
            assert!(!outcome.success);
            if broker.state("docs").expect("state should be readable")
                != McpServerLifecycleState::Quarantined
            {
                let _ = broker.start_server("docs", &FakeTransport::default());
            }
        }

        assert_eq!(
            broker.state("docs").expect("server should be quarantined"),
            McpServerLifecycleState::Quarantined
        );
    }

    #[test]
    fn invocation_rejects_egress_outside_allowlist_and_large_output() {
        let transport = FakeTransport {
            response: Some(McpToolResponse {
                output: json!({"data": "x".repeat(2048)}),
                sampling_requested: false,
                egress_host_requested: Some("evil.example".to_owned()),
            }),
            ..Default::default()
        };
        let mut broker = broker_with_ready_manifest(&transport);

        let outcome = broker
            .invoke_tool(invocation_request(), &transport)
            .expect("denial should be attested");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.egress_denied");

        let transport = FakeTransport {
            response: Some(McpToolResponse {
                output: json!({"data": "x".repeat(2048)}),
                sampling_requested: false,
                egress_host_requested: Some("api.example.com".to_owned()),
            }),
            ..Default::default()
        };
        let _ = broker.start_server("docs", &FakeTransport::default());
        let outcome = broker
            .invoke_tool(invocation_request(), &transport)
            .expect("large output should be attested");
        assert!(!outcome.success);
        assert!(outcome.attestation.output_truncated);
    }

    #[test]
    fn scrubbed_stdio_env_never_exposes_values() {
        let env = scrub_stdio_env(&manifest());

        assert_eq!(env.get("API_TOKEN"), Some(&"<redacted>".to_owned()));
    }

    fn tool_config(allowed_tools: &[&str]) -> ToolCallConfig {
        ToolCallConfig {
            allowed_tools: allowed_tools.iter().map(|tool| (*tool).to_owned()).collect(),
            max_calls_per_run: 4,
            execution_timeout_ms: 1_000,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: false,
                tier: SandboxProcessRunnerTier::B,
                workspace_root: ".".into(),
                path_access_mode: PathAccessMode::WorkspaceOnly,
                allowed_executables: Vec::new(),
                allow_interpreters: false,
                egress_enforcement_mode: EgressEnforcementMode::Strict,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 1_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: WasmPluginRunnerPolicy {
                enabled: false,
                allow_inline_modules: false,
                max_module_size_bytes: 256 * 1024,
                fuel_budget: 10_000_000,
                max_memory_bytes: 64 * 1024 * 1024,
                max_table_elements: 100_000,
                max_instances: 256,
                allowed_http_hosts: Vec::new(),
                allowed_secrets: Vec::new(),
                allowed_storage_prefixes: Vec::new(),
                allowed_channels: Vec::new(),
            },
        }
    }
}
