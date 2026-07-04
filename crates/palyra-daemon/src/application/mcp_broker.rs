//! Secure Model Context Protocol broker contracts for external tool servers.
//!
//! The broker imports externally-discovered tools only after manifest,
//! namespace, policy, schema, output-size, approval, and vault-reference gates
//! pass. This module intentionally keeps transport side effects behind
//! [`McpTransport`] so catalog import and invocation decisions remain
//! deterministic and testable.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt,
    future::Future,
    io::Read,
    net::IpAddr,
    process::Stdio,
    sync::mpsc,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_common::{redaction::redact_diagnostic_text, runtime_preview::RuntimePreviewMode};
use palyra_egress_proxy::{EgressProxyPolicyService, EgressProxyRequest};
use reqwest::{blocking::Response, redirect::Policy, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::{McpServerConfig, McpServerTransport, McpServersConfig};

use super::tool_registry::{
    build_model_visible_tool_catalog_snapshot_with_external_records, sanitize_schema_for_provider,
    stable_hash_bytes, stable_hash_value, FilteredToolCatalogEntry,
    ModelVisibleToolCatalogSnapshot, ToolApprovalPosture, ToolCatalogBuildRequest,
    ToolCatalogFilterReasonCode, ToolExposureSurface, ToolParallelismPolicy, ToolRegistryEntry,
    ToolReplaySafetyClass, ToolResultProjectionPolicy, ToolSchemaDialect,
};

const MCP_SCHEMA_VERSION: u32 = 1;
const MCP_RUNTIME_SUPERVISOR_SCHEMA_VERSION: u32 = 2;
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_START_TIMEOUT_MS: u64 = 2_500;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_IDENTIFIER_LEN: usize = 64;
const QUARANTINE_AFTER_VIOLATIONS: u32 = 3;
const DEFAULT_SUPERVISOR_MAX_RETRIES: u32 = 3;
const DEFAULT_SUPERVISOR_BASE_BACKOFF_MS: i64 = 1_000;
const DEFAULT_SUPERVISOR_MAX_BACKOFF_MS: i64 = 30_000;
const DEFAULT_SUPERVISOR_STDERR_TAIL_BYTES: usize = 4 * 1024;
const MCP_JSONRPC_VERSION: &str = "2.0";
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const MCP_STDIO_MAX_HEADER_BYTES: usize = 8 * 1024;
const MCP_STDIO_STDERR_TAIL_BYTES: usize = 4 * 1024;
const MCP_STDIO_INHERITED_ENV_ALLOWLIST: &[&str] =
    &["PATH", "Path", "PATHEXT", "SystemRoot", "SYSTEMROOT", "WINDIR", "TMP", "TEMP"];

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
    #[serde(default)]
    pub oauth_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_grant: Option<McpOAuthGrant>,
    #[serde(default)]
    pub sampling_policy: McpSamplingPolicy,
}

/// Vault-backed OAuth grant descriptor for one MCP server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthGrant {
    pub grant_id: String,
    pub access_token_vault_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token_vault_ref: Option<String>,
    pub metadata_vault_ref: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_id: Option<String>,
    pub issued_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at_unix_ms: Option<i64>,
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

/// Stable operational class for the last MCP runtime failure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpRuntimeErrorClass {
    ProtocolViolation,
    InvalidSchema,
    AuthFailure,
    OutputLimitAbuse,
    TransportFlapping,
    PolicyViolation,
    Unknown,
}

impl McpRuntimeErrorClass {
    /// Returns the canonical snake_case wire label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProtocolViolation => "protocol_violation",
            Self::InvalidSchema => "invalid_schema",
            Self::AuthFailure => "auth_failure",
            Self::OutputLimitAbuse => "output_limit_abuse",
            Self::TransportFlapping => "transport_flapping",
            Self::PolicyViolation => "policy_violation",
            Self::Unknown => "unknown",
        }
    }
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
    pub catalog_generation: u64,
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
    pub last_successful_probe_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_class: Option<McpRuntimeErrorClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr_tail_redacted: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quarantine_reason: Option<String>,
    pub catalog_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_hidden_reason: Option<String>,
    pub repair_hint: String,
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
    last_successful_probe_at_unix_ms: Option<i64>,
    next_retry_at_unix_ms: Option<i64>,
    last_error_class: Option<McpRuntimeErrorClass>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
    stderr_tail_redacted: Option<String>,
    quarantine_reason: Option<String>,
    updated_at_unix_ms: i64,
}

/// In-memory supervisor for configured MCP server runtime state.
#[derive(Debug, Clone)]
pub struct McpRuntimeSupervisor {
    mode: RuntimePreviewMode,
    policy: McpRuntimeSupervisorPolicy,
    catalog_generation: u64,
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
        Self { mode: config.mode, policy, catalog_generation: 0, servers }
    }

    /// Returns a deterministic redacted supervisor snapshot.
    #[must_use]
    pub fn snapshot(&self, generated_at_unix_ms: i64) -> McpRuntimeSupervisorSnapshot {
        let servers =
            self.servers.values().map(McpRuntimeServerRecord::snapshot).collect::<Vec<_>>();
        McpRuntimeSupervisorSnapshot {
            schema_version: MCP_RUNTIME_SUPERVISOR_SCHEMA_VERSION,
            generated_at_unix_ms,
            catalog_generation: self.catalog_generation,
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

    /// Replaces the configured MCP registry and invalidates future catalog snapshots.
    ///
    /// Existing records keep health evidence when the server id and transport are
    /// unchanged. New runs will observe the incremented catalog generation, while
    /// already-started runs keep their previously recorded catalog snapshot.
    pub fn reload_from_config(&mut self, config: &McpServersConfig, now_unix_ms: i64) {
        let global_enabled = config.mode != RuntimePreviewMode::Disabled;
        let previous = std::mem::take(&mut self.servers);
        self.mode = config.mode;
        self.catalog_generation = self.catalog_generation.saturating_add(1);
        self.servers = config
            .servers
            .iter()
            .map(|server| {
                let mut next = McpRuntimeServerRecord::from_config(server, global_enabled);
                if let Some(existing) = previous.get(next.id.as_str()) {
                    next.apply_reload_evidence(existing, now_unix_ms);
                }
                next.updated_at_unix_ms = now_unix_ms;
                (next.id.clone(), next)
            })
            .collect();
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
        record.last_successful_probe_at_unix_ms = Some(now_unix_ms);
        record.last_error_class = None;
        record.last_error_code = None;
        record.last_error_message = None;
        record.quarantine_reason = None;
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
        let error_class = classify_mcp_runtime_error(reason_code);
        append_redacted_stderr(record, stderr, stderr_tail_bytes);
        record.consecutive_failures = record.consecutive_failures.saturating_add(1);
        record.total_failures = record.total_failures.saturating_add(1);
        record.last_error_class = Some(error_class);
        record.last_error_code = Some(reason_code.trim().to_owned());
        record.last_error_message = Some(redact_diagnostic_text(message));
        record.updated_at_unix_ms = now_unix_ms;
        if record.consecutive_failures >= max_retries {
            record.state = McpServerLifecycleState::Quarantined;
            record.next_retry_at_unix_ms = None;
            record.quarantine_reason = Some(error_class.as_str().to_owned());
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
        record.last_error_class = Some(classify_mcp_runtime_error(reason_code));
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
            last_successful_probe_at_unix_ms: None,
            next_retry_at_unix_ms: None,
            last_error_class: None,
            last_error_code: None,
            last_error_message: None,
            stderr_tail_redacted: None,
            quarantine_reason: None,
            updated_at_unix_ms: 0,
        }
    }

    fn apply_reload_evidence(&mut self, existing: &Self, now_unix_ms: i64) {
        if !(self.enabled && existing.enabled && self.transport == existing.transport) {
            return;
        }
        if existing.state == McpServerLifecycleState::Quarantined {
            self.state = McpServerLifecycleState::Stopped;
            self.consecutive_failures = 0;
            self.next_retry_at_unix_ms = None;
            self.quarantine_reason = None;
        } else {
            self.state = existing.state;
            self.consecutive_failures = existing.consecutive_failures;
            self.next_retry_at_unix_ms = existing.next_retry_at_unix_ms;
            self.quarantine_reason.clone_from(&existing.quarantine_reason);
        }
        self.total_failures = existing.total_failures;
        self.restart_count = existing.restart_count;
        self.last_successful_probe_at_unix_ms = existing.last_successful_probe_at_unix_ms;
        self.last_error_class = existing.last_error_class;
        self.last_error_code.clone_from(&existing.last_error_code);
        self.last_error_message.clone_from(&existing.last_error_message);
        self.stderr_tail_redacted.clone_from(&existing.stderr_tail_redacted);
        self.updated_at_unix_ms = now_unix_ms;
    }

    fn snapshot(&self) -> McpRuntimeServerSnapshot {
        let catalog_available = self.enabled && self.state == McpServerLifecycleState::Healthy;
        McpRuntimeServerSnapshot {
            id: self.id.clone(),
            namespace: self.namespace.clone(),
            transport: self.transport.as_str().to_owned(),
            enabled: self.enabled,
            state: self.state,
            consecutive_failures: self.consecutive_failures,
            total_failures: self.total_failures,
            restart_count: self.restart_count,
            last_successful_probe_at_unix_ms: self.last_successful_probe_at_unix_ms,
            next_retry_at_unix_ms: self.next_retry_at_unix_ms,
            last_error_class: self.last_error_class,
            last_error_code: self.last_error_code.clone(),
            last_error_message: self.last_error_message.clone(),
            stderr_tail_redacted: self.stderr_tail_redacted.clone(),
            quarantine_reason: self.quarantine_reason.clone(),
            catalog_available,
            catalog_hidden_reason: mcp_catalog_hidden_reason(self),
            repair_hint: mcp_runtime_repair_hint(self),
            updated_at_unix_ms: self.updated_at_unix_ms,
        }
    }
}

fn classify_mcp_runtime_error(reason_code: &str) -> McpRuntimeErrorClass {
    let code = reason_code.trim().to_ascii_lowercase();
    if code.contains("protocol") || code.contains("jsonrpc") || code.contains("handshake") {
        McpRuntimeErrorClass::ProtocolViolation
    } else if code.contains("schema") {
        McpRuntimeErrorClass::InvalidSchema
    } else if code.contains("auth")
        || code.contains("oauth")
        || code.contains("unauthorized")
        || code.contains("forbidden")
        || code.contains("permission")
    {
        McpRuntimeErrorClass::AuthFailure
    } else if code.contains("output") || code.contains("limit") || code.contains("too_large") {
        McpRuntimeErrorClass::OutputLimitAbuse
    } else if code.contains("policy")
        || code.contains("approval")
        || code.contains("egress")
        || code.contains("vault")
        || code.contains("capability_denied")
    {
        McpRuntimeErrorClass::PolicyViolation
    } else if code.contains("transport")
        || code.contains("process")
        || code.contains("exit")
        || code.contains("timeout")
        || code.contains("connection")
    {
        McpRuntimeErrorClass::TransportFlapping
    } else {
        McpRuntimeErrorClass::Unknown
    }
}

fn mcp_catalog_hidden_reason(record: &McpRuntimeServerRecord) -> Option<String> {
    if record.enabled && record.state == McpServerLifecycleState::Healthy {
        return None;
    }
    let reason = if !record.enabled {
        "mcp.server_disabled"
    } else if record.state == McpServerLifecycleState::Quarantined {
        "mcp.server_quarantined"
    } else {
        "mcp.server_not_healthy"
    };
    Some(reason.to_owned())
}

fn mcp_runtime_repair_hint(record: &McpRuntimeServerRecord) -> String {
    if !record.enabled {
        return format!("run `palyra mcp enable {}` and then `palyra mcp reload`", record.id);
    }
    match record.state {
        McpServerLifecycleState::Healthy => "no action required".to_owned(),
        McpServerLifecycleState::Starting => {
            format!(
                "wait for MCP server `{}` startup to finish or run `palyra mcp probe {}`",
                record.id, record.id
            )
        }
        McpServerLifecycleState::Stopped => {
            format!("run `palyra mcp probe {}` to start a health check", record.id)
        }
        McpServerLifecycleState::Backoff => {
            format!(
                "wait for retry backoff or run `palyra mcp reload` after fixing `{}`",
                record.id
            )
        }
        McpServerLifecycleState::Degraded | McpServerLifecycleState::Quarantined => {
            match record.last_error_class.unwrap_or(McpRuntimeErrorClass::Unknown) {
                McpRuntimeErrorClass::ProtocolViolation => {
                    "upgrade or fix the MCP server protocol implementation, then run `palyra mcp reload`".to_owned()
                }
                McpRuntimeErrorClass::InvalidSchema => {
                    "fix the MCP tool schema, then run `palyra mcp reload`".to_owned()
                }
                McpRuntimeErrorClass::AuthFailure => {
                    format!("run `palyra mcp login {}` or fix MCP auth vault refs", record.id)
                }
                McpRuntimeErrorClass::OutputLimitAbuse => {
                    "reduce MCP response size or adjust output limits before reloading".to_owned()
                }
                McpRuntimeErrorClass::TransportFlapping => {
                    "inspect the MCP transport command or URL and stderr tail, then run `palyra mcp reload`".to_owned()
                }
                McpRuntimeErrorClass::PolicyViolation => {
                    "review MCP policy, approval, egress, and vault grant configuration".to_owned()
                }
                McpRuntimeErrorClass::Unknown => {
                    format!("inspect `palyra mcp doctor {}` and reload after fixing the server", record.id)
                }
            }
        }
        McpServerLifecycleState::Disabled => {
            format!("run `palyra mcp enable {}` and then `palyra mcp reload`", record.id)
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
    pub result_projection: String,
    pub policy_outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_model_capability: Option<String>,
    pub executed_at_unix_ms: i64,
    pub output_truncated: bool,
    #[serde(default)]
    pub vault_grant_ids: Vec<String>,
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

/// Normalized transport failure with redacted operator-facing detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpTransportError {
    pub reason_code: String,
    pub message: String,
}

impl McpTransportError {
    /// Builds a transport error and strips credentials from the message.
    #[must_use]
    pub fn new(reason_code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            reason_code: reason_code.into(),
            message: sanitize_mcp_transport_message(message.into().as_str()),
        }
    }
}

impl fmt::Display for McpTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.reason_code, self.message)
    }
}

impl std::error::Error for McpTransportError {}

impl From<McpTransportError> for McpBrokerError {
    fn from(error: McpTransportError) -> Self {
        Self::new(error.reason_code, error.message)
    }
}

/// Default real MCP transport implementation for stdio, HTTP, streamable HTTP, and SSE.
#[derive(Debug, Default, Clone, Copy)]
pub struct McpRuntimeTransport;

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
    imported_tools: BTreeMap<String, ToolRegistryEntry>,
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
                imported_tools: BTreeMap::new(),
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
        let (manifest, state) = {
            let record = self.server_record(server_id.as_str())?;
            (record.manifest.clone(), record.state)
        };
        if state != McpServerLifecycleState::Healthy {
            return Err(McpBrokerError::new(
                "mcp.server_not_ready",
                format!("MCP server '{}' is {}", server_name, state.as_str()),
            ));
        }
        let tools = transport.list_tools(&manifest)?;
        let report = import_discovered_tools(&manifest, state, tools);
        let has_protocol_violation =
            report.filtered_tools.iter().any(mcp_discovery_filter_is_protocol_violation);
        self.server_record_mut(server_id.as_str())?.imported_tools = report
            .registry_entries
            .iter()
            .cloned()
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        if has_protocol_violation {
            self.record_discovery_protocol_violation(server_id.as_str())?;
        }
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
        let transport_id = transport_id_for_manifest(&record.manifest);
        let mut audit_context = McpInvocationAuditContext::new(
            server_id.clone(),
            namespaced_tool_id,
            transport_id,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        );
        if record.state != McpServerLifecycleState::Healthy {
            return Ok(denied_invocation(
                &request,
                &audit_context,
                "mcp.server_not_ready",
                format!("MCP server is {}", record.state.as_str()).as_str(),
            ));
        }
        if !tool_allowed_by_manifest(&record.manifest, request.tool_name.as_str()) {
            return Ok(denied_invocation(
                &request,
                &audit_context,
                "mcp.tool_not_allowed",
                "tool is not allowed by the MCP server manifest",
            ));
        }
        let Some(registry_entry) =
            record.imported_tools.get(audit_context.namespaced_tool_id.as_str())
        else {
            return Ok(denied_invocation(
                &request,
                &audit_context,
                "mcp.tool_not_discovered",
                "tool must be discovered and cataloged before invocation",
            ));
        };
        audit_context.result_projection = registry_entry.projection_policy;
        if request.schema_hash != registry_entry.schema_hash {
            return Ok(denied_invocation(
                &request,
                &audit_context,
                "mcp.schema_hash_mismatch",
                "request schema_hash does not match the discovered MCP tool schema",
            ));
        }
        if !request.policy.allowed {
            return Ok(denied_invocation(
                &request,
                &audit_context,
                "mcp.policy_denied",
                request.policy.reason.as_str(),
            ));
        }
        let approval_required = request.policy.approval_required
            || mcp_registry_entry_requires_approval(registry_entry);
        if approval_required && !request.approval_granted {
            return Ok(denied_invocation(
                &request,
                &audit_context,
                "mcp.approval_required",
                "operator approval is required before this MCP tool may execute",
            ));
        }
        if approval_required && !valid_optional_invocation_id(request.approval_id.as_deref()) {
            return Ok(denied_invocation(
                &request,
                &audit_context,
                "mcp.approval_id_required",
                "operator approval must include a bounded approval id",
            ));
        }
        if !request.input.is_object() {
            return Ok(denied_invocation(
                &request,
                &audit_context,
                "mcp.input_not_object",
                "MCP tool input must be a JSON object",
            ));
        }
        match evaluate_mcp_oauth_grant(&record.manifest, current_unix_ms()) {
            Ok(grant_id) => {
                audit_context.oauth_grant_id = grant_id;
            }
            Err(error) => {
                return Ok(denied_invocation_with_hint(
                    &request,
                    &audit_context,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                    Some(error.repair_hint.as_str()),
                ));
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
                return Ok(denied_invocation(
                    &request,
                    &audit_context,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                ));
            }
        }

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
            if !sampling_allowed_by_manifest(
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

    fn record_discovery_protocol_violation(
        &mut self,
        server_name: &str,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        self.record_protocol_violation_with_policy(server_name, false)
    }

    fn record_protocol_violation_with_policy(
        &mut self,
        server_name: &str,
        degrade_before_quarantine: bool,
    ) -> Result<McpServerLifecycleState, McpBrokerError> {
        let server_id = normalize_mcp_identifier(server_name, "server_name")?;
        let record = self.server_record_mut(server_id.as_str())?;
        record.protocol_violations = record.protocol_violations.saturating_add(1);
        if record.protocol_violations >= QUARANTINE_AFTER_VIOLATIONS {
            record.state = McpServerLifecycleState::Quarantined;
        } else if degrade_before_quarantine && record.state == McpServerLifecycleState::Healthy {
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
    validate_oauth_grant(manifest, &mut findings);
    validate_tool_filters(manifest, &mut findings);
    validate_egress_allowlist(manifest.egress_allowlist.as_slice(), &mut findings);
    if manifest.sampling_enabled {
        findings.push(finding(
            McpFindingSeverity::Error,
            "mcp.sampling_denied",
            "legacy MCP sampling toggle is denied for external servers",
            "use sampling_policy.mode=allowlist with explicit model capabilities",
        ));
    }
    validate_sampling_policy(&manifest.sampling_policy, &mut findings);
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

/// Projects MCP discovery reports into the model-visible tool catalog.
///
/// Only reports for enabled, healthy servers in `supervisor_snapshot` supply
/// external registry entries. Filtered tools and unhealthy server entries are
/// still surfaced as catalog availability evidence with their MCP reason codes.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "MCP discovery scheduling will call this service-layer projection entrypoint"
    )
)]
pub(crate) fn build_mcp_tool_catalog_snapshot(
    request: ToolCatalogBuildRequest<'_>,
    supervisor_snapshot: &McpRuntimeSupervisorSnapshot,
    discovery_reports: &[McpToolDiscoveryReport],
) -> ModelVisibleToolCatalogSnapshot {
    let healthy_servers = supervisor_snapshot
        .servers
        .iter()
        .filter(|server| server.enabled && server.state == McpServerLifecycleState::Healthy)
        .map(|server| server.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut registry_entries = Vec::new();
    let mut filtered_entries = Vec::new();
    let mut external_registered_names = Vec::new();

    for report in discovery_reports {
        let server_id = normalize_mcp_identifier(report.server_name.as_str(), "server_name")
            .unwrap_or_else(|_| report.server_name.trim().to_ascii_lowercase());
        let server_healthy = report.state == McpServerLifecycleState::Healthy
            && healthy_servers.contains(server_id.as_str());
        if server_healthy {
            registry_entries.extend(report.registry_entries.iter().cloned());
        } else {
            for entry in &report.registry_entries {
                external_registered_names.push(entry.name.clone());
                filtered_entries.push(mcp_catalog_filtered_entry(
                    entry.name.clone(),
                    ToolCatalogFilterReasonCode::RuntimeUnavailable,
                    "mcp.server_not_healthy",
                    format!(
                        "MCP server '{}' is not enabled and healthy in the supervisor snapshot",
                        report.server_name
                    ),
                ));
            }
        }
        for filtered_tool in &report.filtered_tools {
            let name = mcp_filtered_catalog_tool_name(
                report.server_name.as_str(),
                filtered_tool.raw_name.as_str(),
            );
            external_registered_names.push(name.clone());
            filtered_entries.push(mcp_catalog_filtered_entry(
                name,
                ToolCatalogFilterReasonCode::ProviderSchemaIncompatible,
                filtered_tool.reason_code.as_str(),
                filtered_tool.message.clone(),
            ));
        }
    }

    registry_entries.sort_by(|left, right| left.name.cmp(&right.name));
    registry_entries.dedup_by(|left, right| left.name == right.name);
    filtered_entries.sort_by(|left, right| {
        left.name.cmp(&right.name).then(
            left.external_reason_code
                .as_deref()
                .unwrap_or_default()
                .cmp(right.external_reason_code.as_deref().unwrap_or_default()),
        )
    });
    filtered_entries.dedup_by(|left, right| {
        left.name == right.name && left.external_reason_code == right.external_reason_code
    });
    external_registered_names.sort();
    external_registered_names.dedup();

    build_model_visible_tool_catalog_snapshot_with_external_records(
        request,
        registry_entries.as_slice(),
        filtered_entries.as_slice(),
        external_registered_names.as_slice(),
    )
}

fn mcp_catalog_filtered_entry(
    name: String,
    reason_code: ToolCatalogFilterReasonCode,
    external_reason_code: &str,
    repair_hint: String,
) -> FilteredToolCatalogEntry {
    FilteredToolCatalogEntry {
        name,
        reason_code,
        external_reason_code: Some(external_reason_code.to_owned()),
        repair_hint,
    }
}

fn mcp_filtered_catalog_tool_name(server_name: &str, raw_tool_name: &str) -> String {
    namespaced_tool_id(server_name, raw_tool_name).unwrap_or_else(|_| {
        let server = normalize_mcp_identifier(server_name, "server_name")
            .unwrap_or_else(|_| "unknown".to_owned());
        let digest = stable_hash_bytes(raw_tool_name.as_bytes());
        format!("mcp.{server}.filtered_{}", &digest[..12])
    })
}

fn execute_remote_jsonrpc(
    manifest: &McpServerManifest,
    method: &str,
    params: Value,
    timeout_ms: u64,
) -> Result<Value, McpTransportError> {
    let (url, expects_sse) = match &manifest.transport {
        McpTransportManifest::Http { url } => (url.as_str(), false),
        McpTransportManifest::Sse { url } => (url.as_str(), true),
        McpTransportManifest::Stdio { .. } => {
            return Err(McpTransportError::new(
                "mcp.transport_mismatch",
                "remote transport requested for stdio manifest",
            ));
        }
    };
    let url = Url::parse(url).map_err(|error| {
        McpTransportError::new(
            "mcp.transport_invalid_url",
            format!("MCP remote transport URL is invalid: {error}"),
        )
    })?;
    let egress_verdict = evaluate_mcp_remote_egress(manifest, &url)?;
    let response = send_mcp_remote_jsonrpc_request(
        &url,
        &egress_verdict.resolved_addresses,
        mcp_jsonrpc_request(1, method, params),
        timeout_ms,
    )?;
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    if !response.status().is_success() {
        return Err(McpTransportError::new(
            "mcp.transport_http_status",
            format!("MCP remote transport returned HTTP {}", response.status().as_u16()),
        ));
    }
    let body = read_bounded_remote_body(response, manifest.max_response_bytes)?;
    let payload = parse_mcp_remote_body(body.as_slice(), content_type.as_str(), expects_sse)?;
    mcp_jsonrpc_result(payload)
}

fn evaluate_mcp_remote_egress(
    manifest: &McpServerManifest,
    url: &Url,
) -> Result<palyra_egress_proxy::EgressPolicyVerdict, McpTransportError> {
    if manifest.egress_allowlist.is_empty() {
        return Err(McpTransportError::new(
            "mcp.egress_allowlist_missing",
            "MCP remote transport requires a non-empty egress allowlist",
        ));
    }
    EgressProxyPolicyService
        .evaluate_request(&EgressProxyRequest {
            method: "POST",
            url: url.as_str(),
            allow_private_targets: mcp_remote_url_targets_loopback(url),
            allowed_hosts: manifest.egress_allowlist.as_slice(),
            allowed_dns_suffixes: &[],
            max_response_bytes: manifest.max_response_bytes,
            credential_bindings: &[],
        })
        .map_err(|error| {
            McpTransportError::new(
                "mcp.egress_denied",
                format!("MCP remote transport target blocked by egress policy: {error}"),
            )
        })
}

fn send_mcp_remote_jsonrpc_request(
    url: &Url,
    resolved_addresses: &[std::net::SocketAddr],
    payload: Value,
    timeout_ms: u64,
) -> Result<Response, McpTransportError> {
    let host = url.host_str().unwrap_or_default();
    let timeout = std::time::Duration::from_millis(timeout_ms);
    let mut client_builder = reqwest::blocking::Client::builder()
        .redirect(Policy::none())
        .connect_timeout(timeout)
        .timeout(timeout);
    if !host.is_empty() && host.parse::<IpAddr>().is_err() {
        for address in resolved_addresses {
            client_builder = client_builder.resolve(host, *address);
        }
    }
    let client = client_builder.build().map_err(|error| {
        McpTransportError::new(
            "mcp.transport_client_build_failed",
            format!("MCP remote transport client build failed: {error}"),
        )
    })?;
    let request_body = serde_json::to_vec(&payload).map_err(|error| {
        McpTransportError::new(
            "mcp.transport_request_encode_failed",
            format!("MCP JSON-RPC request serialization failed: {error}"),
        )
    })?;
    client
        .post(url.clone())
        .header(reqwest::header::ACCEPT, "application/json, text/event-stream")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(request_body)
        .send()
        .map_err(|error| {
            McpTransportError::new(
                "mcp.transport_http_failed",
                format!("MCP remote transport request failed: {error}"),
            )
        })
}

fn read_bounded_remote_body(
    mut response: Response,
    max_response_bytes: usize,
) -> Result<Vec<u8>, McpTransportError> {
    let mut body = Vec::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = response.read(&mut buffer).map_err(|error| {
            McpTransportError::new(
                "mcp.transport_read_failed",
                format!("MCP remote transport response read failed: {error}"),
            )
        })?;
        if read == 0 {
            break;
        }
        if body.len().saturating_add(read) > max_response_bytes {
            return Err(McpTransportError::new(
                "mcp.transport_response_too_large",
                format!("MCP remote transport response exceeded {max_response_bytes} bytes"),
            ));
        }
        body.extend_from_slice(&buffer[..read]);
    }
    Ok(body)
}

fn parse_mcp_remote_body(
    body: &[u8],
    content_type: &str,
    expects_sse: bool,
) -> Result<Value, McpTransportError> {
    let content_type =
        content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    if expects_sse || content_type == "text/event-stream" {
        let body = std::str::from_utf8(body).map_err(|error| {
            McpTransportError::new(
                "mcp.transport_invalid_response",
                format!("MCP SSE response was not UTF-8: {error}"),
            )
        })?;
        return parse_mcp_sse_response(body);
    }
    serde_json::from_slice::<Value>(body).map_err(|error| {
        McpTransportError::new(
            "mcp.transport_invalid_response",
            format!("MCP JSON-RPC response was not valid JSON: {error}"),
        )
    })
}

fn parse_mcp_sse_response(body: &str) -> Result<Value, McpTransportError> {
    let mut event_data = String::new();
    for line in body.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            if let Some(value) = parse_mcp_sse_event(event_data.as_str())? {
                return Ok(value);
            }
            event_data.clear();
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        if let Some(data) = line.strip_prefix("data:") {
            if !event_data.is_empty() {
                event_data.push('\n');
            }
            event_data.push_str(data.trim_start());
        }
    }
    if let Some(value) = parse_mcp_sse_event(event_data.as_str())? {
        return Ok(value);
    }
    Err(McpTransportError::new(
        "mcp.transport_invalid_response",
        "MCP SSE response did not contain a JSON data event",
    ))
}

fn parse_mcp_sse_event(data: &str) -> Result<Option<Value>, McpTransportError> {
    let data = data.trim();
    if data.is_empty() || data == "[DONE]" {
        return Ok(None);
    }
    serde_json::from_str::<Value>(data).map(Some).map_err(|error| {
        McpTransportError::new(
            "mcp.transport_invalid_response",
            format!("MCP SSE data event was not valid JSON: {error}"),
        )
    })
}

fn execute_stdio_jsonrpc(
    manifest: &McpServerManifest,
    operation: Option<(&'static str, Value)>,
    timeout_ms: u64,
) -> Result<Value, McpTransportError> {
    let McpTransportManifest::Stdio { command, env } = &manifest.transport else {
        return Err(McpTransportError::new(
            "mcp.transport_mismatch",
            "stdio transport requested for remote manifest",
        ));
    };
    let command = command.clone();
    let env = env.clone();
    let max_response_bytes = manifest.max_response_bytes;
    run_transport_future(async move {
        tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            execute_stdio_session(command, env, operation, max_response_bytes),
        )
        .await
        .map_err(|_| {
            McpTransportError::new(
                "mcp.transport_timeout",
                format!("MCP stdio transport timed out after {timeout_ms} ms"),
            )
        })?
    })
}

async fn execute_stdio_session(
    command: Vec<String>,
    env: BTreeMap<String, String>,
    operation: Option<(&'static str, Value)>,
    max_response_bytes: usize,
) -> Result<Value, McpTransportError> {
    let (program, args) = command.split_first().ok_or_else(|| {
        McpTransportError::new("mcp.stdio_command_empty", "MCP stdio command is empty")
    })?;
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    apply_stdio_environment(&mut command, &env)?;
    let mut child = command.spawn().map_err(|error| {
        McpTransportError::new(
            "mcp.transport_spawn_failed",
            format!("MCP stdio transport failed to spawn process: {error}"),
        )
    })?;
    let mut stdin = child.stdin.take().ok_or_else(|| {
        McpTransportError::new("mcp.transport_pipe_missing", "MCP stdio stdin pipe is missing")
    })?;
    let mut stdout = child.stdout.take().ok_or_else(|| {
        McpTransportError::new("mcp.transport_pipe_missing", "MCP stdio stdout pipe is missing")
    })?;
    let stderr = child.stderr.take();
    let stderr_task = stderr
        .map(|stderr| tokio::spawn(read_stdio_stderr_tail(stderr, MCP_STDIO_STDERR_TAIL_BYTES)));

    let result = async {
        write_mcp_stdio_message(
            &mut stdin,
            &mcp_jsonrpc_request(1, "initialize", mcp_initialize_params()),
        )
        .await?;
        let initialized = read_mcp_stdio_message(&mut stdout, max_response_bytes).await?;
        let initialized = mcp_jsonrpc_result(initialized)?;
        write_mcp_stdio_message(&mut stdin, &mcp_initialized_notification()).await?;
        if let Some((method, params)) = operation {
            write_mcp_stdio_message(&mut stdin, &mcp_jsonrpc_request(2, method, params)).await?;
            let response = read_mcp_stdio_message(&mut stdout, max_response_bytes).await?;
            mcp_jsonrpc_result(response)
        } else {
            Ok(initialized)
        }
    }
    .await;

    drop(stdin);
    let _ = child.kill().await;
    let _ = child.wait().await;
    if let Some(stderr_task) = stderr_task {
        let stderr_tail = stderr_task.await.ok().unwrap_or_default();
        if result.is_err() && !stderr_tail.trim().is_empty() {
            return result.map_err(|error| {
                McpTransportError::new(
                    error.reason_code,
                    format!("{}; stderr: {}", error.message, stderr_tail.trim()),
                )
            });
        }
    }
    result
}

fn apply_stdio_environment(
    command: &mut tokio::process::Command,
    env_values: &BTreeMap<String, String>,
) -> Result<(), McpTransportError> {
    command.env_clear();
    for key in MCP_STDIO_INHERITED_ENV_ALLOWLIST {
        if let Some(value) = env::var_os(key) {
            command.env(key, value);
        }
    }
    for (key, value) in env_values {
        if is_vault_ref(value.as_str()) {
            return Err(McpTransportError::new(
                "mcp.vault_ref_unresolved",
                format!("MCP stdio environment variable '{key}' requires vault resolution"),
            ));
        }
        command.env(key, value);
    }
    Ok(())
}

async fn write_mcp_stdio_message<W>(
    writer: &mut W,
    message: &Value,
) -> Result<(), McpTransportError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let body = serde_json::to_vec(message).map_err(|error| {
        McpTransportError::new(
            "mcp.transport_request_encode_failed",
            format!("MCP stdio JSON-RPC request serialization failed: {error}"),
        )
    })?;
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    tokio::io::AsyncWriteExt::write_all(writer, header.as_bytes()).await.map_err(|error| {
        McpTransportError::new(
            "mcp.transport_write_failed",
            format!("MCP stdio header write failed: {error}"),
        )
    })?;
    tokio::io::AsyncWriteExt::write_all(writer, body.as_slice()).await.map_err(|error| {
        McpTransportError::new(
            "mcp.transport_write_failed",
            format!("MCP stdio body write failed: {error}"),
        )
    })?;
    tokio::io::AsyncWriteExt::flush(writer).await.map_err(|error| {
        McpTransportError::new(
            "mcp.transport_write_failed",
            format!("MCP stdio flush failed: {error}"),
        )
    })
}

async fn read_mcp_stdio_message<R>(
    reader: &mut R,
    max_response_bytes: usize,
) -> Result<Value, McpTransportError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut header = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        let read = tokio::io::AsyncReadExt::read(reader, &mut byte).await.map_err(|error| {
            McpTransportError::new(
                "mcp.transport_read_failed",
                format!("MCP stdio header read failed: {error}"),
            )
        })?;
        if read == 0 {
            return Err(McpTransportError::new(
                "mcp.transport_eof",
                "MCP stdio server closed stdout before a complete response header",
            ));
        }
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
        if header.len() > MCP_STDIO_MAX_HEADER_BYTES {
            return Err(McpTransportError::new(
                "mcp.transport_header_too_large",
                format!("MCP stdio response header exceeded {MCP_STDIO_MAX_HEADER_BYTES} bytes"),
            ));
        }
    }
    let content_length = mcp_stdio_content_length(header.as_slice())?;
    if content_length > max_response_bytes {
        return Err(McpTransportError::new(
            "mcp.transport_response_too_large",
            format!("MCP stdio response exceeded {max_response_bytes} bytes"),
        ));
    }
    let mut body = vec![0_u8; content_length];
    let mut offset = 0_usize;
    while offset < content_length {
        let read =
            tokio::io::AsyncReadExt::read(reader, &mut body[offset..]).await.map_err(|error| {
                McpTransportError::new(
                    "mcp.transport_read_failed",
                    format!("MCP stdio body read failed: {error}"),
                )
            })?;
        if read == 0 {
            return Err(McpTransportError::new(
                "mcp.transport_eof",
                "MCP stdio server closed stdout before a complete response body",
            ));
        }
        offset = offset.saturating_add(read);
    }
    serde_json::from_slice::<Value>(body.as_slice()).map_err(|error| {
        McpTransportError::new(
            "mcp.transport_invalid_response",
            format!("MCP stdio response was not valid JSON: {error}"),
        )
    })
}

async fn read_stdio_stderr_tail<R>(mut reader: R, max_bytes: usize) -> String
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut buffer = [0_u8; 1024];
    while let Ok(read) = tokio::io::AsyncReadExt::read(&mut reader, &mut buffer).await {
        if read == 0 {
            break;
        }
        output.extend_from_slice(&buffer[..read]);
        if output.len() > max_bytes {
            let keep_from = output.len().saturating_sub(max_bytes);
            output.drain(..keep_from);
        }
    }
    sanitize_mcp_transport_message(String::from_utf8_lossy(output.as_slice()).as_ref())
}

fn mcp_stdio_content_length(header: &[u8]) -> Result<usize, McpTransportError> {
    let header = std::str::from_utf8(header).map_err(|error| {
        McpTransportError::new(
            "mcp.transport_invalid_response",
            format!("MCP stdio response header was not UTF-8: {error}"),
        )
    })?;
    for line in header.split("\r\n") {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("content-length") {
            return value.trim().parse::<usize>().map_err(|error| {
                McpTransportError::new(
                    "mcp.transport_invalid_response",
                    format!("MCP stdio content-length was invalid: {error}"),
                )
            });
        }
    }
    Err(McpTransportError::new(
        "mcp.transport_invalid_response",
        "MCP stdio response header did not include content-length",
    ))
}

fn run_transport_future<F, T>(future: F) -> Result<T, McpTransportError>
where
    F: Future<Output = Result<T, McpTransportError>> + Send + 'static,
    T: Send + 'static,
{
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|error| {
                McpTransportError::new(
                    "mcp.transport_runtime_failed",
                    format!("MCP stdio runtime initialization failed: {error}"),
                )
            })
            .and_then(|runtime| runtime.block_on(future));
        let _ = sender.send(result);
    });
    receiver.recv().map_err(|error| {
        McpTransportError::new(
            "mcp.transport_runtime_failed",
            format!("MCP stdio runtime worker failed: {error}"),
        )
    })?
}

fn mcp_jsonrpc_request(id: u64, method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": MCP_JSONRPC_VERSION,
        "id": id,
        "method": method,
        "params": params,
    })
}

fn mcp_initialized_notification() -> Value {
    json!({
        "jsonrpc": MCP_JSONRPC_VERSION,
        "method": "notifications/initialized",
        "params": {},
    })
}

fn mcp_initialize_params() -> Value {
    json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": {
            "name": "palyra",
            "version": env!("CARGO_PKG_VERSION"),
        },
    })
}

fn mcp_jsonrpc_result(response: Value) -> Result<Value, McpTransportError> {
    if response.get("jsonrpc").and_then(Value::as_str) != Some(MCP_JSONRPC_VERSION) {
        return Err(McpTransportError::new(
            "mcp.transport_invalid_response",
            "MCP response missing JSON-RPC version",
        ));
    }
    if let Some(error) = response.get("error") {
        let code = error.get("code").and_then(Value::as_i64).unwrap_or_default();
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("MCP server returned a JSON-RPC error");
        return Err(McpTransportError::new(
            "mcp.transport_rpc_error",
            format!("MCP server returned JSON-RPC error {code}: {message}"),
        ));
    }
    response.get("result").cloned().ok_or_else(|| {
        McpTransportError::new("mcp.transport_invalid_response", "MCP response missing result")
    })
}

fn tools_from_mcp_result(result: &Value) -> Result<Vec<McpDiscoveredTool>, McpTransportError> {
    let tools = result.get("tools").and_then(Value::as_array).ok_or_else(|| {
        McpTransportError::new(
            "mcp.transport_invalid_response",
            "MCP tools/list result missing tools array",
        )
    })?;
    let mut discovered = Vec::with_capacity(tools.len());
    for tool in tools {
        let name = tool.get("name").and_then(Value::as_str).ok_or_else(|| {
            McpTransportError::new(
                "mcp.transport_invalid_response",
                "MCP discovered tool missing string name",
            )
        })?;
        let description =
            tool.get("description").and_then(Value::as_str).unwrap_or_default().to_owned();
        let input_schema = tool
            .get("inputSchema")
            .or_else(|| tool.get("input_schema"))
            .cloned()
            .unwrap_or_else(|| json!({"type": "object"}));
        discovered.push(McpDiscoveredTool {
            name: name.to_owned(),
            description,
            input_schema,
            capabilities: Vec::new(),
            sensitivity: None,
            approval_policy: None,
        });
    }
    Ok(discovered)
}

fn mcp_remote_url_targets_loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
    })
}

fn sanitize_mcp_transport_message(message: &str) -> String {
    redact_vault_refs(redact_diagnostic_text(message).as_str())
        .replace("\r\n", "\n")
        .replace('\r', "\n")
}

fn redact_vault_refs(message: &str) -> String {
    let mut output = String::with_capacity(message.len());
    let mut cursor = 0_usize;
    while cursor < message.len() {
        let rest = &message[cursor..];
        let marker_len = if rest.starts_with("vault://") {
            Some("vault://".len())
        } else if rest.starts_with("vault:") {
            Some("vault:".len())
        } else {
            None
        };
        let Some(marker_len) = marker_len else {
            let Some(ch) = rest.chars().next() else {
                break;
            };
            output.push(ch);
            cursor = cursor.saturating_add(ch.len_utf8());
            continue;
        };
        output.push_str("<redacted-vault-ref>");
        cursor = cursor.saturating_add(marker_len);
        while cursor < message.len() {
            let Some(ch) = message[cursor..].chars().next() else {
                break;
            };
            if ch.is_whitespace() || matches!(ch, '"' | '\'' | ',' | ')' | ']' | '}' | '<' | '>') {
                break;
            }
            cursor = cursor.saturating_add(ch.len_utf8());
        }
    }
    output
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

fn mcp_discovery_filter_is_protocol_violation(tool: &McpFilteredTool) -> bool {
    tool.reason_code.starts_with("schema.")
        || matches!(tool.reason_code.as_str(), "mcp.identifier_invalid" | "mcp.tool_collision")
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
        replay_safety_class: if approval_posture == ToolApprovalPosture::ApprovalRequired {
            ToolReplaySafetyClass::RequiresHumanConfirmation
        } else {
            ToolReplaySafetyClass::ExternalSideEffect
        },
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

fn validate_oauth_grant(manifest: &McpServerManifest, findings: &mut Vec<McpValidationFinding>) {
    let Some(grant) = manifest.oauth_grant.as_ref() else {
        if manifest.oauth_required {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.oauth_grant_missing",
                "OAuth is required but no MCP OAuth grant is configured",
                "run `palyra mcp login <server>` to create vault-backed grant references",
            ));
        }
        return;
    };
    if normalize_mcp_identifier(grant.grant_id.as_str(), "oauth_grant_id").is_err() {
        findings.push(finding(
            McpFindingSeverity::Error,
            "mcp.oauth_grant_id_invalid",
            "OAuth grant id must be a bounded identifier",
            "regenerate the MCP OAuth grant with `palyra mcp login`",
        ));
    }
    for (label, vault_ref) in [
        ("access_token_vault_ref", Some(grant.access_token_vault_ref.as_str())),
        ("metadata_vault_ref", Some(grant.metadata_vault_ref.as_str())),
        ("refresh_token_vault_ref", grant.refresh_token_vault_ref.as_deref()),
    ] {
        let Some(vault_ref) = vault_ref else {
            continue;
        };
        if !is_mcp_vault_ref_descriptor(vault_ref) {
            findings.push(finding(
                McpFindingSeverity::Error,
                "mcp.oauth_vault_ref_invalid",
                format!("OAuth {label} must be a vault reference").as_str(),
                "store OAuth material in the vault and keep only vault refs in config",
            ));
        }
    }
    if grant.issued_at_unix_ms < 0 || grant.updated_at_unix_ms < 0 {
        findings.push(finding(
            McpFindingSeverity::Error,
            "mcp.oauth_timestamp_invalid",
            "OAuth grant timestamps must be non-negative unix milliseconds",
            "regenerate the MCP OAuth grant metadata",
        ));
    }
}

fn validate_sampling_policy(policy: &McpSamplingPolicy, findings: &mut Vec<McpValidationFinding>) {
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

#[derive(Debug, Clone)]
struct McpOAuthGrantEvaluationError {
    reason_code: String,
    message: String,
    repair_hint: String,
}

fn evaluate_mcp_oauth_grant(
    manifest: &McpServerManifest,
    now_unix_ms: i64,
) -> Result<Option<String>, McpOAuthGrantEvaluationError> {
    if !manifest.oauth_required {
        return Ok(None);
    }
    let repair_hint =
        format!("run `palyra mcp login {}` to refresh the MCP OAuth grant", manifest.name);
    let Some(grant) = manifest.oauth_grant.as_ref() else {
        return Err(McpOAuthGrantEvaluationError {
            reason_code: "mcp.oauth_grant_missing".to_owned(),
            message: "MCP OAuth grant is missing".to_owned(),
            repair_hint,
        });
    };
    if grant.revoked_at_unix_ms.is_some() {
        return Err(McpOAuthGrantEvaluationError {
            reason_code: "mcp.oauth_grant_revoked".to_owned(),
            message: "MCP OAuth grant was revoked".to_owned(),
            repair_hint,
        });
    }
    if grant.expires_at_unix_ms.is_some_and(|expires_at| expires_at <= now_unix_ms) {
        return Err(McpOAuthGrantEvaluationError {
            reason_code: "mcp.oauth_grant_expired".to_owned(),
            message: "MCP OAuth grant has expired".to_owned(),
            repair_hint,
        });
    }
    Ok(Some(grant.grant_id.clone()))
}

/// Returns doctor-style findings for MCP OAuth grants that cannot authorize tools.
#[must_use]
pub fn mcp_oauth_grant_doctor_findings(
    manifests: &[McpServerManifest],
    now_unix_ms: i64,
) -> Vec<McpValidationFinding> {
    manifests
        .iter()
        .filter_map(|manifest| {
            evaluate_mcp_oauth_grant(manifest, now_unix_ms).err().map(|error| {
                finding(
                    McpFindingSeverity::Error,
                    error.reason_code.as_str(),
                    error.message.as_str(),
                    error.repair_hint.as_str(),
                )
            })
        })
        .collect()
}

fn sampling_allowed_by_manifest(
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

fn normalize_sampling_model_capability(raw: &str) -> Result<String, McpBrokerError> {
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

#[derive(Debug, Clone)]
struct McpInvocationAuditContext {
    server_id: String,
    namespaced_tool_id: String,
    transport_id: String,
    result_projection: ToolResultProjectionPolicy,
    oauth_grant_id: Option<String>,
    sampling_model_capability: Option<String>,
    vault_grant_ids: Vec<String>,
}

impl McpInvocationAuditContext {
    fn new(
        server_id: String,
        namespaced_tool_id: String,
        transport_id: String,
        result_projection: ToolResultProjectionPolicy,
    ) -> Self {
        Self {
            server_id,
            namespaced_tool_id,
            transport_id,
            result_projection,
            oauth_grant_id: None,
            sampling_model_capability: None,
            vault_grant_ids: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct McpVaultGrantResolutionError {
    reason_code: String,
    message: String,
}

#[derive(Debug, Clone)]
struct ProjectedMcpOutput {
    output_json: Value,
    output_truncated: bool,
}

fn resolve_scoped_vault_grants(
    requested: &[String],
    manifest_grants: &[McpVaultRefGrant],
    scoped_grants: &[McpVaultScopedGrant],
) -> Result<Vec<String>, McpVaultGrantResolutionError> {
    if let Some(denied) = first_ungranted_vault_ref(requested, manifest_grants) {
        return Err(McpVaultGrantResolutionError {
            reason_code: "mcp.vault_ref_not_granted".to_owned(),
            message: format!("vault reference '{denied}' is not granted by the MCP manifest"),
        });
    }

    let scoped_by_name = scoped_grants
        .iter()
        .map(|grant| (grant.name.trim().to_ascii_lowercase(), grant.grant_id.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut grant_ids = Vec::new();
    for requested_ref in requested {
        let name = requested_ref.trim().to_ascii_lowercase();
        let Some(grant_id) = scoped_by_name.get(name.as_str()) else {
            return Err(McpVaultGrantResolutionError {
                reason_code: "mcp.vault_scoped_grant_missing".to_owned(),
                message: format!("vault reference '{requested_ref}' requires a scoped grant id"),
            });
        };
        let grant_id = *grant_id;
        if !valid_optional_invocation_id(Some(grant_id)) || is_vault_ref(grant_id) {
            return Err(McpVaultGrantResolutionError {
                reason_code: "mcp.vault_scoped_grant_invalid".to_owned(),
                message: format!(
                    "vault reference '{requested_ref}' has an invalid scoped grant id"
                ),
            });
        }
        if sanitize_mcp_transport_message(grant_id) != grant_id {
            return Err(McpVaultGrantResolutionError {
                reason_code: "mcp.vault_scoped_grant_invalid".to_owned(),
                message: format!("vault reference '{requested_ref}' has an unsafe scoped grant id"),
            });
        }
        grant_ids.push(grant_id.to_owned());
    }
    grant_ids.sort();
    grant_ids.dedup();
    Ok(grant_ids)
}

fn mcp_registry_entry_requires_approval(entry: &ToolRegistryEntry) -> bool {
    entry.approval_posture == ToolApprovalPosture::ApprovalRequired
        || entry.capabilities.iter().any(|capability| {
            matches!(
                capability.trim().to_ascii_lowercase().as_str(),
                "approval:require_approval"
                    | "sensitivity:sensitive"
                    | "sensitivity:secret"
                    | "filesystem_write"
                    | "secrets_read"
                    | "process_exec"
                    | "network"
                    | "egress"
                    | "external_side_effect"
                    | "write"
                    | "delete"
                    | "mutation"
                    | "mutating"
            )
        })
}

fn valid_optional_invocation_id(value: Option<&str>) -> bool {
    let Some(value) = value.map(str::trim) else {
        return false;
    };
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':' | '/'))
}

fn project_mcp_output(
    output: &Value,
    max_response_bytes: usize,
    projection_policy: ToolResultProjectionPolicy,
) -> ProjectedMcpOutput {
    let bytes = serde_json::to_vec(output).unwrap_or_else(|_| b"null".to_vec());
    let output_truncated = bytes.len() > max_response_bytes;
    let redacted_preview =
        redacted_json_preview(output, max_response_bytes.min(DEFAULT_MAX_RESPONSE_BYTES));
    let output_sha256 = stable_hash_value(output);
    let output_json = match projection_policy {
        ToolResultProjectionPolicy::InlineUnlessLarge if !output_truncated => {
            redact_value_for_model(output)
        }
        ToolResultProjectionPolicy::InlineUnlessLarge => json!({
            "artifact_required": true,
            "redacted_preview": redacted_preview,
            "raw_output_sha256": output_sha256,
        }),
        ToolResultProjectionPolicy::SummarizeAndArtifact => json!({
            "artifact_required": output_truncated,
            "summary": redacted_preview,
            "raw_output_sha256": output_sha256,
        }),
        ToolResultProjectionPolicy::RedactedPreviewAndArtifact => json!({
            "artifact_required": output_truncated,
            "redacted_preview": redacted_preview,
            "raw_output_sha256": output_sha256,
        }),
    };
    ProjectedMcpOutput { output_json, output_truncated }
}

fn redact_value_for_model(output: &Value) -> Value {
    let serialized = serde_json::to_string(output).unwrap_or_else(|_| "null".to_owned());
    let redacted = sanitize_mcp_transport_message(serialized.as_str());
    serde_json::from_str(redacted.as_str())
        .unwrap_or_else(|_| json!({ "redacted_preview": redacted }))
}

fn redacted_json_preview(output: &Value, max_bytes: usize) -> String {
    let serialized = serde_json::to_string(output).unwrap_or_else(|_| "null".to_owned());
    let redacted = sanitize_mcp_transport_message(serialized.as_str());
    truncate_at_char_boundary(redacted.as_str(), max_bytes)
}

fn truncate_at_char_boundary(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = value[..end].to_owned();
    truncated.push_str("...");
    truncated
}

fn transport_id_for_manifest(manifest: &McpServerManifest) -> String {
    let payload = match &manifest.transport {
        McpTransportManifest::Stdio { command, .. } => json!({
            "kind": "stdio",
            "command": command,
            "env": scrub_stdio_env(manifest),
        }),
        McpTransportManifest::Http { url } => json!({
            "kind": "http",
            "url": sanitize_mcp_transport_message(url),
        }),
        McpTransportManifest::Sse { url } => json!({
            "kind": "sse",
            "url": sanitize_mcp_transport_message(url),
        }),
    };
    let transport_hash = stable_hash_value(&payload);
    format!("mcp.transport.{}", &transport_hash[..16])
}

fn denied_invocation(
    request: &McpToolCallRequest,
    audit_context: &McpInvocationAuditContext,
    reason_code: &str,
    message: &str,
) -> McpToolInvocationOutcome {
    denied_invocation_with_hint(request, audit_context, reason_code, message, None)
}

fn denied_invocation_with_hint(
    request: &McpToolCallRequest,
    audit_context: &McpInvocationAuditContext,
    reason_code: &str,
    message: &str,
    repair_hint: Option<&str>,
) -> McpToolInvocationOutcome {
    let message = sanitize_mcp_transport_message(message);
    let mut output_json = json!({
        "success": false,
        "reason_code": reason_code,
        "message": message,
    });
    if let Some(repair_hint) = repair_hint {
        output_json["repair_hint"] = json!(sanitize_mcp_transport_message(repair_hint));
    }
    McpToolInvocationOutcome {
        success: false,
        output_json: output_json.clone(),
        error: Some(message.to_owned()),
        attestation: invocation_attestation(
            request,
            audit_context,
            &output_json,
            reason_code,
            false,
        ),
    }
}

fn invocation_attestation(
    request: &McpToolCallRequest,
    audit_context: &McpInvocationAuditContext,
    raw_output_json: &Value,
    policy_outcome: &str,
    output_truncated: bool,
) -> McpInvocationAttestation {
    let executed_at_unix_ms = current_unix_ms();
    let input_hash = stable_hash_value(&request.input);
    let output_hash = stable_hash_value(raw_output_json);
    let attestation_seed = format!(
        "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        audit_context.server_id,
        request.server_name,
        request.tool_name,
        request.schema_hash,
        input_hash,
        output_hash,
        request.approval_id.as_deref().unwrap_or_default(),
        audit_context.oauth_grant_id.as_deref().unwrap_or_default(),
        audit_context.transport_id,
        audit_context.vault_grant_ids.join(","),
        audit_context.sampling_model_capability.as_deref().unwrap_or_default(),
        policy_outcome,
        executed_at_unix_ms
    );
    McpInvocationAttestation {
        attestation_id: format!("mcpatt_{}", &stable_hash_bytes(attestation_seed.as_bytes())[..16]),
        server_id: audit_context.server_id.clone(),
        server_name: request.server_name.clone(),
        tool_name: request.tool_name.clone(),
        namespaced_tool_id: audit_context.namespaced_tool_id.clone(),
        schema_hash: request.schema_hash.clone(),
        input_hash,
        output_hash,
        approval_id: request.approval_id.clone(),
        oauth_grant_id: audit_context.oauth_grant_id.clone(),
        transport_id: audit_context.transport_id.clone(),
        result_projection: audit_context.result_projection.as_str().to_owned(),
        policy_outcome: policy_outcome.to_owned(),
        sampling_model_capability: audit_context.sampling_model_capability.clone(),
        executed_at_unix_ms,
        output_truncated,
        vault_grant_ids: audit_context.vault_grant_ids.clone(),
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

fn is_mcp_vault_ref_descriptor(value: &str) -> bool {
    let value = value.trim();
    is_vault_ref(value)
        || value.split_once('/').is_some_and(|(scope, key)| {
            !scope.trim().is_empty()
                && !key.trim().is_empty()
                && !key.contains('/')
                && key.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        })
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
    use std::{
        cell::Cell,
        io::{Read as _, Write as _},
        net::TcpListener,
        path::Path,
        process::Command,
        time::Duration,
    };

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
                sampling_model_capability: None,
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
            oauth_required: false,
            oauth_grant: None,
            sampling_policy: McpSamplingPolicy::default(),
        }
    }

    fn remote_manifest(transport: McpTransportManifest) -> McpServerManifest {
        let mut manifest = manifest();
        manifest.transport = transport;
        manifest.egress_allowlist = vec!["127.0.0.1".to_owned()];
        manifest.max_response_bytes = 4 * 1024;
        manifest
    }

    fn discovered_tool(name: &str) -> McpDiscoveredTool {
        McpDiscoveredTool {
            name: name.to_owned(),
            description: format!("{name} test tool"),
            input_schema: search_tool_schema(),
            capabilities: vec!["docs".to_owned()],
            sensitivity: None,
            approval_policy: None,
        }
    }

    fn discovered_tool_with_schema(name: &str, input_schema: Value) -> McpDiscoveredTool {
        McpDiscoveredTool {
            name: name.to_owned(),
            description: format!("{name} test tool"),
            input_schema,
            capabilities: vec!["docs".to_owned()],
            sensitivity: None,
            approval_policy: None,
        }
    }

    fn search_tool_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "query": { "type": "string" }
            },
            "required": ["query"],
            "additionalProperties": false
        })
    }

    fn broker_with_ready_manifest(transport: &dyn McpTransport) -> McpBroker {
        let mut broker = McpBroker::new(policy());
        broker.register_manifest(manifest()).expect("manifest should register");
        broker.start_server("docs", transport).expect("server should start");
        broker
    }

    fn broker_with_discovered_manifest(transport: &dyn McpTransport) -> McpBroker {
        let mut broker = broker_with_ready_manifest(transport);
        broker.discover_tools("docs", transport).expect("tool discovery should run");
        broker
    }

    fn invocation_request() -> McpToolCallRequest {
        McpToolCallRequest {
            server_name: "docs".to_owned(),
            tool_name: "search".to_owned(),
            input: json!({"query": "rust"}),
            schema_hash: stable_hash_value(&search_tool_schema()),
            policy: McpInvocationPolicyDecision {
                allowed: true,
                approval_required: false,
                reason: "allowlisted".to_owned(),
            },
            approval_granted: false,
            approval_id: None,
            vault_refs_requested: vec!["api_token".to_owned()],
            vault_scoped_grants: vec![McpVaultScopedGrant {
                name: "api_token".to_owned(),
                grant_id: "grant.docs.api_token.01".to_owned(),
            }],
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
            oauth_required: false,
            oauth_grant: None,
            sampling_policy: crate::config::McpServerSamplingPolicy::default(),
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

        assert_eq!(snapshot.schema_version, 2);
        assert_eq!(snapshot.generated_at_unix_ms, 42);
        assert_eq!(snapshot.catalog_generation, 0);
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
        assert_eq!(server.last_error_class, Some(McpRuntimeErrorClass::TransportFlapping));
        assert_eq!(server.quarantine_reason.as_deref(), Some("transport_flapping"));
        assert!(!server.catalog_available);
        assert_eq!(server.catalog_hidden_reason.as_deref(), Some("mcp.server_quarantined"));
        assert!(server.repair_hint.contains("transport command or URL"));
        assert_eq!(
            supervisor
                .start_server("docs", retry_at + 2)
                .expect_err("quarantine blocks starts")
                .reason_code,
            "mcp.server_quarantined"
        );
    }

    #[test]
    fn runtime_supervisor_classifies_quarantine_reasons_for_operator_doctor() {
        let policy = McpRuntimeSupervisorPolicy {
            max_retries: 1,
            base_backoff_ms: 100,
            max_backoff_ms: 1_000,
            stderr_tail_bytes: 256,
        };
        let cases = [
            ("mcp.protocol_violation", McpRuntimeErrorClass::ProtocolViolation),
            ("mcp.invalid_schema", McpRuntimeErrorClass::InvalidSchema),
            ("mcp.auth_failure", McpRuntimeErrorClass::AuthFailure),
            ("mcp.output_limit_abuse", McpRuntimeErrorClass::OutputLimitAbuse),
            ("mcp.transport_flapping", McpRuntimeErrorClass::TransportFlapping),
            ("mcp.policy_violation", McpRuntimeErrorClass::PolicyViolation),
        ];

        for (reason_code, expected_class) in cases {
            let mut supervisor = McpRuntimeSupervisor::from_config_with_policy(
                &runtime_config(vec![runtime_server("docs", true)]),
                policy.clone(),
            );
            supervisor.start_server("docs", 1_000).expect("server should start");
            supervisor
                .record_failure("docs", reason_code, "runtime failure", None, 1_001)
                .expect("failure should quarantine");

            let snapshot = supervisor.snapshot(1_001);
            let server = runtime_server_snapshot(&snapshot, "docs");
            assert_eq!(server.state, McpServerLifecycleState::Quarantined);
            assert_eq!(server.last_error_class, Some(expected_class));
            assert_eq!(server.quarantine_reason.as_deref(), Some(expected_class.as_str()));
        }
    }

    #[test]
    fn runtime_supervisor_reload_invalidates_catalog_generation_for_future_runs() {
        let mut supervisor =
            McpRuntimeSupervisor::from_config(&runtime_config(vec![runtime_server("docs", true)]));
        supervisor.start_server("docs", 100).expect("server should start");
        supervisor.record_start_success("docs", 110).expect("server should become healthy");

        let mut next_server = runtime_server("wiki", true);
        next_server.namespace = "wiki".to_owned();
        supervisor.reload_from_config(&runtime_config(vec![next_server]), 200);

        let snapshot = supervisor.snapshot(200);
        assert_eq!(snapshot.catalog_generation, 1);
        assert_eq!(snapshot.total_servers, 1);
        assert!(snapshot.servers.iter().all(|server| server.id != "docs"));
        let wiki = runtime_server_snapshot(&snapshot, "wiki");
        assert_eq!(wiki.state, McpServerLifecycleState::Stopped);
        assert_eq!(wiki.catalog_hidden_reason.as_deref(), Some("mcp.server_not_healthy"));
    }

    #[test]
    fn runtime_supervisor_reload_reopens_quarantined_server_for_probe() {
        let policy = McpRuntimeSupervisorPolicy {
            max_retries: 1,
            base_backoff_ms: 100,
            max_backoff_ms: 1_000,
            stderr_tail_bytes: 256,
        };
        let config = runtime_config(vec![runtime_server("docs", true)]);
        let mut supervisor = McpRuntimeSupervisor::from_config_with_policy(&config, policy);
        supervisor.start_server("docs", 100).expect("server should start");
        supervisor
            .record_failure("docs", "mcp.auth_failure", "auth failed", None, 101)
            .expect("failure should quarantine");

        supervisor.reload_from_config(&config, 200);

        let snapshot = supervisor.snapshot(200);
        let server = runtime_server_snapshot(&snapshot, "docs");
        assert_eq!(snapshot.catalog_generation, 1);
        assert_eq!(server.state, McpServerLifecycleState::Stopped);
        assert_eq!(server.last_error_class, Some(McpRuntimeErrorClass::AuthFailure));
        assert_eq!(server.consecutive_failures, 0);
        assert!(server.quarantine_reason.is_none());
        assert_eq!(server.catalog_hidden_reason.as_deref(), Some("mcp.server_not_healthy"));
        assert_eq!(
            supervisor.start_server("docs", 201).expect("reload should reopen probe"),
            McpServerLifecycleState::Starting
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
        assert_eq!(server.last_successful_probe_at_unix_ms, Some(110));
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
    fn mcp_catalog_projection_exposes_healthy_allowlisted_tools_with_hashes() {
        let transport =
            FakeTransport { tools: vec![discovered_tool("search")], ..Default::default() };
        let mut broker = broker_with_ready_manifest(&transport);
        let report = broker.discover_tools("docs", &transport).expect("discovery should run");
        let supervisor = healthy_supervisor_snapshot("docs", 42);
        let config = tool_config(&["mcp.docs.search"]);
        let policy = ToolCatalogPolicySnapshot::direct_from_allowed_tools(&config.allowed_tools);
        let context = mcp_catalog_request_context();

        let snapshot = build_mcp_tool_catalog_snapshot(
            tool_catalog_request(&config, &policy, &context, 42),
            &supervisor,
            std::slice::from_ref(&report),
        );

        let tool = snapshot
            .tools
            .iter()
            .find(|tool| tool.name == "mcp.docs.search")
            .expect("healthy allowlisted MCP tool should be model-visible");
        assert_eq!(tool.internal_schema_hash, report.registry_entries[0].schema_hash);
        assert!(!tool.provider_schema_hash.is_empty());
        assert!(snapshot.catalog_hash.len() >= 16);
    }

    #[test]
    fn mcp_catalog_projection_filters_unhealthy_servers_with_mcp_reason_code() {
        let transport =
            FakeTransport { tools: vec![discovered_tool("search")], ..Default::default() };
        let mut broker = broker_with_ready_manifest(&transport);
        let report = broker.discover_tools("docs", &transport).expect("discovery should run");
        let supervisor = stopped_supervisor_snapshot("docs", 42);
        let config = tool_config(&["mcp.docs.search"]);
        let policy = ToolCatalogPolicySnapshot::direct_from_allowed_tools(&config.allowed_tools);
        let context = mcp_catalog_request_context();

        let snapshot = build_mcp_tool_catalog_snapshot(
            tool_catalog_request(&config, &policy, &context, 42),
            &supervisor,
            &[report],
        );

        assert!(snapshot.tools.iter().all(|tool| tool.name != "mcp.docs.search"));
        let filtered = filtered_catalog_entry(&snapshot, "mcp.docs.search");
        assert_eq!(filtered.reason_code, ToolCatalogFilterReasonCode::RuntimeUnavailable);
        assert_eq!(filtered.external_reason_code.as_deref(), Some("mcp.server_not_healthy"));
        assert!(!snapshot.filtered_tools.iter().any(|tool| {
            tool.name == "mcp.docs.search"
                && tool.reason_code == ToolCatalogFilterReasonCode::UnknownTool
        }));
    }

    #[test]
    fn mcp_catalog_projection_preserves_filtered_discovery_reason_codes() {
        let mut invalid = discovered_tool("write_note");
        invalid
            .input_schema
            .as_object_mut()
            .expect("schema object")
            .insert("$ref".to_owned(), Value::String("https://attacker.example/schema".to_owned()));
        let transport = FakeTransport { tools: vec![invalid], ..Default::default() };
        let mut broker = broker_with_ready_manifest(&transport);
        let report = broker.discover_tools("docs", &transport).expect("discovery should run");
        let supervisor = healthy_supervisor_snapshot("docs", 42);
        let config = tool_config(&["mcp.docs.write_note"]);
        let policy = ToolCatalogPolicySnapshot::direct_from_allowed_tools(&config.allowed_tools);
        let context = mcp_catalog_request_context();

        let snapshot = build_mcp_tool_catalog_snapshot(
            tool_catalog_request(&config, &policy, &context, 42),
            &supervisor,
            &[report],
        );

        let filtered = filtered_catalog_entry(&snapshot, "mcp.docs.write_note");
        assert_eq!(filtered.reason_code, ToolCatalogFilterReasonCode::ProviderSchemaIncompatible);
        assert_eq!(filtered.external_reason_code.as_deref(), Some("schema.unsupported_keyword"));
    }

    #[test]
    fn mcp_catalog_projection_hash_is_deterministic_and_changes_with_schema() {
        let first_report = discovery_report_for_tool(discovered_tool("search"));
        let second_report = discovery_report_for_tool(discovered_tool_with_schema(
            "search",
            json!({
                "type": "object",
                "properties": {
                    "query": { "type": "integer" }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
        ));
        let supervisor = healthy_supervisor_snapshot("docs", 42);
        let config = tool_config(&["mcp.docs.search"]);
        let policy = ToolCatalogPolicySnapshot::direct_from_allowed_tools(&config.allowed_tools);
        let context = mcp_catalog_request_context();

        let first = build_mcp_tool_catalog_snapshot(
            tool_catalog_request(&config, &policy, &context, 42),
            &supervisor,
            std::slice::from_ref(&first_report),
        );
        let first_again = build_mcp_tool_catalog_snapshot(
            tool_catalog_request(&config, &policy, &context, 42),
            &supervisor,
            &[first_report],
        );
        let second = build_mcp_tool_catalog_snapshot(
            tool_catalog_request(&config, &policy, &context, 42),
            &supervisor,
            &[second_report],
        );

        assert_eq!(first.catalog_hash, first_again.catalog_hash);
        assert_ne!(first.catalog_hash, second.catalog_hash);
        assert_ne!(first.tools[0].provider_schema_hash, second.tools[0].provider_schema_hash);
    }

    #[test]
    fn discovery_invalid_schema_quarantines_after_repeated_protocol_violations() {
        let mut invalid = discovered_tool("write_note");
        invalid
            .input_schema
            .as_object_mut()
            .expect("schema object")
            .insert("$ref".to_owned(), Value::String("https://attacker.example/schema".to_owned()));
        let transport =
            FakeTransport { tools: vec![discovered_tool("search"), invalid], ..Default::default() };
        let mut broker = broker_with_ready_manifest(&transport);

        for attempt in 1..=QUARANTINE_AFTER_VIOLATIONS {
            let report = broker.discover_tools("docs", &transport).expect("discovery should run");
            assert_eq!(report.imported_count, 1);
            let state = broker.state("docs").expect("state should be readable");
            if attempt < QUARANTINE_AFTER_VIOLATIONS {
                assert_eq!(state, McpServerLifecycleState::Healthy);
            }
        }

        assert_eq!(
            broker.state("docs").expect("server should be quarantined"),
            McpServerLifecycleState::Quarantined
        );
    }

    #[test]
    fn invocation_denies_policy_before_transport_call() {
        let transport =
            FakeTransport { tools: vec![discovered_tool("search")], ..Default::default() };
        let mut broker = broker_with_discovered_manifest(&transport);
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
        let transport =
            FakeTransport { tools: vec![discovered_tool("search")], ..Default::default() };
        let mut broker = broker_with_discovered_manifest(&transport);
        let mut request = invocation_request();
        request.policy.approval_required = true;

        let outcome = broker.invoke_tool(request, &transport).expect("denial should be attested");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.approval_required");
        assert_eq!(transport.call_count.get(), 0);
    }

    #[test]
    fn invocation_requires_discovered_schema_match_before_transport_call() {
        let transport =
            FakeTransport { tools: vec![discovered_tool("search")], ..Default::default() };
        let mut broker = broker_with_discovered_manifest(&transport);
        let mut request = invocation_request();
        request.schema_hash = "stale_schema_hash".to_owned();

        let outcome = broker.invoke_tool(request, &transport).expect("denial should be attested");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.schema_hash_mismatch");
        assert_eq!(transport.call_count.get(), 0);
    }

    #[test]
    fn invocation_requires_sensitive_discovered_tools_to_have_approval_id() {
        let mut sensitive_tool = discovered_tool("search");
        sensitive_tool.sensitivity = Some(McpToolSensitivity::Sensitive);
        let transport = FakeTransport { tools: vec![sensitive_tool], ..Default::default() };
        let mut broker = broker_with_discovered_manifest(&transport);

        let outcome = broker
            .invoke_tool(invocation_request(), &transport)
            .expect("approval denial should be attested");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.approval_required");
        assert_eq!(transport.call_count.get(), 0);

        let mut approved = invocation_request();
        approved.approval_granted = true;
        approved.approval_id = Some("approval.docs.search.01".to_owned());
        let outcome = broker
            .invoke_tool(approved, &transport)
            .expect("approved sensitive call should execute");

        assert!(outcome.success);
        assert_eq!(outcome.attestation.approval_id.as_deref(), Some("approval.docs.search.01"));
        assert_eq!(transport.call_count.get(), 1);
    }

    #[test]
    fn invocation_requires_scoped_vault_grants_without_leaking_vault_refs() {
        let transport =
            FakeTransport { tools: vec![discovered_tool("search")], ..Default::default() };
        let mut broker = broker_with_discovered_manifest(&transport);
        let mut request = invocation_request();
        request.vault_scoped_grants.clear();

        let outcome = broker.invoke_tool(request, &transport).expect("denial should be attested");
        let serialized = serde_json::to_string(&outcome).expect("outcome should serialize");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.vault_scoped_grant_missing");
        assert!(!serialized.contains("vault://global/docs_api_token"));
        assert_eq!(transport.call_count.get(), 0);
    }

    #[test]
    fn invocation_redacts_model_visible_output_and_attests_transport() {
        let transport = FakeTransport {
            tools: vec![discovered_tool("search")],
            response: Some(McpToolResponse {
                output: json!({
                    "authorization": "Bearer sk-secret-value",
                    "vault_ref": "vault://global/docs_api_token",
                    "ok": true,
                }),
                sampling_requested: false,
                sampling_model_capability: None,
                egress_host_requested: None,
            }),
            ..Default::default()
        };
        let mut broker = broker_with_discovered_manifest(&transport);

        let outcome = broker
            .invoke_tool(invocation_request(), &transport)
            .expect("invocation should be attested");
        let serialized = serde_json::to_string(&outcome).expect("outcome should serialize");

        assert!(outcome.success);
        assert!(outcome.attestation.transport_id.starts_with("mcp.transport."));
        assert_eq!(outcome.attestation.vault_grant_ids, vec!["grant.docs.api_token.01".to_owned()]);
        assert!(!serialized.contains("sk-secret-value"));
        assert!(!serialized.contains("vault://global/docs_api_token"));
        assert_eq!(transport.call_count.get(), 1);
    }

    #[test]
    fn invocation_blocks_expired_oauth_grant_with_repair_hint_before_transport() {
        let transport =
            FakeTransport { tools: vec![discovered_tool("search")], ..Default::default() };
        let mut manifest = manifest();
        manifest.oauth_required = true;
        manifest.oauth_grant = Some(McpOAuthGrant {
            grant_id: "grant.docs.oauth.01".to_owned(),
            access_token_vault_ref: "global/mcp.docs.access".to_owned(),
            refresh_token_vault_ref: Some("global/mcp.docs.refresh".to_owned()),
            metadata_vault_ref: "global/mcp.docs.grant".to_owned(),
            scopes: vec!["docs.read".to_owned()],
            expires_at_unix_ms: Some(1),
            rotation_id: Some("rotation-1".to_owned()),
            issued_at_unix_ms: 0,
            updated_at_unix_ms: 0,
            revoked_at_unix_ms: None,
        });
        let mut broker = McpBroker::new(policy());
        broker.register_manifest(manifest).expect("manifest should register");
        broker.start_server("docs", &transport).expect("server should start");
        broker.discover_tools("docs", &transport).expect("discovery should run");

        let outcome = broker
            .invoke_tool(invocation_request(), &transport)
            .expect("expired grant should be attested");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.oauth_grant_expired");
        assert_eq!(
            outcome.output_json["repair_hint"],
            "run `palyra mcp login docs` to refresh the MCP OAuth grant"
        );
        assert_eq!(transport.call_count.get(), 0);
    }

    #[test]
    fn expired_oauth_grant_produces_doctor_finding() {
        let mut manifest = manifest();
        manifest.oauth_required = true;
        manifest.oauth_grant = Some(McpOAuthGrant {
            grant_id: "grant.docs.oauth.01".to_owned(),
            access_token_vault_ref: "global/mcp.docs.access".to_owned(),
            refresh_token_vault_ref: None,
            metadata_vault_ref: "global/mcp.docs.grant".to_owned(),
            scopes: Vec::new(),
            expires_at_unix_ms: Some(99),
            rotation_id: None,
            issued_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            revoked_at_unix_ms: None,
        });

        let findings = mcp_oauth_grant_doctor_findings(&[manifest], 100);

        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, "mcp.oauth_grant_expired");
        assert!(findings[0].fix_hint.contains("palyra mcp login docs"));
    }

    #[test]
    fn invocation_rejects_sampling_and_quarantines_after_repeated_violations() {
        let transport = FakeTransport {
            tools: vec![discovered_tool("search")],
            response: Some(McpToolResponse {
                output: json!({"ok": true}),
                sampling_requested: true,
                sampling_model_capability: Some("model:gpt-5".to_owned()),
                egress_host_requested: None,
            }),
            ..Default::default()
        };
        let mut broker = broker_with_discovered_manifest(&transport);

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
    fn invocation_allows_sampling_only_for_explicit_model_capability() {
        let transport = FakeTransport {
            tools: vec![discovered_tool("search")],
            response: Some(McpToolResponse {
                output: json!({"ok": true}),
                sampling_requested: true,
                sampling_model_capability: Some("model:gpt-5".to_owned()),
                egress_host_requested: None,
            }),
            ..Default::default()
        };
        let mut manifest = manifest();
        manifest.sampling_policy = McpSamplingPolicy {
            mode: McpSamplingMode::Allowlist,
            allowed_model_capabilities: vec!["model:gpt-5".to_owned()],
        };
        let mut broker = McpBroker::new(policy());
        broker.register_manifest(manifest).expect("manifest should register");
        broker.start_server("docs", &transport).expect("server should start");
        broker.discover_tools("docs", &transport).expect("discovery should run");

        let outcome = broker
            .invoke_tool(invocation_request(), &transport)
            .expect("allowlisted sampling request should be attested");

        assert!(outcome.success);
        assert_eq!(outcome.attestation.sampling_model_capability.as_deref(), Some("model:gpt-5"));
        assert_eq!(
            broker.state("docs").expect("state should remain healthy"),
            McpServerLifecycleState::Healthy
        );
    }

    #[test]
    fn invocation_rejects_egress_outside_allowlist_and_large_output() {
        let transport = FakeTransport {
            tools: vec![discovered_tool("search")],
            response: Some(McpToolResponse {
                output: json!({"data": "x".repeat(2048)}),
                sampling_requested: false,
                sampling_model_capability: None,
                egress_host_requested: Some("evil.example".to_owned()),
            }),
            ..Default::default()
        };
        let mut broker = broker_with_discovered_manifest(&transport);

        let outcome = broker
            .invoke_tool(invocation_request(), &transport)
            .expect("denial should be attested");

        assert!(!outcome.success);
        assert_eq!(outcome.attestation.policy_outcome, "mcp.egress_denied");

        let transport = FakeTransport {
            tools: vec![discovered_tool("search")],
            response: Some(McpToolResponse {
                output: json!({"data": "x".repeat(2048)}),
                sampling_requested: false,
                sampling_model_capability: None,
                egress_host_requested: Some("api.example.com".to_owned()),
            }),
            ..Default::default()
        };
        let _ = broker.start_server("docs", &FakeTransport::default());
        let outcome = broker
            .invoke_tool(invocation_request(), &transport)
            .expect("large output should be attested");
        assert!(outcome.success);
        assert!(outcome.attestation.output_truncated);
        assert_eq!(outcome.output_json["artifact_required"], true);
    }

    #[test]
    fn scrubbed_stdio_env_never_exposes_values() {
        let env = scrub_stdio_env(&manifest());

        assert_eq!(env.get("API_TOKEN"), Some(&"<redacted>".to_owned()));
    }

    fn healthy_supervisor_snapshot(
        id: &str,
        generated_at_unix_ms: i64,
    ) -> McpRuntimeSupervisorSnapshot {
        let mut supervisor =
            McpRuntimeSupervisor::from_config(&runtime_config(vec![runtime_server(id, true)]));
        supervisor.start_server(id, generated_at_unix_ms - 2).expect("server should start");
        supervisor
            .record_start_success(id, generated_at_unix_ms - 1)
            .expect("server should become healthy");
        supervisor.snapshot(generated_at_unix_ms)
    }

    fn stopped_supervisor_snapshot(
        id: &str,
        generated_at_unix_ms: i64,
    ) -> McpRuntimeSupervisorSnapshot {
        McpRuntimeSupervisor::from_config(&runtime_config(vec![runtime_server(id, true)]))
            .snapshot(generated_at_unix_ms)
    }

    fn tool_catalog_request<'a>(
        config: &'a ToolCallConfig,
        catalog_policy: &'a ToolCatalogPolicySnapshot,
        request_context: &'a ToolRequestContext,
        created_at_unix_ms: i64,
    ) -> ToolCatalogBuildRequest<'a> {
        ToolCatalogBuildRequest {
            config,
            catalog_policy,
            browser_service_enabled: false,
            browser_service_configured: false,
            request_context,
            provider_kind: "openai_compatible",
            provider_model_id: None,
            surface: ToolExposureSurface::RunStream,
            remaining_tool_budget: None,
            created_at_unix_ms,
        }
    }

    fn mcp_catalog_request_context() -> ToolRequestContext {
        ToolRequestContext {
            principal: "user:test".to_owned(),
            device_id: None,
            channel: Some("console".to_owned()),
            session_id: None,
            run_id: None,
            skill_id: None,
        }
    }

    fn filtered_catalog_entry<'a>(
        snapshot: &'a ModelVisibleToolCatalogSnapshot,
        name: &str,
    ) -> &'a FilteredToolCatalogEntry {
        snapshot
            .filtered_tools
            .iter()
            .find(|entry| entry.name == name)
            .expect("filtered catalog entry should exist")
    }

    fn discovery_report_for_tool(tool: McpDiscoveredTool) -> McpToolDiscoveryReport {
        let transport = FakeTransport { tools: vec![tool], ..Default::default() };
        let mut broker = broker_with_ready_manifest(&transport);
        broker.discover_tools("docs", &transport).expect("discovery should run")
    }

    #[test]
    fn runtime_transport_http_lists_tools_via_egress_policy() {
        let body = jsonrpc_response(json!({
            "tools": [{
                "name": "search",
                "description": "Search docs",
                "inputSchema": {"type": "object"}
            }]
        }));
        let url = spawn_fake_mcp_http_server("application/json", body);
        let manifest = remote_manifest(McpTransportManifest::Http { url });

        let tools =
            McpRuntimeTransport.list_tools(&manifest).expect("HTTP tools/list should succeed");

        assert_eq!(tools, vec![discovered_tool_without_capabilities("search", "Search docs")]);
    }

    #[test]
    fn runtime_transport_sse_reads_json_data_event() {
        let data = jsonrpc_response(json!({
            "tools": [{
                "name": "search",
                "description": "Search docs",
                "inputSchema": {"type": "object"}
            }]
        }));
        let url = spawn_fake_mcp_http_server(
            "text/event-stream",
            format!("event: message\ndata: {data}\n\n"),
        );
        let manifest = remote_manifest(McpTransportManifest::Sse { url });

        let tools =
            McpRuntimeTransport.list_tools(&manifest).expect("SSE tools/list should succeed");

        assert_eq!(tools, vec![discovered_tool_without_capabilities("search", "Search docs")]);
    }

    #[test]
    fn runtime_transport_remote_egress_denies_hosts_outside_manifest_allowlist() {
        let mut manifest = manifest();
        manifest.transport =
            McpTransportManifest::Http { url: "https://blocked.example/mcp".to_owned() };
        manifest.egress_allowlist = vec!["allowed.example".to_owned()];

        let error = McpRuntimeTransport
            .start(&manifest)
            .expect_err("host outside manifest allowlist must fail closed");

        assert_eq!(error.reason_code, "mcp.egress_denied");
        assert!(!error.message.contains("blocked.example/mcp"));
    }

    #[test]
    fn runtime_transport_strips_credentials_from_rpc_errors() {
        let body = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": -32000,
                "message": "Authorization: Bearer sk-secret-value token=plain-secret vault://global/api_key"
            }
        }))
        .expect("JSON-RPC error fixture should serialize");
        let url = spawn_fake_mcp_http_server("application/json", body);
        let manifest = remote_manifest(McpTransportManifest::Http { url });

        let error = McpRuntimeTransport
            .list_tools(&manifest)
            .expect_err("JSON-RPC error should fail transport call");

        assert_eq!(error.reason_code, "mcp.transport_rpc_error");
        assert!(!error.message.contains("sk-secret-value"));
        assert!(!error.message.contains("plain-secret"));
        assert!(!error.message.contains("vault://global/api_key"));
    }

    #[test]
    fn runtime_transport_stdio_lists_tools_from_fake_server() {
        let tempdir = tempfile::tempdir().expect("tempdir should be available");
        let script_path = tempdir.path().join("fake_mcp_stdio.py");
        std::fs::write(&script_path, fake_stdio_server_script())
            .expect("fake MCP stdio script should be written");
        let mut manifest = manifest();
        manifest.transport = McpTransportManifest::Stdio {
            command: python_command_for_script(&script_path),
            env: BTreeMap::new(),
        };
        manifest.max_response_bytes = 4 * 1024;
        // Windows CI can spend several seconds spawning Python from a cold runner;
        // this test asserts stdio protocol behavior, not timeout policy.
        manifest.timeout_ms = 10_000;
        manifest.start_timeout_ms = 5_000;

        let tools =
            McpRuntimeTransport.list_tools(&manifest).expect("stdio tools/list should succeed");

        assert_eq!(tools, vec![discovered_tool_without_capabilities("search", "Search docs")]);
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

    fn discovered_tool_without_capabilities(name: &str, description: &str) -> McpDiscoveredTool {
        McpDiscoveredTool {
            name: name.to_owned(),
            description: description.to_owned(),
            input_schema: json!({"type": "object"}),
            capabilities: Vec::new(),
            sensitivity: None,
            approval_policy: None,
        }
    }

    fn jsonrpc_response(result: Value) -> String {
        serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        }))
        .expect("JSON-RPC fixture should serialize")
    }

    fn spawn_fake_mcp_http_server(content_type: &str, body: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("fake server should bind");
        let address = listener.local_addr().expect("fake server should expose address");
        let content_type = content_type.to_owned();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("fake server should accept request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout should be configurable");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).expect("fake server request read should work");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).expect("fake server response write should work");
        });
        format!("http://{address}/mcp")
    }

    fn python_command_for_script(script_path: &Path) -> Vec<String> {
        let script = script_path.display().to_string();
        if command_succeeds("python3", &["--version"]) {
            return vec!["python3".to_owned(), script];
        }
        if command_succeeds("python", &["--version"]) {
            return vec!["python".to_owned(), script];
        }
        if command_succeeds("py", &["-3", "--version"]) {
            return vec!["py".to_owned(), "-3".to_owned(), script];
        }
        panic!("python interpreter is required for fake MCP stdio integration test");
    }

    fn command_succeeds(program: &str, args: &[&str]) -> bool {
        Command::new(program)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    fn fake_stdio_server_script() -> &'static str {
        r#"
import json
import sys

def read_message():
    header = b""
    while not header.endswith(b"\r\n\r\n"):
        chunk = sys.stdin.buffer.read(1)
        if not chunk:
            sys.exit(2)
        header += chunk
    length = None
    for line in header.decode("utf-8").split("\r\n"):
        if line.lower().startswith("content-length:"):
            length = int(line.split(":", 1)[1].strip())
            break
    if length is None:
        sys.exit(3)
    return json.loads(sys.stdin.buffer.read(length))

def write_message(payload):
    body = json.dumps(payload).encode("utf-8")
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8"))
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()

while True:
    message = read_message()
    method = message.get("method")
    if method == "initialize":
        write_message({
            "jsonrpc": "2.0",
            "id": message.get("id"),
            "result": {
                "protocolVersion": message["params"]["protocolVersion"],
                "capabilities": {},
                "serverInfo": {"name": "fake-stdio", "version": "1.0.0"},
            },
        })
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        write_message({
            "jsonrpc": "2.0",
            "id": message.get("id"),
            "result": {
                "tools": [{
                    "name": "search",
                    "description": "Search docs",
                    "inputSchema": {"type": "object"},
                }]
            },
        })
        break
    elif method == "tools/call":
        write_message({
            "jsonrpc": "2.0",
            "id": message.get("id"),
            "result": {"ok": True, "echo": message["params"]["arguments"]},
        })
        break
    else:
        write_message({
            "jsonrpc": "2.0",
            "id": message.get("id"),
            "error": {"code": -32601, "message": "unknown method"},
        })
        break
"#
    }
}
